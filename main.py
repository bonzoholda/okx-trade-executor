import asyncio
import logging
import ccxt.async_support as ccxt
from fastapi import FastAPI, HTTPException, Header, BackgroundTasks
from pydantic import BaseModel
import pandas as pd
import numpy as np
from config import Config

# Setup Logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("OKX-Executor")

app = FastAPI(title="OKX Trade Executor Engine")

# --- GLOBAL IN-MEMORY STATE ---
active_strategy = {
    "symbol": "ADA/USDT",
    "rsi_period": 14,
    "rsi_lower": 23.0,
    "rsi_upper": 60.0,
    "stop_loss_pct": 0.025,
    "take_profit_pct": 0.070,
    "is_active": True,
}

current_position = None  # None atau dict {'entry_price': float, 'amount': float, 'sl': float, 'tp': float}


# --- SCHEMA PAYLOAD WEBHOOK ---
# 1. Update Skema Payload agar Fleksibel Menerima Tipe Data
class StrategyPayload(BaseModel):
    symbol: str
    rsi_period: int
    rsi_lower: float
    rsi_upper: float
    stop_loss_pct: float
    take_profit_pct: float

    class Config:
        coerce_numbers_to_str = False # Memastikan parsing angka presisi


# 2. Tambahkan Endpoint Root untuk Mencegah Error 404 dari Railway Health Check
@app.get("/")
async def root_check():
    return {"status": "ok", "service": "OKX Trade Executor"}


# --- OKX CLIENT INITIALIZATION ---
def get_okx_exchange():
    exchange = ccxt.okx(
        {
            "apiKey": Config.OKX_API_KEY,
            "secret": Config.OKX_SECRET_KEY,
            "password": Config.OKX_PASSPHRASE,
            "enableRateLimit": True,
        }
    )
    if Config.OKX_SANDBOX:
        exchange.set_sandbox_mode(True)
    return exchange


# --- CALCULATION HELPER ---
def calculate_rsi(series: pd.Series, period: int) -> float:
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return float(rsi.iloc[-1])


# --- WEBHOOK ENDPOINT (RECEIVER FROM REPO 1) ---
@app.post("/webhook/strategy-update")
async def update_strategy(
    payload: StrategyPayload, authorization: str = Header(None)
):
    """
    Endpoint yang dipanggil oleh Repo 1 (Research Engine) saat menemukan Winner Strategy baru
    """
    if authorization != f"Bearer {Config.WEBHOOK_SECRET}":
        raise HTTPException(
            status_code=401, detail="Unauthorized Webhook Secret"
        )

    global active_strategy
    active_strategy.update(
        {
            "symbol": payload.symbol,
            "rsi_period": payload.rsi_period,
            "rsi_lower": payload.rsi_lower,
            "rsi_upper": payload.rsi_upper,
            "stop_loss_pct": payload.stop_loss_pct,
            "take_profit_pct": payload.take_profit_pct,
            "is_active": True,
        }
    )

    logger.info(
        f"🔥 [Hot-Reload] New Strategy Received for [{payload.symbol}]!"
    )
    logger.info(
        f"   RSI Period: {payload.rsi_period} | Buy Threshold: < {payload.rsi_lower} | Sell Threshold: > {payload.rsi_upper}"
    )

    return {
        "status": "success",
        "message": f"Strategy for {payload.symbol} updated successfully.",
    }


@app.get("/health")
async def health_check():
    return {
        "status": "online",
        "active_symbol": active_strategy.get("symbol"),
        "has_position": current_position is not None,
    }


# --- BACKGROUND EXECUTION LOOP ---
async def execution_loop():
    global current_position
    logger.info("⚡ Execution Loop Started. Monitoring OKX Price Action...")

    while True:
        try:
            if not active_strategy.get("is_active"):
                await asyncio.sleep(10)
                continue

            symbol = active_strategy["symbol"]
            period = active_strategy["rsi_period"]

            exchange = get_okx_exchange()

            # 1. Fetch Latest Candles dari OKX
            ohlcv = await exchange.fetch_ohlcv(symbol, timeframe="1h", limit=100)
            await exchange.close()

            df = pd.DataFrame(
                ohlcv,
                columns=["timestamp", "open", "high", "low", "close", "volume"],
            )
            current_price = float(df["close"].iloc[-1])
            current_rsi = calculate_rsi(df["close"], period)

            logger.info(
                f"📊 [{symbol}] Price: ${current_price:,.4f} | RSI({period}): {current_rsi:.2f}"
            )

            # 2. LOGIC EKSEKUSI TRADE
            # A. Jika Belum Punya Posisi -> Cek Sinyal BUY
            if current_position is None:
                if current_rsi < active_strategy["rsi_lower"]:
                    logger.info(
                        f"🟢 BUY SIGNAL MATCHED! RSI ({current_rsi:.2f}) < Threshold ({active_strategy['rsi_lower']})"
                    )

                    # Hitung Order Amount berdasarkan USDT Allocation
                    amount = Config.TRADE_AMOUNT_USDT / current_price

                    # Simulasi / Live Order Execution
                    if Config.OKX_API_KEY:
                        exchange_live = get_okx_exchange()
                        order = await exchange_live.create_market_buy_order(
                            symbol, amount
                        )
                        await exchange_live.close()
                        logger.info(f"✅ Market BUY Order Executed: {order['id']}")
                    else:
                        logger.info("⚠️ [Paper Trade] Simulating Market BUY Order...")

                    # Record State Posisi
                    sl_price = current_price * (
                        1 - active_strategy["stop_loss_pct"]
                    )
                    tp_price = current_price * (
                        1 + active_strategy["take_profit_pct"]
                    )
                    current_position = {
                        "entry_price": current_price,
                        "amount": amount,
                        "sl": sl_price,
                        "tp": tp_price,
                    }
                    logger.info(
                        f"🎯 Position Opened | SL: ${sl_price:,.4f} | TP: ${tp_price:,.4f}"
                    )

            # B. Jika Sedang Punya Posisi -> Cek Stop Loss, Take Profit, atau RSI EXIT
            else:
                entry = current_position["entry_price"]
                sl = current_position["sl"]
                tp = current_position["tp"]

                should_close = False
                reason = ""

                if current_price <= sl:
                    should_close = True
                    reason = "STOP LOSS HIT"
                elif current_price >= tp:
                    should_close = True
                    reason = "TAKE PROFIT HIT"
                elif current_rsi > active_strategy["rsi_upper"]:
                    should_close = True
                    reason = (
                        f"RSI EXIT SIGNAL ({current_rsi:.2f} > {active_strategy['rsi_upper']})"
                    )

                if should_close:
                    logger.info(
                        f"🔴 CLOSING POSITION ({reason}) at ${current_price:,.4f}"
                    )

                    if Config.OKX_API_KEY:
                        exchange_live = get_okx_exchange()
                        order = await exchange_live.create_market_sell_order(
                            symbol, current_position["amount"]
                        )
                        await exchange_live.close()
                        logger.info(
                            f"✅ Market SELL Order Executed: {order['id']}"
                        )
                    else:
                        logger.info(
                            "⚠️ [Paper Trade] Simulating Market SELL Order..."
                        )

                    pnl_pct = (current_price - entry) / entry * 100
                    logger.info(f"💵 Closed Trade PnL: {pnl_pct:+.2f}%")
                    current_position = None

        except Exception as e:
            logger.error(f"❌ Error in Execution Loop: {e}")

        # Polling Interval (misal: Cek tiap 30 detik)
        await asyncio.sleep(30)


@app.on_event("startup")
async def startup_event():
    # Jalankan background loop asyncio bersamaan dengan Uvicorn Webserver
    asyncio.create_task(execution_loop())


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=Config.PORT, reload=False)