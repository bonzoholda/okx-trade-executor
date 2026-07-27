import asyncio
import logging
import ccxt.async_support as ccxt
from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel
import pandas as pd
import numpy as np
import os

# Setup Logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("OKX-Executor")

app = FastAPI(title="OKX Paper Trading Executor Engine")

# --- CONFIGURATION ---
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "my_secret_token_123")
TRADE_AMOUNT_USDT = float(os.getenv("TRADE_AMOUNT_USDT", "20.0"))
PORT = int(os.getenv("PORT", "8080"))

# --- GLOBAL IN-MEMORY STATE ---
active_strategy = {
    "symbol": "BTC/USDT",
    "rsi_period": 14,
    "rsi_lower": 30.0,
    "rsi_upper": 70.0,
    "stop_loss_pct": 0.02,
    "take_profit_pct": 0.05,
    "is_active": True,
}

# State Posisi Aktif (None jika tidak ada posisi)
current_position = None

# History Transactions & Stats Tracker
trade_history = []
stats = {
    "initial_balance": 1000.0,
    "current_balance": 1000.0,
    "total_trades": 0,
    "winning_trades": 0,
    "losing_trades": 0,
    "total_pnl_usdt": 0.0,
    "win_rate_pct": 0.0,
}


# --- SCHEMA PAYLOAD WEBHOOK ---
class StrategyPayload(BaseModel):
    symbol: str
    rsi_period: int
    rsi_lower: float
    rsi_upper: float
    stop_loss_pct: float
    take_profit_pct: float


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
    if authorization != f"Bearer {WEBHOOK_SECRET}":
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
        f"🔥 [Hot-Reload] Strategy Updated for [{payload.symbol}]!"
    )
    logger.info(
        f"   RSI({payload.rsi_period}) | Buy: < {payload.rsi_lower} | Sell: > {payload.rsi_upper} | SL: {payload.stop_loss_pct*100:.1f}% | TP: {payload.take_profit_pct*100:.1f}%"
    )

    return {
        "status": "success",
        "message": f"Strategy for {payload.symbol} updated successfully.",
    }


# --- MONITORING ENDPOINTS ---
@app.get("/")
async def root_check():
    return {"status": "ok", "service": "OKX Paper Trading Executor"}


@app.get("/position")
async def get_active_position():
    """Melihat detail posisi yang sedang terbuka saat ini"""
    if current_position is None:
        return {"has_position": False, "message": "No active position."}
    return {"has_position": True, "position": current_position}


@app.get("/stats")
async def get_performance_stats():
    """Melihat statistik PnL dan histori transaksi paper trading"""
    return {
        "summary": stats,
        "active_strategy": active_strategy,
        "recent_trades": trade_history[-10:],  # 10 transaksi terakhir
    }


# --- BACKGROUND EXECUTION LOOP ---
async def execution_loop():
    global current_position, stats, trade_history
    logger.info("⚡ Paper Trading Execution Loop Started. Monitoring OKX Public Feed...")

    exchange = ccxt.okx({"enableRateLimit": True})

    while True:
        try:
            if not active_strategy.get("is_active"):
                await asyncio.sleep(10)
                continue

            symbol = active_strategy["symbol"]
            period = active_strategy["rsi_period"]

            # 1. Fetch Price Feed dari OKX Public REST API
            ohlcv = await exchange.fetch_ohlcv(symbol, timeframe="1h", limit=100)
            df = pd.DataFrame(
                ohlcv,
                columns=["timestamp", "open", "high", "low", "close", "volume"],
            )
            current_price = float(df["close"].iloc[-1])
            current_rsi = calculate_rsi(df["close"], period)

            # 2. STRATEGY EVALUATION & POSITION MONITORING
            if current_position is None:
                # --- NO POSITION: Check for BUY Signal ---
                logger.info(
                    f"👀 [{symbol}] Price: ${current_price:,.4f} | RSI({period}): {current_rsi:.2f} (Target Buy: < {active_strategy['rsi_lower']})"
                )

                if current_rsi < active_strategy["rsi_lower"]:
                    amount = TRADE_AMOUNT_USDT / current_price
                    sl_price = current_price * (1 - active_strategy["stop_loss_pct"])
                    tp_price = current_price * (1 + active_strategy["take_profit_pct"])

                    current_position = {
                        "symbol": symbol,
                        "entry_price": current_price,
                        "current_price": current_price,
                        "amount": amount,
                        "invested_usdt": TRADE_AMOUNT_USDT,
                        "stop_loss_price": sl_price,
                        "take_profit_price": tp_price,
                        "floating_pnl_pct": 0.0,
                        "entry_time": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }

                    logger.info("=" * 60)
                    logger.info(f"🟢 [PAPER TRADE] MARKET BUY EXECUTED for [{symbol}]")
                    logger.info(f"   Entry Price : ${current_price:,.4f}")
                    logger.info(f"   Position Size: {amount:.4f} {symbol.split('/')[0]} (${TRADE_AMOUNT_USDT} USDT)")
                    logger.info(f"   Stop Loss    : ${sl_price:,.4f} (-{active_strategy['stop_loss_pct']*100:.1f}%)")
                    logger.info(f"   Take Profit  : ${tp_price:,.4f} (+{active_strategy['take_profit_pct']*100:.1f}%)")
                    logger.info("=" * 60)

            else:
                # --- HAS POSITION: Track PnL & Check EXIT Signals ---
                pos_symbol = current_position["symbol"]

                # Handle jika ada hot-reload symbol saat posisi masih terbuka
                if pos_symbol != symbol:
                    fetch_pos_ohlcv = await exchange.fetch_ohlcv(pos_symbol, timeframe="1h", limit=50)
                    pos_df = pd.DataFrame(fetch_pos_ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
                    check_price = float(pos_df["close"].iloc[-1])
                    check_rsi = calculate_rsi(pos_df["close"], period)
                else:
                    check_price = current_price
                    check_rsi = current_rsi

                entry_p = current_position["entry_price"]
                sl_p = current_position["stop_loss_price"]
                tp_p = current_position["take_profit_price"]

                # Update Floating Stats
                floating_pnl_pct = ((check_price - entry_p) / entry_p) * 100
                current_position["current_price"] = check_price
                current_position["floating_pnl_pct"] = floating_pnl_pct

                logger.info(
                    f"📈 [{pos_symbol} POSITION ACTIVE] Current: ${check_price:,.4f} | Entry: ${entry_p:,.4f} | PnL: {floating_pnl_pct:+.2f}% | RSI: {check_rsi:.2f}"
                )

                # Check Exit Trigger
                should_close = False
                exit_reason = ""

                if check_price <= sl_p:
                    should_close = True
                    exit_reason = "STOP LOSS HIT 🔴"
                elif check_price >= tp_p:
                    should_close = True
                    exit_reason = "TAKE PROFIT HIT 🟢"
                elif check_rsi > active_strategy["rsi_upper"]:
                    should_close = True
                    exit_reason = f"RSI OVERBOUGHT SIGNAL ({check_rsi:.1f} > {active_strategy['rsi_upper']}) 🟡"

                if should_close:
                    realized_pnl_usdt = (check_price - entry_p) * current_position["amount"]
                    realized_pnl_pct = floating_pnl_pct

                    # Update Portfolio Balance & Stats
                    stats["total_trades"] += 1
                    stats["total_pnl_usdt"] += realized_pnl_usdt
                    stats["current_balance"] += realized_pnl_usdt

                    if realized_pnl_usdt >= 0:
                        stats["winning_trades"] += 1
                    else:
                        stats["losing_trades"] += 1

                    stats["win_rate_pct"] = (
                        stats["winning_trades"] / stats["total_trades"]
                    ) * 100

                    # Record Closed Trade
                    trade_record = {
                        "id": len(trade_history) + 1,
                        "symbol": pos_symbol,
                        "entry_price": entry_p,
                        "exit_price": check_price,
                        "entry_time": current_position["entry_time"],
                        "exit_time": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "exit_reason": exit_reason,
                        "pnl_usdt": round(realized_pnl_usdt, 4),
                        "pnl_pct": round(realized_pnl_pct, 2),
                    }
                    trade_history.append(trade_record)

                    logger.info("=" * 60)
                    logger.info(f"🔴 [PAPER TRADE CLOSED] {pos_symbol} | Reason: {exit_reason}")
                    logger.info(f"   Exit Price  : ${check_price:,.4f}")
                    logger.info(f"   Realized PnL: {realized_pnl_pct:+.2f}% (${realized_pnl_usdt:+.4f} USDT)")
                    logger.info(f"   Total Balance: ${stats['current_balance']:,.2f} USDT (WinRate: {stats['win_rate_pct']:.1f}%)")
                    logger.info("=" * 60)

                    # Reset State Posisi
                    current_position = None

        except Exception as e:
            logger.error(f"❌ Error in Execution Loop: {e}")

        # Polling Interval (Cek harga OKX setiap 30 detik)
        await asyncio.sleep(30)


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(execution_loop())


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=False)