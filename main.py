import asyncio
import logging
import os
import ccxt.async_support as ccxt
from fastapi import FastAPI, HTTPException, Header
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import pandas as pd

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("OKX-Futures-Executor")

app = FastAPI(title="OKX Futures Paper Trading Executor Engine")

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "my_secret_token_123")
TRADE_AMOUNT_USDT = float(os.getenv("TRADE_AMOUNT_USDT", "20.0"))
LEVERAGE = int(os.getenv("LEVERAGE", "3"))
PORT = int(os.getenv("PORT", "8080"))

active_strategy = {
    "symbol": "BTC/USDT",
    "direction": "LONG",
    "leverage": LEVERAGE,
    "rsi_period": 14,
    "rsi_lower": 30.0,
    "rsi_upper": 70.0,
    "stop_loss_pct": 0.02,
    "take_profit_pct": 0.05,
    "current_rsi": 50.0,  # Menyediakan fallback nilai RSI live untuk UI
    "is_active": True,
}

current_position = None
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


class StrategyPayload(BaseModel):
    symbol: str
    direction: str
    rsi_period: int
    rsi_lower: float
    rsi_upper: float
    stop_loss_pct: float
    take_profit_pct: float


def calculate_rsi(series: pd.Series, period: int) -> float:
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return float(rsi.iloc[-1])


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
            "direction": payload.direction.upper(),
            "leverage": LEVERAGE,
            "rsi_period": payload.rsi_period,
            "rsi_lower": payload.rsi_lower,
            "rsi_upper": payload.rsi_upper,
            "stop_loss_pct": payload.stop_loss_pct,
            "take_profit_pct": payload.take_profit_pct,
            "is_active": True,
        }
    )

    logger.info(
        f"🔥 [Hot-Reload] Futures Strategy Received for [{payload.symbol}] ({payload.direction})!"
    )
    return {
        "status": "success",
        "message": f"Futures strategy for {payload.symbol} ({payload.direction}) updated.",
    }


if os.path.exists("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
async def root_check():
    return {"status": "ok", "service": "OKX Futures Paper Trading Executor"}


@app.get("/dashboard")
async def serve_dashboard():
    if os.path.exists("static/index.html"):
        return FileResponse("static/index.html")
    return {"error": "Dashboard UI file not found."}


@app.get("/position")
async def get_active_position():
    if current_position is None:
        return {"has_position": False, "message": "No active position."}
    return {"has_position": True, "position": current_position}


@app.get("/stats")
async def get_performance_stats():
    return {
        "summary": stats,
        "active_strategy": active_strategy,
        "recent_trades": trade_history[-10:],
    }


async def execution_loop():
    global current_position, stats, trade_history, active_strategy
    logger.info(
        f"⚡ Futures Execution Loop Started. Leverage: {LEVERAGE}x | Monitoring OKX..."
    )

    exchange = ccxt.okx({"enableRateLimit": True})

    while True:
        try:
            if not active_strategy.get("is_active"):
                await asyncio.sleep(10)
                continue

            symbol = active_strategy["symbol"]
            direction = active_strategy["direction"]
            period = active_strategy["rsi_period"]

            ohlcv = await exchange.fetch_ohlcv(symbol, timeframe="1h", limit=100)
            df = pd.DataFrame(
                ohlcv,
                columns=["timestamp", "open", "high", "low", "close", "volume"],
            )
            current_price = float(df["close"].iloc[-1])
            current_rsi = calculate_rsi(df["close"], period)

            # Update RSI live ke global state agar terbaca oleh dashboard /stats
            active_strategy["current_rsi"] = current_rsi

            # 1. BELUM PUNYA POSISI -> CEK ENTRY
            if current_position is None:
                is_entry_triggered = False

                if direction == "LONG" and current_rsi < active_strategy["rsi_lower"]:
                    is_entry_triggered = True
                elif direction == "SHORT" and current_rsi > active_strategy["rsi_upper"]:
                    is_entry_triggered = True

                logger.info(
                    f"👀 [{symbol} {direction} {LEVERAGE}x] Price: ${current_price:,.4f} | RSI({period}): {current_rsi:.2f}"
                )

                if is_entry_triggered:
                    effective_margin = TRADE_AMOUNT_USDT * LEVERAGE
                    amount = effective_margin / current_price

                    if direction == "LONG":
                        sl_price = current_price * (1 - active_strategy["stop_loss_pct"])
                        tp_price = current_price * (1 + active_strategy["take_profit_pct"])
                    else:  # SHORT
                        sl_price = current_price * (1 + active_strategy["stop_loss_pct"])
                        tp_price = current_price * (1 - active_strategy["take_profit_pct"])

                    current_position = {
                        "symbol": symbol,
                        "direction": direction,
                        "leverage": LEVERAGE,
                        "entry_price": current_price,
                        "current_price": current_price,
                        "current_rsi": current_rsi,
                        "amount": amount,
                        "margin_usdt": TRADE_AMOUNT_USDT,
                        "stop_loss_price": sl_price,
                        "take_profit_price": tp_price,
                        "floating_pnl_pct": 0.0,
                        "floating_pnl_usdt": 0.0,
                        "entry_time": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }

                    logger.info("=" * 60)
                    logger.info(f"🟢 [FUTURES {direction}] ENTRY EXECUTED for [{symbol}]")
                    logger.info(f"   Entry Price  : ${current_price:,.4f}")
                    logger.info(f"   Margin / Leverage: ${TRADE_AMOUNT_USDT} USDT @ {LEVERAGE}x")
                    logger.info(f"   Stop Loss Target : ${sl_price:,.4f}")
                    logger.info(f"   Take Profit Target: ${tp_price:,.4f}")
                    logger.info("=" * 60)

            # 2. SEDANG PUNYA POSISI -> CEK EXIT
            else:
                pos_direction = current_position["direction"]
                entry_p = current_position["entry_price"]
                sl_p = current_position["stop_loss_price"]
                tp_p = current_position["take_profit_price"]

                # Hitung PnL Berdasarkan Direction & Leverage
                if pos_direction == "LONG":
                    raw_price_change_pct = (current_price - entry_p) / entry_p
                else:  # SHORT
                    raw_price_change_pct = (entry_p - current_price) / entry_p

                floating_pnl_pct = raw_price_change_pct * LEVERAGE * 100
                floating_pnl_usdt = TRADE_AMOUNT_USDT * (raw_price_change_pct * LEVERAGE)

                current_position["current_price"] = current_price
                current_position["current_rsi"] = current_rsi  # Update live RSI posisi aktif
                current_position["floating_pnl_pct"] = floating_pnl_pct
                current_position["floating_pnl_usdt"] = floating_pnl_usdt

                logger.info(
                    f"📈 [{symbol} {pos_direction} {LEVERAGE}x ACTIVE] Price: ${current_price:,.4f} | Entry: ${entry_p:,.4f} | PnL: {floating_pnl_pct:+.2f}% (${floating_pnl_usdt:+.2f} USDT)"
                )

                should_close = False
                exit_reason = ""

                # Sinyal Exit
                if pos_direction == "LONG":
                    if current_price <= sl_p:
                        should_close = True; exit_reason = "STOP LOSS HIT 🔴"
                    elif current_price >= tp_p:
                        should_close = True; exit_reason = "TAKE PROFIT HIT 🟢"
                    elif current_rsi > active_strategy["rsi_upper"]:
                        should_close = True; exit_reason = f"RSI EXIT SIGNAL ({current_rsi:.1f}) 🟡"
                else:  # SHORT
                    if current_price >= sl_p:
                        should_close = True; exit_reason = "STOP LOSS HIT 🔴"
                    elif current_price <= tp_p:
                        should_close = True; exit_reason = "TAKE PROFIT HIT 🟢"
                    elif current_rsi < active_strategy["rsi_lower"]:
                        should_close = True; exit_reason = f"RSI EXIT SIGNAL ({current_rsi:.1f}) 🟡"

                if should_close:
                    stats["total_trades"] += 1
                    stats["total_pnl_usdt"] += floating_pnl_usdt
                    stats["current_balance"] += floating_pnl_usdt

                    if floating_pnl_usdt >= 0:
                        stats["winning_trades"] += 1
                    else:
                        stats["losing_trades"] += 1

                    stats["win_rate_pct"] = (stats["winning_trades"] / stats["total_trades"]) * 100

                    trade_record = {
                        "id": len(trade_history) + 1,
                        "symbol": symbol,
                        "direction": pos_direction,
                        "leverage": f"{LEVERAGE}x",
                        "entry_price": entry_p,
                        "exit_price": current_price,
                        "entry_time": current_position["entry_time"],
                        "exit_time": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "exit_reason": exit_reason,
                        "pnl_usdt": round(floating_pnl_usdt, 4),
                        "pnl_pct": round(floating_pnl_pct, 2),
                    }
                    trade_history.append(trade_record)

                    logger.info("=" * 60)
                    logger.info(f"🔴 [FUTURES CLOSED] {symbol} {pos_direction} | Reason: {exit_reason}")
                    logger.info(f"   Exit Price  : ${current_price:,.4f}")
                    logger.info(f"   Realized PnL: {floating_pnl_pct:+.2f}% (${floating_pnl_usdt:+.4f} USDT)")
                    logger.info(f"   New Balance : ${stats['current_balance']:,.2f} USDT")
                    logger.info("=" * 60)

                    current_position = None

        except Exception as e:
            logger.error(f"❌ Error in Futures Execution Loop: {e}")

        await asyncio.sleep(30)


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(execution_loop())


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=False)