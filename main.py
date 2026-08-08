import asyncio
import logging
import os
import asyncpg
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

app = FastAPI(title="OKX Multi-Slot Futures Paper Trading Executor Engine")

# --- CONFIGURATION ---
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "my_secret_token_123")
TRADE_AMOUNT_USDT = float(os.getenv("TRADE_AMOUNT_USDT", "20.0")) # Margin per slot
LEVERAGE = int(os.getenv("LEVERAGE", "3"))
MAX_SLOTS = int(os.getenv("MAX_SLOTS", "3"))                      # Maksimal 3 slot aktif
PORT = int(os.getenv("PORT", "8080"))
DATABASE_URL = os.getenv("DATABASE_URL", "")

# --- RISK PERFORMANCE PARAMETERS ---
MIN_RSI_EXIT_PNL_PCT = 2.5    # Minimum Floating PnL (%) sebelum RSI Exit diizinkan
BREAKEVEN_TRIGGER_PNL_PCT = 1.5 # Floating PnL (%) untuk menggeser SL ke Entry Price
TRAILING_STOP_DIST_PCT = 1.0   # Jarak Trailing Stop (%)

# PostgreSQL Connection Pool
db_pool = None

# --- MULTI-SLOT IN-MEMORY STATE ---
# Active Strategies Map -> Format: {"BTC/USDT": strat_dict, "ADA/USDT": strat_dict, ...}
active_strategies = {
    "BTC/USDT": {
        "symbol": "BTC/USDT",
        "direction": "LONG",
        "leverage": LEVERAGE,
        "rsi_period": 14,
        "rsi_lower": 30.0,
        "rsi_upper": 70.0,
        "stop_loss_pct": 0.02,
        "take_profit_pct": 0.05,
        "current_rsi": 50.0,
        "is_active": True,
    }
}

# Active Positions Dict -> Format: {"BTC/USDT": pos_dict, "ETH/USDT": pos_dict, ...}
active_positions = {}


class StrategyPayload(BaseModel):
    symbol: str
    direction: str
    rsi_period: int
    rsi_lower: float
    rsi_upper: float
    stop_loss_pct: float
    take_profit_pct: float


# --- DATABASE HELPER FUNCTIONS ---
async def init_db():
    global db_pool
    if not DATABASE_URL:
        logger.warning("⚠️ DATABASE_URL not set! Running in-memory mode without persistence.")
        return

    try:
        pg_url = DATABASE_URL.replace("postgres://", "postgresql://")
        db_pool = await asyncpg.create_pool(pg_url, min_size=1, max_size=10)
        
        async with db_pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL,
                    direction VARCHAR(10) NOT NULL,
                    leverage VARCHAR(10) NOT NULL,
                    entry_price NUMERIC(18, 6) NOT NULL,
                    exit_price NUMERIC(18, 6) NOT NULL,
                    entry_time VARCHAR(30) NOT NULL,
                    exit_time VARCHAR(30) NOT NULL,
                    exit_reason VARCHAR(100) NOT NULL,
                    pnl_usdt NUMERIC(18, 4) NOT NULL,
                    pnl_pct NUMERIC(10, 2) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
        logger.info("🐘 PostgreSQL Database Connected & Table 'trades' Verified!")
    except Exception as e:
        logger.error(f"❌ Database Connection Error: {e}")


async def save_closed_trade_to_db(trade_record: dict):
    if not db_pool:
        return
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO trades (symbol, direction, leverage, entry_price, exit_price, entry_time, exit_time, exit_reason, pnl_usdt, pnl_pct)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            """, 
            trade_record['symbol'], trade_record['direction'], trade_record['leverage'],
            trade_record['entry_price'], trade_record['exit_price'], trade_record['entry_time'],
            trade_record['exit_time'], trade_record['exit_reason'], trade_record['pnl_usdt'],
            trade_record['pnl_pct']
            )
            logger.info(f"💾 Trade [{trade_record['symbol']}] saved permanently to PostgreSQL!")
    except Exception as e:
        logger.error(f"❌ Failed to save trade to DB: {e}")


async def fetch_stats_from_db():
    if not db_pool:
        return {
            "initial_balance": 1000.0, "current_balance": 1000.0,
            "total_trades": 0, "winning_trades": 0, "losing_trades": 0,
            "total_pnl_usdt": 0.0, "win_rate_pct": 0.0
        }, []

    try:
        async with db_pool.acquire() as conn:
            summary_row = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_trades,
                    COALESCE(SUM(CASE WHEN pnl_usdt >= 0 THEN 1 ELSE 0 END), 0) as winning_trades,
                    COALESCE(SUM(CASE WHEN pnl_usdt < 0 THEN 1 ELSE 0 END), 0) as losing_trades,
                    COALESCE(SUM(pnl_usdt), 0) as total_pnl
                FROM trades
            """)

            recent_trades_rows = await conn.fetch("""
                SELECT id, symbol, direction, leverage, entry_price, exit_price, exit_reason, pnl_usdt, pnl_pct, exit_time
                FROM trades ORDER BY id ASC LIMIT 50
            """)

            total_trades = summary_row['total_trades'] or 0
            winning_trades = summary_row['winning_trades'] or 0
            losing_trades = summary_row['losing_trades'] or 0
            total_pnl = float(summary_row['total_pnl'] or 0.0)
            win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0.0

            recent_trades = [dict(r) for r in recent_trades_rows]

            return {
                "initial_balance": 1000.0,
                "current_balance": 1000.0 + total_pnl,
                "total_trades": total_trades,
                "winning_trades": winning_trades,
                "losing_trades": losing_trades,
                "total_pnl_usdt": round(total_pnl, 4),
                "win_rate_pct": round(win_rate, 1)
            }, recent_trades
    except Exception as e:
        logger.error(f"❌ Failed to fetch stats from DB: {e}")
        return {"initial_balance": 1000.0, "current_balance": 1000.0, "total_trades": 0, "winning_trades": 0, "losing_trades": 0, "total_pnl_usdt": 0.0, "win_rate_pct": 0.0}, []


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

    global active_strategies
    active_strategies[payload.symbol] = {
        "symbol": payload.symbol,
        "direction": payload.direction.upper(),
        "leverage": LEVERAGE,
        "rsi_period": payload.rsi_period,
        "rsi_lower": payload.rsi_lower,
        "rsi_upper": payload.rsi_upper,
        "stop_loss_pct": payload.stop_loss_pct,
        "take_profit_pct": payload.take_profit_pct,
        "current_rsi": 50.0,
        "is_active": True,
    }

    logger.info(
        f"🔥 [Hot-Reload] Strategy Updated for [{payload.symbol}] ({payload.direction})! Total Active Strategies: {len(active_strategies)}"
    )
    return {
        "status": "success",
        "message": f"Strategy for {payload.symbol} ({payload.direction}) registered successfully.",
    }


if os.path.exists("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
async def root_check():
    return {"status": "ok", "service": "OKX Multi-Slot Futures Paper Trading Executor"}


@app.get("/dashboard")
async def serve_dashboard():
    if os.path.exists("static/index.html"):
        return FileResponse("static/index.html")
    return {"error": "Dashboard UI file not found."}


@app.get("/position")
async def get_active_positions():
    """Mengembalikan daftar seluruh posisi aktif (maksimal 3 slot)"""
    return {
        "max_slots": MAX_SLOTS,
        "active_slots_count": len(active_positions),
        "has_positions": len(active_positions) > 0,
        "positions": list(active_positions.values())
    }


@app.get("/stats")
async def get_performance_stats():
    summary_stats, recent_trades = await fetch_stats_from_db()
    return {
        "summary": summary_stats,
        "max_slots": MAX_SLOTS,
        "active_slots_count": len(active_positions),
        "active_strategies": list(active_strategies.values()),
        "recent_trades": recent_trades,
    }


# --- BACKGROUND EXECUTION LOOP (MULTI-SLOT ENGINE) ---
async def execution_loop():
    global active_positions, active_strategies
    logger.info(
        f"⚡ Multi-Slot Futures Engine Started. Max Slots: {MAX_SLOTS} | Margin/Slot: ${TRADE_AMOUNT_USDT} USDT @ {LEVERAGE}x"
    )

    exchange = ccxt.okx({"enableRateLimit": True})

    while True:
        try:
            if not active_strategies:
                await asyncio.sleep(10)
                continue

            # ------------------------------------------------------------------
            # 1. EVALUASI POSISI AKTIF (DYNAMIC EXIT & TRAILING STOP MONITORING)
            # ------------------------------------------------------------------
            for symbol, pos in list(active_positions.items()):
                try:
                    strat = active_strategies.get(symbol, {})
                    period = strat.get("rsi_period", 14)

                    ohlcv = await exchange.fetch_ohlcv(symbol, timeframe="15m", limit=100)
                    df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
                    current_price = float(df["close"].iloc[-1])
                    current_rsi = calculate_rsi(df["close"], period)

                    pos_direction = pos["direction"]
                    entry_p = pos["entry_price"]
                    sl_p = pos["stop_loss_price"]
                    tp_p = pos["take_profit_price"]

                    # Hitung PnL Berdasarkan Direction & Leverage
                    if pos_direction == "LONG":
                        raw_price_change_pct = (current_price - entry_p) / entry_p
                        if current_price > pos["best_price"]:
                            pos["best_price"] = current_price
                    else:  # SHORT
                        raw_price_change_pct = (entry_p - current_price) / entry_p
                        if current_price < pos["best_price"]:
                            pos["best_price"] = current_price

                    floating_pnl_pct = raw_price_change_pct * LEVERAGE * 100
                    floating_pnl_usdt = TRADE_AMOUNT_USDT * (raw_price_change_pct * LEVERAGE)

                    pos["current_price"] = current_price
                    pos["current_rsi"] = current_rsi
                    pos["floating_pnl_pct"] = floating_pnl_pct
                    pos["floating_pnl_usdt"] = floating_pnl_usdt

                    # Update status RSI di active_strategies
                    if symbol in active_strategies:
                        active_strategies[symbol]["current_rsi"] = current_rsi

                    # Trailing Stop & Break-Even Protector
                    best_p = pos["best_price"]

                    if floating_pnl_pct >= BREAKEVEN_TRIGGER_PNL_PCT and not pos["is_breakeven_active"]:
                        pos["stop_loss_price"] = entry_p
                        pos["is_breakeven_active"] = True
                        sl_p = entry_p
                        logger.info(f"🛡️ [BREAK-EVEN ACTIVATED] [{symbol}] SL moved to Entry Price (${entry_p:,.4f}).")

                    if floating_pnl_pct >= MIN_RSI_EXIT_PNL_PCT:
                        if pos_direction == "LONG":
                            new_trailing_sl = best_p * (1 - (TRAILING_STOP_DIST_PCT / 100 / LEVERAGE))
                            if new_trailing_sl > sl_p:
                                pos["stop_loss_price"] = new_trailing_sl
                                sl_p = new_trailing_sl
                                logger.info(f"📈 [TRAILING STOP UPDATED] [{symbol}] New Long SL: ${sl_p:,.4f}")
                        else:  # SHORT
                            new_trailing_sl = best_p * (1 + (TRAILING_STOP_DIST_PCT / 100 / LEVERAGE))
                            if new_trailing_sl < sl_p:
                                pos["stop_loss_price"] = new_trailing_sl
                                sl_p = new_trailing_sl
                                logger.info(f"📉 [TRAILING STOP UPDATED] [{symbol}] New Short SL: ${sl_p:,.4f}")

                    logger.info(
                        f"📈 [SLOT ACTIVE: {symbol} {pos_direction} {LEVERAGE}x] Price: ${current_price:,.4f} | Entry: ${entry_p:,.4f} | PnL: {floating_pnl_pct:+.2f}% (${floating_pnl_usdt:+.2f} USDT)"
                    )

                    # Cek Trigger Exit
                    should_close = False
                    exit_reason = ""

                    rsi_upper = strat.get("rsi_upper", 70.0)
                    rsi_lower = strat.get("rsi_lower", 30.0)

                    if pos_direction == "LONG":
                        if current_price <= sl_p:
                            should_close = True
                            exit_reason = "TRAILING / STOP LOSS HIT 🔴" if pos["is_breakeven_active"] else "STOP LOSS HIT 🔴"
                        elif current_price >= tp_p:
                            should_close = True
                            exit_reason = "TAKE PROFIT HIT 🟢"
                        elif current_rsi > rsi_upper and floating_pnl_pct >= MIN_RSI_EXIT_PNL_PCT:
                            should_close = True
                            exit_reason = f"RSI EXIT SIGNAL ({current_rsi:.1f}) @ PnL {floating_pnl_pct:+.2f}% 🟡"
                    else:  # SHORT
                        if current_price >= sl_p:
                            should_close = True
                            exit_reason = "TRAILING / STOP LOSS HIT 🔴" if pos["is_breakeven_active"] else "STOP LOSS HIT 🔴"
                        elif current_price <= tp_p:
                            should_close = True
                            exit_reason = "TAKE PROFIT HIT 🟢"
                        elif current_rsi < rsi_lower and floating_pnl_pct >= MIN_RSI_EXIT_PNL_PCT:
                            should_close = True
                            exit_reason = f"RSI EXIT SIGNAL ({current_rsi:.1f}) @ PnL {floating_pnl_pct:+.2f}% 🟡"

                    # Execute Close & Recycle Slot
                    if should_close:
                        trade_record = {
                            "symbol": symbol,
                            "direction": pos_direction,
                            "leverage": f"{LEVERAGE}x",
                            "entry_price": entry_p,
                            "exit_price": current_price,
                            "entry_time": pos["entry_time"],
                            "exit_time": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "exit_reason": exit_reason,
                            "pnl_usdt": round(floating_pnl_usdt, 4),
                            "pnl_pct": round(floating_pnl_pct, 2),
                        }

                        await save_closed_trade_to_db(trade_record)

                        logger.info("=" * 60)
                        logger.info(f"🔴 [SLOT CLOSED] {symbol} {pos_direction} | Reason: {exit_reason}")
                        logger.info(f"   Exit Price  : ${current_price:,.4f}")
                        logger.info(f"   Realized PnL: {floating_pnl_pct:+.2f}% (${floating_pnl_usdt:+.4f} USDT)")
                        logger.info("=" * 60)

                        # Hapus dari active_positions (SLOT BEBAS KEMBALI)
                        del active_positions[symbol]

                except Exception as pos_err:
                    logger.error(f"❌ Error monitoring position for {symbol}: {pos_err}")

            # ------------------------------------------------------------------
            # 2. CEK PELUANG ENTRY UNTUK SLOT KOSONG (BEBAS DUPLIKASI KOIN)
            # ------------------------------------------------------------------
            if len(active_positions) < MAX_SLOTS:
                for symbol, strat in list(active_strategies.items()):
                    # Cegah duplikasi koin jika symbol ini sudah punya posisi aktif
                    if symbol in active_positions:
                        continue

                    # Jika slot masih tersedia, evaluasi sinyal entry
                    if len(active_positions) >= MAX_SLOTS:
                        break

                    try:
                        direction = strat["direction"]
                        period = strat["rsi_period"]

                        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe="15m", limit=100)
                        df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
                        current_price = float(df["close"].iloc[-1])
                        current_rsi = calculate_rsi(df["close"], period)

                        strat["current_rsi"] = current_rsi

                        is_entry_triggered = False
                        if direction == "LONG" and current_rsi < strat["rsi_lower"]:
                            is_entry_triggered = True
                        elif direction == "SHORT" and current_rsi > strat["rsi_upper"]:
                            is_entry_triggered = True

                        logger.info(
                            f"👀 [SLOT CHECK: {symbol} {direction} {LEVERAGE}x] Price: ${current_price:,.4f} | RSI({period}): {current_rsi:.2f} (Active Slots: {len(active_positions)}/{MAX_SLOTS})"
                        )

                        if is_entry_triggered:
                            effective_margin = TRADE_AMOUNT_USDT * LEVERAGE
                            amount = effective_margin / current_price

                            if direction == "LONG":
                                sl_price = current_price * (1 - strat["stop_loss_pct"])
                                tp_price = current_price * (1 + strat["take_profit_pct"])
                            else:
                                sl_price = current_price * (1 + strat["stop_loss_pct"])
                                tp_price = current_price * (1 - strat["take_profit_pct"])

                            active_positions[symbol] = {
                                "symbol": symbol,
                                "direction": direction,
                                "leverage": LEVERAGE,
                                "entry_price": current_price,
                                "current_price": current_price,
                                "best_price": current_price,
                                "current_rsi": current_rsi,
                                "amount": amount,
                                "margin_usdt": TRADE_AMOUNT_USDT,
                                "initial_stop_loss": sl_price,
                                "stop_loss_price": sl_price,
                                "take_profit_price": tp_price,
                                "floating_pnl_pct": 0.0,
                                "floating_pnl_usdt": 0.0,
                                "is_breakeven_active": False,
                                "entry_time": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
                            }

                            logger.info("=" * 60)
                            logger.info(f"🟢 [NEW SLOT OCCUPIED] [{symbol} {direction}] ENTRY EXECUTED ({len(active_positions)}/{MAX_SLOTS} Slots)")
                            logger.info(f"   Entry Price  : ${current_price:,.4f}")
                            logger.info(f"   Margin / Leverage: ${TRADE_AMOUNT_USDT} USDT @ {LEVERAGE}x")
                            logger.info(f"   Initial Stop Loss: ${sl_price:,.4f}")
                            logger.info(f"   Take Profit Target: ${tp_price:,.4f}")
                            logger.info("=" * 60)

                    except Exception as entry_err:
                        logger.error(f"❌ Error checking entry for {symbol}: {entry_err}")

        except Exception as e:
            logger.error(f"❌ Error in Multi-Slot Execution Loop: {e}")

        await asyncio.sleep(30)


@app.on_event("startup")
async def startup_event():
    await init_db()
    asyncio.create_task(execution_loop())


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=False)
