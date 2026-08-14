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
from datetime import datetime, timedelta

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("OKX-Futures-Executor")

app = FastAPI(title="OKX Multi-Slot Futures Paper Trading Executor Engine")

# --- CONFIGURATION ---
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "my_secret_token_123")
RISK_EQUITY_PCT = float(os.getenv("RISK_EQUITY_PCT", "0.10"))     # 10% Equity per trade
MIN_MARGIN_USDT = float(os.getenv("MIN_MARGIN_USDT", "10.0"))     # Minimum Margin per slot
LEVERAGE = int(os.getenv("LEVERAGE", "3"))
MAX_SLOTS = int(os.getenv("MAX_SLOTS", "3"))                      # Maksimal 3 slot aktif
PORT = int(os.getenv("PORT", "8080"))
DATABASE_URL = os.getenv("DATABASE_URL", "")

# --- ENHANCED STEP-LOCK & TIME-BASED RISK PARAMETERS ---
BREAKEVEN_TRIGGER_PNL_PCT = 1.2   # Tier 1: Lock Break-Even (0.0%) at +1.2% PnL
LOCK_PROFIT_TRIGGER_PNL_PCT = 2.0  # Tier 2: Lock +1.0% Profit at +2.0% PnL
MIN_RSI_EXIT_PNL_PCT = 2.5        # Tier 3: Enable Dynamic Trailing & RSI Exit
TRAILING_STOP_DIST_PCT = 1.0      # Trailing Stop Distance (%)
MAX_HOLDING_HOURS = 12             # Timeout exit for stagnant trades (>12 hrs)

# --- ⛔ CIRCUIT BREAKER COOLDOWN PARAMETERS ---
COOLDOWN_HOURS = float(os.getenv("COOLDOWN_HOURS", "2.0")) # Durasi pemblokiran pair setelah Hit SL (Jam)
pair_cooldowns = {}  # Memori internal: {"NEAR/USDT": datetime_target_expire}

# PostgreSQL Connection Pool
db_pool = None

# --- MULTI-SLOT IN-MEMORY STATE ---
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

active_positions = {}


class StrategyPayload(BaseModel):
    symbol: str
    direction: str
    rsi_period: int
    rsi_lower: float
    rsi_upper: float
    stop_loss_pct: float
    take_profit_pct: float


# --- COOLDOWN HELPER FUNCTIONS ---
def is_pair_in_cooldown(symbol: str) -> bool:
    """Mengecek apakah pair sedang diblokir karena baru saja terkena Stop Loss"""
    if symbol in pair_cooldowns:
        expire_time = pair_cooldowns[symbol]
        if datetime.now() < expire_time:
            return True
        else:
            del pair_cooldowns[symbol]
            logger.info(f"🟢 [COOLDOWN EXPIRED] {symbol} is now unblocked and ready for new entries.")
    return False


def trigger_pair_cooldown(symbol: str, reason: str):
    """Memasang proteksi cooldown pada pair setelah terkena SL"""
    expire_time = datetime.now() + timedelta(hours=COOLDOWN_HOURS)
    pair_cooldowns[symbol] = expire_time
    logger.warning(
        f"⛔ [CIRCUIT BREAKER ACTIVATED] {symbol} hit {reason}! Blocked from re-entry for {COOLDOWN_HOURS} hours (Until {expire_time.strftime('%H:%M:%S')})."
    )


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
                FROM trades ORDER BY id DESC LIMIT 500
            """)

            total_trades = summary_row['total_trades'] or 0
            winning_trades = summary_row['winning_trades'] or 0
            losing_trades = summary_row['losing_trades'] or 0
            total_pnl = float(summary_row['total_pnl'] or 0.0)
            win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0.0

            recent_trades = [dict(r) for r in reversed(recent_trades_rows)]

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
        f"🔥 [Hot-Reload] Strategy Updated for [{payload.symbol}] ({payload.direction})!"
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
    return {
        "max_slots": MAX_SLOTS,
        "active_slots_count": len(active_positions),
        "has_positions": len(active_positions) > 0,
        "positions": list(active_positions.values())
    }


@app.get("/cooldowns")
async def get_cooldown_status():
    """Endpoint untuk memantau pair mana saja yang sedang diblokir cooldown"""
    active_cooldowns = {}
    now = datetime.now()
    for symbol, expire in list(pair_cooldowns.items()):
        if now < expire:
            remaining_seconds = int((expire - now).total_seconds())
            active_cooldowns[symbol] = {
                "expires_at": expire.strftime("%Y-%m-%d %H:%M:%S"),
                "remaining_minutes": round(remaining_seconds / 60, 1)
            }
    return {
        "cooldown_duration_hours": COOLDOWN_HOURS,
        "blocked_pairs_count": len(active_cooldowns),
        "blocked_pairs": active_cooldowns
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


@app.delete("/admin/clean-old-trades")
async def clean_old_trades(secret: str):
    """Endpoint admin untuk membersihkan transaksi lama sebelum ID #48"""
    if secret != WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    if not db_pool:
        return {"error": "Database not connected"}

    try:
        async with db_pool.acquire() as conn:
            result = await conn.execute("DELETE FROM trades WHERE id < 48;")
            logger.info("🧹 [DB CLEANUP] Deleted all trades prior to Trade #48.")
            return {"status": "success", "message": f"Successfully deleted trades < #48. Result: {result}"}
    except Exception as e:
        logger.error(f"❌ Failed to delete old trades: {e}")
        return {"status": "error", "message": str(e)}


# --- BACKGROUND EXECUTION LOOP (DYNAMIC SIZING & STEP-LOCK RISK ENGINE) ---
async def execution_loop():
    global active_positions, active_strategies
    logger.info(
        f"⚡ Dynamic Sizing Futures Engine Started. Max Slots: {MAX_SLOTS} | Dynamic Sizing: {RISK_EQUITY_PCT * 100}% Equity/Slot @ {LEVERAGE}x | Cooldown: {COOLDOWN_HOURS}h"
    )

    exchange = ccxt.okx({"enableRateLimit": True})

    while True:
        try:
            if not active_strategies:
                await asyncio.sleep(10)
                continue

            # ------------------------------------------------------------------
            # 1. EVALUASI POSISI AKTIF (STEP-LOCK & TIME-BASED STAGNANCY EXIT)
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
                    slot_margin_usdt = pos.get("margin_usdt", MIN_MARGIN_USDT)

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
                    floating_pnl_usdt = slot_margin_usdt * (raw_price_change_pct * LEVERAGE)

                    pos["current_price"] = current_price
                    pos["current_rsi"] = current_rsi
                    pos["floating_pnl_pct"] = floating_pnl_pct
                    pos["floating_pnl_usdt"] = floating_pnl_usdt

                    if symbol in active_strategies:
                        active_strategies[symbol]["current_rsi"] = current_rsi

                    # --- 🛡️ ENHANCED STEP-LOCK TRAILING LOGIC ---
                    best_p = pos["best_price"]

                    # TIER 1: Break-Even Lock at +1.2% PnL (Risk-Free)
                    if floating_pnl_pct >= BREAKEVEN_TRIGGER_PNL_PCT and not pos["is_breakeven_active"]:
                        pos["stop_loss_price"] = entry_p
                        pos["is_breakeven_active"] = True
                        sl_p = entry_p
                        logger.info(f"🛡️ [STEP-LOCK TIER 1] [{symbol}] SL locked to Break-Even (${entry_p:,.4f}).")

                    # TIER 2: Lock Minimum +1.0% Profit at +2.0% PnL
                    if floating_pnl_pct >= LOCK_PROFIT_TRIGGER_PNL_PCT and not pos.get("is_profit_locked", False):
                        if pos_direction == "LONG":
                            locked_sl = entry_p * (1 + (1.0 / 100 / LEVERAGE))
                        else:  # SHORT
                            locked_sl = entry_p * (1 - (1.0 / 100 / LEVERAGE))

                        if (pos_direction == "LONG" and locked_sl > sl_p) or (pos_direction == "SHORT" and locked_sl < sl_p):
                            pos["stop_loss_price"] = locked_sl
                            sl_p = locked_sl
                            pos["is_profit_locked"] = True
                            logger.info(f"💰 [STEP-LOCK TIER 2] [{symbol}] Minimum +1.0% profit locked at SL ${sl_p:,.4f}.")

                    # TIER 3: Dynamic Trailing Stop at >= +2.5% PnL
                    if floating_pnl_pct >= MIN_RSI_EXIT_PNL_PCT:
                        if pos_direction == "LONG":
                            new_trailing_sl = best_p * (1 - (TRAILING_STOP_DIST_PCT / 100 / LEVERAGE))
                            if new_trailing_sl > sl_p:
                                pos["stop_loss_price"] = new_trailing_sl
                                sl_p = new_trailing_sl
                                logger.info(f"📈 [STEP-LOCK TIER 3] [{symbol}] Dynamic Trailing SL updated: ${sl_p:,.4f}")
                        else:  # SHORT
                            new_trailing_sl = best_p * (1 + (TRAILING_STOP_DIST_PCT / 100 / LEVERAGE))
                            if new_trailing_sl < sl_p:
                                pos["stop_loss_price"] = new_trailing_sl
                                sl_p = new_trailing_sl
                                logger.info(f"📉 [STEP-LOCK TIER 3] [{symbol}] Dynamic Trailing SL updated: ${sl_p:,.4f}")

                    # --- ⏳ TIME-BASED STAGNANCY CHECK (> 12 Hours) ---
                    entry_dt = pd.to_datetime(pos["entry_time"])
                    hours_held = (pd.Timestamp.now() - entry_dt).total_seconds() / 3600.0

                    logger.info(
                        f"📈 [SLOT ACTIVE: {symbol} {pos_direction} {LEVERAGE}x] Price: ${current_price:,.4f} | Margin: ${slot_margin_usdt:.2f} | PnL: {floating_pnl_pct:+.2f}% | Duration: {hours_held:.1f}h"
                    )

                    # --- 🔴 CHECK EXIT TRIGGERS ---
                    should_close = False
                    exit_reason = ""

                    rsi_upper = strat.get("rsi_upper", 70.0)
                    rsi_lower = strat.get("rsi_lower", 30.0)

                    # 1. Stop Loss & Trailing Hit
                    if pos_direction == "LONG" and current_price <= sl_p:
                        should_close = True
                        exit_reason = "STEP-LOCK / TRAILING SL HIT 🔴" if pos["is_breakeven_active"] else "STOP LOSS HIT 🔴"
                    elif pos_direction == "SHORT" and current_price >= sl_p:
                        should_close = True
                        exit_reason = "STEP-LOCK / TRAILING SL HIT 🔴" if pos["is_breakeven_active"] else "STOP LOSS HIT 🔴"

                    # 2. Take Profit Hit
                    elif pos_direction == "LONG" and current_price >= tp_p:
                        should_close = True
                        exit_reason = "TAKE PROFIT HIT 🟢"
                    elif pos_direction == "SHORT" and current_price <= tp_p:
                        should_close = True
                        exit_reason = "TAKE PROFIT HIT 🟢"

                    # 3. RSI Exit (Only if PnL >= +2.5%)
                    elif pos_direction == "LONG" and current_rsi > rsi_upper and floating_pnl_pct >= MIN_RSI_EXIT_PNL_PCT:
                        should_close = True
                        exit_reason = f"RSI EXIT SIGNAL ({current_rsi:.1f}) @ PnL {floating_pnl_pct:+.2f}% 🟡"
                    elif pos_direction == "SHORT" and current_rsi < rsi_lower and floating_pnl_pct >= MIN_RSI_EXIT_PNL_PCT:
                        should_close = True
                        exit_reason = f"RSI EXIT SIGNAL ({current_rsi:.1f}) @ PnL {floating_pnl_pct:+.2f}% 🟡"

                    # 4. Time-Based Stagnancy Cut (>12 Hrs & PnL between -0.5% and +0.5%)
                    elif hours_held >= MAX_HOLDING_HOURS and (-0.5 <= floating_pnl_pct <= 0.5):
                        should_close = True
                        exit_reason = f"TIME-BASED STAGNANCY CUT ({hours_held:.1f}h Held) ⏱️"

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

                        # ⛔ TRIGGER CIRCUIT BREAKER JIKA HIT SL
                        if "SL HIT" in exit_reason or "STOP LOSS" in exit_reason:
                            trigger_pair_cooldown(symbol, exit_reason)

                        del active_positions[symbol]

                except Exception as pos_err:
                    logger.error(f"❌ Error monitoring position for {symbol}: {pos_err}")

            # ------------------------------------------------------------------
            # 2. CEK PELUANG ENTRY UNTUK SLOT KOSONG (DYNAMIC 10% EQUITY SIZING)
            # ------------------------------------------------------------------
            if len(active_positions) < MAX_SLOTS:
                for symbol, strat in list(active_strategies.items()):
                    if symbol in active_positions:
                        continue

                    if len(active_positions) >= MAX_SLOTS:
                        break

                    # ⛔ COOLDOWN CHECK
                    if is_pair_in_cooldown(symbol):
                        continue

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
                            # 💡 DYNAMIC POSITION SIZING: Ambil saldo terkini dari DB & hitung 10% Equity
                            stats_summary, _ = await fetch_stats_from_db()
                            current_equity = stats_summary.get("current_balance", 1000.0)
                            
                            dynamic_margin_usdt = round(current_equity * RISK_EQUITY_PCT, 2)
                            if dynamic_margin_usdt < MIN_MARGIN_USDT:
                                dynamic_margin_usdt = MIN_MARGIN_USDT

                            effective_notional = dynamic_margin_usdt * LEVERAGE
                            amount = effective_notional / current_price

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
                                "margin_usdt": dynamic_margin_usdt,
                                "initial_stop_loss": sl_price,
                                "stop_loss_price": sl_price,
                                "take_profit_price": tp_price,
                                "floating_pnl_pct": 0.0,
                                "floating_pnl_usdt": 0.0,
                                "is_breakeven_active": False,
                                "is_profit_locked": False,
                                "entry_time": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
                            }

                            logger.info("=" * 60)
                            logger.info(f"🟢 [NEW SLOT OCCUPIED] [{symbol} {direction}] DYNAMIC ENTRY EXECUTED ({len(active_positions)}/{MAX_SLOTS} Slots)")
                            logger.info(f"   Entry Price  : ${current_price:,.4f}")
                            logger.info(f"   Equity Balance: ${current_equity:,.2f} USDT")
                            logger.info(f"   Margin / Slot: ${dynamic_margin_usdt:,.2f} USDT ({RISK_EQUITY_PCT * 100}% Equity) @ {LEVERAGE}x")
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
