"""
Cloud Trading Bot for Railway + MetaAPI (MT4/MT5)
Fixed: 'no running event loop' under Gunicorn
"""

import os
import time
import asyncio
import threading
import logging
import traceback
from datetime import datetime
from typing import Optional, Any

import numpy as np
import pandas as pd
from flask import Flask, jsonify, request, render_template_string
from flask_cors import CORS

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# ============================================================
#  DEDICATED ASYNCIO LOOP (fixes "no running event loop")
# ============================================================
_loop: Optional[asyncio.AbstractEventLoop] = None
_loop_thread: Optional[threading.Thread] = None
_loop_ready = threading.Event()


def _start_background_loop():
    """Start one permanent event loop in a background thread."""
    global _loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    _loop = loop
    _loop_ready.set()
    logger.info("Asyncio event loop started")
    loop.run_forever()


def ensure_loop():
    """Make sure the background loop is running."""
    global _loop_thread
    if _loop_thread is None or not _loop_thread.is_alive():
        _loop_ready.clear()
        _loop_thread = threading.Thread(target=_start_background_loop, daemon=True)
        _loop_thread.start()
        if not _loop_ready.wait(timeout=10):
            raise RuntimeError("Failed to start asyncio event loop")
    return _loop


def run_async(coro, timeout=300):
    """
    Run a coroutine on the background loop from any thread.
    This is the key fix for MetaAPI + Gunicorn.
    """
    loop = ensure_loop()
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    return future.result(timeout=timeout)


# ============================================================
#  GLOBAL STATE
# ============================================================
bot_state = {
    "connected": False,
    "connecting": False,
    "platform": "",
    "account_info": None,
    "connection_error": None,
    "running": False,
    "config": {
        "symbols": ["EURUSD", "GBPUSD", "USDJPY"],
        "timeframe": "1h",
        "max_trades": 3,
        "risk_percent": 1.0,
        "max_daily_loss_percent": 5.0,
        "stop_loss_pips": 50,
        "take_profit_pips": 100,
        "trailing_stop_pips": 20,
        "min_signal_strength": 65,
    },
    "positions": [],
    "daily_pnl": 0.0,
    "unrealized_pnl": 0.0,
    "balance": 0.0,
    "equity": 0.0,
    "signals_log": [],
    "trade_log": [],
    "cycle_count": 0,
    "uptime": "0m",
    "last_update": None,
    "error_message": None,
}
state_lock = threading.Lock()

meta_api = None
mt_account = None
connection = None  # MetaAPI RPC connection
trading_thread = None
start_time = None


# ============================================================
#  CONNECT TO MT4/MT5 VIA METAAPI
# ============================================================
async def _async_connect(token: str, login: str, password: str, server: str, platform: str):
    global meta_api, mt_account, connection

    from metaapi_cloud_sdk import MetaApi

    api = MetaApi(token.strip())
    meta_api = api

    account_api = api.metatrader_account_api
    accounts = await account_api.get_accounts_with_infinite_scroll_pagination()

    existing = None
    for acc in accounts:
        try:
            if str(getattr(acc, "login", "")) == str(login).strip():
                existing = acc
                break
        except Exception:
            continue

    if existing:
        account = existing
        logger.info(f"Reusing existing MetaAPI account: {account.id}")
        # Update password if needed
        try:
            await account.update({
                "password": password.strip(),
                "server": server.strip(),
            })
        except Exception as e:
            logger.warning(f"Account update skipped: {e}")
    else:
        logger.info("Creating new MetaAPI cloud account...")
        account = await account_api.create_account({
            "name": f"Bot-{login}",
            "type": "cloud",
            "login": str(login).strip(),
            "password": str(password).strip(),
            "server": str(server).strip(),
            "platform": platform.strip().lower(),  # mt4 or mt5
            "magic": 123456,
            "quoteStreamingIntervalInSeconds": 2.5,
            "reliability": "regular",
        })

    # Deploy
    state = getattr(account, "state", None)
    logger.info(f"Account state: {state}")
    if state in ("UNDEPLOYED", "DEPLOY_FAILED", None) or str(state) == "UNDEPLOYED":
        logger.info("Deploying account (can take 1–3 minutes)...")
        await account.deploy()

    logger.info("Waiting until connected to broker...")
    await account.wait_connected()

    # RPC connection
    conn = account.get_rpc_connection()
    await conn.connect()
    await conn.wait_synchronized()

    info = await conn.get_account_information()
    mt_account = account
    connection = conn

    return {
        "login": info.get("login", login),
        "server": info.get("server", server),
        "platform": platform.upper(),
        "balance": float(info.get("balance", 0) or 0),
        "equity": float(info.get("equity", 0) or 0),
        "leverage": info.get("leverage", 100),
        "currency": info.get("currency", "USD"),
        "broker": info.get("broker", ""),
        "name": info.get("name", ""),
    }


def connect_to_mt(token, login, password, server, platform):
    with state_lock:
        bot_state["connecting"] = True
        bot_state["connection_error"] = None
        bot_state["error_message"] = None

    try:
        ensure_loop()
        info = run_async(
            _async_connect(token, login, password, server, platform),
            timeout=360,
        )
        with state_lock:
            bot_state["connected"] = True
            bot_state["connecting"] = False
            bot_state["platform"] = platform.upper()
            bot_state["account_info"] = info
            bot_state["balance"] = info["balance"]
            bot_state["equity"] = info["equity"]
            bot_state["connection_error"] = None
        logger.info(f"Connected OK — balance {info['balance']}")
        return {"success": True, "account_info": info}
    except Exception as e:
        err = str(e)
        logger.error(f"Connect failed: {err}\n{traceback.format_exc()}")
        with state_lock:
            bot_state["connected"] = False
            bot_state["connecting"] = False
            bot_state["connection_error"] = err
            bot_state["error_message"] = err
        return {"success": False, "error": err}


# ============================================================
#  INDICATORS + STRATEGY
# ============================================================
def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or len(df) < 50:
        return df
    df = df.copy()
    c, h, l = df["Close"], df["High"], df["Low"]

    df["EMA9"] = c.ewm(span=9, adjust=False).mean()
    df["EMA21"] = c.ewm(span=21, adjust=False).mean()
    df["EMA50"] = c.ewm(span=50, adjust=False).mean()
    df["EMA200"] = c.ewm(span=200, adjust=False).mean()

    ema12 = c.ewm(span=12, adjust=False).mean()
    ema26 = c.ewm(span=26, adjust=False).mean()
    df["MACD"] = ema12 - ema26
    df["MACD_sig"] = df["MACD"].ewm(span=9, adjust=False).mean()

    delta = c.diff()
    gain = delta.where(delta > 0, 0).ewm(span=14, adjust=False).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(span=14, adjust=False).mean()
    df["RSI"] = 100 - (100 / (1 + gain / (loss + 1e-10)))

    sma20 = c.rolling(20).mean()
    std20 = c.rolling(20).std()
    df["BB_upper"] = sma20 + 2 * std20
    df["BB_lower"] = sma20 - 2 * std20

    low14, high14 = l.rolling(14).min(), h.rolling(14).max()
    df["Stoch_K"] = 100 * (c - low14) / (high14 - low14 + 1e-10)
    df["Stoch_D"] = df["Stoch_K"].rolling(3).mean()

    tr = pd.concat([h - l, (h - c.shift(1)).abs(), (l - c.shift(1)).abs()], axis=1).max(axis=1)
    df["ATR"] = tr.ewm(span=14, adjust=False).mean()
    return df


def analyze_market(df: pd.DataFrame, symbol: str, config: dict) -> Optional[dict]:
    df = calculate_indicators(df)
    if df is None or len(df) < 50:
        return None

    last, prev = df.iloc[-2], df.iloc[-3]
    score, reasons = 0.0, []

    if last["EMA9"] > last["EMA21"] > last["EMA50"]:
        score += 0.30
        reasons.append("EMA bullish (9>21>50)")
    elif last["EMA9"] < last["EMA21"] < last["EMA50"]:
        score -= 0.30
        reasons.append("EMA bearish (9<21<50)")

    if last["Close"] > last["EMA200"]:
        score += 0.20
        reasons.append("Above EMA200")
    else:
        score -= 0.20
        reasons.append("Below EMA200")

    if last["MACD"] > last["MACD_sig"] and prev["MACD"] <= prev["MACD_sig"]:
        score += 0.25
        reasons.append("MACD bullish cross")
    elif last["MACD"] < last["MACD_sig"] and prev["MACD"] >= prev["MACD_sig"]:
        score -= 0.25
        reasons.append("MACD bearish cross")

    rsi = float(last["RSI"])
    if rsi < 30:
        score += 0.25
        reasons.append(f"RSI oversold ({rsi:.0f})")
    elif rsi > 70:
        score -= 0.25
        reasons.append(f"RSI overbought ({rsi:.0f})")

    if last["Close"] < last["BB_lower"]:
        score += 0.20
        reasons.append("Below lower BB")
    elif last["Close"] > last["BB_upper"]:
        score -= 0.20
        reasons.append("Above upper BB")

    if last["Stoch_K"] < 20 and last["Stoch_K"] > last["Stoch_D"]:
        score += 0.15
        reasons.append("Stoch oversold turn up")
    elif last["Stoch_K"] > 80 and last["Stoch_K"] < last["Stoch_D"]:
        score -= 0.15
        reasons.append("Stoch overbought turn down")

    strength = min(abs(score), 1.0)
    if strength < config.get("min_signal_strength", 65) / 100.0:
        return None

    direction = "buy" if score > 0 else "sell"
    price = float(last["Close"])
    pip = 0.01 if "JPY" in symbol else (0.01 if symbol.startswith("XAU") else 0.0001)
    # Gold often uses 0.1 pip scale depending on broker — keep simple
    if "XAU" in symbol:
        pip = 0.1

    sl_dist = config.get("stop_loss_pips", 50) * pip
    tp_dist = config.get("take_profit_pips", 100) * pip

    if direction == "buy":
        sl, tp = round(price - sl_dist, 5), round(price + tp_dist, 5)
    else:
        sl, tp = round(price + sl_dist, 5), round(price - tp_dist, 5)

    return {
        "symbol": symbol,
        "direction": direction,
        "strength": int(strength * 100),
        "price": price,
        "sl": sl,
        "tp": tp,
        "reasons": reasons,
        "rsi": round(rsi, 1),
    }


# ============================================================
#  METAAPI HELPERS (async)
# ============================================================
async def _get_candles(symbol: str, timeframe: str, count: int = 250):
    """Fetch candles — MetaAPI method names vary by SDK version."""
    # Preferred: get_historical_candles
    try:
        candles = await connection.get_historical_candles(symbol, timeframe, None, count)
        if candles:
            return candles
    except Exception as e:
        logger.debug(f"get_historical_candles: {e}")

    try:
        candles = await connection.get_candles(symbol, timeframe, count)
        if candles:
            return candles
    except Exception as e:
        logger.debug(f"get_candles: {e}")

    # Fallback: rates via market data
    raise RuntimeError(f"Could not fetch candles for {symbol}")


def candles_to_df(candles) -> Optional[pd.DataFrame]:
    if not candles:
        return None
    rows = []
    for c in candles:
        if isinstance(c, dict):
            rows.append({
                "Open": float(c.get("open", c.get("Open", 0))),
                "High": float(c.get("high", c.get("High", 0))),
                "Low": float(c.get("low", c.get("Low", 0))),
                "Close": float(c.get("close", c.get("Close", 0))),
                "Volume": float(c.get("tickVolume", c.get("volume", c.get("Volume", 1)))),
            })
    if not rows:
        return None
    return pd.DataFrame(rows)


async def _place_order(direction, symbol, lots, sl, tp):
    opts = {"comment": "RailwayBot", "magic": 123456}
    if direction == "buy":
        return await connection.create_market_buy_order(symbol, lots, sl, tp, opts)
    return await connection.create_market_sell_order(symbol, lots, sl, tp, opts)


# ============================================================
#  TRADING WORKER
# ============================================================
def trading_bot_worker():
    global start_time
    start_time = datetime.now()
    logger.info("Trading worker started")

    while True:
        with state_lock:
            running = bot_state["running"]
            connected = bot_state["connected"]
            config = dict(bot_state["config"])

        if not running or not connected or connection is None:
            time.sleep(3)
            continue

        try:
            with state_lock:
                bot_state["cycle_count"] += 1
                cycle = bot_state["cycle_count"]
            logger.info(f"Cycle #{cycle}")

            # Account info
            try:
                info = run_async(connection.get_account_information(), timeout=60)
                with state_lock:
                    bot_state["balance"] = float(info.get("balance", 0) or 0)
                    bot_state["equity"] = float(info.get("equity", 0) or 0)
            except Exception as e:
                logger.warning(f"Account refresh: {e}")

            # Positions
            try:
                raw = run_async(connection.get_positions(), timeout=60) or []
            except Exception as e:
                logger.warning(f"Positions: {e}")
                raw = []

            positions, unrealized = [], 0.0
            for p in raw:
                typ = str(p.get("type", "")).lower()
                if "buy" in typ:
                    side = "buy"
                elif "sell" in typ:
                    side = "sell"
                else:
                    side = typ
                pos = {
                    "id": p.get("id"),
                    "symbol": p.get("symbol"),
                    "type": side,
                    "volume": float(p.get("volume", 0) or 0),
                    "open_price": float(p.get("openPrice", p.get("price", 0)) or 0),
                    "current_price": float(p.get("currentPrice", 0) or 0),
                    "sl": float(p.get("stopLoss", 0) or 0),
                    "tp": float(p.get("takeProfit", 0) or 0),
                    "profit": float(p.get("profit", 0) or 0) + float(p.get("swap", 0) or 0),
                    "magic": p.get("magic", 0),
                }
                positions.append(pos)
                unrealized += pos["profit"]

            with state_lock:
                bot_state["positions"] = positions
                bot_state["unrealized_pnl"] = round(unrealized, 2)
                balance = bot_state["balance"]

            # Daily loss guard
            max_loss = balance * (config.get("max_daily_loss_percent", 5) / 100.0)
            if max_loss > 0 and unrealized < -max_loss:
                with state_lock:
                    bot_state["error_message"] = f"Max daily loss hit (${unrealized:.2f})"
                time.sleep(30)
                continue

            max_trades = int(config.get("max_trades", 3))
            active = len(positions)

            for sym in config.get("symbols", []):
                if not bot_state["running"] or active >= max_trades:
                    break
                if any(p["symbol"] == sym for p in positions):
                    continue

                try:
                    tf = config.get("timeframe", "1h")
                    candles = run_async(_get_candles(sym, tf, 250), timeout=90)
                    df = candles_to_df(candles)
                    if df is None or len(df) < 50:
                        continue

                    sig = analyze_market(df, sym, config)
                    if not sig:
                        continue

                    text = f"{sig['direction'].upper()} {sig['symbol']} | {sig['strength']}% | RSI {sig['rsi']}"
                    with state_lock:
                        bot_state["signals_log"].insert(0, {
                            "time": datetime.now().strftime("%H:%M:%S"),
                            "text": text,
                            "reasons": sig["reasons"],
                        })
                        bot_state["signals_log"] = bot_state["signals_log"][:40]

                    risk_pct = float(config.get("risk_percent", 1.0)) / 100.0
                    risk_amount = balance * risk_pct
                    sl_pips = float(config.get("stop_loss_pips", 50))
                    pip_value = 10.0  # approx $ per pip per lot
                    lots = risk_amount / (sl_pips * pip_value) if sl_pips * pip_value > 0 else 0.01
                    lots = max(0.01, min(round(lots, 2), 2.0))

                    result = run_async(
                        _place_order(sig["direction"], sym, lots, sig["sl"], sig["tp"]),
                        timeout=90,
                    )
                    logger.info(f"Order result: {result}")

                    with state_lock:
                        bot_state["trade_log"].insert(0, {
                            "time": datetime.now().strftime("%H:%M:%S"),
                            "symbol": sym,
                            "type": sig["direction"].upper(),
                            "volume": lots,
                            "price": sig["price"],
                            "sl": sig["sl"],
                            "tp": sig["tp"],
                        })
                        bot_state["trade_log"] = bot_state["trade_log"][:40]
                    active += 1

                except Exception as e:
                    logger.error(f"Symbol {sym}: {e}")

            # Simple trailing stop
            trail = float(config.get("trailing_stop_pips", 20))
            for p in positions:
                try:
                    pip = 0.01 if "JPY" in (p["symbol"] or "") else 0.0001
                    if "XAU" in (p["symbol"] or ""):
                        pip = 0.1
                    dist = trail * pip
                    new_sl = p["sl"]
                    if p["type"] == "buy":
                        candidate = round(p["current_price"] - dist, 5)
                        if candidate > p["sl"] and candidate > p["open_price"]:
                            new_sl = candidate
                    elif p["type"] == "sell":
                        candidate = round(p["current_price"] + dist, 5)
                        if (p["sl"] == 0 or candidate < p["sl"]) and candidate < p["open_price"]:
                            new_sl = candidate
                    if new_sl != p["sl"] and new_sl > 0 and p.get("id"):
                        run_async(
                            connection.modify_position(p["id"], stop_loss=new_sl, take_profit=p["tp"] or None),
                            timeout=60,
                        )
                except Exception as e:
                    logger.debug(f"Trail skip: {e}")

            delta = datetime.now() - start_time
            with state_lock:
                bot_state["uptime"] = f"{int(delta.total_seconds()//3600)}h {int((delta.total_seconds()%3600)//60)}m"
                bot_state["last_update"] = datetime.now().isoformat()
                bot_state["error_message"] = None

            time.sleep(30)

        except Exception as e:
            logger.error(f"Worker error: {e}\n{traceback.format_exc()}")
            with state_lock:
                bot_state["error_message"] = str(e)[:200]
            time.sleep(15)


# ============================================================
#  API ROUTES
# ============================================================
@app.route("/api/connect", methods=["POST"])
def api_connect():
    data = request.get_json(silent=True) or {}
    for f in ("metaapi_token", "login", "password", "server", "platform"):
        if not str(data.get(f, "")).strip():
            return jsonify({"error": f"Missing: {f}"}), 400

    if bot_state.get("connected"):
        return jsonify({"error": "Already connected. Disconnect first."}), 400
    if bot_state.get("connecting"):
        return jsonify({"status": "connecting"}), 200

    def job():
        connect_to_mt(
            data["metaapi_token"], data["login"], data["password"],
            data["server"], data["platform"],
        )

    threading.Thread(target=job, daemon=True).start()
    return jsonify({"status": "connecting", "message": "Connecting… can take 1–3 minutes"})


@app.route("/api/disconnect", methods=["POST"])
def api_disconnect():
    global connection, mt_account
    with state_lock:
        bot_state["running"] = False
        bot_state["connected"] = False
        bot_state["connecting"] = False
        bot_state["account_info"] = None
    connection = None
    mt_account = None
    return jsonify({"status": "disconnected"})


@app.route("/api/start", methods=["POST"])
def api_start():
    global trading_thread
    if not bot_state.get("connected"):
        return jsonify({"error": "Connect first"}), 400

    data = request.get_json(silent=True) or {}
    with state_lock:
        cfg = bot_state["config"]
        if data.get("symbols"):
            cfg["symbols"] = [s.strip().upper() for s in data["symbols"] if str(s).strip()]
        for key, cast in [
            ("timeframe", str), ("max_trades", int), ("risk_percent", float),
            ("max_daily_loss_percent", float), ("stop_loss_pips", int),
            ("take_profit_pips", int), ("trailing_stop_pips", int),
            ("min_signal_strength", int),
        ]:
            if key in data and data[key] is not None:
                try:
                    cfg[key] = cast(data[key])
                except Exception:
                    pass
        bot_state["running"] = True
        bot_state["error_message"] = None

    if trading_thread is None or not trading_thread.is_alive():
        trading_thread = threading.Thread(target=trading_bot_worker, daemon=True)
        trading_thread.start()

    return jsonify({"status": "started", "config": bot_state["config"]})


@app.route("/api/stop", methods=["POST"])
def api_stop():
    with state_lock:
        bot_state["running"] = False
    return jsonify({"status": "stopped"})


@app.route("/api/close_all", methods=["POST"])
def api_close_all():
    if not bot_state.get("connected") or connection is None:
        return jsonify({"error": "Not connected"}), 400
    try:
        positions = run_async(connection.get_positions(), timeout=60) or []
        closed = 0
        for p in positions:
            try:
                run_async(connection.close_position(p["id"]), timeout=60)
                closed += 1
            except Exception as e:
                logger.error(f"Close failed: {e}")
        return jsonify({"status": "ok", "closed": closed})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/status")
def api_status():
    with state_lock:
        # return a plain copy safe for JSON
        return jsonify({
            "connected": bot_state["connected"],
            "connecting": bot_state["connecting"],
            "running": bot_state["running"],
            "platform": bot_state["platform"],
            "account": bot_state["account_info"],
            "account_info": bot_state["account_info"],
            "config": bot_state["config"],
            "positions": bot_state["positions"],
            "daily_pnl": bot_state["daily_pnl"],
            "unrealized_pnl": bot_state["unrealized_pnl"],
            "balance": bot_state["balance"],
            "equity": bot_state["equity"],
            "signals_log": bot_state["signals_log"][:20],
            "trade_log": bot_state["trade_log"][:20],
            "cycle_count": bot_state["cycle_count"],
            "uptime": bot_state["uptime"],
            "last_update": bot_state["last_update"],
            "error_message": bot_state["error_message"],
            "connection_error": bot_state["connection_error"],
        })


# ============================================================
#  UI (same dashboard)
# ============================================================
DASHBOARD_HTML = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<title>Cloud Trading Bot</title>
<style>
:root{--bg:#090d16;--card:#121826;--border:#1f2b3e;--green:#00e676;--red:#ff5252;--blue:#2979ff;--yellow:#ffd740;--text:#e8ecf4;--muted:#64748b;--input-bg:#0b111e}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:var(--bg);color:var(--text);padding-bottom:50px}
.hdr{background:#0e1626;padding:14px 16px;border-bottom:1px solid var(--border);position:sticky;top:0;z-index:50}
.hdr-row{display:flex;justify-content:space-between;align-items:center}
.hdr h1{font-size:1.1em;color:var(--green)}
.badge{display:inline-flex;align-items:center;gap:6px;font-size:.75em;padding:4px 10px;border-radius:20px;font-weight:600}
.badge-on{background:rgba(0,230,118,.15);color:var(--green)}
.badge-off{background:rgba(255,82,82,.15);color:var(--red)}
.badge-wait{background:rgba(255,215,64,.15);color:var(--yellow)}
.dot{width:8px;height:8px;border-radius:50%}
.dot-g{background:var(--green);animation:pulse 1.5s infinite}
.dot-r{background:var(--red)}
.dot-y{background:var(--yellow);animation:pulse 1.5s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
.wrap{padding:12px;max-width:500px;margin:0 auto}
.card{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:14px;margin-bottom:12px}
.card-t{font-size:.75em;color:var(--muted);text-transform:uppercase;letter-spacing:1px;margin-bottom:10px;font-weight:700}
.pnl-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px}
.pnl-box{background:#0a0e1a;border-radius:8px;padding:12px;text-align:center}
.pnl-lbl{font-size:.7em;color:var(--muted);margin-bottom:4px}
.pnl-val{font-size:1.3em;font-weight:700}
.profit{color:var(--green)}.loss{color:var(--red)}.neutral{color:var(--text)}
input,select{width:100%;padding:11px 12px;background:var(--input-bg);border:1px solid var(--border);border-radius:8px;color:var(--text);font-size:.9em;margin-bottom:10px;outline:none}
label{font-size:.78em;color:var(--muted);margin-bottom:4px;display:block}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:8px}
.grid3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px}
.btn{padding:13px;border:none;border-radius:10px;font-size:.95em;font-weight:700;cursor:pointer;width:100%;text-align:center}
.btn-green{background:var(--green);color:#000}
.btn-red{background:var(--red);color:#fff}
.btn-blue{background:var(--blue);color:#fff}
.btn-yellow{background:var(--yellow);color:#000}
.btn:disabled{opacity:.4}
.chip-wrap{display:flex;flex-wrap:wrap;gap:6px;margin-bottom:10px}
.chip{padding:6px 12px;border-radius:20px;font-size:.8em;font-weight:600;border:1px solid var(--border);background:var(--input-bg);color:var(--muted)}
.chip.active{background:var(--blue);color:#fff;border-color:var(--blue)}
.tabs{display:flex;margin-bottom:12px;background:var(--card);border-radius:10px;overflow:hidden;border:1px solid var(--border)}
.tab{flex:1;padding:12px;text-align:center;font-size:.82em;font-weight:600;color:var(--muted)}
.tab.active{background:var(--blue);color:#fff}
.pos-item{display:flex;justify-content:space-between;padding:10px 12px;background:#090d18;border-radius:8px;margin-bottom:6px;font-size:.85em}
.pos-buy{color:var(--green);font-weight:bold}.pos-sell{color:var(--red);font-weight:bold}
.log{max-height:180px;overflow-y:auto;font-family:monospace;font-size:.75em;line-height:1.6}
.err{background:rgba(255,82,82,.15);border:1px solid var(--red);border-radius:8px;padding:10px;margin-bottom:10px;font-size:.85em;color:var(--red)}
.info{background:rgba(41,121,255,.12);border:1px solid var(--blue);border-radius:8px;padding:10px;margin-bottom:10px;font-size:.8em;color:#90caf9}
</style>
</head>
<body>
<div class="hdr"><div class="hdr-row">
  <h1>Cloud Trading Bot</h1>
  <div class="badge" id="statusBadge"><div class="dot" id="statusDot"></div><span id="statusText">Offline</span></div>
</div></div>
<div class="wrap">
  <div class="err" id="errBanner" style="display:none"></div>
  <div class="info" id="infoBanner" style="display:none"></div>
  <div class="tabs">
    <div class="tab active" onclick="setTab('connect')">Connect</div>
    <div class="tab" onclick="setTab('config')">Strategy</div>
    <div class="tab" onclick="setTab('live')">Live Trade</div>
  </div>

  <div id="sec-connect">
    <div class="card">
      <div class="card-t">1. MetaAPI Token</div>
      <label>From app.metaapi.cloud → API Access → Tokens</label>
      <input type="password" id="token" placeholder="Paste MetaAPI token" autocomplete="off">
    </div>
    <div class="card">
      <div class="card-t">2. Broker Credentials</div>
      <label>Platform</label>
      <select id="platform"><option value="mt5">MetaTrader 5</option><option value="mt4">MetaTrader 4</option></select>
      <label>Account Login</label>
      <input type="text" id="login" placeholder="Account number" inputmode="numeric">
      <label>Password</label>
      <input type="password" id="password" placeholder="Investor or trading password">
      <label>Server</label>
      <input type="text" id="server" placeholder="e.g. Exness-MT5Trial9">
      <button class="btn btn-blue" id="btnConn" onclick="connect()">Connect to Broker</button>
      <button class="btn btn-red" id="btnDisconn" onclick="disconnect()" style="display:none;margin-top:8px">Disconnect</button>
    </div>
  </div>

  <div id="sec-config" style="display:none">
    <div class="card"><div class="card-t">Pairs</div><div class="chip-wrap" id="pairs"></div></div>
    <div class="card">
      <div class="card-t">Settings</div>
      <label>Timeframe</label>
      <select id="tf">
        <option value="15m">M15</option><option value="30m">M30</option>
        <option value="1h" selected>H1</option><option value="4h">H4</option>
      </select>
      <div class="grid2">
        <div><label>Max Trades</label><input type="number" id="maxTrades" value="3"></div>
        <div><label>Risk %</label><input type="number" id="riskPct" value="1" step="0.1"></div>
      </div>
      <div class="grid3">
        <div><label>SL pips</label><input type="number" id="slPips" value="50"></div>
        <div><label>TP pips</label><input type="number" id="tpPips" value="100"></div>
        <div><label>Trail</label><input type="number" id="trailPips" value="20"></div>
      </div>
      <div class="grid2">
        <div><label>Min signal %</label><input type="number" id="minStr" value="65"></div>
        <div><label>Max daily loss %</label><input type="number" id="maxLoss" value="5" step="0.5"></div>
      </div>
    </div>
    <button class="btn btn-green" id="btnRun" onclick="startBot()" disabled>Start Automated Trading</button>
    <button class="btn btn-red" id="btnStop" onclick="stopBot()" style="margin-top:8px;display:none">Stop Bot</button>
  </div>

  <div id="sec-live" style="display:none">
    <div class="card"><div class="card-t">Overview</div>
      <div class="pnl-grid">
        <div class="pnl-box"><div class="pnl-lbl">Unrealized</div><div class="pnl-val neutral" id="unrealPnl">$0.00</div></div>
        <div class="pnl-box"><div class="pnl-lbl">Balance</div><div class="pnl-val neutral" id="balance">$0.00</div></div>
      </div>
    </div>
    <div class="card">
      <div class="card-t">Positions (<span id="posCount">0</span>)</div>
      <div id="posList" style="color:var(--muted);text-align:center;padding:10px">None</div>
      <button class="btn btn-yellow" style="margin-top:8px" onclick="closeAll()">Close All</button>
    </div>
    <div class="card"><div class="card-t">Signals</div><div class="log" id="sigLog">Waiting...</div></div>
  </div>
</div>
<script>
const PAIRS=['EURUSD','GBPUSD','USDJPY','AUDUSD','USDCAD','NZDUSD','EURJPY','GBPJPY','XAUUSD'];
let selPairs=['EURUSD','GBPUSD','USDJPY'];
function initPairs(){document.getElementById('pairs').innerHTML=PAIRS.map(p=>`<div class="chip ${selPairs.includes(p)?'active':''}" onclick="togglePair('${p}',this)">${p}</div>`).join('')}
function togglePair(p,el){if(selPairs.includes(p)){selPairs=selPairs.filter(x=>x!==p);el.classList.remove('active')}else{selPairs.push(p);el.classList.add('active')}}
initPairs();
function setTab(t){['connect','config','live'].forEach(x=>{document.getElementById('sec-'+x).style.display=x===t?'block':'none'});document.querySelectorAll('.tab').forEach((el,i)=>el.classList.toggle('active',['connect','config','live'][i]===t))}
async function connect(){
  const body={metaapi_token:token.value.trim(),login:login.value.trim(),password:password.value.trim(),server:server.value.trim(),platform:platform.value};
  if(!body.metaapi_token||!body.login||!body.password||!body.server){alert('Fill all fields');return}
  btnConn.disabled=true;btnConn.textContent='Connecting (1–3 min)...';
  infoBanner.style.display='block';infoBanner.textContent='Connecting to broker via MetaAPI. First deploy can take 1–3 minutes. Keep this page open.';
  await fetch('/api/connect',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
}
async function disconnect(){await fetch('/api/disconnect',{method:'POST'})}
async function startBot(){
  const cfg={symbols:selPairs,timeframe:tf.value,max_trades:+maxTrades.value,risk_percent:+riskPct.value,stop_loss_pips:+slPips.value,take_profit_pips:+tpPips.value,trailing_stop_pips:+trailPips.value,min_signal_strength:+minStr.value,max_daily_loss_percent:+maxLoss.value};
  const r=await fetch('/api/start',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(cfg)});
  const d=await r.json();if(d.error)alert(d.error);else setTab('live');
}
async function stopBot(){await fetch('/api/stop',{method:'POST'})}
async function closeAll(){if(confirm('Close all positions?'))await fetch('/api/close_all',{method:'POST'})}
async function update(){
  try{
    const d=await(await fetch('/api/status')).json();
    const badge=statusBadge,dot=statusDot,txt=statusText;
    if(d.connecting){badge.className='badge badge-wait';dot.className='dot dot-y';txt.textContent='Connecting'}
    else if(d.connected&&d.running){badge.className='badge badge-on';dot.className='dot dot-g';txt.textContent='Trading'}
    else if(d.connected){badge.className='badge badge-wait';dot.className='dot dot-y';txt.textContent='Ready'}
    else{badge.className='badge badge-off';dot.className='dot dot-r';txt.textContent='Offline'}
    btnConn.style.display=d.connected?'none':'block';
    if(!d.connecting){btnConn.disabled=false;btnConn.textContent='Connect to Broker'}
    btnDisconn.style.display=d.connected?'block':'none';
    btnRun.style.display=d.running?'none':'block';btnRun.disabled=!d.connected;
    btnStop.style.display=d.running?'block':'none';
    if(d.connecting){infoBanner.style.display='block'} else if(d.connected){infoBanner.style.display='none'}
    balance.textContent='$'+(d.balance||0).toFixed(2);
    const u=d.unrealized_pnl||0;unrealPnl.textContent=(u>=0?'+$':'-$')+Math.abs(u).toFixed(2);
    unrealPnl.className='pnl-val '+(u>0?'profit':u<0?'loss':'neutral');
    posCount.textContent=(d.positions||[]).length;
    posList.innerHTML=(d.positions&&d.positions.length)?d.positions.map(p=>`<div class="pos-item"><div><span class="pos-${p.type}">${(p.type||'').toUpperCase()}</span> ${p.volume} ${p.symbol}</div><div class="${p.profit>=0?'profit':'loss'}">$${(p.profit||0).toFixed(2)}</div></div>`).join(''):'<div style="color:var(--muted);text-align:center">None</div>';
    if(d.signals_log&&d.signals_log.length)sigLog.innerHTML=d.signals_log.map(s=>`<div>[${s.time}] ${s.text}</div>`).join('');
    const err=d.error_message||d.connection_error;
    if(err){errBanner.style.display='block';errBanner.textContent=err}else{errBanner.style.display='none'}
  }catch(e){}
}
setInterval(update,3000);update();
</script>
</body>
</html>
"""


@app.route("/")
def index():
    return render_template_string(DASHBOARD_HTML)


# Start loop when app loads (Gunicorn import)
try:
    ensure_loop()
except Exception as e:
    logger.error(f"Loop bootstrap: {e}")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
