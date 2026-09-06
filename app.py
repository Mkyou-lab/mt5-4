"""
🤖 AUTO TRADING BOT — Complete Web App & Engine for Railway
Works with MT4 & MT5 via MetaAPI Cloud.
"""

import os
import time
import asyncio
import threading
import logging
import traceback
from datetime import datetime
from typing import Optional, List, Dict

import numpy as np
import pandas as pd
from flask import Flask, jsonify, request, render_template_string
from flask_cors import CORS

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

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
        "max_spread_pips": 5,
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

metaapi_instance = None
mt_account = None
rpc_api = None
trading_thread = None
bot_loop = None


# ============================================================
#  META API CONNECTION HANDLER
# ============================================================
def connect_to_mt(metaapi_token: str, login: str, password: str, server: str, platform: str) -> dict:
    global metaapi_instance, mt_account, rpc_api, bot_loop

    result = {"success": False, "error": None, "account_info": None}

    try:
        from metaapi_cloud_sdk import MetaApi

        with state_lock:
            bot_state["connecting"] = True
            bot_state["connection_error"] = None

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        bot_loop = loop

        api = MetaApi(metaapi_token.strip())
        metaapi_instance = api

        accounts = loop.run_until_complete(api.metatrader_account_api.get_accounts())
        existing = None
        for acc in accounts:
            if str(acc.login) == str(login.strip()) and acc.type == "cloud":
                existing = acc
                break

        if existing:
            account = existing
        else:
            account = loop.run_until_complete(
                api.metatrader_account_api.create_account({
                    "name": f"TradingBot-{login}",
                    "type": "cloud",
                    "login": str(login).strip(),
                    "password": str(password).strip(),
                    "server": str(server).strip(),
                    "platform": platform.strip().lower(),
                    "magic": 123456,
                    "quoteStreamingIntervalInSeconds": 2.5,
                })
            )

        if account.state not in ["DEPLOYED", "CONNECTED"]:
            loop.run_until_complete(account.deploy())

        loop.run_until_complete(account.wait_connected(timeout=300))

        rpc = account.get_rpc_api()
        loop.run_until_complete(rpc.connect())
        info = loop.run_until_complete(rpc.get_account_information())

        mt_account = account
        rpc_api = rpc

        result["success"] = True
        result["account_info"] = {
            "login": info.get("login", login),
            "server": info.get("server", server),
            "platform": platform.upper(),
            "balance": info.get("balance", 0.0),
            "equity": info.get("equity", 0.0),
            "leverage": info.get("leverage", 100),
            "currency": info.get("currency", "USD"),
        }

    except Exception as e:
        result["error"] = str(e)
        logger.error(f"Connection failed: {e}")

    finally:
        with state_lock:
            bot_state["connecting"] = False
            if result["success"]:
                bot_state["connected"] = True
                bot_state["platform"] = platform.upper()
                bot_state["account_info"] = result["account_info"]
                bot_state["balance"] = result["account_info"]["balance"]
                bot_state["equity"] = result["account_info"]["equity"]
                bot_state["connection_error"] = None
            else:
                bot_state["connected"] = False
                bot_state["connection_error"] = result["error"]

    return result


# ============================================================
#  TECHNICAL INDICATORS
# ============================================================
def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or len(df) < 50:
        return df

    df = df.copy()
    c = df["Close"]
    h = df["High"]
    l = df["Low"]
    o = df["Open"]
    v = df.get("Volume", pd.Series(1, index=df.index))

    # Trend Indicators
    df["EMA9"] = c.ewm(span=9, adjust=False).mean()
    df["EMA21"] = c.ewm(span=21, adjust=False).mean()
    df["EMA50"] = c.ewm(span=50, adjust=False).mean()
    df["EMA200"] = c.ewm(span=200, adjust=False).mean()

    # MACD
    ema12 = c.ewm(span=12, adjust=False).mean()
    ema26 = c.ewm(span=26, adjust=False).mean()
    df["MACD"] = ema12 - ema26
    df["MACD_sig"] = df["MACD"].ewm(span=9, adjust=False).mean()

    # RSI
    delta = c.diff()
    gain = delta.where(delta > 0, 0).ewm(span=14, adjust=False).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(span=14, adjust=False).mean()
    rs = gain / (loss + 1e-10)
    df["RSI"] = 100 - (100 / (1 + rs))

    # Bollinger Bands
    sma20 = c.rolling(20).mean()
    std20 = c.rolling(20).std()
    df["BB_upper"] = sma20 + 2 * std20
    df["BB_lower"] = sma20 - 2 * std20

    # ATR
    tr = pd.concat([h - l, (h - c.shift(1)).abs(), (l - c.shift(1)).abs()], axis=1).max(axis=1)
    df["ATR"] = tr.ewm(span=14, adjust=False).mean()

    # Stochastic
    low14 = l.rolling(14).min()
    high14 = h.rolling(14).max()
    df["Stoch_K"] = 100 * (c - low14) / (high14 - low14 + 1e-10)
    df["Stoch_D"] = df["Stoch_K"].rolling(3).mean()

    return df


def analyze_market(df: pd.DataFrame, symbol: str, config: dict) -> Optional[dict]:
    df = calculate_indicators(df)
    if df is None or len(df) < 50:
        return None

    last = df.iloc[-2]
    prev = df.iloc[-3]
    score = 0.0
    reasons = []

    # Trend Signals
    if last["EMA9"] > last["EMA21"] > last["EMA50"]:
        score += 0.3
        reasons.append("EMA Bullish Alignment (9>21>50)")
    elif last["EMA9"] < last["EMA21"] < last["EMA50"]:
        score -= 0.3
        reasons.append("EMA Bearish Alignment (9<21<50)")

    if last["Close"] > last["EMA200"]:
        score += 0.2
        reasons.append("Above 200 EMA")
    else:
        score -= 0.2
        reasons.append("Below 200 EMA")

    # MACD Cross
    if last["MACD"] > last["MACD_sig"] and prev["MACD"] <= prev["MACD_sig"]:
        score += 0.25
        reasons.append("MACD Bullish Crossover")
    elif last["MACD"] < last["MACD_sig"] and prev["MACD"] >= prev["MACD_sig"]:
        score -= 0.25
        reasons.append("MACD Bearish Crossover")

    # RSI Check
    rsi = last["RSI"]
    if rsi < 30:
        score += 0.25
        reasons.append(f"RSI Oversold ({rsi:.0f})")
    elif rsi > 70:
        score -= 0.25
        reasons.append(f"RSI Overbought ({rsi:.0f})")

    # Bollinger Bands Reversion
    if last["Close"] < last["BB_lower"]:
        score += 0.2
        reasons.append("Price below Lower Bollinger Band")
    elif last["Close"] > last["BB_upper"]:
        score -= 0.2
        reasons.append("Price above Upper Bollinger Band")

    # Stochastic
    if last["Stoch_K"] < 20 and last["Stoch_K"] > last["Stoch_D"]:
        score += 0.15
        reasons.append("Stochastic Oversold Hook Up")
    elif last["Stoch_K"] > 80 and last["Stoch_K"] < last["Stoch_D"]:
        score -= 0.15
        reasons.append("Stochastic Overbought Hook Down")

    strength = min(abs(score), 1.0)
    min_strength = config.get("min_signal_strength", 65) / 100.0

    if strength < min_strength:
        return None

    direction = "buy" if score > 0 else "sell"
    price = last["Close"]

    # Calculate SL and TP
    pip_size = 0.01 if "JPY" in symbol else 0.0001
    sl_dist = config.get("stop_loss_pips", 50) * pip_size
    tp_dist = config.get("take_profit_pips", 100) * pip_size

    if direction == "buy":
        sl = round(price - sl_dist, 5)
        tp = round(price + tp_dist, 5)
    else:
        sl = round(price + sl_dist, 5)
        tp = round(price - tp_dist, 5)

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
#  BACKGROUND TRADING THREAD
# ============================================================
def trading_bot_worker():
    global bot_loop, rpc_api

    start_time = datetime.now()
    logger.info("Trading worker loop started.")

    while True:
        with state_lock:
            running = bot_state["running"]
            connected = bot_state["connected"]
            config = bot_state["config"].copy()

        if not running or not connected or rpc_api is None or bot_loop is None:
            time.sleep(3)
            continue

        try:
            with state_lock:
                bot_state["cycle_count"] += 1
                cycle = bot_state["cycle_count"]

            # 1. Update Account Information
            try:
                acc_info = bot_loop.run_until_complete(rpc_api.get_account_information())
                with state_lock:
                    bot_state["balance"] = acc_info.get("balance", 0.0)
                    bot_state["equity"] = acc_info.get("equity", 0.0)
            except Exception as e:
                logger.warning(f"Could not refresh account info: {e}")

            # 2. Get Open Positions
            raw_positions = bot_loop.run_until_complete(rpc_api.get_positions()) or []
            positions = []
            unrealized = 0.0

            for p in raw_positions:
                pos = {
                    "id": p.get("id"),
                    "symbol": p.get("symbol"),
                    "type": p.get("type", "").replace("POSITION_TYPE_", "").lower(),
                    "volume": p.get("volume", 0.0),
                    "open_price": p.get("openPrice", 0.0),
                    "current_price": p.get("currentPrice", 0.0),
                    "sl": p.get("stopLoss", 0.0),
                    "tp": p.get("takeProfit", 0.0),
                    "profit": p.get("profit", 0.0) + p.get("swap", 0.0),
                    "magic": p.get("magic", 0),
                }
                positions.append(pos)
                unrealized += pos["profit"]

            with state_lock:
                bot_state["positions"] = positions
                bot_state["unrealized_pnl"] = round(unrealized, 2)

            # 3. Check Daily Loss Limit
            with state_lock:
                balance = bot_state["balance"]
                max_daily_loss = balance * (config["max_daily_loss_percent"] / 100.0)
                if unrealized + bot_state["daily_pnl"] < -max_daily_loss and max_daily_loss > 0:
                    bot_state["error_message"] = f"Max daily loss triggered (-${max_daily_loss:.2f})"
                    time.sleep(30)
                    continue

            # 4. Scan Symbols for Opportunities
            max_trades = config.get("max_trades", 3)
            active_count = len(positions)

            for sym in config.get("symbols", []):
                if not bot_state["running"] or active_count >= max_trades:
                    break

                # Prevent multiple orders on the same symbol
                if any(p["symbol"] == sym for p in positions):
                    continue

                try:
                    tf = config.get("timeframe", "1h")
                    candles = bot_loop.run_until_complete(rpc_api.get_symbol_prices(sym, tf, 250))
                    if not candles or len(candles) < 50:
                        continue

                    df = pd.DataFrame(candles)
                    df.columns = [c.capitalize() for c in df.columns]

                    sig = analyze_market(df, sym, config)
                    if sig:
                        # Log signal
                        with state_lock:
                            bot_state["signals_log"].insert(0, {
                                "time": datetime.now().strftime("%H:%M:%S"),
                                "text": f"{sig['direction'].upper()} {sig['symbol']} (Strength: {sig['strength']}%, RSI: {sig['rsi']})",
                                "reasons": sig["reasons"],
                            })
                            bot_state["signals_log"] = bot_state["signals_log"][:30]

                        # Calculate volume based on risk percentage
                        risk_pct = config.get("risk_percent", 1.0) / 100.0
                        risk_amount = balance * risk_pct
                        sl_pips = config.get("stop_loss_pips", 50)
                        pip_value = 10.0  # Approx $10 per lot per pip for standard pairs
                        lots = max(0.01, min(round(risk_amount / (sl_pips * pip_value), 2), 5.0))

                        # Place order
                        if sig["direction"] == "buy":
                            bot_loop.run_until_complete(
                                rpc_api.create_market_buy_order(
                                    symbol=sym, volume=lots, stop_loss=sig["sl"], take_profit=sig["tp"],
                                    options={"magic": 123456, "comment": "RailwayAutoBot"}
                                )
                            )
                        else:
                            bot_loop.run_until_complete(
                                rpc_api.create_market_sell_order(
                                    symbol=sym, volume=lots, stop_loss=sig["sl"], take_profit=sig["tp"],
                                    options={"magic": 123456, "comment": "RailwayAutoBot"}
                                )
                            )

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
                            bot_state["trade_log"] = bot_state["trade_log"][:30]

                        active_count += 1

                except Exception as e:
                    logger.error(f"Execution error on {sym}: {e}")

            # 5. Trailing Stop Management
            trail_pips = config.get("trailing_stop_pips", 20)
            for p in positions:
                if p.get("magic") == 123456:
                    pip_size = 0.01 if "JPY" in p["symbol"] else 0.0001
                    trail_dist = trail_pips * pip_size
                    new_sl = p["sl"]

                    if p["type"] == "buy" and p["current_price"] - trail_dist > p["sl"] and p["current_price"] - trail_dist > p["open_price"]:
                        new_sl = round(p["current_price"] - trail_dist, 5)
                    elif p["type"] == "sell" and (p["sl"] == 0 or p["current_price"] + trail_dist < p["sl"]) and p["current_price"] + trail_dist < p["open_price"]:
                        new_sl = round(p["current_price"] + trail_dist, 5)

                    if new_sl != p["sl"] and new_sl > 0:
                        try:
                            bot_loop.run_until_complete(rpc_api.modify_position(p["id"], new_sl, p["tp"]))
                        except Exception as e:
                            logger.warning(f"Trailing stop update failed: {e}")

            # Uptime calculation
            delta = datetime.now() - start_time
            with state_lock:
                bot_state["uptime"] = f"{int(delta.total_seconds() // 3600)}h {int((delta.total_seconds() % 3600) // 60)}m"
                bot_state["last_update"] = datetime.now().isoformat()
                bot_state["error_message"] = None

            time.sleep(30)

        except Exception as e:
            logger.error(f"Error in trading loop: {e}\n{traceback.format_exc()}")
            with state_lock:
                bot_state["error_message"] = str(e)[:150]
            time.sleep(15)


# ============================================================
#  API ROUTES
# ============================================================
@app.route("/api/connect", methods=["POST"])
def api_connect():
    data = request.get_json(silent=True) or {}
    for f in ["metaapi_token", "login", "password", "server", "platform"]:
        if not data.get(f, "").strip():
            return jsonify({"error": f"Missing field: {f}"}), 400

    if bot_state["connected"]:
        return jsonify({"error": "Already connected. Disconnect first."}), 400

    def background_connect():
        connect_to_mt(
            metaapi_token=data["metaapi_token"],
            login=data["login"],
            password=data["password"],
            server=data["server"],
            platform=data["platform"],
        )

    t = threading.Thread(target=background_connect, daemon=True)
    t.start()
    return jsonify({"status": "connecting"})


@app.route("/api/disconnect", methods=["POST"])
def api_disconnect():
    global rpc_api
    with state_lock:
        bot_state["running"] = False
        bot_state["connected"] = False
        bot_state["account_info"] = None
    if rpc_api and bot_loop:
        try:
            bot_loop.run_until_complete(rpc_api.close())
        except Exception:
            pass
    return jsonify({"status": "disconnected"})


@app.route("/api/start", methods=["POST"])
def api_start():
    global trading_thread
    if not bot_state["connected"]:
        return jsonify({"error": "Connect to your broker first"}), 400

    data = request.get_json(silent=True) or {}
    with state_lock:
        cfg = bot_state["config"]
        if "symbols" in data:
            cfg["symbols"] = data["symbols"]
        if "timeframe" in data:
            cfg["timeframe"] = data["timeframe"]
        if "max_trades" in data:
            cfg["max_trades"] = int(data["max_trades"])
        if "risk_percent" in data:
            cfg["risk_percent"] = float(data["risk_percent"])
        if "max_daily_loss_percent" in data:
            cfg["max_daily_loss_percent"] = float(data["max_daily_loss_percent"])
        if "stop_loss_pips" in data:
            cfg["stop_loss_pips"] = int(data["stop_loss_pips"])
        if "take_profit_pips" in data:
            cfg["take_profit_pips"] = int(data["take_profit_pips"])
        if "trailing_stop_pips" in data:
            cfg["trailing_stop_pips"] = int(data["trailing_stop_pips"])
        if "min_signal_strength" in data:
            cfg["min_signal_strength"] = int(data["min_signal_strength"])

        bot_state["running"] = True

    if trading_thread is None or not trading_thread.is_alive():
        trading_thread = threading.Thread(target=trading_bot_worker, daemon=True)
        trading_thread.start()

    return jsonify({"status": "started"})


@app.route("/api/stop", methods=["POST"])
def api_stop():
    with state_lock:
        bot_state["running"] = False
    return jsonify({"status": "stopped"})


@app.route("/api/close_all", methods=["POST"])
def api_close_all():
    global bot_loop, rpc_api
    if not bot_state["connected"] or not rpc_api:
        return jsonify({"error": "Not connected"}), 400

    try:
        positions = bot_loop.run_until_complete(rpc_api.get_positions()) or []
        closed = 0
        for p in positions:
            if p.get("magic") == 123456:
                bot_loop.run_until_complete(rpc_api.close_position(p["id"]))
                closed += 1
        return jsonify({"status": "ok", "closed": closed})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/status")
def api_status():
    with state_lock:
        return jsonify(bot_state)


# ============================================================
#  MOBILE DASHBOARD UI
# ============================================================
DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<title>🤖 Cloud Trading Bot</title>
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
.btn{padding:13px;border:none;border-radius:10px;font-size:.95em;font-weight:700;cursor:pointer;width:100%;transition:all .2s;text-align:center}
.btn-green{background:var(--green);color:#000}
.btn-red{background:var(--red);color:#fff}
.btn-blue{background:var(--blue);color:#fff}
.btn-yellow{background:var(--yellow);color:#000}
.btn:disabled{opacity:.4;cursor:not-allowed}
.chip-wrap{display:flex;flex-wrap:wrap;gap:6px;margin-bottom:10px}
.chip{padding:6px 12px;border-radius:20px;font-size:.8em;font-weight:600;cursor:pointer;border:1px solid var(--border);background:var(--input-bg);color:var(--muted)}
.chip.active{background:var(--blue);color:#fff;border-color:var(--blue)}
.tabs{display:flex;margin-bottom:12px;background:var(--card);border-radius:10px;overflow:hidden;border:1px solid var(--border)}
.tab{flex:1;padding:12px;text-align:center;font-size:.82em;font-weight:600;cursor:pointer;color:var(--muted)}
.tab.active{background:var(--blue);color:#fff}
.pos-item{display:flex;justify-content:space-between;align-items:center;background:#090d18;border-radius:8px;padding:10px 12px;margin-bottom:6px;font-size:.85em}
.pos-buy{color:var(--green);font-weight:bold}
.pos-sell{color:var(--red);font-weight:bold}
.log{max-height:180px;overflow-y:auto;font-family:monospace;font-size:.75em;line-height:1.6}
.err{background:rgba(255,82,82,.15);border:1px solid var(--red);border-radius:8px;padding:10px;margin-bottom:10px;font-size:.85em;color:var(--red)}
</style>
</head>
<body>

<div class="hdr">
  <div class="hdr-row">
    <h1>🤖 Cloud Trading Bot</h1>
    <div class="badge" id="statusBadge"><div class="dot" id="statusDot"></div><span id="statusText">Offline</span></div>
  </div>
</div>

<div class="wrap">
  <div class="err" id="errBanner" style="display:none"></div>

  <div class="tabs">
    <div class="tab active" onclick="setTab('connect')">🔗 Connect</div>
    <div class="tab" onclick="setTab('config')">⚙️ Strategy</div>
    <div class="tab" onclick="setTab('live')">📊 Live Trade</div>
  </div>

  <!-- CONNECT TAB -->
  <div id="sec-connect">
    <div class="card">
      <div class="card-t">1. MetaAPI Cloud Token</div>
      <label>Get it free at app.metaapi.cloud</label>
      <input type="text" id="token" placeholder="Paste MetaAPI Token">
    </div>

    <div class="card">
      <div class="card-t">2. Broker Credentials</div>
      <label>Platform</label>
      <select id="platform"><option value="mt5">MetaTrader 5 (MT5)</option><option value="mt4">MetaTrader 4 (MT4)</option></select>
      <label>Account Login</label>
      <input type="text" id="login" placeholder="e.g. 50012345">
      <label>Password</label>
      <input type="password" id="password" placeholder="Trading password">
      <label>Server</label>
      <input type="text" id="server" placeholder="e.g. ICMarketsSC-Demo">
      <button class="btn btn-blue" id="btnConn" onclick="connect()">Connect to Broker</button>
      <button class="btn btn-red" id="btnDisconn" onclick="disconnect()" style="display:none;margin-top:8px">Disconnect</button>
    </div>
  </div>

  <!-- CONFIG TAB -->
  <div id="sec-config" style="display:none">
    <div class="card">
      <div class="card-t">Select Trading Pairs</div>
      <div class="chip-wrap" id="pairs"></div>
    </div>

    <div class="card">
      <div class="card-t">Strategy Settings</div>
      <label>Timeframe</label>
      <select id="tf"><option value="15m">M15</option><option value="30m">M30</option><option value="1h" selected>H1</option><option value="4h">H4</option></select>
      
      <div class="grid2">
        <div><label>Max Trades</label><input type="number" id="maxTrades" value="3"></div>
        <div><label>Risk Per Trade (%)</label><input type="number" id="riskPct" value="1.0" step="0.5"></div>
      </div>

      <div class="grid3">
        <div><label>SL (Pips)</label><input type="number" id="slPips" value="50"></div>
        <div><label>TP (Pips)</label><input type="number" id="tpPips" value="100"></div>
        <div><label>Trail (Pips)</label><input type="number" id="trailPips" value="20"></div>
      </div>
    </div>

    <button class="btn btn-green" id="btnRun" onclick="startBot()" disabled>▶ Start Automated Trading</button>
    <button class="btn btn-red" id="btnStop" onclick="stopBot()" style="margin-top:8px;display:none">⏹ Stop Bot</button>
  </div>

  <!-- LIVE TAB -->
  <div id="sec-live" style="display:none">
    <div class="card">
      <div class="card-t">Live Financial Overview</div>
      <div class="pnl-grid">
        <div class="pnl-box"><div class="pnl-lbl">Unrealized P&L</div><div class="pnl-val neutral" id="unrealPnl">$0.00</div></div>
        <div class="pnl-box"><div class="pnl-lbl">Balance</div><div class="pnl-val neutral" id="balance">$0.00</div></div>
      </div>
    </div>

    <div class="card">
      <div class="card-t">Active Positions (<span id="posCount">0</span>)</div>
      <div id="posList" style="color:var(--muted);text-align:center;padding:10px">No open positions</div>
      <button class="btn btn-yellow" style="margin-top:8px" onclick="closeAll()">Close All Positions</button>
    </div>

    <div class="card">
      <div class="card-t">Recent Bot Signals</div>
      <div class="log" id="sigLog">No signals detected yet</div>
    </div>
  </div>
</div>

<script>
const PAIRS = ['EURUSD','GBPUSD','USDJPY','AUDUSD','USDCAD','NZDUSD','EURJPY','GBPJPY','XAUUSD','BTCUSD'];
let selPairs = ['EURUSD','GBPUSD','USDJPY'];

function initPairs(){
  document.getElementById('pairs').innerHTML = PAIRS.map(p => 
    `<div class="chip ${selPairs.includes(p)?'active':''}" onclick="togglePair('${p}',this)">${p}</div>`
  ).join('');
}
function togglePair(p, el){
  if(selPairs.includes(p)){ selPairs = selPairs.filter(x=>x!==p); el.classList.remove('active'); }
  else { selPairs.push(p); el.classList.add('active'); }
}
initPairs();

function setTab(t){
  ['connect','config','live'].forEach(x => {
    document.getElementById('sec-'+x).style.display = (x===t)?'block':'none';
  });
  document.querySelectorAll('.tab').forEach((el,i) => {
    el.classList.toggle('active', ['connect','config','live'][i] === t);
  });
}

async function connect(){
  const token = document.getElementById('token').value.trim();
  const login = document.getElementById('login').value.trim();
  const pass = document.getElementById('password').value.trim();
  const server = document.getElementById('server').value.trim();
  const platform = document.getElementById('platform').value;

  if(!token || !login || !pass || !server){ alert('Please fill in all connection fields'); return; }

  document.getElementById('btnConn').disabled = true;
  document.getElementById('btnConn').textContent = 'Connecting...';

  await fetch('/api/connect', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({metaapi_token: token, login, password: pass, server, platform})
  });
}

async function disconnect(){ await fetch('/api/disconnect', {method:'POST'}); }

async function startBot(){
  const cfg = {
    symbols: selPairs,
    timeframe: document.getElementById('tf').value,
    max_trades: parseInt(document.getElementById('maxTrades').value),
    risk_percent: parseFloat(document.getElementById('riskPct').value),
    stop_loss_pips: parseInt(document.getElementById('slPips').value),
    take_profit_pips: parseInt(document.getElementById('tpPips').value),
    trailing_stop_pips: parseInt(document.getElementById('trailPips').value),
  };
  await fetch('/api/start', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(cfg)});
  setTab('live');
}

async function stopBot(){ await fetch('/api/stop', {method:'POST'}); }
async function closeAll(){ if(confirm('Close all open trades?')) await fetch('/api/close_all', {method:'POST'}); }

async function update(){
  try {
    const res = await fetch('/api/status');
    const d = await res.json();

    const badge = document.getElementById('statusBadge');
    const dot = document.getElementById('statusDot');
    const txt = document.getElementById('statusText');

    if(d.connecting){
      badge.className='badge badge-wait'; dot.className='dot dot-y'; txt.textContent='Connecting';
    } else if(d.connected && d.running){
      badge.className='badge badge-on'; dot.className='dot dot-g'; txt.textContent='Trading Active';
    } else if(d.connected){
      badge.className='badge badge-wait'; dot.className='dot dot-y'; txt.textContent='Ready';
    } else {
      badge.className='badge badge-off'; dot.className='dot dot-r'; txt.textContent='Offline';
    }

    document.getElementById('btnConn').style.display = d.connected ? 'none' : 'block';
    document.getElementById('btnDisconn').style.display = d.connected ? 'block' : 'none';
    document.getElementById('btnRun').style.display = d.running ? 'none' : 'block';
    document.getElementById('btnRun').disabled = !d.connected;
    document.getElementById('btnStop').style.display = d.running ? 'block' : 'none';

    document.getElementById('balance').textContent = '$' + (d.balance || 0).toFixed(2);
    const unEl = document.getElementById('unrealPnl');
    unEl.textContent = (d.unrealized_pnl >= 0 ? '+$' : '-$') + Math.abs(d.unrealized_pnl || 0).toFixed(2);
    unEl.className = 'pnl-val ' + (d.unrealized_pnl > 0 ? 'profit' : d.unrealized_pnl < 0 ? 'loss' : 'neutral');

    document.getElementById('posCount').textContent = (d.positions || []).length;
    if(d.positions && d.positions.length > 0){
      document.getElementById('posList').innerHTML = d.positions.map(p => `
        <div class="pos-item">
          <div><span class="pos-${p.type}">${p.type.toUpperCase()}</span> ${p.volume} ${p.symbol}</div>
          <div class="${p.profit>=0?'profit':'loss'}">$${p.profit.toFixed(2)}</div>
        </div>
      `).join('');
    } else {
      document.getElementById('posList').innerHTML = '<div style="color:var(--muted);text-align:center;padding:10px">No open positions</div>';
    }

    if(d.signals_log && d.signals_log.length > 0){
      document.getElementById('sigLog').innerHTML = d.signals_log.map(s => `<div>[${s.time}] ${s.text}</div>`).join('');
    }

    const err = document.getElementById('errBanner');
    if(d.error_message || d.connection_error){
      err.style.display = 'block';
      err.textContent = d.error_message || d.connection_error;
    } else {
      err.style.display = 'none';
    }

  } catch(e){}
}

setInterval(update, 3000);
update();
</script>
</body>
</html>
"""

@app.route("/")
def index():
    return render_template_string(DASHBOARD_HTML)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)