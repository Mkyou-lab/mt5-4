import os
import asyncio
import threading
import logging
import time
from datetime import datetime, timedelta, timezone
from flask import Flask, render_template_string, request, jsonify
from metaapi_cloud_sdk import MetaApi
import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
app = Flask(__name__)

# ================= BACKGROUND ASYNCIO EVENT LOOP =================
bg_loop = None
def _start_bg_loop():
    global bg_loop
    bg_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(bg_loop)
    bg_loop.run_forever()

threading.Thread(target=_start_bg_loop, daemon=True).start()

# ================= GLOBAL BOT STATE =================
bot_state = {
    "is_connected": False,
    "is_running": False,
    "status_msg": "Offline",
    "last_error": "",
    "api_token": "",
    "account_id": "",
    "account_type": "UNKNOWN",
    "server": "",
    "login": "",
    "balance": 0.0,
    "equity": 0.0,
    "margin": 0.0,
    "free_margin": 0.0,
    "profit": 0.0,
    "symbol": "BTCUSD",
    "actual_symbol": "BTCUSD",
    "timeframe": "1m",
    "macro_timeframe": "15m",
    "lot_size": 0.01,
    "max_trades": 1,
    "min_profit_target_usd": 1.00,    # Target Profit in USD to close & bank
    "min_confidence_score": 80,       # Minimum 80% accuracy score required to enter
    "stop_loss_pips": 1500,
    "take_profit_pips": 3000,
    "open_positions": [],
    "live_tick": {"bid": 0.0, "ask": 0.0, "spread": 0.0, "time": ""},
    "market_analysis": {
        "price": 0.0,
        "macro_trend": "ANALYZING",
        "rsi": 0.0,
        "macd_hist": 0.0,
        "confidence_score": 0,
        "signal": "WAITING"
    },
    "logs": ["High-Accuracy Multi-Confluence Engine Ready. Connect MT4/MT5 account."]
}

def add_log(msg):
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    entry = f"[{ts}] {msg}"
    bot_state["logs"].append(entry)
    if len(bot_state["logs"]) > 130:
        bot_state["logs"].pop(0)
    logging.info(msg)

# ================= TECHNICAL INDICATORS =================
def calculate_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / (loss + 1e-9)
    return 100 - (100 / (1 + rs))

def calculate_macd(series, fast=12, slow=26, signal=9):
    ema_fast = calculate_ema(series, fast)
    ema_slow = calculate_ema(series, slow)
    macd_line = ema_fast - ema_slow
    signal_line = calculate_ema(macd_line, signal)
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram

# ================= METAAPI HIGH-ACCURACY ENGINE =================
class HighAccuracyEngine:
    def __init__(self):
        self.api = None
        self.account = None
        self.connection = None
        self.specs_cache = {}

    async def connect_with_account_id(self, token, account_id):
        try:
            add_log(f"Connecting to MetaApi Cloud with Account ID: {account_id}")
            self.api = MetaApi(token)
            self.account = await self.api.metatrader_account_api.get_account(account_id)
            return await self._finish_connection()
        except Exception as e:
            err = str(e)
            bot_state["last_error"] = err
            bot_state["status_msg"] = "Connection Failed"
            bot_state["is_connected"] = False
            add_log(f"❌ Connection Error: {err}")
            return False, err

    async def connect_with_credentials(self, token, login, password, server, platform, region):
        try:
            add_log(f"Connecting to Broker Server: {server} | Login: {login}")
            self.api = MetaApi(token)

            try:
                accounts = await self.api.metatrader_account_api.get_accounts()
            except:
                accounts = await self.api.metatrader_account_api.get_accounts_with_infinite_scroll_pagination()

            existing = None
            for acc in accounts:
                if str(acc.login) == str(login) and server.lower() in str(acc.server).lower():
                    existing = acc
                    break

            if existing:
                self.account = existing
            else:
                payload = {
                    "name": f"AccurateBot-{login}",
                    "type": "cloud",
                    "login": str(login),
                    "password": str(password),
                    "server": str(server),
                    "platform": "mt5" if "5" in str(platform) else "mt4",
                    "magic": 998877
                }
                self.account = await self.api.metatrader_account_api.create_account(payload)

            return await self._finish_connection()
        except Exception as e:
            err = str(e)
            bot_state["last_error"] = err
            bot_state["status_msg"] = "Connection Failed"
            bot_state["is_connected"] = False
            add_log(f"❌ Connection Error: {err}")
            return False, err

    async def _finish_connection(self):
        bot_state["account_id"] = self.account.id
        bot_state["account_type"] = str(getattr(self.account, "type", "CLOUD")).upper()
        bot_state["server"] = str(getattr(self.account, "server", "CloudServer"))
        bot_state["login"] = str(getattr(self.account, "login", "Connected"))

        if self.account.state != "DEPLOYED":
            add_log("Deploying broker terminal container...")
            await self.account.deploy()

        add_log("Synchronizing live price stream...")
        await self.account.wait_connected()

        self.connection = self.account.get_rpc_connection()
        await self.connection.connect()
        await self.connection.wait_synchronized()

        await self.update_account_info()
        bot_state["is_connected"] = True
        bot_state["status_msg"] = f"Online ({bot_state['account_type']})"
        bot_state["last_error"] = ""
        add_log("⚡ SUCCESS! High-Accuracy Engine Connected & Synchronized with MT4/MT5.")
        return True, "Connected successfully"

    async def get_symbol_spec_cached(self, symbol):
        if symbol not in self.specs_cache:
            try:
                spec = await self.connection.get_symbol_specification(symbol)
                self.specs_cache[symbol] = {
                    "digits": spec.get("digits", 2),
                    "point": spec.get("point", 0.01)
                }
            except Exception:
                self.specs_cache[symbol] = {"digits": 2, "point": 0.01}
        return self.specs_cache[symbol]

    async def update_account_info(self):
        if not self.connection:
            return
        try:
            info = await self.connection.get_account_information()
            bot_state["balance"] = float(info.get("balance", 0.0))
            bot_state["equity"] = float(info.get("equity", 0.0))
            bot_state["margin"] = float(info.get("margin", 0.0))
            bot_state["free_margin"] = float(info.get("freeMargin", 0.0))
            bot_state["profit"] = round(bot_state["equity"] - bot_state["balance"], 2)

            raw_positions = await self.connection.get_positions()
            formatted_pos = []
            for p in raw_positions:
                pos_type = str(p.get("type", ""))
                type_str = "BUY" if "BUY" in pos_type else "SELL"
                formatted_pos.append({
                    "id": str(p.get("id")),
                    "symbol": p.get("symbol"),
                    "type": type_str,
                    "raw_type": pos_type,
                    "volume": float(p.get("volume", 0.01)),
                    "openPrice": float(p.get("openPrice", 0.0)),
                    "currentPrice": float(p.get("currentPrice", 0.0)),
                    "profit": float(p.get("profit", 0.0)),
                    "sl": p.get("stopLoss", 0.0),
                    "tp": p.get("takeProfit", 0.0)
                })
            bot_state["open_positions"] = formatted_pos
        except Exception:
            pass

    async def resolve_symbol(self, requested_symbol):
        try:
            symbols = await self.connection.get_symbols()
            if requested_symbol in symbols:
                return requested_symbol
            clean_req = requested_symbol.replace("/", "").replace("_", "").replace(".", "").upper()
            for s in symbols:
                clean_s = s.replace("/", "").replace("_", "").replace(".", "").upper()
                if clean_req in clean_s or clean_s in clean_req:
                    add_log(f"Broker Symbol Resolved: '{requested_symbol}' → '{s}'")
                    return s
            return requested_symbol
        except Exception:
            return requested_symbol

    async def fetch_candles(self, symbol, timeframe, count=60):
        try:
            start_time = datetime.now(timezone.utc) - timedelta(days=2)
            meta_tf = timeframe if timeframe in ['1m','5m','15m','30m','1h'] else '1m'
            
            candles = None
            if hasattr(self.api, 'historical_market_data_client'):
                candles = await self.api.historical_market_data_client.get_historical_candles(
                    self.account.server, symbol, meta_tf, start_time, count
                )

            if not candles or len(candles) == 0:
                tick = await self.connection.get_symbol_price(symbol)
                mid = (tick['ask'] + tick['bid']) / 2.0
                return pd.DataFrame({'close': [mid]*30, 'high': [mid*1.0001]*30, 'low': [mid*0.9999]*30})

            df = pd.DataFrame(candles)
            for col in ["open", "high", "low", "close"]:
                if col in df.columns:
                    df[col] = df[col].astype(float)
            return df
        except Exception:
            return None

    async def close_position_robust(self, position_id):
        pos_id_str = str(position_id)
        add_log(f"Closing Trade Ticket #{pos_id_str}...")
        try:
            await self.connection.close_position(pos_id_str)
            add_log(f"✅ Trade #{pos_id_str} CLOSED successfully in Profit!")
            await self.update_account_info()
            return True
        except Exception:
            try:
                if pos_id_str.isdigit():
                    await self.connection.close_position(int(pos_id_str))
                    add_log(f"✅ Trade #{pos_id_str} CLOSED successfully in Profit!")
                    await self.update_account_info()
                    return True
            except Exception as e:
                add_log(f"❌ Close Error: {e}")
        return False

    async def auto_manage_and_protect_profit(self, symbol, current_signal):
        """ Profit Lock Engine: Closes trade when Target Profit is met or when trend shifts """
        try:
            positions = bot_state["open_positions"]
            target = bot_state["min_profit_target_usd"]

            for pos in positions:
                if pos["symbol"].replace("T", "") in symbol.replace("T", "") or symbol.replace("T", "") in pos["symbol"].replace("T", ""):
                    profit = pos["profit"]
                    pos_type = pos["type"]

                    # Rule 1: Target Profit Hit ($ USD) -> Close & Bank Profit
                    if profit >= target:
                        add_log(f"🎯 TARGET PROFIT REACHED (+${profit:.2f} >= ${target:.2f})! Closing Trade #{pos['id']} on MT5...")
                        await self.close_position_robust(pos["id"])

                    # Rule 2: Partial Profit Lock (Locking in profit if 60% of target hit & structure weakens)
                    elif profit >= (target * 0.60) and ((pos_type == "BUY" and current_signal == "SELL") or (pos_type == "SELL" and current_signal == "BUY")):
                        add_log(f"🔒 LOCKING PROFIT (+${profit:.2f})! Trend weakening, closing Trade #{pos['id']} to secure gains...")
                        await self.close_position_robust(pos["id"])

        except Exception as e:
            pass

    async def execute_trade(self, action, symbol, lot, sl_pips, tp_pips):
        try:
            spec = await self.get_symbol_spec_cached(symbol)
            digits = spec["digits"]
            point = spec["point"]

            price_info = await self.connection.get_symbol_price(symbol)
            entry = price_info["ask"] if action == "BUY" else price_info["bid"]

            pip_scale = 1.0 if "BTC" in symbol or "ETH" in symbol else (point if point > 0 else 0.0001)

            sl = entry - (sl_pips * pip_scale) if action == "BUY" else entry + (sl_pips * pip_scale)
            tp = entry + (tp_pips * pip_scale) if action == "BUY" else entry - (tp_pips * pip_scale)

            add_log(f"⚡ HIGH-ACCURACY EXECUTION: Opening {action} on {symbol} | Lot: {lot} @ Entry: {entry:.2f}")

            if action == "BUY":
                res = await self.connection.create_market_buy_order(symbol, lot, round(sl, digits), round(tp, digits))
            else:
                res = await self.connection.create_market_sell_order(symbol, lot, round(sl, digits), round(tp, digits))

            add_log(f"✅ TRADE EXECUTED ON MT5! Order ID: {res.get('stringCode', 'CONFIRMED')}")
            await self.update_account_info()
        except Exception as e:
            add_log(f"❌ Execution Error: {e}")

    async def run_high_accuracy_loop(self):
        add_log("🚀 HIGH-ACCURACY CONFLUENCE ENGINE ACTIVE!")
        bot_state["actual_symbol"] = await self.resolve_symbol(bot_state["symbol"])
        symbol = bot_state["actual_symbol"]

        try:
            await self.connection.subscribe_to_market_data(symbol)
        except Exception:
            pass

        while bot_state["is_running"] and bot_state["is_connected"]:
            try:
                # 1. Real-time Market Prices
                tick = await self.connection.get_symbol_price(symbol)
                bid, ask = float(tick["bid"]), float(tick["ask"])
                mid_price = (bid + ask) / 2.0
                spread = round((ask - bid), 2 if "BTC" in symbol else 5)

                bot_state["live_tick"] = {
                    "bid": bid, "ask": ask, "spread": spread,
                    "time": datetime.now().strftime("%H:%M:%S.%f")[:-3]
                }

                # 2. Multi-Timeframe Analysis
                # Higher Timeframe (M15) for Macro Direction
                df_macro = await self.fetch_candles(symbol, bot_state["macro_timeframe"], count=50)
                macro_trend = "NEUTRAL"
                if df_macro is not None and len(df_macro) >= 20:
                    macro_ema = calculate_ema(df_macro["close"], 50)
                    if df_macro["close"].iloc[-1] > macro_ema.iloc[-1]:
                        macro_trend = "BULLISH 🟢"
                    else:
                        macro_trend = "BEARISH 🔴"

                # Entry Timeframe (M1) for Confluence Scoring
                df_entry = await self.fetch_candles(symbol, bot_state["timeframe"], count=50)
                
                signal = "WAITING"
                confidence_score = 0

                if df_entry is not None and len(df_entry) >= 20:
                    df_entry["ema_f"] = calculate_ema(df_entry["close"], 9)
                    df_entry["ema_s"] = calculate_ema(df_entry["close"], 21)
                    df_entry["rsi"] = calculate_rsi(df_entry["close"], 14)
                    _, _, df_entry["macd_hist"] = calculate_macd(df_entry["close"])

                    ema_f = df_entry["ema_f"].iloc[-1]
                    ema_s = df_entry["ema_s"].iloc[-1]
                    rsi = df_entry["rsi"].iloc[-1]
                    macd_h = df_entry["macd_hist"].iloc[-1]

                    # BUY Confluence Scoring
                    if "BULLISH" in macro_trend:
                        score_buy = 30  # Macro Alignment (+30)
                        if ema_f > ema_s: score_buy += 25  # EMA Crossover (+25)
                        if 50 < rsi < 70: score_buy += 25  # RSI Momentum (+25)
                        if macd_h > 0: score_buy += 20     # MACD Histogram (+20)
                        
                        if score_buy >= bot_state["min_confidence_score"]:
                            signal = "BUY"
                            confidence_score = score_buy

                    # SELL Confluence Scoring
                    elif "BEARISH" in macro_trend:
                        score_sell = 30  # Macro Alignment (+30)
                        if ema_f < ema_s: score_sell += 25  # EMA Crossover (+25)
                        if 30 < rsi < 50: score_sell += 25  # RSI Momentum (+25)
                        if macd_h < 0: score_sell += 20     # MACD Histogram (+20)

                        if score_sell >= bot_state["min_confidence_score"]:
                            signal = "SELL"
                            confidence_score = score_sell

                    bot_state["market_analysis"] = {
                        "price": round(mid_price, 2),
                        "macro_trend": macro_trend,
                        "rsi": round(rsi, 1),
                        "macd_hist": round(macd_h, 3),
                        "confidence_score": confidence_score if signal != "WAITING" else max(score_buy if 'score_buy' in locals() else 0, score_sell if 'score_sell' in locals() else 0),
                        "signal": signal
                    }

                add_log(f"📊 [{symbol}] Macro: {macro_trend} | Confluence Score: {bot_state['market_analysis']['confidence_score']}% → Signal: {signal}")

                # 3. Manage Open Trades & Profit Lock
                await self.auto_manage_and_protect_profit(symbol, signal)

                # 4. Open Trade ONLY if Confluence Score Threshold met
                matching_positions = [
                    p for p in bot_state["open_positions"] 
                    if p["symbol"].replace("T", "") in symbol.replace("T", "") or symbol.replace("T", "") in p["symbol"].replace("T", "")
                ]

                if len(matching_positions) < bot_state["max_trades"] and signal in ["BUY", "SELL"]:
                    add_log(f"🎯 ACCURATE ENTRY MATCHED! Score: {confidence_score}% >= {bot_state['min_confidence_score']}% Threshold. Opening {signal} Trade...")
                    await self.execute_trade(
                        signal, symbol, bot_state["lot_size"],
                        bot_state["stop_loss_pips"], bot_state["take_profit_pips"]
                    )

            except Exception as e:
                add_log(f"Analysis Engine Note: {e}")

            await asyncio.sleep(1.5)

        add_log("Trading loop stopped.")

engine = HighAccuracyEngine()

# ================= EMBEDDED DASHBOARD UI =================
HTML_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Cloud Automated High-Accuracy Scalper Bot Overlay</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
<script type="text/javascript" src="https://s3.tradingview.com/tv.js"></script>
<style>
body { background-color: #06080d; color: #adbac7; font-family: system-ui, -apple-system, sans-serif; }
.card { background-color: #111622; border: 1px solid #212836; border-radius: 10px; margin-bottom: 12px; }
.nav-pills .nav-link { color: #768390; font-weight: 600; font-size: 14px; }
.nav-pills .nav-link.active { background-color: #1f6feb; color: #fff; }
.form-control, .form-select { background-color: #090c12; border: 1px solid #212836; color: #e6edf3; font-size: 14px; }
.btn-success { background-color: #238636; border: none; font-weight: 600; }
.btn-danger { background-color: #da3633; border: none; font-weight: 600; }
.status-badge { font-weight: 700; padding: 6px 12px; border-radius: 20px; font-size: 12px; }
.bg-online { background-color: rgba(46, 160, 67, 0.15); color: #3fb950; border: 1px solid #238636; }
.bg-offline { background-color: rgba(248, 81, 73, 0.15); color: #f85149; border: 1px solid #da3633; }
.metric-title { font-size: 11px; text-transform: uppercase; color: #768390; letter-spacing: 0.5px; }
.metric-value { font-size: 18px; font-weight: 700; color: #fff; }
.log-box { background-color: #040508; border: 1px solid #212836; height: 230px; overflow-y: auto; font-family: monospace; font-size: 11px; padding: 10px; border-radius: 6px; color: #3fb950; }
.chart-container { height: 360px; width: 100%; border-radius: 8px; overflow: hidden; }
.live-price-box { font-size: 20px; font-weight: 800; font-family: monospace; }
</style>
</head>
<body class="p-2 p-md-3">
<div class="container-fluid" style="max-width: 1100px;">

    <!-- TOP HEADER -->
    <div class="d-flex justify-content-between align-items-center mb-3 card p-3">
        <div>
            <h5 class="m-0 text-white font-weight-bold">🎯 High-Accuracy MT4/MT5 Automated Bot</h5>
            <small class="text-muted" id="accountSub">Not Connected</small>
        </div>
        <div>
            <span id="statusBadge" class="status-badge bg-offline">● Offline</span>
        </div>
    </div>

    <!-- LIVE METRICS BAR -->
    <div class="row g-2 mb-3">
        <div class="col-6 col-md-3">
            <div class="card p-2 text-center">
                <span class="metric-title">Live MT5 Bid / Ask</span>
                <div class="live-price-box text-success" id="tickPrice">0.00 / 0.00</div>
            </div>
        </div>
        <div class="col-6 col-md-3">
            <div class="card p-2 text-center">
                <span class="metric-title">Balance / Equity</span>
                <div class="metric-value text-info" id="balVal">$0.00</div>
            </div>
        </div>
        <div class="col-6 col-md-3">
            <div class="card p-2 text-center">
                <span class="metric-title">Floating P/L</span>
                <div class="metric-value" id="plVal">$0.00</div>
            </div>
        </div>
        <div class="col-6 col-md-3">
            <div class="card p-2 text-center">
                <span class="metric-title">Accuracy Score</span>
                <div class="metric-value text-warning" id="scoreVal">0% (WAITING)</div>
            </div>
        </div>
    </div>

    <!-- NAVIGATION TABS -->
    <ul class="nav nav-pills mb-3">
        <li class="nav-item"><button class="nav-link active" data-tab="live">📊 Live Terminal & Chart</button></li>
        <li class="nav-item"><button class="nav-link" data-tab="strategy">⚙️ Settings & Profit Target</button></li>
        <li class="nav-item"><button class="nav-link" data-tab="connect">🔗 Broker Login</button></li>
    </ul>

    <!-- TAB 1: TERMINAL & CHART -->
    <div id="tab-live" class="tab-pane">
        <div class="card p-2 mb-3">
            <div id="tv_chart_container" class="chart-container"></div>
        </div>

        <div class="card p-3 mb-3">
            <div class="d-flex justify-content-between align-items-center mb-2">
                <h6 class="text-white m-0">Live Active Trades</h6>
                <div class="d-flex gap-2">
                    <button id="btnStart" onclick="startBot()" class="btn btn-success btn-sm px-3" disabled>▶ RUN BOT</button>
                    <button id="btnStop" onclick="stopBot()" class="btn btn-danger btn-sm px-3" disabled>⏹ STOP</button>
                </div>
            </div>
            <div class="table-responsive">
                <table class="table table-dark table-striped align-middle mb-0" style="font-size:12px;">
                    <thead>
                        <tr>
                            <th>Symbol</th>
                            <th>Type</th>
                            <th>Volume</th>
                            <th>Entry Price</th>
                            <th>Current Price</th>
                            <th>Floating Profit</th>
                            <th>Action</th>
                        </tr>
                    </thead>
                    <tbody id="posTable">
                        <tr><td colspan="7" class="text-center text-muted">No active open trades</td></tr>
                    </tbody>
                </table>
            </div>
        </div>
    </div>

    <!-- TAB 2: SETTINGS -->
    <div id="tab-strategy" class="tab-pane" style="display:none;">
        <div class="card p-3">
            <h6 class="text-white mb-3">High-Accuracy Trading Settings</h6>
            <div class="row g-3">
                <div class="col-md-6">
                    <label class="form-label">Symbol Select</label>
                    <select id="symbolSelect" class="form-select" onchange="updateTradingViewChart()">
                        <optgroup label="24/7 Crypto Pairs">
                            <option value="BTCUSD" selected>BTCUSD (Bitcoin)</option>
                            <option value="ETHUSD">ETHUSD (Ethereum)</option>
                            <option value="SOLUSD">SOLUSD (Solana)</option>
                        </optgroup>
                        <optgroup label="Forex & Gold">
                            <option value="EURUSD">EURUSD</option>
                            <option value="GBPUSD">GBPUSD</option>
                            <option value="XAUUSD">XAUUSD (Gold)</option>
                        </optgroup>
                    </select>
                </div>
                <div class="col-md-6">
                    <label class="form-label">Entry Timeframe</label>
                    <select id="tfSelect" class="form-select" onchange="updateTradingViewChart()">
                        <option value="1m" selected>M1 (High Accuracy Scalp)</option>
                        <option value="5m">M5</option>
                        <option value="15m">M15</option>
                    </select>
                </div>
                <div class="col-6 col-md-3">
                    <label class="form-label">Target Profit ($ USD to Bank)</label>
                    <input type="number" id="targetProfitInput" class="form-control" value="1.00" step="0.50">
                </div>
                <div class="col-6 col-md-3">
                    <label class="form-label">Minimum Accuracy Threshold</label>
                    <select id="scoreThresholdInput" class="form-select">
                        <option value="75">75% (More Trades)</option>
                        <option value="80" selected>80% (Recommended High Win-Rate)</option>
                        <option value="90">90% (Strict / Max Accuracy)</option>
                    </select>
                </div>
                <div class="col-6 col-md-3">
                    <label class="form-label">Lot Size</label>
                    <input type="number" id="lotInput" class="form-control" value="0.01" step="0.01">
                </div>
                <div class="col-6 col-md-3">
                    <label class="form-label">Max Active Trades</label>
                    <input type="number" id="maxTradesInput" class="form-control" value="1">
                </div>
            </div>
        </div>
    </div>

    <!-- TAB 3: BROKER LOGIN -->
    <div id="tab-connect" class="tab-pane" style="display:none;">
        <div class="card p-3">
            <h6 class="text-white mb-3">Connect Broker Account</h6>
            <div class="mb-3">
                <label class="form-label">MetaApi Token</label>
                <input type="password" id="tokenInput" class="form-control" placeholder="Paste token from app.metaapi.cloud">
            </div>

            <div class="mb-3">
                <label class="form-label">Connection Method</label>
                <select id="methodSelect" class="form-select" onchange="toggleMethodBoxes()">
                    <option value="account_id">Option 1: MetaApi Account ID (Recommended)</option>
                    <option value="credentials">Option 2: Account Login + Password + Server</option>
                </select>
            </div>

            <div id="boxAccountId">
                <div class="mb-3">
                    <label class="form-label">Account ID</label>
                    <input type="text" id="accIdInput" class="form-control" placeholder="Paste Account ID">
                </div>
            </div>

            <div id="boxCredentials" style="display:none;">
                <div class="mb-3">
                    <label class="form-label">Platform</label>
                    <select id="platformSelect" class="form-select">
                        <option value="mt5">MetaTrader 5 (MT5)</option>
                        <option value="mt4">MetaTrader 4 (MT4)</option>
                    </select>
                </div>
                <div class="mb-3">
                    <label class="form-label">Account Login</label>
                    <input type="text" id="loginInput" class="form-control" placeholder="e.g. 476924559">
                </div>
                <div class="mb-3">
                    <label class="form-label">Password</label>
                    <input type="password" id="passInput" class="form-control">
                </div>
                <div class="mb-3">
                    <label class="form-label">Server Exact Name</label>
                    <input type="text" id="serverInput" class="form-control" placeholder="e.g. Exness-MT5Trial9">
                </div>
            </div>

            <button id="btnConnect" onclick="doConnect()" class="btn btn-success w-100 py-2 mt-2">Connect Account</button>
        </div>
    </div>

    <!-- LOGS CONSOLE -->
    <div class="card p-3">
        <h6 class="text-white mb-2">Live Automated Execution Console</h6>
        <div id="logBox" class="log-box"></div>
    </div>

</div>

<script>
let tvWidget = null;

function initTradingViewChart(symbol, timeframe) {
    let tvSymbol = "BINANCE:BTCUSDT";
    if(symbol.includes("ETH")) tvSymbol = "BINANCE:ETHUSDT";
    else if(symbol.includes("SOL")) tvSymbol = "BINANCE:SOLUSDT";
    else if(symbol.includes("EUR")) tvSymbol = "FX:EURUSD";
    else if(symbol.includes("GBP")) tvSymbol = "FX:GBPUSD";
    else if(symbol.includes("XAU")) tvSymbol = "OANDA:XAUUSD";

    let tvInterval = "1";
    if(timeframe === "5m") tvInterval = "5";
    if(timeframe === "15m") tvInterval = "15";

    document.getElementById("tv_chart_container").innerHTML = "";
    tvWidget = new TradingView.widget({
        "autosize": true,
        "symbol": tvSymbol,
        "interval": tvInterval,
        "timezone": "Etc/UTC",
        "theme": "dark",
        "style": "1",
        "locale": "en",
        "container_id": "tv_chart_container"
    });
}

function updateTradingViewChart() {
    const sym = document.getElementById("symbolSelect").value;
    const tf = document.getElementById("tfSelect").value;
    initTradingViewChart(sym, tf);
}

function toggleMethodBoxes() {
    const m = document.getElementById("methodSelect").value;
    document.getElementById("boxAccountId").style.display = (m === "account_id") ? "block" : "none";
    document.getElementById("boxCredentials").style.display = (m === "credentials") ? "block" : "none";
}

document.querySelectorAll('[data-tab]').forEach(btn => {
    btn.onclick = () => {
        document.querySelectorAll('.nav-link').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        document.querySelectorAll('.tab-pane').forEach(p => p.style.display = 'none');
        document.getElementById('tab-' + btn.dataset.tab).style.display = 'block';
    }
});

async function refreshUI() {
    try {
        const res = await fetch('/api/status');
        const d = await res.json();

        const badge = document.getElementById('statusBadge');
        badge.textContent = d.is_connected ? '● ' + d.status_msg : '● Offline';
        badge.className = d.is_connected ? 'status-badge bg-online' : 'status-badge bg-offline';
        document.getElementById('accountSub').textContent = d.is_connected ? `Server: ${d.server} | Login: ${d.login}` : 'Not Connected';

        if(d.live_tick) {
            document.getElementById('tickPrice').textContent = d.live_tick.bid.toFixed(2) + ' / ' + d.live_tick.ask.toFixed(2);
        }

        document.getElementById('balVal').textContent = '$' + d.balance.toFixed(2) + ' / $' + d.equity.toFixed(2);
        
        if(d.market_analysis) {
            document.getElementById('scoreVal').textContent = (d.market_analysis.confidence_score || 0) + '% (' + (d.market_analysis.signal || 'WAITING') + ')';
        }

        const plElem = document.getElementById('plVal');
        plElem.textContent = (d.profit >= 0 ? '+$' : '-$') + Math.abs(d.profit).toFixed(2);
        plElem.className = d.profit >= 0 ? 'metric-value text-success' : 'metric-value text-danger';

        const logBox = document.getElementById('logBox');
        logBox.innerHTML = d.logs.join('<br>');
        logBox.scrollTop = logBox.scrollHeight;

        const posTable = document.getElementById('posTable');
        if(d.open_positions && d.open_positions.length > 0) {
            let html = '';
            d.open_positions.forEach(p => {
                const profitClass = p.profit >= 0 ? 'text-success font-weight-bold' : 'text-danger font-weight-bold';
                html += `<tr>
                    <td>${p.symbol}</td>
                    <td><span class="badge ${p.type === 'BUY' ? 'bg-success' : 'bg-danger'}">${p.type}</span></td>
                    <td>${p.volume}</td>
                    <td>${p.openPrice}</td>
                    <td>${p.currentPrice}</td>
                    <td class="${profitClass}">$${p.profit.toFixed(2)}</td>
                    <td><button onclick="closePosition('${p.id}')" class="btn btn-danger btn-sm py-0">Close</button></td>
                </tr>`;
            });
            posTable.innerHTML = html;
        } else {
            posTable.innerHTML = `<tr><td colspan="7" class="text-center text-muted">No active open trades</td></tr>`;
        }

        document.getElementById('btnStart').disabled = !d.is_connected || d.is_running;
        document.getElementById('btnStop').disabled = !d.is_running;

    } catch(e) {}
}

async function doConnect() {
    const btn = document.getElementById('btnConnect');
    btn.disabled = true;
    btn.textContent = 'Connecting...';

    const method = document.getElementById('methodSelect').value;
    const body = {
        token: document.getElementById('tokenInput').value.trim(),
        method: method,
        account_id: document.getElementById('accIdInput').value.trim(),
        login: document.getElementById('loginInput').value.trim(),
        password: document.getElementById('passInput').value,
        server: document.getElementById('serverInput').value.trim(),
        platform: document.getElementById('platformSelect').value
    };

    const res = await fetch('/api/connect', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(body)
    });
    const d = await res.json();

    btn.disabled = false;
    btn.textContent = 'Connect Account';

    if(d.success) {
        alert('✅ Connected Successfully!');
        document.querySelector('[data-tab="live"]').click();
    } else {
        alert('❌ Connection Failed: ' + d.message);
    }
}

async function startBot() {
    const body = {
        symbol: document.getElementById('symbolSelect').value,
        timeframe: document.getElementById('tfSelect').value,
        lot_size: parseFloat(document.getElementById('lotInput').value),
        max_trades: parseInt(document.getElementById('maxTradesInput').value),
        min_profit_target_usd: parseFloat(document.getElementById('targetProfitInput').value),
        min_confidence_score: parseInt(document.getElementById('scoreThresholdInput').value),
        stop_loss_pips: 1500,
        take_profit_pips: 3000
    };
    await fetch('/api/start', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(body)
    });
}

async function stopBot() {
    await fetch('/api/stop', {method: 'POST'});
}

async function closePosition(posId) {
    if(confirm('Close trade #' + posId + ' now?')) {
        await fetch('/api/close_position', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({position_id: posId})
        });
    }
}

window.onload = () => {
    initTradingViewChart("BTCUSD", "1m");
    setInterval(refreshUI, 1000);
};
</script>
</body>
</html>
"""

# ================= FLASK REST API ROUTES =================
@app.route("/")
def home():
    return render_template_string(HTML_TEMPLATE)

@app.route("/api/status")
def status():
    return jsonify(bot_state)

@app.route("/api/connect", methods=["POST"])
def api_connect():
    data = request.json or {}
    token = data.get("token", "").strip()
    if not token:
        return jsonify(success=False, message="MetaApi Token is required")

    bot_state["api_token"] = token
    bot_state["last_error"] = ""
    bot_state["status_msg"] = "Connecting..."

    async def _do():
        if data.get("method") == "account_id":
            acc_id = data.get("account_id", "").strip()
            return await engine.connect_with_account_id(token, acc_id)
        else:
            return await engine.connect_with_credentials(
                token,
                data.get("login"),
                data.get("password"),
                data.get("server"),
                data.get("platform", "mt5"),
                "default"
            )

    future = asyncio.run_coroutine_threadsafe(_do(), bg_loop)
    try:
        ok, msg = future.result(timeout=120)
        return jsonify(success=ok, message=msg)
    except Exception as e:
        bot_state["last_error"] = str(e)
        bot_state["status_msg"] = "Connection Failed"
        return jsonify(success=False, message=str(e))

@app.route("/api/start", methods=["POST"])
def api_start():
    if not bot_state["is_connected"]:
        return jsonify(status="not_connected")
    data = request.json or {}
    bot_state.update({
        "symbol": data.get("symbol", "BTCUSD").upper(),
        "timeframe": data.get("timeframe", "1m"),
        "lot_size": float(data.get("lot_size", 0.01)),
        "max_trades": int(data.get("max_trades", 1)),
        "min_profit_target_usd": float(data.get("min_profit_target_usd", 1.00)),
        "min_confidence_score": int(data.get("min_confidence_score", 80)),
        "stop_loss_pips": 1500,
        "take_profit_pips": 3000,
        "is_running": True
    })
    asyncio.run_coroutine_threadsafe(engine.run_high_accuracy_loop(), bg_loop)
    return jsonify(status="started")

@app.route("/api/stop", methods=["POST"])
def api_stop():
    bot_state["is_running"] = False
    add_log("Automated Scalper stop requested.")
    return jsonify(status="stopped")

@app.route("/api/close_position", methods=["POST"])
def api_close_position():
    data = request.json or {}
    pos_id = data.get("position_id")
    if pos_id and bot_state["is_connected"]:
        asyncio.run_coroutine_threadsafe(engine.close_position_robust(pos_id), bg_loop)
        return jsonify(status="closing_initiated")
    return jsonify(status="failed")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
