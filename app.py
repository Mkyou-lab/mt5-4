import os
import asyncio
import threading
import logging
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
    "timeframe": "15m",
    "lot_size": 0.01,
    "max_trades": 1,
    "stop_loss_pips": 2000,
    "take_profit_pips": 4000,
    "open_positions": [],
    "logs": ["Bot engine initialized. Please connect your broker account."],
    "last_analysis": {
        "price": 0.0,
        "ema_fast": 0.0,
        "ema_slow": 0.0,
        "rsi": 0.0,
        "macd_hist": 0.0,
        "atr": 0.0,
        "signal": "WAITING"
    }
}

def add_log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    entry = f"[{ts}] {msg}"
    bot_state["logs"].append(entry)
    if len(bot_state["logs"]) > 120:
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

def calculate_atr(df, period=14):
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift()).abs()
    low_close = (df['low'] - df['close'].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.rolling(window=period).mean()

# ================= METAAPI ENGINE =================
class MetaApiEngine:
    def __init__(self):
        self.api = None
        self.account = None
        self.connection = None

    async def connect_with_account_id(self, token, account_id):
        try:
            add_log(f"Initiating cloud sync with Account ID: {account_id}")
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
            add_log(f"Provisioning account for Login: {login} on Server: {server}")
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
                add_log(f"Found existing cloud instance: {existing.id}")
                self.account = existing
            else:
                add_log("Creating new MetaApi Cloud terminal instance...")
                payload = {
                    "name": f"CloudBot-{login}",
                    "type": "cloud",
                    "login": str(login),
                    "password": str(password),
                    "server": str(server),
                    "platform": "mt5" if "5" in str(platform) else "mt4",
                    "magic": 998877
                }
                if region and region != "default":
                    payload["region"] = region

                self.account = await self.api.metatrader_account_api.create_account(payload)

            return await self._finish_connection()
        except Exception as e:
            err = str(e)
            bot_state["last_error"] = err
            bot_state["status_msg"] = "Connection Failed"
            bot_state["is_connected"] = False
            add_log(f"❌ Provisioning Error: {err}")
            return False, err

    async def _finish_connection(self):
        bot_state["account_id"] = self.account.id
        bot_state["account_type"] = str(getattr(self.account, "type", "CLOUD")).upper()
        bot_state["server"] = str(getattr(self.account, "server", "CloudServer"))
        bot_state["login"] = str(getattr(self.account, "login", "Connected"))

        if self.account.state != "DEPLOYED":
            add_log("Deploying broker terminal container in cloud...")
            await self.account.deploy()

        add_log("Synchronizing real-time market stream...")
        await self.account.wait_connected()

        self.connection = self.account.get_rpc_connection()
        await self.connection.connect()
        await self.connection.wait_synchronized()

        await self.update_account_info()
        bot_state["is_connected"] = True
        bot_state["status_msg"] = f"Online ({bot_state['account_type']})"
        bot_state["last_error"] = ""
        add_log("✅ Live MT4/MT5 Broker Connection Established!")
        return True, "Connected successfully"

    async def update_account_info(self):
        if not self.connection:
            return
        try:
            info = await self.connection.get_account_information()
            bot_state["balance"] = float(info.get("balance", 0.0))
            bot_state["equity"] = float(info.get("equity", 0.0))
            bot_state["margin"] = float(info.get("margin", 0.0))
            bot_state["free_margin"] = float(info.get("freeMargin", 0.0))
            bot_state["profit"] = float(bot_state["equity"] - bot_state["balance"])

            # Live Open Positions Sync
            raw_positions = await self.connection.get_positions()
            formatted_pos = []
            for p in raw_positions:
                formatted_pos.append({
                    "id": p.get("id"),
                    "symbol": p.get("symbol"),
                    "type": p.get("type"),
                    "volume": p.get("volume"),
                    "openPrice": p.get("openPrice"),
                    "currentPrice": p.get("currentPrice"),
                    "profit": p.get("profit"),
                    "sl": p.get("stopLoss", 0.0),
                    "tp": p.get("takeProfit", 0.0)
                })
            bot_state["open_positions"] = formatted_pos
        except Exception as e:
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
                    add_log(f"Broker Symbol Auto-Resolved: '{requested_symbol}' → '{s}'")
                    return s
            return requested_symbol
        except Exception as e:
            return requested_symbol

    async def fetch_candles(self, symbol, timeframe, count=120):
        try:
            start_time = datetime.now(timezone.utc) - timedelta(days=3)
            
            tf_map = {"1m": "1m", "5m": "5m", "15m": "15m", "30m": "30m", "1h": "1h", "4h": "4h"}
            meta_tf = tf_map.get(timeframe, "15m")

            candles = None
            if hasattr(self.connection, 'get_candles'):
                candles = await self.connection.get_candles(symbol, meta_tf, start_time, count)
            elif hasattr(self.account, 'get_historical_market_data_client'):
                client = self.account.get_historical_market_data_client()
                candles = await client.get_historical_candles(symbol, meta_tf, start_time, count)

            if not candles:
                await self.connection.subscribe_to_market_data(symbol)
                await asyncio.sleep(2)
                if hasattr(self.connection, 'get_candles'):
                    candles = await self.connection.get_candles(symbol, meta_tf, start_time, count)

            if not candles:
                return None

            df = pd.DataFrame(candles)
            for col in ["open", "high", "low", "close"]:
                if col in df.columns:
                    df[col] = df[col].astype(float)
            return df
        except Exception as e:
            add_log(f"Candle Stream Error: {e}")
            return None

    async def execute_trade(self, action, symbol, lot, sl_pips, tp_pips):
        try:
            spec = await self.connection.get_symbol_specification(symbol)
            digits = spec.get("digits", 2)
            point = spec.get("point", 0.01)

            price_info = await self.connection.get_symbol_price(symbol)
            entry = price_info["ask"] if action == "BUY" else price_info["bid"]

            # Scale pips according to asset type
            pip_scale = 1.0 if "BTC" in symbol or "ETH" in symbol else (point if point > 0 else 0.0001)

            sl = entry - (sl_pips * pip_scale) if action == "BUY" else entry + (sl_pips * pip_scale)
            tp = entry + (tp_pips * pip_scale) if action == "BUY" else entry - (tp_pips * pip_scale)

            add_log(f"🚀 EXECUTING {action} ORDER on {symbol} | Lot: {lot} | Entry: {entry:.2f} | SL: {sl:.2f} | TP: {tp:.2f}")

            if action == "BUY":
                res = await self.connection.create_market_buy_order(symbol, lot, round(sl, digits), round(tp, digits))
            else:
                res = await self.connection.create_market_sell_order(symbol, lot, round(sl, digits), round(tp, digits))

            add_log(f"✅ TRADE EXECUTED SUCCESSFULLY! Ticket ID: {res.get('stringCode', 'CONFIRMED')}")
            await self.update_account_info()
        except Exception as e:
            add_log(f"❌ TRADE EXECUTION ERROR: {e}")

    async def close_position(self, position_id):
        try:
            add_log(f"Closing position #{position_id}...")
            await self.connection.close_position(position_id)
            add_log(f"✅ Position #{position_id} closed in profit/loss.")
            await self.update_account_info()
        except Exception as e:
            add_log(f"❌ Error closing position: {e}")

    async def run_analysis_and_trade_loop(self):
        add_log("🔍 Analyzing live market structure...")
        bot_state["actual_symbol"] = await self.resolve_symbol(bot_state["symbol"])
        symbol = bot_state["actual_symbol"]

        while bot_state["is_running"] and bot_state["is_connected"]:
            try:
                await self.update_account_info()
                timeframe = bot_state["timeframe"]
                df = await self.fetch_candles(symbol, timeframe)

                if df is not None and len(df) >= 35:
                    # High Precision Multi-Indicator Calculation
                    df["ema_f"] = calculate_ema(df["close"], 9)
                    df["ema_s"] = calculate_ema(df["close"], 21)
                    df["rsi"] = calculate_rsi(df["close"], 14)
                    _, _, df["macd_hist"] = calculate_macd(df["close"], 12, 26, 9)
                    df["atr"] = calculate_atr(df, 14)

                    price = df["close"].iloc[-1]
                    ema_f = df["ema_f"].iloc[-1]
                    ema_s = df["ema_s"].iloc[-1]
                    prev_f = df["ema_f"].iloc[-2]
                    prev_s = df["ema_s"].iloc[-2]
                    rsi = df["rsi"].iloc[-1]
                    macd_h = df["macd_hist"].iloc[-1]
                    atr = df["atr"].iloc[-1]

                    # Multi-Confirmation Strategy Engine
                    signal = "WAITING"
                    
                    # BUY Rules: EMA Bullish Cross + RSI Above 50 + MACD Histogram Positive
                    if (prev_f <= prev_s and ema_f > ema_s) or (ema_f > ema_s and rsi > 52 and macd_h > 0):
                        if rsi < 70:  # Avoid Overbought entries
                            signal = "BUY"

                    # SELL Rules: EMA Bearish Cross + RSI Below 50 + MACD Histogram Negative
                    elif (prev_f >= prev_s and ema_f < ema_s) or (ema_f < ema_s and rsi < 48 and macd_h < 0):
                        if rsi > 30:  # Avoid Oversold entries
                            signal = "SELL"

                    bot_state["last_analysis"] = {
                        "price": round(price, 2),
                        "ema_fast": round(ema_f, 2),
                        "ema_slow": round(ema_s, 2),
                        "rsi": round(rsi, 1),
                        "macd_hist": round(macd_h, 3),
                        "atr": round(atr, 2),
                        "signal": signal
                    }

                    add_log(f"📊 [{symbol} - {timeframe}] Price: {price:.2f} | RSI: {rsi:.1f} | MACD Hist: {macd_h:.2f} | Signal: {signal}")

                    # Position Execution Check
                    matching_positions = [p for p in bot_state["open_positions"] if p["symbol"] == symbol]
                    
                    if len(matching_positions) < bot_state["max_trades"] and signal in ["BUY", "SELL"]:
                        add_log(f"🎯 High Probability Entry Signal Detected ({signal}) on {symbol}!")
                        await self.execute_trade(
                            signal,
                            symbol,
                            bot_state["lot_size"],
                            bot_state["stop_loss_pips"],
                            bot_state["take_profit_pips"]
                        )
                    elif len(matching_positions) >= bot_state["max_trades"]:
                        add_log(f"Max trades active ({len(matching_positions)}/{bot_state['max_trades']}). Monitoring positions...")

                else:
                    add_log(f"Gathering candle history for {symbol}...")

            except Exception as e:
                add_log(f"Analysis Loop Warning: {e}")

            await asyncio.sleep(12)

        add_log("Trading & Analysis Loop stopped.")

engine = MetaApiEngine()

# ================= EMBEDDED DASHBOARD UI =================
HTML_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Cloud Automated MT4/MT5 Overlay Terminal</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
<script type="text/javascript" src="https://s3.tradingview.com/tv.js"></script>
<style>
body { background-color: #080b10; color: #adbac7; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; }
.card { background-color: #121721; border: 1px solid #232a35; border-radius: 10px; margin-bottom: 12px; }
.nav-pills .nav-link { color: #768390; font-weight: 600; font-size: 14px; }
.nav-pills .nav-link.active { background-color: #1f6feb; color: #fff; }
.form-control, .form-select { background-color: #0b0e14; border: 1px solid #232a35; color: #e6edf3; font-size: 14px; }
.form-control:focus, .form-select:focus { background-color: #0b0e14; color: #fff; border-color: #1f6feb; box-shadow: none; }
.btn-success { background-color: #238636; border: none; font-weight: 600; }
.btn-danger { background-color: #da3633; border: none; font-weight: 600; }
.status-badge { font-weight: 700; padding: 6px 12px; border-radius: 20px; font-size: 12px; }
.bg-online { background-color: rgba(46, 160, 67, 0.15); color: #3fb950; border: 1px solid #238636; }
.bg-offline { background-color: rgba(248, 81, 73, 0.15); color: #f85149; border: 1px solid #da3633; }
.metric-title { font-size: 11px; text-transform: uppercase; color: #768390; letter-spacing: 0.5px; }
.metric-value { font-size: 18px; font-weight: 700; color: #fff; }
.log-box { background-color: #05070a; border: 1px solid #232a35; height: 200px; overflow-y: auto; font-family: monospace; font-size: 11px; padding: 10px; border-radius: 6px; color: #3fb950; }
.chart-container { height: 380px; width: 100%; border-radius: 8px; overflow: hidden; }
</style>
</head>
<body class="p-2 p-md-3">
<div class="container-fluid" style="max-width: 1100px;">

    <!-- TOP HEADER -->
    <div class="d-flex justify-content-between align-items-center mb-3 card p-3">
        <div>
            <h5 class="m-0 text-white font-weight-bold">🤖 MT4/MT5 Cloud Overlay Terminal</h5>
            <small class="text-muted" id="accountSub">Not Connected</small>
        </div>
        <div>
            <span id="statusBadge" class="status-badge bg-offline">● Offline</span>
        </div>
    </div>

    <!-- METRICS DISPLAY -->
    <div class="row g-2 mb-3">
        <div class="col-6 col-md-3">
            <div class="card p-2 text-center">
                <span class="metric-title">Balance</span>
                <div class="metric-value text-info" id="balVal">$0.00</div>
            </div>
        </div>
        <div class="col-6 col-md-3">
            <div class="card p-2 text-center">
                <span class="metric-title">Equity</span>
                <div class="metric-value text-warning" id="eqVal">$0.00</div>
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
                <span class="metric-title">Free Margin</span>
                <div class="metric-value text-light" id="marginVal">$0.00</div>
            </div>
        </div>
    </div>

    <!-- NAVIGATION TABS -->
    <ul class="nav nav-pills mb-3">
        <li class="nav-item"><button class="nav-link active" data-tab="live">📊 Live Chart & Terminal</button></li>
        <li class="nav-item"><button class="nav-link" data-tab="strategy">⚙️ Pair & Strategy</button></li>
        <li class="nav-item"><button class="nav-link" data-tab="connect">🔗 Broker Connection</button></li>
    </ul>

    <!-- TAB 1: LIVE CHART & TERMINAL -->
    <div id="tab-live" class="tab-pane">
        <div class="card p-2 mb-3">
            <div id="tv_chart_container" class="chart-container"></div>
        </div>

        <!-- POSITIONS TABLE -->
        <div class="card p-3 mb-3">
            <div class="d-flex justify-content-between align-items-center mb-2">
                <h6 class="text-white m-0">Live Active Positions</h6>
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
                            <th>Entry</th>
                            <th>Current</th>
                            <th>Profit</th>
                            <th>Action</th>
                        </tr>
                    </thead>
                    <tbody id="posTable">
                        <tr><td colspan="7" class="text-center text-muted">No open positions</td></tr>
                    </tbody>
                </table>
            </div>
        </div>
    </div>

    <!-- TAB 2: PAIR & STRATEGY SETTINGS -->
    <div id="tab-strategy" class="tab-pane" style="display:none;">
        <div class="card p-3">
            <h6 class="text-white mb-3">Trading Pair & Risk Management</h6>
            <div class="row g-3">
                <div class="col-md-6">
                    <label class="form-label">Symbol Select</label>
                    <select id="symbolSelect" class="form-select" onchange="updateTradingViewChart()">
                        <optgroup label="24/7 Crypto Pairs">
                            <option value="BTCUSD" selected>BTCUSD (Bitcoin)</option>
                            <option value="ETHUSD">ETHUSD (Ethereum)</option>
                            <option value="SOLUSD">SOLUSD (Solana)</option>
                            <option value="XRPUSD">XRPUSD (Ripple)</option>
                        </optgroup>
                        <optgroup label="Forex & Gold">
                            <option value="EURUSD">EURUSD</option>
                            <option value="GBPUSD">GBPUSD</option>
                            <option value="XAUUSD">XAUUSD (Gold)</option>
                        </optgroup>
                    </select>
                </div>
                <div class="col-md-6">
                    <label class="form-label">Timeframe</label>
                    <select id="tfSelect" class="form-select" onchange="updateTradingViewChart()">
                        <option value="1m">M1</option>
                        <option value="5m">M5</option>
                        <option value="15m" selected>M15</option>
                        <option value="1h">H1</option>
                    </select>
                </div>
                <div class="col-6 col-md-3">
                    <label class="form-label">Lot Size</label>
                    <input type="number" id="lotInput" class="form-control" value="0.01" step="0.01">
                </div>
                <div class="col-6 col-md-3">
                    <label class="form-label">Max Trades</label>
                    <input type="number" id="maxTradesInput" class="form-control" value="1">
                </div>
                <div class="col-6 col-md-3">
                    <label class="form-label">Stop Loss (Pips/Points)</label>
                    <input type="number" id="slInput" class="form-control" value="2000">
                </div>
                <div class="col-6 col-md-3">
                    <label class="form-label">Take Profit (Pips/Points)</label>
                    <input type="number" id="tpInput" class="form-control" value="4000">
                </div>
            </div>
        </div>
    </div>

    <!-- TAB 3: BROKER CONNECTION -->
    <div id="tab-connect" class="tab-pane" style="display:none;">
        <div class="card p-3">
            <h6 class="text-white mb-3">Connect MT4 / MT5 Account</h6>
            <div class="mb-3">
                <label class="form-label">MetaApi Access Token</label>
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
                    <input type="text" id="accIdInput" class="form-control" placeholder="e.g. b5978e20-507a-4b9a-b1bf-aca87dcfec95">
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
        <h6 class="text-white mb-2">Live Strategy Engine Console</h6>
        <div id="logBox" class="log-box"></div>
    </div>

</div>

<script>
let tvWidget = null;

function initTradingViewChart(symbol, timeframe) {
    let tvSymbol = "BINANCE:BTCUSDT";
    if(symbol.includes("ETH")) tvSymbol = "BINANCE:ETHUSDT";
    else if(symbol.includes("SOL")) tvSymbol = "BINANCE:SOLUSDT";
    else if(symbol.includes("XRP")) tvSymbol = "BINANCE:XRPUSDT";
    else if(symbol.includes("EUR")) tvSymbol = "FX:EURUSD";
    else if(symbol.includes("GBP")) tvSymbol = "FX:GBPUSD";
    else if(symbol.includes("XAU")) tvSymbol = "OANDA:XAUUSD";

    let tvInterval = "15";
    if(timeframe === "1m") tvInterval = "1";
    if(timeframe === "5m") tvInterval = "5";
    if(timeframe === "1h") tvInterval = "60";

    document.getElementById("tv_chart_container").innerHTML = "";
    tvWidget = new TradingView.widget({
        "autosize": true,
        "symbol": tvSymbol,
        "interval": tvInterval,
        "timezone": "Etc/UTC",
        "theme": "dark",
        "style": "1",
        "locale": "en",
        "toolbar_bg": "#f1f3f6",
        "enable_publishing": false,
        "hide_top_toolbar": false,
        "save_image": false,
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

        // Status Badge & Header
        const badge = document.getElementById('statusBadge');
        badge.textContent = d.is_connected ? '● ' + d.status_msg : '● Offline';
        badge.className = d.is_connected ? 'status-badge bg-online' : 'status-badge bg-offline';
        document.getElementById('accountSub').textContent = d.is_connected ? `Server: ${d.server} | Login: ${d.login}` : 'Not Connected';

        // Account Metrics
        document.getElementById('balVal').textContent = '$' + d.balance.toFixed(2);
        document.getElementById('eqVal').textContent = '$' + d.equity.toFixed(2);
        document.getElementById('marginVal').textContent = '$' + d.free_margin.toFixed(2);
        
        const plElem = document.getElementById('plVal');
        plElem.textContent = (d.profit >= 0 ? '+$' : '-$') + Math.abs(d.profit).toFixed(2);
        plElem.className = d.profit >= 0 ? 'metric-value text-success' : 'metric-value text-danger';

        // Logs
        const logBox = document.getElementById('logBox');
        logBox.innerHTML = d.logs.join('<br>');
        logBox.scrollTop = logBox.scrollHeight;

        // Active Positions Table
        const posTable = document.getElementById('posTable');
        if(d.open_positions && d.open_positions.length > 0) {
            let html = '';
            d.open_positions.forEach(p => {
                const profitClass = p.profit >= 0 ? 'text-success font-weight-bold' : 'text-danger font-weight-bold';
                html += `<tr>
                    <td>${p.symbol}</td>
                    <td><span class="badge ${p.type === 'ORDER_TYPE_BUY' ? 'bg-success' : 'bg-danger'}">${p.type.replace('ORDER_TYPE_','')}</span></td>
                    <td>${p.volume}</td>
                    <td>${p.openPrice}</td>
                    <td>${p.currentPrice}</td>
                    <td class="${profitClass}">$${p.profit.toFixed(2)}</td>
                    <td><button onclick="closePosition('${p.id}')" class="btn btn-danger btn-sm py-0">Close</button></td>
                </tr>`;
            });
            posTable.innerHTML = html;
        } else {
            posTable.innerHTML = `<tr><td colspan="7" class="text-center text-muted">No open positions</td></tr>`;
        }

        // Run/Stop Buttons
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
        stop_loss_pips: parseInt(document.getElementById('slInput').value),
        take_profit_pips: parseInt(document.getElementById('tpInput').value)
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
    if(confirm('Close position #' + posId + ' now?')) {
        await fetch('/api/close_position', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({position_id: posId})
        });
    }
}

window.onload = () => {
    initTradingViewChart("BTCUSD", "15m");
    setInterval(refreshUI, 2500);
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
        "timeframe": data.get("timeframe", "15m"),
        "lot_size": float(data.get("lot_size", 0.01)),
        "max_trades": int(data.get("max_trades", 1)),
        "stop_loss_pips": int(data.get("stop_loss_pips", 2000)),
        "take_profit_pips": int(data.get("take_profit_pips", 4000)),
        "is_running": True
    })
    asyncio.run_coroutine_threadsafe(engine.run_analysis_and_trade_loop(), bg_loop)
    return jsonify(status="started")

@app.route("/api/stop", methods=["POST"])
def api_stop():
    bot_state["is_running"] = False
    add_log("Trading loop stop requested.")
    return jsonify(status="stopped")

@app.route("/api/close_position", methods=["POST"])
def api_close_position():
    data = request.json or {}
    pos_id = data.get("position_id")
    if pos_id and bot_state["is_connected"]:
        asyncio.run_coroutine_threadsafe(engine.close_position(pos_id), bg_loop)
        return jsonify(status="closing_initiated")
    return jsonify(status="failed")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
