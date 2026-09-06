import os
import asyncio
import threading
import time
import logging
from datetime import datetime, timezone
from flask import Flask, render_template_string, request, jsonify
from metaapi_cloud_sdk import MetaApi
import pandas as pd
import numpy as np

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)

# Dedicated Event Loop Thread Management to fix "no running event loop"
bg_loop = None

def start_background_loop():
    global bg_loop
    bg_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(bg_loop)
    bg_loop.run_forever()

# Start background loop on startup
loop_thread = threading.Thread(target=start_background_loop, daemon=True)
loop_thread.start()

# Global Bot State
bot_state = {
    "is_connected": False,
    "is_running": False,
    "api_token": "",
    "platform": "mt5",
    "login": "",
    "password": "",
    "server": "",
    "account_id": "",
    "account_type": "UNKNOWN",
    "symbol": "BTCUSD",
    "actual_symbol": "BTCUSD",
    "timeframe": "15m",
    "lot_size": 0.01,
    "max_trades": 1,
    "stop_loss_pips": 200,
    "take_profit_pips": 400,
    "balance": 0.0,
    "equity": 0.0,
    "logs": ["Bot ready. Enter credentials to connect."],
    "last_signal": "NONE",
    "status_msg": "Offline"
}

def add_log(msg):
    timestamp = datetime.now().strftime("%H:%M:%S")
    log_entry = f"[{timestamp}] {msg}"
    bot_state["logs"].append(log_entry)
    if len(bot_state["logs"]) > 60:
        bot_state["logs"].pop(0)
    logging.info(msg)

# ==================== INDICATORS ====================
def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def calculate_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

# ==================== BOT ENGINE ====================
class MetaApiEngine:
    def __init__(self):
        self.api = None
        self.account = None
        self.connection = None

    async def connect_account(self, token, platform, login, password, server):
        try:
            bot_state["status_msg"] = "Connecting..."
            add_log(f"Initializing MetaApi with Server: {server}, Login: {login}...")
            self.api = MetaApi(token)
            
            # Check if account already provisioned on MetaApi
            accounts = await self.api.metatrader_account_api.get_accounts()
            target_account = None
            
            for acc in accounts:
                if str(acc.login) == str(login) and str(acc.server).lower() == str(server).lower():
                    target_account = acc
                    break

            # If not provisioned, create it automatically
            if not target_account:
                add_log(f"Provisioning new {platform.upper()} account on MetaApi...")
                target_account = await self.api.metatrader_account_api.create_account({
                    'name': f'Bot-{login}',
                    'type': 'cloud',
                    'login': str(login),
                    'password': str(password),
                    'server': str(server),
                    'platform': 'mt5' if '5' in platform.lower() else 'mt4',
                    'magic': 100001
                })

            bot_state["account_id"] = target_account.id
            self.account = target_account

            # Auto Detect Real vs Demo
            bot_state["account_type"] = str(target_account.type).upper()
            add_log(f"Account Type: {bot_state['account_type']} | State: {target_account.state}")

            if target_account.state != 'DEPLOYED':
                add_log("Deploying terminal container... Please wait 10-20 seconds...")
                await target_account.deploy()

            add_log("Waiting for connection to broker server...")
            await target_account.wait_connected()

            self.connection = target_account.get_rpc_connection()
            await self.connection.connect()
            await self.connection.wait_synchronized()

            bot_state["is_connected"] = True
            bot_state["status_msg"] = "Online (Connected)"
            add_log("SUCCESS: MT4/MT5 Connected & Synchronized!")
            
            await self.update_account_info()
            return True, "Connected successfully"

        except Exception as e:
            error_msg = str(e)
            bot_state["is_connected"] = False
            bot_state["status_msg"] = "Connection Failed"
            add_log(f"Connection Error: {error_msg}")
            return False, error_msg

    async def update_account_info(self):
        if not self.connection:
            return
        try:
            info = await self.connection.get_account_information()
            bot_state["balance"] = float(info.get("balance", 0.0))
            bot_state["equity"] = float(info.get("equity", 0.0))
        except Exception as e:
            pass

    async def resolve_broker_symbol(self, requested_symbol):
        """ Auto-resolves symbols like BTCUSD -> BTCUSD.m / BTCUSDm / BTC/USD """
        try:
            symbols = await self.connection.get_symbols()
            if requested_symbol in symbols:
                return requested_symbol
            
            clean_req = requested_symbol.replace("/", "").replace("_", "").upper()
            for s in symbols:
                clean_s = s.replace("/", "").replace("_", "").replace(".", "").upper()
                if clean_req in clean_s or clean_s in clean_req:
                    add_log(f"Resolved Symbol: '{requested_symbol}' -> Broker Symbol: '{s}'")
                    return s
            return requested_symbol
        except Exception as e:
            return requested_symbol

    async def fetch_candles(self, symbol, timeframe, count=100):
        try:
            tf_map = {"1m": "1m", "5m": "5m", "15m": "15m", "30m": "30m", "1h": "1h", "4h": "4h", "1d": "1d"}
            meta_tf = tf_map.get(timeframe, "15m")

            candles = await self.connection.get_historical_candles(symbol, meta_tf, datetime.now(timezone.utc), count)
            if not candles or len(candles) == 0:
                await self.connection.subscribe_to_market_data(symbol)
                await asyncio.sleep(2)
                candles = await self.connection.get_historical_candles(symbol, meta_tf, datetime.now(timezone.utc), count)

            if not candles:
                add_log(f"No candle data for {symbol}. Ensure symbol is active in broker market watch.")
                return None

            df = pd.DataFrame(candles)
            df['close'] = df['close'].astype(float)
            df['high'] = df['high'].astype(float)
            df['low'] = df['low'].astype(float)
            df['open'] = df['open'].astype(float)
            return df
        except Exception as e:
            add_log(f"Candle Fetch Error ({symbol}): {e}")
            return None

    async def get_open_positions(self, symbol):
        try:
            positions = await self.connection.get_positions()
            return [p for p in positions if p.get('symbol') == symbol]
        except Exception as e:
            return []

    async def execute_trade(self, action, symbol, lot_size, sl_pips, tp_pips):
        try:
            spec = await self.connection.get_symbol_specification(symbol)
            digits = spec.get('digits', 2)
            point = spec.get('point', 0.01)

            tick = await self.connection.get_symbol_price(symbol)
            current_price = tick['ask'] if action == "BUY" else tick['bid']

            sl_price = current_price - (sl_pips * point) if action == "BUY" else current_price + (sl_pips * point)
            tp_price = current_price + (tp_pips * point) if action == "BUY" else current_price - (tp_pips * point)

            add_log(f"Executing {action} on {symbol} @ {current_price} | Lot: {lot_size}")
            
            if action == "BUY":
                res = await self.connection.create_market_buy_order(symbol, lot_size, round(sl_price, digits), round(tp_price, digits))
            else:
                res = await self.connection.create_market_sell_order(symbol, lot_size, round(sl_price, digits), round(tp_price, digits))

            add_log(f"Trade Success! Order ID: {res.get('stringCode', 'EXECUTED')}")
        except Exception as e:
            add_log(f"Execution Error: {e}")

    async def trade_loop(self):
        add_log("Automated Trading Loop Started.")
        resolved_sym = await self.resolve_broker_symbol(bot_state["symbol"])
        bot_state["actual_symbol"] = resolved_sym

        while bot_state["is_running"] and bot_state["is_connected"]:
            try:
                await self.update_account_info()
                symbol = bot_state["actual_symbol"]
                timeframe = bot_state["timeframe"]

                df = await self.fetch_candles(symbol, timeframe)
                if df is not None and len(df) > 25:
                    df['ema_fast'] = calculate_ema(df['close'], 9)
                    df['ema_slow'] = calculate_ema(df['close'], 21)
                    df['rsi'] = calculate_rsi(df['close'], 14)

                    latest_rsi = df['rsi'].iloc[-1]
                    fast_ema = df['ema_fast'].iloc[-1]
                    slow_ema = df['ema_slow'].iloc[-1]
                    prev_fast = df['ema_fast'].iloc[-2]
                    prev_slow = df['ema_slow'].iloc[-2]

                    signal = "NONE"
                    if prev_fast <= prev_slow and fast_ema > slow_ema and latest_rsi > 45:
                        signal = "BUY"
                    elif prev_fast >= prev_slow and fast_ema < slow_ema and latest_rsi < 55:
                        signal = "SELL"

                    bot_state["last_signal"] = f"{signal} (RSI: {latest_rsi:.1f})"

                    positions = await self.get_open_positions(symbol)
                    if len(positions) < bot_state["max_trades"] and signal in ["BUY", "SELL"]:
                        add_log(f"Strategy Triggered: {signal} on {symbol}")
                        await self.execute_trade(signal, symbol, bot_state["lot_size"], bot_state["stop_loss_pips"], bot_state["take_profit_pips"])

            except Exception as e:
                add_log(f"Loop Error: {e}")

            await asyncio.sleep(12)

        add_log("Trading Loop Stopped.")

engine = MetaApiEngine()

# ==================== WEB DASHBOARD UI ====================
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Cloud Trading Bot</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        body { background-color: #0b0e14; color: #adbac7; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; }
        .card { background-color: #151b23; border: 1px solid #2d333b; border-radius: 10px; margin-bottom: 16px; }
        .nav-tabs { border-bottom: 1px solid #2d333b; }
        .nav-tabs .nav-link { color: #768390; border: none; font-weight: 600; padding: 10px 18px; }
        .nav-tabs .nav-link.active { color: #539bf5; background-color: #151b23; border-bottom: 2px solid #539bf5; }
        .form-control, .form-select { background-color: #0d1117; border: 1px solid #2d333b; color: #adbac7; }
        .form-control:focus, .form-select:focus { background-color: #0d1117; color: #fff; border-color: #539bf5; box-shadow: none; }
        .btn-primary { background-color: #347d39; border: none; font-weight: 600; }
        .btn-primary:hover { background-color: #46954a; }
        .btn-danger { background-color: #c93c37; border: none; font-weight: 600; }
        .status-dot { height: 10px; width: 10px; border-radius: 50%; display: inline-block; margin-right: 6px; }
        .dot-online { background-color: #57ab5a; }
        .dot-offline { background-color: #c93c37; }
        .log-box { background-color: #0d1117; border: 1px solid #2d333b; height: 220px; overflow-y: auto; font-family: monospace; font-size: 12px; padding: 10px; border-radius: 6px; color: #57ab5a; }
    </style>
</head>
<body class="p-2 p-md-4">
    <div class="container-fluid" style="max-width: 800px;">
        <div class="d-flex justify-content-between align-items-center mb-3">
            <h4 class="m-0 text-white">🤖 Cloud Trading Bot</h4>
            <div id="statusBadge" class="badge bg-dark border border-secondary p-2">
                <span id="statusDot" class="status-dot dot-offline"></span>
                <span id="statusText">Offline</span>
            </div>
        </div>

        <!-- Navigation Tabs -->
        <ul class="nav nav-tabs mb-3" id="botTabs">
            <li class="nav-item">
                <button class="nav-link active" onclick="switchTab('connect')">🔗 Connect</button>
            </li>
            <li class="nav-item">
                <button class="nav-link" onclick="switchTab('strategy')">⚙️ Strategy & Risk</button>
            </li>
            <li class="nav-item">
                <button class="nav-link" onclick="switchTab('trade')">📊 Live Trading</button>
            </li>
        </ul>

        <!-- TAB 1: CONNECT -->
        <div id="tab-connect" class="tab-content">
            <div class="card p-3">
                <h6 class="text-white mb-3">1. METAAPI CLOUD TOKEN</h6>
                <div class="mb-3">
                    <input type="password" id="apiToken" class="form-control" placeholder="Paste MetaApi Token">
                    <small class="text-muted">Get your free token from app.metaapi.cloud</small>
                </div>

                <h6 class="text-white mb-3 mt-2">2. BROKER CREDENTIALS</h6>
                <div class="row g-3">
                    <div class="col-12">
                        <label class="form-label">Platform</label>
                        <select id="platform" class="form-select">
                            <option value="mt5" selected>MetaTrader 5 (MT5)</option>
                            <option value="mt4">MetaTrader 4 (MT4)</option>
                        </select>
                    </div>
                    <div class="col-12">
                        <label class="form-label">Account Login</label>
                        <input type="text" id="login" class="form-control" placeholder="e.g. 476924559">
                    </div>
                    <div class="col-12">
                        <label class="form-label">Password</label>
                        <input type="password" id="password" class="form-control" placeholder="Trading Password">
                    </div>
                    <div class="col-12">
                        <label class="form-label">Server</label>
                        <input type="text" id="server" class="form-control" placeholder="e.g. Exness-MT5Trial9">
                    </div>
                </div>

                <button id="connectBtn" onclick="connectAccount()" class="btn btn-primary w-100 mt-4 p-2">Connect Broker Account</button>
            </div>
        </div>

        <!-- TAB 2: STRATEGY -->
        <div id="tab-strategy" class="tab-content d-none">
            <div class="card p-3">
                <h6 class="text-white mb-3">PAIR & WEEKEND TRADING</h6>
                <div class="row g-3">
                    <div class="col-md-6">
                        <label class="form-label">Currency / Crypto Pair</label>
                        <select id="symbol" class="form-select">
                            <optgroup label="24/7 Weekend Crypto Pairs">
                                <option value="BTCUSD" selected>BTCUSD (Bitcoin)</option>
                                <option value="ETHUSD">ETHUSD (Ethereum)</option>
                                <option value="SOLUSD">SOLUSD (Solana)</option>
                                <option value="XRPUSD">XRPUSD (Ripple)</option>
                                <option value="DOGEUSD">DOGEUSD (Dogecoin)</option>
                            </optgroup>
                            <optgroup label="Forex & Metals (Mon-Fri)">
                                <option value="EURUSD">EURUSD</option>
                                <option value="GBPUSD">GBPUSD</option>
                                <option value="XAUUSD">XAUUSD (Gold)</option>
                            </optgroup>
                        </select>
                    </div>
                    <div class="col-md-6">
                        <label class="form-label">Timeframe</label>
                        <select id="timeframe" class="form-select">
                            <option value="1m">M1</option>
                            <option value="5m">M5</option>
                            <option value="15m" selected>M15</option>
                            <option value="1h">H1</option>
                        </select>
                    </div>
                    <div class="col-6">
                        <label class="form-label">Lot Size</label>
                        <input type="number" step="0.01" id="lotSize" class="form-control" value="0.01">
                    </div>
                    <div class="col-6">
                        <label class="form-label">Max Active Trades</label>
                        <input type="number" id="maxTrades" class="form-control" value="1">
                    </div>
                    <div class="col-6">
                        <label class="form-label">Stop Loss (Pips/Points)</label>
                        <input type="number" id="slPips" class="form-control" value="200">
                    </div>
                    <div class="col-6">
                        <label class="form-label">Take Profit (Pips/Points)</label>
                        <input type="number" id="tpPips" class="form-control" value="400">
                    </div>
                </div>
            </div>
        </div>

        <!-- TAB 3: LIVE TRADE -->
        <div id="tab-trade" class="tab-content d-none">
            <div class="card p-3">
                <div class="row text-center mb-3">
                    <div class="col-6">
                        <small class="text-muted">Balance</small>
                        <h4 id="balanceText" class="text-white">$0.00</h4>
                    </div>
                    <div class="col-6">
                        <small class="text-muted">Equity</small>
                        <h4 id="equityText" class="text-warning">$0.00</h4>
                    </div>
                </div>

                <div class="d-flex gap-2">
                    <button id="runBtn" onclick="startBot()" class="btn btn-primary w-50 p-2">▶ RUN BOT</button>
                    <button id="stopBtn" onclick="stopBot()" class="btn btn-danger w-50 p-2" disabled>⏹ STOP</button>
                </div>
            </div>
        </div>

        <!-- LIVE LOGS PANEL -->
        <div class="card p-3">
            <h6 class="text-white mb-2">Live Console Logs</h6>
            <div id="logBox" class="log-box"></div>
        </div>
    </div>

    <script>
        function switchTab(tabName) {
            document.querySelectorAll('.tab-content').forEach(el => el.classList.add('d-none'));
            document.querySelectorAll('.nav-link').forEach(el => el.classList.remove('active'));
            document.getElementById('tab-' + tabName).classList.remove('d-none');
            event.target.classList.add('active');
        }

        async function updateUI() {
            try {
                const res = await fetch('/api/status');
                const data = await res.json();

                document.getElementById('statusText').innerText = data.status_msg;
                const dot = document.getElementById('statusDot');
                dot.className = data.is_connected ? 'status-dot dot-online' : 'status-dot dot-offline';

                document.getElementById('balanceText').innerText = '$' + data.balance.toFixed(2);
                document.getElementById('equityText').innerText = '$' + data.equity.toFixed(2);

                const logBox = document.getElementById('logBox');
                logBox.innerHTML = data.logs.join('<br>');
                logBox.scrollTop = logBox.scrollHeight;

                if(data.is_running) {
                    document.getElementById('runBtn').disabled = true;
                    document.getElementById('stopBtn').disabled = false;
                } else {
                    document.getElementById('runBtn').disabled = false;
                    document.getElementById('stopBtn').disabled = true;
                }
            } catch(e) {}
        }

        async function connectAccount() {
            const btn = document.getElementById('connectBtn');
            btn.innerText = "Connecting...";
            btn.disabled = true;

            const payload = {
                api_token: document.getElementById('apiToken').value,
                platform: document.getElementById('platform').value,
                login: document.getElementById('login').value,
                password: document.getElementById('password').value,
                server: document.getElementById('server').value
            };

            const res = await fetch('/api/connect', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(payload)
            });
            const data = await res.json();
            btn.innerText = "Connect Broker Account";
            btn.disabled = false;

            if(data.success) {
                alert("Connected successfully!");
                switchTab('strategy');
            } else {
                alert("Connection failed: " + data.message);
            }
        }

        async function startBot() {
            const config = {
                symbol: document.getElementById('symbol').value,
                timeframe: document.getElementById('timeframe').value,
                lot_size: parseFloat(document.getElementById('lotSize').value),
                max_trades: parseInt(document.getElementById('maxTrades').value),
                stop_loss_pips: parseInt(document.getElementById('slPips').value),
                take_profit_pips: parseInt(document.getElementById('tpPips').value)
            };

            await fetch('/api/start', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(config)
            });
        }

        async function stopBot() {
            await fetch('/api/stop', {method: 'POST'});
        }

        setInterval(updateUI, 2500);
    </script>
</body>
</html>
"""

# ==================== FLASK API ROUTES ====================
@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/api/status', methods=['GET'])
def get_status():
    return jsonify(bot_state)

@app.route('/api/connect', methods=['POST'])
def api_connect():
    data = request.json
    token = data.get("api_token")
    platform = data.get("platform", "mt5")
    login = data.get("login")
    password = data.get("password")
    server = data.get("server")

    if not token or not login or not password or not server:
        return jsonify({"success": False, "message": "All fields are required"})

    bot_state["api_token"] = token
    bot_state["platform"] = platform
    bot_state["login"] = login
    bot_state["password"] = password
    bot_state["server"] = server

    # Safely schedule coroutine in background loop
    future = asyncio.run_coroutine_threadsafe(
        engine.connect_account(token, platform, login, password, server), 
        bg_loop
    )
    
    try:
        success, msg = future.result(timeout=60)
        return jsonify({"success": success, "message": msg})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})

@app.route('/api/start', methods=['POST'])
def api_start():
    if not bot_state["is_connected"]:
        return jsonify({"status": "not connected"})

    data = request.json
    bot_state["symbol"] = data.get("symbol", "BTCUSD").upper()
    bot_state["timeframe"] = data.get("timeframe", "15m")
    bot_state["lot_size"] = float(data.get("lot_size", 0.01))
    bot_state["max_trades"] = int(data.get("max_trades", 1))
    bot_state["stop_loss_pips"] = int(data.get("slPips", 200))
    bot_state["take_profit_pips"] = int(data.get("tpPips", 400))
    
    bot_state["is_running"] = True
    asyncio.run_coroutine_threadsafe(engine.trade_loop(), bg_loop)
    return jsonify({"status": "started"})

@app.route('/api/stop', methods=['POST'])
def api_stop():
    bot_state["is_running"] = False
    add_log("Bot Stop Requested.")
    return jsonify({"status": "stopped"})

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
