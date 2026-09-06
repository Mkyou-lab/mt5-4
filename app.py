import os
import asyncio
import threading
import time
import logging
from datetime import datetime, timedelta, timezone
from flask import Flask, render_template_string, request, jsonify
from metaapi_cloud_sdk import MetaApi
import pandas as pd
import numpy as np

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)

# Global Bot State
bot_state = {
    "is_running": False,
    "api_token": "",
    "account_id": "",
    "symbol": "BTCUSD",
    "actual_symbol": "BTCUSD",
    "timeframe": "15m",
    "lot_size": 0.01,
    "max_trades": 1,
    "strategy": "EMA_RSI",
    "stop_loss_pips": 200,
    "take_profit_pips": 400,
    "account_type": "UNKNOWN",
    "balance": 0.0,
    "equity": 0.0,
    "logs": [],
    "last_signal": "NONE"
}

def add_log(msg):
    timestamp = datetime.now().strftime("%H:%M:%S")
    log_entry = f"[{timestamp}] {msg}"
    bot_state["logs"].append(log_entry)
    if len(bot_state["logs"]) > 50:
        bot_state["logs"].pop(0)
    logging.info(msg)

# ==================== TECHNICAL INDICATORS ====================
def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def calculate_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

# ==================== METAAPI BOT ENGINE ====================
class TradingBotEngine:
    def __init__(self, token, account_id):
        self.api = MetaApi(token)
        self.account_id = account_id
        self.account = None
        self.connection = None

    async def initialize(self):
        add_log("Connecting to MetaApi Account...")
        self.account = await self.api.metatrader_account_api.get_account(self.account_id)
        
        # Auto Detect Real vs Demo
        bot_state["account_type"] = str(self.account.type).upper()
        add_log(f"Account Detected: {bot_state['account_type']} | Server: {self.account.server}")
        
        # Ensure account is deployed
        if self.account.state != 'DEPLOYED':
            add_log("Deploying account terminal...")
            await self.account.deploy()
            
        await self.account.wait_connected()
        self.connection = self.account.get_rpc_connection()
        await self.connection.connect()
        await self.connection.wait_synchronized()
        add_log("MT4/MT5 Connection Synchronized & Ready!")

    async def resolve_broker_symbol(self, requested_symbol):
        """ Fixes 'No candle data for BTCUSD' by finding the broker's exact symbol name """
        try:
            symbols = await self.connection.get_symbols()
            # Direct match
            if requested_symbol in symbols:
                return requested_symbol
            
            # Search for variations (e.g., BTCUSD.m, BTCUSDm, BTC/USD, BTCUSD_i)
            clean_req = requested_symbol.replace("/", "").replace("_", "").upper()
            for s in symbols:
                clean_s = s.replace("/", "").replace("_", "").replace(".", "").upper()
                if clean_req in clean_s or clean_s in clean_req:
                    add_log(f"Symbol Auto-Resolved: '{requested_symbol}' -> Broker Name: '{s}'")
                    return s
            
            add_log(f"Warning: Exact symbol '{requested_symbol}' not found. Using default.")
            return requested_symbol
        except Exception as e:
            add_log(f"Error resolving symbol: {e}")
            return requested_symbol

    async def fetch_candles(self, symbol, timeframe, count=100):
        try:
            # Timeframe mapping
            tf_map = {"1m": "1m", "5m": "5m", "15m": "15m", "30m": "30m", "1h": "1h", "4h": "4h", "1d": "1d"}
            meta_tf = tf_map.get(timeframe, "15m")

            candles = await self.connection.get_historical_candles(symbol, meta_tf, datetime.now(timezone.utc), count)
            if not candles or len(candles) == 0:
                add_log(f"No candle data returned for {symbol}. Attempting market data subscription...")
                await self.connection.subscribe_to_market_data(symbol)
                await asyncio.sleep(2)
                candles = await self.connection.get_historical_candles(symbol, meta_tf, datetime.now(timezone.utc), count)

            if not candles:
                add_log(f"ERROR: Still no candle data for {symbol}.")
                return None

            df = pd.DataFrame(candles)
            df['close'] = df['close'].astype(float)
            df['high'] = df['high'].astype(float)
            df['low'] = df['low'].astype(float)
            df['open'] = df['open'].astype(float)
            return df
        except Exception as e:
            add_log(f"Candle Fetch Error: {str(e)}")
            return None

    async def update_account_metrics(self):
        try:
            account_info = await self.connection.get_account_information()
            bot_state["balance"] = float(account_info.get("balance", 0.0))
            bot_state["equity"] = float(account_info.get("equity", 0.0))
        except Exception as e:
            add_log(f"Account Info Fetch Error: {e}")

    async def get_open_positions(self, symbol):
        try:
            positions = await self.connection.get_positions()
            return [p for p in positions if p.get('symbol') == symbol]
        except Exception as e:
            add_log(f"Position Check Error: {e}")
            return []

    async def execute_trade(self, action, symbol, lot_size, sl_pips, tp_pips):
        try:
            price_info = await self.connection.get_symbol_specification(symbol)
            digits = price_info.get('digits', 2)
            point = price_info.get('point', 0.01)

            # Get current prices
            tick = await self.connection.get_symbol_price(symbol)
            current_price = tick['ask'] if action == "BUY" else tick['bid']

            sl_price = current_price - (sl_pips * point) if action == "BUY" else current_price + (sl_pips * point)
            tp_price = current_price + (tp_pips * point) if action == "BUY" else current_price - (tp_pips * point)

            trade_action = "ORDER_TYPE_BUY" if action == "BUY" else "ORDER_TYPE_SELL"

            add_log(f"Opening {action} Trade on {symbol} | Lot: {lot_size} | Entry: {current_price}")
            
            result = await self.connection.create_market_buy_order(symbol, lot_size, round(sl_price, digits), round(tp_price, digits)) if action == "BUY" \
                else await self.connection.create_market_sell_order(symbol, lot_size, round(sl_price, digits), round(tp_price, digits))

            add_log(f"Trade Order Executed Successfully! Order ID: {result.get('stringCode', 'OK')}")
        except Exception as e:
            add_log(f"Execution Error: {str(e)}")

    async def run_loop(self):
        await self.initialize()
        
        # Resolve Symbol Fix
        resolved_symbol = await self.resolve_broker_symbol(bot_state["symbol"])
        bot_state["actual_symbol"] = resolved_symbol

        while bot_state["is_running"]:
            try:
                await self.update_account_metrics()
                symbol = bot_state["actual_symbol"]
                timeframe = bot_state["timeframe"]

                df = await self.fetch_candles(symbol, timeframe)
                
                if df is not None and len(df) > 30:
                    # Strategy Calculations (EMA + RSI Example)
                    df['ema_fast'] = calculate_ema(df['close'], 9)
                    df['ema_slow'] = calculate_ema(df['close'], 21)
                    df['rsi'] = calculate_rsi(df['close'], 14)

                    latest_rsi = df['rsi'].iloc[-1]
                    fast_ema = df['ema_fast'].iloc[-1]
                    slow_ema = df['ema_slow'].iloc[-1]
                    prev_fast = df['ema_fast'].iloc[-2]
                    prev_slow = df['ema_slow'].iloc[-2]

                    # Signal Logic
                    signal = "NONE"
                    if prev_fast <= prev_slow and fast_ema > slow_ema and latest_rsi > 45:
                        signal = "BUY"
                    elif prev_fast >= prev_slow and fast_ema < slow_ema and latest_rsi < 55:
                        signal = "SELL"

                    bot_state["last_signal"] = f"{signal} (RSI: {latest_rsi:.1f})"

                    # Position & Trade Logic
                    open_positions = await self.get_open_positions(symbol)
                    
                    if len(open_positions) < bot_state["max_trades"] and signal in ["BUY", "SELL"]:
                        add_log(f"Strategy Triggered: {signal} Signal Detected on {symbol}")
                        await self.execute_trade(
                            signal, 
                            symbol, 
                            bot_state["lot_size"], 
                            bot_state["stop_loss_pips"], 
                            bot_state["take_profit_pips"]
                        )
                else:
                    add_log(f"Waiting for sufficient candle data for {symbol}...")

            except Exception as e:
                add_log(f"Error in trading loop: {str(e)}")

            # Sleep interval between strategy checks
            await asyncio.sleep(15)

# Async Thread Runner
def start_bot_thread():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    engine = TradingBotEngine(bot_state["api_token"], bot_state["account_id"])
    loop.run_until_complete(engine.run_loop())

# ==================== FLASK MOBILE/DESKTOP DASHBOARD ====================
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MT4/MT5 Automated Cloud Bot</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        body { background-color: #0d1117; color: #c9d1d9; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }
        .card { background-color: #161b22; border: 1px solid #30363d; border-radius: 12px; margin-bottom: 20px; }
        .btn-success { background-color: #238636; border: none; }
        .btn-danger { background-color: #da3633; border: none; }
        .log-box { background-color: #010409; border: 1px solid #30363d; height: 250px; overflow-y: scroll; font-family: monospace; font-size: 13px; padding: 10px; border-radius: 8px; color: #3fb950; }
        .badge-real { background-color: #f1e05a; color: #000; font-weight: bold; }
        .badge-demo { background-color: #388bfd; color: #fff; font-weight: bold; }
    </style>
</head>
<body class="p-3 p-md-4">
    <div class="container-fluid" style="max-width: 900px;">
        <div class="d-flex justify-content-between align-items-center mb-4">
            <h2>🤖 Cloud Overlay Trading Bot</h2>
            <span id="accountBadge" class="badge badge-demo p-2">ACCOUNT: UNKNOWN</span>
        </div>

        <!-- Connection Settings -->
        <div class="card p-3">
            <h5 class="text-white mb-3">1. MT4/MT5 MetaApi Credentials</h5>
            <div class="row g-3">
                <div class="col-md-6">
                    <label class="form-label">MetaApi Token</label>
                    <input type="password" id="apiToken" class="form-control bg-dark text-white border-secondary" placeholder="Paste MetaApi Token">
                </div>
                <div class="col-md-6">
                    <label class="form-label">MetaApi Account ID</label>
                    <input type="text" id="accountId" class="form-control bg-dark text-white border-secondary" placeholder="Paste Account ID">
                </div>
            </div>
        </div>

        <!-- Strategy & Risk Management -->
        <div class="card p-3">
            <h5 class="text-white mb-3">2. Pair, Strategy & Risk Control</h5>
            <div class="row g-3">
                <div class="col-md-3 col-6">
                    <label class="form-label">Asset Pair</label>
                    <input type="text" id="symbol" class="form-control bg-dark text-white border-secondary" value="BTCUSD">
                </div>
                <div class="col-md-3 col-6">
                    <label class="form-label">Timeframe</label>
                    <select id="timeframe" class="form-select bg-dark text-white border-secondary">
                        <option value="1m">M1</option>
                        <option value="5m">M5</option>
                        <option value="15m" selected>M15</option>
                        <option value="1h">H1</option>
                        <option value="4h">H4</option>
                    </select>
                </div>
                <div class="col-md-3 col-6">
                    <label class="form-label">Lot Size</label>
                    <input type="number" step="0.01" id="lotSize" class="form-control bg-dark text-white border-secondary" value="0.01">
                </div>
                <div class="col-md-3 col-6">
                    <label class="form-label">Max Active Trades</label>
                    <input type="number" id="maxTrades" class="form-control bg-dark text-white border-secondary" value="1">
                </div>
                <div class="col-md-6 col-6">
                    <label class="form-label">Stop Loss (Pips/Points)</label>
                    <input type="number" id="slPips" class="form-control bg-dark text-white border-secondary" value="200">
                </div>
                <div class="col-md-6 col-6">
                    <label class="form-label">Take Profit (Pips/Points)</label>
                    <input type="number" id="tpPips" class="form-control bg-dark text-white border-secondary" value="400">
                </div>
            </div>
        </div>

        <!-- Action Controls & Status -->
        <div class="card p-3 text-center">
            <div class="row align-items-center">
                <div class="col-md-6 mb-3 mb-md-0">
                    <div>Balance: <strong id="balanceText" class="text-info">$0.00</strong> | Equity: <strong id="equityText" class="text-warning">$0.00</strong></div>
                    <small class="text-muted">Last Signal: <span id="signalText">NONE</span></small>
                </div>
                <div class="col-md-6 d-flex gap-2 justify-content-center">
                    <button id="startBtn" onclick="startBot()" class="btn btn-success btn-lg w-50">▶ RUN BOT</button>
                    <button id="stopBtn" onclick="stopBot()" class="btn btn-danger btn-lg w-50" disabled>⏹ STOP</button>
                </div>
            </div>
        </div>

        <!-- Live Activity Log -->
        <div class="card p-3">
            <h5 class="text-white mb-2">Live Logs & Analysis</h5>
            <div id="logBox" class="log-box">Bot initialized and ready. Fill credentials and press RUN BOT.</div>
        </div>
    </div>

    <script>
        async function updateStatus() {
            try {
                const res = await fetch('/api/status');
                const data = await res.json();
                
                document.getElementById('balanceText').innerText = '$' + data.balance.toFixed(2);
                document.getElementById('equityText').innerText = '$' + data.equity.toFixed(2);
                document.getElementById('signalText').innerText = data.last_signal;
                
                const badge = document.getElementById('accountBadge');
                badge.innerText = 'ACCOUNT: ' + data.account_type;
                badge.className = data.account_type.includes('REAL') ? 'badge badge-real p-2' : 'badge badge-demo p-2';

                const logBox = document.getElementById('logBox');
                logBox.innerHTML = data.logs.join('<br>');
                logBox.scrollTop = logBox.scrollHeight;

                if(data.is_running) {
                    document.getElementById('startBtn').disabled = true;
                    document.getElementById('stopBtn').disabled = false;
                } else {
                    document.getElementById('startBtn').disabled = false;
                    document.getElementById('stopBtn').disabled = true;
                }
            } catch(e) {}
        }

        async function startBot() {
            const config = {
                api_token: document.getElementById('apiToken').value,
                account_id: document.getElementById('accountId').value,
                symbol: document.getElementById('symbol').value,
                timeframe: document.getElementById('timeframe').value,
                lot_size: parseFloat(document.getElementById('lotSize').value),
                max_trades: parseInt(document.getElementById('maxTrades').value),
                stop_loss_pips: parseInt(document.getElementById('slPips').value),
                take_profit_pips: parseInt(document.getElementById('tpPips').value)
            };

            if(!config.api_token || !config.account_id) {
                alert('Please enter your MetaApi Token and Account ID!');
                return;
            }

            await fetch('/api/start', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(config)
            });
        }

        async function stopBot() {
            await fetch('/api/stop', {method: 'POST'});
        }

        setInterval(updateStatus, 3000);
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/api/status', methods=['GET'])
def get_status():
    return jsonify(bot_state)

@app.route('/api/start', methods=['POST'])
def start_bot():
    data = request.json
    if bot_state["is_running"]:
        return jsonify({"status": "already running"})

    bot_state["api_token"] = data.get("api_token")
    bot_state["account_id"] = data.get("account_id")
    bot_state["symbol"] = data.get("symbol", "BTCUSD").upper()
    bot_state["timeframe"] = data.get("timeframe", "15m")
    bot_state["lot_size"] = float(data.get("lot_size", 0.01))
    bot_state["max_trades"] = int(data.get("max_trades", 1))
    bot_state["stop_loss_pips"] = int(data.get("stop_loss_pips", 200))
    bot_state["take_profit_pips"] = int(data.get("take_profit_pips", 400))
    
    bot_state["is_running"] = True
    add_log("Starting Bot Thread...")

    t = threading.Thread(target=start_bot_thread)
    t.daemon = True
    t.start()

    return jsonify({"status": "started"})

@app.route('/api/stop', methods=['POST'])
def stop_bot():
    bot_state["is_running"] = False
    add_log("Bot Stop Requested. Shutting down loop...")
    return jsonify({"status": "stopped"})

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
