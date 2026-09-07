import os
import asyncio
import threading
import logging
import time
import re
from datetime import datetime, timezone
from flask import Flask, render_template_string, request, jsonify
from metaapi_cloud_sdk import MetaApi

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
    "lot_size": 0.01,
    "max_trades": 1,
    "min_profit_target_usd": 0.30,  # Target profit in USD to close instantly
    "stop_loss_pips": 1000,
    "take_profit_pips": 2000,
    "open_positions": [],
    "live_tick": {"bid": 0.0, "ask": 0.0, "spread": 0.0, "time": ""},
    "tick_acceleration": 0.0,
    "micro_signal": "WAITING",
    "logs": ["Sub-Second Acceleration Scalper Ready. Connect MT4/MT5 to begin."]
}

def add_log(msg):
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    entry = f"[{ts}] {msg}"
    bot_state["logs"].append(entry)
    if len(bot_state["logs"]) > 130:
        bot_state["logs"].pop(0)
    logging.info(msg)

# ================= METAAPI ULTRA-FAST SCALPER ENGINE =================
class UltraScalperEngine:
    def __init__(self):
        self.api = None
        self.account = None
        self.connection = None
        self.price_history = []
        self.specs_cache = {}

    def sanitize_server_name(self, server_name):
        """ Auto-corrects server name typos like Exness-MT5TriaI9 -> Exness-MT5Trial9 """
        if not server_name:
            return ""
        cleaned = server_name.strip()
        cleaned = re.sub(r'TriaI', 'Trial', cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r'Triai', 'Trial', cleaned, flags=re.IGNORECASE)
        return cleaned

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
            clean_server = self.sanitize_server_name(server)
            add_log(f"Connecting to Broker Server: '{clean_server}' | Login: {login}")
            self.api = MetaApi(token)

            try:
                accounts = await self.api.metatrader_account_api.get_accounts()
            except:
                accounts = await self.api.metatrader_account_api.get_accounts_with_infinite_scroll_pagination()

            existing = None
            for acc in accounts:
                if str(acc.login) == str(login) and clean_server.lower() in str(acc.server).lower():
                    existing = acc
                    break

            if existing:
                self.account = existing
            else:
                payload = {
                    "name": f"TickScalper-{login}",
                    "type": "cloud",
                    "login": str(login),
                    "password": str(password),
                    "server": str(clean_server),
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
            add_log("Deploying cloud terminal container...")
            await self.account.deploy()

        add_log("Synchronizing real-time price stream...")
        await self.account.wait_connected()

        self.connection = self.account.get_rpc_connection()
        await self.connection.connect()
        await self.connection.wait_synchronized()

        await self.update_account_info()
        bot_state["is_connected"] = True
        bot_state["status_msg"] = f"Online ({bot_state['account_type']})"
        bot_state["last_error"] = ""
        add_log("⚡ SUCCESS! High-Speed Scalper Connected & Stream Active.")
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

    async def close_position_instant(self, position_id):
        pos_id_str = str(position_id)
        add_log(f"⚡ INSTANT CLOSE: Sending close command for Trade Ticket #{pos_id_str}...")
        try:
            await self.connection.close_position(pos_id_str)
            add_log(f"💰 Trade #{pos_id_str} CLOSED successfully in Profit!")
            await self.update_account_info()
            return True
        except Exception:
            try:
                if pos_id_str.isdigit():
                    await self.connection.close_position(int(pos_id_str))
                    add_log(f"💰 Trade #{pos_id_str} CLOSED successfully in Profit!")
                    await self.update_account_info()
                    return True
            except Exception as e:
                add_log(f"❌ Close Error: {e}")
        return False

    async def check_and_auto_close_profits(self, symbol):
        """ Sub-Second Profit Lock: Monitors floating P/L 4 times per second """
        try:
            positions = bot_state["open_positions"]
            target = bot_state["min_profit_target_usd"]

            for pos in positions:
                if pos["symbol"].replace("T", "") in symbol.replace("T", "") or symbol.replace("T", "") in pos["symbol"].replace("T", ""):
                    profit = pos["profit"]
                    if profit >= target:
                        add_log(f"🎯 PROFIT TARGET HIT (+${profit:.2f} >= ${target:.2f})! Banking profit now...")
                        await self.close_position_instant(pos["id"])
        except Exception:
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

            add_log(f"⚡ PRE-MOVE ENTRY: Opening {action} on {symbol} | Lot: {lot} @ Entry: {entry:.2f}")

            if action == "BUY":
                res = await self.connection.create_market_buy_order(symbol, lot, round(sl, digits), round(tp, digits))
            else:
                res = await self.connection.create_market_sell_order(symbol, lot, round(sl, digits), round(tp, digits))

            add_log(f"✅ EXECUTED INSTANTLY ON BROKER! Ticket: {res.get('stringCode', 'CONFIRMED')}")
            await self.update_account_info()
        except Exception as e:
            add_log(f"❌ Execution Error: {e}")

    async def run_sub_second_scalper_loop(self):
        add_log("🚀 SUB-SECOND ACCELERATION ENGINE ACTIVE (250ms Loop)")
        bot_state["actual_symbol"] = await self.resolve_symbol(bot_state["symbol"])
        symbol = bot_state["actual_symbol"]

        try:
            await self.connection.subscribe_to_market_data(symbol)
        except Exception:
            pass

        while bot_state["is_running"] and bot_state["is_connected"]:
            try:
                # 1. Sample Live Tick Price (Every 250ms)
                tick = await self.connection.get_symbol_price(symbol)
                bid, ask = float(tick["bid"]), float(tick["ask"])
                mid_price = (bid + ask) / 2.0
                spread = round((ask - bid), 2 if "BTC" in symbol else 5)

                bot_state["live_tick"] = {
                    "bid": bid, "ask": ask, "spread": spread,
                    "time": datetime.now().strftime("%H:%M:%S.%f")[:-3]
                }

                # 2. Buffer Sub-Second Ticks
                self.price_history.append(mid_price)
                if len(self.price_history) > 8:
                    self.price_history.pop(0)

                # 3. Calculate Tick Acceleration (Rate of Change of Speed)
                signal = "WAITING"
                accel = 0.0

                if len(self.price_history) >= 4:
                    # Velocity 1: Change between last two ticks
                    v1 = self.price_history[-1] - self.price_history[-2]
                    # Velocity 2: Change between previous two ticks
                    v2 = self.price_history[-2] - self.price_history[-3]
                    # Acceleration: Difference in velocities
                    accel = round(v1 - v2, 3)
                    bot_state["tick_acceleration"] = accel

                    # Trigger Signal on Positive/Negative Acceleration Spikes
                    threshold = 0.50 if "BTC" in symbol else 0.0001
                    if v1 > 0 and accel > threshold:
                        signal = "BUY"
                        bot_state["micro_signal"] = "BULLISH SURGE 🚀"
                    elif v1 < 0 and accel < -threshold:
                        signal = "SELL"
                        bot_state["micro_signal"] = "BEARISH DROP 📉"
                    else:
                        bot_state["micro_signal"] = "WAITING FOR MOVE"

                # 4. Instant Profit Manager Check (Runs 4x a second)
                await self.check_and_auto_close_profits(symbol)

                # 5. Open Instant Trade Before Price Surge Finishes
                matching_positions = [
                    p for p in bot_state["open_positions"] 
                    if p["symbol"].replace("T", "") in symbol.replace("T", "") or symbol.replace("T", "") in p["symbol"].replace("T", "")
                ]

                if len(matching_positions) < bot_state["max_trades"] and signal in ["BUY", "SELL"]:
                    add_log(f"🔥 TICK SURGE DETECTED ({bot_state['micro_signal']})! Accel: {accel}. Opening Trade...")
                    await self.execute_trade(
                        signal, symbol, bot_state["lot_size"],
                        bot_state["stop_loss_pips"], bot_state["take_profit_pips"]
                    )

            except Exception as e:
                pass

            # Loop every 250 milliseconds (Sub-Second Execution)
            await asyncio.sleep(0.25)

        add_log("Sub-second scalper loop stopped.")

engine = UltraScalperEngine()

# ================= EMBEDDED DASHBOARD UI =================
HTML_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Cloud Sub-Second Scalper Overlay Bot</title>
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
            <h5 class="m-0 text-white font-weight-bold">⚡ MT4/MT5 Sub-Second Acceleration Scalper</h5>
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
                <span class="metric-title">Pre-Move Momentum</span>
                <div class="metric-value text-warning" id="surgeVal">WAITING FOR MOVE</div>
            </div>
        </div>
    </div>

    <!-- NAVIGATION TABS -->
    <ul class="nav nav-pills mb-3">
        <li class="nav-item"><button class="nav-link active" data-tab="live">📊 Live Terminal & Chart</button></li>
        <li class="nav-item"><button class="nav-link" data-tab="strategy">⚙️ Settings & Target Profit</button></li>
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
            <h6 class="text-white mb-3">Instant Profit Target & Risk Controls</h6>
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
                    <label class="form-label">Execution Timeframe</label>
                    <select id="tfSelect" class="form-select" onchange="updateTradingViewChart()">
                        <option value="1m" selected>M1 (Sub-Second Scalp)</option>
                        <option value="5m">M5</option>
                        <option value="15m">M15</option>
                    </select>
                </div>
                <div class="col-6 col-md-3">
                    <label class="form-label">Target Profit ($ USD to Close)</label>
                    <input type="number" id="targetProfitInput" class="form-control" value="0.30" step="0.10">
                </div>
                <div class="col-6 col-md-3">
                    <label class="form-label">Lot Size</label>
                    <input type="number" id="lotInput" class="form-control" value="0.01" step="0.01">
                </div>
                <div class="col-6 col-md-3">
                    <label class="form-label">Max Active Trades</label>
                    <input type="number" id="maxTradesInput" class="form-control" value="1">
                </div>
                <div class="col-6 col-md-3">
                    <label class="form-label">Stop Loss (Points)</label>
                    <input type="number" id="slInput" class="form-control" value="1000">
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
        <h6 class="text-white mb-2">Sub-Second Scalper Console</h6>
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
        document.getElementById('surgeVal').textContent = d.micro_signal || 'WAITING FOR MOVE';

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
        stop_loss_pips: parseInt(document.getElementById('slInput').value),
        take_profit_pips: 2000
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
    // Dashboard updates every 300ms for live tracking
    setInterval(refreshUI, 300);
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
        "min_profit_target_usd": float(data.get("min_profit_target_usd", 0.30)),
        "stop_loss_pips": int(data.get("stop_loss_pips", 1000)),
        "take_profit_pips": 2000,
        "is_running": True
    })
    asyncio.run_coroutine_threadsafe(engine.run_sub_second_scalper_loop(), bg_loop)
    return jsonify(status="started")

@app.route("/api/stop", methods=["POST"])
def api_stop():
    bot_state["is_running"] = False
    add_log("Sub-second scalper stop requested.")
    return jsonify(status="stopped")

@app.route("/api/close_position", methods=["POST"])
def api_close_position():
    data = request.json or {}
    pos_id = data.get("position_id")
    if pos_id and bot_state["is_connected"]:
        asyncio.run_coroutine_threadsafe(engine.close_position_instant(pos_id), bg_loop)
        return jsonify(status="closing_initiated")
    return jsonify(status="failed")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
