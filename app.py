import os
import asyncio
import threading
import logging
from datetime import datetime, timezone
from flask import Flask, render_template_string, request, jsonify
from metaapi_cloud_sdk import MetaApi
import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
app = Flask(__name__)

# ========== BACKGROUND EVENT LOOP ==========
bg_loop = None
def _start_bg_loop():
    global bg_loop
    bg_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(bg_loop)
    bg_loop.run_forever()

threading.Thread(target=_start_bg_loop, daemon=True).start()

# ========== GLOBAL STATE ==========
bot_state = {
    "is_connected": False,
    "is_running": False,
    "status_msg": "Offline",
    "last_error": "",
    "api_token": "",
    "account_id": "",
    "account_type": "UNKNOWN",
    "balance": 0.0,
    "equity": 0.0,
    "symbol": "BTCUSD",
    "actual_symbol": "BTCUSD",
    "timeframe": "15m",
    "lot_size": 0.01,
    "max_trades": 1,
    "stop_loss_pips": 200,
    "take_profit_pips": 400,
    "logs": ["Bot ready. Fill credentials and press Connect."],
    "last_signal": "NONE"
}

def add_log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    entry = f"[{ts}] {msg}"
    bot_state["logs"].append(entry)
    if len(bot_state["logs"]) > 100:
        bot_state["logs"].pop(0)
    logging.info(msg)

# ========== INDICATORS ==========
def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def calculate_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

# ========== METAAPI ENGINE ==========
class MetaApiEngine:
    def __init__(self):
        self.api = None
        self.account = None
        self.connection = None

    async def connect_with_credentials(self, token, login, password, server, platform, region):
        try:
            add_log(f"Connecting → Server: {server} | Login: {login}")
            self.api = MetaApi(token)          # ← FIXED (no domain argument)

            # Try to find existing account first
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
                add_log(f"Using existing MetaApi account: {existing.id}")
                self.account = existing
            else:
                add_log("Creating new cloud account on MetaApi (20-50 sec)...")
                payload = {
                    "name": f"Bot-{login}",
                    "type": "cloud",
                    "login": str(login),
                    "password": str(password),
                    "server": str(server),
                    "platform": "mt5" if "5" in platform.lower() else "mt4",
                    "magic": 123456
                }
                if region and region != "default":
                    payload["region"] = region

                self.account = await self.api.metatrader_account_api.create_account(payload)
                add_log(f"Account created successfully: {self.account.id}")

            return await self._finish_connection()

        except Exception as e:
            error = str(e)
            bot_state["last_error"] = error
            bot_state["status_msg"] = "Connection Failed"
            bot_state["is_connected"] = False
            add_log(f"❌ CONNECTION ERROR: {error}")
            return False, error

    async def connect_with_account_id(self, token, account_id):
        try:
            add_log(f"Connecting with Account ID: {account_id}")
            self.api = MetaApi(token)
            self.account = await self.api.metatrader_account_api.get_account(account_id)
            return await self._finish_connection()
        except Exception as e:
            error = str(e)
            bot_state["last_error"] = error
            bot_state["status_msg"] = "Connection Failed"
            bot_state["is_connected"] = False
            add_log(f"❌ CONNECTION ERROR: {error}")
            return False, error

    async def _finish_connection(self):
        bot_state["account_id"] = self.account.id
        bot_state["account_type"] = str(getattr(self.account, "type", "cloud")).upper()

        if self.account.state != "DEPLOYED":
            add_log("Deploying terminal (please wait 20-60 seconds)...")
            await self.account.deploy()

        add_log("Waiting for broker connection & synchronization...")
        await self.account.wait_connected()

        self.connection = self.account.get_rpc_connection()
        await self.connection.connect()
        await self.connection.wait_synchronized()

        info = await self.connection.get_account_information()
        bot_state["balance"] = float(info.get("balance", 0))
        bot_state["equity"] = float(info.get("equity", 0))
        bot_state["is_connected"] = True
        bot_state["status_msg"] = f"Online • {bot_state['account_type']}"
        bot_state["last_error"] = ""
        add_log("✅ SUCCESS! Connected & Synchronized with broker")
        return True, "Connected successfully"

    async def resolve_symbol(self, symbol):
        try:
            symbols = await self.connection.get_symbols()
            if symbol in symbols:
                return symbol
            clean = symbol.replace("/", "").replace("_", "").replace(".", "").upper()
            for s in symbols:
                s_clean = s.replace("/", "").replace("_", "").replace(".", "").upper()
                if clean in s_clean or s_clean in clean:
                    add_log(f"Symbol auto-resolved: {symbol} → {s}")
                    return s
            return symbol
        except Exception as e:
            add_log(f"Symbol resolve warning: {e}")
            return symbol

    async def fetch_candles(self, symbol, timeframe, count=100):
        try:
            candles = await self.connection.get_historical_candles(
                symbol, timeframe, datetime.now(timezone.utc), count
            )
            if not candles:
                await self.connection.subscribe_to_market_data(symbol)
                await asyncio.sleep(2)
                candles = await self.connection.get_historical_candles(
                    symbol, timeframe, datetime.now(timezone.utc), count
                )
            if not candles:
                add_log(f"No candle data for {symbol}")
                return None
            df = pd.DataFrame(candles)
            for col in ["open", "high", "low", "close"]:
                df[col] = df[col].astype(float)
            return df
        except Exception as e:
            add_log(f"Candle error: {e}")
            return None

    async def execute_trade(self, action, symbol, lot, sl_pips, tp_pips):
        try:
            spec = await self.connection.get_symbol_specification(symbol)
            digits = spec.get("digits", 2)
            point = spec.get("point", 0.01)
            price = await self.connection.get_symbol_price(symbol)
            entry = price["ask"] if action == "BUY" else price["bid"]

            sl = entry - sl_pips * point if action == "BUY" else entry + sl_pips * point
            tp = entry + tp_pips * point if action == "BUY" else entry - tp_pips * point

            add_log(f"Placing {action} {symbol} | Lot: {lot} @ {entry:.5f}")
            if action == "BUY":
                res = await self.connection.create_market_buy_order(
                    symbol, lot, round(sl, digits), round(tp, digits)
                )
            else:
                res = await self.connection.create_market_sell_order(
                    symbol, lot, round(sl, digits), round(tp, digits)
                )
            add_log(f"✅ Trade opened! {res}")
        except Exception as e:
            add_log(f"❌ Trade failed: {e}")

    async def trading_loop(self):
        add_log("🚀 Trading engine started")
        bot_state["actual_symbol"] = await self.resolve_symbol(bot_state["symbol"])
        add_log(f"Trading symbol: {bot_state['actual_symbol']}")

        while bot_state["is_running"] and bot_state["is_connected"]:
            try:
                info = await self.connection.get_account_information()
                bot_state["balance"] = float(info.get("balance", 0))
                bot_state["equity"] = float(info.get("equity", 0))

                df = await self.fetch_candles(bot_state["actual_symbol"], bot_state["timeframe"])
                if df is not None and len(df) > 30:
                    df["ema_f"] = calculate_ema(df["close"], 9)
                    df["ema_s"] = calculate_ema(df["close"], 21)
                    df["rsi"] = calculate_rsi(df["close"], 14)

                    rsi = df["rsi"].iloc[-1]
                    f, s = df["ema_f"].iloc[-1], df["ema_s"].iloc[-1]
                    pf, ps = df["ema_f"].iloc[-2], df["ema_s"].iloc[-2]

                    signal = "NONE"
                    if pf <= ps and f > s and rsi > 45:
                        signal = "BUY"
                    elif pf >= ps and f < s and rsi < 55:
                        signal = "SELL"

                    bot_state["last_signal"] = f"{signal} (RSI {rsi:.1f})"

                    positions = await self.connection.get_positions()
                    my_pos = [p for p in positions if p.get("symbol") == bot_state["actual_symbol"]]

                    if len(my_pos) < bot_state["max_trades"] and signal in ("BUY", "SELL"):
                        add_log(f"Strategy signal: {signal}")
                        await self.execute_trade(
                            signal,
                            bot_state["actual_symbol"],
                            bot_state["lot_size"],
                            bot_state["stop_loss_pips"],
                            bot_state["take_profit_pips"]
                        )
            except Exception as e:
                add_log(f"Loop error: {e}")

            await asyncio.sleep(15)

        add_log("Trading loop stopped")

engine = MetaApiEngine()

# ========== HTML UI ==========
HTML = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Cloud MT4/MT5 Bot</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
<style>
body{background:#0b0e14;color:#adbac7;font-family:system-ui}
.card{background:#151b23;border:1px solid #2d333b;border-radius:12px;margin-bottom:16px}
.nav-pills .nav-link{color:#768390;font-weight:600}
.nav-pills .nav-link.active{background:#1f6feb;color:#fff}
.form-control,.form-select{background:#0d1117;border:1px solid #2d333b;color:#e6edf3}
.form-control:focus,.form-select:focus{background:#0d1117;color:#fff;border-color:#1f6feb;box-shadow:none}
.btn-success{background:#238636;border:none}
.btn-danger{background:#da3633;border:none}
.log-box{background:#010409;border:1px solid #2d333b;height:260px;overflow-y:auto;font-family:monospace;font-size:12px;padding:12px;border-radius:8px;color:#3fb950}
.status-online{color:#3fb950}
.status-offline{color:#f85149}
.error-box{background:#2d1515;border:1px solid #f85149;color:#ff7b72;padding:10px;border-radius:8px;font-size:13px;display:none;margin-bottom:12px}
</style>
</head>
<body class="p-3">
<div class="container" style="max-width:820px">

<div class="d-flex justify-content-between align-items-center mb-3">
  <h4 class="text-white mb-0">🤖 Cloud Trading Bot</h4>
  <span id="statusBadge" class="status-offline">● Offline</span>
</div>

<ul class="nav nav-pills mb-3">
  <li class="nav-item"><button class="nav-link active" data-tab="connect">1. Connect</button></li>
  <li class="nav-item"><button class="nav-link" data-tab="strategy">2. Strategy</button></li>
  <li class="nav-item"><button class="nav-link" data-tab="live">3. Live Trade</button></li>
</ul>

<!-- CONNECT -->
<div id="tab-connect" class="tab-pane">
  <div class="card p-3">
    <div class="mb-3">
      <label class="form-label">MetaApi Token</label>
      <input type="password" id="token" class="form-control" placeholder="eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9...">
    </div>

    <div class="mb-3">
      <label class="form-label">Connection Method</label>
      <select id="method" class="form-select" onchange="toggleMethod()">
        <option value="account_id">Recommended: Account ID only</option>
        <option value="credentials">Login + Password + Server</option>
      </select>
    </div>

    <div id="box-account-id">
      <div class="mb-3">
        <label class="form-label">MetaApi Account ID</label>
        <input type="text" id="accountId" class="form-control" placeholder="Paste Account ID here">
        <small class="text-muted">Create the account first at app.metaapi.cloud → Accounts</small>
      </div>
    </div>

    <div id="box-credentials" style="display:none">
      <div class="mb-3">
        <label class="form-label">Platform</label>
        <select id="platform" class="form-select">
          <option value="mt5">MetaTrader 5</option>
          <option value="mt4">MetaTrader 4</option>
        </select>
      </div>
      <div class="mb-3">
        <label class="form-label">Account Login</label>
        <input type="text" id="login" class="form-control" value="476924559">
      </div>
      <div class="mb-3">
        <label class="form-label">Password (Main password, not Investor)</label>
        <input type="password" id="password" class="form-control">
      </div>
      <div class="mb-3">
        <label class="form-label">Server (exact name)</label>
        <input type="text" id="server" class="form-control" value="Exness-MT5Trial9">
      </div>
      <div class="mb-3">
        <label class="form-label">Region</label>
        <select id="region" class="form-select">
          <option value="default">Auto</option>
          <option value="new-york">New York</option>
          <option value="london">London</option>
          <option value="singapore">Singapore</option>
        </select>
      </div>
    </div>

    <div id="errorBox" class="error-box"></div>
    <button id="btnConnect" onclick="doConnect()" class="btn btn-success w-100 py-2">Connect to Broker</button>
  </div>
</div>

<!-- STRATEGY -->
<div id="tab-strategy" class="tab-pane" style="display:none">
  <div class="card p-3">
    <div class="row g-3">
      <div class="col-md-6">
        <label class="form-label">Symbol (works on weekend)</label>
        <select id="symbol" class="form-select">
          <optgroup label="Crypto 24/7">
            <option value="BTCUSD" selected>BTCUSD</option>
            <option value="ETHUSD">ETHUSD</option>
            <option value="SOLUSD">SOLUSD</option>
            <option value="XRPUSD">XRPUSD</option>
            <option value="DOGEUSD">DOGEUSD</option>
          </optgroup>
          <optgroup label="Forex / Metals">
            <option value="EURUSD">EURUSD</option>
            <option value="GBPUSD">GBPUSD</option>
            <option value="XAUUSD">XAUUSD</option>
            <option value="USDJPY">USDJPY</option>
          </optgroup>
        </select>
      </div>
      <div class="col-md-6">
        <label class="form-label">Timeframe</label>
        <select id="timeframe" class="form-select">
          <option value="1m">M1</option>
          <option value="5m">M5</option>
          <option value="15m" selected>M15</option>
          <option value="30m">M30</option>
          <option value="1h">H1</option>
          <option value="4h">H4</option>
        </select>
      </div>
      <div class="col-6">
        <label class="form-label">Lot Size</label>
        <input type="number" id="lot" class="form-control" value="0.01" step="0.01">
      </div>
      <div class="col-6">
        <label class="form-label">Max Open Trades</label>
        <input type="number" id="maxTrades" class="form-control" value="1">
      </div>
      <div class="col-6">
        <label class="form-label">Stop Loss (points)</label>
        <input type="number" id="sl" class="form-control" value="200">
      </div>
      <div class="col-6">
        <label class="form-label">Take Profit (points)</label>
        <input type="number" id="tp" class="form-control" value="400">
      </div>
    </div>
  </div>
</div>

<!-- LIVE -->
<div id="tab-live" class="tab-pane" style="display:none">
  <div class="card p-3 text-center">
    <div class="row mb-3">
      <div class="col-6">
        <small class="text-muted">Balance</small>
        <h3 id="bal" class="text-info">$0.00</h3>
      </div>
      <div class="col-6">
        <small class="text-muted">Equity</small>
        <h3 id="eq" class="text-warning">$0.00</h3>
      </div>
    </div>
    <div class="mb-3">Last Signal: <strong id="signal">NONE</strong></div>
    <div class="d-flex gap-2">
      <button id="btnStart" onclick="startBot()" class="btn btn-success w-50" disabled>▶ RUN BOT</button>
      <button id="btnStop" onclick="stopBot()" class="btn btn-danger w-50" disabled>⏹ STOP</button>
    </div>
  </div>
</div>

<div class="card p-3">
  <h6 class="text-white">Live Logs</h6>
  <div id="logs" class="log-box"></div>
</div>
</div>

<script>
function toggleMethod(){
  const m = document.getElementById('method').value;
  document.getElementById('box-account-id').style.display = m === 'account_id' ? 'block' : 'none';
  document.getElementById('box-credentials').style.display = m === 'credentials' ? 'block' : 'none';
}
document.querySelectorAll('[data-tab]').forEach(btn=>{
  btn.onclick = () => {
    document.querySelectorAll('.nav-link').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    document.querySelectorAll('.tab-pane').forEach(p => p.style.display = 'none');
    document.getElementById('tab-' + btn.dataset.tab).style.display = 'block';
  }
});

async function refresh(){
  try {
    const r = await fetch('/api/status');
    const d = await r.json();
    document.getElementById('statusBadge').textContent = '● ' + d.status_msg;
    document.getElementById('statusBadge').className = d.is_connected ? 'status-online' : 'status-offline';
    document.getElementById('bal').textContent = '$' + d.balance.toFixed(2);
    document.getElementById('eq').textContent = '$' + d.equity.toFixed(2);
    document.getElementById('signal').textContent = d.last_signal;
    document.getElementById('logs').innerHTML = d.logs.join('<br>');
    document.getElementById('logs').scrollTop = 99999;

    const err = document.getElementById('errorBox');
    if(d.last_error){
      err.style.display = 'block';
      err.textContent = 'Error: ' + d.last_error;
    } else {
      err.style.display = 'none';
    }

    document.getElementById('btnStart').disabled = !d.is_connected || d.is_running;
    document.getElementById('btnStop').disabled = !d.is_running;
  } catch(e){}
}
setInterval(refresh, 2500);

async function doConnect(){
  const btn = document.getElementById('btnConnect');
  btn.disabled = true;
  btn.textContent = 'Connecting... please wait';

  const method = document.getElementById('method').value;
  let body = { token: document.getElementById('token').value.trim(), method };

  if(method === 'account_id'){
    body.account_id = document.getElementById('accountId').value.trim();
  } else {
    body.login = document.getElementById('login').value.trim();
    body.password = document.getElementById('password').value;
    body.server = document.getElementById('server').value.trim();
    body.platform = document.getElementById('platform').value;
    body.region = document.getElementById('region').value;
  }

  const r = await fetch('/api/connect', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body)
  });
  const d = await r.json();

  btn.disabled = false;
  btn.textContent = 'Connect to Broker';

  if(d.success){
    alert('✅ Connected successfully!');
    document.querySelector('[data-tab="strategy"]').click();
  } else {
    alert('❌ Failed: ' + d.message);
  }
}

async function startBot(){
  const body = {
    symbol: document.getElementById('symbol').value,
    timeframe: document.getElementById('timeframe').value,
    lot_size: parseFloat(document.getElementById('lot').value),
    max_trades: parseInt(document.getElementById('maxTrades').value),
    stop_loss_pips: parseInt(document.getElementById('sl').value),
    take_profit_pips: parseInt(document.getElementById('tp').value)
  };
  await fetch('/api/start', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body)
  });
}
async function stopBot(){
  await fetch('/api/stop', {method: 'POST'});
}
</script>
</body>
</html>
"""

@app.route("/")
def home():
    return render_template_string(HTML)

@app.route("/api/status")
def status():
    return jsonify(bot_state)

@app.route("/api/connect", methods=["POST"])
def api_connect():
    data = request.json or {}
    token = data.get("token", "").strip()
    if not token:
        return jsonify(success=False, message="MetaApi token is required")

    bot_state["api_token"] = token
    bot_state["last_error"] = ""
    bot_state["status_msg"] = "Connecting..."

    async def _do():
        if data.get("method") == "account_id":
            acc_id = data.get("account_id", "").strip()
            if not acc_id:
                return False, "Account ID is required"
            return await engine.connect_with_account_id(token, acc_id)
        else:
            return await engine.connect_with_credentials(
                token,
                data.get("login"),
                data.get("password"),
                data.get("server"),
                data.get("platform", "mt5"),
                data.get("region", "default")
            )

    future = asyncio.run_coroutine_threadsafe(_do(), bg_loop)
    try:
        ok, msg = future.result(timeout=120)
        return jsonify(success=ok, message=msg)
    except Exception as e:
        bot_state["last_error"] = str(e)
        bot_state["status_msg"] = "Connection Failed"
        add_log(f"Fatal error: {e}")
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
        "stop_loss_pips": int(data.get("stop_loss_pips", 200)),
        "take_profit_pips": int(data.get("take_profit_pips", 400)),
        "is_running": True
    })
    asyncio.run_coroutine_threadsafe(engine.trading_loop(), bg_loop)
    return jsonify(status="started")

@app.route("/api/stop", methods=["POST"])
def api_stop():
    bot_state["is_running"] = False
    add_log("Stop requested by user")
    return jsonify(status="stopped")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
