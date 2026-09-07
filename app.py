import os
import re
import asyncio
import threading
import logging
from datetime import datetime, timezone

from flask import Flask, render_template_string, request, jsonify, Response
from metaapi_cloud_sdk import MetaApi

logging.basicConfig(level=logging.INFO)
app = Flask(__name__)

# ---------- BACKGROUND EVENT LOOP ----------
_loop = None

def _bg():
    global _loop
    _loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_loop)
    _loop.run_forever()

threading.Thread(target=_bg, daemon=True).start()

def run_async(coro, timeout=120):
    return asyncio.run_coroutine_threadsafe(coro, _loop).result(timeout=timeout)

# ---------- GLOBAL STATE ----------
S = {
    "connected": False,
    "running": False,
    "status": "Offline",
    "error": "",
    "token": "",
    "account_id": "",
    "login": "",
    "server": "",
    "account_type": "",
    "balance": 0.0,
    "equity": 0.0,
    "profit": 0.0,
    "symbol": "XAUUSD",
    "real_symbol": "XAUUSD",
    "lot": 0.01,
    "max_trades": 1,
    "target_profit": 1.0,        # Bank USD target
    "sl_points": 250,            # Hard Stop Loss in points
    "tp_points": 500,            # Hard Take Profit in points
    "max_daily_loss_usd": 5.0,   # HARD PROTECTION: Auto-stop bot if daily loss hits $5.00
    "day_start_balance": 0.0,
    "day_pnl": 0.0,
    "positions": [],
    "tick": {"bid": 0.0, "ask": 0.0},
    "direction": "WAITING",
    "score": 0,
    "last_action": "—",
    "logs": ["Risk-Protected Engine Ready. ALWAYS test on DEMO first."]
}

def log(msg):
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    S["logs"].append(line)
    if len(S["logs"]) > 150:
        S["logs"].pop(0)
    logging.info(line)

# ---------- RISK-MANAGED ENGINE ----------
class RiskManagedEngine:
    def __init__(self):
        self.api = None
        self.account = None
        self.conn = None
        self.prices = []
        self.specs = {}

    def fix_server(self, server):
        if not server:
            return server
        server = server.strip()
        server = re.sub(r"TriaI", "Trial", server, flags=re.I)
        server = re.sub(r"Triai", "Trial", server, flags=re.I)
        return server

    async def connect_id(self, token, acc_id):
        self.api = MetaApi(token)
        self.account = await self.api.metatrader_account_api.get_account(acc_id.strip())
        return await self._ready()

    async def connect_login(self, token, login, password, server, platform):
        server = self.fix_server(server)
        log(f"Connecting {server} | {login}")
        self.api = MetaApi(token)
        accounts = await self.api.metatrader_account_api.get_accounts_with_infinite_scroll_pagination()
        found = next((a for a in accounts if str(a.login) == str(login) and server.lower() in str(a.server).lower()), None)
        if found:
            self.account = found
        else:
            self.account = await self.api.metatrader_account_api.create_account({
                "name": f"Protected-{login}",
                "type": "cloud",
                "login": str(login),
                "password": str(password),
                "server": server,
                "platform": "mt5" if "5" in str(platform).lower() else "mt4",
                "magic": 260318
            })
        return await self._ready()

    async def _ready(self):
        S["account_id"] = self.account.id
        S["login"] = str(getattr(self.account, "login", ""))
        S["server"] = str(getattr(self.account, "server", ""))
        S["account_type"] = str(getattr(self.account, "type", "cloud")).upper()
        if self.account.state != "DEPLOYED":
            await self.account.deploy()
        await self.account.wait_connected()
        self.conn = self.account.get_rpc_connection()
        await self.conn.connect()
        await self.conn.wait_synchronized()
        await self.refresh()
        S["day_start_balance"] = S["balance"]
        S["connected"] = True
        S["status"] = f"Online • {S['account_type']}"
        log("✅ Connected with Risk Management Enabled")
        return True, "OK"

    async def refresh(self):
        if not self.conn:
            return
        try:
            info = await self.conn.get_account_information()
            S["balance"] = float(info.get("balance", 0))
            S["equity"] = float(info.get("equity", 0))
            S["profit"] = round(S["equity"] - S["balance"], 2)
            if S["day_start_balance"] > 0:
                S["day_pnl"] = round(S["equity"] - S["day_start_balance"], 2)

            raw = await self.conn.get_positions()
            out = []
            for p in raw:
                side = "BUY" if "BUY" in str(p.get("type", "")) else "SELL"
                out.append({
                    "id": str(p.get("id")),
                    "symbol": p.get("symbol"),
                    "type": side,
                    "volume": float(p.get("volume", 0.01)),
                    "open": float(p.get("openPrice", 0)),
                    "current": float(p.get("currentPrice", 0)),
                    "profit": float(p.get("profit", 0)),
                    "sl": float(p.get("stopLoss", 0) or 0),
                    "tp": float(p.get("takeProfit", 0) or 0),
                })
            S["positions"] = out
        except Exception:
            pass

    async def resolve(self, symbol):
        try:
            symbols = await self.conn.get_symbols()
            if symbol in symbols:
                return symbol
            c = re.sub(r"[/._]", "", symbol).upper()
            for s in symbols:
                sc = re.sub(r"[/._]", "", s).upper()
                if c in sc or sc in c:
                    return s
            return symbol
        except Exception:
            return symbol

    async def close(self, pid):
        try:
            await self.conn.close_position(str(pid))
            log(f" Closed Trade #{pid}")
            await self.refresh()
            return True
        except Exception as e:
            log(f"Close error: {e}")
            return False

    async def open_trade(self, side, symbol, lot, sl_pts, tp_pts):
        # HARD RISK CHECK: Stop trading if Daily Loss Limit is exceeded
        if S["day_pnl"] <= -abs(S["max_daily_loss_usd"]):
            log(f"🛑 DAILY LOSS PROTECTION TRIGGERED (-${abs(S['day_pnl']):.2f}). Trading halted to protect capital.")
            S["running"] = False
            return False

        try:
            px = await self.conn.get_symbol_price(symbol)
            bid, ask = float(px["bid"]), float(px["ask"])
            entry = ask if side == "BUY" else bid
            
            # Simple point calculation
            point = 0.01 if "XAU" in symbol or "BTC" in symbol else 0.0001
            sl_dist = sl_pts * point
            tp_dist = tp_pts * point

            sl = round(entry - sl_dist if side == "BUY" else entry + sl_dist, 2)
            tp = round(entry + tp_dist if side == "BUY" else entry - tp_dist, 2)

            log(f" Entry Attempt: {side} {symbol} Lot:{lot} @ {entry:.2f} | SL:{sl} | TP:{tp}")

            if side == "BUY":
                res = await self.conn.create_market_buy_order(symbol, lot, sl, tp)
            else:
                res = await self.conn.create_market_sell_order(symbol, lot, sl, tp)

            log(f" Order Executed: {res.get('stringCode', 'OK')}")
            await asyncio.sleep(1.0)
            await self.refresh()
            return True
        except Exception as e:
            log(f"❌ Order Failed: {e}")
            return False

    async def run(self):
        log("🔥 Engine Running with Active Risk Protection")
        symbol = await self.resolve(S["symbol"])
        S["real_symbol"] = symbol

        while S["running"] and S["connected"]:
            try:
                await self.refresh()

                # 1. Check Hard Daily Loss Limit
                if S["day_pnl"] <= -abs(S["max_daily_loss_usd"]):
                    log(f"🛑 Equity Guard Activated: Daily loss of -${abs(S['day_pnl']):.2f} reached limit. Stopping bot.")
                    S["running"] = False
                    break

                # 2. Price Update
                px = await self.conn.get_symbol_price(symbol)
                bid, ask = float(px["bid"]), float(px["ask"])
                mid = (bid + ask) / 2.0
                S["tick"] = {"bid": bid, "ask": ask}
                
                self.prices.append(mid)
                if len(self.prices) > 50:
                    self.prices.pop(0)

                # 3. Simple Momentum Check
                side = "WAITING"
                if len(self.prices) >= 5:
                    change = self.prices[-1] - self.prices[-4]
                    if change > 0.10:
                        side = "BUY"
                    elif change < -0.10:
                        side = "SELL"

                S["direction"] = side

                # 4. Manage Open Positions (Take Profit Check)
                for p in list(S["positions"]):
                    if p["profit"] >= S["target_profit"]:
                        log(f" Target Profit of ${p['profit']:.2f} hit. Closing trade #{p['id']}")
                        await self.close(p["id"])

                # 5. Open Trade if Slot Available
                if len(S["positions"]) < S["max_trades"] and side in ("BUY", "SELL"):
                    await self.open_trade(side, symbol, S["lot"], S["sl_points"], S["tp_points"])

            except Exception as e:
                log(f"Loop Notice: {e}")

            await asyncio.sleep(1.5)

        log("Engine Halted.")

engine = RiskManagedEngine()

# ---------- DASHBOARD UI ----------
HTML = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Capital-Protected Trading Bot</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
<style>
body { background:#0a0e17; color:#c9d1d9; font-family:system-ui; padding:15px; }
.card { background:#121824; border:1px solid #243247; border-radius:12px; padding:15px; margin-bottom:12px; }
.btn-go { background:#238636; border:0; color:#fff; font-weight:bold; width:100%; padding:10px; border-radius:8px; }
.btn-stop { background:#da3633; border:0; color:#fff; font-weight:bold; width:100%; padding:10px; border-radius:8px; }
.log { background:#05080f; height:200px; overflow-y:auto; font-family:monospace; font-size:11px; color:#3fb950; padding:10px; border-radius:8px; }
</style>
</head>
<body>
<div class="container" style="max-width:500px;">
  <h4 class="text-white mb-3">🛡️ Protected Trading Terminal</h4>
  
  <div class="card">
    <div class="row text-center">
      <div class="col-6"><small class="text-muted">Balance</small><h5 id="bal">$0.00</h5></div>
      <div class="col-6"><small class="text-muted">Daily P/L</small><h5 id="pnl">$0.00</h5></div>
    </div>
  </div>

  <div class="card">
    <div class="row g-2 mb-3">
      <div class="col-6">
        <label class="form-label text-muted">Lot Size</label>
        <input id="lot" type="number" class="form-control bg-dark text-light border-secondary" value="0.01" step="0.01">
      </div>
      <div class="col-6">
        <label class="form-label text-muted">Daily Loss Limit ($)</label>
        <input id="maxLoss" type="number" class="form-control bg-dark text-light border-secondary" value="5.0">
      </div>
    </div>
    <div class="row g-2">
      <div class="col-6"><button id="btnStart" onclick="startBot()" class="btn-go">RUN BOT</button></div>
      <div class="col-6"><button id="btnStop" onclick="stopBot()" class="btn-stop">STOP BOT</button></div>
    </div>
  </div>

  <div class="card">
    <h6>Console Logs</h6>
    <div id="log" class="log"></div>
  </div>
</div>

<script>
async function refresh() {
  try {
    const res = await fetch('/api/status');
    const data = await res.json();
    document.getElementById('bal').innerText = '$' + data.balance.toFixed(2);
    document.getElementById('pnl').innerText = (data.day_pnl >= 0 ? '+$' : '-$') + Math.abs(data.day_pnl).toFixed(2);
    document.getElementById('pnl').style.color = data.day_pnl >= 0 ? '#3fb950' : '#f85149';
    document.getElementById('log').innerHTML = data.logs.join('<br>');
    document.getElementById('log').scrollTop = 99999;
  } catch(e) {}
}

async function startBot() {
  await fetch('/api/start', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({
      lot: parseFloat(document.getElementById('lot').value),
      max_loss: parseFloat(document.getElementById('maxLoss').value)
    })
  });
}

async function stopBot() {
  await fetch('/api/stop', {method: 'POST'});
}

setInterval(refresh, 1000);
</script>
</body>
</html>
"""

# ---------- API ROUTES ----------
@app.route("/")
def home():
    return render_template_string(HTML)

@app.route("/api/status")
def api_status():
    return jsonify(S)

@app.route("/api/connect", methods=["POST"])
def api_connect():
    data = request.json or {}
    token = (data.get("token") or "").strip()
    if not token:
        return jsonify(success=False, message="Token required")
    try:
        ok, msg = run_async(engine.connect_id(token, data.get("account_id", "")))
        return jsonify(success=ok, message=msg)
    except Exception as e:
        return jsonify(success=False, message=str(e))

@app.route("/api/start", methods=["POST"])
def api_start():
    if not S["connected"]:
        return jsonify(ok=False, message="Connect first")
    data = request.json or {}
    S["lot"] = float(data.get("lot", 0.01))
    S["max_daily_loss_usd"] = float(data.get("max_loss", 5.0))
    S["running"] = True
    asyncio.run_coroutine_threadsafe(engine.run(), _loop)
    return jsonify(ok=True)

@app.route("/api/stop", methods=["POST"])
def api_stop():
    S["running"] = False
    log("Stop Requested by User.")
    return jsonify(ok=True)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
