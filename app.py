import os
import re
import asyncio
import threading
import logging
from datetime import datetime

from flask import Flask, render_template_string, request, jsonify
from metaapi_cloud_sdk import MetaApi

logging.basicConfig(level=logging.INFO)
app = Flask(__name__)

_loop = None
def _bg():
    global _loop
    _loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_loop)
    _loop.run_forever()
threading.Thread(target=_bg, daemon=True).start()

def run_async(coro, timeout=120):
    return asyncio.run_coroutine_threadsafe(coro, _loop).result(timeout=timeout)

S = {
    "connected": False,
    "running": False,
    "status": "Offline",
    "error": "",
    "balance": 0.0,
    "equity": 0.0,
    "profit": 0.0,
    "login": "",
    "server": "",
    "account_id": "",
    "symbol": "XAUUSD",
    "real_symbol": "XAUUSD",
    "lot": 0.01,
    "sl_points": 250,
    "tp_points": 500,
    "target_profit": 1.0,
    "max_daily_loss_usd": 5.0,
    "day_start_balance": 0.0,
    "day_pnl": 0.0,
    "positions": [],
    "tick": {"bid": 0.0, "ask": 0.0},
    "direction": "WAITING",
    "logs": ["Connect using Account ID (recommended) or Login+Password+Server."]
}

def log(msg):
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    S["logs"].append(line)
    if len(S["logs"]) > 120:
        S["logs"].pop(0)
    print(line)

class Engine:
    def __init__(self):
        self.api = None
        self.account = None
        self.conn = None
        self.prices = []

    def fix_server(self, server):
        if not server:
            return ""
        server = server.strip()
        server = re.sub(r"TriaI", "Trial", server, flags=re.I)
        server = re.sub(r"Triai", "Trial", server, flags=re.I)
        return server

    async def list_accounts(self):
        api = self.api.metatrader_account_api
        try:
            return await api.get_accounts_with_infinite_scroll_pagination()
        except Exception:
            try:
                return await api.get_accounts()
            except Exception:
                return []

    async def connect_with_account_id(self, token, account_id):
        account_id = (account_id or "").strip()
        if not account_id or account_id == "1000" or account_id.isdigit():
            raise Exception(
                "Invalid MetaApi Account ID. Use the UUID from app.metaapi.cloud "
                "(example: b5978e20-507a-4b9a-b1bf-aca87dcfec95), not 1000 and not your MT5 login."
            )
        log(f"Connecting by Account ID: {account_id}")
        self.api = MetaApi(token)
        self.account = await self.api.metatrader_account_api.get_account(account_id)
        return await self._ready()

    async def connect_with_login(self, token, login, password, server, platform):
        server = self.fix_server(server)
        login = str(login or "").strip()
        password = str(password or "")
        platform = "mt5" if "5" in str(platform).lower() else "mt4"

        if not login or not password or not server:
            raise Exception("Login, password and server are required")

        log(f"Connecting by credentials: server={server} login={login} platform={platform}")
        self.api = MetaApi(token)

        accounts = await self.list_accounts()
        found = None
        for a in accounts:
            if str(getattr(a, "login", "")) == login and server.lower() in str(getattr(a, "server", "")).lower():
                found = a
                break

        if found:
            log(f"Found existing MetaApi account: {found.id}")
            self.account = found
        else:
            log("Creating new MetaApi cloud account...")
            self.account = await self.api.metatrader_account_api.create_account({
                "name": f"Bot-{login}",
                "type": "cloud",
                "login": login,
                "password": password,
                "server": server,
                "platform": platform,
                "magic": 260318
            })
            log(f"Created account: {self.account.id}")

        return await self._ready()

    async def _ready(self):
        S["account_id"] = self.account.id
        S["login"] = str(getattr(self.account, "login", ""))
        S["server"] = str(getattr(self.account, "server", ""))

        if self.account.state != "DEPLOYED":
            log("Deploying account...")
            await self.account.deploy()

        log("Waiting for broker connection...")
        await self.account.wait_connected()
        self.conn = self.account.get_rpc_connection()
        await self.conn.connect()
        await self.conn.wait_synchronized()
        await self.refresh()

        S["connected"] = True
        S["status"] = "Online"
        S["day_start_balance"] = S["balance"]
        S["error"] = ""
        log("✅ Connected successfully")
        return True, "Connected"

    async def refresh(self):
        if not self.conn:
            return
        try:
            info = await self.conn.get_account_information()
            S["balance"] = float(info.get("balance", 0))
            S["equity"] = float(info.get("equity", 0))
            S["profit"] = round(S["equity"] - S["balance"], 2)
            if S["day_start_balance"]:
                S["day_pnl"] = round(S["equity"] - S["day_start_balance"], 2)

            positions = await self.conn.get_positions()
            out = []
            for p in positions:
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
        except Exception as e:
            log(f"Refresh note: {e}")

    async def resolve(self, symbol):
        try:
            symbols = await self.conn.get_symbols()
            if symbol in symbols:
                return symbol
            c = re.sub(r"[/._]", "", symbol.upper())
            for s in symbols:
                sc = re.sub(r"[/._]", "", s.upper())
                if c in sc or sc in c:
                    log(f"Symbol {symbol} → {s}")
                    return s
            return symbol
        except Exception:
            return symbol

    async def close(self, pid):
        try:
            await self.conn.close_position(str(pid))
            log(f"Closed #{pid}")
            await self.refresh()
            return True
        except Exception as e:
            log(f"Close error: {e}")
            return False

    async def open_trade(self, side, symbol):
        # daily loss guard
        if S["day_pnl"] <= -abs(float(S["max_daily_loss_usd"])):
            log("Daily loss limit reached. Bot stopped for protection.")
            S["running"] = False
            return False
        try:
            px = await self.conn.get_symbol_price(symbol)
            bid, ask = float(px["bid"]), float(px["ask"])
            entry = ask if side == "BUY" else bid
            point = 0.01 if ("XAU" in symbol or "BTC" in symbol) else 0.0001
            sl = entry - S["sl_points"] * point if side == "BUY" else entry + S["sl_points"] * point
            tp = entry + S["tp_points"] * point if side == "BUY" else entry - S["tp_points"] * point
            sl, tp = round(sl, 2), round(tp, 2)
            lot = float(S["lot"])

            log(f"Opening {side} {symbol} lot={lot} entry={entry:.2f} SL={sl} TP={tp}")
            if side == "BUY":
                res = await self.conn.create_market_buy_order(symbol, lot, sl, tp)
            else:
                res = await self.conn.create_market_sell_order(symbol, lot, sl, tp)
            log(f"Order result: {res}")
            await asyncio.sleep(1)
            await self.refresh()
            return True
        except Exception as e:
            log(f"Order failed: {e}")
            return False

    async def run(self):
        symbol = await self.resolve(S["symbol"])
        S["real_symbol"] = symbol
        log(f"Engine started on {symbol}")

        while S["running"] and S["connected"]:
            try:
                await self.refresh()
                if S["day_pnl"] <= -abs(float(S["max_daily_loss_usd"])):
                    log("Protection stop: daily loss limit hit")
                    S["running"] = False
                    break

                px = await self.conn.get_symbol_price(symbol)
                bid, ask = float(px["bid"]), float(px["ask"])
                mid = (bid + ask) / 2.0
                S["tick"] = {"bid": bid, "ask": ask}
                self.prices.append(mid)
                if len(self.prices) > 30:
                    self.prices.pop(0)

                side = "WAITING"
                if len(self.prices) >= 5:
                    ch = self.prices[-1] - self.prices[-4]
                    if ch > 0.08:
                        side = "BUY"
                    elif ch < -0.08:
                        side = "SELL"
                S["direction"] = side

                # manage TP in USD
                for p in list(S["positions"]):
                    if p["profit"] >= float(S["target_profit"]):
                        log(f"Target reached ${p['profit']:.2f} -> close #{p['id']}")
                        await self.close(p["id"])

                if len(S["positions"]) == 0 and side in ("BUY", "SELL"):
                    await self.open_trade(side, symbol)
                else:
                    log(f"{symbol} {bid:.2f}/{ask:.2f} dir={side} positions={len(S['positions'])} dayPnL={S['day_pnl']:.2f}")

            except Exception as e:
                log(f"Loop: {e}")
            await asyncio.sleep(1.2)

        log("Engine stopped")

engine = Engine()

HTML = r"""
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>MT Connect Fix</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
<style>
body{background:#0b0f17;color:#d0d7de;font-family:system-ui;padding:14px}
.card{background:#121a24;border:1px solid #243041;border-radius:12px;padding:14px;margin-bottom:12px}
.form-control,.form-select{background:#0d141e;border-color:#2a3648;color:#e6edf3}
.btn-go{background:#238636;border:0;color:#fff;font-weight:700;width:100%;padding:10px;border-radius:10px}
.btn-stop{background:#da3633;border:0;color:#fff;font-weight:700;width:100%;padding:10px;border-radius:10px}
.log{height:220px;overflow:auto;background:#070b12;border:1px solid #243041;border-radius:10px;padding:10px;font:12px monospace;color:#3fb950}
</style>
</head>
<body>
<div class="container" style="max-width:520px">
  <h4 class="text-white">MT5 Bot</h4>
  <div class="card">
    <div class="row text-center">
      <div class="col-4"><small>Balance</small><div id="bal">$0.00</div></div>
      <div class="col-4"><small>Equity</small><div id="eq">$0.00</div></div>
      <div class="col-4"><small>Day PnL</small><div id="pnl">$0.00</div></div>
    </div>
  </div>

  <div class="card">
    <label>MetaApi Token</label>
    <input id="token" type="password" class="form-control mb-2">
    <label>Method</label>
    <select id="method" class="form-select mb-2">
      <option value="id">Account ID (recommended)</option>
      <option value="login">Login + Password + Server</option>
    </select>
    <div id="box-id">
      <label>MetaApi Account ID (UUID)</label>
      <input id="accid" class="form-control mb-2" placeholder="b5978e20-507a-4b9a-b1bf-...">
      <small class="text-muted">Do NOT put 1000 or MT5 login here</small>
    </div>
    <div id="box-login" style="display:none">
      <select id="plat" class="form-select mb-2"><option value="mt5">MT5</option><option value="mt4">MT4</option></select>
      <input id="login" class="form-control mb-2" placeholder="MT5 Login" value="476924559">
      <input id="pass" type="password" class="form-control mb-2" placeholder="Password">
      <input id="server" class="form-control mb-2" value="Exness-MT5Trial9">
    </div>
    <button class="btn-go" onclick="connect()">Connect</button>
  </div>

  <div class="card">
    <div class="row g-2">
      <div class="col-6"><button class="btn-go" onclick="startBot()">RUN</button></div>
      <div class="col-6"><button class="btn-stop" onclick="stopBot()">STOP</button></div>
    </div>
  </div>

  <div class="card">
    <div id="log" class="log"></div>
  </div>
</div>
<script>
const $=id=>document.getElementById(id);
$('method').onchange=()=>{
  $('box-id').style.display=$('method').value==='id'?'block':'none';
  $('box-login').style.display=$('method').value==='login'?'block':'none';
};
async function refresh(){
  const d=await(await fetch('/api/status')).json();
  $('bal').textContent='$'+d.balance.toFixed(2);
  $('eq').textContent='$'+d.equity.toFixed(2);
  $('pnl').textContent=(d.day_pnl>=0?'+':'')+d.day_pnl.toFixed(2);
  $('log').innerHTML=(d.logs||[]).join('<br>');
  $('log').scrollTop=1e9;
}
async function connect(){
  const body={
    token:$('token').value.trim(),
    method:$('method').value,
    account_id:$('accid').value.trim(),
    login:$('login').value.trim(),
    password:$('pass').value,
    server:$('server').value.trim(),
    platform:$('plat').value
  };
  const d=await(await fetch('/api/connect',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)})).json();
  alert(d.success?'Connected':'Error: '+d.message);
}
async function startBot(){ await fetch('/api/start',{method:'POST'}); }
async function stopBot(){ await fetch('/api/stop',{method:'POST'}); }
setInterval(refresh,1000); refresh();
</script>
</body>
</html>
"""

@app.route("/")
def home():
    return render_template_string(HTML)

@app.route("/api/status")
def status():
    return jsonify(S)

@app.route("/api/connect", methods=["POST"])
def api_connect():
    data = request.json or {}
    token = (data.get("token") or "").strip()
    if not token:
        return jsonify(success=False, message="MetaApi token is required")

    method = (data.get("method") or "id").strip()
    S["status"] = "Connecting..."
    S["error"] = ""

    try:
        if method == "id":
            ok, msg = run_async(engine.connect_with_account_id(token, data.get("account_id", "")))
        else:
            ok, msg = run_async(engine.connect_with_login(
                token,
                data.get("login"),
                data.get("password"),
                data.get("server"),
                data.get("platform", "mt5"),
            ))
        return jsonify(success=ok, message=msg)
    except Exception as e:
        S["status"] = "Failed"
        S["error"] = str(e)
        log(f"Connect error: {e}")
        return jsonify(success=False, message=str(e))

@app.route("/api/start", methods=["POST"])
def api_start():
    if not S["connected"]:
        return jsonify(ok=False, message="Connect first")
    S["running"] = True
    if not S["day_start_balance"]:
        S["day_start_balance"] = S["balance"]
    asyncio.run_coroutine_threadsafe(engine.run(), _loop)
    return jsonify(ok=True)

@app.route("/api/stop", methods=["POST"])
def api_stop():
    S["running"] = False
    log("Stop requested")
    return jsonify(ok=True)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
