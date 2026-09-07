import os
import asyncio
import threading
import logging
import re
from datetime import datetime
from flask import Flask, render_template_string, request, jsonify
from metaapi_cloud_sdk import MetaApi

logging.basicConfig(level=logging.INFO)
app = Flask(__name__)

# ============== BACKGROUND LOOP ==============
bg_loop = None
def start_bg_loop():
    global bg_loop
    bg_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(bg_loop)
    bg_loop.run_forever()
threading.Thread(target=start_bg_loop, daemon=True).start()

# ============== BOT STATE ==============
bot = {
    "connected": False,
    "running": False,
    "status": "Offline",
    "error": "",
    "token": "",
    "account_id": "",
    "login": "",
    "server": "",
    "balance": 0.0,
    "equity": 0.0,
    "profit": 0.0,
    "symbol": "BTCUSD",
    "real_symbol": "BTCUSD",
    "lot": 0.01,
    "max_trades": 1,
    "target_profit": 0.50,
    "sl_points": 800,
    "positions": [],
    "tick": {"bid": 0.0, "ask": 0.0},
    "direction": "WAITING",
    "logs": ["Scalper ready. Connect your MT4/MT5 account."]
}

def log(msg):
    t = datetime.now().strftime("%H:%M:%S")
    bot["logs"].append(f"[{t}] {msg}")
    if len(bot["logs"]) > 100:
        bot["logs"].pop(0)
    print(msg)

# ============== ENGINE ==============
class Scalper:
    def __init__(self):
        self.api = None
        self.account = None
        self.conn = None
        self.prices = []
        self.specs = {}

    def fix_server(self, s):
        if not s:
            return s
        s = s.strip()
        s = re.sub(r"TriaI", "Trial", s, flags=re.I)
        s = re.sub(r"Triai", "Trial", s, flags=re.I)
        return s

    async def get_all_accounts(self):
        """Compatible with metaapi-cloud-sdk v28+"""
        try:
            # New method (v20+)
            return await self.api.metatrader_account_api.get_accounts_with_infinite_scroll_pagination()
        except Exception:
            try:
                # Very old method fallback
                return await self.api.metatrader_account_api.get_accounts()
            except Exception as e:
                log(f"Could not list accounts: {e}")
                return []

    async def connect_id(self, token, acc_id):
        try:
            log(f"Connecting with Account ID: {acc_id}")
            self.api = MetaApi(token)
            self.account = await self.api.metatrader_account_api.get_account(acc_id)
            return await self._ready()
        except Exception as e:
            bot["error"] = str(e)
            bot["status"] = "Failed"
            log(f"❌ {e}")
            return False, str(e)

    async def connect_login(self, token, login, password, server, platform):
        try:
            server = self.fix_server(server)
            log(f"Connecting → Server: {server} | Login: {login}")
            self.api = MetaApi(token)

            accounts = await self.get_all_accounts()
            found = None
            for a in accounts:
                if str(a.login) == str(login) and server.lower() in str(a.server).lower():
                    found = a
                    break

            if found:
                log(f"Found existing account: {found.id}")
                self.account = found
            else:
                log("Creating new cloud account (20-40 sec)...")
                self.account = await self.api.metatrader_account_api.create_account({
                    "name": f"Scalper-{login}",
                    "type": "cloud",
                    "login": str(login),
                    "password": str(password),
                    "server": server,
                    "platform": "mt5" if "5" in str(platform).lower() else "mt4",
                    "magic": 123456
                })
                log(f"Account created: {self.account.id}")

            return await self._ready()
        except Exception as e:
            bot["error"] = str(e)
            bot["status"] = "Failed"
            log(f"❌ {e}")
            return False, str(e)

    async def _ready(self):
        bot["account_id"] = self.account.id
        bot["login"] = str(getattr(self.account, "login", ""))
        bot["server"] = str(getattr(self.account, "server", ""))
        bot["account_type"] = str(getattr(self.account, "type", "cloud")).upper()

        if self.account.state != "DEPLOYED":
            log("Deploying terminal...")
            await self.account.deploy()

        log("Waiting for broker connection...")
        await self.account.wait_connected()

        self.conn = self.account.get_rpc_connection()
        await self.conn.connect()
        await self.conn.wait_synchronized()

        await self.refresh()
        bot["connected"] = True
        bot["status"] = f"Online • {bot.get('account_type', 'CLOUD')}"
        bot["error"] = ""
        log("✅ Connected & ready to scalp")
        return True, "OK"

    async def refresh(self):
        try:
            info = await self.conn.get_account_information()
            bot["balance"] = float(info.get("balance", 0))
            bot["equity"] = float(info.get("equity", 0))
            bot["profit"] = round(bot["equity"] - bot["balance"], 2)

            pos = await self.conn.get_positions()
            bot["positions"] = []
            for p in pos:
                t = "BUY" if "BUY" in str(p.get("type", "")) else "SELL"
                bot["positions"].append({
                    "id": str(p.get("id")),
                    "symbol": p.get("symbol"),
                    "type": t,
                    "volume": float(p.get("volume", 0.01)),
                    "open": float(p.get("openPrice", 0)),
                    "current": float(p.get("currentPrice", 0)),
                    "profit": float(p.get("profit", 0))
                })
        except Exception:
            pass

    async def resolve(self, sym):
        try:
            symbols = await self.conn.get_symbols()
            if sym in symbols:
                return sym
            clean = sym.replace("/", "").replace(".", "").replace("_", "").upper()
            for s in symbols:
                s_clean = s.replace("/", "").replace(".", "").replace("_", "").upper()
                if clean in s_clean or s_clean in clean:
                    log(f"Symbol resolved: {sym} → {s}")
                    return s
            return sym
        except Exception:
            return sym

    async def get_spec(self, sym):
        if sym not in self.specs:
            try:
                s = await self.conn.get_symbol_specification(sym)
                self.specs[sym] = {"digits": s.get("digits", 2), "point": s.get("point", 0.01)}
            except Exception:
                self.specs[sym] = {"digits": 2, "point": 0.01}
        return self.specs[sym]

    async def close(self, pid):
        try:
            await self.conn.close_position(str(pid))
            log(f"💰 Closed #{pid} in profit")
            await self.refresh()
            return True
        except Exception:
            try:
                await self.conn.close_position(int(pid))
                log(f"💰 Closed #{pid} in profit")
                await self.refresh()
                return True
            except Exception as e:
                log(f"Close error: {e}")
                return False

    async def open_trade(self, side, sym, lot, sl_pts):
        try:
            spec = await self.get_spec(sym)
            digits = spec["digits"]
            point = spec["point"]
            price = await self.conn.get_symbol_price(sym)
            entry = price["ask"] if side == "BUY" else price["bid"]

            scale = 1.0 if "BTC" in sym or "ETH" in sym else point
            sl = entry - sl_pts * scale if side == "BUY" else entry + sl_pts * scale
            tp = entry + sl_pts * 1.8 * scale if side == "BUY" else entry - sl_pts * 1.8 * scale

            log(f"🚀 {side} {sym} | Lot {lot} @ {entry:.2f}")

            if side == "BUY":
                r = await self.conn.create_market_buy_order(sym, lot, round(sl, digits), round(tp, digits))
            else:
                r = await self.conn.create_market_sell_order(sym, lot, round(sl, digits), round(tp, digits))

            log(f"✅ Opened → {r.get('stringCode', 'OK')}")
            await self.refresh()
        except Exception as e:
            log(f"Open error: {e}")

    async def run(self):
        log("🔥 SCALPER STARTED – hunting micro moves")
        bot["real_symbol"] = await self.resolve(bot["symbol"])
        sym = bot["real_symbol"]

        try:
            await self.conn.subscribe_to_market_data(sym)
        except Exception:
            pass

        while bot["running"] and bot["connected"]:
            try:
                tick = await self.conn.get_symbol_price(sym)
                bid = float(tick["bid"])
                ask = float(tick["ask"])
                mid = (bid + ask) / 2
                bot["tick"] = {"bid": bid, "ask": ask}

                self.prices.append(mid)
                if len(self.prices) > 12:
                    self.prices.pop(0)

                direction = "WAITING"
                if len(self.prices) >= 6:
                    recent = self.prices[-1] - self.prices[-3]
                    older = self.prices[-3] - self.prices[-6]
                    th = 2.5 if "BTC" in sym else 0.00012

                    if recent > th and recent > older * 0.5:
                        direction = "BUY"
                        bot["direction"] = "UP 🚀"
                    elif recent < -th and recent < older * 0.5:
                        direction = "SELL"
                        bot["direction"] = "DOWN 📉"
                    else:
                        bot["direction"] = "FLAT"

                # Auto close when target profit reached
                for p in list(bot["positions"]):
                    if p["symbol"].replace("T", "") in sym.replace("T", "") or sym.replace("T", "") in p["symbol"].replace("T", ""):
                        if p["profit"] >= bot["target_profit"]:
                            log(f"🎯 Target ${p['profit']:.2f} hit → closing")
                            await self.close(p["id"])

                # Open new trade
                my_pos = [p for p in bot["positions"]
                          if p["symbol"].replace("T", "") in sym.replace("T", "") or sym.replace("T", "") in p["symbol"].replace("T", "")]
                if len(my_pos) < bot["max_trades"] and direction in ("BUY", "SELL"):
                    log(f"⚡ Clear {direction} move → entering now")
                    await self.open_trade(direction, sym, bot["lot"], bot["sl_points"])

                await self.refresh()

            except Exception as e:
                log(f"Loop: {e}")

            await asyncio.sleep(0.4)

        log("Scalper stopped")

engine = Scalper()

# ============== UI ==============
HTML = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>MT4/MT5 Scalper Bot</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
<script src="https://s3.tradingview.com/tv.js"></script>
<style>
body{background:#0a0e17;color:#c9d1d9;font-family:system-ui}
.card{background:#161b22;border:1px solid #30363d;border-radius:12px;margin-bottom:14px}
.nav-pills .nav-link{color:#8b949e;font-weight:600}
.nav-pills .nav-link.active{background:#238636;color:#fff}
.form-control,.form-select{background:#0d1117;border-color:#30363d;color:#e6edf3}
.btn-success{background:#238636;border:none}
.btn-danger{background:#da3633;border:none}
.log{background:#010409;border:1px solid #30363d;height:220px;overflow-y:auto;font-family:monospace;font-size:12px;padding:10px;border-radius:8px;color:#3fb950}
.online{color:#3fb950} .offline{color:#f85149}
.big{font-size:20px;font-weight:800;font-family:monospace}
</style>
</head>
<body class="p-3">
<div class="container" style="max-width:900px">

<div class="d-flex justify-content-between align-items-center card p-3 mb-3">
  <div>
    <h4 class="text-white m-0">⚡ MT4/MT5 Scalper Bot</h4>
    <small id="accInfo" class="text-muted">Not connected</small>
  </div>
  <span id="status" class="offline">● Offline</span>
</div>

<div class="row g-2 mb-3">
  <div class="col-6 col-md-3"><div class="card p-2 text-center"><small>Bid / Ask</small><div class="big text-success" id="price">0 / 0</div></div></div>
  <div class="col-6 col-md-3"><div class="card p-2 text-center"><small>Balance</small><div class="big text-info" id="bal">$0</div></div></div>
  <div class="col-6 col-md-3"><div class="card p-2 text-center"><small>Floating</small><div class="big" id="pl">$0</div></div></div>
  <div class="col-6 col-md-3"><div class="card p-2 text-center"><small>Direction</small><div class="big text-warning" id="dir">WAITING</div></div></div>
</div>

<ul class="nav nav-pills mb-3">
  <li class="nav-item"><button class="nav-link active" data-t="live">Live</button></li>
  <li class="nav-item"><button class="nav-link" data-t="set">Settings</button></li>
  <li class="nav-item"><button class="nav-link" data-t="conn">Connect</button></li>
</ul>

<div id="t-live">
  <div class="card p-2 mb-3"><div id="tv" style="height:340px"></div></div>
  <div class="card p-3">
    <div class="d-flex justify-content-between mb-2">
      <h6 class="text-white m-0">Open Trades</h6>
      <div>
        <button id="start" class="btn btn-success btn-sm" disabled onclick="start()">▶ RUN</button>
        <button id="stop" class="btn btn-danger btn-sm" disabled onclick="stop()">⏹ STOP</button>
      </div>
    </div>
    <div class="table-responsive">
      <table class="table table-dark table-sm mb-0" style="font-size:12px">
        <thead><tr><th>Symbol</th><th>Type</th><th>Lot</th><th>Entry</th><th>Now</th><th>Profit</th><th></th></tr></thead>
        <tbody id="pos"><tr><td colspan="7" class="text-center text-muted">No trades</td></tr></tbody>
      </table>
    </div>
  </div>
</div>

<div id="t-set" style="display:none">
  <div class="card p-3">
    <div class="row g-3">
      <div class="col-md-6">
        <label>Symbol</label>
        <select id="sym" class="form-select" onchange="chart()">
          <option value="BTCUSD">BTCUSD</option>
          <option value="ETHUSD">ETHUSD</option>
          <option value="XAUUSD">XAUUSD</option>
          <option value="EURUSD">EURUSD</option>
        </select>
      </div>
      <div class="col-md-6">
        <label>Target Profit ($)</label>
        <input type="number" id="target" class="form-control" value="0.50" step="0.10">
      </div>
      <div class="col-6">
        <label>Lot Size</label>
        <input type="number" id="lot" class="form-control" value="0.01" step="0.01">
      </div>
      <div class="col-6">
        <label>Max Trades</label>
        <input type="number" id="max" class="form-control" value="1">
      </div>
    </div>
  </div>
</div>

<div id="t-conn" style="display:none">
  <div class="card p-3">
    <div class="mb-3">
      <label>MetaApi Token</label>
      <input type="password" id="token" class="form-control">
    </div>
    <div class="mb-3">
      <label>Method</label>
      <select id="method" class="form-select" onchange="toggle()">
        <option value="id">Account ID (Recommended)</option>
        <option value="login">Login + Password + Server</option>
      </select>
    </div>
    <div id="box-id">
      <input type="text" id="accid" class="form-control mb-3" placeholder="Paste Account ID here">
    </div>
    <div id="box-login" style="display:none">
      <select id="plat" class="form-select mb-2"><option value="mt5">MT5</option><option value="mt4">MT4</option></select>
      <input type="text" id="login" class="form-control mb-2" placeholder="Login" value="476924559">
      <input type="password" id="pass" class="form-control mb-2" placeholder="Password">
      <input type="text" id="server" class="form-control mb-2" placeholder="Exness-MT5Trial9" value="Exness-MT5Trial9">
    </div>
    <button class="btn btn-success w-100" onclick="connect()">Connect</button>
  </div>
</div>

<div class="card p-3">
  <h6 class="text-white">Live Log</h6>
  <div id="log" class="log"></div>
</div>
</div>

<script>
function toggle(){
  const m=document.getElementById('method').value;
  document.getElementById('box-id').style.display = m==='id' ? 'block' : 'none';
  document.getElementById('box-login').style.display = m==='login' ? 'block' : 'none';
}
document.querySelectorAll('[data-t]').forEach(b=>{
  b.onclick=()=>{
    document.querySelectorAll('.nav-link').forEach(x=>x.classList.remove('active'));
    b.classList.add('active');
    document.getElementById('t-live').style.display = b.dataset.t==='live'?'block':'none';
    document.getElementById('t-set').style.display = b.dataset.t==='set'?'block':'none';
    document.getElementById('t-conn').style.display = b.dataset.t==='conn'?'block':'none';
  }
});
function chart(){
  const s=document.getElementById('sym').value;
  let tv="BINANCE:BTCUSDT";
  if(s.includes("ETH")) tv="BINANCE:ETHUSDT";
  if(s.includes("XAU")) tv="OANDA:XAUUSD";
  if(s.includes("EUR")) tv="FX:EURUSD";
  document.getElementById('tv').innerHTML="";
  new TradingView.widget({autosize:true,symbol:tv,interval:"1",theme:"dark",container_id:"tv"});
}
async function refresh(){
  try{
    const r=await fetch('/api/status'); const d=await r.json();
    document.getElementById('status').textContent='● '+d.status;
    document.getElementById('status').className=d.connected?'online':'offline';
    document.getElementById('accInfo').textContent=d.connected?`Server: ${d.server} | ${d.login}`:'Not connected';
    document.getElementById('price').textContent=d.tick.bid.toFixed(2)+' / '+d.tick.ask.toFixed(2);
    document.getElementById('bal').textContent='$'+d.balance.toFixed(2);
    document.getElementById('pl').textContent=(d.profit>=0?'+':'')+d.profit.toFixed(2);
    document.getElementById('pl').className=d.profit>=0?'big text-success':'big text-danger';
    document.getElementById('dir').textContent=d.direction;
    document.getElementById('log').innerHTML=d.logs.join('<br>');
    document.getElementById('log').scrollTop=9999;

    let h='';
    if(d.positions && d.positions.length){
      d.positions.forEach(p=>{
        h+=`<tr>
          <td>${p.symbol}</td>
          <td><span class="badge ${p.type==='BUY'?'bg-success':'bg-danger'}">${p.type}</span></td>
          <td>${p.volume}</td><td>${p.open}</td><td>${p.current}</td>
          <td class="${p.profit>=0?'text-success':'text-danger'}">$${p.profit.toFixed(2)}</td>
          <td><button class="btn btn-danger btn-sm py-0" onclick="closePos('${p.id}')">Close</button></td>
        </tr>`;
      });
    } else h='<tr><td colspan="7" class="text-center text-muted">No trades</td></tr>';
    document.getElementById('pos').innerHTML=h;
    document.getElementById('start').disabled=!d.connected||d.running;
    document.getElementById('stop').disabled=!d.running;
  }catch(e){}
}
async function connect(){
  const body={
    token: document.getElementById('token').value.trim(),
    method: document.getElementById('method').value,
    account_id: document.getElementById('accid').value.trim(),
    login: document.getElementById('login').value.trim(),
    password: document.getElementById('pass').value,
    server: document.getElementById('server').value.trim(),
    platform: document.getElementById('plat').value
  };
  const r=await fetch('/api/connect',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
  const d=await r.json();
  alert(d.success ? '✅ Connected successfully!' : '❌ '+d.message);
}
async function start(){
  await fetch('/api/start',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({
    symbol: document.getElementById('sym').value,
    lot: parseFloat(document.getElementById('lot').value),
    max_trades: parseInt(document.getElementById('max').value),
    target_profit: parseFloat(document.getElementById('target').value)
  })});
}
async function stop(){ await fetch('/api/stop',{method:'POST'}); }
async function closePos(id){
  if(confirm('Close this trade?')) await fetch('/api/close',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id})});
}
window.onload=()=>{ chart(); setInterval(refresh,800); };
</script>
</body>
</html>
"""

@app.route("/")
def home():
    return render_template_string(HTML)

@app.route("/api/status")
def status():
    return jsonify(bot)

@app.route("/api/connect", methods=["POST"])
def api_connect():
    data = request.json or {}
    token = data.get("token", "").strip()
    if not token:
        return jsonify(success=False, message="Token required")

    bot["token"] = token
    bot["status"] = "Connecting..."
    bot["error"] = ""

    async def do():
        if data.get("method") == "id":
            return await engine.connect_id(token, data.get("account_id", "").strip())
        return await engine.connect_login(
            token,
            data.get("login"),
            data.get("password"),
            data.get("server"),
            data.get("platform", "mt5")
        )

    fut = asyncio.run_coroutine_threadsafe(do(), bg_loop)
    try:
        ok, msg = fut.result(timeout=100)
        return jsonify(success=ok, message=msg)
    except Exception as e:
        bot["error"] = str(e)
        bot["status"] = "Failed"
        return jsonify(success=False, message=str(e))

@app.route("/api/start", methods=["POST"])
def api_start():
    if not bot["connected"]:
        return jsonify(ok=False)
    data = request.json or {}
    bot["symbol"] = data.get("symbol", "BTCUSD").upper()
    bot["lot"] = float(data.get("lot", 0.01))
    bot["max_trades"] = int(data.get("max_trades", 1))
    bot["target_profit"] = float(data.get("target_profit", 0.50))
    bot["running"] = True
    asyncio.run_coroutine_threadsafe(engine.run(), bg_loop)
    return jsonify(ok=True)

@app.route("/api/stop", methods=["POST"])
def api_stop():
    bot["running"] = False
    log("Stop requested")
    return jsonify(ok=True)

@app.route("/api/close", methods=["POST"])
def api_close():
    pid = (request.json or {}).get("id")
    if pid and bot["connected"]:
        asyncio.run_coroutine_threadsafe(engine.close(pid), bg_loop)
    return jsonify(ok=True)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
