import os
import asyncio
import threading
import logging
import re
from datetime import datetime
from flask import Flask, render_template_string, request, jsonify, Response
from metaapi_cloud_sdk import MetaApi

logging.basicConfig(level=logging.INFO)
app = Flask(__name__)

# ================= BACKGROUND LOOP =================
bg_loop = None
def _bg():
    global bg_loop
    bg_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(bg_loop)
    bg_loop.run_forever()
threading.Thread(target=_bg, daemon=True).start()

# ================= STATE =================
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
    "symbol": "BTCUSD",
    "real_symbol": "BTCUSD",
    "lot": 0.01,
    "max_trades": 1,
    "target_profit": 0.50,
    "sl_points": 1000,
    "strategy": "MOMENTUM",
    "positions": [],
    "tick": {"bid": 0.0, "ask": 0.0},
    "direction": "WAITING",
    "last_action": "—",
    "logs": ["Bot ready. Add to Home Screen on mobile for app mode."]
}

def log(msg):
    t = datetime.now().strftime("%H:%M:%S")
    S["logs"].append(f"[{t}] {msg}")
    if len(S["logs"]) > 80:
        S["logs"].pop(0)
    logging.info(msg)

# ================= ENGINE =================
class Engine:
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

    async def list_accounts(self):
        try:
            return await self.api.metatrader_account_api.get_accounts_with_infinite_scroll_pagination()
        except Exception:
            try:
                return await self.api.metatrader_account_api.get_accounts()
            except Exception:
                return []

    async def connect_id(self, token, acc_id):
        try:
            log("Connecting with Account ID...")
            self.api = MetaApi(token)
            self.account = await self.api.metatrader_account_api.get_account(acc_id.strip())
            return await self._ready()
        except Exception as e:
            S["error"] = str(e)
            S["status"] = "Failed"
            log(f"❌ {e}")
            return False, str(e)

    async def connect_login(self, token, login, password, server, platform):
        try:
            server = self.fix_server(server)
            log(f"Connecting {server} | {login}")
            self.api = MetaApi(token)
            accounts = await self.list_accounts()
            found = None
            for a in accounts:
                if str(a.login) == str(login) and server.lower() in str(a.server).lower():
                    found = a
                    break
            if found:
                self.account = found
                log(f"Using existing account {found.id}")
            else:
                log("Creating cloud account...")
                self.account = await self.api.metatrader_account_api.create_account({
                    "name": f"Scalper-{login}",
                    "type": "cloud",
                    "login": str(login),
                    "password": str(password),
                    "server": server,
                    "platform": "mt5" if "5" in str(platform).lower() else "mt4",
                    "magic": 260318
                })
            return await self._ready()
        except Exception as e:
            S["error"] = str(e)
            S["status"] = "Failed"
            log(f"❌ {e}")
            return False, str(e)

    async def _ready(self):
        S["account_id"] = self.account.id
        S["login"] = str(getattr(self.account, "login", ""))
        S["server"] = str(getattr(self.account, "server", ""))
        S["account_type"] = str(getattr(self.account, "type", "cloud")).upper()
        if self.account.state != "DEPLOYED":
            log("Deploying terminal...")
            await self.account.deploy()
        await self.account.wait_connected()
        self.conn = self.account.get_rpc_connection()
        await self.conn.connect()
        await self.conn.wait_synchronized()
        await self.refresh()
        S["connected"] = True
        S["status"] = f"Online • {S['account_type']}"
        S["error"] = ""
        log("✅ Connected — ready")
        return True, "OK"

    async def refresh(self):
        if not self.conn:
            return
        try:
            info = await self.conn.get_account_information()
            S["balance"] = float(info.get("balance", 0))
            S["equity"] = float(info.get("equity", 0))
            S["profit"] = round(S["equity"] - S["balance"], 2)
            raw = await self.conn.get_positions()
            out = []
            for p in raw:
                t = "BUY" if "BUY" in str(p.get("type", "")) else "SELL"
                out.append({
                    "id": str(p.get("id")),
                    "symbol": p.get("symbol"),
                    "type": t,
                    "volume": float(p.get("volume", 0.01)),
                    "open": float(p.get("openPrice", 0)),
                    "current": float(p.get("currentPrice", 0)),
                    "profit": float(p.get("profit", 0))
                })
            S["positions"] = out
        except Exception:
            pass

    async def resolve(self, sym):
        try:
            symbols = await self.conn.get_symbols()
            if sym in symbols:
                return sym
            c = sym.replace("/", "").replace(".", "").replace("_", "").upper()
            for s in symbols:
                sc = s.replace("/", "").replace(".", "").replace("_", "").upper()
                if c in sc or sc in c:
                    log(f"Symbol {sym} → {s}")
                    return s
            return sym
        except Exception:
            return sym

    async def spec(self, sym):
        if sym not in self.specs:
            try:
                sp = await self.conn.get_symbol_specification(sym)
                self.specs[sym] = {"digits": sp.get("digits", 2), "point": sp.get("point", 0.01)}
            except Exception:
                self.specs[sym] = {"digits": 2, "point": 0.01}
        return self.specs[sym]

    async def close(self, pid):
        try:
            await self.conn.close_position(str(pid))
            log(f"💰 Closed #{pid}")
            S["last_action"] = f"Closed #{pid}"
            await self.refresh()
            return True
        except Exception:
            try:
                await self.conn.close_position(int(pid))
                log(f"💰 Closed #{pid}")
                await self.refresh()
                return True
            except Exception as e:
                log(f"Close error: {e}")
                return False

    async def open_trade(self, side, sym, lot, sl_pts):
        try:
            sp = await self.spec(sym)
            digits, point = sp["digits"], sp["point"]
            px = await self.conn.get_symbol_price(sym)
            entry = px["ask"] if side == "BUY" else px["bid"]
            scale = 1.0 if ("BTC" in sym or "ETH" in sym) else point
            sl = entry - sl_pts * scale if side == "BUY" else entry + sl_pts * scale
            tp = entry + sl_pts * 2 * scale if side == "BUY" else entry - sl_pts * 2 * scale
            log(f"🚀 {side} {sym} lot={lot} @ {entry:.2f}")
            S["last_action"] = f"{side} {sym}"
            if side == "BUY":
                await self.conn.create_market_buy_order(sym, lot, round(sl, digits), round(tp, digits))
            else:
                await self.conn.create_market_sell_order(sym, lot, round(sl, digits), round(tp, digits))
            log("✅ Order sent")
            await self.refresh()
        except Exception as e:
            log(f"Open error: {e}")

    def _match(self, pos_sym, sym):
        a = pos_sym.replace("T", "").replace(".", "")
        b = sym.replace("T", "").replace(".", "")
        return a in b or b in a

    async def run(self):
        log("🔥 AUTO SCALPER ON")
        S["real_symbol"] = await self.resolve(S["symbol"])
        sym = S["real_symbol"]
        try:
            await self.conn.subscribe_to_market_data(sym)
        except Exception:
            pass

        while S["running"] and S["connected"]:
            try:
                tick = await self.conn.get_symbol_price(sym)
                bid, ask = float(tick["bid"]), float(tick["ask"])
                mid = (bid + ask) / 2.0
                S["tick"] = {"bid": bid, "ask": ask}

                self.prices.append(mid)
                if len(self.prices) > 15:
                    self.prices.pop(0)

                direction = "WAITING"
                if len(self.prices) >= 6:
                    fast = self.prices[-1] - self.prices[-3]
                    slow = self.prices[-3] - self.prices[-6]
                    th = 2.0 if "BTC" in sym else (0.15 if "XAU" in sym else 0.00012)
                    if S["strategy"] == "MOMENTUM":
                        if fast > th and fast > slow * 0.4:
                            direction = "BUY"
                            S["direction"] = "UP 🚀"
                        elif fast < -th and fast < slow * 0.4:
                            direction = "SELL"
                            S["direction"] = "DOWN 📉"
                        else:
                            S["direction"] = "FLAT"
                    else:  # EMA-style on ticks
                        ema_f = sum(self.prices[-5:]) / 5
                        ema_s = sum(self.prices[-10:]) / min(10, len(self.prices))
                        if ema_f > ema_s and fast > th * 0.5:
                            direction = "BUY"
                            S["direction"] = "UP 🚀"
                        elif ema_f < ema_s and fast < -th * 0.5:
                            direction = "SELL"
                            S["direction"] = "DOWN 📉"
                        else:
                            S["direction"] = "FLAT"

                # Profit lock
                for p in list(S["positions"]):
                    if self._match(p["symbol"], sym) and p["profit"] >= S["target_profit"]:
                        log(f"🎯 +${p['profit']:.2f} target → close")
                        await self.close(p["id"])

                mine = [p for p in S["positions"] if self._match(p["symbol"], sym)]
                if len(mine) < S["max_trades"] and direction in ("BUY", "SELL"):
                    log(f"⚡ {direction} setup → open")
                    await self.open_trade(direction, sym, S["lot"], S["sl_points"])

                await self.refresh()
            except Exception as e:
                log(f"Loop: {e}")
            await asyncio.sleep(0.45)

        log("Scalper stopped")

engine = Engine()

# ================= PWA + UI =================
HTML = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no, viewport-fit=cover">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<meta name="mobile-web-app-capable" content="yes">
<meta name="theme-color" content="#0a0e17">
<link rel="manifest" href="/manifest.json">
<title>MT Scalper</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
<style>
:root { --bg:#0a0e17; --card:#12181f; --line:#243044; --g:#3fb950; --r:#f85149; --b:#58a6ff; }
*{box-sizing:border-box;-webkit-tap-highlight-color:transparent}
body{margin:0;background:var(--bg);color:#c9d1d9;font-family:system-ui,-apple-system,sans-serif;padding-bottom:88px}
.wrap{max-width:480px;margin:0 auto;padding:12px}
.top{display:flex;justify-content:space-between;align-items:center;padding:10px 4px 14px}
.brand{font-weight:800;font-size:18px;color:#fff}
.pill{font-size:11px;font-weight:700;padding:6px 10px;border-radius:20px}
.on{background:rgba(63,185,80,.15);color:var(--g);border:1px solid #238636}
.off{background:rgba(248,81,73,.12);color:var(--r);border:1px solid #da3633}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:12px}
.m{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:12px;text-align:center}
.m small{display:block;color:#8b949e;font-size:10px;text-transform:uppercase;letter-spacing:.4px}
.m b{display:block;font-size:18px;margin-top:4px;font-variant-numeric:tabular-nums}
.tabs{display:flex;gap:6px;margin-bottom:12px;overflow-x:auto}
.tab{flex:1;border:1px solid var(--line);background:transparent;color:#8b949e;border-radius:12px;padding:10px;font-weight:700;font-size:13px}
.tab.active{background:#238636;border-color:#238636;color:#fff}
.card{background:var(--card);border:1px solid var(--line);border-radius:16px;padding:14px;margin-bottom:12px}
label{font-size:12px;color:#8b949e;margin-bottom:4px;display:block}
.form-control,.form-select{background:#0d1117!important;border:1px solid var(--line)!important;color:#e6edf3!important;border-radius:10px!important}
.btn-go{background:#238636;border:none;color:#fff;font-weight:800;border-radius:12px;padding:12px;width:100%}
.btn-stop{background:#da3633;border:none;color:#fff;font-weight:800;border-radius:12px;padding:12px;width:100%}
.btn-soft{background:#21262d;border:1px solid var(--line);color:#e6edf3;border-radius:12px;padding:10px;width:100%;font-weight:700}
.log{background:#010409;border:1px solid var(--line);height:180px;overflow:auto;border-radius:12px;padding:10px;font-family:ui-monospace,monospace;font-size:11px;color:var(--g)}
.table{font-size:11px;margin:0}
.table td,.table th{border-color:var(--line)!important;vertical-align:middle}
.hint{font-size:11px;color:#8b949e;line-height:1.4}
/* Floating mini overlay */
#fab{
  position:fixed; right:14px; bottom:18px; z-index:9999;
  width:58px;height:58px;border-radius:50%;
  background:linear-gradient(145deg,#238636,#2ea043);
  box-shadow:0 8px 24px rgba(0,0,0,.45);
  display:flex;align-items:center;justify-content:center;
  color:#fff;font-size:22px;font-weight:900;border:2px solid #3fb950;
  touch-action:none; user-select:none;
}
#fab.pulse{animation:pulse 1.6s infinite}
@keyframes pulse{0%{box-shadow:0 0 0 0 rgba(63,185,80,.5)}70%{box-shadow:0 0 0 14px rgba(63,185,80,0)}100%{box-shadow:0 0 0 0 rgba(63,185,80,0)}}
#mini{
  display:none; position:fixed; left:10px; right:10px; bottom:14px; z-index:9998;
  background:rgba(18,24,31,.96); border:1px solid var(--line); border-radius:18px;
  padding:10px 12px; backdrop-filter:blur(10px);
  box-shadow:0 10px 30px rgba(0,0,0,.5);
}
#mini.show{display:block}
.mini-row{display:flex;align-items:center;justify-content:space-between;gap:8px}
.mini-row b{color:#fff;font-size:13px}
.install-banner{
  display:none; background:#1f6feb22; border:1px solid #1f6feb55; color:#79b8ff;
  border-radius:12px; padding:10px 12px; margin-bottom:12px; font-size:12px;
}
</style>
</head>
<body>
<div class="wrap">
  <div class="top">
    <div class="brand">⚡ MT Scalper</div>
    <div id="st" class="pill off">● Offline</div>
  </div>

  <div id="installBanner" class="install-banner">
    <b>Install as App:</b> Browser menu → <b>Add to Home Screen</b>. Then it opens like a real app and keeps a quick icon.
  </div>

  <div class="grid">
    <div class="m"><small>Bid / Ask</small><b id="px" class="text-success">0 / 0</b></div>
    <div class="m"><small>Balance</small><b id="bal" class="text-info">$0.00</b></div>
    <div class="m"><small>Floating P/L</small><b id="pl">$0.00</b></div>
    <div class="m"><small>Direction</small><b id="dir" class="text-warning">WAITING</b></div>
  </div>

  <div class="tabs">
    <button class="tab active" data-t="live">Live</button>
    <button class="tab" data-t="set">Settings</button>
    <button class="tab" data-t="conn">Connect</button>
  </div>

  <!-- LIVE -->
  <div id="p-live">
    <div class="card">
      <div class="d-flex justify-content-between align-items-center mb-2">
        <div>
          <div style="color:#fff;font-weight:800">Auto Trades</div>
          <div class="hint" id="last">Last: —</div>
        </div>
        <div style="display:flex;gap:8px;min-width:160px">
          <button id="btnStart" class="btn-go" style="padding:10px" disabled onclick="startBot()">RUN</button>
          <button id="btnStop" class="btn-stop" style="padding:10px" disabled onclick="stopBot()">STOP</button>
        </div>
      </div>
      <div class="table-responsive">
        <table class="table table-dark table-sm">
          <thead><tr><th>Sym</th><th>Side</th><th>Lot</th><th>P/L</th><th></th></tr></thead>
          <tbody id="pos"><tr><td colspan="5" class="text-center text-muted">No open trades</td></tr></tbody>
        </table>
      </div>
    </div>
    <div class="card">
      <div style="color:#fff;font-weight:700;margin-bottom:8px">Live Log</div>
      <div id="log" class="log"></div>
    </div>
  </div>

  <!-- SETTINGS -->
  <div id="p-set" style="display:none">
    <div class="card">
      <label>Symbol</label>
      <select id="sym" class="form-select mb-3">
        <option value="BTCUSD">BTCUSD</option>
        <option value="ETHUSD">ETHUSD</option>
        <option value="XAUUSD">XAUUSD</option>
        <option value="EURUSD">EURUSD</option>
      </select>
      <label>Strategy</label>
      <select id="strategy" class="form-select mb-3">
        <option value="MOMENTUM">Fast Momentum Scalp</option>
        <option value="EMA">EMA Tick Trend</option>
      </select>
      <div class="row g-2">
        <div class="col-6">
          <label>Target Profit ($)</label>
          <input id="target" type="number" class="form-control" value="0.50" step="0.10">
        </div>
        <div class="col-6">
          <label>Lot Size</label>
          <input id="lot" type="number" class="form-control" value="0.01" step="0.01">
        </div>
        <div class="col-6">
          <label>Max Trades</label>
          <input id="max" type="number" class="form-control" value="1">
        </div>
        <div class="col-6">
          <label>SL Points</label>
          <input id="sl" type="number" class="form-control" value="1000">
        </div>
      </div>
      <p class="hint mt-3 mb-0">Tip: start with lot 0.01 and target $0.30–$0.50 on demo. Increase only after stable results.</p>
    </div>
  </div>

  <!-- CONNECT -->
  <div id="p-conn" style="display:none">
    <div class="card">
      <label>MetaApi Token</label>
      <input id="token" type="password" class="form-control mb-3" placeholder="Token from app.metaapi.cloud">
      <label>Method</label>
      <select id="method" class="form-select mb-3" onchange="toggleMethod()">
        <option value="id">Account ID (best)</option>
        <option value="login">Login + Password + Server</option>
      </select>
      <div id="box-id">
        <label>Account ID</label>
        <input id="accid" class="form-control mb-3" placeholder="UUID from MetaApi Accounts">
      </div>
      <div id="box-login" style="display:none">
        <label>Platform</label>
        <select id="plat" class="form-select mb-2"><option value="mt5">MT5</option><option value="mt4">MT4</option></select>
        <label>Login</label>
        <input id="login" class="form-control mb-2" value="476924559">
        <label>Password</label>
        <input id="pass" type="password" class="form-control mb-2">
        <label>Server</label>
        <input id="server" class="form-control mb-3" value="Exness-MT5Trial9">
      </div>
      <button class="btn-go" onclick="doConnect()">Connect Account</button>
      <p class="hint mt-3 mb-0">If login method fails, create the account once in MetaApi dashboard and use Account ID.</p>
    </div>
  </div>
</div>

<!-- Floating app bubble -->
<div id="fab" title="Open bot">⚡</div>

<!-- Mini overlay bar (when you want quick controls) -->
<div id="mini">
  <div class="mini-row">
    <div>
      <b id="miniDir">WAITING</b>
      <div class="hint" id="miniBal">$0.00</div>
    </div>
    <div style="display:flex;gap:6px">
      <button class="btn-soft" style="width:auto;padding:8px 12px" onclick="startBot()">RUN</button>
      <button class="btn-soft" style="width:auto;padding:8px 12px" onclick="stopBot()">STOP</button>
      <button class="btn-soft" style="width:auto;padding:8px 12px" onclick="hideMini()">Open</button>
    </div>
  </div>
</div>

<script>
// Tabs
document.querySelectorAll('.tab').forEach(btn=>{
  btn.onclick=()=>{
    document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
    btn.classList.add('active');
    const t=btn.dataset.t;
    document.getElementById('p-live').style.display=t==='live'?'block':'none';
    document.getElementById('p-set').style.display=t==='set'?'block':'none';
    document.getElementById('p-conn').style.display=t==='conn'?'block':'none';
  };
});
function toggleMethod(){
  const m=document.getElementById('method').value;
  document.getElementById('box-id').style.display=m==='id'?'block':'none';
  document.getElementById('box-login').style.display=m==='login'?'block':'none';
}

// Floating bubble drag + tap
(function(){
  const fab=document.getElementById('fab');
  let ox=0,oy=0,sx=0,sy=0,moved=false;
  fab.addEventListener('touchstart',e=>{
    const t=e.touches[0]; moved=false;
    sx=t.clientX; sy=t.clientY;
    const r=fab.getBoundingClientRect();
    ox=t.clientX-r.left; oy=t.clientY-r.top;
  },{passive:true});
  fab.addEventListener('touchmove',e=>{
    const t=e.touches[0];
    if(Math.abs(t.clientX-sx)>8||Math.abs(t.clientY-sy)>8) moved=true;
    fab.style.left=(t.clientX-ox)+'px';
    fab.style.top=(t.clientY-oy)+'px';
    fab.style.right='auto'; fab.style.bottom='auto';
  },{passive:true});
  fab.addEventListener('touchend',()=>{
    if(!moved){
      // tap = show mini or scroll top
      document.getElementById('mini').classList.add('show');
      window.scrollTo({top:0,behavior:'smooth'});
    }
  });
  fab.addEventListener('click',()=>{
    document.getElementById('mini').classList.add('show');
    window.scrollTo({top:0,behavior:'smooth'});
  });
})();
function hideMini(){ document.getElementById('mini').classList.remove('show'); window.scrollTo({top:0,behavior:'smooth'}); }

// PWA install hint
if('serviceWorker' in navigator){
  navigator.serviceWorker.register('/sw.js').catch(()=>{});
}
setTimeout(()=>{ 
  const isStandalone = window.matchMedia('(display-mode: standalone)').matches || window.navigator.standalone;
  if(!isStandalone) document.getElementById('installBanner').style.display='block';
},800);

async function refresh(){
  try{
    const r=await fetch('/api/status'); const d=await r.json();
    const st=document.getElementById('st');
    st.textContent='● '+d.status;
    st.className='pill '+(d.connected?'on':'off');
    document.getElementById('px').textContent=d.tick.bid.toFixed(2)+' / '+d.tick.ask.toFixed(2);
    document.getElementById('bal').textContent='$'+d.balance.toFixed(2);
    document.getElementById('miniBal').textContent='$'+d.balance.toFixed(2)+' | P/L '+(d.profit>=0?'+':'')+d.profit.toFixed(2);
    const pl=document.getElementById('pl');
    pl.textContent=(d.profit>=0?'+':'')+'$'+Math.abs(d.profit).toFixed(2);
    pl.style.color=d.profit>=0?'#3fb950':'#f85149';
    document.getElementById('dir').textContent=d.direction;
    document.getElementById('miniDir').textContent=d.direction;
    document.getElementById('last').textContent='Last: '+(d.last_action||'—');
    document.getElementById('log').innerHTML=(d.logs||[]).join('<br>');
    document.getElementById('log').scrollTop=99999;
    document.getElementById('fab').className=d.running?'pulse':'';

    let h='';
    if(d.positions && d.positions.length){
      d.positions.forEach(p=>{
        h+=`<tr>
          <td>${p.symbol}</td>
          <td><span class="badge ${p.type==='BUY'?'bg-success':'bg-danger'}">${p.type}</span></td>
          <td>${p.volume}</td>
          <td style="color:${p.profit>=0?'#3fb950':'#f85149'}">$${p.profit.toFixed(2)}</td>
          <td><button class="btn btn-danger btn-sm py-0" onclick="closePos('${p.id}')">X</button></td>
        </tr>`;
      });
    } else h='<tr><td colspan="5" class="text-center text-muted">No open trades</td></tr>';
    document.getElementById('pos').innerHTML=h;
    document.getElementById('btnStart').disabled=!d.connected||d.running;
    document.getElementById('btnStop').disabled=!d.running;
  }catch(e){}
}
async function doConnect(){
  const body={
    token:document.getElementById('token').value.trim(),
    method:document.getElementById('method').value,
    account_id:document.getElementById('accid').value.trim(),
    login:document.getElementById('login').value.trim(),
    password:document.getElementById('pass').value,
    server:document.getElementById('server').value.trim(),
    platform:document.getElementById('plat').value
  };
  const r=await fetch('/api/connect',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
  const d=await r.json();
  alert(d.success?'✅ Connected':'❌ '+(d.message||'Failed'));
  if(d.success) document.querySelector('[data-t="live"]').click();
}
async function startBot(){
  await fetch('/api/start',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({
    symbol:document.getElementById('sym').value,
    strategy:document.getElementById('strategy').value,
    lot:parseFloat(document.getElementById('lot').value),
    max_trades:parseInt(document.getElementById('max').value),
    target_profit:parseFloat(document.getElementById('target').value),
    sl_points:parseInt(document.getElementById('sl').value)
  })});
  document.getElementById('mini').classList.remove('show');
}
async function stopBot(){ await fetch('/api/stop',{method:'POST'}); }
async function closePos(id){
  if(confirm('Close trade?')) await fetch('/api/close',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id})});
}
setInterval(refresh,900);
refresh();
</script>
</body>
</html>
"""

@app.route("/")
def home():
    return render_template_string(HTML)

@app.route("/manifest.json")
def manifest():
    data = {
        "name": "MT Scalper Bot",
        "short_name": "MT Scalper",
        "start_url": "/",
        "display": "standalone",
        "background_color": "#0a0e17",
        "theme_color": "#0a0e17",
        "orientation": "portrait",
        "icons": [
            {"src": "https://cdn.jsdelivr.net/gh/twitter/twemoji@14.0.2/assets/72x72/26a1.png", "sizes": "72x72", "type": "image/png"},
            {"src": "https://cdn.jsdelivr.net/gh/twitter/twemoji@14.0.2/assets/72x72/26a1.png", "sizes": "192x192", "type": "image/png"},
            {"src": "https://cdn.jsdelivr.net/gh/twitter/twemoji@14.0.2/assets/72x72/26a1.png", "sizes": "512x512", "type": "image/png"}
        ]
    }
    return jsonify(data)

@app.route("/sw.js")
def sw():
    js = """
self.addEventListener('install', e => self.skipWaiting());
self.addEventListener('activate', e => e.waitUntil(self.clients.claim()));
self.addEventListener('fetch', e => {});
"""
    return Response(js, mimetype="application/javascript")

@app.route("/api/status")
def status():
    return jsonify(S)

@app.route("/api/connect", methods=["POST"])
def api_connect():
    data = request.json or {}
    token = (data.get("token") or "").strip()
    if not token:
        return jsonify(success=False, message="Token required")
    S["token"] = token
    S["status"] = "Connecting..."
    S["error"] = ""

    async def job():
        if data.get("method") == "id":
            return await engine.connect_id(token, data.get("account_id", ""))
        return await engine.connect_login(
            token, data.get("login"), data.get("password"),
            data.get("server"), data.get("platform", "mt5")
        )

    fut = asyncio.run_coroutine_threadsafe(job(), bg_loop)
    try:
        ok, msg = fut.result(timeout=120)
        return jsonify(success=ok, message=msg)
    except Exception as e:
        S["status"] = "Failed"
        S["error"] = str(e)
        return jsonify(success=False, message=str(e))

@app.route("/api/start", methods=["POST"])
def api_start():
    if not S["connected"]:
        return jsonify(ok=False, message="Not connected")
    data = request.json or {}
    S["symbol"] = (data.get("symbol") or "BTCUSD").upper()
    S["strategy"] = data.get("strategy") or "MOMENTUM"
    S["lot"] = float(data.get("lot") or 0.01)
    S["max_trades"] = int(data.get("max_trades") or 1)
    S["target_profit"] = float(data.get("target_profit") or 0.50)
    S["sl_points"] = int(data.get("sl_points") or 1000)
    S["running"] = True
    asyncio.run_coroutine_threadsafe(engine.run(), bg_loop)
    return jsonify(ok=True)

@app.route("/api/stop", methods=["POST"])
def api_stop():
    S["running"] = False
    log("Stop requested")
    return jsonify(ok=True)

@app.route("/api/close", methods=["POST"])
def api_close():
    pid = (request.json or {}).get("id")
    if pid and S["connected"]:
        asyncio.run_coroutine_threadsafe(engine.close(pid), bg_loop)
    return jsonify(ok=True)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
