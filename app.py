import os
import re
import asyncio
import threading
import logging
from datetime import datetime, timedelta, timezone

from flask import Flask, render_template_string, request, jsonify, Response
from metaapi_cloud_sdk import MetaApi
import pandas as pd

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
    "mode": "SCALP",
    "strategy": "CONFLUENCE",
    "lot": 0.01,
    "max_trades": 1,
    "target_profit": 0.40,
    "swing_target": 5.0,
    "min_score": 55,
    "sl_points": 1200,
    "positions": [],
    "tick": {"bid": 0.0, "ask": 0.0},
    "direction": "WAITING",
    "score": 0,
    "last_action": "—",
    "logs": ["Ready. Connect → set SCALP → RUN."]
}

def log(msg):
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    S["logs"].append(line)
    if len(S["logs"]) > 120:
        S["logs"].pop(0)
    print(line)

def ema(s, n):
    return s.ewm(span=n, adjust=False).mean()

def rsi(s, n=14):
    d = s.diff()
    g = d.clip(lower=0).rolling(n).mean()
    l = (-d.clip(upper=0)).rolling(n).mean()
    rs = g / (l + 1e-9)
    return 100 - (100 / (1 + rs))

class Engine:
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

    async def list_accounts(self):
        api = self.api.metatrader_account_api
        try:
            return await api.get_accounts_with_infinite_scroll_pagination()
        except Exception:
            try:
                return await api.get_accounts()
            except Exception:
                return []

    async def connect_id(self, token, acc_id):
        self.api = MetaApi(token)
        self.account = await self.api.metatrader_account_api.get_account(acc_id.strip())
        return await self._ready()

    async def connect_login(self, token, login, password, server, platform):
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
            log(f"Existing account {found.id}")
        else:
            log("Creating cloud account...")
            self.account = await self.api.metatrader_account_api.create_account({
                "name": f"MK-{login}",
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
            log("Deploying...")
            await self.account.deploy()
        await self.account.wait_connected()
        self.conn = self.account.get_rpc_connection()
        await self.conn.connect()
        await self.conn.wait_synchronized()
        await self.refresh()
        S["connected"] = True
        S["status"] = f"Online • {S['account_type']}"
        S["error"] = ""
        log("✅ Connected")
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
                side = "BUY" if "BUY" in str(p.get("type", "")) else "SELL"
                out.append({
                    "id": str(p.get("id")),
                    "symbol": p.get("symbol"),
                    "type": side,
                    "volume": float(p.get("volume", 0.01)),
                    "open": float(p.get("openPrice", 0)),
                    "current": float(p.get("currentPrice", 0)),
                    "profit": float(p.get("profit", 0)),
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
                    log(f"Symbol {symbol} → {s}")
                    return s
            return symbol
        except Exception:
            return symbol

    async def spec(self, symbol):
        if symbol not in self.specs:
            try:
                sp = await self.conn.get_symbol_specification(symbol)
                self.specs[symbol] = {
                    "digits": int(sp.get("digits", 2)),
                    "point": float(sp.get("point", 0.01)),
                }
            except Exception:
                self.specs[symbol] = {"digits": 2, "point": 0.01}
        return self.specs[symbol]

    def same_symbol(self, a, b):
        x = a.replace("T", "").replace(".", "").replace("_", "")
        y = b.replace("T", "").replace(".", "").replace("_", "")
        return x in y or y in x

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

    async def open_trade(self, side, symbol, lot, sl_points):
        try:
            sp = await self.spec(symbol)
            digits, point = sp["digits"], sp["point"]
            px = await self.conn.get_symbol_price(symbol)
            entry = float(px["ask"] if side == "BUY" else px["bid"])
            scale = 1.0 if ("BTC" in symbol or "ETH" in symbol) else point
            sl = entry - sl_points * scale if side == "BUY" else entry + sl_points * scale
            mult = 4 if S["mode"] == "SWING" else 2
            tp = entry + sl_points * mult * scale if side == "BUY" else entry - sl_points * mult * scale
            log(f"🚀 OPEN {side} {symbol} lot={lot} @ {entry:.2f}")
            S["last_action"] = f"OPEN {side} {symbol}"
            if side == "BUY":
                await self.conn.create_market_buy_order(symbol, lot, round(sl, digits), round(tp, digits))
            else:
                await self.conn.create_market_sell_order(symbol, lot, round(sl, digits), round(tp, digits))
            log("✅ Order sent to MT5")
            await self.refresh()
        except Exception as e:
            log(f"❌ Open failed: {e}")

    async def candles(self, symbol, tf="1m", count=80):
        # best effort only; scalp can work without perfect candles
        try:
            start = datetime.now(timezone.utc) - timedelta(days=2)
            if hasattr(self.api, "historical_market_data_client"):
                rows = await self.api.historical_market_data_client.get_historical_candles(
                    self.account.server, symbol, tf, start, count
                )
                if rows:
                    df = pd.DataFrame(rows)
                    for c in ("open", "high", "low", "close"):
                        if c in df.columns:
                            df[c] = df[c].astype(float)
                    return df
        except Exception:
            pass
        if len(self.prices) >= 20:
            arr = self.prices[-80:]
            return pd.DataFrame({"close": arr, "high": arr, "low": arr})
        return None

    def signal(self, df):
        """
        SCALP: tick momentum first (so trades actually open)
        SWING/CONFLUENCE: add EMA/RSI confirmation
        """
        buy = 0
        sell = 0
        reason = []

        # --- tick momentum (main trigger for scalp) ---
        mom = 0.0
        if len(self.prices) >= 6:
            mom = self.prices[-1] - self.prices[-4]
            wave = self.prices[-1] - self.prices[-6]
        else:
            wave = 0.0

        # thresholds
        sym = S["real_symbol"]
        if "BTC" in sym:
            th = 1.0
        elif "ETH" in sym:
            th = 0.4
        elif "XAU" in sym:
            th = 0.05
        else:
            th = 0.00008

        if mom > th and wave > 0:
            buy += 45
            reason.append("tickUP")
        if mom < -th and wave < 0:
            sell += 45
            reason.append("tickDOWN")

        # consecutive ticks
        if len(self.prices) >= 4:
            a, b, c, d = self.prices[-4], self.prices[-3], self.prices[-2], self.prices[-1]
            if d > c >= b:
                buy += 20
                reason.append("stairsUP")
            if d < c <= b:
                sell += 20
                reason.append("stairsDOWN")

        # optional candle confluence
        if df is not None and len(df) >= 25 and S["strategy"] in ("CONFLUENCE", "EMA", "MOMENTUM"):
            try:
                e9 = float(ema(df["close"], 9).iloc[-1])
                e21 = float(ema(df["close"], 21).iloc[-1])
                r = float(rsi(df["close"], 14).iloc[-1])
                if e9 > e21:
                    buy += 15
                    reason.append("emaUP")
                if e9 < e21:
                    sell += 15
                    reason.append("emaDOWN")
                if r >= 52:
                    buy += 10
                if r <= 48:
                    sell += 10
            except Exception:
                pass

        buy = max(0, min(100, buy))
        sell = max(0, min(100, sell))
        need = int(S["min_score"])
        if S["mode"] == "SCALP":
            need = min(need, 55)  # scalp more active

        if buy >= need and buy >= sell and buy > 0:
            return "BUY", buy, "UP", ",".join(reason) or "buy"
        if sell >= need and sell > buy and sell > 0:
            return "SELL", sell, "DOWN", ",".join(reason) or "sell"
        return "WAITING", max(buy, sell), "FLAT", ",".join(reason) or "no-edge"

    async def manage(self, symbol, side_now):
        target = S["target_profit"] if S["mode"] == "SCALP" else S["swing_target"]
        for p in list(S["positions"]):
            if not self.same_symbol(p["symbol"], symbol):
                continue
            pr = p["profit"]
            if pr >= target:
                log(f"🎯 Target ${pr:.2f} >= ${target:.2f} → CLOSE")
                await self.close(p["id"])
                continue
            # reverse-in-profit protection
            if pr > 0 and ((p["type"] == "BUY" and side_now == "SELL") or (p["type"] == "SELL" and side_now == "BUY")):
                log(f"🔄 Reverse while green ${pr:.2f} → CLOSE")
                await self.close(p["id"])

    async def run(self):
        log(f"🔥 RUN {S['mode']} | strategy={S['strategy']} | min_score={S['min_score']}% | target=${S['target_profit']}")
        S["real_symbol"] = await self.resolve(S["symbol"])
        symbol = S["real_symbol"]
        log(f"Trading symbol: {symbol}")

        while S["running"] and S["connected"]:
            try:
                px = await self.conn.get_symbol_price(symbol)
                bid, ask = float(px["bid"]), float(px["ask"])
                mid = (bid + ask) / 2.0
                S["tick"] = {"bid": bid, "ask": ask}
                self.prices.append(mid)
                if len(self.prices) > 100:
                    self.prices.pop(0)

                df = await self.candles(symbol, "1m" if S["mode"] == "SCALP" else "5m")
                side, score, label, reason = self.signal(df)
                S["score"] = score
                S["direction"] = label

                await self.refresh()
                mine = [p for p in S["positions"] if self.same_symbol(p["symbol"], symbol)]

                log(f"📊 {symbol} {bid:.2f}/{ask:.2f} score={score}% dir={label} reason={reason} openPos={len(mine)}")

                await self.manage(symbol, side)

                if len(mine) >= S["max_trades"]:
                    log("⏳ Max trades reached — waiting to close/bank")
                elif side in ("BUY", "SELL"):
                    log(f"✅ ENTRY SIGNAL {side} ({score}%) → opening now")
                    await self.open_trade(side, symbol, S["lot"], S["sl_points"])
                else:
                    log(f"… waiting | need>={S['min_score']}% | best={score}%")

            except Exception as e:
                log(f"Loop error: {e}")

            await asyncio.sleep(0.8)

        log("Stopped")

engine = Engine()

HTML = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="theme-color" content="#070b12">
<link rel="manifest" href="/manifest.json">
<title>MK Scalper</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
<style>
body{margin:0;background:#070b12;color:#c9d1d9;font-family:system-ui,sans-serif;padding-bottom:90px}
.wrap{max-width:480px;margin:0 auto;padding:12px}
.brand{font-weight:900;color:#fff}
.pill{font-size:11px;font-weight:800;padding:6px 10px;border-radius:20px;border:1px solid #444}
.on{color:#3fb950;border-color:#238636}
.off{color:#f85149;border-color:#da3633}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin:10px 0}
.m{background:#101826;border:1px solid #243247;border-radius:14px;padding:10px;text-align:center}
.m small{color:#8b949e;font-size:10px}.m b{display:block;margin-top:4px;font-size:16px}
.tabs{display:flex;gap:6px;margin-bottom:10px}
.tab{flex:1;border:1px solid #243247;background:transparent;color:#8b949e;border-radius:12px;padding:10px;font-weight:800}
.tab.active{background:#238636;border-color:#238636;color:#fff}
.card{background:#101826;border:1px solid #243247;border-radius:16px;padding:14px;margin-bottom:10px}
label{font-size:11px;color:#8b949e}
.form-control,.form-select{background:#0b1220!important;border-color:#243247!important;color:#e6edf3!important}
.btn-go{background:#238636;border:0;color:#fff;font-weight:900;border-radius:12px;padding:12px;width:100%}
.btn-stop{background:#da3633;border:0;color:#fff;font-weight:900;border-radius:12px;padding:12px;width:100%}
.log{height:190px;overflow:auto;background:#05080f;border:1px solid #243247;border-radius:12px;padding:10px;font:11px monospace;color:#3fb950}
#fab{position:fixed;right:14px;bottom:18px;width:56px;height:56px;border-radius:50%;background:#238636;color:#fff;display:flex;align-items:center;justify-content:center;font-weight:900;font-size:22px;z-index:99}
</style>
</head>
<body>
<div class="wrap">
  <div class="d-flex justify-content-between align-items-center">
    <div class="brand">⚡ MK Scalper</div>
    <div id="st" class="pill off">● Offline</div>
  </div>
  <div class="grid">
    <div class="m"><small>Bid/Ask</small><b id="px">0/0</b></div>
    <div class="m"><small>Balance</small><b id="bal">$0</b></div>
    <div class="m"><small>Floating</small><b id="pl">$0</b></div>
    <div class="m"><small>Score/Dir</small><b id="dir">0% WAIT</b></div>
  </div>
  <div class="tabs">
    <button class="tab active" data-t="live">Live</button>
    <button class="tab" data-t="set">Settings</button>
    <button class="tab" data-t="conn">Connect</button>
  </div>

  <div id="panel-live">
    <div class="card">
      <div class="d-flex justify-content-between align-items-center mb-2">
        <div><b style="color:#fff">Trades</b><div style="font-size:11px;color:#8b949e" id="last">Last: —</div></div>
        <div style="display:flex;gap:8px;width:170px">
          <button id="btnStart" class="btn-go" style="padding:10px" disabled>RUN</button>
          <button id="btnStop" class="btn-stop" style="padding:10px" disabled>STOP</button>
        </div>
      </div>
      <table class="table table-dark table-sm mb-0" style="font-size:11px">
        <thead><tr><th>Sym</th><th>Side</th><th>Lot</th><th>P/L</th><th></th></tr></thead>
        <tbody id="pos"><tr><td colspan="5" class="text-center text-muted">No trades</td></tr></tbody>
      </table>
    </div>
    <div class="card"><div style="color:#fff;font-weight:800;margin-bottom:6px">Live Log</div><div id="log" class="log"></div></div>
  </div>

  <div id="panel-set" style="display:none">
    <div class="card">
      <label>Mode</label>
      <select id="mode" class="form-select mb-2">
        <option value="SCALP" selected>SCALP (fast open/close)</option>
        <option value="SWING">SWING (hold longer)</option>
      </select>
      <label>Symbol</label>
      <select id="sym" class="form-select mb-2"><option>BTCUSD</option><option>ETHUSD</option><option>XAUUSD</option><option>EURUSD</option></select>
      <div class="row g-2">
        <div class="col-6"><label>Target profit $</label><input id="target" class="form-control" type="number" value="0.40" step="0.1"></div>
        <div class="col-6"><label>Min score %</label><input id="score" class="form-control" type="number" value="55"></div>
        <div class="col-6"><label>Lot</label><input id="lot" class="form-control" type="number" value="0.01" step="0.01"></div>
        <div class="col-6"><label>Max trades</label><input id="max" class="form-control" type="number" value="1"></div>
      </div>
    </div>
  </div>

  <div id="panel-conn" style="display:none">
    <div class="card">
      <label>MetaApi Token</label>
      <input id="token" type="password" class="form-control mb-2">
      <label>Method</label>
      <select id="method" class="form-select mb-2">
        <option value="id">Account ID</option>
        <option value="login">Login + Password + Server</option>
      </select>
      <div id="box-id"><label>Account ID</label><input id="accid" class="form-control mb-2"></div>
      <div id="box-login" style="display:none">
        <select id="plat" class="form-select mb-2"><option value="mt5">MT5</option><option value="mt4">MT4</option></select>
        <input id="login" class="form-control mb-2" value="476924559">
        <input id="pass" type="password" class="form-control mb-2">
        <input id="server" class="form-control mb-2" value="Exness-MT5Trial9">
      </div>
      <button class="btn-go" id="btnConnect">Connect</button>
    </div>
  </div>
</div>
<div id="fab">⚡</div>
<script>
const $=id=>document.getElementById(id);
document.querySelectorAll('.tab').forEach(b=>b.onclick=()=>{
  document.querySelectorAll('.tab').forEach(x=>x.classList.remove('active')); b.classList.add('active');
  const t=b.dataset.t;
  $('panel-live').style.display=t==='live'?'block':'none';
  $('panel-set').style.display=t==='set'?'block':'none';
  $('panel-conn').style.display=t==='conn'?'block':'none';
});
$('method').onchange=()=>{
  $('box-id').style.display=$('method').value==='id'?'block':'none';
  $('box-login').style.display=$('method').value==='login'?'block':'none';
};
async function refresh(){
  const d=await(await fetch('/api/status')).json();
  $('st').textContent='● '+d.status; $('st').className='pill '+(d.connected?'on':'off');
  $('px').textContent=d.tick.bid.toFixed(2)+' / '+d.tick.ask.toFixed(2);
  $('bal').textContent='$'+d.balance.toFixed(2);
  $('pl').textContent=(d.profit>=0?'+':'')+d.profit.toFixed(2);
  $('dir').textContent=(d.score||0)+'% '+d.direction;
  $('last').textContent='Last: '+(d.last_action||'—');
  $('log').innerHTML=(d.logs||[]).join('<br>'); $('log').scrollTop=1e9;
  let h='';
  (d.positions||[]).forEach(p=>{
    h+=`<tr><td>${p.symbol}</td><td>${p.type}</td><td>${p.volume}</td>
    <td style="color:${p.profit>=0?'#3fb950':'#f85149'}">$${p.profit.toFixed(2)}</td>
    <td><button class="btn btn-danger btn-sm py-0" onclick="closePos('${p.id}')">X</button></td></tr>`;
  });
  $('pos').innerHTML=h||'<tr><td colspan="5" class="text-center text-muted">No trades</td></tr>';
  $('btnStart').disabled=!d.connected||d.running;
  $('btnStop').disabled=!d.running;
}
async function doConnect(){
  const body={token:$('token').value.trim(),method:$('method').value,account_id:$('accid').value.trim(),login:$('login').value.trim(),password:$('pass').value,server:$('server').value.trim(),platform:$('plat').value};
  const d=await(await fetch('/api/connect',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)})).json();
  alert(d.success?'✅ Connected':('❌ '+d.message));
}
async function startBot(){
  await fetch('/api/start',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({
    mode:$('mode').value, symbol:$('sym').value,
    target_profit:parseFloat($('target').value), min_score:parseInt($('score').value),
    lot:parseFloat($('lot').value), max_trades:parseInt($('max').value)
  })});
}
async function stopBot(){await fetch('/api/stop',{method:'POST'});}
async function closePos(id){if(confirm('Close?'))await fetch('/api/close',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id})});}
$('btnConnect').onclick=doConnect; $('btnStart').onclick=startBot; $('btnStop').onclick=stopBot;
$('fab').onclick=()=>window.scrollTo({top:0,behavior:'smooth'});
setInterval(refresh,1000); refresh();
</script>
</body>
</html>
"""

@app.route("/")
def home():
    return render_template_string(HTML)

@app.route("/manifest.json")
def manifest():
    return jsonify({"name":"MK Scalper","short_name":"MK","start_url":"/","display":"standalone","background_color":"#070b12","theme_color":"#070b12",
                    "icons":[{"src":"https://cdn.jsdelivr.net/gh/twitter/twemoji@14.0.2/assets/72x72/26a1.png","sizes":"192x192","type":"image/png"}]})

@app.route("/sw.js")
def sw():
    return Response("self.addEventListener('install',e=>self.skipWaiting());", mimetype="application/javascript")

@app.route("/api/status")
def api_status():
    return jsonify(S)

@app.route("/api/connect", methods=["POST"])
def api_connect():
    data = request.json or {}
    token = (data.get("token") or "").strip()
    if not token:
        return jsonify(success=False, message="Token required")
    S["status"] = "Connecting..."
    try:
        if data.get("method") == "id":
            ok, msg = run_async(engine.connect_id(token, data.get("account_id", "")))
        else:
            ok, msg = run_async(engine.connect_login(token, data.get("login"), data.get("password"), data.get("server"), data.get("platform", "mt5")))
        return jsonify(success=ok, message=msg)
    except Exception as e:
        S["status"] = "Failed"
        log(f"❌ {e}")
        return jsonify(success=False, message=str(e))

@app.route("/api/start", methods=["POST"])
def api_start():
    if not S["connected"]:
        return jsonify(ok=False, message="Connect first")
    d = request.json or {}
    S["mode"] = d.get("mode", "SCALP")
    S["symbol"] = (d.get("symbol") or "BTCUSD").upper()
    S["target_profit"] = float(d.get("target_profit", 0.40))
    S["min_score"] = int(d.get("min_score", 55))
    S["lot"] = float(d.get("lot", 0.01))
    S["max_trades"] = int(d.get("max_trades", 1))
    S["running"] = True
    asyncio.run_coroutine_threadsafe(engine.run(), _loop)
    return jsonify(ok=True)

@app.route("/api/stop", methods=["POST"])
def api_stop():
    S["running"] = False
    log("STOP")
    return jsonify(ok=True)

@app.route("/api/close", methods=["POST"])
def api_close():
    pid = (request.json or {}).get("id")
    if pid and S["connected"]:
        asyncio.run_coroutine_threadsafe(engine.close(pid), _loop)
    return jsonify(ok=True)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
