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
    "symbol": "XAUUSD",
    "real_symbol": "XAUUSD",
    "mode": "SCALP",
    "lot": 0.01,
    "max_trades": 1,
    "target_profit": 1.0,
    "min_score": 55,
    "sl_points": 300,
    "positions": [],
    "tick": {"bid": 0.0, "ask": 0.0},
    "direction": "WAITING",
    "score": 0,
    "last_action": "—",
    "logs": ["Ready. Connect → RUN. Orders now retry + verify."]
}

def log(msg):
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    S["logs"].append(line)
    if len(S["logs"]) > 150:
        S["logs"].pop(0)
    print(line)

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
        except Exception as e:
            log(f"Refresh note: {e}")

    async def resolve(self, symbol):
        try:
            symbols = await self.conn.get_symbols()
            if symbol in symbols:
                return symbol
            c = re.sub(r"[/._]", "", symbol).upper()
            # prefer exact-ish metals/crypto names
            best = None
            for s in symbols:
                sc = re.sub(r"[/._]", "", s).upper()
                if c in sc or sc in c:
                    best = s
                    if s.upper().startswith(symbol.upper()):
                        break
            if best:
                log(f"Symbol {symbol} → {best}")
                return best
            return symbol
        except Exception:
            return symbol

    async def load_spec(self, symbol):
        if symbol in self.specs:
            return self.specs[symbol]
        try:
            sp = await self.conn.get_symbol_specification(symbol)
            spec = {
                "digits": int(sp.get("digits", 2)),
                "point": float(sp.get("point", 0.01)),
                "min_volume": float(sp.get("minVolume", sp.get("volumeMin", 0.01)) or 0.01),
                "max_volume": float(sp.get("maxVolume", sp.get("volumeMax", 100)) or 100),
                "volume_step": float(sp.get("volumeStep", sp.get("volumeStep", 0.01)) or 0.01),
                "stops_level": float(sp.get("stopsLevel", sp.get("tradeStopsLevel", 0)) or 0),
                "filling": sp.get("fillingModes", sp.get("fillingMode")),
            }
            self.specs[symbol] = spec
            log(f"Spec {symbol}: minLot={spec['min_volume']} step={spec['volume_step']} stopsLevel={spec['stops_level']} point={spec['point']}")
            return spec
        except Exception as e:
            log(f"Spec fallback ({e})")
            spec = {"digits": 2, "point": 0.01, "min_volume": 0.01, "max_volume": 100, "volume_step": 0.01, "stops_level": 0, "filling": None}
            self.specs[symbol] = spec
            return spec

    def normalize_lot(self, lot, spec):
        step = spec["volume_step"] if spec["volume_step"] > 0 else 0.01
        mn = spec["min_volume"]
        mx = spec["max_volume"]
        # round down to step
        steps = int(lot / step)
        norm = max(mn, min(mx, steps * step))
        # fix float artifacts
        norm = float(f"{norm:.2f}") if step >= 0.01 else float(norm)
        if norm < mn:
            norm = mn
        return norm

    def same_symbol(self, a, b):
        x = re.sub(r"[T._]", "", a.upper())
        y = re.sub(r"[T._]", "", b.upper())
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
        """Robust order sender with retry + position verify"""
        try:
            spec = await self.load_spec(symbol)
            lot = self.normalize_lot(lot, spec)
            digits = spec["digits"]
            point = spec["point"] if spec["point"] > 0 else 0.01

            px = await self.conn.get_symbol_price(symbol)
            bid, ask = float(px["bid"]), float(px["ask"])
            entry = ask if side == "BUY" else bid

            # For metals/crypto point scaling
            # stops_level is in points
            stops_level = spec["stops_level"]
            min_dist_points = max(sl_points, stops_level + 5, 50)
            dist = min_dist_points * point

            if side == "BUY":
                sl = entry - dist
                tp = entry + dist * 2
            else:
                sl = entry + dist
                tp = entry - dist * 2

            sl = round(sl, digits)
            tp = round(tp, digits)

            log(f"🧾 Sending {side} {symbol} lot={lot} entry~{entry:.{digits}f} SL={sl} TP={tp}")

            result = None
            last_err = None

            # Attempt A: with SL/TP
            try:
                if side == "BUY":
                    result = await self.conn.create_market_buy_order(symbol, lot, sl, tp)
                else:
                    result = await self.conn.create_market_sell_order(symbol, lot, sl, tp)
                log(f"✅ Order response (with SL/TP): {result}")
            except Exception as e:
                last_err = e
                log(f"⚠️ Order with SL/TP rejected: {e}")
                log("↩️ Retrying WITHOUT SL/TP...")
                # Attempt B: market only
                try:
                    if side == "BUY":
                        result = await self.conn.create_market_buy_order(symbol, lot)
                    else:
                        result = await self.conn.create_market_sell_order(symbol, lot)
                    log(f"✅ Order response (market only): {result}")
                except Exception as e2:
                    last_err = e2
                    log(f"❌ Market-only order also failed: {e2}")
                    # Attempt C: options dict style used by some SDK versions
                    try:
                        opts = {"comment": "MK-SCALP", "magic": 260318}
                        if side == "BUY":
                            result = await self.conn.create_market_buy_order(symbol, lot, None, None, opts)
                        else:
                            result = await self.conn.create_market_sell_order(symbol, lot, None, None, opts)
                        log(f"✅ Order response (opts): {result}")
                    except Exception as e3:
                        last_err = e3
                        log(f"❌ All order attempts failed: {e3}")
                        S["last_action"] = f"OPEN FAIL: {e3}"
                        return False

            # Verify position really exists
            await asyncio.sleep(1.0)
            await self.refresh()
            mine = [p for p in S["positions"] if self.same_symbol(p["symbol"], symbol)]
            if mine:
                p = mine[-1]
                log(f"🎉 POSITION OPEN ON MT5: #{p['id']} {p['type']} {p['symbol']} lot={p['volume']} entry={p['open']}")
                S["last_action"] = f"OPENED #{p['id']} {p['type']}"
                return True

            log("❌ Broker returned no open position after order. Check MT5 Expo / trade permissions / symbol contract.")
            if last_err:
                log(f"Last error was: {last_err}")
            S["last_action"] = "OPEN sent but not found"
            return False

        except Exception as e:
            log(f"❌ open_trade crash: {e}")
            S["last_action"] = f"OPEN ERROR: {e}"
            return False

    def signal(self):
        buy = 0
        sell = 0
        reason = []

        if len(self.prices) < 6:
            return "WAITING", 0, "WAIT", "warmup"

        mom = self.prices[-1] - self.prices[-4]
        wave = self.prices[-1] - self.prices[-6]
        a, b, c, d = self.prices[-4], self.prices[-3], self.prices[-2], self.prices[-1]

        sym = S["real_symbol"]
        if "XAU" in sym:
            th = 0.04
        elif "BTC" in sym:
            th = 1.0
        elif "ETH" in sym:
            th = 0.4
        else:
            th = 0.00008

        if mom > th and wave > 0:
            buy += 45
            reason.append("tickUP")
        if mom < -th and wave < 0:
            sell += 45
            reason.append("tickDOWN")

        if d > c >= b:
            buy += 20
            reason.append("stairsUP")
        if d < c <= b:
            sell += 20
            reason.append("stairsDOWN")

        # tiny continuation boost
        if len(self.prices) >= 8:
            if self.prices[-1] > self.prices[-8]:
                buy += 10
            if self.prices[-1] < self.prices[-8]:
                sell += 10

        buy = max(0, min(100, buy))
        sell = max(0, min(100, sell))
        need = int(S["min_score"])

        if buy >= need and buy >= sell:
            return "BUY", buy, "UP", ",".join(reason)
        if sell >= need and sell > buy:
            return "SELL", sell, "DOWN", ",".join(reason)
        return "WAITING", max(buy, sell), "FLAT", ",".join(reason) or "no-edge"

    async def manage(self, symbol, side_now):
        target = float(S["target_profit"])
        for p in list(S["positions"]):
            if not self.same_symbol(p["symbol"], symbol):
                continue
            pr = float(p["profit"])
            if pr >= target:
                log(f"🎯 TP hit ${pr:.2f} >= ${target:.2f} → closing #{p['id']}")
                await self.close(p["id"])
                continue
            if pr > 0 and ((p["type"] == "BUY" and side_now == "SELL") or (p["type"] == "SELL" and side_now == "BUY")):
                log(f"🔄 Reverse-in-profit ${pr:.2f} → closing #{p['id']}")
                await self.close(p["id"])

    async def run(self):
        log(f"🔥 RUN SCALP | min_score={S['min_score']}% | target=${S['target_profit']} | lot={S['lot']}")
        S["real_symbol"] = await self.resolve(S["symbol"])
        symbol = S["real_symbol"]
        log(f"Trading symbol: {symbol}")
        await self.load_spec(symbol)

        # try subscribe (ignore if unsupported)
        try:
            await self.conn.subscribe_to_market_data(symbol)
        except Exception:
            pass

        cooldown_until = 0.0

        while S["running"] and S["connected"]:
            try:
                px = await self.conn.get_symbol_price(symbol)
                bid, ask = float(px["bid"]), float(px["ask"])
                mid = (bid + ask) / 2.0
                S["tick"] = {"bid": bid, "ask": ask}
                self.prices.append(mid)
                if len(self.prices) > 120:
                    self.prices.pop(0)

                side, score, label, reason = self.signal()
                S["score"] = score
                S["direction"] = label

                await self.refresh()
                mine = [p for p in S["positions"] if self.same_symbol(p["symbol"], symbol)]

                log(f"📊 {symbol} {bid:.2f}/{ask:.2f} score={score}% dir={label} reason={reason} openPos={len(mine)}")

                await self.manage(symbol, side)

                now = datetime.now(timezone.utc).timestamp()
                if len(mine) >= S["max_trades"]:
                    log("⏳ Position open — managing for profit")
                elif side in ("BUY", "SELL") and now >= cooldown_until:
                    log(f"✅ ENTRY {side} ({score}%) reason={reason}")
                    ok = await self.open_trade(side, symbol, S["lot"], S["sl_points"])
                    cooldown_until = now + (8 if ok else 5)
                else:
                    log(f"… waiting need>={S['min_score']}% best={score}%")

            except Exception as e:
                log(f"Loop error: {e}")

            await asyncio.sleep(0.9)

        log("Stopped")

engine = Engine()

HTML = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1">
<meta name="theme-color" content="#070b12">
<meta name="apple-mobile-web-app-capable" content="yes">
<link rel="manifest" href="/manifest.json">
<title>MK Scalper</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
<style>
body{margin:0;background:#070b12;color:#c9d1d9;font-family:system-ui,sans-serif;padding-bottom:80px}
.wrap{max-width:480px;margin:0 auto;padding:12px}
.brand{font-weight:900;color:#fff}
.pill{font-size:11px;font-weight:800;padding:6px 10px;border-radius:20px;border:1px solid #444}
.on{color:#3fb950;border-color:#238636}.off{color:#f85149;border-color:#da3633}
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
.log{height:210px;overflow:auto;background:#05080f;border:1px solid #243247;border-radius:12px;padding:10px;font:11px monospace;color:#3fb950}
#fab{position:fixed;right:14px;bottom:18px;width:56px;height:56px;border-radius:50%;background:#238636;color:#fff;display:flex;align-items:center;justify-content:center;font-weight:900;font-size:22px}
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
        <div><b style="color:#fff">Positions</b><div style="font-size:11px;color:#8b949e" id="last">Last: —</div></div>
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
      <label>Symbol</label>
      <select id="sym" class="form-select mb-2">
        <option>XAUUSD</option>
        <option>BTCUSD</option>
        <option>ETHUSD</option>
        <option>EURUSD</option>
      </select>
      <div class="row g-2">
        <div class="col-6"><label>Target profit $</label><input id="target" class="form-control" type="number" value="1.0" step="0.1"></div>
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
    symbol:$('sym').value,
    target_profit:parseFloat($('target').value),
    min_score:parseInt($('score').value),
    lot:parseFloat($('lot').value),
    max_trades:parseInt($('max').value)
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
    return jsonify({
        "name": "MK Scalper", "short_name": "MK", "start_url": "/", "display": "standalone",
        "background_color": "#070b12", "theme_color": "#070b12",
        "icons": [{"src": "https://cdn.jsdelivr.net/gh/twitter/twemoji@14.0.2/assets/72x72/26a1.png", "sizes": "192x192", "type": "image/png"}]
    })

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
    S["symbol"] = (d.get("symbol") or "XAUUSD").upper()
    S["target_profit"] = float(d.get("target_profit", 1.0))
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
