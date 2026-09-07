import os
import asyncio
import threading
import logging
import re
from datetime import datetime, timedelta, timezone
from flask import Flask, render_template_string, request, jsonify, Response
from metaapi_cloud_sdk import MetaApi
import pandas as pd

logging.basicConfig(level=logging.INFO)
app = Flask(__name__)

bg_loop = None
def _bg():
    global bg_loop
    bg_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(bg_loop)
    bg_loop.run_forever()
threading.Thread(target=_bg, daemon=True).start()

S = {
    "connected": False, "running": False, "status": "Offline", "error": "",
    "token": "", "account_id": "", "login": "", "server": "", "account_type": "",
    "balance": 0.0, "equity": 0.0, "profit": 0.0,
    "symbol": "BTCUSD", "real_symbol": "BTCUSD",
    "mode": "SCALP",              # SCALP | SWING
    "strategy": "CONFLUENCE",     # CONFLUENCE | MOMENTUM | EMA
    "lot": 0.01, "max_trades": 1,
    "target_profit": 0.50,        # SCALP $ close target
    "swing_target": 5.00,         # SWING larger $ target (optional bank)
    "min_score": 70,              # accuracy threshold 0-100
    "sl_points": 1200,
    "positions": [],
    "tick": {"bid": 0.0, "ask": 0.0},
    "direction": "WAITING",
    "score": 0,
    "last_action": "—",
    "logs": ["MK Dual-Mode Engine ready. Connect → choose SCALP or SWING → RUN."]
}

def log(msg):
    t = datetime.now().strftime("%H:%M:%S")
    S["logs"].append(f"[{t}] {msg}")
    if len(S["logs"]) > 100:
        S["logs"].pop(0)
    logging.info(msg)

def ema(series, n):
    return series.ewm(span=n, adjust=False).mean()

def rsi(series, n=14):
    d = series.diff()
    g = d.clip(lower=0).rolling(n).mean()
    l = (-d.clip(upper=0)).rolling(n).mean()
    rs = g / (l + 1e-9)
    return 100 - (100 / (1 + rs))

def macd_hist(series):
    ef, es = ema(series, 12), ema(series, 26)
    line = ef - es
    sig = ema(line, 9)
    return line - sig

class Engine:
    def __init__(self):
        self.api = None
        self.account = None
        self.conn = None
        self.prices = []
        self.specs = {}

    def fix_server(self, s):
        if not s: return s
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
            log("Connecting via Account ID...")
            self.api = MetaApi(token)
            self.account = await self.api.metatrader_account_api.get_account(acc_id.strip())
            return await self._ready()
        except Exception as e:
            S["error"] = str(e); S["status"] = "Failed"; log(f"❌ {e}")
            return False, str(e)

    async def connect_login(self, token, login, password, server, platform):
        try:
            server = self.fix_server(server)
            log(f"Connecting {server} | {login}")
            self.api = MetaApi(token)
            accounts = await self.list_accounts()
            found = next((a for a in accounts if str(a.login) == str(login) and server.lower() in str(a.server).lower()), None)
            if found:
                self.account = found
                log(f"Existing account {found.id}")
            else:
                log("Creating cloud account...")
                self.account = await self.api.metatrader_account_api.create_account({
                    "name": f"MK-{login}", "type": "cloud",
                    "login": str(login), "password": str(password), "server": server,
                    "platform": "mt5" if "5" in str(platform).lower() else "mt4", "magic": 202603
                })
            return await self._ready()
        except Exception as e:
            S["error"] = str(e); S["status"] = "Failed"; log(f"❌ {e}")
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
        log("✅ Connected — engine armed")
        return True, "OK"

    async def refresh(self):
        if not self.conn: return
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
                    "id": str(p.get("id")), "symbol": p.get("symbol"), "type": t,
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
            if sym in symbols: return sym
            c = re.sub(r"[/._]", "", sym).upper()
            for s in symbols:
                sc = re.sub(r"[/._]", "", s).upper()
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

    async def candles(self, sym, tf="1m", count=80):
        try:
            start = datetime.now(timezone.utc) - timedelta(days=2)
            meta_tf = tf if tf in ("1m", "5m", "15m", "1h") else "1m"
            rows = None
            if hasattr(self.api, "historical_market_data_client"):
                rows = await self.api.historical_market_data_client.get_historical_candles(
                    self.account.server, sym, meta_tf, start, count
                )
            if not rows:
                # tick synthetic fallback
                px = await self.conn.get_symbol_price(sym)
                m = (float(px["bid"]) + float(px["ask"])) / 2
                return pd.DataFrame({"close": [m] * 40, "high": [m * 1.0002] * 40, "low": [m * 0.9998] * 40})
            df = pd.DataFrame(rows)
            for c in ("open", "high", "low", "close"):
                if c in df.columns:
                    df[c] = df[c].astype(float)
            return df
        except Exception:
            return None

    def _match(self, a, b):
        x = a.replace("T", "").replace(".", "")
        y = b.replace("T", "").replace(".", "")
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

    async def open_trade(self, side, sym, lot, sl_pts):
        try:
            sp = await self.spec(sym)
            digits, point = sp["digits"], sp["point"]
            px = await self.conn.get_symbol_price(sym)
            entry = px["ask"] if side == "BUY" else px["bid"]
            scale = 1.0 if ("BTC" in sym or "ETH" in sym) else point
            sl = entry - sl_pts * scale if side == "BUY" else entry + sl_pts * scale
            # wider TP on swing, tighter on scalp (broker TP as backup)
            mult = 4 if S["mode"] == "SWING" else 2
            tp = entry + sl_pts * mult * scale if side == "BUY" else entry - sl_pts * mult * scale
            log(f"🚀 {S['mode']} {side} {sym} lot={lot} @ {float(entry):.2f} score={S['score']}%")
            S["last_action"] = f"{side} {sym} ({S['mode']})"
            if side == "BUY":
                await self.conn.create_market_buy_order(sym, lot, round(sl, digits), round(tp, digits))
            else:
                await self.conn.create_market_sell_order(sym, lot, round(sl, digits), round(tp, digits))
            log("✅ Order sent to MT5/MT4")
            await self.refresh()
        except Exception as e:
            log(f"Open error: {e}")

    def score_signal(self, df, mid):
        """Return (side, score 0-100, label)"""
        if df is None or len(df) < 25:
            return "WAITING", 0, "WAIT"

        df = df.copy()
        df["e9"] = ema(df["close"], 9)
        df["e21"] = ema(df["close"], 21)
        df["rsi"] = rsi(df["close"], 14)
        df["mh"] = macd_hist(df["close"])

        e9, e21 = float(df["e9"].iloc[-1]), float(df["e21"].iloc[-1])
        pe9, pe21 = float(df["e9"].iloc[-2]), float(df["e21"].iloc[-2])
        r = float(df["rsi"].iloc[-1])
        mh = float(df["mh"].iloc[-1])
        pmh = float(df["mh"].iloc[-2])

        # tick momentum
        mom = 0.0
        if len(self.prices) >= 5:
            mom = self.prices[-1] - self.prices[-5]

        th = 1.5 if "BTC" in S["real_symbol"] else (0.08 if "XAU" in S["real_symbol"] else 0.00008)

        buy = 0
        sell = 0

        # EMA structure
        if e9 > e21: buy += 25
        if e9 < e21: sell += 25
        if pe9 <= pe21 and e9 > e21: buy += 15
        if pe9 >= pe21 and e9 < e21: sell += 15

        # RSI
        if 52 <= r <= 68: buy += 20
        if 32 <= r <= 48: sell += 20
        if r > 72: buy -= 15
        if r < 28: sell -= 15

        # MACD hist expand
        if mh > 0 and mh >= pmh: buy += 20
        if mh < 0 and mh <= pmh: sell += 20

        # Momentum kick
        if mom > th: buy += 20
        if mom < -th: sell += 20

        strategy = S["strategy"]
        if strategy == "MOMENTUM":
            buy = (20 if mom > th else 0) + (20 if mh > 0 else 0) + (15 if e9 > e21 else 0)
            sell = (20 if mom < -th else 0) + (20 if mh < 0 else 0) + (15 if e9 < e21 else 0)
        elif strategy == "EMA":
            buy = (40 if e9 > e21 else 0) + (20 if pe9 <= pe21 and e9 > e21 else 0) + (15 if r > 50 else 0)
            sell = (40 if e9 < e21 else 0) + (20 if pe9 >= pe21 and e9 < e21 else 0) + (15 if r < 50 else 0)

        buy = max(0, min(100, buy))
        sell = max(0, min(100, sell))
        need = int(S["min_score"])

        if buy >= need and buy >= sell and buy > 0:
            return "BUY", buy, "UP 🚀"
        if sell >= need and sell > buy and sell > 0:
            return "SELL", sell, "DOWN 📉"
        return "WAITING", max(buy, sell), "FLAT" if max(buy, sell) < need else "WAIT"

    async def manage_positions(self, sym, side_now):
        target = S["target_profit"] if S["mode"] == "SCALP" else S["swing_target"]
        for p in list(S["positions"]):
            if not self._match(p["symbol"], sym):
                continue
            pr = p["profit"]

            # always bank hard target
            if pr >= target:
                log(f"🎯 Target ${pr:.2f} ≥ ${target:.2f} → close")
                await self.close(p["id"])
                continue

            if S["mode"] == "SWING":
                # hold until opposite signal AND already green (protect growth)
                if p["type"] == "BUY" and side_now == "SELL" and pr > 0:
                    log(f"🔄 Direction flip + profit ${pr:.2f} → close BUY")
                    await self.close(p["id"])
                elif p["type"] == "SELL" and side_now == "BUY" and pr > 0:
                    log(f"🔄 Direction flip + profit ${pr:.2f} → close SELL")
                    await self.close(p["id"])
                # optional: lock 50% of swing target if reverse appears
                elif pr >= target * 0.5 and (
                    (p["type"] == "BUY" and side_now == "SELL") or
                    (p["type"] == "SELL" and side_now == "BUY")
                ):
                    log(f"🔒 Lock partial swing profit ${pr:.2f}")
                    await self.close(p["id"])
            else:
                # SCALP: if reverse and small green, bank early
                if pr >= max(0.15, target * 0.4) and (
                    (p["type"] == "BUY" and side_now == "SELL") or
                    (p["type"] == "SELL" and side_now == "BUY")
                ):
                    log(f"⚡ Scalp reverse bank ${pr:.2f}")
                    await self.close(p["id"])

    async def run(self):
        log(f"🔥 RUN — mode={S['mode']} strategy={S['strategy']} score≥{S['min_score']}%")
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
                if len(self.prices) > 30:
                    self.prices.pop(0)

                df = await self.candles(sym, "1m" if S["mode"] == "SCALP" else "5m", 80)
                side, score, label = self.score_signal(df, mid)
                S["score"] = score
                S["direction"] = label if side == "WAITING" else label

                log(f"📊 {sym} {bid:.2f}/{ask:.2f} | score {score}% | {label} | mode {S['mode']}")

                await self.manage_positions(sym, side)

                mine = [p for p in S["positions"] if self._match(p["symbol"], sym)]
                if len(mine) < S["max_trades"] and side in ("BUY", "SELL"):
                    log(f"✅ Accurate entry {side} ({score}% ≥ {S['min_score']}%)")
                    await self.open_trade(side, sym, S["lot"], S["sl_points"])

                await self.refresh()
            except Exception as e:
                log(f"Loop: {e}")
            await asyncio.sleep(0.8 if S["mode"] == "SCALP" else 1.5)

        log("Engine stopped")

engine = Engine()

HTML = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no, viewport-fit=cover">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="theme-color" content="#070b12">
<link rel="manifest" href="/manifest.json">
<title>MK Dual Scalper</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
<style>
:root{--bg:#070b12;--card:#101826;--line:#243247;--g:#3fb950;--r:#f85149}
body{margin:0;background:var(--bg);color:#c9d1d9;font-family:system-ui,sans-serif;padding-bottom:90px}
.wrap{max-width:480px;margin:0 auto;padding:12px}
.top{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px}
.brand{font-weight:900;color:#fff;font-size:18px}
.pill{font-size:11px;font-weight:800;padding:6px 10px;border-radius:20px}
.on{color:var(--g);border:1px solid #238636;background:#23863622}
.off{color:var(--r);border:1px solid #da3633;background:#da363322}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:10px}
.m{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:10px;text-align:center}
.m small{color:#8b949e;font-size:10px;text-transform:uppercase}
.m b{display:block;margin-top:4px;font-size:17px}
.tabs{display:flex;gap:6px;margin-bottom:10px}
.tab{flex:1;border:1px solid var(--line);background:transparent;color:#8b949e;border-radius:12px;padding:10px;font-weight:800;font-size:12px}
.tab.active{background:#238636;border-color:#238636;color:#fff}
.card{background:var(--card);border:1px solid var(--line);border-radius:16px;padding:14px;margin-bottom:10px}
label{font-size:11px;color:#8b949e}
.form-control,.form-select{background:#0b1220!important;border-color:var(--line)!important;color:#e6edf3!important;border-radius:10px!important}
.btn-go{background:#238636;border:0;color:#fff;font-weight:900;border-radius:12px;padding:12px;width:100%}
.btn-stop{background:#da3633;border:0;color:#fff;font-weight:900;border-radius:12px;padding:12px;width:100%}
.log{height:170px;overflow:auto;background:#05080f;border:1px solid var(--line);border-radius:12px;padding:10px;font:11px ui-monospace,monospace;color:var(--g)}
#fab{position:fixed;right:14px;bottom:18px;width:56px;height:56px;border-radius:50%;background:#238636;color:#fff;display:flex;align-items:center;justify-content:center;font-size:22px;font-weight:900;z-index:99;box-shadow:0 10px 25px #0008}
#mini{display:none;position:fixed;left:10px;right:10px;bottom:14px;z-index:98;background:#101826f2;border:1px solid var(--line);border-radius:16px;padding:10px}
#mini.show{display:block}
.hint{font-size:11px;color:#8b949e}
</style>
</head>
<body>
<div class="wrap">
  <div class="top">
    <div class="brand">⚡ MK Dual Engine</div>
    <div id="st" class="pill off">● Offline</div>
  </div>
  <div class="grid">
    <div class="m"><small>Bid/Ask</small><b id="px" class="text-success">0/0</b></div>
    <div class="m"><small>Balance</small><b id="bal" class="text-info">$0</b></div>
    <div class="m"><small>Floating</small><b id="pl">$0</b></div>
    <div class="m"><small>Score / Dir</small><b id="dir" class="text-warning">0% WAIT</b></div>
  </div>
  <div class="tabs">
    <button class="tab active" data-t="live">Live</button>
    <button class="tab" data-t="set">Settings</button>
    <button class="tab" data-t="conn">Connect</button>
  </div>

  <div id="p-live">
    <div class="card">
      <div class="d-flex justify-content-between align-items-center mb-2">
        <div><b style="color:#fff">Automation</b><div class="hint" id="last">Last: —</div></div>
        <div style="display:flex;gap:8px;width:160px">
          <button id="btnStart" class="btn-go" style="padding:10px" disabled onclick="startBot()">RUN</button>
          <button id="btnStop" class="btn-stop" style="padding:10px" disabled onclick="stopBot()">STOP</button>
        </div>
      </div>
      <table class="table table-dark table-sm mb-0" style="font-size:11px">
        <thead><tr><th>Sym</th><th>Side</th><th>Lot</th><th>P/L</th><th></th></tr></thead>
        <tbody id="pos"><tr><td colspan="5" class="text-muted text-center">No trades</td></tr></tbody>
      </table>
    </div>
    <div class="card"><div style="color:#fff;font-weight:800;margin-bottom:6px">Live Log</div><div id="log" class="log"></div></div>
  </div>

  <div id="p-set" style="display:none">
    <div class="card">
      <label>Mode</label>
      <select id="mode" class="form-select mb-2">
        <option value="SCALP">SCALP — fast open/close (minutes style)</option>
        <option value="SWING">SWING — hold until reverse / larger target</option>
      </select>
      <label>Strategy</label>
      <select id="strategy" class="form-select mb-2">
        <option value="CONFLUENCE">Confluence (best accuracy)</option>
        <option value="MOMENTUM">Momentum only</option>
        <option value="EMA">EMA trend only</option>
      </select>
      <label>Symbol</label>
      <select id="sym" class="form-select mb-2">
        <option>BTCUSD</option><option>ETHUSD</option><option>XAUUSD</option><option>EURUSD</option>
      </select>
      <div class="row g-2">
        <div class="col-6"><label>Scalp target $</label><input id="target" type="number" class="form-control" value="0.50" step="0.1"></div>
        <div class="col-6"><label>Swing target $</label><input id="swing" type="number" class="form-control" value="5.00" step="0.5"></div>
        <div class="col-6"><label>Min accuracy %</label><input id="score" type="number" class="form-control" value="70"></div>
        <div class="col-6"><label>Lot</label><input id="lot" type="number" class="form-control" value="0.01" step="0.01"></div>
        <div class="col-6"><label>Max trades</label><input id="max" type="number" class="form-control" value="1"></div>
        <div class="col-6"><label>SL points</label><input id="sl" type="number" class="form-control" value="1200"></div>
      </div>
      <p class="hint mt-2 mb-0">SWING holds while trend continues and banks on reverse-in-profit. SCALP banks quickly at scalp target. No guarantee of 10× — risk stays on.</p>
    </div>
  </div>

  <div id="p-conn" style="display:none">
    <div class="card">
      <label>MetaApi Token</label>
      <input id="token" type="password" class="form-control mb-2">
      <label>Method</label>
      <select id="method" class="form-select mb-2" onchange="toggleMethod()">
        <option value="id">Account ID (recommended)</option>
        <option value="login">Login + Password + Server</option>
      </select>
      <div id="box-id"><label>Account ID</label><input id="accid" class="form-control mb-2"></div>
      <div id="box-login" style="display:none">
        <select id="plat" class="form-select mb-2"><option value="mt5">MT5</option><option value="mt4">MT4</option></select>
        <input id="login" class="form-control mb-2" placeholder="Login" value="476924559">
        <input id="pass" type="password" class="form-control mb-2" placeholder="Password">
        <input id="server" class="form-control mb-2" value="Exness-MT5Trial9">
      </div>
      <button class="btn-go" onclick="doConnect()">Connect</button>
    </div>
  </div>
</div>

<div id="fab">⚡</div>
<div id="mini">
  <div class="d-flex justify-content-between align-items-center">
    <div><b id="miniDir" style="color:#fff">WAIT</b><div class="hint" id="miniBal">$0</div></div>
    <div style="display:flex;gap:6px">
      <button class="btn btn-success btn-sm" onclick="startBot()">RUN</button>
      <button class="btn btn-danger btn-sm" onclick="stopBot()">STOP</button>
      <button class="btn btn-secondary btn-sm" onclick="document.getElementById('mini').classList.remove('show')">Open</button>
    </div>
  </div>
</div>

<script>
document.querySelectorAll('.tab').forEach(b=>b.onclick=()=>{
  document.querySelectorAll('.tab').forEach(x=>x.classList.remove('active')); b.classList.add('active');
  const t=b.dataset.t;
  p-live.style.display=t==='live'?'block':'none';
  p-set.style.display=t==='set'?'block':'none';
  p-conn.style.display=t==='conn'?'block':'none';
});
function toggleMethod(){const m=method.value; box-id.style.display=m==='id'?'block':'none'; box-login.style.display=m==='login'?'block':'none';}
document.getElementById('fab').onclick=()=>document.getElementById('mini').classList.add('show');
if('serviceWorker' in navigator) navigator.serviceWorker.register('/sw.js').catch(()=>{});

async function refresh(){
  try{
    const d=await (await fetch('/api/status')).json();
    st.textContent='● '+d.status; st.className='pill '+(d.connected?'on':'off');
    px.textContent=d.tick.bid.toFixed(2)+' / '+d.tick.ask.toFixed(2);
    bal.textContent='$'+d.balance.toFixed(2);
    pl.textContent=(d.profit>=0?'+':'')+d.profit.toFixed(2);
    pl.style.color=d.profit>=0?'#3fb950':'#f85149';
    dir.textContent=(d.score||0)+'% '+d.direction;
    miniDir.textContent=d.direction; miniBal.textContent='$'+d.balance.toFixed(2)+' | '+(d.profit>=0?'+':'')+d.profit.toFixed(2);
    last.textContent='Last: '+(d.last_action||'—');
    log.innerHTML=(d.logs||[]).join('<br>'); log.scrollTop=1e9;
    let h='';
    (d.positions||[]).forEach(p=>{
      h+=`<tr><td>${p.symbol}</td><td>${p.type}</td><td>${p.volume}</td>
      <td style="color:${p.profit>=0?'#3fb950':'#f85149'}">$${p.profit.toFixed(2)}</td>
      <td><button class="btn btn-danger btn-sm py-0" onclick="closePos('${p.id}')">X</button></td></tr>`;
    });
    pos.innerHTML=h||'<tr><td colspan="5" class="text-center text-muted">No trades</td></tr>';
    btnStart.disabled=!d.connected||d.running; btnStop.disabled=!d.running;
  }catch(e){}
}
async function doConnect(){
  const body={token:token.value.trim(),method:method.value,account_id:accid.value.trim(),login:login.value.trim(),password:pass.value,server:server.value.trim(),platform:plat.value};
  const d=await (await fetch('/api/connect',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)})).json();
  alert(d.success?'✅ Connected':('❌ '+d.message));
}
async function startBot(){
  await 
