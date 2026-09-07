import os
import re
import asyncio
import threading
import logging
from datetime import datetime, timedelta, timezone

from flask import Flask, render_template_string, request, jsonify, Response
from metaapi_cloud_sdk import MetaApi
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
app = Flask(__name__)

# ---------- background asyncio loop (Railway/gunicorn safe) ----------
_loop = None

def _run_loop():
    global _loop
    _loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_loop)
    _loop.run_forever()

threading.Thread(target=_run_loop, daemon=True).start()

def run_async(coro, timeout=120):
    fut = asyncio.run_coroutine_threadsafe(coro, _loop)
    return fut.result(timeout=timeout)

# ---------- state ----------
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
    "target_profit": 0.50,
    "swing_target": 5.0,
    "min_score": 70,
    "sl_points": 1200,
    "positions": [],
    "tick": {"bid": 0.0, "ask": 0.0},
    "direction": "WAITING",
    "score": 0,
    "last_action": "—",
    "logs": ["MK Engine ready. Connect account, set mode, press RUN."],
}

def log(msg: str):
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    S["logs"].append(line)
    if len(S["logs"]) > 120:
        S["logs"].pop(0)
    logging.info(msg)

# ---------- indicators ----------
def ema(series, n):
    return series.ewm(span=n, adjust=False).mean()

def rsi(series, n=14):
    d = series.diff()
    gain = d.clip(lower=0).rolling(n).mean()
    loss = (-d.clip(upper=0)).rolling(n).mean()
    rs = gain / (loss + 1e-9)
    return 100 - (100 / (1 + rs))

def macd_hist(series):
    line = ema(series, 12) - ema(series, 26)
    signal = ema(line, 9)
    return line - signal

# ---------- engine ----------
class Engine:
    def __init__(self):
        self.api = None
        self.account = None
        self.conn = None
        self.prices = []
        self.specs = {}

    def fix_server(self, server: str) -> str:
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

    async def connect_id(self, token, account_id):
        self.api = MetaApi(token)
        self.account = await self.api.metatrader_account_api.get_account(account_id.strip())
        return await self._ready()

    async def connect_login(self, token, login, password, server, platform):
        server = self.fix_server(server)
        log(f"Connecting {server} | login {login}")
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
            log("Creating MetaApi cloud account...")
            self.account = await self.api.metatrader_account_api.create_account({
                "name": f"MK-{login}",
                "type": "cloud",
                "login": str(login),
                "password": str(password),
                "server": server,
                "platform": "mt5" if "5" in str(platform).lower() else "mt4",
                "magic": 260318,
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
        log("✅ Connected and synchronized")
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
            pos = []
            for p in raw:
                side = "BUY" if "BUY" in str(p.get("type", "")) else "SELL"
                pos.append({
                    "id": str(p.get("id")),
                    "symbol": p.get("symbol"),
                    "type": side,
                    "volume": float(p.get("volume", 0.01)),
                    "open": float(p.get("openPrice", 0)),
                    "current": float(p.get("currentPrice", 0)),
                    "profit": float(p.get("profit", 0)),
                })
            S["positions"] = pos
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
                    log(f"Symbol resolved {symbol} → {s}")
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

    async def candles(self, symbol, tf="1m", count=80):
        try:
            start = datetime.now(timezone.utc) - timedelta(days=2)
            rows = None
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

        # fallback synthetic series from live tick
        try:
            px = await self.conn.get_symbol_price(symbol)
            m = (float(px["bid"]) + float(px["ask"])) / 2.0
            self.prices.append(m)
            if len(self.prices) > 80:
                self.prices.pop(0)
            arr = list(self.prices) if len(self.prices) >= 30 else [m] * 40
            return pd.DataFrame({
                "close": arr,
                "high": [x * 1.0002 for x in arr],
                "low": [x * 0.9998 for x in arr],
            })
        except Exception:
            return None

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
        sp = await self.spec(symbol)
        digits, point = sp["digits"], sp["point"]
        px = await self.conn.get_symbol_price(symbol)
        entry = float(px["ask"] if side == "BUY" else px["bid"])
        scale = 1.0 if ("BTC" in symbol or "ETH" in symbol) else point
        sl = entry - sl_points * scale if side == "BUY" else entry + sl_points * scale
        mult = 4 if S["mode"] == "SWING" else 2
        tp = entry + sl_points * mult * scale if side == "BUY" else entry - sl_points * mult * scale

        log(f"🚀 {S['mode']} {side} {symbol} lot={lot} @ {entry:.2f} score={S['score']}%")
        S["last_action"] = f"{side} {symbol}"

        if side == "BUY":
            await self.conn.create_market_buy_order(symbol, lot, round(sl, digits), round(tp, digits))
        else:
            await self.conn.create_market_sell_order(symbol, lot, round(sl, digits), round(tp, digits))
        log("✅ Order sent")
        await self.refresh()

    def score_signal(self, df):
        if df is None or len(df) < 25:
            return "WAITING", 0, "WAITING"

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

        mom = 0.0
        if len(self.prices) >= 5:
            mom = self.prices[-1] - self.prices[-5]

        th = 1.5
        if "XAU" in S["real_symbol"]:
            th = 0.08
        elif "BTC" not in S["real_symbol"] and "ETH" not in S["real_symbol"]:
            th = 0.00008

        buy = 0
        sell = 0
        st = S["strategy"]

        if st in ("CONFLUENCE", "EMA"):
            if e9 > e21:
                buy += 25
            if e9 < e21:
                sell += 25
            if pe9 <= pe21 and e9 > e21:
                buy += 15
            if pe9 >= pe21 and e9 < e21:
                sell += 15

        if st in ("CONFLUENCE", "MOMENTUM"):
            if 52 <= r <= 68:
                buy += 20
            if 32 <= r <= 48:
                sell += 20
            if mh > 0 and mh >= pmh:
                buy += 20
            if mh < 0 and mh <= pmh:
                sell += 20
            if mom > th:
                buy += 20
            if mom < -th:
                sell += 20

        if st == "MOMENTUM":
            buy = (25 if mom > th else 0) + (20 if mh > 0 else 0) + (15 if e9 > e21 else 0)
            sell = (25 if mom < -th else 0) + (20 if mh < 0 else 0) + (15 if e9 < e21 else 0)

        buy = max(0, min(100, buy))
        sell = max(0, min(100, sell))
        need = int(S["min_score"])

        if buy >= need and buy >= sell:
            return "BUY", buy, "UP"
        if sell >= need and sell > buy:
            return "SELL", sell, "DOWN"
        return "WAITING", max(buy, sell), "FLAT"

    async def manage(self, symbol, side_now):
        target = S["target_profit"] if S["mode"] == "SCALP" else S["swing_target"]
        for p in list(S["positions"]):
            if not self.same_symbol(p["symbol"], symbol):
                continue
            pr = p["profit"]

            if pr >= target:
                log(f"🎯 Target hit ${pr:.2f} → close")
                await self.close(p["id"])
                continue

            # reverse-in-profit exits
            if p["type"] == "BUY" and side_now == "SELL" and pr > 0:
                log(f"🔄 Flip + profit ${pr:.2f} → close BUY")
                await self.close(p["id"])
            elif p["type"] == "SELL" and side_now == "BUY" and pr > 0:
                log(f"🔄 Flip + profit ${pr:.2f} → close SELL")
                await self.close(p["id"])

    async def run(self):
        log(f"🔥 RUN mode={S['mode']} strategy={S['strategy']} min_score={S['min_score']}%")
        S["real_symbol"] = await self.resolve(S["symbol"])
        symbol = S["real_symbol"]
        try:
            await self.conn.subscribe_to_market_data(symbol)
        except Exception:
            pass

        while S["running"] and S["connected"]:
            try:
                px = await self.conn.get_symbol_price(symbol)
                bid, ask = float(px["bid"]), float(px["ask"])
                mid = (bid + ask) / 2.0
                S["tick"] = {"bid": bid, "ask": ask}
                self.prices.append(mid)
                if len(self.prices) > 60:
                    self.prices.pop(0)

                tf = "1m" if S["mode"] == "SCALP" else "5m"
                df = await self.candles(symbol, tf, 80)
                side, score, label = self.score_signal(df)
                S["score"] = score
                S["direction"] = label

                log(f"📊 {symbol} {bid:.2f}/{ask:.2f} score={score}% dir={label}")

                await self.manage(symbol, side)

                mine = [p for p in S["positions"] if self.same_symbol(p["symbol"], symbol)]
                if len(mine) < S["max_trades"] and side in ("BUY", "SELL"):
                    log(f"✅ Entry {side} score {score}%")
                    await self.open_trade(side, symbol, S["lot"], S["sl_points"])

                await self.refresh()
            except Exception as e:
                log(f"Loop: {e}")

            await asyncio.sleep(0.9 if S["mode"] == "SCALP" else 1.6)

        log("Engine stopped")

engine = Engine()

# ---------- UI ----------
HTML = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="theme-color" content="#070b12">
<link rel="manifest" href="/manifest.json">
<title>MK Dual Engine</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
<style>
body{margin:0;background:#070b12;color:#c9d1d9;font-family:system-ui,sans-serif;padding-bottom:90px}
.wrap{max-width:480px;margin:0 auto;padding:12px}
.top{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px}
.brand{font-weight:900;color:#fff}
.pill{font-size:11px;font-weight:800;padding:6px 10px;border-radius:20px;border:1px solid #444}
.on{color:#3fb950;border-color:#238636;background:#23863622}
.off{color:#f85149;border-color:#da3633;background:#da363322}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:10px}
.m{background:#101826;border:1px solid #243247;border-radius:14px;padding:10px;text-align:center}
.m small{color:#8b949e;font-size:10px}
.m b{display:block;margin-top:4px;font-size:16px}
.tabs{display:flex;gap:6px;margin-bottom:10px}
.tab{flex:1;border:1px solid #243247;background:transparent;color:#8b949e;border-radius:12px;padding:10px;font-weight:800}
.tab.active{background:#238636;border-color:#238636;color:#fff}
.card{background:#101826;border:1px solid #243247;border-radius:16px;padding:14px;margin-bottom:10px}
label{font-size:11px;color:#8b949e}
.form-control,.form-select{background:#0b1220!important;border-color:#243247!important;color:#e6edf3!important}
.btn-go{background:#238636;border:0;color:#fff;font-weight:900;border-radius:12px;padding:12px;width:100%}
.btn-stop{background:#da3633;border:0;color:#fff;font-weight:900;border-radius:12px;padding:12px;width:100%}
.log{height:170px;overflow:auto;background:#05080f;border:1px solid #243247;border-radius:12px;padding:10px;font:11px monospace;color:#3fb950}
#fab{position:fixed;right:14px;bottom:18px;width:56px;height:56px;border-radius:50%;background:#238636;color:#fff;display:flex;align-items:center;justify-content:center;font-size:22px;font-weight:900;z-index:99}
#mini{display:none;position:fixed;left:10px;right:10px;bottom:14px;z-index:98;background:#101826f2;border:1px solid #243247;border-radius:16px;padding:10px}
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
        <div><b style="color:#fff">Automation</b><div class="hint" id="last">Last: —</div></div>
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
        <option value="SCALP">SCALP (fast close at $ target)</option>
        <option value="SWING">SWING (hold until reverse / larger target)</option>
      </select>
      <label>Strategy</label>
      <select id="strategy" class="form-select mb-2">
        <option value="CONFLUENCE">Confluence (recommended)</option>
        <option value="MOMENTUM">Momentum</option>
        <option value="EMA">EMA</option>
      </select>
      <label>Symbol</label>
      <select id="sym" class="form-select mb-2">
        <option>BTCUSD</option><option>ETHUSD</option><option>XAUUSD</option><option>EURUSD</option>
      </select>
      <div class="row g-2">
        <div class="col-6"><label>Scalp target $</label><input id="target" type="number" class="form-control" value="0.50" step="0.1"></div>
        <div class="col-6"><label>Swing target $</label><input id="swing" type="number" class="form-control" value="5" step="0.5"></div>
        <div class="col-6"><label>Min accuracy %</label><input id="score" type="number" class="form-control" value="70"></div>
        <div class="col-6"><label>Lot</label><input id="lot" type="number" class="form-control" value="0.01" step="0.01"></div>
        <div class="col-6"><label>Max trades</label><input id="max" type="number" class="form-control" value="1"></div>
        <div class="col-6"><label>SL points</label><input id="sl" type="number" class="form-control" value="1200"></div>
      </div>
      <p class="hint mt-2 mb-0">No bot guarantees 10x. Use demo first. Start lot 0.01.</p>
    </div>
  </div>

  <div id="panel-conn" style="display:none">
    <div class="card">
      <label>MetaApi Token</label>
      <input id="token" type="password" class="form-control mb-2">
      <label>Method</label>
      <select id="method" class="form-select mb-2">
        <option value="id">Account ID (recommended)</option>
        <option value="login">Login + Password + Server</option>
      </select>
      <div id="box-id">
        <label>Account ID</label>
        <input id="accid" class="form-control mb-2" placeholder="MetaApi account UUID">
      </div>
      <div id="box-login" style="display:none">
        <select id="plat" class="form-select mb-2"><option value="mt5">MT5</option><option value="mt4">MT4</option></select>
        <input id="login" class="form-control mb-2" placeholder="Login" value="476924559">
        <input id="pass" type="password" class="form-control mb-2" placeholder="Password">
        <input id="server" class="form-control mb-2" value="Exness-MT5Trial9">
      </div>
      <button class="btn-go" id="btnConnect">Connect</button>
    </div>
  </div>
</div>

<div id="fab">⚡</div>
<div id="mini">
  <div class="d-flex justify-content-between align-items-center">
    <div><b id="miniDir" style="color:#fff">WAIT</b><div class="hint" id="miniBal">$0</div></div>
    <div style="display:flex;gap:6px">
      <button class="btn btn-success btn-sm" id="miniStart">RUN</button>
      <button class="btn btn-danger btn-sm" id="miniStop">STOP</button>
      <button class="btn btn-secondary btn-sm" id="miniOpen">Open</button>
    </div>
  </div>
</div>

<script>
const $ = (id) => document.getElementById(id);

document.querySelectorAll('.tab').forEach(btn => {
  btn.onclick = () => {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    btn.classList.add('active');
    const t = btn.dataset.t;
    $('panel-live').style.display = t === 'live' ? 'block' : 'none';
    $('panel-set').style.display = t === 'set' ? 'block' : 'none';
    $('panel-conn').style.display = t === 'conn' ? 'block' : 'none';
  };
});

$('method').onchange = () => {
  const m = $('method').value;
  $('box-id').style.display = m === 'id' ? 'block' : 'none';
  $('box-login').style.display = m === 'login' ? 'block' : 'none';
};

$('fab').onclick = () => $('mini').classList.add('show');
$('miniOpen').onclick = () => { $('mini').classList.remove('show'); window.scrollTo(0,0); };

async function refresh(){
  try{
    const d = await (await fetch('/api/status')).json();
    $('st').textContent = '● ' + d.status;
    $('st').className = 'pill ' + (d.connected ? 'on' : 'off');
    $('px').textContent = d.tick.bid.toFixed(2) + ' / ' + d.tick.ask.toFixed(2);
    $('bal').textContent = '$' + d.balance.toFixed(2);
    $('pl').textContent = (d.profit >= 0 ? '+' : '') + d.profit.toFixed(2);
    $('pl').style.color = d.profit >= 0 ? '#3fb950' : '#f85149';
    $('dir').textContent = (d.score || 0) + '% ' + d.direction;
    $('miniDir').textContent = d.direction;
    $('miniBal').textContent = '$' + d.balance.toFixed(2) + ' | ' + ((d.profit>=0?'+':'') + d.profit.toFixed(2));
    $('last').textContent = 'Last: ' + (d.last_action || '—');
    $('log').innerHTML = (d.logs || []).join('<br>');
    $('log').scrollTop = 99999;

    let h = '';
    (d.positions || []).forEach(p => {
      h += `<tr>
        <td>${p.symbol}</td><td>${p.type}</td><td>${p.volume}</td>
        <td style="color:${p.profit>=0?'#3fb950':'#f85149'}">$${p.profit.toFixed(2)}</td>
        <td><button class="btn btn-danger btn-sm py-0" data-id="${p.id}">X</button></td>
      </tr>`;
    });
    $('pos').innerHTML = h || '<tr><td colspan="5" class="text-center text-muted">No trades</td></tr>';
    $('pos').querySelectorAll('button[data-id]').forEach(b => b.onclick = () => closePos(b.dataset.id));
    $('btnStart').disabled = !d.connected || d.running;
    $('btnStop').disabled = !d.running;
    $('miniStart').disabled = !d.connected || d.running;
    $('miniStop').disabled = !d.running;
  }catch(e){}
}

async function doConnect(){
  const body = {
    token: $('token').value.trim(),
    method: $('method').value,
    account_id: $('accid').value.trim(),
    login: $('login').value.trim(),
    password: $('pass').value,
    server: $('server').value.trim(),
    platform: $('plat').value
  };
  const d = await (await fetch('/api/connect', {
    method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body)
  })).json();
  alert(d.success ? '✅ Connected' : ('❌ ' + d.message));
}

async function startBot(){
  await fetch('/api/start', {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({
      mode: $('mode').value,
      strategy: $('strategy').value,
      symbol: $('sym').value,
      target_profit: parseFloat($('target').value),
      swing_target: parseFloat($('swing').value),
      min_score: parseInt($('score').value),
      lot: parseFloat($('lot').value),
      max_trades: parseInt($('max').value),
      sl_points: parseInt($('sl').value)
    })
  });
  $('mini').classList.remove('show');
}

async function stopBot(){ await fetch('/api/stop', {method:'POST'}); }
async function closePos(id){
  if(confirm('Close trade?')) await fetch('/api/close', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({id})});
}

$('btnConnect').onclick = doConnect;
$('btnStart').onclick = startBot;
$('btnStop').onclick = stopBot;
$('miniStart').onclick = startBot;
$('miniStop').onclick = stopBot;

if('serviceWorker' in navigator) navigator.serviceWorker.register('/sw.js').catch(()=>{});
setInterval(refresh, 1000);
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
    return jsonify({
        "name": "MK Dual Engine",
        "short_name": "MK Bot",
        "start_url": "/",
        "display": "standalone",
        "background_color": "#070b12",
        "theme_color": "#070b12",
        "icons": [{
            "src": "https://cdn.jsdelivr.net/gh/twitter/twemoji@14.0.2/assets/72x72/26a1.png",
            "sizes": "192x192",
            "type": "image/png"
        }]
    })

@app.route("/sw.js")
def sw():
    return Response(
        "self.addEventListener('install',e=>self.skipWaiting());self.addEventListener('activate',e=>e.waitUntil(clients.claim()));",
        mimetype="application/javascript",
    )

@app.route("/api/status")
def api_status():
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

    try:
        if data.get("method") == "id":
            ok, msg = run_async(engine.connect_id(token, data.get("account_id", "")), timeout=120)
        else:
            ok, msg = run_async(
                engine.connect_login(
                    token,
                    data.get("login"),
                    data.get("password"),
                    data.get("server"),
                    data.get("platform", "mt5"),
                ),
                timeout=120,
            )
        return jsonify(success=ok, message=msg)
    except Exception as e:
        S["status"] = "Failed"
        S["error"] = str(e)
        log(f"❌ {e}")
        return jsonify(success=False, message=str(e))

@app.route("/api/start", methods=["POST"])
def api_start():
    if not S["connected"]:
        return jsonify(ok=False, message="Connect first")
    d = request.json or {}
    S["mode"] = d.get("mode", "SCALP")
    S["strategy"] = d.get("strategy", "CONFLUENCE")
    S["symbol"] = (d.get("symbol") or "BTCUSD").upper()
    S["target_profit"] = float(d.get("target_profit", 0.5))
    S["swing_target"] = float(d.get("swing_target", 5.0))
    S["min_score"] = int(d.get("min_score", 70))
    S["lot"] = float(d.get("lot", 0.01))
    S["max_trades"] = int(d.get("max_trades", 1))
    S["sl_points"] = int(d.get("sl_points", 1200))
    S["running"] = True
    asyncio.run_coroutine_threadsafe(engine.run(), _loop)
    return jsonify(ok=True)

@app.route("/api/stop", methods=["POST"])
def api_stop():
    S["running"] = False
    log("STOP requested")
    return jsonify(ok=True)

@app.route("/api/close", methods=["POST"])
def api_close():
    pid = (request.json or {}).get("id")
    if pid and S["connected"]:
        asyncio.run_coroutine_threadsafe(engine.close(pid), _loop)
    return jsonify(ok=True)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
