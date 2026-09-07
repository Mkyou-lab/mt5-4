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
    "lot": 0.01,
    "max_trades": 1,
    "target_profit": 0.80,
    "min_score": 60,
    "sl_points": 250,
    "tp_points": 500,   # TP twice SL by default (better R:R)
    "flip_signals": False,
    "positions": [],
    "tick": {"bid": 0.0, "ask": 0.0},
    "direction": "WAITING",
    "score": 0,
    "phase": "SCAN",
    "last_action": "—",
    "logs": ["Pullback-Continuation engine ready. Connect → RUN."]
}

def log(msg):
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    S["logs"].append(line)
    if len(S["logs"]) > 160:
        S["logs"].pop(0)
    print(line)

class Engine:
    def __init__(self):
        self.api = None
        self.account = None
        self.conn = None
        self.prices = []
        self.specs = {}
        # setup memory
        self.impulse = None          # "UP" or "DOWN"
        self.impulse_price = None
        self.impulse_time = 0.0
        self.pullback_seen = False
        self.pullback_ext = None
        self.last_entry_ts = 0.0
        self.be_done = set()

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
            c = re.sub(r"[/._]", "", symbol).upper()
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
                "point": float(sp.get("point", 0.01) or 0.01),
                "min_volume": float(sp.get("minVolume", sp.get("volumeMin", 0.01)) or 0.01),
                "max_volume": float(sp.get("maxVolume", sp.get("volumeMax", 100)) or 100),
                "volume_step": float(sp.get("volumeStep", 0.01) or 0.01),
                "stops_level": float(sp.get("stopsLevel", sp.get("tradeStopsLevel", 0)) or 0),
            }
            self.specs[symbol] = spec
            log(f"Spec {symbol}: point={spec['point']} stops={spec['stops_level']} minLot={spec['min_volume']}")
            return spec
        except Exception as e:
            log(f"Spec fallback ({e})")
            spec = {"digits": 2, "point": 0.01, "min_volume": 0.01, "max_volume": 100, "volume_step": 0.01, "stops_level": 0}
            self.specs[symbol] = spec
            return spec

    def normalize_lot(self, lot, spec):
        step = spec["volume_step"] if spec["volume_step"] > 0 else 0.01
        mn, mx = spec["min_volume"], spec["max_volume"]
        steps = int(float(lot) / step)
        norm = max(mn, min(mx, steps * step))
        return float(f"{norm:.2f}") if step >= 0.01 else float(norm)

    def same_symbol(self, a, b):
        x = re.sub(r"[T._]", "", str(a).upper())
        y = re.sub(r"[T._]", "", str(b).upper())
        return x in y or y in x

    def thresholds(self, symbol):
        if "XAU" in symbol:
            return {"impulse": 0.12, "pullback": 0.05, "continue": 0.04, "spread_max": 0.60}
        if "BTC" in symbol:
            return {"impulse": 8.0, "pullback": 3.0, "continue": 2.5, "spread_max": 25.0}
        if "ETH" in symbol:
            return {"impulse": 1.5, "pullback": 0.6, "continue": 0.5, "spread_max": 2.0}
        return {"impulse": 0.00025, "pullback": 0.00010, "continue": 0.00008, "spread_max": 0.00030}

    async def close(self, pid):
        try:
            await self.conn.close_position(str(pid))
            log(f"💰 Closed #{pid}")
            S["last_action"] = f"Closed #{pid}"
            self.be_done.discard(str(pid))
            await self.refresh()
            return True
        except Exception:
            try:
                await self.conn.close_position(int(pid))
                log(f"💰 Closed #{pid}")
                self.be_done.discard(str(pid))
                await self.refresh()
                return True
            except Exception as e:
                log(f"Close error: {e}")
                return False

    def build_sl_tp(self, side, entry, spec, sl_points, tp_points):
        digits = spec["digits"]
        point = spec["point"] if spec["point"] > 0 else 0.01
        stops_level = spec["stops_level"]
        sl_pts = max(float(sl_points), stops_level + 5, 30)
        tp_pts = max(float(tp_points), sl_pts * 1.5, stops_level + 5, 40)  # force better R:R
        sl_dist = sl_pts * point
        tp_dist = tp_pts * point
        if side == "BUY":
            sl, tp = entry - sl_dist, entry + tp_dist
        else:
            sl, tp = entry + sl_dist, entry - tp_dist
        return round(sl, digits), round(tp, digits), sl_pts, tp_pts

    async def open_trade(self, side, symbol, lot, sl_points, tp_points):
        try:
            if S.get("flip_signals"):
                side = "SELL" if side == "BUY" else "BUY"
                log(f"🔁 Flip enabled → side now {side}")

            spec = await self.load_spec(symbol)
            lot = self.normalize_lot(lot, spec)
            digits = spec["digits"]
            px = await self.conn.get_symbol_price(symbol)
            bid, ask = float(px["bid"]), float(px["ask"])
            spread = ask - bid
            th = self.thresholds(symbol)
            if spread > th["spread_max"]:
                log(f"⛔ Spread too wide {spread:.5f} > {th['spread_max']} — skip entry")
                return False

            entry = ask if side == "BUY" else bid
            attempts = [(sl_points, tp_points), (sl_points*1.5, tp_points*1.5), (sl_points*2.0, tp_points*2.2)]
            last_err = None

            for i, (slp, tpp) in enumerate(attempts, start=1):
                sl, tp, usl, utp = self.build_sl_tp(side, entry, spec, slp, tpp)
                log(f"🧾 Attempt {i}: {side} {symbol} lot={lot} @ {entry:.{digits}f} | SL={sl} ({usl:.0f}pts) | TP={tp} ({utp:.0f}pts)")
                try:
                    if side == "BUY":
                        result = await self.conn.create_market_buy_order(symbol, lot, sl, tp)
                    else:
                        result = await self.conn.create_market_sell_order(symbol, lot, sl, tp)
                    log(f"✅ Order accepted: {result}")
                    await asyncio.sleep(1.0)
                    await self.refresh()
                    mine = [p for p in S["positions"] if self.same_symbol(p["symbol"], symbol)]
                    if mine:
                        p = mine[-1]
                        log(f"🎉 OPENED #{p['id']} {p['type']} SL={p['sl']} TP={p['tp']}")
                        S["last_action"] = f"OPENED #{p['id']} {p['type']}"
                        self.last_entry_ts = datetime.now(timezone.utc).timestamp()
                        # reset setup after entry
                        self.impulse = None
                        self.pullback_seen = False
                        self.pullback_ext = None
                        S["phase"] = "IN_TRADE"
                        return True
                    log("❌ No position after accept")
                except Exception as e:
                    last_err = e
                    log(f"⚠️ Attempt {i} rejected: {e}")
                    if any(k in str(e).lower() for k in ["stop", "invalid", "distance", "level", "s/l", "t/p"]):
                        continue
                    break

            log(f"❌ Open failed: {last_err}")
            S["last_action"] = f"OPEN FAIL: {last_err}"
            return False
        except Exception as e:
            log(f"❌ open_trade crash: {e}")
            return False

    def update_setup(self, mid, symbol):
        """Impulse → pullback → continuation (anti-chase)."""
        th = self.thresholds(symbol)
        now = datetime.now(timezone.utc).timestamp()

        if len(self.prices) < 10:
            S["phase"] = "WARMUP"
            return "WAITING", 0, "WAIT", "warmup"

        # recent swings
        window = self.prices[-10:]
        mom3 = self.prices[-1] - self.prices[-4]
        mom6 = self.prices[-1] - self.prices[-7]
        hi = max(window)
        lo = min(window)

        # timeout old impulse
        if self.impulse and now - self.impulse_time > 45:
            self.impulse = None
            self.pullback_seen = False
            self.pullback_ext = None
            S["phase"] = "SCAN"

        # 1) detect fresh impulse (not entry yet)
        if self.impulse is None:
            S["phase"] = "SCAN"
            if mom3 > th["impulse"] and mom6 > th["impulse"] * 0.7:
                self.impulse = "UP"
                self.impulse_price = mid
                self.impulse_time = now
                self.pullback_seen = False
                self.pullback_ext = mid
                log(f"📡 Impulse UP detected @ {mid:.5f} — waiting pullback (no chase)")
                return "WAITING", 40, "IMPULSE UP", "impulseUP"
            if mom3 < -th["impulse"] and mom6 < -th["impulse"] * 0.7:
                self.impulse = "DOWN"
                self.impulse_price = mid
                self.impulse_time = now
                self.pullback_seen = False
                self.pullback_ext = mid
                log(f"📡 Impulse DOWN detected @ {mid:.5f} — waiting pullback (no chase)")
                return "WAITING", 40, "IMPULSE DOWN", "impulseDOWN"
            return "WAITING", 10, "FLAT", "no-impulse"

        # 2) wait pullback against impulse
        if self.impulse == "UP":
            # pullback = price dips
            if mid < self.impulse_price - th["pullback"]:
                self.pullback_seen = True
                self.pullback_ext = min(self.pullback_ext or mid, mid)
                S["phase"] = "PULLBACK"
            if self.pullback_seen:
                # continuation: turn back up from pullback low
                rebound = mid - (self.pullback_ext or mid)
                # avoid entering if already re-extended too far (chase protection)
                ext_from_impulse = mid - self.impulse_price
                if rebound >= th["continue"] and ext_from_impulse < th["impulse"] * 1.8:
                    score = 70
                    if mid > self.prices[-2] > self.prices[-3]:
                        score += 15
                    if mom3 > 0:
                        score += 10
                    score = min(100, score)
                    S["phase"] = "CONTINUE"
                    return "BUY", score, "UP", "pullback-continue-UP"
                return "WAITING", 55, "PULLBACK UP", "wait-continue-UP"
            return "WAITING", 45, "IMPULSE UP", "wait-pullback-UP"

        if self.impulse == "DOWN":
            if mid > self.impulse_price + th["pullback"]:
                self.pullback_seen = True
                self.pullback_ext = max(self.pullback_ext or mid, mid)
                S["phase"] = "PULLBACK"
            if self.pullback_seen:
                drop = (self.pullback_ext or mid) - mid
                ext_from_impulse = self.impulse_price - mid
                if drop >= th["continue"] and ext_from_impulse < th["impulse"] * 1.8:
                    score = 70
                    if mid < self.prices[-2] < self.prices[-3]:
                        score += 15
                    if mom3 < 0:
                        score += 10
                    score = min(100, score)
                    S["phase"] = "CONTINUE"
                    return "SELL", score, "DOWN", "pullback-continue-DOWN"
                return "WAITING", 55, "PULLBACK DOWN", "wait-continue-DOWN"
            return "WAITING", 45, "IMPULSE DOWN", "wait-pullback-DOWN"

        return "WAITING", 0, "FLAT", "idle"

    async def manage(self, symbol, side_now):
        target = float(S["target_profit"])
        for p in list(S["positions"]):
            if not self.same_symbol(p["symbol"], symbol):
                continue
            pid = str(p["id"])
            pr = float(p["profit"])

            # hard USD bank
            if pr >= target:
                log(f"🎯 USD TP ${pr:.2f} >= ${target:.2f} → close #{pid}")
                await self.close(pid)
                continue

            # break-even style early protect using reverse signal only if green
            if pr > 0 and (
                (p["type"] == "BUY" and side_now == "SELL") or
                (p["type"] == "SELL" and side_now == "BUY")
            ):
                log(f"🔒 Protect profit ${pr:.2f} on reverse → close #{pid}")
                await self.close(pid)
                continue

            # soft BE mark in logs / state (actual SL modify if supported)
            if pr >= max(0.25, target * 0.35) and pid not in self.be_done:
                self.be_done.add(pid)
                log(f"📌 Profit cushion on #{pid} ${pr:.2f} — protection armed")
                # try move SL near open if API supports
                try:
                    if hasattr(self.conn, "modify_position"):
                        open_px = float(p["open"])
                        # tiny lock-in offset
                        spec = await self.load_spec(symbol)
                        point = spec["point"]
                        if p["type"] == "BUY":
                            new_sl = open_px + 2 * point
                        else:
                            new_sl = open_px - 2 * point
                        tp = float(p["tp"]) if p["tp"] else None
                        if tp:
                            await self.conn.modify_position(pid, new_sl, tp)
                            log(f"✅ Moved SL near BE on #{pid}")
                except Exception as e:
                    log(f"BE modify note: {e}")

    async def run(self):
        log(
            f"🔥 RUN PULLBACK-CONTINUE | SL={S['sl_points']} TP={S['tp_points']} | "
            f"USD={S['target_profit']} | score>={S['min_score']} | flip={S['flip_signals']}"
        )
        S["real_symbol"] = await self.resolve(S["symbol"])
        symbol = S["real_symbol"]
        log(f"Trading symbol: {symbol}")
        await self.load_spec(symbol)
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
                if len(self.prices) > 200:
                    self.prices.pop(0)

                side, score, label, reason = self.update_setup(mid, symbol)
                S["score"] = score
                S["direction"] = label

                await self.refresh()
                mine = [p for p in S["positions"] if self.same_symbol(p["symbol"], symbol)]

                log(
                    f"📊 {symbol} {bid:.2f}/{ask:.2f} phase={S['phase']} score={score}% "
                    f"dir={label} reason={reason} openPos={len(mine)}"
                )

                await self.manage(symbol, side)

                now = datetime.now(timezone.utc).timestamp()
                if len(mine) >= S["max_trades"]:
                    p = mine[0]
                    log(f"⏳ Manage #{p['id']} P/L=${p['profit']:.2f} SL={p['sl']} TP={p['tp']}")
                elif side in ("BUY", "SELL") and score >= int(S["min_score"]) and now - self.last_entry_ts > 12:
                    log(f"✅ HIGH-QUALITY ENTRY {side} ({score}%) after pullback")
                    await self.open_trade(side, symbol, S["lot"], S["sl_points"], S["tp_points"])
                else:
                    need = int(S["min_score"])
                    log(f"… no entry yet | need>={need}% best={score}% phase={S['phase']}")

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
<title>MK Pullback Bot</title>
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
.log{height:220px;overflow:auto;background:#05080f;border:1px solid #243247;border-radius:12px;padding:10px;font:11px monospace;color:#3fb950}
#fab{position:fixed;right:14px;bottom:18px;width:56px;height:56px;border-radius:50%;background:#238636;color:#fff;display:flex;align-items:center;justify-content:center;font-weight:900;font-size:22px}
.hint{font-size:11px;color:#8b949e;margin-top:8px}
</style>
</head>
<body>
<div class="wrap">
  <div class="d-flex justify-content-between align-items-center">
    <div class="brand">⚡ MK Pullback Engine</div>
    <div id="st" class="pill off">● Offline</div>
  </div>
  <div class="grid">
    <div class="m"><small>Bid/Ask</small><b id="px">0/0</b></div>
    <div class="m"><small>Balance</small><b id="bal">$0</b></div>
    <div class="m"><small>Floating</small><b id="pl">$0</b></div>
    <div class="m"><small>Phase/Score</small><b id="dir">SCAN 0%</b></div>
  </div>
  <div class="tabs">
    <button class="tab active" data-t="live">Live</button>
    <button class="tab" data-t="set">Settings</button>
    <button class="tab" data-t="conn">Connect</button>
  </div>

  <div id="panel-live">
    <div class="card">
      <div class="d-flex justify-content-between align-items-center mb-2">
        <div><b style="color:#fff">Positions</b><div class="hint" id="last">Last: —</div></div>
        <div style="display:flex;gap:8px;width:170px">
          <button id="btnStart" class="btn-go" style="padding:10px" disabled>RUN</button>
          <button id="btnStop" class="btn-stop" style="padding:10px" disabled>STOP</button>
        </div>
      </div>
      <table class="table table-dark table-sm mb-0" style="font-size:11px">
        <thead><tr><th>Sym</th><th>Side</th><th>P/L</th><th>SL</th><th>TP</th><th></th></tr></thead>
        <tbody id="pos"><tr><td colspan="6" class="text-center text-muted">No trades</td></tr></tbody>
      </table>
    </div>
    <div class="card"><div style="color:#fff;font-weight:800;margin-bottom:6px">Live Log</div><div id="log" class="log"></div></div>
  </div>

  <div id="panel-set" style="display:none">
    <div class="card">
      <label>Symbol</label>
      <select id="sym" class="form-select mb-2">
        <option>XAUUSD</option><option>BTCUSD</option><option>ETHUSD</option><option>EURUSD</option>
      </select>
      <div class="row g-2">
        <div class="col-6"><label>SL points</label><input id="sl" class="form-control" type="number" value="250"></div>
        <div class="col-6"><label>TP points</label><input id="tp" class="form-control" type="number" value="500"></div>
        <div class="col-6"><label>USD close target</label><input id="target" class="form-control" type="number" value="0.8" step="0.1"></div>
        <div class="col-6"><label>Min score %</label><input id="score" class="form-control" type="number" value="60"></div>
        <div class="col-6"><label>Lot</label><input id="lot" class="form-control" type="number" value="0.01" step="0.01"></div>
        <div class="col-6"><label>Max trades</label><input id="max" class="form-control" type="number" value="1"></div>
      </div>
      <div class="form-check mt-3">
        <input class="form-check-input" type="checkbox" id="flip">
        <label class="form-check-label" for="flip">Flip signals (only if entries still feel inverted)</label>
      </div>
      <p class="hint">This version waits for pullback after impulse. It will trade less often, but should stop the “buy top / sell bottom” SL loop. TP every trade is impossible — better entries + TP>SL is the real fix.</p>
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
  $('dir').textContent=(d.phase||'SCAN')+' '+(d.score||0)+'%';
  $('last').textContent='Last: '+(d.last_action||'—');
  $('log').innerHTML=(d.logs||[]).join('<br>'); $('log').scrollTop=1e9;
  let h='';
  (d.positions||[]).forEach(p=>{
    h+=`<tr><td>${p.symbol}</td><td>${p.type}</td>
    <td style="color:${p.profit>=0?'#3fb950':'#f85149'}">$${p.profit.toFixed(2)}</td>
    <td>${p.sl||'-'}</td><td>${p.tp||'-'}</td>
    <td><button class="btn btn-danger btn-sm py-0" onclick="closePos('${p.id}')">X</button></td></tr>`;
  });
  $('pos').innerHTML=h||'<tr><td colspan="6" class="text-center text-muted">No trades</td></tr>';
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
    sl_points:parseInt($('sl').value),
    tp_points:parseInt($('tp').value),
    target_profit:parseFloat($('target').value),
    min_score:parseInt($('score').value),
    lot:parseFloat($('lot').value),
    max_trades:parseInt($('max').value),
    flip_signals:$('flip').checked
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
        "name": "MK Pullback Bot", "short_name": "MK", "start_url": "/", "display": "standalone",
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
    S["sl_points"] = int(d.get("sl_points", 250))
    S["tp_points"] = int(d.get("tp_points", 500))
    S["target_profit"] = float(d.get("target_profit", 0.8))
    S["min_score"] = int(d.get("min_score", 60))
    S["lot"] = float(d.get("lot", 0.01))
    S["max_trades"] = int(d.get("max_trades", 1))
    S["flip_signals"] = bool(d.get("flip_signals", False))
    S["running"] = True
    # reset setup state
    engine.impulse = None
    engine.pullback_seen = False
    engine.pullback_ext = None
    engine.prices = []
    engine.be_done = set()
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
