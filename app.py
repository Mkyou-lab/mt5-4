import os
import re
import math
import asyncio
import threading
import logging
from datetime import datetime, timezone, timedelta

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
    "positions": [],
    "tick": {"bid": 0.0, "ask": 0.0},
    "direction": "WAITING",
    "score": 0,
    "phase": "SCAN",
    "session": "UNKNOWN",
    "last_action": "—",
    "day_start_balance": 0.0,
    "day_pnl": 0.0,
    "logs": ["Auto-Pilot ready. Connect once, press RUN. No manual tuning required."]
}

def log(msg):
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    S["logs"].append(line)
    if len(S["logs"]) > 180:
        S["logs"].pop(0)
    print(line)

class AutoPilot:
    def __init__(self):
        self.api = None
        self.account = None
        self.conn = None
        self.prices = []
        self.specs = {}
        self.impulse = None
        self.impulse_price = None
        self.impulse_ts = 0.0
        self.pullback_seen = False
        self.pullback_ext = None
        self.last_entry_ts = 0.0
        self.trailed = {}

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
                "name": f"Auto-{login}",
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
        S["day_start_balance"] = S["balance"] or S["equity"]
        S["connected"] = True
        S["status"] = f"Online • {S['account_type']}"
        S["error"] = ""
        log("✅ Connected — Auto-Pilot armed")
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
        except Exception as e:
            log(f"Refresh: {e}")

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

    async def pick_symbol(self):
        """Auto pick a liquid symbol if possible."""
        preferred = ["XAUUSD", "BTCUSD", "EURUSD", "GBPUSD", "ETHUSD"]
        try:
            symbols = await self.conn.get_symbols()
            upper_map = {re.sub(r"[/._]", "", s.upper()): s for s in symbols}
            for p in preferred:
                key = p.upper()
                for k, real in upper_map.items():
                    if key in k:
                        return real
            return symbols[0] if symbols else "XAUUSD"
        except Exception:
            return S.get("symbol") or "XAUUSD"

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
                "tick_value": float(sp.get("tickValue", 0) or 0),
                "tick_size": float(sp.get("tickSize", 0) or 0),
            }
            self.specs[symbol] = spec
            log(f"Spec {symbol}: point={spec['point']} stops={spec['stops_level']} minLot={spec['min_volume']}")
            return spec
        except Exception as e:
            log(f"Spec fallback: {e}")
            spec = {"digits": 2, "point": 0.01, "min_volume": 0.01, "max_volume": 100, "volume_step": 0.01, "stops_level": 0, "tick_value": 0, "tick_size": 0}
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

    def session_now(self):
        # UTC hours rough session map
        h = datetime.now(timezone.utc).hour
        if 7 <= h < 10:
            return "LONDON_OPEN", 1.0
        if 12 <= h < 16:
            return "NY_OVERLAP", 1.1
        if 0 <= h < 6:
            return "ASIA", 0.7
        if 16 <= h < 21:
            return "NY", 0.95
        return "OFFPEAK", 0.6

    def news_caution(self):
        """
        Lightweight caution windows (UTC) around common high-impact times.
        Not a full news calendar, but reduces trading into obvious risk windows.
        """
        now = datetime.now(timezone.utc)
        # top of hour :00-:03 and :30-:33 often event prints
        if now.minute in (0, 1, 2, 30, 31, 32):
            return True, "clock-event-window"
        # Friday late / Sunday open caution
        if now.weekday() == 4 and now.hour >= 18:
            return True, "friday-late"
        if now.weekday() == 6:
            return True, "sunday"
        return False, ""

    def auto_lot(self, balance, spec, symbol):
        # Risk small and compound slowly
        risk_pct = 0.007  # 0.7% risk budget reference
        equity = max(balance, 1.0)
        # base by balance tiers
        if equity < 50:
            lot = spec["min_volume"]
        elif equity < 200:
            lot = max(spec["min_volume"], 0.01)
        elif equity < 1000:
            lot = 0.02
        elif equity < 5000:
            lot = 0.05
        else:
            lot = min(0.20, equity / 100000)  # conservative
        # metals often need smaller
        if "XAU" in symbol and lot > 0.05:
            lot = 0.05
        if "BTC" in symbol and lot > 0.05:
            lot = 0.05
        return self.normalize_lot(lot, spec)

    def atr_like(self):
        if len(self.prices) < 20:
            return None
        seg = self.prices[-20:]
        hi, lo = max(seg), min(seg)
        return max(hi - lo, 1e-8)

    def thresholds(self, symbol):
        atr = self.atr_like()
        if "XAU" in symbol:
            base = {"impulse": 0.15, "pullback": 0.06, "continue": 0.05, "spread_max": 0.70}
        elif "BTC" in symbol:
            base = {"impulse": 10.0, "pullback": 4.0, "continue": 3.0, "spread_max": 30.0}
        elif "ETH" in symbol:
            base = {"impulse": 1.8, "pullback": 0.7, "continue": 0.55, "spread_max": 2.5}
        else:
            base = {"impulse": 0.00030, "pullback": 0.00012, "continue": 0.00010, "spread_max": 0.00035}
        if atr:
            # adapt a bit to current range
            base["impulse"] = max(base["impulse"], atr * 0.25)
            base["pullback"] = max(base["pullback"], atr * 0.10)
            base["continue"] = max(base["continue"], atr * 0.08)
        return base

    def build_sl_tp(self, side, entry, spec, symbol):
        digits = spec["digits"]
        point = spec["point"] if spec["point"] > 0 else 0.01
        stops = spec["stops_level"]
        atr = self.atr_like() or (50 * point)

        # SL around 0.8–1.2 ATR, TP around 1.6–2.2 ATR
        sl_dist = max(atr * 0.9, (stops + 10) * point, 30 * point)
        tp_dist = max(atr * 1.8, sl_dist * 1.7)

        if "XAU" in symbol:
            sl_dist = max(sl_dist, 0.8)
            tp_dist = max(tp_dist, 1.6)
        if "BTC" in symbol:
            sl_dist = max(sl_dist, 40)
            tp_dist = max(tp_dist, 80)

        if side == "BUY":
            sl, tp = entry - sl_dist, entry + tp_dist
        else:
            sl, tp = entry + sl_dist, entry - tp_dist
        return round(sl, digits), round(tp, digits), sl_dist, tp_dist

    async def close(self, pid):
        try:
            await self.conn.close_position(str(pid))
            log(f"💰 Closed #{pid}")
            S["last_action"] = f"Closed #{pid}"
            self.trailed.pop(str(pid), None)
            await self.refresh()
            return True
        except Exception:
            try:
                await self.conn.close_position(int(pid))
                log(f"💰 Closed #{pid}")
                self.trailed.pop(str(pid), None)
                await self.refresh()
                return True
            except Exception as e:
                log(f"Close error: {e}")
                return False

    async def open_trade(self, side, symbol):
        try:
            spec = await self.load_spec(symbol)
            lot = self.auto_lot(S["balance"] or S["equity"], spec, symbol)
            S["lot"] = lot
            px = await self.conn.get_symbol_price(symbol)
            bid, ask = float(px["bid"]), float(px["ask"])
            spread = ask - bid
            th = self.thresholds(symbol)
            if spread > th["spread_max"]:
                log(f"⛔ Spread {spread:.5f} too wide — skip")
                return False

            entry = ask if side == "BUY" else bid
            sl, tp, sl_dist, tp_dist = self.build_sl_tp(side, entry, spec, symbol)
            log(f"🧾 AUTO {side} {symbol} lot={lot} @ {entry} | SL={sl} TP={tp} | slDist={sl_dist:.5f} tpDist={tp_dist:.5f}")

            # one retry with wider stops if needed
            for i, widen in enumerate((1.0, 1.6), start=1):
                sl2, tp2 = sl, tp
                if widen != 1.0:
                    if side == "BUY":
                        sl2 = entry - sl_dist * widen
                        tp2 = entry + tp_dist * widen
                    else:
                        sl2 = entry + sl_dist * widen
                        tp2 = entry - tp_dist * widen
                    digits = spec["digits"]
                    sl2, tp2 = round(sl2, digits), round(tp2, digits)
                    log(f"↩️ Retry {i} wider SL/TP {sl2}/{tp2}")
                try:
                    if side == "BUY":
                        res = await self.conn.create_market_buy_order(symbol, lot, sl2, tp2)
                    else:
                        res = await self.conn.create_market_sell_order(symbol, lot, sl2, tp2)
                    log(f"✅ Order accepted: {res}")
                    await asyncio.sleep(1.0)
                    await self.refresh()
                    mine = [p for p in S["positions"] if self.same_symbol(p["symbol"], symbol)]
                    if mine:
                        p = mine[-1]
                        log(f"🎉 OPENED #{p['id']} {p['type']} SL={p['sl']} TP={p['tp']}")
                        S["last_action"] = f"OPENED #{p['id']}"
                        self.last_entry_ts = datetime.now(timezone.utc).timestamp()
                        self.impulse = None
                        self.pullback_seen = False
                        self.pullback_ext = None
                        S["phase"] = "IN_TRADE"
                        return True
                except Exception as e:
                    log(f"⚠️ Open reject: {e}")
                    if "stop" in str(e).lower() or "invalid" in str(e).lower():
                        continue
                    break
            S["last_action"] = "OPEN FAIL"
            return False
        except Exception as e:
            log(f"❌ open crash: {e}")
            return False

    def setup_signal(self, mid, symbol):
        th = self.thresholds(symbol)
        now = datetime.now(timezone.utc).timestamp()
        if len(self.prices) < 12:
            S["phase"] = "WARMUP"
            return "WAITING", 0, "WAIT", "warmup"

        mom3 = self.prices[-1] - self.prices[-4]
        mom6 = self.prices[-1] - self.prices[-7]
        trend = self.prices[-1] - self.prices[-12]

        # expire impulse
        if self.impulse and now - self.impulse_ts > 50:
            self.impulse = None
            self.pullback_seen = False
            self.pullback_ext = None
            S["phase"] = "SCAN"

        if self.impulse is None:
            S["phase"] = "SCAN"
            # require trend alignment + impulse
            if mom3 > th["impulse"] and mom6 > th["impulse"] * 0.6 and trend > 0:
                self.impulse, self.impulse_price, self.impulse_ts = "UP", mid, now
                self.pullback_seen, self.pullback_ext = False, mid
                log(f"📡 Trend+Impulse UP @ {mid:.5f} — wait pullback")
                return "WAITING", 45, "IMPULSE UP", "impulseUP"
            if mom3 < -th["impulse"] and mom6 < -th["impulse"] * 0.6 and trend < 0:
                self.impulse, self.impulse_price, self.impulse_ts = "DOWN", mid, now
                self.pullback_seen, self.pullback_ext = False, mid
                log(f"📡 Trend+Impulse DOWN @ {mid:.5f} — wait pullback")
                return "WAITING", 45, "IMPULSE DOWN", "impulseDOWN"
            return "WAITING", 15, "FLAT", "no-setup"

        if self.impulse == "UP":
            if mid < self.impulse_price - th["pullback"]:
                self.pullback_seen = True
                self.pullback_ext = min(self.pullback_ext or mid, mid)
                S["phase"] = "PULLBACK"
            if self.pullback_seen:
                rebound = mid - (self.pullback_ext or mid)
                if rebound >= th["continue"] and (mid - self.impulse_price) < th["impulse"] * 1.7:
                    score = 72
                    if mid > self.prices[-2] > self.prices[-3]:
                        score += 12
                    if trend > 0:
                        score += 8
                    S["phase"] = "CONTINUE"
                    return "BUY", min(100, score), "UP", "pb-continue-UP"
                return "WAITING", 58, "PULLBACK UP", "wait-UP"
            return "WAITING", 48, "IMPULSE UP", "wait-pb-UP"

        if self.impulse == "DOWN":
            if mid > self.impulse_price + th["pullback"]:
                self.pullback_seen = True
                self.pullback_ext = max(self.pullback_ext or mid, mid)
                S["phase"] = "PULLBACK"
            if self.pullback_seen:
                drop = (self.pullback_ext or mid) - mid
                if drop >= th["continue"] and (self.impulse_price - mid) < th["impulse"] * 1.7:
                    score = 72
                    if mid < self.prices[-2] < self.prices[-3]:
                        score += 12
                    if trend < 0:
                        score += 8
                    S["phase"] = "CONTINUE"
                    return "SELL", min(100, score), "DOWN", "pb-continue-DOWN"
                return "WAITING", 58, "PULLBACK DOWN", "wait-DOWN"
            return "WAITING", 48, "IMPULSE DOWN", "wait-pb-DOWN"

        return "WAITING", 0, "FLAT", "idle"

    async def manage_positions(self, symbol, side_now):
        for p in list(S["positions"]):
            if not self.same_symbol(p["symbol"], symbol):
                continue
            pid = str(p["id"])
            pr = float(p["profit"])
            open_px = float(p["open"])
            cur = float(p["current"])
            side = p["type"]

            # bank solid green quickly on scalp-ish profits relative to balance
            bal = max(S["balance"], 1)
            soft_tp = max(0.40, bal * 0.004)   # ~0.4% equity soft bank
            hard_tp = max(0.80, bal * 0.008)

            if pr >= hard_tp:
                log(f"🎯 Hard bank ${pr:.2f} → close #{pid}")
                await self.close(pid)
                continue

            # reverse protect when green
            if pr > 0 and ((side == "BUY" and side_now == "SELL") or (side == "SELL" and side_now == "BUY")):
                log(f"🔒 Reverse-in-profit ${pr:.2f} → close #{pid}")
                await self.close(pid)
                continue

            # trailing / BE via modify if possible
            try:
                if hasattr(self.conn, "modify_position") and p["tp"]:
                    spec = await self.load_spec(symbol)
                    point = spec["point"]
                    trail = self.trailed.get(pid, {"be": False, "trail": None})

                    # break-even first
                    if pr >= soft_tp * 0.6 and not trail.get("be"):
                        if side == "BUY":
                            new_sl = open_px + 3 * point
                        else:
                            new_sl = open_px - 3 * point
                        await self.conn.modify_position(pid, round(new_sl, spec["digits"]), p["tp"])
                        trail["be"] = True
                        self.trailed[pid] = trail
                        log(f"📌 BE SL set on #{pid}")

                    # trail further as profit grows
                    if pr >= soft_tp:
                        if side == "BUY":
                            new_sl = max(p["sl"] or 0, cur - max(self.atr_like() or 10*point, 8*point) * 0.6)
                            if new_sl > (p["sl"] or 0) and new_sl < cur:
                                await self.conn.modify_position(pid, round(new_sl, spec["digits"]), p["tp"])
                                log(f"📈 Trail SL up #{pid} → {new_sl}")
                        else:
                            base = p["sl"] if p["sl"] else 1e18
                            new_sl = min(base, cur + max(self.atr_like() or 10*point, 8*point) * 0.6)
                            if (p["sl"] == 0 or new_sl < p["sl"]) and new_sl > cur:
                                await self.conn.modify_position(pid, round(new_sl, spec["digits"]), p["tp"])
                                log(f"📉 Trail SL down #{pid} → {new_sl}")
            except Exception as e:
                log(f"Trail note: {e}")

    async def run(self):
        log("🔥 AUTO-PILOT STARTED — self-managed entries, SL/TP, trailing, session filters")
        # auto symbol if default
        symbol = await self.pick_symbol()
        S["symbol"] = symbol
        S["real_symbol"] = await self.resolve(symbol)
        symbol = S["real_symbol"]
        log(f"Auto symbol selected: {symbol}")
        await self.load_spec(symbol)
        try:
            await self.conn.subscribe_to_market_data(symbol)
        except Exception:
            pass

        while S["running"] and S["connected"]:
            try:
                # daily loss pause ~4%
                if S["day_start_balance"] > 0 and S["day_pnl"] <= -(S["day_start_balance"] * 0.04):
                    log("🛑 Daily loss limit reached — pausing new entries")
                    await self.refresh()
                    await asyncio.sleep(5)
                    continue

                session, sess_mult = self.session_now()
                S["session"] = session
                caution, why = self.news_caution()

                px = await self.conn.get_symbol_price(symbol)
                bid, ask = float(px["bid"]), float(px["ask"])
                mid = (bid + ask) / 2.0
                S["tick"] = {"bid": bid, "ask": ask}
                self.prices.append(mid)
                if len(self.prices) > 250:
                    self.prices.pop(0)

                side, score, label, reason = self.setup_signal(mid, symbol)
                # session confidence adjust
                score = int(max(0, min(100, score * sess_mult)))
                S["score"] = score
                S["direction"] = label

                await self.refresh()
                mine = [p for p in S["positions"] if self.same_symbol(p["symbol"], symbol)]

                log(
                    f"📊 {symbol} {bid:.2f}/{ask:.2f} session={session} phase={S['phase']} "
                    f"score={score}% dir={label} reason={reason} open={len(mine)} dayPnL={S['day_pnl']}"
                )

                await self.manage_positions(symbol, side)

                now = datetime.now(timezone.utc).timestamp()
                allow = True
                if caution:
                    allow = False
                    log(f"⏸️ Caution window ({why}) — no new entries")
                if session == "OFFPEAK" and score < 85:
                    allow = False
                if len(mine) >= 1:
                    allow = False
                    p = mine[0]
                    log(f"⏳ Managing #{p['id']} P/L=${p['profit']:.2f} SL={p['sl']} TP={p['tp']}")

                if allow and side in ("BUY", "SELL") and score >= 70 and now - self.last_entry_ts > 15:
                    log(f"✅ AUTO ENTRY {side} score={score}%")
                    await self.open_trade(side, symbol)
                elif not mine:
                    log("… scanning for pullback-continuation setup")

            except Exception as e:
                log(f"Loop error: {e}")
            await asyncio.sleep(0.9)

        log("Auto-Pilot stopped")

engine = AutoPilot()

HTML = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1">
<meta name="theme-color" content="#070b12">
<meta name="apple-mobile-web-app-capable" content="yes">
<link rel="manifest" href="/manifest.json">
<title>MK Auto-Pilot</title>
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
.log{height:240px;overflow:auto;background:#05080f;border:1px solid #243247;border-radius:12px;padding:10px;font:11px monospace;color:#3fb950}
#fab{position:fixed;right:14px;bottom:18px;width:56px;height:56px;border-radius:50%;background:#238636;color:#fff;display:flex;align-items:center;justify-content:center;font-weight:900;font-size:22px}
.hint{font-size:11px;color:#8b949e}
</style>
</head>
<body>
<div class="wrap">
  <div class="d-flex justify-content-between align-items-center">
    <div class="brand">🤖 MK Auto-Pilot</div>
    <div id="st" class="pill off">● Offline</div>
  </div>
  <div class="grid">
    <div class="m"><small>Bid/Ask</small><b id="px">0/0</b></div>
    <div class="m"><small>Balance</small><b id="bal">$0</b></div>
    <div class="m"><small>Day P/L</small><b id="pl">$0</b></div>
    <div class="m"><small>Phase/Score</small><b id="dir">SCAN 0%</b></div>
  </div>
  <div class="tabs">
    <button class="tab active" data-t="live">Live</button>
    <button class="tab" data-t="conn">Connect</button>
  </div>

  <div id="panel-live">
    <div class="card">
      <div class="d-flex justify-content-between align-items-center mb-2">
        <div>
          <b style="color:#fff">Fully Automatic</b>
          <div class="hint" id="meta">Session: — | Lot: —</div>
          <div class="hint" id="last">Last: —</div>
        </div>
        <div style="display:flex;gap:8px;width:170px">
          <button id="btnStart" class="btn-go" style="padding:10px" disabled>RUN</button>
          <button id="btnStop" class="btn-stop" style="padding:10px" disabled>STOP</button>
        </div>
      </div>
      <table class="table table-dark table-sm mb-0" style="font-size:11px">
        <thead><tr><th>Sym</th><th>Side</th><th>P/L</th><th>SL</th><th>TP</th><th></th></tr></thead>
        <tbody id="pos"><tr><td colspan="6" class="text-center text-muted">No trades</td></tr></tbody>
      </table>
      <p class="hint mt-2 mb-0">No bot can guarantee no losses. This auto-pilot uses trend+pullback entries, auto lot, SL/TP, trailing, session/news caution, and daily loss pause.</p>
    </div>
    <div class="card"><div style="color:#fff;font-weight:800;margin-bottom:6px">Live Log</div><div id="log" class="log"></div></div>
  </div>

  <div id="panel-conn" style="display:none">
    <div class="card">
      <label>MetaApi Token</label>
      <input id="token" type="password" class="form-control mb-2">
      <label>Method</label>
      <select id="method" class="form-select mb-2">
        <option value="id">Account ID (best)</option>
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
  $('pl').textContent=(d.day_pnl>=0?'+':'')+d.day_pnl.toFixed(2);
  $('pl').style.color=d.day_pnl>=0?'#3fb950':'#f85149';
  $('dir').textContent=(d.phase||'SCAN')+' '+(d.score||0)+'%';
  $('meta').textContent=`Session: ${d.session||'—'} | Lot: ${d.lot||'—'} | Symbol: ${d.real_symbol||d.symbol||'—'}`;
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
async function startBot(){ await fetch('/api/start',{method:'POST'}); }
async function stopBot(){ await fetch('/api/stop',{method:'POST'}); }
async function closePos(id){ if(confirm('Close?')) await fetch('/api/close',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id})}); }
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
        "name": "MK Auto-Pilot", "short_name": "MK Auto", "start_url": "/", "display": "standalone",
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
    S["running"] = True
    if not S.get("day_start_balance"):
        S["day_start_balance"] = S["balance"] or S["equity"]
    engine.impulse = None
    engine.pullback_seen = False
    engine.pullback_ext = None
    engine.prices = []
    engine.trailed = {}
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
