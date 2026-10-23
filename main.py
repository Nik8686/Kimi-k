import asyncio
import json
import os
import time
import math
import random
import urllib.request
import urllib.parse
import websockets
from dataclasses import dataclass, field
from typing import List, Dict, Optional

# ================== 0) الإعدادات ==================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
SOL_USD = 110.0  # حدّثه تقريبياً (لتحويل marketCapSol إلى $)
WINDOW_SEC = 300  # نافذة التحليل: 5 دقائق
MAX_CONCURRENT = 4  # ≤ حد الاشتراكات الحية المجانية (5)
MIN_SCORE_ALERT = 45.0  # مُخفّض بعد المعايرة الواقعية

# ================== 1) تيليغرام ==================
class TelegramNotifier:
    def __init__(self, token=TELEGRAM_BOT_TOKEN, chat_id=TELEGRAM_CHAT_ID, cooldown=20):
        self.token, self.chat_id, self.cooldown, self._last = token, chat_id, cooldown, 0.0
    
    def send(self, text: str, force=False) -> bool:
        if not self.token:
            print(f"[TELEGRAM: غير مهيأ — اطبع محلياً]\n{text}\n")
            return False
        if not force and time.time() - self._last < self.cooldown:
            return False
        try:
            data = urllib.parse.urlencode({"chat_id": self.chat_id, "text": text, "parse_mode": "HTML"}).encode()
            req = urllib.request.Request(f"https://api.telegram.org/bot{self.token}/sendMessage", data=data)
            ok = urllib.request.urlopen(req, timeout=10).status == 200
            if ok:
                self._last = time.time()
            return ok
        except Exception as e:
            print(f"[TELEGRAM خطأ] {e}")
            return False

# ================== 2) الخصائص (الدقائق الخمس الأولى) ==================
@dataclass
class TokenFeatures:
    mint: str
    creator: str
    unique_wallets_5m: int = 0
    trade_count_5m: int = 0
    tx_velocity: float = 0.0
    unique_per_trade: float = 0.0
    dev_holding_pct: float = 0.0
    dev_sold_within_5m: bool = False
    bundle_detected: bool = False
    top3_wallet_share: float = 0.0
    buy_vol_sol: float = 0.0
    sell_vol_sol: float = 0.0
    buy_sell_delta: float = 0.0
    max_mcap_usd: float = 0.0

def _get(d: dict, *keys, default=None):
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return default

def compute_features(mint, creator, trades) -> TokenFeatures:
    if not trades:
        return TokenFeatures(mint, creator)
    
    wallets, buy, sell = [], {}, {}
    first_dev_sell = False
    for i, tr in enumerate(trades):
        w = _get(tr, "trader", "traderPublicKey", "maker", default="?")
        usd = float(_get(tr, "solAmount", "quoteAmount", "amountSol", default=0) or 0)
        if usd <= 0:
            raw = float(_get(tr, "solAmountRaw", "quoteAmountRaw", default=0) or 0)
            usd = raw / 1e9
        wallets.append(w)
        is_buy = str(_get(tr, "txType", "type", "side", default="buy")).lower() in ("buy", "b")
        (buy if is_buy else sell)[w] = (buy if is_buy else sell).get(w, 0) + usd
        if (not is_buy) and w == creator:
            first_dev_sell = True

    uw = len(set(wallets))
    n = len(trades)
    
    def _slot(t):
        return _get(t, "slot", "blockSlot")
    def _ts(t):
        return _get(t, "timestamp", "blockTime", "time")
    
    s0, t0s = _slot(trades[0]), _ts(trades[0])
    early_nondev = [t for t in trades[1:6] if _get(t, "txType", "type", default="").lower() in ("buy", "b") and _get(t, "trader", "traderPublicKey", "maker", default="") != creator]
    
    if s0 is not None:
        bundle = sum(1 for t in early_nondev if _slot(t) == s0) >= 2
    elif t0s is not None:
        bundle = sum(1 for t in early_nondev if _ts(t) is not None and abs(float(_ts(t)) - float(t0s)) < 1.0) >= 2
    else:
        bundle = len(early_nondev) >= 3

    bv, sv = sum(buy.values()), sum(sell.values())
    net = {w: buy.get(w, 0) - sell.get(w, 0) for w in set(wallets)}
    nets = sorted((v for v in net.values() if v > 0), reverse=True)
    total_net = sum(nets)
    dev_net = net.get(creator, 0.0)
    mcaps = [float(_get(t, "marketCapSol", "marketCap", default=0) or 0) for t in trades]

    return TokenFeatures(
        mint=mint,
        creator=creator,
        unique_wallets_5m=uw,
        trade_count_5m=n,
        tx_velocity=n / WINDOW_SEC,
        unique_per_trade=uw / n if n else 0,
        dev_holding_pct=(dev_net / total_net) if total_net > 0 else 0.0,
        dev_sold_within_5m=first_dev_sell,
        bundle_detected=bundle,
        top3_wallet_share=(sum(nets[:3]) / total_net) if total_net > 0 else 0.0,
        buy_vol_sol=bv,
        sell_vol_sol=sv,
        buy_sell_delta=(bv - sv) / (bv + sv) if (bv + sv) > 0 else 0.0,
        max_mcap_usd=(max(mcaps) * SOL_USD) if mcaps else 0.0
    )

# ================== 3) التنقيط ==================
WEIGHTS = {"unique_wallets": .24, "velocity": .10, "buy_pressure": .16, "unique_ratio": .10, "bundle": -.12, "dev_holding": -.10, "dev_sold": -.20, "concentration": -.08}
THR = {"max_dev_holding_pct": .20, "max_top3_share": .65, "min_unique_wallets": 12, "min_buy_delta": -.25}
WATCH_SCORE = 33.0

def _norm(v, lo, hi):
    return max(0.0, min(1.0, (v - lo) / (hi - lo)))

def _norm_log(v, lo, hi):
    return _norm(math.log1p(max(v, 0)), math.log1p(lo), math.log1p(hi))

def score_token(f: TokenFeatures):
    w, t = WEIGHTS, THR
    reasons = []
    if f.dev_holding_pct > t["max_dev_holding_pct"]:
        reasons.append("dev_heavy")
    if f.unique_wallets_5m >= 25 and f.top3_wallet_share > t["max_top3_share"]:
        reasons.append("concentrated")
    if f.unique_wallets_5m < t["min_unique_wallets"]:
        reasons.append("few_wallets")
    if f.buy_sell_delta < t["min_buy_delta"]:
        reasons.append("sell_pressure")

    s = (w["unique_wallets"] * _norm_log(f.unique_wallets_5m, 5, 250) +
         w["velocity"] * _norm_log(f.tx_velocity, .01, 2.0) +
         w["buy_pressure"] * _norm(f.buy_sell_delta, -1, 1) +
         w["unique_ratio"] * _norm(f.unique_per_trade, .05, 1) +
         w["bundle"] * (1 if f.bundle_detected else 0) +
         w["dev_holding"] * _norm(f.dev_holding_pct, 0, .5) +
         w["dev_sold"] * (1 if f.dev_sold_within_5m else 0) +
         w["concentration"] * _norm(f.top3_wallet_share, .2, .9))
    
    pos = sum(v for v in w.values() if v > 0)
    return max(0.0, s / pos) * 100, not reasons, reasons

def score_breakdown(f: TokenFeatures) -> str:
    return (f"w={f.unique_wallets_5m} vel={f.tx_velocity:.2f} d={f.buy_sell_delta:+.2f} "
            f"upt={f.unique_per_trade:.2f} dev={f.dev_holding_pct:.0%} "
            f"devsold={int(f.dev_sold_within_5m)} top3={f.top3_wallet_share:.0%} "
            f"bundle={int(f.bundle_detected)}")

def format_alert(f: TokenFeatures, score: float) -> str:
    return (f"🚀 <b>عملة قوية مرشحة!</b>\n\n"
            f"📌 <b>العقد:</b> <code>{f.mint}</code>\n"
            f"⭐ <b>النقاط:</b> {score:.0f}/100\n"
            f"📊 <b>المؤشرات:</b> المحافظ ({f.unique_wallets_5m}) | السرعة ({f.tx_velocity:.2f})\n"
            f"🔗 <a href='https://photon-sol.tinyastro.io/en/token/{f.mint}'>Photon</a> | "
            f"<a href='https://solscan.io/token/{f.mint}'>Solscan</a>")

# ================== 4) العميل الحي — pumpdev.io ==================
WS_URLS = ["wss://pumpdev.io/ws", "wss://pumpportal.fun/api/data"]

class PumpScanner:
    def __init__(self, notifier: TelegramNotifier):
        self.notifier, self.pending = notifier, {}
        self.tokens_seen = 0

    async def _sub(self, ws, method, keys=None):
        p = {"method": method}
        if keys:
            p["keys"] = keys
        await ws.send(json.dumps(p))

    async def _evaluate(self, mint):
        info = self.pending.pop(mint, None)
        if not info:
            return
        f = compute_features(mint, info["creator"], info["trades"])
        score, passed, reasons = score_token(f)
        line = f"{mint[:12]}… score={score:.0f} [{score_breakdown(f)}] reasons={reasons or '-'}"
        if passed and score >= MIN_SCORE_ALERT:
            self.notifier.send(format_alert(f, score), force=True)
            print(f"🚨 ALERT {line}", flush=True)
        elif passed and score >= WATCH_SCORE:
            print(f"👀 WATCH {line}", flush=True)
        else:
            print(f"⏭️ SKIP {line}", flush=True)

    async def run(self):
        delay = 5
        while True:
            for url in WS_URLS:
                try:
                    print(f"🔌 اتصال بـ {url} …")
                    async with websockets.connect(url, ping_interval=20, max_queue=1000) as ws:
                        await self._sub(ws, "subscribeNewToken")
                        print("✅ متصل. بانتظار إطلاقات جديدة…")
                        async for raw in ws:
                            try:
                                ev = json.loads(raw)
                            except Exception:
                                continue
                            if ev.get("type") in ("connected", "auth", "subscribed", "connectionStatus"):
                                continue
                            tx = _get(ev, "txType", "type", default="")
                            if tx == "create":
                                mint = _get(ev, "mint")
                                if not mint or mint in self.pending:
                                    continue
                                self.tokens_seen += 1
                                self.pending[mint] = {
                                    "t0": time.time(),
                                    "creator": _get(ev, "traderPublicKey", "trader", "creator", default=""),
                                    "trades": []
                                }
                                await self._sub(ws, "subscribeTokenTrade", [mint])
                                print(f"🆕 إطلاق #{self.tokens_seen}: {mint[:12]}…")
                            elif tx in ("buy", "sell"):
                                mint = _get(ev, "mint")
                                if mint in self.pending:
                                    self.pending[mint]["trades"].append(ev)

                            now = time.time()
                            for m, info in list(self.pending.items()):
                                if now - info["t0"] >= WINDOW_SEC and len(info["trades"]) > 0:
                                    await self._sub(ws, "unsubscribeTokenTrade", [m])
                                    await self._evaluate(m)
                            if len(self.pending) >= MAX_CONCURRENT:
                                await asyncio.sleep(0.5)
                except (websockets.exceptions.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
                    print(f"⚠️ انقطع ({e}) — إعادة اتصال بعد {delay}s")
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 60)
                    continue

# ================== 5) التشغيل الصحيح لـ Railway ==================
if __name__ == "__main__":
    notifier = TelegramNotifier()
    scanner = PumpScanner(notifier)
    asyncio.run(scanner.run())
