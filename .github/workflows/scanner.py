import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

TEST_MODE = os.getenv("TEST_MODE", "0") == "1"
TIMEFRAMES = [("4H", "4h"), ("1D", "1d"), ("1W", "1w")]
LENGTH = 5
VOL_MULT = float(os.getenv("VOL_MULT", "2.0"))
VOL_AVG_BARS = 20
MIN_QUOTE_VOL = float(os.getenv("MIN_QUOTE_VOL", "3000000"))
FRESH_MIN = float(os.getenv("FRESH_MINUTES", "100"))
LOOKBACK = int(os.getenv("LOOKBACK_BARS", "3" if TEST_MODE else "1"))
REQUIRE_OI_RISE = os.getenv("REQUIRE_OI_RISE", "0") == "1"
MAX_WORKERS = 8

SPOT = ["https://data-api.binance.vision", "https://api.binance.com"]
FAPI = ["https://fapi.binance.com"]

STABLES = {"USDC", "FDUSD", "TUSD", "USDP", "DAI", "BUSD", "USDE", "USD1", "AEUR",
           "EUR", "EURI", "PYUSD", "RLUSD", "XUSD", "BFUSD"}

S = requests.Session()
S.headers["User-Agent"] = "smc-scanner/1.0"


class Blocked(Exception):
    pass


def get(bases, path, params=None, tries=3, timeout=15):
    last = None
    blocked = False
    for base in bases:
        for _ in range(tries):
            try:
                r = S.get(base + path, params=params, timeout=timeout)
            except requests.RequestException as e:
                last = str(e)
                time.sleep(1)
                continue
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 418):
                try:
                    wait = int(r.headers.get("Retry-After", 5))
                except ValueError:
                    wait = 5
                time.sleep(min(wait, 30))
                last = str(r.status_code)
                continue
            if r.status_code in (451, 403):
                blocked = True
                last = str(r.status_code)
                break
            last = f"{r.status_code} {r.text[:80]}"
            if 400 <= r.status_code < 500:
                break
            time.sleep(1)
    if blocked:
        raise Blocked(path)
    raise RuntimeError(f"{path}: {last}")


def tg_send(text):
    token = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TELEGRAM_TOKEN") or os.getenv("BOT_TOKEN")
    chat = os.getenv("TELEGRAM_CHAT_ID") or os.getenv("CHAT_ID")
    if not token or not chat:
        print("!! Telegram secrets missing (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID)")
        print(text)
        return False
    r = S.post(f"https://api.telegram.org/bot{token}/sendMessage",
               json={"chat_id": chat, "text": text, "disable_web_page_preview": True},
               timeout=20)
    if r.status_code != 200:
        print("!! Telegram error:", r.status_code, r.text[:200])
        return False
    return True


def tg_send_chunks(blocks, header=""):
    ok = True
    msg = header
    for b in blocks:
        if len(msg) + len(b) + 2 > 3500:
            ok &= tg_send(msg)
            msg = ""
        msg += ("\n\n" if msg else "") + b
    if msg.strip():
        ok &= tg_send(msg)
    return ok


def ema(vals, n):
    if len(vals) < n:
        return None
    k = 2 / (n + 1)
    e = sum(vals[:n]) / n
    for v in vals[n:]:
        e = v * k + e * (1 - k)
    return e


def atr(h, l, c, n=14):
    if len(c) < n + 1:
        return None
    tr = [max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1])) for i in range(1, len(c))]
    a = sum(tr[:n]) / n
    for v in tr[n:]:
        a = (a * (n - 1) + v) / n
    return a


def detect(h, l, c, length=LENGTH):
    n = len(c)
    p = length // 2
    sgn = lambda x: (x > 0) - (x < 0)
    sh = [0] * n
    sl = [0] * n
    for i in range(1, n):
        sh[i] = sgn(h[i] - h[i - 1])
        sl[i] = sgn(l[i] - l[i - 1])

    up_v = up_loc = up_prev = None
    lo_v = lo_loc = lo_prev = None
    up_x = lo_x = False
    os_ = 0
    ev = []
    start = max(2 * p, length - 1)

    for i in range(n):
        bullf = bearf = False
        if i >= start:
            dh = sum(sh[i - p + 1:i + 1])
            dh_p = sum(sh[i - 2 * p + 1:i - p + 1])
            dl = sum(sl[i - p + 1:i + 1])
            dl_p = sum(sl[i - 2 * p + 1:i - p + 1])
            bullf = dh == -p and dh_p == p and h[i - p] == max(h[i - length + 1:i + 1])
            bearf = dl == p and dl_p == -p and l[i - p] == min(l[i - length + 1:i + 1])

        if bullf:
            up_v, up_loc, up_x = h[i - p], i - p, False
        if (up_v is not None and up_prev is not None and not up_x
                and c[i] > up_v and c[i - 1] <= up_prev):
            ev.append(dict(i=i, side="BUY", kind="ChoCH" if os_ == -1 else "BOS",
                           level=up_v, loc=up_loc))
            up_x = True
            os_ = 1

        if bearf:
            lo_v, lo_loc, lo_x = l[i - p], i - p, False
        if (lo_v is not None and lo_prev is not None and not lo_x
                and c[i] < lo_v and c[i - 1] >= lo_prev):
            ev.append(dict(i=i, side="SELL", kind="ChoCH" if os_ == 1 else "BOS",
                           level=lo_v, loc=lo_loc))
            lo_x = True
            os_ = -1

        up_prev, lo_prev = up_v, lo_v
    return ev


def load_klines(sym, itv, limit=500):
    rows = get(SPOT, "/api/v3/klines", {"symbol": sym, "interval": itv, "limit": limit})
    now = int(time.time() * 1000)
    rows = [r for r in rows if r[6] < now]
    return dict(
        t=[r[0] for r in rows], ct=[r[6] for r in rows],
        o=[float(r[1]) for r in rows], h=[float(r[2]) for r in rows],
        l=[float(r[3]) for r in rows], c=[float(r[4]) for r in rows],
        v=[float(r[5]) for r in rows],
    )


def universe():
    data = get(SPOT, "/api/v3/ticker/24hr")
    out = []
    for d in data:
        s = d["symbol"]
        if not s.endswith("USDT"):
            continue
        if s[:-4] in STABLES:
            continue
        if float(d["quoteVolume"]) < MIN_QUOTE_VOL:
            continue
        out.append(s)
    return sorted(out)


def tf_is_due(itv):
    if TEST_MODE:
        return True
    k = load_klines("BTCUSDT", itv, limit=3)
    if not k["ct"]:
        return False
    age_min = (time.time() * 1000 - (k["ct"][-1] + 1)) / 60000
    return 0 <= age_min <= FRESH_MIN


def scan_one(sym, label, itv):
    k = load_klines(sym, itv)
    n = len(k["c"])
    res = dict(events=0, hits=[])
    if n < 30:
        return res
    events = [e for e in detect(k["h"], k["l"], k["c"]) if e["i"] >= n - LOOKBACK]
    res["events"] = len(events)
    if not events:
        return res

    e200 = ema(k["c"], 200)
    a14 = atr(k["h"], k["l"], k["c"], 14)

    for e in events:
        i = e["i"]
        if i < VOL_AVG_BARS:
            continue
        avg = sum(k["v"][i - VOL_AVG_BARS:i]) / VOL_AVG_BARS
        if avg <= 0:
            continue
        ratio = k["v"][i] / avg
        if ratio < VOL_MULT:
            continue
        close = k["c"][i]
        ext_pct = abs(close - e["level"]) / e["level"] * 100
        ext_atr = abs(close - e["level"]) / a14 if a14 else None
        if e200 is not None:
            trend_ok = close > e200 if e["side"] == "BUY" else close < e200
            trend = ("above" if close > e200 else "below") + " EMA200"
        else:
            trend_ok, trend = None, "EMA200 n/a"
        res["hits"].append(dict(
            sym=sym, tf=label, itv=itv, side=e["side"], kind=e["kind"],
            level=e["level"], close=close, ratio=ratio, ext_pct=ext_pct, ext_atr=ext_atr,
            trend=trend, trend_ok=trend_ok, bars_ago=n - 1 - i, level_age=i - e["loc"],
        ))
    return res


_fapi_ok = True


def enrich(hit):
    global _fapi_ok
    hit["oi"] = None
    hit["funding"] = None
    if not _fapi_ok:
        return
    period, back = {"4h": ("4h", 1), "1d": ("1d", 1), "1w": ("1d", 7)}[hit["itv"]]
    try:
        rows = get(FAPI, "/futures/data/openInterestHist",
                   {"symbol": hit["sym"], "period": period, "limit": back + 2}, tries=2)
        if len(rows) > back:
            now_v = float(rows[-1]["sumOpenInterest"])
            prev = float(rows[-1 - back]["sumOpenInterest"])
            if prev > 0:
                hit["oi"] = (now_v / prev - 1) * 100
        pi = get(FAPI, "/fapi/v1/premiumIndex", {"symbol": hit["sym"]}, tries=2)
        hit["funding"] = float(pi["lastFundingRate"]) * 100
    except Blocked:
        _fapi_ok = False
        print("futures API blocked from this IP -> OI/funding skipped")
    except Exception as ex:
        print(f"  no futures data for {hit['sym']}: {ex}")


def fmt(p):
    return f"{p:.6g}"


def oi_text(hit):
    oi = hit.get("oi")
    if oi is None:
        return "OI: n/a"
    buy = hit["side"] == "BUY"
    if oi >= 0:
        note = "fresh longs" if buy else "fresh shorts"
    else:
        note = "short covering" if buy else "longs closing"
    return f"OI: {oi:+.1f}% ({note})"


def build(hit):
    icon = "🟢" if hit["side"] == "BUY" else "🔴"
    arrow = ">" if hit["side"] == "BUY" else "<"
    ext = f"{hit['ext_pct']:.1f}%"
    if hit["ext_atr"] is not None:
        ext += f" / {hit['ext_atr']:.1f} ATR"
    lines = [
        f"{icon} {hit['side']} | {hit['sym']} | {hit['tf']}",
        f"{hit['kind']}: close {fmt(hit['close'])} {arrow} fractal {fmt(hit['level'])}"
        f" (fractal {hit['level_age']} bars back)",
        f"Volume: {hit['ratio']:.1f}x avg",
        oi_text(hit) + (f" | Funding: {hit['funding']:+.3f}%" if hit.get("funding") is not None else ""),
        f"Trend: {hit['trend']} " + ("✅" if hit["trend_ok"] else "⚠️" if hit["trend_ok"] is False else ""),
        f"Extension past level: {ext}",
    ]
    if hit["bars_ago"]:
        lines.append(f"(signal candle was {hit['bars_ago']} candle(s) ago)")
    return "\n".join(lines)


def main():
    t0 = time.time()
    syms = universe()
    due = [(lb, it) for lb, it in TIMEFRAMES if tf_is_due(it)]
    print(f"{len(syms)} coins | due timeframes: {[d[0] for d in due]} | test={TEST_MODE}")
    if not due:
        print("No timeframe closed recently - nothing to scan.")
        return 0

    jobs = [(s, lb, it) for s in syms for lb, it in due]
    hits, events_total, errors = [], 0, 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = [ex.submit(scan_one, *j) for j in jobs]
        for f in as_completed(futs):
            try:
                r = f.result()
                events_total += r["events"]
                hits.extend(r["hits"])
            except Exception as e:
                errors += 1
                print("scan error:", e)

    for h in hits:
        enrich(h)
    if REQUIRE_OI_RISE:
        hits = [h for h in hits if h.get("oi") is None or h["oi"] > 0]

    tf_rank = {"1W": 0, "1D": 1, "4H": 2}
    hits.sort(key=lambda h: (tf_rank[h["tf"]], -h["ratio"]))
    print(f"structure breaks: {events_total} | with volume: {len(hits)} | errors: {errors} "
          f"| {time.time() - t0:.0f}s")
    for h in hits:
        print(build(h), "\n")

    ok = True
    if hits:
        head = f"📡 SMC scan - {len(hits)} signal(s)"
        ok = tg_send_chunks([build(h) for h in hits], header=head)
    elif TEST_MODE:
        ok = tg_send(f"✅ Test scan OK\nCoins: {len(syms)} | TF: {', '.join(d[0] for d in due)}\n"
                     f"BOS/ChoCH in last {LOOKBACK} candles: {events_total}\n"
                     f"With volume >= {VOL_MULT}x: 0\nErrors: {errors}")
    if errors and errors > len(jobs) * 0.2:
        print("too many errors")
        return 1
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
