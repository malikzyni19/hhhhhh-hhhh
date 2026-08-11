#!/usr/bin/env python3
"""Does aggregated CVD lead price? A conditional forward-return study.

WHY THIS EXISTS
---------------
The observation we are testing: before a rally, CVD rises hard while the
candles barely move — large buying the sellers cannot absorb. Before a dump,
the mirror. The competing reading is classic absorption: flow that moves a lot
while price does NOT move means the flow is being *absorbed*, and price should
go the OTHER way.

Both readings describe the same picture. They make opposite predictions. This
script settles it with history instead of argument, and answers two questions:

  Q1 — WHICH STREAM?  spot delta, perp delta, or spot+perp combined.
  Q2 — WHICH DIRECTION?  does price FOLLOW the flow, or FADE it.

It does NOT touch the scanner, the database, or any site code. It only reads
Binance klines and prints a table.

WHAT IT MEASURES
----------------
Per bar i, over a window W:
    flow(i)   = sum of per-bar delta over bars [i-W+1 .. i]
    move(i)   = log return of close over the same bars
Both are turned into z-scores against a trailing sample of the SAME symbol, so
a 1000-dollar coin and a 0.001-dollar coin are comparable.

Bars are then bucketed:
    flow bucket   UP    flow_z >= +A
                  DOWN  flow_z <= -A
    price bucket  QUIET |move_z| <= B        <- the user's setup
                  ALIGNED move_z beyond +B in the flow's direction
                  OPPOSED move_z beyond  B against the flow

For each bucket we report the forward return over H bars, and the key number:

    SIGNED EDGE = mean(forward return x sign(flow)) - baseline drift

    positive  -> price FOLLOWS the flow   (the "flow leads price" reading)
    negative  -> price FADES the flow     (the "absorption" reading)
    ~zero     -> no usable information in that bucket

The baseline is each symbol's own unconditional mean forward return over the
same period, subtracted per symbol before pooling. Without that, a bull sample
makes every long-side bucket look predictive.

HONEST LIMITS (read before trusting a number)
---------------------------------------------
* Overlapping windows. Consecutive bars share history, so the pooled t-stat is
  optimistic. Use --stride to sample non-overlapping bars, and weight the
  PER-SYMBOL CONSISTENCY line more heavily than the t-stat: an edge that shows
  up in 8 of 10 symbols is worth more than a big t on pooled overlapping bars.
* Survivorship. The symbol list is today's top-volume pairs. Coins that died
  are absent. This inflates long-side results. It does NOT bias the
  FOLLOW-vs-FADE verdict, which is what we actually need.
* Spot and perp series are aligned by bar open time and intersected, so a
  symbol with no spot listing is silently dropped from the spot/global streams.
* Delta from taker_buy_base is the exchange's own aggressor tagging, the same
  quantity the site's backtest already uses in _bt_fl_cvd_series(). It is
  Binance-only here. Multi-venue aggregation is the next step, and only worth
  building if this study says the signal is real on one venue first.
* No fees, no slippage, no execution model. This measures information, not a
  strategy.

USAGE
-----
    python3 studies/cvd_flow_study.py                       # defaults, 4h
    python3 studies/cvd_flow_study.py --tf 1h --symbols 40
    python3 studies/cvd_flow_study.py --tf 15m --bars 1500 --window 4
    python3 studies/cvd_flow_study.py --in-base             # only bars inside
                                                            # an accumulation base
    python3 studies/cvd_flow_study.py --json out.json

Zero dependencies — stdlib only, so it runs anywhere python3 does.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Dict, List, Optional, Sequence, Tuple

# Same hosts main.py uses.
FAPI = "https://fapi.binance.com"
SAPI = "https://api.binance.com"
SAPI_MIRROR = "https://data-api.binance.vision"   # geo-safe spot mirror

USER_AGENT = "cvd-flow-study/1.0"


# ───────────────────────────── HTTP ─────────────────────────────

def _get_json(url: str, params: Dict, timeout: int = 20, retries: int = 4):
    """GET with backoff. Returns parsed JSON or None (never raises)."""
    qs = urllib.parse.urlencode(params)
    full = f"{url}?{qs}" if qs else url
    delay = 1.0
    for attempt in range(retries):
        try:
            rq = urllib.request.Request(full, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(rq, timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            # 400 = bad symbol/interval. Retrying will not fix it.
            if e.code in (400, 404):
                return None
            if attempt == retries - 1:
                return None
        except Exception:
            if attempt == retries - 1:
                return None
        time.sleep(delay)
        delay *= 2
    return None


def _parse_klines(raw) -> List[Dict]:
    """Binance kline rows -> dicts, keeping taker_buy_base (field 9).

    Field 9 is what makes CVD reconstructable at all: it is the taker BUY
    base volume, so sell volume = volume - taker_buy_base and the per-bar
    delta is 2*taker_buy_base - volume. Identical to _bt_fl_cvd_series().
    """
    out = []
    for k in raw or []:
        try:
            out.append({
                "t": int(k[0]),
                "o": float(k[1]),
                "h": float(k[2]),
                "l": float(k[3]),
                "c": float(k[4]),
                "v": float(k[5]),
                "tb": float(k[9]),
            })
        except (IndexError, TypeError, ValueError):
            continue
    return out


def fetch_klines(symbol: str, interval: str, want: int, market: str) -> List[Dict]:
    """Latest `want` closed klines, paginating backward by endTime.

    market='perp' -> fapi (cap 1500/req), market='spot' -> spot api (cap 1000).
    Unlike main.py's get_klines(), this does NOT silently fall back from
    futures to spot: the whole point of the study is to keep the two streams
    separate, and a silent fallback would quietly compare spot against itself.
    """
    if market == "perp":
        host, path, cap = FAPI, "/fapi/v1/klines", 1500
    else:
        host, path, cap = SAPI, "/api/v3/klines", 1000

    rows: List[Dict] = []
    end_ms: Optional[int] = None
    tried_mirror = False

    while len(rows) < want:
        params = {"symbol": symbol, "interval": interval,
                  "limit": min(cap, want - len(rows))}
        if end_ms is not None:
            params["endTime"] = end_ms
        data = _get_json(host + path, params)
        if data is None and market == "spot" and not tried_mirror:
            tried_mirror = True
            host = SAPI_MIRROR          # geo-blocked region: use the mirror
            continue
        batch = _parse_klines(data)
        if not batch:
            break
        rows = batch + rows
        end_ms = batch[0]["t"] - 1
        if len(batch) < params["limit"]:
            break                        # ran out of history
        time.sleep(0.12)                 # be polite to the weight limiter

    # Deduplicate and sort; drop the still-forming last bar.
    seen = {}
    for r in rows:
        seen[r["t"]] = r
    rows = [seen[t] for t in sorted(seen)]
    return rows[:-1] if rows else rows


def top_symbols(market: str, n: int, quote: str = "USDT") -> List[str]:
    """Top-N USDT pairs by 24h quote volume."""
    if market == "perp":
        data = _get_json(FAPI + "/fapi/v1/ticker/24hr", {})
    else:
        data = _get_json(SAPI + "/api/v3/ticker/24hr", {}) \
            or _get_json(SAPI_MIRROR + "/api/v3/ticker/24hr", {})
    if not data:
        return []
    rows = []
    for it in data:
        sym = str(it.get("symbol", ""))
        if not sym.endswith(quote):
            continue
        # Leveraged tokens and non-perp contracts carry synthetic flow.
        if any(x in sym for x in ("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT")):
            continue
        if "_" in sym:                   # dated futures, not perps
            continue
        try:
            qv = float(it.get("quoteVolume") or 0.0)
        except (TypeError, ValueError):
            continue
        rows.append((qv, sym))
    rows.sort(reverse=True)
    return [s for _, s in rows[:n]]


# ─────────────────────── series construction ───────────────────────

def bar_deltas(rows: Sequence[Dict]) -> List[float]:
    """Per-bar delta = 2*taker_buy_base - volume. Mirrors _bt_fl_bar_delta()."""
    return [2.0 * r["tb"] - r["v"] for r in rows]


def align(spot: Sequence[Dict], perp: Sequence[Dict]) -> Tuple[List[Dict], List[Dict]]:
    """Intersect two kline series on bar open time.

    Both must describe the SAME bars or spot+perp aggregation is nonsense —
    adding a 12:00 spot delta to a 12:15 perp delta invents flow that never
    happened.
    """
    ps = {r["t"]: r for r in perp}
    a, b = [], []
    for r in spot:
        m = ps.get(r["t"])
        if m is not None:
            a.append(r)
            b.append(m)
    return a, b


def rolling_z(vals: Sequence[float], lookback: int) -> List[Optional[float]]:
    """Causal z-score: bar i scaled by mean/stdev of bars [i-lookback .. i-1].

    Strictly trailing — bar i's own value never enters its own scaling, so
    there is no look-ahead and no self-normalisation artefact.
    """
    out: List[Optional[float]] = [None] * len(vals)
    for i in range(len(vals)):
        if i < lookback:
            continue
        win = vals[i - lookback:i]
        sd = statistics.pstdev(win)
        if sd <= 1e-12:
            continue
        out[i] = (vals[i] - statistics.fmean(win)) / sd
    return out


def window_sums(vals: Sequence[float], w: int) -> List[Optional[float]]:
    """Trailing sum over w bars ending at i."""
    out: List[Optional[float]] = [None] * len(vals)
    run = 0.0
    for i, x in enumerate(vals):
        run += x
        if i >= w:
            run -= vals[i - w]
        if i >= w - 1:
            out[i] = run
    return out


def window_logret(closes: Sequence[float], w: int) -> List[Optional[float]]:
    """Trailing log return over w bars ending at i."""
    out: List[Optional[float]] = [None] * len(closes)
    for i in range(len(closes)):
        if i < w:
            continue
        a, b = closes[i - w], closes[i]
        if a > 0 and b > 0:
            out[i] = math.log(b / a)
    return out


# ───────────────────── accumulation-base filter ─────────────────────
# Ported from main.py's _base_* helpers so "inside a base" means the same
# thing here as it does in the scanner. numpy is replaced with stdlib maths;
# the arithmetic is unchanged.

def _percentile(sorted_vals: Sequence[float], pct: float) -> float:
    """numpy.percentile with linear interpolation (its default method)."""
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * (pct / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return float(sorted_vals[int(k)])
    return float(sorted_vals[f]) * (c - k) + float(sorted_vals[c]) * (k - f)


def _drift_pct(closes: Sequence[float]) -> Optional[float]:
    """Linear-regression drift across the window as % of mean price."""
    n = len(closes)
    if n < 10:
        return None
    mean_p = statistics.fmean(closes)
    if mean_p <= 0:
        return None
    xbar = (n - 1) / 2.0
    sxx = sum((i - xbar) ** 2 for i in range(n))
    if sxx <= 0:
        return None
    sxy = sum((i - xbar) * (closes[i] - mean_p) for i in range(n))
    slope = sxy / sxx
    return (slope * n / mean_p) * 100.0


def _mid_crossings(closes: Sequence[float], mid: float) -> int:
    crossings, prev = 0, None
    for cl in closes:
        side = 1 if cl > mid else (-1 if cl < mid else 0)
        if side == 0:
            continue
        if prev is not None and side != prev:
            crossings += 1
        prev = side
    return crossings


def base_mask(rows: Sequence[Dict], window: int, max_drift: float) -> List[bool]:
    """True on every bar that sits inside a qualifying accumulation base.

    A bar qualifies when the `window` bars ending at it form a two-sided range:
    flat enough (drift within max_drift) and genuinely oscillating (>=3 mid
    crossings, both edges touched). Same gates as _base_evaluate(); the score
    and the drawdown requirement are dropped because here we only need a
    yes/no context flag, not a ranking.
    """
    n = len(rows)
    mask = [False] * n
    closes = [r["c"] for r in rows]
    highs = [r["h"] for r in rows]
    lows = [r["l"] for r in rows]
    for i in range(window - 1, n):
        seg = closes[i - window + 1:i + 1]
        srt = sorted(seg)
        lo, hi = _percentile(srt, 10.0), _percentile(srt, 90.0)
        if hi - lo <= 1e-10:
            continue
        mid = (hi + lo) / 2.0
        d = _drift_pct(seg)
        if d is None or abs(d) > max_drift:
            continue
        if _mid_crossings(seg, mid) < 3:
            continue
        if not any(x >= hi for x in highs[i - window + 1:i + 1]):
            continue
        if not any(x <= lo for x in lows[i - window + 1:i + 1]):
            continue
        mask[i] = True
    return mask


# ───────────────────────────── statistics ─────────────────────────────

def _tstat(xs: Sequence[float]) -> Optional[float]:
    if len(xs) < 3:
        return None
    sd = statistics.stdev(xs)
    if sd <= 1e-12:
        return None
    return statistics.fmean(xs) / (sd / math.sqrt(len(xs)))


class Cell:
    """Accumulator for one (stream, flow bucket, price bucket, horizon)."""

    def __init__(self):
        self.signed: List[float] = []          # fwd return * sign(flow), excess
        self.raw: List[float] = []             # fwd return, raw
        self.per_symbol: Dict[str, List[float]] = {}

    def add(self, symbol: str, signed_excess: float, raw: float):
        self.signed.append(signed_excess)
        self.raw.append(raw)
        self.per_symbol.setdefault(symbol, []).append(signed_excess)

    def summary(self, min_per_symbol: int = 5) -> Dict:
        n = len(self.signed)
        if n == 0:
            return {"n": 0}
        wins = sum(1 for x in self.signed if x > 0)
        syms = [s for s, v in self.per_symbol.items() if len(v) >= min_per_symbol]
        pos = sum(1 for s in syms if statistics.fmean(self.per_symbol[s]) > 0)
        return {
            "n": n,
            "mean": statistics.fmean(self.signed),
            "median": statistics.median(self.signed),
            "win": 100.0 * wins / n,
            "t": _tstat(self.signed),
            "raw_mean": statistics.fmean(self.raw),
            "symbols": len(syms),
            "symbols_positive": pos,
        }


# ───────────────────────────── the study ─────────────────────────────

STREAMS = ("spot", "perp", "global")
FLOW_BUCKETS = ("UP", "DOWN")
PRICE_BUCKETS = ("QUIET", "ALIGNED", "OPPOSED")


def run(args) -> Dict:
    symbols = args.symbol_list or top_symbols("perp", args.symbols)
    if not symbols:
        print("Could not fetch a symbol list. Check network access to Binance.",
              file=sys.stderr)
        return {}

    horizons = args.horizons
    # cells[stream][flow][price][H]
    cells: Dict = {
        s: {f: {p: {h: Cell() for h in horizons} for p in PRICE_BUCKETS}
            for f in FLOW_BUCKETS}
        for s in STREAMS
    }
    baseline: Dict[int, Cell] = {h: Cell() for h in horizons}

    used, skipped = [], []
    for idx, sym in enumerate(symbols, 1):
        print(f"[{idx}/{len(symbols)}] {sym} ...", end=" ", flush=True)
        perp = fetch_klines(sym, args.tf, args.bars, "perp")
        spot = fetch_klines(sym, args.tf, args.bars, "spot")
        if len(perp) < args.lookback + args.window + max(horizons) + 50:
            print("skip (perp history too short)")
            skipped.append((sym, "perp history"))
            continue
        if len(spot) < args.lookback + args.window + max(horizons) + 50:
            print("skip (no/short spot listing)")
            skipped.append((sym, "spot history"))
            continue
        srows, prows = align(spot, perp)
        if len(srows) < args.lookback + args.window + max(horizons) + 50:
            print("skip (too little overlap)")
            skipped.append((sym, "overlap"))
            continue

        _accumulate(sym, srows, prows, args, cells, baseline)
        used.append(sym)
        print(f"ok ({len(srows)} bars)")

    return {
        "params": {
            "tf": args.tf, "bars": args.bars, "window": args.window,
            "lookback": args.lookback, "flow_z": args.flow_z,
            "quiet_z": args.quiet_z, "stride": args.stride,
            "horizons": horizons, "in_base": args.in_base,
            "base_window": args.base_window, "base_drift": args.base_drift,
        },
        "symbols_used": used,
        "symbols_skipped": skipped,
        "cells": cells,
        "baseline": baseline,
        "horizons": horizons,
    }


def _accumulate(sym, srows, prows, args, cells, baseline):
    """Bucket every eligible bar of one symbol into the cell accumulators."""
    closes = [r["c"] for r in prows]        # perp price is the traded price
    n = len(closes)

    sd = bar_deltas(srows)
    pd_ = bar_deltas(prows)
    gd = [a + b for a, b in zip(sd, pd_)]   # base units, same coin: additive

    flows = {
        "spot": window_sums(sd, args.window),
        "perp": window_sums(pd_, args.window),
        "global": window_sums(gd, args.window),
    }
    flow_z = {k: rolling_z([0.0 if v is None else v for v in vs], args.lookback)
              for k, vs in flows.items()}
    move = window_logret(closes, args.window)
    move_z = rolling_z([0.0 if v is None else v for v in move], args.lookback)

    inbase = base_mask(prows, args.base_window, args.base_drift) if args.in_base \
        else [True] * n

    hmax = max(args.horizons)
    start = args.lookback + args.window + 1

    # Unconditional forward return per symbol, per horizon — the drift we
    # subtract so a bull sample cannot masquerade as predictive flow.
    drift: Dict[int, float] = {}
    for h in args.horizons:
        rets = [(closes[i + h] / closes[i] - 1.0) * 100.0
                for i in range(start, n - hmax) if closes[i] > 0]
        drift[h] = statistics.fmean(rets) if rets else 0.0
        for r in rets:
            baseline[h].add(sym, r - drift[h], r)

    for i in range(start, n - hmax, args.stride):
        if not inbase[i]:
            continue
        mz = move_z[i]
        if mz is None:
            continue
        for stream in STREAMS:
            fz = flow_z[stream][i]
            if fz is None:
                continue
            if fz >= args.flow_z:
                fb, sign = "UP", 1.0
            elif fz <= -args.flow_z:
                fb, sign = "DOWN", -1.0
            else:
                continue
            if abs(mz) <= args.quiet_z:
                pb = "QUIET"
            elif (mz > 0) == (sign > 0):
                pb = "ALIGNED"
            else:
                pb = "OPPOSED"
            for h in args.horizons:
                if closes[i] <= 0:
                    continue
                raw = (closes[i + h] / closes[i] - 1.0) * 100.0
                cells[stream][fb][pb][h].add(sym, sign * (raw - drift[h]), raw)


# ───────────────────────────── reporting ─────────────────────────────

def _fmt(v, nd=3, width=8):
    if v is None:
        return "—".rjust(width)
    return f"{v:.{nd}f}".rjust(width)


def report(res: Dict, args) -> None:
    if not res:
        return
    horizons = res["horizons"]
    cells = res["cells"]
    baseline = res["baseline"]

    print()
    print("=" * 78)
    print("  CVD FLOW STUDY — does price follow the flow, or fade it?")
    print("=" * 78)
    p = res["params"]
    print(f"  timeframe {p['tf']}   bars/symbol {p['bars']}   flow window {p['window']} bars")
    print(f"  z lookback {p['lookback']}   flow threshold |z|>={p['flow_z']}   quiet |z|<={p['quiet_z']}")
    print(f"  sampling stride {p['stride']}   horizons {horizons}"
          + (f"   [inside accumulation bases only, window {p['base_window']}]"
             if p["in_base"] else ""))
    print(f"  symbols used {len(res['symbols_used'])}"
          f"   skipped {len(res['symbols_skipped'])}")
    print()
    print("  SIGNED EDGE = mean(forward return x sign(flow)) minus that symbol's")
    print("  own drift.  POSITIVE = price FOLLOWS the flow.  NEGATIVE = price FADES it.")
    print("  'syms' = how many symbols show a positive edge, out of those with enough")
    print("  events. That ratio is the honest robustness check, not the t-stat.")
    print()

    for h in horizons:
        b = baseline[h].summary()
        print("-" * 78)
        print(f"  HORIZON: {h} bars forward"
              f"   (unconditional mean move {b.get('raw_mean', 0.0):+.3f}%)")
        print("-" * 78)
        print(f"  {'stream':<7} {'flow':<5} {'price':<8} {'N':>7} "
              f"{'edge %':>8} {'med %':>8} {'win %':>7} {'t':>7} {'syms':>9}")
        for stream in STREAMS:
            for fb in FLOW_BUCKETS:
                for pb in PRICE_BUCKETS:
                    s = cells[stream][fb][pb][h].summary(args.min_events_per_symbol)
                    if s["n"] == 0:
                        continue
                    syms = f"{s['symbols_positive']}/{s['symbols']}"
                    print(f"  {stream:<7} {fb:<5} {pb:<8} {s['n']:>7} "
                          f"{_fmt(s['mean'])} {_fmt(s['median'])} "
                          f"{s['win']:>6.1f} {_fmt(s['t'], 2, 7)} {syms:>9}")
            print()

    _verdict(res, args)


def _verdict(res: Dict, args) -> None:
    """Plain-language answer to Q1 and Q2, from the QUIET bucket only."""
    horizons = res["horizons"]
    cells = res["cells"]
    print("=" * 78)
    print("  VERDICT")
    print("=" * 78)

    # Ranked by |t|, NOT by |edge|. Forward returns compound, so the longest
    # horizon always has the largest raw edge and picking by magnitude would
    # mechanically crown it regardless of whether the signal got any cleaner.
    best = None
    for stream in STREAMS:
        line = []
        agree, total = 0, 0
        for h in horizons:
            up = cells[stream]["UP"]["QUIET"][h].summary(args.min_events_per_symbol)
            dn = cells[stream]["DOWN"]["QUIET"][h].summary(args.min_events_per_symbol)
            merged = []
            merged += cells[stream]["UP"]["QUIET"][h].signed
            merged += cells[stream]["DOWN"]["QUIET"][h].signed
            if not merged:
                line.append(f"h{h}: —")
                continue
            m = statistics.fmean(merged)
            t = _tstat(merged)
            sp = (up.get("symbols_positive", 0) + dn.get("symbols_positive", 0))
            st = (up.get("symbols", 0) + dn.get("symbols", 0))
            agree += sp
            total += st
            line.append(f"h{h}: {m:+.3f}% (t={t:.1f})" if t is not None
                        else f"h{h}: {m:+.3f}%")
            # A handful of events can produce a huge t by luck; require a
            # sample worth reading before a cell can be called the winner.
            if t is not None and len(merged) >= args.min_cell_events:
                if best is None or abs(t) > abs(best[3]):
                    best = (stream, m, h, t)
        share = (100.0 * agree / total) if total else 0.0
        print(f"  {stream:<7} quiet-price flow  " + "   ".join(line))
        print(f"  {'':<7} symbol agreement {share:.0f}%")
    print()

    if best is None:
        print(f"  No cell reached {args.min_cell_events} events. Loosen --flow-z,")
        print("  raise --quiet-z, or widen the sample with --symbols / --bars.")
        return
    stream, m, h, _t = best
    if abs(m) < 0.05:
        print("  No usable edge in either direction. The 'big CVD, quiet price'")
        print("  setup does not predict the next move on this sample. Do NOT wire")
        print("  it into the scanner as a signal.")
    elif m > 0:
        print(f"  Price FOLLOWS the flow. Strongest on {stream} at {h} bars"
              f" ({m:+.3f}% edge).")
        print("  This supports the 'large buying the sellers cannot absorb' reading:")
        print("  quiet price plus one-sided flow precedes a move in the flow's")
        print("  direction. Build the scanner column on the")
        print(f"  {stream} stream, scored at roughly a {h}-bar horizon.")
    else:
        print(f"  Price FADES the flow. Strongest on {stream} at {h} bars"
              f" ({m:+.3f}% edge).")
        print("  This is the absorption reading: flow that cannot move price is")
        print("  being absorbed, and price resolves AGAINST it. If it holds up,")
        print("  the scanner column should be inverted relative to the raw flow.")
    print()
    print("  Compare the QUIET rows against ALIGNED. If ALIGNED is just as strong,")
    print("  the quiet-price condition is adding nothing and plain flow momentum")
    print("  is the whole effect — that would be a much less interesting result.")


# ───────────────────────────── cli ─────────────────────────────

def main(argv=None):
    ap = argparse.ArgumentParser(
        description="Conditional forward-return study for aggregated CVD flow.")
    ap.add_argument("--tf", default="4h",
                    help="kline interval (15m, 1h, 4h, 1d). default 4h")
    ap.add_argument("--bars", type=int, default=1500,
                    help="bars per symbol per market. default 1500")
    ap.add_argument("--symbols", type=int, default=25,
                    help="how many top-volume perp pairs to test. default 25")
    ap.add_argument("--symbol-list", nargs="*", default=None,
                    help="explicit symbols instead of top-volume (e.g. BTCUSDT ETHUSDT)")
    ap.add_argument("--window", type=int, default=6,
                    help="bars the flow/price move is measured over. default 6")
    ap.add_argument("--lookback", type=int, default=200,
                    help="trailing bars for the z-score scaling. default 200")
    ap.add_argument("--flow-z", type=float, default=2.0, dest="flow_z",
                    help="|z| a flow must reach to count as one-sided. default 2.0")
    ap.add_argument("--quiet-z", type=float, default=0.5, dest="quiet_z",
                    help="|z| the price move must stay under to count as quiet. default 0.5")
    ap.add_argument("--horizons", type=int, nargs="*", default=[1, 3, 6, 12, 24],
                    help="forward horizons in bars. default 1 3 6 12 24")
    ap.add_argument("--stride", type=int, default=1,
                    help="sample every Nth bar; set to --window (or more) for "
                         "non-overlapping samples and honest t-stats. default 1")
    ap.add_argument("--min-events-per-symbol", type=int, default=5,
                    dest="min_events_per_symbol",
                    help="symbols with fewer events are left out of the agreement "
                         "ratio. default 5")
    ap.add_argument("--min-cell-events", type=int, default=30,
                    dest="min_cell_events",
                    help="a bucket needs this many events before the verdict "
                         "will call it the winner. default 30")
    ap.add_argument("--in-base", action="store_true",
                    help="restrict to bars inside an accumulation base "
                         "(same gates as the scanner's _base_evaluate)")
    ap.add_argument("--base-window", type=int, default=60, dest="base_window",
                    help="bars in the base when --in-base is set. default 60")
    ap.add_argument("--base-drift", type=float, default=25.0, dest="base_drift",
                    help="max %% drift for a base when --in-base is set. default 25")
    ap.add_argument("--json", default=None, help="write raw results to this file")
    args = ap.parse_args(argv)

    res = run(args)
    if not res:
        return 1
    report(res, args)

    if args.json:
        dump = {"params": res["params"],
                "symbols_used": res["symbols_used"],
                "symbols_skipped": res["symbols_skipped"],
                "results": []}
        for stream in STREAMS:
            for fb in FLOW_BUCKETS:
                for pb in PRICE_BUCKETS:
                    for h in res["horizons"]:
                        s = res["cells"][stream][fb][pb][h].summary(
                            args.min_events_per_symbol)
                        if s["n"]:
                            dump["results"].append(
                                {"stream": stream, "flow": fb, "price": pb,
                                 "horizon": h, **s})
        with open(args.json, "w") as f:
            json.dump(dump, f, indent=2)
        print(f"\n  raw results written to {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
