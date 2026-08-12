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

# Benchmark for market-neutral returns. Nearly every alt is a levered bet
# on this one, so "does the flow predict the coin BEYOND what BTC did" is a
# different and much harder question than raw forward return.
BENCHMARK = "BTCUSDT"

# Pause between kline requests. Raised by the admin route: there the study
# shares a Binance IP with the live scanner, and a rate-limit ban would take
# the production scanner down with it. From a laptop 0.12s is plenty.
REQUEST_SLEEP = 0.12


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
        time.sleep(REQUEST_SLEEP)        # be polite to the weight limiter

    # Deduplicate and sort; drop the still-forming last bar.
    seen = {}
    for r in rows:
        seen[r["t"]] = r
    rows = [seen[t] for t in sorted(seen)]
    return rows[:-1] if rows else rows


# Binance lists small-denomination coins as multiplied perp contracts —
# 1000PEPEUSDT, 1000SATSUSDT — while spot lists the plain coin. Looking the
# perp name up on spot returns nothing, which silently drops exactly the
# high-volume alts the study most wants. The multiplier also matters
# arithmetically: one 1000PEPE contract is 1000 PEPE, so spot base volume has
# to be divided by it before spot and perp deltas can be added together.
_MULT_PREFIXES = (("1000000", 1_000_000.0), ("10000", 10_000.0),
                  ("1000", 1_000.0))


def spot_symbol_for(perp: str) -> Tuple[str, float]:
    """(spot symbol, units of spot base per 1 perp contract)."""
    for pref, mult in _MULT_PREFIXES:
        if perp.startswith(pref) and len(perp) > len(pref) + 4:
            return perp[len(pref):], mult
    return perp, 1.0


def spot_universe(quote: str = "USDT") -> set:
    """Every symbol that actually trades on Binance spot.

    Binance now lists tokenized equities and commodities as perps — XAUUSDT,
    SOXLUSDT, SAMSUNGUSDT, CLUSDT — which rank high on futures volume but have
    no spot market at all. Ranking by perp volume alone spent most of the
    symbol budget fetching pairs that could never qualify, so the perp list is
    intersected with this set before anything is downloaded.
    """
    data = (_get_json(SAPI + "/api/v3/exchangeInfo", {"permissions": "SPOT"})
            or _get_json(SAPI_MIRROR + "/api/v3/exchangeInfo", {"permissions": "SPOT"}))
    out = set()
    for s in (data or {}).get("symbols", []):
        if s.get("status") == "TRADING" and str(s.get("symbol", "")).endswith(quote):
            out.add(s["symbol"])
    return out


def top_symbols(market: str, n: int, quote: str = "USDT",
                require_spot: bool = False) -> List[str]:
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
    ranked = [s for _, s in rows]
    if require_spot:
        uni = spot_universe(quote)
        if uni:      # only filter when the lookup succeeded — never silently
                     # return an empty universe because one request failed
            ranked = [s for s in ranked
                      if s in uni or spot_symbol_for(s)[0] in uni]
    return ranked[:n]


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
        # Same edge measured on the coin's move RELATIVE TO BITCOIN. Almost
        # every alt tracks BTC, so 30 symbols agreeing is not 30 independent
        # confirmations — it can be one BTC move counted thirty times. This
        # column asks whether the flow predicted anything BTC did not.
        self.signed_mn: List[float] = []
        self.per_symbol: Dict[str, List[float]] = {}

    def add(self, symbol: str, signed_excess: float, raw: float,
            signed_mn: Optional[float] = None):
        self.signed.append(signed_excess)
        self.raw.append(raw)
        if signed_mn is not None:
            self.signed_mn.append(signed_mn)
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
            "n_mn": len(self.signed_mn),
            "mean_mn": statistics.fmean(self.signed_mn) if self.signed_mn else None,
            "t_mn": _tstat(self.signed_mn),
        }


# ───────────────────────────── the study ─────────────────────────────

STREAMS = ("spot", "perp", "global")
FLOW_BUCKETS = ("UP", "DOWN")
PRICE_BUCKETS = ("QUIET", "ALIGNED", "OPPOSED")


def run(args, progress=None) -> Dict:
    """Run the study. `progress(done, total, message)` is called per symbol.

    The callback exists so a long run can report itself from somewhere other
    than a terminal — the admin route drives a status page from it.
    """
    symbols = args.symbol_list or top_symbols("perp", args.symbols,
                                              require_spot=True)
    if not symbols:
        print("Could not fetch a symbol list. Check network access to Binance.",
              file=sys.stderr)
        if progress:
            progress(0, 0, "Could not fetch a symbol list — no route to Binance.")
        return {}

    horizons = args.horizons
    hmax = max(horizons)

    def _new_cells():
        return {s: {f: {p: {h: Cell() for h in horizons} for p in PRICE_BUCKETS}
                    for f in FLOW_BUCKETS}
                for s in STREAMS}

    # Two parallel sets. `cells` follows --stride and drives the big
    # exploration table. `icells` always samples one bar in hmax so forward
    # windows never overlap, and it is what the robustness screen reads —
    # significance must not depend on a UI field anyone can leave at 1.
    cells = _new_cells()
    icells = _new_cells()
    baseline: Dict[int, Cell] = {h: Cell() for h in horizons}

    # Benchmark for the market-neutral column.
    btc = fetch_klines(BENCHMARK, args.tf, args.bars, "perp")
    btc_map = {r["t"]: r["c"] for r in btc}
    if not btc_map:
        print("warning: no benchmark data — market-neutral column unavailable",
              file=sys.stderr)

    used, skipped = [], []
    total = len(symbols)
    need = args.lookback + args.window + max(horizons) + 50
    for idx, sym in enumerate(symbols, 1):
        print(f"[{idx}/{total}] {sym} ...", end=" ", flush=True)

        def _skip(reason: str, note: str):
            print(f"skip ({note})")
            skipped.append((sym, reason))
            if progress:
                progress(idx, total, f"{sym} — skipped, {note}")

        perp = fetch_klines(sym, args.tf, args.bars, "perp")
        if len(perp) < need:
            _skip("perp history", f"perp history {len(perp)} < {need} bars")
            continue

        # Try the perp's own name on spot first, then the de-multiplied name.
        spot_scale = 1.0
        spot = fetch_klines(sym, args.tf, args.bars, "spot")
        if len(spot) < need:
            alt, mult = spot_symbol_for(sym)
            if alt != sym:
                alt_rows = fetch_klines(alt, args.tf, args.bars, "spot")
                if len(alt_rows) >= need:
                    spot, spot_scale = alt_rows, mult
        if len(spot) < need:
            _skip("spot history", f"no/short spot listing ({len(spot)} bars)")
            continue

        srows, prows = align(spot, perp)
        if len(srows) < need:
            _skip("overlap", f"only {len(srows)} overlapping bars")
            continue

        _accumulate(sym, srows, prows, args, cells, icells, baseline,
                    spot_scale, btc_map)
        used.append(sym)
        print(f"ok ({len(srows)} bars)")
        if progress:
            progress(idx, total, f"{sym} — ok, {len(srows)} bars")

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
        "icells": icells,
        "baseline": baseline,
        "horizons": horizons,
    }


def _accumulate(sym, srows, prows, args, cells, icells, baseline,
                spot_scale: float = 1.0, btc_map: Optional[Dict] = None):
    """Bucket every eligible bar of one symbol into the cell accumulators."""
    closes = [r["c"] for r in prows]        # perp price is the traded price
    n = len(closes)

    sd = bar_deltas(srows)
    pd_ = bar_deltas(prows)
    # Convert spot base units into perp contract units before adding, or a
    # 1000x-denominated contract would let spot swamp the global stream by
    # three orders of magnitude while looking perfectly reasonable.
    gd = [a / spot_scale + b for a, b in zip(sd, pd_)]

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

    # Benchmark-relative return at each bar, per horizon. None where the
    # benchmark has no bar at that timestamp (or for the benchmark itself,
    # where the answer would be a trivial zero).
    is_bench = (sym == BENCHMARK)
    btc_map = btc_map or {}

    def _rel(i: int, h: int, raw: float) -> Optional[float]:
        if is_bench or not btc_map:
            return None
        a = btc_map.get(prows[i]["t"])
        b = btc_map.get(prows[i + h]["t"])
        if a is None or b is None or a <= 0:
            return None
        return raw - (b / a - 1.0) * 100.0

    # Unconditional forward return per symbol, per horizon — the drift we
    # subtract so a bull sample cannot masquerade as predictive flow. The same
    # is done for the benchmark-relative series, so a coin that simply
    # outperformed BTC all period does not read as predictive flow either.
    drift: Dict[int, float] = {}
    drift_rel: Dict[int, float] = {}
    for h in args.horizons:
        rets, rels = [], []
        for i in range(start, n - hmax):
            if closes[i] <= 0:
                continue
            r = (closes[i + h] / closes[i] - 1.0) * 100.0
            rets.append(r)
            rl = _rel(i, h, r)
            if rl is not None:
                rels.append(rl)
        drift[h] = statistics.fmean(rets) if rets else 0.0
        drift_rel[h] = statistics.fmean(rels) if rels else 0.0
        for r in rets:
            baseline[h].add(sym, r - drift[h], r)

    for i in range(start, n - hmax):
        in_sample = ((i - start) % args.stride == 0)
        in_indep = ((i - start) % hmax == 0)
        if not (in_sample or in_indep):
            continue
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
                sgn = sign * (raw - drift[h])
                rl = _rel(i, h, raw)
                mn = None if rl is None else sign * (rl - drift_rel[h])
                if in_sample:
                    cells[stream][fb][pb][h].add(sym, sgn, raw, mn)
                if in_indep:
                    icells[stream][fb][pb][h].add(sym, sgn, raw, mn)


# ───────────────────────────── reporting ─────────────────────────────

def _fmt(v, nd=3, width=8):
    if v is None:
        return "\u2014".rjust(width)
    return f"{v:.{nd}f}".rjust(width)


def render(res: Dict, args) -> str:
    """Build the whole report as text.

    Returns a string rather than printing so the same output can go to a
    terminal, a file, or an HTML page without three copies of the formatting.
    """
    if not res:
        return "No results.\n"
    L: List[str] = []
    add = L.append

    horizons = res["horizons"]
    cells = res["cells"]
    baseline = res["baseline"]
    p = res["params"]

    add("")
    add("=" * 78)
    add("  CVD FLOW STUDY \u2014 does price follow the flow, or fade it?")
    add("=" * 78)
    add(f"  timeframe {p['tf']}   bars/symbol {p['bars']}   flow window {p['window']} bars")
    add(f"  z lookback {p['lookback']}   flow threshold |z|>={p['flow_z']}   quiet |z|<={p['quiet_z']}")
    add(f"  sampling stride {p['stride']}   horizons {horizons}"
        + (f"   [inside accumulation bases only, window {p['base_window']}]"
           if p["in_base"] else ""))
    add(f"  symbols used {len(res['symbols_used'])}"
        f"   skipped {len(res['symbols_skipped'])}")
    if res["symbols_used"]:
        add("  used:    " + " ".join(res["symbols_used"]))
    if res["symbols_skipped"]:
        # A run that quietly drops most of its universe produces confident
        # numbers from almost no data, so the skips are printed, not buried.
        reasons: Dict[str, List[str]] = {}
        for sym, why in res["symbols_skipped"]:
            reasons.setdefault(why, []).append(sym)
        for why, syms in sorted(reasons.items()):
            add(f"  skipped ({why}): " + " ".join(syms))
    if len(res["symbols_used"]) < 8:
        add("")
        add("  ** WARNING: too few symbols for a trustworthy result. Anything")
        add("     below ~8 symbols is a pilot run, not evidence. **")
    add("")
    add("  SIGNED EDGE = mean(forward return x sign(flow)) minus that symbol's")
    add("  own drift.  POSITIVE = price FOLLOWS the flow.  NEGATIVE = price FADES it.")
    add("  'syms' = how many symbols show a positive edge, out of those with enough")
    add("  events. That ratio is the honest robustness check, not the t-stat.")
    add("")

    for h in horizons:
        b = baseline[h].summary()
        add("-" * 78)
        add(f"  HORIZON: {h} bars forward"
            f"   (unconditional mean move {b.get('raw_mean', 0.0):+.3f}%)")
        add("-" * 78)
        add(f"  {'stream':<7} {'flow':<5} {'price':<8} {'N':>7} "
            f"{'edge %':>8} {'med %':>8} {'win %':>7} {'t':>7} {'syms':>9}")
        for stream in STREAMS:
            for fb in FLOW_BUCKETS:
                for pb in PRICE_BUCKETS:
                    s = cells[stream][fb][pb][h].summary(args.min_events_per_symbol)
                    if s["n"] == 0:
                        continue
                    syms = f"{s['symbols_positive']}/{s['symbols']}"
                    add(f"  {stream:<7} {fb:<5} {pb:<8} {s['n']:>7} "
                        f"{_fmt(s['mean'])} {_fmt(s['median'])} "
                        f"{s['win']:>6.1f} {_fmt(s['t'], 2, 7)} {syms:>9}")
            add("")

    L.extend(_verdict_lines(res, args))
    return "\n".join(L) + "\n"


def report(res: Dict, args, stream=None) -> None:
    """Print the report. Thin wrapper over render()."""
    (stream or sys.stdout).write(render(res, args))


def _screen(res: Dict, args, min_t: float = 2.0, min_agree: float = 0.70) -> List:
    """Every cell that survives a robustness screen, strongest first.

    Four filters, each one added because a cell passed the previous ones while
    still being junk:
      * enough events            — a big t on 6 samples means nothing
      * |t| >= min_t             — the verdict once crowned a cell at t=1.0
      * symbol agreement         — one coin can carry a pooled average
      * mean and median agree    — at a 24-bar horizon a single 300% alt move
                                   drags the mean positive while the median
                                   sits negative. Sign disagreement means the
                                   average describes an outlier, not a rule.
    """
    out = []
    for stream in STREAMS:
        for fb in FLOW_BUCKETS:
            for pb in PRICE_BUCKETS:
                for h in res["horizons"]:
                    s = res.get("icells", res["cells"])[stream][fb][pb][h]\
                        .summary(args.min_events_per_symbol)
                    if s["n"] < args.min_cell_events:
                        continue
                    if s["t"] is None or abs(s["t"]) < min_t:
                        continue
                    if s["symbols"] < 3:
                        continue
                    # Agreement is measured toward the edge's own direction:
                    # a consistent FADE is as real a finding as a consistent
                    # FOLLOW, and counting only positives would hide it.
                    agree = s["symbols_positive"] / s["symbols"]
                    if s["mean"] < 0:
                        agree = 1.0 - agree
                    if agree < min_agree:
                        continue
                    if (s["mean"] > 0) != (s["median"] > 0):
                        continue
                    out.append((stream, fb, pb, h, s, agree))
    out.sort(key=lambda r: -abs(r[4]["t"]))
    return out


def _verdict_lines(res: Dict, args) -> List[str]:
    """Plain-language answer to Q1 and Q2, from the QUIET bucket only."""
    horizons = res["horizons"]
    cells = res["cells"]
    L: List[str] = []
    add = L.append
    add("=" * 78)
    add("  VERDICT")
    add("=" * 78)

    # Ranked by |t|, NOT by |edge|. Forward returns compound, so the longest
    # horizon always has the largest raw edge and picking by magnitude would
    # mechanically crown it regardless of whether the signal got any cleaner.
    best = None
    eligible = set()
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
                line.append(f"h{h}: \u2014")
                continue
            m = statistics.fmean(merged)
            t = _tstat(merged)
            agree += up.get("symbols_positive", 0) + dn.get("symbols_positive", 0)
            total += up.get("symbols", 0) + dn.get("symbols", 0)
            line.append(f"h{h}: {m:+.3f}% (t={t:.1f})" if t is not None
                        else f"h{h}: {m:+.3f}%")
            # A handful of events can produce a huge t by luck; require a
            # sample worth reading before a cell can be called the winner.
            if t is not None and len(merged) >= args.min_cell_events:
                eligible.add(stream)
                if best is None or abs(t) > abs(best[3]):
                    best = (stream, m, h, t)
        share = (100.0 * agree / total) if total else 0.0
        nq = (len(cells[stream]["UP"]["QUIET"][horizons[0]].signed)
              + len(cells[stream]["DOWN"]["QUIET"][horizons[0]].signed))
        mark = "" if stream in eligible else f"   [only {nq} events — not eligible]"
        add(f"  {stream:<7} quiet-price flow  " + "   ".join(line))
        add(f"  {'':<7} symbol agreement {share:.0f}%{mark}")
    add("")

    # Naming a winner drawn from a field of one is not a comparison. Say so
    # rather than letting the headline imply the streams were weighed against
    # each other.
    if best is not None and len(eligible) < len(STREAMS):
        missing = [s for s in STREAMS if s not in eligible]
        add(f"  ** {', '.join(missing)} did not reach {args.min_cell_events} events,")
        add(f"     so the line below compares nothing — {best[0]} was the only")
        add("     stream eligible. Re-run with more symbols before reading it")
        add("     as 'which stream is best'. **")
        add("")

    # \u2500\u2500 Q2: does the quiet-price setup predict anything at all? \u2500\u2500
    quiet_hit = None
    for stream, fb, pb, h, s, agree in _screen(res, args):
        if pb == "QUIET":
            quiet_hit = (stream, fb, h, s, agree)
            break

    if best is None:
        add(f"  No QUIET cell reached {args.min_cell_events} events. Loosen")
        add("  --flow-z, raise --quiet-z, or widen the sample.")
    elif quiet_hit is None:
        add("  THE QUIET-PRICE SETUP SHOWS NO USABLE EDGE on this sample.")
        add("  Not one 'big CVD move, quiet candles' cell survives a basic")
        add("  robustness screen (enough events, |t| >= 2, 70% of symbols")
        add("  agreeing, mean and median pointing the same way).")
        add("  On this evidence, do NOT build a scanner signal on it.")
    else:
        stream, fb, h, s, agree = quiet_hit
        d = "FOLLOWS" if s["mean"] > 0 else "FADES"
        add(f"  Quiet-price flow survives the screen: price {d} the flow on")
        add(f"  {stream}, {fb} flow, at {h} bars \u2014 edge {s['mean']:+.3f}%,")
        add(f"  t={s['t']:.1f}, {agree*100:.0f}% of symbols agree, n={s['n']}.")
    add("")

    # \u2500\u2500 The wider screen: where IS the signal, if not in QUIET? \u2500\u2500
    survivors = _screen(res, args)
    add("-" * 78)
    add("  ROBUST CELLS  (n >= %d, |t| >= 2, >=70%% symbols agree, mean and"
        % args.min_cell_events)
    add("                 median same sign) \u2014 strongest first")
    add("-" * 78)
    if not survivors:
        add("  Nothing survives. No bucket on this sample carries a signal that")
        add("  is both statistically and cross-sectionally consistent. That is")
        add("  a real answer, not a failed run \u2014 it says this feature set does")
        add("  not predict forward returns here.")
    else:
        add(f"  {'stream':<7} {'flow':<5} {'price':<8} {'bars':>5} {'edge %':>8}"
            f" {'med %':>8} {'t':>6} {'agree':>6} {'n':>6} {'vsBTC%':>8} {'tBTC':>6}")
        for stream, fb, pb, h, s, agree in survivors[:12]:
            mn = "     —" if s.get("mean_mn") is None else f"{s['mean_mn']:>8.2f}"
            tm = "     —" if s.get("t_mn") is None else f"{s['t_mn']:>6.1f}"
            add(f"  {stream:<7} {fb:<5} {pb:<8} {h:>5} {s['mean']:>8.2f}"
                f" {s['median']:>8.2f} {s['t']:>6.1f} {agree*100:>5.0f}%"
                f" {s['n']:>6} {mn} {tm}")
        add("")
        # The BTC columns are the ones that decide whether any of this is
        # tradeable information or just beta wearing a costume.
        held = [r for r in survivors
                if r[4].get("t_mn") is not None and abs(r[4]["t_mn"]) >= 2.0
                and (r[4]["mean_mn"] > 0) == (r[4]["mean"] > 0)]
        add(f"  vsBTC% / tBTC = the same edge measured on each coin's move")
        add("  RELATIVE TO BITCOIN. Almost every alt is a levered BTC bet, so")
        add("  30 symbols agreeing can be one BTC move counted 30 times.")
        add(f"  Of {len(survivors)} survivors, {len(held)} keep a same-signed")
        add("  edge at |t|>=2 once BTC is removed.")
        if survivors and not held:
            add("  NONE survive it. On this sample the flow signal is BTC beta:")
            add("  it tells you where the whole market went, not which coin to")
            add("  pick. Useful as a market-wide risk filter, useless as a")
            add("  per-pair scanner column.")
        add("")
        add("  Read the price column: QUIET is the 'quiet candles' setup,")
        add("  ALIGNED is plain flow momentum, OPPOSED is flow diverging from")
        add("  price. Whichever dominates this list is where the information")
        add("  actually is \u2014 and it is not necessarily the one we set out to")
        add("  test.")
        add("")
        ups = sum(1 for r in survivors if r[1] == "UP")
        if survivors and ups == 0:
            add("  NOTE: every surviving cell is DOWN flow. Not one buy-side cell")
            add("  cleared the screen. Whatever this measures, it works on")
            add("  selling and not on buying \u2014 do not assume it mirrors.")
        elif survivors and ups == len(survivors):
            add("  NOTE: every surviving cell is UP flow. Nothing on the sell")
            add("  side cleared the screen.")

    L.extend(_luck_lines(res, args, len(survivors)))
    return L


def _luck_lines(res: Dict, args, n_survivors: int) -> List[str]:
    """Two corrections that decide whether the table above means anything.

    Screening many cells and reporting the winners is how noise gets published:
    at |t| >= 2 roughly one test in twenty passes by chance alone, so a screen
    over 90 cells is expected to hand back about four survivors even if CVD
    carried no information whatsoever.

    Overlapping samples compound it. At stride 1 with a 24-bar horizon,
    consecutive events share almost all of their forward window, so the
    effective sample is a small fraction of n and every t-stat is inflated.
    """
    L: List[str] = []
    add = L.append
    tested = 0
    for stream in STREAMS:
        for fb in FLOW_BUCKETS:
            for pb in PRICE_BUCKETS:
                for h in res["horizons"]:
                    s = res.get("icells", res["cells"])[stream][fb][pb][h]\
                        .summary(args.min_events_per_symbol)
                    if s["n"] >= args.min_cell_events and s.get("t") is not None:
                        tested += 1
    expected = tested * 0.0455        # two-sided |t| >= 2 under the null

    add("")
    add("-" * 78)
    add("  HOW MUCH OF THIS IS LUCK")
    add("-" * 78)
    add(f"  cells screened {tested}   expected to pass |t|>=2 by chance alone"
        f" {expected:.1f}   actually passed {n_survivors}")
    if n_survivors <= expected:
        add("  The survivor count is at or below what pure noise produces.")
        add("  Treat NOTHING in the table above as a finding.")
    else:
        add("  Only cells well clear of that budget \u2014 and repeated across")
        add("  neighbouring horizons or streams \u2014 should be believed. A lone")
        add("  cell at t just over 2 is exactly what chance delivers.")

    hmax = max(res["horizons"])
    add("")
    add(f"  The ROBUST CELLS screen above always samples one bar in {hmax}, so")
    add("  its forward windows never overlap regardless of the stride setting.")
    if args.stride < hmax:
        add(f"  The big per-horizon tables use stride {args.stride}, so their")
        add("  t columns ARE inflated by overlap \u2014 read those as a ranking")
        add("  only. The screen is the part to trust.")
    return L


def _verdict(res: Dict, args, stream=None) -> None:
    (stream or sys.stdout).write("\n".join(_verdict_lines(res, args)) + "\n")


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
