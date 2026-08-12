#!/usr/bin/env python3
"""Does bar-direction classification reproduce true taker delta?

WHY THIS EXISTS
---------------
Only Binance publishes the taker buy/sell split in its klines (field 9).
Bybit, OKX, MEXC, Gate, KuCoin and the rest return total volume and nothing
else — you can confirm this in main.py, where every non-Binance kline parser
stops at index 5. So multi-venue CVD cannot be built from real aggressor data
by anyone, including TradingView.

What TradingView does instead is approximate: take lower-timeframe candles
and count each sub-bar's whole volume as BUY if it closed up, SELL if it
closed down. That is the "bar direction" rule the Pine engine uses.

Before aggregating that proxy across five exchanges, it is worth knowing
whether the proxy tracks the real thing at all. Binance is the only venue
where both exist, so it is the only place the question can be answered.

If the proxy tracks true delta closely, multi-venue aggregation rests on a
sound base. If it does not, then aggregating it across venues is stacking
five copies of the same measurement error, and a multi-venue engine would
look more authoritative while being less accurate.

WHAT IT MEASURES
----------------
For every chart bar, two numbers:

    true  = 2*taker_buy_base - volume            (from the chart-tf kline)
    proxy = sum over sub-bars of                 (from lower-tf klines)
            volume * (+1 if close > open else -1 if close < open else 0)

and then three things, in rising order of what actually matters:

  1. CORRELATION      per symbol, between true and proxy per-bar deltas.
  2. SIGN AGREEMENT   how often the two agree on buy-side vs sell-side.
  3. BUCKET AGREEMENT the decisive one. The flow study never uses raw delta;
                      it uses a rolling z-score of windowed flow to sort bars
                      into UP / DOWN / neutral. If the proxy sorts bars into
                      the same buckets, it is good enough for the study's
                      purpose even if the raw correlation is imperfect. If it
                      does not, nothing built on it can be trusted.

Bucket agreement is reported both over all bars and over the bars that
actually matter — the ones true delta calls UP or DOWN, since agreeing that
a quiet bar is quiet is easy and worth nothing.
"""

from __future__ import annotations

import math
import statistics
import sys
from typing import Dict, List, Optional, Sequence, Tuple

sys.path.insert(0, __file__.rsplit("/", 1)[0])

from cvd_flow_study import (  # noqa: E402
    fetch_klines, rolling_z, window_sums, spot_symbol_for, top_symbols,
)

_TF_MS = {
    "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000,
    "30m": 1_800_000, "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000,
    "6h": 21_600_000, "12h": 43_200_000, "1d": 86_400_000,
}


def tf_ms(interval: str) -> int:
    return _TF_MS.get(interval, 0)


def proxy_deltas(sub_rows: Sequence[Dict], chart_ms: int) -> Dict[int, float]:
    """Bar-direction delta per chart bar, keyed by chart-bar open time.

    Whole sub-bar volume is signed by that sub-bar's own direction — the rule
    TradingView's built-in CVD uses, confirmed against it in Phase 1. A doji
    (close == open) contributes nothing rather than being forced to a side.
    """
    out: Dict[int, float] = {}
    for r in sub_rows:
        bucket = (r["t"] // chart_ms) * chart_ms
        c, o, v = r["c"], r["o"], r["v"]
        sign = 1.0 if c > o else (-1.0 if c < o else 0.0)
        out[bucket] = out.get(bucket, 0.0) + v * sign
    return out


def pearson(xs: Sequence[float], ys: Sequence[float]) -> Optional[float]:
    n = len(xs)
    if n < 3:
        return None
    mx, my = statistics.fmean(xs), statistics.fmean(ys)
    sxx = sum((x - mx) ** 2 for x in xs)
    syy = sum((y - my) ** 2 for y in ys)
    if sxx <= 0 or syy <= 0:
        return None
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    return sxy / math.sqrt(sxx * syy)


def _bucket(z: Optional[float], thr: float) -> str:
    if z is None:
        return "na"
    if z >= thr:
        return "UP"
    if z <= -thr:
        return "DOWN"
    return "flat"


def run_proxy_check(args, progress=None) -> Dict:
    """Compare proxy against true delta on Binance perps."""
    symbols = args.symbol_list or top_symbols("perp", args.symbols,
                                              require_spot=True)
    if not symbols:
        return {}

    chart_ms = tf_ms(args.tf)
    sub_ms = tf_ms(args.sub_tf)
    if not chart_ms or not sub_ms or sub_ms >= chart_ms:
        return {"error": f"sub timeframe {args.sub_tf} must be finer than {args.tf}"}
    per_bar = chart_ms // sub_ms

    rows: List[Dict] = []
    total = len(symbols)
    for idx, sym in enumerate(symbols, 1):
        if progress:
            progress(idx - 1, total, f"{sym} — fetching")
        chart = fetch_klines(sym, args.tf, args.bars, "perp")
        if len(chart) < args.lookback + args.window + 20:
            if progress:
                progress(idx, total, f"{sym} — skipped, short history")
            continue
        # Only fetch sub-bars covering the chart bars we actually have.
        want_sub = min(len(chart) * per_bar, args.max_sub_bars)
        sub = fetch_klines(sym, args.sub_tf, want_sub, "perp")
        if not sub:
            if progress:
                progress(idx, total, f"{sym} — skipped, no {args.sub_tf} data")
            continue

        pmap = proxy_deltas(sub, chart_ms)
        # Keep only chart bars fully covered by sub-bars, so a partially
        # fetched tail cannot masquerade as a weak proxy.
        counts: Dict[int, int] = {}
        for r in sub:
            b = (r["t"] // chart_ms) * chart_ms
            counts[b] = counts.get(b, 0) + 1

        true_v, prox_v = [], []
        for r in chart:
            b = (r["t"] // chart_ms) * chart_ms
            if counts.get(b, 0) < per_bar:
                continue
            true_v.append(2.0 * r["tb"] - r["v"])
            prox_v.append(pmap.get(b, 0.0))
        if len(true_v) < args.lookback + args.window + 20:
            if progress:
                progress(idx, total, f"{sym} — skipped, {len(true_v)} covered bars")
            continue

        corr = pearson(true_v, prox_v)
        both = [(a, b) for a, b in zip(true_v, prox_v) if a != 0 and b != 0]
        sign_agree = (100.0 * sum(1 for a, b in both if (a > 0) == (b > 0))
                      / len(both)) if both else 0.0

        # Bucket agreement — the metric that decides whether the proxy is
        # usable for this study, since the study only ever sees buckets.
        tz = rolling_z([v or 0.0 for v in window_sums(true_v, args.window)],
                       args.lookback)
        pz = rolling_z([v or 0.0 for v in window_sums(prox_v, args.window)],
                       args.lookback)
        same = tot = same_act = tot_act = 0
        for a, b in zip(tz, pz):
            ba, bb = _bucket(a, args.flow_z), _bucket(b, args.flow_z)
            if ba == "na" or bb == "na":
                continue
            tot += 1
            if ba == bb:
                same += 1
            if ba in ("UP", "DOWN"):        # bars the study would act on
                tot_act += 1
                if ba == bb:
                    same_act += 1

        rows.append({
            "symbol": sym,
            "bars": len(true_v),
            "corr": corr,
            "sign_agree": sign_agree,
            "bucket_agree": (100.0 * same / tot) if tot else 0.0,
            "active_agree": (100.0 * same_act / tot_act) if tot_act else None,
            "active_n": tot_act,
        })
        if progress:
            progress(idx, total,
                     f"{sym} — r={corr:.2f}" if corr is not None else f"{sym} — done")

    return {"rows": rows, "params": {
        "tf": args.tf, "sub_tf": args.sub_tf, "bars": args.bars,
        "window": args.window, "lookback": args.lookback, "flow_z": args.flow_z,
    }}


def render_proxy(res: Dict) -> str:
    if not res:
        return "No results.\n"
    if res.get("error"):
        return f"Error: {res['error']}\n"
    rows = res["rows"]
    p = res["params"]
    L: List[str] = []
    add = L.append
    add("")
    add("=" * 78)
    add("  PROXY CHECK — does bar-direction reproduce true taker delta?")
    add("=" * 78)
    add(f"  chart {p['tf']}   sub-bars {p['sub_tf']}   flow window {p['window']}"
        f"   z lookback {p['lookback']}   |z|>={p['flow_z']}")
    add("")
    add("  Only Binance publishes the real taker split, so this is the one")
    add("  venue where the proxy can be graded. Multi-venue aggregation can")
    add("  only ever use the proxy — if it fails here, it fails everywhere.")
    add("")
    if not rows:
        add("  No symbols produced enough covered bars. Raise --max-sub-bars")
        add("  or lower --bars.")
        return "\n".join(L) + "\n"

    add(f"  {'symbol':<16}{'bars':>6}{'corr':>7}{'sign%':>7}"
        f"{'bucket%':>9}{'active%':>9}{'actN':>7}")
    for r in rows:
        c = "    —" if r["corr"] is None else f"{r['corr']:>7.2f}"
        a = "    —" if r["active_agree"] is None else f"{r['active_agree']:>9.1f}"
        add(f"  {r['symbol']:<16}{r['bars']:>6}{c}{r['sign_agree']:>7.1f}"
            f"{r['bucket_agree']:>9.1f}{a}{r['active_n']:>7}")

    corrs = [r["corr"] for r in rows if r["corr"] is not None]
    acts = [r["active_agree"] for r in rows if r["active_agree"] is not None]
    add("")
    add("-" * 78)
    add("  SUMMARY")
    add("-" * 78)
    if corrs:
        add(f"  correlation      median {statistics.median(corrs):.2f}"
            f"   worst {min(corrs):.2f}   best {max(corrs):.2f}")
    add(f"  sign agreement   median "
        f"{statistics.median([r['sign_agree'] for r in rows]):.1f}%")
    add(f"  bucket agreement median "
        f"{statistics.median([r['bucket_agree'] for r in rows]):.1f}%")
    if acts:
        med_act = statistics.median(acts)
        add(f"  ACTIVE-bar agreement median {med_act:.1f}%"
            "   <- the number that matters")
        add("")
        # Thresholds are judgement calls, stated plainly rather than hidden.
        if med_act >= 80:
            add("  VERDICT: the proxy reproduces the buckets the study acts on.")
            add("  Multi-venue aggregation on bar-direction is sound — build it.")
        elif med_act >= 60:
            add("  VERDICT: partial. The proxy agrees on most active bars but")
            add("  misclassifies a sizeable minority. Aggregating five venues")
            add("  of this adds coverage AND noise; worth building only if the")
            add("  extra venues carry flow Binance genuinely cannot see.")
        else:
            add("  VERDICT: the proxy does NOT reproduce true delta's buckets.")
            add("  A multi-venue engine built on it would be measuring its own")
            add("  classification error across five exchanges. Do not build it")
            add("  on this basis — and treat the Pine indicator's readings as")
            add("  indicative rather than precise, since it uses this same rule.")
    add("")
    add("  corr    = per-bar correlation, true delta vs proxy delta")
    add("  sign%   = how often they agree the bar was net buying or selling")
    add("  bucket% = how often the z-scored flow lands in the same UP/DOWN/flat")
    add("  active% = same, but ONLY over bars true delta calls UP or DOWN.")
    add("            Agreeing that a quiet bar is quiet is easy and worthless;")
    add("            this column is the one to judge the proxy on.")
    return "\n".join(L) + "\n"
