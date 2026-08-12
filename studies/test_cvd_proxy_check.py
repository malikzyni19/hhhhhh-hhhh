#!/usr/bin/env python3
"""Self-test for cvd_proxy_check.py — can it tell a good proxy from a bad one?

Two synthetic worlds, both built from sub-bars that roll up into chart bars:

  FAITHFUL  each sub-bar's volume really is all buy when it closes up and all
            sell when it closes down. Bar direction is then EXACTLY true
            delta, so correlation must be ~1.0 and active-bar bucket
            agreement ~100%.
  BLIND     each sub-bar's true buy/sell split is drawn independently of its
            direction. Bar direction then carries no information about the
            real split, so correlation must sit near 0 and bucket agreement
            must collapse.

If the checker cannot separate these two, it cannot grade the real proxy
either — and a "the proxy is fine" verdict is exactly the kind of result that
would send us off building a five-exchange engine on sand.

    python3 studies/test_cvd_proxy_check.py
"""

import math
import random
import sys
import types
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import cvd_proxy_check as P

CHART_TF, SUB_TF = "1h", "5m"
CHART_MS, SUB_MS = 3_600_000, 300_000
PER_BAR = CHART_MS // SUB_MS          # 12 sub-bars per chart bar


def build(mode: str, n_chart: int, seed: int):
    """Return (chart_rows, sub_rows) that are exactly consistent with each other."""
    rng = random.Random(seed)
    chart, sub = [], []
    price = 100.0
    for b in range(n_chart):
        t0 = b * CHART_MS
        vol_sum = 0.0
        buy_sum = 0.0
        for k in range(PER_BAR):
            o = price
            price *= math.exp(rng.gauss(0.0, 0.002))
            c = price
            v = rng.uniform(50.0, 150.0)
            if mode == "faithful":
                # direction IS the split
                f = 1.0 if c > o else (0.0 if c < o else 0.5)
            else:
                # split independent of direction
                f = rng.random()
            sub.append({"t": t0 + k * SUB_MS, "o": o, "h": max(o, c),
                        "l": min(o, c), "c": c, "v": v, "tb": v * f})
            vol_sum += v
            buy_sum += v * f
        chart.append({"t": t0, "o": o, "h": price, "l": price, "c": price,
                      "v": vol_sum, "tb": buy_sum})
    return chart, sub


def run(mode: str):
    chart, sub = build(mode, 400, seed=11)

    def fake_fetch(symbol, interval, want, market):
        return chart if interval == CHART_TF else sub

    P.fetch_klines = fake_fetch
    args = types.SimpleNamespace(
        tf=CHART_TF, sub_tf=SUB_TF, bars=400, symbols=1,
        symbol_list=["TESTUSDT"], window=6, lookback=50, flow_z=1.0,
        max_sub_bars=100000,
    )
    res = P.run_proxy_check(args)
    return res["rows"][0] if res.get("rows") else None


def main():
    failures = []
    for mode, want_corr, want_active in (("faithful", 0.95, 90.0),
                                         ("blind", 0.30, 70.0)):
        r = run(mode)
        if r is None:
            failures.append(f"{mode}: produced no row")
            print(f"  {mode:<9} FAIL — no row produced")
            continue
        corr = r["corr"] or 0.0
        act = r["active_agree"] or 0.0
        if mode == "faithful":
            ok = corr >= want_corr and act >= want_active
        else:
            ok = corr <= want_corr and act <= want_active
        print(f"  {mode:<9} corr={corr:+.2f}  active-agree={act:5.1f}%  "
              f"n={r['active_n']:<5} {'OK' if ok else 'FAIL'}")
        if not ok:
            failures.append(f"{mode}: corr={corr:.2f} active={act:.1f}%")

    print()
    if failures:
        for f in failures:
            print(f"  FAIL: {f}")
        return 1
    print("  the checker separates a faithful proxy from a blind one, so its")
    print("  verdict on real data can be trusted.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
