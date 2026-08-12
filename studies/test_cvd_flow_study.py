#!/usr/bin/env python3
"""Self-test for cvd_flow_study.py — synthetic data with a KNOWN answer.

This does not test whether CVD predicts price. It tests that the machinery
would notice if it did. Two worlds are generated:

  FOLLOW world  quiet-price + one-sided flow is followed by a move in the
                flow's direction. The study must report a positive edge.
  FADE world    the same setup is followed by a move against the flow. The
                study must report a negative edge.

If the harness reports the wrong sign, every real result it prints is
worthless — which is exactly the failure mode a study like this hides well.

    python3 studies/test_cvd_flow_study.py
"""

import math
import random
import sys
import types
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import cvd_flow_study as S


def make_series(n: int, mode: str, seed: int, window: int = 6, horizon: int = 6):
    """Build klines whose flow/price relationship is fixed by `mode`.

    Every `period` bars an "event" is planted: a burst of one-sided taker
    volume with almost no price movement, then a move over the following
    `horizon` bars whose direction is set by `mode`. Between events, price is
    a random walk and flow is noise, so the study has a realistic background
    to separate the events from.
    """
    rng = random.Random(seed)
    price = 100.0
    rows = []
    pending = []            # (bars_remaining, per_bar_drift)
    period = window + horizon + 8
    for i in range(n):
        drift = 0.0
        if pending:
            bars_left, per_bar = pending[0]
            drift = per_bar
            if bars_left <= 1:
                pending.pop(0)
            else:
                pending[0] = (bars_left - 1, per_bar)

        noise = rng.gauss(0.0, 0.004)
        vol = rng.uniform(800.0, 1200.0)
        # Background flow: balanced with noise.
        tb = vol * (0.5 + rng.gauss(0.0, 0.02))

        # Plant an event: the burst spans the `window` bars ending at the
        # event bar, so the study's trailing window sees all of it.
        in_burst = (i % period) >= (period - window - horizon) and \
                   (i % period) < (period - horizon)
        is_event_bar = (i % period) == (period - horizon - 1)
        if in_burst and i > 260:
            side = 1 if (i // period) % 2 == 0 else -1
            tb = vol * (0.5 + side * 0.42)      # heavily one-sided taker flow
            noise = rng.gauss(0.0, 0.0004)      # price barely moves: QUIET
            if is_event_bar:
                if mode == "follow":
                    total = side * 0.05          # +5% in the flow's direction
                else:
                    total = -side * 0.05         # -5%: absorbed, price fades it
                pending.append((horizon, total / horizon))

        price *= math.exp(noise + drift)
        rows.append({
            "t": i * 60_000,
            "o": price, "h": price * 1.001, "l": price * 0.999, "c": price,
            "v": vol, "tb": tb,
        })
    return rows


def run_world(mode: str, symbols=("AAAUSDT", "BBBUSDT", "CCCUSDT", "DDDUSDT"),
              benchmark=None):
    """Run the real study end to end against synthetic klines.

    `benchmark` supplies the BTCUSDT series the market-neutral column is
    measured against. When it is None the study still asks for it, so the
    fetch must return an empty list rather than raising — a missing benchmark
    has to degrade to "no vsBTC column", never to a crash.
    """
    data = {s: make_series(1400, mode, seed=hash(s) & 0xFFFF) for s in symbols}
    if benchmark is not None:
        data[S.BENCHMARK] = benchmark

    S.fetch_klines = lambda sym, tf, want, market: data.get(sym, [])
    S.top_symbols = lambda market, n, quote="USDT", require_spot=False: list(symbols)

    args = types.SimpleNamespace(
        tf="1m", bars=1400, symbols=len(symbols), symbol_list=list(symbols),
        window=6, lookback=200, flow_z=2.0, quiet_z=0.5,
        horizons=[6], stride=1, min_events_per_symbol=3, min_cell_events=30,
        in_base=False, base_window=60, base_drift=25.0, json=None,
    )
    res = S.run(args)
    merged = []
    merged += res["cells"]["global"]["UP"]["QUIET"][6].signed
    merged += res["cells"]["global"]["DOWN"]["QUIET"][6].signed
    return res, merged


def main():
    failures = []

    for mode, want_sign in (("follow", +1), ("fade", -1)):
        res, merged = run_world(mode)
        if not merged:
            failures.append(f"{mode}: no events matched at all")
            print(f"  {mode:<7} FAIL — no events matched")
            continue
        mean = sum(merged) / len(merged)
        ok = (mean > 0) == (want_sign > 0) and abs(mean) > 0.5
        print(f"  {mode:<7} n={len(merged):<5} edge={mean:+.3f}%  "
              f"expected {'positive' if want_sign > 0 else 'negative'}  "
              f"{'OK' if ok else 'FAIL'}")
        if not ok:
            failures.append(f"{mode}: edge {mean:+.3f}% has the wrong sign")

    # BETA WORLD: every symbol IS the benchmark, bar for bar. The raw edge
    # must still show up, and the vs-BTC edge must collapse to ~0 — otherwise
    # the market-neutral column cannot tell alpha from beta, which is the one
    # job it exists to do.
    syms = ("AAAUSDT", "BBBUSDT", "CCCUSDT", "DDDUSDT")
    bench = make_series(1400, "follow", seed=hash("AAAUSDT") & 0xFFFF)
    res, merged = run_world("follow", syms, benchmark=bench)
    cell = res["icells"]["global"]["UP"]["QUIET"][6]
    raw_edge = sum(cell.signed) / len(cell.signed) if cell.signed else 0.0
    mn_edge = (sum(cell.signed_mn) / len(cell.signed_mn)) if cell.signed_mn else None
    if mn_edge is None:
        failures.append("beta: no market-neutral samples collected")
        print("  beta    FAIL — vsBTC column never populated")
    else:
        ok = abs(raw_edge) > 0.5 and abs(mn_edge) < 0.5
        print(f"  beta    raw={raw_edge:+.3f}%  vsBTC={mn_edge:+.3f}%  "
              f"(vsBTC must be ~0)  {'OK' if ok else 'FAIL'}")
        if not ok:
            failures.append(
                f"beta: raw {raw_edge:+.3f}% / vsBTC {mn_edge:+.3f}% — the "
                "market-neutral column did not cancel pure beta")

    print()
    if failures:
        for f in failures:
            print(f"  FAIL: {f}")
        return 1
    print("  all checks passed — bucketing, sign convention and reporting agree")
    print("  with the planted ground truth in both directions.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
