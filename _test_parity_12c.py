"""
Phase 12C parity test — runs _bt_run_ob_historical_backtest() locally with
synthetic OHLCV data (network egress restricted: Binance is not reachable).

The parity audit compares the same algorithm against itself on the same candle
prefix. Synthetic data is valid because the goal is algorithm correctness,
not real-market statistics.

Usage:
    cd /home/user/hhhhhh-hhhh && python3 _test_parity_12c.py
"""
import os, sys, json, math, traceback, random

# ── Environment stubs ─────────────────────────────────────────────────────────
os.environ.setdefault("DATABASE_URL",   "sqlite:///parity_test.db")
os.environ.setdefault("SECRET_KEY",     "parity-test-key")
os.environ.setdefault("RESEND_API_KEY", "test-resend-key")
os.environ.setdefault("TURNSTILE_SECRET", "")

sys.path.insert(0, os.path.dirname(__file__))

import unittest.mock as _mock
import importlib, types

def _stub_module(name):
    m = types.ModuleType(name)
    for attr in ("__spec__", "__loader__", "__package__"):
        setattr(m, attr, None)
    sys.modules.setdefault(name, m)
    return m

for _mn in ["psycopg2", "psycopg2.extras", "resend"]:
    try:
        importlib.import_module(_mn)
    except ImportError:
        _stub_module(_mn)

print("Importing main.py …", flush=True)
try:
    import main as _m
    print(f"Import OK  (build={getattr(_m,'_BUILD_COMMIT','?')})\n", flush=True)
except Exception as e:
    traceback.print_exc()
    sys.exit(1)


# ── Synthetic candle generator ────────────────────────────────────────────────
def _make_candles(n: int, seed: int = 42) -> list:
    """
    Generate n synthetic OHLCV candles with realistic price action:
    - trending + mean-reverting base using sin + noise
    - clear pivot highs and lows for OB detection
    - price returns to prior swing zones (OB touches)

    Returns list of dicts with keys: openTime, open, high, low, close, volume
    """
    rng = random.Random(seed)
    base = 40000.0
    ts   = 1_700_000_000_000  # ms epoch
    interval_ms = 4 * 3600 * 1000

    candles = []
    price = base
    trend = 0.0

    for i in range(n):
        # Sinusoidal trend for clear pivots
        cycle_pos = i / n
        trend_component = 2000.0 * math.sin(cycle_pos * math.pi * 4)
        noise = rng.gauss(0, 50)
        mean_rev = (base + trend_component - price) * 0.05

        move = noise + mean_rev + rng.gauss(0, 30)
        close = max(100.0, price + move)

        # OHLCV
        wick_hi = abs(rng.gauss(0, 80))
        wick_lo = abs(rng.gauss(0, 80))
        body    = abs(close - price)
        high    = max(price, close) + wick_hi
        low     = min(price, close) - wick_lo
        open_   = price

        vol = max(10.0, rng.gauss(1500, 300))

        candles.append({
            "openTime": ts + i * interval_ms,
            "open":     round(open_, 2),
            "high":     round(high,  2),
            "low":      round(low,   2),
            "close":    round(close, 2),
            "volume":   round(vol,   2),
            "time":     ts + i * interval_ms,
        })
        price = close

    return candles


def _make_candles_1h(n: int, seed: int = 99) -> list:
    """1H candle variant — same logic, different interval and seed."""
    rng   = random.Random(seed)
    base  = 40000.0
    ts    = 1_700_000_000_000
    iv_ms = 3600 * 1000

    candles = []
    price = base
    for i in range(n):
        cycle_pos      = i / n
        trend_comp     = 1500.0 * math.sin(cycle_pos * math.pi * 6)
        noise          = rng.gauss(0, 40)
        mean_rev       = (base + trend_comp - price) * 0.04
        move           = noise + mean_rev + rng.gauss(0, 20)
        close          = max(100.0, price + move)
        wick_hi        = abs(rng.gauss(0, 60))
        wick_lo        = abs(rng.gauss(0, 60))
        high           = max(price, close) + wick_hi
        low            = min(price, close) - wick_lo
        vol            = max(10.0, rng.gauss(1000, 200))
        candles.append({
            "openTime": ts + i * iv_ms,
            "open":  round(price,  2),
            "high":  round(high,   2),
            "low":   round(low,    2),
            "close": round(close,  2),
            "volume":round(vol,    2),
            "time":  ts + i * iv_ms,
        })
        price = close
    return candles


# ── Test setup ────────────────────────────────────────────────────────────────
TESTS = [
    {"label": "BTCUSDT 4H 1000c", "candles": _make_candles(1000, seed=42)},
    {"label": "ETHUSDT 4H 500c",  "candles": _make_candles( 500, seed=77)},
    {"label": "BTCUSDT 1H 500c",  "candles": _make_candles_1h(500, seed=99)},
]

results = []

for cfg in TESTS:
    print(f"{'='*60}", flush=True)
    print(f"TEST: {cfg['label']}", flush=True)

    raw_candles = cfg["candles"]
    n           = len(raw_candles)

    # Normalize the same way _bt_normalize_candles does
    candles = _m._bt_normalize_candles(raw_candles)

    params = {
        "symbol":        "SYNTHETIC",
        "timeframe":     "4h",
        "candle_count":  n,
        "market":        "perpetual",
        "rr_values":     [1, 2, 3],
        "i_len":         _m._BT_PIVOT_LEN,
        "s_len":         _m._BT_PIVOT_LEN,
        "include_parity": False,
        "include_tv_analysis": True,   # this IS the TV audit — opt in explicitly
    }

    # Patch get_klines so it returns our synthetic candles
    with _mock.patch.object(_m, "get_klines", return_value=raw_candles):
        try:
            result = _m._bt_run_ob_historical_backtest(params)
        except Exception as e:
            traceback.print_exc()
            results.append({"label": cfg["label"], "ok": False, "error": str(e)})
            continue

    if not result.get("ok"):
        print(f"  ERROR: {result.get('error')}", flush=True)
        results.append({"label": cfg["label"], "ok": False, "error": result.get("error")})
        continue

    os_     = result.get("outcome_summary", {})
    tv_par  = os_.get("tv_ob_pct_parity",      {})
    tv_ana  = os_.get("tv_ob_pct_analysis",     {})
    tv_perf = os_.get("tv_ob_pct_performance",  {})
    events  = result.get("events", [])

    # ── Acceptance gate ──────────────────────────────────────────────────────
    sc     = tv_par.get("snapshots_checked") or 0
    id_r   = tv_par.get("identity_match_rate_pct")
    vis_r  = tv_par.get("visibility_match_rate_pct")
    pct_r  = tv_par.get("percentage_exact_match_rate_pct")
    mm_cnt = tv_par.get("percentage_mismatches", -1)
    tce    = tv_par.get("touch_candle_excluded")
    trust  = tv_par.get("trusted")

    passed = (
        sc     > 0
        and id_r  == 100.0
        and vis_r == 100.0
        and pct_r == 100.0
        and mm_cnt == 0
        and tce    is True
        and trust  is True
    )

    print(f"  candles: {n}", flush=True)
    print(f"  events total: {result.get('events_total', 0)}", flush=True)
    print(f"  audit_type:                      {tv_par.get('audit_type')}", flush=True)
    print(f"  snapshots_checked:               {sc}", flush=True)
    print(f"  formation_samples:               {tv_par.get('formation_samples')}", flush=True)
    print(f"  pretouch_samples:                {tv_par.get('pretouch_samples')}", flush=True)
    print(f"  identity_match_rate_pct:         {id_r}", flush=True)
    print(f"  visibility_match_rate_pct:       {vis_r}", flush=True)
    print(f"  percentage_exact_match_rate_pct: {pct_r}", flush=True)
    print(f"  percentage_mismatches:           {mm_cnt}", flush=True)
    print(f"  touch_candle_excluded:           {tce}", flush=True)
    print(f"  trusted:                         {trust}", flush=True)

    if tv_par.get("mismatches"):
        print(f"\n  MISMATCHES ({len(tv_par['mismatches'])}):", flush=True)
        for mm in tv_par["mismatches"][:5]:
            print(f"    {json.dumps(mm, default=str)}", flush=True)

    # ── Production mode constant ──────────────────────────────────────────────
    prod_mode = _m._BT_TV_OB_PRODUCTION_OB_LOGIC_MODE
    print(f"\n  Production mode (from detect_obs default): {prod_mode}", flush=True)
    # Verify it matches what detect_obs actually uses
    import inspect as _insp
    actual_default = _insp.signature(_m.detect_obs).parameters["ob_logic_mode"].default
    mode_match = (prod_mode == actual_default)
    print(f"  Mode constant matches detect_obs default: {mode_match} ({actual_default})", flush=True)

    # ── Performance check ─────────────────────────────────────────────────────
    sr   = tv_perf.get('snapshot_requests',    0)
    up   = tv_perf.get('unique_prefixes',       0)
    ch   = tv_perf.get('prefix_cache_hits',     0)
    pdr  = tv_perf.get('prefix_detector_runs',  0)
    ela  = tv_perf.get('elapsed_ms',            0)
    perf_ok = (pdr <= up)
    print(f"\n  Performance:", flush=True)
    print(f"    snapshot_requests:    {sr}", flush=True)
    print(f"    unique_prefixes:      {up}", flush=True)
    print(f"    prefix_cache_hits:    {ch}", flush=True)
    print(f"    prefix_detector_runs: {pdr}", flush=True)
    print(f"    elapsed_ms:           {ela}", flush=True)
    print(f"    perf_ok (runs<=unique): {perf_ok}", flush=True)

    # ── realized_summary_by_rr present ───────────────────────────────────────
    f_buckets = (tv_ana.get("at_formation") or {}).get("buckets") or {}
    has_rz = any(
        bool(b.get("realized_summary_by_rr"))
        for b in f_buckets.values()
    )
    print(f"\n  realized_summary_by_rr present in buckets: {has_rz}", flush=True)

    # ── Winner cards ──────────────────────────────────────────────────────────
    wc = (tv_ana.get("at_formation") or {}).get("winner_cards") or {}
    winners_present = any(wc.get(k) for k in ["by_expectancy","by_profit_factor","by_highest_net_r"])
    trusted_for_cards = bool(trust)
    # Winner cards should only show when trusted=True (the backend sets this flag)
    print(f"  trusted={trusted_for_cards}, winner_cards_populated={winners_present}", flush=True)

    # ── Manual RR=1 bucket verification ──────────────────────────────────────
    manual_ok = None
    manual_detail = {}
    for lbl, bkt in f_buckets.items():
        if lbl == "no_data":
            continue
        rz1 = (bkt.get("realized_summary_by_rr") or {}).get("1") or {}
        trades = rz1.get("trades", 0)
        if trades < 1:
            continue
        wins     = rz1.get("wins",      0)
        losses   = rz1.get("losses",    0)
        amb      = rz1.get("ambiguity", 0)
        net_r    = rz1.get("net_r")
        exp_s    = rz1.get("expectancy_r")
        pf_s     = rz1.get("profit_factor")
        # Recompute
        exp_r    = round(net_r / trades, 4) if (net_r is not None and trades > 0) else None
        exp_ok   = (exp_r == exp_s)
        # trades = wins + losses + ambiguity
        wla_ok   = (wins + losses + amb == trades)
        manual_ok = exp_ok and wla_ok
        manual_detail = {
            "bucket": lbl, "rr": 1,
            "trades": trades, "wins": wins, "losses": losses, "ambiguity": amb,
            "net_r": net_r,
            "expectancy_stored": exp_s, "expectancy_recomputed": exp_r, "exp_ok": exp_ok,
            "wla_sum_ok (wins+losses+amb==trades)": wla_ok,
        }
        break

    if manual_ok is not None:
        print(f"\n  Manual RR=1 bucket verification ({manual_detail['bucket']}):", flush=True)
        for k, v in manual_detail.items():
            print(f"    {k}: {v}", flush=True)
        print(f"  Manual check passed: {manual_ok}", flush=True)
    else:
        print(f"\n  Manual RR check: no bucket with trades (all events may be untouched)", flush=True)

    # ── Alias fields on first event ───────────────────────────────────────────
    first_ev = events[0] if events else {}
    alias_fields = [
        "tv_ob_pct_before_first_touch",
        "tv_ob_pct_before_touch_visible_count",
        "tv_ob_pct_before_touch_total_volume",
        "tv_ob_pct_before_touch_source_volume",
        "tv_ob_pct_formation_visible_count",
        "tv_ob_pct_formation_source_volume",
        "tv_ob_pct_snapshot_bar",
        "tv_ob_pct_snapshot_time",
        "tv_ob_pct_source",
    ]
    missing = [f for f in alias_fields if f not in first_ev]
    print(f"\n  Alias fields on first event — missing: {missing if missing else 'none (all present)'}", flush=True)
    print(f"  tv_ob_pct_source = {first_ev.get('tv_ob_pct_source')}", flush=True)

    # ── Bucket pct range check (Part B validation) ──────────────────────────
    bucket_range_ok = True
    for bkt_lbl, bkt_data in f_buckets.items():
        if bkt_lbl == "no_data":
            continue
        rz_map = bkt_data.get("realized_summary_by_rr") or {}
        for rk, rz in rz_map.items():
            if rz.get("profit_factor") is not None:
                pf = rz["profit_factor"]
                if pf < 0 or (pf > 1000 and rz.get("losses", 0) > 0):
                    bucket_range_ok = False
                    print(f"  WARNING: unreasonable PF {pf} in bucket {bkt_lbl} rr={rk}", flush=True)
    print(f"  Bucket PF range ok: {bucket_range_ok}", flush=True)

    # ── insufficient < 10 excluded from winner cards ─────────────────────────
    for wk, wcard in wc.items():
        if wcard:
            trades_w = wcard.get("trades", 0)
            if trades_w < _m._MIN_RANK_TRADES:
                print(f"  ERROR: winner card {wk} has only {trades_w} trades < {_m._MIN_RANK_TRADES}", flush=True)
                passed = False

    status = "PASS" if passed else "FAIL"
    print(f"\n  ► RESULT: {status}", flush=True)

    results.append({
        "label":   cfg["label"],
        "ok":      True,
        "passed":  passed,
        "trusted": trust,
        "prod_mode_ok": mode_match,
        "perf_ok": perf_ok,
        "manual_ok": manual_ok,
        "parity": {
            "snapshots_checked":               sc,
            "identity_match_rate_pct":         id_r,
            "visibility_match_rate_pct":       vis_r,
            "percentage_exact_match_rate_pct": pct_r,
            "percentage_mismatches":           mm_cnt,
            "touch_candle_excluded":           tce,
        },
        "perf": tv_perf,
        "manual_rr_check": manual_detail,
    })


# ── Summary ───────────────────────────────────────────────────────────────────
print(f"\n{'='*60}", flush=True)
print("PHASE 12C ACCEPTANCE SUMMARY", flush=True)
print(f"{'='*60}", flush=True)

all_passed = True
for r in results:
    st = "PASS" if r.get("passed") else "FAIL"
    if not r.get("passed"):
        all_passed = False
    tr = r.get("trusted")
    sc = (r.get("parity") or {}).get("snapshots_checked", 0)
    id_ = (r.get("parity") or {}).get("identity_match_rate_pct")
    pct = (r.get("parity") or {}).get("percentage_exact_match_rate_pct")
    print(f"  {st}  {r['label']}"
          f"  [trusted={tr}, checked={sc}, id={id_}%, pct={pct}%]", flush=True)

print(f"\n  Production mode constant source: detect_obs default parameter", flush=True)
print(f"  _BT_TV_OB_PRODUCTION_OB_LOGIC_MODE = {_m._BT_TV_OB_PRODUCTION_OB_LOGIC_MODE!r}", flush=True)

print(f"\n  Phase 12C: {'PASSED' if all_passed else 'FAILED'}", flush=True)
print(f"{'='*60}", flush=True)

if not all_passed:
    sys.exit(1)
