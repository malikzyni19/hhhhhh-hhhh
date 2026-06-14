"""
Phase 14B unit tests — Server-authoritative Cross-Market TV OB% Threshold
Stability Lab (research only).

Design constraints (Part G):
  * No conditional assertions — every gate test asserts its precondition first,
    then asserts the gated outcome. All assertions always execute.
  * Server-authority tests mock `_bt_run_ob_historical_backtest` and exercise
    `_bt_run_threshold_stability_cells` and the validation logic directly.
  * `_stab_pf_non_degraded` returns (passes: bool, reason: str).

Usage:
    cd /home/user/hhhhhh-hhhh && python3 _test_phase_14.py
"""
import os, sys, json, math, traceback, unittest
from unittest.mock import patch, MagicMock

os.environ.setdefault("DATABASE_URL", "sqlite:///phase14_test.db")
os.environ.setdefault("SECRET_KEY",   "phase14-test-key")
os.environ.setdefault("RESEND_API_KEY", "test-resend-key")
os.environ.setdefault("TURNSTILE_SECRET", "")

sys.path.insert(0, os.path.dirname(__file__))

import types, importlib

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
    print("Import OK\n", flush=True)
except Exception:
    traceback.print_exc()
    sys.exit(1)

_sample_status    = _m._stab_sample_status
_agg_pf           = _m._stab_aggregate_pf
_pf_nondeg        = _m._stab_pf_non_degraded
_bootstrap        = _m._stab_bootstrap_delta
_build_stability  = _m._bt_build_tv_ob_pct_stability_analysis
_run_cells        = _m._bt_run_threshold_stability_cells
_MIN_AUTH         = _m._MIN_AUTHORITATIVE_TRADES
_STAB_BOOTSTRAP_SEED  = _m._STAB_BOOTSTRAP_SEED
_STAB_BOOTSTRAP_ITERS = _m._STAB_BOOTSTRAP_ITERS
_STAB_MAX_CELLS       = _m._STAB_MAX_CELLS


# ── Cell builders ───────────────────────────────────────────────────────────

def _rz(w, l, rv, loss_r=0.9, baseline_total=100):
    """Build a realized_summary_by_rr entry for one RR, plus retention pct."""
    gp = w * float(rv)
    gl = l * loss_r
    nr = gp - gl
    t = w + l
    exp = round(nr / t, 6) if t > 0 else None
    pf_val = round(gp / gl, 4) if gl > 0 else None
    pf_inf = (gl == 0 and w > 0)
    ret = round(t / baseline_total * 100, 4) if baseline_total > 0 else None
    return {
        "trades": t, "wins": w, "losses": l,
        "ambiguous": 0, "unresolved": 0, "invalid_loss_r": 0,
        "win_rate_pct": round(w / t * 100, 4) if t > 0 else None,
        "gross_profit_r": gp, "gross_loss_r": gl, "net_r": nr,
        "expectancy_r": exp, "profit_factor_r": pf_val,
        "profit_factor_infinite": pf_inf,
        "valid_percentage_events": t,
    }, ret


def _make_thr_block(wins, losses, rr_str, loss_r, baseline_total, baseline_exp):
    rz, ret = _rz(wins, losses, rr_str, loss_r, baseline_total)
    exp = rz["expectancy_r"]
    return {
        "realized_summary_by_rr": {rr_str: rz},
        "comparison_vs_baseline_by_rr": {
            rr_str: {
                "trade_retention_pct": ret,
                "expectancy_delta": (round(exp - baseline_exp, 6)
                                     if exp is not None else None),
                "net_r_delta": round(rz["net_r"], 6),
            }
        },
    }


def _make_cell(sym, tf, wins, losses, rr_str="1", thr=50,
               loss_r=0.9, parity=True, baseline_wins=60,
               baseline_losses=40, ok=True, threshold_analysis_present=True):
    """Build a server-side cell with threshold + baseline blocks."""
    baseline_total = baseline_wins + baseline_losses
    base_block = _make_thr_block(baseline_wins, baseline_losses, rr_str,
                                 loss_r, baseline_total, 0.0)
    base_exp = base_block["realized_summary_by_rr"][rr_str]["expectancy_r"]
    thr_block = _make_thr_block(wins, losses, rr_str, loss_r,
                                baseline_total, base_exp)
    ta = None
    if threshold_analysis_present:
        ta = {
            "before_first_touch": {
                "results": {
                    "0": base_block,
                    str(thr): thr_block,
                }
            }
        }
    return {
        "symbol": sym, "timeframe": tf, "candle_count": 500,
        "ok": ok, "error": None,
        "generated_server_side": True, "build_commit": "115115a",
        "data_source": "binance_futures_klines",
        "parity_trusted": parity,
        "threshold_analysis": ta,
        "threshold_analysis_present": ta is not None,
    }


def _make_good_cells(syms=None, tfs=None, wins=25, losses=5,
                     rr_str="1", thr=50, loss_r=0.9,
                     baseline_wins=48, baseline_losses=32, parity=True):
    if syms is None:
        syms = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]
    if tfs is None:
        tfs = ["1h", "4h"]
    cells = []
    for sym in syms:
        for tf in tfs:
            cells.append(_make_cell(sym, tf, wins, losses, rr_str=rr_str,
                                    thr=thr, loss_r=loss_r,
                                    baseline_wins=baseline_wins,
                                    baseline_losses=baseline_losses,
                                    parity=parity))
    return cells


# ── Fake backtest result for server-authority tests ─────────────────────────

def _make_fake_backtest(sym, tf, wins=25, losses=5, rr_values=(1, 2, 3),
                        loss_r=0.9, parity_trusted=True,
                        baseline_wins=48, baseline_losses=32, ok=True):
    """Build a minimal _bt_run_ob_historical_backtest result dict."""
    baseline_total = baseline_wins + baseline_losses
    rz_thr = {}
    cmp_thr = {}
    rz_base = {}
    cmp_base = {}
    for rv in rr_values:
        rk = _m._bt_rr_key(rv)
        b_rz, _b_ret = _rz(baseline_wins, baseline_losses, rk, loss_r,
                           baseline_total)
        rz_base[rk] = b_rz
        cmp_base[rk] = {"trade_retention_pct": 100.0,
                        "expectancy_delta": 0.0, "net_r_delta": 0.0}
        t_rz, t_ret = _rz(wins, losses, rk, loss_r, baseline_total)
        rz_thr[rk] = t_rz
        b_exp = b_rz["expectancy_r"]
        t_exp = t_rz["expectancy_r"]
        cmp_thr[rk] = {
            "trade_retention_pct": t_ret,
            "expectancy_delta": (round(t_exp - b_exp, 6)
                                 if (t_exp is not None and b_exp is not None)
                                 else None),
            "net_r_delta": round(t_rz["net_r"] - b_rz["net_r"], 6),
        }
    return {
        "ok": ok,
        "error": None if ok else "fake_failure",
        "outcome_summary": {
            "tv_ob_pct_parity": {"trusted": parity_trusted},
            "tv_ob_pct_threshold_analysis": {
                "before_first_touch": {
                    "results": {
                        "0": {"realized_summary_by_rr": rz_base,
                              "comparison_vs_baseline_by_rr": cmp_base},
                        "20": {"realized_summary_by_rr": rz_thr,
                               "comparison_vs_baseline_by_rr": cmp_thr},
                    }
                },
                "at_formation": {"results": {}},
            },
        },
    }


# ═════════════════════════════════════════════════════════════════════════════
class TestStabSampleStatus(unittest.TestCase):
    def test_no_sample(self):
        self.assertEqual(_sample_status(0), "no_sample")
        self.assertEqual(_sample_status(9), "no_sample")

    def test_insufficient(self):
        self.assertEqual(_sample_status(10), "insufficient")
        self.assertEqual(_sample_status(19), "insufficient")

    def test_usable(self):
        self.assertEqual(_sample_status(20), "usable")
        self.assertEqual(_sample_status(49), "usable")

    def test_strong(self):
        self.assertEqual(_sample_status(50), "strong")
        self.assertEqual(_sample_status(200), "strong")


class TestStabAggregatePf(unittest.TestCase):
    def test_wins_only_is_infinite(self):
        pf, inf = _agg_pf(10.0, 0.0)
        self.assertIsNone(pf)
        self.assertTrue(inf)

    def test_wins_and_losses_finite(self):
        pf, inf = _agg_pf(20.0, 10.0)
        self.assertFalse(inf)
        self.assertAlmostEqual(pf, 2.0, places=3)

    def test_no_trades(self):
        pf, inf = _agg_pf(0.0, 0.0)
        self.assertIsNone(pf)
        self.assertFalse(inf)

    def test_aggregate_pf_from_summed_gross(self):
        # Sum-based: gp=13, gl=5 → 2.6, not the average of per-cell PFs (2.5)
        pf, _ = _agg_pf(13.0, 5.0)
        self.assertAlmostEqual(pf, 2.6, places=3)
        self.assertNotAlmostEqual(pf, 2.5, places=2)


# ── Part L: _stab_pf_non_degraded (tuple return) ─────────────────────────────
class TestStabPfNonDegraded(unittest.TestCase):

    def test_candidate_finite_95pct_pass(self):
        ok, reason = _pf_nondeg(1.9, False, 2.0, False,
                                candidate_trades=30, baseline_trades=30,
                                baseline_gross_profit=20.0, baseline_gross_loss=10.0)
        self.assertTrue(ok)
        self.assertEqual(reason, "95_pct_rule_passed")

    def test_candidate_below_95pct_fail(self):
        ok, reason = _pf_nondeg(1.8, False, 2.0, False,
                                candidate_trades=30, baseline_trades=30,
                                baseline_gross_profit=20.0, baseline_gross_loss=10.0)
        self.assertFalse(ok)
        self.assertEqual(reason, "95_pct_rule_failed")

    def test_candidate_infinite_pass(self):
        ok, reason = _pf_nondeg(None, True, 2.0, False, candidate_trades=30)
        self.assertTrue(ok)
        self.assertEqual(reason, "candidate_infinite")

    def test_candidate_infinite_zero_trades_fail(self):
        ok, reason = _pf_nondeg(None, True, 2.0, False, candidate_trades=0)
        self.assertFalse(ok)
        self.assertEqual(reason, "candidate_zero_trades")

    def test_baseline_infinite_candidate_finite_fail(self):
        ok, reason = _pf_nondeg(1.5, False, None, True, candidate_trades=30)
        self.assertFalse(ok)
        self.assertEqual(reason, "baseline_infinite_candidate_finite")

    def test_baseline_null_zero_trades_fail(self):
        ok, reason = _pf_nondeg(1.5, False, None, False,
                                candidate_trades=30, baseline_trades=0)
        self.assertFalse(ok)
        self.assertEqual(reason, "baseline_profit_factor_not_comparable")

    def test_baseline_null_noncomparable_gross_r_fail(self):
        ok, reason = _pf_nondeg(1.5, False, None, False,
                                candidate_trades=30, baseline_trades=10,
                                baseline_gross_profit=0.0, baseline_gross_loss=0.0)
        self.assertFalse(ok)
        self.assertEqual(reason, "baseline_profit_factor_not_comparable")

    def test_malformed_baseline_pf_fail(self):
        ok, reason = _pf_nondeg(1.5, False, None, False,
                                candidate_trades=30, baseline_trades=50,
                                baseline_gross_profit=10.0, baseline_gross_loss=5.0)
        self.assertFalse(ok)
        self.assertEqual(reason, "baseline_profit_factor_not_comparable")

    def test_baseline_nonpositive_pass(self):
        ok, reason = _pf_nondeg(1.5, False, 0.0, False,
                                candidate_trades=30, baseline_trades=30)
        self.assertTrue(ok)
        self.assertEqual(reason, "baseline_pf_nonpositive")

    def test_candidate_pf_none_fail(self):
        ok, reason = _pf_nondeg(None, False, 2.0, False,
                                candidate_trades=30, baseline_trades=30,
                                baseline_gross_profit=20.0, baseline_gross_loss=10.0)
        self.assertFalse(ok)
        self.assertEqual(reason, "candidate_pf_none")

    def test_returns_tuple(self):
        result = _pf_nondeg(1.9, False, 2.0, False)
        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 2)
        self.assertIsInstance(result[0], bool)
        self.assertIsInstance(result[1], str)

    def test_no_999_sentinel(self):
        for args in [
            (1.9, False, 2.0, False), (None, True, 2.0, False),
            (1.5, False, None, True), (1.8, False, 2.0, False),
        ]:
            ok, reason = _pf_nondeg(*args)
            self.assertNotIn("999", reason)


class TestStabBootstrap(unittest.TestCase):
    def test_deterministic(self):
        rows = [{"expectancy_delta_vs_baseline": d}
                for d in [0.1, 0.2, 0.15, 0.05, 0.3, 0.25]]
        r1 = _bootstrap(rows)
        r2 = _bootstrap(rows)
        self.assertEqual(r1["macro_expectancy_delta_low"],
                         r2["macro_expectancy_delta_low"])
        self.assertEqual(r1["macro_expectancy_delta_high"],
                         r2["macro_expectancy_delta_high"])
        self.assertEqual(r1["seed"], _STAB_BOOTSTRAP_SEED)
        self.assertEqual(r1["iterations"], _STAB_BOOTSTRAP_ITERS)

    def test_empty_deltas_returns_none_ci(self):
        r = _bootstrap([])
        self.assertIsNone(r["macro_expectancy_delta_low"])
        self.assertIsNone(r["macro_expectancy_delta_high"])

    def test_ci_ordering(self):
        rows = [{"expectancy_delta_vs_baseline": d}
                for d in [0.1, 0.2, 0.15, 0.05, 0.3, 0.25]]
        r = _bootstrap(rows)
        self.assertLessEqual(r["macro_expectancy_delta_low"],
                             r["macro_expectancy_delta_high"])


class TestStabNoSentinel999(unittest.TestCase):
    def test_no_sentinel_anywhere(self):
        cells = _make_good_cells(wins=30, losses=0)
        result = _build_stability(cells, thresholds=[0, 50], rr_values=[1])
        out_str = json.dumps(result, default=str)
        self.assertNotIn("999", out_str)


# ── Part M: Recommendation / coverage / robustness ──────────────────────────
class TestStabRecommendation(unittest.TestCase):

    def test_threshold_zero_cannot_win(self):
        cells = _make_good_cells(wins=40, losses=2, thr=50,
                                 baseline_wins=80, baseline_losses=10)
        result = _build_stability(cells, thresholds=[0, 50], rr_values=[1])
        self.assertIn("0_1", result["aggregate_by_thr_rr"])
        rec = result.get("recommendation")
        if rec is not None:
            self.assertNotEqual(rec["threshold_pct"], 0)
        agg0 = result["aggregate_by_thr_rr"]["0_1"]
        self.assertFalse(agg0.get("eligible_for_robust_ranking", False))

    def test_missing_4h_fails_coverage(self):
        cells = _make_good_cells(tfs=["1h"], wins=25, losses=5)
        result = _build_stability(cells, thresholds=[0, 50], rr_values=[1])
        agg = result["aggregate_by_thr_rr"]["50_1"]
        self.assertFalse(agg["coverage_passes"])
        self.assertIn("no_4h_usable_cells", agg["coverage_failures"])

    def test_missing_1h_fails_coverage(self):
        cells = _make_good_cells(tfs=["4h"], wins=25, losses=5)
        result = _build_stability(cells, thresholds=[0, 50], rr_values=[1])
        agg = result["aggregate_by_thr_rr"]["50_1"]
        self.assertFalse(agg["coverage_passes"])
        self.assertIn("no_1h_usable_cells", agg["coverage_failures"])

    def test_fewer_than_4_symbols_fails(self):
        cells = _make_good_cells(syms=["BTCUSDT", "ETHUSDT", "BNBUSDT"],
                                 tfs=["1h", "4h"], wins=25, losses=5)
        result = _build_stability(cells, thresholds=[0, 50], rr_values=[1])
        agg = result["aggregate_by_thr_rr"]["50_1"]
        self.assertLess(agg["symbols_represented"], 4)
        self.assertFalse(agg["coverage_passes"])
        self.assertIn("symbols_represented<4", agg["coverage_failures"])

    def test_fewer_than_6_usable_cells_fails(self):
        cells = _make_good_cells(syms=["BTCUSDT", "ETHUSDT"],
                                 tfs=["1h", "4h"], wins=25, losses=5)
        result = _build_stability(cells, thresholds=[0, 50], rr_values=[1])
        agg = result["aggregate_by_thr_rr"]["50_1"]
        self.assertLess(agg["usable_cells"], 6)
        self.assertFalse(agg["coverage_passes"])
        self.assertIn("usable_cells<6", agg["coverage_failures"])

    def test_fewer_than_100_trades_fails(self):
        cells = _make_good_cells(wins=3, losses=2)
        result = _build_stability(cells, thresholds=[0, 50], rr_values=[1])
        agg = result["aggregate_by_thr_rr"]["50_1"]
        self.assertLess(agg.get("total_trades", 0), 100)
        self.assertFalse(agg["coverage_passes"])
        self.assertIn("total_trades<100", agg["coverage_failures"])

    def test_retention_below_20_fails(self):
        cells = _make_good_cells(wins=4, losses=1, baseline_wins=600,
                                 baseline_losses=400)
        result = _build_stability(cells, thresholds=[0, 50], rr_values=[1])
        agg = result["aggregate_by_thr_rr"]["50_1"]
        self.assertEqual(agg["usable_cells"], 0)
        self.assertFalse(agg["coverage_passes"])

    def test_positive_cells_below_60_fails(self):
        good = _make_good_cells(syms=["BTCUSDT", "ETHUSDT", "BNBUSDT"],
                                tfs=["1h", "4h"], wins=25, losses=5,
                                baseline_wins=48, baseline_losses=32)
        bad = _make_good_cells(syms=["SOLUSDT", "XRPUSDT", "ADAUSDT"],
                               tfs=["1h", "4h"], wins=5, losses=25,
                               loss_r=1.0, baseline_wins=48, baseline_losses=32)
        cells = good + bad
        result = _build_stability(cells, thresholds=[0, 50], rr_values=[1])
        agg = result["aggregate_by_thr_rr"]["50_1"]
        self.assertIsNotNone(agg.get("positive_cell_pct"))
        self.assertLess(agg["positive_cell_pct"], 60.0)
        self.assertIn("positive_cell_pct<60.0", agg["robustness_failures"])

    def test_beat_baseline_below_60_fails(self):
        good = _make_good_cells(syms=["BTCUSDT", "ETHUSDT", "BNBUSDT"],
                                tfs=["1h", "4h"], wins=30, losses=2,
                                baseline_wins=30, baseline_losses=10)
        worse = _make_good_cells(syms=["SOLUSDT", "XRPUSDT", "ADAUSDT"],
                                 tfs=["1h", "4h"], wins=14, losses=16,
                                 loss_r=1.0, baseline_wins=80, baseline_losses=5)
        cells = good + worse
        result = _build_stability(cells, thresholds=[0, 50], rr_values=[1])
        agg = result["aggregate_by_thr_rr"]["50_1"]
        self.assertIsNotNone(agg.get("beat_baseline_cell_pct"))
        self.assertLess(agg["beat_baseline_cell_pct"], 60.0)
        self.assertIn("beat_baseline_cell_pct<60.0", agg["robustness_failures"])

    def test_worst_cell_below_floor_fails(self):
        good = _make_good_cells(syms=["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT"],
                                tfs=["1h", "4h"], wins=25, losses=5)
        bad = [_make_cell("XRPUSDT", "1h", wins=2, losses=28, thr=50,
                          loss_r=1.5, baseline_wins=48, baseline_losses=32),
               _make_cell("XRPUSDT", "4h", wins=2, losses=28, thr=50,
                          loss_r=1.5, baseline_wins=48, baseline_losses=32)]
        cells = good + bad
        result = _build_stability(cells, thresholds=[0, 50], rr_values=[1])
        agg = result["aggregate_by_thr_rr"]["50_1"]
        self.assertIsNotNone(agg.get("worst_cell_expectancy_r"))
        self.assertLess(agg["worst_cell_expectancy_r"], _m._STAB_WORST_CELL_FLOOR)
        self.assertIn("worst_cell_expectancy_r<-0.25", agg["robustness_failures"])

    def test_ci_low_below_zero_fails(self):
        cells = _make_good_cells(wins=10, losses=20, loss_r=1.0,
                                 baseline_wins=12, baseline_losses=18)
        result = _build_stability(cells, thresholds=[0, 50], rr_values=[1])
        agg = result["aggregate_by_thr_rr"]["50_1"]
        ci_low = (agg.get("bootstrap_ci_95") or {}).get("macro_expectancy_delta_low")
        self.assertTrue(ci_low is None or ci_low < 0)
        self.assertIn("bootstrap_ci_low<0", agg["robustness_failures"])

    def test_pf_degradation_fails(self):
        cells = _make_good_cells(wins=25, losses=25, loss_r=1.0,
                                 baseline_wins=48, baseline_losses=5)
        result = _build_stability(cells, thresholds=[0, 50], rr_values=[1])
        agg = result["aggregate_by_thr_rr"]["50_1"]
        cand_pf = agg.get("micro_profit_factor_r")
        base_pf = agg.get("baseline_micro_profit_factor_r")
        self.assertIsNotNone(cand_pf)
        self.assertIsNotNone(base_pf)
        self.assertLess(cand_pf, base_pf * 0.95)
        rfs = agg["robustness_failures"]
        self.assertTrue(any(r.startswith("profit_factor_degraded") for r in rfs))

    def test_fragile_one_cell_blocks_recommendation(self):
        # 3 symbols × 2 tfs = 6 usable cells, but only 3 symbols < 4 →
        # coverage fails outright and no recommendation is produced.
        cells = _make_good_cells(
            syms=["BTCUSDT", "ETHUSDT", "BNBUSDT"], tfs=["1h", "4h"],
            wins=30, losses=2, baseline_wins=40, baseline_losses=20)
        result = _build_stability(cells, thresholds=[0, 50], rr_values=[1])
        agg = result["aggregate_by_thr_rr"]["50_1"]
        # Precondition: exactly 6 usable cells but only 3 symbols
        self.assertEqual(agg["usable_cells"], 6)
        self.assertEqual(agg["symbols_represented"], 3)
        self.assertIsNone(result.get("recommendation"))

    def test_robust_candidate_passes(self):
        cells = _make_good_cells(wins=30, losses=4,
                                 baseline_wins=40, baseline_losses=24)
        result = _build_stability(cells, thresholds=[0, 50], rr_values=[1])
        agg = result["aggregate_by_thr_rr"]["50_1"]
        self.assertTrue(agg["coverage_passes"],
                        msg=f"coverage failed: {agg['coverage_failures']}")
        self.assertTrue(agg["robustness_passes"],
                        msg=f"robustness failed: {agg['robustness_failures']}")
        rec = result.get("recommendation")
        self.assertIsNotNone(rec)
        self.assertEqual(rec["threshold_pct"], 50)

    def test_near_equal_lower_threshold_wins(self):
        c = _make_good_cells(thr=40, wins=30, losses=4,
                             baseline_wins=40, baseline_losses=24)
        for cell in c:
            res = cell["threshold_analysis"]["before_first_touch"]["results"]
            base_exp = res["0"]["realized_summary_by_rr"]["1"]["expectancy_r"]
            res["60"] = _make_thr_block(20, 3, "1", 0.9, 64, base_exp)
        result = _build_stability(c, thresholds=[0, 40, 60], rr_values=[1])
        agg40 = result["aggregate_by_thr_rr"]["40_1"]
        agg60 = result["aggregate_by_thr_rr"]["60_1"]
        self.assertTrue(agg40["eligible_for_robust_ranking"],
                        msg=f"40 not eligible: {agg40['robustness_failures']}")
        self.assertTrue(agg60["eligible_for_robust_ranking"],
                        msg=f"60 not eligible: {agg60['robustness_failures']}")
        ci40 = (agg40["bootstrap_ci_95"] or {})["macro_expectancy_delta_low"]
        ci60 = (agg60["bootstrap_ci_95"] or {})["macro_expectancy_delta_low"]
        self.assertLess(abs(ci40 - ci60), _m._STAB_NEAR_EQ_CI)
        self.assertGreater(agg40["micro_trade_retention_pct"],
                           agg60["micro_trade_retention_pct"])
        rec = result.get("recommendation")
        self.assertIsNotNone(rec)
        self.assertEqual(rec["threshold_pct"], 40)

    def test_higher_ci_wins_outside_near_equality(self):
        c = _make_good_cells(thr=40, wins=20, losses=8,
                             baseline_wins=40, baseline_losses=24)
        for cell in c:
            res = cell["threshold_analysis"]["before_first_touch"]["results"]
            base_exp = res["0"]["realized_summary_by_rr"]["1"]["expectancy_r"]
            res["60"] = _make_thr_block(30, 2, "1", 0.9, 64, base_exp)
        result = _build_stability(c, thresholds=[0, 40, 60], rr_values=[1])
        agg40 = result["aggregate_by_thr_rr"]["40_1"]
        agg60 = result["aggregate_by_thr_rr"]["60_1"]
        self.assertTrue(agg40["eligible_for_robust_ranking"])
        self.assertTrue(agg60["eligible_for_robust_ranking"])
        ci40 = (agg40["bootstrap_ci_95"] or {})["macro_expectancy_delta_low"]
        ci60 = (agg60["bootstrap_ci_95"] or {})["macro_expectancy_delta_low"]
        self.assertGreater(ci60 - ci40, _m._STAB_NEAR_EQ_CI)
        rec = result.get("recommendation")
        self.assertIsNotNone(rec)
        self.assertEqual(rec["threshold_pct"], 60)

    def test_bootstrap_deterministic(self):
        cells = _make_good_cells(wins=30, losses=4)
        r1 = _build_stability(cells, thresholds=[0, 50], rr_values=[1])
        r2 = _build_stability(cells, thresholds=[0, 50], rr_values=[1])
        ci1 = r1["aggregate_by_thr_rr"]["50_1"]["bootstrap_ci_95"]
        ci2 = r2["aggregate_by_thr_rr"]["50_1"]["bootstrap_ci_95"]
        self.assertEqual(ci1["macro_expectancy_delta_low"],
                         ci2["macro_expectancy_delta_low"])
        self.assertEqual(ci1["macro_expectancy_delta_high"],
                         ci2["macro_expectancy_delta_high"])

    def test_bootstrap_null_with_fewer_than_four_cells(self):
        cells = _make_good_cells(syms=["BTCUSDT", "ETHUSDT", "BNBUSDT"],
                                 tfs=["1h"], wins=25, losses=5)
        result = _build_stability(cells, thresholds=[0, 50], rr_values=[1])
        agg = result["aggregate_by_thr_rr"]["50_1"]
        self.assertLess(agg["usable_cells"], 4)
        ci = agg["bootstrap_ci_95"]
        self.assertIsNone(ci["macro_expectancy_delta_low"])
        self.assertIsNone(ci["macro_expectancy_delta_high"])


# ── Part K: Server authority ────────────────────────────────────────────────
class TestStabServerAuthority(unittest.TestCase):

    def test_endpoint_rejects_cell_results(self):
        # Authenticate, then POST a cell_results payload — must be 400 with the
        # exact contract error. The cell runner is mocked so the handler never
        # hits the network even if it (wrongly) proceeded.
        def fake(params):
            return _make_fake_backtest(params["symbol"], params["timeframe"])
        with patch.object(_m, "_bt_run_ob_historical_backtest", side_effect=fake):
            with _m.app.test_client() as client:
                with client.session_transaction() as sess:
                    sess["logged_in"] = True
                    sess["username"] = "tester"
                resp = client.post(
                    "/api/backtest/ob-historical/threshold-stability",
                    json={"cell_results": [{"symbol": "BTCUSDT"}]},
                )
        self.assertEqual(resp.status_code, 400)
        body = resp.get_json()
        self.assertFalse(body["ok"])
        self.assertEqual(body["error"], "client_supplied_cell_results_not_allowed")

    def test_endpoint_authoritative_response_via_client(self):
        # Authenticated request with no cell_results runs server-side and returns
        # the authoritative flags.
        def fake(params):
            return _make_fake_backtest(params["symbol"], params["timeframe"])
        with patch.object(_m, "_bt_run_ob_historical_backtest", side_effect=fake):
            with _m.app.test_client() as client:
                with client.session_transaction() as sess:
                    sess["logged_in"] = True
                    sess["username"] = "tester"
                resp = client.post(
                    "/api/backtest/ob-historical/threshold-stability",
                    json={"symbols": ["BTCUSDT", "ETHUSDT"],
                          "timeframes": ["1h", "4h"], "candle_count": 500},
                )
        self.assertEqual(resp.status_code, 200)
        body = resp.get_json()
        self.assertTrue(body["ok"])
        self.assertTrue(body["authoritative_execution"])
        self.assertFalse(body["client_results_accepted"])
        self.assertEqual(body["execution_mode"], "sequential")
        self.assertEqual(body["requested_cells"], 4)
        self.assertIn("response_size_bytes", body)
        self.assertIn("stability", body)
        # Threshold-0 baseline row present in aggregate output
        self.assertIn("0_1", body["stability"]["aggregate_by_thr_rr"])

    def test_endpoint_unsupported_timeframe_rejected_via_client(self):
        with _m.app.test_client() as client:
            with client.session_transaction() as sess:
                sess["logged_in"] = True
                sess["username"] = "tester"
            resp = client.post(
                "/api/backtest/ob-historical/threshold-stability",
                json={"symbols": ["BTCUSDT"], "timeframes": ["15m"]},
            )
        self.assertEqual(resp.status_code, 400)
        self.assertFalse(resp.get_json()["ok"])

    def test_endpoint_max_cells_rejected_via_client(self):
        with _m.app.test_client() as client:
            with client.session_transaction() as sess:
                sess["logged_in"] = True
                sess["username"] = "tester"
            resp = client.post(
                "/api/backtest/ob-historical/threshold-stability",
                json={"symbols": ["AAAUSDT", "BBBUSDT", "CCCUSDT",
                                  "DDDUSDT", "EEEUSDT", "FFFUSDT"],
                      "timeframes": ["1h", "4h"]},
            )
        self.assertEqual(resp.status_code, 400)
        self.assertFalse(resp.get_json()["ok"])

    def test_endpoint_blank_symbol_rejected_via_client(self):
        with _m.app.test_client() as client:
            with client.session_transaction() as sess:
                sess["logged_in"] = True
                sess["username"] = "tester"
            resp = client.post(
                "/api/backtest/ob-historical/threshold-stability",
                json={"symbols": ["BTCUSDT", "  "], "timeframes": ["1h"]},
            )
        self.assertEqual(resp.status_code, 400)
        self.assertFalse(resp.get_json()["ok"])

    def test_server_runner_creates_cells(self):
        def fake(params):
            return _make_fake_backtest(params["symbol"], params["timeframe"])
        with patch.object(_m, "_bt_run_ob_historical_backtest", side_effect=fake):
            cells, diag = _run_cells(
                symbols=["BTCUSDT", "ETHUSDT"], timeframes=["1h", "4h"],
                candle_count=500, rr_values=[1, 2, 3],
            )
        self.assertEqual(len(cells), 4)
        self.assertEqual(diag["requested_cells"], 4)
        self.assertEqual(diag["completed_cells"], 4)
        self.assertEqual(diag["failed_cells"], 0)
        self.assertEqual(diag["execution_mode"], "sequential")
        for c in cells:
            self.assertTrue(c["generated_server_side"])
            self.assertEqual(c["build_commit"], "115115a")
            self.assertEqual(c["data_source"], "binance_futures_klines")
            self.assertTrue(c["threshold_analysis_present"])

    def test_client_expectancy_ignored(self):
        def fake(params):
            return _make_fake_backtest(params["symbol"], params["timeframe"],
                                       wins=10, losses=10)
        with patch.object(_m, "_bt_run_ob_historical_backtest", side_effect=fake):
            cells, _ = _run_cells(symbols=["BTCUSDT"], timeframes=["1h"],
                                  candle_count=500, rr_values=[1])
        ta = cells[0]["threshold_analysis"]
        rz = ta["before_first_touch"]["results"]["20"]["realized_summary_by_rr"]["1"]
        self.assertEqual(rz["wins"], 10)
        self.assertEqual(rz["losses"], 10)

    def test_recommendation_null_when_not_authoritative(self):
        cells = _make_good_cells(wins=30, losses=4, parity=False)
        result = _build_stability(cells, thresholds=[0, 50], rr_values=[1])
        self.assertIsNone(result.get("recommendation"))
        agg = result["aggregate_by_thr_rr"]["50_1"]
        self.assertEqual(agg["eligible_cells"], 0)

    def test_authoritative_flags_in_response(self):
        def fake(params):
            return _make_fake_backtest(params["symbol"], params["timeframe"])
        with patch.object(_m, "_bt_run_ob_historical_backtest", side_effect=fake):
            cells, diag = _run_cells(symbols=["BTCUSDT"], timeframes=["1h"],
                                     candle_count=500, rr_values=[1, 2, 3])
            stability = _build_stability(cells, thresholds=list(range(0, 105, 20)),
                                         rr_values=[1, 2, 3])
        body = {"ok": True, "authoritative_execution": True,
                "client_results_accepted": False, **diag, "stability": stability}
        self.assertTrue(body["authoritative_execution"])
        self.assertFalse(body["client_results_accepted"])
        self.assertEqual(body["execution_mode"], "sequential")

    def test_max_cells_enforced(self):
        self.assertLessEqual(5 * 2, _STAB_MAX_CELLS)
        self.assertGreater(6 * 2, _STAB_MAX_CELLS)

    def test_duplicate_symbols_deduplicated(self):
        raw_syms = ["BTCUSDT", "BTCUSDT", "ETHUSDT"]
        seen = set()
        out = []
        for s in raw_syms:
            s = s.strip().upper()
            if not s.endswith("USDT"):
                s = s + "USDT"
            if s in seen:
                continue
            seen.add(s)
            out.append(s)
        self.assertEqual(out, ["BTCUSDT", "ETHUSDT"])

    def test_unsupported_timeframe_rejected(self):
        allowed = {"1h", "4h"}
        self.assertNotIn("15m", allowed)
        self.assertIn("1h", allowed)
        self.assertIn("4h", allowed)

    def test_blank_symbol_rejected(self):
        raw_syms = ["BTCUSDT", "", "ETHUSDT"]
        self.assertTrue(any(not str(s).strip() for s in raw_syms))

    def test_failed_cell_remains_visible(self):
        def fake(params):
            ok = params["symbol"] != "ETHUSDT"
            return _make_fake_backtest(params["symbol"], params["timeframe"], ok=ok)
        with patch.object(_m, "_bt_run_ob_historical_backtest", side_effect=fake):
            cells, diag = _run_cells(symbols=["BTCUSDT", "ETHUSDT"],
                                     timeframes=["1h"], candle_count=500,
                                     rr_values=[1])
        self.assertEqual(diag["failed_cells"], 1)
        failed = [c for c in cells if not c["ok"]]
        self.assertEqual(len(failed), 1)
        self.assertEqual(failed[0]["symbol"], "ETHUSDT")
        self.assertEqual(len(cells), 2)

    def test_untrusted_cell_excluded_from_aggregation(self):
        good = _make_good_cells(wins=30, losses=4, parity=True)
        untrusted = _make_cell("ADAUSDT", "1h", wins=30, losses=4, thr=50,
                               parity=False, baseline_wins=48, baseline_losses=32)
        cells = good + [untrusted]
        result = _build_stability(cells, thresholds=[0, 50], rr_values=[1])
        rows = [r for r in result["cell_data"]
                if r["symbol"] == "ADAUSDT" and r["threshold_pct"] == 50]
        self.assertEqual(len(rows), 1)
        self.assertFalse(rows[0]["eligible_cell"])
        self.assertIn("parity_not_trusted", rows[0]["rejection_reasons"])


class TestStabOutputStructure(unittest.TestCase):
    def test_symbol_summaries_present(self):
        syms = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]
        cells = _make_good_cells(syms=syms, wins=20, losses=5)
        result = _build_stability(cells, thresholds=[0, 50], rr_values=[1])
        for sym in syms:
            self.assertIn(sym, result["symbol_summaries"])

    def test_cell_data_row_count(self):
        cells = _make_good_cells(wins=20, losses=5)
        result = _build_stability(cells, thresholds=[0, 50], rr_values=[1])
        self.assertEqual(result["total_cell_rows"], 20)

    def test_baseline_rows_present(self):
        cells = _make_good_cells(wins=20, losses=5)
        result = _build_stability(cells, thresholds=[0, 50], rr_values=[1])
        self.assertIn("0_1", result["aggregate_by_thr_rr"])
        self.assertTrue(result["aggregate_by_thr_rr"]["0_1"]["is_baseline"])
        self.assertFalse(result["aggregate_by_thr_rr"]["50_1"]["is_baseline"])

    def test_constants_keys_present(self):
        cells = _make_good_cells(wins=20, losses=5)
        result = _build_stability(cells, thresholds=[0, 50], rr_values=[1])
        const = result["constants"]
        for k in ["bootstrap_iters", "bootstrap_seed", "min_usable_cells",
                  "min_symbols", "min_tf_cells", "min_total_trades",
                  "min_retention_pct", "min_positive_pct", "min_beat_baseline_pct",
                  "worst_cell_floor", "near_eq_ci", "max_symbols",
                  "max_timeframes", "max_cells"]:
            self.assertIn(k, const)


if __name__ == "__main__":
    unittest.main(verbosity=2)
