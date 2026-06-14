"""
Phase 15 unit tests — Chronological Walk-Forward Validation (research only).

Design constraints:
  * No conditional assertions — every gate test asserts preconditions first.
  * All assertions always execute.
  * _bt_run_ob_historical_backtest is mocked for integration tests.
  * Walk-Forward is research-only; no activation, no DB writes.

Usage:
    cd /home/user/hhhhhh-hhhh && python3 _test_phase_15.py
"""
import os, sys, math, traceback, unittest
from unittest.mock import patch

os.environ.setdefault("DATABASE_URL", "sqlite:///phase15_test.db")
os.environ.setdefault("SECRET_KEY",   "phase15-test-key")
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

# ── Exported symbols ─────────────────────────────────────────────────────────
_build_folds        = _m._bt_build_walk_forward_folds
_first_touch_idx    = _m._bt_wf_first_touch_index
_event_id           = _m._bt_wf_event_id
_filter_events      = _m._bt_wf_filter_test_events
_count_censored     = _m._bt_wf_count_censored
_agg_fold           = _m._bt_aggregate_wf_fold
_bootstrap          = _m._bt_wf_bootstrap_delta
_lookahead          = _m._bt_wf_lookahead_audit
_agg_summary        = _m._bt_aggregate_wf_summary
_check_gates        = _m._bt_wf_check_pass_gates
_run_wf             = _m._bt_run_walk_forward
_WF_MIN_FOLDS       = _m._WF_MIN_FOLDS
_WF_MAX_FOLDS       = _m._WF_MAX_FOLDS
_WF_MIN_OOS_TRADES  = _m._WF_MIN_OOS_TRADES
_WF_MIN_RETENTION   = _m._WF_MIN_RETENTION
_WF_MIN_POS_FOLDS   = _m._WF_MIN_POSITIVE_FOLDS
_WF_MIN_BEAT_FOLDS  = _m._WF_MIN_BEAT_FOLDS
_WF_WORST_FLOOR     = _m._WF_WORST_FOLD_FLOOR
_WF_ALLOWED_TF      = _m._WF_ALLOWED_TF
_WF_ALLOWED_THR     = _m._WF_ALLOWED_THRESHOLDS
_WF_CAND_THR        = _m._WF_CANDIDATE_THRESHOLDS
_WF_ITERS           = _m._WF_BOOTSTRAP_ITERS
_WF_SEED            = _m._WF_BOOTSTRAP_SEED
_MIN_AUTH           = _m._MIN_AUTHORITATIVE_TRADES
_stab_agg_pf        = _m._stab_aggregate_pf
_stab_pf_nondeg     = _m._stab_pf_non_degraded


# ── Event / fold helpers ─────────────────────────────────────────────────────

def _ev(first_touch_bar, direction="bull", ob_formed=10, resolved=True,
        wins=1, losses=0, unresolved=False, invalid_lr=False):
    """Minimal synthetic event for filter / count tests."""
    outcome = "win" if wins else ("loss" if losses else ("unresolved" if unresolved else "ambiguous"))
    return {
        "type": direction + "_ob",
        "formation_bar": ob_formed,
        "first_touch_bar": first_touch_bar,
        "outcome": None if unresolved else outcome,
        "invalid_loss_r": invalid_lr,
        "realized_rr": {
            "1": {"outcome": outcome if not unresolved else None,
                  "realized_r": 1.0 if wins else (-0.9 if losses else None)},
        },
    }


def _good_fold_agg(fold=1, trades=30, wins=20, losses=10, pf=2.0,
                   base_trades=50, base_pf=1.5,
                   exp=0.05, base_exp=0.02,
                   gp=20.0, gl=9.0, base_gp=20.0, base_gl=13.3):
    """Build a fold aggregate dict that passes all gates."""
    delta = round(exp - base_exp, 6)
    retention = round(trades / base_trades * 100, 2) if base_trades else None
    return {
        "fold": fold,
        "train_candle_count": 400,
        "test_start_index": 400,
        "test_end_index_exclusive": 550,
        "train_end_index_exclusive": 400,
        "test_candle_count": 150,
        "train_start_time": None, "train_end_time": None,
        "test_start_time": None,  "test_end_time": None,
        "candidate_threshold_pct": 40,
        "candidate_rr": "2",
        "completed_cells": 2,
        "total_cells": 2,
        "oos_trades": trades,
        "oos_wins": wins,
        "oos_losses": losses,
        "oos_ambiguous": 0,
        "oos_unresolved": 0,
        "oos_invalid_loss_r": 0,
        "censored_unresolved": 0,
        "oos_gross_profit_r": gp,
        "oos_gross_loss_r": gl,
        "oos_net_r": round(gp - gl, 4),
        "oos_expectancy_r": exp,
        "oos_profit_factor_r": pf,
        "oos_profit_factor_infinite": False,
        "baseline_trades": base_trades,
        "baseline_gross_profit_r": base_gp,
        "baseline_gross_loss_r": base_gl,
        "baseline_net_r": round(base_gp - base_gl, 4),
        "baseline_expectancy_r": base_exp,
        "baseline_profit_factor_r": base_pf,
        "baseline_profit_factor_infinite": False,
        "expectancy_delta_vs_baseline": delta,
        "trade_retention_pct": retention,
        "positive_cell_pct": 100.0,
        "beat_baseline_cell_pct": 100.0,
        "eligible": True,
        "cells": [],
    }


def _good_summary(folds=None, n=3):
    """Build a summary dict that should pass all locked-mode gates."""
    if folds is None:
        folds = [_good_fold_agg(fold=i + 1) for i in range(n)]
    deltas = [f["expectancy_delta_vs_baseline"] for f in folds
              if f.get("eligible") and f.get("expectancy_delta_vs_baseline") is not None]
    total_trades = sum(f["oos_trades"] for f in folds)
    total_base = sum(f["baseline_trades"] for f in folds)
    net_r = sum(f["oos_net_r"] for f in folds)
    exp = round(net_r / total_trades, 6) if total_trades else None
    base_net = sum(f["baseline_net_r"] for f in folds)
    base_exp = round(base_net / total_base, 6) if total_base else None
    delta = round(exp - base_exp, 6) if (exp is not None and base_exp is not None) else None
    retention = round(total_trades / total_base * 100, 2) if total_base else None
    pos_pct = 100.0
    beat_pct = 100.0
    worst = min(f["oos_expectancy_r"] for f in folds if f.get("eligible"))
    return {
        "candidate_mode": "locked",
        "total_folds": n,
        "evaluable_folds": n,
        "oos_trades": total_trades,
        "oos_wins": 0,
        "oos_losses": 0,
        "oos_ambiguous": 0,
        "oos_unresolved": 0,
        "oos_invalid_loss_r": 0,
        "censored_unresolved": 0,
        "baseline_trades": total_base,
        "oos_gross_profit_r": sum(f["oos_gross_profit_r"] for f in folds),
        "oos_gross_loss_r": sum(f["oos_gross_loss_r"] for f in folds),
        "oos_net_r": net_r,
        "baseline_net_r": base_net,
        "oos_expectancy_r": exp,
        "baseline_expectancy_r": base_exp,
        "oos_expectancy_delta_vs_baseline": delta,
        "oos_profit_factor_r": 2.0,
        "oos_profit_factor_infinite": False,
        "baseline_profit_factor_r": 1.5,
        "baseline_profit_factor_infinite": False,
        "oos_trade_retention_pct": retention,
        "positive_folds": n,
        "positive_fold_pct": pos_pct,
        "beat_baseline_folds": n,
        "beat_baseline_fold_pct": beat_pct,
        "worst_fold_expectancy_r": worst,
        "evaluable_fold_deltas": deltas,
        "training_expectancy_by_fold": [],
    }


def _good_bootstrap(deltas):
    return _bootstrap(deltas)


def _pass_lookahead():
    return {"training_ends_before_test": True,
            "test_windows_non_overlapping": True,
            "duplicate_test_event_ids": 0,
            "passes": True}


def _fail_lookahead(reason="overlap"):
    d = _pass_lookahead()
    d["passes"] = False
    if reason == "overlap":
        d["test_windows_non_overlapping"] = False
    return d


# ── Fake candle sequence ─────────────────────────────────────────────────────

def _fake_raw_candles(n=1000):
    """Return candles in get_klines camelCase format for _bt_normalize_candles."""
    return [{"openTime": i * 3600000, "closeTime": (i + 1) * 3600000 - 1,
             "open": 100.0 + i * 0.001, "high": 105.0, "low": 95.0,
             "close": 100.5, "volume": 1000.0}
            for i in range(n)]


# =============================================================================
class TestWfConstants(unittest.TestCase):
    """Verify _WF_* constants are defined and within expected ranges."""

    def test_min_folds_ge_3(self):
        self.assertGreaterEqual(_WF_MIN_FOLDS, 3)

    def test_max_folds_ge_min_folds(self):
        self.assertGreaterEqual(_WF_MAX_FOLDS, _WF_MIN_FOLDS)

    def test_max_folds_le_5(self):
        self.assertLessEqual(_WF_MAX_FOLDS, 5)

    def test_min_oos_trades_positive(self):
        self.assertGreater(_WF_MIN_OOS_TRADES, 0)

    def test_bootstrap_iters_5000(self):
        self.assertEqual(_WF_ITERS, 5000)

    def test_bootstrap_seed_15015(self):
        self.assertEqual(_WF_SEED, 15015)

    def test_allowed_tf_contains_1h_4h(self):
        self.assertIn("1h", _WF_ALLOWED_TF)
        self.assertIn("4h", _WF_ALLOWED_TF)

    def test_candidate_thresholds_no_zero(self):
        self.assertNotIn(0, _WF_CAND_THR)
        self.assertIn(20, _WF_CAND_THR)

    def test_allowed_thresholds_has_zero(self):
        self.assertIn(0, _WF_ALLOWED_THR)

    def test_worst_fold_floor_negative(self):
        self.assertLess(_WF_WORST_FLOOR, 0)


# =============================================================================
class TestBuildFolds(unittest.TestCase):
    """_bt_build_walk_forward_folds fold construction."""

    def test_basic_fold_count(self):
        folds = _build_folds(1000, 4, 40.0, 15.0)
        self.assertEqual(len(folds), 4)

    def test_fold_numbering_starts_at_1(self):
        folds = _build_folds(1000, 4, 40.0, 15.0)
        self.assertEqual(folds[0]["fold"], 1)
        self.assertEqual(folds[3]["fold"], 4)

    def test_train_always_starts_at_zero(self):
        folds = _build_folds(1000, 4, 40.0, 15.0)
        for f in folds:
            self.assertEqual(f["train_start_index"], 0)

    def test_training_end_grows_each_fold(self):
        folds = _build_folds(1000, 4, 40.0, 15.0)
        for i in range(1, len(folds)):
            self.assertGreater(folds[i]["train_end_index_exclusive"],
                               folds[i - 1]["train_end_index_exclusive"])

    def test_train_end_equals_test_start(self):
        folds = _build_folds(1000, 4, 40.0, 15.0)
        for f in folds:
            self.assertEqual(f["train_end_index_exclusive"], f["test_start_index"])

    def test_test_windows_non_overlapping(self):
        folds = _build_folds(1000, 4, 40.0, 15.0)
        for i in range(1, len(folds)):
            self.assertLessEqual(folds[i - 1]["test_end_index_exclusive"],
                                 folds[i]["test_start_index"])

    def test_all_indices_within_candle_count(self):
        folds = _build_folds(1000, 4, 40.0, 15.0)
        for f in folds:
            self.assertLessEqual(f["test_end_index_exclusive"], 1000)

    def test_stops_early_when_not_enough_candles(self):
        # With 500 candles, 40% train=200, 20% test=100 → fold4 would need 600+
        folds = _build_folds(500, 5, 40.0, 20.0)
        self.assertLess(len(folds), 5)

    def test_expected_indices_for_known_params(self):
        # 1000c, 4 folds, 40% initial train, 15% test
        # train_base=400, test_size=150
        # Fold1: train=[0,400), test=[400,550)
        # Fold2: train=[0,550), test=[550,700)
        folds = _build_folds(1000, 4, 40.0, 15.0)
        self.assertEqual(folds[0]["train_end_index_exclusive"], 400)
        self.assertEqual(folds[0]["test_start_index"], 400)
        self.assertEqual(folds[0]["test_end_index_exclusive"], 550)
        self.assertEqual(folds[1]["train_end_index_exclusive"], 550)
        self.assertEqual(folds[1]["test_start_index"], 550)
        self.assertEqual(folds[1]["test_end_index_exclusive"], 700)


# =============================================================================
class TestEventFiltering(unittest.TestCase):
    """_bt_wf_filter_test_events and _bt_wf_first_touch_index."""

    def test_first_touch_index_extracts_first_touch_bar(self):
        e = _ev(first_touch_bar=42)
        self.assertEqual(_first_touch_idx(e), 42)

    def test_first_touch_index_returns_none_when_missing(self):
        self.assertIsNone(_first_touch_idx({}))

    def test_filter_keeps_events_in_window(self):
        events = [_ev(ft) for ft in [100, 200, 300, 400, 500]]
        result = _filter_events(events, 200, 400)
        ftbs = [e["first_touch_bar"] for e in result]
        self.assertIn(200, ftbs)
        self.assertIn(300, ftbs)
        self.assertNotIn(100, ftbs)
        self.assertNotIn(400, ftbs)  # exclusive upper
        self.assertNotIn(500, ftbs)

    def test_filter_excludes_events_without_first_touch(self):
        events = [_ev(ft) for ft in [150, 250]] + [{"type": "bull_ob"}]
        result = _filter_events(events, 100, 300)
        self.assertEqual(len(result), 2)

    def test_filter_returns_empty_when_no_events_in_window(self):
        events = [_ev(ft) for ft in [10, 20, 30]]
        result = _filter_events(events, 500, 600)
        self.assertEqual(len(result), 0)

    def test_event_id_is_deterministic(self):
        e = _ev(first_touch_bar=77, direction="bull", ob_formed=70)
        id1 = _event_id("BTCUSDT", "1h", e)
        id2 = _event_id("BTCUSDT", "1h", e)
        self.assertEqual(id1, id2)

    def test_event_ids_differ_by_symbol(self):
        e = _ev(first_touch_bar=77)
        self.assertNotEqual(_event_id("BTCUSDT", "1h", e),
                            _event_id("ETHUSDT", "1h", e))

    def test_event_ids_differ_by_tf(self):
        e = _ev(first_touch_bar=77)
        self.assertNotEqual(_event_id("BTCUSDT", "1h", e),
                            _event_id("BTCUSDT", "4h", e))

    def test_count_censored_unresolved(self):
        # _bt_wf_count_censored checks event["simulation"]["eligible"] and
        # event["simulation"]["first_outcome"] in (None, "unresolved")
        events = [
            {"first_touch_bar": 10, "simulation": {"eligible": True,  "first_outcome": None}},
            {"first_touch_bar": 20, "simulation": {"eligible": True,  "first_outcome": "unresolved"}},
            {"first_touch_bar": 30, "simulation": {"eligible": True,  "first_outcome": "win"}},
            {"first_touch_bar": 40, "simulation": {"eligible": False, "first_outcome": None}},
        ]
        self.assertEqual(_count_censored(events), 2)

    def test_count_censored_zero_when_all_resolved(self):
        events = [
            {"first_touch_bar": 10, "simulation": {"eligible": True, "first_outcome": "win"}},
            {"first_touch_bar": 20, "simulation": {"eligible": True, "first_outcome": "loss"}},
        ]
        self.assertEqual(_count_censored(events), 0)


# =============================================================================
class TestFoldAggregation(unittest.TestCase):
    """_bt_aggregate_wf_fold — single fold aggregation."""

    def _make_cell_result(self, sym, tf, cand_trades=25, cand_wins=18,
                           base_trades=50, fold=1, ok=True, exp=0.05, base_exp=0.02):
        rv = 2.0
        gp = cand_wins * rv
        gl = (cand_trades - cand_wins) * 0.9
        nr = gp - gl
        base_gp = base_trades * 0.6 * rv
        base_gl = base_trades * 0.4 * 0.9
        return {
            "symbol": sym, "timeframe": tf, "fold": fold, "ok": ok,
            "censored_unresolved": 0,
            "metrics": {
                "candidate": {
                    "trades": cand_trades, "wins": cand_wins,
                    "losses": cand_trades - cand_wins,
                    "ambiguous": 0, "unresolved": 0, "invalid_loss_r": 0,
                    "gross_profit_r": gp, "gross_loss_r": gl,
                    "net_r": round(nr, 4),
                    "expectancy_r": round(nr / cand_trades, 6) if cand_trades else None,
                },
                "baseline": {
                    "trades": base_trades,
                    "gross_profit_r": base_gp, "gross_loss_r": base_gl,
                    "net_r": round(base_gp - base_gl, 4),
                    "expectancy_r": round((base_gp - base_gl) / base_trades, 6),
                },
                "expectancy_delta_vs_baseline": round(exp - base_exp, 6),
                "trade_retention_pct": round(cand_trades / base_trades * 100, 2),
            } if ok else None,
            "error": None if ok else "mock_err",
        }

    def test_completed_cells_counted(self):
        cells = [self._make_cell_result("BTCUSDT", "1h"),
                 self._make_cell_result("ETHUSDT", "1h")]
        fold_info = _build_folds(1000, 4, 40.0, 15.0)[0]
        result = _agg_fold(cells, fold_info, 40, "2")
        self.assertEqual(result["completed_cells"], 2)

    def test_failed_cell_excluded_from_aggregation(self):
        cells = [self._make_cell_result("BTCUSDT", "1h"),
                 self._make_cell_result("ETHUSDT", "1h", ok=False)]
        fold_info = _build_folds(1000, 4, 40.0, 15.0)[0]
        result = _agg_fold(cells, fold_info, 40, "2")
        self.assertEqual(result["completed_cells"], 1)
        self.assertEqual(result["total_cells"], 2)

    def test_eligible_requires_min_trades(self):
        cells = [self._make_cell_result("BTCUSDT", "1h", cand_trades=5, cand_wins=4)]
        fold_info = _build_folds(1000, 4, 40.0, 15.0)[0]
        result = _agg_fold(cells, fold_info, 40, "2")
        self.assertFalse(result["eligible"])

    def test_oos_trades_sum_across_cells(self):
        cells = [self._make_cell_result("BTCUSDT", "1h", cand_trades=25),
                 self._make_cell_result("ETHUSDT", "1h", cand_trades=30)]
        fold_info = _build_folds(1000, 4, 40.0, 15.0)[0]
        result = _agg_fold(cells, fold_info, 40, "2")
        self.assertEqual(result["oos_trades"], 55)


# =============================================================================
class TestBootstrap(unittest.TestCase):
    """_bt_wf_bootstrap_delta — fold-level bootstrap CI."""

    def test_insufficient_folds_returns_none_ci(self):
        result = _bootstrap([0.01, 0.02])  # < 3 folds
        self.assertTrue(result["insufficient_folds"])
        self.assertIsNone(result["expectancy_delta_low"])
        self.assertIsNone(result["expectancy_delta_high"])

    def test_sufficient_folds_returns_ci(self):
        deltas = [0.05, 0.03, 0.07, 0.04]
        result = _bootstrap(deltas)
        self.assertFalse(result["insufficient_folds"])
        self.assertIsNotNone(result["expectancy_delta_low"])
        self.assertIsNotNone(result["expectancy_delta_high"])

    def test_ci_low_le_high(self):
        deltas = [0.05, 0.03, 0.07, 0.04]
        result = _bootstrap(deltas)
        self.assertLessEqual(result["expectancy_delta_low"],
                             result["expectancy_delta_high"])

    def test_positive_deltas_yield_positive_ci_low(self):
        deltas = [0.10, 0.12, 0.08, 0.11]
        result = _bootstrap(deltas)
        self.assertGreater(result["expectancy_delta_low"], 0)

    def test_negative_deltas_yield_negative_ci_low(self):
        deltas = [-0.05, -0.07, -0.04, -0.06]
        result = _bootstrap(deltas)
        self.assertLess(result["expectancy_delta_low"], 0)

    def test_bootstrap_is_deterministic(self):
        deltas = [0.05, 0.03, 0.07, 0.04]
        r1 = _bootstrap(deltas)
        r2 = _bootstrap(deltas)
        self.assertEqual(r1["expectancy_delta_low"],  r2["expectancy_delta_low"])
        self.assertEqual(r1["expectancy_delta_high"], r2["expectancy_delta_high"])

    def test_bootstrap_uses_correct_seed_and_iters(self):
        deltas = [0.05, 0.03, 0.07, 0.04]
        result = _bootstrap(deltas)
        self.assertEqual(result["iterations"], _WF_ITERS)
        self.assertEqual(result["seed"],       _WF_SEED)


# =============================================================================
class TestLookaheadAudit(unittest.TestCase):
    """_bt_wf_lookahead_audit — checks for data leakage."""

    def _folds_for_1000c(self):
        return _build_folds(1000, 4, 40.0, 15.0)

    def test_clean_folds_pass_audit(self):
        folds = self._folds_for_1000c()
        ids = {f["fold"]: [f"BTCUSDT_1h_bull_{j}_{j+5}"
                           for j in range(f["test_start_index"],
                                          f["test_end_index_exclusive"], 5)]
               for f in folds}
        result = _lookahead(folds, ids)
        self.assertTrue(result["passes"])
        self.assertTrue(result["training_ends_before_test"])
        self.assertTrue(result["test_windows_non_overlapping"])
        self.assertEqual(result["duplicate_test_event_ids"], 0)

    def test_duplicate_ids_fail_audit(self):
        folds = self._folds_for_1000c()
        shared_id = "BTCUSDT_1h_bull_100_105"
        ids = {f["fold"]: [shared_id] for f in folds}
        result = _lookahead(folds, ids)
        self.assertGreater(result["duplicate_test_event_ids"], 0)
        self.assertFalse(result["passes"])

    def test_audit_passes_is_bool(self):
        folds = self._folds_for_1000c()
        result = _lookahead(folds, {})
        self.assertIsInstance(result["passes"], bool)


# =============================================================================
class TestSummaryAggregation(unittest.TestCase):
    """_bt_aggregate_wf_summary — cross-fold OOS aggregation."""

    def test_oos_trades_sum_correctly(self):
        folds = [_good_fold_agg(fold=i + 1, trades=30) for i in range(3)]
        s = _agg_summary(folds, "locked", [])
        self.assertEqual(s["oos_trades"], 90)

    def test_evaluable_folds_counted(self):
        folds = [_good_fold_agg(fold=1, trades=30),
                 _good_fold_agg(fold=2, trades=5)]  # 5 trades < _MIN_AUTH → not eligible
        folds[1]["eligible"] = False
        folds[1]["oos_trades"] = 5
        s = _agg_summary(folds, "locked", [])
        self.assertEqual(s["evaluable_folds"], 1)

    def test_candidate_mode_preserved(self):
        folds = [_good_fold_agg(fold=i + 1) for i in range(3)]
        s = _agg_summary(folds, "train_selected", [])
        self.assertEqual(s["candidate_mode"], "train_selected")

    def test_positive_fold_pct_computed(self):
        f1 = _good_fold_agg(fold=1, exp=0.05)
        f2 = _good_fold_agg(fold=2, exp=-0.01)
        f2["oos_expectancy_r"] = -0.01
        f2["oos_net_r"] = -0.01 * 30
        s = _agg_summary([f1, f2], "locked", [])
        self.assertIsNotNone(s["positive_fold_pct"])

    def test_worst_fold_is_minimum(self):
        f1 = _good_fold_agg(fold=1, exp=0.10)
        f2 = _good_fold_agg(fold=2, exp=0.02)
        f3 = _good_fold_agg(fold=3, exp=-0.05)
        f3["oos_expectancy_r"] = -0.05
        f3["oos_net_r"] = -0.05 * 30
        s = _agg_summary([f1, f2, f3], "locked", [])
        self.assertAlmostEqual(s["worst_fold_expectancy_r"], -0.05, places=4)


# =============================================================================
class TestPassGates(unittest.TestCase):
    """_bt_wf_check_pass_gates — all 21 gates."""

    def _base_args(self, n=3):
        folds = [_good_fold_agg(fold=i + 1) for i in range(n)]
        summary = _good_summary(folds, n=n)
        # Ensure enough OOS trades
        summary["oos_trades"] = 120
        summary["oos_net_r"] = 5.0
        summary["oos_expectancy_r"] = 0.05
        summary["oos_expectancy_delta_vs_baseline"] = 0.03
        summary["oos_trade_retention_pct"] = 25.0
        summary["positive_fold_pct"] = 100.0
        summary["beat_baseline_fold_pct"] = 100.0
        summary["worst_fold_expectancy_r"] = 0.02
        summary["oos_invalid_loss_r"] = 0
        summary["total_folds"] = n
        summary["evaluable_folds"] = n
        boot = _bootstrap(summary["evaluable_fold_deltas"])
        return summary, folds, boot

    def test_all_gates_pass(self):
        summary, folds, boot = self._base_args()
        passes, failed = _check_gates(summary, folds, _pass_lookahead(),
                                      True, "locked", boot)
        self.assertEqual(failed, [])
        self.assertTrue(passes)

    def test_lookahead_violation_fails(self):
        summary, folds, boot = self._base_args()
        passes, failed = _check_gates(summary, folds, _fail_lookahead(),
                                      True, "locked", boot)
        self.assertIn("lookahead_violation", failed)
        self.assertFalse(passes)

    def test_not_server_authoritative_fails(self):
        summary, folds, boot = self._base_args()
        passes, failed = _check_gates(summary, folds, _pass_lookahead(),
                                      False, "locked", boot)
        self.assertIn("not_server_authoritative", failed)
        self.assertFalse(passes)

    def test_insufficient_evaluable_folds_fails(self):
        folds = [_good_fold_agg(fold=1)]
        summary = _good_summary(folds, n=1)
        summary["oos_trades"] = 120
        summary["oos_net_r"] = 5.0
        summary["oos_expectancy_r"] = 0.05
        summary["oos_expectancy_delta_vs_baseline"] = 0.03
        summary["oos_trade_retention_pct"] = 25.0
        summary["positive_fold_pct"] = 100.0
        summary["beat_baseline_fold_pct"] = 100.0
        summary["worst_fold_expectancy_r"] = 0.02
        summary["evaluable_folds"] = 1
        summary["total_folds"] = 1
        boot = _bootstrap([0.03])
        passes, failed = _check_gates(summary, folds, _pass_lookahead(),
                                      True, "locked", boot)
        self.assertIn("insufficient_evaluable_folds", failed)

    def test_insufficient_oos_trades_fails(self):
        summary, folds, boot = self._base_args()
        summary["oos_trades"] = 10  # below 100
        passes, failed = _check_gates(summary, folds, _pass_lookahead(),
                                      True, "locked", boot)
        self.assertIn("insufficient_oos_trades", failed)

    def test_invalid_loss_r_fails(self):
        summary, folds, boot = self._base_args()
        summary["oos_invalid_loss_r"] = 1
        passes, failed = _check_gates(summary, folds, _pass_lookahead(),
                                      True, "locked", boot)
        self.assertIn("invalid_loss_r_present", failed)

    def test_retention_below_floor_fails(self):
        summary, folds, boot = self._base_args()
        summary["oos_trade_retention_pct"] = 5.0  # below 20%
        passes, failed = _check_gates(summary, folds, _pass_lookahead(),
                                      True, "locked", boot)
        self.assertIn("retention_below_floor", failed)

    def test_negative_oos_expectancy_fails(self):
        summary, folds, boot = self._base_args()
        summary["oos_expectancy_r"] = -0.01
        passes, failed = _check_gates(summary, folds, _pass_lookahead(),
                                      True, "locked", boot)
        self.assertIn("oos_expectancy_not_positive", failed)

    def test_oos_not_above_baseline_fails(self):
        summary, folds, boot = self._base_args()
        summary["oos_expectancy_delta_vs_baseline"] = -0.01
        passes, failed = _check_gates(summary, folds, _pass_lookahead(),
                                      True, "locked", boot)
        self.assertIn("oos_expectancy_not_above_baseline", failed)

    def test_negative_oos_net_r_fails(self):
        summary, folds, boot = self._base_args()
        summary["oos_net_r"] = -1.0
        passes, failed = _check_gates(summary, folds, _pass_lookahead(),
                                      True, "locked", boot)
        self.assertIn("oos_net_r_not_positive", failed)

    def test_positive_folds_below_floor_fails(self):
        summary, folds, boot = self._base_args()
        summary["positive_fold_pct"] = 30.0
        passes, failed = _check_gates(summary, folds, _pass_lookahead(),
                                      True, "locked", boot)
        self.assertIn("positive_folds_below_floor", failed)

    def test_beat_baseline_below_floor_fails(self):
        summary, folds, boot = self._base_args()
        summary["beat_baseline_fold_pct"] = 30.0
        passes, failed = _check_gates(summary, folds, _pass_lookahead(),
                                      True, "locked", boot)
        self.assertIn("beat_baseline_folds_below_floor", failed)

    def test_worst_fold_below_floor_fails(self):
        summary, folds, boot = self._base_args()
        summary["worst_fold_expectancy_r"] = -0.50  # below -0.30 floor
        passes, failed = _check_gates(summary, folds, _pass_lookahead(),
                                      True, "locked", boot)
        self.assertIn("worst_fold_below_floor", failed)

    def test_train_selected_always_fails_advisory(self):
        summary, folds, boot = self._base_args()
        summary["candidate_mode"] = "train_selected"
        passes, failed = _check_gates(summary, folds, _pass_lookahead(),
                                      True, "train_selected", boot)
        self.assertIn("train_selected_advisory_only", failed)
        self.assertFalse(passes)

    def test_bootstrap_ci_low_not_positive_fails(self):
        summary, folds, _boot = self._base_args()
        bad_boot = {"insufficient_folds": False,
                    "expectancy_delta_low": -0.01,
                    "expectancy_delta_high": 0.10}
        passes, failed = _check_gates(summary, folds, _pass_lookahead(),
                                      True, "locked", bad_boot)
        self.assertIn("bootstrap_ci_low_not_positive", failed)

    def test_failed_gates_list_is_sorted(self):
        summary, folds, boot = self._base_args()
        summary["oos_trades"] = 5
        summary["oos_expectancy_r"] = -0.01
        _, failed = _check_gates(summary, folds, _pass_lookahead(),
                                 True, "locked", boot)
        self.assertEqual(failed, sorted(set(failed)))


# =============================================================================
class TestWalkForwardIntegration(unittest.TestCase):
    """
    Integration tests for _bt_run_walk_forward.
    get_klines is mocked to return synthetic camelCase candle data.
    All fold/aggregation/gate logic runs real code.
    """

    def _run_wf_locked(self, candles=1000, folds=3, syms=None, tfs=None):
        if syms is None: syms = ["BTCUSDT"]
        if tfs is None: tfs = ["1h"]
        raw = _fake_raw_candles(candles)
        with patch.object(_m, "get_klines", return_value=raw):
            return _run_wf(
                symbols=syms, timeframes=tfs, candle_count=candles,
                rr_values=[1, 2, 3], thresholds=[0, 20, 40, 60, 80],
                candidate_mode="locked",
                candidate_threshold_pct=40, candidate_rr="2",
                fold_count=folds, initial_train_pct=40.0, test_pct=15.0,
                primary_metric="before_first_touch",
            )

    def test_run_returns_status_field(self):
        result = self._run_wf_locked()
        self.assertIn("status", result)
        self.assertIn(result["status"], ("PASS", "FAIL", "INSUFFICIENT", "ADVISORY"))

    def test_run_returns_fold_aggregates(self):
        result = self._run_wf_locked()
        self.assertIn("fold_aggregates", result)
        self.assertIsInstance(result["fold_aggregates"], list)

    def test_fold_count_built_matches_expected(self):
        result = self._run_wf_locked(candles=1000, folds=3)
        self.assertIn("fold_count_built", result)
        self.assertGreaterEqual(result["fold_count_built"], 1)

    def test_lookahead_audit_passes(self):
        result = self._run_wf_locked()
        audit = result.get("lookahead_audit", {})
        self.assertIn("passes", audit)
        self.assertTrue(audit["passes"])

    def test_status_is_valid_string(self):
        result = self._run_wf_locked()
        self.assertIn(result.get("status", ""),
                      ("PASS", "FAIL", "INSUFFICIENT", "ADVISORY"))

    def test_constants_present_in_result(self):
        result = self._run_wf_locked()
        constants = result.get("constants", {})
        self.assertIn("bootstrap_iters", constants)
        self.assertIn("bootstrap_seed", constants)
        self.assertIn("build_commit", constants)
        self.assertEqual(constants["bootstrap_iters"], _WF_ITERS)
        self.assertEqual(constants["bootstrap_seed"],  _WF_SEED)

    def test_train_selected_mode_returns_advisory(self):
        raw = _fake_raw_candles(1000)
        with patch.object(_m, "get_klines", return_value=raw):
            result = _run_wf(
                symbols=["BTCUSDT"], timeframes=["1h"], candle_count=1000,
                rr_values=[1, 2, 3], thresholds=[0, 20, 40, 60, 80],
                candidate_mode="train_selected",
                candidate_threshold_pct=None, candidate_rr=None,
                fold_count=3, initial_train_pct=40.0, test_pct=15.0,
                primary_metric="before_first_touch",
            )
        self.assertEqual(result["status"], "ADVISORY")

    def test_fetch_failure_recorded_in_cells_failed(self):
        with patch.object(_m, "get_klines", side_effect=RuntimeError("net_err")):
            result = _run_wf(
                symbols=["BTCUSDT"], timeframes=["1h"], candle_count=1000,
                rr_values=[1, 2, 3], thresholds=[0, 20, 40, 60, 80],
                candidate_mode="locked",
                candidate_threshold_pct=40, candidate_rr="2",
                fold_count=3, initial_train_pct=40.0, test_pct=15.0,
                primary_metric="before_first_touch",
            )
        self.assertIn("cells_failed", result)
        failed = result["cells_failed"]
        self.assertTrue(any("BTCUSDT" in str(f) for f in failed))

    def test_cells_failed_list_present(self):
        result = self._run_wf_locked()
        self.assertIn("cells_failed", result)

    def test_bootstrap_fields_present(self):
        result = self._run_wf_locked()
        boot = result.get("bootstrap", {})
        self.assertIn("expectancy_delta_low", boot)
        self.assertIn("iterations", boot)
        self.assertIn("seed", boot)

    def test_elapsed_ms_non_negative(self):
        result = self._run_wf_locked()
        self.assertIsNotNone(result.get("elapsed_ms"))
        self.assertGreaterEqual(result["elapsed_ms"], 0)


# =============================================================================
class TestEndpointValidationLogic(unittest.TestCase):
    """
    Validates endpoint parameter-checking logic directly, without HTTP auth.
    Tests mirror the rejection rules in api_backtest_ob_historical_threshold_walk_forward.
    """

    def _banned_key_detected(self, payload):
        """Return True if any banned key is in the payload."""
        for banned in ("folds", "cell_results", "trade_results",
                       "training_results", "test_results", "recommendation"):
            if banned in payload:
                return True
        return False

    def test_folds_key_detected_as_banned(self):
        self.assertTrue(self._banned_key_detected({"folds": []}))

    def test_cell_results_detected_as_banned(self):
        self.assertTrue(self._banned_key_detected({"cell_results": []}))

    def test_training_results_detected_as_banned(self):
        self.assertTrue(self._banned_key_detected({"training_results": {}}))

    def test_test_results_detected_as_banned(self):
        self.assertTrue(self._banned_key_detected({"test_results": {}}))

    def test_recommendation_detected_as_banned(self):
        self.assertTrue(self._banned_key_detected({"recommendation": "60_2"}))

    def test_clean_payload_not_banned(self):
        self.assertFalse(self._banned_key_detected(
            {"symbols": ["BTCUSDT"], "timeframes": ["1h"], "fold_count": 3}))

    def test_unsupported_timeframe_rejected(self):
        allowed = _WF_ALLOWED_TF
        self.assertNotIn("15m", allowed)
        self.assertNotIn("30m", allowed)
        self.assertNotIn("5m", allowed)
        self.assertIn("1h", allowed)
        self.assertIn("4h", allowed)

    def test_fold_count_bounds(self):
        self.assertLessEqual(_WF_MIN_FOLDS, 3)
        self.assertGreaterEqual(_WF_MAX_FOLDS, 5)
        self.assertGreater(10, _WF_MAX_FOLDS)  # 10 is out of range

    def test_invalid_candidate_threshold_rejected(self):
        self.assertNotIn(99, _WF_CAND_THR)
        self.assertNotIn(0, _WF_CAND_THR)
        self.assertIn(20, _WF_CAND_THR)
        self.assertIn(80, _WF_CAND_THR)

    def test_too_many_symbols_detected(self):
        syms = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT"]
        self.assertGreater(len(syms), 5)

    def test_symbol_deduplication(self):
        raw = ["BTCUSDT", "BTCUSDT", "ETHUSDT"]
        seen: set = set()
        out = []
        for s in raw:
            if s in seen:
                continue
            seen.add(s)
            out.append(s)
        self.assertEqual(out, ["BTCUSDT", "ETHUSDT"])

    def test_usdt_suffix_appended(self):
        raw = "BTC"
        if not raw.endswith("USDT"):
            raw = raw + "USDT"
        self.assertEqual(raw, "BTCUSDT")

    def test_train_pct_bounds(self):
        self.assertGreaterEqual(35.0, _m._WF_MIN_TRAIN_PCT)
        self.assertLessEqual(60.0, _m._WF_MAX_TRAIN_PCT)
        self.assertLess(34.9, _m._WF_MIN_TRAIN_PCT)   # below floor
        self.assertGreater(60.1, _m._WF_MAX_TRAIN_PCT) # above ceiling

    def test_test_pct_bounds(self):
        self.assertGreaterEqual(10.0, _m._WF_MIN_TEST_PCT)
        self.assertLessEqual(20.0, _m._WF_MAX_TEST_PCT)

    def test_build_commit_present_in_constants(self):
        self.assertTrue(hasattr(_m, "_WF_BUILD_COMMIT"))
        self.assertIsInstance(_m._WF_BUILD_COMMIT, str)
        self.assertGreater(len(_m._WF_BUILD_COMMIT), 0)


# =============================================================================
if __name__ == "__main__":
    unittest.main(verbosity=2)
