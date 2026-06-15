"""
Phase 15B unit tests — Walk-Forward Validation integrity hotfix (research only).

Design constraints:
  * No conditional assertions — every assertion always executes.
  * No external API calls; get_klines / _bt_run_walk_forward are mocked.
  * Walk-Forward is research-only; no activation, no DB writes.

Usage:
    cd /home/user/hhhhhh-hhhh && python3 _test_phase_15.py
"""
import os, sys, traceback, unittest
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
_prefix_backtest    = _m._bt_wf_run_prefix_backtest
_future_invariance  = _m._bt_wf_future_candle_invariance
_agg_fold           = _m._bt_aggregate_wf_fold
_bootstrap          = _m._bt_wf_bootstrap_delta
_lookahead          = _m._bt_wf_lookahead_audit
_agg_summary        = _m._bt_aggregate_wf_summary
_check_gates        = _m._bt_wf_check_pass_gates
_constancy          = _m._bt_wf_locked_candidate_constancy_audit
_ts_audit           = _m._bt_wf_training_selection_timestamp_audit
_adaptive           = _m._bt_wf_adaptive_process_check
_run_wf             = _m._bt_run_walk_forward
_WF_MIN_FOLDS       = _m._WF_MIN_FOLDS
_WF_MIN_OOS_TRADES  = _m._WF_MIN_OOS_TRADES
_WF_MIN_SYMBOLS     = _m._WF_MIN_SYMBOLS
_WF_MIN_TF_CELL     = _m._WF_MIN_TF_CELL_FOLDS
_MIN_AUTH           = _m._MIN_AUTHORITATIVE_TRADES


# ── Fold-aggregate / summary builders (new schema) ───────────────────────────

def _cell(symbol="BTCUSDT", tf="1h", trades=30, exp=0.05, base_exp=0.02,
          delta=0.03, eligible=True):
    return {
        "symbol": symbol, "timeframe": tf, "ok": True, "error": None,
        "baseline_trades": 50, "trades": trades, "wins": 20, "losses": 10,
        "ambiguous": 0, "unresolved": 0, "censored_unresolved": 0,
        "invalid_loss_r": 0,
        "expectancy_r": exp, "net_r": 1.5, "baseline_expectancy_r": base_exp,
        "expectancy_delta_vs_baseline": delta, "trade_retention_pct": 60.0,
        "candidate_profit_factor_r": 2.0, "candidate_profit_factor_infinite": False,
        "baseline_profit_factor_r": 1.5, "baseline_profit_factor_infinite": False,
        "baseline_net_r": 1.0, "net_r_delta": 0.5,
        "resolved_inside_window": trades,
        "prefix_contract_passed": True,
        "eligible": eligible, "rejection_reasons": [],
    }


def _fold_agg(fold=1, trades=120, exp=0.05, base_exp=0.02, pf=2.0, base_pf=1.5,
              gp=40.0, gl=10.0, base_gp=8.0, base_gl=5.0,
              symbols=("BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT"),
              tfs=("1h", "4h"), cells=None, eligible=True, cand_thr=40,
              cand_rr="2"):
    delta = round(exp - base_exp, 6)
    if cells is None:
        cells = []
        for s in symbols:
            for t in tfs:
                cells.append(_cell(symbol=s, tf=t, trades=trades,
                                   exp=exp, base_exp=base_exp, delta=delta))
    pf_nd_ok, pf_nd_reason = _m._stab_pf_non_degraded(
        pf, False, base_pf, False,
        candidate_trades=trades, baseline_trades=50,
        baseline_gross_profit=base_gp, baseline_gross_loss=base_gl,
    )
    # Distinct non-overlapping windows per fold for valid lookahead audit
    train_end = 400 + (fold - 1) * 150
    test_start = train_end
    test_end = test_start + 150
    return {
        "fold": fold, "train_start_index": 0,
        "train_end_index_exclusive": train_end, "test_start_index": test_start,
        "test_end_index_exclusive": test_end, "train_candle_count": train_end,
        "test_candle_count": 150,
        "train_start_time": None, "train_end_time": None,
        "test_start_time": None, "test_end_time": None,
        "candidate_threshold_pct": cand_thr, "candidate_rr": cand_rr,
        "requested_cells": len(cells), "completed_cells": len(cells),
        "trusted_cells": len(cells),
        "usable_cells": sum(1 for c in cells if c.get("eligible")),
        "symbols_represented": sorted(set(c["symbol"] for c in cells if c.get("eligible"))),
        "timeframes_represented": sorted(set(c["timeframe"] for c in cells if c.get("eligible"))),
        "baseline_trades": 50, "oos_trades": trades, "oos_wins": 20,
        "oos_losses": 10, "oos_ambiguous": 0, "oos_unresolved": 0,
        "oos_invalid_loss_r": 0, "censored_unresolved": 0,
        "resolved_inside_window": trades,
        "oos_gross_profit_r": gp, "oos_gross_loss_r": gl,
        "oos_net_r": round(gp - gl, 4),
        "oos_expectancy_r": exp,
        "candidate_micro_expectancy_r": exp,
        "candidate_macro_expectancy_r": exp,
        "baseline_expectancy_r": base_exp,
        "baseline_gross_profit_r": base_gp, "baseline_gross_loss_r": base_gl,
        "baseline_net_r": round(base_gp - base_gl, 4),
        "baseline_profit_factor_r": base_pf, "baseline_profit_factor_infinite": False,
        "oos_profit_factor_r": pf, "oos_profit_factor_infinite": False,
        "pf_non_degradation_passed": pf_nd_ok, "pf_non_degradation_reason": pf_nd_reason,
        "expectancy_delta_vs_baseline": delta,
        "micro_expectancy_delta": delta, "macro_expectancy_delta": delta,
        "trade_retention_pct": 60.0,
        "positive_cell_pct": 100.0, "beat_baseline_cell_pct": 100.0,
        "worst_cell_expectancy_r": exp, "training_expectancy_r": None,
        "eligible": eligible, "cells": cells,
    }


def _summary(folds, mode="locked", training_exp=None):
    return _agg_summary(folds, mode, training_exp or [None] * len(folds),
                        ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT"], ["1h", "4h"])


def _good_constancy(folds, thr=40, rr="2"):
    return _constancy(folds, thr, rr)


def _good_bootstrap(folds):
    deltas = [f["expectancy_delta_vs_baseline"] for f in folds if f.get("eligible")]
    return _bootstrap(deltas)


def _good_lookahead(folds):
    fold_defs = [{"fold": f["fold"],
                  "train_end_index_exclusive": f["train_end_index_exclusive"],
                  "test_start_index": f["test_start_index"],
                  "test_end_index_exclusive": f["test_end_index_exclusive"]}
                 for f in folds]
    cell_fold_results = {}
    for f in folds:
        key = "BTCUSDT_1h"
        cell_fold_results.setdefault(key, []).append({
            "symbol": "BTCUSDT", "timeframe": "1h", "fold": f["fold"],
            "events_outside_window": 0, "future_candle_invariance": True,
            "prefix_audits": [
                {"stage": "training", "prefix_contract_passed": True,
                 "symbol": "BTCUSDT", "timeframe": "1h", "fold": f["fold"]},
                {"stage": "test", "prefix_contract_passed": True,
                 "symbol": "BTCUSDT", "timeframe": "1h", "fold": f["fold"]},
            ],
        })
    return _lookahead(fold_defs, {}, cell_fold_results)


def _gates(summary, folds, lookahead=None, bootstrap=None, constancy=None,
           mode="locked", thr=40, tfs=None):
    lookahead = lookahead or _good_lookahead(folds)
    bootstrap = bootstrap or _good_bootstrap(folds)
    constancy = constancy or _good_constancy(folds, thr)
    return _check_gates(summary, folds, lookahead, True, mode, bootstrap,
                        constancy, thr, tfs or ["1h", "4h"])


# ── Synthetic candle helper ──────────────────────────────────────────────────

def _make_candles(n, base=50000.0):
    out = []
    t = 1_600_000_000_000
    for i in range(n):
        px = base + (i % 50) * 10.0
        out.append({
            "open_time": t + i * 3_600_000,
            "close_time": t + (i + 1) * 3_600_000 - 1,
            "open": px, "high": px + 30.0, "low": px - 30.0,
            "close": px + (5.0 if i % 2 else -5.0),
            "volume": 1000.0 + i,
        })
    return out


# ══════════════════════════════════════════════════════════════════════════════
# Part A — Fold construction (unchanged behaviour)
# ══════════════════════════════════════════════════════════════════════════════

class TestFolds(unittest.TestCase):
    def test_fold_count(self):
        folds = _build_folds(1000, 4, 40.0, 15.0)
        self.assertEqual(len(folds), 4)

    def test_boundaries_train_eq_test_start(self):
        for f in _build_folds(1000, 4, 40.0, 15.0):
            self.assertEqual(f["train_end_index_exclusive"], f["test_start_index"])

    def test_windows_non_overlapping(self):
        folds = _build_folds(1000, 4, 40.0, 15.0)
        for i in range(1, len(folds)):
            self.assertGreaterEqual(folds[i]["test_start_index"],
                                    folds[i - 1]["test_end_index_exclusive"])

    def test_expanding_train(self):
        folds = _build_folds(1000, 4, 40.0, 15.0)
        for i in range(1, len(folds)):
            self.assertGreater(folds[i]["train_end_index_exclusive"],
                               folds[i - 1]["train_end_index_exclusive"])


# ══════════════════════════════════════════════════════════════════════════════
# Part B — Event helpers
# ══════════════════════════════════════════════════════════════════════════════

class TestEventHelpers(unittest.TestCase):
    def _ev(self, ft, fb=10, direction="bull", outcome="win"):
        return {"type": direction + "_ob", "formation_bar": fb,
                "first_touch_bar": ft,
                "simulation": {"eligible": True, "first_outcome": outcome}}

    def test_filter_by_first_touch(self):
        evs = [self._ev(390), self._ev(410), self._ev(560)]
        kept = _filter_events(evs, 400, 550)
        self.assertEqual(len(kept), 1)
        self.assertEqual(kept[0]["first_touch_bar"], 410)

    def test_event_id_stable(self):
        e = self._ev(410)
        self.assertEqual(_event_id("BTCUSDT", "1h", e),
                         _event_id("BTCUSDT", "1h", e))

    def test_event_id_distinct(self):
        self.assertNotEqual(_event_id("BTCUSDT", "1h", self._ev(410)),
                            _event_id("ETHUSDT", "1h", self._ev(410)))

    def test_censored_count(self):
        evs = [self._ev(410, outcome="unresolved"), self._ev(411, outcome="win")]
        self.assertEqual(_count_censored(evs), 1)


# ══════════════════════════════════════════════════════════════════════════════
# Part C — Prefix backtest + contract
# ══════════════════════════════════════════════════════════════════════════════

class TestPrefixBacktest(unittest.TestCase):
    def test_prefix_contract_passed(self):
        candles = _make_candles(300)
        res = _prefix_backtest(candles, 200, "BTCUSDT", "1h", [1, 2, 3],
                               "before_first_touch", "training", 1)
        audit = res["prefix_audit"]
        self.assertTrue(audit["prefix_contract_passed"])
        self.assertEqual(audit["input_candle_count"], 200)
        self.assertEqual(audit["allowed_end_index_exclusive"], 200)

    def test_prefix_max_index(self):
        candles = _make_candles(300)
        res = _prefix_backtest(candles, 150, "BTCUSDT", "1h", [1, 2, 3],
                               "before_first_touch", "test", 2)
        self.assertEqual(res["prefix_audit"]["max_detector_index_read"], 149)
        self.assertEqual(res["prefix_audit"]["stage"], "test")

    def test_prefix_returns_threshold_analysis(self):
        candles = _make_candles(250)
        res = _prefix_backtest(candles, 200, "BTCUSDT", "1h", [1, 2, 3],
                               "before_first_touch", "training", 1)
        self.assertIn("before_first_touch", res["threshold_analysis"])


# ══════════════════════════════════════════════════════════════════════════════
# Part D — Future-candle invariance
# ══════════════════════════════════════════════════════════════════════════════

class TestFutureInvariance(unittest.TestCase):
    def test_invariance_passes_on_clean_data(self):
        candles = _make_candles(300)
        res = _prefix_backtest(candles, 200, "BTCUSDT", "1h", [1, 2, 3],
                               "before_first_touch", "test", 1)
        ids = [_event_id("BTCUSDT", "1h", e) for e in res["events"]]
        inv = _future_invariance(candles, 200, "BTCUSDT", "1h", [1, 2, 3],
                                 "before_first_touch", 1, ids, {})
        self.assertTrue(inv["passed"])
        self.assertEqual(inv["future_candles_added"], 20)

    def test_invariance_event_counts_match(self):
        candles = _make_candles(300)
        res = _prefix_backtest(candles, 220, "BTCUSDT", "1h", [1, 2, 3],
                               "before_first_touch", "test", 1)
        ids = [_event_id("BTCUSDT", "1h", e) for e in res["events"]]
        inv = _future_invariance(candles, 220, "BTCUSDT", "1h", [1, 2, 3],
                                 "before_first_touch", 1, ids, {})
        self.assertEqual(inv["original_event_count"], inv["rerun_event_count"])


# ══════════════════════════════════════════════════════════════════════════════
# Part E — Lookahead audit (new `passed` field)
# ══════════════════════════════════════════════════════════════════════════════

class TestLookahead(unittest.TestCase):
    def setUp(self):
        self.folds = [_fold_agg(fold=i + 1) for i in range(3)]

    def test_passed_field_present(self):
        la = _good_lookahead(self.folds)
        self.assertIn("passed", la)
        self.assertTrue(la["passed"])

    def test_all_subfields(self):
        la = _good_lookahead(self.folds)
        for k in ("fold_boundaries_valid", "training_ends_before_test_starts",
                  "test_windows_non_overlapping", "events_assigned_by_first_touch",
                  "training_prefix_isolated", "test_prefix_isolated",
                  "outcomes_capped_at_fold_end", "percentages_point_in_time",
                  "future_candle_invariance"):
            self.assertTrue(la[k], k)

    def test_boundary_violation(self):
        bad = [{"fold": 1, "train_end_index_exclusive": 100,
                "test_start_index": 120, "test_end_index_exclusive": 200}]
        la = _lookahead(bad, {}, {})
        self.assertFalse(la["fold_boundaries_valid"])
        self.assertFalse(la["passed"])

    def test_overlap_violation(self):
        bad = [
            {"fold": 1, "train_end_index_exclusive": 100, "test_start_index": 100,
             "test_end_index_exclusive": 250},
            {"fold": 2, "train_end_index_exclusive": 200, "test_start_index": 200,
             "test_end_index_exclusive": 350},
        ]
        la = _lookahead(bad, {}, {})
        self.assertFalse(la["test_windows_non_overlapping"])
        self.assertFalse(la["passed"])

    def test_duplicate_event_ids_detected(self):
        fold_defs = [{"fold": 1, "train_end_index_exclusive": 100,
                      "test_start_index": 100, "test_end_index_exclusive": 200}]
        la = _lookahead(fold_defs, {1: ["a", "a", "b"]}, {})
        self.assertEqual(la["duplicate_test_event_ids"], 1)
        self.assertFalse(la["passed"])

    def test_prefix_failure_detected(self):
        fold_defs = [{"fold": 1, "train_end_index_exclusive": 100,
                      "test_start_index": 100, "test_end_index_exclusive": 200}]
        cfr = {"BTCUSDT_1h": [{
            "symbol": "BTCUSDT", "timeframe": "1h", "fold": 1,
            "events_outside_window": 0, "future_candle_invariance": True,
            "prefix_audits": [
                {"stage": "training", "prefix_contract_passed": False,
                 "symbol": "BTCUSDT", "timeframe": "1h", "fold": 1},
            ],
        }]}
        la = _lookahead(fold_defs, {}, cfr)
        self.assertEqual(la["prefix_audits_failed"], 1)
        self.assertFalse(la["training_prefix_isolated"])
        self.assertFalse(la["passed"])

    def test_future_read_detected(self):
        fold_defs = [{"fold": 1, "train_end_index_exclusive": 100,
                      "test_start_index": 100, "test_end_index_exclusive": 200}]
        cfr = {"BTCUSDT_1h": [{
            "symbol": "BTCUSDT", "timeframe": "1h", "fold": 1,
            "events_outside_window": 0, "future_candle_invariance": False,
            "prefix_audits": [],
        }]}
        la = _lookahead(fold_defs, {}, cfr)
        self.assertEqual(la["future_candle_reads_detected"], 1)
        self.assertFalse(la["passed"])

    def test_events_outside_window_detected(self):
        fold_defs = [{"fold": 1, "train_end_index_exclusive": 100,
                      "test_start_index": 100, "test_end_index_exclusive": 200}]
        cfr = {"BTCUSDT_1h": [{
            "symbol": "BTCUSDT", "timeframe": "1h", "fold": 1,
            "events_outside_window": 3, "future_candle_invariance": True,
            "prefix_audits": [
                {"stage": "training", "prefix_contract_passed": True,
                 "symbol": "BTCUSDT", "timeframe": "1h", "fold": 1},
                {"stage": "test", "prefix_contract_passed": True,
                 "symbol": "BTCUSDT", "timeframe": "1h", "fold": 1},
            ],
        }]}
        la = _lookahead(fold_defs, {}, cfr)
        self.assertEqual(la["events_outside_test_window"], 3)
        self.assertFalse(la["passed"])


# ══════════════════════════════════════════════════════════════════════════════
# Part F — Summary fields
# ══════════════════════════════════════════════════════════════════════════════

class TestSummary(unittest.TestCase):
    def setUp(self):
        self.folds = [_fold_agg(fold=i + 1) for i in range(3)]
        self.s = _summary(self.folds)

    def test_symbols_represented(self):
        self.assertEqual(self.s["symbols_represented"], 4)

    def test_timeframes_represented(self):
        self.assertEqual(sorted(self.s["timeframes_represented"]), ["1h", "4h"])

    def test_eligible_1h_cell_folds(self):
        self.assertGreaterEqual(self.s["eligible_1h_cell_folds"], _WF_MIN_TF_CELL)

    def test_eligible_4h_cell_folds(self):
        self.assertGreaterEqual(self.s["eligible_4h_cell_folds"], _WF_MIN_TF_CELL)

    def test_positive_cell_fold_pct(self):
        self.assertEqual(self.s["positive_cell_fold_pct"], 100.0)

    def test_beat_baseline_cell_fold_pct(self):
        self.assertEqual(self.s["beat_baseline_cell_fold_pct"], 100.0)

    def test_training_trades_in_oos_zero(self):
        self.assertEqual(self.s["training_trades_in_oos"], 0)

    def test_locked_constants_flags(self):
        self.assertTrue(self.s["locked_threshold_constant"])
        self.assertTrue(self.s["locked_rr_constant"])


# ══════════════════════════════════════════════════════════════════════════════
# Part G — Constancy audit
# ══════════════════════════════════════════════════════════════════════════════

class TestConstancy(unittest.TestCase):
    def test_constant_passes(self):
        folds = [_fold_agg(fold=i + 1, cand_thr=40, cand_rr="2") for i in range(3)]
        ca = _constancy(folds, 40, "2")["locked_candidate_audit"]
        self.assertTrue(ca["passed"])
        self.assertTrue(ca["threshold_constant"])
        self.assertTrue(ca["rr_constant"])

    def test_varying_threshold_fails(self):
        folds = [_fold_agg(fold=1, cand_thr=40), _fold_agg(fold=2, cand_thr=60),
                 _fold_agg(fold=3, cand_thr=40)]
        ca = _constancy(folds, 40, "2")["locked_candidate_audit"]
        self.assertFalse(ca["threshold_constant"])
        self.assertFalse(ca["passed"])

    def test_varying_rr_fails(self):
        folds = [_fold_agg(fold=1, cand_rr="2"), _fold_agg(fold=2, cand_rr="3"),
                 _fold_agg(fold=3, cand_rr="2")]
        ca = _constancy(folds, 40, "2")["locked_candidate_audit"]
        self.assertFalse(ca["rr_constant"])
        self.assertFalse(ca["passed"])


# ══════════════════════════════════════════════════════════════════════════════
# Part H — Timestamp / training-selection audit
# ══════════════════════════════════════════════════════════════════════════════

class TestTimestampAudit(unittest.TestCase):
    def test_selection_before_test_passes(self):
        folds = _build_folds(1000, 3, 40.0, 15.0)
        sel = [{"fold": f["fold"],
                "selected": {"threshold_pct": 40, "rr": "2"}} for f in folds]
        out = _ts_audit(folds, sel)
        self.assertTrue(all(r["selection_completed_before_test"] for r in out))
        self.assertTrue(all(r["training_selection_passed"] for r in out))

    def test_fallback_flagged(self):
        folds = _build_folds(1000, 3, 40.0, 15.0)
        sel = [{"fold": f["fold"], "selected": None} for f in folds]
        out = _ts_audit(folds, sel)
        self.assertTrue(all(r["fallback_used"] for r in out))
        self.assertTrue(all(not r["training_selection_passed"] for r in out))


# ══════════════════════════════════════════════════════════════════════════════
# Part I — 30-gate evaluator
# ══════════════════════════════════════════════════════════════════════════════

class TestGates(unittest.TestCase):
    def setUp(self):
        self.folds = [_fold_agg(fold=i + 1) for i in range(3)]
        self.s = _summary(self.folds)

    def test_clean_passes(self):
        passes, failed = _gates(self.s, self.folds)
        self.assertTrue(passes, failed)
        self.assertEqual(failed, [])

    def test_insufficient_symbols_gate(self):
        folds = [_fold_agg(fold=i + 1, symbols=("BTCUSDT", "ETHUSDT"))
                 for i in range(3)]
        s = _summary(folds)
        _, failed = _gates(s, folds)
        self.assertIn("insufficient_symbols_represented", failed)

    def test_missing_1h_or_4h_gate(self):
        folds = [_fold_agg(fold=i + 1, tfs=("1h",)) for i in range(3)]
        s = _summary(folds)
        _, failed = _gates(s, folds, tfs=["1h", "4h"])
        self.assertIn("missing_1h_or_4h_coverage", failed)

    def test_insufficient_1h_cell_folds_gate(self):
        folds = [_fold_agg(fold=i + 1, tfs=("4h",)) for i in range(3)]
        s = _summary(folds)
        _, failed = _gates(s, folds)
        self.assertIn("insufficient_1h_cell_folds", failed)

    def test_insufficient_4h_cell_folds_gate(self):
        folds = [_fold_agg(fold=i + 1, tfs=("1h",)) for i in range(3)]
        s = _summary(folds)
        _, failed = _gates(s, folds)
        self.assertIn("insufficient_4h_cell_folds", failed)

    def test_insufficient_oos_trades(self):
        folds = [_fold_agg(fold=i + 1, trades=10) for i in range(3)]
        s = _summary(folds)
        s["oos_trades"] = 30
        _, failed = _gates(s, folds)
        self.assertIn("insufficient_oos_trades", failed)

    def test_locked_threshold_not_constant(self):
        folds = [_fold_agg(fold=1, cand_thr=40), _fold_agg(fold=2, cand_thr=60),
                 _fold_agg(fold=3, cand_thr=40)]
        s = _summary(folds)
        ca = _constancy(folds, 40, "2")
        _, failed = _gates(s, folds, constancy=ca)
        self.assertIn("locked_threshold_not_constant", failed)

    def test_locked_rr_not_constant(self):
        folds = [_fold_agg(fold=1, cand_rr="2"), _fold_agg(fold=2, cand_rr="3"),
                 _fold_agg(fold=3, cand_rr="2")]
        s = _summary(folds)
        ca = _constancy(folds, 40, "2")
        _, failed = _gates(s, folds, constancy=ca)
        self.assertIn("locked_rr_not_constant", failed)

    def test_threshold_zero_rejected(self):
        folds = [_fold_agg(fold=i + 1, cand_thr=0) for i in range(3)]
        s = _summary(folds)
        ca = _constancy(folds, 0, "2")
        _, failed = _gates(s, folds, constancy=ca, thr=0)
        self.assertIn("candidate_threshold_is_zero", failed)

    def test_training_trades_in_oos_gate(self):
        s = dict(self.s)
        s["training_trades_in_oos"] = 5
        _, failed = _gates(s, self.folds)
        self.assertIn("training_trades_in_oos", failed)

    def test_duplicate_event_ids_gate(self):
        fold_defs = [{"fold": f["fold"],
                      "train_end_index_exclusive": f["train_end_index_exclusive"],
                      "test_start_index": f["test_start_index"],
                      "test_end_index_exclusive": f["test_end_index_exclusive"]}
                     for f in self.folds]
        la = _lookahead(fold_defs, {1: ["x", "x"]}, {
            "BTCUSDT_1h": [{"symbol": "BTCUSDT", "timeframe": "1h", "fold": 1,
                            "events_outside_window": 0, "future_candle_invariance": True,
                            "prefix_audits": [
                                {"stage": "training", "prefix_contract_passed": True},
                                {"stage": "test", "prefix_contract_passed": True}]}]})
        _, failed = _gates(self.s, self.folds, lookahead=la)
        self.assertIn("duplicate_event_ids", failed)

    def test_prefix_audit_failed_gate(self):
        fold_defs = [{"fold": 1, "train_end_index_exclusive": 100,
                      "test_start_index": 100, "test_end_index_exclusive": 200}]
        cfr = {"BTCUSDT_1h": [{
            "symbol": "BTCUSDT", "timeframe": "1h", "fold": 1,
            "events_outside_window": 0, "future_candle_invariance": True,
            "prefix_audits": [{"stage": "test", "prefix_contract_passed": False}],
        }]}
        la = _lookahead(fold_defs, {}, cfr)
        _, failed = _gates(self.s, self.folds, lookahead=la)
        self.assertIn("prefix_audit_failed", failed)

    def test_train_selected_never_locked_pass(self):
        passes, failed = _gates(self.s, self.folds, mode="train_selected")
        self.assertFalse(passes)
        self.assertIn("train_selected_not_authoritative_locked", failed)

    def test_negative_net_r_gate(self):
        folds = [_fold_agg(fold=i + 1, gp=5.0, gl=20.0) for i in range(3)]
        s = _summary(folds)
        _, failed = _gates(s, folds)
        self.assertIn("oos_net_r_not_positive", failed)


# ══════════════════════════════════════════════════════════════════════════════
# Part J — Adaptive process check
# ══════════════════════════════════════════════════════════════════════════════

class TestAdaptive(unittest.TestCase):
    def test_train_selected_gets_adaptive_result(self):
        folds = [_fold_agg(fold=i + 1) for i in range(3)]
        s = _summary(folds, mode="train_selected")
        la = _good_lookahead(folds)
        bs = _good_bootstrap(folds)
        ts = [{"fold": i + 1, "training_selection_passed": True,
               "fallback_used": False, "selection_completed_before_test": True}
              for i in range(3)]
        ap = _adaptive(s, folds, la, bs, ts, ["1h", "4h"])
        self.assertIn("status", ap)
        self.assertIn("passes", ap)

    def test_adaptive_fallback_fails(self):
        folds = [_fold_agg(fold=i + 1) for i in range(3)]
        s = _summary(folds, mode="train_selected")
        la = _good_lookahead(folds)
        bs = _good_bootstrap(folds)
        ts = [{"fold": i + 1, "training_selection_passed": False,
               "fallback_used": True, "selection_completed_before_test": True}
              for i in range(3)]
        ap = _adaptive(s, folds, la, bs, ts, ["1h", "4h"])
        self.assertFalse(ap["passes"])
        self.assertIn("fallback_candidate_used_in_train_selected", ap["failed_gates"])


# ══════════════════════════════════════════════════════════════════════════════
# Part K — Bootstrap
# ══════════════════════════════════════════════════════════════════════════════

class TestBootstrap(unittest.TestCase):
    def test_insufficient_folds(self):
        bs = _bootstrap([0.05, 0.03])
        self.assertTrue(bs["insufficient_folds"])
        self.assertIsNone(bs["expectancy_delta_low"])

    def test_deterministic(self):
        a = _bootstrap([0.05, 0.04, 0.06])
        b = _bootstrap([0.05, 0.04, 0.06])
        self.assertEqual(a["expectancy_delta_low"], b["expectancy_delta_low"])


# ══════════════════════════════════════════════════════════════════════════════
# Part L — fold aggregation (new schema)
# ══════════════════════════════════════════════════════════════════════════════

class TestFoldAggregate(unittest.TestCase):
    def _cellresult(self, sym="BTCUSDT", tf="1h", trades=30, trusted=True):
        return {
            "ok": True, "symbol": sym, "timeframe": tf, "fold": 1,
            "test_parity_trusted": trusted,
            "censored_unresolved": 0, "resolved_inside_window": trades,
            "events_outside_window": 0, "prefix_contract_passed": True,
            "metrics": {
                "candidate": {"trades": trades, "wins": 20, "losses": 10,
                              "ambiguous": 0, "unresolved": 0, "invalid_loss_r": 0,
                              "gross_profit_r": 20.0, "gross_loss_r": 9.0,
                              "net_r": 11.0, "expectancy_r": 0.05},
                "baseline": {"trades": 50, "wins": 25, "losses": 25,
                             "gross_profit_r": 20.0, "gross_loss_r": 13.3,
                             "net_r": 6.7, "expectancy_r": 0.02},
                "expectancy_delta_vs_baseline": 0.03, "trade_retention_pct": 60.0,
            },
        }

    def test_eligible_requires_trust(self):
        fi = {"fold": 1, "train_end_index_exclusive": 400, "test_start_index": 400,
              "test_end_index_exclusive": 550, "train_candle_count": 400,
              "test_candle_count": 150}
        agg = _agg_fold([self._cellresult(trusted=False)], fi, 40, "2")
        self.assertFalse(agg["cells"][0]["eligible"])

    def test_macro_expectancy_present(self):
        fi = {"fold": 1, "train_end_index_exclusive": 400, "test_start_index": 400,
              "test_end_index_exclusive": 550, "train_candle_count": 400,
              "test_candle_count": 150}
        agg = _agg_fold([self._cellresult()], fi, 40, "2")
        self.assertIn("candidate_macro_expectancy_r", agg)
        self.assertIn("usable_cells", agg)
        self.assertIn("trusted_cells", agg)


# ══════════════════════════════════════════════════════════════════════════════
# Part M — Real Flask endpoint (mocked run)
# ══════════════════════════════════════════════════════════════════════════════

class TestEndpoint(unittest.TestCase):
    def setUp(self):
        _m.app.config["TESTING"] = True
        self.client = _m.app.test_client()
        with self.client.session_transaction() as sess:
            sess["logged_in"] = True
            sess["username"] = "tester"
            sess["is_admin"] = True

    def _post(self, body):
        return self.client.post(
            "/api/backtest/ob-historical/threshold-walk-forward", json=body)

    def _ok_body(self, **over):
        b = {"symbols": ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT"],
             "timeframes": ["1h", "4h"], "candle_count": 1000,
             "candidate_mode": "locked", "candidate_threshold_pct": 40,
             "candidate_rr": 2, "fold_count": 4, "initial_train_pct": 40.0,
             "test_pct": 15.0}
        b.update(over)
        return b

    # ── Rejection cases (validation; no run mock needed) ──────────────────────
    def test_reject_client_folds(self):
        r = self._post(self._ok_body(folds=[1, 2]))
        self.assertEqual(r.status_code, 400)

    def test_reject_client_cell_results(self):
        r = self._post(self._ok_body(cell_results=[1]))
        self.assertEqual(r.status_code, 400)

    def test_reject_client_trade_results(self):
        r = self._post(self._ok_body(trade_results=[1]))
        self.assertEqual(r.status_code, 400)

    def test_reject_client_recommendation(self):
        r = self._post(self._ok_body(recommendation={"x": 1}))
        self.assertEqual(r.status_code, 400)

    def test_reject_blank_symbol(self):
        r = self._post(self._ok_body(symbols=[""]))
        self.assertEqual(r.status_code, 400)

    def test_reject_too_many_symbols(self):
        r = self._post(self._ok_body(symbols=["A", "B", "C", "D", "E", "F"]))
        self.assertEqual(r.status_code, 400)

    def test_reject_bad_timeframe(self):
        r = self._post(self._ok_body(timeframes=["5m"]))
        self.assertEqual(r.status_code, 400)

    def test_reject_too_many_timeframes(self):
        r = self._post(self._ok_body(timeframes=["1h", "4h", "1d"]))
        self.assertEqual(r.status_code, 400)

    def test_reject_bad_candle_count_type(self):
        r = self._post(self._ok_body(candle_count="abc"))
        self.assertEqual(r.status_code, 400)

    def test_reject_bad_mode(self):
        r = self._post(self._ok_body(candidate_mode="nonsense"))
        self.assertEqual(r.status_code, 400)

    def test_reject_missing_locked_threshold(self):
        b = self._ok_body()
        del b["candidate_threshold_pct"]
        r = self._post(b)
        self.assertEqual(r.status_code, 400)

    def test_reject_bad_locked_threshold(self):
        r = self._post(self._ok_body(candidate_threshold_pct=99))
        self.assertEqual(r.status_code, 400)

    def test_reject_bad_candidate_rr(self):
        r = self._post(self._ok_body(candidate_rr=7))
        self.assertEqual(r.status_code, 400)

    def test_reject_bad_fold_count_type(self):
        r = self._post(self._ok_body(fold_count="x"))
        self.assertEqual(r.status_code, 400)

    def test_reject_fold_count_too_low(self):
        r = self._post(self._ok_body(fold_count=2))
        self.assertEqual(r.status_code, 400)

    def test_reject_fold_count_too_high(self):
        r = self._post(self._ok_body(fold_count=6))
        self.assertEqual(r.status_code, 400)

    def test_reject_bad_train_pct_type(self):
        r = self._post(self._ok_body(initial_train_pct="x"))
        self.assertEqual(r.status_code, 400)

    def test_reject_train_pct_out_of_range(self):
        r = self._post(self._ok_body(initial_train_pct=10.0))
        self.assertEqual(r.status_code, 400)

    def test_reject_bad_test_pct_type(self):
        r = self._post(self._ok_body(test_pct="x"))
        self.assertEqual(r.status_code, 400)

    def test_reject_test_pct_out_of_range(self):
        r = self._post(self._ok_body(test_pct=5.0))
        self.assertEqual(r.status_code, 400)

    def test_reject_folds_dont_fit(self):
        r = self._post(self._ok_body(initial_train_pct=60.0, test_pct=20.0,
                                     fold_count=5))
        self.assertEqual(r.status_code, 400)

    # ── Success path (run mocked) ─────────────────────────────────────────────
    def test_success_response_shape(self):
        fake = {"status": "PASS", "passes_all_gates": True,
                "summary": {"oos_trades": 200}, "fold_aggregates": [],
                "lookahead_audit": {"passed": True}}
        with patch.object(_m, "_bt_run_walk_forward", return_value=fake):
            r = self._post(self._ok_body())
        self.assertEqual(r.status_code, 200)
        data = r.get_json()
        self.assertTrue(data["ok"])
        self.assertTrue(data["authoritative_execution"])
        self.assertFalse(data["client_results_accepted"])
        self.assertIn("request", data)
        self.assertIn("response_size_bytes", data)
        self.assertEqual(data["walk_forward"]["status"], "PASS")

    def test_request_echo(self):
        fake = {"status": "PASS"}
        with patch.object(_m, "_bt_run_walk_forward", return_value=fake):
            r = self._post(self._ok_body(candidate_threshold_pct=60))
        self.assertEqual(r.get_json()["request"]["candidate_threshold_pct"], 60)


# ══════════════════════════════════════════════════════════════════════════════
# Part N — Full run with mocked candles (end-to-end, no network)
# ══════════════════════════════════════════════════════════════════════════════

class TestFullRun(unittest.TestCase):
    def test_run_with_mocked_klines(self):
        candles = _make_candles(1000)
        with patch.object(_m, "get_klines", return_value=candles), \
             patch.object(_m, "_bt_normalize_candles", side_effect=lambda c: c):
            res = _run_wf(
                symbols=["BTCUSDT", "ETHUSDT"], timeframes=["1h"],
                candle_count=1000, rr_values=[1, 2, 3],
                thresholds=_m._WF_ALLOWED_THRESHOLDS, candidate_mode="locked",
                candidate_threshold_pct=40, candidate_rr="2", fold_count=3,
                initial_train_pct=40.0, test_pct=15.0,
            )
        self.assertIn("status", res)
        self.assertIn("lookahead_audit", res)
        self.assertIn("passed", res["lookahead_audit"])
        self.assertIn("performance", res)
        self.assertTrue(res["performance"]["candles_fetched_once_per_cell"])

    def test_run_strips_internal_arrays(self):
        candles = _make_candles(1000)
        with patch.object(_m, "get_klines", return_value=candles), \
             patch.object(_m, "_bt_normalize_candles", side_effect=lambda c: c):
            res = _run_wf(
                symbols=["BTCUSDT"], timeframes=["1h"],
                candle_count=1000, rr_values=[1, 2, 3],
                thresholds=_m._WF_ALLOWED_THRESHOLDS, candidate_mode="locked",
                candidate_threshold_pct=40, candidate_rr="2", fold_count=3,
                initial_train_pct=40.0, test_pct=15.0,
            )
        import json
        blob = json.dumps(res, default=str)
        self.assertNotIn("_test_events", blob)
        self.assertNotIn("_train_events", blob)

    def test_lookahead_passes_on_real_run(self):
        candles = _make_candles(1000)
        with patch.object(_m, "get_klines", return_value=candles), \
             patch.object(_m, "_bt_normalize_candles", side_effect=lambda c: c):
            res = _run_wf(
                symbols=["BTCUSDT"], timeframes=["1h"],
                candle_count=1000, rr_values=[1, 2, 3],
                thresholds=_m._WF_ALLOWED_THRESHOLDS, candidate_mode="locked",
                candidate_threshold_pct=40, candidate_rr="2", fold_count=3,
                initial_train_pct=40.0, test_pct=15.0,
            )
        la = res["lookahead_audit"]
        self.assertTrue(la["fold_boundaries_valid"])
        self.assertTrue(la["test_windows_non_overlapping"])
        self.assertEqual(la["future_candle_reads_detected"], 0)
        self.assertEqual(la["prefix_audits_failed"], 0)
        self.assertTrue(la["future_candle_invariance"])

    def test_train_selected_produces_adaptive(self):
        candles = _make_candles(1000)
        with patch.object(_m, "get_klines", return_value=candles), \
             patch.object(_m, "_bt_normalize_candles", side_effect=lambda c: c):
            res = _run_wf(
                symbols=["BTCUSDT"], timeframes=["1h"],
                candle_count=1000, rr_values=[1, 2, 3],
                thresholds=_m._WF_ALLOWED_THRESHOLDS,
                candidate_mode="train_selected",
                candidate_threshold_pct=None, candidate_rr=None, fold_count=3,
                initial_train_pct=40.0, test_pct=15.0,
            )
        self.assertEqual(res["status"], "ADVISORY")
        self.assertIsNotNone(res["adaptive_process_result"])
        self.assertFalse(res["walk_forward_result"]["authoritative"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
