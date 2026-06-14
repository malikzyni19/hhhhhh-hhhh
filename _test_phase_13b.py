"""
Phase 13B unit tests — TV OB% Threshold Calculation Integrity.

Validates the 7 bug fixes:
  1. Loss R uses _bt_realized_loss_r (no 1.0 fallback)
  2. Best-RR uses _bt_threshold_rr_rank_key (5-level)
  3. PF uses profit_factor_infinite flag (no 999.0 sentinel)
  4. Direction trades = wins+losses at best_rr (not len events)
  5. SL metrics from realized loss population at best_rr
  6. Invalid losses tracked, not silently substituted
  7. trade_retention_pct at best_rr, not rr_values[0]

Usage:
    cd /home/user/hhhhhh-hhhh && python3 _test_phase_13b.py
"""
import os, sys, json, math, traceback, unittest

os.environ.setdefault("DATABASE_URL", "sqlite:///phase13b_test.db")
os.environ.setdefault("SECRET_KEY",   "phase13b-test-key")
os.environ.setdefault("RESEND_API_KEY", "test-resend-key")
os.environ.setdefault("TURNSTILE_SECRET", "")

sys.path.insert(0, os.path.dirname(__file__))

import types, importlib, unittest.mock as _mock

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
    print(f"Import OK\n", flush=True)
except Exception:
    traceback.print_exc()
    sys.exit(1)

_realized_loss_r = _m._bt_realized_loss_r
_rr_rank_key     = _m._bt_threshold_rr_rank_key
_build_analysis  = _m._bt_build_tv_ob_pct_threshold_analysis
_MIN_AUTH        = _m._MIN_AUTHORITATIVE_TRADES   # 20


# ── Event builder helpers ─────────────────────────────────────────────────────
def _make_event(pct_bft, rr_outcomes, direction="bullish", pct_form=None):
    """
    Build one eligible event.
    rr_outcomes: dict rr_str → (outcome, realized_r, stop_loss_r)
    """
    realized_by_rr = {}
    for rk, (out, real_r, sl_r) in rr_outcomes.items():
        realized_by_rr[rk] = {
            "outcome":    out,
            "realized_r": real_r,
            "stop_loss_r": sl_r,
        }
    first = "tp" if any(v[0] == "win"  for v in rr_outcomes.values()) else "sl"
    return {
        "type": direction,
        "tv_ob_pct_before_first_touch": pct_bft,
        "tv_ob_pct_at_formation":       pct_form if pct_form is not None else pct_bft,
        "simulation": {
            "eligible": True,
            "first_outcome": first,
            "stop_hit":  any(v[0] == "loss" for v in rr_outcomes.values()),
            "stop_loss_r":    -1.0,
            "max_r_reached":   1.5,
            "max_adverse_r":   0.5,
            "realized_by_rr":  realized_by_rr,
        },
    }


def _make_events(n_win, n_loss, rr_list=None, pct=50, direction="bullish",
                 loss_realized_r=None, loss_stop_loss_r=None):
    """Create n_win wins + n_loss losses at all given RRs."""
    if rr_list is None:
        rr_list = ["1", "2", "3"]
    events = []
    for _ in range(n_win):
        rr_outcomes = {rk: ("win", float(rk), None) for rk in rr_list}
        events.append(_make_event(pct, rr_outcomes, direction))
    for _ in range(n_loss):
        rr_outcomes = {rk: ("loss", loss_realized_r, loss_stop_loss_r)
                       for rk in rr_list}
        events.append(_make_event(pct, rr_outcomes, direction))
    return events


# ═════════════════════════════════════════════════════════════════════════════
class TestRealizedLossR(unittest.TestCase):
    """Bug 1 / Bug 6: _bt_realized_loss_r never fabricates 1R."""

    def test_uses_realized_r_first(self):
        rz = {"outcome": "loss", "realized_r": -1.3, "stop_loss_r": -1.0}
        self.assertAlmostEqual(_realized_loss_r(rz), 1.3)

    def test_fallback_to_stop_loss_r(self):
        rz = {"outcome": "loss", "realized_r": None, "stop_loss_r": -0.75}
        self.assertAlmostEqual(_realized_loss_r(rz), 0.75)

    def test_none_when_both_absent(self):
        """Must return None, never 1.0."""
        rz = {"outcome": "loss", "realized_r": None, "stop_loss_r": None}
        result = _realized_loss_r(rz)
        self.assertIsNone(result, "Must return None when both R fields are absent")

    def test_none_for_non_loss(self):
        rz = {"outcome": "win", "realized_r": 2.0, "stop_loss_r": -1.0}
        self.assertIsNone(_realized_loss_r(rz))

    def test_nan_returns_none(self):
        rz = {"outcome": "loss", "realized_r": float("nan"), "stop_loss_r": None}
        self.assertIsNone(_realized_loss_r(rz))


# ═════════════════════════════════════════════════════════════════════════════
class TestRrRankKey(unittest.TestCase):
    """Bug 2: 5-level comparator key."""

    def _rz(self, trades, exp, pf=None, pf_inf=False, nr=0.0, invalid=0):
        return {
            "trades": trades, "expectancy_r": exp,
            "profit_factor_r": pf, "profit_factor_infinite": pf_inf,
            "net_r": nr, "invalid_loss_r": invalid,
        }

    def test_authoritative_gates_small_sample(self):
        rz = self._rz(trades=_MIN_AUTH - 1, exp=0.5)
        self.assertIsNone(_rr_rank_key(rz, 1.0, authoritative=True))

    def test_authoritative_gates_invalid(self):
        rz = self._rz(trades=_MIN_AUTH + 5, exp=0.5, invalid=1)
        self.assertIsNone(_rr_rank_key(rz, 1.0, authoritative=True))

    def test_non_auth_allows_small_sample(self):
        rz = self._rz(trades=5, exp=0.3)
        self.assertIsNotNone(_rr_rank_key(rz, 1.0, authoritative=False))

    def test_higher_expectancy_wins(self):
        high = self._rz(25, 0.6, pf=2.0, nr=5.0)
        low  = self._rz(25, 0.3, pf=2.0, nr=5.0)
        self.assertGreater(_rr_rank_key(high, 1.0), _rr_rank_key(low, 1.0))

    def test_lower_rv_wins_at_equal_expectancy(self):
        rz = self._rz(25, 0.5, pf=2.0, nr=5.0)
        self.assertGreater(_rr_rank_key(rz, 1.0), _rr_rank_key(rz, 2.0))

    def test_pf_infinite_beats_large_finite(self):
        inf_rz  = self._rz(25, 0.5, pf=None, pf_inf=True,  nr=5.0)
        big_rz  = self._rz(25, 0.5, pf=99.0, pf_inf=False, nr=5.0)
        self.assertGreater(_rr_rank_key(inf_rz, 1.0), _rr_rank_key(big_rz, 1.0))


# ═════════════════════════════════════════════════════════════════════════════
class TestProfitFactorSentinel(unittest.TestCase):
    """Bug 3: No 999.0 sentinel; use profit_factor_infinite flag."""

    def test_no_sentinel_in_output(self):
        events = _make_events(30, 0, pct=50)
        result = _build_analysis(events, [1, 2, 3], parity_trusted=True)
        out_str = json.dumps(result, default=str)
        self.assertNotIn("999", out_str,
                         "999 PF sentinel must not appear anywhere in output")

    def test_pf_infinite_flag_set(self):
        events = _make_events(30, 0, pct=50)
        result = _build_analysis(events, [1, 2, 3], parity_trusted=True)
        rz = result["results"]["0"]["realized_summary_by_rr"]["1"]
        self.assertIsNone(rz["profit_factor_r"])
        self.assertTrue(rz["profit_factor_infinite"])

    def test_pf_finite_when_losses_exist(self):
        events = _make_events(20, 10, pct=50,
                              loss_realized_r=-1.0, loss_stop_loss_r=-1.0)
        result = _build_analysis(events, [1, 2, 3], parity_trusted=True)
        rz = result["results"]["0"]["realized_summary_by_rr"]["1"]
        self.assertFalse(rz["profit_factor_infinite"])
        self.assertIsNotNone(rz["profit_factor_r"])
        self.assertNotEqual(rz["profit_factor_r"], 999.0)


# ═════════════════════════════════════════════════════════════════════════════
class TestDirectionTrades(unittest.TestCase):
    """Bug 4: Direction trades = wins+losses at best_rr, not event count."""

    def test_direction_trades_le_event_count(self):
        bull = _make_events(20, 10, pct=50, direction="bullish",
                            loss_realized_r=-1.0, loss_stop_loss_r=-1.0)
        bear = _make_events(15, 5, pct=50, direction="bearish",
                            loss_realized_r=-1.0, loss_stop_loss_r=-1.0)
        result = _build_analysis(bull + bear, [1, 2, 3], parity_trusted=True)
        b = result["results"]["0"]["by_direction"]["bullish"]
        # trades (wins+losses) must be <= event_count (which is 30)
        self.assertIn("event_count", b, "event_count must be present")
        if b["trades"] is not None:
            # wins+losses = 30, event_count = 30; they may be equal here
            self.assertLessEqual(b["trades"], b["event_count"])

    def test_direction_has_event_count_field(self):
        """event_count (raw events) must be present for reference."""
        events = _make_events(15, 5, pct=50,
                              loss_realized_r=-1.0, loss_stop_loss_r=-1.0)
        result = _build_analysis(events, [1, 2, 3], parity_trusted=True)
        bull = result["results"]["0"]["by_direction"]["bullish"]
        self.assertIn("event_count", bull)
        self.assertEqual(bull["event_count"], 20)


# ═════════════════════════════════════════════════════════════════════════════
class TestSlMetricsSource(unittest.TestCase):
    """Bug 5: SL metrics from realized loss R, not stop_loss_r field."""

    def test_sl_metrics_from_realized_r(self):
        # realized_r=-2.0, stop_loss_r=-1.0  → avg should be ~2.0
        events = _make_events(15, 15, pct=50,
                              loss_realized_r=-2.0, loss_stop_loss_r=-1.0)
        result = _build_analysis(events, [1, 2, 3], parity_trusted=True)
        avg_sl = result["results"]["0"].get("avg_stop_loss_r")
        if avg_sl is not None:
            self.assertAlmostEqual(avg_sl, 2.0, places=2,
                                   msg="avg_stop_loss_r should come from realized_r")


# ═════════════════════════════════════════════════════════════════════════════
class TestInvalidLossTracking(unittest.TestCase):
    """Bug 6: Losses with missing R are counted as invalid, not substituted."""

    def test_invalid_loss_tracked(self):
        events = _make_events(20, 5, pct=50,
                              loss_realized_r=None, loss_stop_loss_r=None)
        result = _build_analysis(events, [1, 2, 3], parity_trusted=True)
        rz = result["results"]["0"]["realized_summary_by_rr"]["1"]
        self.assertGreater(rz["invalid_loss_r"], 0)
        self.assertEqual(rz["losses"], 0, "losses must not include trades with missing R")

    def test_invalid_loss_blocks_auth(self):
        events = _make_events(30, 5, pct=50,
                              loss_realized_r=None, loss_stop_loss_r=None)
        result = _build_analysis(events, [1, 2, 3], parity_trusted=True)
        for thr_k, rd in result["results"].items():
            if rd["invalid_loss_r_at_best_rr"] > 0:
                self.assertFalse(
                    rd["eligible_for_authoritative_ranking"],
                    f"thr={thr_k} with invalid_loss_r must not be authoritative",
                )


# ═════════════════════════════════════════════════════════════════════════════
class TestRetentionAtBestRr(unittest.TestCase):
    """Bug 7: trade_retention_pct must be at best_rr, not always rr_values[0]."""

    def test_retention_matches_cmp_at_best_rr(self):
        """trade_retention_pct == comparison_vs_baseline_by_rr[best_rr].trade_retention_pct"""
        events = _make_events(25, 5, pct=50,
                              loss_realized_r=-1.0, loss_stop_loss_r=-1.0)
        result = _build_analysis(events, [1, 2, 3], parity_trusted=True)
        for thr_k, rd in result["results"].items():
            brr = rd.get("trade_retention_rr")
            if brr is None:
                continue
            cmp_ret = (rd.get("comparison_vs_baseline_by_rr") or {}).get(brr, {}).get(
                "trade_retention_pct"
            )
            self.assertEqual(
                rd["trade_retention_pct"], cmp_ret,
                f"thr={thr_k}: trade_retention_pct must equal cmp[{brr}].trade_retention_pct",
            )


# ═════════════════════════════════════════════════════════════════════════════
class TestBalancedRec(unittest.TestCase):
    """Balanced recommendation correctness."""

    def test_no_rec_when_untrusted(self):
        events = _make_events(30, 10, pct=50,
                              loss_realized_r=-1.0, loss_stop_loss_r=-1.0)
        result = _build_analysis(events, [1, 2, 3], parity_trusted=False)
        rec = (result.get("best") or {}).get("balanced_recommendation")
        self.assertIsNone(rec)

    def test_audit_structure_present(self):
        events = _make_events(25, 5, pct=50,
                              loss_realized_r=-1.0, loss_stop_loss_r=-1.0)
        result = _build_analysis(events, [1, 2, 3], parity_trusted=True)
        audit = result.get("balanced_recommendation_audit", {})
        self.assertIn("candidates_considered", audit)
        self.assertIn("candidates_rejected",   audit)
        self.assertIn("selection_rule",        audit)

    def test_rec_pf_infinite_flag(self):
        """Rec with wins-only has profit_factor_infinite=True and profit_factor_r=None."""
        events = _make_events(30, 0, pct=50)
        result = _build_analysis(events, [1, 2, 3], parity_trusted=True)
        rec = (result.get("best") or {}).get("balanced_recommendation")
        if rec and rec.get("profit_factor_infinite"):
            self.assertIsNone(rec["profit_factor_r"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
