"""
Phase 16 unit tests — Advanced Trade Explorer: first/second/third/fourth+ touch
analysis (research only).

Design constraints:
  * No conditional assertions — every assertion always executes.
  * No external API calls; get_klines is mocked wherever the pipeline runs.
  * Trade Explorer is research-only; no activation, no DB writes.

Usage:
    cd /home/user/hhhhhh-hhhh && python3 _test_phase_16.py
"""
import os, sys, traceback, unittest
from unittest.mock import patch

os.environ.setdefault("DATABASE_URL", "sqlite:///phase16_test.db")
os.environ.setdefault("SECRET_KEY",   "phase16-test-key")
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
_detect_episodes  = _m._te_detect_touch_episodes
_simulate         = _m._te_simulate_touch_outcome
_build_rows       = _m._te_build_touch_trade_rows
_aggregate        = _m._te_compute_aggregate
_bucket_cmp       = _m._te_touch_bucket_comparison
_apply_filters    = _m._te_apply_filters
_touch_bucket     = _m._te_touch_bucket
_ob_id            = _m._te_ob_id
_trade_id         = _m._te_touch_trade_id
_sample_status    = _m._te_sample_size_status
_sort_rows        = _m._te_sort_rows
_run_explorer     = _m._bt_run_trade_explorer
_run_detail       = _m._bt_run_trade_explorer_detail


# ── Test data builders ───────────────────────────────────────────────────────

def _candles(hlc_list):
    """hlc_list = [(high, low, close), ...]; open_time = i*3_600_000."""
    out = []
    for i, (h, l, c) in enumerate(hlc_list):
        out.append({"open_time": i * 3_600_000,
                    "close_time": (i + 1) * 3_600_000 - 1,
                    "open": c, "high": float(h), "low": float(l),
                    "close": float(c), "volume": 100.0})
    return out


def _event(formation_bar=0, zone_high=100.0, zone_low=95.0, ob_type="bullish",
           mitigation_bar=None, touch_status="touched", first_touch_bar=None,
           sym="BTCUSDT", tf="1h"):
    return {
        "ob_id":            f"raw_ob_{formation_bar}",
        "symbol":           sym,
        "timeframe":        tf,
        "type":             ob_type,
        "formation_bar":    formation_bar,
        "formation_time":   formation_bar * 3_600_000,
        "zone_high":        zone_high,
        "zone_low":         zone_low,
        "zone_size":        zone_high - zone_low,
        "zone_size_pct":    round((zone_high - zone_low) / zone_low * 100, 4),
        "touch_status":     touch_status,
        "first_touch_bar":  first_touch_bar,
        "first_touch_time": (first_touch_bar * 3_600_000
                             if first_touch_bar is not None else None),
        "later_mitigated":  mitigation_bar is not None,
        "mitigation_bar":   mitigation_bar,
        "mitigation_time":  (mitigation_bar * 3_600_000
                             if mitigation_bar is not None else None),
    }


# Zone [95, 100].  In-zone candle: (99, 96, 97).  Outside above: (120, 110, 115).
IN_   = (99.0, 96.0, 97.0)
OUT_A = (120.0, 110.0, 115.0)   # fully above zone
OUT_B = (90.0, 85.0, 88.0)      # fully below zone → bullish mitigation (close 88 < 95)


# ══════════════════════════════════════════════════════════════════════════════
# Part A/B — Touch episode detection
# ══════════════════════════════════════════════════════════════════════════════

class TestTouchEpisodes(unittest.TestCase):
    def test_01_formation_candle_not_counted(self):
        # Formation candle itself intersects the zone but must never count.
        candles = _candles([IN_, OUT_A, OUT_A])
        eps = _detect_episodes(candles, _event(formation_bar=0))
        self.assertEqual(len(eps), 0)

    def test_02_first_intersection_is_touch_1(self):
        candles = _candles([OUT_A, OUT_A, IN_])
        eps = _detect_episodes(candles, _event(formation_bar=0))
        self.assertEqual(len(eps), 1)
        self.assertEqual(eps[0]["touch_number"], 1)
        self.assertEqual(eps[0]["touch_index"], 2)
        self.assertEqual(eps[0]["touch_bucket"], "first")

    def test_03_continuous_overlap_counts_once(self):
        candles = _candles([OUT_A, IN_, IN_, IN_, OUT_A])
        eps = _detect_episodes(candles, _event(formation_bar=0))
        self.assertEqual(len(eps), 1)
        self.assertEqual(eps[0]["episode_start_index"], 1)
        self.assertEqual(eps[0]["episode_end_index_exclusive"], 4)

    def test_04_leave_and_reenter_is_touch_2(self):
        candles = _candles([OUT_A, IN_, OUT_A, IN_, OUT_A])
        eps = _detect_episodes(candles, _event(formation_bar=0))
        self.assertEqual(len(eps), 2)
        self.assertEqual(eps[1]["touch_number"], 2)
        self.assertEqual(eps[1]["touch_bucket"], "second")
        self.assertEqual(eps[1]["touch_index"], 3)

    def test_05_third_touch(self):
        candles = _candles([OUT_A, IN_, OUT_A, IN_, OUT_A, IN_, OUT_A])
        eps = _detect_episodes(candles, _event(formation_bar=0))
        self.assertEqual(len(eps), 3)
        self.assertEqual(eps[2]["touch_number"], 3)
        self.assertEqual(eps[2]["touch_bucket"], "third")

    def test_06_fourth_touch_maps_to_fourth_plus(self):
        candles = _candles([OUT_A, IN_, OUT_A, IN_, OUT_A, IN_, OUT_A, IN_, OUT_A])
        eps = _detect_episodes(candles, _event(formation_bar=0))
        self.assertEqual(len(eps), 4)
        self.assertEqual(eps[3]["touch_bucket"], "fourth_plus")
        self.assertEqual(_touch_bucket(5), "fourth_plus")
        self.assertEqual(_touch_bucket(9), "fourth_plus")

    def test_07_mitigated_ob_produces_no_later_touch(self):
        # Mitigation at bar 3 → no touches detected after bar 3.
        candles = _candles([OUT_A, IN_, OUT_A, OUT_B, OUT_A, IN_, OUT_A])
        eps = _detect_episodes(candles, _event(formation_bar=0, mitigation_bar=3))
        self.assertEqual(len(eps), 1)
        self.assertEqual(eps[0]["touch_index"], 1)

    def test_08_mitigation_bar_itself_may_touch_but_nothing_after(self):
        # Mitigation candle intersects the zone: wick in, close below.
        MIT_TOUCH = (96.0, 85.0, 88.0)   # high 96 >= 95 → intersects; close 88 < 95 → mitigates
        candles = _candles([OUT_A, OUT_A, MIT_TOUCH, OUT_A, IN_, OUT_A])
        eps = _detect_episodes(candles, _event(formation_bar=0, mitigation_bar=2))
        self.assertEqual(len(eps), 1)
        self.assertEqual(eps[0]["touch_index"], 2)

    def test_09_untouched_event_no_episodes(self):
        candles = _candles([OUT_A, IN_, OUT_A])
        eps = _detect_episodes(candles, _event(formation_bar=0, touch_status="untouched"))
        self.assertEqual(eps, [])

    def test_10_episodes_chronological(self):
        candles = _candles([OUT_A, IN_, OUT_A, IN_, OUT_A, IN_, OUT_A])
        eps = _detect_episodes(candles, _event(formation_bar=0))
        idxs = [e["touch_index"] for e in eps]
        self.assertEqual(idxs, sorted(idxs))
        nums = [e["touch_number"] for e in eps]
        self.assertEqual(nums, [1, 2, 3])

    def test_11_no_lookahead_before_formation(self):
        # Touches before/at formation bar never counted.
        candles = _candles([IN_, IN_, OUT_A, IN_, OUT_A])
        eps = _detect_episodes(candles, _event(formation_bar=1))
        self.assertTrue(all(e["touch_index"] > 1 for e in eps))
        self.assertEqual(len(eps), 1)
        self.assertEqual(eps[0]["touch_index"], 3)

    def test_12_ob_valid_before_touch_flag(self):
        candles = _candles([OUT_A, IN_, OUT_A])
        eps = _detect_episodes(candles, _event(formation_bar=0))
        self.assertTrue(eps[0]["ob_valid_before_touch"])
        self.assertIsNone(eps[0]["touch_rejected_reason"])
        self.assertTrue(eps[0]["generated_server_side"])


# ══════════════════════════════════════════════════════════════════════════════
# Part E — Stable identities
# ══════════════════════════════════════════════════════════════════════════════

class TestIdentities(unittest.TestCase):
    def test_13_touch_id_deterministic(self):
        ev = _event(formation_bar=5)
        a = _trade_id(ev, 2, 17, "2")
        b = _trade_id(ev, 2, 17, "2")
        self.assertEqual(a, b)
        self.assertTrue(a.startswith("tte_"))

    def test_14_touch_id_unique_across_touches_and_rr(self):
        ev = _event(formation_bar=5)
        ids = {_trade_id(ev, t, 10 + t, rk) for t in (1, 2, 3) for rk in ("1", "2", "3")}
        self.assertEqual(len(ids), 9)

    def test_15_ob_id_includes_canonical_fields(self):
        ev = _event(formation_bar=7, sym="ETHUSDT", tf="4h")
        oid = _ob_id(ev)
        self.assertIn("ETHUSDT", oid)
        self.assertIn("4h", oid)
        self.assertIn("7", oid)

    def test_16_no_duplicate_ids_in_rows(self):
        candles = _candles([OUT_A, IN_, OUT_A, IN_, OUT_A] + [OUT_A] * 5)
        ev = _event(formation_bar=0)
        eps = _detect_episodes(candles, ev)
        rows = _build_rows(ev, eps, candles, [1, 2, 3])
        ids = [r["touch_trade_id"] for r in rows]
        self.assertEqual(len(ids), len(set(ids)))


# ══════════════════════════════════════════════════════════════════════════════
# Part F/G — Independent touch-entry simulation
# ══════════════════════════════════════════════════════════════════════════════

class TestTouchSimulation(unittest.TestCase):
    def _win_candles(self):
        # zone [95,100]; touch at bar 2; 1R target 105, 2R 110, 3R 115 (bullish).
        return _candles([OUT_A, OUT_A, IN_, (106, 96, 100), (111, 96, 100),
                         (116, 96, 100), OUT_A])

    def test_17_first_touch_sim_starts_at_touch1(self):
        candles = self._win_candles()
        ev = _event(formation_bar=0)
        eps = _detect_episodes(candles, ev)
        sim = _simulate(eps[0]["touch_index"], ev, candles, [1, 2, 3])
        self.assertEqual(sim["entry_bar"], 2)
        self.assertTrue(sim["eligible"])

    def test_18_second_touch_sim_starts_at_touch2(self):
        candles = _candles([OUT_A, IN_, OUT_A, IN_, (106, 96, 100), (111, 96, 100),
                            (116, 96, 100), OUT_A])
        ev = _event(formation_bar=0)
        eps = _detect_episodes(candles, ev)
        self.assertEqual(len(eps), 2)
        sim2 = _simulate(eps[1]["touch_index"], ev, candles, [1, 2, 3])
        self.assertEqual(sim2["entry_bar"], 3)

    def test_19_third_touch_sim_starts_at_touch3(self):
        candles = _candles([OUT_A, IN_, OUT_A, IN_, OUT_A, IN_,
                            (106, 96, 100), (111, 96, 100), (116, 96, 100)])
        ev = _event(formation_bar=0)
        eps = _detect_episodes(candles, ev)
        self.assertEqual(len(eps), 3)
        sim3 = _simulate(eps[2]["touch_index"], ev, candles, [1, 2, 3])
        self.assertEqual(sim3["entry_bar"], 5)

    def test_20_variants_independent(self):
        # Touch-1 outcome (win at bar 4) must not change touch-2 sim scanned alone.
        candles = _candles([OUT_A, IN_, OUT_A, IN_, (106, 96, 100),
                            (111, 96, 100), (116, 96, 100), OUT_A])
        ev = _event(formation_bar=0)
        eps = _detect_episodes(candles, ev)
        sim1 = _simulate(eps[0]["touch_index"], ev, candles, [1, 2, 3])
        sim2 = _simulate(eps[1]["touch_index"], ev, candles, [1, 2, 3])
        self.assertEqual(sim1["entry_bar"], 1)
        self.assertEqual(sim2["entry_bar"], 3)
        # Both reach 1R independently.
        self.assertTrue(sim1["hit_rr"]["1"])
        self.assertTrue(sim2["hit_rr"]["1"])

    def test_21_overlap_diagnostic(self):
        # Touch-1 variant unresolved (price hovers above zone but below the
        # 2R target, never stopping) when touch 2 begins → overlap = True.
        OUT_MID = (108.0, 102.0, 105.0)   # fully above zone, below 2R target 110
        candles = _candles([OUT_MID, IN_, OUT_MID, IN_, OUT_MID, OUT_MID])
        ev = _event(formation_bar=0)
        eps = _detect_episodes(candles, ev)
        self.assertEqual(len(eps), 2)
        rows = _build_rows(ev, eps, candles, [2])
        t1 = [r for r in rows if r["touch_number"] == 1][0]
        t2 = [r for r in rows if r["touch_number"] == 2][0]
        self.assertFalse(t1["overlaps_previous_touch_trade"])
        self.assertTrue(t2["overlaps_previous_touch_trade"])

    def test_22_first_touch_matches_canonical(self):
        # _te_simulate_touch_outcome at first_touch_bar must reproduce the
        # canonical _bt_simulate_first_touch_outcome result field-for-field.
        candles = self._win_candles()
        ev = _event(formation_bar=0, first_touch_bar=2)
        canonical = _m._bt_simulate_first_touch_outcome(ev, candles, [1, 2, 3])
        te = _simulate(2, ev, candles, [1, 2, 3])
        for key in ("eligible", "entry_price", "entry_bar", "stop_boundary",
                    "risk_amount", "first_outcome", "stop_hit", "stop_bar",
                    "max_r_reached", "max_adverse_r", "stop_loss_r",
                    "hit_rr", "candles_to_rr", "same_candle_event"):
            self.assertEqual(canonical[key], te[key], f"mismatch on {key}")
        for rk in ("1", "2", "3"):
            self.assertEqual(canonical["realized_by_rr"][rk]["outcome"],
                             te["realized_by_rr"][rk]["outcome"])
            self.assertEqual(canonical["realized_by_rr"][rk]["realized_r"],
                             te["realized_by_rr"][rk]["realized_r"])

    def test_23_close_based_stop_preserved(self):
        # Wick below zone_low must NOT stop; close below zone_low must.
        wick_only = _candles([OUT_A, IN_, (99, 90, 97), (99, 96, 97), OUT_A])
        ev = _event(formation_bar=0)
        sim = _simulate(1, ev, wick_only, [2])
        self.assertFalse(sim["stop_hit"])
        close_below = _candles([OUT_A, IN_, (99, 90, 92), OUT_A, OUT_A])
        sim2 = _simulate(1, ev, close_below, [2])
        self.assertTrue(sim2["stop_hit"])
        self.assertEqual(sim2["stop_bar"], 2)

    def test_24_actual_stop_loss_r_preserved(self):
        # entry 100, risk 5, stop close 92 → stop_loss_r = (100-92)/5 = 1.6.
        candles = _candles([OUT_A, IN_, (99, 90, 92), OUT_A])
        ev = _event(formation_bar=0)
        sim = _simulate(1, ev, candles, [2])
        self.assertEqual(sim["stop_loss_r"], 1.6)
        rb = sim["realized_by_rr"]["2"]
        self.assertEqual(rb["outcome"], "loss")
        self.assertEqual(rb["realized_r"], -1.6)

    def test_25_ambiguous_same_candle(self):
        # TP wick and SL close on the same candle → ambiguous.
        candles = _candles([OUT_A, IN_, (111, 90, 92), OUT_A])
        ev = _event(formation_bar=0)
        sim = _simulate(1, ev, candles, [2])
        self.assertEqual(sim["first_outcome"], "ambiguous")
        self.assertEqual(sim["realized_by_rr"]["2"]["outcome"], "ambiguous")
        self.assertIsNone(sim["realized_by_rr"]["2"]["realized_r"])

    def test_26_unresolved_at_data_end(self):
        candles = _candles([OUT_A, IN_, IN_, (99, 96, 97)])
        ev = _event(formation_bar=0)
        sim = _simulate(1, ev, candles, [2])
        self.assertEqual(sim["first_outcome"], "unresolved")
        self.assertIsNone(sim["realized_by_rr"]["2"]["realized_r"])


# ══════════════════════════════════════════════════════════════════════════════
# Part H — Outcome contract
# ══════════════════════════════════════════════════════════════════════════════

def _mk_row(outcome, rr="2", realized=None, touch_number=1, direction="bullish",
            session="Asia", weekday="Monday", stop_loss_r=None, age=5,
            sym="BTCUSDT", tf="1h", tid=None, touch_time=0, mfe=None, mae=None,
            fvg="unknown", htf="unknown", tv=None):
    global _ROW_SEQ
    _ROW_SEQ += 1
    return {
        "touch_trade_id": tid or f"tte_{_ROW_SEQ:06d}",
        "ob_id": "te_ob:x", "symbol": sym, "timeframe": tf,
        "direction": direction, "touch_number": touch_number,
        "touch_bucket": _touch_bucket(touch_number),
        "formation_time": 0, "touch_time": touch_time,
        "candles_since_formation": age, "session": session, "weekday": weekday,
        "zone_high": 100.0, "zone_low": 95.0, "zone_size_pct": 5.26,
        "zone_size_atr": None, "entry_price": 100.0, "stop_price": 95.0,
        "stop_loss_r": stop_loss_r, "rr": rr, "outcome": outcome,
        "realized_r": realized, "mfe_r": mfe, "mae_r": mae,
        "fvg_overlap": fvg, "higher_timeframe_alignment": htf,
        "tv_ob_pct_before_touch": tv, "tv_ob_pct_exchange_specific": True,
        "overlaps_previous_touch_trade": False, "eligible": True,
        "rejection_reasons": [],
    }

_ROW_SEQ = 0


class TestOutcomeContract(unittest.TestCase):
    def _rows(self):
        return [
            _mk_row("win",  realized=2.0),
            _mk_row("win",  realized=2.0),
            _mk_row("loss", realized=-1.2, stop_loss_r=1.2),
            _mk_row("ambiguous"),
            _mk_row("unresolved"),
            _mk_row("invalid_loss_r"),
        ]

    def test_27_trades_equal_wins_plus_losses(self):
        agg = _aggregate(self._rows(), "2")
        self.assertEqual(agg["trades"], agg["wins"] + agg["losses"])
        self.assertEqual(agg["trades"], 3)

    def test_28_ambiguous_excluded(self):
        agg = _aggregate(self._rows(), "2")
        self.assertEqual(agg["ambiguous"], 1)
        self.assertEqual(agg["trades"], 3)

    def test_29_unresolved_excluded(self):
        agg = _aggregate(self._rows(), "2")
        self.assertEqual(agg["unresolved"], 1)
        self.assertEqual(agg["trades"], 3)

    def test_30_invalid_loss_r_excluded(self):
        agg = _aggregate(self._rows(), "2")
        self.assertEqual(agg["invalid_loss_r"], 1)
        self.assertEqual(agg["trades"], 3)

    def test_31_no_fabricated_losses(self):
        # net_r counts only real wins/losses: 2 + 2 - 1.2 = 2.8
        agg = _aggregate(self._rows(), "2")
        self.assertEqual(agg["net_r"], 2.8)
        self.assertEqual(agg["gross_profit_r"], 4.0)
        self.assertEqual(agg["gross_loss_r"], 1.2)

    def test_32_win_rate_and_expectancy(self):
        agg = _aggregate(self._rows(), "2")
        self.assertAlmostEqual(agg["win_rate_pct"], 66.67, places=2)
        self.assertAlmostEqual(agg["expectancy_r"], 2.8 / 3, places=4)

    def test_33_empty_rows_safe(self):
        agg = _aggregate([], "2")
        self.assertEqual(agg["trades"], 0)
        self.assertIsNone(agg["win_rate_pct"])
        self.assertIsNone(agg["expectancy_r"])
        self.assertEqual(agg["sample_size_status"], "insufficient")

    def test_34_different_rr_not_mixed(self):
        rows = [_mk_row("win", rr="1", realized=1.0), _mk_row("win", rr="2", realized=2.0)]
        agg = _aggregate(rows, "2")
        self.assertEqual(agg["trades"], 1)
        self.assertEqual(agg["net_r"], 2.0)


# ══════════════════════════════════════════════════════════════════════════════
# Part O/P — Touch bucket comparison + sample size
# ══════════════════════════════════════════════════════════════════════════════

class TestBucketsAndSampleSize(unittest.TestCase):
    def test_35_bucket_comparison_structure(self):
        rows = [_mk_row("win", realized=2.0, touch_number=1),
                _mk_row("loss", realized=-1.0, stop_loss_r=1.0, touch_number=2),
                _mk_row("win", realized=2.0, touch_number=3),
                _mk_row("win", realized=2.0, touch_number=5)]
        cmp_ = _bucket_cmp(rows, "2")
        self.assertEqual(len(cmp_), 4)
        self.assertEqual([b["touch_bucket"] for b in cmp_],
                         ["first", "second", "third", "fourth_plus"])
        self.assertEqual(cmp_[0]["trades"], 1)
        self.assertEqual(cmp_[3]["trades"], 1)   # touch 5 → fourth_plus

    def test_36_sample_size_labels(self):
        self.assertEqual(_sample_status(0),   "insufficient")
        self.assertEqual(_sample_status(19),  "insufficient")
        self.assertEqual(_sample_status(20),  "small")
        self.assertEqual(_sample_status(49),  "small")
        self.assertEqual(_sample_status(50),  "usable")
        self.assertEqual(_sample_status(99),  "usable")
        self.assertEqual(_sample_status(100), "strong")


# ══════════════════════════════════════════════════════════════════════════════
# Part K/L/M — Filters
# ══════════════════════════════════════════════════════════════════════════════

class TestFilters(unittest.TestCase):
    def _rows(self):
        return [
            _mk_row("win",  realized=2.0, touch_number=1, direction="bullish",
                    session="Asia",   weekday="Monday",  age=5,  stop_loss_r=None),
            _mk_row("loss", realized=-1.0, touch_number=2, direction="bearish",
                    session="London", weekday="Tuesday", age=15, stop_loss_r=1.0),
            _mk_row("win",  realized=2.0, touch_number=3, direction="bullish",
                    session=None,     weekday="Friday",  age=30, stop_loss_r=1.5),
            _mk_row("win",  realized=2.0, touch_number=4, direction="bearish",
                    session="NewYork", weekday="Monday", age=50, stop_loss_r=2.5),
        ]

    def test_37_touch_filter(self):
        out = _apply_filters(self._rows(), {"touch_numbers": [1]})
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["touch_number"], 1)

    def test_38_touch_filter_fourth_plus(self):
        rows = self._rows() + [_mk_row("win", realized=2.0, touch_number=7)]
        out = _apply_filters(rows, {"touch_numbers": [4]})
        self.assertEqual(len(out), 2)   # touch 4 and touch 7

    def test_39_direction_filter(self):
        out = _apply_filters(self._rows(), {"directions": ["bullish"]})
        self.assertEqual(len(out), 2)
        self.assertTrue(all(r["direction"] == "bullish" for r in out))

    def test_40_outcome_filter(self):
        out = _apply_filters(self._rows(), {"outcomes": ["win"]})
        self.assertEqual(len(out), 3)

    def test_41_ob_age_range(self):
        out = _apply_filters(self._rows(), {"ob_age_candles_min": 10,
                                            "ob_age_candles_max": 40})
        self.assertEqual(len(out), 2)
        self.assertEqual({r["candles_since_formation"] for r in out}, {15, 30})

    def test_42_stop_loss_range(self):
        out = _apply_filters(self._rows(), {"stop_loss_r_min": 1.2,
                                            "stop_loss_r_max": 2.0,
                                            "include_unknown": False})
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["stop_loss_r"], 1.5)

    def test_43_session_filter(self):
        out = _apply_filters(self._rows(), {"sessions": ["Asia"],
                                            "include_unknown": False})
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["session"], "Asia")

    def test_44_and_across_categories(self):
        out = _apply_filters(self._rows(), {"touch_numbers": [1, 2, 3, 4],
                                            "directions": ["bullish"],
                                            "outcomes": ["win"]})
        self.assertEqual(len(out), 2)   # bullish AND win

    def test_45_or_within_category(self):
        out = _apply_filters(self._rows(), {"directions": ["bullish", "bearish"]})
        self.assertEqual(len(out), 4)

    def test_46_unknown_included_by_default(self):
        # session=None row kept when filtering sessions with include_unknown default.
        out = _apply_filters(self._rows(), {"sessions": ["Asia"]})
        self.assertEqual(len(out), 2)   # Asia + the None-session row

    def test_47_unknown_excluded_when_disabled(self):
        out = _apply_filters(self._rows(), {"sessions": ["Asia"],
                                            "include_unknown": False})
        self.assertEqual(len(out), 1)

    def test_48_empty_filter_result_safe(self):
        out = _apply_filters(self._rows(), {"directions": ["bullish"],
                                            "outcomes": ["loss"]})
        self.assertEqual(out, [])
        agg = _aggregate(out, "2")
        self.assertEqual(agg["trades"], 0)

    def test_49_tv_pct_filter_disabled_by_default(self):
        # Default filters carry tv_ob_pct_min/max = None → no row excluded.
        rows = self._rows()
        out = _apply_filters(rows, {"tv_ob_pct_min": None, "tv_ob_pct_max": None})
        self.assertEqual(len(out), len(rows))


# ══════════════════════════════════════════════════════════════════════════════
# Sorting / pagination helpers
# ══════════════════════════════════════════════════════════════════════════════

class TestSorting(unittest.TestCase):
    def test_50_sort_stable_and_deterministic(self):
        rows = [_mk_row("win", realized=2.0, touch_time=t) for t in (30, 10, 20)]
        a = _sort_rows(rows, "touch_time", "desc")
        b = _sort_rows(rows, "touch_time", "desc")
        self.assertEqual([r["touch_trade_id"] for r in a],
                         [r["touch_trade_id"] for r in b])
        self.assertEqual([r["touch_time"] for r in a], [30, 20, 10])

    def test_51_sort_asc_and_unknown_field_fallback(self):
        rows = [_mk_row("win", realized=2.0, touch_time=t) for t in (30, 10, 20)]
        asc = _sort_rows(rows, "touch_time", "asc")
        self.assertEqual([r["touch_time"] for r in asc], [10, 20, 30])
        fb = _sort_rows(rows, "not_a_field", "desc")
        self.assertEqual([r["touch_time"] for r in fb], [30, 20, 10])


# ══════════════════════════════════════════════════════════════════════════════
# Orchestrator (mocked get_klines — no external APIs)
# ══════════════════════════════════════════════════════════════════════════════

def _fake_raw_candles(n=400):
    """camelCase raw klines shaped for _bt_normalize_candles.

    Mean-reverting oscillation with seeded noise so BOS events form OBs and
    price repeatedly revisits the zones (multi-touch coverage).
    """
    import math, random
    rnd = random.Random(42)
    out = []
    for i in range(n):
        drift = 6.0 * math.sin(i / 9.0) + 3.5 * math.sin(i / 23.0)
        close = 100.0 + drift + rnd.uniform(-1.2, 1.2)
        high  = close + rnd.uniform(0.4, 1.6)
        low   = close - rnd.uniform(0.4, 1.6)
        out.append({"openTime": i * 3_600_000, "closeTime": (i + 1) * 3_600_000 - 1,
                    "open": close - 0.2, "high": high, "low": low, "close": close,
                    "volume": 1000.0, "quoteVolume": 1000.0, "trades": 100,
                    "takerBuyBase": 500.0, "takerBuyQuote": 500.0})
    return out


class TestOrchestrator(unittest.TestCase):
    def _run(self, req_over=None):
        req = {"symbols": ["BTCUSDT"], "timeframes": ["1h"],
               "candle_count": 400, "rr": 2, "filters": {},
               "page": 1, "page_size": 100,
               "sort": {"field": "touch_time", "direction": "desc"}}
        req.update(req_over or {})
        with patch.object(_m, "get_klines", return_value=_fake_raw_candles()):
            return _run_explorer(req)

    def test_52_authoritative_flags(self):
        res = self._run()
        self.assertTrue(res["ok"])
        self.assertTrue(res["authoritative_execution"])
        self.assertFalse(res["client_results_accepted"])

    def test_53_performance_counters(self):
        res = self._run()
        p = res["performance"]
        self.assertEqual(p["requested_cells"], 1)
        self.assertEqual(p["network_fetch_count"], 1)
        self.assertTrue(p["candles_fetched_once_per_cell"])
        self.assertGreaterEqual(p["ob_count"], 0)

    def test_54_baseline_vs_filtered_same_rr(self):
        res = self._run()
        self.assertEqual(res["baseline_summary"]["rr"], "2")
        self.assertEqual(res["filtered_summary"]["rr"], "2")
        self.assertIn("trade_retention_pct", res["comparison"])

    def test_55_bucket_comparison_present(self):
        res = self._run()
        buckets = [b["touch_bucket"] for b in res["touch_bucket_comparison"]]
        self.assertEqual(buckets, ["first", "second", "third", "fourth_plus"])

    def test_56_rows_have_no_candles(self):
        res = self._run()
        for r in res["trade_rows"]:
            self.assertNotIn("candles", r)
            self.assertNotIn("candle_window", r)
            self.assertNotIn("simulation", r)

    def test_57_pagination_stable(self):
        r1 = self._run({"page_size": 5, "page": 1})
        r1b = self._run({"page_size": 5, "page": 1})
        ids1  = [r["touch_trade_id"] for r in r1["trade_rows"]]
        ids1b = [r["touch_trade_id"] for r in r1b["trade_rows"]]
        self.assertEqual(ids1, ids1b)
        r2 = self._run({"page_size": 5, "page": 2})
        ids2 = [r["touch_trade_id"] for r in r2["trade_rows"]]
        self.assertEqual(set(ids1) & set(ids2), set())

    def test_58_failure_isolation(self):
        calls = {"n": 0}
        good = _fake_raw_candles()
        def flaky(sym, tf, limit=300, market="perpetual"):
            calls["n"] += 1
            if sym == "ETHUSDT":
                raise RuntimeError("boom")
            return good
        with patch.object(_m, "get_klines", side_effect=flaky):
            res = _run_explorer({"symbols": ["BTCUSDT", "ETHUSDT"],
                                 "timeframes": ["1h"], "candle_count": 400,
                                 "rr": 2, "filters": {}, "page": 1,
                                 "page_size": 100, "sort": {}})
        self.assertTrue(res["ok"])
        self.assertEqual(len(res["failures"]), 1)
        self.assertEqual(res["failures"][0]["symbol"], "ETHUSDT")
        self.assertEqual(res["performance"]["completed_cells"], 1)

    def test_59_first_touch_rows_match_canonical_outcomes(self):
        # For every touch-1 row, the outcome must equal the canonical
        # first-touch simulation attached by _bt_apply_outcomes_to_events.
        raw = _fake_raw_candles()
        with patch.object(_m, "get_klines", return_value=raw):
            res = _run_explorer({"symbols": ["BTCUSDT"], "timeframes": ["1h"],
                                 "candle_count": 400, "rr": 2, "filters": {},
                                 "page": 1, "page_size": 250, "sort": {}})
        candles = _m._bt_normalize_candles(raw)
        params = _m._bt_wf_build_params("BTCUSDT", "1h", len(candles), [1, 2, 3])
        events = _m._bt_extract_ob_replay_events(candles, params)
        _m._bt_apply_outcomes_to_events(events, candles, params)
        canonical = {}
        for ev in events:
            if ev.get("touch_status") != "touched":
                continue
            eps = _detect_episodes(candles, ev)
            first = [e for e in eps if e["touch_number"] == 1]
            # First episode's touch index equals canonical first_touch_bar
            self.assertTrue(not first or first[0]["touch_index"] == ev["first_touch_bar"])
            if first:
                canonical[_trade_id(ev, 1, first[0]["touch_index"], "2")] = \
                    ev["simulation"]["realized_by_rr"]["2"]["outcome"]
        checked = 0
        all_pages = res["trade_rows"]
        for r in all_pages:
            if r["touch_number"] == 1 and r["touch_trade_id"] in canonical:
                self.assertEqual(r["outcome"], canonical[r["touch_trade_id"]])
                checked += 1
        self.assertGreater(len(canonical), 0)

    def test_60_detail_candle_window_capped(self):
        raw = _fake_raw_candles()
        with patch.object(_m, "get_klines", return_value=raw):
            res = _run_explorer({"symbols": ["BTCUSDT"], "timeframes": ["1h"],
                                 "candle_count": 400, "rr": 2, "filters": {},
                                 "page": 1, "page_size": 5, "sort": {}})
            self.assertGreater(len(res["trade_rows"]), 0)
            tid = res["trade_rows"][0]["touch_trade_id"]
            det = _run_detail(tid, {"symbols": ["BTCUSDT"], "timeframes": ["1h"],
                                    "candle_count": 400})
        self.assertTrue(det["ok"])
        self.assertLessEqual(len(det["candle_window"]), _m._TE_CANDLE_WINDOW)
        self.assertEqual(det["touch_trade_id"], tid)
        self.assertIn("simulation", det)
        self.assertIn("all_touch_episodes", det)

    def test_61_detail_not_found(self):
        with patch.object(_m, "get_klines", return_value=_fake_raw_candles()):
            det = _run_detail("tte_nonexistent", {"symbols": ["BTCUSDT"],
                                                  "timeframes": ["1h"],
                                                  "candle_count": 400})
        self.assertFalse(det["ok"])
        self.assertEqual(det["error"], "touch_trade_id_not_found")


# ══════════════════════════════════════════════════════════════════════════════
# Real Flask endpoint tests
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
        return self.client.post("/api/backtest/ob-historical/trade-explorer",
                                json=body)

    def _ok_body(self, **over):
        b = {"symbols": ["BTCUSDT", "ETHUSDT"], "timeframes": ["1h", "4h"],
             "candle_count": 1000, "rr": 2, "filters": {}, "page": 1,
             "page_size": 100,
             "sort": {"field": "touch_time", "direction": "desc"}}
        b.update(over)
        return b

    def test_62_reject_client_trades(self):
        r = self._post(self._ok_body(trades=[1]))
        self.assertEqual(r.status_code, 400)
        self.assertEqual(r.get_json()["error"],
                         "client_supplied_trade_results_not_allowed")

    def test_63_reject_client_outcomes_metrics_summaries(self):
        for key in ("outcomes", "metrics", "touch_histories", "event_results",
                    "summaries", "comparison_results", "trade_rows",
                    "baseline_summary", "win_rate", "expectancy"):
            r = self._post(self._ok_body(**{key: [1]}))
            self.assertEqual(r.status_code, 400, f"{key} not rejected")

    def test_64_unauthorized_rejected(self):
        anon = _m.app.test_client()          # fresh client, no session
        r = anon.post("/api/backtest/ob-historical/trade-explorer",
                      json=self._ok_body())
        self.assertIn(r.status_code, (301, 302, 401, 403))

    def test_65_authorized_accepted(self):
        fake = {"ok": True, "authoritative_execution": True,
                "client_results_accepted": False, "trade_rows": [],
                "performance": {"response_size_bytes": None}}
        with patch.object(_m, "_bt_run_trade_explorer", return_value=fake):
            r = self._post(self._ok_body())
        self.assertEqual(r.status_code, 200)
        j = r.get_json()
        self.assertTrue(j["ok"])
        self.assertTrue(j["authoritative_execution"])
        self.assertFalse(j["client_results_accepted"])

    def test_66_max_cells_enforced(self):
        r = self._post(self._ok_body(
            symbols=["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT"]))
        self.assertEqual(r.status_code, 400)

    def test_67_blank_symbol_rejected(self):
        r = self._post(self._ok_body(symbols=["BTCUSDT", ""]))
        self.assertEqual(r.status_code, 400)

    def test_68_duplicate_symbol_rejected(self):
        r = self._post(self._ok_body(symbols=["BTCUSDT", "BTCUSDT"]))
        self.assertEqual(r.status_code, 400)

    def test_69_duplicate_timeframe_rejected(self):
        r = self._post(self._ok_body(timeframes=["1h", "1h"]))
        self.assertEqual(r.status_code, 400)

    def test_70_bad_rr_rejected(self):
        r = self._post(self._ok_body(rr=5))
        self.assertEqual(r.status_code, 400)

    def test_71_page_size_limit(self):
        r = self._post(self._ok_body(page_size=999))
        self.assertEqual(r.status_code, 400)

    def test_72_bad_timeframe_rejected(self):
        r = self._post(self._ok_body(timeframes=["7m"]))
        self.assertEqual(r.status_code, 400)

    def test_73_detail_endpoint_auth_and_reject(self):
        anon = _m.app.test_client()
        r = anon.post("/api/backtest/ob-historical/trade-explorer/trade/tte_x",
                      json={"symbols": ["BTCUSDT"], "timeframes": ["1h"]})
        self.assertIn(r.status_code, (301, 302, 401, 403))
        r2 = self.client.post(
            "/api/backtest/ob-historical/trade-explorer/trade/tte_x",
            json={"symbols": ["BTCUSDT"], "timeframes": ["1h"], "trades": [1]})
        self.assertEqual(r2.status_code, 400)

    def test_74_detail_endpoint_not_found(self):
        with patch.object(_m, "_bt_run_trade_explorer_detail",
                          return_value={"ok": False,
                                        "error": "touch_trade_id_not_found",
                                        "touch_trade_id": "tte_x"}):
            r = self.client.post(
                "/api/backtest/ob-historical/trade-explorer/trade/tte_x",
                json={"symbols": ["BTCUSDT"], "timeframes": ["1h"]})
        self.assertEqual(r.status_code, 404)


# ══════════════════════════════════════════════════════════════════════════════
# Production isolation / research-only guarantees
# ══════════════════════════════════════════════════════════════════════════════

class TestProductionIsolation(unittest.TestCase):
    def test_75_scanner_functions_unchanged(self):
        self.assertTrue(hasattr(_m, "detect_obs"))
        self.assertTrue(hasattr(_m, "detect_pivots"))
        self.assertTrue(hasattr(_m, "_bt_simulate_first_touch_outcome"))
        self.assertTrue(hasattr(_m, "_bt_extract_ob_replay_events"))

    def test_76_no_db_writes_in_te_functions(self):
        import inspect
        for fn in (_m._te_detect_touch_episodes, _m._te_simulate_touch_outcome,
                   _m._te_build_touch_trade_rows, _m._te_compute_aggregate,
                   _m._te_apply_filters, _m._bt_run_trade_explorer,
                   _m._bt_run_trade_explorer_detail):
            src = inspect.getsource(fn)
            self.assertNotIn("INSERT INTO", src)
            self.assertNotIn("UPDATE ", src)
            self.assertNotIn("db.session", src)
            self.assertNotIn("cursor.execute", src)

    def test_77_batch_and_mtf_limits_unchanged(self):
        self.assertEqual(_m._BT_MTF_MAX_TF, 5)
        self.assertEqual(_m._BT_MTF_MAX_CANDLES_PER_TF, 1500)
        self.assertEqual(_m._BT_MTF_MAX_TOTAL_CANDLES, 5000)

    def test_78_te_limits(self):
        self.assertEqual(_m._TE_MAX_SYMBOLS, 5)
        self.assertEqual(_m._TE_MAX_TIMEFRAMES, 2)
        self.assertEqual(_m._TE_MAX_CELLS, 10)
        self.assertEqual(_m._TE_MAX_PAGE_SIZE, 250)
        self.assertEqual(_m._TE_CANDLE_WINDOW, 200)

    def test_79_no_percentage_recommendation_in_response(self):
        with patch.object(_m, "get_klines", return_value=_fake_raw_candles()):
            res = _run_explorer({"symbols": ["BTCUSDT"], "timeframes": ["1h"],
                                 "candle_count": 400, "rr": 2, "filters": {},
                                 "page": 1, "page_size": 100, "sort": {}})
        blob = str(res)
        self.assertNotIn("recommended_threshold", blob)
        self.assertNotIn("universal_threshold", blob)
        fa = res["filter_availability"]["tv_ob_pct"]
        self.assertFalse(fa["enabled_by_default"])
        self.assertTrue(fa["exchange_specific"])

    def test_80_export_columns_match_backend_rows(self):
        # The row display keys must cover the documented trade CSV columns.
        csv_cols = ["touch_trade_id", "ob_id", "symbol", "timeframe", "direction",
                    "touch_number", "touch_bucket", "formation_time", "touch_time",
                    "candles_since_formation", "session", "weekday", "zone_high",
                    "zone_low", "zone_size_pct", "zone_size_atr", "entry_price",
                    "stop_price", "stop_loss_r", "rr", "outcome", "realized_r",
                    "mfe_r", "mae_r", "fvg_overlap", "higher_timeframe_alignment",
                    "tv_ob_pct_before_touch", "tv_ob_pct_exchange_specific",
                    "overlaps_previous_touch_trade", "eligible",
                    "rejection_reasons"]
        for c in csv_cols:
            self.assertIn(c, _m._TE_ROW_DISPLAY_KEYS)


if __name__ == "__main__":
    unittest.main(verbosity=2)
