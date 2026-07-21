"""
Phase 20 unit tests — Bad-Trade Filter Lab (research only).

Core guarantee under test: LABELS ONLY — no trade is ever removed from the
log. Grades, volume context, pattern book, and pass-rules are all overlays
on the full trade population.

Usage:
    cd /home/user/hhhhhh-hhhh && python3 _test_phase_20.py
"""
import os, sys, traceback, unittest, inspect, math, random
from unittest.mock import patch

os.environ.setdefault("DATABASE_URL", "sqlite:///phase20_test.db")
os.environ.setdefault("SECRET_KEY",   "phase20-test-key")
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


# ── Builders ─────────────────────────────────────────────────────────────────

def _vc(vol, i=0):
    return {"open_time": i * 3_600_000, "open": 100.0, "high": 101.0,
            "low": 99.0, "close": 100.0, "volume": float(vol)}


_REC_SEQ = 0

def _rec(outcome, grade=None, mfe=0.0, mae=0.0, realized=None,
         feat_over=None, htf_alignment="with", tf_alignment="with",
         touch_number=1, fvr=None, tvr=None, touch_time=0):
    global _REC_SEQ
    _REC_SEQ += 1
    if realized is None:
        realized = 2.0 if outcome == "win" else (-1.0 if outcome == "loss" else None)
    features = {k: False for k in _m._AP_FEATURE_KEYS}
    features.update(feat_over or {})
    if grade is None:
        grade = _m._bt_fl_grade(outcome, mfe, mae)
    return {"touch_trade_id": f"tte_{_REC_SEQ:06d}", "symbol": "BTCUSDT",
            "timeframe": "1h", "direction": "bullish", "rr": "2",
            "outcome": outcome, "realized_r": realized,
            "mfe_r": mfe, "mae_r": mae, "stop_loss_r": None,
            "performance_grade": grade, "features": features,
            "htf_alignment": htf_alignment, "tf_alignment": tf_alignment,
            "swing_alignment": "with", "session": "Asia", "weekday": "Monday",
            "touch_number": touch_number, "touch_bucket": "first",
            "touch_time": touch_time, "failure_mode": None,
            "formation_volume_ratio": fvr, "touch_volume_ratio": tvr,
            "alignments": {}}


def _fake_raw_candles(seed=42, n=1000):
    rnd = random.Random(seed)
    out = []
    for i in range(n):
        drift = 6.0 * math.sin(i / 9.0) + 3.5 * math.sin(i / 23.0)
        close = 100.0 + drift + rnd.uniform(-1.2, 1.2)
        vol = 800 + 600 * abs(math.sin(i / 7.0)) + rnd.uniform(0, 300)
        # varying taker-buy share so delta/CVD are non-trivial
        tb = vol * (0.5 + 0.3 * math.sin(i / 5.0))
        out.append({"openTime": i * 3_600_000, "closeTime": (i + 1) * 3_600_000 - 1,
                    "open": close - 0.2,
                    "high": close + rnd.uniform(0.4, 1.6),
                    "low":  close - rnd.uniform(0.4, 1.6),
                    "close": close, "volume": vol, "quoteVolume": vol * close,
                    "tradeCount": 100, "takerBuyBase": tb, "takerBuyQuote": tb * close})
    return out


def _det_gk():
    cache = {}
    def gk(sym, tf, limit=300, market="perpetual", extended=False):
        key = (sym, tf)
        if key not in cache:
            cache[key] = _fake_raw_candles(sum(ord(c) for c in sym + tf))
        return cache[key]
    return gk


# ══════════════════════════════════════════════════════════════════════════════
# Performance grades
# ══════════════════════════════════════════════════════════════════════════════

class TestGrades(unittest.TestCase):
    def test_01_clean_win(self):
        self.assertEqual(_m._bt_fl_grade("win", 2.0, 0.3), "clean_win")
        self.assertEqual(_m._bt_fl_grade("win", 2.0, 0.5), "clean_win")

    def test_02_stressed_win(self):
        self.assertEqual(_m._bt_fl_grade("win", 2.0, 0.51), "stressed_win")
        self.assertEqual(_m._bt_fl_grade("win", 2.0, 1.4), "stressed_win")

    def test_03_reversal_loss(self):
        self.assertEqual(_m._bt_fl_grade("loss", 1.0, 2.0), "reversal_loss")
        self.assertEqual(_m._bt_fl_grade("loss", 1.7, 2.0), "reversal_loss")

    def test_04_hard_loss(self):
        self.assertEqual(_m._bt_fl_grade("loss", 0.99, 2.0), "hard_loss")
        self.assertEqual(_m._bt_fl_grade("loss", 0.0, 2.0), "hard_loss")

    def test_05_paused(self):
        self.assertEqual(_m._bt_fl_grade("unresolved", 1.2, 0.2), "paused_positive")
        self.assertEqual(_m._bt_fl_grade("unresolved", 0.4, 0.2), "paused_flat")

    def test_06_ambiguous_and_none_inputs(self):
        self.assertEqual(_m._bt_fl_grade("ambiguous", 3.0, 0.0), "ambiguous")
        self.assertEqual(_m._bt_fl_grade("loss", None, None), "hard_loss")
        self.assertEqual(_m._bt_fl_grade("win", None, None), "clean_win")

    def test_07_distribution(self):
        recs = [_rec("win", mae=0.2), _rec("win", mae=0.9),
                _rec("loss", mfe=1.5), _rec("loss", mfe=0.2),
                _rec("unresolved", mfe=1.5), _rec("unresolved", mfe=0.1),
                _rec("ambiguous")]
        gd = _m._bt_fl_grade_distribution(recs)
        self.assertEqual(gd["total"], 7)
        self.assertEqual(gd["counts"]["clean_win"], 1)
        self.assertEqual(gd["counts"]["stressed_win"], 1)
        self.assertEqual(gd["counts"]["reversal_loss"], 1)
        self.assertEqual(gd["counts"]["hard_loss"], 1)
        self.assertEqual(gd["counts"]["paused_positive"], 1)
        self.assertEqual(gd["counts"]["paused_flat"], 1)
        self.assertEqual(gd["counts"]["ambiguous"], 1)
        self.assertEqual(gd["working"], 2)
        self.assertEqual(gd["failing"], 2)
        self.assertEqual(gd["paused"], 2)


# ══════════════════════════════════════════════════════════════════════════════
# Volume ratio
# ══════════════════════════════════════════════════════════════════════════════

class TestVolumeRatio(unittest.TestCase):
    def test_08_ratio_math(self):
        candles = [_vc(100, i) for i in range(20)] + [_vc(200, 20)]
        self.assertEqual(_m._bt_fl_volume_ratio(candles, 20), 2.0)

    def test_09_no_lookahead(self):
        base = [_vc(100, i) for i in range(30)]
        extended = base + [_vc(9999, 30 + i) for i in range(10)]
        self.assertEqual(_m._bt_fl_volume_ratio(base, 25),
                         _m._bt_fl_volume_ratio(extended, 25))

    def test_10_none_cases(self):
        candles = [_vc(100, i) for i in range(30)]
        self.assertIsNone(_m._bt_fl_volume_ratio(candles, 0))     # bar 0
        self.assertIsNone(_m._bt_fl_volume_ratio(candles, 99))    # out of range
        self.assertIsNone(_m._bt_fl_volume_ratio(candles, None))
        self.assertIsNone(_m._bt_fl_volume_ratio(candles, 3))     # < min samples
        zero = [_vc(0, i) for i in range(30)]
        self.assertIsNone(_m._bt_fl_volume_ratio(zero, 25))       # all zero volume

    def test_11_volume_features_in_keys(self):
        for k in ("HIGH_FORMATION_VOLUME", "LOW_FORMATION_VOLUME",
                  "HIGH_TOUCH_VOLUME", "LOW_TOUCH_VOLUME"):
            self.assertIn(k, _m._AP_FEATURE_KEYS)


# ══════════════════════════════════════════════════════════════════════════════
# Pattern book + volume by grade
# ══════════════════════════════════════════════════════════════════════════════

class TestPatternBook(unittest.TestCase):
    def test_12_present_vs_absent_mix(self):
        recs = ([_rec("loss", mfe=0.1, feat_over={"LATE_TOUCH": True})] * 6 +
                [_rec("win",  mae=0.1, feat_over={"LATE_TOUCH": True})] * 2 +
                [_rec("win",  mae=0.1, feat_over={"LATE_TOUCH": False})] * 8 +
                [_rec("loss", mfe=0.1, feat_over={"LATE_TOUCH": False})] * 2)
        book = _m._bt_fl_pattern_book(recs)
        lt = next(b for b in book if b["feature"] == "LATE_TOUCH")
        self.assertEqual(lt["present"]["count"], 8)
        self.assertEqual(lt["present"]["failing_pct"], 75.0)
        self.assertEqual(lt["absent"]["count"], 10)
        self.assertEqual(lt["absent"]["failing_pct"], 20.0)
        self.assertEqual(lt["failing_delta_pct"], 55.0)

    def test_13_unknown_counted_not_dropped(self):
        recs = [_rec("win", feat_over={"FVG_CONFLUENCE": None}),
                _rec("loss", mfe=0.1, feat_over={"FVG_CONFLUENCE": True})]
        book = _m._bt_fl_pattern_book(recs)
        fvg = next(b for b in book if b["feature"] == "FVG_CONFLUENCE")
        self.assertEqual(fvg["unknown_count"], 1)
        self.assertEqual(fvg["present"]["count"], 1)
        self.assertEqual(fvg["absent"]["count"], 0)

    def test_14_sorted_by_failing_delta(self):
        recs = ([_rec("loss", mfe=0.1, feat_over={"STALE_ZONE": True})] * 3 +
                [_rec("win", mae=0.1)] * 3)
        book = _m._bt_fl_pattern_book(recs)
        deltas = [b["failing_delta_pct"] for b in book
                  if b["failing_delta_pct"] is not None]
        self.assertEqual(deltas, sorted(deltas, reverse=True))

    def test_15_volume_by_grade(self):
        recs = [_rec("win", mae=0.1, fvr=1.5, tvr=1.2),
                _rec("win", mae=0.1, fvr=2.5, tvr=0.8),
                _rec("loss", mfe=0.1, fvr=0.5, tvr=0.6)]
        vg = _m._bt_fl_volume_by_grade(recs)
        cw = next(v for v in vg if v["grade"] == "clean_win")
        self.assertEqual(cw["count"], 2)
        self.assertEqual(cw["avg_formation_volume_ratio"], 2.0)
        self.assertEqual(cw["avg_touch_volume_ratio"], 1.0)
        hl = next(v for v in vg if v["grade"] == "hard_loss")
        self.assertEqual(hl["count"], 1)
        self.assertEqual(hl["avg_formation_volume_ratio"], 0.5)

    def test_16_volume_by_grade_none_safe(self):
        recs = [_rec("win", mae=0.1, fvr=None, tvr=None)]
        vg = _m._bt_fl_volume_by_grade(recs)
        cw = next(v for v in vg if v["grade"] == "clean_win")
        self.assertEqual(cw["count"], 1)
        self.assertIsNone(cw["avg_formation_volume_ratio"])


# ══════════════════════════════════════════════════════════════════════════════
# Pass rules — the core "labels only" guarantee
# ══════════════════════════════════════════════════════════════════════════════

class TestRules(unittest.TestCase):
    def test_17_no_trade_removed(self):
        recs = ([_rec("win", mae=0.1, htf_alignment="with")] * 5 +
                [_rec("loss", mfe=0.1, htf_alignment="against")] * 5 +
                [_rec("win", mae=0.1, htf_alignment="unknown")] * 2)
        n_before = len(recs)
        rv = _m._bt_fl_evaluate_rules(recs)
        self.assertEqual(len(recs), n_before)              # list untouched
        for r in recs:                                     # every trade labeled
            self.assertIn("rule_passes", r)
            self.assertEqual(set(r["rule_passes"].keys()),
                             {ru["id"] for ru in _m._bt_fl_rule_predicates()})
        # coverage invariant per rule: pass + fail + unknown == all trades
        for ru in rv["rules"]:
            self.assertEqual(ru["pass"]["opportunities"]
                             + ru["fail"]["opportunities"]
                             + ru["unknown_count"], n_before)

    def test_18_with_htf_rule_logic(self):
        recs = [_rec("win", htf_alignment="with"),
                _rec("win", htf_alignment="neutral"),
                _rec("loss", mfe=0.1, htf_alignment="against"),
                _rec("win", htf_alignment="unknown")]
        _m._bt_fl_evaluate_rules(recs)
        self.assertTrue(recs[0]["rule_passes"]["with_htf_trend"])
        self.assertTrue(recs[1]["rule_passes"]["with_htf_trend"])
        self.assertFalse(recs[2]["rule_passes"]["with_htf_trend"])
        self.assertIsNone(recs[3]["rule_passes"]["with_htf_trend"])

    def test_19_touch_rules(self):
        recs = [_rec("win", touch_number=1), _rec("win", touch_number=2),
                _rec("win", touch_number=3)]
        _m._bt_fl_evaluate_rules(recs)
        self.assertTrue(recs[0]["rule_passes"]["first_touch_only"])
        self.assertFalse(recs[1]["rule_passes"]["first_touch_only"])
        self.assertTrue(recs[1]["rule_passes"]["early_touch"])
        self.assertFalse(recs[2]["rule_passes"]["early_touch"])

    def test_20_feature_rules_none_propagates(self):
        recs = [_rec("win", feat_over={"WEAK_DISPLACEMENT": None,
                                       "OVERSIZED_ZONE": None})]
        _m._bt_fl_evaluate_rules(recs)
        self.assertIsNone(recs[0]["rule_passes"]["strong_displacement"])
        self.assertIsNone(recs[0]["rule_passes"]["not_oversized"])

    def test_21_volume_rule(self):
        recs = [_rec("win", fvr=1.2), _rec("win", fvr=0.8), _rec("win", fvr=None)]
        _m._bt_fl_evaluate_rules(recs)
        self.assertTrue(recs[0]["rule_passes"]["formation_volume_above_avg"])
        self.assertFalse(recs[1]["rule_passes"]["formation_volume_above_avg"])
        self.assertIsNone(recs[2]["rule_passes"]["formation_volume_above_avg"])

    def test_22_combo_quality_rule(self):
        good = _rec("win", htf_alignment="with", touch_number=1,
                    feat_over={"WEAK_DISPLACEMENT": False, "STALE_ZONE": False})
        late = _rec("win", htf_alignment="with", touch_number=3,
                    feat_over={"WEAK_DISPLACEMENT": False, "STALE_ZONE": False})
        weak = _rec("win", htf_alignment="with", touch_number=1,
                    feat_over={"WEAK_DISPLACEMENT": True, "STALE_ZONE": False})
        unk  = _rec("win", htf_alignment="unknown", touch_number=1,
                    feat_over={"WEAK_DISPLACEMENT": False, "STALE_ZONE": False})
        _m._bt_fl_evaluate_rules([good, late, weak, unk])
        self.assertTrue(good["rule_passes"]["combo_quality"])
        self.assertFalse(late["rule_passes"]["combo_quality"])
        self.assertFalse(weak["rule_passes"]["combo_quality"])
        self.assertIsNone(unk["rule_passes"]["combo_quality"])

    def test_23_retention_and_deltas(self):
        recs = ([_rec("win", htf_alignment="with")] * 6 +
                [_rec("loss", mfe=0.1, htf_alignment="against")] * 4)
        rv = _m._bt_fl_evaluate_rules(recs)
        rule = next(r for r in rv["rules"] if r["rule_id"] == "with_htf_trend")
        self.assertEqual(rule["pass"]["trades"], 6)
        self.assertEqual(rule["trade_retention_pct"], 60.0)
        self.assertEqual(rule["pass"]["win_rate_pct"], 100.0)
        self.assertEqual(rv["baseline"]["trades"], 10)
        self.assertEqual(rule["win_rate_delta_vs_baseline"], 40.0)

    def test_24_baseline_contract(self):
        recs = [_rec("win"), _rec("loss", mfe=0.1), _rec("ambiguous"),
                _rec("unresolved")]
        rv = _m._bt_fl_evaluate_rules(recs)
        b = rv["baseline"]
        self.assertEqual(b["trades"], 2)
        self.assertEqual(b["trades"], b["wins"] + b["losses"])
        self.assertEqual(b["ambiguous"], 1)
        self.assertEqual(b["unresolved"], 1)

    def test_25_empty_records_safe(self):
        rv = _m._bt_fl_evaluate_rules([])
        self.assertEqual(rv["baseline"]["trades"], 0)
        self.assertEqual(len(rv["rules"]), len(_m._bt_fl_rule_predicates()))


# ══════════════════════════════════════════════════════════════════════════════
# Trade log
# ══════════════════════════════════════════════════════════════════════════════

class TestTradeLog(unittest.TestCase):
    def test_26_full_population_no_removal(self):
        recs = [_rec("win", touch_time=i * 1000) for i in range(50)] + \
               [_rec("loss", mfe=0.1, touch_time=i * 1000 + 500) for i in range(50)]
        _m._bt_fl_evaluate_rules(recs)
        log = _m._bt_fl_trade_log(recs)
        self.assertEqual(log["total"], 100)
        self.assertEqual(log["returned"], 100)
        self.assertFalse(log["truncated"])
        ids_in  = {r["touch_trade_id"] for r in recs}
        ids_out = {r["touch_trade_id"] for r in log["rows"]}
        self.assertEqual(ids_in, ids_out)                  # nothing vanished

    def test_27_chronological_and_capped(self):
        recs = [_rec("win", touch_time=(1500 - i) * 1000)
                for i in range(1500)]
        _m._bt_fl_evaluate_rules(recs)
        log = _m._bt_fl_trade_log(recs)
        self.assertEqual(log["total"], 1500)
        self.assertEqual(log["returned"], _m._FL_TRADE_LOG_CAP)
        self.assertTrue(log["truncated"])
        times = [r["touch_time"] for r in log["rows"]]
        self.assertEqual(times, sorted(times))

    def test_28_rows_carry_labels(self):
        recs = [_rec("loss", mfe=0.1, fvr=0.6,
                     feat_over={"LATE_TOUCH": True, "STALE_ZONE": True})]
        _m._bt_fl_evaluate_rules(recs)
        log = _m._bt_fl_trade_log(recs)
        row = log["rows"][0]
        self.assertEqual(row["performance_grade"], "hard_loss")
        self.assertIn("LATE_TOUCH", row["pattern_tags"])
        self.assertIn("STALE_ZONE", row["pattern_tags"])
        self.assertEqual(row["formation_volume_ratio"], 0.6)
        self.assertIn("with_htf_trend", row["rule_passes"])


# ══════════════════════════════════════════════════════════════════════════════
# Full pipeline (mocked get_klines)
# ══════════════════════════════════════════════════════════════════════════════

class TestPipeline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with patch.object(_m, "get_klines", side_effect=_det_gk()):
            cls.res = _m._bt_run_autopsy({
                "symbols": ["BTCUSDT"], "timeframes": ["1h"],
                "candle_count": 1000, "rr": 2,
                "ob_classes": ["internal", "swing"]})

    def test_29_all_sections_present(self):
        for cls_name in ("internal", "swing"):
            rep = self.res["reports_by_class"][cls_name]
            for key in ("grade_distribution", "pattern_book",
                        "volume_by_grade", "rule_evaluation", "trade_log"):
                self.assertIn(key, rep)

    def test_30_log_covers_all_records(self):
        for cls_name in ("internal", "swing"):
            rep = self.res["reports_by_class"][cls_name]
            self.assertEqual(rep["trade_log"]["total"], rep["records_total"])

    def test_31_rule_coverage_invariant(self):
        for cls_name in ("internal", "swing"):
            rep = self.res["reports_by_class"][cls_name]
            total = rep["records_total"]
            for ru in rep["rule_evaluation"]["rules"]:
                self.assertEqual(ru["pass"]["opportunities"]
                                 + ru["fail"]["opportunities"]
                                 + ru["unknown_count"], total)

    def test_32_grades_cover_all_records(self):
        for cls_name in ("internal", "swing"):
            rep = self.res["reports_by_class"][cls_name]
            gd = rep["grade_distribution"]
            self.assertEqual(gd["total"], rep["records_total"])
            self.assertEqual(sum(gd["counts"].values()), rep["records_total"])

    def test_33_volume_ratios_populated(self):
        rep = self.res["reports_by_class"]["internal"]
        rows = rep["trade_log"]["rows"]
        with_vol = [r for r in rows if r["formation_volume_ratio"] is not None]
        self.assertGreater(len(with_vol), 0)

    def test_34_definitions_documented(self):
        defs = self.res["definitions"]
        for key in ("performance_grades", "volume_ratio", "rules"):
            self.assertIn(key, defs)

    def test_35_classes_never_pooled(self):
        ri = self.res["reports_by_class"]["internal"]
        rs = self.res["reports_by_class"]["swing"]
        ids_i = {r["touch_trade_id"] for r in ri["trade_log"]["rows"]}
        ids_s = {r["touch_trade_id"] for r in rs["trade_log"]["rows"]}
        # IDs are class-independent hashes of (ob, touch, rr) — but the two
        # classes detect different OBs, so their log populations must differ.
        self.assertNotEqual(ids_i, ids_s)

    def test_36_deterministic(self):
        with patch.object(_m, "get_klines", side_effect=_det_gk()):
            again = _m._bt_run_autopsy({
                "symbols": ["BTCUSDT"], "timeframes": ["1h"],
                "candle_count": 1000, "rr": 2,
                "ob_classes": ["internal", "swing"]})
        a = self.res["reports_by_class"]["internal"]["trade_log"]["rows"]
        b = again["reports_by_class"]["internal"]["trade_log"]["rows"]
        self.assertEqual([(r["touch_trade_id"], r["performance_grade"]) for r in a],
                         [(r["touch_trade_id"], r["performance_grade"]) for r in b])


# ══════════════════════════════════════════════════════════════════════════════
# Isolation
# ══════════════════════════════════════════════════════════════════════════════

class TestIsolation(unittest.TestCase):
    def test_37_no_db_writes(self):
        for fn in (_m._bt_fl_grade, _m._bt_fl_volume_ratio,
                   _m._bt_fl_grade_distribution, _m._bt_fl_pattern_book,
                   _m._bt_fl_volume_by_grade, _m._bt_fl_evaluate_rules,
                   _m._bt_fl_trade_log):
            src = inspect.getsource(fn)
            self.assertNotIn("INSERT INTO", src)
            self.assertNotIn("db.session", src)
            self.assertNotIn("cursor.execute", src)

    def test_38_no_production_filter_activation(self):
        # Rules live only in the research module; scanner/monitor untouched.
        for fn in (_m._bt_fl_rule_predicates, _m._bt_fl_evaluate_rules):
            src = inspect.getsource(fn)
            self.assertNotIn("scanner", src.lower())
            self.assertNotIn("live_monitor", src.lower())
            self.assertNotIn("alert", src.lower())
        self.assertEqual(_m._BT_PIVOT_LEN, 5)
        self.assertEqual(_m._BT_SWING_PIVOT_LEN, 30)

    def test_39_grade_constants(self):
        self.assertEqual(_m._FL_CLEAN_WIN_MAX_MAE, 0.5)
        self.assertEqual(_m._FL_REVERSAL_LOSS_MIN_MFE, 1.0)
        self.assertEqual(_m._FL_HIGH_VOL_RATIO, 1.5)
        self.assertEqual(_m._FL_LOW_VOL_RATIO, 0.7)
        self.assertEqual(_m._FL_TRADE_LOG_CAP, 1000)


# ══════════════════════════════════════════════════════════════════════════════
# Verification-pass regressions (defects caught by the adversarial review)
# ══════════════════════════════════════════════════════════════════════════════

class TestVerificationRegressions(unittest.TestCase):
    def test_40_win_mae_bounded_at_exit(self):
        """A post-exit crash must NOT flip a clean win to stressed_win.

        Scenario: bullish zone [95,100]. Touch at bar 1, 2R TP (110) hit at
        bar 2 with zero adverse movement, then price collapses far below the
        entry AFTER the exit. The sim's raw MAE keeps running (stop is close-
        based and hit later) — the grade must use the exit-bounded MAE.
        """
        def c(h, l, cl, i):
            return {"open_time": i * 3_600_000, "open": cl, "high": float(h),
                    "low": float(l), "close": float(cl), "volume": 100.0}
        candles = [c(120, 110, 115, 0),      # formation-ish, outside zone
                   c(99, 98, 98.5, 1),        # touch (inside zone), MAE 0.4R max
                   c(111, 98, 110, 2),        # 2R TP hit (>=110), low 98 (0.4R)
                   c(98, 60, 62, 3),          # post-exit collapse (close < 95)
                   c(62, 50, 55, 4)]
        ev = {"ob_id": "x", "symbol": "BTCUSDT", "timeframe": "1h",
              "type": "bullish", "formation_bar": 0,
              "formation_time": 0, "zone_high": 100.0, "zone_low": 95.0,
              "zone_size": 5.0, "zone_size_pct": 5.26, "zone_mid": 97.5,
              "touch_status": "touched", "first_touch_bar": 1,
              "first_touch_time": 3_600_000, "later_mitigated": True,
              "mitigation_bar": 3, "mitigation_time": 3 * 3_600_000}
        atr = _m._bt_ap_atr_series(candles)
        idx = _m._bt_ap_build_trend_index(candles, 5)
        recs = _m._bt_ap_build_trade_records(
            [ev], candles, "2", "internal", idx, idx, None, None, atr)
        self.assertEqual(len(recs), 1)
        r = recs[0]
        self.assertEqual(r["outcome"], "win")
        # raw sim MAE would be huge ((100-50)/5 = 10R); bounded must be 0.8R
        # (low 96 at touch bar → (100-96)/5, and low 98 on exit bar)
        self.assertLessEqual(r["mae_r"], _m._FL_CLEAN_WIN_MAX_MAE)
        self.assertEqual(r["performance_grade"], "clean_win")

    def test_41_excursions_helper_math(self):
        def c(h, l, cl, i):
            return {"open_time": i, "open": cl, "high": float(h),
                    "low": float(l), "close": float(cl), "volume": 1.0}
        ev = {"type": "bullish", "zone_high": 100.0, "zone_low": 95.0}
        candles = [c(101, 98, 99, 0), c(108, 97, 107, 1), c(90, 80, 85, 2)]
        mfe, mae = _m._bt_fl_excursions_to_exit(ev, candles, 0, 1)
        self.assertEqual(mfe, 1.6)   # (108-100)/5
        self.assertEqual(mae, 0.6)   # (100-97)/5 — bar 2 crash excluded
        # invalid ranges → None
        self.assertEqual(_m._bt_fl_excursions_to_exit(ev, candles, 2, 1),
                         (None, None))
        bad = {"type": "bullish", "zone_high": 95.0, "zone_low": 95.0}
        self.assertEqual(_m._bt_fl_excursions_to_exit(bad, candles, 0, 1),
                         (None, None))

    def test_42_loss_grades_unchanged_by_fix(self):
        # Losses stop at the stop bar in the sim — grades must still use the
        # sim excursions (reversal vs hard loss distinction intact).
        self.assertEqual(_m._bt_fl_grade("loss", 1.5, 2.0), "reversal_loss")
        self.assertEqual(_m._bt_fl_grade("loss", 0.3, 2.0), "hard_loss")

    def test_43_pipeline_log_rows_carry_rule_passes(self):
        # Hardened ordering: class report must produce log rows with the
        # full non-empty rule verdict map (would catch a reordering bug).
        with patch.object(_m, "get_klines", side_effect=_det_gk()):
            res = _m._bt_run_autopsy({
                "symbols": ["BTCUSDT"], "timeframes": ["1h"],
                "candle_count": 1000, "rr": 2, "ob_classes": ["internal"]})
        rows = res["reports_by_class"]["internal"]["trade_log"]["rows"]
        self.assertGreater(len(rows), 0)
        expected_rules = {ru["id"] for ru in _m._bt_fl_rule_predicates()}
        for r in rows:
            self.assertEqual(set(r["rule_passes"].keys()), expected_rules)


# ══════════════════════════════════════════════════════════════════════════════
# Chunk 2 — Respect framing (labels only)
# ══════════════════════════════════════════════════════════════════════════════

class TestRespectClass(unittest.TestCase):
    def test_44_grade_to_respect_mapping(self):
        self.assertEqual(_m._bt_fl_respect_class("clean_win"), "respected")
        self.assertEqual(_m._bt_fl_respect_class("stressed_win"), "respected")
        self.assertEqual(_m._bt_fl_respect_class("reversal_loss"), "partial")
        self.assertEqual(_m._bt_fl_respect_class("paused_positive"), "partial")
        self.assertEqual(_m._bt_fl_respect_class("hard_loss"), "not_respected")
        self.assertEqual(_m._bt_fl_respect_class("paused_flat"), "neutral")
        self.assertEqual(_m._bt_fl_respect_class("ambiguous"), "neutral")
        self.assertEqual(_m._bt_fl_respect_class("unknown_grade"), "neutral")

    def test_45_every_grade_maps(self):
        for g in _m._FL_GRADES:
            self.assertIn(_m._bt_fl_respect_class(g), _m._FL_RESPECT_CLASSES)


class TestHtfJointState(unittest.TestCase):
    js = staticmethod(lambda a: _m._bt_fl_htf_joint_state(a))

    def test_46_all_with_against(self):
        self.assertEqual(self.js({"1h": "with", "4h": "with"}), "all_with")
        self.assertEqual(self.js({"1h": "against", "4h": "against"}), "all_against")
        self.assertEqual(self.js({"1d": "with"}), "all_with")

    def test_47_conflicting(self):
        self.assertEqual(self.js({"1h": "with", "4h": "against"}), "conflicting")
        self.assertEqual(self.js({"1h": "against", "4h": "with"}), "conflicting")

    def test_48_partial_neutral(self):
        self.assertEqual(self.js({"1h": "with", "4h": "neutral"}), "with_some_neutral")
        self.assertEqual(self.js({"1h": "against", "4h": "neutral"}), "against_some_neutral")
        self.assertEqual(self.js({"1h": "neutral", "4h": "neutral"}), "all_neutral")

    def test_49_unknown(self):
        self.assertEqual(self.js({"1h": "with", "4h": "unknown"}), "unknown")
        self.assertEqual(self.js({}), "unknown")
        self.assertEqual(self.js(None), "unknown")


class TestRespectCounts(unittest.TestCase):
    def _r(self, respect_class, tn=1, alignments=None):
        return {"respect_class": respect_class, "touch_number": tn,
                "timeframe": "15m",
                "alignments": alignments if alignments is not None else {"1h": "with"},
                "htf_joint_state": "all_with"}

    def test_50_counts_and_rate(self):
        rows = ([self._r("respected")] * 6 + [self._r("partial")] * 2 +
                [self._r("not_respected")] * 2 + [self._r("neutral")] * 3)
        c = _m._bt_fl_respect_counts(rows)
        self.assertEqual(c["total"], 13)
        self.assertEqual(c["counts"]["respected"], 6)
        self.assertEqual(c["classified"], 10)         # excludes 3 neutral
        self.assertEqual(c["respected_rate_pct"], 60.0)  # 6/10
        self.assertEqual(c["not_respected_rate_pct"], 20.0)

    def test_51_empty_safe(self):
        c = _m._bt_fl_respect_counts([])
        self.assertEqual(c["total"], 0)
        self.assertIsNone(c["respected_rate_pct"])

    def test_52_summary_touch_buckets_and_invariant(self):
        rows = ([self._r("respected", tn=1)] * 5 +
                [self._r("not_respected", tn=1)] * 2 +
                [self._r("respected", tn=2)] * 3 +
                [self._r("partial", tn=3)] * 2 +
                [self._r("neutral", tn=5)] * 1)
        s = _m._bt_fl_respect_summary(rows)
        self.assertEqual(s["primary_view"], "first")
        first = next(b for b in s["by_touch"] if b["touch_bucket"] == "first")
        self.assertEqual(first["total"], 7)
        self.assertEqual(first["respected_rate_pct"], round(5/7*100, 1))
        fourth = next(b for b in s["by_touch"] if b["touch_bucket"] == "fourth_plus")
        self.assertEqual(fourth["total"], 1)
        # invariant: overall counts sum to all records
        self.assertEqual(sum(s["overall"]["counts"].values()), len(rows))
        # invariant: touch buckets partition the records
        self.assertEqual(sum(b["total"] for b in s["by_touch"]), len(rows))

    def test_53_htf_state_table_excludes_no_alignment(self):
        rows = ([self._r("respected", alignments={"1h": "with", "4h": "with"})] * 4 +
                [self._r("not_respected", alignments={"1h": "against", "4h": "against"})] * 3 +
                [self._r("respected", alignments={})] * 2)  # no mapped TFs → excluded
        # give the no-alignment rows a distinct joint state so grouping is clear
        for r in rows:
            r["htf_joint_state"] = _m._bt_fl_htf_joint_state(r["alignments"])
        table = _m._bt_fl_respect_by_htf_state(rows)
        total_in_table = sum(c["total"] for c in table)
        self.assertEqual(total_in_table, 7)   # 2 no-alignment rows excluded
        states = {c["htf_joint_state"] for c in table}
        self.assertIn("all_with", states)
        self.assertIn("all_against", states)


class TestRespectPipeline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with patch.object(_m, "get_klines", side_effect=_det_gk()):
            cls.res = _m._bt_run_autopsy({
                "symbols": ["BTCUSDT"], "timeframes": ["15m", "1h"],
                "candle_count": 1500, "rr": 2, "ob_classes": ["internal", "swing"]})

    def test_54_respect_sections_present(self):
        for cls_name in ("internal", "swing"):
            rep = self.res["reports_by_class"][cls_name]
            self.assertIn("respect_summary", rep)
            self.assertIn("respect_by_htf_state", rep)
            self.assertIn("overall", rep["respect_summary"])
            self.assertEqual(len(rep["respect_summary"]["by_touch"]), 4)

    def test_55_respect_counts_cover_all_records(self):
        for cls_name in ("internal", "swing"):
            rep = self.res["reports_by_class"][cls_name]
            ov = rep["respect_summary"]["overall"]
            self.assertEqual(sum(ov["counts"].values()), rep["records_total"])

    def test_56_log_rows_carry_respect_fields(self):
        rows = self.res["reports_by_class"]["internal"]["trade_log"]["rows"]
        self.assertGreater(len(rows), 0)
        for r in rows:
            self.assertIn(r["respect_class"], _m._FL_RESPECT_CLASSES)
            self.assertIn("htf_joint_state", r)

    def test_57_definitions_documented(self):
        defs = self.res["definitions"]
        self.assertIn("respect_class", defs)
        self.assertIn("htf_joint_state", defs)

    def test_58_respect_consistent_with_grade(self):
        # respect_class must equal the mapping of the row's performance grade
        rows = self.res["reports_by_class"]["internal"]["trade_log"]["rows"]
        for r in rows:
            self.assertEqual(r["respect_class"],
                             _m._bt_fl_respect_class(r["performance_grade"]))


# ══════════════════════════════════════════════════════════════════════════════
# Chunk 3 — RSI + CVD divergence at touch (primary signals)
# ══════════════════════════════════════════════════════════════════════════════

def _cvd_candle(i, high, low, close, taker_buy, vol=1000.0):
    return {"open_time": i * 3_600_000, "open": close, "high": float(high),
            "low": float(low), "close": float(close), "volume": vol,
            "taker_buy_base": taker_buy}


class TestCvdDelta(unittest.TestCase):
    def test_59_cvd_series_and_delta(self):
        cds = [_cvd_candle(i, 101 + i, 99 + i, 100 + i, 700.0) for i in range(30)]
        cvd = _m._bt_fl_cvd_series(cds)
        self.assertIsNotNone(cvd[0])
        self.assertAlmostEqual(cvd[0], 400.0)          # 2*700 - 1000
        self.assertAlmostEqual(cvd[1], 800.0)          # running sum
        self.assertEqual(_m._bt_fl_bar_delta(cds[0]), 400.0)

    def test_60_cvd_all_none_without_taker(self):
        cds = [{"open_time": i, "open": 100, "high": 101, "low": 99,
                "close": 100, "volume": 1000.0} for i in range(30)]
        self.assertTrue(all(v is None for v in _m._bt_fl_cvd_series(cds)))
        self.assertIsNone(_m._bt_fl_bar_delta(cds[0]))

    def test_61_cvd_no_lookahead(self):
        base = [_cvd_candle(i, 101, 99, 100, 600.0) for i in range(40)]
        ext  = base + [_cvd_candle(40 + i, 101, 99, 100, 9999.0) for i in range(10)]
        cvd_a = _m._bt_fl_cvd_series(base)
        cvd_b = _m._bt_fl_cvd_series(ext)
        for i in range(40):
            self.assertEqual(cvd_a[i], cvd_b[i])


class TestDivergence(unittest.TestCase):
    def _trend(self, low_bars=(), high_bars=(), pivot_len=5):
        return {"low_bars": list(low_bars), "high_bars": list(high_bars),
                "pivot_len": pivot_len, "h": [], "l": []}

    def test_62_bullish_divergence_true(self):
        cc = [{"open_time": i, "open": 100, "high": 110, "low": 95,
               "close": 100, "volume": 1} for i in range(40)]
        cc[10]["low"] = 90.0; cc[30]["low"] = 85.0   # price lower low
        s = [None] * 40; s[10] = 20.0; s[30] = 30.0   # indicator higher low
        self.assertTrue(_m._bt_fl_divergence_at_touch(
            "bullish", 30, cc, s, self._trend(low_bars=[10])))

    def test_63_bullish_no_divergence(self):
        cc = [{"open_time": i, "open": 100, "high": 110, "low": 95,
               "close": 100, "volume": 1} for i in range(40)]
        cc[10]["low"] = 90.0; cc[30]["low"] = 85.0
        s = [None] * 40; s[10] = 20.0; s[30] = 15.0   # indicator also lower → no div
        self.assertFalse(_m._bt_fl_divergence_at_touch(
            "bullish", 30, cc, s, self._trend(low_bars=[10])))

    def test_64_bearish_divergence_true(self):
        cc = [{"open_time": i, "open": 100, "high": 105, "low": 95,
               "close": 100, "volume": 1} for i in range(40)]
        cc[10]["high"] = 110.0; cc[30]["high"] = 115.0  # price higher high
        s = [None] * 40; s[10] = 80.0; s[30] = 70.0     # indicator lower high
        self.assertTrue(_m._bt_fl_divergence_at_touch(
            "bearish", 30, cc, s, self._trend(high_bars=[10])))

    def test_65_none_without_prior_pivot(self):
        cc = [{"open_time": i, "open": 100, "high": 110, "low": 95,
               "close": 100, "volume": 1} for i in range(40)]
        s = [None] * 40; s[30] = 30.0
        # pivot at bar 10 but confirmation needs touch-pivot_len; touch=8 → cutoff=3 → no pivot
        self.assertIsNone(_m._bt_fl_divergence_at_touch(
            "bullish", 8, cc, s, self._trend(low_bars=[10])))

    def test_66_none_when_indicator_missing(self):
        cc = [{"open_time": i, "open": 100, "high": 110, "low": 95,
               "close": 100, "volume": 1} for i in range(40)]
        cc[10]["low"] = 90.0; cc[30]["low"] = 85.0
        s = [None] * 40   # no indicator values at all
        self.assertIsNone(_m._bt_fl_divergence_at_touch(
            "bullish", 30, cc, s, self._trend(low_bars=[10])))

    def test_67_confirmed_pivot_only(self):
        # touch at 30, pivot_len 5 → cutoff 25; pivot at 28 is NOT yet confirmed
        cc = [{"open_time": i, "open": 100, "high": 110, "low": 95,
               "close": 100, "volume": 1} for i in range(40)]
        cc[28]["low"] = 90.0; cc[30]["low"] = 85.0
        s = [None] * 40; s[28] = 20.0; s[30] = 30.0
        # only pivot bar is 28 (> cutoff 25) → not confirmed → None
        self.assertIsNone(_m._bt_fl_divergence_at_touch(
            "bullish", 30, cc, s, self._trend(low_bars=[28])))


class TestExtendedFetchAndFeatures(unittest.TestCase):
    def test_68_new_feature_keys(self):
        for k in ("RSI_DIVERGENCE_AT_TOUCH", "CVD_DIVERGENCE_AT_TOUCH",
                  "RSI_OVERSOLD_AT_TOUCH", "RSI_OVERBOUGHT_AT_TOUCH"):
            self.assertIn(k, _m._AP_FEATURE_KEYS)

    def test_69_get_klines_extended_default_off(self):
        # Production callers (no extended kwarg) get the plain 6-field dicts.
        import inspect as _insp
        sig = _insp.signature(_m.get_klines)
        self.assertIn("extended", sig.parameters)
        self.assertEqual(sig.parameters["extended"].default, False)

    def test_70_normalize_carries_taker(self):
        norm = _m._bt_normalize_candles([{
            "openTime": 0, "open": "100", "high": "101", "low": "99",
            "close": "100.5", "volume": "1000", "takerBuyBase": "600"}])
        self.assertEqual(norm[0]["taker_buy_base"], 600.0)

    def test_71_pipeline_divergence_fields(self):
        with patch.object(_m, "get_klines", side_effect=_det_gk()):
            res = _m._bt_run_autopsy({
                "symbols": ["BTCUSDT"], "timeframes": ["15m", "1h"],
                "candle_count": 1500, "rr": 2, "ob_classes": ["internal"]})
        rows = res["reports_by_class"]["internal"]["trade_log"]["rows"]
        self.assertGreater(len(rows), 0)
        for r in rows:
            self.assertIn("rsi_at_touch", r)
            self.assertIn("cvd_at_touch", r)
            self.assertIn("rsi_divergence", r)
            self.assertIn("cvd_divergence", r)
            # divergence is True / False / None only
            self.assertIn(r["rsi_divergence"], (True, False, None))
            self.assertIn(r["cvd_divergence"], (True, False, None))
        # with taker data present, at least some CVD divergences are evaluable
        evaluable = sum(1 for r in rows if r["cvd_divergence"] is not None)
        self.assertGreater(evaluable, 0)
        # features present in the pattern book
        pb = {p["feature"] for p in res["reports_by_class"]["internal"]["pattern_book"]}
        self.assertIn("RSI_DIVERGENCE_AT_TOUCH", pb)
        self.assertIn("CVD_DIVERGENCE_AT_TOUCH", pb)

    def test_72_definitions_documented(self):
        with patch.object(_m, "get_klines", side_effect=_det_gk()):
            res = _m._bt_run_autopsy({
                "symbols": ["BTCUSDT"], "timeframes": ["1h"],
                "candle_count": 1000, "rr": 2, "ob_classes": ["internal"]})
        for k in ("rsi_divergence", "cvd_divergence", "rsi_flags"):
            self.assertIn(k, res["definitions"])

    def test_73_feature_consistency_with_row(self):
        # the RSI_DIVERGENCE feature must equal the row's rsi_divergence field
        candles = _m._bt_normalize_candles(_fake_raw_candles(seed=7, n=1200))
        params = _m._bt_wf_build_params("BTCUSDT", "1h", len(candles), [1, 2, 3])
        events = _m._bt_extract_ob_replay_events(candles, params)
        atr = _m._bt_ap_atr_series(candles)
        idx = _m._bt_ap_build_trend_index(candles, _m._BT_PIVOT_LEN)
        sidx = _m._bt_ap_build_trend_index(candles, _m._BT_SWING_PIVOT_LEN)
        closes = [c["close"] for c in candles]
        rsi = _m.calc_rsi(closes, _m._FL_RSI_PERIOD)
        cvd = _m._bt_fl_cvd_series(candles)
        recs = _m._bt_ap_build_trade_records(
            events, candles, "2", "internal", idx, sidx, None, None, atr,
            rsi_series=rsi, cvd_series=cvd)
        self.assertGreater(len(recs), 0)
        for r in recs:
            self.assertEqual(r["features"]["RSI_DIVERGENCE_AT_TOUCH"], r["rsi_divergence"])
            self.assertEqual(r["features"]["CVD_DIVERGENCE_AT_TOUCH"], r["cvd_divergence"])


# ══════════════════════════════════════════════════════════════════════════════
# Chunk 4 — Open Interest context (last-30-days window)
# ══════════════════════════════════════════════════════════════════════════════

class TestOiLookup(unittest.TestCase):
    def _oi(self, n=100, base=1000.0, step=10.0):
        return [(i * 3_600_000, base + i * step) for i in range(n)]

    def test_74_oi_at_basic(self):
        oi = self._oi()
        val, avail = _m._bt_fl_oi_at(oi, 50 * 3_600_000)
        self.assertTrue(avail)
        self.assertEqual(val, 1500.0)

    def test_75_oi_at_between_points(self):
        oi = self._oi()
        # touch between bar 50 and 51 → uses bar 50 (last <= touch)
        val, avail = _m._bt_fl_oi_at(oi, 50 * 3_600_000 + 1800_000)
        self.assertTrue(avail)
        self.assertEqual(val, 1500.0)

    def test_76_oi_before_window_not_available(self):
        oi = self._oi()
        val, avail = _m._bt_fl_oi_at(oi, -5)
        self.assertIsNone(val)
        self.assertFalse(avail)

    def test_77_oi_empty_not_available(self):
        val, avail = _m._bt_fl_oi_at([], 100)
        self.assertIsNone(val)
        self.assertFalse(avail)
        self.assertIsNone(_m._bt_fl_oi_at(self._oi(), None)[0])

    def test_78_oi_context_rising(self):
        ctx = _m._bt_fl_oi_context(self._oi(step=10.0), 50 * 3_600_000)
        self.assertTrue(ctx["available"])
        self.assertGreater(ctx["oi_change_pct"], 0)
        self.assertTrue(ctx["rising"])
        self.assertFalse(ctx["falling"])

    def test_79_oi_context_falling(self):
        ctx = _m._bt_fl_oi_context(self._oi(base=3000.0, step=-40.0), 50 * 3_600_000)
        self.assertLess(ctx["oi_change_pct"], 0)
        self.assertTrue(ctx["falling"])
        self.assertFalse(ctx["rising"])

    def test_80_oi_context_insufficient_lookback(self):
        # touch at bar 3, lookback 6 → no prior point → change None but available
        ctx = _m._bt_fl_oi_context(self._oi(), 3 * 3_600_000)
        self.assertTrue(ctx["available"])
        self.assertIsNone(ctx["oi_change_pct"])
        self.assertIsNone(ctx["rising"])

    def test_81_oi_context_no_lookahead(self):
        # OI change at a touch uses only points <= touch; future OI can't leak
        oi = self._oi(n=100)
        ctx_a = _m._bt_fl_oi_context(oi, 40 * 3_600_000)
        ctx_b = _m._bt_fl_oi_context(oi[:41], 40 * 3_600_000)   # truncated after touch
        self.assertEqual(ctx_a["oi_change_pct"], ctx_b["oi_change_pct"])

    def test_82_oi_feature_keys(self):
        self.assertIn("OI_RISING_AT_TOUCH", _m._AP_FEATURE_KEYS)
        self.assertIn("OI_FALLING_AT_TOUCH", _m._AP_FEATURE_KEYS)


class TestOiPipeline(unittest.TestCase):
    def test_83_no_oi_all_not_available(self):
        with patch.object(_m, "get_klines", side_effect=_det_gk()), \
             patch.object(_m, "_bt_fl_fetch_oi_history", return_value=[]):
            res = _m._bt_run_autopsy({
                "symbols": ["BTCUSDT"], "timeframes": ["1h"],
                "candle_count": 1500, "rr": 2, "ob_classes": ["internal"]})
        rep = res["reports_by_class"]["internal"]
        self.assertEqual(rep["oi_coverage"]["available"], 0)
        self.assertEqual(rep["oi_coverage"]["coverage_pct"], 0.0)
        self.assertEqual(res["performance"]["oi_fetch_count"], 0)
        for r in rep["trade_log"]["rows"]:
            self.assertFalse(r["oi_available"])
            self.assertIsNone(r["oi_at_touch"])
            # OI features must be None (unknown), never fabricated False
            # (verified via record features below)

    def test_84_oi_available_coverage_and_features(self):
        # _det_gk returns 1000 candles (bars 0-999). OI covers the LAST half
        # (bars 500-999) so coverage is partial: recent touches available,
        # older touches "not available".
        def oi_hist(sym, period, now_ms=None):
            return [(i * 3_600_000, 1000.0 + 50.0 * math.sin(i / 8.0) + i * 0.5)
                    for i in range(500, 1000)]
        with patch.object(_m, "get_klines", side_effect=_det_gk()), \
             patch.object(_m, "_bt_fl_fetch_oi_history", side_effect=oi_hist):
            res = _m._bt_run_autopsy({
                "symbols": ["BTCUSDT"], "timeframes": ["1h"],
                "candle_count": 1000, "rr": 2, "ob_classes": ["internal"]})
        rep = res["reports_by_class"]["internal"]
        cov = rep["oi_coverage"]
        self.assertGreater(cov["available"], 0)
        self.assertLess(cov["available"], cov["total"])   # partial coverage
        self.assertEqual(res["performance"]["oi_fetch_count"], 1)
        pb = {p["feature"] for p in rep["pattern_book"]}
        self.assertIn("OI_RISING_AT_TOUCH", pb)
        self.assertIn("OI_FALLING_AT_TOUCH", pb)
        # not-available trades → None OI fields; available → numeric
        for r in rep["trade_log"]["rows"]:
            if not r["oi_available"]:
                self.assertIsNone(r["oi_at_touch"])
            else:
                self.assertIsNotNone(r["oi_at_touch"])

    def test_85_oi_feature_none_when_unavailable(self):
        # a record with no OI must carry OI features = None (not False)
        candles = _m._bt_normalize_candles(_fake_raw_candles(seed=3, n=1000))
        params = _m._bt_wf_build_params("BTCUSDT", "1h", len(candles), [1, 2, 3])
        events = _m._bt_extract_ob_replay_events(candles, params)
        atr = _m._bt_ap_atr_series(candles)
        idx = _m._bt_ap_build_trend_index(candles, _m._BT_PIVOT_LEN)
        sidx = _m._bt_ap_build_trend_index(candles, _m._BT_SWING_PIVOT_LEN)
        recs = _m._bt_ap_build_trade_records(
            events, candles, "2", "internal", idx, sidx, None, None, atr,
            oi_list=[])   # no OI
        self.assertGreater(len(recs), 0)
        for r in recs:
            self.assertIsNone(r["features"]["OI_RISING_AT_TOUCH"])
            self.assertIsNone(r["features"]["OI_FALLING_AT_TOUCH"])
            self.assertFalse(r["oi_available"])

    def test_86b_oi_feature_none_when_change_uncomputable(self):
        # A touch inside the OI window but within the first _FL_OI_LOOKBACK OI
        # points has change=None → the OI feature must be None (unknown), NOT a
        # definitive False (which would pollute pattern-book denominators).
        # Anchor the OI window to start AT an actual touch so that touch sits at
        # OI index 0 (no prior point → change uncomputable, available=True).
        candles = _m._bt_normalize_candles(_fake_raw_candles(seed=11, n=1000))
        params = _m._bt_wf_build_params("BTCUSDT", "1h", len(candles), [1, 2, 3])
        events = _m._bt_extract_ob_replay_events(candles, params)
        atr = _m._bt_ap_atr_series(candles)
        idx = _m._bt_ap_build_trend_index(candles, _m._BT_PIVOT_LEN)
        sidx = _m._bt_ap_build_trend_index(candles, _m._BT_SWING_PIVOT_LEN)
        # find a real touch index
        touch_idx = None
        for ev in events:
            if ev.get("touch_status") != "touched":
                continue
            eps = _m._te_detect_touch_episodes(candles, ev)
            if eps:
                touch_idx = eps[0]["touch_index"]
                break
        self.assertIsNotNone(touch_idx)
        touch_time = candles[touch_idx]["open_time"]
        # OI window begins exactly at that touch → touch at OI index 0
        oi = [(touch_time + k * 3_600_000, 1000.0 + k) for k in range(50)]
        ctx = _m._bt_fl_oi_context(oi, touch_time)
        self.assertTrue(ctx["available"])          # inside window
        self.assertIsNone(ctx["oi_change_pct"])    # but no prior point → None
        recs = _m._bt_ap_build_trade_records(
            events, candles, "2", "internal", idx, sidx, None, None, atr,
            oi_list=oi)
        target = [r for r in recs if r["touch_time"] == touch_time]
        self.assertTrue(target)
        for r in target:
            self.assertTrue(r["oi_available"])
            self.assertIsNone(r["oi_change_pct"])
            self.assertIsNone(r["features"]["OI_RISING_AT_TOUCH"])
            self.assertIsNone(r["features"]["OI_FALLING_AT_TOUCH"])

    def test_86_definitions_documented(self):
        with patch.object(_m, "get_klines", side_effect=_det_gk()), \
             patch.object(_m, "_bt_fl_fetch_oi_history", return_value=[]):
            res = _m._bt_run_autopsy({
                "symbols": ["BTCUSDT"], "timeframes": ["1h"],
                "candle_count": 1000, "rr": 2, "ob_classes": ["internal"]})
        self.assertIn("open_interest", res["definitions"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
