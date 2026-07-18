"""
Phase 19 unit tests — Feature Enrichment + Timeframe Alignment Matrix
(research only).

Covers:
  * FVG detection + OB-zone confluence (formation-time only, no look-ahead)
  * Dealing-range position (premium/discount) with confirmed pivots only
  * ATR percentile volatility regime (trailing window only)
  * Per-record multi-trend alignments using the closed-bar rule
  * Alignment matrix aggregation (outcome contract per cell, never pooled)
  * Orchestrator matrix fetches (unified cache) + endpoint response shape

Usage:
    cd /home/user/hhhhhh-hhhh && python3 _test_phase_19.py
"""
import os, sys, traceback, unittest, inspect, math, random
from unittest.mock import patch

os.environ.setdefault("DATABASE_URL", "sqlite:///phase19_test.db")
os.environ.setdefault("SECRET_KEY",   "phase19-test-key")
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


# ── Data builders ────────────────────────────────────────────────────────────

def _c(o, h, l, cl, i=0, spacing=3_600_000):
    return {"open_time": i * spacing, "open": float(o), "high": float(h),
            "low": float(l), "close": float(cl), "volume": 1.0}


def _zig(drift, n=120, spacing=3_600_000):
    out = []
    price = 100.0
    for i in range(n):
        step = 1.5 if (i % 12) < 8 else -1.0
        price += step + drift
        out.append({"open_time": i * spacing, "open": price - 0.2,
                    "high": price + 0.5, "low": price - 0.5,
                    "close": price, "volume": 1.0})
    return out


def _fake_raw_candles(seed=42, n=1000):
    rnd = random.Random(seed)
    out = []
    for i in range(n):
        drift = 6.0 * math.sin(i / 9.0) + 3.5 * math.sin(i / 23.0)
        close = 100.0 + drift + rnd.uniform(-1.2, 1.2)
        out.append({"openTime": i * 3_600_000, "closeTime": (i + 1) * 3_600_000 - 1,
                    "open": close - 0.2,
                    "high": close + rnd.uniform(0.4, 1.6),
                    "low":  close - rnd.uniform(0.4, 1.6),
                    "close": close, "volume": 1000.0, "quoteVolume": 1000.0,
                    "trades": 100, "takerBuyBase": 500.0, "takerBuyQuote": 500.0})
    return out


def _det_gk():
    cache = {}
    def gk(sym, tf, limit=300, market="perpetual"):
        key = (sym, tf)
        if key not in cache:
            cache[key] = _fake_raw_candles(sum(ord(c) for c in sym + tf))
        return cache[key]
    return gk


# ══════════════════════════════════════════════════════════════════════════════
# FVG detection + confluence
# ══════════════════════════════════════════════════════════════════════════════

class TestFvg(unittest.TestCase):
    def test_01_bullish_fvg_detected(self):
        # bar2 low (110) > bar0 high (105) → bullish gap [105, 110]
        candles = [_c(100, 105, 99, 104, 0), _c(104, 112, 103, 111, 1),
                   _c(111, 118, 110, 117, 2)]
        fvgs = _m._bt_al_detect_fvgs(candles, 0, 2)
        self.assertEqual(len(fvgs), 1)
        self.assertEqual(fvgs[0]["type"], "bullish")
        self.assertEqual(fvgs[0]["lo"], 105.0)
        self.assertEqual(fvgs[0]["hi"], 110.0)

    def test_02_bearish_fvg_detected(self):
        # bar2 high (95) < bar0 low (99) → bearish gap [95, 99]
        candles = [_c(104, 105, 99, 100, 0), _c(99, 100, 90, 91, 1),
                   _c(91, 95, 85, 86, 2)]
        fvgs = _m._bt_al_detect_fvgs(candles, 0, 2)
        self.assertEqual(len(fvgs), 1)
        self.assertEqual(fvgs[0]["type"], "bearish")
        self.assertEqual(fvgs[0]["lo"], 95.0)
        self.assertEqual(fvgs[0]["hi"], 99.0)

    def test_03_no_gap_no_fvg(self):
        candles = [_c(100, 105, 99, 104, i) for i in range(5)]
        self.assertEqual(_m._bt_al_detect_fvgs(candles, 0, 4), [])

    def test_04_confluence_true_when_overlapping(self):
        # FVG [105,110] at bar 2; OB zone [104,106] overlaps it.
        candles = [_c(100, 105, 99, 104, 0), _c(104, 112, 103, 111, 1),
                   _c(111, 118, 110, 117, 2), _c(117, 119, 116, 118, 3)]
        self.assertTrue(_m._bt_al_fvg_confluence(candles, 3, 106.0, 104.0))

    def test_05_confluence_false_when_disjoint(self):
        candles = [_c(100, 105, 99, 104, 0), _c(104, 112, 103, 111, 1),
                   _c(111, 118, 110, 117, 2), _c(117, 119, 116, 118, 3)]
        self.assertFalse(_m._bt_al_fvg_confluence(candles, 3, 95.0, 90.0))

    def test_06_confluence_none_early(self):
        candles = [_c(100, 105, 99, 104, 0), _c(104, 112, 103, 111, 1)]
        self.assertIsNone(_m._bt_al_fvg_confluence(candles, 1, 106.0, 104.0))

    def test_07_no_lookahead_formation_window(self):
        # An FVG that forms AFTER the formation bar must not count.
        candles = [_c(100, 101, 99, 100, 0), _c(100, 101, 99, 100, 1),
                   _c(100, 101, 99, 100, 2), _c(100, 101, 99, 100, 3),
                   # future gap at bar 6 (low 110 > bar-4 high 101)
                   _c(100, 101, 99, 100, 4), _c(103, 112, 102, 111, 5),
                   _c(111, 118, 110, 117, 6)]
        self.assertFalse(_m._bt_al_fvg_confluence(candles, 3, 111.0, 100.0))


# ══════════════════════════════════════════════════════════════════════════════
# Range position + ATR percentile
# ══════════════════════════════════════════════════════════════════════════════

class TestRangeAndVol(unittest.TestCase):
    def test_08_range_position_math(self):
        idx = _m._bt_ap_build_trend_index(_zig(0.0), 5)
        # find a bar late enough for confirmed pivots
        import bisect
        fb = 60
        cutoff = fb - 5
        hb = [b for b in idx["high_bars"] if b <= cutoff]
        lb = [b for b in idx["low_bars"] if b <= cutoff]
        self.assertTrue(hb and lb)
        rh = idx["h"][hb[-1]]
        rl = idx["l"][lb[-1]]
        mid = (rh + rl) / 2.0
        pos = _m._bt_al_range_position(idx, fb, mid)
        self.assertAlmostEqual(pos, 50.0, places=1)
        # at range low → 0; at range high → 100; beyond → clamped
        self.assertAlmostEqual(_m._bt_al_range_position(idx, fb, rl), 0.0)
        self.assertAlmostEqual(_m._bt_al_range_position(idx, fb, rh), 100.0)
        self.assertEqual(_m._bt_al_range_position(idx, fb, rh + 100), 100.0)
        self.assertEqual(_m._bt_al_range_position(idx, fb, rl - 100), 0.0)

    def test_09_range_position_none_cases(self):
        idx = _m._bt_ap_build_trend_index(_zig(0.0, 8), 5)
        self.assertIsNone(_m._bt_al_range_position(idx, 3, 100.0))
        self.assertIsNone(_m._bt_al_range_position(idx, None, 100.0))
        self.assertIsNone(_m._bt_al_range_position(idx, -1, 100.0))

    def test_10_atr_percentile(self):
        # ATR constant → current never strictly below any → percentile 0
        flat = [_c(100, 102, 98, 100, i) for i in range(60)]
        atr = _m._bt_ap_atr_series(flat)
        self.assertEqual(_m._bt_al_atr_percentile(atr, 50), 0.0)
        # rising ranges → last ATR above all prior → high percentile
        widening = [_c(100, 100 + i * 0.5 + 1, 100 - i * 0.5 - 1, 100, i)
                    for i in range(60)]
        atr2 = _m._bt_ap_atr_series(widening)
        pct = _m._bt_al_atr_percentile(atr2, 59)
        self.assertGreater(pct, 90.0)

    def test_11_atr_percentile_none_cases(self):
        atr = _m._bt_ap_atr_series([_c(100, 102, 98, 100, i) for i in range(5)])
        self.assertIsNone(_m._bt_al_atr_percentile(atr, 4))   # < 10 values
        self.assertIsNone(_m._bt_al_atr_percentile(atr, 99))  # out of range
        self.assertIsNone(_m._bt_al_atr_percentile(atr, None))

    def test_12_atr_percentile_no_lookahead(self):
        base = _zig(0.3, 80)
        atr_a = _m._bt_ap_atr_series(base)
        atr_b = _m._bt_ap_atr_series(base + _zig(9.0, 40))
        self.assertEqual(_m._bt_al_atr_percentile(atr_a, 70),
                         _m._bt_al_atr_percentile(atr_b, 70))


# ══════════════════════════════════════════════════════════════════════════════
# Multi-trend alignments on records
# ══════════════════════════════════════════════════════════════════════════════

class TestMultiTrendRecords(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.candles = _m._bt_normalize_candles(_fake_raw_candles())
        cls.params  = _m._bt_wf_build_params("BTCUSDT", "1h", len(cls.candles), [1, 2, 3])
        cls.events  = _m._bt_extract_ob_replay_events(cls.candles, cls.params)
        cls.atr     = _m._bt_ap_atr_series(cls.candles)
        cls.tf_idx  = _m._bt_ap_build_trend_index(cls.candles, _m._BT_PIVOT_LEN)
        cls.sw_idx  = _m._bt_ap_build_trend_index(cls.candles, _m._BT_SWING_PIVOT_LEN)
        # 4h up-zigzag trend TF covering the LTF range
        cls.trend_up = []
        price = 100.0
        for i in range(400):
            step = 1.5 if (i % 12) < 8 else -1.0
            price += step + 0.35
            cls.trend_up.append({"open_time": i * 4 * 3_600_000,
                                 "open": price - 0.2, "high": price + 0.5,
                                 "low": price - 0.5, "close": price, "volume": 1.0})
        cls.mt = {"4h": {"candles": cls.trend_up,
                         "trend_idx": _m._bt_ap_build_trend_index(cls.trend_up, 5)}}
        cls.records = _m._bt_ap_build_trade_records(
            cls.events, cls.candles, "2", "internal",
            cls.tf_idx, cls.sw_idx, None, None, cls.atr, multi_trend=cls.mt)

    def test_13_alignments_present(self):
        self.assertGreater(len(self.records), 0)
        for r in self.records:
            self.assertIn("4h", r["alignments"])

    def test_14_alignment_values_correct(self):
        # trend TF is a clean uptrend → bullish trades "with", bearish "against"
        known = [r for r in self.records if r["alignments"]["4h"] != "unknown"]
        self.assertGreater(len(known), 0)
        for r in known:
            expected = "with" if r["direction"] == "bullish" else "against"
            self.assertEqual(r["alignments"]["4h"], expected)

    def test_15_new_features_present(self):
        for r in self.records:
            self.assertIn("FVG_CONFLUENCE", r["features"])
            self.assertIn("IN_PREMIUM_HALF", r["features"])
            self.assertIn("HIGH_VOL_REGIME", r["features"])
            self.assertIn("range_position_pct", r)
            self.assertIn("atr_percentile", r)

    def test_16_feature_keys_extended(self):
        for k in ("FVG_CONFLUENCE", "IN_PREMIUM_HALF", "HIGH_VOL_REGIME"):
            self.assertIn(k, _m._AP_FEATURE_KEYS)
        ranking = _m._bt_ap_rank_reasons([])
        self.assertEqual(len(ranking), len(_m._AP_FEATURE_KEYS))

    def test_17_premium_half_matches_position(self):
        for r in self.records:
            pos = r["range_position_pct"]
            feat = r["features"]["IN_PREMIUM_HALF"]
            if pos is None:
                self.assertIsNone(feat)
            else:
                self.assertEqual(feat, pos > 50.0)

    def test_18_no_multi_trend_empty_alignments(self):
        recs = _m._bt_ap_build_trade_records(
            self.events, self.candles, "2", "internal",
            self.tf_idx, self.sw_idx, None, None, self.atr)
        self.assertGreater(len(recs), 0)
        for r in recs:
            self.assertEqual(r["alignments"], {})

    def test_19_in_progress_trend_bar_never_used(self):
        # One giant trend-TF bar covering the whole LTF range → every touch's
        # containing bar is in progress → alignment must be "unknown".
        span = len(self.candles) * 3_600_000 + 1
        giant = [{"open_time": 0, "open": 100, "high": 200, "low": 50,
                  "close": 150, "volume": 1.0}]
        for i in range(1, 60):
            giant.append({"open_time": span + i * 3_600_000, "open": 100 + i,
                          "high": 101 + i, "low": 99 + i, "close": 100.5 + i,
                          "volume": 1.0})
        mt = {"4h": {"candles": giant,
                     "trend_idx": _m._bt_ap_build_trend_index(giant, 5)}}
        recs = _m._bt_ap_build_trade_records(
            self.events, self.candles, "2", "internal",
            self.tf_idx, self.sw_idx, None, None, self.atr, multi_trend=mt)
        self.assertGreater(len(recs), 0)
        self.assertEqual({r["alignments"]["4h"] for r in recs}, {"unknown"})

    def test_20_deterministic(self):
        again = _m._bt_ap_build_trade_records(
            self.events, self.candles, "2", "internal",
            self.tf_idx, self.sw_idx, None, None, self.atr, multi_trend=self.mt)
        self.assertEqual(
            [(r["touch_trade_id"], r["alignments"], r["features"]["FVG_CONFLUENCE"])
             for r in self.records],
            [(r["touch_trade_id"], r["alignments"], r["features"]["FVG_CONFLUENCE"])
             for r in again])


# ══════════════════════════════════════════════════════════════════════════════
# Alignment matrix aggregation
# ══════════════════════════════════════════════════════════════════════════════

def _mrec(outcome, tf="1h", alignments=None, realized=None):
    if realized is None:
        realized = 2.0 if outcome == "win" else (-1.0 if outcome == "loss" else None)
    return {"timeframe": tf, "outcome": outcome, "realized_r": realized,
            "alignments": alignments or {}}


class TestMatrix(unittest.TestCase):
    def test_21_cell_grouping_and_contract(self):
        recs = ([_mrec("win",  alignments={"4h": "with"})] * 6 +
                [_mrec("loss", alignments={"4h": "with"})] * 2 +
                [_mrec("ambiguous", alignments={"4h": "with"})] +
                [_mrec("loss", alignments={"4h": "against"})] * 3 +
                [_mrec("win",  alignments={"4h": "against"})] +
                [_mrec("unresolved", alignments={"4h": "neutral"})])
        matrix = _m._bt_al_build_matrix(recs)
        self.assertEqual(len(matrix), 1)
        cell = matrix[0]
        self.assertEqual(cell["ob_timeframe"], "1h")
        self.assertEqual(cell["trend_timeframe"], "4h")
        w = cell["alignments"]["with"]
        self.assertEqual(w["trades"], 8)
        self.assertEqual(w["wins"], 6)
        self.assertEqual(w["losses"], 2)
        self.assertEqual(w["ambiguous"], 1)
        self.assertEqual(w["trades"], w["wins"] + w["losses"])
        self.assertEqual(w["win_rate_pct"], 75.0)
        a = cell["alignments"]["against"]
        self.assertEqual(a["trades"], 4)
        self.assertEqual(a["win_rate_pct"], 25.0)
        n = cell["alignments"]["neutral"]
        self.assertEqual(n["trades"], 0)
        self.assertEqual(n["unresolved"], 1)
        self.assertIsNone(n["win_rate_pct"])

    def test_22_edge_computation(self):
        recs = ([_mrec("win",  alignments={"1d": "with"})] * 4 +
                [_mrec("loss", alignments={"1d": "against"})] * 4)
        matrix = _m._bt_al_build_matrix(recs)
        cell = matrix[0]
        w_exp = cell["alignments"]["with"]["expectancy_r"]
        a_exp = cell["alignments"]["against"]["expectancy_r"]
        self.assertEqual(w_exp, 2.0)
        self.assertEqual(a_exp, -1.0)
        self.assertEqual(cell["with_minus_against_expectancy_r"], 3.0)

    def test_23_multiple_cells_sorted(self):
        recs = [_mrec("win", tf="15m", alignments={"1h": "with", "4h": "with"}),
                _mrec("win", tf="1h",  alignments={"4h": "with", "1d": "with"})]
        matrix = _m._bt_al_build_matrix(recs)
        keys = [(c["ob_timeframe"], c["trend_timeframe"]) for c in matrix]
        self.assertEqual(len(keys), 4)
        self.assertEqual(keys, sorted(keys))

    def test_24_empty_records(self):
        self.assertEqual(_m._bt_al_build_matrix([]), [])

    def test_25_no_fabricated_losses_in_cells(self):
        recs = [_mrec("ambiguous", alignments={"4h": "with"}),
                _mrec("unresolved", alignments={"4h": "with"})]
        cell = _m._bt_al_build_matrix(recs)[0]
        w = cell["alignments"]["with"]
        self.assertEqual(w["trades"], 0)
        self.assertEqual(w["net_r"], 0.0)
        self.assertEqual(w["opportunities"], 2)


# ══════════════════════════════════════════════════════════════════════════════
# Orchestrator + endpoint
# ══════════════════════════════════════════════════════════════════════════════

class TestOrchestrator(unittest.TestCase):
    def test_26_matrix_in_reports(self):
        with patch.object(_m, "get_klines", side_effect=_det_gk()):
            res = _m._bt_run_autopsy({"symbols": ["BTCUSDT"], "timeframes": ["1h"],
                                      "candle_count": 1000, "rr": 2,
                                      "ob_classes": ["internal", "swing"]})
        self.assertTrue(res["ok"])
        for cls in ("internal", "swing"):
            rep = res["reports_by_class"][cls]
            self.assertIn("alignment_matrix", rep)
            pairs = {(c["ob_timeframe"], c["trend_timeframe"])
                     for c in rep["alignment_matrix"]}
            # 1h OB → 4h and 1d trend TFs per the frozen matrix map
            self.assertEqual(pairs, {("1h", "4h"), ("1h", "1d")})

    def test_27_matrix_fetches_cached_once(self):
        calls = []
        cache = {}
        def gk(sym, tf, limit=300, market="perpetual"):
            calls.append((sym, tf))
            key = (sym, tf)
            if key not in cache:
                cache[key] = _fake_raw_candles(sum(ord(c) for c in sym + tf))
            return cache[key]
        with patch.object(_m, "get_klines", side_effect=gk):
            _m._bt_run_autopsy({"symbols": ["BTCUSDT"], "timeframes": ["1h"],
                                "candle_count": 1000, "rr": 2,
                                "ob_classes": ["internal", "swing"]})
        # 1h LTF once; 4h serves both the HTF feature and the matrix — once;
        # 1d for the matrix — once.  No duplicate fetches across classes.
        self.assertEqual(calls.count(("BTCUSDT", "1h")), 1)
        self.assertEqual(calls.count(("BTCUSDT", "4h")), 1)
        self.assertEqual(calls.count(("BTCUSDT", "1d")), 1)

    def test_28_matrix_definitions_documented(self):
        with patch.object(_m, "get_klines", side_effect=_det_gk()):
            res = _m._bt_run_autopsy({"symbols": ["BTCUSDT"], "timeframes": ["1h"],
                                      "candle_count": 1000, "rr": 2,
                                      "ob_classes": ["internal"]})
        defs = res["definitions"]
        for key in ("matrix_map", "fvg_confluence", "in_premium_half",
                    "high_vol_regime", "alignment_matrix"):
            self.assertIn(key, defs)
        self.assertEqual(defs["matrix_map"]["1h"], ["4h", "1d"])
        self.assertEqual(defs["matrix_map"]["15m"], ["1h", "4h"])
        self.assertEqual(defs["matrix_map"]["4h"], ["1d"])

    def test_29_matrix_classes_never_pooled(self):
        with patch.object(_m, "get_klines", side_effect=_det_gk()):
            res = _m._bt_run_autopsy({"symbols": ["BTCUSDT"], "timeframes": ["1h"],
                                      "candle_count": 1000, "rr": 2,
                                      "ob_classes": ["internal", "swing"]})
        ri = res["reports_by_class"]["internal"]
        rs = res["reports_by_class"]["swing"]
        # per-cell opportunity totals must equal that class's own record count
        for rep in (ri, rs):
            for cell in rep["alignment_matrix"]:
                total_opps = sum(a["opportunities"]
                                 for a in cell["alignments"].values())
                self.assertEqual(total_opps, rep["records_total"])

    def test_30_endpoint_returns_matrix(self):
        _m.app.config["TESTING"] = True
        client = _m.app.test_client()
        with client.session_transaction() as sess:
            sess["logged_in"] = True
            sess["username"] = "tester"
            sess["is_admin"] = True
        fake = {"ok": True, "reports_by_class":
                {"internal": {"alignment_matrix": []}}}
        with patch.object(_m, "_bt_run_autopsy", return_value=fake):
            r = client.post("/api/backtest/ob-historical/autopsy",
                            json={"symbols": ["BTCUSDT"], "timeframes": ["15m"],
                                  "candle_count": 1000, "rr": 2,
                                  "ob_classes": ["internal"]})
        self.assertEqual(r.status_code, 200)
        self.assertIn("alignment_matrix",
                      r.get_json()["reports_by_class"]["internal"])


# ══════════════════════════════════════════════════════════════════════════════
# Research-only isolation
# ══════════════════════════════════════════════════════════════════════════════

class TestIsolation(unittest.TestCase):
    def test_31_no_db_writes(self):
        for fn in (_m._bt_al_detect_fvgs, _m._bt_al_fvg_confluence,
                   _m._bt_al_range_position, _m._bt_al_atr_percentile,
                   _m._bt_al_build_matrix):
            src = inspect.getsource(fn)
            self.assertNotIn("INSERT INTO", src)
            self.assertNotIn("db.session", src)
            self.assertNotIn("cursor.execute", src)

    def test_32_scanner_untouched(self):
        self.assertTrue(hasattr(_m, "detect_obs"))
        self.assertEqual(_m._BT_PIVOT_LEN, 5)
        self.assertEqual(_m._BT_SWING_PIVOT_LEN, 30)

    def test_33_matrix_map_frozen_pairs(self):
        self.assertEqual(_m._AL_MATRIX_MAP["15m"], ["1h", "4h"])
        self.assertEqual(_m._AL_MATRIX_MAP["1h"],  ["4h", "1d"])
        self.assertEqual(_m._AL_MATRIX_MAP["4h"],  ["1d"])
        self.assertEqual(_m._AL_MATRIX_MAP["1d"],  [])
        self.assertNotIn("5m", _m._AL_MATRIX_MAP["15m"])  # no 5m in the matrix


if __name__ == "__main__":
    unittest.main(verbosity=2)
