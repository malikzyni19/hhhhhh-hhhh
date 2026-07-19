"""
Phase 18 unit tests — Autopsy Agent (research only).

Deterministic per-trade reason codes, loser-vs-winner ranking, setup profiles.
No AI API in the core. No external API calls — get_klines is mocked.

Usage:
    cd /home/user/hhhhhh-hhhh && python3 _test_phase_18.py
"""
import os, sys, traceback, unittest, inspect, math, random
from unittest.mock import patch

os.environ.setdefault("DATABASE_URL", "sqlite:///phase18_test.db")
os.environ.setdefault("SECRET_KEY",   "phase18-test-key")
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

def _zig(drift, n=120):
    """Zigzag candles with clean pivots: 8 bars up, 4 bars down + drift."""
    out = []
    price = 100.0
    for i in range(n):
        step = 1.5 if (i % 12) < 8 else -1.0
        price += step + drift
        out.append({"open_time": i * 3_600_000, "open": price - 0.2,
                    "high": price + 0.5, "low": price - 0.5,
                    "close": price, "volume": 1.0})
    return out


def _fake_raw_candles(seed=42, n=1000):
    """camelCase mean-reverting klines for the full pipeline."""
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


_REC_SEQ = 0

def _rec(outcome, feat_over=None, realized=None, session="Asia",
         htf_alignment="with", touch_bucket="first", failure_mode=None):
    """Minimal synthetic autopsy record for ranking/profile math tests."""
    global _REC_SEQ
    _REC_SEQ += 1
    features = {k: False for k in _m._AP_FEATURE_KEYS}
    features.update(feat_over or {})
    if realized is None:
        realized = 2.0 if outcome == "win" else (-1.0 if outcome == "loss" else None)
    return {"touch_trade_id": f"tte_{_REC_SEQ:06d}", "outcome": outcome,
            "realized_r": realized, "features": features, "session": session,
            "htf_alignment": htf_alignment, "touch_bucket": touch_bucket,
            "failure_mode": failure_mode}


# ══════════════════════════════════════════════════════════════════════════════
# ATR
# ══════════════════════════════════════════════════════════════════════════════

class TestAtr(unittest.TestCase):
    def test_01_atr_values(self):
        candles = [{"open_time": i, "open": 100, "high": 102, "low": 98,
                    "close": 100, "volume": 1} for i in range(30)]
        atr = _m._bt_ap_atr_series(candles, period=14)
        self.assertEqual(len(atr), 30)
        # constant 4-range candles, close==prev close → TR always 4
        self.assertAlmostEqual(atr[0], 4.0)
        self.assertAlmostEqual(atr[29], 4.0)

    def test_02_atr_no_lookahead(self):
        base = _zig(0.3, 60)
        atr_full  = _m._bt_ap_atr_series(base + _zig(5.0, 20), period=14)
        atr_short = _m._bt_ap_atr_series(base, period=14)
        for i in range(60):
            self.assertAlmostEqual(atr_full[i], atr_short[i],
                                   msg=f"ATR look-ahead at bar {i}")

    def test_03_atr_empty(self):
        self.assertEqual(_m._bt_ap_atr_series([]), [])


# ══════════════════════════════════════════════════════════════════════════════
# HH/HL trend structure
# ══════════════════════════════════════════════════════════════════════════════

class TestTrend(unittest.TestCase):
    def test_04_uptrend(self):
        idx = _m._bt_ap_build_trend_index(_zig(0.35), 5)
        self.assertEqual(_m._bt_ap_trend_at(idx, 100), "up")

    def test_05_downtrend(self):
        idx = _m._bt_ap_build_trend_index(_zig(-0.9), 5)
        self.assertEqual(_m._bt_ap_trend_at(idx, 100), "down")

    def test_06_unknown_early(self):
        idx = _m._bt_ap_build_trend_index(_zig(0.35), 5)
        self.assertEqual(_m._bt_ap_trend_at(idx, 5), "unknown")
        self.assertEqual(_m._bt_ap_trend_at(idx, -1), "unknown")
        self.assertEqual(_m._bt_ap_trend_at(idx, None), "unknown")

    def test_07_no_lookahead(self):
        # Trend at bar 60 must be identical whether or not future bars exist.
        full  = _zig(0.35, 120)
        idx_f = _m._bt_ap_build_trend_index(full, 5)
        idx_t = _m._bt_ap_build_trend_index(full[:66], 5)  # bar 60 + 5 confirm window
        self.assertEqual(_m._bt_ap_trend_at(idx_f, 60),
                         _m._bt_ap_trend_at(idx_t, 60))

    def test_08_short_series_safe(self):
        idx = _m._bt_ap_build_trend_index(_zig(0.35, 8), 5)
        self.assertEqual(_m._bt_ap_trend_at(idx, 7), "unknown")

    def test_09_alignment_mapping(self):
        self.assertEqual(_m._bt_ap_alignment("bullish", "up"),      "with")
        self.assertEqual(_m._bt_ap_alignment("bullish", "down"),    "against")
        self.assertEqual(_m._bt_ap_alignment("bearish", "down"),    "with")
        self.assertEqual(_m._bt_ap_alignment("bearish", "up"),      "against")
        self.assertEqual(_m._bt_ap_alignment("bullish", "neutral"), "neutral")
        self.assertEqual(_m._bt_ap_alignment("bullish", "unknown"), "unknown")

    def test_10_htf_bar_index(self):
        htf = [{"open_time": t * 1000} for t in (0, 100, 200, 300)]
        self.assertEqual(_m._bt_ap_htf_bar_index(htf, 150_000), 1)
        self.assertEqual(_m._bt_ap_htf_bar_index(htf, 300_000), 3)
        self.assertEqual(_m._bt_ap_htf_bar_index(htf, -5), None)
        self.assertEqual(_m._bt_ap_htf_bar_index([], 100), None)
        self.assertEqual(_m._bt_ap_htf_bar_index(htf, None), None)


# ══════════════════════════════════════════════════════════════════════════════
# Failure modes
# ══════════════════════════════════════════════════════════════════════════════

class TestFailureModes(unittest.TestCase):
    def _sim(self, outcome, entry_bar=10, stop_bar=None, cte=None):
        return {"entry_bar": entry_bar, "stop_bar": stop_bar,
                "realized_by_rr": {"2": {"outcome": outcome,
                                         "candles_to_exit": cte}}}

    def test_11_instant_mitigation(self):
        sim = self._sim("loss", entry_bar=10, stop_bar=10, cte=0)
        self.assertEqual(_m._bt_ap_failure_mode(sim, "2"), "INSTANT_MITIGATION")

    def test_12_slow_bleed(self):
        sim = self._sim("loss", entry_bar=10, stop_bar=35,
                        cte=_m._AP_SLOW_BLEED_CANDLES)
        self.assertEqual(_m._bt_ap_failure_mode(sim, "2"), "SLOW_BLEED")

    def test_13_standard_stop(self):
        sim = self._sim("loss", entry_bar=10, stop_bar=15, cte=5)
        self.assertEqual(_m._bt_ap_failure_mode(sim, "2"), "STANDARD_STOP")

    def test_14_non_loss_none(self):
        self.assertIsNone(_m._bt_ap_failure_mode(self._sim("win"), "2"))
        self.assertIsNone(_m._bt_ap_failure_mode(self._sim("ambiguous"), "2"))
        self.assertIsNone(_m._bt_ap_failure_mode(self._sim("unresolved"), "2"))


# ══════════════════════════════════════════════════════════════════════════════
# 18B — Reason ranking math
# ══════════════════════════════════════════════════════════════════════════════

class TestReasonRanking(unittest.TestCase):
    def test_15_frequencies_and_delta(self):
        recs = ([_rec("loss", {"LATE_TOUCH": True})] * 6 +
                [_rec("loss", {"LATE_TOUCH": False})] * 4 +
                [_rec("win",  {"LATE_TOUCH": True})] * 2 +
                [_rec("win",  {"LATE_TOUCH": False})] * 8)
        ranking = _m._bt_ap_rank_reasons(recs)
        lt = next(r for r in ranking if r["feature"] == "LATE_TOUCH")
        self.assertEqual(lt["loser_pct"], 60.0)
        self.assertEqual(lt["winner_pct"], 20.0)
        self.assertEqual(lt["delta_pct"], 40.0)
        self.assertEqual(lt["lift"], 3.0)

    def test_16_unknown_excluded_from_denominator(self):
        recs = [_rec("loss", {"AGAINST_HTF_TREND": True}),
                _rec("loss", {"AGAINST_HTF_TREND": None}),
                _rec("win",  {"AGAINST_HTF_TREND": False}),
                _rec("win",  {"AGAINST_HTF_TREND": None})]
        ranking = _m._bt_ap_rank_reasons(recs)
        r = next(x for x in ranking if x["feature"] == "AGAINST_HTF_TREND")
        self.assertEqual(r["losers_known"], 1)
        self.assertEqual(r["winners_known"], 1)
        self.assertEqual(r["loser_pct"], 100.0)
        self.assertEqual(r["winner_pct"], 0.0)

    def test_17_zero_winner_pct_lift_guard(self):
        recs = [_rec("loss", {"STALE_ZONE": True}),
                _rec("win",  {"STALE_ZONE": False})]
        ranking = _m._bt_ap_rank_reasons(recs)
        r = next(x for x in ranking if x["feature"] == "STALE_ZONE")
        self.assertIsNone(r["lift"])          # division by zero guarded
        self.assertEqual(r["delta_pct"], 100.0)

    def test_18_sorted_by_delta_desc(self):
        recs = ([_rec("loss", {"LATE_TOUCH": True, "STALE_ZONE": False})] * 5 +
                [_rec("win",  {"LATE_TOUCH": False, "STALE_ZONE": False})] * 5)
        ranking = _m._bt_ap_rank_reasons(recs)
        deltas = [r["delta_pct"] for r in ranking if r["delta_pct"] is not None]
        self.assertEqual(deltas, sorted(deltas, reverse=True))

    def test_19_empty_records_safe(self):
        ranking = _m._bt_ap_rank_reasons([])
        self.assertEqual(len(ranking), len(_m._AP_FEATURE_KEYS))
        for r in ranking:
            self.assertIsNone(r["loser_pct"])
            self.assertIsNone(r["delta_pct"])

    def test_20_failure_mode_distribution(self):
        recs = [_rec("loss", failure_mode="INSTANT_MITIGATION"),
                _rec("loss", failure_mode="SLOW_BLEED"),
                _rec("loss", failure_mode="STANDARD_STOP"),
                _rec("loss", failure_mode="STANDARD_STOP"),
                _rec("win")]
        dist = _m._bt_ap_failure_mode_distribution(recs)
        self.assertEqual(dist["total_losses"], 4)
        self.assertEqual(dist["counts"]["INSTANT_MITIGATION"], 1)
        self.assertEqual(dist["counts"]["SLOW_BLEED"], 1)
        self.assertEqual(dist["counts"]["STANDARD_STOP"], 2)
        self.assertEqual(dist["pcts"]["STANDARD_STOP"], 50.0)


# ══════════════════════════════════════════════════════════════════════════════
# 18C — Setup profiles
# ══════════════════════════════════════════════════════════════════════════════

class TestProfiles(unittest.TestCase):
    def test_21_grouping_and_contract(self):
        recs = ([_rec("win",  htf_alignment="with", touch_bucket="first", session="Asia")] * 6 +
                [_rec("loss", htf_alignment="with", touch_bucket="first", session="Asia")] * 2 +
                [_rec("ambiguous", htf_alignment="with", touch_bucket="first", session="Asia")] +
                [_rec("unresolved", htf_alignment="with", touch_bucket="first", session="Asia")] +
                [_rec("loss", htf_alignment="against", touch_bucket="second", session="London")] * 3)
        profiles = _m._bt_ap_build_profiles(recs, "2")
        self.assertEqual(len(profiles), 2)
        p1 = next(p for p in profiles if p["profile_key"] == "with|first|Asia")
        self.assertEqual(p1["trades"], 8)          # 6 wins + 2 losses
        self.assertEqual(p1["wins"], 6)
        self.assertEqual(p1["losses"], 2)
        self.assertEqual(p1["ambiguous"], 1)
        self.assertEqual(p1["unresolved"], 1)
        self.assertEqual(p1["opportunities"], 10)
        self.assertEqual(p1["win_rate_pct"], 75.0)
        self.assertEqual(p1["trades"], p1["wins"] + p1["losses"])

    def test_22_sorted_by_win_rate(self):
        recs = ([_rec("win",  htf_alignment="with", session="Asia")] * 5 +
                [_rec("loss", htf_alignment="against", session="Asia")] * 5 +
                [_rec("win",  htf_alignment="neutral", session="Asia")] * 3 +
                [_rec("loss", htf_alignment="neutral", session="Asia")] * 3)
        profiles = _m._bt_ap_build_profiles(recs, "2")
        rates = [p["win_rate_pct"] for p in profiles if p["win_rate_pct"] is not None]
        self.assertEqual(rates, sorted(rates, reverse=True))

    def test_23_below_min_trades_flagged(self):
        recs = [_rec("win", session="Asia")] * 2
        profiles = _m._bt_ap_build_profiles(recs, "2")
        self.assertTrue(profiles[0]["below_min_trades"])
        self.assertEqual(profiles[0]["sample_size_status"], "insufficient")

    def test_24_no_fabricated_losses(self):
        recs = [_rec("ambiguous", session="Asia"), _rec("unresolved", session="Asia")]
        profiles = _m._bt_ap_build_profiles(recs, "2")
        self.assertEqual(profiles[0]["trades"], 0)
        self.assertEqual(profiles[0]["net_r"], 0.0)
        self.assertIsNone(profiles[0]["win_rate_pct"])


# ══════════════════════════════════════════════════════════════════════════════
# Trade records from the canonical pipeline
# ══════════════════════════════════════════════════════════════════════════════

class TestTradeRecords(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.candles = _m._bt_normalize_candles(_fake_raw_candles())
        cls.params  = _m._bt_wf_build_params("BTCUSDT", "1h", len(cls.candles), [1, 2, 3])
        cls.events  = _m._bt_extract_ob_replay_events(cls.candles, cls.params)
        _m._bt_apply_outcomes_to_events(cls.events, cls.candles, cls.params)
        cls.atr        = _m._bt_ap_atr_series(cls.candles)
        cls.tf_idx     = _m._bt_ap_build_trend_index(cls.candles, _m._BT_PIVOT_LEN)
        cls.swing_idx  = _m._bt_ap_build_trend_index(cls.candles, _m._BT_SWING_PIVOT_LEN)
        cls.records = _m._bt_ap_build_trade_records(
            cls.events, cls.candles, "2", "internal",
            cls.tf_idx, cls.swing_idx, None, None, cls.atr)

    def test_25_records_built(self):
        self.assertGreater(len(self.records), 0)

    def test_26_ids_unique_and_match_explorer(self):
        ids = [r["touch_trade_id"] for r in self.records]
        self.assertEqual(len(ids), len(set(ids)))
        # Same trade IDs as the Phase 16 Trade Explorer produces
        for r in self.records[:5]:
            self.assertTrue(r["touch_trade_id"].startswith("tte_"))

    def test_27_features_present(self):
        for r in self.records:
            for feat in _m._AP_FEATURE_KEYS:
                self.assertIn(feat, r["features"])

    def test_28_htf_unknown_without_htf_data(self):
        for r in self.records:
            self.assertEqual(r["htf_trend"], "unknown")
            self.assertEqual(r["htf_alignment"], "unknown")
            self.assertIsNone(r["features"]["AGAINST_HTF_TREND"])

    def test_29_failure_mode_only_on_losses(self):
        for r in self.records:
            if r["outcome"] == "loss":
                self.assertIn(r["failure_mode"],
                              ("INSTANT_MITIGATION", "SLOW_BLEED", "STANDARD_STOP"))
            else:
                self.assertIsNone(r["failure_mode"])

    def test_30_outcomes_match_explorer_rows(self):
        # The autopsy must analyze the SAME trades the Trade Explorer reports.
        te_rows = []
        for ev in self.events:
            if ev.get("touch_status") != "touched":
                continue
            eps = _m._te_detect_touch_episodes(self.candles, ev)
            te_rows.extend(_m._te_build_touch_trade_rows(ev, eps, self.candles, [1, 2, 3]))
        te_by_id = {r["touch_trade_id"]: r for r in te_rows if r["rr"] == "2"
                    and r["eligible"]}
        matched = 0
        for r in self.records:
            te = te_by_id.get(r["touch_trade_id"])
            if te is not None:
                self.assertEqual(r["outcome"], te["outcome"],
                                 f"outcome mismatch for {r['touch_trade_id']}")
                matched += 1
        self.assertGreater(matched, 0)

    def test_31_deterministic(self):
        again = _m._bt_ap_build_trade_records(
            self.events, self.candles, "2", "internal",
            self.tf_idx, self.swing_idx, None, None, self.atr)
        self.assertEqual([r["touch_trade_id"] for r in self.records],
                         [r["touch_trade_id"] for r in again])

    def test_31b_htf_trend_correct_value(self):
        # HTF = clean up-zigzag on 4h spacing covering the whole LTF range →
        # every record with a known HTF trend must read "up", never "down",
        # and AGAINST_HTF_TREND becomes a real boolean for those records.
        htf = []
        price = 100.0
        for i in range(400):
            step = 1.5 if (i % 12) < 8 else -1.0
            price += step + 0.35
            htf.append({"open_time": i * 4 * 3_600_000, "open": price - 0.2,
                        "high": price + 0.5, "low": price - 0.5,
                        "close": price, "volume": 1.0})
        htf_idx = _m._bt_ap_build_trend_index(htf, _m._BT_PIVOT_LEN)
        recs = _m._bt_ap_build_trade_records(
            self.events, self.candles, "2", "internal",
            self.tf_idx, self.swing_idx, htf, htf_idx, self.atr)
        known = [r for r in recs if r["htf_trend"] != "unknown"]
        self.assertGreater(len(known), 0)
        self.assertEqual({r["htf_trend"] for r in known}, {"up"})
        for r in known:
            self.assertIsNotNone(r["features"]["AGAINST_HTF_TREND"])
            self.assertEqual(r["features"]["AGAINST_HTF_TREND"],
                             r["direction"] == "bearish")

    def test_31c_htf_in_progress_bar_never_used(self):
        # All touches fall inside HTF bar 0 (one giant bar covering the whole
        # LTF range) → the containing bar is still in progress at every touch,
        # so the HTF trend must stay "unknown" — the in-progress bar and
        # anything after it can never leak in.
        span = len(self.candles) * 3_600_000 + 1
        htf = [{"open_time": 0, "open": 100, "high": 200, "low": 50,
                "close": 150, "volume": 1.0}]
        for i in range(1, 60):
            htf.append({"open_time": span + i * 3_600_000, "open": 100 + i,
                        "high": 101 + i, "low": 99 + i, "close": 100.5 + i,
                        "volume": 1.0})
        htf_idx = _m._bt_ap_build_trend_index(htf, _m._BT_PIVOT_LEN)
        recs = _m._bt_ap_build_trade_records(
            self.events, self.candles, "2", "internal",
            self.tf_idx, self.swing_idx, htf, htf_idx, self.atr)
        self.assertGreater(len(recs), 0)
        self.assertEqual({r["htf_trend"] for r in recs}, {"unknown"})


# ══════════════════════════════════════════════════════════════════════════════
# Orchestrator (mocked get_klines)
# ══════════════════════════════════════════════════════════════════════════════

class TestOrchestrator(unittest.TestCase):
    def _gk(self):
        cache = {}
        def gk(sym, tf, limit=300, market="perpetual", extended=False):
            key = (sym, tf)
            if key not in cache:
                # Deterministic seed (hash() is process-randomized for strings)
                seed = sum(ord(c) for c in sym + tf)
                cache[key] = _fake_raw_candles(seed)
            return cache[key]
        return gk

    def test_32_dual_class_reports(self):
        with patch.object(_m, "get_klines", side_effect=self._gk()):
            res = _m._bt_run_autopsy({"symbols": ["BTCUSDT"], "timeframes": ["1h"],
                                      "candle_count": 1000, "rr": 2,
                                      "ob_classes": ["internal", "swing"]})
        self.assertTrue(res["ok"])
        self.assertTrue(res["authoritative_execution"])
        self.assertFalse(res["client_results_accepted"])
        self.assertIn("internal", res["reports_by_class"])
        self.assertIn("swing", res["reports_by_class"])
        ri = res["reports_by_class"]["internal"]
        rs = res["reports_by_class"]["swing"]
        self.assertEqual(ri["trades"], ri["wins"] + ri["losses"])
        self.assertEqual(rs["trades"], rs["wins"] + rs["losses"])
        # classes analyzed separately — reports are independent objects and
        # only the requested classes appear (raw counts can legitimately tie,
        # so equality of totals is NOT asserted either way)
        self.assertEqual(set(res["reports_by_class"].keys()),
                         {"internal", "swing"})
        self.assertIsNot(ri, rs)
        self.assertGreater(ri["records_total"], 0)
        self.assertGreater(rs["records_total"], 0)

    def test_33_htf_fetch_cached(self):
        calls = []
        cache = {}
        def gk(sym, tf, limit=300, market="perpetual", extended=False):
            calls.append((sym, tf))
            key = (sym, tf)
            if key not in cache:
                cache[key] = _fake_raw_candles(sum(ord(c) for c in sym + tf))
            return cache[key]
        with patch.object(_m, "get_klines", side_effect=gk):
            _m._bt_run_autopsy({"symbols": ["BTCUSDT"], "timeframes": ["1h"],
                                "candle_count": 1000, "rr": 2,
                                "ob_classes": ["internal"]})
        # 1 LTF fetch (1h) + 1 HTF fetch (4h) — HTF not refetched per class
        self.assertEqual(calls.count(("BTCUSDT", "1h")), 1)
        self.assertEqual(calls.count(("BTCUSDT", "4h")), 1)

    def test_34_failure_isolation(self):
        good = _fake_raw_candles()
        def gk(sym, tf, limit=300, market="perpetual", extended=False):
            if sym == "ETHUSDT" and tf == "1h":
                raise RuntimeError("boom")
            return good
        with patch.object(_m, "get_klines", side_effect=gk):
            res = _m._bt_run_autopsy({"symbols": ["BTCUSDT", "ETHUSDT"],
                                      "timeframes": ["1h"], "candle_count": 1000,
                                      "rr": 2, "ob_classes": ["internal"]})
        self.assertTrue(res["ok"])
        self.assertEqual(len(res["failures"]), 1)
        self.assertEqual(res["performance"]["completed_cells"], 1)
        self.assertGreater(res["reports_by_class"]["internal"]["records_total"], 0)

    def test_35_definitions_documented(self):
        with patch.object(_m, "get_klines", side_effect=self._gk()):
            res = _m._bt_run_autopsy({"symbols": ["BTCUSDT"], "timeframes": ["1h"],
                                      "candle_count": 1000, "rr": 2,
                                      "ob_classes": ["internal"]})
        defs = res["definitions"]
        for key in ("trend", "htf_map", "atr", "displacement", "oversized_zone",
                    "stale_zone", "late_touch", "slow_bleed", "sessions"):
            self.assertIn(key, defs)


# ══════════════════════════════════════════════════════════════════════════════
# Endpoint (real Flask)
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
        return self.client.post("/api/backtest/ob-historical/autopsy", json=body)

    def _ok_body(self, **over):
        b = {"symbols": ["BTCUSDT", "ETHUSDT"], "timeframes": ["1h", "4h"],
             "candle_count": 1000, "rr": 2, "ob_classes": ["internal", "swing"]}
        b.update(over)
        return b

    def test_36_reject_client_results(self):
        for key in ("trades", "outcomes", "records", "reports_by_class",
                    "reason_ranking", "setup_profiles", "win_rate"):
            r = self._post(self._ok_body(**{key: [1]}))
            self.assertEqual(r.status_code, 400, f"{key} not rejected")
            self.assertEqual(r.get_json()["error"],
                             "client_supplied_trade_results_not_allowed")

    def test_37_unauthorized_rejected(self):
        anon = _m.app.test_client()
        r = anon.post("/api/backtest/ob-historical/autopsy", json=self._ok_body())
        self.assertIn(r.status_code, (301, 302, 401, 403))

    def test_38_authorized_accepted(self):
        fake = {"ok": True, "authoritative_execution": True,
                "client_results_accepted": False, "reports_by_class": {}}
        with patch.object(_m, "_bt_run_autopsy", return_value=fake):
            r = self._post(self._ok_body())
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.get_json()["ok"])

    def test_39_limits_enforced(self):
        r = self._post(self._ok_body(
            symbols=["A", "B", "C", "D", "E", "F"]))
        self.assertEqual(r.status_code, 400)
        r2 = self._post(self._ok_body(symbols=["BTCUSDT", "BTCUSDT"]))
        self.assertEqual(r2.status_code, 400)
        r3 = self._post(self._ok_body(symbols=[""]))
        self.assertEqual(r3.status_code, 400)
        r4 = self._post(self._ok_body(timeframes=["7m"]))
        self.assertEqual(r4.status_code, 400)
        r5 = self._post(self._ok_body(rr=9))
        self.assertEqual(r5.status_code, 400)
        r6 = self._post(self._ok_body(ob_classes=["pooled"]))
        self.assertEqual(r6.status_code, 400)
        r7 = self._post(self._ok_body(ob_classes=[]))
        self.assertEqual(r7.status_code, 400)


# ══════════════════════════════════════════════════════════════════════════════
# Research-only isolation
# ══════════════════════════════════════════════════════════════════════════════

class TestIsolation(unittest.TestCase):
    def test_40_no_db_writes(self):
        for fn in (_m._bt_ap_atr_series, _m._bt_ap_build_trend_index,
                   _m._bt_ap_trend_at, _m._bt_ap_build_trade_records,
                   _m._bt_ap_rank_reasons, _m._bt_ap_build_profiles,
                   _m._bt_run_autopsy):
            src = inspect.getsource(fn)
            self.assertNotIn("INSERT INTO", src)
            self.assertNotIn("db.session", src)
            self.assertNotIn("cursor.execute", src)

    def test_41_no_ai_api_in_core(self):
        for fn in (_m._bt_ap_rank_reasons, _m._bt_ap_build_profiles,
                   _m._bt_run_autopsy):
            src = inspect.getsource(fn)
            self.assertNotIn("openai", src.lower())
            self.assertNotIn("anthropic", src.lower())
            self.assertNotIn("requests.post", src)

    def test_42_scanner_untouched(self):
        self.assertTrue(hasattr(_m, "detect_obs"))
        self.assertTrue(hasattr(_m, "detect_obs_all"))
        self.assertEqual(_m._BT_PIVOT_LEN, 5)
        self.assertEqual(_m._BT_SWING_PIVOT_LEN, 30)


if __name__ == "__main__":
    unittest.main(verbosity=2)
