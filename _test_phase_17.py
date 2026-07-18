"""
Phase 17 unit tests — Detection Parity + Dual-Class Backtest (research only).

Covers:
  * Internal pivot lookback = 5 (production iLen parity; was 3)
  * Swing OB class (pivot 30 = production sLen) as a separate research class
  * Event tagging (ob_class / pivot_len) and no-pooling guarantees
  * Parity check now uses REAL production parameters (5/30, not 3/3)
  * Dual-class runner + endpoint validation (get_klines mocked; no external APIs)

Usage:
    cd /home/user/hhhhhh-hhhh && python3 _test_phase_17.py
"""
import os, sys, traceback, unittest, inspect, math, random
from unittest.mock import patch

os.environ.setdefault("DATABASE_URL", "sqlite:///phase17_test.db")
os.environ.setdefault("SECRET_KEY",   "phase17-test-key")
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


def _fake_raw_candles(n=1000, seed=42):
    """camelCase raw klines; mean-reverting so OBs form and get revisited."""
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


def _params(**over):
    p = {"symbol": "BTCUSDT", "timeframe": "1h", "candle_count": 1000,
         "exchange": "binance", "market": "perpetual",
         "rr_values": [1, 2, 3], "entry_rule": "zone_high",
         "stop_rule": "close_beyond_zone", "include_parity": False,
         "ob_class_mode": "both"}
    p.update(over)
    return p


_CANDLES = _m._bt_normalize_candles(_fake_raw_candles())


# ══════════════════════════════════════════════════════════════════════════════
# Constants / production parity
# ══════════════════════════════════════════════════════════════════════════════

class TestConstants(unittest.TestCase):
    def test_01_internal_pivot_is_5(self):
        self.assertEqual(_m._BT_PIVOT_LEN, 5)

    def test_02_swing_pivot_is_30(self):
        self.assertEqual(_m._BT_SWING_PIVOT_LEN, 30)

    def test_03_matches_production_defaults(self):
        # detect_breakers carries the production defaults in its signature.
        sig = inspect.signature(_m.detect_breakers)
        self.assertEqual(sig.parameters["i_len"].default, 5)
        self.assertEqual(sig.parameters["s_len"].default, 30)
        self.assertEqual(_m._BT_PIVOT_LEN, sig.parameters["i_len"].default)
        self.assertEqual(_m._BT_SWING_PIVOT_LEN, sig.parameters["s_len"].default)

    def test_04_parity_check_uses_production_params(self):
        src = inspect.getsource(_m._bt_run_parity_check)
        self.assertIn("i_len=_BT_PIVOT_LEN", src)
        self.assertIn("s_len=_BT_SWING_PIVOT_LEN", src)
        # The old hard-coded 3/3 call must be gone (comment mentions are fine).
        self.assertNotIn("i_len=3,", src)


# ══════════════════════════════════════════════════════════════════════════════
# Event tagging + class separation
# ══════════════════════════════════════════════════════════════════════════════

class TestClassTagging(unittest.TestCase):
    def test_05_default_is_internal(self):
        evs = _m._bt_extract_ob_replay_events(_CANDLES, _params())
        self.assertGreater(len(evs), 0)
        self.assertEqual({e["ob_class"] for e in evs}, {"internal"})
        self.assertEqual({e["pivot_len"] for e in evs}, {5})

    def test_06_swing_class_tagged(self):
        evs = _m._bt_extract_ob_replay_events(_CANDLES, _params(ob_class="swing"))
        self.assertGreater(len(evs), 0)
        self.assertEqual({e["ob_class"] for e in evs}, {"swing"})
        self.assertEqual({e["pivot_len"] for e in evs}, {30})

    def test_07_classes_detect_differently(self):
        internal = _m._bt_extract_ob_replay_events(_CANDLES, _params())
        swing    = _m._bt_extract_ob_replay_events(_CANDLES, _params(ob_class="swing"))
        self.assertNotEqual(len(internal), len(swing))
        # 30-length pivots confirm far less often than 5-length pivots.
        self.assertGreater(len(internal), len(swing))

    def test_08_swing_produces_touchable_events(self):
        swing = _m._bt_extract_ob_replay_events(_CANDLES, _params(ob_class="swing"))
        touched = [e for e in swing if e["touch_status"] == "touched"]
        self.assertGreater(len(touched), 0)

    def test_09_deterministic_both_classes(self):
        a1 = _m._bt_extract_ob_replay_events(_CANDLES, _params())
        a2 = _m._bt_extract_ob_replay_events(_CANDLES, _params())
        self.assertEqual([e["ob_id"] for e in a1], [e["ob_id"] for e in a2])
        s1 = _m._bt_extract_ob_replay_events(_CANDLES, _params(ob_class="swing"))
        s2 = _m._bt_extract_ob_replay_events(_CANDLES, _params(ob_class="swing"))
        self.assertEqual([e["ob_id"] for e in s1], [e["ob_id"] for e in s2])

    def test_10_swing_needs_more_history(self):
        # Below the swing minimum (30*2+4 candles) swing returns no events
        # while internal (5*2+4) still can.
        short = _CANDLES[:40]
        swing = _m._bt_extract_ob_replay_events(short, _params(ob_class="swing"))
        self.assertEqual(swing, [])

    def test_11_unknown_class_falls_back_to_internal(self):
        evs = _m._bt_extract_ob_replay_events(_CANDLES, _params(ob_class="nonsense"))
        self.assertEqual({e["ob_class"] for e in evs}, {"nonsense"})
        self.assertEqual({e["pivot_len"] for e in evs}, {5})


# ══════════════════════════════════════════════════════════════════════════════
# Parity at production parameters
# ══════════════════════════════════════════════════════════════════════════════

class TestParity(unittest.TestCase):
    def test_12_parity_runs_and_matches(self):
        internal = _m._bt_extract_ob_replay_events(_CANDLES, _params())
        par = _m._bt_run_parity_check(_CANDLES, internal, _params(include_parity=True))
        self.assertTrue(par.get("enabled"))
        # Every production OB (iLen=5/sLen=30) must match a replay event.
        self.assertEqual(par.get("match_rate_pct"), 100.0)


# ══════════════════════════════════════════════════════════════════════════════
# Dual-class runner (get_klines mocked)
# ══════════════════════════════════════════════════════════════════════════════

class TestDualClassRunner(unittest.TestCase):
    def _run(self, **over):
        with patch.object(_m, "get_klines", return_value=_fake_raw_candles()):
            return _m._bt_run_ob_historical_backtest(_params(**over))

    def test_13_top_level_is_internal(self):
        res = self._run()
        self.assertTrue(res["ok"])
        self.assertEqual(res["ob_class"], "internal")
        self.assertEqual(res["pivot_len"], 5)
        self.assertEqual({e["ob_class"] for e in res["events"]}, {"internal"})

    def test_14_swing_results_separate(self):
        res = self._run()
        sw = res["swing_results"]
        self.assertTrue(sw["enabled"])
        self.assertEqual(sw["ob_class"], "swing")
        self.assertEqual(sw["pivot_len"], 30)
        self.assertGreater(sw["events_total"], 0)
        self.assertEqual({e["ob_class"] for e in sw["events"]}, {"swing"})

    def test_15_no_pooling(self):
        res = self._run()
        # Internal summary counts == internal event count; swing == swing.
        self.assertEqual(res["replay_summary"]["ob_events_detected"],
                         res["events_total"])
        sw = res["swing_results"]
        self.assertEqual(sw["replay_summary"]["ob_events_detected"],
                         sw["events_total"])
        self.assertNotEqual(res["events_total"], sw["events_total"])

    def test_16_full_range_both_classes(self):
        # Both classes replay the SAME full candle range.
        res = self._run()
        self.assertEqual(res["replay_summary"]["replay_bars"],
                         res["swing_results"]["replay_summary"]["replay_bars"])

    def test_17_internal_mode_skips_swing(self):
        res = self._run(ob_class_mode="internal")
        self.assertTrue(res["ok"])
        self.assertFalse(res["swing_results"]["enabled"])

    def test_18_swing_outcomes_use_canonical_rules(self):
        res = self._run()
        sw_events = res["swing_results"]["events"]
        elig = [e for e in sw_events if (e.get("simulation") or {}).get("eligible")]
        self.assertGreater(len(elig), 0)
        for e in elig:
            sim = e["simulation"]
            # entry = zone_high, canonical rule
            self.assertEqual(sim["entry_price"], round(float(e["zone_high"]), 8))

    def test_19_swing_has_no_tv_threshold_analysis(self):
        # TV OB% research is parked — swing pass must not compute it.
        res = self._run()
        sw_os = res["swing_results"]["outcome_summary"]
        self.assertNotIn("tv_ob_pct_threshold_analysis", sw_os)


# ══════════════════════════════════════════════════════════════════════════════
# Endpoint validation (real Flask, mocked runner where needed)
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
        return self.client.post("/api/backtest/ob-historical", json=body)

    def test_20_invalid_ob_class_mode_rejected(self):
        r = self._post({"symbol": "BTCUSDT", "timeframe": "1h",
                        "ob_class_mode": "pooled"})
        self.assertEqual(r.status_code, 400)

    def test_21_valid_modes_accepted(self):
        fake = {"ok": True, "ob_class": "internal",
                "swing_results": {"enabled": True}}
        for mode in ("both", "internal"):
            with patch.object(_m, "_bt_run_ob_historical_backtest",
                              return_value=fake):
                r = self._post({"symbol": "BTCUSDT", "timeframe": "1h",
                                "ob_class_mode": mode})
            self.assertEqual(r.status_code, 200, f"mode {mode} rejected")

    def test_22_default_mode_is_both(self):
        params, err = _m._bt_parse_ob_historical_payload(
            {"symbol": "BTCUSDT", "timeframe": "1h"})
        self.assertIsNone(err)
        self.assertEqual(params["ob_class_mode"], "both")


# ══════════════════════════════════════════════════════════════════════════════
# Downstream isolation — Phase 14/15/16 labs stay internal-only
# ══════════════════════════════════════════════════════════════════════════════

class TestDownstreamIsolation(unittest.TestCase):
    def test_23_wf_params_have_no_ob_class(self):
        p = _m._bt_wf_build_params("BTCUSDT", "1h", 1000, [1, 2, 3])
        self.assertNotIn("ob_class", p)
        evs = _m._bt_extract_ob_replay_events(_CANDLES, p)
        self.assertEqual({e["ob_class"] for e in evs}, {"internal"})

    def test_24_trade_explorer_internal_only(self):
        with patch.object(_m, "get_klines", return_value=_fake_raw_candles()):
            res = _m._bt_run_trade_explorer({
                "symbols": ["BTCUSDT"], "timeframes": ["1h"],
                "candle_count": 1000, "rr": 2, "filters": {},
                "page": 1, "page_size": 50, "sort": {}})
        self.assertTrue(res["ok"])
        self.assertGreater(res["performance"]["ob_count"], 0)

    def test_25_scanner_functions_untouched(self):
        self.assertTrue(hasattr(_m, "detect_obs"))
        self.assertTrue(hasattr(_m, "detect_obs_all"))
        self.assertTrue(hasattr(_m, "detect_pivots"))
        sig = inspect.signature(_m.detect_obs)
        # detect_obs signature unchanged (i_len/s_len still caller-supplied)
        self.assertIn("i_len", sig.parameters)
        self.assertIn("s_len", sig.parameters)

    def test_26_no_db_writes_in_changed_functions(self):
        for fn in (_m._bt_extract_ob_replay_events,
                   _m._bt_run_ob_historical_backtest,
                   _m._bt_run_parity_check):
            src = inspect.getsource(fn)
            self.assertNotIn("INSERT INTO", src)
            self.assertNotIn("db.session", src)
            self.assertNotIn("cursor.execute", src)


if __name__ == "__main__":
    unittest.main(verbosity=2)
