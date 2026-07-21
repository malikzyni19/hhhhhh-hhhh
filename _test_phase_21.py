"""
Phase 21 unit tests — Pass-Profile Walk-Forward Validation (research only).

Covers:
  * record-passes AND logic (True/False/None)
  * fold partition by touch time (train/test, no double-leak)
  * train-selected rule choice (train-only)
  * orchestrator: locked + train_selected, gates/verdict, bootstrap CI
  * no look-ahead (features causal; partition == prefix behaviour)
  * endpoint validation; production isolation

Usage:
    cd /home/user/hhhhhh-hhhh && python3 _test_phase_21.py
"""
import os, sys, traceback, unittest, inspect, math, random
from unittest.mock import patch

os.environ.setdefault("DATABASE_URL", "sqlite:///phase21_test.db")
os.environ.setdefault("SECRET_KEY",   "phase21-test-key")
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


def _fake_raw_candles(seed=42, n=2000):
    rnd = random.Random(seed)
    out = []
    for i in range(n):
        drift = 6.0 * math.sin(i / 9.0) + 3.5 * math.sin(i / 23.0)
        close = 100.0 + drift + rnd.uniform(-1.2, 1.2)
        vol = 800 + 600 * abs(math.sin(i / 7.0))
        out.append({"openTime": i * 3_600_000, "closeTime": (i + 1) * 3_600_000 - 1,
                    "open": close - 0.2, "high": close + rnd.uniform(0.4, 1.6),
                    "low": close - rnd.uniform(0.4, 1.6), "close": close,
                    "volume": vol, "quoteVolume": vol * close, "tradeCount": 100,
                    "takerBuyBase": vol / 2, "takerBuyQuote": vol / 2})
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
# record-passes AND logic
# ══════════════════════════════════════════════════════════════════════════════

class TestRecordPasses(unittest.TestCase):
    def test_01_all_true(self):
        r = {"touch_number": 1}
        fns = [lambda x: x["touch_number"] == 1, lambda x: True]
        self.assertIs(_m._bt_pwf_record_passes(r, fns), True)

    def test_02_any_false(self):
        r = {"touch_number": 1}
        fns = [lambda x: x["touch_number"] == 1, lambda x: False]
        self.assertIs(_m._bt_pwf_record_passes(r, fns), False)

    def test_03_none_without_false(self):
        r = {"touch_number": 1}
        fns = [lambda x: x["touch_number"] == 1, lambda x: None]
        self.assertIsNone(_m._bt_pwf_record_passes(r, fns))

    def test_04_false_beats_none(self):
        r = {}
        fns = [lambda x: None, lambda x: False]
        self.assertIs(_m._bt_pwf_record_passes(r, fns), False)

    def test_05_exception_treated_as_none(self):
        r = {}
        fns = [lambda x: x["missing"]]   # KeyError → None
        self.assertIsNone(_m._bt_pwf_record_passes(r, fns))


# ══════════════════════════════════════════════════════════════════════════════
# Fold partition by touch time
# ══════════════════════════════════════════════════════════════════════════════

class TestPartition(unittest.TestCase):
    def _candles(self, n=100):
        return [{"open_time": i * 1000, "high": 1, "low": 1, "close": 1} for i in range(n)]

    def test_06_train_test_split(self):
        candles = self._candles(100)
        fold = {"fold": 1, "train_end_index_exclusive": 50,
                "test_start_index": 50, "test_end_index_exclusive": 70}
        recs = [{"touch_time": i * 1000} for i in range(100)]
        train, test = _m._bt_pwf_partition(recs, fold, candles)
        # train = touch before test_start_time (bar 50 → 50000)
        self.assertTrue(all(r["touch_time"] < 50000 for r in train))
        # test = [50000, 70000)
        self.assertTrue(all(50000 <= r["touch_time"] < 70000 for r in test))
        self.assertEqual(len(test), 20)
        self.assertEqual(len(train), 50)

    def test_07_no_test_leak_into_train(self):
        candles = self._candles(100)
        fold = {"fold": 1, "train_end_index_exclusive": 50,
                "test_start_index": 50, "test_end_index_exclusive": 70}
        recs = [{"touch_time": i * 1000} for i in range(100)]
        train, test = _m._bt_pwf_partition(recs, fold, candles)
        train_ids = {id(r) for r in train}
        test_ids = {id(r) for r in test}
        self.assertEqual(train_ids & test_ids, set())   # disjoint

    def test_08_records_without_touch_time_skipped(self):
        candles = self._candles(100)
        fold = {"fold": 1, "train_end_index_exclusive": 50,
                "test_start_index": 50, "test_end_index_exclusive": 70}
        recs = [{"touch_time": None}, {"touch_time": 55000}]
        train, test = _m._bt_pwf_partition(recs, fold, candles)
        self.assertEqual(len(train), 0)
        self.assertEqual(len(test), 1)


# ══════════════════════════════════════════════════════════════════════════════
# Train-selected rule choice
# ══════════════════════════════════════════════════════════════════════════════

class TestRuleSelect(unittest.TestCase):
    def _rec(self, tn, outcome, realized):
        # minimal record the rule fns can read
        return {"touch_number": tn, "outcome": outcome, "realized_r": realized,
                "htf_alignment": "with", "tf_alignment": "with",
                "formation_volume_ratio": 1.2,
                "features": {k: False for k in _m._AP_FEATURE_KEYS}}

    def test_09_selects_rule_meeting_min_train(self):
        # 20 first-touch wins vs some noise → first_touch_only should be pickable
        recs = ([self._rec(1, "win", 2.0)] * 20 +
                [self._rec(3, "loss", -1.0)] * 20)
        sel = _m._bt_pwf_select_rule(recs, _m._bt_fl_rule_predicates())
        self.assertIsNotNone(sel)
        self.assertIn("id", sel)
        self.assertGreaterEqual(sel["train_trades"], _m._PWF_MIN_TRAIN_TRADES)
        self.assertIsNotNone(sel["train_expectancy"])

    def test_10_none_when_no_rule_qualifies(self):
        recs = [self._rec(1, "win", 2.0)] * 3   # below min train trades
        self.assertIsNone(_m._bt_pwf_select_rule(recs, _m._bt_fl_rule_predicates()))


# ══════════════════════════════════════════════════════════════════════════════
# Orchestrator
# ══════════════════════════════════════════════════════════════════════════════

class TestOrchestrator(unittest.TestCase):
    def _run(self, **over):
        req = {"symbols": ["BTCUSDT", "ETHUSDT", "XRPUSDT"],
               "timeframes": ["15m", "1h"], "candle_count": 2000, "rr": 2,
               "ob_class": "internal", "candidate_mode": "locked",
               "profile_rules": ["first_touch_only", "not_oversized"],
               "fold_count": 4, "initial_train_pct": 45.0, "test_pct": 15.0}
        req.update(over)
        with patch.object(_m, "get_klines", side_effect=_det_gk()):
            return _m._bt_run_profile_walk_forward(req)

    def test_11_locked_shape(self):
        res = self._run()
        self.assertTrue(res["ok"])
        self.assertTrue(res["authoritative_execution"])
        self.assertFalse(res["client_results_accepted"])
        self.assertIn(res["verdict"], ("PASS", "FAIL", "INSUFFICIENT"))
        s = res["summary"]
        self.assertIn("oos_pass_trades", s)
        self.assertIn("oos_expectancy_delta", s)
        self.assertIn("beat_baseline_pct", s)
        self.assertIn("expectancy_delta_low", res["bootstrap_delta_ci"])

    def test_12_verdict_gates_consistent(self):
        res = self._run()
        # verdict PASS requires zero failed gates
        if res["verdict"] == "PASS":
            self.assertEqual(res["failed_gates"], [])
        else:
            self.assertTrue(res["verdict"] in ("FAIL", "INSUFFICIENT"))

    def test_13_per_fold_delta_math(self):
        res = self._run()
        for f in res["fold_windows"]:
            if f["evaluable"]:
                self.assertAlmostEqual(
                    f["expectancy_delta"],
                    round(f["pass_expectancy_r"] - f["baseline_expectancy_r"], 6),
                    places=5)
                self.assertEqual(f["beat_baseline"], f["expectancy_delta"] > 0)

    def test_14_oos_totals_are_pass_subset_of_baseline(self):
        res = self._run()
        s = res["summary"]
        # pass trades <= baseline trades (pass group is a subset of test trades)
        self.assertLessEqual(s["oos_pass_trades"], s["oos_baseline_trades"])
        if s["oos_baseline_trades"]:
            self.assertIsNotNone(s["trade_retention_pct"])

    def test_15_train_selected_reports_rules(self):
        res = self._run(candidate_mode="train_selected", profile_rules=[])
        self.assertTrue(res["ok"])
        self.assertIsNotNone(res["selected_rule_counts"])
        # every selected rule id is a real library rule
        valid = {ru["id"] for ru in _m._bt_fl_rule_predicates()}
        for rid in res["selected_rule_counts"]:
            self.assertIn(rid, valid)

    def test_16_locked_no_rules_no_folds(self):
        # locked with empty profile → no evaluable folds (function-level guard)
        res = self._run(profile_rules=[])
        self.assertEqual(res["summary"]["evaluable_folds"], 0)
        self.assertEqual(res["verdict"], "INSUFFICIENT")

    def test_17_deterministic_folds(self):
        # Same input → identical fold windows (no Date/random in the path;
        # the no-look-ahead property itself rests on feature causality,
        # verified in the Phase 20 suites).
        a = self._run()
        b = self._run()
        ea = [(f["symbol"], f["fold"], f["pass_trades"], f["expectancy_delta"])
              for f in a["fold_windows"]]
        eb = [(f["symbol"], f["fold"], f["pass_trades"], f["expectancy_delta"])
              for f in b["fold_windows"]]
        self.assertEqual(ea, eb)   # deterministic

    def test_18_failure_isolation(self):
        good = _fake_raw_candles()
        def gk(sym, tf, limit=300, market="perpetual", extended=False):
            if sym == "ETHUSDT" and tf == "15m":
                raise RuntimeError("boom")
            return good
        with patch.object(_m, "get_klines", side_effect=gk):
            res = _m._bt_run_profile_walk_forward({
                "symbols": ["BTCUSDT", "ETHUSDT"], "timeframes": ["15m"],
                "candle_count": 2000, "rr": 2, "ob_class": "internal",
                "candidate_mode": "locked",
                "profile_rules": ["first_touch_only"],
                "fold_count": 4, "initial_train_pct": 45.0, "test_pct": 15.0})
        self.assertTrue(res["ok"])
        self.assertEqual(len(res["failures"]), 1)
        self.assertEqual(res["failures"][0]["symbol"], "ETHUSDT")

    def test_19_definitions_documented(self):
        res = self._run()
        for k in ("method", "locked", "train_selected", "verdict", "note"):
            self.assertIn(k, res["definitions"])


# ══════════════════════════════════════════════════════════════════════════════
# Endpoint
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
        return self.client.post("/api/backtest/ob-historical/profile-walk-forward", json=body)

    def _ok(self, **over):
        b = {"symbols": ["BTCUSDT", "ETHUSDT"], "timeframes": ["15m", "1h"],
             "candle_count": 2000, "rr": 2, "ob_class": "internal",
             "candidate_mode": "locked", "profile_rules": ["first_touch_only"]}
        b.update(over)
        return b

    def test_20_reject_client_results(self):
        for k in ("trades", "fold_windows", "summary", "verdict", "win_rate"):
            r = self._post(self._ok(**{k: [1]}))
            self.assertEqual(r.status_code, 400, f"{k} not rejected")

    def test_21_unauthorized(self):
        anon = _m.app.test_client()
        r = anon.post("/api/backtest/ob-historical/profile-walk-forward", json=self._ok())
        self.assertIn(r.status_code, (301, 302, 401, 403))

    def test_22_authorized_ok(self):
        fake = {"ok": True, "verdict": "FAIL"}
        with patch.object(_m, "_bt_run_profile_walk_forward", return_value=fake):
            r = self._post(self._ok())
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.get_json()["ok"])

    def test_23_validation(self):
        self.assertEqual(self._post(self._ok(symbols=["A","B","C","D","E","F"])).status_code, 400)
        self.assertEqual(self._post(self._ok(symbols=["BTCUSDT","BTCUSDT"])).status_code, 400)
        self.assertEqual(self._post(self._ok(timeframes=["7m"])).status_code, 400)
        self.assertEqual(self._post(self._ok(rr=9)).status_code, 400)
        self.assertEqual(self._post(self._ok(ob_class="pooled")).status_code, 400)
        self.assertEqual(self._post(self._ok(candidate_mode="magic")).status_code, 400)
        self.assertEqual(self._post(self._ok(profile_rules=["not_a_rule"])).status_code, 400)
        self.assertEqual(self._post(self._ok(candidate_mode="locked", profile_rules=[])).status_code, 400)

    def test_24_train_selected_needs_no_rules(self):
        fake = {"ok": True, "verdict": "FAIL"}
        with patch.object(_m, "_bt_run_profile_walk_forward", return_value=fake):
            r = self._post(self._ok(candidate_mode="train_selected", profile_rules=[]))
        self.assertEqual(r.status_code, 200)


# ══════════════════════════════════════════════════════════════════════════════
# Isolation
# ══════════════════════════════════════════════════════════════════════════════

class TestIsolation(unittest.TestCase):
    def test_25_no_db_writes(self):
        for fn in (_m._bt_pwf_record_passes, _m._bt_pwf_partition,
                   _m._bt_pwf_select_rule, _m._bt_run_profile_walk_forward):
            src = inspect.getsource(fn)
            self.assertNotIn("INSERT INTO", src)
            self.assertNotIn("db.session", src)
            self.assertNotIn("cursor.execute", src)

    def test_26_reuses_wf_and_rule_machinery(self):
        # sanity: the phase reuses the audited fold builder + bootstrap + rules
        self.assertTrue(hasattr(_m, "_bt_build_walk_forward_folds"))
        self.assertTrue(hasattr(_m, "_bt_wf_bootstrap_delta"))
        self.assertTrue(hasattr(_m, "_bt_fl_rule_predicates"))
        self.assertTrue(hasattr(_m, "_bt_fl_aggregate"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
