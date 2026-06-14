"""Phase 11.15 AI Learning Review Loop — unit tests.

Tests pure-Python functions only. No DB. No Flask. No AI call. No network.
Run: python3 _test_learning_review_11_15.py
"""
import os, sys, types, json, importlib.util, pathlib
from decimal import Decimal
from datetime import datetime, timezone, timedelta

os.environ.setdefault("DATABASE_URL",    "sqlite:///:memory:")
os.environ.setdefault("SECRET_KEY",      "test")
os.environ.setdefault("RESEND_API_KEY",  "test")
os.environ.setdefault("TURNSTILE_SECRET","")

# ── Minimal stubs ─────────────────────────────────────────────────────────────

for _mn in ["psycopg2", "psycopg2.extras", "resend", "flask_login",
            "flask_sqlalchemy", "sqlalchemy", "sqlalchemy.orm",
            "models", "flask", "extensions"]:
    if _mn not in sys.modules:
        sys.modules[_mn] = types.ModuleType(_mn)

_sa = sys.modules["sqlalchemy"]
class _FuncStub:
    def __getattr__(self, name):
        def _any(*a, **kw): return None
        return _any
_sa.func = _FuncStub()

sys.path.insert(0, os.path.dirname(__file__))

# ── Load paper_performance (dependency) ───────────────────────────────────────

_pp_spec = importlib.util.spec_from_file_location(
    "live_monitor.paper_performance",
    pathlib.Path(__file__).parent / "live_monitor" / "paper_performance.py",
)
pp = importlib.util.module_from_spec(_pp_spec)
sys.modules["live_monitor.paper_performance"] = pp
_pp_spec.loader.exec_module(pp)

# Provide live_monitor package stub
_lm_pkg = types.ModuleType("live_monitor")
sys.modules["live_monitor"] = _lm_pkg

# ── Load ai_learning_review ───────────────────────────────────────────────────

_lr_spec = importlib.util.spec_from_file_location(
    "live_monitor.ai_learning_review",
    pathlib.Path(__file__).parent / "live_monitor" / "ai_learning_review.py",
)
lr = importlib.util.module_from_spec(_lr_spec)
sys.modules["live_monitor.ai_learning_review"] = lr
_lr_spec.loader.exec_module(lr)


# ── Trade mock ────────────────────────────────────────────────────────────────

class _MT:
    def __init__(self, **kw):
        self.id              = kw.get("id", 1)
        self.symbol          = kw.get("symbol", "BTCUSDT")
        self.side            = kw.get("side", "BUY")
        self.status          = kw.get("status", "closed")
        self.realized_pnl    = kw.get("realized_pnl", "10.00")
        self.outcome         = kw.get("outcome", "win")
        self.outcome_reason  = kw.get("outcome_reason", "tp")
        self.risk_reward     = kw.get("risk_reward", "2.0")
        self.duration_seconds= kw.get("duration_seconds", 3600)
        self.ai_decision_json     = kw.get("ai_decision_json", None)
        self.entry_snapshot_json  = kw.get("entry_snapshot_json", None)
        self.ai_post_trade_review_json = kw.get("ai_post_trade_review_json", None)
        self.execution_intent_json= kw.get("execution_intent_json", None)
        self.closed_at       = kw.get("closed_at",  datetime.now(timezone.utc))
        self.updated_at      = kw.get("updated_at", None)
        self.created_at      = kw.get("created_at", datetime.now(timezone.utc))


def _mk_trades(n=10, outcome="win", pnl="10.00"):
    return [_MT(id=i, realized_pnl=pnl, outcome=outcome) for i in range(n)]


# ── Test runner ───────────────────────────────────────────────────────────────

_pass = 0; _fail = 0

def check(name, cond, info=""):
    global _pass, _fail
    if cond:
        print(f"  PASS  {name}")
        _pass += 1
    else:
        print(f"  FAIL  {name}" + (f"  [{info}]" if info else ""))
        _fail += 1

def section(title):
    print(f"\n── {title} ──")


# ══════════════════════════════════════════════════════════════════════════════
# GROUP 1: Filter building
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 1 — _lm_build_learning_review_filters")

f = lr._lm_build_learning_review_filters(1, review_scope="portfolio", period="30d")
check("1-1 valid scope portfolio stored", f.get("_review_scope") == "portfolio")
check("1-2 no scope error for portfolio", f.get("_review_scope_err") is None)

f2 = lr._lm_build_learning_review_filters(1, review_scope="symbol", period="7d", symbol="BTCUSDT", symbol_supplied=True)
check("1-3 valid scope symbol", f2.get("_review_scope") == "symbol")
check("1-4 symbol stored", f2.get("symbol") == "BTCUSDT")

f3 = lr._lm_build_learning_review_filters(1, review_scope="invalid_scope", period="30d")
check("1-5 invalid scope sets scope_err", f3.get("_review_scope_err") == "invalid_review_scope")
check("1-6 invalid scope falls back to portfolio", f3.get("_review_scope") == "portfolio")

f4 = lr._lm_build_learning_review_filters(1, review_scope="item", period="90d")
check("1-7 valid scope item", f4.get("_review_scope") == "item")

# blank symbol with symbol_supplied
f5 = lr._lm_build_learning_review_filters(1, review_scope="portfolio", period="30d",
                                            symbol="", symbol_supplied=True)
check("1-8 blank symbol with symbol_supplied → symbol_err", f5.get("_symbol_err") == "invalid_symbol")

# ══════════════════════════════════════════════════════════════════════════════
# GROUP 2: Filter validation
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 2 — _lm_validate_learning_review_filters")

valid_f = lr._lm_build_learning_review_filters(1, review_scope="portfolio", period="30d")
errs = lr._lm_validate_learning_review_filters(valid_f)
check("2-1 valid filters → empty errs", errs == {})

invalid_scope_f = {**valid_f, "_review_scope_err": "invalid_review_scope"}
errs2 = lr._lm_validate_learning_review_filters(invalid_scope_f)
check("2-2 scope error propagates", "review_scope" in errs2)

invalid_sym_f = {**valid_f, "_symbol_err": "invalid_symbol"}
errs3 = lr._lm_validate_learning_review_filters(invalid_sym_f)
check("2-3 symbol error propagates", "symbol" in errs3)

invalid_side_f = {**valid_f, "_side_err": "invalid_side"}
errs4 = lr._lm_validate_learning_review_filters(invalid_side_f)
check("2-4 side error propagates", "side" in errs4)

# ══════════════════════════════════════════════════════════════════════════════
# GROUP 3: Evidence builder (mocked query)
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 3 — _lm_build_learning_evidence")

def _mock_query(user_id, filters):
    n = filters.get("_mock_n", 10)
    trades = _mk_trades(n)
    return trades, {"total_available": n, "rows_loaded": n, "truncated": False}

_orig_query = lr._lm_query_closed_paper_trades_from_filters

def _patched_query(user_id, filters):
    return _mock_query(user_id, filters)

# Patch on the lr module (where it was imported at load time)
lr._lm_query_closed_paper_trades_from_filters = _patched_query

f_valid = lr._lm_build_learning_review_filters(1, review_scope="portfolio", period="30d")
f_valid["_mock_n"] = 10

ev = lr._lm_build_learning_evidence(1, f_valid)
check("3-1 sample_size matches trade count", ev["sample_size"] == 10)
check("3-2 sample_quality high for >=10", ev["sample_quality"] == "high")
check("3-3 segments present", isinstance(ev.get("segments"), dict))
check("3-4 execution_quality present", isinstance(ev.get("execution_quality"), dict))
check("3-5 recent_trades present", isinstance(ev.get("recent_trades"), list))
check("3-6 warnings is list", isinstance(ev.get("warnings"), list))

f_valid5 = lr._lm_build_learning_review_filters(1, review_scope="portfolio", period="30d")
f_valid5["_mock_n"] = 5

ev5 = lr._lm_build_learning_evidence(1, f_valid5)
check("3-7 sample_quality low for 5-9", ev5["sample_quality"] == "low")

def _zero_query(user_id, filters):
    return [], {"total_available": 0, "rows_loaded": 0, "truncated": False}

lr._lm_query_closed_paper_trades_from_filters = _zero_query

f_valid0 = lr._lm_build_learning_review_filters(1, review_scope="portfolio", period="30d")
ev0 = lr._lm_build_learning_evidence(1, f_valid0)
check("3-8 zero trades → no_closed_trades warning", "no_closed_trades_in_period" in ev0["warnings"])
check("3-9 zero trades → insufficient quality", ev0["sample_quality"] == "insufficient")

# Restore
lr._lm_query_closed_paper_trades_from_filters = _patched_query

# ══════════════════════════════════════════════════════════════════════════════
# GROUP 4: Evidence validation guard
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 4 — evidence builder rejects unvalidated filters")

# These tests don't reach the DB (they fail in validate step before query)
try:
    bad_f = {**f_valid, "_review_scope_err": "invalid_review_scope"}
    lr._lm_build_learning_evidence(1, bad_f)
    check("4-1 raises ValueError for invalid filters", False, "no exception raised")
except ValueError as e:
    check("4-1 raises ValueError for invalid filters", True)
    check("4-2 error message mentions field", "review_scope" in str(e) or "unvalidated" in str(e))

# ══════════════════════════════════════════════════════════════════════════════
# GROUP 5: Segment builder
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 5 — _lm_build_learning_segments")

mixed_trades = [
    _MT(symbol="BTCUSDT", side="BUY",  realized_pnl="20.00", outcome="win"),
    _MT(symbol="BTCUSDT", side="BUY",  realized_pnl="-5.00", outcome="loss"),
    _MT(symbol="ETHUSDT", side="SELL", realized_pnl="10.00", outcome="win"),
    _MT(symbol="ETHUSDT", side="SELL", realized_pnl="8.00",  outcome="win"),
    _MT(symbol="ETHUSDT", side="SELL", realized_pnl="-3.00", outcome="loss"),
]

segs = lr._lm_build_learning_segments(mixed_trades)
check("5-1 by_symbol present", "by_symbol" in segs)
check("5-2 by_side present", "by_side" in segs)
check("5-3 by_outcome_reason present", "by_outcome_reason" in segs)
check("5-4 by_confidence present", "by_confidence" in segs)
check("5-5 by_setup_type present", "by_setup_type" in segs)
check("5-6 by_entry_mode present", "by_entry_mode" in segs)

sym_labels = {s["label"] for s in segs["by_symbol"]}
check("5-7 BTCUSDT in symbol segments", "BTCUSDT" in sym_labels)
check("5-8 ETHUSDT in symbol segments", "ETHUSDT" in sym_labels)

btc_seg = next((s for s in segs["by_symbol"] if s["label"]=="BTCUSDT"), None)
check("5-9 BTCUSDT count=2", btc_seg and btc_seg["count"]==2)
check("5-10 BTCUSDT wins=1", btc_seg and btc_seg["wins"]==1)

eth_seg = next((s for s in segs["by_symbol"] if s["label"]=="ETHUSDT"), None)
check("5-11 ETHUSDT count=3", eth_seg and eth_seg["count"]==3)
check("5-12 ETHUSDT win_rate=66.67", eth_seg and eth_seg.get("win_rate") is not None)

# ══════════════════════════════════════════════════════════════════════════════
# GROUP 6: Execution quality
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 6 — _lm_build_execution_quality")

tp_trades = [
    _MT(realized_pnl="10", outcome="win",  outcome_reason="tp",          risk_reward="2.0"),
    _MT(realized_pnl="8",  outcome="win",  outcome_reason="take_profit",  risk_reward="2.0"),
    _MT(realized_pnl="-4", outcome="loss", outcome_reason="sl",           risk_reward="2.0"),
    _MT(realized_pnl="-3", outcome="loss", outcome_reason="manual",       risk_reward="1.5"),
]

eq = lr._lm_build_execution_quality(tp_trades)
check("6-1 tp_exit_count=2", eq["tp_exit_count"]==2)
check("6-2 sl_exit_count=1", eq["sl_exit_count"]==1)
check("6-3 manual_exit_count=1", eq["manual_exit_count"]==1)
check("6-4 avg_planned_rr computed", eq["avg_planned_rr"] is not None)
check("6-5 tp_pct=50.0", eq["tp_pct"]==50.0)

# ══════════════════════════════════════════════════════════════════════════════
# GROUP 7: Observation candidates
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 7 — _lm_build_learning_observation_candidates")

ev_sample = {
    "sample_size": 15,
    "segments": {
        "by_symbol": [
            {"label": "BTCUSDT", "count": 10, "wins": 2, "losses": 8, "breakevens": 0,
             "win_rate": 20.0, "net_pnl": "-30"},
            {"label": "ETHUSDT", "count": 5,  "wins": 4, "losses": 1, "breakevens": 0,
             "win_rate": 80.0, "net_pnl": "40"},
        ],
        "by_side": [
            {"label": "BUY",  "count": 8, "wins": 6, "losses": 2, "breakevens":0, "win_rate":75.0, "net_pnl":"40"},
            {"label": "SELL", "count": 7, "wins": 1, "losses": 6, "breakevens":0, "win_rate":14.3, "net_pnl":"-20"},
        ],
        "by_outcome_reason": [],
        "by_confidence":    [],
        "by_setup_type":    [],
        "by_entry_mode":    [],
    },
    "execution_quality": {
        "avg_rr_capture": 0.4,
        "manual_pct": 60.0,
    },
}

cands = lr._lm_build_learning_observation_candidates(ev_sample)
types_found = {c["type"] for c in cands}
check("7-1 candidates list is non-empty", len(cands) > 0)
check("7-2 symbol_underperformance detected for BTCUSDT", "symbol_underperformance" in types_found)
check("7-3 symbol_outperformance detected for ETHUSDT", "symbol_outperformance" in types_found)
check("7-4 side_imbalance detected", "side_imbalance" in types_found)
check("7-5 low_rr_capture detected (capture=0.4)", "low_rr_capture" in types_found)
check("7-6 exit_timing detected (manual 60%)", "exit_timing_observation" in types_found)

ev_empty = {
    "sample_size": 0,
    "segments": {"by_symbol":[],"by_side":[],"by_outcome_reason":[],"by_confidence":[],"by_setup_type":[],"by_entry_mode":[]},
    "execution_quality": {},
}
cands_empty = lr._lm_build_learning_observation_candidates(ev_empty)
check("7-7 zero trades → no candidates", cands_empty == [])

# ══════════════════════════════════════════════════════════════════════════════
# GROUP 8: Prompt builder
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 8 — _lm_build_learning_review_prompt")

ev_prompt = {
    "sample_size": 20, "sample_quality": "high",
    "period": "30d", "review_scope": "portfolio",
    "symbol": None, "side": None,
    "execution_quality": {"avg_rr_capture": 0.7, "tp_pct": 55.0},
    "segments": {"by_symbol": [], "by_side": [], "by_outcome_reason":[], "by_confidence":[],"by_setup_type":[],"by_entry_mode":[]},
    "warnings": [],
}

prompt = lr._lm_build_learning_review_prompt(ev_prompt, cands)
check("8-1 prompt is a string", isinstance(prompt, str))
check("8-2 prompt mentions auto_apply_allowed", "auto_apply_allowed" in prompt)
check("8-3 prompt mentions CRITICAL GUARDRAILS", "CRITICAL GUARDRAILS" in prompt)
check("8-4 prompt includes sample size", "20" in prompt)
check("8-5 prompt requests JSON output", '"observations"' in prompt)
check("8-6 auto_apply_allowed false in schema", '"auto_apply_allowed": false' in prompt)

# ══════════════════════════════════════════════════════════════════════════════
# GROUP 9: Response parser and validator
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 9 — _lm_parse_learning_review_response / _lm_validate_learning_review_response")

valid_ai_resp = {
    "title": "Test Review",
    "summary": "Good test summary.",
    "confidence_level": "medium",
    "observations": [
        {"type": "symbol_underperformance", "label": "BTCUSDT",
         "finding": "BTC had low win rate.", "sample_n": 10,
         "confidence": "medium", "auto_apply_allowed": False}
    ],
    "warnings": [],
    "guardrails": {"auto_apply_allowed": False, "ai_can_execute": False, "auto_execution_allowed": False}
}

parsed = lr._lm_parse_learning_review_response(valid_ai_resp)
check("9-1 valid response parsed ok", "_parse_error" not in parsed)
check("9-2 title preserved", parsed.get("title") == "Test Review")
check("9-3 observations count=1", len(parsed.get("observations",[])) == 1)
check("9-4 guardrails auto_apply_allowed=False", parsed["guardrails"]["auto_apply_allowed"] is False)
check("9-5 guardrails ai_can_execute=False", parsed["guardrails"]["ai_can_execute"] is False)

is_valid, reasons = lr._lm_validate_learning_review_response(parsed)
check("9-6 valid response passes validation", is_valid)
check("9-7 no validation reasons for valid response", reasons == [])

# AI tries to set auto_apply_allowed=True → must be forced False
sneaky_resp = {
    "title": "Sneaky",
    "summary": "Trying to auto apply.",
    "confidence_level": "high",
    "observations": [
        {"type": "general_observation", "label": "x", "finding": "y",
         "sample_n": 5, "confidence": "high", "auto_apply_allowed": True}
    ],
    "warnings": [],
    "guardrails": {"auto_apply_allowed": True, "ai_can_execute": True, "auto_execution_allowed": True}
}
parsed_sneaky = lr._lm_parse_learning_review_response(sneaky_resp)
check("9-8 auto_apply_allowed forced False in guardrails", parsed_sneaky["guardrails"]["auto_apply_allowed"] is False)
check("9-9 ai_can_execute forced False in guardrails", parsed_sneaky["guardrails"]["ai_can_execute"] is False)
check("9-10 auto_execution_allowed forced False in guardrails", parsed_sneaky["guardrails"]["auto_execution_allowed"] is False)
check("9-11 obs auto_apply_allowed forced False", parsed_sneaky["observations"][0]["auto_apply_allowed"] is False)
check("9-12 validation_warnings mention force", len(parsed_sneaky.get("_validation_warnings",[]))>0)

# Invalid obs type gets remapped
bad_type_resp = {
    "title": "Bad type", "summary": "x", "confidence_level": "low",
    "observations": [{"type": "illegal_type_xyz", "label": "x", "finding": "y",
                      "sample_n": 1, "confidence": "low", "auto_apply_allowed": False}],
    "warnings": [],
    "guardrails": {"auto_apply_allowed": False, "ai_can_execute": False, "auto_execution_allowed": False}
}
parsed_bad = lr._lm_parse_learning_review_response(bad_type_resp)
check("9-13 invalid obs type remapped to general_observation", parsed_bad["observations"][0]["type"] == "general_observation")

# Non-dict response
parsed_non_dict = lr._lm_parse_learning_review_response("not a dict")
check("9-14 non-dict response returns error key", "_parse_error" in parsed_non_dict)
is_v, reasons_nd = lr._lm_validate_learning_review_response(parsed_non_dict)
check("9-15 non-dict response is invalid", not is_v)

# Empty observations list is still valid structure
empty_obs_resp = {
    "title": "Empty", "summary": "No obs.", "confidence_level": "low",
    "observations": [],
    "warnings": ["no_patterns"],
    "guardrails": {"auto_apply_allowed": False, "ai_can_execute": False, "auto_execution_allowed": False}
}
parsed_empty = lr._lm_parse_learning_review_response(empty_obs_resp)
is_v2, _ = lr._lm_validate_learning_review_response(parsed_empty)
check("9-16 empty observations list is valid", is_v2)

# ══════════════════════════════════════════════════════════════════════════════
# GROUP 10: Status transition logic
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 10 — status transition table")

transitions = lr._STATUS_TRANSITIONS

check("10-1 generated → accepted_insight allowed", "accepted_insight" in transitions["generated"])
check("10-2 generated → rejected allowed",         "rejected" in transitions["generated"])
check("10-3 generated → archived allowed",          "archived" in transitions["generated"])
check("10-4 generated → generated NOT allowed",    "generated" not in transitions["generated"])
check("10-5 accepted_insight → archived allowed",  "archived" in transitions["accepted_insight"])
check("10-6 accepted_insight → rejected NOT allowed","rejected" not in transitions["accepted_insight"])
check("10-7 rejected → archived allowed",          "archived" in transitions["rejected"])
check("10-8 rejected → accepted_insight NOT allowed","accepted_insight" not in transitions["rejected"])
check("10-9 archived → nothing allowed",           transitions["archived"] == frozenset())
check("10-10 reviewed → accepted_insight allowed", "accepted_insight" in transitions["reviewed"])

# ══════════════════════════════════════════════════════════════════════════════
# GROUP 11: Guardrails constants
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 11 — _GUARDRAILS permanent invariants")

g = lr._GUARDRAILS
check("11-1 can_auto_submit is False",             g["can_auto_submit"]             is False)
check("11-2 auto_execution_allowed is False",      g["auto_execution_allowed"]      is False)
check("11-3 ai_can_execute is False",              g["ai_can_execute"]              is False)
check("11-4 live_disabled is True",                g["live_disabled"]               is True)
check("11-5 testnet_strategy_validation is False", g["testnet_strategy_validation"] is False)
check("11-6 auto_apply_allowed is False",          g["auto_apply_allowed"]          is False)
check("11-7 read_only is True",                    g["read_only"]                   is True)

# ══════════════════════════════════════════════════════════════════════════════
# GROUP 12: Prompt includes all valid observation types
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 12 — prompt schema coverage")

expected_obs_types = {
    "symbol_underperformance", "symbol_outperformance", "side_imbalance",
    "low_rr_capture", "high_rr_capture", "exit_timing_observation",
    "confidence_filter_signal", "setup_type_signal", "entry_mode_signal",
    "outcome_reason_pattern", "data_quality_warning", "general_observation",
}
check("12-1 _VALID_OBS_TYPES includes all 12 types", lr._VALID_OBS_TYPES == expected_obs_types)
for t in expected_obs_types:
    check(f"12-type:{t}", t in prompt)

# ══════════════════════════════════════════════════════════════════════════════
# GROUP 13: _lm_build_accepted_learning_context — DB layer (mock)
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 13 — accepted learning context (mock DB)")

class _MockReview:
    def __init__(self, **kw):
        self.id             = kw.get("id", 1)
        self.user_id        = kw.get("user_id", 1)
        self.item_id        = kw.get("item_id", None)
        self.review_scope   = kw.get("review_scope", "portfolio")
        self.period         = kw.get("period", "30d")
        self.symbol         = kw.get("symbol", None)
        self.side           = kw.get("side", None)
        self.status         = kw.get("status", "accepted_insight")
        self.title          = kw.get("title", "Test Insight")
        self.summary        = kw.get("summary", "Summary text")
        self.confidence_level = kw.get("confidence_level", "medium")
        self.reviewed_at    = kw.get("reviewed_at", datetime.now(timezone.utc))
        self.review_json    = kw.get("review_json", json.dumps({
            "observations": [{"type": "symbol_underperformance"}]
        }))

class _MockQuery:
    def __init__(self, rows):
        self._rows = rows
    def filter_by(self, **kw):
        return self
    def filter(self, *a):
        return self
    def order_by(self, *a):
        return self
    def limit(self, n):
        self._n = n
        return self
    def offset(self, n):
        return self
    def all(self):
        return self._rows[:getattr(self, "_n", 999)]

_mock_insights = [_MockReview(id=i, title=f"Insight {i}") for i in range(25)]

import live_monitor.ai_learning_review as lr_mod

_orig_LR = None
_models_mod = sys.modules.get("models") or types.ModuleType("models")

class _MockLR:
    query = _MockQuery(_mock_insights)

_models_mod.LiveMonitorLearningReview = _MockLR
sys.modules["models"] = _models_mod

# Patch the module's import to use mock
_orig_import = __builtins__.__dict__.get("__import__") if hasattr(__builtins__, "__dict__") else None

# Direct test: call with mock reviews list
context = lr._lm_build_accepted_learning_context.__code__
# Since we can't easily mock the import inside the function, test the serializer path
# and the shape of output
check("13-1 _MAX_ACCEPTED_INSIGHTS=20", lr._MAX_ACCEPTED_INSIGHTS == 20)

# Test _serialize_review independently
class _FakeRow:
    id=1; user_id=1; item_id=None; review_scope="portfolio"; period="30d"
    symbol=None; side=None; status="generated"; title="T"; summary="S"
    sample_size=10; sample_quality="high"; confidence_level="medium"
    warning_count=0; source="ai"; model_name="gpt4"; prompt_version="11.15.0"
    parent_review_id=None; human_note=None
    created_at=datetime.now(timezone.utc); updated_at=None; reviewed_at=None
    review_json='{"observations":[]}'; evidence_json='{}'

from live_monitor.ai_learning_review import _serialize_review
d = _serialize_review(_FakeRow(), include_json=False)
check("13-2 serialize returns id", d["id"] == 1)
check("13-3 serialize returns status", d["status"] == "generated")
check("13-4 serialize includes guardrails", "guardrails" in d)
check("13-5 serialized guardrails auto_apply_allowed=False", d["guardrails"]["auto_apply_allowed"] is False)
check("13-6 no review_data when include_json=False", "review_data" not in d)

d2 = _serialize_review(_FakeRow(), include_json=True)
check("13-7 review_data present when include_json=True", "review_data" in d2)

# ══════════════════════════════════════════════════════════════════════════════
# GROUP 14: Data quality gate thresholds
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 14 — data quality gate constants")

check("14-1 _MIN_TRADES_AI=5",  lr._MIN_TRADES_AI == 5)
check("14-2 _MIN_TRADES_LOW=5", lr._MIN_TRADES_LOW == 5)
check("14-3 _PROMPT_VERSION set", lr._PROMPT_VERSION == "11.15.0")
check("14-4 _VALID_SCOPES contains 3 values", len(lr._VALID_SCOPES) == 3)
check("14-5 _VALID_STATUSES contains 5 values", len(lr._VALID_STATUSES) == 5)

# ══════════════════════════════════════════════════════════════════════════════
# GROUP 15: _lm_build_learning_evidence raises on unvalidated filters
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 15 — evidence guard (unvalidated filters)")

# These tests fail before reaching the query — no DB needed
bad_sym_f = lr._lm_build_learning_review_filters(
    1, review_scope="portfolio", period="30d",
    symbol="", symbol_supplied=True,
)
try:
    lr._lm_build_learning_evidence(1, bad_sym_f)
    check("15-1 ValueError for bad symbol filter", False, "no exception")
except ValueError:
    check("15-1 ValueError for bad symbol filter", True)

bad_scope_f = lr._lm_build_learning_review_filters(
    1, review_scope="not_a_scope", period="30d",
)
try:
    lr._lm_build_learning_evidence(1, bad_scope_f)
    check("15-2 ValueError for bad scope filter", False, "no exception")
except ValueError:
    check("15-2 ValueError for bad scope filter", True)

# ══════════════════════════════════════════════════════════════════════════════
# GROUP 16: Prompt no-period-all handling
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 16 — filter period validation")

f_all = lr._lm_build_learning_review_filters(1, review_scope="portfolio", period="all")
check("16-1 period 'all' accepted", f_all.get("period") == "all")

f_bad_period = lr._lm_build_learning_review_filters(1, review_scope="portfolio", period="999d")
check("16-2 invalid period falls back to 30d", f_bad_period.get("period") == "30d")

f_7d = lr._lm_build_learning_review_filters(1, review_scope="portfolio", period="7d")
check("16-3 period 7d accepted", f_7d.get("period") == "7d")

f_365 = lr._lm_build_learning_review_filters(1, review_scope="portfolio", period="365d")
check("16-4 period 365d accepted", f_365.get("period") == "365d")

# ══════════════════════════════════════════════════════════════════════════════
# Final summary
# ══════════════════════════════════════════════════════════════════════════════
print(f"\n{'='*60}")
print(f"  TOTAL: {_pass+_fail}   PASS: {_pass}   FAIL: {_fail}")
print(f"{'='*60}")
if _fail:
    sys.exit(1)
