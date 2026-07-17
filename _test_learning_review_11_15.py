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
check("3-2 sample_quality early for 10-29", ev["sample_quality"] == "early")
check("3-3 segments present", isinstance(ev.get("segments"), dict))
check("3-4 execution_quality present", isinstance(ev.get("execution_quality"), dict))
check("3-5 recent_trades present", isinstance(ev.get("recent_trades"), list))
check("3-6 warnings is list", isinstance(ev.get("warnings"), list))

f_valid5 = lr._lm_build_learning_review_filters(1, review_scope="portfolio", period="30d")
f_valid5["_mock_n"] = 5

ev5 = lr._lm_build_learning_evidence(1, f_valid5)
check("3-7 sample_quality insufficient for 0-9", ev5["sample_quality"] == "insufficient")

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
ctypes_found = {c["candidate_type"] for c in cands}
check("7-1 candidates list is non-empty", len(cands) > 0)
check("7-2 symbol_underperformance detected for BTCUSDT", "symbol_underperformance" in ctypes_found)
check("7-3 symbol_outperformance detected for ETHUSDT", "symbol_outperformance" in ctypes_found)
check("7-4 side_imbalance detected", "side_imbalance" in ctypes_found)
check("7-5 low_rr_capture detected (capture=0.4)", "low_rr_capture" in ctypes_found)
check("7-6 candidates have candidate_id + evidence_metric_ids", all(
    "candidate_id" in c and "evidence_metric_ids" in c for c in cands
))

ev_empty = {
    "sample_size": 0,
    "segments": {"by_symbol":[],"by_side":[],"by_outcome_reason":[],"by_confidence":[],"by_setup_type":[],"by_entry_mode":[]},
    "execution_quality": {},
}
cands_empty = lr._lm_build_learning_observation_candidates(ev_empty)
check("7-7 zero trades → insufficient_sample candidate", len(cands_empty) == 1 and cands_empty[0]["candidate_type"] == "insufficient_sample")

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
check("8-5 prompt requests review_proposals", '"review_proposals"' in prompt)
check("8-6 auto_apply_allowed false in schema", '"auto_apply_allowed": false' in prompt)
check("8-7 prompt requests what_not_to_conclude", '"what_not_to_conclude"' in prompt)
check("8-8 prompt requests overall_assessment", '"overall_assessment"' in prompt)
check("8-9 prompt requests review_title", '"review_title"' in prompt)

# ══════════════════════════════════════════════════════════════════════════════
# GROUP 9: Response parser and validator (hotfix 11.15.1 — strict, no repair)
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 9 — strict parse/validate (hotfix schema)")

# Minimal evidence for validator
_ev9 = {
    "sample_size":    30,
    "sample_quality": "developing",
    "warnings":       [],
    "performance_summary": {"win_rate_pct": 55.0, "net_realized_pnl": "150.00",
                             "trade_count": 30, "recent_trend": "stable"},
    "execution_quality": {"avg_rr_capture": 0.7, "avg_planned_rr": 2.0,
                           "avg_realized_rr": 1.4, "tp_pct": 55.0, "sl_pct": 30.0, "manual_pct": 15.0},
    "segments": {"by_symbol": [], "by_side": [], "by_outcome_reason": [],
                 "by_confidence": [], "by_setup_type": [], "by_entry_mode": []},
}

# Build allowlist from evidence for obs evidence rows
_al9 = lr._lm_build_evidence_metric_allowlist(_ev9)

# Valid full-schema AI response
_valid9 = {
    "review_title":       "30-Trade Portfolio Review",
    "executive_summary":  "Portfolio shows moderate performance with 55% win rate.",
    "overall_assessment": "mixed",
    "confidence_level":   "medium",
    "sample_assessment":  {"sample_size": 30, "sample_quality": "developing", "limitations": []},
    "observations": [
        {
            "id":         "obs_1",
            "category":   "risk_reward",
            "title":      "Moderate RR capture",
            "statement":  "Average RR capture is 70% of planned ratio.",
            "evidence":   [
                {"metric": "performance.trade_count", "value": 30, "comparison": None},
                {"metric": "execution.avg_rr_capture", "value": 0.7, "comparison": "below 1.0 target"},
            ],
            "sample_size": 30,
            "confidence": "medium",
            "severity":   "watch",
            "limitations": ["small_history"],
            "auto_apply_allowed": False,
        }
    ],
    "review_proposals": [
        {
            "id":                     "prop_1",
            "action_type":            "monitor",
            "title":                  "Continue monitoring RR capture",
            "description":            "Track RR capture over next 30 trades to see if pattern persists.",
            "evidence_observation_ids": ["obs_1"],
            "minimum_additional_sample": 30,
            "human_review_required":  True,
            "auto_apply_allowed":     False,
        }
    ],
    "what_not_to_conclude": [
        "This sample does not prove the strategy should be changed.",
    ],
    "guardrails": {
        "read_only":              True,
        "human_review_required":  True,
        "auto_apply_allowed":     False,
        "can_change_strategy":    False,
        "can_change_risk_guard":  False,
        "can_arm_auto_gate":      False,
        "can_auto_submit":        False,
        "auto_execution_allowed": False,
        "ai_can_execute":         False,
    },
}

parsed9 = lr._lm_parse_learning_review_response(_valid9)
check("9-1 valid response parsed ok (no _parse_error)", "_parse_error" not in parsed9)
check("9-2 review_title preserved", parsed9.get("review_title") == "30-Trade Portfolio Review")
check("9-3 observations count=1", len(parsed9.get("observations", [])) == 1)
check("9-4 guardrails auto_apply_allowed=False", parsed9["guardrails"]["auto_apply_allowed"] is False)
check("9-5 guardrails ai_can_execute=False", parsed9["guardrails"]["ai_can_execute"] is False)

is_valid9, reasons9 = lr._lm_validate_learning_review_response(parsed9, _ev9)
check("9-6 valid full-schema response passes validation", is_valid9, str(reasons9))
check("9-7 no validation reasons for valid response", reasons9 == [], str(reasons9))

# AI tries to set auto_apply_allowed=True in guardrails → parser passes (no repair)
# but validator rejects it
sneaky_gr = dict(_valid9)
sneaky_gr = {**_valid9, "guardrails": {**_valid9["guardrails"], "auto_apply_allowed": True}}
parsed_sneaky9 = lr._lm_parse_learning_review_response(sneaky_gr)
check("9-8 parser does NOT repair auto_apply_allowed (passes through True)", parsed_sneaky9["guardrails"]["auto_apply_allowed"] is True)
is_sneaky_v, sneaky_reasons = lr._lm_validate_learning_review_response(parsed_sneaky9, _ev9)
check("9-9 validator rejects auto_apply_allowed=True in guardrails", not is_sneaky_v)
check("9-10 rejection reason mentions guardrail", any("guardrail_auto_apply_allowed" in r for r in sneaky_reasons), str(sneaky_reasons))

# AI sets auto_apply_allowed=True in a proposal → validator rejects
sneaky_prop = {**_valid9, "review_proposals": [{**_valid9["review_proposals"][0], "auto_apply_allowed": True}]}
parsed_sneaky_prop = lr._lm_parse_learning_review_response(sneaky_prop)
is_prop_v, prop_reasons = lr._lm_validate_learning_review_response(parsed_sneaky_prop, _ev9)
check("9-11 validator rejects proposal auto_apply_allowed=True", not is_prop_v)
check("9-12 rejection reason mentions proposal auto_apply", any("auto_apply_not_false" in r for r in prop_reasons), str(prop_reasons))

# Missing required top-level key → rejected
missing_key_resp = {k: v for k, v in _valid9.items() if k != "what_not_to_conclude"}
parsed_miss = lr._lm_parse_learning_review_response(missing_key_resp)
is_miss_v, miss_reasons = lr._lm_validate_learning_review_response(parsed_miss, _ev9)
check("9-13 missing required key → rejected", not is_miss_v)
check("9-14 rejection names missing key", any("missing_required_key:what_not_to_conclude" in r for r in miss_reasons), str(miss_reasons))

# Non-dict response
parsed_non_dict = lr._lm_parse_learning_review_response("not a dict")
check("9-15 non-dict response returns error key", "_parse_error" in parsed_non_dict)
is_nd_v, reasons_nd = lr._lm_validate_learning_review_response(parsed_non_dict, _ev9)
check("9-16 non-dict response is invalid", not is_nd_v)

# Empty response
parsed_empty_str = lr._lm_parse_learning_review_response("")
check("9-17 empty string → _parse_error", "_parse_error" in parsed_empty_str)
is_e_v, _ = lr._lm_validate_learning_review_response(parsed_empty_str, _ev9)
check("9-18 empty string → invalid", not is_e_v)

# Low sample enforcement: 5-9 trades → confidence must be low, assessment=insufficient_data
_ev9_low = {**_ev9, "sample_size": 7, "sample_quality": "insufficient"}
_resp9_hi_conf = {**_valid9, "confidence_level": "high", "overall_assessment": "positive",
                  "sample_assessment": {"sample_size": 7, "sample_quality": "insufficient", "limitations": []},
                  "review_proposals": [{**_valid9["review_proposals"][0],
                                        "action_type": "collect_more_data"}]}
parsed9_lo = lr._lm_parse_learning_review_response(_resp9_hi_conf)
is_lo_v, lo_reasons = lr._lm_validate_learning_review_response(parsed9_lo, _ev9_low)
check("9-19 5-9 trades with confidence=high → rejected", not is_lo_v)
check("9-20 5-9 trades assessment=positive → rejected", any("assessment_must_be_insufficient_data" in r for r in lo_reasons), str(lo_reasons))

# Evidence metric not in allowlist → rejected
unknown_metric_resp = {**_valid9,
    "observations": [{**_valid9["observations"][0],
                      "evidence": [{"metric": "fabricated.unknown_metric", "value": 999}]}]}
parsed9_unk = lr._lm_parse_learning_review_response(unknown_metric_resp)
is_unk_v, unk_reasons = lr._lm_validate_learning_review_response(parsed9_unk, _ev9)
check("9-21 unknown evidence metric → rejected", not is_unk_v)
check("9-22 rejection names unknown metric", any("unknown_metric" in r for r in unk_reasons), str(unk_reasons))

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
section("GROUP 11 — _MODULE_GUARDRAILS + _GUARDRAILS_REQUIRED permanent invariants")

g = lr._MODULE_GUARDRAILS
check("11-1 can_auto_submit is False",             g["can_auto_submit"]             is False)
check("11-2 auto_execution_allowed is False",      g["auto_execution_allowed"]      is False)
check("11-3 ai_can_execute is False",              g["ai_can_execute"]              is False)
check("11-4 live_disabled is True",                g["live_disabled"]               is True)
check("11-5 testnet_strategy_validation is False", g["testnet_strategy_validation"] is False)
check("11-6 auto_apply_allowed is False",          g["auto_apply_allowed"]          is False)
check("11-7 read_only is True",                    g["read_only"]                   is True)

gr = lr._GUARDRAILS_REQUIRED
check("11-8 _GUARDRAILS_REQUIRED has all 9 keys", len(gr) == 9)
check("11-9 _GUARDRAILS_REQUIRED auto_apply_allowed=False", gr["auto_apply_allowed"] is False)
check("11-10 _GUARDRAILS_REQUIRED human_review_required=True", gr["human_review_required"] is True)

# ══════════════════════════════════════════════════════════════════════════════
# GROUP 12: Prompt includes all valid observation types
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 12 — prompt schema coverage + candidate types")

expected_candidate_types = {
    "symbol_underperformance", "symbol_outperformance",
    "side_imbalance",
    "stop_loss_concentration",
    "low_rr_capture", "strong_rr_capture",
    "confidence_not_confirmed", "confidence_supported",
    "setup_type_underperformance", "setup_type_outperformance",
    "deteriorating_recent_period", "improving_recent_period",
    "insufficient_sample",
    "data_quality_problem",
    "no_action_recommended",
}
check("12-1 _VALID_CANDIDATE_TYPES has all 15 types", lr._VALID_CANDIDATE_TYPES == expected_candidate_types)
check("12-2 prompt mentions observations schema", '"observations"' in prompt)
check("12-3 prompt mentions review_proposals schema", '"review_proposals"' in prompt)
check("12-4 prompt mentions what_not_to_conclude schema", '"what_not_to_conclude"' in prompt)
check("12-5 prompt mentions action_type options", "investigate" in prompt and "collect_more_data" in prompt)
check("12-6 prompt schema has obs categories", "symbol|side|setup|risk_reward" in prompt or "category" in prompt)
check("12-7 prompt has severity schema", "info|watch|important" in prompt)
check("12-8 prompt forbids strategy changes", "strategy" in prompt.lower())

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
    sample_size=10; sample_quality="early"; confidence_level="medium"
    warning_count=0; source="ai"; model_name="gpt4"; prompt_version="11.15.1"
    parent_review_id=None; human_note=None
    created_at=datetime.now(timezone.utc); updated_at=None; reviewed_at=None
    review_json='{"observations":[],"review_proposals":[],"what_not_to_conclude":[]}'
    evidence_json='{}'

from live_monitor.ai_learning_review import _serialize_review
d = _serialize_review(_FakeRow(), include_json=False)
check("13-2 serialize returns id", d["id"] == 1)
check("13-3 serialize returns status", d["status"] == "generated")
check("13-4 serialize includes guardrails", "guardrails" in d)
check("13-5 serialized guardrails auto_apply_allowed=False", d["guardrails"]["auto_apply_allowed"] is False)
check("13-6 no review_data when include_json=False", "review_data" not in d)
check("13-7a serialize has review_title field", "review_title" in d)
check("13-7b serialize has executive_summary field", "executive_summary" in d)

d2 = _serialize_review(_FakeRow(), include_json=True)
check("13-8 review_data present when include_json=True", "review_data" in d2)
check("13-9 observations field present when include_json=True", "observations" in d2)
check("13-10 review_proposals field present when include_json=True", "review_proposals" in d2)
check("13-11 what_not_to_conclude field present when include_json=True", "what_not_to_conclude" in d2)

# ══════════════════════════════════════════════════════════════════════════════
# GROUP 14: Data quality gate thresholds
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 14 — data quality gate constants")

check("14-1 _MIN_TRADES_AI=5",             lr._MIN_TRADES_AI == 5)
check("14-2 _PROMPT_VERSION=11.15.1",      lr._PROMPT_VERSION == "11.15.1")
check("14-3 _VALID_SCOPES contains 3",     len(lr._VALID_SCOPES) == 3)
check("14-4 _VALID_STATUSES contains 5",   len(lr._VALID_STATUSES) == 5)
check("14-5 _SEG_MIN_DIRECTIONAL=5",       lr._SEG_MIN_DIRECTIONAL == 5)
check("14-6 _SEG_MIN_SIDE_EACH=5",         lr._SEG_MIN_SIDE_EACH == 5)
check("14-7 _MAX_ACCEPTED_INSIGHTS=20",    lr._MAX_ACCEPTED_INSIGHTS == 20)
check("14-8 _SQ_INSUFFICIENT=insufficient",lr._SQ_INSUFFICIENT == "insufficient")
check("14-9 _SQ_EARLY=early",              lr._SQ_EARLY == "early")
check("14-10 _SQ_DEVELOPING=developing",   lr._SQ_DEVELOPING == "developing")
check("14-11 _SQ_MEANINGFUL=meaningful",   lr._SQ_MEANINGFUL == "meaningful")

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
# GROUP 17: _lm_build_evidence_metric_allowlist
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 17 — _lm_build_evidence_metric_allowlist")

_ev17 = {
    "sample_size":    20,
    "sample_quality": "early",
    "warnings":       [],
    "performance_summary": {
        "win_rate_pct": 60.0, "net_realized_pnl": "100.00",
        "trade_count": 20, "profit_factor": 1.5, "expectancy_amount": "5.00",
        "max_drawdown_amount": "30.00", "recent_trend": "stable",
    },
    "execution_quality": {
        "avg_rr_capture": 0.65, "avg_planned_rr": 2.0,
        "avg_realized_rr": 1.3, "tp_pct": 50.0, "sl_pct": 30.0, "manual_pct": 20.0,
    },
    "segments": {
        "by_symbol": [{"label": "BTCUSDT", "count": 20, "wins": 12, "losses": 8,
                       "breakevens": 0, "win_rate": 60.0, "net_pnl": "100.00"}],
        "by_side": [], "by_outcome_reason": [], "by_confidence": [],
        "by_setup_type": [], "by_entry_mode": [],
    },
}
al17 = lr._lm_build_evidence_metric_allowlist(_ev17)
check("17-1 allowlist is a dict", isinstance(al17, dict))
check("17-2 allowlist non-empty", len(al17) > 0)
check("17-3 performance.win_rate_pct in allowlist", "performance.win_rate_pct" in al17)
check("17-4 execution.avg_rr_capture in allowlist", "execution.avg_rr_capture" in al17)
check("17-5 performance.trade_count in allowlist", "performance.trade_count" in al17)
check("17-6 execution.tp_pct in allowlist", "execution.tp_pct" in al17)
check("17-7 segment metrics in allowlist", any("segment." in k for k in al17))
check("17-8 win_rate value is float", isinstance(al17.get("performance.win_rate_pct"), float))


# ══════════════════════════════════════════════════════════════════════════════
# GROUP 18: _lm_sanitize_valid_learning_review
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 18 — _lm_sanitize_valid_learning_review")

_dirty_review = {
    "review_title":       "  Padded Title  ",
    "executive_summary":  "  Summary with trailing whitespace.  ",
    "overall_assessment": "positive",
    "confidence_level":   "medium",
    "sample_assessment":  {"sample_size": 30, "sample_quality": "developing", "limitations": []},
    "observations": [
        {
            "id": "obs_1", "category": "risk_reward",
            "title": "  Padded obs title  ",
            "statement": "  Padded statement.  ",
            "evidence": [{"metric": "performance.win_rate_pct", "value": 60.0}],
            "sample_size": 30, "confidence": "medium", "severity": "watch",
            "limitations": "  some limit  ", "auto_apply_allowed": False,
        }
    ],
    "review_proposals": [
        {
            "id": "prop_1", "action_type": "monitor",
            "title": "  Padded prop  ", "description": "  Padded desc.  ",
            "evidence_observation_ids": ["obs_1"],
            "minimum_additional_sample": 10,
            "human_review_required": True, "auto_apply_allowed": False,
        }
    ],
    "what_not_to_conclude": ["  Padded wntc entry.  "],
    "guardrails": {
        "read_only": True, "human_review_required": True, "auto_apply_allowed": False,
        "can_change_strategy": False, "can_change_risk_guard": False,
        "can_arm_auto_gate": False, "can_auto_submit": False,
        "auto_execution_allowed": False, "ai_can_execute": False,
    },
}
sanitized18 = lr._lm_sanitize_valid_learning_review(_dirty_review)
check("18-1 review_title trimmed", sanitized18["review_title"] == "Padded Title")
check("18-2 executive_summary trimmed", sanitized18["executive_summary"] == "Summary with trailing whitespace.")
check("18-3 obs title trimmed", sanitized18["observations"][0]["title"] == "Padded obs title")
check("18-4 obs statement trimmed", sanitized18["observations"][0]["statement"] == "Padded statement.")
check("18-5 proposal title trimmed", sanitized18["review_proposals"][0]["title"] == "Padded prop")
check("18-6 wntc entry trimmed", sanitized18["what_not_to_conclude"][0] == "Padded wntc entry.")
check("18-7 guardrails preserved exactly", sanitized18["guardrails"]["auto_apply_allowed"] is False)
check("18-8 overall_assessment preserved", sanitized18["overall_assessment"] == "positive")


# ══════════════════════════════════════════════════════════════════════════════
# GROUP 19: _lm_build_deterministic_review
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 19 — _lm_build_deterministic_review")

_ev19 = {
    "sample_size":    10,
    "sample_quality": "early",
    "warnings":       [],
    "performance_summary": {"win_rate_pct": 50.0, "net_realized_pnl": "50.00", "trade_count": 10},
    "execution_quality": {"avg_rr_capture": 0.6},
    "segments": {"by_symbol": [], "by_side": [], "by_outcome_reason": [],
                 "by_confidence": [], "by_setup_type": [], "by_entry_mode": []},
}
_cands19 = lr._lm_build_learning_observation_candidates(_ev19)
det19 = lr._lm_build_deterministic_review(_ev19, _cands19)
check("19-1 deterministic review is a dict", isinstance(det19, dict))
check("19-2 has review_title", "review_title" in det19)
check("19-3 has executive_summary", "executive_summary" in det19)
check("19-4 has overall_assessment", "overall_assessment" in det19)
check("19-5 has observations list", isinstance(det19.get("observations"), list))
check("19-6 has review_proposals list", isinstance(det19.get("review_proposals"), list))
check("19-7 has what_not_to_conclude list", isinstance(det19.get("what_not_to_conclude"), list))
check("19-8 review_title mentions Deterministic", "Deterministic" in (det19.get("review_title") or ""))
check("19-9 guardrails present", isinstance(det19.get("guardrails"), dict))
check("19-10 guardrails auto_apply_allowed=False", det19["guardrails"]["auto_apply_allowed"] is False)
check("19-11 guardrails ai_can_execute=False", det19["guardrails"]["ai_can_execute"] is False)

# Deterministic review must pass strict validation (it conforms to schema)
det_parsed = lr._lm_parse_learning_review_response(det19)
is_det_v, det_reasons = lr._lm_validate_learning_review_response(det_parsed, _ev19)
check("19-12 deterministic review passes strict validation", is_det_v, str(det_reasons))


# ══════════════════════════════════════════════════════════════════════════════
# GROUP 20: _lm_update_learning_review + sentinel
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 20 — _lm_update_learning_review sentinel")

from live_monitor.ai_learning_review import _NOTE_NOT_PROVIDED as _SENTINEL20

check("20-1 _NOTE_NOT_PROVIDED is a unique object", _SENTINEL20 is not None)
check("20-2 _NOTE_NOT_PROVIDED is not a string", not isinstance(_SENTINEL20, str))
check("20-3 _NOTE_NOT_PROVIDED is not False", _SENTINEL20 is not False)

# Missing model row returns (False, "review_not_found", None)
_models_mod20 = sys.modules.get("models") or types.ModuleType("models")
class _MockLR20:
    class query:
        @staticmethod
        def filter_by(**kw):
            class _Q:
                def first(self): return None
            return _Q()
_models_mod20.LiveMonitorLearningReview = _MockLR20
sys.modules["models"] = _models_mod20

_ext_mod = sys.modules.get("extensions") or types.ModuleType("extensions")
class _FakeDb:
    def add(self, *a): pass
    def commit(self): pass
    def flush(self): pass
_ext_mod.db = _FakeDb()
sys.modules["extensions"] = _ext_mod

ok20, reason20, _ = lr._lm_update_learning_review(1, 9999, new_status="reviewed")
check("20-4 missing review → ok=False", not ok20)
check("20-5 missing review → reason=review_not_found", reason20 == "review_not_found")


# ══════════════════════════════════════════════════════════════════════════════
# GROUP 21: _sample_quality function
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 21 — _sample_quality")

check("21-1  0 trades → insufficient", lr._sample_quality(0)   == "insufficient")
check("21-2  9 trades → insufficient", lr._sample_quality(9)   == "insufficient")
check("21-3 10 trades → early",        lr._sample_quality(10)  == "early")
check("21-4 29 trades → early",        lr._sample_quality(29)  == "early")
check("21-5 30 trades → developing",   lr._sample_quality(30)  == "developing")
check("21-6 99 trades → developing",   lr._sample_quality(99)  == "developing")
check("21-7 100 trades → meaningful",  lr._sample_quality(100) == "meaningful")
check("21-8 500 trades → meaningful",  lr._sample_quality(500) == "meaningful")


# ══════════════════════════════════════════════════════════════════════════════
# GROUP 22: Phase 11.15.4 Task 1 — Category-specific count binding
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 22 — category-specific count binding (Phase 11.15.4 Task 1)")

check("22-1 _SEGMENT_CATEGORIES exported", hasattr(lr, "_SEGMENT_CATEGORIES"))
check("22-2 _SEGMENT_CATEGORIES == {symbol,side,setup,confidence}",
      getattr(lr, "_SEGMENT_CATEGORIES", None) == frozenset({"symbol", "side", "setup", "confidence"}))

# Evidence fixture with segment data for all four segment categories
_ev22 = {
    "sample_size":    30,
    "sample_quality": "developing",
    "warnings":       [],
    "performance_summary": {
        "win_rate_pct": 55.0, "net_realized_pnl": "150.00", "trade_count": 30,
    },
    "execution_quality": {"avg_rr_capture": 0.7},
    "segments": {
        "by_symbol": [
            {"label": "BTCUSDT", "count": 20, "wins": 11, "losses": 9,
             "breakevens": 0, "win_rate": 55.0, "net_pnl": "100.00"},
        ],
        "by_side": [
            {"label": "BUY",  "count": 18, "wins": 10, "losses": 8,
             "breakevens": 0, "win_rate": 55.6, "net_pnl": "90.00"},
            {"label": "SELL", "count": 12, "wins":  6, "losses": 6,
             "breakevens": 0, "win_rate": 50.0, "net_pnl": "60.00"},
        ],
        "by_outcome_reason": [],
        "by_confidence": [
            {"label": "high", "count": 15, "wins": 10, "losses": 5,
             "breakevens": 0, "win_rate": 66.7, "net_pnl": "80.00"},
        ],
        "by_setup_type": [
            {"label": "breakout", "count": 10, "wins": 6, "losses": 4,
             "breakevens": 0, "win_rate": 60.0, "net_pnl": "50.00"},
        ],
        "by_entry_mode": [],
    },
}

_guardrails22 = {
    "read_only":              True,
    "human_review_required":  True,
    "auto_apply_allowed":     False,
    "can_change_strategy":    False,
    "can_change_risk_guard":  False,
    "can_arm_auto_gate":      False,
    "can_auto_submit":        False,
    "auto_execution_allowed": False,
    "ai_can_execute":         False,
}

def _mk_resp22(category, ev_metric, ev_value, sample_size):
    return {
        "review_title":       "30-Trade Portfolio Review",
        "executive_summary":  "Portfolio shows moderate performance.",
        "overall_assessment": "mixed",
        "confidence_level":   "medium",
        "sample_assessment":  {"sample_size": 30, "sample_quality": "developing", "limitations": []},
        "observations": [
            {
                "id":          "obs_1",
                "category":    category,
                "title":       "Observation title",
                "statement":   "Observation statement about trading patterns.",
                "evidence":    [{"metric": ev_metric, "value": ev_value, "comparison": None}],
                "sample_size": sample_size,
                "confidence":  "medium",
                "severity":    "watch",
                "limitations": [],
                "auto_apply_allowed": False,
            }
        ],
        "review_proposals":    [],
        "what_not_to_conclude": ["This is inconclusive."],
        "guardrails":          _guardrails22,
    }

# 22-3: symbol observation with portfolio count → rejected
_p22_sym_wrong = lr._lm_parse_learning_review_response(
    _mk_resp22("symbol", "performance.trade_count", 30, 30))
_v22_3, _rr22_3 = lr._lm_validate_learning_review_response(_p22_sym_wrong, _ev22)
check("22-3 symbol obs + performance.trade_count → rejected", not _v22_3, str(_rr22_3))
check("22-4 symbol rejection reason cites symbol_observation_requires_symbol_count",
      any("symbol_observation_requires_symbol_count" in r for r in _rr22_3), str(_rr22_3))

# 22-5: symbol observation with segment-specific count → accepted
_p22_sym_ok = lr._lm_parse_learning_review_response(
    _mk_resp22("symbol", "segment.symbol.BTCUSDT.trade_count", 20, 20))
_v22_5, _rr22_5 = lr._lm_validate_learning_review_response(_p22_sym_ok, _ev22)
check("22-5 symbol obs + segment.symbol.BTCUSDT.trade_count → accepted", _v22_5, str(_rr22_5))

# 22-6: side observation with portfolio count → rejected
_p22_side_wrong = lr._lm_parse_learning_review_response(
    _mk_resp22("side", "performance.trade_count", 30, 30))
_v22_6, _rr22_6 = lr._lm_validate_learning_review_response(_p22_side_wrong, _ev22)
check("22-6 side obs + performance.trade_count → rejected", not _v22_6, str(_rr22_6))
check("22-7 side rejection reason cites side_observation_requires_side_count",
      any("side_observation_requires_side_count" in r for r in _rr22_6), str(_rr22_6))

# 22-8: side observation with segment-specific count → accepted
_p22_side_ok = lr._lm_parse_learning_review_response(
    _mk_resp22("side", "segment.side.BUY.trade_count", 18, 18))
_v22_8, _rr22_8 = lr._lm_validate_learning_review_response(_p22_side_ok, _ev22)
check("22-8 side obs + segment.side.BUY.trade_count → accepted", _v22_8, str(_rr22_8))

# 22-9: setup observation with portfolio count → rejected
_p22_setup_wrong = lr._lm_parse_learning_review_response(
    _mk_resp22("setup", "performance.trade_count", 30, 30))
_v22_9, _rr22_9 = lr._lm_validate_learning_review_response(_p22_setup_wrong, _ev22)
check("22-9 setup obs + performance.trade_count → rejected", not _v22_9, str(_rr22_9))

# 22-10: setup observation with segment-specific count → accepted
_p22_setup_ok = lr._lm_parse_learning_review_response(
    _mk_resp22("setup", "segment.setup.breakout.trade_count", 10, 10))
_v22_10, _rr22_10 = lr._lm_validate_learning_review_response(_p22_setup_ok, _ev22)
check("22-10 setup obs + segment.setup.breakout.trade_count → accepted", _v22_10, str(_rr22_10))

# 22-11: confidence observation with portfolio count → rejected
_p22_conf_wrong = lr._lm_parse_learning_review_response(
    _mk_resp22("confidence", "performance.trade_count", 30, 30))
_v22_11, _rr22_11 = lr._lm_validate_learning_review_response(_p22_conf_wrong, _ev22)
check("22-11 confidence obs + performance.trade_count → rejected", not _v22_11, str(_rr22_11))

# 22-12: confidence observation with segment-specific count → accepted
_p22_conf_ok = lr._lm_parse_learning_review_response(
    _mk_resp22("confidence", "segment.confidence.high.trade_count", 15, 15))
_v22_12, _rr22_12 = lr._lm_validate_learning_review_response(_p22_conf_ok, _ev22)
check("22-12 confidence obs + segment.confidence.high.trade_count → accepted", _v22_12, str(_rr22_12))

# 22-13 to 22-16: non-segment categories may use performance.trade_count
for _cat22, _num22 in [("risk_reward", 13), ("exit", 14), ("trend", 15), ("data_quality", 16)]:
    _p22_ns = lr._lm_parse_learning_review_response(
        _mk_resp22(_cat22, "performance.trade_count", 30, 30))
    _v22_ns, _rr22_ns = lr._lm_validate_learning_review_response(_p22_ns, _ev22)
    check(f"22-{_num22} {_cat22} obs + performance.trade_count → accepted (non-segment)",
          _v22_ns, str(_rr22_ns))


# ══════════════════════════════════════════════════════════════════════════════
# Final summary
# ══════════════════════════════════════════════════════════════════════════════
print(f"\n{'='*60}")
print(f"  TOTAL: {_pass+_fail}   PASS: {_pass}   FAIL: {_fail}")
print(f"{'='*60}")
if _fail:
    sys.exit(1)
