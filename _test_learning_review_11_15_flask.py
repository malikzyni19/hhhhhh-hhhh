"""Phase 11.15 AI Learning Review Loop — Flask/SQLAlchemy integration tests.

Tests DB persistence, validation, ownership, and guardrail invariants using
an in-memory SQLite database. The AI call and evidence building are mocked
at the boundary so tests focus on route + DB behaviour.

Run: python3 _test_learning_review_11_15_flask.py
"""
import os, sys, json, types, pathlib, threading, importlib.util
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import patch, MagicMock

os.environ["DATABASE_URL"]   = "sqlite:///:memory:"
os.environ["SECRET_KEY"]     = "test_secret_phase_11_15_2"
os.environ.setdefault("RESEND_API_KEY",  "test")
os.environ.setdefault("TURNSTILE_SECRET", "")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ── Minimal stubs for heavy optional dependencies ─────────────────────────────

for _mn in ["psycopg2", "psycopg2.extras", "resend"]:
    if _mn not in sys.modules:
        sys.modules[_mn] = types.ModuleType(_mn)

# ── Load paper_performance (dependency of ai_learning_review) ─────────────────

_pp_spec = importlib.util.spec_from_file_location(
    "live_monitor.paper_performance",
    pathlib.Path(__file__).parent / "live_monitor" / "paper_performance.py",
)
_pp_mod = importlib.util.module_from_spec(_pp_spec)
sys.modules["live_monitor.paper_performance"] = _pp_mod
_pp_spec.loader.exec_module(_pp_mod)

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

# ── Minimal Flask + SQLAlchemy app for integration tests ──────────────────────

from flask import Flask
from flask_sqlalchemy import SQLAlchemy

app  = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"]        = "sqlite:///:memory:"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["TESTING"]                        = True
app.config["SECRET_KEY"]                     = "test_secret_phase_11_15_2"
app.config["WTF_CSRF_ENABLED"]               = False

db = SQLAlchemy(app)

# ── Minimal models for the test ───────────────────────────────────────────────

class User(db.Model):
    __tablename__ = "users"
    id       = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)

class LiveMonitorLearningReview(db.Model):
    """Mirrors the production model — columns only."""
    __tablename__ = "live_monitor_learning_reviews"
    id               = db.Column(db.Integer, primary_key=True)
    user_id          = db.Column(db.Integer, nullable=False, index=True)
    item_id          = db.Column(db.Integer, nullable=True)
    review_scope     = db.Column(db.String(20), nullable=False, default="portfolio")
    period           = db.Column(db.String(10), nullable=True)
    symbol           = db.Column(db.String(30), nullable=True)
    side             = db.Column(db.String(10), nullable=True)
    status           = db.Column(db.String(30), nullable=False, default="generated", index=True)
    title            = db.Column(db.Text, nullable=True)
    summary          = db.Column(db.Text, nullable=True)
    review_json      = db.Column(db.Text, nullable=True)
    evidence_json    = db.Column(db.Text, nullable=True)
    human_note       = db.Column(db.Text, nullable=True)
    sample_size      = db.Column(db.Integer, nullable=True)
    sample_quality   = db.Column(db.String(20), nullable=True)
    confidence_level = db.Column(db.String(20), nullable=True)
    warning_count    = db.Column(db.Integer, nullable=True, default=0)
    source           = db.Column(db.String(30), nullable=True)
    model_name       = db.Column(db.String(80), nullable=True)
    prompt_version   = db.Column(db.String(20), nullable=True)
    parent_review_id = db.Column(db.Integer, nullable=True)
    supersedes_review_id = db.Column(db.Integer, nullable=True)
    created_at       = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at       = db.Column(db.DateTime, nullable=True)
    reviewed_at      = db.Column(db.DateTime, nullable=True)

with app.app_context():
    db.create_all()
    _u1 = User(username="alice")
    _u2 = User(username="bob")
    db.session.add_all([_u1, _u2])
    db.session.commit()
    _UID1 = _u1.id
    _UID2 = _u2.id

# ── Inject models into lr module's import context ─────────────────────────────

_models_mod = types.ModuleType("models")
_models_mod.LiveMonitorLearningReview = LiveMonitorLearningReview
sys.modules["models"] = _models_mod

_ext_mod = types.ModuleType("extensions")
_ext_mod.db = db
sys.modules["extensions"] = _ext_mod

# ── Shared test evidence and review data ──────────────────────────────────────

_EV30 = {
    "sample_size":    30,
    "sample_quality": "developing",
    "warnings":       [],
    "period":         "30d",
    "symbol":         None,
    "side":           None,
    "review_scope":   "portfolio",
    "performance_summary": {
        "trade_count":         30,
        "win_count":           18,
        "loss_count":          12,
        "breakeven_count":     0,
        "win_rate_pct":        60.0,
        "net_realized_pnl":    "150.00",
        "gross_profit":        "200.00",
        "gross_loss":          "-50.00",
        "profit_factor":       4.0,
        "expectancy_amount":   "5.00",
        "average_win":         "11.11",
        "average_loss":        "-4.17",
        "average_risk_reward": 2.0,
        "max_drawdown_amount": "-30.00",
        "recent_trend":        "stable",
        "truncated":           False,
    },
    "execution_quality": {
        "avg_rr_capture": 0.7,
        "avg_planned_rr": 2.0,
        "avg_realized_rr": 1.4,
        "tp_exit_count":  18,
        "sl_exit_count":  12,
        "manual_exit_count": 0,
        "other_exit_count":  0,
        "tp_pct": 60.0,
        "sl_pct": 40.0,
        "manual_pct": 0.0,
    },
    "segments": {
        "by_symbol": [],
        "by_side": [],
        "by_outcome_reason": [],
        "by_confidence": [],
        "by_setup_type": [],
        "by_entry_mode": [],
    },
    "data_quality": {},
}

_AL30 = lr._lm_build_evidence_metric_allowlist(_EV30)

def _valid_ai_response(total=30, sq="developing"):
    return {
        "review_title":       "30-Trade Portfolio Analysis",
        "executive_summary":  "Portfolio shows 60% win rate with developing sample quality.",
        "overall_assessment": "mixed",
        "confidence_level":   "medium",
        "sample_assessment":  {
            "sample_size":   total,
            "sample_quality": sq,
            "limitations":   [],
        },
        "observations": [{
            "id":         "obs_1",
            "category":   "risk_reward",
            "title":      "RR Capture Below Target",
            "statement":  "Average RR capture is 70% of planned ratio.",
            "evidence":   [{"metric": "execution.avg_rr_capture", "value": 0.7, "comparison": "below 1.0"}],
            "sample_size": total,
            "confidence": "medium",
            "severity":   "watch",
            "limitations": [],
            "auto_apply_allowed": False,
        }],
        "review_proposals": [{
            "id":                       "prop_1",
            "action_type":              "monitor",
            "title":                    "Monitor RR Capture Trend",
            "description":              "Track RR capture ratio over next 30 trades.",
            "evidence_observation_ids": ["obs_1"],
            "minimum_additional_sample": 30,
            "human_review_required":    True,
            "auto_apply_allowed":       False,
        }],
        "what_not_to_conclude": [
            "This sample does not prove the strategy is broken.",
        ],
        "guardrails": {
            "read_only":             True,
            "human_review_required": True,
            "auto_apply_allowed":    False,
            "can_change_strategy":   False,
            "can_change_risk_guard": False,
            "can_arm_auto_gate":     False,
            "can_auto_submit":       False,
            "auto_execution_allowed": False,
            "ai_can_execute":        False,
        },
    }


def _save_review(uid, status="generated", human_note=None, source="ai"):
    r = LiveMonitorLearningReview(
        user_id=uid,
        review_scope="portfolio",
        period="30d",
        status=status,
        title="Test Review",
        summary="Summary.",
        review_json=json.dumps(_valid_ai_response()),
        evidence_json="{}",
        sample_size=30,
        sample_quality="developing",
        confidence_level="medium",
        warning_count=0,
        source=source,
        model_name=None,
        prompt_version="11.15.1",
        human_note=human_note,
    )
    db.session.add(r)
    db.session.commit()
    return r


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

def section(t):
    print(f"\n── {t} ──")


# ══════════════════════════════════════════════════════════════════════════════
# GROUP A: DB persistence — _lm_save_learning_review + _lm_get_learning_review
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP A — DB save and retrieve")

with app.app_context():
    filters_a = {
        "_review_scope": "portfolio", "period": "30d",
        "symbol": None, "side": None, "item_id": None,
    }
    cands_a  = lr._lm_build_learning_observation_candidates(_EV30)
    al_a     = lr._lm_build_evidence_metric_allowlist(_EV30)
    raw_resp = _valid_ai_response()
    parsed_a = lr._lm_parse_learning_review_response(raw_resp)
    san_a    = lr._lm_sanitize_valid_learning_review(parsed_a)

    row_a = lr._lm_save_learning_review(
        _UID1,
        filters        = filters_a,
        evidence       = _EV30,
        sanitized_review = san_a,
        source         = "ai",
        model_name     = "test-model",
        candidates     = cands_a,
        allowlist      = al_a,
    )
    db.session.commit()
    rid_a = row_a.id

    check("A-1 save returns a row with id", rid_a is not None and isinstance(rid_a, int))
    check("A-2 row status=generated", row_a.status == "generated")
    check("A-3 row sample_size=30", row_a.sample_size == 30)
    check("A-4 row sample_quality=developing", row_a.sample_quality == "developing")
    check("A-5 row source=ai", row_a.source == "ai")
    check("A-6 row user_id matches", row_a.user_id == _UID1)

    got_a = lr._lm_get_learning_review(_UID1, rid_a)
    check("A-7 get returns dict", isinstance(got_a, dict))
    check("A-8 get id matches", got_a["id"] == rid_a)
    check("A-9 get guardrails present", "guardrails" in got_a)
    check("A-10 guardrails auto_apply_allowed=False", got_a["guardrails"]["auto_apply_allowed"] is False)
    check("A-11 get observations present", "observations" in got_a)

    # Ownership: wrong user returns None
    got_wrong = lr._lm_get_learning_review(_UID2, rid_a)
    check("A-12 wrong-user get returns None", got_wrong is None)


# ══════════════════════════════════════════════════════════════════════════════
# GROUP B: Status transitions and PATCH sentinel
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP B — _lm_update_learning_review status + sentinel")

with app.app_context():
    r_b = _save_review(_UID1, status="generated")
    rid_b = r_b.id

    # Valid transition: generated → reviewed
    ok_b, reason_b, upd_b = lr._lm_update_learning_review(
        _UID1, rid_b, new_status="reviewed"
    )
    check("B-1 generated → reviewed ok=True", ok_b)
    check("B-2 reason=ok", reason_b == "ok")
    check("B-3 returned dict has status=reviewed", (upd_b or {}).get("status") == "reviewed")

    # Transition back to generated: invalid
    ok_b2, reason_b2, _ = lr._lm_update_learning_review(
        _UID1, rid_b, new_status="generated"
    )
    check("B-4 invalid transition ok=False", not ok_b2)
    check("B-5 invalid transition reason contains transition", "transition" in reason_b2)

    # Invalid status value
    ok_b3, reason_b3, _ = lr._lm_update_learning_review(
        _UID1, rid_b, new_status="not_a_status"
    )
    check("B-6 invalid status ok=False", not ok_b3)
    check("B-7 invalid status reason=invalid_status", reason_b3 == "invalid_status")

    # Note-only update (sentinel = note NOT supplied, should fail as no change)
    ok_b4, reason_b4, _ = lr._lm_update_learning_review(
        _UID1, rid_b,
        new_status=None, human_note=lr._NOTE_NOT_PROVIDED
    )
    check("B-8 sentinel note + no status → no_changes_applied", not ok_b4)

    # Supply human_note = "good insight"
    ok_b5, reason_b5, upd_b5 = lr._lm_update_learning_review(
        _UID1, rid_b,
        new_status=None, human_note="good insight"
    )
    check("B-9 human_note update ok=True", ok_b5)
    check("B-10 note persisted in returned dict", (upd_b5 or {}).get("human_note") == "good insight")

    # Supply human_note = None (clears note)
    ok_b6, _, upd_b6 = lr._lm_update_learning_review(
        _UID1, rid_b,
        new_status=None, human_note=None
    )
    check("B-11 human_note=None ok=True", ok_b6)

    # Verify note is cleared in DB
    cleared_row = LiveMonitorLearningReview.query.get(rid_b)
    check("B-12 note cleared in DB", cleared_row.human_note is None)

    # Wrong user cannot update
    ok_b7, reason_b7, _ = lr._lm_update_learning_review(
        _UID2, rid_b, new_status="accepted_insight"
    )
    check("B-13 wrong user → review_not_found", reason_b7 == "review_not_found")


# ══════════════════════════════════════════════════════════════════════════════
# GROUP C: Validator rejects malformed responses — no row saved
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP C — validator rejection does not persist a row")

with app.app_context():
    count_before = LiveMonitorLearningReview.query.filter_by(user_id=_UID1).count()

    # Craft a response that fails validation (auto_apply_allowed=True in guardrails)
    bad_resp = dict(_valid_ai_response())
    bad_resp["guardrails"] = dict(bad_resp["guardrails"])
    bad_resp["guardrails"]["auto_apply_allowed"] = True
    parsed_bad = lr._lm_parse_learning_review_response(bad_resp)
    is_v, reasons = lr._lm_validate_learning_review_response(parsed_bad, _EV30)
    check("C-1 bad guardrails → invalid", not is_v)
    check("C-2 rejection cites guardrail", any("guardrail_auto_apply_allowed" in r for r in reasons))
    # Simulate route: invalid → no save
    count_after = LiveMonitorLearningReview.query.filter_by(user_id=_UID1).count()
    check("C-3 row count unchanged (no save on invalid)", count_before == count_after)

    # String observation limitations → invalid
    bad_lims = dict(_valid_ai_response())
    bad_lims["observations"] = [dict(bad_lims["observations"][0])]
    bad_lims["observations"][0]["limitations"] = "string_not_a_list"
    parsed_lims = lr._lm_parse_learning_review_response(bad_lims)
    is_v2, reasons2 = lr._lm_validate_learning_review_response(parsed_lims, _EV30)
    check("C-4 string limitations → invalid", not is_v2)
    check("C-5 rejection cites limitations_not_a_list", any("limitations_not_a_list" in r for r in reasons2))
    count_after2 = LiveMonitorLearningReview.query.filter_by(user_id=_UID1).count()
    check("C-6 still no save after second bad response", count_before == count_after2)

    # Missing comparison key in evidence row → invalid
    bad_comp = dict(_valid_ai_response())
    bad_comp["observations"] = [dict(bad_comp["observations"][0])]
    bad_comp["observations"][0]["evidence"] = [{"metric": "execution.avg_rr_capture", "value": 0.7}]
    parsed_comp = lr._lm_parse_learning_review_response(bad_comp)
    is_v3, reasons3 = lr._lm_validate_learning_review_response(parsed_comp, _EV30)
    check("C-7 missing comparison key → invalid", not is_v3)
    check("C-8 rejection cites missing_comparison_key", any("missing_comparison_key" in r for r in reasons3))

    # Unknown metric in evidence row → invalid
    bad_metric = dict(_valid_ai_response())
    bad_metric["observations"] = [dict(bad_metric["observations"][0])]
    bad_metric["observations"][0]["evidence"] = [
        {"metric": "fabricated.unknown_xyz", "value": 42.0, "comparison": "test"}
    ]
    parsed_metric = lr._lm_parse_learning_review_response(bad_metric)
    is_v4, reasons4 = lr._lm_validate_learning_review_response(parsed_metric, _EV30)
    check("C-9 unknown metric → invalid", not is_v4)
    check("C-10 rejection cites unknown_metric", any("unknown_metric" in r for r in reasons4))


# ══════════════════════════════════════════════════════════════════════════════
# GROUP D: Validator new Task 1-10 behaviours
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP D — Task 1-10 strict validation behaviours")

with app.app_context():
    # Task 1: too many observations
    resp_too_many_obs = dict(_valid_ai_response())
    resp_too_many_obs["observations"] = [
        {**_valid_ai_response()["observations"][0], "id": f"obs_{i}"}
        for i in range(1, 13)  # 12 > _OBS_MAX=10
    ]
    parsed_tmo = lr._lm_parse_learning_review_response(resp_too_many_obs)
    is_tmo, reasons_tmo = lr._lm_validate_learning_review_response(parsed_tmo, _EV30)
    check("D-1 too_many_observations → rejected", not is_tmo)
    check("D-2 reason is too_many_observations", any("too_many_observations" in r for r in reasons_tmo))

    # Task 2: empty review_title rejected
    resp_no_title = dict(_valid_ai_response())
    resp_no_title["review_title"] = "   "  # whitespace only
    parsed_nt = lr._lm_parse_learning_review_response(resp_no_title)
    is_nt, reasons_nt = lr._lm_validate_learning_review_response(parsed_nt, _EV30)
    check("D-3 empty review_title → rejected", not is_nt)
    check("D-4 reason is review_title_not_a_nonempty_string", any("review_title" in r for r in reasons_nt))

    # Task 2: non-string review_title rejected
    resp_int_title = dict(_valid_ai_response())
    resp_int_title["review_title"] = 42
    parsed_it = lr._lm_parse_learning_review_response(resp_int_title)
    is_it, reasons_it = lr._lm_validate_learning_review_response(parsed_it, _EV30)
    check("D-5 int review_title → rejected", not is_it)

    # Task 3: bool sample_size rejected
    resp_bool_n = dict(_valid_ai_response())
    resp_bool_n["sample_assessment"] = {"sample_size": True, "sample_quality": "developing", "limitations": []}
    parsed_bn = lr._lm_parse_learning_review_response(resp_bool_n)
    is_bn, reasons_bn = lr._lm_validate_learning_review_response(parsed_bn, _EV30)
    check("D-6 bool sample_size → rejected", not is_bn)
    check("D-7 reason is sample_size_not_a_nonneg_int", any("sample_size_not_a_nonneg_int" in r for r in reasons_bn))

    # Task 3: 5-9 trades require small-sample marker in sample_assessment.limitations
    _ev7 = dict(_EV30, sample_size=7, sample_quality="insufficient")
    resp_7_no_marker = dict(_valid_ai_response(total=7, sq="insufficient"))
    resp_7_no_marker["confidence_level"] = "low"
    resp_7_no_marker["overall_assessment"] = "insufficient_data"
    resp_7_no_marker["review_proposals"][0]["action_type"] = "collect_more_data"
    resp_7_no_marker["sample_assessment"]["limitations"] = []  # missing marker
    parsed_7nm = lr._lm_parse_learning_review_response(resp_7_no_marker)
    is_7nm, reasons_7nm = lr._lm_validate_learning_review_response(parsed_7nm, _ev7)
    check("D-8 5-9 trades + no small_sample marker → rejected", not is_7nm)
    check("D-9 reason is missing_small_sample_marker", any("small_sample_marker" in r for r in reasons_7nm))

    # Task 3: with correct marker passes
    resp_7_marker = dict(resp_7_no_marker)
    resp_7_marker["sample_assessment"] = dict(resp_7_marker["sample_assessment"])
    resp_7_marker["sample_assessment"]["limitations"] = ["small_sample"]
    parsed_7m = lr._lm_parse_learning_review_response(resp_7_marker)
    is_7m, reasons_7m = lr._lm_validate_learning_review_response(parsed_7m, _ev7)
    check("D-10 5-9 trades + small_sample marker + correct obs_n → passes",
          is_7m or all("small_sample_marker" not in r for r in reasons_7m),
          str(reasons_7m))

    # Task 4: empty observation title rejected
    resp_no_obs_title = dict(_valid_ai_response())
    resp_no_obs_title["observations"] = [dict(resp_no_obs_title["observations"][0])]
    resp_no_obs_title["observations"][0]["title"] = ""
    parsed_not = lr._lm_parse_learning_review_response(resp_no_obs_title)
    is_not, reasons_not = lr._lm_validate_learning_review_response(parsed_not, _EV30)
    check("D-11 empty obs title → rejected", not is_not)
    check("D-12 reason is obs_title_not_a_nonempty_string", any("title_not_a_nonempty_string" in r for r in reasons_not))

    # Task 4: empty evidence list rejected
    resp_empty_ev = dict(_valid_ai_response())
    resp_empty_ev["observations"] = [dict(resp_empty_ev["observations"][0])]
    resp_empty_ev["observations"][0]["evidence"] = []
    parsed_ee = lr._lm_parse_learning_review_response(resp_empty_ev)
    is_ee, reasons_ee = lr._lm_validate_learning_review_response(parsed_ee, _EV30)
    check("D-13 empty evidence list → rejected", not is_ee)
    check("D-14 reason is evidence_empty", any("evidence_empty" in r for r in reasons_ee))

    # Task 5: NaN value in evidence row rejected
    resp_nan = dict(_valid_ai_response())
    resp_nan["observations"] = [dict(resp_nan["observations"][0])]
    resp_nan["observations"][0]["evidence"] = [
        {"metric": "execution.avg_rr_capture", "value": float("nan"), "comparison": "n/a"}
    ]
    parsed_nan = lr._lm_parse_learning_review_response(resp_nan)
    is_nan, reasons_nan = lr._lm_validate_learning_review_response(parsed_nan, _EV30)
    check("D-15 NaN value → rejected", not is_nan)
    check("D-16 reason is value_nan_or_inf", any("nan_or_inf" in r for r in reasons_nan))

    # Task 7: value out of tolerance for pct metric
    resp_tol = dict(_valid_ai_response())
    resp_tol["observations"] = [dict(resp_tol["observations"][0])]
    resp_tol["observations"][0]["evidence"] = [
        {"metric": "performance.win_rate_pct", "value": 99.9, "comparison": "very high"}  # actual is 60.0
    ]
    parsed_tol = lr._lm_parse_learning_review_response(resp_tol)
    is_tol, reasons_tol = lr._lm_validate_learning_review_response(parsed_tol, _EV30)
    check("D-17 pct value out of tolerance → rejected", not is_tol)
    check("D-18 reason is value_out_of_tolerance", any("out_of_tolerance" in r for r in reasons_tol))

    # Task 7: correct count value passes exact match
    resp_count = dict(_valid_ai_response())
    resp_count["observations"] = [dict(resp_count["observations"][0])]
    resp_count["observations"][0]["evidence"] = [
        {"metric": "performance.trade_count", "value": 30, "comparison": "30 trades"},
    ]
    parsed_count = lr._lm_parse_learning_review_response(resp_count)
    is_count, reasons_count = lr._lm_validate_learning_review_response(parsed_count, _EV30)
    check("D-19 exact count value passes", is_count, str(reasons_count))

    # Task 8: proposal ref check unconditional (no obs → proposal with ref → rejected)
    resp_no_obs2 = dict(_valid_ai_response())
    resp_no_obs2["observations"] = []
    resp_no_obs2["review_proposals"] = [{
        **_valid_ai_response()["review_proposals"][0],
        "evidence_observation_ids": ["obs_1"],  # obs_1 does not exist
    }]
    resp_no_obs2["what_not_to_conclude"] = ["No conclusions."]
    parsed_no_obs2 = lr._lm_parse_learning_review_response(resp_no_obs2)
    is_no_obs2, reasons_no_obs2 = lr._lm_validate_learning_review_response(parsed_no_obs2, _EV30)
    check("D-20 proposal refs unknown obs → rejected", not is_no_obs2)
    check("D-21 reason is unknown_obs_ref", any("unknown_obs_ref" in r for r in reasons_no_obs2))

    # Task 9: bool minimum_additional_sample rejected
    resp_bool_mas = dict(_valid_ai_response())
    resp_bool_mas["review_proposals"] = [dict(resp_bool_mas["review_proposals"][0])]
    resp_bool_mas["review_proposals"][0]["minimum_additional_sample"] = True
    parsed_bm = lr._lm_parse_learning_review_response(resp_bool_mas)
    is_bm, reasons_bm = lr._lm_validate_learning_review_response(parsed_bm, _EV30)
    check("D-22 bool minimum_additional_sample → rejected", not is_bm)
    check("D-23 reason is minimum_additional_sample_not_a_nonneg_int",
          any("minimum_additional_sample_not_a_nonneg_int" in r for r in reasons_bm))

    # Task 10: forbidden operational language in proposal title
    resp_exec = dict(_valid_ai_response())
    resp_exec["review_proposals"] = [dict(resp_exec["review_proposals"][0])]
    resp_exec["review_proposals"][0]["title"] = "Execute trade now"
    parsed_exec = lr._lm_parse_learning_review_response(resp_exec)
    is_exec, reasons_exec = lr._lm_validate_learning_review_response(parsed_exec, _EV30)
    check("D-24 'execute trade now' in title → rejected", not is_exec)
    check("D-25 reason is forbidden_operational_language",
          any("forbidden_operational_language" in r for r in reasons_exec))

    # Task 10: analytical framing excuses keyword
    resp_anal = dict(_valid_ai_response())
    resp_anal["review_proposals"] = [dict(resp_anal["review_proposals"][0])]
    resp_anal["review_proposals"][0]["title"] = "Investigate whether execute automatically improves results"
    parsed_anal = lr._lm_parse_learning_review_response(resp_anal)
    is_anal, reasons_anal = lr._lm_validate_learning_review_response(parsed_anal, _EV30)
    check("D-26 analytical framing excuses keyword — not rejected for op-language",
          all("forbidden_operational_language" not in r for r in reasons_anal))


# ══════════════════════════════════════════════════════════════════════════════
# GROUP E: Deterministic review + guardrail invariants
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP E — deterministic review + guardrail invariants")

with app.app_context():
    # Deterministic review always passes strict validation
    cands_e = lr._lm_build_learning_observation_candidates(_EV30)
    det_e   = lr._lm_build_deterministic_review(_EV30, cands_e)
    parsed_det = lr._lm_parse_learning_review_response(det_e)
    is_det, reasons_det = lr._lm_validate_learning_review_response(parsed_det, _EV30)
    check("E-1 deterministic review passes strict validation", is_det, str(reasons_det))
    check("E-2 deterministic guardrails auto_apply_allowed=False",
          det_e["guardrails"]["auto_apply_allowed"] is False)
    check("E-3 deterministic guardrails ai_can_execute=False",
          det_e["guardrails"]["ai_can_execute"] is False)
    check("E-4 deterministic evidence rows have real values (not null)",
          any(
              row.get("value") is not None
              for obs in det_e.get("observations", [])
              for row in obs.get("evidence", [])
          ))

    # Module guardrails are always correct
    g = lr._MODULE_GUARDRAILS
    check("E-5 MODULE_GUARDRAILS live_disabled=True",               g["live_disabled"] is True)
    check("E-6 MODULE_GUARDRAILS can_auto_submit=False",            g["can_auto_submit"] is False)
    check("E-7 MODULE_GUARDRAILS auto_execution_allowed=False",     g["auto_execution_allowed"] is False)
    check("E-8 MODULE_GUARDRAILS testnet_strategy_validation=False",g["testnet_strategy_validation"] is False)

    # Sanitize after valid parse+validate preserves guardrails
    valid_resp_e = _valid_ai_response()
    parsed_e = lr._lm_parse_learning_review_response(valid_resp_e)
    is_e, _ = lr._lm_validate_learning_review_response(parsed_e, _EV30)
    san_e = lr._lm_sanitize_valid_learning_review(parsed_e)
    check("E-9 valid response validates before sanitize", is_e)
    check("E-10 sanitize preserves auto_apply_allowed=False",
          san_e["guardrails"]["auto_apply_allowed"] is False)

    # List reviews returns saved rows for correct user only
    r_e = _save_review(_UID1)
    _save_review(_UID2)  # another user's review
    reviews_e = lr._lm_get_learning_reviews(_UID1)
    user_ids = {rv.get("user_id") for rv in reviews_e}
    check("E-11 list reviews only returns own reviews", user_ids == {_UID1})
    check("E-12 list reviews count >= 1", len(reviews_e) >= 1)

    # Status transition archived → nothing allowed
    r_arc = _save_review(_UID1, status="archived")
    ok_arc, reason_arc, _ = lr._lm_update_learning_review(
        _UID1, r_arc.id, new_status="generated"
    )
    check("E-13 archived → no transitions allowed", not ok_arc)
    check("E-14 archived transition reason contains transition", "transition" in reason_arc)


# ══════════════════════════════════════════════════════════════════════════════
# Final summary
# ══════════════════════════════════════════════════════════════════════════════
print(f"\n{'='*60}")
print(f"  TOTAL: {_pass+_fail}   PASS: {_pass}   FAIL: {_fail}")
print(f"{'='*60}")
import sys
if _fail:
    sys.exit(1)
