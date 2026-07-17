"""Phase 11.15 AI Learning Review Loop — REAL production HTTP route tests.

Phase 11.15.4 Tasks 5 & 6. Unlike the isolated unit/flask suites, this file
imports the actual production application (main.py) and drives the real Flask
routes through the test client:

    POST  /api/live-monitor/learning-reviews/generate
    GET   /api/live-monitor/learning-reviews
    GET   /api/live-monitor/learning-reviews/<id>
    PATCH /api/live-monitor/learning-reviews/<id>
    GET   /api/live-monitor/items/<item_id>/learning-reviews

Only the AI-provider boundary and the trade-evidence query are mocked; auth,
routing, ownership, in-flight locking, validation, persistence, and status
codes are exercised for real against a file-backed SQLite database.

Run: python3 _test_learning_review_11_15_routes.py
"""
import os, sys, types, json, tempfile
from unittest.mock import patch

# ── Environment must be set BEFORE importing main ─────────────────────────────
_DBFILE = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False).name
os.environ["DATABASE_URL"]   = f"sqlite:///{_DBFILE}"
os.environ["SECRET_KEY"]     = "test_secret_phase_11_15_4_routes"
os.environ.setdefault("RESEND_API_KEY",  "test")
os.environ.setdefault("TURNSTILE_SECRET", "")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Stub only the heavy optional infra deps that main imports lazily/at top.
for _mn in ["psycopg2", "psycopg2.extras", "resend"]:
    sys.modules.setdefault(_mn, types.ModuleType(_mn))

import main                                   # noqa: E402  (real production app)
from models import db, User, LiveMonitorItem, LiveMonitorLearningReview  # noqa: E402

# ── Build schema + two users ──────────────────────────────────────────────────
with main.app.app_context():
    db.create_all()
    _alice = User(username="alice", password_hash="x", role="user", status="active")
    _bob   = User(username="bob",   password_hash="x", role="user", status="active")
    db.session.add_all([_alice, _bob])
    db.session.commit()
    UID_A = _alice.id
    UID_B = _bob.id
    # An item owned by alice, for item-scope + ownership tests
    _item = LiveMonitorItem(user_id=UID_A, symbol="BTCUSDT", exchange="binance",
                            market="perpetual", source_tab="scan")
    db.session.add(_item)
    db.session.commit()
    ITEM_A = _item.id

# ── Test runner ───────────────────────────────────────────────────────────────
_pass = 0; _fail = 0
def check(name, cond, info=""):
    global _pass, _fail
    if cond:
        print(f"  PASS  {name}"); _pass += 1
    else:
        print(f"  FAIL  {name}" + (f"  [{info}]" if info else "")); _fail += 1
def section(t):
    print(f"\n── {t} ──")

# ── Helpers ───────────────────────────────────────────────────────────────────
def client_for(username=None):
    """Return a test client; if username given, establish a logged-in session."""
    c = main.app.test_client()
    if username is not None:
        with c.session_transaction() as s:
            s["logged_in"] = True
            s["username"]  = username
    return c

def reset_rl():
    """Clear the sliding-window rate limiter so generate tests don't 429."""
    try:
        main.rate_limiter._hits.clear()
    except Exception:
        pass

def row_count(uid):
    with main.app.app_context():
        return LiveMonitorLearningReview.query.filter_by(user_id=uid).count()

# ── Evidence + AI fixtures ────────────────────────────────────────────────────
def evidence(total=30):
    return {
        "sample_size": total, "sample_quality": "developing", "warnings": [],
        "period": "30d", "symbol": None, "side": None, "review_scope": "portfolio",
        "performance_summary": {
            "trade_count": total, "win_rate_pct": 60.0, "net_realized_pnl": "150.00",
            "profit_factor": 4.0, "expectancy_amount": "5.00",
            "max_drawdown_amount": "-30.00", "recent_trend": "stable", "truncated": False,
        },
        "execution_quality": {"avg_rr_capture": 0.7, "avg_planned_rr": 2.0,
                              "avg_realized_rr": 1.4, "tp_pct": 60.0, "sl_pct": 40.0,
                              "manual_pct": 0.0},
        "segments": {
            "by_symbol": [{"label": "BTCUSDT", "count": 20, "wins": 11, "losses": 9,
                           "breakevens": 0, "win_rate": 55.0, "net_pnl": "100.00"}],
            "by_side": [{"label": "BUY", "count": 18, "wins": 10, "losses": 8,
                         "breakevens": 0, "win_rate": 55.6, "net_pnl": "90.00"},
                        {"label": "SELL", "count": 12, "wins": 6, "losses": 6,
                         "breakevens": 0, "win_rate": 50.0, "net_pnl": "60.00"}],
            "by_outcome_reason": [],
            "by_confidence": [{"label": "high", "count": 15, "wins": 10, "losses": 5,
                               "breakevens": 0, "win_rate": 66.7, "net_pnl": "80.00"}],
            "by_setup_type": [{"label": "breakout", "count": 10, "wins": 6, "losses": 4,
                               "breakevens": 0, "win_rate": 60.0, "net_pnl": "50.00"}],
            "by_entry_mode": [],
        },
        "data_quality": {},
    }

_GUARDRAILS = {
    "read_only": True, "human_review_required": True, "auto_apply_allowed": False,
    "can_change_strategy": False, "can_change_risk_guard": False,
    "can_arm_auto_gate": False, "can_auto_submit": False,
    "auto_execution_allowed": False, "ai_can_execute": False,
}

def ai_response(obs_category="risk_reward", tc_metric="performance.trade_count",
                tc_value=30, obs_n=30):
    return {
        "review_title": "30-Trade Portfolio Analysis",
        "executive_summary": "Portfolio shows 60% win rate with developing sample.",
        "overall_assessment": "mixed",
        "confidence_level": "medium",
        "sample_assessment": {"sample_size": 30, "sample_quality": "developing", "limitations": []},
        "observations": [{
            "id": "obs_1", "category": obs_category,
            "title": "Observation title",
            "statement": "Average RR capture is 70% of planned ratio.",
            "evidence": [{"metric": tc_metric, "value": tc_value, "comparison": None}],
            "sample_size": obs_n, "confidence": "medium", "severity": "watch",
            "limitations": [], "auto_apply_allowed": False,
        }],
        "review_proposals": [{
            "id": "prop_1", "action_type": "monitor", "title": "Monitor RR",
            "description": "Track RR capture over next 30 trades.",
            "evidence_observation_ids": ["obs_1"], "minimum_additional_sample": 30,
            "human_review_required": True, "auto_apply_allowed": False,
        }],
        "what_not_to_conclude": ["This sample does not prove the strategy is broken."],
        "guardrails": dict(_GUARDRAILS),
    }

def mock_ai(resp=None, ok=True):
    """patch.object context for the AI provider boundary."""
    val = {"ok": ok, "analysis": (resp if resp is not None else ai_response()),
           "model": "test-model"} if ok else {"ok": False}
    return patch.object(main, "_lm_call_ai_provider", return_value=val)

def mock_evidence(total=30):
    return patch.object(main, "_lm_build_learning_evidence", return_value=evidence(total))

def generate(client, body=None, resp=None, ai_ok=True, ev_total=30):
    reset_rl()
    with mock_evidence(ev_total), mock_ai(resp=resp, ok=ai_ok):
        return client.post("/api/live-monitor/learning-reviews/generate",
                           json=(body if body is not None else {"review_scope": "portfolio", "period": "30d"}))


# ══════════════════════════════════════════════════════════════════════════════
# TASK 5 — Real production generate endpoint
# ══════════════════════════════════════════════════════════════════════════════
section("TASK 5 — POST /learning-reviews/generate (real route)")

# 5-1: unauthenticated → redirected by custom login_required (302, not 200/201)
anon = client_for(None)
r = generate(anon)
check("5-1 unauthenticated generate → 302 redirect (login_required)", r.status_code == 302)

alice = client_for("alice")

# 5-2: authenticated + valid AI → 201, ok, review persisted
before = row_count(UID_A)
r = generate(alice)
check("5-2 valid generate → 201", r.status_code == 201, str(r.status_code))
b = r.get_json()
check("5-3 response ok=True", b.get("ok") is True)
check("5-4 response has review with id", isinstance((b.get("review") or {}).get("id"), int))
check("5-5 exactly one row persisted", row_count(UID_A) == before + 1)

# 5-6/5-7: guardrails present and locked in response
gr = b.get("guardrails") or {}
check("5-6 guardrails.auto_apply_allowed=False", gr.get("auto_apply_allowed") is False)
check("5-7 guardrails.ai_can_execute=False", gr.get("ai_can_execute") is False)

# 5-8: AI provider unavailable → deterministic fallback still 201, row saved
before = row_count(UID_A)
r = generate(alice, ai_ok=False)
check("5-8 AI-unavailable → deterministic fallback 201", r.status_code == 201, str(r.status_code))
check("5-9 fallback row persisted", row_count(UID_A) == before + 1)
with main.app.app_context():
    _last = LiveMonitorLearningReview.query.filter_by(user_id=UID_A).order_by(
        LiveMonitorLearningReview.id.desc()).first()
check("5-10 fallback row source=deterministic_fallback", _last.source == "deterministic_fallback")

# 5-11/5-12: invalid AI output (auto_apply=True) → 422, review_saved False, NO row
bad = ai_response()
bad["guardrails"]["auto_apply_allowed"] = True
before = row_count(UID_A)
r = generate(alice, resp=bad)
check("5-11 invalid AI output → 422", r.status_code == 422, str(r.status_code))
b = r.get_json()
check("5-12 response review_saved=False", b.get("review_saved") is False)
check("5-13 zero rows persisted on invalid (no save)", row_count(UID_A) == before)

# 5-14: lock released after success (uid not left in inflight set)
check("5-14 in-flight lock released after success", UID_A not in main._lm_learning_review_inflight)
# lock released after 422 too
r = generate(alice, resp=bad)
check("5-15 in-flight lock released after 422", UID_A not in main._lm_learning_review_inflight)

# 5-16: in-flight duplicate → 409 (simulate concurrent by pre-holding the lock)
main._lm_learning_review_inflight.add(UID_A)
try:
    r = generate(alice)
    check("5-16 duplicate in-flight generate → 409", r.status_code == 409, str(r.status_code))
    check("5-17 409 body error=generation_in_progress",
          (r.get_json() or {}).get("error") == "generation_in_progress")
finally:
    main._lm_learning_review_inflight.discard(UID_A)

# 5-18: insufficient sample (<5) → 200 insufficient_learning_sample, no row
before = row_count(UID_A)
r = generate(alice, ev_total=3)
check("5-18 insufficient sample → 200", r.status_code == 200, str(r.status_code))
check("5-19 body error=insufficient_learning_sample",
      (r.get_json() or {}).get("error") == "insufficient_learning_sample")
check("5-20 no row saved on insufficient sample", row_count(UID_A) == before)

# 5-21: invalid filter (bad review_scope) → 400
r = generate(alice, body={"review_scope": "not_a_scope", "period": "30d"})
check("5-21 invalid filter → 400", r.status_code == 400, str(r.status_code))

# 5-22: item ownership — item_id not owned by alice → 404
#   (bob owns nothing; use a non-existent item id for alice)
r = generate(alice, body={"review_scope": "item", "item_id": 999999, "period": "30d"})
check("5-22 unowned item_id → 404", r.status_code == 404, str(r.status_code))

# 5-23 (bonus): category-specific binding enforced THROUGH the real route.
#   symbol observation citing performance.trade_count → 422
sym_bad = ai_response(obs_category="symbol", tc_metric="performance.trade_count",
                      tc_value=30, obs_n=30)
before = row_count(UID_A)
r = generate(alice, resp=sym_bad)
check("5-23 symbol obs + performance.trade_count → 422 (category binding)",
      r.status_code == 422, str(r.status_code))
check("5-24 no row saved on category-binding rejection", row_count(UID_A) == before)

# 5-25: symbol observation citing segment.symbol count → 201 accepted
sym_ok = ai_response(obs_category="symbol",
                     tc_metric="segment.symbol.BTCUSDT.trade_count",
                     tc_value=20, obs_n=20)
r = generate(alice, resp=sym_ok)
check("5-25 symbol obs + segment.symbol.BTCUSDT.trade_count → 201",
      r.status_code == 201, str(r.status_code) + " " + str(r.get_json()))

# 5-26: rate limit — 6th generate in window → 429
reset_rl()
codes = []
with mock_evidence(30), mock_ai():
    for _ in range(6):
        codes.append(alice.post("/api/live-monitor/learning-reviews/generate",
                     json={"review_scope": "portfolio", "period": "30d"}).status_code)
check("5-26 rate limit: first 5 allowed (<=201)", all(c == 201 for c in codes[:5]), str(codes))
check("5-27 rate limit: 6th request → 429", codes[5] == 429, str(codes))
reset_rl()


# ══════════════════════════════════════════════════════════════════════════════
# TASK 6 — Real list / detail / PATCH routes
# ══════════════════════════════════════════════════════════════════════════════
section("TASK 6 — list / detail / PATCH (real routes)")

bob = client_for("bob")

# Seed one review for bob to test cross-user isolation
r = generate(bob)
check("6-1 bob generate → 201", r.status_code == 201)
bob_review_id = (r.get_json().get("review") or {}).get("id")

# 6-2/6-3: GET list ownership — alice sees only alice's, bob only bob's
la = alice.get("/api/live-monitor/learning-reviews").get_json()
lb = bob.get("/api/live-monitor/learning-reviews").get_json()
alice_ids = {rv["id"] for rv in la.get("reviews", [])}
bob_ids   = {rv["id"] for rv in lb.get("reviews", [])}
check("6-2 alice list excludes bob's review", bob_review_id not in alice_ids)
check("6-3 bob list contains only bob's review", bob_ids == {bob_review_id})

# 6-4: GET detail owned → 200
r = bob.get(f"/api/live-monitor/learning-reviews/{bob_review_id}")
check("6-4 owner GET detail → 200", r.status_code == 200)
check("6-5 detail body has review.id", (r.get_json().get("review") or {}).get("id") == bob_review_id)

# 6-6: GET detail NOT owned (alice requests bob's) → 404
r = alice.get(f"/api/live-monitor/learning-reviews/{bob_review_id}")
check("6-6 non-owner GET detail → 404", r.status_code == 404, str(r.status_code))

# 6-7: GET detail nonexistent → 404
r = bob.get("/api/live-monitor/learning-reviews/99999999")
check("6-7 nonexistent GET detail → 404", r.status_code == 404)

# 6-8: PATCH human_note set → 200 and persisted
r = bob.patch(f"/api/live-monitor/learning-reviews/{bob_review_id}",
              json={"human_note": "reviewed by bob"})
check("6-8 PATCH note → 200", r.status_code == 200, str(r.status_code))
with main.app.app_context():
    _row = LiveMonitorLearningReview.query.get(bob_review_id)
check("6-9 note persisted in DB", _row.human_note == "reviewed by bob")

# 6-10: PATCH status valid transition generated → reviewed → 200
r = bob.patch(f"/api/live-monitor/learning-reviews/{bob_review_id}",
              json={"status": "reviewed"})
check("6-10 PATCH valid transition → 200", r.status_code == 200, str(r.status_code))
check("6-11 status now reviewed", (r.get_json().get("review") or {}).get("status") == "reviewed")

# 6-12: PATCH invalid transition (reviewed → generated) → 400
r = bob.patch(f"/api/live-monitor/learning-reviews/{bob_review_id}",
              json={"status": "generated"})
check("6-12 PATCH invalid transition → 400", r.status_code == 400, str(r.status_code))

# 6-13: PATCH invalid status value → 400
r = bob.patch(f"/api/live-monitor/learning-reviews/{bob_review_id}",
              json={"status": "not_a_status"})
check("6-13 PATCH invalid status value → 400", r.status_code == 400)

# 6-14: PATCH with no fields → 400
r = bob.patch(f"/api/live-monitor/learning-reviews/{bob_review_id}", json={})
check("6-14 PATCH no fields → 400", r.status_code == 400)

# 6-15: PATCH not owned (alice patches bob's) → 404
r = alice.patch(f"/api/live-monitor/learning-reviews/{bob_review_id}",
                json={"human_note": "hijack"})
check("6-15 non-owner PATCH → 404", r.status_code == 404, str(r.status_code))
with main.app.app_context():
    _row = LiveMonitorLearningReview.query.get(bob_review_id)
check("6-16 non-owner PATCH did not mutate note", _row.human_note == "reviewed by bob")

# 6-17: PATCH clear note (explicit null) → 200 and note cleared
r = bob.patch(f"/api/live-monitor/learning-reviews/{bob_review_id}",
              json={"human_note": None})
check("6-17 PATCH note=null → 200", r.status_code == 200)
with main.app.app_context():
    _row = LiveMonitorLearningReview.query.get(bob_review_id)
check("6-18 note cleared in DB", _row.human_note is None)

# 6-19: unauthenticated list → 302 redirect
r = client_for(None).get("/api/live-monitor/learning-reviews")
check("6-19 unauthenticated list → 302", r.status_code == 302)

# 6-20: item-scoped list ownership — alice's item returns 200, bob's access → 404
r = alice.get(f"/api/live-monitor/items/{ITEM_A}/learning-reviews")
check("6-20 owner item-scoped list → 200", r.status_code == 200, str(r.status_code))
r = bob.get(f"/api/live-monitor/items/{ITEM_A}/learning-reviews")
check("6-21 non-owner item-scoped list → 404", r.status_code == 404, str(r.status_code))


# ══════════════════════════════════════════════════════════════════════════════
print(f"\n{'='*60}")
print(f"  TOTAL: {_pass+_fail}   PASS: {_pass}   FAIL: {_fail}")
print(f"{'='*60}")
if _fail:
    sys.exit(1)
