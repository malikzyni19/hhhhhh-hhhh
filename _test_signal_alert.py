"""Phase SIG-6 — the alert gate, the message, and delivery.

The gate is what stands between a working funnel and a phone that buzzes
until it gets muted, so most of these tests attack it: duplicate alerts,
daily caps, per-pair cooldowns, and the ordering rule that says a suppressed
setup must never cost an AI call.

Run: python3 _test_signal_alert.py
"""
import os, sys, types, tempfile, json
from datetime import datetime, timezone, timedelta

_dbf = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False).name
os.environ["DATABASE_URL"] = f"sqlite:///{_dbf}"
os.environ["SECRET_KEY"]   = "test_secret_signal_alert"
os.environ["ZYNI_SIG_PROMOTION_ENABLED"] = "0"
os.environ.setdefault("RESEND_API_KEY",  "test")
os.environ.setdefault("TURNSTILE_SECRET", "")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
for _mn in ["psycopg2", "psycopg2.extras", "resend"]:
    if _mn not in sys.modules:
        sys.modules[_mn] = types.ModuleType(_mn)

from unittest.mock import patch                                    # noqa: E402
import main                                                        # noqa: E402
from models import db, User, LiveMonitorSignalAlert as A           # noqa: E402
from live_monitor.signal_alert import (                            # noqa: E402
    check_gate, build_alert_key, assign_tier, format_alert_message,
    record_and_send, run_alert_cycle,
)
from live_monitor.signal_settings import get_or_create_signal_settings  # noqa: E402
from live_monitor.signal_trigger import ARMED                      # noqa: E402

with main.app.app_context():
    db.create_all()
    _u = User(username="alerter", password_hash="x", role="user", status="active")
    db.session.add(_u); db.session.commit()
    UID = _u.id

_pass = 0; _fail = 0
def check(name, cond, info=""):
    global _pass, _fail
    if cond: print(f"  PASS  {name}"); _pass += 1
    else:    print(f"  FAIL  {name}" + (f"  [{info}]" if info else "")); _fail += 1
def section(t): print(f"\n── {t} ──")

NOW = datetime(2026, 8, 13, 12, 0, 0, tzinfo=timezone.utc)
GOOD_TOKEN = "123456789:AAFxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

def grp(symbol="ALRTUSDT", direction="long", mods=None, strength=80):
    mods = mods or ["ob", "bias_shift"]
    return {"symbol": symbol, "direction": direction, "exchange": "binance",
            "modules": mods, "matched_modules": mods, "module_count": len(mods),
            "strength": strength, "timeframes": ["4h"],
            "zone_high": 101, "zone_low": 99, "detected_price": 100}

EVAL = {"state": ARMED, "price": 100, "alert_kind": "in_zone",
        "reasons": ["ready_for_verdict"]}
VERDICT = {"ok": True, "verdict": "send", "confidence": 85, "entry": 100,
           "stop_loss": 98, "take_profit_1": 104, "take_profit_2": 108,
           "risk_reward": 2.0, "summary": "CVD rising into demand.",
           "key_evidence": ["CVD rising", "OI up 4%"], "model": "test-model"}

def settings_for(uid, **kw):
    st = get_or_create_signal_settings(uid)
    st.enabled = kw.get("enabled", True)
    st.delivery_enabled = kw.get("delivery_enabled", False)
    st.max_signals_per_day = kw.get("max_signals_per_day", 6)
    st.per_pair_cooldown_min = kw.get("per_pair_cooldown_min", 240)
    st.min_confidence = kw.get("min_confidence", 65)
    if kw.get("telegram"):
        st.telegram_bot_token = GOOD_TOKEN
        st.telegram_chat_id   = "555"
    db.session.commit()
    return st


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 1 — one alert per setup per candle")
# ══════════════════════════════════════════════════════════════════════════════

k1 = build_alert_key(UID, "BTCUSDT", "long", "4h", NOW)
k2 = build_alert_key(UID, "BTCUSDT", "long", "4h", NOW + timedelta(minutes=30))
k3 = build_alert_key(UID, "BTCUSDT", "long", "4h", NOW + timedelta(hours=5))
k4 = build_alert_key(UID, "BTCUSDT", "short", "4h", NOW)
k5 = build_alert_key(UID + 1, "BTCUSDT", "long", "4h", NOW)

check("1-1 same candle gives the same key", k1 == k2)
check("1-2 next candle gives a new key", k1 != k3)
check("1-3 opposite direction is a different alert", k1 != k4)
check("1-4 a different user gets their own key", k1 != k5)


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 2 — the gate")
# ══════════════════════════════════════════════════════════════════════════════

with main.app.app_context():
    A.query.delete(); db.session.commit()
    st = settings_for(UID)

    g = check_gate(UID, grp(), st, now=NOW)
    check("2-1 first alert for a pair is allowed", g["allowed"] is True, g)
    check("2-2 remaining budget reported", g["remaining_today"] == 6, g)

    # Record one, then the same setup must be blocked.
    record_and_send(UID, grp(), EVAL, VERDICT, st, g, now=NOW)
    g2 = check_gate(UID, grp(), st, now=NOW)
    check("2-3 the same setup is not alerted twice in one candle",
          g2["allowed"] is False, g2)
    check("2-4 reason is explicit",
          g2["reason"] == "already_alerted_this_candle", g2["reason"])

    # A new candle clears the duplicate check, but the pair cooldown must
    # still hold — an 8h cooldown covers a point 5h after the first alert.
    st.per_pair_cooldown_min = 480
    db.session.commit()
    g3 = check_gate(UID, grp(), st, now=NOW + timedelta(hours=5))
    check("2-5 pair cooldown blocks the same coin in a later candle",
          g3["allowed"] is False and "pair_cooldown" in str(g3["reason"]), g3)

    # Shorten the cooldown so that same moment now falls outside it.
    st.per_pair_cooldown_min = 60
    db.session.commit()
    g5 = check_gate(UID, grp(), st, now=NOW + timedelta(hours=5))
    check("2-6 a shorter cooldown lets the coin through again",
          g5["allowed"] is True, g5)

    # A different coin is unaffected by another coin's cooldown.
    g6 = check_gate(UID, grp("OTHERUSDT"), st, now=NOW)
    check("2-7 another coin is not blocked by this one's cooldown",
          g6["allowed"] is True, g6)


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 3 — the daily cap")
# ══════════════════════════════════════════════════════════════════════════════

with main.app.app_context():
    A.query.delete(); db.session.commit()
    st = settings_for(UID, max_signals_per_day=2, per_pair_cooldown_min=15)

    for i, sym in enumerate(["C1USDT", "C2USDT"]):
        g = check_gate(UID, grp(sym), st, now=NOW)
        check(f"3-{i+1} alert {i+1} allowed under a cap of 2", g["allowed"] is True, g)
        record_and_send(UID, grp(sym), EVAL, VERDICT, st, g, now=NOW)

    g = check_gate(UID, grp("C3USDT"), st, now=NOW)
    check("3-3 the third is blocked by the daily cap", g["allowed"] is False, g)
    check("3-4 reason names the cap", "daily_cap" in g["reason"], g["reason"])

    # Tomorrow the budget resets.
    g = check_gate(UID, grp("C3USDT"), st, now=NOW + timedelta(days=1))
    check("3-5 the cap resets the next day", g["allowed"] is True, g)

    # Recorded-only alerts still count, so switching delivery on later cannot
    # release a backlog all at once.
    check("3-6 recorded-only alerts count against the cap",
          A.query.filter_by(user_id=UID).count() == 2)


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 4 — tiers")
# ══════════════════════════════════════════════════════════════════════════════

check("4-1 three scans plus high confidence is tier A", assign_tier(3, 85) == "A")
check("4-2 three scans but low confidence is tier B", assign_tier(3, 60) == "B")
check("4-3 high confidence but one scan is tier B", assign_tier(1, 95) == "B")
check("4-4 missing values do not crash", assign_tier(None, None) == "B")


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 5 — the message")
# ══════════════════════════════════════════════════════════════════════════════

msg = format_alert_message(grp(), EVAL, VERDICT, "A")
check("5-1 symbol and direction present", "ALRTUSDT" in msg and "LONG" in msg)
check("5-2 confidence shown", "85" in msg)
check("5-3 scan names shown in plain words",
      "Order Block" in msg and "Bias Shift" in msg, msg[:200])
check("5-4 levels present",
      "Entry" in msg and "Stop" in msg and "Targets" in msg)
check("5-5 both targets shown", "104" in msg and "108" in msg)
check("5-6 risk reward shown", "R:R" in msg)
check("5-7 the AI's summary is included", "CVD rising into demand." in msg)
check("5-8 evidence lines included", "OI up 4%" in msg)
check("5-9 tier A marked", "🅰️" in msg)
check("5-10 long is marked green", "🟢" in msg)

short_msg = format_alert_message(grp(direction="short"), EVAL,
                                 dict(VERDICT, stop_loss=103, take_profit_1=94,
                                      take_profit_2=None), "B")
check("5-11 short is marked red", "🔴" in short_msg)
check("5-12 tier B marked", "🅱️" in short_msg)
check("5-13 a single target renders without a stray separator",
      "/" not in short_msg.split("Targets")[1].split("\n")[0], short_msg)

# HTML-unsafe content must not break the markup.
nasty = dict(VERDICT, summary="<script>alert(1)</script> & more",
             key_evidence=["a < b & c"])
m = format_alert_message(grp(symbol="X<Y>USDT"), EVAL, nasty, "B")
check("5-14 dangerous characters are escaped",
      "<script>" not in m and "&lt;script&gt;" in m, m[:160])
check("5-15 ampersands escaped", "&amp;" in m)

tiny = format_alert_message(grp(), EVAL,
                            dict(VERDICT, entry=0.00001234, stop_loss=0.00001,
                                 take_profit_1=0.00002), "B")
check("5-16 very small prices keep their precision", "0.00001234" in tiny, tiny)
big = format_alert_message(grp(), EVAL,
                           dict(VERDICT, entry=64000, stop_loss=63000,
                                take_profit_1=66000), "B")
check("5-17 large prices are readable", "64,000" in big, big)


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 6 — recording and delivery")
# ══════════════════════════════════════════════════════════════════════════════

class _Resp:
    def __init__(self, p): self._p = p; self.status_code = 200
    def json(self): return self._p

def _ok_send(url, **kw):
    return _Resp({"ok": True, "result": {"message_id": 777}})

with main.app.app_context():
    A.query.delete(); db.session.commit()

    # Delivery off: a real record, explicitly marked as not sent.
    st = settings_for(UID, delivery_enabled=False)
    g = check_gate(UID, grp("SHADOWUSDT"), st, now=NOW)
    res = record_and_send(UID, grp("SHADOWUSDT"), EVAL, VERDICT, st, g, now=NOW)
    check("6-1 with sending off the alert is still recorded",
          res["ok"] is True and res["status"] == "shadow", res)
    row = A.query.filter_by(symbol="SHADOWUSDT").first()
    check("6-2 the record holds the AI's levels",
          row.entry_price == 100 and row.stop_loss == 98)
    check("6-3 nothing was marked as sent", row.sent_at is None)

    # Delivery on and Telegram linked.
    A.query.delete(); db.session.commit()
    st = settings_for(UID, delivery_enabled=True, telegram=True)
    g = check_gate(UID, grp("SENTUSDT"), st, now=NOW)
    with patch("live_monitor.telegram_notify.requests.post", side_effect=_ok_send):
        res = record_and_send(UID, grp("SENTUSDT"), EVAL, VERDICT, st, g, now=NOW)
    check("6-4 the alert is delivered", res["delivered"] is True, res)
    row = A.query.filter_by(symbol="SENTUSDT").first()
    check("6-5 marked sent", row.delivery_status == "sent")
    check("6-6 telegram message id stored for follow-ups",
          row.telegram_message_id == "777", row.telegram_message_id)

    # Telegram down: the record must survive.
    A.query.delete(); db.session.commit()
    g = check_gate(UID, grp("FAILUSDT"), st, now=NOW)
    import requests as _rq
    with patch("live_monitor.telegram_notify.requests.post",
               side_effect=_rq.exceptions.Timeout()):
        res = record_and_send(UID, grp("FAILUSDT"), EVAL, VERDICT, st, g, now=NOW)
    check("6-7 a failed send reports failure", res["ok"] is False, res)
    row = A.query.filter_by(symbol="FAILUSDT").first()
    check("6-8 the alert is NOT lost when Telegram is down", row is not None)
    check("6-9 marked failed with the reason kept",
          row.delivery_status == "failed" and row.delivery_error, row.delivery_error)

    # Delivery on but Telegram not linked.
    A.query.delete(); db.session.commit()
    st = settings_for(UID, delivery_enabled=True, telegram=False)
    st.telegram_bot_token = None; st.telegram_chat_id = None
    db.session.commit()
    g = check_gate(UID, grp("NOLINKUSDT"), st, now=NOW)
    res = record_and_send(UID, grp("NOLINKUSDT"), EVAL, VERDICT, st, g, now=NOW)
    check("6-10 unlinked telegram fails clearly rather than silently",
          res["ok"] is False and res["reason"] == "telegram_not_linked", res)


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 7 — the full cycle, and its cost ordering")
# ══════════════════════════════════════════════════════════════════════════════

with main.app.app_context():
    A.query.delete(); db.session.commit()
    st = settings_for(UID, enabled=False)
    res = run_alert_cycle(UID, now=NOW)
    check("7-1 engine off means nothing runs", res.get("skipped") == "engine_off", res)

    st = settings_for(UID, enabled=True, delivery_enabled=False,
                      max_signals_per_day=5, per_pair_cooldown_min=15)

    armed = {"armed": [{"group": grp("Z1USDT"), "evaluation": EVAL},
                       {"group": grp("Z2USDT"), "evaluation": EVAL}],
             "waiting": [], "invalid": []}

    with patch("live_monitor.signal_confluence.build_confluence_groups", return_value=[]), \
         patch("live_monitor.signal_confluence.filter_groups_for_settings", return_value=[]), \
         patch("live_monitor.signal_trigger.evaluate_groups", return_value=armed), \
         patch("live_monitor.signal_verdict.judge_setup", return_value=VERDICT) as judged:
        res = run_alert_cycle(UID, now=NOW)
    check("7-2 both armed setups recorded",
          res["shadow"] == 2 and res["sent"] == 0, res)
    check("7-3 the AI was asked once per setup", judged.call_count == 2,
          judged.call_count)

    # Re-running immediately: the gate blocks, and the AI must NOT be called.
    with patch("live_monitor.signal_confluence.build_confluence_groups", return_value=[]), \
         patch("live_monitor.signal_confluence.filter_groups_for_settings", return_value=[]), \
         patch("live_monitor.signal_trigger.evaluate_groups", return_value=armed), \
         patch("live_monitor.signal_verdict.judge_setup", return_value=VERDICT) as judged2:
        res = run_alert_cycle(UID, now=NOW)
    check("7-4 duplicates are suppressed", res["gated"] == 2, res)
    check("7-5 a suppressed setup costs no AI call", judged2.call_count == 0,
          judged2.call_count)

    # The AI declining is respected.
    A.query.delete(); db.session.commit()
    with patch("live_monitor.signal_confluence.build_confluence_groups", return_value=[]), \
         patch("live_monitor.signal_confluence.filter_groups_for_settings", return_value=[]), \
         patch("live_monitor.signal_trigger.evaluate_groups", return_value=armed), \
         patch("live_monitor.signal_verdict.judge_setup",
               return_value=dict(VERDICT, verdict="hold")):
        res = run_alert_cycle(UID, now=NOW)
    check("7-6 an AI hold sends nothing", res["held"] == 2 and res["shadow"] == 0, res)
    check("7-7 nothing recorded when the AI holds",
          A.query.filter_by(user_id=UID).count() == 0)

    # Confidence floor.
    with patch("live_monitor.signal_confluence.build_confluence_groups", return_value=[]), \
         patch("live_monitor.signal_confluence.filter_groups_for_settings", return_value=[]), \
         patch("live_monitor.signal_trigger.evaluate_groups", return_value=armed), \
         patch("live_monitor.signal_verdict.judge_setup",
               return_value=dict(VERDICT, confidence=40)):
        res = run_alert_cycle(UID, now=NOW)
    check("7-8 low confidence is held back", res["held"] == 2, res)
    check("7-9 reason names the floor",
          any("below_confidence_floor" in s["reason"] for s in res["suppressed"]),
          res["suppressed"])

    # A failing AI must not stop the cycle.
    with patch("live_monitor.signal_confluence.build_confluence_groups", return_value=[]), \
         patch("live_monitor.signal_confluence.filter_groups_for_settings", return_value=[]), \
         patch("live_monitor.signal_trigger.evaluate_groups", return_value=armed), \
         patch("live_monitor.signal_verdict.judge_setup",
               return_value={"ok": False, "reason": "ai_unavailable"}):
        res = run_alert_cycle(UID, now=NOW)
    check("7-10 an unavailable AI records an error and sends nothing",
          res["sent"] == 0 and res["shadow"] == 0 and len(res["errors"]) == 2, res)

    # Once the cap is hit the cycle stops asking the AI at all.
    A.query.delete(); db.session.commit()
    st = settings_for(UID, enabled=True, max_signals_per_day=1,
                      per_pair_cooldown_min=15)
    with patch("live_monitor.signal_confluence.build_confluence_groups", return_value=[]), \
         patch("live_monitor.signal_confluence.filter_groups_for_settings", return_value=[]), \
         patch("live_monitor.signal_trigger.evaluate_groups", return_value=armed), \
         patch("live_monitor.signal_verdict.judge_setup", return_value=VERDICT) as j3:
        res = run_alert_cycle(UID, now=NOW)
    check("7-11 the cap is honoured", res["shadow"] == 1, res)
    check("7-12 the cycle stops asking the AI once the budget is spent",
          j3.call_count == 1, j3.call_count)


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 8 — the manual run route")
# ══════════════════════════════════════════════════════════════════════════════

client = main.app.test_client()
with client.session_transaction() as s:
    s["logged_in"] = True; s["username"] = "alerter"

r = client.post("/api/live-monitor/signal-alerts/run")
check("8-1 manual alert run returns 200", r.status_code == 200, r.status_code)
check("8-2 result reported", "result" in (r.get_json() or {}), r.get_json())

anon = main.app.test_client()
check("8-3 unauthenticated run blocked",
      anon.post("/api/live-monitor/signal-alerts/run").status_code in (302, 401))


print(f"\n{'='*60}")
print(f"  TOTAL: {_pass+_fail}   PASS: {_pass}   FAIL: {_fail}")
print(f"{'='*60}")
if _fail:
    sys.exit(1)
