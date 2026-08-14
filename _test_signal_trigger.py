"""Phase SIG-4/5 — trigger (when to judge) and AI verdict (what to send).

The verdict stage is the last thing between the AI and a message on someone's
phone, so most of these tests attack it: contradictory levels, hallucinated
prices, unparseable responses, and a provider that is simply not configured.

Run: python3 _test_signal_trigger.py
"""
import os, sys, types, tempfile, json
from datetime import datetime, timezone

_dbf = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False).name
os.environ["DATABASE_URL"] = f"sqlite:///{_dbf}"
os.environ["SECRET_KEY"]   = "test_secret_signal_trigger"
os.environ["ZYNI_SIG_PROMOTION_ENABLED"] = "0"
os.environ.setdefault("RESEND_API_KEY",  "test")
os.environ.setdefault("TURNSTILE_SECRET", "")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
for _mn in ["psycopg2", "psycopg2.extras", "resend"]:
    if _mn not in sys.modules:
        sys.modules[_mn] = types.ModuleType(_mn)

from unittest.mock import patch                                    # noqa: E402
import main                                                        # noqa: E402
from models import db, User, LiveMonitorFlowCandle as FC           # noqa: E402
from live_monitor.signal_trigger import (                          # noqa: E402
    evaluate_group, evaluate_groups, zone_position, flow_history_ready,
    get_symbol_price, WAITING, ARMED, INVALID, MIN_FLOW_CANDLES,
)
from live_monitor.signal_verdict import (                          # noqa: E402
    validate_verdict, _extract_json, judge_setup, build_verdict_context,
)
from live_monitor.signal_settings import get_or_create_signal_settings  # noqa: E402

with main.app.app_context():
    db.create_all()
    _u = User(username="trig", password_hash="x", role="user", status="active")
    db.session.add(_u); db.session.commit()
    UID = _u.id

_pass = 0; _fail = 0
def check(name, cond, info=""):
    global _pass, _fail
    if cond: print(f"  PASS  {name}"); _pass += 1
    else:    print(f"  FAIL  {name}" + (f"  [{info}]" if info else "")); _fail += 1
def section(t): print(f"\n── {t} ──")

NOW = datetime(2026, 8, 13, 12, 0, 0, tzinfo=timezone.utc)

class S:
    """Minimal settings stand-in."""
    def __init__(self, trigger_mode="in_zone", approach_pct=1.5):
        self.trigger_mode = trigger_mode
        self.approach_pct = approach_pct

def grp(symbol="TRIGUSDT", direction="long", zh=101, zl=99, **kw):
    return {"symbol": symbol, "direction": direction, "exchange": "binance",
            "zone_high": zh, "zone_low": zl, "modules": ["ob", "bias_shift"],
            "matched_modules": ["ob", "bias_shift"], "module_count": 2,
            "strength": kw.get("strength", 75), "timeframes": ["4h"]}

def _price(p, age=1.0):
    return {"ok": True, "price": p, "age_sec": age, "source": "websocket",
            "status": "fresh"}


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 1 — where price sits relative to the zone")
# ══════════════════════════════════════════════════════════════════════════════

p = zone_position(100, 101, 99)
check("1-1 inside the zone", p["inside"] is True and p["distance_pct"] == 0.0, p)
check("1-2 above the zone", zone_position(110, 101, 99)["side"] == "above")
check("1-3 below the zone", zone_position(90, 101, 99)["side"] == "below")

d = zone_position(102, 101, 99)["distance_pct"]
check("1-4 distance measured as a percent", 0.9 < d < 1.1, d)
check("1-5 zone given upside down is handled",
      zone_position(100, 99, 101)["inside"] is True)
check("1-6 missing zone reports unknown rather than guessing",
      zone_position(100, None, None)["known"] is False)
check("1-7 nonsense prices do not crash", zone_position(0, 0, 0)["known"] is False)


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 2 — waiting until price arrives")
# ══════════════════════════════════════════════════════════════════════════════

with patch("live_monitor.signal_trigger.get_symbol_price", return_value=_price(120)), \
     patch("live_monitor.signal_trigger.flow_history_ready",
           return_value={"ok": True, "candles": 50, "required": 12}):
    ev = evaluate_group(grp(), S("in_zone"), now=NOW)
check("2-1 price far above a long zone waits", ev["state"] == WAITING, ev["reasons"])
check("2-2 reason explains it is outside the zone",
      any("outside_zone" in r for r in ev["reasons"]), ev["reasons"])

with patch("live_monitor.signal_trigger.get_symbol_price", return_value=_price(100)), \
     patch("live_monitor.signal_trigger.flow_history_ready",
           return_value={"ok": True, "candles": 50, "required": 12}):
    ev = evaluate_group(grp(), S("in_zone"), now=NOW)
check("2-3 price inside the zone arms", ev["state"] == ARMED, ev["reasons"])
check("2-4 tagged as an in-zone alert", ev.get("alert_kind") == "in_zone")

# Approach mode: 101.5 is ~0.5% above a zone topping at 101.
with patch("live_monitor.signal_trigger.get_symbol_price", return_value=_price(101.5)), \
     patch("live_monitor.signal_trigger.flow_history_ready",
           return_value={"ok": True, "candles": 50, "required": 12}):
    ev_near = evaluate_group(grp(), S("approach", 1.5), now=NOW)
    ev_strict = evaluate_group(grp(), S("in_zone"), now=NOW)
check("2-5 approach mode arms while price is near", ev_near["state"] == ARMED,
      ev_near["reasons"])
check("2-6 tagged as an approach alert", ev_near.get("alert_kind") == "approach")
check("2-7 in-zone mode still waits at the same price",
      ev_strict["state"] == WAITING, ev_strict["reasons"])

with patch("live_monitor.signal_trigger.get_symbol_price", return_value=_price(105)), \
     patch("live_monitor.signal_trigger.flow_history_ready",
           return_value={"ok": True, "candles": 50, "required": 12}):
    ev = evaluate_group(grp(), S("approach", 1.5), now=NOW)
check("2-8 too far away still waits even in approach mode",
      ev["state"] == WAITING, ev["reasons"])


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 3 — a setup price has run past is dead")
# ══════════════════════════════════════════════════════════════════════════════

with patch("live_monitor.signal_trigger.get_symbol_price", return_value=_price(90)), \
     patch("live_monitor.signal_trigger.flow_history_ready",
           return_value={"ok": True, "candles": 50, "required": 12}):
    ev = evaluate_group(grp(direction="long"), S(), now=NOW)
check("3-1 price collapsed below a long zone is invalid, not waiting",
      ev["state"] == INVALID, ev)
check("3-2 reason is explicit", "price_below_long_zone" in ev["reasons"])

with patch("live_monitor.signal_trigger.get_symbol_price", return_value=_price(120)), \
     patch("live_monitor.signal_trigger.flow_history_ready",
           return_value={"ok": True, "candles": 50, "required": 12}):
    ev = evaluate_group(grp(direction="short"), S(), now=NOW)
check("3-3 price ran above a short zone is invalid", ev["state"] == INVALID, ev)


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 4 — never judge on missing or stale data")
# ══════════════════════════════════════════════════════════════════════════════

with patch("live_monitor.signal_trigger.get_symbol_price",
           return_value={"ok": False, "error": "price_unavailable"}):
    ev = evaluate_group(grp(), S(), now=NOW)
check("4-1 no price means wait, never arm", ev["state"] == WAITING)
check("4-2 reason names the missing price", "price_unavailable" in ev["reasons"])

with patch("live_monitor.signal_trigger.get_symbol_price",
           return_value=_price(100, age=600)):
    ev = evaluate_group(grp(), S(), now=NOW)
check("4-3 a stale price feed blocks arming", ev["state"] == WAITING)
check("4-4 reason names staleness", any("stale" in r for r in ev["reasons"]),
      ev["reasons"])

with patch("live_monitor.signal_trigger.get_symbol_price", return_value=_price(100)), \
     patch("live_monitor.signal_trigger.flow_history_ready",
           return_value={"ok": False, "candles": 3, "required": 12}):
    ev = evaluate_group(grp(), S(), now=NOW)
check("4-5 too little order-flow history blocks arming", ev["state"] == WAITING)
check("4-6 reason shows the progress", any("flow_history_building" in r
      for r in ev["reasons"]), ev["reasons"])

with patch("live_monitor.signal_trigger.get_symbol_price", return_value=_price(100)), \
     patch("live_monitor.signal_trigger.flow_history_ready",
           return_value={"ok": True, "candles": 50, "required": 12}):
    ev = evaluate_group(grp(direction="neutral"), S(), now=NOW)
check("4-7 a group with no direction never arms", ev["state"] == WAITING)
check("4-8 reason is explicit", "no_direction_yet" in ev["reasons"])

    # Directional scans with no zone can still arm — their evidence is not positional.
with patch("live_monitor.signal_trigger.get_symbol_price", return_value=_price(100)), \
     patch("live_monitor.signal_trigger.flow_history_ready",
           return_value={"ok": True, "candles": 50, "required": 12}):
    ev = evaluate_group(grp(zh=None, zl=None), S(), now=NOW)
check("4-9 a zoneless directional setup can still arm", ev["state"] == ARMED,
      ev["reasons"])


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 5 — real flow-history counting")
# ══════════════════════════════════════════════════════════════════════════════

with main.app.app_context():
    r = flow_history_ready("EMPTYUSDT")
    check("5-1 a symbol with no history is not ready", r["ok"] is False and r["candles"] == 0)

    for i in range(MIN_FLOW_CANDLES + 3):
        db.session.add(FC(symbol="HISTUSDT", timeframe="1m",
                          candle_open_ms=1_700_000_000_000 + i * 60_000,
                          price_close=1, buy_vol_usd=0, sell_vol_usd=0,
                          delta_usd=0, cvd_usd=0, tick_count=1))
    db.session.commit()
    r = flow_history_ready("HISTUSDT")
    check("5-2 enough stored candles marks it ready", r["ok"] is True, r)


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 6 — the AI's levels must agree with its own direction")
# ══════════════════════════════════════════════════════════════════════════════

good_long = {"verdict": "send", "confidence": 80, "entry": 100,
             "stop_loss": 98, "take_profit_1": 104, "take_profit_2": 108,
             "summary": "ok", "key_evidence": ["cvd rising"]}
ok, v, errs = validate_verdict(good_long, "long", price=100)
check("6-1 a coherent long is accepted", ok is True, errs)
check("6-2 risk/reward computed", v["risk_reward"] == 2.0, v["risk_reward"])

bad = dict(good_long, stop_loss=105)
ok, v, errs = validate_verdict(bad, "long", price=100)
check("6-3 a long with the stop ABOVE entry is rejected", ok is False, v)
check("6-4 error names the problem", "long_stop_not_below_entry" in errs, errs)

bad = dict(good_long, take_profit_1=96)
ok, v, errs = validate_verdict(bad, "long", price=100)
check("6-5 a long with the target BELOW entry is rejected", ok is False)

good_short = {"verdict": "send", "confidence": 70, "entry": 100,
              "stop_loss": 103, "take_profit_1": 94, "summary": "s"}
ok, v, errs = validate_verdict(good_short, "short", price=100)
check("6-6 a coherent short is accepted", ok is True, errs)

ok, v, errs = validate_verdict(dict(good_short, stop_loss=97), "short", price=100)
check("6-7 a short with the stop BELOW entry is rejected", ok is False)

# Internally coherent (stop below, targets above) but the whole cluster sits
# nowhere near the market — this isolates the drift check from the direction ones.
drifted = {"verdict": "send", "confidence": 80, "entry": 500,
           "stop_loss": 490, "take_profit_1": 520, "summary": "x"}
ok, v, errs = validate_verdict(drifted, "long", price=100)
check("6-8 an entry nowhere near the market is rejected", ok is False, errs)
check("6-9 error names the drift", any("entry_far_from_price" in e for e in errs), errs)

ok, v, errs = validate_verdict(dict(good_long, stop_loss=99.99, take_profit_1=1000),
                               "long", price=100)
check("6-10 an absurd risk/reward is rejected", ok is False, errs)

ok, v, errs = validate_verdict(dict(good_long, stop_loss=100), "long", price=100)
check("6-11 zero risk distance is rejected", ok is False, errs)

ok, v, errs = validate_verdict(dict(good_long, take_profit_2=101), "long", price=100)
check("6-12 a second target closer than the first is dropped, not fatal",
      ok is True and v["take_profit_2"] is None, (ok, v and v.get("take_profit_2")))

ok, v, errs = validate_verdict({"verdict": "hold", "confidence": 40,
                                "summary": "not yet"}, "long")
check("6-13 hold needs no levels", ok is True and v["entry"] is None, (ok, v))

ok, v, errs = validate_verdict({"verdict": "maybe"}, "long")
check("6-14 an unknown verdict word is rejected", ok is False, errs)

ok, v, errs = validate_verdict(dict(good_long, entry=None), "long", price=100)
check("6-15 send with a missing entry is rejected", ok is False, errs)

ok, v, errs = validate_verdict("not a dict", "long")
check("6-16 a non-object response is rejected", ok is False)

ok, v, errs = validate_verdict(dict(good_long, confidence=9999), "long", price=100)
check("6-17 confidence is clamped to 0-100", ok is True and v["confidence"] == 100,
      v and v.get("confidence"))


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 7 — reading the AI's response")
# ══════════════════════════════════════════════════════════════════════════════

check("7-1 plain JSON parsed", _extract_json('{"verdict":"hold"}')["verdict"] == "hold")
check("7-2 fenced JSON parsed",
      _extract_json('```json\n{"verdict":"send"}\n```')["verdict"] == "send")
check("7-3 JSON after prose parsed",
      _extract_json('Here you go:\n{"verdict":"discard"}')["verdict"] == "discard")
check("7-4 a dict passes straight through",
      _extract_json({"verdict": "hold"})["verdict"] == "hold")
check("7-5 malformed JSON returns None rather than a guess",
      _extract_json('{"verdict": broken') is None)
check("7-6 empty input returns None", _extract_json("") is None)
check("7-7 prose with no JSON returns None", _extract_json("I think you should wait") is None)


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 8 — judging end to end, with the AI faked")
# ══════════════════════════════════════════════════════════════════════════════

ev_armed = {"state": ARMED, "price": 100, "alert_kind": "in_zone",
            "reasons": ["ready_for_verdict"]}

with main.app.app_context():
    with patch.object(main, "_lm_call_ai_provider", return_value={
            "ok": True, "model": "test-model", "agent_label": "Test",
            "analysis": json.dumps(good_long)}):
        res = judge_setup(grp(), ev_armed)
    check("8-1 a good verdict comes back ok", res.get("ok") is True, res.get("reason"))
    check("8-2 verdict and levels carried through",
          res["verdict"] == "send" and res["entry"] == 100 and res["stop_loss"] == 98)
    check("8-3 model recorded for the audit trail", res.get("model") == "test-model")

    # A provider that is not configured must NOT quietly become a hold.
    with patch.object(main, "_lm_call_ai_provider", return_value={
            "ok": False, "configured": False, "error": "AI provider not configured",
            "analysis": "unavailable"}):
        res = judge_setup(grp(), ev_armed)
    check("8-4 an unconfigured AI reports failure, not a silent hold",
          res.get("ok") is False and res.get("reason") == "ai_unavailable", res)

    with patch.object(main, "_lm_call_ai_provider", return_value={
            "ok": True, "model": "m", "analysis": "I cannot answer that."}):
        res = judge_setup(grp(), ev_armed)
    check("8-5 an unparseable response is rejected",
          res.get("ok") is False and res["reason"] == "ai_response_unparseable", res)

    # The dangerous case: model says send, but its stop contradicts the direction.
    with patch.object(main, "_lm_call_ai_provider", return_value={
            "ok": True, "model": "m",
            "analysis": json.dumps(dict(good_long, stop_loss=110))}):
        res = judge_setup(grp(), ev_armed)
    check("8-6 contradictory levels are rejected, never repaired",
          res.get("ok") is False and res["reason"] == "ai_response_rejected", res)
    check("8-7 the specific contradiction is reported",
          any("stop_not_below" in e for e in res.get("errors", [])), res.get("errors"))

    with patch.object(main, "_lm_call_ai_provider", side_effect=RuntimeError("boom")):
        res = judge_setup(grp(), ev_armed)
    check("8-8 a crashing provider is caught, not raised",
          res.get("ok") is False and "ai_call_failed" in res["reason"], res)


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 9 — batch evaluation and ordering")
# ══════════════════════════════════════════════════════════════════════════════

groups = [grp("AUSDT", strength=60), grp("BUSDT", strength=95),
          grp("CUSDT", direction="long", zh=101, zl=99)]

def _fake_price(symbol, exchange="binance"):
    return _price(90) if symbol == "CUSDT" else _price(100)

with patch("live_monitor.signal_trigger.get_symbol_price", side_effect=_fake_price), \
     patch("live_monitor.signal_trigger.flow_history_ready",
           return_value={"ok": True, "candles": 50, "required": 12}):
    res = evaluate_groups(groups, S(), now=NOW)

check("9-1 armed and invalid separated",
      len(res["armed"]) == 2 and len(res["invalid"]) == 1,
      (len(res["armed"]), len(res["waiting"]), len(res["invalid"])))
check("9-2 strongest armed setup ranked first",
      res["armed"][0]["group"]["symbol"] == "BUSDT",
      [r["group"]["symbol"] for r in res["armed"]])
check("9-3 an empty list is safe", evaluate_groups([], S())["armed"] == [])

with patch("live_monitor.signal_trigger.get_symbol_price",
           side_effect=RuntimeError("nope")):
    res = evaluate_groups(groups, S(), now=NOW)
check("9-4 one broken group does not stop the batch",
      isinstance(res.get("armed"), list), res)


print(f"\n{'='*60}")
print(f"  TOTAL: {_pass+_fail}   PASS: {_pass}   FAIL: {_fail}")
print(f"{'='*60}")
if _fail:
    sys.exit(1)
