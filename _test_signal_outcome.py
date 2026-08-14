"""Phase SIG-7 — outcome tracking: did the signal actually work?

The rule that matters most here is ORDER. A setup that touched its stop
before its target is a loss, even if the price later reached the target.
Getting that backwards would inflate the win rate — the one number this whole
system exists to make honest.

Run: python3 _test_signal_outcome.py
"""
import os, sys, types, tempfile, json
from datetime import datetime, timezone, timedelta

_dbf = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False).name
os.environ["DATABASE_URL"] = f"sqlite:///{_dbf}"
os.environ["SECRET_KEY"]   = "test_secret_signal_outcome"
os.environ["ZYNI_SIG_PROMOTION_ENABLED"] = "0"
os.environ.setdefault("RESEND_API_KEY",  "test")
os.environ.setdefault("TURNSTILE_SECRET", "")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
for _mn in ["psycopg2", "psycopg2.extras", "resend"]:
    if _mn not in sys.modules:
        sys.modules[_mn] = types.ModuleType(_mn)

from unittest.mock import patch                                    # noqa: E402
import main                                                        # noqa: E402
from models import (db, User, LiveMonitorSignalAlert as A,         # noqa: E402
                    LiveMonitorFlowCandle as FC)
from live_monitor.signal_outcome import (                          # noqa: E402
    resolve_from_path, result_in_r, resolve_alert, track_open_alerts,
    module_accuracy, MAX_TRACK_HOURS,
)
from live_monitor.signal_settings import get_or_create_signal_settings  # noqa: E402

with main.app.app_context():
    db.create_all()
    _u = User(username="outc", password_hash="x", role="user", status="active")
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

def path(prices, start=NOW):
    base = int(start.timestamp() * 1000)
    return [(base + i * 60_000, p) for i, p in enumerate(prices)]


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 1 — order decides the outcome")
# ══════════════════════════════════════════════════════════════════════════════

# Long: entry 100, stop 98, target 104.
r = resolve_from_path(path([100, 101, 102, 104.5]), "long", 100, 98, 104)
check("1-1 long reaching target is a win", r["outcome"] == "target_hit", r)
check("1-2 exit price recorded", r["price"] == 104.5)

r = resolve_from_path(path([100, 99, 97.5]), "long", 100, 98, 104)
check("1-3 long reaching stop is a loss", r["outcome"] == "stop_hit", r)

# The case that matters: stop first, target later.
r = resolve_from_path(path([100, 97, 105]), "long", 100, 98, 104)
check("1-4 stop touched BEFORE target is a loss, not a win",
      r["outcome"] == "stop_hit", r)

# And the mirror: target first, stop later.
r = resolve_from_path(path([100, 105, 97]), "long", 100, 98, 104)
check("1-5 target touched BEFORE stop is a win", r["outcome"] == "target_hit", r)

# Short: entry 100, stop 103, target 94.
r = resolve_from_path(path([100, 98, 93]), "short", 100, 103, 94)
check("1-6 short reaching target is a win", r["outcome"] == "target_hit", r)
r = resolve_from_path(path([100, 101, 104]), "short", 100, 103, 94)
check("1-7 short reaching stop is a loss", r["outcome"] == "stop_hit", r)
r = resolve_from_path(path([100, 104, 90]), "short", 100, 103, 94)
check("1-8 short stop before target is still a loss", r["outcome"] == "stop_hit", r)

r = resolve_from_path(path([100, 101, 102, 103]), "long", 100, 98, 104)
check("1-9 neither level touched stays unresolved",
      r["resolved"] is False and r["outcome"] == "pending", r)

check("1-10 empty path is safe",
      resolve_from_path([], "long", 100, 98, 104)["resolved"] is False)
check("1-11 missing levels are safe",
      resolve_from_path(path([100]), "long", 100, None, None)["resolved"] is False)


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 2 — excursions and entry touch")
# ══════════════════════════════════════════════════════════════════════════════

r = resolve_from_path(path([100, 103, 99, 104.5]), "long", 100, 98, 104)
check("2-1 best move recorded", r["mfe_pct"] == 4.5, r["mfe_pct"])
check("2-2 worst move recorded", r["mae_pct"] == -1.0, r["mae_pct"])

r = resolve_from_path(path([100, 97, 96]), "short", 100, 103, 94)
check("2-3 for a short, price falling is favourable",
      r["mfe_pct"] == 4.0, r["mfe_pct"])

r = resolve_from_path(path([101, 102, 103]), "long", 100, 98, 104)
check("2-4 entry never traded is reported",
      r["entry_touched"] is False, r)
r = resolve_from_path(path([101, 99.5, 103]), "long", 100, 98, 104)
check("2-5 entry traded is reported", r["entry_touched"] is True, r)


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 3 — result in R")
# ══════════════════════════════════════════════════════════════════════════════

check("3-1 long win of two risk units", result_in_r("long", 100, 98, 104) == 2.0)
check("3-2 long loss is minus one", result_in_r("long", 100, 98, 98) == -1.0)
check("3-3 short win of two risk units", result_in_r("short", 100, 103, 94) == 2.0)
check("3-4 short loss is minus one", result_in_r("short", 100, 103, 103) == -1.0)
check("3-5 partial move", result_in_r("long", 100, 98, 101) == 0.5)
check("3-6 zero risk returns nothing rather than dividing by zero",
      result_in_r("long", 100, 100, 105) is None)
check("3-7 junk input is safe", result_in_r("long", None, None, None) is None)


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 4 — resolving a stored alert from real candles")
# ══════════════════════════════════════════════════════════════════════════════

def make_alert(symbol, direction="long", entry=100, stop=98, tp=104,
               created=None, status="sent", msg_id="900"):
    a = A(user_id=UID, alert_key=f"k-{symbol}-{created or NOW}",
          symbol=symbol, exchange="binance", direction=direction,
          timeframe="4h", alert_kind="in_zone", tier="A",
          modules_json=json.dumps(["ob", "bias_shift"]), confluence_count=2,
          entry_price=entry, stop_loss=stop, take_profit_1=tp,
          ai_confidence=80, delivery_status=status,
          telegram_message_id=msg_id, created_at=created or NOW,
          outcome="pending")
    db.session.add(a); db.session.commit()
    return a

def add_candles(symbol, prices, start=NOW):
    base = int(start.timestamp() * 1000)
    for i, p in enumerate(prices):
        db.session.add(FC(symbol=symbol, timeframe="1m",
                          candle_open_ms=base + i * 60_000, price_close=p,
                          buy_vol_usd=0, sell_vol_usd=0, delta_usd=0,
                          cvd_usd=0, tick_count=1))
    db.session.commit()

with main.app.app_context():
    st = get_or_create_signal_settings(UID)
    st.delivery_enabled = False
    db.session.commit()

    a = make_alert("WINUSDT")
    add_candles("WINUSDT", [100, 101, 103, 105])
    res = resolve_alert(a, settings=st, now=NOW + timedelta(hours=1))
    check("4-1 a winning alert resolves to target", res["outcome"] == "target_hit", res)
    check("4-2 result in R stored", a.result_r == 2.5, a.result_r)
    check("4-3 outcome time stamped", a.outcome_at is not None)

    a2 = make_alert("LOSEUSDT", created=NOW)
    add_candles("LOSEUSDT", [100, 99, 97])
    res = resolve_alert(a2, settings=st, now=NOW + timedelta(hours=1))
    check("4-4 a losing alert resolves to stop", res["outcome"] == "stop_hit", res)
    check("4-5 loss recorded as negative R", a2.result_r < 0, a2.result_r)

    a3 = make_alert("OPENUSDT", created=NOW)
    add_candles("OPENUSDT", [100, 101, 102])
    res = resolve_alert(a3, settings=st, now=NOW + timedelta(hours=1))
    check("4-6 an unresolved alert stays open", res["outcome"] == "pending", res)
    check("4-7 excursions still recorded while open", a3.mfe_pct == 2.0, a3.mfe_pct)

    a4 = make_alert("STALEUSDT", created=NOW)
    add_candles("STALEUSDT", [100, 101])
    res = resolve_alert(a4, settings=st,
                        now=NOW + timedelta(hours=MAX_TRACK_HOURS + 1))
    check("4-8 an old unresolved alert expires", res["outcome"] == "expired", res)

    a5 = make_alert("FRESHUSDT", created=NOW)
    res = resolve_alert(a5, settings=st, now=NOW + timedelta(seconds=30))
    check("4-9 a brand new alert is not judged yet", res["reason"] == "too_fresh", res)

    a6 = A(user_id=UID, alert_key="k-nolevels", symbol="NOLVLUSDT",
           direction="long", entry_price=100, stop_loss=None,
           take_profit_1=None, delivery_status="shadow",
           created_at=NOW, outcome="pending")
    db.session.add(a6); db.session.commit()
    res = resolve_alert(a6, settings=st,
                        now=NOW + timedelta(hours=MAX_TRACK_HOURS + 1))
    check("4-10 an alert with no levels expires rather than hanging forever",
          res["outcome"] == "expired" and a6.outcome_reason == "no_levels_to_track",
          (res, a6.outcome_reason))


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 5 — follow-up messages")
# ══════════════════════════════════════════════════════════════════════════════

class _Resp:
    def __init__(self, p): self._p = p; self.status_code = 200
    def json(self): return self._p

_calls = []
def _capture(url, **kw):
    _calls.append(kw.get("json") or {})
    return _Resp({"ok": True, "result": {"message_id": 1234}})

with main.app.app_context():
    st = get_or_create_signal_settings(UID)
    st.delivery_enabled = True
    st.telegram_bot_token = GOOD_TOKEN
    st.telegram_chat_id = "555"
    db.session.commit()

    _calls.clear()
    a = make_alert("FUPUSDT", created=NOW, status="sent", msg_id="900")
    add_candles("FUPUSDT", [100, 102, 105])
    with patch("live_monitor.telegram_notify.requests.post", side_effect=_capture):
        res = resolve_alert(a, settings=st, now=NOW + timedelta(hours=1))
    check("5-1 a resolved sent alert gets a follow-up",
          res.get("followup_sent") is True, res)
    check("5-2 follow-up threaded onto the original message",
          _calls and _calls[0].get("reply_parameters", {}).get("message_id") == 900,
          _calls[:1])
    body = _calls[0]["text"] if _calls else ""
    check("5-3 follow-up names the coin and result",
          "FUPUSDT" in body and "target" in body.lower(), body)
    check("5-4 follow-up shows the R result", "R" in body, body)

    # Resolving again must not send a second follow-up.
    _calls.clear()
    with patch("live_monitor.telegram_notify.requests.post", side_effect=_capture):
        resolve_alert(a, settings=st, now=NOW + timedelta(hours=2))
    check("5-5 no duplicate follow-up on re-resolve", len(_calls) == 0, len(_calls))

    # A shadow alert was never sent, so there is no thread to reply into.
    _calls.clear()
    a2 = make_alert("SHDWUSDT", created=NOW, status="shadow", msg_id=None)
    add_candles("SHDWUSDT", [100, 102, 105])
    with patch("live_monitor.telegram_notify.requests.post", side_effect=_capture):
        res = resolve_alert(a2, settings=st, now=NOW + timedelta(hours=1))
    check("5-6 a recorded-only alert still resolves",
          res["outcome"] == "target_hit", res)
    check("5-7 but sends no follow-up", len(_calls) == 0, len(_calls))

    # A failing follow-up must not lose the resolution.
    _calls.clear()
    import requests as _rq
    a3 = make_alert("FUPFAILUSDT", created=NOW, status="sent", msg_id="901")
    add_candles("FUPFAILUSDT", [100, 102, 105])
    with patch("live_monitor.telegram_notify.requests.post",
               side_effect=_rq.exceptions.Timeout()):
        res = resolve_alert(a3, settings=st, now=NOW + timedelta(hours=1))
    check("5-8 outcome is saved even when the follow-up fails",
          a3.outcome == "target_hit", a3.outcome)
    check("5-9 follow-up marked as not sent", a3.followup_sent_at is None)


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 6 — batch tracking")
# ══════════════════════════════════════════════════════════════════════════════

with main.app.app_context():
    A.query.delete(); FC.query.delete(); db.session.commit()
    st = get_or_create_signal_settings(UID)
    st.delivery_enabled = False
    db.session.commit()

    make_alert("B1USDT", created=NOW); add_candles("B1USDT", [100, 103, 105])
    make_alert("B2USDT", created=NOW); add_candles("B2USDT", [100, 99, 97])
    make_alert("B3USDT", created=NOW); add_candles("B3USDT", [100, 101])

    stats = track_open_alerts(UID, now=NOW + timedelta(hours=1))
    check("6-1 all three checked", stats["checked"] == 3, stats)
    check("6-2 one win, one loss, one still open",
          stats["target_hit"] == 1 and stats["stop_hit"] == 1
          and stats["still_open"] == 1, stats)

    stats2 = track_open_alerts(UID, now=NOW + timedelta(hours=2))
    check("6-3 already-resolved alerts are not re-checked",
          stats2["checked"] == 1, stats2)


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 7 — measured accuracy replaces the guesses")
# ══════════════════════════════════════════════════════════════════════════════

with main.app.app_context():
    A.query.delete(); db.session.commit()

    def resolved(symbol, mods, outcome, r, tier="A", conf=2):
        db.session.add(A(user_id=UID, alert_key=f"acc-{symbol}", symbol=symbol,
                         direction="long", modules_json=json.dumps(mods),
                         confluence_count=conf, tier=tier, outcome=outcome,
                         result_r=r, delivery_status="sent", created_at=NOW))

    for i in range(3):
        resolved(f"W{i}", ["ob", "bias_shift"], "target_hit", 2.0)
    resolved("L1", ["ob"], "stop_hit", -1.0, tier="B", conf=1)
    resolved("L2", ["accumulation"], "stop_hit", -1.0, tier="B", conf=1)
    db.session.commit()

    acc = module_accuracy(UID, days=90, now=NOW + timedelta(hours=1))
    check("7-1 accuracy computes", acc["ok"] is True, acc)
    check("7-2 five resolved alerts counted", acc["resolved_total"] == 5)

    ob = acc["per_module"].get("ob")
    check("7-3 order block counted in every alert it contributed to",
          ob["resolved"] == 4 and ob["wins"] == 3, ob)
    check("7-4 win rate computed", ob["win_rate"] == 75.0, ob["win_rate"])
    check("7-5 total R computed", ob["total_r"] == 5.0, ob["total_r"])

    accum = acc["per_module"].get("accumulation")
    check("7-6 a losing module shows a zero win rate",
          accum["win_rate"] == 0.0 and accum["wins"] == 0, accum)

    check("7-7 small samples are flagged as unreliable, not hidden",
          ob["reliable"] is False, ob)

    check("7-8 tier A beats tier B in this data",
          acc["per_tier"]["A"]["win_rate"] > acc["per_tier"]["B"]["win_rate"],
          acc["per_tier"])
    check("7-9 confluence broken out too",
          "2" in acc["per_confluence"] and "1" in acc["per_confluence"],
          list(acc["per_confluence"]))

    empty = module_accuracy(UID + 999, days=90, now=NOW)
    check("7-10 a user with no history returns empty, not an error",
          empty["ok"] is True and empty["resolved_total"] == 0, empty)


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 8 — the routes")
# ══════════════════════════════════════════════════════════════════════════════

client = main.app.test_client()
with client.session_transaction() as s:
    s["logged_in"] = True; s["username"] = "outc"

r = client.get("/api/live-monitor/signal-accuracy?days=30")
check("8-1 accuracy route returns 200", r.status_code == 200, r.status_code)
check("8-2 buckets present", "per_module" in (r.get_json() or {}), r.get_json())

r = client.post("/api/live-monitor/signal-alerts/track")
check("8-3 manual tracking returns 200", r.status_code == 200, r.status_code)

anon = main.app.test_client()
check("8-4 unauthenticated accuracy blocked",
      anon.get("/api/live-monitor/signal-accuracy").status_code in (302, 401))


print(f"\n{'='*60}")
print(f"  TOTAL: {_pass+_fail}   PASS: {_pass}   FAIL: {_fail}")
print(f"{'='*60}")
if _fail:
    sys.exit(1)
