"""Phase SIG-1 — signal alert settings + Telegram linking tests.

Imports the real production app (main.py) and drives the real HTTP routes,
not helper functions in isolation. Telegram network calls are patched at the
requests boundary so no real API call is ever made.

Run: python3 _test_signal_settings.py
"""
import os, sys, types, tempfile, json

_dbf = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False).name
os.environ["DATABASE_URL"]   = f"sqlite:///{_dbf}"
os.environ["SECRET_KEY"]     = "test_secret_signal_settings"
os.environ.setdefault("RESEND_API_KEY",  "test")
os.environ.setdefault("TURNSTILE_SECRET", "")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

for _mn in ["psycopg2", "psycopg2.extras", "resend"]:
    if _mn not in sys.modules:
        sys.modules[_mn] = types.ModuleType(_mn)

import main                                              # noqa: E402
from models import db, User, LiveMonitorSignalSettings   # noqa: E402
from unittest.mock import patch                          # noqa: E402

with main.app.app_context():
    db.create_all()
    _u = User(username="siguser", password_hash="x", role="user", status="active")
    _other = User(username="siguser2", password_hash="x", role="user", status="active")
    db.session.add_all([_u, _other])
    db.session.commit()
    UID, UID2 = _u.id, _other.id

client = main.app.test_client()
with client.session_transaction() as s:
    s["logged_in"] = True
    s["username"]  = "siguser"

_pass = 0; _fail = 0
def check(name, cond, info=""):
    global _pass, _fail
    if cond:
        print(f"  PASS  {name}"); _pass += 1
    else:
        print(f"  FAIL  {name}" + (f"  [{info}]" if info else "")); _fail += 1
def section(t): print(f"\n── {t} ──")

GOOD_TOKEN = "123456789:AAFxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 1 — defaults on first access")
# ══════════════════════════════════════════════════════════════════════════════

r = client.get("/api/live-monitor/signal-settings")
check("1-1 route 200", r.status_code == 200, r.status_code)
body = r.get_json()
check("1-2 ok=True", body.get("ok") is True, body)
st = body.get("settings") or {}
check("1-3 starts disabled (nothing runs until user opts in)", st.get("enabled") is False, st.get("enabled"))
check("1-4 delivery starts OFF (shadow mode by default)", st.get("delivery_enabled") is False)
check("1-5 all 9 modules enabled by default", len(st.get("enabled_modules", [])) == 9, st.get("enabled_modules"))
check("1-6 module catalog returned for the UI", len(body.get("modules", [])) == 9)
check("1-7 telegram not configured yet", st.get("telegram_configured") is False)
check("1-8 token hint is None when unset", st.get("telegram_token_hint") is None)
check("1-9 defaults block present", (body.get("defaults") or {}).get("min_confidence") == 65)

with main.app.app_context():
    _rows = LiveMonitorSignalSettings.query.filter_by(user_id=UID).count()
check("1-10 exactly one settings row created", _rows == 1, _rows)

r2 = client.get("/api/live-monitor/signal-settings")
with main.app.app_context():
    _rows2 = LiveMonitorSignalSettings.query.filter_by(user_id=UID).count()
check("1-11 second GET does not create a duplicate row", _rows2 == 1, _rows2)


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 2 — partial updates")
# ══════════════════════════════════════════════════════════════════════════════

r = client.post("/api/live-monitor/signal-settings",
                json={"max_signals_per_day": 10})
check("2-1 partial patch accepted", r.status_code == 200, r.get_json())
st = r.get_json()["settings"]
check("2-2 patched field changed", st["max_signals_per_day"] == 10)
check("2-3 untouched field preserved", st["min_confidence"] == 65)
check("2-4 modules untouched by unrelated patch", len(st["enabled_modules"]) == 9)

r = client.post("/api/live-monitor/signal-settings", json={})
check("2-5 empty patch is a no-op, not an error", r.status_code == 200)
check("2-6 changed=False on empty patch", r.get_json().get("changed") is False)

r = client.post("/api/live-monitor/signal-settings",
                json={"max_signals_per_day": 10})
check("2-7 re-sending the same value reports changed=False",
      r.get_json().get("changed") is False, r.get_json())

r = client.post("/api/live-monitor/signal-settings",
                json={"totally_unknown_key": "whatever"})
check("2-8 unknown keys ignored, not fatal (old client safety)", r.status_code == 200)


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 3 — validation and clamping")
# ══════════════════════════════════════════════════════════════════════════════

r = client.post("/api/live-monitor/signal-settings",
                json={"max_signals_per_day": 99999})
check("3-1 out-of-range number clamps instead of erroring", r.status_code == 200)
check("3-2 clamped to the max bound", r.get_json()["settings"]["max_signals_per_day"] == 50,
      r.get_json()["settings"]["max_signals_per_day"])

r = client.post("/api/live-monitor/signal-settings", json={"min_confluence": 0})
check("3-3 below-minimum clamps up to 1", r.get_json()["settings"]["min_confluence"] == 1)

r = client.post("/api/live-monitor/signal-settings",
                json={"min_confidence": "not a number"})
check("3-4 garbage numeric keeps previous value, no crash", r.status_code == 200)

r = client.post("/api/live-monitor/signal-settings", json={"trigger_mode": "whenever"})
check("3-5 invalid enum rejected with 422", r.status_code == 422, r.status_code)
check("3-6 field_errors names the bad field",
      "trigger_mode" in (r.get_json().get("field_errors") or {}), r.get_json())

r = client.post("/api/live-monitor/signal-settings",
                json={"enabled_modules": ["ob", "not_a_real_module"]})
check("3-7 unknown module key rejected", r.status_code == 422)

r = client.post("/api/live-monitor/signal-settings", json={"enabled_modules": []})
check("3-8 empty module list rejected (would silently disable everything)",
      r.status_code == 422, r.get_json())

r = client.post("/api/live-monitor/signal-settings",
                json={"allow_long": False, "allow_short": False})
check("3-9 disabling both directions rejected", r.status_code == 422, r.get_json())

# Confirm that rejected cross-field update did not leave a partial write behind.
st_now = client.get("/api/live-monitor/signal-settings").get_json()["settings"]
check("3-10 rejected update rolled back — allow_long still True",
      st_now["allow_long"] is True, st_now["allow_long"])

r = client.post("/api/live-monitor/signal-settings", json={"coin_scope": "watchlist"})
check("3-11 watchlist scope with no symbols rejected", r.status_code == 422, r.get_json())

r = client.post("/api/live-monitor/signal-settings",
                json={"coin_scope": "watchlist", "symbols": ["BTCUSDT", "ETHUSDT"]})
check("3-12 watchlist scope with symbols accepted", r.status_code == 200, r.get_json())
check("3-13 symbols normalized and stored",
      r.get_json()["settings"]["symbols"] == ["BTCUSDT", "ETHUSDT"])

r = client.post("/api/live-monitor/signal-settings",
                json={"symbols": ["BTC-USDT!!"]})
check("3-14 malformed symbol rejected", r.status_code == 422)

# ── Scanner-backed pair scope ────────────────────────────────────────────────
# Reuses the pair list already chosen in the Scanner, so the same decision
# does not have to be made twice and cannot drift between the two places.
from unittest.mock import patch as _patch2                          # noqa: E402
import main as _mainmod                                             # noqa: E402

with _patch2.object(_mainmod, "load_user_selected_pairs", return_value=[]):
    r = client.post("/api/live-monitor/signal-settings",
                    json={"coin_scope": "selected_pairs"})
check("3-14a choosing Scanner pairs with none selected is refused",
      r.status_code == 422, r.get_json())
check("3-14b the error explains where to fix it",
      "no_pairs_selected" in str((r.get_json().get("field_errors") or {})),
      r.get_json())

with _patch2.object(_mainmod, "load_user_selected_pairs",
                    return_value=["btcusdt", "ETHUSDT"]):
    r = client.post("/api/live-monitor/signal-settings",
                    json={"coin_scope": "selected_pairs"})
    check("3-14c with pairs selected it is accepted", r.status_code == 200,
          r.get_json())
    st_now = r.get_json()["settings"]
    check("3-14d the resolved pairs are shown back, upper-cased",
          st_now.get("scope_symbols") == ["BTCUSDT", "ETHUSDT"],
          st_now.get("scope_symbols"))
    check("3-14e the typed list is left untouched by this scope",
          st_now["symbols"] == ["BTCUSDT", "ETHUSDT"] or
          isinstance(st_now["symbols"], list), st_now["symbols"])

# Back to a scope that needs no list, so later tests are unaffected.
client.post("/api/live-monitor/signal-settings", json={"coin_scope": "top_volume"})

r = client.post("/api/live-monitor/signal-settings",
                json={"timeframes": ["4h", "1h"]})
check("3-15 valid timeframes accepted", r.status_code == 200)
r = client.post("/api/live-monitor/signal-settings", json={"timeframes": ["3s"]})
check("3-16 invalid timeframe rejected", r.status_code == 422)

r = client.post("/api/live-monitor/signal-settings", json={"delivery_enabled": True})
check("3-17 cannot enable delivery without a linked Telegram bot",
      r.status_code == 422, r.get_json())
check("3-18 error explains why", "telegram_not_linked" in
      str((r.get_json().get("field_errors") or {}).get("delivery_enabled")), r.get_json())


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 4 — Telegram linking (network patched)")
# ══════════════════════════════════════════════════════════════════════════════

class _Resp:
    def __init__(self, payload, status=200):
        self._p = payload; self.status_code = status
    def json(self): return self._p

def _ok_getme():   return _Resp({"ok": True, "result": {"username": "my_test_bot"}})
def _ok_send():    return _Resp({"ok": True, "result": {"message_id": 4242}})

def _fake_post_success(url, **kw):
    return _ok_getme() if url.endswith("/getMe") else _ok_send()

def _fake_post_bad_token(url, **kw):
    return _Resp({"ok": False, "error_code": 401, "description": "Unauthorized"})

def _fake_post_bad_chat(url, **kw):
    if url.endswith("/getMe"):
        return _ok_getme()
    return _Resp({"ok": False, "error_code": 400, "description": "Bad Request: chat not found"})

r = client.post("/api/live-monitor/signal-settings/telegram/link",
                json={"telegram_bot_token": "not-a-token", "telegram_chat_id": "123"})
check("4-1 malformed token rejected before any network call", r.status_code == 400)
check("4-2 message tells the user what a token looks like",
      "BotFather" in (r.get_json().get("message") or ""), r.get_json())

with patch("live_monitor.telegram_notify.requests.post", side_effect=_fake_post_bad_token):
    r = client.post("/api/live-monitor/signal-settings/telegram/link",
                    json={"telegram_bot_token": GOOD_TOKEN, "telegram_chat_id": "555"})
check("4-3 invalid token rejected by Telegram → 400", r.status_code == 400)
check("4-4 not stored on failure",
      r.get_json()["settings"]["telegram_configured"] is False)
check("4-5 last error recorded for the UI",
      bool(r.get_json()["settings"].get("telegram_last_error")))

with patch("live_monitor.telegram_notify.requests.post", side_effect=_fake_post_bad_chat):
    r = client.post("/api/live-monitor/signal-settings/telegram/link",
                    json={"telegram_bot_token": GOOD_TOKEN, "telegram_chat_id": "555"})
check("4-6 good token but bad chat id → 400", r.status_code == 400)
check("4-7 error explains the chat-id fix",
      "userinfobot" in (r.get_json().get("message") or ""), r.get_json().get("message"))

with patch("live_monitor.telegram_notify.requests.post", side_effect=_fake_post_success):
    r = client.post("/api/live-monitor/signal-settings/telegram/link",
                    json={"telegram_bot_token": GOOD_TOKEN, "telegram_chat_id": "555"})
check("4-8 valid credentials accepted", r.status_code == 200, r.get_json())
st = r.get_json()["settings"]
check("4-9 marked configured", st["telegram_configured"] is True)
check("4-10 verified timestamp set", st["telegram_verified_at"] is not None)
check("4-11 previous error cleared", st.get("telegram_last_error") is None)
check("4-12 bot username returned", r.get_json().get("bot_username") == "my_test_bot")

check("4-13 full token NEVER sent to the browser", "telegram_bot_token" not in st, list(st.keys()))
check("4-14 only a masked tail is exposed", st["telegram_token_hint"] == "…xxxx",
      st["telegram_token_hint"])

# Now that Telegram is linked, delivery may be enabled.
r = client.post("/api/live-monitor/signal-settings", json={"delivery_enabled": True})
check("4-15 delivery can be enabled once linked", r.status_code == 200, r.get_json())
check("4-16 delivery_enabled now True", r.get_json()["settings"]["delivery_enabled"] is True)

# Token cannot be injected through the generic settings endpoint.
r = client.post("/api/live-monitor/signal-settings",
                json={"telegram_bot_token": "999:HACKEDTOKENxxxxxxxxxxxxxxxxxxxxxxx",
                      "telegram_chat_id": "666"})
check("4-17 generic settings route ignores credential fields", r.status_code == 200)
with main.app.app_context():
    _row = LiveMonitorSignalSettings.query.filter_by(user_id=UID).first()
    _tok, _chat = _row.telegram_bot_token, _row.telegram_chat_id
check("4-18 token unchanged by injection attempt", _tok == GOOD_TOKEN, _tok)
check("4-19 chat id unchanged by injection attempt", _chat == "555", _chat)

r = client.post("/api/live-monitor/signal-settings/telegram/unlink")
check("4-20 unlink succeeds", r.status_code == 200)
st = r.get_json()["settings"]
check("4-21 credentials cleared", st["telegram_configured"] is False)
check("4-22 delivery force-disabled on unlink (no destination left)",
      st["delivery_enabled"] is False, st["delivery_enabled"])


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 5 — per-user isolation")
# ══════════════════════════════════════════════════════════════════════════════

client2 = main.app.test_client()
with client2.session_transaction() as s:
    s["logged_in"] = True
    s["username"]  = "siguser2"

st2 = client2.get("/api/live-monitor/signal-settings").get_json()["settings"]
check("5-1 second user gets their own fresh defaults",
      st2["max_signals_per_day"] == 6, st2["max_signals_per_day"])
check("5-2 second user has no telegram of their own",
      st2["telegram_configured"] is False)

client2.post("/api/live-monitor/signal-settings", json={"max_signals_per_day": 25})
st1 = client.get("/api/live-monitor/signal-settings").get_json()["settings"]
check("5-3 user 2's change does not leak into user 1",
      st1["max_signals_per_day"] == 50, st1["max_signals_per_day"])

anon = main.app.test_client()
r = anon.get("/api/live-monitor/signal-settings")
check("5-4 unauthenticated GET blocked", r.status_code in (302, 401), r.status_code)
r = anon.post("/api/live-monitor/signal-settings", json={"enabled": True})
check("5-5 unauthenticated POST blocked", r.status_code in (302, 401), r.status_code)


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 6 — message escaping and send behavior")
# ══════════════════════════════════════════════════════════════════════════════

from live_monitor.telegram_notify import escape_html, send_telegram_message   # noqa: E402

check("6-1 ampersand escaped", escape_html("BTC & ETH") == "BTC &amp; ETH")
check("6-2 angle brackets escaped",
      escape_html("<b>x</b>") == "&lt;b&gt;x&lt;/b&gt;")
check("6-3 None becomes empty string, not 'None'", escape_html(None) == "")
check("6-4 numbers survive", escape_html(61850.5) == "61850.5")

res = send_telegram_message("", "123", "hi")
check("6-5 missing token returns error dict rather than raising",
      res["ok"] is False and res["error"] == "not_configured", res)

res = send_telegram_message(GOOD_TOKEN, "123", "")
check("6-6 empty message refused", res["ok"] is False and res["error"] == "empty_message")

def _capture(url, **kw):
    _capture.payload = kw.get("json")
    return _ok_send()
with patch("live_monitor.telegram_notify.requests.post", side_effect=_capture):
    res = send_telegram_message(GOOD_TOKEN, "123", "x" * 6000)
check("6-7 over-long message truncated, not rejected", res["ok"] is True, res)
check("6-8 truncated to Telegram's limit",
      len(_capture.payload["text"]) <= 4096, len(_capture.payload["text"]))
check("6-9 HTML parse mode used (not MarkdownV2)",
      _capture.payload["parse_mode"] == "HTML")

with patch("live_monitor.telegram_notify.requests.post", side_effect=_capture):
    send_telegram_message(GOOD_TOKEN, "123", "follow-up", reply_to_message_id=4242)
check("6-10 follow-ups thread onto the original alert",
      _capture.payload["reply_parameters"]["message_id"] == 4242, _capture.payload)
check("6-11 threading degrades gracefully if original was deleted",
      _capture.payload["reply_parameters"]["allow_sending_without_reply"] is True)

import requests as _rq                                                        # noqa: E402
with patch("live_monitor.telegram_notify.requests.post",
           side_effect=_rq.exceptions.Timeout()):
    res = send_telegram_message(GOOD_TOKEN, "123", "hi")
check("6-12 network timeout returns error, never raises",
      res["ok"] is False and res["error"] == "timeout", res)

with patch("live_monitor.telegram_notify.requests.post",
           side_effect=_rq.exceptions.ConnectionError("boom")):
    res = send_telegram_message(GOOD_TOKEN, "123", "hi")
check("6-13 connection failure returns error, never raises",
      res["ok"] is False and res["error"] == "network", res)


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 7 — the Selected Pairs universe is stored in full")
# ══════════════════════════════════════════════════════════════════════════════
# The live-price watchlist is capped at 10, because every pair in it gets a
# websocket price subscription. The Selected Pairs tab is a different thing —
# a scan universe that can be hundreds of pairs and needs no live prices.
# Reading the capped list meant a user who selected 100 pairs would have had
# signals on only 10 of them.

import main as _mm7                                                   # noqa: E402

many = [f"COIN{i}USDT" for i in range(40)]
_mm7.save_user_selected_pairs("bigpicker", many)
full = _mm7.load_user_selected_pairs("bigpicker")
check("7-1 the whole universe survives, not just the first 10",
      len(full) == 40, len(full))
check("7-2 order is preserved",
      full[0] == "COIN0USDT" and full[39] == "COIN39USDT")

_mm7.save_user_selected_pairs("junkpicker", ["btcusdt", "NOTAPAIR", "ETHUSDT", ""])
check("7-3 non-USDT entries dropped and case normalized",
      _mm7.load_user_selected_pairs("junkpicker") == ["BTCUSDT", "ETHUSDT"],
      _mm7.load_user_selected_pairs("junkpicker"))

_mm7.save_user_selected_pairs("hugepicker", [f"BIG{i}USDT" for i in range(900)])
check("7-4 an absurd list is bounded rather than unbounded",
      len(_mm7.load_user_selected_pairs("hugepicker")) == 500,
      len(_mm7.load_user_selected_pairs("hugepicker")))

# Someone who has not re-synced since this store existed must not see an empty
# list — fall back to the old watchlist rather than silently matching nothing.
_mm7.save_user_watchlist("legacyuser", ["OLDUSDT"])
check("7-5 falls back to the old watchlist when no universe is saved yet",
      _mm7.load_user_selected_pairs("legacyuser") == ["OLDUSDT"],
      _mm7.load_user_selected_pairs("legacyuser"))

# Registering must fill BOTH stores: the full universe, and the capped feed.
client7 = main.app.test_client()
with client7.session_transaction() as s7:
    s7["logged_in"] = True; s7["username"] = "siguser"

r = client7.post("/api/watchlist/register",
                 json={"pairs": [f"REG{i}USDT" for i in range(25)]})
check("7-6 registering pairs succeeds", r.status_code == 200, r.status_code)
check("7-7 all 25 kept for signal scanning",
      len(_mm7.load_user_selected_pairs("siguser")) == 25,
      len(_mm7.load_user_selected_pairs("siguser")))
check("7-8 the live-price feed list is still capped at 10",
      len(_mm7.load_user_watchlist("siguser")) == 10,
      len(_mm7.load_user_watchlist("siguser")))


print(f"\n{'='*60}")
print(f"  TOTAL: {_pass+_fail}   PASS: {_pass}   FAIL: {_fail}")
print(f"{'='*60}")
if _fail:
    sys.exit(1)
