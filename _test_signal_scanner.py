"""Phase SIG-8 — background scanning with no browser involved.

The gap this closes: every scan lived behind a button, so a closed browser
meant nothing was ever found. The important test here is the end-to-end one —
call the scanner with no request context at all and prove candidate rows
appear.

Run: python3 _test_signal_scanner.py
"""
import os, sys, types, tempfile, json

_dbf = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False).name
os.environ["DATABASE_URL"] = f"sqlite:///{_dbf}"
os.environ["SECRET_KEY"]   = "test_secret_signal_scanner"
os.environ["ZYNI_SIG_PROMOTION_ENABLED"] = "0"
os.environ["ZYNI_SIG_SCAN_ENABLED"]      = "0"   # no loop during tests
os.environ["ZYNI_SIG_SCAN_PAIRS"]        = "7"
os.environ.setdefault("RESEND_API_KEY",  "test")
os.environ.setdefault("TURNSTILE_SECRET", "")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
for _mn in ["psycopg2", "psycopg2.extras", "resend"]:
    if _mn not in sys.modules:
        sys.modules[_mn] = types.ModuleType(_mn)

from unittest.mock import patch                                    # noqa: E402
import main                                                        # noqa: E402
from models import db, User, LiveMonitorSignalCandidate as C       # noqa: E402
from live_monitor.signal_scanner import (                          # noqa: E402
    run_one_scan, run_scan_cycle, run_all_scans_once, scanner_state,
    _scan_runner_identity, _payload_for, pairs_per_cycle, SCAN_SEQUENCE,
)
from live_monitor.signal_settings import get_or_create_signal_settings  # noqa: E402

with main.app.app_context():
    db.create_all()
    _admin = User(username="boss", password_hash="x", role="admin", status="active")
    _plain = User(username="plain", password_hash="x", role="user", status="active")
    db.session.add_all([_admin, _plain]); db.session.commit()
    ADMIN_ID, PLAIN_ID = _admin.id, _plain.id

_pass = 0; _fail = 0
def check(name, cond, info=""):
    global _pass, _fail
    if cond: print(f"  PASS  {name}"); _pass += 1
    else:    print(f"  FAIL  {name}" + (f"  [{info}]" if info else "")); _fail += 1
def section(t): print(f"\n── {t} ──")

_ALERT = {"type": "ob", "side": "bullish", "zoneHigh": 101, "zoneLow": 99,
          "score": 77, "strength": 77}
_FAKE_RESULT = {"symbol": "AUTOUSDT", "price": 100, "timeframe": "4h",
                "topAlert": _ALERT, "topAlerts": [_ALERT]}
_CANDLES = [{"open": 1, "high": 1, "low": 1, "close": 1, "volume": 1}] * 60


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 1 — nothing runs until someone wants signals")
# ══════════════════════════════════════════════════════════════════════════════

with main.app.app_context():
    check("1-1 no enabled user means no scan identity",
          _scan_runner_identity() is None)
    res = run_scan_cycle()
    check("1-2 a cycle with nobody enabled is skipped, not an error",
          res.get("ok") is True and res.get("skipped") == "no_enabled_user", res)

    st = get_or_create_signal_settings(PLAIN_ID)
    st.enabled = True
    db.session.commit()
    ident = _scan_runner_identity()
    check("1-3 an enabled user becomes the scan identity",
          ident and ident["username"] == "plain", ident)

    st_admin = get_or_create_signal_settings(ADMIN_ID)
    st_admin.enabled = True
    db.session.commit()
    ident = _scan_runner_identity()
    check("1-4 an admin is preferred, so scans do not spend a user's tokens",
          ident and ident["username"] == "boss" and ident["admin"] is True, ident)


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 2 — payloads sweep the market rather than one fixed batch")
# ══════════════════════════════════════════════════════════════════════════════

url, body = _payload_for("scan")
check("2-1 main scan targets the right endpoint", url == "/api/scan", url)
check("2-2 it scans the whole market, not a hand-picked list",
      body["scanMode"] == "market", body)
check("2-3 round robin on, so each run continues where the last stopped",
      body["roundRobin"] is True)
check("2-4 batch size comes from config", body["pairsPerCycle"] == 7,
      body["pairsPerCycle"])

for kind in SCAN_SEQUENCE:
    u, b = _payload_for(kind)
    check(f"2-5 {kind} has a route and a body", bool(u) and isinstance(b, dict),
          (kind, u))

check("2-6 an unknown scan type is refused", _payload_for("nonsense") == (None, None))
check("2-7 batch size is clamped to something sane", 5 <= pairs_per_cycle() <= 120)


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 3 — a scan really runs with NO browser and NO request")
# ══════════════════════════════════════════════════════════════════════════════

with main.app.app_context():
    C.query.delete(); db.session.commit()

# Note there is no test_client and no request context here — this is called
# exactly the way the background thread calls it.
with patch.object(main, "analyze_pair", return_value=_FAKE_RESULT), \
     patch.object(main, "get_klines_exchange", return_value=_CANDLES), \
     patch.object(main, "get_pairs_exchange",
                  return_value=[{"symbol": "AUTOUSDT"}]):
    res = run_one_scan("scan")

check("3-1 the scan completed", res.get("ok") is True, res)
check("3-2 it ran as the admin account", res.get("as_user") == "boss", res)
check("3-3 elapsed time reported", res.get("elapsed_sec") is not None)

with main.app.app_context():
    rows = C.query.filter_by(symbol="AUTOUSDT").all()
check("3-4 a candidate was stored with no browser involved", len(rows) == 1,
      [(r.module, r.direction) for r in rows])
if rows:
    check("3-5 stored with the right module and direction",
          rows[0].module == "ob" and rows[0].direction == "long",
          (rows[0].module, rows[0].direction))

# Running again in the same candle must not duplicate.
with patch.object(main, "analyze_pair", return_value=_FAKE_RESULT), \
     patch.object(main, "get_klines_exchange", return_value=_CANDLES), \
     patch.object(main, "get_pairs_exchange",
                  return_value=[{"symbol": "AUTOUSDT"}]):
    run_one_scan("scan")
with main.app.app_context():
    check("3-6 re-scanning the same candle does not duplicate rows",
          C.query.filter_by(symbol="AUTOUSDT").count() == 1)


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 4 — rotation spreads the load")
# ══════════════════════════════════════════════════════════════════════════════

seen = []
def _record_kind(kind, identity=None, **kw):
    seen.append(kind)
    return {"ok": True, "kind": kind, "elapsed_sec": 0.1}

with patch("live_monitor.signal_scanner.run_one_scan", side_effect=_record_kind):
    with main.app.app_context():
        for _ in range(len(SCAN_SEQUENCE) * 2):
            run_scan_cycle()

check("4-1 one scan type per cycle", len(seen) == len(SCAN_SEQUENCE) * 2, len(seen))
check("4-2 every scan type gets covered", set(seen) == set(SCAN_SEQUENCE), set(seen))
check("4-3 the rotation repeats in order",
      seen[:len(SCAN_SEQUENCE)] == seen[len(SCAN_SEQUENCE):], seen)

seen.clear()
with patch("live_monitor.signal_scanner.run_one_scan", side_effect=_record_kind):
    with main.app.app_context():
        run_scan_cycle(force_kind="bias")
check("4-4 a forced scan type overrides the rotation", seen == ["bias"], seen)

# Pairs a user pinned must be scanned directly — waiting for the market
# sweep to reach them could take hours.
targeted = []
def _capture_symbols(kind, identity=None, symbols=None, **kw):
    targeted.append({"kind": kind, "symbols": list(symbols or [])})
    return {"ok": True, "kind": kind, "elapsed_sec": 0.1}

with main.app.app_context():
    st = get_or_create_signal_settings(ADMIN_ID)
    st.enabled = True
    st.coin_scope = "selected_pairs"
    db.session.commit()

with patch("live_monitor.signal_scanner.run_one_scan", side_effect=_capture_symbols), \
     patch.object(main, "load_user_watchlist", return_value=["MYPAIRUSDT"]):
    with main.app.app_context():
        res = run_scan_cycle(force_kind="scan")

check("4-5 a pinned pair gets its own targeted scan",
      any(t["symbols"] == ["MYPAIRUSDT"] for t in targeted), targeted)
check("4-6 the market sweep still runs alongside it",
      any(not t["symbols"] for t in targeted), targeted)
check("4-7 the cycle reports how many pinned pairs it covered",
      res.get("chosen_pairs_scanned") == 1, res)

with main.app.app_context():
    st = get_or_create_signal_settings(ADMIN_ID)
    st.coin_scope = "top_volume"
    db.session.commit()


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 5 — failures stay contained")
# ══════════════════════════════════════════════════════════════════════════════

with patch.object(main, "get_pairs_exchange", side_effect=RuntimeError("exchange down")):
    res = run_one_scan("scan")
check("5-1 a broken scan returns a failure instead of raising",
      res.get("ok") is False, res)
check("5-2 the failure is described", bool(res.get("error") or res.get("reason")), res)

with patch("live_monitor.signal_scanner._m.app.test_client",
           side_effect=RuntimeError("boom")):
    res = run_one_scan("scan")
check("5-3 a crash inside the runner is caught",
      res.get("ok") is False and "crashed" in str(res.get("reason")), res)

with main.app.app_context():
    state = scanner_state()
check("5-4 state records the last result", "last_result" in state, list(state))
check("5-5 state exposes the rotation and cadence",
      state.get("sequence") == list(SCAN_SEQUENCE) and state.get("cycle_sec") > 0,
      state)


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 6 — the manual scan and status routes")
# ══════════════════════════════════════════════════════════════════════════════

client = main.app.test_client()
with client.session_transaction() as s:
    s["logged_in"] = True; s["username"] = "boss"

r = client.get("/api/live-monitor/signal-scan/status")
check("6-1 status route returns 200", r.status_code == 200, r.status_code)
body = (r.get_json() or {}).get("scanner") or {}
check("6-2 status says whether the thread is alive", "thread_alive" in body, body)
check("6-3 status reports how many setups are stored",
      "candidates_stored" in body, body)

with patch("live_monitor.signal_scanner.run_one_scan",
           return_value={"ok": True, "kind": "bias"}):
    r = client.post("/api/live-monitor/signal-scan/run", json={"kind": "bias"})
check("6-4 a single scan can be triggered by hand", r.status_code == 200, r.status_code)

r = client.post("/api/live-monitor/signal-scan/run", json={"kind": "not_a_scan"})
check("6-5 an unknown scan type is rejected", r.status_code == 400, r.status_code)
check("6-6 the allowed list is returned to help",
      "allowed" in (r.get_json() or {}), r.get_json())

with patch("live_monitor.signal_scanner.run_one_scan",
           return_value={"ok": True, "kind": "x"}):
    r = client.post("/api/live-monitor/signal-scan/run", json={})
check("6-7 scanning everything at once works", r.status_code == 200, r.status_code)
check("6-8 every scan type is reported back",
      len((r.get_json() or {}).get("scans") or {}) == len(SCAN_SEQUENCE),
      r.get_json())

anon = main.app.test_client()
check("6-9 unauthenticated scan blocked",
      anon.post("/api/live-monitor/signal-scan/run").status_code in (302, 401))
check("6-10 unauthenticated status blocked",
      anon.get("/api/live-monitor/signal-scan/status").status_code in (302, 401))


print(f"\n{'='*60}")
print(f"  TOTAL: {_pass+_fail}   PASS: {_pass}   FAIL: {_fail}")
print(f"{'='*60}")
if _fail:
    sys.exit(1)
