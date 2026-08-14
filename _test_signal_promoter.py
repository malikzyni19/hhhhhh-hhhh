"""Phase SIG-3 — promotion: watching the best coins, under a hard ceiling.

This stage decides the system's running cost, so the tests focus on the two
things that could hurt: watching too many coins, and touching coins the user
added by hand.

Run: python3 _test_signal_promoter.py
"""
import os, sys, types, tempfile, json
from datetime import datetime, timezone, timedelta

_dbf = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False).name
os.environ["DATABASE_URL"]     = f"sqlite:///{_dbf}"
os.environ["SECRET_KEY"]       = "test_secret_signal_promoter"
os.environ["ZYNI_SIG_MAX_WATCHED"]   = "3"      # small ceiling, easy to prove
os.environ["ZYNI_SIG_MIN_WATCH_MIN"] = "60"
os.environ["ZYNI_SIG_PROMOTION_ENABLED"] = "0"  # no background thread in tests
os.environ.setdefault("RESEND_API_KEY",  "test")
os.environ.setdefault("TURNSTILE_SECRET", "")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
for _mn in ["psycopg2", "psycopg2.extras", "resend"]:
    if _mn not in sys.modules:
        sys.modules[_mn] = types.ModuleType(_mn)

from unittest.mock import patch                                   # noqa: E402
import main                                                       # noqa: E402
from models import (db, User, LiveMonitorItem as LMI,             # noqa: E402
                    LiveMonitorSignalCandidate as C)
from live_monitor.signal_promoter import (                        # noqa: E402
    promote_groups, demote_stale_watches, run_promotion_cycle,
    run_promotion_for_all_enabled, max_watched_coins, AUTO_SOURCE_TAB,
)
from live_monitor.signal_settings import get_or_create_signal_settings  # noqa: E402
from live_monitor.signal_intake import adapt_bias, adapt_zone_scan, record_candidates  # noqa: E402

with main.app.app_context():
    db.create_all()
    _u  = User(username="promo",  password_hash="x", role="user", status="active")
    _u2 = User(username="promo2", password_hash="x", role="user", status="active")
    db.session.add_all([_u, _u2]); db.session.commit()
    UID, UID2 = _u.id, _u2.id

_pass = 0; _fail = 0
def check(name, cond, info=""):
    global _pass, _fail
    if cond: print(f"  PASS  {name}"); _pass += 1
    else:    print(f"  FAIL  {name}" + (f"  [{info}]" if info else "")); _fail += 1
def section(t): print(f"\n── {t} ──")

NOW = datetime(2026, 8, 13, 12, 0, 0, tzinfo=timezone.utc)

def grp(symbol, direction="long", modules=None, strength=70, **kw):
    mods = modules or ["ob", "bias_shift"]
    return {"symbol": symbol, "direction": direction, "exchange": "binance",
            "market": "perpetual", "modules": mods, "matched_modules": mods,
            "module_count": len(mods), "strength": strength,
            "timeframes": kw.get("timeframes", ["4h"]),
            "zone_high": kw.get("zone_high", 101), "zone_low": kw.get("zone_low", 99),
            "detected_price": kw.get("price", 100)}

# Backfill spawns real REST work — stub it everywhere.
_NOOP = patch.multiple(main, _lm_flow_backfill_bg=lambda *a, **k: None,
                             _lm_spot_flow_backfill_bg=lambda *a, **k: None)
_NOOP.start()


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 1 — promoting creates watched items")
# ══════════════════════════════════════════════════════════════════════════════

with main.app.app_context():
    r = promote_groups(UID, [grp("AAAUSDT"), grp("BBBUSDT", "short")], now=NOW)
    check("1-1 both groups promoted", r["promoted"] == 2, r)

    rows = LMI.query.filter_by(user_id=UID, is_active=True).all()
    check("1-2 two watched items exist", len(rows) == 2, len(rows))
    check("1-3 marked as auto-added so they can be dropped safely",
          all(x.source_tab == AUTO_SOURCE_TAB for x in rows))
    check("1-4 direction carried through",
          sorted(x.direction for x in rows) == ["long", "short"])

    a = LMI.query.filter_by(user_id=UID, symbol="AAAUSDT").first()
    check("1-5 zone carried through", a.zone_high == 101 and a.zone_low == 99)
    check("1-6 strength stored as the score", a.score == 70)
    check("1-7 setup name records which scans agreed",
          "ob" in (a.setup_type or "") and "bias_shift" in (a.setup_type or ""),
          a.setup_type)
    snap = json.loads(a.snapshot_json)
    check("1-8 snapshot records why it is being watched",
          snap.get("signal_watch", {}).get("module_count") == 2, snap.get("signal_watch"))

    # Same group again → refresh, not a second row.
    r2 = promote_groups(UID, [grp("AAAUSDT", strength=88)], now=NOW)
    check("1-9 re-promoting refreshes instead of duplicating",
          r2["refreshed"] == 1 and r2["promoted"] == 0, r2)
    check("1-10 still only two items",
          LMI.query.filter_by(user_id=UID, is_active=True).count() == 2)
    check("1-11 refreshed score updated",
          LMI.query.filter_by(user_id=UID, symbol="AAAUSDT").first().score == 88)

    check("1-12 empty group list is a safe no-op",
          promote_groups(UID, [], now=NOW)["promoted"] == 0)


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 2 — the ceiling is respected")
# ══════════════════════════════════════════════════════════════════════════════

check("2-1 ceiling read from the environment", max_watched_coins() == 3,
      max_watched_coins())

with main.app.app_context():
    r = promote_groups(UID, [grp("CCCUSDT"), grp("DDDUSDT"), grp("EEEUSDT")], now=NOW)
    total = LMI.query.filter_by(user_id=UID, is_active=True).count()
    check("2-2 watching never exceeds the ceiling", total <= 3, total)
    check("2-3 the excess is reported, not silently dropped",
          r["skipped_cap"] >= 1, r)


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 3 — hand-added coins are never touched")
# ══════════════════════════════════════════════════════════════════════════════

with main.app.app_context():
    LMI.query.filter_by(user_id=UID2).delete(); db.session.commit()

    manual = LMI(user_id=UID2, symbol="MYCOINUSDT", exchange="binance",
                 market="perpetual", source_tab="manual", direction="long",
                 timeframe="4h", is_active=True, status="watching")
    db.session.add(manual); db.session.commit()
    manual_id = manual.id

    promote_groups(UID2, [grp("ZZZUSDT"), grp("YYYUSDT"), grp("XXXUSDT")], now=NOW)

    still = db.session.get(LMI, manual_id)
    check("3-1 hand-added coin still active after promotion",
          still.is_active is True and still.source_tab == "manual")

    total = LMI.query.filter_by(user_id=UID2, is_active=True).count()
    check("3-2 hand-added coins count against the ceiling too", total <= 3, total)

    # Demote with nothing live — the manual one must survive.
    old = NOW + timedelta(hours=5)
    d = demote_stale_watches(UID2, [], now=old)
    still = db.session.get(LMI, manual_id)
    check("3-3 demotion never deactivates a hand-added coin",
          still.is_active is True, still.is_active)
    check("3-4 auto-added ones were dropped", d["demoted"] >= 1, d)


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 4 — dropping stale watches, without thrashing")
# ══════════════════════════════════════════════════════════════════════════════

with main.app.app_context():
    LMI.query.filter_by(user_id=UID).delete(); db.session.commit()
    promote_groups(UID, [grp("KEEPUSDT"), grp("DROPUSDT")], now=NOW)

    # Straight away: too young to drop, even though DROPUSDT is not live.
    d = demote_stale_watches(UID, [grp("KEEPUSDT")], now=NOW + timedelta(minutes=5))
    check("4-1 a just-added coin is not dropped immediately",
          d["demoted"] == 0 and d["kept_min_age"] >= 1, d)
    check("4-2 both still watched",
          LMI.query.filter_by(user_id=UID, is_active=True).count() == 2)

    # After the minimum watch time, the one no longer live goes.
    d = demote_stale_watches(UID, [grp("KEEPUSDT")], now=NOW + timedelta(hours=3))
    check("4-3 stale coin dropped once old enough", d["demoted"] == 1, d)
    check("4-4 the live one is kept",
          LMI.query.filter_by(user_id=UID, symbol="KEEPUSDT",
                              is_active=True).count() == 1)
    dropped = LMI.query.filter_by(user_id=UID, symbol="DROPUSDT").first()
    check("4-5 dropped coin marked expired, not deleted",
          dropped.is_active is False and dropped.status == "expired",
          (dropped.is_active, dropped.status))

    check("4-6 demoting with nothing to do is safe",
          demote_stale_watches(UID, [grp("KEEPUSDT")],
                               now=NOW + timedelta(hours=4))["demoted"] == 0)


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 5 — full cycle, end to end")
# ══════════════════════════════════════════════════════════════════════════════

with main.app.app_context():
    LMI.query.delete(); C.query.delete(); db.session.commit()

    st = get_or_create_signal_settings(UID)
    st.enabled = False
    db.session.commit()

    res = run_promotion_cycle(UID, now=NOW)
    check("5-1 engine off means nothing runs at all",
          res.get("skipped") == "engine_off", res)
    check("5-2 no coins watched while off",
          LMI.query.filter_by(user_id=UID, is_active=True).count() == 0)

    st.enabled = True
    db.session.commit()

    batch  = adapt_zone_scan({"symbol": "CYCUSDT", "price": 100, "timeframe": "4h",
        "topAlerts": [{"type": "ob", "side": "bull", "zoneHigh": 101,
                       "zoneLow": 99, "score": 85}]}, now=NOW)
    batch += adapt_bias({"symbol": "CYCUSDT", "price": 100, "timeframe": "4h",
                         "bias": "bullish", "score": 80}, now=NOW)
    batch += adapt_bias({"symbol": "SOLOUSDT", "price": 50, "timeframe": "4h",
                         "bias": "bearish", "score": 60}, now=NOW)
    record_candidates(batch)

    res = run_promotion_cycle(UID, now=NOW)
    check("5-3 cycle succeeds", res.get("ok") is True, res)
    check("5-4 groups were found", res.get("groups_found", 0) >= 2, res)
    check("5-5 coins promoted", res["promote"]["promoted"] >= 1, res["promote"])

    watched = {r.symbol for r in LMI.query.filter_by(user_id=UID, is_active=True).all()}
    check("5-6 the multi-scan coin is being watched", "CYCUSDT" in watched, watched)

    res2 = run_promotion_cycle(UID, now=NOW + timedelta(minutes=10))
    check("5-7 a second cycle refreshes rather than re-adding",
          res2["promote"]["promoted"] == 0 and res2["promote"]["refreshed"] >= 1,
          res2["promote"])
    check("5-8 ceiling reported back for visibility", res2.get("cap") == 3)

    # A user who never switched the engine on must cost nothing.
    all_res = run_promotion_for_all_enabled(now=NOW)
    check("5-9 only opted-in users are processed",
          all_res["users"] == 1 and UID in all_res["results"], all_res["users"])


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 6 — failures stay contained")
# ══════════════════════════════════════════════════════════════════════════════

with main.app.app_context():
    with patch("live_monitor.signal_promoter.build_confluence_groups"
               if False else "live_monitor.signal_confluence.build_confluence_groups",
               side_effect=RuntimeError("db down")):
        res = run_promotion_cycle(UID, now=NOW)
    check("6-1 a grouping failure returns an error instead of raising",
          res.get("ok") is False and "grouping_error" in str(res.get("reason")), res)

    with patch.object(main, "_lm_flow_backfill_bg", side_effect=RuntimeError("boom")):
        LMI.query.filter_by(user_id=UID).delete(); db.session.commit()
        r = promote_groups(UID, [grp("BACKFAILUSDT")], now=NOW)
    check("6-2 a backfill failure does not undo the promotion",
          r["promoted"] == 1, r)
    check("6-3 the coin is still watched",
          LMI.query.filter_by(user_id=UID, symbol="BACKFAILUSDT",
                              is_active=True).count() == 1)


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 7 — the manual trigger route")
# ══════════════════════════════════════════════════════════════════════════════

client = main.app.test_client()
with client.session_transaction() as s:
    s["logged_in"] = True; s["username"] = "promo"

r = client.post("/api/live-monitor/signal-promotion/run")
check("7-1 manual run returns 200", r.status_code == 200, r.status_code)
check("7-2 result reported back", "result" in (r.get_json() or {}), r.get_json())

r = client.get("/api/live-monitor/signal-candidates?hours=12")
check("7-3 candidate view returns 200", r.status_code == 200, r.status_code)
body = r.get_json() or {}
check("7-4 shows both totals so filtering is visible",
      "total_groups" in body and "matched_groups" in body, list(body))
check("7-5 reports the ceiling", body.get("max_watched") == 3, body.get("max_watched"))

anon = main.app.test_client()
check("7-6 unauthenticated promotion run blocked",
      anon.post("/api/live-monitor/signal-promotion/run").status_code in (302, 401))

_NOOP.stop()
print(f"\n{'='*60}")
print(f"  TOTAL: {_pass+_fail}   PASS: {_pass}   FAIL: {_fail}")
print(f"{'='*60}")
if _fail:
    sys.exit(1)
