"""Phase SIG-9 — the Selected Pairs universe rebuilds itself server-side.

The Selected Pairs tab does not hold a fixed list: it reloads from the current
biggest movers, so the universe changes through the day. That rebuild lived
only in the browser, so with the browser closed the server kept scanning a
frozen list.

These tests pin the selection rules against the tab's own rules, and prove the
rebuild happens on every scan cycle.

Run: python3 _test_selected_universe.py
"""
import os, sys, types, tempfile

_dbf = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False).name
os.environ["DATABASE_URL"] = f"sqlite:///{_dbf}"
os.environ["SECRET_KEY"]   = "test_secret_universe"
os.environ["ZYNI_SIG_PROMOTION_ENABLED"] = "0"
os.environ["ZYNI_SIG_SCAN_ENABLED"]      = "0"
os.environ["ZYNI_SIG_UNIVERSE_GAINERS"]  = "3"
os.environ["ZYNI_SIG_UNIVERSE_LOSERS"]   = "2"
os.environ.setdefault("RESEND_API_KEY",  "test")
os.environ.setdefault("TURNSTILE_SECRET", "")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
for _mn in ["psycopg2", "psycopg2.extras", "resend"]:
    if _mn not in sys.modules:
        sys.modules[_mn] = types.ModuleType(_mn)

from unittest.mock import patch                                    # noqa: E402
import main                                                        # noqa: E402
from models import db, User                                        # noqa: E402
from live_monitor.selected_universe import (                       # noqa: E402
    build_universe, refresh_universe_for_user,
    refresh_universes_for_enabled_users, is_junk, is_stock,
    universe_config, auto_universe_enabled,
)
from live_monitor.signal_settings import get_or_create_signal_settings  # noqa: E402
from live_monitor.signal_scanner import run_scan_cycle             # noqa: E402

with main.app.app_context():
    db.create_all()
    _u = User(username="uni", password_hash="x", role="admin", status="active")
    db.session.add(_u); db.session.commit()
    UID = _u.id

_pass = 0; _fail = 0
def check(name, cond, info=""):
    global _pass, _fail
    if cond: print(f"  PASS  {name}"); _pass += 1
    else:    print(f"  FAIL  {name}" + (f"  [{info}]" if info else "")); _fail += 1
def section(t): print(f"\n── {t} ──")

def pair(sym, chg, vol=10_000_000):
    return {"symbol": sym, "changePct": chg, "quoteVolume": vol, "price": 1.0}

# Ordered by change: three clear gainers, three clear losers, one flat.
MARKET = [
    pair("AAAUSDT", 30.0), pair("BBBUSDT", 20.0), pair("CCCUSDT", 10.0),
    pair("MIDUSDT", 0.5),
    pair("XXXUSDT", -8.0), pair("YYYUSDT", -15.0), pair("ZZZUSDT", -25.0),
]


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 1 — the same junk filters the tab uses")
# ══════════════════════════════════════════════════════════════════════════════

check("1-1 leveraged UP token is junk",  is_junk("BTCUPUSDT") is True)
check("1-2 leveraged DOWN token is junk", is_junk("BTCDOWNUSDT") is True)
check("1-3 BULL token is junk",  is_junk("ETHBULLUSDT") is True)
check("1-4 numbered leveraged token is junk", is_junk("BTC3LUSDT") is True)
check("1-5 stablecoin pair is junk", is_junk("USDCUSDT") is True)
check("1-6 a normal coin is not junk", is_junk("BTCUSDT") is False)
check("1-7 tokenized stock detected", is_stock("AAPLSTOCKUSDT") is True)
check("1-8 a normal coin is not a stock", is_stock("SOLUSDT") is False)


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 2 — building the universe from current movers")
# ══════════════════════════════════════════════════════════════════════════════

with patch.object(main, "get_pairs_exchange", return_value=MARKET):
    uni = build_universe()

check("2-1 gainers and losers both included, at the configured counts",
      len(uni) == 5, uni)
check("2-2 the strongest gainers are picked",
      all(s in uni for s in ("AAAUSDT", "BBBUSDT", "CCCUSDT")), uni)
check("2-3 the deepest losers are picked",
      all(s in uni for s in ("ZZZUSDT", "YYYUSDT")), uni)
check("2-4 a flat middling coin is left out", "MIDUSDT" not in uni, uni)

DIRTY = MARKET + [pair("BTCUPUSDT", 40.0), pair("AAPLSTOCKUSDT", 35.0),
                  pair("USDCUSDT", 32.0)]
with patch.object(main, "get_pairs_exchange", return_value=DIRTY):
    uni2 = build_universe()
check("2-5 junk and stock pairs are excluded even when they top the movers",
      not any(s in uni2 for s in ("BTCUPUSDT", "AAPLSTOCKUSDT", "USDCUSDT")),
      uni2)

THIN = [pair("LOWVOLUSDT", 50.0, vol=1_000), pair("BIGUSDT", 9.0, vol=50_000_000)]
with patch.object(main, "get_pairs_exchange", return_value=THIN), \
     patch("live_monitor.selected_universe.universe_config",
           return_value=dict(universe_config(), min_volume=1_000_000)):
    uni3 = build_universe()
check("2-6 a minimum-volume floor drops thin pairs",
      "LOWVOLUSDT" not in uni3 and "BIGUSDT" in uni3, uni3)

with patch("live_monitor.selected_universe.universe_config",
           return_value=dict(universe_config(), direction="gainers")):
    with patch.object(main, "get_pairs_exchange", return_value=MARKET):
        only_up = build_universe()
check("2-7 gainers-only direction excludes the losers",
      "ZZZUSDT" not in only_up and "AAAUSDT" in only_up, only_up)

with patch.object(main, "get_pairs_exchange", return_value=[]):
    check("2-8 an empty market returns empty rather than raising",
          build_universe() == [])
with patch.object(main, "get_pairs_exchange", side_effect=RuntimeError("down")):
    check("2-9 an exchange failure returns empty rather than raising",
          build_universe() == [])

with patch.object(main, "get_pairs_exchange",
                  return_value=[{"symbol": "OKUSDT", "changePct": 5.0},
                                {"symbol": None, "changePct": 9.0},
                                {"symbol": "NOCHGUSDT"},
                                "not a dict"]):
    messy = build_universe()
check("2-10 malformed rows are skipped, good ones still land",
      messy == ["OKUSDT"], messy)


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 3 — refreshing a user's stored universe")
# ══════════════════════════════════════════════════════════════════════════════

with main.app.app_context():
    main.save_user_selected_pairs("uni", ["OLDCOINUSDT"])
    # Declares this list as movers-built, which is what makes it eligible for
    # an automatic rebuild at all — a hand-picked list is left alone (GROUP 7).
    main.save_user_universe_config("uni", {
        "source": "movers",
        "cfg": {"dir": "both", "gainers": 3, "losers": 2},
    })

    with patch.object(main, "get_pairs_exchange", return_value=MARKET):
        res = refresh_universe_for_user("uni")
    check("3-1 refresh succeeds", res.get("ok") is True, res)
    stored = main.load_user_selected_pairs("uni")
    check("3-2 the stale coin is gone", "OLDCOINUSDT" not in stored, stored)
    check("3-3 current movers are stored", "AAAUSDT" in stored, stored)
    check("3-4 the change is reported", res.get("added") == 5 and
          res.get("removed") == 1, res)

    # A brief exchange outage must not wipe the universe — scanning the
    # previous list is far better than scanning nothing.
    with patch.object(main, "get_pairs_exchange", return_value=[]):
        res = refresh_universe_for_user("uni")
    check("3-5 an empty market never wipes the stored universe",
          res.get("kept_previous") is True, res)
    check("3-6 the previous universe survives",
          main.load_user_selected_pairs("uni") == stored,
          main.load_user_selected_pairs("uni"))

    with patch("live_monitor.selected_universe.auto_universe_enabled",
               return_value=False):
        res = refresh_universe_for_user("uni")
    check("3-7 auto refresh can be switched off",
          res.get("skipped") == "auto_universe_disabled", res)


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 4 — only users who actually use the universe are refreshed")
# ══════════════════════════════════════════════════════════════════════════════

with main.app.app_context():
    st = get_or_create_signal_settings(UID)
    st.enabled = True
    st.coin_scope = "top_volume"
    db.session.commit()
    main.save_user_universe_config("uni", {
        "source": "movers",
        "cfg": {"dir": "both", "gainers": 3, "losers": 2},
    })

with patch.object(main, "get_pairs_exchange", return_value=MARKET):
    res = refresh_universes_for_enabled_users()
check("4-1 a user on a different scope is not touched",
      res.get("users") == 0, res)

with main.app.app_context():
    st = get_or_create_signal_settings(UID)
    st.coin_scope = "selected_pairs"
    db.session.commit()

with patch.object(main, "get_pairs_exchange", return_value=MARKET):
    res = refresh_universes_for_enabled_users()
check("4-2 a user on the Selected Pairs scope IS refreshed",
      res.get("users") == 1, res)

with main.app.app_context():
    st = get_or_create_signal_settings(UID)
    st.enabled = False
    db.session.commit()
with patch.object(main, "get_pairs_exchange", return_value=MARKET):
    res = refresh_universes_for_enabled_users()
check("4-3 a disabled engine means no refresh work", res.get("users") == 0, res)


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 5 — the refresh really happens on every scan cycle")
# ══════════════════════════════════════════════════════════════════════════════

with main.app.app_context():
    st = get_or_create_signal_settings(UID)
    st.enabled = True
    st.coin_scope = "selected_pairs"
    db.session.commit()
    main.save_user_selected_pairs("uni", ["STALEUSDT"])
    main.save_user_universe_config("uni", {
        "source": "movers",
        "cfg": {"dir": "both", "gainers": 3, "losers": 2},
    })

calls = []
def _count_refresh(*a, **kw):
    calls.append(1)
    return {"users": 1, "results": {"uni": 5}}

with patch("live_monitor.selected_universe.refresh_universes_for_enabled_users",
           side_effect=_count_refresh), \
     patch("live_monitor.signal_scanner.run_one_scan",
           return_value={"ok": True, "kind": "scan", "elapsed_sec": 0.1}):
    res = run_scan_cycle(force_kind="scan")

check("5-1 the cycle refreshes the universe before scanning",
      len(calls) == 1, len(calls))
check("5-2 the cycle reports it", res.get("universe_refreshed_for") == 1, res)

# End to end: a real cycle must replace the stale list with live movers.
with patch.object(main, "get_pairs_exchange", return_value=MARKET), \
     patch("live_monitor.signal_scanner.run_one_scan",
           return_value={"ok": True, "kind": "scan", "elapsed_sec": 0.1}):
    run_scan_cycle(force_kind="scan")

with main.app.app_context():
    now_stored = main.load_user_selected_pairs("uni")
check("5-3 the stale pair is replaced by current movers",
      "STALEUSDT" not in now_stored and "AAAUSDT" in now_stored, now_stored)

# A failing refresh must not stop the scan.
with patch("live_monitor.selected_universe.refresh_universes_for_enabled_users",
           side_effect=RuntimeError("boom")), \
     patch("live_monitor.signal_scanner.run_one_scan",
           return_value={"ok": True, "kind": "scan", "elapsed_sec": 0.1}):
    res = run_scan_cycle(force_kind="scan")
check("5-4 a failed universe refresh does not stop the scan",
      res.get("ok") is True, res)



# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 6 — the rebuild uses YOUR tab settings, not server defaults")
# ══════════════════════════════════════════════════════════════════════════════
# Someone who picked "200 gainers, no losers" must get exactly that rebuilt.
# A server default of 100 gainers + 100 losers would hand them a different
# universe than the one their tab shows, with nothing explaining the mismatch.

from live_monitor.selected_universe import user_universe_config    # noqa: E402

BIG = ([pair(f"UP{i}USDT", 50.0 - i) for i in range(10)] +
       [pair(f"DN{i}USDT", -50.0 + i) for i in range(10)])

with main.app.app_context():
    main.save_user_universe_config("uni", {
        "source": "movers",
        "cfg": {"dir": "gainers", "gainers": 4, "losers": 0,
                "minVol": 0, "excludeJunk": True, "excludeStocks": True},
    })
    cfg = user_universe_config("uni")

check("6-1 the saved direction is used", cfg["direction"] == "gainers", cfg)
check("6-2 the saved counts are used",
      cfg["gainers"] == 4 and cfg["losers"] == 0, cfg)
check("6-3 the list is recognised as movers-built", cfg["source"] == "movers")

with main.app.app_context():
    with patch.object(main, "get_pairs_exchange", return_value=BIG):
        res = refresh_universe_for_user("uni")
    stored = main.load_user_selected_pairs("uni")

check("6-4 exactly the requested number of gainers", len(stored) == 4, stored)
check("6-5 gainers only — no losers crept in",
      all(s.startswith("UP") for s in stored), stored)
check("6-6 the strongest gainer is first", stored[0] == "UP0USDT", stored)
check("6-7 the settings used are reported back",
      res.get("direction") == "gainers" and res.get("gainers") == 4, res)

# Losers-only is the mirror case.
with main.app.app_context():
    main.save_user_universe_config("uni", {
        "source": "movers",
        "cfg": {"dir": "losers", "gainers": 0, "losers": 3},
    })
    with patch.object(main, "get_pairs_exchange", return_value=BIG):
        refresh_universe_for_user("uni")
    stored = main.load_user_selected_pairs("uni")
check("6-8 losers-only returns only losers",
      len(stored) == 3 and all(s.startswith("DN") for s in stored), stored)

# A user's own volume floor must be honoured too.
with main.app.app_context():
    main.save_user_universe_config("uni", {
        "source": "movers",
        "cfg": {"dir": "gainers", "gainers": 10, "losers": 0,
                "minVol": 5_000_000},
    })
    THIN = [pair("RICHUSDT", 20.0, vol=9_000_000),
            pair("POORUSDT", 40.0, vol=1_000)]
    with patch.object(main, "get_pairs_exchange", return_value=THIN):
        refresh_universe_for_user("uni")
    stored = main.load_user_selected_pairs("uni")
check("6-9 the user's own volume floor is applied",
      stored == ["RICHUSDT"], stored)


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 7 — a hand-picked list is never rebuilt")
# ══════════════════════════════════════════════════════════════════════════════
# Starring individual coins is a deliberate choice. Replacing those with top
# movers would throw away exactly the pairs the user cared about most.

with main.app.app_context():
    main.save_user_selected_pairs("handy", ["MYCOINUSDT", "OTHERUSDT"])
    main.save_user_universe_config("handy", {"source": "manual"})

    with patch.object(main, "get_pairs_exchange", return_value=BIG):
        res = refresh_universe_for_user("handy")
    stored = main.load_user_selected_pairs("handy")

check("7-1 a hand-picked list is skipped, not rebuilt",
      res.get("skipped") == "hand_picked_list", res)
check("7-2 the hand-picked pairs survive untouched",
      stored == ["MYCOINUSDT", "OTHERUSDT"], stored)

# The register endpoint records which kind of list was saved.
client7 = main.app.test_client()
with client7.session_transaction() as s7:
    s7["logged_in"] = True; s7["username"] = "uni"

# Stub the live-price thread: fake symbols would otherwise spawn real
# websocket connections that can crash the interpreter at shutdown.
_wl_stub = patch.object(main, "_ensure_wl_thread", lambda *a, **k: None)
_wl_stub.start()

client7.post("/api/watchlist/register",
             json={"pairs": ["AUSDT", "BUSDT"],
                   "universe": {"source": "movers",
                                "cfg": {"dir": "both", "gainers": 7, "losers": 7}}})
with main.app.app_context():
    saved = main.load_user_universe_config("uni")
check("7-3 a movers load is recorded as movers", saved.get("source") == "movers", saved)
check("7-4 the settings are stored with it",
      (saved.get("cfg") or {}).get("gainers") == 7, saved)

client7.post("/api/watchlist/register", json={"pairs": ["CUSDT"]})
with main.app.app_context():
    saved = main.load_user_universe_config("uni")
check("7-5 a plain star sync is recorded as hand-picked",
      saved.get("source") == "manual", saved)

# And an out-of-range count cannot be injected through the API.
client7.post("/api/watchlist/register",
             json={"pairs": ["DUSDT"],
                   "universe": {"source": "movers",
                                "cfg": {"dir": "nonsense", "gainers": 99999,
                                        "losers": -5}}})
with main.app.app_context():
    saved = main.load_user_universe_config("uni")
c = saved.get("cfg") or {}
check("7-6 an invalid direction falls back to both", c.get("direction") == "both", c)
check("7-7 counts are clamped to a sane range",
      c.get("gainers") == 400 and c.get("losers") == 0, c)

_wl_stub.stop()

print(f"\n{'='*60}")
print(f"  TOTAL: {_pass+_fail}   PASS: {_pass}   FAIL: {_fail}")
print(f"{'='*60}")

# Registering pairs starts the watchlist price worker, which opens websocket
# connections for each symbol. Fake test symbols leave those daemon threads
# doing network I/O that intermittently crashes the interpreter during
# shutdown — after every assertion has already run. Exiting immediately with
# the real verdict keeps the result honest and the exit code deterministic.
sys.stdout.flush()
sys.stderr.flush()
os._exit(1 if _fail else 0)
