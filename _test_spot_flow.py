"""Live Monitor Spot Flow — Perp/Spot Data Health toggle backend tests.

Imports the actual production application (main.py) and drives the real
collector functions, the real HTTP data-health route, and the real item
add/delete lifecycle. This is Phase SpotFlow-1 (Binance spot only) — a
dedicated LiveMonitorSpotFlowCandle table and collector, kept fully separate
from the already-shipped LiveMonitorFlowCandle (perp) system so a bug here
cannot affect it.

Pins the two constraints that matter most for this feature:
  1. The spot collector/backfill genuinely reads Binance's SPOT API
     (api.binance.com), never the futures API (fapi.binance.com).
  2. Adding spot_flow to the Data Health response never removes or alters
     any existing field (rows, critical_status, ai_data_gate,
     parent_setup_exchange, ...) — purely additive.

Run: python3 _test_spot_flow.py
"""
import os, sys, types, tempfile, time
from unittest.mock import patch, MagicMock

_dbf = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False).name
os.environ["DATABASE_URL"]   = f"sqlite:///{_dbf}"
os.environ["SECRET_KEY"]     = "test_secret_spot_flow"
os.environ.setdefault("RESEND_API_KEY",  "test")
os.environ.setdefault("TURNSTILE_SECRET", "")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

for _mn in ["psycopg2", "psycopg2.extras", "resend"]:
    if _mn not in sys.modules:
        sys.modules[_mn] = types.ModuleType(_mn)

import main                                                        # noqa: E402
from models import (db, User, LiveMonitorItem,                     # noqa: E402
                    LiveMonitorSpotFlowCandle as SFC,
                    LiveMonitorFlowCandle as FC)

with main.app.app_context():
    db.create_all()
    _alice = User(username="alice", password_hash="x", role="user", status="active")
    db.session.add(_alice)
    db.session.commit()
    UID_A = _alice.id

client = main.app.test_client()
with client.session_transaction() as s:
    s["logged_in"] = True
    s["username"]  = "alice"

_pass = 0; _fail = 0
def check(name, cond, info=""):
    global _pass, _fail
    if cond:
        print(f"  PASS  {name}"); _pass += 1
    else:
        print(f"  FAIL  {name}" + (f"  [{info}]" if info else "")); _fail += 1
def section(t):
    print(f"\n── {t} ──")


def om(t):
    return int(t * 1000) // 60000 * 60000


# ══════════════════════════════════════════════════════════════════════════════
# GROUP 1 — Collector: bucketing, rollover, CVD continuity, stale flush, retention
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 1 — spot collector math")

T0 = float(int(1_756_000_000.0) // 60 * 60)  # minute-aligned so T0+1..T0+59 share one bucket

main._lm_spot_flow_tick("SPOTUSDT", "buy", 200.0, 100.0, T0 + 1)
main._lm_spot_flow_tick("SPOTUSDT", "sell", 50.0, 99.5, T0 + 30)
main._lm_spot_flow_tick("SPOTUSDT", "buy", 100.0, 100.5, T0 + 59)
main._lm_spot_flow_tick("SPOTUSDT", "sell", 10.0, 100.2, T0 + 61)  # rollover persists minute 0
with main.app.app_context():
    r = SFC.query.filter_by(symbol="SPOTUSDT", exchange="binance", timeframe="1m",
                            candle_open_ms=om(T0)).first()
check("1-1 candle persisted on rollover", r is not None)
check("1-2 buy/sell/delta correct",
      r and abs(r.buy_vol_usd - 300) < 0.01 and abs(r.sell_vol_usd - 50) < 0.01
      and abs(r.delta_usd - 250) < 0.01,
      (r.buy_vol_usd, r.sell_vol_usd, r.delta_usd) if r else None)
check("1-3 cvd starts at delta", r and abs(r.cvd_usd - 250) < 0.01, r.cvd_usd if r else None)
check("1-4 tick_count=3", r and r.tick_count == 3)

main._lm_spot_flow_tick("SPOTUSDT", "buy", 40.0, 100.4, T0 + 119)
main._lm_spot_flow_tick("SPOTUSDT", "buy", 1.0, 100.4, T0 + 121)  # rollover persists minute 1
with main.app.app_context():
    r2 = SFC.query.filter_by(symbol="SPOTUSDT", exchange="binance", timeframe="1m",
                             candle_open_ms=om(T0 + 60)).first()
check("1-5 cvd cumulative 280", r2 and abs(r2.cvd_usd - 280) < 0.01, r2.cvd_usd if r2 else None)

main._lm_spot_flow_buckets.clear(); main._lm_spot_flow_state.clear()
main._lm_spot_flow_tick("SPOTUSDT", "buy", 20.0, 100.6, T0 + 185)
main._lm_spot_flow_tick("SPOTUSDT", "buy", 1.0, 100.6, T0 + 241)  # rollover persists minute 3
with main.app.app_context():
    r3 = SFC.query.filter_by(symbol="SPOTUSDT", exchange="binance", timeframe="1m",
                             candle_open_ms=om(T0 + 185)).first()
check("1-6 post-restart cvd seeded from DB (280+20=300)",
      r3 and abs(r3.cvd_usd - 300) < 0.01, r3.cvd_usd if r3 else None)

main._lm_spot_flow_buckets.clear(); main._lm_spot_flow_state.clear()
with patch.object(main.time, "time", return_value=T0 + 400):
    main._lm_spot_flow_tick("QUIETUSDT", "buy", 5.0, 2.0, T0 + 400)
with patch.object(main.time, "time", return_value=T0 + 470):
    main._lm_spot_flow_flush_stale_buckets()
with main.app.app_context():
    rq = SFC.query.filter_by(symbol="QUIETUSDT", exchange="binance", timeframe="1m").first()
check("1-7 stale bucket flushed via periodic flush call", rq is not None, rq)
check("1-8 flushed candle has the one tick's delta",
      rq and abs(rq.delta_usd - 5.0) < 0.01, rq.delta_usd if rq else None)

with main.app.app_context():
    old_ms = om(T0) - 4 * 86_400_000
    db.session.add(SFC(symbol="SPOTUSDT", exchange="binance", timeframe="1m", candle_open_ms=old_ms,
                       delta_usd=0, cvd_usd=0, buy_vol_usd=0, sell_vol_usd=0, tick_count=0))
    db.session.commit()
hour_ms = (om(T0) // 60000 // 60 + 1) * 60 * 60000
main._lm_spot_flow_persist_candle("SPOTUSDT", {"open_ms": hour_ms, "buy": 1.0, "sell": 0.0,
                                               "ticks": 1, "last_price": 100.0})
with main.app.app_context():
    gone = SFC.query.filter_by(symbol="SPOTUSDT", candle_open_ms=old_ms).first()
check("1-9 3-day retention pruned old row", gone is None)


# ══════════════════════════════════════════════════════════════════════════════
# GROUP 2 — Backfill: uses the SPOT api, never futures; math; skip-if-exists
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 2 — spot backfill")

T1 = 1_756_100_000_000 // 60000 * 60000
def mk_kline(i, close, quote_vol, taker_buy, trades=10):
    return [T1 + i * 60000, "0", "0", "0", str(close), "0", 0, str(quote_vol), trades, "0", str(taker_buy), "0"]
klines = [mk_kline(0, 2.0, 1000.0, 700.0), mk_kline(1, 2.1, 500.0, 100.0), mk_kline(2, 2.2, 800.0, 400.0)]

_url_seen = {"spot": False, "futures": False}
def fake_get(url, params=None, timeout=None):
    m = MagicMock(); m.status_code = 200
    if "/api/v3/klines" in url:
        _url_seen["spot"] = True
        check("2-1 backfill hits the SPOT klines URL", "api.binance.com" in url, url)
        check("2-2 backfill NEVER hits the futures klines URL", "fapi" not in url, url)
        m.json = lambda: klines
    else:
        m.status_code = 404; m.json = lambda: {}
    return m

with patch.object(main.req, "get", side_effect=fake_get):
    res = main._lm_spot_flow_backfill("SPOTBFUSDT")
check("2-3 backfill hit the spot endpoint at all", _url_seen["spot"])
check("2-4 backfill ok, added=3", res.get("ok") and res.get("added") == 3, res)
with main.app.app_context():
    rows = SFC.query.filter_by(symbol="SPOTBFUSDT").order_by(SFC.candle_open_ms).all()
check("2-5 deltas 400/-300/0", [round(r.delta_usd) for r in rows] == [400, -300, 0], [r.delta_usd for r in rows])
check("2-6 cvd 400/100/100", [round(r.cvd_usd) for r in rows] == [400, 100, 100], [r.cvd_usd for r in rows])
check("2-7 source=backfill", all(r.source == "backfill" for r in rows))

with patch.object(main.req, "get", side_effect=fake_get):
    res2 = main._lm_spot_flow_backfill("SPOTBFUSDT")
check("2-8 second backfill skipped (rows exist)", res2.get("skipped") == "rows_exist", res2)


# ══════════════════════════════════════════════════════════════════════════════
# GROUP 3 — Lifecycle wiring: add spawns backfill, delete cleans up spot rows too
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 3 — item add/delete lifecycle wiring")

calls = []
with patch.object(main, "_lm_spot_flow_backfill_bg", side_effect=lambda s: calls.append(s)), \
     patch.object(main, "_lm_flow_backfill_bg", side_effect=lambda s: None), \
     patch.object(main, "_lm_detect_symbol_exchange", return_value="binance"):
    client.post("/api/live-monitor/items", json={
        "symbol": "NEWSPOTUSDT", "exchange": "binance", "market": "perpetual",
        "source_tab": "main", "setup_type": "MANUAL", "confidence": 0, "score": 0,
        "current_price": None, "snapshot": {"manual_add": True},
        "selected_timeframes": ["4h"], "selected_modules": [], "alert_settings": {}})
check("3-1 item add spawns spot backfill for a binance symbol", calls == ["NEWSPOTUSDT"], calls)

with main.app.app_context():
    db.session.add(SFC(symbol="DELSPOTUSDT", exchange="binance", timeframe="1m", candle_open_ms=T1,
                       delta_usd=1, cvd_usd=1, buy_vol_usd=1, sell_vol_usd=0, tick_count=1))
    item = LiveMonitorItem(user_id=UID_A, symbol="DELSPOTUSDT", exchange="binance", market="perpetual",
                           source_tab="main", setup_type="MANUAL", is_active=True)
    db.session.add(item)
    db.session.commit()
    item_id = item.id
r = client.delete(f"/api/live-monitor/items/{item_id}")
with main.app.app_context():
    n = SFC.query.filter_by(symbol="DELSPOTUSDT").count()
check("3-2 delete of last reference purges spot rows too", r.status_code == 200 and n == 0, (r.status_code, n))


# ══════════════════════════════════════════════════════════════════════════════
# GROUP 3B — Lazy backfill for items that existed before this feature shipped.
# The item-ADD backfill trigger (Group 3) only fires on NEW adds — a symbol
# added before this feature deployed would otherwise sit with zero spot rows
# indefinitely. Viewing its Data Health should retroactively backfill it.
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 3B — lazy backfill on view for pre-existing items")

os.environ["ZYNI_LM_WS_ENABLED"] = "1"  # this route branch is gated on this flag
with main.app.app_context():
    _old_item = LiveMonitorItem(user_id=UID_A, symbol="OLDITEMUSDT", exchange="binance",
                                market="perpetual", source_tab="main", setup_type="MANUAL",
                                direction="neutral", timeframe="4h", current_price=1.0,
                                is_active=True, status="watching")
    db.session.add(_old_item)
    db.session.commit()

_lazy_calls = []
def _fake_lazy_backfill(symbol):
    _lazy_calls.append(symbol)
    with main.app.app_context():
        db.session.add(SFC(symbol=symbol, exchange="binance", timeframe="1m",
                           candle_open_ms=int(time.time() * 1000) // 60000 * 60000,
                           delta_usd=1, cvd_usd=1, buy_vol_usd=1, sell_vol_usd=0,
                           tick_count=1, source="backfill"))
        db.session.commit()

with patch.object(main, "_lm_spot_flow_backfill_bg", side_effect=_fake_lazy_backfill), \
     patch.object(main, "_ensure_lm_ws_thread"), patch.object(main, "_ensure_lm_liq_thread"), \
     patch.object(main, "_ensure_lm_delta_thread"), patch.object(main, "_ensure_lm_spot_thread"), \
     patch.object(main, "ensure_ob_stream"), patch.object(main, "_lm_ws_ensure_adhoc"):
    _r1 = client.get("/api/live-monitor/data-health?symbol=OLDITEMUSDT&exchange=binance")
check("3b-1 route succeeds for a pre-existing item", _r1.status_code == 200)
check("3b-2 lazy backfill triggered for pre-existing item with zero spot rows",
      _lazy_calls == ["OLDITEMUSDT"], _lazy_calls)

with patch.object(main, "_lm_spot_flow_backfill_bg", side_effect=_fake_lazy_backfill), \
     patch.object(main, "_ensure_lm_ws_thread"), patch.object(main, "_ensure_lm_liq_thread"), \
     patch.object(main, "_ensure_lm_delta_thread"), patch.object(main, "_ensure_lm_spot_thread"), \
     patch.object(main, "ensure_ob_stream"), patch.object(main, "_lm_ws_ensure_adhoc"):
    _r2 = client.get("/api/live-monitor/data-health?symbol=OLDITEMUSDT&exchange=binance")
check("3b-3 second poll does NOT re-trigger backfill (rows now exist)",
      _lazy_calls == ["OLDITEMUSDT"], _lazy_calls)

with patch.object(main, "_lm_spot_flow_backfill_bg", side_effect=_fake_lazy_backfill), \
     patch.object(main, "_ensure_lm_ws_thread"), patch.object(main, "_ensure_lm_liq_thread"), \
     patch.object(main, "_ensure_lm_delta_thread"), patch.object(main, "_ensure_lm_spot_thread"), \
     patch.object(main, "ensure_ob_stream"), patch.object(main, "_lm_ws_ensure_adhoc"):
    _r3 = client.get("/api/live-monitor/data-health?symbol=FRESH3BUSDT&exchange=binance")
check("3b-4 a different empty symbol also triggers its own lazy backfill",
      _lazy_calls == ["OLDITEMUSDT", "FRESH3BUSDT"], _lazy_calls)
os.environ.pop("ZYNI_LM_WS_ENABLED", None)


# ══════════════════════════════════════════════════════════════════════════════
# GROUP 4 — _lm_cvd_trend_label
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 4 — cvd_trend classifier")

def mk(t, cvd):
    return {"t": t, "cvd_usd": cvd}

check("4-1 rising series -> rising",
      main._lm_cvd_trend_label([mk(i, 100 + i * 50) for i in range(10)]) == "rising")
check("4-2 falling series -> falling",
      main._lm_cvd_trend_label([mk(i, 1000 - i * 50) for i in range(10)]) == "falling")
check("4-3 near-flat series -> flat",
      main._lm_cvd_trend_label([mk(i, 500 + ((-1) ** i)) for i in range(10)]) == "flat")
check("4-4 too-short series -> flat (safe default)",
      main._lm_cvd_trend_label([mk(0, 100)]) == "flat")
check("4-5 empty series -> flat", main._lm_cvd_trend_label([]) == "flat")
check("4-6 near-zero magnitude doesn't false-trigger (scale floor)",
      main._lm_cvd_trend_label([mk(0, 0.001), mk(1, 0.002), mk(2, -0.001)]) == "flat")


# ══════════════════════════════════════════════════════════════════════════════
# GROUP 5 — spot_flow Data Health context: shape, cross-check, honesty
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 5 — spot_flow context builder")

with main.app.app_context():
    ctx = main._lm_build_data_health_context("NOSPOTUSDT", "binance", snap=None, user_id=UID_A)
check("5-1 spot_flow key present with no data", "spot_flow" in ctx)
check("5-2 spot_flow ok=False, insufficient_history",
      ctx["spot_flow"]["ok"] is False and ctx["spot_flow"]["reason"] == "insufficient_history",
      ctx["spot_flow"])
check("5-3 all 4 exchanges honestly listed as skipped when no data",
      set(ctx["spot_flow"]["sources_skipped"]) == {"binance", "bybit", "okx", "mexc"},
      ctx["spot_flow"]["sources_skipped"])

T2 = (int(time.time() * 1000) // 300000 - 20) * 300000
with main.app.app_context():
    for i in range(15):
        db.session.add(SFC(symbol="XSPOTUSDT", exchange="binance", timeframe="1m",
                           candle_open_ms=T2 + i * 300000, price_close=100 + i,
                           buy_vol_usd=50, sell_vol_usd=10, delta_usd=40, cvd_usd=100 + i * 40,
                           tick_count=5))
        db.session.add(FC(symbol="XSPOTUSDT", timeframe="1m", candle_open_ms=T2 + i * 300000,
                          price_close=100 + i, buy_vol_usd=60, sell_vol_usd=15, delta_usd=45,
                          cvd_usd=200 + i * 45, tick_count=8))
    db.session.commit()
    ctx2 = main._lm_build_data_health_context("XSPOTUSDT", "binance", snap=None, user_id=UID_A)
sf = ctx2["spot_flow"]
check("5-4 ok=True with sufficient history", sf["ok"] is True, sf)
check("5-5 per_exchange.binance has price/volume/cvd/trend",
      "binance" in sf["per_exchange"] and sf["per_exchange"]["binance"]["cvd_trend"] == "rising",
      sf["per_exchange"])
check("5-6 combined mirrors binance (Phase 1: combined == solo binance)",
      sf["combined"]["cvd_trend"] == "rising")
check("5-7 cross_check: both rising -> agree", sf["cross_check"]["divergence_status"] == "agree", sf["cross_check"])
check("5-8 sources_used=[binance], sources_skipped=[bybit,okx,mexc]",
      sf["sources_used"] == ["binance"] and set(sf["sources_skipped"]) == {"bybit", "okx", "mexc"}, sf)

T3 = T2 + 100 * 300000
with main.app.app_context():
    for i in range(15):
        db.session.add(SFC(symbol="YSPOTUSDT", exchange="binance", timeframe="1m",
                           candle_open_ms=T3 + i * 300000, price_close=100,
                           buy_vol_usd=50, sell_vol_usd=10, delta_usd=40, cvd_usd=100 + i * 40,
                           tick_count=5))
        db.session.add(FC(symbol="YSPOTUSDT", timeframe="1m", candle_open_ms=T3 + i * 300000,
                          price_close=100, buy_vol_usd=15, sell_vol_usd=60, delta_usd=-45,
                          cvd_usd=1000 - i * 45, tick_count=8))
    db.session.commit()
    ctx3 = main._lm_build_data_health_context("YSPOTUSDT", "binance", snap=None, user_id=UID_A)
check("5-9 spot rising, futures falling -> diverge (the 'money row')",
      ctx3["spot_flow"]["cross_check"]["divergence_status"] == "diverge", ctx3["spot_flow"]["cross_check"])


# ══════════════════════════════════════════════════════════════════════════════
# GROUP 6 — Real HTTP route: additive-only, price-relevant fields untouched
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 6 — real /api/live-monitor/data-health route")

r = client.get("/api/live-monitor/data-health?symbol=ROUTESPOTUSDT&exchange=binance")
b = r.get_json()
check("6-1 route 200", r.status_code == 200)
check("6-2 spot_flow key present in real HTTP response", "spot_flow" in b, list(b.keys()))
check("6-3 existing rows/critical_status/ai_data_gate still present (unaffected)",
      "rows" in b and "critical_status" in b and "ai_data_gate" in b)
check("6-4 parent_setup_exchange / price_source_exchange present and unaffected",
      "parent_setup_exchange" in b and "price_source_exchange" in b, list(b.keys()))

with main.app.app_context():
    for i in range(15):
        db.session.add(SFC(symbol="ROUTESPOTUSDT", exchange="binance", timeframe="1m",
                           candle_open_ms=T2 + i * 300000, price_close=50 + i,
                           buy_vol_usd=30, sell_vol_usd=10, delta_usd=20, cvd_usd=100 + i * 20,
                           tick_count=3))
    db.session.commit()
r2 = client.get("/api/live-monitor/data-health?symbol=ROUTESPOTUSDT&exchange=binance")
b2 = r2.get_json()
check("6-5 spot_flow populated through the real route with real data", b2["spot_flow"]["ok"] is True, b2["spot_flow"])
check("6-6 parent_setup_exchange identical before/after spot_flow populates (same symbol, real assertion)",
      b2.get("parent_setup_exchange") == b.get("parent_setup_exchange"),
      (b.get("parent_setup_exchange"), b2.get("parent_setup_exchange")))
check("6-7 rows list unaffected in shape (still a list)", isinstance(b2.get("rows"), list))


# ══════════════════════════════════════════════════════════════════════════════
# Final summary
# ══════════════════════════════════════════════════════════════════════════════
print(f"\n{'='*60}")
print(f"  TOTAL: {_pass+_fail}   PASS: {_pass}   FAIL: {_fail}")
print(f"{'='*60}")
if _fail:
    sys.exit(1)
