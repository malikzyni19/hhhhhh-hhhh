"""Live Monitor Flow Candles — CVD divergence + OI regime engine tests.

Imports the actual production application (main.py) and drives the real
functions and HTTP route. Pins the 4-way divergence label truth table and the
4-way OI regime matrix against standard technical-analysis definitions — this
is exactly the kind of logic where a future refactor could silently invert a
label without a test catching it.

Standard TA divergence definitions being pinned here:
  regular bearish (swing highs): price higher-high, indicator lower-high
                                  -> reversal-down signal
  hidden  bearish (swing highs): price lower-high,  indicator higher-high
                                  -> downtrend continuation
  regular bullish (swing lows):  price lower-low,   indicator higher-low
                                  -> reversal-up signal
  hidden  bullish (swing lows):  price higher-low,  indicator lower-low
                                  -> uptrend continuation

Run: python3 _test_flow_divergence.py
"""
import os, sys, types, tempfile, time

_dbf = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False).name
os.environ["DATABASE_URL"]   = f"sqlite:///{_dbf}"
os.environ["SECRET_KEY"]     = "test_secret_flow_divergence"
os.environ.setdefault("RESEND_API_KEY",  "test")
os.environ.setdefault("TURNSTILE_SECRET", "")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

for _mn in ["psycopg2", "psycopg2.extras", "resend"]:
    if _mn not in sys.modules:
        sys.modules[_mn] = types.ModuleType(_mn)

import main                                             # noqa: E402  (real production app)
from models import db, User, LiveMonitorItem, LiveMonitorFlowCandle as FC  # noqa: E402

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


def mk(t, price, cvd, oi=None):
    return {"t": t, "price_close": price, "cvd_usd": cvd, "oi_close": oi}


# ══════════════════════════════════════════════════════════════════════════════
# GROUP 1 — _lm_pivot_indices
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 1 — _lm_pivot_indices (close-based swing detector)")

vals = [1, 2, 5, 2, 1, 2, 6, 2, 1]
hi, lo = main._lm_pivot_indices(vals, left=2, right=2)
check("1-1 strict peaks detected", hi == [2, 6], hi)
check("1-2 strict troughs detected (interior only)", lo == [4], lo)

plateau = [1, 2, 5, 5, 5, 2, 1]
hi_p, lo_p = main._lm_pivot_indices(plateau, left=2, right=2)
check("1-3 plateau/tie is NOT a pivot (strict inequality required)", hi_p == [], hi_p)

vals_none = [1, 2, None, 2, 1]
hi_n, lo_n = main._lm_pivot_indices(vals_none, left=1, right=1)
check("1-4 None values skipped without crash", hi_n == [] and lo_n == [], (hi_n, lo_n))

# Boundary: last candle within `right` of the array end must never be confirmed
vals_b = [1, 2, 3, 9, 3, 2]  # peak at idx3, but only 2 candles after it (right=3 needed)
hi_b, lo_b = main._lm_pivot_indices(vals_b, left=3, right=3)
check("1-5 no false pivot within `right` of array end", hi_b == [], hi_b)


# ══════════════════════════════════════════════════════════════════════════════
# GROUP 2 — _lm_detect_metric_divergence: all 4 kinds, pinned against standard TA
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 2 — CVD divergence: 4-kind truth table")

# Regular bearish: price higher-high (100->110), CVD lower-high (500->300)
candles_rb = [
    mk(0,90,100), mk(1,95,200), mk(2,98,400),
    mk(3,100,500),
    mk(4,97,450), mk(5,94,400), mk(6,96,420),
    mk(7,99,350), mk(8,105,320),
    mk(9,110,300),
    mk(10,107,280), mk(11,103,260), mk(12,101,240),
]
divs_rb = main._lm_detect_metric_divergence(candles_rb, "cvd_usd", left=3, right=3)
check("2-1 regular_bearish: price HH + cvd LH",
      any(d["kind"] == "regular_bearish" and d["swing_type"] == "high" for d in divs_rb), divs_rb)

# Hidden bearish: price LOWER-high (100->97), CVD HIGHER-high (250->320) — downtrend continuation
candles_hb = [
    mk(0,90,100), mk(1,95,150), mk(2,98,200),
    mk(3,100,250),
    mk(4,85,230), mk(5,80,210), mk(6,82,220),
    mk(7,88,260), mk(8,95,290),
    mk(9,97,320),
    mk(10,80,300), mk(11,75,280), mk(12,70,260),
]
divs_hb = main._lm_detect_metric_divergence(candles_hb, "cvd_usd", left=3, right=3)
check("2-2 hidden_bearish: price LOWER-high + cvd HIGHER-high (downtrend continuation)",
      any(d["kind"] == "hidden_bearish" and d["swing_type"] == "high" for d in divs_hb), divs_hb)

# Regular bullish: price lower-low (100->90), CVD higher-low (-400->-150)
candles_rbull = [
    mk(0,110,-100), mk(1,105,-150), mk(2,102,-200),
    mk(3,100,-400),
    mk(4,103,-350), mk(5,106,-300), mk(6,104,-320),
    mk(7,101,-250), mk(8,97,-200),
    mk(9,90,-150),
    mk(10,94,-180), mk(11,98,-220), mk(12,100,-240),
]
divs_rbull = main._lm_detect_metric_divergence(candles_rbull, "cvd_usd", left=3, right=3)
check("2-3 regular_bullish: price LL + cvd HL",
      any(d["kind"] == "regular_bullish" and d["swing_type"] == "low" for d in divs_rbull), divs_rbull)

# Hidden bullish: price HIGHER-low (90->93), CVD LOWER-low (-100->-250) — uptrend continuation
candles_hbull = [
    mk(0,100,-50), mk(1,95,-80), mk(2,92,-120),
    mk(3,90,-100),
    mk(4,98,-70), mk(5,102,-40), mk(6,99,-60),
    mk(7,96,-140), mk(8,95,-200),
    mk(9,93,-250),
    mk(10,99,-200), mk(11,103,-160), mk(12,107,-130),
]
divs_hbull = main._lm_detect_metric_divergence(candles_hbull, "cvd_usd", left=3, right=3)
check("2-4 hidden_bullish: price HIGHER-low + cvd LOWER-low (uptrend continuation)",
      any(d["kind"] == "hidden_bullish" and d["swing_type"] == "low" for d in divs_hbull), divs_hbull)

# No divergence when price and metric move the SAME direction (confirming)
candles_conf = [
    mk(0,90,100), mk(1,95,150), mk(2,98,200),
    mk(3,100,250),
    mk(4,97,220), mk(5,94,200), mk(6,96,210),
    mk(7,99,280), mk(8,105,350),
    mk(9,110,400),
    mk(10,107,380), mk(11,103,360), mk(12,101,340),
]
divs_conf = main._lm_detect_metric_divergence(candles_conf, "cvd_usd", left=3, right=3)
check("2-5 no divergence when price+cvd confirm (same direction)", divs_conf == [], divs_conf)

check("2-6 empty candles -> no crash, empty result", main._lm_detect_metric_divergence([], "cvd_usd") == [])
check("2-7 too-short candles -> no crash", main._lm_detect_metric_divergence(candles_rb[:3], "cvd_usd") == [])


# ══════════════════════════════════════════════════════════════════════════════
# GROUP 3 — _lm_classify_oi_regime: 4-way price/OI matrix
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 3 — OI regime: 4-quadrant matrix")

r1 = main._lm_classify_oi_regime([mk(0,100,None,1000), mk(1,105,None,1100)], lookback=5)
check("3-1 price up + OI up -> new_longs", r1["current_regime"] == "new_longs", r1)

r2 = main._lm_classify_oi_regime([mk(0,100,None,1000), mk(1,105,None,900)], lookback=5)
check("3-2 price up + OI down -> short_covering", r2["current_regime"] == "short_covering", r2)

r3 = main._lm_classify_oi_regime([mk(0,100,None,1000), mk(1,95,None,1100)], lookback=5)
check("3-3 price down + OI up -> new_shorts", r3["current_regime"] == "new_shorts", r3)

r4 = main._lm_classify_oi_regime([mk(0,100,None,1000), mk(1,95,None,900)], lookback=5)
check("3-4 price down + OI down -> long_unwind", r4["current_regime"] == "long_unwind", r4)

r5 = main._lm_classify_oi_regime([mk(0,100,None,1000), mk(1,100,None,1100)], lookback=5)
check("3-5 flat price -> no regime (avoid false classification)", r5["current_regime"] is None, r5)

r6 = main._lm_classify_oi_regime([], lookback=5)
check("3-6 empty candles -> safe empty result", r6["current_regime"] is None and r6["sample_count"] == 0, r6)


# ══════════════════════════════════════════════════════════════════════════════
# GROUP 4 — GET /api/live-monitor/flow-divergence (real route)
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 4 — real /api/live-monitor/flow-divergence route")

anon = main.app.test_client()
r = anon.get("/api/live-monitor/flow-divergence?symbol=XUSDT")
check("4-1 unauthenticated -> 302 redirect", r.status_code == 302, r.status_code)

r = client.get("/api/live-monitor/flow-divergence")
check("4-2 missing symbol -> 400", r.status_code == 400)
r = client.get("/api/live-monitor/flow-divergence?symbol=XUSDT&tf=2m")
check("4-3 invalid tf -> 400", r.status_code == 400)

r = client.get("/api/live-monitor/flow-divergence?symbol=XUSDT")
b = r.get_json()
check("4-4 too little history -> ok=True, empty results, message present",
      r.status_code == 200 and b["ok"] and b["cvd_divergences"] == [] and b["oi_regime"] is None
      and "message" in b, b)
check("4-5 default tf is 5m when unspecified", b["timeframe"] == "5m", b["timeframe"])
r_blank = client.get("/api/live-monitor/flow-divergence?symbol=XUSDT&tf=")
check("4-6 explicit blank tf= also defaults to 5m (not 1m)",
      r_blank.get_json()["timeframe"] == "5m", r_blank.get_json()["timeframe"])

T0 = 1_755_200_000_000 // 60000 * 60000
prices = [90,95,98,100,97,94,96,99,105,110,107,103,101]
cvds   = [100,200,400,500,450,400,420,350,320,300,280,260,240]
with main.app.app_context():
    for i, (p, cv) in enumerate(zip(prices, cvds)):
        db.session.add(FC(symbol="DIVUSDT", timeframe="1m", candle_open_ms=T0 + i*60000,
                          price_close=p, buy_vol_usd=0, sell_vol_usd=0, delta_usd=0,
                          cvd_usd=cv, tick_count=1))
    db.session.commit()

r = client.get("/api/live-monitor/flow-divergence?symbol=DIVUSDT&tf=1m")
b = r.get_json()
check("4-7 sufficient history -> ok, candle_count=13", r.status_code == 200 and b["ok"] and b["candle_count"] == 13,
      b.get("candle_count"))
check("4-8 finds the regular_bearish divergence via the real route",
      any(d["kind"] == "regular_bearish" for d in b["cvd_divergences"]), b["cvd_divergences"])
check("4-9 oi_regime present structurally", b["oi_regime"] is not None and "current_regime" in b["oi_regime"],
      b["oi_regime"])

r = client.get("/api/live-monitor/flow-divergence?symbol=DIVUSDT&tf=1m&limit=-5")
check("4-10 negative limit doesn't crash (clamped)", r.status_code == 200)
r = client.get("/api/live-monitor/flow-divergence?symbol=DIVUSDT&tf=1m&limit=abc")
check("4-11 garbage limit doesn't crash (falls back to default)", r.status_code == 200)


# ══════════════════════════════════════════════════════════════════════════════
# GROUP 5 — Data Health rows via the real context builder
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 5 — CVD Divergence + OI Regime Data Health rows")

def rowmap(ctx):
    return {row["metric"]: row for row in ctx.get("rows", [])}

with main.app.app_context():
    ctx = main._lm_build_data_health_context("NODATAUSDT", "binance", snap=None, user_id=UID_A)
rm = rowmap(ctx)
check("5-1 CVD Divergence row present with no data", "CVD Divergence" in rm)
check("5-2 CVD Divergence unavailable with no data", rm["CVD Divergence"]["status"] == "unavailable")
check("5-3 OI Regime row present with no data", "OI Regime" in rm)
check("5-4 OI Regime unavailable with no data", rm["OI Regime"]["status"] == "unavailable")

now_ms = int(time.time() * 1000)
T1 = (now_ms // 300000 - 14) * 300000
prices5 = [90,95,98,100,97,94,96,99,105,110,107,103,101,99,97]
cvds5   = [100,200,400,500,450,400,420,350,320,300,280,260,240,220,200]
ois5    = [1000,1010,1020,1030,1040,1050,1060,1070,1080,1090,1100,1110,1120,1130,1140]
with main.app.app_context():
    for i, (p, cv, oi) in enumerate(zip(prices5, cvds5, ois5)):
        db.session.add(FC(symbol="DHDIVUSDT", timeframe="1m", candle_open_ms=T1 + i*300000,
                          price_close=p, buy_vol_usd=0, sell_vol_usd=0, delta_usd=0,
                          cvd_usd=cv, oi_close=oi, tick_count=1))
    db.session.commit()
    ctx2 = main._lm_build_data_health_context("DHDIVUSDT", "binance", snap=None, user_id=UID_A)
rm2 = rowmap(ctx2)
check("5-5 CVD Divergence row detects regular_bearish shape",
      "Regular Bearish" in rm2["CVD Divergence"]["value"], rm2["CVD Divergence"])
check("5-6 CVD Divergence status fresh for recent candle", rm2["CVD Divergence"]["status"] == "fresh")
check("5-7 OI Regime current shows New Shorts (last step: price dipped, OI kept rising)",
      "New Shorts" in rm2["OI Regime"]["value"], rm2["OI Regime"])
check("5-8 OI Regime dominant-across-window shows New Longs (overall trend)",
      "New Longs" in rm2["OI Regime"]["notes"], rm2["OI Regime"]["notes"])
metrics = [row["metric"] for row in ctx2["rows"]]
check("5-9 no duplicate CVD Divergence row", metrics.count("CVD Divergence") == 1)
check("5-10 no duplicate OI Regime row", metrics.count("OI Regime") == 1)


# ══════════════════════════════════════════════════════════════════════════════
# GROUP 6 — AI execution context order_flow_series section
# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 6 — order_flow_series in AI execution context")

from live_monitor.ai_execution_context import _lm_build_ai_execution_context
from unittest.mock import patch

with main.app.app_context():
    item = LiveMonitorItem(user_id=UID_A, symbol="AICTXUSDT", exchange="binance",
                           market="perpetual", source_tab="main", setup_type="MANUAL",
                           direction="neutral", timeframe="4h", zone_high=100.0, zone_low=90.0,
                           current_price=95.0, is_active=True, status="watching")
    db.session.add(item)
    db.session.commit()
    item_id = item.id

with main.app.app_context():
    row = LiveMonitorItem.query.get(item_id)
    ctx3 = _lm_build_ai_execution_context(row, snapshot={})
check("6-1 context builds without crashing", ctx3.get("ok") is True, ctx3.get("error"))
check("6-2 order_flow_series key present", "order_flow_series" in ctx3)
ofs = ctx3.get("order_flow_series")
check("6-3 gracefully reports insufficient_history with no flow candles",
      ofs is not None and ofs.get("ok") is False and ofs.get("reason") == "insufficient_history", ofs)
check("6-4 danger_context unaffected", "danger_context" in ctx3 and ctx3["danger_context"] is not None)
check("6-5 ai_allowed_actions_preview unaffected", isinstance(ctx3.get("ai_allowed_actions_preview"), list))

now_ms2 = int(time.time() * 1000)
T2 = (now_ms2 // 300000 - 24) * 300000
prices24 = [90,95,98,100,97,94,96,99,105,110,107,103,101,99,97,96,95,94,93,92,91,90,89,88]
cvds24   = [100,200,400,500,450,400,420,350,320,300,280,260,240,220,200,190,180,170,160,150,140,130,120,110]
ois24    = [i*10 + 1000 for i in range(24)]
with main.app.app_context():
    for i, (p, cv, oi) in enumerate(zip(prices24, cvds24, ois24)):
        db.session.add(FC(symbol="AICTXUSDT", timeframe="1m", candle_open_ms=T2 + i*300000,
                          price_close=p, buy_vol_usd=0, sell_vol_usd=0, delta_usd=0,
                          cvd_usd=cv, oi_close=oi, tick_count=1))
    db.session.commit()
    row2 = LiveMonitorItem.query.get(item_id)
    ctx4 = _lm_build_ai_execution_context(row2, snapshot={})
ofs2 = ctx4.get("order_flow_series")
check("6-6 ok=True with sufficient history", ofs2 is not None and ofs2.get("ok") is True, ofs2)
check("6-7 recent_candles bounded to 12", len(ofs2.get("recent_candles", [])) == 12)
check("6-8 cvd_divergences bounded to 3", len(ofs2.get("cvd_divergences", [])) <= 3, ofs2.get("cvd_divergences"))
check("6-9 finds the regular_bearish divergence through the AI context path",
      any(d["kind"] == "regular_bearish" for d in ofs2.get("cvd_divergences", [])), ofs2.get("cvd_divergences"))
check("6-10 oi_regime has current+dominant fields",
      "current" in ofs2.get("oi_regime", {}) and "dominant_recent" in ofs2.get("oi_regime", {}))

with main.app.app_context():
    row3 = LiveMonitorItem.query.get(item_id)
    with patch.object(main, "_lm_get_flow_candles_series", side_effect=RuntimeError("boom")):
        ctx5 = _lm_build_ai_execution_context(row3, snapshot={})
check("6-11 error isolated: overall context still ok=True", ctx5.get("ok") is True, ctx5.get("error"))
ofs3 = ctx5.get("order_flow_series")
check("6-12 order_flow_series reports the error gracefully", ofs3 is not None and ofs3.get("ok") is False
      and "error" in ofs3.get("reason", ""), ofs3)
check("6-13 rest of context unaffected by the flow-series error",
      "danger_context" in ctx5 and ctx5["danger_context"] is not None)


# ══════════════════════════════════════════════════════════════════════════════
# Final summary
# ══════════════════════════════════════════════════════════════════════════════
print(f"\n{'='*60}")
print(f"  TOTAL: {_pass+_fail}   PASS: {_pass}   FAIL: {_fail}")
print(f"{'='*60}")
if _fail:
    sys.exit(1)
