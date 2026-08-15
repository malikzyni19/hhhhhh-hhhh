"""Phase SIG-2 — signal intake (all scan tabs) + cross-module confluence.

Drives the real adapters and the real confluence engine against the actual
result shapes each scanner produces, then exercises the live /api/scan route
end to end to prove intake is genuinely wired in.

Run: python3 _test_signal_funnel.py
"""
import os, sys, types, tempfile, json
from datetime import datetime, timezone, timedelta

_dbf = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False).name
os.environ["DATABASE_URL"]   = f"sqlite:///{_dbf}"
os.environ["SECRET_KEY"]     = "test_secret_signal_funnel"
os.environ.setdefault("RESEND_API_KEY",  "test")
os.environ.setdefault("TURNSTILE_SECRET", "")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
for _mn in ["psycopg2", "psycopg2.extras", "resend"]:
    if _mn not in sys.modules:
        sys.modules[_mn] = types.ModuleType(_mn)

import main                                                     # noqa: E402
from models import (db, User, LiveMonitorSignalCandidate as C,  # noqa: E402
                    LiveMonitorSignalSettings)
from live_monitor.signal_intake import (                        # noqa: E402
    normalize_scan_results, record_candidates, expire_stale_candidates,
    build_candidate_key, adapt_zone_scan, adapt_compressed,
    adapt_ath_atl, adapt_bias, adapt_accumulation,
)
from live_monitor.signal_confluence import (                    # noqa: E402
    build_confluence_groups, filter_groups_for_settings, score_group,
)
from live_monitor.signal_settings import get_or_create_signal_settings  # noqa: E402

with main.app.app_context():
    db.create_all()
    _u = User(username="funnel", password_hash="x", role="user", status="active")
    db.session.add(_u); db.session.commit()
    UID = _u.id

_pass = 0; _fail = 0
def check(name, cond, info=""):
    global _pass, _fail
    if cond: print(f"  PASS  {name}"); _pass += 1
    else:    print(f"  FAIL  {name}" + (f"  [{info}]" if info else "")); _fail += 1
def section(t): print(f"\n── {t} ──")

NOW = datetime(2026, 8, 13, 12, 0, 0, tzinfo=timezone.utc)


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 1 — adapters normalize each scanner's own shape")
# ══════════════════════════════════════════════════════════════════════════════

zone_result = {
    "symbol": "BTCUSDT", "price": 62000, "timeframe": "4h",
    "topAlerts": [
        {"type": "ob",  "side": "bullish", "zoneHigh": 61800, "zoneLow": 61200, "score": 82},
        {"type": "fvg", "side": "bull",    "zoneHigh": 61900, "zoneLow": 61500, "score": 61},
        {"type": "noise", "side": "bull"},          # unknown kind → ignored
    ],
}
cands = adapt_zone_scan(zone_result, now=NOW)
check("1-1 zone scan yields one candidate per known alert", len(cands) == 2, len(cands))
check("1-2 unknown alert type skipped, not fatal",
      all(c["module"] in ("ob", "fvg") for c in cands))
check("1-3 bullish variants map to long",
      all(c["direction"] == "long" for c in cands), [c["direction"] for c in cands])
check("1-4 zone carried through", cands[0]["zone_high"] == 61800)

flipped = adapt_zone_scan({"symbol": "X", "topAlerts": [
    {"type": "ob", "side": "bear", "zoneHigh": 100, "zoneLow": 200}]}, now=NOW)
check("1-5 inverted zone bounds are corrected",
      flipped[0]["zone_high"] == 200 and flipped[0]["zone_low"] == 100, flipped[0])

comp = adapt_compressed({"symbol": "ETHUSDT", "price": 3000, "timeframe": "1h",
                         "boxHigh": 3050, "boxLow": 2950,
                         "compressionScore": 74, "compressionGrade": "A"}, now=NOW)
check("1-6 compressed becomes a neutral candidate (direction unknown by design)",
      comp[0]["direction"] == "neutral", comp[0]["direction"])
check("1-7 squeeze box becomes the zone", comp[0]["zone_high"] == 3050)
check("1-8 native score preserved", comp[0]["score"] == 74)

ath_up = adapt_ath_atl({"symbol": "SOLUSDT", "price": 150, "statusKind": "ath_break",
                        "breakPct": 2.0, "windowHigh": 149, "windowLow": 120}, now=NOW)
check("1-9 ATH break reads long", ath_up[0]["direction"] == "long", ath_up[0])
atl_dn = adapt_ath_atl({"symbol": "SOLUSDT", "statusKind": "atl_break",
                        "breakPct": 1.0}, now=NOW)
check("1-10 ATL break reads short", atl_dn[0]["direction"] == "short")
near = adapt_ath_atl({"symbol": "SOLUSDT", "statusKind": "near_ath",
                      "distancePct": 0.5}, now=NOW)
check("1-11 approaching an extreme stays neutral (resolves either way)",
      near[0]["direction"] == "neutral", near[0]["direction"])
check("1-12 derived score stays inside 0-100",
      0 <= ath_up[0]["score"] <= 100 and 0 <= near[0]["score"] <= 100)

bias = adapt_bias({"symbol": "BTCUSDT", "price": 62000, "timeframe": "4h",
                   "bias": "bullish", "score": 88, "grade": "A"}, now=NOW)
check("1-13 bias maps direction and score directly",
      bias[0]["direction"] == "long" and bias[0]["score"] == 88, bias[0])

acc_b = adapt_accumulation({"symbol": "BTCUSDT", "mode": "bullish",
                            "changePct": 5.0, "relVol": 3.0, "price": 62000}, now=NOW)
check("1-14 trending bullish mode reads long", acc_b[0]["direction"] == "long")
acc_flat = adapt_accumulation({"symbol": "X", "mode": "movers",
                               "changePct": 0.4, "relVol": 1.1}, now=NOW)
check("1-15 an undirected small move stays neutral",
      acc_flat[0]["direction"] == "neutral", acc_flat[0]["direction"])
check("1-16 higher volume scores higher",
      acc_b[0]["score"] > acc_flat[0]["score"], (acc_b[0]["score"], acc_flat[0]["score"]))

check("1-17 garbage input returns empty, never raises",
      adapt_bias(None) == [] and adapt_compressed("nope") == [] and
      adapt_zone_scan({"topAlerts": "bad"}) == [])
check("1-18 a result with no symbol is dropped",
      adapt_bias({"bias": "bullish"}, now=NOW) == [])


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 2 — dedup identity")
# ══════════════════════════════════════════════════════════════════════════════

k1 = build_candidate_key("ob", "BTCUSDT", "binance", "4h", "long", 100)
k2 = build_candidate_key("ob", "BTCUSDT", "binance", "4h", "long", 100)
k3 = build_candidate_key("ob", "BTCUSDT", "binance", "4h", "short", 100)
k4 = build_candidate_key("ob", "BTCUSDT", "binance", "4h", "long", 200)
check("2-1 same setup, same candle → same key", k1 == k2)
check("2-2 opposite direction → different key", k1 != k3)
check("2-3 different candle → different key", k1 != k4)

later_same_candle = NOW + timedelta(minutes=30)     # still inside the 4h candle
next_candle       = NOW + timedelta(hours=5)
a = adapt_bias({"symbol": "B", "timeframe": "4h", "bias": "bullish"}, now=NOW)[0]
b = adapt_bias({"symbol": "B", "timeframe": "4h", "bias": "bullish"}, now=later_same_candle)[0]
c_ = adapt_bias({"symbol": "B", "timeframe": "4h", "bias": "bullish"}, now=next_candle)[0]
check("2-4 re-detected inside the same candle is the same candidate",
      a["candidate_key"] == b["candidate_key"])
check("2-5 the next candle is a new candidate",
      a["candidate_key"] != c_["candidate_key"])


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 3 — persistence refreshes rather than duplicates")
# ══════════════════════════════════════════════════════════════════════════════

with main.app.app_context():
    r1 = record_candidates(adapt_bias(
        {"symbol": "DUPUSDT", "timeframe": "4h", "bias": "bullish", "score": 70}, now=NOW))
    check("3-1 first write inserts", r1["inserted"] == 1, r1)

    r2 = record_candidates(adapt_bias(
        {"symbol": "DUPUSDT", "timeframe": "4h", "bias": "bullish", "score": 85},
        now=NOW + timedelta(minutes=10)))
    check("3-2 same candle re-scan refreshes instead of inserting",
          r2["refreshed"] == 1 and r2["inserted"] == 0, r2)
    check("3-3 exactly one row exists",
          C.query.filter_by(symbol="DUPUSDT").count() == 1)
    check("3-4 score updated to the newer reading",
          C.query.filter_by(symbol="DUPUSDT").first().score == 85)

    check("3-5 empty input is a safe no-op",
          record_candidates([])["inserted"] == 0)

    # Regression: a single scan can contain the same setup twice, because the
    # round-robin pair picker wraps and repeats symbols whenever the pair list
    # is shorter than the batch size. Two identical keys in one INSERT batch
    # used to trip the unique constraint and lose the ENTIRE batch.
    dupe_batch = (adapt_bias({"symbol": "TWICEUSDT", "timeframe": "4h",
                              "bias": "bullish", "score": 70}, now=NOW) +
                  adapt_bias({"symbol": "TWICEUSDT", "timeframe": "4h",
                              "bias": "bullish", "score": 70}, now=NOW) +
                  adapt_bias({"symbol": "SURVIVEUSDT", "timeframe": "4h",
                              "bias": "bearish", "score": 65}, now=NOW))
    r3 = record_candidates(dupe_batch)
    check("3-5b a repeated setup inside one batch does not lose the batch",
          r3["inserted"] == 2, r3)
    check("3-5c the repeat is folded into one row",
          C.query.filter_by(symbol="TWICEUSDT").count() == 1)
    check("3-5d the other candidate in the batch still survived",
          C.query.filter_by(symbol="SURVIVEUSDT").count() == 1)

    # Expiry sweep
    old = adapt_bias({"symbol": "OLDUSDT", "timeframe": "5m", "bias": "bearish"},
                     now=NOW - timedelta(days=2))
    record_candidates(old)
    n = expire_stale_candidates(now=NOW)
    check("3-6 stale candidates expire", n >= 1, n)
    check("3-7 expired row is no longer active",
          C.query.filter_by(symbol="OLDUSDT").first().status == "expired")


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 4 — confluence scoring")
# ══════════════════════════════════════════════════════════════════════════════

class _Fake:
    def __init__(self, **kw):
        self.id = kw.pop("id", 1)
        self.zone_high = kw.pop("zone_high", None)
        self.zone_low  = kw.pop("zone_low", None)
        self.timeframe = kw.pop("timeframe", "4h")
        self.detected_price = kw.pop("price", 100)
        self.exchange = "binance"; self.market = "perpetual"
        for k, v in kw.items(): setattr(self, k, v)

one   = [_Fake(module="ob", symbol="A", direction="long", score=80,
               zone_high=101, zone_low=99)]
three = [_Fake(module="ob",         symbol="A", direction="long", score=80, zone_high=101, zone_low=99),
         _Fake(module="bias_shift", symbol="A", direction="long", score=80, zone_high=101, zone_low=99),
         _Fake(module="fvg",        symbol="A", direction="long", score=80, zone_high=100.5, zone_low=99.5)]

s1, s3 = score_group(one), score_group(three)
check("4-1 three agreeing modules beat one", s3["strength"] > s1["strength"],
      (s1["strength"], s3["strength"]))
check("4-2 module count reported", s3["module_count"] == 3, s3["module_count"])
check("4-3 overlapping zones score full agreement", s3["zone_agreement"] == 1.0,
      s3["zone_agreement"])

apart = [_Fake(module="ob",         symbol="A", direction="long", score=80, zone_high=101, zone_low=99),
         _Fake(module="bias_shift", symbol="A", direction="long", score=80, zone_high=500, zone_low=490)]
s_apart = score_group(apart)
check("4-4 zones far apart score no agreement", s_apart["zone_agreement"] == 0.0,
      s_apart["zone_agreement"])
check("4-5 aligned zones beat scattered ones at equal confluence",
      score_group(three[:2])["strength"] > s_apart["strength"])

dupe_mod = [_Fake(module="ob", symbol="A", direction="long", score=90, id=1),
            _Fake(module="ob", symbol="A", direction="long", score=90, id=2)]
check("4-6 one module firing twice is not counted as confluence",
      score_group(dupe_mod)["module_count"] == 1)

neutral_only = [_Fake(module="compressed", symbol="A", direction="neutral", score=90)]
directional  = [_Fake(module="ob",         symbol="A", direction="long",    score=90)]
check("4-7 a neutral find alone never outranks a directional one",
      score_group(neutral_only)["strength"] < score_group(directional)["strength"],
      (score_group(neutral_only)["strength"], score_group(directional)["strength"]))
check("4-8 empty group is safe", score_group([]) == {})


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 5 — grouping over stored candidates")
# ══════════════════════════════════════════════════════════════════════════════

with main.app.app_context():
    C.query.delete(); db.session.commit()
    batch = []
    batch += adapt_zone_scan({"symbol": "CONFUSDT", "price": 100, "timeframe": "4h",
        "topAlerts": [{"type": "ob", "side": "bull", "zoneHigh": 101, "zoneLow": 99, "score": 85}]}, now=NOW)
    batch += adapt_bias({"symbol": "CONFUSDT", "price": 100, "timeframe": "4h",
                         "bias": "bullish", "score": 80}, now=NOW)
    batch += adapt_compressed({"symbol": "CONFUSDT", "price": 100, "timeframe": "1h",
                               "boxHigh": 101, "boxLow": 99, "compressionScore": 70}, now=NOW)
    batch += adapt_bias({"symbol": "SOLOUSDT", "price": 50, "timeframe": "4h",
                         "bias": "bearish", "score": 60}, now=NOW)
    record_candidates(batch)

    groups = build_confluence_groups(window_hours=12, now=NOW)
    by_sym = {g["symbol"]: g for g in groups}
    check("5-1 both symbols grouped", len(by_sym) == 2, list(by_sym))

    conf = by_sym.get("CONFUSDT")
    check("5-2 multi-module symbol grouped as long", conf and conf["direction"] == "long")
    check("5-3 neutral squeeze counted as support alongside directional modules",
          conf and "compressed" in conf["modules"], conf and conf["modules"])
    check("5-4 confluence symbol ranks above the single-module one",
          groups[0]["symbol"] == "CONFUSDT", [g["symbol"] for g in groups])
    check("5-5 single-module symbol still surfaces",
          by_sym.get("SOLOUSDT", {}).get("module_count") == 1)
    check("5-6 group carries a usable price zone",
          conf and conf["zone_high"] is not None and conf["zone_low"] is not None, conf)

    check("5-7 min_modules filters out weak groups",
          all(g["module_count"] >= 2
              for g in build_confluence_groups(window_hours=12, min_modules=2, now=NOW)))
    check("5-8 window excludes older detections",
          build_confluence_groups(window_hours=12, now=NOW + timedelta(days=3)) == [])


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 6 — per-user settings filter the groups")
# ══════════════════════════════════════════════════════════════════════════════

with main.app.app_context():
    st = get_or_create_signal_settings(UID)
    groups = build_confluence_groups(window_hours=12, now=NOW)

    kept = filter_groups_for_settings(groups, st)
    check("6-1 defaults (all modules on) keep everything", len(kept) == len(groups),
          (len(kept), len(groups)))

    st.min_confluence = 2
    kept = filter_groups_for_settings(groups, st)
    check("6-2 raising required confluence drops single-module groups",
          all(len(g["matched_modules"]) >= 2 for g in kept) and len(kept) < len(groups),
          [(g["symbol"], g["matched_modules"]) for g in kept])

    st.min_confluence = 1
    st.allow_long = False
    kept = filter_groups_for_settings(groups, st)
    check("6-3 disabling longs removes long groups",
          all(g["direction"] != "long" for g in kept), [g["direction"] for g in kept])

    st.allow_long = True
    st.enabled_modules_json = json.dumps(["bias_shift"])
    kept = filter_groups_for_settings(groups, st)
    check("6-4 only enabled modules count toward confluence",
          all(g["matched_modules"] == ["bias_shift"] for g in kept),
          [g["matched_modules"] for g in kept])

    st.enabled_modules_json = json.dumps(["ob", "bias_shift", "compressed"])
    st.coin_scope = "watchlist"
    st.symbols_json = json.dumps(["CONFUSDT"])
    kept = filter_groups_for_settings(groups, st)
    check("6-5 watchlist scope restricts to chosen pairs",
          all(g["symbol"] == "CONFUSDT" for g in kept) and len(kept) >= 1,
          [g["symbol"] for g in kept])

    # Scanner-backed scope filters exactly like the typed list.
    from unittest.mock import patch as _p3
    import main as _mm
    st.coin_scope = "selected_pairs"
    st.symbols_json = json.dumps([])          # deliberately empty: not used here
    with _p3.object(_mm, "load_user_watchlist", return_value=["CONFUSDT"]):
        kept = filter_groups_for_settings(groups, st)
    check("6-5b Scanner-selected pairs filter the same way",
          kept and all(g["symbol"] == "CONFUSDT" for g in kept),
          [g["symbol"] for g in kept])
    with _p3.object(_mm, "load_user_watchlist", return_value=["NOTHINGUSDT"]):
        check("6-5c a Scanner list matching nothing yields no groups",
              filter_groups_for_settings(groups, st) == [])

    st.coin_scope = "top_volume"
    st.timeframes_json = json.dumps(["1d"])
    check("6-6 timeframe filter excludes non-matching groups",
          filter_groups_for_settings(groups, st) == [])

    st.timeframes_json = json.dumps([])
    check("6-7 no settings row is handled safely",
          filter_groups_for_settings(groups, None) == [])
    db.session.rollback()


# ══════════════════════════════════════════════════════════════════════════════
section("GROUP 7 — intake is really wired into the live /api/scan route")
# ══════════════════════════════════════════════════════════════════════════════

client = main.app.test_client()
with client.session_transaction() as s:
    s["logged_in"] = True; s["username"] = "funnel"

with main.app.app_context():
    C.query.delete(); db.session.commit()

from unittest.mock import patch                                   # noqa: E402

_ALERT = {"type": "ob", "side": "bullish", "zoneHigh": 101, "zoneLow": 99,
          "score": 77, "strength": 77}
_FAKE_SCAN = [{
    "symbol": "HOOKUSDT", "price": 100, "timeframe": "4h",
    "topAlert":  _ALERT,          # the route sorts on this
    "topAlerts": [_ALERT],        # intake reads this
}]

# Replace only the per-pair analysis; the route's intake hook still runs for real.
with patch.object(main, "analyze_pair", return_value=_FAKE_SCAN[0]), \
     patch.object(main, "get_klines_exchange", return_value=[{"open":1,"high":1,"low":1,"close":1,"volume":1}]*60), \
     patch.object(main, "get_pairs_exchange", return_value=[{"symbol": "HOOKUSDT"}]):
    resp = client.post("/api/scan", json={
        "symbols": ["HOOKUSDT"], "exchange": "binance",
        "market": "perpetual", "scanMode": "selected", "settings": {},
    })

check("7-1 scan route still returns 200 with intake attached",
      resp.status_code == 200, resp.status_code)

with main.app.app_context():
    rows = C.query.filter_by(symbol="HOOKUSDT").all()
check("7-2 a candidate was recorded by the real route", len(rows) == 1,
      [(r.module, r.direction) for r in rows])
if rows:
    check("7-3 recorded with the right module and direction",
          rows[0].module == "ob" and rows[0].direction == "long",
          (rows[0].module, rows[0].direction))

# Intake must never be able to break a scan.
with patch.object(main, "analyze_pair", return_value=_FAKE_SCAN[0]), \
     patch.object(main, "get_klines_exchange", return_value=[{"open":1,"high":1,"low":1,"close":1,"volume":1}]*60), \
     patch.object(main, "get_pairs_exchange", return_value=[{"symbol": "HOOKUSDT"}]), \
     patch.object(main, "record_candidates", side_effect=RuntimeError("db down")):
    resp2 = client.post("/api/scan", json={
        "symbols": ["HOOKUSDT"], "exchange": "binance",
        "market": "perpetual", "scanMode": "selected", "settings": {},
    })
check("7-4 a failing intake never breaks the scan response",
      resp2.status_code == 200, resp2.status_code)


print(f"\n{'='*60}")
print(f"  TOTAL: {_pass+_fail}   PASS: {_pass}   FAIL: {_fail}")
print(f"{'='*60}")
if _fail:
    sys.exit(1)
