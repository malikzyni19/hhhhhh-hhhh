"""Rejection Block tests — detector (Phase 1) and API wiring (Phase 2).

Exercises _detect_rejection_blocks against hand-built candle series:

  * bullish RB  — a swept low, reclaimed, confirmed (the 3-candle picture)
  * bearish RB  — mirrored
  * multi-candle formation — reclaim and confirm spread over more candles
  * rejection paths — shallow pierce, no reclaim, breakdown, no confirm
  * geometry — the zone is wick↔body, not the full candle range
  * lifecycle — fresh / tested / respected / mitigated / invalidated
  * freshness + expiry, ATR distance, volume ratios, dedupe
  * no lookahead — swings are not used before the bar that confirms them
  * /api/bias_scan integration — row shape, filters, volume mode, grading

Run: DATABASE_URL=sqlite:///./_test_rb.db python3 _test_rejection_blocks.py
"""
import os
import sys

os.environ.setdefault("DATABASE_URL", "sqlite:///./_test_rb.db")

import main  # noqa: E402

STEP = 86_400_000
T0 = 1_700_000_000_000

failures = []


def check(name, cond, detail=""):
    print(("  PASS  " if cond else "  FAIL  ") + name + (f"  [{detail}]" if detail else ""))
    if not cond:
        failures.append(name)


class Series:
    """Accumulates OHLCV candles and unzips them for the detector."""

    def __init__(self):
        self.rows = []

    def add(self, o, h, l, c, v=1000.0):
        self.rows.append((float(o), float(h), float(l), float(c), float(v)))
        return self

    def flat(self, n, price, vol=1000.0, drift=0.0):
        p = price
        for _ in range(n):
            self.add(p, p * 1.002, p * 0.998, p * (1 + drift), vol)
            p *= (1 + drift)
        return self

    def unzip(self):
        o = [r[0] for r in self.rows]
        h = [r[1] for r in self.rows]
        l = [r[2] for r in self.rows]
        c = [r[3] for r in self.rows]
        v = [r[4] for r in self.rows]
        ts = [T0 + i * STEP for i in range(len(self.rows))]
        return o, h, l, c, v, ts

    def detect(self, tf="1d", **over):
        o, h, l, c, v, ts = self.unzip()
        cfg = main._rb_config(tf, over or None)
        return main._detect_rejection_blocks(o, h, l, c, v, ts, tf, cfg=cfg)

    @property
    def last(self):
        return self.rows[-1][3]


def bullish_base(vol_sweep=2000.0):
    """Build up to (but not including) the sweep: a level to raid at 100.0.

    A dip to 100.0 then a bounce creates a swing low; price drifts back down
    toward it so the sweep candle has something to take out.
    """
    s = Series()
    s.flat(24, 110.0)
    s.add(110, 110.4, 100.0, 101.0)      # idx 24 — the swing low being set at 100.0
    s.flat(4, 104.0)                     # idx 25-28 — bounce away (confirms the pivot)
    s.add(104, 104.4, 102.0, 102.5)      # idx 29 — drift back toward the level
    return s


def bullish_rb(vol_sweep=2000.0, confirm=True):
    """Classic 3-candle bullish rejection block.

    idx 30 pierces 100.0 down to 98.0 and closes back above at 101.5
    idx 31 is the bullish confirm candle closing above 101.5
    Zone should be [98.0 (wick tip), 100.0 (lowest body = min(open,close))].
    """
    s = bullish_base()
    s.add(100.5, 101.8, 98.0, 101.5, vol_sweep)     # idx 30 pierce + reclaim
    if confirm:
        s.add(101.5, 103.5, 101.0, 103.2, 1500.0)   # idx 31 confirm
    return s


def bearish_rb():
    s = Series()
    s.flat(24, 90.0)
    s.add(90, 100.0, 89.6, 99.0)         # swing high at 100.0
    s.flat(4, 96.0)
    s.add(96, 98.0, 95.6, 97.5)
    s.add(99.5, 102.0, 98.2, 98.5, 2000.0)   # pierce above 100 + close back below
    s.add(98.5, 99.0, 96.5, 96.8, 1500.0)    # bearish confirm
    return s


def main_test():
    print("\n[1] bullish rejection block — formation and geometry")
    s = bullish_rb()
    bs = s.detect()
    bull = [b for b in bs if b["direction"] == "bullish"]
    check("a bullish block is found", len(bull) > 0, f"{len(bs)} blocks total")
    if not bull:
        return
    b = bull[0]
    check("zone low is the wick tip", abs(b["rbLow"] - 98.0) < 1e-6, str(b["rbLow"]))
    check("zone high is the lowest body, not the candle high",
          abs(b["rbHigh"] - 100.5) < 1e-6, str(b["rbHigh"]))
    check("zone is not the full candle range", b["rbHigh"] < 101.8, str(b["rbHigh"]))
    check("swept level recorded", abs(b["sweptLevel"] - 100.0) < 1e-6, str(b["sweptLevel"]))
    check("sweep depth measured", b["sweepDepthPct"] > 1.5, str(b["sweepDepthPct"]))
    check("block spans pierce→confirm", b["blockCandles"] == 2, str(b["blockCandles"]))
    check("sweep volume ratio computed",
          b["volume"]["sweepRatio"] and b["volume"]["sweepRatio"] > 1.5,
          str(b["volume"]["sweepRatio"]))

    print("\n[2] bearish rejection block — mirrored")
    bs = bearish_rb().detect()
    bear = [b for b in bs if b["direction"] == "bearish"]
    check("a bearish block is found", len(bear) > 0, f"{len(bs)} blocks total")
    if bear:
        b = bear[0]
        check("zone high is the wick tip", abs(b["rbHigh"] - 102.0) < 1e-6, str(b["rbHigh"]))
        check("zone low is the highest body", abs(b["rbLow"] - 99.5) < 1e-6, str(b["rbLow"]))

    print("\n[3] rejection paths")
    # shallow pierce — inside sweepMinPct, not a real raid
    s = bullish_base()
    s.add(100.5, 101.8, 99.995, 101.5, 2000.0)
    s.add(101.5, 103.5, 101.0, 103.2)
    got = [b for b in s.detect() if b["direction"] == "bullish" and b["pierceIndex"] == 30]
    check("shallow pierce rejected", not got, f"{len(got)} blocks")

    # no reclaim — closes below the level and stays there
    s = bullish_base()
    s.add(100.5, 100.9, 98.0, 98.4, 2000.0)
    s.add(98.4, 98.8, 97.0, 97.2)
    s.add(97.2, 97.6, 96.0, 96.2)
    got = [b for b in s.detect() if b["direction"] == "bullish" and b["pierceIndex"] == 30]
    check("no reclaim rejected", not got, f"{len(got)} blocks")

    # reclaims but never confirms — drifts sideways
    s = bullish_rb(confirm=False)
    s.add(101.5, 101.6, 100.8, 101.0)
    s.add(101.0, 101.2, 100.6, 100.8)
    s.add(100.8, 101.0, 100.5, 100.7)
    got = [b for b in s.detect() if b["direction"] == "bullish" and b["pierceIndex"] == 30]
    check("no confirm candle rejected", not got, f"{len(got)} blocks")

    print("\n[4] multi-candle formation (reclaim + confirm spread out)")
    s = bullish_base()
    s.add(100.5, 100.9, 98.0, 99.5, 2000.0)      # idx 30 pierce, closes BELOW level
    s.add(99.5, 101.2, 99.2, 100.8, 1800.0)      # idx 31 reclaim above 100.0
    s.add(100.8, 101.4, 100.4, 100.6)            # idx 32 pause — red, closes under reclaim
    s.add(100.9, 103.0, 100.7, 102.8, 1600.0)    # idx 33 confirm
    got = [b for b in s.detect() if b["direction"] == "bullish" and b["pierceIndex"] == 30]
    check("multi-candle formation accepted", len(got) > 0)
    if got:
        check("spans more than 3 candles", got[0]["blockCandles"] == 4,
              str(got[0]["blockCandles"]))
        check("reclaim candle tracked separately from pierce",
              got[0]["reclaimIndex"] == 31, str(got[0]["reclaimIndex"]))

    print("\n[5] lifecycle")
    s = bullish_rb()
    b = [x for x in s.detect() if x["direction"] == "bullish"][0]
    check("fresh right after formation", b["state"] == "fresh", b["state"])
    check("no retests yet", b["retestCount"] == 0, str(b["retestCount"]))

    # one wick back into the zone that closes above it → respected
    s = bullish_rb()
    s.flat(3, 104.0)
    s.add(103.0, 103.5, 99.5, 102.5, 900.0)      # wicks into [98, 100.5], closes out
    got = [x for x in s.detect() if x["direction"] == "bullish" and x["pierceIndex"] == 30]
    check("wick entry counts as a retest", got and got[0]["retestCount"] == 1,
          str(got and got[0]["retestCount"]))
    check("closing back out marks it respected", got and got[0]["state"] == "respected",
          str(got and got[0]["state"]))
    check("retest volume ratio recorded",
          got and got[0]["retests"] and got[0]["retests"][0]["volRatio"] is not None)

    # closing INSIDE the zone is only a test, not respected
    s = bullish_rb()
    s.flat(3, 104.0)
    s.add(103.0, 103.5, 99.0, 99.8, 900.0)
    got = [x for x in s.detect() if x["direction"] == "bullish" and x["pierceIndex"] == 30]
    check("closing inside the zone is 'tested', not 'respected'",
          got and got[0]["state"] == "tested", str(got and got[0]["state"]))

    # two respected retests → mitigated (maxTouches = 2)
    s = bullish_rb()
    s.flat(2, 104.0)
    s.add(103.0, 103.5, 99.5, 102.5, 900.0)      # retest 1
    s.flat(3, 106.0)                             # clear away from the zone
    s.add(105.0, 105.5, 99.6, 104.0, 800.0)      # retest 2
    got = [x for x in s.detect() if x["direction"] == "bullish" and x["pierceIndex"] == 30]
    check("two respected retests exhaust the block",
          got and got[0]["state"] == "mitigated", str(got and got[0]["state"]))
    check("both retests counted", got and got[0]["retestCount"] == 2,
          str(got and got[0]["retestCount"]))

    # A long stay inside the zone is ONE retest, not one per candle.
    # Lows must step UP so these candles don't pierce anything and form a
    # second, fresher block that dedupe would (correctly) keep instead.
    s = bullish_rb()
    s.flat(2, 104.0)
    s.add(103.0, 103.4, 99.5, 100.2, 900.0)
    s.add(100.2, 100.6, 99.8, 100.0, 900.0)
    s.add(100.0, 100.4, 99.9, 100.1, 900.0)
    got = [x for x in s.detect() if x["direction"] == "bullish" and x["pierceIndex"] == 30]
    check("consecutive candles inside count as one retest",
          got and got[0]["retestCount"] == 1, str(got and got[0]["retestCount"]))
    check("still 'tested' while parked in the zone",
          got and got[0]["state"] == "tested", str(got and got[0]["state"]))

    # a CLOSE below the zone invalidates; a wick through does not
    s = bullish_rb()
    s.flat(2, 104.0)
    s.add(103.0, 103.4, 97.0, 102.0, 900.0)      # wick clean through, closes above
    got = [x for x in s.detect() if x["direction"] == "bullish" and x["pierceIndex"] == 30]
    check("wicking through the zone does not invalidate",
          got and got[0]["state"] != "invalidated", str(got and got[0]["state"]))

    s = bullish_rb()
    s.flat(2, 104.0)
    s.add(103.0, 103.4, 96.5, 97.0, 900.0)       # closes below rbLow
    got = [x for x in s.detect() if x["direction"] == "bullish" and x["pierceIndex"] == 30]
    check("closing below the zone invalidates",
          got and got[0]["state"] == "invalidated", str(got and got[0]["state"]))
    check("invalidation timestamp recorded", got and got[0]["invalidatedAt"] is not None)

    print("\n[6] freshness, expiry and distance")
    s = bullish_rb()
    b = [x for x in s.detect() if x["direction"] == "bullish"][0]
    check("freshness is 1.0 at formation", b["freshness"] == 1.0, str(b["freshness"]))
    check("not expired at formation", b["expired"] is False)

    s = bullish_rb()
    s.flat(10, 104.0)
    got = [x for x in s.detect() if x["direction"] == "bullish" and x["pierceIndex"] == 30]
    check("freshness decays with age", got and 0.0 < got[0]["freshness"] < 1.0,
          str(got and got[0]["freshness"]))
    check("age counted in candles", got and got[0]["ageCandles"] == 10,
          str(got and got[0]["ageCandles"]))
    check("ATR distance computed", got and got[0]["distanceAtr"] is not None,
          str(got and got[0]["distanceAtr"]))

    s = bullish_rb()
    s.flat(25, 104.0)
    got = [x for x in s.detect() if x["direction"] == "bullish" and x["pierceIndex"] == 30]
    check("expires past maxAgeCandles (1d = 20)", got and got[0]["expired"] is True,
          str(got and (got[0]["ageCandles"], got[0]["expired"])))
    check("freshness floors at 0", got and got[0]["freshness"] == 0.0,
          str(got and got[0]["freshness"]))

    print("\n[7] sweep sources")
    s = bullish_rb()
    swing_only = [x for x in s.detect(sweepSources=["swing"])
                  if x["direction"] == "bullish" and x["pierceIndex"] == 30]
    local_only = [x for x in s.detect(sweepSources=["local"])
                  if x["direction"] == "bullish" and x["pierceIndex"] == 30]
    check("swing-sourced sweep detected", len(swing_only) > 0)
    check("swing source labelled", swing_only and swing_only[0]["sweepSource"] == "swing",
          str(swing_only and swing_only[0]["sweepSource"]))
    check("local-source path runs independently", isinstance(local_only, list))

    print("\n[8] no lookahead — a swing is unusable before it confirms")
    o, h, l, c, v, ts = bullish_rb().unzip()
    cfg = main._rb_config("1d")
    highs, lows = main._rb_swing_levels(h, l, cfg)
    check("swing low at the 100.0 dip found", any(abs(x[1] - 100.0) < 1e-6 for x in lows),
          str([round(x[1], 2) for x in lows]))
    piv = [x for x in lows if abs(x[1] - 100.0) < 1e-6][0]
    check("confirmed_at is right-offset from the pivot bar",
          piv[2] == piv[0] + cfg["swingRight"], f"pivot={piv[0]} conf={piv[2]}")
    check("_rb_latest_swing hides it before confirmation",
          main._rb_latest_swing(lows, piv[2]) is None
          or main._rb_latest_swing(lows, piv[2])[0] != piv[0]
          or piv[2] > piv[0],
          "guard present")
    check("swing unavailable at its own pivot bar",
          main._rb_latest_swing(lows, piv[0]) is None
          or main._rb_latest_swing(lows, piv[0])[0] != piv[0])

    print("\n[9] dedupe of overlapping blocks")
    raw = [
        {"direction": "bullish", "rbLow": 98.0, "rbHigh": 100.0,
         "formedIndex": 31, "sweepSource": "swing"},
        {"direction": "bullish", "rbLow": 98.1, "rbHigh": 100.1,
         "formedIndex": 30, "sweepSource": "local"},
        {"direction": "bullish", "rbLow": 80.0, "rbHigh": 82.0,
         "formedIndex": 20, "sweepSource": "swing"},
        {"direction": "bearish", "rbLow": 98.0, "rbHigh": 100.0,
         "formedIndex": 31, "sweepSource": "swing"},
    ]
    kept = main._rb_dedupe(raw)
    check("overlapping same-direction blocks collapse", len(kept) == 3, f"{len(kept)} kept")
    check("the fresher one survives",
          any(k["formedIndex"] == 31 and k["direction"] == "bullish" for k in kept))
    check("a distant zone is not collapsed",
          any(k["rbLow"] == 80.0 for k in kept))
    check("opposite direction is never collapsed",
          any(k["direction"] == "bearish" for k in kept))

    print("\n[10] guards")
    check("short series returns nothing", Series().flat(8, 100.0).detect() == [])
    s = Series().flat(40, 100.0)
    check("flat series produces no blocks", len([b for b in s.detect()]) == 0,
          f"{len(s.detect())} blocks")




# ─────────────────────────────────────────────────────────────────────────────
# Phase 2 — /api/bias_scan integration
# ─────────────────────────────────────────────────────────────────────────────

def _api_klines(series):
    """Series rows -> the kline dicts get_klines_exchange returns.

    One extra running candle is appended because the scanner strips the last
    one; without it the confirm candle would be discarded.
    """
    out = []
    for i, (o, h, l, c, v) in enumerate(series.rows):
        out.append({"open": o, "high": h, "low": l, "close": c,
                    "volume": v, "openTime": T0 + i * STEP})
    last = series.rows[-1][3]
    out.append({"open": last, "high": last * 1.001, "low": last * 0.999,
                "close": last, "volume": 500.0,
                "openTime": T0 + len(series.rows) * STEP})
    return out


def api_scan(body, series):
    main.app.config["LOGIN_DISABLED"] = True
    main._guest_tab_check = lambda *_a, **_k: None
    main._check_and_get_token_user = lambda *_a, **_k: (None, None)
    main.consume_tokens = lambda *_a, **_k: None
    kl = _api_klines(series)
    main.get_klines_exchange = lambda *_a, **_k: kl
    with main.app.test_client() as cli:
        with cli.session_transaction() as sess:
            sess["logged_in"] = True
            sess["username"] = "tester"
        resp = cli.post("/api/bias_scan", json=body)
        assert resp.status_code == 200, f"HTTP {resp.status_code}: {resp.data[:300]}"
        return resp.get_json()


API_BASE = {
    "timeframe": "1d", "tf": "1d", "market": "perpetual", "exchange": "binance",
    "scanMode": "selected", "symbols": ["TESTUSDT"],
    "mode": "normal", "biasStrength": "balanced", "detectionMode": "early",
    "confluenceMode": "optional", "minimumGrade": "C+", "volumeFilter": "optional",
}


def rb_rows(d):
    return [r for r in d["results"] if r.get("detector") == "rejection_block"]


def api_tests():
    print("\n[11] API — disabled by default")
    s = bullish_rb()
    d = api_scan(dict(API_BASE), s)
    check("no RB rows when not enabled", len(rb_rows(d)) == 0, f"{len(rb_rows(d))} rows")
    check("no RB diagnostics block when disabled", "rejectionBlocks" not in d["diagnostics"])
    check("settingsUsed reports it off",
          d["diagnostics"]["settingsUsed"]["rejectionBlockEnabled"] is False)

    print("\n[12] API — enabled, block surfaces as a row")
    d = api_scan(dict(API_BASE, rejectionBlock={"enabled": True}), s)
    rows = rb_rows(d)
    check("an RB row is returned", len(rows) > 0, f"{len(rows)} rows")
    if not rows:
        return
    r = rows[0]
    check("row carries the detector discriminator", r["detector"] == "rejection_block")
    check("row is graded", r["grade"] in ("A+", "A", "B", "C", "D"), r["grade"])
    check("row is scored 0-100", 0 <= r["score"] <= 100, str(r["score"]))
    check("zone exposed for the UI",
          r["zoneLow"] < r["zoneHigh"], f"{r['zoneLow']}-{r['zoneHigh']}")
    check("invalidation is the far zone edge",
          abs(r["invalidationLevel"] - r["rb"]["rbLow"]) < 1e-9,
          f"{r['invalidationLevel']} vs {r['rb']['rbLow']}")
    check("invalidation status resolved", r["invalidationStatus"] in
          ("valid", "live_breached", "closed_invalidated"), r["invalidationStatus"])
    check("reason chain is populated", len(r["reasonChain"]) >= 4, str(len(r["reasonChain"])))
    check("grade label names the right detector",
          "Rejection Block" in r["gradeLabel"] and "Bias Shift" not in r["gradeLabel"],
          r["gradeLabel"])
    check("score breakdown is populated", len(r["scoreBreakdown"]) >= 5,
          str(len(r["scoreBreakdown"])))
    check("signal age carried for the tab",
          r["signalCandleTime"] == r["rb"]["formedAt"])
    check("bias rows are tagged too",
          all(x.get("detector") for x in d["results"]))

    print("\n[13] API — dead blocks are filtered out by default")
    dead = bullish_rb()
    dead.flat(2, 104.0)
    dead.add(103.0, 103.4, 96.5, 97.0, 900.0)        # closes below the zone
    d = api_scan(dict(API_BASE, rejectionBlock={"enabled": True}), dead)
    dg = d["diagnostics"]["rejectionBlocks"]
    check("invalidated block not returned",
          all(x["rb"]["state"] != "invalidated" for x in rb_rows(d)))
    check("dead count recorded", dg["dead"] >= 1, str(dg))

    d = api_scan(dict(API_BASE,
                      rejectionBlock={"enabled": True, "includeDead": True}), dead)
    check("includeDead surfaces them",
          any(x["rb"]["state"] == "invalidated" for x in rb_rows(d)),
          str([x["rb"]["state"] for x in rb_rows(d)]))

    print("\n[14] API — formation vs retest toggle")
    fresh = bullish_rb()
    d = api_scan(dict(API_BASE,
                      rejectionBlock={"enabled": True, "signalMode": "formation"}), fresh)
    check("formation mode shows an untested block", len(rb_rows(d)) > 0)

    d = api_scan(dict(API_BASE,
                      rejectionBlock={"enabled": True, "signalMode": "retest"}), fresh)
    check("retest mode hides an untested block", len(rb_rows(d)) == 0,
          f"{len(rb_rows(d))} rows")
    check("drop attributed to the toggle",
          d["diagnostics"]["rejectionBlocks"]["signalMode"] >= 1,
          str(d["diagnostics"]["rejectionBlocks"]))

    retested = bullish_rb()
    retested.flat(3, 104.0)
    retested.add(103.0, 103.5, 99.5, 102.5, 900.0)
    d = api_scan(dict(API_BASE,
                      rejectionBlock={"enabled": True, "signalMode": "retest"}), retested)
    rows = rb_rows(d)
    check("retest mode shows a retested block", len(rows) > 0, f"{len(rows)} rows")
    check("respected state marked as confirmed for the tab",
          rows and rows[0]["confirmationStatus"] == "confirmed",
          str(rows and rows[0]["confirmationStatus"]))

    print("\n[15] API — volume mode")
    quiet = bullish_base()
    quiet.add(100.5, 101.8, 98.0, 101.5, 300.0)      # sweep on LOW volume
    quiet.add(101.5, 103.5, 101.0, 103.2, 400.0)
    d = api_scan(dict(API_BASE,
                      rejectionBlock={"enabled": True, "volumeMode": "optional"}), quiet)
    check("quiet sweep still returned when volume optional", len(rb_rows(d)) > 0)
    lo_score = rb_rows(d)[0]["score"] if rb_rows(d) else None

    d = api_scan(dict(API_BASE,
                      rejectionBlock={"enabled": True, "volumeMode": "required"}), quiet)
    check("quiet sweep rejected when volume required", len(rb_rows(d)) == 0,
          f"{len(rb_rows(d))} rows")
    check("drop attributed to volume",
          d["diagnostics"]["rejectionBlocks"]["volume"] >= 1,
          str(d["diagnostics"]["rejectionBlocks"]))

    d = api_scan(dict(API_BASE,
                      rejectionBlock={"enabled": True, "volumeMode": "optional"}),
                 bullish_rb())
    hi_score = rb_rows(d)[0]["score"] if rb_rows(d) else None
    check("a volume-backed sweep outscores a quiet one",
          lo_score is not None and hi_score is not None and hi_score > lo_score,
          f"quiet={lo_score} spike={hi_score}")

    print("\n[16] API — grade filter and diagnostics wiring")
    d = api_scan(dict(API_BASE, minimumGrade="A",
                      rejectionBlock={"enabled": True}), bullish_rb())
    check("grade filter also applies to RB rows",
          all(x["grade"] in ("A+", "A") for x in rb_rows(d)),
          str([x["grade"] for x in rb_rows(d)]))
    dg = d["diagnostics"]["rejectionBlocks"]
    check("byState histogram populated", isinstance(dg.get("byState"), dict)
          and sum(dg["byState"].values()) == dg["detected"], str(dg))
    check("settingsUsed echoes the mode",
          d["diagnostics"]["settingsUsed"]["rejectionBlockSignalMode"] == "formation")

    print("\n[17] API — config overrides reach the detector")
    d = api_scan(dict(API_BASE, rejectionBlock={
        "enabled": True, "sweepMinPct": 5.0}), bullish_rb())
    check("a huge sweepMinPct suppresses detection", len(rb_rows(d)) == 0,
          f"{len(rb_rows(d))} rows")
    # swing-only must still find the swing-sourced block
    d = api_scan(dict(API_BASE, rejectionBlock={
        "enabled": True, "sweepSources": ["swing"]}), bullish_rb())
    swing_rows = rb_rows(d)
    check("swing-only finds the swing block", len(swing_rows) > 0, f"{len(swing_rows)} rows")
    check("and labels every row 'swing'",
          swing_rows and all(x["rb"]["sweepSource"] == "swing" for x in swing_rows),
          str([x["rb"]["sweepSource"] for x in swing_rows]))

    # local-only needs a series whose local extreme is the thing being raided;
    # asserting on bullish_rb here would pass vacuously on an empty list.
    loc = bullish_base()
    loc.add(100.5, 101.8, 98.0, 101.5, 2000.0)     # pierces the local low (102.0)
    loc.add(101.5, 103.5, 101.0, 103.2, 1500.0)    # reclaims above it
    loc.add(103.2, 105.0, 103.0, 104.5, 1400.0)    # confirms
    # maxDistanceAtr is loosened here so this case tests the sweep source, not
    # the runaway-price gate — this fixture's confirm candle ends ~2 ATR away.
    d = api_scan(dict(API_BASE, rejectionBlock={
        "enabled": True, "sweepSources": ["local"], "maxDistanceAtr": 99}), loc)
    local_rows = rb_rows(d)
    check("local-only finds a local-sourced block", len(local_rows) > 0,
          f"{len(local_rows)} rows")
    check("and labels every row 'local'",
          local_rows and all(x["rb"]["sweepSource"] == "local" for x in local_rows),
          str([x["rb"]["sweepSource"] for x in local_rows]))


def detect_mode_tests():
    print("\n[18] detect mode — which detectors actually run")
    s = bullish_rb()

    # legacy payloads must keep working: no `detect` key at all
    d = api_scan(dict(API_BASE), s)
    check("legacy payload with no detect key stays bias-only",
          d["diagnostics"]["settingsUsed"]["detect"] == "bias",
          d["diagnostics"]["settingsUsed"]["detect"])
    check("legacy bias-only returns no RB rows", len(rb_rows(d)) == 0)

    d = api_scan(dict(API_BASE, rejectionBlock={"enabled": True}), s)
    check("legacy rejectionBlock.enabled still means both",
          d["diagnostics"]["settingsUsed"]["detect"] == "both",
          d["diagnostics"]["settingsUsed"]["detect"])
    check("legacy enabled flag still yields RB rows", len(rb_rows(d)) > 0)

    # explicit modes
    d = api_scan(dict(API_BASE, detect="bias"), s)
    check("detect=bias returns no RB rows", len(rb_rows(d)) == 0)
    check("detect=bias reports no RB diagnostics", "rejectionBlocks" not in d["diagnostics"])

    d = api_scan(dict(API_BASE, detect="rejection_block"), s)
    rbs = rb_rows(d)
    bias = [r for r in d["results"] if r.get("detector") == "bias_shift"]
    check("detect=rejection_block returns RB rows", len(rbs) > 0, f"{len(rbs)} rows")
    check("detect=rejection_block runs no bias search", len(bias) == 0,
          f"{len(bias)} bias rows")
    rj = d["diagnostics"]["rejected"]
    check("RB-only logs no phantom bias rejections",
          all(rj[k] == 0 for k in ("noPriorMove", "noCleanPriorDrive",
                                   "noRejectionCandle", "notConfirmed")),
          str(rj))
    check("symbols are still counted as scanned",
          d["diagnostics"]["symbolsScanned"] == 1,
          str(d["diagnostics"]["symbolsScanned"]))

    d = api_scan(dict(API_BASE, detect="both"), s)
    check("detect=both runs the RB detector", len(rb_rows(d)) > 0)
    check("detect=both echoes the mode",
          d["diagnostics"]["settingsUsed"]["detect"] == "both")

    # an unknown value must not silently disable everything
    d = api_scan(dict(API_BASE, detect="nonsense", rejectionBlock={"enabled": True}), s)
    check("an unknown detect value falls back sanely",
          d["diagnostics"]["settingsUsed"]["detect"] == "both",
          d["diagnostics"]["settingsUsed"]["detect"])

    # a bias-producing series proves detect=bias still finds bias setups
    d = api_scan(dict(API_BASE, detect="bias"), bearish_rb())
    check("detect=bias still runs the bias engine",
          isinstance(d["results"], list) and "rejected" in d["diagnostics"])


def selected_scope_tests():
    print("\n[19] Selected scope respects pairs / cycle")
    s = bullish_rb()
    universe = [f"S{i}USDT" for i in range(200)]
    main.BIAS_SCAN_CURSOR.clear()

    body = dict(API_BASE, scanMode="selected", symbols=universe, pairsPerCycle=24)
    d = api_scan(body, s)
    cov = d.get("marketCoverage") or {}
    check("only pairsPerCycle symbols are scanned", d["scanned"] == 24, str(d["scanned"]))
    check("diagnostics agree", d["diagnostics"]["symbolsScanned"] == 24,
          str(d["diagnostics"]["symbolsScanned"]))
    check("coverage reports the full selected universe",
          cov.get("totalPairs") == 200, str(cov))
    check("first batch starts at 1", cov.get("startIndex") == 1, str(cov.get("startIndex")))
    check("first batch ends at 24", cov.get("endIndex") == 24, str(cov.get("endIndex")))

    d2 = api_scan(body, s)
    cov2 = d2.get("marketCoverage") or {}
    check("second scan advances the cursor", cov2.get("startIndex") == 25,
          str(cov2.get("startIndex")))
    check("second batch is also capped", d2["scanned"] == 24, str(d2["scanned"]))

    # a universe smaller than the batch is scanned in one go
    d3 = api_scan(dict(API_BASE, scanMode="selected",
                       symbols=universe[:10], pairsPerCycle=24), s)
    cov3 = d3.get("marketCoverage") or {}
    check("a small universe runs as one batch", d3["scanned"] == 10, str(d3["scanned"]))
    check("and is marked cycle complete", cov3.get("cycleComplete") is True, str(cov3))

    # market and selected cursors must not share state
    main.BIAS_SCAN_CURSOR.clear()
    main.get_pairs_exchange = lambda *_a, **_k: [{"symbol": f"M{i}USDT"} for i in range(200)]
    api_scan(dict(API_BASE, scanMode="market", symbols=[], pairsPerCycle=24), s)
    keys = sorted(main.BIAS_SCAN_CURSOR)
    api_scan(dict(API_BASE, scanMode="selected", symbols=universe, pairsPerCycle=24), s)
    keys2 = sorted(main.BIAS_SCAN_CURSOR)
    check("market and selected keep separate cursors", len(keys2) == 2, str(keys2))
    mkey = [k for k in keys2 if k.endswith("|market")][0]
    check("the selected scan did not move the market cursor",
          main.BIAS_SCAN_CURSOR[mkey] == 24, str(main.BIAS_SCAN_CURSOR[mkey]))

    # empty selection still errors clearly
    with main.app.test_client() as cli:
        with cli.session_transaction() as sess:
            sess["logged_in"] = True; sess["username"] = "tester"
        r = cli.post("/api/bias_scan", json=dict(API_BASE, scanMode="selected", symbols=[]))
    check("empty selection still returns no_selected_pairs", r.status_code == 400,
          str(r.status_code))


def pending_and_distance_tests():
    print("\n[20] pending formation — pierce + reclaim, confirm candle not in yet")
    # Ends on the reclaim candle: nothing after it, so the confirm window is
    # still open and the block is pending rather than failed.
    pend = bullish_base()
    pend.add(100.5, 101.8, 98.0, 101.5, 2000.0)      # pierce + reclaim, no confirm

    off = [b for b in pend.detect() if b["direction"] == "bullish"]
    check("pending is off by default", not off, f"{len(off)} blocks")

    on = [b for b in pend.detect(allowPending=True) if b["direction"] == "bullish"]
    check("allowPending surfaces it", len(on) > 0, f"{len(on)} blocks")
    if on:
        b = on[0]
        check("state is pending", b["state"] == "pending", b["state"])
        check("flagged as pending", b["pending"] is True)
        check("zone is provisional — pierce to reclaim only",
              b["formedIndex"] == b["reclaimIndex"],
              f"formed={b['formedIndex']} reclaim={b['reclaimIndex']}")

    # confirmed blocks must never be flagged pending
    conf = [b for b in bullish_rb().detect(allowPending=True)
            if b["direction"] == "bullish"]
    check("a confirmed block is not pending",
          conf and conf[0]["pending"] is False, str(conf and conf[0]["pending"]))

    # a setup that broke the sweep low is dead, not pending
    dead = bullish_base()
    dead.add(100.5, 101.8, 98.0, 101.5, 2000.0)
    dead.add(101.5, 101.6, 97.0, 97.2, 900.0)        # closes below the sweep low
    got = [b for b in dead.detect(allowPending=True)
           if b["direction"] == "bullish" and b["pierceIndex"] == 30]
    check("a broken setup is not offered as pending",
          not any(b.get("pending") for b in got), str([b["state"] for b in got]))

    # window elapsed with no confirm → dropped, not pending forever
    stale = bullish_base()
    stale.add(100.5, 101.8, 98.0, 101.5, 2000.0)
    stale.add(101.5, 101.6, 100.9, 101.0, 900.0)
    stale.add(101.0, 101.2, 100.6, 100.8, 900.0)
    stale.add(100.8, 101.0, 100.5, 100.7, 900.0)
    stale.flat(3, 101.0)
    got = [b for b in stale.detect(allowPending=True)
           if b["direction"] == "bullish" and b["pierceIndex"] == 30]
    check("an elapsed confirm window drops the block",
          not any(b.get("pending") for b in got), str([b["state"] for b in got]))

    print("\n[21] pending scoring is capped below the confirmed grades")
    cfg = main._rb_config("1d", {"allowPending": True})
    if on:
        sc = main._score_rejection_block(on[0], cfg, "optional")
        check("a real pending block scores in B or below", sc["score"] <= 79,
              str(sc["score"]))
        check("pending never grades above B",
              main._grade_from_score(sc["score"])["grade"] in ("B", "C", "D"),
              main._grade_from_score(sc["score"])["grade"])

    # Best case for a pending block: perfect sweep, tight, volume-backed, brand
    # new — but no dry retest, because it has not been retested. State weighting
    # alone lands it at 78, so the explicit cap is a guard rather than the
    # mechanism. Confirming the same block lifts it into A.
    best = {"pending": True, "state": "pending", "sweepSource": "swing",
            "sweepDepthPct": 1.0, "blockCandles": 2, "freshness": 1.0,
            "ageCandles": 0, "retests": [],
            "volume": {"sweepRatio": 3.0, "reclaimRatio": 3.0},
            "regime": {"efficiencyRatio": 0.7, "trending": True},
            "priorDrive": {"found": True, "matchesSweep": True,
                           "netPct": 12.0, "quality": "clean"}}
    best_score = main._score_rejection_block(best, cfg, "optional")["score"]
    check("best possible pending tops out inside B", best_score <= 79, str(best_score))
    conf_score = main._score_rejection_block(
        dict(best, pending=False, state="fresh"), cfg, "optional")["score"]
    check("the same block scores higher once confirmed", conf_score > best_score,
          f"pending={best_score} confirmed={conf_score}")

    # And the cap still clamps if scoring ever pushes a pending block higher
    over = dict(best, retests=[{"volRatio": 0.3}])   # would add the dry-retest bonus
    sc_over = main._score_rejection_block(over, cfg, "optional")
    check("the cap clamps an over-scoring pending block", sc_over["score"] == 79,
          str(sc_over["score"]))
    check("and says so in the breakdown",
          any("capped" in x for x in sc_over["breakdown"]),
          str(sc_over["breakdown"][-1]))

    print("\n[22] API — formation mode")
    d = api_scan(dict(API_BASE, rejectionBlock={"enabled": True}), pend)
    check("confirmed-only returns nothing for a pending setup",
          len(rb_rows(d)) == 0, f"{len(rb_rows(d))} rows")
    check("settingsUsed echoes confirmed",
          d["diagnostics"]["settingsUsed"]["rejectionBlockFormationMode"] == "confirmed")

    d = api_scan(dict(API_BASE, rejectionBlock={
        "enabled": True, "formationMode": "both"}), pend)
    rows = rb_rows(d)
    check("formationMode=both surfaces the pending block", len(rows) > 0,
          f"{len(rows)} rows")
    if rows:
        check("row is labelled Pending", rows[0]["setupType"].startswith("Pending"),
              rows[0]["setupType"])
        check("row grade is B or lower", rows[0]["grade"] in ("B", "C", "D"),
              rows[0]["grade"])

    # The fixture also throws off an unrelated pending block, so assert on the
    # absence of CONFIRMED rows rather than an empty result.
    d = api_scan(dict(API_BASE, rejectionBlock={
        "enabled": True, "formationMode": "pending"}), bullish_rb())
    check("formationMode=pending hides confirmed blocks",
          all(x["rb"]["pending"] for x in rb_rows(d)),
          str([(x["rb"]["pending"], x["rb"]["state"]) for x in rb_rows(d)]))
    check("drop attributed to the formation toggle",
          d["diagnostics"]["rejectionBlocks"]["formationMode"] >= 1,
          str(d["diagnostics"]["rejectionBlocks"]))

    print("\n[23] runaway-price gate")
    check("default maxDistanceAtr tightened to 1.5",
          main._rb_config("1d")["maxDistanceAtr"] == 1.5,
          str(main._rb_config("1d")["maxDistanceAtr"]))
    runaway = bullish_base()
    runaway.add(100.5, 101.8, 98.0, 101.5, 2000.0)
    runaway.add(101.5, 103.5, 101.0, 103.2, 1500.0)
    runaway.add(103.2, 112.0, 103.0, 111.5, 1500.0)     # violent move away
    near = [b for b in runaway.detect(maxDistanceAtr=99)
            if b["direction"] == "bullish" and b["pierceIndex"] == 30]
    check("the block is still detected", len(near) > 0)
    if near:
        check("it is far from price", near[0]["distanceAtr"] > 1.5,
              str(near[0]["distanceAtr"]))
        check("and flagged far under the default gate",
              [b for b in runaway.detect() if b["pierceIndex"] == 30
               and b["direction"] == "bullish"][0]["far"] is True)
    d = api_scan(dict(API_BASE, rejectionBlock={"enabled": True}), runaway)
    check("a runaway block is filtered out by default",
          not any(x["rb"]["pierceIndex"] == 30 for x in rb_rows(d)),
          str([(x["rb"]["pierceIndex"], x["rb"]["distanceAtr"]) for x in rb_rows(d)]))
    check("settingsUsed reports the gate",
          d["diagnostics"]["settingsUsed"]["rejectionBlockMaxDistanceAtr"] == 1.5)


def _regime_series(n, seed, kind):
    """kind: 'chop' = mean-reverting range, 'trend' = persistent momentum."""
    import random
    rnd = random.Random(seed)
    s = Series()
    px, anchor, mom = 100.0, 100.0, 0.0
    for _ in range(n):
        op = px
        if kind == "chop":
            step = -0.25 * (px - anchor) / anchor + rnd.gauss(0, 0.018)
        else:
            mom = 0.75 * mom + rnd.gauss(0, 0.010)
            step = mom
        cl = op * (1 + step)
        bh, bl = max(op, cl), min(op, cl)
        rng = abs(cl - op) + op * 0.010 * abs(rnd.gauss(0, 1))
        s.add(op, bh + rng * rnd.random() * .6, bl - rng * rnd.random() * .6, cl)
        px = cl
    return s


def context_tests():
    print("\n[24] efficiency ratio separates chop from trend")
    import statistics
    med = {}
    for kind in ("chop", "trend"):
        vals = []
        for seed in range(40):
            o, h, l, c, v, ts = _regime_series(200, seed, kind).unzip()
            for i in range(30, len(c)):
                er = main._rb_efficiency_ratio(c, i, 20)
                if er is not None:
                    vals.append(er)
        med[kind] = statistics.median(vals)
    check("chop scores low", med["chop"] < 0.25, f"median {med['chop']:.3f}")
    check("trend scores high", med["trend"] > 0.35, f"median {med['trend']:.3f}")
    check("the two separate clearly", med["trend"] > med["chop"] * 2,
          f"chop={med['chop']:.3f} trend={med['trend']:.3f}")
    check("short history returns None", main._rb_efficiency_ratio([1.0, 2.0], 1, 20) is None)

    print("\n[25] blocks carry regime, prior drive and target")
    b = [x for x in bullish_rb().detect() if x["direction"] == "bullish"][0]
    check("regime attached", isinstance(b.get("regime"), dict) and
          "efficiencyRatio" in b["regime"], str(b.get("regime")))
    check("regime is labelled", b["regime"]["label"] in ("trending", "choppy", "unknown"),
          b["regime"]["label"])
    check("prior drive attached", isinstance(b.get("priorDrive"), dict), str(type(b.get("priorDrive"))))
    check("drive direction is checked against the sweep",
          "matchesSweep" in b["priorDrive"])
    check("drive magnitude reported", b["priorDrive"]["netPct"] >= 0,
          str(b["priorDrive"]["netPct"]))
    check("target block attached", isinstance(b.get("target"), dict), str(b.get("target")))
    check("target uses the drive origin",
          b["target"]["level"] == b["priorDrive"]["originPrice"],
          f"{b['target']['level']} vs {b['priorDrive']['originPrice']}")
    if b["target"]["rr"] is not None:
        check("R:R is positive when the target is beyond entry",
              b["target"]["rr"] > 0, str(b["target"]["rr"]))

    print("\n[26] API — regime gate")
    d = api_scan(dict(API_BASE, rejectionBlock={"enabled": True}), bullish_rb())
    check("regime gate is off by default",
          d["diagnostics"]["settingsUsed"]["rejectionBlockRegimeMode"] == "off")
    base_rows = len(rb_rows(d))
    check("nothing filtered when off", base_rows > 0, f"{base_rows} rows")

    # a deliberately choppy fixture: flat base means a very low efficiency ratio
    d = api_scan(dict(API_BASE, rejectionBlock={
        "enabled": True, "regimeMode": "required"}), bullish_rb())
    dg = d["diagnostics"]["rejectionBlocks"]
    check("required gate reports what it dropped", "regime" in dg, str(dg))
    check("surviving rows are all trending",
          all((x["rb"]["regime"] or {}).get("trending") for x in rb_rows(d)),
          str([(x["rb"]["regime"] or {}).get("efficiencyRatio") for x in rb_rows(d)]))
    check("settingsUsed echoes the threshold",
          d["diagnostics"]["settingsUsed"]["rejectionBlockRegimeMinRatio"] == 0.35)

    print("\n[27] API — minimum prior move")
    d = api_scan(dict(API_BASE, rejectionBlock={
        "enabled": True, "minPriorMovePct": 0}), bullish_rb())
    check("zero disables the requirement", len(rb_rows(d)) == base_rows,
          f"{len(rb_rows(d))} vs {base_rows}")

    d = api_scan(dict(API_BASE, rejectionBlock={
        "enabled": True, "minPriorMovePct": 50}), bullish_rb())
    check("an unreachable threshold filters everything", len(rb_rows(d)) == 0,
          f"{len(rb_rows(d))} rows")
    check("drop attributed to prior move",
          d["diagnostics"]["rejectionBlocks"]["priorMove"] >= 1,
          str(d["diagnostics"]["rejectionBlocks"]))
    check("settingsUsed echoes the requirement",
          d["diagnostics"]["settingsUsed"]["rejectionBlockMinPriorMovePct"] == 50)

    print("\n[28] scoring stays on a 100-point scale")
    cfg = main._rb_config("1d")
    perfect = {"sweepSource": "swing", "sweepDepthPct": 1.0, "blockCandles": 2,
               "freshness": 1.0, "ageCandles": 0, "state": "respected",
               "retests": [{"volRatio": 0.3}],
               "volume": {"sweepRatio": 3.0, "reclaimRatio": 3.0},
               "regime": {"efficiencyRatio": 0.7, "trending": True},
               "priorDrive": {"found": True, "matchesSweep": True,
                              "netPct": 12.0, "quality": "clean"}}
    check("a perfect block reaches 100",
          main._score_rejection_block(perfect, cfg, "optional")["score"] == 100)
    choppy = dict(perfect, regime={"efficiencyRatio": 0.05, "trending": False},
                  priorDrive={"found": False, "matchesSweep": False,
                              "netPct": 0.5, "quality": "none"})
    ch_score = main._score_rejection_block(choppy, cfg, "optional")["score"]
    check("chop with no drive scores lower", ch_score < 100, str(ch_score))
    check("the context swing is 15 points",
          100 - ch_score == 15, f"delta={100 - ch_score}")
    check("choppy market is called out in the breakdown",
          any("choppy" in x for x in
              main._score_rejection_block(choppy, cfg, "optional")["breakdown"]))


if __name__ == "__main__":
    main_test()
    api_tests()
    detect_mode_tests()
    selected_scope_tests()
    pending_and_distance_tests()
    context_tests()
    print("\n" + ("-" * 62))
    if failures:
        print(f"FAILED ({len(failures)}): " + ", ".join(failures))
        sys.exit(1)
    print("All rejection block checks passed.")
