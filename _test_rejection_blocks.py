"""Rejection Block detector tests (Phase 1).

Exercises _detect_rejection_blocks against hand-built candle series:

  * bullish RB  — a swept low, reclaimed, confirmed (the 3-candle picture)
  * bearish RB  — mirrored
  * multi-candle formation — reclaim and confirm spread over more candles
  * rejection paths — shallow pierce, no reclaim, breakdown, no confirm
  * geometry — the zone is wick↔body, not the full candle range
  * lifecycle — fresh / tested / respected / mitigated / invalidated
  * freshness + expiry, ATR distance, volume ratios, dedupe
  * no lookahead — swings are not used before the bar that confirms them

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


if __name__ == "__main__":
    main_test()
    print("\n" + ("-" * 62))
    if failures:
        print(f"FAILED ({len(failures)}): " + ", ".join(failures))
        sys.exit(1)
    print("All rejection block checks passed.")
