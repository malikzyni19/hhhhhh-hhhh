"""
breaker_quality.py — Phase 6C.2: Breaker Quality Guard

Read-only quality classification layer for resolver audit results.
Separates INVALID_RETEST (immediate entry-candle failures) from real LOST signals.

Rules (bb module only):
  1. bb LOST + same_candle_entry_stop=True → INVALID_RETEST / entry_candle_hit_stop
  2. bb LOST + bullish + entry_candle low <= zone_low → INVALID_RETEST / entry_candle_pierced_stop
  3. bb LOST + bearish + entry_candle high >= zone_high → INVALID_RETEST / entry_candle_pierced_stop
  4. All other modules or non-LOST results → pass through unchanged

Does NOT mutate DB. Does NOT modify SignalEvent or SignalOutcome.
"""


def _safe_float(v, default=None):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def apply_breaker_quality_guard(audit_signal: dict) -> dict:
    """
    Accept a signal dict from audit_resolver_outcomes() and return a new dict
    with quality fields added. Never raises; returns with quality_result=None on error.

    Added fields:
      raw_result                  — copy of final_result before quality adjustment
      quality_result              — INVALID_RETEST | same as final_result | None on error
      quality_reason              — entry_candle_hit_stop | entry_candle_pierced_stop | None
      breaker_invalid_retest      — True if classified INVALID_RETEST, else False
      entry_candle_penetration_pct — pct price moved beyond stop on entry candle (0 if none)
    """
    out = dict(audit_signal)
    out["raw_result"]                  = audit_signal.get("final_result")
    out["quality_result"]              = audit_signal.get("final_result")
    out["quality_reason"]              = None
    out["breaker_invalid_retest"]      = False
    out["entry_candle_penetration_pct"] = 0.0

    try:
        module       = str(audit_signal.get("module", ""))
        final_result = str(audit_signal.get("final_result", ""))
        direction    = str(audit_signal.get("direction", ""))

        # Only apply guard to bb LOST signals
        if module != "bb" or final_result != "LOST":
            return out

        zone_high = _safe_float(audit_signal.get("zone_high"))
        zone_low  = _safe_float(audit_signal.get("zone_low"))
        ohlc      = audit_signal.get("entry_candle_ohlc") or {}
        entry_low  = _safe_float(ohlc.get("low"))
        entry_high = _safe_float(ohlc.get("high"))
        same_candle = bool(audit_signal.get("same_candle_entry_stop", False))

        # Rule 1: same-candle stop flag
        if same_candle:
            out["quality_result"]         = "INVALID_RETEST"
            out["quality_reason"]         = "entry_candle_hit_stop"
            out["breaker_invalid_retest"] = True

            # Calculate penetration even for rule-1 hits
            if zone_high is not None and zone_low is not None:
                thickness = zone_high - zone_low
                if thickness > 0:
                    if direction == "bullish" and entry_low is not None:
                        out["entry_candle_penetration_pct"] = round(
                            max(0.0, (zone_low - entry_low) / thickness), 4
                        )
                    elif direction == "bearish" and entry_high is not None:
                        out["entry_candle_penetration_pct"] = round(
                            max(0.0, (entry_high - zone_high) / thickness), 4
                        )
            return out

        # Rule 2 & 3: entry candle pierced through stop (no OHLC → can't classify)
        if zone_high is None or zone_low is None:
            return out

        thickness = zone_high - zone_low
        if thickness <= 0:
            return out

        if direction == "bullish" and entry_low is not None:
            if entry_low <= zone_low:
                pen = max(0.0, (zone_low - entry_low) / thickness)
                out["quality_result"]                = "INVALID_RETEST"
                out["quality_reason"]                = "entry_candle_pierced_stop"
                out["breaker_invalid_retest"]        = True
                out["entry_candle_penetration_pct"]  = round(pen, 4)
                return out

        elif direction == "bearish" and entry_high is not None:
            if entry_high >= zone_high:
                pen = max(0.0, (entry_high - zone_high) / thickness)
                out["quality_result"]                = "INVALID_RETEST"
                out["quality_reason"]                = "entry_candle_pierced_stop"
                out["breaker_invalid_retest"]        = True
                out["entry_candle_penetration_pct"]  = round(pen, 4)
                return out

    except Exception:
        # Safety: on any error leave quality_result as final_result (already set above)
        pass

    return out


# ─────────────────────────────────────────────────────────────────────────────
# Unit tests (run with: python3 breaker_quality.py)
# ─────────────────────────────────────────────────────────────────────────────

def _run_tests():
    errors = []

    def check(name, got, expected):
        if got != expected:
            errors.append(f"FAIL [{name}]: got {got!r}, expected {expected!r}")

    base_bb_lost = {
        "module": "bb", "final_result": "LOST", "direction": "bullish",
        "zone_high": 100.0, "zone_low": 98.0,
        "same_candle_entry_stop": False,
        "entry_candle_ohlc": None,
    }

    # Test 1: bb LOST + same_candle_entry_stop=True → INVALID_RETEST
    sig = dict(base_bb_lost, same_candle_entry_stop=True)
    r = apply_breaker_quality_guard(sig)
    check("1.quality_result",         r["quality_result"],         "INVALID_RETEST")
    check("1.quality_reason",         r["quality_reason"],         "entry_candle_hit_stop")
    check("1.breaker_invalid_retest", r["breaker_invalid_retest"], True)
    check("1.raw_result",             r["raw_result"],             "LOST")

    # Test 2: bb LOST + clean (no ohlc, no same_candle) → stays LOST
    sig = dict(base_bb_lost)
    r = apply_breaker_quality_guard(sig)
    check("2.quality_result",         r["quality_result"],         "LOST")
    check("2.breaker_invalid_retest", r["breaker_invalid_retest"], False)

    # Test 3: bb LOST + bullish + entry candle low pierced below zone_low
    sig = dict(base_bb_lost,
               entry_candle_ohlc={"open": 99.0, "high": 100.5, "low": 97.0, "close": 97.5})
    r = apply_breaker_quality_guard(sig)
    check("3.quality_result",  r["quality_result"],  "INVALID_RETEST")
    check("3.quality_reason",  r["quality_reason"],  "entry_candle_pierced_stop")
    check("3.penetration",     r["entry_candle_penetration_pct"], round((98.0 - 97.0) / 2.0, 4))

    # Test 4: bb LOST + bearish + entry candle high pierced above zone_high
    sig = {
        "module": "bb", "final_result": "LOST", "direction": "bearish",
        "zone_high": 100.0, "zone_low": 98.0,
        "same_candle_entry_stop": False,
        "entry_candle_ohlc": {"open": 99.0, "high": 101.5, "low": 98.5, "close": 99.0},
    }
    r = apply_breaker_quality_guard(sig)
    check("4.quality_result",  r["quality_result"],  "INVALID_RETEST")
    check("4.quality_reason",  r["quality_reason"],  "entry_candle_pierced_stop")
    check("4.penetration",     r["entry_candle_penetration_pct"], round((101.5 - 100.0) / 2.0, 4))

    # Test 5: bb WON → pass through unchanged
    sig = {"module": "bb", "final_result": "WON", "direction": "bullish",
           "zone_high": 100.0, "zone_low": 98.0, "same_candle_entry_stop": False}
    r = apply_breaker_quality_guard(sig)
    check("5.quality_result",         r["quality_result"],         "WON")
    check("5.breaker_invalid_retest", r["breaker_invalid_retest"], False)

    # Test 6: bb AMBIGUOUS → pass through
    sig = {"module": "bb", "final_result": "AMBIGUOUS", "direction": "bullish",
           "zone_high": 100.0, "zone_low": 98.0, "same_candle_entry_stop": True}
    r = apply_breaker_quality_guard(sig)
    check("6.quality_result", r["quality_result"], "AMBIGUOUS")

    # Test 7: ob LOST + same_candle → stays LOST (non-bb modules not affected)
    sig = {"module": "ob", "final_result": "LOST", "direction": "bullish",
           "zone_high": 100.0, "zone_low": 98.0, "same_candle_entry_stop": True}
    r = apply_breaker_quality_guard(sig)
    check("7.quality_result",         r["quality_result"],         "LOST")
    check("7.breaker_invalid_retest", r["breaker_invalid_retest"], False)

    # Test 8: fib_confluence LOST → stays LOST
    sig = {"module": "fib_confluence", "final_result": "LOST", "direction": "bearish",
           "zone_high": 100.0, "zone_low": 98.0, "same_candle_entry_stop": True}
    r = apply_breaker_quality_guard(sig)
    check("8.quality_result", r["quality_result"], "LOST")

    # Test 9: bb WAITING_FOR_ENTRY → pass through
    sig = {"module": "bb", "final_result": "WAITING_FOR_ENTRY", "direction": "bullish",
           "zone_high": 100.0, "zone_low": 98.0, "same_candle_entry_stop": False}
    r = apply_breaker_quality_guard(sig)
    check("9.quality_result", r["quality_result"], "WAITING_FOR_ENTRY")

    # Test 10: bb LOST + missing ohlc → safe, stays LOST (can't classify pierce)
    sig = dict(base_bb_lost, entry_candle_ohlc=None, same_candle_entry_stop=False)
    r = apply_breaker_quality_guard(sig)
    check("10.quality_result",         r["quality_result"],         "LOST")
    check("10.breaker_invalid_retest", r["breaker_invalid_retest"], False)

    if errors:
        for e in errors:
            print(e)
        raise SystemExit(f"{len(errors)} test(s) failed")
    else:
        print(f"breaker_quality.py: all 10 tests passed")


if __name__ == "__main__":
    _run_tests()
