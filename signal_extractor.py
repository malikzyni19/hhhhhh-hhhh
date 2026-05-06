"""
Zone Intelligence v2 — Signal Extractor
Converts api_scan result dicts into normalized signal dicts
ready for logging via signal_logger.log_signal().

Supported setups and modules:
  OB_APPROACH / OB_CONSOL  → module=ob,            zone from meta.obTop / meta.obBottom
  BREAKER_APPROACH / INSIDE → module=bb,            zone priority: breakerTop > obTop > top
  FIB_APPROACH / FIB_REACTION → module=fib_confluence (ONLY when fibPrice overlaps OB or FVG zone)
  FVG                       → NOT logged standalone; FVG zones used as confluence data only

Skipped always:
  Standalone FVG as main signal — FVG is confluence helper only
  FIB with no OB/FVG overlap  — logged as fib_confluence ONLY when fibPrice is inside a zone
  RSI and any unrecognised setup
"""

import json

# ── Setup classification sets ──────────────────────────────────────────────────
_OB_SETUPS      = frozenset({"OB_APPROACH", "OB_CONSOL"})
_BREAKER_SETUPS = frozenset({"BREAKER_APPROACH", "BREAKER_INSIDE"})
_FVG_SETUPS     = frozenset({"FVG"})
_FIB_SETUPS     = frozenset({"FIB_APPROACH", "FIB_REACTION"})

# OB setup → (module, zone_high_key, zone_low_key)
_OB_SETUP_MAP = {
    "OB_APPROACH": ("ob", "obTop", "obBottom"),
    "OB_CONSOL":   ("ob", "obTop", "obBottom"),
}

# Fib confluence: fibPrice must be within this % of a zone boundary to count
_FIB_OVERLAP_TOLERANCE = 0.0015   # 0.15 %


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _safe_float(val, default=None):
    """Convert val to float, return default on failure."""
    if val is None:
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _fib_in_zone(fib_price: float, zone_high: float, zone_low: float) -> bool:
    """True if fibPrice is inside the zone or within _FIB_OVERLAP_TOLERANCE."""
    extended_high = zone_high * (1.0 + _FIB_OVERLAP_TOLERANCE)
    extended_low  = zone_low  * (1.0 - _FIB_OVERLAP_TOLERANCE)
    return extended_low <= fib_price <= extended_high


def _extract_ob_alert(alert: dict, result: dict, exchange: str, timeframe: str,
                      allowed_modules) -> dict | None:
    """Extract a normalized OB signal from one OB_APPROACH / OB_CONSOL alert."""
    try:
        raw_setup = alert.get("setup", "")
        setup_key = raw_setup.strip().upper()
        if setup_key not in _OB_SETUP_MAP:
            return None

        module, zh_key, zl_key = _OB_SETUP_MAP[setup_key]
        if allowed_modules is not None and module not in allowed_modules:
            return None

        meta      = alert.get("meta") or {}
        zone_high = _safe_float(meta.get(zh_key))
        zone_low  = _safe_float(meta.get(zl_key))

        if zone_high is None or zone_low is None:
            return None
        if zone_high <= zone_low or zone_low <= 0:
            return None

        direction      = str(alert.get("direction") or "bullish")
        pair           = result.get("symbol", "")
        detected_price = _safe_float(result.get("price"), 0.0)
        tf             = result.get("timeframe") or timeframe

        if not pair or detected_price <= 0:
            return None

        # Build enriched meta — start from meta, then add top-level alert fields
        # that are NOT inside meta so they aren't silently dropped.
        raw_meta = dict(meta)

        # Preserve top-level alert["strength"] for debug purposes ONLY.
        # It is a coarse priority integer (3–5), NOT an OB strength percentage.
        # Stored as alert_strength_debug so it cannot be confused with ob_strength.
        _alert_strength_debug = alert.get("strength")
        if _alert_strength_debug is not None:
            raw_meta["alert_strength_debug"] = _alert_strength_debug

        # Preserve other useful top-level alert fields if present
        for _ak in ("score", "label"):
            _av = alert.get(_ak)
            if _av is not None:
                raw_meta.setdefault(f"alert_{_ak}", _av)

        # Normalized ob_strength: only true OB-specific strength keys allowed.
        # alert_strength_debug is intentionally excluded — it is not a percentage.
        _ob_str = (
            _safe_float(meta.get("obStrengthPct")) or
            _safe_float(meta.get("obStrength")) or
            _safe_float(meta.get("ob_strength_pct")) or
            _safe_float(meta.get("orderBlockStrength")) or
            _safe_float(meta.get("order_block_strength")) or
            _safe_float(meta.get("obVolumeStrength")) or
            _safe_float(meta.get("ob_volume_strength"))
        )
        if _ob_str is not None:
            raw_meta["ob_strength"] = round(_ob_str, 2)

        try:
            raw_meta_json = json.dumps(raw_meta, default=str)
        except Exception:
            raw_meta_json = None

        return {
            "pair":           pair,
            "module":         module,
            "timeframe":      tf,
            "direction":      direction,
            "score":          int(result.get("score", 0)),
            "zone_high":      zone_high,
            "zone_low":       zone_low,
            "detected_price": detected_price,
            "exchange":       exchange,
            "setup_type":     setup_key,
            "raw_setup":      raw_setup,
            "raw_meta_json":  raw_meta_json,
        }
    except Exception:
        return None


def _extract_breaker_alert(alert: dict, result: dict, exchange: str, timeframe: str,
                           allowed_modules) -> dict | None:
    """
    Extract a normalized Breaker signal from BREAKER_APPROACH / BREAKER_INSIDE.

    Zone priority:
      zone_high: breakerTop → obTop → top
      zone_low:  breakerBottom → obBottom → bottom
      direction: breakerDir → alert.direction → "bullish"
    """
    try:
        raw_setup = alert.get("setup", "")
        setup_key = raw_setup.strip().upper()
        if setup_key not in _BREAKER_SETUPS:
            return None

        if allowed_modules is not None and "bb" not in allowed_modules:
            return None

        meta = alert.get("meta") or {}

        # Priority zone extraction
        zone_high = (
            _safe_float(meta.get("breakerTop")) or
            _safe_float(meta.get("obTop")) or
            _safe_float(meta.get("top"))
        )
        zone_low = (
            _safe_float(meta.get("breakerBottom")) or
            _safe_float(meta.get("obBottom")) or
            _safe_float(meta.get("bottom"))
        )

        if zone_high is None or zone_low is None:
            return None
        if zone_high <= zone_low or zone_low <= 0:
            return None

        direction = str(
            meta.get("breakerDir") or
            alert.get("direction") or
            "bullish"
        )

        pair           = result.get("symbol", "")
        detected_price = _safe_float(result.get("price"), 0.0)
        tf             = result.get("timeframe") or timeframe

        if not pair or detected_price <= 0:
            return None

        try:
            raw_meta_json = json.dumps(meta, default=str)
        except Exception:
            raw_meta_json = None

        return {
            "pair":           pair,
            "module":         "bb",
            "timeframe":      tf,
            "direction":      direction,
            "score":          int(result.get("score", 0)),
            "zone_high":      zone_high,
            "zone_low":       zone_low,
            "detected_price": detected_price,
            "exchange":       exchange,
            "setup_type":     setup_key,
            "raw_setup":      raw_setup,
            "raw_meta_json":  raw_meta_json,
        }
    except Exception:
        return None


def _collect_confluence_zones(alerts: list) -> tuple:
    """
    Scan all alerts in a result and return (ob_zones, fvg_zones) for fib
    confluence detection. Called regardless of allowed_modules so that
    fib_confluence can see all available zones.

    ob_zones:  list of (zone_high, zone_low, meta)  — OB + Breaker zones
    fvg_zones: list of (zone_high, zone_low, meta)  — FVG zones only
    """
    ob_zones  = []
    fvg_zones = []

    for alert in alerts:
        setup_key = (alert.get("setup") or "").strip().upper()
        meta = alert.get("meta") or {}

        if setup_key in _OB_SETUPS:
            zh = _safe_float(meta.get("obTop"))
            zl = _safe_float(meta.get("obBottom"))
            if zh and zl and zh > zl > 0:
                ob_zones.append((zh, zl, meta))

        elif setup_key in _BREAKER_SETUPS:
            zh = (
                _safe_float(meta.get("breakerTop")) or
                _safe_float(meta.get("obTop")) or
                _safe_float(meta.get("top"))
            )
            zl = (
                _safe_float(meta.get("breakerBottom")) or
                _safe_float(meta.get("obBottom")) or
                _safe_float(meta.get("bottom"))
            )
            if zh and zl and zh > zl > 0:
                ob_zones.append((zh, zl, meta))   # breaker treated as OB-family

        elif setup_key in _FVG_SETUPS:
            zh = _safe_float(meta.get("fvgTop"))
            zl = _safe_float(meta.get("fvgBottom"))
            if zh and zl and zh > zl > 0:
                fvg_zones.append((zh, zl, meta))

    return ob_zones, fvg_zones


def _extract_fib_confluence_signals(
    alerts: list,
    result: dict,
    exchange: str,
    timeframe: str,
    ob_zones: list,
    fvg_zones: list,
) -> list:
    """
    For each FIB alert, check if fibPrice overlaps with an OB/Breaker or FVG zone.
    Returns list of fib_confluence signal dicts.

    Rules:
    - fib-only (no OB/FVG overlap) → skipped
    - fib inside OB zone            → module=fib_confluence, zone=OB zone
    - fib inside FVG zone           → module=fib_confluence, zone=FVG zone
    - fib inside OB + FVG           → one signal, zone=OB (OB preferred), secondary FVG stored
    - If fibPrice is missing/invalid → skipped
    """
    signals   = []
    seen_fibs: set = set()

    pair           = result.get("symbol", "")
    detected_price = _safe_float(result.get("price"), 0.0)
    tf             = result.get("timeframe") or timeframe

    if not pair or detected_price <= 0:
        return signals

    for alert in alerts:
        setup_key = (alert.get("setup") or "").strip().upper()
        if setup_key not in _FIB_SETUPS:
            continue

        meta = alert.get("meta") or {}
        fib_price = _safe_float(meta.get("fibPrice"))
        if fib_price is None or fib_price <= 0:
            continue

        fib_dedup = f"{fib_price:.6g}"
        if fib_dedup in seen_fibs:
            continue
        seen_fibs.add(fib_dedup)

        # Find first OB/Breaker zone that overlaps
        matched_ob = None
        for (zh, zl, ob_meta) in ob_zones:
            if _fib_in_zone(fib_price, zh, zl):
                matched_ob = (zh, zl, ob_meta)
                break

        # Find first FVG zone that overlaps
        matched_fvg = None
        for (zh, zl, fvg_meta) in fvg_zones:
            if _fib_in_zone(fib_price, zh, zl):
                matched_fvg = (zh, zl, fvg_meta)
                break

        # No overlap at all → skip
        if matched_ob is None and matched_fvg is None:
            continue

        # Determine confluence types and zone to use
        if matched_ob:
            zone_high              = matched_ob[0]
            zone_low               = matched_ob[1]
            confluence_types       = ["ob"]
            confluence_zone_source = "ob"
            secondary_fvg          = matched_fvg
        else:
            zone_high              = matched_fvg[0]
            zone_low               = matched_fvg[1]
            confluence_types       = ["fvg"]
            confluence_zone_source = "fvg"
            secondary_fvg          = None

        if matched_ob and matched_fvg:
            confluence_types = ["ob", "fvg"]

        # Build raw_meta_json preserving all original fib fields
        raw_meta = dict(meta)
        raw_meta["fibSetup"]               = setup_key
        raw_meta["confluence_types"]       = confluence_types
        raw_meta["confluence_zone_source"] = confluence_zone_source
        if secondary_fvg:
            raw_meta["secondary_fvg_zone"] = {
                "zone_high": secondary_fvg[0],
                "zone_low":  secondary_fvg[1],
            }

        try:
            raw_meta_json = json.dumps(raw_meta, default=str)
        except Exception:
            raw_meta_json = None

        signals.append({
            "pair":           pair,
            "module":         "fib_confluence",
            "timeframe":      tf,
            "direction":      str(alert.get("direction") or "bullish"),
            "score":          int(result.get("score", 0)),
            "zone_high":      zone_high,
            "zone_low":       zone_low,
            "detected_price": detected_price,
            "exchange":       exchange,
            "setup_type":     "FIB_CONFLUENCE",
            "raw_setup":      setup_key,
            "raw_meta_json":  raw_meta_json,
        })

    return signals


# ─────────────────────────────────────────────────────────────────────────────
# Internal per-alert extractor — OB/Breaker only (called by legacy wrapper)
# ─────────────────────────────────────────────────────────────────────────────

def _extract_one_alert(
    alert: dict,
    result: dict,
    exchange: str,
    timeframe: str,
    allowed_modules,
) -> dict | None:
    """
    Extract a normalized signal from one alert dict.

    Handles OB setups and Breaker setups.
    FVG and FIB are intentionally not handled here (FVG=standalone skipped,
    FIB=requires multi-alert confluence logic).
    """
    setup_key = (alert.get("setup") or "").strip().upper()

    if setup_key in _BREAKER_SETUPS:
        return _extract_breaker_alert(alert, result, exchange, timeframe, allowed_modules)
    elif setup_key in _OB_SETUPS:
        return _extract_ob_alert(alert, result, exchange, timeframe, allowed_modules)
    else:
        return None   # FVG standalone, FIB, RSI, unknown


# ─────────────────────────────────────────────────────────────────────────────
# Public: single-signal extractor (backward-compatible, topAlert only)
# ─────────────────────────────────────────────────────────────────────────────

def extract_scan_signal(result: dict, exchange: str, timeframe: str) -> dict | None:
    """
    Extract a normalized signal dict from a single api_scan result using
    only result["topAlert"].

    Kept for backward compatibility. Prefer extract_zone_signals_from_api_scan_result
    for new code (it respects allowed_modules and reads all alerts).
    """
    try:
        top_alert = result.get("topAlert") or {}
        if not top_alert:
            return None
        return _extract_one_alert(top_alert, result, exchange, timeframe, None)
    except Exception:
        return None


def extract_zone_signal_from_api_scan_result(result: dict, exchange: str = "binance") -> dict | None:
    """
    Convenience wrapper (backward-compatible).
    Reads topAlert only. Use extract_zone_signals_from_api_scan_result for
    module-filtered, multi-alert extraction.
    """
    return extract_scan_signal(
        result,
        exchange=exchange,
        timeframe=result.get("timeframe", "1h"),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Public: multi-signal extractor with allowed_modules filter
# ─────────────────────────────────────────────────────────────────────────────

def extract_zone_signals_from_api_scan_result(
    result: dict,
    exchange: str = "binance",
    allowed_modules=None,
) -> list:
    """
    Extract ALL loggable signals from result["alerts"], respecting allowed_modules.

    Two-pass approach:
      Pass 1 — collect OB/FVG zone data from ALL alerts (for fib confluence,
               ignoring allowed_modules so fib can see every zone).
      Pass 2 — extract OB and Breaker signals filtered by allowed_modules.
      Pass 3 — extract fib_confluence signals if fib_confluence is allowed.

    Standalone FVG is NEVER returned as a signal.

    Deduplicates by (module, direction, zone_high_6sf, zone_low_6sf) within
    one result — the same zone detected via multiple setups logs only once.

    Args:
        result:          One element from api_scan's "results" list.
        exchange:        Exchange string (e.g. "binance").
        allowed_modules: Set of module strings to include:
                         "ob", "bb", "fib_confluence"
                         None or empty = all main modules accepted.

    Returns:
        List of normalized signal dicts (may be empty, never raises).
    """
    try:
        alerts   = result.get("alerts") or []
        tf       = result.get("timeframe", "1h")
        _allowed = allowed_modules if allowed_modules else None

        signals = []
        seen: set = set()

        # ── Pass 1: collect zone data for fib confluence (always, regardless of filter) ──
        ob_zones, fvg_zones = _collect_confluence_zones(alerts)

        # ── Pass 2: OB and Breaker signals ────────────────────────────────────
        for alert in alerts:
            setup_key = (alert.get("setup") or "").strip().upper()
            # Skip FVG (standalone not logged) and FIB (handled in pass 3)
            if setup_key in _FVG_SETUPS or setup_key in _FIB_SETUPS:
                continue

            sig = _extract_one_alert(alert, result, exchange, tf, _allowed)
            if sig is None:
                continue

            dedup_key = (
                sig["module"],
                sig["direction"],
                f"{sig['zone_high']:.6g}",
                f"{sig['zone_low']:.6g}",
            )
            if dedup_key in seen:
                continue
            seen.add(dedup_key)
            signals.append(sig)

        # ── Pass 3: fib confluence signals ────────────────────────────────────
        if _allowed is None or "fib_confluence" in _allowed:
            fib_sigs = _extract_fib_confluence_signals(
                alerts, result, exchange, tf, ob_zones, fvg_zones
            )
            for sig in fib_sigs:
                dedup_key = (
                    sig["module"],
                    sig["direction"],
                    f"{sig['zone_high']:.6g}",
                    f"{sig['zone_low']:.6g}",
                )
                if dedup_key in seen:
                    continue
                seen.add(dedup_key)
                signals.append(sig)

        return signals

    except Exception:
        return []


# ─────────────────────────────────────────────────────────────────────────────
# Tests — run directly: python3 signal_extractor.py
# ─────────────────────────────────────────────────────────────────────────────

def _run_extractor_tests():
    PASS = FAIL = 0

    def chk(label, condition, detail=""):
        nonlocal PASS, FAIL
        if condition:
            print(f"  ✓  {label}")
            PASS += 1
        else:
            print(f"  ✗  {label}" + (f" — {detail}" if detail else ""))
            FAIL += 1

    # ── Fixture builders ─────────────────────────────────────────────────────
    def _result(symbol="BTCUSDT", price=100.0, score=70, timeframe="1h",
                alerts=None, topAlert=None):
        a = alerts or []
        return {
            "symbol":    symbol,
            "price":     price,
            "score":     score,
            "timeframe": timeframe,
            "alerts":    a,
            "topAlert":  topAlert or (a[0] if a else {}),
        }

    def _ob_alert(direction="bullish", top=110.0, bottom=105.0):
        return {"setup": "OB_APPROACH", "direction": direction, "strength": 6,
                "meta": {"obTop": top, "obBottom": bottom}}

    def _fvg_alert(direction="bullish", top=108.0, bottom=104.0):
        return {"setup": "FVG", "direction": direction, "strength": 5,
                "meta": {"fvgTop": top, "fvgBottom": bottom}}

    def _fib_alert(fib_price=106.0, level="0.618", direction="bullish"):
        return {"setup": "FIB_APPROACH", "direction": direction, "strength": 3,
                "meta": {"fibPrice": fib_price, "fibLevel": level}}

    def _bb_alert_native(direction="bullish", top=110.0, bottom=105.0):
        """Breaker with breakerTop/breakerBottom (scanner-native)."""
        return {"setup": "BREAKER_APPROACH", "direction": direction, "strength": 7,
                "meta": {"breakerTop": top, "breakerBottom": bottom,
                         "breakerDir": direction, "breakerStrength": 75.0}}

    def _bb_alert_fallback(direction="bullish", top=110.0, bottom=105.0):
        """Breaker with obTop/obBottom only (fallback path)."""
        return {"setup": "BREAKER_APPROACH", "direction": direction, "strength": 6,
                "meta": {"obTop": top, "obBottom": bottom}}

    def _bb_inside_alert(direction="bullish", top=110.0, bottom=105.0):
        return {"setup": "BREAKER_INSIDE", "direction": direction, "strength": 8,
                "meta": {"breakerTop": top, "breakerBottom": bottom,
                         "breakerDir": direction}}

    print()
    print("=" * 60)
    print("signal_extractor v2 — test suite")
    print("=" * 60)

    # ── 1. OB logs normally ──────────────────────────────────────────────────
    print("\n[1] OB logs normally")
    r = _result(alerts=[_ob_alert()])
    sigs = extract_zone_signals_from_api_scan_result(r)
    chk("one signal", len(sigs) == 1, str(len(sigs)))
    chk("module=ob", sigs[0]["module"] == "ob" if sigs else False)

    # ── 2. Breaker logs from breakerTop/breakerBottom ────────────────────────
    print("\n[2] BREAKER_APPROACH with breakerTop/breakerBottom → module=bb")
    r = _result(alerts=[_bb_alert_native()])
    sigs = extract_zone_signals_from_api_scan_result(r)
    chk("one signal", len(sigs) == 1, str(len(sigs)))
    chk("module=bb", sigs[0]["module"] == "bb" if sigs else False)
    chk("zone_high=110", sigs[0]["zone_high"] == 110.0 if sigs else False)
    chk("zone_low=105",  sigs[0]["zone_low"]  == 105.0 if sigs else False)

    # ── 3. BREAKER_INSIDE with breakerTop/breakerBottom ─────────────────────
    print("\n[3] BREAKER_INSIDE with breakerTop/breakerBottom → module=bb")
    r = _result(alerts=[_bb_inside_alert()])
    sigs = extract_zone_signals_from_api_scan_result(r)
    chk("one signal", len(sigs) == 1, str(len(sigs)))
    chk("module=bb", sigs[0]["module"] == "bb" if sigs else False)
    chk("setup_type=BREAKER_INSIDE", sigs[0]["setup_type"] == "BREAKER_INSIDE" if sigs else False)

    # ── 4. Breaker fallback: obTop/obBottom works ────────────────────────────
    print("\n[4] BREAKER_APPROACH with only obTop/obBottom → fallback still logs bb")
    r = _result(alerts=[_bb_alert_fallback()])
    sigs = extract_zone_signals_from_api_scan_result(r)
    chk("one signal", len(sigs) == 1, str(len(sigs)))
    chk("module=bb", sigs[0]["module"] == "bb" if sigs else False)

    # ── 5. Breaker direction from breakerDir ─────────────────────────────────
    print("\n[5] Breaker direction comes from meta.breakerDir")
    r = _result(alerts=[_bb_alert_native(direction="bearish")])
    sigs = extract_zone_signals_from_api_scan_result(r)
    chk("direction=bearish", sigs[0]["direction"] == "bearish" if sigs else False)

    # ── 6. Invalid breaker zone skips safely ────────────────────────────────
    print("\n[6] Invalid breaker zone skips safely")
    bad = [
        {"setup": "BREAKER_APPROACH", "direction": "bullish",
         "meta": {"breakerTop": 95.0, "breakerBottom": 110.0}},   # top < bottom
        {"setup": "BREAKER_APPROACH", "direction": "bullish",
         "meta": {"breakerTop": 0.0,  "breakerBottom": 0.0}},     # zero
        {"setup": "BREAKER_APPROACH", "direction": "bullish", "meta": {}}, # no keys
    ]
    r = _result(alerts=bad)
    sigs = extract_zone_signals_from_api_scan_result(r)
    chk("zero signals (all invalid)", len(sigs) == 0, str(len(sigs)))

    # ── 7. Standalone FVG skipped ────────────────────────────────────────────
    print("\n[7] Standalone FVG skipped as main signal")
    r = _result(alerts=[_fvg_alert()])
    sigs = extract_zone_signals_from_api_scan_result(r)
    chk("zero signals", len(sigs) == 0, str(len(sigs)))

    # ── 8. FVG still usable as confluence for fib ────────────────────────────
    print("\n[8] FVG used as confluence for fib — logs fib_confluence")
    # fibPrice=106 inside FVG zone 104-108
    r = _result(alerts=[_fvg_alert(top=108.0, bottom=104.0),
                        _fib_alert(fib_price=106.0)])
    sigs = extract_zone_signals_from_api_scan_result(r)
    chk("one signal", len(sigs) == 1, str(len(sigs)))
    chk("module=fib_confluence", sigs[0]["module"] == "fib_confluence" if sigs else False)
    if sigs:
        raw = json.loads(sigs[0]["raw_meta_json"] or "{}")
        chk("confluence_types=['fvg']", raw.get("confluence_types") == ["fvg"], str(raw.get("confluence_types")))

    # ── 9. Fib-only (no OB/FVG) skipped ────────────────────────────────────
    print("\n[9] Fib-only (no OB/FVG overlap) skipped")
    r = _result(alerts=[_fib_alert(fib_price=99.0)])  # price outside any zone
    sigs = extract_zone_signals_from_api_scan_result(r)
    chk("zero signals", len(sigs) == 0, str(len(sigs)))

    # ── 10. Fib inside OB zone → fib_confluence ─────────────────────────────
    print("\n[10] Fib inside OB zone → fib_confluence (zone=OB)")
    # fibPrice=107 inside OB zone 105-110
    r = _result(alerts=[_ob_alert(top=110.0, bottom=105.0),
                        _fib_alert(fib_price=107.0)])
    sigs = extract_zone_signals_from_api_scan_result(r)
    # Expect: 1 OB signal + 1 fib_confluence signal = 2 total
    ob_sigs  = [s for s in sigs if s["module"] == "ob"]
    fib_sigs = [s for s in sigs if s["module"] == "fib_confluence"]
    chk("OB signal logged", len(ob_sigs) == 1, str(len(ob_sigs)))
    chk("fib_confluence signal logged", len(fib_sigs) == 1, str(len(fib_sigs)))
    if fib_sigs:
        raw = json.loads(fib_sigs[0]["raw_meta_json"] or "{}")
        chk("confluence_zone_source=ob", raw.get("confluence_zone_source") == "ob",
            str(raw.get("confluence_zone_source")))

    # ── 11. Fib inside OB + FVG → one fib_confluence (not duplicate) ────────
    print("\n[11] Fib overlaps OB + FVG → one fib_confluence, zone=OB, secondary FVG stored")
    r = _result(alerts=[
        _ob_alert(top=110.0, bottom=105.0),
        _fvg_alert(top=109.0, bottom=104.0),   # overlapping with fib price
        _fib_alert(fib_price=107.0),
    ])
    sigs = extract_zone_signals_from_api_scan_result(r)
    fib_sigs = [s for s in sigs if s["module"] == "fib_confluence"]
    chk("exactly one fib_confluence", len(fib_sigs) == 1, str(len(fib_sigs)))
    if fib_sigs:
        raw = json.loads(fib_sigs[0]["raw_meta_json"] or "{}")
        chk("confluence_types=['ob','fvg']", set(raw.get("confluence_types", [])) == {"ob", "fvg"},
            str(raw.get("confluence_types")))
        chk("secondary_fvg_zone present", "secondary_fvg_zone" in raw, str(raw.keys()))
        chk("zone=OB (preferred)", fib_sigs[0]["zone_high"] == 110.0, str(fib_sigs[0]["zone_high"]))

    # ── 12. fibPrice outside OB zone (even with OB present) → skipped ────────
    print("\n[12] fibPrice outside OB zone → no fib_confluence")
    r = _result(alerts=[_ob_alert(top=110.0, bottom=105.0),
                        _fib_alert(fib_price=95.0)])   # far below zone
    sigs = extract_zone_signals_from_api_scan_result(r)
    fib_sigs = [s for s in sigs if s["module"] == "fib_confluence"]
    chk("zero fib_confluence", len(fib_sigs) == 0, str(len(fib_sigs)))

    # ── 13. Missing fibPrice skips safely ────────────────────────────────────
    print("\n[13] Missing fibPrice skips safely")
    bad_fib = {"setup": "FIB_APPROACH", "direction": "bullish", "meta": {}}
    r = _result(alerts=[_ob_alert(), bad_fib])
    sigs = extract_zone_signals_from_api_scan_result(r)
    fib_sigs = [s for s in sigs if s["module"] == "fib_confluence"]
    chk("zero fib_confluence", len(fib_sigs) == 0, str(len(fib_sigs)))

    # ── 14. allowed_modules={"bb"} → only bb, no OB or fib ─────────────────
    print("\n[14] allowed_modules={'bb'} → only bb logged")
    r = _result(alerts=[_ob_alert(), _bb_alert_native(top=120.0, bottom=115.0),
                        _fvg_alert(), _fib_alert(fib_price=107.0)])
    sigs = extract_zone_signals_from_api_scan_result(r, allowed_modules={"bb"})
    chk("one signal", len(sigs) == 1, str(len(sigs)))
    chk("module=bb", sigs[0]["module"] == "bb" if sigs else False)

    # ── 15. allowed_modules={"fib_confluence"} → fib confluences logged ─────
    print("\n[15] allowed_modules={'fib_confluence'} → only fib_confluence")
    r = _result(alerts=[_ob_alert(top=110.0, bottom=105.0),
                        _fib_alert(fib_price=107.0)])
    sigs = extract_zone_signals_from_api_scan_result(r, allowed_modules={"fib_confluence"})
    chk("one fib_confluence signal", len(sigs) == 1, str(len(sigs)))
    chk("module=fib_confluence", sigs[0]["module"] == "fib_confluence" if sigs else False)

    # ── 16. allowed_modules={"ob","bb","fib_confluence"} → all three ─────────
    print("\n[16] allowed_modules={'ob','bb','fib_confluence'} → OB + BB + fib_confluence")
    r = _result(alerts=[
        _ob_alert(top=110.0, bottom=105.0),
        _bb_alert_native(top=120.0, bottom=115.0),
        _fvg_alert(top=109.0, bottom=104.0),
        _fib_alert(fib_price=107.0),
    ])
    sigs = extract_zone_signals_from_api_scan_result(r, allowed_modules={"ob", "bb", "fib_confluence"})
    modules = {s["module"] for s in sigs}
    chk("ob present",            "ob" in modules, str(modules))
    chk("bb present",            "bb" in modules, str(modules))
    chk("fib_confluence present", "fib_confluence" in modules, str(modules))
    chk("no fvg standalone",     "fvg" not in modules, str(modules))

    # ── 17. allowed_modules=None → all main modules (no standalone fvg) ─────
    print("\n[17] allowed_modules=None → all main modules, standalone FVG not returned")
    r = _result(alerts=[_ob_alert(), _fvg_alert(), _bb_alert_native(top=120.0, bottom=115.0)])
    sigs = extract_zone_signals_from_api_scan_result(r, allowed_modules=None)
    modules = {s["module"] for s in sigs}
    chk("ob present",        "ob" in modules, str(modules))
    chk("bb present",        "bb" in modules, str(modules))
    chk("no fvg standalone", "fvg" not in modules, str(modules))

    # ── 18. Missing alerts[] → empty list, no crash ──────────────────────────
    print("\n[18] Missing alerts[] → empty, no crash")
    r = {"symbol": "BTCUSDT", "price": 100.0, "score": 50}
    sigs = extract_zone_signals_from_api_scan_result(r)
    chk("returns empty list", sigs == [], str(sigs))

    # ── 19. OB-only allowed_modules: logs OB, skips FVG ─────────────────────
    print("\n[19] OB-only allowed_modules logs OB, skips FVG and bb")
    r = _result(alerts=[_ob_alert(), _fvg_alert(), _bb_alert_native(top=120.0, bottom=115.0)])
    sigs = extract_zone_signals_from_api_scan_result(r, allowed_modules={"ob"})
    chk("one signal", len(sigs) == 1, str(len(sigs)))
    chk("module=ob", sigs[0]["module"] == "ob" if sigs else False)

    # ── 20. Dedup: same OB zone via OB_APPROACH and OB_CONSOL → one signal ───
    print("\n[20] Same zone via OB_APPROACH + OB_CONSOL deduplicates to one signal")
    a1 = {"setup": "OB_APPROACH", "direction": "bullish", "strength": 6,
          "meta": {"obTop": 110.0, "obBottom": 105.0}}
    a2 = {"setup": "OB_CONSOL",   "direction": "bullish", "strength": 5,
          "meta": {"obTop": 110.0, "obBottom": 105.0}}
    r = _result(alerts=[a1, a2])
    sigs = extract_zone_signals_from_api_scan_result(r)
    chk("one signal (deduped)", len(sigs) == 1, str(len(sigs)))
    chk("module=ob", sigs[0]["module"] == "ob" if sigs else False)

    print()
    print("=" * 60)
    print(f"Results: {PASS} passed, {FAIL} failed")
    print("=" * 60)
    return FAIL == 0


if __name__ == "__main__":
    import sys
    ok = _run_extractor_tests()
    sys.exit(0 if ok else 1)
