"""
Zone Intelligence v1 — Signal Extractor
Converts a single api_scan result dict into a normalized signal dict
ready for logging via signal_logger.log_signal().

Supported setups (v1):
  OB_APPROACH   → module=ob,  zone from meta.obTop / meta.obBottom
  OB_CONSOL     → module=ob,  zone from meta.obTop / meta.obBottom
  FVG           → module=fvg, zone from meta.fvgTop / meta.fvgBottom
  BREAKER_APPROACH → module=bb, zone from meta.obTop / meta.obBottom (only if zone valid)
  BREAKER_INSIDE   → module=bb, zone from meta.obTop / meta.obBottom (only if zone valid)

Skipped in v1 (returns None):
  FIB_APPROACH, FIB_REACTION  — no natural zone; synthetic ±0.3% deferred to v2
  Any result with no topAlert or unrecognised setup
"""

import json


# Maps setup strings → (module, zone_high_key, zone_low_key)
_SETUP_MAP = {
    "OB_APPROACH":       ("ob",  "obTop",  "obBottom"),
    "OB_CONSOL":         ("ob",  "obTop",  "obBottom"),
    "FVG":               ("fvg", "fvgTop", "fvgBottom"),
    "BREAKER_APPROACH":  ("bb",  "obTop",  "obBottom"),
    "BREAKER_INSIDE":    ("bb",  "obTop",  "obBottom"),
}


def extract_scan_signal(result: dict, exchange: str, timeframe: str) -> dict | None:
    """
    Extract a normalized signal dict from a single api_scan result.

    Args:
        result:    One element from api_scan's "results" list.
        exchange:  Exchange string (e.g. "binance").
        timeframe: The scan timeframe (e.g. "1h") from scan settings.

    Returns:
        Normalized dict ready for log_signal(), or None if not loggable.
    """
    try:
        top_alert = result.get("topAlert") or {}
        if not top_alert:
            return None

        raw_setup = top_alert.get("setup", "")
        if not raw_setup:
            return None

        # Normalise: strip and upper so "ob_approach" and "OB_APPROACH" both match
        setup_key = raw_setup.strip().upper()

        mapping = _SETUP_MAP.get(setup_key)
        if mapping is None:
            # Unrecognised or skipped setup (FIB_APPROACH etc.)
            return None

        module, zh_key, zl_key = mapping
        meta = top_alert.get("meta") or {}

        zone_high = meta.get(zh_key)
        zone_low  = meta.get(zl_key)

        # Both zone boundaries must be present, numeric, and logically valid
        if zone_high is None or zone_low is None:
            return None
        zone_high = float(zone_high)
        zone_low  = float(zone_low)
        if zone_high <= zone_low or zone_low <= 0:
            return None

        direction      = top_alert.get("direction", "bullish")
        score          = int(result.get("score", 0))
        detected_price = float(result.get("price", 0))
        pair           = result.get("symbol", "")
        tf             = result.get("timeframe") or timeframe

        if not pair or detected_price <= 0:
            return None

        # Serialise meta for audit storage (safe — never raises)
        try:
            raw_meta_json = json.dumps(meta, default=str)
        except Exception:
            raw_meta_json = None

        return {
            "pair":           pair,
            "module":         module,
            "timeframe":      tf,
            "direction":      direction,
            "score":          score,
            "zone_high":      zone_high,
            "zone_low":       zone_low,
            "detected_price": detected_price,
            "exchange":       exchange,
            "setup_type":     setup_key,       # normalised e.g. "OB_APPROACH"
            "raw_setup":      raw_setup,        # original string from alert
            "raw_meta_json":  raw_meta_json,
        }

    except Exception:
        return None
