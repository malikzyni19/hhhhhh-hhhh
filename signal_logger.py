"""
signal_logger.py — Zone Intelligence v1
Receives normalized signal dicts (from signal_extractor.py) and persists
them to PostgreSQL as SignalEvent + SignalOutcome rows.

Design decisions:
  - target_price and stop_price are NOT set here.
    outcome_resolver.py sets them after a real entry_price is confirmed.
  - bounce_threshold_pct IS stored in SignalOutcome so the resolver
    knows which threshold was in effect when the signal was detected.
  - Duplicate prevention: SHA256 of (pair|tf|module|direction|setup_type|
    rounded_zone_high|rounded_zone_low|candle_time_bucket).
    Same zone on the same candle always produces the same ID.
  - FIB module is not accepted (no zone boundaries, deferred to v2).
  - Every public function is wrapped in try/except — callers never raise.
"""

import hashlib
from datetime import datetime, timezone

STRATEGY_VERSION = "1.0"

# ── Bounce thresholds per timeframe ───────────────────────────────────────────
BOUNCE_THRESHOLDS: dict[str, float] = {
    "1m":  0.003,
    "3m":  0.003,
    "5m":  0.003,
    "15m": 0.003,
    "30m": 0.005,
    "1h":  0.008,
    "2h":  0.010,
    "4h":  0.020,
    "6h":  0.025,
    "12h": 0.030,
    "1d":  0.040,
}

VALID_MODULES    = {"ob", "fvg", "bb"}
VALID_DIRECTIONS = {"bullish", "bearish"}

# Minutes per timeframe (1d handled separately)
_TF_MINUTES: dict[str, int] = {
    "1m": 1, "3m": 3, "5m": 5, "15m": 15, "30m": 30,
    "1h": 60, "2h": 120, "4h": 240, "6h": 360, "12h": 720,
}


# ─────────────────────────────────────────────────────────────────────────────
# Public helpers
# ─────────────────────────────────────────────────────────────────────────────

def get_bounce_threshold(timeframe: str) -> float:
    """Return the bounce threshold fraction for the given timeframe."""
    return BOUNCE_THRESHOLDS.get(timeframe, 0.010)


def get_candle_time_bucket(timeframe: str, detected_at: datetime) -> str:
    """
    Floor detected_at to the candle boundary for the given timeframe.

    Returns a string used in signal_id generation so that multiple detections
    of the same zone within one candle produce a single deduplicated ID.

    Format:
      sub-hour  → "YYYYMMDDHHMM"  (HH and MM floored to TF boundary)
      hour-based → "YYYYMMDDHH"   (HH floored to TF boundary)
      1d        → "YYYYMMDD"
    """
    dt = detected_at

    if timeframe == "1d":
        return dt.strftime("%Y%m%d")

    tf_mins = _TF_MINUTES.get(timeframe, 60)

    if tf_mins < 60:
        total_mins  = dt.hour * 60 + dt.minute
        floored     = (total_mins // tf_mins) * tf_mins
        fh, fm      = floored // 60, floored % 60
        return dt.strftime(f"%Y%m%d") + f"{fh:02d}{fm:02d}"
    else:
        tf_hours    = tf_mins // 60
        floored_h   = (dt.hour // tf_hours) * tf_hours
        return dt.strftime(f"%Y%m%d") + f"{floored_h:02d}"


def _fmt_zone(x: float) -> str:
    """6 significant figures — eliminates float noise while handling all price magnitudes."""
    return f"{x:.6g}"


def generate_signal_id(signal: dict, candle_time_bucket: str) -> str:
    """
    Stable 40-char SHA256 from key signal fields.
    Same zone detected on the same candle always returns the same ID.

    Fields hashed (pipe-delimited):
      pair | timeframe | module | direction | setup_type |
      zone_high (6 sig-fig) | zone_low (6 sig-fig) | candle_time_bucket
    """
    components = "|".join([
        str(signal.get("pair", "")),
        str(signal.get("timeframe", "")),
        str(signal.get("module", "")),
        str(signal.get("direction", "")),
        str(signal.get("setup_type", "")),
        _fmt_zone(float(signal["zone_high"])),
        _fmt_zone(float(signal["zone_low"])),
        candle_time_bucket,
    ])
    return hashlib.sha256(components.encode()).hexdigest()[:40]


# ─────────────────────────────────────────────────────────────────────────────
# Validation (internal — returns structured result, never raises)
# ─────────────────────────────────────────────────────────────────────────────

def _validate(signal: dict) -> dict:
    """Return {"ok": True} or {"ok": False, "reason": "..."}"""

    if not signal.get("pair"):
        return {"ok": False, "reason": "pair required"}

    module = str(signal.get("module", ""))
    if module not in VALID_MODULES:
        return {"ok": False, "reason": f"module '{module}' not supported (must be ob/fvg/bb)"}

    if not signal.get("timeframe"):
        return {"ok": False, "reason": "timeframe required"}

    direction = str(signal.get("direction", ""))
    if direction not in VALID_DIRECTIONS:
        return {"ok": False, "reason": f"direction '{direction}' invalid (must be bullish/bearish)"}

    try:
        zh = float(signal["zone_high"])
        zl = float(signal["zone_low"])
    except (KeyError, TypeError, ValueError):
        return {"ok": False, "reason": "zone_high and zone_low must be numeric"}

    if zl <= 0 or zh <= zl:
        return {"ok": False, "reason": f"invalid zone: zone_high={zh} zone_low={zl}"}

    try:
        price = float(signal["detected_price"])
    except (KeyError, TypeError, ValueError):
        return {"ok": False, "reason": "detected_price must be numeric"}

    if price <= 0:
        return {"ok": False, "reason": "detected_price must be > 0"}

    try:
        score = int(float(signal.get("score", 0)))
    except (TypeError, ValueError):
        return {"ok": False, "reason": "score must be numeric"}

    if not (0 <= score <= 100):
        return {"ok": False, "reason": f"score {score} out of range 0–100"}

    return {"ok": True}


# ─────────────────────────────────────────────────────────────────────────────
# Main public function
# ─────────────────────────────────────────────────────────────────────────────

def log_normalized_signal(signal: dict, source: str = "live") -> dict:
    """
    Validate, deduplicate, and persist a normalized signal to PostgreSQL.

    Expects the dict format produced by signal_extractor.extract_scan_signal().
    Must be called within a Flask application context (db session available).

    Returns one of:
      {"logged": True,  "signal_id": "<40-char-hex>"}
      {"logged": False, "reason": "duplicate", "signal_id": "<40-char-hex>"}
      {"logged": False, "reason": "<validation failure>"}
      {"logged": False, "error":  "<exception message>"}

    Note:
      target_price and stop_price are NOT set here.
      They will be computed by outcome_resolver.py once entry is confirmed.
    """
    try:
        # ── 1. Validate ──────────────────────────────────────────────────────
        v = _validate(signal)
        if not v["ok"]:
            return {"logged": False, "reason": v["reason"]}

        # Coerce score — accepts int, float, or numeric string
        score = int(float(signal.get("score", 0)))

        # ── 2. Deduplication fields ──────────────────────────────────────────
        detected_at = datetime.now(timezone.utc)
        bounce      = get_bounce_threshold(signal["timeframe"])
        bucket      = get_candle_time_bucket(signal["timeframe"], detected_at)
        signal_id   = generate_signal_id(signal, bucket)

        # ── 3. DB operations (requires Flask app context) ────────────────────
        from models import db, SignalEvent, SignalOutcome

        existing = SignalEvent.query.filter_by(signal_id=signal_id).first()
        if existing:
            return {"logged": False, "reason": "duplicate", "signal_id": signal_id}

        event = SignalEvent(
            signal_id      = signal_id,
            pair           = str(signal["pair"]),
            module         = str(signal["module"]),
            timeframe      = str(signal["timeframe"]),
            direction      = str(signal["direction"]),
            score          = score,
            zone_high      = float(signal["zone_high"]),
            zone_low       = float(signal["zone_low"]),
            detected_price = float(signal["detected_price"]),
            detected_at    = detected_at,
            exchange       = str(signal.get("exchange", "binance")),
            strategy_ver   = STRATEGY_VERSION,
            settings_json  = None,
            status         = "WAITING_FOR_ENTRY",
            source         = source,
            setup_type     = signal.get("setup_type"),
            raw_setup      = signal.get("raw_setup"),
            raw_meta_json  = signal.get("raw_meta_json"),
        )
        db.session.add(event)
        db.session.flush()  # flush to validate FK before outcome insert

        # target_price and stop_price intentionally left None —
        # outcome_resolver sets them after real entry_price is known.
        outcome = SignalOutcome(
            signal_id            = signal_id,
            entry_price          = None,
            entry_time           = None,
            target_price         = None,
            stop_price           = None,
            exit_price           = None,
            exit_time            = None,
            result               = None,
            result_reason        = None,
            mfe_pct              = None,
            mae_pct              = None,
            time_to_entry_hours  = None,
            time_to_result_hours = None,
            bounce_threshold_pct = bounce,
        )
        db.session.add(outcome)
        db.session.commit()

        print(f"[SignalLogger] {signal_id[:12]}… | {signal['pair']} {signal['module'].upper()} "
              f"{signal['timeframe']} {signal['direction']} score={score}")
        return {"logged": True, "signal_id": signal_id}

    except Exception as _log_err:
        try:
            from models import db
            db.session.rollback()
        except Exception:
            pass
        print(f"[SignalLogger] error: {_log_err}")
        return {"logged": False, "error": str(_log_err)}
