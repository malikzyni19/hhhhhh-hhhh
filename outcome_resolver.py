"""
outcome_resolver.py — Zone Intelligence v1
Resolves WAITING_FOR_ENTRY and ENTERED live signals by replaying
candles that arrived after detected_at.

Usage (manual only — no background thread, no route hooks):
    from outcome_resolver import resolve_pending_signals
    summary = resolve_pending_signals(app, limit=50)

Design decisions:
  - Pure function _run_resolution_loop for full testability without DB.
  - Entry: bullish when candle.low  <= zone_high → entry_price = zone_high
           bearish when candle.high >= zone_low  → entry_price = zone_low
  - target / stop set from entry_price and bounce_threshold_pct stored in
    SignalOutcome at signal creation time.
  - EXPIRED when candle count since detection exceeds EXPIRY_CANDLES[tf].
  - AMBIGUOUS when WON and LOST criteria both trigger on the same candle.
  - MFE/MAE measured as fraction from entry_price (positive = favourable).
  - Returns summary dict; never raises to caller.
"""

from datetime import datetime, timezone
from typing import Optional

# ── Expiry candle limits per timeframe ────────────────────────────────────────
EXPIRY_CANDLES: dict[str, int] = {
    "1m":  30,
    "3m":  20,
    "5m":  24,
    "15m": 16,
    "30m": 14,
    "1h":  12,
    "2h":  10,
    "4h":  8,
    "6h":  6,
    "12h": 5,
    "1d":  5,
}

# ── Timeframe → minutes (for candle-fetch limit calculation) ──────────────────
_TF_MINUTES: dict[str, int] = {
    "1m": 1, "3m": 3, "5m": 5, "15m": 15, "30m": 30,
    "1h": 60, "2h": 120, "4h": 240, "6h": 360, "12h": 720, "1d": 1440,
}


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _ts_ms(dt: datetime) -> int:
    """UTC datetime → Unix milliseconds."""
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000) if dt.tzinfo is None \
        else int(dt.timestamp() * 1000)


def _ts(unix_ms: int) -> datetime:
    """Unix milliseconds → UTC datetime."""
    return datetime.fromtimestamp(unix_ms / 1000, tz=timezone.utc)


def _hrs(start: datetime, end: datetime) -> float:
    """Hours between two datetimes (positive)."""
    delta = end - (start if start.tzinfo else start.replace(tzinfo=timezone.utc))
    return abs(delta.total_seconds()) / 3600


# ─────────────────────────────────────────────────────────────────────────────
# Pure resolution loop — no DB, fully testable
# ─────────────────────────────────────────────────────────────────────────────

def _run_resolution_loop(
    zone_high: float,
    zone_low: float,
    direction: str,
    bounce: float,
    detected_at_ms: int,
    candles: list,
    timeframe: str,
) -> dict:
    """
    Replay candles after detected_at_ms and return a resolution dict.

    Args:
        zone_high:      Upper zone boundary.
        zone_low:       Lower zone boundary.
        direction:      "bullish" or "bearish".
        bounce:         bounce_threshold_pct stored at signal creation.
        detected_at_ms: Unix-ms of signal detection; candles at or before
                        this timestamp are skipped.
        candles:        List of OHLCV dicts with keys openTime/open/high/low/close.
        timeframe:      Used to look up EXPIRY_CANDLES limit.

    Returns dict with keys:
        status          — "ENTERED", "WON", "LOST", "EXPIRED", "AMBIGUOUS",
                          or "WAITING_FOR_ENTRY" (no update needed yet)
        entry_price     — float or None
        entry_time      — datetime (UTC) or None
        target_price    — float or None
        stop_price      — float or None
        exit_price      — float or None
        exit_time       — datetime (UTC) or None
        result          — "WIN" / "LOSS" / "EXPIRED" / "AMBIGUOUS" / None
        result_reason   — str or None
        mfe_pct         — float or None (best favourable excursion from entry)
        mae_pct         — float or None (worst adverse excursion from entry)
        time_to_entry_hours  — float or None
        time_to_result_hours — float or None
    """
    expiry_limit = EXPIRY_CANDLES.get(timeframe, 12)
    is_bullish   = (direction == "bullish")

    entry_price:  Optional[float]    = None
    entry_time:   Optional[datetime] = None
    target_price: Optional[float]    = None
    stop_price:   Optional[float]    = None

    mfe = mae = 0.0
    candles_post = 0

    for candle in candles:
        open_time_ms = int(candle.get("openTime", 0))
        if open_time_ms <= detected_at_ms:
            continue

        candles_post += 1
        c_open  = float(candle.get("open",  0))
        c_high  = float(candle.get("high",  0))
        c_low   = float(candle.get("low",   0))
        c_close = float(candle.get("close", 0))
        candle_time = _ts(open_time_ms)

        # ── Entry detection ──────────────────────────────────────────────
        if entry_price is None:
            if is_bullish and c_low <= zone_high:
                entry_price  = zone_high
                entry_time   = candle_time
                target_price = entry_price * (1 + bounce)
                stop_price   = zone_low
            elif not is_bullish and c_high >= zone_low:
                entry_price  = zone_low
                entry_time   = candle_time
                target_price = entry_price * (1 - bounce)
                stop_price   = zone_high

            if entry_price is None:
                if candles_post >= expiry_limit:
                    return {
                        "status": "EXPIRED",
                        "entry_price": None, "entry_time": None,
                        "target_price": None, "stop_price": None,
                        "exit_price": None, "exit_time": None,
                        "result": "EXPIRED", "result_reason": "no_entry_within_expiry",
                        "mfe_pct": None, "mae_pct": None,
                        "time_to_entry_hours": None, "time_to_result_hours": None,
                    }
                continue

        # ── Post-entry: MFE / MAE tracking ──────────────────────────────
        if is_bullish:
            favourable = (c_high - entry_price) / entry_price
            adverse    = (entry_price - c_low)  / entry_price
        else:
            favourable = (entry_price - c_low)  / entry_price
            adverse    = (c_high - entry_price) / entry_price

        mfe = max(mfe, favourable)
        mae = max(mae, adverse)

        # ── Win / Loss check ─────────────────────────────────────────────
        if is_bullish:
            hit_target = c_high >= target_price
            hit_stop   = c_low  <= stop_price
        else:
            hit_target = c_low  <= target_price
            hit_stop   = c_high >= stop_price

        if hit_target and hit_stop:
            # Both on same candle — ambiguous
            detected_dt = _ts(detected_at_ms)
            return {
                "status": "AMBIGUOUS",
                "entry_price": entry_price, "entry_time": entry_time,
                "target_price": target_price, "stop_price": stop_price,
                "exit_price": None, "exit_time": candle_time,
                "result": "AMBIGUOUS", "result_reason": "target_and_stop_same_candle",
                "mfe_pct": round(mfe, 6), "mae_pct": round(mae, 6),
                "time_to_entry_hours":  round(_hrs(detected_dt, entry_time),  4),
                "time_to_result_hours": round(_hrs(entry_time,  candle_time),  4),
            }

        if hit_target:
            detected_dt = _ts(detected_at_ms)
            exit_price  = float(target_price)
            return {
                "status": "WON",
                "entry_price": entry_price, "entry_time": entry_time,
                "target_price": target_price, "stop_price": stop_price,
                "exit_price": exit_price, "exit_time": candle_time,
                "result": "WIN", "result_reason": "target_reached",
                "mfe_pct": round(mfe, 6), "mae_pct": round(mae, 6),
                "time_to_entry_hours":  round(_hrs(detected_dt, entry_time),  4),
                "time_to_result_hours": round(_hrs(entry_time,  candle_time),  4),
            }

        if hit_stop:
            detected_dt = _ts(detected_at_ms)
            exit_price  = float(stop_price)
            return {
                "status": "LOST",
                "entry_price": entry_price, "entry_time": entry_time,
                "target_price": target_price, "stop_price": stop_price,
                "exit_price": exit_price, "exit_time": candle_time,
                "result": "LOSS", "result_reason": "stop_hit",
                "mfe_pct": round(mfe, 6), "mae_pct": round(mae, 6),
                "time_to_entry_hours":  round(_hrs(detected_dt, entry_time),  4),
                "time_to_result_hours": round(_hrs(entry_time,  candle_time),  4),
            }

    # Candles exhausted without resolution
    if entry_price is not None:
        # Entered but no exit yet
        return {
            "status": "ENTERED",
            "entry_price": entry_price, "entry_time": entry_time,
            "target_price": target_price, "stop_price": stop_price,
            "exit_price": None, "exit_time": None,
            "result": None, "result_reason": None,
            "mfe_pct": round(mfe, 6) if mfe else None,
            "mae_pct": round(mae, 6) if mae else None,
            "time_to_entry_hours": round(_hrs(_ts(detected_at_ms), entry_time), 4),
            "time_to_result_hours": None,
        }

    # No entry detected yet
    return {
        "status": "WAITING_FOR_ENTRY",
        "entry_price": None, "entry_time": None,
        "target_price": None, "stop_price": None,
        "exit_price": None, "exit_time": None,
        "result": None, "result_reason": None,
        "mfe_pct": None, "mae_pct": None,
        "time_to_entry_hours": None, "time_to_result_hours": None,
    }


# ─────────────────────────────────────────────────────────────────────────────
# resolve_one — fetches candles and updates one signal row
# ─────────────────────────────────────────────────────────────────────────────

def _reason_label(new_status: str) -> str:
    return {
        "WAITING_FOR_ENTRY": "NO_ENTRY_YET",
        "ENTERED":           "ENTRY_TOUCHED",
        "WON":               "TARGET_HIT",
        "LOST":              "STOP_HIT",
        "EXPIRED":           "EXPIRED_NO_ENTRY",
        "AMBIGUOUS":         "AMBIGUOUS_CANDLE",
    }.get(new_status, "UNKNOWN")


def resolve_one(signal, db, dry_run: bool = False) -> dict:
    """
    Resolve a single SignalEvent ORM object.

    Fetches candles for signal.pair / signal.timeframe / signal.exchange,
    replays post-detected_at candles, and writes the result back to
    SignalEvent + SignalOutcome rows.

    Args:
        signal:   SignalEvent ORM instance (must be within an app context).
        db:       SQLAlchemy db instance.
        dry_run:  If True, compute resolution but do NOT commit to DB.

    Returns dict with keys:
        signal_id, pair, timeframe, old_status, new_status,
        result, reason, updated, [error]
    """
    try:
        from main import get_klines_exchange

        old_status = signal.status
        tf         = signal.timeframe
        exchange   = signal.exchange or "binance"
        pair       = signal.pair
        outcome    = signal.outcome

        def _base(new_status, result=None, updated=False, **extra):
            return {
                "signal_id":  signal.signal_id,
                "pair":       pair,
                "timeframe":  tf,
                "old_status": old_status,
                "new_status": new_status,
                "result":     result,
                "reason":     _reason_label(new_status),
                "updated":    updated,
                **extra,
            }

        if outcome is None:
            return _base("error", error="no outcome row")

        bounce = outcome.bounce_threshold_pct or 0.010

        # How many candles to fetch: expiry limit + 30 buffer
        fetch_limit = EXPIRY_CANDLES.get(tf, 12) + 30

        candles = get_klines_exchange(pair, tf, fetch_limit, "perpetual", exchange)
        if not candles:
            return _base("no_data", reason="NO_CANDLE_DATA")

        detected_at_ms = _ts_ms(signal.detected_at)

        res = _run_resolution_loop(
            zone_high      = signal.zone_high,
            zone_low       = signal.zone_low,
            direction      = signal.direction,
            bounce         = bounce,
            detected_at_ms = detected_at_ms,
            candles        = candles,
            timeframe      = tf,
        )

        new_status = res["status"]

        # No state change — skip write
        if new_status == old_status == "WAITING_FOR_ENTRY":
            return _base(new_status)

        if dry_run:
            return _base(new_status, result=res["result"], dry_run=True)

        # ── Write updates ────────────────────────────────────────────────
        signal.status = new_status

        outcome.entry_price          = res["entry_price"]
        outcome.entry_time           = res["entry_time"]
        outcome.target_price         = res["target_price"]
        outcome.stop_price           = res["stop_price"]
        outcome.exit_price           = res["exit_price"]
        outcome.exit_time            = res["exit_time"]
        outcome.result               = res["result"]
        outcome.result_reason        = res["result_reason"]
        outcome.mfe_pct              = res["mfe_pct"]
        outcome.mae_pct              = res["mae_pct"]
        outcome.time_to_entry_hours  = res["time_to_entry_hours"]
        outcome.time_to_result_hours = res["time_to_result_hours"]

        db.session.commit()

        return _base(new_status, result=res["result"], updated=True)

    except Exception as _err:
        try:
            db.session.rollback()
        except Exception:
            pass
        print(f"[OutcomeResolver] resolve_one error: {_err}")
        return {
            "signal_id":  getattr(signal, "signal_id", "?"),
            "pair":       getattr(signal, "pair", "?"),
            "timeframe":  getattr(signal, "timeframe", "?"),
            "old_status": getattr(signal, "status", "?"),
            "new_status": "error",
            "result":     None,
            "reason":     "ERROR",
            "updated":    False,
            "error":      str(_err),
        }


# ─────────────────────────────────────────────────────────────────────────────
# resolve_pending_signals — top-level entry point
# ─────────────────────────────────────────────────────────────────────────────

def resolve_pending_signals(app, limit: int = 50) -> dict:
    """
    Query WAITING_FOR_ENTRY and ENTERED signals and attempt resolution.

    Must be called with a Flask app instance; runs within app_context().
    Safe to call from any context (admin route, cron, CLI).

    Args:
        app:   Flask app instance.
        limit: Max signals to process per call (default 50).

    Returns summary dict:
        {
            "processed": int,
            "updated":   int,
            "won":       int,
            "lost":      int,
            "expired":   int,
            "ambiguous": int,
            "entered":   int,
            "no_change": int,
            "errors":    int,
        }
    """
    summary = {
        "processed": 0, "updated": 0,
        "won": 0, "lost": 0, "expired": 0, "ambiguous": 0,
        "entered": 0, "no_change": 0, "errors": 0,
    }
    try:
        from models import db, SignalEvent

        with app.app_context():
            signals = (
                SignalEvent.query
                .filter(SignalEvent.status.in_(["WAITING_FOR_ENTRY", "ENTERED"]))
                .order_by(SignalEvent.detected_at.asc())
                .limit(limit)
                .all()
            )

            for sig in signals:
                summary["processed"] += 1
                r = resolve_one(sig, db)

                if r.get("error"):
                    summary["errors"] += 1
                    continue

                status = r.get("status", "")
                if r.get("updated"):
                    summary["updated"] += 1
                    if status == "WON":
                        summary["won"] += 1
                    elif status == "LOST":
                        summary["lost"] += 1
                    elif status == "EXPIRED":
                        summary["expired"] += 1
                    elif status == "AMBIGUOUS":
                        summary["ambiguous"] += 1
                    elif status == "ENTERED":
                        summary["entered"] += 1
                else:
                    summary["no_change"] += 1

            print(f"[OutcomeResolver] {summary}")
            return summary

    except Exception as _outer_err:
        print(f"[OutcomeResolver] resolve_pending_signals error: {_outer_err}")
        summary["errors"] += 1
        return summary


# ─────────────────────────────────────────────────────────────────────────────
# resolve_pending_admin — admin endpoint helper (requires existing app context)
# ─────────────────────────────────────────────────────────────────────────────

def resolve_pending_admin(limit: int = 20, dry_run: bool = False) -> dict:
    """
    Resolve pending signals within an already-active Flask app context.

    Unlike resolve_pending_signals(), this does NOT wrap its own app_context()
    — it must be called from within a Flask route or existing context.
    Filters source='live' only. Returns full JSON-ready summary with per-signal
    details list.

    Args:
        limit:   Max signals to process (default 20, max enforced by caller).
        dry_run: If True, compute resolutions but do NOT commit DB changes.

    Returns:
        {
            "ok": bool,
            "checked": int,
            "won": int, "lost": int, "expired": int, "ambiguous": int,
            "entered": int, "no_resolution": int, "errors": int,
            "dry_run": bool,
            "details": [{"signal_id", "pair", "timeframe",
                          "old_status", "new_status", "result", "reason"}, ...]
        }
    """
    details = []
    summary: dict = {
        "ok":           True,
        "checked":      0,
        "won":          0,
        "lost":         0,
        "expired":      0,
        "ambiguous":    0,
        "entered":      0,
        "no_resolution":0,
        "errors":       0,
        "dry_run":      dry_run,
        "details":      details,
    }

    try:
        from models import db, SignalEvent

        signals = (
            SignalEvent.query
            .filter(SignalEvent.status.in_(["WAITING_FOR_ENTRY", "ENTERED"]))
            .filter(SignalEvent.source == "live")
            .order_by(SignalEvent.detected_at.asc())
            .limit(limit)
            .all()
        )

        for sig in signals:
            summary["checked"] += 1
            r = resolve_one(sig, db, dry_run=dry_run)

            details.append({
                "signal_id":  r.get("signal_id", "?"),
                "pair":       r.get("pair",       sig.pair),
                "timeframe":  r.get("timeframe",  sig.timeframe),
                "old_status": r.get("old_status", sig.status),
                "new_status": r.get("new_status", "error"),
                "result":     r.get("result"),
                "reason":     r.get("reason", "ERROR"),
            })

            if r.get("error") or r.get("new_status") in ("error", "no_data"):
                summary["errors"] += 1
                continue

            new_status = r.get("new_status", "")
            if new_status == "WON":
                summary["won"] += 1
            elif new_status == "LOST":
                summary["lost"] += 1
            elif new_status == "EXPIRED":
                summary["expired"] += 1
            elif new_status == "AMBIGUOUS":
                summary["ambiguous"] += 1
            elif new_status == "ENTERED":
                summary["entered"] += 1
            else:
                summary["no_resolution"] += 1

        print(f"[OutcomeResolver admin] dry_run={dry_run} {summary}")
        return summary

    except Exception as _err:
        print(f"[OutcomeResolver] resolve_pending_admin error: {_err}")
        summary["ok"]     = False
        summary["errors"] += 1
        summary["error"]  = str(_err)
        return summary


# ─────────────────────────────────────────────────────────────────────────────
# Tests — run directly: python3 outcome_resolver.py
# ─────────────────────────────────────────────────────────────────────────────

def _run_tests():
    """7 test cases covering all resolution paths via _run_resolution_loop."""
    from datetime import timedelta

    PASS = FAIL = 0

    def chk(label, condition, detail=""):
        nonlocal PASS, FAIL
        if condition:
            print(f"  ✓  {label}")
            PASS += 1
        else:
            print(f"  ✗  {label}" + (f" — {detail}" if detail else ""))
            FAIL += 1

    BASE_MS = _ts_ms(datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc))
    TF = "1h"
    STEP_MS = 60 * 60 * 1000  # 1 hour in ms

    def make_candle(idx, open_, high, low, close):
        return {
            "openTime": BASE_MS + idx * STEP_MS,
            "open": open_, "high": high, "low": low, "close": close, "volume": 1.0,
        }

    # Common zone: 100.0 / 95.0
    ZH, ZL, BOUNCE = 100.0, 95.0, 0.008

    print()
    print("=" * 60)
    print("outcome_resolver — _run_resolution_loop tests")
    print("=" * 60)

    # ── Test 1: Bullish WIN ──────────────────────────────────────────────────
    print("\n[1] Bullish WIN")
    candles = [
        make_candle(1, 102, 103, 99.5, 102),  # entry: low(99.5) <= zone_high(100)
        make_candle(2, 102, 108.5, 101, 108),  # target = 100 * 1.008 = 100.8 → high hits
    ]
    r = _run_resolution_loop(ZH, ZL, "bullish", BOUNCE, BASE_MS, candles, TF)
    chk("status=WON",              r["status"] == "WON",              str(r["status"]))
    chk("result=WIN",              r["result"] == "WIN",              str(r["result"]))
    chk("entry_price=100.0",       r["entry_price"] == 100.0,         str(r["entry_price"]))
    chk("target_price=100.8",      abs(r["target_price"] - 100.8) < 0.0001, str(r["target_price"]))
    chk("stop_price=95.0",         r["stop_price"] == 95.0,           str(r["stop_price"]))
    chk("exit_price=100.8",        abs(r["exit_price"] - 100.8) < 0.0001, str(r["exit_price"]))
    chk("mfe_pct set",             r["mfe_pct"] is not None)
    chk("time_to_entry_hours set", r["time_to_entry_hours"] is not None)

    # ── Test 2: Bullish LOSS ─────────────────────────────────────────────────
    # Entry candle must have high < target(100.8) so it doesn't resolve on entry.
    print("\n[2] Bullish LOSS")
    candles = [
        make_candle(1, 102, 100.7, 99.5, 100),  # entry: low(99.5)<=100, high(100.7)<target(100.8)
        make_candle(2, 100, 100.3, 94.0, 94),   # low(94.0) <= stop(95.0) → LOSS
    ]
    r = _run_resolution_loop(ZH, ZL, "bullish", BOUNCE, BASE_MS, candles, TF)
    chk("status=LOST",             r["status"] == "LOST",             str(r["status"]))
    chk("result=LOSS",             r["result"] == "LOSS",             str(r["result"]))
    chk("exit_price=95.0",         r["exit_price"] == 95.0,           str(r["exit_price"]))

    # ── Test 3: Bearish WIN ──────────────────────────────────────────────────
    print("\n[3] Bearish WIN")
    candles = [
        make_candle(1, 93, 95.5, 92, 93),    # entry: high(95.5) >= zone_low(95)
        make_candle(2, 93, 94, 86.2, 87),    # target = 95 * (1-0.008) = 94.24 → low hits
    ]
    r = _run_resolution_loop(ZH, ZL, "bearish", BOUNCE, BASE_MS, candles, TF)
    chk("status=WON",              r["status"] == "WON",              str(r["status"]))
    chk("result=WIN",              r["result"] == "WIN",              str(r["result"]))
    chk("entry_price=95.0",        r["entry_price"] == 95.0,          str(r["entry_price"]))
    chk("stop_price=100.0",        r["stop_price"] == 100.0,          str(r["stop_price"]))

    # ── Test 4: Bearish LOSS ─────────────────────────────────────────────────
    # Entry candle: high>=zone_low(95) but low must NOT be <= target(94.24).
    # target = 95 * (1 - 0.008) = 94.24
    print("\n[4] Bearish LOSS")
    candles = [
        make_candle(1, 93, 95.5, 94.5, 95),   # entry: high(95.5)>=95, low(94.5)>target(94.24)
        make_candle(2, 95, 101.0, 94.5, 100),  # high(101.0) >= stop(100.0) → LOSS
    ]
    r = _run_resolution_loop(ZH, ZL, "bearish", BOUNCE, BASE_MS, candles, TF)
    chk("status=LOST",             r["status"] == "LOST",             str(r["status"]))
    chk("result=LOSS",             r["result"] == "LOSS",             str(r["result"]))

    # ── Test 5: EXPIRED (no entry) ───────────────────────────────────────────
    print("\n[5] EXPIRED — no entry within expiry limit")
    expiry = EXPIRY_CANDLES[TF]  # 12 for 1h
    # All candles outside zone — never entry
    candles = [make_candle(i + 1, 105, 107, 103, 106) for i in range(expiry)]
    r = _run_resolution_loop(ZH, ZL, "bullish", BOUNCE, BASE_MS, candles, TF)
    chk("status=EXPIRED",          r["status"] == "EXPIRED",          str(r["status"]))
    chk("result=EXPIRED",          r["result"] == "EXPIRED",          str(r["result"]))
    chk("entry_price=None",        r["entry_price"] is None)

    # ── Test 6: AMBIGUOUS (target and stop both hit on same candle) ───────────
    print("\n[6] AMBIGUOUS — target and stop on same candle")
    candles = [
        make_candle(1, 102, 100.7, 99.5, 100),  # entry only: high(100.7)<target(100.8)
        make_candle(2, 100, 101.5, 94.0, 100),  # high(101.5)>=target AND low(94.0)<=stop
    ]
    r = _run_resolution_loop(ZH, ZL, "bullish", BOUNCE, BASE_MS, candles, TF)
    chk("status=AMBIGUOUS",        r["status"] == "AMBIGUOUS",        str(r["status"]))
    chk("result=AMBIGUOUS",        r["result"] == "AMBIGUOUS",        str(r["result"]))

    # ── Test 7: No resolution yet — candles exhausted, entered ───────────────
    print("\n[7] ENTERED — candles exhausted after entry, no exit")
    candles = [
        make_candle(1, 102, 100.7, 99.5, 100),  # entry only: high(100.7)<target(100.8)
        make_candle(2, 100, 100.5, 99.8, 100),  # high(100.5)<target, low(99.8)>stop → no exit
    ]
    r = _run_resolution_loop(ZH, ZL, "bullish", BOUNCE, BASE_MS, candles, TF)
    chk("status=ENTERED",          r["status"] == "ENTERED",          str(r["status"]))
    chk("result=None",             r["result"] is None)
    chk("entry_price set",         r["entry_price"] == 100.0)
    chk("mfe_pct set",             r["mfe_pct"] is not None)

    print()
    print("=" * 60)
    print(f"Results: {PASS} passed, {FAIL} failed")
    print("=" * 60)
    return FAIL == 0


if __name__ == "__main__":
    import sys
    ok = _run_tests()
    sys.exit(0 if ok else 1)
