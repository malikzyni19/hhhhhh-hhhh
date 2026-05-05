"""
resolver_audit.py — Phase 6B.5: Diagnostic dry-run audit for outcome resolver.

Read-only. Never commits to DB. Replays candles with full decision tracing
and counterfactual comparison to diagnose why signals resolve as LOST/WON/EXPIRED.

Counterfactuals computed per signal:
  current        — wick stop, zone-edge entry price, current bounce threshold
  small_target   — same but smaller bounce (~60 % of current)
  close_stop     — close-based stop trigger instead of wick
  candle_close_entry — entry_price = candle close, not zone edge
"""

from datetime import datetime, timezone

# ── Alternative (smaller) bounce thresholds for counterfactual B ─────────────
_SMALL_BOUNCE: dict = {
    "1m": 0.002, "3m": 0.002, "5m": 0.002,
    "15m": 0.002, "30m": 0.003, "1h": 0.005,
    "2h": 0.006,  "4h": 0.010,  "6h": 0.012,
    "12h": 0.015, "1d": 0.020,
}


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _ts(unix_ms: int) -> datetime:
    return datetime.fromtimestamp(unix_ms / 1000, tz=timezone.utc)


def _ts_ms(dt) -> int:
    if dt is None:
        return 0
    if dt.tzinfo is None:
        return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)
    return int(dt.timestamp() * 1000)


def _fmt(dt) -> str:
    if dt is None:
        return "—"
    return dt.strftime("%Y-%m-%d %H:%M UTC")


def _fmt_ms(ms: int) -> str:
    return _fmt(_ts(ms)) if ms else "—"


# ─────────────────────────────────────────────────────────────────────────────
# Traced resolution loop — like _run_resolution_loop but emits trace strings
# ─────────────────────────────────────────────────────────────────────────────

def _run_traced(
    zone_high: float,
    zone_low: float,
    direction: str,
    bounce: float,
    detected_at_ms: int,
    candles: list,
    timeframe: str,
    stop_mode: str = "wick",          # "wick" | "close"
    entry_price_mode: str = "zone",   # "zone" | "candle_close"
) -> tuple:
    """
    Replay candles with full tracing.

    stop_mode="wick"  → current production rule (c_low/c_high triggers stop)
    stop_mode="close" → close-based stop (c_close triggers stop)

    entry_price_mode="zone"         → entry_price = zone_high/zone_low (current)
    entry_price_mode="candle_close" → entry_price = candle.close at entry candle

    Returns: (result_dict, trace_list, first_5_candles_after_detection)
    """
    from outcome_resolver import EXPIRY_CANDLES

    expiry_limit = EXPIRY_CANDLES.get(timeframe, 12)
    is_bullish   = (direction == "bullish")

    entry_price  = None
    entry_time   = None
    target_price = None
    stop_price   = None
    entry_candle_idx  = None
    entry_candle_ohlc = None

    trace = [
        f"Signal: {direction}  zone=[{zone_low:.6g}, {zone_high:.6g}]  "
        f"tf={timeframe}  bounce={bounce:.4%}",
        f"Detected: {_fmt_ms(detected_at_ms)}",
        f"Stop mode: {stop_mode}  |  Entry price: {entry_price_mode}",
    ]

    candles_post   = 0
    no_entry_shown = 0
    first_5        = []
    mfe = mae      = 0.0

    for candle in candles:
        open_time_ms = int(candle.get("openTime", 0))
        if open_time_ms <= detected_at_ms:
            continue

        candles_post += 1
        c_open  = float(candle.get("open",  0))
        c_high  = float(candle.get("high",  0))
        c_low   = float(candle.get("low",   0))
        c_close = float(candle.get("close", 0))
        candle_dt    = _ts(open_time_ms)
        candle_label = candle_dt.strftime("%m-%d %H:%M")

        if len(first_5) < 5:
            first_5.append({
                "time":  _fmt(candle_dt),
                "open":  c_open, "high": c_high,
                "low":   c_low,  "close": c_close,
            })

        # ── Entry detection ──────────────────────────────────────────────────
        if entry_price is None:
            entered = False
            if is_bullish and c_low <= zone_high:
                ep = zone_high if entry_price_mode == "zone" else c_close
                entry_price = ep
                entry_time  = candle_dt
                target_price = entry_price * (1 + bounce)
                stop_price   = zone_low
                entry_candle_idx  = candles_post
                entry_candle_ohlc = {"open": c_open, "high": c_high, "low": c_low, "close": c_close}
                trace.append(
                    f"[{candle_label}] ENTRY bullish: low={c_low:.6g} <= "
                    f"zone_high={zone_high:.6g} → "
                    f"entry_price={entry_price:.6g}  "
                    f"target={target_price:.6g}  stop={stop_price:.6g}"
                )
                entered = True
            elif not is_bullish and c_high >= zone_low:
                ep = zone_low if entry_price_mode == "zone" else c_close
                entry_price = ep
                entry_time  = candle_dt
                target_price = entry_price * (1 - bounce)
                stop_price   = zone_high
                entry_candle_idx  = candles_post
                entry_candle_ohlc = {"open": c_open, "high": c_high, "low": c_low, "close": c_close}
                trace.append(
                    f"[{candle_label}] ENTRY bearish: high={c_high:.6g} >= "
                    f"zone_low={zone_low:.6g} → "
                    f"entry_price={entry_price:.6g}  "
                    f"target={target_price:.6g}  stop={stop_price:.6g}"
                )
                entered = True

            if not entered:
                if no_entry_shown < 3:
                    trace.append(
                        f"[{candle_label}] no entry  "
                        f"H={c_high:.6g} L={c_low:.6g} C={c_close:.6g}"
                    )
                    no_entry_shown += 1
                elif no_entry_shown == 3:
                    trace.append("  ... (subsequent no-entry candles omitted) ...")
                    no_entry_shown += 1

                if candles_post >= expiry_limit:
                    trace.append(
                        f"EXPIRED: {candles_post} candles without entry (limit={expiry_limit})"
                    )
                    return (
                        {
                            "status": "EXPIRED",
                            "entry_price": None, "entry_time": None,
                            "target_price": None, "stop_price": None,
                            "exit_price": None, "exit_time": None,
                            "result": "EXPIRED",
                            "result_reason": "no_entry_within_expiry",
                            "same_candle_entry_stop": False,
                            "entry_candle_ohlc": None,
                            "mfe_pct": None, "mae_pct": None,
                            "candles_checked": candles_post,
                        },
                        trace, first_5,
                    )
                continue

        # ── Post-entry: MFE / MAE ────────────────────────────────────────────
        if is_bullish:
            mfe = max(mfe, (c_high - entry_price) / entry_price)
            mae = max(mae, (entry_price - c_low)  / entry_price)
        else:
            mfe = max(mfe, (entry_price - c_low)  / entry_price)
            mae = max(mae, (c_high - entry_price) / entry_price)

        # ── Target check (wick-based for both modes) ─────────────────────────
        if is_bullish:
            hit_target = c_high >= target_price
        else:
            hit_target = c_low <= target_price

        # ── Stop check ───────────────────────────────────────────────────────
        if stop_mode == "close":
            hit_stop = (c_close <= stop_price) if is_bullish else (c_close >= stop_price)
            stop_val = c_close
            stop_field = "close"
        else:
            hit_stop = (c_low <= stop_price) if is_bullish else (c_high >= stop_price)
            stop_val = c_low if is_bullish else c_high
            stop_field = "wick"

        same_entry_candle = (candles_post == entry_candle_idx)

        if hit_target and hit_stop:
            suffix = " [ON ENTRY CANDLE]" if same_entry_candle else ""
            trace.append(
                f"[{candle_label}] AMBIGUOUS: target AND stop both hit  "
                f"H={c_high:.6g} L={c_low:.6g} C={c_close:.6g}{suffix}"
            )
            return (
                {
                    "status": "AMBIGUOUS",
                    "entry_price": entry_price, "entry_time": entry_time,
                    "target_price": target_price, "stop_price": stop_price,
                    "exit_price": None, "exit_time": candle_dt,
                    "result": "AMBIGUOUS",
                    "result_reason": "target_and_stop_same_candle",
                    "same_candle_entry_stop": same_entry_candle,
                    "entry_candle_ohlc": entry_candle_ohlc,
                    "mfe_pct": round(mfe, 6), "mae_pct": round(mae, 6),
                    "candles_checked": candles_post,
                },
                trace, first_5,
            )

        if hit_target:
            target_val = c_high if is_bullish else c_low
            op         = ">=" if is_bullish else "<="
            trace.append(
                f"[{candle_label}] TARGET HIT: "
                f"{'high' if is_bullish else 'low'}={target_val:.6g} {op} "
                f"target={target_price:.6g}"
            )
            return (
                {
                    "status": "WON",
                    "entry_price": entry_price, "entry_time": entry_time,
                    "target_price": target_price, "stop_price": stop_price,
                    "exit_price": float(target_price), "exit_time": candle_dt,
                    "result": "WIN", "result_reason": "target_reached",
                    "same_candle_entry_stop": False,
                    "entry_candle_ohlc": entry_candle_ohlc,
                    "mfe_pct": round(mfe, 6), "mae_pct": round(mae, 6),
                    "candles_checked": candles_post,
                },
                trace, first_5,
            )

        if hit_stop:
            suffix = " [ON ENTRY CANDLE]" if same_entry_candle else ""
            # Show what close-stop would have decided if in wick mode
            close_note = ""
            if stop_mode == "wick":
                close_would_stop = (c_close <= stop_price) if is_bullish else (c_close >= stop_price)
                if not close_would_stop:
                    close_note = f"  (close={c_close:.6g} would NOT trigger close-stop)"
            trace.append(
                f"[{candle_label}] STOP HIT ({stop_field}): "
                f"{stop_field}={stop_val:.6g} vs stop={stop_price:.6g}"
                f"{close_note}{suffix}"
            )
            return (
                {
                    "status": "LOST",
                    "entry_price": entry_price, "entry_time": entry_time,
                    "target_price": target_price, "stop_price": stop_price,
                    "exit_price": float(stop_price), "exit_time": candle_dt,
                    "result": "LOSS", "result_reason": "stop_hit",
                    "same_candle_entry_stop": same_entry_candle,
                    "entry_candle_ohlc": entry_candle_ohlc,
                    "mfe_pct": round(mfe, 6), "mae_pct": round(mae, 6),
                    "candles_checked": candles_post,
                },
                trace, first_5,
            )

    # ── Candles exhausted ────────────────────────────────────────────────────
    if entry_price is not None:
        trace.append(
            f"Candles exhausted with entry but no exit  "
            f"(candles_post={candles_post}) → ENTERED"
        )
        return (
            {
                "status": "ENTERED",
                "entry_price": entry_price, "entry_time": entry_time,
                "target_price": target_price, "stop_price": stop_price,
                "exit_price": None, "exit_time": None,
                "result": None, "result_reason": None,
                "same_candle_entry_stop": False,
                "entry_candle_ohlc": entry_candle_ohlc,
                "mfe_pct": round(mfe, 6) if mfe else None,
                "mae_pct": round(mae, 6) if mae else None,
                "candles_checked": candles_post,
            },
            trace, first_5,
        )

    trace.append(
        f"No entry in {candles_post} candles checked → WAITING_FOR_ENTRY"
    )
    return (
        {
            "status": "WAITING_FOR_ENTRY",
            "entry_price": None, "entry_time": None,
            "target_price": None, "stop_price": None,
            "exit_price": None, "exit_time": None,
            "result": None, "result_reason": None,
            "same_candle_entry_stop": False,
            "entry_candle_ohlc": None,
            "mfe_pct": None, "mae_pct": None,
            "candles_checked": candles_post,
        },
        trace, first_5,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Public audit function — call inside active Flask app context
# ─────────────────────────────────────────────────────────────────────────────

_COMPACT_FIELDS = frozenset({
    "pair", "module", "timeframe", "direction",
    "final_result", "result_reason", "detected_at",
    "entry_time", "entry_price", "target_price", "stop_price",
    "exit_time", "exit_price",
    "target_hit_before_stop", "stop_hit_before_target", "same_candle_entry_stop",
    "entry_candle_ohlc",
    "raw_result", "quality_result", "quality_reason",
    "breaker_invalid_retest", "entry_candle_penetration_pct",
    "counterfactuals", "decision_trace",
})

# Maps ?result= value → set of audit statuses that match
_FILTER_STATUSES = {
    "lost":           {"LOST"},
    "won":            {"WON"},
    "expired":        {"EXPIRED"},
    "ambiguous":      {"AMBIGUOUS"},
    "no_entry":       {"WAITING_FOR_ENTRY"},
    "invalid_retest": {"LOST"},   # pre-filters to LOST, then quality guard reclassifies
}

# Main test modules — FVG is available as confluence data but excluded from
# default audit/stats so results reflect the primary setups being tested.
_MAIN_TEST_MODULES    = ["ob", "bb", "fib_confluence"]
_FVG_STANDALONE_MODULE = "fvg"


def audit_resolver_outcomes(
    limit: int = 50,
    module: str = None,
    timeframe: str = None,
    pair: str = None,
    result_filter: str = None,        # "lost"|"won"|"expired"|"ambiguous"|"no_entry"|"invalid_retest"|"all"|None
    compact: bool = False,
    include_fvg_standalone: bool = False,  # False = exclude standalone FVG from main stats
    breaker_quality_guard: bool = False,   # True = apply quality reclassification to bb LOST signals
) -> dict:
    """
    Dry-run diagnostic audit. Must be called inside an active Flask app context.
    Never commits to DB. Never mutates SignalEvent or SignalOutcome.

    Returns:
        {ok, mode, main_modules, fvg_standalone_excluded, excluded_fvg_count,
         filters, summary_all, summary_quality (when guard enabled), summary_filtered, signals}
    """
    try:
        from models import db, SignalEvent, SignalOutcome
        from main import get_klines_exchange
        from signal_logger import BOUNCE_THRESHOLDS
        from outcome_resolver import EXPIRY_CANDLES

        q = SignalEvent.query.filter_by(source="live")
        if module:
            q = q.filter_by(module=module)
        if timeframe:
            q = q.filter_by(timeframe=timeframe)
        if pair:
            q = q.filter_by(pair=pair.upper())

        cap = max(1, min(int(limit), 100))
        signals = q.order_by(SignalEvent.detected_at.desc()).limit(cap).all()

        summary_all = {
            "checked": 0,
            "won": 0, "lost": 0, "expired": 0,
            "ambiguous": 0, "waiting_for_entry": 0,
            "entered": 0, "errors": 0,
        }
        summary_quality = {
            "checked": 0,
            "won": 0, "lost": 0, "expired": 0,
            "ambiguous": 0, "waiting_for_entry": 0,
            "entered": 0, "invalid_retest": 0, "errors": 0,
        }

        # Normalise filter — "all" or None → no filtering
        rf = (result_filter or "").strip().lower()
        rf = rf if rf and rf != "all" else None
        # invalid_retest filter: pre-filter to LOST, then narrow post-guard
        filter_invalid_retest = (rf == "invalid_retest")
        target_statuses    = _FILTER_STATUSES.get(rf) if rf else None
        excluded_fvg_count = 0
        audit_list         = []

        def _iso(dt):
            return dt.isoformat() if dt else None

        for ev in signals:
            # Exclude standalone FVG from main test audit unless explicitly requested
            if not include_fvg_standalone and ev.module == _FVG_STANDALONE_MODULE:
                excluded_fvg_count += 1
                continue
            try:
                outcome = SignalOutcome.query.filter_by(signal_id=ev.signal_id).first()
                bounce  = (
                    outcome.bounce_threshold_pct
                    if outcome and outcome.bounce_threshold_pct
                    else BOUNCE_THRESHOLDS.get(ev.timeframe, 0.010)
                )
                small_b = _SMALL_BOUNCE.get(ev.timeframe, round(bounce * 0.6, 4))

                detected_ms = _ts_ms(ev.detected_at)
                fetch_limit = EXPIRY_CANDLES.get(ev.timeframe, 12) + 30
                exchange    = ev.exchange or "binance"

                try:
                    candles = get_klines_exchange(
                        ev.pair, ev.timeframe, fetch_limit, "perpetual", exchange
                    )
                except Exception:
                    candles = []

                if not candles:
                    summary_all["errors"] += 1
                    audit_list.append({
                        "pair": ev.pair, "module": ev.module,
                        "timeframe": ev.timeframe, "direction": ev.direction,
                        "final_result": "ERROR", "result_reason": "no_candle_data",
                    })
                    continue

                # ── Current rules ─────────────────────────────────────────
                res, trace, first5 = _run_traced(
                    ev.zone_high, ev.zone_low, ev.direction,
                    bounce, detected_ms, candles, ev.timeframe,
                    stop_mode="wick", entry_price_mode="zone",
                )

                # ── Counterfactuals (silent, no trace) ────────────────────
                res_small, _, _ = _run_traced(
                    ev.zone_high, ev.zone_low, ev.direction,
                    small_b, detected_ms, candles, ev.timeframe,
                    stop_mode="wick", entry_price_mode="zone",
                )
                res_close, _, _ = _run_traced(
                    ev.zone_high, ev.zone_low, ev.direction,
                    bounce, detected_ms, candles, ev.timeframe,
                    stop_mode="close", entry_price_mode="zone",
                )
                res_cce, _, _   = _run_traced(
                    ev.zone_high, ev.zone_low, ev.direction,
                    bounce, detected_ms, candles, ev.timeframe,
                    stop_mode="wick", entry_price_mode="candle_close",
                )

                cur = res["status"]

                # ── Tally summary_all (before filter) ─────────────────────
                summary_all["checked"] += 1
                if   cur == "WON":               summary_all["won"]               += 1
                elif cur == "LOST":              summary_all["lost"]              += 1
                elif cur == "EXPIRED":           summary_all["expired"]           += 1
                elif cur == "AMBIGUOUS":         summary_all["ambiguous"]         += 1
                elif cur == "WAITING_FOR_ENTRY": summary_all["waiting_for_entry"] += 1
                elif cur == "ENTERED":           summary_all["entered"]           += 1

                # ── Apply result filter (pre-quality) ─────────────────────
                if target_statuses and cur not in target_statuses:
                    continue

                entry = {
                    "signal_id":      ev.signal_id,
                    "pair":           ev.pair,
                    "module":         ev.module,
                    "timeframe":      ev.timeframe,
                    "direction":      ev.direction,
                    "detected_at":    _iso(ev.detected_at),
                    "detected_price": ev.detected_price,
                    "zone_high":      ev.zone_high,
                    "zone_low":       ev.zone_low,
                    "bounce_threshold_pct": bounce,
                    "db_status":      ev.status,
                    "entry_found":    res["entry_price"] is not None,
                    "entry_time":     _iso(res.get("entry_time")),
                    "entry_price":    res.get("entry_price"),
                    "target_price":   res.get("target_price"),
                    "stop_price":     res.get("stop_price"),
                    "final_result":   cur,
                    "result_reason":  res.get("result_reason"),
                    "exit_time":      _iso(res.get("exit_time")),
                    "exit_price":     res.get("exit_price"),
                    "same_candle_entry_stop": res.get("same_candle_entry_stop", False),
                    "entry_candle_ohlc":      res.get("entry_candle_ohlc"),
                    "target_hit_before_stop": cur == "WON",
                    "stop_hit_before_target": cur == "LOST",
                    "mfe_pct":         res.get("mfe_pct"),
                    "mae_pct":         res.get("mae_pct"),
                    "candles_checked": res.get("candles_checked", 0),
                    "first_5_relevant_candles": first5,
                    "decision_trace":  trace,
                    "counterfactuals": {
                        "current_wick_stop":  cur,
                        f"small_target_{int(round(small_b * 100, 1) * 10):03d}pct": res_small["status"],
                        "close_stop":         res_close["status"],
                        "candle_close_entry": res_cce["status"],
                    },
                }

                # ── Breaker Quality Guard ─────────────────────────────────
                if breaker_quality_guard:
                    from breaker_quality import apply_breaker_quality_guard
                    entry = apply_breaker_quality_guard(entry)

                    # Tally quality summary (parallel to summary_all)
                    summary_quality["checked"] += 1
                    qr = entry.get("quality_result", cur)
                    if   qr == "WON":               summary_quality["won"]               += 1
                    elif qr == "LOST":              summary_quality["lost"]              += 1
                    elif qr == "INVALID_RETEST":    summary_quality["invalid_retest"]    += 1
                    elif qr == "EXPIRED":           summary_quality["expired"]           += 1
                    elif qr == "AMBIGUOUS":         summary_quality["ambiguous"]         += 1
                    elif qr == "WAITING_FOR_ENTRY": summary_quality["waiting_for_entry"] += 1
                    elif qr == "ENTERED":           summary_quality["entered"]           += 1

                    # When filter is invalid_retest, drop non-INVALID_RETEST signals
                    if filter_invalid_retest and qr != "INVALID_RETEST":
                        continue

                if compact:
                    entry = {k: v for k, v in entry.items() if k in _COMPACT_FIELDS}

                audit_list.append(entry)

            except Exception as _sig_err:
                summary_all["errors"] += 1
                audit_list.append({
                    "pair":         getattr(ev, "pair",      "?"),
                    "final_result": "ERROR",
                    "result_reason": str(_sig_err),
                })

        out = {
            "ok":   True,
            "mode": "audit_dry_run",
            "main_modules":          _MAIN_TEST_MODULES,
            "fvg_standalone_excluded": not include_fvg_standalone,
            "excluded_fvg_count":    excluded_fvg_count,
            "breaker_quality_guard": breaker_quality_guard,
            "filters": {
                "module":                module,
                "timeframe":             timeframe,
                "pair":                  pair,
                "result":                rf or "all",
                "limit":                 cap,
                "compact":               compact,
                "include_fvg_standalone": include_fvg_standalone,
                "breaker_quality_guard": breaker_quality_guard,
            },
            "summary_all": summary_all,
            "summary_filtered": {
                "result":   rf or "all",
                "returned": len(audit_list),
            },
            "signals": audit_list,
        }
        if breaker_quality_guard:
            out["summary_quality"] = summary_quality
        return out

    except Exception as _outer:
        return {"ok": False, "error": str(_outer), "mode": "audit_dry_run"}
