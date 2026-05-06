"""
backtest_ob.py — Phase 7B/7C/7D/7E: OB Backtest with Loss Diagnostics + Freshness + Strength

Phase 7A base: read-only OB-only candle-replay backtest.
Phase 7B adds: per-signal loss_reason_detail, 3 counterfactual variants,
and a diagnostics summary + per-breakdown diagnostic counts.
Phase 7C adds: stop_mode (wick/close) selector.
Phase 7D adds: first-touch freshness filter using pre-signal candles.
Phase 7E adds: OB strength extraction helper used in backtest rows.

Rules:
  - Never mutates SignalEvent or SignalOutcome.
  - Never writes to DB.
  - module="ob" ONLY, setup_type in OB_APPROACH / OB_CONSOL.
  - Must be called inside an active Flask app context.
"""

import json
from datetime import timezone

_OB_MODULE      = "ob"
_OB_SETUP_TYPES = frozenset({"OB_APPROACH", "OB_CONSOL"})

_RESULT_FILTER_MAP = {
    "won":       {"WON"},
    "lost":      {"LOST"},
    "expired":   {"EXPIRED"},
    "ambiguous": {"AMBIGUOUS"},
    "waiting":   {"WAITING_FOR_ENTRY"},
    "entered":   {"ENTERED"},
}

# All possible loss_reason_detail values
_DIAG_KEYS = (
    "same_candle_stop",
    "wick_stop_only",
    "close_stop_also_lost",
    "stop_after_clean_entry",
    "target_and_stop_same_candle",
    "no_entry_expired",
    "entered_then_expired",
    "entered_open",
    "waiting_for_entry",
    "won_clean",
    "unknown",
)

# Valid freshness filter values
_FRESHNESS_VALUES = frozenset({"all", "first_touch", "already_touched", "unknown"})

# Ordered candidate keys for OB strength — checked left to right, first match wins
_OB_STRENGTH_KEYS = (
    "ob_strength",        # normalized key written by updated signal_extractor
    "obStrengthPct",      # already in meta for most logged signals
    "obStrength",
    "ob_strength_pct",
    "strengthPct",
    "strength",
    "alert_strength",     # top-level alert["strength"] preserved by updated extractor
    "volumeStrength",
    "volume_strength",
    "obVolumeStrength",
    "ob_volume_strength",
    "percentage",
    "pct",
)


def extract_ob_strength_from_meta(raw_meta: dict) -> "tuple[float | None, str]":
    """
    Extract the best available OB strength number from raw_meta_json dict.

    Checks _OB_STRENGTH_KEYS left to right; returns (value, source_key) for
    the first non-None numeric hit. Returns (None, "missing") if nothing found.
    """
    for key in _OB_STRENGTH_KEYS:
        val = raw_meta.get(key)
        if val is None:
            continue
        try:
            f = float(val)
            if f > 0:
                return round(f, 2), key
        except (TypeError, ValueError):
            continue
    return None, "missing"


# Candidate keys that would contain OB formation timestamp (none stored yet)
_OB_ORIGIN_KEYS = (
    "ob_formed_at", "formed_at", "origin_time", "origin_ts",
    "ob_time", "start_time", "start_ts", "left_time",
    "candle_time", "formation_time",
)


def get_ob_origin_time(ev) -> "datetime | None":
    """
    Try to extract OB formation timestamp from raw_meta_json.

    Checks all known candidate keys. Returns None if none found —
    current DB rows do not store formation time.
    """
    try:
        meta = json.loads(ev.raw_meta_json or "{}")
    except Exception:
        return None
    for key in _OB_ORIGIN_KEYS:
        val = meta.get(key)
        if val:
            # Handle ISO strings or numeric timestamps
            try:
                from datetime import datetime
                if isinstance(val, (int, float)):
                    ts = float(val)
                    if ts > 1e12:
                        ts /= 1000.0
                    return datetime.fromtimestamp(ts, tz=timezone.utc)
                return datetime.fromisoformat(str(val).replace("Z", "+00:00"))
            except Exception:
                continue
    return None


def classify_ob_freshness(ev, candles: list, detected_ms: int) -> dict:
    """
    Classify OB freshness using pre-signal candles from the already-fetched
    candle array (zero extra API calls).

    Candles with openTime < detected_ms are pre-signal. We count how many
    overlap the OB zone (high >= zone_low AND low <= zone_high).

    Returns:
        freshness_status : "first_touch" | "already_touched" | "unknown"
        first_touch      : True | False | None
        touch_count_before_signal : int | None
        first_touch_time : ISO str | None
        origin_time      : ISO str | None (always None until metadata added)
        reason           : str
    """
    zone_high = ev.zone_high
    zone_low  = ev.zone_low

    # Try stored origin time (currently returns None for all DB rows)
    origin_dt = get_ob_origin_time(ev)
    origin_ms = None
    if origin_dt:
        origin_ms = int(origin_dt.timestamp() * 1000)

    # Identify pre-signal candles in the already-fetched array
    pre_signal = [
        c for c in candles
        if c.get("openTime", 0) < detected_ms
        and (origin_ms is None or c.get("openTime", 0) >= origin_ms)
    ]

    if not pre_signal:
        return {
            "freshness_status":          "unknown",
            "first_touch":               None,
            "touch_count_before_signal": None,
            "first_touch_time":          None,
            "origin_time":               origin_dt.isoformat() if origin_dt else None,
            "reason":                    (
                "no_origin_metadata_and_no_pre_signal_candles_in_fetch_window"
                if origin_dt is None
                else "no_candles_between_origin_and_detection"
            ),
        }

    # Count zone overlaps: candle touches zone when high >= zone_low AND low <= zone_high
    touches = []
    for c in pre_signal:
        c_high = c.get("high", 0)
        c_low  = c.get("low", 0)
        if c_high >= zone_low and c_low <= zone_high:
            touches.append(c)

    touch_count = len(touches)
    first_touch_time = None
    if touches:
        ft_ms = touches[0].get("openTime")
        if ft_ms:
            from datetime import datetime
            first_touch_time = datetime.fromtimestamp(ft_ms / 1000, tz=timezone.utc).isoformat()

    if touch_count == 0:
        status = "first_touch"
        reason = (
            "no_zone_touches_in_pre_signal_window"
            if origin_dt is None
            else "no_zone_touches_between_origin_and_detection"
        )
    else:
        status = "already_touched"
        reason = (
            "zone_touched_in_pre_signal_window"
            if origin_dt is None
            else "zone_touched_between_origin_and_detection"
        )

    return {
        "freshness_status":          status,
        "first_touch":               touch_count == 0,
        "touch_count_before_signal": touch_count,
        "first_touch_time":          first_touch_time,
        "origin_time":               origin_dt.isoformat() if origin_dt else None,
        "reason":                    reason,
    }


def _pct(num, denom):
    return round(num / denom * 100, 2) if denom else None


def _classify_loss_reason(
    primary_res: dict, alt_res: dict, bounce: float, stop_mode: str = "wick"
) -> str:
    """
    Classify why a signal ended the way it did.

    primary_res  — the result for the requested stop_mode (wick or close).
    alt_res      — the result for the opposite stop_mode.
    stop_mode    — "wick" | "close" — the mode used for primary_res.

    When stop_mode="wick", alt_res is the close-stop result.
      wick_stop_only = wick LOST but close would have survived.
    When stop_mode="close", alt_res is the wick-stop result.
      close_stop_also_lost = both modes agree on LOST.
    """
    status = primary_res.get("status")
    if status == "WON":               return "won_clean"
    if status == "AMBIGUOUS":         return "target_and_stop_same_candle"
    if status == "EXPIRED":           return "no_entry_expired"
    if status == "WAITING_FOR_ENTRY": return "waiting_for_entry"
    if status == "ENTERED":           return "entered_open"
    if status == "LOST":
        if primary_res.get("same_candle_entry_stop"):
            return "same_candle_stop"
        alt_status = alt_res.get("status", "LOST")
        if stop_mode == "wick" and alt_status != "LOST":
            # Wick triggered stop but candle close would have survived
            return "wick_stop_only"
        mfe = primary_res.get("mfe_pct") or 0.0
        if mfe >= bounce * 0.5:
            return "stop_after_clean_entry"
        return "close_stop_also_lost"
    return "unknown"


def _cf_compact(res: dict) -> dict:
    """Compact counterfactual summary for per-signal JSON."""
    return {
        "status":        res.get("status"),
        "result_reason": res.get("result_reason"),
        "mfe_pct":       res.get("mfe_pct"),
        "mae_pct":       res.get("mae_pct"),
    }


def run_ob_backtest(
    limit: int = 100,
    timeframe: str = None,
    pair: str = None,
    setup_type: str = None,
    source: str = "live",
    result_filter: str = None,
    stop_mode: str = "wick",
    freshness: str = "all",
) -> dict:
    """
    OB-only candle-replay backtest with Phase 7B/7C diagnostics.

    stop_mode="wick"  — traditional: loss when wick crosses OB boundary.
    stop_mode="close" — strict: loss only when candle CLOSES beyond boundary.

    Runs 4 _run_traced variants per signal then classifies each outcome.
    Never commits to DB. Never mutates SignalEvent or SignalOutcome.
    """
    try:
        from models import db, SignalEvent, SignalOutcome
        from main import get_klines_exchange
        from signal_logger import BOUNCE_THRESHOLDS
        from outcome_resolver import EXPIRY_CANDLES
        from resolver_audit import _run_traced, _ts_ms, _SMALL_BOUNCE

        # ── Normalise params ─────────────────────────────────────────────
        cap = max(1, min(int(limit), 500))

        stop_mode = (stop_mode or "wick").strip().lower()
        if stop_mode not in {"wick", "close"}:
            return {"ok": False, "error": f"stop_mode '{stop_mode}' invalid; must be wick or close"}

        freshness_filter = (freshness or "all").strip().lower()
        if freshness_filter not in _FRESHNESS_VALUES:
            return {"ok": False, "error": f"freshness '{freshness_filter}' invalid; must be all/first_touch/already_touched/unknown"}

        rf = (result_filter or "").strip().lower()
        rf = rf if rf and rf != "all" else None
        target_statuses = _RESULT_FILTER_MAP.get(rf) if rf else None

        st_filter = (setup_type or "").strip().upper()
        st_filter = st_filter if st_filter and st_filter != "ALL" else None
        if st_filter and st_filter not in _OB_SETUP_TYPES:
            return {
                "ok": False,
                "error": f"setup_type '{st_filter}' invalid; must be OB_APPROACH or OB_CONSOL",
            }

        src = (source or "live").strip().lower()
        if src == "all":
            src = None

        # ── Query — OB module only ───────────────────────────────────────
        q = SignalEvent.query.filter(SignalEvent.module == _OB_MODULE)
        if src:
            q = q.filter(SignalEvent.source == src)
        if timeframe:
            q = q.filter(SignalEvent.timeframe == timeframe.strip())
        if pair:
            q = q.filter(SignalEvent.pair == pair.strip().upper())
        if st_filter:
            q = q.filter(SignalEvent.setup_type == st_filter)
        else:
            q = q.filter(SignalEvent.setup_type.in_(list(_OB_SETUP_TYPES)))

        events = q.order_by(SignalEvent.detected_at.desc()).limit(cap).all()

        # ── Counters ─────────────────────────────────────────────────────
        summary = {
            "checked": 0, "won": 0, "lost": 0, "expired": 0,
            "ambiguous": 0, "waiting_for_entry": 0, "entered": 0, "errors": 0,
        }
        freshness_summary = {"first_touch": 0, "already_touched": 0, "unknown": 0}
        diagnostics = {k: 0 for k in _DIAG_KEYS}
        diag_summary = {
            "all_variants_lost":                0,
            "small_target_would_win":           0,
            "candle_close_entry_would_win":     0,
            "candle_close_entry_would_survive": 0,
            "all_variants_expired":             0,
            "no_entry_expired":                 0,
            "target_reached":                   0,
            "target_and_stop_same_candle":      0,
            "stop_hit":                         0,
        }

        # ── Breakdowns ───────────────────────────────────────────────────
        tf_data:    dict = {}
        setup_data: dict = {}
        score_buckets = [
            {"bucket": "80-100", "lo": 80, "hi": 100,
             "checked": 0, "won": 0, "lost": 0, "diag": {k: 0 for k in _DIAG_KEYS}},
            {"bucket": "60-79",  "lo": 60, "hi": 79,
             "checked": 0, "won": 0, "lost": 0, "diag": {k: 0 for k in _DIAG_KEYS}},
            {"bucket": "40-59",  "lo": 40, "hi": 59,
             "checked": 0, "won": 0, "lost": 0, "diag": {k: 0 for k in _DIAG_KEYS}},
            {"bucket": "0-39",   "lo": 0,  "hi": 39,
             "checked": 0, "won": 0, "lost": 0, "diag": {k: 0 for k in _DIAG_KEYS}},
        ]

        def _tally_breakdowns(ev, status, diag_key):
            tf = ev.timeframe
            if tf not in tf_data:
                tf_data[tf] = {
                    "timeframe": tf, "checked": 0, "won": 0,
                    "lost": 0, "expired": 0, "ambiguous": 0,
                    "waiting_for_entry": 0, "entered": 0,
                    "diag": {k: 0 for k in _DIAG_KEYS},
                }
            d = tf_data[tf]
            d["checked"] += 1
            if   status == "WON":               d["won"]               += 1
            elif status == "LOST":              d["lost"]              += 1
            elif status == "EXPIRED":           d["expired"]           += 1
            elif status == "AMBIGUOUS":         d["ambiguous"]         += 1
            elif status == "WAITING_FOR_ENTRY": d["waiting_for_entry"] += 1
            elif status == "ENTERED":           d["entered"]           += 1
            d["diag"][diag_key] = d["diag"].get(diag_key, 0) + 1

            st = ev.setup_type or "unknown"
            if st not in setup_data:
                setup_data[st] = {
                    "setup_type": st, "checked": 0, "won": 0,
                    "lost": 0, "expired": 0, "ambiguous": 0,
                    "waiting_for_entry": 0, "entered": 0,
                    "diag": {k: 0 for k in _DIAG_KEYS},
                }
            sd = setup_data[st]
            sd["checked"] += 1
            if   status == "WON":               sd["won"]               += 1
            elif status == "LOST":              sd["lost"]              += 1
            elif status == "EXPIRED":           sd["expired"]           += 1
            elif status == "AMBIGUOUS":         sd["ambiguous"]         += 1
            elif status == "WAITING_FOR_ENTRY": sd["waiting_for_entry"] += 1
            elif status == "ENTERED":           sd["entered"]           += 1
            sd["diag"][diag_key] = sd["diag"].get(diag_key, 0) + 1

            score = ev.score or 0
            for b in score_buckets:
                if b["lo"] <= score <= b["hi"]:
                    b["checked"] += 1
                    if status == "WON":  b["won"]  += 1
                    if status == "LOST": b["lost"] += 1
                    b["diag"][diag_key] = b["diag"].get(diag_key, 0) + 1
                    break

        def _iso(dt):
            return dt.isoformat() if dt else None

        signal_rows = []

        for ev in events:
            try:
                outcome = SignalOutcome.query.filter_by(
                    signal_id=ev.signal_id
                ).first()

                bounce = (
                    outcome.bounce_threshold_pct
                    if outcome and outcome.bounce_threshold_pct
                    else BOUNCE_THRESHOLDS.get(ev.timeframe, 0.010)
                )
                small_bounce = _SMALL_BOUNCE.get(ev.timeframe, bounce * 0.6)

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
                    summary["errors"] += 1
                    signal_rows.append({
                        "signal_id":        ev.signal_id,
                        "pair":             ev.pair,
                        "timeframe":        ev.timeframe,
                        "setup_type":       ev.setup_type,
                        "direction":        ev.direction,
                        "score":            ev.score,
                        "result":           "ERROR",
                        "result_reason":    "no_candle_data",
                        "loss_reason_detail": "unknown",
                        "freshness_status": "unknown",
                    })
                    continue

                # ── OB strength from raw_meta_json ───────────────────────
                try:
                    ev_meta = json.loads(ev.raw_meta_json or "{}")
                except Exception:
                    ev_meta = {}
                ob_strength, ob_strength_source = extract_ob_strength_from_meta(ev_meta)

                # ── Freshness classification (uses pre-signal candles in ──
                # ── already-fetched array — no extra API calls)           ──
                freshness_info = classify_ob_freshness(ev, candles, detected_ms)
                fs = freshness_info["freshness_status"]
                freshness_summary[fs] = freshness_summary.get(fs, 0) + 1

                # Apply freshness filter — skip signal if it doesn't match
                if freshness_filter != "all" and fs != freshness_filter:
                    continue

                # ── 4 variants — same candle list, no extra fetches ──────
                res_wick, _, _  = _run_traced(
                    ev.zone_high, ev.zone_low, ev.direction,
                    bounce, detected_ms, candles, ev.timeframe,
                    stop_mode="wick", entry_price_mode="zone",
                )
                res_close, _, _ = _run_traced(
                    ev.zone_high, ev.zone_low, ev.direction,
                    bounce, detected_ms, candles, ev.timeframe,
                    stop_mode="close", entry_price_mode="zone",
                )
                res_small, _, _ = _run_traced(
                    ev.zone_high, ev.zone_low, ev.direction,
                    small_bounce, detected_ms, candles, ev.timeframe,
                    stop_mode="wick", entry_price_mode="zone",
                )
                res_cce, _, _   = _run_traced(
                    ev.zone_high, ev.zone_low, ev.direction,
                    bounce, detected_ms, candles, ev.timeframe,
                    stop_mode="wick", entry_price_mode="candle_close",
                )

                res_primary = res_close if stop_mode == "close" else res_wick
                cf_alt_res  = res_wick  if stop_mode == "close" else res_close

                status     = res_primary["status"]
                diag_key   = _classify_loss_reason(res_primary, cf_alt_res, bounce, stop_mode)

                summary["checked"] += 1
                if   status == "WON":               summary["won"]               += 1
                elif status == "LOST":              summary["lost"]              += 1
                elif status == "EXPIRED":           summary["expired"]           += 1
                elif status == "AMBIGUOUS":         summary["ambiguous"]         += 1
                elif status == "WAITING_FOR_ENTRY": summary["waiting_for_entry"] += 1
                elif status == "ENTERED":           summary["entered"]           += 1

                diagnostics[diag_key] = diagnostics.get(diag_key, 0) + 1
                _tally_breakdowns(ev, status, diag_key)

                # ── Cross-variant diagnostic summary ─────────────────────
                _alt_s  = cf_alt_res["status"]
                _st_s   = res_small["status"]
                _cce_s  = res_cce["status"]
                _reason = res_primary.get("result_reason")

                if status == "LOST":
                    if _alt_s == "LOST" and _st_s == "LOST" and _cce_s == "LOST":
                        diag_summary["all_variants_lost"] += 1
                    if _st_s == "WON":
                        diag_summary["small_target_would_win"] += 1
                    if _cce_s == "WON":
                        diag_summary["candle_close_entry_would_win"] += 1
                    if _cce_s in {"ENTERED", "WAITING_FOR_ENTRY", "EXPIRED", "AMBIGUOUS"}:
                        diag_summary["candle_close_entry_would_survive"] += 1

                if status == "EXPIRED" and _alt_s == "EXPIRED" and _st_s == "EXPIRED" and _cce_s == "EXPIRED":
                    diag_summary["all_variants_expired"] += 1

                if _reason == "no_entry_within_expiry":
                    diag_summary["no_entry_expired"] += 1
                elif _reason == "target_reached":
                    diag_summary["target_reached"] += 1
                elif _reason == "target_and_stop_same_candle":
                    diag_summary["target_and_stop_same_candle"] += 1
                elif _reason == "stop_hit":
                    diag_summary["stop_hit"] += 1

                if target_statuses and status not in target_statuses:
                    continue

                signal_rows.append({
                    "signal_id":          ev.signal_id,
                    "pair":               ev.pair,
                    "timeframe":          ev.timeframe,
                    "setup_type":         ev.setup_type,
                    "direction":          ev.direction,
                    "score":              ev.score,
                    "detected_at":        _iso(ev.detected_at),
                    "detected_price":     ev.detected_price,
                    "zone_high":          ev.zone_high,
                    "zone_low":           ev.zone_low,
                    "entry_time":         _iso(res_primary.get("entry_time")),
                    "entry_price":        res_primary.get("entry_price"),
                    "target_price":       res_primary.get("target_price"),
                    "stop_price":         res_primary.get("stop_price"),
                    "exit_time":          _iso(res_primary.get("exit_time")),
                    "exit_price":         res_primary.get("exit_price"),
                    "result":             status,
                    "result_reason":      res_primary.get("result_reason"),
                    "candles_checked":    res_primary.get("candles_checked", 0),
                    "mfe_pct":            res_primary.get("mfe_pct"),
                    "mae_pct":            res_primary.get("mae_pct"),
                    "bounce_threshold_pct": bounce,
                    "loss_reason_detail": diag_key,
                    "cf_alt_stop":           _cf_compact(cf_alt_res),
                    "cf_small_target":       _cf_compact(res_small),
                    "cf_candle_close_entry": _cf_compact(res_cce),
                    "freshness_status":          fs,
                    "touch_count_before_signal": freshness_info.get("touch_count_before_signal"),
                    "origin_time":               freshness_info.get("origin_time"),
                    "freshness_reason":          freshness_info.get("reason"),
                    "ob_strength":               ob_strength,
                    "ob_strength_source":        ob_strength_source,
                })

            except Exception as _sig_err:
                summary["errors"] += 1
                signal_rows.append({
                    "pair":             getattr(ev, "pair", "?"),
                    "result":           "ERROR",
                    "result_reason":    str(_sig_err),
                    "loss_reason_detail": "unknown",
                })

        # ── Win rates ────────────────────────────────────────────────────
        clean         = summary["won"] + summary["lost"]
        total_decided = clean + summary["expired"] + summary["ambiguous"]
        summary["win_rate_entered"] = _pct(summary["won"], clean)
        summary["win_rate_total"]   = _pct(summary["won"], total_decided)

        def _add_wr(d):
            c = d["won"] + d["lost"]
            t = c + d.get("expired", 0) + d.get("ambiguous", 0)
            d["win_rate_entered"] = _pct(d["won"], c)
            d["win_rate_total"]   = _pct(d["won"], t)

        by_tf    = sorted(tf_data.values(),    key=lambda x: x["checked"], reverse=True)
        by_setup = sorted(setup_data.values(), key=lambda x: x["checked"], reverse=True)
        for d in by_tf:    _add_wr(d)
        for d in by_setup: _add_wr(d)

        by_score = [
            {
                "bucket":           b["bucket"],
                "checked":          b["checked"],
                "won":              b["won"],
                "lost":             b["lost"],
                "win_rate_entered": _pct(b["won"], b["won"] + b["lost"]),
                "diag":             b["diag"],
            }
            for b in score_buckets
        ]

        rule_note = (
            "Loss requires candle close beyond OB zone"
            if stop_mode == "close"
            else "Loss uses wick stop (default)"
        )
        return {
            "ok":               True,
            "mode":             "ob_backtest_dry_run",
            "stop_mode":        stop_mode,
            "rule_note":        rule_note,
            "freshness_filter": freshness_filter,
            "filters": {
                "module":      _OB_MODULE,
                "setup_types": list(_OB_SETUP_TYPES),
                "setup_type":  st_filter or "all",
                "timeframe":   timeframe,
                "pair":        pair,
                "source":      source,
                "result":      rf or "all",
                "limit":       cap,
                "stop_mode":   stop_mode,
                "freshness":   freshness_filter,
            },
            "freshness_summary": freshness_summary,
            "summary":            summary,
            "diagnostics":        diagnostics,
            "diagnostic_summary": diag_summary,
            "by_timeframe":    by_tf,
            "by_setup_type":   by_setup,
            "by_score_bucket": by_score,
            "summary_filtered": {
                "result":   rf or "all",
                "returned": len(signal_rows),
            },
            "signals": signal_rows,
        }

    except Exception as _outer:
        return {"ok": False, "error": str(_outer), "mode": "ob_backtest_dry_run"}
