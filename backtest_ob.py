"""
backtest_ob.py — Phase 7B: OB Backtest with Loss Diagnostics

Phase 7A base: read-only OB-only candle-replay backtest.
Phase 7B adds: per-signal loss_reason_detail, 3 counterfactual variants,
and a diagnostics summary + per-breakdown diagnostic counts.

Rules:
  - Never mutates SignalEvent or SignalOutcome.
  - Never writes to DB.
  - module="ob" ONLY, setup_type in OB_APPROACH / OB_CONSOL.
  - Must be called inside an active Flask app context.
"""

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


def _pct(num, denom):
    return round(num / denom * 100, 2) if denom else None


def _classify_loss_reason(wick_res: dict, close_res: dict, bounce: float) -> str:
    """
    Classify why a signal ended the way it did.
    Uses the wick-stop result as primary and close-stop counterfactual for LOST cases.
    """
    status = wick_res.get("status")
    if status == "WON":               return "won_clean"
    if status == "AMBIGUOUS":         return "target_and_stop_same_candle"
    if status == "EXPIRED":           return "no_entry_expired"
    if status == "WAITING_FOR_ENTRY": return "waiting_for_entry"
    if status == "ENTERED":           return "entered_open"
    if status == "LOST":
        if wick_res.get("same_candle_entry_stop"):
            return "same_candle_stop"
        cs_status = close_res.get("status", "LOST")
        if cs_status != "LOST":
            # Close-based stop would have survived — wick triggered prematurely
            return "wick_stop_only"
        # Both wick and close would have stopped out
        mfe = wick_res.get("mfe_pct") or 0.0
        if mfe >= bounce * 0.5:
            # Price moved favorably before reversing — genuine reversal loss
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
) -> dict:
    """
    OB-only candle-replay backtest with Phase 7B diagnostics.

    Runs 4 _run_traced variants per signal (wick-stop, close-stop,
    small-target, candle-close-entry) then classifies each outcome.
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
                    })
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

                status     = res_wick["status"]
                diag_key   = _classify_loss_reason(res_wick, res_close, bounce)

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
                _cs  = res_close["status"]
                _st  = res_small["status"]
                _cce = res_cce["status"]
                _reason = res_wick.get("result_reason")

                if status == "LOST":
                    if _st == "LOST" and _cce == "LOST":
                        diag_summary["all_variants_lost"] += 1
                    if _st == "WON":
                        diag_summary["small_target_would_win"] += 1
                    if _cce == "WON":
                        diag_summary["candle_close_entry_would_win"] += 1
                    if _cce in {"ENTERED", "WAITING_FOR_ENTRY", "EXPIRED", "AMBIGUOUS"}:
                        diag_summary["candle_close_entry_would_survive"] += 1

                if status == "EXPIRED" and _cs == "EXPIRED" and _st == "EXPIRED" and _cce == "EXPIRED":
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
                    "entry_time":         _iso(res_wick.get("entry_time")),
                    "entry_price":        res_wick.get("entry_price"),
                    "target_price":       res_wick.get("target_price"),
                    "stop_price":         res_wick.get("stop_price"),
                    "exit_time":          _iso(res_wick.get("exit_time")),
                    "exit_price":         res_wick.get("exit_price"),
                    "result":             status,
                    "result_reason":      res_wick.get("result_reason"),
                    "candles_checked":    res_wick.get("candles_checked", 0),
                    "mfe_pct":            res_wick.get("mfe_pct"),
                    "mae_pct":            res_wick.get("mae_pct"),
                    "bounce_threshold_pct": bounce,
                    "loss_reason_detail": diag_key,
                    "cf_close_stop":      _cf_compact(res_close),
                    "cf_small_target":    _cf_compact(res_small),
                    "cf_candle_close_entry": _cf_compact(res_cce),
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

        return {
            "ok":   True,
            "mode": "ob_backtest_dry_run",
            "filters": {
                "module":      _OB_MODULE,
                "setup_types": list(_OB_SETUP_TYPES),
                "setup_type":  st_filter or "all",
                "timeframe":   timeframe,
                "pair":        pair,
                "source":      source,
                "result":      rf or "all",
                "limit":       cap,
            },
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
