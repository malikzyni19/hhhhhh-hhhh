"""
backtest_ob.py — Phase 7A: OB-Only Backtest Foundation

Read-only dry-run backtest for Order Block signals only.
Replays candles after detected_at and determines entry/exit outcomes
using the same rules as the production outcome resolver.

Design:
  - module="ob" ONLY. Breaker, FVG, Fib Confluence excluded.
  - setup_type in ["OB_APPROACH", "OB_CONSOL"] only.
  - Never mutates SignalEvent or SignalOutcome.
  - Reuses _run_traced() from resolver_audit.py for replay logic.
  - Must be called inside an active Flask app context.
"""

from datetime import timezone

_OB_MODULE      = "ob"
_OB_SETUP_TYPES = frozenset({"OB_APPROACH", "OB_CONSOL"})

# Valid result filter values → set of replay statuses that match
_RESULT_FILTER_MAP = {
    "won":     {"WON"},
    "lost":    {"LOST"},
    "expired": {"EXPIRED"},
    "ambiguous":     {"AMBIGUOUS"},
    "waiting":       {"WAITING_FOR_ENTRY"},
    "entered":       {"ENTERED"},
}


def _pct(num, denom):
    return round(num / denom * 100, 2) if denom else None


def run_ob_backtest(
    limit: int = 100,
    timeframe: str = None,
    pair: str = None,
    setup_type: str = None,     # "all" | "OB_APPROACH" | "OB_CONSOL"
    source: str = "live",
    result_filter: str = None,  # "all"|"won"|"lost"|"expired"|"ambiguous"|"waiting"|"entered"
) -> dict:
    """
    OB-only candle-replay backtest.

    Must be called inside an active Flask app context.
    Never commits to DB. Never mutates SignalEvent or SignalOutcome.

    Returns:
      {ok, mode, filters, summary, by_timeframe, by_setup_type,
       by_score_bucket, signals}
    """
    try:
        from models import db, SignalEvent, SignalOutcome
        from main import get_klines_exchange
        from signal_logger import BOUNCE_THRESHOLDS
        from outcome_resolver import EXPIRY_CANDLES
        from resolver_audit import _run_traced, _ts_ms

        # ── Normalise params ─────────────────────────────────────────────
        cap = max(1, min(int(limit), 500))

        rf = (result_filter or "").strip().lower()
        rf = rf if rf and rf != "all" else None
        target_statuses = _RESULT_FILTER_MAP.get(rf) if rf else None

        st_filter = (setup_type or "").strip().upper()
        st_filter = st_filter if st_filter and st_filter != "ALL" else None
        if st_filter and st_filter not in _OB_SETUP_TYPES:
            return {"ok": False, "error": f"setup_type '{st_filter}' invalid; must be OB_APPROACH or OB_CONSOL"}

        src = (source or "live").strip().lower()
        if src == "all":
            src = None  # no source filter

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
            # Always restrict to valid OB setup types
            q = q.filter(SignalEvent.setup_type.in_(list(_OB_SETUP_TYPES)))

        events = q.order_by(SignalEvent.detected_at.desc()).limit(cap).all()

        # ── Summary counters ─────────────────────────────────────────────
        summary = {
            "checked": 0, "won": 0, "lost": 0, "expired": 0,
            "ambiguous": 0, "waiting_for_entry": 0, "entered": 0, "errors": 0,
        }

        # Breakdowns — built before result filter is applied
        tf_data:    dict = {}
        setup_data: dict = {}
        score_buckets = [
            {"bucket": "80-100", "lo": 80, "hi": 100,
             "checked": 0, "won": 0, "lost": 0},
            {"bucket": "60-79",  "lo": 60, "hi": 79,
             "checked": 0, "won": 0, "lost": 0},
            {"bucket": "40-59",  "lo": 40, "hi": 59,
             "checked": 0, "won": 0, "lost": 0},
            {"bucket": "0-39",   "lo": 0,  "hi": 39,
             "checked": 0, "won": 0, "lost": 0},
        ]

        def _tally_breakdowns(ev, status):
            # By timeframe
            tf = ev.timeframe
            if tf not in tf_data:
                tf_data[tf] = {"timeframe": tf, "checked": 0, "won": 0,
                                "lost": 0, "expired": 0, "ambiguous": 0,
                                "waiting_for_entry": 0, "entered": 0}
            d = tf_data[tf]
            d["checked"] += 1
            if   status == "WON":               d["won"]               += 1
            elif status == "LOST":              d["lost"]              += 1
            elif status == "EXPIRED":           d["expired"]           += 1
            elif status == "AMBIGUOUS":         d["ambiguous"]         += 1
            elif status == "WAITING_FOR_ENTRY": d["waiting_for_entry"] += 1
            elif status == "ENTERED":           d["entered"]           += 1

            # By setup type
            st = ev.setup_type or "unknown"
            if st not in setup_data:
                setup_data[st] = {"setup_type": st, "checked": 0, "won": 0,
                                   "lost": 0, "expired": 0, "ambiguous": 0,
                                   "waiting_for_entry": 0, "entered": 0}
            sd = setup_data[st]
            sd["checked"] += 1
            if   status == "WON":               sd["won"]               += 1
            elif status == "LOST":              sd["lost"]              += 1
            elif status == "EXPIRED":           sd["expired"]           += 1
            elif status == "AMBIGUOUS":         sd["ambiguous"]         += 1
            elif status == "WAITING_FOR_ENTRY": sd["waiting_for_entry"] += 1
            elif status == "ENTERED":           sd["entered"]           += 1

            # By score bucket
            score = ev.score or 0
            for b in score_buckets:
                if b["lo"] <= score <= b["hi"]:
                    b["checked"] += 1
                    if status == "WON":  b["won"]  += 1
                    if status == "LOST": b["lost"] += 1
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
                        "signal_id":   ev.signal_id,
                        "pair":        ev.pair,
                        "timeframe":   ev.timeframe,
                        "setup_type":  ev.setup_type,
                        "direction":   ev.direction,
                        "score":       ev.score,
                        "result":      "ERROR",
                        "result_reason": "no_candle_data",
                    })
                    continue

                res, _, _ = _run_traced(
                    ev.zone_high, ev.zone_low, ev.direction,
                    bounce, detected_ms, candles, ev.timeframe,
                    stop_mode="wick", entry_price_mode="zone",
                )

                status = res["status"]

                # Tally summary_all before filter
                summary["checked"] += 1
                if   status == "WON":               summary["won"]               += 1
                elif status == "LOST":              summary["lost"]              += 1
                elif status == "EXPIRED":           summary["expired"]           += 1
                elif status == "AMBIGUOUS":         summary["ambiguous"]         += 1
                elif status == "WAITING_FOR_ENTRY": summary["waiting_for_entry"] += 1
                elif status == "ENTERED":           summary["entered"]           += 1

                _tally_breakdowns(ev, status)

                # Apply result filter
                if target_statuses and status not in target_statuses:
                    continue

                signal_rows.append({
                    "signal_id":     ev.signal_id,
                    "pair":          ev.pair,
                    "timeframe":     ev.timeframe,
                    "setup_type":    ev.setup_type,
                    "direction":     ev.direction,
                    "score":         ev.score,
                    "detected_at":   _iso(ev.detected_at),
                    "detected_price":ev.detected_price,
                    "zone_high":     ev.zone_high,
                    "zone_low":      ev.zone_low,
                    "entry_time":    _iso(res.get("entry_time")),
                    "entry_price":   res.get("entry_price"),
                    "target_price":  res.get("target_price"),
                    "stop_price":    res.get("stop_price"),
                    "exit_time":     _iso(res.get("exit_time")),
                    "exit_price":    res.get("exit_price"),
                    "result":        status,
                    "result_reason": res.get("result_reason"),
                    "candles_checked": res.get("candles_checked", 0),
                    "mfe_pct":       res.get("mfe_pct"),
                    "mae_pct":       res.get("mae_pct"),
                    "bounce_threshold_pct": bounce,
                })

            except Exception as _sig_err:
                summary["errors"] += 1
                signal_rows.append({
                    "pair":          getattr(ev, "pair", "?"),
                    "result":        "ERROR",
                    "result_reason": str(_sig_err),
                })

        # ── Win rates ────────────────────────────────────────────────────
        clean = summary["won"] + summary["lost"]
        total_decided = clean + summary["expired"] + summary["ambiguous"]
        summary["win_rate_entered"] = _pct(summary["won"], clean)
        summary["win_rate_total"]   = _pct(summary["won"], total_decided)

        # ── Add win rates to breakdowns ──────────────────────────────────
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
                "bucket":          b["bucket"],
                "checked":         b["checked"],
                "won":             b["won"],
                "lost":            b["lost"],
                "win_rate_entered": _pct(b["won"], b["won"] + b["lost"]),
            }
            for b in score_buckets
        ]

        return {
            "ok":   True,
            "mode": "ob_backtest_dry_run",
            "filters": {
                "module":        _OB_MODULE,
                "setup_types":   list(_OB_SETUP_TYPES),
                "setup_type":    st_filter or "all",
                "timeframe":     timeframe,
                "pair":          pair,
                "source":        source,
                "result":        rf or "all",
                "limit":         cap,
            },
            "summary":       summary,
            "by_timeframe":  by_tf,
            "by_setup_type": by_setup,
            "by_score_bucket": by_score,
            "summary_filtered": {
                "result":   rf or "all",
                "returned": len(signal_rows),
            },
            "signals": signal_rows,
        }

    except Exception as _outer:
        return {"ok": False, "error": str(_outer), "mode": "ob_backtest_dry_run"}
