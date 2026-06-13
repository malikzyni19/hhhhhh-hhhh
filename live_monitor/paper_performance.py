"""Phase 11.14 (Hotfix): Paper Performance Dashboard — read-only analytics.

Query-only. No execution. No order creation. No position mutation.
No Paper Auto Gate or Risk Guard changes. No exchange calls.
can_auto_submit is ALWAYS False. auto_execution_allowed is ALWAYS False.
ai_can_execute is ALWAYS False.

Hotfix changes:
- Canonical performance timestamp: coalesce(closed_at, updated_at, created_at) for
  all SQL filters, ordering, and prior-period queries.
- Case-insensitive closed-status filter.
- Canonical outcome classifier: PnL-sign wins over label when they disagree;
  mismatches tracked in data_quality without corrupting monetary aggregates.
- Monetary aggregates (gross_profit, gross_loss, avg_win, avg_loss) always use
  realized_pnl sign — never the outcome label.
- PnL sum reconciliation: direct_sum vs gross_profit−gross_loss, reported + warned.
- Safe item_id parsing (no ValueError).
- Case-insensitive side normalization via lowercase lookup.
- Row-limit metadata: total_available, rows_loaded, truncated.
- analytics_row_limit_reached sample warning when truncated.
- Data quality counters: explicit/derived/mismatch/missing_pnl/invalid_pnl/
  missing_timestamp/missing_rr/missing_duration/missing_pnl_pct.
- Prior-period comparison uses canonical timestamp + drawdown delta + expectancy delta.
- Breakdowns use canonical outcome for win/loss counts and PnL-sign for monetary.
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone, timedelta
from statistics import median as _stat_median
from sqlalchemy import func as _sa_func

_PHASE          = "phase11_14_paper_performance_dashboard"
_VALID_PERIODS  = frozenset({"7d", "30d", "90d", "365d", "all"})
_DEFAULT_PERIOD = "30d"
_MAX_ROWS       = 5_000
_MAX_RECENT     = 50
_MAX_CURVE      = 500
_MAX_SYMS       = 20
_PERIOD_DAYS    = {"7d": 7, "30d": 30, "90d": 90, "365d": 365}

# Lowercase-only keys — lookup normalises input with .lower() before checking
_SIDE_NORM: dict = {
    "long": "BUY",  "buy": "BUY",  "bullish": "BUY",
    "short": "SELL", "sell": "SELL", "bearish": "SELL",
}

_GUARDRAILS: dict = {
    "read_only":                   True,
    "paper_primary":               True,
    "can_auto_submit":             False,
    "auto_execution_allowed":      False,
    "ai_can_execute":              False,
    "live_disabled":               True,
    "testnet_strategy_validation": False,
}


# ── Decimal / string helpers ──────────────────────────────────────────────────

def _sdec(v):
    """Safe Decimal. Returns None for None/NaN/Infinity/unparseable."""
    if v is None:
        return None
    try:
        d = Decimal(str(v).strip())
        if d.is_nan() or d.is_infinite():
            return None
        return d
    except (InvalidOperation, TypeError, ValueError):
        return None


def _ds(d):
    """Compact fixed-point string, or None."""
    if d is None:
        return None
    try:
        s = format(d, "f")
        if "." in s:
            s = s.rstrip("0").rstrip(".")
        return s
    except Exception:
        return str(d)


def _ts(trade):
    """Best close timestamp: closed_at → updated_at → created_at, UTC-normalised."""
    raw = (
        getattr(trade, "closed_at",  None)
        or getattr(trade, "updated_at", None)
        or getattr(trade, "created_at", None)
    )
    if raw is None:
        return None
    if hasattr(raw, "tzinfo") and raw.tzinfo is None:
        raw = raw.replace(tzinfo=timezone.utc)
    return raw


# ── Canonical outcome classification ─────────────────────────────────────────

def _lm_classify_paper_trade_outcome(trade):
    """Canonical outcome for a trade record.

    Returns (canonical, pnl, mismatch, source) where:
      canonical : 'win' | 'loss' | 'breakeven' | None (missing/invalid PnL)
      pnl       : Decimal or None
      mismatch  : True when explicit label disagreed with PnL sign
      source    : 'explicit' | 'pnl_override' | 'derived' | 'missing_pnl' | 'invalid_pnl'

    Rules (in order):
      1. Parse realized_pnl. Missing → ('missing_pnl'). Malformed → ('invalid_pnl').
      2. Derive PnL-sign outcome: pnl>0 → win, pnl<0 → loss, pnl==0 → breakeven.
      3. Read explicit outcome field.
      4. Explicit present and AGREES with PnL-sign → source='explicit'.
      5. Explicit present and DISAGREES → canonical=PnL-sign, mismatch=True, source='pnl_override'.
      6. Explicit absent/invalid → canonical=PnL-sign, source='derived'.
    """
    raw_pnl = getattr(trade, "realized_pnl", None)
    if raw_pnl is None:
        return None, None, False, "missing_pnl"
    pnl = _sdec(raw_pnl)
    if pnl is None:
        return None, None, False, "invalid_pnl"

    pnl_outcome = "win" if pnl > 0 else ("loss" if pnl < 0 else "breakeven")
    explicit    = (getattr(trade, "outcome", "") or "").lower().strip()

    if explicit in ("win", "loss", "breakeven"):
        if explicit == pnl_outcome:
            return pnl_outcome, pnl, False, "explicit"
        else:
            return pnl_outcome, pnl, True, "pnl_override"
    else:
        return pnl_outcome, pnl, False, "derived"


# ── Period / filter normalization ─────────────────────────────────────────────

def _lm_normalize_performance_period(period) -> str:
    if period and str(period) in _VALID_PERIODS:
        return str(period)
    return _DEFAULT_PERIOD


def _lm_build_paper_performance_filters(
    user_id,
    period  = "30d",
    symbol  = None,
    side    = None,
    item_id = None,
) -> dict:
    period  = _lm_normalize_performance_period(period)
    now_utc = datetime.now(timezone.utc)
    from_dt = None if period == "all" else now_utc - timedelta(days=_PERIOD_DAYS[period])

    # Symbol: trim + uppercase + length guard
    sym = None
    if symbol:
        sym_raw = str(symbol).strip().upper()
        if sym_raw and len(sym_raw) <= 30:
            sym = sym_raw

    # Side: case-insensitive lookup
    side_n   = None
    side_err = None
    if side:
        side_key = str(side).strip().lower()
        side_n   = _SIDE_NORM.get(side_key)
        if not side_n:
            side_err = "invalid_performance_filter"

    # item_id: safe integer parse — never raises
    iid          = None
    item_id_err  = None
    if item_id is not None:
        try:
            iid = int(item_id)
            if iid <= 0:
                iid         = None
                item_id_err = "must_be_positive_integer"
        except (TypeError, ValueError):
            item_id_err = "must_be_positive_integer"

    return {
        "period":   period,
        "symbol":   sym,
        "side":     side_n,
        "item_id":  iid,
        "date_from": from_dt.isoformat() if from_dt else None,
        "date_to":   now_utc.isoformat(),
        # Internal keys
        "_from_dt":     from_dt,
        "_to_dt":       now_utc,
        "_side_err":    side_err,
        "_item_id_err": item_id_err,
    }


# ── Query ─────────────────────────────────────────────────────────────────────

def _lm_query_closed_paper_trades(
    user_id,
    period  = "30d",
    symbol  = None,
    side    = None,
    item_id = None,
) -> tuple:
    """Return (trades_list, filters_dict, query_meta).

    Canonical performance timestamp: coalesce(closed_at, updated_at, created_at).
    Case-insensitive closed-status filter.
    Counts total_available before applying the row limit.
    Deduplicates by position_id (belt-and-suspenders above DB UNIQUE).
    """
    from models import LiveMonitorPaperTrade as _T

    f = _lm_build_paper_performance_filters(user_id, period, symbol, side, item_id)

    # Canonical performance timestamp SQL expression
    _perf_ts = _sa_func.coalesce(_T.closed_at, _T.updated_at, _T.created_at)

    def _base_q():
        q = _T.query.filter(
            _T.user_id == user_id,
            _sa_func.lower(_sa_func.trim(_T.status)) == "closed",
            _T.realized_pnl.isnot(None),
        )
        if f["item_id"]:  q = q.filter(_T.item_id == f["item_id"])
        if f["symbol"]:   q = q.filter(_T.symbol  == f["symbol"])
        if f["side"]:     q = q.filter(_T.side     == f["side"])
        if f["_from_dt"]:
            q = q.filter(_perf_ts >= f["_from_dt"])
            q = q.filter(_perf_ts <= f["_to_dt"])
        return q

    # Total qualifying rows (before limit) — separate COUNT query
    try:
        total_available = _base_q().count()
    except Exception:
        total_available = None

    # Count closed trades with NULL PnL in the same scope (for data quality)
    missing_pnl_db = 0
    try:
        q_null = _T.query.filter(
            _T.user_id == user_id,
            _sa_func.lower(_sa_func.trim(_T.status)) == "closed",
            _T.realized_pnl.is_(None),
        )
        if f["item_id"]:  q_null = q_null.filter(_T.item_id == f["item_id"])
        if f["symbol"]:   q_null = q_null.filter(_T.symbol  == f["symbol"])
        if f["side"]:     q_null = q_null.filter(_T.side     == f["side"])
        if f["_from_dt"]:
            q_null = q_null.filter(_perf_ts >= f["_from_dt"])
            q_null = q_null.filter(_perf_ts <= f["_to_dt"])
        missing_pnl_db = q_null.count()
    except Exception:
        missing_pnl_db = 0

    # Fetch ordered by canonical timestamp, bounded
    rows       = _base_q().order_by(_perf_ts.asc()).limit(_MAX_ROWS).all()
    rows_loaded = len(rows)
    truncated   = (total_available is not None) and (total_available > _MAX_ROWS)

    # Python-side: detect any Numeric column values that still can't parse
    invalid_pnl_c = 0
    valid_rows    = []
    for t in rows:
        if _sdec(t.realized_pnl) is None:
            invalid_pnl_c += 1
        else:
            valid_rows.append(t)
    rows = valid_rows

    # Deduplicate by position_id
    seen:   set  = set()
    deduped: list = []
    for t in rows:
        pid = getattr(t, "position_id", None)
        if pid is not None:
            if pid in seen:
                continue
            seen.add(pid)
        deduped.append(t)

    query_meta = {
        "row_limit":       _MAX_ROWS,
        "total_available": total_available,
        "rows_loaded":     rows_loaded,
        "truncated":       truncated,
        "missing_pnl_db":  missing_pnl_db,
        "invalid_pnl_c":   invalid_pnl_c,
    }

    return deduped, f, query_meta


# ── Core metrics ──────────────────────────────────────────────────────────────

def _compute_core_metrics(trades: list, extra_dq: dict = None) -> dict:
    """Compute all core performance metrics.

    Monetary aggregates use realized_pnl SIGN — never the outcome label.
    Canonical outcome is resolved via _lm_classify_paper_trade_outcome.
    """
    trade_count = win_count = loss_count = be_count = 0
    explicit_c  = derived_c = mismatch_c             = 0
    missing_ts_c = missing_rr_c = missing_dur_c = missing_pct_c = 0

    gross_profit = Decimal("0")   # sum of pnl where pnl > 0
    gross_loss   = Decimal("0")   # abs sum of pnl where pnl < 0
    direct_sum   = Decimal("0")   # direct sum of every valid pnl

    pnl_list  = []
    pct_list  = []
    rr_list   = []
    dur_list  = []
    win_pnls  = []
    loss_mags = []

    dq_extra = extra_dq or {}

    for t in trades:
        canonical, pnl, mismatch, source = _lm_classify_paper_trade_outcome(t)
        if canonical is None:
            continue  # missing / invalid PnL counted at query time

        trade_count += 1
        direct_sum  += pnl
        pnl_list.append(pnl)

        # Outcome counts (canonical, PnL-sign based)
        if canonical == "win":
            win_count += 1
        elif canonical == "loss":
            loss_count += 1
        else:
            be_count += 1

        # Source tracking
        if source == "explicit":
            explicit_c += 1
        elif source == "pnl_override":
            mismatch_c += 1
        else:
            derived_c += 1

        # Monetary: ALWAYS PnL sign, NEVER outcome label
        if pnl > 0:
            gross_profit += pnl
            win_pnls.append(pnl)
        elif pnl < 0:
            gross_loss += abs(pnl)
            loss_mags.append(abs(pnl))

        # Timestamp quality
        if _ts(t) is None:
            missing_ts_c += 1

        # Optional metrics
        pct = _sdec(getattr(t, "realized_pnl_pct", None))
        if pct is not None:
            pct_list.append(pct)
        else:
            missing_pct_c += 1

        rr = _sdec(getattr(t, "risk_reward", None))
        if rr is not None and rr > 0:
            rr_list.append(rr)
        else:
            missing_rr_c += 1

        dur = getattr(t, "duration_seconds", None)
        if dur is not None:
            try:
                dur_list.append(int(dur))
            except (TypeError, ValueError):
                missing_dur_c += 1
        else:
            missing_dur_c += 1

    base_dq = {
        "explicit_outcome_count":       explicit_c,
        "derived_outcome_count":        derived_c,
        "outcome_pnl_mismatch_count":   mismatch_c,
        "missing_pnl_count":            dq_extra.get("missing_pnl_db", 0),
        "invalid_pnl_count":            dq_extra.get("invalid_pnl_c", 0),
        "missing_timestamp_count":      missing_ts_c,
        "missing_risk_reward_count":    missing_rr_c,
        "missing_duration_count":       missing_dur_c,
        "missing_pnl_percentage_count": missing_pct_c,
    }

    if trade_count == 0:
        return {
            "trade_count": 0,
            "win_count": 0, "loss_count": 0, "breakeven_count": 0,
            "win_rate_pct": None, "loss_rate_pct": None, "breakeven_rate_pct": None,
            "gross_profit": "0", "gross_loss": "0", "net_realized_pnl": "0",
            "average_pnl_per_trade": None, "average_win": None, "average_loss": None,
            "median_pnl": None, "largest_win": None, "largest_loss": None,
            "profit_factor": None, "profit_factor_reason": None,
            "payoff_ratio": None, "expectancy_amount": None,
            "average_risk_reward": None, "median_risk_reward": None,
            "average_realized_pnl_pct": None, "median_realized_pnl_pct": None,
            "average_duration_seconds": None, "median_duration_seconds": None,
            "pnl_sum_reconciliation": None,
            "data_quality": base_dq,
        }

    tc  = Decimal(str(trade_count))
    net = gross_profit - gross_loss   # = direct_sum (verified below)
    wr  = Decimal(str(win_count))  / tc * Decimal("100")
    lr  = Decimal(str(loss_count)) / tc * Decimal("100")
    ber = Decimal(str(be_count))   / tc * Decimal("100")

    avg_pnl  = direct_sum / tc
    avg_win  = gross_profit / Decimal(str(len(win_pnls)))  if win_pnls  else None
    avg_loss = gross_loss   / Decimal(str(len(loss_mags))) if loss_mags else None
    med_pnl  = Decimal(str(_stat_median(float(p) for p in pnl_list)))

    largest_win  = max(win_pnls,  default=None)
    largest_loss = max(loss_mags, default=None)

    # Profit factor — never Infinity
    pf_reason = None
    if gross_loss > 0:
        pf = gross_profit / gross_loss
    elif gross_profit > 0:
        pf        = None
        pf_reason = "no_losses_in_sample"
    else:
        pf = None

    # Payoff ratio
    payoff = (
        (avg_win / avg_loss)
        if avg_win is not None and avg_loss is not None and avg_loss > 0
        else None
    )

    # Expectancy: (win_rate × avg_win) − (loss_rate × avg_loss)
    if avg_win is not None and avg_loss is not None:
        exp = (wr / Decimal("100") * avg_win) - (lr / Decimal("100") * avg_loss)
    else:
        exp = None

    # R:R
    avg_rr = (
        sum(rr_list, Decimal("0")) / Decimal(str(len(rr_list)))
        if rr_list else None
    )
    med_rr = (
        Decimal(str(_stat_median(float(r) for r in rr_list)))
        if rr_list else None
    )

    # Duration
    avg_dur = int(sum(dur_list) / len(dur_list)) if dur_list else None
    med_dur = int(_stat_median(dur_list))         if dur_list else None

    # PnL %
    avg_pct = (
        sum(pct_list, Decimal("0")) / Decimal(str(len(pct_list)))
        if pct_list else None
    )
    med_pct = (
        Decimal(str(_stat_median(float(p) for p in pct_list)))
        if pct_list else None
    )

    # PnL sum reconciliation: gross_profit − gross_loss must equal direct_sum
    recon_match = abs(net - direct_sum) < Decimal("0.00000001")
    recon = {
        "direct_sum":                    _ds(direct_sum),
        "gross_profit_minus_gross_loss": _ds(net),
        "matches":                       recon_match,
    }

    return {
        "trade_count":             trade_count,
        "win_count":               win_count,
        "loss_count":              loss_count,
        "breakeven_count":         be_count,
        "win_rate_pct":            _ds(wr),
        "loss_rate_pct":           _ds(lr),
        "breakeven_rate_pct":      _ds(ber),
        "gross_profit":            _ds(gross_profit),
        "gross_loss":              _ds(gross_loss),
        "net_realized_pnl":        _ds(net),
        "average_pnl_per_trade":   _ds(avg_pnl),
        "average_win":             _ds(avg_win),
        "average_loss":            _ds(avg_loss),
        "median_pnl":              _ds(med_pnl),
        "largest_win":             _ds(largest_win),
        "largest_loss":            _ds(largest_loss),
        "profit_factor":           _ds(pf),
        "profit_factor_reason":    pf_reason,
        "payoff_ratio":            _ds(payoff),
        "expectancy_amount":       _ds(exp),
        "average_risk_reward":     _ds(avg_rr),
        "median_risk_reward":      _ds(med_rr),
        "average_realized_pnl_pct": _ds(avg_pct),
        "median_realized_pnl_pct":  _ds(med_pct),
        "average_duration_seconds": avg_dur,
        "median_duration_seconds":  med_dur,
        "pnl_sum_reconciliation":   recon,
        "data_quality":             base_dq,
    }


# ── Sample quality ────────────────────────────────────────────────────────────

def _compute_sample_quality(
    metrics: dict,
    trades: list,
    query_meta: dict = None,
) -> dict:
    tc = metrics.get("trade_count", 0)
    sq = (
        "insufficient" if tc < 10  else
        "early"        if tc < 30  else
        "developing"   if tc < 100 else
        "meaningful"
    )
    dq = metrics.get("data_quality", {})
    warnings: list = []

    if tc < 10:
        warnings.append("small_sample_size")
    if tc > 0 and metrics.get("win_count",  0) == 0:
        warnings.append("no_wins_in_sample")
    if tc > 0 and metrics.get("loss_count", 0) == 0:
        warnings.append("no_losses_in_sample")
    if dq.get("outcome_pnl_mismatch_count", 0) > 0:
        warnings.append("outcome_pnl_mismatch_detected")
    if dq.get("missing_risk_reward_count",    0) > 0:
        warnings.append("missing_risk_reward_data")
    if dq.get("missing_duration_count",       0) > 0:
        warnings.append("missing_duration_data")
    if dq.get("missing_pnl_percentage_count", 0) > 0:
        warnings.append("missing_pnl_percentage_data")
    if dq.get("missing_timestamp_count",      0) > 0:
        warnings.append("missing_timestamp_data")
    if dq.get("missing_pnl_count", 0) > 0 or dq.get("invalid_pnl_count", 0) > 0:
        warnings.append("journal_data_incomplete")
    recon = metrics.get("pnl_sum_reconciliation") or {}
    if recon and not recon.get("matches", True):
        warnings.append("analytics_reconciliation_failed")
    if (query_meta or {}).get("truncated"):
        warnings.append("analytics_row_limit_reached")

    # Deduplicate, preserve order
    seen_w:    set  = set()
    deduped_w: list = []
    for w in warnings:
        if w not in seen_w:
            seen_w.add(w)
            deduped_w.append(w)

    return {
        "sample_quality":             sq,
        "sample_size":                tc,
        "minimum_recommended_trades": 30,
        "warnings":                   deduped_w,
    }


# ── Equity curve + drawdown ───────────────────────────────────────────────────

def _build_equity_raw(trades: list) -> list:
    """Build raw [{trade_id, _c}] series sorted by canonical timestamp."""
    _epoch  = datetime.min.replace(tzinfo=timezone.utc)
    ordered = sorted(trades, key=lambda t: (_ts(t) or _epoch))
    running = Decimal("0")
    raw     = []
    for t in ordered:
        pnl = _sdec(t.realized_pnl)
        if pnl is None:
            continue
        running += pnl
        ts_v = _ts(t)
        raw.append({
            "trade_id":               t.id,
            "position_id":            getattr(t, "position_id", None),
            "symbol":                 t.symbol or "",
            "side":                   getattr(t, "side", "") or "",
            "closed_at":              ts_v.isoformat() if ts_v else None,
            "realized_pnl":           _ds(pnl),
            "cumulative_realized_pnl": _ds(running),
            "_c":                     running,
        })
    return raw


def _lm_build_paper_equity_curve(trades: list) -> tuple:
    """Build realized cumulative-PnL curve. Returns (curve_points, total_valid, drawdown)."""
    raw        = _build_equity_raw(trades)
    total_valid = len(raw)
    drawdown   = _compute_drawdown(raw)

    if total_valid > _MAX_CURVE:
        raw = _downsample_curve(raw)

    clean = [{k: v for k, v in p.items() if k != "_c"} for p in raw]
    return clean, total_valid, drawdown


def _downsample_curve(points: list) -> list:
    n = len(points)
    if n <= _MAX_CURVE:
        return points
    stride = max(1, n // (_MAX_CURVE - 2))
    sel: set = {0, n - 1}
    for i in range(0, n, stride):
        sel.add(min(i, n - 1))
    for i in range(1, n - 1):
        if len(sel) >= _MAX_CURVE:
            break
        prev_c = points[i - 1]["_c"]
        curr_c = points[i]["_c"]
        next_c = points[i + 1]["_c"]
        if (curr_c >= prev_c and curr_c >= next_c) or (curr_c <= prev_c and curr_c <= next_c):
            sel.add(i)
    return [points[i] for i in sorted(sel)]


def _compute_drawdown(raw: list) -> dict:
    """Realized cumulative-PnL drawdown from an equity raw series."""
    EMPTY = {
        "max_drawdown_amount": "0",
        "max_drawdown_pct":    None,
        "drawdown_pct_reason": "no_trades",
        "peak_trade_id":       None,
        "trough_trade_id":     None,
        "recovered":           False,
    }
    if not raw:
        return EMPTY

    peak         = Decimal("0")
    max_dd       = Decimal("0")
    peak_tid     = None
    trough_tid   = None
    cur_peak_tid = None

    for p in raw:
        c = p["_c"]
        if c >= peak:
            peak         = c
            cur_peak_tid = p["trade_id"]
        else:
            dd = peak - c
            if dd > max_dd:
                max_dd     = dd
                peak_tid   = cur_peak_tid
                trough_tid = p["trade_id"]

    if max_dd == 0:
        return {
            "max_drawdown_amount": "0",
            "max_drawdown_pct":    "0",
            "drawdown_pct_reason": None,
            "peak_trade_id":       None,
            "trough_trade_id":     None,
            "recovered":           True,
        }

    # Recovery check
    recovered = False
    if trough_tid is not None:
        idx = next((i for i, p in enumerate(raw) if p["trade_id"] == trough_tid), None)
        if idx is not None:
            peak_c    = raw[idx]["_c"] + max_dd
            recovered = any(p["_c"] >= peak_c for p in raw[idx + 1:])

    return {
        "max_drawdown_amount": _ds(max_dd),
        "max_drawdown_pct":    None,
        "drawdown_pct_reason": "period_start_equity_unavailable",
        "peak_trade_id":       peak_tid,
        "trough_trade_id":     trough_tid,
        "recovered":           recovered,
    }


def _compute_drawdown_from_trades(trades: list) -> dict:
    """Convenience: build equity raw from trades and compute drawdown."""
    raw = _build_equity_raw(trades)
    return _compute_drawdown(raw)


# ── Streaks ───────────────────────────────────────────────────────────────────

def _compute_streaks(trades: list) -> dict:
    _epoch  = datetime.min.replace(tzinfo=timezone.utc)
    ordered = sorted(trades, key=lambda t: (_ts(t) or _epoch))

    outcomes = []
    for t in ordered:
        canonical, _, _, _ = _lm_classify_paper_trade_outcome(t)
        if canonical is not None:
            outcomes.append(canonical)

    max_win = max_loss = 0
    rw = rl = 0
    for o in outcomes:
        if o == "win":
            rw += 1; rl = 0
        elif o == "loss":
            rl += 1; rw = 0
        else:
            rw = rl = 0
        max_win  = max(max_win,  rw)
        max_loss = max(max_loss, rl)

    cur_win = cur_loss = 0
    for o in reversed(outcomes):
        if cur_win == 0 and cur_loss == 0:
            if o == "win":    cur_win  = 1
            elif o == "loss": cur_loss = 1
            else:             break
        elif cur_win > 0:
            if o == "win": cur_win += 1
            else:          break
        else:
            if o == "loss": cur_loss += 1
            else:           break

    return {
        "current_win_streak":  cur_win,
        "current_loss_streak": cur_loss,
        "max_win_streak":      max_win,
        "max_loss_streak":     max_loss,
    }


# ── Breakdowns ────────────────────────────────────────────────────────────────

def _lm_build_paper_performance_breakdowns(trades: list, period: str) -> dict:
    by_day = period in ("7d", "30d")

    def _nb():
        return {"w": 0, "l": 0, "be": 0, "gp": Decimal("0"), "gl": Decimal("0")}

    sym_m = {}; side_m = {}; reason_m = {}; time_m = {}

    for t in trades:
        canonical, pnl, _, _ = _lm_classify_paper_trade_outcome(t)
        if canonical is None:
            continue

        sym    = (getattr(t, "symbol",         "") or "").upper() or "UNKNOWN"
        side   = (getattr(t, "side",           "") or "").upper() or "UNKNOWN"
        reason = (getattr(t, "outcome_reason", "") or "unknown").lower()
        ts_v   = _ts(t)

        def _push(mp, key):
            b = mp.setdefault(key, _nb())
            # Win/loss counts use canonical outcome
            if canonical == "win":
                b["w"] += 1
            elif canonical == "loss":
                b["l"] += 1
            else:
                b["be"] += 1
            # Monetary: PnL sign, never outcome label
            if pnl > 0:
                b["gp"] += pnl
            elif pnl < 0:
                b["gl"] += abs(pnl)

        _push(sym_m,    sym)
        _push(side_m,   side)
        _push(reason_m, reason)

        if ts_v:
            iso = ts_v.isocalendar()
            bk  = ts_v.date().isoformat() if by_day else f"{iso[0]}-W{iso[1]:02d}"
            _push(time_m, bk)

    def _rows(mp, lbl, limit=_MAX_SYMS):
        out = []
        for k, b in mp.items():
            cnt = b["w"] + b["l"] + b["be"]
            net = b["gp"] - b["gl"]
            wr  = _ds(Decimal(str(b["w"])) / Decimal(str(cnt)) * Decimal("100")) if cnt else None
            avg = _ds(net / Decimal(str(cnt))) if cnt else None
            pf  = _ds(b["gp"] / b["gl"]) if b["gl"] > 0 else None
            out.append({
                lbl:                k,
                "trade_count":      cnt,
                "wins":             b["w"],
                "losses":           b["l"],
                "win_rate_pct":     wr,
                "net_realized_pnl": _ds(net),
                "gross_profit":     _ds(b["gp"]),
                "gross_loss":       _ds(b["gl"]),
                "average_pnl":      avg,
                "profit_factor":    pf,
            })
        out.sort(key=lambda x: (-x["trade_count"], -(float(x["net_realized_pnl"] or "0"))))
        return out[:limit]

    def _buckets(mp):
        out = []
        for k, b in mp.items():
            cnt = b["w"] + b["l"] + b["be"]
            net = b["gp"] - b["gl"]
            out.append({
                "bucket":           k,
                "trade_count":      cnt,
                "net_realized_pnl": _ds(net),
                "wins":             b["w"],
                "losses":           b["l"],
            })
        out.sort(key=lambda x: x["bucket"])
        return out

    return {
        "symbols":         _rows(sym_m,    "symbol"),
        "sides":           _rows(side_m,   "side",          limit=5),
        "outcome_reasons": _rows(reason_m, "outcome_reason"),
        "time_buckets":    _buckets(time_m),
    }


# ── Period comparison ─────────────────────────────────────────────────────────

def _compute_trend(
    cur: dict,
    pri: dict,
    cur_drawdown: dict = None,
    pri_drawdown: dict = None,
) -> str:
    if cur.get("trade_count", 0) < 5 or pri.get("trade_count", 0) < 5:
        return "insufficient_data"
    imp = det = 0

    def _cmp(key, better="higher"):
        nonlocal imp, det
        cv = _sdec(cur.get(key))
        pv = _sdec(pri.get(key))
        if cv is None or pv is None:
            return
        if better == "higher":
            if cv > pv:   imp += 1
            elif cv < pv: det += 1
        else:
            if cv < pv:   imp += 1
            elif cv > pv: det += 1

    _cmp("net_realized_pnl")
    _cmp("win_rate_pct")
    _cmp("profit_factor")
    _cmp("expectancy_amount")

    # Optional drawdown comparison (lower is better)
    if cur_drawdown and pri_drawdown:
        cv = _sdec((cur_drawdown or {}).get("max_drawdown_amount"))
        pv = _sdec((pri_drawdown or {}).get("max_drawdown_amount"))
        if cv is not None and pv is not None:
            if cv < pv:   imp += 1
            elif cv > pv: det += 1

    if imp >= 2 and det == 0: return "improving"
    if det >= 2 and imp == 0: return "deteriorating"
    return "mixed"


def _lm_compute_period_comparison(
    user_id,
    period,
    symbol,
    side_norm,
    item_id,
    current_metrics,
    current_drawdown = None,
) -> dict:
    if period == "all":
        return {"available": False, "reason": "period_all_no_comparison"}

    days        = _PERIOD_DAYS[period]
    now_utc     = datetime.now(timezone.utc)
    prior_end   = now_utc   - timedelta(days=days)
    prior_start = prior_end - timedelta(days=days)

    try:
        from models import LiveMonitorPaperTrade as _T
        _perf_ts = _sa_func.coalesce(_T.closed_at, _T.updated_at, _T.created_at)

        q = _T.query.filter(
            _T.user_id == user_id,
            _sa_func.lower(_sa_func.trim(_T.status)) == "closed",
            _T.realized_pnl.isnot(None),
            _perf_ts >= prior_start,
            _perf_ts <  prior_end,
        )
        if item_id:   q = q.filter(_T.item_id == item_id)
        if symbol:    q = q.filter(_T.symbol  == symbol)
        if side_norm: q = q.filter(_T.side     == side_norm)

        prior_rows = q.order_by(_perf_ts.asc()).limit(_MAX_ROWS).all()
    except Exception as _e:
        return {"available": False, "reason": f"query_error:{str(_e)[:80]}"}

    pri          = _compute_core_metrics(prior_rows)
    prior_dd     = _compute_drawdown_from_trades(prior_rows)
    trend        = _compute_trend(current_metrics, pri, current_drawdown, prior_dd)

    def _delta(key):
        cv = _sdec(current_metrics.get(key))
        pv = _sdec(pri.get(key))
        return _ds(cv - pv) if cv is not None and pv is not None else None

    cur_dd_amt = _sdec((current_drawdown or {}).get("max_drawdown_amount"))
    pri_dd_amt = _sdec(prior_dd.get("max_drawdown_amount"))
    dd_delta   = _ds(cur_dd_amt - pri_dd_amt) if cur_dd_amt is not None and pri_dd_amt is not None else None

    return {
        "available":  True,
        "trend":      trend,
        "current_period": {
            "trade_count":       current_metrics.get("trade_count"),
            "win_rate_pct":      current_metrics.get("win_rate_pct"),
            "net_realized_pnl":  current_metrics.get("net_realized_pnl"),
            "profit_factor":     current_metrics.get("profit_factor"),
            "expectancy_amount": current_metrics.get("expectancy_amount"),
        },
        "prior_period": {
            "trade_count":       pri.get("trade_count"),
            "win_rate_pct":      pri.get("win_rate_pct"),
            "net_realized_pnl":  pri.get("net_realized_pnl"),
            "profit_factor":     pri.get("profit_factor"),
            "expectancy_amount": pri.get("expectancy_amount"),
        },
        "deltas": {
            "trade_count_delta":         (current_metrics.get("trade_count") or 0) - (pri.get("trade_count") or 0),
            "win_rate_pct_delta":        _delta("win_rate_pct"),
            "net_pnl_delta":             _delta("net_realized_pnl"),
            "average_pnl_delta":         _delta("average_pnl_per_trade"),
            "profit_factor_delta":       _delta("profit_factor"),
            "expectancy_delta":          _delta("expectancy_amount"),
            "max_drawdown_amount_delta": dd_delta,
        },
    }


# ── Recent trades ─────────────────────────────────────────────────────────────

def _build_recent_trades(trades: list) -> list:
    _epoch = datetime.min.replace(tzinfo=timezone.utc)
    srt    = sorted(trades, key=lambda t: (_ts(t) or _epoch), reverse=True)
    out    = []
    for t in srt[:_MAX_RECENT]:
        canonical, _, _, _ = _lm_classify_paper_trade_outcome(t)
        ts_v = _ts(t)
        out.append({
            "id":               t.id,
            "position_id":      getattr(t, "position_id",   None),
            "symbol":           t.symbol or "",
            "side":             getattr(t, "side",          "") or "",
            "quantity":         getattr(t, "quantity",      None),
            "entry_price":      getattr(t, "entry_price",   None),
            "exit_price":       getattr(t, "exit_price",    None),
            "realized_pnl":     _ds(_sdec(t.realized_pnl)),
            "realized_pnl_pct": _ds(_sdec(getattr(t, "realized_pnl_pct", None))),
            "outcome":          canonical or "",
            "outcome_raw":      getattr(t, "outcome",       "") or "",
            "outcome_reason":   getattr(t, "outcome_reason","") or "",
            "risk_reward":      _ds(_sdec(getattr(t, "risk_reward", None))),
            "duration_seconds": getattr(t, "duration_seconds", None),
            "closed_at":        ts_v.isoformat() if ts_v else None,
        })
    return out


# ── Account context ───────────────────────────────────────────────────────────

def _build_account_context(user_id) -> dict:
    try:
        from live_monitor.paper_trading import _lm_get_paper_account_summary
        a = _lm_get_paper_account_summary(user_id) or {}
        return {
            "cash_balance":   a.get("cash_balance"),
            "equity":         a.get("equity"),
            "realized_pnl":   a.get("realized_pnl"),
            "unrealized_pnl": a.get("unrealized_pnl"),
            "open_orders":    a.get("open_orders"),
            "open_positions": a.get("open_positions"),
        }
    except Exception:
        return {}


# ── Main state getter ─────────────────────────────────────────────────────────

def _lm_get_paper_performance_state(
    user_id,
    period  = "30d",
    symbol  = None,
    side    = None,
    item_id = None,
) -> dict:
    """Build complete paper performance dashboard state. Read-only analytics."""
    now_iso = datetime.now(timezone.utc).isoformat()

    try:
        trades, f, qmeta = _lm_query_closed_paper_trades(
            user_id, period, symbol, side, item_id
        )
    except Exception as _e:
        return {
            "ok":          False,
            "phase":       _PHASE,
            "error":       f"query_failed:{str(_e)[:120]}",
            "guardrails":  dict(_GUARDRAILS),
            "source":      "internal_paper_performance",
            "computed_at": now_iso,
        }

    pub_f = {k: v for k, v in f.items() if not k.startswith("_")}

    # Validate filters
    if f.get("_item_id_err"):
        return {
            "ok":           False,
            "phase":        _PHASE,
            "error":        "invalid_performance_filter",
            "field_errors": {"item_id": f["_item_id_err"]},
            "filters":      pub_f,
            "guardrails":   dict(_GUARDRAILS),
            "source":       "internal_paper_performance",
            "computed_at":  now_iso,
        }
    if f.get("_side_err"):
        return {
            "ok":           False,
            "phase":        _PHASE,
            "error":        "invalid_performance_filter",
            "field_errors": {"side": f["_side_err"]},
            "filters":      pub_f,
            "guardrails":   dict(_GUARDRAILS),
            "source":       "internal_paper_performance",
            "computed_at":  now_iso,
        }

    query_section = {
        "row_limit":       qmeta["row_limit"],
        "total_available": qmeta["total_available"],
        "rows_loaded":     qmeta["rows_loaded"],
        "truncated":       qmeta["truncated"],
    }

    if not trades:
        return {
            "ok":      True,
            "phase":   _PHASE,
            "filters": pub_f,
            "query":   query_section,
            "summary": {"trade_count": 0},
            "drawdown": _compute_drawdown([]),
            "streaks":  {"current_win_streak": 0, "current_loss_streak": 0,
                         "max_win_streak": 0,  "max_loss_streak": 0},
            "sample":   {"sample_quality": "insufficient", "sample_size": 0,
                         "minimum_recommended_trades": 30,
                         "warnings": ["no_closed_paper_trades"]},
            "comparison":   {"available": False, "reason": "no_trades"},
            "account":      _build_account_context(user_id),
            "equity_curve": [], "equity_curve_total_valid": 0,
            "breakdowns":   {"symbols": [], "sides": [], "outcome_reasons": [], "time_buckets": []},
            "recent_trades": [],
            "data_quality":  {
                "explicit_outcome_count": 0, "derived_outcome_count": 0,
                "outcome_pnl_mismatch_count": 0,
                "missing_pnl_count": qmeta.get("missing_pnl_db", 0),
                "invalid_pnl_count": qmeta.get("invalid_pnl_c",  0),
                "missing_timestamp_count": 0, "missing_risk_reward_count": 0,
                "missing_duration_count": 0,  "missing_pnl_percentage_count": 0,
            },
            "guardrails":  dict(_GUARDRAILS),
            "source":      "internal_paper_performance",
            "computed_at": now_iso,
        }

    summary    = _compute_core_metrics(trades, extra_dq=qmeta)
    curve, tv, drawdown = _lm_build_paper_equity_curve(trades)
    streaks    = _compute_streaks(trades)
    sample     = _compute_sample_quality(summary, trades, query_meta=qmeta)
    breakdowns = _lm_build_paper_performance_breakdowns(trades, f["period"])
    comparison = _lm_compute_period_comparison(
        user_id, f["period"], f["symbol"], f["side"], f["item_id"],
        summary, drawdown,
    )
    recent  = _build_recent_trades(trades)
    account = _build_account_context(user_id)

    return {
        "ok":      True,
        "phase":   _PHASE,
        "filters": pub_f,
        "query":   query_section,
        "summary": summary,
        "drawdown": drawdown,
        "streaks":  streaks,
        "sample":   sample,
        "comparison": comparison,
        "account":  account,
        "equity_curve":             curve,
        "equity_curve_total_valid": tv,
        "breakdowns":               breakdowns,
        "recent_trades":            recent,
        "data_quality":             summary.get("data_quality", {}),
        "guardrails":               dict(_GUARDRAILS),
        "source":                   "internal_paper_performance",
        "computed_at":              now_iso,
    }


def _lm_build_paper_performance_summary(
    user_id, period="30d", symbol=None, side=None, item_id=None,
) -> dict:
    """Alias for _lm_get_paper_performance_state."""
    return _lm_get_paper_performance_state(user_id, period, symbol, side, item_id)
