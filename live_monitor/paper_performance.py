"""Phase 11.14: Paper Performance Dashboard — read-only analytics for Internal Paper Trading.

Query-only. No execution. No order creation. No position mutation.
No Paper Auto Gate or Risk Guard changes. No exchange calls.
can_auto_submit is ALWAYS False. auto_execution_allowed is ALWAYS False.
ai_can_execute is ALWAYS False.
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone, timedelta
from statistics import median as _stat_median

_PHASE = "phase11_14_paper_performance_dashboard"
_VALID_PERIODS   = frozenset({"7d", "30d", "90d", "365d", "all"})
_DEFAULT_PERIOD  = "30d"
_MAX_ROWS        = 5_000
_MAX_RECENT      = 50
_MAX_CURVE       = 500
_MAX_SYMS        = 20
_PERIOD_DAYS     = {"7d": 7, "30d": 30, "90d": 90, "365d": 365}

_SIDE_NORM: dict = {
    "long": "BUY",  "buy": "BUY",  "bullish": "BUY",
    "LONG": "BUY",  "BUY": "BUY",
    "short": "SELL", "sell": "SELL", "bearish": "SELL",
    "SHORT": "SELL", "SELL": "SELL",
}

_GUARDRAILS: dict = {
    "read_only":                  True,
    "paper_primary":              True,
    "can_auto_submit":            False,
    "auto_execution_allowed":     False,
    "ai_can_execute":             False,
    "live_disabled":              True,
    "testnet_strategy_validation": False,
}


# ── Decimal / string helpers ──────────────────────────────────────────────────

def _sdec(v):
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
    """Best-available close timestamp for a trade record."""
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


def _outcome_of(trade) -> str:
    """Return 'win' / 'loss' / 'breakeven' — prefer explicit field, fall back to PnL sign."""
    o = (getattr(trade, "outcome", "") or "").lower().strip()
    if o in ("win", "loss", "breakeven"):
        return o
    pnl = _sdec(trade.realized_pnl)
    if pnl is None:
        return ""
    return "win" if pnl > 0 else ("loss" if pnl < 0 else "breakeven")


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
    period   = _lm_normalize_performance_period(period)
    now_utc  = datetime.now(timezone.utc)
    from_dt  = None if period == "all" else now_utc - timedelta(days=_PERIOD_DAYS[period])

    sym      = (str(symbol).strip().upper() if symbol else None) or None
    side_n   = _SIDE_NORM.get(str(side or "").strip()) if side else None
    side_err = (f"unknown_side:{side!r}" if side and not side_n else None)

    return {
        "period":   period,
        "symbol":   sym,
        "side":     side_n,
        "item_id":  int(item_id) if item_id else None,
        "date_from": from_dt.isoformat() if from_dt else None,
        "date_to":   now_utc.isoformat(),
        # Internal keys (prefixed with _)
        "_from_dt": from_dt,
        "_to_dt":   now_utc,
        "_err":     side_err,
    }


# ── Query ─────────────────────────────────────────────────────────────────────

def _lm_query_closed_paper_trades(
    user_id,
    period  = "30d",
    symbol  = None,
    side    = None,
    item_id = None,
) -> tuple:
    """Return (trades_list, filters_dict).

    Bounded query. Deduplicates by position_id.
    """
    from models import LiveMonitorPaperTrade as _T

    f = _lm_build_paper_performance_filters(user_id, period, symbol, side, item_id)

    q = _T.query.filter(
        _T.user_id      == user_id,
        _T.status       == "closed",
        _T.realized_pnl .isnot(None),
    )
    if f["item_id"]:
        q = q.filter(_T.item_id == f["item_id"])
    if f["symbol"]:
        q = q.filter(_T.symbol == f["symbol"])
    if f["side"]:
        q = q.filter(_T.side == f["side"])
    if f["_from_dt"]:
        q = q.filter(_T.created_at >= f["_from_dt"])

    rows = q.order_by(_T.created_at.asc()).limit(_MAX_ROWS).all()

    # Python-refine by best timestamp
    if f["_from_dt"]:
        rows = [t for t in rows if (_ts(t) or f["_to_dt"]) >= f["_from_dt"]]

    # Deduplicate by position_id (DB UNIQUE constraint already prevents this,
    # but belt-and-suspenders for safety)
    seen: set = set()
    deduped   = []
    for t in rows:
        pid = getattr(t, "position_id", None)
        if pid is not None:
            if pid in seen:
                continue
            seen.add(pid)
        deduped.append(t)

    return deduped, f


# ── Core metrics ──────────────────────────────────────────────────────────────

def _compute_core_metrics(trades: list) -> dict:
    trade_count = win_count = loss_count = be_count = 0
    explicit_c = derived_c = missing_pnl_c = 0

    gross_profit = Decimal("0")
    gross_loss   = Decimal("0")

    pnl_list  = []
    pct_list  = []
    rr_list   = []
    dur_list  = []
    win_pnls  = []
    loss_mags = []  # abs value of losses

    for t in trades:
        pnl = _sdec(t.realized_pnl)
        if pnl is None:
            missing_pnl_c += 1
            continue

        trade_count += 1
        o_raw = (getattr(t, "outcome", "") or "").lower().strip()
        if o_raw in ("win", "loss", "breakeven"):
            explicit_c += 1
            o = o_raw
        else:
            derived_c += 1
            o = "win" if pnl > 0 else ("loss" if pnl < 0 else "breakeven")

        if o == "win":
            win_count    += 1
            gross_profit += pnl
            win_pnls.append(pnl)
        elif o == "loss":
            loss_count += 1
            gross_loss += abs(pnl)
            loss_mags.append(abs(pnl))
        else:
            be_count += 1

        pnl_list.append(pnl)

        pct = _sdec(getattr(t, "realized_pnl_pct", None))
        if pct is not None:
            pct_list.append(pct)

        rr = _sdec(getattr(t, "risk_reward", None))
        if rr is not None and rr > 0:
            rr_list.append(rr)

        dur = getattr(t, "duration_seconds", None)
        if dur is not None:
            try:
                dur_list.append(int(dur))
            except (TypeError, ValueError):
                pass

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
            "data_quality": {
                "explicit_outcome_count": 0, "derived_outcome_count": 0,
                "missing_pnl_count": missing_pnl_c, "invalid_pnl_count": 0,
            },
        }

    tc   = Decimal(str(trade_count))
    net  = gross_profit - gross_loss
    wr   = Decimal(str(win_count))  / tc * Decimal("100")
    lr   = Decimal(str(loss_count)) / tc * Decimal("100")
    ber  = Decimal(str(be_count))   / tc * Decimal("100")

    avg_pnl  = net / tc
    avg_win  = gross_profit / Decimal(str(win_count))  if win_count  > 0 else None
    avg_loss = gross_loss   / Decimal(str(loss_count)) if loss_count > 0 else None
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
        wr_d = wr / Decimal("100")
        lr_d = lr / Decimal("100")
        exp  = (wr_d * avg_win) - (lr_d * avg_loss)
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

    return {
        "trade_count":           trade_count,
        "win_count":             win_count,
        "loss_count":            loss_count,
        "breakeven_count":       be_count,
        "win_rate_pct":          _ds(wr),
        "loss_rate_pct":         _ds(lr),
        "breakeven_rate_pct":    _ds(ber),
        "gross_profit":          _ds(gross_profit),
        "gross_loss":            _ds(gross_loss),
        "net_realized_pnl":      _ds(net),
        "average_pnl_per_trade": _ds(avg_pnl),
        "average_win":           _ds(avg_win),
        "average_loss":          _ds(avg_loss),
        "median_pnl":            _ds(med_pnl),
        "largest_win":           _ds(largest_win),
        "largest_loss":          _ds(largest_loss),
        "profit_factor":         _ds(pf),
        "profit_factor_reason":  pf_reason,
        "payoff_ratio":          _ds(payoff),
        "expectancy_amount":     _ds(exp),
        "average_risk_reward":   _ds(avg_rr),
        "median_risk_reward":    _ds(med_rr),
        "average_realized_pnl_pct": _ds(avg_pct),
        "median_realized_pnl_pct":  _ds(med_pct),
        "average_duration_seconds": avg_dur,
        "median_duration_seconds":  med_dur,
        "data_quality": {
            "explicit_outcome_count": explicit_c,
            "derived_outcome_count":  derived_c,
            "missing_pnl_count":      missing_pnl_c,
            "invalid_pnl_count":      0,
        },
    }


# ── Sample quality ────────────────────────────────────────────────────────────

def _compute_sample_quality(metrics: dict, trades: list) -> dict:
    tc = metrics.get("trade_count", 0)
    sq = (
        "insufficient" if tc < 10  else
        "early"        if tc < 30  else
        "developing"   if tc < 100 else
        "meaningful"
    )
    warnings = []
    if tc < 10:
        warnings.append("small_sample_size")
    if tc > 0 and metrics.get("win_count", 0) == 0:
        warnings.append("no_wins_in_sample")
    if tc > 0 and metrics.get("loss_count", 0) == 0:
        warnings.append("no_losses_in_sample")
    if trades:
        if any(_sdec(getattr(t, "risk_reward",     None)) is None for t in trades):
            warnings.append("missing_risk_reward_data")
        if any(getattr(t, "duration_seconds", None)        is None for t in trades):
            warnings.append("missing_duration_data")
        if any(_sdec(getattr(t, "realized_pnl_pct", None)) is None for t in trades):
            warnings.append("missing_pnl_percentage_data")
    if metrics.get("data_quality", {}).get("missing_pnl_count", 0) > 0:
        warnings.append("journal_data_incomplete")
    return {
        "sample_quality":             sq,
        "sample_size":                tc,
        "minimum_recommended_trades": 30,
        "warnings":                   warnings,
    }


# ── Equity curve + drawdown ───────────────────────────────────────────────────

def _lm_build_paper_equity_curve(trades: list) -> tuple:
    """Build realized cumulative-PnL curve. Returns (curve_points, total_valid, drawdown)."""
    _epoch = datetime.min.replace(tzinfo=timezone.utc)

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

    total_valid = len(raw)
    drawdown    = _compute_drawdown(raw)

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
        if curr_c >= prev_c and curr_c >= next_c:
            sel.add(i)
        elif curr_c <= prev_c and curr_c <= next_c:
            sel.add(i)
    return [points[i] for i in sorted(sel)]


def _compute_drawdown(raw: list) -> dict:
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

    peak      = Decimal("0")
    max_dd    = Decimal("0")
    peak_tid  = None
    trough_tid = None
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
            peak_c = raw[idx]["_c"] + max_dd
            recovered = any(p["_c"] >= peak_c for p in raw[idx + 1:])

    return {
        "max_drawdown_amount": _ds(max_dd),
        "max_drawdown_pct":    None,
        "drawdown_pct_reason": "period_start_equity_unavailable",
        "peak_trade_id":       peak_tid,
        "trough_trade_id":     trough_tid,
        "recovered":           recovered,
    }


# ── Streaks ───────────────────────────────────────────────────────────────────

def _compute_streaks(trades: list) -> dict:
    outcomes = []
    _epoch   = datetime.min.replace(tzinfo=timezone.utc)
    ordered  = sorted(trades, key=lambda t: (_ts(t) or _epoch))
    for t in ordered:
        if _sdec(t.realized_pnl) is None:
            continue
        outcomes.append(_outcome_of(t))

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
            if o == "win":   cur_win  = 1
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
        pnl = _sdec(t.realized_pnl)
        if pnl is None:
            continue
        o   = _outcome_of(t)
        sym    = (getattr(t, "symbol",        "") or "").upper() or "UNKNOWN"
        side   = (getattr(t, "side",          "") or "").upper() or "UNKNOWN"
        reason = (getattr(t, "outcome_reason","") or "unknown").lower()
        ts_v   = _ts(t)

        def _push(mp, key):
            if key not in mp:
                mp[key] = _nb()
            b = mp[key]
            if o == "win":
                b["w"] += 1; b["gp"] += pnl
            elif o == "loss":
                b["l"] += 1; b["gl"] += abs(pnl)
            else:
                b["be"] += 1

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
            wr  = _ds(Decimal(str(b["w"])) / Decimal(str(cnt)) * Decimal("100")) if cnt > 0 else None
            avg = _ds(net / Decimal(str(cnt))) if cnt > 0 else None
            pf  = _ds(b["gp"] / b["gl"]) if b["gl"] > 0 else None
            out.append({
                lbl:              k,
                "trade_count":    cnt,
                "wins":           b["w"],
                "losses":         b["l"],
                "win_rate_pct":   wr,
                "net_realized_pnl": _ds(net),
                "average_pnl":    avg,
                "profit_factor":  pf,
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

def _compute_trend(cur: dict, pri: dict) -> str:
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

    if imp >= 2 and det == 0: return "improving"
    if det >= 2 and imp == 0: return "deteriorating"
    return "mixed"


def _lm_compute_period_comparison(
    user_id, period, symbol, side_norm, item_id, current_metrics,
) -> dict:
    if period == "all":
        return {"available": False, "reason": "period_all_no_comparison"}

    days        = _PERIOD_DAYS[period]
    now_utc     = datetime.now(timezone.utc)
    prior_end   = now_utc   - timedelta(days=days)
    prior_start = prior_end - timedelta(days=days)

    try:
        from models import LiveMonitorPaperTrade as _T
        q = _T.query.filter(
            _T.user_id      == user_id,
            _T.status       == "closed",
            _T.realized_pnl .isnot(None),
            _T.created_at   >= prior_start,
            _T.created_at   <  prior_end,
        )
        if item_id:  q = q.filter(_T.item_id == item_id)
        if symbol:   q = q.filter(_T.symbol  == symbol)
        if side_norm: q = q.filter(_T.side   == side_norm)

        prior_rows = q.order_by(_T.created_at.asc()).limit(_MAX_ROWS).all()
        prior_rows = [
            t for t in prior_rows
            if (_ts(t) or now_utc) >= prior_start and (_ts(t) or now_utc) < prior_end
        ]
    except Exception as _e:
        return {"available": False, "reason": f"query_error:{str(_e)[:80]}"}

    pri = _compute_core_metrics(prior_rows)
    trend = _compute_trend(current_metrics, pri)

    def _delta(key):
        cv = _sdec(current_metrics.get(key))
        pv = _sdec(pri.get(key))
        return _ds(cv - pv) if cv is not None and pv is not None else None

    return {
        "available":  True,
        "trend":      trend,
        "current_period": {
            "trade_count":      current_metrics.get("trade_count"),
            "win_rate_pct":     current_metrics.get("win_rate_pct"),
            "net_realized_pnl": current_metrics.get("net_realized_pnl"),
            "profit_factor":    current_metrics.get("profit_factor"),
            "expectancy_amount": current_metrics.get("expectancy_amount"),
        },
        "prior_period": {
            "trade_count":      pri.get("trade_count"),
            "win_rate_pct":     pri.get("win_rate_pct"),
            "net_realized_pnl": pri.get("net_realized_pnl"),
            "profit_factor":    pri.get("profit_factor"),
            "expectancy_amount": pri.get("expectancy_amount"),
        },
        "deltas": {
            "trade_count_delta":   (current_metrics.get("trade_count") or 0) - (pri.get("trade_count") or 0),
            "win_rate_pct_delta":  _delta("win_rate_pct"),
            "net_pnl_delta":       _delta("net_realized_pnl"),
            "average_pnl_delta":   _delta("average_pnl_per_trade"),
            "profit_factor_delta": _delta("profit_factor"),
        },
    }


# ── Recent trades ─────────────────────────────────────────────────────────────

def _build_recent_trades(trades: list) -> list:
    _epoch = datetime.min.replace(tzinfo=timezone.utc)
    srt    = sorted(trades, key=lambda t: (_ts(t) or _epoch), reverse=True)
    out    = []
    for t in srt[:_MAX_RECENT]:
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
            "outcome":          getattr(t, "outcome",       "") or "",
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
        trades, f = _lm_query_closed_paper_trades(user_id, period, symbol, side, item_id)
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

    if f.get("_err"):
        return {
            "ok":          False,
            "phase":       _PHASE,
            "error":       "invalid_performance_filter",
            "field_errors": {"side": f["_err"]},
            "filters":     pub_f,
            "guardrails":  dict(_GUARDRAILS),
            "source":      "internal_paper_performance",
            "computed_at": now_iso,
        }

    if not trades:
        return {
            "ok":      True,
            "phase":   _PHASE,
            "filters": pub_f,
            "summary": {"trade_count": 0},
            "drawdown": _compute_drawdown([]),
            "streaks":  {"current_win_streak": 0, "current_loss_streak": 0,
                         "max_win_streak": 0,  "max_loss_streak": 0},
            "sample":   {"sample_quality": "insufficient", "sample_size": 0,
                         "minimum_recommended_trades": 30,
                         "warnings": ["no_closed_paper_trades"]},
            "comparison": {"available": False, "reason": "no_trades"},
            "account":    _build_account_context(user_id),
            "equity_curve": [], "equity_curve_total_valid": 0,
            "breakdowns": {"symbols": [], "sides": [], "outcome_reasons": [], "time_buckets": []},
            "recent_trades": [],
            "data_quality": {},
            "guardrails":  dict(_GUARDRAILS),
            "source":      "internal_paper_performance",
            "computed_at": now_iso,
        }

    summary    = _compute_core_metrics(trades)
    curve, tv, drawdown = _lm_build_paper_equity_curve(trades)
    streaks    = _compute_streaks(trades)
    sample     = _compute_sample_quality(summary, trades)
    breakdowns = _lm_build_paper_performance_breakdowns(trades, f["period"])
    comparison = _lm_compute_period_comparison(
        user_id, f["period"], f["symbol"], f["side"], f["item_id"], summary,
    )
    recent  = _build_recent_trades(trades)
    account = _build_account_context(user_id)

    return {
        "ok":      True,
        "phase":   _PHASE,
        "filters": pub_f,
        "summary": summary,
        "drawdown": drawdown,
        "streaks":  streaks,
        "sample":   sample,
        "comparison": comparison,
        "account":  account,
        "equity_curve": curve,
        "equity_curve_total_valid": tv,
        "breakdowns": breakdowns,
        "recent_trades": recent,
        "data_quality": summary.get("data_quality", {}),
        "guardrails":  dict(_GUARDRAILS),
        "source":      "internal_paper_performance",
        "engine_source": "internal_paper_performance",
        "computed_at":   now_iso,
    }


def _lm_build_paper_performance_summary(
    user_id, period="30d", symbol=None, side=None, item_id=None,
) -> dict:
    """Alias for _lm_get_paper_performance_state."""
    return _lm_get_paper_performance_state(user_id, period, symbol, side, item_id)
