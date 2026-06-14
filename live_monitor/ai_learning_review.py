"""Phase 11.15 Hotfix 11.15.1: AI Learning Review Loop — strict validation.

HOTFIX PRINCIPLE: Unsafe or malformed AI output is REJECTED, not repaired.
Three-stage pipeline:
  1. _lm_parse_learning_review_response   — pure JSON decoding, no semantic repair
  2. _lm_validate_learning_review_response — strict validation against evidence
  3. _lm_sanitize_valid_learning_review   — display-text trimming only (after pass)

HARD INVARIANTS (never negotiable):
  - No execution. No orders. No position mutations. No exchange API calls.
  - No automatic strategy changes. No automatic parameter application.
  - can_auto_submit is ALWAYS False.
  - auto_execution_allowed is ALWAYS False.
  - ai_can_execute is ALWAYS False.
  - auto_apply_allowed in ANY proposal or guardrail MUST be exactly False or review is REJECTED.
  - Human review decisions are tracked but never automated.
  - No background workers. No schedulers. No setInterval. No polling.
"""
from __future__ import annotations

import json as _json
import math as _math
import re as _re
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation

from live_monitor.paper_performance import (
    _lm_build_paper_performance_filters,
    _lm_validate_paper_performance_filters,
    _lm_query_closed_paper_trades_from_filters,
    _lm_get_paper_performance_state,
    _lm_classify_paper_trade_outcome,
    _lm_normalize_performance_period,
    _sdec,
    _ds,
    _ts,
    _VALID_PERIODS,
)

_PHASE          = "phase11_15_ai_learning_review"
_PROMPT_VERSION = "11.15.1"  # bumped for schema change
_MIN_TRADES_AI  = 5          # fewer than this: no AI call, no deterministic save
_MAX_ACCEPTED_INSIGHTS = 20
_MAX_EVIDENCE_RECENT   = 10

# Sentinel — "caller did not supply human_note at all"
_NOTE_NOT_PROVIDED = object()

# ── Sample quality scale (one scale used everywhere) ─────────────────────────
_SQ_INSUFFICIENT = "insufficient"  # 0–9
_SQ_EARLY        = "early"         # 10–29
_SQ_DEVELOPING   = "developing"    # 30–99
_SQ_MEANINGFUL   = "meaningful"    # 100+


def _sample_quality(n: int) -> str:
    """Deterministic sample quality label — single canonical implementation."""
    if n < 10:  return _SQ_INSUFFICIENT
    if n < 30:  return _SQ_EARLY
    if n < 100: return _SQ_DEVELOPING
    return _SQ_MEANINGFUL


# ── Segment minimum samples ───────────────────────────────────────────────────
_SEG_MIN_DIRECTIONAL  = 5     # below: no directional segment claim at all
_SEG_MIN_SIDE_EACH    = 5     # each side needs this for imbalance comparison
_SEG_UNDERPERF_WIN_MAX = 35.0  # win rate at or below → underperformance
_SEG_OUTPERF_WIN_MIN   = 65.0  # win rate at or above → outperformance
_SEG_IMBALANCE_DIFF    = 20.0  # abs win-rate gap to flag imbalance

# ── Schema constants ──────────────────────────────────────────────────────────
_VALID_SCOPES       = frozenset({"portfolio", "symbol", "item"})
_VALID_STATUSES     = frozenset({
    "generated", "reviewed", "accepted_insight", "rejected", "archived",
})
_STATUS_TRANSITIONS: dict[str, frozenset] = {
    "generated":        frozenset({"reviewed", "accepted_insight", "rejected", "archived"}),
    "reviewed":         frozenset({"accepted_insight", "rejected", "archived"}),
    "accepted_insight": frozenset({"archived"}),
    "rejected":         frozenset({"archived"}),
    "archived":         frozenset(),
}
_VALID_OBS_CATEGORIES = frozenset({
    "symbol", "side", "setup", "risk_reward", "exit",
    "confidence", "trend", "data_quality",
})
_VALID_SEVERITIES     = frozenset({"info", "watch", "important"})
_VALID_CONFIDENCES    = frozenset({"low", "medium", "high"})
_VALID_ASSESSMENTS    = frozenset({"positive", "negative", "mixed", "insufficient_data"})
_VALID_SAMPLE_QUALITIES = frozenset({
    _SQ_INSUFFICIENT, _SQ_EARLY, _SQ_DEVELOPING, _SQ_MEANINGFUL,
})
_VALID_PROPOSAL_ACTIONS = frozenset({
    "investigate", "monitor", "collect_more_data", "compare",
    "retain_current_behavior", "future_controlled_experiment",
})
_FORBIDDEN_ACTION_WORDS = frozenset({
    "apply", "activate", "enable", "disable", "execute", "submit",
    "modify_strategy", "change_threshold", "change_risk", "arm_gate", "switch_mode",
})
_VALID_CANDIDATE_TYPES = frozenset({
    "symbol_underperformance", "symbol_outperformance",
    "side_imbalance",
    "stop_loss_concentration",
    "low_rr_capture", "strong_rr_capture",
    "confidence_not_confirmed", "confidence_supported",
    "setup_type_underperformance", "setup_type_outperformance",
    "deteriorating_recent_period", "improving_recent_period",
    "insufficient_sample",
    "data_quality_problem",
    "no_action_recommended",
})

# Required top-level keys in AI response
_REQUIRED_TOP_KEYS = frozenset({
    "review_title", "executive_summary", "overall_assessment", "confidence_level",
    "sample_assessment", "observations", "review_proposals", "what_not_to_conclude",
    "guardrails",
})
# Required guardrail keys and their exact values
_GUARDRAILS_REQUIRED: dict = {
    "read_only":             True,
    "human_review_required": True,
    "auto_apply_allowed":    False,
    "can_change_strategy":   False,
    "can_change_risk_guard": False,
    "can_arm_auto_gate":     False,
    "can_auto_submit":       False,
    "auto_execution_allowed": False,
    "ai_can_execute":        False,
}
# Observation required keys
_OBS_REQUIRED_KEYS = frozenset({
    "id", "category", "title", "statement", "evidence",
    "sample_size", "confidence", "severity", "limitations",
})
# Proposal required keys
_PROP_REQUIRED_KEYS = frozenset({
    "id", "action_type", "title", "description", "evidence_observation_ids",
    "minimum_additional_sample", "human_review_required", "auto_apply_allowed",
})
# Array limits
_OBS_MAX       = 10
_PROPOSAL_MAX  = 10
_EVIDENCE_ROWS = 10
_LIMIT_MAX     = 20
_WNTC_MAX      = 20
_OBS_ID_MAX    = 50   # max chars for obs/proposal IDs
_PROP_EOI_MAX  = 20   # max evidence_observation_ids per proposal
_SA_LIM_MAX    = 20   # max limitations in sample_assessment

# Numeric comparison tolerance — replaced by metric-specific logic; kept as fallback only
_EV_NUMERIC_REL_TOL = 0.005   # 0.5 % relative (generic fallback only)
_EV_NUMERIC_ABS_TOL = 0.01    # 0.01 absolute (generic fallback only)

# Operational language phrases that are forbidden in proposal title/description
_OPERATIONAL_PHRASES = (
    "apply this", "apply the change", "apply these", "apply now",
    "enable automatic", "enable auto-",
    "submit order", "place order", "order submission",
    "execute trade", "execute automatically", "execute now",
    "change threshold", "modify threshold",
    "change strategy", "modify strategy",
    "update pine script",
    "arm gate", "arm auto gate",
    "change risk guard", "modify risk guard",
    "switch execution mode",
    "enable testnet validation",
    "enable live trading", "disable paper trading", "start live trading",
)
# Analytical-framing prefixes that excuse an operational keyword in context
_ANALYTICAL_PREFIXES = (
    "whether", "if ", "investigate", "check", "evaluate",
    "analyze", "analyse", "review", "observe", "study", "verify",
)

# Small-sample limitation markers accepted in sample_assessment.limitations
_SMALL_SAMPLE_MARKERS = frozenset({
    "small_sample", "insufficient_sample", "limited_sample_size",
    "early_evidence_only", "below_minimum", "insufficient", "early_evidence",
    "small_segment_sample", "low_sample", "insufficient_data",
})

# Module-level guardrails for API responses
_MODULE_GUARDRAILS: dict = {
    "read_only":                   True,
    "human_review_required":       True,
    "auto_apply_allowed":          False,
    "can_change_strategy":         False,
    "can_change_risk_guard":       False,
    "can_arm_auto_gate":           False,
    "can_auto_submit":             False,
    "auto_execution_allowed":      False,
    "ai_can_execute":              False,
    "live_disabled":               True,
    "testnet_strategy_validation": False,
}


# ── Generic helpers ───────────────────────────────────────────────────────────

def _safe_pct(num, den):
    try:
        if den == 0:
            return None
        return round(float(num) / float(den) * 100, 2)
    except Exception:
        return None


def _safe_avg(values: list):
    vals = [v for v in values if v is not None]
    if not vals:
        return None
    try:
        return round(float(sum(vals)) / len(vals), 4)
    except Exception:
        return None


def _extract_json_field(trade_obj, json_col: str, field: str):
    try:
        raw = getattr(trade_obj, json_col, None)
        if raw is None:
            return None
        parsed = raw if isinstance(raw, dict) else _json.loads(raw)
        return parsed.get(field)
    except Exception:
        return None


# ── Filter building / validation ──────────────────────────────────────────────

def _lm_build_learning_review_filters(
    user_id,
    review_scope    = "portfolio",
    period          = "30d",
    symbol          = None,
    side            = None,
    item_id         = None,
    symbol_supplied = False,
) -> dict:
    """Build validated filter dict for a learning review request.

    Reuses Phase 11.14 filter architecture; adds review_scope validation.
    """
    scope_err  = None
    scope_norm = (review_scope or "portfolio").strip().lower()
    if scope_norm not in _VALID_SCOPES:
        scope_err  = "invalid_review_scope"
        scope_norm = "portfolio"

    perf_filters = _lm_build_paper_performance_filters(
        user_id,
        period          = period,
        symbol          = symbol,
        side            = side,
        item_id         = item_id,
        symbol_supplied = symbol_supplied,
    )
    perf_filters["_review_scope"]     = scope_norm
    perf_filters["_review_scope_err"] = scope_err
    return perf_filters


def _lm_validate_learning_review_filters(filters: dict) -> dict:
    """Return field_errors dict. Empty = valid."""
    errs: dict = {}
    if filters.get("_review_scope_err"):
        errs["review_scope"] = filters["_review_scope_err"]
    if filters.get("_symbol_err"):
        errs["symbol"] = filters["_symbol_err"]
    if filters.get("_side_err"):
        errs["side"] = filters["_side_err"]
    if filters.get("_item_id_err"):
        errs["item_id"] = filters["_item_id_err"]
    return errs


# ── Evidence building ─────────────────────────────────────────────────────────

def _lm_build_learning_evidence(user_id, filters: dict) -> dict:
    """Build evidence snapshot from closed paper trades + Phase 11.14 analytics.

    Raises ValueError if filters are invalid (must be pre-validated).
    Side is read from filters["side"] — the Phase 11.14 normalized key.
    """
    field_errors = _lm_validate_learning_review_filters(filters)
    if field_errors:
        raise ValueError(f"unvalidated_learning_filters:{list(field_errors.keys())}")

    trades, qmeta = _lm_query_closed_paper_trades_from_filters(user_id, filters)
    total = len(trades)

    # Task 8 fix: Phase 11.14 stores normalized side under "side" not "side_norm"
    side   = filters.get("side")
    symbol = filters.get("symbol")
    period = filters.get("period", "30d")
    iid    = filters.get("item_id")

    sq = _sample_quality(total)

    evidence: dict = {
        "sample_size":         total,
        "sample_quality":      sq,
        "query_meta":          qmeta,
        "period":              period,
        "symbol":              symbol,
        "side":                side,        # corrected key
        "item_id":             iid,
        "review_scope":        filters.get("_review_scope", "portfolio"),
        "performance_summary": {},
        "segments":            {},
        "execution_quality":   {},
        "recent_trades":       [],
        "data_quality":        {},
        "warnings":            [],
    }

    if total == 0:
        evidence["warnings"].append("no_closed_trades_in_period")
        return evidence

    if qmeta.get("truncated"):
        evidence["warnings"].append("analytics_row_limit_reached")

    # Task 6: Phase 11.14 trusted performance summary (separate query but trusted formulas)
    try:
        perf_state = _lm_get_paper_performance_state(
            user_id,
            period          = period,
            symbol          = symbol,
            side            = side,
            item_id         = iid,
            symbol_supplied = bool(symbol),
        )
        if perf_state.get("ok"):
            ps    = perf_state.get("summary") or {}
            dd    = perf_state.get("drawdown") or {}
            comp  = perf_state.get("comparison") or {}
            dq    = perf_state.get("data_quality") or {}
            evidence["performance_summary"] = {
                "trade_count":          ps.get("trade_count", 0),
                "win_count":            ps.get("win_count", 0),
                "loss_count":           ps.get("loss_count", 0),
                "breakeven_count":      ps.get("breakeven_count", 0),
                "win_rate_pct":         ps.get("win_rate_pct"),
                "net_realized_pnl":     ps.get("net_realized_pnl"),
                "gross_profit":         ps.get("gross_profit"),
                "gross_loss":           ps.get("gross_loss"),
                "profit_factor":        ps.get("profit_factor"),
                "expectancy_amount":    ps.get("expectancy_amount"),
                "average_win":          ps.get("average_win"),
                "average_loss":         ps.get("average_loss"),
                "payoff_ratio":         ps.get("payoff_ratio"),
                "average_risk_reward":  ps.get("average_risk_reward"),
                "max_drawdown_amount":  dd.get("max_drawdown_amount"),
                "max_drawdown_pct":     dd.get("max_drawdown_pct"),
                "recent_trend":         comp.get("trend", "insufficient_data"),
                "trend_reason":         comp.get("trend_reason"),
                "truncated":            bool(qmeta.get("truncated", False)),
            }
            evidence["data_quality"] = dq
    except Exception:
        evidence["warnings"].append("performance_summary_unavailable")

    evidence["segments"]        = _lm_build_learning_segments(trades)
    evidence["execution_quality"] = _lm_build_execution_quality(trades)
    evidence["recent_trades"]   = _build_recent_trade_summaries(trades, _MAX_EVIDENCE_RECENT)

    return evidence


# ── Segment builder ───────────────────────────────────────────────────────────

def _lm_build_learning_segments(trades: list) -> dict:
    """Per-dimension breakdown. Win/loss/breakeven from canonical outcome."""
    by_symbol: dict = {}
    by_side:   dict = {}
    by_reason: dict = {}
    by_conf:   dict = {}
    by_setup:  dict = {}
    by_entry:  dict = {}

    for t in trades:
        outcome, pnl, _, _ = _lm_classify_paper_trade_outcome(t)
        pnl_f  = float(pnl) if pnl is not None else None
        sym    = (getattr(t, "symbol", None) or "unknown").upper()
        side_v = (getattr(t, "side",   None) or "unknown").upper()
        reason = (getattr(t, "outcome_reason", None) or "").lower().strip() or "unknown"
        conf_b = _extract_json_field(t, "ai_decision_json",    "confidence_bucket") or "unknown"
        setup  = _extract_json_field(t, "entry_snapshot_json", "setup_type")         or "unknown"
        entry  = _extract_json_field(t, "entry_snapshot_json", "entry_mode")         or "unknown"

        for seg_d, key in [
            (by_symbol, sym), (by_side, side_v), (by_reason, reason),
            (by_conf, conf_b), (by_setup, setup), (by_entry, entry),
        ]:
            if key not in seg_d:
                seg_d[key] = {"count": 0, "wins": 0, "losses": 0, "breakevenS": 0,
                               "pnl_sum": Decimal(0)}
            seg = seg_d[key]
            seg["count"] += 1
            if outcome == "win":
                seg["wins"] += 1
                seg["pnl_sum"] += pnl if pnl else Decimal(0)
            elif outcome == "loss":
                seg["losses"] += 1
                seg["pnl_sum"] += pnl if pnl else Decimal(0)
            elif outcome == "breakeven":
                seg["breakevenS"] += 1

    def _finalize(d: dict) -> list:
        out = []
        for lbl, s in d.items():
            total_s = s["count"]
            out.append({
                "label":      lbl,
                "count":      total_s,
                "wins":       s["wins"],
                "losses":     s["losses"],
                "breakevens": s["breakevenS"],
                "win_rate":   _safe_pct(s["wins"], total_s),
                "net_pnl":    _ds(s["pnl_sum"]),
            })
        out.sort(key=lambda x: -x["count"])
        return out

    return {
        "by_symbol":          _finalize(by_symbol),
        "by_side":            _finalize(by_side),
        "by_outcome_reason":  _finalize(by_reason),
        "by_confidence":      _finalize(by_conf),
        "by_setup_type":      _finalize(by_setup),
        "by_entry_mode":      _finalize(by_entry),
    }


# ── Execution quality ─────────────────────────────────────────────────────────

def _lm_build_execution_quality(trades: list) -> dict:
    planned_rrs: list = []
    realized_rrs: list = []
    rr_captures: list = []
    tp_exit = sl_exit = manual_exit = other_exit = 0

    for t in trades:
        planned = _sdec(getattr(t, "risk_reward", None))
        if planned is not None:
            planned_rrs.append(float(planned))

        realized = _extract_json_field(t, "ai_post_trade_review_json", "realized_rr")
        if realized is None:
            realized = _extract_json_field(t, "execution_intent_json", "realized_rr")
        d_rr = _sdec(realized)
        if d_rr is not None:
            realized_rrs.append(float(d_rr))

        if planned and d_rr is not None and planned > 0:
            rr_captures.append(float(d_rr / planned))

        reason = (getattr(t, "outcome_reason", None) or "").lower()
        if "tp" in reason or "take_profit" in reason or "take profit" in reason:
            tp_exit += 1
        elif "sl" in reason or "stop_loss" in reason or "stop loss" in reason:
            sl_exit += 1
        elif "manual" in reason:
            manual_exit += 1
        else:
            other_exit += 1

    n = len(trades)
    return {
        "avg_planned_rr":    _safe_avg(planned_rrs),
        "avg_realized_rr":   _safe_avg(realized_rrs),
        "avg_rr_capture":    _safe_avg(rr_captures),
        "tp_exit_count":     tp_exit,
        "sl_exit_count":     sl_exit,
        "manual_exit_count": manual_exit,
        "other_exit_count":  other_exit,
        "tp_pct":    _safe_pct(tp_exit, n),
        "sl_pct":    _safe_pct(sl_exit, n),
        "manual_pct": _safe_pct(manual_exit, n),
    }


def _build_recent_trade_summaries(trades: list, limit: int = 10) -> list:
    recent = sorted(
        trades,
        key=lambda t: (_ts(t) or datetime.min.replace(tzinfo=timezone.utc)),
        reverse=True,
    )[:limit]
    out = []
    for t in recent:
        outcome, pnl, _, _ = _lm_classify_paper_trade_outcome(t)
        ts = _ts(t)
        out.append({
            "symbol":         getattr(t, "symbol", None),
            "side":           getattr(t, "side", None),
            "outcome":        outcome,
            "realized_pnl":   _ds(pnl),
            "outcome_reason": getattr(t, "outcome_reason", None),
            "closed_at":      ts.isoformat() if ts else None,
        })
    return out


# ── Evidence metric allowlist ─────────────────────────────────────────────────

def _lm_build_evidence_metric_allowlist(evidence: dict) -> dict:
    """Build deterministic metric allowlist for AI evidence-row validation.

    Returns dict: metric_id -> value (None = metric exists but value unknown).
    AI evidence rows must only reference metric IDs present in this allowlist.
    """
    metrics: dict = {}

    def _m(key: str, val):
        if val is not None:
            metrics[key] = val

    ps = evidence.get("performance_summary") or {}
    _m("performance.trade_count",       ps.get("trade_count"))
    _m("performance.win_count",         ps.get("win_count"))
    _m("performance.loss_count",        ps.get("loss_count"))
    _m("performance.breakeven_count",   ps.get("breakeven_count"))
    _m("performance.win_rate_pct",      ps.get("win_rate_pct"))
    _m("performance.net_realized_pnl",  ps.get("net_realized_pnl"))
    _m("performance.gross_profit",      ps.get("gross_profit"))
    _m("performance.gross_loss",        ps.get("gross_loss"))
    _m("performance.profit_factor",     ps.get("profit_factor"))
    _m("performance.expectancy_amount", ps.get("expectancy_amount"))
    _m("performance.average_win",       ps.get("average_win"))
    _m("performance.average_loss",      ps.get("average_loss"))
    _m("performance.average_risk_reward",ps.get("average_risk_reward"))
    _m("performance.max_drawdown_amount",ps.get("max_drawdown_amount"))
    _m("performance.recent_trend",      ps.get("recent_trend"))
    _m("performance.truncated",         ps.get("truncated"))
    _m("performance.sample_size",       evidence.get("sample_size"))
    _m("performance.sample_quality",    evidence.get("sample_quality"))

    eq = evidence.get("execution_quality") or {}
    _m("execution.avg_planned_rr",  eq.get("avg_planned_rr"))
    _m("execution.avg_realized_rr", eq.get("avg_realized_rr"))
    _m("execution.avg_rr_capture",  eq.get("avg_rr_capture"))
    _m("execution.tp_pct",          eq.get("tp_pct"))
    _m("execution.sl_pct",          eq.get("sl_pct"))
    _m("execution.manual_pct",      eq.get("manual_pct"))
    _m("execution.tp_count",        eq.get("tp_exit_count"))
    _m("execution.sl_count",        eq.get("sl_exit_count"))
    _m("execution.manual_count",    eq.get("manual_exit_count"))

    segs = evidence.get("segments") or {}
    _dim_map = [
        ("by_symbol",       "symbol"),
        ("by_side",         "side"),
        ("by_setup_type",   "setup"),
        ("by_confidence",   "confidence"),
        ("by_outcome_reason","reason"),
        ("by_entry_mode",   "entry"),
    ]
    for dim_key, dim_label in _dim_map:
        for seg in segs.get(dim_key) or []:
            lbl = seg.get("label") or ""
            if not lbl:
                continue
            pfx = f"segment.{dim_label}.{lbl}"
            _m(f"{pfx}.trade_count", seg.get("count"))
            _m(f"{pfx}.win_rate_pct", seg.get("win_rate"))
            _m(f"{pfx}.net_pnl",     seg.get("net_pnl"))
            _m(f"{pfx}.wins",        seg.get("wins"))
            _m(f"{pfx}.losses",      seg.get("losses"))
            _m(f"{pfx}.breakevens",  seg.get("breakevens"))

    return metrics


def _metric_kind(metric_id: str) -> str:
    """Classify a metric ID into a comparison kind for tolerance selection."""
    m = metric_id.lower()
    if (m.endswith(".trade_count") or m.endswith(".win_count") or m.endswith(".loss_count")
            or m.endswith(".breakeven_count") or m.endswith(".wins") or m.endswith(".losses")
            or m.endswith(".breakevens") or m.endswith("_count")):
        return "count"
    if m.endswith("_pct") or ".win_rate" in m:
        return "pct"
    if m.endswith("_rr") or m.endswith("_capture") or ".profit_factor" in m or "payoff_ratio" in m:
        return "ratio"
    if (m.endswith("_pnl") or "gross_" in m or "expectancy" in m
            or "drawdown" in m or m.endswith(".average_win") or m.endswith(".average_loss")):
        return "monetary"
    return "generic"


def _lm_compare_evidence_metric(metric_id: str, trusted_val, ai_val) -> bool:
    """Return True if AI value matches trusted value within metric-specific tolerance.

    Task 7: metric-specific tolerances.
    - count: exact int equality
    - pct: abs <= 0.1
    - ratio: abs <= 0.01
    - monetary: Decimal abs <= 0.01
    - generic fallback: max(0.5% relative, 0.01 absolute)
    """
    if trusted_val is None:
        return True
    try:
        float(str(trusted_val).replace(",", ""))
    except (TypeError, ValueError):
        return str(trusted_val).strip().lower() == str(ai_val).strip().lower()
    try:
        e = float(str(trusted_val).replace(",", ""))
        a = float(str(ai_val).replace(",", ""))
    except (TypeError, ValueError):
        return False
    abs_diff = abs(e - a)
    kind = _metric_kind(metric_id)
    if kind == "count":
        return int(round(e)) == int(round(a))
    if kind == "pct":
        return abs_diff <= 0.1
    if kind == "ratio":
        return abs_diff <= 0.01
    if kind == "monetary":
        try:
            e_d = Decimal(str(trusted_val))
            a_d = Decimal(str(ai_val))
            return abs(e_d - a_d) <= Decimal("0.01")
        except (InvalidOperation, TypeError):
            return abs_diff <= 0.01
    tol = max(abs(e) * _EV_NUMERIC_REL_TOL, _EV_NUMERIC_ABS_TOL)
    return abs_diff <= tol


def _has_operational_language(text: str) -> bool:
    """Return True if text contains an operational command phrase not excused by analytical framing.

    Task 10: checks _OPERATIONAL_PHRASES with _ANALYTICAL_PREFIXES exclusion.
    """
    if not isinstance(text, str):
        return False
    t = text.lower()
    for phrase in _OPERATIONAL_PHRASES:
        idx = t.find(phrase)
        if idx == -1:
            continue
        prefix_window = t[max(0, idx - 30):idx]
        if any(pref in prefix_window for pref in _ANALYTICAL_PREFIXES):
            continue
        return True
    return False


# ── Deterministic candidates ──────────────────────────────────────────────────

def _lm_build_learning_observation_candidates(evidence: dict) -> list:
    """Build deterministic pre-AI observation candidates.

    Task 9 enforcement:
     - Symbol/setup claims require >= _SEG_MIN_DIRECTIONAL trades in that segment.
     - Side imbalance requires >= _SEG_MIN_SIDE_EACH trades on EACH side.
     - 5–9 trades → low confidence + limitation "small_segment_sample".

    Task 10: Full candidate schema with candidate_id, evidence_metric_ids, etc.
    """
    candidates: list = []
    total  = evidence.get("sample_size", 0)
    segs   = evidence.get("segments") or {}
    eq     = evidence.get("execution_quality") or {}
    ps     = evidence.get("performance_summary") or {}
    warns  = evidence.get("warnings") or []
    sq     = evidence.get("sample_quality", _SQ_INSUFFICIENT)

    cid = [0]  # mutable counter

    def _next_id() -> str:
        cid[0] += 1
        return f"candidate_{cid[0]}"

    # Insufficient sample
    if total < _MIN_TRADES_AI:
        candidates.append({
            "candidate_id":        _next_id(),
            "candidate_type":      "insufficient_sample",
            "category":            "data_quality",
            "segment_type":        "portfolio",
            "segment_value":       None,
            "evidence_metric_ids": ["performance.trade_count", "performance.sample_quality"],
            "sample_size":         total,
            "sample_quality":      sq,
            "severity":            "important",
            "confidence":          "low",
            "limitations":         ["below_minimum_generation_gate"],
        })
        return candidates

    # Data quality problems
    if warns:
        cands_warns = []
        for w in warns:
            if "truncated" in w or "limit" in w:
                cands_warns.append(w)
        if cands_warns:
            candidates.append({
                "candidate_id":        _next_id(),
                "candidate_type":      "data_quality_problem",
                "category":            "data_quality",
                "segment_type":        "portfolio",
                "segment_value":       None,
                "evidence_metric_ids": ["performance.truncated"],
                "sample_size":         total,
                "sample_quality":      sq,
                "severity":            "watch",
                "confidence":          "low",
                "limitations":         cands_warns,
            })

    # Performance trend
    trend = ps.get("recent_trend")
    if trend == "deteriorating" and total >= _SEG_MIN_DIRECTIONAL:
        candidates.append({
            "candidate_id":        _next_id(),
            "candidate_type":      "deteriorating_recent_period",
            "category":            "trend",
            "segment_type":        "portfolio",
            "segment_value":       None,
            "evidence_metric_ids": ["performance.recent_trend", "performance.trade_count"],
            "sample_size":         total,
            "sample_quality":      sq,
            "severity":            "watch",
            "confidence":          "low" if total < 10 else "medium",
            "limitations":         ["trend_comparison_requires_prior_period"],
        })
    elif trend == "improving" and total >= _SEG_MIN_DIRECTIONAL:
        candidates.append({
            "candidate_id":        _next_id(),
            "candidate_type":      "improving_recent_period",
            "category":            "trend",
            "segment_type":        "portfolio",
            "segment_value":       None,
            "evidence_metric_ids": ["performance.recent_trend", "performance.trade_count"],
            "sample_size":         total,
            "sample_quality":      sq,
            "severity":            "info",
            "confidence":          "low" if total < 10 else "medium",
            "limitations":         ["trend_comparison_requires_prior_period"],
        })

    # Symbol under/outperformance
    for seg in segs.get("by_symbol") or []:
        n_seg = seg.get("count", 0)
        wr    = seg.get("win_rate")
        if n_seg < _SEG_MIN_DIRECTIONAL or wr is None:
            continue
        lbl  = seg.get("label", "unknown")
        pfx  = f"segment.symbol.{lbl}"
        lims: list = []
        if n_seg < 10:
            lims.append("small_segment_sample")
        sq_seg = _sample_quality(n_seg)
        conf = "low" if n_seg < 10 else ("medium" if n_seg < 30 else "medium")

        if wr <= _SEG_UNDERPERF_WIN_MAX:
            candidates.append({
                "candidate_id":        _next_id(),
                "candidate_type":      "symbol_underperformance",
                "category":            "symbol",
                "segment_type":        "symbol",
                "segment_value":       lbl,
                "evidence_metric_ids": [f"{pfx}.trade_count", f"{pfx}.win_rate_pct", f"{pfx}.net_pnl"],
                "sample_size":         n_seg,
                "sample_quality":      sq_seg,
                "severity":            "watch" if n_seg >= 10 else "info",
                "confidence":          conf,
                "limitations":         lims,
            })
        elif wr >= _SEG_OUTPERF_WIN_MIN:
            candidates.append({
                "candidate_id":        _next_id(),
                "candidate_type":      "symbol_outperformance",
                "category":            "symbol",
                "segment_type":        "symbol",
                "segment_value":       lbl,
                "evidence_metric_ids": [f"{pfx}.trade_count", f"{pfx}.win_rate_pct", f"{pfx}.net_pnl"],
                "sample_size":         n_seg,
                "sample_quality":      sq_seg,
                "severity":            "info",
                "confidence":          conf,
                "limitations":         lims,
            })

    # Side imbalance (requires >= _SEG_MIN_SIDE_EACH on each side)
    by_side_map = {s.get("label", ""): s for s in (segs.get("by_side") or [])}
    buy_seg  = by_side_map.get("BUY")
    sell_seg = by_side_map.get("SELL")
    buy_n    = (buy_seg  or {}).get("count", 0)
    sell_n   = (sell_seg or {}).get("count", 0)
    if buy_n >= _SEG_MIN_SIDE_EACH and sell_n >= _SEG_MIN_SIDE_EACH:
        buy_wr  = (buy_seg  or {}).get("win_rate") or 0
        sell_wr = (sell_seg or {}).get("win_rate") or 0
        if abs(buy_wr - sell_wr) >= _SEG_IMBALANCE_DIFF:
            lims_side = []
            if min(buy_n, sell_n) < 10:
                lims_side.append("small_segment_sample")
            sq_side = _sample_quality(min(buy_n, sell_n))
            candidates.append({
                "candidate_id":        _next_id(),
                "candidate_type":      "side_imbalance",
                "category":            "side",
                "segment_type":        "side",
                "segment_value":       "BUY_vs_SELL",
                "evidence_metric_ids": [
                    "segment.side.BUY.trade_count",  "segment.side.BUY.win_rate_pct",
                    "segment.side.SELL.trade_count", "segment.side.SELL.win_rate_pct",
                ],
                "sample_size":         buy_n + sell_n,
                "sample_quality":      sq_side,
                "severity":            "watch",
                "confidence":          "low" if min(buy_n, sell_n) < 10 else "medium",
                "limitations":         lims_side,
            })

    # Stop-loss concentration
    sl_pct = eq.get("sl_pct") or 0
    n_sl   = eq.get("sl_exit_count") or 0
    if sl_pct > 60 and total >= _SEG_MIN_DIRECTIONAL:
        candidates.append({
            "candidate_id":        _next_id(),
            "candidate_type":      "stop_loss_concentration",
            "category":            "exit",
            "segment_type":        "portfolio",
            "segment_value":       None,
            "evidence_metric_ids": ["execution.sl_pct", "execution.sl_count", "performance.trade_count"],
            "sample_size":         total,
            "sample_quality":      sq,
            "severity":            "watch",
            "confidence":          "low" if total < 10 else "medium",
            "limitations":         [] if total >= 10 else ["small_segment_sample"],
        })

    # RR capture
    avg_cap = eq.get("avg_rr_capture")
    if avg_cap is not None and total >= _SEG_MIN_DIRECTIONAL:
        if avg_cap < 0.5:
            candidates.append({
                "candidate_id":        _next_id(),
                "candidate_type":      "low_rr_capture",
                "category":            "risk_reward",
                "segment_type":        "portfolio",
                "segment_value":       None,
                "evidence_metric_ids": ["execution.avg_rr_capture", "execution.avg_planned_rr", "execution.avg_realized_rr"],
                "sample_size":         total,
                "sample_quality":      sq,
                "severity":            "watch",
                "confidence":          "low" if total < 10 else "medium",
                "limitations":         [] if total >= 10 else ["small_segment_sample"],
            })
        elif avg_cap > 1.1:
            candidates.append({
                "candidate_id":        _next_id(),
                "candidate_type":      "strong_rr_capture",
                "category":            "risk_reward",
                "segment_type":        "portfolio",
                "segment_value":       None,
                "evidence_metric_ids": ["execution.avg_rr_capture", "execution.avg_planned_rr"],
                "sample_size":         total,
                "sample_quality":      sq,
                "severity":            "info",
                "confidence":          "low",
                "limitations":         ["low_realized_rr_data_availability"],
            })

    # Confidence bucket signals
    for seg in segs.get("by_confidence") or []:
        n_seg = seg.get("count", 0)
        wr    = seg.get("win_rate")
        lbl   = seg.get("label", "unknown")
        if n_seg < _SEG_MIN_DIRECTIONAL or wr is None or lbl == "unknown":
            continue
        pfx  = f"segment.confidence.{lbl}"
        lims = ["small_segment_sample"] if n_seg < 10 else []
        sq_seg = _sample_quality(n_seg)
        if wr >= _SEG_OUTPERF_WIN_MIN:
            candidates.append({
                "candidate_id":        _next_id(),
                "candidate_type":      "confidence_supported",
                "category":            "confidence",
                "segment_type":        "confidence_bucket",
                "segment_value":       lbl,
                "evidence_metric_ids": [f"{pfx}.trade_count", f"{pfx}.win_rate_pct"],
                "sample_size":         n_seg,
                "sample_quality":      sq_seg,
                "severity":            "info",
                "confidence":          "low" if n_seg < 10 else "medium",
                "limitations":         lims,
            })
        elif wr <= _SEG_UNDERPERF_WIN_MAX:
            candidates.append({
                "candidate_id":        _next_id(),
                "candidate_type":      "confidence_not_confirmed",
                "category":            "confidence",
                "segment_type":        "confidence_bucket",
                "segment_value":       lbl,
                "evidence_metric_ids": [f"{pfx}.trade_count", f"{pfx}.win_rate_pct"],
                "sample_size":         n_seg,
                "sample_quality":      sq_seg,
                "severity":            "watch",
                "confidence":          "low" if n_seg < 10 else "medium",
                "limitations":         lims,
            })

    # Setup type under/outperformance
    for seg in segs.get("by_setup_type") or []:
        n_seg = seg.get("count", 0)
        wr    = seg.get("win_rate")
        lbl   = seg.get("label", "unknown")
        if n_seg < _SEG_MIN_DIRECTIONAL or wr is None or lbl == "unknown":
            continue
        pfx  = f"segment.setup.{lbl}"
        lims = ["small_segment_sample"] if n_seg < 10 else []
        sq_seg = _sample_quality(n_seg)
        if wr <= _SEG_UNDERPERF_WIN_MAX:
            candidates.append({
                "candidate_id":        _next_id(),
                "candidate_type":      "setup_type_underperformance",
                "category":            "setup",
                "segment_type":        "setup_type",
                "segment_value":       lbl,
                "evidence_metric_ids": [f"{pfx}.trade_count", f"{pfx}.win_rate_pct"],
                "sample_size":         n_seg,
                "sample_quality":      sq_seg,
                "severity":            "watch",
                "confidence":          "low" if n_seg < 10 else "medium",
                "limitations":         lims,
            })
        elif wr >= _SEG_OUTPERF_WIN_MIN:
            candidates.append({
                "candidate_id":        _next_id(),
                "candidate_type":      "setup_type_outperformance",
                "category":            "setup",
                "segment_type":        "setup_type",
                "segment_value":       lbl,
                "evidence_metric_ids": [f"{pfx}.trade_count", f"{pfx}.win_rate_pct"],
                "sample_size":         n_seg,
                "sample_quality":      sq_seg,
                "severity":            "info",
                "confidence":          "low" if n_seg < 10 else "medium",
                "limitations":         lims,
            })

    # No candidates found
    if not candidates:
        candidates.append({
            "candidate_id":        _next_id(),
            "candidate_type":      "no_action_recommended",
            "category":            "data_quality",
            "segment_type":        "portfolio",
            "segment_value":       None,
            "evidence_metric_ids": ["performance.trade_count", "performance.win_rate_pct"],
            "sample_size":         total,
            "sample_quality":      sq,
            "severity":            "info",
            "confidence":          "low",
            "limitations":         [],
        })

    return candidates


# ── Prompt builder ────────────────────────────────────────────────────────────

def _lm_build_learning_review_prompt(evidence: dict, candidates: list) -> str:
    """Build the bounded, structured prompt sent to the AI provider.

    The prompt includes the full evidence metric allowlist so AI can only
    reference metrics we actually have. The strict JSON schema is specified.
    """
    period  = evidence.get("period", "30d")
    scope   = evidence.get("review_scope", "portfolio")
    sym     = evidence.get("symbol")
    side    = evidence.get("side")
    total   = evidence.get("sample_size", 0)
    sq      = evidence.get("sample_quality", _SQ_INSUFFICIENT)
    ps      = evidence.get("performance_summary") or {}
    eq      = evidence.get("execution_quality") or {}
    segs    = evidence.get("segments") or {}
    warns   = evidence.get("warnings") or []

    sym_label  = f" for {sym}"    if sym  else ""
    side_label = f" ({side} side)" if side else ""

    cand_json = _json.dumps(candidates[:8], separators=(",", ":"))

    # Compact performance summary for prompt
    ps_compact = {
        "trade_count":   ps.get("trade_count"),
        "win_rate_pct":  ps.get("win_rate_pct"),
        "net_pnl":       ps.get("net_realized_pnl"),
        "profit_factor": ps.get("profit_factor"),
        "expectancy":    ps.get("expectancy_amount"),
        "max_drawdown":  ps.get("max_drawdown_amount"),
        "recent_trend":  ps.get("recent_trend", "insufficient_data"),
        "truncated":     ps.get("truncated", False),
    }
    ps_json = _json.dumps(ps_compact, separators=(",", ":"))

    eq_json = _json.dumps({
        "avg_planned_rr":  eq.get("avg_planned_rr"),
        "avg_realized_rr": eq.get("avg_realized_rr"),
        "avg_rr_capture":  eq.get("avg_rr_capture"),
        "tp_pct":  eq.get("tp_pct"),
        "sl_pct":  eq.get("sl_pct"),
        "manual_pct": eq.get("manual_pct"),
    }, separators=(",", ":"))

    segs_compact = {
        k: (v[:5] if isinstance(v, list) else v)
        for k, v in segs.items()
    }
    segs_json = _json.dumps(segs_compact, separators=(",", ":"))

    warn_note = ""
    if warns:
        warn_note = f"\nData warnings present: {', '.join(warns[:5])}. " \
                    f"You MUST add at least one entry to what_not_to_conclude."

    # 5-9 trade constraint
    low_sample_note = ""
    if total < 10:
        low_sample_note = (
            "\nSMALL SAMPLE CONSTRAINT: Fewer than 10 trades. "
            "confidence_level MUST be 'low'. overall_assessment MUST be 'insufficient_data'. "
            "All proposals must be 'collect_more_data' or 'monitor' only."
        )

    prompt = f"""You are an objective paper-trade learning analysis assistant.
Analyse the evidence below and return a structured JSON learning review.

Scope: {scope}{sym_label}{side_label} | Period: {period} | Trades: {total} | Quality: {sq}{warn_note}{low_sample_note}

Performance summary: {ps_json}
Execution quality: {eq_json}
Segment summary (top-5 each): {segs_json}
Deterministic candidates: {cand_json}

CRITICAL GUARDRAILS — these override everything:
1. auto_apply_allowed MUST be exactly false in EVERY proposal and in guardrails.
2. human_review_required MUST be exactly true in EVERY proposal and in guardrails.
3. Do NOT recommend: strategy changes, threshold changes, Pine Script edits,
   Risk Guard changes, Auto Gate arming, execution mode changes, order submission.
4. Allowed proposal action_types ONLY: investigate, monitor, collect_more_data,
   compare, retain_current_behavior, future_controlled_experiment.
5. Every observation must reference only metric IDs from the supplied evidence.
6. Do not fabricate symbols, segments or numbers not present in the evidence.
7. Every proposal must reference at least one valid observation ID.
8. You must provide what_not_to_conclude (minimum 1 entry for small/truncated data).

Return valid JSON ONLY — no markdown fences, no prose outside JSON:
{{
  "review_title": "short title (max 80 chars)",
  "executive_summary": "2-3 sentence summary",
  "overall_assessment": "positive|negative|mixed|insufficient_data",
  "confidence_level": "low|medium|high",
  "sample_assessment": {{
    "sample_size": {total},
    "sample_quality": "{sq}",
    "limitations": ["list of data quality limitations"]
  }},
  "observations": [
    {{
      "id": "obs_1",
      "category": "symbol|side|setup|risk_reward|exit|confidence|trend|data_quality",
      "title": "short title",
      "statement": "1-2 sentences describing the pattern",
      "evidence": [
        {{"metric": "performance.win_rate_pct", "value": 45.5, "comparison": "below 50% threshold"}}
      ],
      "sample_size": <integer from evidence>,
      "confidence": "low|medium|high",
      "severity": "info|watch|important",
      "limitations": [],
      "auto_apply_allowed": false
    }}
  ],
  "review_proposals": [
    {{
      "id": "proposal_1",
      "action_type": "investigate|monitor|collect_more_data|compare|retain_current_behavior|future_controlled_experiment",
      "title": "proposal title",
      "description": "what to investigate and why",
      "evidence_observation_ids": ["obs_1"],
      "minimum_additional_sample": 0,
      "human_review_required": true,
      "auto_apply_allowed": false
    }}
  ],
  "what_not_to_conclude": [
    "This sample does not prove <X> should be disabled.",
    "Observed correlation does not prove causation."
  ],
  "guardrails": {{
    "read_only": true,
    "human_review_required": true,
    "auto_apply_allowed": false,
    "can_change_strategy": false,
    "can_change_risk_guard": false,
    "can_arm_auto_gate": false,
    "can_auto_submit": false,
    "auto_execution_allowed": false,
    "ai_can_execute": false
  }}
}}"""
    return prompt


# ── Stage 1: Pure parse ───────────────────────────────────────────────────────

def _lm_parse_learning_review_response(raw) -> dict:
    """Stage 1: Pure JSON decoding. No semantic repair.

    Accepts raw text or already-decoded dict.
    Returns the parsed dict (or a dict with _parse_error key).
    Does NOT force, remap, repair, or invent any semantic value.
    """
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {"_parse_error": "empty_response", "raw_text": ""}
    t = str(raw).strip()
    if t.startswith("```"):
        lines = t.splitlines()
        t = "\n".join(lines[1:-1] if lines and lines[-1].strip() == "```" else lines[1:])
    t = t.strip()
    try:
        return _json.loads(t)
    except Exception as _e:
        return {
            "_parse_error": "invalid_json",
            "raw_text":     t[:400],
            "json_error":   str(_e)[:120],
        }


# ── Stage 2: Strict validation ────────────────────────────────────────────────

def _lm_validate_learning_review_response(
    parsed: dict, evidence: dict
) -> tuple[bool, list[str]]:
    """Stage 2: Strict validation. Rejects; does NOT repair.

    Returns (is_valid, reason_list).
    An empty reason_list means validation passed.
    """
    reasons: list[str] = []

    # Parse error check
    if not isinstance(parsed, dict):
        return False, ["response_not_a_dict"]
    if "_parse_error" in parsed:
        return False, [f"parse_error:{parsed['_parse_error']}"]

    # Required top-level keys
    missing_keys = _REQUIRED_TOP_KEYS - set(parsed.keys())
    if missing_keys:
        for k in sorted(missing_keys):
            reasons.append(f"missing_required_key:{k}")
        return False, reasons

    # Task 2: strict non-empty string types for review_title and executive_summary
    title_v = parsed.get("review_title")
    if not isinstance(title_v, str) or not title_v.strip():
        reasons.append("review_title_not_a_nonempty_string")
    elif len(title_v.strip()) > 80:
        reasons.append("review_title_exceeds_80_chars")

    summary_v = parsed.get("executive_summary")
    if not isinstance(summary_v, str) or not summary_v.strip():
        reasons.append("executive_summary_not_a_nonempty_string")
    elif len(summary_v.strip()) > 1000:
        reasons.append("executive_summary_exceeds_1000_chars")

    # overall_assessment
    if parsed.get("overall_assessment") not in _VALID_ASSESSMENTS:
        reasons.append(f"invalid_overall_assessment:{parsed.get('overall_assessment')!r}")

    # confidence_level
    if parsed.get("confidence_level") not in _VALID_CONFIDENCES:
        reasons.append(f"invalid_confidence_level:{parsed.get('confidence_level')!r}")

    # Low-sample enforcement: 5-9 trades must be low + insufficient_data
    total = evidence.get("sample_size", 0)
    if 5 <= total < 10:
        if parsed.get("confidence_level") != "low":
            reasons.append("insufficient_sample_confidence_must_be_low")
        if parsed.get("overall_assessment") != "insufficient_data":
            reasons.append("insufficient_sample_assessment_must_be_insufficient_data")

    # Task 3: strict sample_assessment schema
    sa = parsed.get("sample_assessment")
    if not isinstance(sa, dict):
        reasons.append("sample_assessment_not_a_dict")
    else:
        ev_sq = evidence.get("sample_quality", _SQ_INSUFFICIENT)
        ai_sq = sa.get("sample_quality")
        if ai_sq not in _VALID_SAMPLE_QUALITIES:
            reasons.append(f"invalid_sample_quality:{ai_sq!r}")
        elif ai_sq != ev_sq:
            _sq_rank = {_SQ_INSUFFICIENT: 0, _SQ_EARLY: 1, _SQ_DEVELOPING: 2, _SQ_MEANINGFUL: 3}
            if _sq_rank.get(ai_sq, 0) > _sq_rank.get(ev_sq, 0):
                reasons.append(f"ai_sample_quality_higher_than_evidence:{ai_sq!r}>{ev_sq!r}")

        # Task 3: sample_size must be int (not bool), non-negative
        ai_n = sa.get("sample_size")
        if not isinstance(ai_n, int) or isinstance(ai_n, bool) or ai_n < 0:
            reasons.append("sample_assessment_sample_size_not_a_nonneg_int")
        elif ai_n != total:
            reasons.append(f"sample_size_mismatch:ai={ai_n}!=evidence={total}")

        # Task 3: limitations must be list of strings
        sa_lims = sa.get("limitations")
        if not isinstance(sa_lims, list):
            reasons.append("sample_assessment_limitations_not_a_list")
        else:
            if len(sa_lims) > _SA_LIM_MAX:
                reasons.append("too_many_sample_limitations")
            for li, lim in enumerate(sa_lims):
                if not isinstance(lim, str):
                    reasons.append(f"sample_limitation[{li}]_not_a_string")
            if 5 <= total < 10:
                has_marker = any(
                    isinstance(l, str) and l in _SMALL_SAMPLE_MARKERS
                    for l in sa_lims
                )
                if not has_marker:
                    reasons.append("sample_assessment_missing_small_sample_marker")

    # Build allowlist for evidence-row validation
    allowlist = _lm_build_evidence_metric_allowlist(evidence)

    # Task 1: explicit length rejection before processing
    obs_list = parsed.get("observations")
    if not isinstance(obs_list, list):
        reasons.append("observations_not_a_list")
        obs_list = []
    elif len(obs_list) > _OBS_MAX:
        reasons.append("too_many_observations")

    obs_ids: set = set()
    for i, obs in enumerate(obs_list[:_OBS_MAX]):
        if not isinstance(obs, dict):
            reasons.append(f"obs[{i}]_not_a_dict")
            continue
        missing_obs = _OBS_REQUIRED_KEYS - set(obs.keys())
        if missing_obs:
            for k in sorted(missing_obs):
                reasons.append(f"obs[{i}]_missing_key:{k}")

        obs_id = obs.get("id")
        if not obs_id or not isinstance(obs_id, str):
            reasons.append(f"obs[{i}]_missing_or_invalid_id")
        elif len(obs_id) > _OBS_ID_MAX:
            reasons.append(f"obs[{i}]_id_too_long:{obs_id!r}")
        elif obs_id in obs_ids:
            reasons.append(f"obs[{i}]_duplicate_id:{obs_id!r}")
        else:
            obs_ids.add(obs_id)

        if obs.get("category") not in _VALID_OBS_CATEGORIES:
            reasons.append(f"obs[{i}]_invalid_category:{obs.get('category')!r}")
        if obs.get("confidence") not in _VALID_CONFIDENCES:
            reasons.append(f"obs[{i}]_invalid_confidence:{obs.get('confidence')!r}")
        if obs.get("severity") not in _VALID_SEVERITIES:
            reasons.append(f"obs[{i}]_invalid_severity:{obs.get('severity')!r}")

        # Task 4: title/statement must be non-empty strings
        obs_title = obs.get("title")
        if not isinstance(obs_title, str) or not obs_title.strip():
            reasons.append(f"obs[{i}]_title_not_a_nonempty_string")
        obs_stmt = obs.get("statement")
        if not isinstance(obs_stmt, str) or not obs_stmt.strip():
            reasons.append(f"obs[{i}]_statement_not_a_nonempty_string")

        # Task 4: sample_size must be int (not bool), non-negative
        obs_n = obs.get("sample_size")
        if not isinstance(obs_n, int) or isinstance(obs_n, bool) or obs_n < 0:
            reasons.append(f"obs[{i}]_sample_size_not_a_nonneg_int")
            obs_n = None  # prevent Task 6 binding on bad value

        # Task 4: limitations must be a list (reject string)
        lims = obs.get("limitations")
        if not isinstance(lims, list):
            reasons.append(f"obs[{i}]_limitations_not_a_list")
        else:
            if len(lims) > _LIMIT_MAX:
                reasons.append(f"obs[{i}]_too_many_limitations")
            for li, lim in enumerate(lims):
                if not isinstance(lim, str):
                    reasons.append(f"obs[{i}].limitation[{li}]_not_a_string")

        # auto_apply_allowed must be exactly False
        if obs.get("auto_apply_allowed") is not False:
            reasons.append(f"obs[{i}]_auto_apply_allowed_not_false")

        # Task 5: validate evidence rows
        ev_rows = obs.get("evidence")
        if not isinstance(ev_rows, list):
            reasons.append(f"obs[{i}]_evidence_not_a_list")
            ev_rows = []
        elif len(ev_rows) == 0:
            reasons.append(f"obs[{i}]_evidence_empty")
        elif len(ev_rows) > _EVIDENCE_ROWS:
            reasons.append(f"obs[{i}]_too_many_evidence_rows")

        # Track first .trade_count metric for Task 6 sample_size binding
        tc_metric_found: str | None = None
        tc_trusted_val = None

        for j, row in enumerate(ev_rows[:_EVIDENCE_ROWS]):
            if not isinstance(row, dict):
                reasons.append(f"obs[{i}].evidence[{j}]_not_a_dict")
                continue
            metric = row.get("metric")
            if not metric or not isinstance(metric, str):
                reasons.append(f"obs[{i}].evidence[{j}]_missing_metric")
                continue

            # Task 5: require value and comparison keys
            if "value" not in row:
                reasons.append(f"obs[{i}].evidence[{j}]_missing_value_key")
            if "comparison" not in row:
                reasons.append(f"obs[{i}].evidence[{j}]_missing_comparison_key")

            if metric not in allowlist:
                reasons.append(f"obs[{i}].evidence[{j}]_unknown_metric:{metric!r}")
                continue

            ai_val = row.get("value")

            # Task 5: reject NaN/Infinity
            if isinstance(ai_val, float) and (_math.isnan(ai_val) or _math.isinf(ai_val)):
                reasons.append(f"obs[{i}].evidence[{j}]_value_nan_or_inf:{metric!r}")
                continue

            trusted_val = allowlist[metric]

            # Task 5: reject null AI value for known non-null trusted metric
            if trusted_val is not None and ai_val is None:
                reasons.append(
                    f"obs[{i}].evidence[{j}]_null_value_for_non_null_metric:{metric!r}"
                )
                continue

            # Task 7: metric-specific tolerance check
            if ai_val is not None:
                if not _lm_compare_evidence_metric(metric, trusted_val, ai_val):
                    reasons.append(
                        f"obs[{i}].evidence[{j}]_value_out_of_tolerance:{metric!r}"
                    )

            # Task 6: track first .trade_count metric for binding check
            if tc_metric_found is None and metric.endswith(".trade_count"):
                tc_metric_found = metric
                tc_trusted_val  = trusted_val

        # Task 6: trusted sample_size binding — obs.sample_size must equal trade_count
        if (
            tc_metric_found is not None
            and tc_trusted_val is not None
            and obs_n is not None
        ):
            if obs_n != int(tc_trusted_val):
                reasons.append(
                    f"obs[{i}]_sample_size_mismatch_with_trade_count:"
                    f"{obs_n}!={tc_metric_found}={tc_trusted_val}"
                )

    # Task 1: explicit length rejection for review_proposals
    prop_list = parsed.get("review_proposals")
    if not isinstance(prop_list, list):
        reasons.append("review_proposals_not_a_list")
        prop_list = []
    elif len(prop_list) > _PROPOSAL_MAX:
        reasons.append("too_many_review_proposals")

    prop_ids: set = set()
    for i, prop in enumerate(prop_list[:_PROPOSAL_MAX]):
        if not isinstance(prop, dict):
            reasons.append(f"proposal[{i}]_not_a_dict")
            continue
        missing_prop = _PROP_REQUIRED_KEYS - set(prop.keys())
        if missing_prop:
            for k in sorted(missing_prop):
                reasons.append(f"proposal[{i}]_missing_key:{k}")

        pid = prop.get("id")
        if not pid or not isinstance(pid, str):
            reasons.append(f"proposal[{i}]_missing_or_invalid_id")
        elif len(pid) > _OBS_ID_MAX:
            reasons.append(f"proposal[{i}]_id_too_long:{pid!r}")
        elif pid in prop_ids:
            reasons.append(f"proposal[{i}]_duplicate_id:{pid!r}")
        else:
            prop_ids.add(pid)

        # action_type strict whitelist
        action = prop.get("action_type")
        if action not in _VALID_PROPOSAL_ACTIONS:
            reasons.append(f"proposal[{i}]_invalid_action_type:{action!r}")
        if isinstance(action, str):
            for forbidden in _FORBIDDEN_ACTION_WORDS:
                if forbidden in action.lower():
                    reasons.append(f"proposal[{i}]_forbidden_action_word:{forbidden!r}")

        # Task 9: strict proposal field types
        prop_title = prop.get("title")
        if not isinstance(prop_title, str) or not prop_title.strip():
            reasons.append(f"proposal[{i}]_title_not_a_nonempty_string")

        prop_desc = prop.get("description")
        if not isinstance(prop_desc, str) or not prop_desc.strip():
            reasons.append(f"proposal[{i}]_description_not_a_nonempty_string")

        # Task 9: minimum_additional_sample must be int (not bool), non-negative
        mas = prop.get("minimum_additional_sample")
        if not isinstance(mas, int) or isinstance(mas, bool) or mas < 0:
            reasons.append(f"proposal[{i}]_minimum_additional_sample_not_a_nonneg_int")

        # human_review_required must be exactly True
        if prop.get("human_review_required") is not True:
            reasons.append(f"proposal[{i}]_human_review_required_not_true")

        # auto_apply_allowed must be exactly False
        if prop.get("auto_apply_allowed") is not False:
            reasons.append(f"proposal[{i}]_auto_apply_not_false")

        # Task 10: forbidden operational language in title/description
        if _has_operational_language(prop_title) or _has_operational_language(prop_desc):
            reasons.append(f"proposal[{i}]_forbidden_operational_language")

        # Task 8: always validate observation refs unconditionally (fix: no elif obs_ids)
        eoi = prop.get("evidence_observation_ids")
        if not isinstance(eoi, list) or len(eoi) == 0:
            reasons.append(f"proposal[{i}]_empty_evidence_observation_ids")
        else:
            if len(eoi) > _PROP_EOI_MAX:
                reasons.append(f"proposal[{i}]_too_many_observation_references")
            for ref_id in eoi:
                if not isinstance(ref_id, str):
                    reasons.append(f"proposal[{i}]_obs_ref_not_a_string:{ref_id!r}")
                elif ref_id not in obs_ids:
                    reasons.append(f"proposal[{i}]_unknown_obs_ref:{ref_id!r}")

        # Low-sample proposals must be collect_more_data or monitor only
        if 5 <= total < 10 and action not in ("collect_more_data", "monitor"):
            reasons.append(f"proposal[{i}]_insufficient_sample_must_be_collect_or_monitor")

    # Task 1: what_not_to_conclude length and type check
    wntc = parsed.get("what_not_to_conclude")
    if not isinstance(wntc, list):
        reasons.append("what_not_to_conclude_not_a_list")
    else:
        if len(wntc) > _WNTC_MAX:
            reasons.append("too_many_what_not_to_conclude")
        for wi, w in enumerate(wntc):
            if not isinstance(w, str):
                reasons.append(f"what_not_to_conclude[{wi}]_not_a_string")
        ev_warns = evidence.get("warnings") or []
        needs_wntc = (
            evidence.get("sample_quality") in (_SQ_INSUFFICIENT, _SQ_EARLY)
            or bool(ev_warns)
            or total < 30
        )
        if needs_wntc and len(wntc) == 0:
            reasons.append("what_not_to_conclude_required_but_empty")

    # guardrails — strict exact value checking
    g = parsed.get("guardrails")
    if not isinstance(g, dict):
        reasons.append("guardrails_not_a_dict")
    else:
        for key, expected in _GUARDRAILS_REQUIRED.items():
            actual = g.get(key)
            if actual != expected:
                reasons.append(f"guardrail_{key}_must_be_{expected}:got_{actual!r}")

    is_valid = len(reasons) == 0
    return is_valid, reasons


# ── Stage 3: Sanitize (text trimming only — after validation passes) ──────────

def _lm_sanitize_valid_learning_review(parsed: dict) -> dict:
    """Stage 3: Trim bounded display strings. Called ONLY after validation passes.

    MUST NOT alter: IDs, action_types, categories, confidence, severity,
    guardrail booleans, human_review_required, auto_apply_allowed, numeric values.
    """
    import copy
    out = copy.deepcopy(parsed)

    def _st(v, max_len: int) -> str:
        return str(v).strip()[:max_len]

    out["review_title"]       = _st(out.get("review_title", ""),       80)
    out["executive_summary"]  = _st(out.get("executive_summary", ""),  1000)

    # Sample assessment limitations
    sa = out.get("sample_assessment") or {}
    if isinstance(sa.get("limitations"), list):
        sa["limitations"] = [_st(x, 200) for x in sa["limitations"][:_LIMIT_MAX]]
    out["sample_assessment"] = sa

    # Observations: trim display text, not schema fields
    obs = out.get("observations") or []
    for o in obs[:_OBS_MAX]:
        if isinstance(o, dict):
            o["title"]     = _st(o.get("title",     ""), 80)
            o["statement"] = _st(o.get("statement", ""), 500)
            ev_rows = o.get("evidence") or []
            for row in ev_rows[:_EVIDENCE_ROWS]:
                if isinstance(row, dict) and isinstance(row.get("comparison"), str):
                    row["comparison"] = _st(row["comparison"], 200)
            o["evidence"] = ev_rows[:_EVIDENCE_ROWS]
            lims = o.get("limitations") or []
            if isinstance(lims, list):
                o["limitations"] = [_st(x, 200) for x in lims[:_LIMIT_MAX]]
            elif isinstance(lims, str):
                o["limitations"] = _st(lims, 200)
    out["observations"] = obs[:_OBS_MAX]

    # Proposals: trim display text, not action_type, IDs, or booleans
    props = out.get("review_proposals") or []
    for p in props[:_PROPOSAL_MAX]:
        if isinstance(p, dict):
            p["title"]       = _st(p.get("title",       ""), 80)
            p["description"] = _st(p.get("description", ""), 500)
    out["review_proposals"] = props[:_PROPOSAL_MAX]

    # what_not_to_conclude
    wntc = out.get("what_not_to_conclude") or []
    out["what_not_to_conclude"] = [_st(x, 300) for x in wntc[:_WNTC_MAX]]

    return out


# ── Deterministic fallback ────────────────────────────────────────────────────

def _lm_build_deterministic_review(evidence: dict, candidates: list) -> dict:
    """Build a valid deterministic review when AI is unavailable.

    Always labelled source="deterministic_fallback". Conforms to full schema.
    """
    total = evidence.get("sample_size", 0)
    sq    = evidence.get("sample_quality", _SQ_INSUFFICIENT)
    ps    = evidence.get("performance_summary") or {}
    warns = evidence.get("warnings") or []

    # Task 13: build allowlist to use real evidence values in evidence rows
    allowlist = _lm_build_evidence_metric_allowlist(evidence)

    # Build observations from candidates
    obs: list = []
    for i, c in enumerate(candidates[:_OBS_MAX]):
        ct = c.get("candidate_type", "general_observation")
        cat_map = {
            "symbol_underperformance": "symbol", "symbol_outperformance": "symbol",
            "side_imbalance": "side",
            "stop_loss_concentration": "exit",
            "low_rr_capture": "risk_reward", "strong_rr_capture": "risk_reward",
            "confidence_not_confirmed": "confidence", "confidence_supported": "confidence",
            "setup_type_underperformance": "setup", "setup_type_outperformance": "setup",
            "deteriorating_recent_period": "trend", "improving_recent_period": "trend",
            "insufficient_sample": "data_quality", "data_quality_problem": "data_quality",
            "no_action_recommended": "data_quality",
        }
        cat = cat_map.get(ct, "data_quality")
        conf = c.get("confidence", "low")
        # Task 13: use real values from allowlist; skip metrics not present there
        ev_rows = [
            {"metric": mid, "value": allowlist[mid], "comparison": None}
            for mid in (c.get("evidence_metric_ids") or [])[:_EVIDENCE_ROWS]
            if mid in allowlist
        ]
        # Ensure at least one evidence row (Task 5: evidence must not be empty)
        if not ev_rows:
            fallback = "performance.sample_size"
            ev_rows = [{
                "metric":     fallback,
                "value":      allowlist.get(fallback, total),
                "comparison": None,
            }]
        seg_val = c.get("segment_value")
        obs.append({
            "id":           f"obs_{i+1}",
            "category":     cat,
            "title":        ct.replace("_", " ").title(),
            "statement":    (
                f"Deterministic analysis of {c.get('sample_size', total)} paper trades "
                f"suggests pattern: {ct.replace('_',' ')}."
                + (f" Segment: {seg_val}." if seg_val else "")
            ),
            "evidence":     ev_rows,
            "sample_size":  c.get("sample_size", total),
            "confidence":   conf,
            "severity":     c.get("severity", "info"),
            "limitations":  c.get("limitations", []) + ["deterministic_fallback_no_ai_analysis"],
            "auto_apply_allowed": False,
        })

    if not obs:
        obs.append({
            "id": "obs_1", "category": "data_quality",
            "title": "Insufficient Data for Analysis",
            "statement": f"Only {total} paper trades available. Insufficient for pattern detection.",
            "evidence": [{"metric": "performance.trade_count", "value": total, "comparison": None}],
            "sample_size": total, "confidence": "low", "severity": "info",
            "limitations": ["deterministic_fallback_no_ai_analysis"],
            "auto_apply_allowed": False,
        })

    obs_ids = [o["id"] for o in obs]
    proposal_action = "collect_more_data" if sq in (_SQ_INSUFFICIENT, _SQ_EARLY) else "monitor"
    proposals = [{
        "id": "proposal_1",
        "action_type": proposal_action,
        "title": "Gather more data for meaningful analysis",
        "description": (
            "Continue paper trading to accumulate statistically meaningful sample. "
            "Re-generate a learning review when more trades are available."
        ),
        "evidence_observation_ids": [obs_ids[0]],
        "minimum_additional_sample": max(0, 30 - total),
        "human_review_required": True,
        "auto_apply_allowed": False,
    }]

    wntc = [
        "This deterministic summary does not prove any pattern is real or persistent.",
        "Do not change strategy, risk settings, or execution mode based on this review.",
    ]
    if warns:
        wntc.append("Data warnings are present — review may be incomplete.")

    overall = "insufficient_data" if sq in (_SQ_INSUFFICIENT, _SQ_EARLY) else "mixed"
    conf    = "low"

    return {
        "review_title":       "Learning Review (Deterministic Fallback)",
        "executive_summary":  (
            f"Deterministic analysis of {total} paper trades ({sq} quality). "
            "AI provider was unavailable. Results are pattern-detection only — no AI reasoning applied."
        ),
        "overall_assessment": overall,
        "confidence_level":   conf,
        "sample_assessment": {
            "sample_size":   total,
            "sample_quality": sq,
            "limitations":   ["ai_provider_unavailable", "deterministic_fallback_used"],
        },
        "observations":       obs,
        "review_proposals":   proposals,
        "what_not_to_conclude": wntc,
        "guardrails":         dict(_GUARDRAILS_REQUIRED),
    }


# ── Persistence ───────────────────────────────────────────────────────────────

def _lm_save_learning_review(
    user_id,
    filters:          dict,
    evidence:         dict,
    sanitized_review: dict,
    source:           str  = "ai",
    model_name:       str  = None,
    parent_review_id: int  = None,
    candidates:       list = None,
    allowlist:        dict = None,
) -> object:
    """Persist a validated + sanitized review to the database.

    Task 13: evidence_json now stores full auditable snapshot including
    performance summary, candidates, and evidence metric allowlist.
    """
    from models import LiveMonitorLearningReview as _LR
    from extensions import db

    ps = evidence.get("performance_summary") or {}

    # Task 13: bounded auditable evidence_json
    evidence_record = {
        "prompt_version":    _PROMPT_VERSION,
        "filters": {
            "period":       filters.get("period"),
            "symbol":       filters.get("symbol"),
            "side":         filters.get("side"),     # Task 8: correct key
            "item_id":      filters.get("item_id"),
            "review_scope": filters.get("_review_scope"),
        },
        "performance_summary": ps,
        "query_meta":   evidence.get("query_meta"),
        "data_quality": evidence.get("data_quality"),
        "execution_quality": evidence.get("execution_quality"),
        "warnings":     evidence.get("warnings"),
        "sample_size":  evidence.get("sample_size"),
        "sample_quality": evidence.get("sample_quality"),
        # Top segments for auditability (bounded)
        "segments_summary": {
            k: v[:10] if isinstance(v, list) else v
            for k, v in (evidence.get("segments") or {}).items()
        },
        # Deterministic candidates used in prompt
        "deterministic_candidates": (candidates or [])[:_OBS_MAX],
        # Evidence metric allowlist (compact — keys only, values are derivable)
        "evidence_metric_keys": list((allowlist or {}).keys())[:100],
    }

    # Task 8: side key correction (read from filters["side"])
    review = _LR(
        user_id          = user_id,
        item_id          = filters.get("item_id"),
        review_scope     = filters.get("_review_scope", "portfolio"),
        period           = filters.get("period", "30d"),
        symbol           = filters.get("symbol"),
        side             = filters.get("side"),          # Task 8 fix
        status           = "generated",
        title            = sanitized_review.get("review_title", "Learning Review")[:200],
        summary          = sanitized_review.get("executive_summary", "")[:1000],
        review_json      = _json.dumps(sanitized_review),
        evidence_json    = _json.dumps(evidence_record),
        sample_size      = evidence.get("sample_size", 0),
        sample_quality   = evidence.get("sample_quality", _SQ_INSUFFICIENT),
        confidence_level = sanitized_review.get("confidence_level", "low"),
        warning_count    = len(sanitized_review.get("what_not_to_conclude") or []) +
                           len(evidence.get("warnings") or []),
        source           = source,
        model_name       = model_name,
        prompt_version   = _PROMPT_VERSION,
        parent_review_id = parent_review_id,
    )
    db.session.add(review)
    return review


# ── Retrieval ─────────────────────────────────────────────────────────────────

def _lm_get_learning_reviews(
    user_id,
    item_id       = None,
    review_scope  = None,
    status_filter = None,
    limit         = 20,
    offset        = 0,
) -> list:
    from models import LiveMonitorLearningReview as _LR
    q = _LR.query.filter_by(user_id=user_id)
    if item_id is not None:
        q = q.filter_by(item_id=int(item_id))
    if review_scope and review_scope in _VALID_SCOPES:
        q = q.filter_by(review_scope=review_scope)
    if status_filter and status_filter in _VALID_STATUSES:
        q = q.filter_by(status=status_filter)
    rows = q.order_by(_LR.created_at.desc()).limit(limit).offset(offset).all()
    return [_serialize_review(r, include_json=False) for r in rows]


def _lm_get_learning_review(user_id, review_id: int) -> dict | None:
    from models import LiveMonitorLearningReview as _LR
    r = _LR.query.filter_by(id=review_id, user_id=user_id).first()
    return None if r is None else _serialize_review(r, include_json=True)


def _lm_update_learning_review(
    user_id,
    review_id:   int,
    new_status:  str | None = None,
    human_note   = _NOTE_NOT_PROVIDED,
) -> tuple[bool, str, dict | None]:
    """Update review status and/or human note.

    Task 15 contract:
      - new_status=None: note-only update, no transition attempted.
      - human_note=_NOTE_NOT_PROVIDED: note field is untouched.
      - human_note=None: explicitly clears the note.
      - human_note="text": sets the note (bounded to 2000 chars).
      - Empty PATCH (no new_status, no human_note supplied): caller should reject before here.
    """
    from models import LiveMonitorLearningReview as _LR
    from extensions import db

    r = _LR.query.filter_by(id=review_id, user_id=user_id).first()
    if r is None:
        return False, "review_not_found", None

    changed = False

    # Status transition (optional)
    if new_status is not None:
        if new_status not in _VALID_STATUSES:
            return False, "invalid_status", None
        allowed = _STATUS_TRANSITIONS.get(r.status, frozenset())
        if new_status not in allowed:
            return False, f"invalid_transition:{r.status}->{new_status}", None
        r.status      = new_status
        r.reviewed_at = datetime.now(timezone.utc)
        changed = True

    # Note update (optional, independent of status)
    if human_note is not _NOTE_NOT_PROVIDED:
        if human_note is None:
            r.human_note = None
        else:
            r.human_note = str(human_note)[:2000]
        changed = True

    if not changed:
        return False, "no_changes_applied", None

    r.updated_at = datetime.now(timezone.utc)
    db.session.commit()
    return True, "ok", _serialize_review(r, include_json=False)


def _lm_update_learning_review_status(
    user_id, review_id: int, new_status: str, human_note: str = None,
) -> tuple[bool, str, dict | None]:
    """Legacy alias. Prefer _lm_update_learning_review."""
    note = _NOTE_NOT_PROVIDED if human_note is None else human_note
    return _lm_update_learning_review(
        user_id, review_id, new_status=new_status, human_note=note,
    )


# ── Serialization ─────────────────────────────────────────────────────────────

def _serialize_review(r, include_json: bool = False) -> dict:
    """Convert a LiveMonitorLearningReview row to a wire-safe dict."""
    d: dict = {
        "id":              r.id,
        "user_id":         r.user_id,
        "item_id":         r.item_id,
        "review_scope":    r.review_scope,
        "period":          r.period,
        "symbol":          r.symbol,
        "side":            r.side,
        "status":          r.status,
        "review_title":    r.title,
        "executive_summary": r.summary,
        "sample_size":     r.sample_size,
        "sample_quality":  r.sample_quality,
        "confidence_level":r.confidence_level,
        "warning_count":   r.warning_count,
        "source":          r.source,
        "model_name":      r.model_name,
        "prompt_version":  r.prompt_version,
        "parent_review_id":r.parent_review_id,
        "human_note":      r.human_note,
        "created_at":      r.created_at.isoformat() if r.created_at else None,
        "updated_at":      r.updated_at.isoformat() if r.updated_at else None,
        "reviewed_at":     r.reviewed_at.isoformat() if r.reviewed_at else None,
        "guardrails":      dict(_MODULE_GUARDRAILS),
    }
    if include_json:
        try:
            review_data = _json.loads(r.review_json) if r.review_json else None
            d["overall_assessment"]   = (review_data or {}).get("overall_assessment")
            d["observations"]         = (review_data or {}).get("observations", [])
            d["review_proposals"]     = (review_data or {}).get("review_proposals", [])
            d["what_not_to_conclude"] = (review_data or {}).get("what_not_to_conclude", [])
            d["review_data"]          = review_data
            d["evidence_data"]        = _json.loads(r.evidence_json) if r.evidence_json else None
        except Exception:
            d["review_data"]          = None
            d["evidence_data"]        = None
            d["overall_assessment"]   = None
            d["observations"]         = []
            d["review_proposals"]     = []
            d["what_not_to_conclude"] = []
    return d


# ── Accepted insights context ─────────────────────────────────────────────────

def _lm_build_accepted_learning_context(
    user_id,
    item_id   = None,
    max_items: int = _MAX_ACCEPTED_INSIGHTS,
) -> list:
    """Build compact accepted-insight context for AI decision context injection.

    Task 14: Returns bounded list with human_note, sample_size, sample_quality,
    read_only, auto_apply_allowed. Excludes full evidence JSON.
    """
    from models import LiveMonitorLearningReview as _LR

    q = _LR.query.filter_by(user_id=user_id, status="accepted_insight")
    if item_id is not None:
        q = q.filter((_LR.item_id == int(item_id)) | (_LR.item_id.is_(None)))
    rows = q.order_by(_LR.reviewed_at.desc()).limit(max_items).all()

    out = []
    for r in rows:
        entry: dict = {
            "review_id":     r.id,
            "date":          r.reviewed_at.isoformat() if r.reviewed_at else None,
            "scope":         r.review_scope,
            "period":        r.period,
            "symbol":        r.symbol,
            "side":          r.side,
            "title":         r.title,
            "summary":       r.summary,
            "sample_size":   r.sample_size,
            "sample_quality": r.sample_quality,
            "confidence_level": r.confidence_level,
            "human_note":    r.human_note,
            "read_only":     True,
            "auto_apply_allowed": False,
        }
        # Accepted observation titles (compact — no full statement text)
        try:
            rd  = _json.loads(r.review_json) if r.review_json else {}
            obs = rd.get("observations") or []
            entry["accepted_observation_titles"] = [
                o.get("title", "") for o in obs if isinstance(o, dict)
            ][:_OBS_MAX]
        except Exception:
            entry["accepted_observation_titles"] = []
        out.append(entry)
    return out
