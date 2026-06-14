"""Phase 11.15: AI Learning Review Loop — read-only advisory analysis.

This module generates AI-assisted learning observations from closed paper trade
history and manages the human review workflow for those observations.

HARD INVARIANTS (never negotiable):
  - No execution. No orders. No position mutations. No exchange API calls.
  - No automatic strategy changes. No automatic parameter application.
  - can_auto_submit is ALWAYS False.
  - auto_execution_allowed is ALWAYS False.
  - ai_can_execute is ALWAYS False.
  - auto_apply_allowed in AI response is ALWAYS rejected/forced False.
  - Human review decisions are tracked but never automated.
  - No background workers. No schedulers. No setInterval. No polling.
"""
from __future__ import annotations

import json as _json
import re as _re
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation

from live_monitor.paper_performance import (
    _lm_build_paper_performance_filters,
    _lm_validate_paper_performance_filters,
    _lm_query_closed_paper_trades_from_filters,
    _lm_classify_paper_trade_outcome,
    _lm_normalize_performance_period,
    _sdec,
    _ds,
    _ts,
    _VALID_PERIODS,
)

_PHASE          = "phase11_15_ai_learning_review"
_PROMPT_VERSION = "11.15.0"
_MIN_TRADES_AI  = 5   # below → no AI call, insufficient_learning_sample
_MIN_TRADES_LOW = 5   # 5-9 → low confidence only
_MAX_ACCEPTED_INSIGHTS = 20

_VALID_SCOPES  = frozenset({"portfolio", "symbol", "item"})
_VALID_STATUSES = frozenset({
    "generated", "reviewed", "accepted_insight", "rejected", "archived",
})
_STATUS_TRANSITIONS: dict[str, frozenset] = {
    "generated":        frozenset({"reviewed", "accepted_insight", "rejected", "archived"}),
    "reviewed":         frozenset({"accepted_insight", "rejected", "archived"}),
    "accepted_insight": frozenset({"archived"}),
    "rejected":         frozenset({"archived"}),
    "archived":         frozenset(),
}

_GUARDRAILS: dict = {
    "read_only":                   True,
    "paper_primary":               True,
    "can_auto_submit":             False,
    "auto_execution_allowed":      False,
    "ai_can_execute":              False,
    "live_disabled":               True,
    "testnet_strategy_validation": False,
    "auto_apply_allowed":          False,
}

_VALID_OBS_TYPES = frozenset({
    "symbol_underperformance",
    "symbol_outperformance",
    "side_imbalance",
    "low_rr_capture",
    "high_rr_capture",
    "exit_timing_observation",
    "confidence_filter_signal",
    "setup_type_signal",
    "entry_mode_signal",
    "outcome_reason_pattern",
    "data_quality_warning",
    "general_observation",
})


# ── Helpers ───────────────────────────────────────────────────────────────────

def _safe_pct(num, den):
    """Return float percentage or None."""
    try:
        if den == 0:
            return None
        return round(float(num) / float(den) * 100, 2)
    except Exception:
        return None


def _safe_avg(values: list):
    """Return float average or None."""
    vals = [v for v in values if v is not None]
    if not vals:
        return None
    try:
        return round(float(sum(vals)) / len(vals), 4)
    except Exception:
        return None


def _sdec_list(rows, attr: str) -> list:
    """Extract Decimal values from a list of objects/dicts, skipping None."""
    out = []
    for r in rows:
        raw = r.get(attr) if isinstance(r, dict) else getattr(r, attr, None)
        d = _sdec(raw)
        if d is not None:
            out.append(d)
    return out


# ── Filter / validation ───────────────────────────────────────────────────────

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
    Returns a dict with _scope_err if scope is invalid.
    """
    scope_err = None
    scope_norm = (review_scope or "portfolio").strip().lower()
    if scope_norm not in _VALID_SCOPES:
        scope_err = "invalid_review_scope"
        scope_norm = "portfolio"

    # Delegate symbol/side/item_id/period validation to Phase 11.14 layer
    perf_filters = _lm_build_paper_performance_filters(
        user_id,
        period=period,
        symbol=symbol,
        side=side,
        item_id=item_id,
        symbol_supplied=symbol_supplied,
    )

    perf_filters["_review_scope"]     = scope_norm
    perf_filters["_review_scope_err"] = scope_err
    return perf_filters


def _lm_validate_learning_review_filters(filters: dict) -> dict:
    """Returns field_errors dict. Empty = valid."""
    errs = {}
    if filters.get("_review_scope_err"):
        errs["review_scope"] = filters["_review_scope_err"]
    if filters.get("_symbol_err"):
        errs["symbol"] = filters["_symbol_err"]
    if filters.get("_side_err"):
        errs["side"] = filters["_side_err"]
    if filters.get("_item_id_err"):
        errs["item_id"] = filters["_item_id_err"]
    return errs


# ── Evidence builder ──────────────────────────────────────────────────────────

def _lm_build_learning_evidence(user_id, filters: dict) -> dict:
    """Build evidence snapshot from closed paper trades.

    Must be called only with validated filters (raises ValueError otherwise).
    Reuses _lm_query_closed_paper_trades_from_filters — no re-query.
    """
    field_errors = _lm_validate_learning_review_filters(filters)
    if field_errors:
        raise ValueError(f"unvalidated_learning_filters:{list(field_errors.keys())}")

    trades, qmeta = _lm_query_closed_paper_trades_from_filters(user_id, filters)

    total = len(trades)
    evidence = {
        "sample_size":    total,
        "sample_quality": "insufficient" if total < _MIN_TRADES_AI
                          else ("low" if total < 10 else "high"),
        "query_meta":     qmeta,
        "period":         filters.get("period", "30d"),
        "symbol":         filters.get("symbol"),
        "side":           filters.get("side_norm"),
        "item_id":        filters.get("item_id"),
        "review_scope":   filters.get("_review_scope", "portfolio"),
        "segments":       {},
        "execution_quality": {},
        "recent_trades":  [],
        "warnings":       [],
    }

    if total == 0:
        evidence["warnings"].append("no_closed_trades_in_period")
        return evidence

    if qmeta.get("truncated"):
        evidence["warnings"].append("analytics_row_limit_reached")

    evidence["segments"]          = _lm_build_learning_segments(trades)
    evidence["execution_quality"] = _lm_build_execution_quality(trades)
    evidence["recent_trades"]     = _build_recent_trade_summaries(trades, limit=10)

    return evidence


def _lm_build_learning_segments(trades: list) -> dict:
    """Compute per-dimension breakdown segments for learning evidence."""
    by_symbol:   dict = {}
    by_side:     dict = {}
    by_reason:   dict = {}
    by_conf:     dict = {}
    by_setup:    dict = {}
    by_entry:    dict = {}

    for t in trades:
        outcome, pnl, _, _ = _lm_classify_paper_trade_outcome(t)
        pnl_f = float(pnl) if pnl is not None else None
        sym   = (getattr(t, "symbol", None) or "unknown").upper()
        side  = (getattr(t, "side",   None) or "unknown").upper()
        reason = (getattr(t, "outcome_reason", None) or "").lower().strip() or "unknown"

        # Confidence bucket from ai_decision_json
        conf_bucket = _extract_json_field(t, "ai_decision_json", "confidence_bucket") or "unknown"
        setup_type  = _extract_json_field(t, "entry_snapshot_json", "setup_type")    or "unknown"
        entry_mode  = _extract_json_field(t, "entry_snapshot_json", "entry_mode")    or "unknown"

        for seg_dict, key in [
            (by_symbol, sym),
            (by_side,   side),
            (by_reason, reason),
            (by_conf,   conf_bucket),
            (by_setup,  setup_type),
            (by_entry,  entry_mode),
        ]:
            if key not in seg_dict:
                seg_dict[key] = {"count": 0, "wins": 0, "losses": 0, "breakevenS": 0,
                                 "pnl_sum": Decimal(0), "pnl_values": []}
            seg = seg_dict[key]
            seg["count"] += 1
            if outcome == "win":
                seg["wins"] += 1
                seg["pnl_sum"] += pnl if pnl else Decimal(0)
                if pnl_f is not None:
                    seg["pnl_values"].append(pnl_f)
            elif outcome == "loss":
                seg["losses"] += 1
                seg["pnl_sum"] += pnl if pnl else Decimal(0)
                if pnl_f is not None:
                    seg["pnl_values"].append(pnl_f)
            elif outcome == "breakeven":
                seg["breakevenS"] += 1

    def _finalize(seg_dict: dict) -> list:
        out = []
        for k, s in seg_dict.items():
            total   = s["count"]
            win_pct = _safe_pct(s["wins"], total)
            out.append({
                "label":    k,
                "count":    total,
                "wins":     s["wins"],
                "losses":   s["losses"],
                "breakevens": s["breakevenS"],
                "win_rate": win_pct,
                "net_pnl":  _ds(s["pnl_sum"]),
            })
        out.sort(key=lambda x: -x["count"])
        return out

    return {
        "by_symbol":        _finalize(by_symbol),
        "by_side":          _finalize(by_side),
        "by_outcome_reason":_finalize(by_reason),
        "by_confidence":    _finalize(by_conf),
        "by_setup_type":    _finalize(by_setup),
        "by_entry_mode":    _finalize(by_entry),
    }


def _lm_build_execution_quality(trades: list) -> dict:
    """Analyse planned vs realised RR to assess execution quality."""
    planned_rrs  = []
    realized_rrs = []
    rr_captures  = []

    tp_exit = 0; sl_exit = 0; manual_exit = 0; other_exit = 0
    wins = 0; losses = 0; total_with_pnl = 0

    for t in trades:
        outcome, pnl, _, _ = _lm_classify_paper_trade_outcome(t)
        if pnl is not None:
            total_with_pnl += 1
            if outcome == "win":
                wins += 1
            elif outcome == "loss":
                losses += 1

        planned_rr = _sdec(getattr(t, "risk_reward", None))
        if planned_rr is not None:
            planned_rrs.append(float(planned_rr))

        # Realized RR from ai_post_trade_review_json or execution_intent_json
        realized_rr = _extract_json_field(t, "ai_post_trade_review_json", "realized_rr")
        if realized_rr is None:
            realized_rr = _extract_json_field(t, "execution_intent_json", "realized_rr")
        d_rr = _sdec(realized_rr)
        if d_rr is not None:
            realized_rrs.append(float(d_rr))

        # RR capture = realised / planned
        if planned_rr and d_rr is not None and planned_rr > 0:
            rr_captures.append(float(d_rr / planned_rr))

        # Exit reason
        reason = (getattr(t, "outcome_reason", None) or "").lower()
        if "tp" in reason or "take_profit" in reason or "take profit" in reason:
            tp_exit += 1
        elif "sl" in reason or "stop_loss" in reason or "stop loss" in reason:
            sl_exit += 1
        elif "manual" in reason:
            manual_exit += 1
        else:
            other_exit += 1

    return {
        "avg_planned_rr":   _safe_avg(planned_rrs),
        "avg_realized_rr":  _safe_avg(realized_rrs),
        "avg_rr_capture":   _safe_avg(rr_captures),
        "tp_exit_count":    tp_exit,
        "sl_exit_count":    sl_exit,
        "manual_exit_count":manual_exit,
        "other_exit_count": other_exit,
        "tp_pct":           _safe_pct(tp_exit, len(trades)),
        "sl_pct":           _safe_pct(sl_exit, len(trades)),
        "manual_pct":       _safe_pct(manual_exit, len(trades)),
    }


def _build_recent_trade_summaries(trades: list, limit: int = 10) -> list:
    """Compact summary list of most recent trades."""
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
            "symbol":       getattr(t, "symbol",  None),
            "side":         getattr(t, "side",    None),
            "outcome":      outcome,
            "realized_pnl": _ds(pnl),
            "outcome_reason": getattr(t, "outcome_reason", None),
            "closed_at":    ts.isoformat() if ts else None,
        })
    return out


def _extract_json_field(trade_obj, json_col: str, field: str):
    """Safely extract a field from a JSON column (stored as text)."""
    try:
        raw = getattr(trade_obj, json_col, None)
        if raw is None:
            return None
        parsed = raw if isinstance(raw, dict) else _json.loads(raw)
        return parsed.get(field)
    except Exception:
        return None


# ── Observation candidates ────────────────────────────────────────────────────

def _lm_build_learning_observation_candidates(evidence: dict) -> list:
    """Build deterministic pre-AI observation candidates from evidence segments.

    These candidates guide the AI prompt — they are NOT the AI output.
    Returned as list of {type, label, finding, confidence} dicts.
    """
    candidates = []
    segs = evidence.get("segments", {})
    total = evidence.get("sample_size", 0)
    if total == 0:
        return candidates

    eq = evidence.get("execution_quality", {})

    # Symbol underperformance / outperformance
    for seg in segs.get("by_symbol", []):
        if seg["count"] < 3:
            continue
        wr = seg.get("win_rate")
        if wr is None:
            continue
        if wr < 35:
            candidates.append({
                "type": "symbol_underperformance",
                "label": seg["label"],
                "finding": f"Win rate {wr}% on {seg['count']} trades",
                "confidence": "medium" if seg["count"] >= 5 else "low",
            })
        elif wr > 65:
            candidates.append({
                "type": "symbol_outperformance",
                "label": seg["label"],
                "finding": f"Win rate {wr}% on {seg['count']} trades",
                "confidence": "medium" if seg["count"] >= 5 else "low",
            })

    # Side imbalance
    by_side = {s["label"]: s for s in segs.get("by_side", [])}
    buy_seg  = by_side.get("BUY")
    sell_seg = by_side.get("SELL")
    if buy_seg and sell_seg:
        buy_wr  = buy_seg.get("win_rate") or 0
        sell_wr = sell_seg.get("win_rate") or 0
        if abs(buy_wr - sell_wr) >= 20:
            weaker = "BUY" if buy_wr < sell_wr else "SELL"
            candidates.append({
                "type": "side_imbalance",
                "label": weaker,
                "finding": f"BUY win_rate={buy_wr}% vs SELL win_rate={sell_wr}%",
                "confidence": "medium",
            })

    # Low / high RR capture
    avg_cap = eq.get("avg_rr_capture")
    if avg_cap is not None:
        if avg_cap < 0.5:
            candidates.append({
                "type": "low_rr_capture",
                "label": f"{avg_cap:.0%}",
                "finding": f"Average RR capture is {avg_cap:.1%} — exits before target",
                "confidence": "medium",
            })
        elif avg_cap > 1.1:
            candidates.append({
                "type": "high_rr_capture",
                "label": f"{avg_cap:.0%}",
                "finding": f"Average RR capture is {avg_cap:.1%} — exceeds planned target",
                "confidence": "low",
            })

    # Exit timing: heavy manual exits
    manual_pct = eq.get("manual_pct") or 0
    if manual_pct > 50:
        candidates.append({
            "type": "exit_timing_observation",
            "label": "manual_heavy",
            "finding": f"{manual_pct:.1f}% of exits are manual — plan adherence low",
            "confidence": "medium",
        })

    # Confidence bucket signal
    for seg in segs.get("by_confidence", []):
        if seg["count"] < 3:
            continue
        wr = seg.get("win_rate")
        if wr is None:
            continue
        if wr > 60 and seg["label"] not in ("unknown",):
            candidates.append({
                "type": "confidence_filter_signal",
                "label": seg["label"],
                "finding": f"'{seg['label']}' confidence → {wr}% win rate on {seg['count']} trades",
                "confidence": "low" if seg["count"] < 5 else "medium",
            })

    return candidates


# ── AI prompt builder ─────────────────────────────────────────────────────────

def _lm_build_learning_review_prompt(evidence: dict, candidates: list) -> str:
    """Build the compact structured prompt sent to the AI provider."""
    period    = evidence.get("period", "30d")
    scope     = evidence.get("review_scope", "portfolio")
    sym       = evidence.get("symbol")
    side      = evidence.get("side")
    total     = evidence.get("sample_size", 0)
    quality   = evidence.get("sample_quality", "unknown")
    eq        = evidence.get("execution_quality", {})
    segs      = evidence.get("segments", {})
    warnings  = evidence.get("warnings", [])

    sym_label   = f" for {sym}"  if sym  else ""
    side_label  = f" ({side} side)" if side else ""
    scope_label = scope

    cand_json = _json.dumps(candidates[:10], separators=(",", ":"))
    segs_json = _json.dumps({
        k: v[:5] for k, v in segs.items() if isinstance(v, list)
    }, separators=(",", ":"))
    eq_json = _json.dumps(eq, separators=(",", ":"))

    warnings_note = ""
    if warnings:
        warnings_note = f"\nData warnings: {', '.join(warnings)}."

    prompt = f"""You are an objective learning analysis assistant for a paper trading system.
Analyse the following closed paper trade evidence and return a structured JSON learning review.

Scope: {scope_label}{sym_label}{side_label}
Period: {period} | Trades: {total} | Sample quality: {quality}{warnings_note}

Execution quality: {eq_json}
Segment summary (top-5 each): {segs_json}
Deterministic candidates: {cand_json}

CRITICAL GUARDRAILS (these override everything else):
- auto_apply_allowed MUST be false in every observation.
- Do NOT recommend strategy parameter changes, threshold changes, or Pine Script edits.
- Do NOT recommend changing execution mode, risk guard settings, or auto gate state.
- Only surface observations about past paper trade patterns.
- Phrase every insight as "based on paper trades" — never as certainty.
- If fewer than 5 trades, respond with observations: [] and a warning.

Return valid JSON only — no markdown fences, no prose outside the JSON:
{{
  "title": "short review title (max 80 chars)",
  "summary": "2-3 sentence plain-language summary",
  "confidence_level": "high|medium|low",
  "observations": [
    {{
      "type": "one of: {', '.join(sorted(_VALID_OBS_TYPES))}",
      "label": "short label",
      "finding": "1-2 sentences describing the pattern",
      "sample_n": <integer>,
      "confidence": "high|medium|low",
      "auto_apply_allowed": false
    }}
  ],
  "warnings": ["list of data quality or caution notes"],
  "guardrails": {{
    "auto_apply_allowed": false,
    "ai_can_execute": false,
    "auto_execution_allowed": false
  }}
}}"""
    return prompt


# ── Response parser + validator ───────────────────────────────────────────────

def _lm_parse_learning_review_response(raw: dict) -> dict:
    """Sanitize and validate AI response dict.

    - Forces guardrails to safe values.
    - Strips any observation with an invalid type.
    - Limits to 10 observations.
    - Returns parsed dict with _validation_warnings list.
    """
    validation_warnings: list = []

    if not isinstance(raw, dict):
        return {
            "_parse_error": "non_dict_response",
            "observations": [],
            "_validation_warnings": ["response_not_dict"],
        }

    if "_parse_error" in raw:
        return {**raw, "observations": [], "_validation_warnings": ["json_parse_error"]}

    # Force all guardrail flags safe
    guardrails = raw.get("guardrails") or {}
    if not isinstance(guardrails, dict):
        guardrails = {}
    for flag in ("auto_apply_allowed", "ai_can_execute", "auto_execution_allowed"):
        if guardrails.get(flag) is True:
            validation_warnings.append(f"guardrail_{flag}_forced_false")
        guardrails[flag] = False
    raw["guardrails"] = guardrails

    # Validate observations
    obs_raw = raw.get("observations") or []
    if not isinstance(obs_raw, list):
        obs_raw = []
        validation_warnings.append("observations_not_list")

    clean_obs = []
    for obs in obs_raw[:10]:
        if not isinstance(obs, dict):
            validation_warnings.append("observation_skipped_non_dict")
            continue
        obs_type = obs.get("type", "")
        if obs_type not in _VALID_OBS_TYPES:
            obs["type"] = "general_observation"
            validation_warnings.append(f"observation_type_remapped:{obs_type}")
        # Force auto_apply_allowed = false on every observation
        if obs.get("auto_apply_allowed") is True:
            validation_warnings.append("obs_auto_apply_forced_false")
        obs["auto_apply_allowed"] = False
        clean_obs.append(obs)

    raw["observations"] = clean_obs
    raw["_validation_warnings"] = validation_warnings

    # Sanitize confidence_level
    valid_conf = {"high", "medium", "low"}
    if raw.get("confidence_level") not in valid_conf:
        raw["confidence_level"] = "low"

    # Ensure title and summary are strings
    raw["title"]   = str(raw.get("title")   or "Learning Review")[:80]
    raw["summary"] = str(raw.get("summary") or "")[:1000]

    # Ensure warnings is a list of strings
    warns = raw.get("warnings") or []
    if not isinstance(warns, list):
        warns = []
    raw["warnings"] = [str(w)[:200] for w in warns[:20]]

    return raw


def _lm_validate_learning_review_response(parsed: dict) -> tuple[bool, list]:
    """Return (is_valid, reasons). Valid = can be saved."""
    reasons = []
    if "_parse_error" in parsed:
        reasons.append(f"parse_error:{parsed['_parse_error']}")
    if not isinstance(parsed.get("observations"), list):
        reasons.append("missing_observations_list")
    # guardrails must be present and all False
    g = parsed.get("guardrails") or {}
    for flag in ("auto_apply_allowed", "ai_can_execute", "auto_execution_allowed"):
        if g.get(flag) is not False:
            reasons.append(f"guardrail_not_false:{flag}")
    is_valid = len(reasons) == 0
    return is_valid, reasons


# ── Persistence ───────────────────────────────────────────────────────────────

def _lm_save_learning_review(
    user_id,
    filters:          dict,
    evidence:         dict,
    parsed_response:  dict,
    source:           str     = "ai",
    model_name:       str     = None,
    parent_review_id: int     = None,
) -> object:
    """Persist a learning review to the database.

    Returns the saved LiveMonitorLearningReview instance.
    Caller is responsible for db.session.commit() after calling this.
    """
    from models import LiveMonitorLearningReview as _LR
    from extensions import db

    review = _LR(
        user_id          = user_id,
        item_id          = filters.get("item_id"),
        review_scope     = filters.get("_review_scope", "portfolio"),
        period           = filters.get("period", "30d"),
        symbol           = filters.get("symbol"),
        side             = filters.get("side_norm"),
        status           = "generated",
        title            = parsed_response.get("title", "Learning Review"),
        summary          = parsed_response.get("summary", ""),
        review_json      = _json.dumps(parsed_response),
        evidence_json    = _json.dumps({
            "sample_size":       evidence.get("sample_size"),
            "query_meta":        evidence.get("query_meta"),
            "period":            evidence.get("period"),
            "symbol":            evidence.get("symbol"),
            "side":              evidence.get("side"),
            "execution_quality": evidence.get("execution_quality"),
            "warnings":          evidence.get("warnings"),
            # omit raw segments — they can be large; kept in review_json
        }),
        sample_size      = evidence.get("sample_size", 0),
        sample_quality   = evidence.get("sample_quality", "unknown"),
        confidence_level = parsed_response.get("confidence_level", "low"),
        warning_count    = len(parsed_response.get("warnings") or []),
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
    item_id          = None,
    review_scope     = None,
    status_filter    = None,
    limit            = 20,
    offset           = 0,
) -> list:
    """Return list of review dicts (newest first)."""
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
    """Return a single review dict with full JSON, or None if not found."""
    from models import LiveMonitorLearningReview as _LR

    r = _LR.query.filter_by(id=review_id, user_id=user_id).first()
    if r is None:
        return None
    return _serialize_review(r, include_json=True)


def _lm_update_learning_review_status(
    user_id,
    review_id:   int,
    new_status:  str,
    human_note:  str = None,
) -> tuple[bool, str, dict | None]:
    """Transition a review's status. Returns (ok, reason, serialized)."""
    from models import LiveMonitorLearningReview as _LR
    from extensions import db

    r = _LR.query.filter_by(id=review_id, user_id=user_id).first()
    if r is None:
        return False, "review_not_found", None

    if new_status not in _VALID_STATUSES:
        return False, "invalid_status", None

    allowed = _STATUS_TRANSITIONS.get(r.status, frozenset())
    if new_status not in allowed:
        return False, f"invalid_transition:{r.status}->{new_status}", None

    r.status     = new_status
    r.updated_at = datetime.now(timezone.utc)
    r.reviewed_at = datetime.now(timezone.utc)
    if human_note is not None:
        r.human_note = str(human_note)[:2000]

    db.session.commit()
    return True, "ok", _serialize_review(r, include_json=False)


def _serialize_review(r, include_json: bool = False) -> dict:
    """Convert a LiveMonitorLearningReview row to a dict."""
    d = {
        "id":              r.id,
        "user_id":         r.user_id,
        "item_id":         r.item_id,
        "review_scope":    r.review_scope,
        "period":          r.period,
        "symbol":          r.symbol,
        "side":            r.side,
        "status":          r.status,
        "title":           r.title,
        "summary":         r.summary,
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
        "guardrails":      _GUARDRAILS,
    }
    if include_json:
        try:
            d["review_data"]   = _json.loads(r.review_json)   if r.review_json   else None
            d["evidence_data"] = _json.loads(r.evidence_json) if r.evidence_json else None
        except Exception:
            d["review_data"]   = None
            d["evidence_data"] = None
    return d


# ── Accepted insights context ─────────────────────────────────────────────────

def _lm_build_accepted_learning_context(user_id, item_id=None, max_items: int = _MAX_ACCEPTED_INSIGHTS) -> list:
    """Build compact list of accepted insights for AI context injection.

    Returns at most max_items entries. Advisory only.
    """
    from models import LiveMonitorLearningReview as _LR

    q = _LR.query.filter_by(user_id=user_id, status="accepted_insight")
    if item_id is not None:
        q = q.filter((_LR.item_id == int(item_id)) | (_LR.item_id.is_(None)))
    rows = q.order_by(_LR.reviewed_at.desc()).limit(max_items).all()

    out = []
    for r in rows:
        entry: dict = {
            "review_id":    r.id,
            "scope":        r.review_scope,
            "period":       r.period,
            "symbol":       r.symbol,
            "side":         r.side,
            "title":        r.title,
            "summary":      r.summary,
            "confidence":   r.confidence_level,
            "accepted_at":  r.reviewed_at.isoformat() if r.reviewed_at else None,
        }
        # Include observation types (compact — no full finding text)
        try:
            rd = _json.loads(r.review_json) if r.review_json else {}
            obs = rd.get("observations") or []
            entry["observation_types"] = [o.get("type") for o in obs if isinstance(o, dict)]
        except Exception:
            entry["observation_types"] = []
        out.append(entry)
    return out
