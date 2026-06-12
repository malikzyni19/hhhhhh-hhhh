"""Phase 11.13: Paper Risk Guard for Internal Paper Trading.

Evaluates risk before a manual paper order is submitted.
- Risk scoring, position sizing, blocking rules
- DB-only. No Binance API. No auto-execution. No live mode.
- can_auto_submit is ALWAYS False.
- auto_execution_allowed is ALWAYS False.
"""
from __future__ import annotations
import json as _json_rg
import time as _time_rg

# ── Defaults ─────────────────────────────────────────────────────────────────

_RG_DEFAULT_SETTINGS = {
    "max_risk_pct_per_trade":    2.0,    # % of account equity
    "max_risk_amount_per_trade": 100.0,  # USD equivalent dollar risk
    "max_position_notional":     1000.0, # max position size in USD
    "min_rr":                    1.5,    # minimum reward:risk ratio
    "require_sl":                True,   # block if no SL
    "require_tp":                False,  # warn if no TP (not block)
}

_RG_GUARDRAILS = {
    "auto_execution_allowed": False,
    "ai_can_execute":         False,
    "live_enabled":           False,
    "paper_primary":          True,
}


# ── Settings ──────────────────────────────────────────────────────────────────

def _lm_get_paper_risk_guard_settings(user_id) -> dict:
    """Load per-user risk guard limits from UserPreference. Returns defaults if not set."""
    try:
        from models import UserPreference as _UP
        pref = _UP.query.filter_by(user_id=user_id).first()
        if pref is None:
            return dict(_RG_DEFAULT_SETTINGS)
        raw = getattr(pref, "paper_risk_guard_settings_json", None)
        if not raw:
            return dict(_RG_DEFAULT_SETTINGS)
        loaded = _json_rg.loads(raw)
        if not isinstance(loaded, dict):
            return dict(_RG_DEFAULT_SETTINGS)
        merged = dict(_RG_DEFAULT_SETTINGS)
        merged.update({k: v for k, v in loaded.items() if k in _RG_DEFAULT_SETTINGS})
        return merged
    except Exception:
        return dict(_RG_DEFAULT_SETTINGS)


def _lm_update_paper_risk_guard_settings(user_id, settings_dict: dict) -> dict:
    """Persist per-user risk guard limits into UserPreference.paper_risk_guard_settings_json."""
    try:
        from models import db as _db_rg, UserPreference as _UP
        pref = _UP.query.filter_by(user_id=user_id).first()
        if pref is None:
            return {"ok": False, "error": "preference_not_found"}
        merged = dict(_RG_DEFAULT_SETTINGS)
        if isinstance(settings_dict, dict):
            merged.update({k: v for k, v in settings_dict.items() if k in _RG_DEFAULT_SETTINGS})
        if not hasattr(pref, "paper_risk_guard_settings_json"):
            return {"ok": False, "error": "column_not_migrated"}
        pref.paper_risk_guard_settings_json = _json_rg.dumps(merged)
        _db_rg.session.commit()
        return {"ok": True, "settings": merged}
    except Exception as _e:
        return {"ok": False, "error": str(_e)[:120]}


# ── Risk computation ──────────────────────────────────────────────────────────

def _lm_compute_paper_risk_metrics(
    entry: float, sl: float, tp: float, qty: float,
    direction: str, equity: float,
) -> dict:
    """Compute risk metrics for a paper order draft.

    Returns risk_per_unit, risk_amount, risk_pct, reward_per_unit, rr.
    direction: "LONG" or "SHORT" (case-insensitive).
    """
    d = (direction or "").upper()
    notional = entry * qty if entry > 0 else 0.0

    if d == "LONG":
        risk_per_unit   = (entry - sl)    if sl > 0 and entry > sl else 0.0
        reward_per_unit = (tp - entry)    if tp > 0 and tp > entry else 0.0
    elif d == "SHORT":
        risk_per_unit   = (sl - entry)    if sl > 0 and sl > entry else 0.0
        reward_per_unit = (entry - tp)    if tp > 0 and entry > tp  else 0.0
    else:
        risk_per_unit   = 0.0
        reward_per_unit = 0.0

    risk_amount = risk_per_unit * qty if risk_per_unit > 0 else 0.0
    risk_pct    = (risk_amount / equity * 100.0) if equity > 0 and risk_amount > 0 else 0.0
    rr          = (reward_per_unit / risk_per_unit) if risk_per_unit > 0 and reward_per_unit > 0 else None

    return {
        "direction":      d or "UNKNOWN",
        "entry_price":    round(entry, 8),
        "sl_price":       round(sl, 8),
        "tp_price":       round(tp, 8),
        "quantity":       round(qty, 8),
        "notional":       round(notional, 4),
        "risk_per_unit":  round(risk_per_unit, 8),
        "risk_amount":    round(risk_amount, 4),
        "risk_pct":       round(risk_pct, 4),
        "reward_per_unit": round(reward_per_unit, 8),
        "rr":             round(rr, 4) if rr is not None else None,
    }


# ── Main builder ──────────────────────────────────────────────────────────────

def _lm_build_paper_risk_guard(item, snapshot=None, quantity_str=None) -> dict:  # noqa: C901
    """Evaluate paper risk guard for an internal paper order.

    Phase 11.13. No execution. No exchange. No API keys.
    can_auto_submit is ALWAYS False.
    """
    now_ts = int(_time_rg.time())
    blocking_reasons: list[str] = []
    warnings:         list[str] = []

    # ── Load dependencies lazily ──────────────────────────────────────────────
    try:
        from live_monitor.execution_account import _lm_get_execution_settings
        from live_monitor.paper_trading import (
            _lm_get_paper_account_summary,
            _lm_build_paper_order_draft,
            _lm_get_paper_positions,
            _lm_get_real_market_price_for_paper,
        )
        from live_monitor.paper_auto_gate import _lm_get_paper_auto_gate_state
    except Exception as _imp_err:
        return {
            "ok":              False,
            "allowed":         False,
            "risk_status":     "blocked",
            "blocking_reasons": [f"import_error:{str(_imp_err)[:80]}"],
            "warnings":        [],
            "risk":            {},
            "limits":          dict(_RG_DEFAULT_SETTINGS),
            "open_state":      {},
            "guardrails":      dict(_RG_GUARDRAILS),
            "source":          "internal_paper_risk_guard",
            "phase":           "phase11_13_paper_risk_guard",
            "computed_at":     now_ts,
        }

    item_id  = getattr(item, "id", None)
    user_id  = getattr(item, "user_id", None)

    if snapshot is None:
        snap = {}
        try:
            raw = getattr(item, "snapshot_json", None)
            if raw:
                snap = _json_rg.loads(raw)
        except Exception:
            pass
    else:
        snap = snapshot if isinstance(snapshot, dict) else {}

    # ── Load settings ─────────────────────────────────────────────────────────
    limits = _lm_get_paper_risk_guard_settings(user_id) if user_id else dict(_RG_DEFAULT_SETTINGS)

    # ── RULE 1: execution mode must be internal_paper ─────────────────────────
    exec_mode = "internal_paper"
    try:
        if user_id:
            es = _lm_get_execution_settings(user_id)
            exec_mode = es.get("execution_mode", "internal_paper")
    except Exception:
        pass
    if exec_mode != "internal_paper":
        blocking_reasons.append("execution_mode_not_internal_paper")

    # ── RULE 2: paper account must be available ───────────────────────────────
    paper_account = {}
    account_ok = False
    try:
        if user_id:
            paper_account = _lm_get_paper_account_summary(user_id) or {}
            account_ok = bool(paper_account.get("status") in ("ok", "active", "open") or
                              "cash_balance" in paper_account)
    except Exception:
        pass
    if not account_ok:
        blocking_reasons.append("paper_account_unavailable")

    cash_balance = float(paper_account.get("cash_balance") or 0)
    equity       = float(paper_account.get("equity") or paper_account.get("cash_balance") or 0) or cash_balance

    # ── RULE 17: execution_intent must be present ─────────────────────────────
    intent_raw = snap.get("latest_execution_intent") or snap.get("execution_intent")
    if not intent_raw:
        blocking_reasons.append("execution_intent_missing")
    intent = intent_raw if isinstance(intent_raw, dict) else {}
    if not isinstance(intent_raw, dict) and intent_raw:
        try:
            intent = _json_rg.loads(intent_raw)
        except Exception:
            intent = {}

    direction = (intent.get("direction") or "").upper().strip()

    # ── RULE 5: direction must be LONG or SHORT ───────────────────────────────
    if direction not in ("LONG", "SHORT"):
        blocking_reasons.append("direction_missing")

    # ── RULE 6: entry price must be non-zero ─────────────────────────────────
    _entry_raw = (
        intent.get("entry_price") or intent.get("entry") or
        intent.get("limit_price")
    )
    entry_price = float(_entry_raw or 0)

    # ── Build order draft ─────────────────────────────────────────────────────
    draft = {}
    draft_ok = False
    try:
        draft = _lm_build_paper_order_draft(item, snapshot=snap, quantity_str=quantity_str) or {}
        draft_ok = bool(draft.get("ok"))
    except Exception:
        pass

    # ── RULE 3: draft must build ──────────────────────────────────────────────
    if not draft_ok:
        blocking_reasons.append("order_draft_unavailable")

    # ── RULE 4: draft must be paper_ready ────────────────────────────────────
    if draft_ok and not draft.get("paper_ready"):
        blocking_reasons.append("order_draft_not_ready")

    # Use draft entry if intent entry is missing
    if entry_price == 0 and draft_ok:
        entry_price = float(draft.get("entry_price") or 0)

    if entry_price == 0:
        blocking_reasons.append("entry_price_missing")

    # ── SL / TP from intent ───────────────────────────────────────────────────
    sl_price = float(intent.get("sl_price") or intent.get("stop_loss") or
                     intent.get("sl")       or 0)
    tp_price = float(intent.get("tp_price") or intent.get("take_profit") or
                     intent.get("tp")       or 0)

    # ── RULE 7: quantity must parse ───────────────────────────────────────────
    qty = 0.0
    qty_parse_ok = False
    if quantity_str is not None:
        try:
            qty = float(str(quantity_str).strip())
            qty_parse_ok = True
        except (ValueError, TypeError):
            blocking_reasons.append("quantity_parse_error")
    else:
        # Attempt from draft
        try:
            qty = float(draft.get("quantity") or 0)
            qty_parse_ok = qty > 0
        except Exception:
            pass

    # ── RULE 8: quantity must be positive ────────────────────────────────────
    if qty_parse_ok and qty <= 0:
        blocking_reasons.append("quantity_zero_or_negative")

    # ── Compute risk metrics ──────────────────────────────────────────────────
    risk_metrics = _lm_compute_paper_risk_metrics(
        entry=entry_price, sl=sl_price, tp=tp_price,
        qty=qty, direction=direction, equity=equity,
    )
    notional    = risk_metrics["notional"]
    risk_amount = risk_metrics["risk_amount"]
    risk_pct    = risk_metrics["risk_pct"]
    rr          = risk_metrics["rr"]

    # ── RULE 9: notional must not exceed max ──────────────────────────────────
    max_notional = float(limits.get("max_position_notional", 1000.0))
    if notional > 0 and notional > max_notional:
        blocking_reasons.append("notional_exceeds_max")

    # ── RULE 10: dollar risk must not exceed max ──────────────────────────────
    max_risk_amt = float(limits.get("max_risk_amount_per_trade", 100.0))
    if risk_amount > 0 and risk_amount > max_risk_amt:
        blocking_reasons.append("risk_amount_exceeds_max")

    # ── RULE 11: risk% must not exceed max ────────────────────────────────────
    max_risk_pct = float(limits.get("max_risk_pct_per_trade", 2.0))
    if risk_pct > 0 and risk_pct > max_risk_pct:
        blocking_reasons.append("risk_pct_exceeds_max")

    # ── RULE 12: sufficient cash balance ──────────────────────────────────────
    if notional > 0 and cash_balance > 0 and notional > cash_balance:
        blocking_reasons.append("insufficient_cash_balance")

    # ── RULE 13: open position direction conflict ──────────────────────────────
    open_positions_list = []
    open_pos_count      = 0
    has_conflict        = False
    conflict_dir        = None
    same_dir_count      = 0
    try:
        if user_id and item_id:
            positions = _lm_get_paper_positions(user_id, item_id=item_id) or []
            open_positions_list = [p for p in positions if (p.get("status") or "") in ("open", "active")]
            open_pos_count = len(open_positions_list)
            if direction in ("LONG", "SHORT"):
                opp_dir = "SHORT" if direction == "LONG" else "LONG"
                for p in open_positions_list:
                    p_side = (p.get("side") or p.get("direction") or "").upper()
                    if p_side == opp_dir:
                        has_conflict = True
                        conflict_dir = opp_dir
                    elif p_side == direction:
                        same_dir_count += 1
    except Exception:
        pass
    if has_conflict:
        blocking_reasons.append("open_position_direction_conflict")

    # ── RULE 14: SL required but missing ─────────────────────────────────────
    require_sl = bool(limits.get("require_sl", True))
    if require_sl and sl_price == 0:
        blocking_reasons.append("sl_required_but_missing")

    # ── RULE 15: RR below minimum ─────────────────────────────────────────────
    min_rr = float(limits.get("min_rr", 1.5))
    if rr is not None and rr < min_rr:
        blocking_reasons.append("rr_below_minimum")

    # ── RULE 16: AI trade control hard block ──────────────────────────────────
    _atc = snap.get("latest_ai_trade_control_decision") or snap.get("latest_ai_trade_control") or {}
    if not isinstance(_atc, dict):
        try:
            _atc = _json_rg.loads(_atc) if _atc else {}
        except Exception:
            _atc = {}
    _atc_action = (_atc.get("action") or _atc.get("decision") or "").lower()
    if _atc_action in ("block_trade", "pause_setup"):
        blocking_reasons.append("ai_trade_control_hard_blocked")

    # ── Warnings (non-blocking) ───────────────────────────────────────────────
    # SL missing (when not required)
    if not require_sl and sl_price == 0:
        warnings.append("sl_missing")

    # TP missing
    require_tp = bool(limits.get("require_tp", False))
    if tp_price == 0:
        if require_tp:
            blocking_reasons.append("tp_required_but_missing")
        else:
            warnings.append("tp_missing")

    # RR below ideal (between 1.0 and min_rr)
    if rr is not None and 1.0 <= rr < min_rr and "rr_below_minimum" not in blocking_reasons:
        warnings.append("rr_below_ideal")

    # Risk % elevated (> 70% of max but below max)
    if risk_pct > 0 and risk_pct > max_risk_pct * 0.7 and "risk_pct_exceeds_max" not in blocking_reasons:
        warnings.append("risk_pct_elevated")

    # Entry above market for LONG / below for SHORT
    market_price = None
    price_source = None
    try:
        if item:
            pr = _lm_get_real_market_price_for_paper(item)
            if pr and pr.get("ok"):
                market_price = float(pr.get("price") or 0) or None
                price_source = pr.get("price_source")
    except Exception:
        pass

    if market_price and entry_price > 0:
        pct_diff = abs(entry_price - market_price) / market_price * 100
        if direction == "LONG"  and entry_price > market_price * 1.005:
            warnings.append("entry_above_market_long")
        if direction == "SHORT" and entry_price < market_price * 0.995:
            warnings.append("entry_below_market_short")
    elif not market_price:
        warnings.append("market_price_unavailable")

    # Scale-in warning
    if same_dir_count > 0:
        warnings.append("open_position_same_direction")

    # ── Build final response ──────────────────────────────────────────────────
    allowed = len(blocking_reasons) == 0
    if not allowed:
        risk_status = "blocked"
    elif warnings:
        risk_status = "warning"
    else:
        risk_status = "allowed"

    risk_metrics["market_price"] = market_price
    risk_metrics["price_source"] = price_source

    return {
        "ok":              True,
        "allowed":         allowed,
        "risk_status":     risk_status,
        "blocking_reasons": blocking_reasons,
        "warnings":        warnings,
        "risk":            risk_metrics,
        "limits":          limits,
        "open_state": {
            "open_positions":     open_pos_count,
            "has_conflict":       has_conflict,
            "conflict_direction": conflict_dir,
            "same_direction_count": same_dir_count,
        },
        "guardrails":   dict(_RG_GUARDRAILS),
        "source":       "internal_paper_risk_guard",
        "phase":        "phase11_13_paper_risk_guard",
        "computed_at":  now_ts,
        # Stable helpers for consumers
        "allowed_actions": {
            "can_paper_manually_submit": allowed,
            "can_auto_submit":           False,  # ALWAYS
            "can_live_trade":            False,  # ALWAYS
        },
    }


# ── State reader ──────────────────────────────────────────────────────────────

def _lm_get_paper_risk_guard_state(item_id, user_id) -> dict:
    """Return stored paper risk guard result from item snapshot. Read-only."""
    try:
        from models import LiveMonitorItem as _LMI_rg
        item = _LMI_rg.query.filter_by(id=item_id, user_id=user_id).first()
        if item is None:
            return {"ok": False, "error": "item_not_found"}
        snap = {}
        try:
            raw = getattr(item, "snapshot_json", None)
            if raw:
                snap = _json_rg.loads(raw)
        except Exception:
            pass
        guard_result = snap.get("latest_paper_risk_guard_result") or {}
        evaluated    = bool(guard_result.get("ok"))
        allowed      = guard_result.get("allowed", False) if evaluated else False
        risk_status  = guard_result.get("risk_status", "not_evaluated")
        return {
            "ok":              True,
            "guard_evaluated": evaluated,
            "allowed":         allowed,
            "risk_status":     risk_status,
            "blocking_reasons": guard_result.get("blocking_reasons", []),
            "warnings":        guard_result.get("warnings", []),
            "risk":            guard_result.get("risk", {}),
            "limits":          guard_result.get("limits", {}),
            "open_state":      guard_result.get("open_state", {}),
            "guardrails":      guard_result.get("guardrails", dict(_RG_GUARDRAILS)),
            "allowed_actions": guard_result.get("allowed_actions", {
                "can_paper_manually_submit": False,
                "can_auto_submit":           False,
                "can_live_trade":            False,
            }),
            "latest_guard_result": guard_result,
            "source":          guard_result.get("source", "internal_paper_risk_guard"),
        }
    except Exception as _e:
        return {"ok": False, "error": str(_e)[:120]}


# ── Submit validator ──────────────────────────────────────────────────────────

def _lm_validate_paper_order_against_risk_guard(item, user_id, quantity_str) -> dict:
    """Run risk guard and return validation result for use by paper submit endpoint.

    Called before _lm_submit_paper_order. Returns {ok, allowed, risk_guard, ...}.
    """
    try:
        snap = {}
        try:
            raw = getattr(item, "snapshot_json", None)
            if raw:
                snap = _json_rg.loads(raw)
        except Exception:
            pass
        guard = _lm_build_paper_risk_guard(item, snapshot=snap, quantity_str=quantity_str)
        return {
            "ok":         True,
            "allowed":    guard.get("allowed", False),
            "risk_guard": guard,
        }
    except Exception as _e:
        return {
            "ok":         False,
            "allowed":    False,
            "risk_guard": {
                "ok":               False,
                "allowed":          False,
                "risk_status":      "blocked",
                "blocking_reasons": [f"risk_guard_error:{str(_e)[:80]}"],
                "warnings":         [],
                "guardrails":       dict(_RG_GUARDRAILS),
                "source":           "internal_paper_risk_guard",
                "phase":            "phase11_13_paper_risk_guard",
            },
        }
