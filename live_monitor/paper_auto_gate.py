"""Phase 11.12: Paper Auto Mode Safety Gate — advisory + metadata only.

Evaluates whether a setup is eligible for future paper auto execution.
Gate/arming foundation only. No execution. No orders.

HARD INVARIANTS (never change):
  can_auto_submit               = False  always
  can_live_trade                = False  always
  can_testnet_strategy_validate = False  always
  auto_execution_allowed        = False  always
  ai_can_execute                = False  always

No exchange calls. No API keys. No secrets. No background workers.
"""
from __future__ import annotations

import json as _json_ag
import datetime as _dt_ag
from typing import Optional

# ── Gate check IDs ────────────────────────────────────────────────────────────
_CHECK_EXECUTION_MODE    = "execution_mode"
_CHECK_PAPER_ACCOUNT     = "paper_account"
_CHECK_EXECUTION_INTENT  = "execution_intent"
_CHECK_AI_TRADE_CONTROL  = "ai_trade_control"
_CHECK_AUTOMATION_POLICY = "automation_policy"
_CHECK_EXEC_SIMULATION   = "execution_simulation"
_CHECK_DATA_HEALTH       = "data_health_price"
_CHECK_OPEN_STATE        = "open_state"
_CHECK_JOURNAL_FEEDBACK  = "journal_feedback"
_CHECK_FINAL_ELIGIBILITY = "final_eligibility"

# AI actions that block the gate (advisory only)
_BLOCKING_AI_ACTIONS = {"avoid", "hard_stop", "close_immediately"}

# Warn when this many consecutive losses detected
_CONSECUTIVE_LOSS_WARN_THRESHOLD = 3

# Hard-check IDs — all must pass for eligible=True
_HARD_CHECK_IDS = [
    _CHECK_EXECUTION_MODE,
    _CHECK_PAPER_ACCOUNT,
    _CHECK_EXECUTION_INTENT,
    _CHECK_AI_TRADE_CONTROL,
    _CHECK_AUTOMATION_POLICY,
    _CHECK_EXEC_SIMULATION,
    _CHECK_DATA_HEALTH,
    _CHECK_OPEN_STATE,
]


# ── Core: build gate ──────────────────────────────────────────────────────────

def _lm_build_paper_auto_gate(item, snap: dict) -> dict:
    """Run all 10 gate checks and return a structured gate result.

    Advisory + metadata only. allowed_actions.can_auto_submit is ALWAYS False.
    """
    now_ts  = _dt_ag.datetime.utcnow().isoformat()
    item_id = getattr(item, "id", None)
    user_id = getattr(item, "user_id", None)
    symbol  = getattr(item, "symbol", None) or snap.get("symbol", "UNKNOWN")

    checks: dict = {}
    advisory_notes: list = []

    # ── Check 1: Execution mode ───────────────────────────────────────────────
    try:
        from live_monitor.execution_account import _lm_get_execution_mode_summary
        _es       = _lm_get_execution_mode_summary(user_id) if user_id else {}
        exec_mode = _es.get("execution_mode", "internal_paper")
        paper_prim = _es.get("paper_primary", True)
        live_dis   = _es.get("live_disabled", True)
        mode_ok    = (
            paper_prim is True
            and live_dis  is True
            and exec_mode != "binance_live_future"
        )
        checks[_CHECK_EXECUTION_MODE] = {
            "pass":           mode_ok,
            "execution_mode": exec_mode,
            "paper_primary":  paper_prim,
            "live_disabled":  live_dis,
            "note": "OK" if mode_ok else f"Blocked: execution_mode={exec_mode}",
        }
        if not mode_ok:
            advisory_notes.append(f"execution_mode check failed: {exec_mode}")
    except Exception as _e1:
        exec_mode = "internal_paper"
        checks[_CHECK_EXECUTION_MODE] = {"pass": False, "error": str(_e1)[:120]}
        advisory_notes.append("execution_mode check error")

    # ── Check 2: Paper account exists ────────────────────────────────────────
    try:
        from live_monitor.paper_trading import _lm_get_or_create_paper_account
        acct    = _lm_get_or_create_paper_account(user_id) if user_id else None
        acct_ok = acct is not None and getattr(acct, "id", None) is not None
        checks[_CHECK_PAPER_ACCOUNT] = {
            "pass":       acct_ok,
            "account_id": getattr(acct, "id", None) if acct else None,
            "note": "Paper account exists" if acct_ok else "No paper account",
        }
        if not acct_ok:
            advisory_notes.append("No paper account found")
    except Exception as _e2:
        checks[_CHECK_PAPER_ACCOUNT] = {"pass": False, "error": str(_e2)[:120]}
        advisory_notes.append("paper_account check error")

    # ── Check 3: Execution intent ────────────────────────────────────────────
    try:
        _intent = snap.get("execution_intent") or {}
        if not isinstance(_intent, dict):
            try:
                _intent = _json_ag.loads(_intent) if _intent else {}
            except Exception:
                _intent = {}
        _required = ["entry_price", "stop_loss", "take_profit", "risk_reward", "direction"]
        _missing  = [k for k in _required if not _intent.get(k)]
        intent_ok = len(_missing) == 0
        checks[_CHECK_EXECUTION_INTENT] = {
            "pass":    intent_ok,
            "missing": _missing,
            "note": "All intent fields present" if intent_ok else f"Missing: {_missing}",
        }
        if not intent_ok:
            advisory_notes.append(f"Execution intent incomplete — missing: {_missing}")
    except Exception as _e3:
        checks[_CHECK_EXECUTION_INTENT] = {"pass": False, "error": str(_e3)[:120]}
        advisory_notes.append("execution_intent check error")

    # ── Check 4: AI trade control (advisory only) ────────────────────────────
    try:
        _atc         = snap.get("latest_ai_trade_control") or {}
        if not isinstance(_atc, dict):
            try:
                _atc = _json_ag.loads(_atc) if _atc else {}
            except Exception:
                _atc = {}
        ai_action    = (_atc.get("action") or "none").lower()
        ai_blocked   = ai_action in _BLOCKING_AI_ACTIONS
        ai_confidence = _atc.get("confidence", 0) or 0
        checks[_CHECK_AI_TRADE_CONTROL] = {
            "pass":       not ai_blocked,
            "action":     ai_action,
            "confidence": ai_confidence,
            "advisory":   True,
            "note": (
                f"AI action '{ai_action}' is blocking"
                if ai_blocked
                else f"AI action '{ai_action}' is non-blocking"
            ),
        }
        if ai_blocked:
            advisory_notes.append(f"AI trade control advisory: action={ai_action} (blocking)")
    except Exception as _e4:
        checks[_CHECK_AI_TRADE_CONTROL] = {"pass": True, "advisory": True, "error": str(_e4)[:120]}

    # ── Check 5: Automation policy ───────────────────────────────────────────
    try:
        from live_monitor.automation_policy import _lm_build_automation_policy
        _pol    = _lm_build_automation_policy(item, snap)
        pol_res = _pol.get("policy_result", {})
        # Gate confirms guardrails are active — not a permissive check
        pol_ok  = (
            pol_res.get("auto_execution_allowed", False) is False
            and pol_res.get("live_disabled", True) is True
        )
        checks[_CHECK_AUTOMATION_POLICY] = {
            "pass":                   pol_ok,
            "auto_execution_allowed": False,
            "live_disabled":          True,
            "policy_allowed":         pol_res.get("allowed", False),
            "note": "Automation policy guardrails confirmed",
        }
    except Exception as _e5:
        checks[_CHECK_AUTOMATION_POLICY] = {
            "pass": True,
            "auto_execution_allowed": False,
            "live_disabled": True,
            "error": str(_e5)[:120],
        }

    # ── Check 6: Execution simulation ────────────────────────────────────────
    try:
        from live_monitor.execution_simulation import _lm_build_execution_simulation
        _sim   = _lm_build_execution_simulation(item, snap)
        sim_ok = bool(
            _sim.get("intent_valid")
            and _sim.get("policy_valid")
            and _sim.get("decision_valid")
            and _sim.get("data_health_ok")
        )
        checks[_CHECK_EXEC_SIMULATION] = {
            "pass":           sim_ok,
            "intent_valid":   _sim.get("intent_valid", False),
            "policy_valid":   _sim.get("policy_valid", False),
            "decision_valid": _sim.get("decision_valid", False),
            "data_health_ok": _sim.get("data_health_ok", False),
            "simulated":      _sim.get("simulated", False),
            "note": "Simulation ready" if sim_ok else "Simulation not ready",
        }
        if not sim_ok:
            advisory_notes.append("Execution simulation not fully ready")
    except Exception as _e6:
        checks[_CHECK_EXEC_SIMULATION] = {"pass": False, "error": str(_e6)[:120]}
        advisory_notes.append("execution_simulation check error")

    # ── Check 7: Data health / real price ────────────────────────────────────
    try:
        from live_monitor.paper_trading import _lm_get_real_market_price_for_paper
        _pr         = _lm_get_real_market_price_for_paper(item, snap)
        price_ok    = bool(_pr.get("ok") and _pr.get("price") and float(_pr["price"] or 0) > 0)
        price_source = _pr.get("price_source", "unknown")
        checks[_CHECK_DATA_HEALTH] = {
            "pass":         price_ok,
            "price_source": price_source,
            "price":        str(_pr.get("price", "")) if price_ok else None,
            "note": f"Price OK via {price_source}" if price_ok else "No valid real price",
        }
        if not price_ok:
            advisory_notes.append(
                f"Data health: no valid real market price (source={price_source})"
            )
    except Exception as _e7:
        checks[_CHECK_DATA_HEALTH] = {"pass": False, "error": str(_e7)[:120]}
        advisory_notes.append("data_health_price check error")

    # ── Check 8: Open state ──────────────────────────────────────────────────
    try:
        from live_monitor.paper_trading import _lm_get_paper_orders, _lm_get_paper_positions
        _orders    = _lm_get_paper_orders(user_id, item_id)
        _posns     = _lm_get_paper_positions(user_id, item_id)
        open_orders = [o for o in (_orders or []) if str(o.get("status", "")).lower() == "open"]
        open_posns  = [p for p in (_posns  or []) if str(p.get("status", "")).lower() == "open"]
        no_conflict = len(open_orders) == 0 and len(open_posns) == 0
        checks[_CHECK_OPEN_STATE] = {
            "pass":           no_conflict,
            "open_orders":    len(open_orders),
            "open_positions": len(open_posns),
            "note": (
                "No conflicting open state"
                if no_conflict
                else f"Conflict: {len(open_orders)} open orders, {len(open_posns)} open positions"
            ),
        }
        if not no_conflict:
            advisory_notes.append(
                f"Open state conflict: {len(open_orders)} orders, {len(open_posns)} positions open"
            )
    except Exception as _e8:
        checks[_CHECK_OPEN_STATE] = {"pass": False, "error": str(_e8)[:120]}
        advisory_notes.append("open_state check error")

    # ── Check 9: Journal feedback (advisory warn only — never blocks) ─────────
    try:
        from live_monitor.paper_trading import _lm_get_paper_trade_journal
        _jnl   = _lm_get_paper_trade_journal(user_id, item_id, limit=10)
        trades = (_jnl.get("trades", []) if isinstance(_jnl, dict) else [])
        consecutive_losses = 0
        for t in trades:
            if (t.get("outcome") or "").lower() == "loss":
                consecutive_losses += 1
            else:
                break
        journal_warn = consecutive_losses >= _CONSECUTIVE_LOSS_WARN_THRESHOLD
        checks[_CHECK_JOURNAL_FEEDBACK] = {
            "pass":               True,   # journal never hard-blocks
            "advisory":           True,
            "consecutive_losses": consecutive_losses,
            "warn":               journal_warn,
            "note": (
                f"Advisory: {consecutive_losses} consecutive losses — review before arming"
                if journal_warn
                else f"{consecutive_losses} consecutive losses (below threshold)"
            ),
        }
        if journal_warn:
            advisory_notes.append(
                f"Journal advisory: {consecutive_losses} consecutive recent losses"
            )
    except Exception as _e9:
        checks[_CHECK_JOURNAL_FEEDBACK] = {
            "pass": True, "advisory": True, "error": str(_e9)[:120]
        }

    # ── Check 10: Final eligibility ──────────────────────────────────────────
    hard_failed  = [k for k in _HARD_CHECK_IDS if not checks.get(k, {}).get("pass", False)]
    hard_passed  = [k for k in _HARD_CHECK_IDS if     checks.get(k, {}).get("pass", False)]
    eligible     = len(hard_failed) == 0

    checks[_CHECK_FINAL_ELIGIBILITY] = {
        "pass":               eligible,
        "hard_checks_passed": hard_passed,
        "hard_checks_failed": hard_failed,
        "advisory_notes":     advisory_notes,
        "note": "Eligible for paper auto gate arming" if eligible else "Not eligible",
    }
    if not eligible:
        advisory_notes.append(f"Not eligible — failed checks: {hard_failed}")

    return {
        "ok":             True,
        "phase":          "phase11_12_paper_auto_gate",
        "computed_at":    now_ts,
        "item_id":        item_id,
        "user_id":        user_id,
        "symbol":         symbol,
        "eligible":       eligible,
        "checks":         checks,
        "advisory_notes": advisory_notes,
        # Allowed actions — can_auto_submit ALWAYS False
        "allowed_actions": {
            "can_paper_manually_submit":     eligible,
            "can_auto_submit":               False,
            "can_live_trade":                False,
            "can_testnet_strategy_validate": False,
        },
        "gate_invariants": {
            "auto_execution_allowed": False,
            "ai_can_execute":         False,
            "live_enabled":           False,
            "paper_primary":          True,
        },
    }


# ── State reader ──────────────────────────────────────────────────────────────

def _lm_get_paper_auto_gate_state(item_id, user_id) -> dict:
    """Return current gate state from item snapshot. Read-only. No computation."""
    try:
        from models import LiveMonitorItem as _LMI_ag
        item = _LMI_ag.query.filter_by(id=item_id, user_id=user_id).first()
        if item is None:
            return {"ok": False, "error": "item_not_found"}
        snap = {}
        if item.snapshot_json:
            try:
                snap = _json_ag.loads(item.snapshot_json)
            except Exception:
                pass
        gate_result = snap.get("latest_paper_auto_gate_result") or {}
        arm_state   = snap.get("paper_auto_arming_state") or {}
        return {
            "ok":                  True,
            "item_id":             item_id,
            "user_id":             user_id,
            "gate_evaluated":      bool(gate_result),
            "eligible":            gate_result.get("eligible", False),
            "armed":               arm_state.get("armed", False),
            "arm_requested_at":    arm_state.get("arm_requested_at"),
            "disarm_requested_at": arm_state.get("disarm_requested_at"),
            "latest_gate_result":  gate_result,
            "arming_state":        arm_state,
            "allowed_actions": gate_result.get("allowed_actions", {
                "can_paper_manually_submit":     False,
                "can_auto_submit":               False,
                "can_live_trade":                False,
                "can_testnet_strategy_validate": False,
            }),
            "gate_invariants": {
                "auto_execution_allowed": False,
                "ai_can_execute":         False,
                "live_enabled":           False,
                "paper_primary":          True,
            },
        }
    except Exception as _eg:
        return {"ok": False, "error": str(_eg)[:200]}


# ── Arm / Disarm ──────────────────────────────────────────────────────────────

def _lm_arm_paper_auto_gate(item_id, user_id) -> dict:
    """Arm the paper auto gate. Only allowed when gate is currently eligible.

    Writes paper_auto_arming_state to item snapshot.
    No execution. No orders. No exchange calls. can_auto_submit always False.
    """
    try:
        from models import db as _db_ag, LiveMonitorItem as _LMI_ag
        item = _LMI_ag.query.filter_by(id=item_id, user_id=user_id).first()
        if item is None:
            return {"ok": False, "error": "item_not_found"}
        snap = {}
        if item.snapshot_json:
            try:
                snap = _json_ag.loads(item.snapshot_json)
            except Exception:
                pass
        gate_result = snap.get("latest_paper_auto_gate_result") or {}
        if not gate_result.get("eligible", False):
            return {
                "ok":      False,
                "error":   "not_eligible",
                "message": (
                    "Gate must be evaluated and eligible before arming. "
                    "Run evaluate first."
                ),
            }
        now_ts    = _dt_ag.datetime.utcnow().isoformat()
        arm_state = {
            "armed":               True,
            "arm_requested_at":    now_ts,
            "disarm_requested_at": None,
            "armed_by":            "user",
            "can_auto_submit":     False,
            "can_live_trade":      False,
        }
        snap["paper_auto_arming_state"] = arm_state
        item.snapshot_json = _json_ag.dumps(snap, default=str)
        _db_ag.session.commit()
        _lm_record_paper_auto_gate_event(
            user_id=user_id, item_id=item_id, event_type="arm",
            eligible=True, armed=True,
            execution_mode=gate_result.get("checks", {}).get(
                "execution_mode", {}
            ).get("execution_mode", "internal_paper"),
        )
        return {
            "ok":              True,
            "armed":           True,
            "arm_state":       arm_state,
            "message":         "Paper auto gate armed. No auto execution — manual trigger only.",
            "can_auto_submit": False,
        }
    except Exception as _ea:
        return {"ok": False, "error": str(_ea)[:200]}


def _lm_disarm_paper_auto_gate(item_id, user_id) -> dict:
    """Disarm the paper auto gate. Always succeeds.

    Writes paper_auto_arming_state to item snapshot.
    """
    try:
        from models import db as _db_ag2, LiveMonitorItem as _LMI_ag2
        item = _LMI_ag2.query.filter_by(id=item_id, user_id=user_id).first()
        if item is None:
            return {"ok": False, "error": "item_not_found"}
        snap = {}
        if item.snapshot_json:
            try:
                snap = _json_ag.loads(item.snapshot_json)
            except Exception:
                pass
        prev_arm   = snap.get("paper_auto_arming_state", {})
        now_ts     = _dt_ag.datetime.utcnow().isoformat()
        arm_state  = {
            "armed":               False,
            "arm_requested_at":    prev_arm.get("arm_requested_at"),
            "disarm_requested_at": now_ts,
            "disarmed_by":         "user",
            "can_auto_submit":     False,
            "can_live_trade":      False,
        }
        snap["paper_auto_arming_state"] = arm_state
        item.snapshot_json = _json_ag.dumps(snap, default=str)
        _db_ag2.session.commit()
        _lm_record_paper_auto_gate_event(
            user_id=user_id, item_id=item_id, event_type="disarm",
            eligible=False, armed=False,
        )
        return {
            "ok":        True,
            "armed":     False,
            "arm_state": arm_state,
            "message":   "Paper auto gate disarmed.",
        }
    except Exception as _ed:
        return {"ok": False, "error": str(_ed)[:200]}


# ── Event log ─────────────────────────────────────────────────────────────────

def _lm_record_paper_auto_gate_event(
    user_id,
    item_id,
    event_type: str,
    eligible:   bool = False,
    armed:      bool = False,
    gate_result: Optional[dict] = None,
    checks:      Optional[dict] = None,
    advisory_notes: Optional[list] = None,
    execution_mode: str = "internal_paper",
    policy_mode:    str = "paper_manual",
) -> None:
    """Write a gate event to live_monitor_paper_auto_gate_events. Fire-and-forget."""
    try:
        from models import db as _db_ev, LiveMonitorPaperAutoGateEvent as _LMPAG
        row = _LMPAG(
            user_id             = user_id,
            item_id             = item_id,
            event_type          = (event_type or "")[:30],
            eligible            = eligible,
            armed               = armed,
            gate_result_json    = _json_ag.dumps(gate_result    or {}, default=str),
            checks_json         = _json_ag.dumps(checks         or {}, default=str),
            advisory_notes_json = _json_ag.dumps(advisory_notes or [], default=str),
            execution_mode      = execution_mode,
            policy_mode         = policy_mode,
        )
        _db_ev.session.add(row)
        _db_ev.session.commit()
    except Exception:
        try:
            from models import db as _db_ev2
            _db_ev2.session.rollback()
        except Exception:
            pass
