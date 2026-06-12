"""Phase 11.4: Automation Policy Layer.

Evaluates whether a Phase 11.3 AI trade-control decision would be allowed
to affect a setup under the current automation policy configuration.

Policy evaluation only — no order placement, no Binance Testnet,
no execution, no automation applied, no live mode switching.
Routes stay in main.py. Evidence-only advisory layer.
"""
from __future__ import annotations
import os
import time

import main as _m

# ── Policy mode constants ─────────────────────────────────────────────────────
POLICY_MODE_PROPOSAL_ONLY       = "proposal_only"
POLICY_MODE_AUTO_TESTNET        = "auto_testnet"
POLICY_MODE_FUTURE_LIVE         = "future_live_execution"

# Phase 11.4 supported modes only — future_live_execution hard-disabled
_SUPPORTED_POLICY_MODES = {POLICY_MODE_PROPOSAL_ONLY, POLICY_MODE_AUTO_TESTNET}

# ── Entry mode constants ──────────────────────────────────────────────────────
ENTRY_MODE_TOUCH_LIMIT  = "touch_limit"
ENTRY_MODE_CONFIRMATION = "confirmation"
ENTRY_MODE_AI_CONTROLLED = "ai_controlled"

_VALID_ENTRY_MODES = {ENTRY_MODE_TOUCH_LIMIT, ENTRY_MODE_CONFIRMATION, ENTRY_MODE_AI_CONTROLLED}

# ── AI action → recommended entry mode mapping ────────────────────────────────
_ACTION_MODE_MAP = {
    "allow_touch_limit":       ENTRY_MODE_TOUCH_LIMIT,
    "switch_to_confirmation":  ENTRY_MODE_CONFIRMATION,
    "block_trade":             None,   # blocking — no mode
    "pause_setup":             None,   # blocking — no mode
    "reduce_size":             None,   # execution concern — no mode change
    "wait":                    None,   # hold current
    "no_action":               None,   # hold current
}


def _lm_build_automation_policy(item, snapshot=None) -> dict:  # noqa: C901
    """Build Phase 11.4 Automation Policy context and evaluation.

    Evaluates whether the current AI trade-control decision (Phase 11.3)
    would be allowed to affect the setup under the configured policy mode
    and entry mode.

    Phase 11.4 evaluates automation policy only.
    No execution authority exists.
    """
    now_ts = int(time.time())
    try:
        snap = (snapshot if isinstance(snapshot, dict)
                else _m._json_loads_safe(getattr(item, "snapshot_json", None), {}))

        # ── Policy configuration (from snapshot or defaults) ─────────────────
        policy_cfg  = (snap.get("automation_policy") or {})
        policy_mode = str(policy_cfg.get("policy_mode", POLICY_MODE_PROPOSAL_ONLY))
        entry_mode  = str(policy_cfg.get("entry_mode",  ENTRY_MODE_TOUCH_LIMIT))

        # Hard lock: future_live_execution is always disabled in Phase 11.4
        if policy_mode == POLICY_MODE_FUTURE_LIVE:
            policy_mode = POLICY_MODE_PROPOSAL_ONLY

        # Sanitise unknown values
        if policy_mode not in _SUPPORTED_POLICY_MODES:
            policy_mode = POLICY_MODE_PROPOSAL_ONLY
        if entry_mode not in _VALID_ENTRY_MODES:
            entry_mode = ENTRY_MODE_TOUCH_LIMIT

        # ── Resolve AI decision (Phase 11.3) ─────────────────────────────────
        ai_decision = snap.get("latest_ai_trade_control_decision") or {}
        if not ai_decision or not ai_decision.get("ok"):
            try:
                from live_monitor.ai_trade_control import _lm_build_ai_trade_control_decision
                ai_exec_ctx = snap.get("latest_ai_execution_context") or None
                ai_decision = _lm_build_ai_trade_control_decision(
                    item, snap, ai_exec_ctx=ai_exec_ctx
                ) or {}
            except Exception as _eb:
                ai_decision = {
                    "ok": False, "action": "no_action", "confidence": 0,
                    "danger_level": "unknown", "danger_score": 0,
                    "primary_reasons": [f"ai_decision_unavailable:{str(_eb)[:40]}"],
                    "risk_factors": [], "advisory_note": "",
                }

        ai_action       = str(ai_decision.get("action", "no_action"))
        ai_confidence   = int(ai_decision.get("confidence", 0))
        ai_danger_level = str(ai_decision.get("danger_level", "low"))
        ai_danger_score = int(ai_decision.get("danger_score", 0))

        # ── Danger context from Phase 11.2 ────────────────────────────────────
        ai_exec_ctx = snap.get("latest_ai_execution_context") or {}
        danger_ctx  = (ai_exec_ctx.get("danger_context") or {})
        danger_level_exec = str(danger_ctx.get("danger_level", ai_danger_level))
        blocking_reasons_danger = list(danger_ctx.get("blocking_reasons") or [])
        rec_behavior = str(danger_ctx.get("recommended_behavior", "wait"))

        # ── Data health ───────────────────────────────────────────────────────
        data_health_ctx  = (ai_exec_ctx.get("data_health") or {})
        data_gate        = (data_health_ctx.get("ai_data_gate") or {})
        data_gate_blocked = not bool(data_gate.get("allowed", True))
        gate_reasons      = list(data_gate.get("reasons") or [])

        # ── Orderflow quality ─────────────────────────────────────────────────
        of_quality_ctx = (ai_exec_ctx.get("orderflow_history_quality") or {})
        of_quality     = str(of_quality_ctx.get("overall", "insufficient"))
        of_limitations = list(of_quality_ctx.get("limitations") or [])

        # ── Risk guard context (derived from item status) ─────────────────────
        item_status = str(getattr(item, "status", None) or "watching").lower()
        rg_blocked  = item_status in getattr(_m, "_LM_RG_HARD_BLOCK_STATES", {"high_risk", "avoid"})
        risk_guard_ctx = {
            "available":    False,
            "status":       item_status,
            "blocked":      rg_blocked,
            "note":         "Risk Guard runs as part of AI proposal flow; "
                            "not available for standalone policy evaluation.",
        }

        # ── Policy context (Task 4) ───────────────────────────────────────────
        policy_context = {
            "policy_mode": policy_mode,
            "entry_mode":  entry_mode,
            "ai_decision": {
                "action":          ai_action,
                "confidence":      ai_confidence,
                "danger_level":    ai_danger_level,
                "danger_score":    ai_danger_score,
                "primary_reasons": (ai_decision.get("primary_reasons")  or [])[:5],
                "risk_factors":    (ai_decision.get("risk_factors")     or [])[:5],
                "advisory_note":   str(ai_decision.get("advisory_note") or ""),
            },
            "danger_context": {
                "danger_level":          danger_level_exec,
                "danger_score":          ai_danger_score,
                "blocking_reasons":      blocking_reasons_danger[:5],
                "recommended_behavior":  rec_behavior,
            },
            "risk_guard_context": risk_guard_ctx,
            "data_health": {
                "gate_allowed": bool(data_gate.get("allowed", True)),
                "gate_blocked": data_gate_blocked,
                "gate_reasons": gate_reasons[:5],
            },
            "orderflow_quality": {
                "overall":     of_quality,
                "limitations": of_limitations[:3],
            },
            "supported_modes": {
                "policy_modes_active":   list(_SUPPORTED_POLICY_MODES),
                "entry_modes_active":    list(_VALID_ENTRY_MODES),
                "future_live_execution": "hard_disabled_phase11_4",
            },
            "binance_testnet_connector": {
                "connector_ready": bool(
                    os.environ.get("BINANCE_TESTNET_API_KEY")
                    and os.environ.get("BINANCE_TESTNET_API_SECRET")
                ),
                "testnet_locked": True,
                "note": "Phase 11.5 read-only connector. No order placement.",
            },
            "phase11_4_note": (
                "Phase 11.4 evaluates automation policy only. "
                "No execution authority exists."
            ),
        }

        # ── Policy evaluation (Tasks 5-7) ─────────────────────────────────────
        allowed          = True
        blocking_reasons: list = []
        future_action    = "none"
        recommended_mode = entry_mode

        # Rule 1: data health gate blocked → block
        if data_gate_blocked:
            allowed = False
            blocking_reasons.append("data_health_gate_blocked")
            future_action = "block_trade"

        # Rule 2: danger level blocked → block
        if allowed and danger_level_exec == "blocked":
            allowed = False
            for br in blocking_reasons_danger[:3]:
                blocking_reasons.append(br)
            if not blocking_reasons:
                blocking_reasons.append("danger_level_blocked")
            future_action = "block_trade"

        # Rule 3: risk guard blocked → block
        if allowed and rg_blocked:
            allowed = False
            blocking_reasons.append(f"risk_guard_status_{item_status}")
            future_action = "block_trade"

        # Rule 4: orderflow quality insufficient → block
        if allowed and of_quality in ("insufficient", "none"):
            allowed = False
            blocking_reasons.append(f"orderflow_quality_{of_quality}")
            future_action = "block_trade"

        # Rule 5: AI decision-based evaluation (Task 6 AI controlled logic)
        if allowed:
            if ai_action == "block_trade":
                allowed = False
                blocking_reasons.append("ai_decision_block_trade")
                future_action = "block_trade"

            elif ai_action == "pause_setup":
                allowed = False
                blocking_reasons.append("ai_decision_pause_setup")
                future_action = "pause_setup"

            elif ai_action == "switch_to_confirmation":
                # Mode switch evaluation (Task 6 example logic)
                if entry_mode == ENTRY_MODE_TOUCH_LIMIT:
                    future_action    = "switch_mode"
                    recommended_mode = ENTRY_MODE_CONFIRMATION
                elif entry_mode == ENTRY_MODE_AI_CONTROLLED:
                    future_action    = "switch_mode"
                    recommended_mode = ENTRY_MODE_CONFIRMATION
                else:
                    # Already in confirmation — no change needed
                    future_action    = "none"
                    recommended_mode = entry_mode

            elif ai_action == "allow_touch_limit":
                if entry_mode != ENTRY_MODE_TOUCH_LIMIT:
                    future_action    = "switch_mode"
                    recommended_mode = ENTRY_MODE_TOUCH_LIMIT
                else:
                    future_action    = "none"
                    recommended_mode = ENTRY_MODE_TOUCH_LIMIT

            elif ai_action == "reduce_size":
                # Size management is an execution concern — no entry mode change
                future_action    = "none"
                recommended_mode = entry_mode

            else:  # wait, no_action
                future_action    = "none"
                recommended_mode = entry_mode

        # ── Reason string ─────────────────────────────────────────────────────
        if not allowed:
            reason = (
                "Policy blocked: "
                + "; ".join(blocking_reasons[:3])
                + ". No action taken — Phase 11.4 evaluates only."
            )
        elif future_action == "switch_mode":
            reason = (
                f"AI recommends switching entry mode from '{entry_mode}' to "
                f"'{recommended_mode}'. Policy allows this change under "
                f"'{policy_mode}' mode. Not applied — Phase 11.4 evaluates only."
            )
        elif future_action == "none":
            reason = (
                f"Policy allows current entry mode '{recommended_mode}' with "
                f"no change required. AI action '{ai_action}' is compatible."
            )
        else:
            reason = (
                f"Policy evaluation complete. Future action: {future_action}. "
                f"Not applied — Phase 11.4 evaluates only."
            )

        # ── Future-ready storage fields (Task 13) ─────────────────────────────
        future_ready_state = {
            "snapshot_current_entry_mode":  entry_mode,
            "snapshot_current_policy_mode": policy_mode,
            "ai_recommended_action":        ai_action,
            "policy_future_action":         future_action,
            "policy_recommended_mode":      recommended_mode,
            "policy_allowed":               allowed,
            "apply_pending":                False,  # never applied in Phase 11.4
            "note":                         "Phase 11.4 only. apply_pending remains False.",
        }

        # Phase 11.11: Execution mode guardrails — paper is ALWAYS primary
        _exec_mode = "internal_paper"
        _uid = getattr(item, "user_id", None)
        try:
            if _uid:
                from live_monitor.execution_account import _lm_get_execution_mode_summary
                _es = _lm_get_execution_mode_summary(_uid)
                _exec_mode = _es.get("execution_mode", "internal_paper")
        except Exception:
            pass

        # Phase 11.12: Gate awareness (read-only — never executes)
        _gate_eligible = False
        _gate_armed    = False
        _gate_phase    = "not_evaluated"
        try:
            if _uid:
                _item_id_ap = getattr(item, "id", None)
                if _item_id_ap:
                    from live_monitor.paper_auto_gate import _lm_get_paper_auto_gate_state
                    _gs = _lm_get_paper_auto_gate_state(_item_id_ap, _uid)
                    if _gs.get("ok"):
                        _gate_eligible = _gs.get("eligible", False)
                        _gate_armed    = _gs.get("armed", False)
                        _gate_phase    = (_gs.get("latest_gate_result") or {}).get(
                            "phase", "not_evaluated"
                        )
        except Exception:
            pass

        # Phase 11.13: Risk Guard awareness (read-only — never executes)
        _rg_allowed     = False
        _rg_status      = "not_evaluated"
        _rg_block_count = 0
        try:
            if _uid:
                _item_id_rg = getattr(item, "id", None)
                if _item_id_rg:
                    from live_monitor.paper_risk_guard import _lm_get_paper_risk_guard_state
                    _rgs = _lm_get_paper_risk_guard_state(_item_id_rg, _uid)
                    if _rgs.get("ok"):
                        _rg_allowed     = _rgs.get("allowed", False)
                        _rg_status      = _rgs.get("risk_status", "not_evaluated")
                        _rg_block_count = len(_rgs.get("blocking_reasons", []))
        except Exception:
            pass

        policy_result = {
            "allowed":               allowed,
            "recommended_mode":      recommended_mode,
            "future_action":         future_action,
            "reason":                reason,
            "blocking_reasons":      blocking_reasons,
            "current_entry_mode":    entry_mode,
            "current_policy_mode":   policy_mode,
            "future_ready_state":    future_ready_state,
            "advisory_note":         (
                "Phase 11.4 evaluates automation policy only. "
                "No execution authority exists."
            ),
            # Phase 11.11 execution mode guardrails
            "execution_mode":                    _exec_mode,
            "policy_mode":                       policy_mode,
            # paper_primary is ALWAYS true regardless of selected execution mode
            "paper_primary":                     True,
            "primary_strategy_testing_mode":     "internal_paper",
            "testnet_strategy_validation":       False,
            "live_disabled":                     True,
            "auto_execution_allowed":            False,
            # Phase 11.12 gate awareness
            "paper_auto_gate_status":            _gate_phase,
            "paper_auto_gate_eligible":          _gate_eligible,
            "paper_auto_gate_armed":             _gate_armed,
            # Phase 11.13 risk guard awareness
            "paper_risk_guard_status":           _rg_status,
            "paper_risk_guard_allowed":          _rg_allowed,
            "paper_risk_guard_blocking_reason_count": _rg_block_count,
        }

        return {
            "ok":             True,
            "phase":          "phase11_4_automation_policy",
            "computed_at":    now_ts,
            "policy_context": policy_context,
            "policy_result":  policy_result,
        }

    except Exception as _e114:
        return {
            "ok":     False,
            "phase":  "phase11_4_automation_policy",
            "computed_at": now_ts,
            "error":  str(_e114)[:200],
            "policy_context": {},
            "policy_result": {
                "allowed":          False,
                "recommended_mode": ENTRY_MODE_CONFIRMATION,
                "future_action":    "none",
                "reason":           f"Policy build error: {str(_e114)[:80]}",
                "blocking_reasons": ["build_error"],
                "current_entry_mode":    ENTRY_MODE_TOUCH_LIMIT,
                "current_policy_mode":   POLICY_MODE_PROPOSAL_ONLY,
                "future_ready_state": {
                    "apply_pending": False,
                    "note": "Phase 11.4 only. apply_pending remains False.",
                },
                "advisory_note": (
                    "Phase 11.4 evaluates automation policy only. "
                    "No execution authority exists."
                ),
            },
        }
