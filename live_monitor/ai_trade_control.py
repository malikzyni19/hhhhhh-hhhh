"""Phase 11.3: AI Trade Control Decision Engine.

Generates structured trade-control decisions from Phase 11.2 AI Execution Context.
Decision generation only — no order placement, no Binance Testnet, no execution.
Routes stay in main.py. Evidence-only: advisory context, no trade execution authority.
"""
from __future__ import annotations
import time

import main as _m


def _lm_build_ai_trade_control_decision(  # noqa: C901
    item,
    snapshot=None,
    ai_exec_ctx: dict | None = None,
) -> dict:
    """Build Phase 11.3 AI trade-control decision (advisory only, no execution).

    Priority order:
      1. data_health gate blocked         → block_trade
      2. danger level blocked             → block_trade
      3. smc fusion state = breaking      → block_trade
      4. danger level high                → block_trade
      5. smart entry invalidated          → pause_setup
      6. smc fusion state = attacked      → switch_to_confirmation
      7. danger level medium              → switch_to_confirmation
      8. smc fusion state = absorbing     → switch_to_confirmation
      9. orderflow quality insufficient   → no_action
     10. orderflow quality low            → wait
     11. smc fusion state = defended      → allow_touch_limit
     12. danger level low                 → allow_touch_limit
     13. default                          → no_action

    Returns decision schema defined in Phase 11.3 spec.
    Phase 11.3 generates decisions only. No trade execution authority exists.
    """
    now_ts = int(time.time())
    try:
        snap = (snapshot if isinstance(snapshot, dict)
                else _m._json_loads_safe(getattr(item, "snapshot_json", None), {}))

        # Resolve AI execution context — prefer caller-supplied, then snapshot, then fresh build
        ctx: dict = {}
        if ai_exec_ctx and isinstance(ai_exec_ctx, dict) and ai_exec_ctx.get("ok"):
            ctx = ai_exec_ctx
        if not ctx:
            cached = snap.get("latest_ai_execution_context") or {}
            if cached.get("ok"):
                ctx = cached
        if not ctx:
            from live_monitor.ai_execution_context import _lm_build_ai_execution_context
            ctx = _lm_build_ai_execution_context(item, snap) or {}

        # ── Extract key sub-dicts ─────────────────────────────────────────────
        danger_ctx       = ctx.get("danger_context")       or {}
        data_health      = ctx.get("data_health")          or {}
        fusion_ctx       = ctx.get("smc_orderflow_fusion") or {}
        of_quality       = ctx.get("orderflow_history_quality") or {}
        ei_ctx           = ctx.get("execution_intelligence") or {}
        smart_entry      = ctx.get("smart_entry")          or {}

        danger_level     = danger_ctx.get("danger_level", "low")
        danger_score     = int(danger_ctx.get("danger_score", 0))
        blocking_reasons = list(danger_ctx.get("blocking_reasons") or [])
        danger_reasons   = list(danger_ctx.get("reasons")          or [])

        data_gate        = data_health.get("ai_data_gate") or {}
        data_gate_blocked = not data_gate.get("allowed", True)

        fusion_state     = fusion_ctx.get("best_zone_flow_state", "uncertain")
        fusion_cands     = fusion_ctx.get("candidates_breakdown") or []

        of_overall       = of_quality.get("overall", "insufficient")
        of_limitations   = list(of_quality.get("limitations") or [])

        candidates       = list(ei_ctx.get("candidates") or [])
        best_cand        = ei_ctx.get("best_candidate")    or {}

        sep_mode         = smart_entry.get("mode")         or ""
        sep_block_reason = smart_entry.get("block_reason") or "unknown"

        # ── Accumulators ──────────────────────────────────────────────────────
        primary_reasons:    list = []
        supporting_reasons: list = []
        risk_factors:       list = []
        limitations:        list = []
        warnings:           list = []
        action = None

        # ── Decision logic (priority order) ───────────────────────────────────

        # Rule 1: data health gate blocked
        if data_gate_blocked:
            action = "block_trade"
            primary_reasons.append("data_health_gate_blocked")
            risk_factors.append("Live price / mark price unavailable — cannot assess setup safely")

        # Rule 2: danger level blocked (blocking_reasons present)
        if action is None and danger_level == "blocked":
            action = "block_trade"
            for br in blocking_reasons[:3]:
                primary_reasons.append(br)
            risk_factors.append(f"danger_score={danger_score}")

        # Rule 3: fusion breaking
        if action is None and fusion_state == "breaking":
            action = "block_trade"
            primary_reasons.append("smc_fusion_breaking: zone likely failing")
            risk_factors.append("Zone showing 3+ danger signals — flow is breaking through support/resistance")

        # Rule 4: danger high
        if action is None and danger_level == "high":
            action = "block_trade"
            primary_reasons.append(f"danger_level_high: score={danger_score}")
            for r in danger_reasons[:3]:
                risk_factors.append(r)

        # Rule 5: smart entry invalidated (before fusion attacked so it takes priority)
        if action is None and sep_mode == "invalidated":
            action = "pause_setup"
            primary_reasons.append("smart_entry_invalidated: parent zone breached")
            risk_factors.append("Live price has breached the parent zone invalidation level")

        # Rule 6: fusion attacked
        if action is None and fusion_state == "attacked":
            action = "switch_to_confirmation"
            primary_reasons.append("smc_fusion_attacked: zone under pressure")
            risk_factors.append("Zone is showing danger signals — wait for rejection confirmation")

        # Rule 7: danger medium
        if action is None and danger_level == "medium":
            action = "switch_to_confirmation"
            primary_reasons.append(f"danger_level_medium: score={danger_score}")
            for r in danger_reasons[:2]:
                supporting_reasons.append(r)

        # Rule 8: fusion absorbing (confirmation preferred)
        if action is None and fusion_state == "absorbing":
            action = "switch_to_confirmation"
            primary_reasons.append("smc_fusion_absorbing: partial zone defence — confirmation preferred")

        # Rule 9: orderflow quality insufficient
        if action is None and of_overall in ("insufficient", "none"):
            action = "no_action"
            primary_reasons.append(f"orderflow_quality_{of_overall}: insufficient evidence")
            limitations.append("MTF orderflow unavailable — zone behaviour cannot be assessed")

        # Rule 10: orderflow quality low
        if action is None and of_overall == "low":
            action = "wait"
            primary_reasons.append("orderflow_quality_low: weak evidence — wait for better data")
            for lim in of_limitations[:2]:
                limitations.append(lim)

        # Rule 11: fusion defended
        if action is None and fusion_state == "defended":
            action = "allow_touch_limit"
            primary_reasons.append("smc_fusion_defended: zone defended with no danger signals")
            supporting_reasons.append(f"danger_level={danger_level}")

        # Rule 12: danger low
        if action is None and danger_level == "low":
            action = "allow_touch_limit"
            primary_reasons.append(f"danger_level_low: score={danger_score}")
            if fusion_state and fusion_state != "uncertain":
                supporting_reasons.append(f"zone_flow={fusion_state}")

        # Default
        if action is None:
            action = "no_action"
            primary_reasons.append("insufficient_context: no clear directional signal")

        # ── Smart entry supplemental context ──────────────────────────────────
        if sep_mode == "blocked":
            risk_factors.append(f"smart_entry_blocked: {sep_block_reason[:50]}")
        elif sep_mode == "refined_internal_entry":
            bc_sep = smart_entry.get("best_candidate") or {}
            if bc_sep.get("tf"):
                supporting_reasons.append(
                    f"smart_entry_refined: {(bc_sep.get('tf') or '').upper()} "
                    f"{(bc_sep.get('type') or '').upper()} internal candidate"
                )
        elif sep_mode == "parent_default":
            supporting_reasons.append("smart_entry_parent_default: entry at parent zone default levels")

        # ── Candidate analysis (Task 5) ───────────────────────────────────────
        candidate_analysis: dict = {}
        if candidates:
            sorted_cands = sorted(candidates, key=lambda c: c.get("score", 0), reverse=True)
            best_in_list = sorted_cands[0]

            top_entries = []
            for c in sorted_cands[:3]:
                top_entries.append({
                    "idx":   c.get("idx"),
                    "tf":    c.get("tf"),
                    "type":  c.get("type"),
                    "score": c.get("score"),
                    "label": c.get("score_label"),
                    "zone":  (f"{float(c.get('zone_low', 0)):.4f}–"
                              f"{float(c.get('zone_high', 0)):.4f}"),
                    "virgin": c.get("virgin"),
                })

            explanation = ""
            if best_in_list.get("tf") and best_in_list.get("score"):
                explanation = (
                    f"Strongest candidate: "
                    f"{(best_in_list.get('tf') or '').upper()} "
                    f"{(best_in_list.get('type') or '').upper()} "
                    f"(score {best_in_list.get('score', 0)}). "
                    "Parent setup zone remains valid — candidate is for entry precision only."
                )
            if len(sorted_cands) >= 2:
                second = sorted_cands[1]
                supporting_reasons.append(
                    f"top_candidates: "
                    f"{(best_in_list.get('tf') or '').upper()}={best_in_list.get('score', 0)} vs "
                    f"{(second.get('tf') or '').upper()}={second.get('score', 0)}"
                )

            candidate_analysis = {
                "top_candidates":       top_entries,
                "best_candidate_idx":   best_cand.get("idx"),
                "best_candidate_score": best_cand.get("score"),
                "best_candidate_tf":    best_cand.get("tf"),
                "best_candidate_type":  best_cand.get("type"),
                "explanation":          explanation,
                "candidate_count":      len(candidates),
            }

        # ── Confidence calculation (Task 6) ───────────────────────────────────
        base_confidence = 50

        # Orderflow quality adjustment
        quality_adj = {"high": 15, "medium": 5, "low": -20, "insufficient": -35, "none": -35}
        base_confidence += quality_adj.get(of_overall, 0)

        # Fusion confidence adjustment
        if fusion_cands:
            best_fus = max(fusion_cands, key=lambda x: x.get("confidence", 0))
            fus_conf = int(best_fus.get("confidence", 0))
            if action in ("allow_touch_limit",):
                base_confidence += round(fus_conf * 0.20)
            elif action in ("block_trade", "pause_setup"):
                base_confidence += round(fus_conf * 0.15)
            else:
                base_confidence += round(fus_conf * 0.10)

        confidence = max(5, min(95, base_confidence))

        # Reduce confidence for low data quality
        if of_overall in ("low", "none"):
            warnings.append(f"orderflow_quality_{of_overall}: confidence reduced")
        if not candidates:
            warnings.append("no_ei_candidates: run Smart Entry + Execution Intelligence first")
        if fusion_state == "uncertain":
            warnings.append("zone_flow_uncertain: conflicting or insufficient orderflow evidence")

        # ── Result ────────────────────────────────────────────────────────────
        return {
            "ok":               True,
            "phase":            "phase11_3_ai_trade_control",
            "decision_version": "phase11_3",
            "computed_at":      now_ts,

            "action":           action,
            "confidence":       confidence,
            "danger_level":     danger_level,
            "danger_score":     danger_score,
            "fusion_state":     fusion_state,
            "orderflow_quality": of_overall,

            "primary_reasons":    primary_reasons[:8],
            "supporting_reasons": supporting_reasons[:8],
            "risk_factors":       risk_factors[:8],

            "candidate_analysis": candidate_analysis,

            "limitations": limitations[:5],
            "warnings":    warnings[:5],

            "advisory_note": (
                "Phase 11.3 generates decisions only. "
                "No trade execution authority exists."
            ),
        }

    except Exception as _e113:
        return {
            "ok":               False,
            "phase":            "phase11_3_ai_trade_control",
            "decision_version": "phase11_3",
            "computed_at":      now_ts,
            "error":            str(_e113)[:200],

            "action":           "no_action",
            "confidence":       0,
            "danger_level":     "unknown",
            "danger_score":     0,
            "fusion_state":     "unknown",
            "orderflow_quality": "unknown",

            "primary_reasons":    ["build_error"],
            "supporting_reasons": [],
            "risk_factors":       [],
            "candidate_analysis": {},
            "limitations":        [],
            "warnings":           [],

            "advisory_note": (
                "Phase 11.3 generates decisions only. "
                "No trade execution authority exists."
            ),
        }
