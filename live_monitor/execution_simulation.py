"""Phase 11.6: Execution Simulation Layer.

Simulation only. No order placement. No position creation. No execution.
No Binance orders. Produces a validated execution_intent and
execution_simulation result from all previous phases (11.1–11.5A).

Flow:
  Smart Entry → Execution Intelligence → AI Context → AI Decision
  → Automation Policy → Execution Intent → Execution Simulation → Result
"""
from __future__ import annotations
import time

import main as _m

# ── Constants ─────────────────────────────────────────────────────────────────
_ES_MIN_RR    = 1.0   # minimum acceptable risk/reward for simulation pass
_ES_TARGET_RR = 2.0   # default RR used when take_profit cannot be resolved


# ── Helpers ───────────────────────────────────────────────────────────────────

def _es_float(v) -> float | None:
    """Safe float conversion — returns None on failure."""
    try:
        return float(v) if v is not None and v != "" else None
    except (TypeError, ValueError):
        return None


def _es_price_aligns_tick(price: float, tick: float) -> bool:
    """Return True if price is approximately a multiple of tick size."""
    if tick <= 0:
        return True
    remainder = price % tick
    return remainder < tick * 0.01 or remainder > tick * 0.99


# ── Phase 11.6A: Execution Intent ─────────────────────────────────────────────

def _lm_build_execution_intent(item, snapshot=None) -> dict:  # noqa: C901
    """Build Phase 11.6 execution intent from Smart Entry + AI Decision + Policy.

    Aggregates price levels, direction, entry mode, and policy state into a
    single intent dict that feeds _lm_build_execution_simulation().

    Simulation only. No order creation. No execution authority.
    """
    now_ts = int(time.time())
    try:
        snap = (snapshot if isinstance(snapshot, dict)
                else _m._json_loads_safe(getattr(item, "snapshot_json", None), {}))

        # ── Source data ───────────────────────────────────────────────────────
        smart_entry = snap.get("latest_smart_entry_plan") or {}
        ai_decision = snap.get("latest_ai_trade_control_decision") or {}
        pol_context = snap.get("latest_automation_policy_context") or {}
        pol_result  = snap.get("latest_automation_policy_result") or {}

        # ── Symbol / direction ────────────────────────────────────────────────
        symbol    = str(getattr(item, "symbol", None) or "").upper().strip()
        direction = str(
            smart_entry.get("direction")
            or getattr(item, "direction", None)
            or "unknown"
        ).lower()

        # ── Price levels from Smart Entry (primary source) ────────────────────
        best_cand = smart_entry.get("best_refined_candidate") or {}
        if not best_cand and smart_entry.get("best_candidate"):
            bc_raw = smart_entry["best_candidate"]
            best_cand = bc_raw[0] if isinstance(bc_raw, list) and bc_raw else (
                bc_raw if isinstance(bc_raw, dict) else {}
            )

        entry_price = (
            _es_float(smart_entry.get("entry_price"))
            or _es_float(best_cand.get("entry"))
            or _es_float(best_cand.get("price"))
            or _es_float(getattr(item, "current_price", None))
        )
        stop_loss = (
            _es_float(smart_entry.get("invalidation_price"))
            or _es_float(best_cand.get("invalidation"))
        )
        take_profit = (
            _es_float(best_cand.get("target"))
            or _es_float(best_cand.get("take_profit"))
        )

        # Compute TP from entry + stop + target RR when not available
        if entry_price and stop_loss and not take_profit:
            risk = abs(entry_price - stop_loss)
            if risk > 0:
                is_bull = "bull" in direction or "long" in direction
                if is_bull:
                    take_profit = round(entry_price + risk * _ES_TARGET_RR, 8)
                elif "bear" in direction or "short" in direction:
                    take_profit = round(entry_price - risk * _ES_TARGET_RR, 8)

        # ── Risk/reward ───────────────────────────────────────────────────────
        risk_reward: float | None = None
        if entry_price and stop_loss and take_profit:
            risk   = abs(entry_price - stop_loss)
            reward = abs(take_profit - entry_price)
            if risk > 0:
                risk_reward = round(reward / risk, 2)

        # ── Confidence (smart entry primary, AI decision fallback) ─────────────
        confidence = int(
            smart_entry.get("confidence")
            or ai_decision.get("confidence")
            or 0
        )

        # ── Entry mode from automation policy ─────────────────────────────────
        entry_mode = str(
            pol_context.get("entry_mode")
            or pol_result.get("current_entry_mode")
            or "touch_limit"
        )

        # ── Intent source ─────────────────────────────────────────────────────
        se_mode = str(smart_entry.get("mode") or "")
        if se_mode in ("refined_internal_entry", "parent_default", "precision_watch"):
            source = "smart_entry"
        elif ai_decision.get("ok"):
            source = "ai_decision"
        else:
            source = "manual"

        # ── Zone context ──────────────────────────────────────────────────────
        zone_high = _es_float(
            smart_entry.get("zone_high") or getattr(item, "zone_high", None)
        )
        zone_low  = _es_float(
            smart_entry.get("zone_low") or getattr(item, "zone_low", None)
        )

        # ── Policy + AI decision state ────────────────────────────────────────
        policy_allowed = bool(pol_result.get("allowed", True))
        ai_action      = str(ai_decision.get("action", "no_action"))
        ai_ok          = bool(ai_decision.get("ok", False))

        # Intent is "allowed" when policy permits AND AI doesn't hard-block
        intent_allowed = (
            policy_allowed
            and ai_action not in ("block_trade", "pause_setup")
        )

        # ── Intent warnings ───────────────────────────────────────────────────
        intent_warnings: list = []
        if not entry_price:
            intent_warnings.append("entry_price_missing")
        if not stop_loss:
            intent_warnings.append("stop_loss_missing")
        if not take_profit:
            intent_warnings.append("take_profit_missing")
        if not symbol:
            intent_warnings.append("symbol_missing")
        if direction == "unknown":
            intent_warnings.append("direction_unknown")
        if ai_action in ("block_trade", "pause_setup"):
            intent_warnings.append(f"ai_decision_{ai_action}")
        if not policy_allowed:
            for br in (pol_result.get("blocking_reasons") or [])[:3]:
                intent_warnings.append(f"policy_{br}")

        return {
            "ok":               True,
            "phase":            "phase11_6_execution_intent",
            "computed_at":      now_ts,
            "allowed":          intent_allowed,
            "source":           source,
            "entry_mode":       entry_mode,
            "symbol":           symbol,
            "direction":        direction,
            "entry_price":      entry_price,
            "stop_loss":        stop_loss,
            "take_profit":      take_profit,
            "risk_reward":      risk_reward,
            "confidence":       confidence,
            "zone_high":        zone_high,
            "zone_low":         zone_low,
            "ai_action":        ai_action,
            "ai_ok":            ai_ok,
            "policy_allowed":   policy_allowed,
            "smart_entry_mode": se_mode,
            "warnings":         intent_warnings,
            "advisory_note":    (
                "Phase 11.6 builds execution intent only. "
                "No order placement. No execution authority exists."
            ),
        }

    except Exception as _e116i:
        return {
            "ok":            False,
            "phase":         "phase11_6_execution_intent",
            "computed_at":   now_ts,
            "allowed":       False,
            "error":         str(_e116i)[:200],
            "warnings":      [f"build_error:{str(_e116i)[:60]}"],
            "advisory_note": "Phase 11.6 simulation only.",
        }


# ── Phase 11.6B: Execution Simulation ─────────────────────────────────────────

def _lm_build_execution_simulation(item, snapshot=None) -> dict:  # noqa: C901
    """Build Phase 11.6 execution simulation result.

    Runs the full validation pipeline over the execution intent and produces
    a ready_for_testnet verdict. No order placement. Simulation only.

    Checks:
      1. Symbol exists          7. Direction valid
      2. Zone valid             8. Policy allowed
      3. Entry valid            9. Data health allowed
      4. Stop valid            10. AI decision not blocking
      5. TP valid              11. Binance symbol filter validation
      6. RR valid
    """
    now_ts = int(time.time())
    try:
        snap = (snapshot if isinstance(snapshot, dict)
                else _m._json_loads_safe(getattr(item, "snapshot_json", None), {}))

        # ── Resolve intent (cached or fresh) ─────────────────────────────────
        cached_intent = snap.get("latest_execution_intent")
        intent = (
            cached_intent
            if (cached_intent and cached_intent.get("ok"))
            else _lm_build_execution_intent(item, snap)
        )

        # ── Supporting context ────────────────────────────────────────────────
        ai_exec_ctx = snap.get("latest_ai_execution_context") or {}
        pol_result  = snap.get("latest_automation_policy_result") or {}
        ai_decision = snap.get("latest_ai_trade_control_decision") or {}

        reasons:  list = []
        warnings: list = list(intent.get("warnings") or [])

        # ── Check 1: Symbol ───────────────────────────────────────────────────
        symbol    = str(intent.get("symbol") or "")
        symbol_ok = bool(symbol)
        if not symbol_ok:
            reasons.append("symbol_missing")

        # ── Check 2: Direction ────────────────────────────────────────────────
        direction    = str(intent.get("direction") or "").lower()
        direction_ok = any(w in direction for w in ("bull", "bear", "long", "short"))
        if not direction_ok:
            reasons.append(f"direction_invalid:{direction}")

        # ── Check 3: Zone valid ───────────────────────────────────────────────
        zone_high = intent.get("zone_high")
        zone_low  = intent.get("zone_low")
        zone_ok   = bool(zone_high and zone_low and float(zone_high) > float(zone_low))
        if not zone_ok:
            reasons.append("invalid_zone")

        # ── Check 4: Entry valid ──────────────────────────────────────────────
        entry_price = intent.get("entry_price")
        entry_ok    = bool(entry_price and float(entry_price) > 0)
        if not entry_ok:
            reasons.append("entry_price_missing")

        # ── Check 5: Stop valid ───────────────────────────────────────────────
        stop_loss = intent.get("stop_loss")
        stop_ok   = bool(
            stop_loss and float(stop_loss) > 0
            and stop_loss != entry_price
        )
        if not stop_ok:
            reasons.append("stop_loss_invalid")

        # ── Check 6: TP valid ─────────────────────────────────────────────────
        take_profit = intent.get("take_profit")
        tp_ok       = bool(
            take_profit and float(take_profit) > 0
            and take_profit != entry_price
        )
        if not tp_ok:
            reasons.append("take_profit_missing")

        # ── Check 7: RR valid ─────────────────────────────────────────────────
        rr    = intent.get("risk_reward")
        rr_ok = bool(rr and float(rr) >= _ES_MIN_RR)
        if not rr_ok:
            reasons.append(f"invalid_rr:{rr}")

        # ── Check 8: Policy allowed ───────────────────────────────────────────
        policy_allowed = (
            bool(pol_result.get("allowed", True))
            and bool(intent.get("policy_allowed", True))
        )
        if not policy_allowed:
            pol_blocking = (pol_result.get("blocking_reasons") or [])
            for br in pol_blocking[:2]:
                reasons.append(f"policy_blocked:{br}")
            if not pol_blocking:
                reasons.append("policy_blocked")

        # ── Check 9: Data health ──────────────────────────────────────────────
        data_health_ctx = ai_exec_ctx.get("data_health") or {}
        data_gate       = (data_health_ctx.get("ai_data_gate") or {})
        data_health_ok  = bool(data_gate.get("allowed", True))
        if not data_health_ok:
            for gr in (data_gate.get("reasons") or [])[:2]:
                reasons.append(f"data_health_blocked:{gr}")
            if not (data_gate.get("reasons") or []):
                reasons.append("data_health_blocked")

        # ── Check 10: AI decision ─────────────────────────────────────────────
        ai_action    = str(ai_decision.get("action", "no_action"))
        ai_ok        = bool(ai_decision.get("ok"))
        decision_ok  = ai_ok and ai_action not in ("block_trade", "pause_setup")
        if not ai_ok:
            reasons.append("decision_missing")
        elif not decision_ok:
            reasons.append(f"decision_{ai_action}")

        # ── Check 11: Binance symbol filter validation ────────────────────────
        filter_valid   = None    # None = unavailable/skipped (non-blocking)
        filter_details: dict = {}
        filter_error   = ""
        filter_skipped = not symbol_ok or not entry_ok

        if not filter_skipped:
            try:
                from live_monitor.binance_testnet import _lm_bt_symbol_filters
                sf = _lm_bt_symbol_filters(symbol)
                if sf.get("ok") and sf.get("found"):
                    filter_valid = True
                    filter_details = {
                        "symbol":            sf.get("symbol"),
                        "status":            sf.get("status"),
                        "pricePrecision":    sf.get("pricePrecision"),
                        "quantityPrecision": sf.get("quantityPrecision"),
                        "tickSize":          sf.get("tickSize"),
                        "stepSize":          sf.get("stepSize"),
                        "minQty":            sf.get("minQty"),
                        "maxQty":            sf.get("maxQty"),
                        "minNotional":       sf.get("minNotional"),
                    }
                    tick = _es_float(sf.get("tickSize"))
                    if tick and entry_price:
                        if not _es_price_aligns_tick(float(entry_price), tick):
                            warnings.append(f"entry_tick_mismatch:tickSize={tick}")
                elif sf.get("ok") and not sf.get("found"):
                    filter_valid = False
                    filter_error = f"symbol_not_found_on_testnet:{symbol}"
                    reasons.append("symbol_filter_failed")
                else:
                    filter_error = str(sf.get("error", "filter_fetch_failed"))[:80]
                    warnings.append(f"symbol_filter_unavailable:{filter_error}")
            except Exception as _efe:
                filter_error = str(_efe)[:80]
                warnings.append(f"symbol_filter_error:{filter_error}")
        else:
            filter_error = "skipped:missing_symbol_or_entry"

        # ── Overall verdict ───────────────────────────────────────────────────
        intent_valid  = (symbol_ok and entry_ok and stop_ok and tp_ok
                         and rr_ok and direction_ok)
        policy_valid  = policy_allowed
        decision_valid = decision_ok

        # filter_valid=None treated as non-blocking (testnet may be unreachable)
        ready_for_testnet = (
            intent_valid
            and policy_valid
            and decision_valid
            and data_health_ok
            and filter_valid is not False
            and not reasons
        )

        return {
            "ok":               True,
            "phase":            "phase11_6_execution_simulation",
            "computed_at":      now_ts,
            "simulated":        True,
            "ready_for_testnet": ready_for_testnet,
            "intent_valid":     intent_valid,
            "filter_valid":     filter_valid,
            "filter_details":   filter_details,
            "filter_skipped":   filter_skipped,
            "filter_error":     filter_error,
            "policy_valid":     policy_valid,
            "decision_valid":   decision_valid,
            "data_health_ok":   data_health_ok,
            "symbol_ok":        symbol_ok,
            "direction_ok":     direction_ok,
            "zone_ok":          zone_ok,
            "entry_ok":         entry_ok,
            "stop_ok":          stop_ok,
            "tp_ok":            tp_ok,
            "rr_ok":            rr_ok,
            "reasons":          reasons,
            "warnings":         warnings[:10],
            "intent_summary": {
                "symbol":      intent.get("symbol"),
                "direction":   intent.get("direction"),
                "entry_price": intent.get("entry_price"),
                "stop_loss":   intent.get("stop_loss"),
                "take_profit": intent.get("take_profit"),
                "risk_reward": intent.get("risk_reward"),
                "confidence":  intent.get("confidence"),
                "entry_mode":  intent.get("entry_mode"),
                "ai_action":   ai_action,
            },
            "advisory_note": (
                "Phase 11.6 is simulation only. "
                "No order placement exists. "
                "No execution authority exists."
            ),
        }

    except Exception as _e116s:
        return {
            "ok":               False,
            "phase":            "phase11_6_execution_simulation",
            "computed_at":      now_ts,
            "simulated":        True,
            "ready_for_testnet": False,
            "intent_valid":     False,
            "filter_valid":     False,
            "policy_valid":     False,
            "decision_valid":   False,
            "data_health_ok":   False,
            "error":            str(_e116s)[:200],
            "reasons":          [f"build_error:{str(_e116s)[:60]}"],
            "warnings":         [],
            "intent_summary":   {},
            "advisory_note":    "Phase 11.6 simulation only.",
        }
