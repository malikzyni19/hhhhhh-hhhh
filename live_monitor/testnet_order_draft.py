"""Phase 11.7A: Testnet Order Draft builder.

Builds a validated LIMIT order draft from execution intent + symbol filters.
No order placement. Draft generation only.

The draft feeds the manual submit UI in Trading Terminal. The user must
manually enter quantity and click Submit Testnet Order to place any order.
"""
from __future__ import annotations
import math
import time

import main as _m


def _tod_float(v) -> float | None:
    """Safe float — None on failure."""
    try:
        return float(v) if v is not None and v != "" else None
    except (TypeError, ValueError):
        return None


def _tod_round_to_step(value: float, step: float, precision: int) -> str:
    """Round value down to the nearest step and return as string."""
    if step <= 0:
        return str(round(value, precision))
    rounded = math.floor(value / step) * step
    return f"{rounded:.{precision}f}"


def _lm_validate_order_quantity(
    qty_str: str,
    step_size: str | None,
    min_qty: str | None,
    price: float | None,
    min_notional: str | None,
    qty_precision: int = 3,
) -> dict:
    """Validate a user-supplied quantity against Binance symbol filters.

    Returns: {ok, qty_float, error, details}
    """
    try:
        qty_f = float(qty_str)
    except (TypeError, ValueError):
        return {"ok": False, "qty_float": 0.0, "error": "quantity_not_a_number", "details": {}}

    if qty_f <= 0:
        return {"ok": False, "qty_float": qty_f, "error": "quantity_must_be_positive", "details": {}}

    details: dict = {"qty": qty_f}

    # stepSize alignment
    step = _tod_float(step_size)
    if step and step > 0:
        remainder = qty_f % step
        aligned   = remainder < step * 0.001 or remainder > step * 0.999
        details["step_size"]      = step
        details["step_aligned"]   = aligned
        if not aligned:
            return {"ok": False, "qty_float": qty_f,
                    "error": f"quantity_not_aligned_to_stepSize:{step}", "details": details}

    # minQty
    min_q = _tod_float(min_qty)
    if min_q and min_q > 0:
        details["min_qty"]    = min_q
        details["qty_ge_min"] = qty_f >= min_q
        if qty_f < min_q:
            return {"ok": False, "qty_float": qty_f,
                    "error": f"quantity_below_minQty:{min_q}", "details": details}

    # minNotional
    if price and min_notional:
        min_n    = _tod_float(min_notional)
        notional = qty_f * price
        details["notional"]     = round(notional, 4)
        details["min_notional"] = min_n
        if min_n and notional < min_n:
            return {"ok": False, "qty_float": qty_f,
                    "error": f"notional_below_minimum:{min_n}", "details": details}

    return {"ok": True, "qty_float": qty_f, "error": "", "details": details}


def _lm_build_testnet_order_draft(
    item,
    snapshot: dict | None = None,
    quantity_str: str | None = None,
) -> dict:
    """Build Phase 11.7A testnet order draft.

    Resolves symbol, side, price from execution intent and validates quantity
    against Binance symbol filters. Does NOT place any order.

    Args:
        item:         LiveMonitorItem
        snapshot:     pre-loaded snap dict (optional)
        quantity_str: user-supplied quantity string (optional, may be None)

    Returns draft dict — always includes draft_ready and reasons.
    """
    now_ts = int(time.time())
    try:
        snap = (snapshot if isinstance(snapshot, dict)
                else _m._json_loads_safe(getattr(item, "snapshot_json", None), {}))

        # ── Source data ───────────────────────────────────────────────────────
        intent = snap.get("latest_execution_intent") or {}
        sim    = snap.get("latest_execution_simulation") or {}

        # ── Symbol ────────────────────────────────────────────────────────────
        symbol = (
            str(intent.get("symbol") or getattr(item, "symbol", None) or "").upper().strip()
        )

        # ── Direction → side ──────────────────────────────────────────────────
        direction = str(intent.get("direction") or getattr(item, "direction", None) or "").lower()
        if "bull" in direction or "long" in direction:
            side = "BUY"
        elif "bear" in direction or "short" in direction:
            side = "SELL"
        else:
            side = None

        # ── Price ─────────────────────────────────────────────────────────────
        price_f  = _tod_float(intent.get("entry_price"))
        price_str = f"{price_f}" if price_f else None

        # ── Simulation / connector status ─────────────────────────────────────
        simulation_ready = bool(sim.get("ready_for_testnet"))
        intent_allowed   = bool(intent.get("allowed"))

        from live_monitor.binance_testnet import (
            _lm_bt_credentials_available,
            _lm_bt_is_testnet_only,
            _lm_bt_symbol_filters,
            _lm_bt_order_enabled,
        )

        connector_ready = _lm_bt_is_testnet_only() and _lm_bt_credentials_available()
        order_enabled   = _lm_bt_order_enabled()

        # ── Symbol filters ────────────────────────────────────────────────────
        filter_details: dict = {}
        filter_ok = False
        if symbol:
            sf = _lm_bt_symbol_filters(symbol)
            if sf.get("ok") and sf.get("found"):
                filter_ok = True
                filter_details = {
                    "pricePrecision":    sf.get("pricePrecision"),
                    "quantityPrecision": sf.get("quantityPrecision"),
                    "tickSize":          sf.get("tickSize"),
                    "stepSize":          sf.get("stepSize"),
                    "minQty":            sf.get("minQty"),
                    "maxQty":            sf.get("maxQty"),
                    "minNotional":       sf.get("minNotional"),
                }

        # ── Quantity validation ───────────────────────────────────────────────
        qty_validation: dict = {"ok": False, "error": "quantity_required", "details": {}}
        qty_str_clean: str | None = None
        notional: float | None = None

        if quantity_str is not None and str(quantity_str).strip():
            qty_str_clean = str(quantity_str).strip()
            qty_prec = int(filter_details.get("quantityPrecision") or 3)
            qty_validation = _lm_validate_order_quantity(
                qty_str_clean,
                filter_details.get("stepSize"),
                filter_details.get("minQty"),
                price_f,
                filter_details.get("minNotional"),
                qty_prec,
            )
            if qty_validation["ok"] and price_f:
                notional = round(qty_validation["qty_float"] * price_f, 4)

        # ── Blocking reasons ──────────────────────────────────────────────────
        reasons: list = []
        if not symbol:
            reasons.append("symbol_missing")
        if not side:
            reasons.append("direction_unknown_cannot_determine_side")
        if not price_f:
            reasons.append("entry_price_missing")
        if not simulation_ready:
            reasons.append("simulation_not_ready")
        if not intent_allowed:
            reasons.append("intent_not_allowed")
        if not connector_ready:
            reasons.append("connector_not_ready")
        if not order_enabled:
            reasons.append(f"env_{_m.os.environ.get('BINANCE_TESTNET_ORDER_ENABLED', 'unset')}_order_disabled")
        if quantity_str is None:
            reasons.append("quantity_required")
        elif not qty_validation.get("ok"):
            reasons.append(qty_validation.get("error", "quantity_invalid"))

        draft_ready = not reasons

        return {
            "ok":              True,
            "phase":           "phase11_7a_order_draft",
            "computed_at":     now_ts,
            "draft_ready":     draft_ready,
            "symbol":          symbol,
            "side":            side,
            "type":            "LIMIT",
            "timeInForce":     "GTC",
            "quantity":        qty_str_clean,
            "price":           price_str,
            "price_float":     price_f,
            "estimated_notional": notional,
            "simulation_ready": simulation_ready,
            "intent_allowed":  intent_allowed,
            "connector_ready": connector_ready,
            "order_enabled":   order_enabled,
            "filter_ok":       filter_ok,
            "filter_details":  filter_details,
            "qty_validation":  qty_validation,
            "reasons":         reasons,
            "advisory_note":   (
                "Phase 11.7A — Manual testnet LIMIT entry order only. "
                "No TP/SL. No automatic execution. "
                "User must manually click Submit Testnet Order."
            ),
        }

    except Exception as _e117:
        return {
            "ok":           False,
            "phase":        "phase11_7a_order_draft",
            "computed_at":  now_ts,
            "draft_ready":  False,
            "error":        str(_e117)[:200],
            "reasons":      [f"build_error:{str(_e117)[:60]}"],
            "advisory_note": "Phase 11.7A manual submit only.",
        }
