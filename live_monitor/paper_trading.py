"""Phase 11.7B: Internal Paper Trading Engine Foundation.

DB-only paper order simulation. No exchange calls. No Binance API.
No real or testnet orders. Manual submit only — no automation.

Price source: item.current_price → snapshot entry_price → last known LM price.
client_order_id prefix: ZYNI_PAPER_
Default account: 10,000 USDT, auto-created per user.
"""
from __future__ import annotations
import time
import uuid

import main as _m


# ── Helpers ───────────────────────────────────────────────────────────────────

def _pt_float(v) -> float | None:
    try:
        return float(v) if v is not None and v != "" else None
    except (TypeError, ValueError):
        return None


def _pt_direction_to_side(direction: str) -> str | None:
    d = direction.lower()
    if "bull" in d or "long" in d:
        return "BUY"
    if "bear" in d or "short" in d:
        return "SELL"
    return None


# ── Account ───────────────────────────────────────────────────────────────────

def _lm_get_or_create_paper_account(user_id: int) -> dict:
    """Return the paper account for user_id, creating it if needed.

    Returns a safe dict (never the ORM object) so callers never hold
    a detached-instance reference across request boundaries.
    """
    try:
        from models import (
            db as _db,
            LiveMonitorPaperAccount as _LMPA,
        )
        import datetime as _dt

        acct = _LMPA.query.filter_by(user_id=user_id, status="active").first()
        if not acct:
            acct = _LMPA(
                user_id=user_id,
                currency="USDT",
                starting_balance=10000.0,
                cash_balance=10000.0,
                equity=10000.0,
                realized_pnl=0.0,
                unrealized_pnl=0.0,
                status="active",
            )
            _db.session.add(acct)
            _db.session.commit()

        return {
            "ok":               True,
            "account_id":       acct.id,
            "user_id":          acct.user_id,
            "currency":         acct.currency,
            "starting_balance": float(acct.starting_balance or 0),
            "cash_balance":     float(acct.cash_balance or 0),
            "equity":           float(acct.equity or 0),
            "realized_pnl":     float(acct.realized_pnl or 0),
            "unrealized_pnl":   float(acct.unrealized_pnl or 0),
            "status":           acct.status,
            "created_at":       str(acct.created_at) if acct.created_at else None,
        }
    except Exception as _e:
        return {"ok": False, "error": str(_e)[:200]}


def _lm_get_paper_account_summary(user_id: int) -> dict:
    """Return paper account summary dict for snapshot storage."""
    acct = _lm_get_or_create_paper_account(user_id)
    if not acct.get("ok"):
        return acct
    acct["phase"] = "phase11_7b_paper_account"
    acct["computed_at"] = int(time.time())
    return acct


# ── Draft ─────────────────────────────────────────────────────────────────────

def _lm_build_paper_order_draft(
    item,
    snapshot: dict | None = None,
    quantity_str: str | None = None,
) -> dict:
    """Build a paper order draft.

    Price source: item.current_price → intent entry_price → snapshot price.
    Does NOT call any exchange API. DB-only.
    """
    now_ts = int(time.time())
    try:
        snap = (snapshot if isinstance(snapshot, dict)
                else _m._json_loads_safe(getattr(item, "snapshot_json", None), {}))

        intent = snap.get("latest_execution_intent") or {}
        sim    = snap.get("latest_execution_simulation") or {}
        ai_dec = snap.get("latest_ai_trade_control_decision") or {}
        pol    = snap.get("latest_automation_policy_result") or {}

        # ── Symbol ─────────────────────────────────────────────────────────────
        symbol = str(
            intent.get("symbol") or getattr(item, "symbol", None) or ""
        ).upper().strip()

        # ── Direction → side ───────────────────────────────────────────────────
        direction = str(
            intent.get("direction") or getattr(item, "direction", None) or ""
        ).lower()
        side = _pt_direction_to_side(direction)

        # ── Price source: current_price → entry_price → snapshot ───────────────
        price_f = (
            _pt_float(getattr(item, "current_price", None))
            or _pt_float(intent.get("entry_price"))
            or _pt_float(snap.get("last_known_price"))
        )
        price_str = f"{price_f}" if price_f else None

        # ── paper_ready from simulation (no connector check needed) ────────────
        intent_valid  = bool(sim.get("intent_valid"))
        policy_valid  = bool(sim.get("policy_valid"))
        decision_valid = bool(sim.get("decision_valid"))
        data_health_ok = bool(sim.get("data_health_ok"))
        paper_ready   = intent_valid and policy_valid and decision_valid and data_health_ok

        # ── Quantity ───────────────────────────────────────────────────────────
        qty_f: float | None = None
        qty_str_clean: str | None = None
        qty_ok = False
        qty_error = "quantity_required"
        notional: float | None = None

        if quantity_str is not None and str(quantity_str).strip():
            qty_str_clean = str(quantity_str).strip()
            try:
                qty_f = float(qty_str_clean)
                if qty_f > 0:
                    qty_ok = True
                    qty_error = ""
                    if price_f:
                        notional = round(qty_f * price_f, 4)
                else:
                    qty_error = "quantity_must_be_positive"
            except (TypeError, ValueError):
                qty_error = "quantity_not_a_number"

        qty_validation = {
            "ok":    qty_ok,
            "error": qty_error,
            "qty_float": qty_f or 0.0,
        }

        # ── Blocking reasons ───────────────────────────────────────────────────
        reasons: list = []
        if not symbol:
            reasons.append("symbol_missing")
        if not side:
            reasons.append("direction_unknown_cannot_determine_side")
        if not price_f:
            reasons.append("entry_price_missing")
        if quantity_str is None:
            reasons.append("quantity_required")
        elif not qty_ok:
            reasons.append(qty_error)

        draft_ready = not reasons

        return {
            "ok":                   True,
            "phase":                "phase11_7b_paper_draft",
            "computed_at":          now_ts,
            "draft_ready":          draft_ready,
            "symbol":               symbol,
            "side":                 side,
            "type":                 "LIMIT",
            "timeInForce":          "GTC",
            "quantity":             qty_str_clean,
            "price":                price_str,
            "price_float":          price_f,
            "estimated_notional":   notional,
            "paper_ready":          paper_ready,
            "intent_valid":         intent_valid,
            "policy_valid":         policy_valid,
            "decision_valid":       decision_valid,
            "data_health_ok":       data_health_ok,
            "qty_validation":       qty_validation,
            "reasons":              reasons,
            "source":               "internal_paper",
            "advisory_note": (
                "Phase 11.7B — Internal paper LIMIT entry order only. "
                "No TP/SL. No real exchange. No automatic execution. "
                "User must manually click Submit Paper Order."
            ),
        }

    except Exception as _e:
        return {
            "ok":           False,
            "phase":        "phase11_7b_paper_draft",
            "computed_at":  now_ts,
            "draft_ready":  False,
            "error":        str(_e)[:200],
            "reasons":      [f"build_error:{str(_e)[:60]}"],
            "advisory_note": "Phase 11.7B manual submit only.",
        }


def _lm_validate_paper_order_draft(draft: dict) -> dict:
    """Validate a pre-built paper order draft.

    Returns {ok, error, reasons}.
    """
    if not draft.get("ok"):
        return {"ok": False, "error": "draft_build_failed", "reasons": draft.get("reasons", [])}
    if not draft.get("draft_ready"):
        return {"ok": False, "error": "draft_not_ready", "reasons": draft.get("reasons", [])}
    return {"ok": True, "error": "", "reasons": []}


# ── Submit ─────────────────────────────────────────────────────────────────────

def _lm_submit_paper_order(
    user_id: int,
    item,
    quantity_str: str,
) -> dict:
    """Persist a paper order to DB. No exchange calls. Manual submit only.

    Returns a safe result dict. The caller (endpoint) must never trigger this
    automatically — only from an explicit user button click.
    """
    now_ts = int(time.time())
    try:
        from models import (
            db as _db,
            LiveMonitorPaperAccount as _LMPA,
            LiveMonitorPaperOrder as _LMPO,
        )
        import datetime as _dt

        snap = _m._json_loads_safe(getattr(item, "snapshot_json", None), {})
        draft = _lm_build_paper_order_draft(item, snap, quantity_str)

        validation = _lm_validate_paper_order_draft(draft)
        if not validation["ok"]:
            return {
                "ok":      False,
                "error":   validation["error"],
                "reasons": validation["reasons"],
                "draft":   draft,
            }

        # ── Ensure account exists ──────────────────────────────────────────────
        acct_summary = _lm_get_or_create_paper_account(user_id)
        if not acct_summary.get("ok"):
            return {"ok": False, "error": "account_error", "detail": acct_summary.get("error")}
        account_id = acct_summary["account_id"]

        symbol   = draft["symbol"]
        side     = draft["side"]
        quantity = draft["quantity"]
        price    = draft["price"]

        client_order_id = f"ZYNI_PAPER_{uuid.uuid4().hex[:12].upper()}"

        intent  = snap.get("latest_execution_intent") or {}
        sim     = snap.get("latest_execution_simulation") or {}
        ai_dec  = snap.get("latest_ai_trade_control_decision") or {}
        pol     = snap.get("latest_automation_policy_result") or {}

        now_dt = _dt.datetime.utcnow()

        db_record = _LMPO(
            user_id=user_id,
            item_id=item.id,
            account_id=account_id,
            symbol=symbol,
            side=side,
            order_type="LIMIT",
            time_in_force="GTC",
            quantity=quantity,
            price=price,
            status="open",
            fill_status="unfilled",
            filled_qty="0",
            avg_fill_price=None,
            client_order_id=client_order_id,
            source="internal_paper",
            execution_intent_json=_m._json_dumps_safe(intent),
            execution_simulation_json=_m._json_dumps_safe(sim),
            ai_decision_json=_m._json_dumps_safe(ai_dec),
            automation_policy_json=_m._json_dumps_safe(pol),
            request_json=_m._json_dumps_safe({
                "symbol":   symbol,
                "side":     side,
                "type":     "LIMIT",
                "quantity": quantity,
                "price":    price,
                "source":   "internal_paper",
            }),
            response_json=_m._json_dumps_safe({"status": "open", "fill_status": "unfilled"}),
            error_json=None,
            submitted_at=now_dt,
        )
        _db.session.add(db_record)
        _db.session.commit()

        return {
            "ok":                 True,
            "order_id":           db_record.id,
            "client_order_id":    client_order_id,
            "symbol":             symbol,
            "side":               side,
            "quantity":           quantity,
            "price":              price,
            "status":             "open",
            "fill_status":        "unfilled",
            "source":             "internal_paper",
            "estimated_notional": draft.get("estimated_notional"),
            "submitted_at":       str(now_dt),
        }

    except Exception as _e:
        return {
            "ok":    False,
            "error": "submit_error",
            "detail": str(_e)[:300],
        }


# ── Query helpers ──────────────────────────────────────────────────────────────

def _lm_get_paper_orders(user_id: int, item_id: int | None = None) -> dict:
    """Return recent paper orders for user, optionally filtered by item."""
    try:
        from models import LiveMonitorPaperOrder as _LMPO

        q = _LMPO.query.filter_by(user_id=user_id)
        if item_id:
            q = q.filter_by(item_id=item_id)
        rows = q.order_by(_LMPO.created_at.desc()).limit(50).all()

        orders = []
        for r in rows:
            orders.append({
                "id":              r.id,
                "item_id":         r.item_id,
                "symbol":          r.symbol,
                "side":            r.side,
                "order_type":      r.order_type,
                "time_in_force":   r.time_in_force,
                "quantity":        r.quantity,
                "price":           r.price,
                "status":          r.status,
                "fill_status":     r.fill_status,
                "filled_qty":      r.filled_qty,
                "avg_fill_price":  r.avg_fill_price,
                "client_order_id": r.client_order_id,
                "source":          r.source,
                "created_at":      str(r.created_at) if r.created_at else None,
                "submitted_at":    str(r.submitted_at) if r.submitted_at else None,
            })
        return {"ok": True, "orders": orders, "count": len(orders)}
    except Exception as _e:
        return {"ok": False, "error": str(_e)[:200], "orders": []}


def _lm_get_paper_positions(user_id: int, item_id: int | None = None) -> dict:
    """Return open paper positions for user, optionally filtered by item."""
    try:
        from models import LiveMonitorPaperPosition as _LMPP

        q = _LMPP.query.filter_by(user_id=user_id, status="open")
        if item_id:
            q = q.filter_by(item_id=item_id)
        rows = q.order_by(_LMPP.opened_at.desc()).limit(50).all()

        positions = []
        for r in rows:
            positions.append({
                "id":             r.id,
                "item_id":        r.item_id,
                "symbol":         r.symbol,
                "side":           r.side,
                "quantity":       r.quantity,
                "entry_price":    r.entry_price,
                "mark_price":     r.mark_price,
                "unrealized_pnl": r.unrealized_pnl,
                "realized_pnl":   r.realized_pnl,
                "status":         r.status,
                "opened_at":      str(r.opened_at) if r.opened_at else None,
            })
        return {"ok": True, "positions": positions, "count": len(positions)}
    except Exception as _e:
        return {"ok": False, "error": str(_e)[:200], "positions": []}
