# live_monitor/paper_trading.py — Phase 11.7B: Internal Paper Trading Engine Foundation
#
# SAFETY RULES (permanent):
# - No Binance API. DB-only. No exchange calls of any kind.
# - No auto-submit. No AI direct execution. No background order placement.
# - No order placed unless user manually clicks Submit Paper Order.
# - _lm_bt_signed_request() is NOT called here — read-only GET-only, untouched.
# - No API keys. No secrets. No exchange order IDs in DB.
import uuid


# ── helpers ──────────────────────────────────────────────────────────────────

def _json_dumps_safe(obj, fallback="{}"):
    try:
        import json
        return json.dumps(obj, default=str)
    except Exception:
        return fallback


def _json_loads_safe(s, fallback=None):
    try:
        import json
        if not s:
            return fallback
        return json.loads(s)
    except Exception:
        return fallback


# ── account ───────────────────────────────────────────────────────────────────

def _lm_get_or_create_paper_account(user_id):
    """Return the paper account row for user_id, creating it if it does not exist.

    Starting balance: 10,000 USDT.
    Returns the ORM row (LiveMonitorPaperAccount).
    """
    import main as _m
    from models import db as _db, LiveMonitorPaperAccount as _PA
    row = _PA.query.filter_by(user_id=user_id).first()
    if row:
        return row
    row = _PA(
        user_id=user_id,
        currency="USDT",
        starting_balance=10000.0,
        cash_balance=10000.0,
        equity=10000.0,
        realized_pnl=0.0,
        unrealized_pnl=0.0,
        status="active",
    )
    try:
        _db.session.add(row)
        _db.session.commit()
    except Exception:
        try:
            _db.session.rollback()
        except Exception:
            pass
        row = _PA.query.filter_by(user_id=user_id).first() or row
    return row


def _lm_get_paper_account_summary(user_id) -> dict:
    """Return a serializable summary dict for the user's paper account."""
    from models import (
        LiveMonitorPaperAccount  as _PA,
        LiveMonitorPaperOrder    as _PO,
        LiveMonitorPaperPosition as _PP,
    )
    acc = _lm_get_or_create_paper_account(user_id)
    open_orders    = _PO.query.filter_by(user_id=user_id, status="open").count()
    open_positions = _PP.query.filter_by(user_id=user_id, status="open").count()
    return {
        "user_id":          user_id,
        "currency":         acc.currency,
        "starting_balance": float(acc.starting_balance or 0),
        "cash_balance":     float(acc.cash_balance or 0),
        "equity":           float(acc.equity or 0),
        "realized_pnl":     float(acc.realized_pnl or 0),
        "unrealized_pnl":   float(acc.unrealized_pnl or 0),
        "status":           acc.status,
        "open_orders":      open_orders,
        "open_positions":   open_positions,
    }


# ── quantity validation ───────────────────────────────────────────────────────

def _lm_validate_paper_order_quantity(qty_str, price_f, cash_balance=None) -> dict:
    """Validate quantity string for a paper order.

    Returns {ok, qty_float, notional, error, details}.
    cash_balance: if provided, reject if estimated_notional > cash_balance.
    """
    if not qty_str or not str(qty_str).strip():
        return {"ok": False, "qty_float": None, "notional": None,
                "error": "quantity_required", "details": "Quantity is required."}
    try:
        qty_f = float(str(qty_str).strip())
    except (ValueError, TypeError):
        return {"ok": False, "qty_float": None, "notional": None,
                "error": "quantity_invalid", "details": "Quantity must be a number."}
    if qty_f <= 0:
        return {"ok": False, "qty_float": None, "notional": None,
                "error": "quantity_not_positive", "details": "Quantity must be > 0."}
    # max 8 decimal places
    qty_str_clean = str(qty_str).strip()
    if "." in qty_str_clean:
        decimals = len(qty_str_clean.rstrip("0").split(".")[-1])
        if decimals > 8:
            return {"ok": False, "qty_float": None, "notional": None,
                    "error": "quantity_too_many_decimals",
                    "details": "Quantity has more than 8 decimal places."}
    price_f = float(price_f) if price_f else 0.0
    notional = round(qty_f * price_f, 6) if price_f else None
    if cash_balance is not None and notional is not None:
        if notional > float(cash_balance):
            return {
                "ok": False, "qty_float": qty_f, "notional": notional,
                "error": "insufficient_cash",
                "details": (
                    f"Estimated notional {notional:.4f} exceeds "
                    f"cash balance {float(cash_balance):.4f} USDT."
                ),
            }
    return {"ok": True, "qty_float": qty_f, "notional": notional, "error": None, "details": None}


# ── draft ─────────────────────────────────────────────────────────────────────

def _lm_build_paper_order_draft(item, snapshot=None, quantity_str=None) -> dict:
    """Build a paper order draft dict (no DB write, no exchange call).

    paper_ready requires ALL of:
      - symbol, side, price present
      - intent.allowed == True  (explicit check)
      - sim.intent_valid, sim.policy_valid, sim.decision_valid, sim.data_health_ok
    """
    import main as _m
    snap = snapshot or {}
    if hasattr(item, "snapshot_json"):
        snap = snap or _json_loads_safe(item.snapshot_json, {})

    symbol = (getattr(item, "symbol", None) or "").strip().upper()
    direction = (getattr(item, "direction", None) or "").strip().lower()
    side = "BUY" if direction in ("long", "buy") else ("SELL" if direction in ("short", "sell") else "")

    # Price priority: current_price → intent.entry_price → snap.last_known_price
    intent = snap.get("latest_execution_intent") or {}
    sim    = snap.get("latest_execution_simulation") or {}

    price_f = None
    price_source = None
    _cp = getattr(item, "current_price", None)
    if _cp:
        try:
            price_f = float(_cp)
            price_source = "current_price"
        except Exception:
            pass
    if price_f is None:
        _ep = intent.get("entry_price")
        if _ep:
            try:
                price_f = float(_ep)
                price_source = "intent_entry_price"
            except Exception:
                pass
    if price_f is None:
        _lp = snap.get("last_known_price")
        if _lp:
            try:
                price_f = float(_lp)
                price_source = "last_known_price"
            except Exception:
                pass

    price_str = f"{price_f:.8f}".rstrip("0").rstrip(".") if price_f else None

    # Simulation-derived readiness flags
    intent_allowed  = bool(intent.get("allowed"))
    intent_valid    = bool(sim.get("intent_valid"))
    policy_valid    = bool(sim.get("policy_valid"))
    decision_valid  = bool(sim.get("decision_valid"))
    data_health_ok  = bool(sim.get("data_health_ok"))

    paper_ready = (
        bool(symbol) and bool(side) and bool(price_f)
        and intent_allowed
        and intent_valid and policy_valid and decision_valid and data_health_ok
    )

    reasons = []
    if not symbol:
        reasons.append("symbol_missing")
    if not side:
        reasons.append("side_missing")
    if not price_f:
        reasons.append("price_missing")
    if not intent_allowed:
        reasons.append("intent_not_allowed")
    if not intent_valid:
        reasons.append("intent_not_valid")
    if not policy_valid:
        reasons.append("policy_not_valid")
    if not decision_valid:
        reasons.append("decision_not_valid")
    if not data_health_ok:
        reasons.append("data_health_not_ok")

    # Quantity validation
    qty_result = {"ok": False, "qty_float": None, "notional": None, "error": None, "details": None}
    cash_balance = None
    try:
        acc = _lm_get_or_create_paper_account(item.user_id)
        cash_balance = float(acc.cash_balance or 0)
    except Exception:
        pass

    if quantity_str and price_f:
        qty_result = _lm_validate_paper_order_quantity(quantity_str, price_f, cash_balance)

    notional = qty_result.get("notional")

    return {
        "ok":                paper_ready,
        "paper_ready":       paper_ready,
        "intent_allowed":    intent_allowed,
        "intent_valid":      intent_valid,
        "policy_valid":      policy_valid,
        "decision_valid":    decision_valid,
        "data_health_ok":    data_health_ok,
        "symbol":            symbol or None,
        "side":              side or None,
        "order_type":        "LIMIT",
        "time_in_force":     "GTC",
        "price":             price_str,
        "price_float":       price_f,
        "price_source":      price_source,
        "quantity":          str(qty_result["qty_float"]) if qty_result.get("qty_float") else None,
        "qty_float":         qty_result.get("qty_float"),
        "estimated_notional": notional,
        "cash_balance":      cash_balance,
        "qty_ok":            qty_result.get("ok", False),
        "qty_error":         qty_result.get("error"),
        "qty_details":       qty_result.get("details"),
        "reasons":           reasons,
        "warnings":          [],
        "advisory_note":     "No Binance API. DB-only paper record. No real order placed.",
        "source":            "internal_paper",
    }


# ── validation ────────────────────────────────────────────────────────────────

def _lm_validate_paper_order_draft(draft) -> dict:
    """Validate a completed draft dict before submission.

    Returns {ok, errors}.
    """
    errors = []
    if not draft.get("paper_ready"):
        errors.append("paper_ready_false")
    if not draft.get("symbol"):
        errors.append("symbol_missing")
    if not draft.get("side"):
        errors.append("side_missing")
    if not draft.get("price"):
        errors.append("price_missing")
    if not draft.get("qty_ok"):
        errors.append(draft.get("qty_error") or "quantity_invalid")
    return {"ok": len(errors) == 0, "errors": errors}


# ── submit ────────────────────────────────────────────────────────────────────

def _lm_submit_paper_order(user_id, item, quantity_str) -> dict:
    """Create and persist a paper LIMIT order. No exchange call. DB-only.

    Returns {ok, order_id, client_order_id, symbol, side, price, quantity,
             estimated_notional, status, error}.
    """
    import main as _m
    from models import db as _db, LiveMonitorPaperOrder as _PO

    snap = _json_loads_safe(getattr(item, "snapshot_json", None), {})
    draft = _lm_build_paper_order_draft(item, snapshot=snap, quantity_str=quantity_str)

    val = _lm_validate_paper_order_draft(draft)
    if not val["ok"]:
        return {
            "ok": False,
            "error": "draft_invalid",
            "errors": val["errors"],
            "draft": draft,
        }

    client_order_id = f"ZYNIPAPER_{uuid.uuid4().hex[:12].upper()}"
    symbol   = draft["symbol"]
    side     = draft["side"]
    price    = draft["price"]
    quantity = draft["quantity"]

    try:
        row = _PO(
            user_id=user_id,
            item_id=item.id,
            symbol=symbol,
            side=side,
            order_type="LIMIT",
            time_in_force="GTC",
            quantity=quantity,
            price=price,
            status="open",
            fill_status="unfilled",
            client_order_id=client_order_id,
            source="internal_paper",
            estimated_notional=draft.get("estimated_notional"),
            execution_intent_json=_json_dumps_safe(
                snap.get("latest_execution_intent") or {}),
            execution_simulation_json=_json_dumps_safe(
                snap.get("latest_execution_simulation") or {}),
            ai_decision_json=_json_dumps_safe(
                snap.get("latest_ai_trade_control_decision") or {}),
            automation_policy_json=_json_dumps_safe(
                snap.get("latest_automation_policy_result") or {}),
        )
        _db.session.add(row)
        _db.session.commit()
    except Exception as _e:
        try:
            _db.session.rollback()
        except Exception:
            pass
        return {"ok": False, "error": "db_error", "detail": str(_e)}

    return {
        "ok":                True,
        "order_id":          row.id,
        "client_order_id":   client_order_id,
        "symbol":            symbol,
        "side":              side,
        "order_type":        "LIMIT",
        "time_in_force":     "GTC",
        "price":             price,
        "quantity":          quantity,
        "estimated_notional": draft.get("estimated_notional"),
        "status":            "open",
        "fill_status":       "unfilled",
        "source":            "internal_paper",
        "advisory_note":     "No Binance API. DB-only paper record. No real order placed.",
    }


# ── query helpers ─────────────────────────────────────────────────────────────

def _lm_get_paper_orders(user_id, item_id=None, limit=50) -> list:
    """Return recent paper orders as a list of dicts."""
    from models import LiveMonitorPaperOrder as _PO
    q = _PO.query.filter_by(user_id=user_id)
    if item_id is not None:
        q = q.filter_by(item_id=item_id)
    rows = q.order_by(_PO.created_at.desc()).limit(limit).all()
    out = []
    for r in rows:
        out.append({
            "id":               r.id,
            "item_id":          r.item_id,
            "symbol":           r.symbol,
            "side":             r.side,
            "order_type":       r.order_type,
            "time_in_force":    r.time_in_force,
            "quantity":         r.quantity,
            "price":            r.price,
            "status":           r.status,
            "fill_status":      r.fill_status,
            "client_order_id":  r.client_order_id,
            "source":           r.source,
            "estimated_notional": float(r.estimated_notional) if r.estimated_notional else None,
            "created_at":       r.created_at.isoformat() if r.created_at else None,
        })
    return out


def _lm_get_paper_positions(user_id, item_id=None) -> list:
    """Return open paper positions as a list of dicts (fill engine: Phase 11.7C)."""
    from models import LiveMonitorPaperPosition as _PP
    q = _PP.query.filter_by(user_id=user_id)
    if item_id is not None:
        q = q.filter_by(item_id=item_id)
    rows = q.order_by(_PP.created_at.desc()).all()
    out = []
    for r in rows:
        out.append({
            "id":           r.id,
            "item_id":      r.item_id,
            "symbol":       r.symbol,
            "side":         r.side,
            "size":         r.size,
            "entry_price":  r.entry_price,
            "status":       r.status,
            "realized_pnl": float(r.realized_pnl) if r.realized_pnl else 0.0,
            "created_at":   r.created_at.isoformat() if r.created_at else None,
        })
    return out
