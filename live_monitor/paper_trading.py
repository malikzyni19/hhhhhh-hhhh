# live_monitor/paper_trading.py — Phase 11.7B/11.7C: Internal Paper Trading Engine
#
# SAFETY RULES (permanent):
# - No Binance API. DB-only. No exchange calls of any kind.
# - No auto-submit. No AI direct execution. No background order placement.
# - No order placed unless user manually clicks Submit Paper Order.
# - Fill engine uses real Live Monitor market price only.
# - No Binance Testnet price. No _lm_bt_* calls. No requests/HTTP calls.
# - _lm_bt_signed_request() is NOT called here — read-only GET-only, untouched.
# - No API keys. No secrets. No exchange order IDs in DB.
import uuid


# ── JSON helpers ──────────────────────────────────────────────────────────────

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


# ── direction / side mapping ──────────────────────────────────────────────────

def _lm_direction_to_paper_side(direction):
    """Map a direction string to BUY or SELL. Returns '' if unknown.

    Supported: bullish/long/buy → BUY; bearish/short/sell → SELL.
    """
    d = (direction or "").strip().lower()
    if d in ("bullish", "long", "buy"):
        return "BUY"
    if d in ("bearish", "short", "sell"):
        return "SELL"
    return ""


# ── schema-safe helpers ───────────────────────────────────────────────────────

def _lm_model_has_attr(model_or_row, attr):
    """Return True if the SQLAlchemy model has the given column attribute."""
    try:
        from sqlalchemy import inspect as _sa_inspect
        mapper = _sa_inspect(type(model_or_row))
        return attr in {c.key for c in mapper.column_attrs}
    except Exception:
        return hasattr(model_or_row, attr)


def _lm_set_if_exists(row, attr, value):
    """Set row.attr = value only if the column exists on the model. Silent on miss."""
    if _lm_model_has_attr(row, attr):
        try:
            setattr(row, attr, value)
        except Exception:
            pass


def _lm_get_qty_from_position(pos):
    """Return position quantity as float, checking 'quantity' then 'size'."""
    v = getattr(pos, "quantity", None) or getattr(pos, "size", None)
    try:
        return float(v or "0")
    except Exception:
        return 0.0


def _lm_set_qty_on_position(pos, qty_str):
    """Set size (and quantity if column exists) on a position row."""
    _lm_set_if_exists(pos, "size",     qty_str)
    _lm_set_if_exists(pos, "quantity", qty_str)


def _lm_get_order_qty(order):
    """Return order quantity as a string."""
    return str(getattr(order, "quantity", None) or "0")


# ── account ───────────────────────────────────────────────────────────────────

def _lm_get_or_create_paper_account(user_id):
    """Return the paper account row for user_id, creating it if it does not exist.

    Starting balance: 10,000 USDT.
    Returns the ORM row (LiveMonitorPaperAccount).
    """
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
    acc            = _lm_get_or_create_paper_account(user_id)
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
    cash_balance: if provided, reject when estimated_notional > cash_balance.
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
    qty_str_clean = str(qty_str).strip()
    if "." in qty_str_clean:
        decimals = len(qty_str_clean.rstrip("0").split(".")[-1])
        if decimals > 8:
            return {"ok": False, "qty_float": None, "notional": None,
                    "error": "quantity_too_many_decimals",
                    "details": "Quantity has more than 8 decimal places."}
    price_f  = float(price_f) if price_f else 0.0
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
    return {"ok": True, "qty_float": qty_f, "notional": notional,
            "error": None, "details": None}


# ── real market price ─────────────────────────────────────────────────────────

def _lm_get_real_market_price_for_paper(item=None, item_id=None, snapshot=None):
    """Return real Live Monitor market price for paper fill checking.

    Price source priority:
    1. Live WebSocket cache (_lm_ws_get in main module) — live/mark price
    2. Data health snapshot (latest_data_health / data_health)
    3. item.current_price
    4. Snapshot fallback keys (last_known_price / latest_price / etc.)

    Returns: {ok, price, price_source, symbol, error}

    NO Binance API. NO Binance Testnet. NO exchange calls. NO _lm_bt_* calls.
    NO requests/HTTP. DB + in-process WS cache only.
    """
    if item is None and item_id is not None:
        try:
            from models import LiveMonitorItem as _LMI
            item = _LMI.query.get(item_id)
        except Exception:
            pass

    symbol   = (getattr(item, "symbol",   None) or "").strip().upper()   if item else ""
    exchange = (getattr(item, "exchange", None) or "binance").strip().lower() if item else "binance"

    snap = snapshot or {}
    if item is not None and not snap:
        snap = _json_loads_safe(getattr(item, "snapshot_json", None), {}) or {}

    def _hit(price_val, src):
        return {"ok": True, "price": float(price_val),
                "price_source": src, "symbol": symbol or None, "error": None}

    # 1. Live WebSocket cache (in-process; no network call)
    if symbol and exchange:
        try:
            import main as _m
            _ws_get_fn = getattr(_m, "_lm_ws_get", None)
            if _ws_get_fn is not None:
                ws_entry, ws_status = _ws_get_fn(exchange, symbol)
                if ws_entry:
                    lp = ws_entry.get("live_price")
                    mp = ws_entry.get("mark_price")
                    use_p = lp if lp is not None else mp
                    if use_p is not None:
                        return _hit(use_p, f"ws_{exchange}_{ws_status}")
        except Exception:
            pass

    # 2. Data health snapshot
    for dh_key in ("latest_data_health", "data_health", "latest_live_data_health"):
        dh = snap.get(dh_key)
        if isinstance(dh, dict):
            for pk in ("price", "last_price", "mark_price", "live_price"):
                v = dh.get(pk)
                if v is not None:
                    try:
                        return _hit(v, f"snap.{dh_key}.{pk}")
                    except Exception:
                        pass

    # 3. item.current_price
    if item is not None:
        cp = getattr(item, "current_price", None)
        if cp is not None:
            try:
                return _hit(cp, "item.current_price")
            except Exception:
                pass

    # 4. Snapshot fallback keys
    for sk in ("latest_price", "last_known_price", "current_price",
               "mark_price", "latest_market_price", "snapshot_latest_price"):
        v = snap.get(sk)
        if v is not None:
            try:
                return _hit(v, f"snap.{sk}")
            except Exception:
                pass

    return {"ok": False, "price": None, "price_source": None,
            "symbol": symbol or None, "error": "price_unavailable"}


def _lm_get_real_market_price_value_for_paper(item=None, item_id=None, snapshot=None):
    """Compatibility shim — returns float or None (old float-returning signature)."""
    r = _lm_get_real_market_price_for_paper(item=item, item_id=item_id, snapshot=snapshot)
    return r.get("price")


# ── draft ─────────────────────────────────────────────────────────────────────

def _lm_build_paper_order_draft(item, snapshot=None, quantity_str=None) -> dict:
    """Build a paper order draft dict (no DB write, no exchange call).

    Limit order price comes from execution_intent.entry_price (PRIMARY).
    item.current_price is NOT the order price — it is the market reference only.
    paper_ready requires: symbol, side, price, intent.allowed, all sim flags.
    """
    snap = snapshot or {}
    if hasattr(item, "snapshot_json"):
        snap = snap or _json_loads_safe(item.snapshot_json, {}) or {}

    symbol = (getattr(item, "symbol", None) or "").strip().upper()
    intent = snap.get("latest_execution_intent") or {}
    sim    = snap.get("latest_execution_simulation") or {}

    # Direction → side: check intent.direction first, then item.direction
    intent_dir = (intent.get("direction") or "").strip()
    item_dir   = (getattr(item, "direction", None) or "").strip()
    side       = _lm_direction_to_paper_side(intent_dir) or _lm_direction_to_paper_side(item_dir)

    # Limit order price: execution_intent.entry_price (PRIMARY — the setup price)
    # DO NOT use item.current_price as the order price
    price_f      = None
    price_source = None
    warnings     = []

    _ep = intent.get("entry_price")
    if _ep:
        try:
            price_f      = float(_ep)
            price_source = "execution_intent.entry_price"
        except Exception:
            pass

    if price_f is None:
        warnings.append("entry_price_missing")
        # No fallback — LIMIT order price must come from execution_intent.entry_price only

    price_str = f"{price_f:.8f}".rstrip("0").rstrip(".") if price_f else None

    # Market price for reference display (separate from order price)
    mkt_r            = _lm_get_real_market_price_for_paper(item=item, snapshot=snap)
    market_price     = mkt_r.get("price")
    market_price_src = mkt_r.get("price_source")

    # Readiness flags
    intent_allowed = bool(intent.get("allowed"))
    intent_valid   = bool(sim.get("intent_valid"))
    policy_valid   = bool(sim.get("policy_valid"))
    decision_valid = bool(sim.get("decision_valid"))
    data_health_ok = bool(sim.get("data_health_ok"))

    draft_ready = bool(symbol) and bool(side) and bool(price_f)
    paper_ready = (
        draft_ready
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

    cash_balance = None
    try:
        acc          = _lm_get_or_create_paper_account(item.user_id)
        cash_balance = float(acc.cash_balance or 0)
    except Exception:
        pass

    qty_result = {"ok": False, "qty_float": None, "notional": None,
                  "error": None, "details": None}
    if quantity_str and price_f:
        qty_result = _lm_validate_paper_order_quantity(quantity_str, price_f, cash_balance)

    notional = qty_result.get("notional")

    return {
        "ok":                  paper_ready,
        "draft_ready":         draft_ready,
        "paper_ready":         paper_ready,
        "intent_allowed":      intent_allowed,
        "intent_valid":        intent_valid,
        "policy_valid":        policy_valid,
        "decision_valid":      decision_valid,
        "data_health_ok":      data_health_ok,
        "symbol":              symbol or None,
        "side":                side or None,
        "type":                "LIMIT",
        "order_type":          "LIMIT",
        "timeInForce":         "GTC",
        "time_in_force":       "GTC",
        "price":               price_str,
        "price_str":           price_str,
        "price_float":         price_f,
        "price_source":        price_source,
        "market_price":        market_price,
        "market_price_source": market_price_src,
        "quantity":            str(qty_result["qty_float"]) if qty_result.get("qty_float") else None,
        "qty_float":           qty_result.get("qty_float"),
        "estimated_notional":  notional,
        "cash_balance":        cash_balance,
        "qty_ok":              qty_result.get("ok", False),
        "qty_error":           qty_result.get("error"),
        "qty_details":         qty_result.get("details"),
        "reasons":             reasons,
        "warnings":            warnings,
        "advisory_note":       "No Binance API. DB-only paper record. No real order placed.",
        "source":              "internal_paper",
    }


# ── validation ────────────────────────────────────────────────────────────────

def _lm_validate_paper_order_draft(draft) -> dict:
    """Validate a completed draft dict before submission. Returns {ok, errors}."""
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
    from models import db as _db, LiveMonitorPaperOrder as _PO

    snap  = _json_loads_safe(getattr(item, "snapshot_json", None), {})
    draft = _lm_build_paper_order_draft(item, snapshot=snap, quantity_str=quantity_str)

    val = _lm_validate_paper_order_draft(draft)
    if not val["ok"]:
        return {"ok": False, "error": "draft_invalid",
                "errors": val["errors"], "draft": draft}

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
        "ok":               True,
        "order_id":         row.id,
        "client_order_id":  client_order_id,
        "symbol":           symbol,
        "side":             side,
        "order_type":       "LIMIT",
        "time_in_force":    "GTC",
        "price":            price,
        "quantity":         quantity,
        "estimated_notional": draft.get("estimated_notional"),
        "status":           "open",
        "fill_status":      "unfilled",
        "source":           "internal_paper",
        "advisory_note":    "No Binance API. DB-only paper record. No real order placed.",
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
    """Return paper positions as a list of dicts with PnL fields."""
    from models import LiveMonitorPaperPosition as _PP
    q = _PP.query.filter_by(user_id=user_id)
    if item_id is not None:
        q = q.filter_by(item_id=item_id)
    rows = q.order_by(_PP.created_at.desc()).all()
    out = []
    for r in rows:
        unrealized_pnl = float(r.unrealized_pnl or 0)
        realized_pnl   = float(r.realized_pnl   or 0)
        # mark_price not in schema — read defensively
        mark_price_raw = getattr(r, "mark_price", None)
        mark_price_f   = None
        try:
            if mark_price_raw is not None:
                mark_price_f = float(mark_price_raw)
        except Exception:
            pass
        # pnl_pct from stored unrealized_pnl + entry_price * size (no mark needed)
        pnl_pct = None
        try:
            ep = float(r.entry_price or "0")
            sz = _lm_get_qty_from_position(r)
            if ep > 0 and sz > 0:
                pnl_pct = round((unrealized_pnl / (ep * sz)) * 100, 4)
        except Exception:
            pass
        out.append({
            "id":             r.id,
            "item_id":        r.item_id,
            "symbol":         r.symbol,
            "side":           r.side,
            "size":           r.size,
            "quantity":       str(_lm_get_qty_from_position(r)),
            "entry_price":    r.entry_price,
            "mark_price":     str(mark_price_f) if mark_price_f is not None else None,
            "unrealized_pnl": unrealized_pnl,
            "realized_pnl":   realized_pnl,
            "pnl_pct":        pnl_pct,
            "status":         r.status,
            "created_at":     r.created_at.isoformat() if r.created_at else None,
            "updated_at":     r.updated_at.isoformat() if r.updated_at else None,
        })
    return out


# ── Phase 11.7C: Paper Fill Engine ────────────────────────────────────────────
# Safety: No Binance API. No Binance Testnet. No _lm_bt_* calls. No HTTP requests.
# Price source: real Live Monitor market price only (WS cache → DB → snapshot).
# Trigger: manual only — no background loop, no auto execution.

def _lm_check_paper_order_fill(order, item=None, current_price=None):
    """Check one open paper order and fill it if conditions are met.

    BUY LIMIT:  fill when current_price <= order.price
    SELL LIMIT: fill when current_price >= order.price
    Fill price: order.price (limit price). No exchange API calls. DB-only.
    """
    import datetime as _dt

    price_source = "passed_in"
    if current_price is None:
        pr            = _lm_get_real_market_price_for_paper(item=item)
        current_price = pr.get("price")
        price_source  = pr.get("price_source") or "unknown"

    order_price_f = None
    try:
        order_price_f = float(order.price)
    except Exception:
        return {
            "ok": False, "checked": False,
            "reason": "order_price_invalid", "order_id": order.id,
            "source": "internal_paper",
        }

    if current_price is None:
        return {
            "ok": True, "checked": True, "filled": False,
            "reason": "no_current_price",
            "order_id":    order.id,
            "symbol":      order.symbol,
            "side":        order.side,
            "order_price": order_price_f,
            "source":      "internal_paper",
        }

    side = (order.side or "").upper()
    should_fill = (
        (side == "BUY"  and current_price <= order_price_f) or
        (side == "SELL" and current_price >= order_price_f)
    )

    if not should_fill:
        reason = "buy_limit_not_touched" if side == "BUY" else "sell_limit_not_touched"
        return {
            "ok": True, "checked": True, "filled": False,
            "reason":        reason,
            "order_id":      order.id,
            "symbol":        order.symbol,
            "side":          side,
            "order_price":   order_price_f,
            "current_price": current_price,
            "source":        "internal_paper",
        }

    # ── Execute fill ──────────────────────────────────────────────────────────
    from models import (
        db                       as _db,
        LiveMonitorPaperFill     as _PF,
        LiveMonitorPaperPosition as _PP,
    )

    fill_price    = order_price_f
    qty_str       = _lm_get_order_qty(order)
    try:
        qty_f = float(qty_str)
    except Exception:
        qty_f = 0.0
    notional      = round(qty_f * fill_price, 6)
    position_side = "LONG" if side == "BUY" else "SHORT"
    fill_id       = None
    position_id   = None
    now           = _dt.datetime.utcnow()

    try:
        # ── Update order status ───────────────────────────────────────────────
        order.status      = "filled"
        order.fill_status = "filled"
        _lm_set_if_exists(order, "filled_qty",     qty_str)
        _lm_set_if_exists(order, "avg_fill_price", str(fill_price))
        _lm_set_if_exists(order, "filled_at",      now)
        _lm_set_if_exists(order, "updated_at",     now)
        _db.session.flush()

        # ── Create fill record ────────────────────────────────────────────────
        fill = _PF(
            user_id=order.user_id,
            order_id=order.id,
            item_id=order.item_id,
            symbol=order.symbol,
            side=side,
            fill_qty=qty_str,
            fill_price=str(fill_price),
            fill_notional=notional,
        )
        _lm_set_if_exists(fill, "quantity",   qty_str)
        _lm_set_if_exists(fill, "price",      str(fill_price))
        _lm_set_if_exists(fill, "notional",   notional)
        _lm_set_if_exists(fill, "fee",        0)
        _lm_set_if_exists(fill, "fill_type",  "entry")
        _lm_set_if_exists(fill, "account_id", None)
        _db.session.add(fill)
        _db.session.flush()
        fill_id = fill.id

        # ── Create or update position ─────────────────────────────────────────
        pos = _PP.query.filter_by(
            user_id=order.user_id,
            item_id=order.item_id,
            symbol=order.symbol,
            side=position_side,
            status="open",
        ).first()

        if pos is None:
            pos = _PP(
                user_id=order.user_id,
                item_id=order.item_id,
                order_id=order.id,
                symbol=order.symbol,
                side=position_side,
                size=qty_str,
                entry_price=str(fill_price),
                status="open",
                realized_pnl=0.0,
                unrealized_pnl=0.0,
            )
            _lm_set_if_exists(pos, "quantity",   qty_str)
            _lm_set_if_exists(pos, "mark_price", str(fill_price))
            _lm_set_if_exists(pos, "opened_at",  now)
            _lm_set_if_exists(pos, "account_id", None)
            _lm_set_if_exists(pos, "updated_at", now)
            _db.session.add(pos)
            _db.session.flush()
        else:
            # Weighted average entry price
            try:
                old_qty   = _lm_get_qty_from_position(pos)
                old_entry = float(pos.entry_price or "0")
                new_qty   = old_qty + qty_f
                new_entry = (
                    ((old_qty * old_entry) + (qty_f * fill_price)) / new_qty
                    if new_qty > 0 else fill_price
                )
                _lm_set_qty_on_position(pos, str(round(new_qty, 8)))
                pos.entry_price = str(round(new_entry, 8))
            except Exception:
                pass
            _lm_set_if_exists(pos, "updated_at", now)
            _db.session.flush()

        position_id = pos.id

        # Link fill → position if model supports it
        _lm_set_if_exists(fill, "position_id", position_id)

        _db.session.commit()

    except Exception as _e:
        try:
            _db.session.rollback()
        except Exception:
            pass
        return {
            "ok": False, "checked": True, "filled": False,
            "reason": "db_error", "error": str(_e)[:200],
            "order_id": order.id,
            "source":   "internal_paper",
        }

    return {
        "ok":           True,
        "checked":      True,
        "filled":       True,
        "reason":       "filled",
        "order_id":     order.id,
        "symbol":       order.symbol,
        "side":         side,
        "order_price":  order_price_f,
        "current_price": current_price,
        "fill_price":   fill_price,
        "filled_qty":   qty_str,
        "notional":     notional,
        "position_id":  position_id,
        "fill_id":      fill_id,
        "source":       "internal_paper",
    }


def _lm_process_paper_fills_for_item(user_id, item_id):
    """Process all open paper orders for the given item/user.

    Manual/process-triggered only. No background loop. No exchange API. DB-only.
    Returns {ok, processed, filled, current_price, price_source, results, source}.
    """
    from models import (
        LiveMonitorItem       as _LMI,
        LiveMonitorPaperOrder as _PO,
    )
    try:
        item = _LMI.query.filter_by(id=item_id, user_id=user_id).first()
        if not item:
            return {"ok": False, "error": "item_not_found",
                    "processed": 0, "filled": 0, "source": "internal_paper"}

        pr            = _lm_get_real_market_price_for_paper(item=item)
        current_price = pr.get("price")
        price_source  = pr.get("price_source")

        open_orders = _PO.query.filter_by(
            user_id=user_id,
            item_id=item_id,
            status="open",
            fill_status="unfilled",
        ).all()

        results      = []
        filled_count = 0
        for order in open_orders:
            res = _lm_check_paper_order_fill(
                order, item=item, current_price=current_price
            )
            results.append(res)
            if res.get("filled"):
                filled_count += 1

        return {
            "ok":           True,
            "item_id":      item_id,
            "processed":    len(open_orders),
            "filled":       filled_count,
            "current_price": current_price,
            "price_source": price_source,
            "results":      results,
            "source":       "internal_paper",
        }
    except Exception as _e:
        return {
            "ok": False, "error": str(_e)[:200],
            "processed": 0, "filled": 0, "source": "internal_paper",
        }


def _lm_process_all_paper_fills_for_user(user_id):
    """Process open paper orders across all items for the user.

    Manual/process-triggered only. No background loop. DB-only.
    Returns {ok, total_processed, total_filled, by_item, source}.
    """
    from models import LiveMonitorPaperOrder as _PO
    try:
        item_ids = [
            r.item_id for r in
            _PO.query.filter_by(user_id=user_id, status="open", fill_status="unfilled")
            .with_entities(_PO.item_id).distinct().all()
        ]
        total_processed = 0
        total_filled    = 0
        by_item         = []
        for iid in item_ids:
            res = _lm_process_paper_fills_for_item(user_id, iid)
            total_processed += res.get("processed", 0)
            total_filled    += res.get("filled", 0)
            by_item.append({
                "item_id":   iid,
                "processed": res.get("processed", 0),
                "filled":    res.get("filled", 0),
            })
        return {
            "ok":              True,
            "total_processed": total_processed,
            "total_filled":    total_filled,
            "by_item":         by_item,
            "source":          "internal_paper",
        }
    except Exception as _e:
        return {
            "ok": False, "error": str(_e)[:200],
            "total_processed": 0, "total_filled": 0, "source": "internal_paper",
        }


# ── Phase 11.8: Paper Position Engine + Real-Time PnL ─────────────────────────
# Safety: No Binance API. No Binance Testnet. No _lm_bt_* calls. No HTTP requests.
# Price source: real Live Monitor market price only.
# Trigger: manual only — no background loop, no auto execution.
# No TP/SL. No position close. No auto execution.

def _lm_update_paper_position_marks(user_id, item_id=None):
    """Update unrealized_pnl (and mark_price if column exists) for open positions.

    LONG:  unrealized_pnl = (mark_price - entry_price) * quantity
    SHORT: unrealized_pnl = (entry_price - mark_price) * quantity

    Manual/process-triggered only. No exchange API. DB-only.
    Returns {ok, processed, updated, skipped, results, source}.
    """
    import datetime as _dt
    from models import (
        db                       as _db,
        LiveMonitorItem          as _LMI,
        LiveMonitorPaperPosition as _PP,
    )
    try:
        q = _PP.query.filter_by(user_id=user_id, status="open")
        if item_id is not None:
            q = q.filter_by(item_id=item_id)
        positions = q.all()

        item_cache   = {}
        results      = []
        updated_count = 0
        skipped_count = 0
        now           = _dt.datetime.utcnow()

        for pos in positions:
            pid = pos.id
            iid = pos.item_id

            # Resolve item (one DB hit per unique item_id)
            if iid not in item_cache:
                try:
                    item_cache[iid] = _LMI.query.get(iid)
                except Exception:
                    item_cache[iid] = None
            item = item_cache.get(iid)

            pr            = _lm_get_real_market_price_for_paper(item=item)
            mark_price_f  = pr.get("price")
            price_source  = pr.get("price_source")

            if mark_price_f is None:
                results.append({
                    "position_id": pid, "item_id": iid,
                    "symbol": pos.symbol, "side": pos.side,
                    "updated": False, "reason": "price_unavailable",
                })
                skipped_count += 1
                continue

            qty_f = _lm_get_qty_from_position(pos)
            try:
                entry_price_f = float(pos.entry_price or "0")
            except Exception:
                entry_price_f = 0.0

            if qty_f <= 0 or entry_price_f <= 0:
                results.append({
                    "position_id": pid, "item_id": iid,
                    "symbol": pos.symbol, "side": pos.side,
                    "updated": False, "reason": "invalid_position_numbers",
                })
                skipped_count += 1
                continue

            side = (pos.side or "").upper()
            if side == "LONG":
                unrealized_pnl = round((mark_price_f - entry_price_f) * qty_f, 8)
                pnl_pct        = round(((mark_price_f - entry_price_f) / entry_price_f) * 100, 4)
            elif side == "SHORT":
                unrealized_pnl = round((entry_price_f - mark_price_f) * qty_f, 8)
                pnl_pct        = round(((entry_price_f - mark_price_f) / entry_price_f) * 100, 4)
            else:
                results.append({
                    "position_id": pid, "item_id": iid,
                    "symbol": pos.symbol, "side": pos.side,
                    "updated": False, "reason": "unknown_side",
                })
                skipped_count += 1
                continue

            pos.unrealized_pnl = unrealized_pnl
            _lm_set_if_exists(pos, "mark_price", str(mark_price_f))
            _lm_set_if_exists(pos, "updated_at", now)

            updated_count += 1
            results.append({
                "position_id":   pid,
                "item_id":       iid,
                "symbol":        pos.symbol,
                "side":          side,
                "quantity":      qty_f,
                "entry_price":   entry_price_f,
                "mark_price":    mark_price_f,
                "unrealized_pnl": unrealized_pnl,
                "pnl_pct":       pnl_pct,
                "price_source":  price_source,
                "updated":       True,
            })

        _db.session.commit()

        return {
            "ok":        True,
            "processed": len(positions),
            "updated":   updated_count,
            "skipped":   skipped_count,
            "results":   results,
            "source":    "internal_paper",
        }

    except Exception as _e:
        try:
            _db.session.rollback()
        except Exception:
            pass
        return {
            "ok": False, "error": str(_e)[:200],
            "processed": 0, "updated": 0, "skipped": 0,
            "results": [], "source": "internal_paper",
        }


def _lm_recalculate_paper_account_equity(user_id):
    """Recalculate paper account equity from open position unrealized PnL.

    equity = cash_balance + total_unrealized_pnl
    Does NOT modify cash_balance.
    Does NOT recalculate realized_pnl — reads existing field.
    No TP/SL. No fees in this phase.
    Returns {ok, account_id, currency, cash_balance, equity, realized_pnl,
             unrealized_pnl, open_positions, source}.
    """
    import datetime as _dt
    from models import (
        db                       as _db,
        LiveMonitorPaperPosition as _PP,
    )
    try:
        acc = _lm_get_or_create_paper_account(user_id)

        open_positions   = _PP.query.filter_by(user_id=user_id, status="open").all()
        total_unrealized = round(
            sum(float(p.unrealized_pnl or 0) for p in open_positions), 8
        )

        realized_pnl  = float(acc.realized_pnl or 0)
        cash_balance  = float(acc.cash_balance or 0)
        equity        = round(cash_balance + total_unrealized, 8)

        acc.unrealized_pnl = total_unrealized
        acc.equity         = equity
        _lm_set_if_exists(acc, "updated_at", _dt.datetime.utcnow())

        _db.session.commit()

        return {
            "ok":            True,
            "account_id":    acc.id,
            "currency":      acc.currency,
            "cash_balance":  cash_balance,
            "equity":        equity,
            "realized_pnl":  realized_pnl,
            "unrealized_pnl": total_unrealized,
            "open_positions": len(open_positions),
            "source":        "internal_paper",
        }

    except Exception as _e:
        try:
            _db.session.rollback()
        except Exception:
            pass
        return {
            "ok": False, "error": str(_e)[:200],
            "source": "internal_paper",
        }


def _lm_get_paper_position_summary(user_id, item_id=None):
    """Return compact paper position summary (all statuses) with PnL totals.

    READ-ONLY — does not mutate DB.

    For open positions where mark_price is not stored in the DB schema,
    resolves current market price via _lm_get_real_market_price_for_paper
    (WS cache → snapshot → item.current_price). Items and prices are cached
    by item_id to avoid repeated lookups.

    If stored unrealized_pnl is zero/stale and mark_price resolves, a
    display-only unrealized_pnl is computed for the response. The DB is
    not written. Account equity recalculation is not done here.

    Returns {ok, positions, open_count, closed_count,
             total_unrealized_pnl, total_realized_pnl, source}.
    """
    from models import (
        LiveMonitorPaperPosition as _PP,
        LiveMonitorItem          as _LMI,
    )
    try:
        q = _PP.query.filter_by(user_id=user_id)
        if item_id is not None:
            q = q.filter_by(item_id=item_id)
        rows = q.order_by(_PP.created_at.desc()).all()

        positions        = []
        open_count       = 0
        closed_count     = 0
        total_unrealized = 0.0
        total_realized   = 0.0

        item_cache  = {}   # {item_id: LiveMonitorItem row}
        price_cache = {}   # {item_id: {ok, price, price_source, ...}}

        for r in rows:
            unrealized_pnl = float(r.unrealized_pnl or 0)
            realized_pnl   = float(r.realized_pnl   or 0)
            qty_f          = _lm_get_qty_from_position(r)

            entry_price_f = 0.0
            try:
                entry_price_f = float(r.entry_price or "0")
            except Exception:
                pass

            # ── mark_price: DB column first; live market price for open if absent ──
            mark_price_raw = getattr(r, "mark_price", None)
            mark_price_f   = None
            mark_price_src = None
            try:
                if mark_price_raw is not None:
                    mark_price_f   = float(mark_price_raw)
                    mark_price_src = "db"
            except Exception:
                pass

            # For open positions with no stored mark_price, resolve live price
            if mark_price_f is None and r.status == "open":
                iid = r.item_id
                if iid not in item_cache:
                    try:
                        item_cache[iid] = _LMI.query.get(iid)
                    except Exception:
                        item_cache[iid] = None
                if iid not in price_cache:
                    price_cache[iid] = _lm_get_real_market_price_for_paper(
                        item=item_cache.get(iid)
                    )
                pr = price_cache.get(iid, {})
                if pr.get("ok") and pr.get("price") is not None:
                    mark_price_f   = float(pr["price"])
                    mark_price_src = pr.get("price_source")

            # ── display unrealized_pnl & pnl_pct ──────────────────────────────────
            # Use stored value if non-zero (set by Refresh PnL endpoint).
            # If stored value is zero for an open position and mark_price resolved,
            # compute a display-only value — never written to DB.
            display_unrealized = unrealized_pnl
            if (unrealized_pnl == 0.0
                    and r.status == "open"
                    and mark_price_f is not None
                    and entry_price_f > 0
                    and qty_f > 0):
                side = (r.side or "").upper()
                if side == "LONG":
                    display_unrealized = round(
                        (mark_price_f - entry_price_f) * qty_f, 8
                    )
                elif side == "SHORT":
                    display_unrealized = round(
                        (entry_price_f - mark_price_f) * qty_f, 8
                    )

            # pnl_pct derived from display_unrealized / notional-at-entry
            pnl_pct = None
            try:
                if entry_price_f > 0 and qty_f > 0:
                    pnl_pct = round(
                        (display_unrealized / (entry_price_f * qty_f)) * 100, 4
                    )
            except Exception:
                pass

            # opened_at: use created_at (opened_at not in schema)
            opened_at = getattr(r, "opened_at", None) or r.created_at
            closed_at = getattr(r, "closed_at", None)

            positions.append({
                "id":               r.id,
                "item_id":          r.item_id,
                "symbol":           r.symbol,
                "side":             r.side,
                "quantity":         str(qty_f),
                "size":             r.size,
                "entry_price":      r.entry_price,
                "mark_price":       str(mark_price_f) if mark_price_f is not None else None,
                "mark_price_source": mark_price_src,
                "unrealized_pnl":   display_unrealized,
                "realized_pnl":     realized_pnl,
                "pnl_pct":          pnl_pct,
                "status":           r.status,
                "opened_at":        opened_at.isoformat() if opened_at else None,
                "closed_at":        closed_at.isoformat() if closed_at else None,
                "updated_at":       r.updated_at.isoformat() if r.updated_at else None,
                "created_at":       r.created_at.isoformat() if r.created_at else None,
            })

            if r.status == "open":
                open_count       += 1
                total_unrealized += display_unrealized
            else:
                closed_count += 1
            total_realized += realized_pnl

        return {
            "ok":                  True,
            "positions":           positions,
            "open_count":          open_count,
            "closed_count":        closed_count,
            "total_unrealized_pnl": round(total_unrealized, 8),
            "total_realized_pnl":  round(total_realized, 8),
            "source":              "internal_paper",
        }

    except Exception as _e:
        return {
            "ok": False, "error": str(_e)[:200],
            "positions": [], "open_count": 0, "closed_count": 0,
            "total_unrealized_pnl": 0.0, "total_realized_pnl": 0.0,
            "source": "internal_paper",
        }
