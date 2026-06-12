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
                "close_reason":     getattr(r, "close_reason", None),
                "close_price":      getattr(r, "close_price",  None),
                "exit_fill_id":     getattr(r, "exit_fill_id", None),
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


# ── Phase 11.9: Paper TP/SL Exit Engine ───────────────────────────────────────
# Safety: No Binance API. No Binance Testnet. No _lm_bt_* calls. No HTTP requests.
# Price source: real Live Monitor market price only.
# Trigger: manual only — no background loop, no auto execution.
# TP/SL levels: from execution_intent_json on entry order only.
# No leverage. No margin. No liquidation. No trailing stop.

def _lm_get_paper_position_exit_levels(position, item=None):
    """Resolve TP/SL/entry levels from the position's entry order execution_intent.

    Falls back to item snapshot latest_execution_intent if order has no intent.
    Returns {ok, exit_ready, entry_price, stop_loss, take_profit, direction,
             risk_reward, reason, source}.
    """
    intent = {}
    order  = None

    if position.order_id is not None:
        try:
            from models import LiveMonitorPaperOrder as _PO
            order = _PO.query.get(position.order_id)
        except Exception:
            pass

    if order is not None:
        try:
            raw = getattr(order, "execution_intent_json", None)
            if raw:
                intent = _json_loads_safe(raw, {}) or {}
        except Exception:
            pass

    # Fallback: item snapshot latest_execution_intent
    if not intent and item is not None:
        try:
            snap   = _json_loads_safe(getattr(item, "snapshot_json", None), {}) or {}
            intent = snap.get("latest_execution_intent") or {}
        except Exception:
            pass

    def _f(key):
        v = intent.get(key)
        if v is None:
            return None
        try:
            return float(v)
        except Exception:
            return None

    entry_price = _f("entry_price")
    stop_loss   = _f("stop_loss")
    take_profit = _f("take_profit")
    risk_reward = _f("risk_reward")

    try:
        direction = str(intent.get("direction") or "").strip().lower() or None
    except Exception:
        direction = None

    if stop_loss is None or take_profit is None:
        return {
            "ok":          True,
            "exit_ready":  False,
            "reason":      "tp_sl_missing",
            "entry_price": entry_price,
            "stop_loss":   stop_loss,
            "take_profit": take_profit,
            "direction":   direction,
            "risk_reward": risk_reward,
            "source":      "execution_intent",
        }

    return {
        "ok":          True,
        "exit_ready":  True,
        "reason":      None,
        "entry_price": entry_price,
        "stop_loss":   stop_loss,
        "take_profit": take_profit,
        "direction":   direction,
        "risk_reward": risk_reward,
        "source":      "execution_intent",
    }


def _lm_check_paper_position_exit(position, item=None, current_price=None):
    """Check whether an open position has hit TP or SL and close it if so.

    Exit price: close at take_profit if TP hit, close at stop_loss if SL hit.
    If both hit simultaneously, SL is preferred (conservative).
    Closes position in DB, creates close fill (linked to entry order_id),
    updates account cash_balance + realized_pnl immediately.
    Does NOT call _lm_recalculate_paper_account_equity — caller handles that.

    SAFETY: DB-only. No Binance API. No exchange calls. Manual trigger only.
    Returns {ok, checked, closed, ...}.
    """
    import datetime as _dt
    from models import (
        db               as _db,
        LiveMonitorPaperFill as _PF,
    )

    pos    = position
    pid    = pos.id
    symbol = pos.symbol
    side   = (pos.side or "").upper()

    # ── Resolve exit levels ───────────────────────────────────────────────────
    levels = _lm_get_paper_position_exit_levels(pos, item=item)
    if not levels.get("exit_ready"):
        return {
            "ok":          True,
            "checked":     True,
            "closed":      False,
            "reason":      levels.get("reason", "tp_sl_missing"),
            "position_id": pid,
            "symbol":      symbol,
            "side":        side,
            "source":      "internal_paper",
        }

    stop_loss   = levels["stop_loss"]
    take_profit = levels["take_profit"]

    # Use actual DB entry_price as authoritative (levels.entry_price is advisory)
    try:
        entry_price_f = float(pos.entry_price or "0")
    except Exception:
        entry_price_f = levels.get("entry_price") or 0.0

    # ── Resolve current mark price ────────────────────────────────────────────
    if current_price is None:
        pr = _lm_get_real_market_price_for_paper(item=item)
        current_price = pr.get("price")

    if current_price is None:
        return {
            "ok":          True,
            "checked":     True,
            "closed":      False,
            "reason":      "price_unavailable",
            "position_id": pid,
            "symbol":      symbol,
            "side":        side,
            "source":      "internal_paper",
        }

    mark_price = float(current_price)

    # ── TP/SL hit checks ──────────────────────────────────────────────────────
    tp_hit = False
    sl_hit = False
    if side == "LONG":
        tp_hit = mark_price >= take_profit
        sl_hit = mark_price <= stop_loss
    elif side == "SHORT":
        tp_hit = mark_price <= take_profit
        sl_hit = mark_price >= stop_loss
    else:
        return {
            "ok":          True,
            "checked":     True,
            "closed":      False,
            "reason":      "unknown_side",
            "position_id": pid,
            "symbol":      symbol,
            "side":        side,
            "source":      "internal_paper",
        }

    if not tp_hit and not sl_hit:
        return {
            "ok":          True,
            "checked":     True,
            "closed":      False,
            "reason":      "not_hit",
            "position_id": pid,
            "symbol":      symbol,
            "side":        side,
            "entry_price": entry_price_f,
            "mark_price":  mark_price,
            "take_profit": take_profit,
            "stop_loss":   stop_loss,
            "source":      "internal_paper",
        }

    # Both hit — prefer SL (conservative)
    warning = None
    if tp_hit and sl_hit:
        tp_hit  = False
        sl_hit  = True
        warning = "tp_and_sl_both_hit_preferred_sl"

    close_reason = "take_profit" if tp_hit else "stop_loss"
    exit_price   = take_profit   if tp_hit else stop_loss
    close_side   = "SELL" if side == "LONG" else "BUY"

    qty_f   = _lm_get_qty_from_position(pos)
    qty_str = str(qty_f)

    if side == "LONG":
        realized_pnl = round((exit_price - entry_price_f) * qty_f, 8)
    else:
        realized_pnl = round((entry_price_f - exit_price) * qty_f, 8)

    notional = round(qty_f * exit_price, 6)
    now      = _dt.datetime.utcnow()
    fill_id  = None

    try:
        # ── Close position ────────────────────────────────────────────────────
        pos.status         = "closed"
        pos.realized_pnl   = realized_pnl
        pos.unrealized_pnl = 0.0
        pos.close_reason   = close_reason
        pos.close_price    = str(exit_price)
        pos.closed_at      = now
        _lm_set_if_exists(pos, "updated_at", now)
        _db.session.flush()

        # ── Create close fill (reuse entry order_id — PaperFill.order_id is non-nullable) ──
        if pos.order_id is not None:
            fill = _PF(
                user_id=pos.user_id,
                order_id=pos.order_id,
                item_id=pos.item_id,
                symbol=symbol,
                side=close_side,
                fill_qty=qty_str,
                fill_price=str(exit_price),
                fill_notional=notional,
                fill_type=close_reason,
                position_id=pid,
                fee=0,
            )
            _db.session.add(fill)
            _db.session.flush()
            fill_id = fill.id
            pos.exit_fill_id = fill_id   # link position → close fill

        # ── Update account cash_balance + realized_pnl ────────────────────────
        acc = _lm_get_or_create_paper_account(pos.user_id)
        acc.cash_balance  = round(float(acc.cash_balance  or 0) + realized_pnl, 8)
        acc.realized_pnl  = round(float(acc.realized_pnl  or 0) + realized_pnl, 8)
        _lm_set_if_exists(acc, "updated_at", now)
        _db.session.flush()

        _db.session.commit()

        result = {
            "ok":           True,
            "checked":      True,
            "closed":       True,
            "close_reason": close_reason,
            "position_id":  pid,
            "symbol":       symbol,
            "side":         side,
            "quantity":     qty_f,
            "entry_price":  entry_price_f,
            "mark_price":   mark_price,
            "exit_price":   exit_price,
            "realized_pnl": realized_pnl,
            "fill_id":      fill_id,
            "source":       "internal_paper",
        }
        if warning:
            result["warning"] = warning
        return result

    except Exception as _e:
        try:
            _db.session.rollback()
        except Exception:
            pass
        return {
            "ok":          False,
            "checked":     True,
            "closed":      False,
            "error":       str(_e)[:200],
            "position_id": pid,
            "symbol":      symbol,
            "side":        side,
            "source":      "internal_paper",
        }


def _lm_process_paper_exits_for_item(user_id, item_id):
    """Check all open paper positions for item against TP/SL levels.

    Manual/process-triggered only. No background loop. No exchange API. DB-only.
    Returns {ok, item_id, processed, closed, tp_hits, sl_hits, results, account, source}.
    """
    from models import (
        LiveMonitorItem          as _LMI,
        LiveMonitorPaperPosition as _PP,
    )
    try:
        item = _LMI.query.filter_by(id=item_id, user_id=user_id).first()
        if not item:
            return {
                "ok": False, "error": "item_not_found",
                "item_id": item_id, "processed": 0, "closed": 0,
                "tp_hits": 0, "sl_hits": 0, "source": "internal_paper",
            }

        pr            = _lm_get_real_market_price_for_paper(item=item)
        current_price = pr.get("price")

        open_positions = _PP.query.filter_by(
            user_id=user_id,
            item_id=item_id,
            status="open",
        ).all()

        results      = []
        closed_count = 0
        tp_hits      = 0
        sl_hits      = 0

        for pos in open_positions:
            res = _lm_check_paper_position_exit(
                pos, item=item, current_price=current_price
            )
            results.append(res)
            if res.get("closed"):
                closed_count += 1
                reason = res.get("close_reason", "")
                if reason == "take_profit":
                    tp_hits += 1
                elif reason == "stop_loss":
                    sl_hits += 1

        # Recalculate equity after all closes in this batch
        equity_result = _lm_recalculate_paper_account_equity(user_id)

        return {
            "ok":        True,
            "item_id":   item_id,
            "processed": len(open_positions),
            "closed":    closed_count,
            "tp_hits":   tp_hits,
            "sl_hits":   sl_hits,
            "results":   results,
            "account":   equity_result,
            "source":    "internal_paper",
        }

    except Exception as _e:
        return {
            "ok": False, "error": str(_e)[:200],
            "item_id": item_id, "processed": 0, "closed": 0,
            "tp_hits": 0, "sl_hits": 0, "source": "internal_paper",
        }


def _lm_process_all_paper_exits_for_user(user_id):
    """Check paper positions across all items for the user against TP/SL levels.

    Manual/process-triggered only. No background loop. DB-only.
    Returns {ok, total_processed, total_closed, tp_hits, sl_hits, by_item, source}.
    """
    from models import LiveMonitorPaperPosition as _PP
    try:
        item_ids = [
            r.item_id for r in
            _PP.query.filter_by(user_id=user_id, status="open")
            .with_entities(_PP.item_id).distinct().all()
        ]
        total_processed = 0
        total_closed    = 0
        total_tp        = 0
        total_sl        = 0
        by_item         = []
        for iid in item_ids:
            res             = _lm_process_paper_exits_for_item(user_id, iid)
            total_processed += res.get("processed", 0)
            total_closed    += res.get("closed", 0)
            total_tp        += res.get("tp_hits", 0)
            total_sl        += res.get("sl_hits", 0)
            by_item.append({
                "item_id":   iid,
                "processed": res.get("processed", 0),
                "closed":    res.get("closed", 0),
                "tp_hits":   res.get("tp_hits", 0),
                "sl_hits":   res.get("sl_hits", 0),
            })
        return {
            "ok":              True,
            "total_processed": total_processed,
            "total_closed":    total_closed,
            "tp_hits":         total_tp,
            "sl_hits":         total_sl,
            "by_item":         by_item,
            "source":          "internal_paper",
        }
    except Exception as _e:
        return {
            "ok": False, "error": str(_e)[:200],
            "total_processed": 0, "total_closed": 0,
            "tp_hits": 0, "sl_hits": 0, "source": "internal_paper",
        }


# ── Phase 11.10: Paper Trade Journal ─────────────────────────────────────────
# Safety: No Binance API. No Binance Testnet. No _lm_bt_* calls. No HTTP requests.
# DB-only. Read-only snapshots captured at sync time. No auto execution.
# No background worker. Manual sync only.


def _lm_build_paper_trade_record_from_position(position, item=None):
    """Build a trade record dict from a closed LiveMonitorPaperPosition.

    Returns {ok, record: {...}} or {ok: False, reason: '...'}.
    No Binance API. No exchange calls. DB-only snapshot capture.
    """
    import datetime as _dt_jnl
    from models import (
        db                       as _db_jnl,
        LiveMonitorPaperOrder    as _PO_jnl,
        LiveMonitorPaperFill     as _PF_jnl,
        LiveMonitorItem          as _LMI_jnl,
    )

    if position.status != "closed":
        return {"ok": False, "reason": "not_closed"}

    pid    = position.id
    uid    = position.user_id
    iid    = position.item_id
    symbol = position.symbol or ""
    side   = (position.side or "").upper()

    if item is None and iid:
        try:
            item = _LMI_jnl.query.get(iid)
        except Exception:
            item = None

    snap = _json_loads_safe(item.snapshot_json if item else None, {})

    # Quantity
    try:
        qty_f = float(_lm_get_qty_from_position(position) or 0)
    except Exception:
        qty_f = 0.0
    qty_str = str(qty_f) if qty_f else None

    # Prices
    try:
        entry_price_f = float(position.entry_price or 0)
    except Exception:
        entry_price_f = 0.0

    exit_price_f = None
    try:
        cp = getattr(position, "close_price", None)
        if cp:
            exit_price_f = float(cp)
    except Exception:
        pass

    # Entry order
    entry_order = None
    if position.order_id:
        try:
            entry_order = _PO_jnl.query.get(position.order_id)
        except Exception:
            pass

    # Exit fill — priority: exit_fill_id → position_id FK → last non-entry fill for order
    exit_fill = None
    efid = getattr(position, "exit_fill_id", None)
    if efid:
        try:
            exit_fill = _PF_jnl.query.get(efid)
        except Exception:
            pass
    if exit_fill is None:
        try:
            exit_fill = (_PF_jnl.query
                         .filter_by(position_id=pid)
                         .order_by(_PF_jnl.id.desc())
                         .first())
        except Exception:
            pass
    if exit_fill is None and entry_order:
        try:
            exit_fill = (_PF_jnl.query
                         .filter(_PF_jnl.order_id == entry_order.id,
                                 _PF_jnl.fill_type != "entry")
                         .order_by(_PF_jnl.id.desc())
                         .first())
        except Exception:
            pass

    if exit_price_f is None and exit_fill:
        try:
            v = float(exit_fill.fill_price or 0)
            if v:
                exit_price_f = v
        except Exception:
            pass

    # PnL
    try:
        realized_pnl = float(position.realized_pnl or 0)
    except Exception:
        realized_pnl = 0.0

    realized_pnl_pct = None
    if entry_price_f > 0 and qty_f > 0:
        try:
            notional = entry_price_f * qty_f
            realized_pnl_pct = round((realized_pnl / notional) * 100, 4)
        except Exception:
            pass

    # Outcome
    if realized_pnl > 0:
        outcome = "win"
    elif realized_pnl < 0:
        outcome = "loss"
    else:
        outcome = "breakeven"

    outcome_reason = getattr(position, "close_reason", None) or "unknown"

    # Duration
    duration_seconds = None
    closed_at  = getattr(position, "closed_at",  None)
    created_at = getattr(position, "created_at", None)
    if closed_at and created_at:
        try:
            def _naive(v):
                return v.replace(tzinfo=None) if getattr(v, "tzinfo", None) else v
            dur = (_naive(closed_at) - _naive(created_at)).total_seconds()
            duration_seconds = int(dur) if dur >= 0 else None
        except Exception:
            pass

    # Risk/reward from execution_intent_json on entry order or snapshot
    risk_reward = None
    exec_intent_obj = None
    if entry_order and entry_order.execution_intent_json:
        exec_intent_obj = _json_loads_safe(entry_order.execution_intent_json, {})
    if not exec_intent_obj:
        exec_intent_obj = snap.get("latest_execution_intent") or {}
    if exec_intent_obj:
        try:
            rr = exec_intent_obj.get("risk_reward")
            risk_reward = float(rr) if rr is not None else None
        except Exception:
            pass

    # Account id (best-effort lookup)
    account_id = None
    try:
        acc = _lm_get_or_create_paper_account(uid)
        account_id = acc.id if acc else None
    except Exception:
        pass

    # Snapshot capture helpers
    def _js(obj):
        return _json_dumps_safe(obj) if obj else None

    exec_intel_raw = snap.get("latest_execution_intelligence")
    mtf_of_raw     = snap.get("latest_mtf_orderflow_history")
    ai_ctx_raw     = snap.get("latest_ai_execution_context")
    draft_raw      = snap.get("latest_paper_order_draft")
    exit_of_raw    = snap.get("latest_mtf_orderflow_history") or snap.get("orderflow_alignment")

    ai_dec_obj = None
    if entry_order and entry_order.ai_decision_json:
        ai_dec_obj = _json_loads_safe(entry_order.ai_decision_json, None)
    if ai_dec_obj is None:
        ai_dec_obj = snap.get("latest_ai_trade_control_decision")

    autopol_obj = None
    if entry_order and entry_order.automation_policy_json:
        autopol_obj = _json_loads_safe(entry_order.automation_policy_json, None)
    if autopol_obj is None:
        autopol_obj = snap.get("latest_automation_policy_result")

    entry_order_compact = None
    if entry_order:
        entry_order_compact = {
            "id":          entry_order.id,
            "symbol":      entry_order.symbol,
            "side":        entry_order.side,
            "quantity":    entry_order.quantity,
            "price":       entry_order.price,
            "status":      entry_order.status,
            "fill_status": entry_order.fill_status,
            "source":      entry_order.source,
            "created_at":  str(entry_order.created_at) if entry_order.created_at else None,
        }

    exit_fill_compact = None
    if exit_fill:
        exit_fill_compact = {
            "id":         exit_fill.id,
            "symbol":     exit_fill.symbol,
            "side":       exit_fill.side,
            "fill_qty":   exit_fill.fill_qty,
            "fill_price": exit_fill.fill_price,
            "fill_type":  getattr(exit_fill, "fill_type", None),
            "created_at": str(exit_fill.created_at) if exit_fill.created_at else None,
        }

    record = {
        "user_id":         uid,
        "item_id":         iid,
        "account_id":      account_id,
        "position_id":     pid,
        "entry_order_id":  position.order_id,
        "exit_fill_id":    efid,
        "symbol":          symbol,
        "side":            side,
        "quantity":        qty_str,
        "entry_price":     str(entry_price_f) if entry_price_f else None,
        "exit_price":      str(exit_price_f)  if exit_price_f is not None else None,
        "status":          "closed",
        "outcome":         outcome,
        "outcome_reason":  outcome_reason,
        "realized_pnl":    realized_pnl,
        "realized_pnl_pct": realized_pnl_pct,
        "risk_reward":     risk_reward,
        "duration_seconds": duration_seconds,
        "closed_at":       closed_at,
        "exit_snapshot_json":           _js(snap),
        "execution_intent_json":        _js(exec_intent_obj),
        "execution_intelligence_json":  _js(exec_intel_raw),
        "mtf_orderflow_history_json":   _js(mtf_of_raw),
        "ai_context_json":              _js(ai_ctx_raw),
        "ai_decision_json":             _js(ai_dec_obj),
        "automation_policy_json":       _js(autopol_obj),
        "paper_order_draft_json":       _js(draft_raw),
        "entry_order_json":             _js(entry_order_compact),
        "exit_fill_json":               _js(exit_fill_compact),
        "exit_orderflow_snapshot_json": _js(exit_of_raw),
    }
    return {"ok": True, "record": record}


def _lm_upsert_paper_trade_from_closed_position(position, item=None):
    """Create or update a LiveMonitorPaperTrade row for a closed position.

    Idempotent: keyed on position_id unique constraint.
    Returns {ok, trade_id, action: created|updated|skipped}.
    No Binance API. DB-only.
    """
    import datetime as _dt_up
    from models import (
        db                      as _db_up,
        LiveMonitorPaperTrade   as _PT_up,
    )

    build = _lm_build_paper_trade_record_from_position(position, item=item)
    if not build.get("ok"):
        return {"ok": False, "reason": build.get("reason", "build_failed"),
                "source": "internal_paper"}

    rec = build["record"]
    pid = rec["position_id"]
    now = _dt_up.datetime.utcnow()

    try:
        existing = _PT_up.query.filter_by(position_id=pid).first()
        if existing:
            # Update only mutable review/snapshot fields
            for _fld in (
                "exit_snapshot_json", "execution_intelligence_json",
                "mtf_orderflow_history_json", "ai_context_json",
                "ai_decision_json", "automation_policy_json",
                "exit_fill_json", "exit_orderflow_snapshot_json",
                "realized_pnl", "realized_pnl_pct",
                "outcome", "outcome_reason",
                "exit_price", "closed_at",
            ):
                v = rec.get(_fld)
                if v is not None:
                    setattr(existing, _fld, v)
            existing.updated_at = now
            _db_up.session.commit()
            return {"ok": True, "trade_id": existing.id, "action": "updated",
                    "source": "internal_paper"}

        trade = _PT_up(
            user_id          = rec["user_id"],
            item_id          = rec["item_id"],
            account_id       = rec["account_id"],
            position_id      = pid,
            entry_order_id   = rec["entry_order_id"],
            exit_fill_id     = rec["exit_fill_id"],
            symbol           = rec["symbol"],
            side             = rec["side"],
            quantity         = rec["quantity"],
            entry_price      = rec["entry_price"],
            exit_price       = rec["exit_price"],
            status           = "closed",
            outcome          = rec["outcome"],
            outcome_reason   = rec["outcome_reason"],
            realized_pnl     = rec["realized_pnl"],
            realized_pnl_pct = rec["realized_pnl_pct"],
            risk_reward      = rec["risk_reward"],
            duration_seconds = rec["duration_seconds"],
            closed_at        = rec["closed_at"],
            exit_snapshot_json           = rec["exit_snapshot_json"],
            execution_intent_json        = rec["execution_intent_json"],
            execution_intelligence_json  = rec["execution_intelligence_json"],
            mtf_orderflow_history_json   = rec["mtf_orderflow_history_json"],
            ai_context_json              = rec["ai_context_json"],
            ai_decision_json             = rec["ai_decision_json"],
            automation_policy_json       = rec["automation_policy_json"],
            paper_order_draft_json       = rec["paper_order_draft_json"],
            entry_order_json             = rec["entry_order_json"],
            exit_fill_json               = rec["exit_fill_json"],
            exit_orderflow_snapshot_json = rec["exit_orderflow_snapshot_json"],
        )
        _db_up.session.add(trade)
        _db_up.session.commit()
        return {"ok": True, "trade_id": trade.id, "action": "created",
                "source": "internal_paper"}

    except Exception as _e:
        try:
            _db_up.session.rollback()
        except Exception:
            pass
        return {"ok": False, "error": str(_e)[:200], "source": "internal_paper"}


def _lm_sync_paper_trade_journal_for_item(user_id, item_id):
    """Sync closed paper positions → paper trade journal for one item.

    Returns {ok, item_id, processed, created, updated, skipped, source}.
    Manual only. No Binance API. DB-only.
    """
    from models import (
        db                       as _db_si,
        LiveMonitorPaperPosition as _PP_si,
        LiveMonitorItem          as _LMI_si,
    )
    try:
        item   = _LMI_si.query.filter_by(id=item_id, user_id=user_id).first()
        if not item:
            return {"ok": False, "reason": "item_not_found", "item_id": item_id,
                    "source": "internal_paper"}

        closed = (_PP_si.query
                  .filter_by(user_id=user_id, item_id=item_id, status="closed")
                  .order_by(_PP_si.id)
                  .all())

        processed = created = updated = skipped = 0
        for pos in closed:
            processed += 1
            res = _lm_upsert_paper_trade_from_closed_position(pos, item=item)
            if not res.get("ok"):
                skipped += 1
            elif res.get("action") == "created":
                created += 1
            elif res.get("action") == "updated":
                updated += 1
            else:
                skipped += 1

        return {
            "ok":        True,
            "item_id":   item_id,
            "processed": processed,
            "created":   created,
            "updated":   updated,
            "skipped":   skipped,
            "source":    "internal_paper",
        }
    except Exception as _e:
        return {
            "ok": False, "error": str(_e)[:200],
            "item_id": item_id, "processed": 0,
            "created": 0, "updated": 0, "skipped": 0,
            "source": "internal_paper",
        }


def _lm_sync_paper_trade_journal_for_user(user_id):
    """Sync closed paper positions → journal for all user items.

    Returns aggregate totals. Manual only. No Binance API. DB-only.
    """
    from models import (
        db                       as _db_su,
        LiveMonitorPaperPosition as _PP_su,
    )
    try:
        item_ids = [
            r[0] for r in
            _db_su.session.query(_PP_su.item_id)
            .filter_by(user_id=user_id, status="closed")
            .distinct()
            .all()
        ]

        total_processed = total_created = total_updated = total_skipped = 0
        by_item = []
        for iid in item_ids:
            res = _lm_sync_paper_trade_journal_for_item(user_id, iid)
            total_processed += res.get("processed", 0)
            total_created   += res.get("created",   0)
            total_updated   += res.get("updated",   0)
            total_skipped   += res.get("skipped",   0)
            by_item.append({
                "item_id":   iid,
                "processed": res.get("processed", 0),
                "created":   res.get("created",   0),
                "updated":   res.get("updated",   0),
                "skipped":   res.get("skipped",   0),
            })

        return {
            "ok":             True,
            "total_processed": total_processed,
            "total_created":   total_created,
            "total_updated":   total_updated,
            "total_skipped":   total_skipped,
            "by_item":         by_item,
            "source":          "internal_paper",
        }
    except Exception as _e:
        return {
            "ok": False, "error": str(_e)[:200],
            "total_processed": 0, "total_created": 0,
            "total_updated": 0, "total_skipped": 0,
            "source": "internal_paper",
        }


def _lm_get_paper_trade_journal(user_id, item_id=None, limit=100):
    """Return latest closed paper trades for a user (optionally filtered by item).

    Returns {ok, trades: [...], count, source}.
    No Binance API. DB-only. Read-only.
    """
    from models import (
        LiveMonitorPaperTrade as _PT_gj,
    )
    try:
        q = _PT_gj.query.filter_by(user_id=user_id)
        if item_id is not None:
            q = q.filter_by(item_id=item_id)
        rows = q.order_by(_PT_gj.id.desc()).limit(limit).all()

        trades = []
        for t in rows:
            ai_review = None
            if t.ai_post_trade_review_json:
                ai_review = _json_loads_safe(t.ai_post_trade_review_json, None)

            trades.append({
                "id":              t.id,
                "item_id":         t.item_id,
                "position_id":     t.position_id,
                "symbol":          t.symbol,
                "side":            t.side,
                "quantity":        t.quantity,
                "entry_price":     t.entry_price,
                "exit_price":      t.exit_price,
                "outcome":         t.outcome,
                "outcome_reason":  t.outcome_reason,
                "realized_pnl":    float(t.realized_pnl)     if t.realized_pnl is not None else None,
                "realized_pnl_pct": float(t.realized_pnl_pct) if t.realized_pnl_pct is not None else None,
                "risk_reward":     float(t.risk_reward)      if t.risk_reward is not None else None,
                "duration_seconds": t.duration_seconds,
                "closed_at":       t.closed_at.isoformat() if t.closed_at else None,
                "ai_review_available": ai_review is not None and ai_review.get("ok", False),
                "source":          "internal_paper",
            })

        return {"ok": True, "trades": trades, "count": len(trades), "source": "internal_paper"}
    except Exception as _e:
        return {"ok": False, "error": str(_e)[:200], "trades": [], "count": 0,
                "source": "internal_paper"}


def _lm_build_ai_post_trade_review_context(trade):
    """Build compact AI post-trade review context for one LiveMonitorPaperTrade.

    Returns a structured context dict ready for storage or review.
    No AI call is made here — returns context_only mode.
    No Binance API. DB-only. Read-only.
    """
    try:
        exec_intent = _json_loads_safe(trade.execution_intent_json,    {})
        ai_decision = _json_loads_safe(trade.ai_decision_json,         {})
        autopol     = _json_loads_safe(trade.automation_policy_json,   {})
        ai_ctx      = _json_loads_safe(trade.ai_context_json,          {})
        exit_snap   = _json_loads_safe(trade.exit_snapshot_json,       {})

        pnl_f   = float(trade.realized_pnl)     if trade.realized_pnl is not None else 0.0
        pnl_pct = float(trade.realized_pnl_pct) if trade.realized_pnl_pct is not None else None

        context = {
            "trade_id":       trade.id,
            "symbol":         trade.symbol,
            "side":           trade.side,
            "quantity":       trade.quantity,
            "entry_price":    trade.entry_price,
            "exit_price":     trade.exit_price,
            "outcome":        trade.outcome,
            "outcome_reason": trade.outcome_reason,
            "realized_pnl":   pnl_f,
            "realized_pnl_pct": pnl_pct,
            "risk_reward":    float(trade.risk_reward) if trade.risk_reward is not None else None,
            "duration_seconds": trade.duration_seconds,
            "closed_at":      trade.closed_at.isoformat() if trade.closed_at else None,
            "execution_intent": {
                "direction":    exec_intent.get("direction"),
                "entry_price":  exec_intent.get("entry_price"),
                "stop_loss":    exec_intent.get("stop_loss"),
                "take_profit":  exec_intent.get("take_profit"),
                "risk_reward":  exec_intent.get("risk_reward"),
            } if exec_intent else None,
            "ai_decision_summary": {
                "decision":    ai_decision.get("decision"),
                "confidence":  ai_decision.get("confidence"),
                "reason":      ai_decision.get("reason"),
            } if ai_decision else None,
            "automation_policy_summary": {
                "policy":      autopol.get("policy"),
                "approved":    autopol.get("approved"),
            } if autopol else None,
            "ai_execution_context_available": bool(ai_ctx),
            "exit_snapshot_keys": list(exit_snap.keys()) if exit_snap else [],
            "source": "internal_paper",
        }

        review_shell = {
            "ok":             True,
            "mode":           "context_only",
            "summary":        "AI review context prepared; model call not configured.",
            "learning_points": [],
            "risk_notes":     [],
            "context":        context,
            "source":         "internal_paper",
        }
        return review_shell

    except Exception as _e:
        return {
            "ok": False, "error": str(_e)[:200], "mode": "context_only",
            "source": "internal_paper",
        }
