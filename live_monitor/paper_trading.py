# live_monitor/paper_trading.py — Phase 11.7B: Internal Paper Trading Engine Foundation
#
# SAFETY RULES (permanent):
# - No Binance API. DB-only. No exchange calls of any kind.
# - No auto-submit. No AI direct execution. No background order placement.
# - No order placed unless user manually clicks Submit Paper Order.
# - _lm_bt_signed_request() is NOT called here — read-only GET-only, untouched.
# - No API keys. No secrets. No exchange order IDs in DB.
import time
import uuid
from datetime import datetime, timezone


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


def _as_float(v):
    try:
        if v is None or v == "":
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def _price_str(v):
    f = _as_float(v)
    if f is None:
        return None
    return f"{f:.8f}".rstrip("0").rstrip(".")


def _snapshot_for_item(item, snapshot=None):
    if isinstance(snapshot, dict):
        return snapshot
    return _json_loads_safe(getattr(item, "snapshot_json", None), {}) or {}


def _dict_get_price(obj):
    """Return first usable price from a dict/list-shaped cache object."""
    if obj is None:
        return None
    if isinstance(obj, (int, float, str)):
        return _as_float(obj)
    if isinstance(obj, dict):
        for key in (
            "price", "last_price", "latest_price", "live_price", "current_price",
            "mark_price", "markPrice", "last", "close", "c", "p",
        ):
            f = _as_float(obj.get(key))
            if f is not None and f > 0:
                return f
        for key in ("ticker", "market", "data", "live", "health", "snapshot"):
            f = _dict_get_price(obj.get(key))
            if f is not None and f > 0:
                return f
    return None


def _direction_to_side(direction):
    d = str(direction or "").strip().lower()
    if d in ("buy", "long", "bullish") or "bull" in d:
        return "BUY"
    if d in ("sell", "short", "bearish") or "bear" in d:
        return "SELL"
    return None


def _model_has_attr(model_cls, attr):
    return hasattr(model_cls, attr)


# ── account ───────────────────────────────────────────────────────────────────

def _lm_get_or_create_paper_account(user_id):
    """Return/create the active paper account ORM row for a user.

    Default account: 10,000 USDT, active. DB-only; no exchange credentials.
    """
    from models import db as _db, LiveMonitorPaperAccount as _PA

    row = _PA.query.filter_by(user_id=user_id, status="active").first()
    if not row:
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
        row = (_PA.query.filter_by(user_id=user_id, status="active").first()
               or _PA.query.filter_by(user_id=user_id).first()
               or row)
    return row


def _lm_get_paper_account_summary(user_id) -> dict:
    """Return a safe serializable paper account summary."""
    from models import LiveMonitorPaperOrder as _PO, LiveMonitorPaperPosition as _PP

    acc = _lm_get_or_create_paper_account(user_id)
    open_orders = _PO.query.filter_by(user_id=user_id, status="open").count()
    open_positions = _PP.query.filter_by(user_id=user_id, status="open").count()
    return {
        "ok": True,
        "account_id": getattr(acc, "id", None),
        "user_id": user_id,
        "currency": getattr(acc, "currency", "USDT") or "USDT",
        "starting_balance": float(getattr(acc, "starting_balance", 0) or 0),
        "cash_balance": float(getattr(acc, "cash_balance", 0) or 0),
        "equity": float(getattr(acc, "equity", 0) or 0),
        "realized_pnl": float(getattr(acc, "realized_pnl", 0) or 0),
        "unrealized_pnl": float(getattr(acc, "unrealized_pnl", 0) or 0),
        "status": getattr(acc, "status", "active") or "active",
        "open_orders": open_orders,
        "open_positions": open_positions,
        "phase": "11.7B_internal_paper_trading",
        "computed_at": int(time.time()),
    }


# ── real market price source ──────────────────────────────────────────────────

def _lm_get_real_market_price_for_paper(item, snapshot=None) -> dict:
    """Resolve real futures market price from Live Monitor data only.

    Priority:
      1. Live Monitor live/websocket price cache on main module, when present.
      2. Latest data-health live price stored in snapshot_json.
      3. item.current_price.
      4. Snapshot latest/last-known price fallback.

    This function never calls Binance Testnet, Binance live, or any exchange API.
    """
    snap = _snapshot_for_item(item, snapshot)
    intent = snap.get("latest_execution_intent") or {}
    symbol = str(intent.get("symbol") or getattr(item, "symbol", "") or "").upper().strip()
    item_id = getattr(item, "id", None)

    try:
        import main as _m
        for cache_name in (
            "_lm_live_price_cache", "LM_LIVE_PRICE_CACHE", "LIVE_PRICE_CACHE",
            "live_price_cache", "_live_price_cache", "WS_PRICE_CACHE",
            "_ws_price_cache", "PRICE_CACHE", "_price_cache", "_ticker_cache",
        ):
            cache = getattr(_m, cache_name, None)
            if not isinstance(cache, dict):
                continue
            keys = []
            if symbol:
                keys.extend([symbol, symbol.upper(), symbol.lower()])
            if item_id is not None:
                keys.extend([item_id, str(item_id)])
            for k in keys:
                if k in cache:
                    f = _dict_get_price(cache.get(k))
                    if f is not None and f > 0:
                        return {
                            "ok": True, "price": f, "price_source": f"live_price_cache:{cache_name}",
                            "symbol": symbol or None,
                        }
    except Exception:
        pass

    for health_key in ("latest_data_health", "data_health", "latest_live_data_health"):
        health = snap.get(health_key)
        f = _dict_get_price(health)
        if f is not None and f > 0:
            return {
                "ok": True, "price": f, "price_source": health_key,
                "symbol": symbol or None,
            }

    f = _as_float(getattr(item, "current_price", None))
    if f is not None and f > 0:
        return {"ok": True, "price": f, "price_source": "item.current_price", "symbol": symbol or None}

    for key in (
        "latest_price", "last_known_price", "current_price", "mark_price",
        "latest_market_price", "snapshot_latest_price",
    ):
        f = _as_float(snap.get(key))
        if f is not None and f > 0:
            return {"ok": True, "price": f, "price_source": f"snapshot.{key}", "symbol": symbol or None}

    return {"ok": False, "price": None, "price_source": None, "symbol": symbol or None, "error": "price_missing"}


# ── quantity validation ───────────────────────────────────────────────────────

def _lm_validate_paper_order_quantity(qty_str, price_f, cash_balance=None) -> dict:
    """Validate quantity string for a paper order."""
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

    price_f = _as_float(price_f) or 0.0
    notional = round(qty_f * price_f, 6) if price_f else None
    if cash_balance is not None and notional is not None and notional > float(cash_balance):
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
    """Build a paper LIMIT order draft. No DB write and no exchange call."""
    snap = _snapshot_for_item(item, snapshot)
    intent = snap.get("latest_execution_intent") or {}
    sim = snap.get("latest_execution_simulation") or {}
    ai_dec = snap.get("latest_ai_trade_control_decision") or {}
    pol = snap.get("latest_automation_policy_result") or {}

    symbol = str(intent.get("symbol") or getattr(item, "symbol", "") or "").upper().strip()
    direction = intent.get("direction") or getattr(item, "direction", None)
    side = _direction_to_side(direction)

    # LIMIT entry price remains the strategy entry from execution intent.
    # Real market price is resolved separately and exposed for paper context.
    entry_price_f = _as_float(intent.get("entry_price"))
    market_price = _lm_get_real_market_price_for_paper(item, snap)
    price_f = entry_price_f
    price_source = "execution_intent.entry_price" if entry_price_f else None
    warnings = []
    if price_f is None and market_price.get("ok"):
        price_f = market_price.get("price")
        price_source = market_price.get("price_source")
        warnings.append("entry_price_missing_using_real_market_price_fallback")

    intent_allowed = bool(intent.get("allowed"))
    intent_valid = bool(sim.get("intent_valid", intent_allowed))
    policy_valid = bool(sim.get("policy_valid", pol.get("allowed", True)))
    decision_valid = bool(sim.get("decision_valid", True))
    data_health_ok = bool(sim.get("data_health_ok", True))

    ai_action = str(ai_dec.get("action") or ai_dec.get("decision") or "").lower()
    policy_blocked = (pol.get("allowed") is False) or bool(pol.get("blocked"))
    ai_blocked = ai_action in ("block", "blocked", "pause", "reject", "avoid")

    draft_ready = bool(symbol and side and price_f and intent_allowed and not policy_blocked)
    paper_ready = bool(draft_ready and intent_valid and policy_valid and decision_valid and data_health_ok and not ai_blocked)

    reasons = []
    if not symbol:
        reasons.append("symbol_missing")
    if not side:
        reasons.append("side_missing")
    if not price_f:
        reasons.append("price_missing")
    if not intent_allowed:
        reasons.append("execution_intent_not_allowed")
    if policy_blocked:
        reasons.append("automation_policy_blocked")
    if not intent_valid:
        reasons.append("intent_not_valid")
    if not policy_valid:
        reasons.append("policy_not_valid")
    if not decision_valid:
        reasons.append("decision_not_valid")
    if not data_health_ok:
        reasons.append("data_health_not_ok")
    if ai_blocked:
        reasons.append("ai_trade_control_blocked")

    cash_balance = None
    account_status = None
    try:
        acc = _lm_get_or_create_paper_account(getattr(item, "user_id", None))
        cash_balance = float(getattr(acc, "cash_balance", 0) or 0)
        account_status = getattr(acc, "status", None)
    except Exception:
        reasons.append("paper_account_unavailable")

    qty_result = {"ok": False, "qty_float": None, "notional": None, "error": None, "details": None}
    if quantity_str is not None and str(quantity_str).strip() and price_f:
        qty_result = _lm_validate_paper_order_quantity(quantity_str, price_f, cash_balance)

    return {
        "ok": True,
        "draft_ready": draft_ready,
        "paper_ready": paper_ready,
        "symbol": symbol or None,
        "side": side,
        "type": "LIMIT",
        "order_type": "LIMIT",
        "timeInForce": "GTC",
        "time_in_force": "GTC",
        "price": price_f,
        "price_str": _price_str(price_f),
        "price_float": price_f,
        "price_source": price_source,
        "market_price": market_price.get("price"),
        "market_price_source": market_price.get("price_source"),
        "quantity": str(qty_result["qty_float"]) if qty_result.get("qty_float") else None,
        "qty_float": qty_result.get("qty_float"),
        "estimated_notional": qty_result.get("notional"),
        "cash_balance": cash_balance,
        "paper_account_status": account_status,
        "intent_allowed": intent_allowed,
        "intent_valid": intent_valid,
        "policy_valid": policy_valid,
        "decision_valid": decision_valid,
        "data_health_ok": data_health_ok,
        "policy_blocked": policy_blocked,
        "ai_blocked": ai_blocked,
        "qty_ok": qty_result.get("ok", False),
        "qty_error": qty_result.get("error"),
        "qty_details": qty_result.get("details"),
        "reasons": reasons,
        "warnings": warnings,
        "source": "internal_paper",
        "advisory_note": "No Binance API. DB-only paper record. No real or testnet order placed.",
    }


# ── validation ────────────────────────────────────────────────────────────────

def _lm_validate_paper_order_draft(draft) -> dict:
    """Validate a completed draft dict before manual paper submission."""
    errors = []
    if not draft.get("paper_ready"):
        errors.append("paper_ready_false")
    if not draft.get("symbol"):
        errors.append("symbol_missing")
    if draft.get("side") not in ("BUY", "SELL"):
        errors.append("side_invalid")
    if not _as_float(draft.get("price")):
        errors.append("price_missing")
    if not draft.get("qty_ok"):
        errors.append(draft.get("qty_error") or "quantity_invalid")
    if draft.get("policy_blocked"):
        errors.append("automation_policy_blocked")
    if draft.get("ai_blocked"):
        errors.append("ai_trade_control_blocked")
    return {"ok": len(errors) == 0, "errors": errors}


# ── submit ────────────────────────────────────────────────────────────────────

def _lm_submit_paper_order(user_id, item, quantity_str) -> dict:
    """Create and persist a manual paper LIMIT order. No exchange call. DB-only."""
    from models import db as _db, LiveMonitorPaperOrder as _PO

    if getattr(item, "user_id", None) != user_id:
        return {"ok": False, "error": "item_not_owned"}

    snap = _snapshot_for_item(item)
    acc = _lm_get_or_create_paper_account(user_id)
    draft = _lm_build_paper_order_draft(item, snapshot=snap, quantity_str=quantity_str)

    val = _lm_validate_paper_order_draft(draft)
    if not val["ok"]:
        return {"ok": False, "error": "draft_invalid", "errors": val["errors"], "draft": draft}

    now = datetime.now(timezone.utc)
    client_order_id = f"ZYNI_PAPER_{uuid.uuid4().hex[:12].upper()}"
    price_str = _price_str(draft.get("price"))
    quantity = str(draft.get("quantity") or "").strip()
    request_payload = {
        "source": "internal_paper",
        "manual_submit": True,
        "symbol": draft.get("symbol"),
        "side": draft.get("side"),
        "type": "LIMIT",
        "timeInForce": "GTC",
        "quantity": quantity,
        "price": draft.get("price"),
        "estimated_notional": draft.get("estimated_notional"),
    }

    kwargs = dict(
        user_id=user_id,
        item_id=item.id,
        symbol=draft.get("symbol"),
        side=draft.get("side"),
        order_type="LIMIT",
        time_in_force="GTC",
        quantity=quantity,
        price=price_str,
        status="open",
        fill_status="unfilled",
        client_order_id=client_order_id,
        source="internal_paper",
        execution_intent_json=_json_dumps_safe(snap.get("latest_execution_intent") or {}),
        execution_simulation_json=_json_dumps_safe(snap.get("latest_execution_simulation") or {}),
        ai_decision_json=_json_dumps_safe(snap.get("latest_ai_trade_control_decision") or {}),
        automation_policy_json=_json_dumps_safe(snap.get("latest_automation_policy_result") or {}),
    )
    optional = {
        "account_id": getattr(acc, "id", None),
        "filled_qty": "0",
        "avg_fill_price": None,
        "request_json": _json_dumps_safe(request_payload),
        "response_json": _json_dumps_safe({"status": "open", "fill_status": "unfilled", "source": "internal_paper"}),
        "error_json": None,
        "submitted_at": now,
    }
    if _model_has_attr(_PO, "estimated_notional"):
        optional["estimated_notional"] = draft.get("estimated_notional")
    for key, value in optional.items():
        if _model_has_attr(_PO, key):
            kwargs[key] = value

    try:
        row = _PO(**kwargs)
        _db.session.add(row)
        _db.session.commit()
    except Exception as _e:
        try:
            _db.session.rollback()
        except Exception:
            pass
        return {"ok": False, "error": "db_error", "detail": str(_e)}

    return {
        "ok": True,
        "order_id": row.id,
        "account_id": getattr(acc, "id", None),
        "client_order_id": client_order_id,
        "symbol": draft.get("symbol"),
        "side": draft.get("side"),
        "type": "LIMIT",
        "order_type": "LIMIT",
        "timeInForce": "GTC",
        "time_in_force": "GTC",
        "price": draft.get("price"),
        "price_str": price_str,
        "quantity": quantity,
        "estimated_notional": draft.get("estimated_notional"),
        "status": "open",
        "fill_status": "unfilled",
        "source": "internal_paper",
        "submitted_at": now.isoformat(),
        "advisory_note": "No Binance API. DB-only paper record. No real or testnet order placed.",
    }


# ── query helpers ─────────────────────────────────────────────────────────────

def _lm_get_paper_orders(user_id, item_id=None, limit=50) -> list:
    """Return recent paper orders as safe dicts."""
    from models import LiveMonitorPaperOrder as _PO

    q = _PO.query.filter_by(user_id=user_id)
    if item_id is not None:
        q = q.filter_by(item_id=item_id)
    rows = q.order_by(_PO.created_at.desc()).limit(limit).all()
    out = []
    for r in rows:
        out.append({
            "id": getattr(r, "id", None),
            "account_id": getattr(r, "account_id", None),
            "item_id": getattr(r, "item_id", None),
            "symbol": getattr(r, "symbol", None),
            "side": getattr(r, "side", None),
            "type": getattr(r, "order_type", None),
            "order_type": getattr(r, "order_type", None),
            "timeInForce": getattr(r, "time_in_force", None),
            "time_in_force": getattr(r, "time_in_force", None),
            "quantity": getattr(r, "quantity", None),
            "price": _as_float(getattr(r, "price", None)) or getattr(r, "price", None),
            "status": getattr(r, "status", None),
            "fill_status": getattr(r, "fill_status", None),
            "filled_qty": getattr(r, "filled_qty", "0"),
            "avg_fill_price": getattr(r, "avg_fill_price", None),
            "client_order_id": getattr(r, "client_order_id", None),
            "source": getattr(r, "source", "internal_paper"),
            "estimated_notional": float(getattr(r, "estimated_notional", 0) or 0) if hasattr(r, "estimated_notional") else None,
            "created_at": r.created_at.isoformat() if getattr(r, "created_at", None) else None,
            "submitted_at": r.submitted_at.isoformat() if getattr(r, "submitted_at", None) else None,
            "filled_at": r.filled_at.isoformat() if getattr(r, "filled_at", None) else None,
            "updated_at": r.updated_at.isoformat() if getattr(r, "updated_at", None) else None,
        })
    return out


def _lm_get_paper_positions(user_id, item_id=None) -> list:
    """Return open paper positions as safe dicts (rows arrive in 11.7C fill engine)."""
    from models import LiveMonitorPaperPosition as _PP

    q = _PP.query.filter_by(user_id=user_id)
    if item_id is not None:
        q = q.filter_by(item_id=item_id)
    rows = q.order_by(_PP.created_at.desc() if hasattr(_PP, "created_at") else _PP.id.desc()).limit(50).all()
    out = []
    for r in rows:
        quantity = getattr(r, "quantity", None) or getattr(r, "size", None)
        out.append({
            "id": getattr(r, "id", None),
            "account_id": getattr(r, "account_id", None),
            "item_id": getattr(r, "item_id", None),
            "symbol": getattr(r, "symbol", None),
            "side": getattr(r, "side", None),
            "quantity": quantity,
            "size": quantity,
            "entry_price": getattr(r, "entry_price", None),
            "mark_price": getattr(r, "mark_price", None),
            "status": getattr(r, "status", None),
            "realized_pnl": float(getattr(r, "realized_pnl", 0) or 0),
            "unrealized_pnl": float(getattr(r, "unrealized_pnl", 0) or 0),
            "opened_at": r.opened_at.isoformat() if getattr(r, "opened_at", None) else (r.created_at.isoformat() if getattr(r, "created_at", None) else None),
            "closed_at": r.closed_at.isoformat() if getattr(r, "closed_at", None) else None,
            "updated_at": r.updated_at.isoformat() if getattr(r, "updated_at", None) else None,
        })
    return out
