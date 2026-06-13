"""Phase 11.13 Hotfix: Paper Risk Guard for Internal Paper Trading.

Evaluates risk before a manual paper order is submitted.
DB-only. No Binance API. No auto-execution. No live mode.
can_auto_submit is ALWAYS False. auto_execution_allowed is ALWAYS False.
"""
from __future__ import annotations

import json as _json_rg
import time as _time_rg
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from datetime import datetime, timezone, date as _date


# ── Decimal helpers ───────────────────────────────────────────────────────────

def _safe_dec(v):
    """Convert value to Decimal. Returns None on any failure, NaN, or Infinity."""
    if v is None:
        return None
    try:
        d = Decimal(str(v).strip())
        if d.is_nan() or d.is_infinite():
            return None
        return d
    except (InvalidOperation, TypeError, ValueError):
        return None


def _d_str(d):
    """Serialize Decimal as a compact fixed-point string (no trailing zeros)."""
    if d is None:
        return None
    try:
        s = format(d, 'f')
        if '.' in s:
            s = s.rstrip('0').rstrip('.')
        return s
    except Exception:
        return str(d)


# ── Default settings ──────────────────────────────────────────────────────────

_RG_SETTINGS_VERSION = "phase11_13_hotfix_1"

_RG_DEFAULT_SETTINGS: dict = {
    "settings_version":               _RG_SETTINGS_VERSION,
    "max_risk_pct_per_trade":         1.0,
    "max_open_paper_positions":       3,
    "max_open_paper_orders":          3,
    "max_symbol_open_positions":      1,
    "max_symbol_open_orders":         1,
    "min_risk_reward":                1.5,
    "max_daily_realized_loss_pct":    3.0,
    "max_consecutive_losses_warning": 3,
    "min_account_equity":             50.0,
    "allow_if_no_stop_loss":          False,
    "allow_if_no_take_profit":        False,
    "allow_if_no_entry_price":        False,
    # Optional retained controls — None = no hard limit
    "max_risk_amount_per_trade":      None,
    "max_position_notional":          None,
    "source":                         "paper_risk_guard_defaults",
}

# Exported constant for callers
_lm_default_paper_risk_settings: dict = dict(_RG_DEFAULT_SETTINGS)

# Safe numeric ranges for validation
_RG_NUMERIC_RANGES: dict = {
    "max_risk_pct_per_trade":         (0.1,   5.0),
    "max_open_paper_positions":       (1,     20),
    "max_open_paper_orders":          (1,     20),
    "max_symbol_open_positions":      (1,     5),
    "max_symbol_open_orders":         (1,     5),
    "min_risk_reward":                (0.5,   10.0),
    "max_daily_realized_loss_pct":    (0.5,   20.0),
    "max_consecutive_losses_warning": (1,     20),
    "min_account_equity":             (0.0,   1_000_000_000.0),
    # Optional — only validated when not None
    "max_risk_amount_per_trade":      (0.001, 1_000_000_000.0),
    "max_position_notional":          (0.001, 1_000_000_000.0),
}

_RG_BOOL_FIELDS = {"allow_if_no_stop_loss", "allow_if_no_take_profit", "allow_if_no_entry_price"}

_RG_INT_FIELDS = {
    "max_open_paper_positions", "max_open_paper_orders",
    "max_symbol_open_positions", "max_symbol_open_orders",
    "max_consecutive_losses_warning",
}

_OPEN_STATUSES = {"open", "active", "pending", "submitted", "partially_filled"}

_RG_GUARDRAILS_BASE = {
    "auto_execution_allowed": False,
    "ai_can_execute":         False,
    "live_enabled":           False,
    "paper_primary":          True,
    "can_auto_submit":        False,
}


# ── Bool parser ───────────────────────────────────────────────────────────────

def _parse_bool(v) -> bool | None:
    """Parse a boolean from various representations. Returns None on failure."""
    if isinstance(v, bool):
        return v
    if isinstance(v, int):
        return bool(v)
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("true", "1", "yes"):
            return True
        if s in ("false", "0", "no"):
            return False
    return None


# ── Settings normalization ────────────────────────────────────────────────────

def _lm_normalize_paper_risk_guard_settings(  # noqa: C901
    raw_settings,
    for_write: bool = False,
) -> tuple[dict, dict]:
    """Normalize and validate risk guard settings.

    Returns (normalized_dict, field_errors_dict).

    for_write=False (read/merge mode):
      - merge with safe defaults
      - clamp out-of-range values
      - never throw

    for_write=True (POST validation mode):
      - reject malformed values
      - reject NaN, Infinity, negatives
      - return field_errors on any problem
      - do not persist if errors exist
    """
    raw = raw_settings if isinstance(raw_settings, dict) else {}
    field_errors: dict = {}
    merged = dict(_RG_DEFAULT_SETTINGS)

    # ── Legacy key compatibility ──────────────────────────────────────────────
    is_legacy = raw.get("settings_version") != _RG_SETTINGS_VERSION

    # require_sl → allow_if_no_stop_loss
    if "require_sl" in raw and "allow_if_no_stop_loss" not in raw:
        b = _parse_bool(raw["require_sl"])
        if b is not None:
            raw = dict(raw)
            raw["allow_if_no_stop_loss"] = not b

    # require_tp → allow_if_no_take_profit — legacy require_tp=false is UNSAFE default
    if "require_tp" in raw and "allow_if_no_take_profit" not in raw:
        b = _parse_bool(raw["require_tp"])
        if b is not None:
            if is_legacy and not b:
                pass  # skip legacy unsafe default; new default (require TP) applies
            else:
                raw = dict(raw)
                raw["allow_if_no_take_profit"] = not b

    # min_rr → min_risk_reward
    if "min_rr" in raw and "min_risk_reward" not in raw:
        raw = dict(raw)
        raw["min_risk_reward"] = raw["min_rr"]

    # ── Boolean fields ────────────────────────────────────────────────────────
    for bf in _RG_BOOL_FIELDS:
        if bf not in raw:
            continue
        v = raw[bf]
        b = _parse_bool(v)
        if b is None:
            if for_write:
                field_errors[bf] = f"must_be_true_or_false"
            # on read, keep default
        else:
            merged[bf] = b

    # ── Numeric fields ────────────────────────────────────────────────────────
    for key, (lo, hi) in _RG_NUMERIC_RANGES.items():
        if key not in raw:
            continue
        raw_val = raw[key]

        # Optional fields — None means no limit
        if raw_val is None and key in ("max_risk_amount_per_trade", "max_position_notional"):
            merged[key] = None
            continue

        d = _safe_dec(raw_val)
        if d is None:
            if for_write:
                field_errors[key] = f"must_be_a_valid_number"
            # on read, keep default
            continue

        float_val = float(d)

        if float_val < lo or float_val > hi:
            if for_write:
                field_errors[key] = f"must_be_between_{lo}_and_{hi}"
            else:
                # clamp
                float_val = max(lo, min(hi, float_val))

        if key in _RG_INT_FIELDS:
            merged[key] = int(round(float_val))
        else:
            merged[key] = float_val

    # settings_version is set by the writer, not the caller
    merged["settings_version"] = _RG_SETTINGS_VERSION
    merged["source"] = "paper_risk_guard_user"

    return merged, field_errors


# ── Settings load ─────────────────────────────────────────────────────────────

def _lm_get_paper_risk_guard_settings(user_id) -> dict:
    """Load per-user risk guard limits from UserPreference. Returns safe defaults if not set."""
    try:
        from models import UserPreference as _UP
        pref = _UP.query.filter_by(user_id=user_id).first()
        if pref is None:
            return dict(_RG_DEFAULT_SETTINGS)
        raw = getattr(pref, "paper_risk_guard_settings_json", None)
        if not raw:
            return dict(_RG_DEFAULT_SETTINGS)
        loaded = _json_rg.loads(raw)
        if not isinstance(loaded, dict):
            return dict(_RG_DEFAULT_SETTINGS)
        normalized, _ = _lm_normalize_paper_risk_guard_settings(loaded, for_write=False)
        return normalized
    except Exception:
        return dict(_RG_DEFAULT_SETTINGS)


# ── Settings update ───────────────────────────────────────────────────────────

def _lm_update_paper_risk_guard_settings(user_id, settings_dict: dict) -> dict:
    """Validate and persist per-user risk guard limits.

    Auto-creates UserPreference if missing.
    Returns field_errors on invalid input.
    """
    try:
        from models import db as _db_rg, UserPreference as _UP
    except Exception as _ie:
        return {"ok": False, "error": f"import_error:{str(_ie)[:80]}"}

    normalized, field_errors = _lm_normalize_paper_risk_guard_settings(
        settings_dict if isinstance(settings_dict, dict) else {},
        for_write=True,
    )
    if field_errors:
        return {"ok": False, "error": "invalid_risk_settings", "field_errors": field_errors}

    try:
        pref = _UP.query.filter_by(user_id=user_id).first()
        if pref is None:
            pref = _UP(
                user_id        = user_id,
                execution_mode = "internal_paper",
                policy_mode    = "paper_manual",
            )
            _db_rg.session.add(pref)
            try:
                _db_rg.session.commit()
            except Exception:
                _db_rg.session.rollback()
                pref = _UP.query.filter_by(user_id=user_id).first()
                if pref is None:
                    return {"ok": False, "error": "preference_create_failed"}

        if not hasattr(pref, "paper_risk_guard_settings_json"):
            return {"ok": False, "error": "column_not_migrated"}

        pref.paper_risk_guard_settings_json = _json_rg.dumps(normalized)
        _db_rg.session.commit()
        return {"ok": True, "settings": normalized}
    except Exception as _e:
        return {"ok": False, "error": str(_e)[:120]}


# ── Daily loss query ──────────────────────────────────────────────────────────

def _lm_compute_daily_loss(user_id, equity_dec) -> dict:
    """Compute today's realized loss from LiveMonitorPaperTrade records.

    Returns dict with loss_amount (Decimal), loss_pct_of_equity (Decimal),
    trades_counted, date_utc, available.
    """
    now_utc   = datetime.now(timezone.utc)
    today_str = now_utc.date().isoformat()
    today_start = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)

    try:
        from models import LiveMonitorPaperTrade as _LMPTrade
        closed_trades = (
            _LMPTrade.query
            .filter(
                _LMPTrade.user_id == user_id,
                _LMPTrade.status  == "closed",
            )
            .all()
        )

        today_trades = []
        for t in closed_trades:
            ts = t.closed_at or t.created_at
            if ts is None:
                continue
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            if ts >= today_start:
                today_trades.append(t)

        loss_total = Decimal("0")
        for t in today_trades:
            pnl = _safe_dec(t.realized_pnl)
            if pnl is not None and pnl < 0:
                loss_total += abs(pnl)

        loss_pct = Decimal("0")
        if equity_dec and equity_dec > 0:
            loss_pct = (loss_total / equity_dec) * Decimal("100")

        return {
            "available":         True,
            "date_utc":          today_str,
            "loss_amount":       loss_total,
            "loss_pct_of_equity": loss_pct,
            "trades_counted":    len(today_trades),
            "denominator":       "current_equity",
        }
    except Exception:
        return {
            "available":         False,
            "date_utc":          today_str,
            "loss_amount":       None,
            "loss_pct_of_equity": None,
            "trades_counted":    0,
            "denominator":       "current_equity",
        }


def _lm_compute_consecutive_losses(user_id) -> int:
    """Count consecutive losses from most recent closed paper trades."""
    try:
        from models import LiveMonitorPaperTrade as _LMPTrade
        recent = (
            _LMPTrade.query
            .filter(
                _LMPTrade.user_id == user_id,
                _LMPTrade.status  == "closed",
            )
            .order_by(_LMPTrade.created_at.desc())
            .limit(30)
            .all()
        )
        count = 0
        for t in recent:
            pnl = _safe_dec(t.realized_pnl)
            if pnl is not None and pnl < 0:
                count += 1
            else:
                break
        return count
    except Exception:
        return 0


# ── Main builder ──────────────────────────────────────────────────────────────

def _lm_build_paper_risk_guard(  # noqa: C901
    item,
    snapshot=None,
    quantity_str=None,
) -> dict:
    """Evaluate paper risk guard for an internal paper order.

    Phase 11.13 Hotfix. No execution. No exchange. No API keys.
    can_auto_submit is ALWAYS False.
    """
    now_ts = int(_time_rg.time())
    now_iso = datetime.now(timezone.utc).isoformat()
    blocking_reasons: list[str] = []
    warnings:         list[str] = []

    item_id  = getattr(item, "id",      None)
    user_id  = getattr(item, "user_id", None)
    symbol   = (getattr(item, "symbol", "") or "").upper().strip()

    # Load snapshot
    if snapshot is None:
        snap = {}
        try:
            raw = getattr(item, "snapshot_json", None)
            if raw:
                snap = _json_rg.loads(raw)
        except Exception:
            pass
    else:
        snap = snapshot if isinstance(snapshot, dict) else {}

    # Load limits
    limits = _lm_get_paper_risk_guard_settings(user_id) if user_id else dict(_RG_DEFAULT_SETTINGS)

    # ── Load dependencies ─────────────────────────────────────────────────────
    try:
        from live_monitor.execution_account import _lm_get_execution_mode_summary
        from live_monitor.paper_trading import (
            _lm_direction_to_paper_side,
            _lm_get_paper_account_summary,
            _lm_get_paper_orders,
            _lm_get_paper_positions,
            _lm_get_real_market_price_for_paper,
        )
        from live_monitor.paper_auto_gate import _lm_get_paper_auto_gate_state
    except Exception as _imp_err:
        _err = f"import_error:{str(_imp_err)[:80]}"
        return _rg_error_response(now_iso, [_err], limits)

    # ─────────────────────────────────────────────────────────────────────────
    # RULE: Execution mode guardrails
    # ─────────────────────────────────────────────────────────────────────────
    exec_summary = {}
    exec_mode    = "internal_paper"
    try:
        if user_id:
            exec_summary = _lm_get_execution_mode_summary(user_id) or {}
            exec_mode    = exec_summary.get("execution_mode", "internal_paper")
    except Exception:
        pass

    paper_primary              = exec_summary.get("paper_primary", True)
    live_disabled              = exec_summary.get("live_disabled", True)
    testnet_strategy_validation = exec_summary.get("testnet_strategy_validation", False)
    ai_can_execute             = exec_summary.get("ai_can_execute", False)

    # Guardrails violation → block
    if not paper_primary:
        blocking_reasons.append("execution_mode_invalid")
    if not live_disabled:
        blocking_reasons.append("live_disabled_required")
    if testnet_strategy_validation:
        blocking_reasons.append("execution_mode_invalid")
    if ai_can_execute:
        blocking_reasons.append("execution_mode_invalid")

    # Testnet selected → warn only (paper trading still available)
    if exec_mode == "binance_testnet":
        warnings.append("current_mode_binance_testnet_api_testing_only")

    guardrails = {
        "execution_mode":                exec_mode,
        "paper_primary":                 True,
        "primary_strategy_testing_mode": "internal_paper",
        "live_disabled":                 True,
        "testnet_strategy_validation":   False,
        "can_auto_submit":               False,
        "auto_execution_allowed":        False,
        "ai_can_execute":                False,
    }

    # ─────────────────────────────────────────────────────────────────────────
    # RULE: Paper account — equity
    # ─────────────────────────────────────────────────────────────────────────
    paper_account   = {}
    cash_balance_dec = Decimal("0")
    equity_dec       = Decimal("0")
    account_ok       = False
    try:
        if user_id:
            paper_account = _lm_get_paper_account_summary(user_id) or {}
            cb = _safe_dec(paper_account.get("cash_balance"))
            eq = _safe_dec(paper_account.get("equity"))
            if cb is not None:
                cash_balance_dec = cb
                account_ok = True
            if eq is not None:
                equity_dec = eq
            else:
                equity_dec = cash_balance_dec
    except Exception:
        pass

    if not account_ok:
        blocking_reasons.append("paper_account_unavailable")
    elif equity_dec <= 0:
        blocking_reasons.append("account_unavailable")
    elif equity_dec < Decimal(str(limits.get("min_account_equity", 50.0))):
        blocking_reasons.append("account_equity_below_minimum")

    min_equity_str = _d_str(Decimal(str(limits.get("min_account_equity", 50.0))))

    # ─────────────────────────────────────────────────────────────────────────
    # RULE: Execution intent — read with key fallback
    # ─────────────────────────────────────────────────────────────────────────
    intent_raw = snap.get("latest_execution_intent") or snap.get("execution_intent")
    intent: dict = {}
    if intent_raw:
        if isinstance(intent_raw, dict):
            intent = intent_raw
        else:
            try:
                intent = _json_rg.loads(intent_raw)
            except Exception:
                intent = {}

    # ─────────────────────────────────────────────────────────────────────────
    # RULE: Direction — use existing mapper
    # ─────────────────────────────────────────────────────────────────────────
    direction_raw = (
        intent.get("direction")
        or getattr(item, "direction", "")
        or ""
    )
    paper_side = _lm_direction_to_paper_side(direction_raw)
    # BUY → LONG, SELL → SHORT
    if paper_side == "BUY":
        direction = "LONG"
    elif paper_side == "SELL":
        direction = "SHORT"
    else:
        direction = ""

    if not direction:
        blocking_reasons.append("direction_missing")

    # ─────────────────────────────────────────────────────────────────────────
    # RULE: Entry price
    # ─────────────────────────────────────────────────────────────────────────
    entry_dec = (
        _safe_dec(intent.get("entry_price"))
        or _safe_dec(intent.get("entry"))
        or _safe_dec(intent.get("limit_price"))
    )
    allow_if_no_entry = bool(limits.get("allow_if_no_entry_price", False))
    if entry_dec is None or entry_dec <= 0:
        entry_dec = None
        if not allow_if_no_entry:
            blocking_reasons.append("entry_price_missing")
        else:
            warnings.append("entry_price_missing")

    # ─────────────────────────────────────────────────────────────────────────
    # RULE: Stop loss
    # ─────────────────────────────────────────────────────────────────────────
    sl_dec = (
        _safe_dec(intent.get("sl_price"))
        or _safe_dec(intent.get("stop_loss"))
        or _safe_dec(intent.get("sl"))
    )
    allow_if_no_sl = bool(limits.get("allow_if_no_stop_loss", False))
    if sl_dec is None or sl_dec <= 0:
        sl_dec = None
        if not allow_if_no_sl:
            blocking_reasons.append("stop_loss_missing")
        else:
            warnings.append("stop_loss_missing")

    # ─────────────────────────────────────────────────────────────────────────
    # RULE: Take profit
    # ─────────────────────────────────────────────────────────────────────────
    tp_dec = (
        _safe_dec(intent.get("tp_price"))
        or _safe_dec(intent.get("take_profit"))
        or _safe_dec(intent.get("tp"))
    )
    allow_if_no_tp = bool(limits.get("allow_if_no_take_profit", False))
    if tp_dec is None or tp_dec <= 0:
        tp_dec = None
        if not allow_if_no_tp:
            blocking_reasons.append("take_profit_missing")
        else:
            warnings.append("take_profit_missing")

    # ─────────────────────────────────────────────────────────────────────────
    # RULE: Quantity
    # ─────────────────────────────────────────────────────────────────────────
    qty_dec = None
    if quantity_str is not None:
        qty_dec = _safe_dec(quantity_str)
        if qty_dec is None:
            blocking_reasons.append("quantity_missing")
        elif qty_dec <= 0:
            blocking_reasons.append("quantity_missing")
    else:
        blocking_reasons.append("quantity_missing")

    # ─────────────────────────────────────────────────────────────────────────
    # GEOMETRY CHECK — only when all prices are valid and direction is known
    # ─────────────────────────────────────────────────────────────────────────
    risk_per_unit_dec    = None
    reward_per_unit_dec  = None
    notional_dec         = None
    risk_amount_dec      = None
    reward_amount_dec    = None
    risk_pct_dec         = None
    risk_reward_dec      = None

    if entry_dec and direction:
        # SL geometry
        if sl_dec:
            if direction == "LONG":
                if sl_dec >= entry_dec:
                    blocking_reasons.append("invalid_stop_distance")
                else:
                    risk_per_unit_dec = entry_dec - sl_dec
            elif direction == "SHORT":
                if sl_dec <= entry_dec:
                    blocking_reasons.append("invalid_stop_distance")
                else:
                    risk_per_unit_dec = sl_dec - entry_dec

            if risk_per_unit_dec is not None and risk_per_unit_dec <= 0:
                blocking_reasons.append("invalid_stop_distance")
                risk_per_unit_dec = None

        # TP geometry
        if tp_dec:
            if direction == "LONG":
                if tp_dec <= entry_dec:
                    blocking_reasons.append("invalid_reward_distance")
                else:
                    reward_per_unit_dec = tp_dec - entry_dec
            elif direction == "SHORT":
                if tp_dec >= entry_dec:
                    blocking_reasons.append("invalid_reward_distance")
                else:
                    reward_per_unit_dec = entry_dec - tp_dec

            if reward_per_unit_dec is not None and reward_per_unit_dec <= 0:
                blocking_reasons.append("invalid_reward_distance")
                reward_per_unit_dec = None

    # ─────────────────────────────────────────────────────────────────────────
    # FINANCIAL CALCULATIONS (only when all components valid)
    # ─────────────────────────────────────────────────────────────────────────
    if entry_dec and qty_dec and entry_dec > 0 and qty_dec > 0:
        notional_dec = entry_dec * qty_dec

    if risk_per_unit_dec and qty_dec and risk_per_unit_dec > 0 and qty_dec > 0:
        risk_amount_dec = risk_per_unit_dec * qty_dec

    if reward_per_unit_dec and qty_dec and reward_per_unit_dec > 0 and qty_dec > 0:
        reward_amount_dec = reward_per_unit_dec * qty_dec

    if risk_amount_dec and equity_dec and equity_dec > 0:
        risk_pct_dec = (risk_amount_dec / equity_dec) * Decimal("100")

    if risk_per_unit_dec and reward_per_unit_dec and risk_per_unit_dec > 0:
        risk_reward_dec = reward_per_unit_dec / risk_per_unit_dec

    # Guard: if no valid risk calc, block as invalid geometry
    if risk_per_unit_dec is None and sl_dec is not None and entry_dec is not None and direction:
        if "invalid_stop_distance" not in blocking_reasons:
            blocking_reasons.append("invalid_stop_distance")

    if reward_per_unit_dec is None and tp_dec is not None and entry_dec is not None and direction:
        if "invalid_reward_distance" not in blocking_reasons:
            blocking_reasons.append("invalid_reward_distance")

    # ─────────────────────────────────────────────────────────────────────────
    # RULE: Notional cap
    # ─────────────────────────────────────────────────────────────────────────
    max_notional = limits.get("max_position_notional")
    if max_notional is not None:
        mn_dec = _safe_dec(max_notional)
        if mn_dec and notional_dec and notional_dec > mn_dec:
            blocking_reasons.append("notional_exceeds_max")

    # ─────────────────────────────────────────────────────────────────────────
    # RULE: Dollar risk cap
    # ─────────────────────────────────────────────────────────────────────────
    max_risk_amt = limits.get("max_risk_amount_per_trade")
    if max_risk_amt is not None:
        mr_dec = _safe_dec(max_risk_amt)
        if mr_dec and risk_amount_dec and risk_amount_dec > mr_dec:
            blocking_reasons.append("risk_amount_exceeds_max")

    # ─────────────────────────────────────────────────────────────────────────
    # RULE: Risk % cap
    # ─────────────────────────────────────────────────────────────────────────
    max_risk_pct = Decimal(str(limits.get("max_risk_pct_per_trade", 1.0)))
    if risk_pct_dec is not None and risk_pct_dec > max_risk_pct:
        blocking_reasons.append("risk_pct_exceeds_max")

    # Warning: risk elevated (> 70% of max but below max)
    if risk_pct_dec is not None and risk_pct_dec > max_risk_pct * Decimal("0.7"):
        if "risk_pct_exceeds_max" not in blocking_reasons:
            warnings.append("risk_pct_elevated")

    # ─────────────────────────────────────────────────────────────────────────
    # RULE: Min R:R
    # ─────────────────────────────────────────────────────────────────────────
    min_rr = Decimal(str(limits.get("min_risk_reward", limits.get("min_rr", 1.5))))
    if risk_reward_dec is not None:
        if risk_reward_dec < min_rr:
            blocking_reasons.append("rr_below_minimum")
    elif risk_per_unit_dec and not reward_per_unit_dec:
        # SL set but no TP — already blocked for missing TP
        pass

    # ─────────────────────────────────────────────────────────────────────────
    # RULE: Insufficient balance
    # ─────────────────────────────────────────────────────────────────────────
    if notional_dec and cash_balance_dec > 0 and notional_dec > cash_balance_dec:
        blocking_reasons.append("insufficient_cash_balance")

    # ─────────────────────────────────────────────────────────────────────────
    # OPEN STATE — user-scoped, all items
    # ─────────────────────────────────────────────────────────────────────────
    open_positions_list    = []
    open_orders_list       = []
    total_open_positions   = 0
    total_open_orders      = 0
    same_sym_positions     = 0
    same_sym_orders        = 0

    try:
        if user_id:
            all_orders = _lm_get_paper_orders(user_id, item_id=None, limit=500) or []
            open_orders_list = [
                o for o in all_orders
                if (o.get("status") or "").lower() in _OPEN_STATUSES
            ]
            total_open_orders = len(open_orders_list)
            if symbol:
                same_sym_orders = sum(
                    1 for o in open_orders_list
                    if (o.get("symbol") or "").upper() == symbol
                )

            all_positions = _lm_get_paper_positions(user_id, item_id=None) or []
            open_positions_list = [
                p for p in all_positions
                if (p.get("status") or "").lower() in {"open", "active"}
            ]
            total_open_positions = len(open_positions_list)
            if symbol:
                same_sym_positions = sum(
                    1 for p in open_positions_list
                    if (p.get("symbol") or "").upper() == symbol
                )
    except Exception:
        pass

    max_pos = int(limits.get("max_open_paper_positions", 3))
    max_ord = int(limits.get("max_open_paper_orders", 3))
    max_sym_pos = int(limits.get("max_symbol_open_positions", 1))
    max_sym_ord = int(limits.get("max_symbol_open_orders", 1))

    if total_open_positions >= max_pos:
        blocking_reasons.append("max_open_positions_reached")
    if total_open_orders >= max_ord:
        blocking_reasons.append("max_open_orders_reached")
    if symbol and same_sym_positions >= max_sym_pos:
        blocking_reasons.append("same_symbol_position_exists")
    if symbol and same_sym_orders >= max_sym_ord:
        blocking_reasons.append("same_symbol_order_exists")

    # ─────────────────────────────────────────────────────────────────────────
    # RULE: AI trade control hard block
    # ─────────────────────────────────────────────────────────────────────────
    _atc = snap.get("latest_ai_trade_control_decision") or snap.get("latest_ai_trade_control") or {}
    if not isinstance(_atc, dict):
        try:
            _atc = _json_rg.loads(_atc) if _atc else {}
        except Exception:
            _atc = {}
    _atc_action = (_atc.get("action") or _atc.get("decision") or "").lower()
    if _atc_action in ("block_trade", "pause_setup"):
        blocking_reasons.append("ai_trade_control_hard_blocked")

    # ─────────────────────────────────────────────────────────────────────────
    # DAILY LOSS
    # ─────────────────────────────────────────────────────────────────────────
    daily_loss_info: dict = {}
    daily_loss_available = False
    try:
        if user_id:
            dl = _lm_compute_daily_loss(user_id, equity_dec if equity_dec > 0 else None)
            daily_loss_available = dl.get("available", False)
            daily_loss_info = {
                "date_utc":          dl.get("date_utc", ""),
                "loss_amount":       _d_str(dl.get("loss_amount")),
                "loss_pct_of_equity": _d_str(dl.get("loss_pct_of_equity")),
                "limit_pct":         _d_str(Decimal(str(limits.get("max_daily_realized_loss_pct", 3.0)))),
                "trades_counted":    dl.get("trades_counted", 0),
                "denominator":       "current_equity",
            }
            if daily_loss_available and dl.get("loss_pct_of_equity") is not None:
                loss_pct_val = dl["loss_pct_of_equity"]
                max_daily    = Decimal(str(limits.get("max_daily_realized_loss_pct", 3.0)))
                if loss_pct_val >= max_daily:
                    blocking_reasons.append("daily_loss_limit_reached")
            elif not daily_loss_available:
                warnings.append("journal_feedback_unavailable")
    except Exception:
        warnings.append("journal_feedback_unavailable")
        daily_loss_info = {}

    # ─────────────────────────────────────────────────────────────────────────
    # CONSECUTIVE LOSSES WARNING
    # ─────────────────────────────────────────────────────────────────────────
    consecutive_losses = 0
    journal_feedback: dict = {"available": False, "consecutive_losses": 0}
    try:
        if user_id:
            consecutive_losses = _lm_compute_consecutive_losses(user_id)
            max_consec = int(limits.get("max_consecutive_losses_warning", 3))
            if consecutive_losses >= max_consec:
                warnings.append("consecutive_loss_warning")
            journal_feedback = {
                "available":          True,
                "consecutive_losses": consecutive_losses,
            }
    except Exception:
        if "journal_feedback_unavailable" not in warnings:
            warnings.append("journal_feedback_unavailable")

    # ─────────────────────────────────────────────────────────────────────────
    # PAPER AUTO GATE AWARENESS
    # ─────────────────────────────────────────────────────────────────────────
    gate_info: dict = {"available": False}
    try:
        if user_id and item_id:
            gs = _lm_get_paper_auto_gate_state(item_id, user_id)
            if gs.get("ok"):
                gate_evaluated = gs.get("gate_evaluated", False)
                gate_eligible  = gs.get("eligible", False)
                gate_armed     = gs.get("armed", False)
                gate_status    = gs.get("gate_status", "not_evaluated")

                if not gate_evaluated:
                    warnings.append("paper_auto_gate_not_evaluated")
                elif gate_status in ("blocked",):
                    warnings.append("paper_auto_gate_blocked")
                if gate_armed:
                    warnings.append("paper_auto_gate_armed_metadata_only")

                gate_info = {
                    "available":     True,
                    "gate_evaluated": gate_evaluated,
                    "eligible":       gate_eligible,
                    "armed":          gate_armed,
                    "gate_status":    gate_status,
                }
    except Exception:
        pass

    # ─────────────────────────────────────────────────────────────────────────
    # MARKET PRICE (context only)
    # ─────────────────────────────────────────────────────────────────────────
    market_price_dec = None
    price_source     = None
    try:
        if item:
            pr = _lm_get_real_market_price_for_paper(item)
            if pr and pr.get("ok"):
                market_price_dec = _safe_dec(pr.get("price"))
                price_source     = pr.get("price_source")
    except Exception:
        pass

    if market_price_dec and entry_dec:
        if direction == "LONG"  and entry_dec > market_price_dec * Decimal("1.005"):
            warnings.append("entry_above_market_long")
        if direction == "SHORT" and entry_dec < market_price_dec * Decimal("0.995"):
            warnings.append("entry_below_market_short")

    if market_price_dec is None:
        warnings.append("market_price_unavailable")

    # ─────────────────────────────────────────────────────────────────────────
    # FINAL VERDICT — deduplicate preserving order
    # ─────────────────────────────────────────────────────────────────────────
    seen_br: set = set()
    unique_br = []
    for r in blocking_reasons:
        if r not in seen_br:
            seen_br.add(r)
            unique_br.append(r)

    seen_w: set = set()
    unique_w = []
    for w in warnings:
        if w not in seen_w:
            seen_w.add(w)
            unique_w.append(w)

    allowed = len(unique_br) == 0
    if not allowed:
        risk_status = "blocked"
    elif unique_w:
        risk_status = "warning"
    else:
        risk_status = "allowed"

    # ─────────────────────────────────────────────────────────────────────────
    # BUILD RESPONSE
    # ─────────────────────────────────────────────────────────────────────────
    rr_str  = _d_str(risk_reward_dec)
    rpc_str = _d_str(risk_pct_dec)

    risk_dict = {
        "symbol":           symbol,
        "direction_raw":    direction_raw,
        "direction":        direction or "",
        "side":             paper_side or "",
        "quantity":         _d_str(qty_dec),
        "entry_price":      _d_str(entry_dec),
        "stop_loss":        _d_str(sl_dec),
        "sl_price":         _d_str(sl_dec),        # alias
        "take_profit":      _d_str(tp_dec),
        "tp_price":         _d_str(tp_dec),        # alias
        "notional":         _d_str(notional_dec),
        "risk_per_unit":    _d_str(risk_per_unit_dec),
        "risk_amount":      _d_str(risk_amount_dec),
        "risk_pct_of_equity": rpc_str,
        "risk_pct":         rpc_str,               # alias
        "reward_per_unit":  _d_str(reward_per_unit_dec),
        "reward_amount":    _d_str(reward_amount_dec),
        "risk_reward":      rr_str,
        "rr":               rr_str,                # alias
        "market_price":     _d_str(market_price_dec),
        "price_source":     price_source,
    }

    account_dict = {
        "cash_balance":     _d_str(cash_balance_dec),
        "equity":           _d_str(equity_dec),
        "min_account_equity": min_equity_str,
    }

    open_state_dict = {
        "symbol":                   symbol,
        "open_orders":              total_open_orders,
        "open_positions":           total_open_positions,
        "same_symbol_open_orders":  same_sym_orders,
        "same_symbol_open_positions": same_sym_positions,
    }

    return {
        "ok":               True,
        "phase":            "phase11_13_paper_risk_guard",
        "allowed":          allowed,
        "risk_status":      risk_status,
        "blocking_reasons": unique_br,
        "warnings":         unique_w,
        "risk":             risk_dict,
        "limits":           limits,
        "account":          account_dict,
        "open_state":       open_state_dict,
        "daily_loss":       daily_loss_info,
        "journal_feedback": journal_feedback,
        "paper_auto_gate":  gate_info,
        "guardrails":       guardrails,
        "allowed_actions": {
            "can_paper_manually_submit":    allowed,
            "can_auto_submit":              False,  # ALWAYS
            "can_live_trade":               False,  # ALWAYS
            "can_testnet_strategy_validate": False, # ALWAYS
        },
        "source":        "paper_risk_guard",
        "engine_source": "internal_paper_risk_guard",
        "computed_at":   now_iso,
    }


def _rg_error_response(now_iso: str, blocking_reasons: list, limits: dict) -> dict:
    """Return a safe error-shape guard result."""
    return {
        "ok":               False,
        "phase":            "phase11_13_paper_risk_guard",
        "allowed":          False,
        "risk_status":      "error",
        "blocking_reasons": blocking_reasons,
        "warnings":         [],
        "risk":             {},
        "limits":           limits,
        "account":          {},
        "open_state":       {},
        "daily_loss":       {},
        "journal_feedback": {"available": False, "consecutive_losses": 0},
        "paper_auto_gate":  {"available": False},
        "guardrails":       dict(_RG_GUARDRAILS_BASE),
        "allowed_actions": {
            "can_paper_manually_submit":    False,
            "can_auto_submit":              False,
            "can_live_trade":               False,
            "can_testnet_strategy_validate": False,
        },
        "source":        "paper_risk_guard",
        "engine_source": "internal_paper_risk_guard",
        "computed_at":   now_iso,
    }


# ── State reader ──────────────────────────────────────────────────────────────

def _lm_get_paper_risk_guard_state(item_id, user_id) -> dict:
    """Return stored paper risk guard result from item snapshot. Read-only."""
    try:
        from models import LiveMonitorItem as _LMI_rg
        item = _LMI_rg.query.filter_by(id=item_id, user_id=user_id).first()
        if item is None:
            return {"ok": False, "error": "item_not_found"}
        snap = {}
        try:
            raw = getattr(item, "snapshot_json", None)
            if raw:
                snap = _json_rg.loads(raw)
        except Exception:
            pass
        guard = snap.get("latest_paper_risk_guard_result") or {}
        evaluated = bool(guard.get("ok"))
        allowed   = guard.get("allowed", False) if evaluated else False
        return {
            "ok":               True,
            "guard_evaluated":  evaluated,
            "allowed":          allowed,
            "risk_status":      guard.get("risk_status", "not_evaluated"),
            "blocking_reasons": guard.get("blocking_reasons", []),
            "warnings":         guard.get("warnings", []),
            "risk":             guard.get("risk", {}),
            "limits":           guard.get("limits", {}),
            "account":          guard.get("account", {}),
            "open_state":       guard.get("open_state", {}),
            "daily_loss":       guard.get("daily_loss", {}),
            "journal_feedback": guard.get("journal_feedback", {"available": False}),
            "paper_auto_gate":  guard.get("paper_auto_gate", {"available": False}),
            "guardrails":       guard.get("guardrails", dict(_RG_GUARDRAILS_BASE)),
            "allowed_actions":  guard.get("allowed_actions", {
                "can_paper_manually_submit":    False,
                "can_auto_submit":              False,
                "can_live_trade":               False,
                "can_testnet_strategy_validate": False,
            }),
            "latest_guard_result": guard,
            "source":           guard.get("source", "paper_risk_guard"),
            "engine_source":    guard.get("engine_source", "internal_paper_risk_guard"),
        }
    except Exception as _e:
        return {"ok": False, "error": str(_e)[:120]}


# ── Submit validator ──────────────────────────────────────────────────────────

def _lm_validate_paper_order_against_risk_guard(item, user_id, quantity_str) -> dict:
    """Run risk guard and return {ok, allowed, risk_guard}. Always fails closed on error."""
    try:
        snap = {}
        try:
            raw = getattr(item, "snapshot_json", None)
            if raw:
                snap = _json_rg.loads(raw)
        except Exception:
            pass
        guard = _lm_build_paper_risk_guard(item, snapshot=snap, quantity_str=quantity_str)
        return {
            "ok":         True,
            "allowed":    guard.get("allowed", False),
            "risk_guard": guard,
        }
    except Exception as _e:
        now_iso = datetime.now(timezone.utc).isoformat()
        return {
            "ok":         False,
            "allowed":    False,  # fail closed
            "risk_guard": _rg_error_response(
                now_iso,
                [f"risk_guard_error:{str(_e)[:80]}"],
                dict(_RG_DEFAULT_SETTINGS),
            ),
        }


# ── Optional event logger ─────────────────────────────────────────────────────

def _lm_record_paper_risk_guard_event(user_id, item_id, result: dict) -> None:
    """Log a paper risk guard evaluation as a LiveMonitorEvent. Failures are silent."""
    try:
        from models import db as _db_rge, LiveMonitorEvent as _LME_rge
        _lme = _LME_rge(
            item_id           = item_id,
            user_id           = user_id,
            symbol            = (result.get("risk") or {}).get("symbol"),
            event_type        = "paper_risk_guard_check",
            event_description = (
                f"Risk guard: {result.get('risk_status','?')} "
                f"allowed={result.get('allowed',False)} "
                f"blocks={result.get('blocking_reasons',[])} "
            )[:255],
            details_json      = _json_rg.dumps({
                "risk_status":      result.get("risk_status"),
                "allowed":          result.get("allowed"),
                "blocking_reasons": result.get("blocking_reasons"),
                "warnings":         result.get("warnings"),
            }, default=str),
        )
        _db_rge.session.add(_lme)
        _db_rge.session.commit()
    except Exception:
        pass
