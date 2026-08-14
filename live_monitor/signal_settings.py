"""Phase SIG-1: Signal alert settings — per-user configuration.

Controls which setups reach a user and how they are delivered. Detection is
global (a setup is found once for everyone); this module decides, per user,
which of those setups they actually receive.

Read/validate/persist only. No exchange calls. No order placement. The only
credential handled here is a user's own Telegram bot token, which grants
message-sending and nothing else.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone


# ── Scan module catalog ───────────────────────────────────────────────────────
# `key` is what gets stored; `label` is what the settings UI shows. Order here
# is the order rendered in the UI.
SIGNAL_MODULES: list = [
    {"key": "ob",              "label": "Order Block",     "desc": "Institutional demand/supply zones"},
    {"key": "bb",              "label": "Breaker Block",   "desc": "Failed OB flipped to opposite side"},
    {"key": "fvg",             "label": "Fair Value Gap",  "desc": "Imbalance gaps left by impulsive moves"},
    {"key": "fib_confluence",  "label": "FIB Confluence",  "desc": "Multiple fib levels stacking"},
    {"key": "ath_atl",         "label": "ATH / ATL",       "desc": "All-time high / low reactions"},
    {"key": "accumulation",    "label": "Accumulation",    "desc": "Range building before expansion"},
    {"key": "compressed",      "label": "Compressed",      "desc": "Volatility squeeze before breakout"},
    {"key": "bias_shift",      "label": "Bias Shift",      "desc": "Higher-timeframe directional flip"},
    {"key": "liquidity_sweep", "label": "Liquidity Sweep", "desc": "Stop-hunt then reclaim"},
]

VALID_MODULE_KEYS = frozenset(m["key"] for m in SIGNAL_MODULES)

VALID_TRIGGER_MODES = frozenset({"approach", "in_zone", "both"})
VALID_COIN_SCOPES   = frozenset({"top_volume", "watchlist", "all"})
VALID_TIMEFRAMES    = frozenset({"5m", "15m", "30m", "1h", "4h", "1d"})

# ── Bounds ────────────────────────────────────────────────────────────────────
_BOUNDS = {
    "min_confluence":        (1, 5),
    "max_signals_per_day":   (1, 50),
    "per_pair_cooldown_min": (15, 1440),
    "min_confidence":        (0, 100),
    "approach_pct":          (0.1, 10.0),
    "coin_scope_limit":      (10, 500),
}

# ── Defaults ──────────────────────────────────────────────────────────────────
# Chosen so the system is useful immediately without configuration:
# every module on, single-module signals allowed, a moderate daily volume, and
# delivery OFF so nothing is sent until the user explicitly turns it on.
DEFAULTS: dict = {
    "enabled":               False,
    "enabled_modules":       [m["key"] for m in SIGNAL_MODULES],
    "min_confluence":        1,
    "max_signals_per_day":   6,
    "per_pair_cooldown_min": 240,
    "min_confidence":        65,
    "trigger_mode":          "in_zone",
    "approach_pct":          1.5,
    "coin_scope":            "top_volume",
    "coin_scope_limit":      100,
    "symbols":               [],
    "allow_long":            True,
    "allow_short":           True,
    "timeframes":            [],       # empty = all timeframes
    "delivery_enabled":      False,
}


def _json_list(raw, fallback: list) -> list:
    """Parse a stored JSON list column, falling back safely."""
    if not raw:
        return list(fallback)
    try:
        val = json.loads(raw)
        return val if isinstance(val, list) else list(fallback)
    except (ValueError, TypeError):
        return list(fallback)


def _clamp(name: str, value, default):
    """Coerce to the column's numeric type and clamp into its allowed range."""
    lo, hi = _BOUNDS[name]
    try:
        num = float(value)
    except (TypeError, ValueError):
        return default
    if num != num or num in (float("inf"), float("-inf")):   # NaN / inf
        return default
    num = max(lo, min(hi, num))
    return num if isinstance(lo, float) else int(num)


def get_or_create_signal_settings(user_id: int):
    """Return this user's settings row, creating it with defaults if absent."""
    from models import db, LiveMonitorSignalSettings as _S

    row = _S.query.filter_by(user_id=user_id).first()
    if row is not None:
        return row

    row = _S(
        user_id               = user_id,
        enabled               = DEFAULTS["enabled"],
        enabled_modules_json  = json.dumps(DEFAULTS["enabled_modules"]),
        min_confluence        = DEFAULTS["min_confluence"],
        max_signals_per_day   = DEFAULTS["max_signals_per_day"],
        per_pair_cooldown_min = DEFAULTS["per_pair_cooldown_min"],
        min_confidence        = DEFAULTS["min_confidence"],
        trigger_mode          = DEFAULTS["trigger_mode"],
        approach_pct          = DEFAULTS["approach_pct"],
        coin_scope            = DEFAULTS["coin_scope"],
        coin_scope_limit      = DEFAULTS["coin_scope_limit"],
        symbols_json          = json.dumps(DEFAULTS["symbols"]),
        allow_long            = DEFAULTS["allow_long"],
        allow_short           = DEFAULTS["allow_short"],
        timeframes_json       = json.dumps(DEFAULTS["timeframes"]),
        delivery_enabled      = DEFAULTS["delivery_enabled"],
    )
    try:
        db.session.add(row)
        db.session.commit()
    except Exception:
        # Concurrent create — fall back to whichever row won the race.
        db.session.rollback()
        row = _S.query.filter_by(user_id=user_id).first()
    return row


def serialize_signal_settings(row, include_secrets: bool = False) -> dict:
    """Convert a settings row to a wire-safe dict for the settings UI.

    The Telegram bot token is never sent to the browser in full — only whether
    one is configured, plus a masked tail so the user can tell which bot it is.
    """
    if row is None:
        return dict(DEFAULTS, telegram_configured=False, telegram_token_hint=None)

    token = (row.telegram_bot_token or "").strip()
    out = {
        "enabled":               bool(row.enabled),
        "enabled_modules":       _json_list(row.enabled_modules_json,
                                            DEFAULTS["enabled_modules"]),
        "min_confluence":        row.min_confluence,
        "max_signals_per_day":   row.max_signals_per_day,
        "per_pair_cooldown_min": row.per_pair_cooldown_min,
        "min_confidence":        row.min_confidence,
        "trigger_mode":          row.trigger_mode,
        "approach_pct":          row.approach_pct,
        "coin_scope":            row.coin_scope,
        "coin_scope_limit":      row.coin_scope_limit,
        "symbols":               _json_list(row.symbols_json, []),
        "allow_long":            bool(row.allow_long),
        "allow_short":           bool(row.allow_short),
        "timeframes":            _json_list(row.timeframes_json, []),
        "delivery_enabled":      bool(row.delivery_enabled),

        "telegram_configured":   bool(token and row.telegram_chat_id),
        "telegram_token_hint":   (f"…{token[-4:]}" if len(token) >= 4 else None),
        "telegram_chat_id":      row.telegram_chat_id or None,
        "telegram_verified_at":  (row.telegram_verified_at.isoformat()
                                  if row.telegram_verified_at else None),
        "telegram_last_error":   row.telegram_last_error or None,

        "updated_at":            (row.updated_at.isoformat()
                                  if row.updated_at else None),
    }
    if include_secrets:
        out["telegram_bot_token"] = token or None
    return out


def apply_signal_settings_update(row, payload: dict) -> tuple:
    """Validate `payload` and apply it to `row`. Returns (ok, errors, changed).

    Every field is optional — only keys present in the payload are touched, so
    the settings UI can patch one control without resending everything.
    Unknown keys are ignored rather than erroring, so an older client can't be
    broken by a newer server.
    """
    errors: dict = {}
    changed = False

    def _set(attr, value):
        nonlocal changed
        if getattr(row, attr) != value:
            setattr(row, attr, value)
            changed = True

    # ── Booleans ─────────────────────────────────────────────────────────────
    for key in ("enabled", "allow_long", "allow_short", "delivery_enabled"):
        if key in payload:
            _set(key, bool(payload[key]))

    # ── Modules ──────────────────────────────────────────────────────────────
    if "enabled_modules" in payload:
        raw = payload["enabled_modules"]
        if not isinstance(raw, list):
            errors["enabled_modules"] = "must_be_a_list"
        else:
            keys = [str(k).strip().lower() for k in raw]
            bad  = [k for k in keys if k not in VALID_MODULE_KEYS]
            if bad:
                errors["enabled_modules"] = f"unknown_modules:{','.join(bad[:5])}"
            elif not keys:
                errors["enabled_modules"] = "at_least_one_module_required"
            else:
                # De-duplicate while preserving catalog order for stable storage.
                ordered = [m["key"] for m in SIGNAL_MODULES if m["key"] in set(keys)]
                _set("enabled_modules_json", json.dumps(ordered))

    # ── Numeric, clamped ─────────────────────────────────────────────────────
    for key in ("min_confluence", "max_signals_per_day", "per_pair_cooldown_min",
                "min_confidence", "approach_pct", "coin_scope_limit"):
        if key in payload:
            _set(key, _clamp(key, payload[key], getattr(row, key)))

    # ── Enumerations ─────────────────────────────────────────────────────────
    if "trigger_mode" in payload:
        val = str(payload["trigger_mode"]).strip().lower()
        if val not in VALID_TRIGGER_MODES:
            errors["trigger_mode"] = "invalid_trigger_mode"
        else:
            _set("trigger_mode", val)

    if "coin_scope" in payload:
        val = str(payload["coin_scope"]).strip().lower()
        if val not in VALID_COIN_SCOPES:
            errors["coin_scope"] = "invalid_coin_scope"
        else:
            _set("coin_scope", val)

    # ── Symbol watchlist ─────────────────────────────────────────────────────
    if "symbols" in payload:
        raw = payload["symbols"]
        if not isinstance(raw, list):
            errors["symbols"] = "must_be_a_list"
        else:
            syms, bad = [], []
            for s in raw[:500]:
                s = str(s).strip().upper()
                if not s:
                    continue
                if not s.isalnum() or len(s) > 30:
                    bad.append(s)
                elif s not in syms:
                    syms.append(s)
            if bad:
                errors["symbols"] = f"invalid_symbols:{','.join(bad[:5])}"
            else:
                _set("symbols_json", json.dumps(syms))

    # ── Timeframes ───────────────────────────────────────────────────────────
    if "timeframes" in payload:
        raw = payload["timeframes"]
        if not isinstance(raw, list):
            errors["timeframes"] = "must_be_a_list"
        else:
            tfs = [str(t).strip().lower() for t in raw]
            bad = [t for t in tfs if t not in VALID_TIMEFRAMES]
            if bad:
                errors["timeframes"] = f"invalid_timeframes:{','.join(bad[:5])}"
            else:
                _set("timeframes_json", json.dumps(sorted(set(tfs))))

    # ── Cross-field checks ───────────────────────────────────────────────────
    # These run against the post-update state so they catch combinations the
    # user is creating right now, not the ones they had before.
    if not row.allow_long and not row.allow_short:
        errors["allow_long"] = "at_least_one_direction_required"

    if row.coin_scope == "watchlist" and not _json_list(row.symbols_json, []):
        errors["symbols"] = "watchlist_scope_requires_at_least_one_symbol"

    # Turning delivery on without a linked Telegram bot would silently do
    # nothing — surface it as an error rather than failing quietly at send time.
    if row.delivery_enabled and not (row.telegram_bot_token and row.telegram_chat_id):
        errors["delivery_enabled"] = "telegram_not_linked"

    if errors:
        return False, errors, False

    if changed:
        row.updated_at = datetime.now(timezone.utc)
    return True, {}, changed


def signal_module_catalog() -> list:
    """The module list the settings UI renders. Safe to expose."""
    return [dict(m) for m in SIGNAL_MODULES]
