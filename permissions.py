"""
Permission resolver for ZyNi SMC.
Priority: UserPermission > RolePermission > hardcoded defaults.
Admin role always bypasses all checks.
Results cached per user_id for 60 seconds.
"""
import json
import time
from datetime import date, datetime, timezone
from models import (
    db, RolePermission, UserPermission,
    DailyTokenUsage, GlobalSetting,
    ALL_MODULES, ALL_TABS, ALL_EXCHANGES, ALL_TIMEFRAMES,
)

_CACHE: dict = {}          # {user_id: (ts, perms_dict)}
_CACHE_TTL   = 60          # seconds

_ROLE_DEFAULTS = {
    "admin": {
        "daily_tokens":        999999,
        "max_pairs_per_scan":  500,
        "max_pairs_per_cycle": 500,
        "allowed_modules":     ALL_MODULES[:],
        "allowed_tabs":        ALL_TABS[:],
        "allowed_exchanges":   ALL_EXCHANGES[:],
        "allowed_timeframes":  ALL_TIMEFRAMES[:],
    },
    "user": {
        "daily_tokens":        500,
        "max_pairs_per_scan":  100,
        "max_pairs_per_cycle": 50,
        "allowed_modules":     ["ob", "fvg", "fib", "bias"],
        "allowed_tabs":        ["scan", "pairs", "settings", "bias"],
        "allowed_exchanges":   ["binance"],
        "allowed_timeframes":  ["15m", "1h", "4h"],
    },
    "guest": {
        "daily_tokens":        50,
        "max_pairs_per_scan":  20,
        "max_pairs_per_cycle": 10,
        "allowed_modules":     ["ob"],
        "allowed_tabs":        ["scan"],
        "allowed_exchanges":   ["binance"],
        "allowed_timeframes":  ["1h"],
    },
}


def _parse_json(val, fallback):
    if val is None:
        return None
    if isinstance(val, list):
        return val
    try:
        return json.loads(val)
    except Exception:
        return fallback


def _bust_cache(user_id: int):
    _CACHE.pop(user_id, None)


def get_setting(key: str, default=None):
    try:
        s = GlobalSetting.query.filter_by(key=key).first()
        return s.value if s else default
    except Exception:
        return default


def get_user_permissions(user) -> dict:
    """Return effective permissions dict for a user, using cache."""
    uid = user.id
    ts, cached = _CACHE.get(uid, (0, None))
    if cached and (time.time() - ts) < _CACHE_TTL:
        return cached

    try:
        role_defaults = _ROLE_DEFAULTS.get(user.role, _ROLE_DEFAULTS["user"])
        role_perm  = RolePermission.query.filter_by(role=user.role).first()
        user_perm  = UserPermission.query.filter_by(user_id=uid).first()
        today_row  = DailyTokenUsage.query.filter_by(user_id=uid, date=date.today()).first()
        tokens_used = today_row.tokens_used if today_row else 0

        def resolve(field, fallback):
            # User override first
            if user_perm:
                v = getattr(user_perm, field, None)
                if v is not None:
                    parsed = _parse_json(v, fallback)
                    return parsed if parsed is not None else fallback
            # Role permission table
            if role_perm:
                v = getattr(role_perm, field, None)
                if v is not None:
                    parsed = _parse_json(v, fallback)
                    return parsed if parsed is not None else fallback
            # Hardcoded role default
            return role_defaults.get(field, fallback)

        daily_tokens = resolve("daily_tokens", 500)
        if isinstance(daily_tokens, str):
            daily_tokens = int(daily_tokens)

        max_scan  = resolve("max_pairs_per_scan",  100)
        max_cycle = resolve("max_pairs_per_cycle", 50)
        if isinstance(max_scan, str):  max_scan  = int(max_scan)
        if isinstance(max_cycle, str): max_cycle = int(max_cycle)

        perms = {
            "daily_tokens":        daily_tokens,
            "tokens_used_today":   tokens_used,
            "tokens_remaining":    max(0, daily_tokens - tokens_used),
            "max_pairs_per_scan":  max_scan,
            "max_pairs_per_cycle": max_cycle,
            "allowed_modules":     resolve("allowed_modules",   role_defaults["allowed_modules"]),
            "allowed_tabs":        resolve("allowed_tabs",      role_defaults["allowed_tabs"]),
            "allowed_exchanges":   resolve("allowed_exchanges", role_defaults["allowed_exchanges"]),
            "allowed_timeframes":  resolve("allowed_timeframes",role_defaults["allowed_timeframes"]),
            "is_admin":            user.role == "admin",
        }
        _CACHE[uid] = (time.time(), perms)
        return perms
    except Exception as e:
        print(f"[PERMS] Error for user_id={uid}: {e}")
        role_defaults = _ROLE_DEFAULTS.get(getattr(user, "role", "user"), _ROLE_DEFAULTS["user"])
        return {
            "daily_tokens":        role_defaults["daily_tokens"],
            "tokens_used_today":   0,
            "tokens_remaining":    role_defaults["daily_tokens"],
            "max_pairs_per_scan":  role_defaults["max_pairs_per_scan"],
            "max_pairs_per_cycle": role_defaults["max_pairs_per_cycle"],
            "allowed_modules":     role_defaults["allowed_modules"],
            "allowed_tabs":        role_defaults["allowed_tabs"],
            "allowed_exchanges":   role_defaults["allowed_exchanges"],
            "allowed_timeframes":  role_defaults["allowed_timeframes"],
            "is_admin":            getattr(user, "role", "") == "admin",
        }


def consume_tokens(user_id: int, count: int) -> None:
    """Add scan token usage. Never raises."""
    try:
        today = date.today()
        row = DailyTokenUsage.query.filter_by(user_id=user_id, date=today).first()
        if not row:
            row = DailyTokenUsage(user_id=user_id, date=today, tokens_used=0, scan_count=0)
            db.session.add(row)
        row.tokens_used += count
        row.scan_count  += 1
        row.last_scan_at = datetime.now(timezone.utc)
        db.session.commit()
        _bust_cache(user_id)
    except Exception as e:
        print(f"[PERMS] consume_tokens error: {e}")


def check_tokens(user) -> bool:
    if user.role == "admin":
        return True
    perms = get_user_permissions(user)
    return perms["tokens_remaining"] > 0


def check_tab_access(user, tab: str) -> bool:
    if user.role == "admin":
        return True
    perms = get_user_permissions(user)
    return tab in perms["allowed_tabs"]


def check_module_access(user, module: str) -> bool:
    if user.role == "admin":
        return True
    perms = get_user_permissions(user)
    return module in perms["allowed_modules"]


def save_user_permissions(user_id: int, overrides: dict, admin_id: int) -> None:
    """Upsert UserPermission overrides. Pass None values to clear a field."""
    row = UserPermission.query.filter_by(user_id=user_id).first()
    if not row:
        row = UserPermission(user_id=user_id)
        db.session.add(row)
    for field in ("daily_tokens", "max_pairs_per_scan", "max_pairs_per_cycle",
                  "allowed_modules", "allowed_tabs", "allowed_exchanges", "allowed_timeframes"):
        val = overrides.get(field)
        if val is None:
            setattr(row, field, None)
        elif isinstance(val, list):
            setattr(row, field, json.dumps(val))
        else:
            setattr(row, field, val)
    row.updated_by = admin_id
    row.updated_at = datetime.now(timezone.utc)
    db.session.commit()
    _bust_cache(user_id)
