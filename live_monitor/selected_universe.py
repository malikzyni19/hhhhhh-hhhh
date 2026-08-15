"""Phase SIG-9: Rebuilding the Selected Pairs universe server-side.

The Selected Pairs tab does not hold a fixed list. It reloads from the
current biggest movers, so the universe changes through the day — which is
the point of it.

That rebuild lived only in the browser, so the server's copy froze at whatever
the tab last pushed. With the browser closed the signal funnel kept scanning
a list that could be hours stale, and the user would never see why.

This mirrors the tab's selection rules exactly, on the server, so the universe
stays current on its own. The rules are duplicated deliberately and kept
side by side with the originals in the comments below — the alternative was
driving a browser from the server, which is far worse.

Read-only market data. No AI, no delivery, no order placement.
"""
from __future__ import annotations

import os
import re

import main as _m


# Mirrors _spIsJunk() in templates/preview.html — leveraged tokens and
# stablecoin pairs, neither of which produce meaningful setups.
_JUNK_SUFFIX = re.compile(r"(UP|DOWN|BULL|BEAR)USDT$")
_JUNK_LEVERAGED = re.compile(r"\d+(L|S)USDT$")
_JUNK_STABLE = re.compile(r"^(USDC|BUSD|TUSD|DAI|FDUSD|USDD|USDP|GUSD|EUR|AEUR)")

# Mirrors _spIsStock() — tokenized equities listed on some venues.
_STOCK = re.compile(r"STOCK")


def is_junk(symbol: str) -> bool:
    s = str(symbol or "")
    return bool(_JUNK_SUFFIX.search(s) or _JUNK_LEVERAGED.search(s)
                or _JUNK_STABLE.match(s))


def is_stock(symbol: str) -> bool:
    return bool(_STOCK.search(str(symbol or "")))


def auto_universe_enabled() -> bool:
    """Whether the server keeps the universe fresh by itself.

    Turning this off freezes the universe at whatever the browser last
    synced — useful only if someone wants a deliberately fixed list.
    """
    return (os.environ.get("ZYNI_SIG_AUTO_UNIVERSE", "1") or "").strip().lower() \
        in ("1", "true", "yes", "on")


def _cfg_int(name: str, default: int, lo: int, hi: int) -> int:
    try:
        return max(lo, min(hi, int(os.environ.get(name, str(default)))))
    except (TypeError, ValueError):
        return default


def universe_config() -> dict:
    """Defaults match the Selected Pairs tab's own defaults."""
    return {
        "direction":      (os.environ.get("ZYNI_SIG_UNIVERSE_DIR", "both") or "both").lower(),
        "gainers":        _cfg_int("ZYNI_SIG_UNIVERSE_GAINERS", 100, 0, 400),
        "losers":         _cfg_int("ZYNI_SIG_UNIVERSE_LOSERS", 100, 0, 400),
        "min_volume":     _cfg_int("ZYNI_SIG_UNIVERSE_MIN_VOL", 0, 0, 1_000_000_000),
        "exclude_junk":   True,
        "exclude_stocks": True,
    }


def build_universe(exchange: str = "binance", market: str = "perpetual",
                   cfg: dict = None) -> list:
    """The current top movers, chosen the same way the tab chooses them.

    Sorted by 24h change; the strongest gainers and the deepest losers are
    both taken, because a setup can be worth alerting in either direction.
    """
    cfg = cfg or universe_config()

    try:
        pairs = _m.get_pairs_exchange(exchange, market) or []
    except Exception as exc:
        print(f"[SIG-9] pair list unavailable: {str(exc)[:120]}")
        return []

    pool = []
    for p in pairs:
        try:
            sym = str(p.get("symbol") or "").upper()
            chg = p.get("changePct")
            if not sym or not isinstance(chg, (int, float)):
                continue
            if cfg["min_volume"] and (p.get("quoteVolume") or 0) < cfg["min_volume"]:
                continue
            if cfg["exclude_junk"] and is_junk(sym):
                continue
            if cfg["exclude_stocks"] and is_stock(sym):
                continue
            pool.append((sym, float(chg)))
        except Exception:
            continue

    if not pool:
        return []

    pool.sort(key=lambda x: x[1], reverse=True)
    direction = cfg["direction"]
    picked: list = []

    def _add(items):
        for sym, _chg in items:
            if sym not in picked:
                picked.append(sym)

    if direction in ("both", "gainers") and cfg["gainers"]:
        _add(pool[:cfg["gainers"]])
    if direction in ("both", "losers") and cfg["losers"]:
        _add(pool[max(0, len(pool) - cfg["losers"]):])

    return picked


def refresh_universe_for_user(username: str, exchange: str = "binance",
                              market: str = "perpetual") -> dict:
    """Rebuild and store one user's universe. Returns a small summary.

    Never raises, and never writes an empty list: if the exchange is briefly
    unreachable, keeping the previous universe is far better than wiping it
    and scanning nothing until the next cycle.
    """
    if not auto_universe_enabled():
        return {"ok": True, "skipped": "auto_universe_disabled"}

    try:
        symbols = build_universe(exchange, market)
        if not symbols:
            return {"ok": False, "reason": "no_pairs_returned",
                    "kept_previous": True}

        before = _m.load_user_selected_pairs(username) or []
        _m.save_user_selected_pairs(username, symbols)
        after = _m.load_user_selected_pairs(username) or []

        return {
            "ok": True,
            "count": len(after),
            "added": len([s for s in after if s not in before]),
            "removed": len([s for s in before if s not in after]),
        }
    except Exception as exc:
        print(f"[SIG-9] universe refresh failed for {username}: {str(exc)[:120]}")
        return {"ok": False, "reason": str(exc)[:120], "kept_previous": True}


def refresh_universes_for_enabled_users(exchange: str = "binance",
                                        market: str = "perpetual") -> dict:
    """Refresh for every user whose coin scope actually uses the universe."""
    from models import User, LiveMonitorSignalSettings as _S

    out = {"users": 0, "results": {}}
    if not auto_universe_enabled():
        return {**out, "skipped": "auto_universe_disabled"}

    try:
        with _m.app.app_context():
            rows = (_S.query
                    .filter(_S.enabled.is_(True),
                            _S.coin_scope == "selected_pairs").all())
            if not rows:
                return out

            # One rebuild serves everyone — the mover list is global, so
            # fetching it per user would just repeat the same work.
            symbols = build_universe(exchange, market)
            if not symbols:
                return {**out, "skipped": "no_pairs_returned"}

            for r in rows:
                user = User.query.filter_by(id=r.user_id).first()
                if not user:
                    continue
                try:
                    _m.save_user_selected_pairs(user.username, symbols)
                    out["results"][user.username] = len(symbols)
                    out["users"] += 1
                except Exception as exc:
                    print(f"[SIG-9] save failed for {user.username}: {str(exc)[:100]}")
    except Exception as exc:
        print(f"[SIG-9] universe refresh sweep failed: {str(exc)[:120]}")
    return out
