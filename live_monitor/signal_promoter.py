"""Phase SIG-3: Promotion — put the best candidates under close watch.

Finding a setup is cheap. Watching one properly is not: it means a websocket
subscription, CVD flow candles, open-interest sampling and order-flow
snapshots, all of which cost database traffic continuously for as long as the
watch lasts.

So the funnel narrows here. Only the top-ranked confluence groups are promoted
into Live Monitor, under a hard ceiling, and they are dropped again once their
setups go stale. This is the stage that decides the system's running cost.

No AI, no delivery, no order placement.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone, timedelta

import main as _m   # deferred: main defines these before importing live_monitor


# Items this module creates carry this source tab, which is what makes it safe
# to demote them later — anything a user added by hand is never touched.
AUTO_SOURCE_TAB = "signal_auto"


def max_watched_coins() -> int:
    """Ceiling on simultaneously watched coins. The main cost control."""
    try:
        return max(1, min(200, int(os.environ.get("ZYNI_SIG_MAX_WATCHED", "20"))))
    except (TypeError, ValueError):
        return 20


def min_promotion_strength() -> int:
    """Quality floor a setup must clear before it is worth watching.

    Watching is not free: every promoted pair starts per-minute CVD candles,
    open-interest sampling, spot flow and order-flow snapshots, and keeps
    doing so for as long as it stays watched. Spending that on a 35%-strength
    setup is spending it on something nobody would trade.
    """
    try:
        return max(0, min(100, int(os.environ.get("ZYNI_SIG_MIN_STRENGTH", "55"))))
    except (TypeError, ValueError):
        return 55


def min_watch_minutes() -> int:
    """Shortest time a promoted coin stays watched.

    Without this, a setup hovering at the edge of the ranking would be added
    and dropped repeatedly, and each add re-triggers history backfill — the
    exact opposite of saving traffic.
    """
    try:
        return max(5, min(1440, int(os.environ.get("ZYNI_SIG_MIN_WATCH_MIN", "60"))))
    except (TypeError, ValueError):
        return 60


def _primary_timeframe(group: dict) -> str:
    """Pick the group's most significant timeframe — the highest one present."""
    order = ["1d", "4h", "2h", "1h", "30m", "15m", "5m", "3m", "1m"]
    tfs = set(group.get("timeframes") or [])
    for tf in order:
        if tf in tfs:
            return tf
    return "4h"


def _setup_label(group: dict) -> str:
    """A readable setup name built from the modules that agreed."""
    mods = group.get("matched_modules") or group.get("modules") or []
    return ("+".join(mods))[:40] or "signal"


def _watch_snapshot(group: dict, now: datetime) -> dict:
    """Snapshot blob recording why this coin is being watched."""
    return {
        "addedFrom": AUTO_SOURCE_TAB,
        "manual_add": False,
        "signal_watch": {
            "promoted_at":   now.isoformat(),
            "modules":       group.get("matched_modules") or group.get("modules") or [],
            "module_count":  group.get("module_count"),
            "strength":      group.get("strength"),
            "direction":     group.get("direction"),
            "zone_high":     group.get("zone_high"),
            "zone_low":      group.get("zone_low"),
            "timeframes":    group.get("timeframes"),
            "avg_module_score": group.get("avg_module_score"),
            "zone_agreement":   group.get("zone_agreement"),
        },
    }


def promote_groups(user_id: int, groups: list, now=None) -> dict:
    """Create or refresh watched items for the strongest groups.

    `groups` should already be filtered to this user's settings. Returns
    {promoted, refreshed, skipped_cap, errors}. Never raises.
    """
    from models import db as _db, LiveMonitorItem as _LMI
    from sqlalchemy.orm import load_only as _load_only

    now = now or datetime.now(timezone.utc)
    cap = max_watched_coins()
    stats = {"promoted": 0, "refreshed": 0, "skipped_cap": 0, "errors": 0}
    if not groups:
        return stats

    try:
        # Everything this user already has active, so we can tell what counts
        # against the ceiling and what is already being watched.
        active = (_LMI.query
                  .options(_load_only(_LMI.id, _LMI.symbol, _LMI.direction,
                                      _LMI.source_tab, _LMI.snapshot_json))
                  .filter_by(user_id=user_id, is_active=True).all())
    except Exception as exc:
        print(f"[SIG-3] promote preload failed: {str(exc)[:120]}")
        return {**stats, "errors": 1}

    auto_rows   = {}
    manual_count = 0
    for r in active:
        if (r.source_tab or "") == AUTO_SOURCE_TAB:
            auto_rows[(r.symbol, r.direction or "neutral")] = r
        else:
            manual_count += 1

    # Manually added coins are the user's own choice and always keep their
    # slot; automation only gets whatever room is left under the ceiling.
    room = max(0, cap - manual_count)

    for g in groups:
        symbol    = g.get("symbol")
        direction = g.get("direction") or "neutral"
        if not symbol:
            continue
        key = (symbol, direction)

        try:
            existing = auto_rows.get(key)
            if existing is not None:
                snap = _m._json_loads_safe(existing.snapshot_json, {}) or {}
                snap.update(_watch_snapshot(g, now))
                existing.snapshot_json = _m._json_dumps_safe(snap)
                existing.score      = int(g.get("strength") or 0)
                existing.confidence = int(g.get("strength") or 0)
                stats["refreshed"] += 1
                continue

            if len(auto_rows) >= room:
                stats["skipped_cap"] += 1
                continue

            row = _LMI(
                user_id       = user_id,
                symbol        = symbol,
                exchange      = (g.get("exchange") or "binance"),
                market        = (g.get("market") or "perpetual"),
                source_tab    = AUTO_SOURCE_TAB,
                setup_type    = _setup_label(g),
                direction     = direction,
                timeframe     = _primary_timeframe(g),
                zone_high     = g.get("zone_high"),
                zone_low      = g.get("zone_low"),
                confidence    = int(g.get("strength") or 0),
                score         = int(g.get("strength") or 0),
                current_price = g.get("detected_price"),
                status        = "watching",
                is_active     = True,
                snapshot_json = _m._json_dumps_safe(_watch_snapshot(g, now)),
                # Stamped from the caller's clock rather than the column
                # default, so the minimum-watch-age rule in demote_stale_watches
                # measures against the same timeline this cycle is using.
                added_at      = now,
            )
            _db.session.add(row)
            auto_rows[key] = row
            stats["promoted"] += 1
        except Exception as exc:
            stats["errors"] += 1
            print(f"[SIG-3] promote error {symbol}: {str(exc)[:120]}")

    try:
        _db.session.commit()
    except Exception as exc:
        _db.session.rollback()
        print(f"[SIG-3] promote commit failed: {str(exc)[:150]}")
        return {**stats, "promoted": 0, "refreshed": 0,
                "errors": stats["errors"] + 1}

    # History backfill runs only for genuinely new watches, after the commit,
    # so a rollback can never leave orphaned background work running.
    if stats["promoted"]:
        for (symbol, _d), row in auto_rows.items():
            try:
                if (row.exchange or "binance") == "binance" and getattr(row, "id", None):
                    _m._lm_flow_backfill_bg(symbol)
                    _m._lm_spot_flow_backfill_bg(symbol)
            except Exception as exc:
                print(f"[SIG-3] backfill spawn failed {symbol}: {str(exc)[:100]}")
        try:
            _m._invalidate_active_syms_cache()
        except Exception:
            pass

    return stats


def demote_stale_watches(user_id: int, live_groups: list, now=None) -> dict:
    """Stop watching auto-added coins whose setups are gone.

    Only touches items this module created. Items younger than the minimum
    watch time are left alone so a coin cannot be added and dropped repeatedly.
    """
    from models import db as _db, LiveMonitorItem as _LMI

    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=min_watch_minutes())
    stats = {"demoted": 0, "kept_min_age": 0, "errors": 0}

    live_keys = {(g.get("symbol"), g.get("direction") or "neutral")
                 for g in (live_groups or [])}

    try:
        rows = _LMI.query.filter_by(user_id=user_id, is_active=True,
                                    source_tab=AUTO_SOURCE_TAB).all()
    except Exception as exc:
        print(f"[SIG-3] demote query failed: {str(exc)[:120]}")
        return {**stats, "errors": 1}

    for r in rows:
        try:
            if (r.symbol, r.direction or "neutral") in live_keys:
                continue

            added = r.added_at
            if added is not None:
                if added.tzinfo is None:
                    added = added.replace(tzinfo=timezone.utc)
                if added > cutoff:
                    stats["kept_min_age"] += 1
                    continue

            r.is_active = False
            r.status    = "expired"
            stats["demoted"] += 1
        except Exception as exc:
            stats["errors"] += 1
            print(f"[SIG-3] demote error item={getattr(r,'id',None)}: {str(exc)[:100]}")

    if stats["demoted"]:
        try:
            _db.session.commit()
            _m._invalidate_active_syms_cache()
        except Exception as exc:
            _db.session.rollback()
            print(f"[SIG-3] demote commit failed: {str(exc)[:150]}")
            return {**stats, "demoted": 0, "errors": stats["errors"] + 1}
    else:
        try:
            _db.session.rollback()
        except Exception:
            pass

    return stats


def run_promotion_cycle(user_id: int, now=None) -> dict:
    """One full pass for one user: rank, promote, drop the stale.

    Does nothing unless the user has switched the signal engine on, so an
    account that never opts in costs nothing.
    """
    from live_monitor.signal_settings import get_or_create_signal_settings
    from live_monitor.signal_confluence import (build_confluence_groups,
                                                filter_groups_for_settings)
    from live_monitor.signal_intake import expire_stale_candidates

    now = now or datetime.now(timezone.utc)

    try:
        settings = get_or_create_signal_settings(user_id)
    except Exception as exc:
        return {"ok": False, "reason": f"settings_error:{str(exc)[:80]}"}

    if not settings or not settings.enabled:
        return {"ok": True, "skipped": "engine_off"}

    try:
        expired = expire_stale_candidates(now=now)
        groups  = build_confluence_groups(window_hours=12, now=now)
        matched = filter_groups_for_settings(groups, settings)
    except Exception as exc:
        return {"ok": False, "reason": f"grouping_error:{str(exc)[:80]}"}

    # Strongest first, then trimmed to the ceiling — everything below the line
    # is simply not watched this cycle.
    # Quality gate before the ceiling, so the limited watch slots go to setups
    # worth the collection cost rather than to whatever ranked highest among a
    # weak field.
    floor = min_promotion_strength()
    strong = [g for g in matched
              if int(g.get("strength") or 0) >= floor
              and g.get("direction") in ("long", "short")]
    rejected = len(matched) - len(strong)

    strong.sort(key=lambda g: (g.get("module_count", 0), g.get("strength", 0)),
                reverse=True)
    top = strong[:max_watched_coins()]

    promoted = promote_groups(user_id, top, now=now)
    demoted  = demote_stale_watches(user_id, top, now=now)

    return {
        "ok":               True,
        "candidates_expired": expired,
        "groups_found":     len(groups),
        "groups_matched":   len(matched),
        "below_quality_floor": rejected,
        "min_strength":     floor,
        "groups_watched":   len(top),
        "promote":          promoted,
        "demote":           demoted,
        "cap":              max_watched_coins(),
    }


def run_promotion_for_all_enabled(now=None) -> dict:
    """Run one cycle for every user who has the engine switched on."""
    from models import LiveMonitorSignalSettings as _S
    from sqlalchemy.orm import load_only as _load_only

    now = now or datetime.now(timezone.utc)
    out = {"users": 0, "results": {}}
    try:
        rows = (_S.query
                .options(_load_only(_S.user_id, _S.enabled))
                .filter(_S.enabled.is_(True)).all())
    except Exception as exc:
        print(f"[SIG-3] enabled-user query failed: {str(exc)[:120]}")
        return out

    for r in rows:
        try:
            out["results"][r.user_id] = run_promotion_cycle(r.user_id, now=now)
            out["users"] += 1
        except Exception as exc:
            print(f"[SIG-3] cycle failed user={r.user_id}: {str(exc)[:120]}")
    return out
