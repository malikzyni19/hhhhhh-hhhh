"""Phase SIG-2: Confluence — which pairs several scans agree on.

The single highest-value question the funnel can ask, and one this app could
not ask before: when order block, accumulation and bias shift all flag the
same pair in the same direction around the same price, that is a materially
different proposition from any one of them alone.

Pure computation over stored candidates. No AI, no delivery, no exchange
calls, no order placement.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta


# How much each module contributes to a group's strength. Modules that
# describe a *directional* event are weighted above ones that describe a
# *condition*, because a condition is only half a setup.
#
# These are informed priors, not measured truth. Once outcomes are being
# resolved, real per-module hit rates should replace them.
MODULE_WEIGHT = {
    "ob":              1.00,
    "bb":              0.95,
    "bias_shift":      0.90,
    "fvg":             0.75,
    "fib_confluence":  0.70,
    "liquidity_sweep": 0.85,
    "ath_atl":         0.65,
    "compressed":      0.60,
    "accumulation":    0.55,
}
_DEFAULT_WEIGHT = 0.5

# A neutral candidate (a squeeze, an accumulation range) cannot set a group's
# direction, but it can support one another module established.
NEUTRAL_SUPPORT = 0.5

# Zones from different modules rarely match exactly. Treat them as the same
# area when they overlap, or sit within this much of each other.
ZONE_PROXIMITY_PCT = 1.5


def _overlap_or_near(a_hi, a_lo, b_hi, b_lo, tol_pct=ZONE_PROXIMITY_PCT) -> bool:
    """True when two zones overlap, or are within `tol_pct` of one another."""
    if None in (a_hi, a_lo, b_hi, b_lo):
        return False
    if a_lo <= b_hi and b_lo <= a_hi:          # direct overlap
        return True
    gap = b_lo - a_hi if b_lo > a_hi else a_lo - b_hi
    mid = (a_hi + a_lo + b_hi + b_lo) / 4.0
    if mid <= 0:
        return False
    return (gap / mid) * 100.0 <= tol_pct


def _zone_bounds(cands: list) -> tuple:
    """Tightest zone the directional members agree on, if any have zones."""
    zones = [(c.zone_high, c.zone_low) for c in cands
             if c.zone_high is not None and c.zone_low is not None]
    if not zones:
        return (None, None)
    # Intersection where they overlap; otherwise the span of all of them,
    # so a group always reports a usable price region.
    hi = min(z[0] for z in zones)
    lo = max(z[1] for z in zones)
    if hi >= lo:
        return (hi, lo)
    return (max(z[0] for z in zones), min(z[1] for z in zones))


def score_group(cands: list) -> dict:
    """Score one symbol+direction group of candidates.

    Strength combines three things, deliberately in this order:
      1. how many independent modules agree (the dominant term),
      2. how strong each of those modules rated its own find,
      3. whether their zones point at the same price area.
    """
    if not cands:
        return {}

    directional = [c for c in cands if c.direction in ("long", "short")]
    neutral     = [c for c in cands if c.direction == "neutral"]

    modules = sorted({c.module for c in cands})
    dir_modules = sorted({c.module for c in directional})

    # Weighted agreement: each distinct module counts once, at its weight.
    # Neutral modules count at a fraction — they support, they do not decide.
    weighted = sum(MODULE_WEIGHT.get(m, _DEFAULT_WEIGHT) for m in dir_modules)
    weighted += sum(MODULE_WEIGHT.get(m, _DEFAULT_WEIGHT) * NEUTRAL_SUPPORT
                    for m in {c.module for c in neutral})

    # Average of each module's own best score, so one module firing twice
    # cannot inflate the group.
    best_per_module = {}
    for c in cands:
        if c.score > best_per_module.get(c.module, -1):
            best_per_module[c.module] = c.score
    avg_score = (sum(best_per_module.values()) / len(best_per_module)) if best_per_module else 0

    # Zone agreement across directional members.
    zoned = [c for c in directional
             if c.zone_high is not None and c.zone_low is not None]
    aligned_pairs = total_pairs = 0
    for i in range(len(zoned)):
        for j in range(i + 1, len(zoned)):
            total_pairs += 1
            if _overlap_or_near(zoned[i].zone_high, zoned[i].zone_low,
                                zoned[j].zone_high, zoned[j].zone_low):
                aligned_pairs += 1
    zone_agreement = (aligned_pairs / total_pairs) if total_pairs else None

    # Blend. Confluence dominates; module quality adjusts; zone agreement is a
    # bonus rather than a requirement, since not every module produces a zone.
    confluence_term = min(60.0, weighted * 22.0)
    quality_term    = (avg_score / 100.0) * 30.0
    zone_term       = 10.0 * zone_agreement if zone_agreement is not None else 5.0
    strength = int(max(0, min(100, round(confluence_term + quality_term + zone_term))))

    zh, zl = _zone_bounds(directional or cands)

    return {
        "modules":          modules,
        "module_count":     len(modules),
        "directional_count": len(dir_modules),
        "neutral_count":    len({c.module for c in neutral}),
        "strength":         strength,
        "avg_module_score": round(avg_score, 1),
        "weighted_agreement": round(weighted, 2),
        "zone_agreement":   (round(zone_agreement, 2) if zone_agreement is not None else None),
        "zone_high":        zh,
        "zone_low":         zl,
        "timeframes":       sorted({c.timeframe for c in cands if c.timeframe}),
        "best_score":       max(best_per_module.values()) if best_per_module else 0,
    }


def build_confluence_groups(window_hours: int = 12, min_modules: int = 1,
                            symbols: list = None, now=None) -> list:
    """Group active candidates by symbol and direction, scored and ranked.

    Neutral candidates (squeeze, accumulation) attach to whichever directional
    group exists for their symbol, and only form a group of their own when no
    directional read exists — a squeeze with no directional context is still
    worth surfacing, but it should never outrank a directional setup.
    """
    from models import LiveMonitorSignalCandidate as _C
    from sqlalchemy.orm import load_only as _load_only

    now = now or datetime.now(timezone.utc)
    since = now - timedelta(hours=max(1, int(window_hours)))

    q = (_C.query
         .options(_load_only(
             _C.id, _C.module, _C.symbol, _C.exchange, _C.market, _C.timeframe,
             _C.direction, _C.zone_high, _C.zone_low, _C.detected_price,
             _C.score, _C.grade, _C.detected_at))
         .filter(_C.status == "active", _C.detected_at >= since))
    if symbols:
        q = q.filter(_C.symbol.in_([s.upper() for s in symbols][:400]))

    try:
        rows = q.order_by(_C.score.desc()).limit(4000).all()
    except Exception as exc:
        print(f"[SIG-2] confluence query failed: {str(exc)[:120]}")
        return []

    # Bucket by symbol, then split by direction.
    by_symbol: dict = {}
    for r in rows:
        by_symbol.setdefault(r.symbol, []).append(r)

    groups = []
    for symbol, cands in by_symbol.items():
        longs    = [c for c in cands if c.direction == "long"]
        shorts   = [c for c in cands if c.direction == "short"]
        neutrals = [c for c in cands if c.direction == "neutral"]

        made_directional = False
        for direction, members in (("long", longs), ("short", shorts)):
            if not members:
                continue
            # Neutral finds support whichever directions exist.
            full = members + neutrals
            info = score_group(full)
            if info.get("module_count", 0) < min_modules:
                continue
            made_directional = True
            groups.append({
                "symbol":    symbol,
                "direction": direction,
                "exchange":  members[0].exchange,
                "market":    members[0].market,
                "detected_price": members[0].detected_price,
                "candidate_ids":  [c.id for c in full],
                **info,
            })

        # Squeeze-only symbols still surface, but can never outrank a
        # directional group because neutral weight is halved in scoring.
        if not made_directional and neutrals:
            info = score_group(neutrals)
            if info.get("module_count", 0) >= min_modules:
                groups.append({
                    "symbol":    symbol,
                    "direction": "neutral",
                    "exchange":  neutrals[0].exchange,
                    "market":    neutrals[0].market,
                    "detected_price": neutrals[0].detected_price,
                    "candidate_ids":  [c.id for c in neutrals],
                    **info,
                })

    groups.sort(key=lambda g: (g["module_count"], g["strength"]), reverse=True)
    return groups


def filter_groups_for_settings(groups: list, settings) -> list:
    """Keep only the groups a given user's settings would accept.

    Applied before any expensive work, so a user who only wants high-confluence
    long setups never pays for order-flow collection on anything else.
    """
    if not groups or settings is None:
        return []

    try:
        enabled = set(json.loads(settings.enabled_modules_json or "[]"))
    except (ValueError, TypeError):
        enabled = set()
    try:
        tfs = set(json.loads(settings.timeframes_json or "[]"))
    except (ValueError, TypeError):
        tfs = set()
    try:
        watch = set(json.loads(settings.symbols_json or "[]"))
    except (ValueError, TypeError):
        watch = set()

    min_conf   = int(settings.min_confluence or 1)
    allow_long  = bool(settings.allow_long)
    allow_short = bool(settings.allow_short)
    scope      = settings.coin_scope or "top_volume"

    out = []
    for g in groups:
        if g["direction"] == "long"  and not allow_long:
            continue
        if g["direction"] == "short" and not allow_short:
            continue
        # A neutral group has no direction to allow; require at least one
        # direction be enabled so a fully-directional user is not spammed.
        if g["direction"] == "neutral" and not (allow_long or allow_short):
            continue

        # Count only the modules this user actually asked for.
        kept = [m for m in g["modules"] if m in enabled]
        if not kept or len(kept) < min_conf:
            continue

        if tfs and not (set(g.get("timeframes") or []) & tfs):
            continue

        if scope == "watchlist" and g["symbol"] not in watch:
            continue

        out.append({**g, "matched_modules": kept})
    return out
