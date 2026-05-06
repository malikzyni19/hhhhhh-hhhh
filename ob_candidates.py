"""
OB Candidate Engine Preview — Phase 8A
Read-only. No DB writes. No paper trading. No per-pair REST polling.

Price fetching strategy (API-efficient):
  1. Try app PAIR_CACHE from main.py (already populated by scanner — zero extra calls)
  2. Fallback: one batch /fapi/v1/ticker/price call (all symbols, single request)
  3. Final fallback: spot geo-safe mirror
  4. Per-signal fallback: detected_price from SignalEvent row
"""

import json
import time

_FAPI_PRICE_URL  = "https://fapi.binance.com/fapi/v1/ticker/price"
_SPOT_PRICE_URL  = "https://data-api.binance.vision/api/v3/ticker/price"
_CACHE_TTL       = 120  # seconds — accept cache up to 2 minutes old


# ─────────────────────────────────────────────────────────────────────────────
# Price fetching — batch only, never per-pair
# ─────────────────────────────────────────────────────────────────────────────

def _prices_from_app_cache() -> "tuple[dict, str]":
    """Try main.py PAIR_CACHE (populated by scanner runs). Zero extra API calls."""
    try:
        from main import PAIR_CACHE
        cache = PAIR_CACHE.get("perpetual", {})
        age = time.time() - (cache.get("ts") or 0)
        pairs = cache.get("pairs") or []
        if age < _CACHE_TTL and pairs:
            pm = {p["symbol"]: p["price"] for p in pairs if p.get("price", 0) > 0}
            if pm:
                return pm, "app_cache"
    except Exception:
        pass
    return {}, ""


def _prices_from_batch_rest() -> "tuple[dict, str]":
    """One batch REST call to Binance futures price ticker. Returns all symbols."""
    try:
        import requests
        r = requests.get(_FAPI_PRICE_URL, timeout=8)
        if r.status_code == 200:
            pm = {}
            for t in r.json():
                sym = t.get("symbol", "")
                try:
                    price = float(t["price"])
                    if price > 0:
                        pm[sym] = price
                except (KeyError, TypeError, ValueError):
                    pass
            if pm:
                return pm, "batch_futures_ticker"
    except Exception:
        pass

    # Geo-safe spot mirror fallback
    try:
        import requests
        r = requests.get(_SPOT_PRICE_URL, timeout=8)
        if r.status_code == 200:
            pm = {}
            for t in r.json():
                sym = t.get("symbol", "")
                if not sym.endswith("USDT"):
                    continue
                try:
                    price = float(t["price"])
                    if price > 0:
                        pm[sym] = price
                except (KeyError, TypeError, ValueError):
                    pass
            if pm:
                return pm, "batch_spot_ticker_fallback"
    except Exception:
        pass

    return {}, "failed"


def _fetch_all_prices() -> "tuple[dict, str]":
    """Returns (price_map, source_label). Never makes per-pair calls."""
    pm, src = _prices_from_app_cache()
    if pm:
        return pm, src
    return _prices_from_batch_rest()


# ─────────────────────────────────────────────────────────────────────────────
# Candidate logic
# ─────────────────────────────────────────────────────────────────────────────

def _candidate_status(direction: str, zone_high: float, zone_low: float,
                      current_price: float, max_distance_pct: float):
    """
    Returns (status, distance_pct).
    direction: "bullish" / "bearish" (as stored by signal_extractor)
    """
    if current_price is None or current_price <= 0:
        return "MISSING_PRICE", None

    # Inside zone
    if zone_low <= current_price <= zone_high:
        return "TOUCHING_ZONE", 0.0

    bull = direction in ("bullish", "bull", "long")
    bear = direction in ("bearish", "bear", "short")

    if bull:
        if current_price > zone_high:
            dist = (current_price - zone_high) / current_price * 100
            return ("APPROACHING_ZONE" if dist <= max_distance_pct else "TOO_FAR"), round(dist, 4)
        else:
            return "BEYOND_ZONE", None
    elif bear:
        if current_price < zone_low:
            dist = (zone_low - current_price) / current_price * 100
            return ("APPROACHING_ZONE" if dist <= max_distance_pct else "TOO_FAR"), round(dist, 4)
        else:
            return "BEYOND_ZONE", None
    else:
        # Unknown direction: nearest boundary
        dist = min(
            abs(current_price - zone_high) / current_price * 100,
            abs(current_price - zone_low)  / current_price * 100,
        )
        return ("APPROACHING_ZONE" if dist <= max_distance_pct else "TOO_FAR"), round(dist, 4)


def _trade_plan(direction: str, zone_high: float, zone_low: float,
                entry_mode: str, tp_mode: str, tp_pct: float, rr: float) -> dict:
    """Read-only trade plan preview. No order is placed."""
    bull = direction in ("bullish", "bull", "long")

    # Entry price
    if entry_mode == "zone_top":
        entry = zone_high if bull else zone_low
    elif entry_mode == "zone_bottom":
        entry = zone_low if bull else zone_high
    else:  # zone_middle
        entry = (zone_high + zone_low) / 2

    # Stop boundary: candle close beyond OB zone
    stop_boundary = zone_low if bull else zone_high

    # TP
    tp_price = None
    rr_preview = None
    if tp_mode == "fixed_pct" and entry and tp_pct > 0:
        tp_price = round(entry * (1 + tp_pct / 100) if bull else entry * (1 - tp_pct / 100), 6)
    elif tp_mode == "rr" and entry and stop_boundary and rr > 0:
        risk = abs(entry - stop_boundary)
        if risk > 0:
            tp_price = round(entry + risk * rr if bull else entry - risk * rr, 6)
            rr_preview = rr

    return {
        "entry_price_preview": round(entry, 6) if entry else None,
        "sl_rule": "candle_close_beyond_ob_zone",
        "stop_boundary": round(stop_boundary, 6) if stop_boundary else None,
        "tp_price_preview": tp_price,
        "rr_preview": rr_preview,
        "entry_mode": entry_mode,
        "tp_mode": tp_mode,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────────────

def run_ob_candidates(
    limit: int = 100,
    timeframe: str = None,
    setup_type: str = None,
    strength_min: float = 0.0,
    max_distance_pct: float = 1.0,
    source: str = "live",
    pair: str = None,
    entry_mode: str = "zone_middle",
    tp_mode: str = "rr",
    tp_pct: float = 0.30,
    rr: float = 1.5,
) -> dict:
    """
    OB Candidate Engine Preview — read-only, no DB writes, no paper trading.
    Uses batch price fetch (one call for all symbols) or app cache.
    """
    from models import SignalEvent
    from backtest_ob import extract_ob_strength_from_meta

    summary = {
        "checked": 0,
        "active_candidates": 0,
        "approaching_zone": 0,
        "touching_zone": 0,
        "too_far": 0,
        "beyond_zone": 0,
        "missing_strength": 0,
        "missing_price": 0,
    }

    # ── DB query ──────────────────────────────────────────────────────────────
    q = SignalEvent.query.filter(
        SignalEvent.module == "ob",
        SignalEvent.setup_type.in_(["OB_APPROACH", "OB_CONSOL"]),
    )
    if source == "live":
        q = q.filter(SignalEvent.source == "live")
    if timeframe:
        q = q.filter(SignalEvent.timeframe == timeframe)
    if setup_type and setup_type != "all":
        q = q.filter(SignalEvent.setup_type == setup_type)
    if pair:
        q = q.filter(SignalEvent.pair.ilike(f"%{pair}%"))

    events = q.order_by(SignalEvent.detected_at.desc()).limit(limit).all()

    # ── Batch price fetch (one call) ──────────────────────────────────────────
    price_map, price_fetch_mode = _fetch_all_prices()

    candidates = []

    for ev in events:
        summary["checked"] += 1

        # Parse meta
        try:
            raw_meta = json.loads(ev.raw_meta_json or "{}")
        except Exception:
            raw_meta = {}

        # True OB strength only — never score, never alert_strength
        ob_strength, ob_strength_source = extract_ob_strength_from_meta(raw_meta)

        # Strength gate
        if strength_min > 0 and (ob_strength is None or ob_strength < strength_min):
            summary["missing_strength"] += 1
            candidates.append({
                "signal_id":          ev.signal_id,
                "pair":               ev.pair,
                "timeframe":          ev.timeframe,
                "setup_type":         ev.setup_type,
                "direction":          ev.direction,
                "detected_at":        ev.detected_at.isoformat() if ev.detected_at else None,
                "zone_high":          ev.zone_high,
                "zone_low":           ev.zone_low,
                "ob_strength":        ob_strength,
                "ob_strength_source": ob_strength_source,
                "current_price":      None,
                "price_source":       None,
                "distance_pct":       None,
                "candidate_status":   "MISSING_STRENGTH",
                "candidate_expiry_reason": "missing_true_ob_strength",
                "would_monitor":      False,
                "trade_plan":         None,
            })
            continue

        # Current price — batch map first, then detected_price fallback
        current_price = price_map.get(ev.pair)
        price_source  = price_fetch_mode if current_price is not None else None

        if current_price is None:
            current_price = ev.detected_price
            price_source  = "detected_price_fallback"

        # Candidate status
        if current_price is None or current_price <= 0:
            status        = "MISSING_PRICE"
            distance_pct  = None
            expiry_reason = "missing_price"
            would_monitor = False
            summary["missing_price"] += 1
        else:
            status, distance_pct = _candidate_status(
                ev.direction, ev.zone_high, ev.zone_low, current_price, max_distance_pct
            )
            if status == "APPROACHING_ZONE":
                summary["approaching_zone"] += 1
                summary["active_candidates"] += 1
                expiry_reason = None
                would_monitor = True
            elif status == "TOUCHING_ZONE":
                summary["touching_zone"] += 1
                summary["active_candidates"] += 1
                expiry_reason = None
                would_monitor = True
            elif status == "TOO_FAR":
                summary["too_far"] += 1
                expiry_reason = "too_far_from_zone"
                would_monitor = False
            elif status == "BEYOND_ZONE":
                summary["beyond_zone"] += 1
                expiry_reason = "beyond_zone"
                would_monitor = False
            else:
                expiry_reason = None
                would_monitor = False

        # Trade plan preview — only for active candidates
        plan = None
        if would_monitor:
            plan = _trade_plan(
                ev.direction, ev.zone_high, ev.zone_low,
                entry_mode, tp_mode, tp_pct, rr,
            )

        candidates.append({
            "signal_id":          ev.signal_id,
            "pair":               ev.pair,
            "timeframe":          ev.timeframe,
            "setup_type":         ev.setup_type,
            "direction":          ev.direction,
            "detected_at":        ev.detected_at.isoformat() if ev.detected_at else None,
            "zone_high":          ev.zone_high,
            "zone_low":           ev.zone_low,
            "ob_strength":        ob_strength,
            "ob_strength_source": ob_strength_source,
            "current_price":      current_price,
            "price_source":       price_source,
            "distance_pct":       distance_pct,
            "candidate_status":   status,
            "candidate_expiry_reason": expiry_reason,
            "would_monitor":      would_monitor,
            "trade_plan":         plan,
        })

    return {
        "ok":   True,
        "mode": "ob_candidate_preview_read_only",
        "filters": {
            "limit":           limit,
            "timeframe":       timeframe or "all",
            "setup_type":      setup_type or "all",
            "strength_min":    strength_min,
            "max_distance_pct": max_distance_pct,
            "source":          source,
            "pair":            pair or None,
            "entry_mode":      entry_mode,
            "tp_mode":         tp_mode,
            "tp_pct":          tp_pct,
            "rr":              rr,
        },
        "summary": summary,
        "api_usage": {
            "price_fetch_mode":   price_fetch_mode,
            "per_pair_rest_calls": 0,
            "notes": "One batch ticker call (or app cache). No per-pair REST polling.",
        },
        "candidates": candidates,
    }
