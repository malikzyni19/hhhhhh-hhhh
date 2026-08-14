"""Phase SIG-4: Trigger — deciding when a setup is actually worth judging.

Finding a setup and acting on one are different moments, sometimes hours
apart. A scan can flag a demand zone while price is still far above it; there
is nothing to say yet. This module holds a setup in a waiting state until the
things that make it real have arrived:

  - price has actually reached (or is nearing) the zone,
  - enough order-flow history exists for CVD and OI to mean anything,
  - the price feed is fresh, so we are not judging stale data.

Only then does it arm, and only armed setups cost an AI call.

No AI, no delivery, no order placement — this stage only decides readiness.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone

import main as _m


# A CVD divergence read on three candles is noise. This is the floor below
# which the order-flow evidence is not worth asking the AI about.
MIN_FLOW_CANDLES = 12

# Price feed older than this and we are judging the past, not the present.
MAX_PRICE_AGE_SEC = 90

# States a group can be in.
WAITING = "waiting"
ARMED   = "armed"
INVALID = "invalid"


def get_symbol_price(symbol: str, exchange: str = "binance") -> dict:
    """Current price for a plain symbol, with its age.

    Prefers the in-memory websocket cache (free, sub-second) and falls back to
    a short-TTL REST read. Returns {ok, price, age_sec, source}.
    """
    symbol = (symbol or "").upper()
    if not symbol:
        return {"ok": False, "error": "no_symbol"}

    try:
        entry, status = _m._lm_ws_get(exchange, symbol)
        if entry:
            price = entry.get("live_price") or entry.get("mark_price")
            if price:
                return {"ok": True, "price": float(price),
                        "age_sec": float(entry.get("data_age_sec") or 0),
                        "source": "websocket", "status": status}
    except Exception:
        pass

    try:
        rest = _m._lm_rest_cached(f"sigmark:{symbol}", 10,
                                  lambda: _m._lm_fetch_mark_price_rest(symbol))
        if rest and rest.get("mark_price"):
            return {"ok": True, "price": float(rest["mark_price"]),
                    "age_sec": 10.0, "source": "rest", "status": "interval"}
    except Exception as exc:
        return {"ok": False, "error": f"price_error:{str(exc)[:60]}"}

    return {"ok": False, "error": "price_unavailable"}


def zone_position(price: float, zone_high, zone_low) -> dict:
    """Where price sits relative to a zone.

    `distance_pct` is 0 inside the zone, and otherwise how far outside it is
    as a percentage of the zone's midpoint — the number the approach setting
    is compared against.
    """
    if price is None or zone_high is None or zone_low is None:
        return {"known": False}

    hi, lo = (zone_high, zone_low) if zone_high >= zone_low else (zone_low, zone_high)
    mid = (hi + lo) / 2.0
    if mid <= 0:
        return {"known": False}

    if lo <= price <= hi:
        return {"known": True, "inside": True, "distance_pct": 0.0, "side": "inside"}

    if price > hi:
        return {"known": True, "inside": False,
                "distance_pct": (price - hi) / mid * 100.0, "side": "above"}
    return {"known": True, "inside": False,
            "distance_pct": (lo - price) / mid * 100.0, "side": "below"}


def flow_history_ready(symbol: str, min_candles: int = MIN_FLOW_CANDLES) -> dict:
    """Whether enough order-flow history exists to be worth reading.

    Counts stored 1m flow candles rather than loading them, so this stays
    cheap enough to run on every group every cycle.
    """
    try:
        from models import LiveMonitorFlowCandle as _FC
        n = (_FC.query
             .filter(_FC.symbol == (symbol or "").upper(), _FC.timeframe == "1m")
             .limit(min_candles).count())
        return {"ok": n >= min_candles, "candles": n, "required": min_candles}
    except Exception as exc:
        return {"ok": False, "candles": 0, "required": min_candles,
                "error": str(exc)[:80]}


def evaluate_group(group: dict, settings, now=None) -> dict:
    """Decide whether one confluence group is ready to be judged.

    Returns {state, reasons, price, zone, ...}. `reasons` always explains the
    outcome, so a setup sitting in waiting can be understood at a glance
    rather than looking like the system is doing nothing.
    """
    now = now or datetime.now(timezone.utc)
    symbol    = group.get("symbol")
    direction = group.get("direction") or "neutral"
    zone_high = group.get("zone_high")
    zone_low  = group.get("zone_low")

    out = {
        "symbol": symbol, "direction": direction,
        "state": WAITING, "reasons": [], "checks": {},
        "evaluated_at": now.isoformat(),
    }

    # A group with no direction has nothing to alert about. It still counts as
    # supporting evidence inside a directional group elsewhere.
    if direction not in ("long", "short"):
        out["state"] = WAITING
        out["reasons"].append("no_direction_yet")
        return out

    trigger_mode = getattr(settings, "trigger_mode", "in_zone") or "in_zone"
    approach_pct = float(getattr(settings, "approach_pct", 1.5) or 1.5)

    # ── Price ────────────────────────────────────────────────────────────────
    px = get_symbol_price(symbol, group.get("exchange") or "binance")
    out["checks"]["price"] = px
    if not px.get("ok"):
        out["state"] = WAITING
        out["reasons"].append("price_unavailable")
        return out

    price = px["price"]
    out["price"] = price

    if px.get("age_sec", 0) > MAX_PRICE_AGE_SEC:
        out["state"] = WAITING
        out["reasons"].append(f"price_stale_{int(px['age_sec'])}s")
        return out

    # ── Zone ─────────────────────────────────────────────────────────────────
    pos = zone_position(price, zone_high, zone_low)
    out["checks"]["zone"] = pos
    out["zone"] = {"high": zone_high, "low": zone_low}

    if not pos.get("known"):
        # Some scans (bias, trending) legitimately produce no zone. Those can
        # still arm — their evidence is directional, not positional.
        out["reasons"].append("no_zone_directional_only")
    else:
        # A setup price has already run past is not the setup any more.
        if direction == "long" and pos["side"] == "below":
            out["state"] = INVALID
            out["reasons"].append("price_below_long_zone")
            return out
        if direction == "short" and pos["side"] == "above":
            out["state"] = INVALID
            out["reasons"].append("price_above_short_zone")
            return out

        if pos["inside"]:
            out["reasons"].append("price_in_zone")
        elif trigger_mode in ("approach", "both"):
            if pos["distance_pct"] > approach_pct:
                out["state"] = WAITING
                out["reasons"].append(
                    f"price_{pos['distance_pct']:.2f}pct_away_needs_{approach_pct}")
                return out
            out["reasons"].append(f"price_approaching_{pos['distance_pct']:.2f}pct")
        else:
            # in_zone mode: nothing short of actually reaching the zone counts.
            out["state"] = WAITING
            out["reasons"].append(f"price_outside_zone_{pos['distance_pct']:.2f}pct")
            return out

    # ── Order-flow history ───────────────────────────────────────────────────
    flow = flow_history_ready(symbol)
    out["checks"]["flow_history"] = flow
    if not flow.get("ok"):
        out["state"] = WAITING
        out["reasons"].append(
            f"flow_history_building_{flow.get('candles',0)}/{flow.get('required')}")
        return out

    # Everything the cheap checks can establish is established.
    out["state"] = ARMED
    out["alert_kind"] = ("in_zone" if (pos.get("known") and pos.get("inside"))
                         else "approach")
    out["reasons"].append("ready_for_verdict")
    return out


def evaluate_groups(groups: list, settings, now=None) -> dict:
    """Evaluate many groups, split by state.

    Returns {armed, waiting, invalid} lists, each entry carrying its group and
    its evaluation so the caller does not have to re-match them.
    """
    armed, waiting, invalid = [], [], []
    for g in (groups or []):
        try:
            ev = evaluate_group(g, settings, now=now)
        except Exception as exc:
            print(f"[SIG-4] trigger error {g.get('symbol')}: {str(exc)[:100]}")
            continue
        row = {"group": g, "evaluation": ev}
        if ev["state"] == ARMED:
            armed.append(row)
        elif ev["state"] == INVALID:
            invalid.append(row)
        else:
            waiting.append(row)

    # Strongest first — if a daily cap bites, it should bite the weakest.
    armed.sort(key=lambda r: (r["group"].get("module_count", 0),
                              r["group"].get("strength", 0)), reverse=True)
    return {"armed": armed, "waiting": waiting, "invalid": invalid}
