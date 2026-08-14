"""Phase SIG-2: Signal intake — every scan tab, one shape.

Each scanner in this app returns its own result shape: the order-block scan
emits zone alerts, the compressed scan emits a squeeze box, ATH/ATL emits a
break status, bias emits a directional flip. None of them can be compared to
each other in that state, which is why cross-module confluence has never been
possible.

This module normalizes all of them into one `candidate` record so the funnel
downstream can ask the question that actually matters: which pairs are being
flagged by several independent scans at once?

Detection only. No AI, no delivery, no order placement.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone, timedelta


# Candle length per timeframe, used to bucket detections so the same setup
# re-scanned within one candle does not create duplicate rows.
_TF_MS = {
    "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000,
    "30m": 1_800_000, "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000,
    "6h": 21_600_000, "12h": 43_200_000, "1d": 86_400_000,
}
_DEFAULT_TF_MS = 3_600_000

# How long a candidate stays eligible after detection, as a multiple of its
# own candle. A 4h setup is worth watching for longer than a 5m one.
_EXPIRY_CANDLES = 3

MAX_META_CHARS = 1200


def _f(val, default=None):
    """Float or default — tolerates strings, None, and junk."""
    if val is None:
        return default
    try:
        out = float(val)
    except (TypeError, ValueError):
        return default
    if out != out or out in (float("inf"), float("-inf")):
        return default
    return out


def _clamp_score(val) -> int:
    n = _f(val, 0) or 0
    return int(max(0, min(100, round(n))))


def _bucket_ms(timeframe: str, when: datetime) -> int:
    span = _TF_MS.get((timeframe or "").lower(), _DEFAULT_TF_MS)
    epoch_ms = int(when.timestamp() * 1000)
    return epoch_ms // span * span


def build_candidate_key(module: str, symbol: str, exchange: str,
                        timeframe: str, direction: str, bucket: int) -> str:
    """Stable dedup identity for one setup within one candle."""
    raw = f"{module}|{symbol}|{exchange}|{timeframe}|{direction}|{bucket}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:40]


def _candidate(module, symbol, direction, *, exchange="binance",
               market="perpetual", timeframe=None, zone_high=None,
               zone_low=None, price=None, score=0, grade=None, meta=None,
               now=None):
    """Assemble one normalized candidate dict."""
    now = now or datetime.now(timezone.utc)
    symbol = (symbol or "").upper().strip()
    if not symbol:
        return None

    direction = (direction or "neutral").lower()
    if direction not in ("long", "short", "neutral"):
        direction = "neutral"

    tf = (timeframe or "").lower() or None
    bucket = _bucket_ms(tf, now)
    span_ms = _TF_MS.get(tf or "", _DEFAULT_TF_MS)

    # Zones arrive in either order depending on the scanner — normalize so
    # high is always the larger of the two.
    zh, zl = _f(zone_high), _f(zone_low)
    if zh is not None and zl is not None and zl > zh:
        zh, zl = zl, zh

    meta_txt = None
    if meta:
        try:
            meta_txt = json.dumps(meta, separators=(",", ":"))[:MAX_META_CHARS]
        except (TypeError, ValueError):
            meta_txt = None

    return {
        "candidate_key":  build_candidate_key(module, symbol, exchange, tf or "", direction, bucket),
        "module":         module,
        "symbol":         symbol,
        "exchange":       (exchange or "binance").lower(),
        "market":         (market or "perpetual").lower(),
        "timeframe":      tf,
        "direction":      direction,
        "zone_high":      zh,
        "zone_low":       zl,
        "detected_price": _f(price),
        "score":          _clamp_score(score),
        "grade":          (str(grade)[:20] if grade else None),
        "meta_json":      meta_txt,
        "detected_at":    now,
        "expires_at":     now + timedelta(milliseconds=span_ms * _EXPIRY_CANDLES),
    }


# ══════════════════════════════════════════════════════════════════════════════
# Per-module adapters
#
# Each takes one raw scanner result and returns a list of candidates (usually
# one, sometimes zero). They are deliberately forgiving: a scanner changing an
# unrelated field should never break intake, so every read is defensive and a
# result that cannot be understood is skipped rather than raising.
# ══════════════════════════════════════════════════════════════════════════════

def adapt_zone_scan(result: dict, exchange: str = "binance",
                    market: str = "perpetual", now=None) -> list:
    """/api/scan — order block, breaker block, FVG, FIB confluence.

    These arrive as `topAlerts`, each already carrying a zone and a side.
    """
    out = []
    if not isinstance(result, dict):
        return out

    symbol = result.get("symbol") or result.get("pair")
    alerts = result.get("topAlerts") or []
    if not isinstance(alerts, list):
        return out

    _MODULE_BY_KIND = {
        "ob": "ob", "orderblock": "ob", "order_block": "ob",
        "bb": "bb", "breaker": "bb", "breakerblock": "bb", "breaker_block": "bb",
        "fvg": "fvg", "imbalance": "fvg",
        "fib": "fib_confluence", "fib_confluence": "fib_confluence",
    }

    for a in alerts:
        if not isinstance(a, dict):
            continue
        kind = str(a.get("type") or a.get("kind") or a.get("module") or "").lower()
        kind = kind.replace(" ", "").replace("-", "")
        module = _MODULE_BY_KIND.get(kind)
        if module is None:
            continue

        side = str(a.get("side") or a.get("direction") or a.get("bias") or "").lower()
        if side in ("bull", "bullish", "long", "demand", "buy"):
            direction = "long"
        elif side in ("bear", "bearish", "short", "supply", "sell"):
            direction = "short"
        else:
            direction = "neutral"

        cand = _candidate(
            module, symbol, direction,
            exchange=exchange, market=market,
            timeframe=a.get("timeframe") or result.get("timeframe"),
            zone_high=a.get("zoneHigh") or a.get("high") or a.get("top"),
            zone_low=a.get("zoneLow") or a.get("low") or a.get("bottom"),
            price=result.get("price") or a.get("price"),
            score=a.get("score") or a.get("confidence") or 50,
            grade=a.get("grade") or a.get("quality"),
            meta={"kind": kind, "label": a.get("label")},
            now=now,
        )
        if cand:
            out.append(cand)
    return out


def adapt_compressed(result: dict, exchange: str = "binance",
                     market: str = "perpetual", now=None) -> list:
    """/api/compressed_scan — a volatility squeeze box awaiting a breakout.

    Direction is genuinely unknown at squeeze time: that is the whole point of
    a compression setup, so it stays neutral and the box becomes the zone.
    Where price sits inside the box is the useful hint, and it is kept in meta
    rather than being forced into a direction the scan did not claim.
    """
    if not isinstance(result, dict):
        return []
    cand = _candidate(
        "compressed", result.get("symbol"), "neutral",
        exchange=exchange, market=market,
        timeframe=result.get("timeframe"),
        zone_high=result.get("boxHigh") or result.get("high"),
        zone_low=result.get("boxLow") or result.get("low"),
        price=result.get("price"),
        score=result.get("compressionScore"),
        grade=result.get("compressionGrade"),
        meta={
            "boxLocation":      result.get("boxLocation"),
            "pricePositionPct": result.get("pricePositionPct"),
            "atrState":         result.get("atrState"),
            "volumeState":      result.get("volumeState"),
            "rangePct":         result.get("rangePct"),
        },
        now=now,
    )
    return [cand] if cand else []


def adapt_ath_atl(result: dict, exchange: str = "binance",
                  market: str = "perpetual", now=None) -> list:
    """/api/ath_atl_scan — reactions at all-time or window extremes.

    `statusKind` carries the meaning. A break above reads long, a break below
    reads short; approaching an extreme without breaking it stays neutral,
    because it resolves either way.
    """
    if not isinstance(result, dict):
        return []

    status = str(result.get("statusKind") or "").lower()
    if "ath" in status and ("break" in status or "new" in status):
        direction = "long"
    elif "atl" in status and ("break" in status or "new" in status):
        direction = "short"
    else:
        direction = "neutral"

    # No native 0-100 score — derive one from how decisive the move is.
    break_pct = abs(_f(result.get("breakPct"), 0) or 0)
    dist_pct  = abs(_f(result.get("distancePct"), 0) or 0)
    score = 55 + min(35, break_pct * 7) if direction != "neutral" \
            else max(20, 50 - min(30, dist_pct * 5))

    cand = _candidate(
        "ath_atl", result.get("symbol"), direction,
        exchange=exchange, market=market,
        timeframe=result.get("timeframe"),
        zone_high=result.get("windowHigh") or result.get("previousAth"),
        zone_low=result.get("windowLow") or result.get("previousAtl"),
        price=result.get("price"),
        score=score,
        grade=result.get("statusKind"),
        meta={
            "tags":        (result.get("tags") or [])[:6],
            "breakPct":    result.get("breakPct"),
            "distancePct": result.get("distancePct"),
            "windowHours": result.get("windowHours"),
        },
        now=now,
    )
    return [cand] if cand else []


def adapt_bias(result: dict, exchange: str = "binance",
               market: str = "perpetual", now=None) -> list:
    """/api/bias_scan — a higher-timeframe directional flip.

    This one already carries an explicit direction and score, so it is the
    closest of the scanners to the normalized shape.
    """
    if not isinstance(result, dict):
        return []

    raw_dir = str(result.get("bias") or result.get("direction") or "").lower()
    if raw_dir in ("bull", "bullish", "long", "up"):
        direction = "long"
    elif raw_dir in ("bear", "bearish", "short", "down"):
        direction = "short"
    else:
        direction = "neutral"

    cand = _candidate(
        "bias_shift", result.get("symbol"), direction,
        exchange=exchange, market=market,
        timeframe=result.get("timeframe"),
        price=result.get("price"),
        score=result.get("score") or result.get("confidence"),
        grade=result.get("grade"),
        meta={
            "signal":       result.get("signal"),
            "gatesPassed":  result.get("gatesPassed"),
            "gatesChecked": result.get("gatesChecked"),
            "volSpike":     result.get("volSpike"),
        },
        now=now,
    )
    return [cand] if cand else []


def adapt_accumulation(result: dict, exchange: str = "binance",
                       market: str = "perpetual", now=None) -> list:
    """/api/trending_scan — volume and momentum expansion.

    This scan reports no zone and no score of its own; direction comes from
    the mode it was run in, and strength is derived from relative volume with
    price change as a secondary term. Volume is weighted more heavily because
    an expansion on flat volume is the weaker of the two signals.
    """
    if not isinstance(result, dict):
        return []

    mode = str(result.get("mode") or "").lower()
    change = _f(result.get("changePct"), 0) or 0
    if mode == "bullish":
        direction = "long"
    elif mode == "bearish":
        direction = "short"
    elif mode in ("movers", "high_volume"):
        # Undirected modes: let the move itself speak, but only when it is
        # decisive enough to mean something.
        direction = "long" if change >= 3 else ("short" if change <= -3 else "neutral")
    else:
        direction = "neutral"

    rel_vol = _f(result.get("relVol"), 1) or 1
    score = 35 + min(40, (rel_vol - 1) * 20) + min(25, abs(change) * 2)

    cand = _candidate(
        "accumulation", result.get("symbol"), direction,
        exchange=exchange, market=market,
        timeframe=result.get("timeframe"),
        price=result.get("price"),
        score=score,
        grade=mode or None,
        meta={
            "mode":        mode,
            "changePct":   result.get("changePct"),
            "relVol":      result.get("relVol"),
            "quoteVolume": result.get("quoteVolume"),
        },
        now=now,
    )
    return [cand] if cand else []


ADAPTERS = {
    "scan":         adapt_zone_scan,     # multi-module: ob / bb / fvg / fib
    "compressed":   adapt_compressed,
    "ath_atl":      adapt_ath_atl,
    "bias":         adapt_bias,
    "accumulation": adapt_accumulation,
}


def normalize_scan_results(source: str, results, exchange="binance",
                           market="perpetual", now=None) -> list:
    """Run the adapter for `source` across a scanner's result list.

    Never raises: one malformed result is skipped, the rest still land.
    """
    fn = ADAPTERS.get(source)
    if fn is None or not results:
        return []
    if isinstance(results, dict):
        results = [results]

    out = []
    for r in results:
        try:
            out.extend(fn(r, exchange=exchange, market=market, now=now) or [])
        except Exception as exc:
            print(f"[SIG-2] adapter {source} skipped a result: {str(exc)[:100]}")
    return out


def record_candidates(candidates: list) -> dict:
    """Persist candidates, updating rather than duplicating repeat detections.

    Returns {inserted, refreshed, errors}. Safe to call from a request path —
    it never raises, because intake must never be able to break a scan.
    """
    if not candidates:
        return {"inserted": 0, "refreshed": 0, "errors": 0}

    from models import db, LiveMonitorSignalCandidate as _C

    inserted = refreshed = errors = 0
    keys = [c["candidate_key"] for c in candidates]

    try:
        # One query for every key, rather than one lookup per candidate.
        existing = {
            row.candidate_key: row
            for row in _C.query.filter(_C.candidate_key.in_(keys)).all()
        }
    except Exception as exc:
        print(f"[SIG-2] candidate preload failed: {str(exc)[:120]}")
        existing = {}

    for c in candidates:
        try:
            row = existing.get(c["candidate_key"])
            if row is not None:
                # Same setup, same candle — refresh liveness and let the score
                # move if the scan now rates it differently.
                row.last_seen_at = c["detected_at"]
                row.score        = c["score"]
                row.detected_price = c["detected_price"]
                if row.status == "expired":
                    row.status = "active"
                refreshed += 1
            else:
                db.session.add(_C(status="active", last_seen_at=c["detected_at"], **c))
                inserted += 1
        except Exception as exc:
            errors += 1
            print(f"[SIG-2] candidate write error: {str(exc)[:120]}")

    try:
        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        print(f"[SIG-2] candidate commit failed: {str(exc)[:150]}")
        return {"inserted": 0, "refreshed": 0, "errors": len(candidates)}

    return {"inserted": inserted, "refreshed": refreshed, "errors": errors}


def expire_stale_candidates(now=None) -> int:
    """Mark candidates whose window has passed. Cheap, bounded UPDATE."""
    from models import db, LiveMonitorSignalCandidate as _C
    now = now or datetime.now(timezone.utc)
    try:
        n = (_C.query
             .filter(_C.status == "active", _C.expires_at.isnot(None),
                     _C.expires_at < now)
             .update({"status": "expired"}, synchronize_session=False))
        db.session.commit()
        return int(n or 0)
    except Exception as exc:
        db.session.rollback()
        print(f"[SIG-2] expiry sweep failed: {str(exc)[:120]}")
        return 0
