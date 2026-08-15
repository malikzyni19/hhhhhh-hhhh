"""Phase SIG-7: Outcomes — did the signal actually work?

This is what turns the whole system from opinions into a measured track
record. Without it, "high accuracy" stays a hope; with it, you can say which
scan tabs earn their place and which do not.

How resolution works, and its one honest limitation:

Rather than polling the current price every few minutes — which would miss a
target touched between two checks — this walks the 1-minute flow candles
already stored for the symbol and replays the price path from the moment the
alert was sent. That gives the correct ORDER of events, which matters: a
setup that hit its stop before its target is a loss, even if it later reached
the target.

The limitation: flow candles record each minute's closing price, not its high
and low. A wick that pierced a level and snapped back inside the same minute
is not seen. Results are therefore slightly conservative — a real touch can
be missed, but a touch is never invented.

No order placement. Read-only measurement plus a follow-up message.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta

import main as _m
from live_monitor.telegram_notify import escape_html, send_telegram_message


# Beyond this, an unresolved alert is closed as expired. Kept under the
# flow-candle retention window so the price path is still available.
MAX_TRACK_HOURS = 48

# Give an alert at least this long before judging it — resolving one second
# after sending would just measure noise.
MIN_TRACK_MINUTES = 2

# Cap on candles replayed per alert, so one very old alert cannot pull a huge
# result set.
MAX_CANDLES = 3000


def _price_path(symbol: str, since: datetime, until: datetime = None) -> list:
    """1-minute closes for `symbol` between two times, oldest first."""
    from models import LiveMonitorFlowCandle as _FC
    from sqlalchemy.orm import load_only as _load_only

    start_ms = int(since.timestamp() * 1000)
    q = (_FC.query
         .options(_load_only(_FC.candle_open_ms, _FC.price_close))
         .filter(_FC.symbol == (symbol or "").upper(),
                 _FC.timeframe == "1m",
                 _FC.candle_open_ms >= start_ms,
                 _FC.price_close.isnot(None)))
    if until is not None:
        q = q.filter(_FC.candle_open_ms <= int(until.timestamp() * 1000))

    rows = q.order_by(_FC.candle_open_ms.asc()).limit(MAX_CANDLES).all()
    return [(int(r.candle_open_ms), float(r.price_close)) for r in rows]


def resolve_from_path(path: list, direction: str, entry, stop, target) -> dict:
    """Replay a price path and report the first level touched.

    Order is the whole point: whichever of stop or target is reached first
    decides the outcome, regardless of what happens afterwards.
    """
    out = {"resolved": False, "outcome": "pending", "reason": None,
           "price": None, "at_ms": None, "mfe_pct": None, "mae_pct": None,
           "entry_touched": False, "samples": len(path or [])}
    if not path or stop is None or target is None:
        return out

    ref = entry if entry else path[0][1]
    if not ref:
        return out

    best = worst = 0.0
    for ts, px in path:
        # Excursion is measured in the trade's favour/against, so a short
        # moving down is favourable.
        if direction == "long":
            move = (px - ref) / ref * 100.0
        else:
            move = (ref - px) / ref * 100.0
        best  = max(best, move)
        worst = min(worst, move)

        if entry and not out["entry_touched"]:
            if (direction == "long" and px <= entry) or \
               (direction == "short" and px >= entry):
                out["entry_touched"] = True

        if direction == "long":
            if px <= stop:
                out.update(resolved=True, outcome="stop_hit",
                           reason="stop_touched", price=px, at_ms=ts)
                break
            if px >= target:
                out.update(resolved=True, outcome="target_hit",
                           reason="target_touched", price=px, at_ms=ts)
                break
        elif direction == "short":
            if px >= stop:
                out.update(resolved=True, outcome="stop_hit",
                           reason="stop_touched", price=px, at_ms=ts)
                break
            if px <= target:
                out.update(resolved=True, outcome="target_hit",
                           reason="target_touched", price=px, at_ms=ts)
                break

    out["mfe_pct"] = round(best, 3)
    out["mae_pct"] = round(worst, 3)
    return out


def result_in_r(direction: str, entry, stop, exit_price) -> float:
    """Result as a multiple of the risk taken — the only comparable unit.

    A +2R on a tight stop and a +2R on a wide one are the same quality of
    call, which raw percentages hide.
    """
    try:
        entry, stop, exit_price = float(entry), float(stop), float(exit_price)
    except (TypeError, ValueError):
        return None
    risk = abs(entry - stop)
    if risk <= 0:
        return None
    gain = (exit_price - entry) if direction == "long" else (entry - exit_price)
    return round(gain / risk, 2)


def _followup_message(alert, res: dict) -> str:
    """Short follow-up, threaded under the original alert."""
    won  = res["outcome"] == "target_hit"
    mark = "✅" if won else ("🛑" if res["outcome"] == "stop_hit" else "⏳")
    word = ("hit target" if won else
            "hit stop" if res["outcome"] == "stop_hit" else "expired")

    lines = [f"{mark} <b>{escape_html(alert.symbol)}</b> "
             f"{escape_html((alert.direction or '').upper())} — {escape_html(word)}"]

    if alert.result_r is not None:
        sign = "+" if alert.result_r > 0 else ""
        lines.append(f"Result  <b>{sign}{escape_html(alert.result_r)}R</b>")
    if res.get("price"):
        lines.append(f"Exit    {escape_html(res['price'])}")
    if alert.created_at:
        created = alert.created_at
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        hours = (datetime.now(timezone.utc) - created).total_seconds() / 3600.0
        lines.append(f"Held    {hours:.1f}h")
    return "\n".join(lines)


def resolve_alert(alert, settings=None, now=None, send_followup: bool = True) -> dict:
    """Resolve one alert if its price path says so. Never raises."""
    from models import db as _db

    now = now or datetime.now(timezone.utc)

    created = alert.created_at
    if created is None:
        return {"ok": False, "reason": "no_created_at"}
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)

    age_min = (now - created).total_seconds() / 60.0
    if age_min < MIN_TRACK_MINUTES:
        return {"ok": True, "outcome": "pending", "reason": "too_fresh"}

    # Alerts the AI never gave levels for cannot be scored.
    if alert.stop_loss is None or alert.take_profit_1 is None:
        if age_min / 60.0 > MAX_TRACK_HOURS:
            try:
                alert.outcome = "expired"
                alert.outcome_reason = "no_levels_to_track"
                alert.outcome_at = now
                _db.session.commit()
            except Exception:
                _db.session.rollback()
            return {"ok": True, "outcome": "expired", "reason": "no_levels"}
        return {"ok": True, "outcome": "pending", "reason": "no_levels"}

    try:
        path = _price_path(alert.symbol, created, now)
    except Exception as exc:
        return {"ok": False, "reason": f"path_error:{str(exc)[:80]}"}

    res = resolve_from_path(path, alert.direction, alert.entry_price,
                            alert.stop_loss, alert.take_profit_1)

    expired = (age_min / 60.0) > MAX_TRACK_HOURS
    if not res["resolved"] and not expired:
        # Still running — record the excursions so an open alert still shows
        # how it is behaving.
        try:
            alert.mfe_pct = res.get("mfe_pct")
            alert.mae_pct = res.get("mae_pct")
            _db.session.commit()
        except Exception:
            _db.session.rollback()
        return {"ok": True, "outcome": "pending",
                "reason": "no_level_touched_yet", "samples": res["samples"]}

    try:
        if res["resolved"]:
            alert.outcome        = res["outcome"]
            alert.outcome_reason = res["reason"]
            alert.outcome_price  = res["price"]
            alert.result_r       = result_in_r(alert.direction, alert.entry_price,
                                               alert.stop_loss, res["price"])
        else:
            alert.outcome        = "expired"
            alert.outcome_reason = (
                "expired_without_entry" if not res.get("entry_touched")
                else "expired_unresolved")
            alert.outcome_price  = path[-1][1] if path else None
            if alert.outcome_price is not None:
                alert.result_r = result_in_r(alert.direction, alert.entry_price,
                                             alert.stop_loss, alert.outcome_price)
        alert.outcome_at = now
        alert.mfe_pct    = res.get("mfe_pct")
        alert.mae_pct    = res.get("mae_pct")
        _db.session.commit()
    except Exception as exc:
        _db.session.rollback()
        return {"ok": False, "reason": f"save_failed:{str(exc)[:80]}"}

    result = {"ok": True, "outcome": alert.outcome, "reason": alert.outcome_reason,
              "result_r": alert.result_r, "followup_sent": False}

    # Follow-up only for alerts that were genuinely delivered — a shadow-mode
    # alert was never sent, so there is no thread to reply into.
    if (send_followup and settings is not None
            and getattr(settings, "delivery_enabled", False)
            and alert.delivery_status == "sent"
            and alert.followup_sent_at is None):
        token   = getattr(settings, "telegram_bot_token", None)
        chat_id = getattr(settings, "telegram_chat_id", None)
        if token and chat_id:
            try:
                sent = send_telegram_message(
                    token, chat_id, _followup_message(alert, res),
                    reply_to_message_id=alert.telegram_message_id or None)
                if sent.get("ok"):
                    alert.followup_sent_at = now
                    _db.session.commit()
                    result["followup_sent"] = True
            except Exception as exc:
                _db.session.rollback()
                print(f"[SIG-7] follow-up failed alert={alert.id}: {str(exc)[:80]}")

    return result


def track_open_alerts(user_id: int, now=None, limit: int = 200) -> dict:
    """Resolve every still-open alert for one user."""
    from models import LiveMonitorSignalAlert as _A
    from live_monitor.signal_settings import get_or_create_signal_settings

    now = now or datetime.now(timezone.utc)
    stats = {"checked": 0, "target_hit": 0, "stop_hit": 0,
             "expired": 0, "still_open": 0, "followups": 0, "errors": 0}

    try:
        settings = get_or_create_signal_settings(user_id)
    except Exception:
        settings = None

    try:
        rows = (_A.query
                .filter(_A.user_id == user_id, _A.outcome == "pending")
                .order_by(_A.created_at.asc())
                .limit(limit).all())
    except Exception as exc:
        print(f"[SIG-7] open-alert query failed: {str(exc)[:100]}")
        return {**stats, "errors": 1}

    for a in rows:
        stats["checked"] += 1
        try:
            res = resolve_alert(a, settings=settings, now=now)
        except Exception as exc:
            stats["errors"] += 1
            print(f"[SIG-7] resolve error alert={a.id}: {str(exc)[:80]}")
            continue

        if not res.get("ok"):
            stats["errors"] += 1
            continue
        oc = res.get("outcome")
        if oc in ("target_hit", "stop_hit", "expired"):
            stats[oc] += 1
        else:
            stats["still_open"] += 1
        if res.get("followup_sent"):
            stats["followups"] += 1

    return stats


def track_open_alerts_for_all(now=None) -> dict:
    """Resolve open alerts for every user who has any."""
    from models import LiveMonitorSignalAlert as _A

    out = {"users": 0, "results": {}}
    try:
        uids = [r[0] for r in _A.query
                .with_entities(_A.user_id)
                .filter(_A.outcome == "pending")
                .distinct().all()]
    except Exception as exc:
        print(f"[SIG-7] user scan failed: {str(exc)[:100]}")
        return out

    for uid in uids:
        try:
            out["results"][uid] = track_open_alerts(uid, now=now)
            out["users"] += 1
        except Exception as exc:
            print(f"[SIG-7] tracking failed user={uid}: {str(exc)[:100]}")
    return out


def module_accuracy(user_id: int, days: int = 90, now=None) -> dict:
    """Measured hit rate per scan module — the number that replaces guesswork.

    A module's stats count every resolved alert it contributed to, so a signal
    from three modules counts once for each. Modules below a small sample
    threshold are reported but flagged, because a 100% rate from two alerts
    means nothing.
    """
    from models import LiveMonitorSignalAlert as _A
    from sqlalchemy.orm import load_only as _load_only

    now = now or datetime.now(timezone.utc)
    since = now - timedelta(days=max(1, int(days)))

    try:
        rows = (_A.query
                .options(_load_only(_A.modules_json, _A.outcome, _A.result_r,
                                    _A.tier, _A.confluence_count))
                .filter(_A.user_id == user_id, _A.created_at >= since,
                        _A.outcome.in_(("target_hit", "stop_hit")))
                .limit(5000).all())
    except Exception as exc:
        return {"ok": False, "error": str(exc)[:100]}

    per_module: dict = {}
    per_tier: dict   = {}
    per_conf: dict   = {}

    for r in rows:
        won = r.outcome == "target_hit"
        rr  = float(r.result_r) if r.result_r is not None else 0.0

        try:
            mods = json.loads(r.modules_json or "[]")
        except (ValueError, TypeError):
            mods = []
        for m in (mods if isinstance(mods, list) else []):
            b = per_module.setdefault(m, {"wins": 0, "losses": 0, "r_sum": 0.0})
            b["wins" if won else "losses"] += 1
            b["r_sum"] += rr

        tb = per_tier.setdefault(r.tier or "?", {"wins": 0, "losses": 0, "r_sum": 0.0})
        tb["wins" if won else "losses"] += 1
        tb["r_sum"] += rr

        key = str(r.confluence_count or 1)
        cb = per_conf.setdefault(key, {"wins": 0, "losses": 0, "r_sum": 0.0})
        cb["wins" if won else "losses"] += 1
        cb["r_sum"] += rr

    def _finish(bucket: dict) -> dict:
        out = {}
        for k, v in bucket.items():
            n = v["wins"] + v["losses"]
            out[k] = {
                "resolved":  n,
                "wins":      v["wins"],
                "losses":    v["losses"],
                "win_rate":  round(v["wins"] * 100.0 / n, 1) if n else None,
                "total_r":   round(v["r_sum"], 2),
                "avg_r":     round(v["r_sum"] / n, 2) if n else None,
                # Below this, the percentage is noise dressed up as a fact.
                "reliable":  n >= 20,
            }
        return dict(sorted(out.items(),
                           key=lambda kv: kv[1]["resolved"], reverse=True))

    return {
        "ok": True, "days": days, "resolved_total": len(rows),
        "per_module":     _finish(per_module),
        "per_tier":       _finish(per_tier),
        "per_confluence": _finish(per_conf),
    }
