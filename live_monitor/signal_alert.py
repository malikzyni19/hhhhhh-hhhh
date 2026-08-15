"""Phase SIG-6: Alerts — the gate, the message, and the send.

The last stage. Everything upstream decides whether a setup is good; this
decides whether it should actually interrupt someone, and then writes the
message.

The gate exists because alert fatigue is what kills signal systems. A setup
can be excellent and still not worth sending — because the same pair alerted
an hour ago, or because today's budget is spent. Those checks run BEFORE the
AI is asked, since a suppressed alert should never cost an AI call.

The record is written before delivery is attempted, so a Telegram outage
leaves a record of what should have gone out instead of losing it.

No order placement anywhere. This module sends messages.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone, timedelta

import main as _m
from live_monitor.telegram_notify import escape_html, send_telegram_message


# Tier A is the rare one: several scans agreeing AND the AI confident.
TIER_A_MIN_MODULES    = 3
TIER_A_MIN_CONFIDENCE = 75

_TF_MS = {
    "1m": 60_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000,
    "1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000,
}


def build_alert_key(user_id: int, symbol: str, direction: str,
                    timeframe: str, now: datetime) -> str:
    """One alert per user, per setup, per candle.

    Folding the candle bucket in means a setup that stays armed across a whole
    4h candle produces one alert, not one per evaluation cycle.
    """
    span = _TF_MS.get((timeframe or "").lower(), 3_600_000)
    bucket = int(now.timestamp() * 1000) // span * span
    raw = f"{user_id}|{symbol}|{direction}|{timeframe}|{bucket}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:40]


def assign_tier(module_count: int, confidence: int) -> str:
    """A for the rare high-conviction ones, B for everything else sent."""
    if (module_count or 0) >= TIER_A_MIN_MODULES and \
       (confidence or 0) >= TIER_A_MIN_CONFIDENCE:
        return "A"
    return "B"


def check_gate(user_id: int, group: dict, settings, now=None) -> dict:
    """Cheap pre-AI checks: have we already sent this, and may we send at all?

    Returns {allowed, reason, alert_key, remaining_today}. Runs before the AI
    so a suppressed setup never costs a call.
    """
    from models import LiveMonitorSignalAlert as _A

    now = now or datetime.now(timezone.utc)
    symbol    = group.get("symbol")
    direction = group.get("direction")
    tf        = (group.get("timeframes") or ["4h"])[0] if group.get("timeframes") else "4h"

    key = build_alert_key(user_id, symbol, direction, tf, now)
    out = {"allowed": False, "alert_key": key, "reason": None,
           "remaining_today": None}

    try:
        # Already alerted for this exact setup in this candle.
        if _A.query.filter_by(alert_key=key).first() is not None:
            out["reason"] = "already_alerted_this_candle"
            return out

        # Daily budget. Counts everything recorded today, including shadow
        # rows, so switching delivery on does not suddenly release a backlog.
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        sent_today = (_A.query
                      .filter(_A.user_id == user_id, _A.created_at >= day_start)
                      .count())
        cap = int(getattr(settings, "max_signals_per_day", 6) or 6)
        out["remaining_today"] = max(0, cap - sent_today)
        if sent_today >= cap:
            out["reason"] = f"daily_cap_reached_{sent_today}/{cap}"
            return out

        # Per-pair cooldown, so one busy coin cannot dominate the day.
        cooldown_min = int(getattr(settings, "per_pair_cooldown_min", 240) or 240)
        since = now - timedelta(minutes=cooldown_min)
        recent = (_A.query
                  .filter(_A.user_id == user_id, _A.symbol == symbol,
                          _A.created_at >= since)
                  .first())
        if recent is not None:
            out["reason"] = f"pair_cooldown_{cooldown_min}min"
            return out

    except Exception as exc:
        out["reason"] = f"gate_error:{str(exc)[:80]}"
        return out

    out["allowed"] = True
    return out


def _fmt_price(val) -> str:
    """Prices span 0.00001 to 100000 — pick a sane precision for each."""
    if val is None:
        return "—"
    try:
        v = float(val)
    except (TypeError, ValueError):
        return "—"
    if v >= 1000:   return f"{v:,.2f}"
    if v >= 1:      return f"{v:,.4f}".rstrip("0").rstrip(".")
    return f"{v:.8f}".rstrip("0").rstrip(".")


_MODULE_LABELS = {
    "ob": "Order Block", "bb": "Breaker Block", "fvg": "Fair Value Gap",
    "fib_confluence": "FIB Confluence", "ath_atl": "ATH / ATL",
    "accumulation": "Accumulation", "compressed": "Compressed",
    "bias_shift": "Bias Shift", "liquidity_sweep": "Liquidity Sweep",
}


def format_alert_message(group: dict, evaluation: dict, verdict: dict,
                         tier: str) -> str:
    """The message itself — written for a tired human, not a log file.

    HTML mode: every interpolated value goes through escape_html, because
    symbols and AI text can contain characters that would break the markup.
    """
    direction = (group.get("direction") or "").upper()
    symbol    = escape_html(group.get("symbol"))
    arrow     = "🟢" if direction == "LONG" else "🔴"
    tier_mark = "🅰️" if tier == "A" else "🅱️"

    lines = [
        f"{tier_mark} {arrow} <b>{symbol} · {direction}</b>   "
        f"confidence {escape_html(verdict.get('confidence'))}",
        "",
    ]

    mods = group.get("matched_modules") or group.get("modules") or []
    if mods:
        lines.append(f"<b>Why</b>  {len(mods)} scan"
                     f"{'s agree' if len(mods) > 1 else ''}")
        for m in mods[:6]:
            lines.append(f"   • {escape_html(_MODULE_LABELS.get(m, m))}")
        lines.append("")

    ev = verdict.get("key_evidence") or []
    if ev:
        lines.append("<b>Order flow</b>")
        for e in ev[:4]:
            lines.append(f"   {escape_html(e)}")
        lines.append("")

    lines.append("<b>Levels</b>")
    lines.append(f"   Entry    {escape_html(_fmt_price(verdict.get('entry')))}")
    lines.append(f"   Stop     {escape_html(_fmt_price(verdict.get('stop_loss')))}")
    tp1 = verdict.get("take_profit_1")
    tp2 = verdict.get("take_profit_2")
    targets = _fmt_price(tp1) + (f" / {_fmt_price(tp2)}" if tp2 else "")
    lines.append(f"   Targets  {escape_html(targets)}")
    if verdict.get("risk_reward"):
        lines.append(f"   R:R      {escape_html(verdict['risk_reward'])}")
    lines.append("")

    summary = (verdict.get("summary") or "").strip()
    if summary:
        lines.append(escape_html(summary))
        lines.append("")

    kind = evaluation.get("alert_kind")
    kind_txt = "price in zone" if kind == "in_zone" else "price approaching zone"
    tfs = ", ".join(group.get("timeframes") or []) or "—"
    lines.append(f"<i>{escape_html(kind_txt)} · {escape_html(tfs)}</i>")

    return "\n".join(lines)


def record_and_send(user_id: int, group: dict, evaluation: dict,
                    verdict: dict, settings, gate: dict, now=None) -> dict:
    """Write the alert, then attempt delivery.

    The record comes first and is committed before any network call, so a
    Telegram failure leaves a durable record rather than losing the alert.
    """
    from models import db as _db, LiveMonitorSignalAlert as _A

    now = now or datetime.now(timezone.utc)
    tier = assign_tier(group.get("module_count"), verdict.get("confidence"))
    delivery_on = bool(getattr(settings, "delivery_enabled", False))

    try:
        row = _A(
            user_id       = user_id,
            alert_key     = gate["alert_key"],
            symbol        = group.get("symbol"),
            exchange      = group.get("exchange") or "binance",
            direction     = group.get("direction"),
            timeframe     = (group.get("timeframes") or ["4h"])[0]
                            if group.get("timeframes") else "4h",
            alert_kind    = evaluation.get("alert_kind") or "in_zone",
            tier          = tier,
            modules_json  = json.dumps(group.get("matched_modules")
                                       or group.get("modules") or []),
            confluence_count = int(group.get("module_count") or 1),
            zone_high     = group.get("zone_high"),
            zone_low      = group.get("zone_low"),
            entry_price   = verdict.get("entry"),
            stop_loss     = verdict.get("stop_loss"),
            take_profit_1 = verdict.get("take_profit_1"),
            take_profit_2 = verdict.get("take_profit_2"),
            risk_reward   = verdict.get("risk_reward"),
            price_at_alert = evaluation.get("price"),
            ai_confidence = verdict.get("confidence"),
            ai_summary    = verdict.get("summary"),
            ai_model      = verdict.get("model"),
            evidence_json = json.dumps({
                "key_evidence": verdict.get("key_evidence") or [],
                "trigger_reasons": evaluation.get("reasons") or [],
                "strength": group.get("strength"),
            })[:4000],
            # "shadow" is a first-class outcome, not a failure: the funnel ran
            # and produced a real alert, the user just has sending switched off.
            delivery_status = "pending" if delivery_on else "shadow",
            created_at    = now,
        )
        _db.session.add(row)
        _db.session.commit()
    except Exception as exc:
        _db.session.rollback()
        return {"ok": False, "reason": f"record_failed:{str(exc)[:100]}"}

    if not delivery_on:
        return {"ok": True, "delivered": False, "status": "shadow",
                "tier": tier, "alert_id": row.id}

    token   = getattr(settings, "telegram_bot_token", None)
    chat_id = getattr(settings, "telegram_chat_id", None)
    if not (token and chat_id):
        try:
            row.delivery_status = "failed"
            row.delivery_error  = "telegram_not_linked"
            _db.session.commit()
        except Exception:
            _db.session.rollback()
        return {"ok": False, "delivered": False, "status": "failed",
                "reason": "telegram_not_linked", "alert_id": row.id}

    try:
        text = format_alert_message(group, evaluation, verdict, tier)
        sent = send_telegram_message(token, chat_id, text)
    except Exception as exc:
        sent = {"ok": False, "message": f"send_crashed:{str(exc)[:80]}"}

    try:
        if sent.get("ok"):
            row.delivery_status     = "sent"
            row.telegram_message_id = str(sent.get("message_id") or "")[:40]
            row.sent_at             = now
            row.delivery_error      = None
        else:
            row.delivery_status = "failed"
            row.delivery_error  = str(sent.get("message") or "")[:255]
        _db.session.commit()
    except Exception:
        _db.session.rollback()

    return {"ok": bool(sent.get("ok")), "delivered": bool(sent.get("ok")),
            "status": row.delivery_status, "tier": tier, "alert_id": row.id,
            "reason": None if sent.get("ok") else sent.get("message")}


def run_alert_cycle(user_id: int, now=None, agent_id: str = None) -> dict:
    """One full pass: rank, arm, gate, judge, send.

    Ordered cheapest-first throughout — the AI is only asked about setups that
    are armed AND would actually be allowed through the gate.
    """
    from live_monitor.signal_settings import get_or_create_signal_settings
    from live_monitor.signal_confluence import (build_confluence_groups,
                                                filter_groups_for_settings)
    from live_monitor.signal_trigger import evaluate_groups
    from live_monitor.signal_verdict import judge_setup

    now = now or datetime.now(timezone.utc)
    stats = {"ok": True, "armed": 0, "gated": 0, "judged": 0,
             "sent": 0, "shadow": 0, "failed": 0, "held": 0,
             "suppressed": [], "errors": []}

    try:
        settings = get_or_create_signal_settings(user_id)
    except Exception as exc:
        return {"ok": False, "reason": f"settings_error:{str(exc)[:80]}"}

    if not settings or not settings.enabled:
        return {"ok": True, "skipped": "engine_off"}

    try:
        groups  = build_confluence_groups(window_hours=12, now=now)
        matched = filter_groups_for_settings(groups, settings)
        evaluated = evaluate_groups(matched, settings, now=now)
    except Exception as exc:
        return {"ok": False, "reason": f"evaluate_error:{str(exc)[:80]}"}

    stats["armed"] = len(evaluated["armed"])
    min_conf = int(getattr(settings, "min_confidence", 65) or 65)

    for entry in evaluated["armed"]:
        group, evaluation = entry["group"], entry["evaluation"]
        symbol = group.get("symbol")

        gate = check_gate(user_id, group, settings, now=now)
        if not gate["allowed"]:
            stats["gated"] += 1
            stats["suppressed"].append({"symbol": symbol, "reason": gate["reason"]})
            # A spent daily budget applies to everything left this cycle.
            if str(gate.get("reason") or "").startswith("daily_cap_reached"):
                break
            continue

        try:
            verdict = judge_setup(group, evaluation, settings, agent_id=agent_id)
        except Exception as exc:
            stats["errors"].append({"symbol": symbol, "error": str(exc)[:100]})
            continue

        stats["judged"] += 1

        if not verdict.get("ok"):
            stats["errors"].append({"symbol": symbol,
                                    "error": verdict.get("reason")})
            continue

        if verdict.get("verdict") != "send":
            stats["held"] += 1
            stats["suppressed"].append({"symbol": symbol,
                                        "reason": f"ai_{verdict.get('verdict')}"})
            continue

        if int(verdict.get("confidence") or 0) < min_conf:
            stats["held"] += 1
            stats["suppressed"].append({
                "symbol": symbol,
                "reason": f"below_confidence_floor_{verdict.get('confidence')}<{min_conf}"})
            continue

        res = record_and_send(user_id, group, evaluation, verdict,
                              settings, gate, now=now)
        if res.get("status") == "sent":
            stats["sent"] += 1
        elif res.get("status") == "shadow":
            stats["shadow"] += 1
        else:
            stats["failed"] += 1
            stats["errors"].append({"symbol": symbol, "error": res.get("reason")})

    return stats


def run_alert_cycle_for_all_enabled(now=None) -> dict:
    """One alert pass for every user who has the engine switched on."""
    from models import LiveMonitorSignalSettings as _S
    from sqlalchemy.orm import load_only as _load_only

    out = {"users": 0, "results": {}}
    try:
        rows = (_S.query.options(_load_only(_S.user_id, _S.enabled))
                .filter(_S.enabled.is_(True)).all())
    except Exception as exc:
        print(f"[SIG-6] enabled-user query failed: {str(exc)[:120]}")
        return out

    for r in rows:
        try:
            out["results"][r.user_id] = run_alert_cycle(r.user_id, now=now)
            out["users"] += 1
        except Exception as exc:
            print(f"[SIG-6] alert cycle failed user={r.user_id}: {str(exc)[:120]}")
    return out
