"""Phase SIG-5: Verdict — the AI's final read on an armed setup.

Runs last in the funnel, and only on setups that already passed every cheap
deterministic check, because this is the one stage that costs money and time.

The AI is asked a narrow question: given this evidence, is this worth waking
the user for, and if so at what levels? It is not asked to find setups, pick
coins, or decide anything about execution — there is no execution.

Its answer is validated hard. A verdict with levels that contradict its own
direction is rejected rather than repaired, because a quietly "fixed" stop
loss is worse than no signal at all.
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone

import main as _m


VALID_VERDICTS = ("send", "hold", "discard")

# Bounds on what the AI may claim, so one bad response cannot produce a
# nonsensical alert.
MIN_RR = 0.5
MAX_RR = 20.0
MAX_SUMMARY_CHARS = 400


SYSTEM_PROMPT = """You judge trading setups that have already been found and \
filtered by a deterministic system. You do NOT find setups and you do NOT \
execute anything — no orders exist anywhere in this system.

Your only job: decide whether this setup is worth sending as an alert to a \
trader's phone, and if so, at what levels.

Answer with STRICT JSON only, no prose, no markdown fences:
{
  "verdict": "send" | "hold" | "discard",
  "confidence": 0-100,
  "entry": number,
  "stop_loss": number,
  "take_profit_1": number,
  "take_profit_2": number or null,
  "summary": "one or two short sentences a tired human can read at 3am",
  "key_evidence": ["short phrase", "short phrase"]
}

Rules you must follow:
- For a LONG: stop_loss MUST be below entry, and both take profits above it.
- For a SHORT: stop_loss MUST be above entry, and both take profits below it.
- Keep entry near the current price or the stated zone. Do not invent a level \
far from the market.
- "send" only if the order-flow evidence genuinely supports the direction. \
Agreement between scans is not by itself a reason to send.
- "hold" if the setup is real but not ready yet.
- "discard" if the evidence contradicts the direction.
- Never claim certainty. Never mention placing orders."""


def _f(val, default=None):
    try:
        out = float(val)
    except (TypeError, ValueError):
        return default
    if out != out or out in (float("inf"), float("-inf")):
        return default
    return out


def build_verdict_context(group: dict, evaluation: dict, settings=None) -> dict:
    """Assemble the evidence the AI judges.

    Reuses the order-flow context the Live Monitor already builds, so the AI
    sees exactly what the Data Health panel shows — CVD, divergences, OI
    regime, spot-vs-perp agreement — rather than a separate, thinner view.
    """
    symbol   = group.get("symbol")
    exchange = group.get("exchange") or "binance"

    ctx = {
        "task": "judge_setup_for_alert",
        "setup": {
            "symbol":        symbol,
            "direction":     group.get("direction"),
            "scans_agreeing": group.get("matched_modules") or group.get("modules") or [],
            "scan_count":    group.get("module_count"),
            "confluence_strength": group.get("strength"),
            "avg_scan_score": group.get("avg_module_score"),
            "zone_high":     group.get("zone_high"),
            "zone_low":      group.get("zone_low"),
            "zone_agreement": group.get("zone_agreement"),
            "timeframes":    group.get("timeframes"),
        },
        "current_price": evaluation.get("price"),
        "trigger": {
            "state":      evaluation.get("state"),
            "alert_kind": evaluation.get("alert_kind"),
            "reasons":    evaluation.get("reasons"),
        },
        "rules": {
            "no_execution":      True,
            "no_order_placement": True,
            "alert_only":        True,
        },
    }

    # Order flow — the evidence that decides this.
    try:
        raw = _m._lm_fetch_flow1m_raw(symbol, 300)
        series = _m._lm_aggregate_flow_candles(raw or [], "5m", 60)
        if series:
            last = series[-1]
            ctx["order_flow"] = {
                "timeframe":   "5m",
                "candle_count": len(series),
                "cvd_now":     last.get("cvd_usd"),
                "oi_now":      last.get("oi_close"),
                "recent": [
                    {"t": c.get("t"), "price": c.get("price_close"),
                     "delta": c.get("delta_usd"), "cvd": c.get("cvd_usd"),
                     "oi": c.get("oi_close")}
                    for c in series[-12:]
                ],
            }
            if len(series) >= 10:
                divs = _m._lm_detect_metric_divergence(series, "cvd_usd")
                ctx["order_flow"]["cvd_divergences"] = [
                    {"kind": d["kind"], "strength": d["strength"]} for d in divs[:3]
                ]
            ctx["order_flow"]["oi_regime"] = _m._lm_classify_oi_regime(series, lookback=20)
    except Exception as exc:
        ctx["order_flow"] = {"error": str(exc)[:100]}

    # Data health — never judge on stale feeds.
    try:
        dh = _m._lm_build_data_health_context(symbol, exchange)
        ctx["data_health"] = {
            "critical_status": dh.get("critical_status"),
            "gate_allowed":    (dh.get("ai_data_gate") or {}).get("allowed"),
            "warnings":        ((dh.get("ai_data_gate") or {}).get("warnings") or [])[:4],
        }
        ctx["spot_vs_perp"] = ((dh.get("spot_flow") or {}).get("cross_check")
                               if dh.get("spot_flow") else None)
    except Exception as exc:
        ctx["data_health"] = {"error": str(exc)[:100]}

    return ctx


def _extract_json(text: str):
    """Pull the JSON object out of a model response.

    Tolerates code fences and leading prose, but does not attempt to repair
    malformed JSON — a response we cannot read cleanly is rejected.
    """
    if not text:
        return None
    if isinstance(text, dict):
        return text

    s = str(text).strip()
    fence = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", s, re.DOTALL)
    if fence:
        s = fence.group(1)
    else:
        start, end = s.find("{"), s.rfind("}")
        if start == -1 or end <= start:
            return None
        s = s[start:end + 1]

    try:
        out = json.loads(s)
        return out if isinstance(out, dict) else None
    except ValueError:
        return None


def validate_verdict(raw: dict, direction: str, price=None) -> tuple:
    """Check the AI's answer is internally consistent. Returns (ok, verdict, errors).

    Levels that contradict the direction are a hard reject. Repairing them
    would mean inventing a stop loss the model never chose, and shipping that
    to someone's phone as if the model had reasoned about it.
    """
    errors = []
    if not isinstance(raw, dict):
        return False, None, ["not_an_object"]

    verdict = str(raw.get("verdict") or "").strip().lower()
    if verdict not in VALID_VERDICTS:
        return False, None, [f"invalid_verdict:{verdict[:20]}"]

    conf = _f(raw.get("confidence"), None)
    if conf is None:
        errors.append("missing_confidence")
        conf = 0
    conf = int(max(0, min(100, conf)))

    summary = str(raw.get("summary") or "").strip()[:MAX_SUMMARY_CHARS]

    evidence = raw.get("key_evidence")
    if not isinstance(evidence, list):
        evidence = []
    evidence = [str(e).strip()[:120] for e in evidence[:5] if str(e).strip()]

    out = {
        "verdict":      verdict,
        "confidence":   conf,
        "summary":      summary,
        "key_evidence": evidence,
        "entry":        None, "stop_loss": None,
        "take_profit_1": None, "take_profit_2": None,
        "risk_reward":  None,
    }

    # Levels are only required for an alert we are actually going to send.
    if verdict != "send":
        return (len(errors) == 0), out, errors

    entry = _f(raw.get("entry"))
    stop  = _f(raw.get("stop_loss"))
    tp1   = _f(raw.get("take_profit_1"))
    tp2   = _f(raw.get("take_profit_2"))

    for name, val in (("entry", entry), ("stop_loss", stop), ("take_profit_1", tp1)):
        if val is None or val <= 0:
            errors.append(f"missing_or_invalid_{name}")
    if errors:
        return False, None, errors

    if direction == "long":
        if stop >= entry:
            errors.append("long_stop_not_below_entry")
        if tp1 <= entry:
            errors.append("long_target_not_above_entry")
        if tp2 is not None and tp2 <= tp1:
            tp2 = None          # a second target that isn't further out is noise
    elif direction == "short":
        if stop <= entry:
            errors.append("short_stop_not_above_entry")
        if tp1 >= entry:
            errors.append("short_target_not_below_entry")
        if tp2 is not None and tp2 >= tp1:
            tp2 = None
    else:
        errors.append("no_direction_to_validate_against")

    if errors:
        return False, None, errors

    risk   = abs(entry - stop)
    reward = abs(tp1 - entry)
    if risk <= 0:
        return False, None, ["zero_risk_distance"]
    rr = reward / risk
    if rr < MIN_RR or rr > MAX_RR:
        return False, None, [f"implausible_risk_reward:{rr:.2f}"]

    # An entry nowhere near the market is a model hallucinating a level.
    if price:
        drift = abs(entry - price) / price * 100.0
        if drift > 15.0:
            return False, None, [f"entry_far_from_price:{drift:.1f}pct"]

    out.update({"entry": entry, "stop_loss": stop,
                "take_profit_1": tp1, "take_profit_2": tp2,
                "risk_reward": round(rr, 2)})
    return True, out, []


def judge_setup(group: dict, evaluation: dict, settings=None,
                agent_id: str = None) -> dict:
    """Ask the AI whether this armed setup is worth sending.

    Returns {ok, verdict, confidence, entry, stop_loss, ...} on success, or
    {ok: False, reason, ...} on any failure. Never raises — a verdict failure
    must leave the funnel running for every other setup.
    """
    direction = group.get("direction")
    context   = build_verdict_context(group, evaluation, settings)

    try:
        result = _m._lm_call_ai_provider(context, SYSTEM_PROMPT, agent_id=agent_id)
    except Exception as exc:
        return {"ok": False, "reason": f"ai_call_failed:{str(exc)[:80]}",
                "context": context}

    if not result or not result.get("ok"):
        # An unconfigured or failing provider must not silently become a
        # "hold" — the caller needs to know no judgement happened.
        return {"ok": False,
                "reason": "ai_unavailable",
                "detail": str(result.get("error") if result else "")[:120],
                "model": (result or {}).get("model"),
                "context": context}

    parsed = _extract_json(result.get("analysis"))
    if parsed is None:
        return {"ok": False, "reason": "ai_response_unparseable",
                "raw": str(result.get("analysis"))[:200],
                "model": result.get("model")}

    ok, verdict, errors = validate_verdict(parsed, direction,
                                           price=evaluation.get("price"))
    if not ok:
        return {"ok": False, "reason": "ai_response_rejected",
                "errors": errors, "model": result.get("model")}

    verdict.update({
        "ok":       True,
        "model":    result.get("model"),
        "agent":    result.get("agent_label"),
        "judged_at": datetime.now(timezone.utc).isoformat(),
        "context":  context,
    })
    return verdict
