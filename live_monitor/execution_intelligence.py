"""Phase 11.1: Execution Intelligence helpers.

Moved from main.py (Phase 11.1C module split). Routes stay in main.py.
Evidence-only: no AI, no orders, no Entry Candidate, no trading.
"""
from __future__ import annotations
import time

# Deferred import — avoids circular dependency.
# main is partially loaded when this module is first imported, but by the
# time any function here is *called* (at request time) main is fully loaded.
import main as _m

# Cross-module helpers from sibling packages (no circular issue)
from live_monitor.mtf_orderflow import (
    _lm_build_mtf_orderflow_history,
    _lm_build_mtf_history_summary,
)
from live_monitor.smc_orderflow_fusion import _lm_build_smc_orderflow_fusion


# ── Phase 11.1 helpers ────────────────────────────────────────────────────────

def _lm_build_orderflow_state(current_val, previous_val) -> dict:  # noqa: C901
    """Build a structured trend state from two scalar values (Phase 11.1 Task 6).

    Returns direction, velocity, acceleration, and negative-but-improving flags.
    """
    c = float(current_val  if current_val  is not None else 0)
    p = float(previous_val if previous_val is not None else 0)
    change_abs = c - p
    change_pct = round(change_abs / abs(p) * 100, 2) if abs(p) > 1e-12 else 0.0
    abs_chg    = abs(change_pct)

    if abs_chg < 0.5:
        direction = "flat"
        velocity  = "slow"
    else:
        direction = "rising" if change_abs > 0 else "falling"
        velocity  = "fast" if abs_chg >= 25 else ("normal" if abs_chg >= 8 else "slow")

    still_negative        = c < 0
    but_improving         = still_negative and change_abs > 0
    negative_accelerating = still_negative and change_abs < 0

    if direction == "flat":
        state = "neutral"
    elif c >= 0 and direction == "rising":
        state = "positive_strengthening"
    elif c >= 0 and direction == "falling":
        state = "positive_weakening"
    elif c < 0 and direction == "rising":
        state = "negative_weakening"   # improving — less negative
    else:
        state = "negative_strengthening"  # getting worse

    acceleration = (
        "strengthening" if velocity == "fast" else
        "weakening"     if velocity == "slow" and direction != "flat" else
        "neutral"
    )

    return {
        "current_value":          round(c, 6),
        "previous_value":         round(p, 6),
        "change_abs":             round(change_abs, 6),
        "change_pct":             change_pct,
        "direction":              direction,
        "velocity":               velocity,
        "acceleration":           acceleration,
        "state":                  state,
        "still_negative":         still_negative,
        "but_improving":          but_improving,
        "negative_accelerating":  negative_accelerating,
    }


def _lm_build_zone_ob_slice(zone_low: float, zone_high: float,
                             symbol: str, exchange: str,
                             analysis_source: str,
                             margin_pct: float = 0.5) -> dict:
    """Return bid/ask depth slice around a zone from the raw orderbook (Phase 11.1 Task 3).

    Always uses _ob_books (Binance full-depth stream) — same as
    _lm_score_orderbook_wall_for_zone. Non-Binance sources fall back to
    the Binance OB stream; no raw depth is available from other exchanges.

    Returns {available, bid_depth, ask_depth, bid_total, ask_total,
             imbalance_ratio, imbalance_side, zone_vol_pct_bids,
             zone_vol_pct_asks, status, notes}.
    """
    _unavail = {
        "available":         False,
        "bid_depth":         [],
        "ask_depth":         [],
        "bid_total":         0.0,
        "ask_total":         0.0,
        "imbalance_ratio":   1.0,
        "imbalance_side":    "neutral",
        "zone_vol_pct_bids": 0.0,
        "zone_vol_pct_asks": 0.0,
        "status":            "unavailable",
        "notes":             "",
    }
    if not symbol or not zone_low or not zone_high or zone_high <= zone_low:
        return {**_unavail, "notes": "invalid zone bounds"}
    try:
        _m.ensure_ob_stream(symbol, wait_sec=0.1)
    except Exception:
        pass
    with _m._ob_book_lock:
        book = dict(_m._ob_books.get(symbol) or {})
    if not book or not book.get("ready"):
        return {**_unavail, "notes": "OB stream not ready"}

    ob_age    = time.time() - book.get("ts", time.time())
    ob_status = "fresh" if ob_age <= 5 else "stale"

    lo = zone_low  * (1.0 - margin_pct / 100.0)
    hi = zone_high * (1.0 + margin_pct / 100.0)

    raw_bids = book.get("bids") or {}
    raw_asks = book.get("asks") or {}

    bid_levels = sorted(
        [(float(p), float(q)) for p, q in raw_bids.items() if lo <= float(p) <= hi],
        reverse=True
    )
    ask_levels = sorted(
        [(float(p), float(q)) for p, q in raw_asks.items() if lo <= float(p) <= hi]
    )

    bid_total = sum(q for _, q in bid_levels)
    ask_total = sum(q for _, q in ask_levels)

    all_bid_vol = sum(float(q) for q in raw_bids.values()) or 1.0
    all_ask_vol = sum(float(q) for q in raw_asks.values()) or 1.0
    zone_vol_pct_bids = round(bid_total / all_bid_vol * 100, 2)
    zone_vol_pct_asks = round(ask_total / all_ask_vol * 100, 2)

    if bid_total + ask_total > 0:
        imbalance_ratio = round((bid_total + 1e-12) / (ask_total + 1e-12), 3)
        if imbalance_ratio >= 1.5:
            imbalance_side = "bid_heavy"
        elif imbalance_ratio <= 0.67:
            imbalance_side = "ask_heavy"
        else:
            imbalance_side = "neutral"
    else:
        imbalance_ratio = 1.0
        imbalance_side  = "neutral"

    exch_note = ""
    if exchange and exchange not in ("binance", "aggregated", ""):
        exch_note = f" [Binance OB fallback — {exchange} raw depth not available]"

    return {
        "available":         True,
        "bid_depth":         [(round(p, 8), round(q, 6)) for p, q in bid_levels[:20]],
        "ask_depth":         [(round(p, 8), round(q, 6)) for p, q in ask_levels[:20]],
        "bid_total":         round(bid_total, 4),
        "ask_total":         round(ask_total, 4),
        "imbalance_ratio":   imbalance_ratio,
        "imbalance_side":    imbalance_side,
        "zone_vol_pct_bids": zone_vol_pct_bids,
        "zone_vol_pct_asks": zone_vol_pct_asks,
        "status":            ob_status,
        "notes":             f"Binance full-depth slice ({ob_status}){exch_note}",
    }


def _lm_build_candidate_orderflow(symbol: str, exchange: str, market: str,  # noqa: C901
                                   analysis_source: str, snap_raw: dict,
                                   direction: str) -> dict:
    """Assemble live orderflow for a candidate: delta, CVD proxy, OI, liq,
    funding, long/short (Phase 11.1 Task 4).

    Previous values are read from snap_raw['latest_execution_intelligence']
    to enable trend state comparison across refresh cycles.
    """
    norm_src = _m._lm_normalize_analysis_source(analysis_source)
    eff_exch = norm_src if norm_src in _m._LM_SPECIFIC_ANALYSIS_SRCS else "binance"

    prev_ei  = snap_raw.get("latest_execution_intelligence") or {}
    prev_of  = prev_ei.get("orderflow") or {}

    # ── Delta (aggTrade / trade stream) ───────────────────────────────────────
    delta_d = None
    if eff_exch == "binance":
        d, _ = _m._lm_delta_get("binance", symbol)
        delta_d = d
    else:
        d, _ = _m._lm_mx_get_delta(eff_exch, symbol)
        delta_d = d
        if not delta_d:  # binance fallback
            d2, _ = _m._lm_delta_get("binance", symbol)
            delta_d = d2

    delta_pct_5m  = float((delta_d or {}).get("delta_pct_5m",  0))
    delta_pct_60s = float((delta_d or {}).get("delta_pct_60s", 0))
    buy_vol_5m    = float((delta_d or {}).get("buy_vol_5m",    0))
    sell_vol_5m   = float((delta_d or {}).get("sell_vol_5m",   0))
    prev_delta    = float((prev_of.get("delta") or {}).get("current_value", 0))
    delta_state   = _lm_build_orderflow_state(delta_pct_5m, prev_delta)

    # ── CVD proxy (5m net delta as proxy — no streaming CVD available) ────────
    cvd_proxy_val = float((delta_d or {}).get("delta_5m", 0))
    prev_cvd      = float((prev_of.get("cvd_proxy") or {}).get("current_value", 0))
    cvd_state     = _lm_build_orderflow_state(cvd_proxy_val, prev_cvd)

    # ── Open Interest ─────────────────────────────────────────────────────────
    oi_d      = _m._lm_fetch_exchange_open_interest(eff_exch, symbol)
    oi_usd    = float((oi_d or {}).get("oi_usd") or 0)
    prev_oi   = float((prev_of.get("open_interest") or {}).get("current_value", 0))
    oi_state  = _lm_build_orderflow_state(oi_usd, prev_oi)

    # ── OI Change ─────────────────────────────────────────────────────────────
    oi_chg_d    = _m._lm_fetch_exchange_oi_change(eff_exch, symbol)
    oi_chg_pct  = float((oi_chg_d or {}).get("change_pct") or 0)
    prev_oic    = float((prev_of.get("oi_change") or {}).get("current_value", 0))
    oi_chg_state = _lm_build_orderflow_state(oi_chg_pct, prev_oic)

    # ── Liquidations (5m window, side-biased) ─────────────────────────────────
    liq_d = None
    if eff_exch == "binance":
        l, _ = _m._lm_liq_get("binance", symbol)
        liq_d = l
    else:
        l, _ = _m._lm_mx_get_liq(eff_exch, symbol)
        liq_d = l
        if not liq_d:
            l2, _ = _m._lm_liq_get("binance", symbol)
            liq_d = l2

    long_liq  = float((liq_d or {}).get("long_liq_usd_5m",  0))
    short_liq = float((liq_d or {}).get("short_liq_usd_5m", 0))
    # Positive = short liqs dominant (bullish pressure); negative = long liqs dominant
    liq_bias_val  = short_liq - long_liq
    prev_liq_bias = float((prev_of.get("liq_bias") or {}).get("current_value", 0))
    liq_state     = _lm_build_orderflow_state(liq_bias_val, prev_liq_bias)

    # ── Funding Rate ──────────────────────────────────────────────────────────
    fund_d    = _m._lm_fetch_exchange_funding(eff_exch, symbol)
    fund_rate = float((fund_d or {}).get("rate") or 0)
    fund_bias = (fund_d or {}).get("bias", "neutral")
    prev_fund = float((prev_of.get("funding") or {}).get("current_value", 0))
    fund_state = _lm_build_orderflow_state(fund_rate, prev_fund)

    # ── Long/Short Ratio ──────────────────────────────────────────────────────
    ls_d      = _m._lm_fetch_exchange_long_short(eff_exch, symbol)
    ls_ratio  = float((ls_d or {}).get("ls_ratio")   or 0)
    long_pct  = float((ls_d or {}).get("long_pct")   or 0)
    short_pct = float((ls_d or {}).get("short_pct")  or 0)
    prev_ls   = float((prev_of.get("long_short") or {}).get("current_value", 0))
    ls_state  = _lm_build_orderflow_state(ls_ratio, prev_ls)

    return {
        "exchange_used":   eff_exch,
        "analysis_source": norm_src,
        "delta": {
            **delta_state,
            "delta_pct_60s": delta_pct_60s,
            "buy_vol_5m":    round(buy_vol_5m,  2),
            "sell_vol_5m":   round(sell_vol_5m, 2),
            "available":     delta_d is not None,
        },
        "cvd_proxy": {
            **cvd_state,
            "note":      "5m net delta used as CVD proxy (no streaming CVD available)",
            "available": delta_d is not None,
        },
        "open_interest": {
            **oi_state,
            "oi_usd":       round(oi_usd, 2),
            "oi_contracts": float((oi_d or {}).get("oi_contracts") or 0),
            "available":    bool((oi_d or {}).get("available")),
        },
        "oi_change": {
            **oi_chg_state,
            "direction": (oi_chg_d or {}).get("direction", ""),
            "available": bool((oi_chg_d or {}).get("available")),
        },
        "liq_bias": {
            **liq_state,
            "long_liq_usd_5m":  round(long_liq,  2),
            "short_liq_usd_5m": round(short_liq, 2),
            "available":        liq_d is not None,
        },
        "funding": {
            **fund_state,
            "rate_pct":  round(fund_rate * 100, 6),
            "bias":      fund_bias,
            "available": bool((fund_d or {}).get("available")),
        },
        "long_short": {
            **ls_state,
            "long_pct":  round(long_pct,  2),
            "short_pct": round(short_pct, 2),
            "available": bool((ls_d or {}).get("available")),
        },
    }


def _lm_build_ltf_confirmation_context(candidates: list, symbol: str,
                                        exchange: str, market: str,
                                        analysis_source: str, direction: str,
                                        snap_raw: dict) -> dict:
    """Build LTF confirmation signals from MTF scan data (Phase 11.1 Task 5).

    Reads latest_mtf_scan.tfs from snapshot. Returns per-TF signal summary
    and an aggregate alignment verdict.
    """
    mtf_scan = snap_raw.get("latest_mtf_scan") or {}
    tfs_data  = mtf_scan.get("tfs") or {}
    cand_tfs  = list({c.get("tf") for c in (candidates or []) if c.get("tf")})

    tf_signals: dict = {}
    for tf, tf_d in tfs_data.items():
        if not isinstance(tf_d, dict):
            continue
        mods   = tf_d.get("modules") or {}
        score  = tf_d.get("score", 0)
        tf_dir = (tf_d.get("direction") or "").lower()

        bullish_mods: list = []
        bearish_mods: list = []
        for mod_name, mod_d in mods.items():
            if not isinstance(mod_d, dict):
                continue
            state = (mod_d.get("state") or "").lower()
            if any(k in state for k in ("bull", "support", "demand", "long")):
                bullish_mods.append(mod_name)
            elif any(k in state for k in ("bear", "resist", "supply", "short")):
                bearish_mods.append(mod_name)

        aligned = (
            (direction == "bullish" and tf_dir == "bullish") or
            (direction == "bearish" and tf_dir == "bearish")
        )
        tf_signals[tf] = {
            "score":        score,
            "direction":    tf_dir,
            "aligned":      aligned,
            "bullish_mods": bullish_mods,
            "bearish_mods": bearish_mods,
            "mod_count":    len(mods),
            "is_cand_tf":   tf in cand_tfs,
        }

    aligned_tfs    = [t for t, s in tf_signals.items() if s["aligned"]]
    misaligned_tfs = [t for t, s in tf_signals.items() if not s["aligned"] and s["direction"]]
    total_tfs      = len([t for t, s in tf_signals.items() if s["direction"]])
    align_score    = round(len(aligned_tfs) / total_tfs * 100, 1) if total_tfs > 0 else 0.0

    if total_tfs == 0:
        verdict = "insufficient_data"
    elif align_score >= 75:
        verdict = "confirmed"
    elif align_score >= 50:
        verdict = "partial"
    else:
        verdict = "conflicting"

    return {
        "tf_signals":      tf_signals,
        "aligned_tfs":     aligned_tfs,
        "misaligned_tfs":  misaligned_tfs,
        "alignment_score": align_score,
        "verdict":         verdict,
        "candidate_tfs":   cand_tfs,
        "scan_available":  bool(tfs_data),
    }


def _lm_score_candidate_intelligence(cand_intel: dict, parent_setup: dict,  # noqa: C901
                                      direction: str) -> dict:
    """Score a candidate intelligence dict 0-100 without AI (Phase 11.1 Task 7).

    Components (total 100):
      OB/FIB quality     25  (structural quality of the candidate zone)
      Orderbook support  20  (depth imbalance inside the zone)
      Delta state        15  (5m delta direction / velocity)
      CVD proxy state    10  (net signed delta trend)
      OI state           10  (open interest + OI change direction)
      Liquidations bias   5  (which side is being liquidated)
      Funding bias        5  (crowd positioning via funding rate)
      Parent alignment    5  (overlap_pct with parent zone)
      LTF confirmation    5  (MTF alignment score)
    """
    score   = 0
    factors: list = []

    # 1. OB/FIB quality (25 pts)
    quality = int(cand_intel.get("quality") or 0)
    if quality >= 85:
        score += 25; factors.append("elite_zone_quality")
    elif quality >= 70:
        score += 18; factors.append("high_zone_quality")
    elif quality >= 50:
        score += 10; factors.append("medium_zone_quality")
    else:
        score += 3;  factors.append("weak_zone_quality")

    # 2. Orderbook support (20 pts)
    ob_sl = cand_intel.get("ob_slice") or {}
    if ob_sl.get("available"):
        imb_side  = ob_sl.get("imbalance_side", "neutral")
        imb_ratio = float(ob_sl.get("imbalance_ratio") or 1.0)
        bid_pct   = float(ob_sl.get("zone_vol_pct_bids") or 0)
        ask_pct   = float(ob_sl.get("zone_vol_pct_asks") or 0)
        if direction == "bullish":
            if imb_side == "bid_heavy" and imb_ratio >= 2.0:
                score += 20; factors.append("strong_bid_depth_in_zone")
            elif imb_side == "bid_heavy":
                score += 14; factors.append("bid_depth_in_zone")
            elif bid_pct >= 3.0:
                score += 8;  factors.append("bid_depth_present")
        else:
            if imb_side == "ask_heavy" and imb_ratio <= 0.5:
                score += 20; factors.append("strong_ask_depth_in_zone")
            elif imb_side == "ask_heavy":
                score += 14; factors.append("ask_depth_in_zone")
            elif ask_pct >= 3.0:
                score += 8;  factors.append("ask_depth_present")

    # 3. Delta state (15 pts)
    of_d     = cand_intel.get("orderflow") or {}
    delta_of = of_d.get("delta") or {}
    d_state  = delta_of.get("state", "")
    if direction == "bullish":
        if d_state == "positive_strengthening":
            score += 15; factors.append("delta_bullish_strengthening")
        elif d_state in ("positive_weakening", "negative_weakening"):
            score += 9;  factors.append("delta_improving")
        elif d_state == "negative_strengthening":
            score += 2
        else:
            score += 5
    else:
        if d_state == "negative_strengthening":
            score += 15; factors.append("delta_bearish_strengthening")
        elif d_state in ("positive_weakening", "negative_weakening"):
            score += 9;  factors.append("delta_improving_bear")
        elif d_state == "positive_strengthening":
            score += 2
        else:
            score += 5

    # 4. CVD proxy state (10 pts)
    cvd_of  = of_d.get("cvd_proxy") or {}
    cvd_val = float(cvd_of.get("current_value") or 0)
    cvd_dir = cvd_of.get("direction", "")
    if direction == "bullish":
        if cvd_val > 0 and cvd_dir == "rising":
            score += 10; factors.append("cvd_positive_rising")
        elif cvd_val > 0:
            score += 6;  factors.append("cvd_positive")
        elif cvd_of.get("but_improving"):
            score += 4;  factors.append("cvd_negative_but_improving")
        else:
            score += 1
    else:
        if cvd_val < 0 and cvd_dir == "falling":
            score += 10; factors.append("cvd_negative_falling")
        elif cvd_val < 0:
            score += 6;  factors.append("cvd_negative")
        elif cvd_of.get("negative_accelerating"):
            score += 4;  factors.append("cvd_negative_accelerating")
        else:
            score += 1

    # 5. OI state (10 pts) — OI level + OI change direction
    oi_of  = of_d.get("open_interest") or {}
    oic_of = of_d.get("oi_change") or {}
    oi_dir = oi_of.get("direction", "")
    oic_dir = oic_of.get("direction", "")
    if oi_dir == "rising" and oic_dir == "rising":
        score += 10; factors.append("oi_rising_strongly")
    elif oi_dir == "rising" or oic_dir == "rising":
        score += 6;  factors.append("oi_rising")
    elif oi_dir == "falling" and oic_dir == "falling":
        score += 1
    else:
        score += 3

    # 6. Liquidations bias (5 pts)
    liq_of  = of_d.get("liq_bias") or {}
    liq_val = float(liq_of.get("current_value") or 0)
    if direction == "bullish":
        if liq_val > 0:   # short liqs dominant
            score += 5; factors.append("shorts_liquidated_bullish")
        elif liq_val < 0:
            score += 1
        else:
            score += 2
    else:
        if liq_val < 0:   # long liqs dominant
            score += 5; factors.append("longs_liquidated_bearish")
        elif liq_val > 0:
            score += 1
        else:
            score += 2

    # 7. Funding bias (5 pts)
    fund_of   = of_d.get("funding") or {}
    fund_bias = fund_of.get("bias", "neutral")
    if direction == "bullish":
        if fund_bias == "short_crowded":
            score += 5; factors.append("funding_favors_long")
        elif fund_bias == "neutral":
            score += 3
        else:
            score += 1  # long_crowded = crowd over-extended long
    else:
        if fund_bias == "long_crowded":
            score += 5; factors.append("funding_favors_short")
        elif fund_bias == "neutral":
            score += 3
        else:
            score += 1

    # 8. Parent zone alignment (5 pts)
    overlap = float(cand_intel.get("overlap_pct") or 0)
    if overlap >= 50:
        score += 5; factors.append("deep_parent_overlap")
    elif overlap >= 20:
        score += 4; factors.append("good_parent_overlap")
    elif overlap >= 5:
        score += 2; factors.append("partial_parent_overlap")
    else:
        score += 1

    # 9. LTF confirmation (5 pts)
    ltf_ctx  = cand_intel.get("ltf_context") or {}
    align_s  = float(ltf_ctx.get("alignment_score") or 0)
    if align_s >= 75:
        score += 5; factors.append("ltf_confirmed")
    elif align_s >= 50:
        score += 3; factors.append("ltf_partial")
    elif align_s > 0:
        score += 1

    score = min(100, max(0, score))
    if score >= 80:
        label = "High Confidence"
    elif score >= 60:
        label = "Moderate Confidence"
    elif score >= 40:
        label = "Low Confidence"
    else:
        label = "Insufficient Evidence"

    return {"score": score, "label": label, "factors": factors}


def _lm_build_candidate_intelligence(candidate: dict, idx: int, symbol: str,  # noqa: C901
                                      exchange: str, market: str,
                                      analysis_source: str, snap_raw: dict,
                                      parent_setup: dict, direction: str,
                                      live_price: float,
                                      ltf_context: dict = None) -> dict:
    """Build full execution intelligence for one refinement candidate (Phase 11.1 Task 2).

    Combines zone geometry, virgin status, OB depth slice, orderflow context,
    and LTF confirmation into a scored intelligence dict.
    """
    zone_low  = float(candidate.get("zone_low")  or 0)
    zone_high = float(candidate.get("zone_high") or 0)
    ctype     = candidate.get("type", "ob")
    cand_tf   = candidate.get("tf", "")
    quality   = int(candidate.get("quality") or 0)
    overlap   = float(candidate.get("overlap_pct") or 0)
    dist      = float(candidate.get("dist") or 0)
    touches   = int(candidate.get("touches") or 0)

    mid_price      = (zone_high + zone_low) / 2.0 if zone_high and zone_low else 0.0
    zone_range     = zone_high - zone_low if zone_high > zone_low else 0.0
    zone_range_pct = round(zone_range / mid_price * 100, 3) if mid_price > 0 else 0.0
    virgin         = touches == 0

    # OB depth slice (Task 3)
    ob_slice = _lm_build_zone_ob_slice(zone_low, zone_high, symbol, exchange,
                                        analysis_source)

    # Orderflow context (Task 4)
    orderflow = _lm_build_candidate_orderflow(symbol, exchange, market,
                                               analysis_source, snap_raw, direction)

    intel_for_score = {
        "quality":     quality,
        "overlap_pct": overlap,
        "ob_slice":    ob_slice,
        "orderflow":   orderflow,
        "ltf_context": ltf_context or {},
    }
    score_result = _lm_score_candidate_intelligence(intel_for_score, parent_setup, direction)

    return {
        "idx":            idx,
        "type":           ctype,
        "tf":             cand_tf,
        "direction":      direction,
        "zone_low":       zone_low,
        "zone_high":      zone_high,
        "mid_price":      round(mid_price, 8),
        "zone_range":     round(zone_range, 8),
        "zone_range_pct": zone_range_pct,
        "quality":        quality,
        "quality_label":  candidate.get("quality_label", ""),
        "overlap_pct":    overlap,
        "dist_pct":       dist,
        "virgin":         virgin,
        "touches":        touches,
        "absorption":     bool(candidate.get("absorption", False)),
        "ob_slice":       ob_slice,
        "orderflow":      orderflow,
        "ltf_context":    ltf_context or {},
        "score":          score_result["score"],
        "score_label":    score_result["label"],
        "score_factors":  score_result["factors"],
    }


def _lm_build_execution_intelligence(item, snapshot: dict = None) -> dict:  # noqa: C901
    """Orchestrator: build Execution Intelligence for one Live Monitor item (Phase 11.1 Task 1).

    Evidence-only. No AI. No trading. No orders. No Entry Candidate.
    Reads from in-memory caches (OB depth, delta, liq) and REST-cached
    exchange data (funding, OI, L/S). Maximum 5 candidates processed.
    """
    now_ts = int(time.time())
    try:
        if snapshot is None:
            from models import LiveMonitorItem as _LMI11b
            row11 = _LMI11b.query.filter_by(id=item.id).first()
            snap_raw = _m._json_loads_safe(getattr(row11, "snapshot_json", None), {})
        else:
            snap_raw = snapshot if isinstance(snapshot, dict) else {}

        uid       = getattr(item, "user_id",   None)
        symbol    = (getattr(item, "symbol",   None) or "").upper()
        exchange  = (getattr(item, "exchange", None) or "binance").lower()
        market    = (getattr(item, "market",   None) or "perpetual").lower()
        parent_tf = (getattr(item, "timeframe", None) or "4h").lower()
        direction = _m._lm_direction_from_item(item)

        stored_src = (
            snap_raw.get("analysis_source") or
            (snap_raw.get("data_sources") or {}).get("analysis_source") or
            exchange
        )
        analysis_source = _m._lm_normalize_analysis_source(stored_src)

        if not symbol:
            return {"ok": False, "error": "no_symbol", "computed_at": now_ts}

        # Live price from WS cache
        ws_e, _ = _m._lm_ws_get("binance", symbol)
        live_price = float(
            (ws_e or {}).get("price") or
            (getattr(item, "entry_price", None) or 0)
        )

        # Parent zone
        zone_high = float(getattr(item, "zone_high", None) or 0)
        zone_low  = float(getattr(item, "zone_low",  None) or 0)
        parent_setup = {
            "direction": direction,
            "zone_high": zone_high,
            "zone_low":  zone_low,
            "symbol":    symbol,
            "exchange":  exchange,
        }

        # Candidates from latest Smart Entry Plan
        sep   = snap_raw.get("latest_smart_entry_plan") or {}
        cands = (sep.get("candidates") or [])[:5]

        # LTF confirmation context — computed once, shared across all candidates
        ltf_ctx = _lm_build_ltf_confirmation_context(
            cands, symbol, exchange, market, analysis_source, direction, snap_raw
        )

        # Item-level orderflow snapshot
        try:
            item_orderflow = _lm_build_candidate_orderflow(
                symbol, exchange, market, analysis_source, snap_raw, direction
            )
        except Exception as _eof:
            item_orderflow = {"error": str(_eof)[:120]}

        # Phase 11.1B: MTF orderflow history (Task 2/7)
        try:
            mtf_history = _lm_build_mtf_orderflow_history(
                uid, exchange, market, symbol, analysis_source, parent_tf
            )
        except Exception as _emtf:
            mtf_history = {"ok": False, "error": str(_emtf)[:120]}

        # Per-candidate intelligence
        cand_intels: list = []
        for idx, cand in enumerate(cands):
            try:
                ci = _lm_build_candidate_intelligence(
                    cand, idx, symbol, exchange, market,
                    analysis_source, snap_raw, parent_setup,
                    direction, live_price, ltf_context=ltf_ctx
                )
                # Phase 11.1B: enrich each candidate with MTF fusion data
                try:
                    ci["mtf_orderflow_history_summary"] = _lm_build_mtf_history_summary(mtf_history)
                    ci["smc_orderflow_fusion"] = _lm_build_smc_orderflow_fusion(ci, mtf_history, parent_setup)
                except Exception as _efus:
                    ci["mtf_orderflow_history_summary"] = {}
                    ci["smc_orderflow_fusion"] = {"error": str(_efus)[:120]}
                cand_intels.append(ci)
            except Exception as _ei:
                cand_intels.append({
                    "idx": idx, "error": str(_ei)[:120],
                    "score": 0, "score_label": "Error", "score_factors": [],
                })

        # Best candidate by score
        valid_ci  = [c for c in cand_intels if c.get("score", 0) > 0]
        best_cand = (max(valid_ci, key=lambda c: c["score"])
                     if valid_ci else None)

        return {
            "ok":              True,
            "item_id":         getattr(item, "id", None),
            "symbol":          symbol,
            "exchange":        exchange,
            "analysis_source": analysis_source,
            "direction":       direction,
            "live_price":      live_price,
            "candidate_count": len(cand_intels),
            "candidates":      cand_intels,
            "best_candidate":  {
                "idx":         best_cand["idx"],
                "score":       best_cand["score"],
                "score_label": best_cand["score_label"],
                "tf":          best_cand.get("tf", ""),
                "type":        best_cand.get("type", ""),
            } if best_cand else None,
            "orderflow":              item_orderflow,
            "ltf_context":            ltf_ctx,
            "mtf_orderflow_history":  mtf_history,
            "computed_at":            now_ts,
        }
    except Exception as _e11:
        return {"ok": False, "error": str(_e11)[:200], "computed_at": now_ts}


def _lm_save_execution_intelligence(uid: int, item_id: int, intel: dict) -> bool:
    """Persist execution intelligence into snapshot_json (Phase 11.1 Task 8)."""
    try:
        from models import db as _db11s, LiveMonitorItem as _LMI11s
        row = _LMI11s.query.filter_by(id=item_id, user_id=uid).first()
        if not row:
            return False
        snap = _m._json_loads_safe(row.snapshot_json, {})
        snap["latest_execution_intelligence"] = intel
        # Phase 11.1B: also index MTF history at top level for direct access
        if intel.get("mtf_orderflow_history"):
            snap["latest_mtf_orderflow_history"] = intel["mtf_orderflow_history"]
        row.snapshot_json = _m._json_dumps_safe(snap)
        _db11s.session.commit()
        return True
    except Exception as _e11s:
        print(f"[11.1] save_ei error item={item_id}: {_e11s}")
        return False
