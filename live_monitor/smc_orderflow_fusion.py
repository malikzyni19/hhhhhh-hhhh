"""Phase 11.1B: SMC Orderflow Fusion helper.

Moved from main.py (Phase 11.1C module split). Routes stay in main.py.
Evidence-only: no AI, no orders, no Entry Candidate, no trading.
Pure function — no external dependencies.
"""
from __future__ import annotations


def _lm_build_smc_orderflow_fusion(candidate: dict, mtf_history: dict,  # noqa: C901
                                    parent_setup: dict) -> dict:
    """Fuse SMC zone state with multi-TF orderflow evidence (Phase 11.1B Task 6).

    Evaluates whether the zone is being defended, absorbing, attacked, breaking,
    ignored, or uncertain based on CVD trend, delta flow, OB depth, and liq bias.

    zone_flow_state values:
      defended   — clear bullish (or bearish) defence with no danger signs
      absorbing  — zone taking flow, partial evidence
      attacked   — danger signals present but zone not yet broken
      breaking   — strong danger signals, zone likely failing
      ignored    — no price action near zone / neutral flow
      uncertain  — insufficient or conflicting evidence
    """
    direction = (candidate.get("direction") or
                 parent_setup.get("direction", "bullish")).lower()
    tfs_data  = (mtf_history or {}).get("timeframes") or {}

    bullish_ev: list = []
    bearish_ev: list = []
    danger_ev:  list = []

    for tf, tf_d in tfs_data.items():
        if not isinstance(tf_d, dict) or not tf_d.get("data_available"):
            continue
        p = tf.upper()

        cvd_st  = tf_d.get("cvd_state")   or {}
        dlt_st  = tf_d.get("delta_state") or {}
        liq_d   = tf_d.get("liquidations") or {}
        oi_d    = tf_d.get("open_interest") or {}
        fund_d  = tf_d.get("funding") or {}
        ob_sl   = candidate.get("ob_slice") or {}

        cvd_state    = cvd_st.get("state", "")
        delta_state  = dlt_st.get("state", "")
        cvd_imp      = bool(cvd_st.get("but_improving"))
        delta_danger = bool(dlt_st.get("danger_increasing"))
        liq_bias     = float(liq_d.get("liq_bias", 0))
        imb_side     = ob_sl.get("imbalance_side", "neutral")
        fund_bias    = fund_d.get("bias", "neutral")
        oi_dir       = oi_d.get("oi_direction", "")

        if direction == "bullish":
            # Positive signals for a bullish zone
            if cvd_state in ("positive_strengthening",) or cvd_imp:
                bullish_ev.append(f"[{p}] CVD {cvd_state or 'improving'}")
            if cvd_state == "negative_weakening":
                bullish_ev.append(f"[{p}] CVD negative-but-improving")
            if delta_state in ("positive_strengthening", "negative_weakening"):
                bullish_ev.append(f"[{p}] delta {delta_state}")
            if imb_side == "bid_heavy":
                bullish_ev.append(f"[{p}] bid wall in zone")
            if liq_bias > 0:
                bullish_ev.append(f"[{p}] short liqs dominant (+{liq_bias:,.0f})")
            if fund_bias == "short_crowded":
                bullish_ev.append(f"[{p}] shorts crowded in funding")
            if oi_dir == "increasing":
                bullish_ev.append(f"[{p}] OI increasing")
            # Danger signals
            if cvd_state == "negative_strengthening" and not cvd_imp:
                danger_ev.append(f"[{p}] CVD negative_strengthening (danger)")
            if delta_state == "negative_strengthening" or delta_danger:
                danger_ev.append(f"[{p}] sell delta accelerating")
            if imb_side == "ask_heavy":
                danger_ev.append(f"[{p}] ask wall forming in zone")
            if liq_bias < -2000:
                danger_ev.append(f"[{p}] long liqs dominant ({liq_bias:,.0f})")
            if fund_bias == "long_crowded" and delta_state == "negative_strengthening":
                danger_ev.append(f"[{p}] long crowd + sell pressure")
            # Bearish contradiction
            if cvd_state == "negative_strengthening" and imb_side == "ask_heavy":
                bearish_ev.append(f"[{p}] CVD down + ask wall (contradicts zone)")

        else:  # bearish zone (resistance)
            if cvd_state in ("negative_strengthening",) or cvd_st.get("danger_increasing"):
                bullish_ev.append(f"[{p}] CVD {cvd_state or 'falling'} (bears winning)")
            if cvd_state == "positive_weakening":
                bullish_ev.append(f"[{p}] CVD positive-but-weakening (bear sign)")
            if delta_state in ("negative_strengthening", "positive_weakening"):
                bullish_ev.append(f"[{p}] delta {delta_state} (bearish)")
            if imb_side == "ask_heavy":
                bullish_ev.append(f"[{p}] ask wall in zone (bearish)")
            if liq_bias < 0:
                bullish_ev.append(f"[{p}] long liqs dominant ({liq_bias:,.0f})")
            if fund_bias == "long_crowded":
                bullish_ev.append(f"[{p}] longs crowded in funding")
            # Danger signals for bearish zone
            if cvd_state in ("positive_strengthening", "negative_weakening") and not cvd_imp:
                danger_ev.append(f"[{p}] CVD rising into resistance (danger)")
            if delta_state == "positive_strengthening":
                danger_ev.append(f"[{p}] buy delta accelerating at resistance")
            if imb_side == "bid_heavy":
                danger_ev.append(f"[{p}] bid wall forming (buyers aggressive)")
            if liq_bias > 2000:
                danger_ev.append(f"[{p}] short liqs dominant — buyers winning")

    bull = len(bullish_ev)
    dng  = len(danger_ev)

    # Determine zone flow state
    no_data = not any(d.get("data_available") for d in tfs_data.values()) if tfs_data else True

    if no_data:
        state   = "uncertain"
        conf    = 0
        behav   = "wait"
        summary = "No multi-TF orderflow data available."
    elif dng >= 3:
        state   = "breaking"
        conf    = min(90, 40 + dng * 12)
        behav   = "block"
        summary = f"Zone showing {dng} danger signals — likely failing."
    elif dng >= 2:
        state   = "attacked"
        conf    = min(75, 30 + dng * 15)
        behav   = "wait"
        summary = f"Zone under attack ({dng} danger signals)."
    elif bull >= 3 and dng == 0:
        state   = "defended"
        conf    = min(85, 40 + bull * 8)
        behav   = "touch_limit"
        summary = f"Zone defended ({bull} flow signals align, no danger)."
    elif bull >= 2:
        state   = "absorbing"
        conf    = min(70, 25 + bull * 10)
        behav   = "confirmation"
        summary = f"Zone absorbing ({bull} flow signals)."
    elif bull == 0 and dng == 0:
        state   = "ignored"
        conf    = 15
        behav   = "wait"
        summary = "Orderflow neutral — zone not being tested."
    else:
        state   = "uncertain"
        conf    = 20
        behav   = "confirmation"
        summary = f"Mixed: {bull} bullish, {dng} danger, {len(bearish_ev)} contra."

    return {
        "zone_flow_state":            state,
        "confidence":                 conf,
        "bullish_evidence":           bullish_ev,
        "bearish_evidence":           bearish_ev,
        "danger_evidence":            danger_ev,
        "recommended_entry_behavior": behav,
        "reason_summary":             summary,
    }
