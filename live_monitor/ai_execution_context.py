"""Phase 11.2: AI Execution Context Builder.

Moved from main.py on creation (Phase 11.1C pattern).
Routes stay in main.py.

Evidence-only: no AI call, no order, no Binance Testnet, no trade execution.
Aggregates Phase 11.1, 11.1B, Smart Entry, Data Health, and Danger signals
into one structured context dict for the AI layer.
"""
from __future__ import annotations
import time

# Deferred import — safe because all needed symbols are defined in main.py
# before the live_monitor import at line ~24034.
import main as _m


# ── Method quality map ────────────────────────────────────────────────────────
_MTF_METHOD_QUALITY = {
    "candle_delta":                "high",
    "aligned_orderflow_snapshots": "medium",
    "delta_proxy":                 "low",
    "unavailable":                 "none",
}


def _lm_build_ai_execution_context(item, snapshot=None) -> dict:  # noqa: C901
    """Build complete structured AI execution context (Phase 11.2).

    Aggregates:
      - Item / setup geometry
      - Analysis source resolution
      - Data Health gate
      - Smart Entry Plan (Phase 10.6)
      - Execution Intelligence (Phase 11.1)
      - MTF Orderflow History (Phase 11.1B)
      - SMC + Orderflow Fusion (Phase 11.1B)
      - Deterministic danger context
      - AI-allowed actions preview (advisory only — no execution in Phase 11.2)

    Evidence-only. No AI call. No order. No Binance Testnet.
    """
    now_ts = int(time.time())
    try:
        snap = (snapshot if isinstance(snapshot, dict)
                else _m._json_loads_safe(getattr(item, "snapshot_json", None), {}))

        # ── TASK 2: Item / setup context ─────────────────────────────────────
        item_id    = getattr(item, "id",        None)
        symbol     = (getattr(item, "symbol",   None) or "").upper()
        exchange   = (getattr(item, "exchange", None) or "binance").lower()
        market     = (getattr(item, "market",   None) or "perpetual").lower()
        source_tab = getattr(item, "source_tab", None)
        setup_type = getattr(item, "setup_type", None)
        timeframe  = (getattr(item, "timeframe", None) or "4h").lower()
        direction  = _m._lm_direction_from_item(item)
        zone_high  = float(getattr(item, "zone_high", None) or 0)
        zone_low   = float(getattr(item, "zone_low",  None) or 0)

        is_bias_shift = (
            source_tab == "bias_shift_watch" or
            (setup_type or "").lower() == "bias_shift_watch"
        )

        limitations: list  = []
        warnings_list: list = []

        # Normalize zone
        if zone_high > 0 and zone_low > 0 and zone_low > zone_high:
            zone_high, zone_low = zone_low, zone_high
            warnings_list.append("zone_swapped: zone_low > zone_high, auto-corrected")
        if not zone_high or not zone_low or zone_high <= zone_low:
            limitations.append("invalid_zone: zone_high/zone_low not set or invalid")

        # Live price
        try:
            ws_e, _ = _m._lm_ws_get("binance", symbol)
            live_price = float((ws_e or {}).get("price") or
                               getattr(item, "current_price", None) or 0)
        except Exception:
            live_price = float(getattr(item, "current_price", None) or 0)

        item_ctx = {
            "item_id":            item_id,
            "symbol":             symbol,
            "exchange":           exchange,
            "market":             market,
            "setup_type":         setup_type,
            "direction":          direction,
            "timeframe":          timeframe,
            "zone_low":           zone_low,
            "zone_high":          zone_high,
            "current_price":      live_price,
            "source_tab":         source_tab,
            "is_bias_shift_item": is_bias_shift,
            "created_at":         str(getattr(item, "added_at",   None) or ""),
            "updated_at":         str(getattr(item, "updated_at", None) or ""),
        }

        # ── TASK 3: Analysis source context ──────────────────────────────────
        try:
            asc = _m._lm_analysis_source_config(item, snapshot=snap)
        except Exception:
            asc = {
                "analysis_source":       exchange,
                "parent_setup_exchange": exchange,
                "price_levels_source":   exchange,
                "execution_exchange":    "not_in_scope_phase11_2",
                "sources_used":          [],
                "sources_skipped":       [],
                "warnings":              [],
            }

        norm_src = _m._lm_normalize_analysis_source(asc.get("analysis_source", exchange))
        src_mode = (
            "specific_exchange" if norm_src in _m._LM_SPECIFIC_ANALYSIS_SRCS else
            "aggregated"        if norm_src == "aggregated" else
            "fallback"
        )

        analysis_source_ctx = {
            "selected_analysis_source": asc.get("analysis_source"),
            "resolved_analysis_source": norm_src,
            "parent_setup_exchange":    asc.get("parent_setup_exchange", exchange),
            "execution_exchange_scope": "not_in_scope_phase11_2",
            "source_mode":              src_mode,
            "sources_used":             asc.get("sources_used"),
            "sources_skipped":          asc.get("sources_skipped"),
            "warnings":                 (asc.get("warnings") or [])[:5],
        }

        # ── TASK 4: Data health context ───────────────────────────────────────
        try:
            dh = _m._lm_build_data_health_context(symbol, norm_src, snap=snap)
        except Exception as _edh:
            dh = {
                "critical_status": "unavailable",
                "rows":            [],
                "ai_data_gate":    {"allowed": False, "reasons": [f"data_health_error:{str(_edh)[:60]}"]},
            }

        dh_gate         = (dh or {}).get("ai_data_gate") or {}
        data_gate_blocked = not dh_gate.get("allowed", True)

        critical_rows: dict = {}
        for dh_row in (dh or {}).get("rows") or []:
            metric = dh_row.get("metric", "")
            if metric:
                critical_rows[metric] = {
                    "status": dh_row.get("status"),
                    "value":  dh_row.get("value"),
                    "source": dh_row.get("source"),
                    "notes":  dh_row.get("notes"),
                }

        stale_count = sum(
            1 for r in (dh or {}).get("rows", [])
            if r.get("status") in ("stale", "unavailable")
        )

        data_health_ctx = {
            "ai_data_gate":      dh_gate,
            "critical_status":   (dh or {}).get("critical_status"),
            "rows":              critical_rows,
            "stale_count":       stale_count,
            "safe_for_analysis": bool(dh_gate.get("allowed", False)),
        }

        # ── TASK 5: Smart entry context ───────────────────────────────────────
        sep = snap.get("latest_smart_entry_plan") or {}
        smart_entry_ctx = None
        if sep:
            raw_cands = sep.get("refinement_candidates") or sep.get("candidates") or []
            cand_summaries_sep = [
                {
                    "idx":           c.get("idx"),
                    "type":          c.get("type"),
                    "tf":            c.get("tf"),
                    "zone_low":      c.get("zone_low"),
                    "zone_high":     c.get("zone_high"),
                    "quality":       c.get("quality"),
                    "quality_label": c.get("quality_label"),
                    "overlap_pct":   c.get("overlap_pct"),
                    "dist":          c.get("dist"),
                    "virgin":        c.get("virgin"),
                    "touches":       c.get("touches"),
                }
                for c in raw_cands[:10]
            ]
            smart_entry_ctx = {
                "mode":                  sep.get("mode"),
                "decision":              sep.get("decision"),
                "direction":             sep.get("direction"),
                "parent_setup":          sep.get("parent_setup"),
                "default_entry_plan":    sep.get("default_entry_plan"),
                "best_candidate":        sep.get("best_refined_candidate") or sep.get("best_candidate"),
                "refinement_candidates": cand_summaries_sep,
                "candidate_count":       len(raw_cands),
                "invalidation":          sep.get("invalidation"),
                "data_quality":          sep.get("data_quality"),
                "confidence":            sep.get("confidence"),
                "reason_summary":        sep.get("reason_summary"),
                "block_reason":          sep.get("block_reason"),
                "warnings":              (sep.get("warnings") or [])[:5],
            }

        # ── TASK 6: Execution intelligence context ────────────────────────────
        ei       = snap.get("latest_execution_intelligence") or {}
        ei_ctx   = None
        if ei and ei.get("ok"):
            cand_ei_list = []
            for c in (ei.get("candidates") or []):
                ob = c.get("ob_slice") or {}
                of = c.get("orderflow") or {}
                cand_ei_list.append({
                    "idx":           c.get("idx"),
                    "type":          c.get("type"),
                    "tf":            c.get("tf"),
                    "direction":     c.get("direction"),
                    "zone_low":      c.get("zone_low"),
                    "zone_high":     c.get("zone_high"),
                    "quality":       c.get("quality"),
                    "quality_label": c.get("quality_label"),
                    "overlap_pct":   c.get("overlap_pct"),
                    "dist_pct":      c.get("dist_pct"),
                    "virgin":        c.get("virgin"),
                    "touches":       c.get("touches"),
                    "score":         c.get("score"),
                    "score_label":   c.get("score_label"),
                    "score_factors": c.get("score_factors"),
                    "ob_slice": {
                        "available":         ob.get("available"),
                        "status":            ob.get("status"),
                        "bid_total":         ob.get("bid_total"),
                        "ask_total":         ob.get("ask_total"),
                        "imbalance_ratio":   ob.get("imbalance_ratio"),
                        "imbalance_side":    ob.get("imbalance_side"),
                        "zone_vol_pct_bids": ob.get("zone_vol_pct_bids"),
                        "zone_vol_pct_asks": ob.get("zone_vol_pct_asks"),
                    },
                    "orderflow_states": {
                        "delta_state":      (of.get("delta") or {}).get("state"),
                        "cvd_proxy_state":  (of.get("cvd_proxy") or {}).get("state"),
                        "oi_state":         (of.get("open_interest") or {}).get("state"),
                        "oi_change_state":  (of.get("oi_change") or {}).get("state"),
                        "liq_bias_state":   (of.get("liq_bias") or {}).get("state"),
                        "funding_bias":     (of.get("funding") or {}).get("bias"),
                        "funding_state":    (of.get("funding") or {}).get("state"),
                        "long_short_state": (of.get("long_short") or {}).get("state"),
                    },
                })

            of_snap = ei.get("orderflow") or {}
            ei_ctx = {
                "ok":              True,
                "computed_at":     ei.get("computed_at"),
                "live_price":      ei.get("live_price"),
                "candidate_count": ei.get("candidate_count"),
                "best_candidate":  ei.get("best_candidate"),
                "candidates":      cand_ei_list,
                "orderflow_snapshot": {
                    "delta_state":     (of_snap.get("delta")          or {}).get("state"),
                    "cvd_proxy_state": (of_snap.get("cvd_proxy")      or {}).get("state"),
                    "oi_direction":    (of_snap.get("open_interest")   or {}).get("direction"),
                    "funding_bias":    (of_snap.get("funding")         or {}).get("bias"),
                    "liq_state":       (of_snap.get("liq_bias")        or {}).get("state"),
                    "exchange_used":   of_snap.get("exchange_used"),
                },
                "ltf_context": {
                    "verdict":         (ei.get("ltf_context") or {}).get("verdict"),
                    "alignment_score": (ei.get("ltf_context") or {}).get("alignment_score"),
                    "aligned_tfs":     (ei.get("ltf_context") or {}).get("aligned_tfs"),
                },
            }

        # ── TASK 7: MTF orderflow history context + quality ───────────────────
        mtf_h   = (snap.get("latest_mtf_orderflow_history") or
                   (ei or {}).get("mtf_orderflow_history") or {})
        mtf_ctx = None
        of_history_quality = {"overall": "insufficient", "by_tf": {}, "limitations": []}

        if mtf_h and (mtf_h.get("timeframes") or {}):
            by_tf_quality: dict = {}
            quality_list:  list = []
            tf_summaries:  dict = {}

            for tf_key, tf_d in (mtf_h.get("timeframes") or {}).items():
                if not isinstance(tf_d, dict):
                    continue
                method      = tf_d.get("cvd_method", "unavailable")
                tf_quality  = _MTF_METHOD_QUALITY.get(method, "none")
                samples     = tf_d.get("candle_count", 0)
                by_tf_quality[tf_key] = {
                    "method":         method,
                    "quality":        tf_quality,
                    "samples":        samples,
                    "data_available": tf_d.get("data_available", False),
                }
                quality_list.append(tf_quality)

                cvd_st = tf_d.get("cvd_state")   or {}
                dlt_st = tf_d.get("delta_state")  or {}
                oi_d   = tf_d.get("open_interest") or {}
                liq_d  = tf_d.get("liquidations")  or {}
                fund_d = tf_d.get("funding")        or {}
                ls_d   = tf_d.get("long_short")     or {}
                tf_summaries[tf_key] = {
                    "tf":              tf_d.get("tf"),
                    "data_available":  tf_d.get("data_available", False),
                    "candles_used":    samples,
                    "cvd_method":      method,
                    "method_quality":  tf_quality,
                    "cvd_state": {
                        "state":            cvd_st.get("state"),
                        "direction":        cvd_st.get("direction"),
                        "slope_pct":        cvd_st.get("slope_pct"),
                        "velocity":         cvd_st.get("velocity"),
                        "acceleration":     cvd_st.get("acceleration"),
                        "but_improving":    cvd_st.get("but_improving"),
                        "danger_increasing": cvd_st.get("danger_increasing"),
                    },
                    "delta_state": {
                        "state":            dlt_st.get("state"),
                        "direction":        dlt_st.get("direction"),
                        "slope_pct":        dlt_st.get("slope_pct"),
                        "velocity":         dlt_st.get("velocity"),
                        "acceleration":     dlt_st.get("acceleration"),
                        "but_improving":    dlt_st.get("but_improving"),
                        "danger_increasing": dlt_st.get("danger_increasing"),
                    },
                    "oi_state":         oi_d.get("oi_direction", ""),
                    "liquidation_bias": round(float(liq_d.get("liq_bias", 0)), 2),
                    "funding_bias":     fund_d.get("bias", "neutral"),
                    "long_short_bias": {
                        "long_pct":  ls_d.get("long_pct"),
                        "short_pct": ls_d.get("short_pct"),
                    },
                }

            # Overall quality from the best methods available
            high  = quality_list.count("high")
            med   = quality_list.count("medium")
            low   = quality_list.count("low")
            if high >= 2:
                overall_q = "high"
            elif high >= 1 or med >= 2:
                overall_q = "medium"
            elif med >= 1 or low >= 2:
                overall_q = "low"
            else:
                overall_q = "insufficient"

            q_limitations = [
                f"{tf}: {info['method']} (quality={info['quality']})"
                for tf, info in by_tf_quality.items()
                if info["quality"] in ("low", "none")
            ]

            of_history_quality = {
                "overall":     overall_q,
                "by_tf":       by_tf_quality,
                "limitations": q_limitations,
            }

            mtf_ctx = {
                "parent_tf":        mtf_h.get("parent_tf"),
                "child_timeframes": mtf_h.get("child_tfs", []),
                "available_tfs":    mtf_h.get("available_tfs", []),
                "computed_at":      mtf_h.get("computed_at"),
                "timeframes":       tf_summaries,
            }

        # ── TASK 8: SMC + orderflow fusion context ────────────────────────────
        fusion_ctx = None
        if ei_ctx and (ei_ctx.get("candidates") or []):
            cands_fusion = []
            for c in ei_ctx["candidates"]:
                raw_c = next(
                    (x for x in (ei.get("candidates") or []) if x.get("idx") == c.get("idx")),
                    {}
                )
                f = raw_c.get("smc_orderflow_fusion") or {}
                cands_fusion.append({
                    "idx":               c.get("idx"),
                    "tf":                c.get("tf"),
                    "type":              c.get("type"),
                    "zone_flow_state":   f.get("zone_flow_state", "uncertain"),
                    "confidence":        f.get("confidence", 0),
                    "recommended_entry_behavior": f.get("recommended_entry_behavior", "wait"),
                    "bullish_evidence":  (f.get("bullish_evidence") or [])[:5],
                    "bearish_evidence":  (f.get("bearish_evidence") or [])[:3],
                    "danger_evidence":   (f.get("danger_evidence")  or [])[:5],
                    "reason_summary":    f.get("reason_summary", ""),
                })

            best_cand_idx = (ei_ctx.get("best_candidate") or {}).get("idx")
            best_fusion = next(
                (c for c in cands_fusion if c["idx"] == best_cand_idx),
                max(cands_fusion, key=lambda x: x.get("confidence", 0)) if cands_fusion else None
            )

            fusion_ctx = {
                "best_zone_flow_state":         (best_fusion or {}).get("zone_flow_state", "uncertain"),
                "best_recommended_behavior":    (best_fusion or {}).get("recommended_entry_behavior", "wait"),
                "highest_confidence_candidate": best_cand_idx,
                "candidates_breakdown":         cands_fusion,
            }

        # ── TASK 9: Danger context (deterministic, no AI) ─────────────────────
        danger_score    = 0
        danger_reasons:   list = []
        blocking_reasons: list = []

        # R1: data health gate blocked
        if data_gate_blocked:
            blocking_reasons.append("data_health_gate_blocked")
            danger_score = 100

        # R2: setup invalidated or smart entry blocked
        if smart_entry_ctx:
            inv = smart_entry_ctx.get("invalidation") or {}
            if inv.get("invalidated"):
                blocking_reasons.append("setup_invalidated")
                danger_score = max(danger_score, 95)
            if smart_entry_ctx.get("mode") == "blocked":
                blocking_reasons.append(
                    f"smart_entry_blocked:{smart_entry_ctx.get('block_reason', 'unknown')[:40]}"
                )
                danger_score = max(danger_score, 100)

        # R3: SMC fusion states
        if fusion_ctx:
            best_state = fusion_ctx.get("best_zone_flow_state", "uncertain")
            best_conf  = 0
            for cf_ in fusion_ctx.get("candidates_breakdown", []):
                if cf_.get("zone_flow_state") == best_state:
                    best_conf = max(best_conf, cf_.get("confidence", 0))
            if best_state == "breaking" and best_conf >= 70:
                danger_reasons.append(f"fusion_breaking_conf_{best_conf}")
                danger_score = max(danger_score, 85)
            elif best_state == "breaking":
                danger_reasons.append("fusion_breaking")
                danger_score = max(danger_score, 65)
            elif best_state == "attacked":
                danger_reasons.append("fusion_attacked")
                danger_score = max(danger_score, 60)
            elif best_state == "ignored":
                danger_reasons.append("fusion_ignored")
                danger_score = max(danger_score, 30)

        # R4: MTF CVD / delta signals across multiple TFs
        if mtf_ctx:
            neg_cvd_count   = 0
            neg_delta_count = 0
            for tf_key, tf_info in (mtf_ctx.get("timeframes") or {}).items():
                cvd_s  = (tf_info.get("cvd_state") or {}).get("state", "")
                dlt_s  = (tf_info.get("delta_state") or {}).get("state", "")
                if direction == "bullish":
                    if cvd_s == "negative_strengthening":
                        neg_cvd_count += 1
                    if dlt_s == "negative_strengthening":
                        neg_delta_count += 1
                        danger_reasons.append(f"{tf_key}:delta_neg_strengthening")
                        danger_score = max(danger_score, 55)
                elif direction == "bearish":
                    if cvd_s == "positive_strengthening":
                        neg_cvd_count += 1
                    if dlt_s == "positive_strengthening":
                        neg_delta_count += 1
                        danger_reasons.append(f"{tf_key}:delta_pos_strengthening_bear")
                        danger_score = max(danger_score, 55)
            if neg_cvd_count >= 2:
                danger_reasons.append(f"cvd_adverse_{neg_cvd_count}_tfs")
                danger_score = max(danger_score, 70)

        # R5: OB imbalance against directional bias
        if ei_ctx:
            for c in (ei_ctx.get("candidates") or []):
                ob = c.get("ob_slice") or {}
                if direction == "bullish" and ob.get("imbalance_side") == "ask_heavy":
                    danger_reasons.append(f"ask_wall_bullish_zone_c{c.get('idx')}")
                    danger_score = max(danger_score, 45)
                elif direction == "bearish" and ob.get("imbalance_side") == "bid_heavy":
                    danger_reasons.append(f"bid_wall_bearish_zone_c{c.get('idx')}")
                    danger_score = max(danger_score, 45)

        # R6: stale data
        if stale_count >= 4:
            danger_reasons.append(f"stale_data_{stale_count}_rows")
            danger_score = max(danger_score, 40)

        # Danger level
        if blocking_reasons:
            danger_level = "blocked"
        elif danger_score >= 70:
            danger_level = "high"
        elif danger_score >= 40:
            danger_level = "medium"
        else:
            danger_level = "low"

        # Recommended behavior
        if blocking_reasons:
            rec_behavior = "block"
        elif danger_level == "high":
            rec_behavior = "wait"
        elif fusion_ctx and fusion_ctx.get("best_recommended_behavior") not in ("block", None):
            rec_behavior = fusion_ctx["best_recommended_behavior"]
        elif danger_level == "medium":
            rec_behavior = "confirmation"
        else:
            rec_behavior = "touch_limit"

        danger_ctx = {
            "danger_level":                   danger_level,
            "danger_score":                   min(100, danger_score),
            "reasons":                        danger_reasons[:10],
            "blocking_reasons":               blocking_reasons,
            "recommended_behavior":           rec_behavior,
            "touch_limit_allowed_by_context": (danger_level == "low" and not blocking_reasons),
            "confirmation_required":          danger_level in ("medium", "high"),
        }

        # ── TASK 10: AI allowed actions preview (advisory only, no execution) ──
        if blocking_reasons:
            allowed_actions = ["block_trade", "pause_setup"]
        elif danger_level == "high":
            allowed_actions = ["block_trade", "switch_to_confirmation", "pause_setup"]
        elif danger_level == "medium":
            allowed_actions = ["switch_to_confirmation", "reduce_size", "pause_setup"]
        else:
            allowed_actions = ["allow_touch_limit", "no_action"]

        # ── TASK 11: Order-flow series context (Phase FlowC.D) ─────────────────
        # Real tick-built CVD + Open Interest series and deterministic CVD
        # divergence / OI regime flags, distinct from the cvd_method/cvd_state
        # candle-wick heuristics in TASK 7 above (used when no tick stream is
        # available). Bounded and rounded. Purely descriptive — does NOT feed
        # danger_ctx, blocking_reasons, or allowed_actions above; this section
        # is computed after and independently of that decision.
        order_flow_series_ctx = None
        try:
            _of_candles = _m._lm_get_flow_candles_series(symbol, "5m", 24)  # last ~2h
            if len(_of_candles) >= 3:
                _of_recent = [{
                    "t":           c.get("t"),
                    "price_close": c.get("price_close"),
                    "delta_usd":   c.get("delta_usd"),
                    "cvd_usd":     c.get("cvd_usd"),
                    "oi_close":    c.get("oi_close"),
                } for c in _of_candles[-12:]]  # last 12 x 5m = 1h, bounded

                _of_divergences = []
                if len(_of_candles) >= 10:
                    _raw_divs = _m._lm_detect_metric_divergence(_of_candles, "cvd_usd")
                    _of_divergences = [{
                        "kind":        d["kind"],
                        "swing_type":  d["swing_type"],
                        "strength":    d["strength"],
                        "candle_b_ms": d["candle_b_ms"],
                    } for d in _raw_divs[:3]]

                _oir = _m._lm_classify_oi_regime(_of_candles, lookback=20)
                order_flow_series_ctx = {
                    "ok":             True,
                    "timeframe":      "5m",
                    "candle_count":   len(_of_candles),
                    "recent_candles": _of_recent,
                    "cvd_divergences": _of_divergences,
                    "oi_regime": {
                        "current":        _oir.get("current_regime"),
                        "dominant_recent": _oir.get("dominant_regime_recent"),
                        "sample_count":   _oir.get("sample_count"),
                    },
                }
            else:
                order_flow_series_ctx = {"ok": False, "reason": "insufficient_history",
                                         "candle_count": len(_of_candles)}
        except Exception as _ofe:
            order_flow_series_ctx = {"ok": False, "reason": f"error: {str(_ofe)[:100]}"}

        return {
            "phase":       "phase11_2_ai_execution_context",
            "ok":          True,
            "computed_at": now_ts,

            "item":                       item_ctx,
            "analysis_source":            analysis_source_ctx,
            "data_health":                data_health_ctx,
            "smart_entry":                smart_entry_ctx,
            "execution_intelligence":     ei_ctx,
            "mtf_orderflow_history":      mtf_ctx,
            "candidate_matrix":           (ei_ctx or {}).get("candidates") or [],
            "best_candidate":             (ei_ctx or {}).get("best_candidate"),
            "smc_orderflow_fusion":       fusion_ctx,
            "danger_context":             danger_ctx,
            "orderflow_history_quality":  of_history_quality,
            "order_flow_series":          order_flow_series_ctx,
            "ai_allowed_actions_preview": allowed_actions,
            "limitations":                limitations,
            "warnings":                   warnings_list,
        }

    except Exception as _e112:
        return {
            "phase":       "phase11_2_ai_execution_context",
            "ok":          False,
            "error":       str(_e112)[:200],
            "computed_at": now_ts,
        }
