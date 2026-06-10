"""Phase 11.1B: MTF Orderflow History helpers.

Moved from main.py (Phase 11.1C module split). Routes stay in main.py.
Evidence-only: no AI, no orders, no Entry Candidate, no trading.
"""
from __future__ import annotations
import time

# Deferred import — avoids circular dependency.
# main is partially loaded when this module is first imported, but all
# needed symbols are defined before the live_monitor import in main.py.
import main as _m


# ── Phase 11.1B helpers ───────────────────────────────────────────────────────

def _lm_child_orderflow_timeframes(parent_tf: str) -> list:
    """Return child orderflow TFs for a given parent TF (Phase 11.1B Task 1).

    Ensures 4h setups are judged by 1h/30m/15m/5m flow — not just 5m snapshot.
    """
    _map = {
        "1d":  ["4h", "1h", "30m", "15m"],
        "4h":  ["1h", "30m", "15m", "5m"],
        "1h":  ["30m", "15m", "5m"],
        "30m": ["15m", "5m"],
        "15m": ["5m"],
        "5m":  ["5m"],
    }
    return list(_map.get((parent_tf or "").lower().strip(), ["5m"]))


def _lm_fetch_klines_with_delta(symbol: str, tf: str, limit: int = 20,
                                  exchange: str = "binance") -> tuple:
    """Fetch candles with per-candle delta from taker buy/sell volumes.

    Returns (candle_list, cvd_method).

    Binance: fetches /fapi/v1/klines with k[7] (quote_vol) and k[10]
    (taker_buy_quote_vol) → exact per-candle delta. CVD method: "candle_delta".

    Non-Binance public klines do not expose taker buy volumes reliably, so
    delta is set to None. CVD method: "unavailable".
    """
    lim = max(1, min(limit, 500))
    eff_exch = (exchange or "binance").lower()

    if eff_exch == "binance":
        tf_ms = _m._LM_TF_MS.get(tf, 3_600_000)
        ttl   = max(30, tf_ms // 1000 // 4)
        cache_key = f"klines_delta:binance:{symbol}:{tf}:{lim}"

        def _fetch_binance():
            try:
                resp = _m.req.get(
                    f"{_m.BINANCE_FUTURES_API}/fapi/v1/klines",
                    params={"symbol": symbol.upper(), "interval": tf, "limit": lim},
                    timeout=10,
                )
                if resp.status_code != 200:
                    return None
                data = resp.json()
                if not isinstance(data, list):
                    return None
                result = []
                for k in data:
                    try:
                        q_vol    = float(k[7])
                        t_buy_q  = float(k[10])
                        t_sell_q = q_vol - t_buy_q
                        result.append({
                            "open_time":        int(k[0]),
                            "close_time":       int(k[6]),
                            "open":             float(k[1]),
                            "high":             float(k[2]),
                            "low":              float(k[3]),
                            "close":            float(k[4]),
                            "volume":           float(k[5]),
                            "taker_buy_quote":  round(t_buy_q, 2),
                            "taker_sell_quote": round(t_sell_q, 2),
                            "delta":            round(t_buy_q - t_sell_q, 2),
                        })
                    except (IndexError, TypeError, ValueError):
                        pass
                return result or None
            except Exception:
                return None

        raw = _m._lm_rest_cached(cache_key, ttl, _fetch_binance)
        if raw:
            return raw, "candle_delta"

    # Non-Binance: fetch basic klines (no taker vol → delta=None)
    raw_klines = _m._lm_fetch_public_klines(eff_exch, symbol, tf, limit=lim)
    candles = [{
        "open_time":        c["open_time"],
        "close_time":       c.get("close_time"),
        "open":             c["open"],
        "high":             c["high"],
        "low":              c["low"],
        "close":            c["close"],
        "volume":           c.get("volume"),
        "taker_buy_quote":  None,
        "taker_sell_quote": None,
        "delta":            None,
    } for c in raw_klines]
    return candles, "unavailable"


def _lm_build_series_orderflow_state(series: list) -> dict:  # noqa: C901
    """Analyze a list of numeric values (oldest→newest) for multi-candle trend state.

    Phase 11.1B Task 5. Extends _lm_build_orderflow_state to understand series
    patterns: [-10,-8,-6,-4] = negative_weakening (improving);
    [-4,-6,-8,-10] = negative_strengthening (danger increasing).

    Uses linear regression slope across the full series for reliable direction.
    """
    clean = [float(v) for v in (series or []) if v is not None]
    n = len(clean)

    _empty = {
        "state": "neutral", "direction": "flat", "slope": 0.0,
        "slope_pct": 0.0, "velocity": "slow", "acceleration": "neutral",
        "but_improving": False, "danger_increasing": False,
        "current": 0.0, "first": 0.0, "series_len": 0,
    }
    if n == 0:
        return _empty
    if n == 1:
        c = clean[0]
        st = "negative" if c < 0 else ("positive" if c > 0 else "neutral")
        return {**_empty, "state": st, "current": round(c, 4), "first": round(c, 4),
                "series_len": 1}

    # Linear regression slope (units per candle step)
    xs     = list(range(n))
    x_mean = (n - 1) / 2.0
    y_mean = sum(clean) / n
    numer  = sum((xs[i] - x_mean) * (clean[i] - y_mean) for i in range(n))
    denom  = sum((x - x_mean) ** 2 for x in xs) or 1.0
    slope  = numer / denom

    abs_mean      = sum(abs(v) for v in clean) / n or 1.0
    slope_pct     = slope / abs_mean * 100
    abs_slope_pct = abs(slope_pct)

    if abs_slope_pct < 1.0:
        direction = "flat"
        velocity  = "slow"
    else:
        all_neg = all(v <= 0 for v in clean)
        all_pos = all(v >= 0 for v in clean)
        if all_neg and slope > 0:
            direction = "rising_toward_zero"
        elif all_pos and slope < 0:
            direction = "falling_toward_zero"
        else:
            direction = "rising" if slope > 0 else "falling"
        velocity = "fast" if abs_slope_pct >= 20 else ("normal" if abs_slope_pct >= 8 else "slow")

    # Acceleration: compare slope of first half vs second half
    half        = max(1, n // 2)
    early_slope = (clean[half - 1] - clean[0]) / half       if half > 1 else 0.0
    late_slope  = (clean[-1]       - clean[n - half]) / half if half > 1 else 0.0
    if abs(late_slope) > abs(early_slope) * 1.25:
        acceleration = "strengthening"
    elif abs(late_slope) < abs(early_slope) * 0.75:
        acceleration = "weakening"
    else:
        acceleration = "neutral"

    mostly_neg     = sum(1 for v in clean if v < 0) >= max(1, n * 0.6)
    but_improving  = mostly_neg and slope > 0
    danger_incr    = mostly_neg and slope < 0

    current = clean[-1]
    first   = clean[0]

    if direction == "flat":
        state = "neutral"
    elif direction == "rising_toward_zero":
        state = "negative_weakening"
    elif direction == "falling_toward_zero":
        state = "positive_weakening"
    elif all(v >= 0 for v in clean) and slope >= 0:
        state = "positive_strengthening"
    elif all(v >= 0 for v in clean) and slope < 0:
        state = "positive_weakening"
    elif mostly_neg and slope > 0:
        state = "negative_weakening"
    elif mostly_neg and slope < 0:
        state = "negative_strengthening"
    elif current >= 0 and slope > 0:
        state = "positive_strengthening"
    elif current >= 0 and slope < 0:
        state = "positive_weakening"
    elif current < 0 and slope > 0:
        state = "negative_weakening"
    else:
        state = "negative_strengthening"

    return {
        "state":            state,
        "direction":        direction,
        "slope":            round(slope, 6),
        "slope_pct":        round(slope_pct, 2),
        "velocity":         velocity,
        "acceleration":     acceleration,
        "but_improving":    but_improving,
        "danger_increasing": danger_incr,
        "current":          round(current, 4),
        "first":            round(first, 4),
        "series_len":       n,
    }


def _lm_build_tf_orderflow_history(uid, exchange: str, market: str,  # noqa: C901
                                    symbol: str, analysis_source: str,
                                    tf: str, limit: int = 20) -> dict:
    """Build orderflow history for a single TF (Phase 11.1B Task 2 sub-function).

    Returns delta_series, cvd_series, series state analysis, and current
    market context (OI, liq, funding, L/S). No raw OHLC stored.

    CVD method labels (in order of quality):
      "candle_delta"                 — Binance taker_buy_quote per kline
      "aligned_orderflow_snapshots"  — LiveMonitorCandleOrderflow delta_net_sum
      "delta_proxy"                  — current-only 5m delta snapshot
      "unavailable"                  — no data at all
    """
    norm_src = _m._lm_normalize_analysis_source(analysis_source)
    eff_exch = norm_src if norm_src in _m._LM_SPECIFIC_ANALYSIS_SRCS else "binance"

    # ── Candle delta series ───────────────────────────────────────────────────
    candles, cvd_method = _lm_fetch_klines_with_delta(symbol, tf, limit=limit,
                                                       exchange=eff_exch)

    # Non-Binance fallback: LiveMonitorCandleOrderflow DB rows
    if cvd_method == "unavailable" and uid:
        try:
            from models import LiveMonitorCandleOrderflow as _LMCO11b
            db_rows = (
                _LMCO11b.query
                .filter_by(user_id=uid, exchange=eff_exch, market=market,
                           symbol=symbol, timeframe=tf, analysis_source=norm_src)
                .filter(_LMCO11b.flow_status != "no_samples")
                .order_by(_LMCO11b.open_time.desc())
                .limit(limit)
                .all()
            )
            if db_rows:
                candles = [{
                    "open_time":  r.open_time,
                    "close_time": r.close_time,
                    "delta":      float(r.delta_net_sum) if r.delta_net_sum is not None else None,
                    "sample_count": r.sample_count,
                } for r in reversed(db_rows)]
                cvd_method = "aligned_orderflow_snapshots"
        except Exception:
            pass

    # ── Build delta and CVD series ────────────────────────────────────────────
    delta_series: list = []
    cvd_series:   list = []
    cvd_r = 0.0
    for c in candles:
        d = c.get("delta")
        if d is not None:
            delta_series.append(round(float(d), 2))
            cvd_r += float(d)
            cvd_series.append(round(cvd_r, 2))

    if not delta_series:
        # Final fallback: current 5m delta snapshot as proxy
        if eff_exch == "binance":
            d_now, _ = _m._lm_delta_get("binance", symbol)
        else:
            d_now, _ = _m._lm_mx_get_delta(eff_exch, symbol)
            if not d_now:
                d_now, _ = _m._lm_delta_get("binance", symbol)
        if d_now:
            v = float(d_now.get("delta_5m", 0))
            delta_series = [v]
            cvd_series   = [v]
            cvd_method   = "delta_proxy"

    # ── Series state analysis ─────────────────────────────────────────────────
    delta_state = _lm_build_series_orderflow_state(delta_series)
    cvd_state   = _lm_build_series_orderflow_state(cvd_series)

    # ── Current market context (OI, liq, funding, L/S) ───────────────────────
    oi_d      = _m._lm_fetch_exchange_open_interest(eff_exch, symbol)
    oi_chg_d  = _m._lm_fetch_exchange_oi_change(eff_exch, symbol)
    fund_d    = _m._lm_fetch_exchange_funding(eff_exch, symbol)
    ls_d      = _m._lm_fetch_exchange_long_short(eff_exch, symbol)

    if eff_exch == "binance":
        liq_d, _ = _m._lm_liq_get("binance", symbol)
    else:
        liq_d, _ = _m._lm_mx_get_liq(eff_exch, symbol)
        if not liq_d:
            liq_d, _ = _m._lm_liq_get("binance", symbol)

    long_liq  = float((liq_d or {}).get("long_liq_usd_5m",  0))
    short_liq = float((liq_d or {}).get("short_liq_usd_5m", 0))

    delta_cur  = delta_series[-1] if delta_series else 0.0
    delta_prev = delta_series[-2] if len(delta_series) >= 2 else 0.0
    cvd_cur    = cvd_series[-1]   if cvd_series   else 0.0
    cvd_prev   = cvd_series[-2]   if len(cvd_series) >= 2 else 0.0

    return {
        "tf":              tf,
        "candle_count":    len(candles),
        "data_available":  bool(delta_series),
        "cvd_method":      cvd_method,
        # Compact series (last 20 values)
        "delta_series":    delta_series[-20:],
        "cvd_series":      cvd_series[-20:],
        # Delta scalar summary
        "delta_current":   round(delta_cur, 2),
        "delta_previous":  round(delta_prev, 2),
        "delta_avg":       round(sum(delta_series) / len(delta_series), 2) if delta_series else 0.0,
        "delta_state":     delta_state,
        # CVD scalar summary
        "cvd_current":     round(cvd_cur, 2),
        "cvd_previous":    round(cvd_prev, 2),
        "cvd_change_abs":  round(cvd_cur - cvd_prev, 2),
        "cvd_change_pct":  round((cvd_cur - cvd_prev) / abs(cvd_prev) * 100, 2)
                           if abs(cvd_prev) > 1e-12 else 0.0,
        "cvd_state":       cvd_state,
        # Market context
        "open_interest": {
            "oi_usd":        round(float((oi_d or {}).get("oi_usd") or 0), 2),
            "oi_change_pct": float((oi_chg_d or {}).get("change_pct") or 0),
            "oi_direction":  (oi_chg_d or {}).get("direction", ""),
            "available":     bool((oi_d or {}).get("available")),
        },
        "liquidations": {
            "long_liq_usd_5m":  round(long_liq,  2),
            "short_liq_usd_5m": round(short_liq, 2),
            "liq_bias":         round(short_liq - long_liq, 2),
            "available":        liq_d is not None,
        },
        "funding": {
            "rate_pct":  round(float((fund_d or {}).get("rate") or 0) * 100, 6),
            "bias":      (fund_d or {}).get("bias", "neutral"),
            "available": bool((fund_d or {}).get("available")),
        },
        "long_short": {
            "long_pct":  float((ls_d or {}).get("long_pct")  or 0),
            "short_pct": float((ls_d or {}).get("short_pct") or 0),
            "ls_ratio":  float((ls_d or {}).get("ls_ratio")  or 0),
            "available": bool((ls_d or {}).get("available")),
        },
    }


def _lm_build_mtf_orderflow_history(uid, exchange: str, market: str,
                                     symbol: str, analysis_source: str,
                                     parent_tf: str,
                                     limit_per_tf: int = 20) -> dict:
    """Build multi-TF orderflow history matrix (Phase 11.1B Task 2).

    Builds history for all child TFs of parent_tf. A 4h parent uses 1h/30m/15m/5m.
    Returns compact dict — delta series, CVD series, series states, market context.
    Stores no raw OHLC or large arrays beyond the 20-window series.
    """
    now_ts    = int(time.time())
    child_tfs = _lm_child_orderflow_timeframes(parent_tf)

    timeframes: dict = {}
    for tf in child_tfs:
        try:
            timeframes[tf] = _lm_build_tf_orderflow_history(
                uid, exchange, market, symbol, analysis_source, tf,
                limit=limit_per_tf,
            )
        except Exception as _etf:
            timeframes[tf] = {
                "tf": tf, "data_available": False,
                "cvd_method": "unavailable",
                "error": str(_etf)[:120],
            }

    available_tfs = [tf for tf, d in timeframes.items() if d.get("data_available")]

    return {
        "parent_tf":     parent_tf,
        "child_tfs":     child_tfs,
        "available_tfs": available_tfs,
        "timeframes":    timeframes,
        "computed_at":   now_ts,
    }


def _lm_build_mtf_history_summary(mtf_history: dict) -> dict:
    """Extract compact per-TF summary from mtf_orderflow_history.

    Used to attach lightweight context to each candidate without duplicating
    the full series arrays.
    """
    summary: dict = {}
    for tf, tf_d in (mtf_history or {}).get("timeframes", {}).items():
        if not isinstance(tf_d, dict):
            continue
        summary[tf] = {
            "data_available":    tf_d.get("data_available", False),
            "cvd_method":        tf_d.get("cvd_method", "unavailable"),
            "cvd_state":         (tf_d.get("cvd_state") or {}).get("state", ""),
            "cvd_direction":     (tf_d.get("cvd_state") or {}).get("direction", ""),
            "cvd_but_improving": (tf_d.get("cvd_state") or {}).get("but_improving", False),
            "delta_state":       (tf_d.get("delta_state") or {}).get("state", ""),
            "delta_direction":   (tf_d.get("delta_state") or {}).get("direction", ""),
            "delta_danger":      (tf_d.get("delta_state") or {}).get("danger_increasing", False),
            "oi_direction":      (tf_d.get("open_interest") or {}).get("oi_direction", ""),
            "funding_bias":      (tf_d.get("funding") or {}).get("bias", ""),
            "liq_bias":          float((tf_d.get("liquidations") or {}).get("liq_bias", 0)),
            "candle_count":      tf_d.get("candle_count", 0),
        }
    return summary
