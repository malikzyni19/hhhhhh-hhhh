import math
import os
import time
import traceback
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import requests as req
from flask import Flask, jsonify, redirect, render_template, request, session, url_for

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "zyni-fallback-secret")

@app.after_request
def no_cache(r):
    r.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    r.headers["Pragma"] = "no-cache"
    r.headers["Expires"] = "0"
    return r

APP_PASSWORD = os.environ.get("APP_PASSWORD", "Ulta8900")

# Binance API endpoints.
# fapi.binance.com (futures) may be geo-blocked in some Replit regions (HTTP 451).
# We try the futures API first; if it fails we fall back to the geo-safe spot mirror.
BINANCE_SPOT_API    = "https://api.binance.com"
BINANCE_FUTURES_API = "https://fapi.binance.com"
SPOT_API            = "https://data-api.binance.vision"  # geo-safe spot mirror (fallback)

# Complete set of USDT perpetual futures symbols actively traded on Binance
PERP_SYMBOLS: frozenset = frozenset([
    "1000BONKUSDT","1000BTTUSDT","1000FLOKIUSDT","1000LUNCUSDT","1000PEPEUSDT",
    "1000RATSUSDT","1000SHIBUSDT","1000XECUSDT","1INCHUSDT","AAVEUSDT",
    "ACHUSDT","ADAUSDT","AGIXUSDT","AGLDUSDT","AKROUSDT","ALGOUSDT","ALPHAUSDT",
    "AMBUSDT","ANKRUSDT","ANTUSDT","APEUSDT","APTUSDT","ARBUSDT","ARDRUSDT",
    "ARKMUSDT","ASTRUSDT","ATAUSDT","ATOMUSDT","AUCTIONUSDT","AVAXUSDT",
    "AXLUSDT","AXSUSDT","BADGERUSDT","BALUSDT","BANDUSDT","BATUSDT","BCHUSDT",
    "BELUSDT","BIGTIMEUSDT","BAKEUSDT","BLZUSDT","BLURUSDT","BNBUSDT","BNXUSDT",
    "BOBAUSDT","BONKUSDT","BSVUSDT","BSWUSDT","BTCDOMUSDT","BTCUSDT",
    "BUSDUSDT","C98USDT","CAKEUSDT","CELOUSDT","CELRUSDT","CFXUSDT","CHZUSDT",
    "COMBINEUSDT","COMPUSDT","COTIUSDT","CRVUSDT","CTSIUSDT","CVCUSDT",
    "CYBERUSDT","DARUSDT","DEFIUSDT","DENTUSDT","DGBUSDT","DODOUSDT","DOGEUSDT",
    "DOTUSDT","DUSKUSDT","EDUUSDT","EIGENUSDT","ENAUSDT","ENJUSDT","EOSUSDT",
    "ETCUSDT","ETHFIUSDT","ETHUSDT","FETUSDT","FILUSDT","FLMUSDT","FLOWUSDT",
    "FLUXUSDT","FORTHUSDT","FTMUSDT","GALAUSDT","GALUSDT","GASUSDT","GLMUSDT",
    "GMXUSDT","GRTUSDT","GUNUSDT","HBARUSDT","HFTUSDT","HIGHUSDT","HOOKUSDT",
    "HOTUSDT","HNTUSDT","ICPUSDT","ICXUSDT","IDEXUSDT","IMXUSDT","INJUSDT",
    "IOTAUSDT","IOTXUSDT","IOSTUSDT","JASMYUSDT","JSTUSDT","JUPUSDT","KAVAUSDT",
    "KEYUSDT","KLAYUSDT","KNCUSDT","LDOUSDT","LEVERUSDT","LINAUSDT","LINKUSDT",
    "LITUSDT","LOOKSUSDT","LPTUSDT","LQTYUSDT","LRCUSDT","LTCUSDT","LUNA2USDT",
    "LUNAUSDT","MAGICUSDT","MANAUSDT","MASKUSDT","MATICUSDT","MAVUSDT",
    "MEMEUSDT","MNTUSDT","MKRUSDT","MOVRUSDT","MTLUSDT","MULTIUSDT","NEARUSDT",
    "NEOUSDT","NKNUSDT","NOTUSDT","OCEANUSDT","OGNUSDT","ONTUSDT","OPUSDT",
    "ORBSUSDT","ORDIUSDT","OXTUSDT","PENDLEUSDT","PEPEUSDT","PEOPLEUSDT",
    "PERPUSDT","POWRUSDT","PYTHUSDT","QTUMUSDT","RAYUSDT","RDNTUSDT","REEFUSDT",
    "REIUSDT","RENDERUSDT","REZUSDT","RLCUSDT","RNDRUSDT","ROSEUSDT","RSRUSDT",
    "RUNEUSDT","RVNUSDT","SANDUSDT","SCUSDT","SEIUSDT","SFPUSDT","SKLUSDT",
    "SNXUSDT","SOLUSDT","SRMUSDT","STGUSDT","STORJUSDT","STRKUSDT","STXUSDT",
    "SUIUSDT","SUSHIUSDT","SXPUSDT","THETAUSDT","TIAUSDT","TNXPUSDT","TONUSDT",
    "TRBUSDT","TRUUSDT","TRXUSDT","TUSDT","UNIUSDT","UNFIUSDT","USDCUSDT",
    "USTCUSDT","VETUSDT","VGXUSDT","WAVESUSDT","WIFUSDT","WLDUSDT","WOOUSDT",
    "XEMUSDT","XLMUSDT","XMRUSDT","XRPUSDT","XTZUSDT","XVSUSDT","YFIUSDT",
    "ZECUSDT","ZENUSDT","ZILUSDT","ZKUSDT","ETHUSDT","BNBUSDT","SOLUSDT",
    # common spot pairs also on futures without special prefix
    "ADAUSDT","AVAXUSDT","DOTUSDT","LINKUSDT","LTCUSDT","BCHUSDT","ATOMUSDT",
    "FILUSDT","AAVEUSDT","COMPUSDT","MKRUSDT","CRVUSDT","SUSHIUSDT","SNXUSDT",
])

PAIR_CACHE: Dict[str, Any] = {
    "spot": {"ts": 0, "pairs": []},
    "perpetual": {"ts": 0, "pairs": []},
}
ROUND_ROBIN_STATE: Dict[str, int] = {"index": 0}

# True ATH/ATL cache — keyed by "symbol:market", TTL 4 hours
ATH_ATL_CACHE: Dict[str, Any] = {}
ATH_ATL_CACHE_TTL = 4 * 3600

# ============================================================
# Utilities
# ============================================================

def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def fmt_price(p: float) -> str:
    if p >= 1000:
        return f"{p:.2f}"
    if p >= 1:
        return f"{p:.4f}"
    if p >= 0.01:
        return f"{p:.6f}"
    return f"{p:.8f}"


def fmt_vol(v: float) -> str:
    if v >= 1e9:
        return f"{v/1e9:.2f}B"
    if v >= 1e6:
        return f"{v/1e6:.2f}M"
    if v >= 1e3:
        return f"{v/1e3:.2f}K"
    return f"{v:.0f}"


def pct(a: float, b: float) -> float:
    if b == 0:
        return 0.0
    return abs(a - b) / abs(b) * 100.0


def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def returns_from_close(close: List[float]) -> np.ndarray:
    if len(close) < 2:
        return np.array([])
    arr = np.array(close, dtype=float)
    prev = arr[:-1]
    nxt = arr[1:]
    return np.where(prev != 0, (nxt - prev) / prev, 0.0)


# ============================================================
# Indicators / structure
# ============================================================

def calc_ema(data: List[float], period: int) -> List[Optional[float]]:
    r: List[Optional[float]] = [None] * len(data)
    if len(data) < period or period <= 0:
        return r
    r[period - 1] = sum(data[:period]) / period
    m = 2.0 / (period + 1)
    for i in range(period, len(data)):
        prev = r[i - 1] if r[i - 1] is not None else data[i - 1]
        r[i] = data[i] * m + prev * (1 - m)
    return r


def calc_rsi(close: List[float], period: int = 14) -> List[Optional[float]]:
    r: List[Optional[float]] = [None] * len(close)
    if len(close) < period + 1:
        return r
    ag = 0.0
    al = 0.0
    for i in range(1, period + 1):
        d = close[i] - close[i - 1]
        if d > 0:
            ag += d
        else:
            al -= d
    ag /= period
    al /= period
    r[period] = 100.0 if al == 0 else 100 - 100 / (1 + ag / al)
    for i in range(period + 1, len(close)):
        d = close[i] - close[i - 1]
        ag = (ag * (period - 1) + max(d, 0)) / period
        al = (al * (period - 1) + max(-d, 0)) / period
        r[i] = 100.0 if al == 0 else 100 - 100 / (1 + ag / al)
    return r


def calc_atr(high: List[float], low: List[float], close: List[float], period: int = 14) -> List[Optional[float]]:
    tr = [0.0] * len(close)
    for i in range(len(close)):
        if i == 0:
            tr[i] = high[i] - low[i]
        else:
            tr[i] = max(high[i] - low[i], abs(high[i] - close[i - 1]), abs(low[i] - close[i - 1]))
    return calc_ema(tr, period)


def detect_pivots(high: List[float], low: List[float], left: int, right: int) -> Tuple[List[bool], List[bool]]:
    n = len(high)
    ph = [False] * n
    pl = [False] * n
    for i in range(left, n - right):
        is_ph = all(high[i] > high[j] for j in range(i - left, i + right + 1) if j != i)
        is_pl = all(low[i] < low[j] for j in range(i - left, i + right + 1) if j != i)
        ph[i] = is_ph
        pl[i] = is_pl
    return ph, pl


def detect_structure(high: List[float], low: List[float], close: List[float], i_len: int, s_len: int) -> Tuple[int, int]:
    ph_i, pl_i = detect_pivots(high, low, i_len, i_len)
    ph_s, pl_s = detect_pivots(high, low, s_len, s_len)
    itrend = 0
    trend = 0
    upP: List[float] = []
    dnP: List[float] = []
    supP: List[float] = []
    sdnP: List[float] = []
    upL: List[float] = []
    dnL: List[float] = []
    supL: List[float] = []
    sdnL: List[float] = []
    start = s_len * 2 + 2
    for i in range(start, len(close)):
        if i - i_len >= 0 and ph_i[i - i_len]:
            upP.insert(0, high[i - i_len])
            upL.insert(0, high[i - i_len])
        if i - i_len >= 0 and pl_i[i - i_len]:
            dnP.insert(0, low[i - i_len])
            dnL.insert(0, low[i - i_len])
        if i - s_len >= 0 and ph_s[i - s_len]:
            supP.insert(0, high[i - s_len])
            supL.insert(0, high[i - s_len])
        if i - s_len >= 0 and pl_s[i - s_len]:
            sdnP.insert(0, low[i - s_len])
            sdnL.insert(0, low[i - s_len])

        if upP and len(dnL) > 1 and close[i] > upP[0]:
            itrend = 1
            upP.clear()
        if dnP and len(upL) > 1 and close[i] < dnP[0]:
            itrend = -1
            dnP.clear()
        if supP and len(sdnL) > 1 and close[i] > supP[0]:
            trend = 1
            supP.clear()
        if sdnP and len(supL) > 1 and close[i] < sdnP[0]:
            trend = -1
            sdnP.clear()
    return itrend, trend


# ============================================================
# FVG / OB engines
# ============================================================

def fvg_touch_depth(direction: str, top: float, bottom: float, bar_high: float, bar_low: float) -> float:
    size = max(top - bottom, 1e-10)
    if direction == "bullish":
        if bar_low > top:
            return 0.0
        touched_price = min(top, max(bottom, bar_low))
        return clamp((top - touched_price) / size, 0.0, 1.0)
    if bar_high < bottom:
        return 0.0
    touched_price = max(bottom, min(top, bar_high))
    return clamp((touched_price - bottom) / size, 0.0, 1.0)


def touch_depth_label(depth: float) -> str:
    if depth <= 0:
        return "untouched"
    if depth <= 0.25:
        return "edge"
    if depth <= 0.60:
        return "mid"
    return "deep"


def detect_fvgs(o: List[float], h: List[float], l: List[float], c: List[float], v: List[float], tf: str) -> List[Dict[str, Any]]:
    fvgs: List[Dict[str, Any]] = []
    n = len(c)
    for i in range(2, n):
        bull = False
        bear = False
        top = 0.0
        bottom = 0.0

        if l[i] > h[i - 2]:
            bull = True
            top = l[i]
            bottom = h[i - 2]
        elif h[i] < l[i - 2]:
            bear = True
            top = l[i - 2]
            bottom = h[i]
        else:
            continue

        vU, vM, vL = v[i], v[i - 1], v[i - 2]
        avg = (vU + vM + vL) / 3.0 if (vU + vM + vL) else 0.0
        uA = vU > avg
        mA = vM > avg
        lA = vL > avg
        is_valid = (mA != uA) and (mA != lA)
        is_bag = (c[i] > h[i - 1]) if bull else (c[i] < l[i - 1])

        touches = 0
        max_depth = 0.0
        first_touch_bar = None
        mitigated = False
        for j in range(i + 1, n):
            touched = l[j] <= top and h[j] >= bottom
            if touched:
                depth = fvg_touch_depth("bullish" if bull else "bearish", top, bottom, h[j], l[j])
                max_depth = max(max_depth, depth)
                touches += 1
                if first_touch_bar is None:
                    first_touch_bar = j
            if bull and c[j] < bottom:
                mitigated = True
                break
            if bear and c[j] > top:
                mitigated = True
                break

        fvgs.append({
            "top": top,
            "bottom": bottom,
            "mid": (top + bottom) / 2,
            "size": top - bottom,
            "bar": i,
            "age": (n - 1) - i,
            "direction": "bullish" if bull else "bearish",
            "isValid": is_valid,
            "isBag": is_bag,
            "timeframe": tf,
            "touches": touches,
            "firstTouchBar": first_touch_bar,
            "mitigated": mitigated,
            "untouched": touches == 0,
            "onceTouched": touches == 1,
            "touchDepth": max_depth,
            "touchDepthLabel": touch_depth_label(max_depth),
        })
    return [f for f in fvgs if not f["mitigated"]][-30:]


def detect_obs(o, h, l, c, v, i_len, s_len, max_ob=5, ob_positioning="Precise", ob_mitigation="Absolute"):
    """
    Order Block detection — audited line-by-line against Pine Script drawVOB().

    CRITICAL DIFFERENCE from previous version:
    Pine Script finds the extreme candle (lowest low / highest high), then applies
    a +1 offset to use the PREVIOUS candle (one bar earlier in time) for zone
    boundary calculation (hl2) and volume. The zone bottom (bullish) or top (bearish)
    still uses the actual extreme value.

    Pine reference:
      int iU = obj.l.indexof(obj.l.min()) + 1   <- the +1 offset
      obj.top.unshift(pos[iU])                   <- hl2 from offset candle
      obj.btm.unshift(obj.l.min())               <- actual minimum low value
      obj.cV.unshift(b.v[iU])                    <- volume from offset candle
    """
    n = len(c)
    ph, pl = detect_pivots(h, l, i_len, i_len)
    obs = []

    upP, upB, upL = [], [], []
    dnP, dnB, dnL = [], [], []

    start = max(i_len * 2 + 2, s_len + 2)

    for i in range(start, n):
        if i - i_len >= 0 and ph[i - i_len]:
            upP.insert(0, h[i - i_len])
            upB.insert(0, i - i_len)
            upL.insert(0, h[i - i_len])
        if i - i_len >= 0 and pl[i - i_len]:
            dnP.insert(0, l[i - i_len])
            dnB.insert(0, i - i_len)
            dnL.insert(0, l[i - i_len])

        # ── INTERNAL BULLISH BREAK → Create Bullish OB ──
        if upP and len(dnL) > 1 and c[i] > upP[0] and (i == start or c[i - 1] <= upP[0]):
            pivot_bar    = upB[0] if upB else i - 10
            # Pine scans from pivot+1 to break bar (excludes pivot itself)
            search_start = max(0, pivot_bar + 1)
            search_end   = i + 1  # include break bar

            if search_end > search_start:
                # Step 1: Find candle with lowest low
                min_idx = search_start
                for j in range(search_start, search_end):
                    if l[j] < l[min_idx]:
                        min_idx = j

                # Step 2: +1 offset — Pine uses the candle ONE BAR EARLIER for hl2/volume
                # In Pine's reversed array: +1 = older = one bar to the left in forward time
                ob_source = max(search_start, min_idx - 1)

                # Step 3: Zone boundaries
                hl2_val   = (h[ob_source] + l[ob_source]) / 2.0
                ohlc4_val = (o[ob_source] + h[ob_source] + l[ob_source] + c[ob_source]) / 4.0
                hlcc4_val = (h[ob_source] + l[ob_source] + c[ob_source] + c[ob_source]) / 4.0

                if ob_positioning == "Full":
                    ob_top = h[ob_source]
                elif ob_positioning == "Middle":
                    ob_top = ohlc4_val
                else:
                    ob_top = hl2_val

                ob_bottom = l[min_idx]  # actual minimum low (NOT from source candle)
                ob_avg    = (ob_top + ob_bottom) / 2.0

                # Step 4: Precise adjustment
                if ob_positioning == "Precise":
                    body_low = min(c[ob_source], o[ob_source])
                    if ob_avg < body_low and ob_top > hlcc4_val:
                        ob_top = ob_avg
                        ob_avg = (ob_top + ob_bottom) / 2.0

                # Step 5: Volume and direction from SOURCE candle (the +1 offset candle)
                candle_dir = 1 if c[ob_source] > o[ob_source] else -1
                total_v    = v[ob_source]
                buy_v      = total_v * (0.6 if candle_dir == 1 else 0.4)
                sell_v     = total_v - buy_v

                if ob_top > ob_bottom:
                    obs.append({
                        "top": ob_top,
                        "bottom": ob_bottom,
                        "avg": ob_avg,
                        "bar": min_idx,
                        "sourceBar": ob_source,
                        "volume": total_v,
                        "buyVolume": buy_v,
                        "sellVolume": sell_v,
                        "type": "bullish",
                        "candleDir": candle_dir,
                        "formationRange": max(ob_top - ob_bottom, 1e-10),
                    })

            upP.clear()
            upB.clear()

        # ── INTERNAL BEARISH BREAK → Create Bearish OB ──
        if dnP and len(upL) > 1 and c[i] < dnP[0] and (i == start or c[i - 1] >= dnP[0]):
            pivot_bar    = dnB[0] if dnB else i - 10
            search_start = max(0, pivot_bar + 1)
            search_end   = i + 1

            if search_end > search_start:
                # Step 1: Find candle with highest high
                max_idx = search_start
                for j in range(search_start, search_end):
                    if h[j] > h[max_idx]:
                        max_idx = j

                # Step 2: +1 offset
                ob_source = max(search_start, max_idx - 1)

                # Step 3: Zone boundaries
                hl2_val   = (h[ob_source] + l[ob_source]) / 2.0
                ohlc4_val = (o[ob_source] + h[ob_source] + l[ob_source] + c[ob_source]) / 4.0
                hlcc4_val = (h[ob_source] + l[ob_source] + c[ob_source] + c[ob_source]) / 4.0

                ob_top    = h[max_idx]  # actual maximum high (NOT from source candle)

                if ob_positioning == "Full":
                    ob_bottom = l[ob_source]
                elif ob_positioning == "Middle":
                    ob_bottom = ohlc4_val
                else:
                    ob_bottom = hl2_val

                ob_avg = (ob_top + ob_bottom) / 2.0

                # Step 4: Precise adjustment
                if ob_positioning == "Precise":
                    body_high = max(c[ob_source], o[ob_source])
                    if ob_avg > body_high and ob_bottom < hlcc4_val:
                        ob_bottom = ob_avg
                        ob_avg    = (ob_top + ob_bottom) / 2.0

                # Step 5: Volume and direction from SOURCE candle
                candle_dir = 1 if c[ob_source] > o[ob_source] else -1
                total_v    = v[ob_source]
                sell_v     = total_v * (0.6 if candle_dir == -1 else 0.4)
                buy_v      = total_v - sell_v

                if ob_top > ob_bottom:
                    obs.append({
                        "top": ob_top,
                        "bottom": ob_bottom,
                        "avg": ob_avg,
                        "bar": max_idx,
                        "sourceBar": ob_source,
                        "volume": total_v,
                        "buyVolume": buy_v,
                        "sellVolume": sell_v,
                        "type": "bearish",
                        "candleDir": candle_dir,
                        "formationRange": max(ob_top - ob_bottom, 1e-10),
                    })

            dnP.clear()
            dnB.clear()

    # ── Mitigate / invalidate OBs ──
    active = []
    max_vol = max(v[-100:]) if len(v) >= 100 else max(v) if v else 1.0

    for ob in obs:
        mitigated = False
        for j in range(ob["bar"] + 1, n):
            if ob_mitigation == "Middle":
                trigger = ob["avg"]
            else:  # Absolute
                trigger = ob["bottom"] if ob["type"] == "bullish" else ob["top"]

            if ob["type"] == "bullish" and c[j] < trigger:
                mitigated = True
                break
            if ob["type"] == "bearish" and c[j] > trigger:
                mitigated = True
                break

        if mitigated:
            continue

        vol_score = clamp((ob["volume"] / max(max_vol, 1e-10)) * 100, 0, 100)
        side_dom  = (ob["buyVolume"] / max(ob["volume"], 1e-10) * 100) if ob["type"] == "bullish" else (ob["sellVolume"] / max(ob["volume"], 1e-10) * 100)
        freshness = clamp(100 - ((n - 1 - ob["bar"]) * 4), 10, 100)
        strength  = round(clamp(vol_score * 0.45 + side_dom * 0.35 + freshness * 0.20, 1, 100), 1)
        ob["strengthPct"]   = strength
        ob["strengthLabel"] = "Strong" if strength >= 80 else "Good" if strength >= 60 else "Medium" if strength >= 40 else "Weak"
        active.append(ob)

    return active[-max_ob:]


def compute_overlap_pct(a_bottom: float, a_top: float, b_bottom: float, b_top: float) -> float:
    inter = max(0.0, min(a_top, b_top) - max(a_bottom, b_bottom))
    base = max(min(a_top - a_bottom, b_top - b_bottom), 1e-10)
    return clamp(inter / base * 100.0, 0.0, 100.0)


def filter_fvg(fvg: Dict[str, Any], obs: List[Dict[str, Any]], price: float, settings: Dict[str, Any]) -> bool:
    if settings.get("useFvgValidOnly") and not fvg["isValid"]:
        return False

    if settings.get("useFvgState"):
        state = settings.get("fvgState", "all")
        if state == "fresh" and not (settings["fvgAgeMin"] <= fvg["age"] <= settings["fvgAgeMax"]):
            return False
        if state == "untouched" and not fvg["untouched"]:
            return False
        if state == "once_touched" and not fvg["onceTouched"]:
            return False
        if state == "old_untouched" and not (fvg["untouched"] and settings["fvgAgeMin"] <= fvg["age"] <= settings["fvgAgeMax"]):
            return False
        if state == "active_retested" and fvg["touches"] < 1:
            return False

    if settings.get("useFvgAgeRange"):
        if not (settings["fvgAgeMin"] <= fvg["age"] <= settings["fvgAgeMax"]):
            return False

    if settings.get("useFvgDistance"):
        dist_pct = abs(price - fvg["mid"]) / max(price, 1e-10) * 100
        if dist_pct > settings["fvgMaxDistancePct"]:
            return False

    if settings.get("useFvgTouchDepth"):
        wanted = settings.get("fvgTouchDepth", "any")
        if wanted != "any" and fvg["touchDepthLabel"] != wanted:
            return False

    if settings.get("useFvgObOverlap"):
        mode = settings.get("fvgObOverlapMode", "same_direction")
        min_overlap = settings.get("fvgObMinOverlapPct", 20.0)
        matched = False
        for ob in obs:
            if mode == "same_direction" and ob["type"] != fvg["direction"]:
                continue
            overlap = compute_overlap_pct(fvg["bottom"], fvg["top"], ob["bottom"], ob["top"])
            if overlap >= min_overlap:
                matched = True
                break
        if not matched:
            return False

    return True


# ============================================================
# Scan modules
# ============================================================

def detect_compression(high: List[float], low: List[float], close: List[float], lookback: int, max_pct: float) -> Tuple[bool, Dict[str, float]]:
    if len(close) < lookback:
        return False, {}
    recent_high = max(high[-lookback:])
    recent_low = min(low[-lookback:])
    price = close[-1]
    range_pct = ((recent_high - recent_low) / max(price, 1e-10)) * 100
    info = {"high": recent_high, "low": recent_low, "rangePct": range_pct}
    return 0.01 < range_pct <= max_pct, info


def detect_trend_mode(close: List[float], volume: List[float]) -> Dict[str, Any]:
    ema20 = calc_ema(close, 20)
    ema50 = calc_ema(close, 50)
    price = close[-1]
    e20 = ema20[-1] if ema20[-1] is not None else price
    e50 = ema50[-1] if ema50[-1] is not None else price
    avg_vol = float(np.mean(volume[-20:])) if len(volume) >= 20 else float(np.mean(volume))
    rel_vol = volume[-1] / max(avg_vol, 1e-10)
    bullish = price > e20 > e50
    bearish = price < e20 < e50
    return {
        "bullish": bullish,
        "bearish": bearish,
        "highVolumeTrend": rel_vol >= 1.5 and (bullish or bearish),
        "relVol": rel_vol,
        "ema20": e20,
        "ema50": e50,
    }


def classify_btc_correlation(symbol_closes: List[float], btc_closes: List[float], lookback: int) -> Tuple[float, str]:
    a = returns_from_close(symbol_closes[-(lookback + 1):])
    b = returns_from_close(btc_closes[-(lookback + 1):])
    if len(a) < 5 or len(b) < 5:
        return 0.0, "unknown"
    m = min(len(a), len(b))
    corr = float(np.corrcoef(a[-m:], b[-m:])[0, 1]) if m >= 5 else 0.0
    if math.isnan(corr):
        corr = 0.0
    if corr >= 0.60:
        label = "correlated"
    elif -0.30 <= corr <= 0.30:
        label = "non_correlated"
    else:
        label = "mixed"
    return corr, label


# ──────────────────────────────────────────────
# FIB helpers
# ──────────────────────────────────────────────

def _build_fib(a, b, method, bullish):
    """Build Fib retracement levels from leg A→B."""
    rng = abs(b - a)
    if rng <= 0:
        return None
    retraces = [0.5, 0.618, 0.705, 0.786]
    levels = {}
    for r in retraces:
        levels[str(r)] = (b - rng * r) if bullish else (b + rng * r)
    return {"bullish": bullish, "a": a, "b": b, "levels": levels, "range": rng, "method": method}


def find_swing_pivots(high, low, left=5, right=5):
    """
    Find swing pivot highs and lows using left/right confirmation.
    Returns list of (bar_index, price) tuples for highs and lows.
    """
    n = len(high)
    pivots_high = []
    pivots_low  = []
    for i in range(left, n - right):
        is_high = all(high[j] < high[i] for j in range(i - left, i + right + 1) if j != i)
        if is_high:
            pivots_high.append((i, high[i]))
        is_low = all(low[j] > low[i] for j in range(i - left, i + right + 1) if j != i)
        if is_low:
            pivots_low.append((i, low[i]))
    return pivots_high, pivots_low


def measure_impulse_strength(o, h, l, c, v, start_bar, end_bar):
    """
    Measure how strong/clean an impulse move is (0-100).
    Higher = cleaner, stronger move.
    """
    if end_bar <= start_bar or end_bar >= len(c):
        return 0
    is_bullish = c[end_bar] > c[start_bar]
    total_bars  = end_bar - start_bar
    if total_bars < 2:
        return 0

    directional_bars = 0
    total_body_size  = 0
    total_range      = 0
    consecutive_max  = 0
    consecutive_now  = 0

    for i in range(start_bar, end_bar + 1):
        body         = abs(c[i] - o[i])
        candle_range = max(h[i] - l[i], 1e-10)
        total_body_size += body
        total_range     += candle_range

        if (is_bullish and c[i] > o[i]) or (not is_bullish and c[i] < o[i]):
            directional_bars += 1
            consecutive_now  += 1
        else:
            consecutive_max = max(consecutive_max, consecutive_now)
            consecutive_now = 0

    consecutive_max = max(consecutive_max, consecutive_now)

    dir_score    = min(25, (directional_bars / max(total_bars, 1)) * 30)
    body_score   = min(25, (total_body_size / max(total_range, 1e-10)) * 30)
    consec_score = min(25, (consecutive_max  / max(total_bars, 1)) * 35)
    move_size    = abs(c[end_bar] - c[start_bar])
    eff_score    = min(25, (move_size / max(total_range, 1e-10)) * 30)

    return round(dir_score + body_score + consec_score + eff_score, 1)


def find_active_fib_leg(o, h, l, c, v, lookback=80, min_impulse_strength=35):
    """
    Find the CURRENT ACTIVE Fib leg using pure price action.
    Independent from SMC structure.
    """
    n = len(c)
    seg = min(lookback, n)
    if seg < 20:
        return None

    offset = n - seg
    seg_o  = o[offset:]
    seg_h  = h[offset:]
    seg_l  = l[offset:]
    seg_c  = c[offset:]
    seg_v  = v[offset:]

    pivot_highs, pivot_lows = find_swing_pivots(seg_h, seg_l, left=5, right=5)
    if not pivot_highs or not pivot_lows:
        return None

    current_price = seg_c[-1]
    candidates    = []

    for hi_idx in range(len(pivot_highs) - 1, max(-1, len(pivot_highs) - 8), -1):
        hi_bar, hi_price = pivot_highs[hi_idx]
        for lo_idx in range(len(pivot_lows) - 1, max(-1, len(pivot_lows) - 8), -1):
            lo_bar, lo_price = pivot_lows[lo_idx]

            if abs(hi_bar - lo_bar) < 3:
                continue

            if lo_bar < hi_bar:
                is_bullish = True
                leg_start, leg_end = lo_bar, hi_bar
                a_price, b_price   = lo_price, hi_price
            else:
                is_bullish = False
                leg_start, leg_end = hi_bar, lo_bar
                a_price, b_price   = hi_price, lo_price

            if abs(b_price - a_price) < 1e-10:
                continue

            # Freshness: leg must end within recent 60% of lookback
            if (len(seg_c) - 1 - leg_end) > lookback * 0.6:
                continue

            # Active: price must be retracing, not extending
            if is_bullish:
                if current_price >= b_price:
                    continue
                if any(seg_c[k] < a_price for k in range(leg_end + 1, len(seg_c))):
                    continue
            else:
                if current_price <= b_price:
                    continue
                if any(seg_c[k] > a_price for k in range(leg_end + 1, len(seg_c))):
                    continue

            strength = measure_impulse_strength(seg_o, seg_h, seg_l, seg_c, seg_v, leg_start, leg_end)
            if strength < min_impulse_strength:
                continue

            recency = leg_end / max(len(seg_c), 1)
            rng     = abs(b_price - a_price)
            score   = (strength * 0.4) + (recency * 100 * 0.35) + (min(rng / max(current_price, 1e-10) * 100, 20) * 0.25)

            candidates.append({
                "a": a_price, "b": b_price, "bullish": is_bullish,
                "leg_start": leg_start + offset, "leg_end": leg_end + offset,
                "impulse_strength": strength, "recency": round(recency, 3),
                "score": round(score, 1), "range": rng,
            })

    if not candidates:
        return None

    candidates.sort(key=lambda x: x["score"], reverse=True)
    best = candidates[0]
    fib  = _build_fib(best["a"], best["b"], "impulse_leg", best["bullish"])
    if fib:
        fib["impulse_strength"] = best["impulse_strength"]
        fib["leg_start"]        = best["leg_start"]
        fib["leg_end"]          = best["leg_end"]
        fib["recency"]          = best["recency"]
        fib["leg_score"]        = best["score"]
    return fib


def check_wick_rejection(o, h, l, c, fib_level, is_bullish_retracement, tolerance_pct=0.3):
    """
    Check if the most recent candle(s) show a wick rejection at a Fib level.
    Returns: (is_rejection: bool, rejection_strength: int 0-100)
    """
    n = len(c)
    if n < 2:
        return False, 0

    for offset in range(1, min(3, n)):
        idx = n - offset
        bar_o, bar_h, bar_l, bar_c = o[idx], h[idx], l[idx], c[idx]
        body_top     = max(bar_o, bar_c)
        body_bottom  = min(bar_o, bar_c)
        body_size    = max(body_top - body_bottom, 1e-10)
        total_range  = max(bar_h - bar_l, 1e-10)
        level_tol    = fib_level * (tolerance_pct / 100)

        if is_bullish_retracement:
            wick_below  = body_bottom - bar_l
            if not (bar_l <= fib_level + level_tol and bar_c > fib_level - level_tol):
                continue
            wick_ratio = wick_below / body_size
            body_ratio = body_size / total_range
            if wick_ratio >= 0.8:
                return True, min(100, int(wick_ratio * 40 + body_ratio * 30 + 30))
        else:
            wick_above = bar_h - body_top
            if not (bar_h >= fib_level - level_tol and bar_c < fib_level + level_tol):
                continue
            wick_ratio = wick_above / body_size
            body_ratio = body_size / total_range
            if wick_ratio >= 0.8:
                return True, min(100, int(wick_ratio * 40 + body_ratio * 30 + 30))

    return False, 0


def analyze_pair(symbol: str, candles: List[Dict[str, float]], tf: str, settings: Dict[str, Any], btc_closes: Optional[List[float]] = None, fib_candles: Optional[List[Dict[str, float]]] = None) -> Optional[Dict[str, Any]]:
    o = [x["open"] for x in candles]
    h = [x["high"] for x in candles]
    l = [x["low"] for x in candles]
    c = [x["close"] for x in candles]
    v = [x["volume"] for x in candles]
    n = len(c)
    if n < 80:
        return None

    price = c[-1]
    itrend, trend = detect_structure(h, l, c, settings["iLen"], settings["sLen"])
    rsi = calc_rsi(c, 14)
    atr = calc_atr(h, l, c, 14)
    current_rsi = rsi[-1] if rsi[-1] is not None else 50.0
    current_atr = atr[-1] if atr[-1] is not None else max((max(h[-14:]) - min(l[-14:])), 1e-10)

    obs = detect_obs(o, h, l, c, v, settings["iLen"], settings["sLen"])
    fvgs = detect_fvgs(o, h, l, c, v, tf)

    corr_value = None
    corr_label = None
    if settings.get("useBtcCorrelation") and btc_closes:
        corr_value, corr_label = classify_btc_correlation(c, btc_closes, settings.get("btcLookback", 60))
        mode = settings.get("btcCorrelationMode", "all")
        if mode == "correlated" and corr_label != "correlated":
            return None
        if mode == "non_correlated" and corr_label != "non_correlated":
            return None

    alerts: List[Dict[str, Any]] = []

    # FVG alerts
    for fvg in fvgs:
        if not filter_fvg(fvg, obs, price, settings):
            continue
        detail = (
            f'{fvg["direction"].upper()} '
            f'{"BAG" if fvg["isBag"] else ("VALID" if fvg["isValid"] else "FVG")} '
            f'| Age: {fvg["age"]} | Touch: {fvg["touchDepthLabel"]} '
            f'| Zone: {fmt_price(fvg["bottom"])} - {fmt_price(fvg["top"])}'
        )
        overlap_best = 0.0
        for ob in obs:
            overlap_best = max(overlap_best, compute_overlap_pct(fvg["bottom"], fvg["top"], ob["bottom"], ob["top"]))
        alerts.append({
            "setup": "FVG",
            "direction": fvg["direction"],
            "timeframe": tf,
            "detail": detail,
            "strength": 5 if fvg["isBag"] else 4 if fvg["isValid"] else 2,
            "meta": {
                "fvgAge": fvg["age"],
                "fvgTouchDepth": fvg["touchDepthLabel"],
                "fvgOverlapPct": round(overlap_best, 1),
            },
        })

    # ═══════════════════════════════════════════════════════════
    # Order Blocks — with TradingView-matching volume percentage
    # ═══════════════════════════════════════════════════════════

    # ── Volume format matching Pine Script exactly ──
    # Pine: math.round(vol / 1000000, 3) + "M"
    def _fmt_ob_vol(vv):
        if vv >= 1e9:
            return str(round(vv / 1e9, 3)) + "B"
        elif vv >= 1e6:
            return str(round(vv / 1e6, 3)) + "M"
        elif vv >= 1e3:
            return str(round(vv / 1e3, 3)) + "K"
        else:
            return str(round(vv))

    # ── Calculate volumePct SEPARATELY for bullish and bearish ──
    # Pine Script calls drawVOB separately for bull/bear — each pool sums to 100%
    bullish_obs = [ob for ob in obs if ob["type"] == "bullish"]
    bearish_obs = [ob for ob in obs if ob["type"] == "bearish"]

    bull_total_vol = sum(ob["volume"] for ob in bullish_obs)
    for ob in bullish_obs:
        ob["volumePct"]       = int((ob["volume"] / bull_total_vol) * 100) if bull_total_vol > 0 else 0
        ob["volumeFormatted"] = _fmt_ob_vol(ob["volume"])

    bear_total_vol = sum(ob["volume"] for ob in bearish_obs)
    for ob in bearish_obs:
        ob["volumePct"]       = int((ob["volume"] / bear_total_vol) * 100) if bear_total_vol > 0 else 0
        ob["volumeFormatted"] = _fmt_ob_vol(ob["volume"])

    # ── Distance helper ──
    def _ob_distance_pct(ob):
        if ob["type"] == "bullish":
            if price < ob["bottom"]:
                return 999999.0
            if ob["bottom"] <= price <= ob["top"]:
                return 0.0
            return ((price - ob["top"]) / max(price, 1e-10)) * 100.0
        else:
            if price > ob["top"]:
                return 999999.0
            if ob["bottom"] <= price <= ob["top"]:
                return 0.0
            return ((ob["bottom"] - price) / max(price, 1e-10)) * 100.0

    # ── Consolidation helpers ──
    def _body_overlap_ratio(j, ob):
        body_top    = max(o[j], c[j])
        body_bottom = min(o[j], c[j])
        overlap     = max(0.0, min(body_top, ob["top"]) - max(body_bottom, ob["bottom"]))
        body_size   = max(abs(c[j] - o[j]), 1e-10)
        return overlap / body_size

    def _body_near_zone(j, ob, tol_pct):
        body_top    = max(o[j], c[j])
        body_bottom = min(o[j], c[j])
        if body_bottom <= ob["top"] and body_top >= ob["bottom"]:
            return True
        ref_price = max(abs(c[j]), 1e-10)
        if body_bottom > ob["top"]:
            dist_pct = ((body_bottom - ob["top"]) / ref_price) * 100.0
            return dist_pct <= tol_pct
        if body_top < ob["bottom"]:
            dist_pct = ((ob["bottom"] - body_top) / ref_price) * 100.0
            return dist_pct <= tol_pct
        return False

    def _ob_consol_consecutive(ob, needed, tol_pct):
        consecutive = 0
        for j in range(n - 1, -1, -1):
            if _body_overlap_ratio(j, ob) >= 0.35 or _body_near_zone(j, ob, tol_pct):
                consecutive += 1
            else:
                break
            if consecutive >= needed:
                break
        return consecutive

    # ── Select nearest OB per direction ──
    bullish_obs.sort(key=lambda ob: (_ob_distance_pct(ob), -(ob.get("volumePct", 0)), -ob.get("bar", 0)))
    bearish_obs.sort(key=lambda ob: (_ob_distance_pct(ob), -(ob.get("volumePct", 0)), -ob.get("bar", 0)))

    nearest_obs = []
    if bullish_obs and _ob_distance_pct(bullish_obs[0]) < 999999.0:
        nearest_obs.append(bullish_obs[0])
    if bearish_obs and _ob_distance_pct(bearish_obs[0]) < 999999.0:
        nearest_obs.append(bearish_obs[0])

    ob_consol_tol_pct = min(settings.get("approachPct", 2.0), 0.50)

    for ob in nearest_obs:
        dist_pct      = _ob_distance_pct(ob)
        price_in_zone = ob["bottom"] <= price <= ob["top"]
        vol_pct       = ob.get("volumePct", 0)
        vol_fmt       = ob.get("volumeFormatted", "0")
        direction     = ob["type"]

        # ── FILTER: uses volumePct (matches TradingView percentage) ──
        if settings.get("useObStrengthFilter"):
            if vol_pct < settings.get("obMinStrengthPct", 70):
                continue

        pos_label = "INSIDE ZONE" if price_in_zone else f"Near zone ({dist_pct:.2f}%)"

        # ── OB_APPROACH alert ──
        if (not price_in_zone) and (0 < dist_pct <= settings.get("obDistancePct", settings.get("approachPct", 2.0))):
            alerts.append({
                "setup": "OB_APPROACH",
                "direction": direction,
                "timeframe": tf,
                "detail": (f'Price approaching {direction.upper()} OB | {pos_label} | '
                           f'Vol: {vol_fmt} ({vol_pct}%) | '
                           f'Zone: {fmt_price(ob["bottom"])} - {fmt_price(ob["top"])}'),
                "strength": 4 if vol_pct >= 70 else 3,
                "meta": {"obVolumePct": vol_pct, "obVolumeFormatted": vol_fmt,
                         "obDistPct": round(dist_pct, 3)},
            })

        # ── OB_CONSOL alert ──
        price_near_zone = price_in_zone or dist_pct <= ob_consol_tol_pct
        if price_near_zone:
            needed_consol = settings.get("consolCandles", 5)
            consecutive   = _ob_consol_consecutive(ob, needed_consol, ob_consol_tol_pct)
            if consecutive >= needed_consol:
                alerts.append({
                    "setup": "OB_CONSOL",
                    "direction": direction,
                    "timeframe": tf,
                    "detail": (f'Consolidating on {direction.upper()} OB | {pos_label} | '
                               f'Candles: {consecutive} | '
                               f'Vol: {vol_fmt} ({vol_pct}%) | '
                               f'Zone: {fmt_price(ob["bottom"])} - {fmt_price(ob["top"])}'),
                    "strength": 4 if vol_pct >= 70 else 3,
                    "meta": {"obVolumePct": vol_pct, "obVolumeFormatted": vol_fmt,
                             "obDistPct": round(dist_pct, 3), "consolCandles": consecutive},
                })

    # RSI context
    if current_rsi >= settings["rsiOB"]:
        alerts.append({
            "setup": "RSI",
            "direction": "bearish",
            "timeframe": tf,
            "detail": f'RSI overbought at {current_rsi:.1f}',
            "strength": 2,
            "meta": {},
        })
    elif current_rsi <= settings["rsiOS"]:
        alerts.append({
            "setup": "RSI",
            "direction": "bullish",
            "timeframe": tf,
            "detail": f'RSI oversold at {current_rsi:.1f}',
            "strength": 2,
            "meta": {},
        })

    # ── Fib Module — Standalone High-Accuracy ──
    if settings.get("useFibModule"):
        sel_levels   = settings.get("fibLevels", ["0.5", "0.618", "0.705", "0.786"])
        tolerance    = settings.get("fibTolerancePct", 0.5)
        approach_pct = settings.get("fibApproachPct", tolerance * 2)
        fib_lookback = int(settings.get("fibLookbackCandles", 80))
        min_impulse  = float(settings.get("fibMinImpulse", 35))

        if fib_candles:
            fh = [x["high"]             for x in fib_candles]
            fl = [x["low"]              for x in fib_candles]
            fc = [x["close"]            for x in fib_candles]
            fo = [x["open"]             for x in fib_candles]
            fv = [x.get("volume", 1.0)  for x in fib_candles]
        else:
            fh, fl, fc, fo, fv = h, l, c, o, v

        active_fib = find_active_fib_leg(fo, fh, fl, fc, fv,
                                         lookback=fib_lookback,
                                         min_impulse_strength=min_impulse)

        if active_fib:
            is_bull_leg    = active_fib["bullish"]
            leg_dir        = "bullish" if is_bull_leg else "bearish"
            impulse_str    = active_fib.get("impulse_strength", 0)
            impulse_label  = "Strong" if impulse_str >= 70 else "Good" if impulse_str >= 50 else "Moderate"

            for level_name in sel_levels:
                if level_name not in active_fib["levels"]:
                    continue
                level_price = active_fib["levels"][level_name]
                dist_pct    = abs(price - level_price) / max(price, 1e-10) * 100
                trade_dir   = "bullish" if is_bull_leg else "bearish"

                # FVG confluence at this level (uses existing fvgs list)
                fvg_at_level = any(
                    fvg["direction"] == trade_dir and fvg["bottom"] <= level_price <= fvg["top"]
                    for fvg in fvgs
                )

                # ── FIB_APPROACH: price getting close, not yet touching ──
                if tolerance < dist_pct <= approach_pct:
                    alerts.append({
                        "setup": "FIB_APPROACH",
                        "direction": trade_dir,
                        "timeframe": tf,
                        "detail": (f'Approaching Fib {level_name} | '
                                   f'Dist: {dist_pct:.2f}% | '
                                   f'Level: {fmt_price(level_price)} | '
                                   f'Leg: {fmt_price(active_fib["a"])} → {fmt_price(active_fib["b"])} | '
                                   f'Impulse: {impulse_str:.0f}% ({impulse_label})'),
                        "strength": 3 if impulse_str >= 60 else 2,
                        "meta": {
                            "fibLevel": level_name, "fibPrice": round(level_price, 8),
                            "fibDist": round(dist_pct, 3), "impulseStrength": impulse_str,
                            "legDirection": leg_dir,
                        },
                    })

                # ── At the level: check wick rejection ──
                if dist_pct <= tolerance:
                    has_rejection, rej_strength = check_wick_rejection(
                        fo, fh, fl, fc, level_price, is_bull_leg, tolerance
                    )

                    if has_rejection and fvg_at_level:
                        alerts.append({
                            "setup": "FIB_REACTION",
                            "direction": trade_dir,
                            "timeframe": tf,
                            "detail": (f'Fib {level_name} REACTION | Wick + FVG | '
                                       f'Level: {fmt_price(level_price)} | '
                                       f'Rejection: {rej_strength}% | '
                                       f'Leg: {fmt_price(active_fib["a"])} → {fmt_price(active_fib["b"])} | '
                                       f'Impulse: {impulse_str:.0f}% ({impulse_label})'),
                            "strength": 5 if rej_strength >= 70 and impulse_str >= 60 else 4,
                            "meta": {
                                "fibLevel": level_name, "fibPrice": round(level_price, 8),
                                "fibDist": round(dist_pct, 3), "rejectionStrength": rej_strength,
                                "fvgConfluence": True, "impulseStrength": impulse_str,
                                "legDirection": leg_dir,
                            },
                        })
                    elif has_rejection:
                        alerts.append({
                            "setup": "FIB_REACTION",
                            "direction": trade_dir,
                            "timeframe": tf,
                            "detail": (f'Fib {level_name} wick rejection (no FVG) | '
                                       f'Level: {fmt_price(level_price)} | '
                                       f'Rejection: {rej_strength}% | '
                                       f'Leg: {fmt_price(active_fib["a"])} → {fmt_price(active_fib["b"])} | '
                                       f'Impulse: {impulse_str:.0f}% ({impulse_label})'),
                            "strength": 3,
                            "meta": {
                                "fibLevel": level_name, "fibPrice": round(level_price, 8),
                                "fibDist": round(dist_pct, 3), "rejectionStrength": rej_strength,
                                "fvgConfluence": False, "impulseStrength": impulse_str,
                                "legDirection": leg_dir,
                            },
                        })
                    elif fvg_at_level:
                        alerts.append({
                            "setup": "FIB_APPROACH",
                            "direction": trade_dir,
                            "timeframe": tf,
                            "detail": (f'At Fib {level_name} + FVG (awaiting rejection) | '
                                       f'Level: {fmt_price(level_price)} | '
                                       f'Leg: {fmt_price(active_fib["a"])} → {fmt_price(active_fib["b"])} | '
                                       f'Impulse: {impulse_str:.0f}% ({impulse_label})'),
                            "strength": 3,
                            "meta": {
                                "fibLevel": level_name, "fibPrice": round(level_price, 8),
                                "fibDist": round(dist_pct, 3), "fvgConfluence": True,
                                "impulseStrength": impulse_str, "legDirection": leg_dir,
                            },
                        })

    if not alerts:
        return None

    # Conflict resolution:
    # 1. OB_CONSOL in any direction cancels OB_APPROACH in the SAME direction
    #    (price can't be both approaching AND consolidating on the same OB)
    # 2. OB_CONSOL in one direction cancels OB_APPROACH in the OPPOSITE direction
    #    (e.g. consolidating on bullish OB cancels bearish approach signal)
    consol_directions = {a["direction"] for a in alerts if a["setup"] == "OB_CONSOL"}
    if consol_directions:
        alerts = [
            a for a in alerts
            if not (a["setup"] == "OB_APPROACH" and a["direction"] in consol_directions)
        ]

    if not alerts:
        return None

    alerts.sort(key=lambda x: x["strength"], reverse=True)
    return {
        "symbol": symbol,
        "price": price,
        "timeframe": tf,
        "trend": trend,
        "itrend": itrend,
        "rsi": round(current_rsi, 2),
        "atr": round(current_atr, 6),
        "correlation": round(corr_value, 3) if corr_value is not None else None,
        "correlationLabel": corr_label,
        "alerts": alerts,
        "topAlert": alerts[0],
    }


# ============================================================
# Binance / routes
# ============================================================

def get_pairs(market: str = "perpetual", force: bool = False) -> List[Dict[str, Any]]:
    """
    Fetch USDT pairs. For perpetual mode, tries Binance Futures API first
    (real futures volume), falls back to geo-safe spot mirror.
    """
    now = time.time()
    mkey = "perpetual" if market == "perpetual" else "spot"
    cache = PAIR_CACHE[mkey]
    if not force and now - cache["ts"] < 120 and cache["pairs"]:
        return cache["pairs"]

    stables = {"BUSDUSDT", "USDCUSDT", "DAIUSDT", "TUSDUSDT", "FDUSDUSDT"}

    # ── Try Binance Futures API first (for perpetual mode) ──
    if mkey == "perpetual":
        try:
            r = req.get(f"{BINANCE_FUTURES_API}/fapi/v1/ticker/24hr", timeout=15)
            if r.status_code == 200:
                data = r.json()
                pairs = []
                for t in data:
                    sym = t.get("symbol", "")
                    if not sym.endswith("USDT") or sym in stables:
                        continue
                    vol = safe_float(t.get("quoteVolume", 0))
                    price = safe_float(t.get("lastPrice", 0))
                    if vol < 500_000 or price <= 0:
                        continue
                    pairs.append({
                        "symbol": sym,
                        "price": price,
                        "changePct": safe_float(t.get("priceChangePercent", 0)),
                        "quoteVolume": vol,
                        "volume": safe_float(t.get("volume", 0)),
                    })
                pairs.sort(key=lambda x: x["quoteVolume"], reverse=True)
                print(f"[DEBUG] get_pairs futures ok: {len(pairs)} pairs")
                cache["ts"] = now
                cache["pairs"] = pairs
                return pairs
        except Exception as e:
            print(f"[DEBUG] get_pairs futures failed ({e}), falling back to spot mirror")

    # ── Fallback: geo-safe spot mirror ──
    try:
        url = f"{SPOT_API}/api/v3/ticker/24hr"
        print(f"[DEBUG] get_pairs spot fallback market={mkey} url={url}")
        data = req.get(url, timeout=20).json()
        pairs = []
        raw_usdt = 0
        for item in data:
            sym = item.get("symbol", "")
            if not sym.endswith("USDT"):
                continue
            if any(x in sym for x in ["UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT"]):
                continue
            raw_usdt += 1
            if mkey == "perpetual" and sym not in PERP_SYMBOLS:
                continue
            quote_vol = safe_float(item.get("quoteVolume"))
            last_price = safe_float(item.get("lastPrice"))
            min_vol = 500_000 if mkey == "perpetual" else 1_000_000
            if quote_vol < min_vol or last_price <= 0:
                continue
            pairs.append({
                "symbol": sym,
                "price": last_price,
                "changePct": safe_float(item.get("priceChangePercent")),
                "quoteVolume": quote_vol,
                "volume": safe_float(item.get("volume")),
            })
        pairs.sort(key=lambda x: x["quoteVolume"], reverse=True)
        print(f"[DEBUG] get_pairs spot raw_usdt={raw_usdt} after_filter={len(pairs)} mkey={mkey}")
        cache["ts"] = now
        cache["pairs"] = pairs
        return pairs
    except Exception as e:
        print(f"[DEBUG] get_pairs error: {e}")
        traceback.print_exc()
        return cache.get("pairs", [])


def get_klines(symbol: str, interval: str, limit: int = 300, market: str = "perpetual") -> List[Dict[str, float]]:
    """
    Fetch OHLCV candles. Tries Binance Futures API first (real futures volume),
    falls back to the geo-safe spot mirror.
    """
    def _parse(data):
        return [{
            "openTime": k[0],
            "open":     float(k[1]),
            "high":     float(k[2]),
            "low":      float(k[3]),
            "close":    float(k[4]),
            "volume":   float(k[5]),
        } for k in data]

    # ── Try Binance Futures API first ──
    try:
        r = req.get(
            f"{BINANCE_FUTURES_API}/fapi/v1/klines",
            params={"symbol": symbol, "interval": interval, "limit": limit},
            timeout=15,
        )
        if r.status_code == 200:
            return _parse(r.json())
    except Exception:
        pass

    # ── Fallback: geo-safe spot mirror ──
    url = f"{SPOT_API}/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
    data = req.get(url, timeout=20).json()
    return _parse(data)


def get_all_daily_klines(symbol: str) -> List[Dict[str, float]]:
    """Paginate through Binance 1D history from 2017-01-01 to get all candles."""
    all_klines: List[Dict[str, float]] = []
    start_ms = 1483228800000  # 2017-01-01 UTC
    max_iters = 15  # safety cap (~41 years max)
    for _ in range(max_iters):
        url = (
            f"{SPOT_API}/api/v3/klines"
            f"?symbol={symbol}&interval=1d&limit=1000&startTime={start_ms}"
        )
        try:
            resp = req.get(url, timeout=30).json()
        except Exception:
            break
        if not resp or not isinstance(resp, list) or len(resp) == 0:
            break
        for k in resp:
            all_klines.append({
                "openTime": int(k[0]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
            })
        if len(resp) < 1000:
            break
        start_ms = int(resp[-1][0]) + 86_400_000  # next day
    return all_klines


def detect_true_ath_atl(symbol: str, market: str = "perpetual") -> Optional[Dict[str, Any]]:
    """Return true ATH/ATL using full 1D history. Cached for 4 h."""
    cache_key = f"{symbol}:{market}"
    now = time.time()
    cached = ATH_ATL_CACHE.get(cache_key)
    if cached and now - cached["ts"] < ATH_ATL_CACHE_TTL:
        return cached["data"]
    klines = get_all_daily_klines(symbol)
    if not klines:
        return None
    ath = max(k["high"] for k in klines)
    atl = min(k["low"] for k in klines)
    data = {"ath": ath, "atl": atl, "bars": len(klines)}
    ATH_ATL_CACHE[cache_key] = {"ts": now, "data": data}
    return data


def parse_settings(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "tf": payload.get("tf", "1h"),
        "iLen": int(payload.get("iLen", 5)),
        "sLen": int(payload.get("sLen", 30)),
        "approachPct": float(payload.get("approachPct", 2.0)),
        "obDistancePct": float(payload.get("obDistancePct", payload.get("approachPct", 2.0))),
        "consolCandles": int(payload.get("consolCandles", 4)),
        "rsiOB": float(payload.get("rsiOB", 75)),
        "rsiOS": float(payload.get("rsiOS", 25)),
        "useObStrengthFilter": bool(payload.get("useObStrengthFilter", False)),
        "obMinStrengthPct": float(payload.get("obMinStrengthPct", 70)),
        "useFvgValidOnly": bool(payload.get("useFvgValidOnly", True)),
        "useFvgState": bool(payload.get("useFvgState", False)),
        "fvgState": payload.get("fvgState", "all"),
        "useFvgAgeRange": bool(payload.get("useFvgAgeRange", False)),
        "fvgAgeMin": int(payload.get("fvgAgeMin", 0)),
        "fvgAgeMax": int(payload.get("fvgAgeMax", 5)),
        "useFvgDistance": bool(payload.get("useFvgDistance", False)),
        "fvgMaxDistancePct": float(payload.get("fvgMaxDistancePct", 1.5)),
        "useFvgTouchDepth": bool(payload.get("useFvgTouchDepth", False)),
        "fvgTouchDepth": payload.get("fvgTouchDepth", "any"),
        "useFvgObOverlap": bool(payload.get("useFvgObOverlap", False)),
        "fvgObOverlapMode": payload.get("fvgObOverlapMode", "same_direction"),
        "fvgObMinOverlapPct": float(payload.get("fvgObMinOverlapPct", 20)),
        "useFibModule": bool(payload.get("useFibModule", False)),
        "fibTf": payload.get("fibTf", payload.get("tf", "1h")),
        "fibLegMethod": payload.get("fibLegMethod", "lookback_range"),
        "fibSwingDirection": payload.get("fibSwingDirection", "auto"),
        "fibLookbackCandles": int(payload.get("fibLookbackCandles", 50)),
        "fibLevels": payload.get("fibLevels", ["0.5", "0.618", "0.705", "0.786"]),
        "fibTolerancePct": float(payload.get("fibTolerancePct", 0.5)),
        "useFibRequireFvg": bool(payload.get("useFibRequireFvg", False)),
        "useFibRequireOb": bool(payload.get("useFibRequireOb", False)),
        "fibSetupType": payload.get("fibSetupType", "both"),
        "fibDisplayMode": payload.get("fibDisplayMode", "best_only"),
        "fibApproachPct": float(payload.get("fibApproachPct", 1.0)),
        "fibMinImpulse": float(payload.get("fibMinImpulse", 35)),
        "useBtcCorrelation": bool(payload.get("useBtcCorrelation", False)),
        "btcCorrelationMode": payload.get("btcCorrelationMode", "all"),
        "btcLookback": int(payload.get("btcLookback", 60)),
    }


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        pwd = request.form.get("password", "")
        if pwd == APP_PASSWORD:
            session["logged_in"] = True
            return redirect(url_for("index"))
        error = "Incorrect password. Try again."
    return render_template("login.html", error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/")
@login_required
def index():
    return render_template("index.html")


@app.route("/api/pairs")
@login_required
def api_pairs():
    limit = int(request.args.get("limit", 200))
    market = request.args.get("market", "perpetual")
    return jsonify(get_pairs(market)[:limit])


@app.route("/api/scan", methods=["POST"])
@login_required
def api_scan():
    payload = request.get_json(force=True) or {}
    settings = parse_settings(payload.get("settings", {}))
    market = payload.get("market", "perpetual")
    symbols = payload.get("symbols", [])
    mode = payload.get("scanMode", "selected")
    pairs_per_cycle = int(payload.get("pairsPerCycle", 20))
    if mode == "market":
        all_pairs = [p["symbol"] for p in get_pairs(market)]
        if payload.get("roundRobin", True):
            start = ROUND_ROBIN_STATE["index"]
            chosen = all_pairs[start:start + pairs_per_cycle]
            if len(chosen) < pairs_per_cycle:
                chosen += all_pairs[:max(0, pairs_per_cycle - len(chosen))]
            ROUND_ROBIN_STATE["index"] = (start + pairs_per_cycle) % max(len(all_pairs), 1)
            symbols = chosen
        else:
            symbols = all_pairs[:pairs_per_cycle]
    elif not symbols:
        symbols = [p["symbol"] for p in get_pairs(market)[:pairs_per_cycle]]

    btc_closes = None
    try:
        btc = get_klines("BTCUSDT", settings["tf"], 300, market)
        btc_closes = [x["close"] for x in btc]
    except Exception:
        btc_closes = None

    fib_tf = settings.get("fibTf", settings["tf"]) if settings.get("useFibModule") else None
    fetch_fib_separately = fib_tf and fib_tf != settings["tf"]

    def scan_symbol(sym):
        try:
            candles = get_klines(sym, settings["tf"], 300, market)
            fib_candles = None
            if fetch_fib_separately:
                try:
                    fib_candles = get_klines(sym, fib_tf, 300, market)
                except Exception:
                    pass
            result = analyze_pair(sym, candles, settings["tf"], settings, btc_closes, fib_candles=fib_candles)
            if result and candles:
                c = [x["close"] for x in candles]
                result["sparkline"] = [float(c[i]) for i in range(max(0, len(c)-24), len(c))]
            return result
        except Exception:
            traceback.print_exc()
            return None

    results = []
    workers = min(10, len(symbols)) if symbols else 1
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(scan_symbol, sym): sym for sym in symbols}
        for fut in as_completed(futures):
            result = fut.result()
            if result:
                results.append(result)

    results.sort(key=lambda x: x["topAlert"]["strength"], reverse=True)

    # Filter mode: only keep results that have ALL required signal types
    filter_mode = payload.get("filterMode", "match")
    required_signals = payload.get("requiredSignals", [])
    if filter_mode == "filter" and required_signals:
        def has_signal(result, sig):
            setups = {a["setup"] for a in result.get("alerts", [result.get("topAlert", {})])}
            if sig == "FVG":
                return "FVG" in setups
            if sig == "OB":
                return bool(setups & {"OB_APPROACH", "OB_CONSOL"})
            if sig == "FIB":
                return "FIB" in setups
            return False
        results = [r for r in results if all(has_signal(r, s) for s in required_signals)]

    return jsonify({
        "scanned": len(symbols),
        "results": results,
        "nextRoundRobinIndex": ROUND_ROBIN_STATE["index"],
    })


@app.route("/api/compressed_scan", methods=["POST"])
@login_required
def api_compressed_scan():
    payload = request.get_json(force=True) or {}
    tf = payload.get("timeframe", "1h")
    market = payload.get("market", "perpetual")
    lookback = int(payload.get("lookback", 12))
    max_pct = float(payload.get("maxPct", 2.0))
    passed_symbols = payload.get("symbols") or []
    if passed_symbols:
        symbols = passed_symbols
    else:
        symbols = [p["symbol"] for p in get_pairs(market)[:80]]
    print(f"[DEBUG] compressed_scan market={market} tf={tf} lookback={lookback} max_pct={max_pct} symbols_count={len(symbols)}")
    data = []
    for sym in symbols:
        try:
            kl = get_klines(sym, tf, max(160, lookback + 20), market)
            h = [x["high"] for x in kl]
            l = [x["low"] for x in kl]
            c = [x["close"] for x in kl]
            v = [x["volume"] for x in kl]
            ok, info = detect_compression(h, l, c, lookback, max_pct)
            if ok:
                sparkline = [float(c[i]) for i in range(max(0, len(c)-24), len(c))]
                data.append({
                    "symbol": sym,
                    "price": c[-1],
                    "timeframe": tf,
                    "rangePct": round(info["rangePct"], 2),
                    "high": round(info["high"], 6),
                    "low": round(info["low"], 6),
                    "volume": v[-1],
                    "sparkline": sparkline,
                })
        except Exception as e:
            print(f"[DEBUG] compressed_scan {sym} error: {e}")
            continue
    data.sort(key=lambda x: x["rangePct"])
    print(f"[DEBUG] compressed_scan results={len(data)}")
    return jsonify(data)


@app.route("/api/trending_scan", methods=["POST"])
@login_required
def api_trending_scan():
    payload = request.get_json(force=True) or {}
    tf = payload.get("timeframe", "1h")
    market = payload.get("market", "perpetual")
    mode = payload.get("mode", "movers")
    limit = int(payload.get("limit", 30))
    pairs = get_pairs(market)[:150]
    if mode == "movers":
        movers = sorted(pairs, key=lambda x: abs(x["changePct"]), reverse=True)[:limit]
        return jsonify(movers)
    out = []
    for item in pairs[:80]:
        sym = item["symbol"]
        try:
            kl = get_klines(sym, tf, 120, market)
            c = [x["close"] for x in kl]
            v = [x["volume"] for x in kl]
            trend_info = detect_trend_mode(c, v)
            ok = (
                (mode == "bullish" and trend_info["bullish"]) or
                (mode == "bearish" and trend_info["bearish"]) or
                (mode == "high_volume" and trend_info["highVolumeTrend"])
            )
            if ok:
                sparkline = [float(c[i]) for i in range(max(0, len(c)-24), len(c))]
                out.append({
                    "symbol": sym,
                    "price": c[-1],
                    "changePct": item["changePct"],
                    "quoteVolume": item["quoteVolume"],
                    "relVol": round(trend_info["relVol"], 2),
                    "mode": mode,
                    "sparkline": sparkline,
                })
        except Exception:
            continue
    out.sort(key=lambda x: (x["relVol"], abs(x["changePct"])), reverse=True)
    return jsonify(out[:limit])


@app.route("/api/ath_atl_scan", methods=["POST"])
@login_required
def api_ath_atl_scan():
    payload = request.get_json(force=True) or {}
    mode = payload.get("mode", "both")          # ath | atl | both
    status = payload.get("status", "current")   # current | recent | near
    market = payload.get("market", "perpetual")
    window_hours = int(payload.get("windowHours", 24))
    near_pct = float(payload.get("nearPct", 1.0))
    limit = int(payload.get("limit", 30))
    symbols = [p["symbol"] for p in get_pairs(market)[:60]]

    def process_symbol(sym: str):
        try:
            # ── Part A: TRUE ATH/ATL from full 1D history ──
            true_levels = detect_true_ath_atl(sym, market)
            if not true_levels:
                return None
            true_ath = true_levels["ath"]
            true_atl = true_levels["atl"]
            days_history = true_levels["bars"]

            # ── Part B: Current price + recency via 1h candles ──
            recent_bars = max(1, window_hours)   # one 1h bar per hour
            fetch_bars = min(recent_bars + 5, 300)
            kl = get_klines(sym, "1h", fetch_bars, market)
            if not kl:
                return None
            current = kl[-1]["close"]
            window_kl = kl[-recent_bars:]
            recent_high = max(x["high"] for x in window_kl)
            recent_low = min(x["low"] for x in window_kl)

            dist_ath = pct(current, true_ath)
            dist_atl = pct(current, true_atl)
            near_ath = dist_ath <= near_pct
            near_atl = dist_atl <= near_pct
            # "Hit within period" = recent window touched within 0.05% of true level
            hit_ath = recent_high >= true_ath * (1 - 0.0005)
            hit_atl = recent_low <= true_atl * (1 + 0.0005)

            tags: List[str] = []
            include = False

            if status == "current":
                if mode in ["ath", "both"] and near_ath:
                    include = True; tags.append("ATH NOW")
                if mode in ["atl", "both"] and near_atl:
                    include = True; tags.append("ATL NOW")
            elif status == "recent":
                if mode in ["ath", "both"] and hit_ath:
                    include = True; tags.append(f"ATH HIT <{window_hours}h")
                if mode in ["atl", "both"] and hit_atl:
                    include = True; tags.append(f"ATL HIT <{window_hours}h")
            else:  # near
                if mode in ["ath", "both"] and near_ath:
                    include = True; tags.append(f"NEAR ATH {dist_ath:.2f}%")
                if mode in ["atl", "both"] and near_atl:
                    include = True; tags.append(f"NEAR ATL {dist_atl:.2f}%")

            if not include:
                return None
            c_all = [x["close"] for x in kl]
            sparkline = [float(c_all[i]) for i in range(max(0, len(c_all)-24), len(c_all))]
            return {
                "symbol": sym,
                "price": fmt_price(current),
                "trueAth": fmt_price(true_ath),
                "trueAtl": fmt_price(true_atl),
                "distAthPct": round(dist_ath, 2),
                "distAtlPct": round(dist_atl, 2),
                "tags": tags,
                "daysHistory": days_history,
                "windowHours": window_hours,
                "sparkline": sparkline,
            }
        except Exception:
            traceback.print_exc()
            return None

    results = []
    workers = min(8, len(symbols)) if symbols else 1
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(process_symbol, sym) for sym in symbols]
        for fut in as_completed(futures):
            r = fut.result()
            if r:
                results.append(r)

    results.sort(key=lambda x: min(x["distAthPct"], x["distAtlPct"]))
    return jsonify(results[:limit])


@app.route("/api/bias_scan", methods=["POST"])
@login_required
def api_bias_scan():
    payload = request.get_json(force=True) or {}
    tf = payload.get("timeframe", "1d")
    market = payload.get("market", "perpetual")
    candle_count = int(payload.get("candleCount", 3))
    bias_filter = payload.get("biasFilter", "all")

    use_wick_gate = payload.get("useWickGate", True)
    wick_min_pct = float(payload.get("wickMinPct", 40)) / 100.0
    use_momentum_gate = payload.get("useMomentumGate", True)
    use_body_gate = payload.get("useBodyGate", True)
    body_min_pct = float(payload.get("bodyMinPct", 20)) / 100.0
    use_volume_gate = payload.get("useVolumeGate", False)

    use_fvg_conf = payload.get("useFvgConf", False)
    fvg_touch = payload.get("fvgTouch", "any")
    use_ob_conf = payload.get("useObConf", False)
    ob_approach_pct = float(payload.get("obApproachPct", 2.0))

    passed_symbols = payload.get("symbols") or []
    if passed_symbols:
        symbols = passed_symbols
    else:
        symbols = [p["symbol"] for p in get_pairs(market)[:80]]

    results = []

    for sym in symbols:
        try:
            kl = get_klines(sym, tf, candle_count + 30, market)
            if not kl or len(kl) < candle_count + 2:
                continue

            o = [x["open"] for x in kl]
            h = [x["high"] for x in kl]
            l = [x["low"] for x in kl]
            c = [x["close"] for x in kl]
            v = [x["volume"] for x in kl]

            sig_idx = len(c) - 2
            if sig_idx < 1:
                continue

            sig_open = o[sig_idx]
            sig_high = h[sig_idx]
            sig_low = l[sig_idx]
            sig_close = c[sig_idx]
            sig_vol = v[sig_idx]
            sig_range = sig_high - sig_low
            if sig_range <= 0:
                continue

            sig_body = abs(sig_close - sig_open)
            sig_is_green = sig_close > sig_open
            upper_wick = sig_high - max(sig_open, sig_close)
            lower_wick = min(sig_open, sig_close) - sig_low

            body_ratio = sig_body / sig_range
            upper_wick_ratio = upper_wick / sig_range
            lower_wick_ratio = lower_wick / sig_range

            prev_start = max(0, sig_idx - candle_count + 1)
            prev_candles = list(range(prev_start, sig_idx))

            prev_bullish = 0
            prev_bearish = 0
            prev_strong = 0
            for pi in prev_candles:
                p_range = h[pi] - l[pi]
                if p_range <= 0:
                    continue
                p_body = abs(c[pi] - o[pi])
                if c[pi] > o[pi]:
                    prev_bullish += 1
                else:
                    prev_bearish += 1
                if p_body / p_range > 0.5:
                    prev_strong += 1

            has_bull_momentum = prev_bullish > prev_bearish and prev_strong >= max(1, len(prev_candles) // 2)
            has_bear_momentum = prev_bearish > prev_bullish and prev_strong >= max(1, len(prev_candles) // 2)
            has_momentum = has_bull_momentum or has_bear_momentum

            vol_lookback = min(20, sig_idx)
            avg_vol = sum(v[sig_idx - vol_lookback:sig_idx]) / max(vol_lookback, 1)
            vol_spike = sig_vol >= avg_vol * 1.2 if avg_vol > 0 else False

            signal_type = None
            bias_direction = None

            has_both_wicks = upper_wick_ratio > 0.25 and lower_wick_ratio > 0.25

            if has_both_wicks and has_momentum:
                signal_type = "INDECISION"
                bias_direction = "bearish" if has_bull_momentum else "bullish"
            elif upper_wick_ratio >= wick_min_pct:
                signal_type = "WICK_REJECTION"
                bias_direction = "bearish"
            elif lower_wick_ratio >= wick_min_pct:
                signal_type = "WICK_REJECTION"
                bias_direction = "bullish"
            elif has_momentum and ((has_bull_momentum and not sig_is_green) or (has_bear_momentum and sig_is_green)):
                signal_type = "EXHAUSTION"
                bias_direction = "bearish" if has_bull_momentum else "bullish"
            elif body_ratio > 0.6 and upper_wick_ratio < 0.2 and lower_wick_ratio < 0.2:
                signal_type = "CONTINUATION"
                bias_direction = "bullish" if sig_is_green else "bearish"

            if not signal_type or not bias_direction:
                continue

            if bias_filter == "bullish" and bias_direction != "bullish":
                continue
            if bias_filter == "bearish" and bias_direction != "bearish":
                continue

            gates_passed = 0
            gates_checked = 0
            gate_details = []

            if use_wick_gate:
                gates_checked += 1
                wick_ok = (upper_wick_ratio >= wick_min_pct or lower_wick_ratio >= wick_min_pct or
                           signal_type in ("EXHAUSTION", "CONTINUATION"))
                if wick_ok:
                    gates_passed += 1
                    gate_details.append("Wick ✓")
                else:
                    gate_details.append("Wick ✗")

            if use_momentum_gate and candle_count >= 2:
                gates_checked += 1
                if has_momentum:
                    gates_passed += 1
                    gate_details.append("Momentum ✓")
                else:
                    gate_details.append("Momentum ✗")

            if use_body_gate:
                gates_checked += 1
                if body_ratio >= body_min_pct:
                    gates_passed += 1
                    gate_details.append("Body ✓")
                else:
                    gate_details.append("Body ✗")

            if use_volume_gate:
                gates_checked += 1
                if vol_spike:
                    gates_passed += 1
                    gate_details.append("Volume ✓")
                else:
                    gate_details.append("Volume ✗")

            if gates_checked >= 2 and gates_passed < 2:
                continue
            if gates_checked == 1 and gates_passed < 1:
                continue

            gate_ratio = gates_passed / gates_checked if gates_checked > 0 else 0.5

            if gate_ratio >= 0.9:
                confidence = "Strong"
            elif gate_ratio >= 0.6:
                confidence = "Moderate"
            else:
                confidence = "Weak"

            if signal_type == "INDECISION" and confidence == "Strong":
                confidence = "Moderate"

            ob_info = None
            if use_ob_conf:
                obs = detect_obs(o, h, l, c, v, 7, 20, max_ob=5)
                price = c[-2]
                for ob in obs:
                    dist = abs(price - (ob["top"] + ob["bottom"]) / 2) / max(price, 1e-10) * 100
                    if dist <= ob_approach_pct and ob["type"] == bias_direction:
                        same_dir_obs = [x for x in obs if x["type"] == ob["type"]]
                        total_vol = sum(x["volume"] for x in same_dir_obs)
                        vol_pct = int((ob["volume"] / total_vol) * 100) if total_vol > 0 else 0
                        vv = ob["volume"]
                        if vv >= 1e9:
                            vol_fmt = f"{round(vv/1e9,3)}B"
                        elif vv >= 1e6:
                            vol_fmt = f"{round(vv/1e6,3)}M"
                        elif vv >= 1e3:
                            vol_fmt = f"{round(vv/1e3,3)}K"
                        else:
                            vol_fmt = f"{round(vv)}"
                        ob_info = {
                            "zone": f"{ob['bottom']:.6f} - {ob['top']:.6f}",
                            "volPct": vol_pct,
                            "volFmt": vol_fmt,
                            "dist": round(dist, 2),
                        }
                        break

            fvg_info = None
            if use_fvg_conf:
                fvgs = detect_fvgs(o, h, l, c, v, tf)
                price = c[-2]
                for fvg in fvgs:
                    if fvg["direction"] == bias_direction:
                        if fvg_touch == "untouched" or fvg_touch == "fresh":
                            if not fvg.get("untouched", True):
                                continue
                        elif fvg_touch == "touched":
                            if fvg.get("untouched", True):
                                continue
                        if fvg["bottom"] <= price <= fvg["top"] or abs(price - (fvg["top"] + fvg["bottom"]) / 2) / max(price, 1e-10) * 100 <= 1.0:
                            touch_label = "untouched" if fvg.get("untouched") else ("once" if fvg.get("onceTouched") else "touched")
                            fvg_info = {
                                "zone": f"{fvg['bottom']:.6f} - {fvg['top']:.6f}",
                                "touch": touch_label,
                            }
                            break

            sparkline = [float(c[i]) for i in range(max(0, len(c)-24), len(c))]
            results.append({
                "symbol": sym,
                "price": round(c[-1], 8),
                "timeframe": tf,
                "bias": bias_direction,
                "signal": signal_type,
                "confidence": confidence,
                "gates": " | ".join(gate_details),
                "gatesPassed": gates_passed,
                "gatesChecked": gates_checked,
                "upperWickPct": round(upper_wick_ratio * 100, 1),
                "lowerWickPct": round(lower_wick_ratio * 100, 1),
                "bodyPct": round(body_ratio * 100, 1),
                "volSpike": vol_spike,
                "obConf": ob_info,
                "fvgConf": fvg_info,
                "sparkline": sparkline,
            })

        except Exception as e:
            print(f"[DEBUG] bias_scan {sym} error: {e}")
            continue

    conf_order = {"Strong": 0, "Moderate": 1, "Weak": 2}
    results.sort(key=lambda x: (conf_order.get(x["confidence"], 3), -x["gatesPassed"]))

    return jsonify(results)


if __name__ == "__main__":
    import os

    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port, debug=False)
