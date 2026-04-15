import math
import os
import time
import traceback
import threading
import smtplib
import json
import ssl
import socket
import struct
import hashlib
import base64
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps
from typing import Any, Dict, List, Optional, Tuple
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timezone

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

# ============================================================
# USERNAME SYSTEM
# Set USERS env var on Koyeb as comma-separated list
# Example: USERS = zyni,abdul manan,friend1
# All users share APP_PASSWORD
# Each gets their own server-side watchlist
# ============================================================
_raw_users = os.environ.get("USERS", "zyni,abdul manan")
ALLOWED_USERS: List[str] = [u.strip().lower() for u in _raw_users.split(",") if u.strip()]

# ── Server-side watchlist storage ──
# Stored in /tmp/wl_{username}.json
# Persists until Koyeb restarts (fine for daily trading use)

def _wl_file(username: str) -> str:
    """Safe file path for a user's watchlist."""
    safe = username.strip().lower().replace(" ", "_").replace("/", "").replace(".", "")
    return f"/tmp/zyni_wl_{safe}.json"

def load_user_watchlist(username: str) -> List[str]:
    try:
        with open(_wl_file(username), "r") as f:
            data = json.load(f)
            return [str(p).strip().upper() for p in data if str(p).strip()]
    except Exception:
        return []

def save_user_watchlist(username: str, pairs: List[str]) -> None:
    try:
        with open(_wl_file(username), "w") as f:
            json.dump(pairs, f)
    except Exception as e:
        print(f"[WL-SAVE] Error for {username}: {e}")

# ============================================================
# EMAIL CONFIG — Login Notifications
# Set these in Koyeb environment variables:
#   ALERT_EMAIL_FROM   = your Gmail address (sender)
#   ALERT_EMAIL_PASS   = Gmail App Password (not your real password)
#   ALERT_EMAIL_TO     = email where you want to receive alerts
# ============================================================
ALERT_EMAIL_FROM = os.environ.get("ALERT_EMAIL_FROM", "")
ALERT_EMAIL_PASS = os.environ.get("ALERT_EMAIL_PASS", "")
ALERT_EMAIL_TO   = os.environ.get("ALERT_EMAIL_TO", "")


def send_email_alert(subject: str, body: str) -> bool:
    """Send email notification. Returns True on success."""
    if not all([ALERT_EMAIL_FROM, ALERT_EMAIL_PASS, ALERT_EMAIL_TO]):
        return False  # Not configured — silently skip
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = ALERT_EMAIL_FROM
        msg["To"]      = ALERT_EMAIL_TO
        msg.attach(MIMEText(body, "html"))
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=10) as srv:
            srv.login(ALERT_EMAIL_FROM, ALERT_EMAIL_PASS)
            srv.sendmail(ALERT_EMAIL_FROM, ALERT_EMAIL_TO, msg.as_string())
        return True
    except Exception as e:
        print(f"[EMAIL] Failed: {e}")
        return False


# ============================================================
# SERVER-SIDE WATCHLIST STREAMING
# Background thread monitors watchlist pairs every 5s
# Browser reads cached results — works even when phone sleeps
# ============================================================

# Shared state — thread-safe via lock
_wl_lock          = threading.Lock()
_wl_pairs: List[str]          = []          # current watchlist pairs
_wl_cache: Dict[str, Any]     = {}          # symbol → latest OF result
_wl_thread: Optional[threading.Thread] = None
_wl_running       = False


def _wl_background_loop():
    """Background thread: fetch orderflow for all watchlist pairs every 5s."""
    global _wl_running
    print("[WL-STREAM] Background thread started")
    while _wl_running:
        with _wl_lock:
            pairs = list(_wl_pairs)

        for sym in pairs:
            if not _wl_running:
                break
            try:
                of_data = fetch_orderflow_data(sym)

                # Get current price + nearest OB from quick kline fetch
                price       = 0.0
                zone_top    = 0.0
                zone_bottom = 0.0
                ob_type     = "bullish"
                try:
                    r = req.get(
                        f"{BINANCE_FUTURES_API}/fapi/v1/klines",
                        params={"symbol": sym, "interval": "15m", "limit": 50},
                        timeout=5,
                    )
                    if r.status_code == 200:
                        klines = r.json()
                        if klines:
                            o_ = [float(k[1]) for k in klines]
                            h_ = [float(k[2]) for k in klines]
                            l_ = [float(k[3]) for k in klines]
                            c_ = [float(k[4]) for k in klines]
                            v_ = [float(k[5]) for k in klines]
                            price = c_[-1]
                            if len(c_) >= 20:
                                obs_ = detect_obs(o_, h_, l_, c_, v_, 5, 10, max_ob=3)
                                if obs_:
                                    nearest = min(obs_, key=lambda ob: obq_dist_from_price(
                                        price, ob["top"], ob["bottom"], ob["type"]))
                                    zone_top    = nearest["top"]
                                    zone_bottom = nearest["bottom"]
                                    ob_type     = nearest["type"]
                except Exception:
                    pass

                of_result = analyze_orderflow(of_data, price, ob_type, zone_top, zone_bottom)
                result = {
                    "symbol":          sym,
                    "price":           round(price, 8),
                    "absorption":      of_result["absorption"],
                    "absorption_str":  of_result["absorption_str"],
                    "delta":           of_result["delta"],
                    "buy_volume":      of_result["buy_volume"],
                    "sell_volume":     of_result["sell_volume"],
                    "oi_signal":       of_result["oi_signal"],
                    "funding_context": of_result["funding_context"],
                    "score_delta":     of_result["score_delta"],
                    "checklist_pass":  of_result["checklist_pass"],
                    "summary":         of_result["summary"],
                    "ob_type":         ob_type,
                    "zone_top":        round(zone_top, 8),
                    "zone_bottom":     round(zone_bottom, 8),
                    "ts":              int(time.time()),
                }
                with _wl_lock:
                    _wl_cache[sym] = result

            except Exception as e:
                print(f"[WL-STREAM] Error for {sym}: {e}")

        # Wait 5s between full cycles
        time.sleep(5)

    print("[WL-STREAM] Background thread stopped")


def _ensure_wl_thread():
    """Start background thread if not running."""
    global _wl_thread, _wl_running
    if _wl_thread and _wl_thread.is_alive():
        return
    _wl_running = True
    _wl_thread = threading.Thread(target=_wl_background_loop, daemon=True, name="wl-stream")
    _wl_thread.start()


# ============================================================
# FULL ORDER BOOK — WebSocket + Snapshot Manager
# Maintains complete order book for watchlist pairs
# No range limitation — captures walls at ANY price level
# ============================================================

_ob_book_lock = threading.Lock()
_ob_books: Dict[str, Dict] = {}
# Structure per symbol:
# {
#   "bids": {price_float: qty_float, ...},  # ALL bid levels
#   "asks": {price_float: qty_float, ...},  # ALL ask levels
#   "lastUpdateId": int,
#   "ts": float,
#   "ready": bool
# }

_ob_ws_threads: Dict[str, threading.Thread] = {}
_ob_ws_running: Dict[str, bool] = {}


def _raw_ws_connect(host: str, path: str) -> socket.socket:
    """
    Raw WebSocket handshake using built-in socket + ssl.
    No external library needed.
    """
    ctx = ssl.create_default_context()
    sock = socket.create_connection((host, 443), timeout=10)
    sock = ctx.wrap_socket(sock, server_hostname=host)

    # WebSocket handshake
    key = base64.b64encode(os.urandom(16)).decode()
    handshake = (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {host}\r\n"
        f"Upgrade: websocket\r\n"
        f"Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        f"Sec-WebSocket-Version: 13\r\n"
        f"\r\n"
    )
    sock.sendall(handshake.encode())

    # Read response
    resp = b""
    while b"\r\n\r\n" not in resp:
        resp += sock.recv(1024)

    return sock


def _ws_recv_frame(sock: socket.socket) -> Optional[bytes]:
    """Read one WebSocket frame."""
    try:
        # Read first 2 bytes
        header = b""
        while len(header) < 2:
            chunk = sock.recv(2 - len(header))
            if not chunk:
                return None
            header += chunk

        fin_opcode = header[0]
        masked_len = header[1]
        opcode = fin_opcode & 0x0F
        payload_len = masked_len & 0x7F

        if opcode == 8:  # Close frame
            return None
        if opcode == 9:  # Ping — send pong
            pong = bytes([0x8A, 0x00])
            sock.sendall(pong)
            return b""

        if payload_len == 126:
            ext = b""
            while len(ext) < 2:
                ext += sock.recv(2 - len(ext))
            payload_len = struct.unpack(">H", ext)[0]
        elif payload_len == 127:
            ext = b""
            while len(ext) < 8:
                ext += sock.recv(8 - len(ext))
            payload_len = struct.unpack(">Q", ext)[0]

        data = b""
        while len(data) < payload_len:
            chunk = sock.recv(min(payload_len - len(data), 65536))
            if not chunk:
                return None
            data += chunk

        return data
    except Exception:
        return None


def _fetch_ob_snapshot(symbol: str) -> Optional[Dict]:
    """Fetch full order book snapshot via REST."""
    try:
        r = req.get(
            f"https://fapi.binance.com/fapi/v1/depth",
            params={"symbol": symbol, "limit": 1000},
            timeout=10,
        )
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        print(f"[OB-SNAP] {symbol}: {e}")
    return None


def _ob_ws_loop(symbol: str):
    """
    WebSocket loop for one symbol.
    1. Subscribe to diff depth stream
    2. Take REST snapshot
    3. Apply buffered + future diffs
    4. Maintain complete in-memory book
    """
    sym_lower = symbol.lower()
    print(f"[OB-WS] Starting for {symbol}")

    while _ob_ws_running.get(symbol, False):
        sock = None
        try:
            # Connect to WebSocket
            path = f"/ws/{sym_lower}@depth@100ms"
            sock = _raw_ws_connect("fstream.binance.com", path)
            sock.settimeout(30)

            # Buffer diffs until snapshot is ready
            buffered = []
            snapshot_done = False

            # Take snapshot in background
            snap_result = [None]
            def _do_snap():
                snap_result[0] = _fetch_ob_snapshot(symbol)
            snap_thread = threading.Thread(target=_do_snap, daemon=True)
            snap_thread.start()

            # Receive frames
            while _ob_ws_running.get(symbol, False):
                raw = _ws_recv_frame(sock)
                if raw is None:
                    break
                if not raw:
                    continue

                try:
                    msg = json.loads(raw.decode("utf-8"))
                except Exception:
                    continue

                if not snapshot_done:
                    buffered.append(msg)

                    # Check if snapshot arrived
                    if snap_result[0] is not None:
                        snap = snap_result[0]
                        last_update_id = snap.get("lastUpdateId", 0)

                        # Build initial book
                        bids = {float(p): float(q) for p, q in snap.get("bids", []) if float(q) > 0}
                        asks = {float(p): float(q) for p, q in snap.get("asks", []) if float(q) > 0}

                        # Apply buffered diffs (only those with U <= lastUpdateId+1 <= u)
                        for diff in buffered:
                            u = diff.get("u", 0)
                            U = diff.get("U", 0)
                            if u <= last_update_id:
                                continue
                            for p, q in diff.get("b", []):
                                pf, qf = float(p), float(q)
                                if qf == 0:
                                    bids.pop(pf, None)
                                else:
                                    bids[pf] = qf
                            for p, q in diff.get("a", []):
                                pf, qf = float(p), float(q)
                                if qf == 0:
                                    asks.pop(pf, None)
                                else:
                                    asks[pf] = qf

                        with _ob_book_lock:
                            _ob_books[symbol] = {
                                "bids": bids,
                                "asks": asks,
                                "lastUpdateId": last_update_id,
                                "ts": time.time(),
                                "ready": True,
                            }

                        snapshot_done = True
                        buffered.clear()
                        print(f"[OB-WS] {symbol} book ready: {len(bids)} bids, {len(asks)} asks")

                        # ── Periodic re-snapshot every 30s to catch far levels ──
                        # Institutional walls far from price rarely generate diffs
                        # Re-snapshotting ensures we always have fresh complete data
                        last_resnap = time.time()

                else:
                    # Apply incremental update
                    with _ob_book_lock:
                        book = _ob_books.get(symbol)
                        if book and book.get("ready"):
                            for p, q in msg.get("b", []):
                                pf, qf = float(p), float(q)
                                if qf == 0:
                                    book["bids"].pop(pf, None)
                                else:
                                    book["bids"][pf] = qf
                            for p, q in msg.get("a", []):
                                pf, qf = float(p), float(q)
                                if qf == 0:
                                    book["asks"].pop(pf, None)
                                else:
                                    book["asks"][pf] = qf
                            book["lastUpdateId"] = msg.get("u", book["lastUpdateId"])
                            book["ts"] = time.time()

                    # Re-snapshot every 30s to refresh far levels
                    if time.time() - last_resnap > 30:
                        try:
                            fresh = _fetch_ob_snapshot(symbol)
                            if fresh:
                                fresh_bids = {float(p): float(q) for p, q in fresh.get("bids", []) if float(q) > 0}
                                fresh_asks = {float(p): float(q) for p, q in fresh.get("asks", []) if float(q) > 0}
                                with _ob_book_lock:
                                    book = _ob_books.get(symbol)
                                    if book:
                                        # Merge: fresh snapshot takes priority for levels it contains
                                        book["bids"].update(fresh_bids)
                                        book["asks"].update(fresh_asks)
                                        book["ts"] = time.time()
                                last_resnap = time.time()
                                print(f"[OB-WS] {symbol} re-snapped: {len(fresh_bids)} bids, {len(fresh_asks)} asks")
                        except Exception:
                            pass

        except Exception as e:
            print(f"[OB-WS] {symbol} error: {e}")
        finally:
            if sock:
                try:
                    sock.close()
                except Exception:
                    pass

        # Mark not ready during reconnect
        with _ob_book_lock:
            if symbol in _ob_books:
                _ob_books[symbol]["ready"] = False

        if _ob_ws_running.get(symbol, False):
            print(f"[OB-WS] {symbol} reconnecting in 3s...")
            time.sleep(3)

    print(f"[OB-WS] {symbol} stopped")


def start_ob_ws(symbol: str):
    """Start WebSocket book for a symbol if not already running."""
    if symbol in _ob_ws_threads and _ob_ws_threads[symbol].is_alive():
        return
    _ob_ws_running[symbol] = True
    t = threading.Thread(target=_ob_ws_loop, args=(symbol,), daemon=True, name=f"ob-ws-{symbol}")
    _ob_ws_threads[symbol] = t
    t.start()


def stop_ob_ws(symbol: str):
    """Stop WebSocket book for a symbol."""
    _ob_ws_running[symbol] = False
    with _ob_book_lock:
        _ob_books.pop(symbol, None)


def stop_all_ob_ws():
    """Stop all WebSocket book streams."""
    for sym in list(_ob_ws_running.keys()):
        _ob_ws_running[sym] = False
    with _ob_book_lock:
        _ob_books.clear()


# ── On-demand stream tracker ──
# Tracks pairs started on-demand (scan page) with auto-timeout
_ob_ondemand: Dict[str, float] = {}  # symbol → last_access_time
_ob_ondemand_lock = threading.Lock()
_OB_ONDEMAND_TTL = 300  # seconds — stop stream after 5 min of inactivity


def _ob_ondemand_cleanup_loop():
    """Background thread: stop on-demand streams that haven't been accessed recently."""
    while True:
        time.sleep(30)
        now = time.time()
        with _ob_ondemand_lock:
            expired = [sym for sym, t in _ob_ondemand.items()
                       if now - t > _OB_ONDEMAND_TTL]
        for sym in expired:
            # Only stop if not in watchlist
            with _wl_lock:
                in_wl = sym in _wl_pairs
            if not in_wl:
                print(f"[OB-ONDEMAND] Auto-stopping {sym} (idle {_OB_ONDEMAND_TTL}s)")
                stop_ob_ws(sym)
                with _ob_ondemand_lock:
                    _ob_ondemand.pop(sym, None)


# Start cleanup thread
_ondemand_cleanup_thread = threading.Thread(
    target=_ob_ondemand_cleanup_loop, daemon=True, name="ob-ondemand-cleanup"
)
_ondemand_cleanup_thread.start()


def ensure_ob_stream(symbol: str, wait_sec: float = 4.0) -> bool:
    """
    Ensure full order book stream is running for a symbol.
    Starts on-demand if not already running.
    Waits up to wait_sec for book to be ready.
    Returns True if book is ready.
    """
    # Update last access time
    with _ob_ondemand_lock:
        _ob_ondemand[symbol] = time.time()

    # Already ready?
    with _ob_book_lock:
        book = _ob_books.get(symbol)
        if book and book.get("ready"):
            return True

    # Start stream if not running
    start_ob_ws(symbol)

    # Wait for book to be ready
    deadline = time.time() + wait_sec
    while time.time() < deadline:
        with _ob_book_lock:
            book = _ob_books.get(symbol)
            if book and book.get("ready"):
                return True
        time.sleep(0.2)

    return False


def get_ob_zone_levels(symbol: str, zone_top: float, zone_bottom: float,
                        ob_type: str, bucket_size: float = 2.0) -> Dict[str, Any]:
    """
    Returns a centered depth ladder for a zone.
    Center = zone midpoint (OB) or exact price (Fib).
    5 rows above + 5 rows below center.
    Bids and Asks shown separately.
    Spacing = dynamic % of center price.
    """
    with _ob_book_lock:
        book = _ob_books.get(symbol)

    if not book or not book.get("ready"):
        return {"ready": False, "ladder": [], "error": "book not ready"}

    bids_all = book.get("bids", {})
    asks_all = book.get("asks", {})
    age_sec  = round(time.time() - book.get("ts", 0), 1)

    # ── Center price ──
    center   = (zone_top + zone_bottom) / 2
    zone_size = abs(zone_top - zone_bottom)

    # ── Step size: max of (0.1% of price) OR (zone_size / 6) ──
    # % of price → correct for Fib (no zone size)
    # zone_size/6 → correct for wide OB zones (4H/1D)
    # Whichever is larger wins so OB always fits in center rows
    raw_step  = center * 0.001
    zone_step = zone_size / 6 if zone_size > 0 else 0
    raw_step  = max(raw_step, zone_step)
    if raw_step <= 0:
        step = 0.01
    else:
        mag = 10 ** math.floor(math.log10(raw_step))
        n   = raw_step / mag
        if n < 1.5:   step = mag
        elif n < 3.5: step = 2 * mag
        elif n < 7.5: step = 5 * mag
        else:         step = 10 * mag

    # Ensure at least 3 steps across zone
    if zone_size > 0 and step > zone_size / 3:
        step = round(zone_size / 3, 8)

    # ── Build 5 rows above + 5 rows below center ──
    ROWS = 5
    row_prices = []
    for i in range(ROWS, 0, -1):
        row_prices.append(("above", round(center + i * step, 8)))
    for i in range(1, ROWS + 1):
        row_prices.append(("below", round(center - i * step, 8)))

    # All row prices sorted descending
    all_row_prices_sorted = sorted(
        [(pos, p) for pos, p in row_prices],
        key=lambda x: x[1], reverse=True
    )

    # ── Aggregate book into step-size buckets ──
    def _bucket_qty(book_side: Dict[float, float], target_price: float) -> float:
        """Sum all orders within ±step/2 of target_price."""
        lo = target_price - step / 2
        hi = target_price + step / 2
        return sum(q for p, q in book_side.items() if lo <= p <= hi and q > 0)

    # ── Global avg for classification (using all book levels) ──
    all_qtys_b = list(bids_all.values())
    all_qtys_a = list(asks_all.values())
    all_qtys   = all_qtys_b + all_qtys_a
    avg_qty    = sum(all_qtys) / max(len(all_qtys), 1) if all_qtys else 1.0
    avg_bucket = avg_qty * max(step / 0.01, 1)

    def _classify(qty: float) -> tuple:
        r = qty / max(avg_bucket, 1e-10)
        if r >= 5.0: return "EXTREME", "🔴"
        if r >= 2.5: return "HEAVY",   "🟠"
        if r >= 1.5: return "MODERATE","🟡"
        return "WEAK", "⚪"

    def _bar(qty: float, max_qty: float) -> str:
        if max_qty <= 0: return ""
        pct = qty / max_qty
        bars = int(pct * 8)
        return "█" * bars + "░" * (8 - bars)

    # Pre-calc max qty for bar scaling
    all_row_qtys = []
    for _, rp in all_row_prices_sorted:
        all_row_qtys.append(_bucket_qty(bids_all, rp) + _bucket_qty(asks_all, rp))
    max_qty = max(all_row_qtys) if all_row_qtys else 1.0

    # ── Build ladder rows ──
    ladder = []
    for (pos, rp), row_qty in zip(all_row_prices_sorted, all_row_qtys):
        bid_qty  = _bucket_qty(bids_all, rp)
        ask_qty  = _bucket_qty(asks_all, rp)
        # Use dominant side for classification
        dom_qty  = max(bid_qty, ask_qty)
        cls, icon = _classify(dom_qty)
        bid_usdt = bid_qty * rp
        ask_usdt = ask_qty * rp

        # Zone position label
        if rp > zone_top:
            zone_pos = "above"
        elif rp < zone_bottom:
            zone_pos = "below"
        else:
            zone_pos = "inside"

        # Price formatting
        if center >= 1000:
            price_str = f"{rp:.2f}"
        elif center >= 10:
            price_str = f"{rp:.3f}"
        elif center >= 0.1:
            price_str = f"{rp:.4f}"
        else:
            price_str = f"{rp:.6f}"

        ladder.append({
            "price":    price_str,
            "price_f":  round(rp, 8),
            "pos":      pos,       # "above" or "below"
            "zone_pos": zone_pos,  # "above", "inside", "below"
            "bid_qty":  round(bid_qty, 4),
            "ask_qty":  round(ask_qty, 4),
            "bid_usdt": round(bid_usdt, 2),
            "ask_usdt": round(ask_usdt, 2),
            "bid_fmt":  fmt_vol(bid_usdt),
            "ask_fmt":  fmt_vol(ask_usdt),
            "bid_coin": fmt_vol(bid_qty),
            "ask_coin": fmt_vol(ask_qty),
            "class":    cls,
            "icon":     icon,
            "bar":      _bar(dom_qty, max_qty),
        })

    # ── Totals ──
    total_bid_usdt = sum(r["bid_usdt"] for r in ladder)
    total_ask_usdt = sum(r["ask_usdt"] for r in ladder)
    extreme_count  = sum(1 for r in ladder if r["class"] == "EXTREME")
    heavy_count    = sum(1 for r in ladder if r["class"] == "HEAVY")

    # ── Insight ──
    below_bids = sum(r["bid_usdt"] for r in ladder if r["zone_pos"] == "below")
    above_asks = sum(r["ask_usdt"] for r in ladder if r["zone_pos"] == "above")
    inside_total = sum(r["bid_usdt"] + r["ask_usdt"] for r in ladder if r["zone_pos"] == "inside")

    if below_bids > above_asks * 1.5:
        insight = "Buy wall below → strong support at this level"
    elif above_asks > below_bids * 1.5:
        insight = "Sell wall above → resistance at this level"
    elif inside_total > (below_bids + above_asks) * 0.5:
        insight = "Heavy orders inside zone → strong reaction likely"
    else:
        insight = "Balanced liquidity — no dominant wall detected"

    # ── Verdict ──
    strong = extreme_count + heavy_count
    if extreme_count >= 2 or (extreme_count >= 1 and heavy_count >= 1):
        verdict = "INSTITUTIONAL"
    elif strong >= 3:
        verdict = "STRONG"
    elif strong >= 1:
        verdict = "MODERATE"
    else:
        verdict = "WEAK"

    # Format center price
    if center >= 1000:
        center_str = f"{center:.2f}"
    elif center >= 10:
        center_str = f"{center:.3f}"
    elif center >= 0.1:
        center_str = f"{center:.4f}"
    else:
        center_str = f"{center:.6f}"

    # Zone boundary strings
    if zone_top >= 1000:
        zone_top_str    = f"{zone_top:.2f}"
        zone_bottom_str = f"{zone_bottom:.2f}"
    elif zone_top >= 10:
        zone_top_str    = f"{zone_top:.3f}"
        zone_bottom_str = f"{zone_bottom:.3f}"
    else:
        zone_top_str    = f"{zone_top:.4f}"
        zone_bottom_str = f"{zone_bottom:.4f}"

    return {
        "ready":         True,
        "ladder":        ladder,
        "center":        center_str,
        "center_f":      round(center, 8),
        "zone_top":      zone_top_str,
        "zone_bottom":   zone_bottom_str,
        "step":          round(step, 8),
        "verdict":       verdict,
        "insight":       insight,
        "total_bid_usdt": round(total_bid_usdt, 2),
        "total_ask_usdt": round(total_ask_usdt, 2),
        "total_bid_fmt":  fmt_vol(total_bid_usdt),
        "total_ask_fmt":  fmt_vol(total_ask_usdt),
        "extreme_count": extreme_count,
        "heavy_count":   heavy_count,
        "book_age_sec":  age_sec,
        "total_bids":    len(bids_all),
        "total_asks":    len(asks_all),
        "ob_type":       ob_type,
        "error":         None,
    }

    side      = "bids" if ob_type == "bullish" else "asks"
    all_levels = book[side]

    if not all_levels:
        return {"ready": True, "levels": [], "total_usdt": 0, "total_fmt": "0",
                "extreme_count": 0, "heavy_count": 0, "moderate_count": 0,
                "verdict": "EMPTY", "verdict_desc": "No orders in book",
                "zone_note": "empty", "book_age_sec": 0,
                "total_bids": 0, "total_asks": 0, "error": None}

    # ── Determine bucket size dynamically — 0.1% of zone midpoint ──
    # Works for any coin at any price:
    # ETH $2240  → $2.24 bucket
    # SOL $140   → $0.14 bucket
    # ALLU $0.10 → $0.0001 bucket
    # PEPE $0.00001 → $0.00000001 bucket
    zone_mid  = (zone_top + zone_bottom) / 2
    zone_size = abs(zone_top - zone_bottom)

    raw_bucket = zone_mid * 0.001  # 0.1% of price

    # Round to a clean number (1, 2, 5, 10 style)
    if raw_bucket <= 0:
        bucket = 0.01
    else:
        magnitude = 10 ** math.floor(math.log10(raw_bucket))
        normalized = raw_bucket / magnitude
        if normalized < 1.5:   bucket = magnitude
        elif normalized < 3.5: bucket = 2 * magnitude
        elif normalized < 7.5: bucket = 5 * magnitude
        else:                  bucket = 10 * magnitude

    # Ensure at least 5 buckets across the zone
    min_buckets = 5
    if zone_size > 0 and bucket > zone_size / min_buckets:
        bucket = zone_size / min_buckets

    # ── Filter: zone + 50% buffer ──
    zone_buffer = max(zone_size * 0.5, bucket * 5)
    zone_low    = zone_bottom - zone_buffer
    zone_high   = zone_top    + zone_buffer

    # Filter to zone range
    zone_prices = {p: q for p, q in all_levels.items()
                   if zone_low <= p <= zone_high and q > 0}

    # Note
    in_zone_count = sum(1 for p in zone_prices if zone_bottom <= p <= zone_top)
    zone_note = "in_zone" if in_zone_count > 0 else "near_zone"

    # If still empty — nearest 20 levels to zone mid
    if not zone_prices:
        nearest = sorted(all_levels.items(), key=lambda x: abs(x[0] - zone_mid))[:20]
        zone_prices = {p: q for p, q in nearest if q > 0}
        zone_note   = "nearest_to_zone"

    # ── Aggregate into price buckets ──
    buckets: Dict[float, float] = {}  # bucket_floor → total_qty
    for p, q in zone_prices.items():
        floor = round(math.floor(p / bucket) * bucket, 8)
        buckets[floor] = buckets.get(floor, 0.0) + q

    # ── Classify relative to all levels in the book ──
    all_qtys = list(all_levels.values())
    avg_qty  = sum(all_qtys) / max(len(all_qtys), 1)

    # But use bucket totals for classification (bucket vs avg individual)
    # Scale avg by expected orders per bucket
    avg_per_bucket = avg_qty * max(bucket / 0.01, 1)  # expected orders in bucket

    def _classify(qty):
        r = qty / max(avg_per_bucket, 1e-10)
        if r >= 5.0:  return "EXTREME", "🔴"
        if r >= 2.5:  return "HEAVY",   "🟠"
        if r >= 1.5:  return "MODERATE","🟡"
        return "WEAK", "⚪"

    levels_out    = []
    total_usdt    = 0.0
    extreme_count = heavy_count = moderate_count = 0

    sorted_buckets = sorted(buckets.items(), key=lambda x: x[0],
                             reverse=(ob_type == "bearish"))

    for floor_p, total_q in sorted_buckets[:20]:  # show top 20 buckets
        mid_p    = floor_p + bucket / 2
        cls, icon = _classify(total_q)
        usdt_val  = total_q * mid_p
        # Price label: show range
        top_p = floor_p + bucket
        label = f"{floor_p:.2f}–{top_p:.2f}" if bucket >= 1 else f"{floor_p:.4f}–{top_p:.4f}"
        levels_out.append({
            "price":   label,
            "qty":     round(total_q, 4),
            "qtyFmt":  fmt_vol(total_q),
            "usdt":    round(usdt_val, 2),
            "usdtFmt": fmt_vol(usdt_val),
            "class":   cls,
            "icon":    icon,
            "ratio":   round(total_q / max(avg_per_bucket, 1e-10), 2),
        })
        total_usdt += usdt_val
        if cls == "EXTREME":    extreme_count += 1
        elif cls == "HEAVY":    heavy_count   += 1
        elif cls == "MODERATE": moderate_count += 1

    # Verdict
    strong     = extreme_count + heavy_count
    zone_label = {"in_zone": "in zone", "near_zone": "near zone",
                  "nearest_to_zone": "nearest to zone", "empty": "empty"}[zone_note]
    if extreme_count >= 2 or (extreme_count >= 1 and heavy_count >= 2):
        verdict, verdict_desc = "INSTITUTIONAL", f"Extreme institutional liquidity ({zone_label})"
    elif strong >= 3:
        verdict, verdict_desc = "STRONG", f"Multiple heavy walls ({zone_label})"
    elif strong >= 1:
        verdict, verdict_desc = "MODERATE", f"Some liquidity present ({zone_label})"
    elif moderate_count >= 2:
        verdict, verdict_desc = "WEAK", f"Mostly normal orders ({zone_label})"
    else:
        verdict, verdict_desc = "EMPTY", f"Very little liquidity ({zone_label})"

    age_sec = time.time() - book.get("ts", 0)

    return {
        "ready":         True,
        "levels":        levels_out,
        "total_usdt":    round(total_usdt, 2),
        "total_fmt":     fmt_vol(total_usdt),
        "extreme_count": extreme_count,
        "heavy_count":   heavy_count,
        "moderate_count": moderate_count,
        "verdict":       verdict,
        "verdict_desc":  verdict_desc,
        "zone_note":     zone_note,
        "book_age_sec":  round(age_sec, 1),
        "total_bids":    len(book["bids"]),
        "total_asks":    len(book["asks"]),
        "bucket_size":   bucket,
        "error":         None,
    }

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


# ============================================================
# ORDER FLOW ENGINE — Phase 1
# aggTrades absorption + Open Interest + Funding Rate
# ============================================================

def fetch_orderflow_data(symbol: str) -> Dict[str, Any]:
    """
    Fetch aggTrades + Open Interest + Funding Rate from Binance Futures.
    Never crashes screener — always returns fallback dict on error.
    """
    result: Dict[str, Any] = {
        "trades":       [],
        "oi":           None,
        "oi_change":    None,
        "funding_rate": None,
        "error":        None,
    }

    # 1. aggTrades — last 1000 trades for absorption
    try:
        r = req.get(
            f"{BINANCE_FUTURES_API}/fapi/v1/aggTrades",
            params={"symbol": symbol, "limit": 1000},
            timeout=5,
        )
        if r.status_code == 200:
            result["trades"] = r.json()
    except Exception as e:
        result["error"] = f"aggTrades:{e}"

    # 2. Open Interest — current + 5min history for change direction
    try:
        r_oih = req.get(
            f"{BINANCE_FUTURES_API}/futures/data/openInterestHist",
            params={"symbol": symbol, "period": "5m", "limit": 2},
            timeout=5,
        )
        if r_oih.status_code == 200:
            hist = r_oih.json()
            if len(hist) >= 2:
                oi_now  = safe_float(hist[-1].get("sumOpenInterest", 0))
                oi_prev = safe_float(hist[-2].get("sumOpenInterest", 0))
                result["oi"] = oi_now
                if oi_prev > 0:
                    result["oi_change"] = (oi_now - oi_prev) / oi_prev * 100.0
    except Exception as e:
        result["error"] = f"OI:{e}"

    # 3. Funding Rate
    try:
        r_fr = req.get(
            f"{BINANCE_FUTURES_API}/fapi/v1/premiumIndex",
            params={"symbol": symbol},
            timeout=5,
        )
        if r_fr.status_code == 200:
            result["funding_rate"] = safe_float(
                r_fr.json().get("lastFundingRate", 0)
            ) * 100.0
    except Exception as e:
        result["error"] = f"Funding:{e}"

    return result


def analyze_orderflow(
    of_data:     Dict[str, Any],
    price:       float,
    ob_type:     str,
    zone_top:    float,
    zone_bottom: float,
) -> Dict[str, Any]:
    """
    Detect absorption, OI confirmation, funding rate context.
    ob_type: "bullish" or "bearish"

    Returns dict with:
      absorption      : BUYER | SELLER | NONE
      absorption_str  : Strong | Moderate | Weak | None
      delta           : float
      buy_volume      : float
      sell_volume     : float
      oi_signal       : rising | falling | flat | unknown
      funding_context : overleveraged_long | overleveraged_short | neutral | unknown
      score_delta     : int  (add to OB quality score)
      checklist_pass  : bool
      summary         : str
    """
    out: Dict[str, Any] = {
        "absorption":      "NONE",
        "absorption_str":  "None",
        "delta":           0.0,
        "buy_volume":      0.0,
        "sell_volume":     0.0,
        "oi_signal":       "unknown",
        "funding_context": "unknown",
        "score_delta":     0,
        "checklist_pass":  False,
        "summary":         "No data",
    }

    trades = of_data.get("trades", [])

    # ── Delta from aggTrades ──
    # m=True  → buyer is maker → SELLER aggressor
    # m=False → buyer is taker → BUYER aggressor
    buy_vol  = 0.0
    sell_vol = 0.0
    for t in trades:
        qty = safe_float(t.get("q", 0))
        if t.get("m", False):
            sell_vol += qty
        else:
            buy_vol  += qty

    total_vol = buy_vol + sell_vol
    delta     = buy_vol - sell_vol
    out["buy_volume"]  = round(buy_vol,  2)
    out["sell_volume"] = round(sell_vol, 2)
    out["delta"]       = round(delta,    2)

    # ── Absorption detection ──
    if total_vol > 0:
        buy_pct  = buy_vol  / total_vol * 100.0
        sell_pct = sell_vol / total_vol * 100.0

        price_in_zone  = zone_bottom <= price <= zone_top
        zone_size      = max(zone_top - zone_bottom, 1e-10)
        dist_from_zone = (
            0.0 if price_in_zone
            else min(abs(price - zone_top), abs(price - zone_bottom)) / zone_size * 100.0
        )
        price_near_zone = price_in_zone or dist_from_zone <= 15.0

        # Buyer absorption: heavy selling but price NOT dropping through bullish OB
        buyer_absorption = (
            sell_pct >= 58.0
            and delta < 0
            and price_near_zone
            and ob_type == "bullish"
        )
        # Seller absorption: heavy buying but price NOT breaking above bearish OB
        seller_absorption = (
            buy_pct  >= 58.0
            and delta > 0
            and price_near_zone
            and ob_type == "bearish"
        )

        if buyer_absorption:
            out["absorption"] = "BUYER"
            out["absorption_str"] = (
                "Strong"   if sell_pct >= 70.0 else
                "Moderate" if sell_pct >= 63.0 else "Weak"
            )
        elif seller_absorption:
            out["absorption"] = "SELLER"
            out["absorption_str"] = (
                "Strong"   if buy_pct >= 70.0 else
                "Moderate" if buy_pct >= 63.0 else "Weak"
            )

    # ── OI Signal ──
    oi_change = of_data.get("oi_change")
    if oi_change is not None:
        out["oi_signal"] = (
            "rising"  if oi_change >  0.5 else
            "falling" if oi_change < -0.5 else "flat"
        )

    # ── Funding Rate ──
    fr = of_data.get("funding_rate")
    if fr is not None:
        out["funding_context"] = (
            "overleveraged_long"  if fr >  0.05 else
            "overleveraged_short" if fr < -0.05 else "neutral"
        )

    # ── Score adjustments ──
    score_adj = 0

    # Absorption confirming OB direction
    if out["absorption"] == "BUYER" and ob_type == "bullish":
        out["checklist_pass"] = True
        score_adj += {"Strong": 15, "Moderate": 10, "Weak": 5}.get(out["absorption_str"], 5)
    elif out["absorption"] == "SELLER" and ob_type == "bearish":
        out["checklist_pass"] = True
        score_adj += {"Strong": 15, "Moderate": 10, "Weak": 5}.get(out["absorption_str"], 5)
    # Absorption opposing OB = trap warning
    elif out["absorption"] == "SELLER" and ob_type == "bullish":
        score_adj -= 10
    elif out["absorption"] == "BUYER" and ob_type == "bearish":
        score_adj -= 10

    # OI confirmation
    if out["oi_signal"] == "rising":
        score_adj += 10
    elif out["oi_signal"] == "falling":
        score_adj += 3

    # Funding rate context
    if ob_type == "bullish" and out["funding_context"] == "overleveraged_short":
        score_adj += 8   # shorts squeezable = fuel for bullish OB
    elif ob_type == "bearish" and out["funding_context"] == "overleveraged_long":
        score_adj += 8   # longs liquidatable = fuel for bearish OB
    elif ob_type == "bullish" and out["funding_context"] == "overleveraged_long":
        score_adj -= 5   # overcrowded long = risky
    elif ob_type == "bearish" and out["funding_context"] == "overleveraged_short":
        score_adj -= 5   # overcrowded short = risky

    out["score_delta"] = score_adj

    # ── Human readable summary ──
    def _fmt_delta(d: float) -> str:
        abs_d = abs(d)
        sign  = "+" if d >= 0 else "-"
        if abs_d >= 1e6:   return f"{sign}{abs_d/1e6:.2f}M"
        if abs_d >= 1e3:   return f"{sign}{abs_d/1e3:.1f}K"
        return f"{sign}{abs_d:.1f}"

    parts = []
    if out["absorption"] != "NONE":
        parts.append(f"{out['absorption']} Abs ({out['absorption_str']})")
    if out["oi_signal"] != "unknown":
        parts.append(f"OI:{out['oi_signal']}")
    fc = out["funding_context"]
    if fc == "overleveraged_long":
        parts.append("Fund:OL-Long")
    elif fc == "overleveraged_short":
        parts.append("Fund:OL-Short")
    parts.append(f"Δ{_fmt_delta(out['delta'])}")
    out["summary"] = " | ".join(parts)

    return out


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
    # ── Skip last candle — it's the currently open (unconfirmed) candle ──
    # FVG requires Candle 3 to be CLOSED to confirm the gap
    # Using the open candle as Candle 3 = unconfirmed FVG
    loop_end = n - 1  # exclude index n-1 (open candle)
    for i in range(2, loop_end):
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

    # Note: We include all candles including current open candle for OB detection.
    # OB is confirmed by BOS (break of structure) — the BOS candle can be current.
    # Unlike FVG which needs 3 closed candles, OB only needs the BOS to occur.
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


def obq_compute_overlap_pct(a_top: float, a_bottom: float, b_top: float, b_bottom: float) -> float:
    """Safe overlap % helper for OB Quality Engine only."""
    overlap_top    = min(a_top, b_top)
    overlap_bottom = max(a_bottom, b_bottom)
    overlap = max(0.0, overlap_top - overlap_bottom)
    a_size = max(a_top - a_bottom, 1e-10)
    b_size = max(b_top - b_bottom, 1e-10)
    base = min(a_size, b_size)
    return (overlap / base) * 100.0 if base > 0 else 0.0


def obq_dist_from_price(price: float, zone_top: float, zone_bottom: float, direction: str) -> float:
    """Safe distance helper for OB Quality Engine only. Returns 0 if inside zone."""
    if zone_bottom <= price <= zone_top:
        return 0.0
    if direction == "bullish":
        if price > zone_top:
            return ((price - zone_top) / max(price, 1e-10)) * 100.0
        return ((zone_bottom - price) / max(price, 1e-10)) * 100.0
    if price < zone_bottom:
        return ((zone_bottom - price) / max(price, 1e-10)) * 100.0
    return ((price - zone_top) / max(price, 1e-10)) * 100.0


def _derive_prev_day_week_levels(times: List[int], high: List[float], low: List[float]) -> Dict[str, Optional[float]]:
    """Derive previous-day and previous-week high/low from the SAME candle stream."""
    from datetime import datetime, timezone
    if not times or len(times) != len(high) or len(times) != len(low):
        return {"pdh": None, "pdl": None, "pwh": None, "pwl": None}
    day_map: Dict[tuple, Dict[str, float]] = {}
    week_map: Dict[tuple, Dict[str, float]] = {}
    for ts, hh, ll in zip(times, high, low):
        if not ts:
            continue
        try:
            dt = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc)
        except Exception:
            continue
        day_key = (dt.year, dt.month, dt.day)
        iso = dt.isocalendar()
        week_key = (iso.year, iso.week)
        if day_key not in day_map:
            day_map[day_key] = {"high": hh, "low": ll}
        else:
            day_map[day_key]["high"] = max(day_map[day_key]["high"], hh)
            day_map[day_key]["low"]  = min(day_map[day_key]["low"],  ll)
        if week_key not in week_map:
            week_map[week_key] = {"high": hh, "low": ll}
        else:
            week_map[week_key]["high"] = max(week_map[week_key]["high"], hh)
            week_map[week_key]["low"]  = min(week_map[week_key]["low"],  ll)
    if not day_map or not week_map:
        return {"pdh": None, "pdl": None, "pwh": None, "pwl": None}
    try:
        last_dt = datetime.fromtimestamp(times[-1] / 1000.0, tz=timezone.utc)
        current_day  = (last_dt.year, last_dt.month, last_dt.day)
        current_iso  = last_dt.isocalendar()
        current_week = (current_iso.year, current_iso.week)
    except Exception:
        return {"pdh": None, "pdl": None, "pwh": None, "pwl": None}
    prev_days  = sorted([k for k in day_map.keys()  if k < current_day])
    prev_weeks = sorted([k for k in week_map.keys() if k < current_week])
    pdh = pdl = pwh = pwl = None
    if prev_days:
        d = day_map[prev_days[-1]];  pdh, pdl = d["high"], d["low"]
    if prev_weeks:
        w = week_map[prev_weeks[-1]]; pwh, pwl = w["high"], w["low"]
    return {"pdh": pdh, "pdl": pdl, "pwh": pwh, "pwl": pwl}


def _format_ob_checks(meta: Dict[str, Any]) -> str:
    """Compact checklist line for OB quality transparency."""
    return (
        f'Checks: '
        f'{"✓" if meta.get("sweepPass") else "✗"} Sweep  '
        f'{"✓" if meta.get("dispPass") else "✗"} Disp  '
        f'{"✓" if meta.get("fvgPass") else "✗"} FVG  '
        f'{"✓" if meta.get("pdPass") else "✗"} PD  '
        f'{"✓" if meta.get("htfPass") else "✗"} HTF  '
        f'{"✓" if meta.get("safePass") else "✗"} Safe  '
        f'{"✓" if meta.get("absorptionPass") else "✗"} Absorption'
    )


def score_ob_quality(ob: Dict[str, Any], o: List[float], h: List[float], l: List[float],
                     c: List[float], v: List[float], obs_all: List[Dict[str, Any]],
                     fvgs: List[Dict[str, Any]], itrend: int, trend: int,
                     times: Optional[List[int]] = None) -> Tuple[int, Dict[str, Any]]:
    """Accuracy-tuned OB quality score (0-100)."""
    n = len(c)
    ob_bar  = int(ob.get("bar", -1))
    ob_type = ob.get("type", "")
    if ob_bar < 1 or ob_bar >= n - 1 or ob_type not in ("bullish", "bearish"):
        return 0, {
            "sweepPass": False, "dispPass": False, "fvgPass": False,
            "pdPass": False, "htfPass": False, "safePass": False,
            "sweepType": "none", "fvgState": "none", "pdState": "none",
            "htfBiasState": "neutral", "dangerState": "unknown", "retestState": "unknown",
        }

    zone_top    = float(ob["top"])
    zone_bottom = float(ob["bottom"])
    zone_mid    = (zone_top + zone_bottom) / 2.0
    zone_size   = max(zone_top - zone_bottom, 1e-10)

    atr_vals = calc_atr(h, l, c, 14)
    atr_here = (atr_vals[ob_bar] if ob_bar < len(atr_vals) and atr_vals[ob_bar] is not None
                else max(h[ob_bar] - l[ob_bar], 1e-10))

    score = 0.0
    meta = {
        "sweepPass": False, "dispPass": False, "fvgPass": False,
        "pdPass": False, "htfPass": False, "safePass": False,
        "sweepType": "none", "fvgState": "none", "pdState": "none",
        "htfBiasState": "neutral", "dangerState": "unknown", "retestState": "fresh",
    }

    # 1) Sweep classification
    lookback = min(60, ob_bar)
    recent_highs: List[float] = []
    recent_lows: List[float]  = []
    for j in range(max(5, ob_bar - lookback), ob_bar):
        left = max(0, j - 5); right = min(n, j + 6)
        if h[j] == max(h[left:right]): recent_highs.append(h[j])
        if l[j] == min(l[left:right]): recent_lows.append(l[j])

    if ob_type == "bearish" and recent_highs:
        close_highs = [x for x in recent_highs if abs(x - zone_top) / max(zone_top, 1e-10) * 100 <= 0.65]
        if len(close_highs) >= 2 and (max(close_highs) - min(close_highs)) / max(zone_top, 1e-10) * 100 <= 0.18:
            meta["sweepPass"] = True; meta["sweepType"] = "equal_highs"; score += 18
        elif close_highs:
            meta["sweepPass"] = True; meta["sweepType"] = "swing_high"; score += 15
        elif any(abs(x - zone_top) / max(zone_top, 1e-10) * 100 <= 1.10 for x in recent_highs):
            meta["sweepType"] = "internal_high"; score += 6
    elif ob_type == "bullish" and recent_lows:
        close_lows = [x for x in recent_lows if abs(x - zone_bottom) / max(zone_bottom, 1e-10) * 100 <= 0.65]
        if len(close_lows) >= 2 and (max(close_lows) - min(close_lows)) / max(zone_bottom, 1e-10) * 100 <= 0.18:
            meta["sweepPass"] = True; meta["sweepType"] = "equal_lows"; score += 18
        elif close_lows:
            meta["sweepPass"] = True; meta["sweepType"] = "swing_low"; score += 15
        elif any(abs(x - zone_bottom) / max(zone_bottom, 1e-10) * 100 <= 1.10 for x in recent_lows):
            meta["sweepType"] = "internal_low"; score += 6

    # 2) Displacement (ATR-relative)
    disp_end   = min(n, ob_bar + 5)
    same_dir   = 0
    total_body = 0.0
    total_range = 0.0
    for j in range(ob_bar + 1, disp_end):
        body = abs(c[j] - o[j]); rng = max(h[j] - l[j], 1e-10)
        total_body += body; total_range += rng
        body_ratio = body / rng
        if ob_type == "bullish" and c[j] > o[j] and body_ratio >= 0.45:
            same_dir += 1
        elif ob_type == "bearish" and c[j] < o[j] and body_ratio >= 0.45:
            same_dir += 1
    if ob_type == "bullish":
        move_away = max(max(h[ob_bar + 1:disp_end], default=zone_top) - zone_top, 0.0)
    else:
        move_away = max(zone_bottom - min(l[ob_bar + 1:disp_end], default=zone_bottom), 0.0)
    body_avg_ratio = (total_body / max(total_range, 1e-10)) if total_range > 0 else 0.0
    move_atr = move_away / max(atr_here, 1e-10)
    if move_atr >= 0.60 and (same_dir >= 2 or body_avg_ratio >= 0.48):
        meta["dispPass"] = True; score += 18
        if move_atr >= 1.0: score += 6
    elif move_atr >= 0.40 and (same_dir >= 1 or body_avg_ratio >= 0.42):
        score += 8

    # 3) FVG (tightened)
    best_overlap  = 0.0
    best_near     = None
    best_age_gap  = None
    for fvg in fvgs:
        if fvg.get("direction") != ob_type:
            continue
        fvg_size_pct = abs(fvg["top"] - fvg["bottom"]) / max(zone_mid, 1e-10) * 100.0
        if fvg_size_pct < 0.08:
            continue
        age_gap = abs(int(fvg.get("bar", ob_bar)) - ob_bar)
        overlap = compute_overlap_pct(zone_bottom, zone_top, fvg["bottom"], fvg["top"])
        fvg_mid = (fvg["top"] + fvg["bottom"]) / 2.0
        near_pct = abs(fvg_mid - zone_mid) / max(zone_mid, 1e-10) * 100.0
        if overlap > best_overlap:          best_overlap = overlap
        if best_near is None or near_pct < best_near:     best_near = near_pct
        if best_age_gap is None or age_gap < best_age_gap: best_age_gap = age_gap
    if best_overlap >= 20.0 and best_age_gap is not None and best_age_gap <= 12:
        meta["fvgPass"] = True; meta["fvgState"] = "overlap"; score += 18
    elif best_near is not None and best_near <= 0.20 and best_age_gap is not None and best_age_gap <= 10:
        meta["fvgPass"] = True; meta["fvgState"] = "near"; score += 12
    elif best_near is not None and best_near <= 0.40 and best_age_gap is not None and best_age_gap <= 8:
        meta["fvgState"] = "near_weak"; score += 4

    # 4) PD array alignment (previous day/week levels)
    pd = _derive_prev_day_week_levels(times or [], h, l)
    pd_hits: List[str] = []
    if ob_type == "bearish":
        if pd["pdh"] is not None and abs(pd["pdh"] - zone_top)    / max(zone_top,    1e-10) * 100 <= 0.50: pd_hits.append("PDH")
        if pd["pwh"] is not None and abs(pd["pwh"] - zone_top)    / max(zone_top,    1e-10) * 100 <= 0.65: pd_hits.append("PWH")
    else:
        if pd["pdl"] is not None and abs(pd["pdl"] - zone_bottom) / max(zone_bottom, 1e-10) * 100 <= 0.50: pd_hits.append("PDL")
        if pd["pwl"] is not None and abs(pd["pwl"] - zone_bottom) / max(zone_bottom, 1e-10) * 100 <= 0.65: pd_hits.append("PWL")
    if pd_hits:
        meta["pdPass"] = True; meta["pdState"] = "/".join(pd_hits)
        score += 12 if len(pd_hits) >= 2 else 9

    # 5) HTF / structure bias alignment
    if ob_type == "bullish" and trend == 1:
        meta["htfPass"] = True; meta["htfBiasState"] = "aligned"; score += 12
    elif ob_type == "bearish" and trend == -1:
        meta["htfPass"] = True; meta["htfBiasState"] = "aligned"; score += 12
    elif ob_type == "bullish" and itrend == 1:
        meta["htfPass"] = True; meta["htfBiasState"] = "internal"; score += 7
    elif ob_type == "bearish" and itrend == -1:
        meta["htfPass"] = True; meta["htfBiasState"] = "internal"; score += 7
    else:
        meta["htfBiasState"] = "counter"

    # 6) Safe / opposing danger (tightened)
    dangers: List[float] = []
    for other in obs_all:
        if other.get("type") == ob_type:
            continue
        if ob_type == "bearish" and other["top"] <= zone_bottom:
            dangers.append((zone_bottom - other["top"]) / max(zone_bottom, 1e-10) * 100.0)
        elif ob_type == "bullish" and other["bottom"] >= zone_top:
            dangers.append((other["bottom"] - zone_top) / max(zone_top, 1e-10) * 100.0)
    for fvg in fvgs:
        if fvg.get("direction") == ob_type:
            continue
        if ob_type == "bearish" and fvg["top"] <= zone_bottom:
            dangers.append((zone_bottom - fvg["top"]) / max(zone_bottom, 1e-10) * 100.0)
        elif ob_type == "bullish" and fvg["bottom"] >= zone_top:
            dangers.append((fvg["bottom"] - zone_top) / max(zone_top, 1e-10) * 100.0)
    if ob_type == "bearish":
        for lv in (pd.get("pdl"), pd.get("pwl")):
            if lv is not None and lv <= zone_bottom:
                dangers.append((zone_bottom - lv) / max(zone_bottom, 1e-10) * 100.0)
    else:
        for lv in (pd.get("pdh"), pd.get("pwh")):
            if lv is not None and lv >= zone_top:
                dangers.append((lv - zone_top) / max(zone_top, 1e-10) * 100.0)
    nearest_danger = min(dangers) if dangers else None
    if nearest_danger is None:
        meta["safePass"] = True; meta["dangerState"] = "clear"; score += 10
    elif nearest_danger > 1.20:
        meta["safePass"] = True; meta["dangerState"] = "safe";  score += 8
    elif nearest_danger > 0.80:
        meta["dangerState"] = "moderate"; score += 2
    else:
        meta["dangerState"] = "close";    score -= 8

    # 7) Freshness / mitigation
    age = max(0, (n - 1) - ob_bar)
    if age <= 12:   score += 6; meta["retestState"] = "fresh"
    elif age <= 28: score += 3; meta["retestState"] = "recent"
    else:           meta["retestState"] = "old"
    current_price = c[-1]
    if zone_bottom <= current_price <= zone_top:
        depth = (zone_top - current_price) / zone_size if ob_type == "bullish" else (current_price - zone_bottom) / zone_size
        if depth > 0.80:
            score -= 5; meta["retestState"] = "deep_mitigation"
        elif depth <= 0.35:
            score += 2; meta["retestState"] = "shallow_tap"

    return int(clamp(score, 0, 100)), meta


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
# FIB MODULE v2 — ZigZag Pivot + ATR Adaptive
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


def _get_fib_tf_defaults(tf):
    defaults = {
        "15m": {"pivot_len": 5, "min_bars": 4, "lookback": 80},
        "30m": {"pivot_len": 5, "min_bars": 4, "lookback": 60},
        "1h":  {"pivot_len": 5, "min_bars": 3, "lookback": 50},
        "2h":  {"pivot_len": 5, "min_bars": 3, "lookback": 40},
        "4h":  {"pivot_len": 6, "min_bars": 3, "lookback": 30},
        "6h":  {"pivot_len": 6, "min_bars": 3, "lookback": 25},
        "12h": {"pivot_len": 6, "min_bars": 2, "lookback": 20},
        "1d":  {"pivot_len": 6, "min_bars": 2, "lookback": 15},
    }
    return defaults.get(tf, defaults["1h"])


def find_zigzag_pivots(high, low, pivot_len):
    """
    Find confirmed swing pivots with left/right bar confirmation.
    Returns list of (bar_index, price, type) where type is 'H' or 'L'.
    """
    n = len(high)
    pivots = []
    for i in range(pivot_len, n - pivot_len):
        is_high = True
        for j in range(i - pivot_len, i + pivot_len + 1):
            if j != i and high[j] >= high[i]:
                is_high = False
                break
        is_low = True
        for j in range(i - pivot_len, i + pivot_len + 1):
            if j != i and low[j] <= low[i]:
                is_low = False
                break
        if is_high:
            pivots.append((i, high[i], "H"))
        if is_low:
            pivots.append((i, low[i], "L"))
    pivots.sort(key=lambda x: x[0])
    return pivots


def filter_zigzag_alternating(pivots):
    """Enforce strict alternating H→L→H→L, keeping the more extreme when duplicates occur."""
    if not pivots:
        return []
    filtered = [pivots[0]]
    for i in range(1, len(pivots)):
        current = pivots[i]
        last = filtered[-1]
        if current[2] == last[2]:
            if current[2] == "H" and current[1] > last[1]:
                filtered[-1] = current
            elif current[2] == "L" and current[1] < last[1]:
                filtered[-1] = current
        else:
            filtered.append(current)
    return filtered


def filter_pivots_by_atr(pivots, high, low, close, atr_multiplier=1.5, min_bar_spacing=3):
    """Remove pivot pairs where the move is too small (noise) or too close together."""
    if len(pivots) < 2:
        return pivots
    atr_values = calc_atr(high, low, close, 14)
    filtered = [pivots[0]]
    for i in range(1, len(pivots)):
        current = pivots[i]
        last = filtered[-1]
        bar_diff = abs(current[0] - last[0])
        if bar_diff < min_bar_spacing:
            if current[2] == last[2]:
                if current[2] == "H" and current[1] > last[1]:
                    filtered[-1] = current
                elif current[2] == "L" and current[1] < last[1]:
                    filtered[-1] = current
            continue
        move = abs(current[1] - last[1])
        mid_bar = min((current[0] + last[0]) // 2, len(atr_values) - 1)
        atr_at_mid = atr_values[mid_bar]
        if atr_at_mid is None:
            atr_at_mid = abs(high[mid_bar] - low[mid_bar])
        min_move = atr_at_mid * atr_multiplier
        if move < min_move:
            if current[2] == last[2]:
                if current[2] == "H" and current[1] > last[1]:
                    filtered[-1] = current
                elif current[2] == "L" and current[1] < last[1]:
                    filtered[-1] = current
            continue
        filtered.append(current)
    return filtered


def find_active_fib_leg_v2(o, h, l, c, v, tf="1h", atr_multiplier=1.5):
    """
    Dominant-leg version.
    Prefers the best meaningful retracing leg, not just the newest tiny move.
    """
    n = len(c)
    if n < 20:
        return None

    tf_defaults = _get_fib_tf_defaults(tf)
    pivot_len = tf_defaults["pivot_len"]
    lookback  = tf_defaults["lookback"]

    seg    = min(lookback, n)
    offset = n - seg
    seg_o  = o[offset:]; seg_h = h[offset:]; seg_l = l[offset:]
    seg_c  = c[offset:]; seg_v = v[offset:]

    if len(seg_c) < pivot_len * 2 + 5:
        return None

    raw_pivots = find_zigzag_pivots(seg_h, seg_l, pivot_len)
    if len(raw_pivots) < 2:
        return None
    alt_pivots = filter_zigzag_alternating(raw_pivots)
    if len(alt_pivots) < 2:
        return None

    min_bars_tf = fib_tf_min_bars(tf)
    valid_pivots = filter_pivots_by_atr(
        alt_pivots, seg_h, seg_l, seg_c,
        atr_multiplier=atr_multiplier,
        min_bar_spacing=min_bars_tf,
    )
    if len(valid_pivots) < 2:
        return None

    current_price = seg_c[-1]
    atr_values    = calc_atr(seg_h, seg_l, seg_c, 14)
    min_move_pct  = fib_tf_min_move_pct(tf)
    candidates    = []

    for i in range(1, len(valid_pivots)):
        p1 = valid_pivots[i - 1]
        p2 = valid_pivots[i]
        p1_bar, p1_price, p1_type = p1
        p2_bar, p2_price, p2_type = p2

        if p1_type == "L" and p2_type == "H":
            bullish = True;  a_price, b_price = p1_price, p2_price
        elif p1_type == "H" and p2_type == "L":
            bullish = False; a_price, b_price = p1_price, p2_price
        else:
            continue

        leg_start  = p1_bar
        leg_end    = p2_bar
        bars_count = _fib_bars_for_move(leg_start, leg_end)
        if bars_count < min_bars_tf:
            continue

        raw_range = abs(b_price - a_price)
        if raw_range <= 1e-10:
            continue
        move_pct = (raw_range / max(abs(a_price), 1e-10)) * 100.0
        if move_pct < min_move_pct:
            continue

        mid_bar = min((leg_start + leg_end) // 2, len(atr_values) - 1)
        atr_mid = atr_values[mid_bar] if atr_values[mid_bar] is not None else max(seg_h[mid_bar] - seg_l[mid_bar], 1e-10)

        # Extension: only if meaningful (> 0.5 ATR beyond B)
        ext_thresh = _fib_extension_threshold(atr_mid)
        if bullish:
            if leg_end + 1 < len(seg_h):
                highest_after = max(seg_h[k] for k in range(leg_end + 1, len(seg_h)))
                if highest_after > b_price + ext_thresh:
                    for k in range(leg_end + 1, len(seg_h)):
                        if seg_h[k] == highest_after:
                            leg_end = k; b_price = highest_after; break
        else:
            if leg_end + 1 < len(seg_l):
                lowest_after = min(seg_l[k] for k in range(leg_end + 1, len(seg_l)))
                if lowest_after < b_price - ext_thresh:
                    for k in range(leg_end + 1, len(seg_l)):
                        if seg_l[k] == lowest_after:
                            leg_end = k; b_price = lowest_after; break

        # Active retracement check
        if bullish:
            if current_price >= b_price: continue
            if any(seg_c[k] < a_price for k in range(leg_end + 1, len(seg_c))): continue
        else:
            if current_price <= b_price: continue
            if any(seg_c[k] > a_price for k in range(leg_end + 1, len(seg_c))): continue

        strength    = measure_impulse_strength(seg_o, seg_h, seg_l, seg_c, seg_v, leg_start, leg_end)
        atr_multiple = raw_range / max(atr_mid, 1e-10)
        recency     = leg_end / max(len(seg_c), 1)
        dominance   = min(move_pct, 25.0)
        score = (
            strength * 0.32 +
            atr_multiple * 12.0 +
            dominance * 1.2 +
            recency * 20.0 +
            min(bars_count, 12) * 1.0
        )

        candidates.append({
            "a": a_price, "b": b_price, "bullish": bullish,
            "leg_start": leg_start + offset, "leg_end": leg_end + offset,
            "impulse_strength": round(strength, 1),
            "move_pct": round(move_pct, 2),
            "atr_multiple": round(atr_multiple, 2),
            "bars_count": bars_count,
            "recency": round(recency, 3),
            "score": round(score, 1),
            "range": abs(b_price - a_price),
        })

    if not candidates:
        return None

    # Prefer dominant meaningful leg, not just latest tiny leg
    candidates.sort(
        key=lambda x: (x["score"], x["atr_multiple"], x["move_pct"], x["bars_count"], x["recency"]),
        reverse=True,
    )

    best = candidates[0]
    fib = _build_fib(best["a"], best["b"], "zigzag_pivot_dominant", best["bullish"])
    if fib:
        fib["leg_start"]        = best["leg_start"]
        fib["leg_end"]          = best["leg_end"]
        fib["impulse_strength"] = best["impulse_strength"]
        fib["move_pct"]         = best["move_pct"]
        fib["atr_multiple"]     = best["atr_multiple"]
        fib["bars_count"]       = best["bars_count"]
        fib["recency"]          = best["recency"]
        fib["leg_score"]        = best["score"]
    return fib


def get_active_fib_level(fib, current_price):
    """
    Single active level rule: once price passes through a level, shallower levels vanish.
    Returns list of valid level names price hasn't fully passed through yet.
    """
    if not fib or "levels" not in fib:
        return []
    is_bullish = fib["bullish"]
    levels = fib["levels"]
    level_order = ["0.5", "0.618", "0.705", "0.786"]
    valid_levels = []
    for level_name in level_order:
        if level_name not in levels:
            continue
        level_price = levels[level_name]
        if is_bullish:
            if current_price > level_price:
                valid_levels.append(level_name)
            else:
                valid_levels = [level_name]
        else:
            if current_price < level_price:
                valid_levels.append(level_name)
            else:
                valid_levels = [level_name]
    return valid_levels


def check_wick_rejection_v2(o, h, l, c, fib_level, is_bullish_leg, tolerance_pct=0.3):
    """Check last 2 closed candles for wick rejection at a Fib level."""
    n = len(c)
    if n < 3:
        return False, 0
    level_tolerance = fib_level * (tolerance_pct / 100)
    for idx in range(n - 2, max(n - 4, -1), -1):
        if idx < 0:
            break
        bar_o, bar_h, bar_l, bar_c = o[idx], h[idx], l[idx], c[idx]
        body_top    = max(bar_o, bar_c)
        body_bottom = min(bar_o, bar_c)
        body_size   = max(body_top - body_bottom, 1e-10)
        total_range = max(bar_h - bar_l, 1e-10)
        if is_bullish_leg:
            wick_below = body_bottom - bar_l
            touches    = bar_l <= fib_level + level_tolerance
            closed_above = bar_c > fib_level - level_tolerance
            if touches and closed_above:
                wick_ratio = wick_below / body_size if body_size > 1e-10 else 0
                if wick_ratio >= 0.8:
                    return True, min(100, int(wick_ratio * 40 + (body_size / total_range) * 30 + 30))
        else:
            wick_above   = bar_h - body_top
            touches      = bar_h >= fib_level - level_tolerance
            closed_below = bar_c < fib_level + level_tolerance
            if touches and closed_below:
                wick_ratio = wick_above / body_size if body_size > 1e-10 else 0
                if wick_ratio >= 0.8:
                    return True, min(100, int(wick_ratio * 40 + (body_size / total_range) * 30 + 30))
    return False, 0


def fib_tf_min_move_pct(tf: str) -> float:
    tf = (tf or "").lower()
    return {"15m": 1.5, "30m": 2.0, "1h": 3.0, "2h": 4.0,
            "4h": 5.0, "6h": 6.0, "12h": 7.0, "1d": 8.0}.get(tf, 3.0)


def fib_tf_min_bars(tf: str) -> int:
    tf = (tf or "").lower()
    return {"15m": 5, "30m": 4, "1h": 4, "2h": 4,
            "4h": 3, "6h": 3, "12h": 2, "1d": 2}.get(tf, 4)


def fib_tf_expiry_atr_mult(tf: str) -> float:
    tf = (tf or "").lower()
    return {"15m": 0.5, "30m": 0.5, "1h": 0.6, "2h": 0.6,
            "4h": 0.8, "6h": 0.8, "12h": 1.0, "1d": 1.0}.get(tf, 0.6)


def _fib_bars_for_move(start_bar: int, end_bar: int) -> int:
    return max(0, end_bar - start_bar)


def _fib_extension_threshold(atr_value: float) -> float:
    return max(atr_value * 0.5, 1e-10)


def _fib_progression_close_reached(level_price: float, close_val: float, bullish: bool) -> bool:
    return close_val <= level_price if bullish else close_val >= level_price


def _fib_touch_reached(level_price: float, high_val: float, low_val: float, tol_pct: float, bullish: bool) -> bool:
    tol = level_price * (tol_pct / 100.0)
    if bullish:
        return low_val <= level_price + tol
    return high_val >= level_price - tol


def measure_impulse_strength(o: List[float], h: List[float], l: List[float], c: List[float],
                              v: List[float], start_bar: int, end_bar: int) -> float:
    """Measure impulse strength of a leg (0-100)."""
    n_bars = max(end_bar - start_bar, 1)
    if end_bar >= len(c) or start_bar >= len(c):
        return 50.0
    leg_move    = abs(c[min(end_bar, len(c) - 1)] - c[start_bar])
    total_range = sum(max(h[j] - l[j], 1e-10) for j in range(start_bar, min(end_bar + 1, len(c))))
    avg_range   = total_range / max(n_bars, 1)
    going_up    = c[min(end_bar, len(c) - 1)] >= c[start_bar]
    directional = sum(
        1 for j in range(start_bar + 1, min(end_bar + 1, len(c)))
        if (going_up and c[j] >= o[j]) or (not going_up and c[j] <= o[j])
    )
    dir_ratio  = directional / max(n_bars, 1)
    body_sum   = sum(abs(c[j] - o[j]) for j in range(start_bar, min(end_bar + 1, len(c))))
    body_ratio = body_sum / max(total_range, 1e-10)
    atr_mult   = leg_move / max(avg_range, 1e-10)
    return round(min(100.0, dir_ratio * 40.0 + body_ratio * 30.0 + min(atr_mult * 10.0, 30.0)), 1)


def get_single_active_fib_level(
    fib: Dict[str, Any],
    high: List[float],
    low: List[float],
    close: List[float],
    tf: str,
    tolerance_pct: float = 0.5,
    atr_values: Optional[List[Optional[float]]] = None,
) -> List[str]:
    """Returns at most ONE active fib level. Expires stale/consumed levels."""
    if not fib or "levels" not in fib:
        return []
    level_order = ["0.5", "0.618", "0.705", "0.786"]
    bullish    = fib["bullish"]
    leg_end    = int(fib.get("leg_end", max(0, len(close) - 1)))
    start_idx  = max(0, min(leg_end + 1, len(close) - 1))
    expiry_mult = fib_tf_expiry_atr_mult(tf)

    # 1) deepest level consumed by candle CLOSE
    deepest_consumed = -1
    for idx, name in enumerate(level_order):
        if name not in fib["levels"]:
            continue
        lp = fib["levels"][name]
        for j in range(start_idx, len(close)):
            if _fib_progression_close_reached(lp, close[j], bullish):
                deepest_consumed = max(deepest_consumed, idx)

    # 2) expire levels after a reaction + move-away
    expired: set = set()
    for idx, name in enumerate(level_order):
        if name not in fib["levels"]:
            continue
        if idx <= deepest_consumed:
            expired.add(name); continue
        lp = fib["levels"][name]
        first_touch_idx = None
        for j in range(start_idx, len(close)):
            if _fib_touch_reached(lp, high[j], low[j], tolerance_pct, bullish):
                first_touch_idx = j; break
        if first_touch_idx is None:
            continue
        for j in range(first_touch_idx + 1, len(close)):
            atr_j = (atr_values[j] if atr_values and j < len(atr_values) and atr_values[j] is not None
                     else abs(high[j] - low[j]))
            expiry_dist = max(atr_j * expiry_mult, 1e-10)
            if bullish and high[j] >= lp + expiry_dist:
                expired.add(name); break
            elif not bullish and low[j] <= lp - expiry_dist:
                expired.add(name); break

    # 3) return only the shallowest valid remaining level
    for idx, name in enumerate(level_order):
        if name not in fib["levels"]:
            continue
        if idx <= deepest_consumed or name in expired:
            continue
        return [name]
    return []


def ob_approach_pct_from_atr(price: float, atr_value: float, atr_mult: float = 0.5) -> float:
    """Converts ATR distance into percentage of price for OB approach."""
    if price <= 0:
        return 0.0
    return (max(atr_value, 0.0) * max(atr_mult, 0.0)) / price * 100.0


def analyze_pair(symbol: str, candles: List[Dict[str, float]], tf: str, settings: Dict[str, Any], btc_closes: Optional[List[float]] = None, fib_candles: Optional[List[Dict[str, float]]] = None) -> Optional[Dict[str, Any]]:
    o = [x["open"] for x in candles]
    h = [x["high"] for x in candles]
    l = [x["low"] for x in candles]
    c = [x["close"] for x in candles]
    v = [x["volume"] for x in candles]
    times = [x.get("time", x.get("openTime", 0)) for x in candles]
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

    # ── FVG alerts — collect ALL qualifying FVGs into one grouped alert ──
    qualifying_fvgs = []
    for fvg in fvgs:
        if not filter_fvg(fvg, obs, price, settings):
            continue
        overlap_best = 0.0
        for ob in obs:
            overlap_best = max(overlap_best, compute_overlap_pct(
                fvg["bottom"], fvg["top"], ob["bottom"], ob["top"]))
        qualifying_fvgs.append({
            "fvgAge":       fvg["age"],
            "fvgTouchDepth": fvg["touchDepthLabel"],
            "fvgOverlapPct": round(overlap_best, 1),
            "fvgTop":       round(fvg["top"], 8),
            "fvgBottom":    round(fvg["bottom"], 8),
            "fvgDirection": fvg["direction"],
            "fvgUntouched": fvg["untouched"],
            "fvgTouches":   fvg["touches"],
            "fvgIsValid":   fvg["isValid"],
            "fvgIsBag":     fvg["isBag"],
        })

    if qualifying_fvgs:
        # Use strongest FVG as top alert representative
        best = max(qualifying_fvgs, key=lambda f: (
            5 if f["fvgIsBag"] else 4 if f["fvgIsValid"] else 2
        ))
        count_str = f" ({len(qualifying_fvgs)} FVGs)" if len(qualifying_fvgs) > 1 else ""
        detail = (
            f'{best["fvgDirection"].upper()} '
            f'{"BAG" if best["fvgIsBag"] else ("VALID" if best["fvgIsValid"] else "FVG")}'
            f'{count_str} '
            f'| Age: {best["fvgAge"]} | Touch: {best["fvgTouchDepth"]} '
            f'| Zone: {fmt_price(best["fvgBottom"])} - {fmt_price(best["fvgTop"])}'
        )
        alerts.append({
            "setup":     "FVG",
            "direction": best["fvgDirection"],
            "timeframe": tf,
            "detail":    detail,
            "strength":  5 if best["fvgIsBag"] else 4 if best["fvgIsValid"] else 2,
            "meta": {
                **best,
                "fvgList":  qualifying_fvgs,   # ← ALL qualifying FVGs for the UI
                "fvgCount": len(qualifying_fvgs),
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

    # ── OB alerts — nearest-only, price-gated, optional quality engine ──────────────────────────────
    ob_approach_pct_base = settings.get("obDistancePct", settings.get("approachPct", 2.0))
    ob_consol_tol_pct    = min(ob_approach_pct_base, 0.50)
    needed_consol        = settings.get("consolCandles", 5)

    min_str = settings.get("obMinStrengthPct", 0) if settings.get("useObStrengthFilter") else 0
    bullish_obs_filt = [ob for ob in obs if ob["type"] == "bullish" and ob["strengthPct"] >= min_str]
    bearish_obs_filt = [ob for ob in obs if ob["type"] == "bearish" and ob["strengthPct"] >= min_str]

    # Optional OB Quality Engine v2
    use_high_prob = settings.get("useHighProbOB", False)
    min_quality   = int(settings.get("obMinQuality", 50))

    # ── Orderflow fetch — once per pair, only if OBs exist near price ──
    ob_approach_pct_base_pre = settings.get("obDistancePct", settings.get("approachPct", 2.0))
    _near_obs_check = [
        ob for ob in bullish_obs_filt + bearish_obs_filt
        if obq_dist_from_price(price, ob["top"], ob["bottom"], ob.get("type","bullish"))
           <= ob_approach_pct_base_pre * 3
    ]
    _of_data = fetch_orderflow_data(symbol) if _near_obs_check else {
        "trades": [], "oi": None, "oi_change": None, "funding_rate": None
    }

    for ob in bullish_obs_filt + bearish_obs_filt:
        q_score, q_meta = score_ob_quality(ob, o, h, l, c, v, obs, fvgs, itrend, trend, times=times)

        # Orderflow analysis for this specific OB zone
        _of_result = analyze_orderflow(
            _of_data, price, ob["type"], ob["top"], ob["bottom"]
        )
        ob["absorption"]      = _of_result["absorption"]
        ob["absorptionStr"]   = _of_result["absorption_str"]
        ob["absorptionPass"]  = _of_result["checklist_pass"]
        ob["delta"]           = _of_result["delta"]
        ob["buyVol"]          = _of_result["buy_volume"]
        ob["sellVol"]         = _of_result["sell_volume"]
        ob["oiSignal"]        = _of_result["oi_signal"]
        ob["fundingContext"]  = _of_result["funding_context"]
        ob["ofScoreDelta"]    = _of_result["score_delta"]
        ob["ofSummary"]       = _of_result["summary"]

        # Inject absorptionPass into quality meta so checklist shows it
        q_meta["absorptionPass"] = _of_result["checklist_pass"]

        # Apply orderflow score adjustment (capped 0–100)
        q_score = int(clamp(q_score + _of_result["score_delta"], 0, 100))

        ob["quality"]      = q_score
        ob["qualityLabel"] = ("Elite" if q_score >= 85 else "High" if q_score >= 70
                              else "Medium" if q_score >= 50 else "Weak")
        ob["qualityMeta"]  = q_meta

    if use_high_prob:
        bullish_obs_filt = [ob for ob in bullish_obs_filt if ob.get("quality", 0) >= min_quality]
        bearish_obs_filt = [ob for ob in bearish_obs_filt if ob.get("quality", 0) >= min_quality]

    def _ob_dist_from_price(ob, px):
        return obq_dist_from_price(px, ob["top"], ob["bottom"], ob.get("type", "bullish"))

    def _ob_recent_touch_and_reaction(ob, lookback_bars=8):
        """Detect if price touched the OB recently and then reacted away."""
        nc = len(c)
        if nc < 2:
            return {"touched": False, "touch_bar": None, "reacted": False, "reaction_side_ok": False}
        zt = ob["top"]; zb = ob["bottom"]; od = ob["type"]
        start     = max(0, nc - lookback_bars)
        touch_bar = None
        for j in range(nc - 1, start - 1, -1):
            if l[j] <= zt and h[j] >= zb:
                touch_bar = j; break
        if touch_bar is None:
            return {"touched": False, "touch_bar": None, "reacted": False, "reaction_side_ok": False}
        cur = c[-1]
        reaction_side_ok = (cur > zt) if od == "bullish" else (cur < zb)
        reacted = touch_bar < (nc - 1) and reaction_side_ok
        return {"touched": True, "touch_bar": touch_bar, "reacted": reacted,
                "reaction_side_ok": reaction_side_ok}

    # Rank nearest first, then strongest, then best quality, then freshest
    bullish_obs_filt.sort(key=lambda ob: (_ob_dist_from_price(ob, price), -ob["strengthPct"], -ob.get("quality", 0), ob.get("bar", 0)))
    bearish_obs_filt.sort(key=lambda ob: (_ob_dist_from_price(ob, price), -ob["strengthPct"], -ob.get("quality", 0), ob.get("bar", 0)))

    for direction, ob_list in [("bullish", bullish_obs_filt), ("bearish", bearish_obs_filt)]:
        if not ob_list:
            continue

        found_alert = False
        for ob in ob_list:  # ← check ALL OBs, not just nearest
            zone_top      = ob["top"]
            zone_bottom   = ob["bottom"]
            price_in_zone = zone_bottom <= price <= zone_top

            # ATR-based approach distance (optional)
            ob_approach_pct = ob_approach_pct_base
            if settings.get("useAtrObApproach"):
                ob_approach_pct = ob_approach_pct_from_atr(
                    price, current_atr, settings.get("obApproachAtrMult", 0.5)
                )

            dist_pct    = _ob_dist_from_price(ob, price)
            quality_str = (f' | Quality: {ob.get("quality", 0)}/100 ({ob.get("qualityLabel", "Weak")})'
                           if use_high_prob else '')

            # Position label
            pos_label = "INSIDE ZONE" if price_in_zone else f"{dist_pct:.2f}% from zone"

            ob_strength = (5 if use_high_prob and ob.get("quality", 0) >= 80
                           else 4 if ob["strengthPct"] >= 70 else 3)
            ob_meta_base = {
                "obStrengthPct": ob["strengthPct"],
                "obStrengthLabel": ob["strengthLabel"],
                "obDistPct": round(dist_pct, 3),
                "obQuality": ob.get("quality", 0),
                "obQualityLabel": ob.get("qualityLabel", "Weak"),
                "obTop":     round(ob["top"], 8),
                "obBottom":  round(ob["bottom"], 8),
                "absorption": ob.get("absorption", "NONE"),
                "absorptionStr": ob.get("absorptionStr", "None"),
                "absorptionPass": ob.get("absorptionPass", False),
                "delta": ob.get("delta", 0.0),
                "oiSignal": ob.get("oiSignal", "unknown"),
                "fundingContext": ob.get("fundingContext", "unknown"),
                "ofSummary": ob.get("ofSummary", ""),
                **ob.get("qualityMeta", {}),
            }

            # OB_CONSOL: price inside or very near zone
            price_near_zone = price_in_zone or dist_pct <= ob_consol_tol_pct
            if price_near_zone:
                consecutive = _ob_consol_consecutive(ob, needed_consol, ob_consol_tol_pct)
                if consecutive >= needed_consol:
                    alerts.append({
                        "setup": "OB_CONSOL",
                        "direction": direction,
                        "timeframe": tf,
                        "detail": (f'Consolidating on {direction} OB | {pos_label} | '
                                   f'Candles: {consecutive} | '
                                   f'Strength: {ob["strengthPct"]:.1f}% ({ob["strengthLabel"]}){quality_str} | '
                                   f'Zone: {fmt_price(zone_bottom)} – {fmt_price(zone_top)}'
                                   + (f' | {ob["ofSummary"]}' if ob.get("ofSummary") else '')),
                        "strength": ob_strength,
                        "meta": {**ob_meta_base, "consolCandles": consecutive, "obState": "inside"},
                    })
                    break
                # Not enough consol candles — fall through to check OB_APPROACH below

            # OB_APPROACH: price approaching from correct side within threshold
            # Also fires when price is near/inside zone but not consolidating enough
            correct_side = ((direction == "bullish" and price >= zone_bottom) or
                            (direction == "bearish" and price <= zone_top))
            if 0 < dist_pct <= ob_approach_pct and correct_side:
                alerts.append({
                    "setup": "OB_APPROACH",
                    "direction": direction,
                    "timeframe": tf,
                    "detail": (f'Approaching {direction} OB | Dist: {dist_pct:.2f}% | '
                               f'Strength: {ob["strengthPct"]:.1f}% ({ob["strengthLabel"]}){quality_str} | '
                               f'Zone: {fmt_price(zone_bottom)} – {fmt_price(zone_top)}'
                               + (f' | {ob["ofSummary"]}' if ob.get("ofSummary") else '')),
                    "strength": ob_strength,
                    "meta": {**ob_meta_base, "obState": "approaching" if not price_in_zone else "inside"},
                })
                break  # found valid OB for this direction

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

    # ── Fib Module v2 — Dominant Leg + Single Active Level ──
    if settings.get("useFibModule"):
        sel_levels   = settings.get("fibLevels", ["0.5", "0.618", "0.705", "0.786"])
        tolerance    = float(settings.get("fibTolerancePct", 0.5))
        approach_pct = float(settings.get("fibApproachPct", 2.0))
        atr_mult     = float(settings.get("fibAtrMultiplier", 1.5))
        fib_tf       = settings.get("fibTf", tf)

        if fib_candles:
            fh = [x["high"]            for x in fib_candles]
            fl = [x["low"]             for x in fib_candles]
            fc = [x["close"]           for x in fib_candles]
            fo = [x["open"]            for x in fib_candles]
            fv = [x.get("volume", 1.0) for x in fib_candles]
        else:
            fh, fl, fc, fo, fv = h, l, c, o, v

        fib_atr_vals = calc_atr(fh, fl, fc, 14)
        active_fib   = find_active_fib_leg_v2(fo, fh, fl, fc, fv, tf=fib_tf, atr_multiplier=atr_mult)

        if active_fib:
            is_bull_leg = active_fib["bullish"]
            leg_dir     = "bullish" if is_bull_leg else "bearish"

            valid_levels = get_single_active_fib_level(
                active_fib, fh, fl, fc, fib_tf,
                tolerance_pct=tolerance,
                atr_values=fib_atr_vals,
            )
            check_levels = [lv for lv in sel_levels if lv in valid_levels]

            for level_name in check_levels:
                if level_name not in active_fib["levels"]:
                    continue

                level_price = active_fib["levels"][level_name]
                dist_pct    = abs(price - level_price) / max(price, 1e-10) * 100.0
                trade_dir   = "bullish" if is_bull_leg else "bearish"

                fvg_at_level = any(
                    fvg["direction"] == trade_dir and
                    fvg["bottom"] <= level_price <= fvg["top"]
                    for fvg in fvgs
                )

                if settings.get("useFibRequireFvg") and not fvg_at_level:
                    continue
                if settings.get("useFibRequireOb"):
                    ob_at_level = any(
                        ob["type"] == trade_dir and ob["bottom"] <= level_price <= ob["top"]
                        for ob in obs
                    )
                    if not ob_at_level:
                        continue

                if tolerance < dist_pct <= approach_pct:
                    alerts.append({
                        "setup": "FIB_APPROACH",
                        "direction": trade_dir,
                        "timeframe": tf,
                        "detail": (f'Approaching Fib {level_name} | '
                                   f'Dist: {dist_pct:.2f}% | '
                                   f'Level: {fmt_price(level_price)} | '
                                   f'Leg: {fmt_price(active_fib["a"])} → {fmt_price(active_fib["b"])}'),
                        "strength": 3 if fvg_at_level else 2,
                        "meta": {
                            "fibLevel": level_name,
                            "fibPrice": round(level_price, 8),
                            "fibDist": round(dist_pct, 3),
                            "legDirection": leg_dir,
                            "legScore": active_fib.get("leg_score"),
                            "movePct": active_fib.get("move_pct"),
                            "atrMultiple": active_fib.get("atr_multiple"),
                            "barsCount": active_fib.get("bars_count"),
                            "legA": round(active_fib.get("a", 0), 8),
                            "legB": round(active_fib.get("b", 0), 8),
                        },
                    })

                if dist_pct <= tolerance:
                    has_rej, rej_str = check_wick_rejection_v2(
                        fo, fh, fl, fc, level_price, is_bull_leg, tolerance
                    )

                    if has_rej and fvg_at_level:
                        alerts.append({
                            "setup": "FIB_REACTION",
                            "direction": trade_dir,
                            "timeframe": tf,
                            "detail": (f'Fib {level_name} REACTION | Wick + FVG | '
                                       f'Level: {fmt_price(level_price)} | '
                                       f'Rejection: {rej_str}% | '
                                       f'Leg: {fmt_price(active_fib["a"])} → {fmt_price(active_fib["b"])}'),
                            "strength": 5,
                            "meta": {
                                "fibLevel": level_name,
                                "fibPrice": round(level_price, 8),
                                "fibDist": round(dist_pct, 3),
                                "rejectionStrength": rej_str,
                                "fvgConfluence": True,
                                "legDirection": leg_dir,
                                "legScore": active_fib.get("leg_score"),
                                "movePct": active_fib.get("move_pct"),
                                "atrMultiple": active_fib.get("atr_multiple"),
                                "barsCount": active_fib.get("bars_count"),
                                "legA": round(active_fib.get("a", 0), 8),
                                "legB": round(active_fib.get("b", 0), 8),
                            },
                        })
                    elif has_rej:
                        alerts.append({
                            "setup": "FIB_REACTION",
                            "direction": trade_dir,
                            "timeframe": tf,
                            "detail": (f'Fib {level_name} wick rejection (no FVG) | '
                                       f'Level: {fmt_price(level_price)} | '
                                       f'Rejection: {rej_str}% | '
                                       f'Leg: {fmt_price(active_fib["a"])} → {fmt_price(active_fib["b"])}'),
                            "strength": 3,
                            "meta": {
                                "fibLevel": level_name,
                                "fibPrice": round(level_price, 8),
                                "fibDist": round(dist_pct, 3),
                                "rejectionStrength": rej_str,
                                "fvgConfluence": False,
                                "legDirection": leg_dir,
                                "legScore": active_fib.get("leg_score"),
                                "movePct": active_fib.get("move_pct"),
                                "atrMultiple": active_fib.get("atr_multiple"),
                                "barsCount": active_fib.get("bars_count"),
                                "legA": round(active_fib.get("a", 0), 8),
                                "legB": round(active_fib.get("b", 0), 8),
                            },
                        })
                    elif fvg_at_level:
                        alerts.append({
                            "setup": "FIB_APPROACH",
                            "direction": trade_dir,
                            "timeframe": tf,
                            "detail": (f'At Fib {level_name} + FVG (awaiting rejection) | '
                                       f'Level: {fmt_price(level_price)} | '
                                       f'Leg: {fmt_price(active_fib["a"])} → {fmt_price(active_fib["b"])}'),
                            "strength": 3,
                            "meta": {
                                "fibLevel": level_name,
                                "fibPrice": round(level_price, 8),
                                "fibDist": round(dist_pct, 3),
                                "fvgConfluence": True,
                                "legDirection": leg_dir,
                                "legScore": active_fib.get("leg_score"),
                                "movePct": active_fib.get("move_pct"),
                                "atrMultiple": active_fib.get("atr_multiple"),
                                "barsCount": active_fib.get("bars_count"),
                                "legA": round(active_fib.get("a", 0), 8),
                                "legB": round(active_fib.get("b", 0), 8),
                            },
                        })

    if not alerts:
        return None

    # Conflict resolution:
    # OB_CONSOL cancels OB_APPROACH in the same direction
    strong_ob_directions = {a["direction"] for a in alerts if a["setup"] == "OB_CONSOL"}
    if strong_ob_directions:
        alerts = [
            a for a in alerts
            if not (a["setup"] == "OB_APPROACH" and a["direction"] in strong_ob_directions)
        ]

    if not alerts:
        return None

    alerts.sort(key=lambda x: x["strength"], reverse=True)

    # ── Overall Setup Score (0-100) ──
    score = 0
    alert_setups = [a["setup"] for a in alerts]

    if "FIB_REACTION" in alert_setups:
        score += 25
    elif "FIB_APPROACH" in alert_setups:
        score += 10

    if "OB_CONSOL" in alert_setups:
        score += 20
    elif "OB_APPROACH" in alert_setups:
        score += 15

    if any("FVG" in s for s in alert_setups):
        score += 15

    signal_types = set()
    for a in alerts:
        if "FIB" in a["setup"]:
            signal_types.add("FIB")
        elif "OB" in a["setup"]:
            signal_types.add("OB")
        elif "FVG" in a["setup"]:
            signal_types.add("FVG")
        elif "RSI" in a["setup"]:
            signal_types.add("RSI")

    if len(signal_types) >= 3:
        score += 20
    elif len(signal_types) >= 2:
        score += 10

    max_strength = max((a.get("strength", 1) for a in alerts), default=1)
    score += max_strength * 4

    if any("untouched" in a.get("detail", "").lower() for a in alerts):
        score += 5

    score = min(100, score)

    if score >= 75:
        confidence = "High"
    elif score >= 45:
        confidence = "Medium"
    else:
        confidence = "Low"

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
        "score": score,
        "confidence": confidence,
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
        "useHighProbOB": bool(payload.get("useHighProbOB", False)),
        "obMinQuality": int(payload.get("obMinQuality", 50)),
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
        "fibLevels": payload.get("fibLevels", ["0.5", "0.618", "0.705", "0.786"]),
        "fibTolerancePct": float(payload.get("fibTolerancePct", 0.5)),
        "useFibRequireFvg": bool(payload.get("useFibRequireFvg", False)),
        "useFibRequireOb": bool(payload.get("useFibRequireOb", False)),
        "fibSetupType": payload.get("fibSetupType", "both"),
        "fibDisplayMode": payload.get("fibDisplayMode", "best_only"),
        "fibApproachPct": float(payload.get("fibApproachPct", 2.0)),
        "fibAtrMultiplier": float(payload.get("fibAtrMultiplier", 1.5)),
        "useBtcCorrelation": bool(payload.get("useBtcCorrelation", False)),
        "btcCorrelationMode": payload.get("btcCorrelationMode", "all"),
        "btcLookback": int(payload.get("btcLookback", 60)),
        "useAtrObApproach": bool(payload.get("useAtrObApproach", False)),
        "obApproachAtrMult": float(payload.get("obApproachAtrMult", 0.5)),
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
        username = request.form.get("username", "").strip().lower()
        pwd      = request.form.get("password", "")

        if not username:
            error = "Please enter your username."
        elif username not in ALLOWED_USERS:
            error = "Username not recognised."
        elif pwd != APP_PASSWORD:
            error = "Incorrect password. Try again."
        else:
            session["logged_in"] = True
            session["username"]  = username

            # ── Restore server-side watchlist for this user ──
            saved_pairs = load_user_watchlist(username)
            with _wl_lock:
                _wl_pairs[:] = saved_pairs
            if saved_pairs:
                _ensure_wl_thread()

            # ── Login notification email ──
            try:
                ip      = request.headers.get("X-Forwarded-For", request.remote_addr or "unknown")
                ip      = ip.split(",")[0].strip()
                ua      = request.headers.get("User-Agent", "unknown")
                now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                geo     = "unknown"
                try:
                    gr = req.get(f"https://ipapi.co/{ip}/json/", timeout=4)
                    if gr.status_code == 200:
                        gd      = gr.json()
                        city    = gd.get("city", "")
                        country = gd.get("country_name", "")
                        geo     = f"{city}, {country}" if city else country
                except Exception:
                    pass

                subject = f"🔐 ZyNi Screener — {username.title()} logged in"
                body = f"""
<html><body style="font-family:monospace;background:#0a0e17;color:#e2e8f0;padding:24px">
<div style="max-width:520px;margin:0 auto;background:#0d1525;border:1px solid #1e3040;border-top:3px solid #22d3ee;border-radius:10px;padding:24px">
  <h2 style="color:#22d3ee;margin:0 0 16px">🔐 New Login — ZyNi SMC Screener</h2>
  <table style="width:100%;border-collapse:collapse">
    <tr><td style="color:#64748b;padding:6px 0;width:120px">User</td><td style="color:#22d3ee;font-weight:700">{username.title()}</td></tr>
    <tr><td style="color:#64748b;padding:6px 0">Time</td><td style="color:#e2e8f0">{now_utc}</td></tr>
    <tr><td style="color:#64748b;padding:6px 0">IP Address</td><td style="color:#e2e8f0">{ip}</td></tr>
    <tr><td style="color:#64748b;padding:6px 0">Location</td><td style="color:#e2e8f0">{geo}</td></tr>
    <tr><td style="color:#64748b;padding:6px 0;vertical-align:top">Device</td><td style="color:#e2e8f0;word-break:break-all;font-size:11px">{ua}</td></tr>
  </table>
  <div style="margin-top:16px;padding:12px;background:#071525;border-radius:6px;font-size:12px;color:#64748b">
    If this was you — no action needed.<br>
    If this was NOT you — remove this username from USERS immediately.
  </div>
</div>
</body></html>"""
                threading.Thread(target=send_email_alert, args=(subject, body), daemon=True).start()
            except Exception as e:
                print(f"[LOGIN-NOTIFY] Error: {e}")

            return redirect(url_for("index"))

    return render_template("login.html", error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# ── Watchlist streaming endpoints ──

@app.route("/api/watchlist/register", methods=["POST"])
@login_required
def api_watchlist_register():
    global _wl_pairs
    username = session.get("username", "default")
    data  = request.get_json(force=True) or {}
    pairs = [str(p).strip().upper() for p in data.get("pairs", []) if str(p).strip()]
    pairs = [p for p in pairs if p.endswith("USDT")][:10]  # max 10 for Nano

    # Save to server permanently
    save_user_watchlist(username, pairs)

    with _wl_lock:
        old_pairs = set(_wl_pairs)
        _wl_pairs[:] = pairs
        for sym in list(_wl_cache.keys()):
            if sym not in pairs:
                del _wl_cache[sym]

    new_pairs = set(pairs)

    # Stop WebSocket for removed pairs
    for sym in old_pairs - new_pairs:
        stop_ob_ws(sym)

    # Start WebSocket for new pairs
    for sym in new_pairs - old_pairs:
        start_ob_ws(sym)

    # Ensure streams running for all pairs
    for sym in pairs:
        start_ob_ws(sym)

    if pairs:
        _ensure_wl_thread()

    return jsonify({"registered": pairs, "count": len(pairs), "user": username})


@app.route("/api/watchlist/get")
@login_required
def api_watchlist_get():
    """Returns the saved watchlist for current user — called on page load."""
    username = session.get("username", "default")
    pairs    = load_user_watchlist(username)
    return jsonify({"pairs": pairs, "user": username})


@app.route("/api/watchlist/cache")
@login_required
def api_watchlist_cache():
    """
    Browser polls this every 5s to get latest cached orderflow.
    Returns all pairs at once — one lightweight request.
    """
    with _wl_lock:
        return jsonify(dict(_wl_cache))


@app.route("/api/watchlist/status")
@login_required
def api_watchlist_status():
    """Health check for streaming thread."""
    with _wl_lock:
        return jsonify({
            "running":  _wl_thread is not None and _wl_thread.is_alive(),
            "pairs":    list(_wl_pairs),
            "cached":   list(_wl_cache.keys()),
        })


# ============================================================
# /api/watchlist/refresh — Multi-timeframe SMC scan
# Called when user taps ↻ REFRESH ALL in watchlist tab
# Scans each pair across 15m + 1H + 4H simultaneously
# Returns full SMC data per timeframe per pair
# ============================================================

# Default settings for watchlist multi-TF scan
_WL_SCAN_SETTINGS: Dict[str, Any] = {
    "iLen": 5, "sLen": 30,
    "approachPct": 2.0, "obDistancePct": 2.0,
    "consolCandles": 5, "rsiOB": 70, "rsiOS": 30,
    "useObStrengthFilter": False, "obMinStrengthPct": 0,
    "useHighProbOB": False, "obMinQuality": 50,
    "useAtrObApproach": False, "obApproachAtrMult": 0.5,
    "useFvgValidOnly": False, "useFvgState": False,
    "fvgState": "all", "fvgAgeMin": 0, "fvgAgeMax": 50,
    "useFvgAgeRange": False, "useFvgDistance": False,
    "fvgMaxDistancePct": 5.0, "useFvgTouchDepth": False,
    "fvgTouchDepth": "any", "useFvgObOverlap": False,
    "fvgObOverlapMode": "same_direction", "fvgObMinOverlapPct": 20,
    "useFibModule": False, "fibTf": "1h",
    "fibLevels": ["0.618"], "fibTolerancePct": 0.5,
    "useFibRequireFvg": False, "useFibRequireOb": False,
    "fibApproachPct": 2.0, "fibAtrMultiplier": 1.5,
    "fibSetupType": "both", "fibDisplayMode": "best_only",
    "fibLegMethod": "lookback_range", "fibSwingDirection": "auto",
    "useBtcCorrelation": False, "btcCorrelationMode": "all",
    "btcLookback": 60, "useAtrObApproach": False,
    "obApproachAtrMult": 0.5,
}

WL_TIMEFRAMES = ["15m", "1h", "4h"]


def _scan_pair_multitf(symbol: str, market: str = "perpetual", wl_config: Optional[Dict] = None) -> Dict[str, Any]:
    """
    Scan one pair across selected timeframes with per-TF settings.
    wl_config = {
        scan_ob: bool, scan_fvg: bool, scan_fib: bool,
        timeframes: [str],
        ob_approach: {tf: float},
        fvg_age_min: {tf: int}, fvg_age_max: {tf: int},
        fib_tf: str, fib_tolerance: float, fib_approach: float,
        fib_atr_mult: float, fib_levels: [str],
        bias_1d: bool
    }
    """
    cfg = wl_config or {}
    scan_ob    = cfg.get("scan_ob", True)
    scan_fvg   = cfg.get("scan_fvg", True)
    scan_fib   = cfg.get("scan_fib", True)
    tfs        = cfg.get("timeframes", ["15m", "1h", "4h"])
    ob_appr    = cfg.get("ob_approach", {"15m": 0.8, "1h": 1.5, "4h": 2.5})
    fvg_min    = cfg.get("fvg_age_min", {"15m": 0, "1h": 0, "4h": 0})
    fvg_max    = cfg.get("fvg_age_max", {"15m": 5, "1h": 10, "4h": 15})
    fib_tf     = cfg.get("fib_tf", "1h")
    fib_tol    = cfg.get("fib_tolerance", 0.5)
    fib_appr   = cfg.get("fib_approach", 2.0)
    fib_atr    = cfg.get("fib_atr_mult", 1.5)
    fib_levels = cfg.get("fib_levels", ["0.5", "0.618", "0.705", "0.786"])
    do_bias_1d = cfg.get("bias_1d", True)

    result: Dict[str, Any] = {
        "symbol":    symbol,
        "price":     0.0,
        "bias_1d":   None,   # 1D trend direction only
        "tfs":       {},
        "obs":       [],
        "fvgs":      [],
        "fibs":      [],
        "structure": {},
        "rsi":       {},
        "atr":       {},
        "error":     None,
    }

    # ── 1D bias (direction only, no zones) ──
    if do_bias_1d:
        try:
            candles_1d = get_klines(symbol, "1d", 100, market)
            if candles_1d and len(candles_1d) >= 80:
                o_ = [x["open"] for x in candles_1d]
                h_ = [x["high"] for x in candles_1d]
                l_ = [x["low"]  for x in candles_1d]
                c_ = [x["close"] for x in candles_1d]
                itrend_1d, trend_1d = detect_structure(h_, l_, c_, 7, 20)
                result["bias_1d"] = trend_1d if trend_1d != 0 else itrend_1d
        except Exception:
            pass

    for tf in tfs:
        try:
            candles = get_klines(symbol, tf, 300, market)
            if not candles or len(candles) < 100:
                result["tfs"][tf] = {"error": "insufficient data"}
                continue

            o = [x["open"]   for x in candles]
            h = [x["high"]   for x in candles]
            l = [x["low"]    for x in candles]
            c = [x["close"]  for x in candles]
            v = [x["volume"] for x in candles]
            times = [x.get("time", x.get("openTime", 0)) for x in candles]
            price = c[-1]

            settings = dict(_WL_SCAN_SETTINGS)
            settings["tf"]            = tf
            settings["obDistancePct"] = ob_appr.get(tf, 2.0)
            settings["approachPct"]   = ob_appr.get(tf, 2.0)

            if scan_fvg:
                settings["useFvgAgeRange"] = True
                settings["fvgAgeMin"]      = fvg_min.get(tf, 0)
                settings["fvgAgeMax"]      = fvg_max.get(tf, 50)
            else:
                settings["useFvgAgeRange"] = False

            if scan_fib:
                settings["useFibModule"]    = True
                settings["fibTf"]           = fib_tf
                settings["fibTolerancePct"] = fib_tol
                settings["fibApproachPct"]  = fib_appr
                settings["fibAtrMultiplier"] = fib_atr
                settings["fibLevels"]       = fib_levels
            else:
                settings["useFibModule"] = False

            tf_obs  = []
            tf_fvgs = []
            tf_fibs = []

            # ── #1 FIX: Detect OBs directly — same logic as scan page ──
            # detect_obs() finds ALL OBs regardless of price proximity
            # No dependency on analyze_pair() alerts
            if scan_ob:
                iLen = settings["iLen"]
                sLen = settings["sLen"]
                raw_obs = detect_obs(o, h, l, c, v, iLen, sLen, max_ob=5)
                ob_approach_pct = ob_appr.get(tf, 2.0)
                fvgs_for_quality = detect_fvgs(o, h, l, c, v, tf)
                itrend_q, trend_q = detect_structure(h, l, c, iLen, sLen)

                for ob in raw_obs:
                    zt  = ob["top"]
                    zb  = ob["bottom"]
                    dist_pct = obq_dist_from_price(price, zt, zb, ob["type"])
                    price_in_zone = zb <= price <= zt

                    if price_in_zone:
                        state = "inside"
                    elif dist_pct <= ob_approach_pct:
                        state = "approaching"
                    else:
                        state = "far"

                    q_score, q_meta = score_ob_quality(
                        ob, o, h, l, c, v,
                        raw_obs, fvgs_for_quality,
                        itrend_q, trend_q,
                        times=times
                    )

                    tf_obs.append({
                        "tf":           tf,
                        "direction":    ob["type"],
                        "setup":        "OB_APPROACH" if state != "far" else "OB_FAR",
                        "top":          round(zt, 8),
                        "bottom":       round(zb, 8),
                        "strength":     round(ob.get("strengthPct", 0), 1),
                        "quality":      q_score,
                        "qualityLabel": ("Elite" if q_score >= 85 else
                                         "High"  if q_score >= 70 else
                                         "Medium" if q_score >= 50 else "Weak"),
                        "dist":         round(dist_pct, 3),
                        "state":        state,
                        "absorption":   "NONE",
                        "absorptionStr": "",
                        "checklist": {
                            "sweep": q_meta.get("sweepPass", False),
                            "disp":  q_meta.get("dispPass",  False),
                            "fvg":   q_meta.get("fvgPass",   False),
                            "pd":    q_meta.get("pdPass",    False),
                            "htf":   q_meta.get("htfPass",   False),
                            "safe":  q_meta.get("safePass",  False),
                            "abs":   False,
                        },
                        "zone_str": f"{fmt_price(zb)} – {fmt_price(zt)}",
                        "detail":   (
                            f'{"Approaching" if state == "approaching" else "Inside" if state == "inside" else "Far from"} '
                            f'{ob["type"]} OB | Dist: {dist_pct:.2f}% | '
                            f'Strength: {ob.get("strengthPct", 0):.1f}% | '
                            f'Zone: {fmt_price(zb)} – {fmt_price(zt)}'
                        ),
                    })

            # ── Run analyze_pair for FVG + Fib + RSI + trend/score ──
            tf_result = analyze_pair(symbol, candles, tf, settings)

            if tf_result:
                if tf == "1h" or result["price"] == 0.0:
                    result["price"] = tf_result.get("price", 0.0)

                for alert in tf_result.get("alerts", []):
                    setup     = alert.get("setup", "")
                    meta      = alert.get("meta", {})
                    direction = alert.get("direction", "")

                    if scan_fvg and setup == "FVG":
                        fvg_list = meta.get("fvgList", [meta])
                        for f in fvg_list:
                            tf_fvgs.append({
                                "tf":        tf,
                                "direction": f.get("fvgDirection", direction),
                                "top":       f.get("fvgTop", 0),
                                "bottom":    f.get("fvgBottom", 0),
                                "age":       f.get("fvgAge", 0),
                                "status":    "UNTOUCHED" if f.get("fvgUntouched") else "TOUCHED",
                                "isValid":   f.get("fvgIsValid", False),
                                "isBag":     f.get("fvgIsBag", False),
                                "touchDepth": f.get("fvgTouchDepth", ""),
                                "touches":   f.get("fvgTouches", 0),
                            })

                    elif scan_fib and setup in ("FIB_APPROACH", "FIB_REACTION"):
                        tf_fibs.append({
                            "tf":        tf,
                            "direction": direction,
                            "setup":     setup,
                            "level":     meta.get("fibLevel", ""),
                            "price":     meta.get("fibPrice", 0),
                            "dist":      meta.get("fibDist", 0),
                            "legDir":    meta.get("legDirection", ""),
                            "legScore":  meta.get("legScore", 0),
                            "movePct":   meta.get("movePct", 0),
                            "legA":      meta.get("legA", 0),
                            "legB":      meta.get("legB", 0),
                            "detail":    alert.get("detail", ""),
                        })

            else:
                # analyze_pair returned None — still store OBs
                if result["price"] == 0.0:
                    result["price"] = price

            result["tfs"][tf] = {
                "score":      tf_result.get("score", 0) if tf_result else 0,
                "confidence": tf_result.get("confidence", "") if tf_result else "",
                "trend":      tf_result.get("trend", 0) if tf_result else 0,
                "itrend":     tf_result.get("itrend", 0) if tf_result else 0,
                "rsi":        tf_result.get("rsi", 0) if tf_result else 0,
                "atr":        tf_result.get("atr", 0) if tf_result else 0,
                "obs":        tf_obs,
                "fvgs":       tf_fvgs,
                "fibs":       tf_fibs,
            }

            result["obs"].extend(tf_obs)
            result["fvgs"].extend(tf_fvgs)
            result["fibs"].extend(tf_fibs)
            result["rsi"][tf] = tf_result.get("rsi", 0) if tf_result else 0
            result["atr"][tf] = tf_result.get("atr", 0) if tf_result else 0

            if tf == "4h":
                result["structure"] = {
                    "trend":  tf_result.get("trend", 0) if tf_result else 0,
                    "itrend": tf_result.get("itrend", 0) if tf_result else 0,
                }

        except Exception as e:
            result["tfs"][tf] = {"error": str(e)}
            print(f"[WL-REFRESH] {symbol} {tf}: {e}")

    return result


@app.route("/api/watchlist/refresh", methods=["POST"])
@login_required
def api_watchlist_refresh():
    data      = request.get_json(force=True) or {}
    pairs     = [str(p).strip().upper() for p in data.get("pairs", []) if str(p).strip()]
    pairs     = [p for p in pairs if p.endswith("USDT")][:30]
    market    = data.get("market", "perpetual")
    wl_config = data.get("config", {})

    if not pairs:
        return jsonify({"results": [], "error": "no pairs"})

    results = []
    workers = min(5, len(pairs))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_scan_pair_multitf, sym, market, wl_config): sym for sym in pairs}
        for fut in as_completed(futures):
            try:
                results.append(fut.result())
            except Exception as e:
                print(f"[WL-REFRESH] Error: {e}")

    results.sort(
        key=lambda x: x.get("tfs", {}).get("1h", {}).get("score", 0),
        reverse=True
    )

    return jsonify({"results": results, "scanned": len(pairs)})


@app.route("/")
@login_required
def index():
    username = session.get("username", "Trader")
    # Capitalize properly
    display_name = " ".join(w.capitalize() for w in username.strip().split())
    return render_template("index.html", username=display_name)


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
                sp = [float(c[i]) for i in range(max(0, len(c)-24), len(c))]
                result["sparkline"] = sp
                markers = []
                if sp:
                    sp_min, sp_max = min(sp), max(sp)
                    for a in result.get("alerts", []):
                        meta = a.get("meta", {})
                        if "fibPrice" in meta:
                            fp = meta["fibPrice"]
                            if sp_min <= fp <= sp_max:
                                markers.append({"price": fp, "type": "fib", "label": str(meta.get("fibLevel", ""))})
                result["sparklineMarkers"] = markers
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
    filter_mode      = payload.get("filterMode", "match")
    required_signals = payload.get("requiredSignals", [])
    checked_signals  = payload.get("checkedSignals", [])

    def has_signal(result, sig):
        setups = {a["setup"] for a in result.get("alerts", [result.get("topAlert", {})])}
        if sig == "FVG":  return "FVG" in setups
        if sig == "OB":   return bool(setups & {"OB_APPROACH", "OB_CONSOL"})
        if sig == "FIB":  return bool(setups & {"FIB_APPROACH", "FIB_REACTION"})
        return False

    if filter_mode == "filter" and required_signals:
        # AND logic — all checked signals must be present
        results = [r for r in results if all(has_signal(r, s) for s in required_signals)]
    elif filter_mode == "match" and checked_signals:
        # OR logic — at least one checked signal must be present
        results = [r for r in results if any(has_signal(r, s) for s in checked_signals)]

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


# ============================================================
# /api/orderflow — Phase 3 live streaming endpoint
# Called every 5s by the Watchlist tab per pair
# Returns lightweight orderflow snapshot: absorption + OI + funding
# ============================================================

@app.route("/api/orderflow")
def api_orderflow():
    symbol = request.args.get("symbol", "").strip().upper()
    if not symbol or not symbol.endswith("USDT"):
        return jsonify({"error": "invalid symbol"}), 400

    # Fetch raw orderflow data
    of_data = fetch_orderflow_data(symbol)

    # We need current price and nearest OB zone to run absorption
    # Use a lightweight kline fetch — last 3 candles only
    price      = 0.0
    zone_top   = 0.0
    zone_bottom = 0.0
    ob_type    = "bullish"

    try:
        r = req.get(
            f"{BINANCE_FUTURES_API}/fapi/v1/klines",
            params={"symbol": symbol, "interval": "15m", "limit": 50},
            timeout=5,
        )
        if r.status_code == 200:
            klines = r.json()
            if klines:
                o = [float(k[1]) for k in klines]
                h = [float(k[2]) for k in klines]
                l = [float(k[3]) for k in klines]
                c = [float(k[4]) for k in klines]
                v = [float(k[5]) for k in klines]
                price = c[-1]

                # Quick OB detection to get nearest zone
                if len(c) >= 20:
                    obs = detect_obs(o, h, l, c, v, 5, 10, max_ob=3)
                    if obs:
                        # Find nearest OB to current price
                        def _dist(ob):
                            return obq_dist_from_price(price, ob["top"], ob["bottom"], ob["type"])
                        nearest = min(obs, key=_dist)
                        zone_top    = nearest["top"]
                        zone_bottom = nearest["bottom"]
                        ob_type     = nearest["type"]
    except Exception:
        pass

    # Run absorption analysis
    of_result = analyze_orderflow(of_data, price, ob_type, zone_top, zone_bottom)

    return jsonify({
        "symbol":          symbol,
        "price":           round(price, 8),
        "absorption":      of_result["absorption"],
        "absorption_str":  of_result["absorption_str"],
        "delta":           of_result["delta"],
        "buy_volume":      of_result["buy_volume"],
        "sell_volume":     of_result["sell_volume"],
        "oi_signal":       of_result["oi_signal"],
        "funding_context": of_result["funding_context"],
        "score_delta":     of_result["score_delta"],
        "checklist_pass":  of_result["checklist_pass"],
        "summary":         of_result["summary"],
        "ob_type":         ob_type,
        "zone_top":        round(zone_top, 8),
        "zone_bottom":     round(zone_bottom, 8),
    })


# ============================================================
# /api/zone_liquidity — OB pending limit orders analysis
# Called when user taps "Zone Liquidity" button on a result card
# ============================================================

@app.route("/api/zone_liquidity")
@login_required
def api_zone_liquidity():
    symbol     = request.args.get("symbol", "").strip().upper()
    zone_top   = safe_float(request.args.get("zone_top", 0))
    zone_bottom = safe_float(request.args.get("zone_bottom", 0))
    ob_type    = request.args.get("ob_type", "bullish")

    if not symbol or not symbol.endswith("USDT"):
        return jsonify({"error": "invalid symbol"}), 400

    auto_mode = request.args.get("auto", "0") == "1"
    price_ref = safe_float(request.args.get("price", 0))

    # Auto mode: detect nearest OB zone from live klines
    if auto_mode or zone_top <= 0:
        try:
            r2 = req.get(
                f"{BINANCE_FUTURES_API}/fapi/v1/klines",
                params={"symbol": symbol, "interval": "15m", "limit": 50},
                timeout=5,
            )
            if r2.status_code == 200:
                klines = r2.json()
                if klines:
                    o_ = [float(k[1]) for k in klines]
                    h_ = [float(k[2]) for k in klines]
                    l_ = [float(k[3]) for k in klines]
                    c_ = [float(k[4]) for k in klines]
                    v_ = [float(k[5]) for k in klines]
                    price_ref = price_ref or c_[-1]
                    obs_ = detect_obs(o_, h_, l_, c_, v_, 5, 10, max_ob=5)
                    if obs_:
                        # Find OB matching ob_type, nearest to price
                        typed = [ob for ob in obs_ if ob["type"] == ob_type]
                        if not typed:
                            typed = obs_
                        nearest = min(typed, key=lambda ob: obq_dist_from_price(
                            price_ref, ob["top"], ob["bottom"], ob["type"]))
                        zone_top    = nearest["top"]
                        zone_bottom = nearest["bottom"]
        except Exception:
            pass

    if zone_top <= 0 or zone_bottom <= 0 or zone_top <= zone_bottom:
        return jsonify({"error": "could not detect OB zone"}), 200

    # ── Ensure full order book stream is running ──
    # Works for both watchlist pairs (already streaming) and
    # scan page pairs (starts on-demand, waits up to 4s, auto-stops after 2min)
    book_ready = ensure_ob_stream(symbol, wait_sec=4.0)

    if book_ready:
        ob_result = get_ob_zone_levels(symbol, zone_top, zone_bottom, ob_type)
        if ob_result.get("ready"):
            return jsonify({
                "symbol":         symbol,
                "ob_type":        ob_type,
                "zone_top":       ob_result.get("zone_top", round(zone_top, 8)),
                "zone_bottom":    ob_result.get("zone_bottom", round(zone_bottom, 8)),
                "side":           "bids" if ob_type == "bullish" else "asks",
                "ladder":         ob_result.get("ladder", []),
                "center":         ob_result.get("center", ""),
                "center_f":       ob_result.get("center_f", 0),
                "step":           ob_result.get("step", 0),
                "verdict":        ob_result.get("verdict", "EMPTY"),
                "verdict_desc":   ob_result.get("verdict_desc", ""),
                "insight":        ob_result.get("insight", ""),
                "total_bid_usdt": ob_result.get("total_bid_usdt", 0),
                "total_ask_usdt": ob_result.get("total_ask_usdt", 0),
                "total_bid_fmt":  ob_result.get("total_bid_fmt", "0"),
                "total_ask_fmt":  ob_result.get("total_ask_fmt", "0"),
                "extreme_count":  ob_result.get("extreme_count", 0),
                "heavy_count":    ob_result.get("heavy_count", 0),
                "book_age_sec":   ob_result.get("book_age_sec", 0),
                "total_bids":     ob_result.get("total_bids", 0),
                "total_asks":     ob_result.get("total_asks", 0),
                "zone_note":      ob_result.get("zone_note", ""),
                "source":         f"live_ws · {ob_result.get('total_bids',0)} bids / {ob_result.get('total_asks',0)} asks · {ob_result.get('book_age_sec',0)}s old",
            })

    # ── Fallback: REST depth endpoint ──
    # Note: limited to ~$10 range from current price for most pairs
    try:
        r = req.get(
            f"{BINANCE_FUTURES_API}/fapi/v1/depth",
            params={"symbol": symbol, "limit": 1000},
            timeout=6,
        )
        if r.status_code != 200:
            return jsonify({"error": "binance error"}), 502

        book = r.json()
        side_key   = "bids" if ob_type == "bullish" else "asks"
        all_levels = book.get(side_key, [])

        parsed = []
        for row in all_levels:
            try:
                parsed.append((float(row[0]), float(row[1])))
            except Exception:
                continue

        if not parsed:
            return jsonify({"error": "no order book data — add pair to watchlist for full depth"}), 200

        all_qtys = [q for _, q in parsed]
        avg_qty  = sum(all_qtys) / max(len(all_qtys), 1)

        zone_size   = abs(zone_top - zone_bottom)
        zone_buffer = zone_size * 0.5
        zone_levels = [(p, q) for p, q in parsed
                       if (zone_bottom - zone_buffer) <= p <= (zone_top + zone_buffer)]
        zone_note = "in_zone"
        if not zone_levels:
            zone_mid    = (zone_top + zone_bottom) / 2
            zone_levels = sorted(parsed, key=lambda x: abs(x[0] - zone_mid))[:15]
            zone_note   = "nearest_to_zone"

        def _classify_wall_r(qty):
            r2 = qty / max(avg_qty, 1e-10)
            if r2 >= 3.0: return "EXTREME", "🔴"
            if r2 >= 2.0: return "HEAVY",   "🟠"
            if r2 >= 1.5: return "MODERATE","🟡"
            return "WEAK", "⚪"

        levels_out = []; total_zone_qty = 0.0
        extreme_count = heavy_count = moderate_count = 0
        for p, q in sorted(zone_levels, reverse=(ob_type == "bearish")):
            cls, icon = _classify_wall_r(q)
            usdt_val  = q * p
            levels_out.append({"price": round(p,8), "qty": round(q,4), "qtyFmt": fmt_vol(q),
                "usdt": round(usdt_val,2), "usdtFmt": fmt_vol(usdt_val), "class": cls, "icon": icon,
                "ratio": round(q/max(avg_qty,1e-10),2)})
            total_zone_qty += usdt_val
            if cls=="EXTREME": extreme_count+=1
            elif cls=="HEAVY": heavy_count+=1
            elif cls=="MODERATE": moderate_count+=1

        strong = extreme_count + heavy_count
        zl     = "in zone" if zone_note=="in_zone" else "nearest to zone"
        if extreme_count>=2 or (extreme_count>=1 and heavy_count>=2):
            verdict="INSTITUTIONAL"; verdict_desc=f"Extreme institutional liquidity ({zl})"
        elif strong>=3:
            verdict="STRONG"; verdict_desc=f"Multiple heavy walls ({zl})"
        elif strong>=1:
            verdict="MODERATE"; verdict_desc=f"Some liquidity present ({zl})"
        elif moderate_count>=2:
            verdict="WEAK"; verdict_desc=f"Mostly normal orders ({zl})"
        else:
            verdict="EMPTY"; verdict_desc=f"Very little liquidity ({zl}) — add pair to watchlist for full depth"

        return jsonify({"symbol": symbol, "ob_type": ob_type,
            "zone_top": round(zone_top,8), "zone_bottom": round(zone_bottom,8),
            "side": "bids" if ob_type=="bullish" else "asks",
            "levels": levels_out, "total_usdt": round(total_zone_qty,2),
            "total_fmt": fmt_vol(total_zone_qty), "extreme_count": extreme_count,
            "heavy_count": heavy_count, "moderate_count": moderate_count,
            "avg_book_qty": round(avg_qty,4), "verdict": verdict,
            "verdict_desc": verdict_desc, "verdict_color": verdict.lower(),
            "zone_note": zone_note,
            "source": "rest_snapshot (limited range — add to watchlist for full depth)"})

    except Exception as e:
        return jsonify({"error": str(e)}), 500
# ============================================================


# ============================================================
# /api/unified_liquidity — Combined OB + Fib zone liquidity
# Used by Live Monitor tab to show both in one panel
# Sorted by distance from current price (closest first)
# ============================================================

@app.route("/api/unified_liquidity", methods=["POST"])
@login_required
def api_unified_liquidity():
    """
    Accepts symbol + list of zones (OB + Fib levels).
    Returns liquidity data for each zone sorted by distance from price.
    """
    data   = request.get_json(force=True) or {}
    symbol = data.get("symbol", "").strip().upper()
    price  = float(data.get("price", 0))
    zones  = data.get("zones", [])
    # zones = [{type: "ob"|"fib", direction, top, bottom, label, tf}, ...]

    if not symbol or not zones:
        return jsonify({"error": "invalid params"}), 400

    # Ensure WebSocket stream running
    ensure_ob_stream(symbol, wait_sec=4.0)

    results = []
    for zone in zones:
        zone_type = zone.get("type", "ob")
        direction = zone.get("direction", "bullish")
        top       = float(zone.get("top", 0))
        bottom    = float(zone.get("bottom", 0))
        label     = zone.get("label", "")
        tf        = zone.get("tf", "")

        if top <= 0 or bottom <= 0:
            continue

        zone_mid = (top + bottom) / 2
        dist_pct = abs(price - zone_mid) / max(price, 1e-10) * 100 if price > 0 else 999

        # Get liquidity for this zone
        liq = get_ob_zone_levels(symbol, top, bottom, direction)

        results.append({
            "type":        zone_type,
            "direction":   direction,
            "tf":          tf,
            "label":       label,
            "top":         round(top, 8),
            "bottom":      round(bottom, 8),
            "dist_pct":    round(dist_pct, 2),
            "liq":         liq if liq.get("ready") else {
                "ready": False, "levels": [],
                "total_usdt": 0, "total_fmt": "0",
                "extreme_count": 0, "heavy_count": 0, "moderate_count": 0,
                "verdict": "LOADING", "verdict_desc": "Book loading...",
                "zone_note": "loading", "bucket_size": 0,
            },
        })

    # Sort by distance from current price (closest first)
    results.sort(key=lambda x: x["dist_pct"])

    return jsonify({
        "symbol":  symbol,
        "price":   price,
        "results": results,
        "source":  "live_ws" if any(r["liq"].get("ready") for r in results) else "loading",
    })


@app.route("/api/fvg_imbalance", methods=["POST"])
@login_required
def api_fvg_imbalance():
    """
    FVG imbalance using kline taker buy/sell volume.
    Much more reliable than aggTrades for low-volume pairs.
    """
    data    = request.get_json(force=True) or {}
    symbol  = data.get("symbol", "").strip().upper()
    fvgs_in = data.get("fvgs", [])
    tf      = data.get("tf", "15m")

    # Sanitize tf — must be valid Binance interval
    VALID_TFS = {"1m","3m","5m","15m","30m","1h","2h","4h","6h","8h","12h","1d","3d","1w"}
    if not tf or str(tf).lower() not in VALID_TFS:
        tf = "15m"
    tf = str(tf).lower()

    if not symbol or not fvgs_in:
        return jsonify({"error": "invalid params"}), 400

    try:
        # Fetch klines — enough to cover FVG formation candles
        # Use 500 candles to cover historical FVGs
        r = req.get(
            f"{BINANCE_FUTURES_API}/fapi/v1/klines",
            params={"symbol": symbol, "interval": tf, "limit": 500},
            timeout=6,
        )
        klines = r.json() if r.status_code == 200 else []

        # Build candle list with buy/sell split
        # kline format: [openTime, open, high, low, close, volume, closeTime,
        #                quoteVolume, trades, takerBuyBase, takerBuyQuote, ignore]
        candles_data = []
        for k in klines:
            try:
                high       = float(k[2])
                low        = float(k[3])
                close      = float(k[4])
                total_vol  = float(k[5])
                taker_buy  = float(k[9])
                taker_sell = total_vol - taker_buy
                candles_data.append({
                    "high":  high,
                    "low":   low,
                    "close": close,
                    "total": total_vol,
                    "buy":   taker_buy,
                    "sell":  taker_sell,
                })
            except Exception:
                continue

        def _analyze_fvg(fvg: Dict) -> Dict:
            top       = float(fvg.get("top", 0))
            bottom    = float(fvg.get("bottom", 0))
            direction = fvg.get("direction", "bullish")
            age       = int(fvg.get("age", 0))

            # ── Find formation candles by price overlap ──
            formation_idx = max(0, len(candles_data) - 1 - age)

            bv = sv = 0.0
            has_data = False
            impulse_vol  = 0.0  # volume of the middle (impulse) candle
            impulse_size = 0.0  # body size of impulse candle

            # Primary: use formation candles (middle candle = impulse)
            for offset in range(-1, 4):
                idx = formation_idx + offset
                if idx < 0 or idx >= len(candles_data):
                    continue
                c = candles_data[idx]
                if c["high"] >= bottom and c["low"] <= top:
                    bv += c["buy"]
                    sv += c["sell"]
                    has_data = True
                    # Track impulse candle (largest in range)
                    vol_here = c["buy"] + c["sell"]
                    if vol_here > impulse_vol:
                        impulse_vol  = vol_here
                        impulse_size = abs(c["close"] - c["open"])

            # Fallback
            if not has_data and formation_idx > 0:
                for idx in range(max(0, formation_idx-1), min(len(candles_data), formation_idx+2)):
                    c = candles_data[idx]
                    bv += c["buy"]
                    sv += c["sell"]
                    has_data = True

            total    = bv + sv
            buy_pct  = round(bv / total * 100, 1) if total > 0 else 50.0
            sell_pct = round(sv / total * 100, 1) if total > 0 else 50.0

            # Which side dominated
            dom_pct = buy_pct if direction == "bullish" else sell_pct

            # ── #6 Impulse score: candle body vs average ──
            recent_candles = candles_data[-20:] if len(candles_data) >= 20 else candles_data
            avg_body = sum(abs(c["close"]-c["open"]) for c in recent_candles) / max(len(recent_candles), 1)
            avg_vol  = sum(c["buy"]+c["sell"] for c in recent_candles) / max(len(recent_candles), 1)
            impulse_ratio = impulse_size / max(avg_body, 1e-10)
            vol_ratio     = impulse_vol  / max(avg_vol,  1e-10)

            # Impulse score 0-100
            impulse_score = min(100, int(
                (min(impulse_ratio, 3) / 3) * 50 +   # body size contribution
                (min(vol_ratio, 3) / 3) * 50           # volume contribution
            ))

            # ── #6 Relative threshold: compare to market average imbalance ──
            # Market baseline: if avg imbalance is 52/48, then 65/35 is very strong
            all_buy_pcts = [c["buy"]/(c["buy"]+c["sell"])*100
                            for c in candles_data[-50:]
                            if (c["buy"]+c["sell"]) > 0]
            market_avg = sum(all_buy_pcts)/max(len(all_buy_pcts),1) if all_buy_pcts else 50.0
            market_std = (sum((x-market_avg)**2 for x in all_buy_pcts)/max(len(all_buy_pcts),1))**0.5 if all_buy_pcts else 5.0

            # How many std deviations above market avg?
            dom_abs = buy_pct if direction == "bullish" else sell_pct
            z_score = (dom_abs - market_avg) / max(market_std, 1.0)

            # ── #6 Enhanced strength classification ──
            # Uses BOTH absolute % AND relative z-score AND impulse
            # #7: ALL percentages show — even 50/50 gets a label
            if dom_pct >= 85 or (dom_pct >= 75 and z_score >= 2.0):
                strength = "EXTREME"
            elif dom_pct >= 70 or (dom_pct >= 62 and z_score >= 1.5):
                strength = "STRONG"
            elif dom_pct >= 58 or (dom_pct >= 53 and z_score >= 1.0):
                strength = "MODERATE"
            elif dom_pct >= 52:
                strength = "MILD"
            else:
                strength = "BALANCED"  # 50/50 — still show, just labeled balanced

            # Vol confirmation label
            if vol_ratio >= 2.0:   vol_label = "HIGH VOL"
            elif vol_ratio >= 1.3: vol_label = "ABOVE AVG"
            elif vol_ratio >= 0.7: vol_label = "NORMAL"
            else:                  vol_label = "LOW VOL"

            # Fill status
            touches   = int(fvg.get("touches", 0))
            untouched = bool(fvg.get("untouched", True))
            mitigated = bool(fvg.get("mitigated", False))
            if mitigated:       status = "FILLED"
            elif untouched:     status = "UNTOUCHED"
            elif touches == 1:  status = "PARTIAL"
            else:               status = "TOUCHED"

            size_pct = round(abs(top - bottom) / max(bottom, 1e-10) * 100, 3)

            return {
                "top":           round(top, 8),
                "bottom":        round(bottom, 8),
                "direction":     direction,
                "buy_pct":       buy_pct,
                "sell_pct":      sell_pct,
                "dom_pct":       dom_pct,
                "opp_pct":       100.0 - dom_pct if total > 0 else 0.0,
                "strength":      strength,
                "status":        status,
                "size_pct":      size_pct,
                "age":           age,
                "has_data":      has_data and total > 0,
                "impulse_score": impulse_score,
                "vol_label":     vol_label,
                "vol_ratio":     round(vol_ratio, 2),
                "z_score":       round(z_score, 2),
            }

        # Analyze each FVG
        analyzed = [_analyze_fvg(f) for f in fvgs_in]

        # ── Stacking detection ──
        # FVGs are stacked when same direction + zones are adjacent/overlapping
        # + formed close together (age similar)
        def _are_adjacent(a: Dict, b: Dict) -> bool:
            gap = abs(a["top"] - b["bottom"]) if a["top"] < b["bottom"] \
                  else abs(b["top"] - a["bottom"])
            zone_size = max(a["top"] - a["bottom"], b["top"] - b["bottom"], 1e-10)
            return gap / zone_size <= 0.5  # within 50% of zone size = adjacent

        # Group into stacks
        stacks = []
        used   = set()
        for i, fvg_a in enumerate(analyzed):
            if i in used:
                continue
            group = [fvg_a]
            used.add(i)
            for j, fvg_b in enumerate(analyzed):
                if j in used or j == i:
                    continue
                if (fvg_b["direction"] == fvg_a["direction"]
                        and _are_adjacent(fvg_a, fvg_b)):
                    group.append(fvg_b)
                    used.add(j)
            stacks.append(group)

        # Build stack summaries
        stack_summaries = []
        for group in stacks:
            if len(group) == 1:
                stack_summaries.append({
                    "type":       "single",
                    "count":      1,
                    "fvgs":       group,
                    "direction":  group[0]["direction"],
                    "verdict":    None,
                })
                continue

            # Multiple FVGs — impulse zone
            direction  = group[0]["direction"]
            all_same   = all(f["direction"] == direction for f in group)
            untouched  = sum(1 for f in group if f["status"] == "UNTOUCHED")
            extreme    = sum(1 for f in group if f["strength"] == "EXTREME")
            strong     = sum(1 for f in group if f["strength"] in ("EXTREME","STRONG"))
            avg_dom    = round(sum(f["dom_pct"] for f in group) / len(group), 1)
            zone_top   = max(f["top"]    for f in group)
            zone_bot   = min(f["bottom"] for f in group)
            total_size = round(abs(zone_top - zone_bot) / max(zone_bot, 1e-10) * 100, 3)

            if not all_same:
                verdict      = "CONFLICTING"
                verdict_desc = "FVGs point in opposite directions — avoid"
            elif extreme >= 2 and untouched == len(group):
                verdict      = "INSTITUTIONAL IMPULSE"
                verdict_desc = f"{len(group)} stacked FVGs, all extreme imbalance, all untouched"
            elif strong >= 2 and untouched >= len(group) - 1:
                verdict      = "STRONG IMPULSE"
                verdict_desc = f"{len(group)} stacked FVGs with strong imbalance"
            elif untouched >= 2:
                verdict      = "MODERATE IMPULSE"
                verdict_desc = f"{len(group)} stacked FVGs, partially untouched"
            else:
                verdict      = "WEAK IMPULSE"
                verdict_desc = f"{len(group)} stacked FVGs but partially filled"

            stack_summaries.append({
                "type":        "stacked",
                "count":       len(group),
                "fvgs":        group,
                "direction":   direction if all_same else "mixed",
                "zone_top":    round(zone_top, 8),
                "zone_bottom": round(zone_bot, 8),
                "total_size":  total_size,
                "avg_dom_pct": avg_dom,
                "all_untouched": untouched == len(group),
                "untouched_count": untouched,
                "verdict":     verdict,
                "verdict_desc": verdict_desc,
            })

        return jsonify({
            "symbol":   symbol,
            "stacks":   stack_summaries,
            "total_fvgs": len(analyzed),
        })

    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    import os

    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port, debug=False)
