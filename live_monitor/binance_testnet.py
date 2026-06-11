"""Phase 11.5: Binance USDⓈ-M Futures Testnet connector foundation.

Read-only connector. No order placement. No position modification.
No leverage changes. No TP/SL orders. No automation execution.
Locked to demo-fapi.binance.com testnet only — mainnet refused.

Credentials loaded from env vars only:
  BINANCE_TESTNET_API_KEY
  BINANCE_TESTNET_API_SECRET
  BINANCE_TESTNET_BASE_URL   (optional override, must still contain demo-fapi.binance.com)

Secrets are never returned, logged, stored in snapshot_json, or exposed to frontend.
"""
from __future__ import annotations
import hashlib
import hmac
import os
import time
from urllib.parse import urlencode

import main as _m

# ── Constants ─────────────────────────────────────────────────────────────────
_BT_DEFAULT_BASE_URL = "https://demo-fapi.binance.com"
_BT_TESTNET_HOST     = "demo-fapi.binance.com"
_BT_RECV_WINDOW      = 5000
_BT_TIMEOUT_PUBLIC   = 8
_BT_TIMEOUT_SIGNED   = 10

# Phase 11.5: read-only paths allowed.  No POST/DELETE order paths.
_BT_ALLOWED_GET_PATHS = {
    "/fapi/v1/ping",
    "/fapi/v1/time",
    "/fapi/v1/exchangeInfo",
    "/fapi/v2/account",
    "/fapi/v2/balance",
    "/fapi/v2/positionRisk",
    "/fapi/v1/ticker/price",
}

# Phase 11.5A Task 1: signed requests restricted to these 3 paths ONLY.
_BT_SIGNED_ALLOWED_PATHS: frozenset = frozenset({
    "/fapi/v2/account",
    "/fapi/v2/balance",
    "/fapi/v2/positionRisk",
})

# Phase 11.5A Task 2: fragments in any path string that indicate order/execution.
# Checked case-insensitively.  Never allowed regardless of whitelist status.
_BT_BLOCKED_FRAGMENTS: frozenset = frozenset({
    "order",
    "batchorders",
    "leverage",
    "margintype",
    "positionside",
    "countdowncancelall",
    "listenkey",
})


def _lm_bt_is_execution_path(path: str) -> bool:
    """Return True if path contains any blocked execution-related fragment."""
    p = path.lower()
    return any(frag in p for frag in _BT_BLOCKED_FRAGMENTS)


# ── Base URL / testnet guard ──────────────────────────────────────────────────

def _lm_bt_base_url() -> str:
    """Return the configured testnet base URL."""
    return os.environ.get("BINANCE_TESTNET_BASE_URL", _BT_DEFAULT_BASE_URL).rstrip("/")


def _lm_bt_is_testnet_only() -> bool:
    """Return True only if the base URL points to the Binance testnet host."""
    return _BT_TESTNET_HOST in _lm_bt_base_url()


def _lm_bt_credentials_available() -> bool:
    """Return True if both API key and secret are present in env."""
    key = os.environ.get("BINANCE_TESTNET_API_KEY", "").strip()
    sec = os.environ.get("BINANCE_TESTNET_API_SECRET", "").strip()
    return bool(key and sec)


# ── Request helpers ───────────────────────────────────────────────────────────

def _lm_bt_public_request(path: str, params: dict | None = None, method: str = "GET") -> dict:
    """GET a public (unauthenticated) Binance Testnet endpoint.

    Returns structured response — no raw exception raised to caller.
    Phase 11.5A: method guard, execution-path block, testnet lock — all hard-fail.
    """
    # Task 3: method guard — GET only, no network call for anything else
    if method.upper() != "GET":
        return {
            "ok": False, "status_code": 0, "data": {},
            "error": "method_not_allowed_phase11_5",
            "is_timeout": False,
        }
    # Task 4: testnet lock — hard fail, no fallback
    if not _lm_bt_is_testnet_only():
        return {
            "ok": False, "status_code": 0, "data": {},
            "error": "Binance connector locked to testnet only — mainnet refused",
            "is_timeout": False,
        }
    # Task 2: execution-path block — reject before any network call
    if _lm_bt_is_execution_path(path):
        return {
            "ok": False, "status_code": 0, "data": {},
            "error": "execution_path_blocked_phase11_5",
            "is_timeout": False,
        }
    url = f"{_lm_bt_base_url()}{path}"
    try:
        resp = _m.req.get(url, params=params or {}, timeout=_BT_TIMEOUT_PUBLIC)
        ok = resp.status_code == 200
        data: dict = {}
        try:
            data = resp.json()
        except Exception:
            pass
        return {
            "ok":          ok,
            "status_code": resp.status_code,
            "data":        data,
            "error":       "" if ok else str(data.get("msg", "") or resp.text[:120]),
            "is_timeout":  False,
        }
    except Exception as _e:
        is_to = "timeout" in type(_e).__name__.lower() or "timeout" in str(_e).lower()
        return {"ok": False, "status_code": 0, "data": {}, "error": str(_e)[:200], "is_timeout": is_to}


def _lm_bt_signed_request(path: str, params: dict | None = None, method: str = "GET") -> dict:
    """Signed GET to a private Binance Testnet endpoint.

    Phase 11.5A safety gates (in order, all hard-fail before any network call):
    1. Method guard — GET only
    2. Path whitelist — only _BT_SIGNED_ALLOWED_PATHS
    3. Execution-path fragment block — rejects order/leverage/etc. substrings
    4. Testnet lock — demo-fapi.binance.com only
    5. Credentials check
    Then: HMAC-SHA256 signature, X-MBX-APIKEY header, GET request.
    Secret is never returned, logged, or stored.
    """
    # Task 3: method guard — reject non-GET before any other processing
    if method.upper() != "GET":
        return {
            "ok": False, "status_code": 0, "data": {},
            "error": "method_not_allowed_phase11_5",
            "is_timeout": False,
        }
    # Task 1: hard path whitelist — only account/balance/positionRisk allowed
    if path not in _BT_SIGNED_ALLOWED_PATHS:
        return {
            "ok": False, "status_code": 0, "data": {},
            "error": "signed_path_not_allowed_phase11_5",
            "is_timeout": False,
        }
    # Task 2: execution-path fragment block — belt-and-suspenders after whitelist
    if _lm_bt_is_execution_path(path):
        return {
            "ok": False, "status_code": 0, "data": {},
            "error": "execution_path_blocked_phase11_5",
            "is_timeout": False,
        }
    # Task 4: testnet lock — hard fail, no fallback
    if not _lm_bt_is_testnet_only():
        return {
            "ok": False, "status_code": 0, "data": {},
            "error": "Binance connector locked to testnet only — mainnet refused",
            "is_timeout": False,
        }
    if not _lm_bt_credentials_available():
        return {
            "ok": False, "status_code": 0, "data": {},
            "error": "credentials_not_configured",
            "is_timeout": False,
        }

    api_key = os.environ.get("BINANCE_TESTNET_API_KEY", "").strip()
    secret  = os.environ.get("BINANCE_TESTNET_API_SECRET", "").strip()

    p: dict = dict(params or {})
    p["timestamp"]  = int(time.time() * 1000)
    p["recvWindow"] = p.get("recvWindow", _BT_RECV_WINDOW)

    query_string = urlencode(p)
    signature    = hmac.new(
        secret.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    url     = f"{_lm_bt_base_url()}{path}"
    headers = {"X-MBX-APIKEY": api_key}

    try:
        resp = _m.req.get(
            url,
            params={**p, "signature": signature},
            headers=headers,
            timeout=_BT_TIMEOUT_SIGNED,
        )
        ok = resp.status_code == 200
        data: dict = {}
        try:
            data = resp.json()
        except Exception:
            pass
        return {
            "ok":          ok,
            "status_code": resp.status_code,
            "data":        data,
            "error":       "" if ok else str(data.get("msg", "") or resp.text[:120]),
            "is_timeout":  False,
        }
    except Exception as _e:
        is_to = "timeout" in type(_e).__name__.lower() or "timeout" in str(_e).lower()
        return {"ok": False, "status_code": 0, "data": {}, "error": str(_e)[:200], "is_timeout": is_to}


# ── Public helpers ────────────────────────────────────────────────────────────

def _lm_bt_ping() -> dict:
    """Ping the Binance Testnet server (/fapi/v1/ping). No credentials needed."""
    r = _lm_bt_public_request("/fapi/v1/ping")
    return {
        "ok":         r["ok"],
        "ping_ok":    r["ok"],
        "latency_ms": None,
        "error":      r.get("error", ""),
        "is_timeout": r.get("is_timeout", False),
    }


def _lm_bt_exchange_info() -> dict:
    """Fetch compact exchange info from /fapi/v1/exchangeInfo. No credentials needed."""
    r = _lm_bt_public_request("/fapi/v1/exchangeInfo")
    if not r["ok"]:
        return {"ok": False, "error": r.get("error", ""), "symbols": []}
    raw = r["data"] or {}
    symbols_out = []
    for s in (raw.get("symbols") or [])[:300]:
        entry = {
            "symbol":               s.get("symbol"),
            "status":               s.get("status"),
            "contractType":         s.get("contractType"),
            "pricePrecision":       s.get("pricePrecision"),
            "quantityPrecision":    s.get("quantityPrecision"),
        }
        filters_map: dict = {f["filterType"]: f for f in (s.get("filters") or [])}
        lsz = filters_map.get("LOT_SIZE") or {}
        pf  = filters_map.get("PRICE_FILTER") or {}
        mn  = filters_map.get("MIN_NOTIONAL") or {}
        entry["minQty"]      = lsz.get("minQty")
        entry["maxQty"]      = lsz.get("maxQty")
        entry["stepSize"]    = lsz.get("stepSize")
        entry["tickSize"]    = pf.get("tickSize")
        entry["minNotional"] = mn.get("notional")
        symbols_out.append(entry)
    return {
        "ok":          True,
        "symbol_count": len(symbols_out),
        "symbols":     symbols_out,
        "timezone":    raw.get("timezone"),
        "server_time": raw.get("serverTime"),
    }


def _lm_bt_symbol_filters(symbol: str) -> dict:
    """Return filter details for a single symbol. No credentials needed."""
    symbol = (symbol or "").upper().strip()
    r = _lm_bt_public_request("/fapi/v1/exchangeInfo")
    if not r["ok"]:
        return {"ok": False, "symbol": symbol, "error": r.get("error", ""), "found": False}
    raw = r["data"] or {}
    for s in (raw.get("symbols") or []):
        if s.get("symbol") == symbol:
            filters_map = {f["filterType"]: f for f in (s.get("filters") or [])}
            lsz = filters_map.get("LOT_SIZE")     or {}
            pf  = filters_map.get("PRICE_FILTER") or {}
            mn  = filters_map.get("MIN_NOTIONAL") or {}
            return {
                "ok":               True,
                "symbol":           symbol,
                "found":            True,
                "status":           s.get("status"),
                "contractType":     s.get("contractType"),
                "pricePrecision":   s.get("pricePrecision"),
                "quantityPrecision": s.get("quantityPrecision"),
                "minQty":           lsz.get("minQty"),
                "maxQty":           lsz.get("maxQty"),
                "stepSize":         lsz.get("stepSize"),
                "tickSize":         pf.get("tickSize"),
                "minNotional":      mn.get("notional"),
            }
    return {"ok": True, "symbol": symbol, "found": False, "error": "symbol_not_found"}


# ── Signed / private helpers ──────────────────────────────────────────────────

def _lm_bt_account() -> dict:
    """Read account summary from /fapi/v2/account. Returns safe compact summary only."""
    r = _lm_bt_signed_request("/fapi/v2/account")
    if not r["ok"]:
        return {"ok": False, "error": r.get("error", ""), "status_code": r.get("status_code")}
    raw = r["data"] or {}
    # Count non-zero assets
    assets   = [a for a in (raw.get("assets")    or []) if float(a.get("walletBalance", 0)) != 0]
    positions = [p for p in (raw.get("positions") or []) if float(p.get("positionAmt", 0)) != 0]
    return {
        "ok":                    True,
        "total_wallet_balance":  float(raw.get("totalWalletBalance",     0)),
        "available_balance":     float(raw.get("availableBalance",       0)),
        "total_unrealized_profit": float(raw.get("totalUnrealizedProfit", 0)),
        "total_margin_balance":  float(raw.get("totalMarginBalance",     0)),
        "total_cross_wallet_balance": float(raw.get("totalCrossWalletBalance", 0)),
        "assets_count":          len(assets),
        "positions_count":       len(positions),
        "can_trade":             bool(raw.get("canTrade")),
        "can_deposit":           bool(raw.get("canDeposit")),
        "fee_tier":              raw.get("feeTier"),
        "update_time":           raw.get("updateTime"),
    }


def _lm_bt_balance() -> dict:
    """Read USDT balance from /fapi/v2/balance. Returns compact USDT summary."""
    r = _lm_bt_signed_request("/fapi/v2/balance")
    if not r["ok"]:
        return {"ok": False, "error": r.get("error", ""), "status_code": r.get("status_code")}
    raw_list = r["data"] if isinstance(r["data"], list) else []
    usdt = next((a for a in raw_list if a.get("asset") == "USDT"), None)
    if usdt is None:
        return {
            "ok":    True,
            "found": False,
            "asset": "USDT",
            "note":  "USDT not in balance list",
            "all_assets": [a.get("asset") for a in raw_list[:20]],
        }
    return {
        "ok":               True,
        "found":            True,
        "asset":            "USDT",
        "wallet_balance":   float(usdt.get("balance",           0)),
        "available_balance": float(usdt.get("availableBalance", 0)),
        "cross_un_pnl":     float(usdt.get("crossUnPnl",        0)),
        "update_time":      usdt.get("updateTime"),
    }


def _lm_bt_positions(symbol: str | None = None) -> dict:
    """Read open positions from /fapi/v2/positionRisk. Returns compact list.

    If symbol is provided, filters to that symbol only.
    Read-only. No modification.
    """
    params = {}
    if symbol:
        params["symbol"] = str(symbol).upper()
    r = _lm_bt_signed_request("/fapi/v2/positionRisk", params=params)
    if not r["ok"]:
        return {"ok": False, "error": r.get("error", ""), "status_code": r.get("status_code"), "positions": []}
    raw_list = r["data"] if isinstance(r["data"], list) else []
    positions = []
    for p in raw_list:
        try:
            amt = float(p.get("positionAmt", 0))
        except (TypeError, ValueError):
            amt = 0.0
        positions.append({
            "symbol":           p.get("symbol"),
            "positionAmt":      amt,
            "entryPrice":       float(p.get("entryPrice",        0) or 0),
            "markPrice":        float(p.get("markPrice",         0) or 0),
            "unRealizedProfit": float(p.get("unRealizedProfit",  0) or 0),
            "liquidationPrice": float(p.get("liquidationPrice",  0) or 0),
            "leverage":         int(p.get("leverage",            1) or 1),
            "marginType":       p.get("marginType"),
            "isolatedMargin":   float(p.get("isolatedMargin",    0) or 0),
            "positionSide":     p.get("positionSide"),
        })
    return {
        "ok":             True,
        "positions":      positions,
        "total":          len(positions),
        "open_count":     sum(1 for pos in positions if pos["positionAmt"] != 0),
        "filter_symbol":  symbol,
    }


# ── Phase 11.7A: Manual testnet LIMIT order placement ────────────────────────
# Isolated from _lm_bt_signed_request (which is read-only GET only).
# This is the ONLY function that may POST to /fapi/v1/order.

_BT_ORDER_PATH           = "/fapi/v1/order"
_BT_ORDER_ENV_FLAG       = "BINANCE_TESTNET_ORDER_ENABLED"
_BT_ORDER_CLIENT_PREFIX  = "ZYNILM_"
_BT_ORDER_TIMEOUT        = 12

# Order types allowed in Phase 11.7A (entry LIMIT only)
_BT_ALLOWED_ORDER_TYPE  = "LIMIT"
_BT_ALLOWED_TIF         = "GTC"
_BT_ALLOWED_SIDES       = frozenset({"BUY", "SELL"})

# Order types/params that are permanently blocked
_BT_BLOCKED_ORDER_TYPES = frozenset({
    "MARKET", "STOP", "TAKE_PROFIT", "STOP_MARKET",
    "TAKE_PROFIT_MARKET", "TRAILING_STOP_MARKET",
})
_BT_BLOCKED_ORDER_PARAMS = frozenset({
    "reduceOnly", "closePosition", "leverage", "marginType",
    "positionSide", "stopPrice", "activationPrice", "callbackRate",
})


def _lm_bt_order_enabled() -> bool:
    """Return True only when BINANCE_TESTNET_ORDER_ENABLED=1 is set in env."""
    return os.environ.get(_BT_ORDER_ENV_FLAG, "").strip() == "1"


def _lm_bt_place_limit_order_testnet(
    symbol: str,
    side: str,
    quantity: str,
    price: str,
    client_order_id: str | None = None,
) -> dict:
    """Phase 11.7A: Place a LIMIT entry order on Binance Futures Testnet.

    THIS IS THE ONLY FUNCTION THAT CAN POST TO /fapi/v1/order.
    _lm_bt_signed_request() is NOT called — it is GET-only and read-only.
    This function builds its own HMAC-SHA256 POST request independently.

    Safety gates (all hard-fail before any network call):
    1. Testnet lock — demo-fapi.binance.com only
    2. BINANCE_TESTNET_ORDER_ENABLED=1 env var required
    3. Credentials available
    4. Side: BUY or SELL only
    5. Type: LIMIT hardcoded — no other type accepted
    6. TimeInForce: GTC hardcoded — no other TIF accepted
    7. quantity > 0
    8. price > 0
    9. No blocked params or order types in call

    Secret is never returned, logged, or stored in any response.
    """
    # Gate 1: testnet lock
    if not _lm_bt_is_testnet_only():
        return {
            "ok": False, "status_code": 0,
            "error": "Binance connector locked to testnet only — mainnet refused",
            "order_placed": False,
        }

    # Gate 2: env flag required
    if not _lm_bt_order_enabled():
        return {
            "ok": False, "status_code": 0,
            "error": "testnet_order_placement_disabled",
            "hint":  f"Set {_BT_ORDER_ENV_FLAG}=1 to enable testnet order placement.",
            "order_placed": False,
        }

    # Gate 3: credentials
    if not _lm_bt_credentials_available():
        return {
            "ok": False, "status_code": 0,
            "error": "credentials_not_configured",
            "order_placed": False,
        }

    # Gate 4: side validation
    side_upper = str(side or "").upper().strip()
    if side_upper not in _BT_ALLOWED_SIDES:
        return {
            "ok": False, "status_code": 0,
            "error": f"invalid_side:{side_upper} — only BUY or SELL allowed",
            "order_placed": False,
        }

    # Gate 5+6: type and TIF are hardcoded — not caller-supplied
    order_type = _BT_ALLOWED_ORDER_TYPE   # always LIMIT
    tif        = _BT_ALLOWED_TIF          # always GTC

    # Gate 7: quantity
    try:
        qty_f = float(quantity)
    except (TypeError, ValueError):
        qty_f = 0.0
    if qty_f <= 0:
        return {
            "ok": False, "status_code": 0,
            "error": "quantity_must_be_positive",
            "order_placed": False,
        }

    # Gate 8: price
    try:
        price_f = float(price)
    except (TypeError, ValueError):
        price_f = 0.0
    if price_f <= 0:
        return {
            "ok": False, "status_code": 0,
            "error": "price_must_be_positive",
            "order_placed": False,
        }

    # Build client order ID
    symbol_upper = str(symbol or "").upper().strip()
    ts_ms        = int(time.time() * 1000)
    coid = (
        str(client_order_id).strip()[:36]
        if client_order_id
        else f"{_BT_ORDER_CLIENT_PREFIX}{symbol_upper[:8]}_{str(ts_ms)[-9:]}"
    )

    # Build POST params
    api_key = os.environ.get("BINANCE_TESTNET_API_KEY", "").strip()
    secret  = os.environ.get("BINANCE_TESTNET_API_SECRET", "").strip()

    params: dict = {
        "symbol":           symbol_upper,
        "side":             side_upper,
        "type":             order_type,
        "timeInForce":      tif,
        "quantity":         str(quantity),
        "price":            str(price),
        "newClientOrderId": coid,
        "timestamp":        ts_ms,
        "recvWindow":       _BT_RECV_WINDOW,
    }

    query_string = urlencode(params)
    signature    = hmac.new(
        secret.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    params["signature"] = signature

    url     = f"{_lm_bt_base_url()}{_BT_ORDER_PATH}"
    headers = {"X-MBX-APIKEY": api_key}

    # Safe request log (no secret, no signature)
    safe_request = {
        "symbol": symbol_upper, "side": side_upper,
        "type": order_type, "timeInForce": tif,
        "quantity": str(quantity), "price": str(price),
        "newClientOrderId": coid,
    }

    try:
        resp = _m.req.post(
            url,
            params=params,
            headers=headers,
            timeout=_BT_ORDER_TIMEOUT,
        )
        placed = resp.status_code == 200
        raw_data: dict = {}
        try:
            raw_data = resp.json()
        except Exception:
            pass

        # Safe compact response — no secrets
        safe_resp = {
            "orderId":        raw_data.get("orderId"),
            "clientOrderId":  raw_data.get("clientOrderId"),
            "symbol":         raw_data.get("symbol"),
            "side":           raw_data.get("side"),
            "type":           raw_data.get("type"),
            "timeInForce":    raw_data.get("timeInForce"),
            "origQty":        raw_data.get("origQty"),
            "price":          raw_data.get("price"),
            "status":         raw_data.get("status"),
            "transactTime":   raw_data.get("transactTime"),
            "msg":            raw_data.get("msg"),
            "code":           raw_data.get("code"),
        }

        return {
            "ok":              placed,
            "order_placed":    placed,
            "status_code":     resp.status_code,
            "client_order_id": coid,
            "binance_order_id": str(raw_data.get("orderId", "")) if placed else None,
            "order_status":    raw_data.get("status") if placed else None,
            "data":            safe_resp,
            "error":           "" if placed else str(raw_data.get("msg", "") or resp.text[:200]),
            "safe_request":    safe_request,
            "is_timeout":      False,
        }

    except Exception as _eo:
        is_to = "timeout" in type(_eo).__name__.lower() or "timeout" in str(_eo).lower()
        return {
            "ok":           False,
            "order_placed": False,
            "status_code":  0,
            "error":        str(_eo)[:200],
            "is_timeout":   is_to,
            "safe_request": safe_request,
        }


# ── Health check ──────────────────────────────────────────────────────────────

def _lm_bt_health() -> dict:
    """Run a full read-only health check on the Binance Testnet connector.

    Public checks (ping, server_time) run regardless of credentials.
    Account, balance, positions checks are skipped if credentials are missing.
    Returns safe summary — no secrets, no full account payload.
    """
    checked_at   = int(time.time())
    base_url     = _lm_bt_base_url()
    creds_ok     = _lm_bt_credentials_available()
    testnet_only = _lm_bt_is_testnet_only()
    errors: list = []

    # Task 4: testnet lock — hard fail, no fallback, no warning-only mode
    if not testnet_only:
        return {
            "ok":                      False,
            "phase":                   "11.5_read_only",
            "orders_enabled":          False,
            "execution_enabled":       False,
            "base_url":                base_url,
            "testnet_locked":          False,
            "credentials_configured":  creds_ok,
            "ping_ok":                 False,
            "server_time_ok":          False,
            "account_ok":              False,
            "balance_ok":              False,
            "positions_ok":            False,
            "error_summary":           ["Binance connector locked to testnet only — mainnet refused"],
            "checked_at":              checked_at,
        }

    # 1. Ping
    ping_r = _lm_bt_public_request("/fapi/v1/ping")
    ping_ok = ping_r["ok"]
    if not ping_ok:
        errors.append(f"ping_failed: {ping_r.get('error', '')[:60]}")

    # 2. Server time
    time_r = _lm_bt_public_request("/fapi/v1/time")
    server_time_ok = time_r["ok"]
    if not server_time_ok:
        errors.append(f"server_time_failed: {time_r.get('error', '')[:60]}")

    # 3-5. Signed checks (skip if no credentials)
    account_ok   = False
    balance_ok   = False
    positions_ok = False
    avail_usdt: float | None = None
    account_skipped  = not creds_ok
    balance_skipped  = not creds_ok
    positions_skipped = not creds_ok

    if creds_ok:
        acc_r = _lm_bt_account()
        account_ok = acc_r.get("ok", False)
        if not account_ok:
            errors.append(f"account_failed: {acc_r.get('error', '')[:60]}")

        bal_r = _lm_bt_balance()
        balance_ok = bal_r.get("ok", False)
        if balance_ok and bal_r.get("found"):
            avail_usdt = bal_r.get("available_balance")
        if not balance_ok:
            errors.append(f"balance_failed: {bal_r.get('error', '')[:60]}")

        pos_r = _lm_bt_positions()
        positions_ok = pos_r.get("ok", False)
        if not positions_ok:
            errors.append(f"positions_failed: {pos_r.get('error', '')[:60]}")

    overall_ok = (
        testnet_only and ping_ok and server_time_ok and
        (account_ok  or account_skipped) and
        (balance_ok  or balance_skipped) and
        (positions_ok or positions_skipped)
    )

    return {
        "ok":                      overall_ok,
        "phase":                   "11.5_read_only",
        "orders_enabled":          False,
        "execution_enabled":       False,
        "base_url":                base_url,
        "testnet_locked":          testnet_only,
        "credentials_configured":  creds_ok,
        "ping_ok":                 ping_ok,
        "server_time_ok":          server_time_ok,
        "account_ok":              account_ok   if creds_ok else None,
        "balance_ok":              balance_ok   if creds_ok else None,
        "positions_ok":            positions_ok if creds_ok else None,
        "account_skipped":         account_skipped,
        "balance_skipped":         balance_skipped,
        "positions_skipped":       positions_skipped,
        "available_usdt":          avail_usdt,
        "error_summary":           errors[:8],
        "checked_at":              checked_at,
        "advisory_note":           (
            "Phase 11.5 read-only connector. "
            "No order placement. No automation execution."
        ),
    }
