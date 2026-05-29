"""
MEXC Capability Audit — ZyNi SMC Screener Phase 8.3 Pre-Flight
===============================================================
Purpose : Check which MEXC API capabilities are reachable WITHOUT placing orders.
Usage   : python scripts/mexc_capability_audit.py [--symbol BTCUSDT]

Environment variables (all optional):
  MEXC_API_KEY       — If absent, all private checks are skipped.
  MEXC_API_SECRET    — Required alongside MEXC_API_KEY for private auth.
  MEXC_DEMO_MODE     — "true" or "false" (informational only; no demo execution).

SAFETY CONTRACT (read before modifying):
  - This script NEVER calls order-submit, order-cancel, or batch-order endpoints.
  - It NEVER stores API keys to disk or logs them to stdout.
  - It NEVER modifies account state.
  - Private checks (account info, open orders) are read-only GET requests.
  - demo_order_submit_supported is always reported as "not_tested" — we do NOT
    probe demo-order endpoints here.
"""

import os
import sys
import time
import hmac
import hashlib
import argparse
import json
from typing import Any, Dict

try:
    import requests
except ImportError:
    print("ERROR: 'requests' not installed. Run: pip install requests")
    sys.exit(1)

# ── Constants ──────────────────────────────────────────────────────────────────
MEXC_SPOT_BASE  = "https://api.mexc.com/api/v3"
MEXC_PERP_BASE  = "https://contract.mexc.com/api/v1/contract"
TIMEOUT         = 8   # seconds per request
SAFE_SYMBOL_SPOT = "BTCUSDT"
SAFE_SYMBOL_PERP = "BTC_USDT"

# ── Helpers ────────────────────────────────────────────────────────────────────

def _get(url: str, params: Dict = None, headers: Dict = None) -> tuple:
    """GET with timeout. Returns (ok: bool, status_code: int, body: Any)."""
    try:
        r = requests.get(url, params=params, headers=headers or {}, timeout=TIMEOUT)
        try:
            body = r.json()
        except Exception:
            body = r.text
        return r.status_code == 200, r.status_code, body
    except Exception as e:
        return False, 0, str(e)


def _sign_query(params: Dict, secret: str) -> str:
    """HMAC-SHA256 signature for MEXC private endpoints."""
    qs = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
    return hmac.new(secret.encode(), qs.encode(), hashlib.sha256).hexdigest()


def _private_get(url: str, params: Dict, api_key: str, api_secret: str) -> tuple:
    """Authenticated GET — read-only. Never submits orders."""
    ts = int(time.time() * 1000)
    p  = {**params, "timestamp": ts, "recvWindow": 5000}
    p["signature"] = _sign_query(p, api_secret)
    headers = {"X-MEXC-APIKEY": api_key}
    return _get(url, params=p, headers=headers)


# ── Public checks ──────────────────────────────────────────────────────────────

def check_public_candles(symbol: str) -> Dict:
    """Fetch last 5 spot klines for symbol."""
    url = f"{MEXC_SPOT_BASE}/klines"
    ok, code, body = _get(url, params={"symbol": symbol, "interval": "15m", "limit": 5})
    if ok and isinstance(body, list) and len(body) > 0:
        return {"ok": True, "candle_count": len(body), "sample_open": body[-1][1] if body else None}
    return {"ok": False, "status_code": code, "error": str(body)[:200]}


def check_public_perp_candles(symbol: str) -> Dict:
    """Fetch last 5 MEXC perpetual klines for symbol."""
    url = f"{MEXC_PERP_BASE}/kline/{symbol}"
    ok, code, body = _get(url, params={"interval": "Min15", "limit": 5})
    if ok:
        return {"ok": True, "raw_keys": list(body.keys()) if isinstance(body, dict) else type(body).__name__}
    return {"ok": False, "status_code": code, "error": str(body)[:200]}


def check_public_spot_ticker(symbol: str) -> Dict:
    """Fetch 24h ticker for symbol."""
    url = f"{MEXC_SPOT_BASE}/ticker/24hr"
    ok, code, body = _get(url, params={"symbol": symbol})
    if ok and isinstance(body, dict) and "lastPrice" in body:
        return {"ok": True, "last_price": body.get("lastPrice"), "volume": body.get("volume")}
    return {"ok": False, "status_code": code, "error": str(body)[:200]}


def check_public_perp_ticker(symbol: str) -> Dict:
    """Fetch perpetual detail for symbol."""
    url = f"{MEXC_PERP_BASE}/detail"
    ok, code, body = _get(url, params={"symbol": symbol})
    if ok:
        data = body.get("data") if isinstance(body, dict) else {}
        return {"ok": True, "contract_size": data.get("contractSize") if data else None}
    return {"ok": False, "status_code": code, "error": str(body)[:200]}


# ── Private checks (read-only) ─────────────────────────────────────────────────

def check_private_account_info(api_key: str, api_secret: str) -> Dict:
    """GET /account — read-only, no order submission."""
    url = f"{MEXC_SPOT_BASE}/account"
    ok, code, body = _private_get(url, {}, api_key, api_secret)
    if ok and isinstance(body, dict) and "balances" in body:
        bal_count = len(body["balances"])
        return {"ok": True, "balance_entries": bal_count}
    return {"ok": False, "status_code": code, "error": str(body)[:200]}


def check_private_open_orders(api_key: str, api_secret: str, symbol: str) -> Dict:
    """GET /openOrders — read-only, no modification."""
    url = f"{MEXC_SPOT_BASE}/openOrders"
    ok, code, body = _private_get(url, {"symbol": symbol}, api_key, api_secret)
    if ok and isinstance(body, list):
        return {"ok": True, "open_order_count": len(body)}
    return {"ok": False, "status_code": code, "error": str(body)[:200]}


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="MEXC Capability Audit (read-only)")
    parser.add_argument("--symbol", default=SAFE_SYMBOL_SPOT,
                        help=f"Spot symbol to test (default: {SAFE_SYMBOL_SPOT})")
    args = parser.parse_args()

    spot_sym = args.symbol.upper().replace("-", "").replace("_", "")
    perp_sym = spot_sym[:-4] + "_USDT" if spot_sym.endswith("USDT") else spot_sym

    api_key    = os.environ.get("MEXC_API_KEY", "")
    api_secret = os.environ.get("MEXC_API_SECRET", "")
    demo_mode  = os.environ.get("MEXC_DEMO_MODE", "false").lower()
    has_keys   = bool(api_key and api_secret)

    report: Dict[str, Any] = {
        "audit_time":                  time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "symbol_tested":               spot_sym,
        "demo_mode_env":               demo_mode,
        "keys_present":                has_keys,
        # Public
        "public_spot_candles":         None,
        "public_perp_candles":         None,
        "public_spot_ticker":          None,
        "public_perp_ticker":          None,
        # Private (skipped if no keys)
        "private_auth_possible":       None,
        "account_info_possible":       None,
        "order_status_possible":       None,
        # Always untested — safety contract
        "demo_order_submit_supported": "not_tested",
        "real_order_submit_supported": "not_tested",
        "warnings":                    [],
    }

    print("=" * 60)
    print("ZyNi MEXC Capability Audit (Phase 8.3 Pre-Flight)")
    print("=" * 60)
    print(f"Symbol       : {spot_sym} / {perp_sym}")
    print(f"Keys present : {has_keys}")
    print(f"Demo mode env: {demo_mode}")
    print()

    # — Public spot candles —
    print("[ ] Checking public spot klines...")
    r = check_public_candles(spot_sym)
    report["public_spot_candles"] = r
    status = "OK" if r["ok"] else "FAIL"
    print(f"    [{status}] spot klines: {r}")

    # — Public perpetual candles —
    print("[ ] Checking public perp klines...")
    r = check_public_perp_candles(perp_sym)
    report["public_perp_candles"] = r
    status = "OK" if r["ok"] else "FAIL"
    print(f"    [{status}] perp klines: {r}")

    # — Public spot ticker —
    print("[ ] Checking public spot ticker...")
    r = check_public_spot_ticker(spot_sym)
    report["public_spot_ticker"] = r
    status = "OK" if r["ok"] else "FAIL"
    print(f"    [{status}] spot ticker: {r}")

    # — Public perp ticker —
    print("[ ] Checking public perp ticker...")
    r = check_public_perp_ticker(perp_sym)
    report["public_perp_ticker"] = r
    status = "OK" if r["ok"] else "FAIL"
    print(f"    [{status}] perp ticker: {r}")

    if not has_keys:
        msg = "MEXC_API_KEY / MEXC_API_SECRET not set — all private checks skipped."
        print(f"\n[INFO] {msg}")
        report["private_auth_possible"] = "skipped_no_keys"
        report["account_info_possible"] = "skipped_no_keys"
        report["order_status_possible"] = "skipped_no_keys"
        report["warnings"].append(msg)
    else:
        # — Private account info (read-only) —
        print("\n[ ] Checking private account info (read-only GET)...")
        r = check_private_account_info(api_key, api_secret)
        report["private_auth_possible"] = r["ok"]
        report["account_info_possible"] = r
        status = "OK" if r["ok"] else "FAIL"
        print(f"    [{status}] account info: {r}")

        # — Private open orders (read-only) —
        print("[ ] Checking private open orders (read-only GET)...")
        r = check_private_open_orders(api_key, api_secret, spot_sym)
        report["order_status_possible"] = r
        status = "OK" if r["ok"] else "FAIL"
        print(f"    [{status}] open orders: {r}")

    # — Safety summary —
    report["warnings"].append(
        "demo_order_submit_supported is always 'not_tested' — "
        "this script never probes order-submit endpoints."
    )
    if demo_mode == "true":
        report["warnings"].append(
            "MEXC_DEMO_MODE=true detected in env. "
            "Demo order execution must be implemented in Phase 9.6 only, "
            "after LiveMonitorTrade table exists and Risk Guard is active."
        )

    print()
    print("=" * 60)
    print("REPORT (JSON)")
    print("=" * 60)
    # Keys are never printed
    safe_report = {k: v for k, v in report.items()}
    print(json.dumps(safe_report, indent=2))
    print()
    print("SAFETY CONTRACT: No orders were placed or cancelled.")
    print("=" * 60)


if __name__ == "__main__":
    main()
