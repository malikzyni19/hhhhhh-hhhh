"""Bias Shift scanner regression tests.

Covers the fixes in this change:
  1. Confirmed detection mode returned zero setups on Balanced/Strong because
     the only candidate signal candle was the last closed one, leaving the
     confirmation loop nothing to scan.
  2. Round-robin cursors are scoped, so the dashboard tile no longer advances
     the Bias tab's position through the market.
  3. Klines are fetched in parallel and every requested symbol is still
     analysed exactly once.

Run: DATABASE_URL=sqlite:///./_test_bias.db python3 _test_bias_shift_fixes.py
"""
import os
import sys

os.environ.setdefault("DATABASE_URL", "sqlite:///./_test_bias.db")

import main  # noqa: E402


# ── synthetic candle builder ────────────────────────────────────────────────
def _k(open_, high, low, close, vol, t):
    return {"open": open_, "high": high, "low": low, "close": close,
            "volume": vol, "openTime": t}


def build_klines(follow_up: str = "none"):
    """Clean uptrend → bearish rejection candle → optional follow-up candle.

    Layout (indices are into the CLOSED list the scanner analyses):
        [0 .. 29]  flat base
        [30 .. 36] clean bullish drive
        [37]       bearish rejection candle
        [38]       follow-up candle, only when follow_up != "none"
        + one extra running candle that the scanner strips

    follow_up:
        "none"    — rejection is the newest closed candle (what Early sees)
        "break"   — follow-up breaks the rejection low  → Confirmed passes
        "nobreak" — follow-up holds above it            → Confirmed rejects
    """
    out, t, price = [], 1_700_000_000_000, 100.0
    step = 86_400_000

    # flat base — keeps average volume low so the rejection isn't a vol outlier
    for _ in range(30):
        out.append(_k(price, price * 1.002, price * 0.998, price, 1000.0, t))
        t += step

    # clean bullish drive: 7 green candles, ~1.6% each, shallow wicks
    for _ in range(7):
        o = price
        c = price * 1.016
        out.append(_k(o, c * 1.002, o * 0.999, c, 1000.0, t))
        price = c
        t += step

    # bearish rejection candle: long upper wick, red body closing below midpoint
    o = price
    high = price * 1.03
    close = price * 0.985
    low = close * 0.998
    out.append(_k(o, high, low, close, 1200.0, t))
    rej_low = low
    price = close
    t += step

    # follow-up candle — breaks the rejection low (confirmation) or does not
    c2 = price
    if follow_up == "break":
        c2 = rej_low * 0.985
        out.append(_k(price, price * 1.002, c2 * 0.999, c2, 1000.0, t))
        t += step
    elif follow_up == "nobreak":
        c2 = rej_low * 1.05
        out.append(_k(price, price * 1.01, rej_low * 1.02, c2, 1000.0, t))
        t += step

    # still-running candle (scanner strips this one)
    out.append(_k(c2, c2 * 1.001, c2 * 0.999, c2, 500.0, t))
    return out


# ── harness ─────────────────────────────────────────────────────────────────
FETCHED: list = []


def install_stubs(klines):
    """Bypass auth/tokens and serve synthetic candles."""
    main.app.config["LOGIN_DISABLED"] = True
    main._guest_tab_check = lambda *_a, **_k: None
    main._check_and_get_token_user = lambda *_a, **_k: (None, None)
    main.consume_tokens = lambda *_a, **_k: None
    main.get_pairs_exchange = lambda *_a, **_k: [
        {"symbol": f"TEST{i}USDT"} for i in range(50)
    ]

    def _fake_klines(sym, tf, limit, market, exchange):
        FETCHED.append(sym)
        return klines

    main.get_klines_exchange = _fake_klines


def scan(body):
    with main.app.test_client() as cli:
        with cli.session_transaction() as sess:
            sess["logged_in"] = True
            sess["username"] = "tester"
        resp = cli.post("/api/bias_scan", json=body)
        assert resp.status_code == 200, f"HTTP {resp.status_code}: {resp.data[:300]}"
        return resp.get_json()


BASE = {
    "timeframe": "1d", "tf": "1d", "market": "perpetual", "exchange": "binance",
    "scanMode": "selected", "symbols": ["TESTUSDT"],
    "mode": "normal", "biasStrength": "balanced",
    "confluenceMode": "optional", "minimumGrade": "C+",
    "volumeFilter": "optional",
}

failures = []


def check(name, cond, detail=""):
    print(("  PASS  " if cond else "  FAIL  ") + name + (f"  [{detail}]" if detail else ""))
    if not cond:
        failures.append(name)


def main_test():
    install_stubs(build_klines("none"))

    print("\n[1] baseline — Early detection finds the rejection setup")
    d = scan(dict(BASE, detectionMode="early"))
    early_n = len(d["results"])
    check("early mode returns a setup", early_n > 0,
          f"{early_n} results, diag={d['diagnostics']['rejected']}")

    print("\n[2] the bug — Confirmed + Balanced (signalSearchCandles=1)")
    install_stubs(build_klines("break"))
    d = scan(dict(BASE, detectionMode="confirmed"))
    su = d["diagnostics"]["settingsUsed"]
    check("signal search widened to >= 2", su["signalSearchCandles"] >= 2,
          f"signalSearchCandles={su['signalSearchCandles']}")
    check("floor flagged in diagnostics", su["signalSearchFloorApplied"] is True)
    check("confirmed mode now returns a setup", len(d["results"]) > 0,
          f"{len(d['results'])} results, diag={d['diagnostics']['rejected']}")
    if d["results"]:
        check("setup is marked confirmed",
              d["results"][0]["confirmationStatus"] == "confirmed",
              d["results"][0]["confirmationStatus"])

    print("\n[3] Confirmed + Strong (also had signalSearchCandles=1)")
    d = scan(dict(BASE, biasStrength="strong", detectionMode="confirmed"))
    check("strong preset confirms too", len(d["results"]) > 0,
          f"{len(d['results'])} results")

    print("\n[4] Confirmed still rejects when no follow-up breaks the level")
    install_stubs(build_klines("nobreak"))
    d = scan(dict(BASE, detectionMode="confirmed"))
    check("unconfirmed setup is filtered out", len(d["results"]) == 0,
          f"{len(d['results'])} results")
    check("counted as notConfirmed",
          d["diagnostics"]["rejected"]["notConfirmed"] > 0,
          str(d["diagnostics"]["rejected"]))

    print("\n[5] Early mode is untouched by the floor")
    install_stubs(build_klines("none"))
    d = scan(dict(BASE, detectionMode="early"))
    check("no floor applied in early mode",
          d["diagnostics"]["settingsUsed"]["signalSearchFloorApplied"] is False)
    check("early result count unchanged", len(d["results"]) == early_n,
          f"{len(d['results'])} vs {early_n}")

    print("\n[6] round-robin cursor is scoped per caller")
    main.BIAS_SCAN_CURSOR.clear()
    mk = dict(BASE, scanMode="market", symbols=[], pairsPerCycle=10,
              detectionMode="early")
    scan(mk)
    after_scanner = dict(main.BIAS_SCAN_CURSOR)
    scan(dict(mk, cursorScope="dashboard"))
    keys = sorted(main.BIAS_SCAN_CURSOR)
    check("scanner and dashboard hold separate cursors", len(keys) == 2, str(keys))
    # cursor keys are user|exchange|market|tf|cursorScope|scanMode
    scanner_key = [k for k in keys if "|scanner|" in k][0]
    check("dashboard scan did not move the scanner cursor",
          main.BIAS_SCAN_CURSOR[scanner_key] == after_scanner[scanner_key],
          f"{after_scanner[scanner_key]} -> {main.BIAS_SCAN_CURSOR[scanner_key]}")

    print("\n[7] parallel fetch analyses every requested symbol once")
    FETCHED.clear()
    syms = [f"P{i}USDT" for i in range(24)]
    # Selected scope now batches by pairsPerCycle too, so ask for the whole
    # list in one batch — otherwise the default 20 would defer the last four.
    d = scan(dict(BASE, symbols=syms, detectionMode="early",
                  pairsPerCycle=len(syms)))
    check("every symbol fetched exactly once", sorted(FETCHED) == sorted(syms),
          f"{len(FETCHED)} fetches for {len(syms)} symbols")
    check("every symbol analysed",
          d["diagnostics"]["symbolsScanned"] == len(syms),
          f"symbolsScanned={d['diagnostics']['symbolsScanned']}")
    check("one result per symbol", len(d["results"]) == len(syms),
          f"{len(d['results'])} results")

    print("\n[7b] Selected scope honours pairs / cycle")
    main.BIAS_SCAN_CURSOR.clear()
    FETCHED.clear()
    many = [f"Q{i}USDT" for i in range(200)]
    d = scan(dict(BASE, symbols=many, detectionMode="early", pairsPerCycle=24))
    check("a 200-pair selection scans only one batch", len(FETCHED) == 24,
          f"{len(FETCHED)} fetched")
    cov = d.get("marketCoverage") or {}
    check("coverage reports the selected universe", cov.get("totalPairs") == 200, str(cov))
    check("batch is reported", (cov.get("startIndex"), cov.get("endIndex")) == (1, 24), str(cov))

    print("\n[8] diagnostics count found setups before the grade filter")
    d = scan(dict(BASE, symbols=syms, detectionMode="early", minimumGrade="A",
                  pairsPerCycle=len(syms)))
    dg = d["diagnostics"]
    check("setupsFoundBeforeFilters counts pre-filter candidates",
          dg["setupsFoundBeforeFilters"] == len(syms),
          f"{dg['setupsFoundBeforeFilters']} vs {len(syms)}")
    check("grade filter accounted for",
          dg["setupsFoundBeforeFilters"] - dg["rejected"]["minimumGrade"]
          - dg["rejected"]["invalidated"] == dg["setupsReturned"],
          f"found={dg['setupsFoundBeforeFilters']} "
          f"grade={dg['rejected']['minimumGrade']} "
          f"inval={dg['rejected']['invalidated']} kept={dg['setupsReturned']}")


if __name__ == "__main__":
    main_test()
    print("\n" + ("-" * 60))
    if failures:
        print(f"FAILED ({len(failures)}): " + ", ".join(failures))
        sys.exit(1)
    print("All bias shift checks passed.")
