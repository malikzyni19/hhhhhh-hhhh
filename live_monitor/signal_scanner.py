"""Phase SIG-8: Background scanning — the funnel finds setups on its own.

Every scan in this app lives behind a button. That was fine when a human was
always the one asking, but a signal service cannot depend on someone having
the site open: with the browser closed nothing was ever scanned, so nothing
was ever found, so nothing was ever sent.

This runs the same scans on a timer, server-side, with no browser involved.

It deliberately calls the app's own scan endpoints rather than reimplementing
their logic. Those endpoints are hundreds of lines each and already carry the
intake hooks; duplicating them would guarantee the copy drifts out of step
with the real scanners the moment either changes.

Load is spread by rotating: one scan type per cycle, each sweeping the market
in batches. Scanning everything every cycle would multiply both exchange API
weight and database traffic for no benefit — a 4-hour setup does not need
checking every minute.
"""
from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timezone

import main as _m


# One scan type per cycle, in this order, looping. With the default 15-minute
# cycle each type runs roughly every 75 minutes.
SCAN_SEQUENCE = ["scan", "compressed", "bias", "ath_atl", "trending"]

_state_lock = threading.Lock()
_state = {"index": 0, "cycles": 0, "last_run": None, "last_result": None,
          "last_error": None, "running": False}


def scanner_enabled() -> bool:
    return (os.environ.get("ZYNI_SIG_SCAN_ENABLED", "1") or "").strip().lower() \
        in ("1", "true", "yes", "on")


def _env_cycle_seconds() -> int:
    try:
        return max(120, min(3600, int(os.environ.get("ZYNI_SIG_SCAN_CYCLE_SEC", "900"))))
    except (TypeError, ValueError):
        return 900


def _env_pairs_per_cycle() -> int:
    try:
        return max(5, min(120, int(os.environ.get("ZYNI_SIG_SCAN_PAIRS", "30"))))
    except (TypeError, ValueError):
        return 30


def _scanner_pace() -> dict:
    """How fast to scan and how much per scan.

    These come from the settings page of the account the scanner runs as, so
    the two numbers that most affect cost and responsiveness can be changed
    without a redeploy. The environment values remain the fallback for a
    server with nobody opted in yet.
    """
    pace = {"interval": _env_cycle_seconds(), "pairs": _env_pairs_per_cycle(),
            "source": "environment"}
    try:
        from models import LiveMonitorSignalSettings as _S
        from sqlalchemy.orm import load_only as _load_only
        with _m.app.app_context():
            row = (_S.query
                   .options(_load_only(_S.user_id, _S.enabled,
                                       _S.scan_interval_sec,
                                       _S.scan_pairs_per_cycle))
                   .filter(_S.enabled.is_(True))
                   .order_by(_S.user_id.asc()).first())
            if row is not None:
                iv = getattr(row, "scan_interval_sec", None)
                pp = getattr(row, "scan_pairs_per_cycle", None)
                if iv:
                    pace["interval"] = max(120, min(3600, int(iv)))
                if pp:
                    pace["pairs"] = max(5, min(120, int(pp)))
                pace["source"] = "settings"
    except Exception as exc:
        print(f"[SIG-8] pace lookup failed, using environment: {str(exc)[:100]}")
    return pace


def cycle_seconds() -> int:
    return _scanner_pace()["interval"]


def pairs_per_cycle() -> int:
    """Pairs swept per scan. The main lever on exchange API weight."""
    return _scanner_pace()["pairs"]


def scan_exchange() -> str:
    return (os.environ.get("ZYNI_SIG_SCAN_EXCHANGE", "binance") or "binance").lower()


def scan_market() -> str:
    return (os.environ.get("ZYNI_SIG_SCAN_MARKET", "perpetual") or "perpetual").lower()


def _payload_for(kind: str) -> tuple:
    """(url, json_body) for one scan type.

    Defaults mirror what the tabs send, with market mode + round robin so each
    run continues where the last left off and the whole market is covered over
    time rather than re-scanning the same handful of pairs.
    """
    ex, mkt, n = scan_exchange(), scan_market(), pairs_per_cycle()

    if kind == "scan":
        return "/api/scan", {
            "scanMode": "market", "roundRobin": True,
            "pairsPerCycle": n, "exchange": ex, "market": mkt,
            "settings": {},
        }
    if kind == "compressed":
        return "/api/compressed_scan", {
            "timeframe": "1h", "exchange": ex, "market": mkt,
            "lookback": 12, "maxPct": 2.0,
        }
    if kind == "bias":
        return "/api/bias_scan", {
            "timeframe": "1d", "exchange": ex, "market": mkt,
            "scanMode": "market", "pairsPerCycle": n, "roundRobin": True,
        }
    if kind == "ath_atl":
        return "/api/ath_atl_scan", {
            "action": "scan", "mode": "both", "status": "breaking_now",
            "exchange": ex, "market": mkt, "windowHours": 24,
            "pairsPerBatch": n,
        }
    if kind == "trending":
        return "/api/trending_scan", {
            "timeframe": "1h", "mode": "movers", "limit": 30,
            "exchange": ex, "market": mkt,
        }
    return None, None


def chosen_pairs_across_users() -> list:
    """Every pair any enabled user has explicitly asked to watch.

    A user who narrows the scope to their own pairs would otherwise depend on
    the market sweep happening to reach those symbols, which with a few dozen
    pairs per cycle out of hundreds could take hours. Scanning them directly
    each cycle guarantees the coins they actually care about stay covered.
    """
    from models import LiveMonitorSignalSettings as _S
    from live_monitor.signal_settings import resolve_scope_symbols

    out: list = []
    try:
        with _m.app.app_context():
            rows = _S.query.filter(_S.enabled.is_(True)).all()
            for r in rows:
                if (r.coin_scope or "") not in ("watchlist", "selected_pairs"):
                    continue
                for sym in (resolve_scope_symbols(r) or []):
                    if sym not in out:
                        out.append(sym)
    except Exception as exc:
        print(f"[SIG-8] chosen-pair lookup failed: {str(exc)[:100]}")
    # Bounded so one user cannot make every cycle enormous.
    return out[:120]


# Scans that can be pointed at an explicit list of pairs. Trending is absent
# deliberately: it ranks the whole market against itself, so asking it about
# five specific coins is meaningless.
CROSS_CHECK_SCANS = ["scan", "compressed", "bias", "ath_atl"]


def cross_check_limit() -> int:
    try:
        return max(0, min(120, int(os.environ.get("ZYNI_SIG_CROSSCHECK_PAIRS", "40"))))
    except (TypeError, ValueError):
        return 40


def candidate_symbols_for_crosscheck(limit: int = None) -> list:
    """Pairs already flagged by at least one scan, worth a full examination.

    The market sweep visits pairs in batches, so whether two scans ever look
    at the same pair is otherwise down to timing. That makes confluence
    accidental — a pair found by bias shift might never be checked for an
    order block at all. These are the pairs to look at from every angle.
    """
    from models import LiveMonitorSignalCandidate as _C
    from sqlalchemy.orm import load_only as _load_only

    limit = cross_check_limit() if limit is None else limit
    if limit <= 0:
        return []

    out: list = []
    try:
        with _m.app.app_context():
            rows = (_C.query
                    .options(_load_only(_C.symbol, _C.score, _C.detected_at))
                    .filter(_C.status == "active")
                    .order_by(_C.detected_at.desc(), _C.score.desc())
                    .limit(limit * 4).all())
            for r in rows:
                if r.symbol and r.symbol not in out:
                    out.append(r.symbol)
                if len(out) >= limit:
                    break
    except Exception as exc:
        print(f"[SIG-10] cross-check symbol lookup failed: {str(exc)[:100]}")
    return out


def cross_check_candidates(identity: dict = None) -> dict:
    """Run every targetable scan against the pairs already flagged by any scan.

    This is what turns confluence from luck into a real measurement: a pair
    that bias shift found now also gets examined for order blocks, fair value
    gaps, compression and extremes, so the AI judges one pair on everything
    the app knows how to look for rather than on whichever scan happened to
    reach it first.
    """
    symbols = candidate_symbols_for_crosscheck()
    if not symbols:
        return {"ok": True, "skipped": "no_candidates"}

    identity = identity or _scan_runner_identity()
    if not identity:
        return {"ok": True, "skipped": "no_enabled_user"}

    out = {"ok": True, "symbols": len(symbols), "scans": {}}
    for kind in CROSS_CHECK_SCANS:
        try:
            res = run_one_scan(kind, identity=identity, symbols=symbols)
            out["scans"][kind] = bool(res.get("ok"))
        except Exception as exc:
            out["scans"][kind] = False
            print(f"[SIG-10] cross-check {kind} failed: {str(exc)[:100]}")
    return out


def _scan_runner_identity():
    """Which account the background scans run as.

    Prefers an admin, because the scan endpoints skip the daily token charge
    for admins — a scan the user never asked for should not spend their quota.
    Falls back to any user with the signal engine on.
    """
    from models import User, LiveMonitorSignalSettings as _S

    try:
        # This runs from a background thread, which carries no app context of
        # its own. Flask allows nesting, so pushing one here is safe whether
        # the caller already had one or not.
        with _m.app.app_context():
            enabled_ids = [r.user_id for r in
                           _S.query.filter(_S.enabled.is_(True)).all()]
            if not enabled_ids:
                return None

            # is_admin is a Python property over `role`, not a column, so the
            # admin preference is expressed against role directly.
            admin = (User.query
                     .filter(User.id.in_(enabled_ids), User.role == "admin")
                     .order_by(User.id.asc()).first())
            if admin:
                return {"id": admin.id, "username": admin.username, "admin": True}

            first = (User.query.filter(User.id.in_(enabled_ids))
                     .order_by(User.id.asc()).first())
            if first:
                return {"id": first.id, "username": first.username,
                        "admin": bool(getattr(first, "is_admin", False))}
    except Exception as exc:
        print(f"[SIG-8] identity lookup failed: {str(exc)[:100]}")
    return None


def run_one_scan(kind: str, identity: dict = None, symbols: list = None) -> dict:
    """Run one scan type as if a browser had pressed its button.

    `symbols` targets specific pairs instead of the market sweep — used to
    guarantee coverage of pairs a user explicitly chose.

    Returns {ok, kind, status, elapsed_sec}. Never raises — a failing scan
    must not take the loop down with it.
    """
    identity = identity or _scan_runner_identity()
    if not identity:
        return {"ok": False, "kind": kind, "reason": "no_enabled_user"}

    url, body = _payload_for(kind)
    if url is None:
        return {"ok": False, "kind": kind, "reason": "unknown_scan_type"}

    if symbols:
        syms = list(symbols)
        if kind in ("scan", "bias"):
            body = dict(body, scanMode="selected", symbols=syms,
                        roundRobin=False, pairsPerCycle=max(len(syms), 5))
        elif kind in ("compressed", "ath_atl"):
            # These take a plain symbol list rather than a scan mode.
            body = dict(body, symbols=syms)
        else:
            # Trending ranks the whole market against itself, so a targeted
            # request would be meaningless — leave it market-wide.
            pass

    started = time.time()
    try:
        client = _m.app.test_client()
        with client.session_transaction() as sess:
            sess["logged_in"] = True
            sess["username"]  = identity["username"]
        resp = client.post(url, json=body)
        elapsed = round(time.time() - started, 1)

        ok = resp.status_code == 200
        out = {"ok": ok, "kind": kind, "status": resp.status_code,
               "elapsed_sec": elapsed, "as_user": identity["username"]}
        if not ok:
            try:
                out["error"] = str(resp.get_json())[:200]
            except Exception:
                out["error"] = f"http_{resp.status_code}"
        return out
    except Exception as exc:
        return {"ok": False, "kind": kind, "reason": f"crashed:{str(exc)[:120]}",
                "elapsed_sec": round(time.time() - started, 1)}


def run_scan_cycle(force_kind: str = None) -> dict:
    """One cycle: run the next scan type in the rotation."""
    identity = _scan_runner_identity()
    if not identity:
        # Nobody has the engine on, so there is nothing to scan for.
        return {"ok": True, "skipped": "no_enabled_user"}

    # The Selected Pairs universe is rebuilt from current movers, so it must
    # be refreshed before the cycle reads it — otherwise a browser-closed
    # server keeps scanning whatever list the tab last happened to push.
    try:
        from live_monitor.selected_universe import refresh_universes_for_enabled_users
        universe = refresh_universes_for_enabled_users(scan_exchange(), scan_market())
    except Exception as exc:
        universe = {"ok": False, "reason": str(exc)[:100]}
        print(f"[SIG-9] universe refresh error: {str(exc)[:120]}")

    with _state_lock:
        kind = force_kind or SCAN_SEQUENCE[_state["index"] % len(SCAN_SEQUENCE)]
        if not force_kind:
            _state["index"] = (_state["index"] + 1) % len(SCAN_SEQUENCE)

    result = run_one_scan(kind, identity=identity)
    if universe and universe.get("users"):
        result["universe_refreshed_for"] = universe["users"]

    # Every pair any scan has flagged gets examined by all the other scans, so
    # a pair is judged on everything the app can see rather than on whichever
    # scan happened to reach it first.
    try:
        cross = cross_check_candidates(identity)
        if cross.get("symbols"):
            result["cross_checked_pairs"] = cross["symbols"]
            result["cross_check_scans"] = cross.get("scans")
    except Exception as exc:
        print(f"[SIG-10] cross-check error: {str(exc)[:120]}")

    # The market sweep moves through the exchange a batch at a time, so pairs
    # a user pinned might not come round for hours. Cover them directly.
    if kind == "scan":
        chosen = chosen_pairs_across_users()
        if chosen:
            result["chosen_pairs_scanned"] = len(chosen)
            targeted = run_one_scan("scan", identity=identity, symbols=chosen)
            result["chosen_scan_ok"] = bool(targeted.get("ok"))

    with _state_lock:
        _state["cycles"]     += 1
        _state["last_run"]    = datetime.now(timezone.utc).isoformat()
        _state["last_result"] = result
        _state["last_error"]  = None if result.get("ok") else result

    return result


def run_all_scans_once() -> dict:
    """Run every scan type back to back. For a manual 'scan now'.

    Heavier than a normal cycle by design — this is what someone presses when
    they want results immediately rather than waiting for the rotation.
    """
    out = {}
    for kind in SCAN_SEQUENCE:
        out[kind] = run_one_scan(kind)
    return {"ok": True, "scans": out}


def scanner_state() -> dict:
    pace = _scanner_pace()
    with _state_lock:
        return dict(_state, sequence=list(SCAN_SEQUENCE),
                    cycle_sec=pace["interval"], pairs=pace["pairs"],
                    pace_source=pace["source"])


def scanner_loop():
    """Background loop. Sleeps first so startup is not a thundering herd."""
    print(f"[SIG-8] background scanner started cycle={cycle_seconds()}s "
          f"pairs={pairs_per_cycle()} sequence={'>'.join(SCAN_SEQUENCE)}")

    with _state_lock:
        _state["running"] = True

    # A short initial delay lets the app finish booting (WS threads, caches)
    # before the first scan competes with it for the exchange rate limit.
    time.sleep(45)

    while True:
        started = time.time()
        try:
            result = run_scan_cycle()
            if result.get("skipped"):
                # Nothing to do — check again next cycle without noise.
                pass
            elif result.get("ok"):
                print(f"[SIG-8] scanned {result['kind']} "
                      f"in {result.get('elapsed_sec')}s")
            else:
                print(f"[SIG-8] scan {result.get('kind')} failed: "
                      f"{result.get('error') or result.get('reason')}")
        except Exception as exc:
            with _state_lock:
                _state["last_error"] = str(exc)[:200]
            print(f"[SIG-8] scan cycle error: {exc}")

        # Re-read each cycle so a settings change takes effect on the next
        # pass rather than requiring a restart.
        cycle = cycle_seconds()
        time.sleep(max(30.0, cycle - (time.time() - started)))
