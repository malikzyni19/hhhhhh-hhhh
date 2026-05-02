"""
auto_resolver_runner.py — Phase 6B dry-run auto-resolver background thread.

PHASE 6B CONSTRAINT: The runner ALWAYS forces dry_run=True.
Even if IntelligenceSettings.auto_resolver_mode == "commit", no SignalEvent
or SignalOutcome rows are updated by this runner. Only
IntelligenceSettings.last_run_at, last_run_summary, and runner_installed
are written.

Phase 6C will replace the hard-coded dry_run=True with the real mode.
"""

import json
import os
import threading
import time
from datetime import datetime, timezone

# ── Module-level duplicate-run guards ────────────────────────────────────────
_runner_started = False
_runner_lock    = threading.Lock()   # guards _runner_started flag
_run_lock       = threading.Lock()   # prevents overlapping cycles in-process

_ADVISORY_LOCK_KEY  = 7_654_321      # pg_try_advisory_xact_lock constant
_MIN_INTERVAL_SECS  = 300            # 5-minute hard floor regardless of settings
_STARTUP_DELAY_SECS = 60             # sleep before first cycle on startup


# ─────────────────────────────────────────────────────────────────────────────
# Public entry-point — call once from main.py after app is ready
# ─────────────────────────────────────────────────────────────────────────────

def start_auto_resolver_runner(app):
    """
    Start the background dry-run resolver thread exactly once per process.

    Safe to call on every app import:
      - Module-level flag prevents duplicate threads within one process.
      - Skips Werkzeug reloader parent process in debug mode so only the
        child worker starts the thread.
    """
    global _runner_started

    # In Flask debug mode the Werkzeug reloader forks: the monitor (parent)
    # process also imports main.py. Only the child has WERKZEUG_RUN_MAIN=true.
    # In production (gunicorn) app.debug is False so this branch is skipped.
    if app.debug and os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        return

    with _runner_lock:
        if _runner_started:
            return
        _runner_started = True

    t = threading.Thread(
        target=_auto_resolver_loop,
        args=(app,),
        daemon=True,
        name="AutoResolver",
    )
    t.start()


# ─────────────────────────────────────────────────────────────────────────────
# Background loop
# ─────────────────────────────────────────────────────────────────────────────

def _auto_resolver_loop(app):
    print("[AutoResolver] runner installed dry-run only")
    time.sleep(_STARTUP_DELAY_SECS)

    while True:
        sleep_secs = _MIN_INTERVAL_SECS
        try:
            with app.app_context():
                snap = _settings_snapshot()
                if snap is None or not snap["enabled"]:
                    print("[AutoResolver] disabled")
                else:
                    sleep_secs = max(_MIN_INTERVAL_SECS, snap["interval_minutes"] * 60)
                    _run_auto_resolver_once(snap)
        except Exception as _loop_err:
            print(f"[AutoResolver] error: {_loop_err}")
        time.sleep(sleep_secs)


# ─────────────────────────────────────────────────────────────────────────────
# Settings snapshot — read inside active app context
# ─────────────────────────────────────────────────────────────────────────────

def _settings_snapshot():
    """Return settings dict from IntelligenceSettings id=1, or None on failure."""
    try:
        from models import db, IntelligenceSettings
        row = db.session.get(IntelligenceSettings, 1)
        if row is None:
            return None
        return {
            "enabled":          row.auto_resolver_enabled,
            "interval_minutes": max(15, row.auto_resolver_interval_minutes),
            "limit":            max(1, min(100, row.auto_resolver_limit)),
            "mode":             row.auto_resolver_mode,
        }
    except Exception as _e:
        print(f"[AutoResolver] error reading settings: {_e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Single resolver cycle — called inside active app context
# ─────────────────────────────────────────────────────────────────────────────

def _run_auto_resolver_once(snap):
    """
    Execute one dry-run resolver cycle.

    Writes ONLY: IntelligenceSettings.runner_installed, last_run_at,
    last_run_summary.  Does NOT touch SignalEvent or SignalOutcome tables.
    """
    # In-process overlap guard (non-blocking acquire)
    if not _run_lock.acquire(blocking=False):
        print("[AutoResolver] skipped overlapping run")
        return

    try:
        from models import db, IntelligenceSettings
        from sqlalchemy import text
        from outcome_resolver import resolve_pending_admin

        # PostgreSQL advisory lock: prevents overlapping runs across multiple
        # Koyeb workers/instances. Transaction-level lock — auto-released on
        # commit or rollback.
        try:
            locked = db.session.execute(
                text("SELECT pg_try_advisory_xact_lock(:k)"),
                {"k": _ADVISORY_LOCK_KEY},
            ).scalar()
            if not locked:
                print("[AutoResolver] skipped overlapping run")
                db.session.rollback()
                return
        except Exception as _lock_err:
            # Advisory lock unavailable (non-PG DB in test, or permission issue)
            print(f"[AutoResolver] advisory lock unavailable ({_lock_err}), proceeding without it")

        limit = snap["limit"]
        print(f"[AutoResolver] dry run started limit={limit}")

        # Phase 6B: ALWAYS force dry_run=True — never commit automatically
        result = resolve_pending_admin(limit=limit, dry_run=True)

        commit_forced_off = (snap["mode"] == "commit")
        summary = {
            "mode":               "dry_run",
            "phase":              "6B",
            "checked":            result.get("checked",       0),
            "won":                result.get("won",           0),
            "lost":               result.get("lost",          0),
            "expired":            result.get("expired",       0),
            "ambiguous":          result.get("ambiguous",     0),
            "entered":            result.get("entered",       0),
            # no_resolution from outcome_resolver = still WAITING_FOR_ENTRY, not yet expired
            "waiting_for_entry":  result.get("no_resolution", 0),
            "errors":             result.get("errors",        0),
            "commit_forced_off":  commit_forced_off,
        }
        if commit_forced_off:
            summary["note"] = (
                "Phase 6B runner is dry-run only. "
                "Commit mode ignored until Phase 6C."
            )

        print(
            f"[AutoResolver] dry run finished"
            f" checked={summary['checked']}"
            f" errors={summary['errors']}"
        )

        # Write only runner metadata — no SignalEvent / SignalOutcome writes
        row = db.session.get(IntelligenceSettings, 1)
        if row:
            row.runner_installed = True
            row.last_run_at      = datetime.now(timezone.utc)
            row.last_run_summary = json.dumps(summary)
            db.session.commit()   # also releases the advisory xact lock
        else:
            db.session.rollback()

    except Exception as _e:
        print(f"[AutoResolver] error: {_e}")
        try:
            from models import db as _db
            _db.session.rollback()
        except Exception:
            pass
    finally:
        _run_lock.release()
