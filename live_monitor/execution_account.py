"""Phase 11.11: Execution Account Architecture — mode storage and guardrails.

Architecture-only. No execution. No order placement. No exchange calls.
No auto trading. No live connector. No API keys. No secrets.

Allowed execution modes:
  internal_paper      — Primary Strategy Testing (DEFAULT)
  binance_testnet     — API Testing Only
  binance_live_future — Future / Disabled (always rejected)

AI may read execution mode.
AI may explain execution mode.
AI must NOT switch mode.
AI must NOT execute.
"""
from __future__ import annotations

# ── Mode registry ─────────────────────────────────────────────────────────────
# (mode_key): (display_label, purpose_string)
_VALID_EXECUTION_MODES: dict[str, tuple[str, str]] = {
    "internal_paper":      ("Internal Paper",  "Primary Strategy Testing"),
    "binance_testnet":     ("Binance Testnet", "API Testing Only"),
    "binance_live_future": ("Binance Live",    "Future / Disabled"),
}

_VALID_POLICY_MODES = {
    "proposal_only",
    "paper_manual",
    "paper_auto_future",
    "testnet_manual",
    "live_disabled",
}

_DEFAULT_EXECUTION_MODE = "internal_paper"
_DEFAULT_POLICY_MODE    = "paper_manual"

# Mode that is always hard-blocked regardless of what is stored in DB
_BLOCKED_MODES = {"binance_live_future"}

# Derived policy_mode per execution_mode (used when auto-deriving on update)
_MODE_TO_POLICY = {
    "internal_paper":      "paper_manual",
    "binance_testnet":     "testnet_manual",
    "binance_live_future": "live_disabled",
}


# ── Validators ────────────────────────────────────────────────────────────────

def _lm_validate_execution_mode(execution_mode) -> bool:
    """Return True if execution_mode is a known valid mode string."""
    return execution_mode in _VALID_EXECUTION_MODES


def _lm_validate_policy_mode(policy_mode) -> bool:
    """Return True if policy_mode is a known valid policy string."""
    return policy_mode in _VALID_POLICY_MODES


def _lm_execution_mode_labels() -> dict:
    """Return all mode labels and purposes for display."""
    return {
        mode: {"label": label, "purpose": purpose}
        for mode, (label, purpose) in _VALID_EXECUTION_MODES.items()
    }


# ── Core helpers ──────────────────────────────────────────────────────────────

def _lm_get_execution_settings(user_id):
    """Get (or auto-create) UserPreference row and return execution_mode + policy_mode.

    Default execution_mode = internal_paper.
    Default policy_mode    = paper_manual.
    No credentials. No API keys. No secrets.
    """
    from models import db as _db, UserPreference as _UP
    try:
        pref = _UP.query.filter_by(user_id=user_id).first()
        if pref is None:
            pref = _UP(
                user_id        = user_id,
                execution_mode = _DEFAULT_EXECUTION_MODE,
                policy_mode    = _DEFAULT_POLICY_MODE,
            )
            _db.session.add(pref)
            _db.session.commit()

        raw_mode = getattr(pref, "execution_mode", None) or _DEFAULT_EXECUTION_MODE
        raw_pol  = getattr(pref, "policy_mode",    None) or _DEFAULT_POLICY_MODE

        # Sanitise — hard-block live, reject unknown values
        mode = raw_mode if raw_mode in _VALID_EXECUTION_MODES else _DEFAULT_EXECUTION_MODE
        if mode in _BLOCKED_MODES:
            mode = _DEFAULT_EXECUTION_MODE
        pol  = raw_pol if raw_pol in _VALID_POLICY_MODES else _DEFAULT_POLICY_MODE

        return {
            "ok":             True,
            "execution_mode": mode,
            "policy_mode":    pol,
            "source":         "execution_account",
        }
    except Exception as _e:
        return {
            "ok":             False,
            "error":          str(_e)[:200],
            "execution_mode": _DEFAULT_EXECUTION_MODE,
            "policy_mode":    _DEFAULT_POLICY_MODE,
            "source":         "execution_account",
        }


def _lm_update_execution_mode(user_id, execution_mode):
    """Update execution_mode for a user.

    Hard-rejects binance_live_future — live mode is always disabled.
    Rejects unknown modes.
    Derives policy_mode automatically.
    No exchange calls. No API keys. No secrets. No order placement.
    """
    import datetime as _dt_ea

    # Hard reject: live mode disabled
    if execution_mode == "binance_live_future":
        return {
            "ok":             False,
            "error":          "live_mode_disabled",
            "message":        "Binance Live Futures is disabled for a future phase.",
            "execution_mode": _DEFAULT_EXECUTION_MODE,
            "source":         "execution_account",
        }

    # Reject unknown
    if execution_mode not in _VALID_EXECUTION_MODES:
        return {
            "ok":             False,
            "error":          "invalid_execution_mode",
            "message":        f"Unknown execution_mode: {execution_mode!r}",
            "execution_mode": _DEFAULT_EXECUTION_MODE,
            "source":         "execution_account",
        }

    from models import db as _db, UserPreference as _UP
    try:
        pref = _UP.query.filter_by(user_id=user_id).first()
        if pref is None:
            pref = _UP(user_id=user_id)
            _db.session.add(pref)

        pol = _MODE_TO_POLICY.get(execution_mode, _DEFAULT_POLICY_MODE)
        pref.execution_mode = execution_mode
        pref.policy_mode    = pol
        pref.updated_at     = _dt_ea.datetime.utcnow()
        _db.session.commit()

        return {
            "ok":             True,
            "execution_mode": execution_mode,
            "policy_mode":    pol,
            "source":         "execution_account",
        }
    except Exception as _e:
        try:
            _db.session.rollback()
        except Exception:
            pass
        return {
            "ok":             False,
            "error":          str(_e)[:200],
            "execution_mode": _DEFAULT_EXECUTION_MODE,
            "source":         "execution_account",
        }


def _lm_get_execution_mode_summary(user_id):
    """Return structured execution mode summary for one user.

    Always returns ok=True with safe defaults even on DB error.
    live_enabled is always False.
    testnet_strategy_validation is always False.
    ai_can_execute is always False.
    No exchange calls. No API keys. Read-only.
    """
    settings = _lm_get_execution_settings(user_id)
    mode = settings.get("execution_mode", _DEFAULT_EXECUTION_MODE)
    pol  = settings.get("policy_mode",    _DEFAULT_POLICY_MODE)

    label, purpose = _VALID_EXECUTION_MODES.get(
        mode, ("Internal Paper", "Primary Strategy Testing")
    )

    return {
        "ok":                          True,
        "execution_mode":              mode,
        "execution_mode_label":        label,
        "execution_mode_purpose":      purpose,
        "policy_mode":                 pol,
        "paper_primary":               mode == "internal_paper",
        "live_enabled":                False,
        "testnet_strategy_validation": False,
        "live_disabled":               True,
        "auto_execution_allowed":      False,
        "ai_can_execute":              False,
        "source":                      "execution_account",
    }
