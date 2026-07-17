"""
security.py — Bot-protection utilities for ZyNi SMC Screener.

Provides:
  - IPRateLimiter  : thread-safe sliding-window rate limiter
  - verify_turnstile: Cloudflare Turnstile server-side validation
  - is_disposable_email: disposable/temp email domain check
  - log_security_event : structured security logging
"""

import os
import time
import threading
import logging
from collections import defaultdict, deque
from typing import Tuple

import requests as _http

# ── Structured Security Logger ─────────────────────────────────────────────────
_sec_logger = logging.getLogger("zyni.security")
if not _sec_logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(
        logging.Formatter("[SECURITY] %(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    )
    _sec_logger.addHandler(_h)
    _sec_logger.setLevel(logging.INFO)


def log_security_event(event: str, ip: str = "", username: str = "", detail: str = "") -> None:
    _sec_logger.info(f"{event} | ip={ip} | user={username} | {detail}")


# ── In-Memory Sliding-Window Rate Limiter ──────────────────────────────────────
class IPRateLimiter:
    """Thread-safe sliding-window rate limiter keyed by (ip, endpoint)."""

    def __init__(self) -> None:
        self._lock: threading.Lock = threading.Lock()
        self._hits: dict = defaultdict(deque)

    def is_allowed(self, ip: str, endpoint: str, max_hits: int, window_seconds: int) -> bool:
        key = (ip, endpoint)
        now = time.monotonic()
        cutoff = now - window_seconds
        with self._lock:
            dq = self._hits[key]
            while dq and dq[0] < cutoff:
                dq.popleft()
            if len(dq) >= max_hits:
                return False
            dq.append(now)
            return True

    def get_retry_after(self, ip: str, endpoint: str, window_seconds: int) -> int:
        key = (ip, endpoint)
        with self._lock:
            dq = self._hits.get(key)
            if not dq:
                return 0
            wait = int(dq[0] + window_seconds - time.monotonic())
            return max(wait, 1)

    def reset(self, ip: str, endpoint: str) -> None:
        key = (ip, endpoint)
        with self._lock:
            self._hits.pop(key, None)


# Singleton instance used across the application
rate_limiter = IPRateLimiter()

# ── Rate Limit Configs ─────────────────────────────────────────────────────────
# Each value is (max_hits, window_seconds)
RATE_LIMITS: dict = {
    "register":            (5,  3600),   # 5 per IP per hour
    "login":               (10, 900),    # 10 per IP per 15 minutes
    "resend_verification": (3,  900),    # 3 per IP per 15 minutes
    "verify_email":        (10, 900),    # 10 per IP per 15 minutes
}


# ── Cloudflare Turnstile ───────────────────────────────────────────────────────
TURNSTILE_SECRET  = os.environ.get("TURNSTILE_SECRET_KEY", "")
TURNSTILE_SITE    = os.environ.get("TURNSTILE_SITE_KEY", "")
TURNSTILE_ENABLED = bool(TURNSTILE_SECRET and TURNSTILE_SITE)

_TURNSTILE_URL = "https://challenges.cloudflare.com/turnstile/v0/siteverify"


def verify_turnstile(token: str, remote_ip: str) -> Tuple[bool, str]:
    """
    Validate a Cloudflare Turnstile challenge token.
    Fails OPEN — if keys are not set, the check is skipped (register still works).
    """
    if not TURNSTILE_ENABLED:
        return True, ""
    if not token:
        return False, "Human verification required. Please complete the security check."
    try:
        resp = _http.post(
            _TURNSTILE_URL,
            data={"secret": TURNSTILE_SECRET, "response": token, "remoteip": remote_ip},
            timeout=5,
        )
        result = resp.json()
        if result.get("success"):
            return True, ""
        codes = result.get("error-codes", [])
        log_security_event("TURNSTILE_FAIL", ip=remote_ip, detail=str(codes))
        return False, "Security check failed. Please refresh and try again."
    except Exception as exc:
        # Fail open: don't block legitimate users if Turnstile is unreachable
        log_security_event("TURNSTILE_ERROR", ip=remote_ip, detail=str(exc))
        return True, ""


# ── Google reCAPTCHA v2 ────────────────────────────────────────────────────────
# FAILS CLOSED: if either key is missing, login is BLOCKED.
RECAPTCHA_SITE_KEY   = os.environ.get("RECAPTCHA_SITE_KEY", "")
RECAPTCHA_SECRET_KEY = os.environ.get("RECAPTCHA_SECRET_KEY", "")
RECAPTCHA_ENABLED    = bool(RECAPTCHA_SITE_KEY and RECAPTCHA_SECRET_KEY)

_RECAPTCHA_VERIFY_URL = "https://www.google.com/recaptcha/api/siteverify"


def verify_recaptcha(token: str, remote_ip: str) -> Tuple[bool, str]:
    """
    Validate a Google reCAPTCHA v2 token server-side.

    FAILS CLOSED:
      - If RECAPTCHA_SITE_KEY / RECAPTCHA_SECRET_KEY are not configured → blocked.
      - If Google's API is unreachable → blocked (no fail-open).
      - The Secret Key is never sent to the frontend.
    """
    if not RECAPTCHA_ENABLED:
        log_security_event("RECAPTCHA_NOT_CONFIGURED", ip=remote_ip)
        return False, "Login is temporarily unavailable. Please contact the administrator."
    if not token:
        return False, "Please complete the reCAPTCHA verification before signing in."
    try:
        resp = _http.post(
            _RECAPTCHA_VERIFY_URL,
            data={
                "secret":   RECAPTCHA_SECRET_KEY,
                "response": token,
                "remoteip": remote_ip,
            },
            timeout=6,
        )
        result = resp.json()
        if result.get("success"):
            return True, ""
        codes = result.get("error-codes", [])
        log_security_event("RECAPTCHA_FAIL", ip=remote_ip, detail=str(codes))
        return False, "reCAPTCHA verification failed. Please try again."
    except Exception as exc:
        # Fail CLOSED — do not allow login if Google is unreachable
        log_security_event("RECAPTCHA_API_ERROR", ip=remote_ip, detail=str(exc))
        return False, "Verification service unavailable. Please try again in a moment."


# ── Disposable / Temp Email Domains ───────────────────────────────────────────
DISPOSABLE_EMAIL_DOMAINS: set = {
    "mailinator.com", "guerrillamail.com", "guerrillamail.info",
    "guerrillamail.biz", "guerrillamail.de", "guerrillamail.net",
    "guerrillamail.org", "guerrillamailblock.com", "grr.la",
    "sharklasers.com", "spam4.me", "yopmail.com",
    "temp-mail.org", "temp-mail.io", "tempr.email",
    "10minutemail.com", "10minutemail.net",
    "trashmail.com", "trashmail.at", "trashmail.io",
    "trashmail.me", "trashmail.net",
    "dispostable.com", "discard.email",
    "mailnull.com", "maildrop.cc",
    "spamgourmet.com", "spamgourmet.net", "spamgourmet.org",
    "spamfree24.org", "spamevader.com",
    "fakeinbox.com", "filzmail.com",
    "getairmail.com", "airmail.cc",
    "getnada.com", "cock.li",
    "mt2014.com", "mt2015.com", "mt2016.com", "mt2017.com",
    "mailexpire.com", "mailnesia.com",
    "emailondeck.com", "tempinbox.com",
    "mytemp.email", "inboxkitten.com",
    "throam.com", "throwam.com",
    "jetable.fr.nf", "cool.fr.nf", "elude.in",
    "crazymailing.com",
}


def is_disposable_email(email: str) -> bool:
    """Return True if the email domain is a known disposable/temp provider."""
    if "@" not in email:
        return False
    domain = email.split("@")[-1].lower().strip()
    return domain in DISPOSABLE_EMAIL_DOMAINS
