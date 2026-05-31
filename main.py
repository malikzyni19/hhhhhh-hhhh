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
import hmac
import secrets
import base64
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps
from typing import Any, Dict, List, Optional, Tuple
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timezone, timedelta

import numpy as np
import requests as req
from flask import Flask, jsonify, make_response, redirect, render_template, request, session, url_for

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "zyni-fallback-secret")
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL', '')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

@app.after_request
def no_cache(r):
    path = request.path
    # Allow proper caching for PWA static assets — do not override
    if (path.startswith('/static/icons/') or path.startswith('/static/images/')
            or path in ('/service-worker.js', '/manifest.json', '/offline')):
        return r
    r.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    r.headers["Pragma"] = "no-cache"
    r.headers["Expires"] = "0"
    return r

from flask_login import LoginManager
from models import (db, User as _DBUser, GlobalSetting as _GlobalSetting,
                    LoginHistory as _LoginHistory,
                    EmailVerification as _EmailVerification,
                    PasswordResetToken as _PasswordResetToken)
from admin import admin_bp
from permissions import get_user_permissions, consume_tokens, check_tokens

db.init_app(app)
_login_manager = LoginManager()
_login_manager.init_app(app)
_login_manager.login_view = "admin.login"

@_login_manager.user_loader
def _load_user(user_id):
    return _DBUser.query.get(int(user_id))

app.register_blueprint(admin_bp)

@app.template_filter("time_ago")
def _time_ago(dt):
    if not dt:
        return "—"
    try:
        diff = int((datetime.now(timezone.utc) - dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else datetime.now(timezone.utc) - dt).total_seconds())
        if diff < 60: return "just now"
        if diff < 3600: return f"{diff//60}m ago"
        if diff < 86400: return f"{diff//3600}h ago"
        return f"{diff//86400}d ago"
    except Exception:
        return "—"

try:
    with app.app_context():
        db.create_all()
except Exception as _db_err:
    print(f"[DB] Could not create tables: {_db_err}")

# ── Intelligence schema migration — idempotent, safe on every deploy ──────────
# db.create_all() creates new tables but does NOT alter existing PostgreSQL
# tables. This patch adds any columns/indexes that were added after the initial
# deploy. All statements use IF NOT EXISTS — safe to run repeatedly.
try:
    from sqlalchemy import text as _sa_text
    with app.app_context():
        try:
            _migration_stmts = [
                # New columns on signal_events
                "ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS setup_type VARCHAR(30)",
                "ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS raw_setup VARCHAR(50)",
                "ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS raw_meta_json TEXT",
                # Indexes — non-unique
                "CREATE INDEX IF NOT EXISTS ix_signal_events_pair        ON signal_events (pair)",
                "CREATE INDEX IF NOT EXISTS ix_signal_events_module      ON signal_events (module)",
                "CREATE INDEX IF NOT EXISTS ix_signal_events_timeframe   ON signal_events (timeframe)",
                "CREATE INDEX IF NOT EXISTS ix_signal_events_status      ON signal_events (status)",
                "CREATE INDEX IF NOT EXISTS ix_signal_events_detected_at ON signal_events (detected_at)",
                # Unique index for signal_id (already enforced by unique=True on column,
                # but explicit name makes it idempotent and inspectable)
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_signal_events_signal_id_unique ON signal_events (signal_id)",
            ]
            for _stmt in _migration_stmts:
                db.session.execute(_sa_text(_stmt))
            db.session.commit()

            # Verify the three new columns are present
            _verify_rows = db.session.execute(_sa_text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'signal_events' "
                "  AND column_name IN ('setup_type', 'raw_setup', 'raw_meta_json')"
            )).fetchall()
            _found_cols  = {r[0] for r in _verify_rows}
            _missing_cols = {"setup_type", "raw_setup", "raw_meta_json"} - _found_cols
            if _missing_cols:
                print(f"[Intelligence Migration] WARNING — columns still missing after patch: {_missing_cols}")
            else:
                print("[Intelligence Migration] schema patch applied — all columns verified OK")

        except Exception as _migration_inner_err:
            db.session.rollback()
            print(f"[Intelligence Migration] skipped/error: {_migration_inner_err}")
except Exception as _migration_outer_err:
    print(f"[Intelligence Migration] skipped/error: {_migration_outer_err}")

# ── Auto Resolver background runner (Phase 6B — dry-run only) ────────────────
try:
    from auto_resolver_runner import start_auto_resolver_runner
    start_auto_resolver_runner(app)
except Exception as _ar_start_err:
    print(f"[AutoResolver] failed to start: {_ar_start_err}")

# ── IP geo cache ────────────────────────────────────────────────────
_ip_geo: dict = {}   # {ip: (ts, {country, city})}

def _geo_lookup(ip: str) -> dict:
    """Return {country, city} for an IP. Cached 24h."""
    if not ip or ip in ("unknown", "127.0.0.1") or ip.startswith("192.168") or ip.startswith("10."):
        return {"country": "", "city": ""}
    cached_ts, cached_geo = _ip_geo.get(ip, (0, None))
    if cached_geo and (time.time() - cached_ts) < 86400:
        return cached_geo
    try:
        r = req.get(f"http://ip-api.com/json/{ip}?fields=country,city", timeout=4)
        if r.status_code == 200:
            d = r.json()
            geo = {"country": d.get("country", ""), "city": d.get("city", "")}
        else:
            geo = {"country": "", "city": ""}
    except Exception:
        geo = {"country": "", "city": ""}
    _ip_geo[ip] = (time.time(), geo)
    return geo

def _parse_ua(ua: str) -> dict:
    """Parse User-Agent string into device/browser/os."""
    ua_l = (ua or "").lower()
    if "mobile" in ua_l or "android" in ua_l or "iphone" in ua_l:
        device = "mobile"
    elif "tablet" in ua_l or "ipad" in ua_l:
        device = "tablet"
    else:
        device = "desktop"

    if "edg" in ua_l:      browser = "Edge"
    elif "chrome" in ua_l: browser = "Chrome"
    elif "firefox" in ua_l:browser = "Firefox"
    elif "safari" in ua_l: browser = "Safari"
    else:                  browser = "Unknown"

    if "android" in ua_l:    os_ = "Android"
    elif "iphone" in ua_l or "ipad" in ua_l: os_ = "iOS"
    elif "windows" in ua_l:  os_ = "Windows"
    elif "mac" in ua_l:      os_ = "macOS"
    elif "linux" in ua_l:    os_ = "Linux"
    else:                    os_ = "Unknown"

    return {"device_type": device, "browser": browser, "os": os_}

# ── Maintenance mode ────────────────────────────────────────────────
_SKIP_MAINTENANCE_PATHS = ("/admin", "/static", "/api/", "/login",
                           "/logout", "/guest", "/favicon")

@app.before_request
def _maintenance_check():
    path = request.path
    if any(path.startswith(p) for p in _SKIP_MAINTENANCE_PATHS):
        return None
    if session.get("is_admin"):
        return None
    try:
        s = _GlobalSetting.query.filter_by(key="maintenance_mode").first()
        if s and s.value == "true":
            msg_s = _GlobalSetting.query.filter_by(key="maintenance_message").first()
            msg   = msg_s.value if msg_s else "We're upgrading. Back soon!"
            return render_template("maintenance.html", message=msg)
    except Exception:
        pass
    return None

APP_PASSWORD = os.environ.get("APP_PASSWORD", "Ulta8900")

# ============================================================
# USERNAME SYSTEM — per-user passwords
# USERS env var: comma-separated list of usernames
# Individual passwords: USER_<NAME>_PASS env var per user
# Falls back to APP_PASSWORD if individual pass not set
# ============================================================
def _build_users_db() -> Dict[str, str]:
    raw = os.environ.get("USERS", "zyni,abdul manan")
    db: Dict[str, str] = {}
    for u in raw.split(","):
        u = u.strip().lower()
        if u:
            key = "USER_" + u.replace(" ", "_").upper() + "_PASS"
            db[u] = os.environ.get(key, APP_PASSWORD)
    return db

_USERS_DB: Dict[str, str] = _build_users_db()
ALLOWED_USERS = _USERS_DB  # alias kept for internal references

# ── Admin credentials (separate from regular users) ──
ADMIN_USERNAME = os.environ.get("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "AdminZyNi2024!")

# ── Active session tracking {username: {ip, ua, login_time, sid, is_admin}} ──
_active_sessions: Dict[str, dict] = {}
_sessions_lock = threading.Lock()
_force_logout_users: set = set()

# ── Login audit log (in-memory, max 500 entries) ──
LOGIN_AUDIT_LOG: deque = deque(maxlen=500)

# ── Feature tab toggles (admin-controlled) ──
_tab_controls: Dict[str, bool] = {
    "scan": True, "compressed": True, "trending": True,
    "ath_atl": True, "bias": True, "live_monitor": True
}

# ── Guest access system ──
_guest_controls: Dict = {
    "enabled": True,
    "tabs": {"scan": True, "compressed": False, "trending": True, "ath_atl": False, "bias": False},
    "max_scans_per_session": 5,
    "max_pairs": 20,
    "session_label": "Guest",
}
_guest_sessions: Dict[str, dict] = {}
_guest_lock = threading.Lock()

# ── App start time for uptime tracking ──
_app_start_time = datetime.now(timezone.utc)

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
# Email env-var helpers — read at call time so Koyeb secrets
# injected after process start are always picked up.
def _email_from() -> str:
    return os.environ.get("ALERT_EMAIL_FROM", "").strip()

def _email_pass() -> str:
    # Strip spaces — Gmail App Passwords are displayed with spaces
    # (e.g. "xpbt jcgt fnds wcfx") but must be used without them.
    return os.environ.get("ALERT_EMAIL_PASS", "").replace(" ", "")

def _email_to() -> str:
    return os.environ.get("ALERT_EMAIL_TO", "").strip()

# Keep module-level aliases for legacy callers (send_email_alert)
ALERT_EMAIL_FROM = ""
ALERT_EMAIL_PASS = ""
ALERT_EMAIL_TO   = ""


def send_email_alert(subject: str, body: str) -> bool:
    """Send login-alert email to admin. Returns True on success."""
    frm  = _email_from()
    pwd  = _email_pass()
    to   = _email_to()
    if not all([frm, pwd, to]):
        return False  # Not configured — silently skip
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = frm
        msg["To"]      = to
        msg.attach(MIMEText(body, "html"))
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=10) as srv:
            srv.login(frm, pwd)
            srv.sendmail(frm, to, msg.as_string())
        return True
    except Exception as e:
        print(f"[EMAIL] Failed: {e}")
        return False


def _send_via_resend(to_email: str, subject: str, html_body: str) -> bool:
    """Send email via official Resend Python SDK.
    Only requires RESEND_API_KEY env var. Sender is always support@smcsetups.com.
    """
    api_key = os.environ.get("RESEND_API_KEY", "").strip()
    if not api_key:
        print("[VERIFY-EMAIL] RESEND_API_KEY not set — Resend skipped")
        return False
    try:
        import resend as _resend_sdk
        _resend_sdk.api_key = api_key
        resp = _resend_sdk.Emails.send({
            "from": "ZyNi SMC <support@smcsetups.com>",
            "to": [to_email],
            "subject": subject,
            "html": html_body,
        })
        email_id = getattr(resp, "id", None) or (resp.get("id") if isinstance(resp, dict) else "n/a")
        print(f"[VERIFY-EMAIL] Sent via Resend SDK to {to_email} — id={email_id}")
        return True
    except Exception as exc:
        print(f"[VERIFY-EMAIL] Resend SDK exception: {exc}")
        return False


def _build_verification_email(username: str, code: str) -> str:
    """Return the premium HTML email body for OTP verification."""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1.0">
  <meta http-equiv="X-UA-Compatible" content="IE=edge">
  <title>Verify Your Email — ZyNi SMC</title>
</head>
<body style="margin:0;padding:0;background:#060a14;font-family:'Segoe UI',Helvetica,Arial,sans-serif;-webkit-font-smoothing:antialiased;">
<table width="100%" cellpadding="0" cellspacing="0" role="presentation" style="background:#060a14;">
  <tr><td align="center" style="padding:40px 16px;">
    <table width="100%" cellpadding="0" cellspacing="0" role="presentation" style="max-width:580px;">

      <!-- HEADER / LOGO BANNER — image fills the header, black bg matches image bg exactly -->
      <tr><td style="background:#000000;border-radius:16px 16px 0 0;padding:0;text-align:center;border:1px solid rgba(249,115,22,0.25);border-bottom:3px solid #f97316;overflow:hidden;line-height:0;font-size:0;">
        <img src="https://smcsetups.com/static/images/logo-email.png"
             alt="ZyNi SMC"
             width="580"
             style="width:100%;max-width:580px;height:auto;display:block;border-radius:16px 16px 0 0;">
      </td></tr>

      <!-- HERO BANNER -->
      <tr><td style="background:linear-gradient(135deg,#0d1525 0%,#0f1e38 60%,#0a0f1e 100%);padding:36px 40px 28px;text-align:center;border-left:1px solid rgba(249,115,22,0.15);border-right:1px solid rgba(249,115,22,0.15);">
        <img src="https://smcsetups.com/static/images/avatar-email.png" alt="ZyNi SMC" width="90" height="90" style="width:90px;height:90px;border-radius:50%;display:block;margin:0 auto 20px;border:2.5px solid rgba(249,115,22,0.70);box-shadow:0 0 0 4px rgba(249,115,22,0.12),0 8px 28px rgba(0,0,0,0.55),0 0 30px rgba(249,115,22,0.18);object-fit:cover;">
        <h1 style="color:#ffffff;font-size:23px;font-weight:800;margin:0 0 14px;letter-spacing:-0.3px;">Verify Your Email Address</h1>
        <p style="color:rgba(232,240,255,0.65);font-size:15px;margin:0;line-height:1.75;">
          Hi <strong style="color:#ffffff;">{username}</strong>, welcome to ZyNi SMC!<br>
          Enter the code below to activate your account and start trading smarter.
        </p>
      </td></tr>

      <!-- OTP CODE -->
      <tr><td style="background:#0d1525;padding:32px 40px;text-align:center;border-left:1px solid rgba(249,115,22,0.15);border-right:1px solid rgba(249,115,22,0.15);">
        <p style="color:rgba(232,240,255,0.50);font-size:12px;margin:0 0 14px;letter-spacing:2px;text-transform:uppercase;font-weight:600;">Your Verification Code</p>
        <div style="background:#07101f;border:2px solid rgba(249,115,22,0.55);border-radius:14px;padding:26px 20px 22px;margin-bottom:14px;display:inline-block;width:100%;box-sizing:border-box;">
          <div style="font-size:50px;font-weight:900;letter-spacing:18px;color:#f97316;font-family:'Courier New',Courier,monospace;line-height:1;padding-left:18px;">{code}</div>
        </div>
        <p style="color:rgba(232,240,255,0.40);font-size:13px;margin:0;line-height:1.6;">
          &#8987; Valid for <strong style="color:#f97316;">10 minutes</strong> only &nbsp;&middot;&nbsp; Do not share this code with anyone
        </p>
      </td></tr>

      <!-- CTA BUTTON -->
      <tr><td style="background:#0d1525;padding:4px 40px 30px;text-align:center;border-left:1px solid rgba(249,115,22,0.15);border-right:1px solid rgba(249,115,22,0.15);">
        <a href="https://smcsetups.com" style="display:inline-block;background:linear-gradient(135deg,#f97316 0%,#ea580c 100%);color:#ffffff;text-decoration:none;font-size:15px;font-weight:700;padding:14px 38px;border-radius:10px;letter-spacing:0.3px;box-shadow:0 4px 20px rgba(249,115,22,0.35);">
          Explore Features &rarr;
        </a>
      </td></tr>

      <!-- DIVIDER -->
      <tr><td style="background:#0d1525;padding:0 40px;border-left:1px solid rgba(249,115,22,0.15);border-right:1px solid rgba(249,115,22,0.15);">
        <div style="height:1px;background:linear-gradient(90deg,transparent,rgba(249,115,22,0.35),transparent);"></div>
      </td></tr>

      <!-- PLATFORM FEATURES -->
      <tr><td style="background:#0d1525;padding:28px 40px;border-left:1px solid rgba(249,115,22,0.15);border-right:1px solid rgba(249,115,22,0.15);">
        <h2 style="color:#ffffff;font-size:17px;font-weight:700;margin:0 0 20px;text-align:center;">What You Can Do on ZyNi SMC</h2>
        <table width="100%" cellpadding="0" cellspacing="0" role="presentation">
          <tr><td style="padding:0 0 15px;">
            <table cellpadding="0" cellspacing="0" role="presentation"><tr>
              <td style="width:40px;vertical-align:top;padding-top:3px;">
                <div style="width:32px;height:32px;background:rgba(249,115,22,0.12);border:1px solid rgba(249,115,22,0.35);border-radius:8px;text-align:center;line-height:32px;font-size:16px;">&#128202;</div>
              </td>
              <td style="padding-left:12px;">
                <div style="color:#ffffff;font-size:14px;font-weight:700;margin-bottom:3px;">Smart Money Scanner</div>
                <div style="color:rgba(232,240,255,0.55);font-size:13px;line-height:1.6;">Scan hundreds of crypto pairs for institutional order blocks, fair value gaps, and breaker patterns in real time.</div>
              </td>
            </tr></table>
          </td></tr>
          <tr><td style="padding:0 0 15px;">
            <table cellpadding="0" cellspacing="0" role="presentation"><tr>
              <td style="width:40px;vertical-align:top;padding-top:3px;">
                <div style="width:32px;height:32px;background:rgba(249,115,22,0.12);border:1px solid rgba(249,115,22,0.35);border-radius:8px;text-align:center;line-height:32px;font-size:16px;">&#9889;</div>
              </td>
              <td style="padding-left:12px;">
                <div style="color:#ffffff;font-size:14px;font-weight:700;margin-bottom:3px;">Multi-Exchange Coverage</div>
                <div style="color:rgba(232,240,255,0.55);font-size:13px;line-height:1.6;">Access signals from Binance, Bybit, OKX &amp; MEXC across 15m, 30m, 1H, 4H, and 1D timeframes simultaneously.</div>
              </td>
            </tr></table>
          </td></tr>
          <tr><td style="padding:0 0 15px;">
            <table cellpadding="0" cellspacing="0" role="presentation"><tr>
              <td style="width:40px;vertical-align:top;padding-top:3px;">
                <div style="width:32px;height:32px;background:rgba(249,115,22,0.12);border:1px solid rgba(249,115,22,0.35);border-radius:8px;text-align:center;line-height:32px;font-size:16px;">&#127919;</div>
              </td>
              <td style="padding-left:12px;">
                <div style="color:#ffffff;font-size:14px;font-weight:700;margin-bottom:3px;">Bias &amp; Trend Analysis</div>
                <div style="color:rgba(232,240,255,0.55);font-size:13px;line-height:1.6;">Get clear bullish/bearish bias per pair per timeframe with scoring, trend direction, and ATH/ATL levels.</div>
              </td>
            </tr></table>
          </td></tr>
          <tr><td>
            <table cellpadding="0" cellspacing="0" role="presentation"><tr>
              <td style="width:40px;vertical-align:top;padding-top:3px;">
                <div style="width:32px;height:32px;background:rgba(249,115,22,0.12);border:1px solid rgba(249,115,22,0.35);border-radius:8px;text-align:center;line-height:32px;font-size:16px;">&#128276;</div>
              </td>
              <td style="padding-left:12px;">
                <div style="color:#ffffff;font-size:14px;font-weight:700;margin-bottom:3px;">Watchlist &amp; Pair Tracking</div>
                <div style="color:rgba(232,240,255,0.55);font-size:13px;line-height:1.6;">Build a personalised watchlist, save top setups, and monitor compressed pairs before major moves.</div>
              </td>
            </tr></table>
          </td></tr>
        </table>
      </td></tr>

      <!-- DIVIDER -->
      <tr><td style="background:#0d1525;padding:0 40px;border-left:1px solid rgba(249,115,22,0.15);border-right:1px solid rgba(249,115,22,0.15);">
        <div style="height:1px;background:linear-gradient(90deg,transparent,rgba(249,115,22,0.35),transparent);"></div>
      </td></tr>

      <!-- HOW TO GET STARTED -->
      <tr><td style="background:#080e1c;padding:26px 40px 28px;border-left:1px solid rgba(249,115,22,0.15);border-right:1px solid rgba(249,115,22,0.15);">
        <h2 style="color:#ffffff;font-size:16px;font-weight:700;margin:0 0 18px;text-align:center;">How to Get Started</h2>
        <table width="100%" cellpadding="0" cellspacing="0" role="presentation">
          <tr><td style="padding:0 0 12px;">
            <table cellpadding="0" cellspacing="0" role="presentation"><tr>
              <td style="width:30px;vertical-align:top;">
                <div style="width:24px;height:24px;background:#f97316;border-radius:50%;text-align:center;line-height:24px;font-size:12px;font-weight:700;color:#fff;">1</div>
              </td>
              <td style="padding-left:10px;"><div style="color:rgba(232,240,255,0.80);font-size:13.5px;line-height:1.6;"><strong style="color:#f97316;">Verify your email</strong> — Enter the 6-digit code on the verification page.</div></td>
            </tr></table>
          </td></tr>
          <tr><td style="padding:0 0 12px;">
            <table cellpadding="0" cellspacing="0" role="presentation"><tr>
              <td style="width:30px;vertical-align:top;">
                <div style="width:24px;height:24px;background:#f97316;border-radius:50%;text-align:center;line-height:24px;font-size:12px;font-weight:700;color:#fff;">2</div>
              </td>
              <td style="padding-left:10px;"><div style="color:rgba(232,240,255,0.80);font-size:13.5px;line-height:1.6;"><strong style="color:#f97316;">Sign in to your dashboard</strong> — Use your credentials on the login page.</div></td>
            </tr></table>
          </td></tr>
          <tr><td style="padding:0 0 12px;">
            <table cellpadding="0" cellspacing="0" role="presentation"><tr>
              <td style="width:30px;vertical-align:top;">
                <div style="width:24px;height:24px;background:#f97316;border-radius:50%;text-align:center;line-height:24px;font-size:12px;font-weight:700;color:#fff;">3</div>
              </td>
              <td style="padding-left:10px;"><div style="color:rgba(232,240,255,0.80);font-size:13.5px;line-height:1.6;"><strong style="color:#f97316;">Run your first scan</strong> — Select exchange, timeframe, and module to discover setups.</div></td>
            </tr></table>
          </td></tr>
          <tr><td>
            <table cellpadding="0" cellspacing="0" role="presentation"><tr>
              <td style="width:30px;vertical-align:top;">
                <div style="width:24px;height:24px;background:#f97316;border-radius:50%;text-align:center;line-height:24px;font-size:12px;font-weight:700;color:#fff;">4</div>
              </td>
              <td style="padding-left:10px;"><div style="color:rgba(232,240,255,0.80);font-size:13.5px;line-height:1.6;"><strong style="color:#f97316;">Build your watchlist</strong> — Save top setups and track them with compressed &amp; trending views.</div></td>
            </tr></table>
          </td></tr>
        </table>
      </td></tr>

      <!-- SECURITY NOTE -->
      <tr><td style="background:#070c1a;padding:16px 40px;border-left:1px solid rgba(249,115,22,0.15);border-right:1px solid rgba(249,115,22,0.15);">
        <p style="color:rgba(232,240,255,0.30);font-size:12px;margin:0;text-align:center;line-height:1.7;">
          &#128274; If you did not create a ZyNi SMC account, ignore this email safely.<br>
          Never share your verification code with anyone.
        </p>
      </td></tr>

      <!-- FOOTER -->
      <tr><td style="background:#050810;border-radius:0 0 16px 16px;padding:22px 40px;text-align:center;border:1px solid rgba(249,115,22,0.15);border-top:none;">
        <div style="margin-bottom:10px;">
          <span style="color:#f97316;font-size:17px;font-weight:900;">ZyNi SMC</span>
          <span style="color:rgba(232,240,255,0.30);font-size:13px;"> &middot; Smart Money Center</span>
        </div>
        <div style="margin-bottom:8px;">
          <a href="https://smcsetups.com" style="color:#f97316;text-decoration:none;font-size:13px;font-weight:500;">smcsetups.com</a>
          <span style="color:rgba(232,240,255,0.20);font-size:13px;"> &middot; </span>
          <a href="mailto:support@smcsetups.com" style="color:rgba(232,240,255,0.50);text-decoration:none;font-size:13px;">support@smcsetups.com</a>
        </div>
        <p style="color:rgba(232,240,255,0.18);font-size:11px;margin:6px 0 0;">&copy; 2026 ZyNi SMC. All rights reserved.</p>
      </td></tr>

    </table>
  </td></tr>
</table>
</body>
</html>"""


def send_verification_email(to_email: str, code: str, username: str) -> "tuple[bool, str]":
    """Send a 6-digit OTP verification email directly to the new user.
    Attempts in order:
      1. Resend Python SDK  (RESEND_API_KEY env var — works on all cloud hosts)
      2. Gmail SMTP_SSL   port 465
      3. Gmail STARTTLS   port 587
    Returns (success: bool, fail_reason: str): 'ok' | 'missing_config' | 'delivery_failed'
    """
    frm = _email_from()
    pwd = _email_pass()
    if not to_email:
        return False, "missing_config"

    subject = "ZyNi SMC — Verify Your Email Address"
    body    = _build_verification_email(username, code)

    print(f"[VERIFY-EMAIL] Attempting to send OTP to {to_email}")

    # ── Method 1: Resend HTTP API (works even when SMTP ports are blocked) ──
    if _send_via_resend(to_email, subject, body):
        return True, "ok"

    # ── Method 2 & 3: Gmail SMTP ─────────────────────────────────────────
    if not frm or not pwd:
        print(f"[VERIFY-EMAIL] SMTP skipped — "
              f"ALERT_EMAIL_FROM={'set' if frm else 'MISSING'}, "
              f"ALERT_EMAIL_PASS={'set' if pwd else 'MISSING'}")
        return False, "missing_config"

    def _build_msg():
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = frm
        msg["To"]      = to_email
        msg.attach(MIMEText(body, "html"))
        return msg

    # Method 2: SMTP_SSL port 465
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=15) as srv:
            srv.login(frm, pwd)
            srv.sendmail(frm, to_email, _build_msg().as_string())
        print(f"[VERIFY-EMAIL] Sent via Gmail SSL/465 to {to_email}")
        return True, "ok"
    except Exception as e1:
        print(f"[VERIFY-EMAIL] Gmail SSL/465 failed: {e1}")

    # Method 3: STARTTLS port 587
    try:
        ctx = ssl.create_default_context()
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=15) as srv:
            srv.ehlo()
            srv.starttls(context=ctx)
            srv.ehlo()
            srv.login(frm, pwd)
            srv.sendmail(frm, to_email, _build_msg().as_string())
        print(f"[VERIFY-EMAIL] Sent via Gmail STARTTLS/587 to {to_email}")
        return True, "ok"
    except Exception as e2:
        print(f"[VERIFY-EMAIL] Gmail STARTTLS/587 also failed: {e2}")

    # Credentials were present but all delivery methods failed (SMTP likely blocked)
    print(f"[VERIFY-EMAIL] All methods failed for {to_email} — credentials set but delivery blocked")
    return False, "delivery_failed"


def _auto_migrate():
    """Add new columns / tables to an existing database without Flask-Migrate."""
    with app.app_context():
        try:
            from sqlalchemy import text
            with db.engine.connect() as conn:
                # Create any brand-new tables (email_verifications, etc.)
                db.create_all()
                # Add email_verified column to users if it doesn't exist yet
                conn.execute(text(
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS "
                    "email_verified BOOLEAN NOT NULL DEFAULT FALSE"
                ))
                # Preserve access for users registered before this migration
                conn.execute(text(
                    "UPDATE users SET email_verified = TRUE "
                    "WHERE email_verified = FALSE AND created_at < NOW() - INTERVAL '1 hour'"
                ))
                conn.commit()
                print("[MIGRATE] email_verified column ensured on users table")
        except Exception as exc:
            print(f"[MIGRATE] Auto-migration warning: {exc}")


# Run auto-migration at startup
threading.Thread(target=_auto_migrate, daemon=True).start()

# Print SMTP configuration status on every startup — visible in Koyeb logs
def _log_smtp_config():
    import time; time.sleep(2)   # let gunicorn finish initialising
    frm = _email_from()
    pwd = _email_pass()
    if frm and pwd:
        print(f"[EMAIL-CONFIG] ✓ SMTP configured — from={frm}, pass={'*'*len(pwd)} ({len(pwd)} chars)")
    else:
        print(f"[EMAIL-CONFIG] ✗ SMTP NOT configured — "
              f"ALERT_EMAIL_FROM={'set' if frm else '*** MISSING ***'}, "
              f"ALERT_EMAIL_PASS={'set' if pwd else '*** MISSING ***'}")

threading.Thread(target=_log_smtp_config, daemon=True).start()


# ============================================================
# SERVER-SIDE WATCHLIST STREAMING
# Background thread monitors watchlist pairs every 5s
# Browser reads cached results — works even when phone sleeps
# ============================================================

# Shared state — thread-safe via lock
_wl_lock          = threading.Lock()
_wl_pairs: List[str]          = []          # union of all users' pairs (background thread reads this)
_wl_user_pairs: Dict[str, List[str]] = {}   # username → their registered pairs (user isolation)
_wl_cache: Dict[str, Any]     = {}          # symbol → latest OF result
_wl_thread: Optional[threading.Thread] = None
_wl_running       = False


def _wl_rebuild_union() -> None:
    """Rebuild _wl_pairs from the union of all users' pairs. Caller must hold _wl_lock."""
    seen: set = set()
    result: List[str] = []
    for up in _wl_user_pairs.values():
        for sym in up:
            if sym not in seen:
                seen.add(sym)
                result.append(sym)
    _wl_pairs[:] = result


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
                                obs_, _ = detect_obs(o_, h_, l_, c_, v_, 5, 10, max_ob=3)
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

# ── Multi-Exchange API endpoints ──
BYBIT_SPOT_API      = "https://api.bybit.com/v5/market"
BYBIT_PERP_API      = "https://api.bybit.com/v5/market"
OKX_SPOT_API        = "https://www.okx.com/api/v5/market"
OKX_PERP_API        = "https://www.okx.com/api/v5/market"
MEXC_SPOT_API       = "https://api.mexc.com/api/v3"
MEXC_PERP_API       = "https://contract.mexc.com/api/v1/contract"   # public market endpoints
MEXC_CONTRACT_PRIV  = "https://contract.mexc.com/api/v1/private"     # private account endpoints

# Interval mapping per exchange
# Binance: 1m 3m 5m 15m 30m 1h 2h 4h 6h 8h 12h 1d
# Bybit:   1  3  5  15  30  60 120 240 360 720 D
# OKX:     1m 3m 5m 15m 30m 1H 2H 4H 6H 12H 1D
# MEXC:    1m 5m 15m 30m 60m 4h 1d

INTERVAL_MAP = {
    "bybit": {
        "1m":"1","3m":"3","5m":"5","15m":"15","30m":"30",
        "1h":"60","2h":"120","4h":"240","6h":"360","12h":"720","1d":"D"
    },
    "okx": {
        "1m":"1m","3m":"3m","5m":"5m","15m":"15m","30m":"30m",
        "1h":"1H","2h":"2H","4h":"4H","6h":"6H","12h":"12H","1d":"1D"
    },
    "mexc": {
        "1m":"Min1","5m":"Min5","15m":"Min15","30m":"Min30",
        "1h":"Min60","4h":"Hour4","1d":"Day1"
    }
}

# Per-exchange pair caches
EXCHANGE_PAIR_CACHE: Dict[str, Dict] = {
    "binance": {"ts": 0, "pairs": {}},
    "bybit":   {"ts": 0, "pairs": {}},
    "okx":     {"ts": 0, "pairs": {}},
    "mexc":    {"ts": 0, "pairs": {}},
}

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
# Per-user round-robin cursor for Bias Shift full-market scans.
# Key: "username|exchange|market|tf" — isolates each user's position.
BIAS_SCAN_CURSOR: Dict[str, int] = {}

# ═══════════════════════════════════════
# REAL API WEIGHT TRACKER
# ═══════════════════════════════════════
import threading as _threading

_api_weight: Dict[str, Any] = {
    "binance": {"used": 0, "limit": 1200, "reset_at": 0},
    "bybit":   {"used": 0, "limit": 600,  "reset_at": 0},
    "okx":     {"used": 0, "limit": 2400, "reset_at": 0},
    "mexc":    {"used": 0, "limit": 500,  "reset_at": 0},
}
_api_weight_lock = _threading.Lock()

def update_api_weight(exchange: str, response=None, increment: int = 1):
    """Update API weight from response headers or manual increment"""
    exchange = (exchange or "binance").lower()
    now = time.time()
    with _api_weight_lock:
        state = _api_weight.get(exchange, _api_weight["binance"])
        # Reset every 60 seconds
        if now >= state["reset_at"]:
            state["used"] = 0
            state["reset_at"] = now + 60
        # Read real weight from Binance headers
        if response is not None and exchange == "binance":
            try:
                weight = response.headers.get("X-MBX-USED-WEIGHT-1M")
                if weight:
                    state["used"] = int(weight)
                    return
            except: pass
        # OKX rate limit remaining
        if response is not None and exchange == "okx":
            try:
                remaining = response.headers.get("OK-ACCESS-RATELIMIT-REMAINING")
                if remaining:
                    state["used"] = state["limit"] - int(remaining)
                    return
            except: pass
        # Fallback — increment counter
        state["used"] = min(state["used"] + increment, state["limit"])

def get_api_status(exchange: str = "binance") -> Dict[str, Any]:
    """Get current API weight status"""
    exchange = (exchange or "binance").lower()
    now = time.time()
    with _api_weight_lock:
        state = dict(_api_weight.get(exchange, _api_weight["binance"]))
        reset_in = max(0, int(state["reset_at"] - now))
        return {
            "exchange": exchange,
            "used": state["used"],
            "limit": state["limit"],
            "remaining": max(0, state["limit"] - state["used"]),
            "reset_in_seconds": reset_in,
            "reset_at": state["reset_at"],
            "pct_used": round((state["used"] / state["limit"]) * 100, 1)
        }

# ── PWA routes ───────────────────────────────────────────────────────────────
@app.route('/service-worker.js')
def pwa_service_worker():
    response = app.send_static_file('service-worker.js')
    response.headers['Content-Type'] = 'application/javascript'
    response.headers['Cache-Control'] = 'no-cache, max-age=0'
    response.headers['Service-Worker-Allowed'] = '/'
    return response

@app.route('/manifest.json')
def pwa_manifest():
    response = app.send_static_file('manifest.json')
    response.headers['Content-Type'] = 'application/manifest+json'
    response.headers['Cache-Control'] = 'public, max-age=86400'
    return response

@app.route('/offline')
def pwa_offline():
    response = app.send_static_file('offline.html')
    response.headers['Content-Type'] = 'text/html'
    response.headers['Cache-Control'] = 'public, max-age=86400'
    return response

# ── End PWA routes ────────────────────────────────────────────────────────────

@app.route("/api/weight_status")
def api_weight_status():
    """Return real API weight usage for current exchange"""
    # No login_required — called frequently for UI counter
    if not session.get("logged_in"):
        return jsonify({"used":0,"limit":1200,"remaining":1200,"reset_in_seconds":60,"pct_used":0})
    exchange = request.args.get("exchange", "binance").lower()
    return jsonify(get_api_status(exchange))



# ═══════════════════════════════════════════════
# HOMEPAGE DATA ROUTES
# ═══════════════════════════════════════════════

import time as _time
_hp_cache: Dict[str, Any] = {}
_hp_cache_ttl: Dict[str, float] = {}

def _hp_cached(key: str, ttl: int, fn):
    """Simple cache wrapper for homepage API calls"""
    now = _time.time()
    if key in _hp_cache and now - _hp_cache_ttl.get(key, 0) < ttl:
        return _hp_cache[key]
    try:
        result = fn()
        _hp_cache[key] = result
        _hp_cache_ttl[key] = now
        return result
    except Exception as e:
        print(f"[HP Cache] {key} error: {e}")
        return _hp_cache.get(key, None)

@app.route('/api/homepage/feargreed')
def hp_feargreed():
    """Fear & Greed Index from alternative.me — cached 1 hour"""
    def fetch():
        import urllib.request
        url = 'https://api.alternative.me/fng/?limit=1&format=json'
        req = urllib.request.Request(url, headers={'User-Agent':'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=8) as r:
            data = json.loads(r.read().decode())
        entry = data['data'][0]
        return {
            'value': int(entry['value']),
            'label': entry['value_classification'],
            'timestamp': entry['timestamp']
        }
    result = _hp_cached('feargreed', 3600, fetch)
    if result is None:
        return jsonify({'value': 50, 'label': 'Neutral', 'timestamp': ''}), 200
    return jsonify(result)

@app.route('/api/homepage/ticker')
def hp_ticker():
    """Live ticker prices from Binance — cached 30 seconds"""
    SYMBOLS = ['BTCUSDT','ETHUSDT','BNBUSDT','SOLUSDT','XRPUSDT',
               'ADAUSDT','DOGEUSDT','AVAXUSDT','DOTUSDT','LINKUSDT']
    def fetch():
        import urllib.request
        url = 'https://api.binance.com/api/v3/ticker/24hr'
        req = urllib.request.Request(url, headers={'User-Agent':'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read().decode())
        result = []
        sym_map = {d['symbol']: d for d in data}
        for sym in SYMBOLS:
            if sym in sym_map:
                d = sym_map[sym]
                result.append({
                    'symbol': sym.replace('USDT',''),
                    'price': float(d['lastPrice']),
                    'change': float(d['priceChangePercent']),
                    'volume': float(d['quoteVolume'])
                })
        return result
    result = _hp_cached('ticker', 30, fetch)
    if result is None:
        return jsonify([]), 200
    return jsonify(result)

@app.route('/api/homepage/gainers')
def hp_gainers():
    """Top gainers and losers — Futures primary, Spot fallback — cached 60s"""
    def fetch():
        stables = {'USDC','BUSD','TUSD','USDP','DAI','FDUSD'}
        data = None
        # Primary: Binance Futures
        try:
            r = req.get('https://fapi.binance.com/fapi/v1/ticker/24hr',
                        headers={'User-Agent': 'Mozilla/5.0'}, timeout=8)
            r.raise_for_status()
            data = r.json()
            print(f"[HP Gainers] Futures OK — {len(data)} symbols")
        except Exception as e:
            print(f"[HP Gainers] Futures failed: {e}")
            print(traceback.format_exc())
        # Fallback: Binance Spot
        if not data:
            try:
                r = req.get('https://api.binance.com/api/v3/ticker/24hr',
                            headers={'User-Agent': 'Mozilla/5.0'}, timeout=8)
                r.raise_for_status()
                data = r.json()
                print(f"[HP Gainers] Spot fallback OK — {len(data)} symbols")
            except Exception as e:
                print(f"[HP Gainers] Spot fallback failed: {e}")
                print(traceback.format_exc())
                return None
        pairs = [
            {
                'symbol': d['symbol'].replace('USDT', ''),
                'price':  float(d['lastPrice']),
                'change': float(d['priceChangePercent']),
                'volume': float(d['quoteVolume'])
            }
            for d in data
            if d['symbol'].endswith('USDT')
            and d['symbol'].replace('USDT', '') not in stables
            and float(d['quoteVolume']) > 1_000_000
        ]
        gainers = sorted(pairs, key=lambda x: x['change'], reverse=True)[:5]
        losers  = sorted(pairs, key=lambda x: x['change'])[:5]
        return {'gainers': gainers, 'losers': losers}
    result = _hp_cached('gainers', 60, fetch)
    if result is None:
        return jsonify({'gainers': [], 'losers': []}), 200
    return jsonify(result)

@app.route('/api/homepage/volume')
def hp_volume():
    """24H exchange volumes — cached 5 minutes"""
    def fetch():
        volumes = []
        hdrs = {'User-Agent': 'Mozilla/5.0'}

        # Binance Futures (most reliable — higher volume than spot)
        try:
            r = req.get('https://fapi.binance.com/fapi/v1/ticker/24hr', headers=hdrs, timeout=8)
            r.raise_for_status()
            data = r.json()
            total = sum(float(d['quoteVolume']) for d in data if d['symbol'].endswith('USDT'))
            volumes.append({'exchange': 'Binance', 'volume': round(total / 1e9, 1), 'color': '#f59e0b'})
            print(f"[HP Volume] Binance Futures: {round(total/1e9,1)}B")
        except Exception as e:
            print(f"[HP Volume Error] Binance: {e}")
            print(traceback.format_exc())
            volumes.append({'exchange': 'Binance', 'volume': 0, 'color': '#f59e0b'})

        # Bybit Linear (perpetual)
        try:
            r = req.get('https://api.bybit.com/v5/market/tickers?category=linear', headers=hdrs, timeout=8)
            r.raise_for_status()
            data = r.json()
            total = sum(
                float(d.get('turnover24h', 0))
                for d in data.get('result', {}).get('list', [])
                if d.get('symbol', '').endswith('USDT')
            )
            volumes.append({'exchange': 'Bybit', 'volume': round(total / 1e9, 1), 'color': '#22d3ee'})
            print(f"[HP Volume] Bybit Linear: {round(total/1e9,1)}B")
        except Exception as e:
            print(f"[HP Volume Error] Bybit: {e}")
            print(traceback.format_exc())
            volumes.append({'exchange': 'Bybit', 'volume': 0, 'color': '#22d3ee'})

        # OKX Swap — vol24h (base) * last (price) = USD volume per symbol
        try:
            r = req.get('https://www.okx.com/api/v5/market/tickers?instType=SWAP', headers=hdrs, timeout=8)
            r.raise_for_status()
            data = r.json()
            total = sum(
                float(d.get('vol24h', 0)) * float(d.get('last', 0))
                for d in data.get('data', [])
                if d.get('instId', '').endswith('USDT-SWAP')
            )
            volumes.append({'exchange': 'OKX', 'volume': round(total / 1e9, 1), 'color': '#22c55e'})
            print(f"[HP Volume] OKX Swap: {round(total/1e9,1)}B")
        except Exception as e:
            print(f"[HP Volume Error] OKX: {e}")
            print(traceback.format_exc())
            volumes.append({'exchange': 'OKX', 'volume': 0, 'color': '#22c55e'})

        # MEXC
        try:
            r = req.get('https://api.mexc.com/api/v3/ticker/24hr', headers=hdrs, timeout=8)
            r.raise_for_status()
            data = r.json()
            total = sum(float(d.get('quoteVolume', 0)) for d in data if d.get('symbol', '').endswith('USDT'))
            volumes.append({'exchange': 'MEXC', 'volume': round(total / 1e9, 1), 'color': '#f97316'})
            print(f"[HP Volume] MEXC: {round(total/1e9,1)}B")
        except Exception as e:
            print(f"[HP Volume Error] MEXC: {e}")
            print(traceback.format_exc())
            volumes.append({'exchange': 'MEXC', 'volume': 0, 'color': '#f97316'})

        # Bitget Futures — V2 API (V1 mix/v1 was deprecated May 2026)
        try:
            r = req.get('https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES', headers=hdrs, timeout=8)
            r.raise_for_status()
            data = r.json()
            total = sum(float(d.get('quoteVolume', d.get('usdtVolume', 0))) for d in data.get('data', []))
            volumes.append({'exchange': 'Bitget', 'volume': round(total / 1e9, 1), 'color': '#a78bfa'})
            print(f"[HP Volume] Bitget: {round(total/1e9,1)}B")
        except Exception as e:
            print(f"[HP Volume Error] Bitget: {e}")
            print(traceback.format_exc())
            volumes.append({'exchange': 'Bitget', 'volume': 0, 'color': '#a78bfa'})

        return volumes

    result = _hp_cached('volume', 300, fetch)
    if result is None:
        return jsonify([]), 200
    return jsonify(result)

@app.route('/api/homepage/marketcap')
def hp_marketcap():
    """Global crypto market cap — cached 5 minutes"""
    def fetch():
        import urllib.request
        url = 'https://api.coingecko.com/api/v3/global'
        req = urllib.request.Request(url, headers={'User-Agent':'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=8) as r:
            data = json.loads(r.read().decode())
        d = data['data']
        return {
            'total_mcap': round(d['total_market_cap']['usd'] / 1e12, 2),
            'total_volume': round(d['total_volume']['usd'] / 1e9, 1),
            'btc_dominance': round(d['market_cap_percentage']['btc'], 1),
            'eth_dominance': round(d['market_cap_percentage']['eth'], 1),
            'change_24h': round(d['market_cap_change_percentage_24h_usd'], 2)
        }
    result = _hp_cached('marketcap', 300, fetch)
    if result is None:
        return jsonify({'total_mcap':0,'total_volume':0,'btc_dominance':0,'eth_dominance':0,'change_24h':0}), 200
    return jsonify(result)

@app.route('/homepage')
def homepage():
    """Serve the homepage"""
    return render_template('homepage.html')



# True ATH/ATL cache — keyed by "symbol:market", TTL 4 hours
ATH_ATL_CACHE: Dict[str, Any] = {}
ATH_ATL_CACHE_TTL = 4 * 3600

# Raw 1D history cache for ATH/ATL window math — keyed by symbol, TTL 4 hours.
# Lets the windowed ATH/ATL endpoint compute "previous level BEFORE window"
# without re-paginating full daily history on every batch.
ATH_ATL_DAILY_CACHE: Dict[str, Any] = {}
ATH_ATL_DAILY_CACHE_TTL = 4 * 3600

# ATH/ATL batch-scan state — keyed by "user:exchange:market".
# Holds the rolling cursor + accumulated results so "Scan Next Batch"
# walks the full market step by step and results survive until reset.
ATH_ATL_SCAN_STATE: Dict[str, Any] = {}
_ath_atl_scan_lock = threading.Lock()

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


def _detect_pivots_relaxed(high: List[float], low: List[float], left: int, right: int) -> Tuple[List[bool], List[bool]]:
    """
    Debug-only plateau-tolerant pivots. A bar is a pivot high when it is
    >= every left neighbour and > every right neighbour, so the last bar of
    an equal-high plateau becomes the confirmed pivot (strict detect_pivots
    rejects every bar of a plateau). Mirror logic for pivot lows.
    """
    n = len(high)
    ph = [False] * n
    pl = [False] * n
    for i in range(left, n - right):
        ph[i] = (all(high[i] >= high[j] for j in range(i - left, i)) and
                 all(high[i] >  high[j] for j in range(i + 1, i + right + 1)))
        pl[i] = (all(low[i] <= low[j] for j in range(i - left, i)) and
                 all(low[i] <  low[j] for j in range(i + 1, i + right + 1)))
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


# ── OB touch counting (clearance-based) ─────────────────────────────────────
# Phase 1A: backend-only metadata. Does not alter OB parity (top/bottom/avg/
# sourceBar/volume/TV OB %) and does not change mitigation behavior.
_OB_TOUCH_CLEARANCE = 0.5  # fixed internal default; settings come in a later phase


def _compute_ob_touch_meta(ob, h, l, c, n, ob_mitigation="Absolute",
                            track_mitigation=False,
                            clearance_factor=_OB_TOUCH_CLEARANCE):
    """
    Walk forward from ob["bar"] + 1 through n - 1 (excluding the current open
    candle) and count zone touches with a clearance-based de-duplication rule.

    A touch begins when a candle wicks into the zone (low <= top and high >=
    bottom). After a touch, a new touch only counts once price has cleared the
    zone by `clearance_factor * height`:

        bullish: clearance_level = top    + clearance_factor * (top - bottom)
        bearish: clearance_level = bottom - clearance_factor * (top - bottom)

    Counting starts at ob["bar"] + 1 (NOT sourceBar) so the formation candle
    itself is never counted.

    track_mitigation:
      False  — used by detect_obs(); mitigation has already been resolved by
               the existing mitigation loop, so we just count touches across
               the full counting window and report mitigationBar=None.
      True   — used by detect_obs_all(); detect mitigation in the same pass.
               If the mitigation candle also enters the zone, the touch on
               that bar is counted FIRST, then mitigation is marked and the
               walk stops. Bars after mitigationBar are not counted.
    """
    top    = ob["top"]
    bottom = ob["bottom"]
    avg_v  = ob.get("avg", (top + bottom) / 2.0)
    height = max(top - bottom, 1e-10)
    is_bull = ob["type"] == "bullish"

    if is_bull:
        clearance_level = top + clearance_factor * height
    else:
        clearance_level = bottom - clearance_factor * height

    if ob_mitigation == "Middle":
        mit_trigger = avg_v
    else:
        mit_trigger = bottom if is_bull else top

    start = ob["bar"] + 1
    end   = n - 1  # exclude current open candle (Pine barstate.isconfirmed)

    touches = 0
    first_touch_bar = None
    last_touch_bar  = None
    cleared = True   # initial state: first contact is allowed to count
    seq = []
    mitigated      = False
    mitigation_bar = None

    for j in range(start, end):
        bar_inside = (l[j] <= top) and (h[j] >= bottom)

        if bar_inside:
            if cleared:
                touches += 1
                if first_touch_bar is None:
                    first_touch_bar = j
                last_touch_bar = j
                cleared = False
                seq.append(j)
            else:
                last_touch_bar = j
        else:
            if is_bull:
                if h[j] >= clearance_level:
                    cleared = True
            else:
                if l[j] <= clearance_level:
                    cleared = True

        if track_mitigation:
            if is_bull and c[j] < mit_trigger:
                mitigated      = True
                mitigation_bar = j
                break
            if (not is_bull) and c[j] > mit_trigger:
                mitigated      = True
                mitigation_bar = j
                break

    currently_inside = False
    if n >= 2 and touches > 0:
        last_idx = n - 2
        if 0 <= last_idx < n and last_idx >= start:
            currently_inside = (l[last_idx] <= top) and (h[last_idx] >= bottom)

    return {
        "touches":         touches,
        "firstTouchBar":   first_touch_bar,
        "lastTouchBar":    last_touch_bar,
        "untouched":       touches == 0,
        "onceTouched":     touches == 1,
        "touchSeq":        seq,
        "isVirgin":        touches == 0,
        "currentlyInside": currently_inside,
        "mitigated":       mitigated,
        "mitigationBar":   mitigation_bar,
    }


def detect_obs(o, h, l, c, v, i_len, s_len, max_ob=5, ob_positioning="Precise", ob_mitigation="Absolute",
               mitigation_closed_only=None, overlap_effective_zone=False,
               bearish_effective_bottom_overlap=None, trace=None, anchor_mode=None,
               extreme_tie_mode=None, ob_logic_mode="tv_parity_v3", structure_candidate="current"):
    """
    Order Block detection — audited line-by-line against Pine Script drawVOB().

    Debug-only parameters (defaults preserve production behavior — DO NOT pass
    these from the screener / analyze_pair path):
      mitigation_closed_only: when True, per-bar mitigation skips the last
        (possibly still-open) candle, i.e. mitigation is only evaluated for
        bars i <= n-2. OB creation still scans the full candle stream.
      overlap_effective_zone: DEPRECATED / WRONG. Collapsed the *new* OB's
        extreme edge to avg (bullish bottom→avg, bearish top→avg). Kept only
        for diagnostic comparison; do not use for production parity.
      bearish_effective_bottom_overlap: CORRECT bearish rule. For bearish
        overlap only, the *previous* OB's effective bottom is its avg (TV
        displays the bearish lower boundary at avg, not the raw extreme). The
        new OB's top stays raw. Bullish overlap stays raw/unchanged.
      trace: optional dict {"events": [], "mitigations": []}. When provided,
        every BOS event and every per-bar mitigation is recorded into it.
        Pure observation — does not alter detection. Default None = no-op.
      anchor_mode: OB search-window anchor.
        "baseline" (default) — search_start = broken pivot_bar + 1.
        "latest_opposite_pivot" — search_start = the latest confirmed
        opposite pivot (pivot LOW for bullish / pivot HIGH for bearish)
        strictly after the broken pivot and confirmed on/before the BOS
        bar; the pivot candle itself, no +1. Falls back to baseline when
        no such pivot exists.
      extreme_tie_mode: tie-break when several bars share the extreme.
        "first" (default) — earliest extreme wins (strict < / >).
        "last" — latest equal extreme wins (<= / >=). The Pine +1
        sourceBar offset is unchanged; only the chosen extreme bar moves.
      ob_logic_mode: production logic mode.
        "tv_parity_v3" (default) — v2 rules + Pine-style relaxed equal-pivot
        detection (plateau highs/lows confirm as pivots).
        "tv_parity_v2" — confirmed TV-parity behavior (strict pivots).
        Kept available for rollback / debug comparison.
        "legacy_baseline" — original pre-parity behavior (deepest rollback).
        Any rule argument left as None is resolved from this mode; an
        explicit per-rule argument (not None) always overrides.
        Mode-resolved rules: mitigation_closed_only,
        bearish_effective_bottom_overlap, anchor_mode, extreme_tie_mode,
        plus pivot-detection (relaxed for v3, strict for v2 / legacy).
        v2 / v3 share: closed mitigation + bearish effective-bottom overlap
        + latest_opposite_pivot anchor + last equal-extreme tie.
      structure_candidate: DEBUG-ONLY structure-lifecycle variant.
        "current" (default) — production behavior, byte-identical.
        "retain_broken_upP" — on a BOS, keep the broken level instead of
        clearing the whole stack (until a newer pivot replaces it).
        "promote_bos_high_to_upP" — on a BOS, replace the stack with the
        BOS candle extreme as the new structure level.
        "equal_high_pivot_relaxed" — use plateau-tolerant pivot detection.
        Production callers must NOT pass this.

    Pine search window:
      search_start = pivot_bar + 1 (Pine loc = hN/lN.first() = absolute pivot bar).
      Pine loop: for i = 0 to math.abs((loc - b.n)) - 1 → covers [pivot+1, BOS_bar].
      Window size = BOS_bar - pivot_bar, independent of iLen.

    Zone source candle (+1 offset within the search range):
      Pine finds the extreme candle (lowest low / highest high), then takes
      the candle ONE BAR EARLIER (the +1 offset) for hl2 and volume.
      ob_source = max(0, extreme_idx - 1)  # floor=0 only, Pine's offset is unconditional

    Pine reference:
      int iU = obj.l.indexof(obj.l.min()) + 1   <- the +1 offset
      obj.top.unshift(pos[iU])                   <- hl2 from offset candle
      obj.btm.unshift(obj.l.min())               <- actual minimum low value
      obj.cV.unshift(b.v[iU])                    <- volume from offset candle
    """
    # ── Production logic mode resolution ──────────────────────────────────
    # A rule left as None inherits from ob_logic_mode; an explicit argument
    # always wins (used by the debug endpoint). Modes:
    #   "tv_parity_v3" — v2 rules + Pine-style relaxed equal-pivot detection
    #                    (the current production default).
    #   "tv_parity_v2" — closed mitigation + bearish effective-bottom overlap
    #                    + latest_opposite_pivot anchor + last equal-extreme
    #                    tie (kept available for rollback / debug comparison).
    #   "legacy_baseline" — original pre-parity behavior (deepest rollback).
    _v2 = ob_logic_mode in ("tv_parity_v2", "tv_parity_v3")
    _v3 = ob_logic_mode == "tv_parity_v3"
    if mitigation_closed_only is None:
        mitigation_closed_only = _v2
    if bearish_effective_bottom_overlap is None:
        bearish_effective_bottom_overlap = _v2
    if anchor_mode is None:
        anchor_mode = "latest_opposite_pivot" if _v2 else "baseline"
    if extreme_tie_mode is None:
        extreme_tie_mode = "last" if _v2 else "first"

    n = len(c)
    # Pivot detection — v3 uses Pine-style relaxed (plateau-tolerant) pivots;
    # v2 / legacy keep strict pivots. structure_candidate can force relaxed
    # explicitly (debug diagnostics).
    if _v3 or structure_candidate == "equal_high_pivot_relaxed":
        ph, pl = _detect_pivots_relaxed(h, l, i_len, i_len)
    else:
        ph, pl = detect_pivots(h, l, i_len, i_len)
    obs = []

    upP, upB, upL = [], [], []
    dnP, dnB, dnL = [], [], []
    prev_upP_first = None   # Pine: up.p.first()[1] — end-of-previous-bar value
    prev_dnP_first = None   # Pine: dn.p.first()[1] — end-of-previous-bar value
    # ─── DIAGNOSTIC TRACE — TEMPORARY ───
    _trace = {"pivot_high": [], "pivot_low": [], "bull_bos": [], "bear_bos": []}

    start = max(i_len * 2 + 2, s_len + 2)

    # Note: We include all candles including current open candle for OB detection.
    # OB is confirmed by BOS (break of structure) — the BOS candle can be current.
    # Unlike FVG which needs 3 closed candles, OB only needs the BOS to occur.
    for i in range(start, n):
        # Debug-only per-bar structure snapshot (opt-in: caller passes a
        # trace dict that already contains a "bars" list). Pure observation.
        _bars_rec    = trace is not None and trace.get("bars") is not None
        _upP0_before = (upP[0] if upP else None) if _bars_rec else None
        _dnP0_before = (dnP[0] if dnP else None) if _bars_rec else None

        if i - i_len >= 0 and ph[i - i_len]:
            upP.insert(0, h[i - i_len])
            upB.insert(0, i - i_len)
            upL.insert(0, h[i - i_len])
            _trace["pivot_high"].append({"bar": i - i_len, "i_at_push": i, "price": h[i - i_len]})
        if i - i_len >= 0 and pl[i - i_len]:
            dnP.insert(0, l[i - i_len])
            dnB.insert(0, i - i_len)
            dnL.insert(0, l[i - i_len])
            _trace["pivot_low"].append({"bar": i - i_len, "i_at_push": i, "price": l[i - i_len]})

        if _bars_rec:
            trace["bars"].append({
                "bar": i,
                "active_upP_before_bar": _upP0_before,
                "active_dnP_before_bar": _dnP0_before,
                "upP_first": upP[0] if upP else None,
                "dnP_first": dnP[0] if dnP else None,
                "prev_upP_first": prev_upP_first,
                "prev_dnP_first": prev_dnP_first,
                "upP_len": len(upP), "dnP_len": len(dnP),
                "upL_len": len(upL), "dnL_len": len(dnL),
            })

        # ── INTERNAL BULLISH BREAK → Create Bullish OB ──
        # Pine: ta.crossover(b.c, up.p.first()) → c[i] > upP[0] AND c[i-1] <= prev value
        # prev_upP_first is None when up.p was empty last bar → cross fails (matches Pine na)
        if upP and len(dnL) > 1 and c[i] > upP[0] \
                and prev_upP_first is not None and c[i - 1] <= prev_upP_first:
            pivot_bar    = upB[0] if upB else i - 10
            # Pine: loc = hN.first() = absolute pivot bar. Loop covers (BOS - pivot)
            # bars, accessing [pivot+1, BOS]. Window size is independent of iLen.
            search_start = max(0, pivot_bar + 1)

            # ── Debug-only OB search anchor (anchor_mode) ──
            _anchor_pivot_bar = None
            if anchor_mode == "latest_opposite_pivot":
                # Latest confirmed pivot LOW strictly after the broken-high
                # pivot, confirmed on/before the BOS bar (pl[b] confirms at
                # b + i_len). search_start = the pivot candle itself (no +1).
                for _b in range(min(i - i_len, i), pivot_bar, -1):
                    if 0 <= _b < n and pl[_b]:
                        _anchor_pivot_bar = _b
                        search_start = _b
                        break

            search_end   = i + 1  # include break bar

            _tev = None
            if trace is not None:
                _tev = {
                    "side": "bullish", "bos_bar": i, "pivot_bar": pivot_bar,
                    "broken_level": upP[0], "prev_break_level": prev_upP_first,
                    "close_prev": c[i - 1], "close_curr": c[i],
                    "anchor_mode": anchor_mode,
                    "extreme_tie_mode": extreme_tie_mode,
                    "anchor_pivot_bar": _anchor_pivot_bar,
                    "anchor_confirmed_at_bar": (_anchor_pivot_bar + i_len
                                                if _anchor_pivot_bar is not None else None),
                    "anchor_fallback": (anchor_mode == "latest_opposite_pivot"
                                        and _anchor_pivot_bar is None),
                    "search_start": search_start, "search_end": search_end,
                    "window_empty": search_end <= search_start, "created": False,
                }

            if search_end > search_start:
                # Step 1: Find candle with lowest low
                # extreme_tie_mode: "first" → strict < (earliest wins);
                #                   "last"  → <= (latest equal low wins).
                min_idx = search_start
                _tie_last = extreme_tie_mode == "last"
                for j in range(search_start, search_end):
                    if (l[j] <= l[min_idx]) if _tie_last else (l[j] < l[min_idx]):
                        min_idx = j

                # Step 2: +1 offset — Pine uses the candle ONE BAR EARLIER for hl2/volume
                # In Pine's reversed array: +1 = older = one bar to the left in forward time
                # Floor is 0 (array boundary), NOT search_start — Pine's offset is unconditional
                ob_source = max(0, min_idx - 1)

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
                _precise_applied = False
                if ob_positioning == "Precise":
                    body_low = min(c[ob_source], o[ob_source])
                    if ob_avg < body_low and ob_top > hlcc4_val:
                        ob_top = ob_avg
                        ob_avg = (ob_top + ob_bottom) / 2.0
                        _precise_applied = True

                # Step 5: Volume and direction from SOURCE candle (the +1 offset candle)
                candle_dir = 1 if c[ob_source] > o[ob_source] else -1
                total_v    = v[ob_source]
                buy_v      = total_v * (0.6 if candle_dir == 1 else 0.4)
                sell_v     = total_v - buy_v

                _trace["bull_bos"].append({
                    "bos_bar": i, "pivot_bar": pivot_bar,
                    "upP_first": upP[0], "prev_upP_first": prev_upP_first,
                    "close_curr": c[i], "close_prev": c[i - 1],
                    "ob_top": ob_top, "ob_bottom": ob_bottom, "ob_source_bar": ob_source,
                })
                if trace is not None and _tev is not None:
                    _tev.update({
                        "min_idx": min_idx, "ob_source": ob_source,
                        "ob_top": ob_top, "ob_bottom": ob_bottom, "ob_avg": ob_avg,
                        "precise_applied": _precise_applied,
                        "candle_dir": candle_dir, "volume": total_v,
                    })
                if ob_top > ob_bottom:
                    _new_payload = {
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
                    }
                    obs.append(_new_payload)
                    # Pine line 1282-1296: overlap deletion (mode="Previous",
                    # rmP=0 → drop the newly-appended OB)
                    _prev = next((x for x in reversed(obs[:-1]) if x["type"] == "bullish"), None)
                    _dropped = False
                    if _prev is not None:
                        _new_lo = _new_payload["avg"] if overlap_effective_zone else _new_payload["bottom"]
                        if _new_lo < _prev["top"]:
                            obs.pop()
                            _dropped = True
                    if trace is not None and _tev is not None:
                        _tev["created"] = True
                        _tev["creation_overlap"] = {
                            "checked": _prev is not None,
                            "prev_ob_bar":    _prev["bar"]        if _prev is not None else None,
                            "prev_ob_top":    _prev["top"]        if _prev is not None else None,
                            "prev_ob_bottom": _prev["bottom"]     if _prev is not None else None,
                            "prev_ob_avg":    _prev.get("avg")    if _prev is not None else None,
                            "rule": ("effective_zone_new_avg" if overlap_effective_zone
                                     else "raw_extreme"),
                            "deleted_by_creation_overlap": _dropped,
                        }
                elif trace is not None and _tev is not None:
                    _tev["created"] = False
                    _tev["not_created_reason"] = "ob_top <= ob_bottom"

            if trace is not None and _tev is not None:
                trace["events"].append(_tev)

            # Structure lifecycle on bullish BOS — structure_candidate variants
            # (debug-only; "current" clears the stack as in production).
            if structure_candidate == "retain_broken_upP":
                upP[:] = [upP[0]]
                upB[:] = [upB[0]]
            elif structure_candidate == "promote_bos_high_to_upP":
                upP[:] = [h[i]]
                upB[:] = [i]
            else:
                upP.clear()
                upB.clear()

        # ── INTERNAL BEARISH BREAK → Create Bearish OB ──
        # Pine: ta.crossunder(b.c, dn.p.first()) → c[i] < dnP[0] AND c[i-1] >= prev value
        if dnP and len(upL) > 1 and c[i] < dnP[0] \
                and prev_dnP_first is not None and c[i - 1] >= prev_dnP_first:
            pivot_bar    = dnB[0] if dnB else i - 10
            # Pine: loc = lN.first() = absolute pivot bar. Loop covers (BOS - pivot)
            # bars, accessing [pivot+1, BOS]. Window size is independent of iLen.
            search_start = max(0, pivot_bar + 1)

            # ── Debug-only OB search anchor (anchor_mode) ──
            _anchor_pivot_bar = None
            if anchor_mode == "latest_opposite_pivot":
                # Latest confirmed pivot HIGH strictly after the broken-low
                # pivot, confirmed on/before the BOS bar (ph[b] confirms at
                # b + i_len). search_start = the pivot candle itself (no +1).
                for _b in range(min(i - i_len, i), pivot_bar, -1):
                    if 0 <= _b < n and ph[_b]:
                        _anchor_pivot_bar = _b
                        search_start = _b
                        break

            search_end   = i + 1

            _tev = None
            if trace is not None:
                _tev = {
                    "side": "bearish", "bos_bar": i, "pivot_bar": pivot_bar,
                    "broken_level": dnP[0], "prev_break_level": prev_dnP_first,
                    "close_prev": c[i - 1], "close_curr": c[i],
                    "anchor_mode": anchor_mode,
                    "extreme_tie_mode": extreme_tie_mode,
                    "anchor_pivot_bar": _anchor_pivot_bar,
                    "anchor_confirmed_at_bar": (_anchor_pivot_bar + i_len
                                                if _anchor_pivot_bar is not None else None),
                    "anchor_fallback": (anchor_mode == "latest_opposite_pivot"
                                        and _anchor_pivot_bar is None),
                    "search_start": search_start, "search_end": search_end,
                    "window_empty": search_end <= search_start, "created": False,
                }

            if search_end > search_start:
                # Step 1: Find candle with highest high
                # extreme_tie_mode: "first" → strict > (earliest wins);
                #                   "last"  → >= (latest equal high wins).
                max_idx = search_start
                _tie_last = extreme_tie_mode == "last"
                for j in range(search_start, search_end):
                    if (h[j] >= h[max_idx]) if _tie_last else (h[j] > h[max_idx]):
                        max_idx = j

                # Step 2: +1 offset — floor is 0, NOT search_start (Pine's offset is unconditional)
                ob_source = max(0, max_idx - 1)

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
                _precise_applied = False
                if ob_positioning == "Precise":
                    body_high = max(c[ob_source], o[ob_source])
                    if ob_avg > body_high and ob_bottom < hlcc4_val:
                        ob_bottom = ob_avg
                        ob_avg    = (ob_top + ob_bottom) / 2.0
                        _precise_applied = True

                # Step 5: Volume and direction from SOURCE candle
                candle_dir = 1 if c[ob_source] > o[ob_source] else -1
                total_v    = v[ob_source]
                sell_v     = total_v * (0.6 if candle_dir == -1 else 0.4)
                buy_v      = total_v - sell_v

                _trace["bear_bos"].append({
                    "bos_bar": i, "pivot_bar": pivot_bar,
                    "dnP_first": dnP[0], "prev_dnP_first": prev_dnP_first,
                    "close_curr": c[i], "close_prev": c[i - 1],
                    "ob_top": ob_top, "ob_bottom": ob_bottom, "ob_source_bar": ob_source,
                })
                if trace is not None and _tev is not None:
                    _tev.update({
                        "max_idx": max_idx, "ob_source": ob_source,
                        "ob_top": ob_top, "ob_bottom": ob_bottom, "ob_avg": ob_avg,
                        "precise_applied": _precise_applied,
                        "candle_dir": candle_dir, "volume": total_v,
                    })
                if ob_top > ob_bottom:
                    _new_payload = {
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
                    }
                    obs.append(_new_payload)
                    # Pine line 1282-1296: overlap deletion (mode="Previous",
                    # rmP=0 → drop the newly-appended OB)
                    _prev = next((x for x in reversed(obs[:-1]) if x["type"] == "bearish"), None)
                    _dropped = False
                    if _prev is not None:
                        if bearish_effective_bottom_overlap:
                            # CORRECT: prev OB effective bottom = its avg;
                            # new OB top stays raw.
                            if _new_payload["top"] > _prev.get("avg", _prev["bottom"]):
                                obs.pop()
                                _dropped = True
                        else:
                            _new_hi = _new_payload["avg"] if overlap_effective_zone else _new_payload["top"]
                            if _new_hi > _prev["bottom"]:
                                obs.pop()
                                _dropped = True
                    if trace is not None and _tev is not None:
                        _tev["created"] = True
                        _tev["creation_overlap"] = {
                            "checked": _prev is not None,
                            "prev_ob_bar":    _prev["bar"]        if _prev is not None else None,
                            "prev_ob_top":    _prev["top"]        if _prev is not None else None,
                            "prev_ob_bottom": _prev["bottom"]     if _prev is not None else None,
                            "prev_ob_avg":    _prev.get("avg")    if _prev is not None else None,
                            "rule": ("bearish_effective_bottom_prev_avg"
                                     if bearish_effective_bottom_overlap
                                     else ("effective_zone_new_avg" if overlap_effective_zone
                                           else "raw_extreme")),
                            "deleted_by_creation_overlap": _dropped,
                        }
                elif trace is not None and _tev is not None:
                    _tev["created"] = False
                    _tev["not_created_reason"] = "ob_top <= ob_bottom"

            if trace is not None and _tev is not None:
                trace["events"].append(_tev)

            # Structure lifecycle on bearish BOS — structure_candidate mirror
            # (debug-only; "current" clears the stack as in production).
            if structure_candidate == "retain_broken_upP":
                dnP[:] = [dnP[0]]
                dnB[:] = [dnB[0]]
            elif structure_candidate == "promote_bos_high_to_upP":
                dnP[:] = [l[i]]
                dnB[:] = [i]
            else:
                dnP.clear()
                dnB.clear()

        # ── Pine line 1298-1321: per-bar mitigation (barstate.isconfirmed) ──
        # ob_mitigation defaults to "Absolute": bull→ob.bottom, bear→ob.top
        # show_breakers=False → mitigated OBs are REMOVED from the array.
        # This must run EVERY bar, not only on BOS bars, so that the array
        # is clean when the next bar's overlap check looks up the
        # "previous" same-direction OB.
        # mitigation_closed_only (debug): skip the last (possibly still-open)
        # candle so mitigation is only evaluated for bars i <= n-2.
        if obs and (not mitigation_closed_only or i < n - 1):
            _close_i = c[i]
            _kept = []
            for _ob in obs:
                if ob_mitigation == "Middle":
                    _trigger = _ob["avg"]
                else:  # Absolute
                    _trigger = _ob["bottom"] if _ob["type"] == "bullish" else _ob["top"]
                _mit = (_ob["type"] == "bullish" and _close_i < _trigger) or \
                       (_ob["type"] == "bearish" and _close_i > _trigger)
                if not _mit:
                    _kept.append(_ob)
                elif trace is not None:
                    trace["mitigations"].append({
                        "ob_bar": _ob["bar"], "ob_type": _ob["type"],
                        "ob_top": _ob["top"], "ob_bottom": _ob["bottom"],
                        "ob_avg": _ob.get("avg"),
                        "mitigated_at_bar": i, "trigger": _trigger,
                        "close_at_mitigation": _close_i,
                    })
            if len(_kept) != len(obs):
                obs[:] = _kept

        # End-of-bar snapshot — next iteration's "[1]" (previous-bar) lookup
        prev_upP_first = upP[0] if upP else None
        prev_dnP_first = dnP[0] if dnP else None

    # ── Surviving OBs (mitigation already applied per-bar above) ──
    # Annotate each surviving OB with touch metadata. No mitigation
    # work here — Pine line 1298-1321 was already replicated inside
    # the bar loop, so obs only contains OBs that survived.
    active = []
    max_vol = max(v[-100:]) if len(v) >= 100 else max(v) if v else 1.0

    for ob in obs:
        _tm = _compute_ob_touch_meta(
            ob, h, l, c, n,
            ob_mitigation=ob_mitigation,
            track_mitigation=False,
        )
        ob["touches"]         = _tm["touches"]
        ob["firstTouchBar"]   = _tm["firstTouchBar"]
        ob["lastTouchBar"]    = _tm["lastTouchBar"]
        ob["untouched"]       = _tm["untouched"]
        ob["onceTouched"]     = _tm["onceTouched"]
        ob["touchSeq"]        = _tm["touchSeq"]
        ob["isVirgin"]        = _tm["isVirgin"]
        ob["currentlyInside"] = _tm["currentlyInside"]
        ob["mitigationBar"]   = None
        active.append(ob)

    return (active if max_ob is None else active[-max_ob:]), _trace


def detect_obs_all(o, h, l, c, v, i_len, s_len, max_ob=20):
    """
    Identical to detect_obs but returns ALL OBs including mitigated ones.
    Uses exact same pivot/BOS logic as detect_obs — only skips the mitigation filter.
    """
    n = len(c)
    ph, pl = detect_pivots(h, l, i_len, i_len)
    obs = []

    upP, upB, upL = [], [], []
    dnP, dnB, dnL = [], [], []
    prev_upP_first = None   # Pine: up.p.first()[1] — end-of-previous-bar value
    prev_dnP_first = None   # Pine: dn.p.first()[1] — end-of-previous-bar value

    start_i = max(i_len * 2 + 2, s_len + 2)

    for i in range(start_i, n):
        if i - i_len >= 0 and ph[i - i_len]:
            upP.insert(0, h[i - i_len])
            upB.insert(0, i - i_len)
            upL.insert(0, h[i - i_len])
        if i - i_len >= 0 and pl[i - i_len]:
            dnP.insert(0, l[i - i_len])
            dnB.insert(0, i - i_len)
            dnL.insert(0, l[i - i_len])

        # Bullish OB — same as detect_obs
        if upP and len(dnL) > 1 and c[i] > upP[0] \
                and prev_upP_first is not None and c[i - 1] <= prev_upP_first:
            pivot_bar    = upB[0] if upB else i - 10
            search_start = max(0, pivot_bar + 1)
            search_end   = i + 1
            if search_end > search_start:
                min_idx = search_start
                for j in range(search_start, search_end):
                    if l[j] < l[min_idx]:
                        min_idx = j
                ob_source = max(0, min_idx - 1)
                hl2_val   = (h[ob_source] + l[ob_source]) / 2.0
                ob_top    = hl2_val
                ob_bottom = l[min_idx]
                ob_avg    = (ob_top + ob_bottom) / 2.0
                total_v   = v[ob_source]
                buy_v     = total_v * 0.6
                sell_v    = total_v - buy_v
                if ob_top > ob_bottom:
                    _new_payload = {
                        "top": ob_top, "bottom": ob_bottom, "avg": ob_avg,
                        "bar": min_idx, "sourceBar": ob_source,
                        "volume": total_v, "buyVolume": buy_v, "sellVolume": sell_v,
                        "type": "bullish",
                    }
                    obs.append(_new_payload)
                    # Pine line 1282-1296: overlap deletion (mode="Previous",
                    # rmP=0 → drop the newly-appended OB)
                    _prev = next((x for x in reversed(obs[:-1]) if x["type"] == "bullish"), None)
                    if _prev is not None and _new_payload["bottom"] < _prev["top"]:
                        obs.pop()
            upP.clear(); upB.clear()

        # Bearish OB — same as detect_obs
        if dnP and len(upL) > 1 and c[i] < dnP[0] \
                and prev_dnP_first is not None and c[i - 1] >= prev_dnP_first:
            pivot_bar    = dnB[0] if dnB else i - 10
            search_start = max(0, pivot_bar + 1)
            search_end   = i + 1
            if search_end > search_start:
                max_idx = search_start
                for j in range(search_start, search_end):
                    if h[j] > h[max_idx]:
                        max_idx = j
                ob_source = max(0, max_idx - 1)
                hl2_val   = (h[ob_source] + l[ob_source]) / 2.0
                ob_top    = h[max_idx]
                ob_bottom = hl2_val
                ob_avg    = (ob_top + ob_bottom) / 2.0
                total_v   = v[ob_source]
                buy_v     = total_v * 0.4
                sell_v    = total_v - buy_v
                if ob_top > ob_bottom:
                    _new_payload = {
                        "top": ob_top, "bottom": ob_bottom, "avg": ob_avg,
                        "bar": max_idx, "sourceBar": ob_source,
                        "volume": total_v, "buyVolume": buy_v, "sellVolume": sell_v,
                        "type": "bearish",
                    }
                    obs.append(_new_payload)
                    # Pine line 1282-1296: overlap deletion (mode="Previous",
                    # rmP=0 → drop the newly-appended OB)
                    _prev = next((x for x in reversed(obs[:-1]) if x["type"] == "bearish"), None)
                    if _prev is not None and _new_payload["top"] > _prev["bottom"]:
                        obs.pop()
            dnP.clear(); dnB.clear()

        # ── Pine line 1298-1321: per-bar mitigation (barstate.isconfirmed) ──
        # detect_obs_all uses Absolute mitigation (bull→ob.bottom, bear→ob.top),
        # matching the touch-meta pass below. Pine deletes mitigated OBs from
        # the array each bar; replicate that here so the next bar's overlap
        # check looks up the correct "previous" same-direction OB.
        if obs:
            _close_i = c[i]
            _kept = []
            for _ob in obs:
                _trigger = _ob["bottom"] if _ob["type"] == "bullish" else _ob["top"]
                _mit = (_ob["type"] == "bullish" and _close_i < _trigger) or \
                       (_ob["type"] == "bearish" and _close_i > _trigger)
                if not _mit:
                    _kept.append(_ob)
            if len(_kept) != len(obs):
                obs[:] = _kept

        # End-of-bar snapshot — next iteration's "[1]" (previous-bar) lookup
        prev_upP_first = upP[0] if upP else None
        prev_dnP_first = dnP[0] if dnP else None

    # Sort by bar descending (most recent first), deduplicate, limit
    seen = set()
    unique = []
    for ob in sorted(obs, key=lambda x: x["bar"], reverse=True):
        if ob["bar"] not in seen:
            seen.add(ob["bar"])
            unique.append(ob)
    unique = unique[:max_ob]

    # ── Phase 1A: touch + mitigation metadata for analytics/backtest ──
    # detect_obs_all() must preserve mitigated OB records. Touches are counted
    # up to and including the mitigation bar (per rule), then counting stops.
    for ob in unique:
        _tm = _compute_ob_touch_meta(
            ob, h, l, c, n,
            ob_mitigation="Absolute",
            track_mitigation=True,
        )
        ob["touches"]         = _tm["touches"]
        ob["firstTouchBar"]   = _tm["firstTouchBar"]
        ob["lastTouchBar"]    = _tm["lastTouchBar"]
        ob["untouched"]       = _tm["untouched"]
        ob["onceTouched"]     = _tm["onceTouched"]
        ob["touchSeq"]        = _tm["touchSeq"]
        ob["isVirgin"]        = _tm["isVirgin"]
        ob["currentlyInside"] = _tm["currentlyInside"]
        ob["mitigated"]       = _tm["mitigated"]
        ob["mitigationBar"]   = _tm["mitigationBar"]

    return unique




def detect_breakers(
    o, h, l, c, v, price, tf,
    i_len=5, s_len=30,
    approach_pct=2.0, max_age=200,
    fvgs=None
):
    """
    Detect Breaker Blocks — mitigated OBs that have flipped direction.

    - Bullish OB broken down (close < ob.bottom) → becomes BEARISH breaker
    - Bearish OB broken up  (close > ob.top)    → becomes BULLISH breaker
    - Breaker is consumed when price crosses zone again after flip
    - Only active breakers (mitigated but not consumed) are returned
    """
    from typing import List, Dict, Any

    # Detect ALL OBs including mitigated — needed for breaker detection
    raw_obs = detect_obs_all(o, h, l, c, v, i_len, s_len, max_ob=20)

    n = len(c)
    breakers = []

    for ob in raw_obs:
        zt       = ob["top"]
        zb       = ob["bottom"]
        is_bull  = ob["type"] == "bullish"
        ob_bar   = int(ob.get("bar", 0))

        mitigated_at  = None
        consumed_at   = None
        mitigated_dir = None  # direction AFTER flip

        # Scan candles after OB formed
        for j in range(ob_bar + 1, n):
            cur = c[j]

            if mitigated_at is None:
                # Check if OB is mitigated (flips to breaker)
                if is_bull and cur < zb:      # Bullish OB broken down → bearish breaker
                    mitigated_at  = j
                    mitigated_dir = "bearish"
                elif not is_bull and cur > zt: # Bearish OB broken up → bullish breaker
                    mitigated_at  = j
                    mitigated_dir = "bullish"
            else:
                # Already a breaker — check if consumed
                if mitigated_dir == "bullish" and cur > zt:
                    consumed_at = j
                    break
                elif mitigated_dir == "bearish" and cur < zb:
                    consumed_at = j
                    break

        # Only keep active breakers (mitigated but not consumed)
        if mitigated_at is None or consumed_at is not None:
            continue

        age = n - 1 - mitigated_at
        if age > max_age:
            continue

        # Determine breaker direction (flipped from original OB)
        breaker_dir = mitigated_dir

        # Distance from current price to breaker zone
        if breaker_dir == "bearish":
            # Bearish breaker: price should be below zone approaching from below
            if price <= zb:
                dist_pct = (zb - price) / max(price, 1e-10) * 100
                state = "inside" if price >= zb else "approaching"
            elif zb <= price <= zt:
                dist_pct = 0.0
                state = "inside"
            else:
                dist_pct = (price - zt) / max(price, 1e-10) * 100
                state = "approaching" if dist_pct <= approach_pct else "far"
        else:
            # Bullish breaker: price approaching from above
            if price >= zt:
                dist_pct = (price - zt) / max(price, 1e-10) * 100
                state = "inside" if price <= zt else "approaching"
            elif zb <= price <= zt:
                dist_pct = 0.0
                state = "inside"
            else:
                dist_pct = (zb - price) / max(price, 1e-10) * 100
                state = "approaching" if dist_pct <= approach_pct else "far"

        # Check FVG overlap
        fvg_overlap = False
        if fvgs:
            for fvg in fvgs:
                # Check if FVG overlaps with breaker zone
                fvg_top = fvg.get("top", 0)
                fvg_bot = fvg.get("bottom", 0)
                if fvg_top > 0 and fvg_bot > 0:
                    overlap = min(fvg_top, zt) - max(fvg_bot, zb)
                    if overlap > 0:
                        fvg_overlap = True
                        break

        breakers.append({
            "type":       breaker_dir,
            "top":        round(zt, 8),
            "bottom":     round(zb, 8),
            "dist":       round(dist_pct, 3),
            "state":      state,
            "age":        age,
            "strength":   round(ob.get("tvObVolumeSharePct") or 0, 1),
            "fvg_overlap": fvg_overlap,
            "zone_str":   f"{zb:.6f} – {zt:.6f}",
        })

    breakers.sort(key=lambda x: x["dist"])
    return breakers[:5]  # max 5 breakers


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


# ── OB touch filter ──────────────────────────────────────────────────────────
# Reads Phase 1A touch metadata only; never recomputes touches and never
# mutates the OB. Returns True when the OB should be considered for alerts.
#
# When useObTouchState is enabled, the OB passes only if its touch count is
# at most obMaxTouches. obMaxTouches=0 keeps virgin OBs only; higher values
# include progressively more retested OBs.
#
# Legacy keys obTouchState / useObVirginApproach / obVirginApproachPct are
# accepted by parse_settings for backward compatibility but no longer affect
# filtering. Normal OB approach / consolidation logic decides proximity.
def filter_ob(ob: Dict[str, Any], price: float, settings: Dict[str, Any]) -> bool:
    if not settings:
        return True

    if not bool(settings.get("useObTouchState", False)):
        return True

    try:
        touches = int(ob.get("touches", 0) or 0)
    except (TypeError, ValueError):
        touches = 0

    try:
        max_touches = int(settings.get("obMaxTouches", 99))
    except (TypeError, ValueError):
        max_touches = 99
    if max_touches < 0:
        max_touches = 0

    if touches > max_touches:
        return False

    return True


def _ob_touch_label(ob: Dict[str, Any]) -> str:
    """Phase 1D: short human-readable OB touch label for alert details."""
    try:
        touches = int(ob.get("touches", 0) or 0)
    except (TypeError, ValueError):
        touches = 0
    if bool(ob.get("isVirgin", touches == 0)) or touches == 0:
        return "VIRGIN"
    return f"Touch: {touches}"


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


# ── Pine-style TV OB volume share settings (matches TradingView OB indicator) ──
_TV_OB_PARITY_SETTINGS: Dict[str, Any] = {
    "showLast":       5,
    "internalOnly":   True,
    "swingOrderBlocks": False,
    "filtering":      "None",
    "mitigation":     "Absolute",
    "positioning":    "Precise",
    "hideOverlap":    True,
    "overlapMode":    "Previous",
    "breakerIncluded": False,
}


def _tv_visible_pool(obs_by_dir: List[Dict[str, Any]], max_ob: int = 5,
                     overlap_effective_zone: bool = False,
                     bearish_effective_bottom_overlap=None,
                     ob_logic_mode: str = "tv_parity_v3") -> List[Dict[str, Any]]:
    """
    Build the Pine-style visible OB pool for a single direction.

    Pine drawVOB (hideOverlap=True, overlapMode=Previous):
      Keep older OBs; hide newer OBs that overlap an already-accepted (older) OB.
      ShowLast=5 is applied AFTER overlap filtering.

      Step 1: Iterate oldest→newest; hide newer OB if it overlaps any accepted older OB.
      Step 2: Apply showLast = keep the most recent max_ob survivors.

    obs_by_dir: all active OBs of ONE direction, oldest-first (detect_obs insertion order).
    Returns the final visible subset, oldest-first, length ≤ max_ob.

    overlap_effective_zone (debug-only, default False preserves production):
      DEPRECATED / WRONG. Collapsed the *new* OB's extreme edge to avg.
      Kept only for diagnostic comparison.
    bearish_effective_bottom_overlap: CORRECT bearish rule — for bearish
      overlap only, the *previous* accepted OB's effective bottom is its avg
      (TV displays the bearish lower boundary at avg); new OB top stays raw,
      bullish stays raw. None inherits from ob_logic_mode; an explicit
      argument overrides.
    ob_logic_mode: "tv_parity_v3" / "tv_parity_v2" both enable the
      confirmed TV-parity overlap rule (v3 inherits v2 here);
      "legacy_baseline" keeps the original raw-overlap behavior.
    """
    if bearish_effective_bottom_overlap is None:
        bearish_effective_bottom_overlap = ob_logic_mode in ("tv_parity_v2", "tv_parity_v3")

    input_count = len(obs_by_dir)

    # Step 1: overlap filter — Pine checks ONLY new OB vs immediately previous accepted OB
    # Pine line 1285-1288: obj.btm.first() < obj.top.get(1) — index 0 vs index 1 only
    accepted: List[Dict[str, Any]] = []
    for ob in obs_by_dir:
        if accepted:
            last = accepted[-1]
            if ob["type"] == "bullish":
                # Bullish stays raw (ETH 4H bull matches TV with raw overlap).
                _lo = ob.get("avg", ob["bottom"]) if overlap_effective_zone else ob["bottom"]
                overlaps = _lo < last["top"]
            elif bearish_effective_bottom_overlap:
                # CORRECT: prev OB effective bottom = its avg; new OB top raw.
                overlaps = ob["top"] > last.get("avg", last["bottom"])
            else:
                _hi = ob.get("avg", ob["top"]) if overlap_effective_zone else ob["top"]
                overlaps = _hi > last["bottom"]
        else:
            overlaps = False
        if not overlaps:
            accepted.append(ob)

    after_overlap_count = len(accepted)

    # Step 2: showLast — keep the most recent max_ob
    visible = accepted[-max_ob:] if len(accepted) > max_ob else list(accepted)
    final_count = len(visible)

    for ob in visible:
        ob["tvObOverlapMode"]        = "previous"
        ob["tvObInputCount"]         = input_count
        ob["tvObAfterOverlapCount"]  = after_overlap_count
        ob["tvObFinalShowLastCount"] = final_count

    return visible


def calculate_tv_ob_volume_share(obs: List[Dict[str, Any]],
                                  pool_name: str = "unknown",
                                  source_pool_count: int = 0) -> None:
    """
    Attach Pine-style TV OB volume share fields to each OB in the visible pool.
    Mutates obs in-place. Caller is responsible for passing the correct visible
    pool (showLast + hideOverlap already applied by _tv_visible_pool).

    Pine reference (drawVOB):
      tV  = sum of obj.cV for visible sequence
      obj.dV = floor((obj.cV / tV) * 100)

    pool_name:         "bullish" or "bearish" — pools must be passed separately.
    source_pool_count: count of all active same-direction OBs before showLast.
    Breaker Blocks must be excluded before calling this function.
    """
    visible = obs
    total_vol = sum(ob.get("volume") or 0 for ob in visible)
    vis_count = len(visible)

    # Build debug snapshot — tvObVolumeSharePct filled in during loop below
    vis_debug = [
        {"direction": pool_name, "bar": ob["bar"],
         "zoneTop": round(ob["top"], 8), "zoneBottom": round(ob["bottom"], 8),
         "volume": ob.get("volume"), "tvObVolumeSharePct": None,
         "touches":         ob.get("touches"),
         "isVirgin":        ob.get("isVirgin"),
         "currentlyInside": ob.get("currentlyInside"),
         "firstTouchBar":   ob.get("firstTouchBar"),
         "lastTouchBar":    ob.get("lastTouchBar"),
         "mitigationBar":   ob.get("mitigationBar"),
         "mitigated":       ob.get("mitigated"),
         "untouched":       ob.get("untouched"),
         "onceTouched":     ob.get("onceTouched"),
         "touchSeq":        ob.get("touchSeq")}
        for ob in visible
    ]

    for idx, ob in enumerate(visible):
        ob_vol = ob.get("volume") or 0
        if total_vol <= 0:
            status   = "zero_total_volume" if total_vol == 0 else "missing_volume"
            tv_share = None
        elif ob_vol <= 0:
            status   = "missing_volume"
            tv_share = None
        else:
            status   = "ok"
            tv_share = int((ob_vol / total_vol) * 100)   # floor for positive = int()

        vis_debug[idx]["tvObVolumeSharePct"] = tv_share

        ob["tvObVolumeSharePct"]                = tv_share
        ob["tvObVolumeShareStatus"]             = status
        ob["tvObFormationVolume"]               = round(ob_vol, 2) if ob_vol else None
        ob["tvObVisibleTotalVolume"]            = round(total_vol, 2)
        ob["tvObVisibleCount"]                  = vis_count
        ob["tvObVolumeShareSource"]             = "pine_visible_active_ob_volume_share"
        ob["tvObVolumeShareFormula"]            = "floor(source_volume / visible_total_volume * 100)"
        ob["tvObParityPool"]                    = pool_name
        ob["tvObParitySeq"]                     = idx
        ob["tvObParitySettings"]                = _TV_OB_PARITY_SETTINGS
        ob["tvObSourcePoolCountBeforeShowLast"] = source_pool_count
        ob["tvObDirectionPoolCount"]            = source_pool_count
        ob["tvObVisiblePoolDebug"]              = vis_debug


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

    obs, _ = detect_obs(o, h, l, c, v, settings["iLen"], settings["sLen"])
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

    # ── TV OB Volume Share — Pine showLast=5 per direction, hideOverlap=Previous ──
    # max_ob=None returns ALL active OBs (no mixed truncation) so each direction
    # gets its own complete pool before showLast=5 + hideOverlap are applied.
    # Alert logic uses the original obs (max_ob=5 mixed) — unchanged.
    obs_tv_src, _ = detect_obs(o, h, l, c, v, settings["iLen"], settings["sLen"], max_ob=None)
    bull_tv_src = [ob for ob in obs_tv_src if ob["type"] == "bullish"]
    bear_tv_src = [ob for ob in obs_tv_src if ob["type"] == "bearish"]
    tv_bull_pool = _tv_visible_pool(bull_tv_src)
    tv_bear_pool = _tv_visible_pool(bear_tv_src)
    calculate_tv_ob_volume_share(tv_bull_pool, pool_name="bullish", source_pool_count=len(bull_tv_src))
    calculate_tv_ob_volume_share(tv_bear_pool, pool_name="bearish", source_pool_count=len(bear_tv_src))

    # Map TV fields onto OBs in the main pool, matched by (direction, formation bar index)
    _tv_by_key   = {(ob["type"], ob["bar"]): ob for ob in tv_bull_pool + tv_bear_pool}
    _tv_src_counts = {"bullish": len(bull_tv_src), "bearish": len(bear_tv_src)}
    _TV_FIELDS = (
        "tvObVolumeSharePct", "tvObVolumeShareStatus", "tvObFormationVolume",
        "tvObVisibleTotalVolume", "tvObVisibleCount", "tvObVolumeShareSource",
        "tvObVolumeShareFormula", "tvObParityPool", "tvObParitySeq", "tvObParitySettings",
        "tvObSourcePoolCountBeforeShowLast", "tvObDirectionPoolCount", "tvObVisiblePoolDebug",
        "tvObOverlapMode", "tvObInputCount", "tvObAfterOverlapCount", "tvObFinalShowLastCount",
    )
    for ob in obs:
        tv_ref = _tv_by_key.get((ob["type"], ob["bar"]))
        if tv_ref:
            for _f in _TV_FIELDS:
                ob[_f] = tv_ref[_f]
        else:
            # Active but hidden by Pine (beyond showLast or overlapped by a more-recent OB)
            ob["tvObVolumeSharePct"]                = None
            ob["tvObVolumeShareStatus"]             = "ob_not_in_tv_visible_pool"
            ob["tvObFormationVolume"]               = ob.get("volume")
            ob["tvObVisibleTotalVolume"]            = None
            ob["tvObVisibleCount"]                  = None
            ob["tvObVolumeShareSource"]             = "pine_visible_active_ob_volume_share"
            ob["tvObVolumeShareFormula"]            = "floor(source_volume / visible_total_volume * 100)"
            ob["tvObParityPool"]                    = ob["type"]
            ob["tvObParitySeq"]                     = None
            ob["tvObParitySettings"]                = _TV_OB_PARITY_SETTINGS
            ob["tvObSourcePoolCountBeforeShowLast"] = _tv_src_counts.get(ob["type"], 0)
            ob["tvObDirectionPoolCount"]            = _tv_src_counts.get(ob["type"], 0)
            ob["tvObVisiblePoolDebug"]              = None

    bullish_obs = [ob for ob in obs if ob["type"] == "bullish"]
    bearish_obs = [ob for ob in obs if ob["type"] == "bearish"]

    # Backward-compatible volumePct + formatted volume label
    for ob in bullish_obs + bearish_obs:
        ob["volumePct"]       = ob["tvObVolumeSharePct"] if ob["tvObVolumeSharePct"] is not None else 0
        ob["volumeFormatted"] = _fmt_ob_vol(ob.get("volume") or 0)

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

    _use_str_filter = settings.get("useObStrengthFilter", False)
    _min_str        = float(settings.get("obMinStrengthPct", 0)) if _use_str_filter else 0.0

    if _use_str_filter:
        bullish_obs_filt = [
            ob for ob in obs if ob["type"] == "bullish"
            and ob.get("tvObVolumeSharePct") is not None
            and ob["tvObVolumeSharePct"] >= _min_str
        ]
        bearish_obs_filt = [
            ob for ob in obs if ob["type"] == "bearish"
            and ob.get("tvObVolumeSharePct") is not None
            and ob["tvObVolumeSharePct"] >= _min_str
        ]
    else:
        bullish_obs_filt = [ob for ob in obs if ob["type"] == "bullish"]
        bearish_obs_filt = [ob for ob in obs if ob["type"] == "bearish"]

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

    # Rank nearest first, then best TV OB %, then best quality, then freshest
    bullish_obs_filt.sort(key=lambda ob: (_ob_dist_from_price(ob, price), -(ob.get("tvObVolumeSharePct") or 0), -ob.get("quality", 0), ob.get("bar", 0)))
    bearish_obs_filt.sort(key=lambda ob: (_ob_dist_from_price(ob, price), -(ob.get("tvObVolumeSharePct") or 0), -ob.get("quality", 0), ob.get("bar", 0)))

    for direction, ob_list in [("bullish", bullish_obs_filt), ("bearish", bearish_obs_filt)]:
        if not ob_list:
            continue

        found_alert = False
        for ob in ob_list:  # ← check ALL OBs, not just nearest
            # Phase 1B: backend touch-state filter (no-op when disabled)
            if not filter_ob(ob, price, settings):
                continue
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
            _tv_share     = ob.get("tvObVolumeSharePct")
            _tv_share_str = f'{_tv_share}%' if _tv_share is not None else '—'

            # Position label
            pos_label = "INSIDE ZONE" if price_in_zone else f"{dist_pct:.2f}% from zone"

            ob_strength = (5 if use_high_prob and ob.get("quality", 0) >= 80 else 3)
            ob_meta_base = {
                # ── TradingView-style OB volume share ──
                "tvObVolumeSharePct":     ob.get("tvObVolumeSharePct"),
                "tvObVolumeShareStatus":  ob.get("tvObVolumeShareStatus"),
                "tvObFormationVolume":    ob.get("tvObFormationVolume"),
                "tvObVisibleTotalVolume": ob.get("tvObVisibleTotalVolume"),
                "tvObVisibleCount":       ob.get("tvObVisibleCount"),
                "tvObVolumeShareSource":  ob.get("tvObVolumeShareSource"),
                "tvObVolumeShareFormula": ob.get("tvObVolumeShareFormula"),
                "tvObParityPool":         ob.get("tvObParityPool"),
                "tvObParitySettings":                ob.get("tvObParitySettings"),
                "tvObSourcePoolCountBeforeShowLast": ob.get("tvObSourcePoolCountBeforeShowLast"),
                "tvObDirectionPoolCount":            ob.get("tvObDirectionPoolCount"),
                "tvObVisiblePoolDebug":              ob.get("tvObVisiblePoolDebug"),
                # ── Zone & proximity ──
                "obDistPct": round(dist_pct, 3),
                "obQuality": ob.get("quality", 0),
                "obQualityLabel": ob.get("qualityLabel", "Weak"),
                "obTop":    round(ob["top"], 8),
                "obBottom": round(ob["bottom"], 8),
                # ── Orderflow ──
                "absorption":     ob.get("absorption", "NONE"),
                "absorptionStr":  ob.get("absorptionStr", "None"),
                "absorptionPass": ob.get("absorptionPass", False),
                "delta":          ob.get("delta", 0.0),
                "oiSignal":       ob.get("oiSignal", "unknown"),
                "fundingContext": ob.get("fundingContext", "unknown"),
                "ofSummary":      ob.get("ofSummary", ""),
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
                                   f'Order Block %: {_tv_share_str}{quality_str} | '
                                   f'Zone: {fmt_price(zone_bottom)} – {fmt_price(zone_top)}'
                                   + (f' | {ob["ofSummary"]}' if ob.get("ofSummary") else '')
                                   + f' | {_ob_touch_label(ob)}'),
                        "strength": ob_strength,
                        "meta": {**ob_meta_base, "consolCandles": consecutive, "obState": "inside"},
                    })
                    break
                # Not enough consol candles — fall through to check OB_APPROACH below

            # OB_APPROACH: price approaching from correct side within threshold
            # Also fires when price is inside zone (dist_pct = 0)
            correct_side = ((direction == "bullish" and price >= zone_bottom) or
                            (direction == "bearish" and price <= zone_top))
            if 0 < dist_pct <= ob_approach_pct and correct_side:
                alerts.append({
                    "setup": "OB_APPROACH",
                    "direction": direction,
                    "timeframe": tf,
                    "detail": (f'Approaching {direction} OB | Dist: {dist_pct:.2f}% | '
                               f'Order Block %: {_tv_share_str}{quality_str} | '
                               f'Zone: {fmt_price(zone_bottom)} – {fmt_price(zone_top)}'
                               + (f' | {ob["ofSummary"]}' if ob.get("ofSummary") else '')
                               + f' | {_ob_touch_label(ob)}'),
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

    # ── Breaker Block detection ──
    # MUST run before early return so breakers can be the ONLY signal
    if settings.get("useBreakerModule"):
        brk_approach = float(settings.get("breakerApproachPct", 2.0))
        brk_max_age  = int(settings.get("breakerMaxAge", 200))
        brk_req_fvg  = bool(settings.get("breakerRequireFvg", False))
        brk_fvgs     = detect_fvgs(o, h, l, c, v, tf)

        breakers = detect_breakers(
            o, h, l, c, v, price, tf,
            i_len=settings["iLen"],
            s_len=settings["sLen"],
            approach_pct=brk_approach,
            max_age=brk_max_age,
            fvgs=brk_fvgs,
        )

        for brk in breakers:
            if brk_req_fvg and not brk["fvg_overlap"]:
                continue
            if brk["state"] not in ("approaching", "inside"):
                continue
            high_prob  = brk["fvg_overlap"]
            setup_name = "BREAKER_INSIDE" if brk["state"] == "inside" else "BREAKER_APPROACH"
            alerts.append({
                "setup":     setup_name,
                "direction": brk["type"],
                "timeframe": tf,
                "strength":  8 if high_prob else 6,
                "detail": (
                    f'{"Bullish" if brk["type"] == "bullish" else "Bearish"} Breaker | '
                    + ("Inside zone" if brk["state"] == "inside" else f'Dist: {brk["dist"]:.2f}%') + ' | '
                    f'Zone: {brk["zone_str"]} | '
                    f'Age: {brk["age"]} bars | '
                    f'Strength: {brk["strength"]}%'
                    + (' | ⚡ HIGH PROB' if high_prob else '')
                ),
                "meta": {
                    "breakerTop":      brk["top"],
                    "breakerBottom":   brk["bottom"],
                    "breakerDir":      brk["type"],
                    "breakerDist":     brk["dist"],
                    "breakerState":    brk["state"],
                    "breakerAge":      brk["age"],
                    "breakerStrength": brk["strength"],
                    "fvgOverlap":      brk["fvg_overlap"],
                    "zoneStr":         brk["zone_str"],
                    "highProb":        high_prob,
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

    if "BREAKER_INSIDE" in alert_setups:
        score += 22
    elif "BREAKER_APPROACH" in alert_setups:
        score += 14

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
        elif "BREAKER" in a["setup"]:
            signal_types.add("BREAKER")

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

    # ── Row extras: RR (TP1/SL) · Age · Zone size (USD) · Confidence breakdown ──
    def _tf_minutes(tfs):
        try:
            num = int("".join(ch for ch in str(tfs) if ch.isdigit()) or 1)
            unit = ("".join(ch for ch in str(tfs) if ch.isalpha()).lower() or "m")
        except Exception:
            return 60
        return num * {"m": 1, "h": 60, "d": 1440, "w": 10080}.get(unit, 60)

    def _fmt_age(bars):
        if bars is None:
            return "—"
        try:
            mins = max(0, int(bars)) * _tf_minutes(tf)
        except Exception:
            return "—"
        if mins < 60:
            return f"{mins}m"
        if mins < 1440:
            hh, mm = divmod(mins, 60)
            return f"{hh}h {mm:02d}m" if mm else f"{hh}h"
        dd, rem = divmod(mins, 1440)
        hh = rem // 60
        return f"{dd}d {hh:02d}h" if hh else f"{dd}d"

    def _fmt_usd(val):
        try:
            val = float(val)
        except Exception:
            return None
        a = abs(val)
        if a >= 1e9:
            return f"~{round(val / 1e9, 2)}B"
        if a >= 1e6:
            return f"~{round(val / 1e6, 2)}M"
        if a >= 1e3:
            return f"~{round(val / 1e3, 2)}K"
        return f"~{round(val, 2)}"

    def _row_extras():
        ta = alerts[0]
        meta = ta.get("meta", {}) or {}
        setup = ta.get("setup", "")
        is_bull = ta.get("direction", "") == "bullish"
        z_hi = z_lo = None
        if setup.startswith("OB"):
            z_hi, z_lo = meta.get("obTop"), meta.get("obBottom")
        elif setup == "FVG":
            z_hi, z_lo = meta.get("fvgTop"), meta.get("fvgBottom")
        elif setup.startswith("BREAKER"):
            z_hi, z_lo = meta.get("breakerTop"), meta.get("breakerBottom")
        elif setup.startswith("FIB"):
            z_hi = z_lo = meta.get("fibPrice")
        try:
            if z_hi is not None and z_lo is not None and z_hi < z_lo:
                z_hi, z_lo = z_lo, z_hi
        except Exception:
            pass

        age_bars = None
        ob_vol = None
        ob_extras = {}
        if setup.startswith("OB") and z_hi is not None:
            best, bestd = None, None
            for ob in obs:
                d = abs(ob["top"] - z_hi) + abs(ob["bottom"] - (z_lo if z_lo is not None else z_hi))
                if bestd is None or d < bestd:
                    bestd, best = d, ob
            if best is not None:
                age_bars = best.get("age")
                ob_vol = best.get("volume")
                # Expose touch / virgin / bar fields so the desktop Zone Details
                # panel can read them (Queue 12).
                ob_extras = {
                    "obTouches":      best.get("touches"),
                    "isVirginOb":     best.get("isVirgin"),
                    "obBar":          best.get("bar"),
                    "obAgeBars":      best.get("age"),
                    "obMitigated":    best.get("mitigated"),
                    "obUntouched":    best.get("untouched"),
                    "obOnceTouched":  best.get("onceTouched"),
                    "obCurrentlyInside": best.get("currentlyInside"),
                }
        elif setup == "FVG":
            age_bars = meta.get("fvgAge")
        elif setup.startswith("BREAKER"):
            age_bars = meta.get("breakerAge")
        elif setup.startswith("FIB"):
            age_bars = meta.get("barsCount")

        zone_size = None
        if ob_vol is not None:
            zone_size = float(ob_vol) * price
        elif meta.get("tvObFormationVolume"):
            try:
                zone_size = float(meta["tvObFormationVolume"]) * price
            except Exception:
                zone_size = None
        else:
            try:
                zone_size = sum(v[-3:]) * price
            except Exception:
                zone_size = None

        rr = entry = stop = tp1 = None
        try:
            entry = price
            buf = 0.25 * current_atr
            lookN = min(60, n - 1) if n > 2 else n
            win_h = h[-lookN:-1] if lookN > 1 else h[-lookN:]
            win_l = l[-lookN:-1] if lookN > 1 else l[-lookN:]
            zh = z_hi if z_hi is not None else entry
            zl = z_lo if z_lo is not None else entry
            if is_bull:
                stop = zl - buf
                if stop >= entry:
                    stop = entry - max(current_atr, entry * 1e-4)
                sw = max(win_h) if win_h else entry
                tp1 = sw if sw > entry else entry + 2.0 * (entry - stop)
                risk, reward = entry - stop, tp1 - entry
            else:
                stop = zh + buf
                if stop <= entry:
                    stop = entry + max(current_atr, entry * 1e-4)
                sw = min(win_l) if win_l else entry
                tp1 = sw if sw < entry else entry - 2.0 * (stop - entry)
                risk, reward = stop - entry, entry - tp1
            if risk > 0 and reward > 0:
                rr = round(max(0.1, min(9.99, reward / risk)), 2)
        except Exception:
            rr = None

        dsign = 1 if is_bull else -1
        htf = 50
        htf += 20 if itrend == dsign else (-15 if itrend == -dsign else 0)
        htf += 20 if trend == dsign else (-15 if trend == -dsign else 0)
        if (is_bull and current_rsi > 50) or ((not is_bull) and current_rsi < 50):
            htf += 10
        htf = int(max(0, min(100, htf)))

        if meta.get("obQuality") is not None:
            zq = float(meta["obQuality"])
        elif meta.get("legScore") is not None:
            ls = float(meta["legScore"])
            zq = ls * 100.0 if ls <= 1 else ls
        else:
            zq = (ta.get("strength", 1) / 8.0) * 100.0
        zq = int(max(0, min(100, round(zq))))

        sig = set()
        for a in alerts:
            for k in ("FIB", "OB", "FVG", "RSI", "BREAKER"):
                if k in a.get("setup", ""):
                    sig.add(k)
        sc = 35 + 22 * max(0, len(sig) - 1)
        if meta.get("fvgConfluence") or meta.get("fvgOverlap"):
            sc += 15
        if meta.get("highProb"):
            sc += 12
        if meta.get("absorptionPass"):
            sc += 10
        if meta.get("fvgUntouched"):
            sc += 8
        sc = int(max(0, min(100, sc)))

        try:
            roc = ((c[-1] - c[-6]) / c[-6]) * 100.0 if len(c) > 6 and c[-6] else 0.0
        except Exception:
            roc = 0.0
        mo = (50 + (current_rsi - 50) * 1.5 + roc * 4) if is_bull else (50 + (50 - current_rsi) * 1.5 - roc * 4)
        mo = int(max(0, min(100, round(mo))))

        # ── Volume sub-score (Task 7): real relative volume — never default to 50 ──
        vol_available = True
        relv = None
        try:
            if not v or len(v) < 2 or v[-1] is None:
                vol_available = False
            else:
                base = sum(v[-21:-1]) / 20.0 if len(v) > 21 else (sum(v) / max(1, len(v)))
                relv = (v[-1] / base) if base else None
                if relv is None:
                    vol_available = False
        except Exception:
            vol_available = False

        # ── strict {score, status, reason} confidence breakdown (Task 7) ──
        def _cb_status(s):
            if s is None:
                return "unavailable"
            if s >= 75:
                return "pass"
            if s >= 55:
                return "medium"
            if s >= 35:
                return "weak"
            return "fail"

        def _cell(s, reason):
            return {"score": (int(s) if s is not None else None), "status": _cb_status(s), "reason": reason}

        def _dirw(x):
            return "up" if x > 0 else ("down" if x < 0 else "flat")

        _setdir = "bullish" if is_bull else "bearish"
        htf_reason = "HTF trend {}, internal trend {} vs {} setup; RSI {}".format(
            _dirw(trend), _dirw(itrend), _setdir, int(round(current_rsi)))

        if meta.get("obQuality") is not None:
            zq_reason = "OB quality {}{}".format(
                int(round(float(meta["obQuality"]))),
                (" (" + str(meta.get("obQualityLabel")) + ")") if meta.get("obQualityLabel") else "")
        elif meta.get("legScore") is not None:
            zq_reason = "Fib leg score {}".format(round(float(meta["legScore"]), 2))
        else:
            zq_reason = "Derived from signal strength (no OB/Fib quality available)"

        sc_reason = "{} confluent signal(s): {}".format(len(sig), ", ".join(sorted(sig)) if sig else "single")
        _sc_extra = []
        if meta.get("fvgConfluence") or meta.get("fvgOverlap"):
            _sc_extra.append("FVG confluence")
        if meta.get("highProb"):
            _sc_extra.append("high-prob filter")
        if meta.get("absorptionPass"):
            _sc_extra.append("absorption")
        if meta.get("fvgUntouched"):
            _sc_extra.append("untouched FVG")
        if _sc_extra:
            sc_reason += " + " + ", ".join(_sc_extra)

        mo_reason = "RSI {}, 5-bar ROC {:.2f}% ({} context)".format(int(round(current_rsi)), roc, _setdir)

        if vol_available and relv is not None:
            vol = int(max(0, min(100, round(50 + (relv - 1.0) * 40))))
            vol_cell = _cell(vol, "Relative volume {:.2f}x vs 20-bar average".format(relv))
        else:
            vol_cell = {"score": None, "status": "unavailable", "reason": "Volume data not available"}

        # Inject OB extras (touches / virgin / bar / age) into the topAlert
        # meta so the desktop Zone Details panel can show real values.
        if ob_extras:
            try:
                ta.setdefault("meta", {})
                for _k, _v in ob_extras.items():
                    if _v is not None and _k not in ta["meta"]:
                        ta["meta"][_k] = _v
            except Exception:
                pass

        return {
            "rr": rr,
            "entry": round(entry, 8) if entry is not None else None,
            "stop": round(stop, 8) if stop is not None else None,
            "tp1": round(tp1, 8) if tp1 is not None else None,
            "ageText": _fmt_age(age_bars),
            "ageBars": age_bars,
            "zoneSizeUsd": zone_size,
            "zoneSizeText": _fmt_usd(zone_size) if zone_size else None,
            "confBreakdown": {
                "htfAlignment":        _cell(htf, htf_reason),
                "zoneQuality":         _cell(zq, zq_reason),
                "structureConfluence": _cell(sc, sc_reason),
                "momentum":            _cell(mo, mo_reason),
                "volume":              vol_cell,
            },
        }

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
        **_row_extras(),
    }


# ============================================================
# Binance / routes
# ============================================================


# ═══════════════════════════════════════════════════
# MULTI-EXCHANGE API FUNCTIONS
# ═══════════════════════════════════════════════════

def get_pairs_bybit(market: str = "perpetual") -> List[Dict[str, Any]]:
    """Fetch USDT pairs from Bybit"""
    cache = EXCHANGE_PAIR_CACHE["bybit"]
    if time.time() - cache["ts"] < 120 and cache["pairs"].get(market):
        return cache["pairs"][market]
    try:
        category = "linear" if market == "perpetual" else "spot"
        r = req.get(
            f"{BYBIT_PERP_API}/tickers",
            params={"category": category},
            timeout=15
        )
        if r.status_code != 200:
            return []
        data = r.json().get("result", {}).get("list", [])
        pairs = []
        stables = {"BUSDUSDT","USDCUSDT","DAIUSDT","TUSDUSDT","FDUSDUSDT"}
        for t in data:
            sym = t.get("symbol","")
            if not sym.endswith("USDT") or sym in stables:
                continue
            vol = safe_float(t.get("turnover24h", 0))
            price = safe_float(t.get("lastPrice", 0))
            if vol < 500_000 or price <= 0:
                continue
            pairs.append({
                "symbol": sym,
                "price": price,
                "changePct": safe_float(t.get("price24hPcnt", 0)) * 100,
                "quoteVolume": vol,
                "volume": safe_float(t.get("volume24h", 0)),
            })
        pairs.sort(key=lambda x: x["quoteVolume"], reverse=True)
        cache["ts"] = time.time()
        cache["pairs"][market] = pairs
        print(f"[Bybit] get_pairs {market}: {len(pairs)} pairs")
        return pairs
    except Exception as e:
        print(f"[Bybit] get_pairs error: {e}")
        return cache["pairs"].get(market, [])


def get_klines_bybit(symbol: str, interval: str, limit: int = 300, market: str = "perpetual") -> List[Dict[str, float]]:
    """Fetch OHLCV candles from Bybit"""
    try:
        category = "linear" if market == "perpetual" else "spot"
        iv = INTERVAL_MAP["bybit"].get(interval, "60")
        r = req.get(
            f"{BYBIT_PERP_API}/kline",
            params={"category": category, "symbol": symbol, "interval": iv, "limit": min(limit, 200)},
            timeout=15
        )
        if r.status_code != 200:
            return []
        data = r.json().get("result", {}).get("list", [])
        # Bybit returns newest first — reverse
        data = list(reversed(data))
        return [{
            "openTime": int(k[0]),
            "open":  float(k[1]),
            "high":  float(k[2]),
            "low":   float(k[3]),
            "close": float(k[4]),
            "volume":float(k[5]),
        } for k in data]
    except Exception as e:
        print(f"[Bybit] get_klines {symbol} error: {e}")
        return []


def get_pairs_okx(market: str = "perpetual") -> List[Dict[str, Any]]:
    """Fetch USDT pairs from OKX"""
    cache = EXCHANGE_PAIR_CACHE["okx"]
    if time.time() - cache["ts"] < 120 and cache["pairs"].get(market):
        return cache["pairs"][market]
    try:
        inst_type = "SWAP" if market == "perpetual" else "SPOT"
        r = req.get(
            f"{OKX_PERP_API}/tickers",
            params={"instType": inst_type},
            timeout=15
        )
        if r.status_code != 200:
            return []
        data = r.json().get("data", [])
        pairs = []
        for t in data:
            inst_id = t.get("instId","")
            # SWAP: BTC-USDT-SWAP → BTCUSDT, SPOT: BTC-USDT → BTCUSDT
            if market == "perpetual":
                if not inst_id.endswith("-USDT-SWAP"):
                    continue
                sym = inst_id.replace("-USDT-SWAP","") + "USDT"
            else:
                if not inst_id.endswith("-USDT"):
                    continue
                sym = inst_id.replace("-","")
            vol = safe_float(t.get("volCcy24h", 0))
            price = safe_float(t.get("last", 0))
            if vol < 500_000 or price <= 0:
                continue
            change_pct = 0.0
            try:
                open24 = safe_float(t.get("open24h", 0))
                if open24 > 0:
                    change_pct = ((price - open24) / open24) * 100
            except: pass
            pairs.append({
                "symbol": sym,
                "instId": inst_id,  # keep for klines
                "price": price,
                "changePct": change_pct,
                "quoteVolume": vol,
                "volume": safe_float(t.get("vol24h", 0)),
            })
        pairs.sort(key=lambda x: x["quoteVolume"], reverse=True)
        cache["ts"] = time.time()
        cache["pairs"][market] = pairs
        print(f"[OKX] get_pairs {market}: {len(pairs)} pairs")
        return pairs
    except Exception as e:
        print(f"[OKX] get_pairs error: {e}")
        return cache["pairs"].get(market, [])


def get_klines_okx(symbol: str, interval: str, limit: int = 300, market: str = "perpetual") -> List[Dict[str, float]]:
    """Fetch OHLCV candles from OKX"""
    try:
        iv = INTERVAL_MAP["okx"].get(interval, "1H")
        # Convert symbol back to OKX format
        if market == "perpetual":
            base = symbol.replace("USDT","")
            inst_id = f"{base}-USDT-SWAP"
        else:
            base = symbol.replace("USDT","")
            inst_id = f"{base}-USDT"
        r = req.get(
            f"{OKX_PERP_API}/candles",
            params={"instId": inst_id, "bar": iv, "limit": min(limit, 300)},
            timeout=15
        )
        if r.status_code != 200:
            return []
        data = r.json().get("data", [])
        data = list(reversed(data))
        return [{
            "openTime": int(k[0]),
            "open":  float(k[1]),
            "high":  float(k[2]),
            "low":   float(k[3]),
            "close": float(k[4]),
            "volume":float(k[5]),
        } for k in data]
    except Exception as e:
        print(f"[OKX] get_klines {symbol} error: {e}")
        return []


def get_pairs_mexc(market: str = "perpetual") -> List[Dict[str, Any]]:
    """Fetch USDT pairs from MEXC"""
    cache = EXCHANGE_PAIR_CACHE["mexc"]
    if time.time() - cache["ts"] < 120 and cache["pairs"].get(market):
        return cache["pairs"][market]
    try:
        if market == "perpetual":
            r = req.get(f"{MEXC_PERP_API}/ticker", timeout=15)
            if r.status_code != 200:
                return []
            data = r.json().get("data", [])
            pairs = []
            for t in data:
                sym = t.get("symbol","").replace("_","")
                if not sym.endswith("USDT"):
                    continue
                vol = safe_float(t.get("amount24", 0))
                price = safe_float(t.get("lastPrice", 0))
                if vol < 500_000 or price <= 0:
                    continue
                pairs.append({
                    "symbol": sym,
                    "price": price,
                    "changePct": safe_float(t.get("riseFallRate", 0)) * 100,
                    "quoteVolume": vol,
                    "volume": safe_float(t.get("volume24", 0)),
                })
        else:
            r = req.get(f"{MEXC_SPOT_API}/ticker/24hr", timeout=15)
            if r.status_code != 200:
                return []
            data = r.json()
            pairs = []
            for t in data:
                sym = t.get("symbol","")
                if not sym.endswith("USDT"):
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
        cache["ts"] = time.time()
        cache["pairs"][market] = pairs
        print(f"[MEXC] get_pairs {market}: {len(pairs)} pairs")
        return pairs
    except Exception as e:
        print(f"[MEXC] get_pairs error: {e}")
        return cache["pairs"].get(market, [])


def get_klines_mexc(symbol: str, interval: str, limit: int = 300, market: str = "perpetual") -> List[Dict[str, float]]:
    """Fetch OHLCV candles from MEXC"""
    try:
        iv = INTERVAL_MAP["mexc"].get(interval, "Min60")
        if market == "perpetual":
            mx_sym = symbol.replace("USDT","_USDT")
            r = req.get(
                f"{MEXC_PERP_API}/kline",
                params={"symbol": mx_sym, "interval": iv, "limit": min(limit, 2000)},
                timeout=15
            )
            if r.status_code != 200:
                return []
            data = r.json().get("data", {})
            times = data.get("time", [])
            opens  = data.get("open", [])
            highs  = data.get("high", [])
            lows   = data.get("low", [])
            closes = data.get("close", [])
            vols   = data.get("vol", [])
            return [{
                "openTime": int(times[i]) * 1000 if i < len(times) else 0,
                "open":  float(opens[i])  if i < len(opens)  else 0,
                "high":  float(highs[i])  if i < len(highs)  else 0,
                "low":   float(lows[i])   if i < len(lows)   else 0,
                "close": float(closes[i]) if i < len(closes) else 0,
                "volume":float(vols[i])   if i < len(vols)   else 0,
            } for i in range(len(times))]
        else:
            r = req.get(
                f"{MEXC_SPOT_API}/klines",
                params={"symbol": symbol, "interval": iv, "limit": min(limit, 1000)},
                timeout=15
            )
            if r.status_code != 200:
                return []
            data = r.json()
            return [{
                "openTime": int(k[0]),
                "open":  float(k[1]),
                "high":  float(k[2]),
                "low":   float(k[3]),
                "close": float(k[4]),
                "volume":float(k[5]),
            } for k in data]
    except Exception as e:
        print(f"[MEXC] get_klines {symbol} error: {e}")
        return []


def get_pairs_exchange(exchange: str, market: str = "perpetual") -> List[Dict[str, Any]]:
    """Universal get_pairs — routes to correct exchange"""
    exchange = (exchange or "binance").lower()
    if exchange == "bybit":
        return get_pairs_bybit(market)
    elif exchange == "okx":
        return get_pairs_okx(market)
    elif exchange == "mexc":
        return get_pairs_mexc(market)
    else:
        return get_pairs(market)  # Binance (default)


def get_klines_exchange(symbol: str, interval: str, limit: int = 300,
                        market: str = "perpetual", exchange: str = "binance") -> List[Dict[str, float]]:
    """Universal get_klines — routes to correct exchange.

    For Binance: if limit > 1500, automatically paginates via
    get_binance_klines_paginated_latest(). get_klines() already handles this
    internally, so the routing here is for clarity.
    Non-Binance exchanges are not paginated (live scanner caps at 1500).
    """
    exchange = (exchange or "binance").lower()
    if exchange == "bybit":
        result = get_klines_bybit(symbol, interval, limit, market)
    elif exchange == "okx":
        result = get_klines_okx(symbol, interval, limit, market)
    elif exchange == "mexc":
        result = get_klines_mexc(symbol, interval, limit, market)
    else:
        # get_klines() auto-paginates when limit > 1500
        result = get_klines(symbol, interval, limit, market)

    # Fallback to Binance if exchange returns empty
    if not result:
        print(f"[{exchange}] klines empty for {symbol}, falling back to Binance")
        result = get_klines(symbol, interval, limit, market)
    return result


def _scan_kline_limit() -> int:
    """Shared candle-fetch limit for normal scanner / watchlist analysis.

    Default 1500 — needed for OB-percentage parity with TradingView and with
    /admin/debug/ob-tv-parity (which historically used 1500 candles too).
    Override with SCAN_KLINE_LIMIT env var. Clamped to [300, 1500].

    Used ONLY by scanner code paths. Admin debug routes pass their own
    `kline_limit` query param and the backtest uses get_klines_exchange_window;
    neither path goes through this helper.
    """
    try:
        v = int(os.environ.get("SCAN_KLINE_LIMIT", "1500"))
    except (TypeError, ValueError):
        v = 1500
    return max(300, min(v, 1500))


def get_klines_exchange_window(symbol: str, interval: str,
                               start_ms: int, end_ms: int,
                               market: str = "perpetual",
                               exchange: str = "binance") -> List[Dict[str, float]]:
    """Fetch OHLCV candles in the time range [start_ms, end_ms] (by bar open).

    Backtest-only helper. Do NOT use in scanner code paths — the scanner is
    locked to the latest-N fetch (`get_klines_exchange`). This helper exists
    so backtest_ob.py can grab a candle slice covering both the pre-signal
    touch-count window AND the post-signal result-replay window in one
    request set, regardless of how old the signal is.

    Uses Binance Futures `/fapi/v1/klines` with `startTime`/`endTime`/`limit`
    (up to 1500 candles per call), paginating until the window is covered.
    Falls back to the geo-safe spot mirror with the same parameters. For
    non-Binance `exchange` tags, falls back to Binance too — most pairs share
    OHLC closely across exchanges for backtest purposes, and the historical
    routes for Bybit/OKX/MEXC do not expose a uniform time-range API today.
    """
    try:
        start_ms = int(start_ms)
        end_ms   = int(end_ms)
    except (TypeError, ValueError):
        return []
    if end_ms <= start_ms:
        return []

    def _parse(data):
        out = []
        for k in data or []:
            try:
                out.append({
                    "openTime": int(k[0]),
                    "open":     float(k[1]),
                    "high":     float(k[2]),
                    "low":      float(k[3]),
                    "close":    float(k[4]),
                    "volume":   float(k[5]),
                })
            except (TypeError, ValueError, IndexError):
                continue
        return out

    def _fapi(start: int, end: int, limit: int):
        try:
            r = req.get(
                f"{BINANCE_FUTURES_API}/fapi/v1/klines",
                params={"symbol": symbol, "interval": interval,
                        "startTime": start, "endTime": end, "limit": limit},
                timeout=15,
            )
            if r.status_code == 200:
                update_api_weight("binance", r)
                return r.json()
        except Exception:
            pass
        return None

    def _spot(start: int, end: int, limit: int):
        try:
            r = req.get(
                f"{SPOT_API}/api/v3/klines",
                params={"symbol": symbol, "interval": interval,
                        "startTime": start, "endTime": end, "limit": limit},
                timeout=20,
            )
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
        return None

    BINANCE_MAX = 1500
    SPOT_MAX    = 1000

    out:    List[Dict[str, float]] = []
    cursor                          = start_ms
    safety                          = 12
    seen_open_times                 = set()

    while cursor < end_ms and safety > 0:
        safety -= 1
        raw          = _fapi(cursor, end_ms, BINANCE_MAX)
        actual_limit = BINANCE_MAX  # track per-source cap for break check
        if raw is None or not raw:
            raw          = _spot(cursor, end_ms, SPOT_MAX)
            actual_limit = SPOT_MAX
        parsed = _parse(raw)
        if not parsed:
            break

        new_rows = [r for r in parsed if r["openTime"] not in seen_open_times]
        if not new_rows:
            break
        for r in new_rows:
            seen_open_times.add(r["openTime"])
        out.extend(new_rows)

        last_open = new_rows[-1]["openTime"]
        # Use actual_limit (not BINANCE_MAX) so spot fallback doesn't stop
        # after its first 1000-candle batch due to len(parsed) < 1500.
        if len(parsed) < actual_limit or last_open >= end_ms:
            break
        cursor = last_open + 1

    out.sort(key=lambda r: r["openTime"])
    return out

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
                update_api_weight("binance", r)
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

    If limit > 1500 (Binance hard cap per request), automatically paginates
    backward via get_binance_klines_paginated_latest(). This lets debug/backtest
    routes request 4000-10000 candles without any per-site changes.
    Live scanner always stays ≤1500 via _scan_kline_limit(), so this path is
    only hit from the debug route.
    """
    try:
        requested = int(limit)
    except (TypeError, ValueError):
        requested = 300

    BINANCE_FUTURES_MAX = 1500
    if requested > BINANCE_FUTURES_MAX:
        return get_binance_klines_paginated_latest(symbol, interval, requested, market)

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
            params={"symbol": symbol, "interval": interval, "limit": requested},
            timeout=15,
        )
        if r.status_code == 200:
            update_api_weight("binance", r)
            return _parse(r.json())
    except Exception:
        pass

    # ── Fallback: geo-safe spot mirror ──
    url = f"{SPOT_API}/api/v3/klines?symbol={symbol}&interval={interval}&limit={requested}"
    data = req.get(url, timeout=20).json()
    return _parse(data)


def get_binance_klines_paginated_latest(symbol: str, interval: str,
                                        total_limit: int = 1500,
                                        market: str = "perpetual") -> List[Dict[str, float]]:
    """Fetch the latest N Binance candles by paginating backward with endTime.

    Binance hard caps:
      - Futures /fapi/v1/klines: 1500 candles per request
      - Spot    /api/v3/klines:  1000 candles per request

    Supports total_limit up to 10000. Each batch is fetched at the correct
    per-source limit so we never exceed the API cap. Deduplicates by openTime,
    returns candles sorted oldest→newest.

    Used only by debug/backtest paths — the live scanner stays ≤1500 via
    _scan_kline_limit() and never calls this function.
    """
    FUTURES_MAX = 1500
    SPOT_MAX    = 1000

    try:
        target = max(1, min(int(total_limit), 10000))
    except (TypeError, ValueError):
        target = 300

    def _parse(data):
        if not isinstance(data, list):
            return []
        out = []
        for k in data:
            try:
                out.append({
                    "openTime": int(k[0]),
                    "open":     float(k[1]),
                    "high":     float(k[2]),
                    "low":      float(k[3]),
                    "close":    float(k[4]),
                    "volume":   float(k[5]),
                })
            except (TypeError, ValueError, IndexError):
                pass
        return out

    collected: List[Dict[str, float]] = []
    seen_open_times: set               = set()
    end_time_ms: Optional[int]         = None
    batches                            = 0
    source                             = "futures"
    import math as _math
    safety = _math.ceil(target / SPOT_MAX) + 5  # worst-case spot batches + margin

    while len(collected) < target and safety > 0:
        safety   -= 1
        remaining = target - len(collected)

        # ── Try Futures ──────────────────────────────────────────────────────
        futures_limit = min(FUTURES_MAX, remaining)
        fparams: Dict[str, Any] = {
            "symbol": symbol, "interval": interval, "limit": futures_limit,
        }
        if end_time_ms is not None:
            fparams["endTime"] = end_time_ms

        batch: List[Dict[str, float]] = []
        actual_limit = futures_limit
        try:
            r = req.get(
                f"{BINANCE_FUTURES_API}/fapi/v1/klines",
                params=fparams, timeout=15,
            )
            if r.status_code == 200:
                update_api_weight("binance", r)
                batch  = _parse(r.json())
                source = "futures"
        except Exception:
            pass

        # ── Spot fallback with correct spot limit ─────────────────────────
        if not batch:
            spot_limit = min(SPOT_MAX, remaining)
            sparams: Dict[str, Any] = {
                "symbol": symbol, "interval": interval, "limit": spot_limit,
            }
            if end_time_ms is not None:
                sparams["endTime"] = end_time_ms
            actual_limit = spot_limit
            try:
                r = req.get(f"{SPOT_API}/api/v3/klines", params=sparams, timeout=20)
                if r.status_code == 200:
                    batch  = _parse(r.json())
                    source = "spot_fallback"
            except Exception:
                pass

        if not batch:
            break

        batches  += 1
        new_rows  = [row for row in batch if row["openTime"] not in seen_open_times]
        if not new_rows:
            break
        for row in new_rows:
            seen_open_times.add(row["openTime"])
        collected.extend(new_rows)

        oldest_open = min(row["openTime"] for row in new_rows)
        end_time_ms = oldest_open - 1

        # If the exchange returned fewer candles than we asked for, we've
        # reached the beginning of its history — stop paginating.
        if len(batch) < actual_limit:
            break

        time.sleep(0.1)

    collected.sort(key=lambda x: x["openTime"])
    if len(collected) > target:
        collected = collected[-target:]

    print(
        f"[KL-PAGINATION] {symbol} {interval} requested={target} "
        f"fetched={len(collected)} batches={batches} source={source}"
    )
    return collected


# Keep old name as a thin alias so any existing call sites still work.
def get_klines_paginated(symbol: str, interval: str, total_limit: int,
                         market: str = "perpetual") -> List[Dict[str, float]]:
    return get_binance_klines_paginated_latest(symbol, interval, total_limit, market)


def fetch_daily_binance(symbol: str, market: str) -> Tuple[List[Dict[str, float]], str]:
    """Binance full 1D history.

      spot       → Binance Spot   /api/v3/klines  (limit 1000, from 2017)
      perpetual  → Binance USDT-M /fapi/v1/klines (limit 1500, from 2019)

    Forward-paginated with startTime. Returns (rows, source)."""
    is_spot = (market == "spot")
    if is_spot:
        base_url, page_limit, start_ms, source = (
            f"{SPOT_API}/api/v3/klines", 1000, 1483228800000, "binance_spot")
    else:
        base_url, page_limit, start_ms, source = (
            f"{BINANCE_FUTURES_API}/fapi/v1/klines", 1500, 1546300800000, "binance_futures")

    out: List[Dict[str, float]] = []
    for _ in range(15):
        try:
            r = req.get(base_url, params={"symbol": symbol, "interval": "1d",
                        "limit": page_limit, "startTime": start_ms}, timeout=30)
            if r.status_code != 200:
                break
            update_api_weight("binance", r)
            resp = r.json()
        except Exception:
            break
        if not resp or not isinstance(resp, list):
            break
        for k in resp:
            try:
                out.append({"openTime": int(k[0]), "high": float(k[2]),
                            "low": float(k[3]), "close": float(k[4])})
            except (TypeError, ValueError, IndexError):
                continue
        if len(resp) < page_limit:
            break
        start_ms = int(resp[-1][0]) + 86_400_000
    out.sort(key=lambda x: x["openTime"])
    return out, source


def fetch_daily_bybit(symbol: str, market: str) -> Tuple[List[Dict[str, float]], str]:
    """Bybit V5 full 1D history. category=linear (perp) / spot. Symbol stays
    ETHUSDT. Newest-first pages, paginated backward via `end`."""
    category = "linear" if market != "spot" else "spot"
    source = "bybit_linear" if category == "linear" else "bybit_spot"
    out: List[Dict[str, float]] = []
    seen: set = set()
    end_ms: Optional[int] = None
    prev_oldest: Optional[int] = None
    for _ in range(40):
        params: Dict[str, Any] = {"category": category, "symbol": symbol,
                                  "interval": "D", "limit": 1000}
        if end_ms is not None:
            params["end"] = end_ms
        try:
            r = req.get(f"{BYBIT_PERP_API}/kline", params=params, timeout=20)
            if r.status_code != 200:
                break
            update_api_weight("bybit", r)
            lst = r.json().get("result", {}).get("list", [])
        except Exception:
            break
        if not lst:
            break
        page: List[int] = []
        for k in lst:                       # newest-first
            try:
                ot = int(k[0])
            except (TypeError, ValueError, IndexError):
                continue
            page.append(ot)
            if ot in seen:
                continue
            try:
                out.append({"openTime": ot, "high": float(k[2]),
                            "low": float(k[3]), "close": float(k[4])})
                seen.add(ot)
            except (TypeError, ValueError, IndexError):
                continue
        if not page:
            break
        oldest = min(page)
        if prev_oldest is not None and oldest >= prev_oldest:
            break                            # no backward progress
        prev_oldest = oldest
        if len(lst) < 1000:
            break
        end_ms = oldest - 1
        time.sleep(0.1)
    out.sort(key=lambda x: x["openTime"])
    return out, source


def fetch_daily_okx(symbol: str, market: str) -> Tuple[List[Dict[str, float]], str]:
    """OKX full 1D history via /history-candles (older history, 100/page).
    instId: spot ETH-USDT, swap ETH-USDT-SWAP. Newest-first, paginated
    backward via `after`."""
    base = symbol[:-4] if symbol.endswith("USDT") else symbol.replace("USDT", "")
    if market != "spot":
        inst_id, source = f"{base}-USDT-SWAP", "okx_swap"
    else:
        inst_id, source = f"{base}-USDT", "okx_spot"
    out: List[Dict[str, float]] = []
    seen: set = set()
    after_ms: Optional[int] = None
    prev_oldest: Optional[int] = None
    for _ in range(80):
        params: Dict[str, Any] = {"instId": inst_id, "bar": "1D", "limit": 100}
        if after_ms is not None:
            params["after"] = after_ms
        try:
            r = req.get(f"{OKX_PERP_API}/history-candles", params=params, timeout=20)
            if r.status_code != 200:
                break
            update_api_weight("okx", r)
            data = r.json().get("data", [])
        except Exception:
            break
        if not data:
            break
        page: List[int] = []
        for k in data:                       # newest-first
            try:
                ot = int(k[0])
            except (TypeError, ValueError, IndexError):
                continue
            page.append(ot)
            if ot in seen:
                continue
            try:
                out.append({"openTime": ot, "high": float(k[2]),
                            "low": float(k[3]), "close": float(k[4])})
                seen.add(ot)
            except (TypeError, ValueError, IndexError):
                continue
        if not page:
            break
        oldest = min(page)
        if prev_oldest is not None and oldest >= prev_oldest:
            break
        prev_oldest = oldest
        if len(data) < 100:
            break
        after_ms = oldest                    # next page returns ts < oldest
        time.sleep(0.1)
    out.sort(key=lambda x: x["openTime"])
    return out, source


def fetch_daily_mexc(symbol: str, market: str) -> Tuple[List[Dict[str, float]], str]:
    """MEXC full 1D history.

      spot       → /api/v3/klines (ETHUSDT, interval 1d, forward via startTime)
      perpetual  → contract /kline (ETH_USDT, interval Day1, forward via start
                   in seconds; response is column arrays in seconds)."""
    if market == "spot":
        out: List[Dict[str, float]] = []
        start_ms = 1483228800000
        for _ in range(15):
            try:
                r = req.get(f"{MEXC_SPOT_API}/klines",
                            params={"symbol": symbol, "interval": "1d",
                                    "limit": 1000, "startTime": start_ms},
                            timeout=20)
                if r.status_code != 200:
                    break
                update_api_weight("mexc", r)
                resp = r.json()
            except Exception:
                break
            if not resp or not isinstance(resp, list):
                break
            for k in resp:
                try:
                    out.append({"openTime": int(k[0]), "high": float(k[2]),
                                "low": float(k[3]), "close": float(k[4])})
                except (TypeError, ValueError, IndexError):
                    continue
            if len(resp) < 1000:
                break
            start_ms = int(resp[-1][0]) + 86_400_000
        out.sort(key=lambda x: x["openTime"])
        return out, "mexc_spot"

    # ── MEXC USDT-M perpetual (contract kline, seconds, column arrays) ──
    mx_sym = symbol.replace("USDT", "_USDT")
    out2: List[Dict[str, float]] = []
    seen: set = set()
    start_s = 1483228800            # 2017-01-01 in seconds
    prev_newest: Optional[int] = None
    for _ in range(15):
        try:
            r = req.get(f"{MEXC_PERP_API}/kline/{mx_sym}",
                        params={"interval": "Day1", "start": start_s},
                        timeout=20)
            if r.status_code != 200:
                break
            update_api_weight("mexc", r)
            d = r.json().get("data", {}) or {}
        except Exception:
            break
        times = d.get("time", []) or []
        if not times:
            break
        highs = d.get("high", []); lows = d.get("low", []); closes = d.get("close", [])
        for i in range(len(times)):
            try:
                ts = int(times[i])
            except (TypeError, ValueError):
                continue
            if ts in seen:
                continue
            try:
                out2.append({"openTime": ts * 1000, "high": float(highs[i]),
                             "low": float(lows[i]), "close": float(closes[i])})
                seen.add(ts)
            except (TypeError, ValueError, IndexError):
                continue
        newest = max(int(t) for t in times)
        if prev_newest is not None and newest <= prev_newest:
            break                            # no forward progress
        prev_newest = newest
        if len(times) < 2:
            break
        start_s = newest + 86_400
        time.sleep(0.1)
    out2.sort(key=lambda x: x["openTime"])
    return out2, "mexc_perp"


def get_all_daily_klines_exchange(symbol: str, exchange: str = "binance",
                                   market: str = "perpetual") -> List[Dict[str, float]]:
    """Full 1D history for ATH/ATL, sourced per exchange + market.

    Phase 3 — every supported exchange has a native adapter:

      binance spot/perp  → fetch_daily_binance
      bybit   spot/perp  → fetch_daily_bybit   (category spot / linear)
      okx     spot/perp  → fetch_daily_okx     (ETH-USDT / ETH-USDT-SWAP)
      mexc    spot/perp  → fetch_daily_mexc    (ETHUSDT / ETH_USDT)

    Hard rules: never mix exchanges, never mix spot/perp, never fall back to
    Binance for another exchange. Unknown exchange → [] (caller skips the
    symbol safely). `symbol` stays the screener symbol (ETHUSDT); each
    adapter converts to its own format internally only for the API call."""
    exchange = (exchange or "binance").lower()
    if exchange == "binance":
        rows, source = fetch_daily_binance(symbol, market)
    elif exchange == "bybit":
        rows, source = fetch_daily_bybit(symbol, market)
    elif exchange == "okx":
        rows, source = fetch_daily_okx(symbol, market)
    elif exchange == "mexc":
        rows, source = fetch_daily_mexc(symbol, market)
    else:
        rows, source = [], "unsupported"

    print(f"[ATH-HIST] symbol={symbol} exchange={exchange} market={market} "
          f"source={source} bars={len(rows)}")
    return rows


def get_all_daily_klines(symbol: str) -> List[Dict[str, float]]:
    """Backward-compatible spot-only daily history.

    DEPRECATED for ATH/ATL: callers that care about spot vs perpetual must
    use get_all_daily_klines_exchange(symbol, exchange, market). Kept as a
    thin Binance-Spot wrapper so any legacy call site behaves exactly as
    before (spot 1D since 2017)."""
    return get_all_daily_klines_exchange(symbol, "binance", "spot")


def detect_true_ath_atl(symbol: str, market: str = "perpetual",
                        exchange: str = "binance") -> Optional[Dict[str, Any]]:
    """DEPRECATED — superseded by compute_window_ath_atl() which is the
    exchange + market aware ATH/ATL path used by /api/ath_atl_scan. Retained
    only for backward safety (no live callers). Now routes through the
    exchange-aware daily history and an exchange:market:symbol cache key so
    it can no longer mislead future ATH/ATL work with spot-only data."""
    cache_key = f"{exchange}:{market}:{symbol}"
    now = time.time()
    cached = ATH_ATL_CACHE.get(cache_key)
    if cached and now - cached["ts"] < ATH_ATL_CACHE_TTL:
        return cached["data"]
    klines = get_all_daily_klines_exchange(symbol, exchange, market)
    if not klines:
        return None
    ath = max(k["high"] for k in klines)
    atl = min(k["low"] for k in klines)
    data = {"ath": ath, "atl": atl, "bars": len(klines)}
    ATH_ATL_CACHE[cache_key] = {"ts": now, "data": data}
    return data


def _get_daily_klines_cached(symbol: str, exchange: str = "binance",
                             market: str = "perpetual") -> List[Dict[str, float]]:
    """Full 1D history for `symbol`, cached 4 h, keyed by
    exchange:market:symbol so Binance Spot and Binance Perpetual keep
    separate histories (e.g. binance:spot:ETHUSDT vs
    binance:perpetual:ETHUSDT). Lets the windowed ATH/ATL scan run
    batch-after-batch without re-paginating full daily history."""
    cache_key = f"{exchange}:{market}:{symbol}"
    now = time.time()
    cached = ATH_ATL_DAILY_CACHE.get(cache_key)
    if cached and now - cached["ts"] < ATH_ATL_DAILY_CACHE_TTL:
        return cached["data"]
    klines = get_all_daily_klines_exchange(symbol, exchange, market)
    if klines:
        ATH_ATL_DAILY_CACHE[cache_key] = {"ts": now, "data": klines}
    return klines


def compute_window_ath_atl(symbol: str, window_hours: int,
                           kl_1h: List[Dict[str, float]],
                           exchange: str = "binance",
                           market: str = "perpetual") -> Optional[Dict[str, float]]:
    """Split history into BEFORE-window vs INSIDE-window and return:

        previous_ath  — highest high strictly BEFORE the selected window
        previous_atl  — lowest  low  strictly BEFORE the selected window
        window_high   — highest high INSIDE the selected window
        window_low    — lowest  low  INSIDE the selected window

    Deep history (older than the 1h fetch covers) comes from cached daily
    candles that fully close before the window starts; the gap between the
    last such daily candle and the window is filled at 1h resolution from
    `kl_1h`, so a high made in the hours just before the window is still
    counted as "previous" (not leaked into the window).
    """
    if not kl_1h:
        return None
    window_hours = max(1, int(window_hours))
    L = len(kl_1h)
    win_start_idx = max(0, L - window_hours)
    window_kl = kl_1h[win_start_idx:]
    if not window_kl:
        return None
    pre_kl = kl_1h[:win_start_idx]                       # 1h bars before window
    cutoff_ms = int(window_kl[0]["openTime"])            # window opens here

    window_high = max(x["high"] for x in window_kl)
    window_low = min(x["low"] for x in window_kl)

    prev_highs: List[float] = []
    prev_lows: List[float] = []

    # 1h bars before the window (precise recent pre-window coverage)
    for x in pre_kl:
        if int(x["openTime"]) < cutoff_ms:
            prev_highs.append(x["high"])
            prev_lows.append(x["low"])

    # Daily candles that fully close on/before the window start. Excluding any
    # daily candle that overlaps the window keeps window highs/lows out of the
    # "previous" level. The 1h pre-window bars above bridge the daily->window
    # gap so a pre-window same-day high is not lost.
    daily = _get_daily_klines_cached(symbol, exchange, market)
    if not daily:
        # No native full history for this exchange/market/symbol → skip the
        # symbol rather than derive a misleading "previous ATH/ATL" from only
        # the recent pre-window 1h bars. Never falls back to another source.
        return None
    for d in daily:
        if int(d["openTime"]) + 86_400_000 <= cutoff_ms:
            prev_highs.append(d["high"])
            prev_lows.append(d["low"])

    if not prev_highs or not prev_lows:
        return None

    return {
        "previous_ath": max(prev_highs),
        "previous_atl": min(prev_lows),
        "window_high": window_high,
        "window_low": window_low,
        "daily_bars": len(daily or []),
    }


def parse_settings(payload: Dict[str, Any]) -> Dict[str, Any]:
    # Phase 1B hardening: safe int/float casts with min-0 clamping for the
    # new OB touch settings. Invalid / empty / missing values fall back to
    # defaults instead of crashing the scan.
    def _safe_int_min0(v: Any, default: int) -> int:
        try:
            return max(0, int(v))
        except (TypeError, ValueError):
            return default

    def _safe_float_min0(v: Any, default: float) -> float:
        try:
            return max(0.0, float(v))
        except (TypeError, ValueError):
            return default

    return {
        "tf": payload.get("tf", "1h"),
        "iLen": int(payload.get("iLen", 5)),
        "sLen": int(payload.get("sLen", 30)),
        "approachPct": float(payload.get("approachPct", 2.0)),
        "obDistancePct": float(payload.get("obDistancePct", payload.get("approachPct", 2.0))),
        "consolCandles": int(payload.get("consolCandles", 4)),
        "rsiOB": float(payload.get("rsiOB", 75)),
        "rsiOS": float(payload.get("rsiOS", 25)),
        "useObStrengthFilter":  bool(payload.get("useObStrengthFilter", False)),
        "obMinStrengthPct":     float(payload.get("obMinStrengthPct", 70)),
        "useHighProbOB": bool(payload.get("useHighProbOB", False)),
        "obMinQuality": int(payload.get("obMinQuality", 50)),
        # Phase 1B: backend OB touch-state filters (no UI yet)
        "useObTouchState":      bool(payload.get("useObTouchState", False)),
        "obTouchState":         payload.get("obTouchState", "all"),
        "obMaxTouches":         _safe_int_min0(payload.get("obMaxTouches"), 99),
        "useObVirginApproach":  bool(payload.get("useObVirginApproach", False)),
        "obVirginApproachPct":  _safe_float_min0(payload.get("obVirginApproachPct"), 1.5),
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
        # Breaker Block
        "useBreakerModule":   bool(payload.get("useBreakerModule", False)),
        "breakerApproachPct": float(payload.get("breakerApproachPct", 2.0)),
        "breakerMaxAge":      int(payload.get("breakerMaxAge", 200)),
        "breakerRequireFvg":  bool(payload.get("breakerRequireFvg", False)),
    }


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("index"))
        uname = session.get("username", "").lower()
        if uname in _force_logout_users:
            _force_logout_users.discard(uname)
            with _sessions_lock:
                _active_sessions.pop(uname, None)
            session.clear()
            return redirect(url_for("index"))
        return f(*args, **kwargs)
    return decorated


def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("is_admin"):
            return redirect(url_for("index"))
        return f(*args, **kwargs)
    return decorated


def _guest_tab_check(tab_name: str):
    """Returns error tuple if guest is blocked, else None. Also increments scan count."""
    if not session.get("is_guest"):
        return None
    if not _guest_controls.get("enabled", True):
        return jsonify({"error": "Guest access is disabled."}), 403
    if not _guest_controls["tabs"].get(tab_name, False):
        return jsonify({"error": f"The '{tab_name}' tab is not available for guests."}), 403
    guest_id = session.get("guest_id", "")
    max_scans = int(_guest_controls.get("max_scans_per_session", 5))
    with _guest_lock:
        gs = _guest_sessions.get(guest_id, {})
        if gs.get("scan_count", 0) >= max_scans:
            return jsonify({"error": f"Guest scan limit reached ({max_scans}/session). Sign in for unlimited access."}), 429
        gs["scan_count"] = gs.get("scan_count", 0) + 1
        tabs = gs.get("tabs_visited", [])
        if tab_name not in tabs:
            tabs.append(tab_name)
        gs["tabs_visited"] = tabs
        _guest_sessions[guest_id] = gs
    return None


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "GET":
        if session.get("logged_in"):
            return redirect(url_for("index"))
        pw_reset = request.args.get("reset") == "1"
        return render_template("login.html", pw_reset_success=pw_reset)

    username = request.form.get("username", "").strip().lower()
    pwd      = request.form.get("password", "")
    ip       = request.headers.get("X-Forwarded-For", request.remote_addr or "unknown").split(",")[0].strip()
    ua       = request.headers.get("User-Agent", "unknown")
    now_utc  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    error    = None
    is_ajax  = request.headers.get("X-Requested-With") == "XMLHttpRequest"

    # ── Try DB login first ──────────────────────────────────────────
    db_user      = None
    db_error     = None
    db_error_code = None
    if not username:
        db_error      = "Please enter your username."
        db_error_code = "empty"
    else:
        try:
            db_user = _DBUser.query.filter_by(username=username).first()
            if db_user:
                if not db_user.check_password(pwd):
                    db_error      = "Incorrect password. Please try again."
                    db_error_code = "wrong_password"
                elif db_user.status == "paused":
                    db_error      = "Your account has been paused. Contact admin."
                    db_error_code = "paused"
                elif db_user.status != "active":
                    db_error      = f"Account is {db_user.status}. Contact admin."
                    db_error_code = "paused"
                elif not db_user.email_verified:
                    db_error      = db_user.username   # carry username for resend link
                    db_error_code = "unverified"
                # success — db_error stays None
            # else: user not in DB, fall through to legacy
        except Exception as _dbe:
            print(f"[LOGIN-DB] Error: {_dbe}")
            db_user = None  # DB unavailable — fall to legacy

    # ── Legacy fallback if DB user not found ──────────────────────
    error_code = None
    if not username:
        error      = db_error
        error_code = db_error_code
    elif db_user is not None:
        # DB user found — honour DB result
        error      = db_error
        error_code = db_error_code
    else:
        # Not in DB — try in-memory legacy list
        if username not in _USERS_DB:
            error      = "No account found. Please create and verify your account first."
            error_code = "no_account"
        elif pwd != _USERS_DB[username]:
            error      = "Incorrect password. Please try again."
            error_code = "wrong_password"
        # else: legacy success, error stays None

    if error is None and username:
        session["logged_in"] = True
        session["username"]  = username
        # persist DB fields if user found
        if db_user is not None:
            try:
                db_user.last_login_at = datetime.now(timezone.utc)
                db_user.last_login_ip = ip
                from models import db as _db
                _db.session.commit()
            except Exception:
                pass
        sid = os.urandom(16).hex()
        session["sid"] = sid

        saved_pairs = load_user_watchlist(username)
        with _wl_lock:
            _wl_user_pairs[username] = saved_pairs
            _wl_rebuild_union()
        if saved_pairs:
            _ensure_wl_thread()

        geo = "unknown"
        try:
            gr = req.get(f"https://ipapi.co/{ip}/json/", timeout=4)
            if gr.status_code == 200:
                gd      = gr.json()
                city    = gd.get("city", "")
                country = gd.get("country_name", "")
                geo     = f"{city}, {country}" if city else country
        except Exception:
            pass

        with _sessions_lock:
            _active_sessions[username] = {
                "ip": ip, "ua": ua,
                "login_time": datetime.now(timezone.utc).isoformat(),
                "sid": sid, "is_admin": False
            }
        LOGIN_AUDIT_LOG.appendleft({
            "username": username, "time": now_utc,
            "ip": ip, "geo": geo, "ua": ua, "success": True
        })

        try:
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

        # Store user_id in session if DB user exists
        if db_user is not None:
            session["user_id"] = db_user.id

        # Record login history asynchronously
        def _record_login(uid, _ip, _ua):
            try:
                with app.app_context():
                    geo  = _geo_lookup(_ip)
                    ua_p = _parse_ua(_ua)
                    lh   = _LoginHistory(
                        user_id    = uid,
                        ip_address = _ip,
                        user_agent = _ua,
                        country    = geo.get("country", ""),
                        city       = geo.get("city", ""),
                        **ua_p,
                    )
                    db.session.add(lh)
                    db.session.commit()
            except Exception as _e:
                print(f"[LOGIN-HIST] {_e}")
        if db_user is not None:
            threading.Thread(target=_record_login, args=(db_user.id, ip, ua), daemon=True).start()

        if is_ajax:
            return jsonify({"success": True, "redirect": url_for("index")})
        return redirect(url_for("index"))

    LOGIN_AUDIT_LOG.appendleft({
        "username": username or "(empty)", "time": now_utc,
        "ip": ip, "geo": "", "ua": ua, "success": False
    })

    if error_code == "unverified":
        unverified_user = error   # error holds the username for this code
        msg = "Your account is not verified yet. Please verify your email before logging in."
        if is_ajax:
            return jsonify({"error": "unverified", "message": msg, "username": unverified_user}), 401
        return render_template("login.html",
                               error=msg,
                               unverified_username=unverified_user)

    if is_ajax:
        return jsonify({"error": error_code or "auth_failed",
                        "message": error or "Login failed. Please try again."}), 401
    return render_template("login.html", error=error)


@app.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "GET":
        return redirect(url_for("login"))

    import random
    is_ajax = request.headers.get("X-Requested-With") == "XMLHttpRequest"

    username = request.form.get("username", "").strip().lower()
    email    = request.form.get("email", "").strip().lower()
    password = request.form.get("password", "")

    def _err(msg, **kw):
        if is_ajax:
            return jsonify({"error": "validation", "message": msg}), 400
        return render_template("login.html", register_error=msg, show_signup=True, **kw)

    if not username:
        return _err("Username is required.")
    if not email:
        return _err("Email address is required.", reg_username=username)
    if "@" not in email or "." not in email.split("@")[-1]:
        return _err("Please enter a valid email address.", reg_username=username, reg_email=email)
    if not password:
        return _err("Password is required.", reg_username=username, reg_email=email)
    if len(password) < 6:
        return _err("Password must be at least 6 characters.", reg_username=username, reg_email=email)
    try:
        if _DBUser.query.filter_by(username=username).first():
            return _err("Username already taken. Choose another.", reg_username=username, reg_email=email)
        if _DBUser.query.filter_by(email=email).first():
            return _err("This email is already registered. Sign in instead.", reg_username=username, reg_email=email)

        new_user = _DBUser(username=username, email=email, role="user", status="active", email_verified=False)
        new_user.set_password(password)
        db.session.add(new_user)
        db.session.flush()  # get new_user.id before commit

        code    = f"{random.randint(0, 999999):06d}"
        expires = datetime.now(timezone.utc) + timedelta(minutes=10)
        ev = _EmailVerification(user_id=new_user.id, code=code, expires_at=expires)
        db.session.add(ev)
        db.session.commit()

        # Send synchronously so we know immediately if it succeeded
        email_sent, fail_reason = send_verification_email(email, code, username)

        # Store outcome in session so verify page can react
        session["verify_email_sent"]    = email_sent
        session["verify_pending_user"]  = username
        session["verify_fail_reason"]   = fail_reason

        # Expose code on-screen so the user can proceed even if delivery failed
        if not email_sent:
            session["verify_fallback_code"] = code

        redirect_url = url_for("verify_email", username=username)
        if is_ajax:
            return jsonify({"success": True, "redirect": redirect_url})
        return redirect(redirect_url)
    except Exception as _re:
        print(f"[REGISTER] Error: {_re}")
        db.session.rollback()
        return _err("Registration failed. Please try again.")


@app.route("/verify-email", methods=["GET", "POST"])
def verify_email():
    if request.method == "GET":
        username = request.args.get("username", "") or session.get("verify_pending_user", "")
        email_sent    = session.pop("verify_email_sent", True)
        fallback_code = session.pop("verify_fallback_code", None)
        fail_reason   = session.pop("verify_fail_reason", "missing_config")
        return render_template("verify.html",
                               username=username,
                               email_sent=email_sent,
                               fallback_code=fallback_code,
                               fail_reason=fail_reason)

    username = request.form.get("username", "").strip().lower()
    code     = request.form.get("code", "").strip()

    if not username or not code:
        return render_template("verify.html", username=username,
                               error="Please enter the 6-digit verification code.")
    try:
        user = _DBUser.query.filter_by(username=username).first()
        if not user:
            return render_template("verify.html", username=username,
                                   error="Account not found. Please register again.")
        if user.email_verified:
            return render_template("login.html", success="Email already verified. You can sign in.",
                                   login_username=username)

        now = datetime.now(timezone.utc)
        ev  = (_EmailVerification.query
               .filter_by(user_id=user.id, code=code, used=False)
               .filter(_EmailVerification.expires_at > now)
               .first())

        if not ev:
            return render_template("verify.html", username=username,
                                   error="Invalid or expired code. Request a new one below.")

        ev.used            = True
        user.email_verified = True
        db.session.commit()

        return render_template("login.html",
                               success="Email verified! Your account is active — sign in below.",
                               login_username=username)
    except Exception as _ve:
        print(f"[VERIFY] Error: {_ve}")
        db.session.rollback()
        return render_template("verify.html", username=username,
                               error="Verification failed. Please try again.")


@app.route("/resend-verification", methods=["POST"])
def resend_verification():
    import random
    username = request.form.get("username", "").strip().lower()
    try:
        user = _DBUser.query.filter_by(username=username).first()
        if not user or not user.email:
            return render_template("verify.html", username=username,
                                   error="Account not found or no email on file.")
        if user.email_verified:
            return render_template("login.html", success="Email already verified. You can sign in.",
                                   login_username=username)

        # Rate-limit: block resend if a fresh code was issued within the last 2 minutes
        cooldown_cutoff = datetime.now(timezone.utc) - timedelta(minutes=2)
        recent = (_EmailVerification.query
                  .filter_by(user_id=user.id, used=False)
                  .filter(_EmailVerification.created_at > cooldown_cutoff)
                  .first())
        if recent:
            wait_sec = int((recent.created_at + timedelta(minutes=2)
                            - datetime.now(timezone.utc)).total_seconds())
            wait_sec = max(wait_sec, 1)
            return render_template("verify.html", username=username,
                                   error=f"Please wait {wait_sec}s before requesting a new code.")

        # Invalidate all previous unused codes
        (_EmailVerification.query
         .filter_by(user_id=user.id, used=False)
         .update({"used": True}))

        code    = f"{random.randint(0, 999999):06d}"
        expires = datetime.now(timezone.utc) + timedelta(minutes=10)
        ev = _EmailVerification(user_id=user.id, code=code, expires_at=expires)
        db.session.add(ev)
        db.session.commit()

        email_sent, fail_reason = send_verification_email(user.email, code, username)

        if email_sent:
            return render_template("verify.html", username=username,
                                   email_sent=True,
                                   success=f"A new code has been sent to {user.email}.")
        else:
            return render_template("verify.html", username=username,
                                   email_sent=False,
                                   fallback_code=code,
                                   fail_reason=fail_reason,
                                   success="New code generated.")
    except Exception as _re:
        print(f"[RESEND-VERIFY] Error: {_re}")
        db.session.rollback()
        return render_template("verify.html", username=username,
                               error="Failed to resend. Please try again later.")


# ── Password Reset ─────────────────────────────────────────────────────────────

def _build_password_reset_email(username: str, reset_url: str) -> str:
    """Return the premium HTML email body for password reset."""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1.0">
  <meta http-equiv="X-UA-Compatible" content="IE=edge">
  <title>Reset Your Password — ZyNi SMC</title>
</head>
<body style="margin:0;padding:0;background:#060a14;font-family:'Segoe UI',Helvetica,Arial,sans-serif;-webkit-font-smoothing:antialiased;">
<table width="100%" cellpadding="0" cellspacing="0" role="presentation" style="background:#060a14;">
  <tr><td align="center" style="padding:40px 16px;">
    <table width="100%" cellpadding="0" cellspacing="0" role="presentation" style="max-width:580px;">

      <!-- HEADER / LOGO BANNER -->
      <tr><td style="background:#000000;border-radius:16px 16px 0 0;padding:0;text-align:center;border:1px solid rgba(249,115,22,0.25);border-bottom:3px solid #f97316;overflow:hidden;line-height:0;font-size:0;">
        <img src="https://smcsetups.com/static/images/logo-email.png"
             alt="ZyNi SMC"
             width="580"
             style="width:100%;max-width:580px;height:auto;display:block;border-radius:16px 16px 0 0;">
      </td></tr>

      <!-- HERO BANNER -->
      <tr><td style="background:linear-gradient(135deg,#0d1525 0%,#0f1e38 60%,#0a0f1e 100%);padding:36px 40px 28px;text-align:center;border-left:1px solid rgba(249,115,22,0.15);border-right:1px solid rgba(249,115,22,0.15);">
        <img src="https://smcsetups.com/static/images/avatar-email.png" alt="ZyNi SMC" width="90" height="90"
             style="width:90px;height:90px;border-radius:50%;display:block;margin:0 auto 20px;border:2.5px solid rgba(249,115,22,0.70);box-shadow:0 0 0 4px rgba(249,115,22,0.12),0 8px 28px rgba(0,0,0,0.55),0 0 30px rgba(249,115,22,0.18);object-fit:cover;">
        <h1 style="color:#ffffff;font-size:23px;font-weight:800;margin:0 0 14px;letter-spacing:-0.3px;">Password Reset Request</h1>
        <p style="color:rgba(232,240,255,0.65);font-size:15px;margin:0;line-height:1.75;">
          Hi <strong style="color:#ffffff;">{username}</strong>, we received a request to reset your ZyNi SMC password.<br>
          Click the button below to choose a new password.
        </p>
      </td></tr>

      <!-- CTA BUTTON -->
      <tr><td style="background:linear-gradient(135deg,#0d1525 0%,#0f1e38 100%);padding:0 40px 36px;text-align:center;border-left:1px solid rgba(249,115,22,0.15);border-right:1px solid rgba(249,115,22,0.15);">
        <table cellpadding="0" cellspacing="0" role="presentation" style="margin:0 auto;">
          <tr><td style="border-radius:12px;background:linear-gradient(135deg,#f97316 0%,#ea580c 100%);box-shadow:0 8px 32px rgba(249,115,22,0.35);">
            <a href="{reset_url}"
               style="display:inline-block;padding:15px 44px;color:#ffffff;font-size:15px;font-weight:700;text-decoration:none;letter-spacing:0.3px;border-radius:12px;">
              Reset My Password
            </a>
          </td></tr>
        </table>
        <p style="color:rgba(232,240,255,0.40);font-size:12px;margin:18px 0 0;line-height:1.6;">
          This link expires in <strong style="color:rgba(249,115,22,0.75);">30 minutes</strong>.
          If you did not request a password reset, you can safely ignore this email — your account is secure.
        </p>
      </td></tr>

      <!-- DIVIDER -->
      <tr><td style="background:linear-gradient(135deg,#0d1525 0%,#0f1e38 100%);padding:0 40px;border-left:1px solid rgba(249,115,22,0.15);border-right:1px solid rgba(249,115,22,0.15);">
        <div style="height:1px;background:linear-gradient(90deg,transparent,rgba(249,115,22,0.25),transparent);"></div>
      </td></tr>

      <!-- SECURITY NOTE -->
      <tr><td style="background:linear-gradient(135deg,#0d1525 0%,#0f1e38 100%);padding:24px 40px 32px;border-left:1px solid rgba(249,115,22,0.15);border-right:1px solid rgba(249,115,22,0.15);">
        <table cellpadding="0" cellspacing="0" role="presentation" width="100%">
          <tr>
            <td style="background:rgba(249,115,22,0.07);border:1px solid rgba(249,115,22,0.2);border-radius:10px;padding:16px 20px;">
              <p style="color:rgba(232,240,255,0.55);font-size:12.5px;margin:0;line-height:1.7;">
                <strong style="color:rgba(249,115,22,0.85);">Security tip:</strong>
                ZyNi SMC will never ask for your password via email or phone.
                This link can only be used once and will expire automatically.
                If you didn't request this, please contact us at
                <a href="mailto:support@smcsetups.com" style="color:#f97316;text-decoration:none;">support@smcsetups.com</a>.
              </p>
            </td>
          </tr>
        </table>
      </td></tr>

      <!-- FOOTER -->
      <tr><td style="background:#040710;border-radius:0 0 16px 16px;padding:24px 40px;text-align:center;border:1px solid rgba(249,115,22,0.12);border-top:none;">
        <p style="color:rgba(232,240,255,0.30);font-size:11.5px;margin:0 0 6px;line-height:1.7;">
          ZyNi SMC — Smart Market Center<br>
          Questions? <a href="mailto:support@smcsetups.com" style="color:#f97316;text-decoration:none;">support@smcsetups.com</a>
        </p>
        <p style="color:rgba(232,240,255,0.18);font-size:10.5px;margin:0;">
          You received this email because a password reset was requested for your account.
        </p>
      </td></tr>

    </table>
  </td></tr>
</table>
</body>
</html>"""


@app.route("/forgot-password", methods=["POST"])
def forgot_password():
    is_ajax = request.headers.get("X-Requested-With") == "XMLHttpRequest"

    data       = request.get_json(silent=True) or {}
    identifier = (data.get("identifier") or request.form.get("identifier", "")).strip().lower()

    # Always return a generic success message to prevent user enumeration
    generic_ok = {"success": True,
                  "message": "If that account exists, a reset link has been sent to the registered email."}

    if not identifier:
        if is_ajax:
            return jsonify({"error": "validation", "message": "Please enter your email or username."}), 400
        return redirect(url_for("login"))

    # Rate-limit: allow at most 1 reset request per email per 5 minutes
    user = _DBUser.query.filter(
        (_DBUser.email == identifier) | (_DBUser.username == identifier)
    ).first()

    if user:
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=5)
        recent = (_PasswordResetToken.query
                  .filter_by(user_id=user.id)
                  .filter(_PasswordResetToken.created_at > cutoff)
                  .first())
        if not recent:
            raw_token  = secrets.token_urlsafe(32)
            token_hash = hashlib.sha256(raw_token.encode()).hexdigest()
            expires    = datetime.now(timezone.utc) + timedelta(minutes=30)
            prt = _PasswordResetToken(user_id=user.id, token_hash=token_hash, expires_at=expires)
            db.session.add(prt)
            db.session.commit()

            reset_url  = url_for("reset_password", token=raw_token, _external=True)
            email_body = _build_password_reset_email(user.username, reset_url)
            _send_via_resend(user.email, "ZyNi SMC — Reset Your Password", email_body)
            print(f"[RESET] Sent password reset to {user.email}")
        else:
            print(f"[RESET] Rate-limited: reset already sent for user_id={user.id}")

    if is_ajax:
        return jsonify(generic_ok)
    return redirect(url_for("login"))


@app.route("/reset-password/<token>", methods=["GET", "POST"])
def reset_password(token):
    token_hash = hashlib.sha256(token.encode()).hexdigest()
    now        = datetime.now(timezone.utc)

    prt = (_PasswordResetToken.query
           .filter_by(token_hash=token_hash, used=False)
           .filter(_PasswordResetToken.expires_at > now)
           .first())

    if not prt:
        return render_template("reset_password.html", invalid=True)

    if request.method == "GET":
        return render_template("reset_password.html", token=token)

    # POST — set new password
    password  = request.form.get("password", "")
    password2 = request.form.get("password2", "")
    is_ajax   = request.headers.get("X-Requested-With") == "XMLHttpRequest"

    def _err(msg):
        if is_ajax:
            return jsonify({"error": "validation", "message": msg}), 400
        return render_template("reset_password.html", token=token, error=msg)

    if not password:
        return _err("Please enter a new password.")
    if len(password) < 6:
        return _err("Password must be at least 6 characters.")
    if password != password2:
        return _err("Passwords do not match.")

    try:
        user = _DBUser.query.get(prt.user_id)
        if not user:
            return render_template("reset_password.html", invalid=True)
        user.set_password(password)
        prt.used = True
        db.session.commit()
        print(f"[RESET] Password updated for user_id={user.id}")
        if is_ajax:
            return jsonify({"success": True, "redirect": url_for("login")})
        return redirect(url_for("login"))
    except Exception as exc:
        db.session.rollback()
        print(f"[RESET] Error updating password: {exc}")
        return _err("Something went wrong. Please try again.")


@app.route("/admin/test-email")
def admin_test_email():
    """Diagnostic endpoint — accessible by admin session OR ?token=zynismctest query param."""
    token = request.args.get("token", "")
    if not session.get("is_admin") and token != "zynismctest2026":
        return "Access denied. Add ?token=zynismctest2026 to the URL.", 403

    frm = _email_from()
    pwd = _email_pass()
    to  = _email_to()

    result = {
        "ALERT_EMAIL_FROM": frm if frm else "*** NOT SET ***",
        "ALERT_EMAIL_PASS": f"{'*' * len(pwd)} ({len(pwd)} chars)" if pwd else "*** NOT SET ***",
        "ALERT_EMAIL_TO":   to  if to  else "(not set — only needed for login alerts)",
        "credentials_ok":  bool(frm and pwd),
        "smtp_test":       "SKIPPED — credentials missing",
        "note":            "",
    }

    if not frm or not pwd:
        result["note"] = ("Set ALERT_EMAIL_FROM and ALERT_EMAIL_PASS as Environment Variables "
                          "(not Secrets) in Koyeb service settings, then redeploy.")
        return jsonify(result)

    # Live SMTP login test
    e465 = e587 = None
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=12) as srv:
            srv.login(frm, pwd)
        result["smtp_test"] = "OK via SSL port 465"
        result["note"] = "Email should work. If OTP still not arriving, check spam folder."
        return jsonify(result)
    except Exception as ex:
        e465 = str(ex)

    try:
        ctx = ssl.create_default_context()
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=12) as srv:
            srv.ehlo(); srv.starttls(context=ctx); srv.ehlo()
            srv.login(frm, pwd)
        result["smtp_test"] = "OK via STARTTLS port 587"
        result["note"] = "Email should work. If OTP still not arriving, check spam folder."
        return jsonify(result)
    except Exception as ex:
        e587 = str(ex)

    result["smtp_test"] = "FAILED on both ports"
    result["error_465"]  = e465
    result["error_587"]  = e587
    if "Application-specific password required" in (e465 or "") or "Username and Password not accepted" in (e465 or ""):
        result["note"] = ("Gmail rejected the password. Make sure you are using a Gmail APP PASSWORD "
                          "(16 chars, no spaces) — NOT your regular Gmail password. "
                          "Enable 2FA first, then generate an App Password at "
                          "myaccount.google.com → Security → App Passwords.")
    elif "timed out" in (e465 or "").lower() or "timed out" in (e587 or "").lower():
        result["note"] = ("SMTP connection timed out — Koyeb may be blocking outbound SMTP ports. "
                          "Use a transactional email API instead: Resend (resend.com) or SendGrid.")
    else:
        result["note"] = "Check the error messages above. Contact support if unclear."
    return jsonify(result)



@app.route("/logout")
def logout():
    username = session.get("username", "").lower()
    # Update session duration in login history
    uid = session.get("user_id")
    if uid:
        def _end_session(_uid):
            try:
                with app.app_context():
                    from models import LoginHistory as _LH
                    lh = _LH.query.filter_by(user_id=_uid).order_by(_LH.logged_in_at.desc()).first()
                    if lh and lh.session_duration is None:
                        mins = int((datetime.now(timezone.utc) - lh.logged_in_at.replace(tzinfo=timezone.utc)).total_seconds() / 60)
                        lh.session_duration = mins
                        db.session.commit()
            except Exception:
                pass
        threading.Thread(target=_end_session, args=(uid,), daemon=True).start()
    with _sessions_lock:
        _active_sessions.pop(username, None)
    session.clear()
    return redirect(url_for("index"))



@app.route("/api/admin/users")
@admin_required
def api_admin_users():
    users = [{"username": u, "default_pass": (p == APP_PASSWORD)} for u, p in _USERS_DB.items()]
    return jsonify({"users": users})


@app.route("/api/admin/users/add", methods=["POST"])
@admin_required
def api_admin_users_add():
    data  = request.get_json(force=True) or {}
    uname = data.get("username", "").strip().lower()
    pwd   = data.get("password", "").strip()
    if not uname:
        return jsonify({"error": "Username required"}), 400
    if not pwd:
        return jsonify({"error": "Password required"}), 400
    if uname in _USERS_DB:
        return jsonify({"error": "User already exists"}), 409
    _USERS_DB[uname] = pwd
    return jsonify({"ok": True, "users": [{"username": u, "default_pass": (p == APP_PASSWORD)} for u, p in _USERS_DB.items()]})


@app.route("/api/admin/users/remove", methods=["POST"])
@admin_required
def api_admin_users_remove():
    data  = request.get_json(force=True) or {}
    uname = data.get("username", "").strip().lower()
    if uname not in _USERS_DB:
        return jsonify({"error": "User not found"}), 404
    del _USERS_DB[uname]
    _force_logout_users.add(uname)
    with _sessions_lock:
        _active_sessions.pop(uname, None)
    return jsonify({"ok": True, "users": [{"username": u, "default_pass": (p == APP_PASSWORD)} for u, p in _USERS_DB.items()]})


@app.route("/api/admin/users/password", methods=["POST"])
@admin_required
def api_admin_users_password():
    data  = request.get_json(force=True) or {}
    uname = data.get("username", "").strip().lower()
    pwd   = data.get("password", "").strip()
    if uname not in _USERS_DB:
        return jsonify({"error": "User not found"}), 404
    if not pwd:
        return jsonify({"error": "Password required"}), 400
    _USERS_DB[uname] = pwd
    return jsonify({"ok": True})


@app.route("/api/admin/users/<username>/detail")
@admin_required
def api_admin_user_detail(username):
    username = username.lower()
    if username not in _USERS_DB:
        return jsonify({"error": "User not found"}), 404
    user_logs = [e for e in LOGIN_AUDIT_LOG if e.get("username") == username]
    with _sessions_lock:
        current = _active_sessions.get(username)
    return jsonify({
        "username": username,
        "is_online": username in _active_sessions,
        "current_session": current,
        "total_logins": sum(1 for e in user_logs if e.get("success")),
        "failed_attempts": sum(1 for e in user_logs if not e.get("success")),
        "last_login": user_logs[0] if user_logs else None,
        "history": user_logs[:10],
    })


@app.route("/api/admin/sessions")
@admin_required
def api_admin_sessions():
    with _sessions_lock:
        sessions_list = [{"username": k, **v} for k, v in _active_sessions.items()]
    return jsonify({"sessions": sessions_list})


@app.route("/api/admin/sessions/logout", methods=["POST"])
@admin_required
def api_admin_sessions_logout():
    data  = request.get_json(force=True) or {}
    uname = data.get("username", "").strip().lower()
    _force_logout_users.add(uname)
    with _sessions_lock:
        _active_sessions.pop(uname, None)
    return jsonify({"ok": True})


@app.route("/api/admin/logs")
@admin_required
def api_admin_logs():
    limit = min(int(request.args.get("limit", 100)), 500)
    return jsonify({"logs": list(LOGIN_AUDIT_LOG)[:limit]})


@app.route("/api/admin/stats")
@admin_required
def api_admin_stats():
    uptime_secs = int((datetime.now(timezone.utc) - _app_start_time).total_seconds())
    h, rem      = divmod(uptime_secs, 3600)
    m, s        = divmod(rem, 60)
    mem_mb = 0
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    mem_mb = int(line.split()[1]) // 1024
                    break
    except Exception:
        pass
    with _sessions_lock:
        active_count = len(_active_sessions)
    return jsonify({
        "uptime": f"{h}h {m}m {s}s",
        "memory_mb": mem_mb,
        "total_users": len(_USERS_DB),
        "active_sessions": active_count,
        "total_logins": sum(1 for e in LOGIN_AUDIT_LOG if e.get("success")),
        "failed_attempts": sum(1 for e in LOGIN_AUDIT_LOG if not e.get("success")),
        "audit_log_size": len(LOGIN_AUDIT_LOG),
    })


@app.route("/api/admin/controls/tabs")
@admin_required
def api_admin_tab_status():
    return jsonify({"tab_controls": _tab_controls})


@app.route("/api/admin/controls/tab", methods=["POST"])
@admin_required
def api_admin_tab_toggle():
    data    = request.get_json(force=True) or {}
    tab     = data.get("tab", "")
    enabled = bool(data.get("enabled", True))
    if tab not in _tab_controls:
        return jsonify({"error": "Unknown tab"}), 400
    _tab_controls[tab] = enabled
    return jsonify({"ok": True, "tab_controls": _tab_controls})


@app.route("/api/admin/guest/controls", methods=["GET", "POST"])
@admin_required
def api_admin_guest_controls():
    if request.method == "GET":
        return jsonify({"guest_controls": _guest_controls})
    data = request.get_json(force=True) or {}
    if "enabled" in data:
        _guest_controls["enabled"] = bool(data["enabled"])
    if "max_scans_per_session" in data:
        _guest_controls["max_scans_per_session"] = max(1, int(data["max_scans_per_session"]))
    if "max_pairs" in data:
        _guest_controls["max_pairs"] = max(1, int(data["max_pairs"]))
    if "tabs" in data and isinstance(data["tabs"], dict):
        for tab, val in data["tabs"].items():
            if tab in _guest_controls["tabs"]:
                _guest_controls["tabs"][tab] = bool(val)
    return jsonify({"ok": True, "guest_controls": _guest_controls})


@app.route("/api/admin/guest/sessions")
@admin_required
def api_admin_guest_sessions():
    with _guest_lock:
        sessions = [{"guest_id": k, **v} for k, v in _guest_sessions.items()]
    return jsonify({"sessions": sessions, "total": len(sessions)})


# ── Guest login ──
@app.route("/guest/login", methods=["POST"])
def guest_login():
    if not _guest_controls.get("enabled", True):
        return jsonify({"error": "Guest access is currently disabled."}), 403
    guest_id = os.urandom(12).hex()
    display  = f"guest_{guest_id[:6]}"
    session["logged_in"]  = True
    session["is_guest"]   = True
    session["guest_id"]   = guest_id
    session["username"]   = display
    ip      = request.headers.get("X-Forwarded-For", request.remote_addr or "unknown").split(",")[0].strip()
    ua      = request.headers.get("User-Agent", "unknown")
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    with _guest_lock:
        _guest_sessions[guest_id] = {
            "ip": ip, "ua": ua,
            "login_time": datetime.now(timezone.utc).isoformat(),
            "scan_count": 0, "tabs_visited": [],
        }
    LOGIN_AUDIT_LOG.appendleft({
        "username": f"GUEST-{guest_id[:6]}", "time": now_utc,
        "ip": ip, "geo": "", "ua": ua, "success": True
    })
    return redirect(url_for("index"))


# ── Watchlist streaming endpoints ──

@app.route("/api/watchlist/register", methods=["POST"])
@login_required
def api_watchlist_register():
    username = session.get("username", "default")
    data  = request.get_json(force=True) or {}
    pairs = [str(p).strip().upper() for p in data.get("pairs", []) if str(p).strip()]
    pairs = [p for p in pairs if p.endswith("USDT")][:10]  # max 10 for Nano

    # Save to server permanently
    save_user_watchlist(username, pairs)

    with _wl_lock:
        old_user_pairs = set(_wl_user_pairs.get(username, []))
        _wl_user_pairs[username] = pairs
        _wl_rebuild_union()
        global_pairs_after = set(_wl_pairs)
        # Evict cache only for symbols no longer needed by any user
        for sym in list(_wl_cache.keys()):
            if sym not in global_pairs_after:
                del _wl_cache[sym]

    new_user_pairs = set(pairs)

    # Stop WebSocket only for pairs no user needs anymore
    for sym in old_user_pairs - new_user_pairs:
        if sym not in global_pairs_after:
            stop_ob_ws(sym)

    # Start WebSocket for new pairs added by this user
    for sym in new_user_pairs - old_user_pairs:
        start_ob_ws(sym)

    # Ensure streams running for this user's pairs
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


# ─── Scan presets (Queue 15) ────────────────────────────────────────────────
def _current_user_id():
    """Resolve the SQLAlchemy User.id for the logged-in session, or None."""
    try:
        from models import db as _db, User as _U
        uname = (session.get("username") or "").strip().lower()
        if not uname:
            return None
        u = _U.query.filter(_db.func.lower(_U.username) == uname).first()
        return u.id if u else None
    except Exception:
        return None


def _current_user_id_and_user():
    """Return (user_id, user_obj) for the logged-in session, or (None, None)."""
    try:
        from models import db as _db, User as _U
        uname = (session.get("username") or "").strip().lower()
        if not uname:
            return None, None
        u = _U.query.filter(_db.func.lower(_U.username) == uname).first()
        if not u:
            return None, None
        return u.id, u
    except Exception:
        return None, None


def _json_dumps_safe(obj) -> str:
    """Safely serialize a dict/list to a JSON string; returns '{}' on failure."""
    try:
        return json.dumps(obj, default=str)
    except Exception:
        return "{}"


def _json_loads_safe(text, fallback=None):
    """Safely parse a JSON Text field; returns fallback on bad input."""
    if fallback is None:
        fallback = {}
    if not text:
        return fallback
    try:
        return json.loads(text)
    except Exception:
        return fallback


_LM_NULL_STRINGS = {"", "—", "n/a", "null", "undefined", "none", "-"}

def _lm_float_or_none(value):
    """Safely convert a frontend value to float; returns None on bad/empty input."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip().lower()
    if s in _LM_NULL_STRINGS:
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _lm_int_or_zero(value) -> int:
    """Safely convert a frontend value to int; returns 0 on bad/empty input."""
    if value is None:
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    s = str(value).strip().lower()
    if s in _LM_NULL_STRINGS:
        return 0
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return 0


# ─── Phase 3: Live Monitor MTF Scanner helpers ────────────────────────────────

_LM_VALID_TIMEFRAMES = ["15m", "30m", "1h", "4h", "1d"]
_LM_VALID_MODULES    = ["OB", "FVG", "FIB", "Breaker", "Bias"]
_LM_ZONE_NEAR_PCT    = 2.0    # % outside zone edge to be considered "near"
_LM_BREACH_RISK_PCT  = 0.25   # % against-zone move required to flag breach_risk


def _lm_allowed_timeframes(value) -> list:
    """Return a clean list of visible analysis TFs. Always excludes 5m."""
    default = ["15m", "30m", "1h", "4h", "1d"]
    if value is None:
        return default
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            return default
    if not isinstance(value, list):
        return default
    cleaned = [tf for tf in (str(t).strip().lower() for t in value)
               if tf != "5m" and tf in _LM_VALID_TIMEFRAMES]
    return cleaned if cleaned else default


def _lm_allowed_modules(value) -> list:
    """Return a clean module list normalized to canonical names."""
    default = ["OB", "FVG", "FIB", "Breaker", "Bias"]
    if value is None:
        return default
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            return default
    if not isinstance(value, list):
        return default
    _alias = {
        "ob": "OB", "fvg": "FVG", "fib": "FIB",
        "breaker": "Breaker", "breakerblock": "Breaker",
        "bias": "Bias",
    }
    seen: set = set()
    cleaned: list = []
    for m in value:
        key = str(m).strip().lower().replace(" ", "").replace("_", "").replace("block", "")
        canonical = _alias.get(key)
        if canonical and canonical not in seen:
            seen.add(canonical)
            cleaned.append(canonical)
    return cleaned if cleaned else default


def _lm_build_scan_config(item) -> dict:
    """Build a wl_config dict compatible with _scan_pair_multitf for a LiveMonitorItem."""
    tfs  = _lm_allowed_timeframes(item.selected_timeframes)
    mods = _lm_allowed_modules(item.selected_modules)
    _ob_app  = {"15m": 0.8, "30m": 1.0, "1h": 1.5, "4h": 2.5, "1d": 3.0}
    _fvg_max = {"15m": 5,   "30m": 8,   "1h": 10,  "4h": 15,  "1d": 20}
    fib_pref = [t for t in ("1h", "4h", "30m", "1d", "15m") if t in tfs]
    return {
        "timeframes":           tfs,
        "scan_ob":              "OB"      in mods,
        "scan_fvg":             "FVG"     in mods,
        "scan_fib":             "FIB"     in mods,
        "scan_breaker":         "Breaker" in mods,
        "bias_1d":              "Bias"    in mods,
        "ob_approach":          {tf: _ob_app.get(tf, 2.0)  for tf in tfs},
        "fvg_age_min":          {tf: 0                     for tf in tfs},
        "fvg_age_max":          {tf: _fvg_max.get(tf, 15)  for tf in tfs},
        "fib_tf":               fib_pref[0] if fib_pref else "1h",
        "fib_tolerance":        0.5,
        "fib_approach":         2.0,
        "fib_atr_mult":         1.5,
        "fib_levels":           ["0.5", "0.618", "0.705", "0.786"],
        "breaker_approach_pct": 2.0,
        "breaker_max_age":      200,
        "breaker_require_fvg":  False,
    }


def _lm_extract_mtf_summary(scan_result: dict, tfs: list, mods: list,
                             exchange: str = None, market: str = None) -> dict:
    """Convert _scan_pair_multitf output to a clean per-TF summary for UI and storage."""
    now_iso   = datetime.now(timezone.utc).isoformat()
    raw_price = scan_result.get("price", 0) or 0
    bias_1d   = scan_result.get("bias_1d")      # 1 / -1 / 0 / None
    raw_tfs   = scan_result.get("tfs", {})

    tfs_out: dict = {}
    for tf in tfs:
        tf_data = raw_tfs.get(tf, {})
        if tf_data.get("error"):
            tfs_out[tf] = {"error": tf_data["error"]}
            continue

        trend   = tf_data.get("trend",  0) or 0
        itrend  = tf_data.get("itrend", 0) or 0
        eff_dir = trend if trend != 0 else itrend
        tf_dir  = "bullish" if eff_dir == 1 else "bearish" if eff_dir == -1 else "neutral"

        obs      = tf_data.get("obs",      [])
        fvgs     = tf_data.get("fvgs",     [])
        fibs     = tf_data.get("fibs",     [])
        breakers = tf_data.get("breakers", [])

        # OB
        if "OB" in mods:
            inside_obs = [z for z in obs if z.get("state") == "inside"]
            appr_obs   = [z for z in obs if z.get("state") == "approaching"]
            if inside_obs:
                ob_s, ob_l = "strong", "In Zone"
            elif appr_obs:
                best_q = max((z.get("quality", 0) for z in appr_obs), default=0)
                ob_s   = "strong" if best_q >= 70 else "yes"
                ob_l   = "Approach"
            else:
                ob_s, ob_l = "none", "—"
        else:
            ob_s, ob_l = "none", "—"

        # FVG
        if "FVG" in mods:
            valid_fvgs = [f for f in fvgs if f.get("isValid") and f.get("status") == "UNTOUCHED"]
            any_unt    = [f for f in fvgs if f.get("status") == "UNTOUCHED"]
            if valid_fvgs:
                fvg_s, fvg_l = "yes", "Untouched"
            elif any_unt:
                fvg_s, fvg_l = "warn", "Touched"
            else:
                fvg_s, fvg_l = "none", "—"
        else:
            fvg_s, fvg_l = "none", "—"

        # FIB
        if "FIB" in mods and fibs:
            best_fib = min(fibs, key=lambda z: abs(z.get("dist", 999)))
            fib_s = "yes"
            fib_l = str(best_fib.get("level", "Yes") or "Yes")
        else:
            fib_s, fib_l = "none", "—"

        # Breaker
        if "Breaker" in mods:
            inside_brk = [z for z in breakers if z.get("state") == "inside"]
            appr_brk   = [z for z in breakers if z.get("state") == "approaching"]
            if inside_brk:
                brk_s, brk_l = "strong", "In Zone"
            elif appr_brk:
                has_hp = any(z.get("highProb") for z in appr_brk)
                brk_s  = "yes" if has_hp else "warn"
                brk_l  = "Approach"
            else:
                brk_s, brk_l = "none", "—"
        else:
            brk_s, brk_l = "none", "—"

        # Bias — use per-TF trend; override with bias_1d on the 1d TF
        if "Bias" in mods:
            bias_eff = (bias_1d if (tf == "1d" and bias_1d is not None) else eff_dir)
            if bias_eff == 1:
                bias_s, bias_l = "strong", "Bull"
            elif bias_eff == -1:
                bias_s, bias_l = "strong", "Bear"
            else:
                bias_s, bias_l = "none", "—"
        else:
            bias_s, bias_l = "none", "—"

        modules_out: dict = {}
        if "OB"      in mods: modules_out["OB"]      = {"state": ob_s,   "label": ob_l}
        if "FVG"     in mods: modules_out["FVG"]      = {"state": fvg_s,  "label": fvg_l}
        if "FIB"     in mods: modules_out["FIB"]      = {"state": fib_s,  "label": fib_l}
        if "Breaker" in mods: modules_out["Breaker"]  = {"state": brk_s,  "label": brk_l}
        if "Bias"    in mods: modules_out["Bias"]     = {"state": bias_s, "label": bias_l}

        tfs_out[tf] = {
            "score":     tf_data.get("score", 0) or 0,
            "direction": tf_dir,
            "price":     raw_price,
            "modules":   modules_out,
        }

    return {
        "symbol":       scan_result.get("symbol", ""),
        "market":       market   or scan_result.get("market")   or "perpetual",
        "exchange":     exchange or scan_result.get("exchange") or "binance",
        "timeframes":   tfs,
        "modules":      mods,
        "tfs":          tfs_out,
        "refreshed_at": now_iso,
        "phase":        "phase3_mtf_scan",
    }


# ── Phase 4: Health / Status Engine helpers ──────────────────────────────────

def _lm_latest_mtf_scan_from_snapshot(snapshot: dict) -> dict:
    if not snapshot or not isinstance(snapshot, dict):
        return {}
    scan = snapshot.get("latest_mtf_scan")
    if not scan or not isinstance(scan, dict):
        return {}
    return scan


def _lm_get_tf_module(scan: dict, tf: str, module: str) -> dict:
    tfs_data = scan.get("tfs", {}) if scan else {}
    tf_data  = tfs_data.get(tf, {})
    if tf_data.get("error"):
        return {}
    return tf_data.get("modules", {}).get(module, {})


def _lm_direction_from_item(item) -> str:
    d = (getattr(item, "direction", None) or "").lower().strip()
    if d in ("bullish", "bull", "long", "up"):
        return "bullish"
    if d in ("bearish", "bear", "short", "down"):
        return "bearish"
    return "neutral"


def _lm_zone_health(item, current_price) -> dict:
    _no_zone = {"zone_status": "no_zone", "distance_pct": None,
                "inside_zone": False, "near_zone": False,
                "breach_risk": False}

    # ── Normalize zone values (Fix 2) ────────────────────────────────────────
    try:
        raw_zh = float(item.zone_high)
        raw_zl = float(item.zone_low)
    except (TypeError, ValueError):
        return {**_no_zone, "reason": "No zone set"}

    if raw_zh <= 0 or raw_zl <= 0:
        return {**_no_zone, "reason": "No zone set"}

    zh = max(raw_zh, raw_zl)   # normalized high
    zl = min(raw_zh, raw_zl)   # normalized low

    direction = _lm_direction_from_item(item)

    try:
        price = float(current_price)
    except (TypeError, ValueError):
        price = 0.0
    if price <= 0:
        return {**_no_zone, "reason": "No current price"}

    inside = zl <= price <= zh

    # ── Zone status with breach risk threshold (Fix 3) ────────────────────────
    if direction == "bullish":
        breach_risk = False
        if inside:
            zone_status, distance_pct = "inside", 0.0
        elif price > zh:
            # price above zone — measuring how far above high
            distance_pct = round((price - zh) / zh * 100, 2)
            zone_status  = "near" if distance_pct <= _LM_ZONE_NEAR_PCT else "watching"
        else:
            # price below zone_low — potential breach
            distance_pct = round((zl - price) / zl * 100, 2)
            if distance_pct > _LM_BREACH_RISK_PCT:
                zone_status, breach_risk = "breach_risk", True
            else:
                zone_status = "near"

    elif direction == "bearish":
        breach_risk = False
        if inside:
            zone_status, distance_pct = "inside", 0.0
        elif price < zl:
            # price below zone — measuring how far below low
            distance_pct = round((zl - price) / zl * 100, 2)
            zone_status  = "near" if distance_pct <= _LM_ZONE_NEAR_PCT else "watching"
        else:
            # price above zone_high — potential breach
            distance_pct = round((price - zh) / zh * 100, 2)
            if distance_pct > _LM_BREACH_RISK_PCT:
                zone_status, breach_risk = "breach_risk", True
            else:
                zone_status = "near"

    else:
        # neutral — no breach risk
        breach_risk = False
        if inside:
            zone_status, distance_pct = "inside", 0.0
        elif price < zl:
            distance_pct = round((zl - price) / zl * 100, 2)
            zone_status  = "near" if distance_pct <= _LM_ZONE_NEAR_PCT else "watching"
        else:
            distance_pct = round((price - zh) / zh * 100, 2)
            zone_status  = "near" if distance_pct <= _LM_ZONE_NEAR_PCT else "watching"

    # distance_pct may be unset for inside; normalise
    if inside:
        distance_pct = 0.0

    return {
        "zone_status":  zone_status,
        "zone_high":    zh,
        "zone_low":     zl,
        "distance_pct": distance_pct,
        "inside_zone":  inside,
        "near_zone":    zone_status == "near",
        "breach_risk":  breach_risk,
        "reason": (
            "Price inside zone"  if inside else
            "Zone breach risk"   if breach_risk else
            "Near zone (<2%)"    if zone_status == "near" else
            "Watching zone"
        ),
    }


def _lm_bias_alignment(scan: dict, direction: str) -> dict:
    def _tf_dir(tf):
        tfs_data = scan.get("tfs", {}) if scan else {}
        tf_data  = tfs_data.get(tf, {})
        if not tf_data or tf_data.get("error"):
            return None
        return tf_data.get("direction")

    bias_1h = _tf_dir("1h")
    bias_4h = _tf_dir("4h")
    bias_1d = _tf_dir("1d")

    if direction == "neutral" or bias_1h is None:
        bias_aligned = None
    else:
        bias_aligned = (bias_1h == direction)

    ctx_count = 0
    if bias_4h == direction:
        ctx_count += 1
    if bias_1d == direction:
        ctx_count += 1

    return {
        "bias_1h":               bias_1h,
        "bias_4h":               bias_4h,
        "bias_1d":               bias_1d,
        "bias_aligned":          bias_aligned,
        "context_aligned_count": ctx_count,
        "reason": (
            "1H bias aligned"   if bias_aligned is True  else
            "1H bias opposing"  if bias_aligned is False else
            "Bias undetermined"
        ),
    }


def _lm_module_confluence(scan: dict, direction: str) -> dict:
    if not scan:
        return {"module_score": 0, "confirmations": 0, "warnings": 0, "tf_summary": {}}

    tfs_data      = scan.get("tfs", {})
    confirmations = 0
    warnings      = 0
    tf_summary    = {}
    scored_tfs    = 0

    for tf, tf_data in tfs_data.items():
        if not tf_data or tf_data.get("error"):
            continue
        mods = tf_data.get("modules", {})
        if not mods:
            continue
        scored_tfs += 1
        tf_conf, tf_warn = 0, 0

        for mod_name, mod_data in mods.items():
            if mod_name == "Bias":
                continue
            state = (mod_data.get("state") or "none").lower()
            if state in ("strong", "yes"):
                tf_conf += 1
            elif state == "warn":
                tf_warn += 1

        if tf_conf > 0:
            confirmations += 1
        if tf_warn > 0 and tf_conf == 0:
            warnings += 1

        tf_summary[tf] = {"confirmations": tf_conf, "warnings": tf_warn}

    module_score = round(confirmations / scored_tfs * 100) if scored_tfs else 0

    return {
        "module_score":  module_score,
        "confirmations": confirmations,
        "warnings":      warnings,
        "tf_summary":    tf_summary,
    }


def _lm_original_setup_score(item, snapshot: dict) -> int:
    """Return the original signal/setup confidence score, never the previously-computed
    health score.  Priority order mirrors the spec (see Phase 4 hotfix Fix 1)."""
    def _safe_int(v):
        try:
            f = float(v)
            return int(f) if 0 <= f <= 100 else None
        except (TypeError, ValueError):
            return None

    # 1. Preserved in snapshot["original_setup_score"]
    v = _safe_int(snapshot.get("original_setup_score"))
    if v is not None:
        return v

    # 2–3. snapshot["row"]["score"] / ["confidence"]
    snap_row = snapshot.get("row") or {}
    v = _safe_int(snap_row.get("score"))
    if v is not None:
        return v
    v = _safe_int(snap_row.get("confidence"))
    if v is not None:
        return v

    # 4–5. snapshot["topAlert"]["score"] / ["confidence"]
    top = snapshot.get("topAlert") or {}
    v = _safe_int(top.get("score"))
    if v is not None:
        return v
    v = _safe_int(top.get("confidence"))
    if v is not None:
        return v

    # 6. item.confidence (never overwritten by health engine)
    v = _safe_int(getattr(item, "confidence", None))
    if v is not None:
        return v

    # 7. item.score only if no previous health run has set it yet
    if not snapshot.get("latest_health"):
        v = _safe_int(getattr(item, "score", None))
        if v is not None:
            return v

    return 0


def _lm_compute_health(item, _snap: dict = None) -> dict:
    now_iso   = datetime.now(timezone.utc).isoformat()
    snap      = _snap if _snap is not None else _json_loads_safe(item.snapshot_json, {})
    scan      = _lm_latest_mtf_scan_from_snapshot(snap)
    direction = _lm_direction_from_item(item)
    price     = item.current_price or 0.0

    zone = _lm_zone_health(item, price)
    bias = _lm_bias_alignment(scan, direction)
    conf = _lm_module_confluence(scan, direction)

    raw_score = _lm_original_setup_score(item, snap)
    base_pts  = min(20, round(raw_score / 100.0 * 20))

    zs = zone["zone_status"]
    zone_pts = {"inside": 25, "near": 18, "watching": 8, "breach_risk": 0}.get(zs, 5)

    ba = bias["bias_aligned"]
    bias_pts = 25 if ba is True else 10 if ba is None else 0

    ctx_pts  = min(15, bias["context_aligned_count"] * 8)
    mod_pts  = round(conf["module_score"] / 100.0 * 15)

    health_score = max(0, min(100, base_pts + zone_pts + bias_pts + ctx_pts + mod_pts))

    if health_score >= 80:
        grade = "A"
    elif health_score >= 65:
        grade = "B"
    elif health_score >= 50:
        grade = "C"
    elif health_score >= 35:
        grade = "D"
    else:
        grade = "F"

    if zone["breach_risk"]:
        status = "breach_risk"
    elif health_score >= 75 and ba is True and zs in ("inside", "near"):
        status = "confirmed"
    elif zs == "inside":
        status = "inzone"
    elif zs == "near":
        status = "near"
    elif ba is False or raw_score < 35:
        status = "warning"
    else:
        status = "watching"

    status_labels = {
        "confirmed":   "Confirmed",
        "inzone":      "In Zone",
        "near":        "Near Zone",
        "watching":    "Watching",
        "warning":     "Warning",
        "breach_risk": "Breach Risk",
    }

    checklist = [
        {"label": "Original Score ≥ 35", "pass": raw_score >= 35,
         "value": str(raw_score)},
        {"label": "Zone Active",           "pass": zs not in ("no_zone", "breach_risk"),
         "value": zone.get("reason", "")},
        {"label": "1H Bias Aligned",       "pass": ba is True,
         "value": bias.get("bias_1h") or "—"},
        {"label": "4H Context",            "pass": bias["bias_4h"] == direction,
         "value": bias.get("bias_4h") or "—"},
        {"label": "1D Context",            "pass": bias["bias_1d"] == direction,
         "value": bias.get("bias_1d") or "—"},
        {"label": "Module Confluence",     "pass": conf["confirmations"] >= 2,
         "value": f"{conf['confirmations']} TF(s)"},
    ]

    health_warnings = []
    if zone["breach_risk"]:
        health_warnings.append("Zone breach detected — price moved against setup")
    if ba is False:
        health_warnings.append("1H bias opposing setup direction")
    if conf["warnings"] > 0:
        health_warnings.append(f"{conf['warnings']} timeframe(s) showing conflicting signals")

    return {
        "phase":        "phase4_health",
        "computed_at":  now_iso,
        "health_score": health_score,
        "grade":        grade,
        "status":       status,
        "status_label": status_labels.get(status, "Watching"),
        "direction":    direction,
        "zone":         zone,
        "bias":         bias,
        "confluence":   conf,
        "checklist":    checklist,
        "reasons":      [zone["reason"], bias["reason"]],
        "warnings":     health_warnings,
        "scoring": {
            "base_pts": base_pts,
            "zone_pts": zone_pts,
            "bias_pts": bias_pts,
            "ctx_pts":  ctx_pts,
            "mod_pts":  mod_pts,
        },
    }


# ── Phase 4.5: Session Context Engine ────────────────────────────────────────

_LM_SESSION_MAP = [
    # (start_hour_inclusive, end_hour_exclusive, key, label, liquidity, volatility, scalp)
    ( 0,  7, "asia",         "Asia / Tokyo",           "medium", "medium", "normal"),
    ( 7,  8, "pre_london",   "Pre-London",             "medium", "medium", "normal"),
    ( 8, 10, "london_open",  "London Open",            "high",   "high",   "good"),
    (10, 12, "london",       "London Session",         "high",   "medium", "good"),
    (12, 17, "ny_overlap",   "London / NY Overlap",    "high",   "high",   "good"),
    (17, 21, "ny_afternoon", "New York Afternoon",     "medium", "medium", "normal"),
    (21, 24, "off_hours",    "Off-Hours",              "low",    "low",    "poor"),
]

_LM_SESSION_NOTES = {
    "asia": {
        "setup_quality_note": "Asian range levels can be key targets for London",
        "risk_note":          "Lower volatility — wider stops may be needed",
        "caution":            "medium",
        "ai_summary":         "Asia session active. Moderate context. Levels can respect SMC structure.",
        "bias_hint":          "Watch for range building and liquidity grabs near extremes",
    },
    "pre_london": {
        "setup_quality_note": "Be cautious — stop hunts likely before London open",
        "risk_note":          "High risk of fake breakouts near London open time",
        "caution":            "medium",
        "ai_summary":         "Pre-London period. Watch for liquidity sweeps before real move begins.",
        "bias_hint":          "Avoid entering at extremes — potential fakeout period",
    },
    "london_open": {
        "setup_quality_note": "Strong momentum possible — watch for clean OB/FVG entries",
        "risk_note":          "Fast moves can trigger stops quickly",
        "caution":            "low",
        "ai_summary":         "London open active. High volatility expansion window. Prime for OB/FVG setups.",
        "bias_hint":          "Confirm direction before entering — momentum can be fast",
    },
    "london": {
        "setup_quality_note": "London trend often continues from the open move",
        "risk_note":          "Monitor for reversals as New York approaches",
        "caution":            "low",
        "ai_summary":         "London session active. Market showing trending behavior.",
        "bias_hint":          "Ride existing trend, watch for New York liquidity setup",
    },
    "ny_overlap": {
        "setup_quality_note": "Best session for confluent setups — highest probability",
        "risk_note":          "Volatility spikes possible — use confirmation",
        "caution":            "low",
        "ai_summary":         "London/NY overlap active. Prime liquidity window. Best time for confirmed setups.",
        "bias_hint":          "Highest probability window — wait for confirmation before entry",
    },
    "ny_afternoon": {
        "setup_quality_note": "Watch for continuation or reversal of NY session trend",
        "risk_note":          "Reduced volume in later NY — moves can be choppy",
        "caution":            "medium",
        "ai_summary":         "New York afternoon session. Medium liquidity. Can show trend continuation.",
        "bias_hint":          "Check for momentum continuation — avoid new positions late session",
    },
    "off_hours": {
        "setup_quality_note": "Low liquidity — best to avoid new positions",
        "risk_note":          "Thin markets can cause erratic moves and wide spreads",
        "caution":            "high",
        "ai_summary":         "Off-hours period. Low liquidity. Not recommended for new entries.",
        "bias_hint":          "Avoid new positions — wait for Asia open",
    },
    "weekend": {
        "setup_quality_note": "Do not trade — weekend low liquidity",
        "risk_note":          "Weekend gaps can cause unexpected price action on Monday open",
        "caution":            "high",
        "ai_summary":         "Weekend period. Very low liquidity. Market context unreliable.",
        "bias_hint":          "Await Monday Asia session before assessing setups",
    },
}


def _lm_session_context(now_utc=None) -> dict:
    """Return current UTC-based market session context. Pure stdlib, no external calls."""
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    now_iso = now_utc.isoformat()
    h       = now_utc.hour
    m       = now_utc.minute
    wd      = now_utc.weekday()   # 0=Mon … 5=Sat, 6=Sun
    is_wknd = wd >= 5

    if is_wknd:
        key        = "weekend"
        label      = "Weekend / Low Liquidity"
        liquidity  = "low"
        volatility = "low"
        scalp      = "poor"
        is_prime   = False
        is_trans   = False
        # minutes to Monday 00:00 UTC
        days_to_mon = (7 - wd) % 7 or 7
        mins_to_next = days_to_mon * 24 * 60 - h * 60 - m
        next_label   = "Asia / Tokyo"
    else:
        key = label = liquidity = volatility = scalp = None
        is_prime = is_trans = False
        for (sh, eh, sk, sl, lq, vo, sc) in _LM_SESSION_MAP:
            if sh <= h < eh:
                key, label, liquidity, volatility, scalp = sk, sl, lq, vo, sc
                break

        is_prime = key == "ny_overlap"
        is_trans = key == "london_open"

        # minutes to next session boundary
        next_sh = None
        for i, (sh, eh, sk, sl, lq, vo, sc) in enumerate(_LM_SESSION_MAP):
            if sh <= h < eh:
                next_sh    = eh if eh < 24 else 0
                next_label = _LM_SESSION_MAP[(i + 1) % len(_LM_SESSION_MAP)][3]
                break
        if next_sh is None:
            next_sh, next_label = 0, "Asia / Tokyo"
        if next_sh == 0:
            mins_to_next = (24 - h) * 60 - m
        else:
            mins_to_next = (next_sh - h) * 60 - m

    notes = _LM_SESSION_NOTES.get(key, _LM_SESSION_NOTES["off_hours"])

    return {
        "phase":                  "phase4_5_session_context",
        "computed_at":            now_iso,
        "utc_hour":               h,
        "utc_minute":             m,
        "utc_weekday":            wd,
        "is_weekend":             is_wknd,
        "session_key":            key,
        "session_label":          label,
        "liquidity_label":        liquidity,
        "volatility_label":       volatility,
        "scalp_quality":          scalp,
        "setup_quality_note":     notes["setup_quality_note"],
        "risk_note":              notes["risk_note"],
        "is_prime_time":          is_prime,
        "is_transition_window":   is_trans,
        "minutes_to_next_session": mins_to_next,
        "next_session_label":     next_label,
        "ai_context": {
            "summary":              notes["ai_summary"],
            "session_bias_hint":    notes["bias_hint"],
            "recommended_caution":  notes["caution"],
        },
    }


# ── Phase 5: Aggregate Market Context Engine ─────────────────────────────────

def _lm_market_context_default(symbol: str, exchange: str, market: str,
                                reason: str = None, provider: str = "none") -> dict:
    """Return a safe placeholder market context dict. Used when provider fails or exchange unsupported."""
    from datetime import datetime, timezone as _tz
    return {
        "phase":      "phase5_market_context",
        "computed_at": datetime.now(_tz.utc).isoformat(),
        "symbol":     symbol,
        "exchange":   exchange,
        "market":     market,
        "provider":   provider,
        "ok":         False,
        "reason":     reason or "Market context unavailable",
        "funding": {
            "available":        False,
            "rate":             None,
            "rate_pct":         None,
            "next_funding_time": None,
            "bias":             "neutral",
            "label":            "Unavailable",
        },
        "open_interest": {
            "available":  False,
            "value":      None,
            "value_usd":  None,
            "change_pct": None,
            "bias":       "neutral",
            "label":      "Unavailable",
        },
        "long_short": {
            "available":  False,
            "ratio":      None,
            "long_pct":   None,
            "short_pct":  None,
            "bias":       "neutral",
            "label":      "Unavailable",
        },
        "taker_pressure": {
            "available":      False,
            "buy_sell_ratio": None,
            "buy_volume":     None,
            "sell_volume":    None,
            "bias":           "neutral",
            "label":          "Unavailable",
        },
        "liquidations": {
            "available":      False,
            "long_liq_usd":   None,
            "short_liq_usd":  None,
            "dominant_side":  "neutral",
            "label":          "Awaiting liquidation feed",
        },
        "activity": {
            "available":           False,
            "volume_24h":          None,
            "price_change_24h_pct": None,
            "label":               "Unavailable",
        },
        "summary": {
            "market_bias": "neutral",
            "risk_level":  "unknown",
            "notes":       [],
            "ai_context":  "Market context unavailable.",
        },
    }


def _lm_fetch_market_context(symbol: str, exchange: str = "binance",
                              market: str = "perpetual") -> dict:
    """Route market context fetch to the correct provider. Returns default on any failure."""
    try:
        exch = (exchange or "binance").lower().strip()
        mkt  = (market   or "perpetual").lower().strip()
        if exch == "binance":
            return _lm_fetch_binance_market_context(symbol, market=mkt)
        return _lm_market_context_default(symbol, exch, mkt,
                                          reason="Unsupported exchange for Phase 5")
    except Exception as _e:
        try:
            return _lm_market_context_default(symbol, exchange, market,
                                              reason=f"Provider error: {_e}")
        except Exception:
            return _lm_market_context_default("UNKNOWN", "unknown", "unknown",
                                              reason="Fatal provider error")


def _lm_fetch_binance_market_context(symbol: str, market: str = "perpetual") -> dict:
    """Fetch market context from Binance public endpoints. No API key required."""
    from datetime import datetime, timezone as _tz
    _sym   = (symbol or "").upper().strip()
    _base  = BINANCE_FUTURES_API   # https://fapi.binance.com
    _TOUT  = 5
    ctx    = _lm_market_context_default(_sym, "binance", market, provider="binance")
    ctx["ok"] = True   # optimistic; set False if all endpoints fail
    errors = []

    # ── 1. Premium index (funding rate + mark price) ──────────────────────────
    try:
        r = req.get(f"{_base}/fapi/v1/premiumIndex",
                    params={"symbol": _sym}, timeout=_TOUT)
        if r.status_code == 200:
            d = r.json()
            rate_raw = d.get("lastFundingRate")
            if rate_raw is not None:
                rate_f   = float(rate_raw)
                rate_pct = round(rate_f * 100, 6)
                if rate_pct > 0.03:
                    bias  = "long_crowded"
                    label = "Positive funding — longs pay"
                elif rate_pct < -0.03:
                    bias  = "short_crowded"
                    label = "Negative funding — shorts pay"
                else:
                    bias  = "neutral"
                    label = "Neutral funding"
                nft = d.get("nextFundingTime")
                ctx["funding"] = {
                    "available":         True,
                    "rate":              rate_f,
                    "rate_pct":          rate_pct,
                    "next_funding_time": int(nft) if nft else None,
                    "bias":              bias,
                    "label":             label,
                }
        else:
            errors.append(f"premiumIndex HTTP {r.status_code}")
    except Exception as _e:
        errors.append(f"premiumIndex: {_e}")

    # ── 2. Open interest ──────────────────────────────────────────────────────
    try:
        r = req.get(f"{_base}/fapi/v1/openInterest",
                    params={"symbol": _sym}, timeout=_TOUT)
        if r.status_code == 200:
            d   = r.json()
            oi  = d.get("openInterest")
            lp  = None
            try:
                lp_r = req.get(f"{_base}/fapi/v1/premiumIndex",
                               params={"symbol": _sym}, timeout=_TOUT)
                if lp_r.status_code == 200:
                    lp = float(lp_r.json().get("markPrice") or 0) or None
            except Exception:
                pass
            oi_f   = float(oi) if oi is not None else None
            oi_usd = round(oi_f * lp, 0) if (oi_f and lp) else None
            ctx["open_interest"] = {
                "available":  True,
                "value":      oi_f,
                "value_usd":  oi_usd,
                "change_pct": None,
                "bias":       "neutral",
                "label":      f"OI: {oi_f:,.0f}" if oi_f else "OI available",
            }
        else:
            errors.append(f"openInterest HTTP {r.status_code}")
    except Exception as _e:
        errors.append(f"openInterest: {_e}")

    # ── 3. 24h ticker (volume + price change) ─────────────────────────────────
    try:
        r = req.get(f"{_base}/fapi/v1/ticker/24hr",
                    params={"symbol": _sym}, timeout=_TOUT)
        if r.status_code == 200:
            d   = r.json()
            qv  = d.get("quoteVolume")
            pcp = d.get("priceChangePercent")
            qv_f  = float(qv)  if qv  is not None else None
            pcp_f = float(pcp) if pcp is not None else None
            ctx["activity"] = {
                "available":            True,
                "volume_24h":           qv_f,
                "price_change_24h_pct": pcp_f,
                "label":                f"Vol 24h: {qv_f:,.0f} USDT" if qv_f else "Active",
            }
        else:
            errors.append(f"ticker24hr HTTP {r.status_code}")
    except Exception as _e:
        errors.append(f"ticker24hr: {_e}")

    # ── 4. Global long / short account ratio ──────────────────────────────────
    try:
        r = req.get(f"{_base}/futures/data/globalLongShortAccountRatio",
                    params={"symbol": _sym, "period": "15m", "limit": 1}, timeout=_TOUT)
        if r.status_code == 200:
            data = r.json()
            if data and isinstance(data, list):
                d     = data[0]
                ratio = d.get("longShortRatio")
                lp_   = d.get("longAccount")
                sp_   = d.get("shortAccount")
                ratio_f = float(ratio) if ratio is not None else None
                lp_f    = float(lp_)   if lp_   is not None else None
                sp_f    = float(sp_)   if sp_   is not None else None
                if ratio_f is not None:
                    if ratio_f > 1.2:
                        ls_bias  = "long_crowded"
                        ls_label = f"Ratio {ratio_f:.2f} — Long heavy"
                    elif ratio_f < 0.8:
                        ls_bias  = "short_crowded"
                        ls_label = f"Ratio {ratio_f:.2f} — Short heavy"
                    else:
                        ls_bias  = "neutral"
                        ls_label = f"Ratio {ratio_f:.2f} — Balanced"
                    ctx["long_short"] = {
                        "available": True,
                        "ratio":     round(ratio_f, 4),
                        "long_pct":  round(lp_f * 100, 2) if lp_f is not None else None,
                        "short_pct": round(sp_f * 100, 2) if sp_f is not None else None,
                        "bias":      ls_bias,
                        "label":     ls_label,
                    }
        else:
            errors.append(f"globalLongShortRatio HTTP {r.status_code}")
    except Exception as _e:
        errors.append(f"globalLongShortRatio: {_e}")

    # ── 5. Taker long / short ratio ───────────────────────────────────────────
    try:
        r = req.get(f"{_base}/futures/data/takerlongshortRatio",
                    params={"symbol": _sym, "period": "15m", "limit": 1}, timeout=_TOUT)
        if r.status_code == 200:
            data = r.json()
            if data and isinstance(data, list):
                d      = data[0]
                bsr    = d.get("buySellRatio")
                bv     = d.get("buyVol")
                sv     = d.get("sellVol")
                bsr_f  = float(bsr) if bsr is not None else None
                bv_f   = float(bv)  if bv  is not None else None
                sv_f   = float(sv)  if sv  is not None else None
                if bsr_f is not None:
                    if bsr_f > 1.15:
                        tk_bias  = "buy_pressure"
                        tk_label = f"B/S {bsr_f:.3f} — Buy pressure"
                    elif bsr_f < 0.85:
                        tk_bias  = "sell_pressure"
                        tk_label = f"B/S {bsr_f:.3f} — Sell pressure"
                    else:
                        tk_bias  = "neutral"
                        tk_label = f"B/S {bsr_f:.3f} — Balanced"
                    ctx["taker_pressure"] = {
                        "available":      True,
                        "buy_sell_ratio": round(bsr_f, 4),
                        "buy_volume":     bv_f,
                        "sell_volume":    sv_f,
                        "bias":           tk_bias,
                        "label":          tk_label,
                    }
        else:
            errors.append(f"takerLongShortRatio HTTP {r.status_code}")
    except Exception as _e:
        errors.append(f"takerLongShortRatio: {_e}")

    # ── 6. Liquidations: not faked — real-time liquidation stream not in public REST ─
    ctx["liquidations"] = {
        "available":     False,
        "long_liq_usd":  None,
        "short_liq_usd": None,
        "dominant_side": "neutral",
        "label":         "Awaiting liquidation feed",
    }

    # ── Summary / market bias ─────────────────────────────────────────────────
    biases = []
    if ctx["funding"]["available"]:
        biases.append(ctx["funding"]["bias"])
    if ctx["long_short"]["available"]:
        biases.append(ctx["long_short"]["bias"])
    if ctx["taker_pressure"]["available"]:
        biases.append(ctx["taker_pressure"]["bias"])

    bull_signals = biases.count("buy_pressure")
    bear_signals = biases.count("sell_pressure")
    long_heavy   = biases.count("long_crowded")
    short_heavy  = biases.count("short_crowded")

    if bull_signals > bear_signals and bull_signals > 0:
        market_bias = "bullish"
    elif bear_signals > bull_signals and bear_signals > 0:
        market_bias = "bearish"
    elif long_heavy > 0 or short_heavy > 0:
        market_bias = "mixed"
    else:
        market_bias = "neutral"

    risk_factors = 0
    if ctx["funding"]["available"] and abs(ctx["funding"].get("rate_pct") or 0) > 0.05:
        risk_factors += 1
    if ctx["long_short"]["available"] and ctx["long_short"]["bias"] != "neutral":
        risk_factors += 1
    risk_level = "high" if risk_factors >= 2 else "medium" if risk_factors == 1 else "low"

    notes = []
    if ctx["funding"]["available"]:
        notes.append(ctx["funding"]["label"])
    if ctx["taker_pressure"]["available"]:
        notes.append(ctx["taker_pressure"]["label"])
    if ctx["long_short"]["available"]:
        notes.append(ctx["long_short"]["label"])
    if ctx["activity"]["available"] and ctx["activity"].get("price_change_24h_pct") is not None:
        pcp = ctx["activity"]["price_change_24h_pct"]
        notes.append(f"24h change: {pcp:+.2f}%")

    if errors:
        ctx["ok"] = False if not ctx["funding"]["available"] and not ctx["taker_pressure"]["available"] else True
        notes.append(f"Partial data ({len(errors)} endpoint(s) failed)")

    ai_parts = []
    if ctx["funding"]["available"]:
        ai_parts.append(f"Funding {ctx['funding']['rate_pct']:+.4f}% ({ctx['funding']['bias']})")
    if ctx["taker_pressure"]["available"]:
        ai_parts.append(f"taker {ctx['taker_pressure']['bias']}")
    if ctx["long_short"]["available"]:
        ai_parts.append(f"L/S ratio {ctx['long_short']['ratio']}")
    ai_context = (", ".join(ai_parts) + ".") if ai_parts else "Market context partially unavailable."

    ctx["summary"] = {
        "market_bias": market_bias,
        "risk_level":  risk_level,
        "notes":       notes,
        "ai_context":  ai_context,
    }
    ctx["computed_at"] = datetime.now(_tz.utc).isoformat()
    return ctx


def _lm_attach_market_context(row, snapshot: dict = None,
                               force: bool = False) -> tuple:
    """Fetch market context and store it in the snapshot dict. Does NOT commit."""
    if snapshot is None:
        snapshot = _json_loads_safe(row.snapshot_json, {})
    try:
        market_ctx = _lm_fetch_market_context(
            row.symbol,
            getattr(row, "exchange", None) or "binance",
            getattr(row, "market",   None) or "perpetual",
        )
    except Exception as _e:
        market_ctx = _lm_market_context_default(
            row.symbol,
            getattr(row, "exchange", None) or "binance",
            getattr(row, "market",   None) or "perpetual",
            reason=f"Attach error: {_e}",
        )
    snapshot["latest_market_context"]  = market_ctx
    snapshot["last_market_context_at"] = market_ctx.get("computed_at")
    return snapshot, market_ctx


# ── Phase 6: AI Agent / DeepSeek Intelligence Layer ──────────────────────────

def _lm_ai_config() -> dict:
    """Read AI provider config from environment. Never returns the actual API key."""
    key      = os.environ.get("OPENROUTER_API_KEY", "")
    model    = os.environ.get("OPENROUTER_MODEL", "")
    api_base = os.environ.get("OPENROUTER_API_BASE",
                               "https://openrouter.ai/api/v1/chat/completions")
    app_title = os.environ.get("OPENROUTER_APP_TITLE", "ZyNi SMC Screener")
    http_ref  = os.environ.get("OPENROUTER_HTTP_REFERER", "")
    has_key   = bool(key and key.strip())
    has_model = bool(model and model.strip())
    return {
        "configured":   has_key and has_model,
        "provider":     "openrouter",
        "api_base":     api_base,
        "model":        model if has_model else "",
        "app_title":    app_title,
        "http_referer": http_ref,
        "has_key":      has_key,
    }


def _lm_ai_is_configured() -> bool:
    """Return True only if both API key and model are set in environment."""
    return _lm_ai_config()["configured"]


# ── Phase 6.5: Multi-Provider AI Router ──────────────────────────────────────

def _lm_ai_agents_config() -> list:
    """Return safe list of configured AI agents. Never returns actual API keys."""
    agents = []

    # ── Method A: JSON config from AI_AGENTS_JSON env var ────────────────────
    raw_json = os.environ.get("AI_AGENTS_JSON", "").strip()
    if raw_json:
        try:
            entries = json.loads(raw_json)
            if isinstance(entries, list):
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    key_env   = entry.get("api_key_env", "")
                    base_env  = entry.get("api_base_env", "")
                    key_val   = os.environ.get(key_env, "").strip() if key_env else ""
                    base_val  = os.environ.get(base_env, "").strip() if base_env else ""
                    model     = (entry.get("model") or "").strip()
                    has_key   = bool(key_val)
                    configured = has_key and bool(model) and entry.get("enabled", True)
                    agents.append({
                        "id":          entry.get("id", "unknown"),
                        "label":       entry.get("label", "Unknown"),
                        "provider":    entry.get("provider", "openrouter"),
                        "model":       model,
                        "configured":  configured,
                        "has_key":     has_key,
                        "enabled":     bool(entry.get("enabled", True)),
                        "api_base":    base_val or "",
                        # Internal backend-only fields (never sent to frontend)
                        "_api_key_env":  key_env,
                        "_api_base_env": base_env,
                    })
                if agents:
                    return agents
        except Exception:
            pass  # Fall through to Method B

    # ── Method B: Build from individual env vars ──────────────────────────────
    # Default OpenRouter (Phase 6 backward compat)
    or_key   = os.environ.get("OPENROUTER_API_KEY", "").strip()
    or_model = os.environ.get("OPENROUTER_MODEL", "").strip()
    or_base  = os.environ.get("OPENROUTER_API_BASE",
                               "https://openrouter.ai/api/v1/chat/completions").strip()
    agents.append({
        "id":         "default_openrouter",
        "label":      "OpenRouter",
        "provider":   "openrouter",
        "model":      or_model,
        "configured": bool(or_key and or_model),
        "has_key":    bool(or_key),
        "enabled":    True,
        "api_base":   or_base,
    })

    # Optional simple additional agents
    _SIMPLE_AGENTS = [
        ("OPENAI_API_KEY",    "OPENAI_MODEL",    "OPENAI_API_BASE",    "openai_direct", "ChatGPT",   "openai",        "https://api.openai.com/v1/chat/completions"),
        ("ANTHROPIC_API_KEY", "ANTHROPIC_MODEL", "ANTHROPIC_API_BASE", "anthropic",     "Claude",    "anthropic",     ""),
        ("GEMINI_API_KEY",    "GEMINI_MODEL",     "GEMINI_API_BASE",   "gemini",        "Gemini",    "gemini",        ""),
        ("DEEPSEEK_API_KEY",  "DEEPSEEK_MODEL",  "DEEPSEEK_API_BASE",  "deepseek",      "DeepSeek",  "deepseek",      "https://api.deepseek.com/v1/chat/completions"),
        ("CUSTOM_AI_API_KEY", "CUSTOM_AI_MODEL", "CUSTOM_AI_API_BASE", "custom",        "Custom AI", "custom_openai", ""),
    ]
    for key_env, model_env, base_env, aid, label, provider, default_base in _SIMPLE_AGENTS:
        k = os.environ.get(key_env, "").strip()
        m = os.environ.get(model_env, "").strip()
        b = os.environ.get(base_env, "").strip() or default_base
        if k or m:  # include if any env is set
            agents.append({
                "id":         aid,
                "label":      label,
                "provider":   provider,
                "model":      m,
                "configured": bool(k and m),
                "has_key":    bool(k),
                "enabled":    True,
                "api_base":   b,
            })

    # Always include local fallback at end
    agents.append({
        "id":         "local_fallback",
        "label":      "Local Fallback",
        "provider":   "local_fallback",
        "model":      "rule_based",
        "configured": True,
        "has_key":    False,
        "enabled":    True,
        "api_base":   "",
    })

    return agents


def _lm_get_ai_agent_config(agent_id: str = None) -> dict:
    """Return a single agent config by id. Never returns actual API keys."""
    agents = _lm_ai_agents_config()

    # Find by id
    if agent_id:
        for a in agents:
            if a["id"] == agent_id:
                return a

    # First configured (non-fallback) agent
    for a in agents:
        if a.get("configured") and a["id"] != "local_fallback":
            return a

    # Absolute fallback
    for a in agents:
        if a["id"] == "local_fallback":
            return a

    return {
        "id": "local_fallback", "label": "Local Fallback", "provider": "local_fallback",
        "model": "rule_based", "configured": True, "has_key": False, "enabled": True, "api_base": "",
    }


# ── Phase 6.6: AI Brain Rules + Custom Instructions Layer ─────────────────────

def _lm_builtin_ai_brain_rules() -> dict:
    """
    Return the built-in AI Brain rules for the Live Monitor AI Agent.

    This is NOT user custom instructions.
    This is core AI reasoning intelligence — always present, not removable.
    The user should NOT need to instruct the AI on basic OB/FIB/order-flow logic.
    """
    return {
        "phase": "phase6_6_builtin_ai_brain",
        "principle": (
            "Built-in AI Brain handles core trading reasoning. "
            "Custom instructions are only extra user preferences layered on top."
        ),
        "primary_entry_modules": [
            {
                "key":   "order_block",
                "label": "Order Block",
                "role":  "Can be a primary setup/trade zone.",
                "notes": [
                    "Use OB zone context, touch/reaction quality, zone validity, "
                    "liquidity around zone, and confirmation quality.",
                    "Do not require user to instruct basic OB logic.",
                    "Do not assume entry from OB alone without context/risk.",
                ],
            },
            {
                "key":   "fibonacci",
                "label": "Fibonacci",
                "role":  "Can be a primary setup/trade zone.",
                "notes": [
                    "Use FIB reaction/rejection quality, key levels, trend leg context, "
                    "liquidity around FIB, wick/close behavior, and confirmation quality.",
                    "Do not require user to instruct basic FIB logic.",
                    "FIB can be primary logic, not only confirmation.",
                ],
            },
            {
                "key":   "order_flow",
                "label": "Order Flow",
                "role":  "Future independent scalp mode and current confirmation layer.",
                "notes": [
                    "Use order-flow facts when available: taker pressure, OI, funding, "
                    "liquidations, volume, sweep/rejection context.",
                    "Order-flow-only trading is more dangerous and must be treated as "
                    "scalp/watch candidate until Risk Guard exists.",
                ],
            },
        ],
        "confirmation_modules": [
            "FVG", "Breaker", "Bias", "Session", "Liquidity",
            "Open Interest", "Funding", "Taker Pressure", "Liquidations",
            "Wick Rejection", "Volume", "Market Context",
        ],
        "reasoning_modes": {
            "setup_mode": "OB or FIB can be primary setup; other modules confirm.",
            "hybrid_mode": "OB/FIB primary setup plus order-flow confirmation.",
            "order_flow_mode_future": (
                "Order-flow can become independent scalp mode later after event "
                "detection, memory, paper trading, and Risk Guard."
            ),
        },
        "safety_rules": [
            "Analysis only. Not financial advice.",
            "Do not place trades.",
            "Do not claim guaranteed outcome.",
            "Do not change strategy rules automatically.",
            "Do not treat custom instructions as permission to bypass risk.",
            "If data is missing, say what is missing.",
        ],
    }


# Blocked phrase fragments (lower-case match) for _lm_instruction_is_safe
_LM_BLOCKED_INSTRUCTION_PHRASES = [
    "place trade", "place order", "place buy", "place sell",
    "buy always", "sell always", "buy now", "sell now",
    "ignore stop loss", "ignore sl", "remove stop loss", "remove sl",
    "bypass risk", "override risk", "bypass risk guard", "override risk guard",
    "change strategy code", "change strategy rules", "modify strategy",
    "use exchange account", "connect exchange", "use api key",
    "revenge trade", "martingale",
    "execute trade", "execute order", "open position", "close position automatically",
]

_LM_INSTRUCTION_TRIGGER_PHRASES = [
    "from now", "remember", "watch for", "alert me", "ignore",
    "only consider", "be stricter", "do not mark", "require confirmation",
    "save this instruction", "add instruction", "only alert", "filter",
    "consider stronger", "consider weaker",
]


def _lm_instruction_is_safe(text: str) -> tuple:
    """
    Check if a custom AI instruction text is safe to store.

    Returns (True, "") if safe, or (False, reason) if blocked.
    Custom instructions must be extra preferences/filters, not execution permissions.
    """
    if not text or not text.strip():
        return False, "Instruction text is empty."
    t = text.strip().lower()
    for phrase in _LM_BLOCKED_INSTRUCTION_PHRASES:
        if phrase in t:
            return False, (
                f"Instruction blocked: contains forbidden phrase '{phrase}'. "
                "Custom instructions must be analysis preferences only, "
                "not execution or risk-bypass commands."
            )
    return True, ""


def _lm_custom_ai_instructions_from_snapshot(snapshot: dict) -> list:
    """Read active custom AI instructions from a snapshot dict."""
    raw = snapshot.get("custom_ai_instructions") if snapshot else None
    if not isinstance(raw, list):
        return []
    return [i for i in raw if isinstance(i, dict) and i.get("is_active", True)]


def _lm_add_custom_ai_instruction(row, text: str, source: str = "chat") -> tuple:
    """
    Add a custom AI instruction to a LiveMonitorItem snapshot.
    Validates safety, max-length, and max-count.
    Does NOT commit — caller must commit.
    Returns (snapshot_dict, new_instruction_dict).
    """
    import uuid as _uuid
    from datetime import datetime, timezone as _tz

    snap = _json_loads_safe(getattr(row, "snapshot_json", None), {})
    if not isinstance(snap.get("custom_ai_instructions"), list):
        snap["custom_ai_instructions"] = []

    # Validate
    text = (text or "").strip()[:300]
    if not text:
        return snap, None

    safe, reason = _lm_instruction_is_safe(text)
    if not safe:
        return snap, {"blocked": True, "reason": reason}

    # Max 20 active instructions per item
    active_count = sum(
        1 for i in snap["custom_ai_instructions"]
        if isinstance(i, dict) and i.get("is_active", True)
    )
    if active_count >= 20:
        return snap, {"blocked": True, "reason": "Max 20 active instructions reached. Remove one first."}

    ins_id  = "ins_" + _uuid.uuid4().hex[:8]
    new_ins = {
        "id":         ins_id,
        "text":       text,
        "scope":      "item",
        "source":     source if source in ("chat", "manual") else "manual",
        "created_at": datetime.now(_tz.utc).isoformat(),
        "is_active":  True,
    }
    snap["custom_ai_instructions"].append(new_ins)
    row.snapshot_json = _json_dumps_safe(snap)
    return snap, new_ins


def _lm_remove_custom_ai_instruction(row, instruction_id: str) -> tuple:
    """
    Mark a custom AI instruction as inactive.
    Does NOT commit — caller must commit.
    Returns (snapshot_dict, removed_instruction_dict | None).
    """
    snap = _json_loads_safe(getattr(row, "snapshot_json", None), {})
    if not isinstance(snap.get("custom_ai_instructions"), list):
        return snap, None

    removed = None
    for ins in snap["custom_ai_instructions"]:
        if isinstance(ins, dict) and ins.get("id") == instruction_id and ins.get("is_active", True):
            ins["is_active"] = False
            removed = ins
            break

    if removed:
        row.snapshot_json = _json_dumps_safe(snap)
    return snap, removed


def _lm_extract_instruction_from_chat(message: str) -> dict:
    """
    Detect whether a user chat message is meant as a custom AI instruction.

    Returns {"text": cleaned_text, "confidence": 0-100} if instruction-like,
    or {"text": "", "confidence": 0} if it is a normal question.

    Rules:
    - High-confidence (>= 70) trigger phrases cause a save attempt.
    - Vague questions or single-word messages return low confidence.
    - Questions ending in "?" are treated as normal chat unless they also
      contain a strong instruction phrase.
    """
    if not message or not message.strip():
        return {"text": "", "confidence": 0}

    msg   = message.strip()
    lower = msg.lower()

    # Strong instruction trigger phrases (each adds to confidence)
    _STRONG_TRIGGERS = [
        ("save this instruction", 90),
        ("add instruction",       90),
        ("from now on",           85),
        ("remember this",         85),
        ("remember that",         80),
        ("from now",              75),
        ("do not mark",           75),
        ("only consider",         75),
        ("require confirmation",  75),
        ("be stricter",           75),
        ("alert me when",         75),
        ("alert me only",         75),
        ("only alert",            75),
        ("watch for",             70),
        ("ignore weak",           70),
        ("filter weak",           70),
        ("consider stronger",     70),
        ("consider weaker",       70),
    ]

    best_conf = 0
    for phrase, conf in _STRONG_TRIGGERS:
        if phrase in lower:
            best_conf = max(best_conf, conf)

    # Penalty: ends with question mark and no strong trigger already dominated
    if lower.rstrip().endswith("?") and best_conf < 80:
        best_conf = max(0, best_conf - 30)

    # Penalty: very short messages are likely questions not instructions
    if len(msg) < 15 and best_conf < 90:
        best_conf = max(0, best_conf - 20)

    return {"text": msg[:300], "confidence": best_conf}


# ── Phase 6.7: Event Detection Engine ────────────────────────────────────────

def _lm_detect_setup_events(row, snapshot=None) -> dict:
    """Detect important setup events from already-computed backend facts.
    No candle data, no exchange calls, no trading logic.
    Returns a structured detection dict with events list + summary.
    """
    from datetime import timezone as _tz
    import datetime as _dt

    snap  = snapshot if snapshot is not None else _json_loads_safe(getattr(row, "snapshot_json", None), {})
    lh    = snap.get("latest_health")    or {}
    sess  = snap.get("latest_session_context") or {}
    mktc  = snap.get("latest_market_context")  or {}
    ins   = _lm_custom_ai_instructions_from_snapshot(snap)

    price     = row.current_price
    raw_high  = row.zone_high
    raw_low   = row.zone_low
    # Normalize so z_hi >= z_lo regardless of how the zone was stored
    if raw_high is not None and raw_low is not None:
        z_hi = max(raw_high, raw_low)
        z_lo = min(raw_high, raw_low)
    else:
        z_hi = raw_high
        z_lo = raw_low
    direction  = (row.direction  or "").lower()
    setup_type = (row.setup_type or "")

    events: list = []

    # ── 1. Price-to-zone proximity ────────────────────────────────────────────
    if price is not None and z_hi is not None and z_lo is not None:
        zone_mid  = (z_hi + z_lo) / 2.0
        zone_size = max(z_hi - z_lo, 0.0001)

        if z_lo <= price <= z_hi:
            events.append({
                "event_key": "zone_touch",
                "type":      "zone_touch",
                "label":     "Price inside zone",
                "severity":  "watch",
                "direction": direction,
                "source":    "built_in",
                "details": {
                    "price":      price,
                    "zone_low":   z_lo,
                    "zone_high":  z_hi,
                    "setup_type": setup_type,
                },
            })
        else:
            dist_pct = (min(abs(price - z_hi), abs(price - z_lo)) / zone_mid * 100.0
                        if zone_mid else None)
            if dist_pct is not None and dist_pct <= 1.5:
                events.append({
                    "event_key": "zone_near",
                    "type":      "zone_near",
                    "label":     f"Price near zone ({dist_pct:.2f}% away)",
                    "severity":  "info",
                    "direction": direction,
                    "source":    "built_in",
                    "details": {
                        "price":      price,
                        "zone_low":   z_lo,
                        "zone_high":  z_hi,
                        "dist_pct":   round(dist_pct, 3),
                    },
                })

    # ── 2. Zone breach risk from health ──────────────────────────────────────
    zone_info  = lh.get("zone") or {}
    zone_state = (zone_info.get("state") or "").lower()
    if zone_state in ("breach_risk", "breached", "invalidated"):
        events.append({
            "event_key": "zone_breach_risk",
            "type":      "zone_breach_risk",
            "label":     f"Zone breach risk — health: {zone_state}",
            "severity":  "risk",
            "direction": direction,
            "source":    "health",
            "details": {
                "zone_state":    zone_state,
                "health_score":  lh.get("health_score"),
                "grade":         lh.get("grade"),
            },
        })

    # ── 3. Bias warning from health ───────────────────────────────────────────
    bias_info = lh.get("bias") or {}
    bias_dir  = (bias_info.get("direction") or bias_info.get("bias") or "").lower()
    bias_str  = (bias_info.get("strength")  or bias_info.get("label") or "").lower()
    if bias_dir and direction:
        is_bull = "bull" in direction
        is_bear = "bear" in direction
        bias_bull = "bull" in bias_dir
        bias_bear = "bear" in bias_dir
        if (is_bull and bias_bear) or (is_bear and bias_bull):
            events.append({
                "event_key": "bias_warning_conflict",
                "type":      "bias_warning",
                "label":     f"Bias conflict — setup {direction}, bias {bias_dir}",
                "severity":  "warning",
                "direction": direction,
                "source":    "health",
                "details": {
                    "setup_direction": direction,
                    "bias_direction":  bias_dir,
                    "bias_strength":   bias_str,
                },
            })
        elif "weak" in bias_str:
            events.append({
                "event_key": "bias_warning_weak",
                "type":      "bias_warning",
                "label":     "Weak bias — setup may lack momentum",
                "severity":  "info",
                "direction": direction,
                "source":    "health",
                "details": {
                    "bias_direction": bias_dir,
                    "bias_strength":  bias_str,
                },
            })

    # ── 4. High-risk session warning ──────────────────────────────────────────
    if sess:
        liq_label  = (sess.get("liquidity_label") or "").lower()
        vol_label  = (sess.get("volatility_label") or "").lower()
        is_weekend = sess.get("is_weekend", False)
        is_trans   = sess.get("is_transition_window", False)

        if "low" in liq_label or is_weekend:
            events.append({
                "event_key": "session_low_liquidity",
                "type":      "session_warning",
                "label":     "Low-liquidity session — wider spreads risk",
                "severity":  "warning",
                "direction": direction,
                "source":    "session",
                "details": {
                    "session":         sess.get("session_label"),
                    "liquidity_label": liq_label,
                    "is_weekend":      is_weekend,
                },
            })
        if "extreme" in vol_label or "high" in vol_label:
            events.append({
                "event_key": "session_high_volatility",
                "type":      "session_warning",
                "label":     "High-volatility session — increased slippage risk",
                "severity":  "warning",
                "direction": direction,
                "source":    "session",
                "details": {
                    "session":          sess.get("session_label"),
                    "volatility_label": vol_label,
                    "is_transition":    is_trans,
                },
            })

    # ── 5. Market pressure events ─────────────────────────────────────────────
    if mktc and mktc.get("ok"):
        funding = mktc.get("funding") or {}
        taker   = mktc.get("taker_pressure") or {}
        ls      = mktc.get("long_short") or {}

        if funding.get("available"):
            rate      = funding.get("rate_pct")
            fund_bias = (funding.get("bias") or "").lower()
            if rate is not None and abs(rate) >= 0.05:
                # Crowded same-side = caution (over-positioning reversal risk)
                # Actual bias values: long_crowded / short_crowded / neutral
                is_caution = (
                    ("bull" in direction and fund_bias == "long_crowded") or
                    ("bear" in direction and fund_bias == "short_crowded")
                )
                events.append({
                    "event_key": "market_funding_pressure",
                    "type":      "market_pressure",
                    "label":     f"High funding rate {rate:+.4f}% ({fund_bias})",
                    "severity":  "warning" if is_caution else "info",
                    "direction": direction,
                    "source":    "market_context",
                    "details": {
                        "funding_rate_pct": rate,
                        "funding_bias":     fund_bias,
                        "is_caution":       is_caution,
                    },
                })

        if taker.get("available"):
            taker_bias = (taker.get("bias") or "").lower()
            # Actual bias values: buy_pressure / sell_pressure / neutral
            is_against = (
                ("bull" in direction and taker_bias == "sell_pressure") or
                ("bear" in direction and taker_bias == "buy_pressure")
            )
            if is_against:
                events.append({
                    "event_key": "market_taker_pressure_against",
                    "type":      "market_pressure",
                    "label":     f"Taker pressure against setup ({taker_bias})",
                    "severity":  "warning",
                    "direction": direction,
                    "source":    "market_context",
                    "details": {
                        "taker_bias":        taker_bias,
                        "against_direction": True,
                    },
                })

        if ls.get("available"):
            ratio   = ls.get("ratio")
            ls_bias = (ls.get("bias") or "").lower()
            if ratio is not None:
                if ratio > 2.5 or ratio < 0.4:
                    extreme_side = "long-heavy" if ratio > 2.5 else "short-heavy"
                    # Crowded same-side = caution
                    is_caution = (
                        ("bull" in direction and extreme_side == "long-heavy") or
                        ("bear" in direction and extreme_side == "short-heavy")
                    )
                    events.append({
                        "event_key": "market_ls_extreme",
                        "type":      "market_pressure",
                        "label":     f"Extreme L/S ratio {ratio:.2f} ({extreme_side})",
                        "severity":  "warning" if is_caution else "info",
                        "direction": direction,
                        "source":    "market_context",
                        "details": {
                            "ls_ratio":    ratio,
                            "extreme_side": extreme_side,
                            "is_caution":  is_caution,
                        },
                    })

    # ── 6. Custom instruction watch note ─────────────────────────────────────
    if ins:
        events.append({
            "event_key": "instruction_watch_active",
            "type":      "instruction_watch",
            "label":     f"{len(ins)} active custom instruction(s) applied",
            "severity":  "info",
            "direction": direction,
            "source":    "custom_instruction",
            "details": {
                "instruction_count": len(ins),
                "instructions": [i.get("text", "")[:80] for i in ins],
            },
        })

    # ── 7. Candle-feature events (Phase 7.1) ─────────────────────────────────
    cf_block   = snap.get("latest_candle_features") or {}
    cf         = cf_block.get("features") or {}
    if cf:
        rej      = (cf.get("strong_rejection") or "none").lower()
        vol_rat  = cf.get("volume_spike_ratio") or 0.0
        compress = cf.get("compression", False)
        bk_ctx   = (cf.get("breakout_context") or "inside_range").lower()
        last_lo  = cf.get("last_low")
        last_hi  = cf.get("last_high")
        last_cl  = cf.get("last_close")
        p_hi50   = cf.get("prev_range_high_50")
        p_lo50   = cf.get("prev_range_low_50")

        # 7a. Wick rejection event
        if rej in ("bullish", "bearish"):
            aligns = (rej == "bullish" and "bull" in direction) or \
                     (rej == "bearish" and "bear" in direction)
            events.append({
                "event_key": f"candle_wick_rejection_{rej}",
                "type":      "candle_wick_rejection",
                "label":     f"{'Bullish' if rej=='bullish' else 'Bearish'} wick rejection on last candle",
                "severity":  "watch" if aligns else "warning",
                "direction": direction,
                "source":    "candle_features",
                "details": {
                    "rejection_type":   rej,
                    "aligns_with_setup": aligns,
                    "body_pct":         cf.get("last_candle_body_pct"),
                    "upper_wick_pct":   cf.get("last_upper_wick_pct"),
                    "lower_wick_pct":   cf.get("last_lower_wick_pct"),
                },
            })

        # 7b. Volume spike event
        if vol_rat >= 1.5:
            events.append({
                "event_key": "candle_volume_spike",
                "type":      "candle_volume_spike",
                "label":     f"Volume spike — {vol_rat:.1f}× 20-candle average",
                "severity":  "watch" if vol_rat >= 2.0 else "info",
                "direction": direction,
                "source":    "candle_features",
                "details": {
                    "volume_spike_ratio": vol_rat,
                    "last_volume":        cf.get("last_volume"),
                    "avg_volume_20":      cf.get("avg_volume_20"),
                },
            })

        # 7c. Compression event
        if compress:
            events.append({
                "event_key": "candle_compression",
                "type":      "candle_compression",
                "label":     "Price compressed — ATR below 5% of 50-candle range",
                "severity":  "watch",
                "direction": direction,
                "source":    "candle_features",
                "details": {
                    "atr_14":        cf.get("atr_14"),
                    "range_high_50": cf.get("range_high_50"),
                    "range_low_50":  cf.get("range_low_50"),
                },
            })

        # 7d. Breakout context event
        if bk_ctx in ("above_range", "below_range"):
            aligns_bk = (bk_ctx == "above_range" and "bull" in direction) or \
                        (bk_ctx == "below_range" and "bear" in direction)
            events.append({
                "event_key": f"candle_breakout_{bk_ctx}",
                "type":      "candle_breakout_context",
                "label":     f"Price {'above' if bk_ctx=='above_range' else 'below'} 50-candle range",
                "severity":  "watch" if aligns_bk else "warning",
                "direction": direction,
                "source":    "candle_features",
                "details": {
                    "breakout_context": bk_ctx,
                    "aligns_with_setup": aligns_bk,
                    "range_high_50":    cf.get("range_high_50"),
                    "range_low_50":     cf.get("range_low_50"),
                },
            })

        # ── 8. OB/FIB reaction near zone (Task 4) ────────────────────────────
        is_ob_fib = any(k in (setup_type or "").lower()
                        for k in ("ob", "order_block", "order block",
                                  "fib", "fibonacci"))
        price_in_zone = (z_hi is not None and z_lo is not None and
                         price is not None and z_lo <= price <= z_hi)
        price_near_zone = False
        if not price_in_zone and z_hi is not None and z_lo is not None and price is not None:
            zone_mid  = (z_hi + z_lo) / 2.0
            zone_size = max(z_hi - z_lo, 0.0001)
            dist_pct  = (min(abs(price - z_hi), abs(price - z_lo)) / zone_mid * 100.0
                         if zone_mid else None)
            price_near_zone = dist_pct is not None and dist_pct <= 1.5

        if is_ob_fib and (price_in_zone or price_near_zone) and rej in ("bullish", "bearish"):
            zone_lbl = "OB" if any(k in (setup_type or "").lower()
                                   for k in ("ob", "order_block", "order block")) else "FIB"
            aligns_rej = (rej == "bullish" and "bull" in direction) or \
                         (rej == "bearish" and "bear" in direction)
            if aligns_rej:
                events.append({
                    "event_key": f"setup_reaction_near_zone_{rej}",
                    "type":      "setup_reaction_near_zone",
                    "label":     f"{'Bullish' if rej=='bullish' else 'Bearish'} rejection near {zone_lbl} zone",
                    "severity":  "watch",
                    "direction": direction,
                    "source":    "candle_features",
                    "details": {
                        "setup_type":     setup_type,
                        "zone_label":     zone_lbl,
                        "rejection_type": rej,
                        "price_in_zone":  price_in_zone,
                    },
                })
            else:
                events.append({
                    "event_key": f"candle_rejection_against_setup_{rej}",
                    "type":      "candle_rejection_against_setup",
                    "label":     f"Rejection against setup direction near {zone_lbl} zone",
                    "severity":  "warning",
                    "direction": direction,
                    "source":    "candle_features",
                    "details": {
                        "setup_type":     setup_type,
                        "zone_label":     zone_lbl,
                        "rejection_type": rej,
                        "price_in_zone":  price_in_zone,
                    },
                })

        # ── 9. Sweep-style rejection detector (Task 5) ───────────────────────
        if (last_lo is not None and last_cl is not None and
                last_hi is not None and p_hi50 is not None and p_lo50 is not None):
            bullish_sweep_style = (last_lo < p_lo50 and last_cl > p_lo50 and rej == "bullish")
            bearish_sweep_style = (last_hi > p_hi50 and last_cl < p_hi50 and rej == "bearish")
            if bullish_sweep_style:
                events.append({
                    "event_key": "range_sweep_style_rejection_bullish",
                    "type":      "range_sweep_style_rejection",
                    "label":     "Sweep-style bullish rejection below prev range low (not confirmed sweep)",
                    "severity":  "watch",
                    "direction": direction,
                    "source":    "candle_features",
                    "details": {
                        "last_low":          last_lo,
                        "last_close":        last_cl,
                        "prev_range_low_50": p_lo50,
                        "strong_rejection":  rej,
                        "note": "Wick dipped below prev range low and closed back above — sweep-style only",
                    },
                })
            elif bearish_sweep_style:
                events.append({
                    "event_key": "range_sweep_style_rejection_bearish",
                    "type":      "range_sweep_style_rejection",
                    "label":     "Sweep-style bearish rejection above prev range high (not confirmed sweep)",
                    "severity":  "watch",
                    "direction": direction,
                    "source":    "candle_features",
                    "details": {
                        "last_high":          last_hi,
                        "last_close":         last_cl,
                        "prev_range_high_50": p_hi50,
                        "strong_rejection":   rej,
                        "note": "Wick pushed above prev range high and closed back below — sweep-style only",
                    },
                })

    now_iso = _dt.datetime.now(_tz.utc).isoformat()
    severity_order = {"risk": 0, "warning": 1, "watch": 2, "info": 3}
    events.sort(key=lambda e: severity_order.get(e.get("severity", "info"), 3))

    risk_count    = sum(1 for e in events if e.get("severity") == "risk")
    warning_count = sum(1 for e in events if e.get("severity") == "warning")
    watch_count   = sum(1 for e in events if e.get("severity") == "watch")
    info_count    = sum(1 for e in events if e.get("severity") == "info")

    top_severity = "info"
    if risk_count:    top_severity = "risk"
    elif warning_count: top_severity = "warning"
    elif watch_count:   top_severity = "watch"

    return {
        "phase":        "phase6_7_event_detection",
        "computed_at":  now_iso,
        "symbol":       row.symbol,
        "events":       events,
        "summary": {
            "total_events":   len(events),
            "risk_count":     risk_count,
            "warning_count":  warning_count,
            "watch_count":    watch_count,
            "info_count":     info_count,
            "top_severity":   top_severity,
        },
    }


def _lm_attach_event_detection(row, snapshot=None) -> tuple:
    """Run event detector and store results in snapshot dict.
    Does NOT commit. Returns (snap, detection_dict).
    """
    snap      = snapshot if snapshot is not None else _json_loads_safe(getattr(row, "snapshot_json", None), {})
    detection = _lm_detect_setup_events(row, snapshot=snap)
    snap["latest_event_detection"]    = detection
    snap["last_event_detection_at"]   = detection["computed_at"]
    return snap, detection


# ── Phase 9.0: Exchange Data Source Abstraction ──────────────────────────────

_LM_ALLOWED_EXCHANGES = {"binance", "mexc"}
_LM_ALLOWED_MARKETS   = {"perpetual", "spot"}


def _lm_normalize_exchange(value) -> str:
    """Normalize exchange name to lowercase known value. Fallback: 'binance'."""
    v = (value or "").lower().strip()
    return v if v in _LM_ALLOWED_EXCHANGES else "binance"


def _lm_normalize_market(value) -> str:
    """Normalize market name to lowercase known value. Fallback: 'perpetual'."""
    v = (value or "").lower().strip()
    return v if v in _LM_ALLOWED_MARKETS else "perpetual"


def _lm_normalize_symbol(symbol: str, exchange: str = "binance", market: str = "perpetual") -> str:
    """Normalize symbol for internal use: uppercase, no dashes/underscores.

    e.g. 'btc_usdt' → 'BTCUSDT', 'BTC-USDT' → 'BTCUSDT'
    """
    return (symbol or "").upper().replace("-", "").replace("_", "").strip()


def _lm_data_source_config(row, snapshot=None) -> dict:
    """Build data_source_config dict for a Live Monitor item.

    Describes which exchange/market/source is used for candles, live price,
    and market context. Reads persisted overrides from snapshot["data_sources"]
    then falls back to row.exchange/market. No DB write. No trading.
    """
    snap   = snapshot if snapshot is not None else _json_loads_safe(getattr(row, "snapshot_json", None), {})
    stored = snap.get("data_sources") or {}

    exec_exchange = _lm_normalize_exchange(
        stored.get("execution_exchange") or getattr(row, "exchange", None) or "binance"
    )
    exec_market = _lm_normalize_market(
        stored.get("execution_market") or getattr(row, "market", None) or "perpetual"
    )
    exec_symbol = _lm_normalize_symbol(
        getattr(row, "symbol", None) or "",
        exec_exchange, exec_market,
    )
    candle_src     = _lm_normalize_exchange(stored.get("candle_source")     or exec_exchange)
    live_price_src = _lm_normalize_exchange(stored.get("live_price_source") or exec_exchange)

    raw_mcs = stored.get("market_context_sources")
    if isinstance(raw_mcs, list):
        mcs = [s for s in [_lm_normalize_exchange(x) for x in raw_mcs] if s in _LM_ALLOWED_EXCHANGES]
    else:
        mcs = ["binance"]
    if not mcs:
        mcs = ["binance"]

    agg             = bool(stored.get("aggregation_enabled", False))
    fallback_policy = stored.get("fallback_policy") or "warn_no_fallback"

    warnings = []
    if exec_exchange == "mexc":
        warnings.append("MEXC execution exchange — candle/price data uses MEXC public API only.")
    if agg and len(mcs) < 2:
        warnings.append("aggregation_enabled=true but only one market_context_source configured.")

    return {
        "phase":                  "phase9_data_sources",
        "execution_exchange":     exec_exchange,
        "execution_market":       exec_market,
        "execution_symbol":       exec_symbol,
        "candle_source":          candle_src,
        "live_price_source":      live_price_src,
        "market_context_sources": mcs,
        "aggregation_enabled":    agg,
        "fallback_policy":        fallback_policy,
        "warnings":               warnings,
    }


def _lm_mexc_capability_status_from_env() -> dict:
    """Return MEXC capability status from environment only — no API calls, no secrets returned.

    Phase 9.05: used by GET /api/live-monitor/mexc-capability-status.
    scripts/mexc_capability_audit.py is the full audit tool; this is a quick env check only.
    """
    import os as _os
    keys_present = bool(_os.environ.get("MEXC_API_KEY") and _os.environ.get("MEXC_API_SECRET"))
    demo_mode    = _os.environ.get("MEXC_DEMO_MODE", "false").lower()
    script_path  = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)),
                                 "scripts", "mexc_capability_audit.py")
    script_exists = _os.path.isfile(script_path)
    return {
        "script_exists":           script_exists,
        "keys_present":            keys_present,
        "demo_mode_env":           demo_mode,
        "safe_to_run_public_audit": True,
        "order_submit_tested":     False,
        "warning": (
            "demo_order_submit_supported is always untested — "
            "this helper never calls order-submit endpoints. "
            "Run scripts/mexc_capability_audit.py for the full public candle/ticker audit."
        ),
    }


# ── Phase 7: Candle Feature Engine ───────────────────────────────────────────

_LM_ALLOWED_CANDLE_INTERVALS = {"5m", "15m", "30m", "1h", "4h", "1d"}
_BINANCE_KLINE_MAX            = 1500   # Binance hard limit per request

# Process-level TTL cache: "exchange:market:symbol:interval:limit" → {"ts": float, "candles": list}
_lm_candle_ttl_cache: Dict[str, Any] = {}
_lm_candle_ttl_lock  = threading.Lock()


def _lm_get_candles_for_features(
    symbol: str,
    interval: str,
    limit: int = 1000,
    ttl: int = 60,
    exchange: str = "binance",
    market: str = "perpetual",
) -> list:
    """Return candles from process-level TTL cache; fetch only when stale or missing.

    Key includes exchange, market, symbol, interval, and limit so different
    request shapes never alias. TTL default 60s so batch detect-all loops
    share one fetch per unique combination.
    """
    key = f"{exchange.lower()}:{market.lower()}:{symbol.upper()}:{interval}:{limit}"
    now = time.time()
    with _lm_candle_ttl_lock:
        entry = _lm_candle_ttl_cache.get(key)
        if entry and (now - entry["ts"]) < ttl:
            return entry["candles"]
    # Phase 9.1: route to MEXC public candles if exchange == mexc
    if exchange.lower() == "mexc":
        candles = _lm_fetch_mexc_perp_candles(symbol, interval, limit=limit)
    else:
        candles = _lm_fetch_futures_candles(symbol, interval, limit=limit)
    if candles:
        with _lm_candle_ttl_lock:
            _lm_candle_ttl_cache[key] = {"ts": time.time(), "candles": candles}
    return candles


def _lm_candle_cache_bust(
    symbol: str,
    interval: str,
    limit: int = 1000,
    exchange: str = "binance",
    market: str = "perpetual",
) -> None:
    """Remove the exact cache entry for this combination (forces next call to re-fetch)."""
    key = f"{exchange.lower()}:{market.lower()}:{symbol.upper()}:{interval}:{limit}"
    with _lm_candle_ttl_lock:
        _lm_candle_ttl_cache.pop(key, None)


def _lm_fetch_futures_candles(symbol: str, interval: str, limit: int = 4000) -> list:
    """Fetch Binance Futures klines for *symbol* / *interval*.

    Paginates backwards when limit > _BINANCE_KLINE_MAX.
    Returns a list of dicts with keys:
        open_time, open, high, low, close, volume, close_time  (all normalised).
    Returns [] on any error.
    No API key required.
    """
    if interval not in _LM_ALLOWED_CANDLE_INTERVALS:
        return []
    limit = max(1, min(limit, 4000))
    url   = f"{BINANCE_FUTURES_API}/fapi/v1/klines"

    def _parse(batch):
        out = []
        for k in batch:
            try:
                out.append({
                    "open_time":  int(k[0]),
                    "open":       float(k[1]),
                    "high":       float(k[2]),
                    "low":        float(k[3]),
                    "close":      float(k[4]),
                    "volume":     float(k[5]),
                    "close_time": int(k[6]),
                })
            except (IndexError, TypeError, ValueError):
                pass
        return out

    try:
        all_candles = []
        remaining   = limit
        end_time    = None  # None → use current time (Binance default)

        while remaining > 0:
            batch_size = min(remaining, _BINANCE_KLINE_MAX)
            params = {"symbol": symbol, "interval": interval, "limit": batch_size}
            if end_time is not None:
                params["endTime"] = end_time

            resp = req.get(url, params=params, timeout=10)
            if resp.status_code != 200:
                break
            data = resp.json()
            if not isinstance(data, list) or len(data) == 0:
                break

            batch = _parse(data)
            if not batch:
                break

            # Prepend so final list is chronologically ascending
            all_candles = batch + all_candles
            remaining  -= len(batch)

            if len(data) < batch_size:
                # Binance returned fewer than requested → we have all available
                break

            # Move end_time backwards to just before the oldest candle we got
            end_time = int(data[0][0]) - 1
            if end_time <= 0:
                break

        # Deduplicate by open_time (keep last occurrence — newest wins on overlap)
        seen  = {}
        for c in all_candles:
            seen[c["open_time"]] = c
        deduped = sorted(seen.values(), key=lambda x: x["open_time"])
        return deduped[-limit:] if len(deduped) > limit else deduped

    except Exception:
        return []


# ── Phase 9.1: MEXC Candle + Live Price Helpers ───────────────────────────────

_LM_MEXC_INTERVAL_MAP = {
    "5m":  "Min5",
    "15m": "Min15",
    "30m": "Min30",
    "1h":  "Min60",
    "4h":  "Hour4",
    "1d":  "Day1",
}
_LM_MEXC_INTERVAL_MS = {
    "Min5":  300_000,
    "Min15": 900_000,
    "Min30": 1_800_000,
    "Min60": 3_600_000,
    "Hour4": 14_400_000,
    "Day1":  86_400_000,
}


def _lm_fetch_mexc_perp_candles(symbol: str, interval: str, limit: int = 1000) -> list:
    """Fetch MEXC perpetual klines and normalise to the same shape as _lm_fetch_futures_candles.

    symbol   : canonical form BTCUSDT  → converted to BTC_USDT for MEXC
    interval : ZyNi standard (5m/15m/30m/1h/4h/1d)
    Returns list of dicts: open_time, open, high, low, close, volume, close_time
    Returns [] on any error. No API key. No order logic.
    """
    mx_iv = _LM_MEXC_INTERVAL_MAP.get(interval)
    if not mx_iv:
        return []
    # MEXC perpetual symbol format: BTCUSDT → BTC_USDT
    sym = symbol.upper().replace("_", "")
    if sym.endswith("USDT"):
        mx_sym = sym[:-4] + "_USDT"
    else:
        mx_sym = sym
    limit = max(1, min(limit, 2000))
    try:
        r = req.get(
            f"{MEXC_PERP_API}/kline/{mx_sym}",
            params={"interval": mx_iv, "limit": limit},
            timeout=12,
        )
        if r.status_code != 200:
            return []
        data = r.json().get("data") or {}
        times  = data.get("time",  [])
        opens  = data.get("open",  [])
        highs  = data.get("high",  [])
        lows   = data.get("low",   [])
        closes = data.get("close", [])
        vols   = data.get("vol",   [])
        if not times:
            return []
        iv_ms = _LM_MEXC_INTERVAL_MS.get(mx_iv, 900_000)
        out = []
        for i in range(len(times)):
            try:
                open_ms = int(times[i]) * 1000
                out.append({
                    "open_time":  open_ms,
                    "open":       float(opens[i])  if i < len(opens)  else 0.0,
                    "high":       float(highs[i])  if i < len(highs)  else 0.0,
                    "low":        float(lows[i])   if i < len(lows)   else 0.0,
                    "close":      float(closes[i]) if i < len(closes) else 0.0,
                    "volume":     float(vols[i])   if i < len(vols)   else 0.0,
                    "close_time": open_ms + iv_ms - 1,
                })
            except (IndexError, TypeError, ValueError):
                pass
        # MEXC returns oldest-first; sort ascending by open_time
        out.sort(key=lambda x: x["open_time"])
        return out[-limit:] if len(out) > limit else out
    except Exception:
        return []


def _lm_fetch_mexc_perp_ticker(symbol: str) -> dict:
    """Fetch MEXC perpetual ticker for symbol. Returns compact dict or {} on error.

    No API key. No order logic. Public endpoint only.
    Returns: {ok, symbol, last_price, volume_24h, funding_rate}
    """
    sym = symbol.upper().replace("_", "")
    if sym.endswith("USDT"):
        mx_sym = sym[:-4] + "_USDT"
    else:
        mx_sym = sym
    try:
        r = req.get(f"{MEXC_PERP_API}/ticker", params={"symbol": mx_sym}, timeout=8)
        if r.status_code != 200:
            return {"ok": False, "error": f"status {r.status_code}"}
        body = r.json().get("data") or {}
        # API may return list or dict
        ticker = None
        if isinstance(body, list):
            for t in body:
                if (t.get("symbol") or "").replace("_", "").upper() == sym:
                    ticker = t
                    break
        elif isinstance(body, dict):
            ticker = body
        if not ticker:
            return {"ok": False, "error": "symbol_not_found"}
        return {
            "ok":          True,
            "symbol":      sym,
            "last_price":  float(ticker.get("lastPrice") or ticker.get("last") or 0),
            "volume_24h":  float(ticker.get("amount24")  or ticker.get("volume24") or 0),
            "funding_rate": float(ticker.get("fundingRate") or 0),
        }
    except Exception as e:
        return {"ok": False, "error": str(e)[:100]}


def _lm_get_live_price_for_item(row, snapshot=None) -> dict:
    """Return current live price for a Live Monitor item based on data_sources.live_price_source.

    Returns {ok, price, source_exchange, source_market} or {ok: False, error}.
    No API key. No order logic. Reads public ticker only.
    """
    snap   = snapshot if snapshot is not None else _json_loads_safe(getattr(row, "snapshot_json", None), {})
    config = _lm_data_source_config(row, snapshot=snap)
    symbol = config["execution_symbol"]
    src    = config["live_price_source"]

    if src == "mexc":
        ticker = _lm_fetch_mexc_perp_ticker(symbol)
        if ticker.get("ok"):
            return {
                "ok":             True,
                "price":          ticker["last_price"],
                "source_exchange": "mexc",
                "source_market":  "perpetual",
            }
        return {"ok": False, "error": ticker.get("error", "mexc_ticker_failed"),
                "source_exchange": "mexc"}

    # Default: return item.current_price (already updated by existing refresh pipeline)
    cp = getattr(row, "current_price", None)
    if cp is not None:
        return {
            "ok":             True,
            "price":          float(cp),
            "source_exchange": "binance",
            "source_market":  "perpetual",
        }
    return {"ok": False, "error": "no_current_price", "source_exchange": "binance"}


def _lm_extract_candle_features(candles: list) -> dict:
    """Extract compact scalar features from a list of normalised candle dicts.

    Input is the output of _lm_fetch_futures_candles.
    Returns a dict of plain numbers / booleans — no raw candle data.
    Returns {} if candles is empty or too short.
    """
    if not candles or len(candles) < 5:
        return {}

    n      = len(candles)
    closes = [c["close"]  for c in candles]
    highs  = [c["high"]   for c in candles]
    lows   = [c["low"]    for c in candles]
    opens  = [c["open"]   for c in candles]
    vols   = [c["volume"] for c in candles]

    last_close = closes[-1]
    if last_close == 0:
        return {}

    # ── Body / wick percentages (last candle) ──────────────────────────────
    lc = candles[-1]
    c_range = lc["high"] - lc["low"]
    body_pct = 0.0
    upper_wick_pct = 0.0
    lower_wick_pct = 0.0
    if c_range > 0:
        body    = abs(lc["close"] - lc["open"])
        upper_w = lc["high"] - max(lc["close"], lc["open"])
        lower_w = min(lc["close"], lc["open"]) - lc["low"]
        body_pct       = round(body    / c_range * 100, 1)
        upper_wick_pct = round(upper_w / c_range * 100, 1)
        lower_wick_pct = round(lower_w / c_range * 100, 1)

    # ── Volume metrics ─────────────────────────────────────────────────────
    vol_20 = vols[-20:] if n >= 20 else vols
    vol_avg_20 = sum(vol_20) / len(vol_20) if vol_20 else 0.0
    vol_ratio  = round(vols[-1] / vol_avg_20, 2) if vol_avg_20 > 0 else 0.0

    # ── ATR-14 (simple, close-to-close true range approx) ──────────────────
    atr_14 = 0.0
    if n >= 15:
        trs = []
        for i in range(n - 14, n):
            tr = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i]  - closes[i - 1]),
            )
            trs.append(tr)
        raw_atr = sum(trs) / len(trs)
        atr_14  = round(raw_atr / last_close * 100, 3)   # % of price

    # ── 50-candle range & price position ──────────────────────────────────
    window_50 = candles[-50:] if n >= 50 else candles
    range_high_50 = max(c["high"]  for c in window_50)
    range_low_50  = min(c["low"]   for c in window_50)
    span_50       = range_high_50 - range_low_50
    price_position_50 = 0.5
    if span_50 > 0:
        price_position_50 = round((last_close - range_low_50) / span_50, 3)

    # ── Previous 50-candle range (excluding last candle) ──────────────────
    if n >= 2:
        prev_window = candles[-51:-1] if n >= 51 else candles[:-1]
        prev_range_high_50 = round(max(c["high"] for c in prev_window), 6)
        prev_range_low_50  = round(min(c["low"]  for c in prev_window), 6)
    else:
        prev_range_high_50 = range_high_50
        prev_range_low_50  = range_low_50

    # ── Trend slope (20-candle linear regression slope, normalised) ────────
    trend_slope_20 = 0.0
    if n >= 20:
        xs = list(range(20))
        ys = closes[-20:]
        x_mean = 9.5
        y_mean = sum(ys) / 20
        num    = sum((xs[i] - x_mean) * (ys[i] - y_mean) for i in range(20))
        den    = sum((xs[i] - x_mean) ** 2              for i in range(20))
        if den > 0 and y_mean > 0:
            slope = num / den
            trend_slope_20 = round(slope / y_mean * 100, 4)  # % per candle

    # ── Strong rejection candle ────────────────────────────────────────────
    # bullish = strong lower wick (buyers rejected lower prices)
    # bearish = strong upper wick (sellers rejected higher prices)
    if body_pct < 30 and lower_wick_pct > 60:
        strong_rejection = "bullish"
    elif body_pct < 30 and upper_wick_pct > 60:
        strong_rejection = "bearish"
    else:
        strong_rejection = "none"

    # ── Compression (low ATR relative to 50-candle span) ──────────────────
    compression = False
    if span_50 > 0 and n >= 15:
        raw_atr_price = atr_14 / 100 * last_close
        compression   = bool(raw_atr_price < span_50 * 0.05)

    # ── Breakout context ───────────────────────────────────────────────────
    # Use prev range (excluding last candle) so last_close can exceed the boundary
    breakout_context = "inside_range"
    if last_close > prev_range_high_50:
        breakout_context = "above_range"
    elif last_close < prev_range_low_50:
        breakout_context = "below_range"

    return {
        "candle_count":         n,
        "last_close":           round(last_close, 6),
        "last_open":            round(opens[-1], 6),
        "last_high":            round(highs[-1], 6),
        "last_low":             round(lows[-1], 6),
        "last_candle_body_pct": body_pct,
        "last_upper_wick_pct":  upper_wick_pct,
        "last_lower_wick_pct":  lower_wick_pct,
        "last_volume":          round(vols[-1], 4),
        "avg_volume_20":        round(vol_avg_20, 4),
        "volume_spike_ratio":   vol_ratio,
        "atr_14":               atr_14,
        "range_high_50":        round(range_high_50, 6),
        "range_low_50":         round(range_low_50, 6),
        "prev_range_high_50":   prev_range_high_50,
        "prev_range_low_50":    prev_range_low_50,
        "price_position_50":    price_position_50,
        "trend_slope_20":       trend_slope_20,
        "strong_rejection":     strong_rejection,
        "compression":          compression,
        "breakout_context":     breakout_context,
    }


def _lm_attach_candle_features(row, interval: str = None, limit: int = 1000) -> tuple:
    """Fetch candles for *row*, extract features, store in snapshot.

    Does NOT commit.
    Returns (snap, candle_features_dict).
    candle_features_dict will be {} if fetch/extraction fails.
    """
    snap = _json_loads_safe(getattr(row, "snapshot_json", None), {})

    # Resolve interval: param → row.timeframe → fallback "15m"
    if not interval or interval not in _LM_ALLOWED_CANDLE_INTERVALS:
        tf = (getattr(row, "timeframe", None) or "").strip()
        interval = tf if tf in _LM_ALLOWED_CANDLE_INTERVALS else "15m"

    limit  = max(5, min(limit, 4000))

    # Phase 9.1: resolve candle source from data_source_config. Use the same
    # normalized execution_symbol the cache bust uses so keys always match.
    config         = _lm_data_source_config(row, snapshot=snap)
    symbol         = config["execution_symbol"]
    if not symbol:
        return snap, {}
    candle_src     = config["candle_source"]          # "binance" or "mexc"
    source_market  = config["execution_market"]       # "perpetual" or "spot"
    fallback_policy = config["fallback_policy"]

    warnings: list = []
    candles = _lm_get_candles_for_features(
        symbol, interval, limit=limit,
        exchange=candle_src, market=source_market,
    )

    # If MEXC fetch failed and fallback is allowed, try Binance with a warning
    if not candles and candle_src == "mexc":
        if fallback_policy == "warn_fallback_binance":
            candles = _lm_get_candles_for_features(
                symbol, interval, limit=limit,
                exchange="binance", market=source_market,
            )
            if candles:
                warnings.append("MEXC candles unavailable; fell back to Binance.")
                candle_src = "binance"
        else:
            warnings.append("MEXC candles unavailable; no fallback (warn_no_fallback policy).")

    features = _lm_extract_candle_features(candles)

    now_iso = datetime.utcnow().isoformat() + "Z"
    snap["latest_candle_features"] = {
        "phase":           "phase9_candle_features",
        "computed_at":     now_iso,
        "interval":        interval,
        "source_exchange": candle_src,
        "source_market":   source_market,
        "features":        features,
        "warnings":        warnings,
    }
    snap["last_candle_features_at"] = now_iso
    row.snapshot_json = _json_dumps_safe(snap)
    return snap, features


def _lm_maybe_refresh_candles(row, snap: dict, stale_seconds: int = 120) -> dict:
    """Refresh candle features into snap if missing or older than stale_seconds.

    Does NOT commit. Returns updated snap dict.
    No LiveMonitorEvent created here.
    """
    from datetime import datetime as _dt2, timezone as _tz2
    last_at = snap.get("last_candle_features_at")
    needs_refresh = True
    if last_at:
        try:
            age = (_dt2.now(_tz2.utc) - _dt2.fromisoformat(last_at.replace("Z", "+00:00"))).total_seconds()
            needs_refresh = age >= stale_seconds
        except Exception:
            needs_refresh = True
    if needs_refresh:
        tf = (getattr(row, "timeframe", None) or "").strip()
        interval = tf if tf in _LM_ALLOWED_CANDLE_INTERVALS else "15m"
        snap, _ = _lm_attach_candle_features(row, interval=interval, limit=1000)
    return snap


# ── Phase 7.5: Server-side Watch Loop ────────────────────────────────────────

_lm_watch_thread: Optional[threading.Thread] = None
_lm_watch_running = False
_LM_WATCH_IMPORTANT_TYPES = {
    "zone_touch", "zone_breach_risk", "session_warning", "market_pressure",
    "candle_breakout", "candle_rejection", "candle_volume_spike",
}


def _lm_server_watch_tick(item_id: int, uid: int) -> dict:
    """Full pipeline tick for one Live Monitor item: candle refresh + event detection + readiness.

    Creates LiveMonitorEvents for new detections. Updates server_watch.last_tick_at.
    Safe to call from background thread (uses app context if needed).
    """
    from models import db as _db, LiveMonitorItem as _LMI, LiveMonitorEvent as _LME
    row = _LMI.query.filter_by(id=item_id).first()
    if not row:
        return {"ok": False, "error": "not_found"}
    if row.user_id != uid:
        return {"ok": False, "error": "forbidden"}
    if not row.is_active:
        return {"ok": False, "error": "inactive"}

    snap = _json_loads_safe(row.snapshot_json, {})
    snap = _lm_maybe_refresh_candles(row, snap)

    # Remember previous readiness state for change detection (Task 5)
    prev_readiness_state = (snap.get("latest_setup_readiness") or {}).get("readiness_state")

    snap, detection = _lm_attach_event_detection(row, snapshot=snap)
    snap, readiness = _lm_attach_setup_readiness(row, snapshot=snap)

    # Phase 8.0 Task 5: capture memory if state changed or is a capture-worthy state
    new_readiness_state = readiness.get("readiness_state")
    if (new_readiness_state in _LM_MEMORY_CAPTURE_STATES or
            new_readiness_state != prev_readiness_state):
        snap, _sw_mem, _sw_appended = _lm_append_setup_memory(row, snapshot=snap, source="server_watch")

    # Phase 8.1 Task 8: auto update outcomes
    snap, _sw_oc = _lm_update_memory_outcomes(row, snapshot=snap)

    seen_keys: list = snap.get("event_detection_keys") or []
    new_events: list = []
    for ev in (detection.get("events") or []):
        ek = ev.get("event_key")
        if ev.get("type") in _LM_WATCH_IMPORTANT_TYPES and ek and ek not in seen_keys:
            new_events.append(ev)
            seen_keys.append(ek)
    snap["event_detection_keys"] = seen_keys[-100:]

    now_iso      = datetime.now(timezone.utc).isoformat()
    sw           = snap.get("server_watch") or {}
    interval_s   = max(30, min(int(sw.get("interval_seconds") or 60), 300))
    from datetime import datetime as _dt2, timezone as _tz2, timedelta as _td
    next_due_iso = (_dt2.now(_tz2.utc) + _td(seconds=interval_s)).isoformat()

    row.snapshot_json = _json_dumps_safe(snap)
    row.updated_at    = datetime.now(timezone.utc)

    try:
        _db.session.flush()
        for ev in new_events:
            lme = _LME(
                item_id           = row.id,
                user_id           = uid,
                symbol            = row.symbol,
                event_type        = ev.get("type", "updated"),
                event_description = ev.get("label", "Server watch event"),
                details_json      = _json_dumps_safe(ev.get("details", {})),
                price_at_event    = row.current_price,
            )
            _db.session.add(lme)
        _db.session.commit()
        # Update server_watch schema fields after successful commit
        snap2 = _json_loads_safe(row.snapshot_json, {})
        sw2   = snap2.get("server_watch") or {}
        sw2["last_tick_at"]    = now_iso
        sw2["next_due_at"]     = next_due_iso
        sw2["last_status"]     = "ok"
        sw2["last_error"]      = ""
        sw2["interval_seconds"] = interval_s
        snap2["server_watch"]  = sw2
        row.snapshot_json      = _json_dumps_safe(snap2)
        row.updated_at         = datetime.now(timezone.utc)
        _db.session.commit()
    except Exception as e:
        _db.session.rollback()
        # Best-effort: record error state in snapshot
        try:
            snap_e = _json_loads_safe(row.snapshot_json, {})
            sw_e   = snap_e.get("server_watch") or {}
            sw_e["last_status"]  = "error"
            sw_e["last_error"]   = str(e)[:200]
            sw_e["next_due_at"]  = next_due_iso
            snap_e["server_watch"] = sw_e
            row.snapshot_json = _json_dumps_safe(snap_e)
            _db.session.commit()
        except Exception:
            pass
        return {"ok": False, "error": str(e)}

    return {
        "ok":               True,
        "item_id":          item_id,
        "symbol":           row.symbol,
        "readiness_state":  readiness.get("readiness_state"),
        "new_events_logged": len(new_events),
        "ticked_at":        now_iso,
        "next_due_at":      next_due_iso,
    }


def _lm_watch_background_loop():
    """Background thread: tick server-watch items at their configured interval."""
    global _lm_watch_running
    print("[LM-WATCH] Background thread started")
    while _lm_watch_running:
        try:
            with app.app_context():
                from models import LiveMonitorItem as _LMI
                from datetime import datetime as _dt2, timezone as _tz2
                now = _dt2.now(_tz2.utc)
                active_rows = _LMI.query.filter_by(is_active=True).all()
                ticked_this_cycle = 0
                for row in active_rows:
                    if not _lm_watch_running or ticked_this_cycle >= 20:
                        break
                    snap = _json_loads_safe(row.snapshot_json, {})
                    sw = snap.get("server_watch") or {}
                    if not sw.get("enabled"):
                        continue
                    interval_s = max(30, min(int(sw.get("interval_seconds") or 60), 300))
                    last_tick_raw = sw.get("last_tick_at")
                    needs_tick = True
                    if last_tick_raw:
                        try:
                            lt = _dt2.fromisoformat(last_tick_raw.replace("Z", "+00:00"))
                            needs_tick = (now - lt).total_seconds() >= interval_s
                        except Exception:
                            needs_tick = True
                    if needs_tick:
                        try:
                            _lm_server_watch_tick(row.id, row.user_id)
                            ticked_this_cycle += 1
                        except Exception as e:
                            print(f"[LM-WATCH] Error ticking item {row.id} ({row.symbol}): {e}")
        except Exception as e:
            print(f"[LM-WATCH] Loop error: {e}")
        time.sleep(60)
    print("[LM-WATCH] Background thread stopped")


def _ensure_lm_watch_thread():
    """Start the LM server-watch background thread if not already running."""
    global _lm_watch_thread, _lm_watch_running
    if _lm_watch_thread and _lm_watch_thread.is_alive():
        return
    _lm_watch_running = True
    _lm_watch_thread = threading.Thread(target=_lm_watch_background_loop, daemon=True, name="lm-watch")
    _lm_watch_thread.start()


if os.environ.get("ZYNI_LM_SERVER_WATCH_ENABLED") == "1":
    _ensure_lm_watch_thread()


# ── Phase 7.2: Setup Readiness Engine ────────────────────────────────────────

def _lm_compute_setup_readiness(row, snapshot=None) -> dict:
    """Score detected events into a deterministic readiness verdict.

    Reads from snapshot: latest_event_detection, latest_candle_features,
    latest_health, latest_session_context, latest_market_context.
    Returns a structured readiness dict. Does NOT commit.
    """
    from datetime import datetime as _dt3, timezone as _tz3

    snap      = snapshot if snapshot is not None else _json_loads_safe(getattr(row, "snapshot_json", None), {})
    detection = snap.get("latest_event_detection") or {}
    events    = detection.get("events") or []
    lh        = snap.get("latest_health") or {}
    zone_info = lh.get("zone") or {}
    zone_state = (zone_info.get("state") or "").lower()

    confirmation_score = 0
    risk_score         = 0
    supporting_events  = []
    risk_events        = []
    missing_confirms   = []

    # Per-event scoring
    _confirmation_weights = {
        "zone_touch":                    15,
        "candle_wick_rejection":         15,
        "candle_volume_spike":           10,
        "setup_reaction_near_zone":      25,
        "range_sweep_style_rejection":   20,
        "candle_breakout_context":       10,
    }
    _risk_weights = {
        "zone_breach_risk":                  40,
        "candle_rejection_against_setup":    20,
        "market_taker_pressure_against":     20,
        "session_warning":                    10,
        "bias_warning":                       15,
        "candle_breakout_context":            10,  # breakout against direction
        "market_pressure":                    10,
    }

    direction = (getattr(row, "direction", "") or "").lower()

    for ev in events:
        ev_type  = ev.get("type", "")
        ev_sev   = ev.get("severity", "")
        ev_label = ev.get("label", "")

        # Confirmation events
        if ev_sev == "watch":
            w = _confirmation_weights.get(ev_type, 10)
            # Breakout against direction counts as risk not confirmation
            if ev_type == "candle_breakout_context":
                bk = (ev.get("details") or {}).get("breakout_context", "")
                aligns = (bk == "above_range" and "bull" in direction) or \
                         (bk == "below_range" and "bear" in direction)
                if not aligns:
                    risk_score += 10
                    risk_events.append(ev_label)
                    continue
            confirmation_score += w
            supporting_events.append(ev_label)

        elif ev_sev in ("warning", "risk"):
            w = _risk_weights.get(ev_type, 15 if ev_sev == "warning" else 30)
            if ev_sev == "risk":
                w = max(w, 30)
            risk_score += w
            risk_events.append(ev_label)

        elif ev_sev == "info":
            # info events: small confirmation boost
            confirmation_score += 5
            supporting_events.append(ev_label)

    # Cap scores at 100
    confirmation_score = min(100, confirmation_score)
    risk_score         = min(100, risk_score)

    # Check for key confirmations
    event_types_present = {ev.get("type") for ev in events}
    if "zone_touch" not in event_types_present and "zone_near" not in event_types_present:
        missing_confirms.append("Price not near zone")
    if "candle_wick_rejection" not in event_types_present and "setup_reaction_near_zone" not in event_types_present:
        missing_confirms.append("No candle rejection signal")
    if not snap.get("latest_candle_features"):
        missing_confirms.append("Candle features not loaded")

    # Determine readiness_state
    if zone_state in ("breached", "invalidated"):
        readiness_state = "avoid"
    elif zone_state == "breach_risk" or risk_score >= 70:
        readiness_state = "high_risk"
    elif confirmation_score >= 55 and risk_score < 35:
        readiness_state = "strong_watch"
    elif confirmation_score >= 25 and risk_score < 60:
        readiness_state = "active_watch"
    else:
        readiness_state = "wait"

    # Compact summary
    summary = (
        f"{readiness_state.replace('_',' ').title()} — "
        f"conf {confirmation_score}/100, risk {risk_score}/100. "
        f"{len(supporting_events)} supporting, {len(risk_events)} risk signal(s)."
    )

    return {
        "phase":               "phase7_2_setup_readiness",
        "computed_at":         _dt3.now(_tz3.utc).isoformat(),
        "symbol":              getattr(row, "symbol", ""),
        "readiness_state":     readiness_state,
        "confirmation_score":  confirmation_score,
        "risk_score":          risk_score,
        "supporting_events":   supporting_events[:10],
        "risk_events":         risk_events[:10],
        "missing_confirmations": missing_confirms,
        "summary":             summary,
    }


def _lm_attach_setup_readiness(row, snapshot=None) -> tuple:
    """Compute readiness and store in snapshot. Does NOT commit."""
    snap      = snapshot if snapshot is not None else _json_loads_safe(getattr(row, "snapshot_json", None), {})
    readiness = _lm_compute_setup_readiness(row, snapshot=snap)
    snap["latest_setup_readiness"]  = readiness
    snap["last_setup_readiness_at"] = readiness["computed_at"]
    row.snapshot_json = _json_dumps_safe(snap)
    return snap, readiness


# ── Phase 8.0: Reasoning Memory Foundation ───────────────────────────────────

_LM_MEMORY_CAPTURE_STATES = {"active_watch", "strong_watch", "high_risk", "avoid"}


def _lm_build_setup_memory_record(row, snapshot=None, source: str = "manual") -> dict:
    """Build a compact reasoning-memory record from current snapshot.

    No raw candles. No order/trade fields. Output is JSON-safe.
    """
    snap = snapshot if snapshot is not None else _json_loads_safe(getattr(row, "snapshot_json", None), {})

    sr  = snap.get("latest_setup_readiness") or {}
    evd = snap.get("latest_event_detection") or {}
    cf  = snap.get("latest_candle_features") or {}
    lh  = snap.get("latest_health") or {}
    ai  = snap.get("latest_ai_analysis") or {}
    con = snap.get("latest_ai_consensus") or {}

    symbol    = (getattr(row, "symbol", None) or "").upper().strip()
    direction = (getattr(row, "direction", None) or "").lower().strip()

    readiness_state    = sr.get("readiness_state") or "wait"
    confirmation_score = sr.get("confirmation_score") or 0
    risk_score         = sr.get("risk_score") or 0
    zone_high          = getattr(row, "zone_high", None) or 0
    zone_low           = getattr(row, "zone_low", None) or 0

    # Deterministic dedup signature — does not include timestamp or price
    sig_str = (
        f"{symbol}:{direction}:{readiness_state}:"
        f"{int(confirmation_score)}:{int(risk_score)}:"
        f"{round(zone_high, 4)}:{round(zone_low, 4)}"
    )
    memory_signature = hashlib.sha256(sig_str.encode()).hexdigest()[:16]

    # Candle summary — no raw candles
    feat = cf.get("features") or {}
    candle_summary = None
    if feat:
        candle_summary = {
            "interval":           cf.get("interval"),
            "candle_count":       feat.get("candle_count"),
            "last_close":         feat.get("last_close"),
            "atr_14":             feat.get("atr_14"),
            "strong_rejection":   feat.get("strong_rejection"),
            "compression":        feat.get("compression"),
            "breakout_context":   feat.get("breakout_context"),
            "price_position_50":  feat.get("price_position_50"),
            "trend_slope_20":     feat.get("trend_slope_20"),
            "volume_spike_ratio": feat.get("volume_spike_ratio"),
        }

    # Compact event lists — max 8 each, labels only
    supporting = (sr.get("supporting_events") or [])[:8]
    risk_evs   = (sr.get("risk_events") or [])[:8]
    all_evs    = evd.get("events") or []
    event_types = list(dict.fromkeys(
        e.get("type") for e in all_evs if e.get("type")
    ))[:12]

    # AI verdict — latest_ai_analysis["analysis"]["verdict/confidence"]
    # with fallback to top-level keys for older snapshots
    ai_analysis_block = ai.get("analysis") or {}
    ai_verdict     = (ai_analysis_block.get("verdict")
                      or ai.get("verdict"))
    ai_confidence  = (ai_analysis_block.get("confidence")
                      or ai.get("confidence_score")
                      or ai.get("confidence"))

    # Consensus — latest_ai_consensus["verdict/confidence"]
    consensus_verdict    = con.get("verdict") or con.get("consensus_verdict")
    consensus_confidence = con.get("confidence") or con.get("consensus_confidence")

    now_iso = datetime.now(timezone.utc).isoformat()
    rec_id  = hashlib.sha256(f"{symbol}:{now_iso}:{source}".encode()).hexdigest()[:12]

    return {
        "id":                   rec_id,
        "phase":                "phase8_reasoning_memory",
        "created_at":           now_iso,
        "source":               source,
        "symbol":               symbol,
        "exchange":             (getattr(row, "exchange", None) or "binance"),
        "market":               (getattr(row, "market",   None) or "perpetual"),
        "setup_type":           getattr(row, "setup_type", None),
        "direction":            direction,
        "timeframe":            getattr(row, "timeframe", None),
        "zone_high":            zone_high,
        "zone_low":             zone_low,
        "current_price":        getattr(row, "current_price", None),
        "readiness_state":      readiness_state,
        "confirmation_score":   confirmation_score,
        "risk_score":           risk_score,
        "supporting_events":    supporting,
        "risk_events":          risk_evs,
        "missing_confirmations": sr.get("missing_confirmations"),
        "event_types":          event_types,
        "candle_summary":       candle_summary,
        "ai_verdict":           ai_verdict,
        "ai_confidence":        ai_confidence,
        "consensus_verdict":    consensus_verdict,
        "consensus_confidence": consensus_confidence,
        "outcome_status":       "pending",
        "outcome_note":         "",
        "outcome_checked_at":   None,
        "max_favorable_pct":    None,
        "max_adverse_pct":      None,
        "memory_signature":     memory_signature,
    }


def _lm_build_memory_summary(records: list) -> dict:
    """Compute summary dict from a list of memory records."""
    total     = len(records)
    by_status = {}
    by_stype  = {}
    by_rstate = {}
    last_at   = None
    for r in records:
        st = r.get("outcome_status") or "pending"
        by_status[st]  = by_status.get(st, 0) + 1
        stype = r.get("setup_type") or "unknown"
        by_stype[stype] = by_stype.get(stype, 0) + 1
        rs = r.get("readiness_state") or "wait"
        by_rstate[rs]   = by_rstate.get(rs, 0) + 1
        cat = r.get("created_at")
        if cat and (last_at is None or cat > last_at):
            last_at = cat
    return {
        "total_records":     total,
        "pending_count":     by_status.get("pending", 0),
        "won_count":         by_status.get("won", 0),
        "lost_count":        by_status.get("lost", 0),
        "invalidated_count": by_status.get("invalidated", 0),
        "neutral_count":     by_status.get("neutral", 0),
        "by_setup_type":     by_stype,
        "by_readiness_state": by_rstate,
        "by_outcome_status": by_status,
        "last_record_at":    last_at,
    }


def _lm_append_setup_memory(row, snapshot=None, source: str = "manual") -> tuple:
    """Build and append a memory record to snapshot["setup_memory_records"] (max 50).

    Dedup: if same memory_signature appears in last 10 records, skip.
    Does NOT commit. Returns (snap, record, appended: bool).

    PHASE 9 SAFETY NOTE (Phase 8.3 audit):
    snapshot_json is appropriate for compact latest-state (max-50 rolling window here).
    Phase 9 trade records MUST NOT be stored fully in snapshot_json.
    Phase 9.3 will create a dedicated LiveMonitorTrade DB table for all trade records.
    snapshot_json may only hold active_trade_id / latest_trade_summary in Phase 9+.
    """
    snap    = snapshot if snapshot is not None else _json_loads_safe(getattr(row, "snapshot_json", None), {})
    records = list(snap.get("setup_memory_records") or [])

    record = _lm_build_setup_memory_record(row, snapshot=snap, source=source)
    sig    = record["memory_signature"]

    # Dedup against last 10 records — return the existing persisted record
    recent = records[-10:]
    for existing in reversed(recent):
        if existing.get("memory_signature") == sig:
            # Refresh summary without touching last_setup_memory_at
            summary = _lm_build_memory_summary(records)
            snap["setup_memory_summary"] = summary
            row.snapshot_json = _json_dumps_safe(snap)
            return snap, existing, False

    records.append(record)
    records = records[-50:]  # keep last 50

    now_iso = datetime.now(timezone.utc).isoformat()
    summary = _lm_build_memory_summary(records)

    snap["setup_memory_records"]   = records
    snap["setup_memory_summary"]   = summary
    snap["last_setup_memory_at"]   = now_iso
    row.snapshot_json = _json_dumps_safe(snap)
    return snap, record, True


# ── Phase 8.1: Outcome Tracker ───────────────────────────────────────────────

_LM_WIN_PCT   = 1.0   # +1.0% from record price → win (bullish) / -1.0% → win (bearish)
_LM_LOSS_PCT  = 0.6   # -0.6% from record price → loss (bullish) / +0.6% → loss (bearish)


def _lm_update_memory_outcomes(row, snapshot=None) -> tuple:
    """Update outcome_status for pending memory records using current row price.

    No raw candles. No entry/SL/TP simulation. No paper trading.
    Uses only the price at capture time vs current price.
    Returns (snap, updated_count).
    """
    snap    = snapshot if snapshot is not None else _json_loads_safe(getattr(row, "snapshot_json", None), {})
    records = list(snap.get("setup_memory_records") or [])
    if not records:
        return snap, 0

    current_price = getattr(row, "current_price", None)
    if not current_price:
        return snap, 0

    zone_high        = getattr(row, "zone_high", None) or 0
    zone_low         = getattr(row, "zone_low", None) or 0
    now_iso          = datetime.now(timezone.utc).isoformat()
    updated          = 0   # records that transitioned to won/lost/invalidated
    progress_changed = False  # pending records whose progress fields changed

    for rec in records:
        if rec.get("outcome_status") != "pending":
            continue
        cap_price = rec.get("current_price")
        if not cap_price or cap_price <= 0:
            continue
        direction = (rec.get("direction") or "").lower()

        pct_move = ((current_price - cap_price) / cap_price) * 100.0

        # Favorable / adverse from direction perspective
        if "bull" in direction or direction in ("long", "buy"):
            favorable_pct = pct_move
            adverse_pct   = -pct_move
        elif "bear" in direction or direction in ("short", "sell"):
            favorable_pct = -pct_move
            adverse_pct   = pct_move
        else:
            # No clear direction — skip outcome resolution
            continue

        # Update running extremes and check-time (always, even if still pending)
        prev_fav = rec.get("max_favorable_pct")
        prev_adv = rec.get("max_adverse_pct")
        new_fav  = max(favorable_pct, prev_fav) if prev_fav is not None else favorable_pct
        new_adv  = max(adverse_pct,   prev_adv) if prev_adv is not None else adverse_pct
        if new_fav != prev_fav or new_adv != prev_adv or rec.get("outcome_checked_at") is None:
            progress_changed = True
        rec["max_favorable_pct"]  = new_fav
        rec["max_adverse_pct"]    = new_adv
        rec["outcome_checked_at"] = now_iso

        # Zone breach / invalidation check (direction-specific)
        zone_invalidated = False
        if zone_high > 0 and zone_low > 0:
            if "bull" in direction or direction in ("long", "buy"):
                if current_price < zone_low * 0.998:
                    zone_invalidated = True
            else:
                if current_price > zone_high * 1.002:
                    zone_invalidated = True

        if zone_invalidated:
            rec["outcome_status"] = "invalidated"
            rec["outcome_note"]   = f"Zone breached — price {current_price:.4f} invalidated zone"
            updated += 1
        elif favorable_pct >= _LM_WIN_PCT:
            rec["outcome_status"] = "won"
            rec["outcome_note"]   = f"Moved +{favorable_pct:.2f}% in favor (threshold {_LM_WIN_PCT}%)"
            updated += 1
        elif adverse_pct >= _LM_LOSS_PCT:
            rec["outcome_status"] = "lost"
            rec["outcome_note"]   = f"Moved -{adverse_pct:.2f}% against (threshold {_LM_LOSS_PCT}%)"
            updated += 1

    # Save whenever resolved outcomes changed OR pending progress fields updated
    if updated > 0 or progress_changed:
        summary = _lm_build_memory_summary(records)
        snap["setup_memory_records"] = records
        snap["setup_memory_summary"] = summary
        row.snapshot_json = _json_dumps_safe(snap)

    return snap, updated


# ── Phase 8.2: Accuracy Dashboard ────────────────────────────────────────────

def _lm_compute_memory_accuracy(snapshot) -> dict:
    """Compute accuracy stats from setup_memory_records. Stores result in snapshot.

    Only counts closed records (won/lost/invalidated) toward win_rate.
    Returns compact accuracy dict.
    """
    records = list(snapshot.get("setup_memory_records") or [])

    closed      = [r for r in records if r.get("outcome_status") in ("won", "lost", "invalidated")]
    pending     = [r for r in records if r.get("outcome_status") == "pending"]
    won         = [r for r in closed  if r.get("outcome_status") == "won"]
    lost        = [r for r in closed  if r.get("outcome_status") == "lost"]
    inv         = [r for r in closed  if r.get("outcome_status") == "invalidated"]

    total_closed = len(closed)
    win_rate = round(len(won) / total_closed * 100, 1) if total_closed > 0 else None

    def _group_by(field: str, recs: list) -> dict:
        out: Dict[str, Dict] = {}
        for r in recs:
            k = r.get(field) or "unknown"
            if k not in out:
                out[k] = {"total": 0, "won": 0, "lost": 0, "invalidated": 0}
            out[k]["total"] += 1
            st = r.get("outcome_status") or "pending"
            if st in out[k]:
                out[k][st] += 1
        return out

    by_setup    = _group_by("setup_type", closed)
    by_rstate   = _group_by("readiness_state", closed)
    by_source   = _group_by("source", closed)

    # Best and worst patterns by setup_type (min 2 closed records)
    best_patterns  = sorted(
        [{"setup_type": k, **v, "win_rate": round(v["won"] / v["total"] * 100, 1)}
         for k, v in by_setup.items() if v["total"] >= 2],
        key=lambda x: x["win_rate"], reverse=True
    )[:3]
    worst_patterns = sorted(
        [{"setup_type": k, **v, "win_rate": round(v["won"] / v["total"] * 100, 1)}
         for k, v in by_setup.items() if v["total"] >= 2],
        key=lambda x: x["win_rate"]
    )[:3]

    now_iso = datetime.now(timezone.utc).isoformat()
    accuracy = {
        "computed_at":       now_iso,
        "total_closed":      total_closed,
        "total_pending":     len(pending),
        "win_count":         len(won),
        "loss_count":        len(lost),
        "invalidated_count": len(inv),
        "win_rate":          win_rate,
        "by_setup_type":     by_setup,
        "by_readiness_state": by_rstate,
        "by_source":         by_source,
        "best_patterns":     best_patterns,
        "worst_patterns":    worst_patterns,
    }

    snapshot["setup_memory_accuracy"]          = accuracy
    snapshot["last_setup_memory_accuracy_at"]  = now_iso
    return accuracy


# ── Phase 9.2: Aggregated Market Context Engine ───────────────────────────────

def _lm_compute_aggregated_market_context(row, snapshot=None) -> dict:
    """Compute aggregated market context, Binance-first.

    Wraps the existing per-exchange market context in an aggregated envelope.
    MEXC may supply ticker supplement only (funding/OI via public ticker).
    Does NOT fake unavailable fields. Does NOT claim multi-exchange if only
    Binance is available. No trading. No order logic.
    Returns a dict stored as snapshot["latest_aggregated_market_context"].
    """
    snap    = snapshot if snapshot is not None else _json_loads_safe(getattr(row, "snapshot_json", None), {})
    config  = _lm_data_source_config(row, snapshot=snap)
    symbol  = config["execution_symbol"]
    mcs     = config["market_context_sources"]  # e.g. ["binance"] or ["binance","mexc"]
    now_iso = datetime.now(timezone.utc).isoformat()

    sources_requested = list(mcs)
    sources_used: list  = []
    sources_failed: list = []
    warnings: list = []

    # ── Primary source: Binance ───────────────────────────────────────────────
    binance_ctx = None
    if "binance" in sources_requested:
        try:
            binance_ctx = _lm_fetch_market_context(symbol, exchange="binance", market="perpetual")
            if binance_ctx.get("ok"):
                sources_used.append("binance")
            else:
                sources_failed.append("binance")
                warnings.append(f"Binance market context unavailable: {binance_ctx.get('reason','')}")
        except Exception as _e:
            sources_failed.append("binance")
            warnings.append(f"Binance market context error: {_e}")

    # ── Supplement: MEXC ticker (public) ─────────────────────────────────────
    mexc_ticker = None
    if "mexc" in sources_requested:
        try:
            t = _lm_fetch_mexc_perp_ticker(symbol)
            if t.get("ok"):
                mexc_ticker = t
                sources_used.append("mexc")
            else:
                sources_failed.append("mexc")
                warnings.append(f"MEXC ticker unavailable: {t.get('error','')}")
        except Exception as _e:
            sources_failed.append("mexc")
            warnings.append(f"MEXC ticker error: {_e}")

    # ── Agreement / conflict scoring ─────────────────────────────────────────
    only_one_source = len(sources_used) <= 1
    agreement_score = None if only_one_source else None  # placeholder for multi-source
    conflict_score  = None if only_one_source else None
    agreement_note  = "single_source" if only_one_source else None

    # ── Build output from Binance primary ────────────────────────────────────
    b = binance_ctx or {}
    funding       = b.get("funding")       or {"available": False}
    open_interest = b.get("open_interest") or {"available": False}
    taker_pressure = b.get("taker_pressure") or {"available": False}
    long_short    = b.get("long_short")    or {"available": False}
    liquidations  = b.get("liquidations")  or {"available": False}
    volume_ctx    = b.get("activity")      or {"available": False}
    b_summary     = b.get("summary")       or {}
    bias          = b_summary.get("market_bias") or "neutral"
    summary_notes = b_summary.get("notes") or []

    if mexc_ticker and mexc_ticker.get("ok"):
        if mexc_ticker.get("funding_rate") is not None:
            warnings.append(
                f"MEXC funding rate (supplement only): {mexc_ticker['funding_rate']:.6f}. "
                "Binance data is primary source."
            )

    if only_one_source and "binance" in sources_used:
        warnings.append("Single source (Binance only) — agreement_score not applicable.")
    if "binance" in sources_failed:
        warnings.append("Primary source (Binance) failed — context may be incomplete.")

    return {
        "phase":           "phase9_aggregated_context",
        "computed_at":     now_iso,
        "symbol":          symbol,
        "sources_requested": sources_requested,
        "sources_used":    sources_used,
        "sources_failed":  sources_failed,
        "primary_source":  "binance",
        "funding":         funding,
        "open_interest":   open_interest,
        "taker_pressure":  taker_pressure,
        "long_short":      long_short,
        "liquidations":    liquidations,
        "volume_context":  volume_ctx,
        "agreement_score": agreement_score,
        "conflict_score":  conflict_score,
        "agreement_note":  agreement_note,
        "bias":            bias,
        "summary_notes":   summary_notes,
        "warnings":        warnings,
        "summary":         b_summary.get("ai_context") or "Binance-first aggregated context.",
    }


# ── Phase 9.3: LiveMonitorTrade helpers ───────────────────────────────────────

_LM_TRADE_ALLOWED_STATUSES = {
    "draft", "proposed", "risk_approved", "risk_rejected", "cancelled",
    # Phase 9.6+ only — not created by Phase 9.3-9.5:
    "submitted", "open", "closed", "failed",
}
_LM_TRADE_CANCELLABLE_STATUSES = {"draft", "proposed", "risk_rejected"}


def _lm_trade_to_dict(trade) -> dict:
    """Serialize a LiveMonitorTrade row to a JSON-friendly dict."""
    return {
        "id":                      trade.id,
        "trade_uid":               trade.trade_uid,
        "user_id":                 trade.user_id,
        "live_monitor_item_id":    trade.live_monitor_item_id,
        "linked_memory_record_id": trade.linked_memory_record_id,
        "mode":                    trade.mode,
        "execution_exchange":      trade.execution_exchange,
        "execution_market":        trade.execution_market,
        "symbol":                  trade.symbol,
        "direction":               trade.direction,
        "setup_type":              trade.setup_type,
        "timeframe":               trade.timeframe,
        "status":                  trade.status,
        "entry_price":             trade.entry_price,
        "stop_loss":               trade.stop_loss,
        "take_profit":             trade.take_profit,
        "risk_reward":             trade.risk_reward,
        "position_size":           trade.position_size,
        "leverage":                trade.leverage,
        "ai_proposal":             _json_loads_safe(trade.ai_proposal_json, None),
        "ai_reasoning_summary":    trade.ai_reasoning_summary,
        "setup_context":           _json_loads_safe(trade.setup_context_json, None),
        "risk_guard":              _json_loads_safe(trade.risk_guard_json, None),
        "risk_guard_status":       trade.risk_guard_status,
        "rejection_reason":        trade.rejection_reason,
        "exchange_order_id":       trade.exchange_order_id,
        "exchange_position_id":    trade.exchange_position_id,
        "opened_at":               trade.opened_at.isoformat() if trade.opened_at else None,
        "closed_at":               trade.closed_at.isoformat() if trade.closed_at else None,
        "pnl":                     trade.pnl,
        "fees":                    trade.fees,
        "outcome":                 trade.outcome,
        "created_at":              trade.created_at.isoformat() if trade.created_at else None,
        "updated_at":              trade.updated_at.isoformat() if trade.updated_at else None,
    }


def _lm_build_trade_setup_context(row, snapshot=None) -> dict:
    """Build compact setup context stored in LiveMonitorTrade.setup_context_json.

    No raw candles. No API keys. No order fields.
    """
    snap = snapshot if snapshot is not None else _json_loads_safe(getattr(row, "snapshot_json", None), {})

    ds  = snap.get("data_sources") or {}
    sr  = snap.get("latest_setup_readiness") or {}
    evd = snap.get("latest_event_detection") or {}
    cf  = snap.get("latest_candle_features") or {}
    amc = snap.get("latest_aggregated_market_context") or {}
    lh  = snap.get("latest_health") or {}

    # Compact candle features — no raw candles
    cf_compact = None
    feat = cf.get("features") or {}
    if feat:
        cf_compact = {
            "interval":          cf.get("interval"),
            "source_exchange":   cf.get("source_exchange"),
            "computed_at":       cf.get("computed_at"),
            "atr_14":            feat.get("atr_14"),
            "strong_rejection":  feat.get("strong_rejection"),
            "compression":       feat.get("compression"),
            "breakout_context":  feat.get("breakout_context"),
            "trend_slope_20":    feat.get("trend_slope_20"),
            "volume_spike_ratio": feat.get("volume_spike_ratio"),
        }

    # Compact event detection — max 8 events, no raw candles
    evd_compact = None
    if evd:
        evd_compact = {
            "computed_at": evd.get("computed_at"),
            "summary":     evd.get("summary"),
            "events": [
                {"type": e.get("type"), "severity": e.get("severity"),
                 "label": e.get("label"), "direction": e.get("direction")}
                for e in (evd.get("events") or [])[:8]
            ],
        }

    # Compact aggregated context — no raw data
    amc_compact = None
    if amc:
        amc_compact = {
            "computed_at":     amc.get("computed_at"),
            "sources_used":    amc.get("sources_used"),
            "bias":            amc.get("bias"),
            "funding":         (amc.get("funding") or {}).get("bias"),
            "taker_pressure":  (amc.get("taker_pressure") or {}).get("bias"),
            "agreement_note":  amc.get("agreement_note"),
        }

    # Compact health
    lh_compact = None
    if lh:
        lh_compact = {
            "health_score": lh.get("health_score"),
            "grade":        lh.get("grade"),
            "direction":    lh.get("direction"),
            "bias":         lh.get("bias"),
        }

    # Latest memory record id/status (not full record)
    mem_records = snap.get("setup_memory_records") or []
    latest_mem  = None
    if mem_records:
        lmr = mem_records[-1]
        latest_mem = {
            "record_id":      lmr.get("id") or lmr.get("record_id"),
            "outcome_status": lmr.get("outcome_status"),
            "readiness_state": lmr.get("readiness_state"),
        }

    return {
        "captured_at":      datetime.now(timezone.utc).isoformat(),
        "symbol":           (getattr(row, "symbol", None) or "").upper(),
        "direction":        getattr(row, "direction", None),
        "setup_type":       getattr(row, "setup_type", None),
        "timeframe":        getattr(row, "timeframe", None),
        "zone_high":        getattr(row, "zone_high", None),
        "zone_low":         getattr(row, "zone_low", None),
        "current_price":    getattr(row, "current_price", None),
        "data_sources":     {k: v for k, v in ds.items() if k != "warnings"},
        "setup_readiness":  {
            "readiness_state":    sr.get("readiness_state"),
            "confirmation_score": sr.get("confirmation_score"),
            "risk_score":         sr.get("risk_score"),
            "supporting_events":  (sr.get("supporting_events") or [])[:8],
            "risk_events":        (sr.get("risk_events") or [])[:8],
        } if sr else None,
        "event_detection":  evd_compact,
        "candle_features":  cf_compact,
        "aggregated_market_context": amc_compact,
        "health":           lh_compact,
        "latest_memory":    latest_mem,
    }


def _lm_create_trade_record_from_proposal(row, proposal: dict, snapshot=None):
    """Build and return (unsaved) LiveMonitorTrade from AI proposal dict.

    Caller must db.session.add() and db.session.commit().
    No order execution. No API keys. No raw candles.
    Returns LiveMonitorTrade instance.
    """
    import uuid
    from models import LiveMonitorTrade as _LMT

    snap    = snapshot if snapshot is not None else _json_loads_safe(getattr(row, "snapshot_json", None), {})
    config  = _lm_data_source_config(row, snapshot=snap)
    action  = (proposal.get("action") or "no_trade").lower()
    symbol  = config["execution_symbol"] or (getattr(row, "symbol", None) or "").upper()

    direction = None
    if action == "propose_long":
        direction = "long"
    elif action == "propose_short":
        direction = "short"
    else:
        direction = (getattr(row, "direction", None) or "").lower() or None

    status = "proposed" if action in ("propose_long", "propose_short") else "draft"

    setup_ctx = _lm_build_trade_setup_context(row, snapshot=snap)

    # Link to latest memory record if available
    mem_records = snap.get("setup_memory_records") or []
    linked_mem_id = (mem_records[-1].get("id") or mem_records[-1].get("record_id")) if mem_records else None

    trade = _LMT(
        trade_uid              = uuid.uuid4().hex[:20],
        user_id                = row.user_id,
        live_monitor_item_id   = row.id,
        linked_memory_record_id = linked_mem_id,
        mode                   = "proposal_only",
        execution_exchange     = config.get("execution_exchange", "none"),
        execution_market       = config.get("execution_market", "perpetual"),
        symbol                 = symbol,
        direction              = direction,
        setup_type             = getattr(row, "setup_type", None),
        timeframe              = getattr(row, "timeframe", None),
        status                 = status,
        ai_proposal_json       = _json_dumps_safe(proposal),
        ai_reasoning_summary   = (proposal.get("reasoning_summary") or "")[:500],
        setup_context_json     = _json_dumps_safe(setup_ctx),
        risk_guard_status      = "not_checked",
    )
    return trade


# ── Phase 9.4: AI Trade Proposal helpers ─────────────────────────────────────

def _lm_ai_trade_proposal_prompt() -> str:
    """System prompt for the AI trade proposal endpoint.

    AI may only propose — never execute, never place orders.
    Returns raw JSON only. No prose outside JSON.
    """
    return (
        "You are the ZyNi Trade Proposal AI — a professional SMC setup analyst.\n"
        "Your job is to evaluate the provided setup context and produce a structured "
        "trade PROPOSAL only. You cannot execute trades, place orders, or access APIs.\n\n"

        "OUTPUT FORMAT — respond with ONLY this JSON object:\n"
        "{\n"
        "  \"action\": \"no_trade\" | \"wait\" | \"propose_long\" | \"propose_short\",\n"
        "  \"setup_type\": \"<OB|FIB|FVG|Breaker|...>\",\n"
        "  \"direction\": \"long\" | \"short\" | null,\n"
        "  \"confidence\": <integer 0-100>,\n"
        "  \"entry_logic\": \"<brief entry rationale>\",\n"
        "  \"invalidation_logic\": \"<what invalidates this proposal>\",\n"
        "  \"take_profit_logic\": \"<brief TP rationale>\",\n"
        "  \"required_confirmations\": [\"<list of still-needed confirmations>\"],\n"
        "  \"risk_notes\": [\"<list of current risk factors>\"],\n"
        "  \"reasoning_summary\": \"<1-2 sentence summary>\"\n"
        "}\n\n"

        "STRICT RULES:\n"
        "- This is a PROPOSAL only. Risk Guard must approve before any execution.\n"
        "- NEVER say 'place order', 'enter now', 'execute', or imply any trade is placed.\n"
        "- NEVER request or use API keys, credentials, or account data.\n"
        "- NEVER invent data that is not present in the provided context.\n"
        "- If action is 'no_trade' or 'wait', still fill the JSON — use null/empty where not applicable.\n\n"

        "READINESS RULES:\n"
        "- strong_watch: may propose_long or propose_short.\n"
        "- active_watch: must use 'wait' — proposal is premature.\n"
        "- high_risk or avoid: must use 'no_trade'. Never propose in these states.\n"
        "- confirmation_score < 65: prefer 'wait'. Must explain in required_confirmations.\n"
        "- risk_score > 35: reduce confidence. If > 60, use 'no_trade'.\n\n"

        "MODULE RULES:\n"
        "- OB and FIB zones are PRIMARY setup modules — they can be the basis of a proposal.\n"
        "- FVG, Breaker, Bias, Session, OI, Funding, Taker, Liquidations, Wick = "
        "CONFIRMATIONS only. They cannot be the sole basis of a proposal.\n"
        "- aggregated_market_context is confirmation only — Binance context is NOT "
        "MEXC execution price or level.\n"
        "- execution_exchange candle/price data is the truth for entry/SL/TP levels.\n\n"

        "RISK GUARD NOTE:\n"
        "- Your proposal is NOT approval. Risk Guard will independently validate.\n"
        "- Do NOT claim your proposal is approved, safe, or executable.\n"
        "- Do not output a disclaimer field — the rules above cover it.\n\n"

        "Do not wrap the JSON in markdown code fences. Output raw JSON only."
    )


def _lm_build_trade_proposal_context(row, snapshot=None) -> dict:
    """Build compact context dict for AI trade proposal. No raw candles. No API keys."""
    snap    = snapshot if snapshot is not None else _json_loads_safe(getattr(row, "snapshot_json", None), {})
    config  = _lm_data_source_config(row, snapshot=snap)
    sr      = snap.get("latest_setup_readiness") or {}
    evd     = snap.get("latest_event_detection") or {}
    cf      = snap.get("latest_candle_features") or {}
    amc     = snap.get("latest_aggregated_market_context") or {}
    lh      = snap.get("latest_health") or {}

    live_price = _lm_get_live_price_for_item(row, snapshot=snap)

    # Compact candle features — no raw data
    feat = cf.get("features") or {}
    cf_compact = {
        "interval":          cf.get("interval"),
        "source_exchange":   cf.get("source_exchange"),
        "atr_14":            feat.get("atr_14"),
        "strong_rejection":  feat.get("strong_rejection"),
        "compression":       feat.get("compression"),
        "breakout_context":  feat.get("breakout_context"),
        "trend_slope_20":    feat.get("trend_slope_20"),
        "volume_spike_ratio": feat.get("volume_spike_ratio"),
        "price_position_50": feat.get("price_position_50"),
        "last_close":        feat.get("last_close"),
    } if feat else None

    # Compact aggregated context
    amc_compact = {
        "sources_used":    amc.get("sources_used"),
        "bias":            amc.get("bias"),
        "funding":         amc.get("funding"),
        "open_interest":   amc.get("open_interest"),
        "taker_pressure":  amc.get("taker_pressure"),
        "long_short":      amc.get("long_short"),
        "agreement_note":  amc.get("agreement_note"),
        "warnings":        (amc.get("warnings") or [])[:5],
        "summary":         amc.get("summary"),
    } if amc else None

    # Latest 3 memory records — compact, no raw candles
    mem_records = snap.get("setup_memory_records") or []
    mem_compact = [
        {
            "created_at":         r.get("created_at"),
            "readiness_state":    r.get("readiness_state"),
            "direction":          r.get("direction"),
            "confirmation_score": r.get("confirmation_score"),
            "risk_score":         r.get("risk_score"),
            "outcome_status":     r.get("outcome_status"),
        }
        for r in mem_records[-3:]
    ] if mem_records else []

    custom_instr = None
    try:
        custom_instr = _lm_custom_ai_instructions_from_snapshot(snap)
    except Exception:
        pass

    return {
        "setup": {
            "symbol":       (getattr(row, "symbol", None) or "").upper(),
            "direction":    getattr(row, "direction", None),
            "setup_type":   getattr(row, "setup_type", None),
            "timeframe":    getattr(row, "timeframe", None),
            "zone_high":    getattr(row, "zone_high", None),
            "zone_low":     getattr(row, "zone_low", None),
            "status":       getattr(row, "status", None),
            "score":        getattr(row, "score", None),
            "confidence":   getattr(row, "confidence", None),
        },
        "data_sources": {k: v for k, v in config.items() if k != "warnings"},
        "live_price":   live_price,
        "setup_readiness": {
            "readiness_state":    sr.get("readiness_state"),
            "confirmation_score": sr.get("confirmation_score"),
            "risk_score":         sr.get("risk_score"),
            "supporting_events":  (sr.get("supporting_events") or [])[:8],
            "risk_events":        (sr.get("risk_events") or [])[:8],
            "missing_confirmations": sr.get("missing_confirmations"),
            "summary":            sr.get("summary"),
        } if sr else None,
        "event_detection": {
            "computed_at": evd.get("computed_at"),
            "summary":     evd.get("summary"),
            "events": [
                {"type": e.get("type"), "severity": e.get("severity"),
                 "label": e.get("label"), "direction": e.get("direction"),
                 "details": e.get("details")}
                for e in (evd.get("events") or [])[:8]
            ],
        } if evd else None,
        "candle_features":           cf_compact,
        "aggregated_market_context": amc_compact,
        "health": {
            "health_score": lh.get("health_score"),
            "grade":        lh.get("grade"),
            "direction":    lh.get("direction"),
            "bias":         lh.get("bias"),
            "warnings":     (lh.get("warnings") or [])[:5],
        } if lh else None,
        "reasoning_memory_latest": mem_compact,
        "custom_ai_instructions":  custom_instr,
        "rules": {
            "proposal_only":        True,
            "no_execution":         True,
            "no_order_placement":   True,
            "no_api_keys":          True,
            "risk_guard_required_for_approval": True,
            "execution_exchange_price_is_truth": True,
            "binance_context_is_confirmation_only": True,
        },
    }


# ── Phase 9.5: Risk Guard ─────────────────────────────────────────────────────

_LM_RG_MAX_ACTIVE_TRADES     = 3   # max proposed/risk_approved per user
_LM_RG_MAX_DAILY_PROPOSALS   = 20  # max AI proposals per user per day
_LM_RG_MIN_CONFIRMATION      = 65
_LM_RG_MAX_RISK_SCORE        = 35
_LM_RG_RULES_VERSION         = "phase9_5_v1"
_LM_RG_HARD_BLOCK_STATES     = {"high_risk", "avoid"}
_LM_RG_EXECUTABLE_STATES     = {"strong_watch"}
_LM_RG_PREVIEW_ONLY_STATES   = {"active_watch", "wait"}


def _lm_run_risk_guard(trade, row=None, snapshot=None) -> dict:
    """Run Risk Guard hard-safety checks against a LiveMonitorTrade proposal.

    No execution. No exchange call. No API keys.
    Returns {status, approved, score, reasons, warnings, hard_blocks, checked_at, rules_version}.
    """
    from models import LiveMonitorTrade as _LMT
    from datetime import datetime as _dt_rg, timezone as _tz_rg

    checked_at  = _dt_rg.now(_tz_rg.utc).isoformat()
    hard_blocks = []
    reasons     = []
    warnings    = []
    score       = 100  # start perfect, deduct per issue

    # ── Load setup context from trade ────────────────────────────────────────
    ctx  = _json_loads_safe(trade.setup_context_json, {})
    sr   = ctx.get("setup_readiness") or {}
    ds   = ctx.get("data_sources") or {}
    proposal = _json_loads_safe(trade.ai_proposal_json, {})

    readiness_state    = (sr.get("readiness_state") or "wait").lower()
    confirmation_score = int(sr.get("confirmation_score") or 0)
    risk_score_val     = int(sr.get("risk_score") or 0)
    action             = (proposal.get("action") or "no_trade").lower()
    direction          = (trade.direction or "").lower()
    symbol             = (trade.symbol or "").upper()
    candle_src         = (ds.get("candle_source") or "binance").lower()
    live_price_src     = (ds.get("live_price_source") or "binance").lower()
    exec_exchange      = (ds.get("execution_exchange") or trade.execution_exchange or "none").lower()

    # ── HARD BLOCK 1: forbidden readiness states ──────────────────────────────
    if readiness_state in _LM_RG_HARD_BLOCK_STATES:
        hard_blocks.append(f"readiness_state={readiness_state} — proposal blocked (high_risk/avoid)")
        score -= 60

    # ── HARD BLOCK 2: active_watch is preview-only ────────────────────────────
    if readiness_state in _LM_RG_PREVIEW_ONLY_STATES and action in ("propose_long", "propose_short"):
        hard_blocks.append(
            f"readiness_state={readiness_state} — only strong_watch allows executable approval"
        )
        score -= 40

    # ── HARD BLOCK 3: confirmation score too low ──────────────────────────────
    if confirmation_score < _LM_RG_MIN_CONFIRMATION:
        hard_blocks.append(
            f"confirmation_score={confirmation_score} < {_LM_RG_MIN_CONFIRMATION} required"
        )
        score -= 20

    # ── HARD BLOCK 4: risk score too high ─────────────────────────────────────
    if risk_score_val > _LM_RG_MAX_RISK_SCORE:
        hard_blocks.append(
            f"risk_score={risk_score_val} > {_LM_RG_MAX_RISK_SCORE} allowed"
        )
        score -= 20

    # ── HARD BLOCK 5: missing required context ────────────────────────────────
    if not ds:
        hard_blocks.append("data_sources missing — cannot verify execution source")
        score -= 15
    if not sr:
        hard_blocks.append("setup_readiness missing — cannot evaluate readiness")
        score -= 15
    evd_ctx = ctx.get("event_detection")
    if not evd_ctx:
        hard_blocks.append("event_detection missing — cannot evaluate events")
        score -= 10
    cf_ctx = ctx.get("candle_features")
    if not cf_ctx:
        reasons.append("candle_features missing — entry levels cannot be verified")
        score -= 10

    # ── HARD BLOCK 6: MEXC execution source consistency ───────────────────────
    if exec_exchange == "mexc":
        if candle_src != "mexc":
            hard_blocks.append(
                "execution_exchange=mexc but candle_source is not mexc — "
                "candles must match execution exchange"
            )
            score -= 20
        if live_price_src != "mexc":
            hard_blocks.append(
                "execution_exchange=mexc but live_price_source is not mexc — "
                "live price must match execution exchange"
            )
            score -= 20

    # ── HARD BLOCK 7: only propose_long/propose_short are executable proposals ──
    if action not in ("propose_long", "propose_short"):
        hard_blocks.append(
            f"AI action='{action}' is not an executable proposal — "
            "only propose_long/propose_short can be risk_approved"
        )
        score -= 50

    # ── Soft checks: duplicate symbol ────────────────────────────────────────
    try:
        existing_active = _LMT.query.filter(
            _LMT.user_id == trade.user_id,
            _LMT.symbol  == symbol,
            _LMT.status.in_(["proposed", "risk_approved"]),
            _LMT.id      != trade.id,
        ).count()
        if existing_active > 0:
            hard_blocks.append(
                f"duplicate active proposal/trade for {symbol} already exists"
            )
            score -= 30
    except Exception:
        warnings.append("Could not check for duplicate symbol proposals")

    # ── Soft checks: max active trades per user ───────────────────────────────
    try:
        active_count = _LMT.query.filter(
            _LMT.user_id == trade.user_id,
            _LMT.status.in_(["proposed", "risk_approved"]),
            _LMT.id      != trade.id,
        ).count()
        if active_count >= _LM_RG_MAX_ACTIVE_TRADES:
            hard_blocks.append(
                f"max {_LM_RG_MAX_ACTIVE_TRADES} active proposals/approved trades reached "
                f"({active_count} existing)"
            )
            score -= 30
    except Exception:
        warnings.append("Could not check active trade count")

    # ── Soft checks: max daily proposals ─────────────────────────────────────
    try:
        from datetime import datetime as _dt_rg2, timezone as _tz_rg2, timedelta as _td_rg
        day_start = _dt_rg2.now(_tz_rg2.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        daily_count = _LMT.query.filter(
            _LMT.user_id    == trade.user_id,
            _LMT.created_at >= day_start,
        ).count()
        if daily_count > _LM_RG_MAX_DAILY_PROPOSALS:
            hard_blocks.append(
                f"daily AI proposal limit {_LM_RG_MAX_DAILY_PROPOSALS} reached ({daily_count} today)"
            )
            score -= 25
    except Exception:
        warnings.append("Could not check daily proposal count")

    # ── Funding / taker / session warnings ───────────────────────────────────
    amc_ctx = ctx.get("aggregated_market_context") or {}
    funding_bias  = amc_ctx.get("funding") or ""
    taker_bias    = amc_ctx.get("taker_pressure") or ""

    if direction == "long" and isinstance(funding_bias, str) and "bearish" in funding_bias.lower():
        warnings.append("Funding rate bias opposes long direction — caution")
        score -= 5
    if direction == "short" and isinstance(funding_bias, str) and "bullish" in funding_bias.lower():
        warnings.append("Funding rate bias opposes short direction — caution")
        score -= 5
    if direction == "long" and isinstance(taker_bias, str) and "bearish" in taker_bias.lower():
        warnings.append("Taker pressure opposes long direction — caution")
        score -= 5
    if direction == "short" and isinstance(taker_bias, str) and "bullish" in taker_bias.lower():
        warnings.append("Taker pressure opposes short direction — caution")
        score -= 5

    # ── Proposal quality checks ───────────────────────────────────────────────
    if not proposal.get("invalidation_logic"):
        reasons.append("AI proposal missing invalidation_logic — required for Risk Guard")
        score -= 10
    if not proposal.get("take_profit_logic"):
        reasons.append("AI proposal missing take_profit_logic — required for Risk Guard")
        score -= 5

    score = max(0, score)

    # ── Final verdict ─────────────────────────────────────────────────────────
    approved = len(hard_blocks) == 0 and score >= 60

    return {
        "status":       "approved" if approved else "rejected",
        "approved":     approved,
        "score":        score,
        "reasons":      reasons,
        "warnings":     warnings,
        "hard_blocks":  hard_blocks,
        "checked_at":   checked_at,
        "rules_version": _LM_RG_RULES_VERSION,
    }


# ── Phase 9.6: MEXC Demo Execution Connector ──────────────────────────────────

_LM_DEMO_ORDER_SIZE_USDT_ENV = "DEMO_ORDER_SIZE_USDT"
_LM_DEMO_ORDER_SIZE_DEFAULT  = 5.0   # USDT notional
_LM_DEMO_MAX_LEVERAGE        = 5


def _lm_demo_trading_enabled() -> dict:
    """Return demo-trading gate status from environment. Never returns secrets.

    All execution endpoints must check this first and return blocked if not fully enabled.
    Required: DEMO_TRADING_ENABLED=true, MEXC_DEMO_MODE=true, MEXC_API_KEY, MEXC_API_SECRET.
    """
    import os as _os
    demo_flag      = _os.environ.get("DEMO_TRADING_ENABLED",  "false").strip().lower()
    mexc_demo_flag = _os.environ.get("MEXC_DEMO_MODE",        "false").strip().lower()
    key_present    = bool(_os.environ.get("MEXC_API_KEY",    "").strip())
    secret_present = bool(_os.environ.get("MEXC_API_SECRET", "").strip())
    keys_present   = key_present and secret_present

    enabled   = demo_flag      == "true"
    demo_mode = mexc_demo_flag == "true"

    blocked_reason = None
    if not enabled:
        blocked_reason = "DEMO_TRADING_ENABLED is not true"
    elif not demo_mode:
        blocked_reason = "MEXC_DEMO_MODE is not true"
    elif not keys_present:
        blocked_reason = "MEXC API keys not configured"

    return {
        "enabled":           enabled,
        "demo_mode":         demo_mode,
        "mexc_keys_present": keys_present,
        "blocked":           blocked_reason is not None,
        "blocked_reason":    blocked_reason,
        "warnings":          [],
    }


def _lm_mexc_private_request(method: str, path: str,
                              params: dict = None, body: dict = None) -> dict:
    """Sign and send a private MEXC Contract API v1 request.

    Base: MEXC_CONTRACT_PRIV (https://contract.mexc.com/api/v1/private)
    Keys read from env only — never logged or returned. 15s timeout.

    Auth per https://mexcdevelop.github.io/apidocs/contract_v1_en/:
      Headers: ApiKey, Request-Time, Recv-Window, Signature, Content-Type
      Signature = HMAC-SHA256(api_key + timestamp_ms + sorted_query_string)

    Returns {ok, status_code, data, error, _debug}.
    _debug contains masked diagnostics only — no keys or secrets.
    """
    import os as _os, time as _time
    api_key    = _os.environ.get("MEXC_API_KEY",    "").strip()
    api_secret = _os.environ.get("MEXC_API_SECRET", "").strip()
    if not api_key or not api_secret:
        return {"ok": False, "error": "mexc_keys_missing", "data": None, "status_code": None,
                "_debug": {"auth_style": "contract_v1_header", "keys_present": False}}

    ts_ms   = str(int(_time.time() * 1000))
    recv_win = "5000"

    # Sorted query string from caller params only
    qs = "&".join(f"{k}={v}" for k, v in sorted((params or {}).items()))

    # Signature: HMAC-SHA256(api_key + timestamp_ms + sorted_query_string)
    sign_str = api_key + ts_ms + qs
    sig = hmac.new(api_secret.encode(), sign_str.encode(), hashlib.sha256).hexdigest()

    headers = {
        "ApiKey":       api_key,
        "Request-Time": ts_ms,
        "Recv-Window":  recv_win,
        "Signature":    sig,
        "Content-Type": "application/json",
    }

    url = MEXC_CONTRACT_PRIV.rstrip("/") + "/" + path.lstrip("/")

    _debug = {
        "auth_style":          "contract_v1_header",
        "base_url":            MEXC_CONTRACT_PRIV,
        "endpoint_path":       path,
        "full_url":            url,
        "request_time_ms":     ts_ms,
        "recv_window":         recv_win,
        "signature_present":   True,
        "api_key_tail":        api_key[-4:] if len(api_key) >= 4 else "****",
        "query_string_length": len(qs),
    }

    try:
        if method.upper() == "GET":
            resp = req.get(url,  params=params or {}, headers=headers, timeout=15)
        elif method.upper() == "POST":
            resp = req.post(url, params=params or {}, json=body or {}, headers=headers, timeout=15)
        else:
            return {"ok": False, "error": f"unsupported_method:{method}",
                    "data": None, "status_code": None, "_debug": _debug}

        _debug["status_code"] = resp.status_code
        _debug["response_content_type"] = resp.headers.get("Content-Type", "")

        is_html = "text/html" in _debug["response_content_type"].lower()
        _debug["response_type"] = "html" if is_html else (
            "json" if "application/json" in _debug["response_content_type"].lower() else "text"
        )

        try:
            data = resp.json()
            if is_html:
                data = {"html_response": True, "preview": resp.text[:120]}
        except Exception:
            data = {"raw": resp.text[:300]}

        ok = resp.status_code < 400
        err = None
        if not ok:
            if is_html:
                err = f"html_response_http{resp.status_code}_likely_auth_or_path_error"
            else:
                err = (data.get("msg") or data.get("message") or resp.text[:160])

        return {
            "ok":          ok,
            "status_code": resp.status_code,
            "data":        data,
            "error":       err,
            "_debug":      _debug,
        }
    except Exception as exc:
        _debug["exception"] = type(exc).__name__
        return {"ok": False, "error": str(exc)[:200], "data": None,
                "status_code": None, "_debug": _debug}


def _lm_mexc_demo_health_check() -> dict:
    """Read-only MEXC Contract account connectivity check. Never submits or cancels orders.

    Calls GET /api/v1/private/account/assets (read-only, no order endpoint).
    Returns {ok, account_reachable, private_account_assets_ok, balance_usdt,
             status_code, error_summary, warnings, _debug}.
    """
    gate = _lm_demo_trading_enabled()
    if gate["blocked"]:
        return {
            "ok":                        False,
            "blocked":                   True,
            "blocked_reason":            gate["blocked_reason"],
            "account_reachable":         False,
            "private_account_assets_ok": False,
        }

    # Correct private path: /api/v1/private/account/assets
    result = _lm_mexc_private_request("GET", "account/assets")
    dbg    = result.get("_debug", {})

    if not result["ok"]:
        raw_err = (result.get("error") or "")[:160]
        # Classify HTML 403/401 as auth/path error, not IP block
        if dbg.get("response_type") == "html" or (result.get("status_code") in (401, 403)):
            err_summary = f"private_auth_or_path_error (HTTP {result.get('status_code')}): {raw_err}"
        else:
            err_summary = raw_err
        return {
            "ok":                        False,
            "account_reachable":         False,
            "private_account_assets_ok": False,
            "status_code":               result.get("status_code"),
            "error_summary":             err_summary,
            "warnings":                  gate.get("warnings", []),
            "_debug":                    dbg,
        }

    data     = result.get("data") or {}
    assets   = data.get("data") or []
    usdt_bal = None
    if isinstance(assets, list):
        for a in assets:
            if isinstance(a, dict) and a.get("currency", "").upper() == "USDT":
                usdt_bal = (
                    a.get("availableBalance") or a.get("available") or a.get("balance")
                )
                break

    return {
        "ok":                        True,
        "account_reachable":         True,
        "private_account_assets_ok": True,
        "balance_usdt":              usdt_bal,
        "status_code":               result.get("status_code"),
        "error_summary":             None,
        "warnings":                  gate.get("warnings", []),
        "_debug":                    dbg,
    }


def _lm_prepare_demo_order_payload(trade, row=None, snapshot=None) -> dict:
    """Build MEXC perpetual demo order payload from a risk_approved trade.

    Notional capped 1–50 USDT (DEMO_ORDER_SIZE_USDT env, default 5).
    Leverage capped at _LM_DEMO_MAX_LEVERAGE (5).
    Returns {ok, payload, blocked, blocked_reason, notional_usdt, leverage, mx_symbol}.
    """
    import os as _os

    if trade.status != "risk_approved":
        return {
            "ok":             False,
            "blocked":        True,
            "blocked_reason": f"trade_status_not_risk_approved:{trade.status}",
        }

    ex_ex  = (getattr(trade, "execution_exchange", "") or "").lower()
    ex_mkt = (getattr(trade, "execution_market",   "") or "").lower()
    if ex_ex != "mexc" or ex_mkt != "perpetual":
        return {
            "ok":             False,
            "blocked":        True,
            "blocked_reason": f"execution_exchange_not_mexc_perpetual:{ex_ex}/{ex_mkt}",
        }

    # Action from ai_proposal_json (no dedicated column); fall back to direction
    proposal = _json_loads_safe(getattr(trade, "ai_proposal_json", None), {})
    action   = (proposal.get("action") or "").lower()
    if action not in ("propose_long", "propose_short"):
        dir_col = (getattr(trade, "direction", "") or "").lower()
        if dir_col == "long":
            action = "propose_long"
        elif dir_col == "short":
            action = "propose_short"

    if action == "propose_long":
        order_side = 1   # MEXC: 1=open long, 3=open short
    elif action == "propose_short":
        order_side = 3
    else:
        return {"ok": False, "blocked": True, "blocked_reason": f"unknown_action:{action}"}

    try:
        notional = float(_os.environ.get(_LM_DEMO_ORDER_SIZE_USDT_ENV,
                                         str(_LM_DEMO_ORDER_SIZE_DEFAULT)))
    except (ValueError, TypeError):
        notional = _LM_DEMO_ORDER_SIZE_DEFAULT
    notional = max(1.0, min(notional, 50.0))

    leverage = 1
    prop_lev = proposal.get("leverage") or proposal.get("suggested_leverage")
    if prop_lev:
        try:
            leverage = min(int(prop_lev), _LM_DEMO_MAX_LEVERAGE)
        except (ValueError, TypeError):
            leverage = 1
    leverage = max(1, min(leverage, _LM_DEMO_MAX_LEVERAGE))

    raw_sym = (getattr(trade, "symbol", "") or "").upper()
    raw_sym = raw_sym.replace("USDT", "").replace("_", "").replace("-", "").strip()
    if not raw_sym:
        return {"ok": False, "blocked": True, "blocked_reason": "missing_symbol"}
    mx_sym = f"{raw_sym}_USDT"

    entry_price = None
    if row is not None:
        entry_price = getattr(row, "current_price", None)
    if not entry_price:
        t_r = _lm_fetch_mexc_perp_ticker(mx_sym)
        if t_r.get("ok"):
            entry_price = t_r.get("last_price")

    return {
        "ok":             True,
        "blocked":        False,
        "blocked_reason": None,
        "payload": {
            "symbol":   mx_sym,
            "side":     order_side,
            "openType": 1,        # 1=isolated margin
            "type":     5,        # 5=market order
            "vol":      notional,
            "leverage": leverage,
            "price":    entry_price,
        },
        "notional_usdt": notional,
        "leverage":      leverage,
        "mx_symbol":     mx_sym,
        "order_side":    order_side,
    }


# ── Phase 9.7: Demo Trade Sync + Post-Trade Review Foundation ─────────────────


def _lm_sync_mexc_demo_trade(trade) -> dict:
    """Sync demo trade status from MEXC. Read-only — never places or cancels orders.

    Requires trade.mode == 'demo_exchange' and trade.status in ('submitted', 'open').
    Updates trade.status, opened_at, closed_at, pnl, fees, outcome in-memory.
    Caller must db.session.commit(). Returns {ok, synced, status, changes, error}.
    """
    # Mode gate: only demo_exchange trades may be synced
    if (getattr(trade, "mode", "") or "") != "demo_exchange":
        return {
            "ok":             False,
            "blocked":        True,
            "blocked_reason": f"trade_mode_not_demo_exchange:{getattr(trade, 'mode', None)}",
            "synced":         False,
        }

    gate = _lm_demo_trading_enabled()
    if gate["blocked"]:
        return {"ok": False, "blocked": True, "blocked_reason": gate["blocked_reason"],
                "synced": False}

    order_id = getattr(trade, "exchange_order_id", None)
    if not order_id:
        return {"ok": False, "error": "no_exchange_order_id", "synced": False}

    if trade.status not in ("submitted", "open"):
        return {
            "ok":     True,
            "synced": False,
            "status": trade.status,
            "note":   f"trade_status_{trade.status}_not_syncable",
        }

    result = _lm_mexc_private_request(
        "GET", "order/external/query_order_by_order_id",
        params={"orderId": str(order_id)},
    )
    if not result["ok"]:
        return {
            "ok":          False,
            "synced":      False,
            "error":       result.get("error"),
            "status_code": result.get("status_code"),
        }

    data      = result.get("data") or {}
    order     = data.get("data") or data
    changes   = {}
    now_utc   = datetime.now(timezone.utc)

    # MEXC order state: 3=partially_filled, 4=filled, 5=cancelled, 6=invalid
    mexc_state = order.get("state") or order.get("status")
    if mexc_state in (4, "4", "FILLED", "filled"):
        if trade.status != "open":
            trade.status    = "open"
            trade.opened_at = trade.opened_at or now_utc
            changes["status"] = "open"
    elif mexc_state in (5, "5", 6, "6", "CANCELED", "CANCELLED", "cancelled", "INVALID"):
        if trade.status not in ("closed", "cancelled", "failed"):
            trade.status  = "failed"
            changes["status"] = "failed"

    # Check for closed position when order is open
    pos_id = getattr(trade, "exchange_position_id", None)
    if pos_id and trade.status == "open":
        pos_r = _lm_mexc_private_request(
            "GET", "position/list/history_positions",
            params={"positionId": str(pos_id)},
        )
        if pos_r.get("ok"):
            pos_data  = pos_r.get("data") or {}
            positions = pos_data.get("data") or pos_data.get("resultList") or []
            for p in (positions if isinstance(positions, list) else [positions]):
                if not isinstance(p, dict):
                    continue
                if str(p.get("positionId", "")) == str(pos_id):
                    if p.get("closeType") or p.get("closeOrderId"):
                        trade.status    = "closed"
                        trade.closed_at = trade.closed_at or now_utc
                        raw_pnl = (
                            p.get("realised") or p.get("realisedPnl") or p.get("profit")
                        )
                        if raw_pnl is not None:
                            try:
                                trade.pnl = float(raw_pnl)
                            except (ValueError, TypeError):
                                pass
                        changes["status"] = "closed"
                    break

    if changes:
        trade.updated_at = now_utc

    return {
        "ok":      True,
        "synced":  bool(changes),
        "status":  trade.status,
        "changes": changes,
        "error":   None,
    }


def _lm_build_post_trade_review(trade) -> dict:
    """Deterministic post-trade review. No AI, no exchange calls.

    Returns compact review dict for storage in post_trade_review_json.
    """
    now_utc  = datetime.now(timezone.utc)
    proposal = _json_loads_safe(getattr(trade, "ai_proposal_json", None), {})
    rg       = _json_loads_safe(getattr(trade, "risk_guard_json",   None), {})

    duration_seconds = None
    if trade.opened_at and trade.closed_at:
        try:
            duration_seconds = (trade.closed_at - trade.opened_at).total_seconds()
        except Exception:
            pass
    elif trade.opened_at:
        try:
            duration_seconds = (now_utc - trade.opened_at).total_seconds()
        except Exception:
            pass

    pnl     = getattr(trade, "pnl", None)
    outcome = getattr(trade, "outcome", None) or "unknown"
    if pnl is not None:
        try:
            pnl_f = float(pnl)
            if pnl_f > 0:
                outcome = "win"
            elif pnl_f < 0:
                outcome = "loss"
            else:
                outcome = "breakeven"
        except (ValueError, TypeError):
            pass

    entry = getattr(trade, "entry_price", None)
    sl    = getattr(trade, "stop_loss",   None)
    tp    = getattr(trade, "take_profit", None)
    rr_actual = None
    if pnl is not None and entry and sl:
        try:
            risk = abs(float(entry) - float(sl))
            if risk > 0:
                rr_actual = round(float(pnl) / risk, 2)
        except (ValueError, TypeError):
            pass

    notes = []
    if outcome == "win":
        notes.append("Trade closed in profit.")
    elif outcome == "loss":
        notes.append("Trade closed at a loss.")
    elif outcome == "breakeven":
        notes.append("Trade closed near breakeven.")
    if duration_seconds is not None:
        notes.append(f"Duration: ~{int(duration_seconds / 60)} minutes.")
    if getattr(trade, "risk_reward", None):
        notes.append(f"Planned R:R was {trade.risk_reward}.")
    if rr_actual is not None:
        notes.append(f"Actual R:R was {rr_actual}.")
    rg_score = rg.get("score") if rg else None
    if rg_score is not None:
        notes.append(f"Risk Guard score at entry: {rg_score}.")

    return {
        "reviewed_at":         now_utc.isoformat(),
        "outcome":             outcome,
        "pnl":                 pnl,
        "fees":                getattr(trade, "fees",        None),
        "duration_seconds":    duration_seconds,
        "rr_planned":          getattr(trade, "risk_reward", None),
        "rr_actual":           rr_actual,
        "entry_price":         entry,
        "stop_loss":           sl,
        "take_profit":         tp,
        "direction":           getattr(trade, "direction",   None),
        "symbol":              getattr(trade, "symbol",      None),
        "rg_score":            rg_score,
        "proposal_confidence": proposal.get("confidence"),
        "notes":               notes,
        "ai_review":           None,
    }


def _lm_build_ai_context(item) -> dict:
    """Build a compact, JSON-safe context dict from a LiveMonitorItem for the AI agent."""
    snap = _json_loads_safe(getattr(item, "snapshot_json", None), {})
    lh   = snap.get("latest_health") or {}
    mtf  = snap.get("latest_mtf_scan") or {}
    sess = snap.get("latest_session_context") or {}
    mktc = snap.get("latest_market_context") or {}
    tp   = snap.get("timeframe_policy") or {
        "visible_analysis_timeframes": ["15m", "30m", "1h", "4h", "1d"],
        "bias_min_timeframe":          "1h",
        "hidden_execution_timeframe":  "5m",
    }

    # Compact health: keep key fields only
    health_compact = None
    if lh:
        health_compact = {
            "health_score": lh.get("health_score"),
            "grade":        lh.get("grade"),
            "status":       lh.get("status"),
            "status_label": lh.get("status_label"),
            "direction":    lh.get("direction"),
            "zone":         lh.get("zone"),
            "bias":         lh.get("bias"),
            "checklist":    lh.get("checklist"),
            "reasons":      lh.get("reasons"),
            "warnings":     lh.get("warnings"),
        }

    # Compact MTF: keep summary fields only
    mtf_compact = None
    if mtf:
        mtf_compact = {
            "exchange":    mtf.get("exchange"),
            "market":      mtf.get("market"),
            "refreshed_at": mtf.get("refreshed_at"),
            "timeframes":  mtf.get("timeframes"),
            "modules":     mtf.get("modules"),
            "summary":     mtf.get("summary"),
        }

    # Compact session: keep key fields only
    sess_compact = None
    if sess:
        sess_compact = {
            "session_key":          sess.get("session_key"),
            "session_label":        sess.get("session_label"),
            "liquidity_label":      sess.get("liquidity_label"),
            "volatility_label":     sess.get("volatility_label"),
            "scalp_quality":        sess.get("scalp_quality"),
            "is_prime_time":        sess.get("is_prime_time"),
            "is_transition_window": sess.get("is_transition_window"),
            "is_weekend":           sess.get("is_weekend"),
            "ai_context":           sess.get("ai_context"),
        }

    # Compact market context: keep key fields only
    mktc_compact = None
    if mktc:
        mktc_compact = {
            "exchange":      mktc.get("exchange"),
            "market":        mktc.get("market"),
            "ok":            mktc.get("ok"),
            "funding":       mktc.get("funding"),
            "open_interest": mktc.get("open_interest"),
            "long_short":    mktc.get("long_short"),
            "taker_pressure": mktc.get("taker_pressure"),
            "liquidations":  mktc.get("liquidations"),
            "summary":       mktc.get("summary"),
        }

    return {
        "symbol":   item.symbol,
        "exchange": item.exchange or "binance",
        "market":   item.market   or "perpetual",
        "setup": {
            "type":          item.setup_type,
            "direction":     item.direction,
            "timeframe":     item.timeframe,
            "source_tab":    getattr(item, "source_tab", None),
            "zone_high":     item.zone_high,
            "zone_low":      item.zone_low,
            "current_price": item.current_price,
            "status":        item.status,
            "score":         item.score,
            "confidence":    item.confidence,
        },
        "timeframe_policy": {
            "visible_analysis_timeframes": tp.get("visible_analysis_timeframes",
                                                   ["15m", "30m", "1h", "4h", "1d"]),
            "bias_min_timeframe":         tp.get("bias_min_timeframe", "1h"),
            "hidden_execution_timeframe": tp.get("hidden_execution_timeframe", "5m"),
            "note": "5m is future execution timeframe only. Do not use it for bias analysis.",
        },
        "health":          health_compact,
        "mtf":             mtf_compact,
        "session":         sess_compact,
        "market_context":  mktc_compact,
        "rules": {
            "bias_min_timeframe":   "1h",
            "do_not_use_5m_for_bias": True,
            "no_trade_execution":   True,
            "analysis_only":        True,
            "built_in_brain_not_user_instructions": True,
            "custom_instructions_are_extra_preferences_only": True,
        },
        "ai_brain":               _lm_builtin_ai_brain_rules(),
        "custom_ai_instructions": _lm_custom_ai_instructions_from_snapshot(snap),
        "event_detection":        (lambda evd: {
            "computed_at": evd.get("computed_at"),
            "summary":     evd.get("summary"),
            "events": [
                {
                    "event_key": e.get("event_key"),
                    "type":      e.get("type"),
                    "label":     e.get("label"),
                    "severity":  e.get("severity"),
                    "direction": e.get("direction"),
                    "source":    e.get("source"),
                    "details":   e.get("details"),
                }
                for e in (evd.get("events") or [])[:8]
            ],
        } if evd and isinstance(evd, dict) else None)(snap.get("latest_event_detection")),
        "candle_features":        (lambda cf: {
            "computed_at": cf.get("computed_at"),
            "interval":    cf.get("interval"),
            "features":    cf.get("features"),
        } if cf and isinstance(cf, dict) else None)(snap.get("latest_candle_features")),
        "setup_readiness":        (lambda sr: {
            "computed_at":          sr.get("computed_at"),
            "readiness_state":      sr.get("readiness_state"),
            "confirmation_score":   sr.get("confirmation_score"),
            "risk_score":           sr.get("risk_score"),
            "supporting_events":    (sr.get("supporting_events") or [])[:5],
            "risk_events":          (sr.get("risk_events") or [])[:5],
            "missing_confirmations": sr.get("missing_confirmations"),
            "summary":              sr.get("summary"),
        } if sr and isinstance(sr, dict) else None)(snap.get("latest_setup_readiness")),
        "reasoning_memory":       (lambda mem_recs, summ, acc: {
            "summary": summ,
            "accuracy": {
                "total_closed":  acc.get("total_closed"),
                "win_count":     acc.get("win_count"),
                "loss_count":    acc.get("loss_count"),
                "win_rate":      acc.get("win_rate"),
            } if acc else None,
            "latest_records": [
                {
                    "created_at":         r.get("created_at"),
                    "source":             r.get("source"),
                    "setup_type":         r.get("setup_type"),
                    "direction":          r.get("direction"),
                    "timeframe":          r.get("timeframe"),
                    "readiness_state":    r.get("readiness_state"),
                    "confirmation_score": r.get("confirmation_score"),
                    "risk_score":         r.get("risk_score"),
                    "supporting_events":  (r.get("supporting_events") or [])[:5],
                    "risk_events":        (r.get("risk_events") or [])[:5],
                    "outcome_status":     r.get("outcome_status"),
                    "outcome_note":       r.get("outcome_note"),
                }
                for r in (mem_recs or [])[-5:]
            ],
        } if mem_recs or summ else None)(
            snap.get("setup_memory_records"),
            snap.get("setup_memory_summary"),
            snap.get("setup_memory_accuracy"),
        ),
        "aggregated_market_context": (lambda amc: {
            "computed_at":     amc.get("computed_at"),
            "sources_used":    amc.get("sources_used"),
            "sources_failed":  amc.get("sources_failed"),
            "primary_source":  amc.get("primary_source"),
            "funding":         amc.get("funding"),
            "open_interest":   amc.get("open_interest"),
            "taker_pressure":  amc.get("taker_pressure"),
            "long_short":      amc.get("long_short"),
            "bias":            amc.get("bias"),
            "agreement_score": amc.get("agreement_score"),
            "agreement_note":  amc.get("agreement_note"),
            "conflict_score":  amc.get("conflict_score"),
            "warnings":        amc.get("warnings"),
            "summary":         amc.get("summary"),
        } if amc and isinstance(amc, dict) else None)(snap.get("latest_aggregated_market_context")),
        # Phase 9.7: compact summary of up to 3 most recent demo trades (no raw candles, no API keys)
        "latest_trade_summary": (lambda item_id: (
            lambda trades: [
                {
                    "trade_uid":         t.trade_uid,
                    "status":            t.status,
                    "direction":         t.direction,
                    "symbol":            t.symbol,
                    "outcome":           t.outcome,
                    "pnl":               t.pnl,
                    "risk_guard_status": t.risk_guard_status,
                    "created_at":        t.created_at.isoformat() if t.created_at else None,
                    "closed_at":         t.closed_at.isoformat()  if t.closed_at  else None,
                    "post_trade_review": _json_loads_safe(t.post_trade_review_json, None),
                }
                for t in trades
            ] if trades else None
        )(_lm_ai_context_recent_trades(item_id))
        )(getattr(item, "id", None)),
    }


def _lm_ai_context_recent_trades(item_id) -> list:
    """Return up to 3 most recent LiveMonitorTrade rows for item_id. Used by _lm_build_ai_context."""
    if not item_id:
        return []
    try:
        from models import LiveMonitorTrade as _LMT
        return (
            _LMT.query
            .filter_by(live_monitor_item_id=item_id)
            .order_by(_LMT.created_at.desc())
            .limit(3)
            .all()
        )
    except Exception:
        return []


def _lm_ai_system_prompt() -> str:
    """Return the system prompt for the ZyNi Live Monitor AI Agent (Phase 6.6 updated)."""
    return (
        "You are the ZyNi Live Monitor AI Agent — a professional SMC (Smart Money Concepts) "
        "setup analyst. Your job is to analyze trading setup facts provided by the backend "
        "and give the user a clear, factual analysis.\n\n"

        "BUILT-IN AI BRAIN (Phase 6.6):\n"
        "You already have built-in trading intelligence. The user does NOT need to teach you:\n"
        "- Order Block (OB): OB zones can be PRIMARY setup modules — use OB touch/reaction quality, "
        "zone validity, liquidity context, and confirmation quality automatically.\n"
        "- Fibonacci (FIB): FIB zones can be PRIMARY setup modules — use FIB reaction quality, "
        "key levels, trend context, wick/close behavior, and confirmation quality automatically.\n"
        "- Order Flow: Use taker pressure, OI, funding, liquidations, volume, and sweep/rejection "
        "context as confirmation when available.\n"
        "- Session context, bias context, liquidity sweeps, wick rejection, FVG, Breaker — "
        "all handled automatically from context data.\n"
        "Do not wait for the user to explain basic OB/FIB/order-flow logic.\n\n"

        "CUSTOM AI INSTRUCTIONS (Phase 6.6):\n"
        "If custom_ai_instructions are provided in the context, apply them as EXTRA FILTERS only.\n"
        "- Custom instructions are user preferences layered ON TOP of built-in brain.\n"
        "- Custom instructions cannot override safety rules or risk management.\n"
        "- Custom instructions cannot grant permission to execute trades.\n"
        "- If a custom instruction conflicts with safety, safety always wins.\n"
        "- If a custom instruction conflicts with built-in brain logic, note the conflict in agent_note.\n\n"

        "EVENT DETECTION (Phase 6.7/6.8):\n"
        "The context may include an event_detection block with pre-computed situational signals.\n"
        "Use it as your current 'eyes' on the setup:\n"
        "- 'risk' severity events: reduce confidence, flag in risks[] and zone_read.\n"
        "- 'warning' severity events: note as caution factors in risks[] or agent_note.\n"
        "- 'watch' events (e.g. zone_touch): setup is in active zone — treat as significant.\n"
        "- 'info' events: useful background context, no urgent action required.\n"
        "- Do NOT invent candle rejection, wick events, liquidity sweeps, or liquidation "
        "spikes that are NOT present in event_detection.\n"
        "- Do NOT treat any detected event as trade execution permission.\n"
        "- If event_detection is null or empty, analyze from other context fields only.\n\n"

        "CANDLE FEATURES (Phase 7.1):\n"
        "The context may include a candle_features block with compact extracted chart statistics.\n"
        "Use these as chart behavior evidence when reasoning:\n"
        "- atr_14: volatility as % of price.\n"
        "- last_candle_body_pct / last_upper_wick_pct / last_lower_wick_pct: last candle structure.\n"
        "- strong_rejection: 'bullish' (large lower wick), 'bearish' (large upper wick), or 'none'.\n"
        "- compression: true = ATR < 5% of 50-candle range — potential breakout setup.\n"
        "- price_position_50: 0 = 50c low, 1 = 50c high, 0.5 = mid-range.\n"
        "- trend_slope_20: % per candle, positive = uptrend, negative = downtrend.\n"
        "- breakout_context: 'above_range', 'below_range', or 'inside_range'.\n"
        "- volume_spike_ratio: last volume vs 20-candle average (>1.5 = elevated).\n"
        "CANDLE-BASED EVENT RULES (Phase 7.1):\n"
        "- Candle-based events (candle_wick_rejection, candle_volume_spike, candle_compression,\n"
        "  candle_breakout_context, setup_reaction_near_zone, range_sweep_style_rejection)\n"
        "  are EVIDENCE ONLY. They are NOT trade permission.\n"
        "- 'range_sweep_style_rejection' means the wick dipped outside the previous range and closed\n"
        "  back inside. This is sweep-STYLE behavior. It is NOT a confirmed liquidity sweep.\n"
        "  Do NOT claim liquidity was swept. Do NOT use it as an entry signal.\n"
        "- 'setup_reaction_near_zone' means a candle rejection occurred near the OB/FIB zone.\n"
        "  This is a watch/confirmation signal only. Do NOT say 'entry confirmed'.\n"
        "- OB/FIB reaction events mean watch closely — not trade now.\n"
        "- Do NOT invent candle patterns not derivable from the provided features.\n"
        "- Do NOT reference raw candle data — only use the extracted scalar features.\n"
        "- If candle_features is null, do not speculate about chart structure.\n"
        "- Features are statistical summaries only. Never treat them as guaranteed signals.\n"
        "- Do NOT use candle features as permission to recommend trade execution.\n\n"

        "SETUP READINESS (Phase 7.2):\n"
        "The context may include a setup_readiness block — a deterministic backend score.\n"
        "- readiness_state values: wait | active_watch | strong_watch | high_risk | avoid\n"
        "- confirmation_score (0-100): how many watch-level signals support the setup.\n"
        "- risk_score (0-100): how many risk/warning-level signals oppose the setup.\n"
        "- supporting_events / risk_events: event labels that drove the score.\n"
        "- missing_confirmations: what is still absent before the setup is considered ready.\n"
        "USE RULES:\n"
        "- setup_readiness is a deterministic backend scoring system. Use it as guidance only.\n"
        "- strong_watch means 'watch closely' — NOT 'enter now'. Do NOT say 'enter now'.\n"
        "- active_watch means 'monitor' — NOT 'buy' or 'sell'.\n"
        "- high_risk or avoid: always reduce your confidence score and flag risks[]. Never say 'safe'.\n"
        "- Do NOT override readiness state. If state is high_risk, do not call it active_watch.\n"
        "- If setup_readiness is null, analyze from other context fields only.\n"
        "- Do NOT use setup_readiness as trade permission under any circumstances.\n\n"

        "REASONING MEMORY (Phase 8.2):\n"
        "The context may include a reasoning_memory block — historical setup records and accuracy stats.\n"
        "USE RULES:\n"
        "- reasoning_memory is historical context only. Use it to note repeated patterns or behaviors.\n"
        "- Do NOT change strategy rules based on memory records.\n"
        "- Do NOT claim the system 'learned' or 'improved' based on past records.\n"
        "- outcome_status 'pending' means no result is known yet — do NOT assume won or lost.\n"
        "- win_rate is an observation from a small sample only — it is NOT a statistical guarantee.\n"
        "- Do NOT use past win/loss records as permission to enter a trade.\n"
        "- memory is context clues only — not trade permission, not strategy modification.\n"
        "- If reasoning_memory is null, analyze from other context fields only.\n\n"

        "AGGREGATED MARKET CONTEXT (Phase 9.2):\n"
        "The context may include an aggregated_market_context block — a Binance-first market view.\n"
        "USE RULES:\n"
        "- primary_source is always 'binance' in Phase 9.2. Binance data is the authoritative source.\n"
        "- If sources_used contains only 'binance', this is a single-source context — "
        "do NOT claim multi-exchange agreement.\n"
        "- agreement_score/conflict_score will be null when only one source is used — do not invent values.\n"
        "- MEXC funding/OI fields (if present in warnings) are supplemental and informational only.\n"
        "- Do NOT use Binance market context as an exact MEXC execution price or level.\n"
        "- Do NOT treat market bias from aggregated context as trade execution permission.\n"
        "- If aggregated_market_context is null, use market_context if available, or note it is missing.\n"
        "- Do NOT invent exchange data that is not present in sources_used.\n\n"

        "RISK GUARD (Phase 9.5):\n"
        "A backend Risk Guard independently validates all AI proposals before any approval.\n"
        "USE RULES:\n"
        "- Your analysis is NEVER approval to execute a trade.\n"
        "- Risk Guard approval is NEVER execution — it is only a safety pass.\n"
        "- No trade or order can be placed in Phase 9.5. Execution is Phase 9.6+ only.\n"
        "- Do NOT imply that a 'confirmed_watch' or 'strong_watch' verdict means enter now.\n"
        "- If you are asked for a trade proposal (separate endpoint), output proposal JSON only.\n\n"

        "STRICT RULES:\n"
        "- Analyze ONLY the provided backend facts. Do not invent missing data.\n"
        "- Do NOT claim any trade is guaranteed or risk-free.\n"
        "- Do NOT give a direct order to buy or sell. Never say 'place buy now'.\n"
        "- Do NOT ask for API keys, credentials, or personal account data.\n"
        "- Do NOT call external tools or make external requests.\n"
        "- Do NOT change or override strategy rules.\n"
        "- Bias detection minimum timeframe is 1H. Never use 5m for bias.\n"
        "- 5m is a FUTURE hidden execution timeframe only — do not reference it for analysis.\n"
        "- Use 15m/30m for entry confirmation, 1H for bias lock, 4H/1D for context.\n"
        "- If data is missing (null), state clearly what is missing.\n"
        "- Keep explanations concise and factual.\n"
        "- Your response must be valid JSON only — no prose outside the JSON object.\n\n"

        "VERDICT OPTIONS:\n"
        "- 'watch'           — setup is worth monitoring, no immediate action.\n"
        "- 'wait'            — setup exists but key confirmations are missing.\n"
        "- 'confirmed_watch' — setup has strong alignment; watch closely for entry signal.\n"
        "- 'high_risk'       — notable warning signs; elevated caution required.\n"
        "- 'avoid'           — setup has critical issues or invalidation conditions.\n\n"

        "OUTPUT FORMAT — respond with ONLY this JSON object:\n"
        "{\n"
        "  \"verdict\": \"watch|wait|confirmed_watch|high_risk|avoid\",\n"
        "  \"confidence\": <integer 0-100>,\n"
        "  \"summary\": \"<1-2 sentence overall read>\",\n"
        "  \"bias_read\": \"<what the 1H/4H/1D bias says>\",\n"
        "  \"zone_read\": \"<zone status, distance, inside/near/breach risk>\",\n"
        "  \"session_read\": \"<session context impact on setup>\",\n"
        "  \"market_context_read\": \"<funding/taker/L-S ratio interpretation>\",\n"
        "  \"confirmations_needed\": [\"<list of missing confirmations>\"],\n"
        "  \"risks\": [\"<list of current risk factors>\"],\n"
        "  \"invalidations\": [\"<list of conditions that would invalidate this setup>\"],\n"
        "  \"next_actions\": [\"<list of what to watch or wait for>\"],\n"
        "  \"agent_note\": \"<any important note; mention applied custom instructions here>\",\n"
        "  \"disclaimer\": \"Analysis only. Not financial advice.\"\n"
        "}\n"
        "Do not wrap the JSON in markdown code fences. Output raw JSON only."
    )


def _lm_parse_ai_json(text: str) -> dict:
    """Parse AI provider output safely. Strips markdown fences if present."""
    if not text:
        return {"_parse_error": "empty response", "raw_text": ""}
    t = text.strip()
    # Strip markdown code fences
    if t.startswith("```"):
        lines = t.splitlines()
        t = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
    t = t.strip()
    try:
        return json.loads(t)
    except Exception:
        return {
            "_parse_error": "Invalid JSON from provider",
            "raw_text":     t[:400],
        }


def _lm_local_ai_fallback(context: dict, user_message: str = None) -> dict:
    """Rule-based analysis from snapshot facts — no external AI required."""
    setup      = context.get("setup") or {}
    health     = context.get("health") or {}
    session    = context.get("session") or {}
    mktc       = context.get("market_context") or {}
    rules      = context.get("rules") or {}

    status     = health.get("status") or setup.get("status") or "unknown"
    score      = health.get("health_score") or setup.get("score") or 0
    grade      = health.get("grade") or "?"
    direction  = health.get("direction") or setup.get("direction") or "neutral"
    zone       = health.get("zone") or {}
    bias       = health.get("bias") or {}
    checklist  = health.get("checklist") or {}
    warnings   = health.get("warnings") or []

    zone_status  = zone.get("zone_status", "unknown")
    bias_aligned = bias.get("bias_aligned")
    sess_label   = session.get("session_label", "Unknown session")
    sess_caut    = (session.get("ai_context") or {}).get("recommended_caution", "medium")

    # Determine verdict
    if status == "confirmed_watch" or (score >= 75 and bias_aligned is True):
        verdict    = "confirmed_watch"
        confidence = min(score, 88)
    elif status == "breach_risk":
        verdict    = "high_risk"
        confidence = 30
    elif status in ("inzone", "near") and bias_aligned is True:
        verdict    = "watch"
        confidence = min(score, 72)
    elif status == "warning" or bias_aligned is False:
        verdict    = "high_risk"
        confidence = 25
    elif status == "watching":
        verdict    = "wait"
        confidence = min(score, 55)
    else:
        verdict    = "wait"
        confidence = 40

    # Bias read
    ctx_count  = bias.get("context_aligned_count", 0)
    bias_read  = (
        f"Bias {'aligned' if bias_aligned else 'not aligned' if bias_aligned is False else 'unknown'}. "
        f"{ctx_count} higher-TF(s) confirm direction."
    )

    # Zone read
    dist_pct = zone.get("distance_pct")
    dist_str = f" ({dist_pct:.2f}% from zone edge)" if dist_pct is not None else ""
    zone_read = f"Zone status: {zone_status}{dist_str}."

    # Session read
    sess_read = f"Session: {sess_label}. Caution level: {sess_caut}."
    if session.get("is_weekend"):
        sess_read += " Weekend — low liquidity."

    # Market context read
    mc_parts = []
    if mktc.get("funding", {}).get("available"):
        mc_parts.append(f"Funding {mktc['funding'].get('bias','neutral')}")
    if mktc.get("taker_pressure", {}).get("available"):
        mc_parts.append(f"Taker {mktc['taker_pressure'].get('bias','neutral')}")
    if mktc.get("long_short", {}).get("available"):
        mc_parts.append(f"L/S ratio {mktc['long_short'].get('ratio')}")
    mc_read = ", ".join(mc_parts) + "." if mc_parts else "Market context not yet fetched."

    # Confirmations needed
    confs = []
    if not checklist.get("bias_aligned"):
        confs.append("1H bias alignment missing")
    if zone_status == "watching":
        confs.append("Price needs to approach zone")
    if zone_status not in ("inside", "near"):
        confs.append("15m confirmation close inside zone")
    if not checklist.get("volume_ok"):
        confs.append("Volume confirmation")

    # Risks
    risks = list(warnings) if warnings else []
    if sess_caut == "high":
        risks.append(f"High caution session: {sess_label}")
    if score < 35:
        risks.append(f"Low health score ({score})")

    # Invalidations
    invs = []
    if zone.get("zone_low") is not None:
        invs.append(f"Close below zone low ({zone.get('zone_low')}) invalidates bullish setup")
    if zone.get("zone_high") is not None:
        invs.append(f"Close above zone high ({zone.get('zone_high')}) invalidates bearish setup")
    invs.append("Health score drops below 35")

    # Summary
    sym    = context.get("symbol", "?")
    tf     = setup.get("timeframe", "?")
    summary = (
        f"{sym} {direction} setup on {tf}. "
        f"Health {score}/100 grade {grade}. "
        f"Zone {zone_status}. "
        f"{'Bias aligned' if bias_aligned else 'Bias not aligned' if bias_aligned is False else 'Bias unknown'}. "
        f"Local fallback analysis (configure OpenRouter for AI)."
    )

    return {
        "verdict":               verdict,
        "confidence":            confidence,
        "summary":               summary,
        "bias_read":             bias_read,
        "zone_read":             zone_read,
        "session_read":          sess_read,
        "market_context_read":   mc_read,
        "confirmations_needed":  confs or ["No immediate confirmations required"],
        "risks":                 risks or ["No critical risks detected"],
        "invalidations":         invs,
        "next_actions":          [
            "Monitor zone approach",
            "Wait for 1H candle close inside zone",
            f"Configure OpenRouter to enable real AI analysis",
        ],
        "agent_note": f"This is a local rule-based fallback. Score: {score}, Status: {status}.",
        "disclaimer": "Analysis only. Not financial advice.",
    }


def _lm_call_openai_compatible_agent(agent: dict, context: dict,
                                      user_message: str = None) -> dict:
    """Call any OpenAI-compatible chat completions endpoint (OpenAI, DeepSeek, OpenRouter, custom)."""
    key_env_map = {
        "openrouter":    "OPENROUTER_API_KEY",
        "openai":        "OPENAI_API_KEY",
        "deepseek":      "DEEPSEEK_API_KEY",
        "custom_openai": "CUSTOM_AI_API_KEY",
    }
    provider = agent.get("provider", "openrouter")
    # Priority 1: agent-specific env name (from AI_AGENTS_JSON api_key_env)
    # Priority 2: provider hardcoded map fallback
    custom_key_env = agent.get("_api_key_env", "").strip()

    # Debug: log what the agent dict contains at call time (env name only, never key value)
    print(
        f"[_lm_call_openai_compatible_agent] agent.id={agent.get('id')!r} "
        f"agent.keys={sorted(agent.keys())} "
        f"_api_key_env={custom_key_env!r} provider={provider!r}"
    )

    key_env = custom_key_env if custom_key_env else key_env_map.get(provider, "OPENROUTER_API_KEY")
    api_key = os.environ.get(key_env, "").strip()

    # Fallback: if key still empty, try well-known env names keyed by agent id.
    # Safety net for cases where _api_key_env is missing or the env var is unset.
    if not api_key:
        _id_to_env = {
            "deepseek": "OPENROUTER_KEY_DEEPSEEK",
            "llama":    "OPENROUTER_KEY_LLAMA",
            "qwen":     "OPENROUTER_KEY_QWEN",
        }
        fallback_env = _id_to_env.get(agent.get("id", ""), "")
        if fallback_env:
            api_key = os.environ.get(fallback_env, "").strip()
            if api_key:
                print(
                    f"[_lm_call_openai_compatible_agent] used id→env fallback "
                    f"agent.id={agent.get('id')!r} fallback_env={fallback_env!r}"
                )

    # Priority 1: agent.api_base (resolved from api_base_env at config time)
    # Priority 2: env var from _api_base_env if present
    # Priority 3: provider default
    custom_base_env = agent.get("_api_base_env", "").strip()
    api_base = (
        agent.get("api_base")
        or (os.environ.get(custom_base_env, "").strip() if custom_base_env else "")
        or os.environ.get("OPENROUTER_API_BASE", "https://openrouter.ai/api/v1/chat/completions")
    )
    model    = agent.get("model", "")

    user_content = json.dumps({
        "context":      context,
        "user_message": user_message or "Analyze this setup and give your verdict.",
    }, ensure_ascii=False)

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type":  "application/json",
    }
    app_title = os.environ.get("OPENROUTER_APP_TITLE", "ZyNi SMC Screener")
    http_ref  = os.environ.get("OPENROUTER_HTTP_REFERER", "")
    if provider == "openrouter":
        if http_ref:
            headers["HTTP-Referer"] = http_ref
        if app_title:
            headers["X-Title"] = app_title

    payload = {
        "model":       model,
        "messages":    [
            {"role": "system", "content": _lm_ai_system_prompt()},
            {"role": "user",   "content": user_content},
        ],
        "temperature": 0.2,
        "max_tokens":  900,
    }

    resp = req.post(api_base, json=payload, headers=headers, timeout=20)
    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code}")
    data = resp.json()
    raw  = (data.get("choices") or [{}])[0].get("message", {}).get("content", "")
    return _lm_parse_ai_json(raw)


def _lm_call_anthropic_agent(agent: dict, context: dict,
                               user_message: str = None) -> dict:
    """Call Anthropic messages API. Falls back safely if env not configured."""
    # Priority 1: agent-specific env name; Priority 2: ANTHROPIC_API_KEY
    custom_key_env = agent.get("_api_key_env", "").strip()
    key_env  = custom_key_env if custom_key_env else "ANTHROPIC_API_KEY"
    api_key  = os.environ.get(key_env, "").strip()
    # Priority 1: agent.api_base; Priority 2: _api_base_env; Priority 3: default
    custom_base_env = agent.get("_api_base_env", "").strip()
    api_base = (
        agent.get("api_base")
        or (os.environ.get(custom_base_env, "").strip() if custom_base_env else "")
        or os.environ.get("ANTHROPIC_API_BASE", "https://api.anthropic.com/v1/messages")
    )
    model    = agent.get("model", "")
    if not api_key or not model:
        raise RuntimeError("Anthropic key or model missing")

    user_content = json.dumps({
        "context":      context,
        "user_message": user_message or "Analyze this setup and give your verdict.",
    }, ensure_ascii=False)

    headers = {
        "x-api-key":         api_key,
        "anthropic-version": "2023-06-01",
        "content-type":      "application/json",
    }
    payload = {
        "model":      model,
        "max_tokens": 900,
        "system":     _lm_ai_system_prompt(),
        "messages":   [{"role": "user", "content": user_content}],
    }
    resp = req.post(api_base, json=payload, headers=headers, timeout=20)
    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code}")
    data = resp.json()
    raw  = (data.get("content") or [{}])[0].get("text", "")
    return _lm_parse_ai_json(raw)


def _lm_call_gemini_agent(agent: dict, context: dict,
                            user_message: str = None) -> dict:
    """Call Google Gemini generateContent API. Falls back safely if env not configured."""
    # Priority 1: agent-specific env name; Priority 2: GEMINI_API_KEY
    custom_key_env = agent.get("_api_key_env", "").strip()
    key_env  = custom_key_env if custom_key_env else "GEMINI_API_KEY"
    api_key  = os.environ.get(key_env, "").strip()
    model    = agent.get("model", "")
    if not api_key or not model:
        raise RuntimeError("Gemini key or model missing")

    # Priority 1: agent.api_base; Priority 2: _api_base_env; Priority 3: default
    custom_base_env = agent.get("_api_base_env", "").strip()
    api_base = (
        agent.get("api_base")
        or (os.environ.get(custom_base_env, "").strip() if custom_base_env else "")
        or os.environ.get("GEMINI_API_BASE", "")
        or f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    )

    user_content = json.dumps({
        "context":      context,
        "user_message": user_message or "Analyze this setup and give your verdict.",
    }, ensure_ascii=False)

    combined = _lm_ai_system_prompt() + "\n\n" + user_content
    payload  = {"contents": [{"parts": [{"text": combined}]}]}
    url      = f"{api_base}?key={api_key}"
    resp     = req.post(url, json=payload, timeout=20)
    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code}")
    data = resp.json()
    raw  = ((data.get("candidates") or [{}])[0]
            .get("content", {}).get("parts", [{}])[0].get("text", ""))
    return _lm_parse_ai_json(raw)


def _lm_call_ai_provider(context: dict, user_message: str = None,
                           agent_id: str = None) -> dict:
    """Route an AI call to the correct provider. Returns local fallback on any failure."""
    agent    = _lm_get_ai_agent_config(agent_id)
    provider = agent.get("provider", "local_fallback")
    aid      = agent.get("id", "local_fallback")
    label    = agent.get("label", "Local Fallback")
    model    = agent.get("model", "")

    base_result = {
        "agent_id":    aid,
        "agent_label": label,
        "provider":    provider,
        "model":       model,
        "configured":  agent.get("configured", False),
    }

    if provider == "local_fallback" or not agent.get("configured"):
        return {**base_result, "ok": False, "configured": False,
                "analysis": _lm_local_ai_fallback(context, user_message),
                "error": "AI provider not configured"}

    try:
        if provider in ("openrouter", "openai", "deepseek", "custom_openai"):
            analysis = _lm_call_openai_compatible_agent(agent, context, user_message)
        elif provider == "anthropic":
            analysis = _lm_call_anthropic_agent(agent, context, user_message)
        elif provider == "gemini":
            analysis = _lm_call_gemini_agent(agent, context, user_message)
        else:
            # Unknown provider — use openai-compatible style as best-effort
            analysis = _lm_call_openai_compatible_agent(agent, context, user_message)

        return {**base_result, "ok": True, "analysis": analysis}

    except Exception as _e:
        return {**base_result, "ok": False,
                "analysis": _lm_local_ai_fallback(context, user_message),
                "error": f"Provider error: {type(_e).__name__}"}


def _lm_call_ai_agent(context: dict, user_message: str = None) -> dict:
    """Backward-compatible wrapper — delegates to _lm_call_ai_provider with default agent."""
    return _lm_call_ai_provider(context, user_message, agent_id=None)


# ── Phase 6.5 Task 5: Consensus Engine ───────────────────────────────────────

def _lm_consensus_from_agent_results(results: list) -> dict:
    """
    Aggregate multiple agent results into a single consensus verdict.

    Tie-break is CONSERVATIVE: when verdicts tie on vote count, the most
    conservative verdict wins (avoid > high_risk > wait > watch > confirmed_watch).
    If agents split between the positive side and the risk side, the winner is
    downgraded to at most wait/high_risk.

    results: list of dicts from _lm_call_ai_provider (one per agent).
    Returns a consensus dict:
      {ok, verdict, confidence, summary, agent_count, ok_count,
       agreement, disagreement, disagreement_note, agents, analysis}
    """
    if not results:
        return {
            "ok": False, "verdict": "wait", "confidence": 0,
            "summary": "No agent results.", "agent_count": 0,
            "ok_count": 0, "agreement": 0.0,
            "disagreement": False, "disagreement_note": "",
            "agents": [], "analysis": {},
        }

    # Conservative rank: lower number = more conservative (preferred in ties)
    _CONSERVATIVE_RANK = {
        "avoid":           0,
        "high_risk":       1,
        "wait":            2,
        "watch":           3,
        "confirmed_watch": 4,
    }
    _POSITIVE_VERDICTS = {"watch", "confirmed_watch"}
    _RISK_VERDICTS     = {"high_risk", "avoid"}

    ok_results  = [r for r in results if r.get("ok") and r.get("analysis")]
    agent_count = len(results)
    ok_count    = len(ok_results)

    agent_summaries = []
    for r in results:
        an = r.get("analysis", {})
        agent_summaries.append({
            "agent_id":    r.get("agent_id", ""),
            "agent_label": r.get("agent_label", ""),
            "provider":    r.get("provider", ""),
            "ok":          r.get("ok", False),
            "verdict":     an.get("verdict", "wait"),
            "confidence":  an.get("confidence", 0),
            "summary":     an.get("summary", ""),
        })

    if not ok_results:
        fallback = _lm_local_ai_fallback({}, None)
        return {
            "ok": False, "verdict": fallback.get("verdict", "wait"),
            "confidence": 0, "summary": "All agents failed; local fallback used.",
            "agent_count": agent_count, "ok_count": 0, "agreement": 0.0,
            "disagreement": False, "disagreement_note": "",
            "agents": agent_summaries, "analysis": fallback,
        }

    # Tally votes
    verdict_votes:  dict = {}
    total_confidence = 0
    for r in ok_results:
        an      = r.get("analysis", {})
        verdict = an.get("verdict", "wait")
        conf    = int(an.get("confidence") or 0)
        verdict_votes[verdict] = verdict_votes.get(verdict, 0) + 1
        total_confidence += conf

    # Pick verdict: highest votes; ties broken by lowest conservative rank
    max_votes      = max(verdict_votes.values())
    tied_verdicts  = [v for v, cnt in verdict_votes.items() if cnt == max_votes]
    winner_verdict = min(tied_verdicts, key=lambda v: _CONSERVATIVE_RANK.get(v, 2))
    winner_count   = verdict_votes[winner_verdict]
    agreement      = round(winner_count / ok_count, 2) if ok_count else 0.0
    avg_confidence = round(total_confidence / ok_count) if ok_count else 0

    # Disagreement detection
    positive_votes = sum(verdict_votes.get(v, 0) for v in _POSITIVE_VERDICTS)
    risk_votes     = sum(verdict_votes.get(v, 0) for v in _RISK_VERDICTS)
    unique_verdicts = len(verdict_votes)
    disagreement      = False
    disagreement_note = ""

    if unique_verdicts > 1 and agreement < 0.67:
        disagreement      = True
        disagreement_note = "Agents disagree on this setup."

    if positive_votes > 0 and risk_votes > 0:
        disagreement      = True
        disagreement_note = (
            "Agents split between positive and risk verdicts — treat as uncertain."
        )
        # Downgrade winner if it falls on the positive side
        if _CONSERVATIVE_RANK.get(winner_verdict, 2) >= _CONSERVATIVE_RANK["watch"]:
            winner_verdict = "high_risk" if risk_votes >= positive_votes else "wait"
            # Recalculate agreement based on overridden verdict count
            winner_count = verdict_votes.get(winner_verdict, 0)
            agreement    = round(winner_count / ok_count, 2) if ok_count else 0.0

    # Build merged analysis from first result whose verdict matches winner
    winning_result = next(
        (r for r in ok_results if r["analysis"].get("verdict") == winner_verdict),
        ok_results[0],
    )
    merged_analysis = dict(winning_result.get("analysis", {}))
    merged_analysis["confidence"] = avg_confidence

    # Aggregate confirmations, risks, invalidations from all ok agents (deduped)
    all_confirmations: list = []
    all_risks: list = []
    all_invalidations: list = []
    all_next_actions: list = []
    seen_c: set = set()
    seen_r: set = set()
    seen_i: set = set()
    seen_n: set = set()
    for r in ok_results:
        an = r.get("analysis", {})
        for item in (an.get("confirmations_needed") or []):
            if item not in seen_c:
                seen_c.add(item); all_confirmations.append(item)
        for item in (an.get("risks") or []):
            if item not in seen_r:
                seen_r.add(item); all_risks.append(item)
        for item in (an.get("invalidations") or []):
            if item not in seen_i:
                seen_i.add(item); all_invalidations.append(item)
        for item in (an.get("next_actions") or []):
            if item not in seen_n:
                seen_n.add(item); all_next_actions.append(item)

    merged_analysis["confirmations_needed"] = all_confirmations[:6]
    merged_analysis["risks"]                = all_risks[:6]
    merged_analysis["invalidations"]        = all_invalidations[:6]
    merged_analysis["next_actions"]         = all_next_actions[:4]

    # Consensus summary line
    labels_ok = [a["agent_label"] for a in agent_summaries if a["ok"]]
    merged_analysis["agent_note"] = (
        f"Consensus from {ok_count}/{agent_count} agents "
        f"({', '.join(labels_ok[:3])}{'...' if len(labels_ok) > 3 else ''}): "
        f"{winner_count}/{ok_count} agree → {winner_verdict}."
        + (f" [{disagreement_note}]" if disagreement else "")
    )

    return {
        "ok":               True,
        "verdict":          winner_verdict,
        "confidence":       avg_confidence,
        "summary":          merged_analysis.get("summary", ""),
        "agent_count":      agent_count,
        "ok_count":         ok_count,
        "agreement":        agreement,
        "disagreement":     disagreement,
        "disagreement_note": disagreement_note,
        "agents":           agent_summaries,
        "analysis":         merged_analysis,
    }


def _live_monitor_item_to_dict(item) -> dict:
    """Serialize a LiveMonitorItem row to a JSON-friendly dict."""
    return {
        "id":                 item.id,
        "user_id":            item.user_id,
        "symbol":             item.symbol,
        "exchange":           item.exchange,
        "market":             item.market,
        "source_tab":         item.source_tab,
        "setup_type":         item.setup_type,
        "direction":          item.direction,
        "timeframe":          item.timeframe,
        "zone_high":          item.zone_high,
        "zone_low":           item.zone_low,
        "confidence":         item.confidence,
        "score":              item.score,
        "current_price":      item.current_price,
        "status":             item.status,
        "snapshot":           _json_loads_safe(item.snapshot_json, {}),
        "selected_timeframes": _json_loads_safe(item.selected_timeframes, ["15m", "30m", "1h", "4h", "1d"]),
        "selected_modules":   _json_loads_safe(item.selected_modules, ["OB", "FVG", "FIB", "Breaker", "Bias"]),
        "alert_settings":     _json_loads_safe(item.alert_settings_json, {}),
        "is_active":          bool(item.is_active),
        "added_at":           item.added_at.isoformat() if item.added_at else None,
        "updated_at":         item.updated_at.isoformat() if item.updated_at else None,
    }


def _live_monitor_event_to_dict(event) -> dict:
    """Serialize a LiveMonitorEvent row to a JSON-friendly dict."""
    return {
        "id":                   event.id,
        "item_id":              event.item_id,
        "user_id":              event.user_id,
        "symbol":               event.symbol,
        "event_type":           event.event_type,
        "event_description":    event.event_description,
        "details":              _json_loads_safe(event.details_json, {}),
        "health_score_at_event": event.health_score_at_event,
        "price_at_event":       event.price_at_event,
        "created_at":           event.created_at.isoformat() if event.created_at else None,
    }


def _preset_to_dict(p):
    try:
        payload = json.loads(p.payload) if p.payload else {}
    except Exception:
        payload = {}
    return {
        "id": p.id,
        "name": p.name,
        "payload": payload,
        "isDefault": bool(p.is_default),
        "createdAt": p.created_at.isoformat() if p.created_at else None,
        "updatedAt": p.updated_at.isoformat() if p.updated_at else None,
    }


@app.route("/api/scan-presets", methods=["GET"])
@login_required
def api_scan_presets_list():
    uid = _current_user_id()
    if not uid:
        return jsonify({"presets": []})
    from models import db as _db, ScanPreset as _P
    rows = (_P.query.filter_by(user_id=uid).order_by(_P.is_default.desc(), _P.name.asc()).all())
    return jsonify({"presets": [_preset_to_dict(r) for r in rows]})


@app.route("/api/scan-presets", methods=["POST"])
@login_required
def api_scan_presets_create():
    uid = _current_user_id()
    if not uid:
        return jsonify({"error": "no_user"}), 401
    data = request.get_json(force=True) or {}
    name = (data.get("name") or "").strip()[:80]
    if not name:
        return jsonify({"error": "name_required"}), 400
    payload = data.get("payload")
    try:
        payload_str = json.dumps(payload if payload is not None else {})
    except Exception:
        return jsonify({"error": "bad_payload"}), 400
    from models import db as _db, ScanPreset as _P
    is_def = bool(data.get("isDefault"))
    if is_def:
        _P.query.filter_by(user_id=uid, is_default=True).update({"is_default": False})
    existing = _P.query.filter_by(user_id=uid, name=name).first()
    if existing:
        existing.payload    = payload_str
        existing.is_default = is_def
        row = existing
    else:
        row = _P(user_id=uid, name=name, payload=payload_str, is_default=is_def)
        _db.session.add(row)
    try:
        _db.session.commit()
    except Exception as e:
        _db.session.rollback()
        return jsonify({"error": "db", "message": str(e)}), 500
    return jsonify(_preset_to_dict(row))


@app.route("/api/scan-presets/<int:preset_id>", methods=["PUT", "PATCH"])
@login_required
def api_scan_presets_update(preset_id):
    uid = _current_user_id()
    if not uid:
        return jsonify({"error": "no_user"}), 401
    data = request.get_json(force=True) or {}
    from models import db as _db, ScanPreset as _P
    row = _P.query.filter_by(id=preset_id, user_id=uid).first()
    if not row:
        return jsonify({"error": "not_found"}), 404
    if "name" in data:
        n = (data.get("name") or "").strip()[:80]
        if n:
            row.name = n
    if "payload" in data:
        try:
            row.payload = json.dumps(data.get("payload") or {})
        except Exception:
            return jsonify({"error": "bad_payload"}), 400
    if "isDefault" in data:
        v = bool(data["isDefault"])
        if v:
            _P.query.filter_by(user_id=uid, is_default=True).update({"is_default": False})
        row.is_default = v
    try:
        _db.session.commit()
    except Exception as e:
        _db.session.rollback()
        return jsonify({"error": "db", "message": str(e)}), 500
    return jsonify(_preset_to_dict(row))


@app.route("/api/scan-presets/<int:preset_id>", methods=["DELETE"])
@login_required
def api_scan_presets_delete(preset_id):
    uid = _current_user_id()
    if not uid:
        return jsonify({"error": "no_user"}), 401
    from models import db as _db, ScanPreset as _P
    row = _P.query.filter_by(id=preset_id, user_id=uid).first()
    if not row:
        return jsonify({"ok": True})
    _db.session.delete(row)
    try:
        _db.session.commit()
    except Exception as e:
        _db.session.rollback()
        return jsonify({"error": "db", "message": str(e)}), 500
    return jsonify({"ok": True, "id": preset_id})


# ─── User preferences (Queue 16) ────────────────────────────────────────────
@app.route("/api/user-preferences", methods=["GET"])
@login_required
def api_user_prefs_get():
    uid = _current_user_id()
    out = {"desktopTutorialNeverShow": False,
           "desktopTutorialCompletedAt": None, "desktopTutorialSkippedAt": None}
    if not uid:
        return jsonify(out)
    from models import db as _db, UserPreference as _UP
    row = _UP.query.filter_by(user_id=uid).first()
    if not row:
        return jsonify(out)
    out["desktopTutorialNeverShow"] = bool(row.desktop_tutorial_never_show)
    out["desktopTutorialCompletedAt"] = row.desktop_tutorial_completed_at.isoformat() if row.desktop_tutorial_completed_at else None
    out["desktopTutorialSkippedAt"]   = row.desktop_tutorial_skipped_at.isoformat()   if row.desktop_tutorial_skipped_at   else None
    return jsonify(out)


@app.route("/api/user-preferences", methods=["POST", "PUT", "PATCH"])
@login_required
def api_user_prefs_set():
    uid = _current_user_id()
    if not uid:
        return jsonify({"error": "no_user"}), 401
    data = request.get_json(force=True) or {}
    from models import db as _db, UserPreference as _UP
    row = _UP.query.filter_by(user_id=uid).first()
    if not row:
        row = _UP(user_id=uid)
        _db.session.add(row)
    now = datetime.now(timezone.utc)
    if "desktopTutorialNeverShow" in data:
        row.desktop_tutorial_never_show = bool(data["desktopTutorialNeverShow"])
    if data.get("markCompleted"):
        row.desktop_tutorial_completed_at = now
    if data.get("markSkipped"):
        row.desktop_tutorial_skipped_at = now
    try:
        _db.session.commit()
    except Exception as e:
        _db.session.rollback()
        return jsonify({"error": "db", "message": str(e)}), 500
    return jsonify({
        "desktopTutorialNeverShow": bool(row.desktop_tutorial_never_show),
        "desktopTutorialCompletedAt": row.desktop_tutorial_completed_at.isoformat() if row.desktop_tutorial_completed_at else None,
        "desktopTutorialSkippedAt":   row.desktop_tutorial_skipped_at.isoformat()   if row.desktop_tutorial_skipped_at   else None,
    })


@app.route("/api/watchlist/cache")
@login_required
def api_watchlist_cache():
    """
    Browser polls this every 5s to get latest cached orderflow.
    Returns only pairs registered by the current user.
    """
    username = session.get("username", "default")
    with _wl_lock:
        user_syms = set(_wl_user_pairs.get(username, []))
        return jsonify({k: v for k, v in _wl_cache.items() if k in user_syms})


@app.route("/api/watchlist/status")
@login_required
def api_watchlist_status():
    """Health check for streaming thread — scoped to current user's pairs."""
    username = session.get("username", "default")
    with _wl_lock:
        user_syms = set(_wl_user_pairs.get(username, []))
        return jsonify({
            "running":  _wl_thread is not None and _wl_thread.is_alive(),
            "pairs":    [p for p in _wl_pairs if p in user_syms],
            "cached":   [k for k in _wl_cache.keys() if k in user_syms],
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
    # Phase 1B: backend OB touch-state filters (no UI yet)
    "useObTouchState": False, "obTouchState": "all", "obMaxTouches": 99,
    "useObVirginApproach": False, "obVirginApproachPct": 1.5,
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


def _scan_pair_multitf(symbol: str, market: str = "perpetual", wl_config: Optional[Dict] = None, exchange: str = "binance") -> Dict[str, Any]:
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
    scan_ob      = cfg.get("scan_ob", True)
    scan_fvg     = cfg.get("scan_fvg", True)
    scan_fib     = cfg.get("scan_fib", True)
    scan_breaker = cfg.get("scan_breaker", False)
    brk_approach = cfg.get("breaker_approach_pct", 2.0)
    brk_max_age  = cfg.get("breaker_max_age", 200)
    brk_req_fvg  = cfg.get("breaker_require_fvg", False)
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
        "bias_1d":   None,
        "tfs":       {},
        "obs":       [],
        "fvgs":      [],
        "fibs":      [],
        "breakers":  [],
        "structure": {},
        "rsi":       {},
        "atr":       {},
        "error":     None,
    }

    # ── 1D bias (direction only, no zones) ──
    if do_bias_1d:
        try:
            candles_1d = get_klines_exchange(symbol, "1d", 100, market, exchange)
            if candles_1d and len(candles_1d) >= 80:
                o_ = [x["open"] for x in candles_1d]
                h_ = [x["high"] for x in candles_1d]
                l_ = [x["low"]  for x in candles_1d]
                c_ = [x["close"] for x in candles_1d]
                itrend_1d, trend_1d = detect_structure(h_, l_, c_, 7, 20)
                result["bias_1d"] = trend_1d if trend_1d != 0 else itrend_1d
        except Exception:
            pass

    scan_limit = _scan_kline_limit()
    for tf in tfs:
        try:
            candles = get_klines_exchange(symbol, tf, scan_limit, market, exchange)
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
            settings["tf"]              = tf
            settings["obDistancePct"]   = ob_appr.get(tf, 2.0)
            settings["approachPct"]     = ob_appr.get(tf, 2.0)
            settings["useBreakerModule"]   = scan_breaker
            settings["breakerApproachPct"] = brk_approach
            settings["breakerMaxAge"]      = brk_max_age
            settings["breakerRequireFvg"]  = brk_req_fvg

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
                raw_obs, _ = detect_obs(o, h, l, c, v, iLen, sLen, max_ob=5)
                ob_approach_pct = ob_appr.get(tf, 2.0)
                fvgs_for_quality = detect_fvgs(o, h, l, c, v, tf)
                itrend_q, trend_q = detect_structure(h, l, c, iLen, sLen)

                for ob in raw_obs:
                    # Phase 1B: backend touch-state filter (no-op when disabled)
                    if not filter_ob(ob, price, settings):
                        continue
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
                        "strength":     round(ob.get("tvObVolumeSharePct") or 0, 1),
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
                            f'Order Block %: {ob.get("tvObVolumeSharePct") or "—"} | '
                            f'Zone: {fmt_price(zb)} – {fmt_price(zt)}'
                        ),
                    })

            # ── Breaker Block detection (direct, same as scan page) ──
            tf_breakers = []
            if scan_breaker:
                brk_fvgs = detect_fvgs(o, h, l, c, v, tf)
                raw_breakers = detect_breakers(
                    o, h, l, c, v, price, tf,
                    i_len=settings["iLen"],
                    s_len=settings["sLen"],
                    approach_pct=brk_approach,
                    max_age=brk_max_age,
                    fvgs=brk_fvgs,
                )
                for brk in raw_breakers:
                    if brk_req_fvg and not brk["fvg_overlap"]:
                        continue
                    high_prob = brk["fvg_overlap"]
                    tf_breakers.append({
                        "tf":        tf,
                        "direction": brk["type"],
                        "setup":     "BREAKER_INSIDE" if brk["state"] == "inside" else "BREAKER_APPROACH",
                        "top":       brk["top"],
                        "bottom":    brk["bottom"],
                        "dist":      brk["dist"],
                        "state":     brk["state"],
                        "age":       brk["age"],
                        "strength":  brk["strength"],
                        "fvgOverlap": high_prob,
                        "highProb":  high_prob,
                        "zoneStr":   brk["zone_str"],
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
                "breakers":   tf_breakers,
                "candleLimitUsed": scan_limit,
                "candlesCount":    len(candles),
            }

            result["obs"].extend(tf_obs)
            result["fvgs"].extend(tf_fvgs)
            result["fibs"].extend(tf_fibs)
            result["tfs"][tf]["breakers"] = tf_breakers
            result["breakers"].extend(tf_breakers)
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
    _tok_user, _tok_uid = _check_and_get_token_user()
    if _tok_user == "limit":
        return _daily_limit_response()
    data      = request.get_json(force=True) or {}
    pairs     = [str(p).strip().upper() for p in data.get("pairs", []) if str(p).strip()]
    pairs     = [p for p in pairs if p.endswith("USDT")][:30]
    market    = data.get("market", "perpetual")
    exchange  = data.get("exchange", "binance").lower()
    wl_config = data.get("config", {})

    if not pairs:
        return jsonify({"results": [], "error": "no pairs"})

    results = []
    workers = min(5, len(pairs))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_scan_pair_multitf, sym, market, wl_config, exchange): sym for sym in pairs}
        for fut in as_completed(futures):
            try:
                results.append(fut.result())
            except Exception as e:
                print(f"[WL-REFRESH] Error: {e}")

    results.sort(
        key=lambda x: x.get("tfs", {}).get("1h", {}).get("score", 0),
        reverse=True
    )
    if _tok_uid:
        try: consume_tokens(_tok_uid, len(pairs))
        except Exception as _te: print(f"[Tokens] watchlist: {_te}")
    return jsonify({"results": results, "scanned": len(pairs)})


# ============================================================
# /api/live-monitor — Phase 1 Live Monitor (DB-backed, per-user)
# Separate from /api/watchlist/* — old watchlist logic untouched.
# ============================================================

@app.route("/api/live-monitor/items", methods=["GET"])
@login_required
def api_lm_items_get():
    """Return all Live Monitor items for the current user, newest first."""
    uid, _ = _current_user_id_and_user()
    if not uid:
        return jsonify({"items": [], "count": 0})
    from models import db as _db, LiveMonitorItem as _LMI
    q = _LMI.query.filter_by(user_id=uid)
    if request.args.get("active_only") == "1":
        q = q.filter_by(is_active=True)
    rows = q.order_by(_LMI.added_at.desc()).all()
    items = [_live_monitor_item_to_dict(r) for r in rows]
    return jsonify({"items": items, "count": len(items)})


@app.route("/api/live-monitor/items", methods=["POST"])
@login_required
def api_lm_items_post():
    """Save a full setup snapshot to Live Monitor. Creates or updates if duplicate zone."""
    uid, _ = _current_user_id_and_user()
    if not uid:
        return jsonify({"error": "no_user"}), 401

    data = request.get_json(force=True) or {}

    symbol = (str(data.get("symbol") or "")).strip().upper()
    if not symbol:
        return jsonify({"error": "symbol_required"}), 400
    if not symbol.endswith("USDT"):
        return jsonify({"error": "invalid_symbol",
                        "message": "Only USDT pairs are supported for Live Monitor right now."}), 400

    exchange    = (str(data.get("exchange") or "binance")).strip().lower()
    market      = (str(data.get("market")   or "perpetual")).strip().lower()
    source_tab  = (str(data.get("source_tab") or "unknown")).strip()[:40]
    setup_type  = (str(data.get("setup_type") or "")).strip()[:40] or None
    direction   = (str(data.get("direction")  or "")).strip()[:10]  or None
    timeframe   = (str(data.get("timeframe")  or "")).strip()[:10]  or None

    # Safe numeric parsing — never crashes on bad/empty/null frontend values
    zone_high     = _lm_float_or_none(data.get("zone_high"))
    zone_low      = _lm_float_or_none(data.get("zone_low"))
    confidence    = _lm_int_or_zero(data.get("confidence") or data.get("score"))
    score         = _lm_int_or_zero(data.get("score"))
    current_price = _lm_float_or_none(data.get("current_price") or data.get("price"))

    # Build the snapshot blob — include topAlert/meta if provided
    snap_src = data.get("snapshot") or {}
    if not isinstance(snap_src, dict):
        snap_src = {}
    if data.get("topAlert"):
        snap_src["topAlert"] = data["topAlert"]
    if data.get("meta"):
        snap_src["meta"] = data["meta"]
    snap_src["addedFrom"] = source_tab
    snapshot_json = _json_dumps_safe(snap_src)

    sel_tf  = data.get("selected_timeframes") or ["15m", "30m", "1h", "4h", "1d"]
    sel_mod = data.get("selected_modules")    or ["OB", "FVG", "FIB", "Breaker", "Bias"]
    alert_s = data.get("alert_settings")      or {}

    from models import db as _db, LiveMonitorItem as _LMI, LiveMonitorEvent as _LME

    # Duplicate check: same user + symbol + setup_type + timeframe + zone (if present)
    created = True
    existing = None
    if setup_type and timeframe and zone_high is not None and zone_low is not None:
        existing = _LMI.query.filter_by(
            user_id=uid, symbol=symbol, setup_type=setup_type,
            timeframe=timeframe, is_active=True
        ).filter(
            _LMI.zone_high == zone_high,
            _LMI.zone_low  == zone_low
        ).first()
    if not existing and setup_type and timeframe:
        existing = _LMI.query.filter_by(
            user_id=uid, symbol=symbol, setup_type=setup_type,
            timeframe=timeframe, is_active=True
        ).first()

    if existing:
        existing.exchange            = exchange
        existing.market              = market
        existing.source_tab          = source_tab
        existing.direction           = direction
        existing.zone_high           = zone_high if zone_high is not None else existing.zone_high
        existing.zone_low            = zone_low  if zone_low  is not None else existing.zone_low
        existing.confidence          = confidence
        existing.score               = score
        existing.current_price       = current_price if current_price is not None else existing.current_price
        existing.snapshot_json       = snapshot_json
        existing.selected_timeframes = _json_dumps_safe(sel_tf)
        existing.selected_modules    = _json_dumps_safe(sel_mod)
        existing.alert_settings_json = _json_dumps_safe(alert_s)
        existing.status              = "watching"
        row = existing
        created = False
    else:
        row = _LMI(
            user_id             = uid,
            symbol              = symbol,
            exchange            = exchange,
            market              = market,
            source_tab          = source_tab,
            setup_type          = setup_type,
            direction           = direction,
            timeframe           = timeframe,
            zone_high           = zone_high,
            zone_low            = zone_low,
            confidence          = confidence,
            score               = score,
            current_price       = current_price,
            status              = "watching",
            snapshot_json       = snapshot_json,
            selected_timeframes = _json_dumps_safe(sel_tf),
            selected_modules    = _json_dumps_safe(sel_mod),
            alert_settings_json = _json_dumps_safe(alert_s),
            is_active           = True,
        )
        _db.session.add(row)

    try:
        _db.session.flush()   # get row.id before event insert
        ev = _LME(
            item_id           = row.id,
            user_id           = uid,
            symbol            = symbol,
            event_type        = "added" if created else "updated",
            event_description = "Added to Live Monitor" if created else "Updated in Live Monitor",
            details_json      = _json_dumps_safe({
                "source_tab": source_tab,
                "setup_type": setup_type,
                "timeframe":  timeframe,
            }),
            price_at_event    = current_price,
        )
        _db.session.add(ev)
        _db.session.commit()
    except Exception as e:
        _db.session.rollback()
        return jsonify({"error": "db", "message": str(e)}), 500

    return jsonify({"ok": True, "item": _live_monitor_item_to_dict(row), "created": created})


@app.route("/api/live-monitor/items/<int:item_id>", methods=["DELETE"])
@login_required
def api_lm_items_delete(item_id):
    """Soft-delete a Live Monitor item (sets is_active=False, status='removed')."""
    uid, _ = _current_user_id_and_user()
    if not uid:
        return jsonify({"error": "no_user"}), 401

    from models import db as _db, LiveMonitorItem as _LMI, LiveMonitorEvent as _LME
    row = _LMI.query.filter_by(id=item_id).first()
    if not row:
        return jsonify({"ok": True, "deleted": item_id})
    if row.user_id != uid:
        return jsonify({"error": "forbidden"}), 403

    row.is_active = False
    row.status    = "removed"
    ev = _LME(
        item_id           = row.id,
        user_id           = uid,
        symbol            = row.symbol,
        event_type        = "removed",
        event_description = "Removed from Live Monitor",
        details_json      = _json_dumps_safe({"item_id": item_id}),
    )
    _db.session.add(ev)
    try:
        _db.session.commit()
    except Exception as e:
        _db.session.rollback()
        return jsonify({"error": "db", "message": str(e)}), 500

    return jsonify({"ok": True, "deleted": item_id})


@app.route("/api/live-monitor/events/<int:item_id>", methods=["GET"])
@login_required
def api_lm_events_get(item_id):
    """Return events for a Live Monitor item (ownership verified)."""
    uid, _ = _current_user_id_and_user()
    if not uid:
        return jsonify({"events": [], "count": 0})

    from models import LiveMonitorItem as _LMI, LiveMonitorEvent as _LME
    row = _LMI.query.filter_by(id=item_id).first()
    if not row:
        return jsonify({"error": "not_found"}), 404
    if row.user_id != uid:
        return jsonify({"error": "forbidden"}), 403

    evs = (_LME.query.filter_by(item_id=item_id)
                     .order_by(_LME.created_at.desc()).all())
    return jsonify({"events": [_live_monitor_event_to_dict(e) for e in evs], "count": len(evs)})


@app.route("/api/live-monitor/items/<int:item_id>/refresh", methods=["POST"])
@login_required
def api_lm_items_refresh(item_id):
    """Run a Phase 3 MTF scan for one Live Monitor item and store the result in snapshot_json."""
    uid, _ = _current_user_id_and_user()
    if not uid:
        return jsonify({"error": "no_user"}), 401

    from models import db as _db, LiveMonitorItem as _LMI, LiveMonitorEvent as _LME
    row = _LMI.query.filter_by(id=item_id).first()
    if not row:
        return jsonify({"error": "not_found"}), 404
    if row.user_id != uid:
        return jsonify({"error": "forbidden"}), 403
    if not row.is_active:
        return jsonify({"error": "inactive", "message": "Item is no longer active."}), 400

    tfs  = _lm_allowed_timeframes(row.selected_timeframes)
    mods = _lm_allowed_modules(row.selected_modules)
    cfg  = _lm_build_scan_config(row)

    try:
        scan_result = _scan_pair_multitf(
            row.symbol,
            row.market    or "perpetual",
            cfg,
            row.exchange  or "binance",
        )
    except Exception as e:
        return jsonify({"error": "scan_failed", "message": str(e)}), 500

    mtf_summary = _lm_extract_mtf_summary(scan_result, tfs, mods,
                                          exchange=row.exchange, market=row.market)

    # Merge into existing snapshot_json, preserving old fields
    snap = _json_loads_safe(row.snapshot_json, {})
    snap["latest_mtf_scan"]     = mtf_summary
    snap["last_mtf_refresh_at"] = mtf_summary["refreshed_at"]
    if "timeframe_policy" not in snap:
        snap["timeframe_policy"] = {
            "visible_analysis_timeframes": ["15m", "30m", "1h", "4h", "1d"],
            "bias_min_timeframe":          "1h",
            "hidden_execution_timeframe":  "5m",
            "source_exchange":             row.exchange or "binance",
            "source_market":               row.market   or "perpetual",
        }
    else:
        tp = snap["timeframe_policy"]
        if "source_exchange" not in tp:
            tp["source_exchange"] = row.exchange or "binance"
        if "source_market" not in tp:
            tp["source_market"] = row.market or "perpetual"

    new_price = scan_result.get("price")
    if new_price and new_price > 0:
        row.current_price = new_price

    # Phase 4: preserve original setup score before health overwrites row.score
    if "original_setup_score" not in snap:
        snap["original_setup_score"] = _lm_original_setup_score(row, snap)

    # Phase 4.5: session context (no score impact)
    session_ctx = _lm_session_context()
    snap["latest_session_context"]  = session_ctx
    snap["last_session_context_at"] = session_ctx["computed_at"]

    # Phase 4: compute health with fresh snap (latest_mtf_scan already merged)
    health = _lm_compute_health(row, _snap=snap)
    health["session_context"] = session_ctx
    snap["latest_health"] = health

    row.snapshot_json = _json_dumps_safe(snap)
    row.updated_at    = datetime.now(timezone.utc)

    prev_status = row.status
    row.status  = health["status"]
    row.score   = health["health_score"]

    ev = _LME(
        item_id           = row.id,
        user_id           = uid,
        symbol            = row.symbol,
        event_type        = "mtf_refreshed",
        event_description = "MTF scan refreshed",
        details_json      = _json_dumps_safe({
            "timeframes":   tfs,
            "modules":      mods,
            "phase":        "phase3_mtf_scan",
            "health_score": health["health_score"],
            "grade":        health["grade"],
        }),
        price_at_event        = new_price if (new_price and new_price > 0) else row.current_price,
        health_score_at_event = health["health_score"],
    )
    _db.session.add(ev)

    if prev_status != health["status"]:
        ev_sc = _LME(
            item_id           = row.id,
            user_id           = uid,
            symbol            = row.symbol,
            event_type        = "status_changed",
            event_description = f"Status changed: {prev_status} → {health['status']} (score {health['health_score']})",
            details_json      = _json_dumps_safe({
                "prev_status":  prev_status,
                "new_status":   health["status"],
                "health_score": health["health_score"],
                "grade":        health["grade"],
            }),
            price_at_event        = new_price if (new_price and new_price > 0) else row.current_price,
            health_score_at_event = health["health_score"],
        )
        _db.session.add(ev_sc)

    try:
        _db.session.commit()
    except Exception as e:
        _db.session.rollback()
        return jsonify({"error": "db", "message": str(e)}), 500

    return jsonify({
        "ok":             True,
        "item":           _live_monitor_item_to_dict(row),
        "latest_mtf_scan": mtf_summary,
        "latest_health":  health,
    })


@app.route("/api/live-monitor/refresh-all", methods=["POST"])
@login_required
def api_lm_refresh_all():
    """Refresh MTF scans for all active Live Monitor items for the current user (max 10)."""
    uid, _ = _current_user_id_and_user()
    if not uid:
        return jsonify({"error": "no_user"}), 401

    from models import db as _db, LiveMonitorItem as _LMI, LiveMonitorEvent as _LME

    _MAX_ITEMS = 10
    rows = (_LMI.query
                .filter_by(user_id=uid, is_active=True)
                .order_by(_LMI.added_at.desc())
                .limit(_MAX_ITEMS)
                .all())

    if not rows:
        return jsonify({"ok": True, "updated": 0, "failed": 0, "items": [], "errors": []})

    updated, failed, errors = 0, 0, []
    now = datetime.now(timezone.utc)

    for row in rows:
        try:
            tfs  = _lm_allowed_timeframes(row.selected_timeframes)
            mods = _lm_allowed_modules(row.selected_modules)
            cfg  = _lm_build_scan_config(row)
            scan_result  = _scan_pair_multitf(
                row.symbol,
                row.market   or "perpetual",
                cfg,
                row.exchange or "binance",
            )
            mtf_summary = _lm_extract_mtf_summary(scan_result, tfs, mods,
                                                  exchange=row.exchange, market=row.market)

            snap = _json_loads_safe(row.snapshot_json, {})
            snap["latest_mtf_scan"]     = mtf_summary
            snap["last_mtf_refresh_at"] = mtf_summary["refreshed_at"]
            if "timeframe_policy" not in snap:
                snap["timeframe_policy"] = {
                    "visible_analysis_timeframes": ["15m", "30m", "1h", "4h", "1d"],
                    "bias_min_timeframe":          "1h",
                    "hidden_execution_timeframe":  "5m",
                    "source_exchange":             row.exchange or "binance",
                    "source_market":               row.market   or "perpetual",
                }
            else:
                tp = snap["timeframe_policy"]
                if "source_exchange" not in tp:
                    tp["source_exchange"] = row.exchange or "binance"
                if "source_market" not in tp:
                    tp["source_market"] = row.market or "perpetual"

            new_price = scan_result.get("price")
            if new_price and new_price > 0:
                row.current_price = new_price

            # Phase 4: preserve original setup score before health overwrites row.score
            if "original_setup_score" not in snap:
                snap["original_setup_score"] = _lm_original_setup_score(row, snap)

            # Phase 4.5: session context
            session_ctx = _lm_session_context()
            snap["latest_session_context"]  = session_ctx
            snap["last_session_context_at"] = session_ctx["computed_at"]

            # Phase 4: compute health with fresh snap
            health = _lm_compute_health(row, _snap=snap)
            health["session_context"] = session_ctx
            snap["latest_health"] = health

            row.snapshot_json = _json_dumps_safe(snap)
            row.updated_at    = now

            prev_status = row.status
            row.status  = health["status"]
            row.score   = health["health_score"]

            if prev_status != health["status"]:
                ev_sc = _LME(
                    item_id           = row.id,
                    user_id           = uid,
                    symbol            = row.symbol,
                    event_type        = "status_changed",
                    event_description = f"Status changed: {prev_status} → {health['status']} (score {health['health_score']})",
                    details_json      = _json_dumps_safe({
                        "prev_status":  prev_status,
                        "new_status":   health["status"],
                        "health_score": health["health_score"],
                        "grade":        health["grade"],
                    }),
                    price_at_event        = new_price if (new_price and new_price > 0) else row.current_price,
                    health_score_at_event = health["health_score"],
                )
                _db.session.add(ev_sc)

            updated += 1
        except Exception as e:
            failed += 1
            errors.append({"symbol": row.symbol, "error": str(e)})

    try:
        _db.session.commit()
    except Exception as e:
        _db.session.rollback()
        return jsonify({"error": "db", "message": str(e)}), 500

    refreshed_rows = (_LMI.query
                         .filter_by(user_id=uid, is_active=True)
                         .order_by(_LMI.added_at.desc())
                         .limit(_MAX_ITEMS)
                         .all())
    return jsonify({
        "ok":      True,
        "updated": updated,
        "failed":  failed,
        "items":   [_live_monitor_item_to_dict(r) for r in refreshed_rows],
        "errors":  errors,
    })


@app.route("/api/live-monitor/items/<int:item_id>/recalc-health", methods=["POST"])
@login_required
def api_lm_items_recalc_health(item_id):
    """Recompute health score from existing snapshot_json without running a new MTF scan."""
    uid, _ = _current_user_id_and_user()
    if not uid:
        return jsonify({"error": "no_user"}), 401

    from models import db as _db, LiveMonitorItem as _LMI, LiveMonitorEvent as _LME
    row = _LMI.query.filter_by(id=item_id).first()
    if not row:
        return jsonify({"error": "not_found"}), 404
    if row.user_id != uid:
        return jsonify({"error": "forbidden"}), 403
    if not row.is_active:
        return jsonify({"error": "inactive", "message": "Item is no longer active."}), 400

    snap = _json_loads_safe(row.snapshot_json, {})

    # Preserve original setup score before health overwrites row.score
    if "original_setup_score" not in snap:
        snap["original_setup_score"] = _lm_original_setup_score(row, snap)

    # Phase 4.5: session context
    session_ctx = _lm_session_context()
    snap["latest_session_context"]  = session_ctx
    snap["last_session_context_at"] = session_ctx["computed_at"]

    health = _lm_compute_health(row, _snap=snap)
    health["session_context"] = session_ctx
    snap["latest_health"] = health

    prev_status = row.status
    row.status  = health["status"]
    row.score   = health["health_score"]
    row.snapshot_json = _json_dumps_safe(snap)
    row.updated_at    = datetime.now(timezone.utc)

    if prev_status != health["status"]:
        ev_sc = _LME(
            item_id           = row.id,
            user_id           = uid,
            symbol            = row.symbol,
            event_type        = "status_changed",
            event_description = f"Status changed: {prev_status} → {health['status']} (score {health['health_score']})",
            details_json      = _json_dumps_safe({
                "prev_status":  prev_status,
                "new_status":   health["status"],
                "health_score": health["health_score"],
                "grade":        health["grade"],
                "source":       "recalc",
            }),
            price_at_event        = row.current_price,
            health_score_at_event = health["health_score"],
        )
        _db.session.add(ev_sc)

    try:
        _db.session.commit()
    except Exception as e:
        _db.session.rollback()
        return jsonify({"error": "db", "message": str(e)}), 500

    return jsonify({
        "ok":            True,
        "item":          _live_monitor_item_to_dict(row),
        "latest_health": health,
    })


@app.route("/api/live-monitor/session-context", methods=["GET"])
@login_required
def api_lm_session_context():
    """Return current UTC-based session context. No DB reads or exchange calls."""
    uid, _ = _current_user_id_and_user()
    if not uid:
        return jsonify({"error": "no_user"}), 401
    return jsonify({"ok": True, "session_context": _lm_session_context()})


@app.route("/api/live-monitor/items/<int:item_id>/refresh-session", methods=["POST"])
@login_required
def api_lm_items_refresh_session(item_id):
    """Update only the session context in snapshot_json — no MTF scan, no health recompute."""
    uid, _ = _current_user_id_and_user()
    if not uid:
        return jsonify({"error": "no_user"}), 401

    from models import db as _db, LiveMonitorItem as _LMI
    row = _LMI.query.filter_by(id=item_id).first()
    if not row:
        return jsonify({"error": "not_found"}), 404
    if row.user_id != uid:
        return jsonify({"error": "forbidden"}), 403
    if not row.is_active:
        return jsonify({"error": "inactive", "message": "Item is no longer active."}), 400

    session_ctx = _lm_session_context()
    snap = _json_loads_safe(row.snapshot_json, {})
    snap["latest_session_context"]  = session_ctx
    snap["last_session_context_at"] = session_ctx["computed_at"]
    row.snapshot_json = _json_dumps_safe(snap)
    row.updated_at    = datetime.now(timezone.utc)

    try:
        _db.session.commit()
    except Exception as e:
        _db.session.rollback()
        return jsonify({"error": "db", "message": str(e)}), 500

    return jsonify({
        "ok":             True,
        "item":           _live_monitor_item_to_dict(row),
        "session_context": session_ctx,
    })


@app.route("/api/live-monitor/items/<int:item_id>/refresh-market-context", methods=["POST"])
@login_required
def api_lm_items_refresh_market_context(item_id):
    """Fetch and store market context for one Live Monitor item. No MTF scan, no health recompute."""
    uid, _ = _current_user_id_and_user()
    if not uid:
        return jsonify({"error": "no_user"}), 401

    from models import db as _db, LiveMonitorItem as _LMI
    row = _LMI.query.filter_by(id=item_id).first()
    if not row:
        return jsonify({"error": "not_found"}), 404
    if row.user_id != uid:
        return jsonify({"error": "forbidden"}), 403
    if not row.is_active:
        return jsonify({"error": "inactive", "message": "Item is no longer active."}), 400

    snap = _json_loads_safe(row.snapshot_json, {})
    snap, market_ctx = _lm_attach_market_context(row, snapshot=snap)
    row.snapshot_json = _json_dumps_safe(snap)
    row.updated_at    = datetime.now(timezone.utc)

    try:
        _db.session.commit()
    except Exception as e:
        _db.session.rollback()
        return jsonify({"error": "db", "message": str(e)}), 500

    return jsonify({
        "ok":             True,
        "item":           _live_monitor_item_to_dict(row),
        "market_context": market_ctx,
    })


@app.route("/api/live-monitor/refresh-market-context-all", methods=["POST"])
@login_required
def api_lm_refresh_market_context_all():
    """Fetch and store market context for all active Live Monitor items (max 10). No MTF scan."""
    uid, _ = _current_user_id_and_user()
    if not uid:
        return jsonify({"error": "no_user"}), 401

    from models import db as _db, LiveMonitorItem as _LMI
    _MAX_ITEMS = 10
    rows = (_LMI.query
                .filter_by(user_id=uid, is_active=True)
                .order_by(_LMI.added_at.desc())
                .limit(_MAX_ITEMS)
                .all())

    if not rows:
        return jsonify({"ok": True, "updated": 0, "failed": 0, "items": [], "errors": []})

    updated, failed, errors, result_items = 0, 0, [], []
    now = datetime.now(timezone.utc)

    for row in rows:
        try:
            snap = _json_loads_safe(row.snapshot_json, {})
            snap, _ = _lm_attach_market_context(row, snapshot=snap)
            row.snapshot_json = _json_dumps_safe(snap)
            row.updated_at    = now
            updated += 1
        except Exception as e:
            failed += 1
            errors.append({"id": row.id, "symbol": row.symbol, "error": str(e)})

    try:
        _db.session.commit()
    except Exception as e:
        _db.session.rollback()
        return jsonify({"error": "db", "message": str(e)}), 500

    for row in rows:
        try:
            result_items.append(_live_monitor_item_to_dict(row))
        except Exception:
            pass

    return jsonify({
        "ok":      True,
        "updated": updated,
        "failed":  failed,
        "items":   result_items,
        "errors":  errors,
    })


@app.route("/api/live-monitor/ai-providers", methods=["GET"])
@login_required
def api_lm_ai_providers():
    """Return safe list of configured AI providers (no API keys)."""
    uid, _ = _current_user_id_and_user()
    if not uid:
        return jsonify({"error": "no_user"}), 401
    agents = _lm_ai_agents_config()
    safe = []
    for a in agents:
        safe.append({
            "id":         a.get("id"),
            "label":      a.get("label"),
            "provider":   a.get("provider"),
            "model":      a.get("model", ""),
            "configured": a.get("configured", False),
            "has_key":    a.get("has_key", False),
            "enabled":    a.get("enabled", True),
        })
    return jsonify({"ok": True, "agents": safe, "count": len(safe)})


@app.route("/api/live-monitor/items/<int:item_id>/ai-analyze", methods=["POST"])
@login_required
def api_lm_items_ai_analyze(item_id):
    """Run AI analysis on one Live Monitor item. Stores result in snapshot_json."""
    uid, _ = _current_user_id_and_user()
    if not uid:
        return jsonify({"error": "no_user"}), 401

    from models import db as _db, LiveMonitorItem as _LMI, LiveMonitorEvent as _LME
    row = _LMI.query.filter_by(id=item_id).first()
    if not row:
        return jsonify({"error": "not_found"}), 404
    if row.user_id != uid:
        return jsonify({"error": "forbidden"}), 403
    if not row.is_active:
        return jsonify({"error": "inactive", "message": "Item is no longer active."}), 400

    body         = request.get_json(silent=True) or {}
    user_message = (body.get("message") or "")[:1000]
    force        = bool(body.get("force", False))
    agent_id     = (body.get("agent_id") or "").strip() or None
    mode         = (body.get("mode") or "single").strip()
    if mode not in ("single", "all"):
        mode = "single"

    # Resolve the actual agent that will be used (so cooldown is agent-specific)
    resolved_agent    = _lm_get_ai_agent_config(agent_id)
    resolved_agent_id = resolved_agent.get("id", "local_fallback")

    snap = _json_loads_safe(row.snapshot_json, {})

    # Cooldown: return cached only if same resolved agent was used within 10s
    if not force and snap.get("latest_ai_analysis"):
        cached = snap["latest_ai_analysis"]
        last_at         = cached.get("computed_at")
        cached_agent_id = cached.get("agent_id", "")
        if last_at and cached_agent_id == resolved_agent_id:
            try:
                from datetime import datetime, timezone as _tz
                diff = (datetime.now(_tz.utc) - datetime.fromisoformat(last_at)).total_seconds()
                if diff < 10:
                    return jsonify({
                        "ok":        True,
                        "item":      _live_monitor_item_to_dict(row),
                        "ai_result": cached,
                        "cached":    True,
                    })
            except Exception:
                pass

    # Auto-refresh event detection before building AI context (no timeline logging)
    snap, _ = _lm_attach_event_detection(row, snapshot=snap)
    row.snapshot_json = _json_dumps_safe(snap)

    context     = _lm_build_ai_context(row)
    ai_result   = _lm_call_ai_provider(context, user_message or None, agent_id=resolved_agent_id)
    computed_at = datetime.now(timezone.utc).isoformat()

    latest_ai = {
        "phase":       "phase6_ai_agent",
        "computed_at": computed_at,
        "agent_id":    ai_result.get("agent_id", ""),
        "agent_label": ai_result.get("agent_label", ""),
        "provider":    ai_result.get("provider", "local_fallback"),
        "model":       ai_result.get("model", ""),
        "configured":  ai_result.get("configured", False),
        "ok":          ai_result.get("ok", False),
        "analysis":    ai_result.get("analysis", {}),
        "source_context_at": {
            "latest_mtf":     snap.get("last_mtf_refresh_at"),
            "latest_health":  snap.get("last_health_at"),
            "latest_session": snap.get("last_session_context_at"),
            "latest_market":  snap.get("last_market_context_at"),
            "latest_events":  snap.get("last_event_detection_at"),
        },
    }

    snap["latest_ai_analysis"]  = latest_ai
    snap["last_ai_analysis_at"] = computed_at
    row.snapshot_json = _json_dumps_safe(snap)
    row.updated_at    = datetime.now(timezone.utc)

    analysis  = ai_result.get("analysis", {})
    ev = _LME(
        item_id           = row.id,
        user_id           = uid,
        symbol            = row.symbol,
        event_type        = "ai_analysis",
        event_description = "AI analysis generated",
        details_json      = _json_dumps_safe({
            "verdict":    analysis.get("verdict"),
            "confidence": analysis.get("confidence"),
            "provider":   ai_result.get("provider"),
            "agent_id":   ai_result.get("agent_id"),
            "configured": ai_result.get("configured"),
        }),
        health_score_at_event = row.score,
        price_at_event        = row.current_price,
    )
    _db.session.add(ev)

    try:
        _db.session.commit()
    except Exception as e:
        _db.session.rollback()
        return jsonify({"error": "db", "message": str(e)}), 500

    return jsonify({
        "ok":        True,
        "item":      _live_monitor_item_to_dict(row),
        "ai_result": latest_ai,
    })


@app.route("/api/live-monitor/items/<int:item_id>/ai-consensus", methods=["POST"])
@login_required
def api_lm_items_ai_consensus(item_id):
    """Run all configured AI agents and return a consensus verdict. Stores in snapshot_json."""
    uid, _ = _current_user_id_and_user()
    if not uid:
        return jsonify({"error": "no_user"}), 401

    from models import db as _db, LiveMonitorItem as _LMI, LiveMonitorEvent as _LME
    row = _LMI.query.filter_by(id=item_id).first()
    if not row:
        return jsonify({"error": "not_found"}), 404
    if row.user_id != uid:
        return jsonify({"error": "forbidden"}), 403
    if not row.is_active:
        return jsonify({"error": "inactive", "message": "Item is no longer active."}), 400

    body              = request.get_json(silent=True) or {}
    user_message      = (body.get("message") or "")[:1000]
    force             = bool(body.get("force", False))
    requested_ids_raw = body.get("agent_ids") or []
    if not isinstance(requested_ids_raw, list):
        requested_ids_raw = []
    # sanitise: string ids only, strip whitespace
    requested_agent_ids = [str(x).strip() for x in requested_ids_raw if x]

    snap = _json_loads_safe(row.snapshot_json, {})

    # 15s cooldown for consensus (more expensive than single-agent)
    if not force and snap.get("latest_ai_consensus"):
        last_at = snap["latest_ai_consensus"].get("computed_at")
        if last_at:
            try:
                from datetime import datetime, timezone as _tz
                diff = (datetime.now(_tz.utc) - datetime.fromisoformat(last_at)).total_seconds()
                if diff < 15:
                    return jsonify({
                        "ok":       True,
                        "item":     _live_monitor_item_to_dict(row),
                        "consensus": snap["latest_ai_consensus"],
                        "cached":   True,
                    })
            except Exception:
                pass

    # Auto-refresh event detection before building AI context (no timeline logging)
    snap, _ = _lm_attach_event_detection(row, snapshot=snap)
    row.snapshot_json = _json_dumps_safe(snap)

    context    = _lm_build_ai_context(row)
    all_agents = _lm_ai_agents_config()

    # Build candidate list: filter to requested ids if provided
    if requested_agent_ids:
        id_set = set(requested_agent_ids)
        candidates = [
            a for a in all_agents
            if a.get("id") in id_set
            and a.get("id") != "local_fallback"
            and a.get("configured", False)
            and a.get("enabled", True)
        ]
    else:
        candidates = [
            a for a in all_agents
            if a.get("id") != "local_fallback"
            and a.get("configured", False)
            and a.get("enabled", True)
        ]

    # Enforce max 4 agents per request
    candidates = candidates[:4]

    results         = []
    used_agent_ids  = []
    for agent in candidates:
        r = _lm_call_ai_provider(context, user_message or None, agent_id=agent["id"])
        results.append(r)
        used_agent_ids.append(agent["id"])

    # If no configured agents produced any ok result, use local_fallback as backstop
    if not any(r.get("ok") for r in results):
        fb_result = {
            "agent_id":    "local_fallback",
            "agent_label": "Local Fallback",
            "provider":    "local_fallback",
            "model":       "",
            "configured":  False,
            "ok":          True,
            "analysis":    _lm_local_ai_fallback(context, user_message or None),
        }
        results.append(fb_result)
        used_agent_ids.append("local_fallback")

    consensus   = _lm_consensus_from_agent_results(results)
    computed_at = datetime.now(timezone.utc).isoformat()

    latest_consensus = {
        "phase":               "phase65_consensus",
        "computed_at":         computed_at,
        "requested_agent_ids": requested_agent_ids,
        "used_agent_ids":      used_agent_ids,
        "agent_count":         consensus["agent_count"],
        "ok_count":            consensus["ok_count"],
        "agreement":           consensus["agreement"],
        "disagreement":        consensus.get("disagreement", False),
        "disagreement_note":   consensus.get("disagreement_note", ""),
        "verdict":             consensus["verdict"],
        "confidence":          consensus["confidence"],
        "summary":             consensus["summary"],
        "agents":              consensus["agents"],
        "analysis":            consensus["analysis"],
        "source_context_at": {
            "latest_mtf":     snap.get("last_mtf_refresh_at"),
            "latest_health":  snap.get("last_health_at"),
            "latest_session": snap.get("last_session_context_at"),
            "latest_market":  snap.get("last_market_context_at"),
            "latest_events":  snap.get("last_event_detection_at"),
        },
    }

    snap["latest_ai_consensus"]  = latest_consensus
    snap["last_ai_consensus_at"] = computed_at
    row.snapshot_json = _json_dumps_safe(snap)
    row.updated_at    = datetime.now(timezone.utc)

    ev = _LME(
        item_id           = row.id,
        user_id           = uid,
        symbol            = row.symbol,
        event_type        = "ai_consensus",
        event_description = "AI consensus analysis generated",
        details_json      = _json_dumps_safe({
            "verdict":             consensus["verdict"],
            "confidence":          consensus["confidence"],
            "agent_count":         consensus["agent_count"],
            "ok_count":            consensus["ok_count"],
            "agreement":           consensus["agreement"],
            "disagreement":        consensus.get("disagreement", False),
            "used_agent_ids":      used_agent_ids,
            "requested_agent_ids": requested_agent_ids,
        }),
        health_score_at_event = row.score,
        price_at_event        = row.current_price,
    )
    _db.session.add(ev)

    try:
        _db.session.commit()
    except Exception as e:
        _db.session.rollback()
        return jsonify({"error": "db", "message": str(e)}), 500

    return jsonify({
        "ok":       True,
        "item":     _live_monitor_item_to_dict(row),
        "consensus": latest_consensus,
    })


# ── Phase 6.6: Custom AI Instruction Endpoints ───────────────────────────────

@app.route("/api/live-monitor/items/<int:item_id>/ai-instructions", methods=["GET"])
@login_required
def api_lm_items_ai_instructions_get(item_id):
    """Return active custom AI instructions for one Live Monitor item."""
    uid, _ = _current_user_id_and_user()
    if not uid:
        return jsonify({"error": "no_user"}), 401

    from models import LiveMonitorItem as _LMI
    row = _LMI.query.filter_by(id=item_id).first()
    if not row:
        return jsonify({"error": "not_found"}), 404
    if row.user_id != uid:
        return jsonify({"error": "forbidden"}), 403
    if not row.is_active:
        return jsonify({"error": "inactive"}), 400

    snap = _json_loads_safe(row.snapshot_json, {})
    return jsonify({
        "ok":           True,
        "instructions": _lm_custom_ai_instructions_from_snapshot(snap),
    })


@app.route("/api/live-monitor/items/<int:item_id>/ai-instructions", methods=["POST"])
@login_required
def api_lm_items_ai_instructions_add(item_id):
    """Add a custom AI instruction to one Live Monitor item."""
    uid, _ = _current_user_id_and_user()
    if not uid:
        return jsonify({"error": "no_user"}), 401

    from models import db as _db, LiveMonitorItem as _LMI, LiveMonitorEvent as _LME
    row = _LMI.query.filter_by(id=item_id).first()
    if not row:
        return jsonify({"error": "not_found"}), 404
    if row.user_id != uid:
        return jsonify({"error": "forbidden"}), 403
    if not row.is_active:
        return jsonify({"error": "inactive"}), 400

    body   = request.get_json(silent=True) or {}
    text   = (body.get("text") or "").strip()
    source = body.get("source", "manual")

    if not text:
        return jsonify({"error": "no_text", "message": "Instruction text is required."}), 400

    safe, reason = _lm_instruction_is_safe(text)
    if not safe:
        return jsonify({"ok": False, "blocked": True, "reason": reason}), 400

    snap, ins = _lm_add_custom_ai_instruction(row, text, source=source)
    if ins and ins.get("blocked"):
        return jsonify({"ok": False, "blocked": True, "reason": ins.get("reason")}), 400

    row.updated_at = datetime.now(timezone.utc)
    ev = _LME(
        item_id           = row.id,
        user_id           = uid,
        symbol            = row.symbol,
        event_type        = "ai_instruction_added",
        event_description = "AI custom instruction added",
        details_json      = _json_dumps_safe({
            "instruction_id": ins.get("id") if ins else None,
            "text":           (text or "")[:80],
        }),
        health_score_at_event = row.score,
        price_at_event        = row.current_price,
    )
    _db.session.add(ev)
    try:
        _db.session.commit()
    except Exception as e:
        _db.session.rollback()
        return jsonify({"error": "db", "message": str(e)}), 500

    return jsonify({
        "ok":          True,
        "instruction": ins,
        "item":        _live_monitor_item_to_dict(row),
    })


@app.route("/api/live-monitor/items/<int:item_id>/ai-instructions/<instruction_id>",
           methods=["DELETE"])
@login_required
def api_lm_items_ai_instructions_delete(item_id, instruction_id):
    """Mark a custom AI instruction as inactive for one Live Monitor item."""
    uid, _ = _current_user_id_and_user()
    if not uid:
        return jsonify({"error": "no_user"}), 401

    from models import db as _db, LiveMonitorItem as _LMI, LiveMonitorEvent as _LME
    row = _LMI.query.filter_by(id=item_id).first()
    if not row:
        return jsonify({"error": "not_found"}), 404
    if row.user_id != uid:
        return jsonify({"error": "forbidden"}), 403
    if not row.is_active:
        return jsonify({"error": "inactive"}), 400

    snap, removed = _lm_remove_custom_ai_instruction(row, instruction_id)
    if not removed:
        return jsonify({"ok": False, "error": "not_found", "message": "Instruction not found or already removed."}), 404

    row.updated_at = datetime.now(timezone.utc)
    ev = _LME(
        item_id           = row.id,
        user_id           = uid,
        symbol            = row.symbol,
        event_type        = "ai_instruction_removed",
        event_description = "AI custom instruction removed",
        details_json      = _json_dumps_safe({"instruction_id": instruction_id}),
        health_score_at_event = row.score,
        price_at_event        = row.current_price,
    )
    _db.session.add(ev)
    try:
        _db.session.commit()
    except Exception as e:
        _db.session.rollback()
        return jsonify({"error": "db", "message": str(e)}), 500

    return jsonify({
        "ok":      True,
        "removed": True,
        "item":    _live_monitor_item_to_dict(row),
    })


# ── Phase 6.7: detect-events endpoint ─────────────────────────────────────────

@app.route("/api/live-monitor/items/<int:item_id>/detect-events", methods=["POST"])
@login_required
def api_lm_items_detect_events(item_id):
    """Run the Phase 6.7 event detector for one Live Monitor item.
    Stores latest_event_detection in snapshot_json. Creates LiveMonitorEvents
    for important detections (zone_touch, zone_breach_risk, session_warning,
    market_pressure). Deduplicates by event_key within current snapshot.
    """
    uid, _ = _current_user_id_and_user()
    if not uid:
        return jsonify({"error": "no_user"}), 401

    from models import db as _db, LiveMonitorItem as _LMI, LiveMonitorEvent as _LME
    row = _LMI.query.filter_by(id=item_id).first()
    if not row:
        return jsonify({"error": "not_found"}), 404
    if row.user_id != uid:
        return jsonify({"error": "forbidden"}), 403
    if not row.is_active:
        return jsonify({"error": "inactive", "message": "Item is no longer active."}), 400

    # Auto-refresh candle features if missing or stale (>120s)
    snap = _json_loads_safe(row.snapshot_json, {})
    snap = _lm_maybe_refresh_candles(row, snap)
    snap, detection = _lm_attach_event_detection(row, snapshot=snap)
    snap, readiness = _lm_attach_setup_readiness(row, snapshot=snap)

    # Phase 8.0 Task 4: auto-capture memory for capture-worthy states
    memory_appended = False
    if readiness.get("readiness_state") in _LM_MEMORY_CAPTURE_STATES:
        snap, _mem_rec, memory_appended = _lm_append_setup_memory(row, snapshot=snap, source="detect_events")
    # Phase 8.1 Task 8: auto update outcomes after memory append
    snap, _outcomes_updated = _lm_update_memory_outcomes(row, snapshot=snap)

    # Deduplication: track which event_keys have already produced a LiveMonitorEvent
    seen_keys: list = snap.get("event_detection_keys") or []

    important_types = {"zone_touch", "zone_breach_risk", "session_warning", "market_pressure"}
    new_events_to_create = []
    for ev in detection.get("events", []):
        ek = ev.get("event_key", "")
        if ev.get("type") in important_types and ek and ek not in seen_keys:
            new_events_to_create.append(ev)
            seen_keys.append(ek)

    # Keep dedup list bounded to last 100 keys
    snap["event_detection_keys"] = seen_keys[-100:]
    row.snapshot_json = _json_dumps_safe(snap)

    try:
        _db.session.flush()
        for ev in new_events_to_create:
            lme = _LME(
                item_id           = row.id,
                user_id           = uid,
                symbol            = row.symbol,
                event_type        = ev.get("type", "updated"),
                event_description = ev.get("label", "Detected event"),
                details_json      = _json_dumps_safe(ev.get("details", {})),
                price_at_event    = row.current_price,
            )
            _db.session.add(lme)
        _db.session.commit()
    except Exception as e:
        _db.session.rollback()
        return jsonify({"error": "db", "message": str(e)}), 500

    return jsonify({
        "ok":              True,
        "item":            _live_monitor_item_to_dict(row),
        "event_detection": detection,
        "setup_readiness": readiness,
        "new_events_logged": len(new_events_to_create),
        "memory_appended": memory_appended,
    })


@app.route("/api/live-monitor/detect-events-all", methods=["POST"])
@login_required
def api_lm_detect_events_all():
    """Run event detection across all active items for the current user (max 10)."""
    uid, _ = _current_user_id_and_user()
    if not uid:
        return jsonify({"error": "no_user"}), 401

    from models import db as _db, LiveMonitorItem as _LMI, LiveMonitorEvent as _LME
    rows = (_LMI.query.filter_by(user_id=uid, is_active=True)
                      .order_by(_LMI.updated_at.desc()).limit(10).all())

    results, errors = [], []
    for row in rows:
        try:
            # Auto-refresh candle features if missing or stale (>120s)
            snap = _json_loads_safe(row.snapshot_json, {})
            snap = _lm_maybe_refresh_candles(row, snap)
            snap, detection = _lm_attach_event_detection(row, snapshot=snap)
            snap, readiness = _lm_attach_setup_readiness(row, snapshot=snap)

            # Phase 8.0 Task 4: auto-capture memory for capture-worthy states
            mem_appended = False
            if readiness.get("readiness_state") in _LM_MEMORY_CAPTURE_STATES:
                snap, _mr, mem_appended = _lm_append_setup_memory(row, snapshot=snap, source="detect_events")
            # Phase 8.1 Task 8: auto update outcomes
            snap, _oc = _lm_update_memory_outcomes(row, snapshot=snap)

            seen_keys: list = snap.get("event_detection_keys") or []
            important_types = {"zone_touch", "zone_breach_risk", "session_warning", "market_pressure"}
            new_events_to_create = []
            for ev in detection.get("events", []):
                ek = ev.get("event_key", "")
                if ev.get("type") in important_types and ek and ek not in seen_keys:
                    new_events_to_create.append(ev)
                    seen_keys.append(ek)

            snap["event_detection_keys"] = seen_keys[-100:]
            row.snapshot_json = _json_dumps_safe(snap)

            _db.session.flush()
            for ev in new_events_to_create:
                lme = _LME(
                    item_id           = row.id,
                    user_id           = uid,
                    symbol            = row.symbol,
                    event_type        = ev.get("type", "updated"),
                    event_description = ev.get("label", "Detected event"),
                    details_json      = _json_dumps_safe(ev.get("details", {})),
                    price_at_event    = row.current_price,
                )
                _db.session.add(lme)
            # Commit immediately so a later row failure cannot roll back this row
            _db.session.commit()
            results.append({
                "id":              row.id,
                "symbol":          row.symbol,
                "event_detection": detection,
                "setup_readiness": readiness,
                "new_events_logged": len(new_events_to_create),
                "memory_appended": mem_appended,
            })
        except Exception as e:
            _db.session.rollback()
            errors.append({"id": row.id, "symbol": row.symbol, "error": str(e)})

    return jsonify({"ok": True, "results": results, "errors": errors})


@app.route("/api/live-monitor/items/<int:item_id>/refresh-candles", methods=["POST"])
@login_required
def api_lm_items_refresh_candles(item_id):
    uid, _ = _current_user_id_and_user()
    if not uid:
        return jsonify({"error": "no_user"}), 401

    from models import db as _db, LiveMonitorItem as _LMI
    row = _LMI.query.filter_by(id=item_id).first()
    if not row:
        return jsonify({"error": "not_found"}), 404
    if row.user_id != uid:
        return jsonify({"error": "forbidden"}), 403
    if not row.is_active:
        return jsonify({"error": "inactive"}), 400

    body = request.get_json(silent=True) or {}
    interval = (body.get("interval") or "").strip() or None
    # Invalid interval → None so _lm_attach_candle_features falls back to row.timeframe then 15m
    if interval and interval not in _LM_ALLOWED_CANDLE_INTERVALS:
        interval = None
    try:
        limit = int(body.get("limit") or 1000)
    except (TypeError, ValueError):
        limit = 1000
    limit = max(5, min(limit, 4000))

    # Bust the TTL cache so the user gets truly fresh candles on manual refresh.
    # Phase 9.1 fix: bust the key that the fetcher actually uses — candle_source
    # from data_sources, not row.exchange (which may differ when MEXC is selected).
    resolved_interval = interval if interval else (
        (getattr(row, "timeframe", None) or "").strip() or "15m"
    )
    if resolved_interval not in _LM_ALLOWED_CANDLE_INTERVALS:
        resolved_interval = "15m"
    snap   = _json_loads_safe(row.snapshot_json, {})
    config = _lm_data_source_config(row, snapshot=snap)
    _lm_candle_cache_bust(
        config["execution_symbol"],
        resolved_interval,
        limit=limit,
        exchange=config["candle_source"],
        market=config["execution_market"],
    )

    try:
        snap, features = _lm_attach_candle_features(row, interval=interval, limit=limit)
        row.snapshot_json = _json_dumps_safe(snap)
        row.updated_at    = datetime.now(timezone.utc)
        _db.session.flush()
        _db.session.commit()
    except Exception as e:
        _db.session.rollback()
        return jsonify({"error": str(e)}), 500

    return jsonify({
        "ok":              True,
        "item":            _live_monitor_item_to_dict(row),
        "candle_features": snap.get("latest_candle_features"),
    })


# ── Phase 7.5: Server-watch toggle + manual tick endpoints ───────────────────

@app.route("/api/live-monitor/items/<int:item_id>/server-watch", methods=["POST"])
@login_required
def api_lm_items_server_watch(item_id):
    """Toggle / configure server-side watch for one Live Monitor item.

    Body (all optional):
      { "enabled": true|false, "interval_seconds": 300 }
    Stores config in snapshot_json["server_watch"].
    """
    uid, _ = _current_user_id_and_user()
    if not uid:
        return jsonify({"error": "no_user"}), 401

    from models import db as _db, LiveMonitorItem as _LMI
    row = _LMI.query.filter_by(id=item_id).first()
    if not row:
        return jsonify({"error": "not_found"}), 404
    if row.user_id != uid:
        return jsonify({"error": "forbidden"}), 403
    if not row.is_active:
        return jsonify({"error": "inactive"}), 400

    body = request.get_json(silent=True) or {}
    enabled = bool(body.get("enabled", True))
    try:
        interval_seconds = max(30, min(int(body.get("interval_seconds") or 60), 300))
    except (TypeError, ValueError):
        interval_seconds = 60

    now_iso = datetime.now(timezone.utc).isoformat()

    snap = _json_loads_safe(row.snapshot_json, {})
    sw   = snap.get("server_watch") or {}

    sw["enabled"]          = enabled
    sw["interval_seconds"] = interval_seconds

    # Preserve existing last_tick_at; default to None
    last_tick_at = sw.get("last_tick_at")

    if enabled:
        # next_due_at: immediately if never ticked, else last_tick + interval
        if last_tick_at:
            try:
                from datetime import datetime as _dt2, timezone as _tz2, timedelta as _td
                lt = _dt2.fromisoformat(last_tick_at.replace("Z", "+00:00"))
                sw["next_due_at"] = (lt + _td(seconds=interval_seconds)).isoformat()
            except Exception:
                sw["next_due_at"] = now_iso
        else:
            sw["next_due_at"] = now_iso
        if "last_status" not in sw:
            sw["last_status"] = "idle"
        if "last_error" not in sw:
            sw["last_error"] = ""
    else:
        sw["next_due_at"] = None
        sw["last_status"] = "idle"
        if "last_error" not in sw:
            sw["last_error"] = ""

    sw["last_tick_at"] = last_tick_at  # unchanged

    snap["server_watch"] = sw
    row.snapshot_json    = _json_dumps_safe(snap)
    row.updated_at       = datetime.now(timezone.utc)

    try:
        _db.session.commit()
    except Exception as e:
        _db.session.rollback()
        return jsonify({"error": str(e)}), 500

    # Background thread only starts if explicitly enabled via environment variable.
    # Saving the config here does NOT start the thread.
    if enabled and os.environ.get("ZYNI_LM_SERVER_WATCH_ENABLED") == "1":
        _ensure_lm_watch_thread()

    return jsonify({
        "ok":          True,
        "item":        _live_monitor_item_to_dict(row),
        "server_watch": sw,
    })


@app.route("/api/live-monitor/watch-loop/tick", methods=["POST"])
@login_required
def api_lm_watch_loop_tick():
    """Manually trigger one server-watch tick for all enabled items belonging to the current user.

    Useful from the frontend "Server Watch" button or for testing.
    """
    uid, _ = _current_user_id_and_user()
    if not uid:
        return jsonify({"error": "no_user"}), 401

    from models import LiveMonitorItem as _LMI
    active_rows = _LMI.query.filter_by(user_id=uid, is_active=True).all()

    results  = []
    errors   = []
    tick_cap = 20  # max items per manual tick to prevent runaway latency
    for row in active_rows:
        if len(results) + len(errors) >= tick_cap:
            break
        snap = _json_loads_safe(row.snapshot_json, {})
        sw   = snap.get("server_watch") or {}
        if not sw.get("enabled"):
            continue
        result = _lm_server_watch_tick(row.id, uid)
        if result.get("ok"):
            results.append(result)
        else:
            errors.append({"id": row.id, "symbol": row.symbol, "error": result.get("error")})

    return jsonify({"ok": True, "ticked": len(results), "results": results, "errors": errors})


# ── Phase 8.0 Task 3: capture-memory endpoint ─────────────────────────────────

@app.route("/api/live-monitor/items/<int:item_id>/capture-memory", methods=["POST"])
@login_required
def api_lm_items_capture_memory(item_id):
    """Manually capture a setup reasoning-memory record into snapshot_json."""
    uid, _ = _current_user_id_and_user()
    if not uid:
        return jsonify({"error": "no_user"}), 401

    from models import db as _db, LiveMonitorItem as _LMI, LiveMonitorEvent as _LME
    row = _LMI.query.filter_by(id=item_id).first()
    if not row:
        return jsonify({"error": "not_found"}), 404
    if row.user_id != uid:
        return jsonify({"error": "forbidden"}), 403
    if not row.is_active:
        return jsonify({"error": "inactive"}), 400

    snap = _json_loads_safe(row.snapshot_json, {})
    snap, record, appended = _lm_append_setup_memory(row, snapshot=snap, source="manual")
    row.snapshot_json = _json_dumps_safe(snap)
    row.updated_at    = datetime.now(timezone.utc)

    try:
        _db.session.flush()
        if appended:
            lme = _LME(
                item_id           = row.id,
                user_id           = uid,
                symbol            = row.symbol,
                event_type        = "setup_memory_captured",
                event_description = "Setup reasoning memory captured",
                details_json      = _json_dumps_safe({"record_id": record["id"], "readiness_state": record["readiness_state"]}),
                price_at_event    = row.current_price,
            )
            _db.session.add(lme)
        _db.session.commit()
    except Exception as e:
        _db.session.rollback()
        return jsonify({"error": str(e)}), 500

    return jsonify({
        "ok":             True,
        "item":           _live_monitor_item_to_dict(row),
        "memory_record":  record,
        "memory_summary": snap.get("setup_memory_summary"),
        "appended":       appended,
    })


# ── Phase 8.1 Task 7: update-memory-outcomes endpoint ─────────────────────────

@app.route("/api/live-monitor/items/<int:item_id>/update-memory-outcomes", methods=["POST"])
@login_required
def api_lm_items_update_memory_outcomes(item_id):
    """Update outcome status for pending setup memory records using current price."""
    uid, _ = _current_user_id_and_user()
    if not uid:
        return jsonify({"error": "no_user"}), 401

    from models import db as _db, LiveMonitorItem as _LMI, LiveMonitorEvent as _LME
    row = _LMI.query.filter_by(id=item_id).first()
    if not row:
        return jsonify({"error": "not_found"}), 404
    if row.user_id != uid:
        return jsonify({"error": "forbidden"}), 403
    if not row.is_active:
        return jsonify({"error": "inactive"}), 400

    snap = _json_loads_safe(row.snapshot_json, {})
    snap, updated_count = _lm_update_memory_outcomes(row, snapshot=snap)
    row.snapshot_json = _json_dumps_safe(snap)
    row.updated_at    = datetime.now(timezone.utc)

    try:
        _db.session.flush()
        if updated_count > 0:
            lme = _LME(
                item_id           = row.id,
                user_id           = uid,
                symbol            = row.symbol,
                event_type        = "setup_memory_outcomes_updated",
                event_description = f"Setup memory outcomes updated: {updated_count} record(s) resolved",
                details_json      = _json_dumps_safe({"updated_count": updated_count}),
                price_at_event    = row.current_price,
            )
            _db.session.add(lme)
        _db.session.commit()
    except Exception as e:
        _db.session.rollback()
        return jsonify({"error": str(e)}), 500

    return jsonify({
        "ok":             True,
        "item":           _live_monitor_item_to_dict(row),
        "memory_summary": snap.get("setup_memory_summary"),
        "updated_count":  updated_count,
    })


# ── Phase 8.2 Task 10: memory-accuracy endpoint ───────────────────────────────

@app.route("/api/live-monitor/items/<int:item_id>/memory-accuracy", methods=["GET"])
@login_required
def api_lm_items_memory_accuracy(item_id):
    """Compute and return setup memory accuracy stats from snapshot. Persists result."""
    uid, _ = _current_user_id_and_user()
    if not uid:
        return jsonify({"error": "no_user"}), 401

    from models import db as _db, LiveMonitorItem as _LMI
    row = _LMI.query.filter_by(id=item_id).first()
    if not row:
        return jsonify({"error": "not_found"}), 404
    if row.user_id != uid:
        return jsonify({"error": "forbidden"}), 403
    if not row.is_active:
        return jsonify({"error": "inactive"}), 400

    snap     = _json_loads_safe(row.snapshot_json, {})
    accuracy = _lm_compute_memory_accuracy(snap)

    # Persist so AI context can see setup_memory_accuracy later
    row.snapshot_json = _json_dumps_safe(snap)
    row.updated_at    = datetime.now(timezone.utc)
    try:
        _db.session.commit()
    except Exception as e:
        _db.session.rollback()
        return jsonify({"error": str(e)}), 500

    return jsonify({
        "ok":             True,
        "item":           _live_monitor_item_to_dict(row),
        "accuracy":       accuracy,
        "memory_summary": snap.get("setup_memory_summary"),
    })


# ── Phase 9.0: Data Source Config Endpoints ──────────────────────────────────

@app.route("/api/live-monitor/items/<int:item_id>/data-sources", methods=["GET"])
@login_required
def api_lm_items_data_sources_get(item_id):
    """Return current data_source_config for a Live Monitor item.

    If missing or not yet computed, builds from defaults and saves to snapshot.
    No trading. No order logic.
    """
    uid, _ = _current_user_id_and_user()
    if not uid:
        return jsonify({"error": "no_user"}), 401

    from models import db as _db, LiveMonitorItem as _LMI
    row = _LMI.query.filter_by(id=item_id).first()
    if not row:
        return jsonify({"error": "not_found"}), 404
    if row.user_id != uid:
        return jsonify({"error": "forbidden"}), 403
    if not row.is_active:
        return jsonify({"error": "inactive"}), 400

    snap   = _json_loads_safe(row.snapshot_json, {})
    config = _lm_data_source_config(row, snapshot=snap)

    now_iso = datetime.now(timezone.utc).isoformat()
    saved   = snap.get("data_sources") or {}
    stale   = not saved or snap.get("last_data_sources_at") is None

    if stale:
        snap["data_sources"]         = {k: v for k, v in config.items()
                                         if k != "warnings"}
        snap["last_data_sources_at"] = now_iso
        row.snapshot_json = _json_dumps_safe(snap)
        row.updated_at    = datetime.now(timezone.utc)
        try:
            _db.session.commit()
        except Exception:
            _db.session.rollback()

    return jsonify({"ok": True, "item": _live_monitor_item_to_dict(row), "data_sources": config})


@app.route("/api/live-monitor/items/<int:item_id>/data-sources", methods=["POST"])
@login_required
def api_lm_items_data_sources_post(item_id):
    """Update data source configuration for a Live Monitor item.

    Body (all optional):
      execution_exchange, execution_market, candle_source,
      live_price_source, market_context_sources, aggregation_enabled

    Unknown or invalid values fall back to safe defaults.
    No API keys. No trading. No order logic.
    """
    uid, _ = _current_user_id_and_user()
    if not uid:
        return jsonify({"error": "no_user"}), 401

    from models import db as _db, LiveMonitorItem as _LMI
    row = _LMI.query.filter_by(id=item_id).first()
    if not row:
        return jsonify({"error": "not_found"}), 404
    if row.user_id != uid:
        return jsonify({"error": "forbidden"}), 403
    if not row.is_active:
        return jsonify({"error": "inactive"}), 400

    body = (request.get_json(silent=True) or {})
    snap = _json_loads_safe(row.snapshot_json, {})

    existing = snap.get("data_sources") or {}
    if "execution_exchange" in body:
        existing["execution_exchange"]     = _lm_normalize_exchange(body["execution_exchange"])
    if "execution_market" in body:
        existing["execution_market"]       = _lm_normalize_market(body["execution_market"])
    if "candle_source" in body:
        existing["candle_source"]          = _lm_normalize_exchange(body["candle_source"])
    if "live_price_source" in body:
        existing["live_price_source"]      = _lm_normalize_exchange(body["live_price_source"])
    if "market_context_sources" in body:
        raw = body["market_context_sources"]
        if isinstance(raw, list):
            mcs = [s for s in [_lm_normalize_exchange(x) for x in raw]
                   if s in _LM_ALLOWED_EXCHANGES]
            existing["market_context_sources"] = mcs or ["binance"]
    if "aggregation_enabled" in body:
        existing["aggregation_enabled"]    = bool(body["aggregation_enabled"])

    now_iso = datetime.now(timezone.utc).isoformat()
    snap["data_sources"]         = existing
    snap["last_data_sources_at"] = now_iso
    row.snapshot_json = _json_dumps_safe(snap)
    row.updated_at    = datetime.now(timezone.utc)

    try:
        _db.session.commit()
    except Exception as e:
        _db.session.rollback()
        return jsonify({"error": str(e)}), 500

    config = _lm_data_source_config(row, snapshot=snap)
    return jsonify({"ok": True, "item": _live_monitor_item_to_dict(row), "data_sources": config})


# ── Phase 9.05: MEXC Capability Status Endpoint ───────────────────────────────

@app.route("/api/live-monitor/mexc-capability-status", methods=["GET"])
@login_required
def api_lm_mexc_capability_status():
    """Return MEXC capability status (env check only).

    Never calls order endpoints. Never returns API keys.
    For the full public candle/ticker audit, run scripts/mexc_capability_audit.py.
    """
    uid, _ = _current_user_id_and_user()
    if not uid:
        return jsonify({"error": "no_user"}), 401

    status = _lm_mexc_capability_status_from_env()
    return jsonify({"ok": True, "mexc_capability": status})


# ── Phase 9.2: Aggregated Market Context Endpoint ────────────────────────────

@app.route("/api/live-monitor/items/<int:item_id>/refresh-aggregated-context", methods=["POST"])
@login_required
def api_lm_items_refresh_aggregated_context(item_id):
    """Compute and store aggregated market context (Binance-first).

    No trading. No order logic. No API key returned.
    Saves to snapshot["latest_aggregated_market_context"].
    """
    uid, _ = _current_user_id_and_user()
    if not uid:
        return jsonify({"error": "no_user"}), 401

    from models import db as _db, LiveMonitorItem as _LMI
    row = _LMI.query.filter_by(id=item_id).first()
    if not row:
        return jsonify({"error": "not_found"}), 404
    if row.user_id != uid:
        return jsonify({"error": "forbidden"}), 403
    if not row.is_active:
        return jsonify({"error": "inactive"}), 400

    snap = _json_loads_safe(row.snapshot_json, {})
    amc  = _lm_compute_aggregated_market_context(row, snapshot=snap)

    now_iso = datetime.now(timezone.utc).isoformat()
    snap["latest_aggregated_market_context"]    = amc
    snap["last_aggregated_market_context_at"]   = now_iso
    row.snapshot_json = _json_dumps_safe(snap)
    row.updated_at    = datetime.now(timezone.utc)

    try:
        _db.session.commit()
    except Exception as e:
        _db.session.rollback()
        return jsonify({"error": str(e)}), 500

    return jsonify({
        "ok":                        True,
        "item":                      _live_monitor_item_to_dict(row),
        "aggregated_market_context": amc,
    })


# ── Phase 9.3: LiveMonitorTrade Endpoints ─────────────────────────────────────

@app.route("/api/live-monitor/items/<int:item_id>/trades", methods=["GET"])
@login_required
def api_lm_items_trades_list(item_id):
    """List trades for a specific Live Monitor item. Max 50."""
    uid, _ = _current_user_id_and_user()
    if not uid:
        return jsonify({"error": "no_user"}), 401

    from models import LiveMonitorItem as _LMI, LiveMonitorTrade as _LMT
    row = _LMI.query.filter_by(id=item_id).first()
    if not row:
        return jsonify({"error": "not_found"}), 404
    if row.user_id != uid:
        return jsonify({"error": "forbidden"}), 403

    trades = (_LMT.query
                  .filter_by(live_monitor_item_id=item_id, user_id=uid)
                  .order_by(_LMT.created_at.desc())
                  .limit(50)
                  .all())
    return jsonify({
        "ok":     True,
        "trades": [_lm_trade_to_dict(t) for t in trades],
        "count":  len(trades),
    })


@app.route("/api/live-monitor/trades", methods=["GET"])
@login_required
def api_lm_trades_list():
    """List all trades for the current user. Max 50."""
    uid, _ = _current_user_id_and_user()
    if not uid:
        return jsonify({"error": "no_user"}), 401

    from models import LiveMonitorTrade as _LMT
    trades = (_LMT.query
                  .filter_by(user_id=uid)
                  .order_by(_LMT.created_at.desc())
                  .limit(50)
                  .all())
    return jsonify({
        "ok":     True,
        "trades": [_lm_trade_to_dict(t) for t in trades],
        "count":  len(trades),
    })


@app.route("/api/live-monitor/trades/<trade_uid>/cancel", methods=["POST"])
@login_required
def api_lm_trade_cancel(trade_uid):
    """Cancel a draft/proposed/risk_rejected trade. No exchange call."""
    uid, _ = _current_user_id_and_user()
    if not uid:
        return jsonify({"error": "no_user"}), 401

    from models import db as _db, LiveMonitorTrade as _LMT
    trade = _LMT.query.filter_by(trade_uid=trade_uid).first()
    if not trade:
        return jsonify({"error": "not_found"}), 404
    if trade.user_id != uid:
        return jsonify({"error": "forbidden"}), 403
    if trade.status not in _LM_TRADE_CANCELLABLE_STATUSES:
        return jsonify({
            "error":   "not_cancellable",
            "message": f"Trade status '{trade.status}' cannot be cancelled.",
        }), 400

    trade.status     = "cancelled"
    trade.updated_at = datetime.now(timezone.utc)
    try:
        _db.session.commit()
    except Exception as e:
        _db.session.rollback()
        return jsonify({"error": str(e)}), 500

    return jsonify({"ok": True, "trade": _lm_trade_to_dict(trade)})


# ── Phase 9.4: AI Trade Proposal Endpoint ─────────────────────────────────────

@app.route("/api/live-monitor/items/<int:item_id>/ai-trade-proposal", methods=["POST"])
@login_required
def api_lm_items_ai_trade_proposal(item_id):
    """Generate AI trade proposal for a Live Monitor item.

    Refreshes stale data if needed, calls AI with proposal prompt, creates
    LiveMonitorTrade record (status=proposed or draft). No execution.
    """
    uid, _ = _current_user_id_and_user()
    if not uid:
        return jsonify({"error": "no_user"}), 401

    from models import db as _db, LiveMonitorItem as _LMI, LiveMonitorTrade as _LMT
    row = _LMI.query.filter_by(id=item_id).first()
    if not row:
        return jsonify({"error": "not_found"}), 404
    if row.user_id != uid:
        return jsonify({"error": "forbidden"}), 403
    if not row.is_active:
        return jsonify({"error": "inactive"}), 400

    snap = _json_loads_safe(row.snapshot_json, {})

    # Ensure data_sources exist
    if not snap.get("data_sources"):
        config = _lm_data_source_config(row, snapshot=snap)
        snap["data_sources"]         = {k: v for k, v in config.items() if k != "warnings"}
        snap["last_data_sources_at"] = datetime.now(timezone.utc).isoformat()

    # Refresh candle features if missing/stale
    from datetime import datetime as _dt_p, timezone as _tz_p
    cf_at = snap.get("last_candle_features_at")
    cf_stale = True
    if cf_at:
        try:
            age = (_dt_p.now(_tz_p.utc) - _dt_p.fromisoformat(cf_at.replace("Z","+00:00"))).total_seconds()
            cf_stale = age > 180
        except Exception:
            cf_stale = True
    if cf_stale or not snap.get("latest_candle_features"):
        snap, _ = _lm_attach_candle_features(row, limit=1000)

    # Refresh event detection if missing
    if not snap.get("latest_event_detection"):
        snap, _ = _lm_attach_event_detection(row, snapshot=snap)

    # Refresh setup readiness if missing
    if not snap.get("latest_setup_readiness"):
        snap, _ = _lm_attach_setup_readiness(row, snapshot=snap)

    # Refresh aggregated context if missing/stale
    amc_at = snap.get("last_aggregated_market_context_at")
    amc_stale = True
    if amc_at:
        try:
            age = (_dt_p.now(_tz_p.utc) - _dt_p.fromisoformat(amc_at.replace("Z","+00:00"))).total_seconds()
            amc_stale = age > 300
        except Exception:
            amc_stale = True
    if amc_stale or not snap.get("latest_aggregated_market_context"):
        amc = _lm_compute_aggregated_market_context(row, snapshot=snap)
        snap["latest_aggregated_market_context"]  = amc
        snap["last_aggregated_market_context_at"] = datetime.now(timezone.utc).isoformat()

    # Save refreshed snapshot
    row.snapshot_json = _json_dumps_safe(snap)
    row.updated_at    = datetime.now(timezone.utc)

    # Build proposal context (no raw candles, no API keys)
    proposal_ctx = _lm_build_trade_proposal_context(row, snapshot=snap)

    # Call AI with proposal system prompt
    agent    = _lm_get_ai_agent_config(None)
    proposal_raw = {}
    ai_ok        = False
    ai_error     = None

    if agent.get("configured"):
        try:
            # Use proposal-specific system prompt instead of main system prompt
            proposal_agent = dict(agent)
            user_content   = json.dumps({
                "context":      proposal_ctx,
                "user_message": "Evaluate this setup and produce a trade proposal JSON.",
            }, ensure_ascii=False)

            provider = agent.get("provider", "local_fallback")
            api_key  = os.environ.get(
                agent.get("_api_key_env", "") or {
                    "openrouter": "OPENROUTER_API_KEY",
                    "openai": "OPENAI_API_KEY",
                    "deepseek": "DEEPSEEK_API_KEY",
                    "custom_openai": "CUSTOM_AI_API_KEY",
                }.get(provider, "OPENROUTER_API_KEY"),
                ""
            ).strip()
            api_base = (
                agent.get("api_base")
                or os.environ.get(agent.get("_api_base_env", "") or "", "").strip()
                or os.environ.get("OPENROUTER_API_BASE", "https://openrouter.ai/api/v1/chat/completions")
            )
            headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
            if provider == "openrouter":
                app_title = os.environ.get("OPENROUTER_APP_TITLE", "ZyNi SMC Screener")
                http_ref  = os.environ.get("OPENROUTER_HTTP_REFERER", "")
                if http_ref:
                    headers["HTTP-Referer"] = http_ref
                if app_title:
                    headers["X-Title"] = app_title

            payload = {
                "model":       agent.get("model", ""),
                "messages":    [
                    {"role": "system", "content": _lm_ai_trade_proposal_prompt()},
                    {"role": "user",   "content": user_content},
                ],
                "temperature": 0.2,
                "max_tokens":  800,
            }
            resp = req.post(api_base, headers=headers, json=payload, timeout=30)
            if resp.status_code == 200:
                data   = resp.json()
                text   = (data.get("choices") or [{}])[0].get("message", {}).get("content", "")
                proposal_raw = _lm_parse_ai_json(text)
                ai_ok = True
            else:
                ai_error = f"AI provider {resp.status_code}"
        except Exception as _e:
            ai_error = f"AI call error: {type(_e).__name__}"
    else:
        ai_error = "AI provider not configured"

    if not ai_ok or not proposal_raw or "_parse_error" in proposal_raw:
        proposal_raw = {
            "action":                 "wait",
            "setup_type":             getattr(row, "setup_type", None),
            "direction":              getattr(row, "direction", None),
            "confidence":             0,
            "entry_logic":            "AI unavailable",
            "invalidation_logic":     "AI unavailable",
            "take_profit_logic":      "AI unavailable",
            "required_confirmations": [],
            "risk_notes":             [ai_error or "AI not configured"],
            "reasoning_summary":      "AI provider unavailable or parse error.",
        }

    # Create trade record
    trade = _lm_create_trade_record_from_proposal(row, proposal_raw, snapshot=snap)

    try:
        _db.session.add(trade)
        _db.session.flush()
        _db.session.commit()
    except Exception as e:
        _db.session.rollback()
        return jsonify({"error": str(e)}), 500

    # Save updated snapshot
    try:
        _db.session.add(row)
        _db.session.commit()
    except Exception:
        _db.session.rollback()

    return jsonify({
        "ok":       True,
        "item":     _live_monitor_item_to_dict(row),
        "proposal": proposal_raw,
        "trade":    _lm_trade_to_dict(trade),
        "ai_ok":    ai_ok,
        "ai_error": ai_error,
    })


# ── Phase 9.5: Risk Guard Endpoints ───────────────────────────────────────────

@app.route("/api/live-monitor/trades/<trade_uid>/risk-check", methods=["POST"])
@login_required
def api_lm_trade_risk_check(trade_uid):
    """Run Risk Guard on an existing trade record. No execution."""
    uid, _ = _current_user_id_and_user()
    if not uid:
        return jsonify({"error": "no_user"}), 401

    from models import db as _db, LiveMonitorTrade as _LMT, LiveMonitorItem as _LMI
    trade = _LMT.query.filter_by(trade_uid=trade_uid).first()
    if not trade:
        return jsonify({"error": "not_found"}), 404
    if trade.user_id != uid:
        return jsonify({"error": "forbidden"}), 403
    if trade.status in ("cancelled", "closed", "failed"):
        return jsonify({
            "error":   "not_checkable",
            "message": f"Trade status '{trade.status}' cannot be risk-checked.",
        }), 400

    row = None
    if trade.live_monitor_item_id:
        row = _LMI.query.filter_by(id=trade.live_monitor_item_id).first()

    snap = None
    if row:
        snap = _json_loads_safe(row.snapshot_json, {})

    rg = _lm_run_risk_guard(trade, row=row, snapshot=snap)

    trade.risk_guard_json   = _json_dumps_safe(rg)
    trade.risk_guard_status = rg["status"]
    trade.updated_at        = datetime.now(timezone.utc)

    if rg["approved"]:
        trade.status = "risk_approved"
    else:
        trade.status           = "risk_rejected"
        trade.rejection_reason = "; ".join(rg.get("hard_blocks") or rg.get("reasons") or ["rejected"])

    try:
        _db.session.commit()
    except Exception as e:
        _db.session.rollback()
        return jsonify({"error": str(e)}), 500

    return jsonify({
        "ok":         True,
        "trade":      _lm_trade_to_dict(trade),
        "risk_guard": rg,
    })


@app.route("/api/live-monitor/items/<int:item_id>/ai-trade-proposal-and-risk-check",
           methods=["POST"])
@login_required
def api_lm_items_proposal_and_risk_check(item_id):
    """Generate AI trade proposal and immediately run Risk Guard. No execution.

    Convenience endpoint: calls ai-trade-proposal then risk-check in one request.
    """
    uid, _ = _current_user_id_and_user()
    if not uid:
        return jsonify({"error": "no_user"}), 401

    from models import db as _db, LiveMonitorItem as _LMI, LiveMonitorTrade as _LMT
    row = _LMI.query.filter_by(id=item_id).first()
    if not row:
        return jsonify({"error": "not_found"}), 404
    if row.user_id != uid:
        return jsonify({"error": "forbidden"}), 403
    if not row.is_active:
        return jsonify({"error": "inactive"}), 400

    # ── Step 1: delegate to proposal endpoint logic (inline to avoid HTTP round-trip)
    snap = _json_loads_safe(row.snapshot_json, {})

    if not snap.get("data_sources"):
        config = _lm_data_source_config(row, snapshot=snap)
        snap["data_sources"]         = {k: v for k, v in config.items() if k != "warnings"}
        snap["last_data_sources_at"] = datetime.now(timezone.utc).isoformat()

    from datetime import datetime as _dt_c, timezone as _tz_c
    def _stale(key, secs):
        at = snap.get(key)
        if not at:
            return True
        try:
            return (_dt_c.now(_tz_c.utc) - _dt_c.fromisoformat(at.replace("Z","+00:00"))).total_seconds() > secs
        except Exception:
            return True

    if _stale("last_candle_features_at", 180) or not snap.get("latest_candle_features"):
        snap, _ = _lm_attach_candle_features(row, limit=1000)
    if not snap.get("latest_event_detection"):
        snap, _ = _lm_attach_event_detection(row, snapshot=snap)
    if not snap.get("latest_setup_readiness"):
        snap, _ = _lm_attach_setup_readiness(row, snapshot=snap)
    if _stale("last_aggregated_market_context_at", 300) or not snap.get("latest_aggregated_market_context"):
        amc = _lm_compute_aggregated_market_context(row, snapshot=snap)
        snap["latest_aggregated_market_context"]  = amc
        snap["last_aggregated_market_context_at"] = datetime.now(timezone.utc).isoformat()

    row.snapshot_json = _json_dumps_safe(snap)
    row.updated_at    = datetime.now(timezone.utc)

    proposal_ctx = _lm_build_trade_proposal_context(row, snapshot=snap)
    agent        = _lm_get_ai_agent_config(None)
    proposal_raw = {}
    ai_ok        = False
    ai_error     = None

    if agent.get("configured"):
        try:
            provider = agent.get("provider", "local_fallback")
            api_key  = os.environ.get(
                agent.get("_api_key_env", "") or {
                    "openrouter": "OPENROUTER_API_KEY", "openai": "OPENAI_API_KEY",
                    "deepseek": "DEEPSEEK_API_KEY", "custom_openai": "CUSTOM_AI_API_KEY",
                }.get(provider, "OPENROUTER_API_KEY"), ""
            ).strip()
            api_base = (
                agent.get("api_base")
                or os.environ.get(agent.get("_api_base_env","") or "", "").strip()
                or os.environ.get("OPENROUTER_API_BASE","https://openrouter.ai/api/v1/chat/completions")
            )
            headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
            if provider == "openrouter":
                ht = os.environ.get("OPENROUTER_HTTP_REFERER","")
                at = os.environ.get("OPENROUTER_APP_TITLE","ZyNi SMC Screener")
                if ht: headers["HTTP-Referer"] = ht
                if at: headers["X-Title"] = at
            payload = {
                "model":       agent.get("model",""),
                "messages":    [
                    {"role": "system", "content": _lm_ai_trade_proposal_prompt()},
                    {"role": "user",   "content": json.dumps({
                        "context": proposal_ctx,
                        "user_message": "Evaluate this setup and produce a trade proposal JSON.",
                    }, ensure_ascii=False)},
                ],
                "temperature": 0.2, "max_tokens": 800,
            }
            resp = req.post(api_base, headers=headers, json=payload, timeout=30)
            if resp.status_code == 200:
                text         = (resp.json().get("choices") or [{}])[0].get("message",{}).get("content","")
                proposal_raw = _lm_parse_ai_json(text)
                ai_ok        = True
            else:
                ai_error = f"AI provider {resp.status_code}"
        except Exception as _e:
            ai_error = f"AI call error: {type(_e).__name__}"
    else:
        ai_error = "AI provider not configured"

    if not ai_ok or not proposal_raw or "_parse_error" in proposal_raw:
        proposal_raw = {
            "action": "wait", "setup_type": getattr(row,"setup_type",None),
            "direction": getattr(row,"direction",None), "confidence": 0,
            "entry_logic": "AI unavailable", "invalidation_logic": "AI unavailable",
            "take_profit_logic": "AI unavailable", "required_confirmations": [],
            "risk_notes": [ai_error or "AI not configured"],
            "reasoning_summary": "AI provider unavailable or parse error.",
        }

    trade = _lm_create_trade_record_from_proposal(row, proposal_raw, snapshot=snap)
    try:
        _db.session.add(trade)
        _db.session.flush()
    except Exception as e:
        _db.session.rollback()
        return jsonify({"error": str(e)}), 500

    # ── Step 2: run Risk Guard immediately ───────────────────────────────────
    rg = _lm_run_risk_guard(trade, row=row, snapshot=snap)
    trade.risk_guard_json   = _json_dumps_safe(rg)
    trade.risk_guard_status = rg["status"]
    trade.status = "risk_approved" if rg["approved"] else "risk_rejected"
    if not rg["approved"]:
        trade.rejection_reason = "; ".join(rg.get("hard_blocks") or rg.get("reasons") or ["rejected"])
    trade.updated_at = datetime.now(timezone.utc)

    try:
        _db.session.add(row)
        _db.session.commit()
    except Exception as e:
        _db.session.rollback()
        return jsonify({"error": str(e)}), 500

    return jsonify({
        "ok":         True,
        "item":       _live_monitor_item_to_dict(row),
        "proposal":   proposal_raw,
        "trade":      _lm_trade_to_dict(trade),
        "risk_guard": rg,
        "ai_ok":      ai_ok,
        "ai_error":   ai_error,
    })


@app.route("/api/live-monitor/items/<int:item_id>/ai-chat", methods=["POST"])
@login_required
def api_lm_items_ai_chat(item_id):
    """Handle a live chat message about one item. Does not store message history in DB."""
    uid, _ = _current_user_id_and_user()
    if not uid:
        return jsonify({"error": "no_user"}), 401

    from models import LiveMonitorItem as _LMI
    row = _LMI.query.filter_by(id=item_id).first()
    if not row:
        return jsonify({"error": "not_found"}), 404
    if row.user_id != uid:
        return jsonify({"error": "forbidden"}), 403
    if not row.is_active:
        return jsonify({"error": "inactive", "message": "Item is no longer active."}), 400

    body         = request.get_json(silent=True) or {}
    user_message = (body.get("message") or "")[:1000].strip()
    agent_id     = (body.get("agent_id") or "").strip() or None
    if not user_message:
        return jsonify({"error": "no_message"}), 400

    # ── Phase 6.6: Detect and save custom AI instruction from chat ───────────
    extracted = _lm_extract_instruction_from_chat(user_message)
    if extracted.get("confidence", 0) >= 70:
        ins_text = extracted["text"]
        safe, block_reason = _lm_instruction_is_safe(ins_text)
        if not safe:
            # Blocked — reply with reason, do not call AI
            return jsonify({
                "ok":               False,
                "reply":            f"Instruction blocked: {block_reason}",
                "instruction_saved": False,
                "blocked":          True,
                "reason":           block_reason,
                "provider":         "local",
                "configured":       False,
            })
        from models import db as _db2, LiveMonitorEvent as _LME2
        snap, ins = _lm_add_custom_ai_instruction(row, ins_text, source="chat")
        if ins and not ins.get("blocked"):
            row.updated_at = datetime.now(timezone.utc)
            ev = _LME2(
                item_id           = row.id,
                user_id           = uid,
                symbol            = row.symbol,
                event_type        = "ai_instruction_added",
                event_description = "AI custom instruction added via chat",
                details_json      = _json_dumps_safe({
                    "instruction_id": ins.get("id"),
                    "text":           (ins_text or "")[:80],
                    "source":         "chat",
                }),
                health_score_at_event = row.score,
                price_at_event        = row.current_price,
            )
            _db2.session.add(ev)
            try:
                _db2.session.commit()
            except Exception:
                _db2.session.rollback()
                ins = None
            if ins:
                return jsonify({
                    "ok":                True,
                    "reply":             f"Saved as a custom AI instruction: \"{ins_text[:120]}\"",
                    "instruction_saved": True,
                    "instruction":       ins,
                    "item":              _live_monitor_item_to_dict(row),
                    "provider":          "local",
                    "configured":        False,
                })
        if ins and ins.get("blocked"):
            return jsonify({
                "ok":               False,
                "reply":            ins.get("reason", "Could not save instruction."),
                "instruction_saved": False,
                "blocked":          True,
                "reason":           ins.get("reason"),
                "provider":         "local",
                "configured":       False,
            })
    # ── Normal chat: call AI provider ───────────────────────────────────────
    # Ensure event detection is available for AI context (lazy init, no commit, no timeline logging)
    snap_chat = _json_loads_safe(row.snapshot_json, {})
    if not snap_chat.get("latest_event_detection"):
        snap_chat, _ = _lm_attach_event_detection(row, snapshot=snap_chat)
        row.snapshot_json = _json_dumps_safe(snap_chat)  # in-memory only; committed only if instruction saved above
    context   = _lm_build_ai_context(row)
    ai_result = _lm_call_ai_provider(context, user_message, agent_id=agent_id)
    analysis  = ai_result.get("analysis", {})

    # Build short chat reply from analysis
    parts = []
    if analysis.get("summary"):
        parts.append(analysis["summary"])
    if analysis.get("agent_note") and analysis["agent_note"] != analysis.get("summary"):
        parts.append(analysis["agent_note"])
    if analysis.get("next_actions"):
        parts.append("Next: " + "; ".join(analysis["next_actions"][:2]))
    reply = " ".join(parts) if parts else (
        analysis.get("summary") or "Unable to generate reply. Try again."
    )

    return jsonify({
        "ok":               True,
        "reply":            reply,
        "instruction_saved": False,
        "analysis":         analysis,
        "provider":         ai_result.get("provider"),
        "agent_id":         ai_result.get("agent_id"),
        "agent_label":      ai_result.get("agent_label"),
        "configured":       ai_result.get("configured", False),
    })


# ── Phase 9.6: Demo Execution Endpoints ──────────────────────────────────────


@app.route("/api/live-monitor/demo-execution/status", methods=["GET"])
@login_required
def api_lm_demo_execution_status():
    """Return demo execution gate status, MEXC account health, and safe diagnostics."""
    uid, _ = _current_user_id_and_user()
    if not uid:
        return jsonify({"error": "no_user"}), 401

    gate   = _lm_demo_trading_enabled()
    health = None
    if not gate["blocked"]:
        health = _lm_mexc_demo_health_check()

    # Safe diagnostics — no keys, no secrets, no full signatures
    priv_debug = (health or {}).get("_debug") or {}
    mexc_private_debug = {
        "base_url_used":         priv_debug.get("base_url"),
        "endpoint_path_used":    priv_debug.get("endpoint_path"),
        "full_url_used":         priv_debug.get("full_url"),
        "auth_style":            priv_debug.get("auth_style"),
        "request_time_present":  bool(priv_debug.get("request_time_ms")),
        "signature_present":     bool(priv_debug.get("signature_present")),
        "api_key_present":       gate.get("mexc_keys_present", False),
        "api_key_tail":          priv_debug.get("api_key_tail"),
        "status_code":           priv_debug.get("status_code"),
        "response_type":         priv_debug.get("response_type"),
        "error_summary":         ((health or {}).get("error_summary") or "")[:160] or None,
    } if health is not None else None

    # Strip internal _debug from health before returning to client
    health_clean = None
    if health is not None:
        health_clean = {k: v for k, v in health.items() if k != "_debug"}

    return jsonify({
        "ok":                  True,
        "gate":                gate,
        "health":              health_clean,
        "mexc_private_debug":  mexc_private_debug,
    })


@app.route("/api/live-monitor/trades/<trade_uid>/demo-submit", methods=["POST"])
@login_required
def api_lm_trade_demo_submit(trade_uid):
    """Safety-gated demo order submission for a risk_approved trade.

    Phase 9.6: All safety gates are checked. Since MEXC demo order submission
    endpoint is not confirmed, this endpoint defaults to returning blocked with
    reason 'mexc_demo_submit_not_confirmed'. No real or demo orders are placed.
    """
    uid, _ = _current_user_id_and_user()
    if not uid:
        return jsonify({"error": "no_user"}), 401

    # Gate 1: demo trading env flags
    gate = _lm_demo_trading_enabled()
    if gate["blocked"]:
        return jsonify({
            "ok":             False,
            "blocked":        True,
            "blocked_reason": gate["blocked_reason"],
        }), 400

    from models import db as _db, LiveMonitorTrade as _LMT, LiveMonitorItem as _LMI
    trade = _LMT.query.filter_by(trade_uid=trade_uid).first()
    if not trade:
        return jsonify({"error": "not_found"}), 404
    if trade.user_id != uid:
        return jsonify({"error": "forbidden"}), 403

    # Gate 2: trade must be risk_approved
    if trade.status != "risk_approved":
        return jsonify({
            "ok":             False,
            "blocked":        True,
            "blocked_reason": f"trade_status_not_risk_approved:{trade.status}",
        }), 400

    # Gate 3: execution exchange must be mexc perpetual
    ex_ex  = (getattr(trade, "execution_exchange", "") or "").lower()
    ex_mkt = (getattr(trade, "execution_market",   "") or "").lower()
    if ex_ex != "mexc" or ex_mkt != "perpetual":
        return jsonify({
            "ok":             False,
            "blocked":        True,
            "blocked_reason": f"execution_exchange_not_mexc_perpetual:{ex_ex}/{ex_mkt}",
        }), 400

    # Gate 4: Risk Guard must have approved
    rg = _json_loads_safe(getattr(trade, "risk_guard_json", None), {})
    if not rg.get("approved"):
        return jsonify({
            "ok":             False,
            "blocked":        True,
            "blocked_reason": "risk_guard_not_approved",
            "risk_guard":     rg,
        }), 400

    # Gate 5: mode must be demo
    if (getattr(trade, "mode", "") or "").lower() not in ("demo", "proposal_only"):
        return jsonify({
            "ok":             False,
            "blocked":        True,
            "blocked_reason": "trade_mode_not_demo",
        }), 400

    # Gate 6: validate order payload can be built
    row = _LMI.query.filter_by(id=trade.live_monitor_item_id).first()
    payload_r = _lm_prepare_demo_order_payload(trade, row=row)
    if not payload_r["ok"]:
        return jsonify({
            "ok":             False,
            "blocked":        True,
            "blocked_reason": payload_r.get("blocked_reason", "payload_build_failed"),
        }), 400

    # Gate 7 (terminal safety gate): MEXC demo order submit endpoint not confirmed.
    # Per Phase 9.6 spec: if MEXC demo order submission is uncertain, do not submit.
    # Return blocked — payload is valid but submission is not attempted.
    return jsonify({
        "ok":             False,
        "blocked":        True,
        "blocked_reason": "mexc_demo_submit_not_confirmed",
        "message": (
            "MEXC demo perpetual order submission endpoint is not confirmed. "
            "All safety gates passed. Payload is ready but no order was placed. "
            "This block will be lifted when MEXC demo order submit is verified."
        ),
        "payload_preview": {
            "symbol":        payload_r.get("mx_symbol"),
            "order_side":    payload_r.get("order_side"),
            "notional_usdt": payload_r.get("notional_usdt"),
            "leverage":      payload_r.get("leverage"),
        },
        "trade": _lm_trade_to_dict(trade),
    })


# ── Phase 9.7: Demo Trade Sync + Post-Trade Review Endpoints ──────────────────


@app.route("/api/live-monitor/trades/<trade_uid>/demo-sync", methods=["POST"])
@login_required
def api_lm_trade_demo_sync(trade_uid):
    """Sync demo trade status from MEXC. Read-only — never places or cancels orders."""
    uid, _ = _current_user_id_and_user()
    if not uid:
        return jsonify({"error": "no_user"}), 401

    from models import db as _db, LiveMonitorTrade as _LMT
    trade = _LMT.query.filter_by(trade_uid=trade_uid).first()
    if not trade:
        return jsonify({"error": "not_found"}), 404
    if trade.user_id != uid:
        return jsonify({"error": "forbidden"}), 403

    # Endpoint-level safety: only demo_exchange trades may be synced
    if (getattr(trade, "mode", "") or "") != "demo_exchange":
        return jsonify({
            "ok":             False,
            "blocked":        True,
            "blocked_reason": f"trade_mode_not_demo_exchange:{trade.mode}",
        }), 400

    # Endpoint-level safety: only submitted/open trades are syncable
    if trade.status not in ("submitted", "open"):
        return jsonify({
            "ok":             False,
            "blocked":        True,
            "blocked_reason": f"trade_status_{trade.status}_not_syncable",
        }), 400

    sync_r = _lm_sync_mexc_demo_trade(trade)
    if sync_r.get("blocked"):
        return jsonify({
            "ok":             False,
            "blocked":        True,
            "blocked_reason": sync_r.get("blocked_reason"),
        }), 400
    if not sync_r.get("ok"):
        return jsonify({
            "ok":    False,
            "error": sync_r.get("error"),
            "synced": False,
        }), 502

    if sync_r.get("synced"):
        try:
            _db.session.commit()
        except Exception as e:
            _db.session.rollback()
            return jsonify({"error": str(e)}), 500

    return jsonify({
        "ok":      True,
        "synced":  sync_r.get("synced"),
        "changes": sync_r.get("changes"),
        "note":    sync_r.get("note"),
        "trade":   _lm_trade_to_dict(trade),
    })


@app.route("/api/live-monitor/trades/<trade_uid>/post-trade-review", methods=["POST"])
@login_required
def api_lm_trade_post_trade_review(trade_uid):
    """Build and store deterministic post-trade review. No AI, no exchange calls."""
    uid, _ = _current_user_id_and_user()
    if not uid:
        return jsonify({"error": "no_user"}), 401

    from models import db as _db, LiveMonitorTrade as _LMT
    trade = _LMT.query.filter_by(trade_uid=trade_uid).first()
    if not trade:
        return jsonify({"error": "not_found"}), 404
    if trade.user_id != uid:
        return jsonify({"error": "forbidden"}), 403

    _LM_REVIEWABLE_STATUSES = {"closed", "failed", "risk_rejected", "cancelled"}
    if trade.status not in _LM_REVIEWABLE_STATUSES:
        return jsonify({
            "ok":             False,
            "blocked":        True,
            "blocked_reason": f"trade_status_not_reviewable:{trade.status}",
        }), 400

    review = _lm_build_post_trade_review(trade)

    trade.post_trade_review_json = _json_dumps_safe(review)
    if review.get("outcome") and review["outcome"] != "unknown":
        trade.outcome = review["outcome"]
    trade.updated_at = datetime.now(timezone.utc)

    try:
        _db.session.commit()
    except Exception as e:
        _db.session.rollback()
        return jsonify({"error": str(e)}), 500

    return jsonify({
        "ok":     True,
        "review": review,
        "trade":  _lm_trade_to_dict(trade),
    })


# mobile UA fragments — keep small, case-insensitive
_MOBILE_UA_KEYS = (
    "iphone", "ipod", "android", "blackberry", "opera mini",
    "windows phone", "iemobile", "mobile safari", "mobile/",
)


def _is_mobile_ua(ua):
    """Heuristic mobile detection by User-Agent string. Modern iPadOS reports
    as desktop Mac — that's intentional (an iPad has the screen for the
    desktop view)."""
    ua = (ua or "").lower()
    return any(k in ua for k in _MOBILE_UA_KEYS)


def _detect_mobile(req):
    """Decide if a request should get the mobile UI.

    Priority:
      1. Sec-CH-UA-Mobile Client Hint (the reliable signal). Chrome &
         Edge — including Android Chrome in "Desktop site" mode —
         flip this from "?1" (mobile) to "?0" (desktop). Safari /
         Firefox don't send it, so we fall back.
      2. User-Agent sniff.
    """
    ch = (req.headers.get("Sec-CH-UA-Mobile") or "").strip().lower()
    if ch == "?0":
        return False     # explicit desktop request
    if ch == "?1":
        return True      # explicit mobile request
    return _is_mobile_ua(req.headers.get("User-Agent"))


_VIEW_COOKIE = "zyni-view"
_VIEW_COOKIE_MAX_AGE = 60 * 60 * 24 * 365   # 1 year


@app.route("/")
def index():
    # Anonymous → homepage (login screen). No cookies needed here.
    if not session.get("logged_in"):
        return render_template("homepage.html")

    username = session.get("username", "Trader")
    display_name = " ".join(w.capitalize() for w in username.strip().split())

    # Priority: explicit ?view= (and remember it via cookie) →
    # cookie set by a previous ?view= → UA sniff.
    forced = (request.args.get("view") or "").lower()
    set_cookie_to = None
    if forced in ("mobile", "desktop"):
        mobile = forced == "mobile"
        set_cookie_to = forced
    else:
        cookie_v = (request.cookies.get(_VIEW_COOKIE) or "").lower()
        if cookie_v in ("mobile", "desktop"):
            mobile = cookie_v == "mobile"
        else:
            mobile = _detect_mobile(request)

    tmpl = "index.html" if mobile else "preview.html"
    resp = make_response(render_template(tmpl, username=display_name))
    if set_cookie_to:
        resp.set_cookie(
            _VIEW_COOKIE, set_cookie_to,
            max_age=_VIEW_COOKIE_MAX_AGE, samesite="Lax", path="/",
        )
    return resp


@app.route("/preview")
def preview():
    # Login-gated preview of the rebuilt screener UI (live /api/scan data).
    if session.get("logged_in"):
        username = session.get("username", "Trader")
        display_name = " ".join(w.capitalize() for w in username.strip().split())
        return render_template("preview.html", username=display_name)
    return redirect(url_for("index"))


@app.route("/api/pairs")
@login_required
def api_pairs():
    limit = int(request.args.get("limit", 200))
    exchange = request.args.get("exchange", "binance").lower()
    market = request.args.get("market", "perpetual")
    return jsonify(get_pairs_exchange(exchange, market)[:limit])


@app.route("/api/my-permissions")
@login_required
def api_my_permissions():
    username = session.get("username", "")
    try:
        user = _DBUser.query.filter_by(username=username).first()
        if not user:
            return jsonify({"is_admin": False, "daily_tokens": 500, "tokens_remaining": 500,
                            "allowed_tabs": ["scan","pairs","settings"],
                            "allowed_modules": ["ob","fvg","fib","bias"],
                            "allowed_exchanges": ["binance"], "allowed_timeframes": ["1h","4h"]})
        perms = get_user_permissions(user)
        maint = _GlobalSetting.query.filter_by(key="maintenance_mode").first()
        perms["maintenance_mode"] = (maint.value == "true") if maint else False
        perms["username"] = user.username
        perms["role"]     = user.role
        return jsonify(perms)
    except Exception as e:
        return jsonify({"error": str(e), "is_admin": False, "daily_tokens": 500,
                        "tokens_remaining": 500, "allowed_tabs": ["scan","pairs","settings"],
                        "allowed_modules": ["ob","fvg","fib","bias"],
                        "allowed_exchanges": ["binance"], "allowed_timeframes": ["1h","4h"]})


@app.route("/guest-access")
def guest_access():
    """Auto-login guest by device fingerprint."""
    import hashlib, secrets
    ip = request.headers.get("X-Forwarded-For", request.remote_addr or "").split(",")[0].strip()
    ua = request.headers.get("User-Agent", "")
    lang = request.headers.get("Accept-Language", "")
    fp_raw = f"{ip}|{ua}|{lang}"
    fp = hashlib.sha256(fp_raw.encode()).hexdigest()
    try:
        from models import GuestDevice
        allow = True
        try:
            gs = _GlobalSetting.query.filter_by(key="allow_guest_access").first()
            if gs and gs.value == "false":
                allow = False
        except Exception:
            pass
        if not allow:
            return redirect(url_for("index"))

        gd = GuestDevice.query.filter_by(device_fingerprint=fp).first()
        if gd:
            user = _DBUser.query.get(gd.user_id)
            if user and user.status == "active":
                gd.last_seen_at = datetime.now(timezone.utc)
                db.session.commit()
                session["logged_in"] = True
                session["username"]  = user.username
                session["user_id"]   = user.id
                return redirect(url_for("index"))

        # Create new guest
        rnd = secrets.token_hex(3)
        gname = f"guest_{rnd}"
        gpwd  = secrets.token_urlsafe(16)
        new_user = _DBUser(username=gname, role="guest", status="active")
        new_user.set_password(gpwd)
        db.session.add(new_user)
        db.session.flush()
        gd_new = GuestDevice(device_fingerprint=fp, user_id=new_user.id,
                              ip_address=ip, user_agent=ua)
        db.session.add(gd_new)
        db.session.commit()
        session["logged_in"] = True
        session["username"]  = gname
        session["user_id"]   = new_user.id
        session["is_guest"]  = True
        session["guest_id"]  = gname
        return redirect(url_for("index"))
    except Exception as e:
        print(f"[GUEST-ACCESS] Error: {e}")
        return redirect(url_for("index"))


def _get_scan_user_id():
    """Return DB user_id for token tracking, or None if not applicable."""
    uid = session.get("user_id")
    if not uid:
        try:
            u = _DBUser.query.filter_by(username=session.get("username", "")).first()
            if u:
                uid = u.id
        except Exception:
            pass
    return uid


def _daily_limit_response():
    """Standardized token-limit response consumed by the mobile/desktop popups.
    Always HTTP 429 with a machine code, human message and next-reset ISO."""
    nxt = (datetime.now(timezone.utc) + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return jsonify({
        "error": "daily_limit_reached",
        "message": "Daily scan tokens exhausted. Resets at midnight UTC.",
        "resetAt": nxt.isoformat(),
    }), 429


def _check_and_get_token_user():
    """Return (db_user, user_id) if token check passes, or (None, None) to skip, raises 429 on limit."""
    uid = _get_scan_user_id()
    if not uid:
        return None, None
    try:
        db_user = _DBUser.query.get(uid)
        if not db_user or db_user.is_admin:
            return None, None
        if not check_tokens(db_user):
            return "limit", uid
        return db_user, uid
    except Exception:
        return None, None


@app.route("/api/scan", methods=["POST"])
@login_required
def api_scan():
    err = _guest_tab_check("scan")
    if err is not None: return err

    # Token check
    _scan_user_id = None
    try:
        _scan_db_user = _DBUser.query.filter_by(username=session.get("username", "")).first()
        if _scan_db_user and not _scan_db_user.is_admin:
            if not check_tokens(_scan_db_user):
                return _daily_limit_response()
            _scan_user_id = _scan_db_user.id
    except Exception:
        pass

    payload = request.get_json(force=True) or {}
    settings = parse_settings(payload.get("settings", {}))
    market = payload.get("market", "perpetual")
    exchange = payload.get("exchange", "binance").lower()
    symbols = payload.get("symbols", [])
    mode = payload.get("scanMode", "selected")
    pairs_per_cycle = int(payload.get("pairsPerCycle", 20))

    # FIX BUG 1: Full Market ALWAYS ignores selectedPairs
    # even if frontend accidentally sends them
    if mode == "market":
        all_pairs = [p["symbol"] for p in get_pairs_exchange(exchange, market)]
        if payload.get("roundRobin", True):
            start = ROUND_ROBIN_STATE["index"]
            chosen = all_pairs[start:start + pairs_per_cycle]
            if len(chosen) < pairs_per_cycle:
                chosen += all_pairs[:max(0, pairs_per_cycle - len(chosen))]
            ROUND_ROBIN_STATE["index"] = (start + pairs_per_cycle) % max(len(all_pairs), 1)
            symbols = chosen
        else:
            symbols = all_pairs[:pairs_per_cycle]
    else:
        # Selected Pairs mode — use only what frontend sent
        # If nothing selected, fall back to top pairs
        if not symbols:
            symbols = [p["symbol"] for p in get_pairs_exchange(exchange, market)[:pairs_per_cycle]]

    btc_closes = None
    try:
        btc = get_klines_exchange("BTCUSDT", settings["tf"], 300, market, exchange)
        btc_closes = [x["close"] for x in btc]
    except Exception:
        btc_closes = None

    fib_tf = settings.get("fibTf", settings["tf"]) if settings.get("useFibModule") else None
    fetch_fib_separately = fib_tf and fib_tf != settings["tf"]

    scan_limit = _scan_kline_limit()

    def scan_symbol(sym):
        try:
            candles = get_klines_exchange(sym, settings["tf"], scan_limit, market, exchange)
            fib_candles = None
            if fetch_fib_separately:
                try:
                    fib_candles = get_klines_exchange(sym, fib_tf, scan_limit, market, exchange)
                except Exception:
                    pass
            result = analyze_pair(sym, candles, settings["tf"], settings, btc_closes, fib_candles=fib_candles)
            if result and candles:
                # Backend visibility for scanner candle depth (no UI yet).
                result["candleLimitUsed"] = scan_limit
                result["candlesCount"]    = len(candles)
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
        if sig == "FVG":     return "FVG" in setups
        if sig == "OB":      return bool(setups & {"OB_APPROACH", "OB_CONSOL"})
        if sig == "FIB":     return bool(setups & {"FIB_APPROACH", "FIB_REACTION"})
        if sig == "BREAKER": return bool(setups & {"BREAKER_APPROACH", "BREAKER_INSIDE"})
        return False

    if filter_mode == "filter" and required_signals:
        # AND logic — all checked signals must be present
        results = [r for r in results if all(has_signal(r, s) for s in required_signals)]
    elif filter_mode == "match" and checked_signals:
        # OR logic — at least one checked signal must be present
        results = [r for r in results if any(has_signal(r, s) for s in checked_signals)]

    if _scan_user_id:
        try:
            consume_tokens(_scan_user_id, len(symbols))
        except Exception:
            pass

    # ── Intelligence logging hook — NEVER blocks scan response ────────────
    try:
        from signal_extractor import extract_zone_signals_from_api_scan_result
        from signal_logger import log_normalized_signal as _log_signal

        # Phase 6C cleanup: OB-only Intelligence logging.
        # Breaker (bb), FVG, and Fib Confluence are paused from Intelligence
        # counting. Scanner still detects and displays them on the frontend;
        # they are simply not logged as SignalEvent rows.
        _intel_allowed: set = {"ob"}

        _intel_extracted = _intel_logged = _intel_dupes = _intel_skipped = _intel_errors = 0

        for _intel_r in results:
            try:
                _intel_sigs = extract_zone_signals_from_api_scan_result(
                    _intel_r, exchange=exchange, allowed_modules=_intel_allowed
                )
                if not _intel_sigs:
                    _intel_skipped += 1
                    continue
                for _intel_norm in _intel_sigs:
                    _intel_extracted += 1
                    _intel_lres = _log_signal(_intel_norm, source="live")
                    if _intel_lres.get("logged"):
                        _intel_logged += 1
                    elif _intel_lres.get("reason") == "duplicate":
                        _intel_dupes += 1
                    else:
                        _intel_errors += 1
            except Exception as _intel_re:
                _intel_errors += 1
                print(f"[Intel Hook api_scan] result error: {_intel_re}")

        print(
            f"[Intel Hook api_scan] ob_only_mode "
            f"extracted={_intel_extracted} logged={_intel_logged} "
            f"dupes={_intel_dupes} skipped={_intel_skipped} errors={_intel_errors}"
        )
    except Exception as _intel_hook_err:
        print(f"[Intel Hook api_scan] error: {_intel_hook_err}")
    # ── end Intelligence hook ──────────────────────────────────────────────

    return jsonify({
        "scanned": len(symbols),
        "results": results,
        "nextRoundRobinIndex": ROUND_ROBIN_STATE["index"],
    })


# ─── Compressed scan confluence helpers ────────────────────────────────────

def _box_zone_relation(zone_lo, zone_hi, box_low, box_high, price, near_pct=0.50):
    """Return (relation_str, dist_pct) for zone vs compression box, or (None, None) if irrelevant."""
    if zone_lo >= box_low and zone_hi <= box_high:
        return "inside_box", 0.0
    if zone_lo < box_high and zone_hi > box_low:
        return "overlaps_box", 0.0
    gap = (box_low - zone_hi) if zone_hi < box_low else (zone_lo - box_high)
    dist_pct = (gap / max(price, 1e-10)) * 100.0
    if dist_pct <= near_pct:
        return "near_box", round(dist_pct, 3)
    return None, None


def _box_ob_confluence(obs_list, box_low, box_high, price):
    relevant = []
    for ob in (obs_list or []):
        rel, dist = _box_zone_relation(ob["bottom"], ob["top"], box_low, box_high, price)
        if rel is None:
            continue
        entry = {
            "direction": ob.get("type", "bullish"),
            "zoneLow": round(ob["bottom"], 6),
            "zoneHigh": round(ob["top"], 6),
            "distancePct": dist,
            "relation": rel,
            "touches": ob.get("touches"),
            "isVirgin": ob.get("isVirgin"),
            "strengthPct": round(
                (ob.get("buyVolume", 0) / max(ob.get("volume", 1e-10), 1e-10)) * 100, 1
            ) if ob.get("volume") else None,
        }
        relevant.append((dist, entry))
    if not relevant:
        return {"has": False, "count": 0, "nearest": None}
    relevant.sort(key=lambda x: x[0])
    return {"has": True, "count": len(relevant), "nearest": relevant[0][1]}


def _box_fvg_confluence(fvgs_list, box_low, box_high, price):
    relevant = []
    for fvg in (fvgs_list or []):
        rel, dist = _box_zone_relation(fvg["bottom"], fvg["top"], box_low, box_high, price)
        if rel is None:
            continue
        if fvg.get("untouched"):
            status = "UNTOUCHED"
        elif fvg.get("mitigated"):
            status = "FILLED"
        elif fvg.get("touches", 0) > 0:
            status = "TOUCHED"
        else:
            status = "UNKNOWN"
        entry = {
            "direction": fvg.get("direction", "bullish"),
            "zoneLow": round(fvg["bottom"], 6),
            "zoneHigh": round(fvg["top"], 6),
            "distancePct": dist,
            "relation": rel,
            "status": status,
            "age": fvg.get("age"),
        }
        relevant.append((dist, entry))
    if not relevant:
        return {"has": False, "count": 0, "nearest": None}
    relevant.sort(key=lambda x: x[0])
    return {"has": True, "count": len(relevant), "nearest": relevant[0][1]}


def _box_fib_confluence(o, h, l, c, v, tf, box_low, box_high):
    price = c[-1]
    fib = find_active_fib_leg_v2(o, h, l, c, v, tf=tf)
    if not fib or "levels" not in fib:
        return {"has": False}
    atr_vals = calc_atr(h, l, c, 14)
    active_names = get_single_active_fib_level(fib, h, l, c, tf, tolerance_pct=0.5, atr_values=atr_vals)
    check_names = active_names if active_names else list(fib["levels"].keys())
    target = {"0.5", "0.618", "0.705", "0.786"}
    best = None
    best_dist = float("inf")
    for name in check_names:
        if name not in target or name not in fib["levels"]:
            continue
        lp = fib["levels"][name]
        rel, dist = _box_zone_relation(lp, lp, box_low, box_high, price)
        if rel is None:
            continue
        if dist < best_dist:
            best_dist = dist
            best = {
                "level": name,
                "price": round(lp, 6),
                "distancePct": round(dist, 3),
                "relation": rel,
                "legDirection": "bullish" if fib.get("bullish") else "bearish",
                "legA": round(fib.get("a", 0), 6),
                "legB": round(fib.get("b", 0), 6),
            }
    if best is None:
        return {"has": False}
    return {"has": True, **best}


def _compressed_action_plan(box_location, trade_score, compression_score,
                             atr_state, vol_state, ob_conf, fvg_conf, box_low, box_high):
    atr_ok = atr_state in ("strong_contraction", "normal")
    vol_ok = vol_state in ("drying", "normal")
    nearest_ob = ob_conf.get("nearest") or {}
    nearest_fvg = fvg_conf.get("nearest") or {}
    has_bull_ob = ob_conf.get("has") and nearest_ob.get("direction") == "bullish"
    has_bear_ob = ob_conf.get("has") and nearest_ob.get("direction") == "bearish"
    has_bull_fvg = fvg_conf.get("has") and nearest_fvg.get("direction") == "bullish"
    has_bear_fvg = fvg_conf.get("has") and nearest_fvg.get("direction") == "bearish"

    if box_location == "near_high" and trade_score >= 75 and atr_ok and vol_ok:
        return {
            "state": "upside_breakout_ready",
            "bias": "bullish",
            "entryTrigger": f"Wait for closed candle above {round(box_high, 6)}",
            "invalidation": "Invalid if candle closes back inside box after breakout",
            "notes": ["Price compressed near box high", "Watch for volume expansion on breakout"],
        }
    if box_location == "near_low" and trade_score >= 75 and atr_ok and vol_ok:
        return {
            "state": "downside_breakout_ready",
            "bias": "bearish",
            "entryTrigger": f"Wait for closed candle below {round(box_low, 6)}",
            "invalidation": "Invalid if candle closes back inside box after breakdown",
            "notes": ["Price compressed near box low", "Watch for volume expansion on breakdown"],
        }
    if box_location == "near_low" and (has_bull_ob or has_bull_fvg) and trade_score >= 65:
        return {
            "state": "bullish_rejection_watch",
            "bias": "bullish",
            "entryTrigger": "Wait for bullish rejection candle from box low / demand zone",
            "invalidation": "Invalid if candle closes below demand zone",
            "notes": ["Bullish confluence near box low", "Look for rejection wick or engulfing"],
        }
    if box_location == "near_high" and (has_bear_ob or has_bear_fvg) and trade_score >= 65:
        return {
            "state": "bearish_rejection_watch",
            "bias": "bearish",
            "entryTrigger": "Wait for bearish rejection candle from box high / supply zone",
            "invalidation": "Invalid if candle closes above supply zone",
            "notes": ["Bearish confluence near box high", "Look for rejection wick or bearish engulfing"],
        }
    if compression_score >= 60:
        return {
            "state": "compression_wait",
            "bias": "neutral",
            "entryTrigger": "Wait for directional breakout or rejection confirmation",
            "invalidation": "N/A — no active trigger yet",
            "notes": ["Compression building", "Monitor for directional commitment"],
        }
    return {
        "state": "avoid_weak_compression",
        "bias": "neutral",
        "entryTrigger": "Skip — compression quality insufficient",
        "invalidation": "N/A",
        "notes": ["Low compression quality score"],
    }


@app.route("/api/compressed_scan", methods=["POST"])
@login_required
def api_compressed_scan():
    err = _guest_tab_check("compressed")
    if err is not None: return err
    _tok_user, _tok_uid = _check_and_get_token_user()
    if _tok_user == "limit":
        return _daily_limit_response()
    payload = request.get_json(force=True) or {}

    # Always define exchange first so it is available regardless of symbol path
    exchange = payload.get("exchange", "binance").lower()

    # Input validation
    VALID_TF = {"15m", "30m", "1h", "4h", "1d"}
    tf = payload.get("timeframe", "1h")
    if tf not in VALID_TF:
        return jsonify({"error": "invalid_input", "message": f"timeframe must be one of {sorted(VALID_TF)}"}), 400

    market = payload.get("market", "perpetual")
    if market not in ("spot", "perpetual"):
        return jsonify({"error": "invalid_input", "message": "market must be spot or perpetual"}), 400

    try:
        lookback = int(payload.get("lookback", 12))
        if not (5 <= lookback <= 80):
            return jsonify({"error": "invalid_input", "message": "lookback must be between 5 and 80"}), 400
    except (ValueError, TypeError):
        return jsonify({"error": "invalid_input", "message": "lookback must be an integer"}), 400

    try:
        max_pct = float(payload.get("maxPct", 2.0))
        if not (0.1 <= max_pct <= 20):
            return jsonify({"error": "invalid_input", "message": "maxPct must be between 0.1 and 20"}), 400
    except (ValueError, TypeError):
        return jsonify({"error": "invalid_input", "message": "maxPct must be a number"}), 400

    # Use passed symbols if provided; never overwrite them with fallback
    raw_symbols = payload.get("symbols") or []
    if raw_symbols:
        normed = []
        for _s in raw_symbols:
            _s = str(_s).strip().upper().replace("/", "").replace("-", "").replace("_", "")
            if _s and not _s.endswith("USDT"):
                _s += "USDT"
            if _s:
                normed.append(_s)
        symbols = list(dict.fromkeys(normed))
    else:
        symbols = [p["symbol"] for p in get_pairs_exchange(exchange, market)[:80]]

    print(f"[DEBUG] compressed_scan exchange={exchange} market={market} tf={tf} lookback={lookback} max_pct={max_pct} symbols_count={len(symbols)}")

    results = []
    errors = 0
    for sym in symbols:
        try:
            kl = get_klines_exchange(sym, tf, max(160, lookback + 20), market, exchange)
            if not kl or len(kl) < lookback + 2:
                errors += 1
                continue
            # Exclude the currently forming candle — detect compression on closed candles only
            closed_kl = kl[:-1]
            h = [x["high"] for x in closed_kl]
            l = [x["low"] for x in closed_kl]
            c = [x["close"] for x in closed_kl]
            v = [x["volume"] for x in closed_kl]
            o = [x["open"] for x in closed_kl]
            ok, info = detect_compression(h, l, c, lookback, max_pct)
            if not ok:
                continue

            price = c[-1]
            box_high = round(info["high"], 6)
            box_low = round(info["low"], 6)
            box_range = max(box_high - box_low, 1e-10)
            box_mid = round((box_high + box_low) / 2, 6)
            range_pct = round(info["rangePct"], 2)

            # Close-only range — separates body compression from wick noise
            recent_close_high = max(c[-lookback:])
            recent_close_low = min(c[-lookback:])
            close_range_pct = round(((recent_close_high - recent_close_low) / max(price, 1e-10)) * 100, 2)

            # Price position inside box, clamped 0–100
            price_pos = round(max(0.0, min(100.0, ((price - box_low) / box_range) * 100)), 1)

            # Distance to box edges
            dist_to_high = round(((box_high - price) / max(price, 1e-10)) * 100, 2)
            dist_to_low = round(((price - box_low) / max(price, 1e-10)) * 100, 2)

            # Box location
            if price_pos >= 70:
                box_location = "near_high"
            elif price_pos <= 30:
                box_location = "near_low"
            else:
                box_location = "middle"

            # ATR contraction — ATR(14) on closed candles
            atr_vals = calc_atr(h, l, c, 14)
            current_atr = next((av for av in reversed(atr_vals) if av is not None), None)
            prior_atr_vals = [av for av in atr_vals[:-1] if av is not None]
            if current_atr is not None and len(prior_atr_vals) >= 10:
                avg_atr = float(np.mean(prior_atr_vals[-50:]))
                atr_ratio = round(current_atr / max(avg_atr, 1e-10), 3)
                if atr_ratio <= 0.70:
                    atr_state = "strong_contraction"
                elif atr_ratio <= 1.10:
                    atr_state = "normal"
                else:
                    atr_state = "expanding"
            else:
                atr_ratio = None
                atr_state = "unknown"

            # Volume dry-up — compare recent lookback to prior lookback
            if len(v) >= 2 * lookback:
                recent_vol_avg = float(np.mean(v[-lookback:]))
                prior_vol_avg = float(np.mean(v[-2 * lookback:-lookback]))
                vol_dry_ratio = round(recent_vol_avg / max(prior_vol_avg, 1e-10), 3)
                if vol_dry_ratio <= 0.75:
                    vol_state = "drying"
                elif vol_dry_ratio <= 1.20:
                    vol_state = "normal"
                else:
                    vol_state = "expanding"
            else:
                vol_dry_ratio = None
                vol_state = "unknown"

            # Candles whose close sits inside the box
            recent_closes = c[-lookback:]
            inside_count = sum(1 for cl in recent_closes if box_low <= cl <= box_high)
            candles_inside_pct = round((inside_count / max(lookback, 1)) * 100, 1)

            # Compression score (0–100)
            score = 0.0
            # Tight high/low range: up to 30 pts — smaller = better
            score += 30.0 * max(0.0, 1.0 - range_pct / max(max_pct, 1e-10))
            # Tight close range relative to high/low range: up to 20 pts
            score += 20.0 * max(0.0, 1.0 - close_range_pct / max(range_pct, 1e-10))
            # ATR contraction: up to 20 pts
            if atr_state == "strong_contraction":
                score += 20.0
            elif atr_state == "normal":
                score += 10.0
            # Volume dry-up: up to 15 pts
            if vol_state == "drying":
                score += 15.0
            elif vol_state == "normal":
                score += 7.0
            # Clean inside box: up to 10 pts
            score += 10.0 * (candles_inside_pct / 100.0)
            # Price near edge: up to 5 pts
            score += 5.0 if box_location in ("near_high", "near_low") else 2.0
            compression_score = round(min(score, 100.0))

            # Grade
            if compression_score >= 85:
                compression_grade = "A+"
            elif compression_score >= 75:
                compression_grade = "A"
            elif compression_score >= 60:
                compression_grade = "B"
            else:
                compression_grade = "C"

            # Watch state
            if box_location == "near_high" and compression_score >= 70:
                watch_state = "upside_breakout_watch"
            elif box_location == "near_low" and compression_score >= 70:
                watch_state = "downside_breakout_watch"
            elif compression_score >= 60:
                watch_state = "compression_building"
            else:
                watch_state = "weak_compression"

            # OB confluence — wrapped individually so one failure never stops the scan
            try:
                _obs_list, _ = detect_obs(o, h, l, c, v, 5, 10, max_ob=8)
                ob_conf = _box_ob_confluence(_obs_list, box_low, box_high, price)
            except Exception:
                ob_conf = {"has": False, "count": 0, "nearest": None}

            try:
                _fvgs_list = detect_fvgs(o, h, l, c, v, tf)
                fvg_conf = _box_fvg_confluence(_fvgs_list, box_low, box_high, price)
            except Exception:
                fvg_conf = {"has": False, "count": 0, "nearest": None}

            try:
                fib_conf = _box_fib_confluence(o, h, l, c, v, tf, box_low, box_high)
            except Exception:
                fib_conf = {"has": False}

            # Confluence score (0–30)
            confluence_score = 0
            if ob_conf["has"]:
                confluence_score += 12
            if fvg_conf["has"]:
                confluence_score += 10
            if fib_conf["has"]:
                confluence_score += 8
            # Bonus: OB + FVG both overlap the box
            if ob_conf["has"] and fvg_conf["has"]:
                ob_rel = (ob_conf.get("nearest") or {}).get("relation", "")
                fvg_rel = (fvg_conf.get("nearest") or {}).get("relation", "")
                if ob_rel in ("inside_box", "overlaps_box") and fvg_rel in ("inside_box", "overlaps_box"):
                    confluence_score += 5
            # Bonus: all three confluences present
            if ob_conf["has"] and fvg_conf["has"] and fib_conf["has"]:
                confluence_score += 5
            confluence_score = min(confluence_score, 30)

            if confluence_score >= 22:
                confluence_grade = "Strong"
            elif confluence_score >= 12:
                confluence_grade = "Moderate"
            elif confluence_score > 0:
                confluence_grade = "Weak"
            else:
                confluence_grade = "None"

            trade_score = min(100, compression_score + confluence_score)
            if trade_score >= 90:
                trade_grade = "A+"
            elif trade_score >= 80:
                trade_grade = "A"
            elif trade_score >= 65:
                trade_grade = "B"
            else:
                trade_grade = "C"

            action_plan = _compressed_action_plan(
                box_location, trade_score, compression_score,
                atr_state, vol_state, ob_conf, fvg_conf, box_low, box_high,
            )

            sparkline = [float(c[i]) for i in range(max(0, len(c) - 24), len(c))]
            results.append({
                "symbol": sym,
                "price": price,
                "timeframe": tf,
                "rangePct": range_pct,
                "high": box_high,
                "low": box_low,
                "volume": v[-1],
                "sparkline": sparkline,
                "boxHigh": box_high,
                "boxLow": box_low,
                "boxMid": box_mid,
                "closeRangePct": close_range_pct,
                "pricePositionPct": price_pos,
                "distanceToHighPct": dist_to_high,
                "distanceToLowPct": dist_to_low,
                "boxLocation": box_location,
                "atrCompressionRatio": atr_ratio,
                "atrState": atr_state,
                "volumeDryRatio": vol_dry_ratio,
                "volumeState": vol_state,
                "candlesInsideBox": inside_count,
                "candlesInsideBoxPct": candles_inside_pct,
                "compressionScore": compression_score,
                "compressionGrade": compression_grade,
                "watchState": watch_state,
                "obConfluence": ob_conf,
                "fvgConfluence": fvg_conf,
                "fibConfluence": fib_conf,
                "confluenceScore": confluence_score,
                "confluenceGrade": confluence_grade,
                "tradeScore": trade_score,
                "tradeGrade": trade_grade,
                "actionPlan": action_plan,
            })
        except Exception as e:
            errors += 1
            print(f"[DEBUG] compressed_scan {sym} error: {e}")
            continue

    # Sort: best trade quality first
    results.sort(key=lambda x: (-x["tradeScore"], -x["compressionScore"], x["rangePct"]))
    print(f"[DEBUG] compressed_scan results={len(results)} errors={errors}")

    if _tok_uid:
        try: consume_tokens(_tok_uid, len(symbols))
        except Exception as _te: print(f"[Tokens] compressed: {_te}")

    return jsonify({
        "ok": True,
        "scanned": len(symbols),
        "results": results,
        "errors": errors,
        "timeframe": tf,
        "market": market,
        "exchange": exchange,
        "usedClosedCandles": True,
    })


@app.route("/api/trending_scan", methods=["POST"])
@login_required
def api_trending_scan():
    err = _guest_tab_check("trending")
    if err is not None: return err
    _tok_user, _tok_uid = _check_and_get_token_user()
    if _tok_user == "limit":
        return _daily_limit_response()
    payload = request.get_json(force=True) or {}
    tf = payload.get("timeframe", "1h")
    market = payload.get("market", "perpetual")
    mode = payload.get("mode", "movers")
    limit = int(payload.get("limit", 30))
    exchange = payload.get("exchange", "binance").lower()
    pairs = get_pairs_exchange(exchange, market)[:150]
    if mode == "movers":
        movers = sorted(pairs, key=lambda x: abs(x["changePct"]), reverse=True)[:limit]
        if _tok_uid:
            try: consume_tokens(_tok_uid, len(movers))
            except Exception as _te: print(f"[Tokens] trending: {_te}")
        return jsonify(movers)
    out = []
    for item in pairs[:80]:
        sym = item["symbol"]
        try:
            kl = get_klines_exchange(sym, tf, 120, market, exchange)
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
    if _tok_uid:
        try: consume_tokens(_tok_uid, len(pairs[:80]))
        except Exception as _te: print(f"[Tokens] trending: {_te}")
    return jsonify(out[:limit])


# Legacy status values still sent by old/cached frontends → new 3-status model.
_ATH_STATUS_ALIASES = {
    "current": "breaking_now",
    "recent": "made_within_window",
    "near": "near_level",
    "at_level_now": "breaking_now",
    "hit_within_window": "made_within_window",
}
_ATH_VALID_STATUS = {"breaking_now", "made_within_window", "near_level"}
_ATH_STATUS_RANK = {"breaking_now": 0, "made_within_window": 1, "near_level": 2}


def _ath_scan_state_key(exchange: str, market: str) -> str:
    user = session.get("username", "anon") or "anon"
    return f"{user}:{exchange}:{market}"


def _ath_window_label(window_hours: int) -> str:
    if window_hours >= 24 and window_hours % 24 == 0:
        return f"{window_hours // 24}D"
    return f"{window_hours}H"


@app.route("/api/ath_atl_scan", methods=["POST"])
@login_required
def api_ath_atl_scan():
    err = _guest_tab_check("ath_atl")
    if err is not None: return err
    _tok_user, _tok_uid = _check_and_get_token_user()
    if _tok_user == "limit":
        return _daily_limit_response()

    payload = request.get_json(force=True) or {}
    action = str(payload.get("action", "scan")).lower()
    mode = payload.get("mode", "both")                       # ath | atl | both
    status = str(payload.get("status", "breaking_now"))
    status = _ATH_STATUS_ALIASES.get(status, status)
    if status not in _ATH_VALID_STATUS:
        status = "breaking_now"
    market = payload.get("market", "perpetual")
    window_hours = max(1, int(payload.get("windowHours", 24)))
    # breakTolerancePct: how far inside the level still counts as a break.
    # nearPct: only used by near_level. They are kept strictly separate.
    break_tol_pct = float(payload.get("breakTolerancePct", 0.10))
    break_tol = max(0.0, break_tol_pct) / 100.0
    near_pct = float(payload.get("nearPct", 3.0))
    batch_size = int(payload.get("pairsPerBatch", payload.get("batchSize", payload.get("limit", 50))) or 50)
    batch_size = max(1, batch_size)
    exchange = payload.get("exchange", "binance").lower()

    state_key = _ath_scan_state_key(exchange, market)

    # ── Reset: clear cursor + accumulated results, start from the top ──
    if action == "reset":
        with _ath_atl_scan_lock:
            ATH_ATL_SCAN_STATE.pop(state_key, None)
        try:
            total_pairs = len(get_pairs_exchange(exchange, market))
        except Exception:
            total_pairs = 0
        return jsonify({
            "totalPairs": total_pairs, "scannedCount": 0,
            "currentBatchStart": 0, "currentBatchEnd": 0, "nextBatchStart": 0,
            "cursor": 0, "completedCycle": False,
            "results": [], "accumulatedResults": [], "found": 0,
            "exchange": exchange, "market": market, "windowHours": window_hours,
            "windowLabel": _ath_window_label(window_hours),
            "status": status, "mode": mode,
            "breakTolerancePct": break_tol_pct, "nearPct": near_pct,
            "reset": True,
        })

    # ── Phase 3: every supported exchange (Binance/Bybit/OKX/MEXC) has a
    # native daily-history adapter. If an adapter returns no history for a
    # given symbol, process_pair() skips that symbol — it never falls back
    # to another exchange or the other market. ──

    pairs = get_pairs_exchange(exchange, market)
    total_pairs = len(pairs)
    if total_pairs == 0:
        return jsonify({
            "totalPairs": 0, "scannedCount": 0,
            "currentBatchStart": 0, "currentBatchEnd": 0, "nextBatchStart": 0,
            "cursor": 0, "completedCycle": True,
            "results": [], "accumulatedResults": [], "found": 0,
            "exchange": exchange, "market": market, "windowHours": window_hours,
            "windowLabel": _ath_window_label(window_hours),
            "status": status, "mode": mode,
            "breakTolerancePct": break_tol_pct, "nearPct": near_pct,
        })

    with _ath_atl_scan_lock:
        st = ATH_ATL_SCAN_STATE.get(state_key)
        if not st:
            st = {"cursor": 0, "accumulated": {}}
            ATH_ATL_SCAN_STATE[state_key] = st
        start = int(st.get("cursor", 0))
        if start >= total_pairs:          # finished a full pass → loop around
            start = 0
        end = min(start + batch_size, total_pairs)
        st["cursor"] = end
        accumulated: Dict[str, Any] = st["accumulated"]

    batch_pairs = pairs[start:end]
    win_label = _ath_window_label(window_hours)

    def process_pair(p: Dict[str, Any]):
        sym = p.get("symbol")
        if not sym:
            return None
        try:
            recent_bars = window_hours                 # one 1h bar per hour
            fetch_bars = min(recent_bars + 30, 300)    # +buffer to bridge daily gap
            kl = get_klines_exchange(sym, "1h", fetch_bars, market, exchange)
            if not kl:
                return None

            lv = compute_window_ath_atl(sym, window_hours, kl, exchange, market)
            if not lv:
                return None
            previous_ath = lv["previous_ath"]
            previous_atl = lv["previous_atl"]
            window_high = lv["window_high"]
            window_low = lv["window_low"]
            days_history = lv["daily_bars"]
            if previous_ath <= 0 or previous_atl <= 0:
                return None

            # Live/current price from the 24h ticker (already fetched for the
            # pair list). Falls back to the last 1h close only if missing.
            live_price = safe_float(p.get("price"), 0.0)
            if live_price <= 0:
                live_price = kl[-1]["close"]
            volume = safe_float(p.get("quoteVolume"), 0.0)

            do_ath = mode in ("ath", "both")
            do_atl = mode in ("atl", "both")

            tags: List[str] = []
            include = False
            break_pct = 0.0
            dist_pct = 0.0

            if status == "breaking_now":
                if do_ath and live_price >= previous_ath * (1 - break_tol):
                    include = True
                    tags.append("ATH BREAKING NOW")
                    break_pct = (live_price - previous_ath) / previous_ath * 100.0
                if do_atl and live_price <= previous_atl * (1 + break_tol):
                    include = True
                    tags.append("ATL BREAKING NOW")
                    b = (previous_atl - live_price) / previous_atl * 100.0
                    break_pct = max(break_pct, b)

            elif status == "made_within_window":
                if do_ath and window_high >= previous_ath * (1 - break_tol):
                    include = True
                    tags.append(f"ATH MADE IN {win_label}")
                    break_pct = (window_high - previous_ath) / previous_ath * 100.0
                if do_atl and window_low <= previous_atl * (1 + break_tol):
                    include = True
                    tags.append(f"ATL MADE IN {win_label}")
                    b = (previous_atl - window_low) / previous_atl * 100.0
                    break_pct = max(break_pct, b)

            else:  # near_level — close to level but NOT broken. nearPct only.
                if do_ath:
                    broke_ath = live_price >= previous_ath * (1 - break_tol)
                    d = (previous_ath - live_price) / previous_ath * 100.0
                    if (not broke_ath) and 0 <= d <= near_pct:
                        include = True
                        tags.append("NEAR ATH")
                        dist_pct = d
                if do_atl:
                    broke_atl = live_price <= previous_atl * (1 + break_tol)
                    d = (live_price - previous_atl) / previous_atl * 100.0
                    if (not broke_atl) and 0 <= d <= near_pct:
                        include = True
                        tags.append("NEAR ATL")
                        dist_pct = d if dist_pct == 0 else min(dist_pct, d)

            if not include:
                return None

            c_all = [x["close"] for x in kl]
            sparkline = [float(c_all[i]) for i in range(max(0, len(c_all) - 24), len(c_all))]
            return {
                "symbol": sym,
                "price": fmt_price(live_price),
                "previousAth": fmt_price(previous_ath),
                "previousAtl": fmt_price(previous_atl),
                "windowHigh": fmt_price(window_high),
                "windowLow": fmt_price(window_low),
                "breakPct": round(break_pct, 3),
                "distancePct": round(dist_pct, 3),
                "tags": tags,
                "statusKind": status,
                "statusRank": _ATH_STATUS_RANK.get(status, 9),
                "windowHours": window_hours,
                "windowSelected": win_label,
                "marketType": market,
                "exchange": exchange,
                "volume": volume,
                "volumeFmt": fmt_vol(volume),
                "daysHistory": days_history,
                "sparkline": sparkline,
            }
        except Exception:
            traceback.print_exc()
            return None

    batch_results: List[Dict[str, Any]] = []
    workers = min(8, len(batch_pairs)) if batch_pairs else 1
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(process_pair, p) for p in batch_pairs]
        for fut in as_completed(futures):
            r = fut.result()
            if r:
                batch_results.append(r)

    # ── Accumulate until reset (newest wins on dup symbol) ──
    with _ath_atl_scan_lock:
        st = ATH_ATL_SCAN_STATE.get(state_key)
        if st is None:
            st = {"cursor": end, "accumulated": {}}
            ATH_ATL_SCAN_STATE[state_key] = st
        acc: Dict[str, Any] = st["accumulated"]
        for r in batch_results:
            acc[r["symbol"]] = r
        cursor = int(st.get("cursor", end))
        accumulated_list = list(acc.values())

    completed_cycle = end >= total_pairs

    def _sort_key(x: Dict[str, Any]):
        # 1) status priority  2) bigger break %  3) smaller distance %
        # 4) higher volume
        return (
            x.get("statusRank", 9),
            -float(x.get("breakPct", 0.0)),
            float(x.get("distancePct", 0.0)),
            -float(x.get("volume", 0.0)),
        )

    batch_results.sort(key=_sort_key)
    accumulated_list.sort(key=_sort_key)

    if _tok_uid:
        try: consume_tokens(_tok_uid, len(batch_pairs))
        except Exception as _te: print(f"[Tokens] ath_atl: {_te}")

    return jsonify({
        "totalPairs": total_pairs,
        "scannedCount": end,
        "currentBatchStart": start + 1 if batch_pairs else 0,
        "currentBatchEnd": end,
        "nextBatchStart": 0 if completed_cycle else end,
        "cursor": cursor,
        "completedCycle": completed_cycle,
        "results": batch_results,
        "accumulatedResults": accumulated_list,
        "found": len(accumulated_list),
        "batchFound": len(batch_results),
        "exchange": exchange,
        "market": market,
        "windowHours": window_hours,
        "windowLabel": win_label,
        "status": status,
        "mode": mode,
        "breakTolerancePct": break_tol_pct,
        "nearPct": near_pct,
    })


# ── Bias Shift Phase 2 helpers ────────────────────────────────────────────────

def _bias_normal_presets(bias_strength: str) -> dict:
    """Return candle-quality presets for Normal mode. Never forces detectionMode."""
    if bias_strength == "early":
        return {
            "prior_move_n": 3, "signal_search_n": 2, "min_prior_checks": 2,
            "min_wick_pct": 0.25, "min_body_pct": 0.15,
            "require_close_beyond_mid": True,
        }
    elif bias_strength == "strong":
        return {
            "prior_move_n": 5, "signal_search_n": 1, "min_prior_checks": 2,
            "min_wick_pct": 0.45, "min_body_pct": 0.25,
            "require_close_beyond_mid": True,
        }
    else:  # balanced
        return {
            "prior_move_n": 4, "signal_search_n": 1, "min_prior_checks": 2,
            "min_wick_pct": 0.35, "min_body_pct": 0.20,
            "require_close_beyond_mid": True,
        }


def _detect_prior_move(o: list, h: list, l: list, c: list, start: int, end: int, tf: str = "1d") -> dict:
    """Detect a clean directional drive in candles [start, end).
    Returns quality='clean' only when all 5 hard conditions pass per direction."""
    _empty: dict = {
        "direction": None, "quality": "none", "checksPassed": 0,
        "summary": "empty", "reasons": [],
        "netPct": 0.0, "greenCount": 0, "redCount": 0,
        "upCloseSteps": 0, "downCloseSteps": 0, "requiredMovePct": 0.0,
    }
    if end <= start:
        return _empty
    seg_o, seg_h, seg_l, seg_c = o[start:end], h[start:end], l[start:end], c[start:end]
    n = len(seg_c)
    if n < 1:
        return _empty

    # Timeframe-aware minimum net move threshold
    tf_lower = tf.lower()
    if   tf_lower in ("1d", "12h"): req_pct = 0.8
    elif tf_lower in ("4h", "6h"):  req_pct = 0.5
    else:                            req_pct = 0.3

    net_close      = seg_c[-1] - seg_c[0]
    net_pct        = abs(net_close) / seg_c[0] * 100 if seg_c[0] > 0 else 0.0
    green_count    = sum(1 for i in range(n) if seg_c[i] > seg_o[i])
    red_count      = n - green_count
    up_close_steps = sum(1 for i in range(1, n) if seg_c[i] > seg_c[i - 1])
    dn_close_steps = sum(1 for i in range(1, n) if seg_c[i] < seg_c[i - 1])
    prior_mid      = min(seg_l) + (max(seg_h) - min(seg_l)) / 2
    green_needed   = math.ceil(n * 0.60)
    steps_needed   = math.ceil((n - 1) * 0.60) if n >= 2 else 0

    # Evaluate all 5 hard conditions for each direction
    up_cond = [
        net_close > 0,
        green_count >= green_needed,
        up_close_steps >= steps_needed,
        net_pct >= req_pct,
        seg_c[-1] > prior_mid,
    ]
    dn_cond = [
        net_close < 0,
        red_count >= green_needed,
        dn_close_steps >= steps_needed,
        net_pct >= req_pct,
        seg_c[-1] < prior_mid,
    ]
    up_passed = sum(up_cond)
    dn_passed = sum(dn_cond)

    if up_passed == 5:
        direction, quality, checks_passed = "up", "clean", 5
        reasons = [
            f"netClose↑{net_pct:.2f}%",
            f"{green_count}/{n}green≥{green_needed}",
            f"closeSteps↑{up_close_steps}≥{steps_needed}",
            f"move≥{req_pct}%",
            "closedAboveMid",
        ]
    elif dn_passed == 5:
        direction, quality, checks_passed = "down", "clean", 5
        reasons = [
            f"netClose↓{net_pct:.2f}%",
            f"{red_count}/{n}red≥{green_needed}",
            f"closeSteps↓{dn_close_steps}≥{steps_needed}",
            f"move≥{req_pct}%",
            "closedBelowMid",
        ]
    else:
        direction, quality = None, "weak" if max(up_passed, dn_passed) > 0 else "none"
        checks_passed = max(up_passed, dn_passed)
        lead = "up" if up_passed >= dn_passed else "dn"
        reasons = [f"{lead}:{checks_passed}/5"]

    return {
        "direction":       direction,
        "quality":         quality,
        "checksPassed":    checks_passed,
        "summary":         f"{direction or 'none'} ({quality}·{checks_passed}/5)",
        "reasons":         reasons,
        "netPct":          round(net_pct, 4),
        "greenCount":      green_count,
        "redCount":        red_count,
        "upCloseSteps":    up_close_steps,
        "downCloseSteps":  dn_close_steps,
        "requiredMovePct": req_pct,
    }


def _detect_prior_move_adaptive(
    o: list, h: list, l: list, c: list,
    sig_idx: int, tf: str, base_n: int,
    bias_strength: str, bias_mode: str = "normal",
) -> dict:
    """Adaptive prior-drive detector.

    Tests multiple prior-window lengths before sig_idx, scores each candidate
    drive, and returns the highest-scoring accepted result.

    Acceptance thresholds:
      strong   -> quality=="clean",          score >= 85
      balanced -> quality in [clean, good],  score >= 70
      early    -> quality in [clean, good, impulse], score >= 60
    """
    _tf = tf.lower()
    if   _tf in ("1d", "12h"): req_pct = 0.8
    elif _tf in ("4h",  "6h"): req_pct = 0.5
    else:                       req_pct = 0.3

    # Window candidates per strength / mode
    if bias_mode == "expert":
        raw_windows = [base_n - 1, base_n, base_n + 1, base_n + 2]
    elif bias_strength == "strong":
        raw_windows = [base_n, base_n + 1]
    elif bias_strength == "early":
        raw_windows = [3, 4, 5, 6]
    else:  # balanced
        raw_windows = [3, 4, 5, 6, 7]

    # Clamp: min 3, max 8, must fit before sig_idx
    windows = sorted(set(
        w for w in raw_windows if 3 <= w <= 8 and sig_idx - w >= 0
    ))

    _null: dict = {
        "direction": None, "quality": "none", "accepted": False,
        "score": 0, "checksPassed": 0, "windowN": base_n,
        "priorStart": max(0, sig_idx - base_n), "priorEnd": sig_idx,
        "driveType": "none", "netPct": 0.0,
        "greenCount": 0, "redCount": 0,
        "upCloseSteps": 0, "downCloseSteps": 0,
        "requiredMovePct": req_pct, "maxPullbackPct": 0.0,
        "impulseFound": False, "reasons": ["no_window"], "summary": "no_window",
    }
    if not windows:
        return _null

    # Mode pullback limits (fib levels)
    if   bias_strength == "strong":  max_pb = 38.2
    elif bias_strength == "early":   max_pb = 61.8
    else:                            max_pb = 50.0

    candidates: list[dict] = []
    _any_choppy = False  # at least one window/direction was rejected for chop

    for w in windows:
        start   = sig_idx - w
        seg_o   = o[start:sig_idx]
        seg_h   = h[start:sig_idx]
        seg_l   = l[start:sig_idx]
        seg_c   = c[start:sig_idx]
        n_seg   = len(seg_c)
        if n_seg < 1:
            continue

        # ── Basic metrics ──────────────────────────────────────────────────────
        net_close       = seg_c[-1] - seg_c[0]
        net_pct         = abs(net_close) / seg_c[0] * 100 if seg_c[0] > 0 else 0.0
        green_count     = sum(1 for i in range(n_seg) if seg_c[i] > seg_o[i])
        red_count       = n_seg - green_count
        up_close_steps  = sum(1 for i in range(1, n_seg) if seg_c[i] > seg_c[i - 1])
        dn_close_steps  = sum(1 for i in range(1, n_seg) if seg_c[i] < seg_c[i - 1])
        total_range     = max(seg_h) - min(seg_l)
        prior_mid       = min(seg_l) + total_range / 2.0

        green_need_60   = math.ceil(n_seg * 0.60)
        steps_need_60   = math.ceil((n_seg - 1) * 0.60) if n_seg >= 2 else 0
        green_need_50   = math.ceil(n_seg * 0.50)
        steps_need_50   = math.ceil((n_seg - 1) * 0.50) if n_seg >= 2 else 0

        # Chop ratio: fraction of adjacent candles that flip color
        alt_count = sum(
            1 for i in range(1, n_seg)
            if (seg_c[i] > seg_o[i]) != (seg_c[i - 1] > seg_o[i - 1])
        )
        chop_ratio = alt_count / max(n_seg - 1, 1)

        # ── Displacement-candle detection (strongest in window) ────────────────
        avg_body = sum(abs(seg_c[i] - seg_o[i]) for i in range(n_seg)) / max(n_seg, 1)
        impulse_found_up  = False
        impulse_found_dn  = False
        best_disp_score   = 0.0
        for i in range(n_seg):
            rng_i  = seg_h[i] - seg_l[i]
            body_i = abs(seg_c[i] - seg_o[i])
            if rng_i <= 0:
                continue
            body_pct = body_i / rng_i
            if body_pct >= 0.55 and body_i >= avg_body * 1.3:
                upper35 = seg_l[i] + rng_i * 0.65
                lower35 = seg_l[i] + rng_i * 0.35
                ds = body_pct * (body_i / max(avg_body, 1e-9))
                if seg_c[i] > seg_o[i] and seg_c[i] >= upper35:
                    impulse_found_up = True
                    best_disp_score  = max(best_disp_score, ds)
                elif seg_c[i] < seg_o[i] and seg_c[i] <= lower35:
                    impulse_found_dn = True
                    best_disp_score  = max(best_disp_score, ds)

        # ── Pullback % for each direction ──────────────────────────────────────
        def _pb_up() -> float:
            peak_i = seg_h.index(max(seg_h))
            if peak_i >= n_seg - 1:
                return 0.0
            post_low   = min(seg_l[peak_i + 1:])
            drv_range  = max(seg_h) - min(seg_l[:peak_i + 1])
            return max(0.0, (max(seg_h) - post_low) / drv_range * 100) if drv_range > 0 else 0.0

        def _pb_dn() -> float:
            trough_i  = seg_l.index(min(seg_l))
            if trough_i >= n_seg - 1:
                return 0.0
            post_high  = max(seg_h[trough_i + 1:])
            drv_range  = max(seg_h[:trough_i + 1]) - min(seg_l)
            return max(0.0, (post_high - min(seg_l)) / drv_range * 100) if drv_range > 0 else 0.0

        # ── Evaluate each direction ────────────────────────────────────────────
        for direction in ("up", "down"):
            is_up     = direction == "up"
            net_ok    = net_close > 0 if is_up else net_close < 0
            if not net_ok:
                continue

            imp_found   = impulse_found_up if is_up else impulse_found_dn
            eff_req     = req_pct * 0.75 if imp_found else req_pct
            if net_pct < eff_req:
                continue

            color_count   = green_count if is_up else red_count
            close_steps   = up_close_steps if is_up else dn_close_steps
            pb_pct        = _pb_up() if is_up else _pb_dn()
            beyond_mid    = seg_c[-1] > prior_mid if is_up else seg_c[-1] < prior_mid
            color_ratio   = color_count / n_seg
            step_ratio    = close_steps / max(n_seg - 1, 1)

            # ── Quality determination ──────────────────────────────────────────
            is_choppy     = chop_ratio > 0.6 and net_pct < req_pct * 1.2

            # Clean: all 5 hard conditions
            clean_ok      = (
                net_ok and
                color_count >= green_need_60 and
                close_steps >= steps_need_60 and
                net_pct >= req_pct and
                beyond_mid
            )
            clean_checks  = sum([
                net_ok,
                color_count >= green_need_60,
                close_steps >= steps_need_60,
                net_pct >= req_pct,
                beyond_mid,
            ])

            # Good: mandatory + 2/3 soft
            soft_ok       = (
                net_ok and net_pct >= req_pct and not is_choppy
            )
            soft_passed   = sum([
                color_count >= green_need_50,
                close_steps >= steps_need_50,
                beyond_mid,
            ])
            good_ok       = soft_ok and soft_passed >= 2 and pb_pct <= max_pb

            # Impulse: displacement candle + correct net direction
            impulse_ok    = (
                imp_found and net_ok and
                net_pct >= req_pct * 0.75 and
                not is_choppy
            )

            if is_choppy and not imp_found:
                _any_choppy = True
                quality = "weak"
            elif clean_ok:
                quality = "clean"
            elif good_ok:
                # Downgrade if pullback too deep and no strong displacement
                if pb_pct > max_pb:
                    quality = "impulse" if (imp_found and best_disp_score >= 1.5) else "weak"
                else:
                    quality = "good"
            elif impulse_ok:
                quality = "impulse"
            else:
                quality = "weak"

            if quality == "weak":
                continue

            # ── Scoring ────────────────────────────────────────────────────────
            score = 0

            if quality == "impulse":
                # Impulse path: reward net move + displacement strength
                if   net_pct >= req_pct * 2.0: score += 35
                elif net_pct >= req_pct * 1.5: score += 30
                else:                           score += 22
                score += 20  # impulse-drive structure credit
                if beyond_mid:
                    score += 12
                score += min(15, int(best_disp_score * 8))
            else:
                # Clean / Good path: traditional structure scoring
                if   net_pct >= req_pct * 2.0: score += 30
                elif net_pct >= req_pct * 1.5: score += 25
                else:                           score += 22

                if   color_ratio >= 0.6: score += 20
                elif color_ratio >= 0.5: score += 17
                elif color_ratio >= 0.4: score += 11
                else:                    score += 5

                if   step_ratio >= 0.6: score += 20
                elif step_ratio >= 0.5: score += 17
                elif step_ratio >= 0.4: score += 11
                else:                   score += 5

                if beyond_mid:
                    score += 15
                    # Near-extreme bonus
                    q_range  = total_range * 0.25
                    extremal = (max(seg_h) - q_range if is_up else min(seg_l) + q_range)
                    if (is_up and seg_c[-1] >= extremal) or (not is_up and seg_c[-1] <= extremal):
                        score += 5

                if imp_found:
                    score += min(10, int(best_disp_score * 4))

            # Common penalties
            if   pb_pct > max_pb * 0.75: score -= 10
            elif pb_pct > max_pb * 0.50: score -= 5
            if chop_ratio > 0.4:
                score -= int(chop_ratio * 10)

            score = max(0, min(100, score))

            # ── Build candidate ────────────────────────────────────────────────
            color_word = "green" if is_up else "red"
            step_word  = "↑" if is_up else "↓"
            checks_val = clean_checks if quality == "clean" else (2 + soft_passed if quality == "good" else 2)
            reasons_c  = [
                f"net{step_word}{net_pct:.2f}%",
                f"{color_count}/{n_seg}{color_word}",
                f"steps{step_word}{close_steps}",
                f"score{score}",
            ]
            if imp_found:
                reasons_c.append("impulse")
            if pb_pct > 0:
                reasons_c.append(f"pb{pb_pct:.0f}%")

            candidate: dict = {
                "direction":      direction,
                "quality":        quality,
                "score":          score,
                "checksPassed":   checks_val,
                "windowN":        w,
                "priorStart":     start,
                "priorEnd":       sig_idx,
                "driveType":      quality,
                "netPct":         round(net_pct, 4),
                "greenCount":     green_count,
                "redCount":       red_count,
                "upCloseSteps":   up_close_steps,
                "downCloseSteps": dn_close_steps,
                "requiredMovePct":  req_pct,
                "maxPullbackPct":   round(pb_pct, 2),
                "impulseFound":    imp_found,
                "reasons":         reasons_c,
                "summary":         f"{direction}({quality}·{score}/100·w{w})",
            }

            candidates.append(candidate)

    # ── Mode-aware candidate selection ────────────────────────────────────────
    if not candidates:
        reject_reason = "choppy_drive" if _any_choppy else "no_drive_found"
        return {**_null, "reasons": [reject_reason], "rejectReason": reject_reason}

    def _allowed_for_mode(cand: dict) -> bool:
        q, s = cand["quality"], cand["score"]
        if   bias_strength == "strong":   return q == "clean"              and s >= 85
        elif bias_strength == "balanced": return q in ("clean", "good")    and s >= 70
        else:                              return q in ("clean", "good", "impulse") and s >= 60

    allowed = [x for x in candidates if _allowed_for_mode(x)]

    if allowed:
        best = max(allowed, key=lambda x: x["score"])
        best["accepted"] = True
        return best

    # No mode-allowed candidate — return highest raw candidate for diagnostics
    best_rejected = max(candidates, key=lambda x: x["score"])
    best_rejected["accepted"] = False
    best_rejected["rejectReason"] = "not_allowed_for_mode_or_score"
    return best_rejected


def _check_rejection_candle(
    o: list, h: list, l: list, c: list,
    idx: int, rejection_dir: str,
    min_wick: float, min_body: float,
    require_close_beyond_mid: bool,
) -> dict | None:
    """Return rejection-candle metrics or None if hard requirements fail."""
    rng = h[idx] - l[idx]
    if rng <= 0:
        return None
    body       = abs(c[idx] - o[idx])
    upper_wick = h[idx] - max(o[idx], c[idx])
    lower_wick = min(o[idx], c[idx]) - l[idx]
    body_pct   = body / rng
    uw_pct     = upper_wick / rng
    lw_pct     = lower_wick / rng
    is_green   = c[idx] > o[idx]
    mid        = l[idx] + rng / 2
    reasons: list[str] = []
    checks = 0

    if rejection_dir == "bearish":
        if is_green or uw_pct < min_wick or body_pct < min_body:
            return None
        if require_close_beyond_mid and c[idx] > mid:
            return None
        checks += 1
        reasons.append(f"red·↑wick{uw_pct*100:.0f}%·body{body_pct*100:.0f}%")
        if c[idx] < mid:
            checks += 1; reasons.append("closeBelowMid")
    else:
        if not is_green or lw_pct < min_wick or body_pct < min_body:
            return None
        if require_close_beyond_mid and c[idx] < mid:
            return None
        checks += 1
        reasons.append(f"green·↓wick{lw_pct*100:.0f}%·body{body_pct*100:.0f}%")
        if c[idx] > mid:
            checks += 1; reasons.append("closeAboveMid")

    return {
        "open": o[idx], "high": h[idx], "low": l[idx], "close": c[idx],
        "bodyPct":      round(body_pct * 100, 1),
        "upperWickPct": round(uw_pct   * 100, 1),
        "lowerWickPct": round(lw_pct   * 100, 1),
        "checksPassed": checks,
        "reasons":      reasons,
    }


def _suggested_conf_tf(tf: str) -> str:
    t = tf.lower()
    if t in ("1d", "12h"):  return "4H / 1H"
    if t in ("6h", "4h"):   return "1H / 15m"
    if t in ("2h", "1h"):   return "15m / 5m"
    if t in ("30m", "15m"): return "5m / 1m"
    return "lower TF"


def _is_better_setup(candidate: dict, current_best: dict) -> bool:
    """True if candidate is strictly better, now driven by score."""
    cs = candidate.get("score") or 0
    bs = current_best.get("score") or 0
    if cs != bs:
        return cs > bs
    return candidate.get("signalCandleOffset", 999) < current_best.get("signalCandleOffset", 999)


# ── Bias Shift Phase 3 — scoring & grading ────────────────────────────────────

def _score_bias_shift(
    prior_checks: int,
    rej_wick_pct: float,
    rej_body_pct: float,
    min_wick_pct: float,
    min_body_pct: float,
    close_beyond_mid: bool,
    confirmation_status: str,
    vol_spike: bool,
    volume_filter_mode: str,
    sig_offset: int,
    ob_found: bool = False,
    fvg_found: bool = False,
    fib_found: bool = False,
    confluence_required_passed: bool = False,
    prior_quality: str = "clean",
    prior_drive_score: int = 100,
) -> dict:
    """Return score 0-100 with breakdown list."""
    pts = 0
    bd: list[str] = []

    # Prior move contribution — scaled by drive quality
    if prior_quality == "impulse":
        pts += 15; bd.append("+15 prior move detected (impulse)")
        pts += 8;  bd.append("+8 prior impulse drive")
    elif prior_quality == "good":
        pts += 20; bd.append("+20 prior move detected")
        pts += 10; bd.append("+10 prior good drive")
    else:  # clean (default)
        pts += 20; bd.append("+20 prior move detected")
        if prior_checks >= 4:
            p = 15
        elif prior_checks >= 3:
            p = 10
        else:
            p = 5
        pts += p; bd.append(f"+{p} prior move checks ({prior_checks})")

    # Rejection candle present
    pts += 25; bd.append("+25 rejection candle valid")

    # Wick quality (relative to required threshold)
    wick_over = rej_wick_pct - round(min_wick_pct * 100)
    if wick_over >= 20:
        wp = 15
    elif wick_over >= 10:
        wp = 10
    else:
        wp = 5
    pts += wp; bd.append(f"+{wp} wick quality ({rej_wick_pct:.0f}%)")

    # Body quality (relative to required threshold)
    body_over = rej_body_pct - round(min_body_pct * 100)
    if body_over >= 20:
        bp = 10
    elif body_over >= 10:
        bp = 8
    else:
        bp = 5
    pts += bp; bd.append(f"+{bp} body quality ({rej_body_pct:.0f}%)")

    # Close beyond midpoint
    if close_beyond_mid:
        pts += 10; bd.append("+10 close beyond midpoint")

    # Confirmation
    if confirmation_status == "confirmed":
        pts += 10; bd.append("+10 confirmed by later candle")

    # Volume
    if vol_spike:
        pts += 5; bd.append("+5 volume spike")
        if volume_filter_mode == "required":
            pts += 5; bd.append("+5 volume required and passed")

    # Recency
    if sig_offset == 0:
        pts += 5; bd.append("+5 latest closed candle")
    elif sig_offset == 1:
        pts += 3; bd.append("+3 second-to-last candle")

    # ── Phase 4: confluence bonuses (OB/FVG/Fib — no-op until Phase 4 enabled) ──
    if ob_found:
        pts += 7; bd.append("+7 OB confluence")
    if fvg_found:
        pts += 7; bd.append("+7 FVG confluence")
    if fib_found:
        pts += 6; bd.append("+6 Fib confluence")
    if confluence_required_passed:
        pts += 5; bd.append("+5 required confluence passed")

    return {"score": min(pts, 100), "breakdown": bd}


def _grade_from_score(score: int) -> dict:
    if score >= 90:
        return {"grade": "A+", "gradeLabel": "A+ — Elite Bias Shift"}
    if score >= 80:
        return {"grade": "A",  "gradeLabel": "A — Strong Bias Shift"}
    if score >= 65:
        return {"grade": "B",  "gradeLabel": "B — Valid Bias Shift"}
    if score >= 50:
        return {"grade": "C",  "gradeLabel": "C — Watch Only"}
    return {"grade": "D",      "gradeLabel": "D — Weak Setup"}


# Allowed grade sets keyed by minimumGrade UI value
_GRADE_ALLOWED: dict[str, set[str]] = {
    "C+": {"A+", "A", "B", "C"},
    "B+": {"A+", "A", "B"},
    "A":  {"A+", "A"},
}

def _grade_passes_filter(grade: str, minimum_grade: str) -> bool:
    return grade in _GRADE_ALLOWED.get(minimum_grade, {"A+", "A", "B"})

def _bias_confluence(
    o: list, h: list, l: list, c: list, v: list,
    tf: str,
    sig_idx: int,
    rej_dir: str,
    rej: dict,
    prior_start: int,
    use_ob: bool,
    use_fvg: bool,
    use_fib: bool,
) -> dict:
    """
    Check OB, FVG, and Fib confluence for a Bias Shift candidate.
    Uses direction-aware reference prices so wick-area confluence is detected:
      bearish → [rej.high, rej.close]  (rejection likely at wick high)
      bullish → [rej.low,  rej.close]  (rejection likely at wick low)
    Distance = minimum distance from ANY reference price to the zone/level.
    """
    if rej_dir == "bearish":
        ref_prices = [p for p in (rej["high"], rej["close"]) if p > 0]
        wick_label = "around rejection high"
    else:
        ref_prices = [p for p in (rej["low"], rej["close"]) if p > 0]
        wick_label = "around rejection low"

    if not ref_prices:
        return {"ob": None, "fvg": None, "fib": None}

    def _zone_dist(bot: float, top: float) -> float:
        """Minimum % distance from any ref price to zone. 0.0 if inside."""
        best = float("inf")
        for p in ref_prices:
            if bot <= p <= top:
                return 0.0
            d = (bot - p) / p * 100 if p < bot else (p - top) / p * 100
            if d < best:
                best = d
        return best

    def _level_dist(level: float) -> float:
        """Minimum % distance from any ref price to a single level."""
        best = float("inf")
        for p in ref_prices:
            d = abs(p - level) / p * 100
            if d < best:
                best = d
        return best

    ob_conf = fvg_conf = fib_conf = None

    # ── OB confluence ──────────────────────────────────────────────────────────
    if use_ob:
        try:
            obs, _ = detect_obs(o, h, l, c, v, 5, 20, max_ob=8)
            for ob in obs:
                if ob["type"] != rej_dir:
                    continue
                dist_pct = _zone_dist(ob["bottom"], ob["top"])
                if dist_pct <= 2.0:
                    ob_conf = {
                        "found":       True,
                        "type":        ob["type"],
                        "zone":        f"{ob['bottom']:.6f} - {ob['top']:.6f}",
                        "distancePct": round(dist_pct, 2),
                        "reason":      f"Rejected near {rej_dir} OB zone {wick_label}",
                    }
                    break
        except Exception:
            pass

    # ── FVG confluence ─────────────────────────────────────────────────────────
    if use_fvg:
        try:
            fvgs = detect_fvgs(o, h, l, c, v, tf)
            for fvg in fvgs:
                if fvg["direction"] != rej_dir:
                    continue
                dist_pct = _zone_dist(fvg["bottom"], fvg["top"])
                if dist_pct <= 1.5:
                    touch = "untouched" if fvg.get("untouched") else "touched"
                    fvg_conf = {
                        "found":       True,
                        "direction":   fvg["direction"],
                        "zone":        f"{fvg['bottom']:.6f} - {fvg['top']:.6f}",
                        "touch":       touch,
                        "distancePct": round(dist_pct, 2),
                        "reason":      f"Rejection near {rej_dir} FVG {wick_label}",
                    }
                    break
        except Exception:
            pass

    # ── Fib confluence ─────────────────────────────────────────────────────────
    if use_fib:
        try:
            prior_h = h[prior_start:sig_idx]
            prior_l = l[prior_start:sig_idx]
            if len(prior_h) >= 2:
                fib_high  = max(prior_h)
                fib_low   = min(prior_l)
                fib_range = fib_high - fib_low
                if fib_range > 0:
                    for lvl in (0.786, 0.705, 0.618, 0.5):
                        if rej_dir == "bearish":
                            fib_price = fib_low + fib_range * lvl
                        else:
                            fib_price = fib_high - fib_range * lvl
                        dist_pct = _level_dist(fib_price)
                        if dist_pct <= 1.0:
                            fib_conf = {
                                "found":       True,
                                "level":       str(lvl),
                                "price":       round(fib_price, 8),
                                "distancePct": round(dist_pct, 2),
                                "reason":      f"Rejection near Fib {lvl} {wick_label}",
                            }
                            break
        except Exception:
            pass

    return {"ob": ob_conf, "fvg": fvg_conf, "fib": fib_conf}

# ─────────────────────────────────────────────────────────────────────────────


@app.route("/api/bias_scan", methods=["POST"])
@login_required
def api_bias_scan():
    err = _guest_tab_check("bias")
    if err is not None: return err
    _tok_user, _tok_uid = _check_and_get_token_user()
    if _tok_user == "limit":
        return _daily_limit_response()
    payload = request.get_json(force=True) or {}
    tf = payload.get("timeframe", "1d")
    market = payload.get("market", "perpetual")
    # Fix #1: frontend sends "mode", not "biasMode"
    bias_mode        = payload.get("mode", payload.get("biasMode", "normal"))
    bias_strength    = payload.get("biasStrength", "balanced")
    bias_filter      = payload.get("biasFilter", "all")
    # detection_mode comes from payload — never overridden by presets
    detection_mode   = payload.get("detectionMode", "early")
    # Fix #3: volumeFilter is "optional" or "required", not a bool
    volume_filter_mode = payload.get("volumeFilter", "optional")
    use_volume_filter  = volume_filter_mode == "required"
    vol_multiplier     = float(payload.get("volumeMultiplier", 1.5))

    if bias_mode == "normal":
        # Fix #2: presets set candle-quality params only; detectionMode stays from payload
        p = _bias_normal_presets(bias_strength)
        prior_move_n             = p["prior_move_n"]
        signal_search_n          = p["signal_search_n"]
        min_prior_checks         = p["min_prior_checks"]
        min_wick_pct             = p["min_wick_pct"]
        min_body_pct             = p["min_body_pct"]
        require_close_beyond_mid = p["require_close_beyond_mid"]
    else:
        prior_move_n             = max(1, int(payload.get("priorMoveCandles", 3)))
        signal_search_n          = max(1, int(payload.get("signalSearchCandles", 2)))
        min_prior_checks         = 2
        min_wick_pct             = float(payload.get("minWickPct", 35)) / 100.0
        min_body_pct             = float(payload.get("minBodyPct", 15)) / 100.0
        require_close_beyond_mid = bool(payload.get("requireCloseBeyondMidpoint", False))

    minimum_grade = payload.get("minimumGrade", "B+")

    # Phase 4: confluence settings
    confluence_mode    = payload.get("confluenceMode", "optional")
    use_ob_confluence  = bool(payload.get("useObConfluence", True))
    use_fvg_confluence = bool(payload.get("useFvgConfluence", True))
    use_fib_confluence = bool(payload.get("useFibConfluence", True))

    passed_symbols  = payload.get("symbols") or []
    scan_mode       = payload.get("scanMode", "selected")
    pairs_per_cycle = max(5, min(100, int(payload.get("pairsPerCycle", 20))))
    exchange        = payload.get("exchange", "binance").lower()

    # Per-user cursor key — prevents different users/settings from sharing state
    username   = session.get("username", "anonymous")
    cursor_key = f"{username}|{exchange}|{market}|{tf}"
    market_coverage = None

    if scan_mode == "market":
        all_pairs   = [p["symbol"] for p in get_pairs_exchange(exchange, market)]
        total_pairs = max(len(all_pairs), 1)
        # Clamp cursor in case market size shrank since last scan
        start    = BIAS_SCAN_CURSOR.get(cursor_key, 0) % total_pairs
        symbols  = all_pairs[start:start + pairs_per_cycle]
        next_cur = (start + pairs_per_cycle) % total_pairs
        cycle_complete = (start + pairs_per_cycle) >= total_pairs
        BIAS_SCAN_CURSOR[cursor_key] = next_cur
        print(f"[BIAS_SCAN] round_robin user={username} tf={tf} "
              f"batch={start+1}-{start+len(symbols)}/{total_pairs}")
        market_coverage = {
            "mode":           "round_robin",
            "totalPairs":     total_pairs,
            "batchSize":      len(symbols),
            "startIndex":     start + 1,
            "endIndex":       start + len(symbols),
            "nextStartIndex": next_cur + 1,
            "cycleComplete":  cycle_complete,
        }
    elif passed_symbols:
        # Selected Pairs mode — use only what the frontend sent
        symbols = passed_symbols
    else:
        # Selected Pairs with nothing selected — tell the user clearly
        return jsonify({
            "error":   "no_selected_pairs",
            "message": "Select at least one pair or switch to Full Market.",
        }), 400

    results = []
    fetch_limit = prior_move_n + signal_search_n + 30

    diagnostics: dict = {
        "symbolsRequested":         len(symbols),
        "symbolsScanned":           0,
        "setupsFoundBeforeFilters": 0,
        "setupsReturned":           0,
        "rejected": {
            "notEnoughCandles":  0,
            "noPriorMove":        0,
            "noCleanPriorDrive":  0,
            "noAdaptivePriorDrive": 0,
            "weakOrChoppyPriorDrive": 0,
            "biasFilterMismatch": 0,
            "volumeFilter":      0,
            "noRejectionCandle": 0,
            "notConfirmed":      0,
            "confluenceRequired": 0,
            "minimumGrade":      0,
            "invalidated":       0,
            "errors":            0,
        },
        "adaptivePriorDrive": {
            "cleanAccepted":   0,
            "goodAccepted":    0,
            "impulseAccepted": 0,
            "weakRejected":    0,
            "chopRejected":    0,
        },
        "settingsUsed": {
            "timeframe":      tf,
            "mode":           bias_mode,
            "biasStrength":   bias_strength,
            "detectionMode":  detection_mode,
            "confluenceMode": confluence_mode,
            "minimumGrade":   minimum_grade,
            "volumeFilter":   volume_filter_mode,
            "scanMode":       scan_mode,
            "pairsPerCycle":  pairs_per_cycle,
        },
    }

    for sym in symbols:
        try:
            kl = get_klines_exchange(sym, tf, fetch_limit, market, exchange)
            if not kl or len(kl) < prior_move_n + signal_search_n + 2:
                diagnostics["rejected"]["notEnoughCandles"] += 1
                continue

            # Running candle close = best live-price proxy without an extra API call
            current_price: float | None = float(kl[-1]["close"]) if kl else None

            # Closed candles only — strip the still-running last candle
            kl_closed = kl[:-1]
            o  = [float(x["open"])   for x in kl_closed]
            h  = [float(x["high"])   for x in kl_closed]
            l  = [float(x["low"])    for x in kl_closed]
            c  = [float(x["close"])  for x in kl_closed]
            v  = [float(x["volume"]) for x in kl_closed]
            ts = [int(x["openTime"]) for x in kl_closed]
            n  = len(c)

            vol_lb  = min(20, n)
            avg_vol = sum(v[n - vol_lb:n]) / max(vol_lb, 1) if vol_lb > 0 else 0

            diagnostics["symbolsScanned"] += 1
            best: dict | None = None

            # Per-symbol rejection trackers for diagnostics
            _d_had_prior      = False
            _d_weak_prior     = False
            _d_had_rej        = False
            _d_passed_confirm = False
            _d_bias_miss      = False
            _d_vol_miss       = False
            _d_conf_miss      = False

            for sig_offset in range(signal_search_n):
                sig_idx = n - 1 - sig_offset
                if sig_idx < prior_move_n + 1:
                    continue

                prior_result = _detect_prior_move_adaptive(
                    o, h, l, c, sig_idx, tf, prior_move_n, bias_strength, bias_mode
                )
                prior_dir = prior_result["direction"]

                if not prior_result.get("accepted"):
                    pq = prior_result.get("quality", "none")
                    rr = prior_result.get("rejectReason", "")
                    # A recognisable (but rejected) drive: quality was determined,
                    # OR chop killed candidates before quality could be assigned.
                    # "no_drive_found" means truly no directional move → noPriorMove.
                    _had_drive = pq not in ("none",) or rr == "choppy_drive"
                    if _had_drive:
                        _d_weak_prior = True
                        if rr == "choppy_drive":
                            diagnostics["adaptivePriorDrive"]["chopRejected"] += 1
                        else:
                            diagnostics["adaptivePriorDrive"]["weakRejected"] += 1
                    continue

                _d_had_prior = True
                prior_start    = prior_result["priorStart"]
                actual_window_n = prior_result["windowN"]
                rej_dir = "bearish" if prior_dir == "up" else "bullish"

                if bias_filter == "bullish" and rej_dir != "bullish":
                    _d_bias_miss = True
                    continue
                if bias_filter == "bearish" and rej_dir != "bearish":
                    _d_bias_miss = True
                    continue

                sig_vol   = v[sig_idx]
                vol_spike = avg_vol > 0 and sig_vol >= avg_vol * vol_multiplier
                if use_volume_filter and not vol_spike:
                    _d_vol_miss = True
                    continue

                rej = _check_rejection_candle(
                    o, h, l, c, sig_idx, rej_dir,
                    min_wick_pct, min_body_pct, require_close_beyond_mid,
                )
                if rej is None:
                    continue

                _d_had_rej = True
                # Fix #6: confirmed mode searches ALL later closed candles
                confirmation_status = "early_unconfirmed"
                if detection_mode == "confirmed":
                    confirmed = False
                    for post_idx in range(sig_idx + 1, n):
                        if rej_dir == "bearish":
                            if l[post_idx] < rej["low"] or c[post_idx] < rej["low"]:
                                confirmed = True; break
                        else:
                            if h[post_idx] > rej["high"] or c[post_idx] > rej["high"]:
                                confirmed = True; break
                    if not confirmed:
                        continue
                    confirmation_status = "confirmed"

                _d_passed_confirm = True

                if rej_dir == "bearish":
                    invalidation_level = round(rej["high"], 8)
                    invalidation_text  = f"Invalid above {rej['high']:.6f}"
                else:
                    invalidation_level = round(rej["low"], 8)
                    invalidation_text  = f"Invalid below {rej['low']:.6f}"

                # ── Phase 3 pre-metrics ───────────────────────────────────────
                rej_mid = rej["low"] + (rej["high"] - rej["low"]) / 2
                close_is_beyond_mid = (
                    (rej_dir == "bearish" and rej["close"] < rej_mid) or
                    (rej_dir == "bullish" and rej["close"] > rej_mid)
                )
                rej_wick_pct_val = (
                    rej["upperWickPct"] if rej_dir == "bearish" else rej["lowerWickPct"]
                )

                # ── Phase 4: confluence check ─────────────────────────────────
                conf_results = _bias_confluence(
                    o, h, l, c, v, tf,
                    sig_idx=sig_idx,
                    rej_dir=rej_dir,
                    rej=rej,
                    prior_start=prior_start,
                    use_ob=use_ob_confluence,
                    use_fvg=use_fvg_confluence,
                    use_fib=use_fib_confluence,
                )
                ob_conf  = conf_results["ob"]
                fvg_conf = conf_results["fvg"]
                fib_conf = conf_results["fib"]
                ob_found  = ob_conf  is not None
                fvg_found = fvg_conf is not None
                fib_found = fib_conf is not None

                enabled_conf_count = sum([use_ob_confluence, use_fvg_confluence, use_fib_confluence])
                found_conf_count   = sum([ob_found, fvg_found, fib_found])
                conf_req_passed    = confluence_mode == "required" and found_conf_count > 0

                # Required mode: skip if at least one confluence is enabled but none found
                if confluence_mode == "required" and enabled_conf_count > 0 and found_conf_count == 0:
                    _d_conf_miss = True
                    continue

                # ── Phase 3+4: score with confluence bonuses ──────────────────
                drive_quality  = prior_result["quality"]   # clean|good|impulse
                scored = _score_bias_shift(
                    prior_checks=prior_result["checksPassed"],
                    rej_wick_pct=rej_wick_pct_val,
                    rej_body_pct=rej["bodyPct"],
                    min_wick_pct=min_wick_pct,
                    min_body_pct=min_body_pct,
                    close_beyond_mid=close_is_beyond_mid,
                    confirmation_status=confirmation_status,
                    vol_spike=vol_spike,
                    volume_filter_mode=volume_filter_mode,
                    sig_offset=sig_offset,
                    ob_found=ob_found,
                    fvg_found=fvg_found,
                    fib_found=fib_found,
                    confluence_required_passed=conf_req_passed,
                    prior_quality=drive_quality,
                    prior_drive_score=prior_result["score"],
                )
                score_val = scored["score"]
                graded    = _grade_from_score(score_val)

                if score_val >= 85:
                    confidence = "Strong"
                elif score_val >= 65:
                    confidence = "Moderate"
                else:
                    confidence = "Weak"

                total_checks = prior_result["checksPassed"] + rej["checksPassed"]

                # Human-readable reason chain
                min_wick_pct_i  = round(min_wick_pct * 100)
                min_body_pct_i  = round(min_body_pct * 100)
                drive_word      = "bullish" if prior_dir == "up" else "bearish"
                quality_label   = drive_quality.capitalize()  # Clean / Good / Impulse
                color_word_r    = "green" if prior_dir == "up" else "red"
                color_cnt_r     = prior_result["greenCount"] if prior_dir == "up" else prior_result["redCount"]
                step_cnt_r      = prior_result["upCloseSteps"] if prior_dir == "up" else prior_result["downCloseSteps"]

                if drive_quality == "impulse":
                    drive_line1 = f"Impulse {drive_word} drive detected before rejection"
                    drive_line2 = (
                        f"Impulse proof: {prior_result['netPct']:.2f}% net"
                        f" · displacement candle found"
                        f" · score {prior_result['score']}"
                        f" · min {prior_result['requiredMovePct']}%"
                    )
                else:
                    drive_line1 = (
                        f"{quality_label} {actual_window_n}-candle {drive_word} drive"
                        f" detected before rejection"
                    )
                    drive_line2 = (
                        f"Drive proof: {prior_result['netPct']:.2f}% net"
                        f" · {color_cnt_r}/{actual_window_n} {color_word_r}"
                        f" · {step_cnt_r} step{'s' if step_cnt_r != 1 else ''}"
                        f" · score {prior_result['score']}"
                        f" · min {prior_result['requiredMovePct']}%"
                    )

                readable_chain: list[str] = [drive_line1, drive_line2]

                if rej_dir == "bearish":
                    readable_chain.append("Signal candle closed bearish")
                    readable_chain.append(
                        f"Upper wick {rej['upperWickPct']}% >= required {min_wick_pct_i}%"
                    )
                else:
                    readable_chain.append("Signal candle closed bullish")
                    readable_chain.append(
                        f"Lower wick {rej['lowerWickPct']}% >= required {min_wick_pct_i}%"
                    )
                readable_chain.append(f"Body {rej['bodyPct']}% >= required {min_body_pct_i}%")
                if close_is_beyond_mid:
                    readable_chain.append(
                        "Close below candle midpoint" if rej_dir == "bearish"
                        else "Close above candle midpoint"
                    )
                if vol_spike:
                    vol_ratio = sig_vol / max(avg_vol, 1)
                    readable_chain.append(f"Volume spike {vol_ratio:.1f}x average")
                if ob_found:
                    readable_chain.append(f"OB confluence: {ob_conf['reason']}")
                if fvg_found:
                    readable_chain.append(f"FVG confluence: {fvg_conf['reason']}")
                if fib_found:
                    readable_chain.append(f"Fib confluence: {fib_conf['reason']}")
                if conf_req_passed:
                    readable_chain.append("Required confluence passed")
                if rej_dir == "bearish":
                    readable_chain.append(
                        f"Invalidation: close above rejection high {rej['high']:.6f}"
                    )
                else:
                    readable_chain.append(
                        f"Invalidation: close below rejection low {rej['low']:.6f}"
                    )

                conf_found_list = (
                    (["OB"]  if ob_found  else []) +
                    (["FVG"] if fvg_found else []) +
                    (["Fib"] if fib_found else [])
                )
                setup_type_label = (
                    "Bearish Bias Shift" if rej_dir == "bearish" else "Bullish Bias Shift"
                )
                pattern    = (f"{'Bearish' if rej_dir == 'bearish' else 'Bullish'} Rejection"
                              f" · {'Uptrend' if prior_dir == 'up' else 'Downtrend'}")
                conf_label = "✓ Confirmed" if confirmation_status == "confirmed" else "Early"
                conf_chip  = f" · {', '.join(conf_found_list)}" if conf_found_list else ""
                detail     = (
                    f"Prior {prior_dir.upper()} {actual_window_n}c({drive_quality})"
                    f" · {conf_label}"
                    f" · Grade {graded['grade']} ({score_val})"
                    f"{conf_chip}"
                    f" · Inv: {invalidation_level}"
                    f" · Conf TF: {_suggested_conf_tf(tf)}"
                )
                compact_reasons = prior_result["reasons"] + rej["reasons"]
                sparkline = [float(c[i]) for i in range(max(0, n - 24), n)]

                candidate = {
                    # ── compat fields ──
                    "symbol":       sym,
                    "price":        round(c[-1], 8),
                    "timeframe":    tf,
                    "bias":         rej_dir,
                    "signal":       "BIAS_SHIFT",
                    "confidence":   confidence,
                    "gates":        " · ".join(compact_reasons),
                    "gatesPassed":  total_checks,
                    "gatesChecked": 6,
                    "upperWickPct": rej["upperWickPct"],
                    "lowerWickPct": rej["lowerWickPct"],
                    "bodyPct":      rej["bodyPct"],
                    "volSpike":     vol_spike,
                    "obConf":       ob_conf,
                    "fvgConf":      fvg_conf,
                    # ── Phase 3: score/grade ──
                    "score":          score_val,
                    "grade":          graded["grade"],
                    "gradeLabel":     graded["gradeLabel"],
                    "scoreBreakdown": scored["breakdown"],
                    "sparkline":      sparkline,
                    # ── Phase 4: confluence ──
                    "fibConf":          fib_conf,
                    "confluenceMode":   confluence_mode,
                    "confluencesFound": conf_found_list,
                    "confluenceCount":  len(conf_found_list),
                    # ── Phase 2 fields ──
                    "direction":               rej_dir,
                    "biasDirection":           rej_dir,
                    "setupType":               setup_type_label,
                    "pattern":                 pattern,
                    "detail":                  detail,
                    "priorMoveDirection":        prior_dir,
                    "priorMoveCandles":          actual_window_n,
                    "priorMoveChecks":           prior_result["checksPassed"],
                    "priorMoveQuality":          prior_result["quality"],
                    "priorMoveNetPct":           prior_result["netPct"],
                    "priorMoveGreenCount":       prior_result["greenCount"],
                    "priorMoveRedCount":         prior_result["redCount"],
                    "priorMoveUpCloseSteps":     prior_result["upCloseSteps"],
                    "priorMoveDownCloseSteps":   prior_result["downCloseSteps"],
                    "priorMoveRequiredMovePct":  prior_result["requiredMovePct"],
                    "priorWindowStartTime":      ts[prior_start],
                    "priorWindowEndTime":        ts[sig_idx - 1],
                    "signalCandleOffset":        sig_offset,
                    "signalCandleTime":          ts[sig_idx],
                    "rejectionOpen":            rej["open"],
                    "rejectionHigh":            rej["high"],
                    "rejectionLow":             rej["low"],
                    "rejectionClose":           rej["close"],
                    "rejectionBodyPct":         rej["bodyPct"],
                    "rejectionUpperWickPct":    rej["upperWickPct"],
                    "rejectionLowerWickPct":    rej["lowerWickPct"],
                    "invalidationLevel":        invalidation_level,
                    "invalidationText":         invalidation_text,
                    "confirmationStatus":       confirmation_status,
                    "suggestedConfirmationTf":  _suggested_conf_tf(tf),
                    "reasonChain":              readable_chain,
                    # ── Adaptive prior drive details (new) ──
                    "priorDrive": {
                        "quality":      drive_quality,
                        "driveType":    prior_result.get("driveType", drive_quality),
                        "score":        prior_result["score"],
                        "windowN":      actual_window_n,
                        "netPct":       prior_result["netPct"],
                        "pullbackPct":  prior_result.get("maxPullbackPct", 0),
                        "impulseFound": prior_result["impulseFound"],
                        "reasons":      prior_result["reasons"],
                    },
                    "debug": {
                        "priorSummary":      prior_result["summary"],
                        "priorMoveReasons":  prior_result["reasons"],
                        "rejReasons":        rej["reasons"],
                    },
                }

                if best is None or _is_better_setup(candidate, best):
                    best = candidate

            if best is not None:
                # Compute live / closed invalidation status
                inv_level  = best["invalidationLevel"]
                inv_dir    = best["bias"]
                latest_cc  = c[-1]
                cp         = current_price
                _cp_or_cc  = cp if cp is not None else latest_cc

                if inv_dir == "bearish":
                    closed_inv = latest_cc > inv_level
                    live_br    = cp is not None and cp > inv_level
                    dist_pct   = round((inv_level - _cp_or_cc) / _cp_or_cc * 100, 4) if inv_level > 0 and _cp_or_cc > 0 else None
                else:
                    closed_inv = latest_cc < inv_level
                    live_br    = cp is not None and cp < inv_level
                    dist_pct   = round((_cp_or_cc - inv_level) / _cp_or_cc * 100, 4) if inv_level > 0 and _cp_or_cc > 0 else None

                inv_status = ("closed_invalidated" if closed_inv
                              else "live_breached" if live_br
                              else "valid")
                best["invalidationStatus"]       = inv_status
                best["invalidationBreachedLive"] = live_br
                best["invalidationClosed"]       = closed_inv
                best["currentPrice"]             = cp
                best["latestClosedClose"]        = round(latest_cc, 8)
                best["invalidationDistancePct"]  = dist_pct

                if live_br and not closed_inv:
                    warn = (
                        "Warning: current price has breached bearish invalidation intrabar"
                        if inv_dir == "bearish" else
                        "Warning: current price has breached bullish invalidation intrabar"
                    )
                    best["reasonChain"].append(warn)

                if closed_inv:
                    diagnostics["rejected"]["invalidated"] += 1
                else:
                    results.append(best)
            else:
                # Attribute primary rejection reason (priority order)
                if not _d_had_prior and _d_weak_prior:
                    diagnostics["rejected"]["noCleanPriorDrive"] += 1
                    diagnostics["rejected"]["noAdaptivePriorDrive"] += 1
                elif not _d_had_prior:
                    diagnostics["rejected"]["noPriorMove"] += 1
                elif _d_bias_miss:
                    diagnostics["rejected"]["biasFilterMismatch"] += 1
                elif _d_vol_miss:
                    diagnostics["rejected"]["volumeFilter"] += 1
                elif not _d_had_rej:
                    diagnostics["rejected"]["noRejectionCandle"] += 1
                elif not _d_passed_confirm:
                    diagnostics["rejected"]["notConfirmed"] += 1
                elif _d_conf_miss:
                    diagnostics["rejected"]["confluenceRequired"] += 1

        except Exception as e:
            print(f"[DEBUG] bias_scan {sym} error: {e}")
            diagnostics["rejected"]["errors"] += 1
            continue

    # Tally adaptive prior-drive quality counts from accepted results
    for _r in results:
        _pd = _r.get("priorDrive", {})
        _q  = _pd.get("quality", "")
        if   _q == "clean":   diagnostics["adaptivePriorDrive"]["cleanAccepted"]   += 1
        elif _q == "good":    diagnostics["adaptivePriorDrive"]["goodAccepted"]    += 1
        elif _q == "impulse": diagnostics["adaptivePriorDrive"]["impulseAccepted"] += 1

    # Phase 3: apply minimum grade filter, then sort by best setup first
    diagnostics["setupsFoundBeforeFilters"] = len(results)
    _before_grade = len(results)
    results = [r for r in results if _grade_passes_filter(r.get("grade", "D"), minimum_grade)]
    diagnostics["rejected"]["minimumGrade"] = _before_grade - len(results)

    # Tally accepted drive quality from FINAL results (after grade filter) — single source of truth
    diagnostics["adaptivePriorDrive"]["cleanAccepted"]   = 0
    diagnostics["adaptivePriorDrive"]["goodAccepted"]    = 0
    diagnostics["adaptivePriorDrive"]["impulseAccepted"] = 0
    for _r in results:
        _q = (_r.get("priorDrive") or {}).get("quality", "")
        if   _q == "clean":   diagnostics["adaptivePriorDrive"]["cleanAccepted"]   += 1
        elif _q == "good":    diagnostics["adaptivePriorDrive"]["goodAccepted"]    += 1
        elif _q == "impulse": diagnostics["adaptivePriorDrive"]["impulseAccepted"] += 1
    _go = {"A+": 0, "A": 1, "B": 2, "C": 3, "D": 4}
    results.sort(key=lambda x: (
        -x.get("score", 0),
        _go.get(x.get("grade", "D"), 4),
        0 if x.get("confirmationStatus") == "confirmed" else 1,
        x.get("signalCandleOffset", 0),
        -(x.get("rejectionUpperWickPct", 0) if x.get("priorMoveDirection") == "up"
          else x.get("rejectionLowerWickPct", 0)),
    ))
    diagnostics["setupsReturned"] = len(results)
    if _tok_uid:
        try: consume_tokens(_tok_uid, len(symbols))
        except Exception as _te: print(f"[Tokens] bias: {_te}")
    return jsonify({
        "results":        results,
        "scanned":        len(symbols),
        "scanMode":       scan_mode,
        "nextBiasIndex":  BIAS_SCAN_CURSOR.get(cursor_key, 0),
        "marketCoverage": market_coverage,
        "diagnostics":    diagnostics,
    })


# ============================================================
# Order Flow v5 — Exchange-Aware REST Adapters
# ============================================================

def normalize_of_symbol(exchange: str, symbol: str) -> str:
    """Normalize symbol to each exchange's perpetual contract format."""
    sym = symbol.upper().replace('.P', '').strip()
    if exchange in ('binance', 'bybit'):
        sym = sym.replace('/', '').replace('-', '').replace('_', '')
        if not sym.endswith('USDT'):
            sym = sym + 'USDT'
        return sym
    elif exchange == 'okx':
        if sym.endswith('-USDT-SWAP'):
            return sym
        base = sym.replace('USDT', '').replace('/', '').replace('-', '').replace('_', '')
        return f"{base}-USDT-SWAP"
    elif exchange == 'mexc':
        if '_USDT' in sym:
            return sym
        base = sym.replace('USDT', '').replace('/', '').replace('-', '').replace('_', '')
        return f"{base}_USDT"
    return sym


def normalize_of_timeframe(exchange: str, timeframe: str) -> Tuple[Optional[str], Optional[str]]:
    """Returns (exchange_interval, error_or_None)."""
    tf = timeframe.lower()
    maps = {
        'binance': {'1h':'1h','4h':'4h','6h':'6h','12h':'12h','1d':'1d'},
        'bybit':   {'1h':'60','4h':'240','6h':'360','12h':'720','1d':'D'},
        'okx':     {'1h':'1H','4h':'4H','6h':'6H','12h':'12H','1d':'1D'},
        'mexc':    {'1h':'Min60','4h':'Hour4','1d':'Day1'},
    }
    iv = maps.get(exchange, {}).get(tf)
    if not iv:
        return None, f"Timeframe {timeframe} not supported for {exchange.upper()}"
    return iv, None


def _of_tf_range_pct(tf: str) -> float:
    return {'1h':1.0,'4h':2.0,'6h':2.5,'12h':3.0,'1d':5.0}.get(tf.lower(), 2.0)


def process_order_book_levels(bids: list, asks: list, current_price: float, range_pct: float) -> Optional[dict]:
    """Python equivalent of frontend _processBookData. Uses USDT notional for sizing/ranking."""
    if not bids or not asks or not current_price or current_price <= 0:
        return None
    lo = current_price * (1 - range_pct / 100)
    hi = current_price * (1 + range_pct / 100)

    def parse_level(lv):
        try:
            p, q = float(lv[0]), float(lv[1])
            return {'price': p, 'qty': q, 'notional': p * q}
        except Exception:
            return None

    fb = [l for l in (parse_level(b) for b in bids) if l and lo <= l['price'] <= current_price]
    fa = [l for l in (parse_level(a) for a in asks) if l and current_price <= l['price'] <= hi]
    max_n = max(
        max((x['notional'] for x in fb), default=0),
        max((x['notional'] for x in fa), default=0), 1)

    def blend(item, is_bid):
        ss = item['notional'] / max_n
        dist = ((current_price - item['price']) / current_price if is_bid
                else (item['price'] - current_price) / current_price)
        cs = max(0.0, 1.0 - dist / (range_pct / 100))
        return ss * 0.75 + cs * 0.25

    top5_b = sorted(fb, key=lambda x: blend(x, True),  reverse=True)[:5]
    top5_a = sorted(fa, key=lambda x: blend(x, False), reverse=True)[:5]
    bid_vol = sum(b['notional'] for b in fb)
    ask_vol = sum(a['notional'] for a in fa)
    tot = bid_vol + ask_vol
    bid_pct = round(bid_vol / tot * 100) if tot > 0 else 50
    ask_pct = 100 - bid_pct
    return {
        'top5Bids': [{'price':b['price'],'qty':b['qty'],'notional':b['notional'],
                      'distancePct':round((current_price-b['price'])/current_price*100,2),
                      'strengthPct':round(b['notional']/max_n*100)} for b in top5_b],
        'top5Asks': [{'price':a['price'],'qty':a['qty'],'notional':a['notional'],
                      'distancePct':round((a['price']-current_price)/current_price*100,2),
                      'strengthPct':round(a['notional']/max_n*100)} for a in top5_a],
        'bidVolumeUSDT': bid_vol, 'askVolumeUSDT': ask_vol,
        'bidPct': bid_pct,        'askPct': ask_pct,
        'imbalancePct': bid_pct - ask_pct,
        'sideHeavier': ('Buy side heavier' if bid_pct >= 55
                        else 'Sell side heavier' if ask_pct >= 55 else 'Balanced book'),
    }


def _of_pad_history(arr: list, length: int = 3) -> list:
    while len(arr) < length:
        arr.insert(0, None)
    return arr[-length:]


def _of_build_candles_binance(klines_raw: list, tf: str) -> list:
    candles, tf_up, now_ms = [], tf.upper(), int(time.time() * 1000)
    n = len(klines_raw)
    for i, k in enumerate(klines_raw):
        total_quote = safe_float(k[7])
        tb_quote    = safe_float(k[10])
        ts_quote    = total_quote - tb_quote
        buy_pct     = round(tb_quote / total_quote * 100, 1) if total_quote > 0 else None
        sell_pct    = round(100 - buy_pct, 1) if buy_pct is not None else None
        delta       = round(tb_quote - ts_quote, 2) if total_quote > 0 else None
        close_ms    = int(k[6])
        is_running  = (i == n - 1) and (close_ms > now_ms)
        label = (f"Current {tf_up} Candle — Running" if i == n - 1
                 else f"Last Closed {tf_up} Candle — Confirmed" if i == n - 2
                 else f"Previous {tf_up} Candle — Confirmed")
        candles.append({'label':label,
            'open':safe_float(k[1]),'high':safe_float(k[2]),'low':safe_float(k[3]),'close':safe_float(k[4]),
            'totalVolumeBase':safe_float(k[5]),'totalVolumeQuote':total_quote,
            'takerBuyQuote':tb_quote,'takerSellQuote':ts_quote,
            'buyPct':buy_pct,'sellPct':sell_pct,'delta':delta,
            'isRunning':is_running,'closeTimeMs':close_ms,'dataQuality':'native'})
    return candles


def _of_build_candles_no_split(klines_norm: list, tf: str) -> list:
    candles, tf_up, now_ms = [], tf.upper(), int(time.time() * 1000)
    n = len(klines_norm)
    for i, k in enumerate(klines_norm):
        close_ms   = int(k.get('closeTimeMs', k.get('openTime', 0)))
        is_running = (i == n - 1) and (close_ms > now_ms)
        label = (f"Current {tf_up} Candle — Running" if i == n - 1
                 else f"Last Closed {tf_up} Candle — Confirmed" if i == n - 2
                 else f"Previous {tf_up} Candle — Confirmed")
        candles.append({'label':label,
            'open':k.get('open',0),'high':k.get('high',0),'low':k.get('low',0),'close':k.get('close',0),
            'totalVolumeBase':k.get('volume',0),'totalVolumeQuote':k.get('quoteVolume',k.get('turnover',0)),
            'takerBuyQuote':None,'takerSellQuote':None,'buyPct':None,'sellPct':None,'delta':None,
            'isRunning':is_running,'closeTimeMs':close_ms,'dataQuality':'unavailable'})
    return candles


def fetch_of_binance(symbol: str, tf: str) -> dict:
    errors, iv = [], tf.lower()
    klines_raw = []
    try:
        r = req.get(f"{BINANCE_FUTURES_API}/fapi/v1/klines",
                    params={'symbol':symbol,'interval':iv,'limit':4}, timeout=10)
        if r.status_code == 200:
            klines_raw = r.json()
    except Exception as e:
        errors.append(f"klines: {e}")
    if not klines_raw or len(klines_raw) < 2:
        return {'ok':False,'errors':[f'Candle data unavailable for {symbol}']+errors}

    oi_delta, oi_value = [], None
    try:
        r = req.get(f"{BINANCE_FUTURES_API}/futures/data/openInterestHist",
                    params={'symbol':symbol,'period':iv,'limit':4}, timeout=10)
        if r.status_code == 200:
            oi_raw = r.json()
            if oi_raw and len(oi_raw) >= 2:
                for i in range(1, len(oi_raw)):
                    pv = safe_float(oi_raw[i-1].get('sumOpenInterestValue',0))
                    cv = safe_float(oi_raw[i].get('sumOpenInterestValue',0))
                    oi_delta.append(round((cv-pv)/pv*100,2) if pv > 0 else 0)
                oi_value = safe_float(oi_raw[-1].get('sumOpenInterestValue',0))
    except Exception as e:
        errors.append(f"OI: {e}")

    fund_hist = []
    try:
        r = req.get(f"{BINANCE_FUTURES_API}/fapi/v1/fundingRate",
                    params={'symbol':symbol,'limit':4}, timeout=10)
        if r.status_code == 200:
            fraw = r.json()
            if fraw:
                fund_hist = [round(safe_float(f.get('fundingRate',0))*100,6) for f in fraw]
    except Exception as e:
        errors.append(f"funding: {e}")

    bid_ask = None
    try:
        for lim in [500, 100]:
            r = req.get(f"{BINANCE_FUTURES_API}/fapi/v1/depth",
                        params={'symbol':symbol,'limit':lim}, timeout=10)
            if r.status_code == 200:
                d = r.json()
                cp = safe_float(klines_raw[-1][4])
                bid_ask = process_order_book_levels(d.get('bids',[]), d.get('asks',[]), cp, _of_tf_range_pct(iv))
                if bid_ask:
                    bid_ask['tfRangePct'] = _of_tf_range_pct(iv)
                break
    except Exception as e:
        errors.append(f"depth: {e}")
    if bid_ask:
        bid_ask['oiValue'] = oi_value

    candles = _of_build_candles_binance(klines_raw, iv)
    cvd, cvd_r = [], 0.0
    for c in candles:
        cvd_r += c['delta'] or 0
        cvd.append(round(cvd_r, 2))
    return {'ok':True,'exchange':'binance','sourceLabel':'Binance Futures',
            'symbol':symbol,'displaySymbol':symbol,'timeframe':iv.upper(),
            'candles':candles,'cvd':cvd,
            'currentCandle':candles[-1] if candles else None,
            'lastClosedCandle':candles[-2] if len(candles)>=2 else None,
            'previousCandle':candles[-3] if len(candles)>=3 else None,
            'oiDeltaHistory':_of_pad_history(oi_delta),
            'oiValueUSDT':oi_value,'fundingHistory':_of_pad_history(fund_hist),
            'bidAsk':bid_ask,'buySellAvailable':True,'errors':errors}


def fetch_of_bybit(symbol: str, tf: str) -> dict:
    errors = []
    iv, err = normalize_of_timeframe('bybit', tf)
    if err:
        return {'ok':False,'errors':[err]}
    iv_ms = int(iv) * 60000 if iv.isdigit() else 86400000

    klines_norm = []
    try:
        r = req.get(f"{BYBIT_PERP_API}/kline",
                    params={'category':'linear','symbol':symbol,'interval':iv,'limit':4}, timeout=10)
        if r.status_code == 200:
            raw = list(reversed(r.json().get('result',{}).get('list',[])))
            for k in raw:
                open_ms = int(k[0])
                klines_norm.append({'open':float(k[1]),'high':float(k[2]),'low':float(k[3]),
                    'close':float(k[4]),'volume':float(k[5]),'turnover':float(k[6]),
                    'openTime':open_ms,'closeTimeMs':open_ms+iv_ms-1})
    except Exception as e:
        errors.append(f"klines: {e}")
    if not klines_norm or len(klines_norm) < 2:
        return {'ok':False,'errors':[f'Candle data unavailable for {symbol}']+errors}
    current_price = klines_norm[-1]['close']

    oi_delta, oi_value = [], None
    oi_iv_map = {'60':'1h','240':'4h','360':'4h','720':'4h','D':'1d'}
    oi_interval = oi_iv_map.get(iv, '1h')
    try:
        r = req.get(f"{BYBIT_PERP_API}/open-interest",
                    params={'category':'linear','symbol':symbol,'intervalTime':oi_interval,'limit':4}, timeout=10)
        if r.status_code == 200:
            oi_list = list(reversed(r.json().get('result',{}).get('list',[])))
            if len(oi_list) >= 2:
                for i in range(1, len(oi_list)):
                    pv = safe_float(oi_list[i-1].get('openInterest',0)) * current_price
                    cv = safe_float(oi_list[i].get('openInterest',0)) * current_price
                    oi_delta.append(round((cv-pv)/pv*100,2) if pv > 0 else 0)
                oi_value = safe_float(oi_list[-1].get('openInterest',0)) * current_price
    except Exception as e:
        errors.append(f"OI: {e}")

    fund_hist = []
    try:
        r = req.get(f"{BYBIT_PERP_API}/funding/history",
                    params={'category':'linear','symbol':symbol,'limit':4}, timeout=10)
        if r.status_code == 200:
            flist = list(reversed(r.json().get('result',{}).get('list',[])))
            fund_hist = [round(safe_float(f.get('fundingRate',0))*100,6) for f in flist]
    except Exception as e:
        errors.append(f"funding: {e}")

    bid_ask = None
    try:
        r = req.get(f"{BYBIT_PERP_API}/orderbook",
                    params={'category':'linear','symbol':symbol,'limit':200}, timeout=10)
        if r.status_code == 200:
            res = r.json().get('result',{})
            bid_ask = process_order_book_levels(res.get('b',[]), res.get('a',[]), current_price, _of_tf_range_pct(tf))
            if bid_ask:
                bid_ask['tfRangePct'] = _of_tf_range_pct(tf)
    except Exception as e:
        errors.append(f"depth: {e}")
    if bid_ask:
        bid_ask['oiValue'] = oi_value

    candles = _of_build_candles_no_split(klines_norm, tf)
    return {'ok':True,'exchange':'bybit','sourceLabel':'Bybit USDT Perp',
            'symbol':symbol,'displaySymbol':symbol,'timeframe':tf.upper(),
            'candles':candles,'cvd':[None]*len(candles),
            'currentCandle':candles[-1] if candles else None,
            'lastClosedCandle':candles[-2] if len(candles)>=2 else None,
            'previousCandle':candles[-3] if len(candles)>=3 else None,
            'oiDeltaHistory':_of_pad_history(oi_delta),
            'oiValueUSDT':oi_value,'fundingHistory':_of_pad_history(fund_hist),
            'bidAsk':bid_ask,'buySellAvailable':False,'errors':errors}


def fetch_of_okx(symbol: str, tf: str) -> dict:
    errors = []
    iv, err = normalize_of_timeframe('okx', tf)
    if err:
        return {'ok':False,'errors':[err]}
    iv_ms_map = {'1H':3600000,'4H':14400000,'6H':21600000,'12H':43200000,'1D':86400000}
    iv_ms = iv_ms_map.get(iv, 3600000)
    okx_pub = "https://www.okx.com/api/v5/public"

    klines_norm = []
    try:
        r = req.get(f"{OKX_PERP_API}/candles",
                    params={'instId':symbol,'bar':iv,'limit':4}, timeout=10)
        if r.status_code == 200:
            raw = list(reversed(r.json().get('data',[])))
            for k in raw:
                open_ms = int(k[0])
                klines_norm.append({'open':float(k[1]),'high':float(k[2]),'low':float(k[3]),
                    'close':float(k[4]),'volume':float(k[5]),
                    'quoteVolume':float(k[7]) if len(k)>7 else 0,
                    'openTime':open_ms,'closeTimeMs':open_ms+iv_ms-1})
    except Exception as e:
        errors.append(f"klines: {e}")
    if not klines_norm or len(klines_norm) < 2:
        return {'ok':False,'errors':[f'Candle data unavailable for {symbol}']+errors}
    current_price = klines_norm[-1]['close']

    oi_value = None
    try:
        r = req.get(f"{okx_pub}/open-interest", params={'instType':'SWAP','instId':symbol}, timeout=10)
        if r.status_code == 200:
            d = r.json().get('data',[])
            if d:
                oi_value = safe_float(d[0].get('oiCcy',0)) * current_price
    except Exception as e:
        errors.append(f"OI: {e}")

    fund_hist = []
    try:
        r = req.get(f"{okx_pub}/funding-rate-history", params={'instId':symbol,'limit':4}, timeout=10)
        if r.status_code == 200:
            fraw = list(reversed(r.json().get('data',[])))
            fund_hist = [round(safe_float(f.get('fundingRate',0))*100,6) for f in fraw]
    except Exception as e:
        errors.append(f"funding: {e}")

    bid_ask = None
    try:
        r = req.get(f"{OKX_PERP_API}/books", params={'instId':symbol,'sz':200}, timeout=10)
        if r.status_code == 200:
            d = r.json().get('data',[])
            if d:
                bids = [[b[0],b[1]] for b in d[0].get('bids',[])]
                asks = [[a[0],a[1]] for a in d[0].get('asks',[])]
                bid_ask = process_order_book_levels(bids, asks, current_price, _of_tf_range_pct(tf))
                if bid_ask:
                    bid_ask['tfRangePct'] = _of_tf_range_pct(tf)
    except Exception as e:
        errors.append(f"depth: {e}")
    if bid_ask:
        bid_ask['oiValue'] = oi_value

    candles = _of_build_candles_no_split(klines_norm, tf)
    return {'ok':True,'exchange':'okx','sourceLabel':'OKX Swap',
            'symbol':symbol,'displaySymbol':symbol,'timeframe':tf.upper(),
            'candles':candles,'cvd':[None]*len(candles),
            'currentCandle':candles[-1] if candles else None,
            'lastClosedCandle':candles[-2] if len(candles)>=2 else None,
            'previousCandle':candles[-3] if len(candles)>=3 else None,
            'oiDeltaHistory':[None,None,None],
            'oiValueUSDT':oi_value,'fundingHistory':_of_pad_history(fund_hist),
            'bidAsk':bid_ask,'buySellAvailable':False,'errors':errors}


def fetch_of_mexc(symbol: str, tf: str) -> dict:
    errors = []
    iv, err = normalize_of_timeframe('mexc', tf)
    if err:
        return {'ok':False,'errors':[err]}
    iv_ms_map = {'Min60':3600000,'Hour4':14400000,'Day1':86400000}
    iv_ms = iv_ms_map.get(iv, 3600000)

    klines_norm = []
    try:
        r = req.get(f"{MEXC_PERP_API}/kline",
                    params={'symbol':symbol,'interval':iv,'limit':4}, timeout=10)
        if r.status_code == 200:
            data = r.json().get('data',{})
            times  = data.get('time',[])
            opens  = data.get('open',[])
            highs  = data.get('high',[])
            lows   = data.get('low',[])
            closes = data.get('close',[])
            vols   = data.get('vol',[])
            for i in range(len(times)):
                open_ms = int(times[i]) * 1000
                klines_norm.append({'open':float(opens[i]) if i<len(opens) else 0,
                    'high':float(highs[i]) if i<len(highs) else 0,
                    'low': float(lows[i])  if i<len(lows)  else 0,
                    'close':float(closes[i]) if i<len(closes) else 0,
                    'volume':float(vols[i]) if i<len(vols) else 0,
                    'openTime':open_ms,'closeTimeMs':open_ms+iv_ms-1})
    except Exception as e:
        errors.append(f"klines: {e}")
    if not klines_norm or len(klines_norm) < 2:
        return {'ok':False,'errors':[f'Candle data unavailable for {symbol}']+errors}
    current_price = klines_norm[-1]['close']

    oi_value, fund_hist = None, []
    try:
        r = req.get(f"{MEXC_PERP_API}/ticker", params={'symbol':symbol}, timeout=10)
        if r.status_code == 200:
            body = r.json().get('data',[])
            ticker = None
            if isinstance(body, list):
                for t in body:
                    if t.get('symbol','').lower() == symbol.lower():
                        ticker = t; break
            elif isinstance(body, dict):
                ticker = body
            if ticker:
                oi_value = safe_float(ticker.get('holdVol',0)) * current_price
                fr = safe_float(ticker.get('fundingRate',0))
                fund_hist = [round(fr * 100, 6)]
    except Exception as e:
        errors.append(f"OI/funding: {e}")

    bid_ask = None
    try:
        r = req.get(f"{MEXC_PERP_API}/depth/{symbol}", timeout=10)
        if r.status_code == 200:
            d = r.json().get('data',{})
            bids = [[str(b[0]),str(b[1])] for b in d.get('bids',[])]
            asks = [[str(a[0]),str(a[1])] for a in d.get('asks',[])]
            bid_ask = process_order_book_levels(bids, asks, current_price, _of_tf_range_pct(tf))
            if bid_ask:
                bid_ask['tfRangePct'] = _of_tf_range_pct(tf)
    except Exception as e:
        errors.append(f"depth: {e}")
    if bid_ask:
        bid_ask['oiValue'] = oi_value

    candles = _of_build_candles_no_split(klines_norm, tf)
    return {'ok':True,'exchange':'mexc','sourceLabel':'MEXC Contract',
            'symbol':symbol,'displaySymbol':symbol,'timeframe':tf.upper(),
            'candles':candles,'cvd':[None]*len(candles),
            'currentCandle':candles[-1] if candles else None,
            'lastClosedCandle':candles[-2] if len(candles)>=2 else None,
            'previousCandle':candles[-3] if len(candles)>=3 else None,
            'oiDeltaHistory':[None,None,None],
            'oiValueUSDT':oi_value,'fundingHistory':_of_pad_history(fund_hist),
            'bidAsk':bid_ask,'buySellAvailable':False,'oiDataQuality':'native_holdVol','errors':errors}


# Supported timeframes per exchange (uppercase); used for early validation.
# MEXC contract API supports Min60/Hour4/Day1 only — 6H and 12H have no equivalent interval.
_OF_SUPPORTED_TF: Dict[str, List[str]] = {
    'binance': ['1H', '4H', '6H', '12H', '1D'],
    'bybit':   ['1H', '4H', '6H', '12H', '1D'],
    'okx':     ['1H', '4H', '6H', '12H', '1D'],
    'mexc':    ['1H', '4H', '1D'],
}
_OF_SOURCE_LABEL: Dict[str, str] = {
    'binance': 'Binance Futures',
    'bybit':   'Bybit USDT Perp',
    'okx':     'OKX Swap',
    'mexc':    'MEXC Contract',
}


@app.route("/api/order-flow")
def api_order_flow():
    """Order Flow v5 — exchange-aware. Returns normalized OF data for Binance/Bybit/OKX/MEXC."""
    exchange = (request.args.get('exchange','binance') or 'binance').lower().strip()
    symbol   = (request.args.get('symbol','') or '').strip().upper()
    tf       = (request.args.get('timeframe','1h') or '1h').strip().lower()
    if not symbol:
        return jsonify({'ok':False,'errors':['symbol is required']}), 400
    supported = _OF_SUPPORTED_TF.get(exchange, [])
    if supported and tf.upper() not in supported:
        exc_label = _OF_SOURCE_LABEL.get(exchange, exchange.upper())
        tf_up     = tf.upper()
        sup_str   = ', '.join(supported)
        msg = f"{exc_label} does not support {tf_up} Order Flow in REST mode. Please select {sup_str}."
        return jsonify({
            'ok': False,
            'errorCode': 'UNSUPPORTED_TIMEFRAME',
            'exchange': exchange,
            'timeframe': tf_up,
            'supportedTimeframes': supported,
            'message': msg,
            'errors': [msg],
        })
    norm_sym = normalize_of_symbol(exchange, symbol)
    if exchange == 'binance':
        data = fetch_of_binance(norm_sym, tf)
    elif exchange == 'bybit':
        data = fetch_of_bybit(norm_sym, tf)
    elif exchange == 'okx':
        data = fetch_of_okx(norm_sym, tf)
    elif exchange == 'mexc':
        data = fetch_of_mexc(norm_sym, tf)
    else:
        return jsonify({'ok':False,'errors':[f'Order Flow not supported for exchange: {exchange}']}), 400
    return jsonify(data)


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
                    obs, _ = detect_obs(o, h, l, c, v, 5, 10, max_ob=3)
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
    # Only runs when auto=1 AND no valid zones passed
    if auto_mode and (zone_top <= 0 or zone_bottom <= 0):
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
                    obs_, _ = detect_obs(o_, h_, l_, c_, v_, 5, 10, max_ob=5)
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
                open_      = float(k[1])
                high       = float(k[2])
                low        = float(k[3])
                close      = float(k[4])
                total_vol  = float(k[5])
                taker_buy  = float(k[9])
                taker_sell = total_vol - taker_buy
                candles_data.append({
                    "open":  open_,
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
