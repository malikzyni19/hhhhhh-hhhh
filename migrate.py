"""
Run once after deploy to seed users + roles + settings into PostgreSQL.
Usage: python3 migrate.py
Requires DATABASE_URL env var to be set.
"""
import json
import os
from main import app, _USERS_DB, APP_PASSWORD
from models import db, User, RolePermission, GlobalSetting, ALL_MODULES, ALL_TABS, ALL_EXCHANGES, ALL_TIMEFRAMES

ADMIN_USER = "zyni"

ROLE_SEEDS = {
    "admin": {
        "daily_tokens":        999999,
        "max_pairs_per_scan":  500,
        "max_pairs_per_cycle": 500,
        "allowed_modules":     ALL_MODULES,
        "allowed_tabs":        ALL_TABS,
        "allowed_exchanges":   ALL_EXCHANGES,
        "allowed_timeframes":  ALL_TIMEFRAMES,
    },
    "user": {
        "daily_tokens":        500,
        "max_pairs_per_scan":  100,
        "max_pairs_per_cycle": 50,
        "allowed_modules":     ["ob", "fvg", "fib", "bias"],
        "allowed_tabs":        ["scan", "pairs", "settings", "bias"],
        "allowed_exchanges":   ["binance"],
        "allowed_timeframes":  ["15m", "1h", "4h"],
    },
    "guest": {
        "daily_tokens":        50,
        "max_pairs_per_scan":  20,
        "max_pairs_per_cycle": 10,
        "allowed_modules":     ["ob"],
        "allowed_tabs":        ["scan"],
        "allowed_exchanges":   ["binance"],
        "allowed_timeframes":  ["1h"],
    },
}

SETTING_SEEDS = [
    ("maintenance_mode",       "false"),
    ("maintenance_message",    "We're upgrading the system. We'll be back shortly!"),
    ("allow_guest_access",     "true"),
    ("guest_session_hours",    "24"),
    ("guest_expire_days",      "30"),
    ("default_exchange",       "binance"),
]

with app.app_context():
    db.create_all()
    print("Tables ensured.\n")

    # ── Users ──────────────────────────────────────────────────
    for username, password in _USERS_DB.items():
        existing = User.query.filter_by(username=username).first()
        if existing:
            print(f"  Skipped  : {username} (already exists)")
            continue
        role = "admin" if username == ADMIN_USER else "user"
        u = User(username=username, role=role, status="active")
        u.set_password(password)
        db.session.add(u)
        db.session.commit()
        print(f"  Created  : {username} ({role})")

    # ── Role permissions ───────────────────────────────────────
    print()
    for role, fields in ROLE_SEEDS.items():
        existing = RolePermission.query.filter_by(role=role).first()
        if existing:
            print(f"  Role skip: {role} (already exists)")
            continue
        rp = RolePermission(role=role)
        rp.daily_tokens        = fields["daily_tokens"]
        rp.max_pairs_per_scan  = fields["max_pairs_per_scan"]
        rp.max_pairs_per_cycle = fields["max_pairs_per_cycle"]
        rp.allowed_modules     = json.dumps(fields["allowed_modules"])
        rp.allowed_tabs        = json.dumps(fields["allowed_tabs"])
        rp.allowed_exchanges   = json.dumps(fields["allowed_exchanges"])
        rp.allowed_timeframes  = json.dumps(fields["allowed_timeframes"])
        db.session.add(rp)
        db.session.commit()
        print(f"  Role seed: {role}")

    # ── Global settings ────────────────────────────────────────
    print()
    for key, value in SETTING_SEEDS:
        existing = GlobalSetting.query.filter_by(key=key).first()
        if existing:
            print(f"  Setting skip: {key}")
            continue
        gs = GlobalSetting(key=key, value=value)
        db.session.add(gs)
        db.session.commit()
        print(f"  Setting seed: {key} = {value}")

    print("\nDone.")
