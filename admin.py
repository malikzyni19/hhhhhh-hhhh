import re
import json
import os
from flask import Blueprint, render_template, redirect, url_for, request, session, flash, jsonify
from flask_login import login_user, logout_user, current_user
from functools import wraps
from datetime import datetime, timezone

from models import (db, User, AdminLog, GlobalSetting, RolePermission, UserPermission,
                    LoginHistory, DailyTokenUsage,
                    ALL_MODULES, ALL_TABS, ALL_EXCHANGES, ALL_TIMEFRAMES)
from permissions import get_user_permissions, save_user_permissions, _bust_cache

admin_bp = Blueprint("admin", __name__, url_prefix="/admin")

_USERNAME_RE = re.compile(r'^[a-zA-Z0-9_]{3,30}$')


def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated or not current_user.is_admin:
            return redirect(url_for("admin.login"))
        return f(*args, **kwargs)
    return decorated


def _get_ip():
    return request.headers.get("X-Forwarded-For", request.remote_addr or "unknown").split(",")[0].strip()


def _log_action(action: str, details: str = None, target_user_id: int = None):
    try:
        entry = AdminLog(
            admin_id=current_user.id,
            action=action,
            target_user_id=target_user_id,
            details=details,
            ip_address=_get_ip(),
        )
        db.session.add(entry)
        db.session.commit()
    except Exception as e:
        print(f"[ADMIN-LOG] Failed to log action: {e}")


def _admin_count():
    try:
        return User.query.filter_by(role="admin", status="active").count()
    except Exception:
        return 1


# ── Login ──────────────────────────────────────────────────────────
@admin_bp.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated and current_user.is_admin:
        return redirect(url_for("admin.dashboard"))

    error = None
    if request.method == "POST":
        username = request.form.get("username", "").strip().lower()
        password = request.form.get("password", "")

        user = User.query.filter_by(username=username).first()

        if not user or not user.check_password(password):
            error = "Invalid username or password."
        elif not user.is_admin:
            error = "Access denied — admin only."
        elif user.status != "active":
            error = f"Account is {user.status}. Contact support."
        else:
            login_user(user, remember=False)
            session["is_admin"] = True

            user.last_login_at = datetime.now(timezone.utc)
            user.last_login_ip = _get_ip()
            db.session.commit()

            return redirect(url_for("admin.dashboard"))

    return render_template("admin/login.html", error=error)


# ── Logout ─────────────────────────────────────────────────────────
@admin_bp.route("/logout")
def logout():
    if current_user.is_authenticated:
        _log_action("logout")
    logout_user()
    session.pop("is_admin", None)
    return redirect(url_for("admin.login"))


# ── Dashboard ──────────────────────────────────────────────────────
@admin_bp.route("/")
@admin_required
def dashboard():
    try:
        total_users  = User.query.filter(User.role != "admin").count()
        active_users = User.query.filter_by(status="active").filter(User.role != "admin").count()
        paused_users = User.query.filter_by(status="paused").count()
        admin_count  = User.query.filter_by(role="admin").count()

        today = datetime.now(timezone.utc).date()
        logs_today = AdminLog.query.filter(
            db.func.date(AdminLog.created_at) == today
        ).count()

        recent_logs = (
            AdminLog.query
            .order_by(AdminLog.created_at.desc())
            .limit(10)
            .all()
        )

        recent_users = (
            User.query
            .order_by(User.created_at.desc())
            .limit(5)
            .all()
        )

        db_connected = True
    except Exception as e:
        print(f"[ADMIN-DASH] DB error: {e}")
        total_users = active_users = paused_users = admin_count = logs_today = 0
        recent_logs = []
        recent_users = []
        db_connected = False

    return render_template(
        "admin/dashboard.html",
        total_users=total_users,
        active_users=active_users,
        paused_users=paused_users,
        admin_count=admin_count,
        logs_today=logs_today,
        recent_logs=recent_logs,
        recent_users=recent_users,
        db_connected=db_connected,
        server_time=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        app_version="1.0.0",
    )



# ── Users List ─────────────────────────────────────────────────────
@admin_bp.route("/users")
@admin_required
def users():
    try:
        status_filter = request.args.get("status")
        role_filter   = request.args.get("role")

        q = User.query.order_by(User.created_at.desc())
        if status_filter:
            q = q.filter_by(status=status_filter)
        if role_filter:
            q = q.filter_by(role=role_filter)

        all_users   = q.all()
        total_count = User.query.count()
    except Exception as e:
        print(f"[ADMIN-USERS] DB error: {e}")
        all_users   = []
        total_count = 0

    return render_template(
        "admin/users.html",
        users=all_users,
        total_count=total_count,
        status_filter=status_filter,
        role_filter=role_filter,
    )


# ── Create User ────────────────────────────────────────────────────
@admin_bp.route("/users/create", methods=["GET", "POST"])
@admin_required
def users_create():
    errors = {}

    if request.method == "POST":
        username = request.form.get("username", "").strip().lower()
        password = request.form.get("password", "")
        confirm  = request.form.get("confirm_password", "")
        role     = request.form.get("role", "user")
        status   = request.form.get("status", "active")
        notes    = request.form.get("notes", "").strip()

        if not username:
            errors["username"] = "Username is required."
        elif not _USERNAME_RE.match(username):
            errors["username"] = "3–30 chars: letters, numbers, underscore only."
        elif User.query.filter_by(username=username).first():
            errors["username"] = f"Username '{username}' is already taken."

        if not password:
            errors["password"] = "Password is required."
        elif len(password) < 8:
            errors["password"] = "Password must be at least 8 characters."

        if not confirm:
            errors["confirm_password"] = "Please confirm the password."
        elif password and password != confirm:
            errors["confirm_password"] = "Passwords do not match."

        if role not in ("user", "admin", "guest"):
            role = "user"
        if status not in ("active", "paused"):
            status = "active"

        if not errors:
            try:
                new_user = User(
                    username=username,
                    role=role,
                    status=status,
                    notes=notes or None,
                )
                new_user.set_password(password)
                db.session.add(new_user)
                db.session.commit()
                _log_action("create_user", f"{username} ({role})", target_user_id=new_user.id)
                flash(f"User '{username}' created successfully.", "success")
                return redirect(url_for("admin.users"))
            except Exception as e:
                db.session.rollback()
                errors["_general"] = f"Database error: {e}"

    return render_template("admin/users/create.html", errors=errors,
                           form=request.form if request.method == "POST" else {})


# ── Edit User ──────────────────────────────────────────────────────
@admin_bp.route("/users/<int:user_id>/edit", methods=["GET", "POST"])
@admin_required
def users_edit(user_id):
    user = User.query.get_or_404(user_id)
    errors = {}
    changed = []

    if request.method == "POST":
        new_username = request.form.get("username", "").strip().lower()
        new_role     = request.form.get("role", user.role)
        new_status   = request.form.get("status", user.status)
        new_notes    = request.form.get("notes", "").strip()
        new_password = request.form.get("new_password", "")
        confirm_pwd  = request.form.get("confirm_password", "")

        if not new_username:
            errors["username"] = "Username is required."
        elif not _USERNAME_RE.match(new_username):
            errors["username"] = "3–30 chars: letters, numbers, underscore only."
        elif new_username != user.username and User.query.filter_by(username=new_username).first():
            errors["username"] = f"Username '{new_username}' is already taken."

        if new_password:
            if len(new_password) < 8:
                errors["new_password"] = "Password must be at least 8 characters."
            elif new_password != confirm_pwd:
                errors["confirm_password"] = "Passwords do not match."

        if new_role not in ("user", "admin", "guest"):
            new_role = user.role
        if new_status not in ("active", "paused", "banned"):
            new_status = user.status

        # Guard: cannot demote last admin
        if user.role == "admin" and new_role != "admin" and _admin_count() <= 1:
            errors["role"] = "Cannot change role — this is the last active admin."

        if not errors:
            try:
                if new_username != user.username:
                    changed.append(f"username: {user.username}→{new_username}")
                    user.username = new_username
                if new_role != user.role:
                    changed.append(f"role: {user.role}→{new_role}")
                    user.role = new_role
                if new_status != user.status:
                    changed.append(f"status: {user.status}→{new_status}")
                    user.status = new_status
                if new_notes != (user.notes or ""):
                    user.notes = new_notes or None
                if new_password:
                    user.set_password(new_password)
                    changed.append("password updated")

                db.session.commit()
                detail = ", ".join(changed) if changed else "no changes"
                _log_action("edit_user", detail, target_user_id=user.id)
                flash(f"User '{user.username}' updated.", "success")
                return redirect(url_for("admin.users"))
            except Exception as e:
                db.session.rollback()
                errors["_general"] = f"Database error: {e}"


    # Gather extra context
    eff = {}
    user_perm = None
    login_history = []
    stats = None
    try:
        eff       = get_user_permissions(user)
        user_perm = UserPermission.query.filter_by(user_id=user_id).first()

        login_history = (
            LoginHistory.query
            .filter_by(user_id=user_id)
            .order_by(LoginHistory.logged_in_at.desc())
            .limit(10)
            .all()
        )

        from datetime import date, timedelta
        today = date.today()
        month_start = today.replace(day=1)
        week_start  = today - timedelta(days=today.weekday())

        month_logins = LoginHistory.query.filter(
            LoginHistory.user_id == user_id,
            LoginHistory.logged_in_at >= month_start
        ).count()
        week_logins = LoginHistory.query.filter(
            LoginHistory.user_id == user_id,
            LoginHistory.logged_in_at >= week_start
        ).count()
        month_usage = DailyTokenUsage.query.filter(
            DailyTokenUsage.user_id == user_id,
            DailyTokenUsage.date >= month_start
        ).all()
        month_scans  = sum(u.scan_count  for u in month_usage)
        month_tokens = sum(u.tokens_used for u in month_usage)
        week_usage   = [u for u in month_usage if u.date >= week_start]
        week_scans   = sum(u.scan_count for u in week_usage)
        stats = {
            "month_logins": month_logins, "month_scans": month_scans, "month_tokens": month_tokens,
            "week_logins":  week_logins,  "week_scans":  week_scans,
        }
    except Exception as e:
        print(f"[ADMIN-EDIT] extra context error: {e}")

    return render_template(
        "admin/users/edit.html",
        user=user, errors=errors,
        eff=eff, user_perm=user_perm,
        login_history=login_history, stats=stats,
        all_modules=ALL_MODULES, all_tabs=ALL_TABS,
        all_exchanges=ALL_EXCHANGES, all_timeframes=ALL_TIMEFRAMES,
    )


# ── Delete User ────────────────────────────────────────────────────
@admin_bp.route("/users/<int:user_id>/delete", methods=["POST"])
@admin_required
def users_delete(user_id):
    user = User.query.get_or_404(user_id)

    if user.id == current_user.id:
        flash("You cannot delete your own account.", "error")
        return redirect(url_for("admin.users"))

    if user.role == "admin" and _admin_count() <= 1:
        flash("Cannot delete the last admin account.", "error")
        return redirect(url_for("admin.users"))

    confirm = request.form.get("confirm_username", "").strip().lower()
    if confirm != user.username:
        flash("Confirmation username did not match. User not deleted.", "error")
        return redirect(url_for("admin.users_edit", user_id=user_id))

    try:
        uname = user.username
        urole = user.role
        _log_action("delete_user", f"{uname} ({urole})", target_user_id=user.id)
        db.session.delete(user)
        db.session.commit()
        flash(f"User '{uname}' deleted.", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Delete failed: {e}", "error")

    return redirect(url_for("admin.users"))


# ── Toggle Status ──────────────────────────────────────────────────
@admin_bp.route("/users/<int:user_id>/toggle-status", methods=["POST"])
@admin_required
def users_toggle_status(user_id):
    user = User.query.get_or_404(user_id)

    if user.id == current_user.id:
        return jsonify({"error": "Cannot change your own status."}), 400

    if user.role == "admin" and user.status == "active" and _admin_count() <= 1:
        return jsonify({"error": "Cannot pause the last active admin."}), 400

    try:
        new_status = "paused" if user.status == "active" else "active"
        user.status = new_status
        db.session.commit()
        action = "pause_user" if new_status == "paused" else "unpause_user"
        _log_action(action, user.username, target_user_id=user.id)
        return jsonify({"success": True, "status": new_status})
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


# ── Reset Password ─────────────────────────────────────────────────
@admin_bp.route("/users/<int:user_id>/reset-password", methods=["POST"])
@admin_required
def users_reset_password(user_id):
    user = User.query.get_or_404(user_id)
    data = request.get_json(force=True) or {}

    new_password = data.get("new_password", "")
    confirm      = data.get("confirm_password", "")

    if not new_password:
        return jsonify({"error": "New password is required."}), 400
    if len(new_password) < 8:
        return jsonify({"error": "Password must be at least 8 characters."}), 400
    if new_password != confirm:
        return jsonify({"error": "Passwords do not match."}), 400

    try:
        user.set_password(new_password)
        db.session.commit()
        _log_action("reset_password", f"Admin reset password for {user.username}", target_user_id=user.id)
        return jsonify({"success": True})
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


# ── User Permissions Override ──────────────────────────────────────
@admin_bp.route("/users/<int:user_id>/permissions", methods=["POST"])
@admin_required
def users_save_permissions(user_id):
    user = User.query.get_or_404(user_id)
    data = request.get_json(force=True) or {}

    if data.get("reset"):
        try:
            up = UserPermission.query.filter_by(user_id=user_id).first()
            if up:
                db.session.delete(up)
                db.session.commit()
            _bust_cache(user_id)
            _log_action("reset_permissions", f"Reset to role defaults for {user.username}", target_user_id=user_id)
            return jsonify({"success": True, "msg": "Permissions reset to role defaults."})
        except Exception as e:
            db.session.rollback()
            return jsonify({"error": str(e)}), 500

    overrides = {}
    for field in ("daily_tokens", "max_pairs_per_scan", "max_pairs_per_cycle"):
        v = data.get(field)
        overrides[field] = int(v) if v not in (None, "", "null") else None

    for field in ("allowed_modules", "allowed_tabs", "allowed_exchanges", "allowed_timeframes"):
        v = data.get(field)
        overrides[field] = v if isinstance(v, list) else None

    try:
        save_user_permissions(user_id, overrides, current_user.id)
        _log_action("edit_permissions", f"Updated permissions for {user.username}", target_user_id=user_id)
        return jsonify({"success": True})
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


# ── Settings ───────────────────────────────────────────────────────
def _get_setting(key, default=""):
    try:
        s = GlobalSetting.query.filter_by(key=key).first()
        return s.value if s and s.value is not None else default
    except Exception:
        return default


def _set_setting(key, value, description=None):
    try:
        s = GlobalSetting.query.filter_by(key=key).first()
        if s:
            s.value = value
            s.updated_at = datetime.now(timezone.utc)
            s.updated_by = current_user.id
        else:
            s = GlobalSetting(key=key, value=value, description=description,
                              updated_by=current_user.id)
            db.session.add(s)
    except Exception as e:
        print(f"[SETTINGS] Error setting {key}: {e}")


@admin_bp.route("/settings", methods=["GET", "POST"])
@admin_required
def settings():
    role_perms = {}
    try:
        for role in ("admin", "user", "guest"):
            rp = RolePermission.query.filter_by(role=role).first()
            if rp:
                role_perms[role] = {
                    "daily_tokens":        rp.daily_tokens,
                    "max_pairs_per_scan":  rp.max_pairs_per_scan,
                    "max_pairs_per_cycle": rp.max_pairs_per_cycle,
                    "allowed_modules":     json.loads(rp.allowed_modules or "[]"),
                    "allowed_tabs":        json.loads(rp.allowed_tabs    or "[]"),
                    "allowed_exchanges":   json.loads(rp.allowed_exchanges or "[]"),
                    "allowed_timeframes":  json.loads(rp.allowed_timeframes or "[]"),
                }
    except Exception:
        pass

    if request.method == "POST":
        try:
            _set_setting("maintenance_mode",    request.form.get("maintenance_mode", "false"))
            _set_setting("maintenance_message", request.form.get("maintenance_message", ""))
            _set_setting("default_exchange",    request.form.get("default_exchange", "binance"))
            _set_setting("allow_guest_access",  request.form.get("allow_guest_access", "true"))
            _set_setting("max_guest_tokens",    request.form.get("max_guest_tokens", "50"))
            _set_setting("guest_session_hours", request.form.get("guest_session_hours", "2"))
            _set_setting("guest_expire_days",   request.form.get("guest_expire_days", "30"))

            # Role permissions
            for role in ("admin", "user", "guest"):
                rp = RolePermission.query.filter_by(role=role).first()
                if not rp:
                    rp = RolePermission(role=role)
                    db.session.add(rp)
                prefix = f"role_{role}_"
                rp.daily_tokens        = int(request.form.get(prefix + "daily_tokens", rp.daily_tokens or 500))
                rp.max_pairs_per_scan  = int(request.form.get(prefix + "max_scan",  rp.max_pairs_per_scan  or 100))
                rp.max_pairs_per_cycle = int(request.form.get(prefix + "max_cycle", rp.max_pairs_per_cycle or 50))
                rp.allowed_modules     = json.dumps([m for m in ALL_MODULES    if request.form.get(prefix + "mod_"  + m)])
                rp.allowed_tabs        = json.dumps([t for t in ALL_TABS       if request.form.get(prefix + "tab_"  + t)])
                rp.allowed_exchanges   = json.dumps([e for e in ALL_EXCHANGES  if request.form.get(prefix + "exch_" + e)])
                rp.allowed_timeframes  = json.dumps([f for f in ALL_TIMEFRAMES if request.form.get(prefix + "tf_"   + f)])
                rp.updated_by = current_user.id
                rp.updated_at = datetime.now(timezone.utc)

            db.session.commit()
            _log_action("update_settings", "Updated global settings")
            flash("Settings saved.", "success")
        except Exception as e:
            db.session.rollback()
            flash(f"Error saving settings: {e}", "error")
        return redirect(url_for("admin.settings"))

    cfg = {
        "maintenance_mode":    _get_setting("maintenance_mode",    "false"),
        "maintenance_message": _get_setting("maintenance_message", ""),
        "default_exchange":    _get_setting("default_exchange",    "binance"),
        "allow_guest_access":  _get_setting("allow_guest_access",  "true"),
        "max_guest_tokens":    _get_setting("max_guest_tokens",    "50"),
        "guest_session_hours": _get_setting("guest_session_hours", "2"),
        "guest_expire_days":   _get_setting("guest_expire_days",   "30"),
    }
    try:
        total_users    = User.query.count()
        db_connected   = True
    except Exception:
        total_users    = 0
        db_connected   = False

    return render_template(
        "admin/settings.html",
        cfg=cfg,
        role_perms=role_perms,
        all_modules=ALL_MODULES,
        all_tabs=ALL_TABS,
        all_exchanges=ALL_EXCHANGES,
        all_timeframes=ALL_TIMEFRAMES,
        total_users=total_users,
        db_connected=db_connected,
        server_time=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
    )


# ── Role Permissions Pages ──────────────────────────────────────────
def _get_role_perm(role):
    rp = RolePermission.query.filter_by(role=role).first()
    if not rp:
        from permissions import _ROLE_DEFAULTS
        defaults = _ROLE_DEFAULTS.get(role, _ROLE_DEFAULTS["user"])
        rp = RolePermission(role=role,
            daily_tokens=defaults["daily_tokens"],
            max_pairs_per_scan=defaults["max_pairs_per_scan"],
            max_pairs_per_cycle=defaults["max_pairs_per_cycle"],
            allowed_modules=json.dumps(defaults["allowed_modules"]),
            allowed_tabs=json.dumps(defaults["allowed_tabs"]),
            allowed_exchanges=json.dumps(defaults["allowed_exchanges"]),
            allowed_timeframes=json.dumps(defaults["allowed_timeframes"]),
        )
    return rp


def _parse_rp_lists(rp):
    out = {}
    for field in ("allowed_modules", "allowed_tabs", "allowed_exchanges", "allowed_timeframes"):
        val = getattr(rp, field, None)
        if isinstance(val, list):
            out[field] = val
        elif val:
            try:
                out[field] = json.loads(val)
            except Exception:
                out[field] = []
        else:
            out[field] = []
    return out


@admin_bp.route("/roles/<role>", methods=["GET", "POST"])
@admin_required
def role_edit(role):
    if role not in ("admin", "user", "guest"):
        return redirect(url_for("admin.settings"))

    rp = _get_role_perm(role)

    errors = {}
    if request.method == "POST":
        try:
            rp.daily_tokens        = int(request.form.get("daily_tokens", 500))
            rp.max_pairs_per_scan  = int(request.form.get("max_pairs_per_scan", 100))
            rp.max_pairs_per_cycle = int(request.form.get("max_pairs_per_cycle", 50))
            rp.allowed_modules     = json.dumps(request.form.getlist("allowed_modules"))
            rp.allowed_tabs        = json.dumps(request.form.getlist("allowed_tabs"))
            rp.allowed_exchanges   = json.dumps(request.form.getlist("allowed_exchanges"))
            rp.allowed_timeframes  = json.dumps(request.form.getlist("allowed_timeframes"))
            rp.updated_at          = datetime.now(timezone.utc)
            rp.updated_by          = current_user.id
            if not RolePermission.query.filter_by(role=role).first():
                db.session.add(rp)
            db.session.commit()
            # bust cache for all users of this role
            try:
                from permissions import _CACHE
                users = User.query.filter_by(role=role).all()
                for u in users:
                    _CACHE.pop(u.id, None)
            except Exception:
                pass
            _log_action(f"role_save:{role}", f"Updated {role} permissions")
            flash(f"{role.capitalize()} role permissions saved.")
            return redirect(url_for("admin.role_edit", role=role))
        except Exception as e:
            errors["_general"] = str(e)
            db.session.rollback()

    lists = _parse_rp_lists(rp)
    return render_template(
        "admin/role.html",
        role=role,
        rp=rp,
        lists=lists,
        all_modules=ALL_MODULES,
        all_tabs=ALL_TABS,
        all_exchanges=ALL_EXCHANGES,
        all_timeframes=ALL_TIMEFRAMES,
        errors=errors,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Phase 2B — one-time live DB insert test (admin-only, returns JSON)
# Hit GET /admin/intelligence/db-test to run.
# Safe: only touches pair=TESTUSDT, source=test rows.
# Remove this route after Phase 2B is confirmed.
# ─────────────────────────────────────────────────────────────────────────────
@admin_bp.route("/intelligence/db-test")
@admin_required
def intelligence_db_test():
    from sqlalchemy import text as _sa_text
    from models import SignalEvent, SignalOutcome
    from signal_logger import log_normalized_signal

    _TEST_SIGNAL = {
        "pair":           "TESTUSDT",
        "module":         "ob",
        "timeframe":      "1h",
        "direction":      "bullish",
        "score":          80,
        "zone_high":      100.0,
        "zone_low":       95.0,
        "detected_price": 110.0,
        "setup_type":     "OB_APPROACH",
        "raw_setup":      "OB_APPROACH",
        "raw_meta_json":  '{"test": true}',
    }

    report = {}

    try:
        # ── 1. Migration column check ─────────────────────────────────────
        col_rows = db.session.execute(_sa_text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name='signal_events' "
            "AND column_name IN ('setup_type','raw_setup','raw_meta_json')"
        )).fetchall()
        found_cols = {r[0] for r in col_rows}
        report["migration_columns"] = {
            "found":   sorted(found_cols),
            "missing": sorted({"setup_type","raw_setup","raw_meta_json"} - found_cols),
            "ok":      len(found_cols) == 3,
        }

        # ── 2. First insert ───────────────────────────────────────────────
        r1 = log_normalized_signal(_TEST_SIGNAL, source="test")
        report["first_insert"] = r1

        # ── 3. Duplicate insert ───────────────────────────────────────────
        r2 = log_normalized_signal(_TEST_SIGNAL, source="test")
        report["duplicate_insert"] = r2

        signal_id = r1.get("signal_id") or r2.get("signal_id")

        # ── 4. Verify rows ────────────────────────────────────────────────
        if signal_id:
            event   = SignalEvent.query.filter_by(signal_id=signal_id).first()
            outcome = SignalOutcome.query.filter_by(signal_id=signal_id).first()

            report["signal_event"] = {
                "found":          event is not None,
                "pair":           event.pair           if event else None,
                "module":         event.module         if event else None,
                "status":         event.status         if event else None,
                "source":         event.source         if event else None,
                "setup_type":     event.setup_type     if event else None,
                "raw_setup":      event.raw_setup      if event else None,
                "raw_meta_json":  event.raw_meta_json  if event else None,
            }
            report["signal_outcome"] = {
                "found":                outcome is not None,
                "target_price":         outcome.target_price         if outcome else "N/A",
                "stop_price":           outcome.stop_price           if outcome else "N/A",
                "bounce_threshold_pct": outcome.bounce_threshold_pct if outcome else "N/A",
                "entry_price":          outcome.entry_price          if outcome else "N/A",
                "result":               outcome.result               if outcome else "N/A",
            }
            report["target_stop_null"] = (
                outcome is not None
                and outcome.target_price is None
                and outcome.stop_price   is None
            )

            # ── 5. Clean up test rows ────────────────────────────────────
            try:
                if outcome:
                    db.session.delete(outcome)
                if event:
                    db.session.delete(event)
                db.session.commit()
                report["cleanup"] = "deleted — pair=TESTUSDT source=test rows removed"
            except Exception as _del_err:
                db.session.rollback()
                report["cleanup"] = f"kept (delete failed: {_del_err}) — source=test rows remain"
        else:
            report["signal_event"]  = {"found": False, "note": "no signal_id available"}
            report["signal_outcome"] = {"found": False}
            report["cleanup"]        = "nothing to clean up"

        report["overall_ok"] = (
            report["migration_columns"]["ok"]
            and (r1.get("logged") or r1.get("reason") == "duplicate")
            and (r2.get("reason") == "duplicate" or not r2.get("logged"))
            and report["signal_event"].get("found", False)
            and report["signal_outcome"].get("found", False)
            and report.get("target_stop_null", False)
        )

    except Exception as _test_err:
        db.session.rollback()
        report["error"] = str(_test_err)
        report["overall_ok"] = False

    return jsonify(report)


# ─────────────────────────────────────────────────────────────────────────────
# Phase 3A verification — recent live signal_events (admin-only, read-only)
# Hit GET /admin/intelligence/recent-signals after running a scan.
# Returns last 20 live rows + linked outcomes so you can confirm the hook.
# ─────────────────────────────────────────────────────────────────────────────
@admin_bp.route("/intelligence/recent-signals")
@admin_required
def intelligence_recent_signals():
    from models import SignalEvent, SignalOutcome

    try:
        # Last 20 live rows newest-first
        rows = (SignalEvent.query
                .filter_by(source="live")
                .order_by(SignalEvent.detected_at.desc())
                .limit(20)
                .all())

        signals = []
        for ev in rows:
            outcome = SignalOutcome.query.filter_by(signal_id=ev.signal_id).first()
            signals.append({
                "signal_id":           ev.signal_id,
                "pair":                ev.pair,
                "module":              ev.module,
                "timeframe":           ev.timeframe,
                "direction":           ev.direction,
                "score":               ev.score,
                "zone_high":           ev.zone_high,
                "zone_low":            ev.zone_low,
                "detected_price":      ev.detected_price,
                "detected_at":         ev.detected_at.isoformat() if ev.detected_at else None,
                "exchange":            ev.exchange,
                "status":              ev.status,
                "source":              ev.source,
                "setup_type":          ev.setup_type,
                "raw_setup":           ev.raw_setup,
                "raw_meta_json_len":   len(ev.raw_meta_json) if ev.raw_meta_json else 0,
                "outcome": {
                    "found":                outcome is not None,
                    "target_price":         outcome.target_price         if outcome else "N/A",
                    "stop_price":           outcome.stop_price           if outcome else "N/A",
                    "bounce_threshold_pct": outcome.bounce_threshold_pct if outcome else "N/A",
                    "entry_price":          outcome.entry_price          if outcome else "N/A",
                    "result":               outcome.result               if outcome else "N/A",
                } if outcome else {"found": False},
            })

        # Counts summary
        total_live    = SignalEvent.query.filter_by(source="live").count()
        modules_found = db.session.execute(
            __import__("sqlalchemy").text(
                "SELECT module, COUNT(*) FROM signal_events WHERE source='live' "
                "GROUP BY module ORDER BY COUNT(*) DESC"
            )
        ).fetchall()

        return jsonify({
            "total_live_signals": total_live,
            "modules_breakdown":  [{"module": r[0], "count": r[1]} for r in modules_found],
            "recent_20":          signals,
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ─────────────────────────────────────────────────────────────────────────────
# Phase 4B — manual resolver endpoint (admin-only, JSON, never crashes app)
# GET  /admin/intelligence/resolve-pending           → dry_run forced (read-only)
# POST /admin/intelligence/resolve-pending           → real commit
# POST /admin/intelligence/resolve-pending?dry_run=1 → simulate, no DB write
# GET/POST ?limit=N                                  → override limit (max 100)
# ─────────────────────────────────────────────────────────────────────────────
@admin_bp.route("/intelligence/resolve-pending", methods=["GET", "POST"])
@admin_required
def intelligence_resolve_pending():
    try:
        from outcome_resolver import resolve_pending_admin

        # GET is always read-only — never mutates DB regardless of query params
        if request.method == "GET":
            dry_run = True
        else:
            dry_run = request.args.get("dry_run", "0") in ("1", "true", "yes")

        try:
            limit = min(int(request.args.get("limit", 20)), 100)
        except (TypeError, ValueError):
            limit = 20

        result = resolve_pending_admin(limit=limit, dry_run=dry_run)
        result["mode"] = "dry_run" if dry_run else "commit"
        return jsonify(result)

    except Exception as _rp_err:
        return jsonify({"ok": False, "error": str(_rp_err)}), 500
