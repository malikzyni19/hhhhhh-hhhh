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


# ── Phase 5B — Intelligence admin page (read-only) ─────────────────────────
@admin_bp.route("/intelligence")
@admin_required
def intelligence():
    return render_template("admin/intelligence.html")


# ─────────────────────────────────────────────────────────────────────────────
# Phase 5A — read-only intelligence stats (admin-only, never mutates DB)
# GET /admin/intelligence/stats
# Optional params: source, module, timeframe, limit_recent (max 100)
# ─────────────────────────────────────────────────────────────────────────────
@admin_bp.route("/intelligence/stats")
@admin_required
def intelligence_stats():
    try:
        from models import SignalEvent, SignalOutcome

        # ── Parse filters ────────────────────────────────────────────────
        source_param = request.args.get("source", "live")
        module_param = request.args.get("module") or None
        tf_param     = request.args.get("timeframe") or None
        try:
            limit_recent = min(int(request.args.get("limit_recent", 50)), 100)
        except (TypeError, ValueError):
            limit_recent = 50

        # Phase 6C: OB-only main test mode.
        # Default (no module filter) → count only module="ob".
        # Explicit module filter (e.g. ?module=bb) passes through as debug view.
        _OB_ONLY_MODE     = True
        _PAUSED_MODULES   = ["bb", "fvg", "fib_confluence"]
        ob_only_active    = (module_param is None) and _OB_ONLY_MODE
        effective_module  = "ob" if ob_only_active else module_param

        # ── Build outer-joined query so signals without outcomes appear ──
        q = (
            db.session.query(SignalEvent, SignalOutcome)
            .outerjoin(SignalOutcome, SignalEvent.signal_id == SignalOutcome.signal_id)
        )
        if source_param != "all":
            q = q.filter(SignalEvent.source == source_param)
        if effective_module:
            q = q.filter(SignalEvent.module == effective_module)
        if tf_param:
            q = q.filter(SignalEvent.timeframe == tf_param)

        rows = q.order_by(SignalEvent.detected_at.desc()).all()

        # ── Helper: safe percentage (returns None when denom == 0) ───────
        def _pct(num, denom):
            return round(num / denom * 100, 2) if denom > 0 else None

        # ── Status counts ────────────────────────────────────────────────
        sc = {}
        for ev, _ in rows:
            sc[ev.status] = sc.get(ev.status, 0) + 1

        waiting   = sc.get("WAITING_FOR_ENTRY", 0)
        entered   = sc.get("ENTERED",           0)
        won       = sc.get("WON",               0)
        lost      = sc.get("LOST",              0)
        expired   = sc.get("EXPIRED",           0)
        ambiguous = sc.get("AMBIGUOUS",         0)

        pending_total  = waiting + entered
        resolved_total = won + lost + expired + ambiguous
        clean_resolved = won + lost   # only WON/LOST for win_rate denominator

        # ── Win rates ────────────────────────────────────────────────────
        win_rates = {
            "win_rate_resolved":      _pct(won, resolved_total),
            "win_rate_entered":       _pct(won, clean_resolved),
            "loss_rate_resolved":     _pct(lost, resolved_total),
            "ambiguous_rate_resolved":_pct(ambiguous, resolved_total),
        }

        # ── By-module breakdown ──────────────────────────────────────────
        mod_data: dict = {}
        for ev, _ in rows:
            m = ev.module
            if m not in mod_data:
                mod_data[m] = {
                    "module": m, "total": 0,
                    "waiting_for_entry": 0, "entered": 0,
                    "won": 0, "lost": 0, "expired": 0, "ambiguous": 0,
                }
            d = mod_data[m]
            d["total"] += 1
            s = ev.status
            if   s == "WAITING_FOR_ENTRY": d["waiting_for_entry"] += 1
            elif s == "ENTERED":           d["entered"]    += 1
            elif s == "WON":               d["won"]        += 1
            elif s == "LOST":              d["lost"]       += 1
            elif s == "EXPIRED":           d["expired"]    += 1
            elif s == "AMBIGUOUS":         d["ambiguous"]  += 1

        by_module = sorted(
            [{**d, "sample_reliable": d["total"] >= 30} for d in mod_data.values()],
            key=lambda x: x["total"], reverse=True,
        )

        # ── By-timeframe breakdown ───────────────────────────────────────
        tf_data: dict = {}
        for ev, _ in rows:
            tf = ev.timeframe
            if tf not in tf_data:
                tf_data[tf] = {
                    "timeframe": tf, "total": 0,
                    "waiting_for_entry": 0, "entered": 0,
                    "won": 0, "lost": 0, "expired": 0, "ambiguous": 0,
                }
            d = tf_data[tf]
            d["total"] += 1
            s = ev.status
            if   s == "WAITING_FOR_ENTRY": d["waiting_for_entry"] += 1
            elif s == "ENTERED":           d["entered"]    += 1
            elif s == "WON":               d["won"]        += 1
            elif s == "LOST":              d["lost"]       += 1
            elif s == "EXPIRED":           d["expired"]    += 1
            elif s == "AMBIGUOUS":         d["ambiguous"]  += 1

        by_timeframe = sorted(
            [{**d, "sample_reliable": d["total"] >= 30} for d in tf_data.values()],
            key=lambda x: x["total"], reverse=True,
        )

        # ── By score bucket ──────────────────────────────────────────────
        _buckets = [
            {"bucket": "80-100", "lo": 80, "hi": 100, "total": 0, "won": 0, "lost": 0},
            {"bucket": "60-79",  "lo": 60, "hi": 79,  "total": 0, "won": 0, "lost": 0},
            {"bucket": "40-59",  "lo": 40, "hi": 59,  "total": 0, "won": 0, "lost": 0},
            {"bucket": "0-39",   "lo": 0,  "hi": 39,  "total": 0, "won": 0, "lost": 0},
        ]
        for ev, _ in rows:
            score = ev.score or 0
            for b in _buckets:
                if b["lo"] <= score <= b["hi"]:
                    b["total"] += 1
                    if ev.status == "WON":  b["won"]  += 1
                    if ev.status == "LOST": b["lost"] += 1
                    break

        by_score_bucket = [
            {
                "bucket":   b["bucket"],
                "total":    b["total"],
                "won":      b["won"],
                "lost":     b["lost"],
                "win_rate": _pct(b["won"], b["won"] + b["lost"]),
            }
            for b in _buckets
        ]

        # ── Recent signals with outcome fields ───────────────────────────
        recent_signals = [
            {
                "signal_id":    ev.signal_id,
                "pair":         ev.pair,
                "module":       ev.module,
                "timeframe":    ev.timeframe,
                "direction":    ev.direction,
                "score":        ev.score,
                "status":       ev.status,
                "result":       oc.result        if oc else None,
                "result_reason":oc.result_reason if oc else None,
                "detected_at":  ev.detected_at.isoformat() if ev.detected_at else None,
                "zone_high":    ev.zone_high,
                "zone_low":     ev.zone_low,
                "entry_price":  oc.entry_price   if oc else None,
                "target_price": oc.target_price  if oc else None,
                "stop_price":   oc.stop_price    if oc else None,
            }
            for ev, oc in rows[:limit_recent]
        ]

        return jsonify({
            "ok": True,
            "main_module_mode":  "ob_only" if ob_only_active else "filtered",
            "excluded_modules":  _PAUSED_MODULES if ob_only_active else [],
            "filters": {
                "source":           source_param,
                "module":           module_param,
                "effective_module": effective_module,
                "timeframe":        tf_param,
            },
            "totals": {
                "total_signals":    len(rows),
                "waiting_for_entry":waiting,
                "entered":          entered,
                "won":              won,
                "lost":             lost,
                "expired":          expired,
                "ambiguous":        ambiguous,
                "pending_total":    pending_total,
                "resolved_total":   resolved_total,
            },
            "win_rates":      win_rates,
            "by_module":      by_module,
            "by_timeframe":   by_timeframe,
            "by_score_bucket":by_score_bucket,
            "recent_signals": recent_signals,
        })

    except Exception as _stats_err:
        return jsonify({"ok": False, "error": str(_stats_err)}), 500


# ─────────────────────────────────────────────────────────────────────────────
# Phase 6A — Auto Resolver Settings (admin-only, no runner, no auto-execution)
# GET  /admin/intelligence/auto-resolver-settings  → read settings (safe)
# POST /admin/intelligence/auto-resolver-settings  → save settings only
# ─────────────────────────────────────────────────────────────────────────────

_AR_VALID_INTERVALS = {15, 30, 60, 120}
_AR_VALID_MODES     = {"dry_run", "commit"}


def _ar_get_or_create():
    """Return singleton IntelligenceSettings row (id=1), creating with defaults if absent."""
    from models import IntelligenceSettings
    try:
        row = db.session.get(IntelligenceSettings, 1)
        if row is None:
            row = IntelligenceSettings(id=1)
            db.session.add(row)
            db.session.commit()
        return row
    except Exception:
        db.session.rollback()
        try:
            return db.session.get(IntelligenceSettings, 1)
        except Exception:
            return None


def _ar_to_dict(row):
    """Serialize IntelligenceSettings to API response dict."""
    installed     = row.runner_installed
    runner_status = "INSTALLED_DRY_RUN_ONLY" if installed else "NOT_INSTALLED"
    return {
        "auto_resolver_enabled": row.auto_resolver_enabled,
        "interval_minutes":      row.auto_resolver_interval_minutes,
        "limit_per_run":         row.auto_resolver_limit,
        "mode":                  row.auto_resolver_mode,
        "runner_installed":      installed,
        "runner_status":         runner_status,
        "last_saved_at":         row.last_saved_at.isoformat() if row.last_saved_at else None,
        "last_saved_by":         row.last_saved_by,
        "last_run_at":           row.last_run_at.isoformat()   if row.last_run_at   else None,
        "last_run_summary":      row.last_run_summary,
    }


@admin_bp.route("/intelligence/auto-resolver-settings", methods=["GET", "POST"])
@admin_required
def intelligence_auto_resolver_settings():
    from datetime import datetime, timezone as _tz

    if request.method == "GET":
        try:
            row = _ar_get_or_create()
            if row is None:
                return jsonify({"ok": False, "error": "Could not load settings"}), 500
            return jsonify({"ok": True, "settings": _ar_to_dict(row)})
        except Exception as _e:
            return jsonify({"ok": False, "error": str(_e)}), 500

    # ── POST: validate inputs, save settings only — no resolver called ────────
    try:
        body = request.get_json(silent=True) or {}

        enabled  = bool(body.get("auto_resolver_enabled", False))
        interval = int(body.get("interval_minutes",  30))
        limit    = int(body.get("limit_per_run",     20))
        mode     = str(body.get("mode", "dry_run")).strip().lower()

        errors = []
        if interval not in _AR_VALID_INTERVALS:
            errors.append(
                f"interval_minutes must be one of {sorted(_AR_VALID_INTERVALS)}, got {interval}"
            )
        if not (1 <= limit <= 100):
            errors.append(f"limit_per_run must be 1–100, got {limit}")
        if mode not in _AR_VALID_MODES:
            errors.append(f"mode must be 'dry_run' or 'commit', got {mode!r}")
        if errors:
            return jsonify({"ok": False, "error": "; ".join(errors)}), 400

        row = _ar_get_or_create()
        if row is None:
            return jsonify({"ok": False, "error": "Could not load settings row"}), 500

        row.auto_resolver_enabled          = enabled
        row.auto_resolver_interval_minutes = interval
        row.auto_resolver_limit            = limit
        row.auto_resolver_mode             = mode
        # runner_installed is managed exclusively by auto_resolver_runner.py —
        # never reset it here; settings saves must not affect runner state.
        row.last_saved_at                  = datetime.now(_tz.utc)
        row.last_saved_by                  = (
            current_user.id if current_user.is_authenticated else None
        )
        db.session.commit()

        msg = "Auto resolver settings saved. Runner is not installed yet."
        if enabled:
            msg += (
                " Auto Resolver is enabled in settings, but background runner"
                " is not active until Phase 6B."
            )
        if mode == "commit":
            msg += (
                " Commit mode will update DB statuses when runner is enabled"
                " in Phase 6B."
            )

        return jsonify({"ok": True, "message": msg, "settings": _ar_to_dict(row)})

    except (ValueError, TypeError) as _ve:
        return jsonify({"ok": False, "error": f"Invalid input: {_ve}"}), 400
    except Exception as _e:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(_e)}), 500


# ─────────────────────────────────────────────────────────────────────────────
# Phase 6B.5 — Resolver diagnostic audit (read-only, dry-run counterfactuals)
# GET /admin/intelligence/resolver-audit
# Params: limit (default 20, max 100), module, timeframe, pair, result_filter
# ─────────────────────────────────────────────────────────────────────────────
@admin_bp.route("/intelligence/resolver-audit", methods=["GET"])
@admin_required
def intelligence_resolver_audit():
    try:
        from resolver_audit import audit_resolver_outcomes

        try:
            limit = min(int(request.args.get("limit", 20)), 100)
        except (TypeError, ValueError):
            limit = 20

        module        = request.args.get("module")    or None
        timeframe     = request.args.get("timeframe") or None
        pair          = request.args.get("pair")      or None
        # Accept both ?result= (UI/docs) and ?result_filter= (legacy) — result wins
        result_filter = (
            request.args.get("result") or
            request.args.get("result_filter") or
            None
        )
        compact             = request.args.get("compact",              "0") in ("1", "true", "yes")
        include_fvg         = request.args.get("include_fvg_standalone","0") in ("1", "true", "yes")
        bqg                 = request.args.get("breaker_quality_guard", "0") in ("1", "true", "yes")
        include_non_ob      = request.args.get("include_non_ob_debug",  "0") in ("1", "true", "yes")

        result = audit_resolver_outcomes(
            limit=limit,
            module=module,
            timeframe=timeframe,
            pair=pair,
            result_filter=result_filter,
            compact=compact,
            include_fvg_standalone=include_fvg,
            breaker_quality_guard=bqg,
            include_non_ob_debug=include_non_ob,
        )
        return jsonify(result)

    except Exception as _e:
        return jsonify({"ok": False, "error": str(_e)}), 500


# ─────────────────────────────────────────────────────────────────────────────
# Phase 7A — OB-only backtest (read-only, dry-run, never mutates DB)
# GET /admin/intelligence/backtest-ob
# Params: limit, timeframe, pair, setup_type, source, result
# ─────────────────────────────────────────────────────────────────────────────
@admin_bp.route("/intelligence/backtest-ob", methods=["GET"])
@admin_required
def intelligence_backtest_ob():
    try:
        from backtest_ob import run_ob_backtest

        try:
            limit = min(int(request.args.get("limit", 100)), 500)
        except (TypeError, ValueError):
            limit = 100

        timeframe    = request.args.get("timeframe")   or None
        pair         = request.args.get("pair")        or None
        setup_type   = request.args.get("setup_type")  or None
        source       = request.args.get("source",  "live")
        result       = (
            request.args.get("result") or
            request.args.get("result_filter") or
            None
        )

        result = run_ob_backtest(
            limit=limit,
            timeframe=timeframe,
            pair=pair,
            setup_type=setup_type,
            source=source,
            result_filter=result,
        )
        return jsonify(result)

    except Exception as _e:
        return jsonify({"ok": False, "error": str(_e)}), 500

