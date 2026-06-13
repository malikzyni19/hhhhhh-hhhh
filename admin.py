import re
import json
import os
from flask import Blueprint, render_template, redirect, url_for, request, session, flash, jsonify
from flask_login import login_user, logout_user, current_user
from functools import wraps
from datetime import datetime, timezone

from models import (db, User, AdminLog, GlobalSetting, RolePermission, UserPermission,
                    LoginHistory, DailyTokenUsage, EmailVerification, GuestDevice,
                    BacktestRun, IntelligenceSettings, PasswordResetToken, UserPreference,
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


# ── User Detail JSON (AJAX panel) ─────────────────────────────────
@admin_bp.route("/users/<int:user_id>/detail-json")
@admin_required
def users_detail_json(user_id):
    user = User.query.get_or_404(user_id)
    return jsonify({
        "id":             user.id,
        "username":       user.username,
        "email":          user.email or "",
        "role":           user.role,
        "status":         user.status,
        "email_verified": bool(user.email_verified),
        "created_at":     user.created_at.strftime("%Y-%m-%d %H:%M UTC") if user.created_at else "—",
        "last_login_at":  user.last_login_at.strftime("%Y-%m-%d %H:%M UTC") if user.last_login_at else "Never",
        "last_login_ip":  getattr(user, "last_login_ip", None) or "—",
        "is_self":        user.id == current_user.id,
    })


# ── Delete User ────────────────────────────────────────────────────
@admin_bp.route("/users/<int:user_id>/delete", methods=["POST"])
@admin_required
def users_delete(user_id):
    user = User.query.get_or_404(user_id)
    is_ajax = request.headers.get("X-Requested-With") == "XMLHttpRequest"

    def _err(msg, code=400):
        if is_ajax:
            return jsonify({"error": msg}), code
        flash(msg, "error")
        return redirect(url_for("admin.users"))

    if user.id == current_user.id:
        return _err("You cannot delete your own account.")

    if user.role == "admin" and _admin_count() <= 1:
        return _err("Cannot delete the last admin account.")

    confirm = request.form.get("confirm_username", "").strip()
    if confirm.lower() != user.username.lower():
        return _err("Username confirmation did not match.")

    try:
        uname = user.username
        urole = user.role
        uid   = user.id

        # Log before deletion so the record exists
        _log_action("delete_user", f"{uname} ({urole})", target_user_id=uid)

        # Delete owned records (strict FK — cannot be nullified)
        EmailVerification.query.filter_by(user_id=uid).delete(synchronize_session=False)
        PasswordResetToken.query.filter_by(user_id=uid).delete(synchronize_session=False)
        GuestDevice.query.filter_by(user_id=uid).delete(synchronize_session=False)
        LoginHistory.query.filter_by(user_id=uid).delete(synchronize_session=False)
        DailyTokenUsage.query.filter_by(user_id=uid).delete(synchronize_session=False)
        UserPermission.query.filter_by(user_id=uid).delete(synchronize_session=False)
        UserPreference.query.filter_by(user_id=uid).delete(synchronize_session=False)
        AdminLog.query.filter_by(admin_id=uid).delete(synchronize_session=False)

        # Nullify nullable audit references
        db.session.query(BacktestRun).filter(BacktestRun.run_by == uid).update(
            {"run_by": None}, synchronize_session=False)
        db.session.query(IntelligenceSettings).filter(
            IntelligenceSettings.last_saved_by == uid).update(
            {"last_saved_by": None}, synchronize_session=False)
        db.session.query(GlobalSetting).filter(GlobalSetting.updated_by == uid).update(
            {"updated_by": None}, synchronize_session=False)
        db.session.query(RolePermission).filter(RolePermission.updated_by == uid).update(
            {"updated_by": None}, synchronize_session=False)
        db.session.query(UserPermission).filter(UserPermission.updated_by == uid).update(
            {"updated_by": None}, synchronize_session=False)

        db.session.delete(user)
        db.session.commit()
        _bust_cache(uid)

        if is_ajax:
            return jsonify({"success": True, "username": uname})
        flash(f"User '{uname}' deleted.", "success")
    except Exception as e:
        db.session.rollback()
        if is_ajax:
            return jsonify({"error": "Deletion failed. Please try again."}), 500
        flash(f"Delete failed: {e}", "error")

    return redirect(url_for("admin.users"))


# ── Bulk Delete ────────────────────────────────────────────────────
@admin_bp.route("/users/bulk-delete", methods=["POST"])
@admin_required
def users_bulk_delete():
    """Delete multiple users by ID list. Admins and the current user are always excluded."""
    data = request.get_json(force=True) or {}
    ids  = data.get("ids", [])

    if not ids or not isinstance(ids, list):
        return jsonify({"error": "Provide a non-empty 'ids' list."}), 400
    if len(ids) > 500:
        return jsonify({"error": "Batch too large. Max 500 per request."}), 400

    try:
        targets = (
            User.query
            .filter(User.id.in_(ids))
            .filter(User.role != "admin")
            .filter(User.id != current_user.id)
            .all()
        )
        if not targets:
            return jsonify({"error": "No eligible users found in selection."}), 400

        deleted_ids   = [u.id for u in targets]
        deleted_names = [u.username for u in targets]

        for u in targets:
            uid = u.id
            EmailVerification.query.filter_by(user_id=uid).delete(synchronize_session=False)
            PasswordResetToken.query.filter_by(user_id=uid).delete(synchronize_session=False)
            GuestDevice.query.filter_by(user_id=uid).delete(synchronize_session=False)
            LoginHistory.query.filter_by(user_id=uid).delete(synchronize_session=False)
            DailyTokenUsage.query.filter_by(user_id=uid).delete(synchronize_session=False)
            UserPermission.query.filter_by(user_id=uid).delete(synchronize_session=False)
            UserPreference.query.filter_by(user_id=uid).delete(synchronize_session=False)
            db.session.query(BacktestRun).filter(BacktestRun.run_by == uid).update(
                {"run_by": None}, synchronize_session=False)
            db.session.delete(u)
            _bust_cache(uid)

        db.session.commit()
        _log_action(
            "bulk_delete_users",
            f"Bulk deleted {len(deleted_names)} users: {', '.join(deleted_names[:30])}",
        )
        return jsonify({"ok": True, "deleted": len(deleted_ids), "deleted_ids": deleted_ids})

    except Exception as _e:
        db.session.rollback()
        return jsonify({"error": str(_e)}), 500


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
# /admin/debug/ob-tv-parity  — OB TV volume share parity inspector (admin only)
# ─────────────────────────────────────────────────────────────────────────────

@admin_bp.route("/debug/ob-tv-parity", methods=["GET"])
@admin_required
def debug_ob_tv_parity():
    try:
        from main import (detect_obs, _tv_visible_pool, calculate_tv_ob_volume_share,
                          get_klines_exchange, _TV_OB_PARITY_SETTINGS)
        import datetime as _dt
        import copy as _copy

        symbol         = (request.args.get("symbol")    or "SOONUSDT").strip().upper()
        timeframe      = (request.args.get("timeframe") or "15m").strip()
        exchange       = (request.args.get("exchange")  or "binance").strip().lower()
        market         = (request.args.get("market")    or "perpetual").strip().lower()
        compare_limits = request.args.get("compare_limits", "false").strip().lower() in ("1", "true", "yes")

        # ── Debug-only OB parity variants (diagnostic; production untouched) ──
        # Flags map: variant -> (mitigation_closed_only,
        #                         overlap_effective_zone [DEPRECATED/WRONG],
        #                         bearish_effective_bottom_overlap [CORRECT])
        _VARIANT_FLAGS = {
            "baseline":                                           (False, False, False),
            "closed_mitigation":                                  (True,  False, False),
            "effective_overlap":                                  (False, True,  False),
            "closed_mitigation_effective_overlap":                (True,  True,  False),
            "bearish_effective_bottom_overlap":                   (False, False, True),
            "closed_mitigation_bearish_effective_bottom_overlap": (True,  False, True),
        }
        _ALLOWED_VARIANTS = list(_VARIANT_FLAGS.keys()) + ["all"]
        _VARIANT_NOTES = {
            "baseline":
                "Production behavior. Per-bar mitigation runs through the last "
                "(possibly still-open) candle (i up to n-1). Overlap-deletion "
                "(creation-time + visible-pool) uses the raw hidden extreme zone "
                "[bottom..top].",
            "closed_mitigation":
                "Mitigation skips the last (possibly still-open) candle — only "
                "evaluated for bars i <= n-2. OB creation still scans the full "
                "candle stream. Overlap uses the raw hidden extreme zone.",
            "effective_overlap":
                "DEPRECATED / WRONG — kept for diagnostic comparison only. "
                "Collapsed the NEW OB's extreme edge to avg (bullish bottom→avg, "
                "bearish top→avg). This is not the TV rule; do not use for "
                "production parity.",
            "closed_mitigation_effective_overlap":
                "DEPRECATED / WRONG — kept for comparison only. Combined the "
                "deprecated effective_overlap rule with closed_mitigation.",
            "bearish_effective_bottom_overlap":
                "CORRECT bearish rule. For bearish overlap ONLY, the previous "
                "OB's effective bottom = its avg (TV displays the bearish lower "
                "boundary at avg, not the raw extreme); the new OB's top stays "
                "raw. Bullish overlap stays raw/unchanged (ETH 4H bull already "
                "matches TV with raw overlap). Applied to creation-time deletion "
                "AND visible-pool filtering. Mitigation runs through the last "
                "candle.",
            "closed_mitigation_bearish_effective_bottom_overlap":
                "Combined: closed_mitigation (mitigation skips the last possibly "
                "open candle, i <= n-2) AND the CORRECT bearish_effective_bottom_"
                "overlap rule. This is the candidate production-parity behavior.",
        }
        ob_debug_variant = (request.args.get("ob_debug_variant") or "baseline").strip().lower()
        if ob_debug_variant == "all":
            _variants_to_run = list(_VARIANT_FLAGS.keys())
        elif ob_debug_variant in _VARIANT_FLAGS:
            # Always include baseline for side-by-side comparison.
            _variants_to_run = (["baseline"] if ob_debug_variant == "baseline"
                                else ["baseline", ob_debug_variant])
        else:
            return jsonify({
                "ok": False,
                "error": f"invalid ob_debug_variant '{ob_debug_variant}'",
                "allowed": _ALLOWED_VARIANTS,
            }), 200

        # ── Debug-only OB trace params (diagnostic; only active when trace_ob) ──
        trace_ob   = request.args.get("trace_ob", "false").strip().lower() in ("1", "true", "yes")
        trace_side = (request.args.get("trace_side") or "").strip().lower() or None
        if trace_side not in (None, "bullish", "bearish"):
            trace_side = None
        trace_from = (request.args.get("trace_from") or "").strip() or None
        trace_to   = (request.args.get("trace_to")   or "").strip() or None

        # ── Debug-only OB search-anchor variant ──
        _ANCHOR_MODES = ["baseline", "latest_opposite_pivot"]
        ob_anchor_variant = (request.args.get("ob_anchor_variant") or "").strip().lower() or None
        if ob_anchor_variant is None:
            _anchors_to_run = None          # not requested → endpoint unchanged
        elif ob_anchor_variant == "all":
            _anchors_to_run = list(_ANCHOR_MODES)
        elif ob_anchor_variant in _ANCHOR_MODES:
            _anchors_to_run = (["baseline"] if ob_anchor_variant == "baseline"
                               else ["baseline", "latest_opposite_pivot"])
        else:
            return jsonify({
                "ok": False,
                "error": f"invalid ob_anchor_variant '{ob_anchor_variant}'",
                "allowed": ["baseline", "latest_opposite_pivot", "all"],
            }), 200

        # ── Debug-only OB extreme-tie variant ──
        ob_extreme_tie_mode = (request.args.get("ob_extreme_tie_mode") or "").strip().lower() or None
        if ob_extreme_tie_mode is not None and ob_extreme_tie_mode not in ("first", "last", "all"):
            return jsonify({
                "ok": False,
                "error": f"invalid ob_extreme_tie_mode '{ob_extreme_tie_mode}'",
                "allowed": ["first", "last", "all"],
            }), 200

        # ── Debug-only structure / BOS-method trace params ──
        structure_trace = request.args.get("structure_trace", "false").strip().lower() in ("1", "true", "yes")
        target_side     = (request.args.get("target_side") or "bullish").strip().lower()
        if target_side not in ("bullish", "bearish"):
            target_side = "bullish"
        target_ob_time  = (request.args.get("target_ob_time") or "").strip() or None
        structure_from  = (request.args.get("structure_from") or "").strip() or None
        structure_to    = (request.args.get("structure_to")   or "").strip() or None

        # ── Debug-only response profile + structure-lifecycle trace params ──
        debug_profile = (request.args.get("debug_profile") or "full").strip().lower()
        if debug_profile not in ("full", "compact", "lifecycle_only"):
            debug_profile = "full"
        structure_lifecycle_trace = request.args.get(
            "structure_lifecycle_trace", "false").strip().lower() in ("1", "true", "yes")
        lifecycle_from = (request.args.get("lifecycle_from") or "").strip() or None
        lifecycle_to   = (request.args.get("lifecycle_to")   or "").strip() or None
        _variant_explicitly_requested = request.args.get("ob_debug_variant") is not None

        # ── Debug-only structure-candidate trace params ──
        structure_candidate_trace = request.args.get(
            "structure_candidate_trace", "false").strip().lower() in ("1", "true", "yes")
        _SC_VARIANTS = ["current", "retain_broken_upP",
                        "promote_bos_high_to_upP", "equal_high_pivot_relaxed"]
        _scv_raw = (request.args.get("structure_candidate_variant") or "all").strip()
        _scv_map = {x.lower(): x for x in _SC_VARIANTS}
        _scv = _scv_map.get(_scv_raw.lower())
        if _scv == "current":
            _sc_to_run = ["current"]
        elif _scv is not None:
            _sc_to_run = ["current", _scv]
        else:                                   # "all" or invalid → all
            _sc_to_run = list(_SC_VARIANTS)
        candidate_from = (request.args.get("candidate_from") or "").strip() or None
        candidate_to   = (request.args.get("candidate_to")   or "").strip() or None
        structure_candidate_global = request.args.get(
            "structure_candidate_global", "false").strip().lower() in ("1", "true", "yes")

        # ── Debug-only ob_logic_mode comparison (admin) ──
        _OB_LOGIC_MODES = ["legacy_baseline", "tv_parity_v2", "tv_parity_v3"]
        ob_logic_mode_debug = (request.args.get("ob_logic_mode") or "").strip() or None
        if ob_logic_mode_debug is not None and ob_logic_mode_debug not in _OB_LOGIC_MODES:
            return jsonify({
                "ok": False,
                "error": f"invalid ob_logic_mode '{ob_logic_mode_debug}'",
                "allowed": _OB_LOGIC_MODES,
            }), 200

        # ── Debug-only "as-of" snapshot slicing (admin-only) ──
        debug_as_of    = (request.args.get("debug_as_of") or "").strip() or None
        def _parse_as_of(s):
            if not s:
                return None
            for _fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S"):
                try:
                    return int(_dt.datetime.strptime(s, _fmt)
                               .replace(tzinfo=_dt.timezone.utc).timestamp() * 1000)
                except ValueError:
                    continue
            return None
        _as_of_ms = _parse_as_of(debug_as_of)

        try:
            kline_limit = min(max(int(request.args.get("kline_limit") or 300), 50), 10000)
        except (TypeError, ValueError):
            kline_limit = 300

        I_LEN, S_LEN = 5, 30  # screener defaults (iLen/sLen from parse_settings)

        # ── Candle fetch — one call; slice for comparison if requested ────────
        fetch_limit = max(kline_limit, 1500) if compare_limits else kline_limit
        candles = get_klines_exchange(symbol, timeframe, fetch_limit, market, exchange)
        if not candles:
            return jsonify({"ok": False, "error": "no candles returned"}), 200

        def _ts(ms):
            if not ms:
                return None
            try:
                return _dt.datetime.utcfromtimestamp(ms / 1000).strftime("%Y-%m-%d %H:%M UTC")
            except Exception:
                return str(ms)

        # ── Core analysis helper — runs on any candle slice ───────────────────
        def _analyse(cnd, mit_closed=False, eff_overlap=False, bear_eff_bottom=False,
                     anchor_mode="baseline", tie_mode="first", structure_candidate="current"):
            _o = [x["open"]   for x in cnd]
            _h = [x["high"]   for x in cnd]
            _l = [x["low"]    for x in cnd]
            _c = [x["close"]  for x in cnd]
            _v = [x["volume"] for x in cnd]
            _t = [x.get("time", x.get("openTime", 0)) for x in cnd]

            # Debug endpoint pins logic mode to legacy and drives every rule
            # explicitly, so variant outputs stay byte-identical regardless of
            # the production default.
            _normal_result = detect_obs(_o, _h, _l, _c, _v, I_LEN, S_LEN, max_ob=5,
                                        ob_logic_mode="legacy_baseline",
                                        mitigation_closed_only=mit_closed,
                                        overlap_effective_zone=eff_overlap,
                                        bearish_effective_bottom_overlap=bear_eff_bottom,
                                        anchor_mode=anchor_mode,
                                        extreme_tie_mode=tie_mode,
                                        structure_candidate=structure_candidate)
            if isinstance(_normal_result, tuple):
                _normal, _bos_trace = _normal_result
            else:
                _normal, _bos_trace = _normal_result, None
            _all, _ = detect_obs(_o, _h, _l, _c, _v, I_LEN, S_LEN, max_ob=None,
                                 ob_logic_mode="legacy_baseline",
                                 mitigation_closed_only=mit_closed,
                                 overlap_effective_zone=eff_overlap,
                                 bearish_effective_bottom_overlap=bear_eff_bottom,
                                 anchor_mode=anchor_mode,
                                 extreme_tie_mode=tie_mode,
                                 structure_candidate=structure_candidate)

            _bull_src = _copy.deepcopy([ob for ob in _all if ob["type"] == "bullish"])
            _bear_src = _copy.deepcopy([ob for ob in _all if ob["type"] == "bearish"])
            _bull_vis = _tv_visible_pool(_bull_src, overlap_effective_zone=eff_overlap,
                                         bearish_effective_bottom_overlap=bear_eff_bottom,
                                         ob_logic_mode="legacy_baseline")
            _bear_vis = _tv_visible_pool(_bear_src, overlap_effective_zone=eff_overlap,
                                         bearish_effective_bottom_overlap=bear_eff_bottom,
                                         ob_logic_mode="legacy_baseline")
            calculate_tv_ob_volume_share(_bull_vis, pool_name="bullish",
                                         source_pool_count=len(_bull_src))
            calculate_tv_ob_volume_share(_bear_vis, pool_name="bearish",
                                         source_pool_count=len(_bear_src))

            _bull_vtot = round(sum(ob.get("volume") or 0 for ob in _bull_vis), 2)
            _bear_vtot = round(sum(ob.get("volume") or 0 for ob in _bear_vis), 2)

            _price = _c[-1] if _c else 0

            def _dist(ob):
                if ob["type"] == "bullish":
                    return ((_price - ob["top"])    / max(_price, 1e-10) * 100) if _price > ob["top"]    else 0.0
                return     ((ob["bottom"] - _price) / max(_price, 1e-10) * 100) if _price < ob["bottom"] else 0.0

            _matched_tv_pct = None
            _matched_type   = None
            if _normal:
                _near   = min(_normal, key=lambda ob: abs(_dist(ob)))
                _tv_map = {(ob["type"], ob["bar"]): ob for ob in _bull_vis + _bear_vis}
                _tv_ref = _tv_map.get((_near["type"], _near["bar"]))
                _matched_tv_pct = _tv_ref.get("tvObVolumeSharePct") if _tv_ref else None
                _matched_type   = _near["type"]

            return {
                "normal": _normal, "all": _all,
                "bull_src": _bull_src, "bear_src": _bear_src,
                "bull_vis": _bull_vis, "bear_vis": _bear_vis,
                "bull_vtot": _bull_vtot, "bear_vtot": _bear_vtot,
                "times": _t, "price": _price,
                "matched_tv_pct": _matched_tv_pct, "matched_type": _matched_type,
                "bos_trace": _bos_trace,
            }

        # ── Main analysis with chosen kline_limit ─────────────────────────────
        # debug_as_of (admin-only): drop candles strictly after the snapshot
        # time first, then take the last kline_limit candles of what remains.
        _orig_newest_ms = (candles[-1].get("time") if candles else None)
        _orig_total     = len(candles)
        if _as_of_ms is not None:
            candles = [x for x in candles
                       if (x.get("time", x.get("openTime", 0)) or 0) <= _as_of_ms]
        _removed_after_as_of = _orig_total - len(candles)
        main_candles = candles[-kline_limit:] if len(candles) >= kline_limit else candles[:]
        a = _analyse(main_candles)

        times = a["times"]
        price = a["price"]

        def _ts_i(bar):
            return _ts(times[bar]) if 0 <= bar < len(times) else None

        candle_info = {
            "symbol":                symbol,
            "timeframe":             timeframe,
            "exchange":              exchange,
            "market":                market,
            "kline_limit":           kline_limit,
            "requestedKlineLimit":   kline_limit,
            "candlesFetched":        len(main_candles),
            "paginationUsed":        kline_limit > 1500 and exchange == "binance",
            "binancePerRequestLimit": 1500,
            "requested_limit":       kline_limit,
            "actual_count":          len(main_candles),
            "candles_count":         len(main_candles),
            "oldest_candle_time":    _ts(times[0])  if times else None,
            "newest_candle_time":    _ts(times[-1]) if times else None,
            "current_price":         price,
        }
        if debug_as_of is not None:
            candle_info["debug_as_of"]                      = debug_as_of
            candle_info["debug_as_of_parsed_ms"]            = _as_of_ms
            candle_info["original_newest_candle_time"]      = _ts(_orig_newest_ms)
            candle_info["sliced_newest_candle_time"]        = (_ts(main_candles[-1].get("time"))
                                                               if main_candles else None)
            candle_info["candles_after_as_of_removed_count"] = _removed_after_as_of

        detection_counts = {
            "normal_obs_count":      len(a["normal"]),
            "tv_all_obs_count":      len(a["all"]),
            "bullish_source_count":  len(a["bull_src"]),
            "bearish_source_count":  len(a["bear_src"]),
            "bullish_visible_count": len(a["bull_vis"]),
            "bearish_visible_count": len(a["bear_vis"]),
        }

        # ── Serialise helpers ─────────────────────────────────────────────────
        def _ob_base(ob):
            bar = ob.get("bar", 0)
            return {
                "type":           ob["type"],
                "bar":            bar,
                "time":           _ts_i(bar),
                "top":            round(ob["top"],    8),
                "bottom":         round(ob["bottom"], 8),
                "avg":            round(ob.get("avg", (ob["top"] + ob["bottom"]) / 2), 8),
                "volume":         ob.get("volume"),
                "strengthPct":    ob.get("strengthPct"),
                "strengthLabel":  ob.get("strengthLabel"),
                "formationRange": ob.get("formationRange"),
                "sourceBar":      ob.get("sourceBar"),
                "candleDir":      ob.get("candleDir"),
                # Phase 1A: OB touch metadata (backend-only)
                "touches":         ob.get("touches"),
                "isVirgin":        ob.get("isVirgin"),
                "currentlyInside": ob.get("currentlyInside"),
                "firstTouchBar":   ob.get("firstTouchBar"),
                "lastTouchBar":    ob.get("lastTouchBar"),
                "mitigationBar":   ob.get("mitigationBar"),
                "mitigated":       ob.get("mitigated"),
                "untouched":       ob.get("untouched"),
                "onceTouched":     ob.get("onceTouched"),
                "touchSeq":        ob.get("touchSeq"),
            }

        def _ob_visible(ob):
            d = _ob_base(ob)
            d.update({
                "tvObVolumeSharePct":      ob.get("tvObVolumeSharePct"),
                "tvObVolumeShareStatus":   ob.get("tvObVolumeShareStatus"),
                "tvObVisibleTotalVolume":  ob.get("tvObVisibleTotalVolume"),
                "tvObVisibleCount":        ob.get("tvObVisibleCount"),
                "tvObParitySeq":           ob.get("tvObParitySeq"),
                "tvObOverlapMode":         ob.get("tvObOverlapMode"),
                "tvObInputCount":          ob.get("tvObInputCount"),
                "tvObAfterOverlapCount":   ob.get("tvObAfterOverlapCount"),
                "tvObFinalShowLastCount":  ob.get("tvObFinalShowLastCount"),
            })
            return d

        bull_source_out  = [_ob_base(ob) for ob in a["bull_src"][-20:]]
        bear_source_out  = [_ob_base(ob) for ob in a["bear_src"][-20:]]
        bull_visible_out = [_ob_visible(ob) for ob in a["bull_vis"]]
        bear_visible_out = [_ob_visible(ob) for ob in a["bear_vis"]]

        # ── Hidden pools with reasons ─────────────────────────────────────────
        def _hidden_with_reasons(src_all, max_ob=5):
            hidden = []
            # Step 1: overlap filter — keep older, hide newer overlapping an accepted older OB
            accepted = []
            for ob in src_all:
                overlapping_with = None
                for acc in accepted:
                    if ob["top"] > acc["bottom"] and ob["bottom"] < acc["top"]:
                        overlapping_with = acc
                        break
                if overlapping_with is not None:
                    d = _ob_base(ob)
                    d["hidden_reason"]          = "overlap_previous"
                    d["hidden_volume"]          = ob.get("volume")
                    d["overlapped_with_volume"] = overlapping_with.get("volume")
                    d["overlap_mode"]           = "previous"
                    hidden.append(d)
                else:
                    accepted.append(ob)
            # Step 2: showLast — oldest beyond max_ob are hidden
            if len(accepted) > max_ob:
                for ob in accepted[:-max_ob]:
                    d = _ob_base(ob)
                    d["hidden_reason"]          = "beyond_show_last"
                    d["hidden_volume"]          = ob.get("volume")
                    d["overlapped_with_volume"] = None
                    d["overlap_mode"]           = "previous"
                    hidden.append(d)
            return hidden

        bull_hidden_out = _hidden_with_reasons(a["bull_src"])
        bear_hidden_out = _hidden_with_reasons(a["bear_src"])

        # ── Nearest OB detail ─────────────────────────────────────────────────
        matched = None
        if a["normal"]:
            def _dist(ob):
                if ob["type"] == "bullish":
                    return ((price - ob["top"])    / max(price, 1e-10) * 100) if price > ob["top"]    else 0.0
                return     ((ob["bottom"] - price) / max(price, 1e-10) * 100) if price < ob["bottom"] else 0.0

            nearest     = min(a["normal"], key=lambda ob: abs(_dist(ob)))
            tv_pool_map = {(ob["type"], ob["bar"]): ob for ob in a["bull_vis"] + a["bear_vis"]}
            tv_ref      = tv_pool_map.get((nearest["type"], nearest["bar"]))
            in_visible  = tv_ref is not None

            if not in_visible:
                src_for_dir = a["bull_src"] if nearest["type"] == "bullish" else a["bear_src"]
                hidden_for_dir = bull_hidden_out if nearest["type"] == "bullish" else bear_hidden_out
                _hidden_map    = {h["bar"]: h.get("hidden_reason", "unknown") for h in hidden_for_dir}
                nv_reason      = _hidden_map.get(nearest["bar"], "overlap_previous_or_unknown")
            else:
                nv_reason = None

            dir_src_count = len(a["bull_src"] if nearest["type"] == "bullish" else a["bear_src"])
            matched = {
                "type":                              nearest["type"],
                "bar":                               nearest["bar"],
                "time":                              _ts_i(nearest["bar"]),
                "top":                               round(nearest["top"],    8),
                "bottom":                            round(nearest["bottom"], 8),
                "volume":                            nearest.get("volume"),
                "obStrengthPct":                     nearest.get("strengthPct"),
                "tvObVolumeSharePct":                tv_ref.get("tvObVolumeSharePct")                if tv_ref else None,
                "tvObVolumeShareStatus":             tv_ref.get("tvObVolumeShareStatus")             if tv_ref else "ob_not_in_tv_visible_pool",
                "tvObVisibleTotalVolume":            tv_ref.get("tvObVisibleTotalVolume")            if tv_ref else None,
                "tvObVisibleCount":                  tv_ref.get("tvObVisibleCount")                  if tv_ref else None,
                "tvObSourcePoolCountBeforeShowLast": tv_ref.get("tvObSourcePoolCountBeforeShowLast") if tv_ref else dir_src_count,
                "tvObDirectionPoolCount":            tv_ref.get("tvObDirectionPoolCount")            if tv_ref else dir_src_count,
                "in_tv_visible_pool":                in_visible,
                "not_visible_reason":                nv_reason,
                # Phase 1A: touch metadata (backend-only)
                "touches":         nearest.get("touches"),
                "isVirgin":        nearest.get("isVirgin"),
                "currentlyInside": nearest.get("currentlyInside"),
                "firstTouchBar":   nearest.get("firstTouchBar"),
                "lastTouchBar":    nearest.get("lastTouchBar"),
                "mitigationBar":   nearest.get("mitigationBar"),
                "mitigated":       nearest.get("mitigated"),
                "untouched":       nearest.get("untouched"),
                "onceTouched":     nearest.get("onceTouched"),
                "touchSeq":        nearest.get("touchSeq"),
            }

        # ── Limit comparison (one fetch, multiple slices) ─────────────────────
        limit_comparison = []
        if compare_limits:
            for lim in [300, 500, 1000, 1500]:
                cnd_slice = candles[-lim:] if len(candles) >= lim else candles[:]
                try:
                    b = _analyse(cnd_slice)
                    bt = b["times"]
                    limit_comparison.append({
                        "limit":                        lim,
                        "candles_count":                len(cnd_slice),
                        "oldest_candle_time":           _ts(bt[0])  if bt else None,
                        "newest_candle_time":           _ts(bt[-1]) if bt else None,
                        "bullish_source_count":         len(b["bull_src"]),
                        "bearish_source_count":         len(b["bear_src"]),
                        "bullish_visible_count":        len(b["bull_vis"]),
                        "bearish_visible_count":        len(b["bear_vis"]),
                        "bullish_visible_total_volume": b["bull_vtot"],
                        "bearish_visible_total_volume": b["bear_vtot"],
                        "matched_nearest_type":         b["matched_type"],
                        "matched_nearest_tv_pct":       b["matched_tv_pct"],
                        "matched_bearish_tv_pct":       b["matched_tv_pct"] if b.get("matched_type") == "bearish" else None,
                    })
                except Exception as _le:
                    limit_comparison.append({"limit": lim, "error": str(_le)})

        # ── Debug-only variant diagnostics (additive — baseline top-level keys
        #    are computed from `a` and remain byte-identical to deployed) ──────
        variant_diagnostics = {}
        variant_summary     = {}
        for _vn in _variants_to_run:
            _mc, _eo, _beb = _VARIANT_FLAGS[_vn]
            _x = a if _vn == "baseline" else _analyse(main_candles,
                                                      mit_closed=_mc,
                                                      eff_overlap=_eo,
                                                      bear_eff_bottom=_beb)
            _bv = [_ob_visible(ob) for ob in _x["bull_vis"]]
            _rv = [_ob_visible(ob) for ob in _x["bear_vis"]]
            variant_diagnostics[_vn] = {
                "variant":        _vn,
                "notes":          _VARIANT_NOTES[_vn],
                "rules":          {"mitigation_closed_only": _mc,
                                   "overlap_effective_zone": _eo,
                                   "bearish_effective_bottom_overlap": _beb},
                "source_counts":  {"bullish": len(_x["bull_src"]),
                                   "bearish": len(_x["bear_src"])},
                "visible_counts": {"bullish": len(_x["bull_vis"]),
                                   "bearish": len(_x["bear_vis"])},
                "bullish_visible_total_volume": _x["bull_vtot"],
                "bearish_visible_total_volume": _x["bear_vtot"],
                "bullish_visible_pool":         _bv,
                "bearish_visible_pool":         _rv,
            }
            variant_summary[_vn] = {
                "bull_times":        [d["time"] for d in _bv],
                "bear_times":        [d["time"] for d in _rv],
                "bull_total_volume": _x["bull_vtot"],
                "bear_total_volume": _x["bear_vtot"],
                "bull_pct_list":     [d.get("tvObVolumeSharePct") for d in _bv],
                "bear_pct_list":     [d.get("tvObVolumeSharePct") for d in _rv],
            }

        # ── Debug-only OB trace (diagnostic; only built when trace_ob=true) ───
        ob_trace_detail = None
        trace_summary   = None
        if trace_ob:
            _to       = [x["open"]   for x in main_candles]
            _th       = [x["high"]   for x in main_candles]
            _tl       = [x["low"]    for x in main_candles]
            _tc_close = [x["close"]  for x in main_candles]
            _tv       = [x["volume"] for x in main_candles]
            _tn       = len(_tc_close)

            # BOS detection is overlap/mitigation-variant-independent →
            # trace once with baseline anchor + legacy flags.
            _trace_coll = {"events": [], "mitigations": []}
            detect_obs(_to, _th, _tl, _tc_close, _tv, I_LEN, S_LEN, max_ob=None,
                       trace=_trace_coll, ob_logic_mode="legacy_baseline",
                       anchor_mode="baseline")

            # When an anchor variant is requested, also trace the
            # latest_opposite_pivot anchor for side-by-side comparison.
            _lop_by_key = {}
            if _anchors_to_run is not None:
                _trace_coll_lop = {"events": [], "mitigations": []}
                detect_obs(_to, _th, _tl, _tc_close, _tv, I_LEN, S_LEN, max_ob=None,
                           trace=_trace_coll_lop, ob_logic_mode="legacy_baseline",
                           anchor_mode="latest_opposite_pivot")
                _lop_by_key = {(e["side"], e["bos_bar"]): e
                               for e in _trace_coll_lop["events"]}

            def _parse_trace_dt(s):
                if not s:
                    return None
                for _fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S"):
                    try:
                        return int(_dt.datetime.strptime(s, _fmt)
                                   .replace(tzinfo=_dt.timezone.utc).timestamp() * 1000)
                    except ValueError:
                        continue
                return None

            _from_ms = _parse_trace_dt(trace_from)
            _to_ms   = _parse_trace_dt(trace_to)

            # Baseline pool membership (trace run uses baseline flags, so its
            # survivors == baseline `a` pools).
            _src_bars = {(ob["type"], ob["bar"]) for ob in a["all"]}
            _vis_bull = {ob["bar"]: idx for idx, ob in enumerate(a["bull_vis"])}
            _vis_bear = {ob["bar"]: idx for idx, ob in enumerate(a["bear_vis"])}
            _mit_by_ob = {}
            for _m in _trace_coll["mitigations"]:
                _mit_by_ob.setdefault((_m["ob_type"], _m["ob_bar"]), _m)

            def _wt(bar):
                return _ts(times[bar]) if (bar is not None and 0 <= bar < _tn) else None

            def _anchor_search_block(ev):
                # Compact per-event search summary, built from one trace event
                # (baseline or latest_opposite_pivot run).
                if ev is None:
                    return None
                _b = ev["side"] == "bullish"
                _sel = ev.get("min_idx" if _b else "max_idx")
                _se2 = ev["search_end"]
                if "ob_top" in ev:
                    _disp = ({"bottom": ev["ob_bottom"], "top": ev["ob_top"]} if _b
                             else {"bottom": ev["ob_avg"], "top": ev["ob_top"]})
                else:
                    _disp = None
                return {
                    "search_start_bar":      ev["search_start"],
                    "search_start_time_utc": _wt(ev["search_start"]),
                    "search_end_bar":        _se2,
                    "search_end_time_utc":   _wt(min(_se2 - 1, _tn - 1)),
                    "selected_bar":          _sel,
                    "selected_time_utc":     _wt(_sel),
                    "selected_low_or_high":  (ev.get("ob_bottom") if _b
                                              else ev.get("ob_top")),
                    "sourceBar":             ev.get("ob_source"),
                    "source_time_utc":       _wt(ev.get("ob_source")),
                    "volume":                ev.get("volume"),
                    "displayed_zone":        _disp,
                }

            _MAX_WIN = 1500  # cap per-event window candle list

            detail = []
            for _ev in _trace_coll["events"]:
                if trace_side and _ev["side"] != trace_side:
                    continue
                _ss = _ev["search_start"]
                _se = _ev["search_end"]            # exclusive
                _last_bar = min(_se - 1, _tn - 1)
                _win_start_ms = times[_ss]       if 0 <= _ss < _tn else None
                _win_end_ms   = times[_last_bar] if 0 <= _last_bar < _tn else None
                # Window-overlap filter against [trace_from, trace_to]
                if _from_ms is not None and _win_end_ms is not None and _win_end_ms < _from_ms:
                    continue
                if _to_ms is not None and _win_start_ms is not None and _win_start_ms > _to_ms:
                    continue

                _is_bull = _ev["side"] == "bullish"
                _sel_bar = _ev.get("min_idx" if _is_bull else "max_idx")

                _win_cands = []
                _truncated = (_last_bar - _ss + 1) > _MAX_WIN
                for _b in range(_ss, min(_se, _ss + _MAX_WIN)):
                    if not (0 <= _b < _tn):
                        continue
                    _metric = _tl[_b] if _is_bull else _th[_b]
                    _sel    = (_b == _sel_bar)
                    _dir    = 1 if _tc_close[_b] > _to[_b] else -1
                    if _sel:
                        _reason = ("selected: " +
                                   ("lowest low" if _is_bull else "highest high") +
                                   " in search window")
                    else:
                        _reason = ("rejected: " +
                                   ("low" if _is_bull else "high") +
                                   f" {_metric:.8f} is not the window " +
                                   ("min" if _is_bull else "max"))
                    _win_cands.append({
                        "bar": _b, "time_utc": _wt(_b),
                        "open": _to[_b], "high": _th[_b], "low": _tl[_b],
                        "close": _tc_close[_b], "volume": _tv[_b],
                        "candle_dir": _dir,
                        "is_bullish": _dir == 1, "is_bearish": _dir == -1,
                        "is_candidate_for_ob": True,
                        "selection_metric_used": "lowest_low" if _is_bull else "highest_high",
                        "selection_metric_value": _metric,
                        "selected_by_python": _sel,
                        "reason_selected_or_rejected": _reason,
                    })

                _selected_ob = None
                if "ob_top" in _ev:
                    _src_bar = _ev.get("ob_source")
                    if _is_bull:
                        _disp_bottom, _disp_top = _ev["ob_bottom"], _ev["ob_top"]
                    else:
                        # TV displays the bearish lower boundary at avg
                        _disp_bottom, _disp_top = _ev["ob_avg"], _ev["ob_top"]
                    _selected_ob = {
                        "selected_bar":      _sel_bar,
                        "selected_time_utc": _wt(_sel_bar),
                        "sourceBar":         _src_bar,
                        "source_time_utc":   _wt(_src_bar),
                        "raw_bottom":        _ev["ob_bottom"],
                        "raw_top":           _ev["ob_top"],
                        "avg":               _ev["ob_avg"],
                        "displayed_bottom":  _disp_bottom,
                        "displayed_top":     _disp_top,
                        "volume":            _ev.get("volume"),
                        "precise_adjustment_notes": (
                            "precise adjustment applied (zone edge collapsed to avg)"
                            if _ev.get("precise_applied") else "no precise adjustment"),
                    }

                _co       = _ev.get("creation_overlap") or {}
                _mit      = _mit_by_ob.get((_ev["side"], _sel_bar)) if _sel_bar is not None else None
                _in_src   = (_ev["side"], _sel_bar) in _src_bars
                _vis_map  = _vis_bull if _is_bull else _vis_bear
                _vis_rank = _vis_map.get(_sel_bar)
                _in_vis   = _vis_rank is not None
                if not _ev.get("created"):
                    _nv_reason = "ob_not_created (" + _ev.get(
                        "not_created_reason", "search window empty") + ")"
                elif _co.get("deleted_by_creation_overlap"):
                    _nv_reason = "deleted_by_creation_overlap"
                elif _mit is not None:
                    _nv_reason = f"mitigated_at_bar_{_mit['mitigated_at_bar']}"
                elif not _in_src:
                    _nv_reason = "not_in_source_pool (removed during bar loop)"
                elif not _in_vis:
                    _nv_reason = "hidden_in_visible_pool (overlap_previous or beyond_show_last)"
                else:
                    _nv_reason = None
                _post = {
                    "creation_overlap_checked":    _co.get("checked", False),
                    "overlap_with_previous":       bool(_co.get("deleted_by_creation_overlap")),
                    "overlap_previous_ob_time":    _wt(_co.get("prev_ob_bar")),
                    "overlap_rule_used":           _co.get("rule"),
                    "deleted_by_creation_overlap": bool(_co.get("deleted_by_creation_overlap")),
                    "mitigation_checked":          True,
                    "mitigated":                   _mit is not None,
                    "mitigation_bar":              _mit["mitigated_at_bar"] if _mit else None,
                    "mitigation_time_utc":         _wt(_mit["mitigated_at_bar"]) if _mit else None,
                    "added_to_source_pool":        _in_src,
                    "included_in_visible_pool":    _in_vis,
                    "not_visible_reason":          _nv_reason,
                    "showLast_rank":               _vis_rank,
                }

                _pivot_bar   = _ev["pivot_bar"]
                _pivot_price = ((_th[_pivot_bar] if _is_bull else _tl[_pivot_bar])
                                if 0 <= _pivot_bar < _tn else None)
                _bos_event = {
                    "side":                  _ev["side"],
                    "bos_bar":               _ev["bos_bar"],
                    "bos_time_utc":          _wt(_ev["bos_bar"]),
                    "close_prev":            _ev["close_prev"],
                    "close_curr":            _ev["close_curr"],
                    "broken_level_value":    _ev["broken_level"],
                    "pivot_bar":             _pivot_bar,
                    "pivot_time_utc":        _wt(_pivot_bar),
                    "pivot_price":           _pivot_price,
                    "internal_trend_before": None,
                    "internal_trend_after":  None,
                    "choch":                 None,
                    "chochplus":             None,
                    "internal_trend_note":   ("Python detect_obs has no separate "
                                              "CHoCH / internal-trend state — it uses "
                                              "pivot-cross BOS detection only."),
                    "search_start_bar":            _ss,
                    "search_start_time_utc":       _wt(_ss),
                    "search_end_bar":              _se,
                    "search_end_exclusive":        True,
                    "search_window_last_bar":      _last_bar,
                    "search_window_last_time_utc": _wt(_last_bar),
                    "search_rule_description": (
                        "search_start = max(0, pivot_bar + 1); search_end = bos_bar + 1 "
                        "(exclusive). Window = bars [pivot_bar+1 .. bos_bar]. Extreme = " +
                        ("lowest low" if _is_bull else "highest high") +
                        "; OB source candle = extreme_bar - 1 (Pine +1 offset)."),
                }

                _dentry = {
                    "bos_event":               _bos_event,
                    "search_window_candles":   _win_cands,
                    "search_window_truncated": _truncated,
                    "selected_ob":             _selected_ob,
                    "post_creation_decisions": _post,
                }

                # Side-by-side anchor comparison (only when ob_anchor_variant
                # is passed) — baseline vs latest_opposite_pivot search window.
                if _anchors_to_run is not None:
                    _base_blk = _anchor_search_block(_ev)
                    _lop_ev   = _lop_by_key.get((_ev["side"], _ev["bos_bar"]))
                    _lop_blk  = _anchor_search_block(_lop_ev)
                    if _lop_blk is not None:
                        _ap = _lop_ev.get("anchor_pivot_bar")
                        _lop_blk["opposite_pivot_bar"]      = _ap
                        _lop_blk["opposite_pivot_time_utc"] = _wt(_ap)
                        _lop_blk["opposite_pivot_price"]    = (
                            (_tl[_ap] if _is_bull else _th[_ap])
                            if (_ap is not None and 0 <= _ap < _tn) else None)
                        _lop_blk["opposite_pivot_confirmed_at_bar"] = \
                            _lop_ev.get("anchor_confirmed_at_bar")
                        _lop_blk["anchor_fallback_to_baseline"] = bool(
                            _lop_ev.get("anchor_fallback"))
                        _lop_blk["did_result_change"] = bool(
                            _base_blk is not None and
                            _base_blk["selected_bar"] != _lop_blk["selected_bar"])
                        _lop_blk["expected_match_hint"] = (
                            "For LINKUSDT 15m old trace, latest_opposite_pivot "
                            "should ideally select 2026-05-04 23:30 UTC")
                    _dentry["baseline_search"] = _base_blk
                    _dentry["latest_opposite_pivot_search"] = _lop_blk

                detail.append(_dentry)

            ob_trace_detail = {
                "trace_run_variant": "baseline",
                "trace_run_note": ("BOS detection, search window and extreme "
                                   "selection are variant-independent; mitigation "
                                   "shown here is baseline (runs through the last "
                                   "candle)."),
                "trace_side":           trace_side or "both",
                "trace_from":           trace_from,
                "trace_to":             trace_to,
                "trace_from_parsed_ms": _from_ms,
                "trace_to_parsed_ms":   _to_ms,
                "events_total":         len(_trace_coll["events"]),
                "events_in_range":      len(detail),
                "events":               detail,
            }

            _sel_obs = []
            _union   = {}
            for _d in detail:
                _so = _d["selected_ob"]
                if _so and _so["selected_bar"] is not None:
                    _pcd = _d["post_creation_decisions"]
                    _sel_obs.append({
                        "time":               _so["selected_time_utc"],
                        "bar":                _so["selected_bar"],
                        "sourceBar":          _so["sourceBar"],
                        "source_time":        _so["source_time_utc"],
                        "raw_top":            _so["raw_top"],
                        "raw_bottom":         _so["raw_bottom"],
                        "volume":             _so["volume"],
                        "in_source_pool":     _pcd["added_to_source_pool"],
                        "in_visible_pool":    _pcd["included_in_visible_pool"],
                        "not_visible_reason": _pcd["not_visible_reason"],
                        "bos_bar":            _d["bos_event"]["bos_bar"],
                        "bos_time":           _d["bos_event"]["bos_time_utc"],
                    })
                for _wc in _d["search_window_candles"]:
                    _u = _union.setdefault(_wc["bar"], {
                        "bar": _wc["bar"], "time": _wc["time_utc"],
                        "low": _wc["low"], "high": _wc["high"],
                        "selected_by_python": False,
                        "in_search_window_of_bos_bars": [],
                    })
                    if _wc["selected_by_python"]:
                        _u["selected_by_python"] = True
                    _u["in_search_window_of_bos_bars"].append(_d["bos_event"]["bos_bar"])
            _union_list = [_union[k] for k in sorted(_union)]

            trace_summary = {
                "side":                trace_side or "both",
                "trace_from":          trace_from,
                "trace_to":            trace_to,
                "bos_events_in_range": len(detail),
                "python_selected_obs": _sel_obs,
                "search_window_union": _union_list,
                "notes": (
                    f"{len(detail)} {trace_side or 'bull+bear'} BOS event(s) have a "
                    "search window overlapping the trace range. Every candle that "
                    "appeared in any of those search windows is listed in "
                    "search_window_union. A candle is the Python-selected OB anchor "
                    "ONLY if it is the window extreme (lowest low for bullish / "
                    "highest high for bearish) — see selected_by_python. If a candle "
                    "is absent from search_window_union, Python's OB search window "
                    "never covered it."),
            }

        # ── Debug-only anchor variant diagnostics (only when ob_anchor_variant
        #    is passed). Each anchor variant runs with baseline overlap /
        #    mitigation flags so the search-anchor effect is isolated. ─────────
        anchor_variant_diagnostics = None
        if _anchors_to_run is not None:
            anchor_variant_diagnostics = {}
            for _am in _anchors_to_run:
                _ax  = a if _am == "baseline" else _analyse(main_candles,
                                                            anchor_mode=_am)
                _abv = [_ob_visible(ob) for ob in _ax["bull_vis"]]
                _arv = [_ob_visible(ob) for ob in _ax["bear_vis"]]
                anchor_variant_diagnostics[_am] = {
                    "variant":              _am,
                    "bullish_source_pool":  [_ob_base(ob) for ob in _ax["bull_src"][-20:]],
                    "bearish_source_pool":  [_ob_base(ob) for ob in _ax["bear_src"][-20:]],
                    "bullish_visible_pool": _abv,
                    "bearish_visible_pool": _arv,
                    "bullish_visible_total_volume": _ax["bull_vtot"],
                    "bearish_visible_total_volume": _ax["bear_vtot"],
                    "source_counts":  {"bullish": len(_ax["bull_src"]),
                                       "bearish": len(_ax["bear_src"])},
                    "visible_counts": {"bullish": len(_ax["bull_vis"]),
                                       "bearish": len(_ax["bear_vis"])},
                    "variant_summary": {
                        "bull_times":        [d["time"] for d in _abv],
                        "bear_times":        [d["time"] for d in _arv],
                        "bull_pct_list":     [d.get("tvObVolumeSharePct") for d in _abv],
                        "bear_pct_list":     [d.get("tvObVolumeSharePct") for d in _arv],
                        "bull_total_volume": _ax["bull_vtot"],
                        "bear_total_volume": _ax["bear_vtot"],
                    },
                }

        # ── Debug-only extreme-tie diagnostics + tv_parity_candidate_v2 ───────
        #    (only when ob_extreme_tie_mode is passed → endpoint byte-identical
        #     otherwise). first-tie = baseline; last-tie = latest equal extreme.
        extreme_tie_diagnostics = None
        tv_parity_candidate_v2  = None
        if ob_extreme_tie_mode is not None:
            _tie_modes_to_run = (["first"] if ob_extreme_tie_mode == "first"
                                 else ["first", "last"])

            _eo2 = [x["open"]   for x in main_candles]
            _eh2 = [x["high"]   for x in main_candles]
            _el2 = [x["low"]    for x in main_candles]
            _ec2 = [x["close"]  for x in main_candles]
            _ev2 = [x["volume"] for x in main_candles]
            _en2 = len(_ec2)

            def _etw(bar):
                return _ts(times[bar]) if (bar is not None and 0 <= bar < _en2) else None

            def _eparse(s):
                if not s:
                    return None
                for _fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S"):
                    try:
                        return int(_dt.datetime.strptime(s, _fmt)
                                   .replace(tzinfo=_dt.timezone.utc).timestamp() * 1000)
                    except ValueError:
                        continue
                return None
            _efrom = _eparse(trace_from)
            _eto_  = _eparse(trace_to)

            # Trace first-tie and last-tie (baseline anchor, legacy flags).
            _tc_first = {"events": [], "mitigations": []}
            detect_obs(_eo2, _eh2, _el2, _ec2, _ev2, I_LEN, S_LEN, max_ob=None,
                       trace=_tc_first, ob_logic_mode="legacy_baseline",
                       anchor_mode="baseline", extreme_tie_mode="first")
            _tc_last = {"events": [], "mitigations": []}
            detect_obs(_eo2, _eh2, _el2, _ec2, _ev2, I_LEN, S_LEN, max_ob=None,
                       trace=_tc_last, ob_logic_mode="legacy_baseline",
                       anchor_mode="baseline", extreme_tie_mode="last")
            _first_by_key = {(e["side"], e["bos_bar"]): e for e in _tc_first["events"]}
            _last_by_key  = {(e["side"], e["bos_bar"]): e for e in _tc_last["events"]}

            def _tie_in_range(ev):
                if trace_side and ev["side"] != trace_side:
                    return False
                _ss = ev["search_start"]; _se = ev["search_end"]
                _ws = times[_ss] if 0 <= _ss < _en2 else None
                _wl = min(_se - 1, _en2 - 1)
                _we = times[_wl] if 0 <= _wl < _en2 else None
                if _efrom is not None and _we is not None and _we < _efrom:
                    return False
                if _eto_ is not None and _ws is not None and _ws > _eto_:
                    return False
                return True

            def _tie_sel(ev):
                if ev is None:
                    return None
                _b = ev["side"] == "bullish"
                _sel = ev.get("min_idx" if _b else "max_idx")
                _disp = None
                if "ob_top" in ev:
                    _disp = ({"bottom": ev["ob_bottom"], "top": ev["ob_top"]} if _b
                             else {"bottom": ev["ob_avg"], "top": ev["ob_top"]})
                return {
                    "selected_bar":         _sel,
                    "selected_time_utc":    _etw(_sel),
                    "selected_low_or_high": (ev.get("ob_bottom") if _b else ev.get("ob_top")),
                    "sourceBar":            ev.get("ob_source"),
                    "source_time_utc":      _etw(ev.get("ob_source")),
                    "source_volume":        ev.get("volume"),
                    "displayed_zone":       _disp,
                }

            # extreme_tie_diagnostics — per tie mode, list of per-BOS-event records
            extreme_tie_diagnostics = {}
            for _tm in _tie_modes_to_run:
                _src_events = (_tc_first if _tm == "first" else _tc_last)["events"]
                _recs = []
                for ev in _src_events:
                    if not _tie_in_range(ev):
                        continue
                    _sel  = _tie_sel(ev)
                    _fsel = _tie_sel(_first_by_key.get((ev["side"], ev["bos_bar"])))
                    _changed = bool(_tm == "last" and _sel and _fsel and
                                    _sel["selected_bar"] != _fsel["selected_bar"])
                    if _tm == "first":
                        _reason = ("first equal extreme wins (strict < / >) — current "
                                   "baseline behavior")
                    elif _changed:
                        _reason = (f"last equal extreme wins (<= / >=): a later bar tied "
                                   f"the extreme; selection moved from bar "
                                   f"{_fsel['selected_bar']} to bar {_sel['selected_bar']}")
                    else:
                        _reason = ("last equal extreme rule applied; no later tie in the "
                                   "search window — selection unchanged")
                    _rec = {
                        "tie_mode":     _tm,
                        "side":         ev["side"],
                        "bos_bar":      ev["bos_bar"],
                        "bos_time_utc": _etw(ev["bos_bar"]),
                        "did_result_change_from_first": _changed,
                        "reason":       _reason,
                    }
                    if _sel:
                        _rec.update(_sel)
                    _recs.append(_rec)
                extreme_tie_diagnostics[_tm] = _recs

            # Inject side-by-side tie selection into ob_trace_detail events.
            if ob_trace_detail is not None:
                for _d in ob_trace_detail["events"]:
                    _bb = _d["bos_event"]["bos_bar"]
                    _sd = _d["bos_event"]["side"]
                    _fe = _first_by_key.get((_sd, _bb))
                    _le = _last_by_key.get((_sd, _bb))
                    _d["first_tie_selection"] = _tie_sel(_fe)
                    _d["last_tie_selection"]  = _tie_sel(_le)
                    _eb = []
                    if _fe is not None:
                        _ss = _fe["search_start"]; _se = _fe["search_end"]
                        _isb = _sd == "bullish"
                        if _se > _ss:
                            _vals = (_el2 if _isb else _eh2)[_ss:_se]
                            _ext  = min(_vals) if _isb else max(_vals)
                            _fsb  = _fe.get("min_idx" if _isb else "max_idx")
                            _lsb  = _le.get("min_idx" if _isb else "max_idx") if _le else None
                            for _b in range(_ss, _se):
                                _val = _el2[_b] if _isb else _eh2[_b]
                                if _val == _ext:
                                    _eb.append({
                                        "bar": _b, "time_utc": _etw(_b),
                                        "low_or_high": _val, "volume": _ev2[_b],
                                        "is_first_tie_pick": _b == _fsb,
                                        "is_last_tie_pick":  _b == _lsb,
                                    })
                    _d["equal_extreme_bars"] = _eb

            # tv_parity_candidate_v2 — combined DEBUG candidate (NOT production)
            _cand = _analyse(main_candles, mit_closed=True, bear_eff_bottom=True,
                             anchor_mode="latest_opposite_pivot", tie_mode="last")
            _cbv = [_ob_visible(ob) for ob in _cand["bull_vis"]]
            _crv = [_ob_visible(ob) for ob in _cand["bear_vis"]]
            tv_parity_candidate_v2 = {
                "variant": "tv_parity_candidate_v2",
                "note": ("Combined DEBUG candidate — NOT production. "
                         "mitigation_closed_only + bearish_effective_bottom_overlap "
                         "+ latest_opposite_pivot anchor + last extreme-tie."),
                "rules": {
                    "mitigation_closed_only":           True,
                    "bearish_effective_bottom_overlap": True,
                    "ob_anchor_variant":                "latest_opposite_pivot",
                    "ob_extreme_tie_mode":              "last",
                },
                "bullish_source_pool":  [_ob_base(ob) for ob in _cand["bull_src"][-20:]],
                "bearish_source_pool":  [_ob_base(ob) for ob in _cand["bear_src"][-20:]],
                "bullish_visible_pool": _cbv,
                "bearish_visible_pool": _crv,
                "bullish_visible_total_volume": _cand["bull_vtot"],
                "bearish_visible_total_volume": _cand["bear_vtot"],
                "source_counts":  {"bullish": len(_cand["bull_src"]),
                                   "bearish": len(_cand["bear_src"])},
                "visible_counts": {"bullish": len(_cand["bull_vis"]),
                                   "bearish": len(_cand["bear_vis"])},
                "variant_summary": {
                    "bull_times":        [d["time"] for d in _cbv],
                    "bear_times":        [d["time"] for d in _crv],
                    "bull_pct_list":     [d.get("tvObVolumeSharePct") for d in _cbv],
                    "bear_pct_list":     [d.get("tvObVolumeSharePct") for d in _crv],
                    "bull_total_volume": _cand["bull_vtot"],
                    "bear_total_volume": _cand["bear_vtot"],
                },
            }

        # ── Debug-only structure / BOS-method trace ──────────────────────────
        #    (only when structure_trace=true → endpoint byte-identical otherwise)
        structure_trace_detail = None
        if structure_trace:
            from main import detect_pivots

            _so = [x["open"]   for x in main_candles]
            _sh = [x["high"]   for x in main_candles]
            _sl = [x["low"]    for x in main_candles]
            _sc = [x["close"]  for x in main_candles]
            _sv = [x["volume"] for x in main_candles]
            _sn = len(_sc)
            _is_bull = target_side == "bullish"

            def _stw(bar):
                return _ts(times[bar]) if (bar is not None and 0 <= bar < _sn) else None

            def _sparse(s):
                if not s:
                    return None
                for _fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S"):
                    try:
                        return int(_dt.datetime.strptime(s, _fmt)
                                   .replace(tzinfo=_dt.timezone.utc).timestamp() * 1000)
                    except ValueError:
                        continue
                return None

            def _bar_at(ms):
                if ms is None or not times:
                    return None
                _best = None; _bd = None
                for _idx, _t in enumerate(times):
                    _d = abs(_t - ms)
                    if _bd is None or _d < _bd:
                        _bd = _d; _best = _idx
                return _best

            _tgt_ms  = _sparse(target_ob_time)
            _sf_ms   = _sparse(structure_from)
            _st_ms   = _sparse(structure_to)
            _tgt_bar = _bar_at(_tgt_ms)

            # internal (iLen) + swing (sLen) pivots
            _phi, _pli = detect_pivots(_sh, _sl, I_LEN, I_LEN)
            _phs, _pls = detect_pivots(_sh, _sl, S_LEN, S_LEN)

            # running latest confirmed swing levels per bar (right=S_LEN)
            _lat_sh = [None] * _sn; _lat_sl = [None] * _sn
            _csh = None; _csl = None
            for _i in range(_sn):
                _cb = _i - S_LEN
                if _cb >= 0 and _phs[_cb]:
                    _csh = _sh[_cb]
                if _cb >= 0 and _pls[_cb]:
                    _csl = _sl[_cb]
                _lat_sh[_i] = _csh; _lat_sl[_i] = _csl

            # structure trace run — legacy logic mode (raw structure
            # observation); opt into per-bar recording via the "bars" key.
            _struct_coll = {"events": [], "mitigations": [], "bars": []}
            detect_obs(_so, _sh, _sl, _sc, _sv, I_LEN, S_LEN, max_ob=None,
                       trace=_struct_coll, ob_logic_mode="legacy_baseline")
            _bars_by_idx = {b["bar"]: b for b in _struct_coll["bars"]}
            _bos_by_bar  = {}
            for _e in _struct_coll["events"]:
                _bos_by_bar.setdefault((_e["side"], _e["bos_bar"]), _e)

            _PYCOND = ("bullish: upP and len(dnL) > 1 and c[i] > upP[0] and "
                       "prev_upP_first is not None and c[i-1] <= prev_upP_first"
                       if _is_bull else
                       "bearish: dnP and len(upL) > 1 and c[i] < dnP[0] and "
                       "prev_dnP_first is not None and c[i-1] >= prev_dnP_first")

            def _bos_method_rec(b):
                bi = _bars_by_idx.get(b, {})
                cp = _sc[b - 1] if b > 0 else None
                cc = _sc[b]
                hp = _sh[b - 1] if b > 0 else None
                lp = _sl[b - 1] if b > 0 else None
                py_trig = (target_side, b) in _bos_by_bar
                if _is_bull:
                    stored  = bi.get("upP_first")
                    pstored = bi.get("prev_upP_first")
                    mb_cross = (stored is not None and cp is not None
                                and cp <= stored and cc > stored)
                    mb_above = (stored is not None and cc > stored)
                    wick     = (stored is not None and _sh[b] > stored)
                    ma       = (stored is not None and hp is not None
                                and hp > stored and cp is not None and cp <= stored
                                and cc > hp)
                    close_prev_wick = (hp is not None and cc > hp)
                else:
                    stored  = bi.get("dnP_first")
                    pstored = bi.get("prev_dnP_first")
                    mb_cross = (stored is not None and cp is not None
                                and cp >= stored and cc < stored)
                    mb_above = (stored is not None and cc < stored)
                    wick     = (stored is not None and _sl[b] < stored)
                    ma       = (stored is not None and lp is not None
                                and lp < stored and cp is not None and cp >= stored
                                and cc < lp)
                    close_prev_wick = (lp is not None and cc < lp)
                wick_only = bool(wick and not mb_above)
                _dnl = bi.get("dnL_len") if _is_bull else bi.get("upL_len")
                if py_trig:
                    reason = None
                elif stored is None:
                    reason = ("no internal pivot level active ("
                              + ("upP" if _is_bull else "dnP") + " empty)")
                elif (_dnl or 0) <= 1:
                    reason = ("len(" + ("dnL" if _is_bull else "upL")
                              + ") <= 1 — need >1 prior opposite internal pivots")
                elif _is_bull and (cc is None or cc <= stored):
                    reason = f"close_curr ({cc}) did not exceed stored high level ({stored})"
                elif (not _is_bull) and (cc is None or cc >= stored):
                    reason = f"close_curr ({cc}) did not break below stored low level ({stored})"
                elif pstored is None:
                    reason = ("prev_" + ("upP" if _is_bull else "dnP")
                              + "_first is None (list empty on previous bar)")
                elif _is_bull and cp is not None and cp > pstored:
                    reason = f"no crossover — previous close ({cp}) already above stored level ({pstored})"
                elif (not _is_bull) and cp is not None and cp < pstored:
                    reason = f"no crossunder — previous close ({cp}) already below stored level ({pstored})"
                else:
                    reason = "python condition components met (see python_current_triggered)"
                return {
                    "bar": b, "time_utc": _stw(b),
                    "open": _so[b], "high": _sh[b], "low": _sl[b], "close": _sc[b],
                    "close_prev": cp, "close_curr": cc,
                    "stored_level": stored,
                    "method_b_close_cross_stored_level":  bool(mb_cross),
                    "method_b_close_above_stored_level":  bool(mb_above),
                    "wick_break_stored_level":            bool(wick),
                    "method_a_wick_then_next_close":      bool(ma),
                    "close_above_previous_wick":          bool(close_prev_wick),
                    "python_current_triggered":           bool(py_trig),
                    "method_a_triggered":                 bool(ma),
                    "method_b_triggered":                 bool(mb_cross),
                    "wick_only_triggered":                wick_only,
                    "reason_python_did_not_trigger":      reason,
                }

            # 1. target_candle
            target_candle = None
            if _tgt_bar is not None:
                _pb = _tgt_bar - 1
                target_candle = {
                    "bar": _tgt_bar, "time_utc": _stw(_tgt_bar),
                    "open": _so[_tgt_bar], "high": _sh[_tgt_bar],
                    "low": _sl[_tgt_bar], "close": _sc[_tgt_bar],
                    "volume": _sv[_tgt_bar],
                    "previous_bar": _pb if _pb >= 0 else None,
                    "previous_bar_time_utc": _stw(_pb),
                    "previous_bar_open":   _so[_pb] if _pb >= 0 else None,
                    "previous_bar_high":   _sh[_pb] if _pb >= 0 else None,
                    "previous_bar_low":    _sl[_pb] if _pb >= 0 else None,
                    "previous_bar_close":  _sc[_pb] if _pb >= 0 else None,
                    "previous_bar_volume": _sv[_pb] if _pb >= 0 else None,
                    "expected_source_volume_if_selected": _sv[_pb] if _pb >= 0 else None,
                    "expected_tv_zone_low_hint":  0.1594,
                    "expected_tv_zone_high_hint": 0.1602,
                    "expected_tv_volume_hint":    200003,
                }

            # 2. python_current_bos_method
            _tbi = _bars_by_idx.get(_tgt_bar, {}) if _tgt_bar is not None else {}
            python_current_bos_method = {
                "side": target_side,
                "condition_expression": _PYCOND,
                "close_prev": _sc[_tgt_bar - 1] if (_tgt_bar is not None and _tgt_bar > 0) else None,
                "close_curr": _sc[_tgt_bar] if _tgt_bar is not None else None,
                "active_structure_level_used": _tbi.get("upP_first" if _is_bull else "dnP_first"),
                "uses_close_crossover_of_stored_level": True,
                "requires_next_candle_close_beyond_prior_wick": False,
                "uses_wick_break": False,
                "uses_internal_or_swing": f"internal (detect_pivots with iLen={I_LEN} both sides; sLen swing pivots are NOT used for OB BOS)",
                "method_classification": "Method B (close crossover of the stored internal pivot level)",
                "function_file": "detect_obs() in main.py — bullish: 'c[i] > upP[0]' crossover; bearish: 'c[i] < dnP[0]' crossunder",
            }

            # 3. pivot_status
            def _near_piv(arr, kind, R, around, before, k=3):
                out = []
                rng = range(around - 1, -1, -1) if before else range(around + 1, _sn)
                for b in rng:
                    if arr[b]:
                        out.append({
                            "bar": b, "time_utc": _stw(b),
                            "price": _sh[b] if kind == "high" else _sl[b],
                            "confirmed_at_bar": b + R,
                            "confirmed_at_time_utc": _stw(b + R),
                        })
                        if len(out) >= k:
                            break
                return out

            pivot_status = None
            if _tgt_bar is not None:
                _is_pl = bool(_pli[_tgt_bar]); _is_ph = bool(_phi[_tgt_bar])
                pivot_status = {
                    "target_bar": _tgt_bar, "target_time_utc": _stw(_tgt_bar),
                    "is_target_confirmed_pivot_low":  _is_pl,
                    "is_target_confirmed_pivot_high": _is_ph,
                    "is_target_confirmed_swing_low":  bool(_pls[_tgt_bar]),
                    "is_target_confirmed_swing_high": bool(_phs[_tgt_bar]),
                    "pivot_confirmed_at_bar":     (_tgt_bar + I_LEN) if (_is_pl or _is_ph) else None,
                    "pivot_confirmed_at_time_utc": _stw(_tgt_bar + I_LEN) if (_is_pl or _is_ph) else None,
                    "pivot_left_right_length": {"internal": I_LEN, "swing": S_LEN},
                    "nearest_internal_pivot_lows_before":  _near_piv(_pli, "low",  I_LEN, _tgt_bar, True),
                    "nearest_internal_pivot_lows_after":   _near_piv(_pli, "low",  I_LEN, _tgt_bar, False),
                    "nearest_internal_pivot_highs_before": _near_piv(_phi, "high", I_LEN, _tgt_bar, True),
                    "nearest_internal_pivot_highs_after":  _near_piv(_phi, "high", I_LEN, _tgt_bar, False),
                }

            # 4 & 5. per-bar structure state + BOS-method comparison
            _range_bars = [b for b in range(_sn)
                           if (_sf_ms is None or times[b] >= _sf_ms)
                           and (_st_ms is None or times[b] <= _st_ms)][:1000]
            structure_state_by_bar = []
            bos_method_comparison_by_bar = []
            for b in _range_bars:
                bi = _bars_by_idx.get(b, {})
                structure_state_by_bar.append({
                    "bar": b, "time_utc": _stw(b),
                    "open": _so[b], "high": _sh[b], "low": _sl[b], "close": _sc[b],
                    "active_upP_before_bar": bi.get("active_upP_before_bar"),
                    "active_dnP_before_bar": bi.get("active_dnP_before_bar"),
                    "upP_first": bi.get("upP_first"),
                    "dnP_first": bi.get("dnP_first"),
                    "previous_upP_first": bi.get("prev_upP_first"),
                    "previous_dnP_first": bi.get("prev_dnP_first"),
                    "latest_internal_high_level": bi.get("upP_first"),
                    "latest_internal_low_level":  bi.get("dnP_first"),
                    "latest_swing_high_level": _lat_sh[b],
                    "latest_swing_low_level":  _lat_sl[b],
                    "internal_trend_before": None,
                    "internal_trend_after":  None,
                })
                bos_method_comparison_by_bar.append(_bos_method_rec(b))

            # 6. bos_after_target
            bos_after_target = []
            if _tgt_bar is not None:
                for b in range(_tgt_bar, min(_sn, _tgt_bar + 600)):
                    r = _bos_method_rec(b)
                    if (r["python_current_triggered"] or r["method_a_triggered"]
                            or r["method_b_triggered"] or r["wick_break_stored_level"]):
                        bos_after_target.append({
                            "bos_bar": b, "bos_time_utc": _stw(b),
                            "close_prev": r["close_prev"], "close_curr": r["close_curr"],
                            "high_curr": _sh[b], "low_curr": _sl[b],
                            "broken_level": r["stored_level"],
                            "condition_used_by_python": _PYCOND,
                            "python_triggered": r["python_current_triggered"],
                            "method_a_would_trigger": r["method_a_triggered"],
                            "method_b_would_trigger": r["method_b_triggered"],
                            "wick_break_would_trigger": r["wick_break_stored_level"],
                            "reason": r["reason_python_did_not_trigger"],
                        })

            # 7. potential_ob_from_target
            potential_ob_from_target = None
            if _tgt_bar is not None and _tgt_bar > 0:
                _src = _tgt_bar - 1
                _hl2 = (_sh[_src] + _sl[_src]) / 2.0
                _hlcc4 = (_sh[_src] + _sl[_src] + _sc[_src] + _sc[_src]) / 4.0
                if _is_bull:
                    _raw_bottom = _sl[_tgt_bar]; _raw_top = _hl2
                else:
                    _raw_top = _sh[_tgt_bar];    _raw_bottom = _hl2
                _ob_avg = (_raw_top + _raw_bottom) / 2.0
                _disp_top, _disp_bottom = _raw_top, _raw_bottom
                _pnote = "no precise adjustment"
                if _is_bull:
                    _body_low = min(_sc[_src], _so[_src])
                    if _ob_avg < _body_low and _raw_top > _hlcc4:
                        _disp_top = _ob_avg
                        _pnote = "precise: top collapsed to avg"
                else:
                    _body_high = max(_sc[_src], _so[_src])
                    if _ob_avg > _body_high and _raw_bottom < _hlcc4:
                        _disp_bottom = _ob_avg
                        _pnote = "precise: bottom collapsed to avg"
                _loh, _hih, _volh = 0.1594, 0.1602, 200003
                _mz = (abs(_disp_bottom - _loh) <= 0.01 * _loh
                       and abs(_disp_top - _hih) <= 0.01 * _hih)
                _mv = abs((_sv[_src] or 0) - _volh) <= max(0.05 * _volh, 1)
                potential_ob_from_target = {
                    "target_bar": _tgt_bar, "target_time_utc": _stw(_tgt_bar),
                    "sourceBar": _src, "source_time_utc": _stw(_src),
                    "source_volume": _sv[_src],
                    "raw_bottom": _raw_bottom, "raw_top": _raw_top,
                    "displayed_bottom": _disp_bottom, "displayed_top": _disp_top,
                    "would_match_tv_zone": bool(_mz),
                    "would_match_tv_volume": bool(_mv),
                    "notes": ("Force-simulated: target bar treated as the OB extreme; "
                              "sourceBar = target_bar - 1 (Pine +1 offset). " + _pnote),
                }

            # 8. structure_trace_summary
            _pybos_after = [r for r in bos_after_target if r["python_triggered"]]
            _mb_only = [r for r in bos_method_comparison_by_bar
                        if r["method_b_triggered"] and not r["python_current_triggered"]]
            _ma_only = [r for r in bos_method_comparison_by_bar
                        if r["method_a_triggered"] and not r["method_b_triggered"]]
            _wonly   = [r for r in bos_method_comparison_by_bar if r["wick_only_triggered"]]
            _tgt_is_pl = bool(_pli[_tgt_bar]) if _tgt_bar is not None else None
            _tgt_is_ph = bool(_phi[_tgt_bar]) if _tgt_bar is not None else None
            if _tgt_bar is None:
                _cause = "target_ob_time not found in candle stream"
            elif _is_bull and not _tgt_is_pl:
                _cause = "(a) target candle is NOT a confirmed internal pivot low — Python never anchors structure there"
            elif (not _is_bull) and not _tgt_is_ph:
                _cause = "(a) target candle is NOT a confirmed internal pivot high"
            elif not _pybos_after:
                _cause = "(b) target IS a pivot, but Python detects no BOS after it (no close crossover of the stored level)"
            elif _mb_only:
                _cause = "(c/e) Method B (close-cross of stored level) triggers on bars where Python's current condition does not — inspect bos_method_comparison_by_bar"
            else:
                _cause = "BOS after target IS detected by Python — missing OB likely from OB selection/overlap/mitigation, not structure"
            structure_trace_summary = {
                "target_is_confirmed_pivot_low":  _tgt_is_pl,
                "target_is_confirmed_pivot_high": _tgt_is_ph,
                "target_pivot_confirmed_at_time_utc": (
                    _stw(_tgt_bar + I_LEN) if (_tgt_bar is not None
                                               and (_tgt_is_pl or _tgt_is_ph)) else None),
                "python_detects_bos_after_target": len(_pybos_after) > 0,
                "first_python_bos_after_target": (_pybos_after[0]["bos_time_utc"]
                                                  if _pybos_after else None),
                "structure_level_python_watches": f"internal iLen={I_LEN} pivot {'highs (upP)' if _is_bull else 'lows (dnP)'}",
                "python_uses_internal_or_swing": "internal",
                "method_b_triggers_where_python_does_not": len(_mb_only) > 0,
                "method_a_triggers_where_method_b_does_not": len(_ma_only) > 0,
                "wick_only_break_without_body_close": len(_wonly) > 0,
                "python_current_method_classification": "Method B (close crossover of stored internal pivot level)",
                "likely_cause": _cause,
                "answers": {
                    "a_no_pivot_low_at_target": bool(_is_bull and _tgt_bar is not None and not _tgt_is_pl),
                    "b_no_bos_after_target": (_tgt_bar is not None and len(_pybos_after) == 0),
                    "c_wrong_stored_structure_level": len(_mb_only) > 0,
                    "d_internal_vs_swing_mismatch": "Python uses internal (iLen) only; compare latest_swing_* in structure_state_by_bar",
                    "e_method_a_vs_method_b_mismatch": (len(_ma_only) > 0 or len(_mb_only) > 0),
                    "f_close_vs_wick_mismatch": len(_wonly) > 0,
                    "g_ob_selection_after_bos": (len(_pybos_after) > 0),
                },
            }

            structure_trace_detail = {
                "target_side":    target_side,
                "target_ob_time": target_ob_time,
                "structure_from": structure_from,
                "structure_to":   structure_to,
                "target_candle":             target_candle,
                "python_current_bos_method": python_current_bos_method,
                "pivot_status":              pivot_status,
                "structure_state_by_bar":    structure_state_by_bar,
                "bos_method_comparison_by_bar": bos_method_comparison_by_bar,
                "bos_after_target":          bos_after_target,
                "potential_ob_from_target":  potential_ob_from_target,
                "structure_trace_summary":   structure_trace_summary,
            }

        # ── Debug-only structure-lifecycle trace (only when
        #    structure_lifecycle_trace=true → endpoint byte-identical otherwise).
        #    Re-simulates the upP/upB/dnP/dnB stacks from the pivot arrays and
        #    the authoritative BOS-event bars; no production logic is touched.
        structure_lifecycle_trace_detail = None
        lifecycle_trace_summary          = None
        if structure_lifecycle_trace:
            from main import detect_pivots

            _lo = [x["open"]   for x in main_candles]
            _lh = [x["high"]   for x in main_candles]
            _ll = [x["low"]    for x in main_candles]
            _lc = [x["close"]  for x in main_candles]
            _lv = [x["volume"] for x in main_candles]
            _ln = len(_lc)
            _is_bull = target_side == "bullish"

            def _lw(bar):
                return _ts(times[bar]) if (bar is not None and 0 <= bar < _ln) else None

            def _lparse(s):
                if not s:
                    return None
                for _fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S"):
                    try:
                        return int(_dt.datetime.strptime(s, _fmt)
                                   .replace(tzinfo=_dt.timezone.utc).timestamp() * 1000)
                    except ValueError:
                        continue
                return None

            def _lbar_at(ms):
                if ms is None or not times:
                    return None
                _best = None; _bd = None
                for _idx, _t in enumerate(times):
                    _d = abs(_t - ms)
                    if _bd is None or _d < _bd:
                        _bd = _d; _best = _idx
                return _best

            _ltgt_ms = _lparse(target_ob_time)
            _lf_ms   = _lparse(lifecycle_from)
            _lt_ms   = _lparse(lifecycle_to)
            _tgt     = _lbar_at(_ltgt_ms)

            _ph, _pl   = detect_pivots(_lh, _ll, I_LEN, I_LEN)
            _phs, _pls = detect_pivots(_lh, _ll, S_LEN, S_LEN)

            # Authoritative BOS events (legacy mode — BOS detection is
            # logic-mode-independent) + opt-in per-bar structure recorder.
            _lc_coll = {"events": [], "mitigations": [], "bars": []}
            detect_obs(_lo, _lh, _ll, _lc, _lv, I_LEN, S_LEN, max_ob=None,
                       trace=_lc_coll, ob_logic_mode="legacy_baseline")
            _bos_bull = {e["bos_bar"] for e in _lc_coll["events"] if e["side"] == "bullish"}
            _bos_bear = {e["bos_bar"] for e in _lc_coll["events"] if e["side"] == "bearish"}
            _rec_bars = {b["bar"]: b for b in _lc_coll["bars"]}

            # Re-simulate upP/upB/dnP/dnB. Pushes come from the pivot arrays;
            # clears use the authoritative BOS-event bars (the BOS *condition*
            # is never re-implemented).
            _start = max(I_LEN * 2 + 2, S_LEN + 2)
            _lcs = {}
            _uP = []; _uB = []; _dP = []; _dB = []
            _dnL_n = 0; _upL_n = 0
            _p_uPf = None; _p_dPf = None
            for i in range(_start, _ln):
                _uP_bef = _uP[0] if _uP else None
                _uB_bef = _uB[0] if _uB else None
                _dP_bef = _dP[0] if _dP else None
                _dB_bef = _dB[0] if _dB else None
                _phx = i - I_LEN >= 0 and _ph[i - I_LEN]
                _plx = i - I_LEN >= 0 and _pl[i - I_LEN]
                if _phx:
                    _uP.insert(0, _lh[i - I_LEN]); _uB.insert(0, i - I_LEN); _upL_n += 1
                if _plx:
                    _dP.insert(0, _ll[i - I_LEN]); _dB.insert(0, i - I_LEN); _dnL_n += 1
                _uPf = _uP[0] if _uP else None
                _uBf = _uB[0] if _uB else None
                _dPf = _dP[0] if _dP else None
                _dBf = _dB[0] if _dB else None
                _uPl = len(_uP); _dPl = len(_dP)
                _bull = i in _bos_bull; _bear = i in _bos_bear
                if _bull:
                    _uP = []; _uB = []
                if _bear:
                    _dP = []; _dB = []
                _lcs[i] = {
                    "bar": i,
                    "upP_before": _uP_bef, "upB_before": _uB_bef,
                    "dnP_before": _dP_bef, "dnB_before": _dB_bef,
                    "upP_first": _uPf, "upB_first": _uBf,
                    "dnP_first": _dPf, "dnB_first": _dBf,
                    "upP_len": _uPl, "dnP_len": _dPl,
                    "upP_after": (_uP[0] if _uP else None),
                    "upB_after": (_uB[0] if _uB else None),
                    "dnP_after": (_dP[0] if _dP else None),
                    "dnB_after": (_dB[0] if _dB else None),
                    "upP_len_after": len(_uP), "dnP_len_after": len(_dP),
                    "upL_len": _upL_n, "dnL_len": _dnL_n,
                    "prev_upP_first": _p_uPf, "prev_dnP_first": _p_dPf,
                    "bull_bos": _bull, "bear_bos": _bear,
                    "pivot_high_pushed": bool(_phx), "pivot_low_pushed": bool(_plx),
                }
                _p_uPf = _uP[0] if _uP else None
                _p_dPf = _dP[0] if _dP else None

            # Cross-check the re-sim against the recorded per-bar snapshot.
            _resim_mismatch = 0
            for i, st in _lcs.items():
                rb = _rec_bars.get(i)
                if rb is not None and (rb.get("upP_first") != st["upP_first"]
                                       or rb.get("dnP_first") != st["dnP_first"]
                                       or rb.get("upP_len") != st["upP_len"]):
                    _resim_mismatch += 1

            _lrange = [b for b in range(_ln)
                       if (_lf_ms is None or times[b] >= _lf_ms)
                       and (_lt_ms is None or times[b] <= _lt_ms)][:1500]

            # ── 1. target_context ──
            target_context = None
            if _tgt is not None:
                _src = _tgt - 1
                _hl2 = ((_lh[_src] + _ll[_src]) / 2.0) if _src >= 0 else None
                _fs_bottom = _ll[_tgt]
                _fs_top    = _hl2
                _loh, _hih, _volh = 0.1594, 0.1602, 200003
                _fs_vol = _lv[_src] if _src >= 0 else None
                _match = bool(_fs_top is not None and _fs_vol is not None
                              and abs(_fs_bottom - _loh) <= 0.01 * _loh
                              and abs(_fs_top - _hih) <= 0.01 * _hih
                              and abs(_fs_vol - _volh) <= 0.10 * _volh)
                target_context = {
                    "target_side": target_side, "target_ob_time": target_ob_time,
                    "target_bar": _tgt,
                    "open": _lo[_tgt], "high": _lh[_tgt], "low": _ll[_tgt],
                    "close": _lc[_tgt], "volume": _lv[_tgt],
                    "previous_bar": _src if _src >= 0 else None,
                    "previous_bar_time_utc": _lw(_src),
                    "previous_bar_open":   _lo[_src] if _src >= 0 else None,
                    "previous_bar_high":   _lh[_src] if _src >= 0 else None,
                    "previous_bar_low":    _ll[_src] if _src >= 0 else None,
                    "previous_bar_close":  _lc[_src] if _src >= 0 else None,
                    "previous_bar_volume": _lv[_src] if _src >= 0 else None,
                    "expected_tv_zone_low_hint":  _loh,
                    "expected_tv_zone_high_hint": _hih,
                    "expected_tv_volume_hint":    _volh,
                    "force_simulated_sourceBar":  _src if _src >= 0 else None,
                    "force_simulated_source_volume": _fs_vol,
                    "force_simulated_zone": {"bottom": _fs_bottom, "top": _fs_top},
                    "would_force_sim_match_tv": _match,
                }

            # last internal pivot HIGH pushed on/before the target bar
            _last_ph_b = None
            if _tgt is not None:
                for b in range(min(_tgt - I_LEN, _ln - 1), -1, -1):
                    if 0 <= b < _ln and _ph[b]:
                        _last_ph_b = b
                        break
            _last_ph_push  = (_last_ph_b + I_LEN) if _last_ph_b is not None else None
            _last_ph_price = _lh[_last_ph_b] if _last_ph_b is not None else None
            # first bullish BOS at/after that push → the clear
            _last_ph_clear = None
            if _last_ph_push is not None:
                _later = sorted(x for x in _bos_bull if x >= _last_ph_push)
                _last_ph_clear = _later[0] if _later else None
            # pivot highs pushed after that clear and on/before target
            _ph_after_clear = []
            if _last_ph_clear is not None and _tgt is not None:
                for b in range(_ln):
                    if _ph[b] and (b + I_LEN) > _last_ph_clear and (b + I_LEN) <= _tgt:
                        _ph_after_clear.append({"pivot_bar": b, "pivot_time_utc": _lw(b),
                                                "push_bar": b + I_LEN,
                                                "push_time_utc": _lw(b + I_LEN),
                                                "price": _lh[b]})

            _tgt_st = _lcs.get(_tgt, {})
            _upP_empty_at_target = (_tgt_st.get("upP_len", 0) == 0)
            _empty_after = True
            _to_bar = _lbar_at(_lt_ms) if _lt_ms is not None else (_ln - 1)
            if _tgt is not None:
                for b in range(_tgt + 1, min((_to_bar or _ln - 1) + 1, _ln)):
                    if _lcs.get(b, {}).get("upP_len_after", 0) > 0:
                        _empty_after = False
                        break

            # ── 5. last_active_bullish_level_before_target ──
            last_active_bullish_level_before_target = None
            if _tgt is not None:
                _lvl = None; _lvl_bar = None
                for b in range(_tgt, _start - 1, -1):
                    st = _lcs.get(b)
                    if st and st.get("upP_len", 0) > 0:
                        _lvl = st["upP_first"]; _lvl_bar = st["upB_first"]
                        break
                if _lvl is not None:
                    _conf = (_lvl_bar + I_LEN) if _lvl_bar is not None else None
                    # active_until: first bullish BOS clear at/after the push
                    _au = None
                    if _conf is not None:
                        _later = sorted(x for x in _bos_bull if x >= _conf)
                        _au = _later[0] if _later else None
                    # would price close above this level after target?
                    _first_above = None
                    for j in range(_tgt + 1, _ln):
                        if _lc[j] > _lvl:
                            _first_above = j
                            break
                    # would BOS trigger if level kept active (close crossover)?
                    _would_bos_bar = None
                    for j in range(max(_tgt, (_conf or 0) + 1), _ln):
                        if j >= 1 and _lc[j] > _lvl and _lc[j - 1] <= _lvl:
                            _would_bos_bar = j
                            break
                    # would the OB search window include the target bar?
                    _win_inc = None; _tgt_is_low = None
                    if _would_bos_bar is not None and _lvl_bar is not None:
                        _ss = _lvl_bar + 1
                        _se = _would_bos_bar + 1
                        _win_inc = bool(_ss <= _tgt < _se)
                        if _win_inc:
                            _wlo = min(_ll[_ss:_se]) if _se > _ss else None
                            _tgt_is_low = bool(_wlo is not None and _ll[_tgt] == _wlo)
                    last_active_bullish_level_before_target = {
                        "level": _lvl,
                        "pivot_bar": _lvl_bar,
                        "pivot_time_utc": _lw(_lvl_bar),
                        "confirmed_at_bar": _conf,
                        "confirmed_at_time_utc": _lw(_conf),
                        "active_from_bar": _conf,
                        "active_until_bar": _au,
                        "active_until_time_utc": _lw(_au),
                        "cleared_reason": ("bullish BOS close-crossover cleared upP"
                                           if _au is not None else "not cleared in range"),
                        "did_price_close_above_this_level_after_target": _first_above is not None,
                        "first_close_above_after_target_bar": _first_above,
                        "first_close_above_after_target_time_utc": _lw(_first_above),
                        "if_level_stayed_active_would_bullish_bos_trigger": _would_bos_bar is not None,
                        "would_bullish_bos_bar": _would_bos_bar,
                        "would_bullish_bos_time_utc": _lw(_would_bos_bar),
                        "if_bos_triggered_would_window_include_target": _win_inc,
                        "target_is_window_lowest_low": _tgt_is_low,
                    }

            # ── 6. candidate_tv_level_probe ──
            candidate_tv_level_probe = []
            for b in _lrange:
                if not _ph[b]:
                    continue
                _lvlb = _lh[b]
                _conf = b + I_LEN
                _cross = None
                _scan0 = _tgt + 1 if _tgt is not None else _conf + 1
                for j in range(max(_scan0, _conf + 1), _ln):
                    if j >= 1 and _lc[j] > _lvlb and _lc[j - 1] <= _lvlb:
                        _cross = j
                        break
                _would_win = None; _would_sel = None
                if _cross is not None and _tgt is not None:
                    _ss = b + 1; _se = _cross + 1
                    _would_win = bool(_ss <= _tgt < _se)
                    if _would_win:
                        _wlo = min(_ll[_ss:_se]) if _se > _ss else None
                        _would_sel = bool(_wlo is not None and _ll[_tgt] == _wlo)
                candidate_tv_level_probe.append({
                    "pivot_bar": b, "pivot_time_utc": _lw(b),
                    "level": _lvlb,
                    "confirmed_at_bar": _conf, "confirmed_at_time_utc": _lw(_conf),
                    "did_close_cross_after_target": _cross is not None,
                    "cross_bar": _cross, "cross_time_utc": _lw(_cross),
                    "would_create_ob_from_target_window": _would_win,
                    "would_select_target_as_lowest_low": _would_sel,
                })
            _likely = next((p for p in candidate_tv_level_probe
                            if p["would_select_target_as_lowest_low"]), None)
            for p in candidate_tv_level_probe:
                p["most_likely_tv_broken_level"] = (p is _likely)

            # ── 3. upP_lifecycle_events ──
            _EVT_TYPES_NOTE = "pivot_high_pushed|pivot_low_pushed|bullish_bos_triggered|bearish_bos_triggered|upP_cleared|dnP_cleared|upP_empty|dnP_empty"
            upP_lifecycle_events = []
            _prev_uPla = None; _prev_dPla = None
            for b in _lrange:
                st = _lcs.get(b)
                if st is None:
                    continue
                _cp = _lc[b - 1] if b > 0 else None
                _cc = _lc[b]
                _common = {
                    "bar": b, "time_utc": _lw(b),
                    "upP_before": st["upP_before"], "upP_after": st["upP_after"],
                    "upB_before": st["upB_before"], "upB_after": st["upB_after"],
                    "dnP_before": st["dnP_before"], "dnP_after": st["dnP_after"],
                    "dnB_before": st["dnB_before"], "dnB_after": st["dnB_after"],
                    "prev_upP_first_before": st["prev_upP_first"],
                    "prev_upP_first_after":  st["upP_after"],
                    "prev_dnP_first_before": st["prev_dnP_first"],
                    "prev_dnP_first_after":  st["dnP_after"],
                    "close_prev": _cp, "close_curr": _cc,
                }
                if st["pivot_high_pushed"]:
                    _e = dict(_common); _e.update({
                        "event_type": "pivot_high_pushed",
                        "reason": f"internal pivot high confirmed (bar {b-I_LEN}) pushed to upP",
                        "cleared_by_event_bar": None, "cleared_by_event_time_utc": None})
                    upP_lifecycle_events.append(_e)
                if st["pivot_low_pushed"]:
                    _e = dict(_common); _e.update({
                        "event_type": "pivot_low_pushed",
                        "reason": f"internal pivot low confirmed (bar {b-I_LEN}) pushed to dnP",
                        "cleared_by_event_bar": None, "cleared_by_event_time_utc": None})
                    upP_lifecycle_events.append(_e)
                if st["bull_bos"]:
                    _e = dict(_common); _e.update({
                        "event_type": "bullish_bos_triggered",
                        "reason": "close crossover of upP[0] — bullish BOS",
                        "cleared_by_event_bar": None, "cleared_by_event_time_utc": None})
                    upP_lifecycle_events.append(_e)
                    if st["upP_len"] > 0:
                        _e2 = dict(_common); _e2.update({
                            "event_type": "upP_cleared",
                            "reason": "bullish BOS cleared upP / upB",
                            "cleared_by_event_bar": b, "cleared_by_event_time_utc": _lw(b)})
                        upP_lifecycle_events.append(_e2)
                if st["bear_bos"]:
                    _e = dict(_common); _e.update({
                        "event_type": "bearish_bos_triggered",
                        "reason": "close crossunder of dnP[0] — bearish BOS",
                        "cleared_by_event_bar": None, "cleared_by_event_time_utc": None})
                    upP_lifecycle_events.append(_e)
                    if st["dnP_len"] > 0:
                        _e2 = dict(_common); _e2.update({
                            "event_type": "dnP_cleared",
                            "reason": "bearish BOS cleared dnP / dnB",
                            "cleared_by_event_bar": b, "cleared_by_event_time_utc": _lw(b)})
                        upP_lifecycle_events.append(_e2)
                if st["upP_len_after"] == 0 and _prev_uPla not in (None, 0):
                    _e = dict(_common); _e.update({
                        "event_type": "upP_empty",
                        "reason": "upP is empty (no active bullish structure level)",
                        "cleared_by_event_bar": None, "cleared_by_event_time_utc": None})
                    upP_lifecycle_events.append(_e)
                if st["dnP_len_after"] == 0 and _prev_dPla not in (None, 0):
                    _e = dict(_common); _e.update({
                        "event_type": "dnP_empty",
                        "reason": "dnP is empty (no active bearish structure level)",
                        "cleared_by_event_bar": None, "cleared_by_event_time_utc": None})
                    upP_lifecycle_events.append(_e)
                _prev_uPla = st["upP_len_after"]
                _prev_dPla = st["dnP_len_after"]

            # ── 4. structure_state_table ──
            structure_state_table = []
            for b in _lrange:
                st = _lcs.get(b, {})
                _cp = _lc[b - 1] if b > 0 else None
                _cc = _lc[b]
                _lvl = st.get("upP_first")
                _plvl = st.get("prev_upP_first")
                _cpp = (_plvl is not None and _cp is not None and _cp <= _plvl)
                _ccp = (_lvl is not None and _cc > _lvl)
                _bull = st.get("bull_bos", False)
                if _bull:
                    _rn = None
                elif _lvl is None:
                    _rn = "upP empty — no active bullish structure level"
                elif not _ccp:
                    _rn = f"close_curr {_cc} did not exceed upP[0] {_lvl}"
                elif _plvl is None:
                    _rn = "prev_upP_first is None (upP empty on previous bar)"
                elif not _cpp:
                    _rn = f"no crossover — prev close {_cp} already above prev upP[0] {_plvl}"
                elif st.get("dnL_len", 0) <= 1:
                    _rn = "len(dnL) <= 1 guard"
                else:
                    _rn = "condition components met (see bullish_bos_triggered)"
                structure_state_table.append({
                    "bar": b, "time_utc": _lw(b),
                    "open": _lo[b], "high": _lh[b], "low": _ll[b], "close": _cc,
                    "upP_before_bar": st.get("upP_before"),
                    "upP_after_bar":  st.get("upP_after"),
                    "dnP_before_bar": st.get("dnP_before"),
                    "dnP_after_bar":  st.get("dnP_after"),
                    "upP_len": st.get("upP_len"), "dnP_len": st.get("dnP_len"),
                    "upL_len": st.get("upL_len"), "dnL_len": st.get("dnL_len"),
                    "prev_upP_first_before": _plvl,
                    "prev_upP_first_after":  st.get("upP_after"),
                    "bullish_bos_condition_level": _lvl,
                    "bullish_bos_close_prev_pass": bool(_cpp),
                    "bullish_bos_close_curr_pass": bool(_ccp),
                    "bullish_bos_triggered": bool(_bull),
                    "bearish_bos_triggered": bool(st.get("bear_bos", False)),
                    "reason_no_bullish_bos": _rn,
                })

            # ── 2. lifecycle_summary ──
            _dnl_at_tgt = _tgt_st.get("dnL_len")
            lifecycle_summary = {
                "upP_empty_at_target": _upP_empty_at_target,
                "upP_empty_after_target_until_to": _empty_after,
                "last_upP_pivot_high_pushed_before_target_bar": _last_ph_push,
                "last_upP_pivot_high_pushed_before_target_time_utc": _lw(_last_ph_push),
                "last_upP_price": _last_ph_price,
                "last_upP_cleared_at_bar": _last_ph_clear,
                "last_upP_cleared_at_time_utc": _lw(_last_ph_clear),
                "cleared_by_event": ("bullish_bos" if _last_ph_clear is not None else None),
                "cleared_by_bullish_bos": _last_ph_clear is not None,
                "cleared_by_bearish_bos": False,
                "pivot_highs_after_last_clear_before_target": _ph_after_clear,
                "any_pivot_high_after_last_clear_before_target": len(_ph_after_clear) > 0,
                "why_not_active_upP": (
                    "a later bullish BOS cleared upP again"
                    if (_ph_after_clear and _upP_empty_at_target)
                    else ("upP holds the pushed pivot(s)" if not _upP_empty_at_target
                          else "no pivot high pushed after the last clear")),
                "dnL_len_at_target": _dnl_at_tgt,
                "dnL_guard_satisfied_at_target": (_dnl_at_tgt or 0) > 1,
                "dnL_guard_blocks_bullish_bos": False if (_dnl_at_tgt or 0) > 1 else None,
                "cause_classification": {
                    "a_no_pivot_high_created": _last_ph_b is None,
                    "b_pivot_high_created_but_cleared_too_early":
                        (_last_ph_b is not None and _upP_empty_at_target),
                    "c_upP_clear_logic_mismatch": _resim_mismatch > 0,
                    "d_previous_bar_crossover_state_mismatch": _resim_mismatch > 0,
                    "e_internal_vs_swing_level_mismatch":
                        "Python OB BOS uses internal iLen pivots only; swing sLen levels are not used",
                    "f_dnL_or_upL_guard_mismatch": (_dnl_at_tgt or 0) <= 1,
                },
            }

            # ── 7. lifecycle_trace_summary ──
            if _tgt is None:
                _concl = "target_ob_time not found in the candle stream"
            elif _upP_empty_at_target:
                _concl = (f"Python has NO active bullish structure level (upP empty) at the "
                          f"target bar {_tgt} ({_lw(_tgt)}), so no bullish BOS can fire and no "
                          f"OB is created. Last upP high was pushed at bar {_last_ph_push} "
                          f"({_lw(_last_ph_push)}, level {_last_ph_price}) and cleared by a "
                          f"bullish BOS at bar {_last_ph_clear} ({_lw(_last_ph_clear)}).")
            else:
                _concl = (f"upP is NOT empty at the target bar (level "
                          f"{_tgt_st.get('upP_first')}); the missing OB is not a bare "
                          f"upP-empty issue — inspect structure_state_table for the BOS "
                          f"condition components.")
            lifecycle_trace_summary = {
                "conclusion": _concl,
                "problem_is_upP_not_created": _last_ph_b is None,
                "problem_is_upP_cleared_too_early":
                    (_last_ph_b is not None and _upP_empty_at_target),
                "bad_state_caused_by_bar": _last_ph_clear,
                "bad_state_caused_by_time_utc": _lw(_last_ph_clear),
                "bad_state_caused_by_event": ("bullish_bos that cleared upP"
                                              if _last_ph_clear is not None else None),
                "suggested_fix_direction": (
                    "Investigate the upP clear/lifecycle: after a bullish BOS clears upP, a "
                    "subsequently confirmed pivot high should re-populate upP before the next "
                    "BOS. If TradingView keeps an active level here, Python's upP clear or "
                    "pivot re-population timing is the likely divergence. DIAGNOSTIC ONLY — "
                    "do not implement yet."),
                "resim_vs_recorded_mismatch_count": _resim_mismatch,
            }

            structure_lifecycle_trace_detail = {
                "target_side":    target_side,
                "target_ob_time": target_ob_time,
                "lifecycle_from": lifecycle_from,
                "lifecycle_to":   lifecycle_to,
                "event_type_legend": _EVT_TYPES_NOTE,
                "resim_vs_recorded_mismatch_count": _resim_mismatch,
                "target_context":            target_context,
                "lifecycle_summary":         lifecycle_summary,
                "upP_lifecycle_events":      upP_lifecycle_events,
                "structure_state_table":     structure_state_table,
                "last_active_bullish_level_before_target": last_active_bullish_level_before_target,
                "candidate_tv_level_probe":  candidate_tv_level_probe,
                "lifecycle_trace_summary":   lifecycle_trace_summary,
            }

        # ── Debug-only structure-candidate simulation (only when
        #    structure_candidate_trace=true → endpoint byte-identical otherwise).
        #    Each variant runs detect_obs with the production tv_parity_v2 OB
        #    logic + the candidate structure_candidate, then checks whether the
        #    target OB appears.
        structure_candidate_trace_detail = None
        if structure_candidate_trace:
            from main import detect_pivots, _detect_pivots_relaxed

            _cco = [x["open"]   for x in main_candles]
            _cch = [x["high"]   for x in main_candles]
            _ccl = [x["low"]    for x in main_candles]
            _ccc = [x["close"]  for x in main_candles]
            _ccv = [x["volume"] for x in main_candles]
            _ccn = len(_ccc)

            def _ctw(bar):
                return _ts(times[bar]) if (bar is not None and 0 <= bar < _ccn) else None

            def _cparse(s):
                if not s:
                    return None
                for _fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S"):
                    try:
                        return int(_dt.datetime.strptime(s, _fmt)
                                   .replace(tzinfo=_dt.timezone.utc).timestamp() * 1000)
                    except ValueError:
                        continue
                return None

            def _cbar_at(ms):
                if ms is None or not times:
                    return None
                _best = None; _bd = None
                for _idx, _t in enumerate(times):
                    _d = abs(_t - ms)
                    if _bd is None or _d < _bd:
                        _bd = _d; _best = _idx
                return _best

            _ctgt   = _cbar_at(_cparse(target_ob_time))
            _cf_ms  = _cparse(candidate_from)
            _ct_ms  = _cparse(candidate_to)
            _LOH, _HIH, _VOLH = 0.1594, 0.1602, 200003
            _SRC = {"current": "current",
                    "retain_broken_upP": "retained_upP",
                    "promote_bos_high_to_upP": "promoted_bos_high",
                    "equal_high_pivot_relaxed": "relaxed_pivot_high"}

            _variants_out = []
            for _var in _sc_to_run:
                _ccoll = {"events": [], "mitigations": []}
                _cobs, _ = detect_obs(_cco, _cch, _ccl, _ccc, _ccv, I_LEN, S_LEN,
                                      max_ob=None, ob_logic_mode="tv_parity_v2",
                                      structure_candidate=_var, trace=_ccoll)
                _tob = next((ob for ob in _cobs
                             if ob["type"] == "bullish" and ob.get("bar") == _ctgt), None)
                _bev = [e for e in _ccoll["events"] if e["side"] == "bullish"]
                _sel_ev = next((e for e in _bev if e.get("min_idx") == _ctgt), None)
                _win_ev = next((e for e in _bev
                                if _ctgt is not None and e.get("search_start") is not None
                                and e["search_start"] <= _ctgt < e["search_end"]), None)
                _ev = _sel_ev or _win_ev

                _win_inc = None; _tgt_low = None
                if _ev is not None and _ctgt is not None:
                    _ss, _se = _ev["search_start"], _ev["search_end"]
                    _win_inc = bool(_ss <= _ctgt < _se)
                    if _win_inc and _se > _ss:
                        _wlo = min(_ccl[_ss:_se])
                        _tgt_low = bool(_ccl[_ctgt] == _wlo)

                _so_bar = (_tob.get("bar") if _tob else (_ev.get("min_idx") if _ev else None))
                _so_src = (_tob.get("sourceBar") if _tob else (_ev.get("ob_source") if _ev else None))
                _so_vol = (_tob.get("volume") if _tob else (_ev.get("volume") if _ev else None))
                _so_bot = (_tob.get("bottom") if _tob else (_ev.get("ob_bottom") if _ev else None))
                _so_top = (_tob.get("top") if _tob else (_ev.get("ob_top") if _ev else None))
                _so_avg = (_tob.get("avg") if _tob else (_ev.get("ob_avg") if _ev else None))
                _matched = bool(_tob is not None and _so_bot is not None and _so_top is not None
                                and abs(_so_bot - _LOH) <= 0.01 * _LOH
                                and abs(_so_top - _HIH) <= 0.01 * _HIH)

                _upP_after = None
                if _ev is not None:
                    if _var == "retain_broken_upP":
                        _upP_after = _ev.get("broken_level")
                    elif _var == "promote_bos_high_to_upP":
                        _bb = _ev.get("bos_bar")
                        _upP_after = _cch[_bb] if (_bb is not None and 0 <= _bb < _ccn) else None

                # ── Target survival trace ─────────────────────────────────
                # Re-run the bullish visible-pool overlap+showLast filter on
                # the candidate's surviving source pool to classify why the
                # target OB (if selected) might be hidden.
                _bull_src = [ob for ob in _cobs if ob["type"] == "bullish"]
                _accepted_b = []; _tgt_in_accepted = False
                _overlap_prev_bar = None; _overlap_prev_top = None
                for _o in _bull_src:
                    _ov = bool(_accepted_b and _o["bottom"] < _accepted_b[-1]["top"])
                    if not _ov:
                        _accepted_b.append(_o)
                        if _o.get("bar") == _ctgt:
                            _tgt_in_accepted = True
                    elif _o.get("bar") == _ctgt:
                        _overlap_prev_bar = _accepted_b[-1].get("bar")
                        _overlap_prev_top = _accepted_b[-1].get("top")
                _visible_b = _accepted_b[-5:] if len(_accepted_b) > 5 else list(_accepted_b)
                _tgt_in_visible = any(ob.get("bar") == _ctgt for ob in _visible_b)
                _show_rank = next((idx for idx, ob in enumerate(_visible_b)
                                   if ob.get("bar") == _ctgt), None)

                _mit = next((m for m in _ccoll["mitigations"]
                             if m.get("ob_type") == "bullish"
                             and m.get("ob_bar") == _ctgt), None)

                _co = (_sel_ev or {}).get("creation_overlap") or {}
                _co_deleted = bool(_co.get("deleted_by_creation_overlap"))
                _co_prev_bar = _co.get("prev_ob_bar")

                if _ctgt is None or _sel_ev is None:
                    _hidden_reason = None
                elif _co_deleted:
                    _hidden_reason = "creation_overlap_deleted"
                elif _mit is not None:
                    _hidden_reason = "mitigated"
                elif not _tgt_in_accepted:
                    _hidden_reason = "overlap_previous"
                elif not _tgt_in_visible:
                    _hidden_reason = "beyond_showLast"
                else:
                    _hidden_reason = "none"

                target_survival_trace = None
                if _sel_ev is not None:
                    target_survival_trace = {
                        "selected_as_ob_extreme": True,
                        "selected_bar":       _sel_ev.get("min_idx"),
                        "selected_time_utc":  _ctw(_sel_ev.get("min_idx")),
                        "sourceBar":          _sel_ev.get("ob_source"),
                        "source_time_utc":    _ctw(_sel_ev.get("ob_source")),
                        "source_volume":      _sel_ev.get("volume"),
                        "zone_bottom":        _sel_ev.get("ob_bottom"),
                        "zone_top":           _sel_ev.get("ob_top"),
                        "bos_bar":            _sel_ev.get("bos_bar"),
                        "bos_time_utc":       _ctw(_sel_ev.get("bos_bar")),
                        "creation_overlap_decision": {
                            "checked": _co.get("checked", False),
                            "previous_same_direction_ob_bar":  _co_prev_bar,
                            "previous_same_direction_ob_time": _ctw(_co_prev_bar),
                            "previous_same_direction_ob_zone": (
                                {"bottom": _co.get("prev_ob_bottom"),
                                 "top":    _co.get("prev_ob_top"),
                                 "avg":    _co.get("prev_ob_avg")}
                                if _co_prev_bar is not None else None),
                            "overlap_rule_used":           _co.get("rule"),
                            "deleted_by_creation_overlap": _co_deleted,
                            "reason": (
                                f"bullish overlap (raw): new bottom {_sel_ev.get('ob_bottom')} "
                                f"< prev top {_co.get('prev_ob_top')}"
                                if _co_deleted else "not deleted by creation overlap"),
                        },
                        "mitigation_decision": {
                            "checked":   True,
                            "mitigated": _mit is not None,
                            "mitigation_bar":          (_mit.get("mitigated_at_bar") if _mit else None),
                            "mitigation_time_utc":     (_ctw(_mit.get("mitigated_at_bar")) if _mit else None),
                            "mitigation_close":        (_mit.get("close_at_mitigation") if _mit else None),
                            "mitigation_trigger_price": (_mit.get("trigger") if _mit else None),
                            "mitigation_rule_used": (
                                "Absolute closed-only — bullish trigger = ob.bottom; "
                                "close < trigger ⇒ mitigated (last possibly-open candle skipped)"
                                if _mit else None),
                            "reason": (
                                f"close {_mit.get('close_at_mitigation')} < trigger "
                                f"{_mit.get('trigger')} at bar {_mit.get('mitigated_at_bar')}"
                                if _mit else "not mitigated"),
                        },
                        "visible_pool_decision": {
                            "added_to_source_pool":     _tob is not None,
                            "included_in_visible_pool": _tgt_in_visible,
                            "hidden_reason":            _hidden_reason,
                            "showLast_rank":            _show_rank,
                            "overlap_previous_bar":     _overlap_prev_bar,
                            "overlap_previous_time":    _ctw(_overlap_prev_bar),
                            "accepted_after_overlap_count": len(_accepted_b),
                            "visible_count":            len(_visible_b),
                        },
                    }

                # target_not_selected_reason — refined using survival info
                if _ctgt is None:
                    _not_sel = "target_ob_time not found in candle stream"
                elif _tob is not None:
                    _not_sel = None
                elif _sel_ev is not None:
                    _not_sel = (f"target selected as OB extreme at BOS bar "
                                f"{_sel_ev.get('bos_bar')} but the OB did not survive — "
                                f"hidden_reason={_hidden_reason}")
                elif _ev is None:
                    _not_sel = "no bullish BOS search window includes the target bar"
                elif not _win_inc:
                    _not_sel = "nearest bullish BOS window does not include the target bar"
                else:
                    _osel = _ev.get("min_idx")
                    _not_sel = (f"window includes target but candle {_osel} "
                                f"({_ctw(_osel)}) is the window extreme, not the target")

                _variants_out.append({
                    "variant_name": _var,
                    "did_create_target_ob": _tob is not None,
                    "selected_target_ob":   _sel_ev is not None,
                    "survived_active_pool": _tob is not None,
                    "visible_at_debug_as_of": _tgt_in_visible,
                    "selected_ob_time": _ctw(_so_bar),
                    "selected_ob_bar": _so_bar,
                    "selected_sourceBar": _so_src,
                    "selected_source_time": _ctw(_so_src),
                    "selected_source_volume": _so_vol,
                    "selected_zone_bottom": _so_bot,
                    "selected_zone_top": _so_top,
                    "selected_avg": _so_avg,
                    "matched_tv_target": _matched,
                    "bos_bar": _ev.get("bos_bar") if _ev else None,
                    "bos_time_utc": _ctw(_ev.get("bos_bar")) if _ev else None,
                    "broken_level": _ev.get("broken_level") if _ev else None,
                    "broken_level_source": _SRC.get(_var, "current"),
                    "upP_level_before_bos": _ev.get("broken_level") if _ev else None,
                    "upP_level_after_bos": _upP_after,
                    "search_start_bar": _ev.get("search_start") if _ev else None,
                    "search_start_time_utc": _ctw(_ev.get("search_start")) if _ev else None,
                    "search_end_bar": _ev.get("search_end") if _ev else None,
                    "search_end_time_utc": (_ctw(_ev["search_end"] - 1)
                                            if _ev and _ev.get("search_end") else None),
                    "anchor_mode_used": "latest_opposite_pivot",
                    "latest_opposite_pivot_used": _ev.get("anchor_pivot_bar") if _ev else None,
                    "latest_opposite_pivot_time": (_ctw(_ev.get("anchor_pivot_bar"))
                                                   if _ev else None),
                    "did_search_window_include_target": _win_inc,
                    "was_target_lowest_low_in_actual_v2_anchor_window": _tgt_low,
                    "target_not_selected_reason": _not_sel,
                    "target_survival_trace": target_survival_trace,
                })

            # equal-high relaxed: pivot highs the relaxed rule finds that the
            # strict rule rejects (the "after 21:00 plateau" question).
            _ph_strict, _ = detect_pivots(_cch, _ccl, I_LEN, I_LEN)
            _ph_relax, _  = _detect_pivots_relaxed(_cch, _ccl, I_LEN, I_LEN)
            _relax_extra = []
            for b in range(_ccn):
                if _ph_relax[b] and not _ph_strict[b]:
                    if ((_cf_ms is None or times[b] >= _cf_ms)
                            and (_ct_ms is None or times[b] <= _ct_ms)):
                        _relax_extra.append({"pivot_bar": b, "pivot_time_utc": _ctw(b),
                                             "high": _cch[b]})

            _creates = [v["variant_name"] for v in _variants_out if v["did_create_target_ob"]]
            _matches = [v["variant_name"] for v in _variants_out if v["matched_tv_target"]]
            _retain = next((v for v in _variants_out
                            if v["variant_name"] == "retain_broken_upP"), None)
            # Per-variant survival summary for quick scanning
            _per_variant_survival = [{
                "variant_name":           v["variant_name"],
                "selected_target_ob":     v["selected_target_ob"],
                "survived_active_pool":   v["survived_active_pool"],
                "visible_at_debug_as_of": v["visible_at_debug_as_of"],
                "hidden_reason":          ((v["target_survival_trace"] or {})
                                            .get("visible_pool_decision", {})
                                            .get("hidden_reason")
                                           if v.get("target_survival_trace") else None),
            } for v in _variants_out]
            _selects   = [v["variant_name"] for v in _variants_out if v["selected_target_ob"]]
            _survives  = [v["variant_name"] for v in _variants_out if v["survived_active_pool"]]
            _visibles  = [v["variant_name"] for v in _variants_out if v["visible_at_debug_as_of"]]
            candidate_variant_summary = {
                "variants_compared": _sc_to_run,
                "variants_that_select_target_ob":   _selects,
                "variants_that_survive_active_pool": _survives,
                "variants_visible_at_debug_as_of":   _visibles,
                "variants_that_create_target_ob": _creates,
                "variants_that_match_tv_target":  _matches,
                "per_variant_survival": _per_variant_survival,
                "current_creates_target_ob": any(
                    v["variant_name"] == "current" and v["did_create_target_ob"]
                    for v in _variants_out),
                "retain_broken_upP_creates_target_ob": bool(
                    _retain and _retain["did_create_target_ob"]),
                "retain_broken_upP_selects_target_ob": bool(
                    _retain and _retain["selected_target_ob"]),
                "retain_broken_upP_visible_at_debug_as_of": bool(
                    _retain and _retain["visible_at_debug_as_of"]),
                "equal_high_relaxed_extra_pivot_highs_count": len(_relax_extra),
                "equal_high_relaxed_extra_pivot_highs": _relax_extra[:50],
                "closest_to_tradingview": (_matches[0] if _matches
                                           else (_creates[0] if _creates else "none")),
                "would_affect_prior_confirmed_cases_theory": (
                    "Retaining / promoting structure levels or relaxing pivots changes "
                    "BOS frequency globally — ETHUSDT 4H, LINKUSDT 15m and SOONUSDT 15m "
                    "bear MUST be re-verified before any production change."),
                "recommended_next_fix_direction": (
                    "If retain_broken_upP creates the 2026-05-22 07:00 OB and the prior "
                    "confirmed cases stay green, the upP-clear lifecycle is the likely fix "
                    "area. If only equal_high_pivot_relaxed creates it, the divergence is "
                    "in strict pivot-high detection. DIAGNOSTIC ONLY — do not implement."),
            }

            structure_candidate_trace_detail = {
                "target_side":    target_side,
                "target_ob_time": target_ob_time,
                "target_bar":     _ctgt,
                "candidate_from": candidate_from,
                "candidate_to":   candidate_to,
                "debug_as_of":               debug_as_of,
                "sliced_newest_candle_time": (_ts(main_candles[-1].get("time"))
                                              if main_candles else None),
                "expected_tv_zone_low_hint":  _LOH,
                "expected_tv_zone_high_hint": _HIH,
                "expected_tv_volume_hint":    _VOLH,
                "variants": _variants_out,
                "candidate_variant_summary": candidate_variant_summary,
            }

        # ── Debug-only structure-candidate GLOBAL visible-pool comparison ────
        #    Runs the full _analyse pipeline (tv_parity_v2 OB rules) per
        #    structure_candidate variant and returns the resulting visible
        #    pools so the candidate's effect on the WHOLE table is visible.
        structure_candidate_global_detail = None
        if structure_candidate_global:
            # variants to run — reuse _sc_to_run if already chosen by the
            # candidate-trace param, else default to "all" (all four).
            _sc_global_run = list(_sc_to_run) if _sc_to_run else list(_SC_VARIANTS)

            _SOON_EXP_BULL = ["2026-05-25 03:15 UTC", "2026-05-24 21:45 UTC",
                              "2026-05-23 14:00 UTC", "2026-05-23 12:15 UTC",
                              "2026-04-15 14:00 UTC"]
            _SOON_EXP_BEAR = ["2026-05-25 20:45 UTC", "2026-05-25 13:00 UTC",
                              "2026-05-12 20:00 UTC", "2026-05-09 10:30 UTC",
                              "2026-05-09 07:45 UTC"]

            def _ob_summary(ob):
                _bar = ob.get("bar")
                _src = ob.get("sourceBar")
                return {
                    "time":               _ts_i(_bar) if _bar is not None else None,
                    "bottom":             ob.get("bottom"),
                    "top":                ob.get("top"),
                    "avg":                ob.get("avg"),
                    "volume":             ob.get("volume"),
                    "tvObVolumeSharePct": ob.get("tvObVolumeSharePct"),
                    "sourceBar":          _src,
                    "source_time_utc":    (_ts_i(_src) if (_src is not None
                                                            and 0 <= _src < len(times)) else None),
                    "touches":            ob.get("touches"),
                    "isVirgin":           ob.get("isVirgin"),
                    "mitigated":          ob.get("mitigated"),
                    "hidden_reason":      None,
                }

            _variants_global = []
            for _gvar in _sc_global_run:
                _gx = _analyse(main_candles,
                               mit_closed=True, bear_eff_bottom=True,
                               anchor_mode="latest_opposite_pivot", tie_mode="last",
                               structure_candidate=_gvar)
                _gb = [_ob_summary(ob) for ob in _gx["bull_vis"]]
                _gr = [_ob_summary(ob) for ob in _gx["bear_vis"]]
                _bull_times = [x["time"] for x in _gb]
                _bear_times = [x["time"] for x in _gr]
                _bull_set   = set(_bull_times)
                _bear_set   = set(_bear_times)
                _exp_bull   = set(_SOON_EXP_BULL)
                _exp_bear   = set(_SOON_EXP_BEAR)
                _bull_missing = [t for t in _SOON_EXP_BULL if t not in _bull_set]
                _bull_extra   = [t for t in _bull_times if t not in _exp_bull]
                _bear_missing = [t for t in _SOON_EXP_BEAR if t not in _bear_set]
                _bear_extra   = [t for t in _bear_times if t not in _exp_bear]
                _score = ((len(_SOON_EXP_BULL) - len(_bull_missing))
                          + (len(_SOON_EXP_BEAR) - len(_bear_missing)))
                _variants_global.append({
                    "variant_name": _gvar,
                    "bullish_visible_pool_summary": _gb,
                    "bearish_visible_pool_summary": _gr,
                    "bullish_visible_total_volume": _gx["bull_vtot"],
                    "bearish_visible_total_volume": _gx["bear_vtot"],
                    "counts": {
                        "bullish_source_count":  len(_gx["bull_src"]),
                        "bearish_source_count":  len(_gx["bear_src"]),
                        "bullish_visible_count": len(_gx["bull_vis"]),
                        "bearish_visible_count": len(_gx["bear_vis"]),
                    },
                    "comparison_helpers": {
                        "bull_times":       _bull_times,
                        "bear_times":       _bear_times,
                        "bull_pct_list":    [x["tvObVolumeSharePct"] for x in _gb],
                        "bear_pct_list":    [x["tvObVolumeSharePct"] for x in _gr],
                        "bull_volume_list": [x["volume"] for x in _gb],
                        "bear_volume_list": [x["volume"] for x in _gr],
                    },
                    "candidate_vs_expected_soon15m": {
                        "applies_to": "SOONUSDT 15m only — hint hard-coded for this debug route",
                        "bull_missing_expected_times": _bull_missing,
                        "bull_extra_times":            _bull_extra,
                        "bear_missing_expected_times": _bear_missing,
                        "bear_extra_times":            _bear_extra,
                        "exact_time_match_score":      _score,
                        "exact_time_match_max":        10,
                    },
                })

            _best = max(_variants_global,
                        key=lambda v: v["candidate_vs_expected_soon15m"]["exact_time_match_score"],
                        default=None)
            structure_candidate_global_detail = {
                "variants_compared": _sc_global_run,
                "ob_rules_used": ("production tv_parity_v2 (closed mitigation + "
                                  "bearish effective-bottom overlap + latest_opposite_pivot "
                                  "anchor + last equal-extreme tie)"),
                "structure_candidate_under_test": "varies per variant block below",
                "expected_soon15m_bull_times": _SOON_EXP_BULL,
                "expected_soon15m_bear_times": _SOON_EXP_BEAR,
                "variants": _variants_global,
                "best_match_variant": (_best["variant_name"] if _best else None),
                "best_match_score":   (_best["candidate_vs_expected_soon15m"]
                                       ["exact_time_match_score"] if _best else None),
                "best_match_score_max": 10,
                "note": ("DIAGNOSTIC ONLY — production is unchanged. Volume / OB %% "
                         "fields use the production formula "
                         "floor(source_volume / visible_same_direction_total_volume * 100). "
                         "The hard-coded SOON 15m expected lists are TradingView snapshot "
                         "hints; they may shift as the live candle window advances."),
            }

        # ── Debug-only ob_logic_mode (legacy / v2 / v3) comparison ───────────
        ob_logic_mode_debug_detail = None
        if ob_logic_mode_debug is not None:
            _MODE_FLAGS = {
                "legacy_baseline": dict(mit_closed=False, bear_eff_bottom=False,
                                        anchor_mode="baseline", tie_mode="first",
                                        structure_candidate="current"),
                "tv_parity_v2":    dict(mit_closed=True,  bear_eff_bottom=True,
                                        anchor_mode="latest_opposite_pivot",
                                        tie_mode="last", structure_candidate="current"),
                "tv_parity_v3":    dict(mit_closed=True,  bear_eff_bottom=True,
                                        anchor_mode="latest_opposite_pivot",
                                        tie_mode="last",
                                        structure_candidate="equal_high_pivot_relaxed"),
            }
            _mx = _analyse(main_candles, **_MODE_FLAGS[ob_logic_mode_debug])
            def _logic_pool_summary(pool):
                _rows = []
                for ob in pool:
                    _bar = ob.get("bar"); _src = ob.get("sourceBar")
                    _rows.append({
                        "time":               _ts_i(_bar) if _bar is not None else None,
                        "bottom":             ob.get("bottom"),
                        "top":                ob.get("top"),
                        "avg":                ob.get("avg"),
                        "volume":             ob.get("volume"),
                        "tvObVolumeSharePct": ob.get("tvObVolumeSharePct"),
                        "sourceBar":          _src,
                        "source_time_utc":    (_ts_i(_src) if (_src is not None
                                                                and 0 <= _src < len(times))
                                               else None),
                        "touches":            ob.get("touches"),
                        "isVirgin":           ob.get("isVirgin"),
                        "mitigated":          ob.get("mitigated"),
                    })
                return _rows
            _lb = _logic_pool_summary(_mx["bull_vis"])
            _lr = _logic_pool_summary(_mx["bear_vis"])
            ob_logic_mode_debug_detail = {
                "logic_mode_used":              ob_logic_mode_debug,
                "logic_mode_allowed":           _OB_LOGIC_MODES,
                "production_default_logic_mode": "tv_parity_v3",
                "bullish_visible_pool_summary": _lb,
                "bearish_visible_pool_summary": _lr,
                "bullish_visible_total_volume": _mx["bull_vtot"],
                "bearish_visible_total_volume": _mx["bear_vtot"],
                "counts": {
                    "bullish_source_count":  len(_mx["bull_src"]),
                    "bearish_source_count":  len(_mx["bear_src"]),
                    "bullish_visible_count": len(_mx["bull_vis"]),
                    "bearish_visible_count": len(_mx["bear_vis"]),
                },
                "bull_times":       [x["time"]               for x in _lb],
                "bear_times":       [x["time"]               for x in _lr],
                "bull_pct_list":    [x["tvObVolumeSharePct"] for x in _lb],
                "bear_pct_list":    [x["tvObVolumeSharePct"] for x in _lr],
                "bull_volume_list": [x["volume"]             for x in _lb],
                "bear_volume_list": [x["volume"]             for x in _lr],
            }

        _resp = {
            "ok":                           True,
            "phase":                        "1A",
            "ob_debug_variant":             ob_debug_variant,
            "ob_debug_variant_allowed":     _ALLOWED_VARIANTS,
            "variant_diagnostics":          variant_diagnostics,
            "variant_summary":              variant_summary,
            "ob_touch_meta_enabled":        True,
            "ob_touch_fields":              [
                "touches", "isVirgin", "currentlyInside",
                "firstTouchBar", "lastTouchBar",
                "mitigationBar", "mitigated",
                "untouched", "onceTouched", "touchSeq",
            ],
            "candle_info":                  candle_info,
            "detection_counts":             detection_counts,
            "bullish_source_pool":          bull_source_out,
            "bearish_source_pool":          bear_source_out,
            "bullish_visible_pool":         bull_visible_out,
            "bearish_visible_pool":         bear_visible_out,
            "bullish_visible_total_volume": a["bull_vtot"],
            "bearish_visible_total_volume": a["bear_vtot"],
            "bullish_hidden_pool":          bull_hidden_out,
            "bearish_hidden_pool":          bear_hidden_out,
            "matched_nearest_ob":           matched,
            "limit_comparison":             limit_comparison,
            "bos_trace":                    a.get("bos_trace"),
            "pine_assumptions": {
                "bull_bear_pools_separate": True,
                "show_last_per_direction":  5,
                "hide_overlap":             True,
                "overlap_mode":             "Previous",
                "breaker_included":         False,
                "formula":                  "floor(source_volume / visible_same_direction_total_volume * 100)",
            },
        }

        # Trace keys are added ONLY when trace_ob=true, so the response is
        # byte-identical to the non-trace output when trace_ob is absent.
        if trace_ob:
            _resp["ob_trace_detail"] = ob_trace_detail
            _resp["trace_summary"]   = trace_summary

        # Anchor-variant keys are added ONLY when ob_anchor_variant is passed,
        # so the response is byte-identical when the param is absent.
        if _anchors_to_run is not None:
            _resp["ob_anchor_variant"]          = ob_anchor_variant
            _resp["ob_anchor_variant_allowed"]  = ["baseline", "latest_opposite_pivot", "all"]
            _resp["anchor_variant_diagnostics"] = anchor_variant_diagnostics

        # Extreme-tie keys are added ONLY when ob_extreme_tie_mode is passed,
        # so the response is byte-identical when the param is absent.
        if ob_extreme_tie_mode is not None:
            _resp["ob_extreme_tie_mode"]         = ob_extreme_tie_mode
            _resp["ob_extreme_tie_mode_allowed"] = ["first", "last", "all"]
            _resp["extreme_tie_diagnostics"]     = extreme_tie_diagnostics
            _resp["tv_parity_candidate_v2"]      = tv_parity_candidate_v2

        # Structure-trace key is added ONLY when structure_trace=true,
        # so the response is byte-identical when the param is absent.
        if structure_trace:
            _resp["structure_trace_detail"] = structure_trace_detail

        # Structure-lifecycle keys added ONLY when structure_lifecycle_trace=true.
        if structure_lifecycle_trace:
            _resp["structure_lifecycle_trace_detail"] = structure_lifecycle_trace_detail
            _resp["lifecycle_trace_summary"]          = lifecycle_trace_summary

        # Structure-candidate key added ONLY when structure_candidate_trace=true.
        if structure_candidate_trace:
            _resp["structure_candidate_trace_detail"] = structure_candidate_trace_detail

        # Structure-candidate GLOBAL key added ONLY when structure_candidate_global=true.
        if structure_candidate_global:
            _resp["structure_candidate_global_detail"] = structure_candidate_global_detail

        # ob_logic_mode debug key added ONLY when ob_logic_mode query param is passed.
        if ob_logic_mode_debug is not None:
            _resp["ob_logic_mode_debug_detail"] = ob_logic_mode_debug_detail

        # ── debug_profile response shaping (default "full" = unchanged) ──────
        if debug_profile == "lifecycle_only":
            _sctd = _resp.get("structure_candidate_trace_detail")
            _resp = {
                "ok":                               _resp["ok"],
                "debug_profile":                    "lifecycle_only",
                "candle_info":                      _resp["candle_info"],
                "production_ob_logic_mode":         "tv_parity_v2",
                "structure_lifecycle_trace_detail": _resp.get("structure_lifecycle_trace_detail"),
                "lifecycle_trace_summary":          _resp.get("lifecycle_trace_summary"),
                # Candidate-trace fields work independently — included when
                # structure_candidate_trace=true, regardless of lifecycle trace.
                "structure_candidate_trace_detail":  _sctd,
                "candidate_variant_summary":         (_sctd.get("candidate_variant_summary")
                                                      if _sctd else None),
                "structure_candidate_global_detail": _resp.get("structure_candidate_global_detail"),
                "ob_logic_mode_debug_detail":        _resp.get("ob_logic_mode_debug_detail"),
            }
        elif debug_profile == "compact":
            def _vis_summary(pool):
                return [{"time": ob.get("time"), "top": ob.get("top"),
                         "bottom": ob.get("bottom"), "volume": ob.get("volume"),
                         "tvObVolumeSharePct": ob.get("tvObVolumeSharePct")}
                        for ob in (pool or [])]
            _compact = {
                "ok":                           _resp["ok"],
                "debug_profile":                "compact",
                "candle_info":                  _resp["candle_info"],
                "detection_counts":             _resp["detection_counts"],
                "bullish_visible_pool_summary": _vis_summary(_resp.get("bullish_visible_pool")),
                "bearish_visible_pool_summary": _vis_summary(_resp.get("bearish_visible_pool")),
                "bullish_visible_total_volume": _resp.get("bullish_visible_total_volume"),
                "bearish_visible_total_volume": _resp.get("bearish_visible_total_volume"),
            }
            # Requested trace blocks only.
            for _k in ("ob_trace_detail", "trace_summary", "structure_trace_detail",
                       "structure_lifecycle_trace_detail", "lifecycle_trace_summary",
                       "structure_candidate_trace_detail",
                       "structure_candidate_global_detail",
                       "ob_logic_mode_debug_detail"):
                if _k in _resp:
                    _compact[_k] = _resp[_k]
            # variant_summary only when ob_debug_variant was explicitly passed.
            if _variant_explicitly_requested:
                _compact["variant_summary"] = _resp.get("variant_summary")
            _resp = _compact

        return jsonify(_resp)

    except Exception as _e:
        import traceback
        return jsonify({"ok": False, "error": str(_e),
                        "traceback": traceback.format_exc()}), 500


# ── Bot / Suspicious Account Cleanup ───────────────────────────────────────────
#
# GET  /admin/security/bots          → identify suspicious accounts (dry-run, read-only)
# POST /admin/security/bots/delete   → delete the listed account IDs after confirmation
#
# Criteria for "suspicious":
#   1. email_verified = False  AND  created_at older than 30 minutes (never verified)
#   2. role = "user", status = "active", never logged in (last_login_at IS NULL),
#      AND created_at older than 24 hours
#   3. Guest accounts with role="user" created in bulk from the same creation window
#      (≥ 5 accounts created within the same 5-minute window)
# ─────────────────────────────────────────────────────────────────────────────

@admin_bp.route("/security/bots", methods=["GET"])
@admin_required
def security_bot_scan():
    """Return a JSON list of suspicious (likely bot-created) accounts."""
    from datetime import timedelta
    now = datetime.now(timezone.utc)

    try:
        # Criterion 1: unverified accounts older than 30 minutes
        cutoff_unverified = now - timedelta(minutes=30)
        unverified = (
            User.query
            .filter_by(email_verified=False, role="user")
            .filter(User.created_at < cutoff_unverified)
            .all()
        )

        # Criterion 2: never logged in AND account older than 24 hours
        cutoff_stale = now - timedelta(hours=24)
        never_logged_in = (
            User.query
            .filter_by(role="user", status="active")
            .filter(User.last_login_at.is_(None))
            .filter(User.created_at < cutoff_stale)
            .all()
        )

        # Merge, deduplicate by id, exclude admins
        seen = set()
        suspicious = []
        for u in unverified + never_logged_in:
            if u.id not in seen and u.role != "admin":
                seen.add(u.id)
                suspicious.append({
                    "id":             u.id,
                    "username":       u.username,
                    "email":          u.email or "",
                    "role":           u.role,
                    "status":         u.status,
                    "email_verified": u.email_verified,
                    "created_at":     u.created_at.isoformat() if u.created_at else None,
                    "last_login_at":  u.last_login_at.isoformat() if u.last_login_at else None,
                    "reason": (
                        "never_verified" if not u.email_verified else "never_logged_in"
                    ),
                })

        return jsonify({
            "ok":   True,
            "count": len(suspicious),
            "accounts": suspicious,
        })

    except Exception as _e:
        return jsonify({"ok": False, "error": str(_e)}), 500


@admin_bp.route("/security/bots/delete", methods=["POST"])
@admin_required
def security_bot_delete():
    """
    Permanently delete the supplied list of user IDs.
    The caller must explicitly pass the IDs to delete — no wildcard deletes.
    Admins are always excluded regardless of what IDs are supplied.
    """
    data = request.get_json(force=True) or {}
    ids_to_delete = data.get("ids", [])

    if not ids_to_delete or not isinstance(ids_to_delete, list):
        return jsonify({"error": "Provide a non-empty 'ids' list."}), 400

    # Safety: never delete more than 500 at a time
    if len(ids_to_delete) > 500:
        return jsonify({"error": "Batch size too large. Max 500 per request."}), 400

    try:
        # Load only non-admin users from the supplied IDs
        targets = (
            User.query
            .filter(User.id.in_(ids_to_delete))
            .filter(User.role != "admin")
            .all()
        )
        if not targets:
            return jsonify({"ok": True, "deleted": 0, "message": "No eligible users found."})

        deleted_usernames = [u.username for u in targets]
        for u in targets:
            # Remove related records to avoid FK violations
            EmailVerification.query.filter_by(user_id=u.id).delete()
            LoginHistory.query.filter_by(user_id=u.id).delete()
            db.session.delete(u)

        db.session.commit()
        _log_action(
            "bot_cleanup",
            f"Deleted {len(deleted_usernames)} suspicious accounts: {', '.join(deleted_usernames[:20])}",
        )
        return jsonify({
            "ok":      True,
            "deleted": len(deleted_usernames),
            "usernames": deleted_usernames,
        })

    except Exception as _e:
        db.session.rollback()
        return jsonify({"error": str(_e)}), 500


# ── Purge ALL non-admin users (one-shot cleanup) ───────────────────────────────
# POST /admin/security/purge-non-admins
# Requires JSON body: {"confirm": "DELETE ALL NON ADMIN USERS"}
# Admin accounts are NEVER touched regardless of role.
# ─────────────────────────────────────────────────────────────────────────────

@admin_bp.route("/security/purge-non-admins", methods=["POST"])
def security_purge_non_admins():
    """Delete every user whose role is not 'admin'. Requires explicit confirmation."""
    # Accept either Flask-Login session (admin panel) or main-app session (session["is_admin"])
    authed = (current_user.is_authenticated and current_user.is_admin) or session.get("is_admin")
    if not authed:
        return jsonify({"error": "Unauthorized. Log in as admin first."}), 403

    data = request.get_json(force=True) or {}
    if data.get("confirm") != "DELETE ALL NON ADMIN USERS":
        return jsonify({
            "error": "Missing confirmation. Send JSON: {\"confirm\": \"DELETE ALL NON ADMIN USERS\"}"
        }), 400

    try:
        targets = User.query.filter(User.role != "admin").all()
        count   = len(targets)
        if count == 0:
            return jsonify({"ok": True, "deleted": 0, "message": "No non-admin users found."})

        sample = [u.username for u in targets[:30]]
        for u in targets:
            EmailVerification.query.filter_by(user_id=u.id).delete()
            LoginHistory.query.filter_by(user_id=u.id).delete()
            DailyTokenUsage.query.filter_by(user_id=u.id).delete()
            GuestDevice.query.filter_by(user_id=u.id).delete()
            db.session.delete(u)

        db.session.commit()
        try:
            admin_id = current_user.id if current_user.is_authenticated else None
            if admin_id:
                log = AdminLog(admin_id=admin_id, action="purge_non_admins",
                               details=f"Deleted ALL {count} non-admin users. Sample: {', '.join(sample)}")
                db.session.add(log)
                db.session.commit()
        except Exception:
            pass
        return jsonify({"ok": True, "deleted": count, "sample": sample})

    except Exception as _e:
        db.session.rollback()
        return jsonify({"error": str(_e)}), 500
