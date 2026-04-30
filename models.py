from flask_sqlalchemy import SQLAlchemy
from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime, timezone

db = SQLAlchemy()

ALL_MODULES     = ["ob", "fvg", "bb", "fib"]
ALL_TABS        = ["scan", "pairs", "settings", "compressed", "trending", "athatl", "bias", "watchlist"]
ALL_EXCHANGES   = ["binance", "bybit", "okx", "mexc"]
ALL_TIMEFRAMES  = ["15m", "30m", "1h", "4h", "1d"]


class User(UserMixin, db.Model):
    __tablename__ = "users"

    id            = db.Column(db.Integer, primary_key=True)
    username      = db.Column(db.String(50), unique=True, nullable=False)
    email         = db.Column(db.String(120), nullable=True)
    password_hash = db.Column(db.String(256), nullable=False)
    role          = db.Column(db.String(20), default="user", nullable=False)
    status        = db.Column(db.String(20), default="active", nullable=False)
    created_at    = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    last_login_at = db.Column(db.DateTime, nullable=True)
    last_login_ip = db.Column(db.String(45), nullable=True)
    notes         = db.Column(db.Text, nullable=True)

    def set_password(self, password: str) -> None:
        self.password_hash = generate_password_hash(password)

    def check_password(self, password: str) -> bool:
        return check_password_hash(self.password_hash, password)

    @property
    def is_admin(self) -> bool:
        return self.role == "admin"

    def __repr__(self) -> str:
        return f"<User {self.username} [{self.role}]>"


class AdminLog(db.Model):
    __tablename__ = "admin_logs"

    id             = db.Column(db.Integer, primary_key=True)
    admin_id       = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    action         = db.Column(db.String(100), nullable=False)
    target_user_id = db.Column(db.Integer, nullable=True)
    details        = db.Column(db.Text, nullable=True)
    ip_address     = db.Column(db.String(45), nullable=True)
    created_at     = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    admin = db.relationship("User", foreign_keys=[admin_id])

    def __repr__(self) -> str:
        return f"<AdminLog {self.action} by admin_id={self.admin_id}>"


class GlobalSetting(db.Model):
    __tablename__ = "global_settings"

    id          = db.Column(db.Integer, primary_key=True)
    key         = db.Column(db.String(100), unique=True, nullable=False)
    value       = db.Column(db.Text, nullable=True)
    description = db.Column(db.String(255), nullable=True)
    updated_at  = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc),
                             onupdate=lambda: datetime.now(timezone.utc))
    updated_by  = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)

    def __repr__(self) -> str:
        return f"<GlobalSetting {self.key}={self.value}>"


class RolePermission(db.Model):
    __tablename__ = "role_permissions"

    id                  = db.Column(db.Integer, primary_key=True)
    role                = db.Column(db.String(20), unique=True, nullable=False)
    daily_tokens        = db.Column(db.Integer, default=500)
    max_pairs_per_scan  = db.Column(db.Integer, default=100)
    max_pairs_per_cycle = db.Column(db.Integer, default=50)
    allowed_modules     = db.Column(db.Text, nullable=True)  # JSON list
    allowed_tabs        = db.Column(db.Text, nullable=True)   # JSON list
    allowed_exchanges   = db.Column(db.Text, nullable=True)   # JSON list
    allowed_timeframes  = db.Column(db.Text, nullable=True)   # JSON list
    updated_at          = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc),
                                    onupdate=lambda: datetime.now(timezone.utc))
    updated_by          = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)

    def __repr__(self) -> str:
        return f"<RolePermission {self.role}>"


class UserPermission(db.Model):
    __tablename__ = "user_permissions"

    id                  = db.Column(db.Integer, primary_key=True)
    user_id             = db.Column(db.Integer, db.ForeignKey("users.id"), unique=True, nullable=False)
    daily_tokens        = db.Column(db.Integer, nullable=True)
    max_pairs_per_scan  = db.Column(db.Integer, nullable=True)
    max_pairs_per_cycle = db.Column(db.Integer, nullable=True)
    allowed_modules     = db.Column(db.Text, nullable=True)
    allowed_tabs        = db.Column(db.Text, nullable=True)
    allowed_exchanges   = db.Column(db.Text, nullable=True)
    allowed_timeframes  = db.Column(db.Text, nullable=True)
    notes               = db.Column(db.Text, nullable=True)
    updated_at          = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc),
                                    onupdate=lambda: datetime.now(timezone.utc))
    updated_by          = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)

    user = db.relationship("User", foreign_keys=[user_id])

    def __repr__(self) -> str:
        return f"<UserPermission user_id={self.user_id}>"


class DailyTokenUsage(db.Model):
    __tablename__ = "daily_token_usage"

    id           = db.Column(db.Integer, primary_key=True)
    user_id      = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    date         = db.Column(db.Date, nullable=False)
    tokens_used  = db.Column(db.Integer, default=0)
    scan_count   = db.Column(db.Integer, default=0)
    last_scan_at = db.Column(db.DateTime, nullable=True)

    __table_args__ = (db.UniqueConstraint("user_id", "date", name="uq_user_date"),)

    def __repr__(self) -> str:
        return f"<DailyTokenUsage user_id={self.user_id} date={self.date}>"


class GuestDevice(db.Model):
    __tablename__ = "guest_devices"

    id                 = db.Column(db.Integer, primary_key=True)
    device_fingerprint = db.Column(db.String(255), unique=True, nullable=False)
    user_id            = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    first_seen_at      = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    last_seen_at       = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    ip_address         = db.Column(db.String(45), nullable=True)
    user_agent         = db.Column(db.Text, nullable=True)

    user = db.relationship("User", foreign_keys=[user_id])

    def __repr__(self) -> str:
        return f"<GuestDevice fp={self.device_fingerprint[:12]}…>"


class LoginHistory(db.Model):
    __tablename__ = "login_history"

    id               = db.Column(db.Integer, primary_key=True)
    user_id          = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    logged_in_at     = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    ip_address       = db.Column(db.String(45), nullable=True)
    user_agent       = db.Column(db.Text, nullable=True)
    country          = db.Column(db.String(100), nullable=True)
    city             = db.Column(db.String(100), nullable=True)
    device_type      = db.Column(db.String(20), nullable=True)
    browser          = db.Column(db.String(100), nullable=True)
    os               = db.Column(db.String(100), nullable=True)
    session_duration = db.Column(db.Integer, nullable=True)  # minutes

    user = db.relationship("User", foreign_keys=[user_id])

    def __repr__(self) -> str:
        return f"<LoginHistory user_id={self.user_id} at={self.logged_in_at}>"


# ─────────────────────────────────────────────────────────────
# Intelligence Foundation Tables
# ─────────────────────────────────────────────────────────────

class SignalEvent(db.Model):
    __tablename__ = "signal_events"

    id             = db.Column(db.Integer, primary_key=True)
    signal_id      = db.Column(db.String(64), unique=True, nullable=False)
    pair           = db.Column(db.String(20), nullable=False, index=True)
    module         = db.Column(db.String(20), nullable=False, index=True)
    timeframe      = db.Column(db.String(10), nullable=False, index=True)
    direction      = db.Column(db.String(10), nullable=False)
    score          = db.Column(db.Integer, default=0)
    zone_high      = db.Column(db.Float, nullable=False)
    zone_low       = db.Column(db.Float, nullable=False)
    detected_price = db.Column(db.Float, nullable=False)
    detected_at    = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), index=True)
    exchange       = db.Column(db.String(20), default="binance")
    strategy_ver   = db.Column(db.String(10), default="1.0")
    settings_json  = db.Column(db.Text, nullable=True)
    status         = db.Column(db.String(30), default="WAITING_FOR_ENTRY", index=True)
    source         = db.Column(db.String(20), default="live")
    setup_type     = db.Column(db.String(30), nullable=True)
    raw_setup      = db.Column(db.String(50), nullable=True)
    raw_meta_json  = db.Column(db.Text, nullable=True)

    outcome = db.relationship("SignalOutcome", uselist=False, backref="event",
                              foreign_keys="SignalOutcome.signal_id",
                              primaryjoin="SignalEvent.signal_id == SignalOutcome.signal_id")

    def __repr__(self) -> str:
        return f"<SignalEvent {self.signal_id} {self.pair} {self.module} {self.status}>"


class SignalOutcome(db.Model):
    __tablename__ = "signal_outcomes"

    id                   = db.Column(db.Integer, primary_key=True)
    signal_id            = db.Column(db.String(64), db.ForeignKey("signal_events.signal_id"),
                                     unique=True, nullable=False)
    entry_price          = db.Column(db.Float, nullable=True)
    entry_time           = db.Column(db.DateTime, nullable=True)
    target_price         = db.Column(db.Float, nullable=True)
    stop_price           = db.Column(db.Float, nullable=True)
    exit_price           = db.Column(db.Float, nullable=True)
    exit_time            = db.Column(db.DateTime, nullable=True)
    result               = db.Column(db.String(20), nullable=True)
    result_reason        = db.Column(db.String(50), nullable=True)
    mfe_pct              = db.Column(db.Float, nullable=True)
    mae_pct              = db.Column(db.Float, nullable=True)
    time_to_entry_hours  = db.Column(db.Float, nullable=True)
    time_to_result_hours = db.Column(db.Float, nullable=True)
    bounce_threshold_pct = db.Column(db.Float, nullable=True)

    def __repr__(self) -> str:
        return f"<SignalOutcome {self.signal_id} result={self.result}>"


class BacktestRun(db.Model):
    __tablename__ = "backtest_runs"

    id               = db.Column(db.Integer, primary_key=True)
    run_name         = db.Column(db.String(100), nullable=True)
    config_json      = db.Column(db.Text, nullable=True)
    run_at           = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    completed_at     = db.Column(db.DateTime, nullable=True)
    status           = db.Column(db.String(20), default="running")
    pairs_tested     = db.Column(db.Integer, default=0)
    total_signals    = db.Column(db.Integer, default=0)
    entered_signals  = db.Column(db.Integer, default=0)
    won_count        = db.Column(db.Integer, default=0)
    lost_count       = db.Column(db.Integer, default=0)
    expired_count    = db.Column(db.Integer, default=0)
    ambiguous_count  = db.Column(db.Integer, default=0)
    win_rate_entered = db.Column(db.Float, nullable=True)
    win_rate_total   = db.Column(db.Float, nullable=True)
    date_from        = db.Column(db.Date, nullable=True)
    date_to          = db.Column(db.Date, nullable=True)
    run_by           = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)
    error_message    = db.Column(db.Text, nullable=True)

    runner = db.relationship("User", foreign_keys=[run_by])

    def __repr__(self) -> str:
        return f"<BacktestRun id={self.id} status={self.status}>"


class IntelligenceSettings(db.Model):
    """Singleton settings row (id=1) for the auto-resolver. Created on first GET."""
    __tablename__ = "intelligence_settings"

    id                             = db.Column(db.Integer,    primary_key=True)
    auto_resolver_enabled          = db.Column(db.Boolean,    default=False,     nullable=False)
    auto_resolver_interval_minutes = db.Column(db.Integer,    default=30,        nullable=False)
    auto_resolver_limit            = db.Column(db.Integer,    default=20,        nullable=False)
    auto_resolver_mode             = db.Column(db.String(20), default="dry_run", nullable=False)
    runner_installed               = db.Column(db.Boolean,    default=False,     nullable=False)
    last_saved_at                  = db.Column(db.DateTime,   nullable=True)
    last_saved_by                  = db.Column(db.Integer,    db.ForeignKey("users.id"), nullable=True)
    last_run_at                    = db.Column(db.DateTime,   nullable=True)
    last_run_summary               = db.Column(db.Text,       nullable=True)
    created_at                     = db.Column(db.DateTime,   default=lambda: datetime.now(timezone.utc))
    updated_at                     = db.Column(db.DateTime,   default=lambda: datetime.now(timezone.utc),
                                               onupdate=lambda: datetime.now(timezone.utc))

    def __repr__(self) -> str:
        return f"<IntelligenceSettings enabled={self.auto_resolver_enabled} mode={self.auto_resolver_mode}>"
