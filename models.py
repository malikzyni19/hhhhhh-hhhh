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

    id             = db.Column(db.Integer, primary_key=True)
    username       = db.Column(db.String(50), unique=True, nullable=False)
    email          = db.Column(db.String(120), nullable=True)
    password_hash  = db.Column(db.String(256), nullable=False)
    role           = db.Column(db.String(20), default="user", nullable=False)
    status         = db.Column(db.String(20), default="active", nullable=False)
    email_verified = db.Column(db.Boolean, default=False, nullable=False, server_default="false")
    created_at     = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    last_login_at  = db.Column(db.DateTime, nullable=True)
    last_login_ip  = db.Column(db.String(45), nullable=True)
    notes          = db.Column(db.Text, nullable=True)

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


class EmailVerification(db.Model):
    __tablename__ = "email_verifications"

    id         = db.Column(db.Integer, primary_key=True)
    user_id    = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    code       = db.Column(db.String(6), nullable=False)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    expires_at = db.Column(db.DateTime, nullable=False)
    used       = db.Column(db.Boolean, default=False, nullable=False)

    user = db.relationship("User", foreign_keys=[user_id])

    def __repr__(self) -> str:
        return f"<EmailVerification user_id={self.user_id} used={self.used}>"


class PasswordResetToken(db.Model):
    __tablename__ = "password_reset_tokens"

    id         = db.Column(db.Integer, primary_key=True)
    user_id    = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    token_hash = db.Column(db.String(64), unique=True, nullable=False)  # SHA-256 hex of raw token
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    expires_at = db.Column(db.DateTime, nullable=False)
    used       = db.Column(db.Boolean, default=False, nullable=False)

    user = db.relationship("User", foreign_keys=[user_id])

    def __repr__(self) -> str:
        return f"<PasswordResetToken user_id={self.user_id} used={self.used}>"


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


class ScanPreset(db.Model):
    """Per-user saved scanner configurations (Queue 15)."""
    __tablename__ = "scan_presets"

    id         = db.Column(db.Integer, primary_key=True)
    user_id    = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False, index=True)
    name       = db.Column(db.String(80), nullable=False)
    payload    = db.Column(db.Text, nullable=False)             # JSON blob — controls snapshot
    is_default = db.Column(db.Boolean, default=False, nullable=False, server_default="false")
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc),
                            onupdate=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        db.UniqueConstraint("user_id", "name", name="uq_scan_preset_user_name"),
    )

    def __repr__(self) -> str:
        return f"<ScanPreset {self.name} user={self.user_id}>"


class UserPreference(db.Model):
    """Per-user UI preferences — desktop tutorial state etc. (Queue 16)."""
    __tablename__ = "user_preferences"

    id      = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), unique=True, nullable=False, index=True)

    desktop_tutorial_never_show  = db.Column(db.Boolean, default=False, nullable=False, server_default="false")
    desktop_tutorial_completed_at = db.Column(db.DateTime, nullable=True)
    desktop_tutorial_skipped_at   = db.Column(db.DateTime, nullable=True)

    # Phase 10.8: OB Distance/Approach settings JSON (per-user, persistent)
    ob_da_settings_json = db.Column(db.Text, nullable=True)

    updated_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc),
                            onupdate=lambda: datetime.now(timezone.utc))

    def __repr__(self) -> str:
        return f"<UserPreference user={self.user_id} tutorial_off={self.desktop_tutorial_never_show}>"


# ─────────────────────────────────────────────────────────────
# Live Monitor — Phase 1
# ─────────────────────────────────────────────────────────────

class LiveMonitorItem(db.Model):
    """Per-user Live Monitor items — full setup snapshot saved from any scanner tab."""
    __tablename__ = "live_monitor_items"

    id                  = db.Column(db.Integer, primary_key=True)
    user_id             = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False, index=True)
    symbol              = db.Column(db.String(20), nullable=False, index=True)
    exchange            = db.Column(db.String(20), default="binance", nullable=False)
    market              = db.Column(db.String(20), default="perpetual", nullable=False)
    source_tab          = db.Column(db.String(40), default="unknown", nullable=False)
    setup_type          = db.Column(db.String(40), nullable=True)
    direction           = db.Column(db.String(10), nullable=True)
    timeframe           = db.Column(db.String(10), nullable=True)
    zone_high           = db.Column(db.Float, nullable=True)
    zone_low            = db.Column(db.Float, nullable=True)
    confidence          = db.Column(db.Integer, default=0)
    score               = db.Column(db.Integer, default=0)
    current_price       = db.Column(db.Float, nullable=True)
    status              = db.Column(db.String(20), default="watching", nullable=False)
    snapshot_json       = db.Column(db.Text, nullable=True)          # full topAlert/meta JSON blob
    selected_timeframes = db.Column(db.Text, nullable=True)          # JSON list  e.g. ["15m","1h","4h"]
    selected_modules    = db.Column(db.Text, nullable=True)          # JSON list  e.g. ["OB","FVG","FIB","Bias"]
    alert_settings_json = db.Column(db.Text, nullable=True)          # JSON dict for future alert config
    is_active           = db.Column(db.Boolean, default=True, nullable=False, server_default="true")
    added_at            = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), index=True)
    updated_at          = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc),
                                    onupdate=lambda: datetime.now(timezone.utc))

    user = db.relationship("User", foreign_keys=[user_id])

    def __repr__(self) -> str:
        return f"<LiveMonitorItem {self.symbol} {self.setup_type} user={self.user_id}>"


class LiveMonitorEvent(db.Model):
    """Events logged against a LiveMonitorItem (added, removed, zone_tap, etc.)."""
    __tablename__ = "live_monitor_events"

    id                    = db.Column(db.Integer, primary_key=True)
    item_id               = db.Column(db.Integer, db.ForeignKey("live_monitor_items.id"), nullable=False, index=True)
    user_id               = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False, index=True)
    symbol                = db.Column(db.String(20), nullable=True)
    event_type            = db.Column(db.String(40), nullable=False)
    event_description     = db.Column(db.String(255), nullable=True)
    details_json          = db.Column(db.Text, nullable=True)
    health_score_at_event = db.Column(db.Integer, nullable=True)
    price_at_event        = db.Column(db.Float, nullable=True)
    created_at            = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), index=True)

    item = db.relationship("LiveMonitorItem", foreign_keys=[item_id])
    user = db.relationship("User", foreign_keys=[user_id])

    def __repr__(self) -> str:
        return f"<LiveMonitorEvent {self.event_type} item={self.item_id}>"


class LiveMonitorTrade(db.Model):
    """AI trade proposals and their risk / execution status. Phase 9.3.

    Phase 9.3-9.5 only uses statuses: draft, proposed, risk_approved,
    risk_rejected, cancelled.
    Execution statuses (submitted/open/closed/failed) are Phase 9.6+.
    Full trade records live here — NOT in snapshot_json.
    snapshot_json may only reference active_trade_id / latest_trade_summary.
    """
    __tablename__ = "live_monitor_trades"

    id                     = db.Column(db.Integer, primary_key=True)
    trade_uid              = db.Column(db.String(40), unique=True, nullable=False, index=True)
    user_id                = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False, index=True)
    live_monitor_item_id   = db.Column(db.Integer, db.ForeignKey("live_monitor_items.id"), nullable=True, index=True)
    linked_memory_record_id = db.Column(db.String(64), nullable=True)
    mode                   = db.Column(db.String(20), default="proposal_only", nullable=False)
    execution_exchange     = db.Column(db.String(20), default="none", nullable=False)
    execution_market       = db.Column(db.String(20), default="perpetual", nullable=False)
    symbol                 = db.Column(db.String(20), nullable=False, index=True)
    direction              = db.Column(db.String(10), nullable=True)
    setup_type             = db.Column(db.String(40), nullable=True)
    timeframe              = db.Column(db.String(10), nullable=True)
    status                 = db.Column(db.String(20), default="draft", nullable=False, index=True)
    entry_price            = db.Column(db.Float, nullable=True)
    stop_loss              = db.Column(db.Float, nullable=True)
    take_profit            = db.Column(db.Float, nullable=True)
    risk_reward            = db.Column(db.Float, nullable=True)
    position_size          = db.Column(db.Float, nullable=True)
    leverage               = db.Column(db.Float, nullable=True)
    ai_proposal_json       = db.Column(db.Text, nullable=True)
    ai_reasoning_summary   = db.Column(db.Text, nullable=True)
    setup_context_json     = db.Column(db.Text, nullable=True)
    risk_guard_json        = db.Column(db.Text, nullable=True)
    risk_guard_status      = db.Column(db.String(20), default="not_checked", nullable=False)
    rejection_reason       = db.Column(db.Text, nullable=True)
    exchange_order_id      = db.Column(db.String(80), nullable=True)
    exchange_position_id   = db.Column(db.String(80), nullable=True)
    opened_at              = db.Column(db.DateTime, nullable=True)
    closed_at              = db.Column(db.DateTime, nullable=True)
    pnl                    = db.Column(db.Float, nullable=True)
    fees                   = db.Column(db.Float, nullable=True)
    outcome                = db.Column(db.String(20), nullable=True)
    post_trade_review_json = db.Column(db.Text, nullable=True)
    created_at             = db.Column(db.DateTime,
                                       default=lambda: datetime.now(timezone.utc),
                                       nullable=False, index=True)
    updated_at             = db.Column(db.DateTime,
                                       default=lambda: datetime.now(timezone.utc),
                                       onupdate=lambda: datetime.now(timezone.utc))

    user = db.relationship("User", foreign_keys=[user_id])
    item = db.relationship("LiveMonitorItem", foreign_keys=[live_monitor_item_id])

    def __repr__(self) -> str:
        return f"<LiveMonitorTrade {self.trade_uid} {self.symbol} {self.status}>"


class LiveMonitorChatMessage(db.Model):
    """Persistent AI chat messages per user + symbol + exchange. Phase 10.5.

    Messages are stored per user + symbol + exchange so history survives
    browser refresh and persists across sessions. Latest 100 messages are
    returned per user+symbol+exchange pair.
    No API keys, secrets, or raw candles stored here.
    """
    __tablename__ = "live_monitor_chat_messages"

    id                   = db.Column(db.Integer, primary_key=True)
    user_id              = db.Column(db.Integer, db.ForeignKey("users.id"),
                                     nullable=False, index=True)
    live_monitor_item_id = db.Column(db.Integer,
                                     db.ForeignKey("live_monitor_items.id"),
                                     nullable=True, index=True)
    symbol               = db.Column(db.String(20), nullable=False, index=True)
    exchange             = db.Column(db.String(20), nullable=False, default="binance")
    role                 = db.Column(db.String(20), nullable=False)   # user / assistant / system
    content              = db.Column(db.Text, nullable=False)
    agent_id             = db.Column(db.String(80), nullable=True)
    agent_label          = db.Column(db.String(80), nullable=True)
    metadata_json        = db.Column(db.Text, nullable=True)
    created_at           = db.Column(db.DateTime,
                                     default=lambda: datetime.now(timezone.utc),
                                     nullable=False, index=True)

    user = db.relationship("User", foreign_keys=[user_id])

    def __repr__(self) -> str:
        return f"<LiveMonitorChatMessage {self.role} {self.symbol} user={self.user_id}>"


class LiveMonitorCandle(db.Model):
    """Phase 10.9B: MTF candle history for Bias Shift Watch items.

    Rows are shared by user+exchange+market+symbol+timeframe — multiple LM
    items for the same symbol reuse the same candle rows. Upsert on the
    unique constraint prevents duplicates on repeated refreshes.

    No raw candle arrays are stored in LiveMonitorItem.snapshot_json;
    only lightweight status metadata goes there.
    No API keys, no order logic, no trading execution.
    """
    __tablename__ = "live_monitor_candles"

    id              = db.Column(db.Integer, primary_key=True)
    user_id         = db.Column(db.Integer, db.ForeignKey("users.id"),
                                nullable=False, index=True)
    exchange        = db.Column(db.String(20), nullable=False)
    market          = db.Column(db.String(20), nullable=False, default="perpetual")
    symbol          = db.Column(db.String(20), nullable=False)
    timeframe       = db.Column(db.String(10), nullable=False)
    open_time       = db.Column(db.BigInteger, nullable=False)   # ms epoch
    open            = db.Column(db.Float,      nullable=False)
    high            = db.Column(db.Float,      nullable=False)
    low             = db.Column(db.Float,      nullable=False)
    close           = db.Column(db.Float,      nullable=False)
    volume          = db.Column(db.Float,      nullable=True)
    close_time      = db.Column(db.BigInteger, nullable=True)    # ms epoch
    quote_volume    = db.Column(db.Float,      nullable=True)
    trade_count     = db.Column(db.Integer,    nullable=True)
    taker_buy_base  = db.Column(db.Float,      nullable=True)
    taker_buy_quote = db.Column(db.Float,      nullable=True)
    raw_json        = db.Column(db.Text,       nullable=True)
    created_at      = db.Column(db.DateTime,
                                default=lambda: datetime.now(timezone.utc))
    updated_at      = db.Column(db.DateTime,
                                default=lambda: datetime.now(timezone.utc),
                                onupdate=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        db.UniqueConstraint(
            "user_id", "exchange", "market", "symbol", "timeframe", "open_time",
            name="uq_lm_candle_key",
        ),
        db.Index("ix_lm_candle_lookup",
                 "user_id", "exchange", "market", "symbol", "timeframe", "open_time"),
    )

    user = db.relationship("User", foreign_keys=[user_id])

    def __repr__(self) -> str:
        return (f"<LiveMonitorCandle {self.symbol} {self.timeframe} "
                f"open_time={self.open_time} user={self.user_id}>")


class LiveMonitorCandleFeature(db.Model):
    """Phase 10.9C: Per-candle pattern feature rows for Bias Shift Watch items.

    One row per (user_id, exchange, market, symbol, timeframe, open_time).
    Upsert on uq_lm_cfeature_key prevents duplicates on repeated feature runs.

    No candle arrays in snapshot_json; only lightweight status goes there.
    No trading execution. No order placement. No private API.
    No BOS/CHoCH engine. No order-flow engine. No S/R Flip logic.
    """
    __tablename__ = "live_monitor_candle_features"

    id                    = db.Column(db.Integer, primary_key=True)
    user_id               = db.Column(db.Integer, db.ForeignKey("users.id"),
                                      nullable=False, index=True)
    exchange              = db.Column(db.String(20), nullable=False)
    market                = db.Column(db.String(20), nullable=False, default="perpetual")
    symbol                = db.Column(db.String(20), nullable=False)
    timeframe             = db.Column(db.String(10), nullable=False)
    open_time             = db.Column(db.BigInteger, nullable=False)   # ms epoch

    # Raw candle snapshot (stored compactly — not a huge array)
    candle_open           = db.Column(db.Float, nullable=False)
    candle_high           = db.Column(db.Float, nullable=False)
    candle_low            = db.Column(db.Float, nullable=False)
    candle_close          = db.Column(db.Float, nullable=False)
    candle_volume         = db.Column(db.Float, nullable=True)

    # Computed candle math
    body_pct              = db.Column(db.Float, nullable=True)   # 0-100
    upper_wick_pct        = db.Column(db.Float, nullable=True)   # 0-100
    lower_wick_pct        = db.Column(db.Float, nullable=True)   # 0-100
    close_position_pct    = db.Column(db.Float, nullable=True)   # 0=at low, 100=at high
    candle_direction      = db.Column(db.String(10), nullable=True)  # bullish/bearish/neutral

    # Detected patterns and summary (compact JSON)
    detected_patterns_json = db.Column(db.Text, nullable=True)   # list of pattern name strings
    feature_summary_json   = db.Column(db.Text, nullable=True)   # {body_pct, wicks, patterns, ...}

    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc),
                           onupdate=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        db.UniqueConstraint(
            "user_id", "exchange", "market", "symbol", "timeframe", "open_time",
            name="uq_lm_cfeature_key",
        ),
        db.Index("ix_lm_cfeature_lookup",
                 "user_id", "exchange", "market", "symbol", "timeframe", "open_time"),
    )

    user = db.relationship("User", foreign_keys=[user_id])

    def __repr__(self) -> str:
        return (f"<LiveMonitorCandleFeature {self.symbol} {self.timeframe} "
                f"open_time={self.open_time} user={self.user_id}>")


class LiveMonitorStructureEvent(db.Model):
    """Phase 10.9D: Structure context events for Bias Shift Watch items.

    Stores swing-based BOS, CHoCH, and liquidity sweep events per TF.
    CONTEXT ONLY — these events are NOT entry signals.
    BOS/CHoCH do NOT create Entry Candidate state.
    No S/R Flip logic. No order-flow. No trading execution. No private API.

    Upsert on uq_lm_struct_event prevents duplicates on repeated runs.
    Full event arrays are never stored in LiveMonitorItem.snapshot_json.
    """
    __tablename__ = "live_monitor_structure_events"

    id               = db.Column(db.Integer, primary_key=True)
    user_id          = db.Column(db.Integer, db.ForeignKey("users.id"),
                                 nullable=False, index=True)
    exchange         = db.Column(db.String(20), nullable=False)
    market           = db.Column(db.String(20), nullable=False, default="perpetual")
    symbol           = db.Column(db.String(20), nullable=False)
    timeframe        = db.Column(db.String(10), nullable=False)
    event_time       = db.Column(db.BigInteger, nullable=False)   # ms epoch of confirming candle
    event_type       = db.Column(db.String(40), nullable=False)   # bullish_bos / bearish_bos / etc.
    direction        = db.Column(db.String(10), nullable=True)    # bullish / bearish / neutral
    level            = db.Column(db.Float,      nullable=True)    # broken/swept swing level price
    candle_open_time = db.Column(db.BigInteger, nullable=True)    # open_time of confirming candle
    confirmation_close = db.Column(db.Float,   nullable=True)     # close of confirming candle
    swing_left       = db.Column(db.Integer,   nullable=True)     # bars left used in detection
    swing_right      = db.Column(db.Integer,   nullable=True)     # bars right used in detection
    threshold_pct    = db.Column(db.Float,     nullable=True)     # break/sweep threshold %
    strength_score   = db.Column(db.Float,     nullable=True)     # 0-100 context weight (reserved)
    context_json     = db.Column(db.Text,      nullable=True)     # additional context

    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc),
                           onupdate=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        db.UniqueConstraint(
            "user_id", "exchange", "market", "symbol", "timeframe",
            "event_time", "event_type",
            name="uq_lm_struct_event",
        ),
        db.Index("ix_lm_struct_lookup",
                 "user_id", "exchange", "market", "symbol", "timeframe", "event_time"),
    )

    user = db.relationship("User", foreign_keys=[user_id])

    def __repr__(self) -> str:
        return (f"<LiveMonitorStructureEvent {self.symbol}/{self.timeframe} "
                f"{self.event_type} t={self.event_time} user={self.user_id}>")
