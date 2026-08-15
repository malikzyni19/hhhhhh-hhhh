from flask_sqlalchemy import SQLAlchemy
from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime, timezone

db = SQLAlchemy()

ALL_MODULES     = ["ob", "fvg", "bb", "fib"]
ALL_TABS        = ["scan", "pairs", "settings", "compressed", "base", "trending", "athatl", "bias", "watchlist"]
ALL_EXCHANGES   = ["binance", "bybit", "okx", "mexc", "upbit"]
ALL_TIMEFRAMES  = ["15m", "30m", "1h", "4h", "1d"]

# ── Subscription tiers ───────────────────────────────────────────────────────
# Ordered cheapest → most capable. "basic" is the free tier every new account
# starts on. Roles (admin/user) control authorisation; tiers control entitlement.
ALL_TIERS       = ["basic", "pro", "pro_plus"]
DEFAULT_TIER    = "basic"

TIER_LABELS = {
    "basic":    "Basic",
    "pro":      "Pro",
    "pro_plus": "Pro Plus",
}


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

    # ── Subscription ──
    tier            = db.Column(db.String(20), default=DEFAULT_TIER, nullable=False,
                                server_default=DEFAULT_TIER, index=True)
    tier_expires_at = db.Column(db.DateTime, nullable=True)   # NULL = never expires
    tier_since      = db.Column(db.DateTime, nullable=True)

    def set_password(self, password: str) -> None:
        self.password_hash = generate_password_hash(password)

    def check_password(self, password: str) -> bool:
        return check_password_hash(self.password_hash, password)

    @property
    def is_admin(self) -> bool:
        return self.role == "admin"

    @property
    def tier_is_expired(self) -> bool:
        """True when a paid tier has lapsed and should be treated as basic."""
        if self.tier == DEFAULT_TIER or self.tier_expires_at is None:
            return False
        exp = self.tier_expires_at
        if exp.tzinfo is None:
            exp = exp.replace(tzinfo=timezone.utc)
        return exp < datetime.now(timezone.utc)

    @property
    def effective_tier(self) -> str:
        """The tier actually in force — a lapsed subscription falls back to basic."""
        if self.tier not in ALL_TIERS:
            return DEFAULT_TIER
        return DEFAULT_TIER if self.tier_is_expired else self.tier

    @property
    def tier_label(self) -> str:
        return TIER_LABELS.get(self.effective_tier, self.effective_tier)

    def __repr__(self) -> str:
        return f"<User {self.username} [{self.role}/{self.tier}]>"


class TierPlan(db.Model):
    """Per-tier entitlements. One row per entry in ALL_TIERS."""
    __tablename__ = "tier_plans"

    id           = db.Column(db.Integer, primary_key=True)
    tier         = db.Column(db.String(20), unique=True, nullable=False, index=True)
    display_name = db.Column(db.String(60), nullable=False, default="")
    description  = db.Column(db.String(255), nullable=True)
    price_monthly = db.Column(db.Float, default=0.0, nullable=False)
    sort_order   = db.Column(db.Integer, default=0, nullable=False)
    is_active    = db.Column(db.Boolean, default=True, nullable=False, server_default="true")

    # Scan limits
    daily_tokens        = db.Column(db.Integer, default=500,  nullable=False)
    max_pairs_per_scan  = db.Column(db.Integer, default=100,  nullable=False)
    max_pairs_per_cycle = db.Column(db.Integer, default=50,   nullable=False)

    # Feature limits
    max_live_monitor_items = db.Column(db.Integer, default=5,  nullable=False)
    ai_calls_per_day       = db.Column(db.Integer, default=10, nullable=False)
    max_scan_presets       = db.Column(db.Integer, default=3,  nullable=False)

    # Access lists — JSON arrays, same shape as RolePermission
    allowed_modules    = db.Column(db.Text, nullable=True)
    allowed_tabs       = db.Column(db.Text, nullable=True)
    allowed_exchanges  = db.Column(db.Text, nullable=True)
    allowed_timeframes = db.Column(db.Text, nullable=True)

    updated_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc),
                           onupdate=lambda: datetime.now(timezone.utc))
    updated_by = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)

    def __repr__(self) -> str:
        return f"<TierPlan {self.tier}>"


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
    # Tier enforcement: AI requests are metered separately from scan tokens.
    ai_calls     = db.Column(db.Integer, default=0, nullable=False, server_default="0")

    __table_args__ = (db.UniqueConstraint("user_id", "date", name="uq_user_date"),)

    def __repr__(self) -> str:
        return f"<DailyTokenUsage user_id={self.user_id} date={self.date}>"


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

    # Phase 11.11: Execution mode architecture — default internal_paper (primary)
    execution_mode = db.Column(db.String(40), nullable=False,
                               default="internal_paper", server_default="internal_paper")
    policy_mode    = db.Column(db.String(40), nullable=False,
                               default="paper_manual",   server_default="paper_manual")

    # Phase 11.13: Paper Risk Guard — per-user configurable limits JSON
    paper_risk_guard_settings_json = db.Column(db.Text, nullable=True)

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


class LiveMonitorOrderflowSnapshot(db.Model):
    """Phase 10.9E: Point-in-time order-flow snapshot for Bias Shift Watch items.

    Collected by a background sampler every N seconds for active Bias Shift items.
    Each row = one sampled order-flow state: OB walls, delta, OI, liquidations,
    L/S ratio, funding. Snapshots are the raw evidence used by the candle-window
    alignment engine.

    EVIDENCE ONLY. Not entry signals. No trading. No private API. No API keys.
    No S/R Flip logic. No AI decision engine. No Entry Candidate state.
    Data Health freshness is the source of truth for stale/unavailable status.
    Rows older than retention window are automatically pruned.
    Full snapshot arrays are never stored in LiveMonitorItem.snapshot_json.
    """
    __tablename__ = "live_monitor_orderflow_snapshots"

    id              = db.Column(db.Integer, primary_key=True)
    user_id         = db.Column(db.Integer, db.ForeignKey("users.id"),
                                nullable=False, index=True)
    exchange        = db.Column(db.String(20), nullable=False)
    market          = db.Column(db.String(20), nullable=False, default="perpetual")
    symbol          = db.Column(db.String(20), nullable=False)
    analysis_source = db.Column(db.String(20), nullable=False, default="binance")
    sample_time     = db.Column(db.BigInteger, nullable=False)   # ms epoch
    source_status   = db.Column(db.String(20), nullable=True)    # fresh/partial/stale/unavailable
    sources_used_json    = db.Column(db.Text, nullable=True)
    sources_skipped_json = db.Column(db.Text, nullable=True)

    # Price
    live_price   = db.Column(db.Float, nullable=True)
    mark_price   = db.Column(db.Float, nullable=True)

    # Order book
    orderbook_status              = db.Column(db.String(20), nullable=True)
    orderbook_imbalance           = db.Column(db.Float, nullable=True)
    bid_wall_price                = db.Column(db.Float, nullable=True)
    bid_wall_size                 = db.Column(db.Float, nullable=True)
    ask_wall_price                = db.Column(db.Float, nullable=True)
    ask_wall_size                 = db.Column(db.Float, nullable=True)
    nearest_bid_wall_distance_pct = db.Column(db.Float, nullable=True)
    nearest_ask_wall_distance_pct = db.Column(db.Float, nullable=True)

    # Delta / taker flow
    delta_status   = db.Column(db.String(20), nullable=True)
    buy_volume     = db.Column(db.Float, nullable=True)
    sell_volume    = db.Column(db.Float, nullable=True)
    delta_net      = db.Column(db.Float, nullable=True)
    delta_pct      = db.Column(db.Float, nullable=True)
    taker_pressure = db.Column(db.String(20), nullable=True)   # bullish/bearish/neutral/missing

    # Open interest
    oi_status     = db.Column(db.String(20), nullable=True)
    open_interest = db.Column(db.Float, nullable=True)
    oi_change     = db.Column(db.Float, nullable=True)
    oi_change_pct = db.Column(db.Float, nullable=True)

    # Liquidations
    liquidation_status = db.Column(db.String(20), nullable=True)
    long_liq_usd       = db.Column(db.Float, nullable=True)
    short_liq_usd      = db.Column(db.Float, nullable=True)
    net_liq_usd        = db.Column(db.Float, nullable=True)

    # Long/short + funding
    long_short_status = db.Column(db.String(20), nullable=True)
    long_short_ratio  = db.Column(db.Float, nullable=True)
    funding_status    = db.Column(db.String(20), nullable=True)
    funding_rate      = db.Column(db.Float, nullable=True)

    # Data health
    data_health_status = db.Column(db.String(20), nullable=True)
    data_health_json   = db.Column(db.Text, nullable=True)
    raw_summary_json   = db.Column(db.Text, nullable=True)

    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc),
                           onupdate=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        db.Index("ix_lm_of_snap_lookup",
                 "user_id", "exchange", "market", "symbol", "sample_time"),
        db.Index("ix_lm_of_snap_sym_time",
                 "user_id", "symbol", "sample_time"),
    )

    user = db.relationship("User", foreign_keys=[user_id])

    def __repr__(self) -> str:
        return (f"<LiveMonitorOrderflowSnapshot {self.symbol} src={self.analysis_source} "
                f"t={self.sample_time} status={self.source_status} user={self.user_id}>")


class LiveMonitorCandleOrderflow(db.Model):
    """Phase 10.9E: Per-candle order-flow alignment for Bias Shift Watch items.

    One row per (user_id, exchange, market, symbol, timeframe, open_time, analysis_source).
    Aggregates snapshots that fell inside the candle's time window and classifies
    whether the observed order flow confirmed, contradicted, or is missing for that candle.

    EVIDENCE ONLY. Not entry signals. No trading. No private API. No API keys.
    No S/R Flip logic. No AI decision engine. No Entry Candidate state.
    Upsert on uq_lm_candle_orderflow — repeated refresh is idempotent.
    Compact summary only — no raw snapshot arrays stored here.
    """
    __tablename__ = "live_monitor_candle_orderflow"

    id              = db.Column(db.Integer, primary_key=True)
    user_id         = db.Column(db.Integer, db.ForeignKey("users.id"),
                                nullable=False, index=True)
    exchange        = db.Column(db.String(20), nullable=False)
    market          = db.Column(db.String(20), nullable=False, default="perpetual")
    symbol          = db.Column(db.String(20), nullable=False)
    timeframe       = db.Column(db.String(10), nullable=False)
    open_time       = db.Column(db.BigInteger, nullable=False)   # ms epoch candle open
    close_time      = db.Column(db.BigInteger, nullable=True)    # ms epoch candle close
    analysis_source = db.Column(db.String(20), nullable=False, default="binance")
    sample_count    = db.Column(db.Integer, nullable=True, default=0)
    first_sample_time = db.Column(db.BigInteger, nullable=True)
    last_sample_time  = db.Column(db.BigInteger, nullable=True)

    # Aggregated status fields
    flow_status        = db.Column(db.String(30), nullable=True)   # ready/partial/no_samples/stale
    data_health_status = db.Column(db.String(20), nullable=True)
    orderbook_status   = db.Column(db.String(20), nullable=True)
    delta_status       = db.Column(db.String(20), nullable=True)
    oi_status          = db.Column(db.String(20), nullable=True)
    liquidation_status = db.Column(db.String(20), nullable=True)
    long_short_status  = db.Column(db.String(20), nullable=True)
    funding_status     = db.Column(db.String(20), nullable=True)

    # Delta / taker summary
    buy_volume_sum     = db.Column(db.Float, nullable=True)
    sell_volume_sum    = db.Column(db.Float, nullable=True)
    delta_net_sum      = db.Column(db.Float, nullable=True)
    delta_pct_avg      = db.Column(db.Float, nullable=True)
    taker_pressure_avg = db.Column(db.String(20), nullable=True)

    # OI summary
    oi_first      = db.Column(db.Float, nullable=True)
    oi_last       = db.Column(db.Float, nullable=True)
    oi_change     = db.Column(db.Float, nullable=True)
    oi_change_pct = db.Column(db.Float, nullable=True)

    # Liquidation summary
    long_liq_usd_sum  = db.Column(db.Float, nullable=True)
    short_liq_usd_sum = db.Column(db.Float, nullable=True)
    net_liq_usd_sum   = db.Column(db.Float, nullable=True)

    # Order book summary
    avg_orderbook_imbalance  = db.Column(db.Float, nullable=True)
    nearest_bid_wall_price   = db.Column(db.Float, nullable=True)
    nearest_bid_wall_size    = db.Column(db.Float, nullable=True)
    nearest_ask_wall_price   = db.Column(db.Float, nullable=True)
    nearest_ask_wall_size    = db.Column(db.Float, nullable=True)
    bid_wall_near_candle_low  = db.Column(db.Boolean, nullable=True)
    ask_wall_near_candle_high = db.Column(db.Boolean, nullable=True)

    # Candle relation and alignment result
    candle_direction           = db.Column(db.String(10), nullable=True)  # bullish/bearish/neutral
    candle_patterns_json       = db.Column(db.Text, nullable=True)        # compact list
    preliminary_flow_direction = db.Column(db.String(20), nullable=True)  # bullish/bearish/neutral/mixed/missing
    candle_flow_alignment      = db.Column(db.String(20), nullable=True)  # confirmed/contradicted/neutral/missing/partial
    alignment_reason_json      = db.Column(db.Text, nullable=True)        # compact reason list

    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc),
                           onupdate=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        db.UniqueConstraint(
            "user_id", "exchange", "market", "symbol", "timeframe",
            "open_time", "analysis_source",
            name="uq_lm_candle_orderflow",
        ),
        db.Index("ix_lm_cof_lookup",
                 "user_id", "exchange", "market", "symbol", "timeframe", "open_time"),
    )

    user = db.relationship("User", foreign_keys=[user_id])

    def __repr__(self) -> str:
        return (f"<LiveMonitorCandleOrderflow {self.symbol}/{self.timeframe} "
                f"open_time={self.open_time} align={self.candle_flow_alignment} "
                f"user={self.user_id}>")


class LiveMonitorAIIntervention(db.Model):
    """Phase 11.3: AI Trade Control Decision record.

    Stores deterministic decision outputs generated by _lm_build_ai_trade_control_decision.
    Decision generation only — no execution, no automation, no order placement.
    No connection to Trading Terminal, Risk Guard, or Trade Journal.
    Storage is for audit/history only.
    """
    __tablename__ = "live_monitor_ai_intervention"

    id              = db.Column(db.Integer, primary_key=True)
    user_id         = db.Column(db.Integer, db.ForeignKey("users.id"),
                                nullable=False, index=True)
    item_id         = db.Column(db.Integer, db.ForeignKey("live_monitor_items.id"),
                                nullable=False, index=True)
    symbol          = db.Column(db.String(20), nullable=False)

    decision_action = db.Column(db.String(40), nullable=False)
    confidence      = db.Column(db.Integer, nullable=True)
    danger_level    = db.Column(db.String(20), nullable=True)

    primary_reasons_json = db.Column(db.Text, nullable=True)
    risk_factors_json    = db.Column(db.Text, nullable=True)

    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc),
                           nullable=False, index=True)

    __table_args__ = (
        db.Index("ix_lm_ai_intervention_item", "user_id", "item_id"),
    )

    user = db.relationship("User",            foreign_keys=[user_id])
    item = db.relationship("LiveMonitorItem", foreign_keys=[item_id])

    def __repr__(self) -> str:
        return (f"<LiveMonitorAIIntervention item={self.item_id} "
                f"action={self.decision_action} conf={self.confidence} "
                f"user={self.user_id}>")


class LiveMonitorTestnetOrder(db.Model):
    """Phase 11.7A: Binance Futures Testnet LIMIT order record.

    Stores the full lifecycle of a manual testnet order: draft, submission,
    Binance response, and context snapshots at the time of submission.

    Rules:
    - No API keys stored.
    - No secrets stored.
    - Populated only by POST /api/live-monitor/items/<id>/testnet-order/submit.
    - No automatic population — manual submit only.
    - Testnet only.
    """
    __tablename__ = "live_monitor_testnet_orders"

    id              = db.Column(db.Integer, primary_key=True)
    user_id         = db.Column(db.Integer, db.ForeignKey("users.id"),
                                nullable=False, index=True)
    item_id         = db.Column(db.Integer, db.ForeignKey("live_monitor_items.id"),
                                nullable=False, index=True)

    symbol          = db.Column(db.String(20), nullable=False)
    side            = db.Column(db.String(10), nullable=False)          # BUY | SELL
    order_type      = db.Column(db.String(20), nullable=False)          # LIMIT
    time_in_force   = db.Column(db.String(10), nullable=False)          # GTC
    quantity        = db.Column(db.String(40), nullable=False)          # decimal string
    price           = db.Column(db.String(40), nullable=False)          # decimal string
    status          = db.Column(db.String(30), nullable=False,          # draft/submitted/filled/failed/cancelled
                                default="draft", index=True)

    client_order_id  = db.Column(db.String(40), nullable=True, index=True)
    binance_order_id = db.Column(db.String(40), nullable=True, index=True)

    # Snapshot context at time of submission (no secrets, no API keys)
    execution_intent_json     = db.Column(db.Text, nullable=True)
    execution_simulation_json = db.Column(db.Text, nullable=True)
    ai_decision_json          = db.Column(db.Text, nullable=True)
    automation_policy_json    = db.Column(db.Text, nullable=True)

    # Request / response audit trail (no secrets)
    request_json  = db.Column(db.Text, nullable=True)   # POST params sent (no signature, no secret)
    response_json = db.Column(db.Text, nullable=True)   # Binance response (safe compact)
    error_json    = db.Column(db.Text, nullable=True)   # error if failed

    created_at   = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc),
                             nullable=False, index=True)
    submitted_at = db.Column(db.DateTime, nullable=True)
    updated_at   = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc),
                             onupdate=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        db.Index("ix_lm_testnet_order_item", "user_id", "item_id"),
    )

    user = db.relationship("User",            foreign_keys=[user_id])
    item = db.relationship("LiveMonitorItem", foreign_keys=[item_id])

    def __repr__(self) -> str:
        return (f"<LiveMonitorTestnetOrder id={self.id} {self.symbol} "
                f"{self.side} qty={self.quantity} status={self.status} "
                f"user={self.user_id}>")


# ── Phase 11.7B: Internal Paper Trading Models ────────────────────────────────

class LiveMonitorPaperAccount(db.Model):
    """Paper trading account (DB-only, no exchange). One per user."""
    __tablename__ = "live_monitor_paper_accounts"

    id               = db.Column(db.Integer, primary_key=True)
    user_id          = db.Column(db.Integer, db.ForeignKey("users.id"),
                                 nullable=False, unique=True, index=True)
    currency         = db.Column(db.String(10), nullable=False, default="USDT")
    starting_balance = db.Column(db.Numeric(20, 8), nullable=False, default=10000.0)
    cash_balance     = db.Column(db.Numeric(20, 8), nullable=False, default=10000.0)
    equity           = db.Column(db.Numeric(20, 8), nullable=False, default=10000.0)
    realized_pnl     = db.Column(db.Numeric(20, 8), nullable=False, default=0.0)
    unrealized_pnl   = db.Column(db.Numeric(20, 8), nullable=False, default=0.0)
    status           = db.Column(db.String(20), nullable=False, default="active")
    created_at       = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc),
                                 nullable=False)
    updated_at       = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc),
                                 onupdate=lambda: datetime.now(timezone.utc))

    user = db.relationship("User", foreign_keys=[user_id])

    def __repr__(self) -> str:
        return (f"<LiveMonitorPaperAccount id={self.id} user={self.user_id} "
                f"cash={self.cash_balance}>")


class LiveMonitorPaperOrder(db.Model):
    """Paper LIMIT order record. DB-only. No exchange calls. Manual submit only."""
    __tablename__ = "live_monitor_paper_orders"

    id               = db.Column(db.Integer, primary_key=True)
    user_id          = db.Column(db.Integer, db.ForeignKey("users.id"),
                                 nullable=False, index=True)
    item_id          = db.Column(db.Integer, db.ForeignKey("live_monitor_items.id"),
                                 nullable=False, index=True)

    symbol           = db.Column(db.String(20), nullable=False)
    side             = db.Column(db.String(10), nullable=False)       # BUY | SELL
    order_type       = db.Column(db.String(20), nullable=False)       # LIMIT
    time_in_force    = db.Column(db.String(10), nullable=False)       # GTC
    quantity         = db.Column(db.String(40), nullable=False)       # decimal string
    price            = db.Column(db.String(40), nullable=False)       # decimal string
    status           = db.Column(db.String(30), nullable=False,
                                 default="open", index=True)          # open/filled/cancelled
    fill_status      = db.Column(db.String(20), nullable=False,
                                 default="unfilled")                  # unfilled/partial/filled
    client_order_id  = db.Column(db.String(40), nullable=True, index=True)
    source           = db.Column(db.String(40), nullable=False,
                                 default="internal_paper")
    estimated_notional = db.Column(db.Numeric(20, 8), nullable=True)

    execution_intent_json     = db.Column(db.Text, nullable=True)
    execution_simulation_json = db.Column(db.Text, nullable=True)
    ai_decision_json          = db.Column(db.Text, nullable=True)
    automation_policy_json    = db.Column(db.Text, nullable=True)

    created_at  = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc),
                            nullable=False, index=True)
    updated_at  = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc),
                            onupdate=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        db.Index("ix_lm_paper_order_item", "user_id", "item_id"),
    )

    user = db.relationship("User",            foreign_keys=[user_id])
    item = db.relationship("LiveMonitorItem", foreign_keys=[item_id])

    def __repr__(self) -> str:
        return (f"<LiveMonitorPaperOrder id={self.id} {self.symbol} "
                f"{self.side} qty={self.quantity} status={self.status}>")


class LiveMonitorPaperPosition(db.Model):
    """Paper position (schema only — fill engine: Phase 11.7C)."""
    __tablename__ = "live_monitor_paper_positions"

    id             = db.Column(db.Integer, primary_key=True)
    user_id        = db.Column(db.Integer, db.ForeignKey("users.id"),
                               nullable=False, index=True)
    item_id        = db.Column(db.Integer, db.ForeignKey("live_monitor_items.id"),
                               nullable=False, index=True)
    order_id       = db.Column(db.Integer, db.ForeignKey("live_monitor_paper_orders.id"),
                               nullable=True)

    symbol         = db.Column(db.String(20), nullable=False)
    side           = db.Column(db.String(10), nullable=False)  # LONG | SHORT
    size           = db.Column(db.String(40), nullable=False)
    entry_price    = db.Column(db.String(40), nullable=False)
    status         = db.Column(db.String(20), nullable=False, default="open", index=True)
    realized_pnl   = db.Column(db.Numeric(20, 8), nullable=False, default=0.0)
    unrealized_pnl = db.Column(db.Numeric(20, 8), nullable=False, default=0.0)

    # Phase 11.9: TP/SL close metadata (nullable — added via idempotent migration)
    close_reason   = db.Column(db.String(30),  nullable=True)   # "take_profit" | "stop_loss" | "manual"
    close_price    = db.Column(db.String(40),  nullable=True)
    closed_at      = db.Column(db.DateTime,    nullable=True)
    exit_fill_id   = db.Column(db.Integer,     nullable=True)   # fill.id of the closing fill

    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc),
                           nullable=False, index=True)
    updated_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc),
                           onupdate=lambda: datetime.now(timezone.utc))

    user  = db.relationship("User",                    foreign_keys=[user_id])
    item  = db.relationship("LiveMonitorItem",         foreign_keys=[item_id])
    order = db.relationship("LiveMonitorPaperOrder",   foreign_keys=[order_id])

    def __repr__(self) -> str:
        return (f"<LiveMonitorPaperPosition id={self.id} {self.symbol} "
                f"{self.side} size={self.size} status={self.status}>")


class LiveMonitorPaperFill(db.Model):
    """Paper fill record (schema only — fill engine: Phase 11.7C)."""
    __tablename__ = "live_monitor_paper_fills"

    id           = db.Column(db.Integer, primary_key=True)
    user_id      = db.Column(db.Integer, db.ForeignKey("users.id"),
                             nullable=False, index=True)
    order_id     = db.Column(db.Integer, db.ForeignKey("live_monitor_paper_orders.id"),
                             nullable=False, index=True)
    item_id      = db.Column(db.Integer, db.ForeignKey("live_monitor_items.id"),
                             nullable=False, index=True)

    symbol       = db.Column(db.String(20), nullable=False)
    side         = db.Column(db.String(10), nullable=False)
    fill_qty     = db.Column(db.String(40), nullable=False)
    fill_price   = db.Column(db.String(40), nullable=False)
    fill_notional = db.Column(db.Numeric(20, 8), nullable=True)

    # Phase 11.9: close fill metadata (nullable — added via idempotent migration)
    fill_type    = db.Column(db.String(30),    nullable=True)   # "entry" | "take_profit" | "stop_loss"
    position_id  = db.Column(db.Integer,       nullable=True)   # position closed by this fill
    fee          = db.Column(db.Numeric(20, 8), nullable=True, default=0)

    created_at   = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc),
                             nullable=False, index=True)

    user  = db.relationship("User",                  foreign_keys=[user_id])
    order = db.relationship("LiveMonitorPaperOrder", foreign_keys=[order_id])
    item  = db.relationship("LiveMonitorItem",       foreign_keys=[item_id])

    def __repr__(self) -> str:
        return (f"<LiveMonitorPaperFill id={self.id} order={self.order_id} "
                f"{self.symbol} {self.side} qty={self.fill_qty}>")


class LiveMonitorPaperTrade(db.Model):
    """Paper trade journal record — Phase 11.10.

    One row per closed LiveMonitorPaperPosition.
    DB-only. No exchange order IDs. No secrets. No API keys.
    """
    __tablename__ = "live_monitor_paper_trades"

    id             = db.Column(db.Integer, primary_key=True)
    user_id        = db.Column(db.Integer, db.ForeignKey("users.id"),
                               nullable=False, index=True)
    item_id        = db.Column(db.Integer, db.ForeignKey("live_monitor_items.id"),
                               nullable=True,  index=True)
    account_id     = db.Column(db.Integer, nullable=True)
    position_id    = db.Column(db.Integer, nullable=True, index=True, unique=True)
    entry_order_id = db.Column(db.Integer, nullable=True)
    exit_fill_id   = db.Column(db.Integer, nullable=True)

    symbol         = db.Column(db.String(20),  nullable=False)
    side           = db.Column(db.String(10),  nullable=False)
    quantity       = db.Column(db.String(40),  nullable=True)
    entry_price    = db.Column(db.String(40),  nullable=True)
    exit_price     = db.Column(db.String(40),  nullable=True)

    status         = db.Column(db.String(20),  nullable=False, default="closed")
    outcome        = db.Column(db.String(20),  nullable=True)   # win | loss | breakeven
    outcome_reason = db.Column(db.String(40),  nullable=True)   # take_profit | stop_loss | manual | unknown
    realized_pnl     = db.Column(db.Numeric(20, 8), nullable=True, default=0.0)
    realized_pnl_pct = db.Column(db.Numeric(10, 4), nullable=True)
    risk_reward      = db.Column(db.Numeric(10, 4), nullable=True)
    duration_seconds = db.Column(db.Integer,   nullable=True)

    max_favorable_excursion = db.Column(db.Numeric(20, 8), nullable=True)
    max_adverse_excursion   = db.Column(db.Numeric(20, 8), nullable=True)

    # Context snapshots — TEXT (JSON), all nullable
    entry_snapshot_json           = db.Column(db.Text, nullable=True)
    exit_snapshot_json            = db.Column(db.Text, nullable=True)
    execution_intent_json         = db.Column(db.Text, nullable=True)
    execution_intelligence_json   = db.Column(db.Text, nullable=True)
    mtf_orderflow_history_json    = db.Column(db.Text, nullable=True)
    ai_context_json               = db.Column(db.Text, nullable=True)
    ai_decision_json              = db.Column(db.Text, nullable=True)
    automation_policy_json        = db.Column(db.Text, nullable=True)
    paper_order_draft_json        = db.Column(db.Text, nullable=True)
    entry_order_json              = db.Column(db.Text, nullable=True)
    exit_fill_json                = db.Column(db.Text, nullable=True)
    entry_orderflow_snapshot_json = db.Column(db.Text, nullable=True)
    exit_orderflow_snapshot_json  = db.Column(db.Text, nullable=True)
    ai_post_trade_review_json     = db.Column(db.Text, nullable=True)

    created_at = db.Column(db.DateTime,
                           default=lambda: datetime.now(timezone.utc),
                           nullable=False, index=True)
    closed_at  = db.Column(db.DateTime, nullable=True)
    updated_at = db.Column(db.DateTime,
                           default=lambda: datetime.now(timezone.utc),
                           onupdate=lambda: datetime.now(timezone.utc),
                           nullable=True)

    user = db.relationship("User",              foreign_keys=[user_id])
    item = db.relationship("LiveMonitorItem",   foreign_keys=[item_id])

    def __repr__(self) -> str:
        return (f"<LiveMonitorPaperTrade id={self.id} pos={self.position_id} "
                f"{self.symbol} {self.side} outcome={self.outcome}>")


class LiveMonitorPaperAutoGateEvent(db.Model):
    """Paper Auto Mode Safety Gate event log — Phase 11.12.

    Each row is one gate evaluation or arm/disarm event.
    Advisory + metadata only. No execution. No orders. No API keys. No secrets.
    """
    __tablename__ = "live_monitor_paper_auto_gate_events"

    id         = db.Column(db.Integer, primary_key=True)
    user_id    = db.Column(db.Integer, db.ForeignKey("users.id"),
                           nullable=False, index=True)
    item_id    = db.Column(db.Integer, db.ForeignKey("live_monitor_items.id"),
                           nullable=True, index=True)

    event_type = db.Column(db.String(30), nullable=False, index=True)
    # "evaluate" | "arm" | "disarm"

    eligible   = db.Column(db.Boolean, nullable=True)
    armed      = db.Column(db.Boolean, nullable=True, default=False)

    gate_result_json    = db.Column(db.Text, nullable=True)
    checks_json         = db.Column(db.Text, nullable=True)
    advisory_notes_json = db.Column(db.Text, nullable=True)

    execution_mode = db.Column(db.String(40), nullable=True)
    policy_mode    = db.Column(db.String(40), nullable=True)

    created_at = db.Column(db.DateTime,
                           default=lambda: datetime.now(timezone.utc),
                           nullable=False, index=True)

    user = db.relationship("User",            foreign_keys=[user_id])
    item = db.relationship("LiveMonitorItem", foreign_keys=[item_id])

    def __repr__(self) -> str:
        return (f"<LiveMonitorPaperAutoGateEvent id={self.id} "
                f"item={self.item_id} type={self.event_type} eligible={self.eligible}>")


class LiveMonitorLearningReview(db.Model):
    """AI-generated learning observations with human review workflow — Phase 11.15.

    Advisory + read-only analysis of paper trading history only.
    No execution. No orders. No strategy mutation. No automatic apply.
    Human review decisions are tracked here; no decision is ever automated.
    """
    __tablename__ = "live_monitor_learning_reviews"

    id      = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"),
                        nullable=False, index=True)
    item_id = db.Column(db.Integer, db.ForeignKey("live_monitor_items.id"),
                        nullable=True, index=True)

    # Scope and filter context used to generate this review
    review_scope = db.Column(db.String(20), nullable=False, default="portfolio")
    # "portfolio" | "symbol" | "item"
    period       = db.Column(db.String(10), nullable=True)
    # "7d" | "30d" | "90d" | "365d" | "all"
    symbol       = db.Column(db.String(30), nullable=True)
    side         = db.Column(db.String(10), nullable=True)
    # "long" | "short" | null (= both)

    # Review status workflow: generated → reviewed / accepted_insight / rejected / archived
    status = db.Column(db.String(30), nullable=False, default="generated", index=True)

    # Human-readable summary fields written by AI and optionally annotated by human
    title   = db.Column(db.Text, nullable=True)
    summary = db.Column(db.Text, nullable=True)

    # Full AI response and evidence snapshot (JSON blobs)
    review_json   = db.Column(db.Text, nullable=True)  # parsed AI observations
    evidence_json = db.Column(db.Text, nullable=True)  # evidence segments snapshot

    # Human annotation
    human_note = db.Column(db.Text, nullable=True)

    # Data quality metadata
    sample_size     = db.Column(db.Integer,    nullable=True)
    sample_quality  = db.Column(db.String(20), nullable=True)  # "insufficient"|"early"|"developing"|"meaningful"
    confidence_level= db.Column(db.String(20), nullable=True)  # "high" | "medium" | "low"
    warning_count   = db.Column(db.Integer,    nullable=True, default=0)

    # Provenance
    source         = db.Column(db.String(30), nullable=True)   # "ai" | "deterministic"
    model_name     = db.Column(db.String(80), nullable=True)
    prompt_version = db.Column(db.String(20), nullable=True, default="11.15.0")

    # Review lineage (self-referential: a re-run supersedes an older review)
    parent_review_id    = db.Column(db.Integer, db.ForeignKey("live_monitor_learning_reviews.id"),
                                    nullable=True, index=True)
    supersedes_review_id= db.Column(db.Integer, db.ForeignKey("live_monitor_learning_reviews.id"),
                                    nullable=True)

    created_at  = db.Column(db.DateTime,
                            default=lambda: datetime.now(timezone.utc),
                            nullable=False, index=True)
    updated_at  = db.Column(db.DateTime,
                            default=lambda: datetime.now(timezone.utc),
                            onupdate=lambda: datetime.now(timezone.utc),
                            nullable=True)
    reviewed_at = db.Column(db.DateTime, nullable=True)

    user = db.relationship("User",            foreign_keys=[user_id])
    item = db.relationship("LiveMonitorItem", foreign_keys=[item_id])
    parent_review = db.relationship(
        "LiveMonitorLearningReview",
        foreign_keys=[parent_review_id],
        remote_side="LiveMonitorLearningReview.id",
        uselist=False,
    )

    def __repr__(self) -> str:
        return (f"<LiveMonitorLearningReview id={self.id} "
                f"user={self.user_id} scope={self.review_scope} status={self.status}>")


class LiveMonitorFlowCandle(db.Model):
    """Per-candle order-flow series: CVD (from ticks) + Open Interest — Phase FlowC.

    One row per symbol + timeframe + candle_open_time. Base timeframe is 1m;
    higher timeframes are aggregated on read. Volumes are USD (quote) amounts.
    Advisory market data only — no execution, no orders, no strategy mutation.
    """
    __tablename__ = "live_monitor_flow_candles"

    id               = db.Column(db.Integer, primary_key=True)
    symbol           = db.Column(db.String(30), nullable=False, index=True)
    exchange         = db.Column(db.String(20), nullable=False, default="binance")
    timeframe        = db.Column(db.String(10), nullable=False, default="1m")
    candle_open_ms   = db.Column(db.BigInteger, nullable=False)   # epoch ms of candle open

    price_close      = db.Column(db.Float, nullable=True)
    buy_vol_usd      = db.Column(db.Float, nullable=False, default=0.0)   # taker-buy quote vol
    sell_vol_usd     = db.Column(db.Float, nullable=False, default=0.0)   # taker-sell quote vol
    delta_usd        = db.Column(db.Float, nullable=False, default=0.0)   # buy - sell
    cvd_usd          = db.Column(db.Float, nullable=False, default=0.0)   # running cumulative delta
    oi_close         = db.Column(db.Float, nullable=True)                 # open interest (contracts)
    oi_delta         = db.Column(db.Float, nullable=True)                 # vs previous sample
    tick_count       = db.Column(db.Integer, nullable=False, default=0)
    source           = db.Column(db.String(20), nullable=False, default="live")  # "live" | "backfill"

    created_at       = db.Column(db.DateTime,
                                 default=lambda: datetime.now(timezone.utc),
                                 nullable=False)

    __table_args__ = (
        db.UniqueConstraint("symbol", "timeframe", "candle_open_ms",
                            name="uq_flow_candle_symbol_tf_open"),
        db.Index("ix_flow_candle_symbol_tf_open", "symbol", "timeframe", "candle_open_ms"),
    )

    def __repr__(self) -> str:
        return (f"<FlowCandle {self.symbol} {self.timeframe} "
                f"open={self.candle_open_ms} delta={self.delta_usd:.0f}>")


class LiveMonitorSpotFlowCandle(db.Model):
    """Per-candle SPOT order-flow series: CVD from spot ticks — Phase SpotFlow-1.

    Dedicated table, separate from LiveMonitorFlowCandle (futures/perp CVD+OI),
    on purpose — this guarantees zero regression risk to the already-shipped
    perp flow-candle system (different collector, different symbols cadence,
    different retention/backfill jobs; a bug here cannot corrupt perp rows).

    Spot markets have no "open interest" concept (outright ownership, not
    open contracts), so unlike LiveMonitorFlowCandle this table has no
    oi_close/oi_delta columns.

    One row per symbol + exchange + timeframe + candle_open_time. Base
    timeframe is 1m. Advisory market data only — no execution, no orders,
    no strategy mutation. Phase 1 scope: Binance spot only (exchange is a
    real column from day one so Bybit/OKX/MEXC spot can be added later
    without a schema change).
    """
    __tablename__ = "live_monitor_spot_flow_candles"

    id               = db.Column(db.Integer, primary_key=True)
    symbol           = db.Column(db.String(30), nullable=False, index=True)
    exchange         = db.Column(db.String(20), nullable=False, default="binance")
    timeframe        = db.Column(db.String(10), nullable=False, default="1m")
    candle_open_ms   = db.Column(db.BigInteger, nullable=False)   # epoch ms of candle open

    price_close      = db.Column(db.Float, nullable=True)
    buy_vol_usd      = db.Column(db.Float, nullable=False, default=0.0)   # taker-buy quote vol
    sell_vol_usd     = db.Column(db.Float, nullable=False, default=0.0)   # taker-sell quote vol
    delta_usd        = db.Column(db.Float, nullable=False, default=0.0)   # buy - sell
    cvd_usd          = db.Column(db.Float, nullable=False, default=0.0)   # running cumulative delta
    tick_count       = db.Column(db.Integer, nullable=False, default=0)
    source           = db.Column(db.String(20), nullable=False, default="live")  # "live" | "backfill"

    created_at       = db.Column(db.DateTime,
                                 default=lambda: datetime.now(timezone.utc),
                                 nullable=False)

    __table_args__ = (
        db.UniqueConstraint("symbol", "exchange", "timeframe", "candle_open_ms",
                            name="uq_spot_flow_candle_symbol_ex_tf_open"),
        db.Index("ix_spot_flow_candle_symbol_ex_tf_open",
                "symbol", "exchange", "timeframe", "candle_open_ms"),
    )

    def __repr__(self) -> str:
        return (f"<SpotFlowCandle {self.exchange}:{self.symbol} {self.timeframe} "
                f"open={self.candle_open_ms} delta={self.delta_usd:.0f}>")


# ═══════════════════════════════════════════════════════════════════════════════
# Signal Alerts (Phase SIG-1) — Live Monitor → Telegram setup alerts
#
# Replaces the retired auto-trading path. Detection is GLOBAL (a setup is found
# once, across all users); delivery is PER-USER (each user receives only the
# signals matching their own settings, through their own Telegram bot).
#
# No order placement. No exchange execution. Alert delivery + outcome tracking
# only — these tables never hold API keys for trading, only a Telegram bot
# token used to send messages.
# ═══════════════════════════════════════════════════════════════════════════════

class LiveMonitorSignalSettings(db.Model):
    """Per-user configuration controlling which signals reach them, and how.

    One row per user. Created on first access with defaults (see
    live_monitor/signal_settings.py) so a user never has to configure
    anything before the system works.
    """
    __tablename__ = "live_monitor_signal_settings"

    id      = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"),
                        nullable=False, unique=True, index=True)

    # ── Master switch ────────────────────────────────────────────────────────
    enabled = db.Column(db.Boolean, nullable=False, default=False,
                        server_default="false")

    # ── Which scan modules may produce signals ───────────────────────────────
    # JSON list of module keys, e.g. ["ob","fvg","bb","ath_atl","accumulation",
    # "compressed","bias_shift","fib_confluence","liquidity_sweep"]
    enabled_modules_json = db.Column(db.Text, nullable=True)

    # ── Confluence ───────────────────────────────────────────────────────────
    # Minimum number of distinct modules that must agree on the same pair and
    # direction before a candidate is eligible. 1 = single-module signals allowed.
    min_confluence = db.Column(db.Integer, nullable=False, default=1,
                               server_default="1")

    # ── Volume control ───────────────────────────────────────────────────────
    max_signals_per_day  = db.Column(db.Integer, nullable=False, default=6,
                                     server_default="6")
    # Minimum minutes between two signals for the SAME pair (anti-spam).
    per_pair_cooldown_min = db.Column(db.Integer, nullable=False, default=240,
                                      server_default="240")
    # Minimum AI confidence (0-100) required to send.
    min_confidence = db.Column(db.Integer, nullable=False, default=65,
                               server_default="65")

    # ── Trigger timing ───────────────────────────────────────────────────────
    # "approach"  → fire when price nears the zone (early warning)
    # "in_zone"   → fire when price is in the zone AND order flow confirms
    # "both"      → send both, tagged as separate alert kinds
    trigger_mode = db.Column(db.String(20), nullable=False, default="in_zone",
                             server_default="in_zone")
    # How close (percent) price must be to the zone to count as "approaching".
    approach_pct = db.Column(db.Float, nullable=False, default=1.5,
                             server_default="1.5")

    # ── Coin scope ───────────────────────────────────────────────────────────
    # "top_volume" → top N pairs by 24h volume
    # "watchlist"  → only pairs in symbols_json
    # "all"        → every supported USDT perpetual
    coin_scope       = db.Column(db.String(20), nullable=False, default="top_volume",
                                 server_default="top_volume")
    coin_scope_limit = db.Column(db.Integer, nullable=False, default=100,
                                 server_default="100")
    symbols_json     = db.Column(db.Text, nullable=True)   # JSON list, watchlist mode

    # ── Direction / timeframe filters ────────────────────────────────────────
    allow_long   = db.Column(db.Boolean, nullable=False, default=True,
                             server_default="true")
    allow_short  = db.Column(db.Boolean, nullable=False, default=True,
                             server_default="true")
    timeframes_json = db.Column(db.Text, nullable=True)    # JSON list, null = all

    # ── Telegram delivery ────────────────────────────────────────────────────
    # Each user links their OWN bot. Token is stored so the server can send on
    # their behalf; it grants no trading access of any kind.
    telegram_bot_token = db.Column(db.Text,        nullable=True)
    telegram_chat_id   = db.Column(db.String(64),  nullable=True)
    telegram_verified_at = db.Column(db.DateTime,  nullable=True)
    telegram_last_error  = db.Column(db.String(255), nullable=True)

    # Master delivery switch, separate from `enabled`: lets the funnel run and
    # record everything while sending nothing (shadow mode).
    delivery_enabled = db.Column(db.Boolean, nullable=False, default=False,
                                 server_default="false")

    # ── Scanner pace ─────────────────────────────────────────────────────────
    # How hard the background scanner works. These were environment-only,
    # which meant the two numbers that most affect cost and responsiveness
    # could not be changed without a redeploy.
    scan_interval_sec = db.Column(db.Integer, nullable=False, default=900,
                                  server_default="900")
    scan_pairs_per_cycle = db.Column(db.Integer, nullable=False, default=30,
                                     server_default="30")

    created_at = db.Column(db.DateTime,
                           default=lambda: datetime.now(timezone.utc),
                           nullable=False)
    updated_at = db.Column(db.DateTime,
                           default=lambda: datetime.now(timezone.utc),
                           onupdate=lambda: datetime.now(timezone.utc),
                           nullable=True)

    user = db.relationship("User", foreign_keys=[user_id])

    def __repr__(self) -> str:
        return (f"<LMSignalSettings user={self.user_id} enabled={self.enabled} "
                f"delivery={self.delivery_enabled} conf>={self.min_confluence}>")


class LiveMonitorSignalAlert(db.Model):
    """One alert generated for one user about one setup, plus its outcome.

    Written when the funnel decides a setup is worth sending, BEFORE delivery
    is attempted — so a failed send leaves a record rather than losing it.
    Outcome fields are filled in later by the tracker.
    """
    __tablename__ = "live_monitor_signal_alerts"

    id      = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"),
                        nullable=False, index=True)

    # Stable dedup key: user + pair + direction + modules + candle bucket.
    alert_key = db.Column(db.String(80), nullable=False, unique=True, index=True)

    # ── What the setup is ────────────────────────────────────────────────────
    symbol     = db.Column(db.String(30), nullable=False, index=True)
    exchange   = db.Column(db.String(20), nullable=False, default="binance")
    direction  = db.Column(db.String(10), nullable=False)   # long | short
    timeframe  = db.Column(db.String(10), nullable=True)
    alert_kind = db.Column(db.String(20), nullable=False, default="in_zone")
    tier       = db.Column(db.String(2),  nullable=True)    # "A" | "B"

    # Which scan modules contributed, and how many agreed.
    modules_json     = db.Column(db.Text,    nullable=True)  # JSON list of keys
    confluence_count = db.Column(db.Integer, nullable=False, default=1)

    # ── Levels (AI-decided, validated deterministically) ─────────────────────
    zone_high     = db.Column(db.Float, nullable=True)
    zone_low      = db.Column(db.Float, nullable=True)
    entry_price      = db.Column(db.Float, nullable=True)
    stop_loss        = db.Column(db.Float, nullable=True)
    take_profit_1    = db.Column(db.Float, nullable=True)
    take_profit_2    = db.Column(db.Float, nullable=True)
    risk_reward      = db.Column(db.Float, nullable=True)
    price_at_alert   = db.Column(db.Float, nullable=True)

    # ── AI verdict ───────────────────────────────────────────────────────────
    ai_confidence = db.Column(db.Integer, nullable=True)     # 0-100
    ai_summary    = db.Column(db.Text,    nullable=True)     # short human rationale
    ai_model      = db.Column(db.String(80), nullable=True)
    evidence_json = db.Column(db.Text,    nullable=True)     # order-flow snapshot used

    # ── Delivery ─────────────────────────────────────────────────────────────
    # pending | sent | failed | suppressed | shadow
    delivery_status  = db.Column(db.String(20), nullable=False,
                                 default="pending", index=True)
    delivery_error   = db.Column(db.String(255), nullable=True)
    telegram_message_id = db.Column(db.String(40), nullable=True)
    sent_at          = db.Column(db.DateTime, nullable=True)

    # ── Outcome tracking ─────────────────────────────────────────────────────
    # pending | target_hit | stop_hit | invalidated | expired
    outcome         = db.Column(db.String(20), nullable=False,
                                default="pending", index=True)
    outcome_reason  = db.Column(db.String(60), nullable=True)
    outcome_price   = db.Column(db.Float,    nullable=True)
    outcome_at      = db.Column(db.DateTime, nullable=True)
    result_r        = db.Column(db.Float,    nullable=True)   # result in R multiples
    mfe_pct         = db.Column(db.Float,    nullable=True)   # best excursion
    mae_pct         = db.Column(db.Float,    nullable=True)   # worst excursion
    followup_sent_at = db.Column(db.DateTime, nullable=True)

    created_at = db.Column(db.DateTime,
                           default=lambda: datetime.now(timezone.utc),
                           nullable=False, index=True)
    updated_at = db.Column(db.DateTime,
                           default=lambda: datetime.now(timezone.utc),
                           onupdate=lambda: datetime.now(timezone.utc),
                           nullable=True)

    user = db.relationship("User", foreign_keys=[user_id])

    __table_args__ = (
        db.Index("ix_lm_sig_alert_user_created", "user_id", "created_at"),
        db.Index("ix_lm_sig_alert_user_symbol",  "user_id", "symbol", "created_at"),
        db.Index("ix_lm_sig_alert_open",         "outcome", "delivery_status"),
    )

    def __repr__(self) -> str:
        return (f"<LMSignalAlert user={self.user_id} {self.symbol} {self.direction} "
                f"tier={self.tier} {self.delivery_status}/{self.outcome}>")


class LiveMonitorSignalCandidate(db.Model):
    """A setup found by one scan module — the top of the signal funnel.

    GLOBAL, not per-user: a setup is detected once and every eligible user's
    settings are evaluated against it later. This is what makes adding users
    cheap, and what makes cross-module confluence possible at all — every
    scan tab writes here in one shape, so they can finally be compared.

    Deduplicated by `candidate_key`, which folds the detection time into its
    timeframe's candle bucket. Re-scanning the same candle updates
    `last_seen_at` instead of inserting a duplicate row.
    """
    __tablename__ = "live_monitor_signal_candidates"

    id            = db.Column(db.Integer, primary_key=True)
    candidate_key = db.Column(db.String(80), nullable=False, unique=True, index=True)

    # ── Origin ───────────────────────────────────────────────────────────────
    module    = db.Column(db.String(30), nullable=False, index=True)  # ob | bias_shift | …
    symbol    = db.Column(db.String(30), nullable=False, index=True)
    exchange  = db.Column(db.String(20), nullable=False, default="binance")
    market    = db.Column(db.String(20), nullable=False, default="perpetual")
    timeframe = db.Column(db.String(10), nullable=True)

    # ── Setup shape ──────────────────────────────────────────────────────────
    direction      = db.Column(db.String(10), nullable=False, default="neutral")
    zone_high      = db.Column(db.Float, nullable=True)
    zone_low       = db.Column(db.Float, nullable=True)
    detected_price = db.Column(db.Float, nullable=True)

    # Normalized 0-100 so modules with different native scales can be ranked
    # against each other. `grade` keeps the module's own label for display.
    score = db.Column(db.Integer, nullable=False, default=0, index=True)
    grade = db.Column(db.String(20), nullable=True)

    # Bounded module-specific extras, for the AI context and the UI.
    meta_json = db.Column(db.Text, nullable=True)

    # ── Lifecycle ────────────────────────────────────────────────────────────
    # active → the setup stands; expired → its candle window passed unused.
    status       = db.Column(db.String(20), nullable=False, default="active", index=True)
    detected_at  = db.Column(db.DateTime, nullable=False,
                             default=lambda: datetime.now(timezone.utc), index=True)
    last_seen_at = db.Column(db.DateTime, nullable=True)
    expires_at   = db.Column(db.DateTime, nullable=True, index=True)

    __table_args__ = (
        db.Index("ix_lm_cand_symbol_active", "symbol", "status", "detected_at"),
        db.Index("ix_lm_cand_module_active", "module", "status"),
    )

    def __repr__(self) -> str:
        return (f"<LMSignalCandidate {self.module}:{self.symbol} {self.direction} "
                f"score={self.score} {self.status}>")
