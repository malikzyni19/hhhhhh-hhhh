"""Phase 11.14 Hotfix — focused analytics unit tests.

Tests pure-Python functions only. No DB. No Flask. No network.
Run: python3 _test_paper_performance_11_14.py

Covers spec Tasks 1-10 correctness scenarios.
"""
import os, sys, types, importlib

# ── Minimal stubs so paper_performance.py can import cleanly ─────────────────
os.environ.setdefault("DATABASE_URL",    "sqlite:///:memory:")
os.environ.setdefault("SECRET_KEY",      "test")
os.environ.setdefault("RESEND_API_KEY",  "test")
os.environ.setdefault("TURNSTILE_SECRET","")

for _mn in ["psycopg2", "psycopg2.extras", "resend", "flask_login",
            "flask_sqlalchemy", "sqlalchemy", "sqlalchemy.orm",
            "models", "flask"]:
    if _mn not in sys.modules:
        m = types.ModuleType(_mn)
        sys.modules[_mn] = m

# Stub sqlalchemy.func so _sa_func.coalesce / lower / trim don't crash
_sa = sys.modules.get("sqlalchemy") or types.ModuleType("sqlalchemy")

class _FuncStub:
    def __getattr__(self, name):
        def _any(*a, **kw): return None
        return _any

_sa.func = _FuncStub()
sys.modules["sqlalchemy"] = _sa

sys.path.insert(0, os.path.dirname(__file__))

# ── Import only the pure-Python helpers ──────────────────────────────────────
import importlib.util, pathlib
_spec = importlib.util.spec_from_file_location(
    "paper_performance",
    pathlib.Path(__file__).parent / "live_monitor" / "paper_performance.py",
)
pp = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(pp)

from decimal import Decimal
from datetime import datetime, timezone, timedelta

# ── Trade mock ────────────────────────────────────────────────────────────────

class _MT:
    """Minimal trade mock."""
    _id_seq = 0
    def __init__(self, **kw):
        _MT._id_seq += 1
        self.id             = kw.pop("id",          _MT._id_seq)
        self.symbol         = kw.pop("symbol",       "BTCUSDT")
        self.side           = kw.pop("side",         "BUY")
        self.realized_pnl   = kw.pop("realized_pnl", None)
        self.outcome        = kw.pop("outcome",      None)
        self.outcome_reason = kw.pop("outcome_reason", None)
        self.realized_pnl_pct = kw.pop("realized_pnl_pct", None)
        self.risk_reward    = kw.pop("risk_reward",  None)
        self.duration_seconds = kw.pop("duration_seconds", None)
        self.status         = kw.pop("status",       "closed")
        self.position_id    = kw.pop("position_id",  self.id)
        self.created_at     = kw.pop("created_at",   datetime.now(timezone.utc))
        self.closed_at      = kw.pop("closed_at",    None)
        self.updated_at     = kw.pop("updated_at",   None)
        for k, v in kw.items():
            setattr(self, k, v)


# ── Helpers ───────────────────────────────────────────────────────────────────

PASS = FAIL = 0

def _check(label, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✅ {label}")
    else:
        FAIL += 1
        print(f"  ❌ {label}{' — ' + str(detail) if detail else ''}")

def _dec(s):
    return Decimal(s) if s is not None else None


# ═══════════════════════════════════════════════════════════════════════════════
# GROUP 1 — Canonical outcome classification
# ═══════════════════════════════════════════════════════════════════════════════
print("\n── GROUP 1: canonical outcome classifier ──────────────────────────────")

# 1-A: Missing PnL
t = _MT(realized_pnl=None)
can, pnl, mis, src = pp._lm_classify_paper_trade_outcome(t)
_check("1A missing PnL → canonical None", can is None and src == "missing_pnl")

# 1-B: Positive PnL, no outcome label → derived win
t = _MT(realized_pnl="50.00", outcome=None)
can, pnl, mis, src = pp._lm_classify_paper_trade_outcome(t)
_check("1B pnl>0, no label → derived win", can == "win" and not mis and src == "derived")

# 1-C: Negative PnL, no label → derived loss
t = _MT(realized_pnl="-30.00", outcome=None)
can, pnl, mis, src = pp._lm_classify_paper_trade_outcome(t)
_check("1C pnl<0, no label → derived loss", can == "loss" and not mis and src == "derived")

# 1-D: Positive PnL labelled 'win' → explicit, no mismatch
t = _MT(realized_pnl="20", outcome="win")
can, pnl, mis, src = pp._lm_classify_paper_trade_outcome(t)
_check("1D pnl>0 + label=win → explicit, no mismatch", can == "win" and not mis and src == "explicit")

# 1-E: Positive PnL labelled 'loss' → MISMATCH → canonical=win
t = _MT(realized_pnl="25.00", outcome="loss")
can, pnl, mis, src = pp._lm_classify_paper_trade_outcome(t)
_check("1E pnl>0 + label=loss → canonical=win, mismatch=True", can == "win" and mis and src == "pnl_override",
       f"got can={can} mis={mis} src={src}")

# 1-F: Negative PnL labelled 'win' → MISMATCH → canonical=loss
t = _MT(realized_pnl="-10.00", outcome="win")
can, pnl, mis, src = pp._lm_classify_paper_trade_outcome(t)
_check("1F pnl<0 + label=win → canonical=loss, mismatch=True", can == "loss" and mis and src == "pnl_override",
       f"got can={can} mis={mis} src={src}")

# 1-G: Zero PnL → breakeven
t = _MT(realized_pnl="0", outcome=None)
can, pnl, mis, src = pp._lm_classify_paper_trade_outcome(t)
_check("1G pnl=0 → breakeven", can == "breakeven")


# ═══════════════════════════════════════════════════════════════════════════════
# GROUP 2 — Core metrics: monetary aggregates use PnL sign
# ═══════════════════════════════════════════════════════════════════════════════
print("\n── GROUP 2: monetary aggregates (PnL sign, not label) ─────────────────")

# 2-A: Positive PnL labelled loss — monetary must count as profit
t1 = _MT(realized_pnl="100", outcome="loss")   # label=loss but pnl>0 → mismatch
t2 = _MT(realized_pnl="-40", outcome="loss")   # correct loss
trades = [t1, t2]
m = pp._compute_core_metrics(trades)
_check("2A gross_profit = sum(pnl>0) regardless of label",
       _dec(m["gross_profit"]) == Decimal("100"),
       f"got {m['gross_profit']}")
_check("2A gross_loss = abs(sum(pnl<0)) regardless of label",
       _dec(m["gross_loss"]) == Decimal("40"),
       f"got {m['gross_loss']}")
_check("2A net = gross_profit - gross_loss",
       _dec(m["net_realized_pnl"]) == Decimal("60"),
       f"got {m['net_realized_pnl']}")
_check("2A mismatch counted",
       m["data_quality"]["outcome_pnl_mismatch_count"] == 1)
_check("2A canonical win_count=1 (t1 pnl>0), loss_count=1 (t2 pnl<0)",
       m["win_count"] == 1 and m["loss_count"] == 1,
       f"w={m['win_count']} l={m['loss_count']}")

# 2-B: Negative PnL labelled win — monetary must count as loss
t3 = _MT(realized_pnl="-80", outcome="win")    # label=win but pnl<0 → mismatch
t4 = _MT(realized_pnl="50", outcome="win")     # correct win
trades = [t3, t4]
m = pp._compute_core_metrics(trades)
_check("2B gross_profit = 50 (only positive)",
       _dec(m["gross_profit"]) == Decimal("50"),
       f"got {m['gross_profit']}")
_check("2B gross_loss = 80 (t3 magnitude)",
       _dec(m["gross_loss"]) == Decimal("80"),
       f"got {m['gross_loss']}")
_check("2B mismatch_count=1", m["data_quality"]["outcome_pnl_mismatch_count"] == 1)

# 2-C: Gross profit = sum of all positive PnL values
t5 = _MT(realized_pnl="10", outcome="win")
t6 = _MT(realized_pnl="20", outcome="win")
t7 = _MT(realized_pnl="-5", outcome="loss")
m = pp._compute_core_metrics([t5, t6, t7])
_check("2C gross_profit = 30", _dec(m["gross_profit"]) == Decimal("30"), f"got {m['gross_profit']}")
_check("2C gross_loss = 5",    _dec(m["gross_loss"])   == Decimal("5"),  f"got {m['gross_loss']}")
_check("2C net = 25",          _dec(m["net_realized_pnl"]) == Decimal("25"), f"got {m['net_realized_pnl']}")

# 2-D: Average win / average loss
t8 = _MT(realized_pnl="100", outcome="win")
t9 = _MT(realized_pnl="200", outcome="win")
t10 = _MT(realized_pnl="-50", outcome="loss")
m = pp._compute_core_metrics([t8, t9, t10])
_check("2D average_win = 150", _dec(m["average_win"]) == Decimal("150"), f"got {m['average_win']}")
_check("2D average_loss = 50", _dec(m["average_loss"]) == Decimal("50"), f"got {m['average_loss']}")

# 2-E: Profit factor handles no losses
t11 = _MT(realized_pnl="50", outcome="win")
t12 = _MT(realized_pnl="30", outcome="win")
m = pp._compute_core_metrics([t11, t12])
_check("2E profit_factor None when no losses", m["profit_factor"] is None)
_check("2E profit_factor_reason=no_losses_in_sample", m["profit_factor_reason"] == "no_losses_in_sample")

# 2-F: PnL reconciliation always matches
for pnls in [["10", "-5", "20", "-8"], ["100"], ["-100"], ["0"]]:
    ts = [_MT(realized_pnl=p) for p in pnls]
    m = pp._compute_core_metrics(ts)
    _check(f"2F PnL reconciliation matches for {pnls}",
           m["pnl_sum_reconciliation"]["matches"],
           m.get("pnl_sum_reconciliation"))

# 2-G: Expectancy correctness
# win_rate=0.6, avg_win=100, avg_loss=50 → expectancy = 0.6*100 - 0.4*50 = 60-20=40
wins  = [_MT(realized_pnl="100") for _ in range(6)]
losses= [_MT(realized_pnl="-50") for _ in range(4)]
m = pp._compute_core_metrics(wins + losses)
exp = _dec(m["expectancy_amount"])
_check("2G expectancy = 0.6*100 - 0.4*50 = 40",
       exp is not None and abs(exp - Decimal("40")) < Decimal("0.0001"),
       f"got {exp}")


# ═══════════════════════════════════════════════════════════════════════════════
# GROUP 3 — Filter normalization
# ═══════════════════════════════════════════════════════════════════════════════
print("\n── GROUP 3: filter normalization ───────────────────────────────────────")

# 3-A: Side normalization — case-insensitive aliases
for alias, expected in [
    ("long","BUY"), ("buy","BUY"), ("bullish","BUY"),
    ("LONG","BUY"), ("BUY","BUY"),
    ("Bullish","BUY"), ("Long","BUY"), ("Buy","BUY"),  # mixed case
    ("short","SELL"), ("sell","SELL"), ("bearish","SELL"),
    ("SHORT","SELL"), ("SELL","SELL"),
    ("Bearish","SELL"), ("Short","SELL"),
]:
    f = pp._lm_build_paper_performance_filters(1, side=alias)
    _check(f"3A side '{alias}'→'{expected}'", f["side"] == expected and not f.get("_side_err"),
           f"got side={f['side']} err={f.get('_side_err')}")

# 3-B: Invalid side returns error
f = pp._lm_build_paper_performance_filters(1, side="up")
_check("3B unknown side returns error", f.get("_side_err") == "invalid_performance_filter",
       f"got {f.get('_side_err')}")

# 3-C: Safe item_id parsing — no ValueError
for bad in ["abc", "", "0", "-5", "1.5", []]:
    try:
        f = pp._lm_build_paper_performance_filters(1, item_id=bad)
        # All of these should safely return iid=None (with or without an error flag)
        # without raising an exception
        _check(f"3C item_id={bad!r} → no crash, iid=None",
               f["item_id"] is None,
               f"err={f.get('_item_id_err')} iid={f['item_id']}")
    except Exception as e:
        FAIL += 1
        print(f"  ❌ 3C item_id={bad!r} raised: {e}")

# None is absent (not a parse error)
f_none = pp._lm_build_paper_performance_filters(1, item_id=None)
_check("3C item_id=None → iid=None, no error", f_none["item_id"] is None and not f_none.get("_item_id_err"))

# 3-C2: Valid item_id
f = pp._lm_build_paper_performance_filters(1, item_id=42)
_check("3C2 valid item_id=42", f["item_id"] == 42 and not f.get("_item_id_err"))

f = pp._lm_build_paper_performance_filters(1, item_id="123")
_check("3C3 string item_id='123'→123", f["item_id"] == 123)

# 3-D: Period normalization
_check("3D '30d' valid", pp._lm_normalize_performance_period("30d") == "30d")
_check("3D 'all' valid", pp._lm_normalize_performance_period("all") == "all")
_check("3D 'bad' → default '30d'", pp._lm_normalize_performance_period("bad") == "30d")
_check("3D None → default '30d'", pp._lm_normalize_performance_period(None) == "30d")


# ═══════════════════════════════════════════════════════════════════════════════
# GROUP 4 — Streaks use canonical outcome
# ═══════════════════════════════════════════════════════════════════════════════
print("\n── GROUP 4: streaks ────────────────────────────────────────────────────")

now = datetime.now(timezone.utc)
# 3 wins then 2 losses — current_loss_streak=2, max_win=3
streak_trades = [
    _MT(realized_pnl="10", created_at=now-timedelta(days=5)),
    _MT(realized_pnl="10", created_at=now-timedelta(days=4)),
    _MT(realized_pnl="10", created_at=now-timedelta(days=3)),
    _MT(realized_pnl="-5", created_at=now-timedelta(days=2)),
    _MT(realized_pnl="-5", created_at=now-timedelta(days=1)),
]
s = pp._compute_streaks(streak_trades)
_check("4A current_loss_streak=2", s["current_loss_streak"] == 2, s)
_check("4A max_win_streak=3",      s["max_win_streak"] == 3,      s)
_check("4A current_win_streak=0",  s["current_win_streak"] == 0,  s)

# Streak with label mismatch — negative pnl labelled "win" should still be a loss
mismatch_trades = [
    _MT(realized_pnl="10",  outcome="win",  created_at=now-timedelta(days=3)),
    _MT(realized_pnl="-5",  outcome="win",  created_at=now-timedelta(days=2)),  # mismatch → loss
    _MT(realized_pnl="-3",  outcome="loss", created_at=now-timedelta(days=1)),
]
s2 = pp._compute_streaks(mismatch_trades)
_check("4B mismatch: last 2 are canonical losses → current_loss_streak=2",
       s2["current_loss_streak"] == 2, s2)


# ═══════════════════════════════════════════════════════════════════════════════
# GROUP 5 — Drawdown uses realized PnL, initial peak=0
# ═══════════════════════════════════════════════════════════════════════════════
print("\n── GROUP 5: drawdown ───────────────────────────────────────────────────")

# First trade is a loss → drawdown from 0
raw_loss_first = [
    {"trade_id": 1, "_c": Decimal("-10")},
    {"trade_id": 2, "_c": Decimal("5")},
]
dd = pp._compute_drawdown(raw_loss_first)
_check("5A first trade is loss → drawdown=10 from peak=0",
       _dec(dd["max_drawdown_amount"]) == Decimal("10"),
       f"got {dd['max_drawdown_amount']}")

# No loss → drawdown=0
raw_no_loss = [{"trade_id": 1, "_c": Decimal("10")}, {"trade_id": 2, "_c": Decimal("20")}]
dd2 = pp._compute_drawdown(raw_no_loss)
_check("5B no drawdown → max_drawdown_amount='0'",
       _dec(dd2["max_drawdown_amount"]) == Decimal("0"))

# 5-C: drawdown_from_trades excludes invalid PnL
t_valid   = _MT(realized_pnl="100")
t_invalid = _MT(realized_pnl=None)   # skipped
dd3 = pp._compute_drawdown_from_trades([t_valid, t_invalid])
_check("5C drawdown_from_trades excludes None PnL", dd3["max_drawdown_amount"] == "0")


# ═══════════════════════════════════════════════════════════════════════════════
# GROUP 6 — Data quality counters
# ═══════════════════════════════════════════════════════════════════════════════
print("\n── GROUP 6: data quality counters ──────────────────────────────────────")

dq_trades = [
    _MT(realized_pnl="50",  outcome="win"),                        # explicit
    _MT(realized_pnl="-20", outcome=None),                         # derived
    _MT(realized_pnl="30",  outcome="loss"),                       # mismatch (pnl>0, label=loss)
    _MT(realized_pnl=None),                                        # missing PnL (skipped in metrics)
]
# missing_pnl_db comes from extra_dq in practice; here we test what _compute_core_metrics tracks
m = pp._compute_core_metrics(dq_trades[:3])  # exclude None PnL one
dq = m["data_quality"]
_check("6A explicit_outcome_count=1", dq["explicit_outcome_count"] == 1, dq)
_check("6B derived_outcome_count=1",  dq["derived_outcome_count"] == 1,  dq)
_check("6C mismatch_count=1",         dq["outcome_pnl_mismatch_count"] == 1, dq)

# Verify missing_rr tracked
t_no_rr = _MT(realized_pnl="10", risk_reward=None)
m2 = pp._compute_core_metrics([t_no_rr])
_check("6D missing_risk_reward_count=1",
       m2["data_quality"]["missing_risk_reward_count"] == 1,
       m2["data_quality"])


# ═══════════════════════════════════════════════════════════════════════════════
# GROUP 7 — Timestamp helpers
# ═══════════════════════════════════════════════════════════════════════════════
print("\n── GROUP 7: timestamp priority (closed_at > updated_at > created_at) ──")

now_utc = datetime.now(timezone.utc)
old     = now_utc - timedelta(days=90)
mid     = now_utc - timedelta(days=15)
new     = now_utc - timedelta(days=1)

# closed_at wins
t = _MT(created_at=old, updated_at=mid, closed_at=new)
_check("7A closed_at wins over others", pp._ts(t) == new)

# updated_at wins when closed_at absent
t2 = _MT(created_at=old, updated_at=mid, closed_at=None)
_check("7B updated_at wins when no closed_at", pp._ts(t2) == mid)

# created_at is the fallback
t3 = _MT(created_at=old, updated_at=None, closed_at=None)
_check("7C created_at is fallback", pp._ts(t3) == old)


# ═══════════════════════════════════════════════════════════════════════════════
# GROUP 8 — Guardrails never change
# ═══════════════════════════════════════════════════════════════════════════════
print("\n── GROUP 8: guardrails ─────────────────────────────────────────────────")

g = pp._GUARDRAILS
_check("8A can_auto_submit=False",        g["can_auto_submit"] == False)
_check("8B auto_execution_allowed=False", g["auto_execution_allowed"] == False)
_check("8C ai_can_execute=False",         g["ai_can_execute"] == False)
_check("8D live_disabled=True",           g["live_disabled"] == True)
_check("8E read_only=True",               g["read_only"] == True)


# ═══════════════════════════════════════════════════════════════════════════════
# GROUP 9 — Sample quality warnings
# ═══════════════════════════════════════════════════════════════════════════════
print("\n── GROUP 9: sample quality warnings ───────────────────────────────────")

# Small sample → small_sample_size
m_small = pp._compute_core_metrics([_MT(realized_pnl="10") for _ in range(5)])
sq_small = pp._compute_sample_quality(m_small, [], query_meta=None)
_check("9A small sample → 'small_sample_size' warning",
       "small_sample_size" in sq_small["warnings"])
_check("9A sample_quality='insufficient'", sq_small["sample_quality"] == "insufficient")

# Truncated → analytics_row_limit_reached
m_trunc = pp._compute_core_metrics([_MT(realized_pnl="10") for _ in range(50)])
sq_trunc = pp._compute_sample_quality(m_trunc, [], query_meta={"truncated": True})
_check("9B truncated → analytics_row_limit_reached",
       "analytics_row_limit_reached" in sq_trunc["warnings"])

# Mismatch → outcome_pnl_mismatch_detected
t_mm = _MT(realized_pnl="-5", outcome="win")  # mismatch
m_mm = pp._compute_core_metrics([t_mm])
sq_mm = pp._compute_sample_quality(m_mm, [], query_meta=None)
_check("9C mismatch → outcome_pnl_mismatch_detected",
       "outcome_pnl_mismatch_detected" in sq_mm["warnings"])


# ═══════════════════════════════════════════════════════════════════════════════
# GROUP 10 — Breakdown monetary correctness
# ═══════════════════════════════════════════════════════════════════════════════
print("\n── GROUP 10: breakdown monetary correctness ────────────────────────────")

now2 = datetime.now(timezone.utc)
bd_trades = [
    _MT(symbol="BTCUSDT", realized_pnl="100", outcome="loss",   # mismatch — pnl>0 counts as profit
        created_at=now2-timedelta(days=2)),
    _MT(symbol="BTCUSDT", realized_pnl="-40", outcome="win",    # mismatch — pnl<0 counts as loss
        created_at=now2-timedelta(days=1)),
    _MT(symbol="ETHUSDT", realized_pnl="20",  outcome="win",
        created_at=now2),
]
bd = pp._lm_build_paper_performance_breakdowns(bd_trades, "30d")
btc = next((x for x in bd["symbols"] if x["symbol"] == "BTCUSDT"), None)
eth = next((x for x in bd["symbols"] if x["symbol"] == "ETHUSDT"), None)

_check("10A BTCUSDT gross_profit=100 (not corrupted by label)",
       btc is not None and _dec(btc.get("gross_profit")) == Decimal("100"),
       f"got {btc}")
_check("10B BTCUSDT gross_loss=40 (not corrupted by label)",
       btc is not None and _dec(btc.get("gross_loss")) == Decimal("40"),
       f"got {btc}")
_check("10C BTCUSDT wins=1 (canonical: pnl>0), losses=1 (pnl<0)",
       btc is not None and btc["wins"] == 1 and btc["losses"] == 1,
       f"got {btc}")
_check("10D ETH only 1 win, gross_profit=20",
       eth is not None and eth["wins"] == 1 and _dec(eth["gross_profit"]) == Decimal("20"),
       f"got {eth}")


# ═══════════════════════════════════════════════════════════════════════════════
# GROUP 11 — Safety: no execution helpers called
# ═══════════════════════════════════════════════════════════════════════════════
print("\n── GROUP 11: safety search ─────────────────────────────────────────────")

import re
src_text = open(
    os.path.join(os.path.dirname(__file__), "live_monitor", "paper_performance.py")
).read()

FORBIDDEN = [
    r"_lm_submit_paper_order",
    r"_lm_process_paper_fills",
    r"_lm_process_all_paper_fills",
    r"_lm_process_paper_exits",
    r"_lm_process_all_paper_exits",
    r"_lm_sync_paper_trade_journal",
    r"db\.session\.add\b",
    r"db\.session\.commit\b",
    r"snapshot_json\s*=",
    r"setInterval",
    r"place_order",
    r"binance.*api",
    r"api_key",
]
for pat in FORBIDDEN:
    found = re.search(pat, src_text, re.IGNORECASE)
    _check(f"11 no '{pat}' in paper_performance.py", not found,
           f"FOUND: {found.group()!r}" if found else "")


# ═══════════════════════════════════════════════════════════════════════════════
# GROUP 12 — Final Contract Corrections (hotfix)
# ═══════════════════════════════════════════════════════════════════════════════
print("\n── GROUP 12: final contract corrections ────────────────────────────────")

# ── 12A: Strict symbol filter validation ──────────────────────────────────────

f_valid = pp._lm_build_paper_performance_filters(1, symbol="BTCUSDT")
_check("12A1 valid symbol accepted", f_valid["symbol"] == "BTCUSDT" and not f_valid.get("_symbol_err"))

f_special = pp._lm_build_paper_performance_filters(1, symbol="BTC!USDT")
_check("12A2 symbol with ! → invalid_symbol",
       f_special.get("_symbol_err") == "invalid_symbol" and f_special["symbol"] is None)

f_long = pp._lm_build_paper_performance_filters(1, symbol="X" * 31)
_check("12A3 symbol > 30 chars → invalid_symbol",
       f_long.get("_symbol_err") == "invalid_symbol" and f_long["symbol"] is None)

f_empty = pp._lm_build_paper_performance_filters(1, symbol="")
_check("12A4 empty symbol → no error and no filter",
       not f_empty.get("_symbol_err") and f_empty["symbol"] is None)

f_ws = pp._lm_build_paper_performance_filters(1, symbol="   ")
_check("12A5 whitespace-only symbol → no error and no filter",
       not f_ws.get("_symbol_err") and f_ws["symbol"] is None)

f_lower = pp._lm_build_paper_performance_filters(1, symbol="btcusdt")
_check("12A6 lowercase symbol normalised to uppercase", f_lower["symbol"] == "BTCUSDT")

f_slash = pp._lm_build_paper_performance_filters(1, symbol="BTC/USDT")
_check("12A7 slash in symbol accepted", f_slash["symbol"] == "BTC/USDT" and not f_slash.get("_symbol_err"))

f_colon = pp._lm_build_paper_performance_filters(1, symbol="BTC:USDT")
_check("12A8 colon in symbol accepted", f_colon["symbol"] == "BTC:USDT" and not f_colon.get("_symbol_err"))

f_30 = pp._lm_build_paper_performance_filters(1, symbol="A" * 30)
_check("12A9 exactly 30 chars accepted", f_30["symbol"] == "A" * 30 and not f_30.get("_symbol_err"))

# ── 12B: Zero-drawdown must not fabricate a percentage ────────────────────────

# Flat equity series — no drawdown
flat_raw = [{"trade_id": i, "_c": Decimal("10")} for i in range(5)]
dd_flat  = pp._compute_drawdown(flat_raw)
_check("12B1 zero drawdown → max_drawdown_pct is None",
       dd_flat["max_drawdown_pct"] is None,
       f"got {dd_flat['max_drawdown_pct']!r}")
_check("12B2 zero drawdown → drawdown_pct_reason is period_start_equity_unavailable",
       dd_flat["drawdown_pct_reason"] == "period_start_equity_unavailable",
       f"got {dd_flat['drawdown_pct_reason']!r}")
_check("12B3 zero drawdown → recovered=True", dd_flat["recovered"] is True)

# Ascending equity — also no drawdown
asc_raw = [{"trade_id": i, "_c": Decimal(str(i * 10))} for i in range(10)]
dd_asc  = pp._compute_drawdown(asc_raw)
_check("12B4 ascending series → max_drawdown_pct is None",
       dd_asc["max_drawdown_pct"] is None,
       f"got {dd_asc['max_drawdown_pct']!r}")

# Non-zero drawdown still returns None pct (no denominator)
raw_dd = [
    {"trade_id": 1, "_c": Decimal("0")},
    {"trade_id": 2, "_c": Decimal("100")},
    {"trade_id": 3, "_c": Decimal("50")},
]
dd_nonzero = pp._compute_drawdown(raw_dd)
_check("12B5 non-zero drawdown → amount is correct",
       dd_nonzero["max_drawdown_amount"] == "50",
       f"got {dd_nonzero['max_drawdown_amount']!r}")
_check("12B6 non-zero drawdown → max_drawdown_pct still None",
       dd_nonzero["max_drawdown_pct"] is None,
       f"got {dd_nonzero['max_drawdown_pct']!r}")

# ── 12C: Equity curve hard cap ≤ 500 points ───────────────────────────────────

import random
rng = random.Random(42)
big_pts = [{"trade_id": i, "_c": Decimal(str(rng.gauss(0, 100))), "realized_pnl": "1"} for i in range(1000)]
# Assign _c as running cumulative
running_c = Decimal("0")
for pt in big_pts:
    pt["_c"] = running_c + Decimal(str(rng.gauss(0, 10)))
    running_c = pt["_c"]

ds_1000 = pp._downsample_curve(big_pts)
_check("12C1 1000 points downsampled to ≤ 500",
       len(ds_1000) <= 500,
       f"got {len(ds_1000)}")
_check("12C2 first point preserved", ds_1000[0] is big_pts[0])
_check("12C3 last point preserved",  ds_1000[-1] is big_pts[-1])

# Edge case: exactly 500 → pass through unchanged
pts_500 = [{"trade_id": i, "_c": Decimal(str(i))} for i in range(500)]
ds_500  = pp._downsample_curve(pts_500)
_check("12C4 exactly 500 points → unchanged", len(ds_500) == 500 and ds_500 is pts_500)

# Edge case: 501 points → downsample triggers, still ≤ 500
pts_501 = [{"trade_id": i, "_c": Decimal(str(i))} for i in range(501)]
ds_501  = pp._downsample_curve(pts_501)
_check("12C5 501 points → ≤ 500 after downsample", len(ds_501) <= 500, f"got {len(ds_501)}")

# ── 12D: State getter returns invalid_performance_filter for bad symbol ────────
# After refactoring, _lm_get_paper_performance_state calls
# _lm_query_closed_paper_trades_from_filters (not the old public wrapper).
# Mock the internal query to avoid DB access; validation returns before it for bad symbols.

_EMPTY_QMETA = {"row_limit": 5000, "total_available": 0, "rows_loaded": 0,
                "truncated": False, "missing_pnl_db": 0, "invalid_pnl_c": 0}

_orig_qtf = pp._lm_query_closed_paper_trades_from_filters

def _mock_qtf(user_id, filters):
    # Replicate the defensive guard so the mock behaves correctly
    errs = pp._lm_validate_paper_performance_filters(filters)
    if errs:
        raise ValueError(f"unvalidated_performance_filters:{list(errs.keys())}")
    return [], dict(_EMPTY_QMETA)

pp._lm_query_closed_paper_trades_from_filters = _mock_qtf

state_bad_sym = pp._lm_get_paper_performance_state(1, symbol="BTC!!!")
_check("12D1 bad symbol → ok=False", state_bad_sym.get("ok") is False)
_check("12D2 bad symbol → error=invalid_performance_filter",
       state_bad_sym.get("error") == "invalid_performance_filter")
_check("12D3 bad symbol → field_errors.symbol=invalid_symbol",
       (state_bad_sym.get("field_errors") or {}).get("symbol") == "invalid_symbol")
_check("12D4 bad symbol → guardrails present",
       "guardrails" in state_bad_sym)
_check("12D5 bad symbol → can_auto_submit always False",
       state_bad_sym.get("guardrails", {}).get("can_auto_submit") is False)

state_good_sym = pp._lm_get_paper_performance_state(1, symbol="BTCUSDT")
_check("12D6 good symbol → no filter error (passes to no-trade response)",
       state_good_sym.get("ok") is True and state_good_sym.get("error") is None)

pp._lm_query_closed_paper_trades_from_filters = _orig_qtf   # restore

# ── 12E: Comparison trend_reason propagated ───────────────────────────────────
# Test _compute_trend directly for insufficient data case
t_small = {"trade_count": 3, "win_rate_pct": "60", "net_realized_pnl": "100",
           "profit_factor": "1.5", "expectancy_amount": "10"}
trend_small = pp._compute_trend(t_small, t_small)
_check("12E1 <5 trades → insufficient_data", trend_small == "insufficient_data")

t_enough = {"trade_count": 10, "win_rate_pct": "70", "net_realized_pnl": "200",
            "profit_factor": "2.0", "expectancy_amount": "20"}
t_worse  = {"trade_count": 10, "win_rate_pct": "40", "net_realized_pnl": "50",
            "profit_factor": "0.8", "expectancy_amount": "5"}
trend_imp = pp._compute_trend(t_enough, t_worse)
_check("12E2 improving metrics → improving", trend_imp == "improving")

trend_det = pp._compute_trend(t_worse, t_enough)
_check("12E3 deteriorating metrics → deteriorating", trend_det == "deteriorating")


# ═══════════════════════════════════════════════════════════════════════════════
# GROUP 13 — Pre-query validation: no DB access for invalid filters
#
# Each test installs a sentinel on _lm_query_closed_paper_trades_from_filters
# that sets called["query"]=True if the DB path is ever reached. Invalid
# filters must return before the sentinel fires.
# ═══════════════════════════════════════════════════════════════════════════════
print("\n── GROUP 13: invalid filters must not reach DB query ───────────────────")

def _make_sentinel():
    tracker = {"called": False}
    _saved  = pp._lm_query_closed_paper_trades_from_filters

    def _sentinel(user_id, filters):
        tracker["called"] = True
        return [], dict(_EMPTY_QMETA)

    pp._lm_query_closed_paper_trades_from_filters = _sentinel
    return tracker, _saved

def _restore_sentinel(saved):
    pp._lm_query_closed_paper_trades_from_filters = saved

# 13-1: Invalid symbol characters — query must NOT be called
tracker, saved = _make_sentinel()
r = pp._lm_get_paper_performance_state(1, symbol="BTC!!!", symbol_supplied=True)
_restore_sentinel(saved)
_check("13-1 invalid symbol chars → ok=False",       r.get("ok") is False)
_check("13-1 invalid symbol chars → no DB query",    not tracker["called"])
_check("13-1 invalid symbol chars → correct error",  r.get("error") == "invalid_performance_filter")
_check("13-1 field_errors.symbol = invalid_symbol",  (r.get("field_errors") or {}).get("symbol") == "invalid_symbol")

# 13-2: Symbol longer than 30 chars — query must NOT be called
tracker, saved = _make_sentinel()
r = pp._lm_get_paper_performance_state(1, symbol="B" * 31, symbol_supplied=True)
_restore_sentinel(saved)
_check("13-2 symbol >30 chars → ok=False",    r.get("ok") is False)
_check("13-2 symbol >30 chars → no DB query", not tracker["called"])

# 13-3: Explicit blank symbol — query must NOT be called
tracker, saved = _make_sentinel()
r = pp._lm_get_paper_performance_state(1, symbol="", symbol_supplied=True)
_restore_sentinel(saved)
_check("13-3 explicit blank symbol → ok=False",    r.get("ok") is False)
_check("13-3 explicit blank symbol → no DB query", not tracker["called"])
_check("13-3 explicit blank → field_errors.symbol", (r.get("field_errors") or {}).get("symbol") == "invalid_symbol")

# 13-4: Whitespace-only symbol — query must NOT be called
tracker, saved = _make_sentinel()
r = pp._lm_get_paper_performance_state(1, symbol="   ", symbol_supplied=True)
_restore_sentinel(saved)
_check("13-4 whitespace-only symbol → ok=False",    r.get("ok") is False)
_check("13-4 whitespace-only symbol → no DB query", not tracker["called"])
_check("13-4 whitespace-only → field_errors.symbol", (r.get("field_errors") or {}).get("symbol") == "invalid_symbol")

# 13-5: Invalid side — query must NOT be called
tracker, saved = _make_sentinel()
r = pp._lm_get_paper_performance_state(1, side="diagonal")
_restore_sentinel(saved)
_check("13-5 invalid side → ok=False",    r.get("ok") is False)
_check("13-5 invalid side → no DB query", not tracker["called"])
_check("13-5 invalid side → field_errors.side", (r.get("field_errors") or {}).get("side") is not None)

# 13-6: Invalid item_id — query must NOT be called
tracker, saved = _make_sentinel()
r = pp._lm_get_paper_performance_state(1, item_id="abc")
_restore_sentinel(saved)
_check("13-6 invalid item_id → ok=False",    r.get("ok") is False)
_check("13-6 invalid item_id → no DB query", not tracker["called"])

# 13-7: Omitted symbol (symbol_supplied=False, symbol=None) — query MUST be called
tracker, saved = _make_sentinel()
r = pp._lm_get_paper_performance_state(1, symbol=None, symbol_supplied=False)
_restore_sentinel(saved)
_check("13-7 omitted symbol → ok=True",       r.get("ok") is True)
_check("13-7 omitted symbol → DB query called", tracker["called"])

# 13-8: Valid lowercase symbol (symbol_supplied=True) — normalized, query IS called
tracker, saved = _make_sentinel()
r = pp._lm_get_paper_performance_state(1, symbol="btcusdt", symbol_supplied=True)
_restore_sentinel(saved)
_check("13-8 valid lowercase symbol → ok=True",   r.get("ok") is True)
_check("13-8 valid lowercase symbol → DB called",  tracker["called"])
_check("13-8 valid lowercase → normalized in filters",
       (r.get("filters") or {}).get("symbol") == "BTCUSDT")

# 13-9: Validated filters are NOT rebuilt inside the query helper
# Verify that the filter object passed to the query carries the same timestamp as built
tracker, saved = _make_sentinel()
_captured = {}
def _capture_sentinel(user_id, filters):
    _captured["filters"] = dict(filters)
    tracker["called"] = True
    return [], dict(_EMPTY_QMETA)
pp._lm_query_closed_paper_trades_from_filters = _capture_sentinel
r = pp._lm_get_paper_performance_state(1, symbol="ETHUSDT", symbol_supplied=True, period="7d")
pp._lm_query_closed_paper_trades_from_filters = saved
_check("13-9 filters passed to query match built filters",
       _captured.get("filters", {}).get("symbol") == "ETHUSDT"
       and _captured.get("filters", {}).get("period") == "7d")
_check("13-9 filters not rebuilt — same object shape",
       "_from_dt" in _captured.get("filters", {}))

# 13-10: Invalid filter → account summary helper NOT called
# _build_account_context calls _lm_get_paper_account_summary — stub it
_acct_called = {"called": False}
_orig_acct = pp._build_account_context
def _acct_sentinel(uid):
    _acct_called["called"] = True
    return {}
pp._build_account_context = _acct_sentinel

tracker, saved = _make_sentinel()
r = pp._lm_get_paper_performance_state(1, symbol="INVALID!!!", symbol_supplied=True)
_restore_sentinel(saved)
pp._build_account_context = _orig_acct
_check("13-10 invalid filter → account context not called", not _acct_called["called"])

# ── 13-A: symbol_supplied flag contract via _lm_build_paper_performance_filters ──

# symbol_supplied=False + symbol=None → no filter, no error (omitted)
f_omit = pp._lm_build_paper_performance_filters(1, symbol=None, symbol_supplied=False)
_check("13-A1 omitted symbol → no _symbol_err and sym=None",
       f_omit["_symbol_err"] is None and f_omit["symbol"] is None)

# symbol_supplied=True + symbol="" → invalid_symbol
f_blank = pp._lm_build_paper_performance_filters(1, symbol="", symbol_supplied=True)
_check("13-A2 supplied blank → _symbol_err=invalid_symbol",
       f_blank["_symbol_err"] == "invalid_symbol")

# symbol_supplied=True + symbol="   " → invalid_symbol
f_ws = pp._lm_build_paper_performance_filters(1, symbol="   ", symbol_supplied=True)
_check("13-A3 supplied whitespace → _symbol_err=invalid_symbol",
       f_ws["_symbol_err"] == "invalid_symbol")

# symbol_supplied=True + valid symbol → normalized, no error
f_valid = pp._lm_build_paper_performance_filters(1, symbol="eth-usdt", symbol_supplied=True)
_check("13-A4 supplied valid → sym normalized, no error",
       f_valid["symbol"] == "ETH-USDT" and f_valid["_symbol_err"] is None)

# symbol_supplied=True + HTML → invalid_symbol
f_html = pp._lm_build_paper_performance_filters(1, symbol="<script>", symbol_supplied=True)
_check("13-A5 supplied HTML → invalid_symbol", f_html["_symbol_err"] == "invalid_symbol")

# symbol_supplied=True + internal space → invalid_symbol
f_space = pp._lm_build_paper_performance_filters(1, symbol="BTC USDT", symbol_supplied=True)
_check("13-A6 supplied internal space → invalid_symbol", f_space["_symbol_err"] == "invalid_symbol")

# symbol_supplied=True + length exactly 30 → valid
f_30 = pp._lm_build_paper_performance_filters(1, symbol="A" * 30, symbol_supplied=True)
_check("13-A7 supplied 30-char symbol → valid", f_30["symbol"] == "A" * 30)

# symbol_supplied=True + length 31 → invalid_symbol
f_31 = pp._lm_build_paper_performance_filters(1, symbol="A" * 31, symbol_supplied=True)
_check("13-A8 supplied 31-char symbol → invalid_symbol", f_31["_symbol_err"] == "invalid_symbol")

# ── 13-B: _lm_validate_paper_performance_filters helper ──────────────────────

errs_none = pp._lm_validate_paper_performance_filters(
    {"_symbol_err": None, "_side_err": None, "_item_id_err": None})
_check("13-B1 all None errors → empty dict", errs_none == {})

errs_sym = pp._lm_validate_paper_performance_filters(
    {"_symbol_err": "invalid_symbol", "_side_err": None, "_item_id_err": None})
_check("13-B2 symbol error → field_errors.symbol", errs_sym == {"symbol": "invalid_symbol"})

errs_multi = pp._lm_validate_paper_performance_filters(
    {"_symbol_err": "invalid_symbol", "_side_err": "invalid_performance_filter",
     "_item_id_err": "must_be_positive_integer"})
_check("13-B3 all three errors → all three keys",
       set(errs_multi.keys()) == {"symbol", "side", "item_id"})


# ═══════════════════════════════════════════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════════════════════════════════════════
print(f"\n{'='*60}")
print(f"  Results: {PASS} passed, {FAIL} failed")
print(f"{'='*60}")
sys.exit(0 if FAIL == 0 else 1)
