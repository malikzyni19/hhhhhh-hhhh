# ZYNI Research Roadmap — Frozen 2026-07-17

Research-only program. Nothing in these phases touches Scanner, Live Monitor,
alerts, paper trading, automation, or execution. All engines are
server-authoritative and deterministic; no DB writes; sample-size gates on
every reported aggregate; TV OB% stays parked as optional exchange-specific
metadata with no threshold recommendation.

## Phase 17 — Detection Parity + Dual-Class Backtest  ✅ (this commit)
- Backtest internal pivot lookback fixed 3 → 5 to match production scanner
  `iLen=5` (the old value matched neither production internal nor swing).
- Parity check now compares against `detect_obs_all(i_len=5, s_len=30)` —
  the REAL production parameters (previously 3/3, which validated the replay
  against itself rather than against production).
- New swing OB research class: same canonical BOS/zone-extraction/outcome
  rules with 30-length pivots (production `sLen=30`). Every event tagged
  `ob_class: internal | swing` + `pivot_len`.
- Main backtest runs BOTH classes over the full selected candle range in one
  request (`ob_class_mode: "both"` default; `"internal"` skips the swing
  pass). Results reported separately — never pooled. UI class toggle
  (Internal / Swing) on the backtest results pages.
- Production has NO swing-OB detection (in `detect_obs`, `s_len` only sets
  the scan start offset), so the swing class is a new research definition,
  not a parity target.

## Phase 18 — Autopsy Agent (deterministic; no AI API for the core)
- 18A: reason-code engine — per-trade autopsy cards
  (`AGAINST_HTF_TREND`, `INSTANT_MITIGATION`, `LATE_TOUCH`,
  `WEAK_DISPLACEMENT`, `OVERSIZED_ZONE`, `STALE_ZONE`, `BAD_SESSION`,
  `COUNTER_SWING`)
- 18B: loser-vs-winner reason ranking (frequency lift per factor, per class —
  full sample, all losers vs all winners, no 35-trade caps)
- 18C: setup-profile table — win rate / expectancy / PF at user-selected RR,
  sample-size labeled, sorted by win ratio
- 18D (optional, last): AI narrative layer reusing the Phase 11.15
  schema-validated advisory pattern — numbers always deterministic, the LLM
  only writes the summary text

## Phase 19 — Feature Enrichment + Alignment Matrix
- Canonical trend = HH/HL pivot structure on 1h / 4h / 1d
- Alignment matrix: 15m OB→{1h,4h}, 1h OB→{4h,1d}, 4h OB→{1d} (no 5m here)
- Features prioritized by Phase 18 autopsy findings; both OB classes

## Phase 20 — Bad-Trade Filter Lab  (REVISED 2026-07-18 by user decision)
Goal: make the ORDER BLOCK module itself better by identifying and filtering
the bad trades — before any new zone types or entry modules are added.
- Run the Autopsy Agent (18) + Alignment Matrix (19) on real data to surface
  the dominant loss patterns (reason ranking, failure modes, matrix edges)
- Turn the strongest loss patterns into explicit, deterministic FILTER
  CANDIDATES (e.g. "exclude against-1d-trend trades", "exclude touch 3+",
  "exclude oversized zones") — each defined as a rule over existing features
- Measure every candidate the honest way: baseline vs filtered comparison
  (same-RR, trade retention, expectancy/PF/net-R delta, sample-size gates) —
  reusing the Phase 16 comparison contract
- Combine surviving candidates into a "refined OB module" filter set and
  report combined-vs-baseline performance per OB class
- Research only — filters are NOT activated anywhere in production

## Phase 21 — Pass-Profile Walk-Forward Validation  ✅ (implemented)
- New endpoint POST /api/backtest/ob-historical/profile-walk-forward + a
  "Profile WF" tab in Backtest > Compare.
- Chronological expanding-anchor folds (reuses _bt_build_walk_forward_folds
  and _bt_wf_bootstrap_delta). A trade is train/test by its touch time vs the
  fold boundary; autopsy features are causal (<= touch bar), so partitioning
  full-cell records equals prefix-slicing — no look-ahead.
- Two modes: locked (a fixed profile of Chunk-20B rule ids, ANDed, applied to
  every fold's test window) and train_selected (per fold the single best rule
  is chosen on TRAIN data only, then applied to TEST — true OOS selection).
- Verdict PASS/FAIL/INSUFFICIENT via gates: >= min folds & OOS pass trades,
  majority of folds beat baseline, OOS expectancy above baseline, AND the
  bootstrap 95% CI lower bound on the fold delta > 0.
- Research only; labels; no production activation. A PASS is out-of-sample
  evidence, not a guarantee.

## Phase 20B — OB Respect Lab  (FROZEN 2026-07-19 by user decision)
Implemented in verified chunks — each chunk passes an adversarial
verification agent before the next begins.

Chunk 1 — TV% removal (full sweep):
- Stability Lab + Walk-Forward tabs removed from the Compare UI (both exist
  only for the TV% question, answered: no edge). TV% filter/export/sections
  stripped from Overview and Trade Explorer. TV% computation OFF by default
  in the backtest runner (faster runs). Backend + tests stay dormant for
  Phase 21 machinery reuse.

Chunk 2 — Respect framing:
- Grades relabeled: Respected (clean/stressed win) / Partial (reversal
  loss, paused+) / Not-Respected (hard loss, instant mitigation)
- First-touch default view; touch 2/3 comparison ("which OBs also work on
  the 2nd/3rd touch")
- HTF joint-state × respect table per OB timeframe: both aligned / only
  lower / only higher / both against / mixed → respect rates.
  15m→(1h,4h), 1h→(4h,1d), 4h→(1d)

Chunk 3 — Divergence features (PRIMARY focus per user):
- Delta/CVD from extended klines (full history): CVD divergence at touch
  (price lower-low into bullish zone vs last confirmed pivot low, CVD
  higher low → absorption) — primary; formation/touch delta secondary
- RSI via production calc_rsi (full history): RSI divergence at touch —
  primary; raw formation/touch values + OB/OS flags secondary
- Divergence reference = last CONFIRMED pivot before the touch (no
  look-ahead); not-evaluable → honest None

Chunk 4 — Open Interest (30-day window):
- Binance free OI history (last 30 days only). Trades older than the
  window show "not available" — never guessed, never removed. Coverage
  by TF made explicit (15m ≈ full, 1h ≈ 24%, 4h ≈ 6% of a 3000-candle run)

Chunk 5 — Structure & flow features (user-approved top 5):
- Liquidity sweep into zone (touch wicked a recent confirmed pivot low
  first, then entered the zone)
- Continuation vs reversal OB (trend state at FORMATION)
- BTC trend state for altcoin OBs at touch
- Zone penetration depth of respected zones (entry-placement insight)
- Absorption at touch (high volume + small range inside zone)

Excluded: liquidations (no free historical source), extra indicators
beyond RSI, zone-formula/mitigation changes, ilen sweep (later
diagnostic only, walk-forward-gated).

## Deferred (by user decision, 2026-07-18)
- Reaction zones (pullback/OTE, FVG standalone, breakers, pivot clusters)
- CAB + valid-pullback entry modules (chart-screenshot definitions)
- 5m scalp entry refinement
These return to the roadmap only after the OB module is refined via
Phases 20-21.

## Standing notes
- Phases 13–16 numbers were produced on pivot-3 zones; after the Phase 17
  parity fix the pivot-5 results are the canonical baseline going forward.
- Production params confirmed in code: `iLen=5` (settings default),
  `sLen=30` (active payload paths + user confirmation; the settings-registry
  entry shows 50 but is not the configured value).
