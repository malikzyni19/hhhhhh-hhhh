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

## Phase 20 — Reaction Zones + Entry Modules
- Pullback/OTE zones, FVG, breakers, pivot clusters
- User's CAB + valid-pullback rules encoded from chart screenshots
  (definitions pending — will be confirmed in plain language before coding)
- 5m used ONLY as scalp entry refinement inside HTF-validated zones — never
  for zone selection

## Phase 21 — Walk-Forward Validation
- High-win-ratio setups from 18C/19/20 validated through the Phase 15B
  walk-forward machinery (locked candidates, 30 gates, look-ahead audit)
  before anything is trusted or discussed for production.

## Standing notes
- Phases 13–16 numbers were produced on pivot-3 zones; after the Phase 17
  parity fix the pivot-5 results are the canonical baseline going forward.
- Production params confirmed in code: `iLen=5` (settings default),
  `sLen=30` (active payload paths + user confirmation; the settings-registry
  entry shows 50 but is not the configured value).
