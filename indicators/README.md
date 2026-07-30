# Aggregated CVD Engine — Pine v6

Multi-exchange cumulative volume delta with spot/perp decomposition, built for
any pair (not BTC-only).

## Why this exists

Watching one exchange's CVD hides the flow that actually moves price. Real
buying often starts on a venue you are not looking at — a Korean spot book, or
a perp venue leading the move. This engine aggregates delta across venues,
separates spot from perpetuals, and shows which venue is driving.

## Files

| File | Phase | Status |
|---|---|---|
| `cvd_engine_p1.pine` | 1 — parity harness | parity confirmed (tick rule) |
| `cvd_engine_p2.pine` | 2 — multi-venue aggregation | verified vs built-in (~1% on live bar) |
| `cvd_engine_p3.pine` | 3 — spot vs perp comparison | ready to test |

## Phase plan

| Phase | Deliverable | Exit criterion |
|---|---|---|
| **1** | Venue registry, unit normalization, single-venue CVD, parity mode | Our number matches TradingView's built-in CVD; classification rule identified |
| 2 | Multi-venue aggregation, spot/perp streams, candles + columns | Sane on BTC, ETH and a mid-cap alt; no crash on missing pairs |
| 3 | Comparison lines, normalization modes, spot−perp spread | Divergence visually obvious on known events |
| 4 | Contribution table, venue lead-lag ranking | Ranking matches reality on a known Korean-led alt pump |
| 5 | Divergence detection, liquidation filter, alerts | Confirmed-bar only, no repaint |

Phase 1 carries all the risk. If parity fails, everything downstream is built
on a wrong measurement.

## Validating Phase 1

1. Add TradingView's built-in **CVD** indicator to the chart.
2. Set this script's **Venue** = `Chart symbol`.
3. Match **Lower Timeframe** and **Anchor Period** between the two.
4. Compare the built-in's last value to the `CVD` row in this script's table.
5. If they disagree, cycle **Volume Classification** until they agree.

Step 5 is the actual experiment. TradingView does not publish which rule its
CVD uses, so we determine it by measurement rather than assumption, then lock
it in for Phase 2.

### Result

**Tick rule.** Measured on `BINANCE:ETHUSDT`, 4H chart, 1m LTF, monthly anchor:
ours `96.84K` vs built-in `97.35K` — 0.52% apart with visually identical candle
shapes. `Bar direction` and `Proportional` were both clearly wrong, so the rule
is unambiguous.

The 0.52% residual came from the tick chain breaking at chart-bar boundaries:
`lastDir` reset to zero each bar, and the first intrabar was compared against
its own open instead of the previous bar's final close. On a monthly anchor at
4H that is ~180 broken links, each able to mis-sign or discard one minute of
volume. Both are now carried across bars via `dirCarry` / `closeCarry`.

## Phase 2 — multi-venue aggregation

`cvd_engine_p2.pine` runs the Phase-1 delta engine (now the shared `f_delta`
function, tick rule as default) across up to 13 venues and sums them into three
streams: **Spot**, **Perp**, **Global**. Pick which to plot as candles, a line,
or per-bar delta columns.

Venues are checkboxes. Defaults are a conservative six — Binance/Coinbase/OKX
spot, Binance/Bybit perp — that run on a free plan; the rest (Kraken, Bitget,
Gate/MEXC, Upbit, Bithumb spot; OKX/Bitget/BitMEX perp) are one click away. A
disabled venue is requested with an empty symbol, so it costs no intrabar
processing and contributes exactly zero.

**Aggregate candle wicks are an upper envelope.** The wick is the sum of each
venue's within-bar running extreme. Because venues are not synchronised, that
sum assumes every venue peaks at once, so the wick is slightly wide — but it
always contains the true open and close, so it never misleads on direction. The
body (open/close) is exact. This is a deliberate, documented approximation;
per-venue precise wicks are not achievable without a merged intrabar timeline,
which Pine can't build across feeds.

The diagnostics table reports the displayed stream's CVD, per-stream bar delta,
**active/enabled venue counts per stream** (so an unlisted venue on an alt is
visible, never mistaken for flat flow), engine + history depth, and request
budget used.

Venues updated after first testing: BitMEX dropped; Hyperliquid (spot + perp)
and KuCoin (spot) added. 15 venues total (10 spot, 5 perp), 30/40 requests.
Hyperliquid tickers are unverified on TradingView — if unlisted, the venue
simply shows inactive.

### Verification result

Tested by setting P2 to Binance spot only and comparing to the built-in CVD on
a `BINANCE:ETHUSDT` chart: `8.27K` vs `8.38K`, ~1.3% apart on a live forming
bar, with identical candle shapes. This confirms the delta engine survived being
wrapped into the per-venue `f_delta` function. An earlier `4.12B` Global reading
was the disabled-venue leak (fixed); post-fix, ETH Global reads a sane `93.58K`.

## Phase 3 — spot vs perp comparison

`cvd_engine_p3.pine` is a superset of Phase 2. Same validated engine, plus the
divergence view the project was built for. Three views:

- **Single stream** — Phase 2 behaviour (candles / line / columns of Global,
  Spot, or Perp).
- **Spot vs Perp** — both streams overlaid as lines. Default scaling is
  **Z-Score**, because perp volume dwarfs spot and a raw overlay flattens spot
  into the baseline, hiding the divergence. Raw is available for true magnitude.
- **Spot-Perp Spread** — the difference `z_spot − z_perp` as one histogram
  crossing zero: **> 0 spot leading** (accumulation), **< 0 perp leading**
  (leverage-driven).

The z-score normalises each cumulative stream against its own recent mean over a
configurable window, making the two comparable. Note the cumulative streams
reset on the anchor, so the z-score window spans multiple anchor periods — fine
for visual comparison; Phase 5 refines this into slope-based divergence
detection with a liquidation-cascade filter and alerts.

Phase 3 only visualises divergence. Detection and alerts are Phase 5.

Not compile-tested — no Pine toolchain locally.

## Design decisions

**Units are base (coins), not USD.** The built-in CVD reports base units — a
`LABUSDT` reading of `-31.33M` means 31.33 million LAB net sold. Matching that
removes an entire subsystem: KRW- and JPY-quoted venues need no FX conversion
because their volume is already denominated in the base asset. Only inverse
contracts (BitMEX, Deribit) need conversion, and they are handled explicitly.

**Dynamic symbols.** Pairs are built from `syminfo.basecurrency`, so the script
works on any asset. Every request uses `ignore_invalid_symbol` — without it, a
single unlisted pair kills the whole script.

**The LTF dropdown is the engine switch.** `Chart` needs no intrabar data, so it
uses `request.security` — cheap, full history, crude delta. Any other value uses
`request.security_lower_tf` — accurate, but history collapses.

**Degrade loudly, never silently.** Plan gating, unlisted pairs and unusable
resolutions all fall back to chart-bar delta and report it in the Status row.
A silently wrong number is the failure mode this design exists to prevent.

## Known platform limits

- **40** `request.*()` calls per script.
- **~100,000** intrabars per `request.security_lower_tf` call. This, not the
  plan, is what caps history:

  | Chart TF | LTF | Intrabars/bar | Usable history |
  |---|---|---|---|
  | 15m | 1m | 15 | 5,000 (plan-capped) |
  | 1H | 1m | 60 | ~1,660 bars |
  | 4H | 1m | 240 | ~415 bars |
  | 4H | 5m | 48 | ~2,080 bars |
  | 1D | 1m | 1440 | ~69 bars |
  | 1D | 15m | 96 | ~1,040 bars |

- **Seconds resolutions require Pro+.** Listed in the dropdown, gated at runtime.
- **Tick resolutions cannot be requested from Pine** at all. Present in the
  dropdown to mirror the built-in's UI; they fall back to 1m.
- Free plan: 5,000 bars of history, 2 indicators per chart, no webhook alerts.

## Free-plan guidance

Run on **15m or lower with 1m LTF** (full history, precise delta), or on
**4H/1D with `Chart` LTF** (full history, crude delta). On 4H with 1m LTF the
script is correct but only reaches back about two months.
