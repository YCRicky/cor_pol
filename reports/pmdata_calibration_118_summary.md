# PMData Aftertake Calibration — BTC 5m L2 Replay

Generated from local PMData `poly_l2` Parquet files using `PMDATA_API_KEY=[REDACTED]`.
Raw PMData files stay local under `pmdata_cache/` and are not redistributed.

## Corpus

- Markets attempted: 118
- Markets successfully replayed: 118
- Window: BTC Up/Down 5m markets around 2026-07-17 18:00 UTC through 2026-07-18 03:45 UTC
- Replay unit: reconstructed YES book from `book` snapshots + `price_change` deltas; NO book derived as the binary complement.
- Entry evaluation: close+100ms through close+1000ms on observed book updates.
- Settlement label: PMData `winning_outcome`.

## Current strict classifier result

Strict live-equivalent V6.4 produced:

```text
strict_entries = 0 / 118
```

Final reject reasons:

```text
loser_bid_drop_insufficient          89
winner_depth_decayed                 12
loser_bid_depth_decay_insufficient    6
bid_support_not_persistent            4
winner_bid_decayed                    3
winner_residual_ask_repriced          2
winner_residual_ask_too_thin          1
loser_bid_refilled                    1
```

This confirms the live dry-run silence is not just plumbing. Under the current definition, the post-close loser-vacuum requirements are too strict for most observed markets.

## Observation-first loose rule sweep

These are **not production fills**. They are calibration buckets that show where the phenomenon appears if winner support is required and loser vacuum is treated as a score instead of four mandatory hard filters.

Rule shape:

```text
support_score >= 5
vacuum_score >= N
entry_ask_proxy = latest confirmed winner ask
PnL proxy = +1-entry_ask if predicted side == winning_outcome else -entry_ask
```

Results:

| Rule | Trades | Hit Rate | Avg PnL / 1x | Sum PnL / 1x | Avg Ask |
|---|---:|---:|---:|---:|---:|
| support5_vacuum0 | 118 | 63.6% | +0.0866 | +10.22 | 0.5490 |
| support5_vacuum1 | 97 | 66.0% | +0.1000 | +9.70 | 0.5598 |
| support5_vacuum2 | 27 | 70.4% | +0.1511 | +4.08 | 0.5526 |
| support5_vacuum3 | 18 | 72.2% | +0.1617 | +2.91 | 0.5606 |
| support5_vacuum4 | 5 | 80.0% | +0.2480 | +1.24 | 0.5520 |

## Ask cap sensitivity

| Rule | Ask Cap | Trades | Hit Rate | Avg PnL / 1x | Sum PnL / 1x |
|---|---:|---:|---:|---:|---:|
| support5_vacuum1 | <=0.55 | 62 | 66.1% | +0.1339 | +8.30 |
| support5_vacuum1 | <=0.58 | 69 | 63.8% | +0.1067 | +7.36 |
| support5_vacuum1 | <=0.60 | 75 | 61.3% | +0.0777 | +5.83 |
| support5_vacuum1 | <=0.65 | 92 | 64.1% | +0.0879 | +8.09 |
| support5_vacuum2 | <=0.55 | 18 | 61.1% | +0.0839 | +1.51 |
| support5_vacuum2 | <=0.58 | 21 | 66.7% | +0.1338 | +2.81 |
| support5_vacuum2 | <=0.60 | 22 | 63.6% | +0.1009 | +2.22 |
| support5_vacuum2 | <=0.65 | 27 | 70.4% | +0.1511 | +4.08 |
| support5_vacuum3 | <=0.58 | 13 | 69.2% | +0.1538 | +2.00 |
| support5_vacuum3 | <=0.65 | 18 | 72.2% | +0.1617 | +2.91 |
| support5_vacuum4 | <=0.58 | 5 | 80.0% | +0.2480 | +1.24 |

## Interpretation

1. The old strict definition requiring all winner-support and all loser-vacuum components is over-filtering. It produced zero entries across this corpus.
2. Winner support is common. The real bottleneck is not support; it is demanding a full loser-side vacuum.
3. Treating loser vacuum as a score bucket is more aligned with the observed data. `vacuum_score >= 2` and `>= 3` are the first interesting calibration bands.
4. `vacuum_score >= 4` is high quality but too sparse in this first corpus: 5 trades over 118 markets.
5. `support5_vacuum2` gives a plausible observation/strategy candidate: 27 trades over 118 markets, 70.4% hit rate, +0.1511 average 1x proxy PnL. It needs stronger fill/size and latency validation before live use.
6. Entry ask cap should not be reintroduced as a blind hard cap yet. The <=0.65 bucket is not worse in this sample for vacuum2/3; the cap should be evaluated jointly with size/fillability, not by price alone.

## Proposed next strategy definition to test — not deployed yet

Candidate V6.5 research rule:

```text
pre-close scene: audit-only
post-close confirmation: spacing-aware 3 samples within 100-1000ms
winner support: all 5 components required
loser vacuum: score >= 2 for observation candidate; score >= 3 for high-confidence candidate
residual ask: require executable ask and displayed size >= qty
ask reprice: log as feature first; do not hard-reject until replay proves it hurts expectancy
notification:
  - vacuum >= 2: local audit candidate
  - vacuum >= 3 + ask size ok: dry-run simulated-take candidate / TG if deployed
```

Before touching runtime, the next validation pass should add true ask-size / fillability and latency stress to this batch replay.

## Artifacts

- Batch script: `scripts/pmdata_batch_calibrate.py`
- Full JSON report: `reports/pmdata_batch_calibration_118.json`
- Candidate CSV: `reports/pmdata_candidates_118.csv`
