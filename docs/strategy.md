# Misprice PM Strategy Document

## Separation from cor_pol and historical dataset names

Misprice PM is an independent project. It is not EMPJP, not gamma_underreaction, not pullback_fear, and not a renamed `cor_pol` variant.

Historical names such as `four_quadrant_v3` are **telemetry dataset names only**. They are not the Misprice strategy identity. The canonical strategy name in this repository is:

```text
Misprice v3 repricing-lag detector
```

The earlier `cor_pol` codebase may be used as infrastructure or raw-data reference only:

```text
Allowed to borrow ideas:
- PM Gamma/CLOB public API access patterns
- dry-run/shadow execution boundary
- Telegram notification shape
- launchd deployment style
- official PM settlement discipline

Not allowed to import as thesis:
- EMPJP fair-bucket logic
- time-LCB gates
- residual gates
- Binance/open-close settlement
- generic gamma_underreaction thesis
```

## Canonical v3 thesis

```text
BTC path-state transition occurs.
Polymarket executable ask should reprice in the matching direction.
Actual PM ask lags that required repricing.
The lag remains executable after confirmation/final book checks.
Only then enter the lagged side.
```

The strategy does **not** say:

```text
BTC moved, therefore buy the matching PM side.
```

It also does **not** say:

```text
Only buy because ask < 0.60.
```

It says:

```text
BTC path changed enough to imply higher settlement probability, but PM ask is still underreacted versus the required repricing.
```

## v3 detector fields

For each candidate tick, the strategy stores explicit thesis-model fields:

```text
pre_bp
signal_bp
transition_bp
pre_ask
signal_ask
required_reprice
actual_reprice
lag_depth
spread
depth
```

The core model is:

```text
required_reprice = min(0.40, reprice_per_bp * abs(transition_bp))
actual_reprice = signal_ask - pre_ask
lag_depth = required_reprice - actual_reprice
```

Entry requires:

```text
abs(pre_bp) <= max_pre_abs_bp
abs(signal_bp) >= min_abs_bp
abs(transition_bp) >= min_transition_bp
lag_depth >= min_lag_depth
min_entry_ask <= executable_ask <= max_entry_ask
spread <= max_spread
depth >= min_depth
book age <= max_book_age_s
```

The live runner repeats the executable-book check immediately before submission. If the final book has already repriced and `lag_depth < min_lag_depth`, the order is blocked with:

```text
repricing_lag_collapsed_before_submit
```

## Current default v3 parameters

```text
lookback_s = 15
min_transition_bp = 3.0
max_pre_abs_bp = 2.5
min_abs_bp = 3.5
reprice_per_bp = 0.04
min_lag_depth = 0.035
min_elapsed_s = 20
max_elapsed_s = 220
min_entry_ask = 0.35
max_entry_ask = 0.65
max_spread = 0.05
min_depth = 5
```

These defaults are the optimized live-candidate settings selected by replaying
the v3 repricing-lag detector on historical raw telemetry. The old 60c cap and
60-120s ban were stopgap risk controls; they are not the v3 thesis model.

## Historical raw replay provenance

Optimizer run:

```text
strategy = misprice_v3_repricing_lag_detector
dataset = research_data/four_quadrant_v*/round_*/snapshots_1s.jsonl.gz
files = 206
usable_rounds = 185
grid_configs = 37,800
fee_rate = 0.07
friction_per_share = 0.02
qty = 5
```

Pushed-default v3 before optimization:

```text
trades = 128
trade_rate = 69.19%
net_pnl = +27.486335 U
win_rate = 71.09%
profit_factor = 1.2255
max_drawdown = 21.106695 U
chronological splits = -10.670470 / +8.782265 / +29.374540 U
```

Optimized live-candidate default:

```text
trades = 76
trade_rate = 41.08%
net_pnl = +71.140725 U
win_rate = 78.95%
profit_factor = 2.5130
max_drawdown = 6.515725 U
chronological splits = +21.716855 / +26.768345 / +22.655525 U
worst_consecutive_losses = 2
```

Dataset split:

```text
four_quadrant_v2: trades=26 pnl=+21.716855 U PF=2.2462
four_quadrant_v3: trades=50 pnl=+49.423870 U PF=2.6701
```

Execution-stress replay subtracting extra adverse fill cost from every trade:

```text
+1c/share: pnl=+67.340725 U PF=2.4082 DD=6.615725 U
+2c/share: pnl=+63.540725 U PF=2.3069 DD=6.715725 U
+3c/share: pnl=+59.740725 U PF=2.2089 DD=6.818780 U
+4c/share: pnl=+55.940725 U PF=2.1139 DD=7.018780 U
```

The historical replay is strong enough for a small-size live candidate only
after operator-controlled dry-run/live preflight. It is not a guarantee that a
larger live wallet will obtain the same queue position, latency, or fill quality.

## Settlement

Only official PM/Gamma outcome data can settle trades:

```text
Gamma outcomePrices: Up=1,Down=0 => YES wins
Gamma outcomePrices: Up=0,Down=1 => NO wins
```

Forbidden for settlement:

```text
- Binance close/open/final spot
- Jina Reader
- frontend probabilities
- local directional inference
```

If PM/Gamma is not resolved, the trade remains pending.

## What clean alpha capture means

Do not declare success from small positive PnL alone. Clean capture means:

```text
- normal-frequency forward dry-run, not a tiny hand-picked set
- every entry has required_reprice / actual_reprice / lag_depth evidence
- lag survives final executable-book check
- net PnL positive after fee/friction assumptions in replay
- official PM settlement coverage matches confirmed trade coverage
- PnL not dominated by one or two lucky trades
- YES/NO, entry bucket, and lag-depth buckets remain auditable
```

## Current status

This repository implements the standalone production boundary. Default mode is dry-run. Real trading requires explicit environment acknowledgement and local credentials.
