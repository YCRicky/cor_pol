# Legacy EMPJP Strategy Brief: `empjp_e75_n30_c1_l1`

This is the dedicated note for the retained EMPJP module. `main.py` now runs
[`twap_price_path_tail_v2`](twap_price_path_tail_live_strategy_v2_2026-08-08.md)
instead; this file remains as a rollback/research reference.

## Executive summary

`empjp_e75_n30_c1_l1` is a BTC-only Polymarket 5-minute up/down strategy. It replaces the older BTC/ETH correlation-arbitrage bot.

The strategy does not try to predict BTC direction from a narrative or from BTC/ETH pair correlation. It uses a frozen empirical probability surface built from historical PM5M replay data, then trades only when the executable Polymarket ask is sufficiently below that empirical probability after expected taker fee.

Production policy:

```text
EMPJP_QTY=5
EMPJP_EDGE_MIN=0.075
EMPJP_MIN_CELL_N=30
EMPJP_CONFIRM_S=1
EMPJP_LATENCY_S=1
EMPJP_WEEKEND_REST_ENABLED=false
```

Default mode remains `DRY_RUN=true`. Live mode requires explicit CLOB credentials and `DRY_RUN=false`.

## What the name means

```text
empjp_e75_n30_c1_l1
```

| Segment | Meaning |
|---|---|
| `empjp` | Empirical joint probability surface |
| `e75` | Minimum fee-adjusted edge is 7.5 percentage points |
| `n30` | Calibration cell must contain at least 30 samples |
| `c1` | Signal must survive 1 second of confirmation |
| `l1` | Entry waits 1 additional second after confirmation |

## How we got here

The project started from a BTC/ETH correlation-arbitrage idea: buy the cheap diagonal across BTC and ETH 5-minute Polymarket markets when the combined PM box looked mispriced.

That original approach surfaced several useful infrastructure pieces:

- Polymarket Gamma market discovery.
- Market websocket orderbook ingestion.
- Share-sized aggressive GTC BUY execution followed by cancel/reconcile.
- PM/UMA settlement accounting.
- User websocket treated as optional telemetry, not execution truth.
- Runtime JSONL audit logs.

But the correlation-arb strategy itself had structural problems:

1. BTC/ETH co-movement was not stable enough at 5-minute expiry.
2. Apparent box gaps could be eaten by taker fee, spread, and adverse selection.
3. Multi-leg exposure created extra failure modes: imbalance, stale marks, partial fills, and settlement divergence diagnostics.
4. The best information was not “BTC vs ETH correlation”; it was the state of the BTC 5-minute market itself.

So the research direction shifted from pair correlation to a single-market empirical probability model.

The key reframing was:

> Instead of asking whether BTC/ETH are correlated, ask whether the current BTC 5-minute state historically implies a final-up probability that is meaningfully different from the current Polymarket ask.

## Alpha definition

The runtime estimates:

```text
P(final_up | tte, z_resid, path_dir)
```

Where:

- `tte`: seconds to expiry.
- `z_resid`: current BTC move in basis points, normalized by rolling BTC sigma and remaining time.
- `path_dir`: whether the path so far is `up_dominant`, `down_dominant`, or `balanced`.

The frozen calibration maps:

```text
(tte_bin, z_bin, path_dir) -> {cell_n, emp_up}
```

Runtime then computes executable edge:

```text
YES edge = emp_up - yes_ask - fee(yes_ask)
NO  edge = (1 - emp_up) - no_ask - fee(no_ask)
```

It buys the side with the larger edge only if all filters pass.

## Frozen calibration

Committed file:

```text
data/empjp_e75_n30_c1_l1_calibration.json
```

Calibration metadata currently says:

| Field | Value |
|---|---:|
| Source rows | 1,050,866 |
| Source rounds | 4,061 |
| Source days | 18 trading dates from 2026-06-09 through 2026-06-28, with gaps where data was unavailable |
| Frozen cells | 369 |
| Minimum cell size | 30 |
| Edge threshold | 0.075 |
| Confirm / latency | 1s / 1s |

The runtime loads this JSON directly. It does not need pandas or the original research panel.

## Entry filters

Default filters:

```text
45s <= elapsed <= 255s
45s <= tte <= 240s
0.18 <= executable ask <= 0.82
spread <= 0.05
book depth >= 5 shares
cell_n >= 30
fee-adjusted edge >= 0.075
```

These filters intentionally avoid:

- the first seconds of a fresh market,
- the final seconds near settlement,
- extremely cheap/expensive tails,
- wide books,
- thin books,
- low-sample calibration cells,
- small edges that disappear after taker fee.

## Execution contract

The live order path is intentionally conservative and reused from the hardened previous bot:

1. Discover the current BTC 5-minute PM market.
2. Subscribe to YES/NO CLOB books.
3. Poll BTC spot for state features.
4. Evaluate the empirical probability surface.
5. If a signal appears, wait `confirm_s + latency_s`.
6. Re-sample the current executable ask.
7. Recompute edge from the original signal probability and current ask.
8. If edge still passes, send a share-sized marketable GTC BUY.
9. Immediately cancel any unfilled remainder.
10. Reconcile fill from CLOB submit/cancel/get_order.
11. If confirmed fill is below target, chase the remaining quantity up to
    `EMPJP_EXEC_MAX_CHASE_ATTEMPTS` times, rechecking live edge before each
    chase order.
12. Track any positive confirmed fill as an open position, even if the target
    quantity was not fully reached.
13. Never blindly retry an order with unknown CLOB state; emit an alert and
    track only confirmed filled quantity.
14. Hold filled position to PM/UMA settlement.

The bot does not use Polymarket dollar-sized market BUY semantics. It uses a marketable limit to preserve share sizing.

## Settlement accounting

Realized PnL is based on PM/UMA outcome via Gamma:

```text
win = (pm_up >= 0.5) for YES, else (pm_up < 0.5)
pnl = qty * win - qty * entry_price - entry_fee
```

Binance spot is used for features only. It does not determine realized PnL.

## Backtest / replay performance snapshot

The currently documented replay split showed both weekday and weekend segments profitable, but weekend quality was lower:

| Segment | Stress PnL | Profit Factor | Avg / trade |
|---|---:|---:|---:|
| Weekday | +1035.6 | 1.76 | +0.550 |
| Weekend | +410.2 | 1.36 | +0.310 |
| Combined shown split | +1445.8 | — | — |

Interpretation:

- Weekends were not bad enough to disable by default.
- Weekend trades had weaker quality, so size should not be increased based only on weekday performance.
- `EMPJP_WEEKEND_REST_ENABLED=false` is deliberate: weekends remain enabled unless the operator explicitly wants weekday-only exposure.

Important caveat: these numbers are historical replay/stress results, not a live execution guarantee. Live validation still needs clean Polymarket network access, websocket ingestion, and real CLOB fill behavior.

## Why this is more practical than the old BTC/ETH correlation bot

| Old correlation-arb bot | EMPJP bot |
|---|---|
| Two-market / two-leg exposure | One BTC market, one side per round |
| Relies on BTC/ETH correlation staying useful intraround | Uses BTC market state directly |
| More imbalance and hedge complexity | Simpler fill and settlement path |
| Box gap can be misleading after fees | Explicit fee-adjusted edge threshold |
| More fragile runtime state | Smaller state surface and cleaner JSONL audit |

## Current readiness

Verified in the committed repo:

```bash
python3.11 -m compileall src tests
python3.11 -m unittest discover -s tests -v
DRY_RUN=true python3.11 main.py --rounds 0 --start-mode current
```

Known local blocker on Diana's Mac mini at the time of this commit:

```text
gamma-api.polymarket.com and clob.polymarket.com resolve/present a self-signed CN=rpz10-landing certificate.
```

That blocks full `--rounds 1` market discovery on this local network. It is a local network/DNS/TLS interception issue, not a strategy-code failure. Full live smoke should be run from a clean network or deployment host before enabling `DRY_RUN=false`.

## Live runbook shortcut

Shadow smoke:

```bash
cp .env.example .env
DRY_RUN=true python3.11 main.py --rounds 1 --start-mode current
```

Live mode only after credentials, wallet funding, and network smoke are verified:

```text
DRY_RUN=false
PRIVATE_KEY=...
CLOB_API_KEY=...
CLOB_API_SECRET=...
CLOB_API_PASSPHRASE=...
CLOB_SIGNATURE_TYPE=...
CLOB_FUNDER_ADDRESS=...
```

Never commit `.env` or credentials.
