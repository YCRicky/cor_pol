# Aftertake strategy

Aftertake is a V6.5 observation-calibrated post-frontend-close CLOB classifier,
not a price feed and not a pre-close direction predictor.

Current strategy version:

```text
aftertake_v6.5_observation_calibrated_vacuum_score
```

## Scene gate

The final ten seconds are used only to describe whether a market is the right
kind of close-window scene. They do **not** select YES or NO, and the scene gate
is audit-only in runtime.

The audit records:

1. Whether there are at least three valid paired pre-close books.
2. Whether the pre-close span is at least 100 ms.
3. Whether the latest book is no more than 250 ms before frontend close.
4. Whether both latest bids are inside the 0.20--0.80 ambiguity band.
5. Whether the latest YES/NO bid edge is no wider than 0.12.
6. Whether each side's pre-close bid range is no wider than 0.08.

Clean terminal 99/1-style books and high-volatility close noise remain visible
in audit, but they are not hard rejections by themselves.

## Winner classifier

Between T+100 ms and T+1000 ms after frontend close, the classifier requires
three valid paired CLOB observations, each at least 100 ms apart, with the same
post-close bid leader.

The winning side must pass all five bid-support checks:

1. Best bid stays at or above the support floor.
2. Displayed best-bid size is at least the evaluated quantity.
3. Near-touch bid depth within $0.02 of best bid is at least 2x the evaluated
   quantity.
4. Winner bid does not materially decay.
5. Winner near-touch depth is retained or refilled.

The opposite side is now scored instead of requiring all vacuum components as
hard gates. Vacuum evidence includes:

1. Bid drops from the latest pre-close ambiguous book.
2. Near-touch depth decays from the latest pre-close baseline.
3. It does not refill enough to regain support.
4. It never comes within the reclaim gap of the winner bid during the evidence
   run.

Runtime thresholds:

```text
winner_support_score = 5 required
loser_vacuum_score >= 2 records an observation candidate
loser_vacuum_score >= 3 allows dry-run/live entry evaluation
```

This reflects PMData replay: requiring vacuum 4/4 was too sparse and caused the
strategy to miss the transition phase where residual winner-side asks still
exist.

## Entry

Entry is checked last. Aftertake enters only when the winner candidate still has
a displayed residual ask whose size can cover the evaluated order size.

```text
winner = post-close bid-support side
quality = opposite-side vacuum score
entry  = winner-side residual displayed ask still executable
reject = cheap ask on the bid-vacuum/loser side
```

A cheap ask on its own is never winner evidence. V6.5 has no blind entry-price
cap; ask repricing is recorded as a feature first, not used as a hard reject.

## Live sizing

Dry-run keeps `AFTERTAKE_QTY` so shadow logs stay comparable. Live mode sizes
from the observed residual ask:

```text
risk_budget = collateral_balance * AFTERTAKE_LIVE_MAX_ACCOUNT_RISK_FRACTION
spend_cap   = min(risk_budget, collateral_allowance)
unit_cost   = take_price + market_fee_per_share + builder_fee_per_share
raw_qty     = min(displayed_ask_size, spend_cap / unit_cost)
final_qty   = floor(raw_qty to AFTERTAKE_LIVE_QTY_FLOOR_STEP)
```

Before a live order is reserved, that final quantity is rechecked against all
five winner-support requirements using the same in-memory post-close
observations. A small shadow quantity can therefore never authorize a larger
live order; insufficient final-size support is blocked rather than downplayed.

Default live settings are:

```text
AFTERTAKE_LIVE_MAX_ACCOUNT_RISK_FRACTION=0.50
AFTERTAKE_LIVE_QTY_FLOOR_STEP=1
```

So a calculated 67.5-share order becomes 67 shares, never 68. The submitted
notional must remain at or below the configured account risk budget and current
collateral allowance.

## Audit telemetry

Every classifier decision records a structured audit payload containing the
strategy version, timing, pre-close scene features, post-close leader sequence,
winner/loser bid series, near-touch depth series, support/vacuum scores,
ask-lag measurements, thresholds, and reject reasons. Entry results additionally
record displayed available size and the live sizing calculation when live mode is
used.

The critical path deliberately has no REST book request and no Telegram call.
Before close it has already completed market/account preflight. After
confirmation it atomically reserves the market in SQLite and submits only the
single bounded execution path configured by the runtime. Any ambiguous live
response becomes `execution_unknown`, which blocks further entries until
reconciled.
