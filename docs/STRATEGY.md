# Aftertake strategy

Aftertake is a V8 event-driven post-frontend-close CLOB classifier,
not a price feed and not a pre-close direction predictor.

Current strategy version:

```text
aftertake_v8_clob_refill_guard_250ms
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

Between T+50 ms and T+250 ms after frontend close, both outcome-token books
must first have a fresh post-close update. The classifier then requires two
distinct executable top/depth states with the same post-close leader. There is
no fixed 100 ms sleep: identical repeated snapshots do not count, but a real
book transition can confirm immediately.

V8 locks to the first observable post-close leader. If the leader reverses at
any time inside the decision sequence, the round is permanently rejected. This
prevents a later reversal from being mistaken for the initial close reaction.

If exactly one side still has a bid, that supported side is the leader and the
missing opposite bid is strong vacuum evidence. If both bids are absent or
equal, direction remains unresolved and entry is rejected.

The winning side must pass all five bid-support checks:

1. Best bid stays at or above the support floor.
2. Displayed best-bid size is at least the evaluated quantity.
3. Near-touch bid depth within $0.02 of best bid is at least 2x the evaluated
   quantity.
4. Winner bid does not materially decay.
5. Winner near-touch depth is retained or refilled.

The opposite side retains a four-component vacuum score. Vacuum evidence
includes:

1. Bid drops from the latest pre-close ambiguous book.
2. Near-touch depth decays from the latest pre-close baseline.
3. It does not refill enough to regain support.
4. It never comes within the reclaim gap of the winner bid during the evidence
   run.

Runtime thresholds:

```text
winner_support_score = 5 required
loser_vacuum_score >= 3 allows dry-run/live entry evaluation
loser_refill_failure = mandatory
post_close_leader_reversal = mandatory rejection
```

The vacuum>=2 variant remains research-only. In the corrected 100-market
close-boundary replay it produced 10 signals with 9 correct directions. V7
vacuum>=3 produced 7 signals with 6 correct directions. V8 removed the one V7
error and produced 6/6 observed directions, but six signals are not enough to
claim future 100% accuracy.

The archived PMData replay reconstructs one token from the binary complement of
the other. It therefore cannot show a missing loser bid while preserving an
executable winner ask. The one-sided-vacuum branch is covered by deterministic
tests but still requires native paired-token forward shadow evidence.

## Entry

Entry is checked last. Aftertake enters only when the winner candidate still has
a displayed residual ask whose size can cover the evaluated order size.

```text
winner = post-close bid-support side
quality = opposite-side vacuum score
entry  = winner-side residual displayed ask still executable
reject = cheap ask on the bid-vacuum/loser side
```

A cheap ask on its own is never winner evidence. V8 has no blind entry-price
cap; ask repricing is recorded as a feature first, not used as a hard reject.

## Live sizing

Dry-run uses the same sizing math with a simulated account balance
(`AFTERTAKE_DRY_RUN_SIM_BALANCE`, default `100`) and equal simulated allowance.
Live mode replaces that simulated collateral with the actual CLOB pUSD balance
and allowance. Both modes size from the observed residual ask:

```text
risk_budget = collateral_balance * AFTERTAKE_LIVE_MAX_ACCOUNT_RISK_FRACTION
spend_cap   = min(risk_budget, collateral_allowance)
unit_cost   = take_price + market_fee_per_share + builder_fee_per_share
raw_qty     = min(displayed_ask_size, spend_cap / unit_cost)
final_qty   = floor(raw_qty to AFTERTAKE_LIVE_QTY_FLOOR_STEP)
```

Before an order is reserved, the dynamically sized final quantity is rechecked
against the same in-memory post-close observations. The recheck still requires
best-bid size to cover the final quantity, but near-touch depth uses a 1x final
size floor so small residual asks (for example 10 displayed shares) are not
blocked by the initial 2x discovery buffer. A small shadow quantity can therefore
never authorize a larger unsupported order, while executable residual depth is
still allowed.

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
per-token freshness, missing-loser-bid evidence, ask-lag measurements,
thresholds, and reject reasons. Entry results additionally record displayed
available size and the live sizing calculation when live mode is used.

The critical path deliberately has no REST book request and no Telegram call.
Before close it has already completed market/account preflight. After
confirmation it atomically reserves the market in SQLite and submits only the
single bounded execution path configured by the runtime. Default GTC submits
remain pending until later CLOB reconciliation / official settlement; submit-path
infrastructure ambiguity skips/rests only the affected market rather than
freezing unrelated future entries.
