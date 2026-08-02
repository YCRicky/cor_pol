# Chainlink BTC label and independent V9 dual-lane research

Date: 2026-08-03

Scope: read-only research from `C:\Users\ycric\OneDrive\Desktop\chainlink_btc_high_frequency_0731.log`, the checked-in Aftertake code/audits, and unauthenticated Polymarket public endpoints. No credential, private key, or API secret was read or written.

## Decision

The Chainlink capture is suitable as an offline BTC price/outcome timing label, not as a live winner signal. It contains no Polymarket market slug, token ID, bid/ask, order, fill, or settlement field. The public address query provides useful examples of aggressive `BUY` fills at `.99`, but it did not return the historical SOL target row in the queried pages and the Gamma slug endpoints returned 404. That is a data-retention/identity/order-detail gap, not evidence that the historical fill did not happen.

The implementation therefore uses a deterministic replay contract rather than claiming a Polymarket order-book backtest. V9 is independent of V8, defaults to shadow/dry-run, and is not enabled by the existing live default.

## Chainlink log schema and measured coverage

Input file: `C:\Users\ycric\OneDrive\Desktop\chainlink_btc_high_frequency_0731.log`.

The header is:

```text
# columns: recv(ET) | price | observation(ET) | age(recv-obs)
```

Measured by a streaming parser:

| Field | Observation |
|---|---|
| File size | 115,312,248 bytes |
| Total lines | 1,747,102 |
| Parsed data rows | 1,746,917 |
| Non-data telemetry rows | 184; these are WebSocket error/close/reconnect lines |
| Receive coverage | 2026-07-31 00:00:00.048 through 23:59:59.994 ET |
| Receive precision | milliseconds |
| Receive cadence | p50 46 ms, p95 95 ms, p99 126 ms, maximum 33,841 ms |
| Gaps | 50 receive intervals greater than 1 second |
| Age | min 90 ms, p50 112 ms, p95 159 ms, p99 187 ms, maximum 3,637 ms |
| Price changes | 713,737 changes; 313,750 distinct textual prices |
| Observation date | only time-of-day is present; two rows require previous-day reconstruction around midnight |

The observation field has no date. A replay must reconstruct its date from the receive date and the logged age, then retain the original millisecond precision. A line containing `WS error`, `WS closed`, or `WS loop ended` is not a price observation and must not become a zero-price or stale-price observation. The 33.8-second maximum receive gap and reconnect telemetry are explicit coverage gaps.

The log has no market close/end timestamp. A replay may join Chainlink receive/observation time to a known market schedule only as an offline label, and must preserve the join uncertainty. It cannot select YES/NO in the live close path.

## Public Polymarket evidence and data gaps

Primary API references:

- [Polymarket API introduction](https://docs.polymarket.com/api-reference/introduction) separates public Gamma market discovery, public Data user/trade analytics, and public CLOB market data from authenticated trading endpoints.
- [Public user/market trades](https://docs.polymarket.com/api-reference/core/get-trades-for-a-user-or-markets) exposes address, side, asset, condition, size, price, timestamp, slug, outcome, and transaction hash.
- [CLOB trade schema](https://docs.polymarket.com/api-reference/trade/get-trades) documents `trader_side`, `maker_address`, `maker_orders`, and `match_time`, but the endpoint is authenticated for user trades.
- [Limit and marketable orders](https://docs.polymarket.com/trading/orders/overview) states that a market order is a limit order submitted at a marketable price; all orders remain limit orders.
- [Order lifecycle](https://docs.polymarket.com/concepts/order-lifecycle) documents off-chain matching and Polygon settlement.
- [Gamma market data](https://docs.polymarket.com/market-data/overview) documents the outcome/token mapping and public market fields.

Public query performed without credentials:

```text
GET https://data-api.polymarket.com/trades?user=0xa11afe967a780acad40841fa647a671874fb64a2&limit=100&offset=0&takerOnly=false
```

The first page contains verified public examples such as SOL/XRP/HYPE `BUY` trades at `.99`, with size, Unix timestamp, slug, outcome, and transaction hash. Paging offsets 0 through 400 reached timestamps before the target round but returned no `sol-updown-5m-1785426000` row. The public Gamma requests `/markets/slug/sol-updown-5m-1785426000` and `/events/slug/sol-updown-5m-1785426000` returned 404. The result is recorded as unavailable historical public data, not a negative trade finding.

The user-reported SOL success remains Telegram-only in the available local context:

```text
sol-updown-5m-1785426000
ORDER_SUBMITTED YES qty=5 limit=.99
ENTRY_CONFIRMED matched=5 avg=.3500 event_ts_utc=2026-07-30T15:45:02.619Z
order=0x9b68d41425bde9be39600d7a6cb99e93e4b25f192cfbdc9c22d9f782fe4d8b5d
```

This proves only the reported submission/fill lifecycle. `.99` is the aggressive limit ceiling; `.35` is the actual fill average. It does not identify the classifier's observed `entry_ask`, the ask levels swept, maker/taker role, or the running code SHA. A public trade row with `transactionHash` can corroborate a fill, but it does not by itself reconstruct the pre-submit order book. The missing order ID/maker-orders/transaction linkage is the unique historical evidence gap.

## Current live audit comparison

The recent audit extract has 268 decision rows:

| Reason | Rows | Interpretation |
|---|---:|---|
| `post_close_window_not_open` | 103 | polling before the decision window, not a terminal rejection |
| `insufficient_post_close_observations` | 87 | paired/fresh evidence was unavailable or not yet sufficient |
| `winner_residual_ask_missing` | 70 | no executable winner ask at the evaluation point |
| `loser_bid_refilled` | 7 | V8 hard refill gate |
| `winner_near_touch_depth_too_thin` | 1 | executable support depth failed |

These are rows, not independent opportunities. They do not establish that V8 rejected a 35--50c executable ask. They do establish the first blockers that V9 shadow must record.

## Falsifiable V9 rules

V9 never uses Chainlink in live eligibility. It consumes only timestamped paired executable books.

### Lane R: residual taker

`would_enter=YES` only if all of the following hold:

1. The current book is inside the V9 close window and both token updates are fresh at or after round close.
2. One side is the winner with a configurable dominance gap over the other side; a missing loser bid is allowed, but a loser bid does not need to be zero.
3. Winner bid floor, best-bid size, and near-touch support depth cover the evaluated quantity.
4. The winner has displayed ask levels whose cumulative size at prices `<= 0.99` covers the quantity.
5. No observed post-close book reverses the winner or brings the loser back inside the configured quantitative reclaim gap.

The post-order price is exactly `0.99`, not the reported historical average. A missing full ask ladder is a fail-closed blocker in live eligibility; top-of-book-only replay may only be marked `insufficient_ask_levels`.

### Lane S: higher-confidence 0.99 sweep

Lane S requires Lane R's executable conditions plus two fresh paired observations, stable leadership, a tighter dominance threshold, bounded quantity and notional, a maximum book age, and explicit non-ambiguous settlement semantics. It is intended to test 0.99 sweeps without opening the live tail risk of an unverified historical label.

Both lanes use one `would_enter` decision and one shared per-market reservation/risk claim. A live post is allowed only after lane eligibility, reservation, and final-size risk checks; a submit-path ambiguity is terminal for that market and cannot trigger a second post.

## Research design and holdout contract

The deterministic replay harness accepts the same `PairedBook` sequence and evaluates V8.1, Lane R, and Lane S with policy labels. It also supports single-factor counterfactual policy labels for confirmation count, loser-refill handling, and window horizon. The factors are logged rather than repeatedly tuned on the same holdout.

The implementation is `src/aftertake/v9_replay.py`: it emits one checkpoint per
progressively observed book, keeps observation and decision timestamps separate,
supports a deterministic decision-latency parameter, and exposes
`counterfactual_policies()` plus `chronological_split()`. `summarize_lane()`
returns `None` for precision and Wilson bounds when no offline outcome label is
provided. This is a replay contract, not a claim that the Chainlink file can
reconstruct Polymarket books.

Data splits are chronological: train for threshold design, validation for one locked policy choice, and an unseen holdout for the final report. The current Chainlink file has no Polymarket book/trade snapshots, so no precision, loss, coverage, or theoretical consumed-depth result is claimed from it. Once a matching order-book capture exists, report observed precision with a Wilson lower bound, maximum loss, one-trade tail loss, coverage, opportunity count, theoretical executable quantity, and latency sensitivity. “100%” may only describe an observed sample and never a future guarantee.
