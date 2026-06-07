# Live trading runbook

This document locks the current live-trading design before EC2 deployment.
It is based on the Polymarket docs reviewed on 2026-05-29:

- Polymarket USD: https://docs.polymarket.com/concepts/pusd
- Deposit wallets: https://docs.polymarket.com/trading/deposit-wallets
- CLOB create order: https://docs.polymarket.com/developers/CLOB/orders/create-order
- CLOB cancel/query order: https://docs.polymarket.com/trading/orders/cancel
- CLOB Python client: https://docs.polymarket.com/developers/CLOB/clients/python-client
- Fees: https://docs.polymarket.com/polymarket-learn/trading/fees

## Polymarket changes that matter

- Collateral is pUSD on Polygon. Do not assume old bridged-USDC wording in
  logs, config, or runbooks.
- The order signer and the order funder are separate concepts for proxy/deposit
  wallet accounts. `PRIVATE_KEY` signs, while `CLOB_FUNDER_ADDRESS` is the
  wallet that actually funds the order.
- Wallet mode is controlled by `CLOB_SIGNATURE_TYPE`:
  `EOA` (`0`) for direct EOA trading, `POLY_PROXY` (`1`) for legacy proxy
  accounts, and `POLY_1271` (`3`) for deposit-wallet accounts.
- Existing env names from older Polymarket bots are accepted as aliases:
  `POLYMARKET_API_KEY`, `POLYMARKET_API_SECRET`,
  `POLYMARKET_PASSPHRASE`, `POLYMARKET_PRIVATE_KEY`,
  `POLYMARKET_PROXY_ADDRESS`, `FUNDER_ADDRESS`, and `SIGNATURE_TYPE`.
- pUSD must sit in the funding wallet selected by `CLOB_FUNDER_ADDRESS`, not
  merely in the owner EOA. After funding or changing approvals, the bot calls
  balance/allowance sync with the selected signature type.
- Legacy builder users can set `POLY_BUILDER_CODE` to their bytes32 builder
  code. The bot attaches it to every live order.
- The CLOB order docs classify `FOK` and `FAK` as immediate execution order
  types, but for BUY orders they use dollar amount, not share count.
- Crypto taker fees use `shares * 0.07 * price * (1 - price)`. The
  `CORR_TAKER_REBATE_RATE` setting represents expected account-level cashback;
  it is intentionally configurable and is not the official maker rebate.
- SDK market BUY orders and `FOK`/`FAK` BUY types take dollar amount, not shares.
  Because this strategy needs "send 5 shares", the bot forces live BUYs through
  marketable GTC limit orders with `OrderArgs(size=5)` and immediately cancels
  any unfilled remainder. This behaves as share-sized IOC from the bot's
  perspective and prevents fills above the requested share count.
- Market and user websocket channels require text `PING` heartbeats every 10
  seconds. The bot sends `PING` every 8 seconds and ignores text `PING/PONG`
  frames before JSON parsing.
- Resolved CTF tokens redeem back into pUSD through Polymarket settlement
  tooling. This bot computes PnL from Gamma/UMA outcomes; redemption is handled
  by the Polymarket UI auto-redeem setting, not by this process.

## Live execution contract

Default live settings:

```text
DRY_RUN=false
CORR_COMBO_QTY=5
CORR_MAX_COMBOS_PER_ROUND=3
CORR_MAX_COST_PER_ROUND_USD=15
CORR_TTE_MAX_S=210
CORR_WEEKEND_REST_ENABLED=true
CORR_US_STOCK_HOURS_FILTER_ENABLED=true
CORR_US_STOCK_HOURS_MIN_LEG_PRICE=0.70
CORR_MIN_MODEL_EDGE=0.05
CORR_TAKER_FEE_RATE=0.07
CORR_TAKER_REBATE_RATE=0.30
CORR_ENTRY_EDGE_RESERVE=0.01
CORR_MIN_NET_MODEL_EDGE=0.02
CORR_EXEC_ORDER_TYPE=GTC
CORR_EXEC_SLIPPAGE_TICKS=2
CORR_EXEC_CHASE_SLIPPAGE_TICKS=1
CORR_EXEC_MAX_CHASE_ATTEMPTS=2
CORR_EXEC_RECONCILE_TIMEOUT_S=6.0
CORR_EXEC_RECONCILE_POLL_S=0.25
CORR_EXEC_FLATTEN_SLIPPAGE_TICKS=24
CORR_EXEC_FLATTEN_MAX_ATTEMPTS=3
CORR_LEG_MISMATCH_TOLERANCE_SHARES=1.0
CORR_USER_WS_ENABLED=false
CORR_PM_RESOLUTION_RETRY_S=60.0
```

Entry execution:

1. Before sending orders, require raw model edge `>= 0.05` and fee-adjusted
   net model edge `>= 0.02`. Net edge subtracts the expected two-leg crypto
   taker fee after configured account rebate and a per-share execution reserve.
2. Buy leg A and leg B with `size = 5` shares each, using aggressive GTC limits
   that are cancelled immediately after submission.
3. Limit price is `best_ask + slippage_ticks * tick_size`, capped at `0.99`.
4. If the two fills differ by at most 1 share, accept the residual and do not
   retry. This prevents the old 4.89 vs 5.00 infinite-retry failure.
5. Fill reconciliation treats CLOB submit/cancel/get_order as the execution
   source of truth. The authenticated user websocket is optional telemetry: it
   can add faster fill/price updates, but it cannot block order submission,
   downgrade a CLOB-confirmed fill, or turn a CLOB-confirmed fill into no-fill.
   Default live mode keeps `CORR_USER_WS_ENABLED=false` to avoid websocket
   latency/noise in the critical order path.
6. If the difference is more than 1 share, chase the underfilled leg by the
   real shortfall with 1 additional tick per chase attempt.
7. A confirmed zero-fill consumes one attempt but does not terminate the
   remaining finite chase attempts.
8. A replacement order is allowed only after CLOB proves the preceding order
   matched or its remainder was canceled. Unknown submission/cancel state
   blocks resubmission to prevent duplicate fills.
9. If the imbalance still remains after all chase attempts, the entry is
   aborted and the filled exposure is flattened by buying the opposite side.
   Emergency flatten starts immediately with the full 24-tick worst-price
   ceiling; retries refresh the current ask instead of widening 8/16/24.
10. Entry-abort residual exposure above the 1-share tolerance is retried every
   strategy cycle until flat or the market window ends. It does not wait for a
   fourth-quadrant signal.
11. `CORR_MAX_COMBOS_PER_ROUND` and `CORR_MAX_COST_PER_ROUND_USD` apply only to
   new entries. Entry-imbalance hedges and Q4 kill orders bypass those caps.

Time gates:

1. All time rules use UTC+8.
2. Weekend rest runs from UTC+8 Saturday 05:00 through Monday 05:00. During
   this window the bot does not start new trading rounds and suppresses
   Telegram notifications.
3. During US stock regular hours in UTC+8, Monday-Friday 21:30 through the
   next day 04:00, a candidate combo is accepted only when at least one leg is
   priced at or above `CORR_US_STOCK_HOURS_MIN_LEG_PRICE`.

Fourth-quadrant kill execution:

1. When the asymmetric Q4 kill rule fires, buy the opposite side of each open
   long leg for the remaining uncovered quantity.
2. Live kill orders use the same aggressive share-limit/cancel wrapper.
3. Partial kill fills are logged and included in PM-settled PnL; the bot does
   not pretend a kill completed if the CLOB fill response says otherwise.
4. User websocket connectivity does not block Q4 kill, entry-flatten, or
   imbalance-chase orders.

## Required environment variables

Shadow mode:

```text
DRY_RUN=true
TG_BOT_TOKEN=...
TG_CHAT_ID=...
```

Live mode:

```text
DRY_RUN=false
CLOB_API_URL=https://clob.polymarket.com
CHAIN_ID=137
PRIVATE_KEY=...
CLOB_API_KEY=...
CLOB_SECRET=...
CLOB_PASS_PHRASE=...
CLOB_SIGNATURE_TYPE=POLY_PROXY
CLOB_FUNDER_ADDRESS=...
POLY_BUILDER_CODE=...
TG_BOT_TOKEN=...
TG_CHAT_ID=...
```

Wallet-mode mapping:

| Account mode | `CLOB_SIGNATURE_TYPE` | `CLOB_FUNDER_ADDRESS` |
|---|---|---|
| Legacy Polymarket proxy | `POLY_PROXY` or `1` | Proxy wallet address |
| Existing Safe/Gnosis flow | `GNOSIS_SAFE` or `2` | Safe/proxy wallet address |
| EOA/direct wallet | `EOA` or `0` | EOA address matching `PRIVATE_KEY` |
| Deposit wallet | `POLY_1271` or `3` | Deposit wallet address |

Before switching to live:

- Confirm the selected funding wallet is correct for the signature mode.
- Confirm pUSD is in that funding wallet.
- Confirm collateral allowance is set from that funding wallet, not a different
  owner/signing address.
- Start once in `DRY_RUN=true` on EC2 and check websocket, Gamma, and
  Telegram logs.
- Switch to `DRY_RUN=false` only after balance/allowance sync succeeds at boot.

## Notifications and accounting

Telegram sends:

- boot config with dry/live execution mode,
- every entry fill with leg quantities, imbalance, gap, rho, and execution mode,
- every Q4 kill fill,
- every PM/UMA settlement, including cumulative run PnL,
- final summary for finite `--rounds` runs.

PnL uses PM/UMA outcome from Gamma only. Binance prices are recorded only for
divergence diagnostics and never drive realized PnL. If Gamma has not published
an outcome within one polling window, the background resolver waits
`CORR_PM_RESOLUTION_RETRY_S` and retries until the round is recorded.

## Known live risks

- The bot is taker-only. It is not designed to quote or manage resting GTC
  inventory.
- Redemption is not performed by this bot. Use Polymarket's own auto-redeem
  setting or manual UI redemption.
- EC2/systemd restarts can interrupt a round. Logs are append-only JSONL, but there
  is no persistent database reconciliation yet.
- Polymarket API or SDK response fields can change. Live fills are logged raw so
  response parsing can be corrected without losing audit data.
