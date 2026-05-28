# Live trading runbook

This document locks the current live-trading design before EC2 deployment.
It is based on the Polymarket docs reviewed on 2026-05-27:

- Polymarket USD: https://docs.polymarket.com/concepts/pusd
- Deposit wallets: https://docs.polymarket.com/trading/deposit-wallets
- CLOB create order: https://docs.polymarket.com/developers/CLOB/orders/create-order
- CLOB Python client: https://docs.polymarket.com/developers/CLOB/clients/python-client

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
  types. `FAK` fills whatever is available immediately and cancels the rest.
- SDK market BUY orders take `amount` as dollars/pUSD, not shares. Because this
  strategy needs "send 5 shares", the bot uses marketable aggressive limit
  orders with `OrderArgs(size=5)` and `OrderType.FAK` instead of SDK market BUY.
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
CORR_EXEC_ORDER_TYPE=FAK
CORR_EXEC_SLIPPAGE_TICKS=2
CORR_EXEC_CHASE_SLIPPAGE_TICKS=1
CORR_EXEC_MAX_CHASE_ATTEMPTS=2
CORR_LEG_MISMATCH_TOLERANCE_SHARES=1.0
```

Entry execution:

1. Buy leg A and leg B with `size = 5` shares each, using aggressive FAK limits.
2. Limit price is `best_ask + slippage_ticks * tick_size`, capped at `0.99`.
3. If the two fills differ by at most 1 share, accept the residual and do not
   retry. This prevents the old 4.89 vs 5.00 infinite-retry failure.
4. Fill reconciliation prefers the authenticated user websocket. If the
   websocket misses an update but the synchronous CLOB post response already
   reports a matched amount or an immediate terminal no-fill status, the bot
   uses that post acknowledgement as provisional execution truth. It does not
   call REST `get_order` as a fallback.
5. If the difference is more than 1 share, chase the underfilled leg by the
   real shortfall with 1 additional tick per chase attempt.
6. If the imbalance still remains after all chase attempts, the entry is
   aborted and the filled exposure is flattened by buying the opposite side.
7. `CORR_MAX_COMBOS_PER_ROUND` and `CORR_MAX_COST_PER_ROUND_USD` apply only to
   new entries. Entry-imbalance hedges and Q4 kill orders bypass those caps.

Fourth-quadrant kill execution:

1. When the asymmetric Q4 kill rule fires, buy the opposite side of each open
   long leg for the remaining uncovered quantity.
2. Live kill orders use the same aggressive FAK wrapper.
3. Partial kill fills are logged and included in PM-settled PnL; the bot does
   not pretend a kill completed if the CLOB fill response says otherwise.

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
divergence diagnostics and never drive realized PnL.

## Known live risks

- The bot is taker-only. It is not designed to quote or manage resting GTC
  inventory.
- Redemption is not performed by this bot. Use Polymarket's own auto-redeem
  setting or manual UI redemption.
- EC2/systemd restarts can interrupt a round. Logs are append-only JSONL, but there
  is no persistent database reconciliation yet.
- Polymarket API or SDK response fields can change. Live fills are logged raw so
  response parsing can be corrected without losing audit data.
