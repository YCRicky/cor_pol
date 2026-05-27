# Live trading runbook

This document locks the current live-trading design before Railway deployment.
It is based on the Polymarket docs reviewed on 2026-05-27:

- Polymarket USD: https://docs.polymarket.com/concepts/pusd
- Deposit wallets: https://docs.polymarket.com/trading/deposit-wallets
- CLOB create order: https://docs.polymarket.com/developers/CLOB/orders/create-order
- CLOB Python client: https://docs.polymarket.com/developers/CLOB/clients/python-client
- CTF redeem: https://docs.polymarket.com/trading/ctf/redeem
- Relayer API keys: https://docs.polymarket.com/api-reference/relayer-api-keys/get-all-relayer-api-keys
- Relayer submit: https://docs.polymarket.com/api-reference/relayer/submit-a-transaction

## Polymarket changes that matter

- Collateral is pUSD on Polygon. Do not assume old bridged-USDC wording in
  logs, config, or runbooks.
- New API users should use a deposit wallet, `signature_type = POLY_1271`
  (`3`), and pass the deposit wallet as the CLOB `funder`.
- pUSD must sit in the deposit wallet, not just the owner EOA. After funding or
  changing approvals, call balance/allowance sync with `signature_type = 3`.
- The CLOB order docs classify `FOK` and `FAK` as immediate execution order
  types. `FAK` fills whatever is available immediately and cancels the rest.
- SDK market BUY orders take `amount` as dollars/pUSD, not shares. Because this
  strategy needs "send 5 shares", the bot uses marketable aggressive limit
  orders with `OrderArgs(size=5)` and `OrderType.FAK` instead of SDK market BUY.
- Resolved CTF tokens redeem back into pUSD through the pUSD adapter flow. This
  bot computes PnL from Gamma/UMA outcomes first, then can optionally submit
  on-chain relayer redeem batches after settlement.

## Live execution contract

Default live settings:

```text
DRY_RUN=false
CORR_COMBO_QTY=5
CORR_MAX_COMBOS_PER_ROUND=3
CORR_MAX_COST_PER_ROUND_USD=15
CORR_EXEC_ORDER_TYPE=FAK
CORR_EXEC_SLIPPAGE_TICKS=2
CORR_EXEC_CHASE_SLIPPAGE_TICKS=4
CORR_EXEC_MAX_CHASE_ATTEMPTS=2
CORR_LEG_MISMATCH_TOLERANCE_SHARES=1.0
```

Entry execution:

1. Buy leg A and leg B with `size = 5` shares each, using aggressive FAK limits.
2. Limit price is `best_ask + slippage_ticks * tick_size`, capped at `0.99`.
3. If the two fills differ by at most 1 share, accept the residual and do not
   retry. This prevents the old 4.89 vs 5.00 infinite-retry failure.
4. If the difference is more than 1 share, chase the underfilled leg by the
   real shortfall with wider slippage.
5. If the imbalance still remains after all chase attempts, buy the opposite
   side of the overfilled leg for `imbalance - tolerance`. That converts excess
   naked exposure back toward locked $1 payoff instead of leaving a one-leg bet.

Fourth-quadrant kill execution:

1. When the asymmetric Q4 kill rule fires, buy the opposite side of each open
   long leg for the remaining uncovered quantity.
2. Live kill orders use the same aggressive FAK wrapper.
3. Partial kill fills are logged and included in PM-settled PnL; the bot does
   not pretend a kill completed if the CLOB fill response says otherwise.

## Required Railway variables

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
DEPOSIT_WALLET_ADDRESS=...
CLOB_SIGNATURE_TYPE=POLY_1271
TG_BOT_TOKEN=...
TG_CHAT_ID=...
```

`CLOB_API_KEY`, `CLOB_SECRET`, and `CLOB_PASS_PHRASE` are optional. If they are
not set, the live executor derives CLOB credentials from `PRIVATE_KEY` during
boot.

Auto redeem:

```text
DRY_RUN=false
CORR_AUTO_REDEEM=true
RELAYER_URL=https://relayer-v2.polymarket.com/
RELAYER_API_KEY=...
RELAYER_API_KEY_ADDRESS=...
CORR_REDEEM_WAIT=true
CORR_REDEEM_DEADLINE_S=600
```

Before switching to live:

- Confirm the deposit wallet is deployed.
- Confirm pUSD is in the deposit wallet.
- Confirm collateral allowance is set from the deposit wallet, not the owner EOA.
- Start once in `DRY_RUN=true` on Railway and check websocket, Gamma, and
  Telegram logs.
- Switch to `DRY_RUN=false` only after balance/allowance sync succeeds at boot.

## Notifications and accounting

Telegram sends:

- boot config with dry/live execution mode,
- every entry fill with leg quantities, imbalance, gap, rho, and execution mode,
- every Q4 kill fill,
- every PM/UMA settlement, including cumulative run PnL,
- every auto-redeem relayer submission/result when enabled,
- final summary for finite `--rounds` runs.

PnL uses PM/UMA outcome from Gamma only. Binance prices are recorded only for
divergence diagnostics and never drive realized PnL.

## Known live risks

- The bot is taker-only. It is not designed to quote or manage resting GTC
  inventory.
- Auto redeem is an on-chain relayer action from the deposit wallet. It runs
  only after PM/UMA settlement and only when `CORR_AUTO_REDEEM=true`.
- Railway restarts can interrupt a round. Logs are append-only JSONL, but there
  is no persistent database reconciliation yet.
- Polymarket API or SDK response fields can change. Live fills are logged raw so
  response parsing can be corrected without losing audit data.

## Auto redeem details

Deposit-wallet positions are held by the deposit wallet contract, so the owner
EOA cannot redeem them by directly sending a transaction from the EOA. The bot
signs a deposit-wallet `WALLET` batch and submits it through the current
Polymarket Relayer API headers, making the deposit wallet call the pUSD CTF
collateral adapter.
`CORR_AUTO_REDEEM=true` is accepted only in live mode; it fails fast when
`DRY_RUN=true` so a shadow run cannot redeem unrelated manual positions.
Authentication uses the current Relayer API headers:
`RELAYER_API_KEY` and `RELAYER_API_KEY_ADDRESS`.

For each resolved round with at least one combo, the bot submits one redeem
call for the BTC condition and one for the ETH condition:

```text
redeemPositions(pUSD, bytes32(0), conditionId, [1, 2])
```

The target adapter is:

- `CTF_COLLATERAL_ADAPTER` for normal markets.
- `NEG_RISK_CTF_COLLATERAL_ADAPTER` if Gamma marks the market `negRisk=true`.

Redeem has no amount parameter. It burns the redeemable token balances for that
condition and returns pUSD to the deposit wallet.
