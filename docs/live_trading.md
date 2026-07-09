# Live trading runbook

This build runs `empjp_e75_n30_c1_l1`, a BTC-only 5-minute Polymarket strategy.
It preserves the old live execution wrapper but replaces the BTC/ETH correlation
entry logic.

## Safe startup

Default is shadow mode:

```text
DRY_RUN=true
```

Run one live-market smoke round first:

```bash
python main.py --rounds 1 --start-mode current
```

Only after websocket, Gamma, Telegram, and fill simulation look normal should you
switch to live:

```text
DRY_RUN=false
```

## Required live environment variables

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
| Safe/Gnosis flow | `GNOSIS_SAFE` or `2` | Safe/proxy wallet address |
| EOA/direct wallet | `EOA` or `0` | EOA address matching `PRIVATE_KEY` |
| Deposit wallet | `POLY_1271` or `3` | Deposit wallet address |

Before live:

- Confirm pUSD is in the selected funding wallet.
- Confirm the selected funding wallet matches `CLOB_SIGNATURE_TYPE`.
- Confirm collateral allowance after funding; the bot calls balance/allowance sync
  on boot in live mode.
- Do not put credentials in git.

## EMPJP runtime settings

```text
EMPJP_QTY=5.0
EMPJP_EDGE_MIN=0.075
EMPJP_MIN_CELL_N=30
EMPJP_CONFIRM_S=1
EMPJP_LATENCY_S=1
EMPJP_MIN_ENTRY_ELAPSED_S=45.0
EMPJP_MAX_ENTRY_ELAPSED_S=255.0
EMPJP_MIN_TTE_S=45.0
EMPJP_MAX_TTE_S=240.0
EMPJP_MIN_ASK=0.18
EMPJP_MAX_ASK=0.82
EMPJP_MAX_SPREAD=0.05
EMPJP_MIN_DEPTH=5.0
EMPJP_WEEKEND_REST_ENABLED=false
```

## Execution contract

1. Signal fires when the frozen empirical probability cell produces edge
   `>= EMPJP_EDGE_MIN`.
2. Bot waits `EMPJP_CONFIRM_S + EMPJP_LATENCY_S` seconds.
3. Bot samples the current ask and recomputes edge using the original signal
   probability.
4. If edge still passes, it buys `EMPJP_QTY` shares on the selected BTC side.
5. Live BUYs use a marketable GTC share-limit order and immediately cancel the
   remainder.
6. CLOB submit/cancel/get_order is the execution source of truth.
7. Authenticated user websocket is optional telemetry only.
8. Zero-fill or edge decay means no trade for that round.
9. Filled positions are held to PM/UMA settlement.

## Notifications and accounting

Telegram sends:

- boot config,
- ENTRY fill result,
- SETTLE result after Gamma/UMA resolution,
- no-trade skip diagnostics.

PnL is PM/UMA outcome based:

```text
pnl = qty * win - qty * entry_price - entry_fee
```

## Known live risks

- PM CLOB response fields can change; raw order responses are logged in JSONL.
- The bot does not redeem resolved CTF tokens; use Polymarket auto-redeem/UI.
- There is no persistent database yet. Runtime JSONL is append-only, but a process
  restart during an open round may need manual reconciliation from logs/CLOB.
- This is taker-only and intentionally aggressive; monitor slippage and no-fill
  rates before increasing size.
