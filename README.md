# Misprice PM

Standalone BTC 5-minute Polymarket strategy with a fail-closed CLOB V2 execution layer.

The strategy thesis is now the Misprice v3 repricing-lag detector: enter only after a BTC
path transition when the executable PM ask has measurably lagged its required repricing and
that lag survives the final book check. Binance is signal-only. Official Polymarket/Gamma
outcomes are the only settlement source.

## What is production-grade

- pinned `py-clob-client-v2`; no retired V1 client
- explicit signer, signature type, deposit-wallet funder, pUSD balance and allowance checks
- official geo and CLOB close-only checks before every real entry
- fresh Gamma/CLOB metadata: outcome mapping, tick size, minimum size, neg-risk, platform fee,
  and builder taker fee
- SQLite WAL state written before submission, one entry per market, single-process lock
- GTC limit order with worst-price protection, short TTL, explicit cancel, fill reconciliation,
  and CLOB heartbeat
- ambiguous submission/cancellation becomes `execution_unknown`; it is never blindly retried
- confirmed fills, partial fills, PM-only settlement, and the strategy's original daily-loss,
  open-position, loss-streak, and cooldown controls survive restarts

Historical names such as `four_quadrant_v3` are dataset names only, not strategy names.
The canonical strategy identity here is `Misprice v3 repricing-lag detector`.

This is trading-capable software, not proof that an account, operator, or location is eligible.
The process refuses new orders whenever the official geo endpoint blocks the current egress.

## Install and verify

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -e '.[dev,live]'
cp .env.example .env
pytest -q
misprice-pm --status
misprice-pm --preflight --dry-run
```

Run a continuous shadow service:

```bash
misprice-pm --forever --dry-run
```

It waits for the next real 5-minute boundary. It never invents or backdates an opening BTC price.

## Live account settings

Live mode requires all of the following locally:

```text
MISPRICE_DRY_RUN=false
MISPRICE_LIVE_ACK=I_UNDERSTAND_THIS_PLACES_REAL_POLYMARKET_ORDERS
PRIVATE_KEY=0x...
CLOB_SIGNATURE_TYPE=POLY_PROXY
CLOB_FUNDER_ADDRESS=0x...
CLOB_API_KEY=...
CLOB_SECRET=...
CLOB_PASS_PHRASE=...
```

These names match `cor_pol`. For the existing API/proxy account, `POLY_PROXY` maps to signature type
`1` and the funder must be that proxy wallet. Existing Safe accounts use `2`; EOA accounts use `0`.
Only new deposit-wallet users use `POLY_1271`/`3`. All account types still submit through the current
V2 SDK and order format. The official CLOB host and Polygon mainnet chain are fixed in code, so they
are not `.env` settings. Never commit `.env`, private keys, or L2 credentials.

Before a live service:

```bash
misprice-pm --preflight
misprice-pm --forever
```

`--preflight` sends no order. It may derive L2 API credentials when none are supplied, then performs
authenticated checks and startup reconciliation. If an earlier execution remains unknown, new risk
stays frozen until the CLOB evidence is reconciled.

## Telegram lifecycle

Set the same variables used by `cor_pol`:

```text
TG_BOT_TOKEN=...
TG_CHAT_ID=...
```

The runtime reports BOOT, strategy SIGNAL, confirmed ENTRY, zero-fill/cancel ORDER_RESULT,
ENTRY_BLOCKED, execution ALERT, one NO_ENTRY summary per inactive round, and official PM SETTLE.
Telegram must return `ok=true`; rejected or failed messages are written to the SQLite/JSONL audit
trail and never change order state or cause an order retry.

See [RUNBOOK.md](docs/RUNBOOK.md), [SAFETY.md](docs/SAFETY.md), and
[ARCHITECTURE.md](docs/ARCHITECTURE.md).
