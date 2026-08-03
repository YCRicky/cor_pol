# Aftertake

Aftertake is a standalone multi-asset 5-minute Polymarket CLOB strategy. It does not
use a pre-close price-direction model or any outside price feed.

At the market frontend close, it watches the official public CLOB book for
50--250 ms. V8 waits until both token books are fresh, then uses two distinct
book events to confirm winner support and opposite-side vacuum. The initial
post-close leader must never reverse, and the loser side must fail to refill.
A missing loser bid is valid vacuum evidence; both bids missing or tied is not
a direction. It may take the supported side only while a displayed residual
ask is executable.
Dry-run keeps the configured quantity. Live mode sizes dynamically from the
displayed ask depth and account collateral risk budget.

A cheap ask on the bid-vacuum side is always rejected. See
[the strategy definition](docs/STRATEGY.md).

## First deployment: dry run

```bash
git clone https://github.com/YCRicky/aftertake.git
cd aftertake
python3 -m venv .venv
. .venv/bin/activate
pip install -e '.[dev,live]'
cp .env.example .env
# Fill TG_BOT_TOKEN and TG_CHAT_ID, retain AFTERTAKE_DRY_RUN=true.
aftertake --forever
```

`--deployment-check` is an optional manual diagnostic that sends no order. It requires an active Gamma market, a
paired snapshot from the official CLOB WebSocket, and a successful Telegram
message. In live mode it additionally checks official geo eligibility,
close-only status, pUSD balance/allowance, market metadata and the Gamma/CLOB
YES/NO token mapping.

The continuous process waits for a fresh 5-minute boundary. It then keeps the
WebSocket warm for the round, so it cannot backdate observations or act before
the next real close.

## Live mode

Only after the shadow results and operator logs are satisfactory, set:

```text
AFTERTAKE_DRY_RUN=false
POLYMARKET_PRIVATE_KEY=0x...
POLYMARKET_API_KEY=...
POLYMARKET_API_SECRET=...
POLYMARKET_PASSPHRASE=...
FUNDER_ADDRESS=0x...
SIGNATURE_TYPE="2"
AFTERTAKE_LIVE_MAX_ACCOUNT_RISK_FRACTION=0.50
AFTERTAKE_LIVE_QTY_FLOOR_STEP=1
```

You may run `aftertake --deployment-check` manually for diagnostics, but it is
not a daemon startup gate. `aftertake --forever` stays running and retries
Polymarket transport/account availability failures without sending an order.
Live execution is a single marketable GTC limit submission at the already
observed winner-side ask. The submitted size is the largest floor-sized quantity
that does not exceed displayed ask depth, collateral allowance, or
`collateral_balance * AFTERTAKE_LIVE_MAX_ACCOUNT_RISK_FRACTION`. With the
default integer floor step, a calculated 67.5 shares is submitted as 67, never
68. That final quantity must also pass the same bid-support checks as the
candidate; insufficient support blocks the order. A default GTC submission is
not locally cancelled after five seconds; it remains `submitted` until later
CLOB reconciliation or official settlement resolves it. The bot never
automatically retries the same market.

Telegram reports `DEPLOYMENT_CHECK_OK`, `BOOT`, `ORDER_SUBMITTED`, actual
`ENTRY_CONFIRMED` or `ORDER_RESULT`, `ENTRY_BLOCKED`, `ALERT`, and official
Polymarket `SETTLE`. It does **not** send a separate signal or round-level
no-entry notification. Every scheduled round instead persists `round_started`
and `round_complete` SQLite audit rows, including each asset's final action and
reason, without adding Telegram noise.

For EC2, Aftertake deliberately keeps the existing cor_pol deployment identity:
use [deploy/ec2/deploy_cor_pol.sh](deploy/ec2/deploy_cor_pol.sh) and
[deploy/systemd/cor-pol.service.example](deploy/systemd/cor-pol.service.example).
The deployment script installs the checked-in systemd unit directly into
`aftertake --forever`; it has no `ExecStartPre` network gate. Polymarket
WebSocket/Gamma/CLOB interruptions suppress the affected round and reconnect
without marking the service failed.

## Railway

Create one Railway persistent service from this GitHub repository, attach one
volume at `/data`, and add the values from `.env.example` as Railway service
variables. Secrets such as the private key, CLOB credentials, and Telegram
token belong only in Railway Variables; do not upload `.env`. On Railway set
`AFTERTAKE_OUT_DIR=/data/out` (do not copy the local `out` value).

The checked-in `Dockerfile` and `railway.json` run exactly one background worker
with SQLite and runtime logs under `/data/out`. No public domain, `PORT`, or HTTP
healthcheck is required. Select a Railway region where the Polymarket geo
preflight is permitted, then set `AFTERTAKE_DRY_RUN=false` only for the live
service. Normal heartbeat, market-stream, reconciliation, and per-asset
transport failures alert and continue; Railway restarts only an actual crashed
process, with at most three retries.

See [RUNBOOK.md](docs/RUNBOOK.md), [SAFETY.md](docs/SAFETY.md), and
[ARCHITECTURE.md](docs/ARCHITECTURE.md).

Default market universe:

```text
AFTERTAKE_ASSETS=BTC,ETH,XRP,HYPE,DOGE,SOL
```

The runner observes every configured 5-minute asset market for the same round boundary concurrently.
