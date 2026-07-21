# Runbook

## 1. Install

Python `>=3.9.10` is required by the pinned V2 SDK.

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -e '.[dev,live]'
cp .env.example .env
pytest -q
```

## 2. Shadow soak

Keep the default dry-run mode:

```bash
misprice-pm --status
misprice-pm --forever --dry-run
```

Verify fresh-book decisions, boundary capture timing, SQLite recovery, JSONL durability, and
resource stability over multiple market cycles. A successful zero-order run validates plumbing,
not alpha.

With `TG_BOT_TOKEN` and `TG_CHAT_ID` set, send a no-order BOOT smoke message and exit:

```bash
misprice-pm --rounds 0 --dry-run
```

Confirm the message arrives before enabling live mode. During operation, the audit trail records
`notification_sent` or a sanitized `notification_failed`; Telegram failure never changes order
state. There are no pre-entry SIGNAL notifications; repeated ENTRY_BLOCKED reasons are deduplicated
within each five-minute market.

## 3. Wallet bootstrap outside this process

Use the deployment account fields exactly as named in `.env.example`: `POLYMARKET_PRIVATE_KEY`,
`POLYMARKET_API_KEY`, `POLYMARKET_API_SECRET`, `POLYMARKET_PASSPHRASE`, `FUNDER_ADDRESS`, and
`SIGNATURE_TYPE`. Set `CLOB_API_URL`, `CHAIN_ID`, and (when applicable) `POLY_BUILDER_CODE` there too.

Use the three existing CLOB L2 values together. After onchain approval is confirmed, explicitly
refresh the CLOB cache:

```bash
misprice-pm --sync-allowance
```

This command performs no order submission. It still requires the live identity and geo gates.

## 4. Live preflight

Set the live variables in the local secret store, then run:

```bash
misprice-pm --preflight
```

Passing means:

- dry-run is disabled and the account identity is valid
- the official geo endpoint permits new orders for the current egress
- the CLOB is not in close-only mode
- pUSD balance and allowance are readable through the authenticated account
- persisted submitted orders reconcile to a terminal CLOB state

It does not prove legal eligibility, account standing, strategy profitability, or future
availability. It submits no trade and requires the supplied static L2 API credentials.

## 5. Start service

```bash
misprice-pm --forever
```

For `deploy/systemd/misprice-pm.service.example`, copy the same completed
environment file to `/etc/misprice-pm.env`.  The unit uses
`StateDirectory=misprice-pm` and a `/var/lib/misprice-pm` working directory,
so the existing `MISPRICE_OUT_DIR=out` writes durable SQLite, lock, and JSONL
files to `/var/lib/misprice-pm/out` even with `ProtectSystem=strict`.  Do not
point the unit's working directory back to `/opt/misprice_pm`.

For this repository deployed at `/opt/cor_pol` as the `ubuntu` user, install
`deploy/systemd/cor-pol.service.example` as `/etc/systemd/system/cor-pol.service`.
It reads the existing `/opt/cor_pol/.env` directly and starts the installed
`python -m misprice_pm.runner --forever` module from `/opt/cor_pol/src`; it
never references the removed legacy `main.py` entrypoint.

That unit runs `--deployment-check` before every strategy process. The check
submits no order. In live mode it requires a current Binance BTC observation,
the active Gamma BTC 5m market and both CLOB books, authenticated CLOB wallet
and market metadata, writable state, and a successful Telegram API response.
It emits `DEPLOYMENT_CHECK_OK` before the normal `BOOT` message. A failed
check prevents `--forever` from starting; systemd retries at most six times in
one minute rather than looping indefinitely.

To apply an update without forgetting the virtualenv or the systemd unit:

```bash
cd /opt/cor_pol
git pull --ff-only
bash deploy/ec2/deploy_cor_pol.sh
```

The script checks the existing account and Telegram field names without
printing their values, installs `.[live]`, replaces `cor-pol.service`, reloads
systemd, and restarts the service. It exits nonzero instead of starting a
partial live deployment.

Only one process may own the SQLite runtime lock. The scheduler waits for the next boundary rather
than joining a market mid-round.

## 6. Incident handling

If status reports `execution_unknown=true`:

1. Stop the service; keep `MISPRICE_DRY_RUN=true` before any restart.
2. Inspect the deposit wallet, CLOB open orders, authenticated order history, and trades.
3. Do not delete the SQLite database, reuse the slug, or blindly resubmit.
4. If the missing order ID is recovered from CLOB history, attach and reconcile it:

   ```bash
   misprice-pm --attach-order-id INTENT_ID --order-id CLOB_ORDER_ID
   ```

5. Restart with `--preflight` only after the order ID and fill state are known.

Submission ambiguity intentionally requires operator reconciliation. Cancellation calls can retry
inside the bounded window because cancellation is idempotent. HTTP 425 retries reuse the exact same
signed V2 order for a short bounded backoff; the runtime never creates a second order intent.

## 7. Settlement and audit

The service polls official Gamma resolution for confirmed open fills. Actual trade fee fields are
preferred; otherwise the fee parameters captured immediately before entry are used.

```bash
misprice-pm --ledger
```

For a historical/manual record:

```bash
misprice-pm --settle SLUG --side YES --entry-price 0.60 --qty 5 --fee-rate 0.07
```

Use `--entry-fee` instead when the actual total fee is known. Add `--fee-exponent` and
`--builder-fee-bps` only when estimating from captured fee parameters. Only
`settlement_source=pm` is counted; Binance and other proxies are never accepted.
