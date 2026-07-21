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
state. SIGNAL and repeated ENTRY_BLOCKED reasons are deduplicated within each five-minute market.

## 3. Wallet bootstrap outside this process

For the existing `cor_pol` API/proxy account, reuse its `PRIVATE_KEY`, the matching
`CLOB_API_KEY/CLOB_SECRET/CLOB_PASS_PHRASE`, `CLOB_SIGNATURE_TYPE=POLY_PROXY`, and the actual proxy
wallet as `CLOB_FUNDER_ADDRESS`. This preserves the account identity while still using CLOB V2.

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

- dry-run is disabled, the acknowledgement is present, and the account identity is valid
- the official geo endpoint permits new orders for the current egress
- the CLOB is not in close-only mode
- pUSD balance and allowance are readable through the authenticated account
- persisted submitted orders reconcile to a terminal CLOB state

It does not prove legal eligibility, account standing, strategy profitability, or future
availability. It submits no trade, but may create/derive L2 API credentials when none were supplied.

## 5. Start service

```bash
misprice-pm --forever
```

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
