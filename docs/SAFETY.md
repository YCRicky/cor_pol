# Safety Policy

## Fail-closed defaults

```text
MISPRICE_DRY_RUN=true
```

Changing `MISPRICE_DRY_RUN` to `false` still requires the exact live acknowledgement and valid V2
wallet identity before an authenticated client is constructed.

## Protocol checks

Every real entry rechecks:

- official geo response and CLOB close-only mode
- pUSD balance and allowance, including the final executable order and platform fee
- active Gamma market and condition ID
- explicit outcome-to-token agreement between Gamma and CLOB
- CLOB tick size, minimum order size, neg-risk, and fee parameters
- orderbook source timestamps and displayed best-ask depth

The live endpoint and geo endpoint cannot be replaced through environment variables.

## Order lifecycle

An entry intent is committed to SQLite before any network submission. A CLOB acknowledgement is not
a fill. The runtime polls authenticated order/trade state, cancels the short-lived GTC remainder,
and records only confirmed matched quantity.

Timeout, missing order ID, non-terminal cancel, process interruption, or ambiguous SDK error becomes
`execution_unknown`. New entries freeze. The process does not guess or create a new signed order.
The only transport replay is a bounded HTTP 425 retry of the exact same signed order object.

## Risk

- one entry reservation per market
- one process per state database
- requested quantity cannot exceed displayed best-ask depth
- the strategy's daily-loss, open-position, consecutive-loss, and entry-cooldown limits
- notifications cannot change execution state

Telegram reports the operator-visible lifecycle (signal, confirmed fill, no-fill/cancel, blocked or
unknown execution, no-entry round, and settlement). A Telegram API response is successful only when
its JSON body contains `ok=true`. Submission notification is dispatched off the reconciliation
thread so Telegram latency cannot extend the order TTL. The managed worker is drained after terminal
reconciliation and during shutdown; a bounded drain timeout is audited before SQLite closes.
Notification failures never trigger order retries.

## Secrets and geography

Never commit `.env`, signer keys, CLOB L2 credentials, or Telegram tokens.
Do not use a proxy, custom DNS/IP mapping, or alternate endpoint to evade geographic restrictions.
If the official response blocks the current egress, the runtime refuses new orders.

## Settlement

Only official PM/Gamma resolution settles PnL. Actual fill quantity and average price are required.
Actual recorded fee is preferred; otherwise the per-market fee parameters captured at entry are
used. Settlement rows are immutable and conflicting duplicates are rejected.
