# Execution safety

- The default is `AFTERTAKE_DRY_RUN=true`.
- A live start requires valid CLOB V2 credentials, funder/signature identity,
  official geoblock clearance, non-close-only account status, pUSD balance and
  allowance, CLOB market metadata and matching Gamma outcome token IDs.
- Live mode performs an official pre-close account/allowance check, then sizes
  the post-close entry from the already observed websocket ask. Worst-case cost
  including market and Builder taker fees is capped by
  `AFTERTAKE_LIVE_MAX_ACCOUNT_RISK_FRACTION` of the CLOB collateral balance and
  by available allowance.
- The calculated final live quantity must pass the same five bid-support checks
  as the candidate. A small shadow quantity never authorizes a larger order.
- It submits one GTC limit order, has a five-second lifetime, and explicitly
  reconciles/cancels it. It never auto-retries a possibly submitted order.
- SQLite WAL persists one reservation per market. Unknown execution freezes
  new risk until an operator reconciles it.
- Dry-run and live mode share the same sizing formula. Dry-run uses simulated
  collateral (`AFTERTAKE_DRY_RUN_SIM_BALANCE`, default `100`) and still never
  submits an order; live mode uses actual CLOB pUSD balance/allowance.
- Execution does not use a fixed five-share quantity: it takes as much displayed
  ask depth as allowed by the account risk budget, then floors size to
  `AFTERTAKE_LIVE_QTY_FLOOR_STEP`.

- Telegram failures are recorded but never alter order state or trigger a retry.
- Polymarket DNS overrides are disabled by default in the EC2 deployment path.
  `AFTERTAKE_RESOLVE_OVERRIDES` is an explicit opt-in emergency guard for
  RPZ-poisoned environments; TLS verification remains enabled when used.
