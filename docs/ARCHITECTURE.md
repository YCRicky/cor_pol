# Architecture

```text
fresh 5m boundary
  -> real Binance open observation
  -> Gamma market + explicit outcome/token mapping
  -> timestamped CLOB books
  -> pure strategy decision
  -> live geo/wallet/market preflight
  -> SQLite entry reservation
  -> CLOB V2 GTC order + heartbeat
  -> get_order/trades -> cancel remainder -> terminal confirmation
  -> confirmed position -> official PM settlement
```

## Modules

- `config`: Misprice v3 repricing-lag controls, CLOB V2 identity, and fixed official endpoints.
- `pm_client`: plain official HTTPS public data and a narrow authenticated V2 adapter.
- `strategy`: pure BTC path-transition -> PM required/actual repricing -> lag-depth decision logic.
- `state`: authoritative SQLite WAL order/market/settlement state and atomic entry lock.
- `risk`: displayed-depth plus the strategy's daily-loss, open-position, loss-streak, and cooldown gates.
- `execution`: submit-once order lifecycle, CLOB heartbeat, cancel/reconcile, partial fills, and
  unknown-execution freeze.
- `runner`: fresh-boundary scheduler, source freshness checks, startup recovery, and PM settlement.
- `ledger`: fsynced JSONL audit mirror and PM-only read-side reconstruction.
- `engine`: market-clock and legacy offline compatibility helpers; not authoritative for live risk.

SQLite is the execution source of truth. JSONL is a durable human-readable audit mirror.
WebSocket or notification data may be added as telemetry, but correctness must continue to come
from authenticated CLOB order/trade queries.
