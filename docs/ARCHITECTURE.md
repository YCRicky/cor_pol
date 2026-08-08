# Architecture

```text
Gamma market metadata ──► 30s-TWAP eligibility gate
                                  │
PM public CLOB WS ─► paired quote buffer ─┤
Binance USD-M Futures aggTrade ─► complete tape ┤──► E-10.25s causal decision
                                  │                    │
State/risk/fees/account preflight ────────┘                    ▼
                                                       one GTC limit (.99 cap)
                                                               │
                                                     CLOB reconciliation
                                                               │
                                                official PM/Gamma settlement
```

`src/aftertake/twap_tail.py` is pure decision logic. It accepts timestamped CLOB quotes and Futures aggregate
trades, filters both at the decision timestamp, and returns ENTER/HOLD plus audit fields. It has no HTTP,
WebSocket, wallet, or execution side effects.

`src/aftertake/binance_proxy.py` owns the bounded Futures tape. A connection started after candle open,
disconnect/reconnect during the candle, missing tape, or buffer overflow marks that round incomplete.

`src/aftertake/runner.py` owns schedule, Gamma metadata gate, streams, live account checks, risk, SQLite
reservation, order submission and audit. It waits for a fresh boundary after startup so it cannot synthesize
coverage for a mid-candle restart.

The old post-close modules remain offline compatibility/research code only; `run_round()` dispatches solely
to `_run_twap_tail_round()`.
