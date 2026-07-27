# Architecture

```text
Gamma market discovery
        |
        v
Official public CLOB WebSocket (YES + NO paired books)
        |
        +--> pre-close contested/reversal scene gate
        |
        +--> T+100..1000ms bid-support / bid-vacuum classifier
                              |
                              v
               winner-side residual ask + local risk check
                              |
                              v
             SQLite reservation -> single CLOB taker-intent order; default FAK, optional bounded GTC/GTD
                              |
                              v
                 reconciliation -> Telegram lifecycle -> PM settlement
```

`aftertake.market_stream` consumes only the public official market WebSocket.
`aftertake.pm_client` handles Gamma discovery and, only in live mode, the
authenticated `py-clob-client-v2` gateway. `aftertake.state` is the durable
source of entry, order, recovery and settlement state. `aftertake.execution`
does not retry ambiguous submissions and freezes later entries through state.

Telegram is intentionally outside the time-sensitive classifier path. It
observes lifecycle events but cannot place, repeat, delay or cancel an order.
