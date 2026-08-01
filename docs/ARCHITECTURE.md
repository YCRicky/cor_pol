# Architecture

```text
Gamma market discovery
        |
        v
Official public CLOB WebSocket (YES + NO paired books)
        |
        +--> pre-close contested/reversal scene gate
        |
        +--> T+50..250ms stable-leader + loser-refill-failure classifier
                              |
                              v
               winner-side residual ask + local risk check
                              |
                              v
             SQLite reservation -> single CLOB GTC taker-intent order; pending until CLOB reconciliation / settlement
                              |
                              v
                 reconciliation -> Telegram lifecycle -> PM settlement
```

`aftertake.market_stream` consumes only the public official market WebSocket.
It sends the provider keepalive, detects a silent socket when neither PONG nor
market data arrives, and reconnects with bounded exponential backoff. Each
reconnect clears the paired book generation; the classifier also clears its
history so stale pre-close evidence cannot cross a dead connection.
`aftertake.pm_client` handles Gamma discovery and, only in live mode, the
authenticated `py-clob-client-v2` gateway. `aftertake.state` is the durable
source of entry, order, recovery and settlement state. `aftertake.execution`
does not retry ambiguous submissions and freezes later entries through state.
The authenticated order heartbeat adopts the replacement id returned by an
expired/invalid-id response, so a stale heartbeat id cannot loop forever.

Telegram is intentionally outside the time-sensitive classifier path. It
observes lifecycle events but cannot place, repeat, delay or cancel an order.
Transport and runtime faults are sent as `ALERT` events without state-change
deduplication; a later healthy heartbeat, stream generation, asset round, or
runtime bootstrap emits `RECOVERY_SUCCESS`.

Runtime liveness is layered: per-asset rounds have a bounded supervisor;
startup/reconciliation/settlement errors are isolated per order or position;
and a 180-second stale-progress watchdog terminates a genuinely wedged process
so systemd can restart it. The watchdog is not a claim that arbitrary kernel
or provider failures are impossible; it defines the recovery boundary for the
known Python/HTTP/WebSocket failure classes and leaves durable state fail-closed.
