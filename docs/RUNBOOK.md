# Runbook

## Shadow deployment

1. Copy `.env.example` to `.env`.
2. Keep `AFTERTAKE_DRY_RUN=true`.
3. Fill `TG_BOT_TOKEN` and `TG_CHAT_ID`.
4. Install `.[dev,live]`; `aftertake --deployment-check` is optional manual diagnostics.
5. Start `aftertake --forever` and confirm `BOOT` arrives.

The shadow runner receives real Gamma/CLOB and Binance Futures data and writes the
same SQLite/JSONL audit trail. It waits for a fresh candle boundary, requires a
Gamma-declared 30-second TWAP market, then records the one `E-10.25s` decision
or its fail-closed reason. No wallet key, signature, or CLOB order is used.

## EC2

```bash
cd /opt/cor_pol
git pull --ff-only
sudo bash deploy/ec2/deploy_cor_pol.sh
sudo journalctl -u cor-pol -f
```

The script loads `/opt/cor_pol/.env`, starts the checked-in continuous runner
without an `ExecStartPre` network gate, and keeps mutable state in
`/var/lib/cor-pol/out`. Polymarket interruptions retry inside the running
process and do not print secret values.

The multi-asset supervisor gives each same-boundary scan a 90-second minimum
bound, extended only when the worker is legitimately carrying the active close
window and its bounded reconciliation grace. A timed-out asset is recorded as
`asset_round_timeout`; the service then exits deliberately because Python
cannot safely kill a worker blocked in an SDK call. The checked-in systemd unit
uses `Restart=always`, so the process is recreated and SQLite recovery handles
any reserved intent without replaying an ambiguous order. An active main-loop
stall is likewise terminated by the 180-second runtime watchdog. Normal
waiting for the next five-minute boundary is explicitly exempt from that
watchdog.

The PM market WebSocket has a 5-second keepalive and a 12-second transport
watchdog. Binance Futures `aggTrade` is collected independently. A PM or Futures
disconnect/reconnect during the candle invalidates the tail round; the service
does not reconstruct an incomplete tape. For live orders,
the CLOB heartbeat is sent every 4 seconds. If Polymarket returns a
400 invalid/expired heartbeat id, the replacement id in that response is
adopted and retried immediately instead of repeating the stale id. Each
heartbeat SDK call is bounded and overlapping hung heartbeat requests are
suppressed, so a transient client stall cannot create an unbounded heartbeat
thread pile-up.

## Live promotion

Set `AFTERTAKE_DRY_RUN=false` plus the required CLOB V2 account identity in
`.env`, then re-run the deployment command. Do not delete SQLite state just to
hide diagnostics. Pending GTC orders should remain submitted until later CLOB
reconciliation / official settlement; submit-path infrastructure failures skip
only the affected market and do not block unrelated future entries.

## Operator evidence

For each process start expect `BOOT`. `RUNTIME_READY` appears when the scheduler
is ready in both dry-run and live mode. Each eligible round emits either
`aftertake_decision`/`twap_tail_hold` or an `ORDER_SUBMITTED` lifecycle record.
`DEPLOYMENT_CHECK_OK` appears only after
an operator manually runs `--deployment-check`; it is not a service gate.
For an actual entry expect `ORDER_SUBMITTED` followed by either
`ENTRY_CONFIRMED`, `ORDER_RESULT`, or `ALERT`; the acknowledgement alone is
not considered a fill. Settlements use only a resolved official Gamma outcome.
Every runtime/transport/heartbeat error now emits an `ALERT` immediately;
when the same component returns to a healthy state it emits
`RECOVERY_SUCCESS`. A process restart is visible as a fresh `BOOT` message.
