# Runbook

## Shadow deployment

1. Copy `.env.example` to `.env`.
2. Keep `AFTERTAKE_DRY_RUN=true`.
3. Fill `TG_BOT_TOKEN` and `TG_CHAT_ID`.
4. Install `.[dev,live]` and run `aftertake --deployment-check`.
5. Confirm Telegram receives `DEPLOYMENT_CHECK_OK` with `mode=SHADOW`.
6. Start `aftertake --forever` and confirm `BOOT` arrives.

The shadow runner receives real Gamma/CLOB data and writes the same SQLite and
JSONL audit trail. A qualifying candidate reserves the round and records a
`shadow_no_order` result; no wallet key, signature or CLOB order is used.

## EC2

```bash
cd /opt/aftertake
git pull --ff-only
sudo bash deploy/ec2/deploy_aftertake.sh
sudo journalctl -u aftertake -f
```

The script loads `/opt/aftertake/.env`, runs the checked-in preflight before
systemd starts the loop, and keeps mutable state in `/var/lib/aftertake/out`.
It does not print secret values.

## Live promotion

Set `AFTERTAKE_DRY_RUN=false` plus the required CLOB V2 account identity in
`.env`, then re-run the deployment command. Do not delete SQLite state when
an `execution_unknown` alert exists: attach/reconcile the confirmed CLOB order
before another entry is permitted.

## Operator evidence

For each process start expect, in order: `DEPLOYMENT_CHECK_OK`, then `BOOT`.
For an actual entry expect `ORDER_SUBMITTED` followed by either
`ENTRY_CONFIRMED`, `ORDER_RESULT`, or `ALERT`; the acknowledgement alone is
not considered a fill. Settlements use only a resolved official Gamma outcome.
