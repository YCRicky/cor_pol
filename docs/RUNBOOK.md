# Runbook

## Shadow deployment

1. Copy `.env.example` to `.env`.
2. Keep `AFTERTAKE_DRY_RUN=true`.
3. Fill `TG_BOT_TOKEN` and `TG_CHAT_ID`.
4. Install `.[dev,live]`; `aftertake --deployment-check` is optional manual diagnostics.
5. Start `aftertake --forever` and confirm `BOOT` arrives.

The shadow runner receives real Gamma/CLOB data and writes the same SQLite and
JSONL audit trail. A qualifying candidate reserves the round and records a
`shadow_no_order` result; no wallet key, signature or CLOB order is used.

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

## Live promotion

Set `AFTERTAKE_DRY_RUN=false` plus the required CLOB V2 account identity in
`.env`, then re-run the deployment command. Do not delete SQLite state just to
hide diagnostics. Pending GTC orders should remain submitted until later CLOB
reconciliation / official settlement; submit-path infrastructure failures skip
only the affected market and do not block unrelated future entries.

## Operator evidence

For each process start expect `BOOT`. `DEPLOYMENT_CHECK_OK` appears only after
an operator manually runs `--deployment-check`; it is not a service gate.
For an actual entry expect `ORDER_SUBMITTED` followed by either
`ENTRY_CONFIRMED`, `ORDER_RESULT`, or `ALERT`; the acknowledgement alone is
not considered a fill. Settlements use only a resolved official Gamma outcome.
