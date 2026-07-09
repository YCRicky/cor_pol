# cor_pol

BTC 5-minute Polymarket EMPJP live/shadow bot.

This build replaces the previous BTC/ETH correlation-arb strategy with the
current production candidate:

```text
empjp_e75_n30_c1_l1
```

Meaning:

- `empjp`: empirical joint probability surface
- `e75`: fee-adjusted edge threshold `7.5%`
- `n30`: calibration cell needs at least 30 historical samples
- `c1`: confirm for 1 second after signal
- `l1`: wait 1 additional second before entry

The strategy trades only the BTC 5-minute up/down market. It estimates:

```text
P(final_up | tte, z_resid, path_dir)
```

and buys either YES or NO when empirical probability minus current executable ask
and expected taker fee exceeds the configured edge threshold. It buys a fixed
share quantity and holds to PM/UMA settlement.

Default mode is still safe:

```text
DRY_RUN=true
```

`DRY_RUN=false` enables live CLOB execution through the existing
`py-clob-client-v2` wrapper. The live order path is preserved from the old system:
marketable GTC share-limit order, immediate cancel of the remainder, raw response
logging, optional user websocket telemetry, and Gamma/UMA settlement accounting.

## Strategy defaults

| Component | Default |
|---|---:|
| Market | BTC 5m only |
| Quantity | `EMPJP_QTY=5` shares |
| Edge | `EMPJP_EDGE_MIN=0.075` |
| Min cell count | `EMPJP_MIN_CELL_N=30` |
| Confirm / latency | `EMPJP_CONFIRM_S=1`, `EMPJP_LATENCY_S=1` |
| Entry window | `45s <= elapsed <= 255s`, `45s <= tte <= 240s` |
| Price band | `0.18 <= ask <= 0.82` |
| Max spread | `0.05` |
| Min depth | `5` shares |
| Weekend rest | `EMPJP_WEEKEND_REST_ENABLED=false` by default |

Calibration is frozen in:

```text
data/empjp_e75_n30_c1_l1_calibration.json
```

The runtime does **not** need pandas or the research panel.

Dedicated strategy note:

```text
docs/empjp_strategy_brief.md
```

It covers the strategy origin, why we moved away from BTC/ETH correlation arb, alpha definition, calibration, replay performance, and live-readiness caveats.

## Run locally

```bash
pip install -r requirements.txt
cp .env.example .env
python main.py --rounds 1 --start-mode current
```

For live execution, fill the CLOB credentials in `.env` and set:

```text
DRY_RUN=false
```

## Telegram notifications

When `TG_BOT_TOKEN` and `TG_CHAT_ID` are set, the bot sends:

- boot summary,
- ENTRY fill result,
- SETTLE result after Gamma/UMA resolution,
- skip/no-trade diagnostics.

## Directory layout

```text
main.py                         # entrypoint; now calls lab.empjp_live_bot
requirements.txt                # runtime deps; no pandas required
src/common.py                   # Gamma/Binance helpers and market discovery
src/execution.py                # preserved live CLOB execution wrapper
src/notifier.py                 # Telegram notifier
src/lab/empjp_core.py           # pure EMPJP probability/signal logic
src/lab/empjp_live_bot.py       # BTC 5m live/shadow runtime
data/empjp_e75_n30_c1_l1_calibration.json
```

## What changed from the old system

- Removed BTC/ETH pair-box entry as the active runtime path.
- Preserved execution, fill parsing, order-cancel/reconcile, and notification infrastructure.
- Replaced strategy with BTC-only EMPJP probability surface and hold-to-settlement accounting.
- Live BUYs remain aggressive share-sized GTC IOC-style orders, not dollar-sized market orders.
