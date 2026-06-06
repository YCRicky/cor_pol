# cor_pol

BTC/ETH 5-minute Polymarket correlation-arbitrage bot.

The bot discovers each new pair of `btc-updown-5m-<ts>` / `eth-updown-5m-<ts>`
markets, subscribes to the Polymarket CLOB books over websocket, and on each
tick re-evaluates the four-leg "box":

```
BTC_YES + ETH_NO     vs     BTC_NO + ETH_YES
```

When the two-leg cost gap exceeds a calibrated threshold and the rolling
BTC/ETH correlation is strong, the bot opens the cheaper opposite diagonal and
tracks each leg with the live order book. PnL is settled against the PM/UMA
outcome reported by the Gamma API.

Default mode is `DRY_RUN=true`, which logs intent and computes simulated fills
against the live book. `DRY_RUN=false` enables live CLOB execution through
`py-clob-client-v2` using pUSD CLOB credentials. The funding wallet mode is
configured by `CLOB_SIGNATURE_TYPE` and `CLOB_FUNDER_ADDRESS`.

## Strategy (four-quadrant PM kill + asym 0.60 + fav_bp >= -4.0)

| Component | Setting | Purpose |
|---|---|---|
| Entry: asymmetric mid | `max(mid_a, mid_b) >= 0.60` AND `min(mid_a, mid_b) <= 0.40` | Skip coin-flip entries that historically blow up in Q4 |
| Entry: min favorable bp | `min(fav_bp_a, fav_bp_b) >= -4.0 bp` | Skip entries where either leg is already underwater vs its strike |
| Entry: quadrant EV | `fair_a + fair_b - cost >= 0.01`, `P(lose,lose) <= 0.22` | Gap alone only proves same-direction breakeven; the bad diagonal must be bounded |
| Sizing: round cap | `max_combos_per_round = 3`, `max_cost_per_round_usd = 15` | Entry-only cap. Defensive Q4/imbalance reverse buys bypass it so stops cannot be blocked |
| Execution | marketable GTC share-limit then immediate cancel, 5 shares per leg, 1-share mismatch tolerance | Targets shares exactly and avoids infinite retries on tiny residual fills |
| Defense: asymmetric Q4 kill | If one executable leg loss exceeds `2 * entry_gap`, the other leg is also below entry, and both legs' `fav_bp` worsened vs entry, buy both opposite asks | Cut the true `(lose, lose)` path while leaving `(win, win)`, `(win, lose)`, and `(lose, win)` alive |
| PnL settlement | PM/UMA outcome via Gamma API | No Binance fallback -- avoids divergence rounds |

See [docs/strategy.md](docs/strategy.md) for the full math, the backtest
record, and the OOS validation that locked these parameters in.
See [docs/live_trading.md](docs/live_trading.md) for the live execution runbook.

## Run locally

```bash
pip install -r requirements.txt
cp .env.example .env  # then edit TG_BOT_TOKEN / TG_CHAT_ID if you want TG
python main.py
```

The bot will wait for the next 5-minute boundary and start round 1.
Per-round JSONL is written to `out/lab_corr_arb_round<N>_<ts>.jsonl`.

## Deploy on EC2

Use systemd on an Ubuntu EC2 instance. The service reads `/etc/cor-pol.env`,
runs from `/opt/cor_pol`, restarts on failure, and writes stdout/stderr to
journald. See [docs/ec2_deploy.md](docs/ec2_deploy.md).

## Telegram notifications

When `TG_BOT_TOKEN` and `TG_CHAT_ID` are set, the bot pings on:

- **Boot** -- run-level config summary.
- **ENTRY** -- every combo fill (round, leg prices, leg quantities, gap, rho).
- **FLIP** -- every fourth-quadrant PM kill (reason, entry/flip prices).
- **SETTLE** -- every round resolution after the PM/UMA outcome is read
  (combos, cost, gross, flip PnL, cumulative PnL, divergence flag).
- **Run done** -- final summary when the rounds loop exits.

## Directory layout

```
main.py                          # entrypoint, adds src/ to sys.path and calls the bot
requirements.txt                 # runtime deps, including py_clob_client_v2 for live CLOB
deploy/                          # systemd and logrotate examples for EC2
.env.example                     # all CORR_* / TG_* settings, documented
src/
  common.py                      # Gamma API + 5m market discovery helpers
  execution.py                   # live CLOB execution wrapper and fill parsing
  notifier.py                    # TelegramNotifier (HTTP)
  lab/
    correlation_arb_bot.py       # main bot: WS consumer, strategy loop, resolver
    correlation_arb_core.py      # math: fair_up, quadrant estimates, signals, gap stats
docs/
  strategy.md                    # strategy spec, backtest table, parameter rationale
```

## What is *not* in this build

- No maker quoting. Live mode uses aggressive share-sized GTC orders and
  immediately cancels unfilled remainders.
- No tail-hedge, no Layer-4 spot kill -- fourth-quadrant control is handled by PM marks.
- No research scripts. Backtest tooling lives in the prior `taker_pol` repo.
