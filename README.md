# cor_pol

BTC/ETH 5-minute Polymarket correlation-arbitrage bot (shadow / dry-run build).

The bot discovers each new pair of `btc-updown-5m-<ts>` / `eth-updown-5m-<ts>`
markets, subscribes to the Polymarket CLOB books over websocket, and on each
tick re-evaluates the four-leg "box":

```
BTC_YES + ETH_NO     vs     BTC_NO + ETH_YES
```

When the two-leg cost gap exceeds a calibrated threshold and the rolling
BTC/ETH correlation is strong, the bot opens a paper combo (the cheaper side)
and tracks each leg with the live order book. PnL is settled against the
PM/UMA outcome reported by the Gamma API.

This build is **shadow-only** -- it logs intent, computes simulated fills
against the live book, and sends Telegram notifications, but never places real
orders.

## Finalized strategy (G7 + asym 0.60 + fav_bp >= -4.0)

| Component | Setting | Purpose |
|---|---|---|
| Entry: asymmetric mid | `max(mid_a, mid_b) >= 0.60` AND `min(mid_a, mid_b) <= 0.40` | Skip coin-flip entries that historically blow up in Q4 |
| Entry: min favorable bp | `min(fav_bp_a, fav_bp_b) >= -4.0 bp` | Skip entries where either leg is already underwater vs its strike |
| Defense: Policy G dual-flip | `(fpnl_a < -0.05 AND fpnl_b < -0.05 AND fa >= 0.55 AND fb >= 0.55)` **OR** `max(fa, fb) >= 0.80` | Flip both legs only when recovery is unlikely; avoids spurious flips on noise |
| PnL settlement | PM/UMA outcome via Gamma API | No Binance fallback -- avoids divergence rounds |

See [docs/strategy.md](docs/strategy.md) for the full math, the backtest
record, and the OOS validation that locked these parameters in.

## Run locally

```bash
pip install -r requirements.txt
cp .env.example .env  # then edit TG_BOT_TOKEN / TG_CHAT_ID if you want TG
python main.py
```

The bot will wait for the next 5-minute boundary and start round 1.
Per-round JSONL is written to `out/lab_corr_arb_round<N>_<ts>.jsonl`.

## Deploy on Railway

1. Connect this repo as a Railway service. `nixpacks.toml` + `railway.json`
   describe the build/start commands -- no manual config needed.
2. In **Variables**, set at minimum:
   - `DRY_RUN=true`
   - `TG_BOT_TOKEN`, `TG_CHAT_ID` (optional, recommended)
   - Any `CORR_*` overrides you want (see `.env.example`).
3. Deploy. The service runs `python main.py` indefinitely; round 1 starts on
   the next 5m boundary.

## Telegram notifications

When `TG_BOT_TOKEN` and `TG_CHAT_ID` are set, the bot pings on:

- **Boot** -- run-level config summary.
- **ENTRY** -- every simulated combo fill (round, leg prices, qty, gap, rho).
- **FLIP** -- every Policy G dual-flip (reason, entry/flip prices).
- **SETTLE** -- every round resolution after the PM/UMA outcome is read
  (combos, cost, gross, flip PnL, total PnL, divergence flag).
- **Run done** -- final summary when the rounds loop exits.

## Directory layout

```
main.py                          # entrypoint, adds src/ to sys.path and calls the bot
requirements.txt                 # runtime deps (websockets)
railway.json + nixpacks.toml     # Railway build/start config
.env.example                     # all CORR_* / TG_* settings, documented
src/
  common.py                      # Gamma API + 5m market discovery helpers
  notifier.py                    # TelegramNotifier (HTTP)
  lab/
    correlation_arb_bot.py       # main bot: WS consumer, strategy loop, resolver
    correlation_arb_core.py      # math: fair_up, signals, policy_g_kill, gap stats
docs/
  strategy.md                    # strategy spec, backtest table, parameter rationale
```

## What is *not* in this build

- No real on-chain orders. `taker_pol` (the legacy market-making bot) and the
  CLOB executor have been removed.
- No tail-hedge, no Layer-4 spot kill -- those were superseded by Policy G.
- No research scripts. Backtest tooling lives in the prior `taker_pol` repo.
