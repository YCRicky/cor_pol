# Safety contract

- No Gamma `twapEnabled=true` + `twapLookbackSeconds=30`: no trade.
- No fresh paired PM quote, tied/weak leader or non-marketable ask: no trade.
- Scheduler after the 1s cutoff: no trade.
- Binance Futures is observational only: it neither blocks entry nor selects the PM side.
- One decision and at most one GTC submission per slug; the strategy never flips side or retries a rejected round.
- Risk, fee floor, account collateral/allowance and SQLite reservation are checked before submission.
- `AFTERTAKE_DRY_RUN=true` never sends an order. It still runs the public data path so the same fail-closed
  reasons are observable.
- Final PnL is settled from official Polymarket/Gamma outcome data, not local Futures candles.
