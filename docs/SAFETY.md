# Safety contract

- No Gamma `twapEnabled=true` + `twapLookbackSeconds=30`: no trade.
- No full Binance Spot tape from candle open: no trade.
- No fresh paired PM quote, tied/weak leader, non-marketable ask, insufficient displayed depth: no trade.
- Weak candle reversal, path/leader mismatch, stale trade or scheduler after the 250ms cutoff: no trade.
- Binance is never treated as the PM settlement source.
- One decision and at most one GTC submission per slug; the strategy never flips side or retries a rejected round.
- Risk, fee floor, account collateral/allowance and SQLite reservation are checked before submission.
- `AFTERTAKE_DRY_RUN=true` never sends an order. It still runs the public data path so the same fail-closed
  reasons are observable.
- Final PnL is settled from official Polymarket/Gamma outcome data, not local Spot candles.
