# Strategy spec: G7 + asym 0.60 + fav_bp >= -4.0

This is the finalized correlation-arb strategy locked in after the OOS
validation on Run #2 (23 entries) and Run #84 (58 entries). The numbers in
this document are reproducible from the `taker_pol` research repo, but the
bot in **this** repo is a clean re-implementation with only the finalized
pieces wired in.

## 1. What we trade

Polymarket exposes back-to-back 5-minute markets on whether BTC or ETH will
end the window above its opening price:

```
btc-updown-5m-<unix_ts>   -> YES = "BTC up", NO = "BTC down"
eth-updown-5m-<unix_ts>   -> YES = "ETH up", NO = "ETH down"
```

Combined, the four binary outcomes form four directional "boxes":

| Direction | Leg A | Leg B |
|---|---|---|
| `BTC_YES_ETH_YES` | BTC_YES | ETH_YES |
| `BTC_YES_ETH_NO`  | BTC_YES | ETH_NO  |
| `BTC_NO_ETH_YES`  | BTC_NO  | ETH_YES |
| `BTC_NO_ETH_NO`   | BTC_NO  | ETH_NO  |

Each combo costs `price_a + price_b` and pays `payoff_a + payoff_b` where
each `payoff_x in {0, 1}` from the PM/UMA outcome.

## 2. Entry signal: cheap diagonal

`evaluate_arb_box` selects the **cheapest** of the four directional combos
and returns its `gap = 1.0 - (price_a + price_b)`. We enter when:

- `rolling_corr(btc_returns, eth_returns) >= 0.65` -- the BTC/ETH co-movement
  hypothesis is alive.
- `min_gap (0.06) <= gap <= max_gap (0.18)` -- enough edge to cover fees but
  not so much that the book is obviously broken.
- `tte` between 60s and 270s -- excludes the noisy first minute and the
  oracle-manipulation prone final ~30s.
- `min_book_size (5)` on both legs and `min_vol_bp_60s (10)` realized vol
  floor -- avoids dead books / zero-info rounds.

## 3. Finalized entry filter

Forensic on Run #2/#84 showed that ~30% of entries were "Q4 coin-flips"
(both legs near 0.50, no directional bias) and accounted for nearly all
catastrophic combo losses. Two additional gates were added:

### 3.1 Asymmetric mid

Let `mid_x = 1.0 - opp_x_ask`, i.e. the PM-implied probability of the leg
we're long. Require:

```
max(mid_a, mid_b) >= 0.60   AND   min(mid_a, mid_b) <= 0.40
```

Rejected with reason `entry_coinflip:mid=A/B need hi>=0.60 lo<=0.40`.

### 3.2 Minimum favorable bp

For each leg, compute the basis-point move of the underlying *in the
direction the leg wants*:

```
bp_x   = (last_spot_x / open_px_x - 1) * 1e4
fav_bp_x = +bp_x if side_x == "YES" else -bp_x
```

Require `min(fav_bp_a, fav_bp_b) >= -4.0`. Rejected with reason
`leg_underwater:fav_bp=A/B need>=-4.0`.

## 4. Defense: Policy G dual-flip

Once a combo is open, on every tick we recompute:

- `mark_x = 1 - opp_x_ask` -- what we could close leg x at, right now.
- `fpnl_x = mark_x - entry_x` -- per-share floating PnL.
- `fa, fb` -- spot-derived fail probability per leg from
  `fair_up_from_spot(open, last, sigma, tte)`.

We flip **both** legs (buy the opposite side on each leg, locking in the
$2-payoff invariant) when:

```
(fpnl_a < -0.05  AND  fpnl_b < -0.05  AND  fa >= 0.55  AND  fb >= 0.55)
                          OR
                  max(fa, fb) >= 0.80
```

The first branch ("both under" / recovery improbable) handles slow Q4
drifts. The second branch ("locked") handles a single-leg blow-up where
recovery would require the spot to whipsaw back through several bp in the
remaining tte.

The exits for both legs are simulated at the opposite-side asks via
`shadow_fill`, the same path the live executor would use.

## 5. PnL accounting

Resolution uses the PM/UMA outcome **only** -- the Gamma API is polled per
slug for up to `CORR_PM_RESOLUTION_WAIT_S` (default 1200s). For each combo:

```
gross   = payoff_a * qty + payoff_b * qty
cost    = (price_a + price_b) * qty
flip_a  = (payoff(opp_a) - flip_price_a) * qty    # only if flipped_a
flip_b  = (payoff(opp_b) - flip_price_b) * qty    # only if flipped_b
pnl     = gross - cost + flip_a + flip_b
```

Binance close-prices are still recorded as `binance_btc_up` /
`binance_eth_up` for divergence tracking, but they do **not** drive PnL.
Rounds where PM and Binance disagree are flagged `divergence: true` in the
`round_end` event and in the SETTLE Telegram notification.

## 6. Backtest summary (2-run OOS)

Source: `taker_pol/scripts/oos_validate.py` against Run #2 + Run #84.

| Metric | Run #2 | Run #84 | Combined |
|---|---:|---:|---:|
| Rounds with entries | 23 | 58 | 81 |
| Simulated combos | 16 | 59 | 75 |
| Total PnL | +$6.40 | +$8.55 | **+$14.95** |
| Per-round PnL | +$0.28 | +$0.15 | +$0.18 |
| Per-combo PnL | +$0.40 | +$0.14 | +$0.20 |

vs. the un-filtered baseline (which lost ~$95 across the same 110 rounds),
the filter+Policy G stack improves expected per-round PnL by ~$1.04.

Caveats:
- 81 rounds is a small sample; absolute monthly EV needs live validation.
- ~30% of legacy entries are filtered out, so trade frequency is roughly
  one-third of the baseline.
- Rounds with PM/Binance divergence (the R53-type DIV cases) remain
  unhedged and cost ~$20 per occurrence when they hit.

This Railway deployment is the long dry-run that will give us the real
per-day, per-week, per-month numbers.
