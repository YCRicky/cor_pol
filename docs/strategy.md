# Strategy spec: four-quadrant PM kill + asym 0.60 + fav_bp >= -4.0

This is the cleaned-up correlation-arb strategy. The bot treats each opened
combo as a four-quadrant option package and focuses risk control on the only
path that matters: both legs losing at the same time.

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
- `min_gap (0.04) <= gap <= max_gap (0.22)` -- enough edge to cover fees but
  not so much that the book is obviously broken.
- `tte` between 60s and 270s -- excludes the noisy first minute and the
  oracle-manipulation prone final ~30s.
- `min_book_size (5)` on both legs -- avoids dead books.
- `fair_a + fair_b - cost >= min_model_edge (0.01)` -- gap alone only says
  the same-direction quadrants can break even; entry also needs positive
  spot-model edge.
- `P(lose, lose) <= max_bad_quad_prob (0.22)` and
  `P(lose, lose) / P(one-win-one-lose) <= 0.38` -- bounds the bad diagonal.
- `max_combos_per_round = 3` and `max_cost_per_round_usd = 15` -- cap
  repeated entries in the same 5-minute window. The 95-round live dry-run
  showed combo #2/#3 carried the marginal alpha while combo #4/#5 were
  negative expectancy.

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

## 4. Defense: asymmetric fourth-quadrant kill

Once a combo is open, on every tick we recompute:

- `exec_mark_x = 1 - opp_x_ask` -- executable mark for the long leg. If we hold
  YES, buying NO at the ask locks $1 at settlement, so YES is worth
  `1 - NO_ask` for stop-loss purposes.
- `loss_x = entry_x - exec_mark_x` -- per-share executable loss.
- `entry_gap = 1 - entry_a - entry_b` -- the original edge/safety cushion.
- `fav_bp_x` from spot vs strike in the direction the leg wants, both at entry
  and at the current tick.

We kill **both** legs immediately when a true asymmetric fourth quadrant forms:

```
max(loss_a, loss_b) >= 2.0 * entry_gap
min(loss_a, loss_b) > 0
current_fav_bp_a < 0
current_fav_bp_b < 0
current_fav_bp_a <= entry_fav_bp_a - 1.0
current_fav_bp_b <= entry_fav_bp_b - 1.0
```

The rule is deliberately asymmetric: one leg can be clearly dead while the
other has only just crossed below entry. Requiring both `fav_bp` values to
worsen relative to entry prevents cutting a normal one-win-one-lose path just
because the PM book briefly widens. In dry-run the exits are simulated at the
opposite-side asks; in live mode they are sent through the same aggressive FAK
execution wrapper as entries.

## 5. PnL accounting

Resolution uses the PM/UMA outcome **only** -- the Gamma API is polled per
slug for up to `CORR_PM_RESOLUTION_WAIT_S` (default 1200s). For each combo:

```
gross   = payoff_a * qty_a + payoff_b * qty_b
cost    = price_a * qty_a + price_b * qty_b
flip_a  = (payoff(opp_a) - flip_price_a) * flip_qty_a
flip_b  = (payoff(opp_b) - flip_price_b) * flip_qty_b
pnl     = gross - cost + flip_a + flip_b
```

Binance close-prices are still recorded as `binance_btc_up` /
`binance_eth_up` for divergence tracking, but they do **not** drive PnL.
Rounds where PM and Binance disagree are flagged `divergence: true` in the
`round_end` event and in the SETTLE Telegram notification.

## 6. Live dry-run sizing check

The 95-round PM-settled dry-run showed total PnL of `+$9.60` across 117
combos, but the marginal combo count was uneven:

| Cap | Combos | PnL | ROI on Cost | Q4 Combos |
|---:|---:|---:|---:|---:|
| 1 | 48 | +$4.55 | +2.05% | 5 |
| 2 | 75 | +$12.75 | +3.67% | 7 |
| 3 | 95 | **+$16.35** | **+3.72%** | 9 |
| 4 | 108 | +$10.75 | +2.15% | 11 |
| 5 | 117 | +$9.60 | +1.78% | 12 |

By combo index, #1/#2/#3 were positive while #4/#5 were negative. The default
round cap is therefore fixed at 3 combos.

## 7. Backtest summary (2-run OOS)

Source: `taker_pol/scripts/oos_validate.py` against Run #2 + Run #84.

| Metric | Run #2 | Run #84 | Combined |
|---|---:|---:|---:|
| Rounds with entries | 23 | 58 | 81 |
| Simulated combos | 16 | 59 | 75 |
| Total PnL | +$6.40 | +$8.55 | **+$14.95** |
| Per-round PnL | +$0.28 | +$0.15 | +$0.18 |
| Per-combo PnL | +$0.40 | +$0.14 | +$0.20 |

vs. the un-filtered baseline (which lost ~$95 across the same 110 rounds),
the old filter stack improved expected per-round PnL by ~$1.04, but this
build should be revalidated because the defense has been replaced with the
asymmetric fourth-quadrant kill.

Caveats:
- 81 rounds is a small sample; absolute monthly EV needs live validation.
- ~30% of legacy entries are filtered out, so trade frequency is roughly
  one-third of the baseline.
- Rounds with PM/Binance divergence (the R53-type DIV cases) remain
  unhedged and cost ~$20 per occurrence when they hit.

This Railway deployment is the long dry-run that will give us the real
per-day, per-week, per-month numbers.
