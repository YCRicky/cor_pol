# Strategy spec: `empjp_e75_n30_c1_l1`

This repository now runs the BTC-only empirical joint probability strategy that
was selected from the PM5M replay research.

## 1. What it trades

The bot trades the BTC 5-minute Polymarket up/down market:

```text
btc-updown-5m-<unix_ts>
```

It buys exactly one side per round at most:

```text
YES = BTC settles above round open
NO  = BTC settles below round open
```

Default size is `5` shares and the position is held until PM/UMA settlement.

## 2. Alpha

The strategy estimates the empirical conditional probability:

```text
P(final_up | tte, z_resid, path_dir)
```

where:

- `tte`: seconds to expiry,
- `z_resid`: current BTC basis-point move normalized by rolling BTC sigma and
  remaining time,
- `path_dir`: whether the path so far is `up_dominant`, `down_dominant`, or
  `balanced`.

It compares the empirical probability to the executable PM ask:

```text
YES edge = emp_up - yes_ask - fee(yes_ask)
NO  edge = (1 - emp_up) - no_ask - fee(no_ask)
```

The bot takes the side with the larger edge if all gates pass.

## 3. Production policy

```text
empjp_e75_n30_c1_l1
```

| Part | Meaning |
|---|---|
| `empjp` | empirical joint probability |
| `e75` | minimum edge `0.075` |
| `n30` | cell must contain at least 30 samples |
| `c1` | 1 second confirmation |
| `l1` | 1 second additional latency before entry |

Default filters:

```text
45s <= elapsed <= 255s
45s <= tte <= 240s
0.18 <= entry ask <= 0.82
spread <= 0.05
book depth >= 5 shares
```

## 4. Calibration

The frozen calibration is committed as:

```text
data/empjp_e75_n30_c1_l1_calibration.json
```

It maps:

```text
(tte_bin, z_bin, path_dir) -> {cell_n, emp_up}
```

Runtime loads this JSON directly. It does not need the research dataframe or
pandas.

## 5. Live execution

Live execution reuses the previous system's order layer:

1. Build a BUY order with share size `EMPJP_QTY`.
2. Use a marketable GTC limit: `best_ask + slippage_ticks * tick_size`.
3. Submit through `py-clob-client-v2`.
4. Immediately cancel unfilled remainder.
5. Reconcile with CLOB `get_order`.
6. Optional user websocket is telemetry only and does not block execution.

This avoids Polymarket's dollar-sized BUY market-order semantics while keeping
share sizing exact.

## 6. Settlement accounting

Realized PnL is computed from PM/UMA outcome via Gamma only:

```text
win = (pm_up >= 0.5) for YES, else (pm_up < 0.5)
pnl = qty * win - qty * entry_price - entry_fee
```

Binance spot is used only for live state features and not for realized PnL.

## 7. Weekend behavior

Backtest split showed weekends were still profitable but lower quality:

```text
weekday: stress PnL +1035.6, PF 1.76, avg/trade +0.550
weekend: stress PnL  +410.2, PF 1.36, avg/trade +0.310
```

Default therefore keeps weekends enabled:

```text
EMPJP_WEEKEND_REST_ENABLED=false
```

Set it to `true` only if you want weekday-only risk exposure.
