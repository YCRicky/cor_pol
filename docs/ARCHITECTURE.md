# Architecture

```text
Gamma market metadata ───────────────► 30s-TWAP eligibility gate
                                               │
PM public CLOB WS ───────────────► paired quote at E-10.25s ─┐
Binance Spot kline + aggTrade ───► causal path veto ─────────┤
State/risk/fees/account preflight ────────────────────────────┘
                                               │
                                      one GTC limit (.99 cap)
                                               │
                                      CLOB reconciliation
                                               │
                               official PM/Gamma settlement label
```

`src/aftertake/twap_tail.py` 是 pure candidate logic。它把所有特徵固定在 `E−10.25s`，並提供
`replay_feature_decision()` 給 parity verifier 使用；兩者共享同一個 price-path gate。

`src/aftertake/binance_proxy.py` 收集 bounded Binance Spot tape 與 `@kline_5m` open。連線晚於
candle open、途中 disconnect/reconnect、缺 kline open 或 buffer overflow 都會使該回合 fail closed。

`src/aftertake/runner.py` 擁有 schedule、Gamma metadata gate、CLOB stream、帳戶檢查、風控、SQLite
reservation、下單和 audit。它可以晚至 `E−10.00s` 提交，但不會使用晚於 feature cutoff 的市場資料。

舊 post-close modules 保留為 compatibility/research code；`run_round()` 僅 dispatch 到
`_run_twap_tail_round()`。
