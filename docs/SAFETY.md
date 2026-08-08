# Safety contract

- 沒有 Gamma `twapEnabled=true` + `twapLookbackSeconds=30`：不交易。
- 特徵固定在 `E−10.25s`；scheduler 超過 `E−10.00s`：不交易。
- 沒有年齡 `<=2s` 的 paired PM quote、PM tie、或 leader `<0.90`：不交易。
- Binance Spot 沒有 causal 5m kline open、完整 aggTrade coverage、或最新交易 `<=2s`：不交易。
- Spot 路徑和 PM side 相反，或弱 K 最後 30/20 秒出現不合格反轉：不交易。
- 一個 slug 最多一筆 `.99` GTC；策略不翻 side、不重試被拒絕的回合。
- 風控、fee floor、帳戶 collateral/allowance 與 SQLite reservation 仍在送單前檢查。
- `AFTERTAKE_DRY_RUN=true` 絕不送單，但會記錄相同的 fail-closed 原因。
- 最終 PnL 只以官方 Polymarket/Gamma outcome 結算，不以 Binance Spot 當結算價。
