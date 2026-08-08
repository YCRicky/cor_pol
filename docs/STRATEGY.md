# TWAP 尾盤策略（`twap_tail_v2`）

這是目前唯一的 production entry path。它取代舊的「收盤後 +0.5 秒、只看 PM
leader bid」規則；舊分類器沒有被 runtime 呼叫。

## 結算與資料角色

- 最終輸贏只依 Polymarket/Gamma 市場的官方結算。
- 只交易 Gamma metadata 明確標示 `cryptoMarketConfig.twapEnabled=true`、
  `twapLookbackSeconds=30` 的市場；metadata 缺失、非 30 秒或切換日前市場一律跳過。
- Binance **Spot** `aggTrade` 不是結算 oracle，也不預測官方價格。它只是一條連續收集的
  因果價格路徑，用來拒絕末段反轉。

## 單一決策點

對每個完整五分鐘 candle，決策時間為 `E - 10.25s`，可接受的 scheduler 遲到最多
`250ms`。超過即 HOLD；不會事後補判、重試或改方向。

1. 取得本地接收、決策時不超過 2 秒的 YES/NO 配對 CLOB quote。
2. 選 best bid 較高的一側，且該 bid 必須嚴格大於 `0.90`。
3. 該側可成交 ask 必須在 `.99` 價格上限內，且顯示深度至少能承接設定數量。
4. Binance Spot tape 必須從 candle 開始前已連線、沒有中斷、沒有 buffer overflow，且最新
   trade 本地接收時間距決策不超過 2 秒。
5. 5 分鐘 Spot 方向要和 PM leader 相同。
   - 若整根 move 絕對值大於 5bp：通過方向與新鮮度即可。
   - 若 move 在 0--5bp：`E-30s → D` 與 `E-20s → D` 必須同向，且最後 30 秒的逆向
     drawdown 不得超過 2bp。
6. 再過既有風控、帳戶餘額/allowance、fee floor、最小單位和 SQLite reservation 後，才送一筆
   marketable GTC limit；價格上限固定 `.99`。

任一資料缺失、WS 重連、quote/tape 過期、方向不合、弱 candle 反轉、費用後下限不足、風控拒絕
都等同 **不交易**。

## 為何不用「0ms」當 alpha

exchange 訊息裡的 `0ms`/source timestamp 不是端到端延遲保證。實作記錄本地 receive time，並在
決策時同時要求 Binance source time 和 receive time 都不晚於決策點，避免回放或網路抖動把未來 tick
帶進來。

## 已知限制

這是一個嚴格篩選器，不是「100% 勝率」保證；所有回放結果都可能受樣本、stream coverage、成交與
實際 fee 影響。策略不把 Binance 報價當成 PM 的結算價格，也不在 coverage 缺口時猜測或自動放寬門檻。
