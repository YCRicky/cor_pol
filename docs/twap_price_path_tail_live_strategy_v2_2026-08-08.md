# 實戰策略：`twap_price_path_tail_v2`

`main.py` 的主策略已切換為本文件的 5 分鐘加密貨幣尾盤 taker 策略。它沿用 repo
既有的 CLOB 送單、立即撤餘單、成交查詢與 PM/UMA 結算帳務；`DRY_RUN=false`
時才會建立實際 CLOB executor。本次程式修改本身不會啟動程式或送單。

## 交易範圍與制度 gate

策略逐場讀取 Gamma metadata，只接受 BTC、ETH、SOL、XRP、BNB、DOGE 的 5 分鐘
市場，且必須同時滿足：

1. `cryptoMarketConfig.twapEnabled=true`；
2. `twapLookbackSeconds=30`；
3. resolution source 是該幣的 `*-usd-twap-30s-streams`；
4. 場次在 2026-08-07 00:00 UTC 的制度切換後、仍 accepting orders。

不符合任一條件時直接 skip；不會把舊制 end-tick 場混入新制。官方逐場規則是
Chainlink 30 秒 TWAP 與場次開始基準比較，`>=` 為 Up。Binance Spot 只是一個
價格路徑 veto，**不是**結算 oracle。

## 訊號：固定在 `E - 10.25s`

令表定結束為 `E`，決策截止為：

```text
D = E - 10.00s - 0.25s = E - 10.25s
```

0.25 秒是研究回放的資訊安全墊。runner 保留每邊本地收到的 BBO 歷史，只取
`received_ts <= D` 的最後一筆，避免 D 之後的 CLOB 更新倒灌進訊號。

### PM 候選邊

在 `D` 的 YES / NO best bid 中選較高者，且需：

```text
leader bid >= 0.90
leader quote age <= 2s
```

相同 bid、缺 quote、過期 quote 都不交易。

### Binance Spot 價格路徑 veto

runner 訂閱六個 `@aggTrade` stream，使用交易所的 `T`（trade time）與本地接收
時間雙重 as-of gate。最後一筆交易的 `D - T` 必須不超過 2 秒。

令 `s=+1` 代表 YES、`s=-1` 代表 NO；令 `P30` 是 `E-30s` 前最後可見成交、
`P20` 是 `E-20s` 前最後可見成交、`PD` 是 `D` 前最後可見成交、`O` 是 Binance
該根 5 分 K 的 open。

```text
candle_bp = s * 10,000 * (PD / O   - 1)
net20_bp  = s * 10,000 * (PD / P30 - 1)
last10_bp = s * 10,000 * (PD / P20 - 1)
```

規則：

- `candle_bp <= 0`：PM 候選邊與本根 Binance 方向相反，skip。
- `0 < candle_bp <= 5 bp`（弱 K）：`net20_bp > 0`、`last10_bp > 0`，且從尾段
  順向極值回到 `PD` 的反向回撤不超過 2 bp。
- `candle_bp > 5 bp`（強 K）：保留方向與 freshness gate；不額外套用尾端
  reversal veto，這是本次時間切割回放選出的模式。

如果程式在 `D + 0.75s` 後才取得 CPU 時間，該 asset 以 `decision_missed` skip，
而不是偷用較晚的資料。

## 實盤進場與出場

所有通過訊號的 asset 在同一 event loop tick 併發送出，預設每 asset 每 round
最多一次：

1. 重查當前選邊 ask；quote 必須仍新鮮。
2. `best ask <= TAIL_PRICE_CAP`，預設 0.99；cap 以下可見 ask depth 必須至少
   `max(TAIL_QTY, TAIL_MIN_VISIBLE_ASK_QTY)`。
3. 以 market metadata 的 `takerBaseFee` 和明確設定的 rebate，驗證最壞 cap
   價仍保有 `TAIL_MIN_NET_WIN_PER_SHARE`。
4. 送一筆 share-sized、marketable GTC limit，limit 為 `best_ask + ticks`，但
   絕不超過 0.99；立刻 cancel 未成交餘額並以 CLOB `get_order` reconcile。
5. 不 chase、不在部分成交後重新決定勝邊。正的已知 fill 會被記錄、持有到
   PM/UMA resolution；未知 CLOB 狀態會 alert，且只記錄已知的 fill。

部位出場不是用 Binance 平倉；它是 binary token 持有至 Polymarket 最終 UMA
resolution。已結算帳務：

```text
pnl = filled_qty * win - filled_qty * average_entry_price - estimated_taker_fee
```

`TAIL_TAKER_REBATE_RATE` 預設 `0.0`，故費用估計不會假設帳戶有 rebate；只有
核實實際 effective rebate 後才應提高它。

## 風險與狀態

- 預設 `TAIL_QTY=5`，沒有沿用舊策略或截圖中的任何隱含放大倉位。若要 50 shares
  必須明確設 `TAIL_QTY=50`，並相應設 `TAIL_MAX_COST_PER_ROUND_USD`。
- 預設每 round 最多 6 個 asset；`TAIL_MAX_COST_PER_ROUND_USD=0` 代表未啟用總成本
  cap。正式部署前應依帳戶風險額度設定它。
- state 存在 `out/twap_price_path_tail_v2/state.json`，已成交未結算 token 會在重啟後
  繼續輪詢 PM 結算。極端情況若程序在 CLOB submit 後、state 落盤前崩潰，仍需以
  CLOB / 錢包手動 reconcile。
- 若 30 秒 Binance aggregate-trade buffer 達到容量，該 asset 直接以
  `binance_tail_buffer_overflow` skip，不會用截斷的價格路徑下單。
- `0ms` Binance timestamp 不代表端到端延遲優勢。策略記錄的是 exchange trade time、
  local receipt、PM quote age 與實際 CLOB execution response。

## 回放證據與不能宣稱的事

本機 TWAP 切換後六幣資料、以 **PM/UMA 最終 label** 判定：全樣本符合全域安全版
規則為 443/443；依時間前 60% 選 `bid>=0.90` 與尾段 gate，後 40% 保留測試為
193/193，Wilson 95% 下界 98.05%。

這些是方向準確率，不含 ask、部分成交、撤單延遲、費用、skew 或真實 fill，因此
不是「已證明 100% 勝率」或保證盈利。程式的 price cap、可見深度、fee floor、
as-of 時鐘與 PM 結算記錄正是為了把這些未被回放覆蓋的失敗模式顯式拒絕或留痕。

研究細節與原始反例請見：

- [價格路徑研究 V2](twap_price_path_tail_strategy_v2_2026-08-08.md)
- [TWAP 官方規則核對](btc_5m_twap_rules_research_2026-08-07.md)
- [Binance Spot 資料時間語義](binance_spot_historical_price_path_data_note_2026-08-08.md)
- [完整回放報告](../research_outputs/twap_price_path_tail_v1/report.md)

## 主要設定

```text
TAIL_QTY=5.0
TAIL_DECISION_LEAD_S=10.25
TAIL_MIN_LEADER_BID=0.90
TAIL_MAX_PM_QUOTE_AGE_S=2.0
TAIL_MAX_BINANCE_TRADE_AGE_S=2.0
TAIL_WEAK_CANDLE_MAX_BP=5.0
TAIL_WEAK_ADVERSE_CAP_BP=2.0
TAIL_PRICE_CAP=0.99
TAIL_EXEC_SLIPPAGE_TICKS=1
TAIL_MIN_VISIBLE_ASK_QTY=5.0
TAIL_MIN_NET_WIN_PER_SHARE=0.001
TAIL_TAKER_REBATE_RATE=0.0
TAIL_MAX_ENTRIES_PER_ROUND=6
TAIL_MAX_COST_PER_ROUND_USD=0.0
```

可直接加入 `.env` 的無祕密範本在
[twap_price_path_tail_v2.env.example](twap_price_path_tail_v2.env.example)。舊 EMPJP
modules 仍保留為 legacy code，但不再由 `main.py` 啟動；若要回退，必須顯式改回其
module import，而不是靠環境變數默默切換。
