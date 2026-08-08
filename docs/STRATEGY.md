# TWAP 尾盤策略（回放 parity contract）

`twap_tail_v2` 是唯一的 runtime entry path。其**勝邊候選 gate** 與
`research_outputs/twap_price_path_tail_v1` 的 global safety rule 對齊；它取代舊的收盤後
`+0.5s` PM-only 規則。

## 結算與資料角色

- 最終輸贏只依 Polymarket/Gamma 的官方結算；回放中 PM/UMA 結果只在候選後作為 label。
- 僅接受 Gamma metadata 的 `twapEnabled=true` 與 `twapLookbackSeconds=30`。
- Binance **Spot** `@kline_5m` 提供當根 open，`@aggTrade` 提供路徑。它只會 veto PM 候選，
  不是結算 oracle。

## 固定資料 cutoff 與候選規則

對每個完整五分鐘市場，特徵 cutoff 固定為 `D = E−10.25s`。scheduler 最多可在 `E−10.00s`
才送出，但 PM quote 與 Binance trades 一律只讀取 `<=D` 的本地可見資料；絕不把後 250ms 的更新
混進判斷。

1. 取 `D` 時最新、年齡 `<=2s` 的 paired PM CLOB quote，選較高 best bid；leader 必須
   **`>=0.90`**，不是 `>0.90`。
2. Binance Spot kline open 與最新 causal aggTrade 的 signed candle 必須同 PM side 且 `>0bp`；
   最新 trade exchange time 距 `D` 必須 `<=2s`。
3. 若 signed candle `>5bp`，通過；這正是已選全域規則的 strong-candle 模式。
4. 若 `0 < signed candle <=5bp`，要求 `E−30→D`、`E−20→D` 都同 PM side，且由 30 秒窗口的
   高/低點回到 `D` 的逆向回撤 `<=2bp`。
5. Spot 流在 candle 開始後才連線、途中斷線、缺 kline open、資料過期、PM tie 或資料缺失，一律 HOLD。

通過候選 gate 後，runner 才依序檢查 Gamma/CLOB metadata、帳戶餘額與 allowance、fee floor、
風控、SQLite reservation，最後以 `.99` GTC 執行。這些是成交安全層，**不是** 443/443 標籤回放的
一部分。

## 回放一致性驗證

在保留的本地 feature cache 上執行：

```bash
PYTHONPATH=src python3 scripts/verify_twap_replay_parity.py \
  --feature-cache research_outputs/twap_price_path_tail_v1/price_path_features.json
```

預期輸出為訓練 `250/250`、保留樣本 `193/193`，合計 `443/443`。驗證程式選邊時不讀
`pm_winner`，只在計分時讀取它。

## 已知限制

443/443 包含用於定參的前 60% 訓練資料；193/193 才是時間保留樣本。它是方向標籤的歷史結果，
不含 CLOB ask、queue、部分成交、撤單、端到端延遲或真實 fee，因此不是未來 100% 勝率或實盤獲利保證。
Binance source timestamp 也不是「0ms」端到端延遲保證。
