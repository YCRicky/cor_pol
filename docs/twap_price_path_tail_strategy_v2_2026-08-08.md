# TWAP 尾盤價格路徑策略 V2（不使用 Binance L2）

狀態：**research / forward-shadow only**。不連錢包、不下單。

## 這次能支持的策略

令場次表定結束為 `E`，使用保守決策時間 `D = E - 10.25s`。只在下列條件同時成立時，把 PM 的 leader side 視為候選：

1. PM 本地接收時間在 `D` 前的 YES / NO best bid 有明確 leader，且 **leader bid >= 0.90**。
2. Binance Spot 的最後成交距 `D` 不超過 **2 秒**；否則價格訊號過期，直接跳過。
3. 令 `s=+1` 為 YES、`s=-1` 為 NO，且 `candle_bp = s × 10,000 × (P_D / 5m_open - 1)` 必須大於 0；也就是 Binance 當根 5m 到 `D` 的方向不能與 PM 候選相反。
4. 若 `0 < candle_bp <= 5`（弱 K）：
   - 30→10 秒淨變動必須同向；
   - 最後 10 秒淨變動必須同向；
   - 從該 20 秒窗口的順向極值回到 `P_D` 的反向回撤不得超過 **2 bp**。
5. 若 `candle_bp > 5`（強 K）：本組資料沒有支持額外的尾段反向 veto；保留第 1–3 項與 fresh-trade gate 即可。

這正是「弱 5m K 遇到尾段逆向就不碰；強 K 可以容忍少量尾段反向」的可回放版本。`任何反向` 不能直接等同一筆 tick 的回撤，否則高頻成交會讓弱 K 幾乎全被拒絕；2 bp 是在訓練段的明確 noise band，而不是口頭判斷。

## 回測結果

資料為 2026-08-07 00:00 UTC TWAP 切換後的六幣本地 PM 場次。每場 label 只接受本地觀察到的 PM 最終 UMA resolution；Binance 只作 proxy gate。

| 做法 | 訓練（前 60% 時間） | 保留測試（後 40% 時間） | 判定 |
| --- | ---: | ---: | --- |
| PM leader `>=0.80` | 523 / 524 | 349 / 352 | 不足；測試有 3 場錯誤 |
| 每幣價格 gate、`>=0.80` | 251 / 251 | 193 / 195 | 不足；BTC、ETH 各留 1 場錯誤 |
| **先以 PM-only 訓練選 leader `>=0.90`，再加上述價格 gate** | **250 / 250** | **193 / 193** | 可作下一輪 forward shadow 候選 |

全域 PM floor 的選擇沒有看後 40%：在訓練段中，`>=0.80` 與 `>=0.85` 各有一場 PM label 錯誤；`>=0.90` 是第一個 0-loss、且容量最大的 floor（505 場）；`>=0.95` 也是 0-loss，但只剩 486 場。

`193/193` 的 Wilson 95% 下界仍只有 **98.05%**，不是「已證明 100%」。此外，這個精煉方向已經看過同一段歷史資料，因此必須再用新收集的 forward-shadow 場次確認，不能直接轉交易。

## 小幣的實際差異

本次訓練沒有發現應把 5bp / 2bp 做成不同幣種門檻；六幣都選到同一組 tail gate。真正顯著的幣種差異是 Binance last-trade 是否新鮮：

| 幣種 | 測試 PM `>=0.80` 候選 | 價格 gate 留下 | 因 Binance 最後成交 >2s 跳過 |
| --- | ---: | ---: | ---: |
| BTC | 63 | 40 | 2 |
| ETH | 59 | 40 | 1 |
| SOL | 60 | 36 | 14 |
| XRP | 59 | 28 | 19 |
| BNB | 53 | 31 | 2 |
| DOGE | 58 | 18 | 26 |

所以在不使用 L2、只看成交價的前提下，DOGE / XRP / SOL 不能把「沒有新成交」當作沒有反轉；應該跳過，而不是放寬門檻硬做。

## 兩個關鍵反例

- `btc-updown-5m-1786105500`：PM NO bid `0.82`，Binance 的整根與尾段都支持 NO，但 PM 最終 UMA 是 YES。純 Binance 價格路徑無法預先辨識它。
- `eth-updown-5m-1786119900`：PM NO bid `0.84`，弱 K 尾段沒有反向，最終仍是 YES。這也說明 Binance Spot 不是結算 oracle。

兩者都被 `leader bid >= 0.90` 拒絕；DOGE 的唯一測試錯單則被弱 K / stale-trade gate 拒絕。

## 下一步（仍是 shadow）

1. 連續收集新的、未參與本次選參數的至少 100 個 `>=0.90` 候選。
2. 逐場記錄 `D` 時的 PM BBO、Binance aggregate-trade path、最後成交 age、及最終 UMA label。
3. 任何一筆錯誤就停止把它稱為 100% gate，回頭按 Chainlink canonical TWAP / RTDS 對照，而不是用 Binance 或 L2 補故事。
4. 之後才另外做 execution replay：實際可吃 ask、數量、費用、部分成交與撤單延遲；本文件尚未覆蓋它們。

可重跑程式：[twap_price_path_tail_backtest.py](../src/lab/twap_price_path_tail_backtest.py)。完整數據與選單在 [report.md](../research_outputs/twap_price_path_tail_v1/report.md)；Binance 資料時間語義見 [官方資料備忘](binance_spot_historical_price_path_data_note_2026-08-08.md)。
