# TWAP 尾盤價格路徑篩選回測

## 結論邊界

- 決策使用 PM 本地接收時間在 `E−10s−250ms` 前可見的 best-bid leader。
- Binance Spot aggTrades 只作風險篩選；勝負標籤一律是本地觀察到的 PM/UMA 最終 resolution。
- 這是方向正確率 replay，**不含** CLOB ask、部分成交、撤單、費用或真實延遲，因此不能宣稱可實盤獲利。
- 訓練段只選「至少 8 筆且 0 loss」的每幣參數；後 40% 時間序列完全保留做測試。若測試有一筆 loss，該幣不應進入你要求的 100% gate。

## 對照結果（測試段）

| 幣種 | 可用列 | PM leader 基線 | 價格路徑篩選 | 訓練選出的規則 |
| --- | ---: | --- | --- | --- |
| BTC | 157 | 62/63 (98.4%) | 40/41 (97.6%) | bid>=0.80; weak_rev<=2bp; strong_last10_tol=off; strong_rev<=off |
| ETH | 147 | 58/59 (98.3%) | 40/41 (97.6%) | bid>=0.80; weak_rev<=2bp; strong_last10_tol=off; strong_rev<=off |
| SOL | 150 | 60/60 (100.0%) | 36/36 (100.0%) | bid>=0.80; weak_rev<=2bp; strong_last10_tol=off; strong_rev<=off |
| XRP | 146 | 59/59 (100.0%) | 28/28 (100.0%) | bid>=0.80; weak_rev<=2bp; strong_last10_tol=off; strong_rev<=off |
| BNB | 132 | 53/53 (100.0%) | 31/31 (100.0%) | bid>=0.80; weak_rev<=2bp; strong_last10_tol=off; strong_rev<=off |
| DOGE | 144 | 57/58 (98.3%) | 18/18 (100.0%) | bid>=0.80; weak_rev<=2bp; strong_last10_tol=off; strong_rev<=off |

合計：PM leader 基線 349/352 (99.1%)；價格路徑篩選 193/195 (99.0%)。

## 分層安全版（全幣共用 PM 信心底線）

- 先只用 PM 訓練段選 confidence floor：`0.90`；再選尾段 gate：`bid>=0.90; weak_rev<=2bp; strong_last10_tol=off; strong_rev<=off`。
- 訓練：250/250 (100.0%)；後 40% 測試：193/193 (100.0%)，Wilson 95% 下界 98.05%。
- 這比「先讓 Binance gate 把訓練錯單遮掉，再選 0.80」更保守；但本次精煉仍源自同一段歷史資料，先做 forward shadow，不把 193/193 叫作已證明的 100%。

## 被檢驗的規則

令 PM 候選邊為 `s ∈ {+1(YES), -1(NO)}`、決策價為 `P_D`：

- `candle_bp = s × 10,000 × (P_D / 5m_open − 1)`；若 `<=0`，PM 邊與 Binance 當根方向相反，跳過。
- 弱 K（`0 < candle_bp <= 5`）：要求 30→10 秒淨變動與最後 10 秒淨變動皆同方向，且由窗口高/低點回到 `P_D` 的反向回撤不超過該幣訓練出的 noise band。
- 強 K（`candle_bp > 5`）：套用每幣訓練選出的強勢模式；最寬鬆模式不因短尾端逆向而跳過，較嚴模式限制最後 10 秒淨逆向與 endpoint 回撤。

「任何反向」不能直接以 0 個價格跳動定義，否則高頻成交的單一 tick 就會讓幾乎所有弱 K 無法交易；報表把 0 / 0.5 / 1 / 2 bp 明確列入訓練選項，並只報告未見過的後段結果。

## 測試段失敗單

| 幣種 | 場次 | PM 候選 / UMA | bid | candle bp | 30→10 bp | 最後10秒 bp | 回撤 bp |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| BTC | btc-updown-5m-1786105500 | NO / YES | 0.82 | 7.88 | 11.23 | 11.23 | 1.35 |
| ETH | eth-updown-5m-1786119900 | NO / YES | 0.84 | 2.61 | 2.14 | 2.25 | 0.00 |

## 資料覆蓋

- PM 原始候選：876；成功補上 Binance 5m open + tail aggTrade 路徑：876。
- PM 候選擷取跳過：`{"asset_windows": 959, "leader_below_base_bid": 81, "pre_cutover_round": 2162, "tied_pm_leader": 2}`。
- Binance 路徑缺失/錯誤：`{}`。

可重跑命令見本報表同目錄的 `report.json` metadata；僅用讀取本地資料與公開官方市場資料，未連錢包、未下單。
