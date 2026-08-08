# BTC 5 分鐘 TWAP 規則核對（2026-08-07）

> 範圍：官方 Polymarket Gamma 市場 metadata、Polymarket/Chainlink 官方文件；只讀研究，未改動 collector、連線設定或交易路徑。核對時間：2026-08-08 06:50 UTC。

## 結論：已驗證

BTC 5 分鐘市場已從「結束瞬間的 Chainlink BTC/USD」切換為 **Chainlink BTC/USD 的 30 秒 TWAP**。切換邊界是 **2026-08-07 00:00:00 UTC**（美國太平洋夏令時間為 2026-08-06 17:00:00 PDT），不是模糊的「8/7 全天」。

| 5m market slug | 區間起點（UTC） | Gamma config | 規則 |
| --- | --- | --- | --- |
| [`1786060500`](https://gamma-api.polymarket.com/events/slug/btc-updown-5m-1786060500) | 2026-08-06 23:55 | `btc-5m`, `twapEnabled=false` | 結束時 BTC 價格 ≥ 開始價格即 Up |
| [`1786060800`](https://gamma-api.polymarket.com/events/slug/btc-updown-5m-1786060800) | 2026-08-07 00:00 | `btc-5m-twap-30`, `twapEnabled=true`, `twapLookbackSeconds=30` | TWAP 規則 |
| [近期樣本 `1786171800`](https://gamma-api.polymarket.com/events/slug/btc-updown-5m-1786171800) | 2026-08-08 06:50 | 同上 | 同上 |

近期官方市場 metadata 的 resolution source 是 [`BTC/USD TWAP: 30s` Chainlink Data Stream](https://data.chain.link/streams/btc-usd-twap-30s-streams)，並明示不以其他來源或現貨市場結算。

## 30 秒 TWAP 實際比較什麼

Gamma 的逐場規則明示「Chainlink 產生的 TWAP」與該五分鐘區間開始價格相比；同一逐場 config 把這個 TWAP 固定為 30 秒 lookback。依此可作以下 **作業定義**：

```text
Up  iff  Chainlink 產生的 BTC 30s TWAP（市場結束側所採用的報告）
          >= 該五分鐘區間開始時的價格
Down otherwise
```

因此平手是 **Up**。令五分鐘結束為 `T`、開始基準為 `P0`，可將公開規則寫成：

```text
Up  iff  TWAP_30s(BTC/USD, T) >= P0
```

這裡的「30 秒」是 **lookback window**，不是每 30 秒才發布一次，也不是整個 5 分鐘的平均。Polymarket 的官方 TWAP 文件明確說 30/60 秒是 lookback window、由 Chainlink 計算與簽名，並要求以 Chainlink 的 `observationsTimestamp` 判斷資料新鮮度：

- [Polymarket：Chainlink TWAP Prices](https://docs.polymarket.com/market-data/chainlink-twap)
- [Chainlink：Data Streams SDK / report timestamps](https://docs.chain.link/data-streams/reference/data-streams-api/ts-sdk)

### 已公開與未公開的邊界

**已公開：** 結算來源、30 秒 lookback、`>=` 的 Up 門檻、RTDS 的資料時間戳與精確小數值。

**未公開，不能自行假設：** `P0` 取得時是否也是同一 TWAP stream 的某一份報告、精確 end-boundary 選取規則、TWAP 的採樣邊界、內部權重、四捨五入與缺失輸入處理。官方文件明確表示這些 custom feed 細節未公開；所以不能用 Binance ticks 或自行重建的 30 秒均價宣稱能重現結算值。

## 可取得的 canonical 即時資料

Polymarket 官方提供無憑證 RTDS relay。對 BTC 30 秒 TWAP 的低階訂閱為：

```json
{
  "action": "subscribe",
  "subscriptions": [{
    "topic": "crypto_prices_twap_thirty",
    "type": "update",
    "filters": "{\"symbol\":\"btc/usd\"}"
  }]
}
```

或 SDK topic `prices.crypto.chainlink.twap`、`windowSeconds: 30`。在 SDK payload 中，`value` 是精確 decimal；在上述低階 RTDS topic 中，應使用 `full_accuracy_value`（E18 字串）而非僅供顯示的 numeric `value`。`payload.timestamp` 是 **Chainlink observation time**；外層 timestamp 只是 RTDS 發布時間。官方文件同時警告：訂閱只從下一筆更新開始，斷線後沒有 snapshot、history 或 replay。

## 對「最後 30 秒掃尾盤」的含義（推論，不是已驗證績效）

1. **可在盤中估計，但不能保證。** 自 `T-30s` 起，最後 TWAP 的未來組成會逐步縮小；這比舊制度的單一終點 tick 更適合建立連續的 fair-value 模型。但在 `T` 前仍有未觀測的價格路徑，沒有有限的 price move bound 就不存在數學上的 100% 勝率。
2. **Binance order book 只能當 proxy feature。** 它可能有預測性，卻不是結算流。可取得的公開即時對齊訊號是 `crypto_prices_twap_thirty` 的 `btc/usd`；最終標籤仍應以 PM resolution 為準。Binance 尾盤反轉、basis、RTDS relay 缺口或 Chainlink 延遲都可令兩者不同。
3. **真正可驗證的機會是 relay/CLOB 反應，而非「0ms」。** 要量測的是 canonical TWAP update 的 observation time → Polymarket CLOB repricing → 舊報價存活/可成交的延遲，並扣除 taker fee、滑點與未成交風險。TWAP 也會鈍化最後幾秒的脈衝，未必單向利好 taker。
4. **資料品質是硬 gate。** 任一場在最後 30 秒遇到 TWAP stream 斷線、缺 observation timestamp、未知 `P0` 或 market rule 不是 `btc-5m-twap-30`，應標成不可交易／不可回測，不要以 Binance 補值。

## 最小研究驗證條件（尚未執行）

要檢驗「盤中掃尾」是否有統計 edge，逐場至少保存：`P0` 的來源與時間、全段 canonical 30s-TWAP updates（精確 decimal 與 observation timestamp）、PM 雙邊 book/trade、實際最終 PM resolution、及斷線狀態。用未見過的場次，以 PM 結算結果標籤；先報告 win rate、費後 PnL、quote survival 和資料缺口率，再討論任何部署門檻。

## 官方來源

1. [切換前 Gamma market（舊 Chainlink BTC/USD 規則）](https://gamma-api.polymarket.com/events/slug/btc-updown-5m-1786060500)
2. [切換首場 Gamma market（`twapLookbackSeconds: 30`）](https://gamma-api.polymarket.com/events/slug/btc-updown-5m-1786060800)
3. [近期 Gamma market（同一 TWAP 規則）](https://gamma-api.polymarket.com/events/slug/btc-updown-5m-1786171800)
4. [Polymarket Chainlink TWAP / RTDS 官方文件](https://docs.polymarket.com/market-data/chainlink-twap)
5. [Chainlink Data Streams 官方 SDK / report metadata](https://docs.chain.link/data-streams/reference/data-streams-api/ts-sdk)
