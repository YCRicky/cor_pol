# Binance Spot 歷史 5m 與尾段價格路徑資料備忘

## 結論

**可做本地回測，但只把 Binance Spot 當代理訊號。**

- `GET /api/v3/klines` 可取得與 UTC 對齊的 5 分鐘 K 線；用它的 `open`
  定義本根 5m 的 Binance 漲跌。
- `GET /api/v3/aggTrades` 可取得指定歷史時間窗內的成交聚合，並以交易
  時間欄位 `T` 重建「尾段成交價格」路徑。它不是 L2，也不是逐筆 raw-fill
  回放；足以測試價格方向／反向回撤，不足以聲稱訂單簿或端到端延遲優勢。
- `1s` kline 是較省請求的近似路徑；若規則是「最後 30→10 秒出現過任何
  反向」，必須用 `aggTrades`，因為一秒 K 線不能得知秒內 high/low 的先後。

Polymarket 最終標籤仍必須取本地記錄的 PM/UMA 結算；Binance 的結果不可以
代替結算標籤。

## 官方契約

來源均為 Binance 官方文件（2026-08-08 查閱）：

- [Spot REST Market API](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/market)
  定義 `GET /api/v3/aggTrades`：IP request weight **4**、`startTime`/`endTime`
  為 **ms 且 inclusive**、`limit` 預設 500、最大 1000，資料源為 Database。
  回應包含 aggregate id `a`、價格 `p`、數量 `q`、及時間戳 `T`。
- 同頁定義 `GET /api/v3/klines`：IP request weight **2**、支援 `1s` 與
  `5m` interval，`limit` 預設 500、最大 1000；K 線以 open time 唯一識別。
  `startTime`/`endTime` 一律按 UTC 解釋（即使指定 `timeZone`）。
- [Spot WebSocket Market Streams](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/ws-streams/~)
  將 aggTrade 的 `T` 定義為 **Trade time**，並把 `E` 另列為 **Event time**。
  歷史 replay 應以 `T` 分窗，不能以 REST 收包時間或本地接收時間當交易時間。
- [General REST API Information](https://developers.binance.com/en/docs/products/spot/rest-api)
  說明資料通常按時間順序回傳、未帶時間參數時只取最近資料、可從
  `X-MBX-USED-WEIGHT-*` header 觀察 IP 權重；遇到 429 必須退避，反覆不退避
  可能變成 418 IP ban。官方也建議純公開市場資料改用
  `https://data-api.binance.vision`。

`aggTrades` 的「aggregate」意思是同一 taker order、同價格、同一時間的
成交會合併；它不是 Binance 撮合簿，也不是一筆一筆的原始成交。

## 已實測的公開 API（不是文件保證）

在 2026-08-08 以官方公開端點實測 BTCUSDT：

| 檢查 | 觀察值 |
| --- | --- |
| `aggTrades` 的 2026-08-07 00:19:30–00:19:50 UTC | HTTP 200，71 筆 aggregate，`T` 從 `1786061970174` 到 `1786061988975` |
| `fromId` | 以先前最後 `a` 再查，第一筆仍為該 `a`，符合官方所述 inclusive |
| `aggTrades` 的 2 小時 `startTime`/`endTime` 範圍、`limit=1` | HTTP 200；現行文件未列此 endpoint 的最大時間跨度，因此不可把這次觀察當長期保證 |
| 精確 5m kline | `startTime=1786061700000` 回傳 open time 完全相同的一根 5m K 線 |
| 1s klines | 可回傳該尾段秒級資料；`endTime` 為 inclusive，若傳到 `E-10s` 會連 `E-10s` 開始的秒 K 一起拿到 |
| 公開資料 host | `data-api.binance.vision/api/v3/aggTrades` 對同一歷史窗回傳相同開頭資料 |

因此「昨天到今天」的本地樣本可以安全補齊；遠期保留長度、缺洞與 API 行為
都應在每次回測前重新檢查，並快取取得的原始資料與 headers。

## 建議的無前視抓取規格

令 PM 場次開始為 `S_ms`，預定結束為 `E_ms = S_ms + 300_000`；先確認兩者
真的對齊 Binance 的 UTC 5 分鐘邊界。決策點設定為 `D_ms = E_ms - 10_000`。

### 1. 本根 5m 的方向

```text
GET /api/v3/klines
    ?symbol=<ASSET>USDT
    &interval=5m
    &startTime=S_ms
    &endTime=E_ms-1
    &limit=1
```

只在回應 `row[0] == S_ms` 時接受資料，並用 `row[1]` 作 candle open。回測在
決策點用最後可見成交價 `P_D` 算：

```text
candle_return_bp = 10_000 * (P_D / kline_open - 1)
```

不要用已收線的 `row[4]` 當 T−10 的判斷，否則把 T−10 到結束的資訊洩漏進去。

### 2. 尾段 30→10 秒的成交路徑

對每個場次採用固定、可記錄的接收安全墊 `L_ms`（例如先以 250 ms 做保守
shadow replay，之後以實測 WebSocket 延遲校準）：

```text
cutoff_ms = D_ms - L_ms
lookback_ms = E_ms - 31_000

GET /api/v3/aggTrades
    ?symbol=<ASSET>USDT
    &startTime=lookback_ms
    &endTime=cutoff_ms
    &limit=1000
```

保留 `E_ms-30_000 <= T <= cutoff_ms` 的 rows；`lookback_ms` 額外往前一秒是為了
取得 T−30 前最後一筆成交，將其價格 forward-fill 到窗口起點。若找不到該
起點價格，將該場標為 `missing_path`，而不是憑空補值。

若第一頁剛好 1000 筆，**不可**只做 `last_T + 1` 後再查：同一毫秒可能有多筆
aggregate，會造成漏資料。第一頁用時間查詢取得起始 `a` 後，後續以
`fromId=last_a+1` 分頁，client 端依 `T <= cutoff_ms` 過濾、以 `a` 去重，直到
第一筆 `T > cutoff_ms` 或已拿到少於 1000 筆。回放的存檔可排序 `(T, a)` 求
可重現性，但它不證明同一毫秒內的因果先後。

可用 1s kline 做低成本預篩：

```text
GET /api/v3/klines
    ?symbol=<ASSET>USDT
    &interval=1s
    &startTime=E_ms-30_000
    &endTime=E_ms-10_001
    &limit=20
```

這剛好避免納入從 `E_ms-10_000` 開始的那一秒。其 close 序列可測試淨方向，
但若規則依賴「窗口內是否曾逆向」，應回到 `aggTrades` 算 peak-to-decision
adverse reversal，而非從 OHLC 推論秒內順序。

## 回測時必須保留的欄位與限制

每個 asset/round 至少存：`symbol`、`S_ms`、`E_ms`、`D_ms`、`L_ms`、kline open、
所有 `(a,T,p,q)`、缺洞旗標、API host、HTTP status、`X-MBX-USED-WEIGHT-*`、以及
**PM 最終 UMA resolution label**。以 `T` 在 cutoff 後的成交、收線 close、或
PM 結算後才出現的資料都不可參與該場 decision feature。

這份資料可以檢驗「弱 5m candle（例如絕對值小於 5 bp）時，尾段反向是否要 veto」
的假說，但不能單獨驗證 Chainlink TWAP 或 Polymarket 成交可得性：Binance
BTC/USDT（及其他 USDT spot）與 PM 的最終 oracle/交易簿是不同資料源。策略
報表需把 proxy-direction 命中率、PM 最終結算命中率、以及實際 CLOB 可成交價格
分開列示。
