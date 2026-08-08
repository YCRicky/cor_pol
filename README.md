# Aftertake

Aftertake 是 Polymarket crypto 5m 的 runner。提交中的 `twap_tail_v2` runtime 採用與本地
443/443 標籤回放相同的**勝邊候選規則**：只交易 Gamma 明確標示為 30 秒 TWAP 的市場，將 PM 與
Binance Spot 資料固定在 `E−10.25s`，再決定是否送出該場的 `.99` GTC。

候選規則是 PM best-bid leader `>= 0.90` 加上 Binance Spot 5m kline open / aggTrade path gate。
Binance 只做風險篩選，不是 PM 結算 oracle；最終結果依 Polymarket/Gamma。帳戶、風控、費用、
下單與成交則是候選規則後的獨立執行保護，沒有被 443/443 的方向回放涵蓋。

完整規則、回放邊界與重現命令見 [策略文件](docs/STRATEGY.md)。

## 安裝與 shadow

```bash
git clone https://github.com/YCRicky/cor_pol.git
cd cor_pol
python3 -m venv .venv
. .venv/bin/activate
pip install -e '.[dev,live]'
cp .env.example .env
aftertake --forever
```

`.env.example` 預設 `AFTERTAKE_DRY_RUN=true`：會連 PM CLOB、Binance Spot，寫入 timing/hold
證據，但不簽名或送單。只有在本地有對應 feature cache 時，才可用策略文件的 parity command
重算 250/250 訓練與 193/193 保留樣本。

## Live

只有在自己檢查 shadow 與執行紀錄後才設 `AFTERTAKE_DRY_RUN=false` 並填入官方 CLOB V2 credentials。
Live 最多只會對同一 slug 送一筆 `.99` cap 的 GTC，且仍受費用、帳戶、風控與 SQLite reservation
限制。`--deployment-check` 是手動診斷，不會下單。

部署說明：[RUNBOOK](docs/RUNBOOK.md)、[SAFETY](docs/SAFETY.md)、
[ARCHITECTURE](docs/ARCHITECTURE.md)。
