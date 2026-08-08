# Aftertake

Aftertake 是 Polymarket crypto 5m 的實戰 runner。現在的 production strategy 是
`twap_tail_v2`：只在 Gamma 標示為 30 秒 TWAP 的市場，於結束前 10 秒以 PM CLOB leader
並同步收集 Binance USD-M Futures 作為 audit，不把它當作進場條件。

Binance Futures 不選邊也不阻擋；最終結算仍由 Polymarket 官方結果決定。CLOB 資料不完整、
過期、弱 candle 反轉、方向不符、風控或 fee floor 不通過時都直接跳過。

完整規則與限制在 [策略文件](docs/STRATEGY.md)。

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

`.env.example` 預設 `AFTERTAKE_DRY_RUN=true`：會連 PM CLOB 與 Binance Futures、寫入 timing/hold
證據，但不簽名或送單。只要距離判斷點仍足以完成 preflight，程式會直接加入當前五分鐘市場。
candle。

## Live

只有在自己檢查 shadow 記錄後才設 `AFTERTAKE_DRY_RUN=false` 並填入官方 CLOB V2 credentials。
Live 仍只有一筆受 `.99` cap、顯示深度、費用與帳戶風控限制的 GTC；同一 slug 不會自動重送。
`--deployment-check` 是手動診斷，不會下單。

部署說明： [RUNBOOK](docs/RUNBOOK.md)、[SAFETY](docs/SAFETY.md)、
[ARCHITECTURE](docs/ARCHITECTURE.md)。
