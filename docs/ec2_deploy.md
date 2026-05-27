# EC2 deployment runbook

This is the operational path for running `cor_pol` on an AWS EC2 instance.
The bot does not expose an HTTP port, so the security group only needs inbound
SSH. Outbound HTTPS is required for Polymarket, Gamma, Binance, and Telegram.

## 1. Instance setup

Use an Ubuntu EC2 instance with persistent EBS storage. The bot is light on CPU;
the practical requirements are reliable networking, time sync, and enough disk
for JSONL logs.

```bash
sudo apt update
sudo apt install -y git python3 python3-venv python3-pip build-essential
```

Clone into `/opt/cor_pol`:

```bash
sudo git clone https://github.com/YCRicky/cor_pol /opt/cor_pol
sudo chown -R ubuntu:ubuntu /opt/cor_pol
cd /opt/cor_pol
python3 -m venv .venv
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -r requirements.txt
```

## 2. Environment file

Create `/etc/cor-pol.env` and keep it readable only by root:

```bash
sudo nano /etc/cor-pol.env
sudo chmod 600 /etc/cor-pol.env
```

Use the variables from `.env.example`. For live mode, these are mandatory:

```text
DRY_RUN=false
CLOB_API_URL=https://clob.polymarket.com
CHAIN_ID=137
PRIVATE_KEY=...
CLOB_API_KEY=...
CLOB_SECRET=...
CLOB_PASS_PHRASE=...
DEPOSIT_WALLET_ADDRESS=...
CLOB_SIGNATURE_TYPE=POLY_1271
TG_BOT_TOKEN=...
TG_CHAT_ID=...
```

Keep the strategy variables explicit in `/etc/cor-pol.env` so a restart cannot
silently pick up different defaults.

## 3. systemd service

Install the service file:

```bash
sudo cp /opt/cor_pol/deploy/cor-pol.service.example /etc/systemd/system/cor-pol.service
sudo systemctl daemon-reload
sudo systemctl enable cor-pol
```

First run in dry-run:

```bash
sudo sed -i 's/^DRY_RUN=.*/DRY_RUN=true/' /etc/cor-pol.env
sudo systemctl start cor-pol
sudo journalctl -u cor-pol -f
```

Switch live only after boot, websocket, Gamma settlement polling, and Telegram
are confirmed:

```bash
sudo systemctl stop cor-pol
sudo sed -i 's/^DRY_RUN=.*/DRY_RUN=false/' /etc/cor-pol.env
sudo systemctl start cor-pol
sudo journalctl -u cor-pol -f
```

Useful commands:

```bash
sudo systemctl status cor-pol
sudo systemctl restart cor-pol
sudo systemctl stop cor-pol
sudo journalctl -u cor-pol --since "1 hour ago"
```

## 4. Logs and disk

Runtime JSONL goes to `/opt/cor_pol/out/`. Journald has stdout/stderr.

Install logrotate for old JSONL/log files:

```bash
sudo cp /opt/cor_pol/deploy/logrotate-cor-pol.example /etc/logrotate.d/cor-pol
```

## 5. Updates

```bash
cd /opt/cor_pol
git pull --ff-only origin main
.venv/bin/pip install -r requirements.txt
sudo systemctl restart cor-pol
sudo journalctl -u cor-pol -n 100
```

## 6. Operational notes

- `CORR_MAX_COMBOS_PER_ROUND` and `CORR_MAX_COST_PER_ROUND_USD` are entry-only
  caps. Entry imbalance hedges and Q4 stop-loss reverse buys bypass those caps.
- Redemption is handled by Polymarket UI auto-redeem, not this bot.
- Do not expose the instance publicly beyond SSH.
- Keep `PRIVATE_KEY` and CLOB credentials only in `/etc/cor-pol.env`; do not
  commit live secrets to the repo.
