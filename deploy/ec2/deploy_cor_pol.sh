#!/usr/bin/env bash
# Apply the checked-in cor_pol systemd unit on the EC2 host.
# Run from the repository checkout after `git pull --ff-only`.
set -euo pipefail

REPO_DIR="/opt/cor_pol"
VENV_PYTHON="${REPO_DIR}/.venv/bin/python"
VENV_PIP="${REPO_DIR}/.venv/bin/pip"
ENV_FILE="${REPO_DIR}/.env"
UNIT_SOURCE="${REPO_DIR}/deploy/systemd/cor-pol.service.example"
UNIT_TARGET="/etc/systemd/system/cor-pol.service"

if [[ "$(pwd -P)" != "${REPO_DIR}" ]]; then
  echo "run from ${REPO_DIR} after updating the checkout" >&2
  exit 2
fi
if [[ ! -x "${VENV_PYTHON}" || ! -x "${VENV_PIP}" ]]; then
  echo "missing virtualenv at ${REPO_DIR}/.venv" >&2
  exit 2
fi
if [[ ! -f "${ENV_FILE}" ]]; then
  echo "missing deployment environment file: ${ENV_FILE}" >&2
  exit 2
fi

required=(
  MISPRICE_DRY_RUN
  POLYMARKET_PRIVATE_KEY
  POLYMARKET_API_KEY
  POLYMARKET_API_SECRET
  POLYMARKET_PASSPHRASE
  FUNDER_ADDRESS
  SIGNATURE_TYPE
  TG_BOT_TOKEN
  TG_CHAT_ID
)
missing=()
for name in "${required[@]}"; do
  if ! grep -Eq "^${name}=.+" "${ENV_FILE}"; then
    missing+=("${name}")
  fi
done
if [[ "${#missing[@]}" -gt 0 ]]; then
  echo "missing or empty live deployment fields: ${missing[*]}" >&2
  exit 2
fi
if ! grep -Eq '^MISPRICE_DRY_RUN=false[[:space:]]*$' "${ENV_FILE}"; then
  echo "refusing to start live service until MISPRICE_DRY_RUN=false" >&2
  exit 2
fi

"${VENV_PIP}" install -e '.[live]'
sudo install -m 0644 "${UNIT_SOURCE}" "${UNIT_TARGET}"
sudo systemctl daemon-reload
sudo systemctl reset-failed cor-pol
sudo systemctl enable cor-pol
sudo systemctl restart cor-pol
sudo systemctl is-active --quiet cor-pol

echo "cor-pol is active; confirm DEPLOYMENT_CHECK_OK followed by BOOT in Telegram or journalctl -u cor-pol -f"
