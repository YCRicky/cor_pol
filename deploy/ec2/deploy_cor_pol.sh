#!/usr/bin/env bash
# Install the Aftertake Python package under cor_pol's existing EC2 contract.
set -euo pipefail

REPO_DIR="/opt/cor_pol"
VENV_DIR="${REPO_DIR}/.venv"
VENV_PYTHON="${VENV_DIR}/bin/python"
VENV_PIP="${VENV_DIR}/bin/pip"
ENV_FILE="${REPO_DIR}/.env"
UNIT_SOURCE="${REPO_DIR}/deploy/systemd/cor-pol.service.example"
UNIT_TARGET="/etc/systemd/system/cor-pol.service"

test "$(id -u)" -eq 0 || { echo "run this script with sudo" >&2; exit 1; }
if [[ "$(pwd -P)" != "${REPO_DIR}" ]]; then
  echo "run from ${REPO_DIR} after updating the checkout" >&2
  exit 2
fi
test -f "${ENV_FILE}" || { echo "missing deployment environment file: ${ENV_FILE}" >&2; exit 2; }
test -f "${UNIT_SOURCE}" || { echo "missing systemd unit source: ${UNIT_SOURCE}" >&2; exit 2; }
python3 -c 'import sys; assert sys.version_info >= (3, 9, 10), "Aftertake requires Python 3.9.10+"'

read_env() {
  local key="$1"
  sed -n -E "s/^[[:space:]]*${key}[[:space:]]*=[[:space:]]*(.*)[[:space:]]*$/\1/p" "${ENV_FILE}" \
    | tail -n 1 \
    | sed -E 's/^"(.*)"$/\1/; s/^'"'"'(.*)'"'"'$/\1/'
}


ensure_env_kv() {
  local key="$1"
  local value="$2"
  local tmp
  tmp="$(mktemp)"
  if grep -q -E "^[[:space:]]*${key}[[:space:]]*=" "${ENV_FILE}"; then
    sed -E "s|^[[:space:]]*${key}[[:space:]]*=.*|${key}=${value}|" "${ENV_FILE}" > "${tmp}"
  else
    cp "${ENV_FILE}" "${tmp}"
    printf '
%s=%s
' "${key}" "${value}" >> "${tmp}"
  fi
  cat "${tmp}" > "${ENV_FILE}"
  rm -f "${tmp}"
}

comment_out_legacy_env() {
  local key="$1"
  local tmp
  tmp="$(mktemp)"
  sed -E "s|^[[:space:]]*(${key}[[:space:]]*=.*)|# legacy disabled by deploy_cor_pol.sh: \1|" "${ENV_FILE}" > "${tmp}"
  cat "${tmp}" > "${ENV_FILE}"
  rm -f "${tmp}"
}

normalize_runtime_env() {
  # Non-secret deployment policy. Keep credentials untouched, but prevent stale
  # legacy single-asset settings from silently turning live back into BTC-only.
  ensure_env_kv AFTERTAKE_ASSETS BTC,ETH,XRP,HYPE,DOGE,SOL
  ensure_env_kv AFTERTAKE_ORDER_TYPE GTC
  if grep -q -E "^[[:space:]]*AFTERTAKE_ASSET[[:space:]]*=" "${ENV_FILE}"; then
    comment_out_legacy_env AFTERTAKE_ASSET
  fi
}

require_env() {
  local key="$1"
  local value
  value="$(read_env "${key}")"
  test -n "${value}" || { echo "required setting is blank: ${key}" >&2; exit 2; }
}

normalize_runtime_env

dry_run="$(read_env AFTERTAKE_DRY_RUN)"
test "${dry_run}" = "true" || test "${dry_run}" = "false" || {
  echo "AFTERTAKE_DRY_RUN must be true or false" >&2; exit 2;
}
require_env TG_BOT_TOKEN
require_env TG_CHAT_ID
if test "${dry_run}" = "false"; then
  for key in POLYMARKET_PRIVATE_KEY POLYMARKET_API_KEY POLYMARKET_API_SECRET POLYMARKET_PASSPHRASE FUNDER_ADDRESS SIGNATURE_TYPE; do
    require_env "${key}"
  done
fi

python3 -m venv "${VENV_DIR}"
"${VENV_PIP}" install -e "${REPO_DIR}[live]"
install -m 0644 "${UNIT_SOURCE}" "${UNIT_TARGET}"
systemctl daemon-reload
systemctl reset-failed cor-pol
systemctl enable cor-pol
systemctl restart cor-pol
systemctl --no-pager --full status cor-pol
