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
OUT_DIR="/var/lib/cor-pol/out"

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

ensure_env_default() {
  local key="$1"
  local value="$2"
  if ! grep -q -E "^[[:space:]]*${key}[[:space:]]*=" "${ENV_FILE}"; then
    ensure_env_kv "${key}" "${value}"
  fi
}

reject_legacy_timing_env() {
  if grep -q -E "^[[:space:]]*AFTERTAKE_PRE_" "${ENV_FILE}"; then
    echo "unsupported legacy pre-entry timing settings; refusing deployment" >&2
    exit 2
  fi
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
  reject_legacy_timing_env
  ensure_env_kv AFTERTAKE_ASSETS BTC,ETH,XRP,HYPE,DOGE,SOL
  ensure_env_default AFTERTAKE_QTY 50
  ensure_env_default AFTERTAKE_ORDER_TYPE GTC
  ensure_env_default AFTERTAKE_POST_CLOSE_SNAPSHOT_DELAY_S 0.5
  ensure_env_default AFTERTAKE_POST_CLOSE_LEADER_BID_THRESHOLD 0.80
  ensure_env_default AFTERTAKE_POST_CLOSE_PAIRED_MAX_AGE_S 0.250
  ensure_env_default AFTERTAKE_POST_CLOSE_SNAPSHOT_MAX_LATENESS_S 0.250
  ensure_env_default AFTERTAKE_POST_CLOSE_LIMIT_PRICE 0.99
  if grep -q -E "^[[:space:]]*AFTERTAKE_ASSET[[:space:]]*=" "${ENV_FILE}"; then
    comment_out_legacy_env AFTERTAKE_ASSET
  fi
}

require_post_close_contract() {
  local qty order_type delay threshold paired_age lateness limit
  qty="$(read_env AFTERTAKE_QTY)"
  order_type="$(read_env AFTERTAKE_ORDER_TYPE)"
  delay="$(read_env AFTERTAKE_POST_CLOSE_SNAPSHOT_DELAY_S)"
  threshold="$(read_env AFTERTAKE_POST_CLOSE_LEADER_BID_THRESHOLD)"
  paired_age="$(read_env AFTERTAKE_POST_CLOSE_PAIRED_MAX_AGE_S)"
  lateness="$(read_env AFTERTAKE_POST_CLOSE_SNAPSHOT_MAX_LATENESS_S)"
  limit="$(read_env AFTERTAKE_POST_CLOSE_LIMIT_PRICE)"
  case "${qty}" in
    50|50.0|50.00) ;;
    *) echo "AFTERTAKE_QTY must be 50 for the close+500ms live contract; found '${qty}'. Refusing deployment." >&2; exit 2 ;;
  esac
  test "${order_type}" = "GTC" || {
    echo "AFTERTAKE_ORDER_TYPE must be GTC for the close+500ms live contract; found '${order_type}'. Refusing deployment" >&2
    exit 2
  }
  case "${delay}" in
    0.5|0.50|0.500) ;;
    *) echo "AFTERTAKE_POST_CLOSE_SNAPSHOT_DELAY_S must be 0.5; found '${delay}'. Refusing deployment." >&2; exit 2 ;;
  esac
  case "${threshold}" in
    0.8|0.80|0.800) ;;
    *) echo "AFTERTAKE_POST_CLOSE_LEADER_BID_THRESHOLD must be 0.80; found '${threshold}'. Refusing deployment." >&2; exit 2 ;;
  esac
  case "${paired_age}" in
    0.25|0.250|0.2500) ;;
    *) echo "AFTERTAKE_POST_CLOSE_PAIRED_MAX_AGE_S must be 0.250; found '${paired_age}'. Refusing deployment." >&2; exit 2 ;;
  esac
  case "${lateness}" in
    0.25|0.250|0.2500) ;;
    *) echo "AFTERTAKE_POST_CLOSE_SNAPSHOT_MAX_LATENESS_S must be 0.250; found '${lateness}'. Refusing deployment." >&2; exit 2 ;;
  esac
  case "${limit}" in
    0.99|0.990) ;;
    *) echo "AFTERTAKE_POST_CLOSE_LIMIT_PRICE must be 0.99; found '${limit}'. Refusing deployment." >&2; exit 2 ;;
  esac
}

require_env() {
  local key="$1"
  local value
  value="$(read_env "${key}")"
  test -n "${value}" || { echo "required setting is blank: ${key}" >&2; exit 2; }
}

normalize_runtime_env
require_post_close_contract

install -d -o ubuntu -g ubuntu -m 0750 "${OUT_DIR}"
test -w "${OUT_DIR}" || { echo "runtime output directory is not writable: ${OUT_DIR}" >&2; exit 2; }

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

if command -v timedatectl >/dev/null 2>&1; then
  ntp_synchronized="$(timedatectl show -p NTPSynchronized --value 2>/dev/null || true)"
  test "${ntp_synchronized}" = "yes" || {
    echo "system clock is not NTP-synchronized; refusing close-timestamp deployment" >&2
    exit 3
  }
fi

python3 -m venv "${VENV_DIR}"
"${VENV_PIP}" install -e "${REPO_DIR}[dev,live]"
"${VENV_PYTHON}" -m compileall -q "${REPO_DIR}/src"
"${VENV_PYTHON}" -m ruff check "${REPO_DIR}/src" "${REPO_DIR}/tests"
"${VENV_PYTHON}" -m pytest -q "${REPO_DIR}/tests"
install -m 0644 "${UNIT_SOURCE}" "${UNIT_TARGET}"
systemctl daemon-reload
systemctl reset-failed cor-pol
systemctl enable cor-pol
systemctl restart cor-pol
initial_pid="$(systemctl show cor-pol -p MainPID --value)"
test "${initial_pid}" -gt 0 || { echo "cor-pol did not acquire a MainPID" >&2; exit 4; }
ready_seen=0
for _attempt in $(seq 1 120); do
  sleep 1
  systemctl is-active --quiet cor-pol || {
    journalctl -u cor-pol -n 100 --no-pager >&2
    exit 4
  }
  current_pid="$(systemctl show cor-pol -p MainPID --value)"
  test "${current_pid}" = "${initial_pid}" || {
    echo "cor-pol restarted during the deployment readiness gate" >&2
    journalctl -u cor-pol -n 100 --no-pager >&2
    exit 4
  }
  # Audit records are one JSON object per line. Require a BOOT record for this
  # exact MainPID before accepting its later RUNTIME_READY record; two separate
  # greps could combine an old READY with a new process's BOOT and falsely pass.
  if test -f "${OUT_DIR}/runtime.jsonl" \
    && awk -v pid="${initial_pid}" '
      function has_pid(line) {
        return line ~ ("\"pid\"[[:space:]]*:[[:space:]]*" pid "([[:space:],}]|$)")
      }
      /"kind"[[:space:]]*:[[:space:]]*"boot"/ && has_pid($0) { boot_seen = 1 }
      boot_seen && /"kind"[[:space:]]*:[[:space:]]*"runtime_ready"/ && has_pid($0) { ready_seen = 1 }
      END { exit ready_seen ? 0 : 1 }
    ' "${OUT_DIR}/runtime.jsonl"; then
    ready_seen=1
    break
  fi
done
test "${ready_seen}" -eq 1 || {
  echo "cor-pol did not reach RUNTIME_READY within 120 seconds" >&2
  journalctl -u cor-pol -n 200 --no-pager >&2
  exit 4
}
systemctl --no-pager --full status cor-pol
