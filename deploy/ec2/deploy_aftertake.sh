#!/usr/bin/env bash
set -euo pipefail

repo=/opt/aftertake
env_file="$repo/.env"
unit_source="$repo/deploy/systemd/aftertake.service.example"
unit_target=/etc/systemd/system/aftertake.service

test "$(id -u)" -eq 0 || { echo "run this script with sudo" >&2; exit 1; }
test -f "$env_file" || { echo "missing $env_file" >&2; exit 1; }
test -f "$unit_source" || { echo "missing $unit_source" >&2; exit 1; }
python3 -c 'import sys; assert sys.version_info >= (3, 9, 10), "Aftertake requires Python 3.9.10+"'

read_env() {
  local key="$1"
  sed -n -E "s/^[[:space:]]*$key[[:space:]]*=[[:space:]]*(.*)[[:space:]]*$/\\1/p" "$env_file" | tail -n 1 | sed -E 's/^"(.*)"$/\\1/; s/^'"'"'(.*)'"'"'$/\\1/'
}

require_env() {
  local key="$1"
  local value
  value="$(read_env "$key")"
  test -n "$value" || { echo "required setting is blank: $key" >&2; exit 1; }
}

dry_run="$(read_env AFTERTAKE_DRY_RUN)"
test "$dry_run" = "true" || test "$dry_run" = "false" || {
  echo "AFTERTAKE_DRY_RUN must be true or false" >&2; exit 1;
}
require_env TG_BOT_TOKEN
require_env TG_CHAT_ID
if test "$dry_run" = "false"; then
  for key in POLYMARKET_PRIVATE_KEY POLYMARKET_API_KEY POLYMARKET_API_SECRET POLYMARKET_PASSPHRASE FUNDER_ADDRESS SIGNATURE_TYPE; do
    require_env "$key"
  done
fi

id -u aftertake >/dev/null 2>&1 || useradd --system --home /var/lib/aftertake --shell /usr/sbin/nologin aftertake
mkdir -p /var/lib/aftertake/out
chown -R aftertake:aftertake /var/lib/aftertake
chown aftertake:aftertake "$env_file"
chmod 600 "$env_file"

python3 -m venv "$repo/.venv"
"$repo/.venv/bin/pip" install -e "${repo}[live]"
install -m 0644 "$unit_source" "$unit_target"
systemctl daemon-reload
systemctl enable aftertake
systemctl restart aftertake
systemctl --no-pager --full status aftertake
