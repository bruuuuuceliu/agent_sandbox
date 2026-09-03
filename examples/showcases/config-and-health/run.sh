#!/usr/bin/env bash
set -euo pipefail

if [[ "${BASH_SOURCE[0]}" != "$0" ]]; then
  echo "error: execute this script instead of sourcing it" >&2
  return 1
fi

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../../.." && pwd)"
PURGE=false

# shellcheck source=../../local/config_env.sh
source "${REPO_ROOT}/examples/local/config_env.sh"

if (($# > 1)); then
  echo "usage: $0 [--purge]" >&2
  exit 2
fi
if (($# == 1)); then
  [[ "$1" == "--purge" ]] || {
    echo "usage: $0 [--purge]" >&2
    exit 2
  }
  PURGE=true
fi

cleanup() {
  if [[ "${PURGE}" == true ]]; then
    "${REPO_ROOT}/examples/local/stop_all.sh" --purge
  else
    "${REPO_ROOT}/examples/local/stop_all.sh"
  fi
}
trap cleanup EXIT

"${REPO_ROOT}/examples/local/start_all.sh" --init

load_env_defaults "${REPO_ROOT}/config/local.env"
load_env_defaults "${REPO_ROOT}/config/local.config"

curl --fail --silent --show-error "http://127.0.0.1:${MANAGER_PORT}/healthz"
echo
curl --fail --silent --show-error "http://127.0.0.1:${MANAGER_PORT}/readyz"
echo
