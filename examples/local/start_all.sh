#!/usr/bin/env bash
set -euo pipefail

if [[ "${BASH_SOURCE[0]}" != "$0" ]]; then
  echo "error: execute this script instead of sourcing it" >&2
  return 1
fi

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
CONFIG_FILE="${REPO_ROOT}/config/local.config"
ENV_FILE="${REPO_ROOT}/config/local.env"
COMPOSE_FILE="${SCRIPT_DIR}/compose.yaml"
COMPOSE_PROJECT="agent-sandbox-local"
INITIALIZE=false

# shellcheck source=config_env.sh
source "${SCRIPT_DIR}/config_env.sh"

usage() {
  cat <<'USAGE'
Usage: examples/local/start_all.sh [--init]

Starts the local sandbox manager.

Options:
  --init   Build and start project-local dependencies and the manager.
  --help   Show this help.
USAGE
}

die() {
  echo "error: $*" >&2
  exit 1
}

wait_for_health() {
  local url="$1"
  local attempts=75

  while ((attempts > 0)); do
    if curl --fail --silent --show-error --max-time 1 "${url}" >/dev/null 2>&1; then
      return 0
    fi
    attempts=$((attempts - 1))
    sleep 0.2
  done
  return 1
}

while (($# > 0)); do
  case "$1" in
    --init)
      INITIALIZE=true
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      usage >&2
      die "unknown argument: $1"
      ;;
  esac
  shift
done

[[ -f "${CONFIG_FILE}" ]] || die "missing ${CONFIG_FILE}"
[[ -f "${ENV_FILE}" ]] || die "missing ${ENV_FILE}"
command -v docker >/dev/null 2>&1 || die "docker is required"
command -v curl >/dev/null 2>&1 || die "curl is required"

# Values are added only when the caller did not already provide them.
load_env_defaults "${ENV_FILE}"
load_env_defaults "${CONFIG_FILE}"

: "${MANAGER_PORT:?MANAGER_PORT is required in local.config}"
: "${NATS_MONITOR_PORT:?NATS_MONITOR_PORT is required in local.config}"
: "${NATS_MANAGER_PASSWORD:?NATS_MANAGER_PASSWORD is required in local.env}"
: "${NATS_AGENT_PASSWORD:?NATS_AGENT_PASSWORD is required in local.env}"

if [[ "${INITIALIZE}" == true ]]; then
  docker compose -p "${COMPOSE_PROJECT}" -f "${COMPOSE_FILE}" up -d --build
else
  wait_for_health "http://127.0.0.1:${NATS_MONITOR_PORT}/jsz" ||
    die "NATS JetStream is unavailable; run with --init"
  docker compose -p "${COMPOSE_PROJECT}" -f "${COMPOSE_FILE}" up -d --no-deps sandbox-manager
fi

if ! wait_for_health "http://127.0.0.1:${MANAGER_PORT}/healthz"; then
  docker compose -p "${COMPOSE_PROJECT}" -f "${COMPOSE_FILE}" logs --no-color sandbox-manager >&2 || true
  die "sandbox manager did not become healthy"
fi

echo "sandbox manager started"
echo "health: http://127.0.0.1:${MANAGER_PORT}/healthz"
echo "logs: docker compose -p ${COMPOSE_PROJECT} -f ${COMPOSE_FILE} logs -f sandbox-manager"
