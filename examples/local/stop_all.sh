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
RUNTIME_DIR="${REPO_ROOT}/.runtime/local"
EXPECTED_RUNTIME_DIR="${REPO_ROOT}/.runtime/local"
PURGE=false

# shellcheck source=config_env.sh
source "${SCRIPT_DIR}/config_env.sh"

usage() {
  cat <<'USAGE'
Usage: examples/local/stop_all.sh [--purge]

Stops the local sandbox manager and its project-local dependencies.

Options:
  --purge  Also remove this project's containers, images, network, volumes, and runtime files.
  --help   Show this help.
USAGE
}

die() {
  echo "error: $*" >&2
  exit 1
}

while (($# > 0)); do
  case "$1" in
    --purge)
      PURGE=true
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

command -v docker >/dev/null 2>&1 || die "docker is required"
[[ -f "${CONFIG_FILE}" ]] || die "missing ${CONFIG_FILE}"
[[ -f "${ENV_FILE}" ]] || die "missing ${ENV_FILE}"

load_env_defaults "${ENV_FILE}"
load_env_defaults "${CONFIG_FILE}"

if [[ "${PURGE}" == true ]]; then
  docker compose -p "${COMPOSE_PROJECT}" -f "${COMPOSE_FILE}" down --volumes --remove-orphans --rmi local
else
  docker compose -p "${COMPOSE_PROJECT}" -f "${COMPOSE_FILE}" stop
fi

if [[ "${PURGE}" == true ]]; then
  [[ "${RUNTIME_DIR}" == "${EXPECTED_RUNTIME_DIR}" ]] ||
    die "refusing to purge unexpected runtime path: ${RUNTIME_DIR}"
  [[ "${RUNTIME_DIR}" == "${REPO_ROOT}/.runtime/local" ]] ||
    die "runtime path escaped the repository"
  rm -rf -- "${RUNTIME_DIR}"
  echo "project-local containers, images, volumes, and runtime files purged"
fi
