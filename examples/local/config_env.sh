#!/usr/bin/env bash

load_env_defaults() {
  local file="$1"
  local line
  local key
  local value

  [[ -f "${file}" ]] || {
    echo "error: missing ${file}" >&2
    return 1
  }

  while IFS= read -r line || [[ -n "${line}" ]]; do
    line="${line#"${line%%[![:space:]]*}"}"
    line="${line%"${line##*[![:space:]]}"}"
    [[ -z "${line}" || "${line}" == \#* ]] && continue
    [[ "${line}" == export\ * ]] && line="${line#export }"
    [[ "${line}" == *=* ]] || {
      echo "error: invalid KEY=VALUE line in ${file}" >&2
      return 1
    }

    key="${line%%=*}"
    value="${line#*=}"
    key="${key#"${key%%[![:space:]]*}"}"
    key="${key%"${key##*[![:space:]]}"}"
    [[ "${key}" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || {
      echo "error: invalid variable name in ${file}" >&2
      return 1
    }
    [[ -v "${key}" ]] && continue

    if [[ "${value}" == \"*\" && "${value}" == *\" ]]; then
      value="${value:1:${#value}-2}"
    elif [[ "${value}" == \'*\' && "${value}" == *\' ]]; then
      value="${value:1:${#value}-2}"
    fi
    export "${key}=${value}"
  done <"${file}"
}
