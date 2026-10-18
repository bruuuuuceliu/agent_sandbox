#!/bin/sh
set -eu
: "${CI_REGISTRY:?The GitLab container registry must be enabled}"
: "${CI_REGISTRY_IMAGE:?Missing project registry path}"
mkdir -p "$HOME/.docker"
AUTH=$(printf '%s:%s' "$CI_REGISTRY_USER" "$CI_REGISTRY_PASSWORD" | base64 | tr -d '\n')
printf '{"auths":{"%s":{"auth":"%s"}}}\n' "$CI_REGISTRY" "$AUTH" > "$HOME/.docker/config.json"
chmod 600 "$HOME/.docker/config.json"
if [ -n "${REGISTRY_CA_FILE:-}" ]; then
  printf '[registry."%s"]\n  ca = ["%s"]\n' "$CI_REGISTRY" "$REGISTRY_CA_FILE" > /tmp/buildkit-registry.toml
  export BUILDKITD_FLAGS="${BUILDKITD_FLAGS:-} --config /tmp/buildkit-registry.toml"
fi
IMAGE_TAG="dev-${CI_PIPELINE_ID}-${CI_COMMIT_SHORT_SHA}"
while read -r name dockerfile; do
  case "$name" in ''|'#'*) continue ;; esac
  image="$CI_REGISTRY_IMAGE"
  if [ "$name" != app ]; then image="$image/$name"; fi
  set -- build --frontend dockerfile.v0 \
    --local context=. --local dockerfile="$(dirname "$dockerfile")" \
    --opt filename="$(basename "$dockerfile")" \
    --opt "label:org.opencontainers.image.revision=$CI_COMMIT_SHA" \
    --opt "label:org.opencontainers.image.source=$CI_PROJECT_URL" \
    --output "type=image,name=$image:$IMAGE_TAG,push=true"
  if [ "${RAG_CPU_ONLY_TORCH:-0}" = 1 ]; then
    set -- "$@" --opt build-arg:INSTALL_CPU_ONLY_TORCH=1
  fi
  for proxy_name in HTTP_PROXY HTTPS_PROXY NO_PROXY; do
    proxy_value=$(printenv "$proxy_name" || true)
    if [ -n "$proxy_value" ]; then
      set -- "$@" --opt "build-arg:$proxy_name=$proxy_value"
    fi
  done
  buildctl-daemonless.sh "$@"
done < .gitlab/ci/images.txt
