#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
scratch="$(mktemp -d)"
network="shinyhub-update-load-$(basename "$scratch" | tr '[:upper:]' '[:lower:]')"
cleanup() {
  local owned
  owned="$(docker ps -aq --filter "label=shinyhub.load_run=$network")"
  if [ -n "$owned" ]; then
    # Docker returns validated container IDs, one per line.
    while IFS= read -r id; do docker rm -f "$id" >/dev/null; done <<< "$owned"
  fi
  docker network rm "$network" >/dev/null 2>&1 || true
  rm -rf "$scratch"
}
trap cleanup EXIT
architecture="$(docker info --format '{{.Architecture}}')"
case "$architecture" in
  aarch64|arm64) arch=arm64 ;;
  x86_64|amd64) arch=amd64 ;;
  *) echo "Unsupported Docker architecture: $architecture" >&2; exit 1 ;;
esac
cd "$root"
if [ -n "${LOAD_BINARY_OVERRIDE:-}" ]; then
  cp "$LOAD_BINARY_OVERRIDE" "$scratch/shinyhub"
else
  GOWORK=off CGO_ENABLED=0 GOOS=linux GOARCH="$arch" go build -o "$scratch/shinyhub" ./cmd/shinyhub
fi
docker build -t shinyhub-update-load:local loadtest/update-capacity
docker build -f loadtest/update-capacity/Dockerfile.driver -t shinyhub-update-load-driver:local loadtest/update-capacity
mkdir -p loadtest/results
docker network create "$network" >/dev/null
network_mode="$network"
network_owner=""
if [ "${LOAD_PROFILE:-pressure}" = latency ]; then
  # Share networking, not compute/memory cgroups, to reach native loopback
  # workers without adding a second proxy to the direct control.
  network_owner="$network-net"
  docker run -d --name "$network_owner" --label "shinyhub.load_run=$network" \
    --network "$network" shinyhub-update-load:local sleep infinity >/dev/null
  network_mode="container:$network_owner"
fi
docker create --name "$network-driver" --label "shinyhub.load_run=$network" --network "$network_mode" \
  --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
  --mount "type=bind,src=$root,dst=/work" \
  -e LOAD_NETWORK="$network" -e LOAD_BINARY=/binary \
  -e LOAD_NETWORK_OWNER="$network_owner" \
  -e LOAD_COMMIT="${LOAD_COMMIT:-$(git rev-parse HEAD)}" \
  -e LOAD_PROFILE="${LOAD_PROFILE:-pressure}" -e LOAD_SESSIONS="${LOAD_SESSIONS:-1000}" \
  -e LOAD_CORES="${LOAD_CORES:-}" \
  -e LOAD_REPLICAS="${LOAD_REPLICAS:-1}" -e LOAD_DIRECT="${LOAD_DIRECT:-0}" \
  -e LOAD_WORKER_PROFILE="${LOAD_WORKER_PROFILE:-0}" \
  -e LOAD_OWNER_UID="$(id -u)" -e LOAD_OWNER_GID="$(id -g)" \
  -e LOAD_CPU_REQUEST="${LOAD_CPU_REQUEST:-}" -e LOAD_CPU_RESERVE="${LOAD_CPU_RESERVE:-}" \
  shinyhub-update-load-driver:local node /work/loadtest/update-capacity/run.mjs >/dev/null
docker cp "$scratch/shinyhub" "$network-driver:/binary"
docker start -a "$network-driver"
exit "$(docker inspect --format '{{.State.ExitCode}}' "$network-driver")"
