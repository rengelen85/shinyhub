#!/usr/bin/env bash
# Private Linux acceptance rig. No host package installation or existing units.
set -euo pipefail
[ "$(uname -s)" = Linux ] || { echo 'Run this on a Linux host with systemd and local Docker.' >&2; exit 1; }
[ "$(id -u)" != 0 ] || { echo 'Run as an unprivileged user with sudo -n and Docker access.' >&2; exit 1; }
[ "$(uname -m)" = x86_64 ] || { echo "Portable R setup currently requires x86_64 Linux." >&2; exit 1; }
export DOCKER_HOST=unix:///var/run/docker.sock
sudo -n true
docker info >/dev/null
if [ -n "$(docker ps -aq --filter label=shinyhub.managed=true)" ]; then
  echo 'Use a test host without existing ShinyHub-managed containers.' >&2; exit 1
fi
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
task="$(mktemp -d /tmp/shinyhub-matrix.XXXXXXXX)"
owned="shinyhub-matrix-tools-$(basename "$task" | tr '[:upper:]' '[:lower:]')"
cleanup() {
  docker rm -f "$owned" >/dev/null 2>&1 || true
  # Go makes extracted module directories read-only; restore owner write
  # permission only inside this run's private cache before removing it.
  if [ -d "$task/modules" ]; then chmod -R u+w "$task/modules"; fi
  rm -rf "$task"
}
trap cleanup EXIT
mkdir -p "$task/tools/bin" "$task/tools/R/lib" "$task/tools/R/libs"
driver_image=shinyhub-update-load-driver:local
python_image="${MATRIX_PYTHON_IMAGE:-shinyhub-update-load:local}"
r_image="${MATRIX_R_IMAGE:-rocker/shiny:4.5.2}"
docker build -f "$root/loadtest/update-capacity/Dockerfile.driver" -t "$driver_image" "$root/loadtest/update-capacity"
if [ -z "${MATRIX_PYTHON_IMAGE:-}" ]; then
  docker build -t "$python_image" "$root/loadtest/update-capacity"
else
  docker image inspect "$python_image" >/dev/null
fi
docker image inspect "$r_image" >/dev/null 2>&1 || docker pull "$r_image"
docker create --name "$owned" "$driver_image" true >/dev/null
docker cp "$owned:/usr/local/bin/node" "$task/tools/bin/node"
docker cp "$owned:/opt/driver" "$task/tools/driver"
docker rm "$owned" >/dev/null
docker create --name "$owned" "$python_image" true >/dev/null
docker cp "$owned:/usr/local/bin/uv" "$task/tools/bin/uv"
docker rm "$owned" >/dev/null
export UV_CACHE_DIR="$task/uv-cache"
"$task/tools/bin/uv" venv "$task/tools/venv" --python "${MATRIX_PYTHON:-/usr/bin/python3}"
"$task/tools/bin/uv" pip install --python "$task/tools/venv/bin/python" shiny==1.8.0
docker create --name "$owned" "$r_image" true >/dev/null
docker cp "$owned:/usr/local/lib/R" "$task/tools/R/lib/R"
docker cp "$owned:/usr/local/bin/Rscript" "$task/tools/R/Rscript"
docker cp "$owned:/usr/lib/x86_64-linux-gnu/." "$task/tools/R/libs/"
docker rm "$owned" >/dev/null
# This private relocatable R extraction is qualified on x86_64 Ubuntu 24.04.
python3 - "$task/tools" <<'PY'
from pathlib import Path
import sys
tools = Path(sys.argv[1])
rhome = tools / 'R/lib/R'
launcher = rhome / 'bin/R'
text = launcher.read_text()
assert 'R_HOME_DIR=/usr/local/lib/R' in text, 'unexpected R launcher layout'
launcher.write_text(text.replace('R_HOME_DIR=/usr/local/lib/R', f'R_HOME_DIR={rhome}', 1))
wrapper = tools / 'bin/Rscript'
wrapper.write_text(f'#!/bin/sh\nexec env RHOME="{rhome}" R_HOME="{rhome}" R_LIBS_SITE="{rhome}/site-library" LD_LIBRARY_PATH="{rhome}/lib:{tools}/R/libs" "{tools}/R/Rscript" "$@"\n')
wrapper.chmod(0o755)
PY
"$task/tools/bin/Rscript" -e 'stopifnot(requireNamespace("shiny"), requireNamespace("jsonlite"))'
if [ -n "${MATRIX_BINARY_OVERRIDE:-}" ]; then
  cp "$MATRIX_BINARY_OVERRIDE" "$task/shinyhub"
else
  mkdir -p "$task/cache" "$task/modules"
  docker run --rm --init --user "$(id -u):$(id -g)" --cpus=2 --memory=4g \
    --mount "type=bind,src=$root,dst=/work" --mount "type=bind,src=$task,dst=/task" \
    -w /work -e GOWORK=off -e CGO_ENABLED=0 -e GOCACHE=/task/cache -e GOMODCACHE=/task/modules \
    golang:1.26.9-alpine go build -o /task/shinyhub ./cmd/shinyhub
fi
mkdir -p "$root/loadtest/results"
export MATRIX_TOOLS="$task/tools" MATRIX_BINARY="$task/shinyhub"
export MATRIX_R_IMAGE="$r_image" MATRIX_PYTHON_IMAGE="$python_image"
if [ -z "${MATRIX_COMMIT:-}" ]; then
  MATRIX_COMMIT="$(git -C "$root" rev-parse HEAD)"
  if [ -n "$(git -C "$root" status --porcelain)" ]; then MATRIX_COMMIT="$MATRIX_COMMIT-working-tree"; fi
fi
export MATRIX_COMMIT
"$task/tools/bin/node" "$root/loadtest/production-runtime/run.mjs" &
matrix_pid=$!
forward_signal() {
  trap '' INT TERM
  kill -s "$1" "$matrix_pid" 2>/dev/null || true
  wait "$matrix_pid" 2>/dev/null || true
  exit "$2"
}
trap 'forward_signal INT 130' INT
trap 'forward_signal TERM 143' TERM
wait "$matrix_pid"
