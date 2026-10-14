#!/usr/bin/env bash
set -euo pipefail

# Local Linux proof, including on macOS: only the disposable container's
# private cgroup subtree is modified. No host cgroup namespace is used.
image=${CGROUP_TEST_IMAGE:-debian:12}
docker image inspect "$image" >/dev/null
architecture=$(docker info --format '{{.Architecture}}')
case "$architecture" in
  aarch64|arm64) architecture=arm64 ;;
  x86_64|amd64) architecture=amd64 ;;
  *) echo "Unsupported Docker architecture: $architecture" >&2; exit 1 ;;
esac
test_dir=$(mktemp -d)
container_id=
cleanup() {
  if [[ -n "$container_id" ]]; then docker rm -f "$container_id" >/dev/null; fi
  rm -rf "$test_dir"
}
trap cleanup EXIT
CGO_ENABLED=0 GOOS=linux GOARCH="$architecture" go test -c ./internal/admission -o "$test_dir/admission.test"
container_id=$(docker create --privileged --cgroupns=private --cpus=2 --memory=512m \
  -e CGROUP_E2E=1 "$image" /tmp/admission.test -test.run '^TestLinuxNestedCgroupCapacity$' -test.v)
docker cp "$test_dir/admission.test" "$container_id:/tmp/admission.test"
docker start -a "$container_id"
result=$(docker inspect --format '{{.State.ExitCode}}' "$container_id")
exit "$result"
