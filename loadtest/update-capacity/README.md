# Linux update-capacity experiment

Run from the repository root:

```sh
bash scripts/test-update-capacity-load.sh
# Positive sizing control: 4 cores, one-core worker requests, 0.5-core reserve.
LOAD_PROFILE=roomy bash scripts/test-update-capacity-load.sh
```

Requires Go, Docker with a Linux engine, and network access to build the two
local images. The launcher cross-compiles the current ShinyHub source for the
engine's architecture. It creates an isolated network, server and generator,
then removes only that run's containers and network. Nothing is published.
Evidence is written to the gitignored `loadtest/results/update-capacity-*`.
On a Linux host without Go, build the binary in a Go container first and pass
its absolute path as `LOAD_BINARY_OVERRIDE`. For an exported source archive,
also set `LOAD_COMMIT` to its exact source commit. The report always records
the executable hash. The binary must match the Docker engine's architecture.

The generator runs in Linux **outside** the server's one-core, 2 GiB cgroup
(four cores in the roomy profile).
This avoids consuming the measured budget and avoids macOS VM port-forwarding
limits at 1,000 connections. One Chromium page and 999 protocol clients establish
actual Shiny sessions. Each input must produce the matching sequence number,
unchanged session identity, and the expected private memory allocation. An idle
WebSocket or a page load alone does not count as an established session.

The synthetic app holds 128 KiB of touched private memory per session. Frequent
users burn 50 ms of process CPU per computation; background inputs burn 5 ms.
Initial rendering does not burn CPU.
Twenty protocol clients interact every five seconds in the light phase and every
second under pressure; the remaining clients interact every two minutes. Clients
allow one outstanding computation, preventing an unbounded synthetic backlog.
The real browser repeatedly clicks Compute and verifies its visible result.
Protocol clients negotiate uncompressed frames: one Node process's shared zlib
pool otherwise delays control frames behind 1,000 compressed streams. Chromium
uses its normal compression negotiation. Throughput figures therefore do not
model server-side compression costs for 1,000 Chrome clients.

The experiment asserts:

- A single-session control and the 1,000-session ramp complete successfully.
- A code update is refused for capacity while the original process and sessions
  remain available. Continuous page probes must return the actual app, with no
  errors or fallback pages.
- A real native producer succeeds and its scheduled roll defers for capacity.
- A preserving reload adopts the original consumer and retains every session,
  including the real browser connection and reactive state.
- With compute pressure released, the scheduled roll retries automatically.
  Original reload-relayed sessions are deliberately closed before this serving
  replacement; their retiring relay must release its ownership fence first.
  The replacement population is idle to release compute capacity.
- A reload waiting for that roll adopts the exact canonical replacement handle
  and endpoint, which remain running, unquarantined and reactive after handoff.
  A session on the temporary surge is deliberately drained; it must not be
  mistaken for a lost canonical worker.
- Retrying the refused code upload publishes v2 successfully.

The roomy profile instead admits a live code update with 1,000 established
sessions, checks a new v2 session, and overlaps a preserving reload with the
handoff. It uses one-core worker requests and a 0.5-core reserve on four cores
to validate the larger accounting budget through the actual runtime.

A roll's configured 12-second drain intentionally closes old-generation
sessions. The report counts those separately from unexpected disconnects.
Reports include source and binary hashes, image identity, reactive/page latency
quantiles, browser observations, API process metrics, raw cgroup CPU/memory
samples and the server log. Failed controls invalidate the run; they are evidence
to investigate, not successful stress results.

The experimental request of `0.35` cores and reserve of `0.1` deliberately permit
an idle replacement on the one-core node. They are **not production sizing
recommendations**. Adjust with `LOAD_CPU_REQUEST`, `LOAD_CPU_RESERVE` and
`LOAD_CORES`; profiles must still reach the intended CPU pressure to be useful.
This fixture tests lifecycle behavior, not the throughput of a real dashboard.
Production sizing must include that dashboard's computation, data, libraries,
background work and latency target.

## Locating reactive latency

Run these controls sequentially on an otherwise idle Linux engine:

```sh
LOAD_PROFILE=latency bash scripts/test-update-capacity-load.sh
LOAD_PROFILE=latency LOAD_DIRECT=1 bash scripts/test-update-capacity-load.sh
LOAD_PROFILE=latency LOAD_REPLICAS=2 bash scripts/test-update-capacity-load.sh
LOAD_PROFILE=latency LOAD_CORES=1 bash scripts/test-update-capacity-load.sh
# Separate diagnostic pass: Python profiling changes the measured workload.
LOAD_PROFILE=latency LOAD_WORKER_PROFILE=1 bash scripts/test-update-capacity-load.sh
```

The latency profile uses four cores by default and holds the heavy workload
for 60 seconds after the same ramp and light phase. It performs no update.
Two-worker controls must actually distribute initialized sessions over both
workers. The direct control requires one worker. Both proxied and direct latency
controls share a network namespace with their server to reach its loopback-bound
native workers, while retaining separate compute and memory cgroups.

Each computation records Linux monotonic timestamps at the client send, Python
render start/finish, and client receipt. The report separates input-to-render
waiting (including input transport), render execution, and result delivery.
Initialization is excluded from those component quantiles. Component p95 values
must not be added: their percentiles need not describe the same request.

Per-second diagnostics record process CPU counters, host CPU and pressure,
generator CPU and event-loop delays, and server cgroup throttling. The latency
profile also collects a 30-second Go CPU profile through the existing
loopback-only metrics listener and a Node CPU profile. Optional Python profiling
writes a 30-second per-worker cProfile snapshot starting on a pressure-phase
input and rejects profiles that stop before covering the heavy window.
All profiles stay in the ignored
evidence directory. Use `go tool pprof -top <path>/proxy.cpu.pprof`, Chromium
DevTools for `generator.cpuprofile`, and Python `pstats` for `load-worker-*.pstats`.

Inspect host and generator measurements before attributing latency to ShinyHub.
For a virtualized engine, also check the physical host: spare guest CPU does not
prove the hypervisor has spare compute or memory. Reject a contended host as an
uncontended performance control.
A single synchronous Python render runs on one worker regardless of the server's
total CPU allocation. Multiple workers can spread independent sessions; they
do not parallelize one session's computation. Availability assertions alone do
not establish acceptable response time.
