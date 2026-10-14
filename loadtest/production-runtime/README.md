# Linux production runtime qualification

Run from the repository root on an otherwise idle **x86_64 Ubuntu 24.04** host
with systemd, Python 3, local Docker access and passwordless sudo:

```sh
bash scripts/test-production-runtime-matrix.sh
```

The launcher creates private tools and builds the current binary. It installs
no host packages. Node, Chromium and Python dependencies come from the existing
load-test images; R 4.5.2 and its Shiny/jsonlite libraries come from
`rocker/shiny:4.5.2`. Network access is needed for uncached images and packages.
The native R extraction is qualified on the host above, not arbitrary Linux
architectures or distributions. Do not run this on a production host.

Each case runs an isolated transient systemd service as the invoking user, with
CPU/memory delegation, `NoNewPrivileges`, `ProtectSystem=full`, `PrivateTmp`, a
four-core CPU ceiling and a 3 GiB memory ceiling. Each worker has a one-core,
512 MiB limit, verified from actual kernel cgroup files. The generator and
Chromium run outside the server cgroup. Cleanup stops only this run's services
and containers. Existing ShinyHub-managed containers cause an early refusal;
unrelated containers are left alone. Results remain under the ignored
`loadtest/results/runtime-matrix-*` directory.

The service uses `serve --supervise` and the reference unit's `Restart=on-failure`,
five-second restart delay, and permanent-exit-code protection. It requires Linux
pidfds (kernel 5.3+). Systemd must track the supervisor separately from the active
controller. Native multiplex cases also require that the same supervisor remains
MainPID across handoffs. After a preserving reload, the test deliberately kills
the active controller while a retired generation still holds initialized sessions.
Systemd must restart the service and restore a warm worker before any app page
request can trigger spawning; a fresh population must calculate correctly without
compatibility quarantine. This deliberate crash interrupts sessions and is
recorded separately from transitions that must preserve them.

The default matrix has eight cases: Python/R × native/Docker ×
multiplex/grouped. Grouped cases use `worker.warm_spares=1`, `max_workers=1` and
`grouped_size=64`, leaving capacity for a replacement; multiplex cases use
`min_warm_replicas=1`. Sixteen independently initialized sessions include one real
Chromium page. Fixtures load a 16 MiB persistent binary dataset, touch 256 KiB
of private memory per session, and recompute both the dataset checksum and an
applied-filter calculation. The test verifies known numeric answers, session
identity and the browser's selected filter. Startup includes a one-second delay;
file gates make native deployment/reload overlaps reproducible. Producer-roll
staging must begin within 15 seconds when the bounded replacement fits, catching
false capacity deferrals caused by counting warm spares above `max_workers`.
Continuous page probes retain their own client cookie, so they measure availability
for that retained client without allocating a new elastic client on every poll.
The report counts successful app-page probes and requires at least five; separate
new protocol clients and browser populations verify the published generation.
Reloads that overlap publication verify serving availability and successful
acknowledgement; the steady preserving reload checks every retained session.

Supported guarantees are deliberately separate:

| Check | Native multiplex | Native grouped | Docker |
| --- | --- | --- | --- |
| Real app, browser, calculations and kernel worker limits | Required | Required | Required |
| Full restart recovers a warm worker without demand | Required | Required | Required |
| Active-controller crash with a live retired relay triggers service restart | Required | Not exercised | Not exercised |
| Candidate startup failure preserves the previous generation | Required | Required | Live handoff refused before startup |
| Live code deployment | Required, overlapping reload | Required | Explicit native-only refusal checked |
| Atomic producer publication and scheduled roll | Required, adjacent reload | Required | Explicit local-native-tier refusal checked |
| Reload preserves every session and applied state | Required | Elastic-pool refusal checked; drain/reconnect tested | Preserve configuration refused; drain/reconnect tested |

A passing Docker case **does not certify zero-downtime deployments or preserving
reloads**. A passing native grouped case **does not certify preserving reloads**.
These cases certify their supported behavior, safe capability refusals, and
recovery after a deliberately draining reload. Page interruptions during drain,
deliberate controller crash, and full service restart are recorded separately. Other tested transitions
require zero page errors, zero session errors and zero unexpected closes.
Old-generation drain deadlines and explicit test teardown closures are counted
as planned. Producer consumers must see the completed atomic snapshot,
activation success, and no compatibility quarantine after the tested transition.

Every case requires reactive p95 ≤ 3 seconds by default. This is a declared
fixture acceptance target, not a throughput claim about a production dashboard.
The population is intentionally small so lifecycle failures can be reproduced
without a thousand-session stress run. Use the separate
[update-capacity experiment](../update-capacity/README.md) for capacity pressure
and 1,000-session qualification. Neither fixture substitutes for qualification
with the deployed application's data, packages and response-time requirements.

Optional controls:

```sh
MATRIX_CASES=r:native:grouped bash scripts/test-production-runtime-matrix.sh
MATRIX_SESSIONS=32 MATRIX_LATENCY_BUDGET_MS=3000 bash scripts/test-production-runtime-matrix.sh
# For a source export: identify the exact source and its matching Linux binary.
MATRIX_COMMIT=<commit> MATRIX_BINARY_OVERRIDE=/absolute/path/shinyhub-linux \
  bash scripts/test-production-runtime-matrix.sh
```

`MATRIX_SESSIONS` accepts 2–32. `MATRIX_PYTHON_IMAGE` and `MATRIX_R_IMAGE` can
select compatible images; reports record worker image identity. Changing images
changes what is qualified. `result.json` records the source label, executable
SHA-256, browser version, effective isolation, kernel limits, correctness checks,
latencies, planned/unexpected closures and capability boundaries. Case folders
contain CLI output and the service journal. Preserve failed evidence: a failed
case invalidates the run and must not be reported as a successful qualification.
