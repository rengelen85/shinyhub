import assert from "node:assert/strict";
import test from "node:test";
import { smokeInspector, websocketPaths } from "../../../scripts/demo-smoke-verified.mjs";
const id = "27a1ae5d-8dce-4e3e-a717-d27d87bc7544";
const trace = (path: string, extra = {}) => ({
  outcome: "ok", exceptions: [],
  event: { request: { url: `https://demo.test${path}?__demo_smoke=${id}`, headers: {} }, response: { status: 101 } },
  logs: [{ level: "info", message: [{ event: "demo_websocket_closed", smoke_id: id, connection_id: path, client_code: 1000, backend_code: 1000 }] }],
  ...extra,
});

test("verification requires distinct two-sided closes for every app", () => {
  const inspector = smokeInspector(id);
  assert.equal(inspector.check(), false);
  for (const path of websocketPaths) inspector.ingest(trace(path));
  assert.equal(inspector.check(), true);
});

test("client success cannot hide server exceptions, error logs, failed invocations or HTTP 5xx", () => {
  for (const fault of [
    { exceptions: [{ message: "Network connection lost" }] },
    { outcome: "exception" }, { outcome: "canceled" },
    { logs: [{ level: "error", message: ["failed"] }] },
    { event: { request: { url: `https://demo.test/?__demo_smoke=${id}` }, response: { status: 500 } } },
  ]) {
    const inspector = smokeInspector(id);
    for (const path of websocketPaths) inspector.ingest(trace(path));
    inspector.ingest(trace(websocketPaths[0], fault));
    assert.throws(() => inspector.check(), /server-side|unsuccessful|5xx/);
  }
});

test("unrelated visitor failures are ignored and missing/abnormal backend evidence never passes", () => {
  const inspector = smokeInspector(id);
  inspector.ingest({ outcome: "exception", event: { request: { url: "https://demo.test/", headers: { cookie: "private" } } } });
  for (const path of websocketPaths.slice(0, 2)) inspector.ingest(trace(path));
  inspector.ingest(trace(websocketPaths[2], { logs: [] }));
  assert.equal(inspector.check(), false);
  inspector.ingest(trace(websocketPaths[2]));
  assert.equal(inspector.check(), true);
});

test("HTTP correlation supports header casing and truncated tail streams fail closed", () => {
  const inspector = smokeInspector(id);
  inspector.ingest({ outcome: "ok", event: { request: { url: "https://demo.test/__demo/status", headers: { "X-ShinyHub-Demo-Smoke": id } } } });
  assert.equal(inspector.observed, true);
  inspector.ingest({ type: "tail-info", message: "sampling" });
  assert.throws(() => inspector.check(), /incomplete telemetry/);
});
