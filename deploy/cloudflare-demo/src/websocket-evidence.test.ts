import assert from "node:assert/strict";
import test from "node:test";
import { SmokeWebSocketEvidence, smokeWebSocketPaths } from "./websocket-evidence.ts";
import { websocketPaths } from "../../../scripts/demo-smoke-verified.mjs";

test("close evidence retains only marked smoke paths and preserves failures", () => {
  assert.deepEqual(smokeWebSocketPaths, websocketPaths);
  const evidence = new SmokeWebSocketEvidence();
  const id = crypto.randomUUID();
  evidence.record(null, smokeWebSocketPaths[0], "visitor", { clean: true, codes: [1000, 1000] });
  evidence.record(id, "/private/visitor/path", "other", { clean: true, codes: [1000, 1000] });
  evidence.record(id, smokeWebSocketPaths[0], "synthetic", { clean: false, codes: [1000, 1006] });
  assert.deepEqual(evidence.read(id), [{ event: "demo_websocket_failed", smoke_id: id,
    smoke_path: smokeWebSocketPaths[0], connection_id: "synthetic", client_code: 1000, backend_code: 1006 }]);
  assert.deepEqual(evidence.read("invalid"), []);
  assert.deepEqual(evidence.read(crypto.randomUUID()), []);
});

test("synthetic evidence is bounded across runs", () => {
  const evidence = new SmokeWebSocketEvidence();
  const first = crypto.randomUUID();
  evidence.record(first, smokeWebSocketPaths[0], "first", { clean: true, codes: [1000, 1000] });
  for (let i = 0; i < 64; i++) evidence.record(crypto.randomUUID(), smokeWebSocketPaths[0], String(i), { clean: true, codes: [1000, 1000] });
  assert.deepEqual(evidence.read(first), []);
});
