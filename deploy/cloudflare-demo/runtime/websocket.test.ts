import assert from "node:assert/strict";
import test from "node:test";
import { spawn } from "node:child_process";

test("workerd relay closes cleanly without uncaught runtime exceptions", { timeout: 60_000 }, async () => {
  // workerd can emit uncaught platform errors even when every client assertion
  // passes. Capture its diagnostics in a child so those errors fail this test.
  const env = { ...process.env };
  delete env.NODE_TEST_CONTEXT;
  const child = spawn(process.execPath, ["--test", "runtime/websocket-scenario.ts"], {
    env, stdio: ["ignore", "pipe", "pipe"], timeout: 55_000,
  });
  let output = "";
  child.stdout.on("data", chunk => { output += chunk; });
  child.stderr.on("data", chunk => { output += chunk; });
  const code = await new Promise<number | null>((resolve, reject) => {
    child.once("error", reject);
    child.once("close", resolve);
  });
  assert.equal(code, 0, output);
  assert.match(output, /workerd relay preserves frames and completes both close directions/, output);
  assert.doesNotMatch(output, /Uncaught\s+(?:Error|exception)|Network connection lost\./i, output);
});
