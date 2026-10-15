import assert from "node:assert/strict";
import test from "node:test";
import { summarizeErrors } from "../../../scripts/demo-error-summary.mjs";

test("diagnostics group duplicate traces and invocations without losing records without IDs", () => {
  const result = summarizeErrors([
    { traceId: "a", requestId: "first", invocationType: "fetch" },
    { $metadata: { traceId: "a", requestId: "second" }, $workers: { eventType: "fetch" } },
    { requestId: "b", invocationType: "alarm" },
    { $metadata: { requestId: "b", origin: "alarm" } },
    { id: "b" }, {}, {},
  ]);
  assert.deepEqual(result, {
    request: { error_records: 5, correlated_failure_groups: 4 },
    lifecycle: { error_records: 2, correlated_failure_groups: 1 },
  });
});

test("diagnostic output contains no URLs, cookies, error text or correlation identifiers", () => {
  const result = JSON.stringify(summarizeErrors([{ traceId: "private", url: "https://private/", error: "private", cookie: "private" }]));
  assert.doesNotMatch(result, /private/);
});
