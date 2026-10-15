import assert from "node:assert/strict";
import test from "node:test";
import { containerRequest, isTransientContainerError } from "./container-request.ts";
const noWait = async () => {};

test("a safe request recovers once after a rollout error or retryable 5xx", async () => {
  for (const first of [500, 502, 503, 504, new Error("Network connection lost."), Object.assign(new Error("replaced"), { retryable: true })]) {
    let attempts = 0, recovered = 0;
    const response = await containerRequest(new Request("https://demo.test/api/server-info"), async () => {
      if (++attempts === 1) {
        if (first instanceof Error) throw first;
        return new Response("failed", { status: first });
      }
      return new Response("ready");
    }, () => recovered++, noWait);
    assert.equal(await response.text(), "ready");
    assert.equal(attempts, 2);
    assert.equal(recovered, 1);
  }
});

test("writes, body-bearing requests and upgrade handshakes are never replayed", async () => {
  for (const init of [
    { method: "POST", body: "write" }, { method: "PUT", body: "write" }, { method: "DELETE" },
    { headers: { upgrade: "websocket" } },
  ]) {
    let attempts = 0;
    const response = await containerRequest(new Request("https://demo.test/", init), async () => {
      attempts++;
      return new Response("failed", { status: 500 });
    }, () => assert.fail("not recovered"), noWait);
    assert.equal(response.status, 500);
    assert.equal(attempts, 1);
  }
});

test("persistent upstream errors remain visible and retry responses release the first body", async () => {
  let canceled = 0, attempts = 0;
  const response = await containerRequest(new Request("https://demo.test/"), async () => {
    attempts++;
    return new Response(new ReadableStream({ cancel() { canceled++; } }), { status: 502 });
  }, () => assert.fail("not recovered"), noWait);
  assert.equal(response.status, 502);
  assert.equal(attempts, 2);
  assert.equal(canceled, 1);
  await response.body!.cancel();
});

test("a failed retry preserves a real upstream response; exhausted transport failures return a safe 503", async () => {
  let attempts = 0;
  const response = await containerRequest(new Request("https://demo.test/"), async () => {
    if (++attempts === 1) return new Response("application error", { status: 500 });
    throw new Error("Network connection lost.");
  }, () => {}, noWait);
  assert.equal(await response.text(), "application error");
  const down = await containerRequest(new Request("https://demo.test/"), async () => {
    throw new Error("Network connection lost. private details");
  }, () => {}, noWait);
  assert.equal(down.status, 503);
  assert.equal(down.headers.get("retry-after"), "2");
  assert.doesNotMatch(await down.text(), /private/);
});

test("unexpected exceptions and caller cancellation are not retried or hidden", async () => {
  const controller = new AbortController();
  let attempts = 0;
  await assert.rejects(containerRequest(new Request("https://demo.test/", { signal: controller.signal }), async () => {
    attempts++;
    controller.abort();
    throw new Error("Network connection lost.");
  }, () => {}, noWait));
  assert.equal(attempts, 1);
  await assert.rejects(containerRequest(new Request("https://demo.test/"), async () => {
    throw new TypeError("programming error");
  }, () => {}, noWait), /programming error/);
  assert.equal(isTransientContainerError("Network connection lost."), false);
});

test("a successful retry does not abort a long-lived response stream", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  let attempts = 0;
  let retrySignal: AbortSignal;
  await containerRequest(new Request("https://demo.test/"), async request => {
    if (++attempts === 1) return new Response(null, { status: 502 });
    retrySignal = request.signal;
    return new Response("stream");
  }, () => {}, noWait);
  t.mock.timers.tick(10_000);
  assert.equal(retrySignal!.aborted, false);
});

test("a stuck recovery attempt has a deadline even if the backend ignores cancellation", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  let attempts = 0;
  const result = containerRequest(new Request("https://demo.test/"), async () => {
    if (++attempts === 1) throw new Error("Network connection lost.");
    return new Promise<Response>(() => {});
  }, () => {}, noWait);
  await new Promise(resolve => setImmediate(resolve));
  t.mock.timers.tick(5_000);
  assert.equal((await result).status, 503);
});

test("state reads reacquire after eviction, never wake a container, and have a total deadline", async (t) => {
  const { readContainerState } = await import("./container-request.ts");
  t.mock.timers.enable({ apis: ["setTimeout"] });
  let attempts = 0;
  const result = readContainerState(async () => {
    if (++attempts === 1) throw new Error("Durable Object instance is no longer active");
    return { status: "stopped" };
  }, new AbortController().signal);
  await new Promise(resolve => setImmediate(resolve));
  t.mock.timers.tick(250);
  assert.deepEqual(await result, { status: "stopped" });
  assert.equal(attempts, 2);
  const stuck = readContainerState(() => new Promise(() => {}), new AbortController().signal);
  const failure = assert.rejects(stuck, { name: "TimeoutError" });
  t.mock.timers.tick(5_000);
  await failure;
});

test("a canceled request never touches the backend; a late retry response is released", async (t) => {
  await assert.rejects(containerRequest(new Request("https://demo.test/", { signal: AbortSignal.abort() }),
    async () => { assert.fail("canceled request reached backend"); }, () => {}, noWait));
  t.mock.timers.enable({ apis: ["setTimeout"] });
  let attempts = 0, canceled = false;
  let finish: (response: Response) => void;
  const result = containerRequest(new Request("https://demo.test/"), async () => {
    if (++attempts === 1) throw new Error("Network connection lost.");
    return new Promise<Response>(resolve => { finish = resolve; });
  }, () => {}, noWait);
  await new Promise(resolve => setImmediate(resolve));
  t.mock.timers.tick(5_000);
  assert.equal((await result).status, 503);
  finish!(new Response(new ReadableStream({ cancel() { canceled = true; } })));
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(canceled, true);
});
