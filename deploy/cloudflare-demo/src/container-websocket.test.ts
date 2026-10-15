import assert from "node:assert/strict";
import test from "node:test";
import { bridgeContainerWebSocket } from "./container-websocket.ts";

class Socket extends EventTarget {
  readyState = 1;
  binaryType = "blob";
  accepted: unknown;
  sent: unknown[] = [];
  closes: unknown[][] = [];
  accept(options: unknown) { this.accepted = options; }
  send(data: unknown) { this.sent.push(data); }
  close(...args: unknown[]) { this.closes.push(args); }
  message(data: unknown) { this.dispatchEvent(Object.assign(new Event("message"), { data })); }
  ended(code = 1000, wasClean = true) {
    this.dispatchEvent(Object.assign(new Event("close"), { code, reason: "done", wasClean }));
  }
}
const flush = () => new Promise(resolve => setImmediate(resolve));
function fixture(timeout = 50) {
  const client = new Socket(), backend = new Socket();
  const results: { clean: boolean; codes: number[] }[] = [];
  let activity = 0;
  const completion = bridgeContainerWebSocket(client as unknown as WebSocket, backend as unknown as WebSocket,
    () => activity++, result => results.push(result), timeout);
  return { client, backend, results, completion, activity: () => activity };
}

test("both directions preserve text/binary ordering and renew activity", async () => {
  const f = fixture();
  f.client.message(new Blob(["binary"]));
  f.client.message("second");
  f.backend.message("reply");
  await flush();
  assert.equal(new TextDecoder().decode(f.backend.sent[0] as ArrayBuffer), "binary");
  assert.equal(f.backend.sent[1], "second");
  assert.deepEqual(f.client.sent, ["reply"]);
  assert.equal(f.activity(), 3);
  assert.deepEqual(f.client.accepted, { allowHalfOpen: true });
  assert.deepEqual(f.backend.accepted, { allowHalfOpen: true });
});

test("client and backend initiated closes wait for the opposite handshake", async () => {
  for (const initiator of ["client", "backend"] as const) {
    const f = fixture();
    const other = initiator === "client" ? "backend" : "client";
    f[initiator].ended();
    await flush();
    assert.equal(f.results.length, 0);
    assert.deepEqual(f[other].closes, [[1000, "done"]]);
    f[other].ended();
    await flush();
    assert.deepEqual(f.results, [{ clean: true, codes: [1000, 1000] }]);
    f.client.dispatchEvent(new Event("error"));
    assert.equal(f.results.length, 1);
  }
});

test("abnormal closure, send failures, and a missing close acknowledgement finish once as errors", async () => {
  const abnormal = fixture();
  abnormal.client.ended(1006, false);
  await flush();
  abnormal.backend.ended(1011, true);
  await flush();
  assert.equal(abnormal.results[0].clean, false);
  assert.equal(abnormal.backend.closes[0][0], 1011);
  const broken = fixture();
  broken.backend.send = () => { throw new Error("disconnected"); };
  broken.client.message("write");
  await flush();
  assert.equal(broken.results[0].clean, false);
  const timeout = fixture(5);
  timeout.client.ended();
  await new Promise(resolve => setTimeout(resolve, 20));
  assert.equal(timeout.results.length, 1);
  assert.equal(timeout.results[0].clean, false);
});

test("simultaneous close frames complete both half-open sockets before settling", async () => {
  const f = fixture();
  f.client.ended();
  f.backend.ended();
  await flush();
  assert.deepEqual(f.client.closes, [[1000, "done"]]);
  assert.deepEqual(f.backend.closes, [[1000, "done"]]);
  assert.equal(f.results.length, 1);
  assert.equal(f.results[0].clean, true);
});

test("failed backend acceptance releases the relay and reports an error once", () => {
  const client = new Socket(), backend = new Socket();
  backend.accept = () => { throw new Error("disconnected during accept"); };
  const results: unknown[] = [];
  assert.throws(() => bridgeContainerWebSocket(client as unknown as WebSocket, backend as unknown as WebSocket,
    () => {}, result => results.push(result)), /disconnected during accept/);
  assert.deepEqual(results, [{ clean: false, codes: [1006, 1006] }]);
  assert.equal(client.closes.length, 1);
});


test("invocation lifetime remains pending until both close acknowledgements or timeout", async () => {
  const f = fixture();
  let settled = false;
  f.completion.then(() => { settled = true; });
  f.client.ended();
  await flush();
  assert.equal(settled, false);
  f.backend.ended();
  await f.completion;
  assert.equal(settled, true);
  const missing = fixture(5);
  missing.client.ended();
  await missing.completion;
  assert.equal(missing.results[0].clean, false);
});

test("Durable Object event delivery releases its registration on close and failure", async () => {
  for (const failure of [false, true]) {
    const client = new Socket(), backend = new Socket(), delivery = new Socket();
    let disposed = 0;
    const control = {
      accept: () => { delivery.accepted = true; },
      addEventListener: delivery.addEventListener.bind(delivery),
      dispose: () => { disposed++; },
    };
    const results: unknown[] = [];
    const completion = bridgeContainerWebSocket(client as unknown as WebSocket, backend as unknown as WebSocket,
      () => {}, result => results.push(result), 50, control as unknown as WebSocket);
    delivery.message("message from DO handler");
    await flush();
    assert.deepEqual(backend.sent, ["message from DO handler"]);
    assert.equal(client.accepted, undefined);
    if (failure) delivery.dispatchEvent(new Event("error"));
    else {
      delivery.ended();
      await flush();
      assert.equal(disposed, 0);
      backend.ended();
    }
    await completion;
    assert.equal(disposed, 1);
    assert.deepEqual(results, [{ clean: !failure, codes: failure ? [1006, 1006] : [1000, 1000] }]);
  }
});
