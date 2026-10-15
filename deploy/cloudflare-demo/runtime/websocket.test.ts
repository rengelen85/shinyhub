import assert from "node:assert/strict";
import test from "node:test";
import { unstable_dev } from "wrangler";
import { setTimeout as delay } from "node:timers/promises";

test("workerd relay preserves frames and completes both close directions", { timeout: 60_000 }, async () => {
  const worker = await unstable_dev("runtime/websocket-worker.ts", {
    config: "runtime/wrangler.jsonc", local: true, logLevel: "error", port: 0, persist: false,
    experimental: { disableExperimentalWarning: true, disableDevRegistry: true },
  });
  try {
    for (const backendCloses of [false, true]) {
      const id = crypto.randomUUID();
      const socket = new WebSocket(`ws://${worker.address}:${worker.port}/socket?id=${id}`);
      socket.binaryType = "arraybuffer";
      const messages: unknown[] = [];
      const closed = new Promise<CloseEvent>((resolve, reject) => {
        socket.addEventListener("error", () => reject(new Error("relay error")));
        socket.addEventListener("close", resolve, { once: true });
      });
      socket.addEventListener("open", () => {
        socket.send("hello");
        socket.send(new Uint8Array([0, 127, 255]));
      });
      socket.addEventListener("message", event => {
        messages.push(event.data);
        if (messages.length === 2) {
          if (backendCloses) socket.send("close");
          else socket.close(1000, "client done");
        }
      });
      const event = await closed;
      assert.equal(event.code, 1000);
      assert.equal(event.wasClean, true);
      assert.equal(messages[0], "hello");
      assert.deepEqual([...new Uint8Array(messages[1] as ArrayBuffer)], [0, 127, 255]);
      let result;
      for (let attempt = 0; attempt < 20; attempt++) {
        result = await (await worker.fetch(`/result?id=${id}`)).json();
        if (result) break;
        await delay(25);
      }
      assert.deepEqual(result, { clean: true, codes: [1000, 1000] });
    }
  } finally { await worker.stop(); }
});
