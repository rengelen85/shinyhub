import assert from "node:assert/strict";
import test from "node:test";
import { unstable_dev } from "wrangler";
import { createRequire } from "node:module";
const { WebSocketServer, WebSocket } = createRequire(import.meta.url)("ws");
import { setTimeout as delay } from "node:timers/promises";

test("workerd relay preserves frames and completes both close directions", { timeout: 60_000 }, async () => {
  const backend = new WebSocketServer({ port: 0, host: "127.0.0.1" });
  await new Promise(resolve => backend.once("listening", resolve));
  backend.on("connection", socket => socket.on("message", (data, binary) => {
    if (!binary && data.toString() === "close") socket.close(1000, "backend done");
    else socket.send(data, { binary });
  }));
  const backendPort = backend.address().port;
  const worker = await unstable_dev("runtime/websocket-worker.ts", {
    config: "runtime/wrangler.jsonc", local: true, logLevel: "error", port: 0, persist: false,
    experimental: { disableExperimentalWarning: true, disableDevRegistry: true },
  });
  try {
    for (const mode of ["client", "backend", "immediate"]) {
      const id = crypto.randomUUID();
      const socket = new WebSocket(`ws://${worker.address}:${worker.port}/socket?id=${id}&backend=${backendPort}`);
      socket.binaryType = "arraybuffer";
      const messages: unknown[] = [];
      const closed = new Promise<CloseEvent>((resolve, reject) => {
        socket.addEventListener("error", () => reject(new Error("relay error")));
        socket.addEventListener("close", resolve, { once: true });
      });
      socket.addEventListener("open", () => {
        if (mode === "immediate") { socket.close(1000, "immediate close"); return; }
        socket.send("hello");
        socket.send(new Uint8Array([0, 127, 255]));
      });
      socket.addEventListener("message", event => {
        messages.push(event.data);
        if (messages.length === 2) {
          if (mode === "backend") socket.send("close");
          else socket.close(1000, "client done");
        }
      });
      const event = await closed;
      assert.equal(event.code, 1000);
      assert.equal(event.wasClean, true);
      if (mode !== "immediate") {
        assert.equal(messages[0], "hello");
        assert.deepEqual([...new Uint8Array(messages[1] as ArrayBuffer)], [0, 127, 255]);
      }
      {
        let result;
        for (let attempt = 0; attempt < 20; attempt++) {
          result = await (await worker.fetch(`/result?id=${id}`)).json();
          if (result) break;
          await delay(25);
        }
        assert.deepEqual(result, { clean: true, codes: [1000, 1000] });
      }
    }
  } finally { await worker.stop(); backend.close(); }
});
