import { pathToFileURL } from "node:url";
import { createRequire } from "node:module";
const require = createRequire(new URL("../deploy/cloudflare-demo/package.json", import.meta.url));

export async function checkWebSocket(endpoint, Socket = WebSocket, timeoutMS = 15_000) {
  await new Promise((resolve, reject) => {
    const socket = new Socket(endpoint);
    let opened = false;
    const timer = setTimeout(() => {
      socket.close();
      reject(new Error(`${endpoint} -> timed out`));
    }, timeoutMS);

    socket.addEventListener("open", () => {
      opened = true;
      socket.close(1000, "Smoke test complete");
    }, { once: true });

    socket.addEventListener("close", (event) => {
      clearTimeout(timer);
      if (opened && event.code === 1000 && event.wasClean) resolve();
      else reject(new Error(`${endpoint} -> unexpected websocket close (${event.code})`));
    }, { once: true });

    socket.addEventListener("error", () => {
      clearTimeout(timer);
      reject(new Error(`${endpoint} -> websocket error`));
    }, { once: true });
  });
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const endpoints = process.argv.slice(2);
  if (endpoints.length === 0) {
    console.error("usage: node scripts/demo-websocket-smoke.mjs <wss-url> [...]");
    process.exit(2);
  }
  for (const endpoint of endpoints) {
    const url = new URL(endpoint);
    if (process.env.SHINYHUB_DEMO_SMOKE_ID) url.searchParams.set("__demo_smoke", process.env.SHINYHUB_DEMO_SMOKE_ID);
    // Browsers cannot add headers, but the synthetic client can. Cloudflare
    // redacts query values in tails; this marker correlates server exceptions.
    const Socket = process.env.SHINYHUB_DEMO_SMOKE_ID
      ? class extends require("ws") {
        constructor(endpoint) { super(endpoint, { headers: { "x-shinyhub-demo-smoke": process.env.SHINYHUB_DEMO_SMOKE_ID } }); }
      } : WebSocket;
    await checkWebSocket(url.toString(), Socket);
    console.log(`${endpoint} -> websocket open and clean close`);
  }
}
