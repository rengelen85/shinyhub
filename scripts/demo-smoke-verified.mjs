import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { fileURLToPath, pathToFileURL } from "node:url";
import { setTimeout as delay } from "node:timers/promises";

export const websocketPaths = [
  "/app/operations-dashboard/websocket/",
  "/app/bookmarking-demo/websocket/",
  "/app/streamlit-demo/_stcore/stream",
];

// Consume only this smoke run's records. Never print/store raw tail payloads:
// unrelated visitors' request headers can include session cookies.
export function smokeInspector(id) {
  const closed = new Set();
  const connections = new Set();
  let observed = false;
  let failure;
  return {
    ingest(record) {
      if (record.type === "tail-info" || (record.event?.type && record.event?.message)) { failure = "Tail stream reported incomplete telemetry"; return; }
      const request = record.event?.request;
      const headers = request?.headers ?? {};
      let url;
      try { url = request?.url ? new URL(request.url) : undefined; } catch { return; }
      const correlated = Object.entries(headers).some(([key, value]) =>
        key.toLowerCase() === "x-shinyhub-demo-smoke" && value === id)
        || url?.searchParams.get("__demo_smoke") === id;
      if (!correlated) return;
      observed = true;
      if ((record.exceptions?.length ?? 0) > 0 || (record.outcome && record.outcome !== "ok")) {
        failure = "Smoke request produced a server-side exception or unsuccessful invocation";
      }
      if (record.event?.response?.status >= 500 && record.event.response.status !== 503) {
        failure = "Smoke request returned HTTP 5xx";
      }
      for (const log of record.logs ?? []) {
        if (["error", "fatal"].includes(log.level)) failure = "Smoke request emitted a server-side error";
        for (let message of log.message ?? []) {
          if (typeof message === "string") {
            try { message = JSON.parse(message); } catch { continue; }
          }
          if (message?.event === "demo_websocket_closed" && message.smoke_id === id
            && message.client_code === 1000 && message.backend_code === 1000 && url
            && typeof message.connection_id === "string") {
            closed.add(url.pathname);
            connections.add(message.connection_id);
          }
        }
      }
    },
    check() {
      if (failure) throw new Error(failure);
      return observed && websocketPaths.every(path => closed.has(path)) && connections.size >= websocketPaths.length;
    },
    get observed() { return observed; },
  };
}

async function until(check, timeoutMS, signal) {
  const end = Date.now() + timeoutMS;
  while (!check()) {
    if (Date.now() >= end) throw new Error("Timed out waiting for correlated server telemetry");
    await delay(100, undefined, { signal });
  }
}

export async function verifiedSmoke(env = process.env) {
  const { CLOUDFLARE_ACCOUNT_ID: account, CLOUDFLARE_API_TOKEN: token } = env;
  if (!/^[0-9a-f]{32}$/.test(account ?? "") || !token) throw new Error("Cloudflare deployment credentials are required for server verification");
  const worker = env.SHINYHUB_DEMO_WORKER ?? "shinyhub-demo";
  const endpoint = `https://api.cloudflare.com/client/v4/accounts/${account}/workers/scripts/${encodeURIComponent(worker)}/tails`;
  const api = async (url, method, body) => {
    const response = await fetch(url, { method, body: body === undefined ? undefined : JSON.stringify(body),
      headers: { authorization: `Bearer ${token}`, "content-type": "application/json", "user-agent": "ShinyHub-Demo-Verification/1.0" },
      signal: AbortSignal.timeout(15_000),
    });
    if (!response.ok) throw new Error(`Cloudflare tail ${method} failed (HTTP ${response.status})`);
    const data = await response.json();
    if (!data.success) throw new Error(`Cloudflare tail ${method} was unsuccessful`);
    return data.result;
  };
  const id = randomUUID();
  const inspector = smokeInspector(id);
  const controller = new AbortController();
  const stop = () => controller.abort(new Error("Verification interrupted"));
  process.once("SIGTERM", stop);
  process.once("SIGINT", stop);
  let session, socket, child, streamFailure;
  try {
    session = await api(endpoint, "POST", { filters: [] });
    socket = new WebSocket(session.url, "trace-v1");
    let opened = false;
    socket.addEventListener("open", () => { opened = true; socket.send(JSON.stringify({ debug: false })); });
    socket.addEventListener("error", () => { streamFailure = new Error("Cloudflare tail connection failed"); });
    socket.addEventListener("close", () => { streamFailure = new Error("Cloudflare tail disconnected before verification finished"); });
    let messages = Promise.resolve();
    socket.addEventListener("message", event => {
      messages = messages.then(async () => {
        const text = typeof event.data === "string" ? event.data : await event.data.text();
        inspector.ingest(JSON.parse(text));
      }).catch(() => { streamFailure = new Error("Could not decode Cloudflare tail telemetry"); });
    });
    const check = () => { if (streamFailure) throw streamFailure; return inspector.check(); };
    await until(() => { if (streamFailure) throw streamFailure; return opened; }, 15_000, controller.signal);
    // Prove the listener is receiving events before starting the smoke suite.
    const status = new URL("/__demo/status", env.SHINYHUB_DEMO_URL ?? "https://demo.shinyhub.dev");
    await fetch(status, { headers: { "x-shinyhub-demo-smoke": id, "user-agent": "ShinyHub-Demo-Verification/1.0" }, signal: AbortSignal.timeout(10_000) }).then(r => r.arrayBuffer());
    await until(() => { check(); return inspector.observed; }, 20_000, controller.signal);
    await new Promise((resolve, reject) => {
      child = spawn(fileURLToPath(new URL("./demo-smoke.sh", import.meta.url)), [], {
        env: { ...env, SHINYHUB_DEMO_SMOKE_ID: id }, stdio: "inherit", signal: controller.signal,
      });
      child.once("error", reject);
      child.once("exit", code => code === 0 ? resolve() : reject(new Error(`Demo smoke exited with status ${code}`)));
    });
    await until(check, 30_000, controller.signal);
    // Include late transport exceptions following the final close callback.
    await delay(2_000, undefined, { signal: controller.signal });
    await messages;
    check();
    console.log("Demo smoke passed: all three WebSockets closed cleanly at both proxy ends; correlated server telemetry contains no errors.");
  } finally {
    child?.kill("SIGTERM");
    socket?.close();
    process.removeListener("SIGTERM", stop);
    process.removeListener("SIGINT", stop);
    if (session) await api(`${endpoint}/${encodeURIComponent(session.id)}`, "DELETE");
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  verifiedSmoke().catch(error => { console.error(error.message); process.exitCode = 1; });
}
