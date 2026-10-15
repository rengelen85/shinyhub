import { DurableWebSocketClients } from "../src/durable-websocket-clients.ts";
import { DurableObject } from "cloudflare:workers";
import { bridgeContainerWebSocket } from "../src/container-websocket.ts";

interface Env { RELAY: DurableObjectNamespace<Relay> }
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    // Match the production edge: forward the upgrade response unchanged.
    return env.RELAY.get(env.RELAY.idFromName("runtime")).fetch(request);
  },
};

export class Relay extends DurableObject {
  private clients = new DurableWebSocketClients(this.ctx);
  webSocketMessage(ws: WebSocket, data: string | ArrayBuffer) { this.clients.message(ws, data); }
  webSocketClose(ws: WebSocket, code: number, reason: string, wasClean: boolean) {
    const mode = ws.deserializeAttachment();
    // A client-initiated close must remain half open until the backend replies.
    // Backend-initiated closes have already sent our client-side close frame.
    if (mode !== "backend" && ws.readyState !== WebSocket.CLOSING) {
      throw new Error("Inbound close was acknowledged before relay coordination");
    }
    this.clients.close(ws, code, reason, wasClean);
  }
  webSocketError(ws: WebSocket) { this.clients.error(ws); }
  private results = new Map<string, unknown>();
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const id = url.searchParams.get("id") ?? "test";
    if (url.pathname === "/result") return Response.json(this.results.get(id) ?? null);
    const upstream = await fetch(`http://127.0.0.1:${url.searchParams.get("backend")}/`, {
      headers: { upgrade: "websocket" },
    });
    const middle = new WebSocketPair();
    middle[1].serializeAttachment(url.searchParams.get("mode"));
    this.ctx.waitUntil(bridgeContainerWebSocket(middle[1], upstream.webSocket!, () => {},
      result => this.results.set(id, result), 5000, this.clients.control(middle[1])));
    return new Response(null, { status: 101, webSocket: middle[0], headers: upstream.headers });
  }
}
