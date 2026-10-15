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
  private results = new Map<string, unknown>();
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const id = url.searchParams.get("id") ?? "test";
    if (url.pathname === "/result") return Response.json(this.results.get(id) ?? null);
    const upstream = await fetch(`http://127.0.0.1:${url.searchParams.get("backend")}/`, {
      headers: { upgrade: "websocket" },
    });
    const middle = new WebSocketPair();
    this.ctx.waitUntil(bridgeContainerWebSocket(middle[1], upstream.webSocket!, () => {},
      result => this.results.set(id, result)));
    return new Response(null, { status: 101, webSocket: middle[0], headers: upstream.headers });
  }
}
