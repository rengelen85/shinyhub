import { bridgeContainerWebSocket } from "../src/container-websocket.ts";
const results = new Map<string, unknown>();
export default {
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const id = url.searchParams.get("id") ?? "test";
    if (url.pathname === "/result") return Response.json(results.get(id) ?? null);
    const backend = new WebSocketPair();
    backend[1].binaryType = "arraybuffer";
    backend[1].accept({ allowHalfOpen: true });
    backend[1].addEventListener("message", event => {
      if (event.data === "close") backend[1].close(1000, "backend done");
      else backend[1].send(event.data);
    });
    backend[1].addEventListener("close", event => backend[1].close(event.code, event.reason));
    const client = new WebSocketPair();
    const middle = new WebSocketPair();
    bridgeContainerWebSocket(middle[1], backend[0], () => {}, result => results.set(`${id}-inner`, result));
    bridgeContainerWebSocket(client[1], middle[0], () => {}, result => results.set(id, result));
    return new Response(null, { status: 101, webSocket: client[0] });
  },
};
