export type WebSocketControl = Pick<WebSocket, "accept" | "addEventListener"> & { dispose?(): void };

// Use DO event delivery for inbound sockets. WebSocket.accept() can report an
// uncaught "Network connection lost" even after a successful close handshake:
// https://github.com/cloudflare/workerd/issues/5290
// The accepted outbound socket keeps this object awake while the relay exists.
export class DurableWebSocketClients {
  private events = new Map<WebSocket, EventTarget>();
  private state: Pick<DurableObjectState, "acceptWebSocket">;
  constructor(state: Pick<DurableObjectState, "acceptWebSocket">) { this.state = state; }

  control(socket: WebSocket): WebSocketControl {
    const events = new EventTarget();
    this.events.set(socket, events);
    return {
      accept: () => this.state.acceptWebSocket(socket),
      addEventListener: events.addEventListener.bind(events) as WebSocket["addEventListener"],
      dispose: () => { this.events.delete(socket); },
    };
  }

  message(socket: WebSocket, data: string | ArrayBuffer): void {
    this.deliver(socket, new MessageEvent("message", { data }));
  }
  close(socket: WebSocket, code: number, reason: string, wasClean: boolean): void {
    this.deliver(socket, new CloseEvent("close", { code, reason, wasClean }));
  }
  error(socket: WebSocket): void { this.deliver(socket, new Event("error")); }

  private deliver(socket: WebSocket, event: Event): void {
    const events = this.events.get(socket);
    if (events) events.dispatchEvent(event);
    else {
      // A reset cannot restore the outbound connection. Fail explicitly rather
      // than silently keeping a client attached to a relay that no longer exists.
      socket.close(1011, "WebSocket relay unavailable");
    }
  }
}
