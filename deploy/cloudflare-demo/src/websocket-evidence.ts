export const smokeWebSocketPaths = [
  "/app/operations-dashboard/websocket/",
  "/app/bookmarking-demo/websocket/",
  "/app/streamlit-demo/_stcore/stream",
];

export function validSmokeID(id: string | null): id is string {
  return id !== null && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(id);
}

export interface WebSocketCloseEvidence {
  event: "demo_websocket_closed" | "demo_websocket_failed";
  smoke_id: string;
  smoke_path: string;
  connection_id: string;
  client_code: number;
  backend_code: number;
}

// Synthetic records only, bounded across runs. No visitor URLs, payloads,
// cookies, or close reasons are retained or exposed through this internal RPC.
export class SmokeWebSocketEvidence {
  private records: WebSocketCloseEvidence[] = [];
  record(id: string | null, path: string, connectionID: string, result: { clean: boolean; codes: number[] }): void {
    if (!validSmokeID(id) || !smokeWebSocketPaths.includes(path)) return;
    this.records.push({
      event: result.clean ? "demo_websocket_closed" : "demo_websocket_failed",
      smoke_id: id, smoke_path: path, connection_id: connectionID,
      client_code: result.codes[0], backend_code: result.codes[1],
    });
    if (this.records.length > 64) this.records.shift();
  }
  read(id: string): WebSocketCloseEvidence[] {
    return validSmokeID(id) ? this.records.filter(record => record.smoke_id === id) : [];
  }
}
