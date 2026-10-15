// A proxy must complete both close handshakes before acknowledging the client.
// Otherwise the client can report a clean close while the backend is torn down.
export function bridgeContainerWebSocket(
  client: WebSocket,
  backend: WebSocket,
  activity: () => void,
  completed: (result: { clean: boolean; codes: number[] }) => void,
  closeTimeoutMS = 5_000,
): void {
  const sockets = [client, backend];
  const closed = [false, false];
  const forwarded = [false, false];
  const codes = [1006, 1006];
  let clean = true;
  let finished = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const queues = [Promise.resolve(), Promise.resolve()];
  const finish = () => {
    if (finished) return;
    finished = true;
    if (timer !== undefined) clearTimeout(timer);
    completed({ clean, codes });
  };
  const fail = () => {
    if (finished) return;
    clean = false;
    for (const socket of sockets) {
      try { socket.close(1011, "WebSocket transport error"); } catch { /* Already disconnected. */ }
    }
    finish();
  };
  sockets.forEach((socket, index) => {
    const peer = sockets[1 - index];
    socket.binaryType = "arraybuffer";
    socket.addEventListener("message", (event) => {
      if (finished || closed.some(Boolean)) return;
      activity();
      queues[index] = queues[index].then(async () => {
        if (finished) return;
        const data = event.data instanceof Blob ? await event.data.arrayBuffer() : event.data;
        peer.send(data);
      }).catch(fail);
    });
    socket.addEventListener("close", (event) => {
      if (finished || closed[index]) return;
      closed[index] = true;
      codes[index] = event.code;
      clean &&= event.wasClean && event.code !== 1006 && event.code !== 1011;
      queues[index] = queues[index].then(() => {
        if (finished) return;
        const code = event.code === 1005 ? 1000 : event.code === 1006 ? 1011 : event.code;
        peer.close(code, event.reason);
        forwarded[index] = true;
        if (forwarded.every(Boolean)) finish();
        else timer ??= setTimeout(fail, closeTimeoutMS);
      }).catch(fail);
    });
    socket.addEventListener("error", fail);
  });
  try {
    client.accept({ allowHalfOpen: true });
    backend.accept({ allowHalfOpen: true });
  } catch (error) {
    fail();
    throw error;
  }
}
