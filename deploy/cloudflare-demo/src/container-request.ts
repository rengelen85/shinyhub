// Only safe, bodyless HTTP requests can be replayed. Never retry an upgraded
// connection, a session POST, or an application write after an ambiguous failure.
export function mayRetryRequest(request: Request): boolean {
  return (request.method === "GET" || request.method === "HEAD")
    && request.body === null && request.headers.get("upgrade") === null;
}

export function isTransientContainerError(error: unknown): boolean {
  return error instanceof Error && (
    (error as Error & { retryable?: boolean }).retryable === true
    || /Network connection lost\.|Durable Object instance is no longer active|no container instance that can be provided/i.test(error.message)
  );
}

export async function containerRequest(
  request: Request,
  fetch: (request: Request) => Promise<Response>,
  recovered: () => void,
  wait: (signal: AbortSignal) => Promise<void> = retryDelay,
): Promise<Response> {
  request.signal.throwIfAborted();
  const retryable = mayRetryRequest(request);
  let first: Response | undefined;
  try {
    first = await fetch(request);
    if (!retryable || ![500, 502, 503, 504].includes(first.status)) return first;
  } catch (error) {
    if (!isTransientContainerError(error) || request.signal.aborted) throw error;
    if (!retryable) return unavailableResponse();
  }
  // Start a fresh bounded attempt, preserving client cancellation. A failed
  // retry must not expose an SDK exception or hold a navigation for another boot.
  const deadline = new AbortController();
  const signal = AbortSignal.any([request.signal, deadline.signal]);
  const timer = setTimeout(() => deadline.abort(new DOMException("Recovery timed out", "TimeoutError")), 5_000);
  try {
    await wait(signal);
    const response = await abortable(() => fetch(new Request(request, { signal })), signal,
      response => { void response.body?.cancel().catch(() => {}); });
    void first?.body?.cancel().catch(() => {});
    if (response.status < 500) recovered();
    return response;
  } catch (error) {
    if (request.signal.aborted) {
      void first?.body?.cancel().catch(() => {});
      throw error;
    }
    // Preserve an actual application response if recovery could not complete.
    if (first) return first;
    if (!signal.aborted && !isTransientContainerError(error)) throw error;
    return unavailableResponse();
  } finally {
    // Keep successful response streams alive beyond the recovery deadline.
    clearTimeout(timer);
  }
}

function retryDelay(signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    signal.throwIfAborted();
    const abort = () => { clearTimeout(timer); reject(signal.reason); };
    const timer = setTimeout(() => { signal.removeEventListener("abort", abort); resolve(); }, 250);
    signal.addEventListener("abort", abort, { once: true });
  });
}

export function unavailableResponse(): Response {
  return new Response("The demo is temporarily unavailable. Please try again shortly.\n", {
    status: 503,
    headers: { "cache-control": "no-store", "retry-after": "2", "content-type": "text/plain; charset=utf-8" },
  });
}

export async function readContainerState<T>(read: () => Promise<T>, signal: AbortSignal): Promise<T> {
  const controller = new AbortController();
  const deadline = AbortSignal.any([signal, controller.signal]);
  const timer = setTimeout(() => controller.abort(new DOMException("State read timed out", "TimeoutError")), 5_000);
  try {
    try { return await abortable(read, deadline); }
    catch (error) {
      if (deadline.aborted || !isTransientContainerError(error)) throw error;
      await retryDelay(deadline);
      return await abortable(read, deadline);
    }
  } finally { clearTimeout(timer); }
}

function abortable<T>(operation: () => Promise<T>, signal: AbortSignal, discard?: (value: T) => void): Promise<T> {
  return new Promise((resolve, reject) => {
    signal.throwIfAborted();
    const abort = () => reject(signal.reason);
    signal.addEventListener("abort", abort, { once: true });
    Promise.resolve().then(operation).then(value => {
      signal.removeEventListener("abort", abort);
      if (signal.aborted) discard?.(value);
      else resolve(value);
    }, error => {
      signal.removeEventListener("abort", abort);
      reject(error);
    });
  });
}
