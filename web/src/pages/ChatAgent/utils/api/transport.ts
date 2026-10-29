/**
 * SSE/stream transport plumbing: auth headers for raw fetch, the shared
 * streamFetch/postSSEStream readers, and Content-Location parsers.
 * Package-internal — the barrel does not re-export streamFetch/postSSEStream.
 */
import { api } from '@/api/client';
import { getAuthHeaders } from '@/lib/authToken';
import { holdReload } from '@/lib/reloadHolds';

export const baseURL = api.defaults.baseURL;

/**
 * Bearer headers for this package's raw fetch() calls (SSE streams).
 *
 * Re-exported, not reimplemented: the pre-expiry margin and the one shared
 * refresh live in `lib/authToken`, so a reconnect loop calling this per attempt
 * costs no extra refreshes and no caller can grow a private margin again.
 */
export { getAuthHeaders };

/**
 * Parse a `run_id` query parameter out of a backend `Content-Location` header
 * value such as `/api/v1/threads/{tid}/messages/stream?run_id={uuid}`.
 * Returns `null` when the value is missing or has no `run_id` param.
 */
export function parseRunIdFromContentLocation(
  contentLocation: string | null | undefined,
): string | null {
  if (!contentLocation) return null;
  const qIdx = contentLocation.indexOf('?');
  if (qIdx === -1) return null;
  try {
    const params = new URLSearchParams(contentLocation.slice(qIdx + 1));
    const runId = params.get('run_id');
    return runId && runId.length > 0 ? runId : null;
  } catch {
    return null;
  }
}

/**
 * Parse the `{tid}` path segment out of a backend `Content-Location` header
 * value such as `/api/v1/threads/{tid}/messages/stream?run_id={uuid}`.
 * Lets a new-thread send latch the server-assigned thread id from the response
 * headers — before the first SSE event — so an early stop can still cancel it.
 * Returns `null` when the value is missing or doesn't match the expected shape.
 */
export function parseThreadIdFromContentLocation(
  contentLocation: string | null | undefined,
): string | null {
  if (!contentLocation) return null;
  const match = contentLocation.match(/\/threads\/([^/?]+)\//);
  const tid = match?.[1];
  if (!tid || tid.length === 0) return null;
  try {
    return decodeURIComponent(tid);
  } catch {
    // Malformed percent-encoding (e.g. "%ZZ") throws URIError. The contract is
    // non-throwing/return-null for unusable input — a bad id can't be latched.
    return null;
  }
}

export async function streamFetch(
  url: string,
  opts: RequestInit,
  onEvent: (event: Record<string, unknown>) => void,
  onHeaders?: (contentLocation: string | null) => void,
): Promise<{ disconnected: boolean; aborted: boolean; contentLocation: string | null }> {
  let res: Response;
  try {
    res = await fetch(`${baseURL}${url}`, opts);
  } catch (error: unknown) {
    // An AbortController.abort() during the initial fetch (e.g. the user hit
    // stop before the response headers arrived) surfaces as AbortError (a
    // DOMException, which is not always an Error instance — match on name).
    // Treat it as an intentional stop rather than a network failure so callers
    // don't show an error toast or run double cleanup.
    if ((error as { name?: string })?.name === 'AbortError') {
      return { disconnected: false, aborted: true, contentLocation: null };
    }
    throw error;
  }
  // Snapshot Content-Location before body errors so callers can recover the
  // canonical reconnect URL (carries ?run_id=…) even when a 4xx aborts later.
  const contentLocation = res.headers.get('Content-Location');
  // Notify the caller of headers IMMEDIATELY — well before any SSE body byte —
  // so the run_id can be latched before the first `metadata` event arrives.
  // Closes the reconnect race window between "clear stale run_id" and "new
  // turn's first metadata frame" (see useChatMessages.resumeWithHitlResponse).
  if (onHeaders) {
    try {
      onHeaders(contentLocation);
    } catch (e) {
      console.warn('[api] onHeaders callback threw', e);
    }
  }
  if (!res.ok) {
    // Handle 429 (rate limit) with structured detail
    if (res.status === 429) {
      let detail: Record<string, unknown> = {};
      try { detail = await res.json(); } catch { /* ignore */ }
      const err: Error & { status?: number; rateLimitInfo?: Record<string, unknown>; retryAfter?: number | null } =
        new Error((detail?.detail as Record<string, unknown>)?.message as string || 'Rate limit exceeded');
      err.status = 429;
      err.rateLimitInfo = (detail?.detail as Record<string, unknown>) || {};
      err.retryAfter = parseInt(res.headers.get('Retry-After') as string, 10) || null;
      throw err;
    }
    // Handle 413 (payload too large) with user-friendly message
    if (res.status === 413) {
      const err: Error & { status?: number } = new Error('Files too large. Try smaller files or fewer attachments.');
      err.status = 413;
      throw err;
    }
    // Handle 404 specifically for history replay (expected for new threads)
    if (res.status === 404 && url.includes('/replay')) {
      throw new Error(`HTTP error! status: ${res.status}`);
    }
    // Read response body for error detail
    let detail = '';
    let errorInfo: Record<string, unknown> | null = null;
    const text = await res.text().catch(() => '');
    try {
      const body = JSON.parse(text);
      if (body?.detail && typeof body.detail === 'object' && 'message' in body.detail) {
        // Structured error detail (e.g., { message, type, link })
        errorInfo = body.detail as Record<string, unknown>;
        detail = (errorInfo.message as string) || '';
      } else {
        detail = typeof body?.detail === 'string' ? body.detail : JSON.stringify(body?.detail || body);
      }
    } catch { /* ignore parse errors */ }
    console.error(`[api] ${opts.method || 'GET'} ${url} failed:`, res.status, detail);
    const err: Error & { status?: number; errorInfo?: Record<string, unknown> } =
      new Error(detail || `HTTP error! status: ${res.status}`);
    err.status = res.status;
    if (errorInfo) err.errorInfo = errorInfo;
    throw err;
  }

  const reader = res.body!.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  let ev: { id?: string; event?: string } = {};
  const processLine = (line: string) => {
    if (line.startsWith('id: ')) ev.id = line.slice(4).trim();
    else if (line.startsWith('event: ')) ev.event = line.slice(7).trim();
    else if (line.startsWith('data: ')) {
      try {
        const d = JSON.parse(line.slice(6));
        if (ev.event) d.event = ev.event;
        if (ev.id != null) d._eventId = parseInt(ev.id, 10) || ev.id;
        onEvent(d);
      } catch (e: unknown) {
        console.warn('[api] SSE parse error', e, line);
      }
      ev = {};
    } else if (line.trim() === '') ev = {};
  };

  let disconnected = false;
  let aborted = false;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop() || '';
      lines.forEach(processLine);
    }
    // Process any remaining buffer
    buffer.split('\n').forEach(processLine);
  } catch (error: unknown) {
    // An AbortController.abort() on the reader (the user hit stop) surfaces as
    // AbortError (a DOMException — match on name, not instanceof Error). This
    // is an INTENTIONAL stop, not a failure — return an aborted marker so
    // callers skip reconnect/error-toast/double-cleanup.
    if ((error as { name?: string })?.name === 'AbortError') {
      aborted = true;
    } else if (error instanceof Error && error.name === 'TypeError') {
      // iOS Safari freezes a backgrounded tab and tears down its connection,
      // rejecting reader.read() with "Load failed" / "The network connection was
      // lost." — neither reliably contains "network", so the old substring guard
      // re-threw it and surfaced a dead-end error banner with no reconnect. Per
      // the Streams/Fetch spec, reader.read() only rejects with a TypeError on a
      // transport-level network error; the loop body (decode/split/processLine,
      // which guards its own JSON.parse) won't otherwise throw one. So treat any
      // TypeError here as a dropped stream and route it into the reconnect path.
      console.warn('[api] Stream interrupted (transport error):', error.message);
      disconnected = true;
    } else {
      throw error;
    }
  }
  return { disconnected, aborted, contentLocation };
}

/**
 * Shared POST→SSE plumbing for the three send paths (new/continue, retry, HITL
 * resume): attaches auth headers, streams the response, and latches the run/thread
 * id out of the `Content-Location` header before the first SSE event. The three
 * callers differ only in path + body.
 */
export async function postSSEStream(
  path: string,
  body: unknown,
  opts: {
    onEvent: (event: Record<string, unknown>) => void;
    onRunIdResolved?: ((runId: string, threadId: string | null) => void) | null;
    signal?: AbortSignal | null;
  },
): Promise<{ disconnected: boolean; aborted: boolean; contentLocation: string | null }> {
  const { onRunIdResolved, signal } = opts;
  // A send holds the stale-build reload until the server has the message, a
  // token refresh before it included; from its headers on, the turn runs there
  // and a reload reattaches to it.
  const release = holdReload();
  try {
    const authHeaders = await getAuthHeaders();
    // A retransmit that raced past the route's duplicate check is refused in
    // the stream, by the same answer the route gives as a 409. It reaches the
    // caller as that 409, so one recovery handles both: the stream's turn is
    // the copy that arrived, not a failure of this one.
    let duplicate: Record<string, unknown> | null = null;
    const onEvent = (event: Record<string, unknown>) => {
      if (event.event === 'error' && event.code === 'duplicate_request') {
        duplicate = event;
        return;
      }
      opts.onEvent(event);
    };
    const result = await streamFetch(
      path,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'text/event-stream',
          ...authHeaders,
        },
        body: JSON.stringify(body),
        ...(signal ? { signal } : {}),
      },
      onEvent,
      (contentLocation) => {
        release();
        const runId = parseRunIdFromContentLocation(contentLocation);
        if (runId && onRunIdResolved) onRunIdResolved(runId, parseThreadIdFromContentLocation(contentLocation));
      },
    );
    if (duplicate && !result.aborted) throw duplicateRequestError(duplicate);
    return result;
  } finally {
    release();
  }
}

/** The route's 409 for a frame carrying the same refusal. The frame names the
 * copy's thread as `run_thread_id`, which a first message that minted a second
 * thread before START refused it tells apart from the thread it streamed on. */
function duplicateRequestError(frame: Record<string, unknown>): Error & { status: number; errorInfo: Record<string, unknown> } {
  const message = typeof frame.error === 'string' ? frame.error : '';
  const errorInfo: Record<string, unknown> = { code: 'duplicate_request', message };
  if (typeof frame.run_id === 'string' && frame.run_id) {
    errorInfo.run_id = frame.run_id;
    errorInfo.run_status = frame.run_status;
    errorInfo.thread_id = frame.run_thread_id || frame.thread_id;
    if (frame.steered === true) errorInfo.steered = true;
  }
  return Object.assign(new Error(message || 'HTTP error! status: 409'), { status: 409, errorInfo });
}
