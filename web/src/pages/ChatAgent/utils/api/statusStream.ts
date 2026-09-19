/**
 * Reader for the backend's lifecycle status streams.
 *
 * One reader, because the workspace channel and the computer channel are one
 * generator on the server (`app/status_stream.py`): same frame names, same
 * 30 s keepalive, same 600 s cap, same terminal set. Two hand-rolled parsers
 * would be two places for a frame shape that changes in one.
 */
import { baseURL, getAuthHeaders } from './transport';

/**
 * A `status` frame. `status` is the only field both channels always send; the
 * rest are present when the server knows them, so every reader treats an
 * absent field as "not told", never as a value.
 */
export interface StatusFrame {
  status: string;
  /** Provider-level refinement (e.g. `archived`) driving the slow-restore copy. */
  sandbox_state?: string;
  /** The machine. Null on the workspace channel when the row is unbound. */
  computer_id?: string;
  workspace_id?: string;
}

function readString(value: unknown): string | undefined {
  return typeof value === 'string' ? value : undefined;
}

/**
 * Subscribe to an SSE status stream and invoke `onFrame` per `status` event.
 * Resolves when the stream closes (terminal status, the server's `timeout`
 * event, or an abort). Best-effort by contract: network errors resolve without
 * throwing, so no caller needs a defensive wrapper.
 */
export async function streamStatusEvents(
  path: string,
  onFrame: (frame: StatusFrame) => void,
  signal: AbortSignal,
): Promise<void> {
  const authHeaders = await getAuthHeaders();
  let res: Response;
  try {
    res = await fetch(`${baseURL}${path}`, {
      method: 'GET',
      headers: { ...authHeaders, Accept: 'text/event-stream' },
      signal,
    });
  } catch {
    return; // network error or aborted, and the caller wants best-effort
  }
  if (!res.ok || !res.body) return;

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) return;
      buffer += decoder.decode(value, { stream: true });
      const chunks = buffer.split('\n\n');
      buffer = chunks.pop() ?? '';
      for (const chunk of chunks) {
        let eventType = '';
        const dataLines: string[] = [];
        for (const raw of chunk.split('\n')) {
          if (raw.startsWith('event:')) eventType = raw.slice(6).trim();
          else if (raw.startsWith('data:')) dataLines.push(raw.slice(5).trim());
          // Comments (lines starting with ':') and unknown fields ignored.
        }
        // Per the SSE spec, multiple data: lines join with a newline. The
        // backend emits single-line json.dumps payloads, so this is one line in
        // practice, but joining correctly keeps a multi-line payload parseable
        // instead of silently corrupting the JSON.
        const data = dataLines.join('\n');
        if (eventType === 'status' && data) {
          try {
            const parsed = JSON.parse(data) as Record<string, unknown>;
            const status = readString(parsed.status);
            if (status) {
              onFrame({
                status,
                sandbox_state: readString(parsed.sandbox_state),
                computer_id: readString(parsed.computer_id),
                workspace_id: readString(parsed.workspace_id),
              });
            }
          } catch { /* ignore malformed payload */ }
        } else if (eventType === 'timeout') {
          return;
        }
      }
    }
  } catch (err) {
    if ((err as { name?: string })?.name === 'AbortError') return;
    // Best-effort, so drop everything else.
  } finally {
    // Deterministically release the stream so repeated navigation doesn't
    // retain fetch/body resources until browser GC. cancel() also releases the
    // lock; both are no-ops if the stream already closed.
    try {
      await reader.cancel();
    } catch {
      /* already closed / aborted */
    }
  }
}
