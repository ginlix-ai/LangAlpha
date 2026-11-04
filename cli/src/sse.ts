export interface SseFrame {
  event: string;
  data: Record<string, unknown>;
}

/** Frames off a ``text/event-stream`` body. A frame whose data is not a JSON
 *  object is skipped: every frame this server sends is one. */
export async function* frames(body: ReadableStream<Uint8Array>): AsyncGenerator<SseFrame> {
  const decoder = new TextDecoder();
  let buffer = "";
  let event = "message";
  let data: string[] = [];
  const reader = body.getReader();
  try {
    for (;;) {
      const { value, done } = await reader.read();
      buffer += done ? decoder.decode() : decoder.decode(value, { stream: true });
      let nl: number;
      while ((nl = buffer.search(/\r?\n/)) !== -1) {
        const line = buffer.slice(0, nl);
        buffer = buffer.slice(nl + (buffer[nl] === "\r" ? 2 : 1));
        if (line === "") {
          if (data.length > 0) {
            const frame = decode(event, data.join("\n"));
            if (frame) yield frame;
          }
          event = "message";
          data = [];
          continue;
        }
        if (line.startsWith(":")) continue;
        const colon = line.indexOf(":");
        const field = colon === -1 ? line : line.slice(0, colon);
        const raw = colon === -1 ? "" : line.slice(colon + 1).replace(/^ /, "");
        if (field === "event") event = raw;
        else if (field === "data") data.push(raw);
      }
      if (done) break;
    }
  } finally {
    reader.releaseLock();
  }
}

function decode(event: string, raw: string): SseFrame | undefined {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return undefined;
  }
  return parsed && typeof parsed === "object" && !Array.isArray(parsed)
    ? { event, data: parsed as Record<string, unknown> }
    : undefined;
}
