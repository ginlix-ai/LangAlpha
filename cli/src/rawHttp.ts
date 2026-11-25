// Requests that carry or return file bytes go through node:http, not fetch.
// fetch copies a Buffer body into its own memory for every request and gives
// the copy back late (a 2 GB upload at four 16 MiB parts held 380 MB of
// buffers), and it reads a response through more layers than a file needs.
import http from "node:http";
import https from "node:https";
import { Readable } from "node:stream";

export interface RawInit {
  method: string;
  headers: Record<string, string>;
  body?: Uint8Array;
  /** How long the connection may sit with no byte moving either way. */
  idleMs?: number;
  /** How long a lookup and connect may take before any byte can move. */
  connectMs?: number;
}

/** A part on a slow link keeps going while it moves; one the far end stopped answering does not hold its turn. */
export const IDLE_MS = 60_000;
/** Longer than a healthy lookup and handshake, far short of the system's own connect wait. */
export const CONNECT_MS = 30_000;

/** The response as it starts, its body still unread. node:http has no deadline
 *  of its own, so a store that takes the connection and then goes silent would
 *  hold it forever: once no byte has moved for ``idleMs``, the request and its
 *  response end with an error, which the callers' retries and their switch to
 *  the relay take as a store that cannot be reached. The inactivity timer
 *  starts only once a socket is connected, so a lookup or connect that stalls
 *  has a deadline of its own. */
export function rawRequest(url: URL, init: RawInit): Promise<http.IncomingMessage> {
  const headers = { ...init.headers };
  if (init.body) headers["content-length"] = String(init.body.length);
  const idleMs = init.idleMs ?? IDLE_MS;
  return new Promise((resolve, reject) => {
    const send = url.protocol === "https:" ? https.request : http.request;
    const req = send(url, { method: init.method, headers }, resolve);
    req.setTimeout(idleMs, () => req.destroy(new Error(`No bytes moved for ${idleMs} ms`)));
    const connectMs = init.connectMs ?? CONNECT_MS;
    req.on("socket", (socket) => {
      if (!socket.connecting) return;
      const timer = setTimeout(() => req.destroy(new Error(`No connection within ${connectMs} ms`)), connectMs);
      socket.once("connect", () => clearTimeout(timer));
      req.once("close", () => clearTimeout(timer));
    });
    req.on("error", reject);
    req.end(init.body);
  });
}

const NO_BODY = new Set([101, 204, 205, 304]);

/** ``rawRequest`` answered as a web Response, for callers that read one. */
export async function rawResponse(url: URL, init: RawInit): Promise<Response> {
  const res = await rawRequest(url, init);
  const headers = new Headers();
  for (const [k, v] of Object.entries(res.headers)) {
    for (const one of Array.isArray(v) ? v : v === undefined ? [] : [v]) headers.append(k, one);
  }
  const status = res.statusCode ?? 502;
  if (NO_BODY.has(status)) {
    res.resume();
    return new Response(null, { status, headers });
  }
  return new Response(Readable.toWeb(res) as ReadableStream, { status, statusText: res.statusMessage, headers });
}
