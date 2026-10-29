/**
 * Shared API client for backend REST calls.
 *
 * The Bearer token comes from `lib/authToken`, the app's single token cache,
 * never from a session read per request, which is what once turned a page load
 * into a refresh storm. In OSS mode there is no Supabase client and both calls
 * resolve null, so no header is set.
 *
 * A thin `fetch` wrapper that keeps the shape axios gave its callers: a
 * `{ data, status, headers }` response, and on a refusal an error carrying
 * `response`. The error readers across the app were written against that
 * shape, so it is the contract here rather than an accident of the old client.
 */
import { authGeneration, bearerTokenOf, getAccessToken, refreshAccessToken } from '../lib/authToken';
import { trackRequest } from '../lib/reloadHolds';

const baseURL: string = import.meta.env.VITE_API_BASE_URL ?? '';

/**
 * Scalars only. A list goes as a `URLSearchParams` with the key repeated,
 * which is what the backend's list parameters read.
 */
export type QueryParams = Record<string, string | number | boolean | null | undefined> | URLSearchParams;

export interface TransferProgress {
  loaded: number;
  /** Absent when the length is unknown. */
  total?: number;
}

export interface RequestConfig {
  params?: QueryParams;
  headers?: Record<string, string>;
  signal?: AbortSignal;
  /** Milliseconds. Unset means no bound. */
  timeout?: number;
  /**
   * Milliseconds the answer may go without a byte, the wait for its headers
   * included, before the call fails as `timeout` does. A slow answer that keeps
   * arriving never trips it. The XHR path a progress callback takes ignores it.
   */
  idleTimeout?: number;
  /**
   * Read the body only when the answer has this status. Any other success
   * comes back with no body, its stream cancelled, so an answer far larger
   * than the one asked for is never downloaded. The XHR path ignores it too.
   */
  expectStatus?: number;
  responseType?: 'json' | 'text' | 'blob' | 'arraybuffer';
  /** A DELETE body, which has no positional argument. */
  data?: unknown;
  /** fetch's cache mode. The XHR a progress callback moves the call onto has none, and ignores it. */
  cache?: RequestCache;
  /** Either progress callback sends the request over XHR: fetch cannot report upload progress. */
  onUploadProgress?: (event: TransferProgress) => void;
  onDownloadProgress?: (event: TransferProgress) => void;
}

// An untyped call reads fields straight off `data`, as every caller did under axios.
type Untyped = any;

export interface ApiResponse<T = Untyped> {
  data: T;
  status: number;
  statusText: string;
  /** Lower-cased names, so `headers['retry-after']` finds the header. */
  headers: Record<string, string>;
}

type ErrorCode = 'ERR_BAD_REQUEST' | 'ERR_BAD_RESPONSE' | 'ERR_NETWORK' | 'ECONNABORTED' | 'ERR_CANCELED';

/**
 * A failed call. Messages and codes match what axios produced, because call
 * sites show `err.message` and tell an abort apart by `name` ('CanceledError').
 */
export class ApiError extends Error {
  code: ErrorCode;
  declare status?: number;
  declare response?: ApiResponse<unknown>;
  declare rateLimitInfo?: Record<string, unknown>;
  declare retryAfter?: number | null;

  constructor(message: string, code: ErrorCode, response?: ApiResponse<unknown>) {
    const detail = (response?.data as { detail?: unknown } | null | undefined)?.detail;
    const body: { message?: unknown } = detail && typeof detail === 'object' ? detail : {};
    // Any gate that writes a structured `detail` wrote a sentence meant for the
    // user. Without this, every call site that falls back to `err.message` shows
    // "Request failed with status code 503" and the explanation the server took
    // care to send is dropped on the floor.
    super(typeof body.message === 'string' ? body.message : message);
    this.name = code === 'ERR_CANCELED' ? 'CanceledError' : 'ApiError';
    this.code = code;
    if (response) {
      this.response = response;
      this.status = response.status;
      if (response.status === 429) {
        this.rateLimitInfo = body;
        this.retryAfter = parseInt(response.headers['retry-after'], 10) || null;
      }
    }
  }
}

/** axios's query encoding, kept so the backend reads the same bytes it always has. */
const encode = (value: string) =>
  encodeURIComponent(value).replace(/%3A/gi, ':').replace(/%24/g, '$').replace(/%2C/gi, ',').replace(/%20/g, '+');

function buildUrl(url: string, params?: QueryParams): string {
  const full = baseURL && !/^([a-z][a-z\d+\-.]*:)?\/\//i.test(url)
    ? `${baseURL.replace(/\/+$/, '')}/${url.replace(/^\/+/, '')}`
    : url;
  const query = params instanceof URLSearchParams
    ? params.toString()
    : Object.entries(params ?? {})
      .flatMap(([key, value]) => (value == null ? [] : [`${encode(key)}=${encode(String(value))}`]))
      .join('&');
  return query ? `${full}${full.includes('?') ? '&' : '?'}${query}` : full;
}

/** The JSON a text body holds, or the text itself when it holds none. */
function parse(text: string): unknown {
  try {
    return text ? JSON.parse(text) : text;
  } catch {
    return text;
  }
}

/** What a transport hands back; `send` turns the header pairs into lower-cased names. */
interface Sent {
  data: unknown;
  status: number;
  statusText: string;
  rawHeaders: Iterable<[string, string]>;
}

/** A body read chunk by chunk, each one reported to `alive`. */
async function readChunks(res: Response, alive: () => void): Promise<Uint8Array<ArrayBuffer>> {
  const chunks: Uint8Array[] = [];
  let size = 0;
  const reader = res.body?.getReader();
  while (reader) {
    const { done, value } = await reader.read();
    if (done) break;
    alive();
    chunks.push(value);
    size += value.byteLength;
  }
  const out = new Uint8Array(size);
  let at = 0;
  for (const chunk of chunks) {
    out.set(chunk, at);
    at += chunk.byteLength;
  }
  return out;
}

async function viaFetch(
  method: string, url: string, headers: Record<string, string>, body: XMLHttpRequestBodyInit | undefined,
  config: RequestConfig, signal: AbortSignal | undefined, alive?: () => void,
): Promise<Sent> {
  const res = await fetch(url, { method, headers, body, signal, cache: config.cache });
  const head = { status: res.status, statusText: res.statusText, rawHeaders: res.headers };
  if (config.expectStatus !== undefined && res.ok && res.status !== config.expectStatus) {
    await res.body?.cancel();
    return { data: null, ...head };
  }
  alive?.();
  // Read in chunks only under an idle bound: `arrayBuffer()` and its kin say
  // nothing until the last byte, and every chunk has to put the bound off.
  const source = alive ? new Response(await readChunks(res, alive), { headers: res.headers }) : res;
  const type = config.responseType;
  const data = type === 'blob' ? await source.blob()
    : type === 'arraybuffer' ? await source.arrayBuffer()
    : type === 'text' ? await source.text()
    : parse(await source.text());
  return { data, ...head };
}

function viaXhr(
  method: string, url: string, headers: Record<string, string>, body: XMLHttpRequestBodyInit | undefined,
  config: RequestConfig, signal: AbortSignal | undefined,
): Promise<Sent> {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    const type = config.responseType;
    const report = (listener: (event: TransferProgress) => void) => (e: ProgressEvent) =>
      listener({ loaded: e.loaded, total: e.lengthComputable ? e.total : undefined });
    const abort = () => xhr.abort();
    xhr.open(method, url);
    for (const [name, value] of Object.entries(headers)) xhr.setRequestHeader(name, value);
    if (type && type !== 'json') xhr.responseType = type;
    if (config.onUploadProgress) xhr.upload.onprogress = report(config.onUploadProgress);
    if (config.onDownloadProgress) xhr.onprogress = report(config.onDownloadProgress);
    xhr.onloadend = () => {
      signal?.removeEventListener('abort', abort);
      // Status 0 is an abort, a timeout or a network failure; `send` tells them apart.
      if (!xhr.status) return reject(new Error('XHR failed'));
      const pairs: [string, string][] = [];
      for (const line of xhr.getAllResponseHeaders().trim().split(/[\r\n]+/)) {
        const colon = line.indexOf(':');
        if (colon > 0) pairs.push([line.slice(0, colon).trim().toLowerCase(), line.slice(colon + 1).trim()]);
      }
      resolve({
        data: type && type !== 'json' ? xhr.response : parse(xhr.responseText),
        status: xhr.status, statusText: xhr.statusText, rawHeaders: pairs,
      });
    };
    if (signal?.aborted) return reject(new Error('aborted'));
    signal?.addEventListener('abort', abort);
    xhr.send(body ?? null);
  });
}

async function send(
  method: string, url: string, headers: Record<string, string>, body: XMLHttpRequestBodyInit | undefined,
  config: RequestConfig,
): Promise<ApiResponse> {
  const { signal, timeout, idleTimeout } = config;
  // One controller carries both the caller's abort and the deadlines, so the
  // transport takes a single signal and nothing outlives the request.
  const ctrl = new AbortController();
  const onAbort = () => ctrl.abort();
  if (signal?.aborted) ctrl.abort();
  else signal?.addEventListener('abort', onAbort, { once: true });
  let expired: string | null = null;
  const expire = (message: string) => () => {
    expired = message;
    ctrl.abort();
  };
  const timer = timeout ? setTimeout(expire(`timeout of ${timeout}ms exceeded`), timeout) : undefined;
  const xhr = !!(config.onUploadProgress || config.onDownloadProgress);
  let idleTimer: ReturnType<typeof setTimeout> | undefined;
  const alive = idleTimeout && !xhr
    ? () => {
      clearTimeout(idleTimer);
      idleTimer = setTimeout(expire(`timeout of ${idleTimeout}ms exceeded`), idleTimeout);
    }
    : undefined;
  alive?.();
  let sent: Sent;
  try {
    sent = xhr
      ? await viaXhr(method, url, headers, body, config, ctrl.signal)
      : await viaFetch(method, url, headers, body, config, ctrl.signal, alive);
  } catch {
    if (signal?.aborted) throw new ApiError('canceled', 'ERR_CANCELED');
    if (expired) throw new ApiError(expired, 'ECONNABORTED');
    throw new ApiError('Network Error', 'ERR_NETWORK');
  } finally {
    clearTimeout(timer);
    clearTimeout(idleTimer);
    signal?.removeEventListener('abort', onAbort);
  }
  // A caller that aborted while the body was arriving asked for no answer.
  if (signal?.aborted) throw new ApiError('canceled', 'ERR_CANCELED');
  const { rawHeaders, ...rest } = sent;
  const response: ApiResponse = { ...rest, headers: Object.fromEntries(rawHeaders) };
  const { status } = response;
  if (status >= 200 && status < 300) return response;
  throw new ApiError(
    `Request failed with status code ${status}`,
    status >= 400 && status < 500 ? 'ERR_BAD_REQUEST' : 'ERR_BAD_RESPONSE',
    response,
  );
}

async function request<T>(method: string, url: string, data: unknown, config: RequestConfig = {}): Promise<ApiResponse<T>> {
  const headers: Record<string, string> = {
    Accept: 'application/json, text/plain, */*',
    'Content-Type': 'application/json',
    ...config.headers,
  };
  let body: XMLHttpRequestBodyInit | undefined;
  if (data === undefined || data instanceof FormData) {
    // No body, or a multipart one whose boundary only the browser can write.
    delete headers['Content-Type'];
    body = data;
  } else {
    body = data instanceof Blob ? data : JSON.stringify(data);
  }
  const target = buildUrl(url, config.params);
  let retried = false;

  const attempt = async (): Promise<ApiResponse<T>> => {
    // Which user the request goes out as, so the 401 replay below can tell
    // that it is still them.
    let sentAs: number | undefined;
    try {
      const token = await getAccessToken();
      if (token) {
        headers.Authorization = `Bearer ${token}`;
        sentAs = authGeneration();
      }
    } catch {
      /* proceed without auth */
    }

    try {
      return await send(method, target, headers, body, config);
    } catch (e) {
      // iOS Safari returns from a frozen tab with a stale token before Supabase's
      // auto-refresh runs, so a refetch hits a 401. Force-refresh once and replay.
      if (!(e instanceof ApiError) || e.status !== 401 || retried) throw e;
      // A 401 can outlive the account it was issued for: the reply arrives
      // after a sign-out or an account switch, and by then the cache holds
      // somebody else's token. `refreshAccessToken` would hand that one over,
      // because all it is asked is whether the cache moved on from the refused
      // token, and it has. Replaying then sends a request built for the
      // previous user as the current one. Their own 401 is the right answer.
      //
      // Asked twice, because the rotation between the two is a network round
      // trip and the switch can land inside it. The request still carries the
      // previous user's URL and body, so a replay stamped with the new user's
      // token is their mutation written to the new account.
      const sameAccount = () => sentAs === undefined || sentAs === authGeneration();
      if (!sameAccount()) throw e;
      retried = true;
      let token: string | null = null;
      try {
        // The token THIS request carried, not whichever one the cache holds by
        // now: a burst of 401s arrives one at a time, and the later ones are
        // being answered after an earlier one already rotated.
        token = await refreshAccessToken(bearerTokenOf(headers.Authorization));
      } catch {
        /* refresh failed: reject with the original error */
      }
      if (!token || !sameAccount()) throw e;
      headers.Authorization = `Bearer ${token}`;
      return attempt();
    }
  };
  // A write the page being left still has out (an archive, a save) outlives
  // it, so the stale-build reload a section switch takes waits for it to land.
  const settle = trackRequest(authGeneration());
  try {
    return await attempt();
  } finally {
    settle();
  }
}

export const api = {
  defaults: { baseURL },
  get: <T = Untyped>(url: string, config?: RequestConfig) => request<T>('GET', url, config?.data, config),
  delete: <T = Untyped>(url: string, config?: RequestConfig) => request<T>('DELETE', url, config?.data, config),
  post: <T = Untyped>(url: string, data?: unknown, config?: RequestConfig) => request<T>('POST', url, data, config),
  put: <T = Untyped>(url: string, data?: unknown, config?: RequestConfig) => request<T>('PUT', url, data, config),
  patch: <T = Untyped>(url: string, data?: unknown, config?: RequestConfig) => request<T>('PATCH', url, data, config),
  /** The URL a GET would request, for handing to the browser directly. */
  getUri: ({ url, params }: { url: string; params?: QueryParams }) => buildUrl(url, params),
  /**
   * A PUT to a URL the backend signed, such as one part of an upload to object
   * storage. None of the app's headers go with it: the signature says what the
   * request may carry, and a bearer token has no business on another origin.
   */
  putSigned: (url: string, body: Blob, config: RequestConfig = {}) => send('PUT', url, {}, body, config),
};
