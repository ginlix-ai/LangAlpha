import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// The client reads straight from the shared token cache. Mocking it here is the
// injection seam: there is no registry to hand a getter to any more, because a
// per-request session read is exactly what caused issue #379.
const mockGetAccessToken = vi.fn<() => Promise<string | null>>();
const mockRefreshAccessToken = vi.fn<(refused: string | null) => Promise<string | null>>();
const mockAuthGeneration = vi.fn<() => number>(() => 0);

vi.mock('../../lib/authToken', async (importActual) => ({
  getAccessToken: () => mockGetAccessToken(),
  // Forwarded, not swallowed: which token the retry rotates against is the
  // decision this seam exists to observe.
  refreshAccessToken: (refused: string | null) => mockRefreshAccessToken(refused),
  // The real one. It is a pure string function, and stubbing it would hide the
  // header parsing that decides what `refused` even is.
  bearerTokenOf: (await importActual<typeof import('../../lib/authToken')>()).bearerTokenOf,
  // Also forwarded: which user a request went out as is the other half of the
  // retry decision, and a stub returning a constant would make the fence below
  // pass without ever being exercised.
  authGeneration: () => mockAuthGeneration(),
  getAuthHeaders: vi.fn(),
  publishSession: vi.fn(),
  clearAuthToken: vi.fn(),
}));

import { api, ApiError } from '../client';

type Reply = { status?: number; body?: BodyInit | null; headers?: Record<string, string> };

const fetchMock = vi.fn<(url: string, init: RequestInit) => Promise<Response>>();

/** Answer the next fetches in order, the last one repeating. */
function reply(...replies: Reply[]) {
  let i = 0;
  fetchMock.mockImplementation(async (_url, init) => {
    if (init.signal?.aborted) throw new DOMException('aborted', 'AbortError');
    const r = replies[Math.min(i++, replies.length - 1)];
    return new Response(r.body ?? null, { status: r.status ?? 200, headers: r.headers });
  });
}

const json = (status: number, body: unknown, headers: Record<string, string> = {}): Reply => ({
  status,
  body: JSON.stringify(body),
  headers: { 'content-type': 'application/json', ...headers },
});

async function failure(call: Promise<unknown>): Promise<ApiError> {
  try {
    await call;
  } catch (e) {
    return e as ApiError;
  }
  throw new Error('expected the call to reject');
}

function sent(call = 0) {
  const [url, init] = fetchMock.mock.calls[call];
  return { url, init, headers: init.headers as Record<string, string> };
}

beforeEach(() => {
  fetchMock.mockReset();
  vi.stubGlobal('fetch', fetchMock);
  mockGetAccessToken.mockReset();
  mockRefreshAccessToken.mockReset();
  mockAuthGeneration.mockReset();
  mockGetAccessToken.mockResolvedValue(null);
  mockRefreshAccessToken.mockResolvedValue(null);
  mockAuthGeneration.mockReturnValue(0);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('bearer token', () => {
  it('attaches the Bearer token the cache serves', async () => {
    mockGetAccessToken.mockResolvedValue('my-token');
    reply(json(200, {}));
    await api.get('/x');
    expect(sent().headers.Authorization).toBe('Bearer my-token');
  });

  it('does not attach Authorization when there is no token', async () => {
    reply(json(200, {}));
    await api.get('/x');
    expect(sent().headers.Authorization).toBeUndefined();
  });

  it('proceeds without auth when the cache rejects', async () => {
    mockGetAccessToken.mockRejectedValue(new Error('auth error'));
    reply(json(200, {}));
    await api.get('/x');
    expect(sent().headers.Authorization).toBeUndefined();
  });
});

describe('what goes on the wire', () => {
  it('encodes params the way axios did, so the backend reads the same bytes', async () => {
    reply(json(200, {}));
    await api.get('/q', {
      params: { path: 'a b/c:d,e$f', offset: 0, flag: false, on: true, skipped: null, gone: undefined, empty: '' },
    });
    expect(sent().url).toBe('/q?path=a+b%2Fc:d,e$f&offset=0&flag=false&on=true&empty=');
  });

  it('sends URLSearchParams as they serialize themselves', async () => {
    reply(json(200, {}));
    await api.get('/orders', { params: new URLSearchParams([['status', 'a:b'], ['status', 'c']]) });
    expect(sent().url).toBe('/orders?status=a%3Ab&status=c');
  });

  it('builds the same URL for getUri as for a request', () => {
    expect(api.getUri({ url: '/dl', params: { path: 'x y', attachment: true } })).toBe('/dl?path=x+y&attachment=true');
  });

  it('sends JSON, a literal null, no body, or a multipart body the browser labels', async () => {
    reply(json(200, {}));
    await api.post('/a', { k: 1 });
    await api.post('/b', null);
    await api.post('/c');
    const form = new FormData();
    form.append('file', new Blob(['x']), 'x.txt');
    await api.post('/d', form);

    expect(sent(0).init.body).toBe('{"k":1}');
    expect(sent(0).headers['Content-Type']).toBe('application/json');
    // axios sent `null` for an explicit null, and several routes are called that way.
    expect(sent(1).init.body).toBe('null');
    expect(sent(1).headers['Content-Type']).toBe('application/json');
    expect(sent(2).init.body).toBeUndefined();
    expect(sent(2).headers['Content-Type']).toBeUndefined();
    // Setting it by hand would drop the boundary the server needs to split the parts.
    expect(sent(3).init.body).toBe(form);
    expect(sent(3).headers['Content-Type']).toBeUndefined();
  });

  it('sends a DELETE body from config.data', async () => {
    reply(json(200, {}));
    await api.delete('/files', { data: { paths: ['a'] } });
    expect(sent().init.method).toBe('DELETE');
    expect(sent().init.body).toBe('{"paths":["a"]}');
  });

  it('hands fetch the cache mode a call asks for, and none otherwise', async () => {
    reply(json(200, {}));
    await api.get('/range', { cache: 'no-store' });
    await api.get('/plain');
    expect(sent(0).init.cache).toBe('no-store');
    expect(sent(1).init.cache).toBeUndefined();
  });
});

describe('what comes back', () => {
  it('parses JSON, and hands back text that is not JSON as it came', async () => {
    reply(json(200, { ok: true }), { status: 200, body: 'plain words' }, { status: 204 });
    expect((await api.get('/j')).data).toEqual({ ok: true });
    expect((await api.get('/t')).data).toBe('plain words');
    const empty = await api.delete('/e');
    expect(empty.status).toBe(204);
    expect(empty.data).toBe('');
  });

  it('honors responseType text, blob and arraybuffer', async () => {
    reply({ status: 200, body: '{"a":1}' });
    expect((await api.get('/t', { responseType: 'text' })).data).toBe('{"a":1}');
    expect(await ((await api.get('/b', { responseType: 'blob' })).data as Blob).text()).toBe('{"a":1}');
    expect(((await api.get('/ab', { responseType: 'arraybuffer' })).data as ArrayBuffer).byteLength).toBe(7);
  });

  it('rejects a refusal with the response on the error, headers read by lower-case name', async () => {
    reply({ status: 502, body: '<html>bad gateway</html>', headers: { 'X-Trace': 't1' } });
    const err = await failure(api.get('/x'));
    expect(err).toBeInstanceOf(ApiError);
    expect(err.message).toBe('Request failed with status code 502');
    expect(err.code).toBe('ERR_BAD_RESPONSE');
    expect(err.status).toBe(502);
    expect(err.response?.data).toBe('<html>bad gateway</html>');
    expect(err.response?.headers['x-trace']).toBe('t1');
  });
});

describe('failures without a response', () => {
  it('rejects an aborted request as a CanceledError', async () => {
    reply(json(200, {}));
    const controller = new AbortController();
    controller.abort();
    const err = await failure(api.get('/x', { signal: controller.signal }));
    expect(err.name).toBe('CanceledError');
    expect(err.code).toBe('ERR_CANCELED');
  });

  it('names the bound a timed-out request ran into', async () => {
    fetchMock.mockImplementation((_url, init) => new Promise((_resolve, reject) => {
      init.signal?.addEventListener('abort', () => reject(new DOMException('timed out', 'TimeoutError')));
    }));
    const err = await failure(api.post('/slow', undefined, { timeout: 5 }));
    expect(err.message).toBe('timeout of 5ms exceeded');
    expect(err.code).toBe('ECONNABORTED');
    // Told apart from an abort and from an answer, so a retry reads it as transient.
    expect(err.name).toBe('ApiError');
    expect(err.status).toBeUndefined();
  });

  it('leaves no listener on a caller signal reused across timed requests', async () => {
    reply(json(200, {}));
    // Safari before 17.4 has no AbortSignal.any.
    const any = AbortSignal.any;
    Object.defineProperty(AbortSignal, 'any', { value: undefined, configurable: true });
    try {
      const controller = new AbortController();
      const add = vi.spyOn(controller.signal, 'addEventListener');
      const remove = vi.spyOn(controller.signal, 'removeEventListener');
      for (let i = 0; i < 3; i++) await api.get('/x', { signal: controller.signal, timeout: 1000 });
      expect(remove.mock.calls.length).toBe(add.mock.calls.length);
    } finally {
      Object.defineProperty(AbortSignal, 'any', { value: any, configurable: true });
    }
  });

  it('reports a failed connection as a network error', async () => {
    fetchMock.mockRejectedValue(new TypeError('Failed to fetch'));
    const err = await failure(api.get('/x'));
    expect(err.message).toBe('Network Error');
    expect(err.code).toBe('ERR_NETWORK');
    expect(err.response).toBeUndefined();
  });
});

/**
 * An answer whose body the test hands over chunk by chunk, so a stall and a
 * slow trickle can be told apart. An abort fails a read in progress, as it
 * fails a fetch body's.
 */
function handAnswer(status = 200, headersAfter = 0) {
  type Read = ReadableStreamReadResult<Uint8Array>;
  const waiting: { resolve: (read: Read) => void; reject: (err: unknown) => void }[] = [];
  const ready: Read[] = [];
  const give = (read: Read) => {
    const reader = waiting.shift();
    if (reader) reader.resolve(read);
    else ready.push(read);
  };
  const read = () => (ready.length > 0
    ? Promise.resolve(ready.shift()!)
    : new Promise<Read>((resolve, reject) => waiting.push({ resolve, reject })));
  const getReader = vi.fn(() => ({ read }));
  const cancel = vi.fn(async () => {});
  fetchMock.mockImplementation(async (_url, init) => {
    init.signal?.addEventListener('abort', () => {
      for (const reader of waiting.splice(0)) reader.reject(new DOMException('aborted', 'AbortError'));
    });
    if (headersAfter) await new Promise((resolve) => setTimeout(resolve, headersAfter));
    return {
      ok: status >= 200 && status < 300, status, statusText: '', headers: new Headers(), body: { getReader, cancel },
    } as unknown as Response;
  });
  return {
    push: (text: string) => give({ done: false, value: new TextEncoder().encode(text) }),
    end: () => give({ done: true, value: undefined }),
    getReader,
    cancel,
  };
}

/** Whether a call has settled yet, read without awaiting it. */
function watch<T>(call: Promise<T>) {
  const state = { settled: false, outcome: call.finally(() => { state.settled = true; }) };
  return state;
}

describe('an idle bound', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it('fails an answer that goes silent for that long, as a timeout does', async () => {
    const answer = handAnswer();
    const call = watch(failure(api.get('/x', { idleTimeout: 1000, responseType: 'arraybuffer' })));
    await vi.advanceTimersByTimeAsync(600);
    answer.push('a');
    // The chunk at 600 put the bound off to 1600.
    await vi.advanceTimersByTimeAsync(999);
    expect(call.settled).toBe(false);
    await vi.advanceTimersByTimeAsync(1);
    const err = await call.outcome;
    expect(err.message).toBe('timeout of 1000ms exceeded');
    expect(err.code).toBe('ECONNABORTED');
    expect(err.name).toBe('ApiError');
    expect(err.status).toBeUndefined();
  });

  it('lets a slow answer finish while each chunk comes inside it', async () => {
    const answer = handAnswer();
    const call = watch(api.get('/x', { idleTimeout: 1000, responseType: 'arraybuffer' }));
    for (const chunk of ['a', 'b', 'c', 'd', 'e']) {
      await vi.advanceTimersByTimeAsync(600);
      answer.push(chunk);
    }
    answer.end();
    const res = await call.outcome;
    expect(new TextDecoder().decode(res.data as ArrayBuffer)).toBe('abcde');
  });

  it('counts the wait for headers', async () => {
    fetchMock.mockImplementation((_url, init) => new Promise((_resolve, reject) => {
      init.signal?.addEventListener('abort', () => reject(new DOMException('aborted', 'AbortError')));
    }));
    const call = watch(failure(api.get('/x', { idleTimeout: 1000 })));
    await vi.advanceTimersByTimeAsync(999);
    expect(call.settled).toBe(false);
    await vi.advanceTimersByTimeAsync(1);
    expect((await call.outcome).code).toBe('ECONNABORTED');
  });

  it('puts the bound off when the headers arrive', async () => {
    const answer = handAnswer(200, 900);
    const call = watch(api.get('/x', { idleTimeout: 1000, responseType: 'text' }));
    await vi.advanceTimersByTimeAsync(1500);
    answer.push('late but there');
    answer.end();
    expect((await call.outcome).data).toBe('late but there');
  });

  it('keeps a total deadline given beside it', async () => {
    const answer = handAnswer();
    const call = watch(failure(api.get('/x', { timeout: 2000, idleTimeout: 1000 })));
    for (let i = 0; i < 4; i++) {
      await vi.advanceTimersByTimeAsync(600);
      answer.push('a');
    }
    const err = await call.outcome;
    expect(err.message).toBe('timeout of 2000ms exceeded');
    expect(err.code).toBe('ECONNABORTED');
  });
});

describe('an expected status', () => {
  it('hands back another success with no body, its stream cancelled unread', async () => {
    const answer = handAnswer(200);
    answer.push('the whole file');
    for (const idleTimeout of [undefined, 1000]) {
      const res = await api.get('/x', { expectStatus: 206, idleTimeout, responseType: 'arraybuffer' });
      expect(res.status).toBe(200);
      expect(res.data).toBeNull();
    }
    expect(answer.cancel).toHaveBeenCalledTimes(2);
    expect(answer.getReader).not.toHaveBeenCalled();
  });

  it('reads the body of the status it expects, and of a refusal', async () => {
    reply({ status: 206, body: 'abc' }, json(503, { detail: 'busy' }));
    const res = await api.get('/x', { expectStatus: 206, responseType: 'text' });
    expect(res.data).toBe('abc');
    const err = await failure(api.get('/x', { expectStatus: 206 }));
    expect(err.response?.data).toEqual({ detail: 'busy' });
  });
});

/** Just enough XMLHttpRequest to drive the progress transport by hand. */
class FakeXhr {
  static opened: FakeXhr[] = [];
  method = '';
  url = '';
  requestHeaders: Record<string, string> = {};
  body: unknown;
  responseType = '';
  status = 0;
  statusText = '';
  response: unknown = null;
  responseText = '';
  responseHeaders = '';
  upload: { onprogress: ((e: Partial<ProgressEvent>) => void) | null } = { onprogress: null };
  onprogress: ((e: Partial<ProgressEvent>) => void) | null = null;
  onloadend: (() => void) | null = null;
  open(method: string, url: string) {
    this.method = method;
    this.url = url;
    FakeXhr.opened.push(this);
  }
  setRequestHeader(name: string, value: string) {
    this.requestHeaders[name] = value;
  }
  getAllResponseHeaders() {
    return this.responseHeaders;
  }
  send(body: unknown) {
    this.body = body;
  }
  abort() {
    this.onloadend?.();
  }
  respond(status: number, body: string, headers = '') {
    Object.assign(this, { status, statusText: 'OK', responseText: body, response: body, responseHeaders: headers });
    this.onloadend?.();
  }
}

/** The request the client opened; the token read before it is async. */
async function opened(): Promise<FakeXhr> {
  await vi.waitFor(() => expect(FakeXhr.opened).toHaveLength(1));
  return FakeXhr.opened[0];
}

describe('with a progress listener', () => {
  beforeEach(() => {
    FakeXhr.opened = [];
    vi.stubGlobal('XMLHttpRequest', FakeXhr);
  });

  it('reports upload progress and resolves the same shape fetch does', async () => {
    const progress = vi.fn();
    const form = new FormData();
    const call = api.post('/upload', form, { onUploadProgress: progress });
    const xhr = await opened();
    expect(xhr.body).toBe(form);
    expect(xhr.requestHeaders['Content-Type']).toBeUndefined();

    xhr.upload.onprogress!({ loaded: 5, total: 10, lengthComputable: true });
    xhr.upload.onprogress!({ loaded: 7, total: 0, lengthComputable: false });
    xhr.respond(200, '{"ok":true}', 'Content-Type: application/json\r\nX-Request-Id: abc\r\n');
    const res = await call;

    expect(progress.mock.calls).toEqual([[{ loaded: 5, total: 10 }], [{ loaded: 7, total: undefined }]]);
    expect(res.data).toEqual({ ok: true });
    expect(res.headers['x-request-id']).toBe('abc');
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('hands back a binary download as the type it asked for', async () => {
    const call = api.get('/file', { responseType: 'blob', onDownloadProgress: () => {} });
    const xhr = await opened();
    expect(xhr.responseType).toBe('blob');
    const blob = new Blob(['bytes']);
    Object.assign(xhr, { status: 200, response: blob });
    xhr.onloadend!();
    expect((await call).data).toBe(blob);
  });

  it('rejects a 4xx with the response it carried', async () => {
    const call = failure(api.post('/upload', new FormData(), { onUploadProgress: () => {} }));
    (await opened()).respond(413, '{"detail":"too big"}', 'Content-Type: application/json\r\n');
    const err = await call;
    expect(err.code).toBe('ERR_BAD_REQUEST');
    expect(err.status).toBe(413);
    expect(err.response?.data).toEqual({ detail: 'too big' });
    expect(err.response?.headers['content-type']).toBe('application/json');
  });

  it('tells an abort from a timeout from a dead link', async () => {
    const controller = new AbortController();
    const aborted = failure(api.get('/x', { signal: controller.signal, onDownloadProgress: () => {} }));
    await opened();
    controller.abort();
    expect((await aborted).name).toBe('CanceledError');

    FakeXhr.opened = [];
    const timedOut = failure(api.get('/x', { timeout: 5, onDownloadProgress: () => {} }));
    await opened();
    expect((await timedOut).code).toBe('ECONNABORTED');

    FakeXhr.opened = [];
    const dropped = failure(api.get('/x', { onDownloadProgress: () => {} }));
    (await opened()).abort();
    expect((await dropped).code).toBe('ERR_NETWORK');
  });
});

describe('429 and structured detail', () => {
  it('enriches 429 errors with rateLimitInfo and retryAfter', async () => {
    reply(json(429, { detail: { message: 'Too many requests', limit: 10 } }, { 'Retry-After': '30' }));
    await expect(api.get('/x')).rejects.toMatchObject({
      status: 429,
      rateLimitInfo: { message: 'Too many requests', limit: 10 },
      retryAfter: 30,
    });
  });

  it('rejects non-429 errors without enrichment', async () => {
    reply(json(500, { detail: 'Server error' }));
    const err = await failure(api.get('/x'));
    expect(err.rateLimitInfo).toBeUndefined();
  });

  it('surfaces a structured detail message on any status', async () => {
    // The credit gate fails closed with a 503 the user is meant to read. Call
    // sites fall back to err.message, which is "Request failed with status
    // code 503" unless the client lifts the real sentence out.
    reply(json(503, { detail: { message: 'Service temporarily unavailable. Please try again shortly.', type: 'service_unavailable' } }));
    const err = await failure(api.get('/x'));
    expect(err.message).toBe('Service temporarily unavailable. Please try again shortly.');
    // Not a rate limit, so nothing pretends it is one.
    expect(err.rateLimitInfo).toBeUndefined();
  });

  it('leaves the message alone when detail is a bare string', async () => {
    reply(json(500, { detail: 'Server error' }));
    await expect(api.get('/x')).rejects.toMatchObject({ message: 'Request failed with status code 500' });
  });
});

describe('401 refresh-and-retry', () => {
  it('refuses the 401 replay when the account switched during the rotation', async () => {
    // The generation is checked twice on purpose. Between the two sits a network
    // round trip, and a sign-out or an account switch can land inside it; the
    // request still carries the previous user's URL and body, so a replay stamped
    // with the new user's token is their mutation written to the new account.
    mockGetAccessToken.mockResolvedValue('sent-token');
    mockAuthGeneration.mockReturnValue(7);
    mockRefreshAccessToken.mockImplementation(async () => {
      mockAuthGeneration.mockReturnValue(8);
      return 'next-users-token';
    });
    reply(json(401, {}));

    await expect(api.post('/threads', { a: 1 })).rejects.toMatchObject({ status: 401 });
    expect(mockRefreshAccessToken).toHaveBeenCalledTimes(1);
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it('refreshes once and replays with the fresh Bearer token and the same body', async () => {
    mockGetAccessToken.mockResolvedValueOnce('sent-token').mockResolvedValue(null);
    mockRefreshAccessToken.mockResolvedValue('refreshed-token');
    reply(json(401, {}), json(200, { ok: true }));

    const result = await api.post('/test', { a: 1 });

    expect(mockRefreshAccessToken).toHaveBeenCalledTimes(1);
    // The token this request CARRIED, bare. Rotating against the cache instead
    // would hand a straggling 401 back the token it just refused, and the
    // single-shot retry guard means there is no second chance to notice.
    expect(mockRefreshAccessToken).toHaveBeenCalledWith('sent-token');
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(sent(1).headers.Authorization).toBe('Bearer refreshed-token');
    expect(sent(1).init.body).toBe('{"a":1}');
    expect(result.status).toBe(200);
    expect(result.data).toEqual({ ok: true });
  });

  it('retries when the same user is still signed in', async () => {
    // The companion to the test below: without this, a fence that blocked every
    // retry would look just as green.
    mockGetAccessToken.mockResolvedValue('sent-token');
    mockAuthGeneration.mockReturnValue(4);
    mockRefreshAccessToken.mockResolvedValue('refreshed-token');
    reply(json(401, {}), json(200, { ok: true }));

    await api.get('/test');
    expect(mockRefreshAccessToken).toHaveBeenCalledTimes(1);
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it('does not replay a 401 that outlived the account it was issued for', async () => {
    // A 401 can arrive after a sign-out or an account switch. By then the cache
    // holds the next user's token, and all `refreshAccessToken` is asked is
    // whether the cache moved on from the refused one, which it has -- so it
    // would hand that token over and the retry would send a request built for
    // the previous user as the current one.
    mockGetAccessToken.mockResolvedValue('departed-users-token');
    mockAuthGeneration.mockReturnValue(8);
    mockRefreshAccessToken.mockResolvedValue('next-users-token');
    fetchMock.mockImplementation(async () => {
      mockAuthGeneration.mockReturnValue(9);
      return new Response('{}', { status: 401 });
    });

    await expect(api.get('/test')).rejects.toMatchObject({ status: 401 });
    expect(mockRefreshAccessToken).not.toHaveBeenCalled();
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it('rejects on a second 401 without refreshing again (no loop)', async () => {
    mockGetAccessToken.mockResolvedValue('sent-token');
    mockRefreshAccessToken.mockResolvedValue('refreshed-token');
    reply(json(401, {}));

    await expect(api.get('/test')).rejects.toMatchObject({ status: 401 });
    expect(mockRefreshAccessToken).toHaveBeenCalledTimes(1);
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it('rejects a 401 unchanged when no rotation is available (local-dev parity)', async () => {
    // The cache answers null: no Supabase client, or the breaker is closed.
    // Either way there is no new token, so nothing is replayed.
    reply(json(401, {}));

    const err = await failure(api.get('/test'));
    expect(err.status).toBe(401);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(err.rateLimitInfo).toBeUndefined();
  });
});
