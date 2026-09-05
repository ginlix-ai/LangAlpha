import { describe, it, expect, vi, beforeEach } from 'vitest';

// The interceptors read straight from the shared token cache. Mocking it here
// is the injection seam: there is no registry to hand a getter to any more,
// because a per-request session read is exactly what caused issue #379.
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

import { api } from '../client';

interface InterceptorHandler<T = unknown> {
  fulfilled: (value: T) => T | Promise<T>;
  rejected: (error: unknown) => unknown;
}

interface InterceptorManager<T> {
  handlers: InterceptorHandler<T>[];
}

describe('api axios instance', () => {
  it('exports an api object with expected methods', async () => {
    const { api } = await import('../client');
    expect(api).toBeDefined();
    expect(typeof api.get).toBe('function');
    expect(typeof api.post).toBe('function');
    expect(typeof api.put).toBe('function');
    expect(typeof api.delete).toBe('function');
  });

  it('has JSON content-type as default header', async () => {
    const { api } = await import('../client');
    expect(api.defaults.headers.common?.['Content-Type'] || api.defaults.headers['Content-Type']).toBe('application/json');
  });

  it('has interceptors registered', async () => {
    const { api } = await import('../client');
    // Axios interceptors have a handlers array
    const reqInterceptors = api.interceptors.request as unknown as InterceptorManager<unknown>;
    const resInterceptors = api.interceptors.response as unknown as InterceptorManager<unknown>;
    expect(reqInterceptors.handlers.length).toBeGreaterThan(0);
    expect(resInterceptors.handlers.length).toBeGreaterThan(0);
  });
});

describe('request interceptor behavior', () => {
  beforeEach(() => {
    mockGetAccessToken.mockReset();
    mockGetAccessToken.mockResolvedValue(null);
  });

  it('attaches the Bearer token the cache serves', async () => {
    const { api } = await import('../client');
    mockGetAccessToken.mockResolvedValue('my-token');

    const reqInterceptors = api.interceptors.request as unknown as InterceptorManager<{ headers: Record<string, string> }>;
    const handler = reqInterceptors.handlers[0];
    const interceptor = handler.fulfilled;

    const config = { headers: {} as Record<string, string> };
    const result = await interceptor(config);
    expect(result.headers.Authorization).toBe('Bearer my-token');
  });

  it('does not attach Authorization when there is no token', async () => {
    const { api } = await import('../client');
    mockGetAccessToken.mockResolvedValue(null);

    const reqInterceptors = api.interceptors.request as unknown as InterceptorManager<{ headers: Record<string, string> }>;
    const handler = reqInterceptors.handlers[0];
    const interceptor = handler.fulfilled;

    const config = { headers: {} as Record<string, string> };
    const result = await interceptor(config);
    expect(result.headers.Authorization).toBeUndefined();
  });

  it('proceeds without auth when the cache rejects', async () => {
    const { api } = await import('../client');
    mockGetAccessToken.mockRejectedValue(new Error('auth error'));

    const reqInterceptors = api.interceptors.request as unknown as InterceptorManager<{ headers: Record<string, string> }>;
    const handler = reqInterceptors.handlers[0];
    const interceptor = handler.fulfilled;

    const config = { headers: {} as Record<string, string> };
    const result = await interceptor(config);
    expect(result.headers.Authorization).toBeUndefined();
  });
});

describe('response interceptor behavior (429 handling)', () => {
  it('enriches 429 errors with rateLimitInfo and retryAfter', async () => {
    const { api } = await import('../client');

    const resInterceptors = api.interceptors.response as unknown as InterceptorManager<unknown>;
    const handler = resInterceptors.handlers[0];
    const errorHandler = handler.rejected;

    const error = {
      response: {
        status: 429,
        data: { detail: { message: 'Too many requests', limit: 10 } },
        headers: { 'retry-after': '30' },
      },
    };

    await expect(errorHandler(error)).rejects.toMatchObject({
      status: 429,
      rateLimitInfo: { message: 'Too many requests', limit: 10 },
      retryAfter: 30,
    });
  });

  it('rejects non-429 errors without enrichment', async () => {
    const { api } = await import('../client');

    const resInterceptors = api.interceptors.response as unknown as InterceptorManager<unknown>;
    const handler = resInterceptors.handlers[0];
    const errorHandler = handler.rejected;

    const error: Record<string, unknown> = {
      response: { status: 500, data: { detail: 'Server error' } },
    };

    await expect(errorHandler(error)).rejects.toBe(error);
    expect(error.rateLimitInfo).toBeUndefined();
  });

  it('surfaces a structured detail message on any status', async () => {
    // The credit gate fails closed with a 503 the user is meant to read. Call
    // sites fall back to err.message, which is axios's "Request failed with
    // status code 503" unless the interceptor lifts the real sentence out.
    const { api } = await import('../client');

    const resInterceptors = api.interceptors.response as unknown as InterceptorManager<unknown>;
    const errorHandler = resInterceptors.handlers[0].rejected;

    const error: Record<string, unknown> = {
      message: 'Request failed with status code 503',
      response: {
        status: 503,
        data: { detail: { message: 'Service temporarily unavailable. Please try again shortly.', type: 'service_unavailable' } },
      },
    };

    await expect(errorHandler(error)).rejects.toMatchObject({
      message: 'Service temporarily unavailable. Please try again shortly.',
    });
    // Not a rate limit, so nothing pretends it is one.
    expect(error.rateLimitInfo).toBeUndefined();
  });

  it('leaves the message alone when detail is a bare string', async () => {
    const { api } = await import('../client');

    const resInterceptors = api.interceptors.response as unknown as InterceptorManager<unknown>;
    const errorHandler = resInterceptors.handlers[0].rejected;

    const error: Record<string, unknown> = {
      message: 'Request failed with status code 500',
      response: { status: 500, data: { detail: 'Server error' } },
    };

    await expect(errorHandler(error)).rejects.toMatchObject({
      message: 'Request failed with status code 500',
    });
  });
});

describe('response interceptor behavior (401 refresh-and-retry)', () => {
  // Module singletons persist across tests — reset both.
  beforeEach(() => {
    mockGetAccessToken.mockReset();
    mockRefreshAccessToken.mockReset();
    mockAuthGeneration.mockReset();
    mockGetAccessToken.mockResolvedValue(null);
    mockRefreshAccessToken.mockResolvedValue(null);
    mockAuthGeneration.mockReturnValue(0);
  });

  /** Reads the Authorization header off a config robust to AxiosHeaders normalization. */
  function readAuth(config: { headers?: { get?: (k: string) => unknown; Authorization?: unknown } }) {
    const headers = config.headers;
    if (!headers) return undefined;
    return headers.get ? headers.get('Authorization') : headers.Authorization;
  }

  it('refuses the 401 replay when the account switched during the rotation', async () => {
    // The generation is checked twice on purpose. Between the two sits a network
    // round trip, and a sign-out or an account switch can land inside it; the
    // config still carries the previous user's URL and body, so a replay stamped
    // with the new user's token is their mutation written to the new account.
    mockAuthGeneration.mockReturnValue(7);
    mockRefreshAccessToken.mockImplementation(async () => {
      mockAuthGeneration.mockReturnValue(8);
      return 'next-users-token';
    });

    const adapter = vi.fn();
    const originalAdapter = api.defaults.adapter;
    api.defaults.adapter = adapter as unknown as typeof api.defaults.adapter;

    try {
      const resInterceptors = api.interceptors.response as unknown as InterceptorManager<unknown>;
      const errorHandler = resInterceptors.handlers[0].rejected;

      const error = {
        response: { status: 401, data: {}, headers: {} },
        config: {
          url: '/threads', method: 'post', _authGeneration: 7,
          headers: { Authorization: 'Bearer sent-token' },
        },
      };

      await expect(errorHandler(error)).rejects.toBe(error);
      expect(mockRefreshAccessToken).toHaveBeenCalledTimes(1);
      expect(adapter).not.toHaveBeenCalled();
    } finally {
      api.defaults.adapter = originalAdapter;
    }
  });

  it('refreshes once and retries with the fresh Bearer token on 401, then succeeds', async () => {
    const refresher = mockRefreshAccessToken;
    refresher.mockResolvedValue('refreshed-token');

    const adapter = vi.fn(async (config: unknown) => ({
      data: { ok: true },
      status: 200,
      statusText: 'OK',
      headers: {},
      config,
    }));
    const originalAdapter = api.defaults.adapter;
    api.defaults.adapter = adapter as unknown as typeof api.defaults.adapter;

    try {
      const resInterceptors = api.interceptors.response as unknown as InterceptorManager<unknown>;
      const errorHandler = resInterceptors.handlers[0].rejected;

      const error = {
        response: { status: 401, data: {}, headers: {} },
        config: { url: '/test', method: 'get', headers: { Authorization: 'Bearer sent-token' } },
      };

      const result = (await errorHandler(error)) as { status: number; data: unknown };

      expect(refresher).toHaveBeenCalledTimes(1);
      // The token this request CARRIED, bare. Rotating against the cache instead
      // would hand a straggling 401 back the token it just refused, and the
      // single-shot `_retry` guard means there is no second chance to notice.
      expect(refresher).toHaveBeenCalledWith('sent-token');
      expect(adapter).toHaveBeenCalledTimes(1);
      const retriedConfig = adapter.mock.calls[0][0] as { headers?: { get?: (k: string) => unknown; Authorization?: unknown } };
      expect(readAuth(retriedConfig)).toBe('Bearer refreshed-token');
      expect(result.status).toBe(200);
      expect(result.data).toEqual({ ok: true });
    } finally {
      api.defaults.adapter = originalAdapter;
    }
  });

  it('retries when the same user is still signed in', async () => {
    // The companion to the test below: without this, a fence that blocked every
    // retry would look just as green.
    mockAuthGeneration.mockReturnValue(4);
    mockRefreshAccessToken.mockResolvedValue('refreshed-token');

    const adapter = vi.fn(async (config: unknown) => ({
      data: { ok: true }, status: 200, statusText: 'OK', headers: {}, config,
    }));
    const originalAdapter = api.defaults.adapter;
    api.defaults.adapter = adapter as unknown as typeof api.defaults.adapter;

    try {
      const resInterceptors = api.interceptors.response as unknown as InterceptorManager<unknown>;
      const errorHandler = resInterceptors.handlers[0].rejected;

      await errorHandler({
        response: { status: 401, data: {}, headers: {} },
        config: {
          url: '/test', method: 'get',
          headers: { Authorization: 'Bearer sent-token' },
          _authGeneration: 4,
        },
      });

      expect(mockRefreshAccessToken).toHaveBeenCalledTimes(1);
      expect(adapter).toHaveBeenCalledTimes(1);
    } finally {
      api.defaults.adapter = originalAdapter;
    }
  });

  it('does not replay a 401 that outlived the account it was issued for', async () => {
    // A 401 can arrive after a sign-out or an account switch. By then the cache
    // holds the next user's token, and all `refreshAccessToken` is asked is
    // whether the cache moved on from the refused one, which it has -- so it
    // would hand that token over and the retry would send a request built for
    // the previous user as the current one.
    mockAuthGeneration.mockReturnValue(9);
    mockRefreshAccessToken.mockResolvedValue('next-users-token');

    const adapter = vi.fn();
    const originalAdapter = api.defaults.adapter;
    api.defaults.adapter = adapter as unknown as typeof api.defaults.adapter;

    try {
      const resInterceptors = api.interceptors.response as unknown as InterceptorManager<unknown>;
      const errorHandler = resInterceptors.handlers[0].rejected;

      await expect(errorHandler({
        response: { status: 401, data: {}, headers: {} },
        config: {
          url: '/test', method: 'get',
          headers: { Authorization: 'Bearer departed-users-token' },
          _authGeneration: 8,
        },
      })).rejects.toBeDefined();

      expect(mockRefreshAccessToken).not.toHaveBeenCalled();
      expect(adapter).not.toHaveBeenCalled();
    } finally {
      api.defaults.adapter = originalAdapter;
    }
  });

  it('rejects on a second 401 without refreshing again (no loop)', async () => {
    const refresher = mockRefreshAccessToken;
    refresher.mockResolvedValue('refreshed-token');

    // A custom adapter is responsible for rejecting on bad status (axios only applies
    // validateStatus inside its built-in adapters). Reject 401 with the merged config so
    // the retry re-enters the interceptor with config._retry already true → branch skipped.
    const adapter = vi.fn((config: unknown) =>
      Promise.reject({ response: { status: 401, data: {}, headers: {} }, config }),
    );
    const originalAdapter = api.defaults.adapter;
    api.defaults.adapter = adapter as unknown as typeof api.defaults.adapter;

    try {
      const resInterceptors = api.interceptors.response as unknown as InterceptorManager<unknown>;
      const errorHandler = resInterceptors.handlers[0].rejected;

      const error = {
        response: { status: 401, data: {}, headers: {} },
        config: { url: '/test', method: 'get', headers: {} },
      };

      await expect(errorHandler(error)).rejects.toBeDefined();
      expect(refresher).toHaveBeenCalledTimes(1);
      expect(adapter).toHaveBeenCalledTimes(1);
    } finally {
      api.defaults.adapter = originalAdapter;
    }
  });

  it('rejects a 401 unchanged when no rotation is available (local-dev parity)', async () => {
    // The cache answers null: no Supabase client, or the breaker is closed.
    // Either way there is no new token, so the adapter must never be reached.
    const adapter = vi.fn();
    const originalAdapter = api.defaults.adapter;
    api.defaults.adapter = adapter as unknown as typeof api.defaults.adapter;

    try {
      const resInterceptors = api.interceptors.response as unknown as InterceptorManager<unknown>;
      const errorHandler = resInterceptors.handlers[0].rejected;

      const error: Record<string, unknown> = {
        response: { status: 401, data: {} },
        config: { headers: {} },
      };

      await expect(errorHandler(error)).rejects.toBe(error);
      expect(adapter).not.toHaveBeenCalled();
      expect(error.rateLimitInfo).toBeUndefined();
    } finally {
      api.defaults.adapter = originalAdapter;
    }
  });
});
