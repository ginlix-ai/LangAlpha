import { describe, it, expect, vi, beforeEach } from 'vitest';
import { api, setTokenGetter, setTokenRefresher } from '../client';

interface InterceptorHandler<T = unknown> {
  fulfilled: (value: T) => T | Promise<T>;
  rejected: (error: unknown) => unknown;
}

interface InterceptorManager<T> {
  handlers: InterceptorHandler<T>[];
}

describe('setTokenGetter', () => {
  beforeEach(() => {
    setTokenGetter(null as unknown as () => Promise<string | null>);
  });

  it('accepts a function that will be used for auth', () => {
    const getter = vi.fn(() => Promise.resolve('test-token'));
    expect(() => setTokenGetter(getter)).not.toThrow();
  });

  it('accepts null to clear the token getter', () => {
    expect(() => setTokenGetter(null as unknown as () => Promise<string | null>)).not.toThrow();
  });
});

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
  it('attaches Bearer token when token getter is set', async () => {
    const { api } = await import('../client');
    setTokenGetter(() => Promise.resolve('my-token'));

    const reqInterceptors = api.interceptors.request as unknown as InterceptorManager<{ headers: Record<string, string> }>;
    const handler = reqInterceptors.handlers[0];
    const interceptor = handler.fulfilled;

    const config = { headers: {} as Record<string, string> };
    const result = await interceptor(config);
    expect(result.headers.Authorization).toBe('Bearer my-token');
  });

  it('does not attach Authorization when token getter is null', async () => {
    const { api } = await import('../client');
    setTokenGetter(null as unknown as () => Promise<string | null>);

    const reqInterceptors = api.interceptors.request as unknown as InterceptorManager<{ headers: Record<string, string> }>;
    const handler = reqInterceptors.handlers[0];
    const interceptor = handler.fulfilled;

    const config = { headers: {} as Record<string, string> };
    const result = await interceptor(config);
    expect(result.headers.Authorization).toBeUndefined();
  });

  it('proceeds without auth when token getter throws', async () => {
    const { api } = await import('../client');
    setTokenGetter(() => Promise.reject(new Error('auth error')));

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
});

describe('response interceptor behavior (401 refresh-and-retry)', () => {
  // Module singletons persist across tests — reset both.
  beforeEach(() => {
    setTokenGetter(null as unknown as () => Promise<string | null>);
    setTokenRefresher(null as unknown as () => Promise<string | null>);
  });

  /** Reads the Authorization header off a config robust to AxiosHeaders normalization. */
  function readAuth(config: { headers?: { get?: (k: string) => unknown; Authorization?: unknown } }) {
    const headers = config.headers;
    if (!headers) return undefined;
    return headers.get ? headers.get('Authorization') : headers.Authorization;
  }

  it('refreshes once and retries with the fresh Bearer token on 401, then succeeds', async () => {
    const refresher = vi.fn(() => Promise.resolve('refreshed-token'));
    setTokenRefresher(refresher);

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
        config: { url: '/test', method: 'get', headers: {} },
      };

      const result = (await errorHandler(error)) as { status: number; data: unknown };

      expect(refresher).toHaveBeenCalledTimes(1);
      expect(adapter).toHaveBeenCalledTimes(1);
      const retriedConfig = adapter.mock.calls[0][0] as { headers?: { get?: (k: string) => unknown; Authorization?: unknown } };
      expect(readAuth(retriedConfig)).toBe('Bearer refreshed-token');
      expect(result.status).toBe(200);
      expect(result.data).toEqual({ ok: true });
    } finally {
      api.defaults.adapter = originalAdapter;
    }
  });

  it('rejects on a second 401 without refreshing again (no loop)', async () => {
    const refresher = vi.fn(() => Promise.resolve('refreshed-token'));
    setTokenRefresher(refresher);

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

  it('rejects a 401 unchanged when no refresher is registered (local-dev parity)', async () => {
    // No refresher registered (cleared in beforeEach). Adapter must never be reached.
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
