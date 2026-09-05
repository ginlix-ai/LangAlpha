/**
 * Locks the one link between a gate's HTTP response and what the user reads.
 *
 * The 503 body below is copied verbatim from a live response of the credit
 * gate with the quota service down. If this parsing ever stops lifting
 * `detail.message` out, the banner silently degrades to "HTTP error! status:
 * 503" and the sentence the server took care to send is lost.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

vi.mock('@/api/client', () => ({
  api: { defaults: { baseURL: '' } },
}));
vi.mock('@/lib/supabase', () => ({
  supabase: { auth: { getSession: async () => ({ data: { session: null } }) } },
}));
vi.mock('@/config/hostMode', () => ({ isPlatformMode: false, HOST_MODE: 'oss' }));

const GATE_503 = {
  detail: {
    message: 'Service temporarily unavailable. Please try again shortly.',
    type: 'service_unavailable',
    retry_after: 15,
  },
};

const GATE_429 = {
  detail: {
    message: 'Monthly credit limit reached (38,000/38,000 credits).',
    type: 'monthly_credit_limit',
    used_credits: 38000,
    credit_limit: 38000,
    retry_after: 42,
  },
};

function respond(status: number, body: unknown, headers: Record<string, string> = {}) {
  return {
    ok: false,
    status,
    headers: { get: (k: string) => headers[k.toLowerCase()] ?? null },
    json: async () => body,
    text: async () => JSON.stringify(body),
  } as unknown as Response;
}

describe('streamFetch error surfacing', () => {
  beforeEach(() => {
    vi.stubGlobal('fetch', vi.fn());
  });
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('surfaces the gate sentence for a 503, not the status code', async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(respond(503, GATE_503));
    const { streamFetch } = await import('../transport');

    await expect(streamFetch('/api/v1/threads/x/messages', {}, () => {})).rejects.toMatchObject({
      status: 503,
      message: 'Service temporarily unavailable. Please try again shortly.',
      errorInfo: { type: 'service_unavailable' },
    });
  });

  it('still routes a 429 into rateLimitInfo with its retry hint', async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(
      respond(429, GATE_429, { 'retry-after': '42' }),
    );
    const { streamFetch } = await import('../transport');

    await expect(streamFetch('/api/v1/threads/x/messages', {}, () => {})).rejects.toMatchObject({
      status: 429,
      retryAfter: 42,
      rateLimitInfo: { type: 'monthly_credit_limit' },
    });
  });
});
