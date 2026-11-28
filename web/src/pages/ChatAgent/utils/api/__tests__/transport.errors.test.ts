// @vitest-environment node
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

describe('streamFetch: which refusals are worded for the reader', () => {
  beforeEach(() => {
    vi.stubGlobal('fetch', vi.fn());
  });
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  const refusedWith = async (status: number, body: unknown) => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(respond(status, body));
    const { streamFetch } = await import('../transport');
    return streamFetch('/api/v1/threads/x/messages', {}, () => {}).then(
      () => ({ reason: 'resolved' as string | undefined }),
      (err: unknown) => err as { reason?: string },
    );
  };

  it('takes a structured message or a plain-text detail as the reason', async () => {
    expect((await refusedWith(503, GATE_503)).reason).toBe('Service temporarily unavailable. Please try again shortly.');
    expect((await refusedWith(409, { detail: 'The thread is being deleted.' })).reason).toBe('The thread is being deleted.');
  });

  it("takes a firewall's own sentence as the reason, since the page it sent says nothing", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(
      respond(403, '<html>blocked</html>', { 'content-type': 'text/html' }),
    );
    const { streamFetch } = await import('../transport');
    const { edgeBlockMessage } = await import('@/lib/edgeBlock');
    await expect(streamFetch('/api/v1/threads/x/messages', {}, () => {})).rejects.toMatchObject({
      status: 403,
      edgeBlocked: 'content',
      reason: edgeBlockMessage('content'),
    });
  });

  it('gives none for a detail it could only print as JSON', async () => {
    expect((await refusedWith(422, { detail: [{ loc: ['body'] }] })).reason).toBeUndefined();
  });
});
