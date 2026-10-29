// @vitest-environment node
/**
 * A retransmit that raced past the route's duplicate check is refused inside
 * the stream. The send paths reach their caller with the route's own 409 for
 * it, so the adopt or retry the 409 gets runs, and the refusal never reaches
 * the processor as a failed turn.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

vi.mock('@/api/client', () => ({
  api: { defaults: { baseURL: '' } },
}));
vi.mock('@/lib/supabase', () => ({
  supabase: { auth: { getSession: async () => ({ data: { session: null } }) } },
}));
vi.mock('@/config/hostMode', () => ({ isPlatformMode: false, HOST_MODE: 'oss' }));

function sse(frames: Array<{ event: string; data: Record<string, unknown> }>) {
  const body = frames.map((f) => `event: ${f.event}\ndata: ${JSON.stringify(f.data)}\n\n`).join('');
  const bytes = new TextEncoder().encode(body);
  let sent = false;
  return {
    ok: true,
    status: 200,
    headers: { get: () => null },
    body: {
      getReader: () => ({
        read: async () => (sent ? { done: true, value: undefined } : ((sent = true), { done: false, value: bytes })),
      }),
    },
  } as unknown as Response;
}

const DUPLICATE = {
  event: 'error',
  data: {
    thread_id: 'thread-2',
    error: 'This request was already accepted; reconnect to the existing run instead of resending.',
    type: 'workflow_error',
    error_type: 'duplicate_request',
    code: 'duplicate_request',
    run_id: 'run-1',
    run_status: 'in_progress',
    run_thread_id: 'thread-1',
  },
};

describe('postSSEStream: a duplicate refused in the stream', () => {
  beforeEach(() => {
    vi.stubGlobal('fetch', vi.fn());
  });
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('rejects with the route 409, naming the copy and the thread it runs on', async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(sse([DUPLICATE]));
    const { postSSEStream } = await import('../transport');
    const onEvent = vi.fn();

    await expect(postSSEStream('/api/v1/threads/thread-2/messages', {}, { onEvent })).rejects.toMatchObject({
      status: 409,
      errorInfo: { code: 'duplicate_request', run_id: 'run-1', run_status: 'in_progress', thread_id: 'thread-1' },
    });
    expect(onEvent).not.toHaveBeenCalled();
  });

  it('says so when the copy went into another turn as a steer, and only then', async () => {
    const { postSSEStream } = await import('../transport');
    const refusal = async (frame: { event: string; data: Record<string, unknown> }) => {
      (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(sse([frame]));
      return postSSEStream('/api/v1/threads/thread-1/messages', {}, { onEvent: vi.fn() }).catch((err: { errorInfo: Record<string, unknown> }) => err.errorInfo);
    };

    expect(await refusal({ ...DUPLICATE, data: { ...DUPLICATE.data, steered: true } })).toMatchObject({ run_id: 'run-1', steered: true });
    expect(await refusal(DUPLICATE)).not.toHaveProperty('steered');
  });

  it('passes any other failure frame to the processor', async () => {
    const failure = { event: 'error', data: { thread_id: 'thread-1', error: 'boom', type: 'workflow_error' } };
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(sse([failure]));
    const { postSSEStream } = await import('../transport');
    const onEvent = vi.fn();

    await expect(postSSEStream('/api/v1/threads/thread-1/messages', {}, { onEvent })).resolves.toMatchObject({ aborted: false });
    expect(onEvent).toHaveBeenCalledWith(expect.objectContaining({ event: 'error', error: 'boom' }));
  });
});
