import React from 'react';

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import type { Mock } from 'vitest';
import { renderHook, waitFor, act } from '@testing-library/react';

vi.mock('@/api/client', () => {
  const mockGet = vi.fn().mockResolvedValue({ data: {} });
  const mockPost = vi.fn().mockResolvedValue({ data: {} });
  return {
    api: {
      get: mockGet,
      post: mockPost,
      put: vi.fn(),
      delete: vi.fn(),
      patch: vi.fn(),
      defaults: { baseURL: 'http://localhost:8000' },
    },
  };
});

vi.mock('@/lib/supabase', () => ({ supabase: null }));

import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

import { api } from '@/api/client';
import { queryKeys } from '@/lib/queryKeys';

import { useWarmWorkspaceSandbox } from '../useWarmWorkspaceSandbox';
import { __resetWarmStateForTests } from '../../utils/warmWorkspace';

const mockGet = api.get as Mock;
const mockPost = api.post as Mock;

function wrapper(qc: QueryClient) {
  return function Wrapper({ children }: { children: React.ReactNode }) {
    return <QueryClientProvider client={qc}>{children}</QueryClientProvider>;
  };
}

function makeClient(): QueryClient {
  return new QueryClient({
    defaultOptions: {
      queries: { retry: false, gcTime: 0, staleTime: 0, refetchOnMount: false },
    },
  });
}

interface SSEStreamHandle {
  /** Push an SSE chunk to the consumer. */
  push: (chunk: string) => Promise<void>;
  /** Signal EOF — resolves the consumer's read loop. */
  close: () => Promise<void>;
  /** Capture aborts triggered by useEffect cleanup. */
  aborted: () => boolean;
}

function makeMockSSEStream(): {
  fetchMock: Mock;
  next: () => Promise<SSEStreamHandle>;
} {
  const pending: Array<(h: SSEStreamHandle) => void> = [];
  const handles: SSEStreamHandle[] = [];

  const fetchMock = vi.fn(async (_url: string, init?: RequestInit) => {
    // Re-create per call so consecutive fetches don't share state.
    const encoder = new TextEncoder();
    let pushChunk: ((c: Uint8Array | null) => void) | null = null;
    let waiting: Promise<Uint8Array | null> = new Promise((resolve) => {
      pushChunk = resolve;
    });
    let aborted = false;

    const signal = init?.signal;
    if (signal) {
      const onAbort = () => {
        aborted = true;
        if (pushChunk) {
          const fn = pushChunk;
          pushChunk = null;
          fn(null);
        }
      };
      if (signal.aborted) onAbort();
      else signal.addEventListener('abort', onAbort);
    }

    const stream = new ReadableStream<Uint8Array>({
      async pull(controller) {
        const chunk = await waiting;
        waiting = new Promise((resolve) => {
          pushChunk = resolve;
        });
        if (chunk === null) {
          controller.close();
          return;
        }
        controller.enqueue(chunk);
      },
    });

    const handle: SSEStreamHandle = {
      push: async (chunk: string) => {
        if (pushChunk) {
          const fn = pushChunk;
          pushChunk = null;
          fn(encoder.encode(chunk));
          await Promise.resolve();
        }
      },
      close: async () => {
        if (pushChunk) {
          const fn = pushChunk;
          pushChunk = null;
          fn(null);
          await Promise.resolve();
        }
      },
      aborted: () => aborted,
    };
    handles.push(handle);
    const waiter = pending.shift();
    if (waiter) waiter(handle);

    return new Response(stream, {
      status: 200,
      headers: { 'Content-Type': 'text/event-stream' },
    });
  });

  return {
    fetchMock,
    next: () =>
      new Promise((resolve) => {
        const existing = handles.find((_h, i) => i === fetchMock.mock.calls.length - 1);
        if (existing) resolve(existing);
        else pending.push(resolve);
      }),
  };
}

const originalFetch = global.fetch;

// The hook reads the workspace row; every GET is a workspace's detail.
let detail: Record<string, unknown> = {};
const detailGets = () => mockGet.mock.calls.filter(([url]) => url === '/api/v1/workspaces/ws-1');

beforeEach(() => {
  detail = { workspace_id: 'ws-1', status: 'stopped' };
  mockGet.mockReset().mockImplementation(async () => ({ data: detail }));
  mockPost.mockReset().mockResolvedValue({
    data: { workspace_id: 'ws-1', status: 'starting', message: 'ok' },
  });
  __resetWarmStateForTests();
});

afterEach(() => {
  __resetWarmStateForTests();
  global.fetch = originalFetch;
});

describe('useWarmWorkspaceSandbox', () => {
  it('fires warmWorkspace on mount when workspaceId is set', async () => {
    const qc = makeClient();
    const { fetchMock } = makeMockSSEStream();
    global.fetch = fetchMock as unknown as typeof fetch;

    renderHook(() => useWarmWorkspaceSandbox('ws-1'), {
      wrapper: wrapper(qc),
    });

    await waitFor(() => {
      expect(mockPost).toHaveBeenCalledWith('/api/v1/workspaces/ws-1/start?lazy=true');
    });
  });

  it('no-ops when workspaceId is null', async () => {
    const qc = makeClient();
    const { fetchMock } = makeMockSSEStream();
    global.fetch = fetchMock as unknown as typeof fetch;

    renderHook(() => useWarmWorkspaceSandbox(null), {
      wrapper: wrapper(qc),
    });

    await new Promise((res) => setTimeout(res, 20));
    expect(mockPost).not.toHaveBeenCalled();
    expect(mockGet).not.toHaveBeenCalled();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('warms Home once, as Home, on a cold entry', async () => {
    // Home starts by binding to the user's computer, whatever its row's status
    // ('unbound' here, which a plain warm skips). Nothing is known on arrival,
    // so the hook waits for the row rather than warming it first as a plain
    // workspace and then again as Home.
    detail = { workspace_id: 'ws-1', is_home: true, status: 'unbound' };
    const qc = makeClient();
    const { fetchMock } = makeMockSSEStream();
    global.fetch = fetchMock as unknown as typeof fetch;

    renderHook(() => useWarmWorkspaceSandbox('ws-1'), { wrapper: wrapper(qc) });

    await waitFor(() => expect(mockPost).toHaveBeenCalledWith('/api/v1/workspaces/ws-1/start?lazy=true'));
    await new Promise((res) => setTimeout(res, 20));
    expect(mockPost).toHaveBeenCalledTimes(1);
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it('opens the events SSE stream after mount', async () => {
    const qc = makeClient();
    const { fetchMock } = makeMockSSEStream();
    global.fetch = fetchMock as unknown as typeof fetch;

    renderHook(() => useWarmWorkspaceSandbox('ws-1'), {
      wrapper: wrapper(qc),
    });

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalled();
    });
    const [url] = fetchMock.mock.calls[0];
    expect(url).toContain('/api/v1/workspaces/ws-1/events');
  });

  it('patches detail + list caches when the stream emits a status event', async () => {
    // gcTime: Infinity so the seeded entries aren't garbage-collected between
    // setQueryData and the assertion (the hook reads via getQueryData, not an
    // observer, so a gcTime:0 client would evict them before we can check).
    const qc = new QueryClient({
      defaultOptions: {
        queries: { retry: false, gcTime: Infinity, staleTime: 0, refetchOnMount: false },
      },
    });
    qc.setQueryData(queryKeys.workspaces.detail('ws-1'), {
      workspace_id: 'ws-1',
      status: 'stopped',
    });
    qc.setQueryData(queryKeys.workspaces.list({ limit: 20 }), {
      workspaces: [{ workspace_id: 'ws-1', status: 'stopped', name: 'A' }],
      total: 1,
    });
    const { fetchMock, next } = makeMockSSEStream();
    global.fetch = fetchMock as unknown as typeof fetch;

    renderHook(() => useWarmWorkspaceSandbox('ws-1'), {
      wrapper: wrapper(qc),
    });

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalled();
    });
    const handle = await next();

    await act(async () => {
      await handle.push(
        'event: status\ndata: {"workspace_id":"ws-1","status":"starting"}\n\n',
      );
    });

    // The hook's onStatus pushes each transition into both caches so the
    // gallery and the detail view reflect the warm without a refetch.
    await waitFor(() => {
      const detail = qc.getQueryData<{ status?: string }>(
        queryKeys.workspaces.detail('ws-1'),
      );
      expect(detail?.status).toBe('starting');
    });
    const list = qc.getQueryData<{ workspaces: Array<{ workspace_id: string; status: string }> }>(
      queryKeys.workspaces.list({ limit: 20 }),
    );
    expect(list?.workspaces.find((w) => w.workspace_id === 'ws-1')?.status).toBe('starting');
  });

  it('returns "archived" when the stream emits a sandbox_state refinement', async () => {
    const qc = makeClient();
    const { fetchMock, next } = makeMockSSEStream();
    global.fetch = fetchMock as unknown as typeof fetch;

    const { result } = renderHook(() => useWarmWorkspaceSandbox('ws-1'), {
      wrapper: wrapper(qc),
    });

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalled();
    });
    const handle = await next();

    // Generic starting first, then the archived refinement.
    await act(async () => {
      await handle.push(
        'event: status\ndata: {"workspace_id":"ws-1","status":"starting"}\n\n',
      );
    });
    await waitFor(() => expect(result.current).toBe('starting'));

    await act(async () => {
      await handle.push(
        'event: status\ndata: {"workspace_id":"ws-1","status":"starting","sandbox_state":"archived"}\n\n',
      );
    });
    await waitFor(() => expect(result.current).toBe('archived'));

    // Reaching a terminal status clears the warming spinner.
    await act(async () => {
      await handle.push(
        'event: status\ndata: {"workspace_id":"ws-1","status":"running"}\n\n',
      );
    });
    await waitFor(() => expect(result.current).toBe(false));
  });

  it('asks the server for the status when the stream drops mid-start', async () => {
    // The app's client keeps data fresh for two minutes, and the stream's own
    // 'starting' patch just stamped the entry, so a read that honours that
    // freshness would hand back the stale 'starting' and pin the spinner.
    const qc = new QueryClient({
      defaultOptions: {
        queries: { retry: false, gcTime: Infinity, staleTime: 2 * 60_000, refetchOnMount: false },
      },
    });
    const { fetchMock, next } = makeMockSSEStream();
    global.fetch = fetchMock as unknown as typeof fetch;

    const { result } = renderHook(() => useWarmWorkspaceSandbox('ws-1'), {
      wrapper: wrapper(qc),
    });
    await waitFor(() => expect(mockPost).toHaveBeenCalled());
    const handle = await next();
    await act(async () => {
      await handle.push(
        'event: status\ndata: {"workspace_id":"ws-1","status":"starting"}\n\n',
      );
    });
    await waitFor(() => expect(result.current).toBe('starting'));
    detail = { workspace_id: 'ws-1', status: 'running' };

    await act(async () => {
      await handle.close();
    });

    await waitFor(() => expect(result.current).toBe(false));
    expect(detailGets()).toHaveLength(2);
  });

  it("reconciles Home's status from the server when its stream drops, like any workspace", async () => {
    detail = { workspace_id: 'ws-1', is_home: true, computer_id: 'comp-1', status: 'stopped' };
    const qc = new QueryClient({
      defaultOptions: {
        queries: { retry: false, gcTime: Infinity, staleTime: 2 * 60_000, refetchOnMount: false },
      },
    });
    const { fetchMock, next } = makeMockSSEStream();
    global.fetch = fetchMock as unknown as typeof fetch;

    const { result } = renderHook(() => useWarmWorkspaceSandbox('ws-1'), {
      wrapper: wrapper(qc),
    });
    await waitFor(() => expect(mockPost).toHaveBeenCalled());
    const handle = await next();
    await act(async () => {
      await handle.push(
        'event: status\ndata: {"workspace_id":"ws-1","status":"starting"}\n\n',
      );
    });
    await waitFor(() => expect(result.current).toBe('starting'));
    detail = { ...detail, status: 'running' };

    await act(async () => {
      await handle.close();
    });

    await waitFor(() => expect(result.current).toBe(false));
    expect(detailGets()).toHaveLength(2);
    expect(qc.getQueryData<{ status?: string }>(queryKeys.workspaces.detail('ws-1'))?.status)
      .toBe('running');
  });

  it('aborts the stream on unmount', async () => {
    const qc = makeClient();
    let capturedSignal: AbortSignal | null = null;
    const fetchMock = vi.fn(async (_url: string, init?: RequestInit) => {
      capturedSignal = init?.signal ?? null;
      // Return a never-completing stream so we can verify the abort.
      const stream = new ReadableStream<Uint8Array>({});
      return new Response(stream, {
        status: 200,
        headers: { 'Content-Type': 'text/event-stream' },
      });
    });
    global.fetch = fetchMock as unknown as typeof fetch;

    const { unmount } = renderHook(() => useWarmWorkspaceSandbox('ws-1'), {
      wrapper: wrapper(qc),
    });

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalled();
    });

    // Cast on read: TS control-flow-narrows a closure-assigned `let` to
    // `never` here, so the union annotation is reasserted at the use site.
    expect((capturedSignal as AbortSignal | null)?.aborted).toBe(false);
    act(() => unmount());
    expect((capturedSignal as AbortSignal | null)?.aborted).toBe(true);
  });
});
