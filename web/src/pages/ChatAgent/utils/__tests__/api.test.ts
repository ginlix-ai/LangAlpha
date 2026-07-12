import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import type { Mock } from 'vitest';

vi.mock('@/api/client', () => {
  const mockGet = vi.fn().mockResolvedValue({ data: {} });
  const mockPost = vi.fn().mockResolvedValue({ data: {} });
  const mockPut = vi.fn().mockResolvedValue({ data: {} });
  const mockDelete = vi.fn().mockResolvedValue({ data: {} });
  const mockPatch = vi.fn().mockResolvedValue({ data: {} });
  return {
    api: {
      get: mockGet,
      post: mockPost,
      put: mockPut,
      delete: mockDelete,
      patch: mockPatch,
      defaults: { baseURL: 'http://localhost:8000' },
    },
  };
});

vi.mock('@/lib/supabase', () => ({
  supabase: null,
}));

import { api } from '@/api/client';
import {
  getWorkspaces,
  createWorkspace,
  deleteWorkspace,
  getWorkspace,
  getThread,
  deleteThread,
  sendHitlResponse,
  sendChatMessageStream,
  fetchMarketWatch,
  streamWorkspaceEvents,
  watchThread,
  reconnectToWorkflowStream,
  getReportBackStatus,
  getDispatchLiveness,
  // Re-exported by the API boundary from ../reportBackSignal — importing them here
  // pins that the boundary surface exposes the decoded signal.
  decodeReportBackSignal,
  shouldArmReportBack,
  formatApiErrorDetail,
  apiErrorDetailMessage,
} from '../api';

const mockGet = api.get as Mock;
const mockPost = api.post as Mock;
const mockDelete = api.delete as Mock;

describe('ChatAgent API utilities', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('getWorkspaces', () => {
    it('calls api.get with default params', async () => {
      const mockData = { workspaces: [], total: 0 };
      mockGet.mockResolvedValue({ data: mockData });

      const result = await getWorkspaces();
      expect(mockGet).toHaveBeenCalledWith('/api/v1/workspaces', {
        params: { limit: 20, offset: 0, sort_by: 'custom' },
      });
      expect(result).toEqual(mockData);
    });

    it('passes custom limit, offset, and sortBy', async () => {
      mockGet.mockResolvedValue({ data: {} });

      await getWorkspaces(10, 5, 'name');
      expect(mockGet).toHaveBeenCalledWith('/api/v1/workspaces', {
        params: { limit: 10, offset: 5, sort_by: 'name' },
      });
    });
  });

  describe('createWorkspace', () => {
    it('posts workspace data and returns response', async () => {
      const mockWs = { workspace_id: 'ws-new', name: 'My Workspace' };
      mockPost.mockResolvedValue({ data: mockWs });

      const result = await createWorkspace('My Workspace', 'desc', { mode: 'ptc' });
      expect(mockPost).toHaveBeenCalledWith('/api/v1/workspaces', {
        name: 'My Workspace',
        description: 'desc',
        config: { mode: 'ptc' },
      });
      expect(result).toEqual(mockWs);
    });
  });

  describe('deleteWorkspace', () => {
    it('throws when workspaceId is falsy', async () => {
      await expect(deleteWorkspace(null as unknown as string)).rejects.toThrow('Workspace ID is required');
      await expect(deleteWorkspace('')).rejects.toThrow('Workspace ID is required');
    });

    it('calls api.delete with trimmed workspace id', async () => {
      mockDelete.mockResolvedValue({});

      await deleteWorkspace('  ws-123  ');
      expect(mockDelete).toHaveBeenCalledWith('/api/v1/workspaces/ws-123');
    });
  });

  describe('getWorkspace', () => {
    it('throws when workspaceId is falsy', async () => {
      await expect(getWorkspace(null as unknown as string)).rejects.toThrow('Workspace ID is required');
    });

    it('returns workspace data', async () => {
      const mockWs = { workspace_id: 'ws-1', name: 'Test' };
      mockGet.mockResolvedValue({ data: mockWs });

      const result = await getWorkspace('ws-1');
      expect(result).toEqual(mockWs);
    });
  });

  describe('getThread', () => {
    it('throws when threadId is falsy', async () => {
      await expect(getThread(null as unknown as string)).rejects.toThrow('Thread ID is required');
    });

    it('fetches thread by id', async () => {
      const mockThread = { thread_id: 't-1', title: 'Thread 1' };
      mockGet.mockResolvedValue({ data: mockThread });

      const result = await getThread('t-1');
      expect(mockGet).toHaveBeenCalledWith('/api/v1/threads/t-1');
      expect(result).toEqual(mockThread);
    });
  });

  describe('deleteThread', () => {
    it('throws when threadId is falsy', async () => {
      await expect(deleteThread(null as unknown as string)).rejects.toThrow('Thread ID is required');
    });

    it('calls api.delete and returns response data', async () => {
      const mockResp = { success: true, thread_id: 't-1' };
      mockDelete.mockResolvedValue({ data: mockResp });

      const result = await deleteThread('t-1');
      expect(mockDelete).toHaveBeenCalledWith('/api/v1/threads/t-1');
      expect(result).toEqual(mockResp);
    });
  });

  describe('sendHitlResponse', () => {
    let originalFetch: typeof global.fetch;

    beforeEach(() => {
      originalFetch = global.fetch;
      // Mock fetch to return a readable stream that ends immediately
      global.fetch = vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        headers: new Headers(),
        body: {
          getReader: () => ({
            read: vi.fn().mockResolvedValue({ done: true, value: undefined }),
          }),
        },
      });
    });

    afterEach(() => {
      global.fetch = originalFetch;
    });

    it('includes agent_mode in request body defaulting to ptc', async () => {
      await sendHitlResponse('ws-1', 't-1', { int1: { decisions: [{ type: 'approve' }] } }, () => {});

      const fetchMock = global.fetch as Mock;
      expect(fetchMock).toHaveBeenCalledTimes(1);
      const [, opts] = fetchMock.mock.calls[0];
      const body = JSON.parse(opts.body);
      expect(body.agent_mode).toBe('ptc');
    });

    it('passes custom agentMode', async () => {
      await sendHitlResponse(
        'ws-1', 't-1',
        { int1: { decisions: [{ type: 'approve' }] } },
        () => {},
        false,
        {},
        'flash',
      );

      const fetchMock = global.fetch as Mock;
      const [, opts] = fetchMock.mock.calls[0];
      const body = JSON.parse(opts.body);
      expect(body.agent_mode).toBe('flash');
    });

    it('includes model options when provided', async () => {
      await sendHitlResponse(
        'ws-1', 't-1',
        { int1: { decisions: [{ type: 'approve' }] } },
        () => {},
        false,
        { model: 'gpt-4o', reasoningEffort: 'high', fastMode: true },
      );

      const fetchMock = global.fetch as Mock;
      const [, opts] = fetchMock.mock.calls[0];
      const body = JSON.parse(opts.body);
      expect(body.llm_model).toBe('gpt-4o');
      expect(body.reasoning_effort).toBe('high');
      expect(body.fast_mode).toBe(true);
    });

    it('invokes onRunIdResolved with the run_id from Content-Location BEFORE reading the body', async () => {
      // Track ordering: did onRunIdResolved fire before any reader.read() call?
      const callOrder: string[] = [];
      const readMock = vi.fn().mockImplementation(() => {
        callOrder.push('body-read');
        return Promise.resolve({ done: true, value: undefined });
      });
      global.fetch = vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        headers: new Headers({
          'Content-Location': '/api/v1/threads/t-1/messages/stream?run_id=abc-123',
        }),
        body: {
          getReader: () => ({ read: readMock }),
        },
      });

      const onRunIdResolved = vi.fn().mockImplementation((rid: string) => {
        callOrder.push(`run-id:${rid}`);
      });

      await sendHitlResponse(
        'ws-1', 't-1',
        { int1: { decisions: [{ type: 'approve' }] } },
        () => {},
        false,
        {},
        'ptc',
        onRunIdResolved,
      );

      expect(onRunIdResolved).toHaveBeenCalledTimes(1);
      // Now also carries the server-assigned thread_id parsed from the same
      // Content-Location path, so an early stop can hard-cancel the run.
      expect(onRunIdResolved).toHaveBeenCalledWith('abc-123', 't-1');
      // The run_id MUST be latched before any body byte is read.
      expect(callOrder[0]).toBe('run-id:abc-123');
    });

    it('does NOT invoke onRunIdResolved when Content-Location lacks run_id', async () => {
      global.fetch = vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        headers: new Headers({ 'Content-Location': '/api/v1/threads/t-1/messages/stream' }),
        body: {
          getReader: () => ({ read: vi.fn().mockResolvedValue({ done: true, value: undefined }) }),
        },
      });

      const onRunIdResolved = vi.fn();
      await sendHitlResponse(
        'ws-1', 't-1',
        { int1: { decisions: [{ type: 'approve' }] } },
        () => {},
        false,
        {},
        'ptc',
        onRunIdResolved,
      );
      expect(onRunIdResolved).not.toHaveBeenCalled();
    });
  });

  describe('streamWorkspaceEvents', () => {
    let originalFetch: typeof global.fetch;

    beforeEach(() => {
      originalFetch = global.fetch;
    });

    afterEach(() => {
      global.fetch = originalFetch;
    });

    /** Build a fetch mock whose body streams the given SSE text chunks. */
    function mockSSEResponse(chunks: string[], { ok = true } = {}) {
      const encoder = new TextEncoder();
      const queue = [...chunks];
      const reader = {
        read: vi.fn(async () => {
          if (queue.length === 0) return { done: true, value: undefined };
          return { done: false, value: encoder.encode(queue.shift()!) };
        }),
        cancel: vi.fn(async () => {}),
      };
      const fetchMock = vi.fn().mockResolvedValue({
        ok,
        status: ok ? 200 : 503,
        body: ok ? { getReader: () => reader } : null,
      });
      global.fetch = fetchMock as unknown as typeof fetch;
      return { fetchMock, reader };
    }

    const ctrl = () => new AbortController().signal;

    it('parses a status event and passes status + sandbox_state', async () => {
      mockSSEResponse([
        'event: status\ndata: {"workspace_id":"ws-1","status":"starting","sandbox_state":"archived"}\n\n',
      ]);
      const onStatus = vi.fn();
      await streamWorkspaceEvents('ws-1', onStatus, ctrl());
      expect(onStatus).toHaveBeenCalledWith('starting', 'archived');
    });

    it('omits sandbox_state when the payload has none', async () => {
      mockSSEResponse([
        'event: status\ndata: {"workspace_id":"ws-1","status":"running"}\n\n',
      ]);
      const onStatus = vi.fn();
      await streamWorkspaceEvents('ws-1', onStatus, ctrl());
      expect(onStatus).toHaveBeenCalledWith('running', undefined);
    });

    it('handles an event split across read() chunks', async () => {
      // The "data:" line arrives in a separate read than "event:".
      mockSSEResponse([
        'event: status\n',
        'data: {"status":"starting"}\n\n',
      ]);
      const onStatus = vi.fn();
      await streamWorkspaceEvents('ws-1', onStatus, ctrl());
      expect(onStatus).toHaveBeenCalledWith('starting', undefined);
    });

    it('stops on a timeout event without emitting further statuses', async () => {
      mockSSEResponse([
        'event: status\ndata: {"status":"starting"}\n\n',
        'event: timeout\ndata: {}\n\n',
        'event: status\ndata: {"status":"running"}\n\n',
      ]);
      const onStatus = vi.fn();
      await streamWorkspaceEvents('ws-1', onStatus, ctrl());
      expect(onStatus).toHaveBeenCalledTimes(1);
      expect(onStatus).toHaveBeenCalledWith('starting', undefined);
    });

    it('skips a malformed payload but keeps consuming the stream', async () => {
      mockSSEResponse([
        'event: status\ndata: {not json}\n\n',
        'event: status\ndata: {"status":"running"}\n\n',
      ]);
      const onStatus = vi.fn();
      await streamWorkspaceEvents('ws-1', onStatus, ctrl());
      expect(onStatus).toHaveBeenCalledTimes(1);
      expect(onStatus).toHaveBeenCalledWith('running', undefined);
    });

    it('returns without emitting when the response is not ok', async () => {
      mockSSEResponse(['event: status\ndata: {"status":"running"}\n\n'], { ok: false });
      const onStatus = vi.fn();
      await streamWorkspaceEvents('ws-1', onStatus, ctrl());
      expect(onStatus).not.toHaveBeenCalled();
    });

    it('no-ops with an empty workspace id and never fetches', async () => {
      const fetchMock = vi.fn();
      global.fetch = fetchMock as unknown as typeof fetch;
      const onStatus = vi.fn();
      await streamWorkspaceEvents('', onStatus, ctrl());
      expect(fetchMock).not.toHaveBeenCalled();
      expect(onStatus).not.toHaveBeenCalled();
    });

    it('swallows a network error and resolves (best-effort)', async () => {
      global.fetch = vi.fn().mockRejectedValue(new Error('network down')) as unknown as typeof fetch;
      const onStatus = vi.fn();
      await expect(
        streamWorkspaceEvents('ws-1', onStatus, ctrl()),
      ).resolves.toBeUndefined();
      expect(onStatus).not.toHaveBeenCalled();
    });

    it('cancels the reader on close to release the stream', async () => {
      const { reader } = mockSSEResponse([
        'event: status\ndata: {"status":"running"}\n\n',
      ]);
      await streamWorkspaceEvents('ws-1', vi.fn(), ctrl());
      expect(reader.cancel).toHaveBeenCalled();
    });
  });

  describe('watchThread', () => {
    let originalFetch: typeof global.fetch;

    beforeEach(() => {
      originalFetch = global.fetch;
    });

    afterEach(() => {
      global.fetch = originalFetch;
    });

    /** Build a fetch mock whose /watch body streams the given SSE text chunks. */
    function mockWatchResponse(chunks: string[], { ok = true } = {}) {
      const encoder = new TextEncoder();
      const queue = [...chunks];
      const reader = {
        read: vi.fn(async () => {
          if (queue.length === 0) return { done: true, value: undefined };
          return { done: false, value: encoder.encode(queue.shift()!) };
        }),
        cancel: vi.fn(async () => {}),
      };
      const fetchMock = vi.fn().mockResolvedValue({
        ok,
        status: ok ? 200 : 503,
        body: ok ? { getReader: () => reader } : null,
      });
      global.fetch = fetchMock as unknown as typeof fetch;
      return { fetchMock, reader };
    }

    /** Run watchThread and resolve with the payload it reports (one-shot). */
    function runWatch(): Promise<{ run_id?: string | null } | undefined> {
      return new Promise((resolve) => {
        watchThread('flash-1', resolve);
      });
    }

    it('parses the run_id from a wake delivered as a single frame', async () => {
      mockWatchResponse([
        'event: workflow_started\ndata: {"thread_id":"flash-1","run_id":"rb-1"}\n\n',
      ]);
      expect(await runWatch()).toEqual({ run_id: 'rb-1', needs_input: null });
    });

    it('parses the run_id when the data line arrives in a later read() chunk', async () => {
      // Regression: the wake frame is split mid-`data:` across reads. Reacting on
      // first sight of the event name parsed partial JSON and lost the run_id,
      // forcing a /status fallback that a fast report-back has already torn down.
      mockWatchResponse([
        'event: workflow_started\ndata: {"thread_id":"flash-1","ru',
        'n_id":"rb-1"}\n\n',
      ]);
      expect(await runWatch()).toEqual({ run_id: 'rb-1', needs_input: null });
    });

    it('skips keepalive pings before reporting the wake run_id', async () => {
      mockWatchResponse([
        ': ping\n\n',
        ': ping\n\n',
        'event: workflow_started\ndata: {"run_id":"rb-9"}\n\n',
      ]);
      expect(await runWatch()).toEqual({ run_id: 'rb-9', needs_input: null });
    });

    it('reports a null run_id for a malformed wake (caller falls back to /status)', async () => {
      mockWatchResponse(['event: workflow_started\ndata: {not json}\n\n']);
      expect(await runWatch()).toEqual({ run_id: null, needs_input: null });
    });

    it('joins multiple data: lines per the SSE spec before parsing', async () => {
      // A wake whose JSON spans multiple data: lines must concatenate with a
      // newline (SSE spec). The old single-line regex captured only the first
      // line, truncating the JSON and forcing a /status fallback.
      mockWatchResponse([
        'event: workflow_started\ndata: {"thread_id":"flash-1",\ndata: "run_id":"rb-7"}\n\n',
      ]);
      expect(await runWatch()).toEqual({ run_id: 'rb-7', needs_input: null });
    });

    it('keeps reading and delivers EVERY wake on one persistent connection (no cancel mid-stream)', async () => {
      // A flash thread can dispatch N PTCs whose report-backs arrive as separate
      // runs. The watch must forward every wake on a single connection — the old
      // one-shot behavior (cancel + return after wake #1) dropped wake #2+, so
      // only the first report-back streamed and the rest needed a page refresh.
      const { reader } = mockWatchResponse([
        'event: workflow_started\ndata: {"run_id":"rb-1"}\n\n',
        'event: workflow_started\ndata: {"run_id":"rb-2"}\n\n',
      ]);
      const payloads: Array<{ run_id?: string | null } | undefined> = [];
      const onResubscribed = vi.fn();
      await new Promise<void>((resolve) => {
        // onClosed (3rd arg) fires when the backend ends the stream — resolve then.
        watchThread('flash-1', (p) => { payloads.push(p); }, resolve, onResubscribed);
      });
      expect(payloads).toEqual([
        { run_id: 'rb-1', needs_input: null },
        { run_id: 'rb-2', needs_input: null },
      ]);
      expect(reader.cancel).not.toHaveBeenCalled();
      // The initial (non-retry) subscription is not a recovery.
      expect(onResubscribed).not.toHaveBeenCalled();
    });

    it('invokes onClosed when the backend closes the stream without a deliberate abort', async () => {
      // A backend-side close (30-min cap / drop) must signal the caller so it can
      // null its abort ref and let a future re-arm re-subscribe a fresh watch.
      mockWatchResponse(['event: workflow_started\ndata: {"run_id":"rb-1"}\n\n']);
      const onClosed = vi.fn();
      await new Promise<void>((resolve) => {
        watchThread('flash-1', () => {}, () => { onClosed(); resolve(); });
      });
      expect(onClosed).toHaveBeenCalledTimes(1);
    });

    it('does NOT invoke onClosed after a caller-initiated abort', async () => {
      // A deliberate teardown (stopReportBackWatch .abort()) already cleaned up;
      // firing onClosed there would clobber a freshly re-armed watch's abort ref.
      let rejectRead: ((e: unknown) => void) | null = null;
      const reader = {
        read: vi.fn(() => new Promise((_res, rej) => { rejectRead = rej; })),
        cancel: vi.fn(async () => {}),
      };
      global.fetch = vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        body: { getReader: () => reader },
      }) as unknown as typeof fetch;

      const onClosed = vi.fn();
      const { abort } = watchThread('flash-1', () => {}, onClosed);
      // Simulate fetch: aborting the signal rejects the in-flight read() with
      // an AbortError, which is exactly how the watch loop unwinds on teardown.
      abort.signal.addEventListener('abort', () =>
        rejectRead?.(Object.assign(new Error('aborted'), { name: 'AbortError' })),
      );
      // Let fetch resolve and the first read() begin before we abort.
      await Promise.resolve();
      await Promise.resolve();
      abort.abort();
      await new Promise((r) => setTimeout(r, 0));
      expect(onClosed).not.toHaveBeenCalled();
    });

    it('fires onResubscribed when the in-loop retry lands a fresh subscription, without double-firing onClosed', async () => {
      // A transient error drops the connection; the internal retry re-subscribes.
      // Wakes published during that gap are lost (pub/sub, no replay), so the
      // caller must be told to run a catch-up — that is onResubscribed. It is a
      // recovery signal, NOT a close: onClosed still fires exactly once, at the
      // eventual final close.
      vi.useFakeTimers();
      try {
        const encoder = new TextEncoder();
        const chunks = ['event: workflow_started\ndata: {"run_id":"rb-after-gap"}\n\n'];
        const reader = {
          read: vi.fn(async () =>
            chunks.length
              ? { done: false, value: encoder.encode(chunks.shift()!) }
              : { done: true, value: undefined },
          ),
          cancel: vi.fn(async () => {}),
        };
        global.fetch = vi
          .fn()
          .mockRejectedValueOnce(new TypeError('network dropped'))
          .mockResolvedValue({ ok: true, status: 200, body: { getReader: () => reader } }) as unknown as typeof fetch;

        const onResubscribed = vi.fn();
        const onClosed = vi.fn();
        const payloads: Array<{ run_id?: string | null } | undefined> = [];
        const done = new Promise<void>((resolve) => {
          watchThread(
            'flash-1',
            (p) => { payloads.push(p); },
            () => { onClosed(); resolve(); },
            onResubscribed,
          );
        });
        // Attempt 0 rejects → the loop backs off 1s → attempt 1 re-subscribes.
        await vi.advanceTimersByTimeAsync(1000);
        await done;

        expect(onResubscribed).toHaveBeenCalledTimes(1);
        // The wake buffered on the fresh connection was still delivered.
        expect(payloads).toEqual([{ run_id: 'rb-after-gap', needs_input: null }]);
        // Final close signalled exactly once — the recovery didn't double-fire it.
        expect(onClosed).toHaveBeenCalledTimes(1);
      } finally {
        vi.useRealTimers();
      }
    });

    it('does NOT fire onResubscribed for a hard-failing endpoint (non-ok response is a final close, not a recovery)', async () => {
      // A non-ok response returns immediately (no retry loop): there is no fresh
      // subscription to catch up from, so signalling a recovery would make the
      // caller reconcile against a dead endpoint. onClosed still fires once.
      mockWatchResponse([], { ok: false });
      const onResubscribed = vi.fn();
      const onClosed = vi.fn();
      await new Promise<void>((resolve) => {
        watchThread('flash-1', () => {}, () => { onClosed(); resolve(); }, onResubscribed);
      });
      expect(onResubscribed).not.toHaveBeenCalled();
      expect(onClosed).toHaveBeenCalledTimes(1);
    });
  });

  describe('sendChatMessageStream — market-watch skill via additional_context', () => {
    // The market_watch body flag was retired: the Watch toggle now rides as a
    // `market-watch` skills item in additional_context (ChatView appends it).
    // These pin the send-path body contract — the skill item is forwarded
    // verbatim, and no market_watch key is ever emitted.
    let originalFetch: typeof global.fetch;

    beforeEach(() => {
      originalFetch = global.fetch;
      global.fetch = vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        headers: new Headers(),
        body: {
          getReader: () => ({
            read: vi.fn().mockResolvedValue({ done: true, value: undefined }),
          }),
        },
      });
    });

    afterEach(() => {
      global.fetch = originalFetch;
    });

    const bodyOfLastSend = () => {
      const fetchMock = global.fetch as Mock;
      const [, opts] = fetchMock.mock.calls[0];
      return JSON.parse(opts.body) as Record<string, unknown>;
    };

    // Watch ON: additional_context carries the market-watch skills item.
    it('forwards the market-watch skills item and sets no market_watch flag', async () => {
      const skillItem = {
        type: 'skills',
        name: 'market-watch',
        instruction:
          'Market watch mode is on for this message. If the central tickers are not yet registered, register them with watch_market.',
      };
      await sendChatMessageStream(
        'hi', 'ws-1', 't-1', [], false, () => {}, [skillItem], 'ptc',
      );

      const body = bodyOfLastSend();
      expect(body.market_watch).toBeUndefined();
      const item = (body.additional_context as Array<Record<string, unknown>>).find(
        (c) => c.type === 'skills' && c.name === 'market-watch',
      );
      expect(item).toBeDefined();
      expect(item!.instruction).toBe(skillItem.instruction);
    });

    // Watch OFF: no skills item, no market_watch key.
    it('sends no market-watch skill and no market_watch flag when off', async () => {
      await sendChatMessageStream('hi', 'ws-1', 't-1', [], false, () => {});

      const body = bodyOfLastSend();
      expect(body.market_watch).toBeUndefined();
      expect(body.additional_context).toBeUndefined();
    });
  });

  describe('fetchMarketWatch', () => {
    it('throws when threadId is falsy', async () => {
      await expect(fetchMarketWatch('')).rejects.toThrow('Thread ID is required');
    });

    it('GETs the thread market-watch endpoint and returns the payload', async () => {
      const payload = { thread_id: 't-1', symbols: ['NVDA', 'TSLA'] };
      mockGet.mockResolvedValue({ data: payload });

      const result = await fetchMarketWatch('t-1');
      expect(mockGet).toHaveBeenCalledWith('/api/v1/threads/t-1/market-watch');
      expect(result).toEqual(payload);
    });
  });

  describe('reconnectToWorkflowStream transport-error classification (Prong A)', () => {
    let originalFetch: typeof global.fetch;

    beforeEach(() => {
      originalFetch = global.fetch;
    });

    afterEach(() => {
      global.fetch = originalFetch;
    });

    /** Build a fetch mock whose reader.read() rejects with the given error. */
    function mockRejectingReader(error: Error) {
      const reader = {
        read: vi.fn().mockRejectedValue(error),
        cancel: vi.fn(async () => {}),
      };
      const fetchMock = vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        headers: new Headers(),
        body: { getReader: () => reader },
      });
      global.fetch = fetchMock as unknown as typeof fetch;
      return { fetchMock, reader };
    }

    it('classifies a TypeError("Load failed") read rejection as a disconnect (iOS Safari background-kill)', async () => {
      // The fail-first proof: "Load failed" contains no "network" substring, so
      // the old classifier re-threw it (error banner, no reconnect). After Prong
      // A, any TypeError out of the read loop resolves as a disconnect.
      mockRejectingReader(new TypeError('Load failed'));
      const result = await reconnectToWorkflowStream('t-1');
      expect(result.disconnected).toBe(true);
      expect(result.aborted).toBe(false);
    });

    it('classifies a TypeError("The network connection was lost.") read rejection as a disconnect', async () => {
      // Forward-looking guard for the other iOS Safari string. (This literal
      // already contains "network", so the old classifier handled it too; this
      // test pins it down regardless of the substring.)
      mockRejectingReader(new TypeError('The network connection was lost.'));
      const result = await reconnectToWorkflowStream('t-1');
      expect(result.disconnected).toBe(true);
      expect(result.aborted).toBe(false);
    });
  });

  describe('report-back signal decoding', () => {
    // The backend's `pending_report_back` is a TRI-STATE wire value; the decoder
    // is the single place it's converted into an explicit domain signal so raw
    // `boolean | null | undefined` never reaches UI control flow.
    it('decodeReportBackSignal maps every wire value to its signal', () => {
      expect(decodeReportBackSignal(true)).toBe('pending');
      expect(decodeReportBackSignal(false)).toBe('idle');
      expect(decodeReportBackSignal(null)).toBe('unknown');
      expect(decodeReportBackSignal(undefined)).toBe('none');
    });

    it('shouldArmReportBack arms on pending|unknown, not on idle|none (arm↔drain asymmetry)', () => {
      // Arm on an explicit pending AND on unknown (the backend's own Redis read
      // failed — keep watching), but never on a drained `idle` or an absent `none`.
      expect(shouldArmReportBack('pending')).toBe(true);
      expect(shouldArmReportBack('unknown')).toBe(true);
      expect(shouldArmReportBack('idle')).toBe(false);
      expect(shouldArmReportBack('none')).toBe(false);
    });

    it('getReportBackStatus returns the raw report-back slice for the decoder', async () => {
      mockGet.mockResolvedValueOnce({
        data: { thread_id: 't-1', pending_report_back: null, report_back_run_id: 'rb-1' },
      });
      const res = await getReportBackStatus('t-1');
      expect(mockGet).toHaveBeenCalledWith('/api/v1/threads/t-1/status', {
        params: { fields: 'report_back' },
      });
      expect(decodeReportBackSignal(res.pending_report_back)).toBe('unknown');
      expect(res.report_back_run_id).toBe('rb-1');
    });
  });

  describe('getDispatchLiveness', () => {
    it('resolves to [] for an empty id list WITHOUT issuing a request', async () => {
      const res = await getDispatchLiveness([]);
      expect(res).toEqual([]);
      expect(mockGet).not.toHaveBeenCalled();
    });

    it('resolves to [] when the response body omits `liveness`, joining ids comma-separated', async () => {
      // Null-safe extraction: a body without the `liveness` key must not throw.
      mockGet.mockResolvedValueOnce({ data: {} });
      const res = await getDispatchLiveness(['t-1', 't-2']);
      expect(res).toEqual([]);
      expect(mockGet).toHaveBeenCalledWith('/api/v1/threads/dispatches/liveness', {
        params: { ids: 't-1,t-2' },
      });
    });
  });

  describe('apiErrorDetailMessage', () => {
    it('returns the message from a structured object detail (platform 429 quota)', () => {
      const err = {
        response: {
          data: {
            detail: { message: 'Performance workspace limit reached', type: 'spec_performance', current: 2, limit: 2, remaining: 0 },
          },
        },
      };
      expect(apiErrorDetailMessage(err)).toBe('Performance workspace limit reached');
    });

    it('returns null for string, array, or absent details', () => {
      expect(apiErrorDetailMessage({ response: { data: { detail: 'plain string' } } })).toBeNull();
      expect(apiErrorDetailMessage({ response: { data: { detail: [{ loc: ['body'], msg: 'bad' }] } } })).toBeNull();
      expect(apiErrorDetailMessage({ response: { data: {} } })).toBeNull();
      expect(apiErrorDetailMessage(new Error('boom'))).toBeNull();
    });

    it('returns null when the object detail has no string message', () => {
      expect(apiErrorDetailMessage({ response: { data: { detail: { type: 'spec_max' } } } })).toBeNull();
    });
  });

  describe('formatApiErrorDetail', () => {
    it('surfaces a structured object detail message', () => {
      const err = { response: { data: { detail: { message: 'Quota exhausted', limit: 0 } } } };
      expect(formatApiErrorDetail(err)).toBe('Quota exhausted');
    });

    it('flattens FastAPI validation arrays', () => {
      const err = { response: { data: { detail: [{ loc: ['body', 'tier'], msg: 'invalid' }] } } };
      expect(formatApiErrorDetail(err)).toBe('body.tier: invalid');
    });

    it('passes through a plain string detail', () => {
      expect(formatApiErrorDetail({ response: { data: { detail: 'nope' } } })).toBe('nope');
    });

    it('falls back to err.message then a generic label', () => {
      expect(formatApiErrorDetail(new Error('network'))).toBe('network');
      expect(formatApiErrorDetail({})).toBe('Request failed');
    });
  });
});
