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
  streamWorkspaceEvents,
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
});
