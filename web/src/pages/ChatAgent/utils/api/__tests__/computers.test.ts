/**
 * Computer client, pinned against payloads captured from the running backend
 * (wt3, :8060) rather than invented: the list and create bodies below are the
 * literal responses, and the SSE frames are what `/computers/{id}/events`
 * actually wrote on the wire.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';

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

vi.mock('@/lib/authToken', () => ({
  getAuthHeaders: vi.fn(async () => ({})),
  getAccessToken: vi.fn(async () => null),
}));

import { api } from '@/api/client';
import {
  createComputer,
  getComputers,
  startComputer,
  stopComputer,
  streamComputerEvents,
} from '../computers';

const mockGet = api.get as unknown as Mock;
const mockPost = api.post as unknown as Mock;

// GET /api/v1/computers, captured verbatim for user wp13-final-1789525377.
const LIVE_LIST = {
  computers: [
    {
      computer_id: '5319ad0e-7835-4ca6-9541-b7c2961a7bf6',
      user_id: 'wp13-final-1789525377',
      kind: 'daytona',
      name: 'Alpha Research',
      status: 'running',
      resource_tier: 'standard',
      is_always_on: false,
      is_primary: true,
      root_dir: '/home/workspace',
      provider_ref: '91885905-79ed-48dc-81ac-0a3d2d608458',
      created_at: '2026-09-16T02:22:57.917242Z',
      updated_at: '2026-09-16T02:23:22.280103Z',
      last_activity_at: '2026-09-16T02:23:22.280103Z',
      stopped_at: null,
      config: {},
    },
    {
      computer_id: '3b788b47-352e-4cfb-94f0-920b599ea227',
      user_id: 'wp13-final-1789525377',
      kind: 'daytona',
      name: 'Second machine',
      status: 'stopped',
      resource_tier: 'standard',
      is_always_on: false,
      is_primary: false,
      root_dir: '/home/workspace',
      provider_ref: null,
      created_at: '2026-09-16T02:23:59.852198Z',
      updated_at: '2026-09-16T02:23:59.852198Z',
      last_activity_at: null,
      stopped_at: null,
      config: {},
    },
  ],
  total: 2,
};

beforeEach(() => {
  vi.clearAllMocks();
});

describe('getComputers', () => {
  it('reads the list route and returns the payload shape as-is', async () => {
    mockGet.mockResolvedValueOnce({ data: LIVE_LIST });
    const res = await getComputers();
    expect(mockGet).toHaveBeenCalledWith('/api/v1/computers');
    expect(res.total).toBe(2);
    expect(res.computers[0].is_primary).toBe(true);
    expect(res.computers[1].provider_ref).toBeNull();
  });
});

describe('createComputer', () => {
  it('posts the tier, which is the machine\'s property and not a workspace\'s', async () => {
    // The live 201 body for {"name":"WP16 probe","resource_tier":"performance"}.
    mockPost.mockResolvedValueOnce({
      data: {
        computer_id: 'ebbab1a6-16fe-46d6-8ae9-08080cd7e084',
        user_id: 'wp13-final-1789525377',
        kind: 'daytona',
        name: 'WP16 probe',
        status: 'stopped',
        resource_tier: 'performance',
        is_always_on: false,
        is_primary: false,
        root_dir: '/home/workspace',
        provider_ref: null,
        config: {},
      },
    });
    const computer = await createComputer({ name: 'WP16 probe', resource_tier: 'performance' });
    expect(mockPost).toHaveBeenCalledWith(
      '/api/v1/computers',
      { name: 'WP16 probe', resource_tier: 'performance' },
      expect.objectContaining({ timeout: expect.any(Number) }),
    );
    // A computer is born stopped, so nothing in the UI may wait on 'creating'.
    expect(computer.status).toBe('stopped');
    expect(computer.resource_tier).toBe('performance');
  });

  it('defaults to standard and omits an absent name so the server names it', async () => {
    mockPost.mockResolvedValueOnce({ data: {} });
    await createComputer();
    expect(mockPost.mock.calls[0][1]).toEqual({ resource_tier: 'standard' });
  });

  it('lets a 429 through with the interceptor-attached quota body intact', async () => {
    const denial = Object.assign(new Error('Request failed'), {
      status: 429,
      rateLimitInfo: {
        message: 'You have reached the 3 workspace limit on the Starter plan.',
        type: 'workspace_limit',
        current: 3,
        limit: 3,
        remaining: 0,
      },
    });
    mockPost.mockRejectedValueOnce(denial);
    await expect(createComputer({ resource_tier: 'max' })).rejects.toMatchObject({
      status: 429,
      rateLimitInfo: { message: 'You have reached the 3 workspace limit on the Starter plan.' },
    });
  });
});

describe('start/stop', () => {
  it('starts lazily so the status stream reports the rest', async () => {
    mockPost.mockResolvedValueOnce({
      data: { computer_id: 'c1', status: 'starting', message: 'Start scheduled' },
    });
    const res = await startComputer('c1', { lazy: true });
    expect(mockPost.mock.calls[0][0]).toBe('/api/v1/computers/c1/start?lazy=true');
    expect(res.status).toBe('starting');
  });

  it('stops without the lazy param', async () => {
    mockPost.mockResolvedValueOnce({
      data: { computer_id: 'c1', status: 'stopped', message: 'Computer stopped successfully' },
    });
    const res = await stopComputer('c1');
    expect(mockPost.mock.calls[0][0]).toBe('/api/v1/computers/c1/stop');
    expect(res.status).toBe('stopped');
  });

  it('refuses an empty id rather than posting to a route with a hole in it', async () => {
    await expect(startComputer('')).rejects.toThrow('Computer ID is required');
    expect(mockPost).not.toHaveBeenCalled();
  });
});

describe('streamComputerEvents', () => {
  function mockSSEResponse(chunks: string[], ok = true) {
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

  it('parses the frame the backend actually writes', async () => {
    // Captured from curl against /api/v1/computers/{id}/events on :8060.
    mockSSEResponse([
      'event: status\ndata: {"computer_id": "5319ad0e-7835-4ca6-9541-b7c2961a7bf6", "status": "running"}\n\n',
    ]);
    const onStatus = vi.fn();
    await streamComputerEvents('5319ad0e-7835-4ca6-9541-b7c2961a7bf6', onStatus, ctrl());
    expect(onStatus).toHaveBeenCalledWith('running', undefined);
  });

  it('hits the computer events route', async () => {
    const { fetchMock } = mockSSEResponse([]);
    await streamComputerEvents('c1', vi.fn(), ctrl());
    expect(fetchMock.mock.calls[0][0]).toBe('http://localhost:8000/api/v1/computers/c1/events');
    expect(fetchMock.mock.calls[0][1].headers.Accept).toBe('text/event-stream');
  });

  it('ignores the keepalive comment the server sends every 30s', async () => {
    mockSSEResponse([
      'event: status\ndata: {"computer_id": "c1", "status": "stopped"}\n\n',
      ': ping\n\n',
    ]);
    const onStatus = vi.fn();
    await streamComputerEvents('c1', onStatus, ctrl());
    expect(onStatus).toHaveBeenCalledTimes(1);
    expect(onStatus).toHaveBeenCalledWith('stopped', undefined);
  });

  it('carries the sandbox_state refinement when the server sends one', async () => {
    mockSSEResponse([
      'event: status\ndata: {"computer_id": "c1", "status": "starting", "sandbox_state": "archived"}\n\n',
    ]);
    const onStatus = vi.fn();
    await streamComputerEvents('c1', onStatus, ctrl());
    expect(onStatus).toHaveBeenCalledWith('starting', 'archived');
  });

  it('stops at the server timeout event', async () => {
    mockSSEResponse([
      'event: status\ndata: {"computer_id": "c1", "status": "starting"}\n\n',
      'event: timeout\ndata: {}\n\n',
      'event: status\ndata: {"computer_id": "c1", "status": "running"}\n\n',
    ]);
    const onStatus = vi.fn();
    await streamComputerEvents('c1', onStatus, ctrl());
    expect(onStatus).toHaveBeenCalledTimes(1);
  });

  it('resolves without throwing when the connection fails', async () => {
    global.fetch = vi.fn().mockRejectedValue(new Error('boom')) as unknown as typeof fetch;
    await expect(streamComputerEvents('c1', vi.fn(), ctrl())).resolves.toBeUndefined();
  });

  it('does nothing for an empty id', async () => {
    const fetchMock = vi.fn();
    global.fetch = fetchMock as unknown as typeof fetch;
    await streamComputerEvents('', vi.fn(), ctrl());
    expect(fetchMock).not.toHaveBeenCalled();
  });
});
