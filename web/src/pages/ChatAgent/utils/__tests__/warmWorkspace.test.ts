import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
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

vi.mock('@/lib/supabase', () => ({
  supabase: null,
}));

import { QueryClient } from '@tanstack/react-query';

import { api } from '@/api/client';
import { queryKeys } from '@/lib/queryKeys';

import {
  warmWorkspace,
  mergeWarmingDisplay,
  __resetWarmStateForTests,
} from '../warmWorkspace';

const mockGet = api.get as Mock;
const mockPost = api.post as Mock;

function makeClient(): QueryClient {
  return new QueryClient({
    defaultOptions: {
      queries: { retry: false, gcTime: 0, staleTime: 0 },
    },
  });
}

beforeEach(() => {
  mockGet.mockReset().mockResolvedValue({ data: {} });
  mockPost.mockReset().mockResolvedValue({
    data: { workspace_id: 'ws-1', status: 'starting', message: 'ok' },
  });
  __resetWarmStateForTests();
});

afterEach(() => {
  __resetWarmStateForTests();
});

describe('warmWorkspace', () => {
  it('skips when cached status is not stopped', async () => {
    const qc = makeClient();
    qc.setQueryData(queryKeys.workspaces.detail('ws-1'), {
      workspace_id: 'ws-1',
      status: 'running',
    });

    await warmWorkspace('ws-1', qc);

    expect(mockPost).not.toHaveBeenCalled();
    expect(mockGet).not.toHaveBeenCalled();
  });

  it('fires /start?lazy=true when cached status is stopped', async () => {
    const qc = makeClient();
    qc.setQueryData(queryKeys.workspaces.detail('ws-1'), {
      workspace_id: 'ws-1',
      status: 'stopped',
    });

    await warmWorkspace('ws-1', qc);

    expect(mockPost).toHaveBeenCalledTimes(1);
    expect(mockPost).toHaveBeenCalledWith('/api/v1/workspaces/ws-1/start?lazy=true');
  });

  it('writes response status into detail cache', async () => {
    const qc = makeClient();
    qc.setQueryData(queryKeys.workspaces.detail('ws-1'), {
      workspace_id: 'ws-1',
      status: 'stopped',
      name: 'foo',
    });

    await warmWorkspace('ws-1', qc);

    const cached = qc.getQueryData(queryKeys.workspaces.detail('ws-1')) as {
      status: string;
      name: string;
    };
    expect(cached.status).toBe('starting');
    expect(cached.name).toBe('foo');
  });

  it('patches matching workspace in cached list query', async () => {
    const qc = makeClient();
    qc.setQueryData(queryKeys.workspaces.detail('ws-1'), {
      workspace_id: 'ws-1',
      status: 'stopped',
    });
    const listKey = queryKeys.workspaces.list({ limit: 20 });
    // The shape every list entry actually has: `getWorkspaces` returns a
    // WorkspacesResponse, so that is the only shape the patcher handles.
    qc.setQueryData(listKey, {
      workspaces: [
        { workspace_id: 'ws-1', status: 'stopped' },
        { workspace_id: 'ws-2', status: 'running' },
      ],
      total: 2,
    });

    await warmWorkspace('ws-1', qc);

    const list = qc.getQueryData(listKey) as { workspaces: Array<{ status: string }> };
    expect(list.workspaces[0].status).toBe('starting');
    expect(list.workspaces[1].status).toBe('running');
  });

  it('dedupes concurrent calls via in-flight Map', async () => {
    const qc = makeClient();
    qc.setQueryData(queryKeys.workspaces.detail('ws-1'), {
      workspace_id: 'ws-1',
      status: 'stopped',
    });

    let resolveStart: (value: { data: unknown }) => void = () => {};
    mockPost.mockReturnValueOnce(
      new Promise((res) => {
        resolveStart = res;
      }),
    );

    const p1 = warmWorkspace('ws-1', qc);
    const p2 = warmWorkspace('ws-1', qc);
    const p3 = warmWorkspace('ws-1', qc);

    expect(mockPost).toHaveBeenCalledTimes(1);

    resolveStart({
      data: { workspace_id: 'ws-1', status: 'starting', message: 'ok' },
    });
    await Promise.all([p1, p2, p3]);

    expect(mockPost).toHaveBeenCalledTimes(1);
  });

  it('fetches workspace detail when cache is empty (direct URL path)', async () => {
    const qc = makeClient();
    mockGet.mockResolvedValueOnce({
      data: { workspace_id: 'ws-1', status: 'stopped' },
    });

    await warmWorkspace('ws-1', qc);

    expect(mockGet).toHaveBeenCalledWith('/api/v1/workspaces/ws-1');
    expect(mockPost).toHaveBeenCalledTimes(1);
  });

  it('does not fire if fetched detail is not stopped', async () => {
    const qc = makeClient();
    mockGet.mockResolvedValueOnce({
      data: { workspace_id: 'ws-1', status: 'running' },
    });

    await warmWorkspace('ws-1', qc);

    expect(mockPost).not.toHaveBeenCalled();
  });

  it('swallows /start errors silently (best-effort)', async () => {
    const qc = makeClient();
    qc.setQueryData(queryKeys.workspaces.detail('ws-1'), {
      workspace_id: 'ws-1',
      status: 'stopped',
    });
    mockPost.mockRejectedValueOnce(new Error('boom'));

    await expect(warmWorkspace('ws-1', qc)).resolves.toBeUndefined();
  });

  it('clears in-flight entry after settle so re-stop allows re-fire', async () => {
    const qc = makeClient();
    qc.setQueryData(queryKeys.workspaces.detail('ws-1'), {
      workspace_id: 'ws-1',
      status: 'stopped',
    });

    await warmWorkspace('ws-1', qc);
    expect(mockPost).toHaveBeenCalledTimes(1);

    // Simulate user stopping the workspace again.
    qc.setQueryData(queryKeys.workspaces.detail('ws-1'), {
      workspace_id: 'ws-1',
      status: 'stopped',
    });

    await warmWorkspace('ws-1', qc);
    expect(mockPost).toHaveBeenCalledTimes(2);
  });

  it('does not clobber a faster SSE running with the 202 starting patch', async () => {
    const qc = makeClient();
    qc.setQueryData(queryKeys.workspaces.detail('ws-1'), {
      workspace_id: 'ws-1',
      status: 'stopped',
    });

    let resolveStart: (value: { data: unknown }) => void = () => {};
    mockPost.mockReturnValueOnce(
      new Promise((res) => {
        resolveStart = res;
      }),
    );

    const p = warmWorkspace('ws-1', qc);

    // SSE pushes a fast 'running' transition before the 202 resolves.
    qc.setQueryData(queryKeys.workspaces.detail('ws-1'), {
      workspace_id: 'ws-1',
      status: 'running',
    });

    resolveStart({
      data: { workspace_id: 'ws-1', status: 'starting', message: 'ok' },
    });
    await p;

    const cached = qc.getQueryData(queryKeys.workspaces.detail('ws-1')) as {
      status: string;
    };
    // The slower 'starting' patch must NOT overwrite the SSE 'running'.
    expect(cached.status).toBe('running');
  });

  it('no-ops for empty workspaceId', async () => {
    const qc = makeClient();
    await warmWorkspace('', qc);
    expect(mockPost).not.toHaveBeenCalled();
  });
});

describe('mergeWarmingDisplay', () => {
  it('returns false when neither source is warming', () => {
    expect(mergeWarmingDisplay(false, false)).toBe(false);
  });

  it('shows the chat-path signal when only it is warming', () => {
    expect(mergeWarmingDisplay('starting', false)).toBe('starting');
  });

  it('shows the entry-time warm signal when only it is warming', () => {
    expect(mergeWarmingDisplay(false, 'starting')).toBe('starting');
  });

  it("lets 'archived' from the chat path win over a plain 'starting' warm", () => {
    expect(mergeWarmingDisplay('archived', 'starting')).toBe('archived');
  });

  it("lets 'archived' from the warm signal win over a plain 'starting' chat", () => {
    expect(mergeWarmingDisplay('starting', 'archived')).toBe('archived');
  });

  it("returns 'archived' when only the warm signal observed the refinement", () => {
    expect(mergeWarmingDisplay(false, 'archived')).toBe('archived');
  });
});
