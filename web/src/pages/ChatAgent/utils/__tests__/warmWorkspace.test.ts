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
  patchWorkspaceStatusInCaches,
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
  it('skips when cached status is transitional', async () => {
    const qc = makeClient();
    qc.setQueryData(queryKeys.workspaces.detail('ws-1'), {
      workspace_id: 'ws-1',
      status: 'starting',
    });

    await warmWorkspace('ws-1', qc);

    expect(mockPost).not.toHaveBeenCalled();
    expect(mockGet).not.toHaveBeenCalled();
  });

  it('fires /start?lazy=true for a running computer so the workspace is attached', async () => {
    const qc = makeClient();
    qc.setQueryData(queryKeys.workspaces.detail('ws-1'), {
      workspace_id: 'ws-1',
      status: 'running',
    });

    await warmWorkspace('ws-1', qc);

    expect(mockPost).toHaveBeenCalledWith('/api/v1/workspaces/ws-1/start?lazy=true');
    expect(
      (qc.getQueryData(queryKeys.workspaces.detail('ws-1')) as { status: string }).status,
    ).toBe('running');
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

  it('arms the computer stream and updates sibling workspaces', async () => {
    const qc = makeClient();
    qc.setQueryData(queryKeys.computers.lists(), {
      computers: [{ computer_id: 'comp-1', status: 'stopped' }],
    });
    qc.setQueryData(queryKeys.workspaces.detail('ws-1'), {
      workspace_id: 'ws-1', computer_id: 'comp-1', status: 'stopped',
    });
    qc.setQueryData(queryKeys.workspaces.detail('ws-2'), {
      workspace_id: 'ws-2', computer_id: 'comp-1', status: 'stopped',
    });
    qc.setQueryData(queryKeys.workspaces.list({ limit: 20 }), {
      workspaces: [
        { workspace_id: 'ws-1', computer_id: 'comp-1', status: 'stopped' },
        { workspace_id: 'ws-2', computer_id: 'comp-1', status: 'stopped' },
      ],
      total: 2,
    });

    await warmWorkspace('ws-1', qc);

    const computers = qc.getQueryData(queryKeys.computers.lists()) as {
      computers: Array<{ status: string }>;
    };
    expect(computers.computers[0].status).toBe('starting');
    expect((qc.getQueryData(queryKeys.workspaces.detail('ws-2')) as { status: string }).status)
      .toBe('starting');
  });

  it('moves a bound Home row with its computer, like any workspace on it', async () => {
    const qc = makeClient();
    qc.setQueryData(queryKeys.workspaces.detail('ws-1'), {
      workspace_id: 'ws-1', computer_id: 'comp-1', status: 'stopped',
    });
    qc.setQueryData(queryKeys.workspaces.detail('home-1'), {
      workspace_id: 'home-1', is_home: true, computer_id: 'comp-1', status: 'stopped',
    });
    const listKey = queryKeys.workspaces.list({ limit: 20, includeHome: true });
    qc.setQueryData(listKey, {
      workspaces: [
        { workspace_id: 'ws-1', computer_id: 'comp-1', status: 'stopped' },
        { workspace_id: 'home-1', is_home: true, computer_id: 'comp-1', status: 'stopped' },
      ],
      total: 2,
    });

    await warmWorkspace('ws-1', qc);

    expect((qc.getQueryData(queryKeys.workspaces.detail('home-1')) as { status: string }).status)
      .toBe('starting');
    const list = qc.getQueryData(listKey) as { workspaces: Array<{ status: string }> };
    expect(list.workspaces.map((w) => w.status)).toEqual(['starting', 'starting']);

    // A status pushed for Home lands on its row too.
    patchWorkspaceStatusInCaches(qc, 'home-1', 'running');
    expect((qc.getQueryData(queryKeys.workspaces.detail('home-1')) as { status: string }).status)
      .toBe('running');
    const patched = qc.getQueryData(listKey) as { workspaces: Array<{ status: string }> };
    expect(patched.workspaces.map((w) => w.status)).toEqual(['starting', 'running']);
  });

  it("drops the root a row was read with once a status names another computer", () => {
    // The root is its computer's, so after a move it would read the new
    // computer's paths against the old one until the row was read again.
    const qc = makeClient();
    const key = queryKeys.workspaces.detail('ws-1');
    qc.setQueryData(key, { workspace_id: 'ws-1', computer_id: 'comp-1', root_dir: '/home/someone/langalpha', status: 'stopped' });

    patchWorkspaceStatusInCaches(qc, 'ws-1', 'running', 'comp-1');
    expect(qc.getQueryData(key)).toMatchObject({ computer_id: 'comp-1', root_dir: '/home/someone/langalpha' });

    patchWorkspaceStatusInCaches(qc, 'ws-1', 'running', 'comp-2');
    expect(qc.getQueryData(key)).toMatchObject({ computer_id: 'comp-2', root_dir: null });
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

  it('does not fire if fetched detail is transitional', async () => {
    const qc = makeClient();
    mockGet.mockResolvedValueOnce({
      data: { workspace_id: 'ws-1', status: 'starting' },
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

  // 'unbound' is Home before its first computer.
  it("starts Home while it is unbound, a status that holds back any other workspace", async () => {
    const qc = makeClient();
    qc.setQueryData(queryKeys.workspaces.detail('ws-1'), {
      workspace_id: 'ws-1', is_home: false, status: 'unbound',
    });
    qc.setQueryData(queryKeys.workspaces.detail('home-1'), {
      workspace_id: 'home-1', is_home: true, status: 'unbound',
    });

    await warmWorkspace('ws-1', qc);
    expect(mockPost).not.toHaveBeenCalled();

    // The start binds Home, so the row is read again.
    mockGet.mockResolvedValueOnce({
      data: { workspace_id: 'home-1', is_home: true, computer_id: 'comp-1', status: 'starting' },
    });
    await warmWorkspace('home-1', qc);
    expect(mockPost).toHaveBeenCalledWith('/api/v1/workspaces/home-1/start?lazy=true');
    expect(qc.getQueryData(queryKeys.workspaces.detail('home-1'))).toMatchObject({
      computer_id: 'comp-1', status: 'starting',
    });
  });

  it("knows Home from Home's own query, before any list or detail holds its row", async () => {
    const qc = makeClient();
    qc.setQueryData(queryKeys.workspaces.home(), {
      workspace_id: 'home-1', is_home: true, computer_id: null, status: 'unbound',
    });
    mockGet.mockResolvedValueOnce({
      data: { workspace_id: 'home-1', is_home: true, computer_id: 'comp-1', status: 'starting' },
    });

    await warmWorkspace('home-1', qc);

    expect(mockPost).toHaveBeenCalledWith('/api/v1/workspaces/home-1/start?lazy=true');
  });

  it('shows a Home warm on the workspaces sharing its stopped computer', async () => {
    const qc = makeClient();
    mockPost.mockResolvedValueOnce({ data: { workspace_id: 'home-1', status: 'starting' } });
    // A bound Home's row carries its machine's state like any row on it.
    qc.setQueryData(queryKeys.workspaces.detail('home-1'), {
      workspace_id: 'home-1', is_home: true, computer_id: 'comp-1', status: 'stopped',
    });
    qc.setQueryData(queryKeys.workspaces.list({ limit: 100 }), {
      workspaces: [
        { workspace_id: 'home-1', is_home: true, computer_id: 'comp-1', status: 'stopped' },
        { workspace_id: 'ws-2', computer_id: 'comp-1', status: 'stopped' },
      ],
    });

    await warmWorkspace('home-1', qc);

    const list = qc.getQueryData(queryKeys.workspaces.list({ limit: 100 })) as {
      workspaces: { workspace_id: string; status: string }[];
    };
    expect(list.workspaces.map((w) => [w.workspace_id, w.status])).toEqual([
      ['home-1', 'starting'],
      ['ws-2', 'starting'],
    ]);
  });

  it('reads Home again after the start that first bound it', async () => {
    const qc = makeClient();
    mockPost.mockResolvedValueOnce({ data: { workspace_id: 'home-1', status: 'starting' } });
    // Every cached copy of Home was read before the start put it on a computer.
    const unbound = {
      workspace_id: 'home-1', is_home: true, computer_id: null, dir_name: null, status: 'unbound',
    };
    qc.setQueryData(queryKeys.workspaces.home(), unbound);
    qc.setQueryData(queryKeys.workspaces.detail('home-1'), unbound);
    qc.setQueryData(queryKeys.workspaces.list({ limit: 100 }), {
      workspaces: [{ workspace_id: 'ws-2', computer_id: 'comp-1', status: 'stopped' }],
    });
    mockGet.mockResolvedValueOnce({
      data: { workspace_id: 'home-1', is_home: true, computer_id: 'comp-1', dir_name: 'Home', status: 'stopped' },
    });

    await warmWorkspace('home-1', qc);

    expect(mockGet).toHaveBeenCalledWith('/api/v1/workspaces/home-1');
    const list = qc.getQueryData(queryKeys.workspaces.list({ limit: 100 })) as {
      workspaces: { workspace_id: string; status: string }[];
    };
    expect(list.workspaces.map((w) => [w.workspace_id, w.status])).toEqual([['ws-2', 'starting']]);
    // The detail every placement reader goes through now names the folder,
    // and moves with the machine like the workspace beside it.
    expect(qc.getQueryData(queryKeys.workspaces.detail('home-1'))).toMatchObject({
      computer_id: 'comp-1', dir_name: 'Home', status: 'starting',
    });
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
