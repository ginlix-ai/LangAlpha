// @vitest-environment node
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
  createSshPairing,
  deleteComputer,
  getComputers,
  probeSsh,
  reconnectComputer,
  reinstallComputer,
  setComputerFilesBackup,
  startComputer,
  stopComputer,
  streamComputerEvents,
} from '../computers';

const mockGet = api.get as unknown as Mock;
const mockPost = api.post as unknown as Mock;
const mockDelete = api.delete as unknown as Mock;

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
    // The 201 body for {"name":"WP16 probe","resource_tier":"performance"}.
    mockPost.mockResolvedValueOnce({
      data: {
        computer_id: 'ebbab1a6-16fe-46d6-8ae9-08080cd7e084',
        user_id: 'wp13-final-1789525377',
        kind: 'daytona',
        name: 'WP16 probe',
        status: 'starting',
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
    // Its sandbox is built in the background from here, so the row comes back
    // starting with none yet: the shape the UI reads as being prepared.
    expect(computer.status).toBe('starting');
    expect(computer.provider_ref).toBeNull();
    expect(computer.resource_tier).toBe('performance');
  });

  it('defaults to standard and omits an absent name so the server names it', async () => {
    mockPost.mockResolvedValueOnce({ data: {} });
    await createComputer();
    expect(mockPost.mock.calls[0][1]).toEqual({ resource_tier: 'standard' });
  });

  it('carries the backup choice for a machine the user brings, off included', async () => {
    mockPost.mockResolvedValueOnce({ data: {} });
    const ssh = {
      host: '198.51.100.7', port: 22, username: 'me',
      host_key: 'ssh-ed25519 AAAAC3Nz', root_dir: '/home/me/langalpha',
    };
    await createComputer({ name: 'devbox', kind: 'ssh', ssh, files_backup: false });
    expect(mockPost.mock.calls[0][1]).toEqual({
      name: 'devbox', resource_tier: 'standard', kind: 'ssh', ssh, files_backup: false,
    });
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

describe('setComputerFilesBackup', () => {
  it('posts the switch to the computer route and answers the updated row', async () => {
    mockPost.mockResolvedValueOnce({ data: { computer_id: 'c1', kind: 'ssh', files_backup: false } });
    const row = await setComputerFilesBackup('c1', false);
    expect(mockPost).toHaveBeenCalledWith(
      '/api/v1/computers/c1/files-backup',
      { enabled: false },
      expect.objectContaining({ timeout: expect.any(Number) }),
    );
    expect(row.files_backup).toBe(false);
  });

  it('refuses an empty id rather than posting to a route with a hole in it', async () => {
    await expect(setComputerFilesBackup('', true)).rejects.toThrow('Computer ID is required');
    expect(mockPost).not.toHaveBeenCalled();
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

describe('deleteComputer', () => {
  it.each([true, false])('deletes the computer route with remove_key=%s and returns what happened to the key', async (removeKey) => {
    const body = { key: { outcome: 'removed', used_by: null, fingerprint: 'SHA256:abc/def+', comment: 'langalpha' } };
    mockDelete.mockResolvedValueOnce({ data: body, status: 200, headers: {} });
    await expect(deleteComputer('c1', { removeKey })).resolves.toEqual(body);
    expect(mockDelete.mock.calls[0][0]).toBe('/api/v1/computers/c1');
    expect(String(mockDelete.mock.calls[0][1].params)).toBe(`remove_key=${removeKey}`);
  });

  it('names each workspace to delete in the body and the query, and waits out their backups', async () => {
    const body = { key: null, workspaces_deleted: ['w1'] };
    mockDelete.mockResolvedValueOnce({ data: body, status: 200, headers: {} });
    await expect(
      deleteComputer('c1', { removeKey: false, deleteWorkspaces: ['w1', 'w2'], deleteFiles: true }),
    ).resolves.toEqual(body);
    const opts = mockDelete.mock.calls[0][1];
    // The query too, for a backend from before the body or a proxy that drops it.
    expect(String(opts.params)).toBe('remove_key=false&delete_files=true&delete_workspaces=w1&delete_workspaces=w2');
    expect(opts.data).toEqual({ delete_workspaces: ['w1', 'w2'] });
    expect(opts.timeout).toBe(600000);
  });

  it('keeps a full computer\'s workspaces out of the query, which they would overflow', async () => {
    mockDelete.mockResolvedValueOnce({ data: { key: null }, status: 200, headers: {} });
    const ids = Array.from({ length: 31 }, (_, i) => `00000000-0000-0000-0000-${String(i).padStart(12, '0')}`);
    await deleteComputer('c1', { removeKey: false, deleteWorkspaces: ids });
    const opts = mockDelete.mock.calls[0][1];
    expect(String(opts.params)).toBe('remove_key=false');
    expect(opts.data).toEqual({ delete_workspaces: ids });
  });

  it('refuses an empty id rather than deleting a route with a hole in it', async () => {
    await expect(deleteComputer('', { removeKey: true })).rejects.toThrow('Computer ID is required');
    expect(mockDelete).not.toHaveBeenCalled();
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
    const onFrame = vi.fn();
    await streamComputerEvents('5319ad0e-7835-4ca6-9541-b7c2961a7bf6', onFrame, ctrl());
    expect(onFrame).toHaveBeenCalledWith(expect.objectContaining({ status: 'running' }));
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
    const onFrame = vi.fn();
    await streamComputerEvents('c1', onFrame, ctrl());
    expect(onFrame).toHaveBeenCalledTimes(1);
    expect(onFrame).toHaveBeenCalledWith(expect.objectContaining({ status: 'stopped' }));
  });

  it('carries the sandbox_state refinement when the server sends one', async () => {
    mockSSEResponse([
      'event: status\ndata: {"computer_id": "c1", "status": "starting", "sandbox_state": "archived"}\n\n',
    ]);
    const onFrame = vi.fn();
    await streamComputerEvents('c1', onFrame, ctrl());
    expect(onFrame).toHaveBeenCalledWith(expect.objectContaining({ status: 'starting', sandbox_state: 'archived' }));
  });

  it("hands over a new sandbox's build stage and a failed start's reason", async () => {
    mockSSEResponse([
      'event: status\ndata: {"computer_id": "c1", "status": "starting", "stage": "tools"}\n\n',
      'event: status\ndata: {"computer_id": "c1", "status": "error", "error": "Sandbox image build failed"}\n\n',
    ]);
    const onFrame = vi.fn();
    await streamComputerEvents('c1', onFrame, ctrl());
    expect(onFrame.mock.calls[0][0]).toMatchObject({ status: 'starting', stage: 'tools' });
    expect(onFrame.mock.calls[1][0]).toMatchObject({ status: 'error', error: 'Sandbox image build failed' });
  });

  it('hands over the failure a failed start recorded, as the row keeps it', async () => {
    const failure = { code: 'folder_marked', message: 'The folder is held.', owner: 'other-1', remedy: 'reinstall', at: '2026-10-26T04:00:00Z' };
    mockSSEResponse([
      `event: status\ndata: ${JSON.stringify({ computer_id: 'c1', status: 'error', error: 'The folder is held.', start_failure: failure })}\n\n`,
    ]);
    const onFrame = vi.fn();
    await streamComputerEvents('c1', onFrame, ctrl());
    expect(onFrame.mock.calls[0][0]).toMatchObject({ status: 'error', start_failure: failure });
  });

  it('stops at the server timeout event', async () => {
    mockSSEResponse([
      'event: status\ndata: {"computer_id": "c1", "status": "starting"}\n\n',
      'event: timeout\ndata: {}\n\n',
      'event: status\ndata: {"computer_id": "c1", "status": "running"}\n\n',
    ]);
    const onFrame = vi.fn();
    await streamComputerEvents('c1', onFrame, ctrl());
    expect(onFrame).toHaveBeenCalledTimes(1);
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

describe('fixing a failed start', () => {
  beforeEach(() => mockPost.mockClear());

  it('asks a reinstall to take the folder back', async () => {
    mockPost.mockResolvedValueOnce({ data: { computer_id: 'c1', status: 'starting' } });
    const res = await reinstallComputer('c1', { folder: 'take_back' });
    expect(mockPost).toHaveBeenCalledWith(
      '/api/v1/computers/c1/reinstall',
      { folder: 'take_back' },
      expect.objectContaining({ timeout: expect.any(Number) }),
    );
    expect(res).toEqual({ computer_id: 'c1', status: 'starting' });
  });

  it('asks a reinstall for a new folder, as typed', async () => {
    mockPost.mockResolvedValueOnce({ data: { computer_id: 'c1', status: 'starting' } });
    await reinstallComputer('c1', { folder: 'new', root_dir: '~/langalpha-2' });
    expect(mockPost.mock.calls[0][1]).toEqual({ folder: 'new', root_dir: '~/langalpha-2' });
  });

  it('sends a reconnect the target the pairing reached, under ssh', async () => {
    mockPost.mockResolvedValueOnce({ data: { computer_id: 'c1', status: 'starting' } });
    const ssh = { host: 'box', port: 22, username: 'dev', tunnel_id: 'tun-1', host_key: 'ssh-ed25519 AAAA' };
    await reconnectComputer('c1', ssh);
    expect(mockPost).toHaveBeenCalledWith(
      '/api/v1/computers/c1/reconnect',
      { ssh },
      expect.objectContaining({ timeout: expect.any(Number) }),
    );
  });

  it('mints a pairing for a computer with its id, and a new machine with no body', async () => {
    mockPost.mockResolvedValue({ data: { token: 't' } });
    await createSshPairing({ computerId: 'c1' });
    expect(mockPost).toHaveBeenLastCalledWith('/api/v1/computers/ssh/pairings', { computer_id: 'c1' });
    await createSshPairing();
    expect(mockPost).toHaveBeenLastCalledWith('/api/v1/computers/ssh/pairings');
  });

  it('probes a machine reached again by hand with the computer it is for', async () => {
    mockPost.mockResolvedValue({ data: {} });
    const target = { host: 'box', port: 22, username: 'dev' };
    await probeSsh(target, { computerId: 'c1' });
    expect(mockPost.mock.calls.at(-1)?.[1]).toEqual({ ...target, computer_id: 'c1' });
    await probeSsh(target);
    expect(mockPost.mock.calls.at(-1)?.[1]).toEqual(target);
  });
});
