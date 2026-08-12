import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { renderHook, waitFor, act } from '@testing-library/react';
import React, { type ReactNode } from 'react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { queryKeys } from '../../lib/queryKeys';
import {
  useWorkspaceMcpServers,
  useToggleWorkspaceMcpServer,
  useAddWorkspaceMcpServer,
  useDeleteWorkspaceMcpServer,
  useCreateMcpCatalogServer,
  useDelayedFalse,
} from '../useMcpServers';
import type { EffectiveServerList } from '../../pages/ChatAgent/utils/api';

vi.mock('../../pages/ChatAgent/utils/api', () => ({
  getWorkspaceMcpServers: vi.fn(),
  addWorkspaceMcpServer: vi.fn(),
  updateWorkspaceMcpServer: vi.fn(),
  setWorkspaceMcpServerEnabled: vi.fn(),
  deleteWorkspaceMcpServer: vi.fn(),
  discoverWorkspaceMcpServer: vi.fn(),
  getMcpCatalog: vi.fn(),
  createMcpCatalogServer: vi.fn(),
  updateMcpCatalogServer: vi.fn(),
  deleteMcpCatalogServer: vi.fn(),
}));

import {
  getWorkspaceMcpServers,
  setWorkspaceMcpServerEnabled,
  addWorkspaceMcpServer,
  deleteWorkspaceMcpServer,
  createMcpCatalogServer,
} from '../../pages/ChatAgent/utils/api';

const WS = 'ws-1';

function makeServer(name: string, enabled: boolean): EffectiveServerList['servers'][number] {
  return {
    name,
    origin: 'workspace',
    transport: 'stdio',
    enabled,
    editable: true,
    deletable: true,
    status: 'connected',
    error: '',
    tool_count: 2,
    tools: [],
    missing_secrets: [],
    env_refs: [],
    header_refs: [],
    description: '',
    instruction: '',
    tool_exposure_mode: 'summary',
    command: 'npx',
    args: [],
    url: null,
    config_version: 1,
  };
}

function makeList(servers: EffectiveServerList['servers']): EffectiveServerList {
  return { servers, sandbox_running: true, max_servers: 20, config_version: 1 };
}

function makeClient() {
  // gcTime kept non-zero so an unobserved query's cache survives the optimistic
  // setQueryData / rollback assertions (no query hook mounts in these tests).
  return new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: Infinity }, mutations: { retry: false } },
  });
}

function wrapperFor(client: QueryClient) {
  return function Wrapper({ children }: { children: ReactNode }) {
    return <QueryClientProvider client={client}>{children}</QueryClientProvider>;
  };
}

beforeEach(() => {
  vi.clearAllMocks();
});

describe('useWorkspaceMcpServers', () => {
  it('fetches the effective list and is disabled without a workspace id', async () => {
    (getWorkspaceMcpServers as Mock).mockResolvedValue(makeList([makeServer('s1', true)]));
    const client = makeClient();
    const { result } = renderHook(() => useWorkspaceMcpServers(WS), { wrapper: wrapperFor(client) });
    await waitFor(() => expect(result.current.data).toBeDefined());
    expect(result.current.data?.servers).toHaveLength(1);

    const disabled = renderHook(() => useWorkspaceMcpServers(null), { wrapper: wrapperFor(makeClient()) });
    expect(disabled.result.current.fetchStatus).toBe('idle');
  });
});

describe('useToggleWorkspaceMcpServer — optimistic with rollback', () => {
  it('optimistically flips enabled AND reconciles status, then settles', async () => {
    const client = makeClient();
    client.setQueryData(queryKeys.mcp.workspace(WS), makeList([makeServer('s1', true)]));
    (setWorkspaceMcpServerEnabled as Mock).mockResolvedValue({ name: 's1', enabled: false });

    const { result } = renderHook(() => useToggleWorkspaceMcpServer(WS), { wrapper: wrapperFor(client) });

    act(() => {
      result.current.mutate({ name: 's1', enabled: false });
    });

    // Optimistic update applies synchronously in onMutate — enabled AND status
    // flip together so the row never renders an incoherent pair (the glitch).
    await waitFor(() => {
      const cached = client.getQueryData<EffectiveServerList>(queryKeys.mcp.workspace(WS));
      expect(cached?.servers[0].enabled).toBe(false);
      expect(cached?.servers[0].status).toBe('disabled');
    });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
  });

  it('enabling a disabled server optimistically goes straight to connected (no verify flash)', async () => {
    const client = makeClient();
    const disabled = { ...makeServer('s1', false), status: 'disabled' as const };
    client.setQueryData(queryKeys.mcp.workspace(WS), makeList([disabled]));
    (setWorkspaceMcpServerEnabled as Mock).mockResolvedValue({ name: 's1', enabled: true });

    const { result } = renderHook(() => useToggleWorkspaceMcpServer(WS), { wrapper: wrapperFor(client) });

    act(() => {
      result.current.mutate({ name: 's1', enabled: true });
    });

    await waitFor(() => {
      const cached = client.getQueryData<EffectiveServerList>(queryKeys.mcp.workspace(WS));
      expect(cached?.servers[0].enabled).toBe(true);
      // 'connected' (re-enable reconnects from the cached schema) — NOT 'pending'
      // (would flash "Verifying…") and NOT the stale 'disabled' (would flash "Ready").
      expect(cached?.servers[0].status).toBe('connected');
    });
  });

  it('rolls back the optimistic update on error', async () => {
    const client = makeClient();
    client.setQueryData(queryKeys.mcp.workspace(WS), makeList([makeServer('s1', true)]));
    (setWorkspaceMcpServerEnabled as Mock).mockRejectedValue(new Error('boom'));

    const { result } = renderHook(() => useToggleWorkspaceMcpServer(WS), { wrapper: wrapperFor(client) });

    act(() => {
      result.current.mutate({ name: 's1', enabled: false });
    });

    await waitFor(() => expect(result.current.isError).toBe(true));
    // After rollback the cached row is back to enabled=true.
    const cached = client.getQueryData<EffectiveServerList>(queryKeys.mcp.workspace(WS));
    expect(cached?.servers[0].enabled).toBe(true);
  });
});

describe('useDelayedFalse — apply-axis anti-flicker', () => {
  it('holds true through a sub-delay dip to false, but lets a lasting false through', () => {
    vi.useFakeTimers();
    try {
      const { result, rerender } = renderHook(({ v }) => useDelayedFalse(v, 2600), {
        initialProps: { v: true },
      });
      expect(result.current).toBe(true);

      // A bump dips synced false; the row must NOT flash out of "Connected".
      act(() => { rerender({ v: false }); });
      expect(result.current).toBe(true);

      // Apply lands within the window → never showed the dip.
      act(() => { rerender({ v: true }); });
      act(() => { vi.advanceTimersByTime(3000); });
      expect(result.current).toBe(true);
    } finally {
      vi.useRealTimers();
    }
  });

  it('propagates a false that outlasts the delay (a genuinely lagging apply)', () => {
    vi.useFakeTimers();
    try {
      const { result, rerender } = renderHook(({ v }) => useDelayedFalse(v, 2600), {
        initialProps: { v: true },
      });
      act(() => { rerender({ v: false }); });
      expect(result.current).toBe(true); // still held
      act(() => { vi.advanceTimersByTime(2700); });
      expect(result.current).toBe(false); // outlasted the window → honest "Applying…"
    } finally {
      vi.useRealTimers();
    }
  });

  it('propagates an initial-mount false immediately (no spurious "synced")', () => {
    vi.useFakeTimers();
    try {
      const { result } = renderHook(() => useDelayedFalse(false, 2600));
      expect(result.current).toBe(false);
    } finally {
      vi.useRealTimers();
    }
  });
});

describe('mcp mutations — invalidation', () => {
  it('add invalidates the workspace list', async () => {
    const client = makeClient();
    const spy = vi.spyOn(client, 'invalidateQueries');
    (addWorkspaceMcpServer as Mock).mockResolvedValue({ name: 's2', source: 'workspace', enabled: true });

    const { result } = renderHook(() => useAddWorkspaceMcpServer(WS), { wrapper: wrapperFor(client) });
    await act(async () => {
      await result.current.mutateAsync({ from_template: 'tmpl' });
    });

    expect(spy).toHaveBeenCalledWith({ queryKey: queryKeys.mcp.workspace(WS) });
  });

  it('delete invalidates the workspace list', async () => {
    const client = makeClient();
    const spy = vi.spyOn(client, 'invalidateQueries');
    (deleteWorkspaceMcpServer as Mock).mockResolvedValue({ ok: true });

    const { result } = renderHook(() => useDeleteWorkspaceMcpServer(WS), { wrapper: wrapperFor(client) });
    await act(async () => {
      await result.current.mutateAsync('s1');
    });

    expect(spy).toHaveBeenCalledWith({ queryKey: queryKeys.mcp.workspace(WS) });
  });

  it('catalog create invalidates the catalog list', async () => {
    const client = makeClient();
    const spy = vi.spyOn(client, 'invalidateQueries');
    (createMcpCatalogServer as Mock).mockResolvedValue({ name: 't1' });

    const { result } = renderHook(() => useCreateMcpCatalogServer(), { wrapper: wrapperFor(client) });
    await act(async () => {
      await result.current.mutateAsync({ name: 't1', transport: 'stdio', command: 'npx' });
    });

    expect(spy).toHaveBeenCalledWith({ queryKey: queryKeys.mcp.catalog() });
  });
});
