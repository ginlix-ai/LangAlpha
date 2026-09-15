import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
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
  useImportMcpCatalogServers,
  useMcpCatalog,
  useDelayedFalse,
} from '../useMcpServers';
import type {
  CatalogServer,
  CatalogServerList,
  EffectiveServerList,
  McpTransport,
  ProbeVerdict,
} from '../../pages/ChatAgent/utils/api';
import { PROBE_KICK_WINDOW_MS } from '../../pages/ChatAgent/components/mcp/mcpState';

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
  importMcpCatalogServers: vi.fn(),
}));

import {
  getWorkspaceMcpServers,
  setWorkspaceMcpServerEnabled,
  addWorkspaceMcpServer,
  deleteWorkspaceMcpServer,
  createMcpCatalogServer,
  importMcpCatalogServers,
  getMcpCatalog,
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

function makeCatalog(
  transport: McpTransport,
  verdict?: ProbeVerdict,
  enabled = true,
  pluginEnabled: boolean | null = null,
): CatalogServerList {
  const server: CatalogServer = {
    name: 'remote',
    transport,
    command: null,
    args: [],
    url: 'https://mcp.example.com/mcp',
    env_refs: [],
    header_refs: [],
    description: '',
    instruction: '',
    tool_exposure_mode: 'summary',
    enabled,
    plugin_enabled: pluginEnabled,
    created_at: null,
    updated_at: null,
    probe: verdict
      ? {
          verdict,
          tools: [],
          server_info: null,
          error: '',
          http_status: null,
          missing_secrets: [],
          probed_at: new Date().toISOString(),
        }
      : null,
  };
  return { servers: [server], max_servers: 20, workspace_servers: [] };
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
      await result.current.mutateAsync({ name: 's2', transport: 'stdio', command: 'node' });
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

  it('catalog create invalidates the whole mcp prefix, not just the catalog', async () => {
    // An enabled catalog row is inherited by every workspace, so a catalog write
    // changes each workspace's effective list — one blast radius for all six.
    const client = makeClient();
    const spy = vi.spyOn(client, 'invalidateQueries');
    (createMcpCatalogServer as Mock).mockResolvedValue({ name: 't1' });

    const { result } = renderHook(() => useCreateMcpCatalogServer(), { wrapper: wrapperFor(client) });
    await act(async () => {
      await result.current.mutateAsync({ name: 't1', transport: 'stdio', command: 'npx' });
    });

    expect(spy).toHaveBeenCalledWith({ queryKey: queryKeys.mcp.all });
  });

  it('catalog import invalidates the user vault too — the backend auto-vaults inline secrets', async () => {
    // Regression: the import only invalidated the catalog, so secrets the
    // import created stayed invisible in Plugins → Secrets (and absent from
    // the server modal's picker) until the 30s staleTime lapsed.
    const client = makeClient();
    const spy = vi.spyOn(client, 'invalidateQueries');
    (importMcpCatalogServers as Mock).mockResolvedValue({
      results: [{ name: 't1', original_name: 't1', renamed: false, status: 'created' }],
      created: 1,
      secrets_created: ['PLACEHOLDER_TOKEN'],
      config_version: 2,
    });

    const { result } = renderHook(() => useImportMcpCatalogServers(), { wrapper: wrapperFor(client) });
    await act(async () => {
      await result.current.mutateAsync({ mcpServers: {} });
    });

    expect(spy).toHaveBeenCalledWith({ queryKey: queryKeys.mcp.all });
    expect(spy).toHaveBeenCalledWith({ queryKey: queryKeys.userVault.all });
  });
});

/**
 * The catalog re-asks while a probe verdict is outstanding. `http` is the whole
 * probeable set (the host dials streamable HTTP), so an `sse` row has no
 * verdict coming and counting one kept the poll running for the full window on
 * every mount.
 *
 * Outstanding includes `unreachable`, which is the one verdict the list route
 * re-kicks: the GET that renders the failure is the same one that went and
 * asked again, so its answer has to be waited on like any other.
 */
describe('useMcpCatalog: the outstanding-probe poll', () => {
  async function pollFor(transport: McpTransport, verdict?: ProbeVerdict) {
    (getMcpCatalog as Mock).mockResolvedValue(makeCatalog(transport, verdict));
    const client = makeClient();
    renderHook(() => useMcpCatalog(), { wrapper: wrapperFor(client) });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });
    const afterFirst = (getMcpCatalog as Mock).mock.calls.length;
    await act(async () => {
      await vi.advanceTimersByTimeAsync(10_000);
    });
    return { afterFirst, total: (getMcpCatalog as Mock).mock.calls.length };
  }

  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it('keeps asking while an http row has no verdict', async () => {
    const { afterFirst, total } = await pollFor('http');
    expect(afterFirst).toBe(1);
    expect(total).toBeGreaterThan(1);
  });

  it('leaves an sse row alone: nothing is going to answer for it', async () => {
    const { afterFirst, total } = await pollFor('sse');
    expect(afterFirst).toBe(1);
    expect(total).toBe(1);
  });

  it('leaves a disabled row alone: the host dials only a row that is on', async () => {
    (getMcpCatalog as Mock).mockResolvedValue(makeCatalog('http', undefined, false));
    const client = makeClient();
    renderHook(() => useMcpCatalog(), { wrapper: wrapperFor(client) });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(10_000);
    });
    expect((getMcpCatalog as Mock).mock.calls.length).toBe(1);
  });

  it('leaves a plugin-disabled row alone: the host refuses it like a switched-off one', async () => {
    // The row keeps `enabled: true`, so only `plugin_enabled` says the host
    // will not dial it. Counting it ran the poll for the whole window on every
    // mount, asking after a verdict nobody was going to produce.
    (getMcpCatalog as Mock).mockResolvedValue(makeCatalog('http', undefined, true, false));
    const client = makeClient();
    renderHook(() => useMcpCatalog(), { wrapper: wrapperFor(client) });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(10_000);
    });
    expect((getMcpCatalog as Mock).mock.calls.length).toBe(1);
  });

  it('waits on an unreachable row: the list GET that showed it also retried it', async () => {
    const { afterFirst, total } = await pollFor('http', 'unreachable');
    expect(afterFirst).toBe(1);
    expect(total).toBeGreaterThan(1);
  });

  it('stops on a verdict the host will not re-kick', async () => {
    const { afterFirst, total } = await pollFor('http', 'credential_rejected');
    expect(afterFirst).toBe(1);
    expect(total).toBe(1);
  });

  it('gives up on a row that stays unreachable past the window', async () => {
    (getMcpCatalog as Mock).mockResolvedValue(makeCatalog('http', 'unreachable'));
    const client = makeClient();
    renderHook(() => useMcpCatalog(), { wrapper: wrapperFor(client) });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(PROBE_KICK_WINDOW_MS + 5_000);
    });
    const exhausted = (getMcpCatalog as Mock).mock.calls.length;
    await act(async () => {
      await vi.advanceTimersByTimeAsync(30_000);
    });
    expect((getMcpCatalog as Mock).mock.calls.length).toBe(exhausted);
  });

  it('opens a second window for a refetch that landed a whole kick throttle later', async () => {
    (getMcpCatalog as Mock).mockResolvedValue(makeCatalog('http', 'unreachable'));
    const client = makeClient();
    renderHook(() => useMcpCatalog(), { wrapper: wrapperFor(client) });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(PROBE_KICK_WINDOW_MS + 5_000);
    });
    // Nothing is polling now, and the host's per-row kick throttle has expired,
    // so the next fetch (a remount, a tab refocus) re-kicks the row.
    await act(async () => {
      await vi.advanceTimersByTimeAsync(180_000);
    });
    const exhausted = (getMcpCatalog as Mock).mock.calls.length;
    await act(async () => {
      await client.refetchQueries({ queryKey: queryKeys.mcp.catalog() });
    });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(10_000);
    });
    expect((getMcpCatalog as Mock).mock.calls.length).toBeGreaterThan(exhausted + 1);
  });
});
