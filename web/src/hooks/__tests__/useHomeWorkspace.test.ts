import { afterEach, describe, it, expect, vi } from 'vitest';
import { waitFor } from '@testing-library/react';
import { QueryClient } from '@tanstack/react-query';
import { renderHookWithProviders } from '@/test/utils';

const api = vi.hoisted(() => ({
  getHomeWorkspace: vi.fn(async () => ({ workspace_id: 'home-1', is_home: true })),
}));
vi.mock('@/pages/ChatAgent/utils/api/workspaces', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getHomeWorkspace: api.getHomeWorkspace,
}));

import { queryKeys } from '@/lib/queryKeys';
import { resolveChatWorkspace, useHomeWorkspaceId } from '../useHomeWorkspace';

afterEach(() => {
  api.getHomeWorkspace.mockClear();
});

/**
 * A catalog read before Home exists: the insert that ensures Home switches it
 * off for every server new workspaces start without, which this read lacks.
 * The test client's `gcTime: 0` would drop it before the hook runs.
 */
function clientWithCatalog(): QueryClient {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  queryClient.setQueryData(queryKeys.mcp.catalog(), { servers: [] });
  return queryClient;
}

describe('useHomeWorkspaceId', () => {
  it('refetches the MCP catalog once the first fetch ensures Home', async () => {
    const queryClient = clientWithCatalog();
    const { result } = renderHookWithProviders(() => useHomeWorkspaceId(), { queryClient });

    await waitFor(() => expect(result.current).toBe('home-1'));
    expect(queryClient.getQueryState(queryKeys.mcp.catalog())?.isInvalidated).toBe(true);
    queryClient.clear();
  });

  it('leaves the catalog alone on a refresh, which cannot be the insert', async () => {
    const queryClient = clientWithCatalog();
    queryClient.setQueryData(queryKeys.workspaces.home(), { workspace_id: 'home-1' });
    // What creating a regular workspace does to Home's key.
    await queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.all });
    renderHookWithProviders(() => useHomeWorkspaceId(), { queryClient });

    await waitFor(() => expect(api.getHomeWorkspace).toHaveBeenCalledTimes(1));
    await waitFor(() =>
      expect(queryClient.getQueryState(queryKeys.workspaces.home())?.isInvalidated).toBe(false),
    );
    expect(queryClient.getQueryState(queryKeys.mcp.catalog())?.isInvalidated).toBe(false);
    queryClient.clear();
  });

});

describe('resolveChatWorkspace', () => {
  it('opens the Chief of Staff in Home through the shared query, read once', async () => {
    const queryClient = clientWithCatalog();
    expect(await resolveChatWorkspace(queryClient, 'all')).toBe('home-1');
    expect(await resolveChatWorkspace(queryClient, 'all', 'ws-1')).toBe('home-1');
    expect(api.getHomeWorkspace).toHaveBeenCalledTimes(1);
    expect(queryClient.getQueryState(queryKeys.mcp.catalog())?.isInvalidated).toBe(true);
    queryClient.clear();
  });

  it('opens a workspace scope in the selected one, or nowhere with none', async () => {
    const queryClient = clientWithCatalog();
    expect(await resolveChatWorkspace(queryClient, 'workspace', 'ws-1')).toBe('ws-1');
    expect(await resolveChatWorkspace(queryClient, 'workspace', null)).toBeNull();
    expect(api.getHomeWorkspace).not.toHaveBeenCalled();
    queryClient.clear();
  });
});
