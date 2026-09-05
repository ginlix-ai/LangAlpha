import { describe, it, expect, vi } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import React, { type ReactNode } from 'react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { queryKeys } from '../../lib/queryKeys';
import {
  useCreateWorkspaceVaultSecret,
  useUpdateWorkspaceVaultSecret,
  useDeleteWorkspaceVaultSecret,
} from '../useWorkspaceVault';
import {
  useCreateUserVaultSecret,
  useUpdateUserVaultSecret,
  useDeleteUserVaultSecret,
} from '../useUserVault';

vi.mock('../../pages/ChatAgent/utils/api', () => ({
  getVaultSecrets: vi.fn(),
  getVaultBlueprints: vi.fn(),
  createVaultSecret: vi.fn().mockResolvedValue({}),
  updateVaultSecret: vi.fn().mockResolvedValue({}),
  deleteVaultSecret: vi.fn().mockResolvedValue({}),
  getUserVaultSecrets: vi.fn(),
  createUserVaultSecret: vi.fn().mockResolvedValue({}),
  updateUserVaultSecret: vi.fn().mockResolvedValue({}),
  deleteUserVaultSecret: vi.fn().mockResolvedValue({}),
}));

const WS = 'ws-1';

async function invalidatedBy<T>(
  hookFn: () => T,
  fire: (mutation: T) => Promise<unknown>,
): Promise<unknown[]> {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  const invalidate = vi.spyOn(client, 'invalidateQueries');
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={client}>{children}</QueryClientProvider>
  );
  const { result } = renderHook(hookFn, { wrapper });
  await act(async () => {
    await fire(result.current);
  });
  return invalidate.mock.calls.map(
    ([arg]) => (arg as { queryKey: unknown }).queryKey,
  );
}

/**
 * needs_secret on server rows derives from vault state, and a settled MCP
 * query stops polling — so every vault mutation must invalidate the MCP keys
 * or the "needs secret" pill outlives the fix that clears it.
 */
describe('workspace vault mutations refresh the workspace MCP list', () => {
  it('create invalidates the workspace MCP key', async () => {
    const keys = await invalidatedBy(
      () => useCreateWorkspaceVaultSecret(WS),
      (m) => m.mutateAsync({ name: 'K', value: 'v' }),
    );
    expect(keys).toContainEqual(queryKeys.mcp.workspace(WS));
  });

  it('update invalidates the workspace MCP key', async () => {
    const keys = await invalidatedBy(
      () => useUpdateWorkspaceVaultSecret(WS),
      (m) => m.mutateAsync({ name: 'K', body: { value: 'v2' } }),
    );
    expect(keys).toContainEqual(queryKeys.mcp.workspace(WS));
  });

  it('delete invalidates the workspace MCP key', async () => {
    const keys = await invalidatedBy(
      () => useDeleteWorkspaceVaultSecret(WS),
      (m) => m.mutateAsync('K'),
    );
    expect(keys).toContainEqual(queryKeys.mcp.workspace(WS));
  });
});

describe('user vault mutations refresh catalog and workspace MCP lists', () => {
  // User-tier secrets feed needs_secret on the catalog AND on inherited rows
  // in every workspace list, so the whole mcp prefix goes.
  it('create invalidates the mcp prefix', async () => {
    const keys = await invalidatedBy(
      () => useCreateUserVaultSecret(),
      (m) => m.mutateAsync({ name: 'K', value: 'v' }),
    );
    expect(keys).toContainEqual(queryKeys.mcp.all);
  });

  it('update invalidates the mcp prefix', async () => {
    const keys = await invalidatedBy(
      () => useUpdateUserVaultSecret(),
      (m) => m.mutateAsync({ name: 'K', body: { value: 'v2' } }),
    );
    expect(keys).toContainEqual(queryKeys.mcp.all);
  });

  it('delete invalidates the mcp prefix', async () => {
    const keys = await invalidatedBy(
      () => useDeleteUserVaultSecret(),
      (m) => m.mutateAsync('K'),
    );
    expect(keys).toContainEqual(queryKeys.mcp.all);
  });
});
