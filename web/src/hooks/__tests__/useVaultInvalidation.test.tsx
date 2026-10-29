import { describe, it, expect, vi } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import React, { type ReactNode } from 'react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { queryKeys } from '../../lib/queryKeys';
import {
  useCreateUserVaultSecret,
  useUpdateUserVaultSecret,
  useDeleteUserVaultSecret,
} from '../useUserVault';

vi.mock('../../pages/ChatAgent/utils/api', () => ({
  getUserVaultSecrets: vi.fn(),
  createUserVaultSecret: vi.fn().mockResolvedValue({}),
  updateUserVaultSecret: vi.fn().mockResolvedValue({}),
  deleteUserVaultSecret: vi.fn().mockResolvedValue({}),
}));

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
describe('vault mutations refresh catalog and workspace MCP lists', () => {
  // Account secrets feed needs_secret on the catalog AND on every workspace
  // list, so the whole mcp prefix goes.
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

  // Probe verdicts ride under that prefix: they are cached with staleTime:
  // Infinity, so `missing_secrets` would outlive the secret that answers it.
  it('covers probe verdicts through the prefix', () => {
    expect(queryKeys.mcp.probes().slice(0, queryKeys.mcp.all.length)).toEqual(queryKeys.mcp.all);
  });
});
