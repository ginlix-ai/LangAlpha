import type { ReactNode } from 'react';
import { act, renderHook } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { expect, it, vi } from 'vitest';
import { toast } from '@/components/ui/use-toast';
import { queryKeys } from '@/lib/queryKeys';
import { useWorkspaceMutation } from '../useWorkspaceMutation';

vi.mock('@/components/ui/use-toast', () => ({ toast: vi.fn() }));

it('refreshes the list and the mutated detail, and leaves siblings alone', async () => {
  const client = new QueryClient();
  const lists = queryKeys.workspaces.lists();
  const active = queryKeys.workspaces.detail('active');
  const sibling = queryKeys.workspaces.detail('sibling');
  client.setQueryData(lists, []);
  client.setQueryData(active, { name: 'old' });
  client.setQueryData(sibling, { name: 'other' });
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={client}>{children}</QueryClientProvider>
  );
  const { result } = renderHook(() => useWorkspaceMutation<string>({
    mutationFn: vi.fn().mockResolvedValue({}),
    errorTitleKey: 'workspace.renameFailed',
  }), { wrapper });
  await act(async () => { expect(await result.current.run('active', 'new')).toBe(true); });
  expect(client.getQueryState(lists)?.isInvalidated).toBe(true);
  expect(client.getQueryState(active)?.isInvalidated).toBe(true);
  expect(client.getQueryState(sibling)?.isInvalidated).toBe(false);
  client.clear();
});

it('rolls back and says a refused name was taken, resolving false so the dialog stays open', async () => {
  const client = new QueryClient();
  client.setQueryData(queryKeys.workspaces.lists(), { workspaces: [{ workspace_id: 'ws-1', name: 'Alpha' }] });
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={client}>{children}</QueryClientProvider>
  );
  const refused = Object.assign(new Error('Request failed with status code 409'), {
    response: { status: 409, data: { detail: { code: 'workspace_name_taken', message: 'taken', name: 'Beta' } } },
  });
  const { result } = renderHook(() => useWorkspaceMutation<string>({
    mutationFn: vi.fn().mockRejectedValue(refused),
    optimisticPatch: (name) => ({ name }),
    errorTitleKey: 'workspace.renameFailed',
  }), { wrapper });
  const quiet = vi.spyOn(console, 'error').mockImplementation(() => {});

  await act(async () => { expect(await result.current.run('ws-1', 'beta')).toBe(false); });

  expect(client.getQueryData(queryKeys.workspaces.lists())).toEqual({ workspaces: [{ workspace_id: 'ws-1', name: 'Alpha' }] });
  expect(toast).toHaveBeenCalledWith({
    variant: 'destructive',
    title: 'Could not rename workspace',
    description: 'A workspace named "Beta" already exists. Choose another name.',
  });
  quiet.mockRestore();
  client.clear();
});
