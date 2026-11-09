/**
 * The count a computer runs at once, saved from its row: a list read still in
 * flight when it is saved never puts the old number back.
 */
import { describe, expect, it, vi } from 'vitest';
import { fireEvent, screen, waitFor } from '@testing-library/react';
import { QueryClient } from '@tanstack/react-query';
import { renderWithProviders } from '@/test/utils';
import type { ClaudeCodeComputer, ClaudeCodeComputers } from '@/api/model';
import { queryKeys } from '@/lib/queryKeys';

const h = vi.hoisted(() => ({ server: 4, setClis: vi.fn() }));

vi.mock('@/api/model', async (original) => ({
  ...(await original<typeof import('@/api/model')>()),
  setClaudeCodeComputerClis: h.setClis,
}));
vi.mock('../../usePreferenceWrite', () => ({ useSaveFailedToast: () => vi.fn() }));

import { ComputerClis } from '../ComputerClis';

const mac = (clis: number): ClaudeCodeComputer => ({
  computer_id: 'mac', name: 'Studio Mac', kind: 'helper', state: 'signed_in', source: null, email: null, expires_at: null, clis,
});
const listOf = (clis: number): ClaudeCodeComputers => ({ computers: [mac(clis)] } as ClaudeCodeComputers);

describe('ComputerClis', () => {
  it('stays compact under a fine pointer and takes a finger\'s height under a coarse one', () => {
    renderWithProviders(<ComputerClis computerId="mac" clis={4} max={8} />, { queryClient: new QueryClient() });
    const select = screen.getByLabelText('Claude Code agents at once');
    expect(select.style.height).toBe('');
    expect(select.parentElement).toHaveClass('[&>select]:h-7', 'pointer-coarse:[&>select]:h-11');
  });

  it('cancels a list read in flight, so it cannot land after the save with the old number', async () => {
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const key = queryKeys.features.claudeCodeComputers();
    const reads: Array<(list: ClaudeCodeComputers) => void> = [];
    const read = () => {
      const clis = h.server;
      return new Promise<ClaudeCodeComputers>((resolve) => reads.push(() => resolve(listOf(clis))));
    };
    queryClient.setQueryData(key, listOf(4));
    const inFlight = queryClient.fetchQuery({ queryKey: key, queryFn: read, staleTime: 0 }).catch(() => null);
    h.setClis.mockImplementation(async (_id: string, clis: number) => {
      h.server = clis;
      return mac(clis);
    });

    renderWithProviders(<ComputerClis computerId="mac" clis={4} max={8} />, { queryClient });
    fireEvent.change(screen.getByLabelText('Claude Code agents at once'), { target: { value: '6' } });
    await waitFor(() => expect(queryClient.getQueryData<ClaudeCodeComputers>(key)?.computers[0].clis).toBe(6));

    reads[0](listOf(4));
    await inFlight;
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(queryClient.getQueryData<ClaudeCodeComputers>(key)?.computers[0].clis).toBe(6);
    expect(h.setClis).toHaveBeenCalledWith('mac', 6);
  });
});
