/**
 * The count a computer runs at once, saved from its row: a list read still in
 * flight when it is saved never puts the old number back.
 */
import { describe, expect, it, onTestFinished, vi } from 'vitest';
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

  it('takes focus where the view was opened for this computer, and only there', async () => {
    const scrolled = vi.fn();
    const { scrollIntoView } = Element.prototype;
    Element.prototype.scrollIntoView = scrolled;
    onTestFinished(() => { Element.prototype.scrollIntoView = scrollIntoView; });
    const { unmount } = renderWithProviders(<ComputerClis computerId="mac" clis={4} max={8} />, { queryClient: new QueryClient() });
    await new Promise((resolve) => requestAnimationFrame(resolve));
    expect(screen.getByLabelText('Claude Code agents at once')).not.toHaveFocus();
    unmount();

    renderWithProviders(<ComputerClis computerId="mac" clis={4} max={8} located />, { queryClient: new QueryClient() });
    await waitFor(() => expect(screen.getByLabelText('Claude Code agents at once')).toHaveFocus());
    expect(scrolled).toHaveBeenCalledWith({ block: 'center' });
  });

  it('says once it has taken focus, so a remount with the link spent does not take it again', async () => {
    const onLocated = vi.fn();
    const { scrollIntoView } = Element.prototype;
    Element.prototype.scrollIntoView = vi.fn();
    onTestFinished(() => { Element.prototype.scrollIntoView = scrollIntoView; });
    renderWithProviders(<ComputerClis computerId="mac" clis={4} max={8} located onLocated={onLocated} />, { queryClient: new QueryClient() });
    await waitFor(() => expect(onLocated).toHaveBeenCalledTimes(1));
  });

  it('offers Automatic with what memory allows, shows it while no number is set, and saves it as null', async () => {
    h.setClis.mockResolvedValue({ ...mac(16), clis_automatic: 16, clis_chosen: false });
    const { unmount } = renderWithProviders(
      <ComputerClis computerId="mac" clis={6} automatic={16} chosen max={16} />, { queryClient: new QueryClient() },
    );
    const select = screen.getByLabelText<HTMLSelectElement>('Claude Code agents at once');
    expect(select.options[0].textContent).toBe('Automatic (16)');
    expect(select.value).toBe('6');
    fireEvent.change(select, { target: { value: 'automatic' } });
    await waitFor(() => expect(h.setClis).toHaveBeenCalledWith('mac', null));
    unmount();

    renderWithProviders(<ComputerClis computerId="mac" clis={16} automatic={16} chosen={false} max={16} />, { queryClient: new QueryClient() });
    expect(screen.getByLabelText<HTMLSelectElement>('Claude Code agents at once').value).toBe('automatic');
  });

  it('keeps the focus through a save and takes no second change until it lands', async () => {
    let land: (entry: ClaudeCodeComputer) => void = () => {};
    h.setClis.mockReset();
    h.setClis.mockImplementation(() => new Promise<ClaudeCodeComputer>((resolve) => { land = resolve; }));
    renderWithProviders(<ComputerClis computerId="mac" clis={4} max={8} />, { queryClient: new QueryClient() });
    const select = screen.getByLabelText<HTMLSelectElement>('Claude Code agents at once');
    select.focus();
    fireEvent.change(select, { target: { value: '6' } });
    await waitFor(() => expect(select).toHaveAttribute('aria-busy', 'true'));
    expect(select).not.toBeDisabled();
    expect(select).toHaveFocus();
    fireEvent.change(select, { target: { value: '2' } });
    expect(h.setClis).toHaveBeenCalledTimes(1);
    land(mac(6));
    await waitFor(() => expect(select).toHaveAttribute('aria-busy', 'false'));
  });

  it('offers no Automatic where the server does not say what memory allows', () => {
    renderWithProviders(<ComputerClis computerId="mac" clis={4} max={8} />, { queryClient: new QueryClient() });
    const select = screen.getByLabelText<HTMLSelectElement>('Claude Code agents at once');
    expect([...select.options].map((o) => o.value)).toEqual(['1', '2', '3', '4', '5', '6', '7', '8']);
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
