import { beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, fireEvent, screen, waitFor, within } from '@testing-library/react';
import { queryOptions } from '@tanstack/react-query';
import { useLocation } from 'react-router';
import { renderWithProviders } from '@/test/utils';
import { api } from '@/api/client';
import { clientOs } from '@/lib/clientOs';
import { toast } from '@/components/ui/use-toast';
import { getRecentThreads, getWorkspaceThreads } from '@/pages/ChatAgent/utils/api';
import { getNavPrefs } from '@/pages/ChatAgent/utils/navPrefs';
import { workspaceThreadsQuery } from '@/pages/ChatAgent/utils/threadQueries';
import { queryKeys } from '@/lib/queryKeys';
import { useArchiveThreadConfirm } from '@/pages/ChatAgent/components/threadArchiveAction';
import { CommandPaletteProvider } from '../CommandPaletteProvider';

// The palette's sources answer at once with one recent thread; its writes go
// through the app's own thread calls to a stubbed PATCH, so a test reads the
// request the server would get.
vi.mock('@/pages/ChatAgent/utils/api', async (importActual) => {
  const actual = await importActual<typeof import('@/pages/ChatAgent/utils/api')>();
  return {
    updateThread: actual.updateThread,
    updateThreadTitle: actual.updateThreadTitle,
    getRecentThreads: vi.fn(),
    getWorkspaceThreads: vi.fn(async () => ({ threads: [] })),
    getThread: vi.fn(async (id: string) => ({ thread_id: id, workspace_id: 'ws1', title: 'NVDA margins' })),
  };
});
vi.mock('@/api/search', () => ({
  searchConversations: vi.fn(async () => ({ threads: [], turns: [], files: [], next_cursor: { threads: null, turns: null, files: null }, failed: [] })),
  getSearchPreview: vi.fn(async () => {
    throw new Error('no preview here');
  }),
}));
vi.mock('@/hooks/useAllWorkspaces', () => ({
  useAllWorkspaces: () => ({ data: { workspaces: [{ workspace_id: 'ws1', name: 'Semis', status: 'running', is_home: false }] } }),
}));
vi.mock('@/hooks/useSkills', () => ({ useSkills: () => ({ data: [] }) }));
vi.mock('@/hooks/useHomeWorkspace', () => ({
  homeWorkspaceQuery: () =>
    queryOptions({ queryKey: ['workspaces', 'home'], queryFn: async () => ({ workspace_id: 'home', is_home: true, name: 'Home' }) }),
}));
vi.mock('@/hooks/useSymbolSearch', () => ({ useSymbolSearch: () => ({ hits: [], loading: false }) }));
vi.mock('@/lib/quotes', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  useQuotes: () => ({ quotes: {} }),
}));
vi.mock('@/contexts/ThemeContext', () => ({ useTheme: () => ({ theme: 'light' }) }));
vi.mock('@/hooks/useAppearance', () => ({ useAppearance: () => ({ pickTheme: vi.fn() }) }));
vi.mock('@/components/nav/useNavItems', () => ({ useNavItems: () => [] }));
vi.mock('@/pages/Settings/panels/usePreferenceWrite', () => ({ useSaveFailureToast: () => vi.fn() }));
vi.mock('@/components/CreateWorkspaceHost', () => ({ useOpenCreateWorkspace: () => null }));
vi.mock('@/components/ui/use-toast', () => ({ toast: vi.fn() }));
vi.mock('@/pages/ChatAgent/components/threadArchiveAction', () => ({ useArchiveThreadConfirm: vi.fn() }));

const requestArchive = vi.fn();
const patch = vi.spyOn(api, 'patch');

function Path() {
  return <output aria-label="Path">{useLocation().pathname}</output>;
}
const path = () => screen.getByLabelText('Path').textContent;

function renderShell(route = '/') {
  return renderWithProviders(
    <CommandPaletteProvider>
      <Path />
    </CommandPaletteProvider>,
    { route },
  );
}

const shortcut = () =>
  fireEvent.keyDown(document.activeElement ?? window, { key: 'k', ...(clientOs() === 'windows' ? { ctrlKey: true } : { metaKey: true }) });
const field = () => screen.findByRole('combobox') as Promise<HTMLInputElement>;
const option = (name: string | RegExp) => screen.getByRole('option', { name });
const closed = () => waitFor(() => expect(screen.queryByRole('combobox')).not.toBeInTheDocument());
const footer = () => document.querySelector('.command-palette-footer')!;

/** Opens the palette on its recent threads and the given row's actions. */
async function openActions(title: RegExp) {
  shortcut();
  const input = await field();
  await screen.findByRole('option', { name: title });
  await waitFor(() => expect(screen.getByRole('option', { name: title })).toHaveAttribute('aria-selected', 'true'));
  fireEvent.keyDown(input, { key: 'ArrowRight' });
  expect(screen.getByRole('listbox')).toHaveAccessibleName(expect.stringMatching(/^Actions for/));
  return input;
}

beforeAll(async () => {
  await import('../CommandPalette');
}, 30_000);

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(getRecentThreads).mockResolvedValue({
    threads: [{ thread_id: 't1', title: 'NVDA margins', workspace_id: 'ws1', updated_at: '2026-11-08T00:00:00Z', is_pinned: false }],
  } as never);
  vi.mocked(useArchiveThreadConfirm).mockReturnValue({ requestArchive, dialog: null });
  patch.mockImplementation(
    async (url: string, body: unknown) =>
      ({ data: { thread_id: url.split('/').pop(), workspace_id: 'ws1', ...(body as object) }, status: 200, headers: {} }) as never,
  );
});

describe('a thread row’s actions', () => {
  it('follow the ways to open it with Rename, Pin to top and Archive', async () => {
    renderShell();
    await openActions(/NVDA margins/);
    const names = within(screen.getByRole('listbox')).getAllByRole('option').map((row) => row.textContent);
    expect(names).toEqual(['Open', 'Open in new tab', 'Copy link', 'Rename', 'Pin to top', 'Archive']);
  });

  it('marks a pinned thread, offers Unpin, and unpins it, saying so once the server took it', async () => {
    vi.mocked(getRecentThreads).mockResolvedValue({
      threads: [{ thread_id: 't1', title: 'NVDA margins', workspace_id: 'ws1', updated_at: '2026-11-08T00:00:00Z', is_pinned: true }],
    } as never);
    renderShell();
    shortcut();
    const row = await screen.findByRole('option', { name: /NVDA margins/ });
    expect(within(row).getByRole('img', { name: 'Pinned' })).toBeInTheDocument();
    const input = await field();
    fireEvent.keyDown(input, { key: 'ArrowRight' });
    fireEvent.click(option('Unpin'));

    await closed();
    expect(patch).toHaveBeenCalledWith('/api/v1/threads/t1', { is_pinned: false }, undefined);
    await waitFor(() => expect(toast).toHaveBeenCalledWith({ title: 'Unpinned' }));
  });
});

describe('Rename', () => {
  it('puts the title in the field, selected, under a Rename chip, over the row’s actions', async () => {
    renderShell();
    const input = await openActions(/NVDA margins/);
    fireEvent.click(option('Rename'));

    expect(input).toHaveValue('NVDA margins');
    expect(input.selectionStart).toBe(0);
    expect(input.selectionEnd).toBe('NVDA margins'.length);
    expect(input).toHaveAccessibleName('Rename: NVDA margins');
    expect(document.querySelector('.command-palette-chip-label')).toHaveTextContent('Rename');
    expect(screen.getByRole('listbox')).toHaveAccessibleName('Actions for NVDA margins');
    expect(option('Rename')).toHaveAttribute('aria-selected', 'true');
    // The pointer passing over another action leaves the edit be.
    fireEvent.mouseMove(option('Archive'));
    expect(input).toHaveValue('NVDA margins');
    expect(option('Rename')).toHaveAttribute('aria-selected', 'true');
    expect(footer()).toHaveTextContent('↵Save');
    expect(footer()).toHaveTextContent('escBack');
    expect(footer()).not.toHaveTextContent('Move');
  });

  it('saves a new title with one PATCH and closes', async () => {
    renderShell();
    const input = await openActions(/NVDA margins/);
    fireEvent.click(option('Rename'));
    fireEvent.change(input, { target: { value: 'NVDA gross margins ' } });
    fireEvent.keyDown(input, { key: 'Enter' });

    await closed();
    await waitFor(() => expect(patch).toHaveBeenCalledTimes(1));
    expect(patch).toHaveBeenCalledWith('/api/v1/threads/t1', { title: 'NVDA gross margins' }, expect.objectContaining({ timeout: expect.any(Number) }));
  });

  it('goes back to the row’s actions on Escape, Rename still picked, and to the results after', async () => {
    renderShell();
    const input = await openActions(/NVDA margins/);
    fireEvent.click(option('Rename'));
    fireEvent.change(input, { target: { value: 'Something else' } });
    fireEvent.keyDown(input, { key: 'Escape' });

    expect(screen.getByRole('listbox')).toHaveAccessibleName('Actions for NVDA margins');
    expect(option('Rename')).toHaveAttribute('aria-selected', 'true');
    expect(input).toHaveValue('');
    fireEvent.keyDown(input, { key: 'Escape' });
    expect(option(/NVDA margins/)).toHaveAttribute('aria-selected', 'true');
    expect(patch).not.toHaveBeenCalled();
  });

  it('saves nothing for an empty title, and Backspace there goes back to the actions', async () => {
    renderShell();
    const input = await openActions(/NVDA margins/);
    fireEvent.click(option('Rename'));
    fireEvent.change(input, { target: { value: '' } });
    expect(footer()).not.toHaveTextContent('Save');
    fireEvent.keyDown(input, { key: 'Enter' });
    await act(async () => {});
    expect(screen.getByRole('combobox')).toBeInTheDocument();
    expect(patch).not.toHaveBeenCalled();

    fireEvent.keyDown(input, { key: 'Backspace' });
    expect(screen.getByRole('listbox')).toHaveAccessibleName('Actions for NVDA margins');
  });

  it('clears the title on a held Backspace and stays in the edit; a fresh press on the empty field goes back', async () => {
    renderShell();
    const input = await openActions(/NVDA margins/);
    fireEvent.click(option('Rename'));
    fireEvent.change(input, { target: { value: '' } });
    // The repeats of the press that emptied it: the field stays the title's.
    fireEvent.keyDown(input, { key: 'Backspace', repeat: true });
    fireEvent.keyDown(input, { key: 'Backspace', repeat: true });
    expect(input).toHaveAccessibleName('Rename: NVDA margins');
    expect(document.querySelector('.command-palette-chip-label')).toHaveTextContent('Rename');

    fireEvent.keyDown(input, { key: 'Backspace' });
    expect(screen.getByRole('listbox')).toHaveAccessibleName('Actions for NVDA margins');
    expect(input).not.toHaveAccessibleName('Rename: NVDA margins');
  });

  it('saves the thread being renamed when a refetch drops its row meanwhile, and opens nothing else', async () => {
    const { queryClient } = renderShell();
    const input = await openActions(/NVDA margins/);
    fireEvent.click(option('Rename'));
    fireEvent.change(input, { target: { value: 'NVDA gross margins' } });
    act(() => {
      queryClient.setQueryData(queryKeys.threads.recent(50), {
        threads: [{ thread_id: 't9', title: 'AMD supply', workspace_id: 'ws1', updated_at: '2026-11-09T00:00:00Z', is_pinned: false }],
      });
    });
    await screen.findByRole('option', { name: /AMD supply/ });
    expect(input).toHaveValue('NVDA gross margins');
    fireEvent.keyDown(input, { key: 'Enter' });

    await closed();
    await waitFor(() => expect(patch).toHaveBeenCalledWith('/api/v1/threads/t1', { title: 'NVDA gross margins' }, expect.anything()));
    expect(path()).toBe('/');
  });

  it('closes without a PATCH when the title did not change', async () => {
    renderShell();
    const input = await openActions(/NVDA margins/);
    fireEvent.click(option('Rename'));
    fireEvent.keyDown(input, { key: 'Enter' });
    await closed();
    expect(patch).not.toHaveBeenCalled();
  });
});

describe('Archive', () => {
  it('asks the archive confirm once the palette has closed, then archives and leaves the thread on screen for its workspace when it was the last', async () => {
    renderShell('/chat/t/t1');
    await openActions(/NVDA margins/);
    fireEvent.click(option('Archive'));

    await closed();
    await waitFor(() => expect(requestArchive).toHaveBeenCalledWith('t1', expect.any(Function)));
    expect(patch).not.toHaveBeenCalled();
    const archive = requestArchive.mock.calls[0][1] as () => void;
    await act(async () => archive());

    expect(patch).toHaveBeenCalledWith('/api/v1/threads/t1', { archived: true }, undefined);
    await waitFor(() => expect(path()).toBe('/chat/ws1'));
    expect(toast).toHaveBeenCalledWith({ title: 'Archived' });
  });

  it('opens the next thread in the sidebar’s order in its place, as the sidebar does', async () => {
    vi.mocked(getWorkspaceThreads).mockResolvedValue({
      threads: [
        { thread_id: 't1', title: 'NVDA margins', workspace_id: 'ws1' },
        { thread_id: 't2', title: 'AMD supply', workspace_id: 'ws1' },
      ],
      total: 2,
    } as never);
    const { queryClient } = renderShell('/chat/t/t1');
    // The list the sidebar keeps, which outlives the test client's instant gc.
    await queryClient.fetchQuery({ ...workspaceThreadsQuery('ws1', getNavPrefs().threadPageSize), gcTime: Infinity });
    await openActions(/NVDA margins/);
    fireEvent.click(option('Archive'));

    await closed();
    await waitFor(() => expect(requestArchive).toHaveBeenCalled());
    await act(async () => (requestArchive.mock.calls[0][1] as () => void)());
    await waitFor(() => expect(path()).toBe('/chat/t/t2'));
  });
});
