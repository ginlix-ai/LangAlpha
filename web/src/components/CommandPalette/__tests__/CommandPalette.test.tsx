import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, fireEvent, screen, waitFor } from '@testing-library/react';
import { queryOptions } from '@tanstack/react-query';
import { useLocation } from 'react-router';
import { renderWithProviders } from '@/test/utils';
import { CreateWorkspaceHost } from '@/components/CreateWorkspaceHost';
import { clientOs } from '@/lib/clientOs';
import { searchConversations } from '@/api/search';
import { getRecentThreads } from '@/pages/ChatAgent/utils/api';
import { CommandPaletteProvider, useCommandPalette } from '../CommandPaletteProvider';

// The palette's sources answer with nothing, and at once: these tests are
// about its keys and focus, not what it finds.
// One workspace, with one thread, is there to scope to.
vi.mock('@/pages/ChatAgent/utils/api', () => ({
  getRecentThreads: vi.fn(async () => ({ threads: [] })),
  getWorkspaceThreads: vi.fn(async () => ({
    threads: [{ thread_id: 't1', title: 'NVDA margins', workspace_id: 'ws1', updated_at: '2026-11-08T00:00:00Z' }],
  })),
}));
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
vi.mock('@/pages/ChatAgent/hooks/useCreateWorkspace', () => ({ useCreateWorkspace: () => vi.fn() }));
vi.mock('@/pages/ChatAgent/components/CreateWorkspaceModal', () => ({
  default: () => (
    <div role="dialog" aria-label="Create workspace">
      <input aria-label="Workspace name" autoFocus />
    </div>
  ),
}));

/** Where the app is, so a test reads which row ran. */
function Path() {
  return <output aria-label="Path">{useLocation().pathname}</output>;
}

/** A surface that opens the palette at a kind, as the phone's Dashboard field does. */
function OpensTickers() {
  const palette = useCommandPalette();
  return (
    <button type="button" onClick={() => palette?.open({ kind: 'tickers' })}>
      Search stocks
    </button>
  );
}

function renderShell() {
  return renderWithProviders(
    <CreateWorkspaceHost>
      <CommandPaletteProvider>
        <textarea aria-label="Composer" />
        <OpensTickers />
        <Path />
      </CommandPaletteProvider>
    </CreateWorkspaceHost>,
  );
}
const path = () => screen.getByLabelText('Path').textContent;

// The shortcut as this keyboard sends it (jsdom's user agent can read as Windows).
const shortcut = () =>
  fireEvent.keyDown(document.activeElement ?? window, { key: 'k', ...(clientOs() === 'windows' ? { ctrlKey: true } : { metaKey: true }) });
/** A key pressed wherever focus is, as the browser would send it. */
const press = (key: string) => fireEvent.keyDown(document.activeElement ?? window, { key });
const field = () => screen.findByRole('combobox');

// The palette's code, loaded once up front so each open waits only on the
// lazy component's own promise, as it does in the app once the code is warm.
beforeAll(async () => {
  await import('../CommandPalette');
}, 30_000);

beforeEach(() => {
  vi.clearAllMocks();
});

afterEach(() => {
  vi.useRealTimers();
});

describe('the key buffer', () => {
  it('gives the field what was typed before it had focus, and keeps it off the page', async () => {
    renderShell();
    const composer = screen.getByLabelText('Composer');
    composer.focus();
    shortcut();
    // The page's own key handlers, which would send a draft on Enter.
    const heard: string[] = [];
    const listen = (e: KeyboardEvent) => heard.push(e.key);
    document.addEventListener('keydown', listen);
    // The palette's code is still loading: these land nowhere but the buffer.
    expect(press('n')).toBe(false);
    expect(press('v')).toBe(false);
    press('x');
    press('Backspace');
    press('Enter');

    document.removeEventListener('keydown', listen);
    expect(heard).not.toContain('Enter');
    expect(composer).not.toHaveFocus();
    expect(composer).toHaveValue('');
    const input = await field();
    await waitFor(() => expect(input).toHaveFocus());
    expect(input).toHaveValue('nv');
  });

  it('takes an IME composition finished before the field had focus', async () => {
    renderShell();
    screen.getByLabelText('Composer').focus();
    shortcut();
    // Focus moved off the composer at once, to where the composition then runs.
    const standIn = document.activeElement as HTMLInputElement;
    expect(standIn).not.toBe(screen.getByLabelText('Composer'));
    press('a');
    fireEvent.compositionStart(standIn);
    standIn.value = '毛利';
    fireEvent.input(standIn);
    fireEvent.compositionEnd(standIn, { data: '毛利' });

    const input = await field();
    await waitFor(() => expect(input).toHaveValue('a毛利'));
    expect(screen.getByLabelText('Composer')).toHaveValue('');
  });

  it('closes on Escape before the palette drew, handing focus back', async () => {
    renderShell();
    const composer = screen.getByLabelText('Composer');
    composer.focus();
    shortcut();
    press('Escape');
    expect(composer).toHaveFocus();
  });
});

describe('focus', () => {
  it('returns to where it was when the palette closes', async () => {
    renderShell();
    const composer = screen.getByLabelText('Composer');
    composer.focus();
    shortcut();
    const input = await field();
    await waitFor(() => expect(input).toHaveFocus());

    fireEvent.keyDown(input, { key: 'Escape' });
    await waitFor(() => expect(screen.queryByRole('combobox')).not.toBeInTheDocument());
    expect(composer).toHaveFocus();
  });
});

describe('New workspace', () => {
  it('opens its dialog as the palette closes, and the dialog keeps focus', async () => {
    renderShell();
    screen.getByLabelText('Composer').focus();
    shortcut();
    const input = await field();
    fireEvent.change(input, { target: { value: 'new workspace' } });
    const row = await screen.findByRole('option', { name: /New workspace/ });
    fireEvent.click(row);

    const dialog = await screen.findByRole('dialog', { name: 'Create workspace' });
    await waitFor(() => expect(screen.queryByRole('combobox')).not.toBeInTheDocument());
    await act(async () => {});
    expect(dialog).toBeInTheDocument();
    expect(screen.getByLabelText('Workspace name')).toHaveFocus();
  });
});

describe('scopes', () => {
  const selected = (name: RegExp) => waitFor(() => expect(screen.getByRole('option', { name })).toHaveAttribute('aria-selected', 'true'));

  it('narrows to a workspace on Tab and to its thread on another, and Backspace steps back out of each', async () => {
    renderShell();
    shortcut();
    const input = await field();
    fireEvent.change(input, { target: { value: 'semis' } });
    await selected(/Semis/);

    fireEvent.keyDown(input, { key: 'Tab' });
    expect(input).toHaveFocus();
    expect(input).toHaveValue('');
    expect(input).toHaveAccessibleName('Search in Semis');
    // Its newest threads, before a word is typed.
    await selected(/NVDA margins/);
    expect(screen.getByText('Tab: search in it.', { exact: false })).toBeInTheDocument();

    fireEvent.keyDown(input, { key: 'Tab' });
    expect(input).toHaveAccessibleName('Search in NVDA margins');
    expect(screen.getByText('Search in this thread')).toBeInTheDocument();

    fireEvent.keyDown(input, { key: 'Backspace' });
    expect(input).toHaveAccessibleName('Search in Semis');
    fireEvent.keyDown(input, { key: 'Backspace' });
    expect(input).toHaveAccessibleName(/Search threads/);
    expect(input).toHaveValue('semis');
    await selected(/Semis/);
  });

  it('takes a leading $ as the tickers chip, and Escape steps out of it before closing', async () => {
    renderShell();
    shortcut();
    const input = await field();
    fireEvent.change(input, { target: { value: '$NVDA' } });
    expect(input).toHaveValue('NVDA');
    expect(input).toHaveAccessibleName('Search in Tickers');

    fireEvent.keyDown(input, { key: 'Escape' });
    expect(input).toHaveAccessibleName(/Search threads/);
    expect(screen.getByRole('combobox')).toBeInTheDocument();
  });

  it('opens in the tickers scope when asked to start there, and Backspace widens it to everything', async () => {
    renderShell();
    fireEvent.click(screen.getByRole('button', { name: 'Search stocks' }));
    const input = await field();
    await waitFor(() => expect(input).toHaveFocus());
    expect(input).toHaveValue('');
    expect(input).toHaveAccessibleName('Search in Tickers');

    fireEvent.change(input, { target: { value: 'nv' } });
    expect(input).toHaveAccessibleName('Search in Tickers');
    fireEvent.change(input, { target: { value: '' } });
    fireEvent.keyDown(input, { key: 'Backspace' });
    expect(input).toHaveAccessibleName(/Search threads/);

    // The next open by the shortcut starts from everything again.
    fireEvent.keyDown(input, { key: 'Escape' });
    await waitFor(() => expect(screen.queryByRole('combobox')).not.toBeInTheDocument());
    shortcut();
    expect(await field()).toHaveAccessibleName(/Search threads/);
  });

  it('starts in the tickers scope on a cold open, keeping keys typed before the field had focus', async () => {
    renderShell();
    fireEvent.click(screen.getByRole('button', { name: 'Search stocks' }));
    // Focus is on the button, outside the palette, while its code loads.
    press('n');
    press('v');
    const input = await field();
    await waitFor(() => expect(input).toHaveFocus());
    expect(input).toHaveAccessibleName('Search in Tickers');
    expect(input).toHaveValue('nv');
  });

  it('takes a leading / as the files chip, says what to type, and Backspace in the empty field takes it off', async () => {
    renderShell();
    shortcut();
    const input = await field();
    // Said under the empty field as text: no row Enter could land on.
    const prefixes = document.querySelector('.command-palette-prefixes');
    expect(prefixes).toHaveTextContent('/files');
    expect(prefixes?.closest('[role="listbox"]')).toBeNull();

    fireEvent.change(input, { target: { value: '/' } });
    expect(input).toHaveValue('');
    expect(input).toHaveAccessibleName('Search in Files');
    expect(screen.getByText("Type part of a file's name or path.")).toBeInTheDocument();
    expect(screen.queryByRole('option')).not.toBeInTheDocument();
    expect(document.querySelector('.command-palette-prefixes')).toBeNull();

    fireEvent.keyDown(input, { key: 'Backspace' });
    expect(input).toHaveAccessibleName(/Search threads/);
    expect(input).toHaveValue('');
  });

  it('narrows a workspace to its files with /, and Backspace returns to the workspace', async () => {
    renderShell();
    shortcut();
    const input = await field();
    fireEvent.change(input, { target: { value: 'semis' } });
    await selected(/Semis/);
    fireEvent.keyDown(input, { key: 'Tab' });
    expect(input).toHaveAccessibleName('Search in Semis');

    fireEvent.change(input, { target: { value: '/' } });
    expect(input).toHaveAccessibleName('Search Files in Semis');
    fireEvent.keyDown(input, { key: 'Backspace' });
    expect(input).toHaveAccessibleName('Search in Semis');
    await selected(/NVDA margins/);
  });
});

describe('drill-ins', () => {
  const thread = (id: string, title: string) => ({ thread_id: id, title, workspace_id: 'ws1', updated_at: '2026-11-08T00:00:00Z' });
  beforeEach(() => {
    vi.mocked(getRecentThreads).mockResolvedValue({ threads: [thread('r1', 'Rates outlook')] } as never);
    vi.mocked(searchConversations).mockImplementation(async ({ q }) => ({
      threads: q ? [{ ...thread('s1', 'NVDA margins'), workspace_name: 'Semis', automation_name: null, is_pinned: false, has_title: true, match: [] }] : [],
      turns: [],
      files: [],
      next_cursor: { threads: 'next', turns: null, files: null },
      failed: [],
    }));
  });
  afterEach(() => {
    vi.mocked(getRecentThreads).mockResolvedValue({ threads: [] } as never);
    vi.mocked(searchConversations).mockResolvedValue({ threads: [], turns: [], files: [], next_cursor: { threads: null, turns: null, files: null }, failed: [] });
  });

  it('opens Show all as the threads view, and Escape brings the results back as they were', async () => {
    renderShell();
    shortcut();
    const input = await field();
    fireEvent.change(input, { target: { value: 'nvda' } });
    fireEvent.click(await screen.findByRole('option', { name: /Show all threads/ }));

    expect(screen.getByRole('listbox')).toHaveAccessibleName('Threads');
    expect(input).toHaveValue('nvda');
    await screen.findByRole('option', { name: /NVDA margins/ });

    fireEvent.keyDown(input, { key: 'Escape' });
    expect(screen.getByRole('combobox')).toBeInTheDocument();
    expect(input).toHaveValue('nvda');
    await waitFor(() => expect(screen.getByRole('option', { name: /Show all threads/ })).toHaveAttribute('aria-selected', 'true'));
  });

  it('lists the newest threads once its field is cleared, as # does', async () => {
    renderShell();
    shortcut();
    const input = await field();
    fireEvent.change(input, { target: { value: 'nvda' } });
    fireEvent.click(await screen.findByRole('option', { name: /Show all threads/ }));
    fireEvent.change(input, { target: { value: '' } });
    await screen.findByRole('option', { name: /Rates outlook/ });
    expect(screen.queryByText('No matches')).not.toBeInTheDocument();

    fireEvent.keyDown(input, { key: 'Escape' });
    fireEvent.change(input, { target: { value: '#' } });
    expect(input).toHaveAccessibleName('Search in Threads');
    await screen.findByRole('option', { name: /Rates outlook/ });
  });
});

describe('Enter while the server answers', () => {
  const EMPTY_ANSWER = { threads: [], turns: [], files: [], next_cursor: { threads: null, turns: null, files: null }, failed: [] };
  const found = {
    ...EMPTY_ANSWER,
    threads: [{ thread_id: 's1', title: 'Zebra stripes', workspace_id: 'ws1', workspace_name: 'Semis', automation_name: null, updated_at: '2026-11-08T00:00:00Z', is_pinned: false, has_title: true, match: [] }],
  };
  /** The server's answer, held until the test lets it go. */
  function holdAnswer() {
    let answer: (value: typeof found) => void = () => {};
    let refuse: (error: unknown) => void = () => {};
    vi.mocked(searchConversations).mockImplementation(
      () =>
        new Promise((resolve, reject) => {
          answer = resolve;
          refuse = reject;
        }),
    );
    return { answer: (value: typeof found) => act(async () => answer(value)), refuse: (error: unknown) => act(async () => refuse(error)) };
  }
  const asked = () => waitFor(() => expect(searchConversations).toHaveBeenCalled());
  afterEach(() => {
    vi.mocked(searchConversations).mockResolvedValue(EMPTY_ANSWER);
  });

  it('waits for the server when only Ask matched, then opens the first row it found', async () => {
    const server = holdAnswer();
    renderShell();
    shortcut();
    const input = await field();
    fireEvent.change(input, { target: { value: 'zebra' } });
    await screen.findByRole('option', { name: /zebra/i });
    fireEvent.keyDown(input, { key: 'Enter' });
    await asked();
    expect(path()).toBe('/');

    await server.answer(found);
    await waitFor(() => expect(path()).toBe('/chat/t/s1'));
  });

  it('runs nothing once the selection moves while Enter waits', async () => {
    const server = holdAnswer();
    renderShell();
    shortcut();
    const input = await field();
    fireEvent.change(input, { target: { value: 'zebra' } });
    await screen.findByRole('option', { name: /zebra/i });
    fireEvent.keyDown(input, { key: 'Enter' });
    fireEvent.keyDown(input, { key: 'ArrowDown' });
    await asked();

    await server.answer(found);
    await screen.findByRole('option', { name: /Zebra stripes/ });
    await act(async () => {});
    expect(path()).toBe('/');
    expect(screen.getByRole('combobox')).toBeInTheDocument();
  });

  it('waits on a stalled server for two seconds at most, then asks', async () => {
    holdAnswer();
    renderShell();
    shortcut();
    const input = await field();
    // Ask is offered once where new work goes is known.
    fireEvent.change(input, { target: { value: 'zebra' } });
    await screen.findByRole('option', { name: /zebra/i });
    vi.useFakeTimers();
    fireEvent.change(input, { target: { value: 'zebras' } });
    await act(async () => { await vi.advanceTimersByTimeAsync(300); });
    expect(searchConversations).toHaveBeenLastCalledWith(expect.objectContaining({ q: 'zebras' }), expect.anything());
    fireEvent.keyDown(input, { key: 'Enter' });

    await act(async () => { await vi.advanceTimersByTimeAsync(1800); });
    expect(path()).toBe('/');
    await act(async () => { await vi.advanceTimersByTimeAsync(300); });
    expect(path()).toBe('/chat/t/__default__');
  });

  it('says a refused search, and Enter no longer waits on it', async () => {
    const server = holdAnswer();
    renderShell();
    shortcut();
    const input = await field();
    fireEvent.change(input, { target: { value: 'zebra' } });
    await asked();
    await server.refuse(Object.assign(new Error('Request failed with status code 429'), { response: { status: 429, data: {} } }));

    await screen.findByText('Too many searches at once. Try again in a moment.');
    fireEvent.keyDown(input, { key: 'Enter' });
    await waitFor(() => expect(path()).toBe('/chat/t/__default__'));
  });

  it('says which kind the server ran out of time on, and shows the others', async () => {
    vi.mocked(searchConversations).mockResolvedValue({ ...found, failed: ['files'] });
    renderShell();
    shortcut();
    const input = await field();
    fireEvent.change(input, { target: { value: 'zebra' } });
    await screen.findByRole('option', { name: /Zebra stripes/ });
    expect(screen.getByText("Couldn't search your files right now.")).toBeInTheDocument();
  });
});
