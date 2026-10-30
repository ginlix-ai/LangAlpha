/**
 * Claude Code on the user's own computers in Settings: one row per computer
 * that says which token it uses and offers the one next step for it, its
 * sheet adds the token to that computer, and with Claude Code off a token
 * left behind still shows, to be removed.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, fireEvent, screen, waitFor, within } from '@testing-library/react';
import { QueryClient } from '@tanstack/react-query';
import { renderWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';
import { claudeCodeComputerTokenUrl } from '@/lib/claudeCode';
import type { ClaudeCodeComputer, ClaudeCodeStatus } from '@/api/model';

vi.mock('@/hooks/useProviderCatalog', () => ({
  useProviderCatalog: () => ({ keyGroups: [], planGroups: [], localEntries: [], placeOf: () => undefined, optionFor: () => null }),
}));
vi.mock('@/hooks/useConfiguredProviders', () => ({
  useConfiguredProviders: () => ({ providers: [], withdrawn: [], configuredSet: new Set(), statusLoading: false, statusError: false }),
}));
vi.mock('@/api/model', () => ({
  getClaudeCodeStatus: vi.fn(),
  getClaudeCodeComputers: vi.fn(),
  saveClaudeCodeToken: vi.fn(),
  removeClaudeCodeToken: vi.fn(),
  startClaudeCodeSignIn: vi.fn(),
  submitClaudeCodeSignInCode: vi.fn(),
  saveClaudeCodeComputerToken: vi.fn(),
  removeClaudeCodeComputerToken: vi.fn(),
  startClaudeCodeComputerSignIn: vi.fn(),
  submitClaudeCodeComputerSignInCode: vi.fn(),
}));

import {
  getClaudeCodeComputers, getClaudeCodeStatus, removeClaudeCodeComputerToken, saveClaudeCodeComputerToken,
} from '@/api/model';
import { ModelAccessSection } from '../ModelAccessSection';

const TOKEN = 'sk-ant-oat01-placeholder';
const MAC: ClaudeCodeComputer = {
  computer_id: 'cmp-mac', name: 'Studio Mac', kind: 'helper', state: 'signed_in', source: 'machine',
  email: 'user@example.com', expires_at: '2027-10-29T12:00:00Z',
};
const BOX: ClaudeCodeComputer = {
  computer_id: 'cmp-box', name: 'Lab box', kind: 'ssh', state: 'no_token', source: null, email: null, expires_at: null,
};
const status = (s: Partial<ClaudeCodeStatus>): ClaudeCodeStatus => ({ enabled: true, state: 'no_token', email: null, expires_at: null, saved: false, ...s });

function render({ flag = true, computers = [MAC, BOX], account = status({}), route = '/' }: {
  flag?: boolean; computers?: ClaudeCodeComputer[]; account?: ClaudeCodeStatus; route?: string;
} = {}) {
  (getClaudeCodeStatus as Mock).mockResolvedValue(account);
  (getClaudeCodeComputers as Mock).mockResolvedValue({ computers });
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  queryClient.setQueryData(queryKeys.features.list(), [{ key: 'claude_code_runtime', enabled: flag }]);
  return renderWithProviders(<ModelAccessSection />, { queryClient, route });
}

/** A computer's row, found by the computer's name beside "Claude Code". */
const rowOf = async (name: string) => (await screen.findByText(name)).closest('li') as HTMLElement;
const buttonsOf = (row: HTMLElement) => within(row).queryAllByRole('button').map((b) => b.textContent);

describe('ModelAccessSection: Claude Code on own computers', () => {
  beforeEach(() => vi.clearAllMocks());

  it('gives each own computer a row that says it uses its own token, and replaces or removes it', async () => {
    render();
    const row = await rowOf('Studio Mac');
    expect(within(row).getByText('Claude Code')).toBeInTheDocument();
    expect(within(row).getByText('Connected')).toBeInTheDocument();
    expect(within(row).getByText('Oct 29, 2027').parentElement).toHaveTextContent(/^Uses its own token · user@example\.com · Expires by Oct 29, 2027$/);
    expect(buttonsOf(row)).toEqual(['Replace', 'Remove']);
  });

  it('offers a token to a computer with none', async () => {
    render();
    const row = await rowOf('Lab box');
    expect(within(row).getByText('Not connected')).toBeInTheDocument();
    expect(within(row).getByText('No setup token for this computer yet')).toBeInTheDocument();
    expect(buttonsOf(row)).toEqual(['Add setup token']);
  });

  it('says when a computer runs on the saved token, and offers it one of its own', async () => {
    render({ computers: [{ ...BOX, state: 'signed_in', source: 'saved' }], account: status({ state: 'signed_in', saved: true }) });
    const row = await rowOf('Lab box');
    expect(within(row).getByText('Connected')).toBeInTheDocument();
    expect(within(row).getByText('Uses your saved token')).toBeInTheDocument();
    expect(buttonsOf(row)).toEqual(['Add its own token']);
  });

  it('says why the saved token a computer runs on stopped working', async () => {
    render({ computers: [{ ...BOX, state: 'rejected', source: 'saved' }], account: status({ state: 'rejected', saved: true }) });
    const row = await rowOf('Lab box');
    expect(within(row).getByText('Claude refused your saved token. It may have expired or been revoked.')).toBeInTheDocument();
    expect(buttonsOf(row)).toEqual(['Add its own token']);
  });

  it('offers a first token while a computer whose token is not known yet is checked', async () => {
    render({ computers: [{ ...BOX, state: 'checking' }] });
    const row = await rowOf('Lab box');
    expect(within(row).getByText('Checking')).toBeInTheDocument();
    expect(buttonsOf(row)).toEqual(['Add setup token']);
  });

  it('says a computer that is not running cannot be checked, and offers nothing', async () => {
    render({ computers: [{ ...MAC, state: 'offline' }] });
    const row = await rowOf('Studio Mac');
    expect(within(row).getByText('Offline')).toBeInTheDocument();
    expect(within(row).getByText("This computer isn't running, so its token can't be checked.")).toBeInTheDocument();
    expect(buttonsOf(row)).toEqual([]);
  });

  // As the saved token's row: a check that never answers keeps the token in reach.
  it('reads the computers again every 5 s while one is checked, then shows the answer', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    try {
      render({ computers: [{ ...MAC, state: 'checking' }, BOX] });
      const row = await rowOf('Studio Mac');
      expect(within(row).getByText('Checking')).toBeInTheDocument();
      expect(buttonsOf(row)).toEqual(['Replace', 'Remove']);
      expect(getClaudeCodeComputers).toHaveBeenCalledTimes(1);
      (getClaudeCodeComputers as Mock).mockResolvedValue({ computers: [MAC, BOX] });
      await act(async () => { await vi.advanceTimersByTimeAsync(5_000); });
      expect(getClaudeCodeComputers).toHaveBeenCalledTimes(2);
      await waitFor(() => expect(within(row).getByText('Connected')).toBeInTheDocument());
      // Nothing is being checked now, so nothing is read again.
      await act(async () => { await vi.advanceTimersByTimeAsync(10_000); });
      expect(getClaudeCodeComputers).toHaveBeenCalledTimes(2);
    } finally {
      vi.useRealTimers();
    }
  });

  it("opens the computer's own sheet, sign-in first, and saves the token onto that computer", async () => {
    (saveClaudeCodeComputerToken as Mock).mockResolvedValue({ ...BOX, state: 'signed_in', source: 'machine' });
    const { queryClient } = render();
    fireEvent.click(within(await rowOf('Lab box')).getByRole('button', { name: 'Add setup token' }));
    const dialog = screen.getByRole('dialog', { name: 'Add a setup token for Lab box' });
    expect(within(dialog).getByText(/runs your Claude threads on Lab box/)).toBeInTheDocument();
    expect(within(dialog).getByText(/puts it on Lab box/)).toBeInTheDocument();
    // Sign in leads; paste follows under it.
    const signIn = within(dialog).getByRole('button', { name: 'Sign in with Claude' });
    const paste = within(dialog).getByLabelText('Setup token');
    expect(signIn.compareDocumentPosition(paste) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    fireEvent.change(paste, { target: { value: TOKEN } });
    fireEvent.click(within(dialog).getByRole('button', { name: 'Save token' }));
    await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull());
    expect(saveClaudeCodeComputerToken).toHaveBeenCalledWith('cmp-box', TOKEN);
    const row = await rowOf('Lab box');
    expect(within(row).getByText('Connected')).toBeInTheDocument();
    expect(JSON.stringify(queryClient.getQueryCache().getAll().map((q) => q.state.data))).not.toContain(TOKEN);
  });

  it("removes a computer's token from that computer, after asking", async () => {
    (removeClaudeCodeComputerToken as Mock).mockResolvedValue(undefined);
    render();
    fireEvent.click(within(await rowOf('Studio Mac')).getByRole('button', { name: 'Remove' }));
    const dialog = screen.getByRole('dialog', { name: 'Remove the setup token from Studio Mac?' });
    expect(within(dialog).getByText(/won't run there until you add a new one/)).toBeInTheDocument();
    fireEvent.click(within(dialog).getByRole('button', { name: 'Remove token' }));
    await waitFor(() => expect(removeClaudeCodeComputerToken).toHaveBeenCalledWith('cmp-mac'));
  });

  it('says the saved token takes over, where this server runs turns on one', async () => {
    render({ account: status({ state: 'signed_in', saved: true }) });
    await screen.findByText('Uses its own token', { exact: false });
    fireEvent.click(within(await rowOf('Studio Mac')).getByRole('button', { name: 'Remove' }));
    expect(screen.getByText(/Claude Code there then uses your saved token/)).toBeInTheDocument();
  });

  it.each([
    ['no saved token runs here', status({ state: 'off', saved: true })],
    ['none is saved', status({ state: 'no_token', saved: false })],
  ])('says nothing of the saved token taking over where %s', async (_, account) => {
    render({ account });
    fireEvent.click(within(await rowOf('Studio Mac')).getByRole('button', { name: 'Remove' }));
    expect(screen.getByText(/won't run there until you add a new one/)).toBeInTheDocument();
  });

  // The link opens the sheet the row's first button opens, titled for it.
  it.each([
    [MAC, 'Replace the setup token on Studio Mac'],
    [{ ...MAC, state: 'rejected' as const }, 'Add a setup token for Studio Mac'],
  ])("opens a computer's sheet from a link that names it", async (entry, title) => {
    render({ computers: [entry], route: claudeCodeComputerTokenUrl('cmp-mac') });
    expect(await screen.findByRole('dialog', { name: title })).toBeInTheDocument();
  });

  it("names the saved token beside computers' rows, and only there", async () => {
    render({ account: status({ state: 'signed_in', saved: true }) });
    const saved = (await screen.findByText('Saved token')).closest('li') as HTMLElement;
    expect(buttonsOf(saved)).toEqual(['Replace', 'Remove']);
  });

  // A hosted deployment runs Claude Code only on the user's own computers.
  it('shows no saved token, and opens none from a link, where no saved token runs', async () => {
    render({ account: status({ state: 'off' }), route: '/?connect=claude-code' });
    await rowOf('Studio Mac');
    expect(screen.queryByText('Saved token')).toBeNull();
    expect(screen.queryByText('Its own setup token, not your Claude Code sign-in')).toBeNull();
    expect(screen.queryByRole('dialog')).toBeNull();
  });

  describe('with Claude Code off', () => {
    it('keeps the saved token and a computer that holds one, each offering Remove only', async () => {
      render({ flag: false, computers: [{ ...MAC, state: 'off' }, { ...BOX, state: 'off' }], account: status({ enabled: false, state: 'off', saved: true }) });
      const row = await rowOf('Studio Mac');
      expect(within(row).getByText('Off')).toBeInTheDocument();
      expect(within(row).getByText('Claude Code is off, but this computer still has its setup token.')).toBeInTheDocument();
      expect(buttonsOf(row)).toEqual(['Remove']);
      const saved = (await screen.findByText('Claude Code is off, but your setup token is still saved.')).closest('li') as HTMLElement;
      expect(buttonsOf(saved)).toEqual(['Remove']);
      // A computer with no token of its own has nothing left to remove.
      expect(screen.queryByText('Lab box')).toBeNull();
    });

    it('shows nothing when no token is left, and the list reads as empty', async () => {
      render({ flag: false, computers: [{ ...BOX, state: 'off' }], account: status({ enabled: false, state: 'off', saved: false }) });
      await waitFor(() => expect(getClaudeCodeComputers).toHaveBeenCalled());
      await act(async () => {});
      expect(screen.queryByText('Claude Code')).toBeNull();
      expect(screen.getByText("No subscription connected.")).toBeInTheDocument();
    });
  });
});
