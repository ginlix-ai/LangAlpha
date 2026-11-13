/**
 * Claude Code on the user's own computers in Settings: Model access lists
 * Claude Code as one row, whose view has one row per computer that says which
 * token it uses and offers the one next step for it, its sheet adds the token
 * to that computer, and with Claude Code off a token left behind still shows,
 * to be removed.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, fireEvent, screen, waitFor, within } from '@testing-library/react';
import { QueryClient } from '@tanstack/react-query';
import { renderWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';
import { claudeCodeTokenUrl } from '@/lib/claudeCode';
import type { ClaudeCodeComputer, ClaudeCodeSetup, ClaudeCodeStatus } from '@/api/model';

vi.mock('@/hooks/useAllModels', () => ({ useAllModels: () => ({ models: {}, metadata: {} }) }));
vi.mock('@/hooks/useSlotDefaultModels', async (original) => ({
  ...(await original<typeof import('@/hooks/useSlotDefaultModels')>()),
  useDefaultModel: () => null,
}));
vi.mock('@/hooks/useProviderCatalog', () => ({
  useProviderCatalog: () => ({ keyGroups: [], planGroups: [], localEntries: [], placeOf: () => undefined, optionFor: () => null }),
}));
vi.mock('@/hooks/useConfiguredProviders', () => ({
  useConfiguredProviders: () => ({ providers: [], withdrawn: [], configuredSet: new Set(), statusLoading: false, statusError: false }),
  useOAuthStatus: () => ({ data: undefined }),
}));
vi.mock('@/api/model', () => ({
  getClaudeCodeSetup: vi.fn(),
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
// A workspace a link names and the test did not seed is read here.
vi.mock('@/pages/ChatAgent/utils/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/pages/ChatAgent/utils/api')>()),
  getWorkspace: vi.fn(),
}));

import {
  getClaudeCodeComputers, getClaudeCodeSetup, getClaudeCodeStatus, removeClaudeCodeComputerToken, saveClaudeCodeComputerToken,
} from '@/api/model';
import { getWorkspace } from '@/pages/ChatAgent/utils/api';
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

const failed = (code: number) => Object.assign(new Error(`Request failed with status code ${code}`), { response: { status: code, data: {} } });
const workspace = (workspaceId: string, computerId: string) => ({ workspace_id: workspaceId, name: 'Desk', is_home: false, computer_id: computerId });

/** `flag`: Claude Code is on. `workspaces`: the computer of each workspace already read. */
function render({ flag = true, computers = [MAC, BOX], account = status({}), route = '/', workspaces = {} }: {
  flag?: boolean; computers?: ClaudeCodeComputer[]; account?: ClaudeCodeStatus; route?: string; workspaces?: Record<string, string>;
} = {}) {
  const setup: ClaudeCodeSetup = {
    state: flag ? 'connected' : 'not_connected', on: flag, computers, saved_token_runs: !flag || account.state !== 'off', cloud: false, too_small: [],
  };
  (getClaudeCodeSetup as Mock).mockResolvedValue(setup);
  (getClaudeCodeStatus as Mock).mockResolvedValue(account);
  (getClaudeCodeComputers as Mock).mockResolvedValue({ computers });
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false, retryDelay: 0 } } });
  queryClient.setQueryData(queryKeys.features.list(), [{ key: 'claude_code_runtime', enabled: flag, gate: 'opt_in' }]);
  for (const [id, computerId] of Object.entries(workspaces)) queryClient.setQueryData(queryKeys.workspaces.detail(id), workspace(id, computerId));
  return renderWithProviders(<ModelAccessSection />, { queryClient, route });
}

/** Claude Code's view, opened once from its one row in the list. */
const view = async () => {
  const open = screen.queryByRole('dialog', { name: 'Claude Code', hidden: true });
  if (open) return open;
  fireEvent.click(await screen.findByRole('button', { name: 'Manage' }));
  return screen.findByRole('dialog', { name: 'Claude Code' });
};
/** A computer's row in the view, found by the computer's name. */
const rowOf = async (name: string) => (await within(await view()).findByText(name)).closest('li') as HTMLElement;
/** The saved token's row, in the view's Setup token group. */
const savedRowOf = async () => within((await within(await view()).findByText('Setup token')).parentElement as HTMLElement).getByRole('listitem');
/** The list is read once Claude Code's row is there, before any view is opened. */
const listRead = async () => {
  await screen.findByText('Claude Code');
  await waitFor(() => expect(getClaudeCodeComputers).toHaveBeenCalled());
  await act(async () => {});
};
const buttonsOf = (row: HTMLElement) => within(row).queryAllByRole('button').map((b) => b.textContent || b.getAttribute('aria-label'));


/** Remove is a line's button where it is the only action, and the item of
 *  its More actions menu beside any other. */
function clickRemove(row: HTMLElement) {
  const more = within(row).queryByRole('button', { name: 'More actions' });
  if (!more) return fireEvent.click(within(row).getByRole('button', { name: 'Remove' }));
  fireEvent.keyDown(more, { key: 'Enter' });
  fireEvent.click(screen.getByRole('menuitem', { name: 'Remove' }));
}

describe('ModelAccessSection: Claude Code on own computers', () => {
  beforeEach(() => vi.clearAllMocks());

  it('gives each own computer a row that names its account, not that the token is its own, and replaces or removes it', async () => {
    render();
    const row = await rowOf('Studio Mac');
    expect(within(row).getByText('Connected')).toBeInTheDocument();
    expect(within(row).getByText('Oct 29, 2027').parentElement).toHaveTextContent(/^user@example\.com · Expires by Oct 29, 2027$/);
    expect(buttonsOf(row)).toEqual(['Replace', 'More actions']);
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

  it('offers to remove a token file a computer refuses while it runs on the saved token', async () => {
    (removeClaudeCodeComputerToken as Mock).mockResolvedValue(undefined);
    render({ computers: [{ ...BOX, state: 'signed_in', source: 'saved', own_file: true }], account: status({ state: 'signed_in', saved: true }) });
    const row = await rowOf('Lab box');
    expect(within(row).getByText('Uses your saved token')).toBeInTheDocument();
    expect(buttonsOf(row)).toEqual(['Add its own token', 'More actions']);
    clickRemove(row);
    const dialog = screen.getByRole('dialog', { name: 'Remove the setup token from Lab box?' });
    expect(within(dialog).getByText(/Claude Code there then uses your saved token/)).toBeInTheDocument();
    fireEvent.click(within(dialog).getByRole('button', { name: 'Remove token' }));
    await waitFor(() => expect(removeClaudeCodeComputerToken).toHaveBeenCalledWith('cmp-box'));
  });

  it('offers a first token while a computer whose token is not known yet is checked', async () => {
    render({ computers: [{ ...BOX, state: 'checking' }] });
    const row = await rowOf('Lab box');
    expect(within(row).getByText('Checking')).toBeInTheDocument();
    expect(buttonsOf(row)).toEqual(['Add setup token']);
  });

  it('says a computer that is not running cannot be checked, and offers only to view it', async () => {
    render({ computers: [{ ...MAC, state: 'offline' }] });
    const row = await rowOf('Studio Mac');
    expect(within(row).getByText('Offline')).toBeInTheDocument();
    expect(within(row).getByText("This computer isn't running, so its token can't be checked.")).toBeInTheDocument();
    expect(buttonsOf(row)).toEqual(['View computer']);
  });

  it("says Claude Code can't run on a computer an organization's policy is installed on, and invites no token there", async () => {
    render({ computers: [{ ...MAC, state: 'policy_blocked' }, { ...BOX, state: 'policy_blocked' }] });
    const blocked = "Your organization's Claude Code policy is installed on this computer, so LangAlpha can't run Claude Code here.";
    const mac = await rowOf('Studio Mac');
    expect(within(mac).getByText('Unavailable')).toBeInTheDocument();
    expect(within(mac).getByText(blocked)).toBeInTheDocument();
    expect(buttonsOf(mac)).toEqual(['Remove']);
    const box = await rowOf('Lab box');
    expect(within(box).getByText(blocked)).toBeInTheDocument();
    expect(buttonsOf(box)).toEqual([]);
  });

  it.each([[true, ['Remove']], [false, []]])(
    "says Claude Code can't run on this server where an organization's policy is installed on it (saved %s)",
    async (saved, buttons) => {
      render({ computers: [BOX], account: status({ state: 'policy_blocked', saved }) });
      const row = await savedRowOf();
      expect(within(row).getByText('Unavailable')).toBeInTheDocument();
      expect(within(row).getByText("Your organization's Claude Code policy is installed on this computer, so LangAlpha can't run Claude Code here.")).toBeInTheDocument();
      expect(buttonsOf(row)).toEqual(buttons);
    },
  );

  it('says a computer without perl needs it, offers Remove only for a token left there, and invites no new token', async () => {
    render({ computers: [{ ...MAC, state: 'needs_perl' }, { ...BOX, state: 'needs_perl' }] });
    const needsPerl = 'Claude Code needs perl on this computer. Install perl, then try again.';
    const mac = await rowOf('Studio Mac');
    expect(within(mac).getByText('Unavailable')).toBeInTheDocument();
    expect(within(mac).getByText(needsPerl)).toBeInTheDocument();
    expect(within(mac).queryByText(/Claude refused this token/)).toBeNull();
    expect(buttonsOf(mac)).toEqual(['Remove']);
    const box = await rowOf('Lab box');
    expect(within(box).getByText(needsPerl)).toBeInTheDocument();
    expect(buttonsOf(box)).toEqual([]);
  });

  it('says a computer on musl is unsupported, offers Remove only for a token left there, and invites no new token', async () => {
    render({ computers: [{ ...MAC, state: 'computer_unsupported' }, { ...BOX, state: 'computer_unsupported' }] });
    const musl = "This computer runs musl (for example Alpine Linux), which LangAlpha's runtime doesn't support yet.";
    const mac = await rowOf('Studio Mac');
    expect(within(mac).getByText('Unavailable')).toBeInTheDocument();
    expect(within(mac).getByText(musl)).toBeInTheDocument();
    expect(buttonsOf(mac)).toEqual(['Remove']);
    const box = await rowOf('Lab box');
    expect(within(box).getByText(musl)).toBeInTheDocument();
    expect(buttonsOf(box)).toEqual([]);
  });

  // As the saved token's row: a check that never answers keeps the token in reach.
  it('reads the computers again every 5 s while one is checked, then shows the answer', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    try {
      render({ computers: [{ ...MAC, state: 'checking' }, BOX] });
      const row = await rowOf('Studio Mac');
      expect(within(row).getByText('Checking')).toBeInTheDocument();
      expect(buttonsOf(row)).toEqual(['Replace', 'More actions']);
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
    expect(within(dialog).getByText(/This token stays on Lab box\./)).toBeInTheDocument();
    expect(within(dialog).getByText(/puts a token on Lab box/)).toBeInTheDocument();
    // Sign in leads; paste follows under it, folded until asked for.
    const signIn = within(dialog).getByRole('button', { name: 'Sign in with Claude' });
    fireEvent.click(within(dialog).getByRole('button', { name: 'Already have a setup token? Paste it' }));
    const paste = within(dialog).getByLabelText('Setup token');
    expect(signIn.compareDocumentPosition(paste) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    fireEvent.change(paste, { target: { value: TOKEN } });
    fireEvent.click(within(dialog).getByRole('button', { name: 'Save token' }));
    await waitFor(() => expect(screen.queryByRole('dialog', { name: 'Add a setup token for Lab box' })).toBeNull());
    expect(saveClaudeCodeComputerToken).toHaveBeenCalledWith('cmp-box', TOKEN);
    const row = await rowOf('Lab box');
    expect(within(row).getByText('Connected')).toBeInTheDocument();
    expect(JSON.stringify(queryClient.getQueryCache().getAll().map((q) => q.state.data))).not.toContain(TOKEN);
  });

  it("removes a computer's token from that computer, after asking", async () => {
    (removeClaudeCodeComputerToken as Mock).mockResolvedValue(undefined);
    render();
    clickRemove(await rowOf('Studio Mac'));
    const dialog = screen.getByRole('dialog', { name: 'Remove the setup token from Studio Mac?' });
    expect(within(dialog).getByText(/won't run there until you add a new one/)).toBeInTheDocument();
    fireEvent.click(within(dialog).getByRole('button', { name: 'Remove token' }));
    await waitFor(() => expect(removeClaudeCodeComputerToken).toHaveBeenCalledWith('cmp-mac'));
  });

  it('says the saved token takes over, where this server runs turns on one', async () => {
    render({ account: status({ state: 'signed_in', saved: true }) });
    const mac = await rowOf('Studio Mac');
    await within(mac).findByText('user@example.com', { exact: false });
    clickRemove(mac);
    expect(screen.getByText(/Claude Code there then uses your saved token/)).toBeInTheDocument();
  });

  it.each([
    ['no saved token runs here', status({ state: 'off', saved: true }), MAC],
    ['none is saved', status({ state: 'no_token', saved: false }), MAC],
    ['Claude refused the saved token', status({ state: 'rejected', saved: true }), MAC],
    ["the computer's policy keeps Claude Code from running", status({ state: 'signed_in', saved: true }), { ...MAC, state: 'policy_blocked' as const }],
    ['the computer has no perl', status({ state: 'signed_in', saved: true }), { ...MAC, state: 'needs_perl' as const }],
    ['the computer runs musl', status({ state: 'signed_in', saved: true }), { ...MAC, state: 'computer_unsupported' as const }],
  ])('says nothing of the saved token taking over where %s', async (_, account, entry) => {
    render({ account, computers: [entry] });
    clickRemove(await rowOf('Studio Mac'));
    expect(screen.getByText(/won't run there until you add a new one/)).toBeInTheDocument();
  });

  // The link opens the sheet the row's first button opens, titled for it.
  it.each([
    [MAC, 'Replace the setup token on Studio Mac'],
    [{ ...MAC, state: 'rejected' as const }, 'Add a setup token for Studio Mac'],
  ])("opens the sheet of the own computer a linked workspace is on", async (entry, title) => {
    render({ computers: [entry], route: claudeCodeTokenUrl('ws-mac'), workspaces: { 'ws-mac': 'cmp-mac' } });
    expect(await screen.findByRole('dialog', { name: title })).toBeInTheDocument();
  });

  it("opens the saved token's sheet from a link to a workspace on a hosted computer", async () => {
    render({ route: claudeCodeTokenUrl('ws-cloud'), workspaces: { 'ws-cloud': 'cmp-hosted' } });
    expect(await screen.findByRole('dialog', { name: 'Add a setup token' })).toBeInTheDocument();
  });

  // The saved token's row offers a sheet too, which must not open first.
  it('opens no sheet from a link until the workspace it names is read', async () => {
    let answer!: (w: unknown) => void;
    (getWorkspace as Mock).mockReturnValue(new Promise((r) => { answer = r; }));
    render({ route: claudeCodeTokenUrl('ws-mac') });
    await listRead();
    expect(screen.queryByRole('dialog')).toBeNull();
    await act(async () => { answer(workspace('ws-mac', 'cmp-mac')); });
    expect(await screen.findByRole('dialog', { name: 'Replace the setup token on Studio Mac' })).toBeInTheDocument();
  });

  // Which computer it runs on is unknown, so the saved token's sheet would
  // stand in for its computer's; the link stays for a read that answers.
  it('opens no sheet from a link while the workspace it names cannot be read, and keeps the link', async () => {
    (getWorkspace as Mock).mockRejectedValue(failed(500));
    const { queryClient } = render({ route: claudeCodeTokenUrl('ws-mac') });
    await listRead();
    await waitFor(() => expect(queryClient.getQueryState(queryKeys.workspaces.detail('ws-mac'))?.status).toBe('error'));
    await act(async () => {});
    expect(screen.queryByRole('dialog')).toBeNull();
    (getWorkspace as Mock).mockResolvedValue(workspace('ws-mac', 'cmp-mac'));
    await act(async () => { await queryClient.refetchQueries({ queryKey: queryKeys.workspaces.detail('ws-mac') }); });
    expect(await screen.findByRole('dialog', { name: 'Replace the setup token on Studio Mac' })).toBeInTheDocument();
  });

  it("opens no sheet from a link while only Home's placeholder row is held", async () => {
    let answer!: (w: unknown) => void;
    (getWorkspace as Mock).mockReturnValue(new Promise((r) => { answer = r; }));
    const { queryClient } = render({ route: claudeCodeTokenUrl('ws-home') });
    queryClient.setQueryData(queryKeys.workspaces.home(), { ...workspace('ws-home', ''), computer_id: null, is_home: true });
    await listRead();
    expect(screen.queryByRole('dialog')).toBeNull();
    await act(async () => { answer({ ...workspace('ws-home', 'cmp-mac'), is_home: true }); });
    expect(await screen.findByRole('dialog', { name: 'Replace the setup token on Studio Mac' })).toBeInTheDocument();
  });

  it.each([403, 404])("opens the saved token's sheet from a link to a workspace the server answers %i for", async (code) => {
    (getWorkspace as Mock).mockRejectedValue(failed(code));
    render({ route: claudeCodeTokenUrl('ws-gone') });
    expect(await screen.findByRole('dialog', { name: 'Add a setup token' })).toBeInTheDocument();
  });

  it("names the saved token in a group of its own beside the computers' rows, and only there", async () => {
    render({ account: status({ state: 'signed_in', saved: true }) });
    const saved = await savedRowOf();
    expect(within(saved).getByText('Your setup token')).toBeInTheDocument();
    expect(buttonsOf(saved)).toEqual(['Replace', 'More actions']);
    const computers = (await within(await view()).findByText('Your computers')).parentElement as HTMLElement;
    expect(within(computers).queryByText('Your setup token')).toBeNull();
    expect(within(computers).queryByText('Setup token')).toBeNull();
  });

  // A hosted deployment runs Claude Code only on the user's own computers.
  it('shows no saved token, and opens none from a link, where no saved token runs', async () => {
    render({ account: status({ state: 'off' }), route: '/?connect=claude-code' });
    await listRead();
    expect(screen.queryByRole('dialog')).toBeNull();
    await rowOf('Studio Mac');
    expect(screen.queryByText('Setup token')).toBeNull();
    expect(screen.queryByText('Runs Claude Code on this server')).toBeNull();
  });

  describe('with Claude Code off', () => {
    const left = () => render({
      flag: false,
      computers: [{ ...MAC, state: 'off', own_file: true }, { ...BOX, state: 'off' }],
      account: status({ enabled: false, state: 'off', saved: true }),
    });

    it('keeps one Claude Code row in the list while a token is left behind, saying it is off', async () => {
      left();
      const listRow = (await screen.findByText('Claude Code')).closest('li') as HTMLElement;
      expect(within(listRow).getByText('Off')).toBeInTheDocument();
      expect(within(listRow).getByText('Claude Code is off, but your setup token is still saved.')).toBeInTheDocument();
      expect(buttonsOf(listRow)).toEqual(['Manage']);
    });

    it('offers to remove the token a computer holds, and a first token to one that holds none', async () => {
      left();
      const mac = await rowOf('Studio Mac');
      expect(within(mac).getByText('Off')).toBeInTheDocument();
      expect(within(mac).getByText('Claude Code is off, but this computer still has its setup token.')).toBeInTheDocument();
      expect(buttonsOf(mac)).toEqual(['Remove']);
      expect(buttonsOf(await rowOf('Lab box'))).toEqual(['Add setup token']);
    });

    it('offers to remove the saved token left behind', async () => {
      left();
      const saved = await savedRowOf();
      expect(within(saved).getByText('Off')).toBeInTheDocument();
      expect(within(saved).getByText('Claude Code is off, but your setup token is still saved.')).toBeInTheDocument();
      expect(buttonsOf(saved)).toEqual(['Remove']);
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
