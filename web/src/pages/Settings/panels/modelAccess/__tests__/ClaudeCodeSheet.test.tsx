/**
 * Claude Code's view in Model access: what each state says and offers,
 * adding a first token turns Claude Code on before its sheet opens, and
 * Turn off asks first.
 */
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { fireEvent, screen, waitFor, within } from '@testing-library/react';
import { useLocation } from 'react-router';
import { queryOptions } from '@tanstack/react-query';
import { renderWithProviders } from '@/test/utils';
import type { ClaudeCodeChiefOfStaff, ClaudeCodeCliPlacement, ClaudeCodeComputer, ClaudeCodeSetup } from '@/api/model';
import type { TokenRow } from '@/components/modelAccess/tokenLine';

const h = vi.hoisted(() => ({
  toggle: vi.fn(),
  remote: false,
  opened: [] as unknown[],
  gate: 'opt_in' as string,
  custody: null as { mode: string } | null,
  statusOn: false,
  spec: [] as string[],
  medium: {} as Record<string, 'resize' | 'plan'>,
  planUrl: null as string | null,
  platform: false,
  cliComputer: null as ClaudeCodeCliPlacement | null,
  computers: [] as ClaudeCodeComputer[],
  clisMax: null as number | null,
  setClis: vi.fn(),
  models: {} as Record<string, { models: string[] }>,
  modelsLoading: false,
  metadata: {} as Record<string, { sdk?: string; model_id?: string }>,
  defaultModel: null as string | null,
  home: vi.fn(),
}));

vi.mock('@/hooks/useFeatures', () => ({
  useFeatures: () => ({ data: [{ key: 'claude_code_runtime', gate: h.gate }] }),
  useSetFeatureOverride: () => ({ mutateAsync: h.toggle, isPending: false }),
}));
vi.mock('@/hooks/useClaudeCode', () => ({
  useClaudeCodeStatus: () => ({ enabled: h.statusOn }),
  useClaudeCodeComputers: () => ({ cliComputer: h.cliComputer, computers: h.computers, clisMax: h.clisMax }),
  useSetComputerClis: (computerId: string) => ({ mutate: (clis: number) => h.setClis(computerId, clis), isPending: false }),
}));
vi.mock('@/hooks/useAllModels', () => ({ useAllModels: () => ({ models: h.models, metadata: h.metadata, isLoading: h.modelsLoading }) }));
vi.mock('@/hooks/useSlotDefaultModels', () => ({ useDefaultModel: () => h.defaultModel }));
vi.mock('@/hooks/useHomeWorkspace', () => ({
  homeWorkspaceQuery: () => queryOptions({ queryKey: ['workspaces', 'home'], queryFn: () => h.home() }),
}));
vi.mock('@/hooks/useClaudeCodeCustody', () => ({ useClaudeCodeCustody: () => ({ custody: h.custody }) }));
vi.mock('../ClaudeCodeHostedChoice', () => ({ ClaudeCodeHostedChoice: () => null }));
vi.mock('../ClaudeCodePlanOutChoice', () => ({ ClaudeCodePlanOutChoice: () => null }));
vi.mock('@/components/modelAccess/ClaudeCodeToken', () => ({
  ClaudeCodeTokenModal: ({ target }: { target: { kind: string; name?: string } }) => (
    <div role="dialog" aria-label="token sheet">{target.kind === 'computer' ? target.name : 'saved'}</div>
  ),
  TokenActions: ({ actions, onOpen }: { actions: string[]; onOpen: (a: string) => void }) => (
    <>{actions.map((a) => <button key={a} type="button" onClick={() => onOpen(a === 'replace' ? 'replace' : 'add')}>{a}</button>)}</>
  ),
}));
vi.mock('@/lib/desktop', () => ({ canRemoteControl: () => h.remote }));
vi.mock('@/pages/ChatAgent/hooks/computerPanelStore', () => ({
  openComputersPanel: (p: unknown) => h.opened.push(p),
  useComputersPanel: () => ({ specComputerId: null }),
}));
vi.mock('@/pages/ChatAgent/hooks/useMediumAction', () => ({
  useMediumAction: (id: string) => {
    const kind = h.medium[id];
    return kind ? { kind, go: () => h.spec.push(id) } : null;
  },
}));
vi.mock('@/config/supportLinks', async (original) => ({
  ...(await original<typeof import('@/config/supportLinks')>()),
  get PLAN_URL() { return h.planUrl; },
}));
vi.mock('@/config/hostMode', async (original) => ({
  ...(await original<typeof import('@/config/hostMode')>()),
  get isPlatformMode() { return h.platform; },
}));
vi.mock('../../usePreferenceWrite', () => ({ useSaveFailedToast: () => vi.fn() }));

import { ClaudeCodeSheet } from '../ClaudeCodeSheet';

const mac = (state: ClaudeCodeComputer['state'], extra: Partial<ClaudeCodeComputer> = {}): ClaudeCodeComputer => ({
  computer_id: 'mac', name: 'MacBook Pro', kind: 'helper', state, source: null, email: null, expires_at: null, ...extra,
});
const setupOf = (over: Partial<ClaudeCodeSetup>): ClaudeCodeSetup => ({ state: 'no_computer', on: false, computers: [], saved_token_runs: false, cloud: false, too_small: [], ...over });

function Where() {
  const { pathname, state } = useLocation();
  return <span>{`at:${pathname}`}<i data-testid="state">{JSON.stringify(state)}</i></span>;
}

function show(setup: ClaudeCodeSetup | null, tokenRows: TokenRow[] = [], onLeave?: () => void) {
  return renderWithProviders(
    <>
      <ClaudeCodeSheet setup={setup} tokenRows={tokenRows} onRemoveToken={vi.fn()} onCover={vi.fn()} onLeave={onLeave} />
      <Where />
    </>,
    { route: '/settings?tab=model' },
  );
}

describe('ClaudeCodeSheet', () => {
  beforeEach(() => {
    h.medium = {};
    h.toggle.mockReset().mockResolvedValue([]);
    h.remote = false;
    h.opened = [];
    h.gate = 'opt_in';
    h.custody = null;
    h.platform = false;
    h.statusOn = false;
    h.spec = [];
    h.planUrl = null;
    h.cliComputer = null;
    h.computers = [];
    h.clisMax = null;
    h.setClis.mockReset();
    h.models = { 'claude-code': { models: ['claude-fable-5-2-code', 'claude-opus-5-5-code', 'claude-sonnet-5-5-code'] } };
    h.metadata = {
      'claude-fable-5-2-code': { sdk: 'claude-code', model_id: 'claude-fable-5-2' },
      'claude-opus-5-5-code': { sdk: 'claude-code', model_id: 'claude-opus-5-5' },
      'claude-sonnet-5-5-code': { sdk: 'claude-code', model_id: 'claude-sonnet-5-5' },
      'gpt-x': {},
    };
    h.defaultModel = null;
    h.modelsLoading = false;
    h.home.mockReset().mockResolvedValue({ workspace_id: 'home-1' });
  });

  const macRow = (line: TokenRow['line']): TokenRow => ({
    key: 'mac', target: { kind: 'computer', computerId: 'mac', name: 'MacBook Pro' } as TokenRow['target'], tag: 'MacBook Pro', line,
    credential: { kind: 'computer', computerId: 'mac' }, checkAfter: null, checkBills: false,
  });

  it('once on, reads a computer the setup read found out of reach as offline, whatever its token last said', () => {
    show(setupOf({ state: 'offline', on: true, computers: [mac('offline')] }), [macRow({ status: 'connected', detail: 'Its own token', actions: ['replace', 'remove'] })]);
    const row = screen.getByText('MacBook Pro', { selector: 'span' }).closest('li') as HTMLElement;
    expect(within(row).getByText('Offline')).toBeInTheDocument();
    expect(within(row).queryByRole('button', { name: 'replace' })).toBeNull();
    fireEvent.click(within(row).getByRole('button'));
    expect(h.opened).toEqual([{ view: 'details', computerId: 'mac' }]);
  });

  it('once on, sets on a computer\'s row how many agents it runs at once, from 1 to the most the server allows', async () => {
    h.computers = [mac('signed_in', { clis: 4 })];
    h.clisMax = 8;
    show(setupOf({ state: 'connected', on: true, computers: h.computers }), [macRow({ status: 'connected', detail: 'Its own token', actions: ['replace'] })]);
    const row = screen.getByText('MacBook Pro', { selector: 'span' }).closest('li') as HTMLElement;
    const select = within(row).getByLabelText('Claude Code agents at once') as HTMLSelectElement;
    expect(select.value).toBe('4');
    expect(Array.from(select.options, (o) => o.value)).toEqual(['1', '2', '3', '4', '5', '6', '7', '8']);
    expect(select).toHaveAccessibleDescription(/Above 4, LangAlpha opens a second connection/);
    expect(screen.getByText('MacBook Pro', { selector: 'span' }).parentElement).not.toContainElement(select);
    fireEvent.change(select, { target: { value: '6' } });
    expect(h.setClis).toHaveBeenCalledWith('mac', 6);
  });

  it('offers no count on a computer whose entry carries none, from a server that predates it', () => {
    h.computers = [mac('signed_in')];
    h.clisMax = 8;
    show(setupOf({ state: 'connected', on: true, computers: h.computers }), [macRow({ status: 'connected', detail: 'Its own token', actions: ['replace'] })]);
    expect(screen.queryByLabelText('Claude Code agents at once')).toBeNull();
  });

  it.each([
    ['before Claude Code is on', false, 8],
    ['on a server that predates it', true, null],
  ])('offers no count %s', (_, on, max) => {
    h.computers = [mac('signed_in', { clis: 4 })];
    h.clisMax = max;
    show(setupOf({ state: 'connected', on, computers: h.computers }), [macRow({ status: 'connected', detail: 'Its own token', actions: ['replace'] })]);
    expect(screen.queryByLabelText('Claude Code agents at once')).toBeNull();
  });

  it('with the setup unread, takes whether it is on from the status read, so adding a token turns nothing on', async () => {
    h.statusOn = true;
    show(null, [macRow({ status: 'not_connected', detail: 'No token', actions: ['add'] })]);
    fireEvent.click(screen.getByRole('button', { name: 'add' }));
    expect(await screen.findByRole('dialog', { name: 'token sheet' })).toBeInTheDocument();
    expect(h.toggle).not.toHaveBeenCalled();
  });

  it('with no computer, says what the mode is, says so, and offers both ways to connect one', () => {
    show(setupOf({}));
    expect(screen.getByText('Connect a computer of your own first')).toBeInTheDocument();
    expect(screen.getByRole('link', { name: /Get the Mac app/ })).toHaveAttribute('href', 'https://langalpha.ai/download');
    fireEvent.click(screen.getByRole('button', { name: 'Add SSH computer' }));
    expect(h.opened).toEqual([{ view: 'create' }]);
    expect(screen.getByText(/^Runs Claude Code on your Claude plan\./)).toBeInTheDocument();
    expect(screen.queryByText(/differs from the other sign-ins/)).toBeNull();
    expect(screen.getByRole('link', { name: /Learn more/ })).toHaveAttribute('href', 'https://docs.langalpha.ai/concepts/claude-code/#requirements');
    expect(screen.queryByRole('button', { name: 'Turn off Claude Code' })).toBeNull();
  });

  it.each([
    ['no_computer', '#requirements'],
    ['unsupported', '#requirements'],
    ['offline', '#where-it-runs-and-when-it-can-run'],
    ['not_connected', '#how-it-runs'],
    ['connected', '#how-it-runs'],
  ] as const)('while %s, links the docs section on it', (state, anchor) => {
    show(setupOf({ state, on: true, computers: state === 'offline' ? [mac('offline')] : [] }));
    expect(screen.getAllByRole('link', { name: /Learn more/ })[0]).toHaveAttribute('href', 'https://docs.langalpha.ai/concepts/claude-code/' + anchor);
  });

  it('where cloud computers could run it, does not say they cannot yet', () => {
    show(setupOf({ cloud: true }));
    expect(screen.getByText('Connect one below, then come back here to sign in to Claude.')).toBeInTheDocument();
    expect(screen.queryByText(/Cloud computers can't run Claude Code yet/)).toBeNull();
  });

  const TOO_SMALL = [{ computer_id: 'c-3', name: 'Scratch' }, { computer_id: 'c-4', name: 'Backtests' }];

  it('names each cloud computer too small for it, with what gets the one picked to Medium', () => {
    h.medium = { 'c-3': 'plan', 'c-4': 'resize' };
    show(setupOf({ state: 'not_connected', on: true, too_small: TOO_SMALL }));
    const row = screen.getByText('Backtests').closest('li') as HTMLElement;
    expect(within(row).getByText('Claude Code runs on Medium or larger. Changing it restarts it for a few minutes.')).toBeInTheDocument();
    fireEvent.click(within(row).getByRole('button', { name: 'Change to Medium' }));
    expect(h.spec).toEqual(['c-4']);
    const scratch = screen.getByText('Scratch').closest('li') as HTMLElement;
    expect(within(scratch).getByText("Claude Code runs on Medium or larger, and Medium isn't on your plan.")).toBeInTheDocument();
    fireEvent.click(within(scratch).getByRole('button', { name: 'Upgrade plan' }));
    expect(h.spec).toEqual(['c-4', 'c-3']);
  });

  it('states the requirement alone where nothing gets the computer to Medium', () => {
    show(setupOf({ state: 'not_connected', on: true, too_small: TOO_SMALL.slice(0, 1) }));
    const row = screen.getByText('Scratch').closest('li') as HTMLElement;
    expect(within(row).getByText('Claude Code runs on Medium or larger cloud computers.')).toBeInTheDocument();
    expect(within(row).queryByRole('button')).toBeNull();
  });

  it('leaves the computers too small to the choice while the user runs it on their own computers', () => {
    h.custody = { mode: 'local' };
    show(setupOf({ state: 'not_connected', on: true, cloud: true, too_small: TOO_SMALL }));
    expect(screen.queryByText('Too small for Claude Code')).toBeNull();
  });

  it('reads a server that predates naming the cloud computers too small for it', () => {
    show({ ...setupOf({ state: 'not_connected', on: true }), too_small: undefined });
    expect(screen.queryByText('Too small for Claude Code')).toBeNull();
  });

  it('says where the setup token runs: this server, or its cloud computers only while they run Claude Code', () => {
    const { unmount } = show(setupOf({ state: 'not_connected', saved_token_runs: true }));
    expect(screen.getByText('Runs Claude Code on this server')).toBeInTheDocument();
    unmount();
    const sandboxed = show(setupOf({ state: 'not_connected', saved_token_runs: true, cli_in_sandbox: true }));
    expect(screen.getByText('Runs Claude Code on your cloud computers')).toBeInTheDocument();
    sandboxed.unmount();
    h.platform = true;
    const cloud = show(setupOf({ state: 'not_connected', saved_token_runs: true, cloud: true }));
    expect(screen.getByText('Runs Claude Code on your cloud computers')).toBeInTheDocument();
    cloud.unmount();
    show(setupOf({ state: 'not_connected', saved_token_runs: true }));
    expect(screen.getByText('Your setup token')).toBeInTheDocument();
    expect(screen.queryByText(/^Runs Claude Code on (this server|your cloud computers)/)).toBeNull();
  });

  it('names no cloud computer where none is too small', () => {
    show(setupOf({ state: 'not_connected', on: true, cloud: true }));
    expect(screen.queryByText(/Medium or larger cloud computer, or a computer of your own/)).toBeNull();
    expect(screen.queryByRole('button', { name: 'Change spec' })).toBeNull();
  });

  it('after turning it off, offers to turn it back on to use the token a computer kept, beside the saved one', async () => {
    show(setupOf({ state: 'not_connected', saved_token_runs: true, computers: [mac('off', { source: 'machine' })] }));
    expect(screen.getByText('Turn Claude Code back on')).toBeInTheDocument();
    expect(screen.getByText('MacBook Pro still has its setup token. Turn on Claude Code to use it again.')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Turn on Claude Code' }));
    await waitFor(() => expect(h.toggle).toHaveBeenCalledWith({ key: 'claude_code_runtime', enabled: true }));
  });

  it('where cloud computers offer a way to run it, offers Turn on beside a computer that has no token', () => {
    show(setupOf({ state: 'not_connected', cloud: true, computers: [mac('off')] }));
    expect(screen.getByRole('button', { name: 'Turn on Claude Code' })).toBeInTheDocument();
  });

  it('where only a cloud computer would run it, asks to set it up rather than to sign in', () => {
    show(setupOf({ state: 'not_connected', cloud: true }));
    expect(screen.getByText('Set up Claude Code to start')).toBeInTheDocument();
    expect(screen.getByText('Turn it on to choose where it runs.')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Turn on Claude Code' })).toBeInTheDocument();
  });

  it('in the Mac app, turns on Remote control from the Computers form instead of linking the download', () => {
    h.remote = true;
    show(setupOf({}));
    fireEvent.click(screen.getByRole('button', { name: 'Turn on Remote control' }));
    expect(h.opened).toEqual([{ view: 'create', pick: 'thisMac' }]);
  });

  it('names an offline computer and what to check there', () => {
    show(setupOf({ state: 'offline', computers: [mac('offline')] }));
    expect(screen.getByText('MacBook Pro is offline')).toBeInTheDocument();
    const row = screen.getByText('MacBook Pro').closest('li') as HTMLElement;
    expect(within(row).getByText('Open LangAlpha on this Mac and check that Remote control is on.')).toBeInTheDocument();
    fireEvent.click(within(row).getByRole('button', { name: 'View computer' }));
    expect(h.opened).toEqual([{ view: 'details', computerId: 'mac' }]);
  });

  it('adding a first token turns Claude Code on, then opens that computer\'s token sheet', async () => {
    show(setupOf({ state: 'not_connected', computers: [mac('off')] }));
    expect(screen.getByText('Sign in to Claude to start')).toBeInTheDocument();
    fireEvent.click(within(screen.getByText('MacBook Pro').closest('li') as HTMLElement).getByRole('button', { name: 'add' }));
    await waitFor(() => expect(screen.getByRole('dialog', { name: 'token sheet' })).toHaveTextContent('MacBook Pro'));
    expect(h.toggle).toHaveBeenCalledWith({ key: 'claude_code_runtime', enabled: true });
  });

  it('opens no token sheet where turning Claude Code on fails', async () => {
    h.toggle.mockRejectedValue(new Error('down'));
    show(setupOf({ state: 'not_connected', computers: [mac('off')] }));
    fireEvent.click(screen.getByRole('button', { name: 'add' }));
    await waitFor(() => expect(h.toggle).toHaveBeenCalled());
    expect(screen.queryByRole('dialog', { name: 'token sheet' })).toBeNull();
  });

  it('where this server runs turns on a saved token, offers one before Claude Code is on', async () => {
    show(setupOf({ state: 'not_connected', saved_token_runs: true }));
    expect(screen.getByText('Setup token')).toBeInTheDocument();
    expect(screen.getByText('Your setup token')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'add' }));
    await waitFor(() => expect(screen.getByRole('dialog', { name: 'token sheet' })).toHaveTextContent('saved'));
  });

  it('once connected, turns off only once asked', async () => {
    const line = { status: 'connected' as const, detail: 'Uses its own token', actions: ['replace' as const, 'remove' as const] };
    show(
      setupOf({ state: 'connected', on: true, computers: [mac('signed_in', { source: 'machine' })] }),
      [{ key: 'mac', target: { kind: 'computer', computerId: 'mac', name: 'MacBook Pro' }, tag: 'MacBook Pro', line, credential: { kind: 'computer', computerId: 'mac' }, checkAfter: null, checkBills: false }],
    );
    expect(screen.getByText('Claude Code is ready')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Turn off Claude Code' }));
    expect(h.toggle).not.toHaveBeenCalled();
    fireEvent.click(within(screen.getByRole('dialog', { name: 'Turn off Claude Code?' })).getByRole('button', { name: 'Turn off' }));
    await waitFor(() => expect(h.toggle).toHaveBeenCalledWith({ key: 'claude_code_runtime', enabled: false }));
    await waitFor(() => expect(screen.queryByRole('dialog', { name: 'Turn off Claude Code?' })).toBeNull());
  });

  // The line names the computer the cloud workspaces run on only where the
  // Chief of Staff runs there too, which is what it says.
  const cos = (over: Partial<ClaudeCodeChiefOfStaff>): ClaudeCodeChiefOfStaff => ({
    kind: 'picked', computer_id: 'mac', name: 'MacBook Pro', refusal: null, default_runs: true, ...over,
  });
  const PICKED: ClaudeCodeCliPlacement = { kind: 'picked', computer_id: 'mac', name: 'MacBook Pro', refusal: null };
  const lab = (state: ClaudeCodeComputer['state'], extra: Partial<ClaudeCodeComputer> = {}) =>
    mac(state, { computer_id: 'lab', name: 'Lab box', source: 'machine', ...extra });
  const HOME_LAB = cos({ kind: 'own', computer_id: 'lab', name: 'Lab box' });
  const HOME_SAYS = 'Your Chief of Staff runs it on Lab box.';
  it.each([
    ['both on the picked computer', cos({}), PICKED, [], 'It runs on MacBook Pro for your cloud workspaces and Chief of Staff.'],
    ["Home's own cloud computer", cos({ kind: 'home_cloud', computer_id: 'cmp-home', name: 'Home desk' }), null, [], 'Your Chief of Staff runs it on Home desk.'],
    ["Home's own computer, the cloud workspaces on a picked one", HOME_LAB, PICKED, [lab('signed_in')], HOME_SAYS],
    ["Home's own computer on a token not checked yet", HOME_LAB, PICKED, [lab('unverified')], HOME_SAYS],
    ["Home's own computer with no token, another one connected", HOME_LAB, PICKED, [lab('no_token', { source: null })], null],
    ["Home's own computer whose token is refused", HOME_LAB, PICKED, [lab('rejected')], null],
    ["Home's own computer whose token has expired", HOME_LAB, PICKED, [lab('signed_in', { expires_at: '2000-01-01T00:00:00Z' })], null],
    ["Home's own computer missing from the computers read", HOME_LAB, PICKED, [], null],
    ['a picked computer the cloud workspaces do not run on', cos({ computer_id: 'lab', name: 'Lab box' }), PICKED, [], null],
    ['a picked computer while the cloud workspaces run their own', cos({}), null, [], null],
    ['a picked computer the cloud workspaces read as refused', cos({}), { ...PICKED, refusal: 'cli_computer_offline' as const }, [], null],
    ['a refusal', cos({ refusal: 'cli_computer_offline' }), PICKED, [], null],
    ["this server's workers", cos({ kind: 'server', computer_id: null, name: null }), null, [], null],
    ['Claude Code off', null, null, [], null],
  ])('once ready, says where the Chief of Staff runs Claude Code: %s', (_, chiefOfStaff, cliComputer, more, words) => {
    h.cliComputer = cliComputer;
    show(setupOf({ state: 'connected', on: true, computers: [mac('signed_in', { source: 'machine' }), ...more], chief_of_staff: chiefOfStaff }));
    expect(screen.getByText('Claude Code is ready')).toBeInTheDocument();
    expect(screen.queryByText(/Chief of Staff/)?.textContent ?? null).toBe(words);
  });

  const SMALL_HOME = cos({ kind: 'home_cloud', computer_id: 'cmp-home', name: 'Cloud computer', refusal: 'needs_bigger_computer' });

  it("while Home's cloud computer is too small and no computer of their own runs it, says it is almost ready, with no chat to start", () => {
    show(setupOf({ state: 'connected', on: true, computers: [], chief_of_staff: SMALL_HOME }));
    expect(screen.getByRole('heading', { name: 'Claude Code is almost ready' })).toBeInTheDocument();
    expect(screen.getByText('It runs once Cloud computer is Medium or larger.')).toBeInTheDocument();
    expect(screen.queryByText('Claude Code is ready')).toBeNull();
    expect(screen.queryByRole('button', { name: 'Start a chat' })).toBeNull();
  });

  it("is ready all the same when a computer of their own has a token that works, though Home's is too small", () => {
    show(setupOf({ state: 'connected', on: true, computers: [mac('signed_in', { source: 'machine' })], chief_of_staff: SMALL_HOME }));
    expect(screen.getByRole('heading', { name: 'Claude Code is ready' })).toBeInTheDocument();
    expect(screen.queryByText('Claude Code is almost ready')).toBeNull();
    // A chat starts in Home, whose computer still refuses it.
    expect(screen.queryByRole('button', { name: 'Start a chat' })).toBeNull();
  });

  it('offers no chat where no Claude Code model can be read, so none starts on the Default', () => {
    h.models = {};
    show(setupOf({ state: 'connected', on: true, computers: [mac('signed_in', { source: 'machine' })] }));
    expect(screen.getByRole('heading', { name: 'Claude Code is ready' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Start a chat' })).toBeNull();
  });

  const startChat = async (default_: string | null) => {
    h.defaultModel = default_;
    const onLeave = vi.fn();
    show(setupOf({ state: 'connected', on: true, computers: [mac('signed_in', { source: 'machine' })] }), [], onLeave);
    fireEvent.click(screen.getByRole('button', { name: 'Start a chat' }));
    await waitFor(() => expect(onLeave).toHaveBeenCalledTimes(1));
    return JSON.parse(screen.getByTestId('state').textContent ?? 'null');
  };

  it("once ready, offers a chat in Home on Claude Code's Sonnet when the account's default is not a Claude Code model", async () => {
    const state = await startChat('gpt-x');
    expect(screen.getByText(/^at:\/chat\/t\/__default__/)).toBeInTheDocument();
    expect(state).toMatchObject({ workspaceId: 'home-1', model: 'claude-sonnet-5-5-code' });
  });

  it('holds Start a chat until the models are read, so the chat does not start on the Default', () => {
    h.modelsLoading = true;
    show(setupOf({ state: 'connected', on: true, computers: [mac('signed_in', { source: 'machine' })] }));
    expect(screen.getByRole('button', { name: 'Start a chat' })).toBeDisabled();
  });

  it('starts on the first Claude Code model where none is a Sonnet', async () => {
    h.models = { 'claude-code': { models: ['claude-fable-5-2-code', 'claude-opus-5-5-code'] } };
    expect(await startChat('gpt-x')).toMatchObject({ model: 'claude-fable-5-2-code' });
  });

  it("starts the chat on the account's default when it is a Claude Code model", async () => {
    expect(await startChat('claude-opus-5-5-code')).toMatchObject({ model: 'claude-opus-5-5-code' });
  });

  it('goes nowhere when Home is read after the user left the view', async () => {
    let answer: (home: { workspace_id: string }) => void = () => {};
    h.home.mockReturnValue(new Promise((resolve) => { answer = resolve; }));
    const onLeave = vi.fn();
    const { unmount } = show(setupOf({ state: 'connected', on: true, computers: [mac('signed_in', { source: 'machine' })] }), [], onLeave);
    fireEvent.click(screen.getByRole('button', { name: 'Start a chat' }));
    await waitFor(() => expect(h.home).toHaveBeenCalled());
    unmount();
    answer({ workspace_id: 'home-1' });
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(onLeave).not.toHaveBeenCalled();
  });

  it('falls back to the chat page where Home cannot be read', async () => {
    h.home.mockRejectedValue(new Error('no home'));
    await startChat(null);
    expect(screen.getByText(/^at:\/chat$/)).toBeInTheDocument();
  });

  it('while a token is checked, leads with a title and a sentence as every other state does', () => {
    show(setupOf({ state: 'checking', on: true, computers: [mac('checking')] }));
    expect(screen.getByRole('heading', { name: 'Checking your setup tokens' })).toBeInTheDocument();
    expect(screen.getByText('LangAlpha is asking Claude whether each token works.')).toBeInTheDocument();
  });

  it('offers the ways to connect a computer where the user chose their own computers and has none', () => {
    h.custody = { mode: 'local' };
    const onLeave = vi.fn();
    show(setupOf({ state: 'not_connected', on: true, cloud: true }), [], onLeave);
    fireEvent.click(screen.getByRole('button', { name: 'Add SSH computer' }));
    expect(h.opened).toEqual([{ view: 'create' }]);
    expect(screen.getByText('at:/chat')).toBeInTheDocument();
    expect(onLeave).toHaveBeenCalledTimes(1);
  });

  it.each([
    ['their token is stored for cloud computers', { mode: 'stored' }, [], true],
    ['they have a computer of their own', { mode: 'local' }, [mac('offline')], true],
    ['Claude Code is off, so where it runs is not chosen yet', { mode: 'local' }, [], false],
  ])('offers no way to connect a computer where %s', (_, custody, computers, on) => {
    h.custody = custody;
    show(setupOf({ state: 'not_connected', on, cloud: true, computers }));
    expect(screen.queryByText('Ways to connect a computer')).toBeNull();
  });

  it('warns that turning off removes the token stored for cloud computers', () => {
    h.custody = { mode: 'stored' };
    show(setupOf({ state: 'connected', on: true }));
    fireEvent.click(screen.getByRole('button', { name: 'Turn off Claude Code' }));
    expect(screen.getByText(/LangAlpha deletes the token stored for cloud computers/)).toBeInTheDocument();
  });

  // An operator's gate the user cannot override offers no switch either way.
  it('offers neither turning on nor off where the user cannot change the flag', () => {
    h.gate = 'none';
    show(setupOf({ state: 'connected', on: true }));
    expect(screen.queryByRole('button', { name: 'Turn off Claude Code' })).toBeNull();
    h.gate = 'plan';
    show(setupOf({ state: 'not_connected', computers: [mac('off')] }));
    expect(screen.queryByRole('button', { name: 'add' })).toBeNull();
  });
});
