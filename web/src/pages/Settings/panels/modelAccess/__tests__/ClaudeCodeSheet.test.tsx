/**
 * Claude Code's view in Model access: what each state says and offers,
 * adding a first token turns Claude Code on before its sheet opens, and
 * Turn off asks first.
 */
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { fireEvent, screen, waitFor, within } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import type { ClaudeCodeComputer, ClaudeCodeSetup } from '@/api/model';
import type { TokenRow } from '@/components/modelAccess/tokenLine';

const h = vi.hoisted(() => ({
  toggle: vi.fn(),
  remote: false,
  opened: [] as unknown[],
  gate: 'opt_in' as string,
  custody: null as { mode: string } | null,
  statusOn: false,
}));

vi.mock('@/hooks/useFeatures', () => ({
  useFeatures: () => ({ data: [{ key: 'claude_code_runtime', gate: h.gate }] }),
  useSetFeatureOverride: () => ({ mutateAsync: h.toggle, isPending: false }),
}));
vi.mock('@/hooks/useClaudeCode', () => ({ useClaudeCodeStatus: () => ({ enabled: h.statusOn }) }));
vi.mock('@/hooks/useClaudeCodeCustody', () => ({ useClaudeCodeCustody: () => ({ custody: h.custody }) }));
vi.mock('../ClaudeCodeHostedChoice', () => ({ ClaudeCodeHostedChoice: () => null }));
vi.mock('@/components/modelAccess/ClaudeCodeToken', () => ({
  ClaudeCodeTokenModal: ({ target }: { target: { kind: string; name?: string } }) => (
    <div role="dialog" aria-label="token sheet">{target.kind === 'computer' ? target.name : 'saved'}</div>
  ),
  TokenActions: ({ actions, onOpen }: { actions: string[]; onOpen: (a: string) => void }) => (
    <>{actions.map((a) => <button key={a} type="button" onClick={() => onOpen(a === 'replace' ? 'replace' : 'add')}>{a}</button>)}</>
  ),
}));
vi.mock('@/lib/desktop', () => ({ canRemoteControl: () => h.remote }));
vi.mock('@/pages/ChatAgent/hooks/computerPanelStore', () => ({ openComputersPanel: (p: unknown) => h.opened.push(p) }));
vi.mock('../../usePreferenceWrite', () => ({ useSaveFailedToast: () => vi.fn() }));

import { ClaudeCodeSheet } from '../ClaudeCodeSheet';

const mac = (state: ClaudeCodeComputer['state'], extra: Partial<ClaudeCodeComputer> = {}): ClaudeCodeComputer => ({
  computer_id: 'mac', name: 'MacBook Pro', kind: 'helper', state, source: null, email: null, expires_at: null, ...extra,
});
const setupOf = (over: Partial<ClaudeCodeSetup>): ClaudeCodeSetup => ({ state: 'no_computer', on: false, computers: [], saved_token_runs: false, cloud: false, ...over });

function show(setup: ClaudeCodeSetup | null, tokenRows: TokenRow[] = []) {
  return renderWithProviders(
    <ClaudeCodeSheet setup={setup} tokenRows={tokenRows} anthropicKey={null} configured={[]} onRemoveToken={vi.fn()} onCover={vi.fn()} />,
  );
}

describe('ClaudeCodeSheet', () => {
  beforeEach(() => {
    h.toggle.mockReset().mockResolvedValue([]);
    h.remote = false;
    h.opened = [];
    h.gate = 'opt_in';
    h.custody = null;
    h.statusOn = false;
  });

  const macRow = (line: TokenRow['line']): TokenRow => ({ key: 'mac', target: { kind: 'computer', computerId: 'mac', name: 'MacBook Pro' } as TokenRow['target'], tag: 'MacBook Pro', line });

  it('once on, reads a computer the setup read found out of reach as offline, whatever its token last said', () => {
    show(setupOf({ state: 'offline', on: true, computers: [mac('offline')] }), [macRow({ status: 'connected', detail: 'Its own token', actions: ['replace', 'remove'] })]);
    const row = screen.getByText('MacBook Pro', { selector: 'span' }).closest('li') as HTMLElement;
    expect(within(row).getByText('Offline')).toBeInTheDocument();
    expect(within(row).queryByRole('button', { name: 'replace' })).toBeNull();
    fireEvent.click(within(row).getByRole('button'));
    expect(h.opened).toEqual([{ view: 'details', computerId: 'mac' }]);
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
    expect(screen.getByText(/runs Anthropic's Claude Code through the Claude Agent SDK/)).toBeInTheDocument();
    expect(screen.queryByText(/differs from the other sign-ins/)).toBeNull();
    expect(screen.getByRole('link', { name: /Learn more/ })).toHaveAttribute('href', 'https://docs.langalpha.ai/concepts/claude-code/#requirements');
    expect(screen.queryByRole('button', { name: 'Turn off Claude Code' })).toBeNull();
  });

  it('where cloud computers run it too, says so instead of saying they cannot yet', () => {
    show(setupOf({ cloud: true }));
    expect(screen.getByText(/or a Medium or larger cloud computer, and none of yours can run it yet/)).toBeInTheDocument();
    expect(screen.queryByText(/Cloud computers can't run Claude Code yet/)).toBeNull();
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
    expect(screen.getByText('Turn on Claude Code to choose how it runs on cloud computers.')).toBeInTheDocument();
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
    expect(screen.getByText('Saved token')).toBeInTheDocument();
    expect(screen.getByText('For computers without a token')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'add' }));
    await waitFor(() => expect(screen.getByRole('dialog', { name: 'token sheet' })).toHaveTextContent('saved'));
  });

  it('once connected, turns off only once asked', async () => {
    const line = { status: 'connected' as const, detail: 'Uses its own token', actions: ['replace' as const, 'remove' as const] };
    show(
      setupOf({ state: 'connected', on: true, computers: [mac('signed_in', { source: 'machine' })] }),
      [{ key: 'mac', target: { kind: 'computer', computerId: 'mac', name: 'MacBook Pro' }, tag: 'MacBook Pro', line }],
    );
    expect(screen.getByText('Claude Code is ready')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Turn off Claude Code' }));
    expect(h.toggle).not.toHaveBeenCalled();
    fireEvent.click(within(screen.getByRole('dialog', { name: 'Turn off Claude Code?' })).getByRole('button', { name: 'Turn off' }));
    await waitFor(() => expect(h.toggle).toHaveBeenCalledWith({ key: 'claude_code_runtime', enabled: false }));
    await waitFor(() => expect(screen.queryByRole('dialog', { name: 'Turn off Claude Code?' })).toBeNull());
  });

  it('while a token is checked, leads with a title and a sentence as every other state does', () => {
    show(setupOf({ state: 'checking', on: true, computers: [mac('checking')] }));
    expect(screen.getByRole('heading', { name: 'Checking your setup tokens' })).toBeInTheDocument();
    expect(screen.getByText('LangAlpha is asking Claude whether each token works.')).toBeInTheDocument();
  });

  it('warns that turning off removes the token stored for cloud computers', () => {
    h.custody = { mode: 'stored' };
    show(setupOf({ state: 'connected', on: true }));
    fireEvent.click(screen.getByRole('button', { name: 'Turn off Claude Code' }));
    expect(screen.getByText(/the setup token for cloud computers is removed/)).toBeInTheDocument();
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
