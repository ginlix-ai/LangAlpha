/**
 * Where the server lets the user choose how Claude Code runs on cloud
 * computers, Model access offers the choice. The Anthropic API key comes
 * first: without one saved, choosing it opens the key's sheet, and the
 * choice is made once the key is saved. Storing the setup token needs
 * consent, then the token; leaving it deletes the token, so it is confirmed.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { fireEvent, screen, waitFor, within } from '@testing-library/react';
import { useLocation } from 'react-router';
import { renderWithProviders } from '@/test/utils';
import { ANTHROPIC_KEY_URL, CLAUDE_CODE_CHOICE_URL } from '@/lib/claudeCode';
import type { ClaudeCodeCustody, ClaudeCodeSetup } from '@/api/model';

const saved = vi.hoisted(() => ({ current: false }));
// Claude Code is on; where it stands as a whole is the row's own line.
const setup = vi.hoisted(() => ({ current: {} as ClaudeCodeSetup }));

// A hosted server, which runs no turn on a saved token.
vi.mock('@/hooks/useClaudeCode', () => ({
  useClaudeCodeSetup: () => ({ setup: setup.current, isLoading: false }),
  useClaudeCodeStatus: () => ({ enabled: true, runsHere: false, state: 'off', saved: saved.current, email: null, expiresAt: null, isLoading: false }),
  useClaudeCodeComputers: () => ({ computers: [], isLoading: false }),
}));
// The catalog the Anthropic key's form comes from, read or not yet.
const catalog = vi.hoisted(() => ({ read: true }));
vi.mock('@/hooks/useProviderCatalog', () => {
  const option = { entry: { provider: 'anthropic', display_name: 'Anthropic', access_type: 'api_key' }, provider: 'anthropic', plan: 'api_key', region: '', base_url: null, sdk: null, use_response_api: false, models_from: 'anthropic' };
  const anthropic = { group: 'anthropic', name: 'Anthropic', category: 'closed_source', markProvider: 'anthropic', options: [option] };
  return { useProviderCatalog: () => ({ keyGroups: catalog.read ? [anthropic] : [], planGroups: [], localEntries: [], placeOf: () => undefined, optionFor: () => null }) };
});
vi.mock('@/hooks/useConfiguredProviders', () => ({
  useConfiguredProviders: () => ({ providers: [], withdrawn: [], configuredSet: new Set(), statusLoading: false, statusError: false }),
}));
vi.mock('@/api/model', () => ({ getClaudeCodeCustody: vi.fn(), setClaudeCodeCustody: vi.fn() }));
// The key form stands in for its own save, which its own tests cover.
vi.mock('../modelAccess/KeySheet', () => ({
  KeySheet: ({ onSaved, onCancel, onBack }: { onSaved: (p: string, fresh: boolean) => void; onCancel?: () => void; onBack?: () => void }) => (
    <>
      <button type="button" onClick={() => onSaved('anthropic', true)}>Save key</button>
      {onCancel && <button type="button" onClick={onCancel}>Cancel key</button>}
      {onBack && <button type="button" onClick={onBack}>Back</button>}
    </>
  ),
}));

import { getClaudeCodeCustody, setClaudeCodeCustody } from '@/api/model';
import { ModelAccessSection } from '../ModelAccessSection';

function Where() {
  const { pathname, search } = useLocation();
  return <span>{`at:${pathname}${search}`}</span>;
}

/** What the server offered before a token could be stored, unless a test says otherwise. */
const BASE: ClaudeCodeCustody = { mode: 'local', api_key_saved: false, offered: ['api_key'], blocked: false, runs_on: null, refusal: null, disclosure: '2026-11-v1' };

function render(custody: Partial<ClaudeCodeCustody> | null, route = '/settings?tab=model') {
  (getClaudeCodeCustody as Mock).mockResolvedValue(custody && { ...BASE, ...custody });
  return renderWithProviders(<><ModelAccessSection /><Where /></>, { route });
}

// Hidden too: the sheet they are in is inert while a dialog is open over it.
const ownComputers = () => screen.getByRole('radio', { name: 'Only on your own computers', hidden: true });
const apiKey = () => screen.getByRole('radio', { name: 'Also for cloud computers, with your Anthropic API key', hidden: true });
const stored = () => screen.queryByRole('radio', { name: 'Also for cloud computers, with your setup token stored by LangAlpha', hidden: true });
const keySheet = () => screen.queryByRole('dialog', { name: 'Connect Anthropic' });
const consentDialog = () => screen.queryByRole('dialog', { name: 'Let LangAlpha store your setup token?' });
const leaveDialog = () => screen.queryByRole('dialog', { name: 'Stop storing your setup token?' });
const tokenSheet = () => screen.queryByRole('dialog', { name: 'Add a setup token' });
const STORING: Partial<ClaudeCodeCustody> = { offered: ['api_key', 'stored'] };
/** Claude Code's row in Model access, whose Manage opens the view the choice is in. */
const openClaudeCode = async () => fireEvent.click(await screen.findByRole('button', { name: 'Manage' }));

describe('ModelAccessSection: Claude Code on cloud computers', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    saved.current = false;
    setup.current = { state: 'not_connected', on: true, computers: [], saved_token_runs: false, cloud: true, too_small: [] };
    catalog.read = true;
    (setClaudeCodeCustody as Mock).mockImplementation(async (mode: string) => ({ ...BASE, ...STORING, mode, api_key_saved: true }));
  });

  it('offers no choice where the server offers none', async () => {
    render(null);
    await openClaudeCode();
    await waitFor(() => expect(getClaudeCodeCustody).toHaveBeenCalled());
    await new Promise((r) => setTimeout(r, 20));
    expect(screen.queryByText('Where Claude Code runs')).toBeNull();
    expect(screen.queryByRole('radio')).toBeNull();
  });

  it('shows the choice only inside Claude Code, whose row says which is chosen', async () => {
    setup.current = { ...setup.current, state: 'connected' };
    render({ mode: 'api_key', api_key_saved: true, runs_on: 'api_key', refusal: null });
    expect(await screen.findByText('Also for cloud computers, with your Anthropic API key')).toBeInTheDocument();
    expect(screen.queryByRole('radiogroup')).toBeNull();
    await openClaudeCode();
    expect(within(screen.getByRole('dialog', { name: 'Claude Code' })).getByRole('radiogroup', { name: 'Where Claude Code runs' })).toBeInTheDocument();
    expect(apiKey()).toBeChecked();
  });

  it('opens Claude Code from a link to the choice', async () => {
    render({ mode: 'local' }, CLAUDE_CODE_CHOICE_URL);
    expect(await screen.findByRole('radiogroup', { name: 'Where Claude Code runs' })).toBeInTheDocument();
  });

  it('offers both ways, each with what it means, the current one picked', async () => {
    render({ mode: 'local', api_key_saved: false, runs_on: null, refusal: null });
    await openClaudeCode();
    expect(await screen.findByRole('radiogroup', { name: 'Where Claude Code runs' })).toBeInTheDocument();
    expect(ownComputers()).toBeChecked();
    expect(ownComputers()).toHaveAccessibleDescription(/^Claude Code runs on your own computers, each with its own setup token.*Workspaces on cloud computers can't use Claude Code models\.$/);
    expect(apiKey()).not.toBeChecked();
    expect(apiKey()).toHaveAccessibleDescription(/^For workspaces on Medium or larger cloud computers, Claude Code runs on the cloud computer itself.*Usage is billed to your Anthropic Console account/);
  });

  it('switches to the API key at once where one is saved', async () => {
    render({ mode: 'local', api_key_saved: true, runs_on: null, refusal: null });
    await openClaudeCode();
    fireEvent.click(await screen.findByText('Also for cloud computers, with your Anthropic API key'));
    await waitFor(() => expect(setClaudeCodeCustody).toHaveBeenCalledWith('api_key', undefined));
    await waitFor(() => expect(apiKey()).toBeChecked());
    expect(keySheet()).toBeNull();
  });

  it('shows the pick as made, and that it is saving, until the server answers', async () => {
    (setClaudeCodeCustody as Mock).mockReturnValue(new Promise(() => {}));
    render({ mode: 'local', api_key_saved: true, runs_on: null, refusal: null });
    await openClaudeCode();
    fireEvent.click(await screen.findByText('Also for cloud computers, with your Anthropic API key'));
    await waitFor(() => expect(apiKey()).toBeChecked());
    expect(screen.getByText('Saving...')).toBeInTheDocument();
  });

  it('asks for the key first where none is saved, and switches once it is saved', async () => {
    render({ mode: 'local', api_key_saved: false, runs_on: null, refusal: null });
    await openClaudeCode();
    fireEvent.click(await screen.findByText('Also for cloud computers, with your Anthropic API key'));
    expect(keySheet()).toBeInTheDocument();
    expect(keySheet()!.closest('[inert]')).toBeNull();
    expect(ownComputers()).toBeChecked();
    expect(screen.queryByRole('button', { name: 'Back' })).toBeNull();
    expect(setClaudeCodeCustody).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole('button', { name: 'Save key' }));
    await waitFor(() => expect(setClaudeCodeCustody).toHaveBeenCalledWith('api_key', undefined));
    await waitFor(() => expect(apiKey()).toBeChecked());
  });

  it('keeps the choice where the key is not saved', async () => {
    render({ mode: 'local', api_key_saved: false, runs_on: null, refusal: null });
    await openClaudeCode();
    fireEvent.click(await screen.findByText('Also for cloud computers, with your Anthropic API key'));
    fireEvent.click(screen.getByRole('button', { name: 'Cancel key' }));
    expect(keySheet()).toBeNull();
    expect(ownComputers()).toBeChecked();
    expect(setClaudeCodeCustody).not.toHaveBeenCalled();
  });

  it('makes the pick as it is while the key has no form to open, never leaving Claude Code covered', async () => {
    catalog.read = false;
    render({ mode: 'local', api_key_saved: false, runs_on: null, refusal: null });
    await openClaudeCode();
    fireEvent.click(await screen.findByText('Also for cloud computers, with your Anthropic API key'));
    await waitFor(() => expect(setClaudeCodeCustody).toHaveBeenCalledWith('api_key', undefined));
    expect(screen.getByRole('dialog', { name: 'Claude Code' }).closest('[inert]')).toBeNull();
  });

  it('switches back to the user\'s own computers', async () => {
    render({ mode: 'api_key', api_key_saved: true, runs_on: null, refusal: null });
    await openClaudeCode();
    fireEvent.click(await screen.findByText('Only on your own computers'));
    await waitFor(() => expect(setClaudeCodeCustody).toHaveBeenCalledWith('local', undefined));
    await waitFor(() => expect(ownComputers()).toBeChecked());
  });

  it('opens the Anthropic key from a link, which closing it drops', async () => {
    render({ mode: 'api_key', api_key_saved: false, runs_on: null, refusal: null }, ANTHROPIC_KEY_URL);
    expect(keySheet()).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Cancel key' }));
    expect(keySheet()).toBeNull();
    expect(screen.getByText(/^at:/).textContent).toBe('at:/settings?tab=model');
  });

  it('drops the link once the key is saved, which already runs Claude Code as chosen', async () => {
    render({ mode: 'api_key', api_key_saved: false, runs_on: null, refusal: null }, ANTHROPIC_KEY_URL);
    fireEvent.click(screen.getByRole('button', { name: 'Save key' }));
    expect(screen.getByText(/^at:/).textContent).toBe('at:/settings?tab=model');
    await new Promise((r) => setTimeout(r, 20));
    expect(setClaudeCodeCustody).not.toHaveBeenCalled();
  });

  it('lists storing the token where the server offers it, or while it is the one chosen', async () => {
    const { unmount } = render({ ...STORING });
    await openClaudeCode();
    await screen.findByRole('radiogroup', { name: 'Where Claude Code runs' });
    expect(stored()).not.toBeChecked();
    expect(stored()).toHaveAccessibleDescription(/^For workspaces on Medium or larger cloud computers, Claude Code runs on the cloud computer itself\. .*LangAlpha sends your setup token/);
    unmount();
    render({ mode: 'stored', offered: ['stored'] });
    await openClaudeCode();
    await screen.findByRole('radiogroup', { name: 'Where Claude Code runs' });
    expect(stored()).toBeChecked();
    expect(screen.queryByRole('radio', { name: 'Also for cloud computers, with your Anthropic API key' })).toBeNull();
  });

  it('says a choice the server no longer offers is paused, keeps it, and still lets the user move', async () => {
    saved.current = true;
    render({ mode: 'stored', offered: ['api_key'], refusal: 'hosted_paused' });
    await openClaudeCode();
    expect(await screen.findByText(/^Claude Code on cloud computers is paused on this server\./)).toBeInTheDocument();
    expect(stored()).toBeChecked();
    fireEvent.click(screen.getByText('Only on your own computers'));
    expect(leaveDialog()).toBeInTheDocument();
  });

  it('says the choice is paused where this server runs no Claude Code on cloud computers', async () => {
    render({ mode: 'api_key', api_key_saved: true, ...STORING, refusal: 'needs_own_computer' });
    await openClaudeCode();
    expect(await screen.findByText(/^Claude Code on cloud computers is paused on this server\./)).toBeInTheDocument();
  });

  it('says nothing is paused while the chosen mode is offered', async () => {
    render({ mode: 'api_key', api_key_saved: true, ...STORING });
    await openClaudeCode();
    await screen.findByRole('radiogroup', { name: 'Where Claude Code runs' });
    expect(screen.queryByText(/is paused on this server/)).toBeNull();
  });

  it('says storing a token is blocked for the user where the option would be', async () => {
    render({ blocked: true });
    await openClaudeCode();
    expect(await screen.findByText("Storing a setup token isn't available for your account. Claude Code still runs on your own computers.")).toBeInTheDocument();
    expect(stored()).toBeNull();
  });

  it('asks for consent before storing the token, then opens the token sheet where none is saved', async () => {
    render({ ...STORING });
    await openClaudeCode();
    fireEvent.click(await screen.findByText('Also for cloud computers, with your setup token stored by LangAlpha'));
    const dialog = consentDialog()!;
    expect(dialog.closest('[inert]')).toBeNull();
    expect(dialog).toHaveTextContent(/LangAlpha keeps your token encrypted.*servers also use the token to check.*Usage counts against your Claude plan.*Anthropic may limit or suspend accounts.*switch back at any time/);
    const terms = within(dialog).getByRole('link', { name: "Read Anthropic's terms" });
    expect(terms).toHaveAttribute('href', 'https://www.anthropic.com/legal/consumer-terms');
    expect(terms).toHaveAttribute('target', '_blank');
    expect(ownComputers()).toBeChecked();
    expect(setClaudeCodeCustody).not.toHaveBeenCalled();
    fireEvent.click(within(dialog).getByRole('button', { name: 'Store my token' }));
    await waitFor(() => expect(setClaudeCodeCustody).toHaveBeenCalledWith('stored', '2026-11-v1'));
    expect(await screen.findByRole('dialog', { name: 'Add a setup token' })).toBeInTheDocument();
    expect(consentDialog()).toBeNull();
    expect(stored()).toBeChecked();
  });

  it('opens no token sheet once the token is stored where one is saved already', async () => {
    saved.current = true;
    render({ ...STORING });
    await openClaudeCode();
    fireEvent.click(await screen.findByText('Also for cloud computers, with your setup token stored by LangAlpha'));
    fireEvent.click(screen.getByRole('button', { name: 'Store my token' }));
    await waitFor(() => expect(stored()).toBeChecked());
    expect(tokenSheet()).toBeNull();
  });

  it('stores nothing where consent is not given', async () => {
    render({ ...STORING });
    await openClaudeCode();
    fireEvent.click(await screen.findByText('Also for cloud computers, with your setup token stored by LangAlpha'));
    fireEvent.click(within(consentDialog()!).getByRole('button', { name: 'Cancel' }));
    expect(consentDialog()).toBeNull();
    expect(ownComputers()).toBeChecked();
    expect(setClaudeCodeCustody).not.toHaveBeenCalled();
  });

  it('confirms leaving the stored token, which deletes it', async () => {
    saved.current = true;
    render({ mode: 'stored', ...STORING });
    await openClaudeCode();
    fireEvent.click(await screen.findByText('Only on your own computers'));
    expect(leaveDialog()).toHaveTextContent(/LangAlpha deletes your stored setup token now, and Claude Code turns using it stop\./);
    expect(leaveDialog()!.closest('[inert]')).toBeNull();
    expect(setClaudeCodeCustody).not.toHaveBeenCalled();
    fireEvent.click(within(leaveDialog()!).getByRole('button', { name: 'Delete token' }));
    await waitFor(() => expect(setClaudeCodeCustody).toHaveBeenCalledWith('local', undefined));
    await waitFor(() => expect(leaveDialog()).toBeNull());
    expect(ownComputers()).toBeChecked();
  });

  it('keeps the stored token where leaving is not confirmed', async () => {
    saved.current = true;
    render({ mode: 'stored', ...STORING });
    await openClaudeCode();
    fireEvent.click(await screen.findByText('Only on your own computers'));
    fireEvent.click(within(leaveDialog()!).getByRole('button', { name: 'Cancel' }));
    expect(leaveDialog()).toBeNull();
    expect(stored()).toBeChecked();
    expect(setClaudeCodeCustody).not.toHaveBeenCalled();
  });

  it('says the token is still stored where leaving fails', async () => {
    (setClaudeCodeCustody as Mock).mockRejectedValue(new Error('unreachable'));
    saved.current = true;
    render({ mode: 'stored', ...STORING });
    await openClaudeCode();
    fireEvent.click(await screen.findByText('Only on your own computers'));
    fireEvent.click(within(leaveDialog()!).getByRole('button', { name: 'Delete token' }));
    expect(await screen.findByRole('alert')).toHaveTextContent("Couldn't switch. Your token is still stored. Try again.");
    expect(leaveDialog()).toBeNull();
    expect(stored()).toBeChecked();
  });

  it('says the token is deleted where leaving it stopped all but a turn on it', async () => {
    (setClaudeCodeCustody as Mock).mockRejectedValue(Object.assign(new Error('500'), { response: { status: 500, data: { error: { code: 'turns_running' } } } }));
    saved.current = true;
    render({ mode: 'stored', ...STORING });
    await openClaudeCode();
    fireEvent.click(await screen.findByText('Only on your own computers'));
    fireEvent.click(within(leaveDialog()!).getByRole('button', { name: 'Delete token' }));
    expect(await screen.findByRole('alert')).toHaveTextContent("Your token is deleted, but a Claude Code turn using it couldn't be stopped. Stop it in its thread.");
  });

  it('asks for the key before leaving for it, then confirms, and switches once confirmed', async () => {
    saved.current = true;
    render({ mode: 'stored', ...STORING });
    await openClaudeCode();
    fireEvent.click(await screen.findByText('Also for cloud computers, with your Anthropic API key'));
    expect(keySheet()).toBeInTheDocument();
    expect(leaveDialog()).toBeNull();
    fireEvent.click(screen.getByRole('button', { name: 'Save key' }));
    fireEvent.click(within(await waitFor(() => leaveDialog()!)).getByRole('button', { name: 'Delete token' }));
    await waitFor(() => expect(setClaudeCodeCustody).toHaveBeenCalledWith('api_key', undefined));
  });

  it('leaves at once when no token is stored yet', async () => {
    render({ mode: 'stored', ...STORING });
    await openClaudeCode();
    fireEvent.click(await screen.findByText('Only on your own computers'));
    expect(leaveDialog()).toBeNull();
    await waitFor(() => expect(setClaudeCodeCustody).toHaveBeenCalledWith('local', undefined));
  });

  it('deletes nothing when the key sheet for leaving closes unsaved', async () => {
    saved.current = true;
    render({ mode: 'stored', ...STORING });
    await openClaudeCode();
    fireEvent.click(await screen.findByText('Also for cloud computers, with your Anthropic API key'));
    fireEvent.click(within(keySheet()!).getByRole('button', { name: 'Cancel key' }));
    expect(leaveDialog()).toBeNull();
    expect(setClaudeCodeCustody).not.toHaveBeenCalled();
    expect(stored()).toBeChecked();
  });
  it('drops a pick made while another is saving, so only one choice is sent', async () => {
    (setClaudeCodeCustody as Mock).mockReturnValue(new Promise(() => {}));
    render({ mode: 'local', api_key_saved: true, runs_on: null, refusal: null });
    await openClaudeCode();
    await screen.findByRole('radiogroup', { name: 'Where Claude Code runs' });
    fireEvent.click(apiKey());
    await waitFor(() => expect(setClaudeCodeCustody).toHaveBeenCalledTimes(1));
    fireEvent.click(ownComputers());
    await new Promise((r) => setTimeout(r, 20));
    expect(setClaudeCodeCustody).toHaveBeenCalledTimes(1);
  });
  it.each([
    ['api_key_endpoint', /^Your Anthropic API key is set to a custom Base URL/],
    ['needs_api_key', /^Claude Code on cloud computers has no Anthropic API key to use/],
  ])('says why the chosen API key cannot run (%s)', async (refusal, line) => {
    render({ mode: 'api_key', api_key_saved: true, runs_on: null, refusal });
    await openClaudeCode();
    expect(await screen.findByText(line)).toBeInTheDocument();
  });
});
