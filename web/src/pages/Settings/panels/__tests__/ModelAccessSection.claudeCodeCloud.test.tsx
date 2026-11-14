/**
 * Where the server lets the user choose how Claude Code runs on cloud
 * computers, Model access offers the choice. Storing the setup token needs
 * consent, then the token; leaving it deletes the token, so it is confirmed.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { fireEvent, screen, waitFor, within } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import { CLAUDE_CODE_CHOICE_URL } from '@/lib/claudeCode';
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
vi.mock('@/api/model', () => ({ getClaudeCodeCustody: vi.fn(), setClaudeCodeCustody: vi.fn() }));

import { getClaudeCodeCustody, setClaudeCodeCustody } from '@/api/model';
import { ModelAccessSection } from '../ModelAccessSection';

/** What the server offers, unless a test says otherwise. */
const BASE: ClaudeCodeCustody = { mode: 'local', offered: ['stored'], blocked: false, runs_on: null, refusal: null };

function render(custody: Partial<ClaudeCodeCustody> | null, route = '/settings?tab=model') {
  (getClaudeCodeCustody as Mock).mockResolvedValue(custody && { ...BASE, ...custody });
  return renderWithProviders(<ModelAccessSection />, { route });
}

// Hidden too: the sheet they are in is inert while a dialog is open over it.
const ownComputers = () => screen.getByRole('radio', { name: 'On your own computers', hidden: true });
const stored = () => screen.queryByRole('radio', { name: 'On cloud computers, with your setup token stored by LangAlpha', hidden: true });
const consentDialog = () => screen.queryByRole('dialog', { name: 'Let LangAlpha store your setup token?' });
const leaveDialog = () => screen.queryByRole('dialog', { name: 'Stop storing your setup token?' });
const tokenSheet = () => screen.queryByRole('dialog', { name: 'Add a setup token' });
/** Claude Code's row in Model access, whose Manage opens the view the choice is in. */
const openClaudeCode = async () => fireEvent.click(await screen.findByRole('button', { name: 'Manage' }));

describe('ModelAccessSection: Claude Code on cloud computers', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    saved.current = false;
    setup.current = { state: 'not_connected', on: true, computers: [], saved_token_runs: false, cloud: true, too_small: [] };
    (setClaudeCodeCustody as Mock).mockImplementation(async (mode: string) => ({ ...BASE, mode }));
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
    render({ mode: 'stored', runs_on: 'stored_token', refusal: null });
    expect(await screen.findByText('On cloud computers, with your setup token stored by LangAlpha')).toBeInTheDocument();
    expect(screen.queryByRole('radiogroup')).toBeNull();
    await openClaudeCode();
    expect(within(screen.getByRole('dialog', { name: 'Claude Code' })).getByRole('radiogroup', { name: 'Where Claude Code runs' })).toBeInTheDocument();
    expect(stored()).toBeChecked();
  });

  it('opens Claude Code from a link to the choice', async () => {
    render({ mode: 'local' }, CLAUDE_CODE_CHOICE_URL);
    expect(await screen.findByRole('radiogroup', { name: 'Where Claude Code runs' })).toBeInTheDocument();
  });

  it('offers both ways, each with what it means, the current one picked', async () => {
    render({ mode: 'local', runs_on: null, refusal: null });
    await openClaudeCode();
    expect(await screen.findByRole('radiogroup', { name: 'Where Claude Code runs' })).toBeInTheDocument();
    expect(ownComputers()).toBeChecked();
    expect(ownComputers()).toHaveAccessibleDescription("A computer of yours runs it, with a setup token LangAlpha doesn't keep.");
    expect(within(screen.getByRole('region', { name: 'Where Claude Code runs' })).getByRole('link', { name: /Learn more/ }))
      .toHaveAttribute('href', 'https://docs.langalpha.ai/concepts/claude-code/#where-it-runs-and-when-it-can-run');
    expect(stored()).not.toBeChecked();
    expect(stored()).toHaveAccessibleDescription('Medium or larger cloud computers run it. LangAlpha keeps your setup token encrypted.');
  });

  it('shows the pick as made, and that it is saving, until the server answers', async () => {
    (setClaudeCodeCustody as Mock).mockReturnValue(new Promise(() => {}));
    render({ mode: 'local', runs_on: null, refusal: null });
    await openClaudeCode();
    fireEvent.click(await screen.findByText('On cloud computers, with your setup token stored by LangAlpha'));
    fireEvent.click(screen.getByRole('button', { name: 'Store my token' }));
    await waitFor(() => expect(stored()).toBeChecked());
    expect(screen.getByText('Saving...')).toBeInTheDocument();
  });

  it('lists storing the token where the server offers it, or while it is the one chosen', async () => {
    const { unmount } = render({});
    await openClaudeCode();
    await screen.findByRole('radiogroup', { name: 'Where Claude Code runs' });
    expect(stored()).not.toBeChecked();
    expect(stored()).toHaveAccessibleDescription(/^Medium or larger cloud computers run it\./);
    unmount();
    render({ mode: 'stored', offered: [] });
    await openClaudeCode();
    await screen.findByRole('radiogroup', { name: 'Where Claude Code runs' });
    expect(stored()).toBeChecked();
  });

  it('says the choice is paused where this server runs no Claude Code on cloud computers, keeps it, and still lets the user move', async () => {
    saved.current = true;
    render({ mode: 'stored', offered: [], refusal: 'needs_own_computer' });
    await openClaudeCode();
    expect(await screen.findByText(/^Paused on cloud computers on this server\./)).toBeInTheDocument();
    expect(stored()).toBeChecked();
    fireEvent.click(screen.getByText('On your own computers'));
    expect(leaveDialog()).toBeInTheDocument();
  });

  it('says nothing is paused while the chosen mode is offered', async () => {
    render({ mode: 'stored' });
    await openClaudeCode();
    await screen.findByRole('radiogroup', { name: 'Where Claude Code runs' });
    expect(screen.queryByText(/is paused on this server/)).toBeNull();
  });

  it('says storing a token is blocked for the user where the option would be', async () => {
    render({ blocked: true, offered: [] });
    await openClaudeCode();
    expect(await screen.findByText("Storing a setup token isn't available for your account. Claude Code still runs on your own computers.")).toBeInTheDocument();
    expect(stored()).toBeNull();
  });

  it('asks for consent before storing the token, then opens the token sheet where none is saved', async () => {
    render({});
    await openClaudeCode();
    fireEvent.click(await screen.findByText('On cloud computers, with your setup token stored by LangAlpha'));
    const dialog = consentDialog()!;
    expect(dialog.closest('[inert]')).toBeNull();
    expect(dialog).toHaveTextContent(/LangAlpha keeps your token encrypted.*servers also use the token to check.*Usage counts against your Claude plan.*Anthropic may limit or suspend accounts.*switch back at any time/);
    const terms = within(dialog).getByRole('link', { name: "Read Anthropic's terms" });
    expect(terms).toHaveAttribute('href', 'https://www.anthropic.com/legal/consumer-terms');
    expect(terms).toHaveAttribute('target', '_blank');
    expect(ownComputers()).toBeChecked();
    expect(setClaudeCodeCustody).not.toHaveBeenCalled();
    fireEvent.click(within(dialog).getByRole('button', { name: 'Store my token' }));
    // The version of the copy this bundle showed, not the one the server reads out.
    await waitFor(() => expect(setClaudeCodeCustody).toHaveBeenCalledWith('stored', '2026-11-v2'));
    expect(await screen.findByRole('dialog', { name: 'Add a setup token' })).toBeInTheDocument();
    expect(consentDialog()).toBeNull();
    expect(stored()).toBeChecked();
  });

  it('opens no token sheet once the token is stored where one is saved already', async () => {
    saved.current = true;
    render({});
    await openClaudeCode();
    fireEvent.click(await screen.findByText('On cloud computers, with your setup token stored by LangAlpha'));
    fireEvent.click(screen.getByRole('button', { name: 'Store my token' }));
    await waitFor(() => expect(stored()).toBeChecked());
    expect(tokenSheet()).toBeNull();
  });

  it('stores nothing where consent is not given', async () => {
    render({});
    await openClaudeCode();
    fireEvent.click(await screen.findByText('On cloud computers, with your setup token stored by LangAlpha'));
    fireEvent.click(within(consentDialog()!).getByRole('button', { name: 'Cancel' }));
    expect(consentDialog()).toBeNull();
    expect(ownComputers()).toBeChecked();
    expect(setClaudeCodeCustody).not.toHaveBeenCalled();
  });

  it('confirms leaving the stored token, which deletes it', async () => {
    saved.current = true;
    render({ mode: 'stored' });
    await openClaudeCode();
    fireEvent.click(await screen.findByText('On your own computers'));
    expect(leaveDialog()).toHaveTextContent(/LangAlpha deletes it now, and turns using it stop\./);
    expect(leaveDialog()!.closest('[inert]')).toBeNull();
    expect(setClaudeCodeCustody).not.toHaveBeenCalled();
    fireEvent.click(within(leaveDialog()!).getByRole('button', { name: 'Delete token' }));
    await waitFor(() => expect(setClaudeCodeCustody).toHaveBeenCalledWith('local', undefined));
    await waitFor(() => expect(leaveDialog()).toBeNull());
    expect(ownComputers()).toBeChecked();
  });

  it('keeps the stored token where leaving is not confirmed', async () => {
    saved.current = true;
    render({ mode: 'stored' });
    await openClaudeCode();
    fireEvent.click(await screen.findByText('On your own computers'));
    fireEvent.click(within(leaveDialog()!).getByRole('button', { name: 'Cancel' }));
    expect(leaveDialog()).toBeNull();
    expect(stored()).toBeChecked();
    expect(setClaudeCodeCustody).not.toHaveBeenCalled();
  });

  it('says the token is still stored where leaving fails', async () => {
    (setClaudeCodeCustody as Mock).mockRejectedValue(new Error('unreachable'));
    saved.current = true;
    render({ mode: 'stored' });
    await openClaudeCode();
    fireEvent.click(await screen.findByText('On your own computers'));
    fireEvent.click(within(leaveDialog()!).getByRole('button', { name: 'Delete token' }));
    expect(await screen.findByRole('alert')).toHaveTextContent("Couldn't switch. Your token is still stored. Try again.");
    expect(leaveDialog()).toBeNull();
    expect(stored()).toBeChecked();
  });

  it('says the token is deleted where leaving it stopped all but a turn on it', async () => {
    (setClaudeCodeCustody as Mock).mockRejectedValue(Object.assign(new Error('500'), { response: { status: 500, data: { error: { code: 'turns_running' } } } }));
    saved.current = true;
    render({ mode: 'stored' });
    await openClaudeCode();
    fireEvent.click(await screen.findByText('On your own computers'));
    fireEvent.click(within(leaveDialog()!).getByRole('button', { name: 'Delete token' }));
    expect(await screen.findByRole('alert')).toHaveTextContent("Your token is deleted, but a Claude Code turn using it couldn't be stopped. Stop it in its thread.");
  });

  it('leaves at once when no token is stored yet', async () => {
    render({ mode: 'stored' });
    await openClaudeCode();
    fireEvent.click(await screen.findByText('On your own computers'));
    expect(leaveDialog()).toBeNull();
    await waitFor(() => expect(setClaudeCodeCustody).toHaveBeenCalledWith('local', undefined));
  });

  it('drops a pick made while another is saving, so only one choice is sent', async () => {
    (setClaudeCodeCustody as Mock).mockReturnValue(new Promise(() => {}));
    render({ mode: 'stored', runs_on: null, refusal: null });
    await openClaudeCode();
    await screen.findByRole('radiogroup', { name: 'Where Claude Code runs' });
    fireEvent.click(ownComputers());
    await waitFor(() => expect(setClaudeCodeCustody).toHaveBeenCalledTimes(1));
    fireEvent.click(stored()!);
    await new Promise((r) => setTimeout(r, 20));
    expect(consentDialog()).toBeNull();
    expect(setClaudeCodeCustody).toHaveBeenCalledTimes(1);
  });
});
