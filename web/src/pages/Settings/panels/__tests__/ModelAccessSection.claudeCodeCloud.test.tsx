/**
 * Where the server lets the user choose how Claude Code runs on cloud
 * computers, Model access offers the choice. The Anthropic API key comes
 * first: without one saved, choosing it opens the key's sheet, and the
 * choice is made once the key is saved.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { fireEvent, screen, waitFor, within } from '@testing-library/react';
import { useLocation } from 'react-router';
import { renderWithProviders } from '@/test/utils';
import { ANTHROPIC_KEY_URL, CLAUDE_CODE_CHOICE_URL } from '@/lib/claudeCode';
import type { ClaudeCodeCustody } from '@/api/model';

// A hosted server, which runs no turn on a saved token.
vi.mock('@/hooks/useClaudeCode', () => ({
  useClaudeCodeStatus: () => ({ enabled: true, runsHere: false, state: 'off', saved: false, email: null, expiresAt: null, isLoading: false }),
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

function render(custody: ClaudeCodeCustody | null, route = '/settings?tab=model') {
  (getClaudeCodeCustody as Mock).mockResolvedValue(custody);
  return renderWithProviders(<><ModelAccessSection /><Where /></>, { route });
}

const ownComputers = () => screen.getByRole('radio', { name: 'Only on your own computers' });
const apiKey = () => screen.getByRole('radio', { name: 'Also for cloud computers, with your Anthropic API key' });
const keySheet = () => screen.queryByRole('dialog', { name: 'Connect Anthropic' });
/** Claude Code's row in Model access, which opens the sheet the choice is in. */
const openClaudeCode = async () => fireEvent.click(await screen.findByRole('button', { name: 'Manage' }));

describe('ModelAccessSection: Claude Code on cloud computers', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    catalog.read = true;
    (setClaudeCodeCustody as Mock).mockImplementation(async (mode: string) => ({ mode, api_key_saved: true, runs_on: null, refusal: null }));
  });

  it('offers no choice where the server offers none', async () => {
    render(null);
    await waitFor(() => expect(getClaudeCodeCustody).toHaveBeenCalled());
    await new Promise((r) => setTimeout(r, 20));
    expect(screen.queryByText('Where Claude Code runs')).toBeNull();
    expect(screen.queryByRole('radio')).toBeNull();
  });

  it('shows the choice only inside Claude Code, whose row says which is chosen', async () => {
    render({ mode: 'api_key', api_key_saved: true, runs_on: 'api_key', refusal: null });
    expect(await screen.findByText('Also for cloud computers, with your Anthropic API key')).toBeInTheDocument();
    expect(screen.queryByRole('radiogroup')).toBeNull();
    await openClaudeCode();
    expect(within(screen.getByRole('dialog', { name: 'Claude Code' })).getByRole('radiogroup', { name: 'Where Claude Code runs' })).toBeInTheDocument();
    expect(apiKey()).toBeChecked();
  });

  it('opens Claude Code from a link to the choice', async () => {
    render({ mode: 'local', api_key_saved: false, runs_on: null, refusal: null }, CLAUDE_CODE_CHOICE_URL);
    expect(await screen.findByRole('radiogroup', { name: 'Where Claude Code runs' })).toBeInTheDocument();
  });

  it('offers both ways, each with what it means, the current one picked', async () => {
    render({ mode: 'local', api_key_saved: false, runs_on: null, refusal: null });
    await openClaudeCode();
    expect(await screen.findByRole('radiogroup', { name: 'Where Claude Code runs' })).toBeInTheDocument();
    expect(ownComputers()).toBeChecked();
    expect(ownComputers()).toHaveAccessibleDescription(/^Claude Code runs on your own computers, each with its own setup token.*Workspaces on cloud computers can't use Claude Code models\.$/);
    expect(apiKey()).not.toBeChecked();
    expect(apiKey()).toHaveAccessibleDescription(/^For workspaces on cloud computers, LangAlpha runs Claude Code on its servers with your API key.*Usage is billed to your Anthropic Console account/);
  });

  it('switches to the API key at once where one is saved', async () => {
    render({ mode: 'local', api_key_saved: true, runs_on: null, refusal: null });
    await openClaudeCode();
    fireEvent.click(await screen.findByText('Also for cloud computers, with your Anthropic API key'));
    await waitFor(() => expect(setClaudeCodeCustody).toHaveBeenCalledWith('api_key'));
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
    expect(screen.getByRole('radio', { name: 'Only on your own computers', hidden: true })).toBeChecked();
    expect(screen.queryByRole('button', { name: 'Back' })).toBeNull();
    expect(setClaudeCodeCustody).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole('button', { name: 'Save key' }));
    await waitFor(() => expect(setClaudeCodeCustody).toHaveBeenCalledWith('api_key'));
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
    await waitFor(() => expect(setClaudeCodeCustody).toHaveBeenCalledWith('api_key'));
    expect(screen.getByRole('dialog', { name: 'Claude Code' }).closest('[inert]')).toBeNull();
  });

  it('switches back to the user\'s own computers', async () => {
    render({ mode: 'api_key', api_key_saved: true, runs_on: null, refusal: null });
    await openClaudeCode();
    fireEvent.click(await screen.findByText('Only on your own computers'));
    await waitFor(() => expect(setClaudeCodeCustody).toHaveBeenCalledWith('local'));
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
