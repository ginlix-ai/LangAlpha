import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, screen, fireEvent, waitFor, within } from '@testing-library/react';
import '@testing-library/jest-dom';
import React from 'react';
import { renderWithProviders } from '@/test/utils';

/**
 * `SecretsManager` is one state machine behind two ports: the workspace Vault
 * tab drives it with imperative API calls + an explicit reload, Plugins →
 * Secrets drives it with React Query mutations. Every branch below is exercised
 * through a real adapter rather than through hand-passed props, because the
 * interesting failures live in the wiring — a mutation shape the component
 * doesn't call the way the adapter expects, or a rejection the adapter swallows
 * before the shared error region can render it.
 *
 * Coverage is split to avoid restating what `SandboxSettingsPanel.test.tsx`
 * already pins on the workspace side (blueprints, the regex hint, the load
 * generation guard, form reset on workspace switch, create + refetch): here the
 * workspace adapter covers the read/reveal/delete half and the error paths.
 */

// ---------------------------------------------------------------------------
// Hook boundary — the user (Plugins) adapter
// ---------------------------------------------------------------------------

const userVault = {
  create: vi.fn(),
  update: vi.fn(),
  del: vi.fn(),
};

interface UserVaultData {
  secrets: Array<{
    user_vault_secret_id: string;
    name: string;
    description: string;
    masked_value: string;
    created_at: string;
    updated_at: string;
  }>;
  remaining_slots: number;
}

let userVaultData: UserVaultData | undefined;
let userVaultError: Error | null = null;
let userVaultLoading = false;

vi.mock('@/hooks/useUserVault', () => ({
  useUserVaultSecrets: () => ({ data: userVaultData, isLoading: userVaultLoading, error: userVaultError }),
  useUserVaultBlueprints: () => ({ data: { blueprints: [], remaining_slots: 0 } }),
  useCreateUserVaultSecret: () => ({ mutateAsync: userVault.create, isPending: false }),
  useUpdateUserVaultSecret: () => ({ mutateAsync: userVault.update, isPending: false }),
  useDeleteUserVaultSecret: () => ({ mutateAsync: userVault.del, isPending: false }),
}));

// ---------------------------------------------------------------------------
// API boundary — the workspace adapter (imperative calls + reload) plus the
// one direct call the user adapter makes (`revealUserVaultSecret`).
// ---------------------------------------------------------------------------

const wsVault = {
  get: vi.fn(),
  create: vi.fn(),
  update: vi.fn(),
  del: vi.fn(),
  reveal: vi.fn(),
  blueprints: vi.fn(),
};
const mockRevealUserVaultSecret = vi.fn();
const mockGetSandboxStats = vi.fn();

vi.mock('@/pages/ChatAgent/utils/api', async (importOriginal) => {
  const actual = await importOriginal<Record<string, unknown>>();
  return {
    ...actual,
    // `formatApiErrorDetail` stays real — the error copy is what's under test.
    getVaultSecrets: (...a: unknown[]) => wsVault.get(...a),
    createVaultSecret: (...a: unknown[]) => wsVault.create(...a),
    updateVaultSecret: (...a: unknown[]) => wsVault.update(...a),
    deleteVaultSecret: (...a: unknown[]) => wsVault.del(...a),
    revealVaultSecret: (...a: unknown[]) => wsVault.reveal(...a),
    getVaultBlueprints: (...a: unknown[]) => wsVault.blueprints(...a),
    getSandboxStats: (...a: unknown[]) => mockGetSandboxStats(...a),
    installSandboxPackages: vi.fn(),
    refreshWorkspace: vi.fn(),
    revealUserVaultSecret: (...a: unknown[]) => mockRevealUserVaultSecret(...a),
  };
});

// The real api module is imported (for `formatApiErrorDetail`), and its
// transport layer reads `api.defaults.baseURL` at module scope — so the stub
// needs that shape, not just the verbs.
vi.mock('@/api/client', () => ({
  api: {
    get: vi.fn(), post: vi.fn(), put: vi.fn(), patch: vi.fn(), delete: vi.fn(),
    defaults: { baseURL: '' },
  },
}));

import { PluginSecrets } from '../components/PluginSecrets';
import { SandboxSettingsContent } from '@/pages/ChatAgent/components/SandboxSettingsPanel';

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

function userSecret(name: string, overrides: Partial<UserVaultData['secrets'][number]> = {}) {
  return {
    user_vault_secret_id: `uvs-${name}`,
    name,
    description: '',
    masked_value: 'sk-…abcd',
    created_at: '2026-01-01T00:00:00Z',
    updated_at: '2026-01-01T00:00:00Z',
    ...overrides,
  };
}

function wsSecret(name: string, description = '') {
  return {
    workspace_vault_secret_id: `wvs-${name}`,
    name,
    description,
    masked_value: 'tok-…9f21',
    created_at: '2026-01-01T00:00:00Z',
    updated_at: '2026-01-01T00:00:00Z',
  };
}

/** Mount the workspace port: the settings panel's Vault tab. */
function renderWorkspaceVault(workspaceId = 'ws-1') {
  const view = renderWithProviders(<SandboxSettingsContent workspaceId={workspaceId} />);
  fireEvent.click(screen.getByRole('button', { name: /vault/i }));
  return view;
}

beforeEach(() => {
  vi.clearAllMocks();
  userVaultData = { secrets: [], remaining_slots: 20 };
  userVaultError = null;
  userVaultLoading = false;
  wsVault.get.mockResolvedValue([]);
  wsVault.blueprints.mockResolvedValue({ blueprints: [], remaining_slots: 20 });
  mockGetSandboxStats.mockResolvedValue({
    state: 'running', sandbox_id: 'sandbox-abc', resources: {}, packages: [], skills: [], mcp_servers: [],
  });
});

// ===========================================================================
// User adapter — Plugins → Secrets (React Query mutations)
// ===========================================================================

describe('SecretsManager via the user vault adapter — list', () => {
  it('renders each secret with its masked value and description', () => {
    userVaultData = {
      secrets: [
        userSecret('ALPHA_TOKEN', { description: 'used by the alpha connector' }),
        userSecret('BETA_TOKEN'),
      ],
      remaining_slots: 18,
    };
    renderWithProviders(<PluginSecrets />);

    expect(screen.getByText('ALPHA_TOKEN')).toBeInTheDocument();
    expect(screen.getByText('BETA_TOKEN')).toBeInTheDocument();
    expect(screen.getByText('used by the alpha connector')).toBeInTheDocument();
    expect(screen.getAllByText('sk-…abcd')).toHaveLength(2);
    // The count line reflects secrets + remaining slots, not a hardcoded cap.
    expect(screen.getByText('2 / 20')).toBeInTheDocument();
  });

  it('shows the user-scope empty state and hint', () => {
    renderWithProviders(<PluginSecrets />);
    expect(screen.getByText(/No secrets stored. Add API keys or credentials for your servers/i)).toBeInTheDocument();
    expect(screen.getByText(/available in every workspace/i)).toBeInTheDocument();
  });

  it('surfaces a load failure', () => {
    userVaultError = new Error('vault unreachable');
    renderWithProviders(<PluginSecrets />);
    expect(screen.getByText('vault unreachable')).toBeInTheDocument();
  });

  it('renders the skeleton while loading, not an empty list', () => {
    userVaultLoading = true;
    userVaultData = undefined;
    renderWithProviders(<PluginSecrets />);
    expect(screen.queryByText(/No secrets stored/i)).not.toBeInTheDocument();
  });

  it('hides Add Secret at the cap', () => {
    userVaultData = { secrets: [userSecret('ONLY_TOKEN')], remaining_slots: 0 };
    renderWithProviders(<PluginSecrets />);
    expect(screen.queryByRole('button', { name: /add secret/i })).not.toBeInTheDocument();
  });
});

describe('SecretsManager via the user vault adapter — create', () => {
  it('creates through the mutation and closes the form', async () => {
    userVault.create.mockResolvedValue({ name: 'NEW_TOKEN' });
    renderWithProviders(<PluginSecrets />);

    fireEvent.click(screen.getByRole('button', { name: /add secret/i }));
    fireEvent.change(screen.getByPlaceholderText('SECRET_NAME'), { target: { value: 'NEW_TOKEN' } });
    fireEvent.change(screen.getByPlaceholderText('Secret value'), { target: { value: 'placeholder-value' } });
    fireEvent.change(screen.getByPlaceholderText('Description (optional)'), { target: { value: 'for testing' } });
    fireEvent.click(screen.getByRole('button', { name: /^save$/i }));

    await waitFor(() =>
      expect(userVault.create).toHaveBeenCalledWith({
        name: 'NEW_TOKEN', value: 'placeholder-value', description: 'for testing',
      }),
    );
    await waitFor(() => expect(screen.queryByPlaceholderText('Secret value')).not.toBeInTheDocument());
  });

  it('normalizes the typed name so the mutation only ever sees a legal one', async () => {
    // The name field is the guard: it upper-cases, drops anything outside
    // [A-Z0-9_], and strips leading digits, so what reaches `onCreate` always
    // satisfies the backend's name rule. (`vault.nameInvalid` still backstops
    // the prefill deep-link, which sets the name without passing through here.)
    userVault.create.mockResolvedValue({ name: 'BAD_NAME' });
    renderWithProviders(<PluginSecrets />);

    fireEvent.click(screen.getByRole('button', { name: /add secret/i }));
    const nameInput = screen.getByPlaceholderText('SECRET_NAME');
    fireEvent.change(nameInput, { target: { value: '9bad-name!' } });
    expect((nameInput as HTMLInputElement).value).toBe('BADNAME');

    fireEvent.change(screen.getByPlaceholderText('Secret value'), { target: { value: 'x' } });
    fireEvent.click(screen.getByRole('button', { name: /^save$/i }));

    await waitFor(() =>
      expect(userVault.create).toHaveBeenCalledWith({ name: 'BADNAME', value: 'x', description: undefined }),
    );
  });

  it('keeps Save disabled until both a name and a value are present', () => {
    renderWithProviders(<PluginSecrets />);

    fireEvent.click(screen.getByRole('button', { name: /add secret/i }));
    const save = screen.getByRole('button', { name: /^save$/i });
    expect(save).toBeDisabled();

    fireEvent.change(screen.getByPlaceholderText('SECRET_NAME'), { target: { value: 'NAME_ONLY' } });
    expect(save).toBeDisabled();

    fireEvent.change(screen.getByPlaceholderText('Secret value'), { target: { value: 'v' } });
    expect(save).not.toBeDisabled();
    expect(userVault.create).not.toHaveBeenCalled();
  });

  it('surfaces a rejected create and keeps the form open', async () => {
    userVault.create.mockRejectedValue({ response: { data: { detail: 'name already taken' } } });
    renderWithProviders(<PluginSecrets />);

    fireEvent.click(screen.getByRole('button', { name: /add secret/i }));
    fireEvent.change(screen.getByPlaceholderText('SECRET_NAME'), { target: { value: 'DUP_TOKEN' } });
    fireEvent.change(screen.getByPlaceholderText('Secret value'), { target: { value: 'v' } });
    fireEvent.click(screen.getByRole('button', { name: /^save$/i }));

    await waitFor(() => expect(screen.getByText('name already taken')).toBeInTheDocument());
    expect(screen.getByPlaceholderText('Secret value')).toBeInTheDocument();
  });
});

describe('SecretsManager via the user vault adapter — update', () => {
  it('sends the adapter-shaped { name, body } payload', async () => {
    userVaultData = { secrets: [userSecret('EDIT_TOKEN', { description: 'old' })], remaining_slots: 19 };
    userVault.update.mockResolvedValue({ name: 'EDIT_TOKEN' });
    renderWithProviders(<PluginSecrets />);

    fireEvent.click(screen.getByTitle('Edit'));
    fireEvent.change(screen.getByPlaceholderText('New value (leave empty to keep current)'), {
      target: { value: 'rotated-value' },
    });
    fireEvent.click(screen.getByRole('button', { name: /^update$/i }));

    await waitFor(() =>
      expect(userVault.update).toHaveBeenCalledWith({
        name: 'EDIT_TOKEN',
        body: { value: 'rotated-value', description: 'old' },
      }),
    );
  });

  it('omits the value entirely when only the description changed', async () => {
    userVaultData = { secrets: [userSecret('EDIT_TOKEN', { description: 'old' })], remaining_slots: 19 };
    userVault.update.mockResolvedValue({ name: 'EDIT_TOKEN' });
    renderWithProviders(<PluginSecrets />);

    fireEvent.click(screen.getByTitle('Edit'));
    fireEvent.change(screen.getByPlaceholderText('Description (optional)'), { target: { value: 'new note' } });
    fireEvent.click(screen.getByRole('button', { name: /^update$/i }));

    await waitFor(() =>
      expect(userVault.update).toHaveBeenCalledWith({ name: 'EDIT_TOKEN', body: { description: 'new note' } }),
    );
  });

  it('surfaces a rejected update', async () => {
    userVaultData = { secrets: [userSecret('EDIT_TOKEN')], remaining_slots: 19 };
    userVault.update.mockRejectedValue({ response: { data: { detail: 'value too long' } } });
    renderWithProviders(<PluginSecrets />);

    fireEvent.click(screen.getByTitle('Edit'));
    fireEvent.click(screen.getByRole('button', { name: /^update$/i }));

    await waitFor(() => expect(screen.getByText('value too long')).toBeInTheDocument());
  });
});

describe('SecretsManager via the user vault adapter — delete', () => {
  it('requires the inline confirm before deleting', async () => {
    userVaultData = { secrets: [userSecret('DOOMED_TOKEN')], remaining_slots: 19 };
    userVault.del.mockResolvedValue({ ok: true });
    renderWithProviders(<PluginSecrets />);

    fireEvent.click(screen.getByTitle('Delete'));
    expect(userVault.del).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole('button', { name: /^confirm$/i }));
    await waitFor(() => expect(userVault.del).toHaveBeenCalledWith('DOOMED_TOKEN'));
  });

  it('cancels the confirm without deleting', () => {
    userVaultData = { secrets: [userSecret('DOOMED_TOKEN')], remaining_slots: 19 };
    renderWithProviders(<PluginSecrets />);

    fireEvent.click(screen.getByTitle('Delete'));
    fireEvent.click(screen.getByRole('button', { name: /^cancel$/i }));

    expect(screen.queryByRole('button', { name: /^confirm$/i })).not.toBeInTheDocument();
    expect(userVault.del).not.toHaveBeenCalled();
  });

  it('surfaces a rejected delete instead of silently dismissing the confirm', async () => {
    userVaultData = { secrets: [userSecret('DOOMED_TOKEN')], remaining_slots: 19 };
    userVault.del.mockRejectedValue({ response: { data: { detail: 'referenced by a live server' } } });
    renderWithProviders(<PluginSecrets />);

    fireEvent.click(screen.getByTitle('Delete'));
    fireEvent.click(screen.getByRole('button', { name: /^confirm$/i }));

    await waitFor(() => expect(screen.getByText('referenced by a live server')).toBeInTheDocument());
    // Still armed — the user can retry or back out.
    expect(screen.getByRole('button', { name: /^confirm$/i })).toBeInTheDocument();
  });
});

describe('SecretsManager via the user vault adapter — reveal', () => {
  it('fetches the value on reveal and hides it again on the second click', async () => {
    userVaultData = { secrets: [userSecret('SHOW_TOKEN')], remaining_slots: 19 };
    mockRevealUserVaultSecret.mockResolvedValue('placeholder-plaintext');
    renderWithProviders(<PluginSecrets />);

    fireEvent.click(screen.getByTitle('Reveal value'));

    await waitFor(() => expect(mockRevealUserVaultSecret).toHaveBeenCalledWith('SHOW_TOKEN'));
    await waitFor(() => expect(screen.getByText('placeholder-plaintext')).toBeInTheDocument());
    expect(screen.queryByText('sk-…abcd')).not.toBeInTheDocument();

    fireEvent.click(screen.getByTitle('Hide value'));
    await waitFor(() => expect(screen.getByText('sk-…abcd')).toBeInTheDocument());
    // Hiding is local state — no second round trip.
    expect(mockRevealUserVaultSecret).toHaveBeenCalledTimes(1);
  });

  it('surfaces a rejected reveal and keeps the value masked', async () => {
    userVaultData = { secrets: [userSecret('SHOW_TOKEN')], remaining_slots: 19 };
    mockRevealUserVaultSecret.mockRejectedValue({ response: { data: { detail: 'decrypt failed' } } });
    renderWithProviders(<PluginSecrets />);

    fireEvent.click(screen.getByTitle('Reveal value'));

    await waitFor(() => expect(screen.getByText('decrypt failed')).toBeInTheDocument());
    expect(screen.getByText('sk-…abcd')).toBeInTheDocument();
  });

  it('discards a reveal that resolves after the secret was deleted', async () => {
    // The reveal cache is keyed by name: if a slow reveal resolved after the
    // delete and still cached, a recreated same-name secret would display the
    // deleted one's plaintext.
    userVaultData = { secrets: [userSecret('RACE_TOKEN')], remaining_slots: 19 };
    let resolveReveal!: (value: string) => void;
    mockRevealUserVaultSecret.mockImplementation(
      () => new Promise<string>((resolve) => { resolveReveal = resolve; }),
    );
    userVault.del.mockResolvedValue({ ok: true });
    renderWithProviders(<PluginSecrets />);

    fireEvent.click(screen.getByTitle('Reveal value'));
    await waitFor(() => expect(mockRevealUserVaultSecret).toHaveBeenCalledWith('RACE_TOKEN'));

    fireEvent.click(screen.getByTitle('Delete'));
    fireEvent.click(screen.getByRole('button', { name: /^confirm$/i }));
    await waitFor(() => expect(userVault.del).toHaveBeenCalledWith('RACE_TOKEN'));

    await act(async () => { resolveReveal('stale-plaintext'); });

    expect(screen.queryByText('stale-plaintext')).not.toBeInTheDocument();
    expect(screen.getByText('sk-…abcd')).toBeInTheDocument();
  });

  it('discards a reveal that resolves after the secret was updated', async () => {
    // The delete fence alone is not enough: an edit+save racing a slow reveal
    // would otherwise repopulate the UI with the pre-edit plaintext.
    userVaultData = { secrets: [userSecret('RACE_TOKEN')], remaining_slots: 19 };
    let resolveReveal!: (value: string) => void;
    mockRevealUserVaultSecret.mockImplementation(
      () => new Promise<string>((resolve) => { resolveReveal = resolve; }),
    );
    userVault.update.mockResolvedValue({ name: 'RACE_TOKEN' });
    renderWithProviders(<PluginSecrets />);

    fireEvent.click(screen.getByTitle('Reveal value'));
    await waitFor(() => expect(mockRevealUserVaultSecret).toHaveBeenCalledWith('RACE_TOKEN'));

    fireEvent.click(screen.getByTitle('Edit'));
    fireEvent.change(screen.getByPlaceholderText('New value (leave empty to keep current)'), {
      target: { value: 'rotated-value' },
    });
    fireEvent.click(screen.getByRole('button', { name: /^update$/i }));
    await waitFor(() => expect(userVault.update).toHaveBeenCalled());

    await act(async () => { resolveReveal('pre-edit-plaintext'); });

    expect(screen.queryByText('pre-edit-plaintext')).not.toBeInTheDocument();
    expect(screen.getByText('sk-…abcd')).toBeInTheDocument();
  });
});

// ===========================================================================
// Workspace adapter — the settings-panel Vault tab (imperative + reload)
// ===========================================================================

describe('SecretsManager via the workspace adapter — list and reveal', () => {
  it('renders the workspace list and its scope-specific copy', async () => {
    wsVault.get.mockResolvedValue([wsSecret('WS_TOKEN', 'workspace-only credential')]);
    renderWorkspaceVault();

    await waitFor(() => expect(screen.getByText('WS_TOKEN')).toBeInTheDocument());
    expect(screen.getByText('workspace-only credential')).toBeInTheDocument();
    expect(screen.getByText('tok-…9f21')).toBeInTheDocument();
    // The workspace port passes a footer the user port does not.
    expect(screen.getByText('Usage')).toBeInTheDocument();
  });

  it('reveals through the workspace endpoint, scoped to the workspace id', async () => {
    wsVault.get.mockResolvedValue([wsSecret('WS_TOKEN')]);
    wsVault.reveal.mockResolvedValue('workspace-plaintext');
    renderWorkspaceVault();

    await waitFor(() => expect(screen.getByText('WS_TOKEN')).toBeInTheDocument());
    fireEvent.click(screen.getByTitle('Reveal value'));

    await waitFor(() => expect(wsVault.reveal).toHaveBeenCalledWith('ws-1', 'WS_TOKEN'));
    await waitFor(() => expect(screen.getByText('workspace-plaintext')).toBeInTheDocument());
  });

  it('surfaces a rejected reveal', async () => {
    wsVault.get.mockResolvedValue([wsSecret('WS_TOKEN')]);
    wsVault.reveal.mockRejectedValue({ response: { data: { detail: 'sandbox key unavailable' } } });
    renderWorkspaceVault();

    await waitFor(() => expect(screen.getByText('WS_TOKEN')).toBeInTheDocument());
    fireEvent.click(screen.getByTitle('Reveal value'));

    await waitFor(() => expect(screen.getByText('sandbox key unavailable')).toBeInTheDocument());
  });
});

describe('SecretsManager via the workspace adapter — delete and errors', () => {
  it('deletes after the confirm and reloads the list', async () => {
    wsVault.get.mockResolvedValue([wsSecret('WS_DOOMED')]);
    wsVault.del.mockResolvedValue({ ok: true });
    renderWorkspaceVault();

    await waitFor(() => expect(screen.getByText('WS_DOOMED')).toBeInTheDocument());
    const callsBefore = wsVault.get.mock.calls.length;

    fireEvent.click(screen.getByTitle('Delete'));
    fireEvent.click(screen.getByRole('button', { name: /^confirm$/i }));

    await waitFor(() => expect(wsVault.del).toHaveBeenCalledWith('ws-1', 'WS_DOOMED'));
    // The workspace port has no cache to invalidate — it re-reads explicitly.
    await waitFor(() => expect(wsVault.get.mock.calls.length).toBeGreaterThan(callsBefore));
  });

  it('surfaces a rejected delete', async () => {
    wsVault.get.mockResolvedValue([wsSecret('WS_DOOMED')]);
    wsVault.del.mockRejectedValue({ response: { data: { detail: 'secret is locked' } } });
    renderWorkspaceVault();

    await waitFor(() => expect(screen.getByText('WS_DOOMED')).toBeInTheDocument());
    fireEvent.click(screen.getByTitle('Delete'));
    fireEvent.click(screen.getByRole('button', { name: /^confirm$/i }));

    await waitFor(() => expect(screen.getByText('secret is locked')).toBeInTheDocument());
  });

  it('surfaces a rejected create', async () => {
    wsVault.create.mockRejectedValue({ response: { data: { detail: 'workspace vault is full' } } });
    renderWorkspaceVault();

    await waitFor(() => expect(screen.getByRole('button', { name: /add secret/i })).toBeInTheDocument());
    fireEvent.click(screen.getByRole('button', { name: /add secret/i }));
    fireEvent.change(screen.getByPlaceholderText('SECRET_NAME'), { target: { value: 'WS_NEW' } });
    fireEvent.change(screen.getByPlaceholderText('Secret value'), { target: { value: 'v' } });
    fireEvent.click(screen.getByRole('button', { name: /^save$/i }));

    await waitFor(() => expect(screen.getByText('workspace vault is full')).toBeInTheDocument());
  });

  it('surfaces a list-load failure through the same error region', async () => {
    wsVault.get.mockRejectedValue({ response: { data: { detail: 'vault service down' } } });
    renderWorkspaceVault();

    await waitFor(() => expect(screen.getByText('vault service down')).toBeInTheDocument());
  });
});

describe('SecretsManager — the two ports drive the same state machine', () => {
  it('opens an edit form scoped to the clicked row in either scope', async () => {
    // Same interaction, both adapters: the edit form replaces exactly one row.
    userVaultData = {
      secrets: [userSecret('FIRST_TOKEN'), userSecret('SECOND_TOKEN')],
      remaining_slots: 18,
    };
    const { unmount } = renderWithProviders(<PluginSecrets />);
    fireEvent.click(within(screen.getByText('SECOND_TOKEN').closest('div.flex')!.parentElement!.parentElement!)
      .getByTitle('Edit'));
    expect(screen.getByPlaceholderText('New value (leave empty to keep current)')).toBeInTheDocument();
    expect(screen.getByText('FIRST_TOKEN')).toBeInTheDocument();
    unmount();

    wsVault.get.mockResolvedValue([wsSecret('WS_FIRST'), wsSecret('WS_SECOND')]);
    renderWorkspaceVault();
    await waitFor(() => expect(screen.getByText('WS_SECOND')).toBeInTheDocument());
    fireEvent.click(screen.getAllByTitle('Edit')[1]);
    expect(screen.getByPlaceholderText('New value (leave empty to keep current)')).toBeInTheDocument();
    expect(screen.getByText('WS_FIRST')).toBeInTheDocument();
  });
});
