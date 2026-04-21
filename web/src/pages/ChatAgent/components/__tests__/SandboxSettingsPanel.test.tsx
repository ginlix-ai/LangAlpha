import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import React from 'react';
import { SandboxSettingsContent } from '../SandboxSettingsPanel';

// ---------------------------------------------------------------------------
// Mocks — cover the full API surface SecretsTab uses
// ---------------------------------------------------------------------------

const mockGetVaultSecrets = vi.fn();
const mockCreateVaultSecret = vi.fn();
const mockUpdateVaultSecret = vi.fn();
const mockDeleteVaultSecret = vi.fn();
const mockRevealVaultSecret = vi.fn();
const mockGetVaultBlueprints = vi.fn();
const mockGetSandboxStats = vi.fn();

vi.mock('../../utils/api', () => ({
  getVaultSecrets: (...args: any[]) => mockGetVaultSecrets(...args),
  createVaultSecret: (...args: any[]) => mockCreateVaultSecret(...args),
  updateVaultSecret: (...args: any[]) => mockUpdateVaultSecret(...args),
  deleteVaultSecret: (...args: any[]) => mockDeleteVaultSecret(...args),
  revealVaultSecret: (...args: any[]) => mockRevealVaultSecret(...args),
  getVaultBlueprints: (...args: any[]) => mockGetVaultBlueprints(...args),
  getSandboxStats: (...args: any[]) => mockGetSandboxStats(...args),
  installSandboxPackages: vi.fn(),
  refreshWorkspace: vi.fn(),
}));

vi.mock('@/api/client', () => ({
  api: { get: vi.fn(), post: vi.fn() },
}));

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const X_BLUEPRINT = {
  name: 'X_BEARER_TOKEN',
  label: 'X (Twitter) Bearer Token',
  description: 'Read-only app-only auth for x_api.',
  docs_url: 'https://console.x.com/',
  regex: '^[A-Za-z0-9%_-]{20,}$',
  sources: ['x_api'],
};

function defaultStats() {
  return {
    state: 'running',
    sandbox_id: 'sandbox-abc',
    resources: {},
    packages: [],
    skills: [],
    mcp_servers: [],
  };
}

function renderVaultTab() {
  const view = render(<SandboxSettingsContent workspaceId="ws-1" />);
  // Switch to the Vault tab
  fireEvent.click(screen.getByRole('button', { name: /vault/i }));
  return view;
}

beforeEach(() => {
  vi.clearAllMocks();
  mockGetSandboxStats.mockResolvedValue(defaultStats());
  mockGetVaultSecrets.mockResolvedValue([]);
  mockGetVaultBlueprints.mockResolvedValue({ blueprints: [], remaining_slots: 20 });
});

// ---------------------------------------------------------------------------
// Recommended credentials section
// ---------------------------------------------------------------------------

describe('SecretsTab — Recommended credentials', () => {
  it('renders when blueprints API returns items', async () => {
    mockGetVaultBlueprints.mockResolvedValue({
      blueprints: [X_BLUEPRINT],
      remaining_slots: 20,
    });

    renderVaultTab();

    await waitFor(() => {
      expect(screen.getByText('Recommended credentials')).toBeInTheDocument();
    });
    expect(screen.getByText('X (Twitter) Bearer Token')).toBeInTheDocument();
    expect(screen.getByText('X_BEARER_TOKEN')).toBeInTheDocument();
  });

  it('hides section when blueprints list is empty', async () => {
    mockGetVaultBlueprints.mockResolvedValue({ blueprints: [], remaining_slots: 20 });

    renderVaultTab();

    await waitFor(() => {
      expect(mockGetVaultBlueprints).toHaveBeenCalled();
    });
    expect(screen.queryByText('Recommended credentials')).not.toBeInTheDocument();
  });

  it('hides section when blueprints fetch fails (graceful degradation)', async () => {
    mockGetVaultBlueprints.mockRejectedValue(new Error('backend down'));
    mockGetVaultSecrets.mockResolvedValue([]);

    renderVaultTab();

    // Primary secrets list still renders — the empty-state message appears.
    await waitFor(() => {
      expect(screen.getByText(/No secrets stored/)).toBeInTheDocument();
    });
    expect(screen.queryByText('Recommended credentials')).not.toBeInTheDocument();
  });

  it('opens the add form with name pre-filled on Set up click', async () => {
    mockGetVaultBlueprints.mockResolvedValue({
      blueprints: [X_BLUEPRINT],
      remaining_slots: 20,
    });

    renderVaultTab();

    await waitFor(() => expect(screen.getByText('Set up')).toBeInTheDocument());
    fireEvent.click(screen.getByText('Set up'));

    // Pre-fill check — name input value equals the blueprint name
    const nameInput = screen.getByPlaceholderText('SECRET_NAME') as HTMLInputElement;
    expect(nameInput.value).toBe('X_BEARER_TOKEN');
    // Docs link rendered
    expect(screen.getByText('Docs').closest('a')).toHaveAttribute(
      'href',
      'https://console.x.com/',
    );
  });

  it('disables Set up button when remaining_slots is 0', async () => {
    mockGetVaultBlueprints.mockResolvedValue({
      blueprints: [X_BLUEPRINT],
      remaining_slots: 0,
    });

    renderVaultTab();

    await waitFor(() => expect(screen.getByText('Set up')).toBeInTheDocument());
    // The button is the parent anchor wrapping the "Set up" label
    const setupRow = screen.getByText('Set up').closest('button') as HTMLButtonElement;
    expect(setupRow).toBeDisabled();
  });
});

// ---------------------------------------------------------------------------
// Regex hint on the add form
// ---------------------------------------------------------------------------

describe('SecretsTab — value regex hint', () => {
  it('shows hint when value does not match blueprint regex', async () => {
    mockGetVaultBlueprints.mockResolvedValue({
      blueprints: [X_BLUEPRINT],
      remaining_slots: 20,
    });

    renderVaultTab();

    await waitFor(() => expect(screen.getByText('Set up')).toBeInTheDocument());
    fireEvent.click(screen.getByText('Set up'));

    const valueInput = screen.getByPlaceholderText('Secret value');
    fireEvent.change(valueInput, { target: { value: 'Bearer abc' } });

    await waitFor(() => {
      expect(screen.getByText(/doesn't look like a valid/i)).toBeInTheDocument();
    });
  });

  it('hides hint when value matches the blueprint regex', async () => {
    mockGetVaultBlueprints.mockResolvedValue({
      blueprints: [X_BLUEPRINT],
      remaining_slots: 20,
    });

    renderVaultTab();

    await waitFor(() => expect(screen.getByText('Set up')).toBeInTheDocument());
    fireEvent.click(screen.getByText('Set up'));

    const valueInput = screen.getByPlaceholderText('Secret value');
    // 25 URL-safe chars — matches the X regex ^[A-Za-z0-9%_-]{20,}$
    fireEvent.change(valueInput, { target: { value: 'A'.repeat(25) } });

    await waitFor(() => {
      expect(screen.queryByText(/doesn't look like a valid/i)).not.toBeInTheDocument();
    });
  });

  it('does not crash when blueprint regex is malformed (safe compile)', async () => {
    const badBlueprint = {
      ...X_BLUEPRINT,
      regex: '[unterminated',
    };
    mockGetVaultBlueprints.mockResolvedValue({
      blueprints: [badBlueprint],
      remaining_slots: 20,
    });

    renderVaultTab();

    await waitFor(() => expect(screen.getByText('Set up')).toBeInTheDocument());
    fireEvent.click(screen.getByText('Set up'));

    const valueInput = screen.getByPlaceholderText('Secret value');
    fireEvent.change(valueInput, { target: { value: 'anything' } });

    // No hint rendered; no crash; form still usable
    expect(screen.queryByText(/doesn't look like a valid/i)).not.toBeInTheDocument();
    expect(valueInput).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// Refresh after create — blueprint disappears
// ---------------------------------------------------------------------------

describe('SecretsTab — load generation guard', () => {
  it('discards stale load results when workspaceId changes mid-flight', async () => {
    // Slow first blueprint fetch (ws-1), fast second (ws-2). If the stale
    // resolution leaks through, ws-1's blueprint would appear after ws-2 rendered.
    let resolveFirstBlueprint: ((v: any) => void) | null = null;
    mockGetVaultSecrets.mockResolvedValue([]);
    mockGetVaultBlueprints
      .mockImplementationOnce(
        () => new Promise(r => { resolveFirstBlueprint = r; }),
      )
      .mockImplementationOnce(
        () => Promise.resolve({ blueprints: [], remaining_slots: 20 }),
      );

    const { rerender } = render(<SandboxSettingsContent workspaceId="ws-1" />);
    fireEvent.click(screen.getByRole('button', { name: /vault/i }));

    // Wait for the ws-1 load() to actually kick off before we swap props,
    // so React's effect scheduling can't collapse both loads into a single call.
    await waitFor(() => expect(mockGetVaultBlueprints).toHaveBeenCalledTimes(1));

    // Switch to ws-2 while ws-1 blueprint fetch is still pending
    rerender(<SandboxSettingsContent workspaceId="ws-2" />);

    // Wait for the ws-2 load to fire
    await waitFor(() => expect(mockGetVaultBlueprints).toHaveBeenCalledTimes(2));

    // Resolve the stale ws-1 fetch AFTER ws-2 has already started
    resolveFirstBlueprint!({
      blueprints: [X_BLUEPRINT],
      remaining_slots: 20,
    });

    // ws-1's stale blueprint must NOT leak into the UI now showing ws-2
    await waitFor(() => {
      expect(screen.queryByText('X (Twitter) Bearer Token')).not.toBeInTheDocument();
    });
  });
});

describe('SecretsTab — workspace change resets form', () => {
  it('closes the open add form when workspaceId changes mid-flow', async () => {
    mockGetVaultBlueprints
      .mockResolvedValueOnce({ blueprints: [X_BLUEPRINT], remaining_slots: 20 })
      .mockResolvedValueOnce({ blueprints: [], remaining_slots: 20 });

    const { rerender } = render(<SandboxSettingsContent workspaceId="ws-1" />);
    fireEvent.click(screen.getByRole('button', { name: /vault/i }));

    // Open the add form on ws-1 and type a value
    await waitFor(() => expect(screen.getByText('Set up')).toBeInTheDocument());
    fireEvent.click(screen.getByText('Set up'));
    const valueInput = screen.getByPlaceholderText('Secret value');
    fireEvent.change(valueInput, { target: { value: 'partially-typed-secret' } });
    expect((valueInput as HTMLInputElement).value).toBe('partially-typed-secret');

    // Swap workspaces mid-flow
    rerender(<SandboxSettingsContent workspaceId="ws-2" />);

    // Form must be gone: no lingering value input, no ws-1 pre-fill visible
    await waitFor(() => {
      expect(screen.queryByPlaceholderText('Secret value')).not.toBeInTheDocument();
    });
    expect(screen.queryByDisplayValue('partially-typed-secret')).not.toBeInTheDocument();
  });
});

describe('SecretsTab — blueprint lifecycle', () => {
  it('refetches blueprints after successful create', async () => {
    mockGetVaultBlueprints
      .mockResolvedValueOnce({ blueprints: [X_BLUEPRINT], remaining_slots: 20 })
      .mockResolvedValueOnce({ blueprints: [], remaining_slots: 19 });
    mockCreateVaultSecret.mockResolvedValue({ name: 'X_BEARER_TOKEN' });

    renderVaultTab();

    await waitFor(() => expect(screen.getByText('Set up')).toBeInTheDocument());
    fireEvent.click(screen.getByText('Set up'));

    // Fill value and save
    fireEvent.change(screen.getByPlaceholderText('Secret value'), {
      target: { value: 'A'.repeat(25) },
    });
    fireEvent.click(screen.getByRole('button', { name: /^save$/i }));

    await waitFor(() => {
      expect(mockCreateVaultSecret).toHaveBeenCalledWith('ws-1', {
        name: 'X_BEARER_TOKEN',
        value: 'A'.repeat(25),
        description: 'Read-only app-only auth for x_api.',
      });
    });
    // Blueprints refetched; second call yields empty list → section gone.
    await waitFor(() => {
      expect(mockGetVaultBlueprints).toHaveBeenCalledTimes(2);
      expect(screen.queryByText('Recommended credentials')).not.toBeInTheDocument();
    });
  });
});
