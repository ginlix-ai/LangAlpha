/**
 * Claude Code in the setup wizard: listed with the subscriptions, connected on
 * its own step, and once connected on to its models, or straight back to the
 * page that sent the user into setup, named `claude-code`.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter, Route, Routes, useLocation } from 'react-router';
import { createTestQueryClient } from '@/test/utils';
import type { ClaudeCodeSetup } from '@/api/model';

const h = vi.hoisted(() => ({
  /** Whether a page sent the user into setup, so a connection goes back there. */
  returnFromSetup: vi.fn((_connected?: string | null) => false),
  setup: null as Partial<ClaudeCodeSetup> | null,
  unread: false,
  setupEnabled: [] as boolean[],
  canSignIn: false,
}));

vi.mock('@/lib/setupReturn', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/setupReturn')>()),
  returnFromSetup: h.returnFromSetup,
}));
vi.mock('@/hooks/useClaudeCode', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useClaudeCode')>()),
  useClaudeCodeSetup: ({ enabled = true }: { enabled?: boolean } = {}) => {
    h.setupEnabled.push(enabled);
    return { setup: h.setup, isLoading: false, unread: h.unread };
  },
}));
vi.mock('@/components/modelAccess/tokenLine', () => ({ useClaudeCodeTokenRows: () => [] }));
vi.mock('@/components/modelAccess/ClaudeCodeToken', () => ({ RemoveClaudeCodeTokenDialog: () => null }));
vi.mock('@/pages/Settings/panels/modelAccess/ClaudeCodeSheet', () => ({
  ClaudeCodeSheet: ({ onLeave }: { onLeave?: () => void }) => <button type="button" onClick={onLeave}>Claude Code sheet</button>,
}));
vi.mock('@/hooks/useProviderCatalog', () => ({ useProviderCatalog: () => ({ keyGroups: [], planGroups: [], localEntries: [], isLoading: false }) }));
vi.mock('@/hooks/usePreferences', () => ({ usePreferences: () => ({ preferences: null }) }));
vi.mock('@/hooks/useConfiguredProviders', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useConfiguredProviders')>()),
  useConfiguredProviders: () => ({ providers: [], configuredSet: new Set<string>() }),
  useOAuthStatus: () => ({ data: { connected: false, can_sign_in: h.canSignIn } }),
}));

import ClaudeCodeStep from '../ClaudeCodeStep';
import ProviderStep from '../ProviderStep';

afterEach(() => {
  vi.clearAllMocks();
  h.setup = null;
  h.unread = false;
  h.setupEnabled = [];
  h.canSignIn = false;
  h.returnFromSetup.mockImplementation(() => false);
});

function ModelsStep() {
  const { state } = useLocation();
  return <p>models from {(state as { modelsFrom?: string } | null)?.modelsFrom}</p>;
}

function renderAt(pathname: string, state?: unknown) {
  return render(
    <QueryClientProvider client={createTestQueryClient()}>
      <MemoryRouter initialEntries={[{ pathname, state }]}>
        <Routes>
          <Route path="/setup/provider" element={<ProviderStep />} />
          <Route path="/setup/claude-code" element={<ClaudeCodeStep />} />
          <Route path="/setup/models" element={<ModelsStep />} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

const connected = { on: true, state: 'connected', computers: [] } as Partial<ClaudeCodeSetup>;

describe('ProviderStep subscriptions', () => {
  it('lists Claude Code without the Claude sign-in for a user it is not offered to, and opens its step', async () => {
    h.setup = { on: false, state: 'not_connected', computers: [] };
    const user = userEvent.setup();
    renderAt('/setup/provider', { list: 'oauth' });

    expect(screen.queryByText('Claude Pro or Max, called by LangAlpha')).toBeNull();
    await user.click(screen.getByRole('button', { name: /Claude Code/ }));

    expect(await screen.findByText('Claude Code sheet')).toBeInTheDocument();
  });

  it('reads Claude Code only on the subscription list', () => {
    renderAt('/setup/provider', { list: 'api_key' });
    expect(h.setupEnabled.length).toBeGreaterThan(0);
    expect(h.setupEnabled.every((enabled) => !enabled)).toBe(true);
  });
});

describe('ClaudeCodeStep', () => {
  it('holds Continue until Claude Code is connected', () => {
    h.setup = { on: true, state: 'not_connected', computers: [] };
    renderAt('/setup/claude-code');
    expect(screen.getByRole('button', { name: 'Continue' })).toBeDisabled();
  });

  it('moves on to Claude Code models once connected', async () => {
    h.setup = connected;
    const user = userEvent.setup();
    renderAt('/setup/claude-code');

    await user.click(screen.getByRole('button', { name: 'Continue' }));

    expect(await screen.findByText('models from claude-code')).toBeInTheDocument();
    expect(h.returnFromSetup).toHaveBeenCalledWith('claude-code');
  });

  it('goes back to the page that sent the user, naming claude-code', async () => {
    h.returnFromSetup.mockImplementation(() => true);
    h.setup = connected;
    const user = userEvent.setup();
    renderAt('/setup/claude-code');

    await user.click(screen.getByRole('button', { name: 'Continue' }));

    expect(h.returnFromSetup).toHaveBeenCalledWith('claude-code');
    expect(screen.queryByText(/models from/)).toBeNull();
  });

  it('leaves setup for the session when the sheet goes to the computers panel in the app', async () => {
    sessionStorage.removeItem('ginlix_setup_skipped');
    h.setup = { on: true, state: 'not_connected', computers: [] };
    renderAt('/setup/claude-code');
    await userEvent.setup().click(screen.getByRole('button', { name: 'Claude Code sheet' }));
    expect(sessionStorage.getItem('ginlix_setup_skipped')).toBe('1');
    sessionStorage.removeItem('ginlix_setup_skipped');
  });
});
