/**
 * The Subagents switch on the Preferences tab sets what a new thread starts
 * with. It reads off only for an explicit false and writes its one key alone,
 * since the server merges other_preference shallowly and a whole cached bag
 * would put back siblings another tab has since changed.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import { MemoryRouter } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

// Hoisted mutable state, varied per test. Stable refs keep Settings'
// prefs-sync effect from looping.
const h = vi.hoisted(() => ({
  mutateAsync: vi.fn(async (_payload: unknown) => ({})),
  toast: vi.fn(),
  user: null as Record<string, unknown> | null,
  preferences: null as Record<string, unknown> | null,
}));

vi.mock('@/config/hostMode', () => ({
  isPlatformMode: false,
}));

vi.mock('@/contexts/AuthContext', () => ({
  useAuth: () => ({ logout: vi.fn() }),
}));

vi.mock('@/hooks/useUser', () => ({
  useUser: () => ({ user: h.user, isLoading: false }),
}));

vi.mock('@/hooks/usePreferences', () => ({
  usePreferences: () => ({ preferences: h.preferences, isLoading: false, isLoaded: true }),
}));

const mutationStub = { mutateAsync: h.mutateAsync };
vi.mock('@/hooks/useUpdatePreferences', () => ({
  useUpdatePreferences: () => mutationStub,
}));

vi.mock('@/contexts/ThemeContext', () => ({
  useTheme: () => ({ theme: 'dark', preference: 'dark', setTheme: vi.fn() }),
}));

vi.mock('@/hooks/useAllModels', () => ({
  useAllModels: () => ({
    models: {},
    modelAccessMap: {},
    systemDefaults: { fallback_models: [] },
    validModelNames: new Set<string>(),
    compactionProfiles: null,
    searchProviders: null,
    isLoading: false,
  }),
}));

const toastStub = { toast: h.toast };
vi.mock('@/components/ui/use-toast', () => ({
  useToast: () => toastStub,
}));

vi.mock('@/hooks/useDebouncedSave', () => ({
  useDebouncedSave: (saveFn: () => Promise<void>) => ({
    trigger: () => { setTimeout(() => { void saveFn(); }, 0); },
    flush: () => { setTimeout(() => { void saveFn(); }, 0); },
    status: 'idle',
  }),
}));

vi.mock('@/components/model/ModelTierConfig', () => ({
  ModelTierConfig: () => <div data-testid="model-tier-config-stub" />,
}));

vi.mock('@/pages/Dashboard/utils/api', () => ({
  updateCurrentUser: vi.fn(async () => ({})),
  clearPreferences: vi.fn(async () => ({})),
  uploadAvatar: vi.fn(async () => ({ avatar_url: '' })),
  getUserApiKeys: vi.fn(async () => ({ providers: [] })),
  initiateCodexDevice: vi.fn(async () => ({})),
  pollCodexDevice: vi.fn(async () => ({})),
  getCodexOAuthStatus: vi.fn(async () => ({ connected: false })),
  disconnectCodexOAuth: vi.fn(async () => ({})),
  initiateClaudeOAuth: vi.fn(async () => ({})),
  submitClaudeCallback: vi.fn(async () => ({})),
  getClaudeOAuthStatus: vi.fn(async () => ({ connected: false })),
  disconnectClaudeOAuth: vi.fn(async () => ({})),
}));

vi.mock('@/pages/ChatAgent/utils/api', () => ({
  getFlashWorkspace: vi.fn(async () => ({ workspace_id: 'ws-flash' })),
}));

// Settings renders the onboarding replay/reset buttons; no provider here.
vi.mock('@/pages/Onboarding', () => ({
  useOnboarding: () => ({ replayGuides: vi.fn(), resetOnboarding: vi.fn() }),
  useStartOnboarding: () => ({ available: true, start: vi.fn() }),
}));

// Import after mocks are registered.
import Settings from '../Settings';

function renderPreferencesTab(otherPreference?: Record<string, unknown>) {
  h.user = { id: 'u-1', email: 'tester@example.com', name: 'Tester', onboarding_completed: true };
  h.preferences = otherPreference === undefined ? {} : { other_preference: otherPreference };
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={['/settings?tab=preferences']}>
        <Settings />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

beforeEach(() => {
  h.mutateAsync.mockReset();
  h.mutateAsync.mockResolvedValue({});
  h.toast.mockReset();
});

describe('Settings: the subagents default for new threads', () => {
  it('reads on when the key is absent', async () => {
    renderPreferencesTab();
    expect(await screen.findByRole('switch', { name: 'Subagents' })).toHaveAttribute('aria-checked', 'true');
  });

  it('reads off for an explicit false', async () => {
    renderPreferencesTab({ subagents_default: false, voice_input_enabled: true });
    expect(await screen.findByRole('switch', { name: 'Subagents' })).toHaveAttribute('aria-checked', 'false');
  });

  it('turning it off writes the one key alone', async () => {
    renderPreferencesTab({ voice_input_enabled: true });
    fireEvent.click(await screen.findByRole('switch', { name: 'Subagents' }));

    await waitFor(() => expect(h.mutateAsync).toHaveBeenCalledTimes(1));
    expect(h.mutateAsync).toHaveBeenCalledWith({ other_preference: { subagents_default: false } });
  });

  it('turning it back on writes true', async () => {
    renderPreferencesTab({ subagents_default: false });
    fireEvent.click(await screen.findByRole('switch', { name: 'Subagents' }));

    await waitFor(() => expect(h.mutateAsync).toHaveBeenCalledWith({ other_preference: { subagents_default: true } }));
  });

  it('says so when the save fails', async () => {
    h.mutateAsync.mockRejectedValue(new Error('500'));
    renderPreferencesTab();
    fireEvent.click(await screen.findByRole('switch', { name: 'Subagents' }));

    await waitFor(() => expect(h.toast).toHaveBeenCalledWith({
      variant: 'destructive',
      title: 'Error',
      description: 'Failed to save settings',
    }));
  });
});
