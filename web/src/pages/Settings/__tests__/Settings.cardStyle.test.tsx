/**
 * The card style row on the account tab picks how the chat's data cards are
 * drawn. It is per device, like the theme and the font size: a choice stamps
 * the page at once and is kept in this browser, and the account is not
 * written.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, within } from '@testing-library/react';
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
    payers: {},
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
  getHomeWorkspace: vi.fn(async () => ({ workspace_id: 'ws-home' })),
}));

// Settings renders the onboarding replay/reset buttons; no provider here.
vi.mock('@/pages/Onboarding', () => ({
  useOnboarding: () => ({ replayGuides: vi.fn(), resetOnboarding: vi.fn() }),
}));

// Import after mocks are registered.
import Settings from '../Settings';

function renderAccountTab() {
  h.user = { id: 'u-1', email: 'tester@example.com', name: 'Tester', onboarding_completed: true };
  h.preferences = {};
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={['/settings?tab=appearance']}>
        <Settings />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

beforeEach(() => {
  h.mutateAsync.mockReset();
  localStorage.clear();
  document.documentElement.removeAttribute('data-card-style');
});

describe('Settings: card style', () => {
  it('starts on Quiet', async () => {
    renderAccountTab();
    const row = await screen.findByRole('group', { name: 'Card style' });
    expect(within(row).getByRole('button', { name: 'Quiet' })).toHaveAttribute('aria-pressed', 'true');
  });

  it('restyles the page at once and keeps the choice on this device only', async () => {
    renderAccountTab();
    const row = await screen.findByRole('group', { name: 'Card style' });
    fireEvent.click(within(row).getByRole('button', { name: 'Terminal' }));

    expect(document.documentElement).not.toHaveAttribute('data-card-style');
    expect(localStorage.getItem('cardStyle')).toBe('terminal');
    expect(within(row).getByRole('button', { name: 'Terminal' })).toHaveAttribute('aria-pressed', 'true');
    expect(h.mutateAsync).not.toHaveBeenCalled();
  });
});
