import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import { MemoryRouter } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

const h = vi.hoisted(() => ({
  platformMode: false,
  mutate: vi.fn((_payload: unknown) => undefined),
  mutateAsync: vi.fn(async (_payload: unknown) => ({})),
  toast: vi.fn(),
  user: {
    id: 'u-1',
    email: 'tester@example.com',
    name: 'Tester',
    access_tier: 1,
    onboarding_completed: true,
  } as Record<string, unknown> | null,
  // Nothing chosen yet: both defaults resolve to the deployment's.
  preferences: { model_preference: {}, other_preference: {} } as Record<string, unknown> | null,
  catalog: {
    openai: { models: ['gpt-sol', 'gpt-terra', 'gpt-luna'] },
    anthropic: { models: ['claude-big', 'claude-small'] },
    deepseek: { models: ['deepseek-flash'] },
  } as Record<string, { models?: string[] }>,
  systemDefaults: { default_model: 'deepseek-flash', flash_model: 'deepseek-flash', fallback_models: [] },
  validModelNames: new Set(['gpt-sol', 'gpt-terra', 'gpt-luna', 'claude-big', 'claude-small', 'deepseek-flash']),
  rawModels: {} as Record<string, { models?: string[] }>,
  rawApiResponse: null as Record<string, unknown> | null,
  searchProviderCatalog: null,
}));

vi.mock('@/config/hostMode', () => ({
  get isPlatformMode() {
    return h.platformMode;
  },
}));

// Auth — Settings only needs logout() from useAuth.
vi.mock('@/contexts/AuthContext', () => ({
  useAuth: () => ({ logout: vi.fn() }),
}));

// Current user — access_tier drives the paid-tier gating. Return the stable
// reference so the authUser effect doesn't fire on every render.
vi.mock('@/hooks/useUser', () => ({
  useUser: () => ({ user: h.user, isLoading: false }),
}));

// Preferences — search_provider is loaded from other_preference. Stable ref so
// the prefs-sync effect (setPreferences(prefsData)) doesn't loop.
vi.mock('@/hooks/usePreferences', () => ({
  usePreferences: () => ({ preferences: h.preferences, isLoading: false, isLoaded: true }),
}));

// Update mutation. Assert the saved payload here: every control on the tab
// writes a patch of the keys it owns through this one hook, so the payload is
// the whole assertion surface. Stable object so the write callback's identity
// stays put across renders.
const mutationStub = { mutate: h.mutate, mutateAsync: h.mutateAsync };
vi.mock('@/hooks/useUpdatePreferences', () => ({
  PREFERENCE_MUTATION_KEY: ['user-preferences'],
  useUpdatePreferences: () => mutationStub,
}));

// Theme — Settings reads preference + setTheme.
vi.mock('@/contexts/ThemeContext', () => ({
  useTheme: () => ({ theme: 'dark', preference: 'dark', setTheme: vi.fn() }),
}));

// Models hook — supply the minimal shape Settings + the (stubbed) tier config read.
vi.mock('@/hooks/useAllModels', () => ({
  useAllModels: () => ({
    models: h.catalog,
    // Per-model tuning metadata — the Advanced section reads it for the
    // reasoning-effort ladder. Always an object from the real hook.
    metadata: {},
    modelAccessMap: {},
    systemDefaults: h.systemDefaults,
    // Stable Set ref — the stale-model cleanup effect keys on its identity.
    validModelNames: h.validModelNames,
    rawModels: h.rawModels,
    rawApiResponse: h.rawApiResponse,
    compactionProfiles: null,
    searchProviders: h.searchProviderCatalog,
    isLoading: false,
  }),
}));

// Toast.
vi.mock('@/components/ui/use-toast', () => ({
  useToast: () => ({ toast: h.toast }),
  toast: h.toast,
}));

// Debounced save. The model tab has no text input and writes on the change
// event, but UserInfoTab (mounted by the same Settings shell) still debounces
// its text fields.
vi.mock('@/hooks/useDebouncedSave', () => ({
  useDebouncedSave: (saveFn: () => Promise<void>) => ({
    trigger: () => { setTimeout(() => { void saveFn(); }, 0); },
    flush: () => { setTimeout(() => { void saveFn(); }, 0); },
    status: 'idle',
  }),
}));

// The real ModelTierConfig runs; only its dropdowns are native selects, so a
// test can pick a model by value and read the empty slot's placeholder.
vi.mock('@/components/model/ModelSelector', () => ({
  ModelSelector: (props: {
    label: string;
    value: string;
    onChange: (v: string) => void;
    models: Record<string, { models?: string[] }>;
    placeholder?: string;
    autoOption?: { value: string; label: string };
  }) => (
    <select aria-label={props.label} value={props.value} onChange={(e) => props.onChange(e.target.value)}>
      <option value="">{props.placeholder}</option>
      {props.autoOption && <option value={props.autoOption.value}>{props.autoOption.label}</option>}
      {Object.values(props.models).flatMap((p) => p.models ?? []).map((m) => (
        <option key={m} value={m}>{m}</option>
      ))}
    </select>
  ),
}));

// The fallback list moved out of ModelTierConfig into the Advanced section's
// own picker. It still maps `selected` with no catalog filter, so what renders
// is exactly the panel's state — which is what the cleanup effect must prune.
vi.mock('@/components/model/FallbackModelsPicker', () => ({
  FallbackModelsPicker: (props: { selected?: string[] }) => (
    <ul data-testid="fallback-chips">
      {(props.selected ?? []).map((m) => <li key={m}>{m}</li>)}
    </ul>
  ),
}));

// Dashboard API surface Settings imports — model tab load calls three of these.
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

// Flash workspace — used by preference-modify navigation, never in these tests.
vi.mock('@/pages/ChatAgent/utils/api', () => ({
  getFlashWorkspace: vi.fn(async () => ({ workspace_id: 'ws-flash' })),
}));

// Onboarding — Settings renders replay/reset buttons; no provider in this harness.
vi.mock('@/pages/Onboarding', () => ({
  useOnboarding: () => ({ replayGuides: vi.fn(), resetOnboarding: vi.fn() }),
}));

// Import after mocks are registered.
import Settings from '../Settings';

function renderModelTab() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={['/settings?tab=model']}>
        <Settings />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

const primary = () => screen.getByRole('combobox', { name: 'Primary Model' }) as HTMLSelectElement;
const flash = () => screen.getByRole('combobox', { name: 'Flash Model' }) as HTMLSelectElement;

/** Whether any preferences write so far named a default model. */
function wroteADefault(): boolean {
  return [...h.mutate.mock.calls, ...h.mutateAsync.mock.calls].some(([payload]) => {
    const prefs = (payload as { model_preference?: Record<string, unknown> } | undefined)?.model_preference ?? {};
    return 'preferred_model' in prefs || 'preferred_flash_model' in prefs;
  });
}

beforeEach(() => {
  localStorage.removeItem('settings:modelMode');
  h.preferences = { model_preference: {}, other_preference: {} };
  h.mutate.mockClear();
  h.mutateAsync.mockClear();
  h.mutateAsync.mockResolvedValue({});
  h.toast.mockClear();
});

/**
 * A primary pick never touches the flash slot: an empty one follows the
 * primary and says so. Both defaults change under one question, and a flash
 * pick made while it is open joins it instead of dropping the primary.
 */
describe('Settings: default model question', () => {
  it('leaves an empty flash slot following the primary, whichever provider it is from', async () => {
    renderModelTab();
    fireEvent.change(await screen.findByRole('combobox', { name: 'Primary Model' }), { target: { value: 'gpt-sol' } });
    expect(flash().value).toBe('');
    expect(flash().selectedOptions[0].textContent).toBe('Same as Primary model');
    expect(screen.getByText(/your default model\?/)).toBeInTheDocument();

    fireEvent.change(primary(), { target: { value: 'claude-big' } });
    expect(flash().value).toBe('');
    expect(wroteADefault()).toBe(false);

    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(primary().value).toBe('');
    expect(wroteADefault()).toBe(false);
  });

  it('keeps both picks under one question and writes them together', async () => {
    renderModelTab();
    fireEvent.change(await screen.findByRole('combobox', { name: 'Primary Model' }), { target: { value: 'gpt-sol' } });
    fireEvent.change(flash(), { target: { value: 'gpt-luna' } });
    fireEvent.change(primary(), { target: { value: 'claude-big' } });

    expect(primary().value).toBe('claude-big');
    expect(flash().value).toBe('gpt-luna');
    expect(screen.getByText(/your default models\?/)).toBeInTheDocument();
    expect(wroteADefault()).toBe(false);

    fireEvent.click(screen.getByRole('button', { name: 'Make default' }));
    await waitFor(() => expect(h.mutateAsync).toHaveBeenCalledTimes(1));
    expect(h.mutateAsync).toHaveBeenCalledWith({
      model_preference: { preferred_model: 'claude-big', preferred_flash_model: 'gpt-luna', flash_follows: null },
      apply_default_to: 'new_threads',
    });
    expect(h.mutate).not.toHaveBeenCalled();
  });

  it('takes a cleared default out of the open question, and closes it once nothing is left', async () => {
    renderModelTab();
    fireEvent.change(await screen.findByRole('combobox', { name: 'Primary Model' }), { target: { value: 'gpt-sol' } });
    fireEvent.change(flash(), { target: { value: 'gpt-luna' } });
    expect(screen.getByText(/your default models\?/)).toBeInTheDocument();

    fireEvent.change(flash(), { target: { value: '' } });
    expect(flash().value).toBe('');
    expect(primary().value).toBe('gpt-sol');
    expect(screen.getByText(/your default model\?/)).toBeInTheDocument();
    expect(h.mutate).toHaveBeenLastCalledWith({ model_preference: { preferred_flash_model: null, flash_follows: null } });

    fireEvent.change(primary(), { target: { value: '' } });
    expect(primary().value).toBe('');
    expect(screen.queryByText(/your default models?\?/)).toBeNull();
    expect(h.mutate).toHaveBeenLastCalledWith({ model_preference: { preferred_model: null } });
    expect(h.mutateAsync).not.toHaveBeenCalled();
  });
});

/**
 * An unset default is a choice: Auto runs the deployment's model. Flash offers
 * Auto beside "Same as Primary" only once a primary is saved, since before
 * that the two run the same model.
 */
describe('Settings: Auto defaults', () => {
  const options = (select: HTMLSelectElement) => [...select.options].map((o) => o.textContent);

  it('shows both unset defaults as Auto with the model each runs', async () => {
    h.systemDefaults = { default_model: 'claude-big', flash_model: 'deepseek-flash', fallback_models: [] };
    try {
      renderModelTab();
      await screen.findByRole('combobox', { name: 'Primary Model' });
      expect(primary().selectedOptions[0].textContent).toBe('Auto (claude-big)');
      expect(flash().selectedOptions[0].textContent).toBe('Auto (deepseek-flash)');
      expect(options(flash())).not.toContain('Same as Primary model');
    } finally {
      h.systemDefaults = { default_model: 'deepseek-flash', flash_model: 'deepseek-flash', fallback_models: [] };
    }
  });

  it('offers Auto beside a saved primary, and writes it without asking', async () => {
    h.preferences = { model_preference: { preferred_model: 'gpt-sol' }, other_preference: {} };
    renderModelTab();
    await screen.findByRole('combobox', { name: 'Flash Model' });
    expect(flash().selectedOptions[0].textContent).toBe('Same as Primary model');

    fireEvent.change(flash(), { target: { value: '__auto__' } });
    expect(h.mutate).toHaveBeenLastCalledWith({
      model_preference: { preferred_flash_model: null, flash_follows: 'deployment' },
    });
    expect(screen.queryByText(/your default models?\?/)).toBeNull();
  });

  it('shows a saved Auto for flash as Auto', async () => {
    h.preferences = {
      model_preference: { preferred_model: 'gpt-sol', flash_follows: 'deployment' },
      other_preference: {},
    };
    renderModelTab();
    await screen.findByRole('combobox', { name: 'Flash Model' });
    expect(flash().selectedOptions[0].textContent).toBe('Auto (deepseek-flash)');
  });
});
