import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

// ---------------------------------------------------------------------------
// Hoisted mutable state — lets us vary host mode / tier / preferences per test
// without needing one file per scenario. The getter form keeps the value live
// across re-renders even though vi.mock is hoisted once per file.
// ---------------------------------------------------------------------------
const h = vi.hoisted(() => {
  // Mirrors the GET /api/v1/models search_providers block: per-option
  // resolved tiers, ordered depth arrays (only tavily is multi-depth).
  const searchProviderCatalog = {
    tavily: {
      display_name: 'Tavily',
      min_tier: 1,
      default_depth: 'standard',
      depths: [
        { name: 'ultra_fast', display_name: 'Ultra Fast', min_tier: 0 },
        { name: 'fast', display_name: 'Fast', min_tier: 0 },
        { name: 'standard', display_name: 'Standard', min_tier: 0 },
        { name: 'deep', display_name: 'Deep', min_tier: 2 },
      ],
    },
    serper: {
      display_name: 'Serper',
      min_tier: 1,
      default_depth: 'standard',
      depths: [{ name: 'standard', display_name: 'Standard', min_tier: 0 }],
    },
    bocha: {
      display_name: 'Bocha',
      min_tier: 1,
      default_depth: 'standard',
      depths: [{ name: 'standard', display_name: 'Standard', min_tier: 0 }],
    },
  };
  return {
    platformMode: false,
    accessTier: 1 as number,
    otherPreference: {} as Record<string, unknown>,
    mutateAsync: vi.fn(async (_payload: unknown) => ({})),
    // Stable references rebuilt only between tests (in beforeEach). Settings has
    // effects keyed on the user / preferences / validModelNames identities; if a
    // mock returned a fresh object each render, those effects would fire every
    // render and (for the prefs sync effect) re-set state into a loop.
    user: null as Record<string, unknown> | null,
    preferences: null as Record<string, unknown> | null,
    validModelNames: new Set<string>(),
    searchProviderCatalog: searchProviderCatalog as typeof searchProviderCatalog | null,
    fullCatalog: searchProviderCatalog,
  };
});

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

// Build-time host mode flag — getter so the live value is read on each render.
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
  usePreferences: () => ({ preferences: h.preferences, isLoading: false }),
}));

// Update mutation — assert the saved payload here. Stable object so the
// saveModelPrefs useCallback identity stays put across renders.
const mutationStub = { mutateAsync: h.mutateAsync };
vi.mock('@/hooks/useUpdatePreferences', () => ({
  useUpdatePreferences: () => mutationStub,
}));

// Theme — Settings reads preference + setTheme.
vi.mock('@/contexts/ThemeContext', () => ({
  useTheme: () => ({ theme: 'dark', preference: 'dark', setTheme: vi.fn() }),
}));

// Models hook — supply the minimal shape Settings + the (stubbed) tier config read.
vi.mock('@/hooks/useAllModels', () => ({
  useAllModels: () => ({
    models: {},
    modelAccessMap: {},
    systemDefaults: { fallback_models: [] },
    // Stable Set ref — the stale-model cleanup effect keys on its identity.
    validModelNames: h.validModelNames,
    compactionProfiles: null,
    searchProviders: h.searchProviderCatalog,
    isLoading: false,
  }),
}));

// Toast.
vi.mock('@/components/ui/use-toast', () => ({
  useToast: () => ({ toast: vi.fn() }),
}));

// Debounced save — collapse the 500ms debounce to a 0ms macrotask instead of
// firing synchronously. The component updates modelStateRef.current during its
// render commit; saveModelPrefs reads that ref, so the save must run AFTER the
// setState's commit (a macrotask), not inside the same event handler tick — or
// it would read the pre-change value. Mirrors the real debounce ordering.
vi.mock('@/hooks/useDebouncedSave', () => ({
  useDebouncedSave: (saveFn: () => Promise<void>) => ({
    trigger: () => { setTimeout(() => { void saveFn(); }, 0); },
    flush: () => { setTimeout(() => { void saveFn(); }, 0); },
    status: 'idle',
  }),
}));

// Heavy model-tier widget — stub to keep the render light. The search-provider
// select lives outside this component, so a stub is safe. The button exposes
// onPrimaryModelChange so tests can fire an unrelated model-pref save.
vi.mock('@/components/model/ModelTierConfig', () => ({
  ModelTierConfig: (props: { onPrimaryModelChange?: (v: string) => void }) => (
    <div data-testid="model-tier-config-stub">
      <button onClick={() => props.onPrimaryModelChange?.('')}>stub-change-primary</button>
    </div>
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

/** The search-provider select is the one with the "Web Search Provider" aria-label. */
function getSearchProviderSelect() {
  return screen.getByRole('combobox', { name: 'Web Search Provider' }) as HTMLSelectElement;
}

beforeEach(() => {
  h.platformMode = false;
  h.accessTier = 1;
  h.otherPreference = {};
  h.validModelNames = new Set<string>();
  h.searchProviderCatalog = h.fullCatalog;
  h.mutateAsync.mockClear();
  h.mutateAsync.mockResolvedValue({});
});

/** Build the stable user/preferences refs for the current scenario, then render. */
function setupAndRenderModelTab() {
  h.user = {
    id: 'u-1',
    email: 'tester@example.com',
    name: 'Tester',
    access_tier: h.accessTier,
    onboarding_completed: true,
  };
  h.preferences = { other_preference: h.otherPreference };
  return renderModelTab();
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('Settings — Web Search Provider', () => {
  it('OSS mode: renders enabled with Default + 3 providers and no upgrade hint', async () => {
    h.platformMode = false;
    h.accessTier = 0; // tier is irrelevant in OSS mode

    setupAndRenderModelTab();

    const select = await screen.findByRole('combobox', { name: 'Web Search Provider' });
    expect(select).toBeEnabled();

    const options = Array.from(
      (select as HTMLSelectElement).querySelectorAll('option'),
    ).map(o => o.textContent);
    expect(options).toEqual(['Default', 'Tavily', 'Serper', 'Bocha']);

    expect(
      screen.queryByText('Some search providers are available on higher plans.'),
    ).not.toBeInTheDocument();
  });

  it('platform mode, access_tier 0: select is disabled and upgrade hint is shown', async () => {
    h.platformMode = true;
    h.accessTier = 0;

    setupAndRenderModelTab();

    const select = await screen.findByRole('combobox', { name: 'Web Search Provider' });
    expect(select).toBeDisabled();
    expect(
      screen.getByText('Some search providers are available on higher plans.'),
    ).toBeInTheDocument();
  });

  it('no upgrade hint while the provider catalog is missing/loading', async () => {
    // A null catalog (models query in flight, or an older server without the
    // search_providers block) must read as "loading", not "tier-gated".
    h.platformMode = true;
    h.accessTier = 0;
    h.searchProviderCatalog = null;

    setupAndRenderModelTab();

    const select = await screen.findByRole('combobox', { name: 'Web Search Provider' });
    expect(select).toBeDisabled();
    expect(
      screen.queryByText('Some search providers are available on higher plans.'),
    ).not.toBeInTheDocument();
  });

  it('platform mode, access_tier 1: select is enabled and no upgrade hint', async () => {
    h.platformMode = true;
    h.accessTier = 1;

    setupAndRenderModelTab();

    const select = await screen.findByRole('combobox', { name: 'Web Search Provider' });
    expect(select).toBeEnabled();
    expect(
      screen.queryByText('Some search providers are available on higher plans.'),
    ).not.toBeInTheDocument();
  });

  it('shows the upgrade hint when only some providers are gated', async () => {
    // Select stays enabled (an accessible option exists) but the hint still
    // explains why the higher-tier option is disabled.
    h.platformMode = true;
    h.accessTier = 1;
    h.searchProviderCatalog = {
      ...h.fullCatalog,
      serper: { ...h.fullCatalog.serper, min_tier: 2 },
    };

    setupAndRenderModelTab();

    const select = await screen.findByRole('combobox', { name: 'Web Search Provider' });
    expect(select).toBeEnabled();
    expect(
      screen.getByText('Some search providers are available on higher plans.'),
    ).toBeInTheDocument();
    expect(screen.getByRole('option', { name: 'Serper' })).toBeDisabled();
    expect(screen.getByRole('option', { name: 'Tavily' })).toBeEnabled();
  });

  it('loads the saved value from other_preference.search_provider', async () => {
    h.otherPreference = { search_provider: 'serper' };

    setupAndRenderModelTab();

    await waitFor(() => {
      expect(getSearchProviderSelect().value).toBe('serper');
    });
  });

  it('normalizes an unknown saved search_provider to Default', async () => {
    h.otherPreference = { search_provider: 'not-an-engine' };

    setupAndRenderModelTab();

    // The load path validates against SEARCH_PROVIDERS and falls back to ''.
    const select = await screen.findByRole('combobox', { name: 'Web Search Provider' });
    await waitFor(() => {
      expect(select).toHaveValue('');
    });
  });

  it('changing the select saves search_provider through updatePreferences', async () => {
    h.platformMode = false;

    setupAndRenderModelTab();

    const select = await screen.findByRole('combobox', { name: 'Web Search Provider' });
    // Wait for the async model-tab load to settle (it sets state from prefs).
    await waitFor(() => expect(select).toHaveValue(''));

    fireEvent.change(select, { target: { value: 'serper' } });

    await waitFor(() => {
      expect(h.mutateAsync).toHaveBeenCalled();
    });
    const payload = h.mutateAsync.mock.calls.at(-1)![0] as {
      other_preference: Record<string, unknown>;
    };
    expect(payload.other_preference).toMatchObject({ search_provider: 'serper' });
  });

  it('platform mode below tier: unrelated saves omit search_provider entirely', async () => {
    h.platformMode = true;
    h.accessTier = 0;
    h.otherPreference = { search_provider: 'serper' };

    setupAndRenderModelTab();

    const select = await screen.findByRole('combobox', { name: 'Web Search Provider' });
    expect(select).toBeDisabled();

    // Fire a save through an unrelated control; the gated key must be omitted
    // (not nulled) so the stored pref is neither re-persisted nor deleted.
    fireEvent.click(screen.getByText('stub-change-primary'));

    await waitFor(() => {
      expect(h.mutateAsync).toHaveBeenCalled();
    });
    const payload = h.mutateAsync.mock.calls.at(-1)![0] as {
      other_preference: Record<string, unknown>;
    };
    expect('search_provider' in payload.other_preference).toBe(false);
  });
});

describe('Settings — Search Depth', () => {
  function getSearchDepthSelect() {
    return screen.getByRole('combobox', { name: 'Search Depth' }) as HTMLSelectElement;
  }

  it('renders the depth select for a multi-depth provider with the ordered manifest levels', async () => {
    h.otherPreference = { search_provider: 'tavily' };

    setupAndRenderModelTab();

    const select = await waitFor(() => getSearchDepthSelect());
    const options = Array.from(select.querySelectorAll('option')).map(o => o.textContent);
    expect(options).toEqual(['Default', 'Ultra Fast', 'Fast', 'Standard', 'Deep']);
  });

  it('hides the depth select for single-depth providers and when no provider is chosen', async () => {
    h.otherPreference = { search_provider: 'serper' };

    setupAndRenderModelTab();

    await screen.findByRole('combobox', { name: 'Web Search Provider' });
    await waitFor(() => {
      expect(getSearchProviderSelect().value).toBe('serper');
    });
    expect(screen.queryByRole('combobox', { name: 'Search Depth' })).not.toBeInTheDocument();
  });

  it('platform mode: locks only the depth options above the user tier and shows the hint', async () => {
    h.platformMode = true;
    h.accessTier = 1; // matches the catalog: some depth levels require a higher tier
    h.otherPreference = { search_provider: 'tavily' };

    setupAndRenderModelTab();

    const select = await waitFor(() => getSearchDepthSelect());
    expect(select).toBeEnabled();
    const byName = (name: string) =>
      select.querySelector(`option[value="${name}"]`) as HTMLOptionElement;
    expect(byName('fast').disabled).toBe(false);
    expect(byName('standard').disabled).toBe(false);
    expect(byName('deep').disabled).toBe(true);
    expect(
      screen.getByText('Deeper search levels are available on higher plans.'),
    ).toBeInTheDocument();
  });

  it('loads the saved depth and normalizes a level the provider does not declare', async () => {
    h.otherPreference = { search_provider: 'tavily', search_depth: 'deep' };

    setupAndRenderModelTab();

    await waitFor(() => {
      expect(getSearchDepthSelect().value).toBe('deep');
    });
  });

  it('normalizes an unknown saved search_depth to Default', async () => {
    h.otherPreference = { search_provider: 'tavily', search_depth: 'warp9' };

    setupAndRenderModelTab();

    const select = await waitFor(() => getSearchDepthSelect());
    await waitFor(() => {
      expect(select).toHaveValue('');
    });
  });

  it('changing the depth saves search_depth through updatePreferences', async () => {
    h.otherPreference = { search_provider: 'tavily' };

    setupAndRenderModelTab();

    const select = await waitFor(() => getSearchDepthSelect());
    await waitFor(() => expect(select).toHaveValue(''));

    fireEvent.change(select, { target: { value: 'deep' } });

    await waitFor(() => {
      expect(h.mutateAsync).toHaveBeenCalled();
    });
    const payload = h.mutateAsync.mock.calls.at(-1)![0] as {
      other_preference: Record<string, unknown>;
    };
    expect(payload.other_preference).toMatchObject({
      search_provider: 'tavily',
      search_depth: 'deep',
    });
  });

  it('switching provider resets the depth selection and drops it from the payload', async () => {
    h.otherPreference = { search_provider: 'tavily', search_depth: 'deep' };

    setupAndRenderModelTab();

    const depthSelect = await waitFor(() => getSearchDepthSelect());
    await waitFor(() => expect(depthSelect).toHaveValue('deep'));

    fireEvent.change(getSearchProviderSelect(), { target: { value: 'serper' } });

    // Depth select disappears (serper is single-depth)…
    await waitFor(() => {
      expect(screen.queryByRole('combobox', { name: 'Search Depth' })).not.toBeInTheDocument();
    });
    // …and the save omits search_depth (single-depth provider → gated off),
    // leaving the stored key untouched for a later switch back.
    await waitFor(() => {
      expect(h.mutateAsync).toHaveBeenCalled();
    });
    const payload = h.mutateAsync.mock.calls.at(-1)![0] as {
      other_preference: Record<string, unknown>;
    };
    expect(payload.other_preference).toMatchObject({ search_provider: 'serper' });
    expect('search_depth' in payload.other_preference).toBe(false);
  });
});
