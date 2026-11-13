/**
 * Every composer starts on `useDefaultModel`. A saved Default the plan no longer
 * covers would be refused on send, so it starts on the deployment's instead,
 * while Settings (`useSlotDefaultModels`) still reads what is saved.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';
import { getHomeWorkspace } from '@/pages/ChatAgent/utils/api';
import { useDefaultModel, useSlotDefaultModels } from '../useSlotDefaultModels';
import { useTurnDefaultModel, useTurnModels } from '../useTurnModels';

/** Where the server places the Chief of Staff's CLI, running a saved Claude Code Default. */
const PLACED = { kind: 'picked', computer_id: 'cmp-mac', name: 'Studio Mac', refusal: null, default_runs: true };

const mocks = vi.hoisted(() => ({
  preferences: null as unknown,
  isLoaded: true,
  offPlanModels: new Set<string>(),
  modelsLoading: false,
  platformMode: false,
  platform: null as unknown,
  catalog: {} as unknown,
  setup: null as unknown,
  setupLoading: false,
}));

vi.mock('../useClaudeCode', async (importActual) => ({
  ...(await importActual<typeof import('../useClaudeCode')>()),
  useClaudeCodeSetup: ({ enabled = true }: { enabled?: boolean } = {}) => ({
    setup: enabled ? mocks.setup : null, isLoading: enabled && mocks.setupLoading, unread: false,
  }),
}));

vi.mock('@/pages/ChatAgent/utils/api', () => ({
  getHomeWorkspace: vi.fn().mockResolvedValue({ workspace_id: 'home-1', name: 'Home', is_home: true }),
}));

vi.mock('../useWorkspaceClaudeCode', () => ({
  useTurnClaudeCode: () => ({ state: 'off', computerId: null, known: true }),
}));

vi.mock('@/config/hostMode', async (importActual) => ({
  ...(await importActual<typeof import('@/config/hostMode')>()),
  get isPlatformMode() {
    return mocks.platformMode;
  },
}));

vi.mock('../usePreferences', () => ({
  usePreferences: () => ({ preferences: mocks.preferences, isLoading: false, isLoaded: mocks.isLoaded }),
}));

vi.mock('../useAllModels', () => ({
  useAllModels: () => ({
    systemDefaults: { default_model: 'model-deployment', background_model: 'model-background' },
    offPlanModels: mocks.offPlanModels,
    metadata: { 'model-code': { provider: 'claude-code', sdk: 'claude-code' } },
    platform: mocks.platform,
    rawApiResponse: mocks.catalog,
    isLoading: mocks.modelsLoading,
  }),
}));

describe('useDefaultModel', () => {
  beforeEach(() => {
    mocks.isLoaded = true;
    mocks.offPlanModels = new Set();
    mocks.modelsLoading = false;
    mocks.platformMode = false;
    mocks.platform = null;
    mocks.catalog = {};
    mocks.setup = null;
    mocks.setupLoading = false;
    mocks.preferences = { model_preference: { preferred_model: 'model-paid' } };
  });

  it('starts on the saved Default while the plan covers it', () => {
    const { result } = renderHookWithProviders(() => useDefaultModel());
    expect(result.current).toBe('model-paid');
  });

  it("starts on the deployment's Default when the plan no longer covers the saved one", () => {
    mocks.offPlanModels = new Set(['model-paid']);
    const { result } = renderHookWithProviders(() => useDefaultModel());
    expect(result.current).toBe('model-deployment');
  });

  // The server says whether a Chief of Staff turn runs a saved Claude Code
  // Default (`default_runs`); where it does not, the turn starts on the
  // deployment's. An analyst's turn runs it.
  it.each([
    ['does not run it there', { on: true, chief_of_staff: { ...PLACED, default_runs: false } }],
    ['has Claude Code off', { chief_of_staff: null }],
    // Turning it off writes `on` at once; the rest of the answer may be stale.
    ['says Claude Code is off, the rest of its answer stale', { on: false, chief_of_staff: PLACED }],
    ['predates the word', {}],
  ])("starts a Chief of Staff turn on the deployment's Default over a saved Claude Code one where the server %s", (_, setup) => {
    mocks.setup = setup;
    mocks.preferences = { model_preference: { preferred_model: 'model-code' } };
    const { result } = renderHookWithProviders(() => ({
      home: useTurnDefaultModel(true),
      analyst: useTurnDefaultModel(false),
      composer: useTurnModels(true, null).defaultModel,
    }));
    expect(result.current).toEqual({ home: 'model-deployment', analyst: 'model-code', composer: 'model-deployment' });
  });

  // Reading Home's id upserts Home and refetches the MCP catalog, so only a
  // Chief of Staff turn reads it, for its token link.
  it("reads Home's id only for a Chief of Staff turn", async () => {
    vi.mocked(getHomeWorkspace).mockClear();
    const analyst = renderHookWithProviders(() => useTurnModels(false, 'ws-1'));
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(getHomeWorkspace).not.toHaveBeenCalled();
    expect(analyst.result.current.workspaceId).toBe('ws-1');

    const home = renderHookWithProviders(() => useTurnModels(true, 'ws-1'));
    await waitFor(() => expect(home.result.current.workspaceId).toBe('home-1'));
  });

  it('keeps a saved Claude Code Default on a Chief of Staff turn where the server runs it', () => {
    mocks.setup = { on: true, chief_of_staff: PLACED };
    mocks.preferences = { model_preference: { preferred_model: 'model-code' } };
    const { result } = renderHookWithProviders(() => useTurnDefaultModel(true));
    expect(result.current).toBe('model-code');
  });

  it("keeps any other Default on a Chief of Staff turn, whatever the server's word", () => {
    mocks.setup = { chief_of_staff: { ...PLACED, default_runs: false } };
    const { result } = renderHookWithProviders(() => useTurnDefaultModel(true));
    expect(result.current).toBe('model-paid');
  });

  it('names no model on a Chief of Staff turn until the server says whether it runs a saved Claude Code Default', () => {
    mocks.setupLoading = true;
    mocks.preferences = { model_preference: { preferred_model: 'model-code' } };
    const { result } = renderHookWithProviders(() => useTurnDefaultModel(true));
    expect(result.current).toBeNull();
  });

  it('leaves the saved Default as Settings reads it', () => {
    mocks.offPlanModels = new Set(['model-paid']);
    const { result } = renderHookWithProviders(() => useSlotDefaultModels());
    expect(result.current.default).toBe('model-paid');
  });

  it('names no model until the preference is read', () => {
    mocks.isLoaded = false;
    mocks.offPlanModels = new Set(['model-paid']);
    const { result } = renderHookWithProviders(() => useDefaultModel());
    expect(result.current).toBeNull();
  });

  it('names no model until the catalog and the plan are read, when an off-plan one reads as on plan', () => {
    mocks.modelsLoading = true;
    const { result } = renderHookWithProviders(() => useDefaultModel());
    expect(result.current).toBeNull();
  });

  it("names no model on a platform whose plan did not answer, so the server's resolution runs", () => {
    mocks.platformMode = true;
    const { result, rerender } = renderHookWithProviders(() => useDefaultModel());
    expect(result.current).toBeNull();

    mocks.platform = { model_tier: 2, byok_providers: [], oauth_providers: [] };
    rerender();
    expect(result.current).toBe('model-paid');
  });

  it('names no model on a platform whose catalog did not load, where nothing reads as off plan', () => {
    mocks.platformMode = true;
    mocks.platform = { model_tier: 0, byok_providers: [], oauth_providers: [] };
    mocks.catalog = null;
    const { result } = renderHookWithProviders(() => useDefaultModel());
    expect(result.current).toBeNull();
  });
});
