/**
 * Every composer starts on `useDefaultModel`. A saved Default the plan no longer
 * covers would be refused on send, so it starts on the deployment's instead,
 * while Settings (`useSlotDefaultModels`) still reads what is saved.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { renderHookWithProviders } from '@/test/utils';
import { useDefaultModel, useSlotDefaultModels } from '../useSlotDefaultModels';
import { useTurnModels } from '../useTurnModels';

const mocks = vi.hoisted(() => ({
  preferences: null as unknown,
  isLoaded: true,
  offPlanModels: new Set<string>(),
  modelsLoading: false,
  platformMode: false,
  platform: null as unknown,
  catalog: {} as unknown,
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

  // The server reads a saved Claude Code Default as unset on a Chief of Staff
  // turn, and runs the deployment's; an analyst's turn runs it.
  it("starts a Chief of Staff turn on the deployment's Default over a saved Claude Code one", () => {
    mocks.preferences = { model_preference: { preferred_model: 'model-code' } };
    const { result } = renderHookWithProviders(() => ({
      home: useTurnModels(true).defaultModel,
      analyst: useTurnModels(false).defaultModel,
    }));
    expect(result.current).toEqual({ home: 'model-deployment', analyst: 'model-code' });
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
