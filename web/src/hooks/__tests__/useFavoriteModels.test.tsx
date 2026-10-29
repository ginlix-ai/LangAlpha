/**
 * The hook reads the list's inputs out of preferences and the model catalog,
 * and saves the whole list as stars on any edit. The seed and the order are
 * tested once, in `lib/favoriteModels`.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { renderHookWithProviders } from '@/test/utils';
import type { Payer } from '@/lib/modelPayer';
import { useFavoriteModels } from '../useFavoriteModels';

const mocks = vi.hoisted(() => ({
  preferences: null as unknown,
  isLoaded: true,
  modelsLoading: false,
  platform: true,
  systemDefaults: null as Record<string, string> | null,
  mutate: vi.fn(),
}));

vi.mock('@/config/hostMode', () => ({
  get isPlatformMode() { return mocks.platform; },
}));

vi.mock('../usePreferences', () => ({
  usePreferences: () => ({ preferences: mocks.preferences, isLoading: false, isLoaded: mocks.isLoaded }),
}));

const PAYERS: Record<string, Payer> = {
  'model-platform': 'credits',
  'model-oauth': 'subscription:claude-oauth',
  'model-key': 'own_key',
  'model-starred': 'credits',
  'model-background': 'credits',
};

vi.mock('../useAllModels', () => ({
  useAllModels: () => ({
    models: {
      platform: { models: ['model-platform', 'model-starred'] },
      oauth: { models: ['model-oauth'] },
      key: { models: ['model-key'] },
    },
    payers: PAYERS,
    validModelNames: new Set(Object.keys(PAYERS)),
    systemDefaults: mocks.systemDefaults,
    offPlanModels: new Set(),
    platform: mocks.platform ? { model_tier: 0, byok_providers: [], oauth_providers: [] } : null,
    rawApiResponse: {},
    isLoading: mocks.modelsLoading,
  }),
}));

vi.mock('../useUpdatePreferences', () => ({
  useUpdatePreferences: () => ({ mutate: mocks.mutate }),
}));

function prefs(other: Record<string, unknown>, model: Record<string, unknown> = {}) {
  mocks.preferences = { other_preference: other, model_preference: model };
}

describe('useFavoriteModels', () => {
  beforeEach(() => {
    mocks.mutate.mockReset();
    mocks.isLoaded = true;
    mocks.modelsLoading = false;
    mocks.platform = true;
    mocks.systemDefaults = null;
    prefs({});
  });

  it('reads the defaults, stars, opt-outs and account access into the list', () => {
    prefs(
      { starred_models: ['model-starred'], hidden_quick_access_models: ['model-oauth'] },
      { preferred_model: 'model-platform' },
    );
    const { result } = renderHookWithProviders(() => useFavoriteModels());
    expect(result.current.models).toEqual(['model-platform', 'model-starred', 'model-key']);
  });

  it('lists the deployment\'s default the user left unset, but not the background model, which no conversation runs', () => {
    // A pick on a thread saves no preference, so this is the way back to it.
    mocks.systemDefaults = { default_model: 'model-platform', background_model: 'model-background' };
    const { result } = renderHookWithProviders(() => useFavoriteModels());
    expect(result.current.models).toEqual(['model-platform', 'model-oauth', 'model-key']);
  });

  it('lists the deployment\'s default for an account with no preferences row', () => {
    // The read answered 404 (a reset deletes the row): known, with nothing saved.
    mocks.preferences = null;
    mocks.systemDefaults = { default_model: 'model-platform', background_model: 'model-background' };
    const { result } = renderHookWithProviders(() => useFavoriteModels());
    expect(result.current.models).toEqual(['model-platform', 'model-oauth', 'model-key']);
  });

  it('lists no default before preferences are read', () => {
    mocks.preferences = null;
    mocks.isLoaded = false;
    mocks.systemDefaults = { default_model: 'model-platform', background_model: 'model-background' };
    const { result } = renderHookWithProviders(() => useFavoriteModels());
    expect(result.current.models).toEqual(['model-oauth', 'model-key']);
  });

  it('saves the list it shows, less the removed one, and turns the seed off', () => {
    prefs({ starred_models: ['model-starred'] }, { preferred_model: 'model-platform' });
    const { result } = renderHookWithProviders(() => useFavoriteModels());
    result.current.remove('model-starred');
    expect(mocks.mutate).toHaveBeenLastCalledWith({
      other_preference: {
        starred_models: ['model-platform', 'model-oauth', 'model-key'],
        hidden_quick_access_models: null,
        favorites_starred_only: true,
      },
    });
  });

  it('adds to the end of the saved list', () => {
    prefs({ starred_models: ['model-key'], favorites_starred_only: true });
    const { result } = renderHookWithProviders(() => useFavoriteModels());
    expect(result.current.models).toEqual(['model-key']);
    result.current.add('model-platform');
    expect(mocks.mutate).toHaveBeenLastCalledWith({
      other_preference: {
        starred_models: ['model-key', 'model-platform'],
        hidden_quick_access_models: null,
        favorites_starred_only: true,
      },
    });
  });

  it('keeps an unreachable favorite saved while not listing it', () => {
    prefs({ starred_models: ['model-gone', 'model-key'], favorites_starred_only: true });
    const { result } = renderHookWithProviders(() => useFavoriteModels());
    expect(result.current.models).toEqual(['model-key']);
    result.current.add('model-oauth');
    expect(mocks.mutate.mock.lastCall?.[0].other_preference.starred_models)
      .toEqual(['model-gone', 'model-key', 'model-oauth']);
  });

  it('makes no edit before preferences are read, since each one replaces the saved lists', () => {
    mocks.preferences = null;
    mocks.isLoaded = false;
    const { result } = renderHookWithProviders(() => useFavoriteModels());
    result.current.remove('model-oauth');
    result.current.add('model-key');
    expect(mocks.mutate).not.toHaveBeenCalled();
  });

  // The seed lists the user's own models from the payers, empty until the
  // models load: an edit then would save a seed without them, for good.
  it('makes no edit before the models load', () => {
    mocks.modelsLoading = true;
    const { result } = renderHookWithProviders(() => useFavoriteModels());
    result.current.add('model-key');
    expect(mocks.mutate).not.toHaveBeenCalled();
  });
});
