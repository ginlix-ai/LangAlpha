/**
 * The hook reads the list's inputs out of preferences and the model catalog,
 * and writes a removal as an opt-out and an add as a star that lifts one. The
 * order and the opt-out rule are tested once, in `lib/quickAccessModels`.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { renderHookWithProviders } from '@/test/utils';
import type { ModelAccess } from '@/types/platform';
import { useQuickAccessModels } from '../useQuickAccessModels';

const mocks = vi.hoisted(() => ({
  preferences: null as unknown,
  isLoaded: true,
  platform: true,
  accessMap: undefined as Record<string, ModelAccess> | undefined,
  systemDefaults: null as Record<string, string> | null,
  mutate: vi.fn(),
}));

vi.mock('@/config/hostMode', () => ({
  get isPlatformMode() { return mocks.platform; },
}));

vi.mock('../usePreferences', () => ({
  usePreferences: () => ({ preferences: mocks.preferences, isLoading: false, isLoaded: mocks.isLoaded }),
}));

const ACCESS: Record<string, ModelAccess> = {
  'model-platform': 'platform',
  'model-oauth': 'oauth',
  'model-key': 'byok',
  'model-starred': 'platform',
  'model-flash': 'platform',
};

vi.mock('../useAllModels', () => ({
  useAllModels: () => ({
    models: {
      platform: { models: ['model-platform', 'model-starred'] },
      oauth: { models: ['model-oauth'] },
      key: { models: ['model-key'] },
    },
    modelAccessMap: mocks.accessMap,
    validModelNames: new Set(Object.keys(ACCESS)),
    systemDefaults: mocks.systemDefaults,
  }),
}));

vi.mock('../useUpdatePreferences', () => ({
  useUpdatePreferences: () => ({ mutate: mocks.mutate }),
}));

function prefs(other: Record<string, unknown>, model: Record<string, unknown> = {}) {
  mocks.preferences = { other_preference: other, model_preference: model };
}

describe('useQuickAccessModels', () => {
  beforeEach(() => {
    mocks.mutate.mockReset();
    mocks.isLoaded = true;
    mocks.platform = true;
    mocks.accessMap = ACCESS;
    mocks.systemDefaults = null;
    prefs({});
  });

  it('reads the defaults, stars, opt-outs and account access into the list', () => {
    prefs(
      { starred_models: ['model-starred'], hidden_quick_access_models: ['model-oauth'] },
      { preferred_model: 'model-platform' },
    );
    const { result } = renderHookWithProviders(() => useQuickAccessModels());
    expect(result.current.models).toEqual(['model-platform', 'model-starred', 'model-key']);
    expect(result.current.starred).toEqual(['model-starred']);
  });

  it('lists the deployment\'s default for each mode the user left unset', () => {
    // A pick on a thread saves no preference, so this is the way back to them.
    mocks.systemDefaults = { default_model: 'model-platform', flash_model: 'model-flash' };
    const { result } = renderHookWithProviders(() => useQuickAccessModels());
    expect(result.current.models).toEqual(['model-platform', 'model-flash', 'model-oauth', 'model-key']);
  });

  it('lists the deployment\'s defaults for an account with no preferences row', () => {
    // The read answered 404 (a reset deletes the row): known, with nothing saved.
    mocks.preferences = null;
    mocks.systemDefaults = { default_model: 'model-platform', flash_model: 'model-flash' };
    const { result } = renderHookWithProviders(() => useQuickAccessModels());
    expect(result.current.models).toEqual(['model-platform', 'model-flash', 'model-oauth', 'model-key']);
  });

  it('lists no default before preferences are read', () => {
    mocks.preferences = null;
    mocks.isLoaded = false;
    mocks.systemDefaults = { default_model: 'model-platform', flash_model: 'model-flash' };
    const { result } = renderHookWithProviders(() => useQuickAccessModels());
    expect(result.current.models).toEqual(['model-oauth', 'model-key']);
  });

  it('records a removal as an opt-out and drops its star', () => {
    prefs({ starred_models: ['model-starred'] });
    const { result } = renderHookWithProviders(() => useQuickAccessModels());
    result.current.remove('model-starred');
    expect(mocks.mutate).toHaveBeenLastCalledWith({
      other_preference: { starred_models: null, hidden_quick_access_models: ['model-starred'] },
    });
  });

  it('stars an added model and lifts its opt-out', () => {
    prefs({ hidden_quick_access_models: ['model-oauth'] });
    const { result } = renderHookWithProviders(() => useQuickAccessModels());
    result.current.add('model-oauth');
    expect(mocks.mutate).toHaveBeenLastCalledWith({
      other_preference: { starred_models: ['model-oauth'], hidden_quick_access_models: null },
    });
  });

  it('makes no edit before preferences are read, since each one replaces the saved lists', () => {
    mocks.preferences = null;
    mocks.isLoaded = false;
    const { result } = renderHookWithProviders(() => useQuickAccessModels());
    result.current.remove('model-oauth');
    result.current.add('model-key');
    expect(mocks.mutate).not.toHaveBeenCalled();
  });

  it('lists none of the user\'s own models on a platform until the access map arrives', () => {
    prefs({}, { preferred_model: 'model-platform' });
    mocks.accessMap = undefined;
    const { result } = renderHookWithProviders(() => useQuickAccessModels());
    expect(result.current.models).toEqual(['model-platform']);
  });
});
