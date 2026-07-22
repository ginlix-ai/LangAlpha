import { useCallback, useEffect, useRef, useState } from 'react';
import { Search, Pin, Settings2 } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import { Select } from '@/components/ui/select';
import { getUserApiKeys } from '@/pages/Dashboard/utils/api';
import { useUser } from '@/hooks/useUser';
import { usePreferences } from '@/hooks/usePreferences';
import { useUpdatePreferences } from '@/hooks/useUpdatePreferences';
import { ModelTierConfig } from '@/components/model/ModelTierConfig';
import type { ByokProvider, CustomModelEntry } from '@/components/model/types';
import { useAllModels } from '@/hooks/useAllModels';
import type { CompactionProfileName } from '@/hooks/useAllModels';
import { useDebouncedSave } from '@/hooks/useDebouncedSave';
import { isPlatformMode } from '@/config/hostMode';
import { useTranslation } from 'react-i18next';
import { ConnectedAccounts } from './ConnectedAccounts';
import type { Preferences } from './types';

/** Model tab: default/flash model selection, starred models, advanced model
 * routing, search provider/depth, connected accounts, and the debounced
 * model-preferences save. */
export function ModelTab() {
  const navigate = useNavigate();
  const { user: authUser } = useUser();
  const { preferences: prefsData } = usePreferences();
  const updatePrefsMutation = useUpdatePreferences();
  const { models: visibleModels, modelAccessMap, systemDefaults: hookSystemDefaults, validModelNames, compactionProfiles, searchProviders, isLoading: isModelsLoading } = useAllModels();
  const { t } = useTranslation();

  // Model tab state
  const [preferredModel, setPreferredModel] = useState('');
  const [preferredFlashModel, setPreferredFlashModel] = useState('');
  const [starredModels, setStarredModels] = useState<string[]>([]);
  const [byokProviders, setByokProviders] = useState<ByokProvider[]>([]);
  const [modelTabError, setModelTabError] = useState<string | null>(null);
  const [showModelPicker, setShowModelPicker] = useState(false);
  const [modelPickerSearch, setModelPickerSearch] = useState('');
  const modelPickerRef = useRef<HTMLDivElement>(null);

  // Other models state
  const [compactionModel, setCompactionModel] = useState('');
  const [fetchModel, setFetchModel] = useState('');
  const [fallbackModels, setFallbackModels] = useState<string[]>([]);
  const [compactionProfile, setCompactionProfile] = useState<CompactionProfileName | ''>('');
  const [searchProvider, setSearchProvider] = useState('');
  const [searchDepth, setSearchDepth] = useState('');

  // Custom Models state
  const [customModels, setCustomModels] = useState<CustomModelEntry[]>([]);

  // Close starred-model picker on click outside
  useEffect(() => {
    if (!showModelPicker) return;
    const handler = (e: MouseEvent) => {
      if (modelPickerRef.current && !modelPickerRef.current.contains(e.target as Node)) {
        setShowModelPicker(false);
      }
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [showModelPicker]);

  // Load model data when the tab mounts and the models hook is ready
  useEffect(() => {
    if (!isModelsLoading) {
      loadModelTabData();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isModelsLoading]);

  const loadModelTabData = async () => {
    setModelTabError(null);
    try {
      const keysRes = await getUserApiKeys() as Record<string, unknown>;
      setByokProviders((keysRes?.providers as ByokProvider[]) || []);
      const otherPref = (prefsData as Preferences | null)?.other_preference as Record<string, unknown> | undefined;
      setPreferredModel((otherPref?.preferred_model as string) || '');
      setPreferredFlashModel((otherPref?.preferred_flash_model as string) || '');
      setStarredModels((otherPref?.starred_models as string[]) || []);
      setCustomModels((otherPref?.custom_models as CustomModelEntry[]) || []);
      setCompactionModel((otherPref?.compaction_model as string) || '');
      setFetchModel((otherPref?.fetch_model as string) || '');
      setFallbackModels((otherPref?.fallback_models as string[]) || (hookSystemDefaults?.fallback_models as string[]) || []);
      const rawProfile = otherPref?.compaction_profile;
      setCompactionProfile(
        rawProfile === 'aggressive' ||
          rawProfile === 'moderate' ||
          rawProfile === 'extended' ||
          rawProfile === 'relaxed'
          ? rawProfile
          : '',
      );
      const rawSearchProvider = otherPref?.search_provider;
      const loadedProvider =
        typeof rawSearchProvider === 'string' && searchProviders?.[rawSearchProvider]
          ? rawSearchProvider
          : '';
      setSearchProvider(loadedProvider);
      // Depth levels are provider-scoped: a stored level that the loaded
      // provider doesn't declare normalizes to Default.
      const rawSearchDepth = otherPref?.search_depth;
      const loadedDepths = loadedProvider ? searchProviders?.[loadedProvider]?.depths ?? [] : [];
      setSearchDepth(
        typeof rawSearchDepth === 'string' && loadedDepths.some(d => d.name === rawSearchDepth)
          ? rawSearchDepth
          : '',
      );
    } catch {
      setModelTabError(t('settings.failedToLoadModels'));
    }
  };

  // Search provider/depth options are tier-gated per the manifest (min_tier
  // comes pre-resolved from the API) in platform mode; OSS is ungated.
  // Server enforces this at resolve time — the disabled state is UX only.
  const tierAllows = (minTier: number) =>
    !isPlatformMode || (authUser?.access_tier ?? -1) >= minTier;
  const providerOptions = Object.entries(searchProviders ?? {});
  const canCustomizeSearchProvider = providerOptions.some(([, p]) => tierAllows(p.min_tier));
  // Depth select renders only for providers declaring more than one level;
  // options mirror the manifest's ordered array verbatim.
  const selectedProviderDepths = searchProvider
    ? searchProviders?.[searchProvider]?.depths ?? []
    : [];
  const depthOptions = selectedProviderDepths.length > 1 ? selectedProviderDepths : [];
  const canCustomizeSearchDepth = depthOptions.some(d => tierAllows(d.min_tier));

  // Refs to hold latest model state for the debounced save callback
  const modelStateRef = useRef({
    preferredModel, preferredFlashModel, starredModels, customModels,
    compactionModel, fetchModel, fallbackModels, byokProviders,
    compactionProfile, searchProvider, canCustomizeSearchProvider,
    searchDepth, canCustomizeSearchDepth,
  });
  modelStateRef.current = {
    preferredModel, preferredFlashModel, starredModels, customModels,
    compactionModel, fetchModel, fallbackModels, byokProviders,
    compactionProfile, searchProvider, canCustomizeSearchProvider,
    searchDepth, canCustomizeSearchDepth,
  };

  const dirtyRef = useRef(false);

  const saveModelPrefs = useCallback(async () => {
    dirtyRef.current = false;
    const s = modelStateRef.current;
    const customProvidersList = s.byokProviders
      .filter(p => p.is_custom)
      .map(p => {
        const entry: Record<string, unknown> = { name: p.provider, parent_provider: p.parent_provider };
        if (p.use_response_api) entry.use_response_api = true;
        return entry;
      });
    const cleanStarred = s.starredModels.filter(m => validModelNames.has(m));
    const cleanFallback = s.fallbackModels.filter(m => validModelNames.has(m));
    const activeProviderKeys = new Set(s.byokProviders.filter(p => p.has_key).map(p => p.provider));
    const cleanCustomProviders = customProvidersList.filter(cp => activeProviderKeys.has(cp.name as string));
    const cleanCustomModels = s.customModels.filter(cm => activeProviderKeys.has(cm.provider) || validModelNames.has(cm.name));
    const cleanModelRef = (val: string) => validModelNames.has(val) ? val : null;

    await updatePrefsMutation.mutateAsync({
      other_preference: {
        preferred_model: s.preferredModel ? cleanModelRef(s.preferredModel) : null,
        preferred_flash_model: s.preferredFlashModel ? cleanModelRef(s.preferredFlashModel) : null,
        starred_models: cleanStarred.length > 0 ? cleanStarred : null,
        custom_models: cleanCustomModels.length > 0 ? cleanCustomModels : null,
        custom_providers: cleanCustomProviders.length > 0 ? cleanCustomProviders : null,
        compaction_model: s.compactionModel ? cleanModelRef(s.compactionModel) : null,
        // Retire the legacy key so the back-compat shim in resolve_llm_config
        // can't resurrect a stale value when the user clears compaction_model.
        summarization_model: null,
        fetch_model: s.fetchModel ? cleanModelRef(s.fetchModel) : null,
        fallback_models: cleanFallback,
        compaction_profile: s.compactionProfile || null,
        // Omitted when gated: JSONB merge then leaves the stored key untouched,
        // so unrelated saves don't re-persist a value the user can't edit and
        // the pref survives for a later re-upgrade.
        ...(s.canCustomizeSearchProvider ? { search_provider: s.searchProvider || null } : {}),
        ...(s.canCustomizeSearchDepth ? { search_depth: s.searchDepth || null } : {}),
      },
    });
  }, [validModelNames, updatePrefsMutation]);

  const { trigger: triggerModelSaveRaw, flush: flushModelSave, status: modelSaveStatus } = useDebouncedSave(saveModelPrefs, 500);
  const triggerModelSave = useCallback(() => { dirtyRef.current = true; triggerModelSaveRaw(); }, [triggerModelSaveRaw]);

  // This panel unmounts on tab switch, and useDebouncedSave cancels its timer
  // on unmount — flush a pending edit so it isn't silently lost.
  useEffect(() => () => { if (dirtyRef.current) flushModelSave(); }, [flushModelSave]);

  // Auto-clean stale starred/fallback models on load
  useEffect(() => {
    if (validModelNames.size === 0) return;
    const cleanS = starredModels.filter(m => validModelNames.has(m));
    if (cleanS.length !== starredModels.length) setStarredModels(cleanS);
    const cleanF = fallbackModels.filter(m => validModelNames.has(m));
    if (cleanF.length !== fallbackModels.length) setFallbackModels(cleanF);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [validModelNames]);

  return (
      <div className="space-y-6">
        {/* Section 1: Model Preferences */}
        <div>
          {/* Default + Flash model selectors */}
          <ModelTierConfig
            models={visibleModels}
            primaryModel={preferredModel}
            onPrimaryModelChange={(v) => { setPreferredModel(v); triggerModelSave(); }}
            flashModel={preferredFlashModel}
            onFlashModelChange={(v) => { setPreferredFlashModel(v); triggerModelSave(); }}
            showAdvanced
            advancedModels={{
              compactionModel: compactionModel,
              fetchModel: fetchModel,
              fallbackModels: fallbackModels,
              compactionProfile: compactionProfile,
            }}
            onAdvancedModelsChange={(models) => {
              if (models.compactionModel !== undefined) setCompactionModel(models.compactionModel);
              if (models.fetchModel !== undefined) setFetchModel(models.fetchModel);
              if (models.fallbackModels !== undefined) setFallbackModels(models.fallbackModels);
              if (models.compactionProfile !== undefined) setCompactionProfile(models.compactionProfile);
              triggerModelSave();
            }}
            systemDefaults={hookSystemDefaults ?? undefined}
            modelAccess={modelAccessMap}
            compactionProfiles={compactionProfiles}
          />

          {/* Quick-access models — compact strip */}
          <div ref={modelPickerRef} style={{ marginTop: '16px' }}>
          <div className="flex flex-col gap-1.5">
            <label className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
              {t('settings.starredModels')}
            </label>
            <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
              {t('settings.starredModelsDesc')}
            </p>
            <div className="flex flex-wrap items-center gap-1.5">
              {starredModels.filter(m => validModelNames.has(m)).map((key) => (
                <span
                  key={key}
                  className="inline-flex items-center gap-1 px-2 py-1 rounded text-xs"
                  style={{
                    background: 'var(--color-bg-surface)',
                    border: '1px solid var(--color-border-default)',
                    color: 'var(--color-text-secondary)',
                  }}
                >
                  {key}
                  <button
                    type="button"
                    onClick={() => { setStarredModels(prev => prev.filter(k => k !== key)); triggerModelSave(); }}
                    className="ml-0.5 hover:opacity-70"
                    style={{ color: 'var(--color-text-tertiary)' }}
                    aria-label={`Remove ${key}`}
                  >
                    &times;
                  </button>
                </span>
              ))}
              <button
                type="button"
                onClick={() => { setShowModelPicker(v => !v); setModelPickerSearch(''); }}
                className="inline-flex items-center px-2 py-1 rounded text-xs font-medium"
                style={{
                  border: '1px dashed var(--color-border-default)',
                  color: 'var(--color-accent-primary)',
                }}
              >
                + {t('settings.addModels', 'Add')}
              </button>
            </div>
          </div>

          {/* Collapsible model picker — hidden by default (inside ref for click-outside) */}
          {showModelPicker && (
            <div
              className="mt-3 rounded-lg overflow-hidden"
              style={{ border: '1px solid var(--color-border-muted)', background: 'var(--color-bg-card)' }}
            >
              {/* Search */}
              <div className="px-3 pt-3 pb-2">
                <div className="relative">
                  <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5" style={{ color: 'var(--color-text-tertiary)' }} />
                  <input
                    type="text"
                    value={modelPickerSearch}
                    onChange={(e) => setModelPickerSearch(e.target.value)}
                    placeholder={t('common.search')}
                    className="w-full rounded-md pl-8 pr-3 py-1.5 text-xs"
                    style={{
                      backgroundColor: 'var(--color-bg-elevated)',
                      border: '1px solid var(--color-border-muted)',
                      color: 'var(--color-text-primary)',
                    }}
                    autoFocus
                  />
                </div>
              </div>
              {/* Provider groups */}
              <div className="px-1 pb-1 max-h-[280px] overflow-y-auto">
                {Object.entries(visibleModels).map(([provider, providerData]) => {
                  const models: string[] = providerData?.models || [];
                  const query = modelPickerSearch.toLowerCase();
                  const filtered = query
                    ? models.filter(m => m.toLowerCase().includes(query))
                    : models;
                  if (filtered.length === 0) return null;
                  const displayName = providerData?.display_name || provider.charAt(0).toUpperCase() + provider.slice(1);
                  return (
                    <div key={provider} className="mb-1">
                      <div className="px-2 py-1 text-[10px] font-semibold uppercase tracking-wider" style={{ color: 'var(--color-text-tertiary)' }}>
                        {displayName}
                      </div>
                      {filtered.map((m) => {
                        const isStarred = starredModels.includes(m);
                        return (
                          <button
                            key={m}
                            type="button"
                            onClick={() => { setStarredModels(prev =>
                              prev.includes(m) ? prev.filter(k => k !== m) : [...prev, m]
                            ); triggerModelSave(); }}
                            className="w-full flex items-center justify-between px-2 py-1.5 rounded-md text-xs transition-colors"
                            style={{
                              color: isStarred ? 'var(--color-accent-light)' : 'var(--color-text-primary)',
                              backgroundColor: isStarred ? 'var(--color-accent-soft)' : 'transparent',
                            }}
                            onMouseEnter={(e) => { if (!isStarred) e.currentTarget.style.backgroundColor = 'var(--color-bg-elevated)'; }}
                            onMouseLeave={(e) => { if (!isStarred) e.currentTarget.style.backgroundColor = 'transparent'; }}
                          >
                            <span>{m}</span>
                            {isStarred && <Pin className="h-3 w-3 flex-shrink-0" style={{ color: 'var(--color-accent-primary)' }} />}
                          </button>
                        );
                      })}
                    </div>
                  );
                })}
              </div>
            </div>
          )}
          </div>

          {/* Web search provider */}
          <div className="flex flex-col gap-1.5" style={{ marginTop: '16px' }}>
            <label className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
              {t('settings.searchProvider', 'Web Search Provider')}
            </label>
            <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
              {t('settings.searchProviderDesc', 'Search engine the agent uses for web searches.')}
            </p>
            <Select
              value={searchProvider}
              onChange={(e) => {
                setSearchProvider(e.target.value);
                // Depth levels are provider-scoped — a stale level may
                // not exist on the new provider.
                setSearchDepth('');
                triggerModelSave();
              }}
              disabled={!canCustomizeSearchProvider}
              aria-label={t('settings.searchProvider', 'Web Search Provider')}
            >
              <option value="">{t('settings.searchProviderDefault', 'Default')}</option>
              {providerOptions.map(([value, p]) => (
                <option key={value} value={value} disabled={!tierAllows(p.min_tier)}>
                  {p.display_name}
                </option>
              ))}
            </Select>
            {providerOptions.some(([, p]) => !tierAllows(p.min_tier)) && (
              <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
                {t('settings.searchProviderUpgradeHint', 'Some search providers are available on higher plans.')}
              </p>
            )}
          </div>

          {/* Web search depth — only for providers with multiple levels */}
          {depthOptions.length > 0 && (
            <div className="flex flex-col gap-1.5" style={{ marginTop: '16px' }}>
              <label className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
                {t('settings.searchDepth', 'Search Depth')}
              </label>
              <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
                {t('settings.searchDepthDesc', 'How thoroughly the agent searches the web. Deeper levels use more credits per search.')}
              </p>
              <Select
                value={searchDepth}
                onChange={(e) => { setSearchDepth(e.target.value); triggerModelSave(); }}
                disabled={!canCustomizeSearchDepth}
                aria-label={t('settings.searchDepth', 'Search Depth')}
              >
                <option value="">{t('settings.searchDepthDefault', 'Default')}</option>
                {depthOptions.map(d => (
                  <option key={d.name} value={d.name} disabled={!tierAllows(d.min_tier)}>
                    {t(`settings.searchDepthLevel.${d.name}`, d.display_name)}
                  </option>
                ))}
              </Select>
              {depthOptions.some(d => !tierAllows(d.min_tier)) && (
                <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
                  {t('settings.searchDepthUpgradeHint', 'Deeper search levels are available on higher plans.')}
                </p>
              )}
            </div>
          )}
        </div>

        <ConnectedAccounts />

        {/* Manage providers */}
        <div
          role="button"
          tabIndex={0}
          className="flex items-center justify-between gap-4 p-4 rounded-lg cursor-pointer transition-colors"
          style={{
            backgroundColor: 'var(--color-accent-soft)',
            border: '1px solid var(--color-border-default)',
          }}
          onClick={() => navigate('/setup/method')}
          onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); navigate('/setup/method'); } }}
        >
          <div className="flex flex-col gap-1 min-w-0">
            <span className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
              {t('settings.manageProviders', 'Manage providers')}
            </span>
            <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
              Add or remove API keys, custom providers, and models
            </span>
          </div>
          <Settings2 className="h-5 w-5 shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
        </div>


        {modelTabError && (
          <div className="p-3 rounded-md" style={{ backgroundColor: 'var(--color-loss-soft)', border: '1px solid var(--color-border-loss)' }}>
            <p className="text-sm" style={{ color: 'var(--color-loss)' }}>{modelTabError}</p>
          </div>
        )}

        {modelSaveStatus !== 'idle' && (
          <div className="flex items-center justify-end pt-2">
            {modelSaveStatus === 'saving' && (
              <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>{t('common.saving')}</span>
            )}
            {modelSaveStatus === 'saved' && (
              <span className="text-xs" style={{ color: 'var(--color-success)' }}>{t('common.saved')}</span>
            )}
            {modelSaveStatus === 'error' && (
              <span className="text-xs" style={{ color: 'var(--color-loss)' }}>{t('settings.failedToSaveSettings')}</span>
            )}
          </div>
        )}
      </div>
  );
}
