import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useMutationState } from '@tanstack/react-query';
import { Search, Pin, Settings2 } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import { Select } from '@/components/ui/select';
import { useUser } from '@/hooks/useUser';
import { usePreferences } from '@/hooks/usePreferences';
import { PREFERENCE_MUTATION_KEY, useUpdatePreferences } from '@/hooks/useUpdatePreferences';
import { ModelTierConfig } from '@/components/model/ModelTierConfig';
import { ModelSelector } from '@/components/model/ModelSelector';
import { FallbackModelsPicker } from '@/components/model/FallbackModelsPicker';
import { PerModelMatrix } from '@/components/model/PerModelMatrix';
import { AccountTuningDefaults } from '@/components/model/AccountTuningDefaults';
import { useAllModels } from '@/hooks/useAllModels';
import { isPlatformMode } from '@/config/hostMode';
import { useTranslation } from 'react-i18next';
import { ConnectedAccounts } from './ConnectedAccounts';
import { modelPrefs, splitPreferenceWrite } from '@/lib/modelPreferences';
import type { PreferencePatch, PreferencesLike } from '@/lib/modelPreferences';

type ModelTabMode = 'simple' | 'advanced';

const MODE_STORAGE_KEY = 'settings:modelMode';

/** View state, not configuration — which is why it stays on the device rather
 *  than travelling with the account like everything else on this tab. */
function readStoredMode(): ModelTabMode {
  try {
    return localStorage.getItem(MODE_STORAGE_KEY) === 'advanced' ? 'advanced' : 'simple';
  } catch {
    return 'simple';
  }
}

function SectionHeading({ children }: { children: React.ReactNode }) {
  return (
    <h3
      className="text-xs font-semibold uppercase tracking-wider"
      style={{ color: 'var(--color-text-tertiary)' }}
    >
      {children}
    </h3>
  );
}

/** Model tab: default/flash model selection, starred models, per-model tuning,
 * account defaults, advanced model routing, search provider/depth, and
 * connected accounts.
 *
 * Every control writes a patch of the keys it owns and reads its value back
 * from the preferences cache. Nothing here is mirrored into local state: this
 * tab has no text input, so there is no keystroke to debounce, and a mirror
 * would only be a second copy that a failed write leaves disagreeing with the
 * settings a scheduled turn actually runs. */
export function ModelTab() {
  const navigate = useNavigate();
  const { user: authUser } = useUser();
  const { preferences: prefsData } = usePreferences();
  const { mutate } = useUpdatePreferences();
  const { models: visibleModels, metadata: modelMetadata, modelAccessMap, systemDefaults: hookSystemDefaults, validModelNames, rawModels, isLoading: modelsLoading, compactionProfiles, searchProviders } = useAllModels();
  const { t } = useTranslation();

  /** Write the keys a control owns, each routed to its own column. */
  const write = useCallback(
    (patch: PreferencePatch) => mutate(splitPreferenceWrite(patch)),
    [mutate],
  );

  const [mode, setMode] = useState<ModelTabMode>(readStoredMode);
  const isAdvanced = mode === 'advanced';
  const changeMode = (next: ModelTabMode) => {
    setMode(next);
    try { localStorage.setItem(MODE_STORAGE_KEY, next); } catch { /* private mode */ }
  };

  const [showModelPicker, setShowModelPicker] = useState(false);
  const [modelPickerSearch, setModelPickerSearch] = useState('');
  const modelPickerRef = useRef<HTMLDivElement>(null);

  // Model routing/tuning lives in model_preference; starred_models and the
  // search prefs below stayed in other_preference.
  const mPref = modelPrefs(prefsData);
  const otherPref = (prefsData as PreferencesLike | null)?.other_preference ?? {};
  const starredModels = Array.isArray(otherPref.starred_models) ? otherPref.starred_models as string[] : [];
  // Every name the catalog still carries, access aside. A fallback the user
  // temporarily cannot reach is still configured; one the manifest dropped is
  // not, and _resolve_fallback_clients skips it without saying so, leaving the
  // picker promising resilience that no longer exists.
  const catalogNames = useMemo(() => {
    const names = new Set<string>();
    for (const data of Object.values(rawModels)) {
      for (const m of data.models ?? []) names.add(m);
    }
    return names;
  }, [rawModels]);
  const storedFallbacks = mPref.fallback_models ?? hookSystemDefaults?.fallback_models ?? [];
  // An empty catalog is "not loaded yet", never "every model was removed".
  const fallbackModels = modelsLoading || catalogNames.size === 0
    ? storedFallbacks
    : storedFallbacks.filter((m) => catalogNames.has(m));
  const setStarred = (next: string[]) => write({ starred_models: next.length > 0 ? next : null });

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

  // Search provider/depth options are tier-gated per the manifest (min_tier
  // comes pre-resolved from the API) in platform mode; OSS is ungated.
  // Server enforces this at resolve time — the disabled state is UX only.
  const tierAllows = (minTier: number) =>
    !isPlatformMode || (authUser?.access_tier ?? -1) >= minTier;
  const providerOptions = Object.entries(searchProviders ?? {});
  const canCustomizeSearchProvider = providerOptions.some(([, p]) => tierAllows(p.min_tier));
  // A stored provider the catalog no longer declares reads as Default.
  const rawSearchProvider = otherPref.search_provider;
  const searchProvider =
    typeof rawSearchProvider === 'string' && searchProviders?.[rawSearchProvider]
      ? rawSearchProvider
      : '';
  // Depth select renders only for providers declaring more than one level;
  // options mirror the manifest's ordered array verbatim. Depth levels are
  // provider-scoped, so a stored level the chosen provider does not declare
  // reads as Default too.
  const selectedProviderDepths = searchProvider
    ? searchProviders?.[searchProvider]?.depths ?? []
    : [];
  const depthOptions = selectedProviderDepths.length > 1 ? selectedProviderDepths : [];
  const canCustomizeSearchDepth = depthOptions.some(d => tierAllows(d.min_tier));
  const rawSearchDepth = otherPref.search_depth;
  const searchDepth =
    typeof rawSearchDepth === 'string' && depthOptions.some(d => d.name === rawSearchDepth)
      ? rawSearchDepth
      : '';

  // Every control on this tab writes through the same mutation, the ones the
  // child components own included, so the footer watches the write rather than
  // any single caller's copy of it.
  const saveStatuses = useMutationState({
    filters: { mutationKey: PREFERENCE_MUTATION_KEY },
    select: (m) => m.state.status,
  });
  const saveStatus = saveStatuses.includes('pending') ? 'pending' : saveStatuses.at(-1) ?? 'idle';

  return (
      <div className="space-y-6">
        {/* Simple hides everything a first-time user has no reason to touch;
            Advanced adds per-model tuning, account defaults and routing. */}
        <div className="flex items-center justify-between gap-4">
          <SectionHeading>{t('settings.modelTuning.sectionModels')}</SectionHeading>
          <div
            role="radiogroup"
            aria-label={t('settings.modelTuning.detailLevel')}
            className="inline-flex rounded-md p-0.5"
            style={{ background: 'var(--color-bg-surface)', border: '1px solid var(--color-border-muted)' }}
          >
            {(['simple', 'advanced'] as const).map((value) => (
              <button
                key={value}
                type="button"
                role="radio"
                aria-checked={mode === value}
                onClick={() => changeMode(value)}
                className="rounded px-2.5 py-1 text-xs font-medium transition-colors"
                style={{
                  background: mode === value ? 'var(--color-bg-elevated)' : 'transparent',
                  color: mode === value ? 'var(--color-text-primary)' : 'var(--color-text-tertiary)',
                }}
              >
                {t(value === 'simple' ? 'settings.modelTuning.simple' : 'settings.modelTuning.advanced')}
              </button>
            ))}
          </div>
        </div>

        <div>
          {/* Default + Flash model selectors. Routing lives in its own group
              below in Advanced, so the component's own drawer stays closed. */}
          <ModelTierConfig
            models={visibleModels}
            primaryModel={mPref.preferred_model ?? ''}
            onPrimaryModelChange={(v) => write({ preferred_model: v || null })}
            flashModel={mPref.preferred_flash_model ?? ''}
            onFlashModelChange={(v) => write({ preferred_flash_model: v || null })}
            modelAccess={modelAccessMap}
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
                    onClick={() => setStarred(starredModels.filter(k => k !== key))}
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
                      <div className="px-2 py-1 text-[0.625rem] font-semibold uppercase tracking-wider" style={{ color: 'var(--color-text-tertiary)' }}>
                        {displayName}
                      </div>
                      {filtered.map((m) => {
                        const isStarred = starredModels.includes(m);
                        return (
                          <button
                            key={m}
                            type="button"
                            onClick={() => setStarred(
                              isStarred ? starredModels.filter(k => k !== m) : [...starredModels, m]
                            )}
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
        </div>

        {isAdvanced && (
          <>
            {/* Defaults first: they are the only place each setting is spelled
                out, and the table below is bare columns. Read the explanation,
                then the exceptions to it. */}
            <div className="flex flex-col gap-3">
              <SectionHeading>{t('settings.modelTuning.sectionDefaults')}</SectionHeading>
              <AccountTuningDefaults
                metadata={modelMetadata}
                validModelNames={validModelNames}
                compactionProfiles={compactionProfiles}
              />
            </div>

            <div className="flex flex-col gap-3">
              <SectionHeading>{t('settings.modelTuning.perModel')}</SectionHeading>
              <PerModelMatrix
                metadata={modelMetadata}
                validModelNames={validModelNames}
                compactionProfiles={compactionProfiles}
                pinnedGuidance={hookSystemDefaults?.prompt_guidance ?? null}
              />
            </div>

            <div className="flex flex-col gap-3">
              <SectionHeading>{t('settings.modelTuning.sectionRouting')}</SectionHeading>
              <div className="flex flex-col gap-4">
                <ModelSelector
                  label={t('settings.modelTuning.fetchModel')}
                  description={t('settings.modelTuning.fetchModelDesc')}
                  value={mPref.fetch_model ?? ''}
                  onChange={(v) => write({ fetch_model: v || null })}
                  models={visibleModels}
                  placeholder={t('settings.modelTuning.defaultsToFlash')}
                  modelAccess={modelAccessMap}
                />
                <ModelSelector
                  label={t('settings.modelTuning.compactionModel')}
                  description={t('settings.modelTuning.compactionModelDesc')}
                  value={mPref.compaction_model ?? ''}
                  onChange={(v) => write({
                    compaction_model: v || null,
                    // Retire the legacy key so the back-compat shim in
                    // resolve_llm_config can't resurrect a stale value when the
                    // user clears compaction_model.
                    summarization_model: null,
                  })}
                  models={visibleModels}
                  placeholder={t('settings.modelTuning.defaultsToFlash')}
                  modelAccess={modelAccessMap}
                />
                <FallbackModelsPicker
                  selected={fallbackModels}
                  onChange={(list) => write({ fallback_models: list })}
                  models={visibleModels}
                />
              </div>
            </div>
          </>
        )}

        {/* Web search */}
        <div className="flex flex-col gap-3">
          <SectionHeading>{t('settings.modelTuning.sectionSearch')}</SectionHeading>
          <div className="flex flex-col gap-1.5">
            <label className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
              {t('settings.searchProvider', 'Web Search Provider')}
            </label>
            <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
              {t('settings.searchProviderDesc', 'Search engine the agent uses for web searches.')}
            </p>
            <Select
              value={searchProvider}
              onChange={(e) => {
                const next = e.target.value;
                const depths = next ? searchProviders?.[next]?.depths ?? [] : [];
                // Depth levels are provider-scoped, so a stale level may not
                // exist on the new provider. A provider offering one level has
                // no depth control at all: leave the stored key standing rather
                // than deleting a level a switch back would want. Default is
                // not that case. It shows no control either, but resolve time
                // still reads the stored level against whichever provider the
                // deployment picks, so an invisible one would keep applying.
                write({
                  search_provider: next || null,
                  ...(!next || depths.length > 1 ? { search_depth: null } : {}),
                });
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
            <div className="flex flex-col gap-1.5">
              <label className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
                {t('settings.searchDepth', 'Search Depth')}
              </label>
              <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
                {t('settings.searchDepthDesc', 'How thoroughly the agent searches the web. Deeper levels use more credits per search.')}
              </p>
              <Select
                value={searchDepth}
                onChange={(e) => write({ search_depth: e.target.value || null })}
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

        {/* Providers & accounts */}
        <div className="flex flex-col gap-3">
          <SectionHeading>{t('settings.modelTuning.sectionProviders')}</SectionHeading>

          <ConnectedAccounts />

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
        </div>

        {saveStatus !== 'idle' && (
          <div className="flex items-center justify-end pt-2">
            {saveStatus === 'pending' && (
              <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>{t('common.saving')}</span>
            )}
            {saveStatus === 'success' && (
              <span className="text-xs" style={{ color: 'var(--color-success)' }}>{t('common.saved')}</span>
            )}
            {saveStatus === 'error' && (
              <span className="text-xs" style={{ color: 'var(--color-loss)' }}>{t('settings.failedToSaveSettings')}</span>
            )}
          </div>
        )}
      </div>
  );
}
