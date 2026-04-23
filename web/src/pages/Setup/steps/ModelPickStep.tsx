import { useState, useCallback, useMemo, useEffect } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { Loader2, Check, Plus, X, KeyRound, Info } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { useAllModels } from '@/hooks/useAllModels';
import { useConfiguredProviders } from '@/hooks/useConfiguredProviders';
import { usePreferences } from '@/hooks/usePreferences';
import { useUpdatePreferences } from '@/hooks/useUpdatePreferences';
import { useTranslation } from 'react-i18next';
import { slugifyModelName } from './slugifyModelName';
import { computeSlotCleanup } from './modelSlotCleanup';

// ---------------------------------------------------------------------------
// ModelPickStep — Step 4: Choose which models to add to the configured model list.
//
// Data model note: the user's "configured list" persists as ``starred_models``
// in preferences. That preference also backs DefaultsStep and the Settings
// quick-picker filters, so the wizard selection carries through without an
// additional concept. The UI language avoids "star" because the action here
// is "include this model in my configured set for this provider" — starring
// for later re-ordering belongs to Settings.
// ---------------------------------------------------------------------------

interface LocationState {
  method?: string;
  provider?: string;
  displayName?: string;
  brandKey?: string;
  // Pass-through for ConnectStep (used by "Update API key" link)
  sdk?: string | null;
  defaultBaseUrl?: string | null;
  useResponseApi?: boolean;
  regionVariants?: unknown[] | null;
  defaultRegion?: string | null;
  dynamicModels?: boolean;
}

export default function ModelPickStep() {
  const navigate = useNavigate();
  const location = useLocation();
  const updatePreferences = useUpdatePreferences();
  const { preferences } = usePreferences();
  const { t } = useTranslation();

  const state = (location.state as LocationState | null) ?? {};

  // Redirect to method step if essential state is missing (e.g. browser refresh)
  useEffect(() => {
    if (!state.provider) {
      navigate('/setup/method', { replace: true });
    }
  }, [state.provider, navigate]);

  const method = state.method;
  const provider = state.provider ?? '';
  const displayName = state.displayName ?? provider;
  const brandKey = state.brandKey ?? provider;

  const { rawModels, metadata: allMetadata, isLoading: modelsLoading } = useAllModels();
  const { configuredSet } = useConfiguredProviders();
  const isConfigured = provider ? configuredSet.has(provider) : false;

  // Get built-in models from the manifest for this provider.
  // Uses rawModels (pre-filter snapshot) so ALL models are visible
  // regardless of the user's current access tier or configured keys.
  const builtInModels = useMemo<string[]>(() => {
    // Specific provider: get the brand group, then filter by exact flat provider key.
    // Variants that share a catalog with the parent (e.g. coding-plan variants
    // like z-ai-coding, minimax-coding) have no own entries in models.json —
    // when the strict filter yields nothing, fall back to the parent's models
    // so the user still sees their provider's catalog.
    if (brandKey && rawModels[brandKey]) {
      const candidates = rawModels[brandKey].models ?? [];
      if (provider) {
        const exact = candidates.filter((m) => allMetadata[m]?.provider === provider);
        if (exact.length > 0 || provider === brandKey) return exact;
        // Variant shares parent's catalog
        return candidates.filter((m) => allMetadata[m]?.provider === brandKey);
      }
      return candidates.filter((m) => allMetadata[m]);
    }

    // Brand group not found — provider has no models in the manifest.
    // Return empty so the user can add custom models instead.
    if (provider) return [];

    // No specific provider: show all configured providers' models
    const models: string[] = [];
    for (const [, data] of Object.entries(rawModels)) {
      for (const m of data.models ?? []) {
        const modelProvider = allMetadata[m]?.provider;
        if (modelProvider && configuredSet.has(modelProvider)) {
          models.push(m);
        }
      }
    }
    return models;
  }, [rawModels, allMetadata, provider, brandKey, configuredSet]);

  // User's custom models for this provider (from preferences)
  //
  // Returns the ``name`` field (the display / stored identifier), not
  // ``model_id``. ``starred_models``, ``preferred_model``, and the rest of
  // the preferences key entries all reference the ``name``. Returning
  // ``model_id`` here orphans custom entries whose ``name`` differs from
  // ``model_id`` (the default when the user types both via the new form).
  const existingCustomModels = useMemo<string[]>(() => {
    if (!preferences) return [];
    const prefs = preferences as Record<string, unknown>;
    const otherPref = (prefs.other_preference ?? {}) as Record<string, unknown>;
    const customModels = (otherPref.custom_models ?? []) as Array<{ name?: string; model_id: string; provider: string }>;
    return customModels
      .filter((cm) => cm.provider === provider || cm.provider === brandKey)
      .map((cm) => cm.name ?? cm.model_id);
  }, [preferences, provider, brandKey]);

  // Combine built-in + custom models
  const allModels = useMemo<string[]>(() => {
    const set = new Set([...builtInModels, ...existingCustomModels]);
    return [...set];
  }, [builtInModels, existingCustomModels]);

  const builtInSet = useMemo(() => new Set(builtInModels), [builtInModels]);

  // Model metadata for display names — use allMetadata from useAllModels directly
  const modelMetadata = allMetadata as Record<string, Record<string, unknown>>;

  // Seed the configured list from preferences (``starred_models`` is the
  // historical key name — it backs the "configured models" UX now).
  const existingStarred = useMemo<string[]>(() => {
    if (!preferences) return [];
    const prefs = preferences as Record<string, unknown>;
    const otherPref = (prefs.other_preference ?? {}) as Record<string, unknown>;
    return (otherPref.starred_models ?? []) as string[];
  }, [preferences]);

  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [initialized, setInitialized] = useState(false);

  // Initialize: use the previously-configured list that overlaps with this
  // provider, or select all on first entry. Crucially, wait for
  // ``preferences`` to load — otherwise the effect runs with
  // ``existingStarred = []`` (before the network call finishes), falls into
  // the "first time: select all" branch, and never re-runs because
  // ``initialized`` is already true. Result: the user's previously-saved
  // list gets blown away on re-entry.
  useEffect(() => {
    if (initialized || allModels.length === 0 || !preferences) return;
    const relevantStarred = existingStarred.filter((m) => allModels.includes(m));
    if (relevantStarred.length > 0) {
      setSelected(new Set(relevantStarred));
    } else {
      // First time: select all
      setSelected(new Set(allModels));
    }
    setInitialized(true);
  }, [allModels, existingStarred, initialized, preferences]);

  // Custom model form — the user types the display name (used as the
  // ``name`` in ``custom_models[]``), the upstream model_id, and toggles
  // input modalities. Shadow semantics: the name may collide with a
  // built-in; the backend resolver picks the custom entry first.
  const [customName, setCustomName] = useState('');
  const [customId, setCustomId] = useState('');
  const [customModalities, setCustomModalities] = useState<Set<string>>(new Set());
  const [showCustomInput, setShowCustomInput] = useState(false);
  // Carries form-supplied metadata (model_id + modalities) for each
  // user-added name. handleNext reads this to construct rich
  // ``custom_models[]`` entries instead of defaulting model_id to the
  // display name and leaving modalities blank.
  const [pendingCustomEntries, setPendingCustomEntries] = useState<
    Map<string, { modelId: string; modalities: string[] }>
  >(new Map());

  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const toggleModel = useCallback((model: string) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(model)) {
        next.delete(model);
      } else {
        next.add(model);
      }
      return next;
    });
  }, []);

  const toggleAll = useCallback(() => {
    if (selected.size === allModels.length) {
      setSelected(new Set());
    } else {
      setSelected(new Set(allModels));
    }
  }, [selected.size, allModels]);

  const toggleCustomModality = useCallback((modality: string) => {
    setCustomModalities((prev) => {
      const next = new Set(prev);
      if (next.has(modality)) next.delete(modality);
      else next.add(modality);
      return next;
    });
  }, []);

  const resetCustomForm = useCallback(() => {
    setCustomName('');
    setCustomId('');
    setCustomModalities(new Set());
    setShowCustomInput(false);
  }, []);

  const handleAddCustomModel = useCallback(() => {
    const name = slugifyModelName(customName);
    if (!name) return;
    // Upstream ID is what the provider's API expects — keep the user's
    // literal input. Only fall back to the slugified name when empty.
    const modelId = customId.trim() || name;
    // text is always on; store the full list when the user picked extras.
    const modalities = customModalities.size > 0
      ? ['text', ...Array.from(customModalities).filter((m) => m !== 'text')]
      : [];
    setPendingCustomEntries((prev) => {
      const next = new Map(prev);
      next.set(name, { modelId, modalities });
      return next;
    });
    setSelected((prev) => new Set([...prev, name]));
    resetCustomForm();
  }, [customName, customId, customModalities, resetCustomForm]);

  const handleRemoveCustomModel = useCallback((model: string) => {
    setSelected((prev) => {
      const next = new Set(prev);
      next.delete(model);
      return next;
    });
  }, []);

  const handleBack = useCallback(() => {
    navigate('/setup/provider', { state: { method } });
  }, [navigate, method]);

  const handleNext = useCallback(async () => {
    setSaving(true);
    setError(null);

    try {
      // The persisted list spans all providers; keep entries the user
      // selected under other providers, then union with the selections for
      // this step.
      const selectedFromOtherProviders = existingStarred.filter((m) => !allModels.includes(m) && !selected.has(m));
      const mergedConfigured = [...selectedFromOtherProviders, ...selected];

      const prefs = (preferences as Record<string, unknown>) ?? {};
      const otherPref = ((prefs.other_preference ?? {}) as Record<string, unknown>);
      const existingCustomModelList = ((otherPref.custom_models ?? []) as Array<{ name?: string; model_id: string; provider: string }>);

      // Keep custom models for other providers
      const otherProviderCustomModels = existingCustomModelList.filter(
        (cm) => cm.provider !== provider && cm.provider !== brandKey,
      );
      // Preserve existing custom models for this provider that are still selected
      const existingThisProvider = existingCustomModelList.filter(
        (cm) => (cm.provider === provider || cm.provider === brandKey) && selected.has(cm.name || cm.model_id),
      );
      const existingIds = new Set(existingThisProvider.map((cm) => cm.name || cm.model_id));

      // A selected model needs a custom_models[] entry when either:
      //   (a) user-typed — not in any built-in catalog, OR
      //   (b) variant context — a parent-catalog model selected while
      //       configuring a variant (e.g. z-ai-coding). The model's own
      //       metadata.provider points at the parent (z-ai), but BYOK must
      //       route through the variant's key. Writing a custom entry with
      //       provider=<variant slug> makes the backend resolver pick it up.
      const providerSlug = provider || brandKey;
      const newCustomModels = [...selected]
        .filter((m) => !existingIds.has(m))
        .filter((m) => {
          if (!builtInSet.has(m)) return true;
          const modelProvider = modelMetadata[m]?.provider as string | undefined;
          return !!providerSlug && !!modelProvider && modelProvider !== providerSlug;
        })
        .map((m) => {
          const pending = pendingCustomEntries.get(m);
          const entry: Record<string, unknown> = {
            name: m,
            model_id: pending?.modelId ?? m,
            provider: providerSlug,
          };
          if (pending && pending.modalities.length > 0) {
            entry.input_modalities = pending.modalities;
          }
          return entry;
        });

      const allCustomModels = [...otherProviderCustomModels, ...existingThisProvider, ...newCustomModels];

      // Clear default/flash/compaction/fetch/fallback slots when the user
      // unchecked the model that was filling them. Otherwise an unchecked
      // model keeps appearing as the current selection in the chat dropdown
      // (since chat-input seeds selectedModel from preferred_model).
      const slotCleanup = computeSlotCleanup({
        otherPref: otherPref as Parameters<typeof computeSlotCleanup>[0]['otherPref'],
        allModels,
        mergedConfigured,
      });

      await updatePreferences.mutateAsync({
        other_preference: {
          // ``starred_models`` is the preference key that backs the
          // configured-list UX in DefaultsStep and Settings. Keep writing
          // to it — the key name is a historical artifact, the semantics
          // the user sees is "configured models".
          starred_models: mergedConfigured,
          ...(allCustomModels.length > 0
            ? { custom_models: allCustomModels }
            : { custom_models: otherProviderCustomModels.length > 0 ? otherProviderCustomModels : null }),
          ...slotCleanup.nulls,
          ...(slotCleanup.fallback_models !== undefined
            ? { fallback_models: slotCleanup.fallback_models }
            : {}),
        },
      });

      navigate('/setup/defaults');
    } catch (e: unknown) {
      const err = e as { response?: { data?: { detail?: string } }; message?: string };
      const detail = err?.response?.data?.detail;
      setError(typeof detail === 'string' ? detail : err?.message ?? t('setup.errorSaveModels'));
    } finally {
      setSaving(false);
    }
  }, [selected, existingStarred, allModels, builtInSet, preferences, provider, brandKey, modelMetadata, pendingCustomEntries, updatePreferences, navigate, t]);

  // "Add another provider" loops back to method selection
  const handleAddAnother = useCallback(() => {
    navigate('/setup/method');
  }, [navigate]);

  // All models to display: built-in + any custom ones the user just added
  const displayModels = useMemo(() => {
    const customAdded = [...selected].filter((m) => !allModels.includes(m));
    return [...allModels, ...customAdded];
  }, [allModels, selected]);

  if (modelsLoading) {
    return (
      <div className="flex items-center justify-center py-20">
        <Loader2 className="h-5 w-5 animate-spin" style={{ color: 'var(--color-text-tertiary)' }} />
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-4 sm:gap-6">
      {/* Section heading */}
      <div className="flex flex-col gap-1">
        <div className="flex items-center justify-between">
          <h2
            className="font-semibold"
            style={{ fontSize: '1.125rem', color: 'var(--color-text-primary)' }}
          >
            {provider ? t('setup.manageModels', { provider: displayName }) : t('setup.modelsTitle')}
          </h2>
          {provider && isConfigured && (
            <button
              type="button"
              onClick={() => navigate('/setup/connect', { state })}
              className="flex items-center gap-1 text-xs font-medium transition-colors hover:opacity-80"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              <KeyRound className="h-3 w-3" />
              {t('setup.updateApiKey')}
            </button>
          )}
        </div>
        <p
          className="text-sm"
          style={{ color: 'var(--color-text-secondary)' }}
        >
          {provider
            ? t('setup.chooseModels', { provider: displayName })
            : t('setup.chooseModelsGeneric')}
        </p>
      </div>

      {/* Model access reminder */}
      <div
        className="flex items-start gap-2.5 rounded-lg px-3.5 py-3"
        style={{
          background: 'var(--color-accent-soft)',
          border: '1px solid var(--color-accent-primary)',
        }}
      >
        <Info
          className="h-4 w-4 shrink-0 mt-0.5"
          style={{ color: 'var(--color-accent-primary)' }}
        />
        <p
          className="text-xs leading-relaxed"
          style={{ color: 'var(--color-text-secondary)' }}
        >
          {t('setup.modelAccessReminder')}
        </p>
      </div>

      {/* Select all toggle */}
      {displayModels.length > 0 && (
        <div className="flex items-center justify-between">
          <button
            type="button"
            onClick={toggleAll}
            className="text-xs font-medium"
            style={{ color: 'var(--color-accent-primary)' }}
          >
            {selected.size === displayModels.length ? t('setup.deselectAll') : t('setup.selectAll')}
          </button>
          {!showCustomInput && (
            <button
              type="button"
              onClick={() => setShowCustomInput(true)}
              className="flex items-center gap-1 text-xs font-medium"
              style={{ color: 'var(--color-accent-primary)' }}
            >
              <Plus className="h-3 w-3" />
              {t('setup.addModel')}
            </button>
          )}
        </div>
      )}

      {/* Custom model input — name, upstream id, and capabilities */}
      {showCustomInput && (
        <div
          className="flex flex-col gap-3 rounded-lg p-3"
          style={{
            background: 'var(--color-bg-surface)',
            border: '1px solid var(--color-border-default)',
          }}
        >
          <div className="flex flex-col gap-1">
            <label
              className="text-xs font-medium"
              style={{ color: 'var(--color-text-secondary)' }}
            >
              {t('setup.customModelNameLabel')}
            </label>
            <Input
              value={customName}
              onChange={(e) => setCustomName(e.target.value)}
              placeholder={t('setup.customModelNamePlaceholder')}
              autoComplete="off"
              spellCheck={false}
              onKeyDown={(e) => {
                if (e.key === 'Escape') resetCustomForm();
              }}
            />
            {(() => {
              const slug = slugifyModelName(customName);
              const trimmed = customName.trim();
              if (!trimmed || slug === trimmed) return null;
              return (
                <span
                  className="text-[11px]"
                  style={{ color: 'var(--color-text-tertiary)' }}
                >
                  {slug
                    ? t('setup.customModelNameSlugPreview', { slug })
                    : t('setup.customModelNameSlugEmpty')}
                </span>
              );
            })()}
          </div>
          <div className="flex flex-col gap-1">
            <label
              className="text-xs font-medium"
              style={{ color: 'var(--color-text-secondary)' }}
            >
              {t('setup.customModelIdLabel')}
            </label>
            <Input
              value={customId}
              onChange={(e) => setCustomId(e.target.value)}
              placeholder={t('setup.customModelIdPlaceholder')}
              autoComplete="off"
              spellCheck={false}
              onKeyDown={(e) => {
                if (e.key === 'Enter') handleAddCustomModel();
                if (e.key === 'Escape') resetCustomForm();
              }}
            />
            <span
              className="text-[11px]"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              {t('setup.customModelIdHint')}
            </span>
          </div>
          <div className="flex flex-wrap items-center gap-1.5">
            <span
              className="text-xs font-medium mr-1"
              style={{ color: 'var(--color-text-secondary)' }}
            >
              {t('setup.capabilities', { defaultValue: 'Capabilities' })}:
            </span>
            <span
              className="inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-medium"
              style={{ background: 'var(--color-accent-soft)', color: 'var(--color-accent-primary)', opacity: 0.6 }}
            >
              Text
            </span>
            {(['image', 'pdf'] as const).map((mod) => {
              const active = customModalities.has(mod);
              return (
                <button
                  key={mod}
                  type="button"
                  onClick={() => toggleCustomModality(mod)}
                  className="inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-medium transition-colors"
                  style={{
                    background: active ? 'var(--color-accent-soft)' : 'transparent',
                    color: active ? 'var(--color-accent-primary)' : 'var(--color-text-tertiary)',
                    border: `1px solid ${active ? 'var(--color-accent-primary)' : 'var(--color-border-default)'}`,
                  }}
                >
                  {mod === 'image' ? 'Image' : 'PDF'}
                </button>
              );
            })}
          </div>
          <div className="flex items-center justify-end gap-2">
            <Button variant="outline" size="sm" onClick={resetCustomForm}>
              {t('common.cancel')}
            </Button>
            <Button
              variant="default"
              size="sm"
              disabled={!customName.trim()}
              onClick={handleAddCustomModel}
            >
              {t('setup.add')}
            </Button>
          </div>
        </div>
      )}

      {/* Model checkboxes */}
      {displayModels.length === 0 ? (
        <div className="flex flex-col items-center gap-3 py-8">
          <p
            className="text-sm text-center"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            {provider ? t('setup.noModelsFor', { provider: displayName }) : t('setup.noModels')}
          </p>
          {!showCustomInput && (
            <Button
              variant="outline"
              size="sm"
              onClick={() => setShowCustomInput(true)}
            >
              <Plus className="h-3.5 w-3.5 mr-1" />
              {t('setup.addCustomModel')}
            </Button>
          )}
        </div>
      ) : (
        <div className="flex flex-col gap-1 max-h-[40vh] sm:max-h-none overflow-y-auto">
          {displayModels.map((model) => {
            const isChecked = selected.has(model);
            const meta = modelMetadata[model] ?? {};
            const label = (meta.display_name as string) ?? model;
            const isCustom = !builtInSet.has(model);
            return (
              <div
                key={model}
                className="flex items-center gap-0"
              >
                <button
                  type="button"
                  role="checkbox"
                  aria-checked={isChecked}
                  onClick={() => toggleModel(model)}
                  className="flex-1 flex items-center gap-3 rounded-lg px-3 py-2.5 text-left transition-colors"
                  style={{
                    background: isChecked ? 'var(--color-accent-soft)' : undefined,
                  }}
                >
                  <div
                    className="flex items-center justify-center w-5 h-5 rounded border shrink-0 transition-colors"
                    style={{
                      borderColor: isChecked
                        ? 'var(--color-accent-primary)'
                        : 'var(--color-border-default)',
                      background: isChecked ? 'var(--color-accent-primary)' : undefined,
                    }}
                  >
                    {isChecked && <Check className="h-3 w-3" style={{ color: '#fff' }} strokeWidth={3} />}
                  </div>
                  <span
                    className="text-sm"
                    style={{ color: 'var(--color-text-primary)' }}
                  >
                    {label}
                  </span>
                  {isCustom && (
                    <span
                      className="text-[10px] px-1.5 py-0.5 rounded font-medium"
                      style={{
                        background: 'var(--color-bg-page)',
                        color: 'var(--color-text-tertiary)',
                        border: '1px solid var(--color-border-default)',
                      }}
                    >
                      {t('setup.customBadge')}
                    </span>
                  )}
                </button>
                {isCustom && (
                  <button
                    type="button"
                    onClick={() => handleRemoveCustomModel(model)}
                    className="p-1.5 rounded transition-colors hover:opacity-80 shrink-0"
                    style={{ color: 'var(--color-text-tertiary)' }}
                    aria-label={t('setup.removeModel', { model })}
                  >
                    <X className="h-3.5 w-3.5" />
                  </button>
                )}
              </div>
            );
          })}
        </div>
      )}

      {error && (
        <p className="text-sm" style={{ color: 'var(--color-loss)' }}>
          {error}
        </p>
      )}

      {/* Add another provider */}
      <div
        className="flex items-center justify-between rounded-lg p-4"
        style={{
          background: 'var(--color-bg-surface)',
          border: '1px solid var(--color-border-default)',
        }}
      >
        <div className="flex flex-col gap-0.5">
          <span
            className="text-sm font-medium"
            style={{ color: 'var(--color-text-primary)' }}
          >
            {t('setup.addAnotherProvider')}
          </span>
          <span
            className="text-xs"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            {t('setup.addAnotherDescription')}
          </span>
        </div>
        <Button variant="outline" size="sm" className="shrink-0" onClick={handleAddAnother}>
          {t('setup.addAnother')}
        </Button>
      </div>

      {/* Navigation buttons */}
      <div className="flex items-center justify-between pt-2">
        <Button variant="outline" onClick={handleBack}>
          {t('setup.back')}
        </Button>
        <Button
          variant="default"
          disabled={saving}
          onClick={handleNext}
          className="min-w-[120px]"
        >
          {saving ? (
            <>
              <Loader2 className="h-4 w-4 mr-1.5 animate-spin" />
              {t('setup.saving')}
            </>
          ) : (
            t('setup.nextStep')
          )}
        </Button>
      </div>
    </div>
  );
}
