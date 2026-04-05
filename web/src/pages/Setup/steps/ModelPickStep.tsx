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

// ---------------------------------------------------------------------------
// ModelPickStep — Step 4: Star models for quick access
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
    // Specific provider: get the brand group, then filter by exact flat provider key
    if (brandKey && rawModels[brandKey]) {
      const candidates = rawModels[brandKey].models ?? [];
      if (provider) {
        return candidates.filter((m) => allMetadata[m]?.provider === provider);
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
  const existingCustomModels = useMemo<string[]>(() => {
    if (!preferences) return [];
    const prefs = preferences as Record<string, unknown>;
    const otherPref = (prefs.other_preference ?? {}) as Record<string, unknown>;
    const customModels = (otherPref.custom_models ?? []) as Array<{ model_id: string; provider: string }>;
    // Filter to current provider or brandKey
    return customModels
      .filter((cm) => cm.provider === provider || cm.provider === brandKey)
      .map((cm) => cm.model_id);
  }, [preferences, provider, brandKey]);

  // Combine built-in + custom models
  const allModels = useMemo<string[]>(() => {
    const set = new Set([...builtInModels, ...existingCustomModels]);
    return [...set];
  }, [builtInModels, existingCustomModels]);

  const builtInSet = useMemo(() => new Set(builtInModels), [builtInModels]);

  // Model metadata for display names — use allMetadata from useAllModels directly
  const modelMetadata = allMetadata as Record<string, Record<string, unknown>>;

  // Initialize starred from preferences or default to all
  const existingStarred = useMemo<string[]>(() => {
    if (!preferences) return [];
    const prefs = preferences as Record<string, unknown>;
    const otherPref = (prefs.other_preference ?? {}) as Record<string, unknown>;
    return (otherPref.starred_models ?? []) as string[];
  }, [preferences]);

  const [starred, setStarred] = useState<Set<string>>(new Set());
  const [initialized, setInitialized] = useState(false);

  // Initialize: use existing starred that overlap with this provider, or all if first time
  useEffect(() => {
    if (initialized || allModels.length === 0) return;
    const relevantStarred = existingStarred.filter((m) => allModels.includes(m));
    if (relevantStarred.length > 0) {
      setStarred(new Set(relevantStarred));
    } else {
      // First time: select all
      setStarred(new Set(allModels));
    }
    setInitialized(true);
  }, [allModels, existingStarred, initialized]);

  // Custom model input
  const [customModelId, setCustomModelId] = useState('');
  const [showCustomInput, setShowCustomInput] = useState(false);

  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const toggleModel = useCallback((model: string) => {
    setStarred((prev) => {
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
    if (starred.size === allModels.length) {
      setStarred(new Set());
    } else {
      setStarred(new Set(allModels));
    }
  }, [starred.size, allModels]);

  const handleAddCustomModel = useCallback(() => {
    const modelId = customModelId.trim();
    if (!modelId) return;
    if (allModels.includes(modelId)) {
      // Already exists, just star it
      setStarred((prev) => new Set([...prev, modelId]));
      setCustomModelId('');
      setShowCustomInput(false);
      return;
    }
    // Will be saved as custom_model in handleNext
    setStarred((prev) => new Set([...prev, modelId]));
    setCustomModelId('');
    setShowCustomInput(false);
  }, [customModelId, allModels]);

  const handleRemoveCustomModel = useCallback((model: string) => {
    setStarred((prev) => {
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
      // Merge with existing starred_models from other providers
      const otherStarred = existingStarred.filter((m) => !allModels.includes(m) && !starred.has(m));
      const mergedStarred = [...otherStarred, ...starred];

      // Collect any new custom models (not in built-in set)
      const prefs = (preferences as Record<string, unknown>) ?? {};
      const otherPref = ((prefs.other_preference ?? {}) as Record<string, unknown>);
      const existingCustomModelList = ((otherPref.custom_models ?? []) as Array<{ name?: string; model_id: string; provider: string }>);

      // Keep custom models for other providers
      const otherProviderCustomModels = existingCustomModelList.filter(
        (cm) => cm.provider !== provider && cm.provider !== brandKey,
      );
      // Preserve existing custom models for this provider that are still starred
      const existingThisProvider = existingCustomModelList.filter(
        (cm) => (cm.provider === provider || cm.provider === brandKey) && starred.has(cm.name || cm.model_id),
      );
      const existingIds = new Set(existingThisProvider.map((cm) => cm.name || cm.model_id));
      // Add newly starred non-builtin models as custom entries
      const newCustomModels = [...starred]
        .filter((m) => !builtInSet.has(m) && !existingIds.has(m))
        .map((m) => ({ name: m, model_id: m, provider: provider || brandKey }));

      const allCustomModels = [...otherProviderCustomModels, ...existingThisProvider, ...newCustomModels];

      await updatePreferences.mutateAsync({
        other_preference: {
          starred_models: mergedStarred,
          ...(allCustomModels.length > 0
            ? { custom_models: allCustomModels }
            : { custom_models: otherProviderCustomModels.length > 0 ? otherProviderCustomModels : null }),
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
  }, [starred, existingStarred, allModels, builtInSet, preferences, provider, brandKey, updatePreferences, navigate, t]);

  // "Add another provider" loops back to method selection
  const handleAddAnother = useCallback(() => {
    navigate('/setup/method');
  }, [navigate]);

  // All models to display: built-in + any custom ones the user just added
  const displayModels = useMemo(() => {
    const customAdded = [...starred].filter((m) => !allModels.includes(m));
    return [...allModels, ...customAdded];
  }, [allModels, starred]);

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
            {starred.size === displayModels.length ? t('setup.deselectAll') : t('setup.selectAll')}
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

      {/* Custom model input */}
      {showCustomInput && (
        <div className="flex gap-2">
          <Input
            value={customModelId}
            onChange={(e) => setCustomModelId(e.target.value)}
            placeholder={t('setup.modelIdInputPlaceholder')}
            className="flex-1"
            autoComplete="off"
            spellCheck={false}
            onKeyDown={(e) => {
              if (e.key === 'Enter') handleAddCustomModel();
              if (e.key === 'Escape') { setShowCustomInput(false); setCustomModelId(''); }
            }}
          />
          <Button
            variant="default"
            size="sm"
            disabled={!customModelId.trim()}
            onClick={handleAddCustomModel}
          >
            {t('setup.add')}
          </Button>
          <Button
            variant="outline"
            size="sm"
            onClick={() => { setShowCustomInput(false); setCustomModelId(''); }}
          >
            {t('common.cancel')}
          </Button>
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
            const isChecked = starred.has(model);
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
