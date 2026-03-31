import { useState, useCallback, useMemo, useEffect } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { Loader2, Check, Plus, X, KeyRound } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { useAllModels } from '@/hooks/useAllModels';
import { useConfiguredProviders } from '@/hooks/useConfiguredProviders';
import { usePreferences } from '@/hooks/usePreferences';
import { useUpdatePreferences } from '@/hooks/useUpdatePreferences';

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

  const { models: modelsData, isLoading: modelsLoading } = useAllModels();
  const { configuredSet } = useConfiguredProviders();
  const isConfigured = provider ? configuredSet.has(provider) : false;

  // Get built-in models from the manifest for this provider
  const builtInModels = useMemo<string[]>(() => {
    if (!modelsData) return [];
    const raw = modelsData as Record<string, unknown>;
    const metadata = (raw.model_metadata ?? {}) as Record<string, { provider?: string }>;
    const providerMap = (raw.models ?? {}) as Record<string, { models?: string[] }>;

    // Specific provider: get the brand group, then filter by exact flat provider key
    if (brandKey && providerMap[brandKey]) {
      const candidates = providerMap[brandKey].models ?? [];
      if (provider) {
        return candidates.filter((m) => metadata[m]?.provider === provider);
      }
      return candidates.filter((m) => metadata[m]);
    }

    // No specific provider: show all configured providers' models
    const models: string[] = [];
    for (const [, data] of Object.entries(providerMap)) {
      for (const m of data.models ?? []) {
        const modelProvider = metadata[m]?.provider;
        if (modelProvider && configuredSet.has(modelProvider)) {
          models.push(m);
        }
      }
    }
    return models;
  }, [modelsData, provider, brandKey, configuredSet]);

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

  // Model metadata for display names
  const modelMetadata = useMemo<Record<string, Record<string, unknown>>>(() => {
    if (!modelsData) return {};
    const raw = modelsData as Record<string, unknown>;
    return (raw.model_metadata as Record<string, Record<string, unknown>>) ?? {};
  }, [modelsData]);

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
      const existingCustomModelList = ((otherPref.custom_models ?? []) as Array<{ model_id: string; provider: string }>);

      // Keep custom models for other providers, add new ones for this provider
      const otherProviderCustomModels = existingCustomModelList.filter(
        (cm) => cm.provider !== provider && cm.provider !== brandKey,
      );
      const thisProviderCustomModels = [...starred]
        .filter((m) => !builtInSet.has(m))
        .map((m) => ({ model_id: m, provider: provider || brandKey }));

      const allCustomModels = [...otherProviderCustomModels, ...thisProviderCustomModels];

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
      setError(typeof detail === 'string' ? detail : err?.message ?? 'Failed to save model selection.');
    } finally {
      setSaving(false);
    }
  }, [starred, existingStarred, allModels, builtInSet, preferences, provider, brandKey, updatePreferences, navigate]);

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
            {provider ? `Manage ${displayName} models` : 'Select models for quick access'}
          </h2>
          {provider && isConfigured && (
            <button
              type="button"
              onClick={() => navigate('/setup/connect', { state })}
              className="flex items-center gap-1 text-xs font-medium transition-colors hover:opacity-80"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              <KeyRound className="h-3 w-3" />
              Update API key
            </button>
          )}
        </div>
        <p
          className="text-sm"
          style={{ color: 'var(--color-text-secondary)' }}
        >
          {provider
            ? `Choose which ${displayName} models to show in your quick-access menu.`
            : 'Choose which models to show in your quick-access menu.'}
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
            {starred.size === displayModels.length ? 'Deselect all' : 'Select all'}
          </button>
          {!showCustomInput && (
            <button
              type="button"
              onClick={() => setShowCustomInput(true)}
              className="flex items-center gap-1 text-xs font-medium"
              style={{ color: 'var(--color-accent-primary)' }}
            >
              <Plus className="h-3 w-3" />
              Add model
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
            placeholder="Enter model ID (e.g. gpt-4o)"
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
            Add
          </Button>
          <Button
            variant="outline"
            size="sm"
            onClick={() => { setShowCustomInput(false); setCustomModelId(''); }}
          >
            Cancel
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
            {provider ? `No models available for ${displayName}.` : 'No models available.'}
          </p>
          {!showCustomInput && (
            <Button
              variant="outline"
              size="sm"
              onClick={() => setShowCustomInput(true)}
            >
              <Plus className="h-3.5 w-3.5 mr-1" />
              Add a custom model
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
                      custom
                    </span>
                  )}
                </button>
                {isCustom && (
                  <button
                    type="button"
                    onClick={() => handleRemoveCustomModel(model)}
                    className="p-1.5 rounded transition-colors hover:opacity-80 shrink-0"
                    style={{ color: 'var(--color-text-tertiary)' }}
                    aria-label={`Remove ${model}`}
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
            Add another provider?
          </span>
          <span
            className="text-xs"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            You can connect multiple providers for more model options.
          </span>
        </div>
        <Button variant="outline" size="sm" className="shrink-0" onClick={handleAddAnother}>
          + Add
        </Button>
      </div>

      {/* Navigation buttons */}
      <div className="flex items-center justify-between pt-2">
        <Button variant="outline" onClick={handleBack}>
          Back
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
              Saving...
            </>
          ) : (
            'Next step'
          )}
        </Button>
      </div>
    </div>
  );
}
