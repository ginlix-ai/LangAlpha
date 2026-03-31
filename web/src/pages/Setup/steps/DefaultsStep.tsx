import { useState, useCallback, useMemo, useEffect, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import { Loader2 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { ModelTierConfig } from '@/components/model/ModelTierConfig';
import type { ProviderModelsData } from '@/components/model/types';
import { useAllModels } from '@/hooks/useAllModels';
import { useConfiguredProviders } from '@/hooks/useConfiguredProviders';
import { usePreferences } from '@/hooks/usePreferences';
import { useUpdatePreferences } from '@/hooks/useUpdatePreferences';

// ---------------------------------------------------------------------------
// DefaultsStep — Step 5: Set default primary + flash models
// ---------------------------------------------------------------------------

export default function DefaultsStep() {
  const navigate = useNavigate();
  const { models, isLoading: modelsLoading } = useAllModels();
  const { configuredSet } = useConfiguredProviders();
  const { preferences } = usePreferences();
  const updatePreferences = useUpdatePreferences();

  // ---------------------------------------------------------------------------
  // Filter models to only those the user has access to.
  //
  // model_metadata has per-model { provider, sdk }. The "provider" is the
  // actual provider key (e.g. "claude-oauth", "anthropic", "codex-oauth").
  // We check if that provider is in the user's configured set.
  //
  // The "models" response groups by parent provider, so we rebuild the
  // grouped structure with only accessible models.
  // ---------------------------------------------------------------------------

  const normalizedModels = useMemo<Record<string, ProviderModelsData>>(() => {
    if (!models) return {};
    const raw = models as Record<string, unknown>;
    const providerMap = (raw.models ?? raw) as Record<string, Record<string, unknown>>;
    const metadata = (raw.model_metadata ?? {}) as Record<string, { provider?: string }>;

    const hasFilter = configuredSet.size > 0;
    const out: Record<string, ProviderModelsData> = {};

    for (const [groupKey, data] of Object.entries(providerMap)) {
      if (!data || typeof data !== 'object') continue;
      const allModels = (data.models as string[]) ?? [];

      // Filter: keep model if its actual provider (from metadata) is configured
      const filtered = hasFilter
        ? allModels.filter((m) => {
            const modelProvider = metadata[m]?.provider;
            if (!modelProvider) return false;
            // Check direct match or parent match
            return configuredSet.has(modelProvider) || configuredSet.has(groupKey);
          })
        : allModels;

      if (filtered.length > 0) {
        out[groupKey] = {
          models: filtered,
          display_name: (data.display_name as string) ?? groupKey,
        };
      }
    }
    return out;
  }, [models, configuredSet]);

  // System defaults from models response
  const systemDefaults = useMemo(() => {
    if (!models) return undefined;
    const raw = models as Record<string, unknown>;
    return raw.system_defaults as {
      default_model?: string;
      flash_model?: string;
      summarization_model?: string;
      fetch_model?: string;
      fallback_models?: string[];
    } | undefined;
  }, [models]);

  // ---------------------------------------------------------------------------
  // Selection state — seed from existing preferences if available
  // ---------------------------------------------------------------------------

  const prefs = preferences as Record<string, unknown> | null;
  const otherPref = (prefs?.other_preference ?? {}) as Record<string, unknown>;

  const [primaryModel, setPrimaryModel] = useState<string>(
    () => (otherPref.preferred_model as string) ?? '',
  );
  const [flashModel, setFlashModel] = useState<string>(
    () => (otherPref.preferred_flash_model as string) ?? '',
  );
  const [advancedModels, setAdvancedModels] = useState<{
    summarizationModel: string;
    fetchModel: string;
    fallbackModels: string[];
  }>({
    summarizationModel: (otherPref.summarization_model as string) ?? '',
    fetchModel: (otherPref.fetch_model as string) ?? '',
    fallbackModels: (otherPref.fallback_models as string[]) ?? [],
  });
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const canContinue = Boolean(primaryModel && flashModel);

  // Collect all user-accessible model names for fallback list
  const allAccessibleModels = useMemo<string[]>(() => {
    const out: string[] = [];
    for (const group of Object.values(normalizedModels)) {
      if (group.models) out.push(...group.models);
    }
    return out;
  }, [normalizedModels]);

  // Seed fallback with all accessible models (minus primary/flash) once
  const fallbackSeeded = useRef(false);
  useEffect(() => {
    if (!fallbackSeeded.current && allAccessibleModels.length > 0) {
      fallbackSeeded.current = true;
      setAdvancedModels((prev) => ({
        ...prev,
        fallbackModels: allAccessibleModels.filter((m) => m !== primaryModel && m !== flashModel),
      }));
    }
  }, [allAccessibleModels, primaryModel, flashModel]);

  // ---------------------------------------------------------------------------
  // Handlers
  // ---------------------------------------------------------------------------

  const handleBack = useCallback(() => {
    navigate('/setup/models');
  }, [navigate]);

  const handleAdvancedChange = useCallback(
    (updated: { summarizationModel?: string; fetchModel?: string; fallbackModels?: string[] }) => {
      setAdvancedModels((prev) => ({ ...prev, ...updated }));
    },
    [],
  );

  const handleNext = useCallback(async () => {
    if (!primaryModel || !flashModel) return;

    setSaving(true);
    setError(null);

    try {
      // Summarization + fetch default to flash model if not explicitly set
      const summarization = advancedModels.summarizationModel || flashModel;
      const fetchModel = advancedModels.fetchModel || flashModel;

      await updatePreferences.mutateAsync({
        other_preference: {
          preferred_model: primaryModel,
          preferred_flash_model: flashModel,
          summarization_model: summarization,
          fetch_model: fetchModel,
          fallback_models: advancedModels.fallbackModels.length > 0
            ? advancedModels.fallbackModels
            : null,
        },
      });

      navigate('/setup/ready');
    } catch (e: unknown) {
      const err = e as { response?: { data?: { detail?: string } }; message?: string };
      const detail = err?.response?.data?.detail;
      setError(typeof detail === 'string' ? detail : err?.message ?? 'Failed to save model preferences.');
    } finally {
      setSaving(false);
    }
  }, [primaryModel, flashModel, advancedModels, updatePreferences, navigate]);

  // ---------------------------------------------------------------------------
  // Render
  // ---------------------------------------------------------------------------

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
        <h2
          className="font-semibold"
          style={{ fontSize: '1.125rem', color: 'var(--color-text-primary)' }}
        >
          Choose your models
        </h2>
        <p
          className="text-sm"
          style={{ color: 'var(--color-text-secondary)' }}
        >
          Select which models to use for deep research and quick answers. You can change these anytime.
        </p>
      </div>

      {/* Model tier config — no filterProviders needed, models are pre-filtered */}
      <ModelTierConfig
        models={normalizedModels}
        primaryModel={primaryModel}
        onPrimaryModelChange={setPrimaryModel}
        flashModel={flashModel}
        onFlashModelChange={setFlashModel}
        showExplainer
        showAdvanced
        advancedModels={advancedModels}
        onAdvancedModelsChange={handleAdvancedChange}
        systemDefaults={systemDefaults}
      />

      {/* Error */}
      {error && (
        <p className="text-sm" style={{ color: 'var(--color-loss)' }}>
          {error}
        </p>
      )}

      {/* Navigation buttons */}
      <div className="flex items-center justify-between pt-2">
        <Button variant="outline" onClick={handleBack}>
          Back
        </Button>
        <Button
          variant="default"
          disabled={saving || !canContinue}
          onClick={handleNext}
          className="min-w-[120px]"
        >
          {saving ? (
            <>
              <Loader2 className="h-4 w-4 mr-1.5 animate-spin" />
              Saving...
            </>
          ) : (
            'Continue'
          )}
        </Button>
      </div>
    </div>
  );
}
