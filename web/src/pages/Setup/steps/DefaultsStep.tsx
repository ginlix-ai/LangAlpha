import { useState, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { Loader2, Info } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { ModelTierConfig } from '@/components/model/ModelTierConfig';
import { useAllModels } from '@/hooks/useAllModels';
import { usePreferences } from '@/hooks/usePreferences';
import { useUpdatePreferences } from '@/hooks/useUpdatePreferences';
import { useTranslation } from 'react-i18next';

// ---------------------------------------------------------------------------
// DefaultsStep — Step 5: Set default primary + flash models
// ---------------------------------------------------------------------------

export default function DefaultsStep() {
  const navigate = useNavigate();
  const { models, modelAccessMap, systemDefaults, isLoading } = useAllModels();
  const { preferences } = usePreferences();
  const updatePreferences = useUpdatePreferences();
  const { t } = useTranslation();

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
    compactionModel: string;
    fetchModel: string;
    fallbackModels: string[];
  }>({
    compactionModel: (otherPref.compaction_model as string) ?? '',
    fetchModel: (otherPref.fetch_model as string) ?? '',
    fallbackModels: (otherPref.fallback_models as string[]) ?? [],
  });
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const canContinue = Boolean(primaryModel && flashModel);

  // ---------------------------------------------------------------------------
  // Handlers
  // ---------------------------------------------------------------------------

  const handleBack = useCallback(() => {
    navigate('/setup/models');
  }, [navigate]);

  const handleAdvancedChange = useCallback(
    (updated: { compactionModel?: string; fetchModel?: string; fallbackModels?: string[] }) => {
      setAdvancedModels((prev) => ({ ...prev, ...updated }));
    },
    [],
  );

  const handleNext = useCallback(async () => {
    if (!primaryModel || !flashModel) return;

    setSaving(true);
    setError(null);

    try {
      // Compaction + fetch default to flash model if not explicitly set
      const compaction = advancedModels.compactionModel || flashModel;
      const fetchModel = advancedModels.fetchModel || flashModel;

      await updatePreferences.mutateAsync({
        other_preference: {
          preferred_model: primaryModel,
          preferred_flash_model: flashModel,
          compaction_model: compaction,
          fetch_model: fetchModel,
          fallback_models: advancedModels.fallbackModels,
        },
      });

      navigate('/setup/ready');
    } catch (e: unknown) {
      const err = e as { response?: { data?: { detail?: string } }; message?: string };
      const detail = err?.response?.data?.detail;
      setError(typeof detail === 'string' ? detail : err?.message ?? t('setup.errorSavePrefs'));
    } finally {
      setSaving(false);
    }
  }, [primaryModel, flashModel, advancedModels, updatePreferences, navigate, t]);

  // ---------------------------------------------------------------------------
  // Render
  // ---------------------------------------------------------------------------

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-20">
        <Loader2 className="h-5 w-5 animate-spin" style={{ color: 'var(--color-text-tertiary)' }} />
      </div>
    );
  }

  // In platform mode, models are already tier-filtered by useAllModels.
  // In OSS mode, they're filtered by configured providers.
  // Either way, `models` is the correct set to display.

  return (
    <div className="flex flex-col gap-4 sm:gap-6">
      {/* Section heading */}
      <div className="flex flex-col gap-1">
        <h2
          className="font-semibold"
          style={{ fontSize: '1.125rem', color: 'var(--color-text-primary)' }}
        >
          {t('setup.chooseYourModels')}
        </h2>
        <p
          className="text-sm"
          style={{ color: 'var(--color-text-secondary)' }}
        >
          {t('setup.chooseYourModelsDesc')}
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

      {/* Model tier config */}
      <ModelTierConfig
        models={models}
        primaryModel={primaryModel}
        onPrimaryModelChange={setPrimaryModel}
        flashModel={flashModel}
        onFlashModelChange={setFlashModel}
        showExplainer
        showAdvanced
        advancedModels={advancedModels}
        onAdvancedModelsChange={handleAdvancedChange}
        systemDefaults={systemDefaults ?? undefined}
        modelAccess={modelAccessMap}
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
          {t('setup.back')}
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
              {t('setup.saving')}
            </>
          ) : (
            t('setup.continue')
          )}
        </Button>
      </div>
    </div>
  );
}
