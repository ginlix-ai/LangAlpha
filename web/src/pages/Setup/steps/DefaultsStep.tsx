import { useState, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { motion, AnimatePresence } from 'framer-motion';
import { ChevronRight, Info } from 'lucide-react';
import { cn } from '@/lib/utils';
import { Button } from '@/components/ui/button';
import { Loader } from '@/components/ui/loader';
import { ModelTierConfig } from '@/components/model/ModelTierConfig';
import { ModelSelector } from '@/components/model/ModelSelector';
import { FallbackModelsPicker } from '@/components/model/FallbackModelsPicker';
import { useAllModels } from '@/hooks/useAllModels';
import { usePreferences } from '@/hooks/usePreferences';
import { useUpdatePreferences } from '@/hooks/useUpdatePreferences';
import { useTranslation } from 'react-i18next';
import { modelPrefs, splitPreferenceWrite } from '@/lib/modelPreferences';

// ---------------------------------------------------------------------------
// DefaultsStep — Step 5: Set default primary + flash models
// ---------------------------------------------------------------------------

export default function DefaultsStep() {
  const navigate = useNavigate();
  const { models, modelAccessMap, isLoading } = useAllModels();
  const { preferences } = usePreferences();
  const updatePreferences = useUpdatePreferences();
  const { t } = useTranslation();

  // ---------------------------------------------------------------------------
  // Selection state — seed from existing preferences if available
  // ---------------------------------------------------------------------------

  const modelPref = modelPrefs(preferences);

  const [primaryModel, setPrimaryModel] = useState<string>(
    () => modelPref.preferred_model ?? '',
  );
  const [flashModel, setFlashModel] = useState<string>(
    () => modelPref.preferred_flash_model ?? '',
  );
  const [advancedModels, setAdvancedModels] = useState<{
    compactionModel: string;
    fetchModel: string;
    fallbackModels: string[];
  }>({
    compactionModel: modelPref.compaction_model ?? '',
    fetchModel: modelPref.fetch_model ?? '',
    fallbackModels: modelPref.fallback_models ?? [],
  });
  const [advancedOpen, setAdvancedOpen] = useState(false);
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

      await updatePreferences.mutateAsync(splitPreferenceWrite({
        preferred_model: primaryModel,
        preferred_flash_model: flashModel,
        compaction_model: compaction,
        fetch_model: fetchModel,
        fallback_models: advancedModels.fallbackModels,
      }));

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
        <Loader size={20} className="text-[color:var(--color-text-tertiary)]" />
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
        modelAccess={modelAccessMap}
      />

      {/* Routing models. Folded away because a first run never needs them, and
          named with the same keys the Settings tab uses so one control does not
          read two ways. */}
      <div>
        <button
          type="button"
          onClick={() => setAdvancedOpen((v) => !v)}
          className="inline-flex items-center gap-1 text-xs font-medium transition-colors"
          style={{ color: 'var(--color-text-tertiary)' }}
          aria-expanded={advancedOpen}
        >
          <ChevronRight
            className={cn('h-3 w-3 transition-transform duration-200', advancedOpen && 'rotate-90')}
          />
          {t('settings.modelTuning.advanced')}
        </button>

        <AnimatePresence initial={false}>
          {advancedOpen && (
            <motion.div
              initial={{ height: 0, opacity: 0 }}
              animate={{ height: 'auto', opacity: 1 }}
              exit={{ height: 0, opacity: 0 }}
              transition={{ duration: 0.2, ease: 'easeInOut' }}
              className="overflow-hidden"
            >
              <div className="flex flex-col pt-4" style={{ gap: '24px' }}>
                <ModelSelector
                  label={t('settings.modelTuning.fetchModel')}
                  description={t('settings.modelTuning.fetchModelDesc')}
                  value={advancedModels.fetchModel}
                  onChange={(v) => handleAdvancedChange({ fetchModel: v })}
                  models={models}
                  placeholder={t('settings.modelTuning.defaultsToFlash')}
                  modelAccess={modelAccessMap}
                />
                <ModelSelector
                  label={t('settings.modelTuning.compactionModel')}
                  description={t('settings.modelTuning.compactionModelDesc')}
                  value={advancedModels.compactionModel}
                  onChange={(v) => handleAdvancedChange({ compactionModel: v })}
                  models={models}
                  placeholder={t('settings.modelTuning.defaultsToFlash')}
                  modelAccess={modelAccessMap}
                />
                <FallbackModelsPicker
                  selected={advancedModels.fallbackModels}
                  onChange={(list) => handleAdvancedChange({ fallbackModels: list })}
                  models={models}
                />
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </div>

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
              <span aria-hidden="true" className="mr-1.5 flex-shrink-0">
                <Loader size={16} className="text-current" />
              </span>
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
