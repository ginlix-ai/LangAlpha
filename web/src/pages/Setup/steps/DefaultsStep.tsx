import { useState, useCallback, useId } from 'react';
import { useNavigate } from 'react-router';
import { motion, AnimatePresence } from '@/lib/framer';
import { ChevronRight, Info } from 'lucide-react';
import { cn } from '@/lib/utils';
import { WizardStepHeader } from '@/components/ui/wizard';
import { TWEEN } from '@/lib/motion';
import { SetupFooter } from '../SetupFooter';
import { Loader } from '@/components/ui/loader';
import { ModelTierConfig } from '@/components/model/ModelTierConfig';
import { ModelSelector } from '@/components/model/ModelSelector';
import { FallbackModelsPicker } from '@/components/model/FallbackModelsPicker';
import { useAllModels } from '@/hooks/useAllModels';
import { usePreferences } from '@/hooks/usePreferences';
import { useUpdatePreferences } from '@/hooks/useUpdatePreferences';
import { useTranslation } from 'react-i18next';
import { modelPrefs, splitPreferenceWrite, starredModels } from '@/lib/modelPreferences';
import { apiErrorDetail } from '@/api/errors';
import { suggestBackgroundModel } from './suggestBackgroundModel';
import { useModelSlotCopy } from '@/components/model/useModelSlotCopy';

// ---------------------------------------------------------------------------
// DefaultsStep — Step 5: Set default primary + Background models
// ---------------------------------------------------------------------------

/**
 * Setup sits outside the onboarding provider, so after a reload this step can
 * mount before the preferences have arrived. The slots are seeded once, from
 * what is saved, so the form mounts again when they arrive; until then nothing
 * is filled and Continue stays off, or it would write the first-run fills over
 * the saved defaults.
 */
export default function DefaultsStep() {
  const { isLoaded, isLoading } = usePreferences();
  return (
    <DefaultsForm key={isLoaded ? 'loaded' : 'unread'} prefsLoaded={isLoaded} prefsFailed={!isLoaded && !isLoading} />
  );
}

function DefaultsForm({ prefsLoaded, prefsFailed }: { prefsLoaded: boolean; prefsFailed: boolean }) {
  const navigate = useNavigate();
  const { models, metadata, payers, systemDefaults, isLoading } = useAllModels();
  const { preferences } = usePreferences();
  const updatePreferences = useUpdatePreferences();
  const { t } = useTranslation();
  const copy = useModelSlotCopy();
  const headingId = useId();

  // ---------------------------------------------------------------------------
  // Selection state — seed from existing preferences if available
  // ---------------------------------------------------------------------------

  const modelPref = modelPrefs(preferences);

  const [primaryModel, setPrimaryModel] = useState<string>(
    () => modelPref.preferred_model ?? '',
  );
  const [backgroundModel, setBackgroundModel] = useState<string>(
    () => modelPref.preferred_background_model ?? '',
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

  // A first run starts with a default model rather than an empty required
  // slot: the deployment's own when this user can run it, else the first model
  // they just picked. Set during render like the Background fill below.
  const available = Object.values(models).flatMap((p) => p.models ?? []);
  const primaryFill = !primaryModel && !isLoading && prefsLoaded
    ? [systemDefaults?.default_model, ...starredModels(preferences)].find(
        (m): m is string => typeof m === 'string' && available.includes(m),
      ) ?? available[0]
    : undefined;
  if (primaryFill) setPrimaryModel(primaryFill);

  // A first run fills both defaults, so an empty Background slot takes a model to
  // go with the primary, set during render so the slot never paints empty.
  // The fill is a pick like any other: it stays when the primary changes, and
  // nothing is saved before Continue.
  const backgroundFill = !backgroundModel && primaryModel && prefsLoaded ? suggestBackgroundModel(models, primaryModel) : undefined;
  if (backgroundFill) setBackgroundModel(backgroundFill);

  const canContinue = prefsLoaded && Boolean(primaryModel && backgroundModel);

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
    if (!canContinue) return;

    setSaving(true);
    setError(null);

    try {
      // Leave unset rather than pinning to today's Background pick: the server
      // resolves a blank fetch model from the Background model, and a
      // blank compaction model from the conversation's own, at turn time, so a
      // later change in Settings keeps following it.
      const compaction = advancedModels.compactionModel || null;
      const fetchModel = advancedModels.fetchModel || null;

      await updatePreferences.mutateAsync(splitPreferenceWrite({
        preferred_model: primaryModel,
        preferred_background_model: backgroundModel,
        compaction_model: compaction,
        fetch_model: fetchModel,
        fallback_models: advancedModels.fallbackModels,
      }));

      navigate('/setup/ready');
    } catch (e: unknown) {
      const detail = apiErrorDetail(e);
      setError(typeof detail === 'string' ? detail : (e instanceof Error && e.message) || t('setup.errorSavePrefs'));
    } finally {
      setSaving(false);
    }
  }, [canContinue, primaryModel, backgroundModel, advancedModels, updatePreferences, navigate, t]);

  // ---------------------------------------------------------------------------
  // Render
  // ---------------------------------------------------------------------------

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-20">
        <Loader size={20} className="text-(--color-text-tertiary)" label={t('common.loading')} />
      </div>
    );
  }

  // In platform mode, models are already tier-filtered by useAllModels.
  // In OSS mode, they're filtered by configured providers.
  // Either way, `models` is the correct set to display.

  return (
    <div>
      <WizardStepHeader titleId={headingId} title={t('setup.chooseYourModels')} helper={copy.chooseDesc} />
      <div className="flex flex-col gap-4 sm:gap-6">
      {/* Model access reminder */}
      <div
        className="flex items-start gap-2.5 rounded-lg px-3.5 py-3"
        // A neutral panel with the accent on its icon alone: amber annotates,
        // it does not wash or ring (DESIGN.md, accent discipline).
        style={{
          background: 'var(--color-bg-card)',
          border: '1px solid var(--color-border-muted)',
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
        backgroundModel={backgroundModel}
        onBackgroundModelChange={setBackgroundModel}
        showExplainer
        payers={payers}
        metadata={metadata}
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
            className={cn('h-3 w-3 transition-transform', advancedOpen && 'rotate-90')}
          />
          {t('settings.modelTuning.advanced')}
        </button>

        <AnimatePresence initial={false}>
          {advancedOpen && (
            // Opacity and a short lift, not height: the fold opens at once and
            // fades in, so nothing below it is animated through a layout pass.
            <motion.div
              initial={{ opacity: 0, y: -4 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, transition: TWEEN.exit }}
              transition={TWEEN.quick}
            >
              <div className="flex flex-col pt-4" style={{ gap: '24px' }}>
                <ModelSelector
                  label={t('settings.modelTuning.fetchModel')}
                  description={t('settings.modelTuning.fetchModelDesc')}
                  value={advancedModels.fetchModel}
                  onChange={(v) => handleAdvancedChange({ fetchModel: v })}
                  models={models}
                  placeholder={copy.defaultsToBackground}
                  payers={payers}
                  metadata={metadata}
                />
                <ModelSelector
                  label={t('settings.modelTuning.compactionModel')}
                  description={t('settings.modelTuning.compactionModelDesc')}
                  value={advancedModels.compactionModel}
                  onChange={(v) => handleAdvancedChange({ compactionModel: v })}
                  models={models}
                  placeholder={copy.defaultsToConversation}
                  payers={payers}
                  metadata={metadata}
                />
                <FallbackModelsPicker
                  selected={advancedModels.fallbackModels}
                  onChange={(list) => handleAdvancedChange({ fallbackModels: list })}
                  models={models}
                  metadata={metadata}
                  payers={payers}
                />
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </div>

      {/* A read that failed: nothing is filled, and nothing can be saved over it. */}
      {prefsFailed && (
        <p role="alert" className="text-sm" style={{ color: 'var(--color-loss)' }}>
          {t('settings.failedToLoadModels')}
        </p>
      )}

      {/* Error */}
      {error && (
        <p role="alert" className="text-sm" style={{ color: 'var(--color-loss)' }}>
          {error}
        </p>
      )}
      </div>

      <SetupFooter
        back={{ label: t('setup.back'), onClick: handleBack }}
        primary={{
          label: saving ? t('setup.saving') : t('setup.continue'),
          onClick: handleNext,
          disabled: !canContinue,
          busy: saving,
        }}
      />
    </div>
  );
}
