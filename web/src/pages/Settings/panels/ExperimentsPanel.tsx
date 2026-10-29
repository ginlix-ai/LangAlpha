import { AlertTriangle } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { useFeatures, useSetFeatureOverride } from '@/hooks/useFeatures';
import { SettingsCardBody, SettingsGroup, ToggleRow } from '../components/SettingsLayout';
import { SETTINGS } from '../sections';
import { useSaveFailureToast } from './usePreferenceWrite';

/** Experiments: user-overridable feature flags, data-driven from the
 *  feature-flag API, so a new flag needs no change here. */
export function ExperimentsPanel() {
  const reportFailure = useSaveFailureToast();
  const { data: featuresData, isLoading: isFeaturesLoading } = useFeatures();
  const setFeatureOverrideMutation = useSetFeatureOverride();
  const { t } = useTranslation();

  const overridable = (featuresData ?? []).filter((f) => f.gate === 'opt_in' || f.gate === 'opt_out');
  // Wait out the features query before committing to the empty state, so it
  // never flashes before the real list lands.
  if (overridable.length === 0 && isFeaturesLoading) return null;

  return (
    <SettingsGroup title={t('settings.experimentsGroup')} description={t(SETTINGS.experiments.descKey)}>
      {overridable.length === 0 ? (
        <SettingsCardBody id={SETTINGS.experiments.anchor} className="py-6 text-center">
          <p className="text-[0.8125rem]" style={{ color: 'var(--color-text-tertiary)' }}>
            {t('settings.experimentsEmpty')}
          </p>
        </SettingsCardBody>
      ) : (
        overridable.map((feature, index) => (
          <ToggleRow
            key={feature.key}
            id={index === 0 ? SETTINGS.experiments.anchor : undefined}
            label={feature.label}
            description={feature.tradeoffs ? (
              <>
                {feature.description}
                <span className="mt-1.5 flex items-start gap-1.5">
                  <AlertTriangle className="h-3.5 w-3.5 mt-px shrink-0" style={{ color: 'var(--color-warning)' }} />
                  <span>
                    <span className="font-medium" style={{ color: 'var(--color-warning)' }}>
                      {t('settings.experimentTradeoff')}:
                    </span>{' '}
                    {feature.tradeoffs}
                  </span>
                </span>
              </>
            ) : feature.description}
            checked={feature.enabled}
            onChange={() => { void reportFailure(setFeatureOverrideMutation.mutateAsync({ key: feature.key, enabled: !feature.enabled })); }}
          />
        ))
      )}
    </SettingsGroup>
  );
}
