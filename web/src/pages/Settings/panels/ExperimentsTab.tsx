import { AlertTriangle } from 'lucide-react';
import { ToggleSwitch } from '@/components/ui/switch';
import { useFeatures, useSetFeatureOverride } from '@/hooks/useFeatures';
import { useTranslation } from 'react-i18next';
import { useToast } from '@/components/ui/use-toast';

/** Experiments tab: user-overridable feature flags, data-driven from the
 * feature-flag API. */
export function ExperimentsTab() {
  const { toast } = useToast();
  const { data: featuresData, isLoading: isFeaturesLoading } = useFeatures();
  const setFeatureOverrideMutation = useSetFeatureOverride();
  const { t } = useTranslation();

  const handleFeatureToggle = async (key: string, nextEnabled: boolean) => {
    try {
      await setFeatureOverrideMutation.mutateAsync({ key, enabled: nextEnabled });
    } catch {
      toast({
        variant: 'destructive',
        title: t('common.error'),
        description: t('settings.failedToSaveSettings'),
      });
    }
  };

  return (
      <div className="space-y-5 max-w-2xl">
        <p className="text-sm leading-relaxed" style={{ color: 'var(--color-text-tertiary)' }}>
          {t('settings.experimentsIntro', 'Early-access features still in development. Turn one on to enable it for your account — you can switch it off at any time.')}
        </p>
        {/* Data-driven from the feature-flag API, so new user-overridable
            flags need zero frontend changes. */}
        {(() => {
          const overridable = (featuresData ?? []).filter((f) => f.gate === 'opt_in' || f.gate === 'opt_out');
          if (overridable.length === 0) {
            // Wait out the features query before committing to the empty
            // state, so it never flashes before the real list lands.
            return isFeaturesLoading ? null : (
              <p className="text-sm" style={{ color: 'var(--color-text-tertiary)', opacity: 0.7 }}>
                {t('settings.experimentsEmpty', 'No experiments are available right now.')}
              </p>
            );
          }
          return (
            <div className="space-y-3">
              {overridable.map((feature) => (
                <div
                  key={feature.key}
                  className="p-4 rounded-lg"
                  style={{ backgroundColor: 'var(--color-bg-card)', border: '1px solid var(--color-border-muted)' }}
                >
                  <div className="flex items-start justify-between gap-4">
                    <div className="space-y-1 min-w-0">
                      <label className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>{feature.label}</label>
                      <p className="text-sm leading-relaxed" style={{ color: 'var(--color-text-secondary)' }}>{feature.description}</p>
                    </div>
                    <ToggleSwitch
                      checked={feature.enabled}
                      onChange={() => handleFeatureToggle(feature.key, !feature.enabled)}
                      className="mt-0.5"
                      ariaLabel={feature.label}
                    />
                  </div>
                  {feature.tradeoffs && (
                    <div
                      className="mt-3 flex items-start gap-2 rounded-md px-3 py-2"
                      style={{ backgroundColor: 'var(--color-warning-soft)' }}
                    >
                      <AlertTriangle className="h-3.5 w-3.5 mt-0.5 flex-shrink-0" style={{ color: 'var(--color-warning)' }} />
                      <p className="text-xs leading-relaxed" style={{ color: 'var(--color-text-secondary)' }}>
                        <span className="font-medium" style={{ color: 'var(--color-warning)' }}>
                          {t('settings.experimentTradeoff', 'Trade-off')}
                        </span>
                        {' — '}
                        {feature.tradeoffs}
                      </p>
                    </div>
                  )}
                </div>
              ))}
            </div>
          );
        })()}
      </div>
  );
}
