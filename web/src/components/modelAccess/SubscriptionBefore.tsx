/* eslint-disable react-refresh/only-export-components -- the panel and the manifest list it shows */
import { useId, useMemo } from 'react';
import { Shield } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { CheckboxChoice } from '@/components/ui/choice';
import { useAllModels } from '@/hooks/useAllModels';
import { modelLabel } from '@/lib/modelLabel';
import { subscriptionKey, type SubscriptionProvider } from '@/api/oauth';
import { isPlatformMode } from '@/config/hostMode';
import { FactRow, ModelChips } from './parts';

/** The manifest's models for a subscription, by display name: what connecting
 *  is expected to add, until the plan's own list says. */
export function useManifestModelNames(provider: string): string[] {
  const { rawModels, metadata } = useAllModels();
  return useMemo(() => {
    const names: string[] = [];
    for (const group of Object.values(rawModels)) {
      for (const m of group.models ?? []) {
        if (metadata[m]?.provider === provider && !metadata[m]?.is_custom_model) names.push(modelLabel(m, metadata));
      }
    }
    return [...new Set(names)];
  }, [rawModels, metadata, provider]);
}

/**
 * What a subscription sign-in needs, gives and costs, read before it starts,
 * with the consent the start waits on. One panel for every subscription so
 * any one can be removed without touching the others.
 */
export function SubscriptionBefore({
  provider,
  agreed,
  onAgreedChange,
}: {
  provider: SubscriptionProvider;
  agreed: boolean;
  onAgreedChange: (agreed: boolean) => void;
}) {
  const { t } = useTranslation();
  const consentId = useId();
  const models = useManifestModelNames(provider);
  const ns = `modelAccess.${subscriptionKey(provider)}`;

  return (
    <div className="flex flex-col gap-4">
      <div
        className="flex flex-col gap-4 rounded-lg p-4"
        style={{ background: 'var(--color-bg-card)', border: '1px solid var(--color-border-muted)' }}
      >
        <FactRow label={t('modelAccess.before.need')}>{t(`${ns}.need`)}</FactRow>
        <FactRow label={t('modelAccess.before.get')}>
          <div className="flex flex-col gap-1.5">
            <ModelChips names={models} emptyText={t('modelAccess.before.getUnknown')} />
            <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
              {t('modelAccess.before.getNote')}
            </span>
          </div>
        </FactRow>
        <FactRow label={t('modelAccess.before.usage')}>{t(isPlatformMode ? `${ns}.usage` : `${ns}.usageOss`)}</FactRow>
        <FactRow label={t('modelAccess.before.time')}>{t(`${ns}.time`)}</FactRow>
      </div>

      <p className="flex items-start gap-1.5 text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
        <Shield aria-hidden="true" className="mt-0.5 h-3.5 w-3.5 shrink-0" />
        <span>{t('modelAccess.before.stored')}</span>
      </p>

      <CheckboxChoice
        size="md"
        checked={agreed}
        onCheckedChange={onAgreedChange}
        aria-labelledby={consentId}
        className="items-start gap-2.5"
      >
        <span id={consentId} className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>
          {t(`${ns}.consent`)}
        </span>
      </CheckboxChoice>
    </div>
  );
}
