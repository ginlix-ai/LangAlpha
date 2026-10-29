import { useTranslation } from 'react-i18next';
import { useOAuthStatus } from '@/hooks/useConfiguredProviders';
import { useAvailableModels } from '@/hooks/useProviderModels';
import { useAllModels } from '@/hooks/useAllModels';
import { modelLabel } from '@/lib/modelLabel';
import { subscriptionKey, type SubscriptionProvider } from '@/api/oauth';
import { planLabel } from './planLabel';
import { FactRow, ModelChips, StatusLabel } from './parts';

/** A connected subscription: the account, its plan by name, and the models it
 *  added, as the plan's own list has them. */
export function SubscriptionSummary({ provider }: { provider: SubscriptionProvider }) {
  const { t } = useTranslation();
  const key = subscriptionKey(provider);
  const status = useOAuthStatus(provider).data;
  const connected = status?.state === 'connected';
  const list = useAvailableModels(provider, { enabled: connected });
  const { metadata } = useAllModels();
  const plan = planLabel(provider, status?.plan_type);
  const account = status?.email || status?.account_id || null;

  return (
    <div
      className="flex flex-col gap-3 rounded-lg p-4"
      style={{ background: 'var(--color-bg-card)', border: '1px solid var(--color-border-muted)' }}
    >
      <div className="flex items-center justify-between gap-3">
        <span className="min-w-0 truncate text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
          {account ?? t(`modelAccess.${key}.name`)}
        </span>
        <StatusLabel status={status?.state === 'needs_reauth' ? 'needs_reauth' : connected ? 'connected' : 'unknown'} />
      </div>
      {plan && <FactRow label={t('modelAccess.summary.plan')}>{plan}</FactRow>}
      {connected && (
        <FactRow label={t('modelAccess.summary.models')}>
          {list.isLoading ? (
            <StatusLabel status="working" />
          ) : (
            <ModelChips
              names={(list.data?.ready ?? []).map((m) => modelLabel(m, metadata))}
              emptyText={t('modelAccess.summary.noModels')}
            />
          )}
        </FactRow>
      )}
    </div>
  );
}
