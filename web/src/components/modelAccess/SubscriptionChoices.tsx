import { useTranslation } from 'react-i18next';
import { SUBSCRIPTIONS, type SubscriptionProvider } from '@/api/oauth';
import { ProviderRow } from './KeyProviderPicker';
import { ProviderChoices } from './ProviderChoices';
import type { ProviderGroup } from './providerOptions';

/**
 * Every subscription the account can connect: the plans it signs in to, then
 * the coding plans it adds with a key. Settings and the setup wizard list them
 * the same way.
 */
export function SubscriptionChoices({
  planGroups,
  configured,
  onPickSignIn,
  onPickPlan,
}: {
  planGroups: readonly ProviderGroup[];
  configured: ReadonlySet<string>;
  onPickSignIn: (provider: SubscriptionProvider) => void;
  onPickPlan: (group: ProviderGroup) => void;
}) {
  const { t } = useTranslation();
  return (
    <div className="flex flex-col gap-3">
      <section className="flex flex-col gap-1">
        <h4 className="px-3 text-xs font-medium" style={{ color: 'var(--color-text-tertiary)' }}>
          {t('modelAccess.signInPlans')}
        </h4>
        <ProviderChoices
          entries={SUBSCRIPTIONS.map((s) => ({
            provider: s.provider,
            name: t(`modelAccess.${s.key}.name`),
            note: t(`modelAccess.${s.key}.pickNote`),
          }))}
          configured={configured}
          onPick={onPickSignIn}
        />
      </section>
      {planGroups.length > 0 && (
        <section className="flex flex-col gap-1">
          <h4 className="px-3 text-xs font-medium" style={{ color: 'var(--color-text-tertiary)' }}>
            {t('modelAccess.codingPlans')}
          </h4>
          <ul className="flex flex-col gap-1">
            {planGroups.map((g) => <ProviderRow key={g.group} g={g} configured={configured} onPick={onPickPlan} layout="list" />)}
          </ul>
        </section>
      )}
    </div>
  );
}
