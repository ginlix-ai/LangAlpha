import { useTranslation } from 'react-i18next';
import { SUBSCRIPTIONS, type SubscriptionProvider } from '@/api/oauth';
import { CLAUDE_CODE, CLAUDE_CODE_CONNECTION } from '@/lib/claudeCode';
import { ProviderRow } from './KeyProviderPicker';
import { ProviderChoice } from './ProviderChoices';
import type { RowStatus } from './parts';
import type { ProviderGroup } from './providerOptions';

/**
 * Every subscription the account can connect: the plans it signs in to, then
 * the coding plans it adds with a key. Settings and the setup wizard list them
 * the same way. Settings also lists Claude Code under the Claude sign-in
 * (`claudeCode`), with where it stands, whenever the server offers it.
 */
export function SubscriptionChoices({
  planGroups,
  configured,
  onPickSignIn,
  onPickPlan,
  claudeCode,
}: {
  planGroups: readonly ProviderGroup[];
  configured: ReadonlySet<string>;
  onPickSignIn: (provider: SubscriptionProvider) => void;
  onPickPlan: (group: ProviderGroup) => void;
  claudeCode?: { status: RowStatus; onPick: () => void };
}) {
  const { t } = useTranslation();
  return (
    <div className="flex flex-col gap-3">
      <section className="flex flex-col gap-1">
        <h4 className="px-3 text-xs font-medium" style={{ color: 'var(--color-text-tertiary)' }}>
          {t('modelAccess.signInPlans')}
        </h4>
        <ul className="flex flex-col gap-1">
          {SUBSCRIPTIONS.flatMap((s) => [
            <ProviderChoice
              key={s.provider}
              markProvider={s.provider}
              name={t(`modelAccess.${s.key}.name`)}
              note={t(`modelAccess.${s.key}.pickNote`)}
              connected={configured.has(s.provider)}
              onPick={() => onPickSignIn(s.provider)}
            />,
            ...(claudeCode && s.provider === 'claude-oauth' ? [(
              <ProviderChoice
                key={CLAUDE_CODE}
                markProvider={CLAUDE_CODE}
                name={CLAUDE_CODE_CONNECTION.display_name}
                note={t('modelAccess.claudeCode.setup.pickNote')}
                connected={claudeCode.status === 'connected'}
                status={claudeCode.status === 'connected' ? undefined : claudeCode.status}
                onPick={claudeCode.onPick}
              />
            )] : []),
          ])}
        </ul>
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
