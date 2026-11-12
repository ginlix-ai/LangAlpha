import { useTranslation } from 'react-i18next';
import { SUBSCRIPTIONS, type SubscriptionProvider } from '@/api/oauth';
import { useOAuthStatus } from '@/hooks/useConfiguredProviders';
import { CLAUDE_CODE, CLAUDE_CODE_CONNECTION, type ClaudeCodeChoice } from '@/lib/claudeCode';
import { ProviderRow } from './KeyProviderPicker';
import { ProviderChoice } from './ProviderChoices';
import type { ProviderGroup } from './providerOptions';

/**
 * Every subscription the account can connect: the plans it signs in to, then
 * the coding plans it adds with a key. Settings and the setup wizard list them
 * the same way, and both list Claude Code where the Claude sign-in is
 * (`claudeCode`), with where it stands, whenever the server offers it.
 * The Claude sign-in itself is listed once its status is read, unless the
 * server says the user may not start one (`can_sign_in`, which counts a
 * stored sign-in, one waiting to be signed in again included) and none is
 * connected. A status that could not be read lists it: the sign-in routes
 * refuse a user the gate leaves out, with words of their own. Once the
 * deployment set it an end day, it is marked legacy.
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
  claudeCode?: ClaudeCodeChoice;
}) {
  const { t } = useTranslation();
  const { data: claude, isError: claudeUnread } = useOAuthStatus('claude-oauth');
  const signInLifecycle = claude?.lifecycle ?? 'active';
  const offered = (provider: SubscriptionProvider) => provider !== 'claude-oauth'
    || configured.has(provider) || claudeUnread || (!!claude && claude.can_sign_in !== false);
  return (
    <div className="flex flex-col gap-3">
      <section className="flex flex-col gap-1">
        <h4 className="px-3 text-xs font-medium" style={{ color: 'var(--color-text-tertiary)' }}>
          {t('modelAccess.signInPlans')}
        </h4>
        <ul className="flex flex-col gap-1">
          {SUBSCRIPTIONS.flatMap((s) => {
            const rows = [];
            if (offered(s.provider)) rows.push(
              <ProviderChoice
                key={s.provider}
                markProvider={s.provider}
                name={t(`modelAccess.${s.key}.name`)}
                tag={s.provider === 'claude-oauth' && signInLifecycle !== 'active' ? t('modelAccess.claude.legacy') : undefined}
                note={t(`modelAccess.${s.key}.pickNote`)}
                connected={configured.has(s.provider)}
                onPick={() => onPickSignIn(s.provider)}
              />,
            );
            if (claudeCode && s.provider === 'claude-oauth') rows.push(
              <ProviderChoice
                key={CLAUDE_CODE}
                markProvider={CLAUDE_CODE}
                name={CLAUDE_CODE_CONNECTION.display_name}
                note={t(claudeCode.note)}
                connected={claudeCode.status === 'connected'}
                status={claudeCode.status === 'connected' ? undefined : claudeCode.status}
                onPick={claudeCode.onPick}
              />,
            );
            return rows;
          })}
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
