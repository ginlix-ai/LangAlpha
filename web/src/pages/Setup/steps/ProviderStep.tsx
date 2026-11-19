import { useState, useCallback, useId, useMemo } from 'react';
import { Navigate, useNavigate, useLocation } from 'react-router';
import { useTranslation } from 'react-i18next';
import { Loader } from '@/components/ui/loader';
import { WizardStepHeader } from '@/components/ui/wizard';
import { SetupFooter } from '../SetupFooter';
import { useProviderCatalog } from '@/hooks/useProviderCatalog';
import { usePreferences } from '@/hooks/usePreferences';
import { useConfiguredProviders } from '@/hooks/useConfiguredProviders';
import { modelPrefs } from '@/lib/modelPreferences';
import { subscriptionKey } from '@/api/oauth';
import { KeyProviderList } from '@/components/modelAccess/KeyProviderPicker';
import { ProviderChoices } from '@/components/modelAccess/ProviderChoices';
import { SubscriptionChoices } from '@/components/modelAccess/SubscriptionChoices';
import { QuietNote } from '@/components/modelAccess/parts';
import { NO_FILTER, type KeyFilter } from '@/components/modelAccess/providerOptions';
import { useClaudeCodeSetup } from '@/hooks/useClaudeCode';
import { claudeCodeChoice } from '@/lib/claudeCode';
import { cn } from '@/lib/utils';
import { QUIET_LINK, connectedProviders, useProviderPick, type LocationState, type ProviderList } from './connectStep/shared';
import { useAccount } from '@/hooks/useAccount';
import { allows, RULE } from '@/lib/accountRules';

// ---------------------------------------------------------------------------
// ProviderStep — Step 2: the provider, as Settings lists them
// ---------------------------------------------------------------------------

export default function ProviderStep() {
  const navigate = useNavigate();
  const location = useLocation();
  const { t } = useTranslation();
  const headingId = useId();

  const list: ProviderList = (location.state as LocationState | null)?.list ?? 'api_key';
  const { policy } = useAccount();
  // Each credential under its own rule: on the subscriptions list, sign-ins
  // under the subscriptions rule and coding plans, added with a key, under
  // the keys rule; keys and local servers under the keys rule.
  const ownKeys = allows(policy, RULE.ownApiKeys);
  const ownSubscriptions = allows(policy, RULE.ownSubscriptions);
  // Sent here by the answers' model step, which Back returns to.
  const answering = (location.state as LocationState | null)?.answering === true;

  const { keyGroups, planGroups, localEntries, isLoading } = useProviderCatalog();
  const { preferences } = usePreferences();
  const { providers, byokEnabled } = useConfiguredProviders();
  const configuredSet = useMemo(
    () => connectedProviders(providers, { answering, byokEnabled }),
    [providers, answering, byokEnabled],
  );
  const { connected, goOn, pickKeyGroup } = useProviderPick({ configured: configuredSet, answering });
  const [filter, setFilter] = useState<KeyFilter>(NO_FILTER);
  const { setup: claudeCode, unread: claudeCodeUnread } = useClaudeCodeSetup({ enabled: list === 'oauth' && !answering });

  // The user's own providers, which the key method also offers.
  const userCustomProviders = useMemo(() => {
    if (!preferences) return [];
    const cp = modelPrefs(preferences).custom_providers;
    return Array.isArray(cp) ? cp : [];
  }, [preferences]);

  const handleBack = useCallback(() => navigate(answering ? '/setup/model' : '/setup/method'), [navigate, answering]);

  // A definition saved before its key is not a connection: without a key it
  // goes on to model management without consuming the return, which then
  // happens from the Done step. Settings lists only connections, so its models
  // have never been picked. From the answers it goes through its key first,
  // since their model step counts only a connection.
  const pickCustom = useCallback((name: string) => {
    const customState = { method: list, list, provider: name, displayName: name, modelsFrom: name };
    if (answering) goOn(customState);
    else if (configuredSet.has(name)) connected(customState);
    else navigate('/setup/models', { state: { ...customState, justConnected: true, answering } });
  }, [list, configuredSet, connected, goOn, navigate, answering]);

  const addCustom = useCallback(() => {
    navigate('/setup/connect', { state: { method: list, list, isCustom: true, answering } });
  }, [navigate, list, answering]);

  // A way a rule keeps off is not offered on the step before; a page reached
  // some other way goes back to it.
  if (!(list === 'oauth' ? ownSubscriptions || ownKeys : ownKeys)) return <Navigate to={answering ? '/setup/model' : '/setup/method'} replace />;

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-20">
        <Loader size={20} className="text-(--color-text-tertiary)" label={t('common.loading')} />
      </div>
    );
  }

  const customLink = (
    <button
      type="button"
      onClick={addCustom}
      className={cn('self-start', QUIET_LINK)}
    >
      {t('modelAccess.customProvider')}
    </button>
  );

  return (
    <div>
      <WizardStepHeader
        titleId={headingId}
        title={t('setup.providerTitle')}
        helper={
          list === 'oauth' ? t('setup.providerSubtitleOAuth')
            : list === 'local' ? t('setup.providerSubtitleLocal')
            : t('setup.providerSubtitleApiKey')
        }
      />
      <div className="flex flex-col gap-4">
        {list === 'oauth' && (
          <SubscriptionChoices
            planGroups={ownKeys ? planGroups : []}
            signIns={ownSubscriptions}
            configured={configuredSet}
            onPickSignIn={(p) => goOn({ method: 'oauth', list: 'oauth', provider: p, displayName: t(`modelAccess.${subscriptionKey(p)}.name`), modelsFrom: p })}
            onPickPlan={pickKeyGroup('oauth')}
            claudeSignIn={!answering}
            claudeCode={answering ? undefined : claudeCodeChoice(claudeCode, claudeCodeUnread, () => navigate('/setup/claude-code'))}
          />
        )}

        {list === 'api_key' && (
          <>
            <KeyProviderList
              groups={keyGroups}
              filter={filter}
              onFilterChange={setFilter}
              configured={configuredSet}
              onPick={pickKeyGroup('api_key')}
              compact
            />
            {userCustomProviders.length > 0 && (
              <section className="flex flex-col gap-1">
                <h4 className="px-3 text-xs font-medium" style={{ color: 'var(--color-text-tertiary)' }}>
                  {t('setup.yourProviders')}
                </h4>
                <ProviderChoices
                  entries={userCustomProviders.map((cp) => ({ provider: cp.name, name: cp.name }))}
                  configured={configuredSet}
                  onPick={pickCustom}
                />
              </section>
            )}
            {customLink}
          </>
        )}

        {list === 'local' && (
          <>
            {localEntries.length === 0 ? (
              <QuietNote>{t('setup.noProviders')}</QuietNote>
            ) : (
              <ProviderChoices
                entries={localEntries.map((e) => ({ provider: e.provider, name: e.display_name }))}
                configured={configuredSet}
                onPick={(p) => {
                  const entry = localEntries.find((e) => e.provider === p);
                  if (entry) goOn({ method: 'local', list: 'local', provider: p, displayName: entry.display_name, modelsFrom: entry.brand_key ?? p });
                }}
              />
            )}
            {customLink}
          </>
        )}
      </div>

      <SetupFooter back={{ label: t('setup.back'), onClick: handleBack }} primary={null} />
    </div>
  );
}
