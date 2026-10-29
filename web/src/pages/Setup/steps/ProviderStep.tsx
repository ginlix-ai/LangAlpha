import { useState, useCallback, useId, useMemo } from 'react';
import { useNavigate, useLocation } from 'react-router';
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
import { NO_FILTER, startingOption, type KeyFilter, type ProviderGroup } from '@/components/modelAccess/providerOptions';
import { useProviderConnected, type LocationState, type ProviderList } from './connectStep/shared';

// ---------------------------------------------------------------------------
// ProviderStep — Step 2: the provider, as Settings lists them
// ---------------------------------------------------------------------------

export default function ProviderStep() {
  const navigate = useNavigate();
  const connected = useProviderConnected();
  const location = useLocation();
  const { t } = useTranslation();
  const headingId = useId();

  const list: ProviderList = (location.state as LocationState | null)?.list ?? 'api_key';

  const { keyGroups, planGroups, localEntries, isLoading } = useProviderCatalog();
  const { preferences } = usePreferences();
  const { providers } = useConfiguredProviders();
  // What already works. A subscription that has to sign in again runs no turn,
  // so choosing it goes through its sign-in like a new one.
  const configuredSet = useMemo(
    () => new Set(providers.filter((p) => p.state !== 'needs_reauth').map((p) => p.provider)),
    [providers],
  );
  const [filter, setFilter] = useState<KeyFilter>(NO_FILTER);

  // The user's own providers, which the key method also offers.
  const userCustomProviders = useMemo(() => {
    if (!preferences) return [];
    const cp = modelPrefs(preferences).custom_providers;
    return Array.isArray(cp) ? cp : [];
  }, [preferences]);

  const handleBack = useCallback(() => navigate('/setup/method'), [navigate]);

  // Already connected: back to the page that sent the user here, or straight
  // on to its models. Otherwise on to its key or sign-in, which reads the
  // rest of what it needs from the catalog by the provider's name.
  const goOn = useCallback((state: LocationState & { provider: string }) => {
    if (configuredSet.has(state.provider)) connected(state);
    else navigate('/setup/connect', { state });
  }, [configuredSet, connected, navigate]);

  // The key step opens on the region a key is saved for, and offers the
  // rest. A coding plan's key form is reached from the subscriptions, which
  // its Back returns to.
  const pickKeyGroup = (from: ProviderList) => (group: ProviderGroup) => {
    const option = startingOption(group, configuredSet);
    goOn({ method: 'api_key', list: from, provider: option.provider, displayName: group.name, modelsFrom: option.models_from });
  };

  // A definition saved before its key is not a connection: without a key it
  // goes on to model management without consuming the return, which then
  // happens from the Done step. Settings lists only connections, so its models
  // have never been picked.
  const pickCustom = useCallback((name: string) => {
    const customState = { method: list, list, provider: name, displayName: name, modelsFrom: name };
    if (configuredSet.has(name)) connected(customState);
    else navigate('/setup/models', { state: { ...customState, justConnected: true } });
  }, [list, configuredSet, connected, navigate]);

  const addCustom = useCallback(() => {
    navigate('/setup/connect', { state: { method: list, list, isCustom: true } });
  }, [navigate, list]);

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
      className="self-start text-xs underline underline-offset-2 hover:opacity-80"
      style={{ color: 'var(--color-text-secondary)' }}
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
            planGroups={planGroups}
            configured={configuredSet}
            onPickSignIn={(p) => goOn({ method: 'oauth', list: 'oauth', provider: p, displayName: t(`modelAccess.${subscriptionKey(p)}.name`), modelsFrom: p })}
            onPickPlan={pickKeyGroup('oauth')}
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
