import { useCallback, useId } from 'react';
import { useNavigate } from 'react-router';
import { useTranslation } from 'react-i18next';
import { WizardStepHeader } from '@/components/ui/wizard';
import { DeviceConnect } from '@/components/modelAccess/DeviceConnect';
import { isSubscriptionProvider, subscriptionKey } from '@/api/oauth';
import { ClaudeConnect } from '@/components/modelAccess/ClaudeConnect';
import { useProviderConnected, type LocationState } from './shared';
import { SetupFooter } from '../../SetupFooter';

/** A subscription sign-in inside the wizard: the shared flow, then on to the
 *  model step (or back to the page that sent the user here). */
export function SubscriptionConnect({ state }: { state: LocationState }) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const connected = useProviderConnected();
  const headingId = useId();
  const provider = state.provider && isSubscriptionProvider(state.provider) ? state.provider : 'codex-oauth';
  const key = subscriptionKey(provider);
  const displayName = state.displayName ?? provider;
  const done = useCallback(() => {
    connected({ method: 'oauth', list: 'oauth', provider, displayName, modelsFrom: state.modelsFrom ?? provider, justConnected: true });
  }, [connected, provider, displayName, state.modelsFrom]);

  return (
    <div>
      <WizardStepHeader
        titleId={headingId}
        title={t('setup.connectTitle', { provider: t(`modelAccess.${key}.name`) })}
        helper={t(`modelAccess.${key}.intro`)}
      />
      {provider === 'claude-oauth'
        ? <ClaudeConnect onConnected={done} />
        : <DeviceConnect key={provider} provider={provider} onConnected={done} />}
      <SetupFooter
        back={{ label: t('setup.back'), onClick: () => navigate('/setup/provider', { state: { list: 'oauth' } }) }}
        primary={null}
      />
    </div>
  );
}

