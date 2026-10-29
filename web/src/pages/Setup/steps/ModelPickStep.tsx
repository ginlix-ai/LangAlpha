import { useCallback, useEffect, useId } from 'react';
import { useNavigate, useLocation } from 'react-router';
import { KeyRound } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { Button } from '@/components/ui/button';
import { WizardStepHeader } from '@/components/ui/wizard';
import { ModelPickerList } from '@/components/modelAccess/ModelPicker';
import { useModelPicker } from '@/components/modelAccess/useModelPicker';
import { SubscriptionSummary } from '@/components/modelAccess/SubscriptionSummary';
import { useConfiguredProviders } from '@/hooks/useConfiguredProviders';
import { isSubscriptionProvider } from '@/api/oauth';
import { SetupFooter } from '../SetupFooter';
import type { LocationState } from './connectStep/shared';

// ---------------------------------------------------------------------------
// ModelPickStep — Step 4: which of the provider's models go on the user's
// list. The list and its save are the shared picker Settings uses too; the
// configured list persists as `starred_models`, which DefaultsStep and the
// Settings quick-picker read.
// ---------------------------------------------------------------------------

export default function ModelPickStep() {
  const navigate = useNavigate();
  const location = useLocation();
  const { t } = useTranslation();
  const headingId = useId();

  const state = (location.state as LocationState | null) ?? {};

  // Redirect to method step if essential state is missing (e.g. browser refresh)
  useEffect(() => {
    if (!state.provider) navigate('/setup/method', { replace: true });
  }, [state.provider, navigate]);

  const provider = state.provider ?? '';
  const displayName = state.displayName ?? provider;
  const modelsFrom = state.modelsFrom ?? provider;
  const isSubscription = isSubscriptionProvider(provider);

  const { configuredSet } = useConfiguredProviders();
  const isConfigured = provider ? configuredSet.has(provider) : false;
  const picker = useModelPicker({ provider, modelsFrom, justConnected: state.justConnected === true });

  const handleBack = useCallback(() => {
    navigate('/setup/provider', { state: { list: state.list } });
  }, [navigate, state.list]);

  const handleNext = async () => {
    if (await picker.save()) navigate('/setup/defaults');
  };

  return (
    <div>
      <WizardStepHeader
        titleId={headingId}
        title={t('setup.manageModels', { provider: displayName })}
        helper={t('setup.chooseModels', { provider: displayName })}
      />
      <div className="flex flex-col gap-4 sm:gap-6">
        {isSubscription && <SubscriptionSummary provider={provider} />}

        {/* A side path back to the key, kept quiet above the list it affects */}
        {!isSubscription && isConfigured && (
          <button
            type="button"
            onClick={() => navigate('/setup/connect', { state })}
            className="flex items-center gap-1 self-start text-xs font-medium transition-colors hover:opacity-80"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            <KeyRound aria-hidden="true" className="h-3 w-3" />
            {t('setup.updateApiKey')}
          </button>
        )}

        <ModelPickerList picker={picker} labelledBy={headingId} />

        {/* Add another provider */}
        <div
          className="flex items-center justify-between gap-3 rounded-lg p-4"
          style={{ background: 'var(--color-bg-card)', border: '1px solid var(--color-border-muted)' }}
        >
          <div className="flex flex-col gap-0.5">
            <span className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
              {t('setup.addAnotherProvider')}
            </span>
            <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
              {t('setup.addAnotherDescription')}
            </span>
          </div>
          <Button variant="outline" size="sm" className="shrink-0" onClick={() => navigate('/setup/method')}>
            {t('setup.addAnother')}
          </Button>
        </div>
      </div>

      <SetupFooter
        back={{ label: t('setup.back'), onClick: handleBack }}
        primary={{
          label: picker.saving ? t('setup.saving') : t('setup.continue'),
          onClick: handleNext,
          disabled: picker.loading,
          busy: picker.saving,
        }}
      />
    </div>
  );
}
