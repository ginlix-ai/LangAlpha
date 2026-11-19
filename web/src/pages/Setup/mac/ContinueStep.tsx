import { useEffect, useId, useState } from 'react';
import { Navigate, useNavigate } from 'react-router';
import { useTranslation } from 'react-i18next';
import { WizardStepHeader } from '@/components/ui/wizard';
import { useOnboardingProgress } from '@/hooks/useOnboardingProgress';
import { canAddThisMac, isDesktopShell } from '@/lib/desktop';
import { useThisMacAvailability } from '@/pages/ChatAgent/hooks/useRemoteControl';
import { SetupFooter } from '../SetupFooter';
import { SetupNote, StepError, StepKicker, StepLoadFailed, StepLoading } from '../answers/parts';
import { useSetupFacts } from '../answers/useSetupFacts';
import { leftItems } from './macRoute';
import { ProgressList } from '../answers/ProgressSteps';

const T = 'setup.continueMac';

/**
 * /setup/continue, in the Mac app, for setup a browser started: "Continue
 * setup on this Mac?" with what is left. Continue here makes setup the
 * app's (`setup_on` app, and the browser tab says it moved); I'll finish
 * in the browser hands it back there and leaves the app at its home. A Mac
 * that could be paired and was never asked goes to the pair prompt first,
 * which comes back here.
 */
export default function ContinueStep() {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const headingId = useId();
  const setup = useSetupFacts();
  const { update, isUpdating } = useOnboardingProgress();
  const shell = canAddThisMac();
  const availability = useThisMacAvailability({ enabled: shell, poll: false });
  const [saveFailed, setSaveFailed] = useState(false);
  const progress = setup.progress;

  const unasked = shell && !!progress && progress.this_mac === null && availability.offered && availability.added === false;
  useEffect(() => {
    if (unasked) navigate('/setup/this-mac', { replace: true });
  }, [unasked, navigate]);

  // Only the Mac app takes setup over: a browser here (the pair prompt's
  // way on, or the address typed) goes where setup resumes instead.
  if (!isDesktopShell()) return <Navigate to="/setup" replace />;
  if (setup.progressFailed && !progress) return <StepLoadFailed onRetry={setup.retryProgress} />;
  if (shell && availability.settled && availability.serverFailed) return <StepLoadFailed onRetry={availability.retryServer} />;
  if (setup.factsFailed) return <StepLoadFailed onRetry={setup.retryFacts} />;
  if (setup.isLoading || !setup.answers || !setup.facts || !setup.next || (shell && !availability.settled) || unasked) {
    return <StepLoading />;
  }

  const paired = progress?.this_mac === 'paired' || availability.added === true;
  const items = leftItems(setup.rail, setup.next, setup.answers, paired);
  const choose = async (setupOn: 'app' | 'browser') => {
    setSaveFailed(false);
    try {
      await update({ setup_on: setupOn });
    } catch {
      setSaveFailed(true);
      return;
    }
    navigate(setupOn === 'app' ? setup.nextPath ?? '/setup' : '/', { replace: true });
  };

  return (
    <div>
      <StepKicker>{t(paired ? 'setup.thisMac.paired.title' : `${T}.kickerNotPaired`)}</StepKicker>
      <WizardStepHeader titleId={headingId} title={t(`${T}.title`)} helper={t(paired ? `${T}.leadPaired` : `${T}.leadNotPaired`)} />
      <ProgressList testId="continue-left">
        {items.map((item, i) => (
          <li key={item} className="flex items-center gap-3 px-4 py-3 text-sm" style={{ borderColor: 'var(--color-border-muted)', color: 'var(--color-text-primary)' }}>
            <span
              aria-hidden="true"
              className="flex h-7 w-7 shrink-0 items-center justify-center rounded-md border text-xs"
              style={{ borderColor: 'var(--color-border-muted)', color: 'var(--color-text-tertiary)' }}
            >
              {i + 1}
            </span>
            {t(`${T}.left.${item}`)}
          </li>
        ))}
      </ProgressList>
      <SetupNote>{t(`${T}.note`)}</SetupNote>
      {saveFailed && <StepError>{t('setup.answers.saveFailed')}</StepError>}
      <SetupFooter
        back={{ label: t(`${T}.inBrowser`), onClick: () => void choose('browser'), disabled: isUpdating }}
        primary={{ label: t(`${T}.here`), onClick: () => void choose('app'), disabled: isUpdating, busy: isUpdating }}
      />
    </div>
  );
}
