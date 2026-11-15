import { useEffect, useId, useRef, useState } from 'react';
import { useLocation, useNavigate } from 'react-router';
import { useTranslation } from 'react-i18next';
import { WizardStepHeader } from '@/components/ui/wizard';
import { ONBOARDING_USE_URL, PLATFORM_URL } from '@/config/supportLinks';
import { useOnboardingProgress } from '@/hooks/useOnboardingProgress';
import { useUser } from '@/hooks/useUser';
import type { OnboardingProgressPatch, OnboardingRunsOn } from '@/api/onboarding';
import { canAddThisMac, openConsolePage } from '@/lib/desktop';
import { useThisMacAvailability } from '@/pages/ChatAgent/hooks/useRemoteControl';
import { SetupFooter } from '../SetupFooter';
import { useAnswerFlow } from '../answers/flow';
import { StepError, StepKicker, StepLoadFailed, StepLoading } from '../answers/parts';
import { markAskedOnThisMac } from '@/pages/ThisMac/computerChoiceGate';
import { afterThisMac, safeThen, type ThisMacOutcome } from './macRoute';
import { ThisMacPairingBody } from './ThisMacPairing';
import { pairingLead, pairingTitleKey, useThisMacPairing } from './useThisMacPairing';

const T = 'setup.thisMac';

/**
 * /setup/this-mac, in the Mac app: "Use this Mac with LangAlpha?", asked
 * once after sign-in and from the computer step. Where pairing cannot apply
 * (a browser, a shell without Remote control, a server that cannot reach a
 * Mac, a Mac that already is a computer, setup finished) the answer is
 * recorded as unavailable and the page moves straight on, so the hosted
 * onboarding can send everyone here. `?then=` is where a user with no answers goes next.
 * Keyed by the account: what it was asked and answered here is its own.
 */
export default function ThisMacStep() {
  const { user } = useUser();
  return <ThisMacPage key={user?.user_id ?? ''} />;
}

function ThisMacPage() {
  const navigate = useNavigate();
  const location = useLocation();
  const flow = useAnswerFlow('computer');
  const { update } = useOnboardingProgress();
  const shell = canAddThisMac();
  const availability = useThisMacAvailability({ enabled: shell, poll: false });
  const [saveFailed, setSaveFailed] = useState(false);
  const [leaving, setLeaving] = useState(false);

  const progress = flow.progress;
  const setupCompleted = !!progress?.setup_completed_at;
  const known = !!progress && !flow.isLoading;
  const applies = shell && !setupCompleted && availability.offered && availability.added === false;
  // A server read that failed leaves whether this Mac can pair, or already
  // is a computer, unknown: no answer is recorded on it, the page offers to
  // read it again. The shell's own channel failing is no answer either, and
  // the page moves on without one.
  const serverFailed = shell && availability.settled && availability.serverFailed;
  const shellFailed = shell && availability.settled && !availability.known && !serverFailed;
  // Once asked, the prompt stays: pairing here is what makes this Mac a
  // computer, which must not then read as a Mac that already was one.
  const [prompted, setPrompted] = useState(false);
  if (known && applies && !prompted) setPrompted(true);
  const unavailable = known && !prompted && !serverFailed && (!shell || setupCompleted || shellFailed || (availability.settled && !applies));

  const go = async (outcome: ThisMacOutcome, patch: OnboardingProgressPatch) => {
    setSaveFailed(false);
    setLeaving(true);
    try {
      if (Object.keys(patch).length) await update(patch);
    } catch {
      setSaveFailed(true);
      setLeaving(false);
      return;
    }
    // Asked here, or this Mac already is a computer: the desktop app's own
    // first run has nothing left to ask on this Mac.
    if (outcome !== 'unavailable' || availability.added === true) markAskedOnThisMac();
    const next = afterThisMac({
      outcome,
      answers: flow.answers,
      setupOn: progress?.setup_on ?? null,
      setupCompleted,
      then: safeThen(new URLSearchParams(location.search).get('then'), window.location.origin, PLATFORM_URL),
      platformUse: ONBOARDING_USE_URL,
      ownComputer: flow.ownComputers.length > 0,
    });
    if (next.kind === 'finishComputer') {
      if (!(await flow.finish())) setLeaving(false);
    } else if (next.kind === 'external') {
      openConsolePage(next.to, { inPlace: true });
    } else {
      navigate(next.to, { replace: outcome === 'unavailable' });
    }
  };

  // Skipped once; a refused write lets it be tried again from the page.
  const skipped = useRef(false);
  const skip = () => {
    skipped.current = true;
    void go('unavailable', progress?.this_mac || shellFailed ? {} : { this_mac: 'unavailable' });
  };
  useEffect(() => {
    if (!unavailable || skipped.current) return;
    skip();
  });

  if (!prompted) {
    if (flow.progressFailed && !progress) return <StepLoadFailed onRetry={flow.retryProgress} />;
    if (serverFailed) return <StepLoadFailed onRetry={availability.retryServer} />;
    if (saveFailed) return <StepLoadFailed saving onRetry={skip} />;
    return <StepLoading />;
  }
  return (
    <ThisMacPrompt
      runsOn={progress?.runs_on}
      account={availability.status?.account ?? ''}
      busy={leaving}
      saveFailed={saveFailed || flow.finishFailed}
      onAnswer={(outcome) => void go(outcome, outcome === 'paired'
        ? { this_mac: 'paired', ...(progress?.runs_on ? {} : { runs_on: 'own_computer' }) }
        : { this_mac: 'declined' })}
      onPaired={() => void update({ this_mac: 'paired', ...(progress?.runs_on ? {} : { runs_on: 'own_computer' }) }).catch(() => {})}
    />
  );
}

function ThisMacPrompt({ runsOn, account, busy, saveFailed, onAnswer, onPaired }: {
  runsOn: OnboardingRunsOn | null | undefined;
  account: string;
  busy: boolean;
  saveFailed: boolean;
  onAnswer: (outcome: 'paired' | 'declined') => void;
  /** Recorded as soon as this Mac is a computer, so closing the app then still keeps it. */
  onPaired: () => void;
}) {
  const { t } = useTranslation();
  const headingId = useId();
  // Not now stops this attempt making this Mac a computer, even while the
  // answer is saved or refused; pairing again lets it go on.
  const [declined, setDeclined] = useState(false);
  const pairing = useThisMacPairing({ runsOn, onPaired, held: declined });
  const { stage } = pairing;
  const lead = pairingLead(stage, runsOn, pairing.home);
  const notNow = { label: t(`${T}.notNow`), onClick: () => { setDeclined(true); onAnswer('declined'); } };
  const pair = () => { setDeclined(false); pairing.pair(); };
  const retry = () => { setDeclined(false); pairing.retry(); };

  return (
    <div>
      <StepKicker>{t(`${T}.kicker`)}</StepKicker>
      <WizardStepHeader titleId={headingId} title={t(pairingTitleKey(stage))} helper={lead ? t(lead.key, { name: lead.name }) : undefined} />
      <ThisMacPairingBody pairing={pairing} account={account} />
      {saveFailed && <StepError>{t('setup.answers.saveFailed')}</StepError>}
      <SetupFooter
        back={stage === 'ask' || stage === 'confirm' || stage === 'failed' ? notNow : null}
        primary={
          stage === 'ask' || stage === 'confirm'
            ? { label: t('setup.answers.computer.pairThisMac'), onClick: pair, disabled: stage === 'confirm', busy: stage === 'confirm' }
            : stage === 'failed'
              ? { label: t('computer.remoteControl.tryAgain'), onClick: retry }
              : { label: t('setup.continue'), onClick: () => onAnswer('paired'), disabled: stage !== 'paired' || busy, busy }
        }
      />
    </div>
  );
}
