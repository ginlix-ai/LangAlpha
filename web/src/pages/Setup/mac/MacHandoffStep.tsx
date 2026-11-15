import { useEffect, useId, useState } from 'react';
import type { OnboardingSetupOn } from '@/api/onboarding';
import { useNavigate } from 'react-router';
import { useTranslation } from 'react-i18next';
import { ExternalLink, Laptop } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Loader } from '@/components/ui/loader';
import { WizardStepHeader } from '@/components/ui/wizard';
import { DESKTOP_DOWNLOAD_URL } from '@/config/supportLinks';
import { useOnboardingProgress } from '@/hooks/useOnboardingProgress';
import { useUser } from '@/hooks/useUser';
import { ArtFrame } from '@/pages/ChatAgent/components/machineArt/ArtFrame';
import { MachineArt } from '@/pages/ChatAgent/components/machineArt/MachineArt';
import { SetupFooter } from '../SetupFooter';
import { Group } from '@/components/modelAccess/parts';
import { AccessRow } from '@/components/modelAccess/AccessRow';
import { SetupNote, StepError, StepKicker, StepLoading } from '../answers/parts';
import { useSetupFacts } from '../answers/useSetupFacts';
import { ProgressList, ProgressStep } from '../answers/ProgressSteps';

const T = 'setup.mac';
/** How often the tab reads the record while the app has setup. */
const POLL_MS = 3000;

/**
 * /setup/mac, in a browser: setup continues in the Mac app, which pairs the
 * Mac. Get the app (then the tab waits, `setup_on` mac_pending), and once
 * the app says Continue here (`app`) the tab shows where setup went and
 * follows it to the first chat when it is done. Handed back to the browser,
 * the tab resumes setup here. The record is read every few seconds, since
 * the app is another device as far as this tab knows. The wait and what the
 * tab saw of the app are the account's, so another account signing in here
 * starts the page over.
 */
export default function MacHandoffStep() {
  const { user } = useUser();
  return <MacHandoff key={user?.user_id ?? ''} />;
}

function MacHandoff() {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const headingId = useId();
  const { user } = useUser();
  const setup = useSetupFacts({ refetchInterval: POLL_MS });
  const { update, isUpdating } = useOnboardingProgress();
  const [waiting, setWaiting] = useState(false);
  const [saveFailed, setSaveFailed] = useState(false);
  const progress = setup.progress;
  const setupOn = progress?.setup_on ?? null;

  // The app handing setup back ("I'll finish in the browser") is setup_on
  // moving to browser once it read as the Mac's (mac_pending, the tab's own
  // Get the Mac app included, or app): the app's continue prompt can hand it
  // straight back from mac_pending. Only the tab's own browser write is no
  // hand-back; "Continue here instead" says so with `continued`. Either way
  // the effect alone navigates, once, to where setup resumes.
  const [written, setWritten] = useState<OnboardingSetupOn | null>(null);
  const [sawApp, setSawApp] = useState(false);
  if (!sawApp && (setupOn === 'app' || setupOn === 'mac_pending')) setSawApp(true);
  const [continued, setContinued] = useState(false);
  const handedBack = continued || (sawApp && setupOn === 'browser' && written !== 'browser');
  const done = !!progress?.setup_completed_at;
  useEffect(() => {
    if (done) navigate('/chat', { replace: true });
    else if (handedBack && setup.nextPath) navigate(setup.nextPath, { replace: true });
  }, [done, handedBack, setup.nextPath, navigate]);

  const state = setupOn === 'app' ? 'moved' : waiting || setupOn === 'mac_pending' ? 'waiting' : 'get';
  const mac = [...setup.ownComputers]
    .filter((c) => c.ssh?.helper)
    .sort((a, b) => (b.created_at ?? '').localeCompare(a.created_at ?? ''))[0] ?? null;

  const write = async (patch: { setup_on: 'mac_pending' | 'browser' }) => {
    setSaveFailed(false);
    // Before the write, since a poll may see it land first; a refused one is
    // not this tab's, so a later move to browser stays the app's hand-back.
    const before = written;
    setWritten(patch.setup_on);
    try {
      await update(patch);
      return true;
    } catch {
      setWritten(before);
      setSaveFailed(true);
      return false;
    }
  };
  const getApp = () => {
    window.open(DESKTOP_DOWNLOAD_URL, '_blank', 'noopener,noreferrer');
    setWaiting(true);
    // A write refused leaves setup the browser's: the way to get the app stays.
    void write({ setup_on: 'mac_pending' }).then((ok) => { if (!ok) setWaiting(false); });
  };
  const continueHere = async () => {
    if (await write({ setup_on: 'browser' })) setContinued(true);
  };
  // Back to this tab's computer step takes setup back from the app first,
  // so the two never drive it at once; a refused write stays here and says so.
  const back = async () => {
    const macHas = setupOn === 'app' || setupOn === 'mac_pending' || written === 'mac_pending';
    if (macHas && !(await write({ setup_on: 'browser' }))) return;
    navigate('/setup/computer');
  };

  if (!progress) return <StepLoading />;

  return (
    <div data-testid={`mac-handoff-${state}`}>
      <StepKicker>{t(`${T}.kicker`)}</StepKicker>
      <WizardStepHeader
        titleId={headingId}
        title={t(`${T}.${state}.title`)}
        helper={state === 'moved'
          ? (mac ? t(`${T}.moved.lead`, { name: mac.name }) : t(`${T}.moved.leadUnnamed`))
          : t(`${T}.${state}.lead`)}
      />

      {state === 'get' && (
        <ProgressList>
          <ProgressStep n={1} state="active" title={t(`${T}.get.step1`)}>
            <Button type="button" size="sm" onClick={getApp} disabled={isUpdating} className="gap-1.5 self-start">
              {t(`${T}.get.download`)}
              <ExternalLink aria-hidden="true" className="h-3.5 w-3.5" />
            </Button>
          </ProgressStep>
          <ProgressStep n={2} state="todo" title={user?.email ? t(`${T}.get.step2`, { email: user.email }) : t(`${T}.get.step2Unnamed`)} />
          <ProgressStep n={3} state="todo" title={t(`${T}.get.step3`)} />
        </ProgressList>
      )}

      {state === 'waiting' && (
        <>
          <ArtFrame className="is-preview" tl={t(`${T}.waiting.artOs`)} tr={t(`${T}.waiting.artStatus`)}>
            <MachineArt art="macbook" state="prep" label={t(`${T}.waiting.artLabel`)} />
          </ArtFrame>
          <p role="status" className="mt-4 flex items-center gap-2 text-sm" style={{ color: 'var(--color-text-secondary)' }}>
            <Loader size={12} style={{ color: 'var(--color-accent-primary)' }} />
            {t(`${T}.waiting.live`)}
          </p>
        </>
      )}

      {state === 'moved' && (
        <>
          <Group>
            <AccessRow
              testId="mac-handoff-mac"
              icon={<Laptop />}
              name={mac?.name ?? t(`${T}.moved.yourMac`)}
              detail={t(`${T}.moved.open`)}
              wrapDetail
              status={mac ? 'paired' : undefined}
            />
          </Group>
          <SetupNote>{t(`${T}.moved.note`)}</SetupNote>
        </>
      )}

      {saveFailed && <StepError>{t('setup.answers.saveFailed')}</StepError>}
      <SetupFooter
        back={{ label: t('setup.back'), onClick: () => void back(), disabled: isUpdating }}
        skip={{ label: t(`${T}.continueHere`), onClick: () => void continueHere(), disabled: isUpdating }}
      />
    </div>
  );
}
