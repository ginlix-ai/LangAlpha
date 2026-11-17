/**
 * The desktop app's first run on a Mac: where should the user's agents work,
 * this Mac or the cloud.
 *
 * Asked once per account and Mac, and only while no computer of the account
 * is this Mac; with nothing to ask (this Mac is one already, or the server
 * cannot take it) the page lets the app through at once. A server read that
 * failed is no such answer: the page offers to try again, or to go on
 * without asking, since Home's warm-up would build a cloud computer for a
 * user who never chose one. "Use the cloud"
 * saves the answer and changes nothing else; with Home on another machine of
 * the user's own, the way out keeps Home there instead. "Work on this Mac"
 * pairs it by remote control, through the same disclosure and consent as the
 * Computers dialog, offers the folder (and taking it over from another
 * computer), and says that backup is what lets the Mac hold Home, which the
 * user may refuse. Backed up, the Mac becomes primary: for a user whose Home
 * is elsewhere (a cloud computer, or another machine of theirs) the create
 * asks the server to move Home to it once the Mac's first start is through,
 * which the question says up front.
 */
import { useEffect, useId, useState, type ReactNode } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { CheckCircle2 } from 'lucide-react';

import PageLoading from '@/components/PageLoading/PageLoading';
import { Loader } from '@/components/ui/loader';
import { WizardFooter, WizardShell, WizardStepHeader, type WizardFooterProps } from '@/components/ui/wizard';
import { queryKeys } from '@/lib/queryKeys';
import type { Computer, ComputerChoice } from '@/types/api';

import { updateCurrentUser } from '@/pages/Dashboard/utils/api';
import { providerIsOwnMachine } from '@/pages/ChatAgent/components/providerUi';
import { CreateRefusal, createRefusalMessage } from '@/pages/ChatAgent/components/addComputer';
import { useAddThisMac } from '@/pages/ChatAgent/components/addThisMac';
import { RemoteControlSetup, remoteControlActionLabel } from '@/pages/ChatAgent/components/remoteControlSetup';
import { useComputerActions } from '@/pages/ChatAgent/hooks/useComputerActions';
import { useComputers } from '@/pages/ChatAgent/hooks/useComputers';
import { useThisMacAvailability } from '@/pages/ChatAgent/hooks/useRemoteControl';
import { folderMarkedOwner } from '@/pages/ChatAgent/utils/sshLimit';

import { markAskedOnThisMac } from './computerChoiceGate';

type Step = 'ask' | 'setup' | 'finishing' | 'done';
const ORDER: Step[] = ['ask', 'setup', 'finishing', 'done'];

/** Where Home is, which says what working on this Mac does to it. */
type HomeNow = { kind: 'none' } | { kind: 'cloud' } | { kind: 'machine'; name: string };

/**
 * Where Home is going: it is on this Mac, moving here (the server moves it
 * once the Mac's first start is through), or staying where it is, since a
 * Mac without backup can't hold it.
 */
type Outcome = 'primary' | 'moving' | 'notPrimary';

const secondaryStyle = { color: 'var(--color-text-secondary)' } as const;
const warningStyle = { color: 'var(--color-warning)' } as const;
const dangerStyle = { color: 'var(--color-icon-danger)' } as const;

export default function ThisMacFirstRun({ onDone }: { onDone: () => void }) {
  const { t } = useTranslation();
  // The page's own copy is a namespace of its own, fetched with its chunk
  // rather than riding the first-load catalog.
  const { t: tRun } = useTranslation('thisMacFirstRun');
  const queryClient = useQueryClient();
  const headingId = useId();

  // Whether there is anything to ask: this Mac can be added here, and no
  // computer is it yet. Latched once asked, since adding it ends that.
  const availability = useThisMacAvailability({ enabled: true, poll: false });
  const askable = availability.settled && availability.offered && availability.added === false;
  const [asking, setAsking] = useState(false);
  if (askable && !asking) setAsking(true);
  const nothingToAsk = availability.settled && !askable && !asking;
  // A read that failed is no answer, and asks again next time.
  const answeredNothing = nothingToAsk && (availability.added === true || (availability.known && !availability.offered));
  const [goOn, setGoOn] = useState(false);
  const unchecked = nothingToAsk && availability.serverFailed && !goOn;
  useEffect(() => {
    if (!nothingToAsk || unchecked) return;
    if (answeredNothing) markAskedOnThisMac();
    onDone();
  }, [nothingToAsk, unchecked, answeredNothing, onDone]);

  const { data: list } = useComputers({ enabled: asking });
  const primary = list?.computers.find((c) => c.is_primary && c.status !== 'deleted') ?? null;
  // Home is on the primary, so a user with one has a Home that moves here.
  const homeNow: HomeNow = !primary
    ? { kind: 'none' }
    : providerIsOwnMachine(primary.kind) ? { kind: 'machine', name: primary.name } : { kind: 'cloud' };

  const [step, setStep] = useState<Step>('ask');
  const addThisMac = useAddThisMac(step === 'setup');
  const mac = addThisMac.setup;
  const { create } = useComputerActions();
  const [saveError, setSaveError] = useState(false);
  const [savingCloud, setSavingCloud] = useState(false);
  // Read when the user picked this Mac, before adding it can make it primary.
  const [homeFrom, setHomeFrom] = useState<HomeNow>({ kind: 'none' });
  const [added, setAdded] = useState<Computer | null>(null);
  const [outcome, setOutcome] = useState<Outcome | null>(null);

  const saveChoice = async (choice: ComputerChoice) => {
    await updateCurrentUser({ computer_choice: choice });
    void queryClient.invalidateQueries({ queryKey: queryKeys.user.me() });
  };

  // Home on another machine of the user's own stays there, so the answer
  // is that they work on their own machines, not on the cloud.
  const chooseElsewhere = () => {
    setSaveError(false);
    setSavingCloud(true);
    saveChoice(homeNow.kind === 'machine' ? 'own_machine' : 'cloud').then(
      () => {
        markAskedOnThisMac();
        onDone();
      },
      () => {
        setSaveError(true);
        setSavingCloud(false);
      },
    );
  };

  const begin = () => {
    setHomeFrom(homeNow);
    create.reset();
    setStep('setup');
  };

  // Backed up, this Mac is where the user said they work, and Home goes to
  // it: the first computer is primary as it is created, and Home elsewhere
  // moves here once the Mac's first start is through, which the create asked
  // the server to do. Not backed up, it cannot hold Home, so Home stays where
  // it is (or goes to a cloud computer) and the saved answer says so. The
  // answer only decides while the account has no primary; either way this
  // Mac is now a computer, so a save that failed asks nothing again.
  const finish = async (row: Computer) => {
    setAdded(row);
    setStep('finishing');
    markAskedOnThisMac();
    const backedUp = row.files_backup !== false;
    await saveChoice(backedUp || homeFrom.kind === 'machine' ? 'own_machine' : 'cloud').catch(() => {});
    setOutcome(!backedUp ? 'notPrimary' : homeFrom.kind === 'none' ? 'primary' : 'moving');
    setStep('done');
  };

  const submitCreate = (takeOver: string | null = null) => {
    const body = addThisMac.body({ takeOver });
    if (!body || create.isPending) return;
    const movesHome = body.files_backup !== false && homeFrom.kind !== 'none';
    create.mutate(movesHome ? { ...body, make_primary: true } : body, { onSuccess: (row) => void finish(row) });
  };

  if (unchecked) {
    return (
      <WizardShell
        testId="this-mac-first-run"
        stepKey="unchecked"
        stepIndex={0}
        progress={null}
        footer={
          <WizardFooter
            skip={{ label: tRun('continueAnyway'), onClick: () => setGoOn(true) }}
            primary={{ label: tRun('retry'), onClick: availability.retryServer, busy: availability.retrying }}
          />
        }
      >
        <WizardStepHeader titleId={headingId} title={tRun('title')} helper={tRun('checkFailed')} />
      </WizardShell>
    );
  }
  if (!asking) return <PageLoading />;

  const busy = create.isPending;
  const takeOver = folderMarkedOwner(create.error);
  const createError = create.error ? createRefusalMessage(create.error, t, create.variables?.resource_tier) : null;
  const backupOff = mac.ready && !mac.readyStep.filesBackup;

  let body: ReactNode;
  let footer: WizardFooterProps;
  switch (step) {
    case 'ask':
      body = (
        <>
          <WizardStepHeader
            titleId={headingId}
            title={tRun('title')}
            helper={askText(tRun, homeNow)}
          />
          {saveError && (
            <p role="alert" className="text-sm" style={dangerStyle}>
              {tRun('saveFailed')}
            </p>
          )}
        </>
      );
      footer = {
        skip: { label: elsewhereLabel(tRun, homeNow), onClick: chooseElsewhere },
        primary: { label: tRun('workOnThisMac'), onClick: begin, disabled: savingCloud },
      };
      break;
    case 'setup':
      body = (
        <>
          <WizardStepHeader titleId={headingId} title={tRun('setupTitle')} helper={tRun('setupHelper')} />
          <RemoteControlSetup state={mac} account={availability.status?.account ?? ''} busy={busy} />
          {mac.ready && (
            <div className="mt-4 flex flex-col gap-2 text-sm" data-testid="this-mac-backup-notice">
              <p style={secondaryStyle}>{tRun('backupNotice')}</p>
              {backupOff && (
                <p role="status" style={warningStyle} data-testid="this-mac-backup-off">
                  {backupOffText(tRun, homeFrom)}
                </p>
              )}
            </div>
          )}
          {createError && !busy && (
            <CreateRefusal
              className="mt-4"
              message={createError}
              onTakeOver={takeOver ? () => submitCreate(takeOver) : undefined}
            />
          )}
        </>
      );
      footer = {
        back: busy ? null : { label: tRun('back'), onClick: () => setStep('ask') },
        primary: mac.action
          ? { label: remoteControlActionLabel(t, mac.action), onClick: mac.start, disabled: !mac.acknowledged }
          : mac.ready
            ? { label: tRun('addThisMac'), onClick: () => submitCreate(), disabled: !mac.create, busy }
            : null,
      };
      break;
    case 'finishing':
      body = (
        <>
          <WizardStepHeader titleId={headingId} title={tRun('finishingTitle')} />
          <div role="status" aria-live="polite" className="flex items-center gap-2 text-sm" style={secondaryStyle}>
            <Loader size={16} />
          </div>
        </>
      );
      footer = {};
      break;
    case 'done':
      body = (
        <>
          <CheckCircle2 aria-hidden="true" className="mb-4 h-12 w-12" style={{ color: 'var(--color-success)' }} />
          <WizardStepHeader titleId={headingId} title={tRun('doneTitle')} helper={outcomeText(tRun, outcome, homeFrom)} />
          {added && (
            <div
              className="w-full overflow-hidden rounded-lg border"
              style={{ borderColor: 'var(--color-border-default)', background: 'var(--color-bg-surface)' }}
            >
              <SummaryRow label={tRun('folderLabel')} value={added.root_dir} />
              <SummaryRow label={tRun('computerLabel')} value={added.name} />
            </div>
          )}
        </>
      );
      footer = { primary: { label: tRun('getStarted'), onClick: onDone } };
      break;
  }

  return (
    <WizardShell
      testId="this-mac-first-run"
      stepKey={step}
      stepIndex={ORDER.indexOf(step)}
      progress={null}
      footer={<WizardFooter {...footer} />}
    >
      {body}
    </WizardShell>
  );
}

type Translate = ReturnType<typeof useTranslation>['t'];

function askText(t: Translate, home: HomeNow): string {
  if (home.kind === 'cloud') return t('movesHome');
  if (home.kind === 'machine') return t('movesHomeFromMachine', { name: home.name });
  return t('intro');
}

function elsewhereLabel(t: Translate, home: HomeNow): string {
  if (home.kind === 'cloud') return t('keepCloud');
  if (home.kind === 'machine') return t('keepMachine', { name: home.name });
  return t('useCloud');
}

function backupOffText(t: Translate, home: HomeNow): string {
  if (home.kind === 'cloud') return t('backupOffCloud');
  if (home.kind === 'machine') return t('backupOffMachine', { name: home.name });
  return t('backupOffNew');
}

function outcomeText(t: Translate, outcome: Outcome | null, home: HomeNow): string {
  switch (outcome) {
    case 'moving':
      return home.kind === 'machine'
        ? t('doneMovingFromMachine', { name: home.name })
        : t('doneMoving');
    case 'notPrimary':
      if (home.kind === 'cloud') return t('doneNotPrimaryCloud');
      if (home.kind === 'machine') return t('doneNotPrimaryMachine', { name: home.name });
      return t('doneNotPrimaryNew');
    default:
      return t('donePrimary');
  }
}

function SummaryRow({ label, value }: { label: string; value: string }) {
  return (
    <div
      className="flex items-center justify-between gap-4 border-b px-4 py-3 last:border-b-0"
      style={{ borderColor: 'var(--color-border-default)' }}
    >
      <span className="text-sm" style={secondaryStyle}>{label}</span>
      <span className="truncate text-right font-mono text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
        {value}
      </span>
    </div>
  );
}
