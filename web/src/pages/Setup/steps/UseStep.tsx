import { useId, useState } from 'react';
import { useNavigate } from 'react-router';
import { useTranslation } from 'react-i18next';
import { Cloud, ExternalLink, Laptop, Server } from 'lucide-react';
import { WizardSection, WizardStepHeader } from '@/components/ui/wizard';
import { ExternalRedirect } from '@/components/ExternalRedirect';
import { DESKTOP_DOWNLOAD_URL, HOSTED_OR_SELF_HOSTED_URL, ONBOARDING_USE_URL } from '@/config/supportLinks';
import { useClaudeCodeSetup } from '@/hooks/useClaudeCode';
import { useComputerProviders, useComputers } from '@/pages/ChatAgent/hooks/useComputers';
import { useProviderGate } from '@/pages/ChatAgent/hooks/useProviderGate';
import { isProviderLocked } from '@/pages/ChatAgent/components/providerUi';
import { useUser } from '@/hooks/useUser';
import { CLAUDE_CODE } from '@/lib/claudeCode';
import { llmProviderArt } from '@/lib/providerArt';
import { BrandMark } from '@/pages/ChatAgent/components/mcp/BrandMark';
import { cn } from '@/lib/utils';
import { SetupFooter } from '../SetupFooter';
import { AnswerOption, AnswerTile, SetupNote, StepError, StepLoadFailed, StepLoading } from '../answers/parts';
import { homeMachine } from '../answers/useSetupFacts';
import { useOnboardingProgress } from '@/hooks/useOnboardingProgress';
import { readAnswers, type ModelsAnswer, type RunsOnAnswer } from '../answers/setupSteps';
import { useInDesktopApp } from '@/hooks/useServerSetup';

// The subscriptions and coding plans the subscription card takes, in the order
// people know them, and the three of them its tile holds, one a coding plan.
const MAKERS = ['openai', 'xai', 'moonshot', 'z-ai', 'minimax', 'opencode-go'] as const;
const STACKED = ['openai', 'xai', 'opencode-go'] as const;

/** Grey and quiet at rest, in their own colours on a hovered or picked card. */
const QUIET_MARK = 'opacity-55 grayscale transition-[opacity,filter] group-hover:opacity-100 group-hover:grayscale-0 group-has-checked:opacity-100 group-has-checked:grayscale-0';

/** Three makers' marks overlapping in the 40px a tile takes, so the titles still line up. */
function MarkStack() {
  const spots = ['left-0 top-0.5 z-10', 'left-[18px] top-0.5 z-10', 'left-[9px] top-[18px]'];
  return (
    <span className="relative block h-10 w-10">
      {STACKED.map((id, i) => (
        <span
          key={id}
          className={cn('absolute grid h-[22px] w-[22px] place-items-center rounded-full', spots[i])}
          style={{ background: 'var(--color-bg-card)', boxShadow: '0 0 0 1.5px var(--color-bg-card), inset 0 0 0 1px var(--color-border-default)' }}
        >
          <BrandMark name={id} art={llmProviderArt(id)} className={cn('h-[13px] w-[13px]', QUIET_MARK)} />
        </span>
      ))}
    </span>
  );
}

/** A link out of the app, from the note that offers the hosted edition. */
function PitchLink({ href, children }: { href: string; children: string }) {
  return (
    <a
      href={href}
      target="_blank"
      rel="noopener noreferrer"
      className="inline-flex items-center gap-1 font-medium text-(--color-text-primary) underline-offset-2 hover:underline"
    >
      {children}
      <ExternalLink aria-hidden="true" className="h-3 w-3" />
    </a>
  );
}

/** The subscription card's line as the makers' marks, then any key. The marks
 *  are pictures only, so the sentence they stand for is kept for a screen reader. */
function MarkRow({ label, more }: { label: string; more: string }) {
  return (
    <>
      <span aria-hidden="true" className="mt-1 flex flex-wrap items-center gap-x-3 gap-y-1">
        {MAKERS.map((id) => <BrandMark key={id} name={id} art={llmProviderArt(id)} className={cn('h-4 w-4', QUIET_MARK)} />)}
        <span className="text-[0.8125rem]" style={{ color: 'var(--color-text-secondary)' }}>{more}</span>
      </span>
      <span className="sr-only">{label}</span>
    </>
  );
}

/**
 * Self-hosted: the onboarding questions, asked here. Claude Code, the
 * server's own models and the user's own computer are offered only where it
 * serves them. On hosted this address goes to the hosted onboarding, which
 * asks them.
 */
export default function UseStep() {
  const { user } = useUser();
  // Picks made for one account are not another's.
  return ONBOARDING_USE_URL ? <ExternalRedirect to={ONBOARDING_USE_URL} /> : <UseForm key={user?.user_id ?? ''} />;
}

function UseForm() {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const headingId = useId();
  const modelsId = useId();
  const whereId = useId();
  const { user, isLoading: userLoading, isError: userFailed, refetch: refetchUser } = useUser();
  const { progress, isLoading, isError: progressFailed, refetch: refetchProgress, update, isUpdating } = useOnboardingProgress();
  const [failed, setFailed] = useState(false);
  const saved = readAnswers(progress);
  const [models, setModels] = useState<ModelsAnswer | null>(null);
  const [runsOn, setRunsOn] = useState<RunsOnAnswer | null>(null);
  const { setup: claudeCodeSetup, isLoading: claudeCodeLoading, unread: claudeCodeUnread, retry: retryClaudeCode } = useClaudeCodeSetup();
  const serverModels = user?.deployment_serves === true;
  const claudeCode = claudeCodeSetup !== null;
  const offered = (answer: ModelsAnswer) =>
    answer === 'langalpha_models' ? serverModels : answer === 'claude_code' ? claudeCode : true;
  const pickedModels = models ?? (saved && offered(saved.models) ? saved.models : null);
  // An own computer is reached over SSH, the Mac app's included.
  const providers = useComputerProviders({ enabled: true });
  const ownComputer = !!providers.data?.providers.some((p) => p.kind === 'ssh');
  // A new answer brings one only where it can be added: the plan offers SSH
  // and no rule keeps machines the user brings off. A saved answer stays.
  const gate = useProviderGate(null);
  const ownOffered = !!providers.data?.providers.some((p) => p.kind === 'ssh' && !isProviderLocked(p, gate));
  const savedRunsOn = saved?.runsOn === 'own_computer' && !ownComputer ? null : saved?.runsOn ?? null;
  // This Mac paired before the questions holds Home, which a cloud answer
  // leaves where it is.
  const computers = useComputers();
  // Said only of a Mac paired through the app (its helper), which is what
  // the line names; an SSH host holding Home gets no line.
  const homeOnMac = !!homeMachine(computers.data?.computers ?? [])?.ssh?.helper;
  // In the desktop app the server is this computer, so "keeps working when
  // your computer is off" would be untrue, whatever runs the agent.
  const { inDesktopApp, isLoading: desktopLoading } = useInDesktopApp();
  // With no own computer to offer, the server's is the only answer.
  const pickedRunsOn = runsOn ?? savedRunsOn ?? (providers.data && !ownOffered ? 'cloud' : null);

  const submit = () => {
    if (!pickedModels || !pickedRunsOn) return;
    const changed = pickedModels !== saved?.models || pickedRunsOn !== saved?.runsOn;
    // New answers start setup's steps over, finished or not; the same ones
    // keep its place.
    setFailed(false);
    update({ models: pickedModels, runs_on: pickedRunsOn, ...(changed ? { setup_step: null, setup_completed: null, plan_step_done: null } : {}) }).then(
      () => navigate('/setup', { replace: true }),
      () => setFailed(true),
    );
  };

  // The options are what the profile, Claude Code's setup and the computers
  // offer, and the saved answers decide whether setup starts over: none is
  // picked before all of them are read, and one never read is unknown.
  const userUnread = userFailed && !user;
  const progressUnread = progressFailed && !progress;
  const providersUnread = providers.isError && !providers.data;
  if (userUnread || progressUnread || providersUnread || claudeCodeUnread) {
    return (
      <StepLoadFailed
        onRetry={() => {
          if (userUnread) void refetchUser();
          if (progressUnread) void refetchProgress();
          if (providersUnread) void providers.refetch();
          if (claudeCodeUnread) retryClaudeCode();
        }}
      />
    );
  }
  if (isLoading || userLoading || claudeCodeLoading || providers.isLoading || computers.isLoading || desktopLoading) return <StepLoading />;

  return (
    <div>
      <WizardStepHeader titleId={headingId} title={t('setup:answers.use.title')} helper={t('setup:answers.use.lead')} />
      <WizardSection title={t('setup:answers.use.modelsLabel')} titleId={modelsId} className="mt-0">
        <div role="radiogroup" aria-labelledby={modelsId} className="flex flex-col gap-2">
          {claudeCode && (
            <AnswerOption
              name={modelsId}
              value="claude_code"
              checked={pickedModels === 'claude_code'}
              onSelect={setModels}
              mark={<AnswerTile><BrandMark name="Claude Code" art={llmProviderArt(CLAUDE_CODE)} className="h-5 w-5" /></AnswerTile>}
              title={t('setup:answers.use.claudeCode')}
              detail={t('setup:answers.use.claudeCodeDesc')}
            />
          )}
          <AnswerOption
            name={modelsId}
            value="subscription_or_key"
            checked={pickedModels === 'subscription_or_key'}
            onSelect={setModels}
            mark={<MarkStack />}
            title={t('setup:answers.use.subscription')}
            detail={<MarkRow label={t('setup:answers.use.subscriptionDesc')} more={t('setup:answers.use.anyKey')} />}
          />
          {serverModels && (
            <AnswerOption
              name={modelsId}
              value="langalpha_models"
              checked={pickedModels === 'langalpha_models'}
              onSelect={setModels}
              mark={<AnswerTile><Server aria-hidden="true" strokeWidth={1.5} /></AnswerTile>}
              title={t('setup:answers.use.serverModels')}
              detail={t('setup:answers.use.serverModelsDesc')}
            />
          )}
        </div>
      </WizardSection>
      <WizardSection title={t('setup:answers.use.whereLabel')} titleId={whereId}>
        <div role="radiogroup" aria-labelledby={whereId} className="flex flex-col gap-2">
          <AnswerOption
            name={whereId}
            value="cloud"
            checked={pickedRunsOn === 'cloud'}
            onSelect={setRunsOn}
            mark={<AnswerTile>{inDesktopApp ? <Laptop aria-hidden="true" strokeWidth={1.5} /> : <Cloud aria-hidden="true" strokeWidth={1.5} />}</AnswerTile>}
            title={t(inDesktopApp ? 'setup:answers.use.thisComputer' : 'setup:answers.use.serverComputer')}
            detail={t(inDesktopApp ? 'setup:answers.use.thisComputerDesc' : 'setup:answers.use.serverComputerDesc')}
          />
          {inDesktopApp && (
            <SetupNote icon={<Cloud />}>
              {t('setup:answers.use.hostedPitch')}
              <span className="mt-1.5 flex flex-wrap gap-x-4 gap-y-1">
                <PitchLink href={DESKTOP_DOWNLOAD_URL}>{t('setup:answers.use.hostedApp')}</PitchLink>
                <PitchLink href={HOSTED_OR_SELF_HOSTED_URL}>{t('setup:answers.use.hostedCompare')}</PitchLink>
              </span>
            </SetupNote>
          )}
          {pickedRunsOn === 'cloud' && homeOnMac && <SetupNote>{t('setup:answers.use.homeStays')}</SetupNote>}
          {(ownOffered || savedRunsOn === 'own_computer') && (
            <AnswerOption
              name={whereId}
              value="own_computer"
              checked={pickedRunsOn === 'own_computer'}
              onSelect={setRunsOn}
              mark={<AnswerTile><Laptop aria-hidden="true" strokeWidth={1.5} /></AnswerTile>}
              title={t('setup:answers.use.own')}
              detail={t('setup:answers.use.ownDesc')}
            />
          )}
        </div>
      </WizardSection>
      {failed && <StepError>{t('setup:answers.use.saveFailed')}</StepError>}
      <SetupFooter
        primary={{
          label: t('setup:continue'),
          onClick: submit,
          disabled: isLoading || !pickedModels || !pickedRunsOn || isUpdating,
          busy: isUpdating,
        }}
      />
    </div>
  );
}
