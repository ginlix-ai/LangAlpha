import { useId, useState } from 'react';
import { useNavigate } from 'react-router';
import { useTranslation } from 'react-i18next';
import { Cloud, KeyRound, Laptop, Server } from 'lucide-react';
import { WizardSection, WizardStepHeader } from '@/components/ui/wizard';
import { ExternalRedirect } from '@/components/ExternalRedirect';
import { ONBOARDING_USE_URL } from '@/config/supportLinks';
import { useClaudeCodeSetup } from '@/hooks/useClaudeCode';
import { useComputerProviders, useComputers } from '@/pages/ChatAgent/hooks/useComputers';
import { useProviderGate } from '@/pages/ChatAgent/hooks/useProviderGate';
import { isProviderLocked } from '@/pages/ChatAgent/components/providerUi';
import { useUser } from '@/hooks/useUser';
import { CLAUDE_CODE } from '@/lib/claudeCode';
import { llmProviderArt } from '@/lib/brandArt';
import { BrandMark } from '@/pages/ChatAgent/components/mcp/BrandMark';
import { SetupFooter } from '../SetupFooter';
import { AnswerOption, SetupNote, StepError, StepLoadFailed, StepLoading } from '../answers/parts';
import { homeMachine } from '../answers/useSetupFacts';
import { useOnboardingProgress } from '@/hooks/useOnboardingProgress';
import { readAnswers, type ModelsAnswer, type RunsOnAnswer } from '../answers/setupSteps';

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
  if (isLoading || userLoading || claudeCodeLoading || providers.isLoading || computers.isLoading) return <StepLoading />;

  return (
    <div>
      <WizardStepHeader titleId={headingId} title={t('setup.answers.use.title')} helper={t('setup.answers.use.lead')} />
      <WizardSection title={t('setup.answers.use.modelsLabel')} titleId={modelsId} className="mt-0">
        <div role="radiogroup" aria-labelledby={modelsId} className="flex flex-col gap-2">
          {claudeCode && (
            <AnswerOption
              name={modelsId}
              value="claude_code"
              checked={pickedModels === 'claude_code'}
              onSelect={setModels}
              mark={<BrandMark name="Claude Code" art={llmProviderArt(CLAUDE_CODE)} size="xs" />}
              title={t('setup.answers.use.claudeCode')}
              detail={t('setup.answers.use.claudeCodeDesc')}
            />
          )}
          <AnswerOption
            name={modelsId}
            value="subscription_or_key"
            checked={pickedModels === 'subscription_or_key'}
            onSelect={setModels}
            mark={<KeyRound aria-hidden="true" className="h-4 w-4" />}
            title={t('setup.answers.use.subscription')}
            detail={t('setup.answers.use.subscriptionDesc')}
          />
          {serverModels && (
            <AnswerOption
              name={modelsId}
              value="langalpha_models"
              checked={pickedModels === 'langalpha_models'}
              onSelect={setModels}
              mark={<Server aria-hidden="true" className="h-4 w-4" />}
              title={t('setup.answers.use.serverModels')}
              detail={t('setup.answers.use.serverModelsDesc')}
            />
          )}
        </div>
      </WizardSection>
      <WizardSection title={t('setup.answers.use.whereLabel')} titleId={whereId}>
        <div role="radiogroup" aria-labelledby={whereId} className="flex flex-col gap-2">
          <AnswerOption
            name={whereId}
            value="cloud"
            checked={pickedRunsOn === 'cloud'}
            onSelect={setRunsOn}
            mark={<Cloud aria-hidden="true" className="h-4 w-4" />}
            title={t('setup.answers.use.serverComputer')}
            detail={t('setup.answers.use.serverComputerDesc')}
          />
          {pickedRunsOn === 'cloud' && homeOnMac && <SetupNote>{t('setup.answers.use.homeStays')}</SetupNote>}
          {(ownOffered || savedRunsOn === 'own_computer') && (
            <AnswerOption
              name={whereId}
              value="own_computer"
              checked={pickedRunsOn === 'own_computer'}
              onSelect={setRunsOn}
              mark={<Laptop aria-hidden="true" className="h-4 w-4" />}
              title={t('setup.answers.use.own')}
              detail={t('setup.answers.use.ownDesc')}
            />
          )}
        </div>
      </WizardSection>
      {failed && <StepError>{t('setup.answers.use.saveFailed')}</StepError>}
      <SetupFooter
        primary={{
          label: t('setup.continue'),
          onClick: submit,
          disabled: isLoading || !pickedModels || !pickedRunsOn || isUpdating,
          busy: isUpdating,
        }}
      />
    </div>
  );
}
