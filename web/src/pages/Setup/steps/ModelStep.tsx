import { useEffect, useId, useMemo, useState } from 'react';
import { Navigate, useLocation, useNavigate } from 'react-router';
import { useTranslation } from 'react-i18next';
import { KeyRound } from 'lucide-react';
import { WizardStepHeader } from '@/components/ui/wizard';
import { SubscriptionChoices } from '@/components/modelAccess/SubscriptionChoices';
import { ClaudeCodeTokenSheet } from '@/components/modelAccess/ClaudeCodeToken';
import { AccessRow } from '@/components/modelAccess/AccessRow';
import { Group } from '@/components/modelAccess/parts';
import { isPlatformMode } from '@/config/hostMode';
import { useConfiguredProviders } from '@/hooks/useConfiguredProviders';
import { useAccount, useRule } from '@/hooks/useAccount';
import { allows, RULE } from '@/lib/accountRules';
import { OwnAccessNote } from '@/components/organization/OwnAccessNote';
import { useProviderCatalog } from '@/hooks/useProviderCatalog';
import { useClaudeCodeSetup } from '@/hooks/useClaudeCode';
import { useClaudeCodeCustody } from '@/hooks/useClaudeCodeCustody';
import { subscriptionKey } from '@/api/oauth';
import { CLAUDE_CODE } from '@/lib/claudeCode';
import { useTurnOnClaudeCode } from '@/hooks/useTurnOnClaudeCode';
import { cn } from '@/lib/utils';
import { SetupFooter } from '../SetupFooter';
import { useAnswerFlow, useChangeAnswers } from '../answers/flow';
import { useSaveConnectedDefault } from '../answers/connectedDefault';
import { AnswersStrip, RowButton, SetupNote, StepError, StepKicker, StepLoadFailed, StepLoading } from '../answers/parts';
import { QUIET_LINK, connectedProviders, useProviderPick } from './connectStep/shared';

const S = 'modelAccess.claudeCode.setup';

/**
 * The model the answers call for: Claude Code's setup token, for the own
 * computer it runs on or saved for the cloud computer; or a subscription or
 * key of the user's own. The deployment's own models need nothing here.
 */
export default function ModelStep() {
  const flow = useAnswerFlow('model');
  const step = useClaudeCodeStep(flow);
  const { t } = useTranslation();
  if (flow.isLoading) return <StepLoading />;
  if (!flow.answers) return <Navigate to="/setup" replace />;
  if (flow.answers.models === 'langalpha_models') return <Navigate to="/setup/default" replace />;
  if (flow.answers.models !== 'claude_code') return <SubscriptionStep flow={flow} />;
  switch (step.kind) {
    case 'loading':
      return <StepLoading />;
    case 'failed':
      return <StepLoadFailed onRetry={step.retry} />;
    case 'unavailable':
      return (
        <AnswersDeadEnd
          flow={flow}
          testId="setup-claude-code-unavailable"
          title={t('setup.answers.model.claudeCodeUnavailableTitle')}
          helper={t('setup.answers.model.claudeCodeUnavailableLead')}
        />
      );
    case 'refused':
      return (
        <AnswersDeadEnd
          flow={flow}
          testId="setup-token-refused"
          title={t('setup.answers.model.tokenTitle')}
          helper={t(step.helper, step.values)}
        />
      );
    case 'open':
      return <ClaudeCodeTokenStep flow={flow} askConsent={step.askConsent} />;
  }
}

type Flow = ReturnType<typeof useAnswerFlow>;

type ClaudeCodeStepState =
  /** Reading whether Claude Code is offered, turning it on, or reading how it runs on the cloud computer. */
  | { kind: 'loading' }
  /** Not offered to this account: no token could run. */
  | { kind: 'unavailable' }
  /** A read or the turn-on failed, which `retry` tries again. */
  | { kind: 'failed'; retry: () => void }
  /** The token would be stored for the cloud computer, where no turn could
   *  run on it: storing is not open to this account, or this server runs
   *  Claude Code on no cloud computer (`helper` says which). */
  | { kind: 'refused'; helper: string; values?: Record<string, string> }
  /** The sheet, asking first for the user's agreement to the token being stored (`askConsent`). */
  | { kind: 'open'; askConsent: boolean };

/**
 * Where the Claude Code answer's step stands. The token saved for the cloud
 * computer waits until Claude Code is on and the choice of how it runs there
 * is read, so no sheet shows before it is known whether storing the token
 * needs the user's agreement. A server that offers no choice runs the token
 * it keeps when self-hosted, and stores none without it when hosted.
 */
function useClaudeCodeStep(flow: Flow): ClaudeCodeStepState {
  const answered = flow.answers?.models === 'claude_code';
  const saved = flow.tokenTarget?.kind === 'saved';
  const { setup, isLoading, unread, retry: retrySetup } = useClaudeCodeSetup({ enabled: answered });
  // A rule against the user's own subscriptions keeps Claude Code off too:
  // it is neither turned on nor given a token.
  const subscriptions = useRule(RULE.ownSubscriptions);
  const turnOn = useTurnOnClaudeCode(answered && subscriptions.allowed, setup);
  const { custody, isLoading: custodyLoading, unread: custodyUnread, retry } = useClaudeCodeCustody({ enabled: answered && saved });
  if (isLoading) return { kind: 'loading' };
  // A failed read is not "not offered", nor a reason to ask for a token
  // whose sheet may not save: it is read again first.
  if (unread) return { kind: 'failed', retry: retrySetup };
  if (!setup) return { kind: 'unavailable' };
  if (!subscriptions.allowed) return { kind: 'refused', helper: 'organization.modelAccess.subscriptionsOff', values: { who: subscriptions.who.start } };
  if (turnOn.failed) return { kind: 'failed', retry: turnOn.retry };
  // The server takes no token while Claude Code is off for the account.
  if (turnOn.pending) return { kind: 'loading' };
  if (!saved) return { kind: 'open', askConsent: false };
  if (custodyLoading) return { kind: 'loading' };
  if (custodyUnread) return { kind: 'failed', retry };
  if (!custody) return isPlatformMode ? { kind: 'refused', helper: BLOCKED } : { kind: 'open', askConsent: false };
  if (custody.refusal === 'needs_own_computer') return { kind: 'refused', helper: 'modelAccess.claudeCode.hosted.paused' };
  if (custody.mode !== 'stored' && !custody.offered.includes('stored')) return { kind: 'refused', helper: BLOCKED };
  return { kind: 'open', askConsent: true };
}

const BLOCKED = 'modelAccess.claudeCode.hosted.blocked';

/** The answers lead where no token can be added: the step says why and leads
 *  back to the questions. */
function AnswersDeadEnd({ flow, testId, title, helper }: { flow: Flow; testId: string; title: string; helper: string }) {
  const { t } = useTranslation();
  const headingId = useId();
  const change = useChangeAnswers();
  return (
    <div data-testid={testId}>
      <StepKicker>Claude Code</StepKicker>
      <WizardStepHeader titleId={headingId} title={title} helper={helper} />
      <SetupFooter
        back={flow.back ? { label: t('setup.back'), onClick: flow.back } : null}
        primary={{ label: t('setup.answers.changeAnswers'), onClick: change }}
      />
    </div>
  );
}

/**
 * The setup token, added on the page: how to get one (signing in to Claude,
 * or pasting one made with the CLI), the check with Claude as it saves, and
 * the saved token, after which Continue goes on. Where storing the token for
 * the cloud computer needs the user's agreement (`askConsent`), the sheet
 * asks for it over the ways to add the token, and the lead leaves the terms
 * to say how it is kept.
 */
function ClaudeCodeTokenStep({ flow, askConsent }: { flow: Flow; askConsent: boolean }) {
  const { t } = useTranslation();
  const headingId = useId();
  const target = flow.tokenTarget;
  const defaults = useSaveConnectedDefault();
  const [replacing, setReplacing] = useState(false);
  const [saving, setSaving] = useState(false);

  // Tasks on an own computer need one connected first.
  if (!target) return <Navigate to="/setup/computer" replace />;

  const cloudDetail = t(isPlatformMode ? S + '.savedDetailCloud' : S + '.savedDetailServer');
  const hasToken = !!flow.facts?.claudeCodeToken && !replacing;
  const lead = target.kind === 'computer'
    ? t('setup.answers.model.tokenForComputer', { name: target.name })
    : askConsent && !hasToken ? cloudDetail
      : t(isPlatformMode ? 'setup.answers.model.tokenForCloud' : 'setup.answers.model.tokenForServer');

  // A token stored for the cloud earlier is left as it is: leaving it deletes
  // it and stops the turns on it, which only Settings does, on the user's
  // confirmation.
  const next = async () => {
    // Connecting is what picks the default; a refusal here, or models not
    // listed yet, is no reason to hold the user on this step, since the next
    // one picks it anyway.
    const model = defaults.modelFor(CLAUDE_CODE);
    if (model) await defaults.save(model);
    await flow.finish();
  };

  return (
    <div>
      {flow.first && flow.answers && <AnswersStrip answers={flow.answers} />}
      <StepKicker>Claude Code</StepKicker>
      <WizardStepHeader titleId={headingId} title={t('setup.answers.model.tokenTitle')} helper={lead} />
      {hasToken ? (
        <Group title={t('modelAccess.claudeCode.savedTag')}>
          <AccessRow
            testId="setup-token-saved"
            icon={<KeyRound />}
            name={t(S + '.savedName')}
            detail={target.kind === 'computer' ? t('modelAccess.claudeCode.computer.separate', { name: target.name }) : cloudDetail}
            wrapDetail
            status="connected"
            action={<RowButton onClick={() => setReplacing(true)}>{t('setup.answers.model.replaceToken')}</RowButton>}
          />
        </Group>
      ) : (
        <ClaudeCodeTokenSheet
          // A new destination (Home moved) starts the sheet over, so a token
          // pasted or a sign-in begun for one never lands on another.
          key={target.kind === 'computer' ? target.computerId : 'saved'}
          target={target}
          askConsent={askConsent}
          onSaved={() => setReplacing(false)}
          onSavingChange={setSaving}
        />
      )}
      {replacing && (
        <div className="mt-3">
          <RowButton onClick={() => setReplacing(false)} disabled={saving}>{t('common.cancel')}</RowButton>
        </div>
      )}
      <SetupNote>{t('setup.answers.model.planOutNote')}</SetupNote>
      {flow.finishFailed && <StepError>{t('setup.answers.saveFailed')}</StepError>}
      <SetupFooter
        back={flow.back ? { label: t('setup.back'), onClick: flow.back } : null}
        primary={{
          label: t('setup.continue'),
          onClick: () => void next(),
          disabled: !flow.facts?.claudeCodeToken || flow.finishing || defaults.saving,
          busy: flow.finishing || defaults.saving,
        }}
      />
    </div>
  );
}

function SubscriptionStep({ flow }: { flow: Flow }) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const location = useLocation();
  const headingId = useId();
  const { providers, byokEnabled } = useConfiguredProviders();
  const { planGroups } = useProviderCatalog();
  const defaults = useSaveConnectedDefault();
  const [defaultFailed, setDefaultFailed] = useState(false);
  const configured = useMemo(() => connectedProviders(providers, { answering: true, byokEnabled }), [providers, byokEnabled]);
  // As main's chooser goes on from a pick, a connection that runs as it is
  // coming back here (with `connected`) to be made the default.
  const { goOn, pickKeyGroup } = useProviderPick({ configured, answering: true });
  // Only the ways in the account's rules leave open: sign-ins under the
  // subscriptions rule, coding plans and keys under the keys rule.
  const { policy } = useAccount();
  const ownKeys = allows(policy, RULE.ownApiKeys);

  // Back from a connect page with what it connected: that becomes the
  // default once it is decided (`settled`), its models listed and its own
  // list read; one it cannot vouch for saves nothing, and the default step
  // picks. Continue waits for that, so the pick is never dropped.
  const justConnected = (location.state as { connected?: string } | null)?.connected ?? null;
  const connectedSettled = !!justConnected && defaults.settled(justConnected);
  const connectedPick = justConnected && connectedSettled ? defaults.modelFor(justConnected) : null;
  const { save } = defaults;
  useEffect(() => {
    if (!connectedSettled) return;
    navigate(location.pathname, { replace: true, state: null });
    if (connectedPick) void save(connectedPick).then((ok) => setDefaultFailed(!ok));
  }, [connectedSettled, connectedPick, save, navigate, location.pathname]);

  return (
    <div>
      {flow.first && flow.answers && <AnswersStrip answers={flow.answers} />}
      <WizardStepHeader titleId={headingId} title={t('setup.answers.model.subTitle')} helper={t('setup.answers.model.subLead')} />
      {/* The legacy Claude sign-in is left to Settings: Claude Code is an answer of its own. */}
      <SubscriptionChoices
        planGroups={ownKeys ? planGroups : []}
        configured={configured}
        claudeSignIn={false}
        signIns={allows(policy, RULE.ownSubscriptions)}
        onPickSignIn={(provider) => goOn({
          method: 'oauth', list: 'oauth', provider, displayName: t(`modelAccess.${subscriptionKey(provider)}.name`), modelsFrom: provider,
        })}
        onPickPlan={pickKeyGroup('oauth')}
      />
      {ownKeys && (
        <button
          type="button"
          data-testid="setup-sub-api-key"
          onClick={() => navigate('/setup/provider', { state: { list: 'api_key', answering: true } })}
          className={cn('tap-target mt-4 px-3', QUIET_LINK)}
        >
          {t('setup.answers.model.withApiKey')}
        </button>
      )}
      <OwnAccessNote variant="setup" />
      {defaultFailed && <StepError quiet>{t('setup.answers.model.saveDefaultFailed')}</StepError>}
      {flow.finishFailed && <StepError>{t('setup.answers.saveFailed')}</StepError>}
      <SetupFooter
        back={flow.back ? { label: t('setup.back'), onClick: flow.back } : null}
        primary={{
          label: t('setup.continue'),
          onClick: () => void flow.finish(),
          disabled: !flow.facts?.providerConnected || flow.finishing || defaults.saving || !!justConnected,
          busy: flow.finishing || defaults.saving || !!justConnected,
        }}
      />
    </div>
  );
}
