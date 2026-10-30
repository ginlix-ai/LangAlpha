import {
  Fragment,
  useEffect,
  useEffectEvent,
  useId,
  useRef,
  useState,
  type Dispatch,
  type SetStateAction,
} from 'react';
import { useNavigate, useSearchParams } from 'react-router';
import { useTranslation } from 'react-i18next';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { preloadRouteChunk } from '@/components/Main/Main';
import PageLoading from '@/components/PageLoading/PageLoading';
import { Button } from '@/components/ui/button';
import ChatInput, { type ChatInputHandle } from '@/components/ui/chat-input';
import type { ModelOptions, ReadyAttachment } from '@/components/ui/chat-input.types';
import { Loader } from '@/components/ui/loader';
import { toast } from '@/components/ui/use-toast';
import { WizardExit } from '@/components/ui/wizard';
import { homeWorkspaceQuery, resolveChatWorkspace } from '@/hooks/useHomeWorkspace';
import { waitForServerTools } from '@/hooks/useMcpServers';
import { useLocale } from '@/hooks/useLocale';
import { useKeepAccountLocale } from '@/hooks/useLocaleChoice';
import { useSyncUserLocale } from '@/hooks/useSyncUserLocale';
import { useUpdatePreferences } from '@/hooks/useUpdatePreferences';
import { useUser } from '@/hooks/useUser';
import { formatList } from '@/lib/format';
import { queryKeys } from '@/lib/queryKeys';
import { scrollMotion } from '@/lib/reducedMotion';
import { warmWorkspace } from '@/pages/ChatAgent/utils/warmWorkspace';
import { getPreferences } from '@/pages/Dashboard/utils/api';
import { useOnboarding } from '../OnboardingProvider';
import { AgentPending, AgentTurn, ConversationShell, TurnCard, UserTurn } from './conversation';
import {
  clearWelcomeDraft,
  readWelcomeDraft,
  writeWelcomeDraft,
  type ElsewhereAnswers,
  type WelcomeDraft,
} from './draft';
import { HANDOFF_DWELL_MS, handoffAnimates, runHandoff } from './handoff';
import { kickoffSend } from './kickoff';
import { marketBriefQuery } from './marketBrief';
import {
  WELCOME_EDIT_PARAM,
  WELCOME_STEP_PARAM,
  isWelcomeStep,
  openingStep,
  welcomeSteps,
  type WelcomeMode,
  type WelcomeStepId,
} from './paths';
import { answered, profilePatch, readProfile, resumeAnswers, unlistedSectors, type InvestingProfile } from './profile';
import { screenshotAttachment } from './screenshots';
import { brokerageNames, brokerageSummary, elsewhereSummary, questionSummary } from './summary';
import { useBrokers } from './useBrokers';
import { AgentStep } from './steps/AgentStep';
import { BrokerageStep } from './steps/BrokerageStep';
import { ExperienceStep } from './steps/ExperienceStep';
import { MarketsStep } from './steps/MarketsStep';
import { OtherBrokerageStep } from './steps/OtherBrokerageStep';
import { RiskStep } from './steps/RiskStep';
import { StyleStep } from './steps/StyleStep';

/** How long a connected brokerage may take to show its tools before the
 *  agent is handed the conversation without them. */
const TOOLS_WAIT_MS = 20_000;

/** How long the hand-off waits for today's market brief, read since the
 *  flow opened, before going without it. */
const BRIEF_WAIT_MS = 3_000;

const SETTINGS_RETURN = '/settings?tab=preferences';

/** The name a self-hosted install gives its user until they set their own. */
const PLACEHOLDER_NAME = 'Local User';

/**
 * The route. Waits for the saved preferences, since a returning user's answers
 * prefill the questions, then hands the flow where to open: Settings' editor
 * at its first question; otherwise the screen this browser's draft was left
 * on, else the one the URL names (a brokerage sign-in comes back to its own),
 * else the first.
 */
export default function Welcome() {
  useSyncUserLocale();
  const [searchParams] = useSearchParams();
  const { user } = useUser();
  // Read again on every visit rather than taken from the cache: the agent
  // writes the same profile and the save sends what differs from this row,
  // so it has to be the row as it is now.
  const row = useQuery({
    queryKey: queryKeys.user.preferences(),
    queryFn: getPreferences,
    refetchOnMount: 'always',
    retry: false,
  });
  const { skipWelcome } = useOnboarding();
  if (!row.isFetchedAfterMount) return <PageLoading />;
  // Not opened on a blank profile: a save is measured against the row read
  // here, so it would replace what the server holds with the few answers
  // changed on top of nothing.
  if (row.isError) return <LoadFailed retrying={row.isFetching} onRetry={() => void row.refetch()} />;

  const mode: WelcomeMode = searchParams.get(WELCOME_EDIT_PARAM) === '1' ? 'edit' : 'new';
  const steps = welcomeSteps(mode);
  const userId = user?.user_id ?? null;
  const saved = readProfile(row.data);
  const draft = mode === 'new' ? readWelcomeDraft(userId) : null;
  const resumed = draft ? resumeAnswers(draft.profile, draft.base, saved) : null;
  const asked = searchParams.get(WELCOME_STEP_PARAM);
  const initialProfile = resumed?.profile ?? saved;
  const opening = openingStep(steps, [draft?.step, isWelcomeStep(asked) ? asked : null], (step) =>
    answered(step, initialProfile),
  );

  return (
    <WelcomeFlow
      mode={mode}
      steps={steps}
      initialStep={opening}
      initialProfile={initialProfile}
      answersBase={resumed?.base ?? saved}
      resumedElsewhere={draft?.elsewhere}
      savedProfile={saved}
      userId={userId}
      userName={user?.name ?? null}
      onSkip={skipWelcome}
    />
  );
}

function LoadFailed({ retrying, onRetry }: { retrying: boolean; onRetry: () => void }) {
  const { t } = useTranslation();
  return (
    <div
      role="alert"
      data-testid="welcome-load-failed"
      className="flex min-h-dvh flex-col items-center justify-center gap-3 bg-(--color-bg-page) px-4 text-center"
    >
      <p className="text-sm text-(--color-text-secondary)">{t('onboarding.welcome.loadFailed')}</p>
      <button
        type="button"
        onClick={onRetry}
        disabled={retrying}
        className="text-sm font-medium text-(--color-text-primary) underline underline-offset-2 disabled:opacity-60"
      >
        {t('common.retry')}
      </button>
    </div>
  );
}

interface WelcomeFlowProps {
  mode: WelcomeMode;
  steps: WelcomeStepId[];
  initialStep: WelcomeStepId;
  initialProfile: InvestingProfile;
  /** The profile the server holds as the flow opens, which a save is
   *  measured against. */
  savedProfile: InvestingProfile;
  /** The saved profile the answers in `initialProfile` were given on top of,
   *  kept with the draft. The saved profile itself unless resuming one. */
  answersBase?: InvestingProfile;
  userId: string | null;
  /** The user's name as the account holds it, for the greeting. */
  userName?: string | null;
  /** Records that the user passed on the flow, so it never opens unasked again. */
  onSkip: () => void;
  /** The brokerage step's own answers, from a draft being resumed. */
  resumedElsewhere?: WelcomeDraft['elsewhere'];
}

/** A turn of the conversation: a step, or the brokerage step's second half
 *  for a user who connects none. */
type TurnId = WelcomeStepId | 'elsewhere';

/** What the composer holds at Start: a first task written in it, in place
 *  of the agent's suggestions (empty when there is none), and the files and
 *  model it brings either way. */
interface TypedStart {
  task: string;
  attachments: ReadyAttachment[];
  options: ModelOptions;
}

/** The last question being left: the answers saving, then the chat being
 *  opened on them, with a task written in the composer shown as sent and the
 *  brokerages whose tools it waits for. */
type Handoff = null | { phase: 'saving' } | { phase: 'starting'; typed: string | null; waitingFor: string[] };

/**
 * The flow, as a conversation the agent has not joined yet. Each step is a
 * turn: a scripted line, and a card under it that answers it. An answered
 * card folds into the user's bubble and the next line is written, so what was
 * said stays in view above the question being asked. Changing an answer goes
 * back to its turn, as editing a message does in a thread.
 *
 * The agent is handed the conversation as the last question is answered:
 * Start saves the answers and opens the chat on them, where the agent brings
 * in what the user holds and suggests three ways to begin, one of them on
 * today's market. A first task written in the composer, which opens on that
 * last question, goes in their place.
 */
export function WelcomeFlow({
  mode,
  steps,
  initialStep,
  initialProfile,
  savedProfile,
  answersBase = savedProfile,
  userId,
  userName = null,
  onSkip,
  resumedElsewhere,
}: WelcomeFlowProps) {
  const { t, i18n } = useTranslation();
  const locale = useLocale();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const updatePreferences = useUpdatePreferences();
  // The language the flow was gone through in, kept on the account once it
  // ends, so the agent writes in it from the first reply on.
  const keepLocale = useKeepAccountLocale();
  const brokers = useBrokers();

  const [step, setStep] = useState<WelcomeStepId>(initialStep);
  const [profile, setProfile] = useState<InvestingProfile>(initialProfile);
  // The profile as the server holds it: what the flow opened on, then each
  // save made here.
  const [saved, setSaved] = useState<InvestingProfile>(savedProfile);
  // Fixed for the visit, unlike `saved`: the draft keeps telling the answers
  // given from the values only shown, including after they are saved.
  const [draftBase] = useState(answersBase);
  // Sectors the list does not name stay on screen once shown, so one removed
  // by mistake can be put back.
  const [otherSectors] = useState(() => unlistedSectors(savedProfile.sectors, initialProfile.sectors));
  // The brokerage step's second half, for a user who connects none. A visit
  // that opens past it with brokerages named, and no draft to say, went that
  // way. The screenshots are for this visit only: they ride the opening
  // message and nothing else.
  const [elsewhere, setElsewhereState] = useState<ElsewhereAnswers>(() => ({
    ...(resumedElsewhere ?? {
      went: initialStep !== 'brokerage' && initialProfile.brokerages.length > 0,
      otherName: '',
      method: null,
    }),
    shots: [],
  }));
  // The same answers as they stand at any moment, for the hand-off to read
  // once its waits are over: a screenshot read during them is in here before
  // a render shows it.
  const elsewhereRef = useRef(elsewhere);
  const setElsewhere: Dispatch<SetStateAction<ElsewhereAnswers>> = (update) => {
    elsewhereRef.current = typeof update === 'function' ? update(elsewhereRef.current) : update;
    setElsewhereState(elsewhereRef.current);
  };
  // Each screenshot add, settled once its files are in the answers; each
  // holding or watchlist row written or removed by search, settled once the
  // write is done; and each trading level picked beside a connected broker,
  // settled once the server has answered the save. Start is a few Continues
  // past the step that took them, and can be pressed before one is in.
  const preparing = useRef<Promise<void>[]>([]);
  const onPreparing = (work: Promise<void>) => {
    preparing.current.push(work);
  };
  const [handoff, setHandoff] = useState<Handoff>(null);
  // The turn just arrived at, which writes itself in. One opened on, or gone
  // back to, is there at once.
  const [fresh, setFresh] = useState<TurnId | null>(null);
  // The hand-off's waits, called off when the flow is left. A ref rather than
  // part of `handoff`: leaving unmounts the flow, and the cleanup that calls
  // them off then needs the one running, not the one of the render it was
  // made in. It also turns away a second Start made before the first has
  // drawn anything, when a save with nothing to write sets no state at once.
  const waitRef = useRef<AbortController | null>(null);
  const composerRef = useRef<ChatInputHandle>(null);
  const currentRef = useRef<HTMLDivElement>(null);
  const placedRef = useRef(false);
  const currentTextId = useId();

  const last = steps[steps.length - 1];
  const connectedLabels = brokers.connected.map((b) => b.label);
  const connected = connectedLabels.length > 0;
  const wentElsewhere = elsewhere.went && !connected;
  const onElsewhere = step === 'brokerage' && wentElsewhere;

  // The visit's turns in order: the steps, with the brokerage step's second
  // half after it for a user who went that way. Every turn before the one
  // being asked is shown with its answer.
  const turns = steps.flatMap((s): TurnId[] => (s === 'brokerage' && wentElsewhere ? ['brokerage', 'elsewhere'] : [s]));
  const current: TurnId = onElsewhere ? 'elsewhere' : step;
  const passed = turns.slice(0, turns.indexOf(current));
  const next = turns[turns.indexOf(current) + 1];

  // Kept until the flow finishes, so a sign-in round trip or a user who leaves
  // halfway comes back to the same screen with the same answers.
  const { went, otherName, method } = elsewhere;
  useEffect(() => {
    if (mode === 'new') {
      writeWelcomeDraft(userId, { step, profile, base: draftBase, elsewhere: { went, otherName, method } });
    }
  }, [mode, userId, step, profile, draftBase, went, otherName, method]);

  // What the hand-off starts runs on Home's computer, and starting one can
  // take a minute; the questions in between are that minute. Today's brief
  // is read in it too, for the agent to start from what moved today, and the
  // chat's code is fetched, so the hand-off finds the thread ready to draw.
  useEffect(() => {
    if (mode !== 'new') return;
    void queryClient.prefetchQuery(marketBriefQuery);
    preloadRouteChunk('/chat/t/__default__');
    let live = true;
    void queryClient.ensureQueryData(homeWorkspaceQuery(queryClient)).then(
      (home) => (live ? warmWorkspace(home.workspace_id, queryClient) : undefined),
      () => {},
    );
    return () => {
      live = false;
    };
  }, [mode, queryClient]);

  useEffect(() => () => waitRef.current?.abort(), []);

  // The turn being asked starts at the top of the transcript, as a sent
  // message does, so its line and its card are in view and the answers above
  // it are a scroll away. A visit that opens on a later turn goes to it at
  // once; the first turn has nothing above it to pass.
  useEffect(() => {
    const el = currentRef.current;
    const first = !placedRef.current;
    placedRef.current = true;
    if (!el || (first && current === steps[0])) return;
    el.scrollIntoView?.({ block: 'start', behavior: first ? 'auto' : scrollMotion() });
  }, [current, steps]);

  const change = (patch: Partial<InvestingProfile>) => setProfile((p) => ({ ...p, ...patch }));

  function leave(to: string) {
    waitRef.current?.abort();
    navigate(to, { replace: true });
  }

  function finish(to: string) {
    clearWelcomeDraft(userId);
    leave(to);
  }

  function exit() {
    if (mode === 'edit') {
      leave(SETTINGS_RETURN);
      return;
    }
    onSkip();
    keepLocale();
    // The answers so far stay in the draft for the dashboard banner to resume,
    // unless they are saved already: the hand-off comes after the save.
    if (handoff?.phase === 'starting') finish('/dashboard');
    else leave('/dashboard');
  }

  /** The one write of the profile, when the last question is left. Saved, the
   *  caller moves on from the saving phase: into the chat, or back to Settings. */
  async function save(): Promise<boolean> {
    // "No account yet" stops being true once a brokerage is connected, which
    // can happen after it was picked (back on the brokers' turn, or from
    // Plugins before a draft is resumed). The agent reads its profile from
    // the saved row, so the row must not say there is no account beside the
    // one it reads; any other brokerage named is still a place the user holds
    // an account, and stays.
    const answers = connected ? { ...profile, brokerages: profile.brokerages.filter((b) => b !== 'none') } : profile;
    const patch = profilePatch(answers, saved);
    // Nothing changed: the server holds these answers already.
    if (!patch) return true;
    setHandoff({ phase: 'saving' });
    try {
      await updatePreferences.mutateAsync(patch);
      setSaved(answers);
      // The server marks onboarding complete once a risk level is saved, and
      // the banner and checklist read that off the user row.
      void queryClient.invalidateQueries({ queryKey: queryKeys.user.me() });
      return true;
    } catch {
      setHandoff(null);
      toast({
        variant: 'destructive',
        title: t('common.error'),
        description: t('onboarding.welcome.saveFailed'),
      });
      return false;
    }
  }

  /** Saves the answers and hands the conversation to the agent: to suggest
   *  where to start, or to do the task written in the composer. */
  async function handOff(written: TypedStart) {
    if (waitRef.current && !waitRef.current.signal.aborted) return;
    // Taken before the save, so leaving while it runs cancels the hand-off
    // too, not only the waits after it.
    const controller = new AbortController();
    waitRef.current = controller;
    // A hand-off that cannot go on puts the card back as it was, with the
    // task written in the composer and its files given back to it.
    const standDown = () => {
      waitRef.current = null;
      setHandoff(null);
      composerRef.current?.giveBack(written.task, written.attachments);
    };
    if (!(await save())) {
      standDown();
      return;
    }
    if (controller.signal.aborted) return;
    // Now rather than with the chat it opens, so it is saved before the
    // agent's first reply reads it.
    keepLocale();
    // The answer and the agent's pending row come up before the page moves,
    // so the transition into the chat starts from them, not from halfway in.
    // With nothing drawn there is nothing to wait for.
    const shown = handoffAnimates() ? new Promise<void>((resolve) => setTimeout(resolve, HANDOFF_DWELL_MS)) : null;
    const home = resolveChatWorkspace(queryClient, 'all').catch(() => null);
    // Today's brief may take as long as the rest of the hand-off does, and
    // no less than BRIEF_WAIT_MS; past both, the turn goes without it rather
    // than keep the user waiting on it.
    const briefRead = queryClient.fetchQuery(marketBriefQuery).catch(() => null);
    const briefDeadline = new Promise<null>((resolve) => setTimeout(() => resolve(null), BRIEF_WAIT_MS));
    // A thread's tools are fixed when it starts, and the agent's first move
    // is to read the holdings from the brokerage, so a turn sent before a
    // fresh connect's discovery lands would start without it. One already
    // showing its tools has nothing to wait for.
    const pending = brokers.connected.filter((b) => !((b.row?.tool_count ?? 0) > 0));
    setHandoff({ phase: 'starting', typed: written.task || null, waitingFor: pending.map((b) => b.label) });
    await Promise.all(
      pending.map((b) =>
        waitForServerTools(queryClient, b.b.name, { timeoutMs: TOOLS_WAIT_MS, signal: controller.signal }),
      ),
    );
    // A screenshot still being read goes with the turn, as it was shown
    // taken; it is the browser's own work, and short. A holding still being
    // written is waited for too, since the agent's first move is to read the
    // lists it goes into. So is a trading level still being saved: until it
    // lands the server holds the level it replaces, and one lowered to ask for
    // approval would not yet hold back an order the first turn places. One
    // that fails has said so in its toast, and the turn goes on the level the
    // server kept.
    await Promise.allSettled(preparing.current);
    // A brief read by now wins the race even when the deadline is past too:
    // of two settled promises, the first listed is taken.
    const [workspaceId, marketBrief] = await Promise.all([home, Promise.race([briefRead, briefDeadline]), shown]);
    if (controller.signal.aborted) return;
    if (!workspaceId) {
      standDown();
      toast({
        variant: 'destructive',
        title: t('common.error'),
        description: t('onboarding.welcome.start.failed'),
      });
      return;
    }
    // What the user said instead of connecting, with what was read since
    // Start. Screenshots go only while they are the way picked, which is the
    // way showing them, and ahead of the composer's files.
    const answers = elsewhereRef.current;
    const shots = !connected && answers.method === 'screenshot' ? answers.shots : [];
    const send = kickoffSend(
      {
        task: written.task,
        brokerages: connectedLabels,
        brokeragesUsed: connected ? [] : brokerageNames(t, profile.brokerages, elsewhere.otherName, { used: true }),
        attachments: [...shots.map(screenshotAttachment), ...written.attachments],
        screenshots: shots.length,
        brief: marketBrief,
      },
      i18n.resolvedLanguage ?? i18n.language,
      t('onboarding.kickoff.threadTitle'),
    );
    const { model, reasoningEffort, fastMode, reasoningEffortOverride, fastModeOverride, subagentsAllowed } = written.options;
    clearWelcomeDraft(userId);
    // Replacing the flow's entry, so Back from the chat goes to the page
    // before it rather than into a finished flow. Inside the hand-off, which
    // carries the conversation into the chat it opens, so the chat is told
    // to send at once and draw no empty chat while it does.
    runHandoff(() =>
      navigate('/chat/t/__default__', {
        replace: true,
        // The state a send from Home opens a new thread with.
        state: {
          workspaceId,
          ...send,
          ...(subagentsAllowed !== undefined ? { subagentsAllowed } : {}),
          ...(model ? { model } : {}),
          ...(reasoningEffort ? { reasoningEffort } : {}),
          ...(typeof fastMode === 'boolean' ? { fastMode } : {}),
          // The composer's own picks, which the thread it opens keeps.
          ...(reasoningEffortOverride ? { reasoningEffortOverride } : {}),
          ...(typeof fastModeOverride === 'boolean' ? { fastModeOverride } : {}),
          handoff: true,
        },
      }),
    );
  }

  const busy = handoff !== null;
  // The brokers' turn goes on once one is connected; its second half asks
  // nothing it needs.
  const ready = step === 'brokerage' ? connected || onElsewhere : answered(step, profile);
  // What is connected is known once the broker lists are in. A reload onto
  // the last turn can beat them there, and a start then would tell the agent
  // that nothing is connected; a list that failed is all there is to go on.
  const brokersKnown = brokers.ready || Boolean(brokers.offersError || brokers.catalogError);
  const canStart = mode !== 'new' || brokersKnown;
  const canLeave = ready && (next !== undefined || canStart);
  const canContinue = canLeave && !busy;

  function arrive(turn: TurnId) {
    setFresh(turn);
    if (turn === 'elsewhere') setElsewhere((e) => ({ ...e, went: true }));
    else setStep(turn);
  }

  async function advance() {
    if (!canContinue) return;
    if (!next) {
      // Start takes what the composer holds, as Enter in it does: a task
      // written there and not sent is not dropped for the suggestions.
      if (mode === 'new') {
        const input = composerRef.current;
        if (!input?.send()) {
          const options = input?.getModelOptions() ?? { model: null, reasoningEffort: null, fastMode: false };
          void handOff({ task: '', attachments: [], options });
        }
      } else if (await save()) finish(SETTINGS_RETURN);
      return;
    }
    arrive(next);
  }

  /** Back to a turn already answered, its answer kept on its card; the turns
   *  after it are asked again, with theirs kept too. */
  function revisit(id: TurnId) {
    if (busy) return;
    setFresh(null);
    // The brokerage step's two halves are one step: the one gone back to
    // says whether the user went elsewhere.
    if (id === 'elsewhere' || id === 'brokerage') {
      setElsewhere((e) => ({ ...e, went: id === 'elsewhere' }));
      setStep('brokerage');
    } else {
      setStep(id);
    }
  }

  // Enter continues from anywhere on the screen that does not take Enter for
  // itself: a button presses itself, a form submits, the composer sends, a
  // dialog over the flow (the brokerage consent) is its own, and so is a menu
  // open over it (the language), which holds focus before an item has it.
  const onEnter = useEffectEvent((event: KeyboardEvent) => {
    if (event.key !== 'Enter' || event.defaultPrevented || event.isComposing) return;
    if (event.metaKey || event.ctrlKey || event.altKey || event.shiftKey) return;
    const target = event.target instanceof Element ? event.target : null;
    if (
      target?.closest('button, a, textarea, select, form, [role="dialog"], [role="menu"], [contenteditable="true"]')
    ) {
      return;
    }
    event.preventDefault();
    void advance();
  });
  useEffect(() => {
    const listener = (event: KeyboardEvent) => onEnter(event);
    window.addEventListener('keydown', listener);
    return () => window.removeEventListener('keydown', listener);
  }, []);

  // A self-hosted install names its one user this until they set a name of
  // their own: a placeholder, not a name to greet.
  const ownName = (userName ?? '').trim();
  const firstName = ownName === PLACEHOLDER_NAME ? '' : ownName.split(/\s+/)[0];
  const greeting =
    mode === 'new'
      ? firstName
        ? t('onboarding.welcome.chat.greeting', { name: firstName })
        : t('onboarding.welcome.chat.greetingNoName')
      : undefined;

  /** The agent's line for a turn; the first one opens the conversation. In a
   *  new one, who the agent is and that questions follow are paragraphs of
   *  their own, so the first question stands apart from both. */
  function line(id: TurnId): string {
    const ask = t(`onboarding.welcome.chat.lead.${id}`);
    if (id !== steps[0]) return ask;
    if (mode === 'edit') return `${t('onboarding.welcome.chat.editIntro')}\n\n${ask}`;
    return `${t('onboarding.welcome.chat.intro')}\n\n${t('onboarding.welcome.chat.introNext')}\n\n${ask}`;
  }

  /** What the user is shown as having answered a turn. */
  function answer(id: TurnId): string {
    if (id === 'brokerage')
      return brokerageSummary(t, locale, { connected: connectedLabels, elsewhere: wentElsewhere });
    if (id === 'elsewhere') return elsewhereSummary(t, locale, profile, elsewhere);
    return questionSummary(t, locale, id, profile);
  }

  const proceed = (label: string) => (
    <CardAction
      label={label}
      onClick={() => void advance()}
      disabled={!canLeave}
      busy={handoff?.phase === 'saving'}
      testId="welcome-continue"
    />
  );

  /** The card that answers the turn being asked. */
  function card(id: TurnId) {
    switch (id) {
      case 'brokerage':
        return (
          <TurnCard footer={connected ? proceed(t('onboarding.welcome.continue')) : null}>
            <BrokerageStep
              labelledBy={currentTextId}
              onElsewhere={() => arrive('elsewhere')}
              onPreparing={onPreparing}
            />
          </TurnCard>
        );
      case 'elsewhere':
        return (
          <TurnCard footer={proceed(t('onboarding.welcome.continue'))}>
            <OtherBrokerageStep
              labelledBy={currentTextId}
              profile={profile}
              onChange={change}
              answers={elsewhere}
              onAnswers={setElsewhere}
              onPreparing={onPreparing}
              onConnect={() => revisit('brokerage')}
            />
          </TurnCard>
        );
      case 'experience':
        return (
          <TurnCard footer={proceed(t('onboarding.welcome.continue'))}>
            <ExperienceStep labelledBy={currentTextId} profile={profile} onChange={change} />
          </TurnCard>
        );
      case 'risk':
        return (
          <TurnCard footer={proceed(t('onboarding.welcome.continue'))}>
            <RiskStep labelledBy={currentTextId} profile={profile} onChange={change} />
          </TurnCard>
        );
      case 'markets':
        return (
          <TurnCard footer={proceed(t('onboarding.welcome.continue'))}>
            <MarketsStep labelledBy={currentTextId} profile={profile} onChange={change} />
          </TurnCard>
        );
      case 'style':
        return (
          <TurnCard footer={proceed(t('onboarding.welcome.continue'))}>
            <StyleStep labelledBy={currentTextId} profile={profile} otherSectors={otherSectors} onChange={change} />
          </TurnCard>
        );
      case 'agent':
        return (
          <TurnCard
            footer={proceed(mode === 'edit' ? t('onboarding.welcome.save') : t('onboarding.welcome.start.submit'))}
          >
            <AgentStep labelledBy={currentTextId} profile={profile} onChange={change} />
          </TurnCard>
        );
    }
  }

  const editLabel = t('onboarding.welcome.chat.edit');
  // The composer is where the agent will be talked to, so it is there from
  // the first turn, shut until the last is answered, where a first task can
  // be written in place of the agent's suggestions. Editing the profile from
  // Settings hands nothing over.
  const composerOpen = current === last && ready && !busy && canStart;
  const composer =
    mode === 'new' ? (
      <ChatInput
        ref={composerRef}
        // Its first message opens a Chief of Staff thread in Home.
        inHome
        disabled={!composerOpen}
        placeholder={t(
          composerOpen ? 'onboarding.welcome.chat.composer.start' : 'onboarding.welcome.chat.composer.waiting',
        )}
        onSend={(message, attachments, _commands, options) => {
          if (composerOpen) void handOff({ task: message.trim(), attachments, options });
        }}
      />
    ) : null;

  return (
    <ConversationShell
      testId="welcome-flow"
      exit={
        <WizardExit
          label={mode === 'edit' ? t('onboarding.welcome.cancel') : t('onboarding.welcome.skip')}
          onClick={exit}
          testId="welcome-exit"
        />
      }
      composer={composer}
      composerCue={composerOpen}
    >
      {passed.map((id) => (
        <Fragment key={id}>
          <AgentTurn heading={id === steps[0] ? greeting : undefined} text={line(id)} />
          <UserTurn
            onEdit={busy ? undefined : () => revisit(id)}
            editLabel={editLabel}
            rise={fresh === current && id === passed.at(-1)}
            testId={`welcome-answer-${id}`}
          >
            {answer(id)}
          </UserTurn>
        </Fragment>
      ))}
      <AgentTurn
        key={current}
        turnRef={currentRef}
        textId={currentTextId}
        heading={current === steps[0] ? greeting : undefined}
        text={line(current)}
        typing={fresh === current}
        // Room under the turn being asked for it to sit at the top of the
        // transcript, as a sent message does, with its card arriving in view.
        className={passed.length > 0 ? 'min-h-[calc(100dvh-16rem)]' : undefined}
        testId={`welcome-turn-${current}`}
      >
        {handoff?.phase === 'starting' ? (
          // Under the last line, where its card was, so the hand-off is in
          // view: the answer, a task they wrote, and the agent taking it. Start
          // alone sends nothing of theirs: the chat opens on the agent's reply.
          <div className="flex flex-col gap-4 sm:gap-6">
            <UserTurn editLabel={editLabel} rise testId={`welcome-answer-${current}`}>
              {answer(current)}
            </UserTurn>
            {handoff.typed !== null && (
              <UserTurn editLabel={editLabel} task rise testId="welcome-answer-start">
                {handoff.typed}
              </UserTurn>
            )}
            <AgentPending label={t('common.loading')} testId="welcome-pending">
              {handoff.waitingFor.length > 0
                ? t('onboarding.welcome.start.waiting', { brokerage: formatList(handoff.waitingFor, locale) })
                : null}
            </AgentPending>
          </div>
        ) : (
          // The card takes no input while its answers are being saved. The
          // save carries the answers as they stood when Start or Save was
          // pressed, so a pick made while it is out would show on the card and
          // never reach the server. A save that fails ends the saving phase,
          // and the card can be answered again.
          <div inert={handoff?.phase === 'saving'}>{card(current)}</div>
        )}
      </AgentTurn>
    </ConversationShell>
  );
}

/** A card's way on, as a thread's cards draw theirs. */
function CardAction({
  label,
  onClick,
  disabled,
  busy,
  testId,
}: {
  label: string;
  onClick: () => void;
  disabled: boolean;
  busy: boolean;
  testId?: string;
}) {
  return (
    <Button
      size="sm"
      onClick={onClick}
      disabled={disabled || busy}
      aria-busy={busy || undefined}
      data-testid={testId}
      className="min-w-[6.5rem]"
    >
      {busy && (
        <span aria-hidden="true" className="wizard-fade-in mr-1.5 shrink-0">
          <Loader size={14} className="text-current" />
        </span>
      )}
      <span>{label}</span>
    </Button>
  );
}
