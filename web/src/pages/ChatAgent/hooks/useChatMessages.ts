/**
 * Manages chat messages and SSE streaming for a workspace.
 * Handles thread persistence, message sending, history loading,
 * and streaming updates.
 */

import { failAssistantMessage, finalizeAssistantMessage } from '../session/stream/finalizeMessage';
import type React from 'react';
import { useState, useRef, useEffect, useCallback, useSyncExternalStore } from 'react';
import { flushSync } from 'react-dom';
import { useTranslation } from 'react-i18next';
import { useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import { holdReload } from '@/lib/reloadHolds';
import { useHomeTimezone } from '@/hooks/useHomeTimezone';
import { useStableHandler } from '@/hooks/useStableHandler';
import { useUser } from '@/hooks/useUser';
import { sendChatMessageStream, sendRetryStream, getWorkflowStatus, sendHitlResponse, fetchThreadTurns, cancelWorkflow } from '../utils/api';
import { useLocalRunPublisher } from '@/lib/threadLifecycle/useLocalRunPublisher';
import { peekThreadMux } from '../session/stream/threadStreamMux';
import type { WorkflowStatusResponse } from '../utils/api';
import type { CancelOutcome, HeldSteer } from '../utils/cancelOutcome';
// Imported from the dependency-free signal module (not `../utils/api`) so this
// keeps decoding wire status even in the hook tests that fully mock `../utils/api`.
import { reportOwedForStatus, shouldArmForStatus } from '../utils/reportBackSignal';
import { useReportBackWatch } from './useReportBackWatch';
import { useChatFeedback } from './useChatFeedback';
import { toast } from '@/components/ui/use-toast';
import { buildRateLimitError, type ErrorLinkSpec, type StructuredError } from '@/utils/rateLimitError';
import { isClientError } from '@/api/errors';
import { getStoredThreadId, setStoredThreadId } from './utils/threadStorage';
import { type SubagentTokenUsage, ZERO_USAGE } from '../utils/tokenUsage';
import { computeSteeringBoundary } from '../session/stream/steeringRollback';
import { isSteeringUserMessage, opensTurn } from '../components/messageList/messagePredicates';
import { hasTurn, hiddenOpenerTurns, newestTurn, projectTurns, turnOf } from '../components/messageList/turnProjection';
import { isSubagentEvent } from '../session/subagents/liveEventHandlers';
import { bumpThreadNavOrder } from './useNavigationData';
import { invalidateNewWorkspace } from './workspaceRowActions';
import { ensureThreadId } from '../session/threadCreation';
export { removeStoredThreadId } from './utils/threadStorage';
import { carriesContext, createUserMessage, createAssistantMessage, createNotificationMessage, appendMessage, updateMessage, type AttachmentMeta } from './utils/messageHelpers';
import type { HitlResponseBody } from '@/types/api';
import type { AssistantMessage, UserMessage, ChatMessage } from '@/types/chat';
import type { PreviewData } from './utils/types';
import { createRecentlySentTracker } from './utils/recentlySentTracker';
import { createRequestKeyTracker, sendFingerprint } from './utils/requestKey';
import { withServerTotals, withoutServerTotals } from './utils/contextWindowEvent';
import { BUSY_REFUSAL_KEYS, createForkRefusalRecovery, forkRefusalMessage, isBusyCode, isRecoverableRetryRefusal, isStaleForkRefusal, type BusyCode, type ForkCut } from './utils/forkRefusal';
import { createEditDraftStore, sameDraftMessage, type EditDraft } from './utils/editDrafts';
import { settleStoppedBubble } from '../session/stream/stopBubble';
import { withoutPending } from '../session/stream/toolCallChunks';
import { useMarketWatch } from './useMarketWatch';
import { invalidateThreadWakeups } from './useWakeups';
import { invalidateThreadCommands } from './useCommands';
import { refreshComputersAfterTurn } from './useComputers';
// Chart-annotation live bridge: writes agent-drawn annotations into the
// shared MarketView store so the desktop MarketView chat panel (which uses
// this engine for both Chief of Staff and PTC threads) renders them live.
// Harmless on the standalone /chat page: the store simply has no chart
// consumer there.

// --- Module scope extracted to session/types + utils (W1) ---
import type {
  TokenUsage, PendingInterrupt,
  SSEEvent, ModelOptions, SendOptions, OffloadBatch, TaskRefs,
  HistoryInterruptInfo, StreamProcessorRefs,
  ModelStatus, FallbackSuggestion,
} from '../session/types';
import { createAnswerBoard, type AnswerBoard } from '../session/interrupts/answerBoard';
import { useInterruptAnswers } from '../session/interrupts/useInterruptAnswers';
export type { ModelStatus, FallbackSuggestion } from '../session/types';
import type { ChatSessionRuntime } from '../session/runtime';
import { noteLiveRun } from '../session/latestRun';
import type { CardUpdater } from '../session/streamRefs';
import { prependSubagentRuns, projectSubagentHistory } from '../session/subagents/projectHistory';
import { createSubagentMuxController, getTaskIdFromEvent } from '../session/subagents/muxSink';
import { sendTaskInstruction } from '../session/subagents/sendTaskInstruction';
import { loadConversationHistory as replayConversationHistory } from '../session/history/replayHistory';
import { loadOlderHistoryPage, commitOlderHistoryPage } from '../session/history/olderPages';
import { createHistoryWindow, HISTORY_PAGE_TURNS } from '../session/history/historyWindow';
import { createStreamEventProcessor, type StreamRouterDeps } from '../session/stream/processStreamEvent';
import { forkRequest, retryRequest, retryRequestFor, sendRequest, settleRetryNotice, type HeldRetry, type RetryNotice, type TurnRequest } from '../session/stream/autoRetry';
import { createLiveTranscript, type LiveMessages } from '../session/stream/liveMessages';
import { useSubagentHistory } from '../session/subagents/useSubagentHistory';
import {
  acquireStreamOwnership as acquireOwnership,
  releaseStreamOwnership as releaseOwnership,
  reconnectToStream as reconnectToStreamImpl,
  attemptReconnectAfterDisconnect as attemptReconnectImpl,
  cleanupAfterStreamEnd as cleanupAfterStreamEndImpl,
  type RecoveryDeps, type ReconnectOptions,
} from '../session/stream/lifecycle';
import { collectRenderedInterruptIds, finalizeTodoListProcessesInMessages } from './utils/messageFinalizers';
export { finalizeTodoListProcessesInMessages, mapToolCallIdToAgentId } from './utils/messageFinalizers';

/** A send the server started no run of. Its automatic resends carry it, so
 * the one that ends the chain can mark it never kept and give its turn back.
 * A hidden opener drew no message, so it has no user bubble to carry. */
interface UnstartedSend {
  userId: string | null;
  watermarkBefore: number | null;
}

/** Bubbles already on screen that a new message, and on a fork its reply,
 * take over: a send parked during compaction or a refused steer going out
 * again, or the ones an edit or regenerate draws in place of its cut while it
 * reads the checkpoint it forks from. The new bubbles take their ids, so the
 * list keeps them mounted and they do not play their entrance a second time.
 * No user id when a fork draws none, and no reply for a send, whose reply
 * opens a bubble of its own. */
interface ShownBubbles {
  userId?: string;
  assistantId?: string;
}

/** A send going out again in place of one waiting on screen: a message parked
 * during compaction, or a refused steer, which goes under the request key it
 * was sent with. */
interface Resend {
  requestKey?: string;
  shown?: ShownBubbles;
}

export function useChatMessages(
  workspaceId: string,
  initialThreadId: string | null = null,
  updateTodoListCard: ((todoData: Record<string, unknown>, isNew?: boolean) => void) | null = null,
  updateSubagentCard: CardUpdater | null = null,
  finalizePendingTodos: (() => void) | null = null,
  onFileArtifact: ((event: SSEEvent) => void) | null = null,
  onPreviewUrl: ((data: PreviewData) => void) | null = null,
  clearSubagentCards: (() => void) | null = null,
  onWorkspaceCreated: ((info: { workspaceId: string; question: string }) => void) | null = null,
  platform: string | null = null,
  readComposerOptions: (() => ModelOptions | undefined) | null = null,
) {
  const { t } = useTranslation();
  const queryClient = useQueryClient();

  // User locale/timezone — prefer saved preference, fall back to browser detection
  const { user } = useUser();
  const userLocale = user?.locale || navigator.language || 'en-US';
  const userTimezone = useHomeTimezone();

  // State
  const [messages, setMessagesState] = useState<ChatMessage[]>([]);
  // Streamed chunks wait for the next frame and then land in `liveMessages`
  // alone, which only the transcript's readers render from: `messages` holds
  // the structure and lags on the text. Every other write goes through
  // `setMessages`, computed on the live value, never on `messages`: nearly
  // every updater copies the streaming message whole, and on a lagging copy it
  // would put the old text back.
  const [liveMessages] = useState(() => createLiveTranscript<ChatMessage[]>([], setMessagesState));
  const setMessages = liveMessages.write;
  useEffect(() => liveMessages.dispose, [liveMessages]);
  // What the views get: read-only, since a write from there would skip `messages`.
  const liveView: LiveMessages<ChatMessage[]> = liveMessages;
  const [threadId, setThreadId] = useState<string>(() => {
    // If threadId is provided from URL, use it; otherwise use localStorage
    if (initialThreadId) {
      return initialThreadId;
    }
    return workspaceId ? getStoredThreadId(workspaceId) : '__default__';
  });
  const [isLoading, setIsLoading] = useState(false);
  // For checks that outlive a render: isLoading is state, and a check that
  // runs between renders reads it here.
  const isLoadingRef = useRef(isLoading);
  isLoadingRef.current = isLoading;
  const [isLoadingHistory, setIsLoadingHistory] = useState(
    () => !!(initialThreadId && initialThreadId !== '__default__')
  );
  const [historyLoadFailed, setHistoryLoadFailed] = useState(false);

  const [hasActiveSubagents, setHasActiveSubagents] = useState(false);  // Subagent streams open after main agent finished
  // false | 'starting' (generic cold start) | 'archived' (slow ~90s restore from cold storage).
  // Widen this union when backend adds a new sandbox_state discriminator.
  const [workspaceStarting, setWorkspaceStarting] = useState<false | 'starting' | 'archived'>(false);
  // Why this send waits on a thread mutation (the `turn_held` reason: 'compact',
  // 'offload', 'handoff', or another/empty), null when it is not held.
  const [turnHeld, setTurnHeld] = useState<string | null>(null);
  const [isCompacting, setIsCompactingState] = useState<string | false>(false);  // Context compaction in progress (summarize/offload)
  // About how many tokens the summary being written has written, null until
  // the server first says.
  const [compactionTokens, setCompactionTokens] = useState<number | null>(null);
  // A compaction that starts or ends starts its count over, so a count never
  // carries into the next one, nor onto a manual /compact, which sends none.
  const setIsCompacting = useCallback((value: string | false) => {
    setIsCompactingState(value);
    setCompactionTokens(null);
  }, []);
  // Transient model-resilience status (retry / fallback) shown as a pill above
  // the input while streaming. Mirrored in a ref so the clear can be
  // ref-guarded — avoids a setState on every streamed chunk once it's already
  // null. Set by model_retry/model_fallback; cleared on first content/tool
  // event, on error, on stream end, and on stop.
  const [modelStatus, setModelStatus] = useState<ModelStatus | null>(null);
  const modelStatusRef = useRef<ModelStatus | null>(null);
  const applyModelStatus = (status: ModelStatus) => {
    modelStatusRef.current = status;
    setModelStatus(status);
  };
  const clearModelStatus = () => {
    if (modelStatusRef.current !== null) {
      modelStatusRef.current = null;
      setModelStatus(null);
    }
  };
  // Persistent (until acted on) switch-to-working-model suggestion. Unlike
  // modelStatus it survives stream end — set by model_fallback (live and
  // history replay), cleared on error, on a new turn (send/edit/regenerate
  // and replayed user_message boundaries), on thread switch, and on
  // dismiss/switch. Chained fallbacks keep the FIRST from-model (the
  // user-configured one) and track the LATEST to-model (the one answering).
  const [fallbackSuggestion, setFallbackSuggestion] = useState<FallbackSuggestion | null>(null);
  const applyFallbackSuggestion = (event: Record<string, unknown>) => {
    const fromModel = (event.from_model as string) || '';
    const toModel = (event.to_model as string) || '';
    if (!toModel) return;
    setFallbackSuggestion((prev) => ({
      fromModel: event.from_is_primary === false && prev ? prev.fromModel : fromModel,
      toModel,
      paidBy: typeof event.to_paid_by === 'string' ? event.to_paid_by : undefined,
      provider: typeof event.to_provider === 'string' ? event.to_provider : undefined,
    }));
  };
  const clearFallbackSuggestion = useCallback(() => setFallbackSuggestion(null), []);
  // A message the user pressed Send on while the agent was compacting. Held
  // until compaction finishes (mirrors the backend admission gate, which 409s
  // a POST that arrives mid-compaction), then auto-sent: steered if a turn is
  // still running, else a fresh turn. queuedSend (the preview text) drives the
  // chip; queuedSendRef holds the full payload to replay.
  const [queuedSend, setQueuedSend] = useState<string | false>(false);
  const queuedSendRef = useRef<{
    message: string;
    additionalContext: Record<string, unknown>[] | null;
    attachmentMeta: Record<string, unknown>[] | null;
    modelOptions: ModelOptions;
    // id of the optimistic shimmer bubble shown while parked, so it can be
    // removed on flush (before the real send re-adds it) or on stop.
    messageId: string;
  } | null>(null);
  const [messageError, setMessageError] = useState<string | StructuredError | null>(null);
  // Steer text for the composer: the user's steers a stop took back from the
  // run, and any the stop overtook on their way to it.
  const [returnedSteering, setReturnedSteering] = useState<string | null>(null);
  const giveBackSteering = (text: string) => setReturnedSteering((prev) => (prev ? `${prev}\n${text}` : text));
  // Steers the server refused as `not_running`, oldest first: each goes out
  // as a send once the view lets the ended turn go (the flush effect below).
  const notRunningSendsRef = useRef<{
    /** The queue's generation when it was sent, from `notRunningGenRef`. */
    gen: number;
    message: string;
    additionalContext: Record<string, unknown>[] | null;
    attachmentMeta: Record<string, unknown>[] | null;
    modelOptions: ModelOptions;
    requestKey: string;
    /** The steer's bubble, which waits on screen until its send replaces it. */
    steerId: string;
    /** When it was sent, from `steerSeqRef`. */
    seq: number;
  }[]>([]);
  // Orders the queue above by when each message was sent: refusals come back
  // on separate POSTs, in whatever order those answer.
  const steerSeqRef = useRef(0);
  // Ends the queue above, and any refusal still on its way to it. The queue
  // lives with its own thread's view, so only an in-place thread switch ends
  // it: the '__default__' → real id move of a new chat's first send keeps it,
  // and a steer sent before the thread existed goes out on the created one.
  const notRunningGenRef = useRef(0);
  const [notRunningTick, setNotRunningTick] = useState(0);
  // Edit text whose message is gone, for the composer.
  const [returnedEdit, setReturnedEdit] = useState<string | null>(null);
  const clearReturnedEdit = useCallback(() => setReturnedEdit(null), []);
  // HITL (Human-in-the-Loop) interrupt state
  const [pendingInterrupt, setPendingInterrupt] = useState<PendingInterrupt | null>(null);

  // Token usage tracking (for context window progress ring)
  const [tokenUsage, setTokenUsage] = useState<TokenUsage | null>(null);
  const [isShared, setIsShared] = useState(false);

  // Bridge: handler modules define their own local SetMessages accepting Record<string, unknown>[]
  const setMessagesForHandlers = setMessages as unknown as (
    updater: (prev: Record<string, unknown>[]) => Record<string, unknown>[]
  ) => void;

  // Track last-used model options so HITL resume can forward them
  const lastModelOptionsRef = useRef<ModelOptions>({ model: null, reasoningEffort: null, fastMode: null });

  // Refs for streaming state
  const currentMessageRef = useRef<string | null>(null);
  const contentOrderCounterRef = useRef(0);
  const currentReasoningIdRef = useRef<string | null>(null);
  const currentToolCallIdRef = useRef<string | null>(null);
  const steeringAtOrderRef = useRef<number | null>(null); // Shared across streams for steering rollback
  // AbortController for the active main-agent stream. stopWorkflow() aborts it so
  // the client-side reader stops immediately (instant stop) — the matching POST
  // /cancel tears down the backend run. Null when no main stream is in flight.
  const mainStreamAbortRef = useRef<AbortController | null>(null);
  // The reconnect that currently owns the "Reconnecting…" spinner. Its finally
  // clears the spinner only if it's still the owner — a newer reconnect takes
  // ownership and manages its own spinner, so a stale/superseded reconnect can
  // neither clobber the new one's spinner nor strand its own.
  const isReconnectingOwnerRef = useRef<AbortController | null>(null);
  // The thread a reconnect stream is attached to (set when reconnectToStream
  // marks isStreamingRef). Lets the thread-load effect tell "this thread is
  // streaming" (skip the load) from "a DIFFERENT thread is streaming" (e.g. a
  // report-back), in which case it supersedes that stream so the
  // navigated-to thread can still load and reconnect. Null when no reconnect
  // stream is in flight.
  const streamingThreadIdRef = useRef<string | null>(null);
  // Guards finalizeStreamingMessage / stopWorkflow so a double-click stop (or
  // handler re-entry) doesn't append duplicate synthetic close events. Cleared
  // on the next send.
  const wasStoppedRef = useRef(false);
  // Set by the foreground (visibilitychange/pageshow) handler when it aborts a
  // likely-dead main stream on tab resume, so the stream's result handler
  // re-kicks the existing reconnect instead of treating the abort as a user
  // stop. Consumed (cleared) by the send/reconnect/HITL/checkpoint result
  // sites; also reset at every stream entry point (alongside wasStoppedRef) so
  // a stream type that doesn't consume it (e.g. steering) can't leak a stale
  // flag into a later abort and mis-fire a reconnect onto the wrong turn.
  const backgroundReconnectRef = useRef(false);
  // True once the tab was GENUINELY suspended (Page Lifecycle `pagehide`/`freeze`)
  // since it last became visible — the only state in which the SSE socket was
  // actually torn down. A plain desktop tab-switch fires `visibilitychange` but
  // NOT these, and keeps the socket alive; gating the foreground reconnect on
  // this flag means alt-tabbing never aborts a healthy stream (no regression for
  // non-suspended users). Set on suspend, consumed+cleared by the first resume
  // handler so one suspend→resume cycle triggers at most one reconnect.
  const tabSuspendedRef = useRef(false);

  // Refs for history loading state
  const historyLoadingRef = useRef(false);
  // The whole thread load, from its /status read through the arm and reconnect
  // after the replay; historyLoadingRef spans only the replay. Holds the load's
  // own token, so a superseded load can't clear its successor's.
  const threadLoadingRef = useRef<object | null>(null);
  const [isLoadingThread, setIsLoadingThread] = useState(false);
  const newMessagesStartIndexRef = useRef(0); // Index where new messages start
  // Where the loaded transcript starts and the older page in flight.
  const [historyWindow] = useState(createHistoryWindow);
  const olderHistoryStatus = useSyncExternalStore(historyWindow.subscribe, historyWindow.snapshot);
  // Guards against the load-history effect doing a redundant replay when
  // (workspaceId, threadId, reloadTrigger) re-resolve to a tuple this hook
  // already loaded — most often during React 18 StrictMode's mount→unmount→
  // remount in dev, but also any future code path that increments
  // ``reloadTrigger`` while the previous load already covered the same state.
  // Without this, ``loadConversationHistory`` would re-run and append a fresh
  // set of ``history-assistant-{pairIndex}-${Date.now()}`` bubbles atop the
  // ones it already created, because the bubble keys aren't deterministic so
  // React can't dedup by id. Cleared on real thread switches (see line ~682)
  // and on load failure (the catch path below) so retries still work.
  const historyLoadedKeyRef = useRef<string | null>(null);
  // Counts `online` events. The retry listener below can only bump
  // reloadTrigger when no load is in flight, and a request issued against a
  // dead link does not always fail fast enough to be out of the way — so the
  // load itself re-checks this counter and re-fires when the link returned
  // underneath it. Without that, the one `online` we get is dropped and the
  // "it will catch up" message never comes true.
  const onlineRetryEpochRef = useRef(0);
  // Monotonic ownership token for this hook's stream session, bumped wherever a
  // new turn takes it and wherever navigation gives it up. A reconnect retry
  // outlives the turn that started it — it can sit in an offline slice for
  // minutes — and every signal it could otherwise key off is reused: the next
  // send clears wasStoppedRef, currentRunIdRef is null between that send and
  // its metadata frame, and threadIdRef is back to its original value after a
  // there-and-back navigation. Only a value that never goes backwards can tell
  // "still my session" from "someone else's".
  const sessionEpochRef = useRef(0);

  // Track all LLM models used in this thread (ordered, deduplicated)
  const [threadModels, setThreadModels] = useState<string[]>([]);

  // Track if streaming is in progress to prevent history loading during streaming
  const isStreamingRef = useRef(false);

  const acquireStreamOwnership = (tid: string | null) => acquireOwnership(runtime, tid);
  // A reload asked for mid-stream waits here (a mux resync, a catch-up reload a
  // report-back attach overtook, or a load a send overtook): bumping the reload
  // trigger while streaming runs the load effect's cleanup (detaching the mux)
  // and then bails on the streaming guard — losing the reload.
  const pendingMuxResyncRef = useRef(false);
  // Steers whose POST has not answered, by send order, with the refused-steer
  // queue's generation each was sent in. One sent as the run ended comes back
  // as a new turn that streams its own run, so a load waits for it as for a
  // stream: attaching to that run first would read it twice. The refused-steer flush waits for those sent
  // before its next, whose refusal may still land.
  const steersInFlightRef = useRef(new Map<number, number>());
  // A refused steer's banner, for a load to show again once it has cleared
  // it: the first load to succeed after the refusal shows it, unless a turn
  // sent or a thread opened since has moved the session on.
  const refusalAfterReloadRef = useRef<{ epoch: number; banner: string | StructuredError } | null>(null);
  // The reloadTrigger the load effect last ran for, so a bail can tell a
  // reload request from a thread change.
  const reloadSeenRef = useRef(0);

  const releaseStreamOwnership = () => releaseOwnership(runtime);

  const { handleThumbUp, handleThumbDown, feedbackByTurn, loadFeedback } = useChatFeedback(threadId);

  // Track if history replay found an unresolved interrupt (skip reconnection in that case)
  const historyHasUnresolvedInterruptRef = useRef(false);
  // Store the full interrupt details from history so loadAndMaybeReconnect can decide
  // whether to make it interactive or reconnect to get resolution events
  const unresolvedHistoryInterruptRef = useRef<HistoryInterruptInfo[]>([]);

  // Batch parallel interrupt responses: track all interrupt IDs in current batch
  // and collect individual responses until all are answered, then resume at once.
  const pendingInterruptIdsRef = useRef(new Set<string>());
  // The one owner of a turn's HITL decisions: card status, batch readiness, the
  // `hitl_response` map and the rollback of a refused resume all come from here.
  const answerBoard = useRef<AnswerBoard>(
    createAnswerBoard({
      pendingIds: pendingInterruptIdsRef,
      sessionEpoch: sessionEpochRef,
      setMessages,
    }),
  ).current;
  // Thread-scoped set of interrupt_ids that already have a rendered card. An
  // unanswered interrupt is re-raised by LangGraph with the SAME interrupt_id on
  // every resume; each re-raise arrives on a later turn's bubble, so the per-map
  // dedup (keyed by interrupt_id) never collides and a duplicate card would be
  // appended. This ref survives resume (NOT cleared with pendingInterruptIdsRef),
  // cleared only on thread switch / fresh history load. Shared by replay + live
  // so a live re-raise after a history load dedupes against replayed cards too.
  const renderedInterruptIdsRef = useRef(new Set<string>());

  // Track approved PTC agent proposals waiting for thread_id backfill from tool_call_result.
  // Set by handleApprovePTCAgent, consumed by tool_call_result handler in the stream processor.
  // Maps tool_call_id → proposalId for exact matching (safe under concurrent dispatches).
  const pendingPTCBackfillRef = useRef<Map<string, string>>(new Map());

  // Track the last received SSE event ID for reconnection
  const lastEventIdRef = useRef<number | string | null>(null);
  // Track the active run_id for this thread. Populated from the SSE
  // ``metadata`` event (first event of every workflow stream) so reconnect
  // can target the exact ``workflow:stream:{tid}:{rid}`` key and so the
  // steering handler can detect when a POST was routed as a new turn
  // (race: status flipped terminal between isLoading check and POST land).
  const currentRunIdRef = useRef<string | null>(null);
  // The thread's newest run as this view has seen it, which a fork names so
  // the server refuses an edit from a view that missed a turn. A load starts
  // it from the run the replay names; every run the view sees start
  // afterwards, from a stream's first frame, a failure frame naming it, a
  // reattach or an adopted duplicate, is the thread's newest at that moment,
  // and outranks the run of a replay it arrived during (session/latestRun.ts).
  // A response header names its run before the server starts it, and a
  // refusal starts none, so a header only aims the reconnect. Null names
  // none, and the fork goes unchecked as before.
  const latestRunIdRef = useRef<string | null>(null);
  const latestRunEpochRef = useRef(0);
  const latchRun = (runId: string): void => {
    currentRunIdRef.current = runId;
    noteLiveRun({ latestRunIdRef, latestRunEpochRef }, runId);
  };
  // Local-layer run-liveness publish (declared here so it sits below the
  // run-id ref it reads).
  useLocalRunPublisher(threadId, isLoading, currentRunIdRef, latestRunIdRef);
  // The host's read of that ref: whether a run is the one this view is
  // streaming or last streamed. Read, not handed out, so no caller can move
  // the reconnect target.
  const isOwnRun = useCallback((runId: string) => runId === currentRunIdRef.current, []);
  // Every run this view starts begins here: its assistant message counts
  // content from zero, and its run_id comes from its own metadata frame unless
  // the response header already named it. A stale one would bias a reconnect
  // into the previous run's stream key.
  const beginRun = (runId: string | null = null): void => {
    contentOrderCounterRef.current = 0;
    currentReasoningIdRef.current = null;
    currentToolCallIdRef.current = null;
    currentRunIdRef.current = runId;
  };
  // Highest turn_index this view has RENDERED, compared against
  // /status.latest_turn_index by the reactivation staleness check (a run that
  // finished while this cached view was hidden is terminal — can_reconnect is
  // false and there is no run_id to compare, so the turn counter is the only
  // staleness signal). null = no successful history load yet (treated as
  // not-stale); -1 = loaded, zero turns. Authoritatively assigned by each
  // history replay (replay emits every persisted turn's turn_index), bumped by
  // in-view sends, and pinned to the fork turn on edit/regenerate (the backend
  // truncates turns > fork). Deliberately a LOWER bound elsewhere (e.g. a
  // report-back turn attached in-view doesn't bump it): under-counting only
  // over-triggers one corrective reload, while over-counting would suppress a
  // genuinely-needed one.
  const lastRenderedTurnIndexRef = useRef<number | null>(null);
  // The newest turn this view knows the thread has, -1 for none. Both
  // sources, since each can lead: the watermark knows turns a paged transcript
  // has not loaded, and the transcript knows turns the watermark (a lower
  // bound) never counted. A send refused before admission opened no turn on
  // the server, so the next send takes its number, as the server will give it.
  const newestKnownTurn = (): number =>
    Math.max(lastRenderedTurnIndexRef.current ?? -1, newestTurn(liveMessages.get()) ?? -1);
  // Terminal-run ids the latest history replay rendered; consumed by the
  // load flow's markRunsRendered call so the report-back catch-up can't
  // re-attach an already-on-screen turn as a duplicate bubble.
  const replayedRunIdsRef = useRef<string[]>([]);
  // Ref-based thread ID for use inside closures (avoids stale React state in callbacks)
  const threadIdRef = useRef(threadId);
  // The edit boxes open on this view's threads, kept across reloads.
  const [editDrafts] = useState(() => createEditDraftStore(() => threadIdRef.current));

  // Report-back watch subsystem (owns its dedicated refs, constants + lifecycle).
  // The host injects the shared stream primitives; `reconnectToStream` is passed
  // via a ref because it's defined later in this body and forms a runtime cycle
  // with the watch (watch → reconnectToStream → cleanupAfterStreamEnd →
  // watch.onStreamEnd). The ref starts as a no-op and is assigned the real reader
  // right after its definition — before any async watch callback can fire.
  const reconnectToStreamRef = useRef<
    (opts?: { activeTasks?: string[]; runId?: string | null; resetCursor?: boolean; idleAbortMs?: number; snapshotAtMs?: number }) => Promise<void>
  >(async () => {});
  // Counter to re-trigger loadAndMaybeReconnect (failed reconnection, or a
  // stale-run reactivation of a cached view). Declared before the watch so
  // `requestHistoryReload` below can close over the setter directly.
  const [reloadTrigger, setReloadTrigger] = useState(0);
  // A finished stream means the server persisted this turn (pair persistence
  // runs at terminal), so every rendered bubble is now reproducible from
  // /messages/replay. Mark them isHistory so a later corrective reload
  // REPLACES them via replay — the history loader only clears isHistory
  // bubbles, so unmarked live bubbles would survive it and the replay would
  // render their twins (duplicated transcript). Failed sends never reach a
  // success finalize, so their (unpersisted) bubbles deliberately stay
  // unmarked and survive reloads.
  const markTranscriptPersisted = useCallback(({ keepQueued = false }: { keepQueued?: boolean } = {}) => {
    // A steer the server refused inside the turn was never stored either, nor
    // yet one still queued in a run that may go on, nor a send it never kept.
    const persisted = (m: ChatMessage) => !m.isHistory && !(m as { queueError?: unknown }).queueError
      && !(keepQueued && (m as { steering?: boolean }).steering)
      && !(m as { unadmitted?: boolean }).unadmitted;
    setMessages((prev) =>
      prev.some(persisted)
        ? prev.map((m) => (persisted(m) ? { ...m, isHistory: true } : m))
        : prev,
    );
    // The recently-sent dedup exists to keep a replay from twinning an
    // optimistic user bubble that is still on screen. The bubbles just became
    // clearable-by-reload, so replay is now their only source — keeping the
    // tracker armed would make the reload's replay SKIP the user message
    // whose optimistic bubble it just cleared (vanished user bubble).
    recentlySentTrackerRef.current.clear();
  }, [setMessages]);
  const reportBackWatch = useReportBackWatch({
    threadId,
    workspaceId,
    threadIdRef,
    isStreamingRef,
    currentRunIdRef,
    lastRenderedTurnIndexRef,
    historyLoadedKeyRef,
    threadLoadingRef,
    isLoadingRef,
    reconnectToStream: (opts) => reconnectToStreamRef.current(opts),
    requestHistoryReload: () => setReloadTrigger((n) => n + 1),
    // Producer-undecided grace: while subagent run channels are open on the
    // thread mux, an idle /status read must not tear the watch down (tail
    // report-backs only become pending once their subagent completes).
    // Deferred call — the helper is declared below and initialized before
    // any watch event can fire.
    hasOpenProducers: () => muxOpenTaskIds().size > 0,
  });
  // `arm` is identity-stable (facade over a latest-impl ref), so callbacks that
  // dispatch through it can dep on it without churning per render — the whole
  // reportBackWatch object would change identity on awaitingReportBack flips.
  const {
    awaitingReportBack, reportBackOwed, owedThreads: reportBackOwedThreads, owedKinds: reportBackOwedKinds,
    arm: armReportBackWatch,
  } = reportBackWatch;

  // Batch back-to-back offload events into a single notification
  const offloadBatchRef = useRef<OffloadBatch>({ args: 0, reads: 0, timer: null });
  // Track reconnection state for UI indicator
  const [isReconnecting, setIsReconnecting] = useState(false);

  // Market-watch chip lifecycle: seed on thread load/switch + refetch on turn
  // completion. Live mid-turn overwrites arrive via `market_watch_update` SSE
  // events, which forward `setMarketWatch` (see processEvent below), and a
  // delivered price alert refetches through `refreshMarketWatch`.
  const { marketWatch, setMarketWatch, refreshMarketWatch } = useMarketWatch(threadId, isLoading, threadIdRef);
  // The wakeups list is a query its readers own; the stream and a reload
  // only say it may have moved. Every change a watched turn makes arrives as
  // an event, so a turn's end needs no read of its own.
  const refreshWakeups = useCallback(
    () => invalidateThreadWakeups(queryClient, threadIdRef.current),
    [queryClient, threadIdRef],
  );
  // The same for the thread's background commands: the stream says when one
  // moved to the background or reported its end, and the listing polls while
  // one runs, so a turn's end needs no read of its own either.
  const refreshCommands = useCallback(
    () => invalidateThreadCommands(queryClient, threadIdRef.current),
    [queryClient, threadIdRef],
  );

  // Track if this is a new conversation (for todo list card management)
  const isNewConversationRef = useRef(false);

  // Recently sent messages tracker
  const recentlySentTrackerRef = useRef(createRecentlySentTracker());
  // v4 idempotent delivery: one request_key per logical send, reused across
  // retransmits of the same send until response headers prove acceptance.
  const requestKeyRef = useRef(createRequestKeyTracker());
  // What each failed reply's Retry sends, once the client stops retrying it,
  // and the fork it stands for, by bubble: a failure on another thread holds
  // its own and leaves this one's in place.
  const heldRetriesRef = useRef(new Map<string, HeldRetry & { cut: ForkCut | null }>());

  // The CURRENT stream processor for subagent frames off the thread mux.
  // Send, reconnect and HITL resume each install theirs at attach time, so
  // task frames always route through live refs instead of a stale closure.
  const subagentProcessEventRef = useRef<((event: SSEEvent) => void) | null>(null);

  // Open subagent run channels, from mux truth (empty when no mux exists).
  const muxOpenTaskIds = (): Set<string> => {
    const tid = threadIdRef.current;
    const mux = tid ? peekThreadMux(tid) : null;
    return mux ? mux.openTaskIds() : new Set<string>();
  };

  // Terminal outcomes the client has observed live (per-task chan_close), keyed by
  // short task id. Authoritative and monotonic: once a task settles here, no stale
  // liveness signal (a reconnect pre-seed off an older /status snapshot, a duplicate
  // spawn artifact, a stale-history refresh) may revert its card to active. Lives
  // with the subagent-card projection — cleared only alongside clearSubagentCards()
  // on a full history-backed reset; a genuine resume deletes just that task's entry.
  const terminalTaskOutcomesRef = useRef(new Map<string, 'completed' | 'cancelled' | 'error'>());

  // Persistent subagent state refs — survives across turns so resumed subagents
  // retain messages from previous runs. Keyed by taskId (e.g., "task:k7Xm2p").
  const subagentStateRefsRef = useRef<Record<string, TaskRefs>>({});
  // Replayed per-task transcripts and the tool-call id to task id index. A
  // publish applies the queued chunks first, like any write that is not one.
  const {
    store: subagentHistory, entries: subagentHistoryEntries, resolveSubagentIdToAgentId, getSubagentHistory, hydrateTaskTranscript,
    getTaskRuns, putTaskRun,
  } = useSubagentHistory(liveMessages.flush, { t, threadId, subagentStateRefsRef });
  // An instruction the user sends a running subagent, bound to the
  // transcripts its stream writes.
  const sendSubagentInstruction = (tid: string, agentId: string, content: string) =>
    sendTaskInstruction({ t, updateSubagentCard, subagentStateRefsRef }, tid, agentId, content);

  /**
   * Handler-refs bag shared by every stream entry point. One construction
   * site so a new ref reaches all streams at once; reconnect paths get the
   * isReconnect-stamping card updater, and only the main reconnect path
   * carries unresolvedHistoryInterruptRef (withUnresolvedInterrupt).
   */
  const buildStreamRefs = (o: {
    isNewConversation?: boolean;
    isReconnect?: boolean;
    withUnresolvedInterrupt?: boolean;
  } = {}): StreamProcessorRefs => {
    const refs: StreamProcessorRefs = {
      contentOrderCounterRef,
      currentReasoningIdRef,
      currentToolCallIdRef,
      steeringAtOrderRef,
      updateTodoListCard: updateTodoListCard || undefined,
      isNewConversation: o.isNewConversation ?? false,
      subagentStateRefs: subagentStateRefsRef.current,
      updateSubagentCard: updateSubagentCard || (() => {}),
      ...(o.isReconnect ? { isReconnect: true } : {}),
      ...(o.withUnresolvedInterrupt ? { unresolvedHistoryInterruptRef } : {}),
    };
    // Read at call time: a reconnect flips the bag live once its backlog is
    // applied, and the cards it updates after that are live too.
    if (updateSubagentCard && o.isReconnect) {
      refs.updateSubagentCard = (agentId: string, data: Record<string, unknown>) =>
        updateSubagentCard(agentId, refs.isReconnect ? { ...data, isReconnect: true } : data);
    }
    return refs;
  };

  // Per-task running total of token usage. Backend emits per-call deltas via
  // `context_window/token_usage` events; we sum here. The ref is the source
  // of truth for accumulation; SubagentData.tokenUsage is overwritten with
  // the running total on each update so the projection in ChatView reads it
  // without needing direct ref access. Reconnect during an active subagent
  // can double-count (event replay) — acceptable for a UI display surface.
  const subagentTokenUsageRef = useRef<Record<string, SubagentTokenUsage>>({});

  // During history load: queue task tool call IDs until the matching artifact 'spawned' event drains them
  const historyPendingTaskToolCallIdsRef = useRef<string[]>([]);

  /**
   * Composition-root runtime: the one per-render literal every carved
   * session/ lane consumes through its narrow port (freshness contract in
   * session/runtime.ts). Deliberately NOT memoized — render-current fields
   * (workspaceId, t, card updaters) must stay fresh.
   */
  const runtime: ChatSessionRuntime = {
    // render-current
    workspaceId,
    threadId,
    t,
    updateSubagentCard,
    updateTodoListCard,
    onWorkspaceCreated,
    streamingThreadIdRef,
    mainStreamAbortRef,
    isReconnectingOwnerRef,
    wasStoppedRef,
    sessionEpochRef,
    backgroundReconnectRef,
    setIsReconnecting,
    onFileArtifact,
    onPreviewUrl,
    // setters (stable)
    setMessages,
    queueMessages: liveMessages.queue,
    flushMessages: liveMessages.flush,
    setIsLoading,
    setIsLoadingHistory,
    setHistoryLoadFailed,
    setIsCompacting,
    setCompactionTokens,
    setMessageError,
    setFallbackSuggestion,
    setThreadModels,
    setTokenUsage,
    setReloadTrigger,
    setHasActiveSubagents,
    setPendingInterrupt,
    setThreadId,
    setWorkspaceStarting,
    setTurnHeld,
    // ref containers (stable identity, ref-current reads)
    threadIdRef,
    isStreamingRef,
    contentOrderCounterRef,
    currentReasoningIdRef,
    currentToolCallIdRef,
    currentMessageRef,
    currentRunIdRef,
    latestRunIdRef,
    latestRunEpochRef,
    steeringAtOrderRef,
    lastEventIdRef,
    pendingInterruptIdsRef,
    renderedInterruptIdsRef,
    pendingPTCBackfillRef,
    historyLoadingRef,
    historyLoadedKeyRef,
    historyHasUnresolvedInterruptRef,
    unresolvedHistoryInterruptRef,
    lastRenderedTurnIndexRef,
    newMessagesStartIndexRef,
    historyPendingTaskToolCallIdsRef,
    recentlySentTrackerRef,
    requestKeyRef,
    offloadBatchRef,
    replayedRunIdsRef,
    subagentStateRefsRef,
    subagentHistory,
    subagentProcessEventRef,
    subagentTokenUsageRef,
    terminalTaskOutcomesRef,
    pendingMuxResyncRef,
  };

  // Keep threadIdRef in sync with state (for use inside closures)
  useEffect(() => {
    threadIdRef.current = threadId;
    if (workspaceId && threadId && threadId !== '__default__') {
      setStoredThreadId(workspaceId, threadId);
    }
  }, [workspaceId, threadId]);

  // iOS Safari freezes a backgrounded tab and tears down its SSE socket; the
  // frozen reader.read() may not reject promptly on return, hanging the turn.
  // On foreground, if a main stream is genuinely active and reconnectable,
  // abort the (likely dead) reader and flag it so the stream's result handler
  // runs the existing reconnect rather than treating the abort as a user stop.
  //
  // CRITICAL: only act when the tab was ACTUALLY suspended (`pagehide`/`freeze`),
  // not on a bare `visibilitychange`. A desktop alt-tab fires visibility events
  // but keeps the socket alive — aborting there would needlessly tear down a
  // healthy stream and flash "Reconnecting…" on every refocus. The lifecycle
  // suspend events fire only when the OS froze the tab (the case the socket
  // dies), so gating on tabSuspendedRef keeps non-suspended users unaffected.
  useEffect(() => {
    const onSuspend = () => { tabSuspendedRef.current = true; };
    const onForeground = () => {
      if (typeof document === 'undefined' || document.visibilityState !== 'visible') return;
      // No genuine suspend since we were last visible → socket is still alive,
      // nothing to recover. Consume the flag so each suspend→resume cycle
      // triggers at most one reconnect (pageshow + visibilitychange both fire).
      if (!tabSuspendedRef.current) return;
      tabSuspendedRef.current = false;
      if (!isLoading || !mainStreamAbortRef.current) return;        // nothing streaming
      // Use the latched thread ref, not the threadId prop: a brand-new chat
      // keeps the prop at '__default__' until the first SSE event updates the
      // route, but Content-Location already latched the real thread into
      // threadIdRef. Keying off the prop would skip recovery for the entire
      // first-answer window (e.g. a PTC sandbox spin-up), which is the most
      // common "ask, switch apps, come back" moment.
      const tid = threadIdRef.current;
      if (!tid || tid === '__default__') return;                    // no addressable run
      if (!currentRunIdRef.current || wasStoppedRef.current) return; // not reconnectable / user stop
      backgroundReconnectRef.current = true;
      mainStreamAbortRef.current.abort();
    };
    // Suspend signals: pagehide (iOS app-background / bfcache) + freeze (Chromium
    // background-tab freeze). Both fire only on a real suspend, never on a tab-switch.
    window.addEventListener('pagehide', onSuspend);
    document.addEventListener('freeze', onSuspend);
    // Resume triggers: pageshow (bfcache restore) + visibilitychange (return to visible).
    document.addEventListener('visibilitychange', onForeground);
    window.addEventListener('pageshow', onForeground);
    return () => {
      window.removeEventListener('pagehide', onSuspend);
      document.removeEventListener('freeze', onSuspend);
      document.removeEventListener('visibilitychange', onForeground);
      window.removeEventListener('pageshow', onForeground);
    };
    // Only isLoading is read in the handler closure; the thread identity comes
    // from threadIdRef.current, not the prop, so threadId is intentionally NOT a
    // dep — including it would re-register the listeners on every thread nav.
  }, [isLoading]); // refs are stable

  // Reset thread ID when workspace or initialThreadId changes
  useEffect(() => {
    if (workspaceId) {
      // If initialThreadId is provided, use it; otherwise use localStorage
      const newThreadId = initialThreadId || getStoredThreadId(workspaceId);

      // Only update and clear if we're switching to a different thread
      // Don't clear if we're just updating from '__default__' to the actual thread ID (handled by streaming)
      const currentThreadId = threadIdRef.current;
      const isThreadSwitch = currentThreadId &&
        currentThreadId !== '__default__' &&
        newThreadId !== '__default__' &&
        currentThreadId !== newThreadId;

      if (currentThreadId !== newThreadId) {
        // Any change of the resolved id relinquishes whatever session the old
        // one owned, so the token moves here rather than inside the narrower
        // isThreadSwitch below: that predicate excludes '__default__' on either
        // side, and a workspace with no stored thread resolves to exactly that,
        // so an A → '__default__' → A trip would leave a parked reconnect retry
        // looking at its own thread with an epoch it still matches, free to
        // resume alongside the reconnect the return trip already started (two
        // readers on one run, mainStreamAbortRef pointing at whichever
        // registered last). Deliberately wider than it strictly needs to be:
        // over-invalidating only makes a stale retry give up, which is the safe
        // direction, and no retry can be running under a '__default__' id
        // anyway (the loop breaks on one immediately).
        sessionEpochRef.current += 1;
        setThreadId(newThreadId);
      }

      // Clear messages only when switching to a different existing thread
      // Preserve messages when transitioning from '__default__' to actual thread ID
      if (isThreadSwitch) {
        setMessages([]);
        setThreadModels([]);
        setTokenUsage(null);
        setFallbackSuggestion(null);
        // A mid-retry pill belongs to the thread we're leaving; without this
        // it would render over thread B until B's next content event.
        clearModelStatus();
        // A compaction + a message parked during it belong to the thread we're
        // leaving. Clear them so the isCompacting→false flush can never replay
        // thread A's queued payload into thread B (and B doesn't inherit A's
        // stale compacting indicator).
        setQueuedSend(false);
        queuedSendRef.current = null;
        // So do refused steers waiting to open its next turn, and those still
        // waiting on their answers. Their bubbles go with the messages above,
        // so nothing would ever send them: their keys are released here.
        notRunningGenRef.current += 1;
        for (const q of notRunningSendsRef.current.splice(0)) requestKeyRef.current.settle(q.requestKey);
        setIsCompacting(false);
        // A send held on the thread we're leaving is no wait of B's.
        setTurnHeld(null);
        // Reset refs
        contentOrderCounterRef.current = 0;
        currentReasoningIdRef.current = null;
        currentToolCallIdRef.current = null;
        steeringAtOrderRef.current = null;
        historyLoadingRef.current = false;
        historyLoadedKeyRef.current = null;
        newMessagesStartIndexRef.current = 0;
        // Paging belongs to the thread we're leaving, and so does its page in flight.
        historyWindow.reset();
        recentlySentTrackerRef.current.clear();
        turnCheckpointsRef.current = null;
        // The rendered-turn watermark belongs to the thread we're leaving, and
        // so does the newest run it saw; the next replay names the new one's.
        lastRenderedTurnIndexRef.current = null;
        latestRunIdRef.current = null;
        // Interrupt cards belong to the thread we're leaving; the next thread's
        // replay repopulates this from its persisted interrupt events.
        renderedInterruptIdsRef.current.clear();
        // So does the answer board. Left behind, thread A's unanswered id keeps
        // failing B's batch gate, so B's own interrupt is collected but never
        // resumed: the card holds a disabled spinner until a full reload. Its
        // only other clears sit on paths a thread switch does not take.
        answerBoard.clear();
        // And the state slot the board arms. Its only other clear sits on
        // answering, which a thread switch also does not take, so thread A's
        // leftover acts on B: a live `pendingInterrupt` disables B's composer
        // outright. A credit pause is the case that makes this ordinary rather than
        // exotic — resolving one means leaving to buy credits, so wandering off
        // to another thread with it unanswered is the expected way to meet it.
        setPendingInterrupt(null);
        // Edit text on its way to the composer is the old thread's. Its open
        // edit boxes stay with it, found by thread.
        setReturnedEdit(null);
      }
    }
    // answerBoard is created once and never replaced, so it is omitted the way
    // a ref would be; re-running this on a stable identity is not the point.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [workspaceId, initialThreadId]);

  /**
   * Gives the composer each edit of the shown thread whose message the
   * transcript no longer holds: the chat changed somewhere else and took it.
   * A box left as it opened has nothing to give. A turn older than the
   * transcript's first is not gone, only not loaded, so its box waits.
   */
  const settleEditDrafts = () => {
    const tid = threadIdRef.current;
    const open = tid ? editDrafts.of(tid) : [];
    if (open.length === 0) return;
    liveMessages.flush();
    const shown = projectTurns(liveMessages.get()).filter(({ message }) =>
      message.role === 'user' && !isSteeringUserMessage(message));
    if (shown.length === 0) return;
    // A send the server refused has no turn; the transcript's first turn is
    // its first stored message's.
    const first = shown.find(hasTurn)?.turnIndex ?? null;
    const given: string[] = [];
    for (const draft of open) {
      if (draft.turnIndex !== null && (first === null || draft.turnIndex < first)) continue;
      if (shown.some(({ message, turnIndex }) => sameDraftMessage(draft, {
        turnIndex,
        original: (message as UserMessage).content,
        messageId: message.id,
      }))) continue;
      editDrafts.drop(draft, draft.threadId);
      const text = draft.text.trim();
      if (text && text !== draft.original) given.push(text);
    }
    if (given.length === 0) return;
    setReturnedEdit((prev) => [prev, ...given].filter(Boolean).join('\n\n'));
    setMessageError(staleForkMessage('edit'));
  };

  /** Puts an edit's text back in its box. A reload in flight settles where
   *  the box goes once it shows the transcript. */
  const returnEditDraft = useStableHandler((draft: EditDraft, focus = true) => {
    if (focus) editDrafts.markFocus(draft, draft.threadId);
    editDrafts.put(draft);
    if (draft.threadId === threadIdRef.current && !historyLoadingRef.current) settleEditDrafts();
  });
  // Nothing failed and the user's text is kept, so these read as notices.
  // Stable handlers, as the recovery below is built once: each reads the
  // language shown when its refusal comes.
  const staleForkMessage = useStableHandler((kind: ForkCut['kind']): StructuredError => ({
    message: t(kind === 'edit' ? 'chat.forkStaleEdit' : 'chat.forkStaleRegenerate'),
    kind: 'notice',
  }));
  const busyRefusalMessage = useStableHandler((code: BusyCode): StructuredError => ({
    message: t(BUSY_REFUSAL_KEYS[code]),
    kind: 'notice',
  }));
  const resendMessage = useStableHandler((givenBack: boolean): StructuredError => ({
    message: t(givenBack ? 'chat.resendRequired' : 'chat.resendLastMessage'),
    kind: 'notice',
  }));
  // Reloads the thread, once a stream has let go of it.
  const requestThreadReload = () => {
    if (isStreamingRef.current) pendingMuxResyncRef.current = true;
    else setReloadTrigger((n) => n + 1);
  };

  const [forkRefusal] = useState(() => createForkRefusalRecovery({
    setMessages,
    setMessageError,
    requestReload: () => requestThreadReload(),
    sessionEpoch: () => sessionEpochRef.current,
    staleMessage: staleForkMessage,
    returnDraft: returnEditDraft,
    busyMessage: busyRefusalMessage,
    resendMessage,
    giveBack: (text) => setReturnedEdit((prev) => [prev, text].filter(Boolean).join('\n\n')),
    markPersisted: () => markTranscriptPersisted(),
  }));

  /**
   * Loads conversation history for the current workspace and thread
   * Uses the threadId from state (which should be a valid thread ID, not '__default__')
   */
  /** History replay lives in session/history/replayHistory; the hook binds
   * the runtime and the cross-lane callbacks. */
  const loadConversationHistory = (
    beforeReveal?: (ok: boolean) => void,
    latestTurnIndex?: number | null,
    live = false,
  ): Promise<boolean> => {
    const tid = threadId;
    // The newest turn the thread has: /status's count, or what this view has
    // seen when the count is missing or behind.
    const newest = Math.max(latestTurnIndex ?? -1, newestKnownTurn());
    const limit = historyWindow.reloadLimit(tid, newest);
    // A replay already running refuses this one, and owns the window.
    const token = historyLoadingRef.current ? null : historyWindow.beginReload();
    return replayConversationHistory(runtime, {
      applyFallbackSuggestion,
      loadFeedback,
      projectSubagentHistory: (byTaskId) => projectSubagentHistory(runtime, byTaskId),
      beforeReveal: (ok) => {
        beforeReveal?.(ok);
        forkRefusal.reveal(ok, live);
        // A failed reload leaves a transcript that cannot say what is gone.
        if (ok) settleEditDrafts();
      },
      limit,
      onPage: (page) => {
        if (token !== null) historyWindow.land(token, tid, page);
      },
    });
  };

  /**
   * Replay the page of turns before the oldest one loaded and put it above the
   * transcript, when the window lets one start. A page that a reload, a thread
   * switch or a fork overtook is dropped, and the trigger asks again.
   * `place` is handed the page's bubble count and the commit, which renders
   * before it returns, so the view holds the reader's place across it in one
   * task: no scroll can land between the place read and the page shown.
   */
  const loadOlderHistory = useStableHandler(async (place?: (prepended: number, commit: () => void) => void) => {
    const tid = threadIdRef.current;
    const request = historyWindow.request(tid);
    if (!request) return;
    const { beforeTurn, carry, signal, token } = request;

    let older: Awaited<ReturnType<typeof loadOlderHistoryPage>> = null;
    try {
      older = await loadOlderHistoryPage(runtime, { threadId: tid, beforeTurn, limit: HISTORY_PAGE_TURNS, signal }, carry);
    } catch (err) {
      console.warn('[History] Older page failed to load:', err);
    }
    if (!historyWindow.isCurrent(token)) return;
    if (!older) {
      historyWindow.fail(token);
      return;
    }

    const landed = older;
    const commit = () => flushSync(() => commitOlderHistoryPage(
      runtime,
      {
        projectSubagentHistory: (byTaskId) => projectSubagentHistory(runtime, byTaskId),
        prependSubagentRuns: (agentId, runs) => prependSubagentRuns(runtime, agentId, runs),
        isTaskLive: (agentId) => muxOpenTaskIds().has(agentId.replace(/^task:/, '')),
        liveOutcome: (agentId) => terminalTaskOutcomesRef.current.get(agentId.replace(/^task:/, '')),
      },
      landed,
    ));
    if (place) place(landed.messages.length, commit);
    else commit();
    historyWindow.land(token, tid, landed.page);
  });

  /** Recovery/ownership lifecycle lives in session/stream/lifecycle; the hook
   * binds the runtime and the composition-level recovery callbacks. */
  const reconnectToStream = (opts?: ReconnectOptions) =>
    reconnectToStreamImpl(runtime, recoveryDeps, opts);

  // Expose the latest reader to the report-back watch (wired via a ref up top to
  // break the render-time cycle described at the useReportBackWatch call site).
  reconnectToStreamRef.current = reconnectToStream;

  const attemptReconnectAfterDisconnect = (assistantMessageId: string) =>
    attemptReconnectImpl(runtime, recoveryDeps, assistantMessageId);

  /**
   * v4 request_key dedup: a 409 `duplicate_request` means an earlier copy of
   * this logical send was already accepted and only its response was lost.
   * Adopt the existing run — latch its thread/run ids and reconnect (a live
   * run replays its stream; a settled one falls through to the history
   * reload) — instead of surfacing an error banner for a turn that exists.
   * Returns true when adopted; the caller must skip its own error/finalize
   * path (the reconnect owns teardown from here).
   */
  const adoptDuplicateRun = (err: unknown, assistantMessageId: string, requestKey: string): boolean => {
    const e = err as { status?: number; errorInfo?: Record<string, unknown> };
    if (e?.status !== 409 || e?.errorInfo?.code !== 'duplicate_request') return false;
    const runId = e.errorInfo.run_id as string | undefined;
    const dupThreadId = e.errorInfo.thread_id as string | undefined;
    // Run identity not disclosed (the key belongs to another user's run —
    // shouldn't happen for an honest client): fall through to a plain error.
    if (!runId || !dupThreadId) return false;
    requestKeyRef.current.settle(requestKey); // consumed by the accepted copy
    // A new chat's copy may have made the thread: the reload, the route and
    // the next send all read the state.
    threadIdRef.current = dupThreadId;
    setThreadId(dupThreadId);
    latchRun(runId);
    // A settled copy has no stream to join, so the reconnect gives up and
    // reloads. The server holds that whole turn, so the reload has to replace
    // the bubbles this send drew: kept, they sit below the turn it renders.
    const runStatus = e.errorInfo.run_status;
    if (typeof runStatus === 'string' && runStatus !== 'in_progress') markTranscriptPersisted();
    attemptReconnectAfterDisconnect(assistantMessageId);
    return true;
  };

  /**
   * A copy of this request did arrive and its run failed, so this click asked
   * for that run's Retry: it goes out at once, by the run's id, in place of a
   * reply that would only show the failure. Any other run is adopted as it
   * is, a stopped one too, since only a failed run can be retried.
   */
  const retryFailedDuplicate = (err: unknown, assistantMessageId: string, request: TurnRequest, modelOptions: ModelOptions, requestKey: string): boolean => {
    const e = err as { status?: number; errorInfo?: Record<string, unknown> };
    if (e?.status !== 409 || e?.errorInfo?.code !== 'duplicate_request' || e.errorInfo.run_status !== 'error') return false;
    // A run the copy steered into is another message's turn.
    if (e.errorInfo.steered === true) return false;
    const runId = e.errorInfo.run_id;
    if (typeof runId !== 'string' || !runId || e.errorInfo.thread_id !== threadIdRef.current) return false;
    requestKeyRef.current.settle(requestKey);
    noteLiveRun({ latestRunIdRef, latestRunEpochRef }, runId);
    const transcript = liveMessages.get();
    const at = transcript.findIndex((m) => m.id === assistantMessageId);
    void streamFromCheckpoint(retryRequest(runId, request.additionalContext), at === -1 ? transcript.length : at, transcript, modelOptions);
    return true;
  };

  /**
   * A steer the server answered with `duplicate_request`: a copy of it under
   * the same request key reached a run first, and only that copy's answer was
   * lost. The server holds the message once, so one bubble shows it: the
   * steer a delivery already marked, else the copy still waiting, else the
   * lost copy, back to waiting, else a delivered copy in its turn. With none
   * of those on screen this steer waits for the delivery itself. A run this
   * view is not following delivers it to a reload instead. A copy is a
   * bubble sent under the same key; one with no key, which a delivery or a
   * replay drew, counts only as the delivered copy. Another send of the same
   * words is its own message, and so is its key.
   */
  const settleDuplicateSteer = (err: unknown, steerId: string, message: string, requestKey: string): boolean => {
    const e = err as { status?: number; errorInfo?: Record<string, unknown> };
    // A copy of another user's names no run: shown as the refusal it is.
    if (e?.status !== 409 || e?.errorInfo?.code !== 'duplicate_request' || !e.errorInfo.run_id) return false;
    requestKeyRef.current.settle(requestKey);
    if (e.errorInfo.steered !== true) {
      // A copy of this send started the turn the answer names, as its
      // question, and only that copy's answer was lost, so the steer is no
      // second message. A turn this view is not following shows on a reload.
      setMessages((prev) => prev.filter((m) => m.id !== steerId));
      if (e.errorInfo.run_id !== currentRunIdRef.current) pendingMuxResyncRef.current = true;
      return true;
    }
    type Steer = UserMessage & { queueError?: unknown };
    const asSteer = (m: ChatMessage | undefined): Steer | null => (m?.role === 'user' ? m as Steer : null);
    const text = message.trim();
    let dropped: unknown = null;
    let held = false;
    setMessages((prev) => {
      const steer = asSteer(prev.find((m) => m.id === steerId));
      const copies = prev.flatMap((m, i) => {
        const copy = asSteer(m);
        if (!copy || copy.id === steerId) return [];
        if (copy.requestKey) return copy.requestKey === requestKey ? [{ m: copy, i, keyed: true }] : [];
        return String(copy.content ?? '').trim() === text ? [{ m: copy, i, keyed: false }] : [];
      });
      const lost = copies.filter(({ m, keyed }) => keyed && !m.isHistory && !!m.queueError && !m.queueRefused).pop();
      const waiting = copies.find(({ m, keyed }) => keyed && !m.isHistory && m.steering);
      const delivered = copies.find(({ m }) => m.steeringDelivered && m.turnIndex === steer?.turnIndex);
      let next = prev;
      if (steer?.steeringDelivered || waiting || (!lost && delivered)) {
        // This steer, or a copy already showing the message, stands for it.
        if (!steer?.steeringDelivered) next = next.filter((m) => m.id !== steerId);
        if (lost) next = next.filter((m) => m !== lost.m);
      } else if (lost) {
        const back: Steer = { ...lost.m, steering: true };
        delete back.queueError;
        delete back.queueRefused;
        next = prev.flatMap((m, i) => (i === lost.i ? [back] : m.id === steerId ? [] : [m]));
      }
      if (lost && next !== prev) {
        dropped = lost.m.queueError;
        held = next.some((m) => asSteer(m)?.queueError === dropped);
      }
      return next;
    });
    if (dropped !== null && !held) {
      setMessageError((prev) => (prev === dropped ? null : prev));
      if (refusalAfterReloadRef.current?.banner === dropped) refusalAfterReloadRef.current = null;
    }
    if (e.errorInfo.run_id !== currentRunIdRef.current || e.errorInfo.run_status !== 'in_progress') {
      pendingMuxResyncRef.current = true;
    }
    return true;
  };

  /**
   * A send the server answered `duplicate_request` with `steered`: a copy of
   * it under the same request key went into another turn as a steer, and only
   * that copy's answer was lost. That run is the other message's turn, so it
   * is neither retried nor followed as this send's: the message shows once,
   * as the steer it was, settled as a steer's duplicate is. The send's reply
   * goes, with the turn it counted, and so do the bubbles of earlier tries
   * that lost their answer, whose Retry would send it again. No stream here
   * follows that run, so a reload shows it.
   */
  const settleSteeredSend = (err: unknown, send: { userId: string; assistantId: string; turnIndex: number; watermarkBefore: number | null; requestKey: string }): boolean => {
    const e = err as { status?: number; errorInfo?: Record<string, unknown> };
    const runId = e?.errorInfo?.run_id;
    if (e?.status !== 409 || e.errorInfo?.code !== 'duplicate_request' || e.errorInfo.steered !== true) return false;
    if (typeof runId !== 'string' || !runId) return false;
    const tries = new Set<string>();
    for (const [id, held] of heldRetriesRef.current) {
      if (held.request.requestKey !== send.requestKey) continue;
      heldRetriesRef.current.delete(id);
      tries.add(held.bubbleId);
    }
    let text = '';
    setMessages((prev) => prev.flatMap<ChatMessage>((m, i) => {
      if (m.id === send.assistantId || tries.has(m.id as string)) return [];
      if (m.role !== 'user') return [m];
      // A try's message sits right above its reply.
      if (m.unadmitted && tries.has(prev[i + 1]?.id as string)) return [];
      if (m.id !== send.userId) return [m];
      text = String(m.content ?? '');
      const steer: UserMessage = { ...m, steering: true, steeredRunId: runId, requestKey: send.requestKey };
      delete steer.unadmitted;
      return [steer];
    }));
    if (lastRenderedTurnIndexRef.current === send.turnIndex) lastRenderedTurnIndexRef.current = send.watermarkBefore;
    settleDuplicateSteer(err, send.userId, text, send.requestKey);
    pendingMuxResyncRef.current = true;
    return true;
  };

  // Load history when workspace or threadId changes, then check for reconnection
  useEffect(() => {
    const reloadAsked = reloadTrigger !== reloadSeenRef.current;
    reloadSeenRef.current = reloadTrigger;
    // A reconnect stream is live on a DIFFERENT thread than the one we're now
    // loading, e.g. a report-back is streaming on the Chief of Staff thread and
    // the user clicked the dispatch card to jump into the running PTC thread.
    // Without this, the isStreamingRef guard below would skip the PTC load and
    // it would appear blank (the report-back stream "holds" the global
    // streaming flag). Supersede that stream: abort it (it continues server-side and replays on
    // return) so THIS thread can load and reconnect. The aborted stream's finally
    // is a no-op now (its `stillActive` check fails — see reconnectToStream).
    //
    // We do NOT stop the report-back watch here. Superseding the visible
    // reader (so the PTC thread can take the slot) must not erase the independent
    // pending report-back: the keyed watch persists, holds its run ids, and
    // renders again when the user returns to the Chief of Staff thread.
    const supersedeOtherThreadStream =
      isStreamingRef.current &&
      streamingThreadIdRef.current !== null &&
      // A '__default__' owner is a new-conversation send whose id hasn't resolved
      // yet; its prop transitions '__default__' → realTid, which must NOT supersede
      // its own in-flight stream. The isStreamingRef guard below skips the
      // redundant load instead.
      streamingThreadIdRef.current !== '__default__' &&
      streamingThreadIdRef.current !== threadId &&
      !!threadId &&
      threadId !== '__default__';
    if (supersedeOtherThreadStream) {
      mainStreamAbortRef.current?.abort();
      mainStreamAbortRef.current = null;
      releaseStreamOwnership();
    }

    // Guard: Only load if we have a workspaceId and a valid threadId (not '__default__')
    if (!workspaceId || !threadId || threadId === '__default__' || historyLoadingRef.current) {
      return;
    }
    // Skip while streaming (prevents race condition when thread ID changes
    // during streaming). A reload asked for meanwhile runs when the stream
    // releases, since the stream shows only its own run, and one asked for
    // while a steer waits on its answer runs once that answer settles.
    if (isStreamingRef.current || (reloadAsked && steersInFlightRef.current.size > 0)) {
      if (reloadAsked) pendingMuxResyncRef.current = true;
      return;
    }

    // Idempotency guard: skip if we already loaded for this exact
    // (workspace, thread, reloadTrigger) tuple. See historyLoadedKeyRef
    // declaration for the failure mode this prevents (duplicate
    // history-assistant bubbles after a stream completes).
    const loadKey = `${workspaceId}::${threadId}::${reloadTrigger}`;
    if (historyLoadedKeyRef.current === loadKey) {
      return;
    }

    let cancelled = false;

    const loadAndMaybeReconnect = async () => {
      // Check workflow status FIRST, then load history.
      // Sequential order avoids a race where /replay lands before the backend
      // persists Turn N (on_background_workflow_complete) while /status already
      // sees COMPLETED — which would cause the frontend to skip reconnect and
      // miss the latest turn's events entirely.
      // Snapshot moment = the client's knowledge horizon: everything below
      // (active_tasks above all) reflects the world at this instant, and the
      // mux attach may lag it by the whole history load.
      const snapshotAtMs = Date.now();
      const onlineEpochAtStart = onlineRetryEpochRef.current;
      // The composer is open while this load reads the status and after its
      // replay, and a turn sent then owns the transcript: this load's snapshot
      // predates it, so replaying or reconnecting would run over that turn, or
      // reconnect to a run it named that has since ended. The load runs again
      // after the turn, when its stream releases or at once if it already has.
      const sessionEpochAtStart = sessionEpochRef.current;
      const turnTookOver = () => {
        if (sessionEpochRef.current === sessionEpochAtStart) return false;
        if (isStreamingRef.current) pendingMuxResyncRef.current = true;
        else setReloadTrigger((n) => n + 1);
        return true;
      };
      const status: WorkflowStatusResponse = await getWorkflowStatus(threadId).catch((statusErr: unknown) => {
        console.log('[Reconnect] Could not check workflow status:', (statusErr as Error).message);
        return { can_reconnect: false, status: 'error' } as WorkflowStatusResponse;
      });

      if (cancelled || turnTookOver()) return;

      // The token totals restart from what the server counted, which is every
      // run but the one this status names live. No stream runs here, and the
      // reconnect below replays that run from its start onto them, so a call
      // this view already added is either in the count or about to come again.
      // Without a count they restart from zero and the replay sums what loads.
      const counted = status.token_totals;
      setTokenUsage((prev) => (counted ? withServerTotals(prev, counted) : withoutServerTotals(prev)));

      // Capture share status from workflow status response
      if (status.is_shared !== undefined) {
        setIsShared(status.is_shared);
      }

      // A run that is over leaves its todos as its last write said: mark the
      // unfinished ones stale in the commit that reveals the transcript, since
      // a step later the drawer first showed a task in progress, then shrank.
      // The tests are the branches' below: the run settles here only when no
      // reconnect, paused interrupt, running subagent or re-fire follows. No
      // blanket completion of inline subagent cards: each is born with its real
      // status from the replayed task-artifact stamp, and one would clobber a
      // legitimately 'cancelled' card.
      const pendingSteers = (status.pending_steers ?? []).filter((p) => p.content);
      const pendingKeys = new Set(pendingSteers.map((p) => p.request_key).filter((key): key is string => !!key));
      const settleEndedRun = (ok: boolean) => {
        if (cancelled || sessionEpochRef.current !== sessionEpochAtStart) return;
        // A steer a give-up kept queued, in a run that ended without the
        // replay delivering it, is queued no more unless the status still
        // lists it: the server made it a turn of its own, which shows when it
        // runs, or a stop handed it back to the tab that stopped.
        if (ok && !status.can_reconnect && status.status !== 'error') {
          const queued = new Set(liveMessages.get()
            .filter((m) => m.role === 'user' && !m.isHistory && (m as { steering?: boolean }).steering
              && !pendingKeys.has((m as { requestKey?: string }).requestKey ?? ''))
            .map((m) => m.id));
          if (queued.size > 0) setMessages((prev) => prev.filter((m) => !queued.has(m.id)));
        }
        // A steer the status lists waits on the server, which delivers it by
        // its key or starts a turn of its own with it, whose replay is then its
        // only copy. So this view's copy becomes the status's, which a reload
        // clears and draws again while the steer still waits.
        if (ok && pendingKeys.size > 0) {
          const listed = (m: ChatMessage) => m.role === 'user' && !m.isHistory && !!(m as { steering?: boolean }).steering
            && pendingKeys.has((m as { requestKey?: string }).requestKey ?? '');
          setMessages((prev) => (prev.some(listed) ? prev.map((m) => (listed(m) ? { ...m, isHistory: true } : m)) : prev));
        }
        // The steers an errored turn left on the server wait under it for the
        // next turn, which reads them and delivers them by their keys, and so
        // do those queued in a live run it has yet to read. The status is
        // their only record until then, so each load draws them again; a
        // bubble of this view's under the same key already does.
        if (ok && pendingSteers.length > 0) {
          const turn = typeof status.latest_turn_index === 'number' ? status.latest_turn_index : lastRenderedTurnIndexRef.current;
          setMessages((prev) => {
            const shown = new Set(prev.map((m) => (m as { requestKey?: string }).requestKey).filter(Boolean));
            const rows = pendingSteers
              .filter((p) => !p.request_key || !shown.has(p.request_key))
              .map((p, i): UserMessage => ({
                id: `pending-steer-${p.request_key ?? i}`,
                role: 'user',
                content: p.content,
                contentType: 'text',
                timestamp: p.timestamp ? new Date(p.timestamp * 1000) : new Date(),
                isStreaming: false,
                isHistory: true,
                steering: true,
                ...(turn !== null && turn >= 0 ? { turnIndex: turn } : {}),
                ...(p.request_key ? { requestKey: p.request_key } : {}),
              }));
            if (rows.length === 0) return prev;
            // A live run's go below the reply it is writing, which the
            // reconnect puts above them, as the run reads them after it.
            const at = status.can_reconnect ? prev.length : Math.min(newMessagesStartIndexRef.current, prev.length);
            if (newMessagesStartIndexRef.current >= at) newMessagesStartIndexRef.current = at + rows.length;
            return [...prev.slice(0, at), ...rows, ...prev.slice(at)];
          });
        }
        if (onlineRetryEpochRef.current !== onlineEpochAtStart && (!ok || status.status === 'error')) return;
        if (status.can_reconnect || historyHasUnresolvedInterruptRef.current) return;
        if ((status.active_tasks || []).some((t) => !isSettledTask(t))) return;
        if (finalizePendingTodos) finalizePendingTodos();
        setMessages((prev) => finalizeTodoListProcessesInMessages(prev));
      };
      // A steer a reload kept was stamped from this view's count when it was
      // sent, which may have been behind. The replay names no run for a live
      // turn, so the status does: the turn of the run it names live is the
      // newest it counts, and the replay places the steer under it. Forward
      // only: this view's stamp is never ahead of the run, and the status's
      // count can itself be behind, by a turn admitted with no question of
      // its own.
      const liveRun = status.can_reconnect ? status.run_id : null;
      const liveTurn = status.latest_turn_index;
      if (liveRun && typeof liveTurn === 'number') {
        const behind = (m: ChatMessage): m is UserMessage => (
          m.role === 'user' && !m.isHistory && m.steeredRunId === liveRun && (m.turnIndex ?? -1) < liveTurn
        );
        setMessages((prev) => (prev.some(behind) ? prev.map((m) => (behind(m) ? { ...m, turnIndex: liveTurn } : m)) : prev));
      }
      const loadOk = await loadConversationHistory(settleEndedRun, status.latest_turn_index, status.can_reconnect === true);

      if (cancelled || turnTookOver()) return;

      // Only mark the (workspace, thread, reloadTrigger) tuple as loaded
      // when the load actually succeeded. On transient errors the key
      // stays clear so a manual `setReloadTrigger(n => n + 1)` retry will
      // re-fire the effect; otherwise the user would be stuck on a partial
      // view until they switch threads. Set AFTER the cancellation check
      // so an in-flight load that's been cancelled doesn't lock out the
      // eventual real load that supersedes it.
      if (loadOk) {
        historyLoadedKeyRef.current = loadKey;
        // A reload catches up on turns this view missed, a wake among them,
        // and the wakeups those turns fired or set; a first load is the
        // list's own seed.
        if (reloadAsked) {
          refreshWakeups();
          refreshCommands();
        }
        const refused = refusalAfterReloadRef.current;
        refusalAfterReloadRef.current = null;
        // Only while a steer still holds it: one the replay delivered is gone.
        if (refused?.epoch === sessionEpochAtStart && liveMessages.get().some((m) => (m as { queueError?: unknown }).queueError === refused.banner)) {
          setMessageError(refused.banner);
        }
        // The replay rendered every persisted turn, so this load's recents slice
        // is now ON SCREEN. Record it BEFORE arming the watch below, or the
        // recent-runs catch-up would re-attach each run as a duplicate turn.
        // The replay's own terminal-run ids are recorded too: a run in the
        // post-finalize/pre-ack outbox window is persisted (and just rendered)
        // but not yet in recents, while /status still names it — without this,
        // the arm's seed or a latched wake re-attaches it as a duplicate.
        reportBackWatch.markRunsRendered([
          ...(status.recent_report_back_run_ids ?? []),
          ...replayedRunIdsRef.current,
        ]);
      }

      // The link can come back mid-load, and the `online` that would normally
      // re-fire this was dropped by the in-flight guard on the listener below —
      // nothing else will, because a link that returns does not fire `online` a
      // second time. Two shapes need the re-fire. A failed replay is the
      // obvious one. The other is subtler: the status probe and the replay are
      // deliberately sequential, so the link can return BETWEEN them, leaving a
      // probe that failed against the dead link (`status: 'error'`, the
      // client-only sentinel synthesized by the catch above) paired with a
      // replay that succeeded against the live one. Trusting that snapshot's
      // can_reconnect:false would leave an active turn unattached until the
      // next navigation. Re-running is safe either way: the replay strips its
      // own history bubbles before rebuilding, so it is not additive.
      if (
        onlineRetryEpochRef.current !== onlineEpochAtStart &&
        (!loadOk || status.status === 'error')
      ) {
        setReloadTrigger((n) => n + 1);
        return;
      }

      // Arm the report-back watch BEFORE the reconnect branch below, so it runs
      // even when this load also reconnects to an active run (a refresh right as
      // the report-back becomes due can report both; reconnecting to
      // status.run_id alone can miss the report-back run). The watch stays
      // dormant while a reconnect stream is live and attach() skips the run
      // already on screen, so this never double-streams.
      if (shouldArmForStatus(status)) {
        if (import.meta.env.DEV) console.log('[ReportBack] Pending report-back detected on load, opening watch');
        // Key the watch to THIS thread (pending_report_back is a dispatching-thread
        // property) and seed the run /status already named. Poke a catch-up
        // reconcile ONLY when the thread isn't also active — poking would race
        // the reconnect branch's attach of status.run_id (double-attach); the
        // seeded id is picked up once that active stream ends.
        reportBackWatch.arm(
          threadId, status.report_back_run_id, status.can_reconnect ? null : 'load', status, reportOwedForStatus(status),
        );
      }

      // The report-back watch attaches its runs on a wake, which can land
      // while this load reads; a second reader would stream the run twice.
      const watchStreamsRun =
        status.can_reconnect && isStreamingRef.current && currentRunIdRef.current === status.run_id;
      if (watchStreamsRun && historyHasUnresolvedInterruptRef.current) {
        // That stream delivers the resolution. Left set, these would make a
        // later reconnect strip the replayed interrupt cards.
        historyHasUnresolvedInterruptRef.current = false;
        unresolvedHistoryInterruptRef.current = [];
      }

      if (historyHasUnresolvedInterruptRef.current && status.can_reconnect) {
        // Workflow is active → interrupt was answered, reconnect will deliver resolution
        console.log('[Reconnect] Unresolved interrupt from history, reconnecting to get resolution events');
        historyHasUnresolvedInterruptRef.current = false;
        await reconnectToStream({ activeTasks: status.active_tasks || [], runId: status.run_id ?? null, resetCursor: true, snapshotAtMs });
        unresolvedHistoryInterruptRef.current = [];
      } else if (historyHasUnresolvedInterruptRef.current && !status.can_reconnect) {
        // Workflow genuinely paused → make interrupt(s) interactive
        // An approval arms only where its verdict has somewhere to land. Order
        // governance is the one middleware that consumes one and it reads its
        // decisions by attempt id, so an approval naming no attempt would take
        // a click that resumes the graph with the verdict unread and let the
        // call run anyway. Arming it is worse than leaving it inert.
        const intInfos = unresolvedHistoryInterruptRef.current.filter(
          (info) => info.type !== 'tool_approval' || info.target?.kind === 'attempt',
        );
        if (intInfos.length > 0) {
          console.log('[Reconnect] Workflow paused, making', intInfos.length, 'interrupt(s) interactive:', intInfos.map((p) => p.type));

          // Arm the board so answer/skip handlers can collect and batch-resume
          answerBoard.arm(intInfos.map((info) => info.interruptId));

          // The slot is single, so it takes the first. Each queued family fills
          // exactly one of questionId and proposalId, so one shape restores all.
          const { type, interruptId, assistantMessageId, questionId, proposalId } = intInfos[0];
          setPendingInterrupt({ type, interruptId, assistantMessageId, questionId, proposalId });
        }
        unresolvedHistoryInterruptRef.current = [];
        historyHasUnresolvedInterruptRef.current = false;
      } else if (status.can_reconnect && !watchStreamsRun) {
        console.log('[Reconnect] Workflow status:', status.status, 'can_reconnect:', status.can_reconnect, 'active_tasks:', status.active_tasks);
        await reconnectToStream({ activeTasks: status.active_tasks || [], runId: status.run_id ?? null, resetCursor: true, snapshotAtMs });
      } else if ((status.active_tasks || []).some((t) => !isSettledTask(t))) {
        // Main workflow completed, or the report-back watch streams it, but
        // subagent tasks still running; this load's cleanup detached the mux.
        // Attach the thread mux so cards stay live after refresh. Tasks the
        // stale /status snapshot calls active but that are already settled
        // (terminal in history, or seen closing live) are never re-activated —
        // the mux would have no closure to send for them.
        const muxTasks = (status.active_tasks || []).filter((t) => !isSettledTask(t));
        console.log('[Reconnect] Main workflow done, attaching mux for active subagents:', muxTasks);
        const dummyAssistantId = `assistant-subagent-reconnect-${Date.now()}`;
        const refs = buildStreamRefs({ isReconnect: true });
        const processEvent = createStreamEventProcessor(runtime, streamRouterDeps, dummyAssistantId, refs, getTaskIdFromEvent);
        // Pre-seed cards from history so per-task events don't create empty cards
        for (const taskId of muxTasks) {
          const agentId = `task:${taskId}`;
          const historyData = subagentHistory.get().entries[agentId];
          if (updateSubagentCard && historyData) {
            subagentTokenUsageRef.current[agentId] = historyData.tokenUsage ?? ZERO_USAGE;
            updateSubagentCard(agentId, {
              agentId,
              displayId: `Task-${taskId}`,
              taskId: agentId,
              description: historyData.description || '',
              prompt: historyData.prompt || historyData.description || '',
              type: historyData.type || 'general-purpose',
              tokenUsage: historyData.tokenUsage ?? ZERO_USAGE,
              status: 'active',
              isActive: true,
              isReconnect: true,
            });
          }
        }
        attachSubagentMux(threadId, processEvent, snapshotAtMs);
        setHasActiveSubagents(true);
      } else {
        // Usually a no-op, the reveal having settled it. Again here because the
        // feedback fetch after the reveal can settle the last running subagent
        // or clear the interrupt, and then only this pass finds the run over.
        settleEndedRun(loadOk);
      }
    };

    const load = {};
    const endLoad = () => {
      if (threadLoadingRef.current !== load) return;
      threadLoadingRef.current = null;
      setIsLoadingThread(false);
    };
    threadLoadingRef.current = load;
    setIsLoadingThread(true);
    void loadAndMaybeReconnect().finally(endLoad);

    // Cleanup: Cancel loading if workspace or thread changes or component unmounts.
    // The report-back watch is deliberately NOT torn down here — it is keyed to its
    // dispatching thread and must survive navigation into the dispatched PTC thread (a
    // dedicated unmount-only effect stops it when the component truly goes away).
    return () => {
      cancelled = true;
      historyLoadingRef.current = false;
      endLoad();
      // Thread switch/unmount: tear the mux down without marking anything
      // completed, and drop the processor so no stale closure can fire.
      if (threadId) peekThreadMux(threadId)?.detach();
      subagentProcessEventRef.current = null;
      subagentStateRefsRef.current = {};
    };
    // Note: loadConversationHistory is not in deps because it uses workspaceId and threadId from closure
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [workspaceId, threadId, reloadTrigger]);

  // Opening a thread while offline fails its history load and tells the user it
  // will catch up when the link returns. Nothing else makes that true: the
  // banner clearing is the only other reaction to `online`, so without this the
  // conversation stays stale until the user navigates away and back. The load
  // effect deliberately leaves historyLoadedKeyRef clear on a failed load
  // precisely so a reloadTrigger bump re-fires it.
  useEffect(() => {
    const retryFailedLoad = () => {
      // Record the arrival before any guard can drop it — a load already in
      // flight consumes this on its way out if it fails.
      onlineRetryEpochRef.current += 1;
      if (!workspaceId || !threadId || threadId === '__default__') return;
      // A live stream owns the catch-up: the reconnect loop is itself waiting
      // on this same `online`, and a history reload would fight it.
      if (historyLoadingRef.current || isStreamingRef.current) return;
      // A matching key means the last load succeeded — there is nothing stale
      // to catch up, and re-firing would replay the thread for no reason.
      if (historyLoadedKeyRef.current === `${workspaceId}::${threadId}::${reloadTrigger}`) return;
      setReloadTrigger((n) => n + 1);
    };
    window.addEventListener('online', retryFailedLoad);
    return () => window.removeEventListener('online', retryFailedLoad);
  }, [workspaceId, threadId, reloadTrigger]);

  // The report-back watch survives thread navigation (so a report-back and a
  // live PTC stream coexist); useReportBackWatch owns its own unmount-time
  // teardown, so the thread-load effect above deliberately doesn't touch it.

  // Leaving ChatAgent entirely gives up the session the same way navigating
  // between threads does, but nothing above notices: the route changes without
  // the resolved thread id changing, so a reconnect retry parked in an offline
  // slice would still match every ownership check and open a reader into a hook
  // nobody is rendering. Coming back mounts a fresh hook that reconnects on its
  // own, and Stop reaches only that one, so the orphan runs to the end of the
  // turn unabortable. Its own effect rather than the thread-load cleanup above,
  // which also fires on every reloadTrigger bump and would abandon retries that
  // are still legitimately ours.
  useEffect(() => () => {
    sessionEpochRef.current += 1;
    historyWindow.reset();
  }, [historyWindow]);

  /**
   * Subagent mux settlement (sink, positive per-task closure, drain dedup)
   * lives in session/subagents/muxSink — the domain owns the monotonicity
   * contract; the hook wires in the two cross-lane callbacks.
   */
  const { isSettledTask, attachSubagentMux } = createSubagentMuxController(runtime, {
    muxOpenTaskIds,
    armReportBackWatch,
    hydrateTaskTranscript,
  });

  const cleanupAfterStreamEnd = (assistantMessageId: string) =>
    cleanupAfterStreamEndImpl(runtime, recoveryDeps, assistantMessageId);

  /**
   * Synthesizes terminal events for a stopped turn and dispatches them through
   * the EXISTING handler pipeline so the open streaming structures close the
   * same way a server-driven terminal event would (no hand-rolled state
   * surgery). Aborting the reader means no server terminal event arrives, so
   * without this the open blocks would render "thinking"/half-streamed forever.
   *
   * Closes the main message through settleStoppedBubble, the same close a
   * cancel from elsewhere gets from its `run_end`. Then closes every active
   * subagent card's open reasoning + marks its last streaming message complete.
   *
   * Idempotent: guarded by wasStoppedRef so a double-stop (or re-entry) is a
   * no-op and synthetic closes can't append twice.
   */
  const finalizeStreamingMessage = (assistantMessageId: string) => {
    if (wasStoppedRef.current) return;
    wasStoppedRef.current = true;

    const refs = {
      contentOrderCounterRef,
      currentReasoningIdRef,
      currentToolCallIdRef,
      steeringAtOrderRef,
      subagentStateRefs: subagentStateRefsRef.current,
    };

    settleStoppedBubble({ assistantMessageId, refs, setMessages: setMessagesForHandlers });

    // Finalize each active subagent card: close its open reasoning block and
    // mark its last streaming message complete. Per-task state lives in
    // subagentStateRefsRef, separate from the main message refs.
    const activeShortIds = muxOpenTaskIds();
    for (const shortId of activeShortIds) {
      const agentId = `task:${shortId}`;
      const taskRefs = subagentStateRefsRef.current[agentId];
      if (!taskRefs) continue;
      // Close an open subagent reasoning block.
      if (taskRefs.currentReasoningIdRef.current) {
        const reasoningId = taskRefs.currentReasoningIdRef.current;
        const msgs = [...taskRefs.messages];
        for (let i = msgs.length - 1; i >= 0; i--) {
          const rp = (msgs[i].reasoningProcesses as Record<string, Record<string, unknown>>) || {};
          if (rp[reasoningId]) {
            const next = { ...rp };
            const completedAt = Date.now();
            const startedAt = next[reasoningId]._startedAt as number | undefined;
            next[reasoningId] = {
              ...next[reasoningId],
              isReasoning: false,
              reasoningComplete: true,
              _completedAt: completedAt,
              // The main transcript's close stamps a duration on the same click.
              elapsedMs: (next[reasoningId].elapsedMs as number | undefined)
                ?? (startedAt ? completedAt - startedAt : undefined),
            };
            msgs[i] = { ...msgs[i], reasoningProcesses: next };
            break;
          }
        }
        taskRefs.messages = msgs;
        taskRefs.currentReasoningIdRef.current = null;
      }
      // Mark the subagent's last streaming message complete + stopped, clear any
      // in-flight tool-call chunks so its preparing row stops shimmering, and
      // fold still-in-progress tool rows (same finalize as the main message).
      const msgs = [...taskRefs.messages];
      for (let i = msgs.length - 1; i >= 0; i--) {
        if (msgs[i].role === 'assistant' && msgs[i].isStreaming) {
          const tcp = (msgs[i].toolCallProcesses as Record<string, Record<string, unknown>>) || {};
          const tp: Record<string, Record<string, unknown>> = {};
          for (const [id, val] of Object.entries(tcp)) {
            tp[id] = val.isInProgress ? { ...val, isInProgress: false, isComplete: true } : val;
          }
          msgs[i] = { ...withoutPending(msgs[i]), isStreaming: false, stopped: true, toolCallProcesses: tp };
          break;
        }
      }
      taskRefs.messages = msgs;
      if (updateSubagentCard) {
        // A user Stop is a cancellation, not a completion — stamping
        // 'completed' would launder the stop as success in the card header.
        updateSubagentCard(agentId, { messages: taskRefs.messages, status: 'cancelled', isActive: false });
      }
    }
  };

  // Forget a message parked during compaction (clear ref + chip + optimistic shimmer bubble).
  const dropQueuedSend = () => {
    const queuedMsgId = queuedSendRef.current?.messageId;
    queuedSendRef.current = null;
    setQueuedSend(false);
    if (queuedMsgId) {
      setMessages((prev) => prev.filter((m) => m.id !== queuedMsgId));
    }
  };

  /**
   * Hard stop: terminates the current turn immediately while preserving state.
   * (a) aborts the main reader (stop feels instant); (b) finalizes the open
   * message to a stopped state + clears loading + active-subagent flag;
   * (c) aborts per-task subagent streams + the report-back watch; (d) fires
   * POST /cancel with one retry, then an error toast on failure so a diverged
   * UI/backend state is visible. Double-stop is a no-op (wasStoppedRef guard).
   */
  const stopWorkflow = async () => {
    const tid = threadIdRef.current;
    // Capture the run we're stopping NOW, before any await. If the cancel POST
    // is slow and the user sends a new turn before the retry fires, this keeps
    // the retry pinned to the stopped run instead of cancelling the new one.
    const stoppedRunId = currentRunIdRef.current;
    if (wasStoppedRef.current) return; // double-click stop is idempotent
    // Steers waiting to open the next turn were never sent anywhere: they go
    // back to the composer with the ones the stop takes back from the run.
    const unsent = notRunningSendsRef.current.splice(0);

    // (a) Abort the main reader NOW so the stop feels instant. The aborted
    // stream resolves with { aborted: true } and the send finally skips
    // cleanup (it checks wasStoppedRef), so we own the teardown here.
    mainStreamAbortRef.current?.abort();
    mainStreamAbortRef.current = null;

    // (b) Finalize the open message (closes reasoning/tool/artifact + stopped
    // chip) and clear loading + the active-subagent indicator. finalizeStreaming-
    // Message sets wasStoppedRef, so this whole block runs at most once.
    const finalId = currentMessageRef.current;
    if (finalId) {
      finalizeStreamingMessage(finalId);
    } else {
      wasStoppedRef.current = true;
    }
    setIsLoading(false);
    setHasActiveSubagents(false);
    // Stopping mid-bringup or mid-compaction must clear these too — otherwise a
    // stuck "starting sandbox" / "compacting" indicator outlives the stop.
    // cleanupAfterStreamEnd resets them, but the stop path skips that cleanup.
    // (isReconnecting is handled by the reconnect finally's ownership check: the
    // abort above unwinds the reader, whose finally still owns and clears it.)
    setWorkspaceStarting(false);
    setTurnHeld(null);
    setIsCompacting(false);
    clearModelStatus();
    // Drop any message queued during compaction: the user just cancelled, so it
    // must NOT auto-send when the isCompacting→false transition fires the flush
    // effect. dropQueuedSend clears the ref synchronously (before the effect runs
    // post-render) and removes its optimistic shimmer bubble.
    dropQueuedSend();
    releaseStreamOwnership();
    currentMessageRef.current = null;

    // An ADMITTED stop (run id latched) finalizes server-side as a cancelled
    // turn. The finalize archives the partial events on the response row,
    // closed with a synthetic finish_reason "stopped", and replay rebuilds the
    // committed steps from the checkpoint and restores the in-flight partial
    // the checkpoint never committed from that archive. So once the finalize
    // lands its bubbles are replay-reproducible: mark them isHistory and
    // release the recently-sent dedup, same as a success finalize. Otherwise
    // a later corrective reload appends a replayed twin of the answer under a
    // dedup-eaten user message, ordered after newer turns. A PRE-ADMISSION
    // stop has no turn row: replay can't reproduce those bubbles, so they must
    // stay unmarked to survive reloads.
    if (stoppedRunId) {
      markTranscriptPersisted();
    }

    // (c) The thread mux stays attached: the backend cancel finalizes this
    // turn's task runs, and their terminal frames flip the cards to their
    // real outcome (cancelled) instead of a client-side guess. Leave the
    // report-back watch running: this Chief of Staff thread cancel does not stop the
    // background PTC analyses on their own threads, so their summaries should
    // still surface live. The aborted reader's finally clears isStreamingRef, so
    // the watch's next reconcile can attach the next head run or drain.

    // (d) Tell the backend to hard-cancel: one retry, then a visible error
    // toast so a failed cancel doesn't silently diverge UI from backend.
    if (tid && tid !== '__default__') {
      let outcome: CancelOutcome | null = null;
      try {
        outcome = await cancelWorkflow(tid, stoppedRunId);
      } catch (firstErr) {
        // Log the first failure — the toast below reflects only the final
        // state, so a degraded-network first error would otherwise vanish
        // from diagnostics.
        console.warn('[stopWorkflow] cancel failed, retrying once:', firstErr);
        try {
          outcome = await cancelWorkflow(tid, stoppedRunId);
        } catch {
          // Both attempts failed; `outcome` stays null and the toast fires.
        }
      }

      // A 200 is not proof of a stop, so an unreachable endpoint and an
      // honest "stopped nothing" get the same treatment. `another_run_active`
      // means the run we named was gone while the thread stayed busy — the
      // turn the user just stopped may still be spending model and sandbox
      // time, and the client already tore its own streaming state down above,
      // so saying nothing would leave the two sides silently diverged. The
      // remaining no-op states genuinely had nothing left to stop.
      if (!outcome || outcome.state === 'another_run_active') {
        toast({ description: t('chat.stopFailed'), variant: 'destructive' });
      }
      giveBackStoppedSteers(outcome?.returned_steers ?? [], unsent);
    } else {
      giveBackStoppedSteers([], unsent);
    }
  };

  /**
   * Puts the user's steers a stop took back into the composer, oldest first,
   * and removes the bubbles that showed them waiting: those the run handed
   * back, then those that never reached it. A returned steer is matched to
   * its bubbles by request key, else by its text, and given back once. One a
   * delivered row on screen carries was read after all, so it stays there.
   */
  const giveBackStoppedSteers = (
    returned: readonly HeldSteer[],
    unsent: readonly { message: string; steerId: string; requestKey: string }[],
  ) => {
    const transcript = liveMessages.get();
    const keyOf = (m: ChatMessage) => (m as { requestKey?: string }).requestKey;
    const drop = new Set<string>(unsent.map((u) => u.steerId));
    const texts: string[] = [];
    const seen = new Set<string>();
    for (const steer of returned) {
      const key = steer.request_key;
      if (key) {
        if (seen.has(key)) continue;
        seen.add(key);
        // The server keeps a returned steer's key spent, so the text sent
        // again from the composer is a new send under a new key.
        requestKeyRef.current.settle(key);
        const copies = transcript.filter((m): m is UserMessage => m.role === 'user' && keyOf(m) === key);
        if (copies.some((m) => m.steeringDelivered)) continue;
        for (const m of copies) drop.add(m.id as string);
      } else {
        const text = steer.content.trim();
        const copy = transcript.find((m) => m.role === 'user' && !drop.has(m.id as string)
          && !m.steeringDelivered && !!m.steering && String(m.content ?? '').trim() === text);
        if (copy) drop.add(copy.id as string);
      }
      if (steer.content) texts.push(steer.content);
    }
    for (const u of unsent) {
      requestKeyRef.current.settle(u.requestKey);
      texts.push(u.message);
    }
    if (drop.size > 0) setMessages((prev) => prev.filter((m) => !drop.has(m.id as string)));
    if (texts.length > 0) giveBackSteering(texts.join('\n'));
  };

  /**
   * Stop an in-flight MANUAL compaction (/compact or /offload). Unlike
   * stopWorkflow this is not a streaming-turn teardown — manual compaction
   * registers no turn — so it only clears the local compaction state, drops any
   * queued send, and asks the backend to cancel the in-flight compaction call
   * (workflow_handler routes a run-less /cancel to cancel_compaction). The
   * summarize/offload request then rejects; ChatView suppresses that error
   * because the user initiated the stop.
   */
  const stopCompaction = async () => {
    const tid = threadIdRef.current;
    setIsCompacting(false);
    // Drop a message queued during compaction so the isCompacting→false flush
    // effect doesn't auto-send it after the user cancelled, and remove its
    // optimistic shimmer bubble.
    dropQueuedSend();
    if (tid && tid !== '__default__') {
      try {
        await cancelWorkflow(tid);
      } catch (err) {
        console.warn('[stopCompaction] cancel failed:', err);
      }
    }
  };

  /** Live event routing lives in session/stream/processStreamEvent; the
   * hook binds the runtime + cross-lane callbacks per stream start. */

  /**
   * The composer banner for a turn request that failed, and whether it is the
   * server's own answer to act on (a quota denial, a provider or access link),
   * under which a failed bubble would read as a second, unrelated fault.
   * Otherwise the banner is the server's own words, and null for a transport
   * or bare HTTP failure: its raw text ("Failed to fetch", "HTTP error!
   * status: 500") is no message to show, and a failed reply already says the
   * request failed and offers Retry.
   */
  const refusalBanner = (err: unknown): { banner: string | StructuredError | null; answered: boolean } => {
    const errObj = err as Record<string, unknown>;
    if (errObj.status === 429) {
      const info = (errObj.rateLimitInfo || {}) as Record<string, unknown>;
      const platformUrl = (import.meta.env.VITE_PLATFORM_URL as string | undefined) || '/account';
      return { banner: buildRateLimitError(info, platformUrl), answered: true };
    }
    // Build structured error with link when backend provides one
    // (byok_key_required, oauth_required, 403, ...).
    const errorInfo = errObj.errorInfo as Record<string, unknown> | undefined;
    // The subscription's token could not be renewed just now, which the next
    // try usually fixes: say so as a notice and leave the failed reply's Retry.
    if (errorInfo?.type === 'oauth_refresh_unavailable') {
      return {
        banner: { message: (errorInfo.message as string) || (err as Error).message, kind: 'notice' },
        answered: false,
      };
    }
    if (errorInfo?.link) {
      // Backend scrubbed stale model names from the user's saved
      // preferences — invalidate our cached copy so the selector and
      // Settings page re-render with the server's (cleaned) state.
      // Without this, the frontend keeps sending the removed model
      // name as request_model on every retry.
      if (errorInfo.type === 'model_removed') {
        queryClient.invalidateQueries({ queryKey: queryKeys.user.preferences() });
      }
      const link = errorInfo.link as ErrorLinkSpec;
      return {
        banner: {
          message: (errorInfo.message as string) || (err as Error).message || 'An error occurred.',
          // The server words the link in English; these we can translate.
          links: [
            errorInfo.type === 'oauth_reauth_required' ? { ...link, labelKey: 'chat.errorLinkReconnect' }
              : errorInfo.type === 'oauth_plan_no_api_access' ? { ...link, labelKey: 'chat.errorLinkAddApiKey' }
                : link,
          ],
        },
        answered: true,
      };
    }
    if (errObj.status === 403) {
      return {
        banner: {
          message: (err as Error).message || 'Access denied.',
          links: [{ url: '/setup/method', label: 'Configure providers' }],
        },
        answered: true,
      };
    }
    // The transport words an upload too large for the server itself.
    if (errObj.status === 413) return { banner: (err as Error).message, answered: false };
    // A busy chat's refusal, in the view's words: the server's name the CLI's
    // commands.
    if (isBusyCode(errorInfo?.code)) {
      return { banner: { message: t(BUSY_REFUSAL_KEYS[errorInfo.code]), kind: 'notice' }, answered: false };
    }
    const worded = typeof errorInfo?.message === 'string' && errorInfo.message ? errorInfo.message : null;
    return { banner: worded, answered: false };
  };

  /**
   * Sends a message into the turn this view shows running, as a steer. The
   * POST may only steer (`steer_only`): the view's loading state lags the
   * server's by the stream's tail, so the turn may already be over, and then
   * the server refuses it as `not_running` instead of starting a turn on a
   * connection that reads only the acceptance. That steer goes out again as
   * a send under the same request key once the view lets the ended turn go,
   * or back to the composer when the user stopped meanwhile.
   */
  const handleSendSteering = async (
    message: string,
    additionalContext: Record<string, unknown>[] | null = null,
    attachmentMeta: Record<string, unknown>[] | null = null,
    modelOptions: ModelOptions = {},
    resend: Resend = {},
  ) => {
    const { widgetSnapshots, chartSelections } = modelOptions;
    // The refused-steer queue the steer belongs to, for a refusal to rejoin
    // it only if no thread switch has ended it since.
    const gen = notRunningGenRef.current;
    // Show user message in chat with steering indicator. Preserve any inline
    // context cards (widget snapshots / chart selections) so a message queued
    // during compaction keeps them when the flush routes through steering.
    const steeredTurn = newestTurn(liveMessages.get()) ?? undefined;
    // Same fingerprint form as handleSendMessage: a re-send of a steer whose
    // answer was lost reuses the key and dedups against the accepted copy.
    // Other steers can be waiting on their answers too, and each keeps its own
    // key for its own re-send.
    const requestKey = resend.requestKey ?? requestKeyRef.current.take(sendFingerprint(threadId, message), { alongside: true });
    const userMessage: ChatMessage = {
      ...createUserMessage(message, steeredTurn, { attachments: attachmentMeta as AttachmentMeta[] | null, widgetSnapshots, chartSelections, id: resend.shown?.userId }),
      steering: true,
      requestKey,
      ...(currentRunIdRef.current ? { steeredRunId: currentRunIdRef.current } : {}),
    };
    recentlySentTrackerRef.current.track(message.trim(), userMessage.timestamp, userMessage.id);
    setMessages((prev) => appendMessage(prev,userMessage));
    const seq = ++steerSeqRef.current;

    // The refusal the stream answered with, once it started: the same answer
    // the route gives as a 409 before it does.
    const answered: { refusal: Record<string, unknown> | null } = { refusal: null };
    steersInFlightRef.current.set(seq, gen);
    try {
      await sendChatMessageStream(message, workspaceId, threadId, {
        onEvent: (event) => {
          if (event.event === 'steering_accepted') {
            // Snapshot the boundary so the primary stream's steering_delivered
            // handler can roll back leaked content. The event's own `_eventId`
            // is the natural boundary in the Redis stream; fall back to the
            // local counter for tests/legacy flows without `_eventId`.
            steeringAtOrderRef.current = computeSteeringBoundary(event, contentOrderCounterRef.current);
            // Update the user message to reflect steering status
            setMessages((prev) =>
              updateMessage(prev,userMessage.id as string, (msg) => ({
                ...msg,
                steering: true,
                queuePosition: event.position,
              }))
            );
            return;
          }
          // A steer-only POST carries no turn of its own: its one other
          // answer is a refusal.
          if (event.event === 'error' && !answered.refusal) answered.refusal = event;
        },
        additionalContext,
        locale: userLocale,
        timezone: userTimezone,
        platform,
        onRunIdResolved: () => requestKeyRef.current.settle(requestKey),
        requestKey,
        steerOnly: true,
      });
      const frame = answered.refusal;
      if (frame) {
        const text = typeof frame.error === 'string' ? frame.error : '';
        throw Object.assign(new Error(text || 'Failed to send message'), {
          status: 409,
          errorInfo: { code: frame.code, message: text },
        });
      }
    } catch (err: unknown) {
      if ((err as Error)?.name === 'AbortError') return;
      const code = (err as { errorInfo?: { code?: unknown } })?.errorInfo?.code;
      if ((err as { status?: number })?.status === 409 && code === 'not_running') {
        settleNotRunningSteer(userMessage.id as string, gen, message, additionalContext, attachmentMeta, modelOptions, requestKey, seq);
        return;
      }
      // A steer the user's stop overtook: the stop hands the turn's steers
      // back to the composer, and this one joins them.
      if (wasStoppedRef.current && code === 'stopping') {
        requestKeyRef.current.settle(requestKey);
        setMessages((prev) => prev.filter((m) => m.id !== userMessage.id));
        giveBackSteering(message);
        return;
      }
      if (wasStoppedRef.current) return;
      console.error('Error sending steering:', err);
      if (settleDuplicateSteer(err, userMessage.id as string, message, requestKey)) return;
      // The server sends a refusal (a 4xx) before it queues a steer. It holds
      // the status line of a steer it takes until the stream's first frame,
      // so a failure in between reads as a 5xx or as no answer, and a
      // delivery the run already made was the answer this POST lost. A
      // delivery marks the steers it names, so one that marked a refused
      // steer was another with its text.
      const refused = isClientError(err);
      if (!refused && liveMessages.get().some((m) => m.id === userMessage.id && (m as { steeringDelivered?: boolean }).steeringDelivered)) return;
      // Shown as a refused send's is. A steer has no reply to fail in its
      // place, so one the server gave no words for still says it was not
      // sent. A load clears the banner as it starts, so the refusal is
      // carried past the next one: a reload may be waiting on this answer,
      // or asked for and yet to run.
      const banner = refusalBanner(err).banner ?? 'Failed to send message';
      refusalAfterReloadRef.current = { epoch: sessionEpochRef.current, banner };
      // Update user message to show steering failure. A stream end or a
      // give-up while it was out may have marked it persisted, but the server
      // never stored it, so a reload must leave it with its error. The error
      // is the banner, so a delivery that proves the server took the steer
      // after all clears that banner and no other.
      setMessages((prev) =>
        updateMessage(prev,userMessage.id as string, (msg) => ({
          ...msg,
          steering: false,
          steeringDelivered: false,
          isHistory: false,
          queueError: banner,
          queueRefused: refused,
        }))
      );
      setMessageError(banner);
    } finally {
      steersInFlightRef.current.delete(seq);
      if (notRunningSendsRef.current.length > 0) setNotRunningTick((n) => n + 1);
      // A load deferred to this answer runs now, unless a stream it started
      // holds it until that stream lets go.
      if (steersInFlightRef.current.size === 0 && pendingMuxResyncRef.current && !isStreamingRef.current) {
        pendingMuxResyncRef.current = false;
        setReloadTrigger((n) => n + 1);
      }
    }
  };

  /**
   * A steer the server refused as `not_running`: the turn it was sent to had
   * ended. After a stop the message goes back to the composer with the
   * turn's other steers. Otherwise its bubble keeps waiting until the view
   * lets the ended turn go, and then it goes out as a send under the same
   * request key (the flush below), behind any sent before it.
   */
  const settleNotRunningSteer = (
    steerId: string,
    gen: number,
    message: string,
    additionalContext: Record<string, unknown>[] | null,
    attachmentMeta: Record<string, unknown>[] | null,
    modelOptions: ModelOptions,
    requestKey: string,
    seq: number,
  ): void => {
    // A thread switch while it was out took its bubble with the old thread.
    if (gen !== notRunningGenRef.current) {
      requestKeyRef.current.settle(requestKey);
      return;
    }
    if (wasStoppedRef.current) {
      requestKeyRef.current.settle(requestKey);
      setMessages((prev) => prev.filter((m) => m.id !== steerId));
      giveBackSteering(message);
      return;
    }
    enqueueNotRunningSend({ gen, message, additionalContext, attachmentMeta, modelOptions, requestKey, steerId, seq });
  };

  // In send order, so a steer whose refusal lands late still goes out ahead
  // of those sent after it.
  const enqueueNotRunningSend = (entry: (typeof notRunningSendsRef.current)[number]): void => {
    const queue = notRunningSendsRef.current;
    const at = queue.findIndex((q) => q.seq > entry.seq);
    queue.splice(at === -1 ? queue.length : at, 0, entry);
    setNotRunningTick((n) => n + 1);
  };

  /**
   * A message sent while refused steers wait to go out joins them, behind the
   * last: the first of them opens the next turn, and steered into that turn
   * this one would reach the model ahead of the others, typed before it.
   */
  const queueBehindRefused = (
    message: string,
    additionalContext: Record<string, unknown>[] | null,
    attachmentMeta: Record<string, unknown>[] | null,
    modelOptions: ModelOptions,
    shown?: ShownBubbles,
  ): void => {
    const requestKey = requestKeyRef.current.take(sendFingerprint(threadId, message), { alongside: true });
    const waiting: ChatMessage = {
      ...createUserMessage(message, newestTurn(liveMessages.get()) ?? undefined, {
        attachments: attachmentMeta as AttachmentMeta[] | null,
        widgetSnapshots: modelOptions.widgetSnapshots,
        chartSelections: modelOptions.chartSelections,
        id: shown?.userId,
      }),
      steering: true,
      requestKey,
    };
    setMessages((prev) => appendMessage(prev, waiting));
    enqueueNotRunningSend({
      gen: notRunningGenRef.current,
      message,
      additionalContext,
      attachmentMeta,
      modelOptions,
      requestKey,
      steerId: waiting.id as string,
      seq: ++steerSeqRef.current,
    });
  };

  /**
   * A plain send's standing with the server, read off its own stream. A send
   * refused or lost before the run started left nothing on the server, so its
   * bubbles take no turn and its Retry sends it again, under the same request
   * key in case a copy arrived.
   */
  const trackPlainSend = (send: {
    /** Null for a hidden opener, whose reply is its only bubble. */
    userId: string | null;
    assistantId: string;
    turnIndex: number;
    request: TurnRequest & { requestKey: string };
    watermarkBefore: number | null;
  }) => {
    const ids = new Set(send.userId === null ? [send.assistantId] : [send.userId, send.assistantId]);
    let started = false;
    let refused = false;
    return {
      /** Reads a frame of the send's stream, ahead of the processor. */
      observe(event: SSEEvent): void {
        if (isSubagentEvent(event)) return;
        if (forkRefusalMessage(event) !== null) {
          // One that names a run was refused after the run started.
          if (typeof event.run_id === 'string') started = true;
          else refused = true;
          return;
        }
        if (event.event === 'metadata') {
          started = true;
          return;
        }
        // A failure that names its run came after the server started it.
        if ((event.event === 'error' || event.event === 'retry') && event.recovery && typeof event.run_id === 'string') started = true;
      },
      /** Whether the server refused the send before starting its run. */
      refused: () => refused && !started,
      /** The send, for a resend to carry, until the server starts its run. */
      unstarted: (): UnstartedSend | null => (started ? null : { userId: send.userId, watermarkBefore: send.watermarkBefore }),
      /** Marks the send as one the server never kept, unless it started the run. */
      unadmit(): void {
        if (started) return;
        // Not history either, whatever counted it so before: a steer the
        // running turn's end took for kept may have turned into this send.
        setMessages((prev) => prev.map((m) => (ids.has(m.id as string) ? { ...m, unadmitted: true, isHistory: false } : m)));
        if (lastRenderedTurnIndexRef.current === send.turnIndex) lastRenderedTurnIndexRef.current = send.watermarkBefore;
        heldRetriesRef.current.set(send.assistantId, { bubbleId: send.assistantId, request: send.request, cut: null });
      },
    };
  };

  /**
   * Gives a hidden opener's reply, from the moment it opens, a Retry that
   * brings the turn's context again. Its words ride that context alone, which
   * no row keeps, and no message of it is on screen to send again: a run that
   * failed before its graph took the input reruns it, which the server
   * refuses without the context (`resend_required`). A recovery the stream
   * names, or a refusal of the send, holds its own Retry in this one's place.
   */
  const holdHiddenOpenerRetry = (bubbleId: string, additionalContext: Record<string, unknown>[] | null) => {
    if (!additionalContext) return;
    heldRetriesRef.current.set(bubbleId, { bubbleId, request: retryRequest(null, additionalContext), cut: null });
  };

  const handleSendMessage = async (message: string, additionalContext: Record<string, unknown>[] | null = null, attachmentMeta: Record<string, unknown>[] | null = null, { model, reasoningEffort, fastMode, widgetSnapshots, chartSelections, subagentsAllowed, reasoningEffortOverride, fastModeOverride, hiddenOpener = false, threadTitle }: SendOptions = {}, resend: Resend = {}) => {
    const hasContent = message.trim() || (additionalContext && additionalContext.length > 0);
    if (!workspaceId || !hasContent) {
      return;
    }

    // Chat activity bumps the thread to the top of the nav panel's list
    // (clicking around never reorders; new threads surface via the new-id rule).
    bumpThreadNavOrder(workspaceId, threadIdRef.current);

    // The three branches below park or steer a send in a chat that is busy. A
    // hidden opener only ever opens a new chat, which is never busy, so they
    // carry neither of its options.

    // If the agent is compacting its context, hold this message and auto-send
    // it once compaction finishes (mirrors the backend admission gate, which
    // 409s a POST that arrives mid-compaction). Must come BEFORE the isLoading
    // steering branch: during an auto Tier-2 summarize the turn is still
    // running, so steering now would corrupt the in-flight context rewrite.
    // Keying off isCompacting covers every compaction path uniformly — SSE
    // auto-summarize plus manual /compact and /offload (both set isCompacting
    // in ChatView).
    if (isCompacting) {
      // Show the parked message as a shimmer bubble (like a pending steering
      // message) so the user sees what will send. Only the latest queued
      // message is held, so replace any earlier optimistic bubble.
      const prevQueuedId = queuedSendRef.current?.messageId;
      // A placeholder: the send it becomes adds a bubble of its own.
      const queuedMsg = createUserMessage(message, undefined, {
        attachments: attachmentMeta as AttachmentMeta[] | null,
        widgetSnapshots,
        chartSelections,
      });
      const queuedMessage: ChatMessage = { ...queuedMsg, queued: true };
      // The subagents setting stays behind: compaction only runs on a thread
      // that exists, whose row a flip made since has already PATCHed, and the
      // value captured now would store over that flip when the send flushes.
      queuedSendRef.current = {
        message,
        additionalContext,
        attachmentMeta,
        modelOptions: { model, reasoningEffort, fastMode, widgetSnapshots, chartSelections },
        messageId: queuedMessage.id as string,
      };
      setMessages((prev) => {
        const base = prevQueuedId ? prev.filter((m) => m.id !== prevQueuedId) : prev;
        return appendMessage(base, queuedMessage);
      });
      setQueuedSend(message.trim() || '…');
      return;
    }

    // The resend of a refused steer is the queue's own; anything else waits behind it.
    if (resend.requestKey === undefined && notRunningSendsRef.current.length > 0) {
      queueBehindRefused(message, additionalContext, attachmentMeta, { model, reasoningEffort, fastMode, widgetSnapshots, chartSelections, subagentsAllowed, reasoningEffortOverride, fastModeOverride }, resend.shown);
      return;
    }

    // If agent is already streaming, send as steering message
    if (isLoading) {
      return handleSendSteering(message, additionalContext, attachmentMeta, { model, reasoningEffort, fastMode, widgetSnapshots, chartSelections, subagentsAllowed, reasoningEffortOverride, fastModeOverride }, resend);
    }

    // Store model options so HITL resume can forward them
    lastModelOptionsRef.current = { model: model || null, reasoningEffort: reasoningEffort || null, fastMode: fastMode ?? null };

    // Create and add user message. A hidden opener has none, and its reply is
    // the first bubble of its turn: the replay knows that reply by the flag
    // it carries, since the empty query it stores draws no row to match.
    const turnIndex = newestKnownTurn() + 1;
    const userMessage = hiddenOpener ? null : createUserMessage(message, turnIndex, {
      attachments: attachmentMeta as AttachmentMeta[] | null,
      widgetSnapshots,
      chartSelections,
      id: resend.shown?.userId,
    });
    if (userMessage) recentlySentTrackerRef.current.track(message.trim(), userMessage.timestamp, userMessage.id);

    // Check if this is a new conversation
    // Only consider it a new conversation if:
    // 1. There are no messages at all, OR
    // 2. We're starting a new thread (threadId is '__default__')
    // This determines if we should overwrite the existing todo list card
    // Note: We don't consider it a new conversation just because all messages are from history
    // - the user might continue the conversation, and we want to keep the todo list card
    const isNewConversation = messages.length === 0 || threadId === '__default__';
    isNewConversationRef.current = isNewConversation;

    // Track model used in this send
    if (model) {
      setThreadModels(prev => prev.includes(model) ? prev : [...prev, model]);
    }

    // Add user message after history messages
    if (userMessage) {
      setMessages((prev) => {
        const newMessages = appendMessage(prev,userMessage);
        // Update new messages start index if this is the first new message
        if (newMessagesStartIndexRef.current === prev.length) {
          newMessagesStartIndexRef.current = newMessages.length;
        }
        return newMessages;
      });
    }

    setIsLoading(true);
    setMessageError(null);
    setFallbackSuggestion(null);
    setHasActiveSubagents(false);
    // NB: do NOT clear terminalTaskOutcomesRef here. A fresh send appends a turn
    // without resetting the subagent-card projection, so a tail subagent that
    // already settled (this turn or a prior one) must keep its observed terminal
    // outcome — otherwise a later reconnect off a stale /status snapshot would
    // re-activate its card. The map is cleared only on a full history-backed reset.
    // Clear the stopped guard so a fresh send can finalize again on stop.
    wasStoppedRef.current = false;
    sessionEpochRef.current += 1;
    backgroundReconnectRef.current = false;
    // This send opens a NEW backend turn rendered in-view; advance the
    // watermark to it so the next reactivation's staleness check doesn't
    // mistake this turn for one missed while hidden (spurious full reload).
    const watermarkBefore = lastRenderedTurnIndexRef.current;
    lastRenderedTurnIndexRef.current = turnIndex;
    // Mark streaming as in progress (prevents history loading during streaming)
    // AND claim ownership for this thread, so navigating to another thread mid-send
    // supersedes this stream rather than leaving it orphaned (the load guard would
    // otherwise block the new thread because isStreamingRef is still set).
    acquireStreamOwnership(threadId);

    // Create assistant message placeholder
    const assistantMessageId = `assistant-${Date.now()}`;
    beginRun();
    // Fresh AbortController so stopWorkflow can abort this stream's reader.
    const abortController = new AbortController();
    mainStreamAbortRef.current = abortController;

    const assistantMessage: AssistantMessage = {
      ...createAssistantMessage(assistantMessageId, turnIndex),
      ...(hiddenOpener ? { hiddenOpener: true } : {}),
    };

    // Add assistant message after history messages
    setMessages((prev) => {
      const newMessages = appendMessage(prev,assistantMessage);
      // Update new messages start index
      newMessagesStartIndexRef.current = newMessages.length;
      return newMessages;
    });
    currentMessageRef.current = assistantMessageId;

    // A new thread's create round-trip comes before the send, and the message
    // lives only in this page until the send's own hold takes over, so the
    // stale-build reload a section switch takes waits for it too.
    const releaseCreate = holdReload();
    const created = await ensureThreadId({
      threadId,
      workspaceId,
      message,
      title: threadTitle,
      platform,
      timezone: userTimezone,
      subagentsAllowed,
      reasoningEffortOverride,
      fastModeOverride,
      queryClient,
      threadIdRef,
      setThreadId,
      wasStoppedRef,
      signal: abortController.signal,
    }).finally(releaseCreate);
    // Stopped during the pre-create round-trip: stopWorkflow already finalized
    // the UI — starting the run now would resurrect it.
    if (created.aborted) return;
    const effectiveThreadId = created.threadId;

    // One request_key per logical send, reused if this exact send is
    // retransmitted after a lost response (fingerprint match) — see
    // createRequestKeyTracker. A steer that found no turn running goes out
    // under the key it was sent with.
    const requestKey = resend.requestKey ?? requestKeyRef.current.take(sendFingerprint(effectiveThreadId, message));
    let wasDisconnected = false;
    const wasInterruptedRef = { current: false };
    const admission = trackPlainSend({
      userId: userMessage?.id ?? null,
      assistantId: assistantMessageId,
      turnIndex,
      request: { ...sendRequest(message, additionalContext, subagentsAllowed), requestKey },
      watermarkBefore,
    });
    if (hiddenOpener) holdHiddenOpenerRetry(assistantMessageId, additionalContext);
    try {
      // Prepare refs for event handlers — use persistent subagent state
      const refs = buildStreamRefs({ isNewConversation: isNewConversationRef.current });

      // Create the event processor using the shared factory
      const processStreamFrame = createStreamEventProcessor(runtime, streamRouterDeps, assistantMessageId, refs, getTaskIdFromEvent, wasInterruptedRef);
      const processEvent = (event: SSEEvent) => {
        admission.observe(event);
        processStreamFrame(event);
      };

      const result = await sendChatMessageStream(message, workspaceId, effectiveThreadId, {
        onEvent: processEvent,
        additionalContext,
        locale: userLocale,
        timezone: userTimezone,
        llmModel: model || null,
        reasoningEffort: reasoningEffort || null,
        fastMode: fastMode ?? null,
        platform,
        // Latch run_id AND the server-assigned thread_id from Content-Location
        // BEFORE the first SSE body byte. run_id closes the reconnect race
        // window; the thread_id latch lets an early stop on a brand-new thread
        // ('__default__' until the first event) still hard-cancel the backend
        // run instead of skipping cancel. The first event still drives the
        // route/storage update (see the thread_id branch in processEvent).
        onRunIdResolved: (runId, resolvedThreadId) => {
          requestKeyRef.current.settle(requestKey);
          currentRunIdRef.current = runId;
          if (resolvedThreadId && resolvedThreadId !== '__default__') {
            threadIdRef.current = resolvedThreadId;
          }
        },
        signal: abortController.signal,
        requestKey,
        threadSettings: { subagentsAllowed },
      });

      // The user hit stop: stopWorkflow already finalized the message and ran
      // teardown. Skip reconnect/cleanup so we don't double-fire. Exception: a
      // foreground handler aborted this stream because the tab resumed
      // (background abort, not a user stop) — re-kick the reconnect instead.
      if (result?.aborted || wasStoppedRef.current) {
        if (backgroundReconnectRef.current && !wasStoppedRef.current) {
          backgroundReconnectRef.current = false;
          attemptReconnectAfterDisconnect(currentMessageRef.current || assistantMessageId);
        }
        return;
      }

      // Checked before the disconnect: the server drops the connection right
      // after a retry notice, and the run it names is already over, so there
      // is nothing to reconnect to. A scheduled resend owns finalization.
      if (refs.retryNotice && settleRetry(refs.retryNotice, currentMessageRef.current || assistantMessageId, 0, { model, reasoningEffort, fastMode }, sendRequest(message, additionalContext, subagentsAllowed), null, admission.unstarted())) {
        wasDisconnected = true;
        return;
      }

      if (result?.disconnected && !refs.retryNotice) {
        console.log('[Send] Stream disconnected, attempting reconnect');
        wasDisconnected = true;
        attemptReconnectAfterDisconnect(assistantMessageId);
        return;
      }

      // Mark message as complete (use live ref in case steering_delivered switched it)
      {
        const finalId = currentMessageRef.current || assistantMessageId;
        setMessages((prev) =>
          updateMessage(prev, finalId, (msg) => finalizeAssistantMessage(msg, wasInterruptedRef.current ? 'paused' : 'completed'))
        );
        // A send the server never started left nothing to replay, so its
        // bubbles have to outlive a reload and take no turn.
        if (admission.refused() || refs.retryNotice?.recovery === 'resend') admission.unadmit();
        else markTranscriptPersisted();
      }
    } catch (err: unknown) {
          // An aborted stream (user hit stop) is intentional, not a failure.
          // streamFetch normally swallows AbortError and returns { aborted },
          // but guard here too so a stop never surfaces an error banner.
          if ((err as Error)?.name === 'AbortError' || wasStoppedRef.current) {
            return;
          }
          // A copy steered into another turn needs a turn running, which a
          // hidden opener's new chat has none of.
          if (userMessage && settleSteeredSend(err, { userId: userMessage.id, assistantId: assistantMessageId, turnIndex, watermarkBefore, requestKey })) return;
          // 409 duplicate_request: an earlier copy of this send was already
          // accepted (its response was lost) — adopt that run instead of
          // erroring; the reconnect owns finalization from here.
          if (
            retryFailedDuplicate(err, assistantMessageId, sendRequest(message, additionalContext, subagentsAllowed), { model, reasoningEffort, fastMode }, requestKey)
            || adoptDuplicateRun(err, assistantMessageId, requestKey)
          ) {
            wasDisconnected = true;
            return;
          }
          admission.unadmit();
          if ((err as Record<string, unknown>).status !== 429) console.error('Error sending message:', err);
          const { banner, answered } = refusalBanner(err);
          if (banner !== null) setMessageError(banner);
          // A hidden opener keeps its reply either way: the user has no
          // message on screen to send again, so the failed reply is the one
          // place the turn's Retry can live, beside the banner's reason.
          if (answered && userMessage) {
            // When a banner with a CTA renders, drop the optimistic assistant
            // bubble so the transcript stays clean, as the `internal` SSE
            // error does. A content-less "Failed to send message" bubble
            // under the banner looks broken.
            setMessages((prev) => prev.filter((m) => m.id !== assistantMessageId));
          } else {
            setMessages((prev) =>
              updateMessage(prev, assistantMessageId, (msg) =>
                failAssistantMessage(msg, { message: 'Failed to send message. Please try again.' }))
            );
          }
        } finally {
          // Skip cleanup on a user stop — stopWorkflow owns the teardown and a
          // second cleanup here would re-toggle loading/subagent state. Also
          // clear the abort ref so a later stop can't abort a finished stream.
          if (mainStreamAbortRef.current === abortController) {
            mainStreamAbortRef.current = null;
          }
          // wasStoppedRef is shared across streams: if the user stopped THIS
          // stream then sent a new one, the new send resets wasStoppedRef to
          // false, so this stale finally would otherwise run cleanup against
          // currentMessageRef — now the NEW stream's message — clobbering it
          // mid-flight. The per-stream abort signal is the reliable guard: an
          // aborted stream's teardown is always owned elsewhere (stopWorkflow
          // or the superseding send), never this finally.
          if (
            !wasDisconnected &&
            !wasInterruptedRef.current &&
            !wasStoppedRef.current &&
            !abortController.signal.aborted
          ) {
            // Mark message as complete (use live ref in case steering_delivered switched it)
            const finalId = currentMessageRef.current || assistantMessageId;
            setMessages((prev) =>
              updateMessage(prev, finalId, (msg) => finalizeAssistantMessage(msg, wasInterruptedRef.current ? 'paused' : 'completed'))
            );

            cleanupAfterStreamEnd(finalId);
          }
        }
      };

  // Flush a message queued during compaction once it finishes, through the
  // branch handleSendMessage takes for a message arriving now: behind refused
  // steers still waiting, else steered into a turn still running (auto Tier-2
  // summarize), else a fresh turn. queuedSendRef is cleared in stopWorkflow,
  // so a queued message is never replayed into a turn the user just cancelled.
  useEffect(() => {
    if (isCompacting) return;
    const queued = queuedSendRef.current;
    if (!queued) return;
    queuedSendRef.current = null;
    setQueuedSend(false);
    const { message, additionalContext, attachmentMeta, modelOptions, messageId } = queued;
    // Drop the optimistic shimmer bubble; the send path re-adds the real one
    // (steering shimmer if a turn is still running, else a normal user bubble)
    // in its place, in the same batch.
    if (messageId) {
      setMessages((prev) => prev.filter((m) => m.id !== messageId));
    }
    void handleSendMessage(message, additionalContext, attachmentMeta, modelOptions, { shown: { userId: messageId } });
    // Fires on isCompacting transitions. The send handler is captured from
    // the render where isCompacting went false — that render is the correct
    // moment to decide steer-vs-fresh. Handlers are omitted from
    // deps because the values they actually close over (workspaceId/threadId)
    // don't change mid-compaction, so a stale closure here isn't possible.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isCompacting]);

  // Send a steer the server refused as `not_running` once the view has let
  // the ended turn go: it opens the next turn, under the key it was sent
  // with. One at a time, oldest first, so a second waits for the turn the
  // first opens to end. A steer sent before it whose POST has not answered
  // holds it too: the turn ending refuses both, and the older refusal can
  // land after the view goes idle. That POST is bounded, so the hold is.
  // Nothing is taken off the queue until the send can go out: one that
  // returned early would leave no turn whose end wakes the rest.
  useEffect(() => {
    if (isLoading || isCompacting || !workspaceId) return;
    const queue = notRunningSendsRef.current;
    const next = queue[0];
    if (!next) return;
    for (const [seq, gen] of steersInFlightRef.current) {
      if (seq < next.seq && gen === next.gen) return;
    }
    queue.shift();
    // The send's bubble takes the waiting one's place, in the same batch.
    setMessages((prev) => prev.filter((m) => m.id !== next.steerId));
    void handleSendMessage(next.message, next.additionalContext, next.attachmentMeta, next.modelOptions, { requestKey: next.requestKey, shown: { userId: next.steerId } });
    // Fires when the view goes idle, a steer is answered or the workspace
    // arrives. The send handler is captured from that render, which is the
    // one that saw the view idle.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isLoading, isCompacting, workspaceId, notRunningTick]);

  /**
   * Resumes an interrupted turn with an HITL response (approve or reject).
   * Follows the same pattern as handleSendMessage but sends messages: [] with hitl_response.
   * Stable, and always the last committed render's body: a resume can come
   * long after the render that armed the interrupt, through handlers memoized
   * on it, and has to use the current runtime, callbacks and model options.
   */
  const resumeWithHitlResponse = useStableHandler(async (hitlResponse: HitlResponseBody) => {
    // Ahead of beginResume, so the settler's fence captures this run's own
    // epoch rather than the previous one (which it would already fail).
    sessionEpochRef.current += 1;
    const settleResume = answerBoard.beginResume(Object.keys(hitlResponse));
    let admitted = false;
    // A fork elsewhere replaced the pause this answers (`stale_fork`), so its
    // card can never be answered: the thread reloads the way a refused fork's
    // does, and the board does not get the pause back.
    let pauseGone = false;
    const transcript = liveMessages.get();
    const recoverStaleAnswer = (source: unknown) => {
      if (!isStaleForkRefusal(source)) return false;
      pauseGone = forkRefusal.recover(source, { kind: 'resume', transcript, draft: null });
      return pauseGone;
    };

    setPendingInterrupt(null);
    answerBoard.clear();

    // Create assistant message placeholder
    const assistantMessageId = `assistant-hitl-${Date.now()}`;
    // HITL resume always opens a fresh run on the backend (1:1 with
    // ``conversation_response_id``).
    beginRun();

    // A resume is a turn of its own on the backend, with no user bubble.
    const assistantMessage = createAssistantMessage(assistantMessageId, newestKnownTurn() + 1);
    setMessages((prev) => appendMessage(prev, assistantMessage));
    currentMessageRef.current = assistantMessageId;

    setIsLoading(true);
    setMessageError(null);
    // New-run boundary like send/edit/regenerate: a pre-interrupt fallback
    // suggestion would go stale if the resumed run's model calls (which start
    // from the primary again) succeed; a re-fired model_fallback re-sets it.
    setFallbackSuggestion(null);
    wasStoppedRef.current = false;
    backgroundReconnectRef.current = false;
    acquireStreamOwnership(threadId);
    // Fresh AbortController so stopWorkflow can abort this resumed stream.
    const abortController = new AbortController();
    mainStreamAbortRef.current = abortController;

    // Prepare refs for event handlers — use persistent subagent state
    const refs = buildStreamRefs();

    const wasInterruptedRef = { current: false };
    const processStreamFrame = createStreamEventProcessor(runtime, streamRouterDeps, assistantMessageId, refs, getTaskIdFromEvent, wasInterruptedRef);
    // Once the stream is open, a refusal arrives as a frame.
    let refusalFrame: SSEEvent | null = null;
    const processEvent = (event: SSEEvent) => {
      if (isStaleForkRefusal(event)) refusalFrame = event;
      processStreamFrame(event);
    };

    // One request_key per resume (keyed by the interrupt answers), reused on
    // a retransmit after a lost response — see createRequestKeyTracker.
    const requestKey = requestKeyRef.current.take(
      `hitl|${threadId}|${JSON.stringify(hitlResponse)}`,
    );
    let wasDisconnected = false;
    // An answer runs what the composer shows now: the model, effort and speed
    // are the thread's own picks, which may have changed since the send that
    // asked. With no composer mounted, the last send's.
    const { model, reasoningEffort, fastMode } = readComposerOptions?.() ?? lastModelOptionsRef.current;
    try {
      const result = await sendHitlResponse(hitlResponse, workspaceId, threadId, {
        onEvent: processEvent,
        llmModel: model,
        reasoningEffort,
        fastMode,
        // Latch the fresh run_id from response headers before the first SSE
        // body byte. Without this, an early disconnect (between the pre-POST
        // clear above and the metadata frame) would let
        // attemptReconnectAfterDisconnect fall back to the prior run's
        // TaskInfo and silently hang.
        onRunIdResolved: (runId) => {
          requestKeyRef.current.settle(requestKey);
          currentRunIdRef.current = runId;
          // The backend opened a run, so the resume was admitted. Settled here
          // rather than left to the `finally` because this is the first moment
          // "Resumed" is true, and the wait is visible on the card.
          admitted = true;
          settleResume(true);
        },
        signal: abortController.signal,
        requestKey,
      });

      // User hit stop: stopWorkflow already finalized + tore down. Exception: a
      // foreground handler aborted this stream on tab resume (background abort,
      // not a user stop) — treat it as a disconnect and re-kick the reconnect so
      // the resumed-from-stop turn recovers instead of dying silently.
      if (result?.aborted || wasStoppedRef.current) {
        if (backgroundReconnectRef.current && !wasStoppedRef.current) {
          backgroundReconnectRef.current = false;
          wasDisconnected = true;
          attemptReconnectAfterDisconnect(assistantMessageId);
        }
        return;
      }

      if (recoverStaleAnswer(refusalFrame)) return;

      // Ahead of the disconnect for the same reason as a send: the run a
      // retry notice names is over, and the drop after it is the server's. A
      // resume the server never started is not kept here to send again; the
      // reconnect finds no run and reloads the thread, whose pause still waits.
      // The server ends that body cleanly, so the notice stands in for the drop.
      const notice = refs.retryNotice?.recovery === 'retry' ? refs.retryNotice : undefined;
      if (notice && settleRetry(notice, currentMessageRef.current || assistantMessageId, 0, { model, reasoningEffort, fastMode }, null)) {
        wasDisconnected = true;
        return;
      }

      if ((result?.disconnected || refs.retryNotice?.recovery === 'resend') && !notice) {
        console.log('[HITL] Stream disconnected, attempting reconnect');
        wasDisconnected = true;
        // A dropped link, not a refusal: the run is live and reconnect owns it.
        admitted = true;
        attemptReconnectAfterDisconnect(assistantMessageId);
        return;
      }

      // Mark message as complete (use live ref in case steering_delivered switched it)
      {
        const finalId = currentMessageRef.current || assistantMessageId;
        setMessages((prev) =>
          updateMessage(prev, finalId, (msg) => finalizeAssistantMessage(msg, wasInterruptedRef.current ? 'paused' : 'completed'))
        );
        markTranscriptPersisted();
      }
      admitted = true;
    } catch (err: unknown) {
      if ((err as Error)?.name === 'AbortError' || wasStoppedRef.current) {
        return;
      }
      // 409 duplicate_request: an earlier copy of this resume was already
      // accepted (its response was lost) — adopt that run instead of erroring.
      if (adoptDuplicateRun(err, assistantMessageId, requestKey)) {
        wasDisconnected = true;
        // Accepted, just not by this attempt — the pause is genuinely answered.
        admitted = true;
        return;
      }
      if (recoverStaleAnswer(err)) return;
      // 429 and 503 are both the quota service answering rather than a failure
      // to reach it, and it authors the copy the banner renders, except a
      // subscription's token refresh, which `refusalBanner` words. Stacking a
      // generic red bubble under that reads as a second, unrelated fault, so
      // the turn settles empty and MessageList hides it.
      const errObj = err as Record<string, unknown>;
      if (errObj.status === 429 || (errObj.status === 503 && (errObj.errorInfo as { type?: unknown } | undefined)?.type !== 'oauth_refresh_unavailable')) {
        // Only a 429 carries `rateLimitInfo`; the transport routes every other
        // structured detail into `errorInfo`. Reading just the first left a 503
        // with an empty object, which loses the outage copy and — because the
        // suppression keys off `type` — offers "Manage plan" to someone who
        // cannot reach the service to spend anything.
        const info = (errObj.rateLimitInfo ||
          errObj.errorInfo ||
          {}) as Record<string, unknown>;
        const platformUrl = (import.meta.env.VITE_PLATFORM_URL as string | undefined) || '/account';
        setMessageError(buildRateLimitError(info, platformUrl));
        setMessages((prev) =>
          updateMessage(prev, assistantMessageId, (msg) => finalizeAssistantMessage(msg, wasInterruptedRef.current ? 'paused' : 'completed'))
        );
      } else {
        console.error('[HITL] Error resuming turn:', err);
        const { banner } = refusalBanner(err);
        if (banner !== null) setMessageError(banner);
        setMessages((prev) =>
          updateMessage(prev, assistantMessageId, (msg) =>
            failAssistantMessage(msg, { message: 'Failed to resume this turn. Please try again.' }))
        );
      }
    } finally {
      // A refusal, abort or stop leaves `admitted` false: restore the interrupt
      // board and put the card back so a top-up can retry. Already-settled
      // paths are a no-op, so a run that did open is never reverted. A pause a
      // fork replaced is not put back, since answering it is refused again.
      settleResume(admitted || pauseGone);
      if (mainStreamAbortRef.current === abortController) {
        mainStreamAbortRef.current = null;
      }
      if (!wasDisconnected && !wasInterruptedRef.current && !wasStoppedRef.current) {
        const finalId = currentMessageRef.current || assistantMessageId;
        cleanupAfterStreamEnd(finalId);
      }
      // NOTE: an `assistant-hitl-*` bubble that finalizes empty (content landed
      // elsewhere, or the turn re-interrupted and the re-raise was deduped) must
      // NOT be pruned from state: a HITL resume is a backend turn, and
      // edit/regenerate map UI position → turn_index by counting non-steering
      // assistant bubbles. MessageList hides empty settled bubbles instead.
    }
  });

  const interruptAnswers = useInterruptAnswers({
    pendingInterrupt,
    setMessages,
    resumeWithHitlResponse,
    answerBoard,
    pendingPTCBackfillRef,
    armReportBackWatch,
    threadIdRef,
  });

  const insertNotification = useCallback(
    (text: string, variant: 'info' | 'success' | 'warning' = 'info', detail?: string) => {
      setMessages((prev) => appendMessage(prev, createNotificationMessage(text, variant, detail)));
    },
    [setMessages],
  );

  // =====================================================================
  // Edit / Regenerate / Retry handlers
  // =====================================================================

  /** Lazy-cached turn checkpoint data. Invalidated after each edit/regenerate. */
  type ThreadTurns = { turns: Array<{ edit_checkpoint_id: string | null; regenerate_checkpoint_id: string; turn_index: number }> };
  const turnCheckpointsRef = useRef<ThreadTurns | null>(null);

  /**
   * Helper: get or fetch turn checkpoints for the current thread.
   * Caches the result in turnCheckpointsRef until invalidated.
   */
  const getTurnCheckpoints = useCallback(async (): Promise<ThreadTurns | null> => {
    if (turnCheckpointsRef.current) return turnCheckpointsRef.current;
    const currentThreadId = threadIdRef.current;
    // A thread the server never created has no turns.
    if (!currentThreadId || currentThreadId === '__default__') return { turns: [] };
    try {
      const data = await fetchThreadTurns(currentThreadId);
      turnCheckpointsRef.current = data;
      return data;
    } catch (err) {
      console.error('[useChatMessages] Failed to fetch turn checkpoints:', err);
      return null;
    }
  }, []);

  /**
   * An edit or regenerate cuts the transcript before it reads the checkpoint it
   * forks from, so it takes the stream slot at the cut: a load or a report-back
   * attach landing in that read waits for the fork instead of writing over the
   * cut. A replay in flight is rebuilding the very transcript the fork would
   * cut, so the fork is refused until it lands; the cut is at an index, so no
   * older page goes above it meanwhile. A failed read hands the slot back,
   * which runs any load that waited.
   */
  const claimForkPreflight = (): boolean => {
    if (isStreamingRef.current || historyLoadingRef.current) return false;
    // Each fork reads the turns afresh: a reload since the last read can
    // have shown other turns than the ones cached.
    turnCheckpointsRef.current = null;
    sessionEpochRef.current += 1;
    historyWindow.hold();
    acquireStreamOwnership(threadId);
    return true;
  };

  /** A fork whose checkpoint read failed puts back the transcript it cut,
   *  and an edit's text. */
  const abandonForkPreflight = (cut: ForkCut, error: string) => {
    setIsLoading(false);
    setMessages([...cut.transcript]);
    setMessageError(error);
    if (cut.draft) returnEditDraft(cut.draft);
    historyWindow.release();
    releaseStreamOwnership();
  };

  /** A fork of a turn the server's thread does not hold under the number this
   *  view gave it: the chat changed after this view read it, so the fork goes
   *  back as one refused as stale, and the reload waiting on the slot runs. */
  const refuseStaleForkPreflight = (cut: ForkCut) => {
    setIsLoading(false);
    forkRefusal.stale(cut);
    historyWindow.release();
    releaseStreamOwnership();
  };

  /**
   * Helper: stream a forked or retried turn (shared by edit, regenerate, retry).
   * Edit/regenerate fork from an explicit `checkpointId`; retry goes through the
   * POST /retry attempt chain with `checkpointId=null` (the server resolves the
   * retry checkpoint). Sets up the assistant placeholder, event processor, and
   * stream lifecycle.
   * Stable, and always the last committed render's body, like resume: an edit
   * can come long after the transcript last changed, and has to send the
   * current platform, locale, timezone and runtime. `snapshot` is the render
   * the caller computed `truncateIndex` against. `rerun` marks another attempt
   * at a turn the transcript already shows, a send included, so its user
   * bubble stays as it is; `spent` counts the automatic resends before it,
   * and `send` is the send they resend while the server has kept none of it.
   * `cut` is what a fork's refusal gives back, and `shown` the bubbles the
   * caller already drew in its place.
   */
  const streamFromCheckpoint = useStableHandler(async (request: TurnRequest, truncateIndex: number, snapshot: readonly ChatMessage[], modelOptions: ModelOptions = {}, { rerun, cut, shown }: { rerun?: { spent: number; send?: UnstartedSend | null }; cut?: ForkCut | null; shown?: ShownBubbles } = {}) => {
    const { message, checkpointId, forkFromTurn, retryOf } = request;
    const viaRetryEndpoint = retryOf !== null;
    // Callers check the slot is free: an edit or regenerate already holds it
    // for its checkpoint read, and takes it again below with the same result.

    // Edit/regenerate/retry are chat activity — bump like a fresh send.
    bumpThreadNavOrder(workspaceId, threadIdRef.current);

    setIsLoading(true);
    setMessageError(null);
    setFallbackSuggestion(null);
    setHasActiveSubagents(false);
    // Like the fresh-send path, do NOT clear terminalTaskOutcomesRef here: a
    // fork/retry rewrites the turn but does not tear down the subagent-card
    // projection, and a re-run spawns fresh task ids — stale evidence for the
    // old ids is harmless, while wiping it could un-settle a live-closed sibling.
    wasStoppedRef.current = false;
    sessionEpochRef.current += 1;
    backgroundReconnectRef.current = false;
    acquireStreamOwnership(threadId);

    // Truncate messages and add new user message (if editing) + assistant placeholder
    const assistantMessageId = shown?.assistantId ?? `assistant-${Date.now()}`;
    // Edit/regenerate opens a fresh backend run.
    beginRun();
    // A fork truncates persisted turns > forkFromTurn server-side; pin the
    // rendered-turn watermark to the fork turn so the reactivation staleness
    // check compares against the post-truncation reality (a stale-high
    // watermark would suppress a genuinely-needed reload later).
    if (forkFromTurn !== null) {
      lastRenderedTurnIndexRef.current = forkFromTurn;
    }
    // Fresh AbortController so stopWorkflow can abort this stream's reader.
    const abortController = new AbortController();
    mainStreamAbortRef.current = abortController;

    // A fork reruns the turn it names. Another attempt (retry, resend) reruns
    // the turn of the bubble it replaces, or the newest one when it appends.
    // A send opens the next turn, in place of one the server never stored too.
    const projected = projectTurns(snapshot);
    const replaced = projected[truncateIndex];
    const plainSend = forkFromTurn === null && !viaRetryEndpoint;
    const turnIndex = forkFromTurn
      ?? replaced?.turnIndex
      ?? (plainSend && (replaced !== undefined || !rerun) ? newestKnownTurn() + 1 : null)
      ?? newestTurn(snapshot)
      ?? newestKnownTurn() + 1;
    // Another attempt at a hidden opener's turn, a resend or a Retry, draws its
    // reply as the opener did, with no message above it, flagged so a replay
    // can still find it and the turn still offers no Regenerate. Its Retry
    // brings the context again, as the opener's did.
    const rerunsHidden = forkFromTurn === null && (replaced?.message as Partial<AssistantMessage> | undefined)?.hiddenOpener === true;
    const reopensHidden = plainSend && rerunsHidden;
    const assistantMessage: AssistantMessage = {
      ...createAssistantMessage(assistantMessageId, turnIndex),
      ...(rerunsHidden ? { hiddenOpener: true } : {}),
    };
    if (rerunsHidden) holdHiddenOpenerRetry(assistantMessageId, request.additionalContext);
    const userMessage = message && !rerun ? createUserMessage(message, turnIndex, { id: shown?.userId }) : null;
    // Another attempt at a send the server never stored keeps its message,
    // which takes the send's turn as it goes again.
    const before = snapshot[truncateIndex - 1];
    const resent = plainSend ? rerun?.send ?? null : null;
    const resentUserId = resent?.userId
      ?? (rerun && plainSend && replaced?.turnIndex === null && before?.role === 'user' && before.unadmitted ? before.id : null);
    const sendUserId = userMessage?.id ?? resentUserId;
    // Another attempt at a hidden opener is a send still, with no message of
    // it on screen: the opener never drew one, and its reply is the bubble
    // the server's verdict on it settles.
    const opensSend = sendUserId !== null || reopensHidden;
    // A resend's send already counted its turn, so the count goes back to
    // where it stood before the send.
    const watermarkBefore = resent ? resent.watermarkBefore : lastRenderedTurnIndexRef.current;
    if (opensSend && turnIndex > (watermarkBefore ?? -1)) lastRenderedTurnIndexRef.current = turnIndex;

    if (userMessage) {
      recentlySentTrackerRef.current.track(message!.trim(), userMessage.timestamp, userMessage.id);
    }

    // Rebuild the rendered-interrupt set from the cards that survive the
    // truncation. The fork re-executes the turn server-side, and LangGraph
    // interrupt ids are deterministic — the new run can legitimately re-raise
    // the id of a card this truncation removes; a stale entry would suppress
    // the new card and leave the interrupt unanswerable. Done synchronously
    // (not in the setMessages updater) so the first stream event can't race
    // the rebuild. The caller's snapshot, since truncateIndex indexes into it.
    renderedInterruptIdsRef.current = collectRenderedInterruptIds(snapshot.slice(0, truncateIndex));
    // The messages the cut removes take their open edit boxes along, as the
    // box warned of messages after the one edited. A refused fork gives them
    // back with the messages.
    const dropped: EditDraft[] = [];
    for (const { message: removed, turnIndex: turn } of projected.slice(truncateIndex)) {
      if (removed.role !== 'user') continue;
      const key = { turnIndex: turn, original: removed.content.trim(), messageId: removed.id };
      const text = editDrafts.get(key);
      if (text === undefined) continue;
      dropped.push({ threadId: threadIdRef.current, ...key, text });
      editDrafts.drop(key);
    }

    setMessages((prev) => {
      const truncated = resentUserId === null
        ? prev.slice(0, truncateIndex)
        : prev.slice(0, truncateIndex).map((m) => (m.id === resentUserId ? { ...m, unadmitted: false, turnIndex } : m));
      const newMsgs = userMessage
        ? [...truncated, userMessage, assistantMessage]
        : [...truncated, assistantMessage];
      newMessagesStartIndexRef.current = newMsgs.length;
      return newMsgs;
    });
    historyWindow.release();
    currentMessageRef.current = assistantMessageId;

    // Invalidate turn checkpoints cache (branch creates new checkpoints)
    turnCheckpointsRef.current = null;

    // One request_key per retry click / fork, reused on a retransmit after a
    // lost response — see createRequestKeyTracker.
    const requestKey = request.requestKey ?? requestKeyRef.current.take(
      viaRetryEndpoint
        ? `retry|${threadId}`
        : `fork|${threadId}|${checkpointId ?? ''}|${forkFromTurn ?? ''}|${message ?? ''}`,
    );
    // The message a reply answered when it is the one right above it, which a
    // refusal that needs the turn's input sent again gives back to the
    // composer. A request that carries its own text, or a reply to an answered
    // pause, was asked by something else.
    const asked = !message && before?.role === 'user' && !isSteeringUserMessage(before)
      && projected[truncateIndex - 1]?.turnIndex === turnIndex
      ? before.content
      : null;
    // A fork cut turns the server still holds, so a refused one puts them
    // back, its resends included. A retry replaced only its own reply, which
    // shows the refusal as before, unless the chat changed under it, the
    // reply never failed, or the user has to send its input again.
    const forkCut: ForkCut | null = forkFromTurn === null
      ? null
      : {
          ...(cut ?? { kind: message ? 'edit' : 'regenerate', transcript: snapshot, draft: null }),
          dropped: [...(cut?.dropped ?? []), ...dropped],
          asked,
        };
    const recoverRefusedFork = (source: unknown) => {
      if (forkCut !== null) return forkRefusal.recover(source, forkCut);
      if (viaRetryEndpoint) return isRecoverableRetryRefusal(source) && forkRefusal.recover(source, { kind: 'retry', transcript: snapshot, draft: null, asked });
      return isStaleForkRefusal(source) && forkRefusal.recover(source, { kind: 'regenerate', transcript: snapshot, draft: null });
    };
    const admission = !opensSend ? null : trackPlainSend({
      userId: sendUserId,
      assistantId: assistantMessageId,
      turnIndex,
      request: { ...request, requestKey },
      watermarkBefore,
    });
    let wasDisconnected = false;
    const wasInterruptedRef = { current: false };
    try {
      const refs = buildStreamRefs();
      const processStreamFrame = createStreamEventProcessor(runtime, streamRouterDeps, assistantMessageId, refs, getTaskIdFromEvent, wasInterruptedRef);
      // Once the stream is open, a refusal arrives as a frame.
      let refusalFrame: SSEEvent | null = null;
      const processEvent = (event: SSEEvent) => {
        if (forkRefusalMessage(event) !== null) refusalFrame = event;
        admission?.observe(event);
        processStreamFrame(event);
      };

      // Retry goes through POST /retry (v4 attempt chain: server validates
      // the latest attempt + resolves the checkpoint, no truncation); edit/
      // regenerate keep the fork path.
      // Latch run_id from response headers — see handleSendMessage for the same
      // closing-the-race rationale. Shared by both branches.
      const latchRunId = (runId: string) => {
        requestKeyRef.current.settle(requestKey);
        currentRunIdRef.current = runId;
      };
      const result = viaRetryEndpoint
        ? await sendRetryStream(
            workspaceId,
            threadId,
            processEvent,
            modelOptions.model || null,
            modelOptions.reasoningEffort || null,
            modelOptions.fastMode ?? null,
            latchRunId,
            abortController.signal,
            requestKey,
            // A Retry that names no run retries the newest one this view saw,
            // which the server refuses when another run came since.
            retryOf.runId ?? latestRunIdRef.current,
            request.additionalContext,
          )
        : await sendChatMessageStream(message || '', workspaceId, threadId, {
            onEvent: processEvent,
            additionalContext: request.additionalContext,
            locale: userLocale,
            timezone: userTimezone,
            checkpointId,
            forkFromTurn,
            expectedLatestRunId: latestRunIdRef.current,
            llmModel: modelOptions.model || null,
            reasoningEffort: modelOptions.reasoningEffort || null,
            fastMode: modelOptions.fastMode ?? null,
            platform,
            onRunIdResolved: latchRunId,
            signal: abortController.signal,
            requestKey,
            threadSettings: { subagentsAllowed: request.subagentsAllowed },
          });

      // User hit stop: stopWorkflow already finalized + tore down. Exception: a
      // foreground handler aborted this stream on tab resume (background abort,
      // not a user stop) — treat it as a disconnect and re-kick the reconnect so
      // the resumed turn recovers instead of dying silently.
      if (result?.aborted || wasStoppedRef.current) {
        if (backgroundReconnectRef.current && !wasStoppedRef.current) {
          backgroundReconnectRef.current = false;
          wasDisconnected = true;
          attemptReconnectAfterDisconnect(assistantMessageId);
        }
        return;
      }

      if (recoverRefusedFork(refusalFrame)) return;
      if (admission?.refused()) {
        admission.unadmit();
        return;
      }

      // A resend that fails the same way counts against the same turn's
      // budget, which is what keeps a chain of notices from looping.
      if (refs.retryNotice && settleRetry(refs.retryNotice, currentMessageRef.current || assistantMessageId, rerun?.spent ?? 0, modelOptions, request, forkCut, plainSend ? admission?.unstarted() ?? null : null)) {
        wasDisconnected = true;
        return;
      }

      if (result?.disconnected && !refs.retryNotice) {
        wasDisconnected = true;
        attemptReconnectAfterDisconnect(assistantMessageId);
        return;
      }

      const finalId = currentMessageRef.current || assistantMessageId;
      setMessages((prev) =>
        updateMessage(prev, finalId, (msg) => finalizeAssistantMessage(msg, wasInterruptedRef.current ? 'paused' : 'completed'))
      );
      // As for a send: nothing to replay when the server started no run. A
      // fork stays as it is, since the turn it would replace is still stored.
      if (refs.retryNotice?.recovery !== 'resend') markTranscriptPersisted();
      else if (plainSend) admission?.unadmit();
    } catch (err: unknown) {
      if ((err as Error)?.name === 'AbortError' || wasStoppedRef.current) {
        return;
      }
      if (sendUserId !== null && settleSteeredSend(err, { userId: sendUserId, assistantId: assistantMessageId, turnIndex, watermarkBefore, requestKey })) return;
      // 409 duplicate_request: an earlier copy of this retry/fork was already
      // accepted (its response was lost) — adopt that run instead of erroring.
      if (retryFailedDuplicate(err, assistantMessageId, request, modelOptions, requestKey) || adoptDuplicateRun(err, assistantMessageId, requestKey)) {
        wasDisconnected = true;
        return;
      }
      if (recoverRefusedFork(err)) return;
      admission?.unadmit();
      console.error('[streamFromCheckpoint] Error:', err);
      const { banner } = refusalBanner(err);
      if (banner !== null) setMessageError(banner);
      setMessages((prev) =>
        updateMessage(prev, assistantMessageId, (msg) =>
          failAssistantMessage(msg, { message: 'Failed to process request. Please try again.' }))
      );
    } finally {
      if (mainStreamAbortRef.current === abortController) {
        mainStreamAbortRef.current = null;
      }
      if (!wasDisconnected && !wasInterruptedRef.current && !wasStoppedRef.current) {
        const finalId = currentMessageRef.current || assistantMessageId;
        setMessages((prev) =>
          // Literal, not the ref: the guard above already read it, and React
          // runs this updater later, so a ternary here would answer from a
          // different moment than the branch it is standing in.
          updateMessage(prev, finalId, (msg) => finalizeAssistantMessage(msg, 'completed'))
        );
        cleanupAfterStreamEnd(finalId);
      }
    }
  });

  /**
   * Ends a stream that closed on a retry notice: schedules the resend the
   * notice's recovery calls for, or marks the reply failed and hands its Retry
   * that recovery. True when the resend owns the turn, so the caller skips its
   * own finalize and cleanup. `request` is what the stream sent, and `cut`
   * the fork it stands for, and `send` the send the server started no run
   * of, which a resend of it carries.
   */
  const settleRetry = (notice: RetryNotice, bubbleId: string, spent: number, modelOptions: ModelOptions, request: TurnRequest | null, cut: ForkCut | null = null, send: UnstartedSend | null = null): boolean =>
    settleRetryNotice(
      {
        setMessages,
        getMessages: liveMessages.get,
        request,
        failedRunId: currentRunIdRef.current,
        resend: (next, truncateIndex, snapshot, nextSpent) => {
          void streamFromCheckpoint(next, truncateIndex, snapshot, modelOptions, { rerun: { spent: nextSpent, send }, cut });
        },
        holdRetry: (held) => {
          if (held) heldRetriesRef.current.set(held.bubbleId, { ...held, cut });
        },
        wasStoppedRef,
        sessionEpochRef,
        threadIdRef,
        failedText: t('chat.errorInternalHeadline'),
        exhaustedText: t('chat.autoRetryExhausted'),
      },
      notice,
      bubbleId,
      spent,
    );

  /**
   * Sends `text` as the user message of turn `turnIndex`, drawn at `msgIndex`
   * of `transcript`, in place of the one there. A turn whose run died before
   * its first checkpoint has no /turns entry of its own: its message forks
   * where the next entry's does, from the state before both, so the first
   * entry at or after the turn serves. The thread's first message forks from
   * the empty root the server names for it. With no entry at all, the chat
   * changed since this view read it.
   */
  const streamEdit = async (
    text: string,
    turnIndex: number,
    msgIndex: number,
    cut: ForkCut,
    turnsData: ThreadTurns,
    modelOptions: ModelOptions,
    shown: ShownBubbles,
  ) => {
    const turn = turnsData.turns.find((entry) => entry.turn_index >= turnIndex);
    if (!turn) {
      refuseStaleForkPreflight(cut);
      return;
    }
    // Only a resume has no edit checkpoint, and its answer has no message bubble.
    if (!turn.edit_checkpoint_id) {
      abandonForkPreflight(cut, 'Failed to process request');
      return;
    }
    await streamFromCheckpoint(forkRequest(text, turn.edit_checkpoint_id, turnIndex), msgIndex, cut.transcript, modelOptions, { cut, shown });
  };

  /**
   * Edit a user message: truncate to before that message, send modified content
   * from the checkpoint before the original message was added.
   */
  const handleEditMessage = useCallback(async (messageId: string, newContent: string, modelOptions: ModelOptions = {}) => {
    if (!newContent?.trim()) return;

    const transcript = liveMessages.get();
    const msgIndex = transcript.findIndex((m) => m.id === messageId);
    if (msgIndex === -1) return;

    // Steering bubbles are mid-turn injections with no boundary in /turns —
    // an edit fork would land on the NEXT turn and leave the original
    // steering text in the agent's context. The UI hides the pencil for
    // them; this guards every other caller.
    const editTarget = transcript[msgIndex];
    if (isSteeringUserMessage(editTarget)) {
      setMessageError("Steering messages can't be edited");
      return;
    }

    // The turn the bubble belongs to, never its position: a paged transcript
    // starts mid-thread.
    const projected = projectTurns(transcript);
    const ownTurn = projected[msgIndex].turnIndex;
    const draft: EditDraft = { threadId: threadIdRef.current, turnIndex: ownTurn, original: (editTarget as UserMessage).content, messageId, text: newContent };
    // A message the server never stored has no turn of its own. Its edit takes
    // the place of the first stored turn after it, so it replaces everything
    // after it as any edit does.
    const turnIndex = ownTurn ?? projected.slice(msgIndex + 1).find(hasTurn)?.turnIndex ?? null;

    // With no stored turn after it, nothing after it reached the server
    // either, and the edit goes out as a send in their place.
    if (turnIndex === null) {
      if (!claimForkPreflight()) {
        returnEditDraft(draft);
        return;
      }
      await streamFromCheckpoint(sendRequest(newContent, null), msgIndex, transcript, modelOptions);
      return;
    }
    const cut: ForkCut = {
      kind: 'edit',
      transcript,
      draft,
    };

    // A turn holds the slot, so the edit waits in its box for the user.
    if (!claimForkPreflight()) {
      returnEditDraft(cut.draft!);
      return;
    }

    // Immediate visual feedback: truncate, show edited message + loading placeholder.
    setIsLoading(true);
    setMessageError(null);
    setFallbackSuggestion(null);
    const edited = createUserMessage(newContent, turnIndex);
    const assistantId = `assistant-${Date.now()}`;
    setMessages((prev) => [
      ...prev.slice(0, msgIndex),
      edited,
      createAssistantMessage(assistantId, turnIndex),
    ]);

    const turnsData = await getTurnCheckpoints();
    if (!turnsData) {
      abandonForkPreflight(cut, 'Failed to process request');
      return;
    }
    await streamEdit(newContent, turnIndex, msgIndex, cut, turnsData, modelOptions, { userId: edited.id, assistantId });
  // The slot helpers reach only refs and threadId.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [liveMessages, threadId, getTurnCheckpoints, streamFromCheckpoint, setMessages]);

  /**
   * Regenerate an assistant response: truncate the assistant message,
   * re-run from the checkpoint that has the user message but before AI response.
   */
  const handleRegenerate = useCallback(async (messageId: string, modelOptions: ModelOptions = {}) => {
    const transcript = liveMessages.get();
    const msgIndex = transcript.findIndex((m) => m.id === messageId);
    if (msgIndex === -1) return;

    // The turn the bubble belongs to (a continuation folds back to the turn it
    // continues), never its position: a paged transcript starts mid-thread.
    const projected = projectTurns(transcript);
    const turnIndex = projected[msgIndex].turnIndex;
    // A reply the server never stored has no turn to run again; its Retry
    // sends its message.
    if (turnIndex === null) return;
    // Nor does a turn that opened hidden: its words rode its send's context,
    // which neither its stored row nor the input checkpoint a regenerate
    // replays holds, so the rerun would answer an empty turn. The list offers
    // it no Regenerate; this guards every other caller.
    if (hiddenOpenerTurns(projected).has(turnIndex)) return;

    // A turn can render as several bubbles (a steered turn's pre-steering half
    // and continuations, the halves of a stream a reconnect resumed) but has
    // only one regenerate: the whole turn re-runs from its input checkpoint,
    // without the mid-run steering. Normalize truncation back to the turn's
    // first assistant bubble so the stale halves and the steering bubbles leave
    // the transcript together with the re-run. Not past the message the turn
    // runs from. A send the server never stored holds no turn, and is passed
    // over.
    let truncateIndex = msgIndex;
    for (let i = msgIndex - 1; i >= 0; i--) {
      const { message, turnIndex: turn } = projected[i];
      if (turn === null) continue;
      if (opensTurn(message)) break;
      if (message.role !== 'assistant') continue;
      if (turn !== turnIndex) break;
      truncateIndex = i;
    }

    if (!claimForkPreflight()) return;

    // Immediate visual feedback: truncate at the assistant message, show loading placeholder.
    const cut: ForkCut = { kind: 'regenerate', transcript, draft: null };
    setIsLoading(true);
    setMessageError(null);
    setFallbackSuggestion(null);
    const assistantId = `assistant-${Date.now()}`;
    setMessages((prev) => [
      ...prev.slice(0, truncateIndex),
      createAssistantMessage(assistantId, turnIndex),
    ]);

    const turnsData = await getTurnCheckpoints();
    if (!turnsData) {
      abandonForkPreflight(cut, 'Failed to process request');
      return;
    }
    const userIndex = projected.findLastIndex(
      ({ message, turnIndex: turn }, i) => i < truncateIndex && turn === turnIndex && message.role === 'user' && !isSteeringUserMessage(message),
    );
    const userMessage = userIndex === -1 ? null : transcript[userIndex] as UserMessage;
    const turn = turnsData.turns.find((entry) => entry.turn_index === turnIndex);
    if (turn) {
      // Truncate at the turn's first assistant bubble (keep everything before it, including user msg)
      await streamFromCheckpoint(forkRequest(null, turn.regenerate_checkpoint_id, turnIndex), truncateIndex, transcript, modelOptions, { cut, shown: { assistantId } });
      return;
    }
    if (!userMessage) {
      abandonForkPreflight(cut, 'Failed to process request');
      return;
    }
    // No checkpoint holds the turn's message to replay. The reply's own Retry
    // holds what its send carried, so it goes as that Retry would.
    const held = heldRetriesRef.current.get(messageId);
    if (held) {
      heldRetriesRef.current.delete(messageId);
      await streamFromCheckpoint(held.request, truncateIndex, transcript, modelOptions, { rerun: { spent: 0 }, cut: held.cut, shown: { assistantId } });
      return;
    }
    // Otherwise only its text is known here, which is the whole of it only
    // when it carried no attachment, chart selection or widget.
    if (carriesContext(userMessage)) {
      abandonForkPreflight(cut, 'Failed to process request');
      return;
    }
    // The message goes again in its own place, as an edit that keeps its text,
    // and its bubble, still on screen, keeps its id.
    await streamEdit(userMessage.content, turnIndex, userIndex, cut, turnsData, modelOptions, { userId: userMessage.id, assistantId });
  // Same as handleEditMessage: the slot helpers reach only refs and threadId.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [liveMessages, threadId, getTurnCheckpoints, streamFromCheckpoint, setMessages]);

  /**
   * Retry the last failed turn as a new attempt on the same turn (v4 attempt
   * chain). The backend validates the latest attempt and resolves the retry
   * checkpoint itself — no client checkpoint fetch, no fork/truncation of
   * persisted turns. The UI still replaces the errored bubble in place so the
   * positional assistant-bubble count stays aligned with backend turn_index.
   * A reply this session saw fail sends the recovery its failure named, with
   * the context the send carried.
   */
  const handleRetry = useCallback(async (modelOptions: ModelOptions = {}) => {
    if (isStreamingRef.current) return;
    const transcript = liveMessages.get();
    const lastErrorIndex = transcript.findLastIndex((m) => m.role === 'assistant' && (m as AssistantMessage).error);
    const truncateIndex = lastErrorIndex !== -1 ? lastErrorIndex : transcript.length;
    const bubbleId = transcript[lastErrorIndex]?.id;
    const held = (bubbleId && heldRetriesRef.current.get(bubbleId)) || null;
    const request = retryRequestFor(held, bubbleId);
    if (bubbleId) heldRetriesRef.current.delete(bubbleId);
    // A held fork resend carries the fork it stands for, which a refusal of
    // it puts back.
    await streamFromCheckpoint(request, truncateIndex, transcript, modelOptions, { rerun: { spent: 0 }, cut: held && request === held.request ? held.cut : null });
  }, [liveMessages, streamFromCheckpoint]);

  // A PTC run's sandbox acquisition settles every folder on its computer, which
  // rewrites the dir_name and previous_dir_names that agent paths fold
  // against, siblings' too: a folder one lands on stops being another's former
  // folder. Cached rows would keep the old folders for minutes.
  const refreshWorkspaceFolder = workspaceId
    ? () => { void queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.details() }); }
    : null;

  /** Cross-lane callbacks for the live event router; direct references, so
   * this literal must stay below every referent. */
  const streamRouterDeps: StreamRouterDeps = {
    applyFallbackSuggestion,
    applyModelStatus,
    clearModelStatus,
    insertNotification,
    releaseStreamOwnership,
    attachSubagentMux,
    setMarketWatch,
    refreshMarketWatch,
    refreshWakeups,
    refreshCommands,
    refreshWorkspaceFolder,
    armReportBack: (owed) => armReportBackWatch(threadIdRef.current, null, null, undefined, owed),
    refreshWorkspaces: () => invalidateNewWorkspace(queryClient),
    // A turn other than the one this view counted for the run's bubble means
    // turns were added where the view never saw them. The stream stamps the
    // server's number, and only a reload shows those turns.
    onAdmittedTurn: (bubbleId, turn) => {
      const counted = turnOf(liveMessages.get(), bubbleId);
      if (counted !== undefined && counted !== turn) requestThreadReload();
    },
    readMessage: (bubbleId) => liveMessages.get().find((m) => m.id === bubbleId),
  };

  /** Composition-level callbacks for the recovery/ownership lifecycle; direct
   * references, so this literal must stay below every referent. */
  const recoveryDeps: RecoveryDeps = {
    createProcessor: (assistantMessageId, refs, wasInterruptedRef) =>
      createStreamEventProcessor(runtime, streamRouterDeps, assistantMessageId, refs, getTaskIdFromEvent, wasInterruptedRef),
    buildStreamRefs,
    clearSubagentCards,
    isSettledTask,
    attachSubagentMux,
    muxOpenTaskIds,
    markTranscriptPersisted,
    clearModelStatus,
    finalizePendingTodos,
    refreshComputerAfterTurn: () => refreshComputersAfterTurn(queryClient),
    refreshWorkspaceFolder,
    reportBackWatch,
  };

  return {
    // Which messages there are; the text as of the last write that was not a
    // chunk. Render the text from `liveMessages` (useLiveMessages).
    messages,
    liveMessages: liveView,
    threadId,
    threadModels,
    isLoading,
    marketWatch,
    hasActiveSubagents,
    awaitingReportBack,
    reportBackOwed,
    reportBackOwedThreads,
    reportBackOwedKinds,
    workspaceStarting,
    turnHeld,
    isCompacting,
    setIsCompacting,
    compactionTokens,
    queuedSend,
    isLoadingHistory,
    historyLoadFailed,
    isLoadingThread,
    // Older history, paged in above the transcript as the reader scrolls up.
    olderHistory: { status: olderHistoryStatus, load: loadOlderHistory },
    isReconnecting,
    modelStatus,
    fallbackSuggestion,
    clearFallbackSuggestion,
    reconnectIfStaleRun: reportBackWatch.reconnectIfStaleRun,
    // Tells this view's own run apart from one the user feed reports
    // starting elsewhere.
    isOwnRun,
    messageError,
    returnedSteering,
    clearReturnedSteering: () => setReturnedSteering(null),
    returnedEdit,
    clearReturnedEdit,
    editDrafts,
    handleSendMessage,
    stopWorkflow,
    stopCompaction,
    pendingInterrupt,
    ...interruptAnswers,
    tokenUsage,
    isShared,
    insertNotification,
    handleEditMessage,
    handleRegenerate,
    handleRetry,
    handleThumbUp,
    handleThumbDown,
    feedbackByTurn,
    resolveSubagentIdToAgentId,
    getSubagentHistory,
    getTaskRuns,
    putTaskRun,
    subagentHistoryEntries,
    hydrateTaskTranscript,
    sendSubagentInstruction,
  };
}
