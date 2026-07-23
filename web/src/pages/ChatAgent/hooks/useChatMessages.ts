/**
 * Manages chat messages and SSE streaming for a workspace.
 * Handles thread persistence, message sending, history loading,
 * and streaming updates.
 */

import type React from 'react';
import { useState, useRef, useEffect, useCallback } from 'react';
import { useTranslation } from 'react-i18next';
import { useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import { useUser } from '@/hooks/useUser';
import { sendChatMessageStream, sendRetryStream, getWorkflowStatus, sendHitlResponse, fetchThreadTurns, cancelWorkflow } from '../utils/api';
import { peekThreadMux } from '../session/stream/threadStreamMux';
import type { WorkflowStatusResponse } from '../utils/api';
// Imported from the dependency-free signal module (not `../utils/api`) so this
// keeps decoding wire status even in the hook tests that fully mock `../utils/api`.
import { shouldArmForStatus } from '../utils/reportBackSignal';
import { useReportBackWatch } from './useReportBackWatch';
import { useChatFeedback } from './useChatFeedback';
import { toast } from '@/components/ui/use-toast';
import { buildRateLimitError, type StructuredError } from '@/utils/rateLimitError';
import { getStoredThreadId, setStoredThreadId } from './utils/threadStorage';
import { type SubagentTokenUsage, ZERO_USAGE } from '../utils/tokenUsage';
import { computeSteeringBoundary } from '../session/stream/steeringRollback';
import { bumpThreadNavOrder } from './useNavigationData';
export { removeStoredThreadId } from './utils/threadStorage';
import { createUserMessage, createAssistantMessage, createNotificationMessage, appendMessage, updateMessage, type AttachmentMeta } from './utils/messageHelpers';
import type { AssistantMessage, UserMessage } from '@/types/chat';
import type { PreviewData } from './utils/types';
import { createRecentlySentTracker } from './utils/recentlySentTracker';
import { createRequestKeyTracker } from './utils/requestKey';
import { handleReasoningSignal, handleTextContent } from './utils/streamEventHandlers';
import { useMarketWatch } from './useMarketWatch';
// Chart-annotation live bridge: writes agent-drawn annotations into the
// shared MarketView store so the desktop MarketView chat panel (which uses
// this engine for both flash and PTC) renders them live. Harmless on the
// standalone /chat page — the store simply has no chart consumer there.

// --- Module scope extracted to session/types + utils (W1) ---
import type {
  MessageRecord, TokenUsage, PendingInterrupt, PendingRejection,
  SSEEvent, ModelOptions, OffloadBatch, SubagentHistoryEntry, TaskRefs,
  HistoryInterruptInfo,
  ModelStatus, FallbackSuggestion,
} from '../session/types';
import { SECRETARY_ACTION_TYPES } from '../session/interrupts/buckets';
export type { ModelStatus, FallbackSuggestion } from '../session/types';
import type { ChatSessionRuntime } from '../session/runtime';
import { projectSubagentHistory } from '../session/subagents/projectHistory';
import { createSubagentMuxController, getTaskIdFromEvent } from '../session/subagents/muxSink';
import { loadConversationHistory as replayConversationHistory } from '../session/history/replayHistory';
import { createStreamEventProcessor, type StreamRouterDeps } from '../session/stream/processStreamEvent';
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


export function useChatMessages(
  workspaceId: string,
  initialThreadId: string | null = null,
  updateTodoListCard: ((todoData: Record<string, unknown>, isNew?: boolean) => void) | null = null,
  updateSubagentCard: ((agentId: string, data: Record<string, unknown>) => void) | null = null,
  finalizePendingTodos: (() => void) | null = null,
  onOnboardingRelatedToolComplete: (() => void) | null = null,
  onFileArtifact: ((event: SSEEvent) => void) | null = null,
  onPreviewUrl: ((data: PreviewData) => void) | null = null,
  agentMode: string = 'ptc',
  clearSubagentCards: (() => void) | null = null,
  onWorkspaceCreated: ((info: { workspaceId: string; question: string }) => void) | null = null,
  platform: string | null = null,
) {
  const { t } = useTranslation();
  const queryClient = useQueryClient();

  // User locale/timezone — prefer saved preference, fall back to browser detection
  const { user } = useUser();
  const userLocale = user?.locale || navigator.language || 'en-US';
  const userTimezone = user?.timezone || Intl.DateTimeFormat().resolvedOptions().timeZone || 'America/New_York';

  // State
  const [messages, setMessages] = useState<MessageRecord[]>([]);
  const [threadId, setThreadId] = useState<string>(() => {
    // If threadId is provided from URL, use it; otherwise use localStorage
    if (initialThreadId) {
      return initialThreadId;
    }
    return workspaceId ? getStoredThreadId(workspaceId) : '__default__';
  });
  const [isLoading, setIsLoading] = useState(false);
  const [isLoadingHistory, setIsLoadingHistory] = useState(
    () => !!(initialThreadId && initialThreadId !== '__default__')
  );
  const [hasActiveSubagents, setHasActiveSubagents] = useState(false);  // Subagent streams open after main agent finished
  // false | 'starting' (generic cold start) | 'archived' (slow ~90s restore from cold storage).
  // Widen this union when backend adds a new sandbox_state discriminator.
  const [workspaceStarting, setWorkspaceStarting] = useState<false | 'starting' | 'archived'>(false);
  const [isCompacting, setIsCompacting] = useState<string | false>(false);  // Context compaction in progress (summarize/offload)
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
    planMode: boolean;
    additionalContext: Record<string, unknown>[] | null;
    attachmentMeta: Record<string, unknown>[] | null;
    modelOptions: ModelOptions;
    // id of the optimistic shimmer bubble shown while parked, so it can be
    // removed on flush (before the real send re-adds it) or on stop.
    messageId: string;
  } | null>(null);
  const [messageError, setMessageError] = useState<string | StructuredError | null>(null);
  // Steering returned by the server (agent finished before consuming it)
  const [returnedSteering, setReturnedSteering] = useState<string | null>(null);
  // HITL (Human-in-the-Loop) plan mode interrupt state
  const [pendingInterrupt, setPendingInterrupt] = useState<PendingInterrupt | null>(null);
  // When user clicks Reject on a plan, this stores the interruptId so the next message
  // sent via handleSendMessage is routed as rejection feedback via hitl_response.
  const [pendingRejection, setPendingRejection] = useState<PendingRejection | null>(null);

  // Token usage tracking (for context window progress ring)
  const [tokenUsage, setTokenUsage] = useState<TokenUsage | null>(null);
  const [isShared, setIsShared] = useState(false);

  // Bridge: handler modules define their own local SetMessages accepting Record<string, unknown>[]
  const setMessagesForHandlers = setMessages as unknown as (
    updater: (prev: Record<string, unknown>[]) => Record<string, unknown>[]
  ) => void;

  // Track current plan mode so HITL resume can forward it
  const currentPlanModeRef = useRef(false);

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
  // flash report-back) — in the latter case it supersedes that stream so the
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
  const newMessagesStartIndexRef = useRef(0); // Index where new messages start
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

  // Track all LLM models used in this thread (ordered, deduplicated)
  const [threadModels, setThreadModels] = useState<string[]>([]);

  // Track the model used by the most recent query in this thread (overwritten,
  // not deduplicated) so re-opening a history thread defaults to the last-used
  // model rather than the first. Distinct from threadModels because dedup there
  // makes its tail unreliable when a thread switches models back and forth.
  const [lastThreadModel, setLastThreadModel] = useState<string | null>(null);

  // Track if streaming is in progress to prevent history loading during streaming
  const isStreamingRef = useRef(false);

  const acquireStreamOwnership = (tid: string | null) => acquireOwnership(runtime, tid);
  // A mux resync that arrived mid-stream waits here: bumping the reload
  // trigger while streaming would run the load effect's cleanup (detaching
  // the mux) and then bail on the streaming guard — losing the reload.
  const pendingMuxResyncRef = useRef(false);

  const releaseStreamOwnership = () => releaseOwnership(runtime);

  const { handleThumbUp, handleThumbDown, getFeedbackForMessage, loadFeedback } = useChatFeedback(threadId, messages);

  // Track if history replay found an unresolved interrupt (skip reconnection in that case)
  const historyHasUnresolvedInterruptRef = useRef(false);
  // Store the full interrupt details from history so loadAndMaybeReconnect can decide
  // whether to make it interactive or reconnect to get resolution events
  const unresolvedHistoryInterruptRef = useRef<HistoryInterruptInfo[]>([]);

  // Batch parallel interrupt responses: track all interrupt IDs in current batch
  // and collect individual responses until all are answered, then resume at once.
  const pendingInterruptIdsRef = useRef(new Set<string>());
  // Thread-scoped set of interrupt_ids that already have a rendered card. An
  // unanswered interrupt is re-raised by LangGraph with the SAME interrupt_id on
  // every resume; each re-raise arrives on a later turn's bubble, so the per-map
  // dedup (keyed by interrupt_id) never collides and a duplicate card would be
  // appended. This ref survives resume (NOT cleared with pendingInterruptIdsRef),
  // cleared only on thread switch / fresh history load. Shared by replay + live
  // so a live re-raise after a history load dedupes against replayed cards too.
  const renderedInterruptIdsRef = useRef(new Set<string>());
  const collectedHitlResponsesRef = useRef<Record<string, { decisions: Array<{ type: string; message?: string }> }>>({});

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
  // Terminal-run ids the latest history replay rendered; consumed by the
  // load flow's markRunsRendered call so the report-back catch-up can't
  // re-attach an already-on-screen turn as a duplicate bubble.
  const replayedRunIdsRef = useRef<string[]>([]);
  // Ref-based thread ID for use inside closures (avoids stale React state in callbacks)
  const threadIdRef = useRef(threadId);

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
  const markTranscriptPersisted = useCallback(() => {
    setMessages((prev) =>
      prev.some((m) => !m.isHistory)
        ? prev.map((m) => (m.isHistory ? m : { ...m, isHistory: true }))
        : prev,
    );
    // The recently-sent dedup exists to keep a replay from twinning an
    // optimistic user bubble that is still on screen. The bubbles just became
    // clearable-by-reload, so replay is now their only source — keeping the
    // tracker armed would make the reload's replay SKIP the user message
    // whose optimistic bubble it just cleared (vanished user bubble).
    recentlySentTrackerRef.current.clear();
  }, []);
  const reportBackWatch = useReportBackWatch({
    threadId,
    workspaceId,
    threadIdRef,
    isStreamingRef,
    currentRunIdRef,
    lastRenderedTurnIndexRef,
    historyLoadedKeyRef,
    historyLoadingRef,
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
  const { awaitingReportBack, arm: armReportBackWatch } = reportBackWatch;

  // Batch back-to-back offload events into a single notification
  const offloadBatchRef = useRef<OffloadBatch>({ args: 0, reads: 0, timer: null });
  // Track reconnection state for UI indicator
  const [isReconnecting, setIsReconnecting] = useState(false);

  // Market-watch chip lifecycle: seed on thread load/switch + refetch on turn
  // completion. Live mid-turn overwrites arrive via `market_watch_update` SSE
  // events, which forward `setMarketWatch` (see processEvent below).
  const { marketWatch, setMarketWatch } = useMarketWatch(threadId, isLoading, threadIdRef);

  // Track if this is a new conversation (for todo list card management)
  const isNewConversationRef = useRef(false);

  // Recently sent messages tracker
  const recentlySentTrackerRef = useRef(createRecentlySentTracker());
  // v4 idempotent delivery: one request_key per logical send, reused across
  // retransmits of the same send until response headers prove acceptance.
  const requestKeyRef = useRef(createRequestKeyTracker());

  // Map tool call IDs (from main agent's task tool calls) to agent_ids for routing subagent events
  const toolCallIdToTaskIdMapRef = useRef(new Map<string, string>()); // Map<toolCallId, agentId>

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

  // Track subagent history loaded from replay so it can be shown lazily
  // Keyed by agent_id. Structure: { [agentId]: { taskId, description, type, messages, status, ... } }
  const subagentHistoryRef = useRef<Record<string, SubagentHistoryEntry>>({});

  // Persistent subagent state refs — survives across turns so resumed subagents
  // retain messages from previous runs. Keyed by taskId (e.g., "task:k7Xm2p").
  const subagentStateRefsRef = useRef<Record<string, TaskRefs>>({});

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
  } = {}) => ({
    contentOrderCounterRef,
    currentReasoningIdRef,
    currentToolCallIdRef,
    steeringAtOrderRef,
    updateTodoListCard: updateTodoListCard || undefined,
    isNewConversation: o.isNewConversation ?? false,
    subagentStateRefs: subagentStateRefsRef.current,
    updateSubagentCard: !updateSubagentCard
      ? (() => {})
      : o.isReconnect
        ? (agentId: string, data: Record<string, unknown>) => updateSubagentCard(agentId, { ...data, isReconnect: true })
        : updateSubagentCard,
    ...(o.isReconnect ? { isReconnect: true } : {}),
    ...(o.withUnresolvedInterrupt ? { unresolvedHistoryInterruptRef } : {}),
  });

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
    messages,
    t,
    updateSubagentCard,
    updateTodoListCard,
    onWorkspaceCreated,
    streamingThreadIdRef,
    mainStreamAbortRef,
    isReconnectingOwnerRef,
    wasStoppedRef,
    backgroundReconnectRef,
    setIsReconnecting,
    onFileArtifact,
    onPreviewUrl,
    onOnboardingRelatedToolComplete,
    // setters (stable)
    setMessages,
    setIsLoading,
    setIsLoadingHistory,
    setIsCompacting,
    setMessageError,
    setFallbackSuggestion,
    setThreadModels,
    setLastThreadModel,
    setTokenUsage,
    setReloadTrigger,
    setHasActiveSubagents,
    setPendingInterrupt,
    setReturnedSteering,
    setThreadId,
    setWorkspaceStarting,
    // ref containers (stable identity, ref-current reads)
    threadIdRef,
    isStreamingRef,
    contentOrderCounterRef,
    currentReasoningIdRef,
    currentToolCallIdRef,
    currentMessageRef,
    currentRunIdRef,
    currentPlanModeRef,
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
    offloadBatchRef,
    replayedRunIdsRef,
    subagentStateRefsRef,
    subagentHistoryRef,
    subagentProcessEventRef,
    subagentTokenUsageRef,
    terminalTaskOutcomesRef,
    toolCallIdToTaskIdMapRef,
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
        setThreadId(newThreadId);
      }

      // Clear messages only when switching to a different existing thread
      // Preserve messages when transitioning from '__default__' to actual thread ID
      if (isThreadSwitch) {
        setMessages([]);
        setThreadModels([]);
        setLastThreadModel(null);
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
        setIsCompacting(false);
        // Reset refs
        contentOrderCounterRef.current = 0;
        currentReasoningIdRef.current = null;
        currentToolCallIdRef.current = null;
        steeringAtOrderRef.current = null;
        historyLoadingRef.current = false;
        historyLoadedKeyRef.current = null;
        newMessagesStartIndexRef.current = 0;
        recentlySentTrackerRef.current.clear();
        turnCheckpointsRef.current = null;
        // The rendered-turn watermark belongs to the thread we're leaving.
        lastRenderedTurnIndexRef.current = null;
        // Interrupt cards belong to the thread we're leaving; the next thread's
        // replay repopulates this from its persisted interrupt events.
        renderedInterruptIdsRef.current.clear();
      }
    }
  }, [workspaceId, initialThreadId]);

  /**
   * Loads conversation history for the current workspace and thread
   * Uses the threadId from state (which should be a valid thread ID, not '__default__')
   */
  /** History replay lives in session/history/replayHistory; the hook binds
   * the runtime and the cross-lane callbacks. */
  const loadConversationHistory = (): Promise<boolean> =>
    replayConversationHistory(runtime, {
      applyFallbackSuggestion,
      loadFeedback,
      projectSubagentHistory: (byTaskId) => projectSubagentHistory(runtime, byTaskId),
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
  const adoptDuplicateRun = (err: unknown, assistantMessageId: string): boolean => {
    const e = err as { status?: number; errorInfo?: Record<string, unknown> };
    if (e?.status !== 409 || e?.errorInfo?.code !== 'duplicate_request') return false;
    const runId = e.errorInfo.run_id as string | undefined;
    const dupThreadId = e.errorInfo.thread_id as string | undefined;
    // Run identity not disclosed (the key belongs to another user's run —
    // shouldn't happen for an honest client): fall through to a plain error.
    if (!runId || !dupThreadId) return false;
    requestKeyRef.current.clear(); // consumed by the accepted copy
    threadIdRef.current = dupThreadId;
    currentRunIdRef.current = runId;
    attemptReconnectAfterDisconnect(assistantMessageId);
    return true;
  };

  // Load history when workspace or threadId changes, then check for reconnection
  useEffect(() => {
    // A reconnect stream is live on a DIFFERENT thread than the one we're now
    // loading — e.g. a flash report-back is streaming on the flash thread and the
    // user clicked the dispatch card to jump into the running PTC thread. Without
    // this, the isStreamingRef guard below would skip the PTC load and it would
    // appear blank (the report-back stream "holds" the global streaming flag).
    // Supersede that stream: abort it (it continues server-side and replays on
    // return) so THIS thread can load and reconnect. The aborted stream's finally
    // is a no-op now (its `stillActive` check fails — see reconnectToStream).
    //
    // We do NOT stop the report-back watch here. Superseding the visible flash
    // reader (so the PTC thread can take the slot) must not erase the independent
    // pending report-back: the keyed watch persists, holds its run ids, and
    // renders again when the user returns to the flash thread.
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
    // Also skip if streaming is in progress (prevents race condition when thread ID changes during streaming)
    if (!workspaceId || !threadId || threadId === '__default__' || historyLoadingRef.current || isStreamingRef.current) {
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
      const status: WorkflowStatusResponse = await getWorkflowStatus(threadId).catch((statusErr: unknown) => {
        console.log('[Reconnect] Could not check workflow status:', (statusErr as Error).message);
        return { can_reconnect: false, status: 'error' } as WorkflowStatusResponse;
      });

      if (cancelled) return;

      // Capture share status from workflow status response
      if (status.is_shared !== undefined) {
        setIsShared(status.is_shared);
      }

      const loadOk = await loadConversationHistory();

      if (cancelled) return;

      // Only mark the (workspace, thread, reloadTrigger) tuple as loaded
      // when the load actually succeeded. On transient errors the key
      // stays clear so a manual `setReloadTrigger(n => n + 1)` retry will
      // re-fire the effect; otherwise the user would be stuck on a partial
      // view until they switch threads. Set AFTER the cancellation check
      // so an in-flight load that's been cancelled doesn't lock out the
      // eventual real load that supersedes it.
      if (loadOk) {
        historyLoadedKeyRef.current = loadKey;
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

      // Arm the report-back watch BEFORE the reconnect branch below, so it runs
      // even when this load also reconnects to an active run (a refresh right as
      // the report-back becomes due can report both; reconnecting to
      // status.run_id alone can miss the report-back run). The watch stays
      // dormant while a reconnect stream is live and attach() skips the run
      // already on screen, so this never double-streams.
      if (shouldArmForStatus(status)) {
        if (import.meta.env.DEV) console.log('[ReportBack] Pending report-back detected on load, opening watch');
        // Key the watch to THIS thread (pending_report_back is a flash-thread
        // property) and seed the run /status already named. Poke a catch-up
        // reconcile ONLY when the thread isn't also active — poking would race
        // the reconnect branch's attach of status.run_id (double-attach); the
        // seeded id is picked up once that active stream ends.
        reportBackWatch.arm(threadId, status.report_back_run_id, status.can_reconnect ? null : 'load');
      }

      if (historyHasUnresolvedInterruptRef.current && status.can_reconnect) {
        // Workflow is active → interrupt was answered, reconnect will deliver resolution
        console.log('[Reconnect] Unresolved interrupt from history, reconnecting to get resolution events');
        historyHasUnresolvedInterruptRef.current = false;
        await reconnectToStream({ activeTasks: status.active_tasks || [], runId: status.run_id ?? null, resetCursor: true, snapshotAtMs });
        unresolvedHistoryInterruptRef.current = [];
      } else if (historyHasUnresolvedInterruptRef.current && !status.can_reconnect) {
        // Workflow genuinely paused → make interrupt(s) interactive
        const intInfos = unresolvedHistoryInterruptRef.current;
        if (intInfos.length > 0) {
          const intInfo = intInfos[0]; // Use first for setPendingInterrupt (single-slot state)
          console.log('[Reconnect] Workflow paused, making', intInfos.length, 'interrupt(s) interactive:', intInfos.map((p) => p.type));

          // Populate batching refs so answer/skip handlers can collect and batch-resume
          pendingInterruptIdsRef.current.clear();
          collectedHitlResponsesRef.current = {};
          for (const info of intInfos) {
            if (info.interruptId) {
              pendingInterruptIdsRef.current.add(info.interruptId);
            }
          }

          if (intInfo.type === 'ask_user_question') {
            setPendingInterrupt({
              type: 'ask_user_question',
              interruptId: intInfo.interruptId,
              assistantMessageId: intInfo.assistantMessageId,
              questionId: intInfo.questionId,
            });
          } else if (intInfo.type === 'create_workspace') {
            setPendingInterrupt({
              type: 'create_workspace',
              interruptId: intInfo.interruptId,
              assistantMessageId: intInfo.assistantMessageId,
              proposalId: intInfo.proposalId,
            });
          } else if (intInfo.type === 'start_question') {
            setPendingInterrupt({
              type: 'start_question',
              interruptId: intInfo.interruptId,
              assistantMessageId: intInfo.assistantMessageId,
              proposalId: intInfo.proposalId,
            });
          } else if (intInfo.type === 'ptc_agent') {
            setPendingInterrupt({
              type: 'ptc_agent',
              interruptId: intInfo.interruptId,
              assistantMessageId: intInfo.assistantMessageId,
              proposalId: intInfo.proposalId,
            });
          } else if (intInfo.type === 'delete_workspace' || intInfo.type === 'stop_workspace' || intInfo.type === 'delete_thread') {
            setPendingInterrupt({
              type: intInfo.type,
              interruptId: intInfo.interruptId,
              assistantMessageId: intInfo.assistantMessageId,
              proposalId: intInfo.proposalId,
            });
          } else {
            // plan_approval
            setPendingInterrupt({
              interruptId: intInfo.interruptId,
              assistantMessageId: intInfo.assistantMessageId,
              planApprovalId: intInfo.planApprovalId,
              planMode: true,
            });
          }
        }
        unresolvedHistoryInterruptRef.current = [];
        historyHasUnresolvedInterruptRef.current = false;
      } else if (status.can_reconnect) {
        console.log('[Reconnect] Workflow status:', status.status, 'can_reconnect:', status.can_reconnect, 'active_tasks:', status.active_tasks);
        await reconnectToStream({ activeTasks: status.active_tasks || [], runId: status.run_id ?? null, resetCursor: true, snapshotAtMs });
      } else if ((status.active_tasks || []).some((t) => !isSettledTask(t))) {
        // Main workflow completed but subagent tasks still running.
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
          const historyData = subagentHistoryRef.current?.[agentId];
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
        // Workflow is not active. Inline subagent cards are already born with
        // their real status from the replayed task-artifact stamp
        // (handleHistoryTaskArtifactStatus), so no blanket completion here —
        // that would clobber a legitimately 'cancelled' card.
        // Finalize any incomplete todos as stale (they weren't completed by the agent)
        if (finalizePendingTodos) finalizePendingTodos();
        // Also patch inline todoListProcesses in messages
        setMessages((prev) => finalizeTodoListProcessesInMessages(prev));
        // (Report-back watch is armed earlier, before the reconnect branch, so it
        // covers the active-reconnect case too — see that block above.)
      }
    };

    loadAndMaybeReconnect();

    // Cleanup: Cancel loading if workspace or thread changes or component unmounts.
    // The report-back watch is deliberately NOT torn down here — it is keyed to its
    // flash thread and must survive navigation into the dispatched PTC thread (a
    // dedicated unmount-only effect stops it when the component truly goes away).
    return () => {
      cancelled = true;
      historyLoadingRef.current = false;
      // Thread switch/unmount: tear the mux down without marking anything
      // completed, and drop the processor so no stale closure can fire.
      if (threadId) peekThreadMux(threadId)?.detach();
      subagentProcessEventRef.current = null;
      subagentStateRefsRef.current = {};
    };
    // Note: loadConversationHistory is not in deps because it uses workspaceId and threadId from closure
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [workspaceId, threadId, reloadTrigger]);

  // The report-back watch survives thread navigation (so a flash report-back and a
  // live PTC stream coexist); useReportBackWatch owns its own unmount-time
  // teardown, so the thread-load effect above deliberately doesn't touch it.

  /**
   * Subagent mux settlement (sink, positive per-task closure, drain dedup)
   * lives in session/subagents/muxSink — the domain owns the monotonicity
   * contract; the hook wires in the two cross-lane callbacks.
   */
  const { isSettledTask, attachSubagentMux } = createSubagentMuxController(runtime, {
    muxOpenTaskIds,
    armReportBackWatch,
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
   * Closes, for the main message: the open reasoning block (synthetic
   * `reasoning_signal: 'complete'` via handleReasoningSignal) and the message
   * itself (synthetic `finish_reason: 'stopped'` via the handleTextContent
   * finishReason branch). Also stamps a `stopped` flag for the per-message chip.
   * Then closes every active subagent card's open reasoning + marks its last
   * streaming message complete.
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

    // Close the main message's open reasoning block (no-op if none open).
    handleReasoningSignal({
      assistantMessageId,
      signalContent: 'complete',
      refs,
      setMessages: setMessagesForHandlers,
    });

    // Drive the message to a terminal "stopped" state through the same
    // finishReason branch the server stream uses. This flips isStreaming off
    // for any open tool-call/artifact rendering that keyed off isStreaming.
    handleTextContent({
      assistantMessageId,
      content: '',
      finishReason: 'stopped',
      refs,
      setMessages: setMessagesForHandlers,
    });

    // Stamp the stopped flag so the per-message "⏹ Stopped" chip renders, force
    // isStreaming off (defensive: covers any branch that left it on), and fold
    // any still-in-progress tool rows. Always-live tools (TaskOutput/WebFetch)
    // render their spinner off `isInProgress` regardless of `isStreaming`, so
    // without this they'd spin forever after the stop. Mirrors the steering
    // finalize.
    setMessages((prev) =>
      updateMessage(prev, assistantMessageId, (msg) => {
        const aMsg = msg as AssistantMessage;
        const tp: typeof aMsg.toolCallProcesses = {};
        for (const [id, val] of Object.entries(aMsg.toolCallProcesses || {})) {
          tp[id] = val.isInProgress ? { ...val, isInProgress: false, isComplete: true } : val;
        }
        return { ...aMsg, isStreaming: false, stopped: true, toolCallProcesses: tp };
      }),
    );

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
            next[reasoningId] = {
              ...next[reasoningId],
              isReasoning: false,
              reasoningComplete: true,
              reasoningTitle: null,
              _completedAt: Date.now(),
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
          msgs[i] = { ...msgs[i], isStreaming: false, stopped: true, pendingToolCallChunks: {}, toolCallProcesses: tp };
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
    setIsCompacting(false);
    clearModelStatus();
    // Drop any message queued during compaction: the user just cancelled, so it
    // must NOT auto-send when the isCompacting→false transition fires the flush
    // effect. dropQueuedSend clears the ref synchronously (before the effect runs
    // post-render) and removes its optimistic shimmer bubble.
    dropQueuedSend();
    releaseStreamOwnership();
    currentMessageRef.current = null;

    // An ADMITTED stop (run id latched) is persisted server-side as a
    // user-cancelled "Stopped" turn (_mark_cancelled folds the partial events
    // into sse_events), so its bubbles are replay-reproducible — mark them
    // isHistory and release the recently-sent dedup, same as a success
    // finalize. Otherwise a later corrective reload appends a replayed twin of
    // the answer under a dedup-eaten user message, ordered after newer turns.
    // A PRE-ADMISSION stop has no turn row: replay can't reproduce those
    // bubbles, so they must stay unmarked to survive reloads.
    if (stoppedRunId) {
      markTranscriptPersisted();
    }

    // (c) The thread mux stays attached: the backend cancel finalizes this
    // turn's task runs, and their terminal frames flip the cards to their
    // real outcome (cancelled) instead of a client-side guess. Leave the
    // report-back watch running: this flash-thread cancel does not stop the
    // background PTC analyses on their own threads, so their summaries should
    // still surface live. The aborted reader's finally clears isStreamingRef, so
    // the watch's next reconcile can attach the next head run or drain.

    // (d) Tell the backend to hard-cancel: one retry, then a visible error
    // toast so a failed cancel doesn't silently diverge UI from backend.
    if (tid && tid !== '__default__') {
      try {
        await cancelWorkflow(tid, stoppedRunId);
      } catch (firstErr) {
        // Log the first failure — when both attempts fail the toast only
        // reflects the second, so a degraded-network first error would
        // otherwise vanish from diagnostics.
        console.warn('[stopWorkflow] cancel failed, retrying once:', firstErr);
        try {
          await cancelWorkflow(tid, stoppedRunId);
        } catch {
          toast({ description: t('chat.stopFailed'), variant: 'destructive' });
        }
      }
    }
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
   * Handles sending a message while the agent is already streaming (steering).
   * The backend will accept it for injection before the next LLM call.
   *
   * Demotion path: the frontend's ``isLoading`` lags the backend's task status by
   * the SSE-drain window. If the user sends after the workflow has flipped to a
   * terminal status but before the terminal SSE event reaches us, ``wait_or_steer``
   * routes this POST as a new turn instead of steering. The backend emits an
   * authoritative ``metadata`` event (with a fresh ``run_id``) as the first event
   * of the new turn's stream — receipt of any ``metadata`` event on the steering
   * POST means we got routed as a new turn. We demote the user bubble (strip the
   * badge) and switch subsequent events to the standard stream processor so the
   * new turn renders normally.
   */
  const handleSendSteering = async (message: string, planMode: boolean = false, additionalContext: Record<string, unknown>[] | null = null, attachmentMeta: Record<string, unknown>[] | null = null, { widgetSnapshots, chartSelections }: ModelOptions = {}) => {
    // Show user message in chat with steering indicator. Preserve any inline
    // context cards (widget snapshots / chart selections) so a message queued
    // during compaction keeps them when the flush routes through steering.
    const userMsg = createUserMessage(message, attachmentMeta as AttachmentMeta[] | null, widgetSnapshots ?? null, chartSelections ?? null);
    const userMessage: MessageRecord = { ...userMsg, steering: true };
    recentlySentTrackerRef.current.track(message.trim(), userMessage.timestamp, userMessage.id);
    setMessages((prev) => appendMessage(prev,userMessage));

    let demotedToNewTurn = false;
    let demotedProcessor: ((event: SSEEvent) => void) | null = null;
    let demotedAssistantId: string | null = null;
    const demotedInterruptedRef = { current: false };
    // Controller for the steering POST. If this POST is demoted to a fresh
    // turn it becomes the active main stream, so we register it on
    // mainStreamAbortRef in demoteToNewTurn — stopWorkflow can then abort it.
    const steeringAbort = new AbortController();
    // Stash the Content-Location run_id but DO NOT commit it to
    // currentRunIdRef until we've seen evidence that this POST actually
    // started a new workflow (i.e., demoteToNewTurn fires). Committing
    // eagerly on Content-Location alone would overwrite the active
    // workflow's run_id when the backend routes this as steering-only.
    let pendingRunIdFromHeader: string | null = null;

    const demoteToNewTurn = (): void => {
      // If the user already hit stop, do NOT promote this in-flight steering
      // POST into a fresh turn: clearing wasStoppedRef + re-enabling the
      // spinner here would make the stop look undone (the backend tore the
      // prior turn down and routed this POST as new). Drop it instead — the
      // finally below honors wasStoppedRef and returns.
      if (wasStoppedRef.current) return;
      demotedToNewTurn = true;
      if (pendingRunIdFromHeader) {
        currentRunIdRef.current = pendingRunIdFromHeader;
      }
      setMessages((prev) =>
        updateMessage(prev, userMessage.id as string, (msg) => {
          if (msg.role !== 'user') return msg;
          const next: UserMessage & { queuePosition?: unknown; queueError?: unknown } = { ...msg };
          delete next.steering;
          delete next.queuePosition;
          delete next.queueError;
          return next;
        })
      );
      const newAssistantId = `assistant-${Date.now()}`;
      demotedAssistantId = newAssistantId;
      contentOrderCounterRef.current = 0;
      currentReasoningIdRef.current = null;
      currentToolCallIdRef.current = null;
      const assistantMessage = createAssistantMessage(newAssistantId);
      setMessages((prev) => appendMessage(prev, assistantMessage));
      currentMessageRef.current = newAssistantId;
      acquireStreamOwnership(threadId);
      setIsLoading(true);
      // This demoted POST is now the active main turn; clear the stopped guard
      // and register its controller so stopWorkflow can abort it.
      wasStoppedRef.current = false;
      backgroundReconnectRef.current = false;
      mainStreamAbortRef.current = steeringAbort;
      const refs = buildStreamRefs();
      demotedProcessor = createStreamEventProcessor(runtime, streamRouterDeps, newAssistantId, refs, getTaskIdFromEvent, demotedInterruptedRef);
    };

    // Same fingerprint form as handleSendMessage: if this POST is demoted to
    // a new turn and its response is lost, the user's re-send (which will
    // route through handleSendMessage once loading clears) reuses the key and
    // dedups against the accepted run.
    const requestKey = requestKeyRef.current.take(`send|${threadId}|${message}`);
    try {
      // Send to same endpoint — backend will auto-accept steering and return steering_accepted SSE
      const result = await sendChatMessageStream(
        message,
        workspaceId,
        threadId,
        [],
        planMode,
        (event) => {
          const eventType = event.event || 'message_chunk';
          if (eventType === 'steering_accepted') {
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
          // First non-steering event = backend routed as a new turn (race: status
          // flipped to terminal before our POST landed). The first frame in that
          // case is the authoritative ``metadata`` event (carrying a fresh
          // run_id) per the backend SSE protocol; we also accept any other
          // non-steering event here as defense-in-depth (e.g. an early error
          // before workflow start). The demoted processor handles ``metadata``
          // itself — it stores ``run_id`` into ``currentRunIdRef``.
          if (!demotedToNewTurn) {
            demoteToNewTurn();
          }
          if (demotedProcessor) {
            demotedProcessor(event);
          }
        },
        additionalContext,
        agentMode,
        userLocale,
        userTimezone,
        undefined,
        undefined,
        null,
        null,
        null,
        platform,
        // Stash the Content-Location run_id but defer the commit until
        // demoteToNewTurn fires. Backend emits Content-Location on every
        // POST including pure-steering responses; committing eagerly would
        // overwrite the active workflow's run_id with a stream key that
        // never gets written to.
        (runId) => {
          requestKeyRef.current.clear();
          pendingRunIdFromHeader = runId;
        },
        steeringAbort.signal,
        requestKey,
      );
      if (mainStreamAbortRef.current === steeringAbort) {
        mainStreamAbortRef.current = null;
      }
      // A background abort (foreground handler on tab resume) or a transport
      // drop returns a result flag instead of throwing. This is the one steering
      // sub-case the foreground handler can hit: once we demote to a real new
      // turn, steeringAbort owns mainStreamAbortRef, so an abort here lands on a
      // live backend turn. Re-kick the existing reconnect instead of finalizing
      // it as truncated-complete. A user stop is owned by stopWorkflow.
      if (result?.aborted || wasStoppedRef.current) {
        const reconnectId = currentMessageRef.current || demotedAssistantId;
        if (
          demotedToNewTurn &&
          backgroundReconnectRef.current &&
          !wasStoppedRef.current &&
          reconnectId
        ) {
          backgroundReconnectRef.current = false;
          attemptReconnectAfterDisconnect(reconnectId);
        }
        return;
      }
      // Natural transport drop on the demoted turn: reconnect rather than
      // finalizing — the turn may still be running on the backend.
      if (result?.disconnected && demotedToNewTurn) {
        const reconnectId = currentMessageRef.current || demotedAssistantId;
        if (reconnectId) {
          attemptReconnectAfterDisconnect(reconnectId);
        }
        return;
      }
      if (demotedToNewTurn) {
        const finalId = currentMessageRef.current || demotedAssistantId;
        if (finalId) {
          setMessages((prev) =>
            updateMessage(prev, finalId, (msg) => ({
              ...msg,
              isStreaming: false,
            }))
          );
          if (!demotedInterruptedRef.current) {
            cleanupAfterStreamEnd(finalId);
          }
        }
      }
    } catch (err: unknown) {
      if (mainStreamAbortRef.current === steeringAbort) {
        mainStreamAbortRef.current = null;
      }
      if ((err as Error)?.name === 'AbortError' || wasStoppedRef.current) {
        return;
      }
      console.error('Error sending steering:', err);
      if (demotedToNewTurn && demotedAssistantId) {
        // Demoted path: the failure belongs to the new turn's assistant, not the steering badge.
        const finalAssistantId = demotedAssistantId;
        setMessages((prev) =>
          updateMessage(prev, finalAssistantId, (msg) => ({
            ...msg,
            content: msg.content || 'Failed to send message. Please try again.',
            isStreaming: false,
            error: true,
          }))
        );
        setMessageError((err as Error).message || 'Failed to send message');
        releaseStreamOwnership();
        setIsLoading(false);
        return;
      }
      // Update user message to show steering failure
      setMessages((prev) =>
        updateMessage(prev,userMessage.id as string, (msg) => ({
          ...msg,
          steering: false,
          queueError: (err as Error).message || 'Failed to send steering',
        }))
      );
    }
  };

  const handleSendMessage = async (message: string, planMode: boolean = false, additionalContext: Record<string, unknown>[] | null = null, attachmentMeta: Record<string, unknown>[] | null = null, { model, reasoningEffort, fastMode, widgetSnapshots, chartSelections }: ModelOptions = {}) => {
    const hasContent = message.trim() || (additionalContext && additionalContext.length > 0);
    if (!workspaceId || !hasContent) {
      return;
    }

    // Chat activity bumps the thread to the top of the nav panel's list
    // (clicking around never reorders; new threads surface via the new-id rule).
    bumpThreadNavOrder(workspaceId, threadIdRef.current);

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
      const queuedMsg = createUserMessage(
        message,
        attachmentMeta as AttachmentMeta[] | null,
        widgetSnapshots ?? null,
        chartSelections ?? null,
      );
      const queuedMessage: MessageRecord = { ...queuedMsg, queued: true };
      queuedSendRef.current = {
        message,
        planMode,
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

    // If agent is already streaming, send as steering message
    if (isLoading) {
      return handleSendSteering(message, planMode, additionalContext, attachmentMeta, { widgetSnapshots, chartSelections });
    }

    // Store planMode so HITL interrupt handler can access it
    currentPlanModeRef.current = planMode;

    // Store model options so HITL resume can forward them
    lastModelOptionsRef.current = { model: model || null, reasoningEffort: reasoningEffort || null, fastMode: fastMode || null };

    // Intercept: if a plan was rejected, route this message as rejection feedback
    if (pendingRejection) {
      const { interruptId, planMode: rejectionPlanMode } = pendingRejection;
      setPendingRejection(null);

      // Show user message in chat
      const userMsg = createUserMessage(message);
      recentlySentTrackerRef.current.track(message.trim(), userMsg.timestamp, userMsg.id);
      setMessages((prev) => appendMessage(prev,userMsg));

      // Send as rejection feedback via hitl_response
      const hitlResponse = {
        [interruptId]: {
          decisions: [{ type: 'reject', message: message.trim() }],
        },
      };
      return resumeWithHitlResponse(hitlResponse, rejectionPlanMode);
    }

    // Create and add user message
    const userMessage = createUserMessage(
      message,
      attachmentMeta as AttachmentMeta[] | null,
      widgetSnapshots ?? null,
      chartSelections ?? null,
    );
    recentlySentTrackerRef.current.track(message.trim(), userMessage.timestamp, userMessage.id);

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
      setLastThreadModel(model);
    }

    // Add user message after history messages
    setMessages((prev) => {
      const newMessages = appendMessage(prev,userMessage);
      // Update new messages start index if this is the first new message
      if (newMessagesStartIndexRef.current === prev.length) {
        newMessagesStartIndexRef.current = newMessages.length;
      }
      return newMessages;
    });

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
    backgroundReconnectRef.current = false;
    // This send opens a NEW backend turn rendered in-view; advance the
    // watermark so the next reactivation's staleness check doesn't mistake
    // this turn for one missed while hidden (spurious full reload).
    lastRenderedTurnIndexRef.current = (lastRenderedTurnIndexRef.current ?? -1) + 1;
    // Mark streaming as in progress (prevents history loading during streaming)
    // AND claim ownership for this thread, so navigating to another thread mid-send
    // supersedes this stream rather than leaving it orphaned (the load guard would
    // otherwise block the new thread because isStreamingRef is still set).
    acquireStreamOwnership(threadId);

    // Create assistant message placeholder
    const assistantMessageId = `assistant-${Date.now()}`;
    // Reset counters for this new message
    contentOrderCounterRef.current = 0;
    currentReasoningIdRef.current = null;
    currentToolCallIdRef.current = null;
    // Clear the active run_id; the new turn's metadata frame will repopulate
    // it. Prevents a stale run_id from biasing a reconnect into an older
    // ``workflow:stream:{tid}:{rid}`` key.
    currentRunIdRef.current = null;
    // Fresh AbortController so stopWorkflow can abort this stream's reader.
    const abortController = new AbortController();
    mainStreamAbortRef.current = abortController;

    const assistantMessage = createAssistantMessage(assistantMessageId);

    // Add assistant message after history messages
    setMessages((prev) => {
      const newMessages = appendMessage(prev,assistantMessage);
      // Update new messages start index
      newMessagesStartIndexRef.current = newMessages.length;
      return newMessages;
    });
    currentMessageRef.current = assistantMessageId;

    // One request_key per logical send, reused if this exact send is
    // retransmitted after a lost response (fingerprint match) — see
    // createRequestKeyTracker.
    const requestKey = requestKeyRef.current.take(`send|${threadId}|${message}`);
    let wasDisconnected = false;
    const wasInterruptedRef = { current: false };
    try {
      // Prepare refs for event handlers — use persistent subagent state
      const refs = buildStreamRefs({ isNewConversation: isNewConversationRef.current });

      // Create the event processor using the shared factory
      const processEvent = createStreamEventProcessor(runtime, streamRouterDeps, assistantMessageId, refs, getTaskIdFromEvent, wasInterruptedRef);

      const result = await sendChatMessageStream(
        message,
        workspaceId,
        threadId,
        [],
        planMode,
        processEvent,
        additionalContext,
        agentMode,
        userLocale, userTimezone, undefined, undefined,
        model || null,
        reasoningEffort || null,
        fastMode || null,
        platform,
        // Latch run_id AND the server-assigned thread_id from Content-Location
        // BEFORE the first SSE body byte. run_id closes the reconnect race
        // window; the thread_id latch lets an early stop on a brand-new thread
        // ('__default__' until the first event) still hard-cancel the backend
        // run instead of skipping cancel. The first event still drives the
        // route/storage update (see the thread_id branch in processEvent).
        (runId, resolvedThreadId) => {
          requestKeyRef.current.clear();
          currentRunIdRef.current = runId;
          if (resolvedThreadId && resolvedThreadId !== '__default__') {
            threadIdRef.current = resolvedThreadId;
          }
        },
        abortController.signal,
        requestKey,
      );

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

      if (result?.disconnected) {
        console.log('[Send] Stream disconnected, attempting reconnect');
        wasDisconnected = true;
        attemptReconnectAfterDisconnect(assistantMessageId);
        return;
      }

      // Mark message as complete (use live ref in case steering_delivered switched it)
      {
        const finalId = currentMessageRef.current || assistantMessageId;
        setMessages((prev) =>
          updateMessage(prev,finalId, (msg) => ({
            ...msg,
            isStreaming: false,
          }))
        );
        markTranscriptPersisted();
      }
    } catch (err: unknown) {
          // An aborted stream (user hit stop) is intentional, not a failure.
          // streamFetch normally swallows AbortError and returns { aborted },
          // but guard here too so a stop never surfaces an error banner.
          if ((err as Error)?.name === 'AbortError' || wasStoppedRef.current) {
            return;
          }
          // 409 duplicate_request: an earlier copy of this send was already
          // accepted (its response was lost) — adopt that run instead of
          // erroring; the reconnect owns finalization from here.
          if (adoptDuplicateRun(err, assistantMessageId)) {
            wasDisconnected = true;
            return;
          }
          // Handle rate limit (429) — show limit message and remove optimistic assistant message
          const errObj = err as Record<string, unknown>;
          if (errObj.status === 429) {
            const info = (errObj.rateLimitInfo || {}) as Record<string, unknown>;
            const platformUrl = (import.meta.env.VITE_PLATFORM_URL as string | undefined) || '/account';
            const structured = buildRateLimitError(info, platformUrl);
            setMessageError(structured);
            setMessages((prev) => prev.filter((m) => m.id !== assistantMessageId));
          } else {
            console.error('Error sending message:', err);
            // Build structured error with link when backend provides one
            // (byok_key_required, oauth_required, 403, ...). When a banner
            // with a CTA renders, drop the optimistic assistant bubble so
            // the transcript stays clean — matches the 429 pattern and the
            // `internal` SSE error pattern. Leaving a content-less "Failed
            // to send message" bubble under the banner looks broken.
            const errorInfo = errObj.errorInfo as Record<string, unknown> | undefined;
            if (errorInfo?.link) {
              // Backend scrubbed stale model names from the user's saved
              // preferences — invalidate our cached copy so the selector and
              // Settings page re-render with the server's (cleaned) state.
              // Without this, the frontend keeps sending the removed model
              // name as request_model on every retry.
              if (errorInfo.type === 'model_removed') {
                queryClient.invalidateQueries({ queryKey: queryKeys.user.preferences() });
              }
              setMessageError({
                message: (errorInfo.message as string) || (err as Error).message || 'An error occurred.',
                link: errorInfo.link as { url: string; label: string },
              });
              setMessages((prev) => prev.filter((m) => m.id !== assistantMessageId));
            } else if (errObj.status === 403) {
              setMessageError({
                message: (err as Error).message || 'Access denied.',
                link: { url: '/setup/method', label: 'Configure providers' },
              });
              setMessages((prev) => prev.filter((m) => m.id !== assistantMessageId));
            } else {
              setMessageError((err as Error).message || 'Failed to send message');
              setMessages((prev) =>
                updateMessage(prev, assistantMessageId, (msg) => ({
                  ...msg,
                  content: msg.content || 'Failed to send message. Please try again.',
                  isStreaming: false,
                  error: true,
                }))
              );
            }
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
              updateMessage(prev,finalId, (msg) => ({
                ...msg,
                isStreaming: false,
              }))
            );

            cleanupAfterStreamEnd(finalId);
          }
        }
      };

  // Flush a message queued during compaction once it finishes. If a turn is
  // still running (auto Tier-2 summarize), steer into it; otherwise start a
  // fresh turn — the exact branch handleSendMessage would have taken had the
  // message arrived now. queuedSendRef is cleared in stopWorkflow, so a queued
  // message is never replayed into a turn the user just cancelled.
  useEffect(() => {
    if (isCompacting) return;
    const queued = queuedSendRef.current;
    if (!queued) return;
    queuedSendRef.current = null;
    setQueuedSend(false);
    const { message, planMode, additionalContext, attachmentMeta, modelOptions, messageId } = queued;
    // Drop the optimistic shimmer bubble; the send path re-adds the real one
    // (steering shimmer if a turn is still running, else a normal user bubble).
    if (messageId) {
      setMessages((prev) => prev.filter((m) => m.id !== messageId));
    }
    if (isLoading) {
      handleSendSteering(message, planMode, additionalContext, attachmentMeta, modelOptions);
    } else {
      handleSendMessage(message, planMode, additionalContext, attachmentMeta, modelOptions);
    }
    // Fires on isCompacting transitions. isLoading and the send handlers are
    // captured from the render where isCompacting went false — that render is
    // the correct moment to decide steer-vs-fresh. Handlers are omitted from
    // deps because the values they actually close over (workspaceId/threadId)
    // don't change mid-compaction, so a stale closure here isn't possible.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isCompacting]);

  /**
   * Resumes an interrupted workflow with an HITL response (approve or reject).
   * Follows the same pattern as handleSendMessage but sends messages: [] with hitl_response.
   */
  const resumeWithHitlResponse = useCallback(async (hitlResponse: Record<string, { decisions: Array<{ type: string; message?: string }> }>, planMode: boolean = false) => {
    setPendingInterrupt(null);
    pendingInterruptIdsRef.current.clear();
    collectedHitlResponsesRef.current = {};

    // Create assistant message placeholder
    const assistantMessageId = `assistant-hitl-${Date.now()}`;
    contentOrderCounterRef.current = 0;
    currentReasoningIdRef.current = null;
    currentToolCallIdRef.current = null;
    // HITL resume always opens a fresh run on the backend (1:1 with
    // ``conversation_response_id``); clear the stale ref so the new turn's
    // metadata frame is the source of truth.
    currentRunIdRef.current = null;

    const assistantMessage = createAssistantMessage(assistantMessageId);
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
    const processEvent = createStreamEventProcessor(runtime, streamRouterDeps, assistantMessageId, refs, getTaskIdFromEvent, wasInterruptedRef);

    // One request_key per resume (keyed by the interrupt answers), reused on
    // a retransmit after a lost response — see createRequestKeyTracker.
    const requestKey = requestKeyRef.current.take(
      `hitl|${threadId}|${JSON.stringify(hitlResponse)}`,
    );
    let wasDisconnected = false;
    try {
      const result = await sendHitlResponse(
        workspaceId,
        threadId,
        hitlResponse,
        processEvent,
        planMode,
        lastModelOptionsRef.current as { model?: string; reasoningEffort?: string; fastMode?: boolean },
        agentMode,
        // Latch the fresh run_id from response headers before the first SSE
        // body byte. Without this, an early disconnect (between the pre-POST
        // clear above and the metadata frame) would let
        // attemptReconnectAfterDisconnect fall back to the prior run's
        // TaskInfo and silently hang.
        (runId) => {
          requestKeyRef.current.clear();
          currentRunIdRef.current = runId;
        },
        abortController.signal,
        requestKey,
      );

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

      if (result?.disconnected) {
        console.log('[HITL] Stream disconnected, attempting reconnect');
        wasDisconnected = true;
        attemptReconnectAfterDisconnect(assistantMessageId);
        return;
      }

      // Mark message as complete (use live ref in case steering_delivered switched it)
      {
        const finalId = currentMessageRef.current || assistantMessageId;
        setMessages((prev) =>
          updateMessage(prev,finalId, (msg) => ({
            ...msg,
            isStreaming: false,
          }))
        );
        markTranscriptPersisted();
      }
    } catch (err: unknown) {
      if ((err as Error)?.name === 'AbortError' || wasStoppedRef.current) {
        return;
      }
      // 409 duplicate_request: an earlier copy of this resume was already
      // accepted (its response was lost) — adopt that run instead of erroring.
      if (adoptDuplicateRun(err, assistantMessageId)) {
        wasDisconnected = true;
        return;
      }
      console.error('[HITL] Error resuming workflow:', err);
      setMessageError((err as Error).message || 'Failed to resume workflow');
      setMessages((prev) =>
        updateMessage(prev,assistantMessageId, (msg) => ({
          ...msg,
          content: msg.content || 'Failed to resume workflow. Please try again.',
          isStreaming: false,
          error: true,
        }))
      );
    } finally {
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
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [workspaceId, threadId, updateTodoListCard, updateSubagentCard, finalizePendingTodos]);

  const handleApproveInterrupt = useCallback(() => {
    if (!pendingInterrupt) return;
    const { interruptId, planApprovalId, planMode } = pendingInterrupt;
    const approvalId = planApprovalId!;

    // Flip the plan card to "approved" wherever it lives (mirrors
    // resolveProposal): a deduped re-raise can point pendingInterrupt at a
    // hidden resume bubble, so a single-bubble write could miss the visible card.
    setMessages((prev) =>
      prev.map((m) => {
        if (m.role !== 'assistant') return m;
        const msg = m as AssistantMessage;
        if (!msg.planApprovals?.[approvalId]) return m;
        return {
          ...msg,
          planApprovals: {
            ...msg.planApprovals,
            [approvalId]: { ...msg.planApprovals[approvalId], status: 'approved' },
          },
        };
      })
    );

    const hitlResponse = {
      [interruptId!]: { decisions: [{ type: 'approve' }] },
    };
    resumeWithHitlResponse(hitlResponse, planMode);
  }, [pendingInterrupt, resumeWithHitlResponse]);

  const handleRejectInterrupt = useCallback(() => {
    if (!pendingInterrupt) return;
    const { interruptId, planApprovalId, planMode } = pendingInterrupt;
    const approvalId = planApprovalId!;

    // Flip the plan card to "rejected" wherever it lives (see approve above).
    setMessages((prev) =>
      prev.map((m) => {
        if (m.role !== 'assistant') return m;
        const msg = m as AssistantMessage;
        if (!msg.planApprovals?.[approvalId]) return m;
        return {
          ...msg,
          planApprovals: {
            ...msg.planApprovals,
            [approvalId]: { ...msg.planApprovals[approvalId], status: 'rejected' },
          },
        };
      })
    );

    // Store interruptId + planMode so next handleSendMessage routes as rejection feedback
    setPendingRejection({ interruptId: interruptId!, planMode: planMode! });
    setPendingInterrupt(null);
  }, [pendingInterrupt]);

  // Shared HITL collect-then-batch-resume. Parallel interrupts must be answered
  // together (one batched resume), so each handler records its own interrupt_id's
  // decision here, and we resume only once EVERY pending interrupt has a collected
  // response. Reading pendingInterrupt (a single slot N dispatches overwrite)
  // instead would answer the wrong interrupt and leave the others to re-interrupt.
  // planMode defaults to false; question handlers pass currentPlanModeRef.current.
  const collectHitlResponseAndMaybeResume = useCallback((
    interruptId: string,
    response: { decisions: Array<{ type: string; message?: string }> },
    planMode: boolean = false,
  ) => {
    collectedHitlResponsesRef.current[interruptId] = response;
    const pending = pendingInterruptIdsRef.current;
    const collected = collectedHitlResponsesRef.current;
    if (pending.size > 0 && [...pending].every((id) => collected[id])) {
      resumeWithHitlResponse({ ...collected }, planMode);
    }
  }, [resumeWithHitlResponse]);

  const handleAnswerQuestion = useCallback((answer: string, questionId: string, interruptId: string) => {
    if (!questionId || !interruptId) return;

    // Optimistically mark the card as answered
    setMessages((prev) =>
      prev.map((m) => {
        if (m.role !== 'assistant') return m;
        const msg = m as AssistantMessage;
        if (!msg.userQuestions?.[questionId]) return m;
        return {
          ...msg,
          userQuestions: {
            ...msg.userQuestions,
            [questionId]: {
              ...msg.userQuestions[questionId],
              status: 'answered',
              answer,
            },
          },
        };
      })
    );

    // Collect this response for batching (parallel interrupts need all responses at once)
    collectHitlResponseAndMaybeResume(
      interruptId,
      { decisions: [{ type: 'approve', message: answer }] },
      currentPlanModeRef.current,
    );
  }, [collectHitlResponseAndMaybeResume]);

  const handleSkipQuestion = useCallback((questionId: string, interruptId: string) => {
    if (!questionId || !interruptId) return;

    // Mark the card as skipped
    setMessages((prev) =>
      prev.map((m) => {
        if (m.role !== 'assistant') return m;
        const msg = m as AssistantMessage;
        if (!msg.userQuestions?.[questionId]) return m;
        return {
          ...msg,
          userQuestions: {
            ...msg.userQuestions,
            [questionId]: {
              ...msg.userQuestions[questionId],
              status: 'skipped',
            },
          },
        };
      })
    );

    // Collect this response for batching (parallel interrupts need all responses at once)
    collectHitlResponseAndMaybeResume(
      interruptId,
      { decisions: [{ type: 'reject' }] },
      currentPlanModeRef.current,
    );
  }, [collectHitlResponseAndMaybeResume]);

  // Shared helper: update a proposal's status within an AssistantMessage.
  // Used by all HITL approve/reject handlers below.
  const resolveProposal = useCallback((proposalKey: string, pid: string, status: string) => {
    setMessages((prev) =>
      prev.map((m) => {
        if (m.role !== 'assistant') return m;
        const msg = m as AssistantMessage;
        const proposals = (msg as unknown as Record<string, Record<string, Record<string, unknown>>>)[proposalKey];
        if (!proposals?.[pid]) return m;
        return { ...msg, [proposalKey]: { ...proposals, [pid]: { ...proposals[pid], status } } };
      })
    );
  }, []);

  const handleApproveCreateWorkspace = useCallback(() => {
    if (!pendingInterrupt || pendingInterrupt.type !== 'create_workspace') return;
    resolveProposal('workspaceProposals', pendingInterrupt.proposalId!, 'approved');
    resumeWithHitlResponse({ [pendingInterrupt.interruptId!]: { decisions: [{ type: 'approve' }] } }, false);
  }, [pendingInterrupt, resumeWithHitlResponse, resolveProposal]);

  const handleRejectCreateWorkspace = useCallback(() => {
    if (!pendingInterrupt || pendingInterrupt.type !== 'create_workspace') return;
    resolveProposal('workspaceProposals', pendingInterrupt.proposalId!, 'rejected');
    resumeWithHitlResponse({ [pendingInterrupt.interruptId!]: { decisions: [{ type: 'reject' }] } }, false);
  }, [pendingInterrupt, resumeWithHitlResponse, resolveProposal]);

  const handleApproveStartQuestion = useCallback(() => {
    if (!pendingInterrupt || pendingInterrupt.type !== 'start_question') return;
    resolveProposal('questionProposals', pendingInterrupt.proposalId!, 'approved');
    resumeWithHitlResponse({ [pendingInterrupt.interruptId!]: { decisions: [{ type: 'approve' }] } }, false);
  }, [pendingInterrupt, resumeWithHitlResponse, resolveProposal]);

  const handleRejectStartQuestion = useCallback(() => {
    if (!pendingInterrupt || pendingInterrupt.type !== 'start_question') return;
    resolveProposal('questionProposals', pendingInterrupt.proposalId!, 'rejected');
    resumeWithHitlResponse({ [pendingInterrupt.interruptId!]: { decisions: [{ type: 'reject' }] } }, false);
  }, [pendingInterrupt, resumeWithHitlResponse, resolveProposal]);

  // --- PTC Agent approve/reject ---
  // The clicked card supplies its OWN proposalId + interruptId, collected then
  // batch-resumed — `pendingInterrupt` is single-slot state that N parallel
  // dispatches overwrite, so reading it would answer the wrong interrupt.
  const handleApprovePTCAgent = useCallback((
    pad?: Record<string, unknown>,
    overrides?: { report_back?: boolean },
    proposalId?: string,
    interruptId?: string,
  ) => {
    if (!proposalId || !interruptId) return;

    // Track this proposal for thread_id backfill from the resumed stream's
    // tool_call_result. tool_call_id comes from the clicked card's own proposal
    // data, NOT pendingInterrupt, so it's right under N parallel dispatches.
    const toolCallId = pad?.tool_call_id as string | undefined;
    if (toolCallId) {
      pendingPTCBackfillRef.current.set(toolCallId, proposalId);
    }

    // Arm the report-back watch AT DISPATCH, not at the dispatch turn's stream
    // end: the wake is pub/sub with no replay, so a fast PTC finishing mid-turn
    // would hit zero subscribers and lose the report-back. Subscribing now
    // latches such a wake (enqueued before the reconcile's isStreamingRef bail)
    // to attach at stream end. No named run exists yet (approval is what
    // dispatches), so no seed and no poke.
    if (overrides?.report_back !== false) {
      armReportBackWatch(threadIdRef.current, null, null);
    }

    resolveProposal('ptcAgentProposals', proposalId, 'approved');

    const decision: { type: string; message?: string; overrides?: { report_back?: boolean } } = { type: 'approve' };
    if (overrides) {
      decision.overrides = overrides;
    }

    // Collect-then-batch: hold each card's decision keyed by its interrupt_id and
    // resume only when ALL pending interrupts have a decision.
    collectHitlResponseAndMaybeResume(interruptId, { decisions: [decision] });
  }, [collectHitlResponseAndMaybeResume, resolveProposal, armReportBackWatch]);

  const handleRejectPTCAgent = useCallback((
    _pad?: Record<string, unknown>,
    proposalId?: string,
    interruptId?: string,
  ) => {
    if (!proposalId || !interruptId) return;
    resolveProposal('ptcAgentProposals', proposalId, 'rejected');
    collectHitlResponseAndMaybeResume(interruptId, { decisions: [{ type: 'reject' }] });
  }, [collectHitlResponseAndMaybeResume, resolveProposal]);

  // --- Secretary action approve/reject (delete_workspace, stop_workspace, delete_thread) ---
  const handleApproveSecretaryAction = useCallback(() => {
    if (!pendingInterrupt || !SECRETARY_ACTION_TYPES.has(pendingInterrupt.type!)) return;
    resolveProposal('secretaryActionProposals', pendingInterrupt.proposalId!, 'approved');
    resumeWithHitlResponse({ [pendingInterrupt.interruptId!]: { decisions: [{ type: 'approve' }] } }, false);
  }, [pendingInterrupt, resumeWithHitlResponse, resolveProposal]);

  const handleRejectSecretaryAction = useCallback(() => {
    if (!pendingInterrupt || !SECRETARY_ACTION_TYPES.has(pendingInterrupt.type!)) return;
    resolveProposal('secretaryActionProposals', pendingInterrupt.proposalId!, 'rejected');
    resumeWithHitlResponse({ [pendingInterrupt.interruptId!]: { decisions: [{ type: 'reject' }] } }, false);
  }, [pendingInterrupt, resumeWithHitlResponse, resolveProposal]);

  const insertNotification = useCallback(
    (text: string, variant: 'info' | 'success' | 'warning' = 'info', detail?: string) => {
      setMessages((prev) => appendMessage(prev, createNotificationMessage(text, variant, detail)));
    },
    [],
  );

  // =====================================================================
  // Edit / Regenerate / Retry handlers
  // =====================================================================

  /** Lazy-cached turn checkpoint data. Invalidated after each edit/regenerate. */
  const turnCheckpointsRef = useRef<{ turns: Array<{ edit_checkpoint_id: string | null; regenerate_checkpoint_id: string; turn_index: number }> } | null>(null);

  /**
   * Helper: get or fetch turn checkpoints for the current thread.
   * Caches the result in turnCheckpointsRef until invalidated.
   */
  const getTurnCheckpoints = useCallback(async () => {
    if (turnCheckpointsRef.current) return turnCheckpointsRef.current;
    const currentThreadId = threadIdRef.current;
    if (!currentThreadId || currentThreadId === '__default__') return null;
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
   * Helper: stream a forked or retried turn (shared by edit, regenerate, retry).
   * Edit/regenerate fork from an explicit `checkpointId`; retry goes through the
   * POST /retry attempt chain with `checkpointId=null` (the server resolves the
   * retry checkpoint). Sets up the assistant placeholder, event processor, and
   * stream lifecycle.
   */
  const streamFromCheckpoint = useCallback(async (message: string | null, checkpointId: string | null, truncateIndex: number, forkFromTurn: number | null = null, modelOptions: ModelOptions = {}, viaRetryEndpoint: boolean = false) => {
    if (isStreamingRef.current) return;

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
    backgroundReconnectRef.current = false;
    acquireStreamOwnership(threadId);

    // Truncate messages and add new user message (if editing) + assistant placeholder
    const assistantMessageId = `assistant-${Date.now()}`;
    contentOrderCounterRef.current = 0;
    currentReasoningIdRef.current = null;
    currentToolCallIdRef.current = null;
    // Edit/regenerate opens a fresh backend run; clear the prior run_id so
    // the new metadata frame becomes the source of truth.
    currentRunIdRef.current = null;
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

    const assistantMessage = createAssistantMessage(assistantMessageId);
    const userMessage = message ? createUserMessage(message) : null;

    if (userMessage) {
      recentlySentTrackerRef.current.track(message!.trim(), userMessage.timestamp, userMessage.id);
    }

    // Rebuild the rendered-interrupt set from the cards that survive the
    // truncation. The fork re-executes the turn server-side, and LangGraph
    // interrupt ids are deterministic — the new run can legitimately re-raise
    // the id of a card this truncation removes; a stale entry would suppress
    // the new card and leave the interrupt unanswerable. Done synchronously
    // (not in the setMessages updater) so the first stream event can't race
    // the rebuild. `messages` here is the same render snapshot the caller
    // computed truncateIndex against.
    renderedInterruptIdsRef.current = collectRenderedInterruptIds(messages.slice(0, truncateIndex));

    setMessages((prev) => {
      const truncated = prev.slice(0, truncateIndex);
      const newMsgs = userMessage
        ? [...truncated, userMessage, assistantMessage]
        : [...truncated, assistantMessage];
      newMessagesStartIndexRef.current = newMsgs.length;
      return newMsgs;
    });
    currentMessageRef.current = assistantMessageId;

    // Invalidate turn checkpoints cache (branch creates new checkpoints)
    turnCheckpointsRef.current = null;

    // One request_key per retry click / fork, reused on a retransmit after a
    // lost response — see createRequestKeyTracker.
    const requestKey = requestKeyRef.current.take(
      viaRetryEndpoint
        ? `retry|${threadId}`
        : `fork|${threadId}|${checkpointId ?? ''}|${forkFromTurn ?? ''}|${message ?? ''}`,
    );
    let wasDisconnected = false;
    const wasInterruptedRef = { current: false };
    try {
      const refs = buildStreamRefs();
      const processEvent = createStreamEventProcessor(runtime, streamRouterDeps, assistantMessageId, refs, getTaskIdFromEvent, wasInterruptedRef);

      // Retry goes through POST /retry (v4 attempt chain: server validates
      // the latest attempt + resolves the checkpoint, no truncation); edit/
      // regenerate keep the fork path.
      // Latch run_id from response headers — see handleSendMessage for the same
      // closing-the-race rationale. Shared by both branches.
      const latchRunId = (runId: string) => {
        requestKeyRef.current.clear();
        currentRunIdRef.current = runId;
      };
      const result = viaRetryEndpoint
        ? await sendRetryStream(
            workspaceId,
            threadId,
            processEvent,
            modelOptions.model || null,
            modelOptions.reasoningEffort || null,
            modelOptions.fastMode || null,
            latchRunId,
            abortController.signal,
            requestKey,
          )
        : await sendChatMessageStream(
            message || '',
            workspaceId,
            threadId,
            [],
            false,
            processEvent,
            null,
            agentMode,
            userLocale,
            userTimezone,
            checkpointId,
            forkFromTurn,
            modelOptions.model || null,
            modelOptions.reasoningEffort || null,
            modelOptions.fastMode || null,
            platform,
            latchRunId,
            abortController.signal,
            requestKey,
          );

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

      if (result?.disconnected) {
        wasDisconnected = true;
        attemptReconnectAfterDisconnect(assistantMessageId);
        return;
      }

      const finalId = currentMessageRef.current || assistantMessageId;
      setMessages((prev) =>
        updateMessage(prev,finalId, (msg) => ({
          ...msg,
          isStreaming: false,
        }))
      );
      markTranscriptPersisted();
    } catch (err: unknown) {
      if ((err as Error)?.name === 'AbortError' || wasStoppedRef.current) {
        return;
      }
      // 409 duplicate_request: an earlier copy of this retry/fork was already
      // accepted (its response was lost) — adopt that run instead of erroring.
      if (adoptDuplicateRun(err, assistantMessageId)) {
        wasDisconnected = true;
        return;
      }
      console.error('[streamFromCheckpoint] Error:', err);
      setMessageError((err as Error).message || 'Failed to process request');
      setMessages((prev) =>
        updateMessage(prev,assistantMessageId, (msg) => ({
          ...msg,
          content: msg.content || 'Failed to process request. Please try again.',
          isStreaming: false,
          error: true,
        }))
      );
    } finally {
      if (mainStreamAbortRef.current === abortController) {
        mainStreamAbortRef.current = null;
      }
      if (!wasDisconnected && !wasInterruptedRef.current && !wasStoppedRef.current) {
        const finalId = currentMessageRef.current || assistantMessageId;
        setMessages((prev) =>
          updateMessage(prev,finalId, (msg) => ({
            ...msg,
            isStreaming: false,
          }))
        );
        cleanupAfterStreamEnd(finalId);
      }
    }
  // `messages` is a real dep: the rendered-interrupt rebuild above needs the
  // same render snapshot the caller computed truncateIndex against.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [messages, workspaceId, threadId, agentMode]);

  /**
   * Edit a user message: truncate to before that message, send modified content
   * from the checkpoint before the original message was added.
   */
  const handleEditMessage = useCallback(async (messageId: string, newContent: string, modelOptions: ModelOptions = {}) => {
    if (!newContent?.trim()) return;

    const msgIndex = messages.findIndex((m) => m.id === messageId);
    if (msgIndex === -1) return;

    // Count non-steering assistant messages before this user message to get turn_index.
    // Excludes steering assistant messages (mid-turn continuations) which don't map to backend turns.
    const turnIndex = messages.slice(0, msgIndex).filter((m) => m.role === 'assistant' && !m.isSteering).length;

    // Immediate visual feedback: truncate, show edited message + loading placeholder.
    // Save snapshot so we can restore on failure.
    const snapshotMessages = messages;
    setIsLoading(true);
    setMessageError(null);
    setFallbackSuggestion(null);
    const editedUserMsg = createUserMessage(newContent);
    setMessages((prev) => [
      ...prev.slice(0, msgIndex),
      editedUserMsg,
      createAssistantMessage(`assistant-pending-${Date.now()}`),
    ]);

    const turnsData = await getTurnCheckpoints();
    if (!turnsData?.turns?.[turnIndex]) {
      setIsLoading(false);
      setMessages(snapshotMessages);
      setMessageError('Unable to edit: checkpoint data unavailable');
      return;
    }

    const checkpointId = turnsData.turns[turnIndex].edit_checkpoint_id;
    if (!checkpointId) {
      setIsLoading(false);
      setMessages(snapshotMessages);
      setMessageError('Unable to edit: this is the first message');
      return;
    }

    await streamFromCheckpoint(newContent, checkpointId, msgIndex, turnIndex, modelOptions);
  }, [messages, getTurnCheckpoints, streamFromCheckpoint]);

  /**
   * Regenerate an assistant response: truncate the assistant message,
   * re-run from the checkpoint that has the user message but before AI response.
   */
  const handleRegenerate = useCallback(async (messageId: string, modelOptions: ModelOptions = {}) => {
    const msgIndex = messages.findIndex((m) => m.id === messageId);
    if (msgIndex === -1) return;

    // Count non-steering assistant messages up to and including this one to get turn_index.
    // Excludes steering assistant messages (mid-turn continuations) which don't map to backend turns.
    const turnIndex = messages.slice(0, msgIndex + 1).filter((m) => m.role === 'assistant' && !m.isSteering).length - 1;

    // Immediate visual feedback: truncate at the assistant message, show loading placeholder.
    // Save snapshot so we can restore on failure.
    const snapshotMessages = messages;
    setIsLoading(true);
    setMessageError(null);
    setFallbackSuggestion(null);
    setMessages((prev) => [
      ...prev.slice(0, msgIndex),
      createAssistantMessage(`assistant-pending-${Date.now()}`),
    ]);

    const turnsData = await getTurnCheckpoints();
    if (!turnsData?.turns?.[turnIndex]) {
      setIsLoading(false);
      setMessages(snapshotMessages);
      setMessageError('Unable to regenerate: checkpoint data unavailable');
      return;
    }

    const checkpointId = turnsData.turns[turnIndex].regenerate_checkpoint_id;
    // Truncate at the assistant message (keep everything before it, including user msg)
    await streamFromCheckpoint(null, checkpointId, msgIndex, turnIndex, modelOptions);
  }, [messages, getTurnCheckpoints, streamFromCheckpoint]);

  /**
   * Retry the last failed turn as a new attempt on the same turn (v4 attempt
   * chain). The backend validates the latest attempt and resolves the retry
   * checkpoint itself — no client checkpoint fetch, no fork/truncation of
   * persisted turns. The UI still replaces the errored bubble in place so the
   * positional assistant-bubble count stays aligned with backend turn_index.
   */
  const handleRetry = useCallback(async (modelOptions: ModelOptions = {}) => {
    const lastErrorIndex = messages.findLastIndex((m) => m.role === 'assistant' && (m as AssistantMessage).error);
    const truncateIndex = lastErrorIndex !== -1 ? lastErrorIndex : messages.length;
    await streamFromCheckpoint(null, null, truncateIndex, null, modelOptions, true);
  }, [messages, streamFromCheckpoint]);

  /** Cross-lane callbacks for the live event router; direct references, so
   * this literal must stay below every referent. */
  const streamRouterDeps: StreamRouterDeps = {
    applyFallbackSuggestion,
    applyModelStatus,
    clearModelStatus,
    handleSendSteering,
    insertNotification,
    loadConversationHistory,
    releaseStreamOwnership,
    attachSubagentMux,
    setMarketWatch,
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
    reportBackWatch,
  };

  return {
    messages,
    threadId,
    threadModels,
    lastThreadModel,
    isLoading,
    marketWatch,
    hasActiveSubagents,
    awaitingReportBack,
    workspaceStarting,
    isCompacting,
    setIsCompacting,
    queuedSend,
    isLoadingHistory,
    isReconnecting,
    modelStatus,
    fallbackSuggestion,
    clearFallbackSuggestion,
    reconnectIfStaleRun: reportBackWatch.reconnectIfStaleRun,
    messageError,
    returnedSteering,
    clearReturnedSteering: () => setReturnedSteering(null),
    handleSendMessage,
    stopWorkflow,
    stopCompaction,
    pendingInterrupt,
    pendingRejection,
    handleApproveInterrupt,
    handleRejectInterrupt,
    handleAnswerQuestion,
    handleSkipQuestion,
    handleApproveCreateWorkspace,
    handleRejectCreateWorkspace,
    handleApproveStartQuestion,
    handleRejectStartQuestion,
    handleApprovePTCAgent,
    handleRejectPTCAgent,
    handleApproveSecretaryAction,
    handleRejectSecretaryAction,
    tokenUsage,
    isShared,
    insertNotification,
    handleEditMessage,
    handleRegenerate,
    handleRetry,
    handleThumbUp,
    handleThumbDown,
    getFeedbackForMessage,
    // Resolve subagentId (e.g. toolCallId from segment) to stable agent_id for card operations.
    resolveSubagentIdToAgentId: (subagentId: string) =>
      toolCallIdToTaskIdMapRef.current.get(subagentId) || subagentId,
    // Expose subagent history for lazy loading. Resolves toolCallId -> agent_id via mapping.
    // Returns { ...historyData, agentId } so caller can use agentId for card operations.
    getSubagentHistory: (subagentId: string) => {
      const agentId = toolCallIdToTaskIdMapRef.current.get(subagentId) || subagentId;
      const data = subagentHistoryRef.current?.[agentId];
      return data ? { ...data, agentId } : null;
    },
  };
}
