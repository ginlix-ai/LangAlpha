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
import { sendChatMessageStream, replayThreadHistory, getWorkflowStatus, reconnectToWorkflowStream, sendHitlResponse, streamSubagentTaskEvents, fetchThreadTurns, submitFeedback, removeFeedback, getThreadFeedback, watchThread, cancelWorkflow } from '../utils/api';
import { toast } from '@/components/ui/use-toast';
import { buildRateLimitError, isUpstreamHint, type StructuredError } from '@/utils/rateLimitError';
import { getStoredThreadId, setStoredThreadId } from './utils/threadStorage';
import { countToolCalls } from '../utils/subagentMetrics';
import { type SubagentTokenUsage, ZERO_USAGE, extractTokenUsageDelta, accumulateTokenUsage } from '../utils/tokenUsage';
import { computeSteeringBoundary, shouldSkipSteeringRollback } from '../utils/steeringRollback';
import { bumpThreadNavOrder } from './useNavigationData';
export { removeStoredThreadId } from './utils/threadStorage';
import { createUserMessage, createAssistantMessage, createNotificationMessage, appendMessage, updateMessage, type AttachmentMeta } from './utils/messageHelpers';
import type { ChatMessage, AssistantMessage, UserMessage } from '@/types/chat';
import type { ActionRequest, ToolCallData, TodoItem } from '@/types/sse';
import type { HtmlWidgetData, PreviewData } from './utils/types';
import { createRecentlySentTracker } from './utils/recentlySentTracker';
import {
  handleReasoningSignal,
  handleReasoningContent,
  handleTextContent,
  handleToolCalls,
  handleToolCallResult,
  handleToolCallChunks,
  handleTodoUpdate,
  handleHtmlWidget,
  handleProvenance,
  isSubagentEvent,
  handleSubagentMessageChunk,
  handleSubagentToolCallChunks,
  handleSubagentToolCalls,
  handleSubagentToolCallResult,
  handleTaskSteeringAccepted,
  getOrCreateTaskRefs,
} from './utils/streamEventHandlers';
import {
  handleHistoryUserMessage,
  handleHistoryReasoningSignal,
  handleHistoryReasoningContent,
  handleHistoryTextContent,
  handleHistoryToolCalls,
  handleHistoryToolCallResult,
  handleHistoryTodoUpdate,
  handleHistoryHtmlWidget,
  handleHistoryProvenance,
  handleHistorySteeringDelivered,
  isSubagentHistoryEvent,
} from './utils/historyEventHandlers';
// Chart-annotation live bridge: writes agent-drawn annotations into the
// shared MarketView store so the desktop MarketView chat panel (which uses
// this engine for both flash and PTC) renders them live. Harmless on the
// standalone /chat page — the store simply has no chart consumer there.
import { applyAnnotationArtifact } from '@/pages/MarketView/stores/chartAnnotationStore';

// --- Internal types for useChatMessages ---

/** Message record — now properly typed as ChatMessage. */
type MessageRecord = ChatMessage;

/** React state setter for messages array. */
type SetMessages = React.Dispatch<React.SetStateAction<MessageRecord[]>>;

/** Token usage state for context window progress ring. */
interface TokenUsage {
  totalInput: number;
  totalOutput: number;
  lastOutput: number;
  total: number;
  threshold: number;
}

/** Interrupt types that map to proposal-based HITL cards (workspace, question, ptc, secretary). */
const PROPOSAL_INTERRUPT_TYPES = new Set([
  'create_workspace', 'start_question', 'ptc_agent',
  'delete_workspace', 'stop_workspace', 'delete_thread',
]);

/** Maps interrupt types to their proposal bucket key on AssistantMessage. */
const PROPOSAL_DATA_KEY_MAP: Record<string, string> = {
  create_workspace: 'workspaceProposals',
  start_question: 'questionProposals',
  ptc_agent: 'ptcAgentProposals',
  delete_workspace: 'secretaryActionProposals',
  stop_workspace: 'secretaryActionProposals',
  delete_thread: 'secretaryActionProposals',
};

/** Secretary action interrupt types (for type guard in handlers). */
const SECRETARY_ACTION_TYPES = new Set(['delete_workspace', 'stop_workspace', 'delete_thread']);

/**
 * Stop polling `/status` for a PTC report-back after this many ~3s ticks (~3min)
 * so a report-back that never arrives (e.g. PTC dispatch failed, flash_watch
 * never cleared) doesn't poll forever.
 */
const REPORT_BACK_MAX_POLLS = 60;

/** Pending HITL interrupt state. */
interface PendingInterrupt {
  type?: string;
  interruptId?: string;
  assistantMessageId?: string;
  planApprovalId?: string;
  questionId?: string;
  proposalId?: string;
  planMode?: boolean;
  actionRequests?: ActionRequest[];
  threadId?: string;
  toolCallId?: string;
}

/** Pending rejection (user rejected a plan). */
interface PendingRejection {
  interruptId: string;
  planMode: boolean;
}

/** Loosely-typed SSE event — all event shapes merged. */
// TODO: type properly — use discriminated union from src/types/sse.ts
interface SSEEvent {
  event?: string;
  agent?: string;
  content?: string | Record<string, unknown>;
  content_type?: string;
  role?: string;
  turn_index?: number;
  _eventId?: number | string;
  timestamp?: string | number;
  metadata?: Record<string, unknown>;
  tool_calls?: ToolCallData[];
  tool_call_id?: string;
  tool_call_chunks?: Array<{ id?: string; name?: string; args?: string }>;
  finish_reason?: string;
  artifact_type?: string;
  artifact_id?: string;
  artifact?: Record<string, unknown>;
  payload?: Record<string, unknown>;
  thread_id?: string;
  messages?: Record<string, unknown>[];
  interrupt_id?: string;
  action_requests?: ActionRequest[];
  status?: string;
  signal?: string;
  action?: string;
  error?: string;
  message?: string;
  input_tokens?: number;
  output_tokens?: number;
  total_tokens?: number;
  threshold?: number;
  original_message_count?: number;
  offloaded_args?: number;
  offloaded_reads?: number;
  kind?: string;
  position?: number;
  active_tasks?: string[];
  can_reconnect?: boolean;
  is_shared?: boolean;
  run_id?: string;
  [key: string]: unknown;
}

/** Workflow status response. */
interface WorkflowStatusResponse {
  can_reconnect: boolean;
  status: string;
  active_tasks?: string[];
  is_shared?: boolean;
  pending_report_back?: boolean;
  report_back_run_id?: string | null;
  run_id?: string | null;
  [key: string]: unknown;
}

/** Model options for send/edit/regenerate. */
interface ModelOptions {
  model?: string | null;
  reasoningEffort?: string | null;
  fastMode?: boolean | null;
  /**
   * Widget context snapshots attached to this send. Stored on the
   * UserMessage so the chat history can render them as inline chip cards
   * below the user bubble (like attachments).
   */
  widgetSnapshots?: import('@/pages/Dashboard/widgets/framework/contextSnapshot').WidgetContextSnapshot[];
  /**
   * Chart selections attached to this send. Stored on the UserMessage so the
   * chat renders read-only pills below the user bubble (like widgetSnapshots).
   */
  chartSelections?: import('@/pages/MarketView/stores/chartSelectionStore').ChartSelectionSnapshot[];
}

/** Offload batch ref state. */
interface OffloadBatch {
  args: number;
  reads: number;
  timer: ReturnType<typeof setTimeout> | null;
  msgId?: string | null;
}

/** Callbacks for handleContextWindowEvent. */
interface ContextWindowCallbacks {
  getMsgId: () => string | null;
  nextOrder: () => number;
  setMessages: SetMessages;
  setTokenUsage: React.Dispatch<React.SetStateAction<TokenUsage | null>>;
  setIsCompacting: ((v: string | false) => void) | null;
  insertNotification: (text: string, variant?: 'info' | 'success' | 'warning', detail?: string) => void;
  t: (key: string, opts?: Record<string, unknown>) => string;
  offloadBatch: React.MutableRefObject<OffloadBatch>;
}

/** Subagent history entry stored in subagentHistoryRef. */
interface SubagentHistoryEntry {
  taskId: string;
  description: string;
  prompt: string;
  type: string;
  messages: Record<string, unknown>[];
  status: string;
  toolCalls: number;
  tokenUsage: SubagentTokenUsage;
  currentTool: string;
}

/** Per-task ref state used by stream handlers.
 *  messages is Record<string, unknown>[] to match the handler module's MessageRecord type. */
interface TaskRefs {
  contentOrderCounterRef: { current: number };
  currentReasoningIdRef: { current: string | null };
  currentToolCallIdRef: { current: string | null };
  messages: Record<string, unknown>[];
  runIndex: number;
}

/** History interrupt info stored during replay. */
interface HistoryInterruptInfo {
  type: string;
  assistantMessageId: string;
  planApprovalId?: string;
  questionId?: string;
  proposalId?: string;
  interruptId?: string;
  answer?: string | null;
}

/** Subagent history data accumulated during replay. */
interface SubagentHistoryData {
  messages: Record<string, unknown>[];
  events: SSEEvent[];
  description?: string;
  prompt?: string;
  type?: string;
  resumePoints: Array<{ description: string; turnIndex?: number }>;
}

/** Refs passed to createStreamEventProcessor and its processEvent closure. */
interface StreamProcessorRefs {
  contentOrderCounterRef: { current: number };
  currentReasoningIdRef: { current: string | null };
  currentToolCallIdRef: { current: string | null };
  steeringAtOrderRef?: { current: number | null };
  updateTodoListCard?: ((data: Record<string, unknown>, isNew: boolean) => void) | undefined;
  isNewConversation?: boolean;
  subagentStateRefs?: Record<string, TaskRefs>;
  updateSubagentCard?: ((agentId: string, data: Record<string, unknown>) => void);
  isReconnect?: boolean;
  unresolvedHistoryInterruptRef?: React.MutableRefObject<HistoryInterruptInfo[]>;
  [key: string]: unknown;
}

/** Pair state tracked per turn_index during history replay. */
interface PairState {
  contentOrderCounter: number;
  reasoningId: string | null;
  toolCallId: string | null;
}


/**
 * Checks if a tool result indicates an onboarding-related success.
 * Onboarding tools: update_user_data for risk_preference, watchlist_item, portfolio_holding.
 * @param {string|object} resultContent - Raw result content (JSON string or parsed object)
 * @returns {boolean}
 */
function isOnboardingRelatedToolSuccess(resultContent: unknown): boolean {
  if (resultContent == null) return false;
  let parsed;
  if (typeof resultContent === 'string') {
    try {
      parsed = JSON.parse(resultContent);
    } catch {
      return false;
    }
  } else if (typeof resultContent === 'object') {
    parsed = resultContent;
  } else {
    return false;
  }
  if (!parsed || parsed.success !== true) return false;
  return !!(parsed.risk_preference || parsed.watchlist_item || parsed.portfolio_holding);
}

/**
 * Shared handler for context_window SSE events (token_usage, summarize, offload).
 * Used by both history replay and live stream to avoid duplication.
 *
 * @param {Object} event - The context_window event
 * @param {Object} callbacks
 * @param {Function} callbacks.getMsgId - Returns current assistant message ID (or null)
 * @param {Function} callbacks.nextOrder - Returns next content order counter value
 * @param {Function} callbacks.setMessages - React state setter for messages
 * @param {Function} callbacks.setTokenUsage - React state setter for token usage
 * @param {Function|null} callbacks.setIsCompacting - React state setter (null for history)
 * @param {Function} callbacks.insertNotification - Fallback: inserts standalone notification message
 * @param {Function} callbacks.t - i18n translation function
 * @param {React.MutableRefObject} callbacks.offloadBatch - Mutable ref for batching offload events
 */
function handleContextWindowEvent(event: SSEEvent, { getMsgId, nextOrder, setMessages, setTokenUsage, setIsCompacting, insertNotification, t, offloadBatch }: ContextWindowCallbacks): void {
  const action = event.action;

  if (action === 'token_usage') {
    const callInput = event.input_tokens || 0;
    const callOutput = event.output_tokens || 0;
    setTokenUsage((prev: TokenUsage | null) => ({
      totalInput: (prev?.totalInput || 0) + callInput,
      totalOutput: (prev?.totalOutput || 0) + callOutput,
      lastOutput: callOutput,
      total: event.total_tokens || 0,
      threshold: event.threshold || prev?.threshold || 0,
    }));
    return;
  }

  if (action === 'summarize') {
    // SSE action value "summarize" preserved as wire protocol; the UI surfaces
    // this as context compaction.
    if (setIsCompacting && event.signal === 'start') {
      setIsCompacting('summarize');
      return;
    }
    if (setIsCompacting) setIsCompacting(false);
    if (event.signal === 'complete') {
      const text = t('chat.compactedNotification', { from: event.original_message_count });
      const detail = (event.summary_text as string | undefined) || undefined;
      const msgId = getMsgId();
      if (msgId) {
        const order = nextOrder();
        setMessages((prev) => updateMessage(prev,msgId, (msg) => {
          if (msg.role !== 'assistant') return msg;
          const aMsg = msg as AssistantMessage;
          return {
            ...aMsg,
            contentSegments: [...(aMsg.contentSegments || []), { type: 'notification' as const, content: text, order, detail }],
          };
        }));
      } else {
        insertNotification(text, 'info', detail);
      }
    }
    return;
  }

  if (action === 'offload') {
    if (event.signal === 'complete') {
      const batch = offloadBatch;

      // Accumulate counts
      if (event.kind === 'reads') {
        batch.current.reads += event.offloaded_reads || 0;
      } else if (event.kind === 'args') {
        batch.current.args += event.offloaded_args || 0;
      } else {
        // Manual /offload — combined event
        batch.current.args += event.offloaded_args || 0;
        batch.current.reads += event.offloaded_reads || 0;
      }

      // Capture msgId from first event in batch
      if (batch.current.msgId === undefined) {
        batch.current.msgId = getMsgId();
      }

      // Debounce: merge back-to-back offload events into a single notification
      if (batch.current.timer) clearTimeout(batch.current.timer);
      batch.current.timer = setTimeout(() => {
        const { args, reads, msgId } = batch.current;
        let text;
        if (args > 0 && reads > 0) {
          text = t('chat.offloadedNotification', { args, reads });
        } else if (reads > 0) {
          text = t('chat.offloadedReadsNotification', { count: reads });
        } else if (args > 0) {
          text = t('chat.offloadedArgsNotification', { count: args });
        }

        if (text) {
          if (msgId) {
            const order = nextOrder();
            setMessages((prev) => updateMessage(prev,msgId, (msg) => {
              if (msg.role !== 'assistant') return msg;
              const aMsg = msg as AssistantMessage;
              return {
                ...aMsg,
                contentSegments: [...(aMsg.contentSegments || []), { type: 'notification' as const, content: text, order }],
              };
            }));
          } else {
            insertNotification(text);
          }
        }

        // Reset batch
        batch.current = { args: 0, reads: 0, timer: null, msgId: undefined };
      }, 100);
    }
    return;
  }
}

/**
 * Marks incomplete todos as 'stale' in todoListProcesses of assistant messages.
 * Used when the agent stream ends without completing all todos.
 * @param messages - Current messages array
 * @param targetMessageId - If provided, only finalize the specific message; otherwise finalize all
 */
export function finalizeTodoListProcessesInMessages(
  messages: MessageRecord[],
  targetMessageId?: string
): MessageRecord[] {
  let anyChanged = false;
  const updated = messages.map((m) => {
    if (m.role !== 'assistant') return m;
    if (targetMessageId && m.id !== targetMessageId) return m;
    const am = m as AssistantMessage;
    if (!am.todoListProcesses || Object.keys(am.todoListProcesses).length === 0) return m;
    const entries = Object.entries(am.todoListProcesses);
    const lastEntry = entries.reduce((a, b) => ((a[1].order || 0) >= (b[1].order || 0) ? a : b));
    const [lastKey, lastVal] = lastEntry;
    if (!Array.isArray(lastVal.todos)) return m;
    const hasIncomplete = lastVal.todos.some(
      (todo: TodoItem) => todo.status !== 'completed' && todo.status !== 'stale'
    );
    if (!hasIncomplete) return m;
    anyChanged = true;
    const finalizedTodos: TodoItem[] = lastVal.todos.map((todo: TodoItem) =>
      todo.status === 'completed' || todo.status === 'stale'
        ? todo
        : { ...todo, status: 'stale' as const }
    );
    return {
      ...am,
      todoListProcesses: {
        ...am.todoListProcesses,
        [lastKey]: { ...lastVal, todos: finalizedTodos, in_progress: 0, pending: 0 },
      },
    };
  });
  return anyChanged ? updated : messages;
}

/**
 * Map a task artifact event's tool_call_id to its agentId and drain the pending queue.
 *
 * When multiple Task tool calls are in a single tool_calls event, the pending queue
 * holds their IDs in array order. Because LangGraph processes tool calls in parallel,
 * artifact events may arrive in a different order. This function uses a direct mapping
 * (by value) when tool_call_id is available, falling back to FIFO for legacy events.
 *
 * @returns Updated pending queue after draining.
 */
export function mapToolCallIdToAgentId(
  eventToolCallId: string | undefined,
  agentId: string,
  action: string,
  pendingToolCallIds: string[],
  toolCallIdMap: Map<string, string>,
): string[] {
  if (eventToolCallId) {
    toolCallIdMap.set(eventToolCallId, agentId);
  }
  if (action !== 'init') {
    return pendingToolCallIds;
  }
  if (pendingToolCallIds.length === 0) {
    return pendingToolCallIds;
  }
  if (eventToolCallId) {
    // Direct mapping — remove by value (not FIFO) since parallel tool calls
    // may complete in different order than the tool_calls array.
    return pendingToolCallIds.filter(id => id !== eventToolCallId);
  }
  // Legacy fallback: FIFO drain for events without tool_call_id.
  const [firstId, ...rest] = pendingToolCallIds;
  if (!toolCallIdMap.has(firstId)) {
    toolCallIdMap.set(firstId, agentId);
  }
  return rest;
}

export function useChatMessages(
  workspaceId: string,
  initialThreadId: string | null = null,
  updateTodoListCard: ((todoData: Record<string, unknown>, isNew?: boolean) => void) | null = null,
  updateSubagentCard: ((agentId: string, data: Record<string, unknown>) => void) | null = null,
  inactivateAllSubagents: (() => void) | null = null,
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

  // Refs for history loading state
  const historyLoadingRef = useRef(false);
  const historyMessagesRef = useRef(new Set<string>()); // Track message IDs from history
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

  // (isStreamingRef, streamingThreadIdRef) are one invariant: a reconnect stream
  // is owned by exactly one thread, or none. Mutate them only through these so
  // the flag and the owner can never drift out of sync (the supersede logic and
  // the thread-load guard both depend on them agreeing).
  const acquireStreamOwnership = (tid: string | null) => {
    isStreamingRef.current = true;
    streamingThreadIdRef.current = tid;
  };
  const releaseStreamOwnership = () => {
    isStreamingRef.current = false;
    streamingThreadIdRef.current = null;
  };

  // Feedback state: { [turnIndex]: { rating, ... } }
  const feedbackMapRef = useRef<Record<number, { rating: string | null; [key: string]: unknown }>>({});

  // Track if history replay found an unresolved interrupt (skip reconnection in that case)
  const historyHasUnresolvedInterruptRef = useRef(false);
  // Store the full interrupt details from history so loadAndMaybeReconnect can decide
  // whether to make it interactive or reconnect to get resolution events
  const unresolvedHistoryInterruptRef = useRef<HistoryInterruptInfo[]>([]);

  // Batch parallel interrupt responses: track all interrupt IDs in current batch
  // and collect individual responses until all are answered, then resume at once.
  const pendingInterruptIdsRef = useRef(new Set<string>());
  const collectedHitlResponsesRef = useRef<Record<string, { decisions: Array<{ type: string; message?: string }> }>>({});

  // Track approved PTC agent proposals waiting for thread_id backfill from tool_call_result.
  // Set by handleApprovePTCAgent, consumed by tool_call_result handler in the stream processor.
  // Maps tool_call_id → proposalId for exact matching (safe under concurrent dispatches).
  const pendingPTCBackfillRef = useRef<Map<string, string>>(new Map());

  // Report-back watch: after PTC dispatch, watch for the flash report-back workflow via SSE
  const awaitingReportBackRef = useRef(false);
  const reportBackWatchAbortRef = useRef<AbortController | null>(null);
  const reportBackPollRef = useRef<ReturnType<typeof setInterval> | null>(null);
  // The flash thread the active report-back watch belongs to. The watch is keyed
  // to this thread, NOT the visible thread, so it survives navigation into the
  // dispatched PTC thread and only renders when this flash thread is back on
  // screen. This is what lets the flash report-back and a live PTC stream coexist
  // instead of fighting over the single visible-thread slot. Null when no watch.
  const reportBackWatchThreadIdRef = useRef<string | null>(null);
  // The report-back run id once the backend names it (wake payload or /status).
  // Captured even while the user is away on the PTC thread, so the report-back
  // still streams when they return to the flash thread. Cleared on attach/stop.
  const reportBackRunIdRef = useRef<string | null>(null);
  // Generation token for the report-back watch. Bumped whenever a watch is torn
  // down (consume, hard-stop, re-arm for a DIFFERENT flash thread, unmount). An
  // in-flight wake/poll callback captures its generation and bails if it no
  // longer matches. (Can't key off threadIdRef: it has two writers — the
  // prop-sync effect and the stream metadata handler — that disagree transiently
  // on a fresh thread, which would bail even the in-session case.)
  const reportBackWatchEpochRef = useRef(0);

  // Track the last received SSE event ID for reconnection
  const lastEventIdRef = useRef<number | string | null>(null);
  // Track the active run_id for this thread. Populated from the SSE
  // ``metadata`` event (first event of every workflow stream) so reconnect
  // can target the exact ``workflow:stream:{tid}:{rid}`` key and so the
  // steering handler can detect when a POST was routed as a new turn
  // (race: status flipped terminal between isLoading check and POST land).
  const currentRunIdRef = useRef<string | null>(null);
  // Ref-based thread ID for use inside closures (avoids stale React state in callbacks)
  const threadIdRef = useRef(threadId);
  // Batch back-to-back offload events into a single notification
  const offloadBatchRef = useRef<OffloadBatch>({ args: 0, reads: 0, timer: null });
  // Track reconnection state for UI indicator
  const [isReconnecting, setIsReconnecting] = useState(false);
  // Counter to re-trigger loadAndMaybeReconnect after failed reconnection
  const [reloadTrigger, setReloadTrigger] = useState(0);

  // Track if this is a new conversation (for todo list card management)
  const isNewConversationRef = useRef(false);

  // Recently sent messages tracker
  const recentlySentTrackerRef = useRef(createRecentlySentTracker());

  // Map tool call IDs (from main agent's task tool calls) to agent_ids for routing subagent events
  const toolCallIdToTaskIdMapRef = useRef(new Map<string, string>()); // Map<toolCallId, agentId>

  // Per-task SSE connections: taskId → AbortController
  const subagentStreamsRef = useRef(new Map<string, AbortController>());

  // Track completed task IDs to prevent reactivation by stale artifact events
  const completedTaskIdsRef = useRef(new Set<string>());

  // Track subagent history loaded from replay so it can be shown lazily
  // Keyed by agent_id. Structure: { [agentId]: { taskId, description, type, messages, status, ... } }
  const subagentHistoryRef = useRef<Record<string, SubagentHistoryEntry>>({});

  // Persistent subagent state refs — survives across turns so resumed subagents
  // retain messages from previous runs. Keyed by taskId (e.g., "task:k7Xm2p").
  const subagentStateRefsRef = useRef<Record<string, TaskRefs>>({});

  // Per-task running total of token usage. Backend emits per-call deltas via
  // `context_window/token_usage` events; we sum here. The ref is the source
  // of truth for accumulation; SubagentData.tokenUsage is overwritten with
  // the running total on each update so the projection in ChatView reads it
  // without needing direct ref access. Reconnect during an active subagent
  // can double-count (event replay) — acceptable for a UI display surface.
  const subagentTokenUsageRef = useRef<Record<string, SubagentTokenUsage>>({});

  // During history load: queue task tool call IDs until the matching artifact 'spawned' event drains them
  const historyPendingTaskToolCallIdsRef = useRef<string[]>([]);

  // Keep threadIdRef in sync with state (for use inside closures)
  useEffect(() => {
    threadIdRef.current = threadId;
    if (workspaceId && threadId && threadId !== '__default__') {
      setStoredThreadId(workspaceId, threadId);
    }
  }, [workspaceId, threadId]);

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
        historyMessagesRef.current.clear();
        newMessagesStartIndexRef.current = 0;
        recentlySentTrackerRef.current.clear();
        turnCheckpointsRef.current = null;
      }
    }
  }, [workspaceId, initialThreadId]);

  /**
   * Loads conversation history for the current workspace and thread
   * Uses the threadId from state (which should be a valid thread ID, not '__default__')
   */
  const loadConversationHistory = async (): Promise<boolean> => {
    if (!workspaceId || !threadId || threadId === '__default__' || historyLoadingRef.current) {
      return false;
    }

    try {
      historyLoadingRef.current = true;
      historyHasUnresolvedInterruptRef.current = false;
      setIsLoadingHistory(true);
      setMessageError(null);

      // Reset history-tracking state so a re-replay (e.g. after a failed
      // reconnect → setReloadTrigger increment) starts from a clean slate.
      // Without this, the bubble-creation handlers in historyEventHandlers
      // would re-insert bubbles atop the prior load's bubbles. With the
      // newly deterministic bubble ids (`history-{role}-{pairIndex}`), the
      // duplicate insert would also trip React's same-key warnings.
      // ``isHistory: true`` only marks bubbles produced by this loader, so
      // any in-flight streaming bubble survives the filter.
      historyMessagesRef.current.clear();
      newMessagesStartIndexRef.current = 0;
      setMessages((prev) => prev.filter((m) => !m.isHistory));

      // Fresh attach (refresh or thread switch): clear the live-stream cursor so
      // the subsequent reconnect replays the run's stream from the start. We
      // hold no live events yet, and the replay id-space must not leak into the
      // live reconnect (see the note in the replay handler below). The
      // mid-stream disconnect-retry path does NOT call this loader, so its
      // resume cursor is preserved.
      lastEventIdRef.current = null;

      const threadIdToUse = threadId;

      // Track pairs being processed - use Map to handle multiple pairs
      const assistantMessagesByPair = new Map<number, string>(); // Map<turn_index, assistantMessageId>
      const pairStateByPair = new Map<number, PairState>(); // Map<turn_index, { contentOrderCounter, reasoningId, toolCallId }>

      // Track the currently active pair for artifacts (which don't have turn_index)
      // This ensures artifacts get the correct chronological order
      let currentActivePairIndex: number | null = null;
      let currentActivePairState: PairState | null | undefined = null;

      // Track pending HITL interrupts from history to resolve status on next user_message
      const pendingHistoryInterrupts: HistoryInterruptInfo[] = [];

      // Track subagent events by task ID for this history load
      // Map<taskId, { messages: Array, events: Array, description?: string, type?: string }>
      const subagentHistoryByTaskId = new Map<string, SubagentHistoryData>();
      // Track which agentIds had steering_accepted actions (for inline card "Updated" label)
      const steeredAgentIds = new Set<string>();
      try {
        await replayThreadHistory(threadIdToUse, (_rawEvent) => {
        // Cast to SSEEvent for type-safe field access within this callback
        const event = _rawEvent as SSEEvent;
        const eventType = event.event;
        const contentType = event.content_type;
        const hasRole = event.role !== undefined;
        const hasPairIndex = event.turn_index !== undefined;

        // NOTE: do NOT write `event._eventId` into `lastEventIdRef` here.
        // Replay (`/messages/replay`) numbers events with a cumulative
        // per-thread counter, while the live workflow stream
        // (`workflow:stream:{tid}:{rid}`) resets its ids to 1 per run. After a
        // refresh, carrying the replay cursor into the live reconnect overshoots
        // the run's id space — the backend's XREAD blocks forever and the stream
        // delivers zero events (frozen response). `lastEventIdRef` must only ever
        // track ids received on the LIVE stream (set in the streaming
        // `processEvent` handler); history dedup uses deterministic bubble ids.

        // compaction_chunk is the side channel for LLM output from the
        // compaction middleware (bracketed by context_window summarize
        // start/complete events). Drop here so it never merges into the
        // assistant message — future UI can subscribe separately to show
        // what was summarized.
        if (eventType === 'compaction_chunk') {
          return;
        }

        // Check if this is a subagent event - filter it out from main chat view
        const isSubagent = isSubagentHistoryEvent(event as Record<string, unknown>);

        // Update current active pair when we see an event with turn_index
        if (hasPairIndex) {
          const pairIndex = event.turn_index!;
          currentActivePairIndex = pairIndex;
          currentActivePairState = pairStateByPair.get(pairIndex);
        }

        // Handle context_window events from history (token_usage, summarize, offload)
        // Subagent context_window events are routed through the isSubagent block below.
        if (eventType === 'context_window' && !isSubagent) {
          handleContextWindowEvent(event, {
            getMsgId: () => currentActivePairIndex !== null
              ? (assistantMessagesByPair.get(currentActivePairIndex) ?? null) : null,
            nextOrder: () => {
              const eventId = event._eventId;
              if (eventId != null) return Number(eventId);
              if (currentActivePairState) {
                currentActivePairState.contentOrderCounter++;
                return currentActivePairState.contentOrderCounter;
              }
              return 0;
            },
            setMessages,
            setTokenUsage,
            setIsCompacting: null,  // no start events in replayed history
            insertNotification: () => {},  // standalone notifications not needed in replay
            t,
            offloadBatch: offloadBatchRef,
          });
          return;
        }

        // Backward compat: handle old token_usage events from history
        if (eventType === 'token_usage') {
          const callInput = event.input_tokens || 0;
          const callOutput = event.output_tokens || 0;
          setTokenUsage((prev: TokenUsage | null) => ({
            totalInput: (prev?.totalInput || 0) + callInput,
            totalOutput: (prev?.totalOutput || 0) + callOutput,
            lastOutput: callOutput,
            total: event.total_tokens || 0,
            threshold: event.threshold || prev?.threshold || 0,
          }));
          return;
        }

        // Handle steering_delivered events from sse_events (main agent only;
        // subagent steering_delivered events are routed through the isSubagent block below)
        if (eventType === 'steering_delivered' && hasPairIndex && !isSubagent) {
          handleHistorySteeringDelivered({
            event: event as Record<string, unknown>,
            pairIndex: event.turn_index!,
            assistantMessagesByPair,
            pairStateByPair,
            refs: { newMessagesStartIndexRef, historyMessagesRef },
            setMessages: setMessagesForHandlers,
          });
          return;
        }

        // Handle provenance events from history replay. Re-attach the
        // accessed-data record to the turn's assistant message via the replay
        // `turn_index` envelope. Placed BEFORE the isSubagent block so
        // subagent-emitted records (agent="task:...") still re-attach to the
        // main turn's message on reload, mirroring the live dispatch.
        if (eventType === 'provenance' && hasPairIndex) {
          const pairIndex = event.turn_index!;
          currentActivePairIndex = pairIndex;
          currentActivePairState = pairStateByPair.get(pairIndex);

          const currentAssistantMessageId = assistantMessagesByPair.get(pairIndex);
          if (!currentAssistantMessageId) {
            console.warn('[History] Received provenance for unknown turn_index:', pairIndex);
            return;
          }

          handleHistoryProvenance({
            assistantMessageId: currentAssistantMessageId,
            event: event as unknown as import('@/types/sse').ProvenanceEvent,
            setMessages: setMessagesForHandlers,
          });
          return;
        }

        // Handle subagent events - store them separately, don't process in main chat
        if (isSubagent) {
          // With task:{task_id} format, the agent field IS the task key
          const taskId = event.agent;

          if (taskId) {
            // Initialize subagent history storage if needed
            if (!subagentHistoryByTaskId.has(taskId)) {
              subagentHistoryByTaskId.set(taskId, {
                messages: [],
                events: [],
                resumePoints: [],
              });
            }

            const subagentHistory = subagentHistoryByTaskId.get(taskId)!;
            // Store the event for later processing
            subagentHistory.events.push(event);
          } else {
            console.warn('[History] Subagent event without agent field:', {
              eventType,
              agent: event.agent,
            });
          }

          // Don't process subagent events in main chat view
          return;
        }

        // Handle user_message events from history
        // Note: event.content may be empty for HITL resume pairs (plan approval/rejection)
        if (eventType === 'user_message' && hasPairIndex) {
          // Collect LLM models from query metadata (may differ across turns)
          if (event.metadata?.llm_model) {
            const llmModel = event.metadata.llm_model as string;
            setThreadModels(prev => prev.includes(llmModel) ? prev : [...prev, llmModel]);
            // History replays chronologically, so the last write wins = most recent query's model.
            setLastThreadModel(llmModel);
          }
          // Resolve pending plan_approval interrupt from content (empty = approved, non-empty = rejected).
          {
            const idx = pendingHistoryInterrupts.findIndex((p) => p.type === 'plan_approval');
            if (idx !== -1) {
              const matched = pendingHistoryInterrupts[idx];
              const hasContent = typeof event.content === 'string' && event.content.trim();
              const resolvedStatus = hasContent ? 'rejected' : 'approved';
              setMessages((prev) =>
                updateMessage(prev,matched.assistantMessageId, (msg) => {
                  if (msg.role !== 'assistant') return msg;
                  const aMsg = msg as AssistantMessage;
                  const approvals = aMsg.planApprovals || {};
                  const key = matched.planApprovalId!;
                  return {
                    ...aMsg,
                    planApprovals: {
                      ...approvals,
                      [key]: {
                        ...(approvals[key] || {}),
                        status: resolvedStatus,
                      },
                    },
                  };
                })
              );
              pendingHistoryInterrupts.splice(idx, 1);
            }
          }

          // Resolve ask_user_question interrupts from resume query metadata (hitl_answers).
          // Persisted immediately by persist_query_start(), keyed by interrupt_id.
          {
            const hitlAnswers = event.metadata?.hitl_answers as Record<string, unknown> | undefined;
            if (hitlAnswers && pendingHistoryInterrupts.length > 0) {
              for (const [interruptId, answerValue] of Object.entries(hitlAnswers)) {
                const idx = pendingHistoryInterrupts.findIndex(
                  (p) => p.type === 'ask_user_question' && p.interruptId === interruptId
                );
                if (idx !== -1) {
                  const matched = pendingHistoryInterrupts[idx];
                  const resolvedStatus = answerValue !== null ? 'answered' : 'skipped';
                  const qKey = matched.questionId!;
                  setMessages((prev) =>
                    updateMessage(prev,matched.assistantMessageId, (msg) => {
                      if (msg.role !== 'assistant') return msg;
                      const aMsg = msg as AssistantMessage;
                      const questions = aMsg.userQuestions || {};
                      return {
                        ...aMsg,
                        userQuestions: {
                          ...questions,
                          [qKey]: {
                            ...(questions[qKey] || {}),
                            status: resolvedStatus,
                            answer: answerValue as string | null,
                          },
                        },
                      };
                    })
                  );
                  pendingHistoryInterrupts.splice(idx, 1);
                }
              }
            }
          }

          const pairIndex = event.turn_index!;
          const refs = {
            recentlySentTracker: recentlySentTrackerRef.current,
            currentMessageRef,
            newMessagesStartIndexRef,
            historyMessagesRef,
          };

          handleHistoryUserMessage({
            event: event as Record<string, unknown>,
            pairIndex,
            assistantMessagesByPair,
            pairStateByPair,
            refs,
            messages: messages as unknown as Record<string, unknown>[],
            setMessages: setMessagesForHandlers,
          });
          return;
        }

        // Handle message_chunk events (assistant messages)
        if (eventType === 'message_chunk' && hasRole && event.role === 'assistant' && hasPairIndex) {
          const pairIndex = event.turn_index!;
          const currentAssistantMessageId = assistantMessagesByPair.get(pairIndex);
          const pairState = pairStateByPair.get(pairIndex);

          if (!currentAssistantMessageId || !pairState) {
            console.warn('[History] Received message_chunk for unknown turn_index:', pairIndex);
            return;
          }

          // Process reasoning_signal
          if (contentType === 'reasoning_signal') {
            const signalContent = (event.content as string) || '';
            handleHistoryReasoningSignal({
              assistantMessageId: currentAssistantMessageId,
              signalContent,
              pairIndex,
              pairState,
              setMessages: setMessagesForHandlers,
              eventId: event._eventId as number | undefined,
            });
            return;
          }

          // Handle reasoning content
          if (contentType === 'reasoning' && event.content) {
            handleHistoryReasoningContent({
              assistantMessageId: currentAssistantMessageId,
              content: event.content as string,
              pairState,
              setMessages: setMessagesForHandlers,
            });
            return;
          }

          // Handle text content
          if (contentType === 'text' && event.content) {
            handleHistoryTextContent({
              assistantMessageId: currentAssistantMessageId,
              content: event.content as string,
              finishReason: event.finish_reason,
              pairState,
              setMessages: setMessagesForHandlers,
              eventId: event._eventId as number | undefined,
            });
            return;
          }

          // Handle finish_reason (end of assistant message)
          if (event.finish_reason) {
            setMessages((prev) =>
              updateMessage(prev,currentAssistantMessageId, (msg) => ({
                ...msg,
                isStreaming: false,
              }))
            );
            return;
          }
        }

        // Filter out tool_call_chunks events
        if (eventType === 'tool_call_chunks') {
          return;
        }

        // Handle artifact events (e.g., todo_update)
        // In history replay, artifacts DO have turn_index, so we can use it directly
        //
        // NOTE: `chart_annotation` artifacts are intentionally NOT replayed here
        // (only the live stream applies them to the annotation store). On reload,
        // MarketView's `useChartAnnotationSync` REST fetch is the authoritative
        // source that repopulates the store from Postgres, and the inline card
        // renders from the replayed `tool_call_result.artifact`. If a live chart
        // surface is ever added to the standalone chat page, add a
        // `chart_annotation` branch here so reloads stay consistent.
        if (eventType === 'artifact') {
          const artifactType = event.artifact_type;
          if (artifactType === 'todo_update') {
            const payload = event.payload || {};

            // Update floating todo card from history (last event wins, shows final state)
            if (updateTodoListCard) {
              updateTodoListCard({
                todos: Array.isArray(payload.todos) ? payload.todos : [],
                total: payload.total || 0,
                completed: payload.completed || 0,
                in_progress: payload.in_progress || 0,
                pending: payload.pending || 0,
              });
            }

            // Artifacts in history replay have turn_index - use it!
            if (hasPairIndex) {
              const pairIndex = event.turn_index!;
              // Update active pair tracking
              currentActivePairIndex = pairIndex;
              currentActivePairState = pairStateByPair.get(pairIndex);

              const currentAssistantMessageId = assistantMessagesByPair.get(pairIndex);
              const pairState = pairStateByPair.get(pairIndex);

              if (!currentAssistantMessageId || !pairState) {
                console.warn('[History] Received artifact for unknown turn_index:', pairIndex);
                return;
              }

              handleHistoryTodoUpdate({
                assistantMessageId: currentAssistantMessageId,
                artifactType: artifactType as string,
                artifactId: event.artifact_id as string,
                payload,
                pairState: pairState,
                setMessages: setMessagesForHandlers,
                eventId: event._eventId as number | undefined,
              });
            } else {
              // Fallback: artifacts without turn_index (shouldn't happen in history, but handle gracefully)
              console.warn('[History] Artifact without turn_index, using active pair fallback');
              let targetAssistantMessageId = null;
              let targetPairState = null;

              if (currentActivePairIndex !== null && currentActivePairState) {
                targetAssistantMessageId = assistantMessagesByPair.get(currentActivePairIndex);
                targetPairState = currentActivePairState;
              } else if (assistantMessagesByPair.size > 0) {
                const pairIndices = Array.from(assistantMessagesByPair.keys()).sort((a, b) => b - a);
                const lastPairIndex = pairIndices[0];
                targetAssistantMessageId = assistantMessagesByPair.get(lastPairIndex);
                targetPairState = pairStateByPair.get(lastPairIndex);
              }

              if (targetAssistantMessageId && targetPairState) {
                handleHistoryTodoUpdate({
                  assistantMessageId: targetAssistantMessageId,
                  artifactType: artifactType as string,
                  artifactId: event.artifact_id as string,
                  payload,
                  pairState: targetPairState,
                  setMessages: setMessagesForHandlers,
                  eventId: event._eventId as number | undefined,
                });
              }
            }
          }
          if (artifactType === 'html_widget') {
            const payload = (event.payload || {}) as unknown as HtmlWidgetData;

            if (hasPairIndex) {
              const pairIndex = event.turn_index!;
              currentActivePairIndex = pairIndex;
              currentActivePairState = pairStateByPair.get(pairIndex);

              const currentAssistantMessageId = assistantMessagesByPair.get(pairIndex);
              const pairState = pairStateByPair.get(pairIndex);

              if (currentAssistantMessageId && pairState) {
                handleHistoryHtmlWidget({
                  assistantMessageId: currentAssistantMessageId,
                  artifactType: artifactType as string,
                  artifactId: event.artifact_id as string,
                  payload: payload as HtmlWidgetData | null,
                  pairState,
                  setMessages: setMessagesForHandlers,
                  eventId: event._eventId as number | undefined,
                });
              }
            } else {
              let targetAssistantMessageId = null;
              let targetPairState = null;

              if (currentActivePairIndex !== null && currentActivePairState) {
                targetAssistantMessageId = assistantMessagesByPair.get(currentActivePairIndex);
                targetPairState = currentActivePairState;
              } else if (assistantMessagesByPair.size > 0) {
                const pairIndices = Array.from(assistantMessagesByPair.keys()).sort((a, b) => b - a);
                const lastPairIndex = pairIndices[0];
                targetAssistantMessageId = assistantMessagesByPair.get(lastPairIndex);
                targetPairState = pairStateByPair.get(lastPairIndex);
              }

              if (targetAssistantMessageId && targetPairState) {
                handleHistoryHtmlWidget({
                  assistantMessageId: targetAssistantMessageId,
                  artifactType: artifactType as string,
                  artifactId: event.artifact_id as string,
                  payload: payload as HtmlWidgetData | null,
                  pairState: targetPairState,
                  setMessages: setMessagesForHandlers,
                  eventId: event._eventId as number | undefined,
                });
              }
            }
          }
          if (artifactType === 'task') {
            const payload = event.payload || {};
            const task_id = payload.task_id as string | undefined;
            const rawAction = payload.action as string | undefined;
            const description = payload.description as string | undefined;
            const prompt = payload.prompt as string | undefined;
            const type = payload.type as string | undefined;
            const action = (() => { if (rawAction === 'spawned') return 'init'; if (rawAction === 'steering_accepted') return 'update'; if (rawAction === 'resumed') return 'resume'; return rawAction || 'init'; })();
            if (task_id) {
              const agentId = `task:${task_id}`;
              if (!subagentHistoryByTaskId.has(agentId)) {
                subagentHistoryByTaskId.set(agentId, {
                  messages: [],
                  events: [],
                  description: description || '',
                  prompt: prompt || description || '',
                  type: type || 'general-purpose',
                  resumePoints: [],
                });
              } else {
                const existing = subagentHistoryByTaskId.get(agentId)!;
                if (description && !existing.description) existing.description = description;
                if (prompt && !existing.prompt) existing.prompt = prompt || description || '';
                if (type && !existing.type) existing.type = type;
              }
              // Track resume boundaries for history replay
              if (action === 'resume') {
                const existing = subagentHistoryByTaskId.get(agentId);
                if (existing) {
                  existing.resumePoints = existing.resumePoints || [];
                  existing.resumePoints.push({
                    description: description || 'Resume',
                    turnIndex: event.turn_index,
                  });
                }
              }
              // Track steering_accepted actions for inline card "Updated" label
              if (action === 'update') {
                steeredAgentIds.add(agentId);
              }
              historyPendingTaskToolCallIdsRef.current = mapToolCallIdToAgentId(
                event.tool_call_id as string | undefined,
                agentId,
                action,
                historyPendingTaskToolCallIdsRef.current,
                toolCallIdToTaskIdMapRef.current,
              );
            }
          }
          return;
        }

        // Handle tool_calls events
        if (eventType === 'tool_calls' && hasPairIndex) {
          const pairIndex = event.turn_index!;
          // Update active pair tracking
          currentActivePairIndex = pairIndex;
          currentActivePairState = pairStateByPair.get(pairIndex);

          const currentAssistantMessageId = assistantMessagesByPair.get(pairIndex);
          const pairState = pairStateByPair.get(pairIndex);

          if (!currentAssistantMessageId || !pairState) {
            console.warn('[History] Received tool_calls for unknown turn_index:', pairIndex);
            return;
          }

          // Queue task tool call IDs for matching against artifact 'spawned' events
          // Skip follow-up/resume calls (task_id present) — they target existing subagents
          if (event.tool_calls) {
            const taskToolCalls = event.tool_calls.filter(
              (tc) => (tc.name === 'task' || tc.name === 'Task') && tc.id && !tc.args?.task_id
            );
            const toolCallIds = taskToolCalls.map((tc) => tc.id).filter(Boolean) as string[];
            if (toolCallIds.length > 0) {
              historyPendingTaskToolCallIdsRef.current = [
                ...historyPendingTaskToolCallIdsRef.current,
                ...toolCallIds,
              ];
            }
          }

          handleHistoryToolCalls({
            assistantMessageId: currentAssistantMessageId,
            toolCalls: (event.tool_calls || []) as unknown as Record<string, unknown>[],
            pairState,
            setMessages: setMessagesForHandlers,
            eventId: event._eventId as number | undefined,
          });
          return;
        }

        // Handle tool_call_result events
        if (eventType === 'tool_call_result' && hasPairIndex) {
          const pairIndex = event.turn_index!;
          // Update active pair tracking
          currentActivePairIndex = pairIndex;
          currentActivePairState = pairStateByPair.get(pairIndex);

          const currentAssistantMessageId = assistantMessagesByPair.get(pairIndex);
          const pairState = pairStateByPair.get(pairIndex);

          if (!currentAssistantMessageId || !pairState) {
            console.warn('[History] Received tool_call_result for unknown turn_index:', pairIndex);
            return;
          }

          // Build toolCallId → agentId mapping from Task tool artifact (preferred over order-based)
          const artifact = event.artifact as Record<string, unknown> | undefined;
          if (artifact?.task_id && event.tool_call_id) {
            const agentId = `task:${artifact.task_id}`;
            toolCallIdToTaskIdMapRef.current.set(event.tool_call_id, agentId);

            // Ensure subagentHistoryByTaskId has description from artifact.
            // Resume calls are filtered out of the tool_calls handler, so this
            // is the only place to pick up the description for resumed tasks.
            if (artifact.description) {
              const existing = subagentHistoryByTaskId.get(agentId);
              if (existing) {
                if (!existing.description) existing.description = artifact.description as string;
                if (!existing.prompt) existing.prompt = (artifact.prompt || artifact.description || '') as string;
              } else {
                subagentHistoryByTaskId.set(agentId, {
                  messages: [],
                  events: [],
                  description: artifact.description as string,
                  prompt: (artifact.prompt || artifact.description || '') as string,
                  type: (artifact.type || 'general-purpose') as string,
                  resumePoints: [],
                });
              }
            }
          }

          handleHistoryToolCallResult({
            assistantMessageId: currentAssistantMessageId,
            toolCallId: event.tool_call_id as string,
            result: {
              content: event.content,
              content_type: event.content_type,
              tool_call_id: event.tool_call_id,
              artifact: event.artifact,
            },
            pairState,
            setMessages: setMessagesForHandlers,
          });

          // Resolve pending ask_user_question interrupt from tool_call_result
          // (fallback for conversations where hitl_answers wasn't persisted)
          {
            const idx = pendingHistoryInterrupts.findIndex((p) => p.type === 'ask_user_question');
            if (idx !== -1 && typeof event.content === 'string' &&
                (event.content.startsWith('User answered:') || event.content.startsWith('User skipped'))) {
              const matched = pendingHistoryInterrupts[idx];
              const content = event.content;
              const isAnswered = content.startsWith('User answered:');
              const answerText = isAnswered ? content.replace('User answered: ', '') : null;
              const qKey = matched.questionId!;
              setMessages((prev) =>
                updateMessage(prev, matched.assistantMessageId, (msg) => {
                  if (msg.role !== 'assistant') return msg;
                  const aMsg = msg as AssistantMessage;
                  const questions = aMsg.userQuestions || {};
                  return {
                    ...aMsg,
                    userQuestions: {
                      ...questions,
                      [qKey]: {
                        ...(questions[qKey] || {}),
                        status: isAnswered ? 'answered' : 'skipped',
                        answer: answerText,
                      },
                    },
                  };
                })
              );
              pendingHistoryInterrupts.splice(idx, 1);
            }
          }

          // Resolve pending create_workspace, start_question, ptc_agent, or secretary action interrupt from tool_call_result
          {
            const idx = pendingHistoryInterrupts.findIndex((p) => PROPOSAL_INTERRUPT_TYPES.has(p.type));
            if (idx !== -1 && typeof event.content === 'string') {
              const matched = pendingHistoryInterrupts[idx];
              const content = event.content;
              const dataKey = PROPOSAL_DATA_KEY_MAP[matched.type] || 'questionProposals';

              let resolvedStatus = 'approved';
              let resultPayload: Record<string, unknown> | null = null;
              if (content.startsWith('User declined')) {
                resolvedStatus = 'rejected';
              } else {
                try {
                  const parsed = JSON.parse(content);
                  if (parsed?.success === false) resolvedStatus = 'rejected';
                  resultPayload = parsed;
                } catch { /* non-JSON → treat as approved */ }
              }

              const pKey = matched.proposalId!;
              // Extract thread_id/workspace_id from ptc_agent result for navigation
              const extraFields: Record<string, unknown> = {};
              if (matched.type === 'ptc_agent' && resultPayload) {
                if (resultPayload.thread_id) extraFields.thread_id = resultPayload.thread_id;
                if (resultPayload.workspace_id) extraFields.workspace_id = resultPayload.workspace_id;
              }
              setMessages((prev) =>
                updateMessage(prev,matched.assistantMessageId, (msg) => {
                  if (msg.role !== 'assistant') return msg;
                  const aMsg = msg as AssistantMessage;
                  const existing = ((aMsg as unknown as Record<string, unknown>)[dataKey] || {}) as Record<string, Record<string, unknown>>;
                  return {
                    ...aMsg,
                    [dataKey]: {
                      ...existing,
                      [pKey]: {
                        ...(existing[pKey] || {}),
                        status: resolvedStatus,
                        ...extraFields,
                      },
                    },
                  };
                })
              );
              pendingHistoryInterrupts.splice(idx, 1);
            }
          }

          return;
        }

        // Handle interrupt events during history replay
        if (eventType === 'interrupt') {
          const pairIndex = event.turn_index ?? currentActivePairIndex;
          const interruptAssistantId = pairIndex != null ? assistantMessagesByPair.get(pairIndex) : null;
          const pairState = pairIndex != null ? pairStateByPair.get(pairIndex) : null;

          if (interruptAssistantId && pairState) {
            const actionRequests = event.action_requests || [];
            const actionType = actionRequests[0]?.type as string | undefined;

            if (actionType === 'ask_user_question') {
              // --- User question interrupt (history) ---
              const questionId = event.interrupt_id || `question-history-${Date.now()}`;
              const questionData = actionRequests[0];
              const order = event._eventId != null ? Number(event._eventId) : ++pairState.contentOrderCounter;

              setMessages((prev) =>
                updateMessage(prev,interruptAssistantId, (m) => {
                  if (m.role !== 'assistant') return m;
                  const msg = m as AssistantMessage;
                  return {
                    ...msg,
                    contentSegments: [...(msg.contentSegments || []), { type: 'user_question' as const, questionId, order }],
                    userQuestions: {
                      ...(msg.userQuestions || {}),
                      [questionId]: {
                        question: questionData.question,
                        options: questionData.options || [],
                        allow_multiple: questionData.allow_multiple || false,
                        interruptId: event.interrupt_id,
                        status: 'pending',
                        answer: null,
                      },
                    },
                  };
                })
              );

              pendingHistoryInterrupts.push({
                type: 'ask_user_question',
                assistantMessageId: interruptAssistantId,
                questionId,
                interruptId: event.interrupt_id,
                answer: null,
              });
            } else if (actionType === 'create_workspace') {
              // --- Create workspace interrupt (history) ---
              const proposalId = event.interrupt_id || `workspace-history-${Date.now()}`;
              const proposalData = actionRequests[0];
              const order = event._eventId != null ? Number(event._eventId) : ++pairState.contentOrderCounter;

              setMessages((prev) =>
                updateMessage(prev,interruptAssistantId, (m) => {
                  if (m.role !== 'assistant') return m;
                  const msg = m as AssistantMessage;
                  return {
                    ...msg,
                    contentSegments: [...(msg.contentSegments || []), { type: 'create_workspace' as const, proposalId, order }],
                    workspaceProposals: {
                      ...(msg.workspaceProposals || {}),
                      [proposalId]: {
                        workspace_name: proposalData.workspace_name,
                        workspace_description: proposalData.workspace_description,
                        interruptId: event.interrupt_id,
                        status: 'pending',
                      },
                    },
                  };
                })
              );

              pendingHistoryInterrupts.push({
                type: 'create_workspace',
                assistantMessageId: interruptAssistantId,
                proposalId,
                interruptId: event.interrupt_id,
              });
            } else if (actionType === 'start_question') {
              // --- Start question interrupt (history) ---
              const proposalId = event.interrupt_id || `question-start-history-${Date.now()}`;
              const proposalData = actionRequests[0];
              const order = event._eventId != null ? Number(event._eventId) : ++pairState.contentOrderCounter;

              setMessages((prev) =>
                updateMessage(prev,interruptAssistantId, (m) => {
                  if (m.role !== 'assistant') return m;
                  const msg = m as AssistantMessage;
                  return {
                    ...msg,
                    contentSegments: [...(msg.contentSegments || []), { type: 'start_question' as const, proposalId, order }],
                    questionProposals: {
                      ...(msg.questionProposals || {}),
                      [proposalId]: {
                        workspace_id: proposalData.workspace_id,
                        question: proposalData.question,
                        interruptId: event.interrupt_id,
                        status: 'pending',
                      },
                    },
                  };
                })
              );

              pendingHistoryInterrupts.push({
                type: 'start_question',
                assistantMessageId: interruptAssistantId,
                proposalId,
                interruptId: event.interrupt_id,
              });
            } else if (actionType === 'ptc_agent') {
              // --- PTC agent interrupt (history) ---
              const proposalId = event.interrupt_id || `ptc-agent-history-${Date.now()}`;
              const proposalData = actionRequests[0];
              const order = event._eventId != null ? Number(event._eventId) : ++pairState.contentOrderCounter;

              setMessages((prev) =>
                updateMessage(prev,interruptAssistantId, (m) => {
                  if (m.role !== 'assistant') return m;
                  const msg = m as AssistantMessage;
                  return {
                    ...msg,
                    contentSegments: [...(msg.contentSegments || []), { type: 'ptc_agent' as const, proposalId, order }],
                    ptcAgentProposals: {
                      ...(msg.ptcAgentProposals || {}),
                      [proposalId]: {
                        workspace_id: proposalData.workspace_id,
                        workspace_name: proposalData.workspace_name,
                        question: proposalData.question,
                        report_back: proposalData.report_back ?? true,
                        interruptId: event.interrupt_id,
                        status: 'pending',
                      },
                    },
                  };
                })
              );

              pendingHistoryInterrupts.push({
                type: 'ptc_agent',
                assistantMessageId: interruptAssistantId,
                proposalId,
                interruptId: event.interrupt_id,
              });
            } else if (actionType === 'delete_workspace' || actionType === 'stop_workspace' || actionType === 'delete_thread') {
              // --- Secretary action interrupt (history) ---
              const proposalId = event.interrupt_id || `secretary-${actionType}-history-${Date.now()}`;
              const proposalData = actionRequests[0];
              const order = event._eventId != null ? Number(event._eventId) : ++pairState.contentOrderCounter;

              setMessages((prev) =>
                updateMessage(prev,interruptAssistantId, (m) => {
                  if (m.role !== 'assistant') return m;
                  const msg = m as AssistantMessage;
                  return {
                    ...msg,
                    contentSegments: [...(msg.contentSegments || []), { type: actionType as 'delete_workspace' | 'stop_workspace' | 'delete_thread', proposalId, order }],
                    secretaryActionProposals: {
                      ...(msg.secretaryActionProposals || {}),
                      [proposalId]: {
                        actionType: actionType as 'delete_workspace' | 'stop_workspace' | 'delete_thread',
                        workspace_id: proposalData.workspace_id,
                        thread_id: proposalData.thread_id,
                        interruptId: event.interrupt_id,
                        status: 'pending',
                      },
                    },
                  };
                })
              );

              pendingHistoryInterrupts.push({
                type: actionType,
                assistantMessageId: interruptAssistantId,
                proposalId,
                interruptId: event.interrupt_id,
              });
            } else {
              // --- Plan approval interrupt (existing) ---
              const planApprovalId = event.interrupt_id || `plan-history-${Date.now()}`;
              const description =
                (actionRequests[0]?.description as string) ||
                (actionRequests[0]?.args?.plan as string) ||
                'No plan description provided.';
              const order = event._eventId != null ? Number(event._eventId) : ++pairState.contentOrderCounter;

              setMessages((prev) =>
                updateMessage(prev,interruptAssistantId, (m) => {
                  if (m.role !== 'assistant') return m;
                  const msg = m as AssistantMessage;
                  return {
                    ...msg,
                    contentSegments: [...(msg.contentSegments || []), { type: 'plan_approval' as const, planApprovalId, order }],
                    planApprovals: {
                      ...(msg.planApprovals || {}),
                      [planApprovalId]: {
                        description,
                        interruptId: event.interrupt_id,
                        status: 'pending',
                      },
                    },
                  };
                })
              );

              pendingHistoryInterrupts.push({
                type: 'plan_approval',
                assistantMessageId: interruptAssistantId,
                planApprovalId,
                interruptId: event.interrupt_id,
              });
            }
          }
          return;
        }

        // Handle replay_done event (final event)
        if (eventType === 'replay_done') {
          if (event.thread_id && event.thread_id !== threadId && event.thread_id !== '__default__') {
            setThreadId(event.thread_id);
            setStoredThreadId(workspaceId, event.thread_id);
          }
        } else if (eventType === 'credit_usage') {
          // credit_usage indicates the end of one conversation pair — no-op boundary marker
        } else if (!eventType) {
          // Fallback: Handle events without event type
          if (event.thread_id && !hasRole && !contentType) {
            if (event.thread_id !== threadId && event.thread_id !== '__default__') {
              setThreadId(event.thread_id);
              setStoredThreadId(workspaceId, event.thread_id);
            }
          }
        }
      });

        // If there's still a pending interrupt after replay (no subsequent user_message
        // resolved it), store it in a ref. loadAndMaybeReconnect will decide whether to
        // make it interactive (workflow paused) or reconnect to get resolution events
        // (workflow active = interrupt was answered but resolution is in Redis buffer).
        if (pendingHistoryInterrupts.length > 0) {
          historyHasUnresolvedInterruptRef.current = true;
          unresolvedHistoryInterruptRef.current = pendingHistoryInterrupts.map((p) => ({ ...p }));
          pendingHistoryInterrupts.length = 0;
        }

        // Process stored subagent events and build their messages
        // NOTE: During history replay we DO NOT open floating cards automatically.
        // We only build per-task message history here; cards are created lazily
        // when the user clicks \"Open subagent details\" in the main chat view.
        if (subagentHistoryByTaskId.size > 0) {
          // Process each subagent's events
          for (const [taskId, subagentHistory] of subagentHistoryByTaskId.entries()) {
            // Create temporary refs structure for processing
            let currentRunIndex = 0;
            // Per-task token-usage accumulator: backend emits per-call deltas
            // and we sum them into a running total before storing on the
            // SubagentHistoryEntry below.
            let tempTokenUsage: SubagentTokenUsage = ZERO_USAGE;
            const tempSubagentStateRefs: Record<string, TaskRefs> = {
              [taskId]: {
                contentOrderCounterRef: { current: 0 },
                currentReasoningIdRef: { current: null },
                currentToolCallIdRef: { current: null },
                messages: [] as Record<string, unknown>[],
                runIndex: 0,
              },
            };

            // tempRefs matches StreamProcessorRefs; tempSubagentStateRefs is already Record<string, TaskRefs>
            const tempRefs: StreamProcessorRefs = {
              contentOrderCounterRef: { current: 0 },
              currentReasoningIdRef: { current: null },
              currentToolCallIdRef: { current: null },
              subagentStateRefs: tempSubagentStateRefs,
              isReconnect: true, // Suppress Date.now() timestamps so items go straight to accordion zone
            };

            // History-specific no-op updater: prevents floating cards from being
            // created during history load while still letting handlers build
            // the in-memory message structures in tempSubagentStateRefs.
            const historyUpdateSubagentCard = () => {};

            // Pre-compute resume boundary turn indices from stored resumePoints
            const resumePoints = subagentHistory.resumePoints || [];
            const resumeByTurnIndex = new Map();
            for (const rp of resumePoints) {
              if (rp.turnIndex != null) {
                resumeByTurnIndex.set(rp.turnIndex, rp);
              }
            }
            let lastTurnIndex = null;

            // Process each event in chronological order
            for (let i = 0; i < subagentHistory.events.length; i++) {
              const event = subagentHistory.events[i];
              const eventType = event.event;
              const contentType = event.content_type;

              // Side channel for compaction-middleware LLM output; drop so
              // it does not mingle with the subagent's own messages.
              if (eventType === 'compaction_chunk') {
                continue;
              }

              // Detect resume boundary: turn_index transitions to a resume turn
              const eventTurnIndex = event.turn_index;
              if (eventTurnIndex != null && eventTurnIndex !== lastTurnIndex && resumeByTurnIndex.has(eventTurnIndex)) {
                const resumePoint = resumeByTurnIndex.get(eventTurnIndex);
                const taskRefsLocal = tempSubagentStateRefs[taskId];

                // Finalize the previous run's last assistant message
                for (let j = taskRefsLocal.messages.length - 1; j >= 0; j--) {
                  const taskMsg = taskRefsLocal.messages[j];
                  if (taskMsg.role === 'assistant' && taskMsg.isStreaming) {
                    taskRefsLocal.messages[j] = { ...taskMsg, isStreaming: false };
                    break;
                  }
                }

                // Inject user message with resume instruction
                taskRefsLocal.messages.push({
                  id: `resume-${taskId}-${currentRunIndex + 1}`,
                  role: 'user',
                  content: resumePoint.description || 'Resume',
                  contentSegments: [{ type: 'text', content: resumePoint.description || 'Resume', order: 0 }],
                  reasoningProcesses: {},
                  toolCallProcesses: {},
                });

                // Bump run index and reset per-run counters
                currentRunIndex++;
                taskRefsLocal.runIndex = currentRunIndex;
                taskRefsLocal.contentOrderCounterRef.current = 0;
                taskRefsLocal.currentReasoningIdRef.current = null;
                taskRefsLocal.currentToolCallIdRef.current = null;
              }
              if (eventTurnIndex != null) {
                lastTurnIndex = eventTurnIndex;
              }

              // Use per-run assistant message ID
              const assistantMessageId = `subagent-${taskId}-assistant-${currentRunIndex}`;

              if (eventType === 'message_chunk' && event.role === 'assistant') {
                handleSubagentMessageChunk({
                  taskId,
                  assistantMessageId,
                  contentType: contentType as string,
                  content: event.content as string,
                  finishReason: event.finish_reason,
                  refs: tempRefs,
                  updateSubagentCard: historyUpdateSubagentCard,
                });
              } else if (eventType === 'tool_calls' && event.tool_calls) {
                handleSubagentToolCalls({
                  taskId,
                  assistantMessageId,
                  toolCalls: event.tool_calls as unknown as Record<string, unknown>[],
                  refs: tempRefs,
                  updateSubagentCard: historyUpdateSubagentCard,
                });
              } else if (eventType === 'tool_call_result') {
                handleSubagentToolCallResult({
                  taskId,
                  assistantMessageId,
                  toolCallId: event.tool_call_id as string,
                  result: {
                    content: event.content,
                    content_type: event.content_type,
                    tool_call_id: event.tool_call_id,
                    artifact: event.artifact,
                  },
                  refs: tempRefs,
                  updateSubagentCard: historyUpdateSubagentCard,
                });
              } else if (eventType === 'subagent_followup_injected' || eventType === 'turn_start') {
                // Legacy subagent_followup_injected had content (steering user message).
                // turn_start was an inter-model-call boundary — no longer emitted,
                // but old persisted data may still contain it. Just extract content.
                if (event.content) {
                  handleTaskSteeringAccepted({
                    taskId,
                    content: event.content as string,
                    refs: tempRefs,
                    updateSubagentCard: historyUpdateSubagentCard,
                  });
                  // Sync local run index — handleTaskSteeringAccepted bumps runIndex
                  currentRunIndex = tempSubagentStateRefs[taskId].runIndex;
                }
              } else if (eventType === 'steering_delivered') {
                if (event.content) {
                  handleTaskSteeringAccepted({
                    taskId,
                    content: event.content as string,
                    refs: tempRefs,
                    updateSubagentCard: historyUpdateSubagentCard,
                  });
                  // Sync local run index — handleTaskSteeringAccepted bumps runIndex
                  currentRunIndex = tempSubagentStateRefs[taskId].runIndex;
                }
              } else if (eventType === 'context_window') {
                // Embed notification as content segment in the assistant message
                const action = event.action;
                if (action === 'token_usage') {
                  tempTokenUsage = accumulateTokenUsage(tempTokenUsage, extractTokenUsageDelta(event));
                } else {
                  let text;
                  let detail: string | undefined;
                  if (action === 'summarize' && event.signal === 'complete') {
                    text = t('chat.compactedNotification', { from: event.original_message_count });
                    detail = (event.summary_text as string | undefined) || undefined;
                  } else if (action === 'offload' && event.signal === 'complete') {
                    const args = event.offloaded_args || 0;
                    const reads = event.offloaded_reads || 0;
                    if (args > 0 && reads > 0) text = t('chat.offloadedNotification', { args, reads });
                    else if (reads > 0) text = t('chat.offloadedReadsNotification', { count: reads });
                    else if (args > 0) text = t('chat.offloadedArgsNotification', { count: args });
                  }
                  if (text) {
                    const taskRefsLocal = tempSubagentStateRefs[taskId];
                    const order = ++taskRefsLocal.contentOrderCounterRef.current;
                    // Find the last assistant message and append notification segment
                    const msgIdx = taskRefsLocal.messages.findLastIndex((m) => m.role === 'assistant');
                    if (msgIdx !== -1) {
                      const taskMsg = taskRefsLocal.messages[msgIdx];
                      if (taskMsg.role === 'assistant') {
                        const aMsg = taskMsg as unknown as AssistantMessage;
                        taskRefsLocal.messages[msgIdx] = { ...aMsg, contentSegments: [...(aMsg.contentSegments || []), { type: 'notification' as const, content: text, order, detail }] } as unknown as Record<string, unknown>;
                      }
                    }
                  }
                }
              } else {
                console.warn('[History] Unhandled subagent event type:', eventType);
              }
            }
            
            // Get final messages from temp refs
            const rawMessages = tempSubagentStateRefs[taskId]?.messages || [];

            // Finalize messages: set isStreaming=false and close open reasoning/tool
            // processes on the last assistant message so SubagentStatusBar shows 'completed'.
            const finalMessages = rawMessages.map((msg) => {
              if (msg.role !== 'assistant') return msg;
              const aMsg = msg as unknown as AssistantMessage;
              // Only finalize the last assistant message (or all, to be safe)
              const m = { ...aMsg, isStreaming: false as const };
              if (m.toolCallProcesses) {
                const procs = { ...m.toolCallProcesses };
                for (const [id, proc] of Object.entries(procs)) {
                  if (proc.isInProgress) {
                    procs[id] = { ...proc, isInProgress: false, isComplete: true };
                  }
                }
                m.toolCallProcesses = procs;
              }
              if (m.reasoningProcesses) {
                const rps = { ...m.reasoningProcesses };
                for (const [id, rp] of Object.entries(rps)) {
                  if (rp.isReasoning) {
                    rps[id] = { ...rp, isReasoning: false, reasoningComplete: true };
                  }
                }
                m.reasoningProcesses = rps;
              }
              return m;
            });

            // Get task metadata from stored history
            const taskMetadata = subagentHistoryByTaskId.get(taskId);

            // Store history in ref so it can be used when the user explicitly
            // opens the subagent card from the main chat view. We do NOT
            // create the floating card here.
            if (!subagentHistoryRef.current) {
              subagentHistoryRef.current = {};
            }
            subagentHistoryRef.current[taskId] = {
              taskId,
              description: taskMetadata?.description || '',
              prompt: taskMetadata?.prompt || taskMetadata?.description || '',
              type: taskMetadata?.type || 'general-purpose',
              messages: finalMessages,
              status: 'completed', // History events are always completed
              toolCalls: countToolCalls(finalMessages),
              tokenUsage: tempTokenUsage,
              currentTool: '',
            };

            // Seed persistent subagent state refs from history so that
            // reconnect or future resume can append to the existing messages.
            subagentStateRefsRef.current[taskId] = {
              contentOrderCounterRef: { current: tempSubagentStateRefs[taskId].contentOrderCounterRef.current },
              currentReasoningIdRef: { current: null },
              currentToolCallIdRef: { current: null },
              messages: finalMessages,
              runIndex: currentRunIndex,
            };
          }
        }
      } catch (replayError: unknown) {
        // Handle 404 gracefully - it's expected for brand new threads that haven't been fully initialized yet
        if ((replayError as Error).message && (replayError as Error).message.includes('404')) {
          // Thread not found (404) is expected for brand-new threads — skip silently.
          // Don't set error message for 404 - it's expected for new threads
        } else {
          throw replayError; // Re-throw other errors
        }
      }

      // NOTE: markAllSubagentTasksCompleted() is NOT called here because
      // loadAndMaybeReconnect will call it after determining whether the
      // workflow is still active (reconnect case) or truly completed.

      // Post-process: update inline cards for steering_accepted actions to show "Updated"
      if (steeredAgentIds.size > 0) {
        setMessages(prev => prev.map(msg => {
          if (msg.role !== 'assistant') return msg;
          const aMsg = msg as AssistantMessage;
          if (!aMsg.subagentTasks) return msg;
          let changed = false;
          const newTasks = { ...aMsg.subagentTasks };
          for (const [tcId, task] of Object.entries(newTasks)) {
            if (task.resumeTargetId && steeredAgentIds.has(task.resumeTargetId) && task.action === 'resume') {
              newTasks[tcId] = { ...task, action: 'update' };
              changed = true;
            }
          }
          return changed ? { ...aMsg, subagentTasks: newTasks } : msg;
        }));
      }

      setIsLoadingHistory(false);
      historyLoadingRef.current = false;

      // Fetch feedback state for the thread
      if (threadId) {
        try {
          const feedbackList = await getThreadFeedback(threadId);
          const map: Record<number, { rating: string | null; [key: string]: unknown }> = {};
          feedbackList.forEach((fb: Record<string, unknown>) => { map[fb.turn_index as number] = fb as { rating: string | null; [key: string]: unknown }; });
          feedbackMapRef.current = map;
        } catch (e) {
          // Non-critical — feedback display is best-effort
          console.warn('[History] Failed to load feedback:', e);
        }
      }
      return true;
    } catch (error: unknown) {
      console.error('[History] Error loading conversation history:', error);
      // Only show error if it's not a 404 (404 is expected for new threads).
      // 404 still counts as a successful "no prior history" load — caller can
      // safely mark the idempotency key.
      const errMsg = (error as Error).message || '';
      const isNotFound = errMsg.includes('404');
      if (errMsg && !isNotFound) {
        setMessageError(errMsg || 'Failed to load conversation history');
      }
      setIsLoadingHistory(false);
      historyLoadingRef.current = false;
      return isNotFound;
    }
  };

  /**
   * Reconnects to an in-progress workflow stream after page refresh.
   * Creates an assistant message placeholder and processes live SSE events.
   */
  const reconnectToStream = async ({ activeTasks = [], runId, resetCursor = false }: { activeTasks?: string[]; runId?: string | null; resetCursor?: boolean } = {}) => {
    if (!threadId || threadId === '__default__') return;

    // Callers that (re)attach to the thread's CURRENT active run — thread-load,
    // cross-thread navigation, post-HITL resume, report-back — pass that run's
    // id and rewind the cursor. Without this, currentRunIdRef/lastEventIdRef
    // still point at the PRIOR thread's stream, so we attach to a dead key
    // (zero live events → content only appears on a later refetch). The
    // mid-stream disconnect path omits both and keeps its in-progress cursor.
    if (runId !== undefined) currentRunIdRef.current = runId;
    if (resetCursor) lastEventIdRef.current = null;

    console.log('[Reconnect] Starting reconnection for thread:', threadId);

    // Clear subagent cards to prevent duplicate content from cache + Redis overlap
    if (clearSubagentCards) {
      clearSubagentCards();
    }
    completedTaskIdsRef.current.clear();

    setIsLoading(true);
    setIsReconnecting(true);
    acquireStreamOwnership(threadId);
    // Fresh stream: clear any stale stop flag from a PRIOR turn (e.g. user
    // hard-stopped thread A, then switched to live thread B). Without this the
    // stop-during-reconnect guards below (result.aborted || wasStoppedRef) would
    // bail this legitimate reconnect and leave isLoading stuck. Matches the reset
    // every other stream entry point does (handleSendMessage, resume, steering).
    wasStoppedRef.current = false;

    // Create assistant message placeholder for reconnection
    const assistantMessageId = `assistant-reconnect-${Date.now()}`;
    contentOrderCounterRef.current = 0;
    currentReasoningIdRef.current = null;
    currentToolCallIdRef.current = null;

    // Strip interrupt segments populated by history replay before the reconnect stream
    // re-delivers them; otherwise the same question/proposal renders twice (once on the
    // history bubble, once on the reconnect bubble). The reconnect stream is
    // authoritative for live interrupt state. Also redirect any unresolved-interrupt
    // refs to point at the new reconnect bubble so the tool_call_result history-resolver
    // writes resolution status to where the proposal actually lives now.
    const stripList = unresolvedHistoryInterruptRef.current;
    if (stripList.length > 0) {
      const stripsByMsgId = new Map<string, HistoryInterruptInfo[]>();
      for (const info of stripList) {
        const arr = stripsByMsgId.get(info.assistantMessageId) || [];
        arr.push(info);
        stripsByMsgId.set(info.assistantMessageId, arr);
      }
      setMessages((prev) =>
        prev.map((m) => {
          if (m.role !== 'assistant') return m;
          const strips = stripsByMsgId.get(m.id);
          if (!strips) return m;
          const msg = m as AssistantMessage;
          const stripQuestionIds = new Set(
            strips.filter((s) => s.type === 'ask_user_question' && s.questionId).map((s) => s.questionId!),
          );
          const stripProposalIds = new Set(strips.filter((s) => s.proposalId).map((s) => s.proposalId!));
          const stripPlanApprovalIds = new Set(
            strips.filter((s) => s.type === 'plan_approval' && s.planApprovalId).map((s) => s.planApprovalId!),
          );
          const newSegments = (msg.contentSegments || []).filter((seg) => {
            if (seg.type === 'user_question') return !stripQuestionIds.has(seg.questionId);
            if (
              seg.type === 'create_workspace' ||
              seg.type === 'start_question' ||
              seg.type === 'ptc_agent' ||
              seg.type === 'delete_workspace' ||
              seg.type === 'stop_workspace' ||
              seg.type === 'delete_thread'
            ) {
              return !stripProposalIds.has(seg.proposalId);
            }
            if (seg.type === 'plan_approval') return !stripPlanApprovalIds.has(seg.planApprovalId);
            return true;
          });
          const next: AssistantMessage = { ...msg, contentSegments: newSegments };
          if (stripQuestionIds.size > 0 && msg.userQuestions) {
            const map = { ...msg.userQuestions };
            for (const qid of stripQuestionIds) delete map[qid];
            next.userQuestions = map;
          }
          if (stripProposalIds.size > 0) {
            for (const key of ['workspaceProposals', 'questionProposals', 'ptcAgentProposals', 'secretaryActionProposals'] as const) {
              const bucket = msg[key];
              if (!bucket) continue;
              const map = { ...bucket };
              for (const pid of stripProposalIds) delete (map as Record<string, unknown>)[pid];
              (next as unknown as Record<string, unknown>)[key] = map;
            }
          }
          if (stripPlanApprovalIds.size > 0 && msg.planApprovals) {
            const map = { ...msg.planApprovals };
            for (const pid of stripPlanApprovalIds) delete map[pid];
            next.planApprovals = map;
          }
          return next;
        }),
      );
      // Redirect refs so the tool_call_result history-resolver targets the new bubble.
      for (const info of stripList) info.assistantMessageId = assistantMessageId;
    }

    {
      const assistantMessage = createAssistantMessage(assistantMessageId);
      // Replace trailing empty history assistant message (created by history replay for the
      // in-progress pair) to avoid a duplicate bubble. If the last message is a non-empty
      // history assistant or something else, just append normally.
      setMessages((prev) => {
        if (prev.length > 0) {
          const lastMsg = prev[prev.length - 1];
          if (
            lastMsg.role === 'assistant' &&
            (lastMsg as AssistantMessage).isHistory &&
            (!(lastMsg as AssistantMessage).contentSegments || (lastMsg as AssistantMessage).contentSegments.length === 0) &&
            !lastMsg.content
          ) {
            return [...prev.slice(0, -1), assistantMessage];
          }
        }
        return appendMessage(prev,assistantMessage);
      });
      currentMessageRef.current = assistantMessageId;
    }

    // Prepare refs for event handlers — use persistent subagent state
    const refs = {
      contentOrderCounterRef,
      currentReasoningIdRef,
      currentToolCallIdRef,
      steeringAtOrderRef,
      updateTodoListCard: updateTodoListCard || undefined,
      isNewConversation: false,
      subagentStateRefs: subagentStateRefsRef.current,
      updateSubagentCard: updateSubagentCard
        ? (agentId: string, data: Record<string, unknown>) => updateSubagentCard(agentId, { ...data, isReconnect: true })
        : (() => {}),
      isReconnect: true,
      unresolvedHistoryInterruptRef,
    };

    const wasInterruptedRef = { current: false };
    const processEvent = createStreamEventProcessor(assistantMessageId, refs, getTaskIdFromEvent, wasInterruptedRef);

    // Register the reconnect reader on mainStreamAbortRef so a user stop aborts
    // it. Without this, stopWorkflow's abort is a no-op during a reconnect: the
    // reader keeps consuming SSE + mutating messages and the finally below
    // re-runs cleanup (re-toggling isLoading / re-opening report-back) after the
    // stop already tore everything down.
    const abortController = new AbortController();
    mainStreamAbortRef.current = abortController;
    // This reconnect now owns the spinner set above (setIsReconnecting(true)).
    isReconnectingOwnerRef.current = abortController;

    try {
      // Replay buffered events first — this processes artifact{task,spawned} events
      // which create subagent cards with the correct description/type. Per-task streams
      // are opened AFTER so they merge into existing cards instead of creating empty ones.
      const result = await reconnectToWorkflowStream(
        threadId,
        currentRunIdRef.current,
        lastEventIdRef.current as number | null,
        processEvent,
        abortController.signal,
      );
      // User stop aborted the reader — stopWorkflow owns teardown; bail.
      if (result?.aborted || wasStoppedRef.current) {
        return;
      }
      if (result?.disconnected) {
        throw new Error('Reconnection stream disconnected');
      }

      // Mark message as complete
      setMessages((prev) =>
        updateMessage(prev,assistantMessageId, (msg) => ({
          ...msg,
          isStreaming: false,
        }))
      );

      // Pre-seed subagent cards from history for tasks whose artifact events were
      // cleared from the Redis buffer after the spawning turn persisted to DB.
      // This mirrors the Scenario B pre-seed at lines 1611-1626.
      if (activeTasks.length > 0 && updateSubagentCard && subagentHistoryRef.current) {
        for (const taskId of activeTasks) {
          const agentId = `task:${taskId}`;
          const historyData = subagentHistoryRef.current[agentId];
          if (historyData) {
            // Seed the live token-usage ref from history so subsequent live
            // deltas accumulate on top of the historical total instead of
            // starting from zero.
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
      }

      // Now open per-task SSE streams for active subagents. Per-task endpoints
      // replay from their own Redis buffer so no events are lost.
      if (activeTasks.length > 0) {
        console.log('[Reconnect] Opening per-task streams for active tasks:', activeTasks);
        for (const taskId of activeTasks) {
          openSubagentStream(threadId, taskId, processEvent);
        }
      }
    } catch (err: unknown) {
      // User stop aborted the reader — stopWorkflow owns teardown; bail before
      // surfacing this as a reconnect error.
      if ((err as Error)?.name === 'AbortError' || wasStoppedRef.current) {
        return;
      }
      // 404/410 = workflow no longer available, not a real error
      const status = (err as Error).message?.match(/status:\s*(\d+)/)?.[1];
      if (status === '404' || status === '410') {
        console.log('[Reconnect] Workflow no longer available (', status, '), cleaning up');
      } else {
        console.error('[Reconnect] Error during reconnection:', err);
        setMessageError((err as Error).message || 'Failed to reconnect to stream');
      }
    } finally {
      // Clean up empty reconnect messages (no content segments = nothing was
      // streamed). Skip on a user stop: finalizeStreamingMessage just stamped
      // this bubble `stopped: true` (with the "⏹ Stopped" chip) but left it
      // content-empty, so removing it here would erase the stop marker.
      setMessages((prev) => {
        if (wasStoppedRef.current) return prev;
        const msg = prev.find((m) => m.id === assistantMessageId);
        if (msg && msg.role === 'assistant' && (!(msg as AssistantMessage).contentSegments || (msg as AssistantMessage).contentSegments.length === 0) && !msg.content) {
          return prev.filter((m) => m.id !== assistantMessageId);
        }
        return prev;
      });

      // Only finalize if this reconnect is still the active stream. Navigation
      // can supersede it (the thread-load effect aborts a report-back stream on
      // the prior thread and starts a fresh stream for the new thread); running
      // cleanup here would then reset isStreamingRef / re-arm watches and clobber
      // the new thread's stream. The owner that superseded us manages that state.
      const stillActive = mainStreamAbortRef.current === abortController;
      // Skip cleanup on a user stop too — stopWorkflow already cleared isLoading /
      // hasActiveSubagents and ran finalize; re-running it here would re-toggle
      // loading and re-open the report-back watch after the stop.
      if (stillActive && !wasInterruptedRef.current && !wasStoppedRef.current) {
        cleanupAfterStreamEnd(assistantMessageId);
      }
      // Clear the spinner only if THIS reconnect still owns it. A newer reconnect
      // (cross-thread nav) took ownership and manages its own spinner — clobbering
      // it would hide the new thread's reconnect. Any other teardown that swapped
      // the stream out from under us (user stop, steering demoted to a new turn)
      // leaves ownership with us, so we still clear and never strand the spinner.
      if (isReconnectingOwnerRef.current === abortController) {
        setIsReconnecting(false);
        isReconnectingOwnerRef.current = null;
      }
      if (stillActive) {
        mainStreamAbortRef.current = null;
      }
    }
  };

  /**
   * Attempts to auto-reconnect after a mid-stream network disconnect.
   * Uses exponential backoff (1s, 2s, 4s, 8s, 16s) with up to 5 retries.
   * Falls back to cleanupAfterStreamEnd if workflow completes or retries exhaust.
   */
  const attemptReconnectAfterDisconnect = async (assistantMessageId: string) => {
    const MAX_RETRIES = 5;
    const BASE_DELAY = 1000;

    setIsReconnecting(true);

    for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
      if (!threadId || threadId === '__default__') break;

      if (attempt > 0) {
        await new Promise((r) => setTimeout(r, BASE_DELAY * Math.pow(2, attempt - 1)));
      }

      try {
        const status = await getWorkflowStatus(threadId);
        if (!status.can_reconnect) {
          console.log('[Reconnect] Workflow no longer reconnectable, cleaning up');
          break;
        }

        console.log('[Reconnect] Attempt', attempt + 1, 'of', MAX_RETRIES);
        await reconnectToStream({ activeTasks: status.active_tasks || [] });

        setIsReconnecting(false);
        return;
      } catch (err: unknown) {
        console.warn('[Reconnect] Attempt', attempt + 1, 'failed:', (err as Error).message);
      }
    }

    setIsReconnecting(false);
    cleanupAfterStreamEnd(assistantMessageId);
    // Reload conversation to show complete response after failed reconnection
    setReloadTrigger((n) => n + 1);
  };

  // --- Report-back watch helpers ---
  // After PTC agent dispatch, the backend fires a report-back flash workflow
  // once the PTC analysis completes. We open a lightweight SSE watch connection
  // that receives a push notification (via Redis pub/sub) when this happens.

  const stopReportBackWatch = () => {
    // Invalidate the current watch generation so any in-flight wake/poll callback
    // bails instead of attaching after teardown.
    reportBackWatchEpochRef.current += 1;
    reportBackWatchThreadIdRef.current = null;
    reportBackRunIdRef.current = null;
    if (reportBackWatchAbortRef.current) {
      reportBackWatchAbortRef.current.abort();
      reportBackWatchAbortRef.current = null;
    }
    if (reportBackPollRef.current) {
      clearInterval(reportBackPollRef.current);
      reportBackPollRef.current = null;
    }
  };

  // Drive the flash thread to the PTC report-back turn once the PTC completes.
  //
  // The watch is KEYED to the flash thread (`flashThreadId`), not the visible
  // thread, and persists across navigation. When the user jumps into the
  // dispatched PTC thread, the watch keeps running but does NOT render — it only
  // attaches when its flash thread is the one on screen and idle. That is how the
  // flash report-back and a live PTC stream coexist: the watch holds the named
  // run id (captured from a wake even while away) and streams it the moment the
  // user returns to the flash thread.
  //
  // The backend names the report-back run explicitly — no inference from run_id
  // changes. It signals readiness via a fire-and-forget Redis pub/sub wake
  // (`thread:wake:{tid}`) carrying the run_id, and durably records that run_id so
  // it's also readable from `/status` (`report_back_run_id`). An in-session
  // client attaches straight from the wake's run_id; a client that missed the
  // wake (slow connect or a full reload) discovers the run via a `/status` poll.
  const startReportBackWatch = (flashThreadId?: string | null) => {
    const tid = flashThreadId ?? threadIdRef.current;
    if (!tid || tid === '__default__') return;

    // Idempotent: a watch for this flash thread is already running. It survives
    // navigation, so re-arming on return (loadAndMaybeReconnect / stream-end)
    // must NOT tear it down — that would drop the run id it has already captured.
    if (reportBackWatchAbortRef.current && reportBackWatchThreadIdRef.current === tid) return;

    stopReportBackWatch();
    reportBackWatchThreadIdRef.current = tid;

    // This watch generation. If a teardown bumps the epoch (consume, hard-stop,
    // re-arm for a different flash thread, unmount), a wake/poll callback captured
    // here is stale and must not attach.
    const epoch = reportBackWatchEpochRef.current;

    let consumed = false;
    let inFlight = false;
    let polls = 0;

    // Attach to the named report-back run's per-run stream — which still replays
    // its buffered events even after completion — so the summary streams into a
    // fresh bubble naturally, with no jarring full-history reload (a reload also
    // duplicates the live dispatch card, since history replay re-adds it next to
    // the surviving live bubble). Skip if the named run is the one already on
    // screen (the dispatch turn, or a report-back run we already attached to).
    const attach = async (runId: string | null, activeTasks: string[]) => {
      if (!runId || runId === currentRunIdRef.current) return false;
      consumed = true;
      awaitingReportBackRef.current = false;
      reportBackRunIdRef.current = null;
      stopReportBackWatch();
      await reconnectToStream({ activeTasks, runId, resetCursor: true });
      return true;
    };

    const reconcile = async (source: string, wakeRunId?: string | null) => {
      // Stale generation (re-armed for another thread / hard-stopped / unmounted).
      if (reportBackWatchEpochRef.current !== epoch) return;
      if (consumed) return;

      // Remember the named run as soon as we learn it — even if the flash thread
      // is NOT the one on screen right now. A wake fires once, when the PTC run
      // finishes; if the user is away on the PTC thread at that instant we still
      // capture the id so the report-back streams on return.
      if (wakeRunId) reportBackRunIdRef.current = wakeRunId;

      // Render gate: only attach when this flash thread is the visible thread and
      // no other stream owns the slot. Off-thread (e.g. the user jumped into the
      // PTC thread) we just hold the run id and leave the PTC stream untouched.
      if (threadIdRef.current !== tid || isStreamingRef.current || inFlight) return;

      // On the flash thread and idle. Attach a known run id immediately (no
      // /status round-trip needed — the wake or a prior poll already named it).
      if (reportBackRunIdRef.current) {
        await attach(reportBackRunIdRef.current, []);
        return;
      }

      inFlight = true;
      let status: WorkflowStatusResponse;
      try {
        status = await getWorkflowStatus(tid);
      } catch {
        inFlight = false;
        return;
      }
      inFlight = false;
      // Re-check generation/visibility after the await: the watch may have been
      // torn down, or the user may have navigated away, while /status was in flight.
      if (consumed || reportBackWatchEpochRef.current !== epoch) return;
      if (threadIdRef.current !== tid || isStreamingRef.current) return;

      if (status.report_back_run_id) reportBackRunIdRef.current = status.report_back_run_id;
      if (await attach(reportBackRunIdRef.current, status.active_tasks || [])) return;

      // No report-back run named yet (still pending) or none is coming (the PTC
      // dispatch failed). Keep polling, but give up after a bounded window so a
      // never-arriving report-back doesn't poll forever.
      if (source === 'poll' && ++polls >= REPORT_BACK_MAX_POLLS) {
        consumed = true;
        awaitingReportBackRef.current = false;
        stopReportBackWatch();
      }
    };

    const { abort } = watchThread(tid, async (payload) => {
      const wakeRunId = payload?.run_id ?? null;
      if (wakeRunId) {
        await reconcile('wake', wakeRunId);
        return;
      }
      // Payload-less wake (older backend / malformed): fall back to a /status
      // reconcile. A short delay lets the report-back run register so
      // report_back_run_id is populated.
      await new Promise((r) => setTimeout(r, 500));
      await reconcile('wake');
    });
    reportBackWatchAbortRef.current = abort;
    reportBackPollRef.current = setInterval(() => reconcile('poll'), 3000);
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
    // pending report-back: the keyed watch persists, holds its run id, and renders
    // again when the user returns to the flash thread. Its render gate
    // (threadIdRef === its flash thread) keeps it off the PTC thread meanwhile.
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
      }

      // A pending PTC report-back is caught by the watch — the reliable mechanism
      // that attaches to the run /status names (report_back_run_id) or a wake
      // carries. Arm it here, BEFORE the reconnect branch below, so it runs
      // regardless of whether this load also reconnects to an active run. On a
      // refresh right as the report-back becomes due, /status can report
      // can_reconnect=true AND pending_report_back=true; reconnecting to
      // status.run_id alone can miss the report-back run (stale/drained run, or a
      // short summary that finishes before attach), and without the watch it would
      // only surface on a later history-replay refresh. The watch stays dormant
      // while a reconnect stream is live (its reconcile bails on isStreamingRef),
      // and attach() skips the run already on screen — so this never double-streams.
      if (status.pending_report_back) {
        if (import.meta.env.DEV) console.log('[ReportBack] Pending report-back detected on load, opening watch');
        awaitingReportBackRef.current = true;
        // Key the watch to THIS thread (the flash thread — pending_report_back is
        // a flash-thread property). Idempotent if a watch for it already persists.
        startReportBackWatch(threadId);
      }

      if (historyHasUnresolvedInterruptRef.current && status.can_reconnect) {
        // Workflow is active → interrupt was answered, reconnect will deliver resolution
        console.log('[Reconnect] Unresolved interrupt from history, reconnecting to get resolution events');
        historyHasUnresolvedInterruptRef.current = false;
        await reconnectToStream({ activeTasks: status.active_tasks || [], runId: status.run_id ?? null, resetCursor: true });
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
        await reconnectToStream({ activeTasks: status.active_tasks || [], runId: status.run_id ?? null, resetCursor: true });
      } else if (status.active_tasks && status.active_tasks.length > 0) {
        // Main workflow completed but subagent tasks still running.
        // Reopen per-task SSE streams so cards stay live after refresh.
        console.log('[Reconnect] Main workflow done, reopening per-task streams for active subagents:', status.active_tasks);
        const dummyAssistantId = `assistant-subagent-reconnect-${Date.now()}`;
        const refs = {
          contentOrderCounterRef,
          currentReasoningIdRef,
          currentToolCallIdRef,
          steeringAtOrderRef,
          updateTodoListCard: updateTodoListCard || undefined,
          isNewConversation: false,
          subagentStateRefs: subagentStateRefsRef.current,
          updateSubagentCard: updateSubagentCard
            ? (agentId: string, data: Record<string, unknown>) => updateSubagentCard(agentId, { ...data, isReconnect: true })
            : (() => {}),
          isReconnect: true,
        };
        const processEvent = createStreamEventProcessor(dummyAssistantId, refs, getTaskIdFromEvent);
        // Pre-seed cards from history so per-task events don't create empty cards
        for (const taskId of status.active_tasks) {
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
          openSubagentStream(threadId, taskId, processEvent);
        }
        setHasActiveSubagents(true);
      } else {
        // Workflow is not active — mark all subagent tasks as completed.
        // (Skipped when reconnecting because per-task SSE streams
        //  will deliver live events with the real status.)
        markAllSubagentTasksCompleted();
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
      closeAllSubagentStreams();
      subagentStateRefsRef.current = {};
    };
    // Note: loadConversationHistory is not in deps because it uses workspaceId and threadId from closure
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [workspaceId, threadId, reloadTrigger]);

  // The report-back watch survives thread navigation (so a flash report-back and a
  // live PTC stream coexist), so the thread-load effect above can't own its
  // teardown. Stop it only when the chat hook itself unmounts.
  useEffect(() => {
    return () => {
      stopReportBackWatch();
      awaitingReportBackRef.current = false;
    };
  }, []);

  /**
   * Marks all subagentTasks in messages as 'completed'.
   * Called when the SSE stream ends or history finishes loading, because a finished
   * workflow implies all subagents are done. This is a safety net — artifact events
   * and per-task SSE streams should have already updated most of them, but the final
   * completion may not be persisted in sse_events or may have been missed.
   */
  const markAllSubagentTasksCompleted = () => {
    // Skip tasks with open per-task SSE streams (still active)
    const activeShortIds = new Set(subagentStreamsRef.current.keys());

    setMessages((prev) => {
      let anyChanged = false;
      const updated = prev.map((msg) => {
        if (msg.role !== 'assistant') return msg;
        const aMsg = msg as AssistantMessage;
        if (!aMsg.subagentTasks || Object.keys(aMsg.subagentTasks).length === 0) return msg;
        let changed = false;
        const updatedTasks = { ...aMsg.subagentTasks };
        Object.keys(updatedTasks).forEach((toolCallId) => {
          const agentId = toolCallIdToTaskIdMapRef.current.get(toolCallId);
          // If the task still has an open per-task stream, skip it
          if (agentId) {
            const shortId = agentId.replace('task:', '');
            if (activeShortIds.has(shortId)) return;
          }

          if (updatedTasks[toolCallId].status !== 'completed') {
            updatedTasks[toolCallId] = { ...updatedTasks[toolCallId], status: 'completed' };
            changed = true;
          }
        });
        if (changed) anyChanged = true;
        return changed ? { ...aMsg, subagentTasks: updatedTasks } : msg;
      });
      return anyChanged ? updated : prev;
    });
  };

  /**
   * Open a dedicated per-task SSE stream for a subagent.
   * Events from the stream are routed through processEvent (same handler as the main stream).
   * Idempotent — skips if a stream is already open for this taskId.
   *
   * @param {string} tid - Thread ID
   * @param {string} shortTaskId - The 6-char task identifier (e.g., 'k7Xm2p')
   * @param {Function} processEvent - The event processor (from createStreamEventProcessor)
   */
  const openSubagentStream = (tid: string, shortTaskId: string, processEvent: (event: SSEEvent) => void) => {
    if (subagentStreamsRef.current.has(shortTaskId)) return; // already open
    const controller = new AbortController();
    subagentStreamsRef.current.set(shortTaskId, controller);

    streamSubagentTaskEvents(tid, shortTaskId, processEvent, controller.signal)
      .catch((err) => {
        if (err.name !== 'AbortError') {
          console.error(`[SubagentStream:${shortTaskId}]`, err);
        }
      })
      .finally(() => {
        subagentStreamsRef.current.delete(shortTaskId);
        completedTaskIdsRef.current.add(shortTaskId);
        // Per-task stream close = task completion signal
        if (updateSubagentCard) {
          updateSubagentCard(`task:${shortTaskId}`, { status: 'completed', isActive: false });
        }
        // Flip the chat-card status for the just-closed task. The function
        // skips tasks whose stream is still in subagentStreamsRef (active),
        // so calling on every close marks only the just-finished one.
        markAllSubagentTasksCompleted();
        // Workflow-level cleanup only when ALL streams have closed.
        if (subagentStreamsRef.current.size === 0) {
          setHasActiveSubagents(false);
          if (inactivateAllSubagents) inactivateAllSubagents();
        }
      });
  };

  /**
   * Abort all open per-task subagent streams.
   */
  const closeAllSubagentStreams = () => {
    for (const [, controller] of subagentStreamsRef.current) {
      controller.abort();
    }
    subagentStreamsRef.current.clear();
  };

  /**
   * Helper to get taskId from event.
   * Routes subagent events to the correct task based on agent ID mapping.
   * Defined at hook level so it can be shared between handleSendMessage and reconnectToStream.
   */
  const getTaskIdFromEvent = (event: SSEEvent): string | null => {
    // With task:{task_id} format, the task ID is embedded in the agent field.
    // e.g., agent = "task:pkyRHQ" → taskId = "task:pkyRHQ"
    // This is the agent_id used as key throughout the frontend.
    const agent = event?.agent;
    if (!agent || typeof agent !== 'string' || !agent.startsWith('task:')) {
      if (import.meta.env.DEV) {
        console.warn('[Stream] Subagent event without task: agent field:', event);
      }
      return null;
    }
    return agent;
  };

  /**
   * Shared cleanup logic for all stream-end paths (send, reconnect, HITL resume).
   * Resets loading/streaming state, finalizes subagents, and auto-completes todos.
   */
  const cleanupAfterStreamEnd = (assistantMessageId: string) => {
    setIsLoading(false);
    setWorkspaceStarting(false);
    setIsCompacting(false);
    currentMessageRef.current = null;
    releaseStreamOwnership();

    const hasOpenStreams = subagentStreamsRef.current.size > 0;
    if (!hasOpenStreams) {
      if (inactivateAllSubagents) inactivateAllSubagents();
      markAllSubagentTasksCompleted();
      closeAllSubagentStreams();
    }
    setHasActiveSubagents(hasOpenStreams);

    // Finalize pending todos as stale
    if (finalizePendingTodos) finalizePendingTodos();
    setMessages((prev) => finalizeTodoListProcessesInMessages(prev, assistantMessageId));

    // Open watch connection for report-back if a PTC agent was dispatched with
    // report_back enabled. The watch attaches to the run the backend names (wake
    // payload or /status's report_back_run_id), skipping the run currently on
    // screen (this just-finished "Dispatched." turn). Key it to the flash thread
    // that dispatched: an already-running watch names it (re-arm is then a no-op);
    // otherwise this is the initial in-session arm, where the visible thread IS
    // the flash thread. Without the key, a stream-end while the user is on the
    // dispatched PTC thread would wrongly re-point the watch at the PTC thread.
    if (awaitingReportBackRef.current) {
      startReportBackWatch(reportBackWatchThreadIdRef.current ?? threadIdRef.current);
    }
  };

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
    const activeShortIds = new Set(subagentStreamsRef.current.keys());
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
        updateSubagentCard(agentId, { messages: taskRefs.messages, status: 'completed', isActive: false });
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
    // Drop any message queued during compaction: the user just cancelled, so it
    // must NOT auto-send when the isCompacting→false transition fires the flush
    // effect. dropQueuedSend clears the ref synchronously (before the effect runs
    // post-render) and removes its optimistic shimmer bubble.
    dropQueuedSend();
    releaseStreamOwnership();
    currentMessageRef.current = null;

    // (c) Abort per-task subagent streams + the report-back watch so no
    // background work lingers on the client after the stop.
    closeAllSubagentStreams();
    stopReportBackWatch();
    awaitingReportBackRef.current = false;

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

  /**
   * Creates a stream event processor that handles SSE events from the backend.
   * Used by both handleSendMessage (live) and reconnectToStream (reconnection).
   *
   * @param {string} assistantMessageId - The assistant message ID to update
   * @param {Object} refs - Refs for event handlers (contentOrderCounterRef, etc.)
   * @param {Function} getTaskIdFromEvent - Helper to route subagent events
   * @returns {Function} Event handler: (event) => void
   */
  // TODO: type properly — refs should use a proper interface matching StreamRefs from streamEventHandlers
  const createStreamEventProcessor = (assistantMessageId: string, refs: StreamProcessorRefs, getTaskIdFromEvent: (event: SSEEvent) => string | null, wasInterruptedRef: { current: boolean } | null = null) => {
    // Snapshot of the old assistant message's content order at the time the user
    // sent a steering message.  Used to roll back any content that leaked into the
    // old bubble due to stream-mode multiplexing (custom events can arrive after
    // message chunks from the post-injection model call).
    let steeringAtOrder: number | null = null;

    // FIFO queue for matching Task tool call IDs to artifact 'spawned' events.
    // Populated by the tool_calls handler, drained by the artifact/spawned handler.
    // This ensures toolCallIdToTaskIdMapRef is populated before tool_call_result.
    const pendingTaskToolCallIds: string[] = [];

    const processEvent = (event: SSEEvent): void => {
      const eventType = event.event || 'message_chunk';

      // Track last event ID for reconnection
      if (event._eventId != null) {
        lastEventIdRef.current = event._eventId;
      }

      // The ``metadata`` event is the first event of every workflow stream
      // and carries the authoritative run_id for this turn. Latch it so
      // reconnect targets ``workflow:stream:{tid}:{rid}`` precisely.
      if (eventType === 'metadata') {
        if (event.run_id) {
          currentRunIdRef.current = event.run_id;
        }
        return;
      }

      // compaction_chunk is the side channel for LLM output from the
      // compaction middleware; swallow so it does not leak into the
      // assistant message stream. The context_window summarize
      // start/complete/error events already surface compaction state.
      if (eventType === 'compaction_chunk') {
        return;
      }

      // Update thread_id if provided in the event (ref = synchronous for closures)
      if (event.thread_id && event.thread_id !== '__default__') {
        threadIdRef.current = event.thread_id;
        if (event.thread_id !== threadId) {
          setThreadId(event.thread_id);
          setStoredThreadId(workspaceId, event.thread_id);
        }
      }

      // Handle workspace_status events (workspace starting/ready).
      // An optional `sandbox_state: "archived"` refinement event follows the
      // generic `starting` on the slow cold-restore path — branch copy on it.
      if (eventType === 'workspace_status') {
        if (event.status === 'starting') {
          const state = event.sandbox_state === 'archived' ? 'archived' : 'starting';
          setWorkspaceStarting(state);
        } else {
          setWorkspaceStarting(false);
        }
        return;
      }

      // Check if this is a subagent event - filter it out from main chat view
      const isSubagent = isSubagentEvent(event);

      // Handle steering_accepted events for the MAIN agent (user sent a message while agent streams).
      // Subagent steering_accepted events are handled below in the isSubagent block.
      if (eventType === 'steering_accepted' && !isSubagent) {
        // The steering_accepted event's own `_eventId` is the boundary: every
        // earlier event in the Redis stream has a smaller id, every later one
        // has a larger id. Use it directly so the rollback filter knows what
        // to keep vs. drop. Fall back to the local counter for tests/legacy
        // flows where SSE events carry no `_eventId`.
        const boundary = computeSteeringBoundary(event, refs.contentOrderCounterRef.current);
        steeringAtOrder = boundary;
        if (refs.steeringAtOrderRef) refs.steeringAtOrderRef.current = boundary;
        return;
      }

      // Handle steering_delivered custom events (middleware picked up the steering message).
      // Subagent steering_delivered events are handled in the isSubagent block below.
      if (eventType === 'steering_delivered' && !isSubagent) {
        const oldAssistantId = assistantMessageId;

        // 1. Roll back old assistant message to the snapshot taken at steering_accepted
        //    time, removing any content that leaked due to stream-mode multiplexing.
        //    Then finalize it (isStreaming: false).
        setMessages((prev) =>
          prev.map((msg) => {
            if (msg.id !== oldAssistantId) return msg;
            if (msg.role !== 'assistant') return msg;
            const aMsg = msg as AssistantMessage;

            // Use closure-local snapshot or fall back to the shared ref
            // (steering_accepted only arrives on the secondary POST stream, so
            // the closure-local steeringAtOrder is typically null — the shared
            // ref is set by handleSendSteering on the secondary stream).
            const effectiveSteeringAtOrder = steeringAtOrder ?? refs.steeringAtOrderRef?.current ?? null;

            // If no snapshot — or snapshot is non-positive / NaN (steering
            // arrived before any ordered content was emitted, or `_eventId`
            // was a non-numeric fallback) — skip the destructive filter and
            // just finalize. Real segment orders are always positive; any
            // other boundary would drop every segment, wiping the visible turn.
            if (shouldSkipSteeringRollback(effectiveSteeringAtOrder)) {
              const tp: typeof aMsg.toolCallProcesses = {};
              for (const [id, val] of Object.entries(aMsg.toolCallProcesses || {})) {
                tp[id] = val.isInProgress ? { ...val, isInProgress: false, isComplete: true } : val;
              }
              const rp: typeof aMsg.reasoningProcesses = {};
              for (const [id, val] of Object.entries(aMsg.reasoningProcesses || {})) {
                rp[id] = val.isReasoning ? { ...val, isReasoning: false, reasoningComplete: true } : val;
              }
              return { ...aMsg, isStreaming: false, toolCallProcesses: tp, reasoningProcesses: rp };
            }

            // Keep only segments at or before the steering point. Guard
            // already proved boundary is a positive finite number.
            const boundary = effectiveSteeringAtOrder as number;
            const keptSegments = (aMsg.contentSegments || []).filter(
              (s) => s.order <= boundary
            );

            // Rebuild plain-text content from kept text segments
            const keptContent = keptSegments
              .filter((s): s is import('@/types/chat').TextSegment => s.type === 'text')
              .map((s) => s.content || '')
              .join('');

            // Collect IDs of kept processes so we can prune orphans
            const keptReasoningIds = new Set(
              keptSegments.filter((s): s is import('@/types/chat').ReasoningSegment => s.type === 'reasoning').map((s) => s.reasoningId)
            );
            const keptToolCallIds = new Set(
              keptSegments.filter((s): s is import('@/types/chat').ToolCallSegment => s.type === 'tool_call').map((s) => s.toolCallId)
            );
            const keptTodoListIds = new Set(
              keptSegments.filter((s): s is import('@/types/chat').TodoListSegment => s.type === 'todo_list').map((s) => s.todoListId)
            );
            const keptSubagentIds = new Set(
              keptSegments.filter((s): s is import('@/types/chat').SubagentTaskSegment => s.type === 'subagent_task').map((s) => s.subagentId)
            );

            const filterObj = <V>(obj: Record<string, V> | undefined, keepSet: Set<string>): Record<string, V> => {
              if (!obj) return {} as Record<string, V>;
              const out: Record<string, V> = {};
              for (const [id, val] of Object.entries(obj)) {
                if (keepSet.has(id)) out[id] = val;
              }
              return out;
            };

            // Finalize retained processes: mark in-progress as complete
            const keptToolCalls = filterObj(aMsg.toolCallProcesses, keptToolCallIds);
            for (const [id, val] of Object.entries(keptToolCalls)) {
              if (val.isInProgress) keptToolCalls[id] = { ...val, isInProgress: false, isComplete: true };
            }
            const keptReasoning = filterObj(aMsg.reasoningProcesses, keptReasoningIds);
            for (const [id, val] of Object.entries(keptReasoning)) {
              if (val.isReasoning) keptReasoning[id] = { ...val, isReasoning: false, reasoningComplete: true };
            }

            return {
              ...aMsg,
              contentSegments: keptSegments,
              content: keptContent,
              reasoningProcesses: keptReasoning,
              toolCallProcesses: keptToolCalls,
              todoListProcesses: filterObj(aMsg.todoListProcesses, keptTodoListIds),
              subagentTasks: filterObj(aMsg.subagentTasks, keptSubagentIds),
              isStreaming: false,
            };
          })
        );
        steeringAtOrder = null;
        if (refs.steeringAtOrderRef) refs.steeringAtOrderRef.current = null;

        // 2. Mark steering user messages as delivered, OR create them from event
        //    data if none exist (reconnect scenario — in-memory state was lost).
        setMessages((prev) => {
          const hasSteeringMessages = prev.some((msg) => 'steering' in msg && msg.steering);
          if (hasSteeringMessages) {
            // Live path: mark existing steering messages as delivered
            return prev.map((msg) =>
              'steering' in msg && msg.steering ? { ...msg, steering: false, steeringDelivered: true } : msg
            );
          }
          // Reconnect path: create user bubbles from event payload
          const steeringMsgs = (event.messages || []).filter((qMsg) => qMsg.content);
          if (steeringMsgs.length === 0) return prev;
          const newUserMessages: MessageRecord[] = steeringMsgs.map((qMsg) => ({
            id: `steering-user-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
            role: 'user' as const,
            content: qMsg.content as string,
            contentType: 'text' as const,
            timestamp: qMsg.timestamp ? new Date((qMsg.timestamp as number) * 1000) : new Date(),
            isStreaming: false as const,
            steeringDelivered: true,
          }));
          return [...prev, ...newUserMessages];
        });

        // 3. Create new assistant message placeholder (steering continuation — not a new backend turn)
        const newAssistantId = `assistant-${Date.now()}`;
        const newAssistant = { ...createAssistantMessage(newAssistantId), isSteering: true };
        setMessages((prev) => appendMessage(prev,newAssistant));

        // 4. Switch closure & refs to new assistant message
        assistantMessageId = newAssistantId;
        currentMessageRef.current = newAssistantId;

        // 5. Reset content counters
        refs.contentOrderCounterRef.current = 0;
        refs.currentReasoningIdRef.current = null;
        refs.currentToolCallIdRef.current = null;
        return;
      }

      // Handle steering_returned — agent finished before consuming the steering message.
      // Remove the steering user message from chat and restore text to input box.
      if (eventType === 'steering_returned') {
        const returnedMessages = event.messages || [];
        if (returnedMessages.length > 0) {
          // Remove steering user messages from the chat
          setMessages((prev) => prev.filter((msg) => !('steering' in msg && msg.steering)));
          // Restore the text to the input box via state
          const combinedText = returnedMessages.map((m) => m.content).join('\n');
          setReturnedSteering(combinedText);
        }
        return;
      }

      // Handle unified context_window events (token_usage, summarize, offload)
      if (eventType === 'context_window') {
        if (isSubagent) {
          // For subagent context_window events, embed notification as a content
          // segment inside the current assistant message (same as main chat) so
          // it appears at the correct chronological position.
          const taskId = getTaskIdFromEvent(event);
          if (taskId && event.action === 'token_usage') {
            // Sum per-call delta into the per-task running total, then push
            // the new total onto SubagentData so the AgentInfo projection
            // (and the inline subagent card) re-renders with it.
            const prev = subagentTokenUsageRef.current[taskId] ?? ZERO_USAGE;
            const next = accumulateTokenUsage(prev, extractTokenUsageDelta(event));
            subagentTokenUsageRef.current[taskId] = next;
            if (updateSubagentCard) {
              updateSubagentCard(taskId, { tokenUsage: next });
            }
            return;
          }
          // token_usage is handled and returned above; this branch is now
          // for summarize / offload / future actions only.
          if (taskId) {
            const action = event.action;
            let text;
            let detail: string | undefined;
            if (action === 'summarize' && event.signal === 'complete') {
              text = t('chat.compactedNotification', { from: event.original_message_count });
              detail = (event.summary_text as string | undefined) || undefined;
            } else if (action === 'offload' && event.signal === 'complete') {
              const args = event.offloaded_args || 0;
              const reads = event.offloaded_reads || 0;
              if (args > 0 && reads > 0) text = t('chat.offloadedNotification', { args, reads });
              else if (reads > 0) text = t('chat.offloadedReadsNotification', { count: reads });
              else if (args > 0) text = t('chat.offloadedArgsNotification', { count: args });
            }
            if (text && updateSubagentCard) {
              const taskRefs = getOrCreateTaskRefs(refs, taskId);
              const order = ++taskRefs.contentOrderCounterRef.current;
              // Find the last assistant message and append the notification segment
              const updatedMessages = [...taskRefs.messages] as Record<string, unknown>[];
              const msgIdx = updatedMessages.findLastIndex((m) => m.role === 'assistant');
              if (msgIdx !== -1) {
                const existingMsg = updatedMessages[msgIdx];
                const segs = (existingMsg.contentSegments || []) as Record<string, unknown>[];
                updatedMessages[msgIdx] = { ...existingMsg, contentSegments: [...segs, { type: 'notification', content: text, order, detail }] };
              }
              taskRefs.messages = updatedMessages;
              updateSubagentCard(taskId, { messages: updatedMessages });
            }
          }
          return;
        }
        handleContextWindowEvent(event, {
          getMsgId: () => currentMessageRef.current,
          nextOrder: () => {
            const eventId = event._eventId;
            return eventId != null ? Number(eventId) : ++refs.contentOrderCounterRef.current;
          },
          setMessages,
          setTokenUsage,
          setIsCompacting,
          insertNotification,
          t,
          offloadBatch: offloadBatchRef,
        });
        return;
      }

      // Handle provenance events BEFORE the isSubagent filter so subagent-emitted
      // accessed-data records still attach to the main turn's assistant message
      // (with their `agent="task:..."` attribution preserved on the record).
      // The event is flat — fields top-level on `event` — matching the live
      // tool_call_result reader.
      if (eventType === 'provenance') {
        handleProvenance({
          assistantMessageId,
          event: event as unknown as import('@/types/sse').ProvenanceEvent,
          setMessages: setMessagesForHandlers,
        });
        return;
      }

      // Handle subagent message events (filter them out from main chat view)
      if (isSubagent) {
        // With task:{task_id} format, the agent field IS the task key
        const taskId = getTaskIdFromEvent(event);

        if (!taskId) {
          return; // Don't process in main chat view
        }

        // Process the event with the correct taskId
        if (updateSubagentCard) {

          // Use a stable message ID per task+run so all events from the same run
          // go into one message. Each resume bumps runIndex, creating a new message
          // so the card shows a unified conversation across resume boundaries.
          const taskRefs = getOrCreateTaskRefs(refs, taskId);
          const subagentAssistantMessageId = `subagent-${taskId}-assistant-${taskRefs.runIndex}`;

          if (eventType === 'message_chunk') {
            const contentType = (event.content_type || 'text') as string;
            handleSubagentMessageChunk({
              taskId,
              assistantMessageId: subagentAssistantMessageId,
              contentType,
              content: event.content as string,
              finishReason: event.finish_reason,
              refs,
              updateSubagentCard,
            });
          } else if (eventType === 'tool_call_chunks') {
            handleSubagentToolCallChunks({
              taskId,
              assistantMessageId: subagentAssistantMessageId,
              chunks: (event.tool_call_chunks || []) as unknown as Record<string, unknown>[],
              refs,
              updateSubagentCard,
            });
          } else if (eventType === 'tool_calls') {
            handleSubagentToolCalls({
              taskId,
              assistantMessageId: subagentAssistantMessageId,
              toolCalls: (event.tool_calls || []) as unknown as Record<string, unknown>[],
              refs,
              updateSubagentCard,
            });
          } else if (eventType === 'tool_call_result') {
            const toolCallId = event.tool_call_id as string;

            handleSubagentToolCallResult({
              taskId,
              assistantMessageId: subagentAssistantMessageId,
              toolCallId: toolCallId,
              result: {
                content: event.content,
                content_type: event.content_type,
                tool_call_id: toolCallId,
                artifact: event.artifact,
              },
              refs,
              updateSubagentCard,
            });
          } else if (eventType === 'artifact') {
            // Subagent artifact events are intentionally filtered out of the main chat view.
          } else if (eventType === 'steering_delivered') {
            if (event.content) {
              handleTaskSteeringAccepted({
                taskId,
                content: event.content as string,
                refs,
                updateSubagentCard,
              });
            }
          }
        }
        return; // Don't process subagent events in main chat view
      }

      if (eventType === 'message_chunk') {
        const contentType = event.content_type || 'text';
        const eventId = event._eventId as number | undefined;

        // Handle reasoning_signal events
        if (contentType === 'reasoning_signal') {
          const signalContent = (event.content || '') as string;
          if (handleReasoningSignal({
            assistantMessageId,
            signalContent,
            refs,
            setMessages: setMessagesForHandlers,
            eventId,
          })) {
            return;
          }
        }

        // Handle reasoning content chunks
        if (contentType === 'reasoning' && event.content) {
          if (handleReasoningContent({
            assistantMessageId,
            content: event.content as string,
            refs,
            setMessages: setMessagesForHandlers,
          })) {
            return;
          }
        }

        // Handle text content chunks
        if (contentType === 'text') {
          if (handleTextContent({
            assistantMessageId,
            content: event.content as string,
            finishReason: event.finish_reason,
            refs,
            setMessages: setMessagesForHandlers,
            eventId,
          })) {
            return;
          }
        }

        // Skip other content types
        return;
      } else if (eventType === 'error' || event.error) {
        const errorMessage = event.error || event.message || 'An error occurred while processing your request.';
        // Backend (streaming_handler.format_error_event) enriches the event
        // with ``error_kind``, ``status_code`` and ``hints``. We route the
        // display by kind to avoid showing the same error twice:
        //   - upstream → inline card on the failed assistant turn (part of
        //     the transcript; the user's model choice is what triggered it)
        //   - internal → banner near the chat input (our service failed;
        //     don't pollute the conversation history)
        const kind = event.error_kind as 'upstream' | 'internal' | undefined;
        const structured: StructuredError | undefined =
          kind === 'upstream' || kind === 'internal'
            ? {
                message: errorMessage as string,
                kind,
                statusCode: typeof event.status_code === 'number' ? event.status_code : undefined,
                // ``hints`` are only meaningful for upstream provider errors
                // (the bullets say "check your API key / plan / provider
                // status"). An internal error doesn't get hints even if the
                // backend ever starts emitting them for some future variant.
                hints: kind === 'upstream' && Array.isArray(event.hints)
                  ? (event.hints.filter(isUpstreamHint) as StructuredError['hints'])
                  : undefined,
              }
            : undefined;

        if (kind === 'internal') {
          // Banner only — drop the optimistic assistant bubble entirely so the
          // transcript stays clean. Matches the 429 rate-limit path; leaving a
          // content-less bubble under the banner looks broken.
          setMessageError(structured ?? (errorMessage as string));
          setMessages((prev) => prev.filter((m) => m.id !== assistantMessageId));
        } else {
          // Upstream (or unclassified legacy) — render inline. Clear any
          // stale banner from a prior turn so the error lives in one place.
          setMessageError(null);
          setMessages((prev) =>
            updateMessage(prev, assistantMessageId, (msg) => ({
              ...msg,
              content: msg.content || errorMessage,
              contentType: 'text',
              isStreaming: false,
              error: true,
              ...(structured ? { structuredError: structured } : {}),
            }))
          );
        }
      } else if (eventType === 'tool_call_chunks') {
        handleToolCallChunks({
          assistantMessageId,
          chunks: (event.tool_call_chunks || []) as unknown as Record<string, unknown>[],
          setMessages: setMessagesForHandlers,
        });
        return;
      } else if (eventType === 'artifact') {
        const artifactType = event.artifact_type as string;
        if (artifactType === 'todo_update') {
          handleTodoUpdate({
            assistantMessageId,
            artifactType,
            artifactId: event.artifact_id as string,
            payload: event.payload || {},
            refs,
            setMessages: setMessagesForHandlers,
            eventId: event._eventId as number,
          });
        } else if (artifactType === 'html_widget') {
          handleHtmlWidget({
            assistantMessageId,
            artifactType,
            artifactId: event.artifact_id as string,
            payload: (event.payload || {}) as unknown as HtmlWidgetData,
            refs,
            setMessages: setMessagesForHandlers,
            eventId: event._eventId as number,
          });
        } else if (artifactType === 'chart_annotation') {
          applyAnnotationArtifact(artifactType, (event.payload || {}) as Record<string, unknown>);
        } else if (artifactType === 'file_operation' && onFileArtifact) {
          onFileArtifact(event);
        } else if (artifactType === 'preview_url' && onPreviewUrl) {
          const payload = (event.payload || {}) as Record<string, unknown>;
          onPreviewUrl({
            url: '',  // resolved by ChatView via authenticated endpoint
            port: payload.port as number,
            title: payload.title as string | undefined,
            command: payload.command as string | undefined,
            path: payload.path as string | undefined,
            loading: true,
          });
        } else if (artifactType === 'task') {
          const payload = (event.payload || {}) as Record<string, unknown>;
          const { task_id, action: rawAction, description, prompt, type } = payload;
          const action = (() => { if (rawAction === 'spawned') return 'init'; if (rawAction === 'steering_accepted') return 'update'; if (rawAction === 'resumed') return 'resume'; return rawAction || 'init'; })() as string;
          if (!task_id) return;
          const agentId = `task:${task_id}`;

          // Establish toolCallId → agentId mapping immediately, so clicking
          // the inline card before tool_call_result resolves correctly.
          {
            const updated = mapToolCallIdToAgentId(
              event.tool_call_id as string | undefined,
              agentId,
              action,
              pendingTaskToolCallIds,
              toolCallIdToTaskIdMapRef.current,
            );
            pendingTaskToolCallIds.length = 0;
            pendingTaskToolCallIds.push(...updated);
          }

          if (action === 'init') {
            const alreadyCompleted = completedTaskIdsRef.current.has(task_id as string);
            if (updateSubagentCard) {
              updateSubagentCard(agentId, {
                agentId,
                displayId: `Task-${task_id}`,
                taskId: agentId,
                type: (type || 'general-purpose') as string,
                description: (description || '') as string,
                prompt: (prompt || description || '') as string,
                status: alreadyCompleted ? 'completed' : 'active',
                isActive: !alreadyCompleted,
              });
            }
            if (!alreadyCompleted) {
              const currentThreadId = (event.thread_id || threadIdRef.current) as string;
              openSubagentStream(currentThreadId, task_id as string, processEvent);
            }
          } else if (action === 'resume') {
            // Resume: preserve existing messages, inject user boundary, bump runIndex
            const taskRefsForResume = getOrCreateTaskRefs(refs, agentId);

            // Finalize the last assistant message from the previous run
            const updatedMessages = [...taskRefsForResume.messages] as Record<string, unknown>[];
            for (let i = updatedMessages.length - 1; i >= 0; i--) {
              if (updatedMessages[i].role === 'assistant' && updatedMessages[i].isStreaming) {
                updatedMessages[i] = { ...updatedMessages[i], isStreaming: false };
                break;
              }
            }

            // Inject user message with resume instruction
            updatedMessages.push({
              id: `resume-${agentId}-${Date.now()}`,
              role: 'user',
              content: prompt || description || 'Resume',
              contentSegments: [{ type: 'text', content: prompt || description || 'Resume', order: 0 }],
              reasoningProcesses: {},
              toolCallProcesses: {},
            });

            // Bump runIndex and reset per-run counters
            taskRefsForResume.runIndex = (taskRefsForResume.runIndex || 0) + 1;
            taskRefsForResume.contentOrderCounterRef.current = 0;
            taskRefsForResume.currentReasoningIdRef.current = null;
            taskRefsForResume.currentToolCallIdRef.current = null;
            taskRefsForResume.messages = updatedMessages;

            if (updateSubagentCard) {
              // Prefer preserving the original spawn description (already on the card).
              // But after reconnect the card may have been wiped + recreated without a
              // description, so fall back to subagentHistoryRef as a safety net.
              const historyDesc = subagentHistoryRef.current?.[agentId]?.description;
              const historyPrompt = subagentHistoryRef.current?.[agentId]?.prompt;
              updateSubagentCard(agentId, {
                agentId,
                displayId: `Task-${task_id}`,
                taskId: agentId,
                type: (type || 'general-purpose') as string,
                status: 'active',
                isActive: true,
                messages: updatedMessages,
                ...(historyDesc ? { description: historyDesc } : {}),
                ...(historyPrompt ? { prompt: historyPrompt } : {}),
              });
            }

            // Abort existing stream before opening new one (race condition safety)
            const existingController = subagentStreamsRef.current.get(task_id as string);
            if (existingController) {
              existingController.abort();
              subagentStreamsRef.current.delete(task_id as string);
            }

            const currentThreadId = (event.thread_id || threadIdRef.current) as string;
            openSubagentStream(currentThreadId, task_id as string, processEvent);
          } else if (action === 'update') {
            if (updateSubagentCard) {
              updateSubagentCard(agentId, { steeringMessage: prompt || payload.description });
            }
            // Update inline card to show "Updated" instead of "Resumed"
            setMessages(prev => prev.map(msg => {
              if (msg.role !== 'assistant') return msg;
              const aMsg = msg as AssistantMessage;
              if (!aMsg.subagentTasks) return msg;
              let changed = false;
              const newTasks = { ...aMsg.subagentTasks };
              for (const [tcId, task] of Object.entries(newTasks)) {
                if (task.resumeTargetId === agentId && task.action === 'resume') {
                  newTasks[tcId] = { ...task, action: 'update' };
                  changed = true;
                }
              }
              return changed ? { ...aMsg, subagentTasks: newTasks } : msg;
            }));
          }
        }
        return;
      } else if (eventType === 'tool_calls') {
        handleToolCalls({
          assistantMessageId,
          toolCalls: (event.tool_calls || []) as unknown as Record<string, unknown>[],
          finishReason: event.finish_reason,
          refs,
          setMessages: setMessagesForHandlers,
          eventId: event._eventId as number,
        });
        // Queue new Task tool call IDs for matching with upcoming artifact 'spawned' events
        if (event.tool_calls) {
          for (const tc of event.tool_calls) {
            if ((tc.name === 'task' || tc.name === 'Task') && tc.id && !tc.args?.task_id) {
              pendingTaskToolCallIds.push(tc.id);
            }
          }
        }
      } else if (eventType === 'tool_call_result') {
        // Check if this resolves an unresolved interrupt from history replay (FIFO array matching)
        const unresolvedList = refs.unresolvedHistoryInterruptRef?.current as HistoryInterruptInfo[] | undefined;
        if (unresolvedList && unresolvedList.length > 0 && typeof event.content === 'string') {
          const content = event.content as string;

          // Try create_workspace / start_question / ptc_agent / secretary actions
          const matchIdx = unresolvedList.findIndex((u: HistoryInterruptInfo) => PROPOSAL_INTERRUPT_TYPES.has(u.type));
          if (matchIdx !== -1) {
            const matched = unresolvedList[matchIdx];
            const dataKey = PROPOSAL_DATA_KEY_MAP[matched.type] || 'questionProposals';
            let resolvedStatus = 'approved';
            let resultPayload: Record<string, unknown> | null = null;
            if (content.startsWith('User declined')) {
              resolvedStatus = 'rejected';
            } else {
              try { const p = JSON.parse(content); if (p?.success === false) resolvedStatus = 'rejected'; resultPayload = p; } catch { /* not JSON */ }
            }
            const proposalId = matched.proposalId!;
            const extraFields: Record<string, unknown> = {};
            if (matched.type === 'ptc_agent' && resultPayload) {
              if (resultPayload.thread_id) extraFields.thread_id = resultPayload.thread_id;
              if (resultPayload.workspace_id) extraFields.workspace_id = resultPayload.workspace_id;
            }
            setMessages((prev) =>
              updateMessage(prev,matched.assistantMessageId, (m) => { if (m.role !== 'assistant') return m; const msg = m as AssistantMessage; return {
                ...msg,
                [dataKey]: {
                  ...((msg as unknown as Record<string, Record<string, unknown>>)[dataKey] || {}),
                  [proposalId]: {
                    ...((msg as unknown as Record<string, Record<string, Record<string, unknown>>>)[dataKey]?.[proposalId] || {}),
                    status: resolvedStatus,
                    ...extraFields,
                  },
                },
              }; })
            );
            unresolvedList.splice(matchIdx, 1);
          }
        }

        const toolCallId = event.tool_call_id as string;

        // Build toolCallId → agentId mapping from Task tool artifact
        if (event.artifact?.task_id && toolCallId) {
          const agentId = `task:${event.artifact.task_id}`;
          toolCallIdToTaskIdMapRef.current.set(toolCallId, agentId);
        }

        handleToolCallResult({
          assistantMessageId,
          toolCallId,
          result: {
            content: event.content,
            content_type: event.content_type,
            tool_call_id: toolCallId,
            artifact: event.artifact,
          },
          refs,
          setMessages: setMessagesForHandlers,
        });

        // When onboarding-related tools succeed, sync onboarding_completed via PUT
        if (onOnboardingRelatedToolComplete && isOnboardingRelatedToolSuccess(event.content)) {
          onOnboardingRelatedToolComplete();
        }

        // Detect navigate_to_workspace action from start_question tool result
        if (onWorkspaceCreated && typeof event.content === 'string') {
          try {
            const parsed = JSON.parse(event.content);
            if (parsed?.success && parsed?.action === 'navigate_to_workspace') {
              onWorkspaceCreated({ workspaceId: parsed.workspace_id, question: parsed.question });
            }
          } catch { /* not JSON, ignore */ }
        }

        // Update ptcAgentProposals with thread_id/workspace_id from tool result.
        // After HITL resume, the tool_call_result arrives on a NEW assistant message
        // while the proposals live on the OLD one (from the interrupt turn).
        // Match by tool_call_id for exact correlation (safe under concurrent dispatches).
        if (pendingPTCBackfillRef.current.size > 0 && typeof event.content === 'string') {
          const backfillPid = toolCallId ? pendingPTCBackfillRef.current.get(toolCallId) : undefined;
          if (backfillPid) {
            try {
              const parsed = JSON.parse(event.content);
              if (parsed?.success && parsed?.thread_id && parsed?.workspace_id) {
                pendingPTCBackfillRef.current.delete(toolCallId);
                setMessages((prev) =>
                  prev.map((m) => {
                    if (m.role !== 'assistant') return m;
                    const msg = m as AssistantMessage;
                    const proposals = msg.ptcAgentProposals;
                    if (!proposals?.[backfillPid]) return m;
                    return {
                      ...msg,
                      ptcAgentProposals: {
                        ...proposals,
                        [backfillPid]: {
                          ...proposals[backfillPid],
                          thread_id: parsed.thread_id,
                          workspace_id: parsed.workspace_id,
                        },
                      },
                    };
                  })
                );
              }
            } catch { /* not JSON, ignore */ }
          }
        }
      } else if (eventType === 'interrupt') {
        const actionRequests = event.action_requests || [];
        const actionType = actionRequests[0]?.type as string | undefined;

        if (actionType === 'ask_user_question') {
          // --- User question interrupt ---
          const questionId = event.interrupt_id || `question-${Date.now()}`;
          const questionData = actionRequests[0];
          const order = event._eventId != null ? Number(event._eventId) : ++refs.contentOrderCounterRef.current;

          setMessages((prev) =>
            updateMessage(prev,assistantMessageId, (m) => { if (m.role !== 'assistant') return m; const msg = m as AssistantMessage; return {
              ...msg,
              contentSegments: [
                ...(msg.contentSegments || []),
                { type: 'user_question', questionId, order },
              ],
              userQuestions: {
                ...(msg.userQuestions || {}),
                [questionId]: {
                  question: questionData.question,
                  options: questionData.options || [],
                  allow_multiple: questionData.allow_multiple || false,
                  interruptId: event.interrupt_id,
                  status: 'pending',
                  answer: null,
                },
              },
              isStreaming: false,
            }; })
          );

          pendingInterruptIdsRef.current.add(event.interrupt_id!);
          setPendingInterrupt({
            type: 'ask_user_question',
            interruptId: event.interrupt_id,
            assistantMessageId,
            questionId,
          });
        } else if (actionType === 'create_workspace') {
          // --- Create workspace interrupt ---
          const proposalId = event.interrupt_id || `workspace-${Date.now()}`;
          const proposalData = actionRequests[0];
          const order = event._eventId != null ? Number(event._eventId) : ++refs.contentOrderCounterRef.current;

          setMessages((prev) =>
            updateMessage(prev,assistantMessageId, (m) => { if (m.role !== 'assistant') return m; const msg = m as AssistantMessage; return {
              ...msg,
              contentSegments: [
                ...(msg.contentSegments || []),
                { type: 'create_workspace', proposalId, order },
              ],
              workspaceProposals: {
                ...(msg.workspaceProposals || {}),
                [proposalId]: {
                  workspace_name: proposalData.workspace_name,
                  workspace_description: proposalData.workspace_description,
                  interruptId: event.interrupt_id,
                  status: 'pending',
                },
              },
              isStreaming: false,
            }; })
          );

          pendingInterruptIdsRef.current.add(event.interrupt_id!);
          setPendingInterrupt({
            type: 'create_workspace',
            interruptId: event.interrupt_id,
            assistantMessageId,
            proposalId,
          });
        } else if (actionType === 'start_question') {
          // --- Start question interrupt ---
          const proposalId = event.interrupt_id || `question-start-${Date.now()}`;
          const proposalData = actionRequests[0];
          const order = event._eventId != null ? Number(event._eventId) : ++refs.contentOrderCounterRef.current;

          setMessages((prev) =>
            updateMessage(prev,assistantMessageId, (m) => { if (m.role !== 'assistant') return m; const msg = m as AssistantMessage; return {
              ...msg,
              contentSegments: [
                ...(msg.contentSegments || []),
                { type: 'start_question', proposalId, order },
              ],
              questionProposals: {
                ...(msg.questionProposals || {}),
                [proposalId]: {
                  workspace_id: proposalData.workspace_id,
                  question: proposalData.question,
                  interruptId: event.interrupt_id,
                  status: 'pending',
                },
              },
              isStreaming: false,
            }; })
          );

          pendingInterruptIdsRef.current.add(event.interrupt_id!);
          setPendingInterrupt({
            type: 'start_question',
            interruptId: event.interrupt_id,
            assistantMessageId,
            proposalId,
          });
        } else if (actionType === 'ptc_agent') {
          // --- PTC agent interrupt ---
          const proposalId = event.interrupt_id || `ptc-agent-${Date.now()}`;
          const proposalData = actionRequests[0];
          const order = event._eventId != null ? Number(event._eventId) : ++refs.contentOrderCounterRef.current;

          setMessages((prev) =>
            updateMessage(prev,assistantMessageId, (m) => { if (m.role !== 'assistant') return m; const msg = m as AssistantMessage; return {
              ...msg,
              contentSegments: [
                ...(msg.contentSegments || []),
                { type: 'ptc_agent' as const, proposalId, order },
              ],
              ptcAgentProposals: {
                ...(msg.ptcAgentProposals || {}),
                [proposalId]: {
                  workspace_id: proposalData.workspace_id,
                  workspace_name: proposalData.workspace_name,
                  question: proposalData.question,
                  report_back: proposalData.report_back ?? true,
                  interruptId: event.interrupt_id,
                  status: 'pending',
                },
              },
              isStreaming: false,
            }; })
          );

          pendingInterruptIdsRef.current.add(event.interrupt_id!);
          setPendingInterrupt({
            type: 'ptc_agent',
            interruptId: event.interrupt_id,
            assistantMessageId,
            proposalId,
            toolCallId: proposalData.tool_call_id,
          });
        } else if (actionType === 'delete_workspace' || actionType === 'stop_workspace' || actionType === 'delete_thread') {
          // --- Secretary action interrupt ---
          const proposalId = event.interrupt_id || `secretary-${actionType}-${Date.now()}`;
          const proposalData = actionRequests[0];
          const order = event._eventId != null ? Number(event._eventId) : ++refs.contentOrderCounterRef.current;

          setMessages((prev) =>
            updateMessage(prev,assistantMessageId, (m) => { if (m.role !== 'assistant') return m; const msg = m as AssistantMessage; return {
              ...msg,
              contentSegments: [
                ...(msg.contentSegments || []),
                { type: actionType as 'delete_workspace' | 'stop_workspace' | 'delete_thread', proposalId, order },
              ],
              secretaryActionProposals: {
                ...(msg.secretaryActionProposals || {}),
                [proposalId]: {
                  actionType: actionType as 'delete_workspace' | 'stop_workspace' | 'delete_thread',
                  workspace_id: proposalData.workspace_id,
                  thread_id: proposalData.thread_id,
                  interruptId: event.interrupt_id,
                  status: 'pending',
                },
              },
              isStreaming: false,
            }; })
          );

          pendingInterruptIdsRef.current.add(event.interrupt_id!);
          setPendingInterrupt({
            type: actionType,
            interruptId: event.interrupt_id,
            assistantMessageId,
            proposalId,
          });
        } else {
          // --- Plan approval interrupt (existing) ---
          const planApprovalId = event.interrupt_id || `plan-${Date.now()}`;
          const description =
            actionRequests[0]?.description ||
            (actionRequests[0]?.args?.plan as string) ||
            'No plan description provided.';

          const order = event._eventId != null ? Number(event._eventId) : ++refs.contentOrderCounterRef.current;

          setMessages((prev) =>
            updateMessage(prev,assistantMessageId, (m) => { if (m.role !== 'assistant') return m; const msg = m as AssistantMessage; return {
              ...msg,
              contentSegments: [
                ...(msg.contentSegments || []),
                { type: 'plan_approval', planApprovalId, order },
              ],
              planApprovals: {
                ...(msg.planApprovals || {}),
                [planApprovalId]: {
                  description,
                  interruptId: event.interrupt_id,
                  status: 'pending',
                },
              },
              isStreaming: false,
            }; })
          );

          pendingInterruptIdsRef.current.add(event.interrupt_id!);
          setPendingInterrupt({
            interruptId: event.interrupt_id,
            actionRequests: actionRequests,
            threadId: event.thread_id,
            assistantMessageId,
            planApprovalId,
            planMode: actionRequests.some((r) => r.name === 'SubmitPlan') || currentPlanModeRef.current,
          });
        }

        setIsLoading(false);
        releaseStreamOwnership();
        currentMessageRef.current = null;
        if (wasInterruptedRef) wasInterruptedRef.current = true;
      }
    };

    return processEvent;
  };

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
      mainStreamAbortRef.current = steeringAbort;
      const refs = {
        contentOrderCounterRef,
        currentReasoningIdRef,
        currentToolCallIdRef,
        steeringAtOrderRef,
        updateTodoListCard: updateTodoListCard || undefined,
        isNewConversation: false,
        subagentStateRefs: subagentStateRefsRef.current,
        updateSubagentCard: updateSubagentCard || (() => {}),
      };
      demotedProcessor = createStreamEventProcessor(newAssistantId, refs, getTaskIdFromEvent, demotedInterruptedRef);
    };

    try {
      // Send to same endpoint — backend will auto-accept steering and return steering_accepted SSE
      await sendChatMessageStream(
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
          pendingRunIdFromHeader = runId;
        },
        steeringAbort.signal,
      );
      if (mainStreamAbortRef.current === steeringAbort) {
        mainStreamAbortRef.current = null;
      }
      // User hit stop on the demoted turn: stopWorkflow owns the teardown.
      if (wasStoppedRef.current) {
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
    setHasActiveSubagents(false);
    completedTaskIdsRef.current.clear();
    // Clear the stopped guard so a fresh send can finalize again on stop.
    wasStoppedRef.current = false;
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

    let wasDisconnected = false;
    const wasInterruptedRef = { current: false };
    try {
      // Prepare refs for event handlers — use persistent subagent state
      const refs = {
        contentOrderCounterRef,
        currentReasoningIdRef,
        currentToolCallIdRef,
        steeringAtOrderRef,
        updateTodoListCard: updateTodoListCard || undefined,
        isNewConversation: isNewConversationRef.current,
        subagentStateRefs: subagentStateRefsRef.current,
        updateSubagentCard: updateSubagentCard || (() => {}),
      };

      // Create the event processor using the shared factory
      const processEvent = createStreamEventProcessor(assistantMessageId, refs, getTaskIdFromEvent, wasInterruptedRef);

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
          currentRunIdRef.current = runId;
          if (resolvedThreadId && resolvedThreadId !== '__default__') {
            threadIdRef.current = resolvedThreadId;
          }
        },
        abortController.signal,
      );

      // The user hit stop: stopWorkflow already finalized the message and ran
      // teardown. Skip reconnect/cleanup so we don't double-fire.
      if (result?.aborted || wasStoppedRef.current) {
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
      }
    } catch (err: unknown) {
          // An aborted stream (user hit stop) is intentional, not a failure.
          // streamFetch normally swallows AbortError and returns { aborted },
          // but guard here too so a stop never surfaces an error banner.
          if ((err as Error)?.name === 'AbortError' || wasStoppedRef.current) {
            return;
          }
          // Handle rate limit (429) — show limit message and remove optimistic assistant message
          const errObj = err as Record<string, unknown>;
          if (errObj.status === 429) {
            const info = (errObj.rateLimitInfo || {}) as Record<string, unknown>;
            const accountUrl = (import.meta.env.VITE_ACCOUNT_URL as string | undefined) || '/account';
            const structured = buildRateLimitError(info, accountUrl);
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
    wasStoppedRef.current = false;
    acquireStreamOwnership(threadId);
    // Fresh AbortController so stopWorkflow can abort this resumed stream.
    const abortController = new AbortController();
    mainStreamAbortRef.current = abortController;

    // Prepare refs for event handlers — use persistent subagent state
    const refs = {
      contentOrderCounterRef,
      currentReasoningIdRef,
      currentToolCallIdRef,
      steeringAtOrderRef,
      updateTodoListCard: updateTodoListCard || undefined,
      isNewConversation: false,
      subagentStateRefs: subagentStateRefsRef.current,
      updateSubagentCard: updateSubagentCard || (() => {}),
    };

    const wasInterruptedRef = { current: false };
    const processEvent = createStreamEventProcessor(assistantMessageId, refs, getTaskIdFromEvent, wasInterruptedRef);

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
          currentRunIdRef.current = runId;
        },
        abortController.signal,
      );

      // User hit stop: stopWorkflow already finalized + tore down.
      if (result?.aborted || wasStoppedRef.current) {
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
      }
    } catch (err: unknown) {
      if ((err as Error)?.name === 'AbortError' || wasStoppedRef.current) {
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
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [workspaceId, threadId, updateTodoListCard, updateSubagentCard, inactivateAllSubagents, finalizePendingTodos]);

  const handleApproveInterrupt = useCallback(() => {
    if (!pendingInterrupt) return;
    const { interruptId, assistantMessageId, planApprovalId, planMode } = pendingInterrupt;
    const approvalId = planApprovalId!;

    // Update plan card status to "approved"
    setMessages((prev) =>
      updateMessage(prev,assistantMessageId!, (m) => { if (m.role !== 'assistant') return m; const msg = m as AssistantMessage; return {
        ...msg,
        planApprovals: {
          ...(msg.planApprovals || {}),
          [approvalId]: {
            ...(msg.planApprovals?.[approvalId] || {}),
            status: 'approved',
          },
        },
      }; })
    );

    const hitlResponse = {
      [interruptId!]: { decisions: [{ type: 'approve' }] },
    };
    resumeWithHitlResponse(hitlResponse, planMode);
  }, [pendingInterrupt, resumeWithHitlResponse]);

  const handleRejectInterrupt = useCallback(() => {
    if (!pendingInterrupt) return;
    const { interruptId, assistantMessageId, planApprovalId, planMode } = pendingInterrupt;
    const approvalId = planApprovalId!;

    // Update plan card status to "rejected"
    setMessages((prev) =>
      updateMessage(prev,assistantMessageId!, (m) => { if (m.role !== 'assistant') return m; const msg = m as AssistantMessage; return {
        ...msg,
        planApprovals: {
          ...(msg.planApprovals || {}),
          [approvalId]: {
            ...(msg.planApprovals?.[approvalId] || {}),
            status: 'rejected',
          },
        },
      }; })
    );

    // Store interruptId + planMode so next handleSendMessage routes as rejection feedback
    setPendingRejection({ interruptId: interruptId!, planMode: planMode! });
    setPendingInterrupt(null);
  }, [pendingInterrupt]);

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
    collectedHitlResponsesRef.current[interruptId] = { decisions: [{ type: 'approve', message: answer }] };

    // Check if all pending interrupts have been responded to
    const pending = pendingInterruptIdsRef.current;
    const collected = collectedHitlResponsesRef.current;
    if (pending.size > 0 && [...pending].every((id) => collected[id])) {
      const batchedResponse = { ...collected };
      resumeWithHitlResponse(batchedResponse, currentPlanModeRef.current);
    }
  }, [resumeWithHitlResponse]);

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
    collectedHitlResponsesRef.current[interruptId] = { decisions: [{ type: 'reject' }] };

    // Check if all pending interrupts have been responded to
    const pending = pendingInterruptIdsRef.current;
    const collected = collectedHitlResponsesRef.current;
    if (pending.size > 0 && [...pending].every((id) => collected[id])) {
      const batchedResponse = { ...collected };
      resumeWithHitlResponse(batchedResponse, currentPlanModeRef.current);
    }
  }, [resumeWithHitlResponse]);

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
  const handleApprovePTCAgent = useCallback((_pad?: Record<string, unknown>, overrides?: { report_back?: boolean }) => {
    if (!pendingInterrupt || pendingInterrupt.type !== 'ptc_agent') return;
    const pid = pendingInterrupt.proposalId!;

    // Track this proposal for thread_id backfill from the resumed stream's tool_call_result.
    // Maps tool_call_id → proposalId for exact matching when the result arrives.
    if (pendingInterrupt.toolCallId) {
      pendingPTCBackfillRef.current.set(pendingInterrupt.toolCallId, pid);
    }

    // Enable report-back polling if report_back is not explicitly disabled
    if (overrides?.report_back !== false) {
      awaitingReportBackRef.current = true;
    }

    resolveProposal('ptcAgentProposals', pid, 'approved');

    const decision: { type: string; message?: string; overrides?: { report_back?: boolean } } = { type: 'approve' };
    if (overrides) {
      decision.overrides = overrides;
    }
    resumeWithHitlResponse({ [pendingInterrupt.interruptId!]: { decisions: [decision] } }, false);
  }, [pendingInterrupt, resumeWithHitlResponse, resolveProposal]);

  const handleRejectPTCAgent = useCallback(() => {
    if (!pendingInterrupt || pendingInterrupt.type !== 'ptc_agent') return;
    resolveProposal('ptcAgentProposals', pendingInterrupt.proposalId!, 'rejected');
    resumeWithHitlResponse({ [pendingInterrupt.interruptId!]: { decisions: [{ type: 'reject' }] } }, false);
  }, [pendingInterrupt, resumeWithHitlResponse, resolveProposal]);

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
  const turnCheckpointsRef = useRef<{ turns: Array<{ edit_checkpoint_id: string | null; regenerate_checkpoint_id: string; turn_index: number }>; retry_checkpoint_id: string | null } | null>(null);

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
   * Helper: run a checkpoint-based stream (shared by edit, regenerate, retry).
   * Sets up assistant placeholder, event processor, and handles the stream lifecycle.
   */
  const streamFromCheckpoint = useCallback(async (message: string | null, checkpointId: string, truncateIndex: number, forkFromTurn: number | null = null, modelOptions: ModelOptions = {}) => {
    if (isStreamingRef.current) return;

    // Edit/regenerate/retry are chat activity — bump like a fresh send.
    bumpThreadNavOrder(workspaceId, threadIdRef.current);

    setIsLoading(true);
    setMessageError(null);
    setHasActiveSubagents(false);
    completedTaskIdsRef.current.clear();
    wasStoppedRef.current = false;
    acquireStreamOwnership(threadId);

    // Truncate messages and add new user message (if editing) + assistant placeholder
    const assistantMessageId = `assistant-${Date.now()}`;
    contentOrderCounterRef.current = 0;
    currentReasoningIdRef.current = null;
    currentToolCallIdRef.current = null;
    // Edit/regenerate opens a fresh backend run; clear the prior run_id so
    // the new metadata frame becomes the source of truth.
    currentRunIdRef.current = null;
    // Fresh AbortController so stopWorkflow can abort this stream's reader.
    const abortController = new AbortController();
    mainStreamAbortRef.current = abortController;

    const assistantMessage = createAssistantMessage(assistantMessageId);
    const userMessage = message ? createUserMessage(message) : null;

    if (userMessage) {
      recentlySentTrackerRef.current.track(message!.trim(), userMessage.timestamp, userMessage.id);
    }

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

    let wasDisconnected = false;
    const wasInterruptedRef = { current: false };
    try {
      const refs = {
        contentOrderCounterRef,
        currentReasoningIdRef,
        currentToolCallIdRef,
        steeringAtOrderRef,
        updateTodoListCard: updateTodoListCard || undefined,
        isNewConversation: false,
        subagentStateRefs: subagentStateRefsRef.current,
        updateSubagentCard: updateSubagentCard || (() => {}),
      };
      const processEvent = createStreamEventProcessor(assistantMessageId, refs, getTaskIdFromEvent, wasInterruptedRef);

      const result = await sendChatMessageStream(
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
        // Latch run_id from response headers — see handleSendMessage for the
        // same closing-the-race rationale.
        (runId) => {
          currentRunIdRef.current = runId;
        },
        abortController.signal,
      );

      // User hit stop: stopWorkflow already finalized + tore down.
      if (result?.aborted || wasStoppedRef.current) {
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
    } catch (err: unknown) {
      if ((err as Error)?.name === 'AbortError' || wasStoppedRef.current) {
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
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [workspaceId, threadId, agentMode]);

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
   * Retry the last failed/errored turn from the latest checkpoint.
   */
  const handleRetry = useCallback(async (modelOptions: ModelOptions = {}) => {
    const turnsData = await getTurnCheckpoints();
    const checkpointId = turnsData?.retry_checkpoint_id;
    if (!checkpointId) {
      setMessageError('Unable to retry: no checkpoint available');
      return;
    }

    if (!turnsData.turns?.length) {
      setMessageError('Unable to retry: checkpoint data unavailable');
      return;
    }

    // Find the last error message and truncate from there
    const lastErrorIndex = messages.findLastIndex((m) => m.role === 'assistant' && (m as AssistantMessage).error);
    const truncateIndex = lastErrorIndex !== -1 ? lastErrorIndex : messages.length;

    // Retry overwrites the last turn
    const forkFromTurn = turnsData.turns.length - 1;
    await streamFromCheckpoint(null, checkpointId, truncateIndex, forkFromTurn, modelOptions);
  }, [messages, getTurnCheckpoints, streamFromCheckpoint]);

  // ==================== Feedback ====================

  const deriveTurnIndex = useCallback((messageId: string): number => {
    const msgIndex = messages.findIndex(m => m.id === messageId);
    if (msgIndex === -1) return -1;
    return messages.slice(0, msgIndex + 1).filter(m => m.role === 'assistant' && !m.isSteering).length - 1;
  }, [messages]);

  const handleThumbUp = useCallback(async (messageId: string) => {
    const turnIndex = deriveTurnIndex(messageId);
    if (turnIndex === -1) return null;

    const existing = feedbackMapRef.current[turnIndex];
    try {
      if (existing?.rating === 'thumbs_up') {
        await removeFeedback(threadId, turnIndex);
        delete feedbackMapRef.current[turnIndex];
        return { rating: null };
      } else {
        const result = await submitFeedback(threadId, turnIndex, 'thumbs_up');
        feedbackMapRef.current[turnIndex] = result;
        return { rating: 'thumbs_up' };
      }
    } catch (e) {
      console.error('[Feedback] Error:', e);
      return null;
    }
  }, [deriveTurnIndex, threadId]);

  const handleThumbDown = useCallback(async (messageId: string, issueCategories: string[], comment: string | null, consentHumanReview: boolean) => {
    const turnIndex = deriveTurnIndex(messageId);
    if (turnIndex === -1) return null;

    try {
      const result = await submitFeedback(threadId, turnIndex, 'thumbs_down', issueCategories, comment, consentHumanReview);
      feedbackMapRef.current[turnIndex] = result;
      return { rating: 'thumbs_down' };
    } catch (e) {
      console.error('[Feedback] Error:', e);
      return null;
    }
  }, [deriveTurnIndex, threadId]);

  const getFeedbackForMessage = useCallback((messageId: string) => {
    const turnIndex = deriveTurnIndex(messageId);
    if (turnIndex === -1) return null;
    return feedbackMapRef.current[turnIndex] || null;
  }, [deriveTurnIndex]);

  return {
    messages,
    threadId,
    threadModels,
    lastThreadModel,
    isLoading,
    hasActiveSubagents,
    workspaceStarting,
    isCompacting,
    setIsCompacting,
    queuedSend,
    isLoadingHistory,
    isReconnecting,
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
