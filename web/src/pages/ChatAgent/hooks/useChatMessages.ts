/**
 * Custom hook for managing chat messages and streaming
 * 
 * Handles:
 * - Message state management
 * - Thread ID management (persisted per workspace)
 * - Message sending with SSE streaming
 * - Conversation history loading
 * - Streaming updates and error handling
 * 
 * @param {string} workspaceId - The workspace ID for the chat session
 * @param {string} [initialThreadId] - Optional initial thread ID (from URL params)
 * @returns {Object} Message state and handlers
 */

import type React from 'react';
import { useState, useRef, useEffect, useCallback } from 'react';
import { useTranslation } from 'react-i18next';
import { sendChatMessageStream, replayThreadHistory, getWorkflowStatus, reconnectToWorkflowStream, sendHitlResponse, streamSubagentTaskEvents, fetchThreadTurns, submitFeedback, removeFeedback, getThreadFeedback } from '../utils/api';
import { getStoredThreadId, setStoredThreadId } from './utils/threadStorage';
export { removeStoredThreadId } from './utils/threadStorage';
import { createUserMessage, createAssistantMessage, createNotificationMessage, appendMessage, updateMessage, type AttachmentMeta } from './utils/messageHelpers';
import type { ChatMessage, AssistantMessage } from '@/types/chat';
import type { ActionRequest, ToolCallData, TodoItem } from '@/types/sse';
import type { PreviewData } from './utils/types';
import { createRecentlySentTracker } from './utils/recentlySentTracker';
import {
  handleReasoningSignal,
  handleReasoningContent,
  handleTextContent,
  handleToolCalls,
  handleToolCallResult,
  handleToolCallChunks,
  handleTodoUpdate,
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
  handleHistorySteeringDelivered,
  isSubagentHistoryEvent,
} from './utils/historyEventHandlers';

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
  [key: string]: unknown;
}

/** Workflow status response. */
interface WorkflowStatusResponse {
  can_reconnect: boolean;
  status: string;
  active_tasks?: string[];
  is_shared?: boolean;
  [key: string]: unknown;
}

/** Model options for send/edit/regenerate. */
interface ModelOptions {
  model?: string | null;
  reasoningEffort?: string | null;
  fastMode?: boolean | null;
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
  insertNotification: (text: string) => void;
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
    if (setIsCompacting && event.signal === 'start') {
      setIsCompacting('summarize');
      return;
    }
    if (setIsCompacting) setIsCompacting(false);
    if (event.signal === 'complete') {
      const text = t('chat.summarizedNotification', { from: event.original_message_count });
      const msgId = getMsgId();
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
    const hasIncomplete = lastVal.todos?.some(
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
) {
  const { t } = useTranslation();
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
  const [workspaceStarting, setWorkspaceStarting] = useState(false);  // Workspace is starting up (stopped/archived sandbox)
  const [isCompacting, setIsCompacting] = useState<string | false>(false);  // Context compaction in progress (summarization/offload)
  const [messageError, setMessageError] = useState<string | null>(null);
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

  // Refs for history loading state
  const historyLoadingRef = useRef(false);
  const historyMessagesRef = useRef(new Set<string>()); // Track message IDs from history
  const newMessagesStartIndexRef = useRef(0); // Index where new messages start

  // Track all LLM models used in this thread (ordered, deduplicated)
  const [threadModels, setThreadModels] = useState<string[]>([]);

  // Track if streaming is in progress to prevent history loading during streaming
  const isStreamingRef = useRef(false);

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

  // Track the last received SSE event ID for reconnection
  const lastEventIdRef = useRef<number | string | null>(null);
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
      const currentThreadId = threadId;
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
        // Reset refs
        contentOrderCounterRef.current = 0;
        currentReasoningIdRef.current = null;
        currentToolCallIdRef.current = null;
        steeringAtOrderRef.current = null;
        historyLoadingRef.current = false;
        historyMessagesRef.current.clear();
        newMessagesStartIndexRef.current = 0;
        recentlySentTrackerRef.current.clear();
      }
    }
  }, [workspaceId, initialThreadId]);

  /**
   * Loads conversation history for the current workspace and thread
   * Uses the threadId from state (which should be a valid thread ID, not '__default__')
   */
  const loadConversationHistory = async () => {
    if (!workspaceId || !threadId || threadId === '__default__' || historyLoadingRef.current) {
      return;
    }

    try {
      historyLoadingRef.current = true;
      historyHasUnresolvedInterruptRef.current = false;
      setIsLoadingHistory(true);
      setMessageError(null);

      const threadIdToUse = threadId;
      console.log('[History] Loading history for thread:', threadIdToUse);

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

        // Track last event ID so reconnectToStream can deduplicate
        if (event._eventId != null) {
          lastEventIdRef.current = event._eventId;
        }

        // Check if this is a subagent event - filter it out from main chat view
        const isSubagent = isSubagentHistoryEvent(event as Record<string, unknown>);

        // Update current active pair when we see an event with turn_index
        if (hasPairIndex) {
          const pairIndex = event.turn_index!;
          currentActivePairIndex = pairIndex;
          currentActivePairState = pairStateByPair.get(pairIndex);
          console.log('[History] Updated active pair to:', pairIndex, 'counter:', currentActivePairState?.contentOrderCounter);
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
        if (eventType === 'artifact') {
          const artifactType = event.artifact_type;
          if (artifactType === 'todo_update') {
            const payload = event.payload || {};

            // Update floating todo card from history (last event wins, shows final state)
            if (updateTodoListCard) {
              updateTodoListCard({
                todos: payload.todos || [],
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

              console.log('[History] Processing todo_update artifact for pair:', pairIndex, 'counter:', pairState.contentOrderCounter);
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
              // Map tool_call_id from the event context
              if (event.tool_call_id) {
                toolCallIdToTaskIdMapRef.current.set(event.tool_call_id, agentId);
              }
              // Match pending tool call IDs from earlier tool_calls events.
              // The artifact 'spawned' event drains the pending queue to
              // establish the toolCallId → agentId mapping for replay.
              if (action === 'init') {
                const pendingToolCallIds = historyPendingTaskToolCallIdsRef.current;
                if (pendingToolCallIds.length > 0) {
                  const toolCallId = pendingToolCallIds[0];
                  if (!toolCallIdToTaskIdMapRef.current.has(toolCallId)) {
                    toolCallIdToTaskIdMapRef.current.set(toolCallId, agentId);
                  }
                  historyPendingTaskToolCallIdsRef.current = pendingToolCallIds.slice(1);
                }
              }
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

          // Resolve pending create_workspace or start_question interrupt from tool_call_result
          {
            const idx = pendingHistoryInterrupts.findIndex((p) => p.type === 'create_workspace' || p.type === 'start_question');
            if (idx !== -1 && typeof event.content === 'string') {
              const matched = pendingHistoryInterrupts[idx];
              const content = event.content;
              const dataKey = matched.type === 'create_workspace' ? 'workspaceProposals' : 'questionProposals';

              let resolvedStatus = 'approved';
              if (content === 'User declined workspace creation.' || content === 'User declined starting the question.') {
                resolvedStatus = 'rejected';
              } else {
                try {
                  const parsed = JSON.parse(content);
                  if (parsed?.success === false) resolvedStatus = 'rejected';
                } catch { /* non-JSON → treat as approved */ }
              }

              const pKey = matched.proposalId!;
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
            console.log('[History] Final thread_id event:', event.thread_id);
            setThreadId(event.thread_id);
            setStoredThreadId(workspaceId, event.thread_id);
          }
        } else if (eventType === 'credit_usage') {
          // credit_usage indicates the end of one conversation pair
          console.log('[History] Credit usage event (end of pair):', event.turn_index);
        } else if (!eventType) {
          // Fallback: Handle events without event type
          if (event.thread_id && !hasRole && !contentType) {
            console.log('[History] Fallback: thread_id only event:', event.thread_id);
            if (event.thread_id !== threadId && event.thread_id !== '__default__') {
              setThreadId(event.thread_id);
              setStoredThreadId(workspaceId, event.thread_id);
            }
          }
        } else {
          // Log unhandled event types for debugging
          console.log('[History] Unhandled event type:', {
            eventType,
            contentType,
            hasRole,
            role: event.role,
            hasPairIndex,
          });
        }
      });

        console.log('[History] Replay completed');

        // If there's still a pending interrupt after replay (no subsequent user_message
        // resolved it), store it in a ref. loadAndMaybeReconnect will decide whether to
        // make it interactive (workflow paused) or reconnect to get resolution events
        // (workflow active = interrupt was answered but resolution is in Redis buffer).
        if (pendingHistoryInterrupts.length > 0) {
          console.log('[History] Unresolved interrupts detected:', pendingHistoryInterrupts.length, pendingHistoryInterrupts.map((p) => p.type));
          historyHasUnresolvedInterruptRef.current = true;
          unresolvedHistoryInterruptRef.current = pendingHistoryInterrupts.map((p) => ({ ...p }));
          pendingHistoryInterrupts.length = 0;
        }

        // Process stored subagent events and build their messages
        // NOTE: During history replay we DO NOT open floating cards automatically.
        // We only build per-task message history here; cards are created lazily
        // when the user clicks \"Open subagent details\" in the main chat view.
        if (subagentHistoryByTaskId.size > 0) {
          console.log('[History] Processing subagent history for', subagentHistoryByTaskId.size, 'tasks');
          
          // Process each subagent's events
          for (const [taskId, subagentHistory] of subagentHistoryByTaskId.entries()) {
            // Create temporary refs structure for processing
            let currentRunIndex = 0;
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
            console.log('[History] Processing', subagentHistory.events.length, 'events for task:', taskId, 'resumePoints:', resumePoints.length);
            for (let i = 0; i < subagentHistory.events.length; i++) {
              const event = subagentHistory.events[i];
              const eventType = event.event;
              const contentType = event.content_type;

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

                console.log('[History] Resume boundary detected at turn_index:', eventTurnIndex, 'runIndex:', currentRunIndex);
              }
              if (eventTurnIndex != null) {
                lastTurnIndex = eventTurnIndex;
              }

              // Use per-run assistant message ID
              const assistantMessageId = `subagent-${taskId}-assistant-${currentRunIndex}`;

              console.log('[History] Processing subagent event', i + 1, 'of', subagentHistory.events.length, ':', {
                taskId,
                eventType,
                contentType,
                hasContent: !!event.content,
                hasToolCalls: !!event.tool_calls,
                toolCallId: event.tool_call_id,
              });

              if (eventType === 'message_chunk' && event.role === 'assistant') {
                const result = handleSubagentMessageChunk({
                  taskId,
                  assistantMessageId,
                  contentType: contentType as string,
                  content: event.content as string,
                  finishReason: event.finish_reason,
                  refs: tempRefs,
                  updateSubagentCard: historyUpdateSubagentCard,
                });
                console.log('[History] handleSubagentMessageChunk result:', result);
              } else if (eventType === 'tool_calls' && event.tool_calls) {
                const result = handleSubagentToolCalls({
                  taskId,
                  assistantMessageId,
                  toolCalls: event.tool_calls as unknown as Record<string, unknown>[],
                  refs: tempRefs,
                  updateSubagentCard: historyUpdateSubagentCard,
                });
                console.log('[History] handleSubagentToolCalls result:', result);
              } else if (eventType === 'tool_call_result') {
                const result = handleSubagentToolCallResult({
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
                console.log('[History] handleSubagentToolCallResult result:', result);
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
                  // Skip — no per-subagent token display
                } else {
                  let text;
                  if (action === 'summarize' && event.signal === 'complete') {
                    text = t('chat.summarizedNotification', { from: event.original_message_count });
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
                        taskRefsLocal.messages[msgIdx] = { ...aMsg, contentSegments: [...(aMsg.contentSegments || []), { type: 'notification' as const, content: text, order }] } as unknown as Record<string, unknown>;
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
              toolCalls: 0,
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

            console.log('[History] Stored subagent history for task:', taskId, 'with', finalMessages.length, 'messages, runIndex:', currentRunIndex);
          }
        }
      } catch (replayError: unknown) {
        // Handle 404 gracefully - it's expected for brand new threads that haven't been fully initialized yet
        if ((replayError as Error).message && (replayError as Error).message.includes('404')) {
          console.log('[History] Thread not found (404) - this is normal for new threads, skipping history load');
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
    } catch (error: unknown) {
      console.error('[History] Error loading conversation history:', error);
      // Only show error if it's not a 404 (404 is expected for new threads)
      if (!(error as Error).message || !(error as Error).message.includes('404')) {
        setMessageError((error as Error).message || 'Failed to load conversation history');
      }
      setIsLoadingHistory(false);
      historyLoadingRef.current = false;
    }
  };

  /**
   * Reconnects to an in-progress workflow stream after page refresh.
   * Creates an assistant message placeholder and processes live SSE events.
   */
  const reconnectToStream = async ({ activeTasks = [] }: { activeTasks?: string[] } = {}) => {
    if (!threadId || threadId === '__default__') return;

    console.log('[Reconnect] Starting reconnection for thread:', threadId);

    // Clear subagent cards to prevent duplicate content from cache + Redis overlap
    if (clearSubagentCards) {
      clearSubagentCards();
    }
    completedTaskIdsRef.current.clear();

    setIsLoading(true);
    setIsReconnecting(true);
    isStreamingRef.current = true;

    // Create assistant message placeholder for reconnection
    const assistantMessageId = `assistant-reconnect-${Date.now()}`;
    contentOrderCounterRef.current = 0;
    currentReasoningIdRef.current = null;
    currentToolCallIdRef.current = null;

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

    try {
      // Replay buffered events first — this processes artifact{task,spawned} events
      // which create subagent cards with the correct description/type. Per-task streams
      // are opened AFTER so they merge into existing cards instead of creating empty ones.
      const result = await reconnectToWorkflowStream(threadId, lastEventIdRef.current as number | null, processEvent);
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
            updateSubagentCard(agentId, {
              agentId,
              displayId: `Task-${taskId}`,
              taskId: agentId,
              description: historyData.description || '',
              prompt: historyData.prompt || historyData.description || '',
              type: historyData.type || 'general-purpose',
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
      // 404/410 = workflow no longer available, not a real error
      const status = (err as Error).message?.match(/status:\s*(\d+)/)?.[1];
      if (status === '404' || status === '410') {
        console.log('[Reconnect] Workflow no longer available (', status, '), cleaning up');
      } else {
        console.error('[Reconnect] Error during reconnection:', err);
        setMessageError((err as Error).message || 'Failed to reconnect to stream');
      }
    } finally {
      setIsReconnecting(false);

      // Clean up empty reconnect messages (no content segments = nothing was streamed)
      setMessages((prev) => {
        const msg = prev.find((m) => m.id === assistantMessageId);
        if (msg && msg.role === 'assistant' && (!(msg as AssistantMessage).contentSegments || (msg as AssistantMessage).contentSegments.length === 0) && !msg.content) {
          return prev.filter((m) => m.id !== assistantMessageId);
        }
        return prev;
      });

      if (!wasInterruptedRef.current) {
        cleanupAfterStreamEnd(assistantMessageId);
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

  // Load history when workspace or threadId changes, then check for reconnection
  useEffect(() => {
    console.log('[History] useEffect triggered, workspaceId:', workspaceId, 'threadId:', threadId, 'isStreaming:', isStreamingRef.current);

    // Guard: Only load if we have a workspaceId and a valid threadId (not '__default__')
    // Also skip if streaming is in progress (prevents race condition when thread ID changes during streaming)
    if (!workspaceId || !threadId || threadId === '__default__' || historyLoadingRef.current || isStreamingRef.current) {
      console.log('[History] Skipping load:', {
        workspaceId,
        threadId,
        isLoading: historyLoadingRef.current,
        isStreaming: isStreamingRef.current,
        reason: !workspaceId ? 'no workspaceId' :
          !threadId ? 'no threadId' :
            threadId === '__default__' ? 'default thread' :
              historyLoadingRef.current ? 'already loading' :
                isStreamingRef.current ? 'streaming in progress' :
                  'unknown'
      });
      return;
    }

    let cancelled = false;

    const loadAndMaybeReconnect = async () => {
      console.log('[History] Calling loadConversationHistory for thread:', threadId);

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

      await loadConversationHistory();

      if (cancelled) return;

      if (historyHasUnresolvedInterruptRef.current && status.can_reconnect) {
        // Workflow is active → interrupt was answered, reconnect will deliver resolution
        console.log('[Reconnect] Unresolved interrupt from history, reconnecting to get resolution events');
        historyHasUnresolvedInterruptRef.current = false;
        await reconnectToStream({ activeTasks: status.active_tasks || [] });
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
        await reconnectToStream({ activeTasks: status.active_tasks || [] });
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
            updateSubagentCard(agentId, {
              agentId,
              displayId: `Task-${taskId}`,
              taskId: agentId,
              description: historyData.description || '',
              prompt: historyData.prompt || historyData.description || '',
              type: historyData.type || 'general-purpose',
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
      }
    };

    loadAndMaybeReconnect();

    // Cleanup: Cancel loading if workspace or thread changes or component unmounts
    return () => {
      console.log('[History] Cleanup: canceling history load for workspace:', workspaceId, 'thread:', threadId);
      cancelled = true;
      historyLoadingRef.current = false;
      closeAllSubagentStreams();
      subagentStateRefsRef.current = {};
    };
    // Note: loadConversationHistory is not in deps because it uses workspaceId and threadId from closure
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [workspaceId, threadId, reloadTrigger]);

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
        // If this was the last open stream, clean up
        if (subagentStreamsRef.current.size === 0) {
          setHasActiveSubagents(false);
          if (inactivateAllSubagents) inactivateAllSubagents();
          markAllSubagentTasksCompleted();
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
      if (process.env.NODE_ENV === 'development') {
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
    isStreamingRef.current = false;

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

      // Debug: Log all events to see what we're receiving
      if (event.artifact_type || eventType === 'artifact') {
        console.log('[Stream] Artifact event detected:', { eventType, event, artifact_type: event.artifact_type });
      }

      // Update thread_id if provided in the event (ref = synchronous for closures)
      if (event.thread_id && event.thread_id !== '__default__') {
        threadIdRef.current = event.thread_id;
        if (event.thread_id !== threadId) {
          setThreadId(event.thread_id);
          setStoredThreadId(workspaceId, event.thread_id);
        }
      }

      // Handle workspace_status events (workspace starting/ready)
      if (eventType === 'workspace_status') {
        setWorkspaceStarting(event.status === 'starting');
        return;
      }

      // Check if this is a subagent event - filter it out from main chat view
      const isSubagent = isSubagentEvent(event);

      // Debug: Log subagent event detection
      if (process.env.NODE_ENV === 'development' && isSubagent) {
        console.log('[Stream] Subagent event detected:', {
          eventType,
          agent: event.agent,
          id: event.id,
          content_type: event.content_type,
        });
      }

      // Handle steering_accepted events for the MAIN agent (user sent a message while agent streams).
      // Subagent steering_accepted events are handled below in the isSubagent block.
      if (eventType === 'steering_accepted' && !isSubagent) {
        // Record the content order counter so we can roll back leaked content
        // when steering_delivered arrives (see handler below).
        steeringAtOrder = refs.contentOrderCounterRef.current;
        if (refs.steeringAtOrderRef) refs.steeringAtOrderRef.current = refs.contentOrderCounterRef.current;
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

            // If no snapshot, just finalize (mark all in-progress processes as complete)
            if (effectiveSteeringAtOrder === null) {
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

            // Keep only segments at or before the steering point
            const keptSegments = (aMsg.contentSegments || []).filter(
              (s) => s.order <= effectiveSteeringAtOrder
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

        // 3. Create new assistant message placeholder
        const newAssistantId = `assistant-${Date.now()}`;
        const newAssistant = createAssistantMessage(newAssistantId);
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
          if (taskId && event.action !== 'token_usage') {
            const action = event.action;
            let text;
            if (action === 'summarize' && event.signal === 'complete') {
              text = t('chat.summarizedNotification', { from: event.original_message_count });
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
                updatedMessages[msgIdx] = { ...existingMsg, contentSegments: [...segs, { type: 'notification', content: text, order }] };
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

            if (process.env.NODE_ENV === 'development') {
              console.log('[Stream] Subagent tool_call_result event:', {
                taskId,
                assistantMessageId: subagentAssistantMessageId,
                toolCallId,
                eventId: event.id,
                hasContent: !!event.content,
              });
            }

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
            if (process.env.NODE_ENV === 'development') {
              console.log('[Stream] Filtering out subagent artifact event:', {
                artifactType: event.artifact_type,
                taskId,
                agent: event.agent,
              });
            }
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
        setMessageError(errorMessage);
        setMessages((prev) =>
          updateMessage(prev,assistantMessageId, (msg) => ({
            ...msg,
            content: msg.content || errorMessage,
            contentType: 'text',
            isStreaming: false,
            error: true,
          }))
        );
      } else if (eventType === 'tool_call_chunks') {
        handleToolCallChunks({
          assistantMessageId,
          chunks: (event.tool_call_chunks || []) as unknown as Record<string, unknown>[],
          setMessages: setMessagesForHandlers,
        });
        return;
      } else if (eventType === 'artifact') {
        const artifactType = event.artifact_type as string;
        console.log('[Stream] Received artifact event:', { artifactType, artifactId: event.artifact_id, payload: event.payload });
        if (artifactType === 'todo_update') {
          console.log('[Stream] Processing todo_update artifact for assistant message:', assistantMessageId);
          const result = handleTodoUpdate({
            assistantMessageId,
            artifactType,
            artifactId: event.artifact_id as string,
            payload: event.payload || {},
            refs,
            setMessages: setMessagesForHandlers,
            eventId: event._eventId as number,
          });
          console.log('[Stream] handleTodoUpdate result:', result);
        } else if (artifactType === 'file_operation' && onFileArtifact) {
          onFileArtifact(event);
        } else if (artifactType === 'preview_url' && onPreviewUrl) {
          const payload = (event.payload || {}) as Record<string, unknown>;
          onPreviewUrl({
            url: '',  // resolved by ChatView via authenticated endpoint
            port: payload.port as number,
            title: payload.title as string | undefined,
            command: payload.command as string | undefined,
            loading: true,
          });
        } else if (artifactType === 'task') {
          const payload = (event.payload || {}) as Record<string, unknown>;
          const { task_id, action: rawAction, description, prompt, type } = payload;
          const action = (() => { if (rawAction === 'spawned') return 'init'; if (rawAction === 'steering_accepted') return 'update'; if (rawAction === 'resumed') return 'resume'; return rawAction || 'init'; })() as string;
          if (!task_id) return;
          const agentId = `task:${task_id}`;

          if (action === 'init') {
            // Drain pending Task tool call ID to establish toolCallId → agentId mapping
            // immediately, so clicking the inline card before tool_call_result resolves correctly
            if (pendingTaskToolCallIds.length > 0) {
              const toolCallId = pendingTaskToolCallIds.shift()!;
              toolCallIdToTaskIdMapRef.current.set(toolCallId, agentId);
            }
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

          // Try create_workspace / start_question
          const matchIdx = unresolvedList.findIndex((u: HistoryInterruptInfo) => u.type === 'create_workspace' || u.type === 'start_question');
          if (matchIdx !== -1) {
            const matched = unresolvedList[matchIdx];
            const dataKey = matched.type === 'create_workspace' ? 'workspaceProposals' : 'questionProposals';
            let resolvedStatus = 'approved';
            if (content === 'User declined workspace creation.' || content === 'User declined starting the question.') {
              resolvedStatus = 'rejected';
            } else {
              try { if (JSON.parse(content)?.success === false) resolvedStatus = 'rejected'; } catch { /* not JSON */ }
            }
            const proposalId = matched.proposalId!;
            setMessages((prev) =>
              updateMessage(prev,matched.assistantMessageId, (m) => { if (m.role !== 'assistant') return m; const msg = m as AssistantMessage; return {
                ...msg,
                [dataKey]: {
                  ...(msg[dataKey] || {}),
                  [proposalId]: {
                    ...(msg[dataKey]?.[proposalId] || {}),
                    status: resolvedStatus,
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
          if (process.env.NODE_ENV === 'development') {
            console.log('[Stream] Mapped toolCallId to agentId from artifact:', {
              toolCallId,
              agentId,
              description: event.artifact.description,
            });
          }
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
        isStreamingRef.current = false;
        currentMessageRef.current = null;
        if (wasInterruptedRef) wasInterruptedRef.current = true;
      }
    };

    return processEvent;
  };

  /**
   * Handles sending a message while the agent is already streaming (steering).
   * The backend will accept it for injection before the next LLM call.
   */
  const handleSendSteering = async (message: string, planMode: boolean = false, additionalContext: Record<string, unknown>[] | null = null, attachmentMeta: Record<string, unknown>[] | null = null) => {
    // Show user message in chat with steering indicator
    const userMsg = createUserMessage(message, attachmentMeta as AttachmentMeta[] | null);
    const userMessage: MessageRecord = { ...userMsg, steering: true };
    recentlySentTrackerRef.current.track(message.trim(), userMessage.timestamp, userMessage.id);
    setMessages((prev) => appendMessage(prev,userMessage));

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
            // Snapshot the content order counter so the primary stream's
            // steering_delivered handler can roll back leaked content.
            steeringAtOrderRef.current = contentOrderCounterRef.current;
            // Update the user message to reflect steering status
            setMessages((prev) =>
              updateMessage(prev,userMessage.id as string, (msg) => ({
                ...msg,
                steering: true,
                queuePosition: event.position,
              }))
            );
          }
        },
        additionalContext,
        agentMode
      );
    } catch (err: unknown) {
      console.error('Error sending steering:', err);
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

  /**
   * Handles sending a message and streaming the response
   *
   * @param {string} message - The user's message
   * @param {boolean} planMode - Whether to use plan mode
   * @param {Array|null} additionalContext - Optional additional context for skill loading
   * @param {Array|null} attachmentMeta - Optional attachment metadata for user message display
   */
  const handleSendMessage = async (message: string, planMode: boolean = false, additionalContext: Record<string, unknown>[] | null = null, attachmentMeta: Record<string, unknown>[] | null = null, { model, reasoningEffort, fastMode }: ModelOptions = {}) => {
    const hasContent = message.trim() || (additionalContext && additionalContext.length > 0);
    if (!workspaceId || !hasContent) {
      return;
    }

    // If agent is already streaming, send as steering message
    if (isLoading) {
      return handleSendSteering(message, planMode, additionalContext, attachmentMeta);
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
    const userMessage = createUserMessage(message, attachmentMeta as AttachmentMeta[] | null);
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
    // Mark streaming as in progress to prevent history loading during streaming
    isStreamingRef.current = true;

    // Create assistant message placeholder
    const assistantMessageId = `assistant-${Date.now()}`;
    // Reset counters for this new message
    contentOrderCounterRef.current = 0;
    currentReasoningIdRef.current = null;
    currentToolCallIdRef.current = null;

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
        undefined, undefined, undefined, undefined,
        model || null,
        reasoningEffort || null,
        fastMode || null
      );

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
          // Handle rate limit (429) — show limit message and remove optimistic assistant message
          const errObj = err as Record<string, unknown>;
          if (errObj.status === 429) {
            const info = (errObj.rateLimitInfo || {}) as Record<string, unknown>;
            const limitMsg = info.type === 'credit_limit'
              ? `Daily credit limit reached (${info.used_credits}/${info.credit_limit} credits). Resets at midnight UTC.`
              : info.type === 'workspace_limit'
                ? `Active workspace limit reached (${info.current}/${info.limit}).`
                : (info.message as string) || 'Rate limit exceeded. Please try again later.';
            setMessageError(limitMsg);
            setMessages((prev) => prev.filter((m) => m.id !== assistantMessageId));
          } else {
            console.error('Error sending message:', err);
            setMessageError((err as Error).message || 'Failed to send message');
            setMessages((prev) =>
              updateMessage(prev,assistantMessageId, (msg) => ({
                ...msg,
                content: msg.content || 'Failed to send message. Please try again.',
                isStreaming: false,
                error: true,
              }))
            );
          }
        } finally {
          if (!wasDisconnected && !wasInterruptedRef.current) {
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

    const assistantMessage = createAssistantMessage(assistantMessageId);
    setMessages((prev) => appendMessage(prev, assistantMessage));
    currentMessageRef.current = assistantMessageId;

    setIsLoading(true);
    setMessageError(null);
    isStreamingRef.current = true;

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
        agentMode
      );

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
      if (!wasDisconnected && !wasInterruptedRef.current) {
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

  const handleApproveCreateWorkspace = useCallback(() => {
    if (!pendingInterrupt || pendingInterrupt.type !== 'create_workspace') return;
    const { interruptId, proposalId } = pendingInterrupt;
    const pid = proposalId!;

    setMessages((prev) =>
      prev.map((m) => {
        if (m.role !== 'assistant') return m;
        const msg = m as AssistantMessage;
        if (!msg.workspaceProposals?.[pid]) return m;
        return {
          ...msg,
          workspaceProposals: {
            ...msg.workspaceProposals,
            [pid]: {
              ...msg.workspaceProposals[pid],
              status: 'approved',
            },
          },
        };
      })
    );

    const hitlResponse = {
      [interruptId!]: { decisions: [{ type: 'approve' }] },
    };
    resumeWithHitlResponse(hitlResponse, false);
  }, [pendingInterrupt, resumeWithHitlResponse]);

  const handleRejectCreateWorkspace = useCallback(() => {
    if (!pendingInterrupt || pendingInterrupt.type !== 'create_workspace') return;
    const { interruptId, proposalId } = pendingInterrupt;
    const pid = proposalId!;

    setMessages((prev) =>
      prev.map((m) => {
        if (m.role !== 'assistant') return m;
        const msg = m as AssistantMessage;
        if (!msg.workspaceProposals?.[pid]) return m;
        return {
          ...msg,
          workspaceProposals: {
            ...msg.workspaceProposals,
            [pid]: {
              ...msg.workspaceProposals[pid],
              status: 'rejected',
            },
          },
        };
      })
    );

    const hitlResponse = {
      [interruptId!]: { decisions: [{ type: 'reject' }] },
    };
    resumeWithHitlResponse(hitlResponse, false);
  }, [pendingInterrupt, resumeWithHitlResponse]);

  const handleApproveStartQuestion = useCallback(() => {
    if (!pendingInterrupt || pendingInterrupt.type !== 'start_question') return;
    const { interruptId, proposalId } = pendingInterrupt;
    const pid = proposalId!;

    setMessages((prev) =>
      prev.map((m) => {
        if (m.role !== 'assistant') return m;
        const msg = m as AssistantMessage;
        if (!msg.questionProposals?.[pid]) return m;
        return {
          ...msg,
          questionProposals: {
            ...msg.questionProposals,
            [pid]: {
              ...msg.questionProposals[pid],
              status: 'approved',
            },
          },
        };
      })
    );

    const hitlResponse = {
      [interruptId!]: { decisions: [{ type: 'approve' }] },
    };
    resumeWithHitlResponse(hitlResponse, false);
  }, [pendingInterrupt, resumeWithHitlResponse]);

  const handleRejectStartQuestion = useCallback(() => {
    if (!pendingInterrupt || pendingInterrupt.type !== 'start_question') return;
    const { interruptId, proposalId } = pendingInterrupt;
    const pid = proposalId!;

    setMessages((prev) =>
      prev.map((m) => {
        if (m.role !== 'assistant') return m;
        const msg = m as AssistantMessage;
        if (!msg.questionProposals?.[pid]) return m;
        return {
          ...msg,
          questionProposals: {
            ...msg.questionProposals,
            [pid]: {
              ...msg.questionProposals[pid],
              status: 'rejected',
            },
          },
        };
      })
    );

    const hitlResponse = {
      [interruptId!]: { decisions: [{ type: 'reject' }] },
    };
    resumeWithHitlResponse(hitlResponse, false);
  }, [pendingInterrupt, resumeWithHitlResponse]);

  const insertNotification = useCallback((text: string, variant: 'info' | 'success' | 'warning' = 'info') => {
    setMessages((prev) => appendMessage(prev, createNotificationMessage(text, variant)));
  }, []);

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
    if (!threadId || threadId === '__default__') return null;
    try {
      const data = await fetchThreadTurns(threadId);
      turnCheckpointsRef.current = data;
      return data;
    } catch (err) {
      console.error('[useChatMessages] Failed to fetch turn checkpoints:', err);
      return null;
    }
  }, [threadId]);

  /**
   * Helper: run a checkpoint-based stream (shared by edit, regenerate, retry).
   * Sets up assistant placeholder, event processor, and handles the stream lifecycle.
   */
  const streamFromCheckpoint = useCallback(async (message: string | null, checkpointId: string, truncateIndex: number, forkFromTurn: number | null = null, modelOptions: ModelOptions = {}) => {
    if (isLoading) return;

    setIsLoading(true);
    setMessageError(null);
    setHasActiveSubagents(false);
    completedTaskIdsRef.current.clear();
    isStreamingRef.current = true;

    // Truncate messages and add new user message (if editing) + assistant placeholder
    const assistantMessageId = `assistant-${Date.now()}`;
    contentOrderCounterRef.current = 0;
    currentReasoningIdRef.current = null;
    currentToolCallIdRef.current = null;

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
        undefined, // locale
        undefined, // timezone
        checkpointId,
        forkFromTurn,
        modelOptions.model || null,
        modelOptions.reasoningEffort || null,
        modelOptions.fastMode || null
      );

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
      if (!wasDisconnected && !wasInterruptedRef.current) {
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
  }, [isLoading, workspaceId, threadId, agentMode]);

  /**
   * Edit a user message: truncate to before that message, send modified content
   * from the checkpoint before the original message was added.
   */
  const handleEditMessage = useCallback(async (messageId: string, newContent: string, modelOptions: ModelOptions = {}) => {
    if (!newContent?.trim()) return;

    const msgIndex = messages.findIndex((m) => m.id === messageId);
    if (msgIndex === -1) return;

    // Count assistant messages before this user message to get turn_index.
    // Uses assistant count because HITL resume turns create assistant bubbles
    // without user bubbles, and backend turns include HITL resumes.
    const turnIndex = messages.slice(0, msgIndex).filter((m) => m.role === 'assistant').length;

    const turnsData = await getTurnCheckpoints();
    if (!turnsData?.turns?.[turnIndex]) {
      setMessageError('Unable to edit: checkpoint data unavailable');
      return;
    }

    const checkpointId = turnsData.turns[turnIndex].edit_checkpoint_id;
    if (!checkpointId) {
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

    // Count assistant messages up to and including this one to get turn_index.
    // Uses assistant count because HITL resume turns create assistant bubbles
    // without user bubbles, and backend turns include HITL resumes.
    const turnIndex = messages.slice(0, msgIndex + 1).filter((m) => m.role === 'assistant').length - 1;

    const turnsData = await getTurnCheckpoints();
    if (!turnsData?.turns?.[turnIndex]) {
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
    return messages.slice(0, msgIndex + 1).filter(m => m.role === 'assistant').length - 1;
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
    isLoading,
    hasActiveSubagents,
    workspaceStarting,
    isCompacting,
    setIsCompacting,
    isLoadingHistory,
    isReconnecting,
    messageError,
    returnedSteering,
    clearReturnedSteering: () => setReturnedSteering(null),
    handleSendMessage,
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
