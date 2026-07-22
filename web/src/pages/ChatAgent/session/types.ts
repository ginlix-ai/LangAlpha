/**
 * Session-level types and interrupt tables for the chat message engine.
 * Extracted verbatim from useChatMessages module scope (W1); useChatMessages
 * remains the public composition root and re-exports the public ones.
 */

import type React from 'react';
import type { ChatMessage } from '@/types/chat';
import type { ActionRequest, ToolCallData } from '@/types/sse';
import type { SubagentTokenUsage } from '../utils/tokenUsage';

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

/**
 * Transient model-resilience status surfaced as a pill above the chat input
 * during streaming: the provider is retrying the current model, or has fallen
 * back to a secondary. Cleared on the first content/tool event, on error, on
 * stream end, and on stop.
 */
export type ModelStatus =
  | { kind: 'retrying'; model: string; attempt: number; maxRetries: number }
  | { kind: 'fallback'; fromModel: string; toModel: string };

/**
 * Suggestion surfaced as a pill above the chat input after a turn was
 * answered by a fallback model: the user-configured `fromModel` had trouble;
 * offer switching to `toModel`, the model that actually answered.
 */
export interface FallbackSuggestion {
  fromModel: string;
  toModel: string;
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
  /** Ledger failure reason, present only for an errored task. Surfaced in the
   *  detail view header so a "Failed" card explains why. */
  error?: string;
  toolCalls: number;
  tokenUsage: SubagentTokenUsage;
  currentTool: string;
  /** Start (epoch ms) of the newest run whose transcript the history
   *  projection contained — the run-level watermark the mux drain guard
   *  filters against. */
  projectedRunStartedMs?: number;
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
  /** Backend-stamped real task status from replayed task artifacts (running|completed|cancelled). */
  status?: string;
  /** Backend-stamped ledger failure reason, present only for an errored task. */
  error?: string;
  /** Build-time stamp: start (epoch ms) of the newest run whose transcript
   *  the projection actually claimed — NOT the ledger's latest run, which
   *  can still be executing and deliberately excluded from the payload. */
  projectedRunStartedMs?: number;
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



export type {
  MessageRecord, SetMessages, TokenUsage, PendingInterrupt, PendingRejection,
  SSEEvent, ModelOptions, OffloadBatch, ContextWindowCallbacks,
  SubagentHistoryEntry, TaskRefs, HistoryInterruptInfo, SubagentHistoryData,
  StreamProcessorRefs, PairState,
};
