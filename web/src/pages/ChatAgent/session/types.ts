/**
 * Session-level types and interrupt tables for the chat message engine.
 * Extracted verbatim from useChatMessages module scope (W1); useChatMessages
 * remains the public composition root and re-exports the public ones.
 */

import type React from 'react';
import type { ChatMessage } from '@/types/chat';
import type { ActionRequest, ToolCallData } from '@/types/sse';
import type { ReportSource } from '@/types/api';
import type { SubagentTokenUsage } from '../utils/tokenUsage';
import type { DecisionTarget } from './interrupts/toolApprovalCard';
import type { StreamRefs, UpdateSubagentCard } from './streamRefs';
import type { RetryNotice } from './stream/autoRetry';
import type { ThreadTokenTotals } from '../utils/api/messages';

// --- Internal types for useChatMessages ---

/** React state setter for messages array. */
type SetMessages = React.Dispatch<React.SetStateAction<ChatMessage[]>>;

/** Token usage state for context window progress ring. The totals are the
 *  thread's: the server's count when it loaded, plus live calls since. The
 *  rest is the newest call's. */
interface TokenUsage {
  totalInput: number;
  totalOutput: number;
  /** Whether the totals started from the server's count. Without one they
   *  sum the calls the view has loaded, the closest it can come. */
  fromServer: boolean;
  lastOutput: number;
  total: number;
  threshold: number;
}

/** Pending HITL interrupt state. */
interface PendingInterrupt {
  type: string;
  interruptId?: string;
  assistantMessageId?: string;
  questionId?: string;
  proposalId?: string;
  toolCallId?: string;
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
  // Set by the thread mux on a frame from a channel still replaying the
  // backlog that existed when it opened; cleared by chan_caught_up.
  _replay?: boolean;
  timestamp?: string | number;
  metadata?: Record<string, unknown>;
  tool_calls?: ToolCallData[];
  tool_call_id?: string;
  tool_call_chunks?: Array<{ id?: string; name?: string; args?: string }>;
  finish_reason?: string;
  phase?: 'commentary' | 'final_answer';
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
  /** About how many tokens a summary being written has written so far. */
  summary_tokens?: number;
  offloaded_args?: number;
  offloaded_reads?: number;
  /** Discriminator carried by several event families; `order_approval` on an
   *  interrupt the order gate raised. */
  kind?: string;
  position?: number;
  active_tasks?: string[];
  can_reconnect?: boolean;
  is_shared?: boolean;
  run_id?: string;
  /** On a report-back's `metadata` frame: what it reports, for the row a live
   *  attach draws, since no user message comes with it. */
  report_source?: ReportSource;
  /** ISO instant the turn's run settled, on a terminal `user_message` only.
   *  Live turns carry none — their tail bubble is stamped when it finalizes. */
  run_completed_at?: string;
  /** The server's measured thinking time, on a `reasoning_signal` close. Both
   *  replay paths and the live stream read it, so it is stated rather than
   *  left to the index signature below. */
  elapsed_ms?: number;
  /** What the thread has used, on a `run_end` frame. */
  token_totals?: ThreadTokenTotals | null;
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
  | { kind: 'fallback'; fromModel: string; toModel: string; paidBy?: string; provider?: string };

/**
 * Suggestion surfaced as a pill above the chat input after a turn was
 * answered by a fallback model: the user-configured `fromModel` had trouble;
 * offer switching to `toModel`, the model that actually answered.
 */
export interface FallbackSuggestion {
  fromModel: string;
  toModel: string;
  /** Who paid for `toModel`'s call (`billing_type`), where the server said. */
  paidBy?: string;
  /** The provider that call went through, which names a sign-in's plan. */
  provider?: string;
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
  /**
   * The subagents setting for the thread this send creates. A send on a live
   * thread carries none: the row's PATCH is its only writer.
   */
  subagentsAllowed?: boolean;
  /**
   * The effort and fast mode the thread this send creates keeps as its own
   * (`useThreadTuning`), apart from `reasoningEffort`/`fastMode`, which are
   * this turn's. A send on a live thread carries none, for the same reason.
   */
  reasoningEffortOverride?: string;
  fastModeOverride?: boolean;
}

/** What a fresh send takes beyond the model options an edit or regenerate does. */
interface SendOptions extends ModelOptions {
  /**
   * The send opens its turn with no message of its own: the model reads only
   * its additionalContext, and the reply is the first bubble the turn shows.
   * A reload draws none for it either, since its stored query is empty.
   */
  hiddenOpener?: boolean;
  /** The title of the thread this send creates, in place of one generated
   *  from its message. A send on a live thread leaves the title alone. */
  threadTitle?: string;
}

/** Offload batch ref state. */
interface OffloadBatch {
  args: number;
  reads: number;
  timer: ReturnType<typeof setTimeout> | null;
  msgId?: string | null;
  /** What the timer will write, so a replay that ends can write it now. */
  flush?: () => void;
}

/** Callbacks for handleContextWindowEvent. */
interface ContextWindowCallbacks {
  getMsgId: () => string | null;
  nextOrder: () => number;
  setMessages: SetMessages;
  setTokenUsage: React.Dispatch<React.SetStateAction<TokenUsage | null>>;
  /** A live call adds to the totals; a replayed one is already in them. */
  live: boolean;
  setIsCompacting: ((v: string | false) => void) | null;
  /** How far the summary being written has got; a live stream's only. */
  setCompactionTokens?: (tokens: number) => void;
  insertNotification: (text: string, variant?: 'info' | 'success' | 'warning', detail?: string) => void;
  t: (key: string, opts?: Record<string, unknown>) => string;
  offloadBatch: React.MutableRefObject<OffloadBatch>;
}

/** Subagent history entry, one per task in the history snapshot (`subagents/historyStore.ts`). */
interface SubagentHistoryEntry {
  taskId: string;
  description: string;
  prompt: string;
  type: string;
  messages: Record<string, unknown>[];
  status: string;
  /** Ledger failure reason, present for any task that settled with one — a
   *  stop as well as a failure. Surfaced in the detail view header and on the
   *  inline card, so neither a "Failed" nor a "Stopped" card is unexplained. */
  error?: string;
  /** That reason's machine spelling (``credit_stop``, ``transport_lost``, …). */
  errorType?: string;
  toolCalls: number;
  tokenUsage: SubagentTokenUsage;
  currentTool: string;
  /** Start (epoch ms) of the newest run whose transcript the history
   *  projection contained — the run-level watermark the mux drain guard
   *  filters against. */
  projectedRunStartedMs?: number;
  /** When the task's latest run last started, finished or delivered its
   *  result (epoch ms), as the ledger read it at load. */
  lastActivityMs?: number;
  /** Reduced workflow-run progress, present only for a workflow run task
   *  (type 'workflow'), rebuilt from replayed workflow_lifecycle events. */
  workflowRun?: import('./subagents/workflowRunState').WorkflowRunState;
  /** Owning workflow run's agent id, present only for a workflow child —
   *  such tasks are hidden from the sidebar and reached via the run's card. */
  ownerTaskId?: string;
}

/** Per-task ref state used by stream handlers.
 *  messages is Record<string, unknown>[] to match the handler module's MessageRecord type. */
interface TaskRefs {
  contentOrderCounterRef: { current: number };
  currentReasoningIdRef: { current: string | null };
  currentToolCallIdRef: { current: string | null };
  messages: Record<string, unknown>[];
  runIndex: number;
  /** Live accumulator for a workflow run task's workflow_lifecycle reducer. */
  workflowRun?: import('./subagents/workflowRunState').WorkflowRunState;
}

/** History interrupt info stored during replay. */
interface HistoryInterruptInfo {
  type: string;
  assistantMessageId: string;
  questionId?: string;
  proposalId?: string;
  interruptId?: string;
  /** The tool call the interrupt paused, when its action request names it (a
   *  dispatch does): only that call's result may settle the card. */
  toolCallId?: string;
  /** Where a stopped call's verdict is looked up, decided when the interrupt
   *  was read so no settler has to rebuild it from the card id. Present only on
   *  a `tool_approval` entry. */
  target?: DecisionTarget;
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
  /** Backend-stamped ledger failure reason, present for any task that
   *  settled with one — a stop as well as a failure. */
  error?: string;
  /** The reason's machine spelling (``credit_stop``, ``transport_lost``, …). */
  errorType?: string;
  /** Owning workflow run's agent id, for a workflow child whose owner is
   *  known from elsewhere: its own transcript never names it. */
  ownerTaskId?: string;
  /** Build-time stamp: start (epoch ms) of the newest run whose transcript
   *  the projection actually claimed — NOT the ledger's latest run, which
   *  can still be executing and deliberately excluded from the payload. */
  projectedRunStartedMs?: number;
  /** Read-time stamp: when the latest run last started, finished or
   *  delivered its result (epoch ms). */
  lastActivityMs?: number;
}

/** Refs passed to createStreamEventProcessor and its processEvent closure:
 *  the handler-facing bag plus the fields only the main stream carries. */
interface StreamProcessorRefs extends StreamRefs {
  steeringAtOrderRef?: { current: number | null };
  updateSubagentCard?: UpdateSubagentCard;
  unresolvedHistoryInterruptRef?: React.MutableRefObject<HistoryInterruptInfo[]>;
  /** Set when the stream ended on the server's `retry`, or on an `error` that
   *  names its recovery; whoever ends the stream hands it to
   *  `settleRetryNotice` instead of finalizing the bubble. */
  retryNotice?: RetryNotice;
}

/** Pair state tracked per turn_index during history replay. */
interface PairState {
  contentOrderCounter: number;
  reasoningId: string | null;
  toolCallId: string | null;
  /** Steering batches delivered in this pair so far. It keys the ids of the bubbles a batch adds, so a replay mints the same ids as the last one. */
  steeringBatches: number;
}



export type {
  SetMessages, TokenUsage, PendingInterrupt,
  SSEEvent, ModelOptions, SendOptions, OffloadBatch, ContextWindowCallbacks,
  SubagentHistoryEntry, TaskRefs, HistoryInterruptInfo, SubagentHistoryData,
  StreamProcessorRefs, PairState,
};
