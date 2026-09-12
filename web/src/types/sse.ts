/** SSE event type union and per-event interfaces */
import type {
  OrderAction,
  OrderFailure,
  OrderMode,
  OrderMoney,
  OrderStatus,
  OrderSummary,
} from './orders';

/**
 * `error_type` on a task's terminal frame when the credit gate stopped it,
 * rather than something failing. Mirrors CREDIT_STOP_ERROR_TYPE in
 * src/server/contracts/status.py by hand — change there first, then here.
 */
export const CREDIT_STOP_ERROR_TYPE = 'credit_stop';

export type SSEEventType =
  | 'metadata'
  | 'reasoning_signal'
  | 'reasoning_content'
  | 'message_chunk'
  | 'tool_calls'
  | 'tool_call_result'
  | 'tool_call_chunks'
  | 'artifact'
  | 'provenance'
  | 'user_message'
  | 'workflow_status'
  | 'thread_created'
  | 'error'
  | 'model_retry'
  | 'model_fallback'
  | 'steering_delivered'
  | 'task_steering_accepted'
  | 'interrupt'
  | 'finish';

/** Base interface for all SSE events */
export interface BaseSSEEvent {
  event: SSEEventType;
  agent?: string;
  _eventId?: number | string;
  timestamp?: string | number;
}

/**
 * First event of every workflow stream. Announces the authoritative
 * ``run_id`` for this turn so the client can latch reconnect/demotion
 * logic onto it. Mirrors the langgraph_sdk SSE ``metadata`` payload.
 */
export interface MetadataEvent extends BaseSSEEvent {
  event: 'metadata';
  run_id: string;
  thread_id: string;
}

export interface ReasoningSignalEvent extends BaseSSEEvent {
  event: 'reasoning_signal';
  content: 'start' | 'complete';
}

export interface ReasoningContentEvent extends BaseSSEEvent {
  event: 'reasoning_content';
  content: string;
}

export interface MessageChunkEvent extends BaseSSEEvent {
  event: 'message_chunk';
  content?: string;
  finish_reason?: string | null;
}

export interface ToolCallData {
  id: string;
  name: string;
  args?: Record<string, unknown>;
}

export interface ToolCallsEvent extends BaseSSEEvent {
  event: 'tool_calls';
  tool_calls: ToolCallData[];
}

export interface ToolCallResultData {
  content: string | unknown;
  content_type: string;
  tool_call_id: string;
  artifact?: unknown;
  /** The ToolMessage status the backend stamped, 'success' or 'error'. Absent
   *  only on turns persisted before the field rode the wire. */
  status?: string;
}

export interface ToolCallResultEvent extends BaseSSEEvent {
  event: 'tool_call_result';
  tool_call_id: string;
  content: string | unknown;
  content_type?: string;
  artifact?: unknown;
  status?: string;
}

export interface ToolCallChunksEvent extends BaseSSEEvent {
  event: 'tool_call_chunks';
  tool_call_chunks: Array<{
    id?: string;
    name?: string;
    args?: string;
  }>;
}

export interface ArtifactEvent extends BaseSSEEvent {
  event: 'artifact';
  artifact_type: string;
  artifact_id?: string;
  payload?: unknown;
}

export type ProvenanceSourceType =
  | 'web_search'
  | 'web_fetch'
  | 'file_read'
  | 'memo_read'
  | 'memory_read'
  | 'sec_filing'
  | 'market_data'
  | 'mcp_tool';

export interface ProvenanceEvent extends BaseSSEEvent {
  event: 'provenance';
  record_id: string;
  /** Originating agent: "main" or "task:{id}". Resolved by the streaming
   *  handler from the LangGraph namespace, so subagent records are attributed. */
  agent?: string;
  timestamp: string;
  source_type: ProvenanceSourceType;
  identifier: string;
  title?: string;
  /** Data-kind slug within this source type (e.g. "company_overview",
   *  "daily_prices"); i18n-mapped by the Sources panel. */
  detail?: string;
  provider?: string;
  tool_call_id?: string;
  args_fingerprint?: Record<string, unknown>;
  /** Tool-call arguments with secrets already redacted server-side. Redacted
   *  values are the literal string "[redacted]". May be absent/empty. */
  args?: Record<string, unknown>;
  result_sha256?: string;
  result_size?: number;
  result_snippet?: string;
  /** Replay envelope: added by GET /threads/{id}/messages/replay so the
   *  frontend can re-attach records to the right turn after reload. */
  turn_index?: number;
  response_id?: string;
}

export interface TodoUpdatePayload {
  todos: TodoItem[];
  total: number;
  completed: number;
  in_progress: number;
  pending: number;
}

export interface TodoItem {
  id?: string;
  content: string;
  status: 'pending' | 'in_progress' | 'completed' | 'stale';
  [key: string]: unknown;
}

export interface WorkflowStatusEvent extends BaseSSEEvent {
  event: 'workflow_status';
  status: string;
  thread_id?: string;
}

export interface ThreadCreatedEvent extends BaseSSEEvent {
  event: 'thread_created';
  thread_id: string;
  workspace_id: string;
}

/** One entry in an ``error`` event's ``attempted_models`` list: a model the
 *  resilience middleware tried before the turn failed, with its own error. */
export interface AttemptedModel {
  model: string;
  error?: string;
  status_code?: number | null;
  attempts?: number;
}

export interface ErrorEvent extends BaseSSEEvent {
  event: 'error';
  /** Legacy single-field message; newer backends send ``error``/``message``. */
  content?: string;
  error_type?: string;
  /** Enriched fields from ``streaming_handler.format_error_event``. */
  error?: string;
  message?: string;
  error_kind?: 'upstream' | 'internal';
  status_code?: number | null;
  hints?: string[];
  /** User-configured (primary) model name, when the failure is model-attributable. */
  model?: string;
  /** Every model the resilience middleware attempted this turn (primary + fallbacks). */
  attempted_models?: AttemptedModel[];
}

/**
 * Emitted before the resilience middleware retries the SAME model after a
 * transient provider error. NOT persisted to history; DOES replay on
 * live-reconnect. ``attempt`` = number of calls that have already FAILED, so
 * the retry about to happen is ``attempt + 1`` of ``max_retries + 1`` total.
 */
export interface ModelRetryEvent extends BaseSSEEvent {
  event: 'model_retry';
  thread_id?: string;
  model: string;
  attempt: number;
  max_retries: number;
  error?: string;
  status_code?: number | null;
  delay_seconds?: number;
}

/**
 * Emitted when the resilience middleware gives up on one model and switches to
 * another. Persisted to history and replayed both on live-reconnect and in
 * history replay, so the transcript notification survives reload.
 */
export interface ModelFallbackEvent extends BaseSSEEvent {
  event: 'model_fallback';
  thread_id?: string;
  from_model: string;
  to_model: string;
  from_is_primary?: boolean;
  error?: string;
  status_code?: number | null;
  attempts_on_from?: number;
}

export interface SteeringDeliveredEvent extends BaseSSEEvent {
  event: 'steering_delivered';
  messages: Array<{
    content: string;
    timestamp?: number;
  }>;
}

export interface TaskSteeringAcceptedEvent extends BaseSSEEvent {
  event: 'task_steering_accepted';
  task_id: string;
  content: string;
  queue_position: number;
}

export interface UserMessageEvent extends BaseSSEEvent {
  event: 'user_message';
  content: string;
  metadata?: {
    attachments?: Attachment[];
    [key: string]: unknown;
  };
}

export interface Attachment {
  name: string;
  type: string;
  size?: number;
  url?: string;
  [key: string]: unknown;
}

export type {
  OrderAction,
  OrderFailure,
  OrderInstrument,
  OrderMode,
  OrderMoney,
  OrderStatus,
  OrderSummary,
} from './orders';

/**
 * The order a stopped call would place, as the server summarizes it: the
 * normalized order plus who would place it and how.
 *
 * The interrupt and the receipt are drawn from this one map. Only `action` and
 * `mode` are guaranteed; every other field is what the vendor's adapter could
 * normalize out of the arguments, so each is read on its own and a missing one
 * is simply not drawn. This rides an order path where showing a stale or
 * invented number is worse than showing none.
 */
export interface OrderProposal extends OrderSummary {
  action: OrderAction;
  mode: OrderMode;
  vendor?: string | null;
  tool?: string | null;
  /** The vendor account, masked on every surface that draws it. */
  account_ref?: string | null;
}

/** What the brokerage answered, mapped onto the attempt lifecycle. */
export interface OrderOutcome {
  status: OrderStatus;
  /** The vendor's own word for the state, kept because ours is a mapping. */
  raw_status?: string | null;
  vendor_order_id?: string | null;
  /**
   * Where the user has to go to finish this themselves. A staged instruction
   * only becomes an order once it is opened in the vendor's own client, and
   * this link is the only way there, so it is the card's primary affordance
   * whenever the vendor sent one.
   */
  action_url?: string | null;
  /** The tokens a later cancel needs (exchange, market, contract id). */
  route?: Record<string, string> | null;
  filled_qty?: string | null;
  avg_fill_price?: string | null;
  fees?: OrderMoney | null;
  failure?: OrderFailure | null;
  /** The reason a person typed when they rejected the order. The receipt is
   *  where the settled card reads it back from, so it survives a reload. */
  decision_message?: string | null;
  executed_at?: string | null;
  completed_at?: string | null;
}

/**
 * The order receipt on a tool result's artifact, under `order_receipt`.
 *
 * One attempt's whole story: what was asked (`order`, the same map the
 * approval card was drawn from) and what came back (`outcome`). It rides the
 * ToolMessage, so a reload replaying the checkpoint draws the same card. A
 * rejected, refused or failed attempt carries one too, which is what lets the
 * card render an order that never reached a brokerage.
 */
export interface OrderReceipt {
  attempt_id: string;
  vendor?: string | null;
  tool?: string | null;
  action?: OrderAction | null;
  mode?: OrderMode | null;
  account_ref?: string | null;
  order?: OrderProposal | null;
  outcome: OrderOutcome;
}

export interface ActionRequest {
  type?: string;
  name?: string;
  description?: string;
  args?: Record<string, unknown>;
  question?: string;
  options?: string[];
  allow_multiple?: boolean;
  workspace_name?: string;
  workspace_description?: string;
  workspace_id?: string;
  thread_id?: string;
  report_back?: boolean;
  tool_call_id?: string;
  /**
   * The order attempt this request answers for. Its presence is what makes a
   * request keyed: the resume answers it by this id rather than by its slot,
   * and history settles its card by this id rather than by position.
   */
  attempt_id?: string;
  /** The order this call would place, or null for a call that places none. */
  order?: OrderProposal | null;
  /** credit_pause: the quota service's denial copy, relayed verbatim. */
  message?: string;
}

export interface InterruptEvent extends BaseSSEEvent {
  event: 'interrupt';
  interrupt_id?: string;
  /** `order_approval` for an interrupt raised by the order gate. Its action
   *  requests still carry the HITL shape, so the card renders the same way;
   *  the kind is what says the requests are keyed. */
  kind?: string;
  action_requests?: ActionRequest[];
  thread_id?: string;
  role?: string;
  finish_reason?: string;
  turn_index?: number;
}

export interface FinishEvent extends BaseSSEEvent {
  event: 'finish';
  finish_reason?: string;
}

/** Discriminated union of all SSE events */
export type SSEEvent =
  | MetadataEvent
  | ReasoningSignalEvent
  | ReasoningContentEvent
  | MessageChunkEvent
  | ToolCallsEvent
  | ToolCallResultEvent
  | ToolCallChunksEvent
  | ArtifactEvent
  | ProvenanceEvent
  | WorkflowStatusEvent
  | ThreadCreatedEvent
  | ErrorEvent
  | ModelRetryEvent
  | ModelFallbackEvent
  | SteeringDeliveredEvent
  | TaskSteeringAcceptedEvent
  | UserMessageEvent
  | InterruptEvent
  | FinishEvent;
