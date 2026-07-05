/** SSE event type union and per-event interfaces */

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
}

export interface ToolCallResultEvent extends BaseSSEEvent {
  event: 'tool_call_result';
  tool_call_id: string;
  content: string | unknown;
  content_type?: string;
  artifact?: unknown;
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
}

export interface InterruptEvent extends BaseSSEEvent {
  event: 'interrupt';
  interrupt_id?: string;
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
