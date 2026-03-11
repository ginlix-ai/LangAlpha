/** SSE event type union and per-event interfaces */

export type SSEEventType =
  | 'reasoning_signal'
  | 'reasoning_content'
  | 'message_chunk'
  | 'tool_calls'
  | 'tool_call_result'
  | 'tool_call_chunks'
  | 'artifact'
  | 'user_message'
  | 'workflow_status'
  | 'thread_created'
  | 'error'
  | 'queued_message_injected'
  | 'task_message_queued'
  | 'finish';

/** Base interface for all SSE events */
export interface BaseSSEEvent {
  event: SSEEventType;
  agent?: string;
  _eventId?: number | string;
  timestamp?: string | number;
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
  status: 'pending' | 'in_progress' | 'completed';
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

export interface ErrorEvent extends BaseSSEEvent {
  event: 'error';
  content: string;
  error_type?: string;
}

export interface QueuedMessageInjectedEvent extends BaseSSEEvent {
  event: 'queued_message_injected';
  messages: Array<{
    content: string;
    timestamp?: number;
  }>;
}

export interface TaskMessageQueuedEvent extends BaseSSEEvent {
  event: 'task_message_queued';
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

/** Discriminated union of all SSE events */
export type SSEEvent =
  | ReasoningSignalEvent
  | ReasoningContentEvent
  | MessageChunkEvent
  | ToolCallsEvent
  | ToolCallResultEvent
  | ToolCallChunksEvent
  | ArtifactEvent
  | WorkflowStatusEvent
  | ThreadCreatedEvent
  | ErrorEvent
  | QueuedMessageInjectedEvent
  | TaskMessageQueuedEvent
  | UserMessageEvent;
