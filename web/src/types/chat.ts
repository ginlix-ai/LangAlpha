/** Chat message types, content segments, and process records */

import type { Attachment, ToolCallData, ToolCallResultData, TodoItem } from './sse';

// --- Content Segments (discriminated union) ---

export interface ReasoningSegment {
  type: 'reasoning';
  reasoningId: string;
  order: number;
}

export interface TextSegment {
  type: 'text';
  content: string;
  order: number;
}

export interface ToolCallSegment {
  type: 'tool_call';
  toolCallId: string;
  order: number;
}

export interface TodoListSegment {
  type: 'todo_list';
  todoListId: string;
  order: number;
}

export interface SubagentTaskSegment {
  type: 'subagent_task';
  subagentId: string;
  order: number;
  resumeTargetId?: string;
}

export type ContentSegment =
  | ReasoningSegment
  | TextSegment
  | ToolCallSegment
  | TodoListSegment
  | SubagentTaskSegment;

// --- Process Records ---

export interface ReasoningProcess {
  content: string;
  isReasoning: boolean;
  reasoningComplete: boolean;
  order: number;
  reasoningTitle?: string | null;
  _completedAt?: number;
}

export interface ToolCallProcess {
  toolName: string;
  toolCall: ToolCallData | null;
  toolCallResult: ToolCallResultData | null;
  isInProgress: boolean;
  isComplete: boolean;
  isFailed?: boolean;
  order: number;
  _createdAt?: number;
}

export interface TodoListProcess {
  todos: TodoItem[];
  total: number;
  completed: number;
  in_progress: number;
  pending: number;
  order: number;
  baseTodoListId: string;
}

export interface SubagentTask {
  subagentId: string;
  description: string;
  prompt: string;
  type: string;
  action: 'init' | 'update' | 'resume';
  status: 'running' | 'completed';
  resumeTargetId?: string;
  result?: string;
  toolCallResult?: string;
}

export interface PendingToolCallChunk {
  toolName: string | null;
  chunkCount: number;
  argsLength: number;
  firstSeenAt: number;
}

// --- Chat Messages ---

export interface UserMessage {
  id: string;
  role: 'user';
  content: string;
  contentType: 'text';
  timestamp: Date;
  isStreaming: false;
  isHistory?: boolean;
  attachments?: Attachment[];
  queueDelivered?: boolean;
}

export interface AssistantMessage {
  id: string;
  role: 'assistant';
  content: string;
  contentType: 'text';
  timestamp: Date;
  isStreaming: boolean;
  isHistory?: boolean;
  contentSegments: ContentSegment[];
  reasoningProcesses: Record<string, ReasoningProcess>;
  toolCallProcesses: Record<string, ToolCallProcess>;
  todoListProcesses?: Record<string, TodoListProcess>;
  subagentTasks?: Record<string, SubagentTask>;
  pendingToolCallChunks?: Record<string, PendingToolCallChunk>;
}

export type NotificationVariant = 'info' | 'success' | 'warning';

export interface NotificationMessage {
  id: string;
  role: 'notification';
  content: string;
  variant: NotificationVariant;
  timestamp: Date;
}

export type ChatMessage = UserMessage | AssistantMessage | NotificationMessage;

// --- Subagent Task Refs ---

export interface SubagentTaskRefs {
  contentOrderCounterRef: { current: number };
  currentReasoningIdRef: { current: string | null };
  currentToolCallIdRef: { current: string | null };
  messages: AssistantMessage[];
  runIndex: number;
}

// --- History Replay ---

export interface PairState {
  contentOrderCounter: number;
  reasoningId: string | null;
  toolCallId: string | null;
}
