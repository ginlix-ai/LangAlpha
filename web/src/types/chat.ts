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

export interface NotificationSegment {
  type: 'notification';
  content: string;
  order: number;
  /** Optional longer text (e.g. the compaction summary) shown in an
   *  expandable panel beneath the notification label. */
  detail?: string;
}

export interface UserQuestionSegment {
  type: 'user_question';
  questionId: string;
  order: number;
}

export interface CreateWorkspaceSegment {
  type: 'create_workspace';
  proposalId: string;
  order: number;
}

export interface StartQuestionSegment {
  type: 'start_question';
  proposalId: string;
  order: number;
}

export interface PTCAgentSegment {
  type: 'ptc_agent';
  proposalId: string;
  order: number;
}

export interface DeleteWorkspaceSegment {
  type: 'delete_workspace';
  proposalId: string;
  order: number;
}

export interface StopWorkspaceSegment {
  type: 'stop_workspace';
  proposalId: string;
  order: number;
}

export interface DeleteThreadSegment {
  type: 'delete_thread';
  proposalId: string;
  order: number;
}

export interface PlanApprovalSegment {
  type: 'plan_approval';
  planApprovalId: string;
  order: number;
}

export type ContentSegment =
  | ReasoningSegment
  | TextSegment
  | ToolCallSegment
  | TodoListSegment
  | SubagentTaskSegment
  | NotificationSegment
  | UserQuestionSegment
  | CreateWorkspaceSegment
  | StartQuestionSegment
  | PTCAgentSegment
  | DeleteWorkspaceSegment
  | StopWorkspaceSegment
  | DeleteThreadSegment
  | PlanApprovalSegment;

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

// --- HITL Interrupt State Records ---

export interface PlanApprovalState {
  status: string;
  description?: string;
  planApprovalId?: string;
  interruptId?: string;
}

export interface UserQuestionState {
  questionId?: string;
  question?: string;
  answered?: boolean;
  skipped?: boolean;
  answer?: string | null;
  options?: string[];
  allow_multiple?: boolean;
  interruptId?: string;
  status?: string;
}

export interface WorkspaceProposalState {
  proposalId?: string;
  status: string;
  question?: string;
  workspace_name?: string;
  workspace_description?: string;
  interruptId?: string;
}

export interface QuestionProposalState {
  proposalId?: string;
  status: string;
  workspace_id?: string;
  question?: string;
  interruptId?: string;
}

export interface PTCAgentProposalState {
  proposalId?: string;
  status: string;
  workspace_id?: string;
  workspace_name?: string;
  thread_id?: string;
  question?: string;
  interruptId?: string;
  report_back?: boolean;
}

export interface SecretaryActionProposalState {
  proposalId?: string;
  status: string;
  actionType: 'delete_workspace' | 'stop_workspace' | 'delete_thread';
  workspace_id?: string;
  thread_id?: string;
  interruptId?: string;
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
  /**
   * Widget context snapshots attached to this message. Rendered as inline
   * chip cards below the user bubble (like attachments) and forwarded to the
   * backend via `additional_context`.
   */
  widgetSnapshots?: import('@/pages/Dashboard/widgets/framework/contextSnapshot').WidgetContextSnapshot[];
  steeringDelivered?: boolean;
  steering?: boolean;
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
  // HITL interrupt state
  planApprovals?: Record<string, PlanApprovalState>;
  userQuestions?: Record<string, UserQuestionState>;
  workspaceProposals?: Record<string, WorkspaceProposalState>;
  questionProposals?: Record<string, QuestionProposalState>;
  ptcAgentProposals?: Record<string, PTCAgentProposalState>;
  secretaryActionProposals?: Record<string, SecretaryActionProposalState>;
  // Runtime flags
  steering?: boolean;
  steeringDelivered?: boolean;
  isSteering?: boolean;
  error?: boolean | string;
}

export type NotificationVariant = 'info' | 'success' | 'warning';

export interface NotificationMessage {
  id: string;
  role: 'notification';
  content: string;
  variant: NotificationVariant;
  timestamp: Date;
  /** Optional longer text (e.g. a compaction summary) surfaced via the
   *  notification's expand toggle. */
  detail?: string;
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
