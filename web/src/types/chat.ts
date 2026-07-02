/** Chat message types, content segments, and process records */

import type {
  Attachment,
  ToolCallData,
  ToolCallResultData,
  TodoItem,
  ProvenanceSourceType,
} from './sse';

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

export interface ProvenanceRecord {
  record_id: string;
  /** Originating agent: "main" or "task:{id}". */
  agent?: string;
  timestamp: string;
  source_type: ProvenanceSourceType;
  identifier: string;
  title?: string;
  /** Data-kind slug within this source type (e.g. "company_overview"); the
   *  Sources panel i18n-maps it to label each access in the hover breakdown. */
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
}

/**
 * Per-access discriminator appended to a provenance dedup key — `mcp_tool` only.
 *
 * Web/SEC/file sources have a per-access-unique `identifier` (a URL or path), so
 * the identifier alone separates two accesses. `market_data` repeats its ticker
 * identifier across calls, but each native tool call carries its own
 * `tool_call_id`, so the storage key stays distinct and the panel deck splits the
 * row by `result_sha256` on expand. An `mcp_tool` source has neither safeguard:
 * its identifier is `"server:tool"` (shared by every call to that tool) AND all
 * in-sandbox calls in one execute_code/bash block share that block's outer
 * `tool_call_id` — so two parts are added here to discriminate:
 *  1. `args_fingerprint` — separates calls with different inputs: get_stock_data
 *     for AAPL vs NVDA, or the same ticker over a different date range/interval.
 *  2. `result_sha256` — separates calls with IDENTICAL inputs that returned
 *     DIFFERENT data. Market data is time-varying, so the same query seconds
 *     apart is a distinct snapshot the agent reasoned over and earns its own
 *     card; collapsing on args alone would silently drop the earlier snapshot.
 *
 * This mirrors the backend persist dedup, which already keys on `result_sha256`.
 * Returns '' for non-mcp_tool (web sources keep their collapse-by-URL behavior).
 * When an mcp_tool body is too large to hash (sha nulled), it falls back to args.
 */
export function provenanceMcpKey(
  record: Pick<ProvenanceRecord, 'source_type' | 'args_fingerprint' | 'result_sha256'>,
): string {
  if (record.source_type !== 'mcp_tool') return '';
  const args = record.args_fingerprint ? JSON.stringify(record.args_fingerprint) : '';
  const sha = record.result_sha256 ?? '';
  return args || sha ? `${args}#${sha}` : '';
}

/**
 * Live-UI dedup key for a provenance record: `(source_type, identifier)`, plus
 * the per-access mcp_tool discriminator (args fingerprint + result hash; see
 * {@link provenanceMcpKey}), since an mcp_tool identifier is shared across calls.
 *
 * NOTE: web sources intentionally omit `result_sha256` here — the same URL
 * collapses to one row even when the DB keeps distinct shas. `mcp_tool` does
 * NOT omit it, because identical args can still return different data (live
 * market data). This per-source-type divergence is intentional.
 */
export function provenanceDisplayKey(
  record: Pick<
    ProvenanceRecord,
    'source_type' | 'identifier' | 'args_fingerprint' | 'result_sha256'
  > & {
    source_type?: ProvenanceRecord['source_type'];
    identifier?: string;
  },
): string {
  const base = `${record.source_type ?? ''} ${record.identifier ?? ''}`;
  const mcp = provenanceMcpKey(record);
  return mcp ? `${base} ${mcp}` : base;
}

/**
 * Count of distinct provenance sources by {@link provenanceDisplayKey}. The
 * single source of truth shared by the Sources pill and the Sources panel so
 * the displayed count and grouped rows can never silently diverge.
 */
export function countDedupedSources(
  records?: Record<
    string,
    Pick<ProvenanceRecord, 'source_type' | 'identifier' | 'args_fingerprint' | 'result_sha256'>
  > | null,
): number {
  if (!records) return 0;
  const seen = new Set<string>();
  for (const r of Object.values(records)) seen.add(provenanceDisplayKey(r));
  return seen.size;
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
  tool_call_id?: string;
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
  /**
   * Chart selections (region / price level) the user attached to this message.
   * Rendered as read-only pills below the user bubble (like widget snapshots)
   * and forwarded to the backend via `additional_context`. A compact camelCase
   * summary is persisted to the turn's query metadata, so history replay
   * re-renders these cards (see serialize_chart_selections_for_metadata).
   */
  chartSelections?: import('@/pages/MarketView/stores/chartSelectionStore').ChartSelectionSnapshot[];
  steeringDelivered?: boolean;
  steering?: boolean;
  /**
   * Set while this message is parked during an in-progress compaction (the
   * backend 409s a POST mid-compaction). Rendered as a shimmer bubble like a
   * pending steering message; auto-sent (or steered) once compaction finishes,
   * and dropped if the user stops the compaction.
   */
  queued?: boolean;
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
  provenanceRecords?: Record<string, ProvenanceRecord>;
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
  // Set when the user hard-stopped this turn (live finalize or history replay
  // of a stopped turn). Drives the per-message "⏹ Stopped" chip.
  stopped?: boolean;
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
  isHistory?: boolean;
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
