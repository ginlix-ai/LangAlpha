// Stable empty object to avoid defeating React.memo with fresh `|| {}` fallbacks
export const EMPTY_OBJ = {} as Record<string, never>;

// --- Shared Types ---

/** Loosely typed message record from SSE/API */
export type MessageRecord = Record<string, unknown>;

/** Loosely typed tool call process record */
export type ToolCallProcessRecord = Record<string, unknown>;

/** Content segment from message data */
export interface ContentSegmentRecord {
  type: string;
  content?: string;
  order: number;
  lastOrder?: number;
  reasoningId?: string;
  toolCallId?: string;
  todoListId?: string;
  subagentId?: string;
  planApprovalId?: string;
  questionId?: string;
  proposalId?: string;
  widgetId?: string;
  /** Notification-only: longer text (compaction summary / fallback error) shown under toggle. */
  detail?: string;
  /** Notification-only: expander toggle label flavor. */
  detailKind?: 'summary' | 'error';
}

/** Subagent info for opening subagent task tabs */
export interface SubagentInfo {
  subagentId: string;
  description?: string;
  type?: string;
  status?: string;
}

/** Feedback result from API */
export interface FeedbackResult {
  rating: string | null;
  [key: string]: unknown;
}
