import type { ModelFallbackNotice, TextChunkMark } from '@/types/chat';

// Stable empty object to avoid defeating React.memo with fresh `|| {}` fallbacks
export const EMPTY_OBJ = {} as Record<string, never>;

// --- Shared Types ---

export type { MessageRecord } from '../../hooks/utils/types';

/** A turn without foldable work is `unfolded`. Both preferences use `live`
 * while running, `collapsed` for a settled answer, and `expanded` on request.
 * `process` keeps cards and activity summaries visible when there is no answer. */
export type FoldState = 'unfolded' | 'live' | 'collapsed' | 'process' | 'expanded';

export type { ToolCallProcessRecord } from '../ToolCallDetailView';

/** Content segment from message data */
export interface ContentSegmentRecord {
  type: string;
  content?: string;
  order: number;
  /** Text-only: where each live chunk ended (see `session/stream/textChunks`). */
  chunks?: TextChunkMark;
  reasoningId?: string;
  toolCallId?: string;
  todoListId?: string;
  subagentId?: string;
  questionId?: string;
  proposalId?: string;
  widgetId?: string;
  /** Notification-only: a model switch, labeled when shown. */
  fallback?: ModelFallbackNotice;
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
