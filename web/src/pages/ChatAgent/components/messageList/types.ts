import type { HandOffNotice, ModelFallbackNotice, NotificationDetailKind, TextChunkMark } from '@/types/chat';
import type { PlanBilled } from '@/api/model';
import type { ThreadFeedback } from '../../utils/api/feedback';

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
  /** Notification-only: a hand-off to Claude Code, labeled when shown. */
  handoff?: HandOffNotice;
  /** Notification-only: longer text (compaction summary, hand-off brief, fallback error) shown under toggle. */
  detail?: string;
  /** Notification-only: expander toggle label flavor. */
  detailKind?: NotificationDetailKind;
  /** Notification-only: a reply billed to usage credits, drawn as a notice line. */
  planBilled?: PlanBilled;
}

/** Subagent info for opening subagent task tabs */
export interface SubagentInfo {
  subagentId: string;
  description?: string;
  type?: string;
  status?: string;
}

/** A turn's rating as a bubble reads it: the server's row, or only the
 *  rating a click settled on. */
export type FeedbackResult = { rating: string | null } & Partial<Omit<ThreadFeedback, 'rating'>>;
