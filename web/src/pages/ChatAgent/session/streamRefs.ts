/**
 * Shared per-turn / per-task streaming ref contracts, used by both the
 * main-stream and subagent live handlers.
 */

import type { MessageRecord } from '../hooks/utils/types';

/** Callback to update a subagent card by task ID. */
type UpdateSubagentCard = (taskId: string, patch: Record<string, unknown>) => void;

/** Per-task ref state created by getOrCreateTaskRefs. */
interface TaskRefs {
  contentOrderCounterRef: { current: number };
  currentReasoningIdRef: { current: string | null };
  currentToolCallIdRef: { current: string | null };
  messages: MessageRecord[];
  runIndex: number;
  /** Live accumulator for a workflow run task's workflow_lifecycle reducer. */
  workflowRun?: import('./subagents/workflowRunState').WorkflowRunState;
}

/** Shape of refs passed to main-agent streaming handlers. */
interface StreamRefs {
  contentOrderCounterRef: { current: number };
  currentReasoningIdRef: { current: string | null };
  currentToolCallIdRef: { current: string | null };
  subagentStateRefs?: Record<string, TaskRefs>;
  isReconnect?: boolean;
  updateTodoListCard?: (data: Record<string, unknown>, isNew: boolean) => void;
  isNewConversation?: boolean;
  [key: string]: unknown;
}

/** Shape of a tool call chunk object. */
interface ToolCallChunkRecord {
  index?: number;
  name?: string;
  args?: string;
  [key: string]: unknown;
}

/**
 * Next value of a message's monotonic arrival counter. Every landed reply
 * text, reasoning text or tool-argument chunk bumps it, so a bubble can tell
 * "text is still flowing" from "the turn has gone quiet" without re-measuring
 * everything it holds.
 */
export function nextArrivalSeq(msg: { arrivalSeq?: unknown }): number {
  return ((msg.arrivalSeq as number) ?? 0) + 1;
}

/**
 * Extracts the last markdown bold title (**...**) from reasoning content for the icon label.
 * Used only during live streaming; history always shows "Reasoning".
 * @param {string} content - Accumulated reasoning text
 * @returns {string|null} Last **title** inner text or null
 */
function extractLastReasoningTitle(content: unknown): string | null {
  if (!content || typeof content !== 'string') return null;
  const matches = content.matchAll(/\*\*([^*]+)\*\*/g);
  let last: string | null = null;
  for (const m of matches) last = m[1].trim();
  return last || null;
}

/**
 * Initializes per-task ref state if it doesn't exist yet.
 * Shared by all subagent event handlers to avoid repeated boilerplate.
 * @param {Object} refs - Refs object with subagentStateRefs
 * @param {string} taskId - Task ID (e.g., "task:k7Xm2p")
 * @returns {Object} The task refs ({ contentOrderCounterRef, currentReasoningIdRef, currentToolCallIdRef, messages })
 */
export function getOrCreateTaskRefs(refs: StreamRefs, taskId: string): TaskRefs {
  const subagentStateRefs = refs.subagentStateRefs || {};
  if (!subagentStateRefs[taskId]) {
    subagentStateRefs[taskId] = {
      contentOrderCounterRef: { current: 0 },
      currentReasoningIdRef: { current: null },
      currentToolCallIdRef: { current: null },
      messages: [],
      runIndex: 0,
    };
  }
  return subagentStateRefs[taskId];
}

export type { TaskRefs, StreamRefs, ToolCallChunkRecord, UpdateSubagentCard };
export { extractLastReasoningTitle };
