/**
 * Shared per-turn / per-task streaming ref contracts, used by both the
 * main-stream and subagent live handlers.
 */

import type { MessageRecord } from '../hooks/utils/types';

/** Callback to update a subagent card by task ID. */
type UpdateSubagentCard = (taskId: string, patch: Record<string, unknown>) => void;

/** The card state's own updater, which can also hold a patch for the next
 *  frame, as a streamed chunk is (useCardState). */
type CardUpdater = (taskId: string, patch: Record<string, unknown>, options?: { nextFrame?: boolean }) => void;

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
 * Initializes per-task ref state if it doesn't exist yet.
 * Shared by all subagent event handlers to avoid repeated boilerplate.
 * @param {Object} refs - Refs object with subagentStateRefs
 * @param {string} taskId - Task ID (e.g., "task:k7Xm2p")
 * @returns {Object} The task refs ({ contentOrderCounterRef, currentReasoningIdRef, currentToolCallIdRef, messages })
 */
export function getOrCreateTaskRefs(refs: Pick<StreamRefs, 'subagentStateRefs'>, taskId: string): TaskRefs {
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

export type { TaskRefs, StreamRefs, ToolCallChunkRecord, UpdateSubagentCard, CardUpdater };
