/**
 * The one place a `subagent_task` segment and its `subagentTasks` record are
 * derived from a tool call. Live streaming and history replay both build the
 * inline card through here, so a reloaded thread renders exactly what the live
 * turn did — the two used to carry copy-pasted branches that drifted silently.
 */
import type { SubagentTaskRecord, SubagentTaskSegment } from '@/types/chat';
import { isToolResultFailure } from './subagentStatus';
import { normalizeAction } from '../../hooks/utils/eventUtils';
import { WORKFLOW_TASK_TYPE } from './workflowRunState';

interface ToolCallLike {
  name?: string;
  args?: Record<string, unknown>;
}

export interface TaskSegmentDerivation {
  segment: SubagentTaskSegment;
  record: SubagentTaskRecord;
  /** A resume stacks a *second* card on the same message: its segment is
   *  pushed unconditionally and its record replaces the previous one. Spawns
   *  and workflow runs are idempotent — one segment, merged in place. */
  stacks: boolean;
}

/**
 * Returns `null` for tool calls that render no inline card. `order` is the
 * caller's already-resolved segment order (event id or counter).
 */
export function deriveTaskSegment(
  toolCall: ToolCallLike,
  toolCallId: string,
  order: number,
): TaskSegmentDerivation | null {
  const args = toolCall.args || {};
  const description = (args.description as string) || '';

  if (toolCall.name === 'RunWorkflow') {
    // Workflow run launch — same segment/record machinery as a Task spawn,
    // discriminated by type 'workflow' at render time.
    return {
      segment: { type: 'subagent_task', subagentId: toolCallId, order },
      record: {
        subagentId: toolCallId,
        description: description || (args.workflow as string) || '',
        prompt: description,
        type: WORKFLOW_TASK_TYPE,
        action: 'init',
        status: 'running',
      },
      stacks: false,
    };
  }

  // Backend uses PascalCase "Task"; accept both for compatibility.
  if (toolCall.name !== 'task' && toolCall.name !== 'Task') return null;

  const action = normalizeAction((args.action as string) || (args.task_id ? 'resume' : 'init'));
  const prompt = (args.prompt as string) || description;
  const subagentType = (args.subagent_type as string) || 'general-purpose';

  if (action === 'init') {
    return {
      segment: { type: 'subagent_task', subagentId: toolCallId, order },
      record: {
        subagentId: toolCallId,
        description,
        prompt,
        type: subagentType,
        action: 'init',
        status: 'running',
      },
      stacks: false,
    };
  }

  // Resume/follow-up call — a new card with a "resumed" indicator, pointing at
  // the original task. Normalized to "task:xxx" to match floating card keys.
  const rawTargetId = (args.task_id as string) || '';
  const resumeTargetId = rawTargetId.startsWith('task:') ? rawTargetId : `task:${rawTargetId}`;
  return {
    segment: { type: 'subagent_task', subagentId: toolCallId, resumeTargetId, order },
    record: {
      subagentId: toolCallId,
      resumeTargetId,
      description,
      prompt,
      type: subagentType,
      action,
      status: 'running',
    },
    stacks: true,
  };
}

/**
 * Stamp a launch card with the reply its `Task` / `RunWorkflow` call returned.
 *
 * The reply is never the task's outcome — both tools return the moment the
 * launch is dispatched, and terminal status arrives later per task (a replayed
 * artifact, or a live `chan_close`). The exception is a launch that was
 * *refused*: it opens no run, produces no artifact and no channel, so nothing
 * else will ever settle its card and the reply's own "Error" prefix has to.
 *
 * Live streaming and history replay both stamp through here — they used to
 * carry a copy each, which is how they came to disagree on the field's name.
 */
export function applyLaunchReply(
  record: SubagentTaskRecord,
  result: { content?: unknown; artifact?: unknown },
): SubagentTaskRecord {
  return {
    ...record,
    ...(typeof result.content === 'string' ? { result: result.content } : {}),
    ...(isToolResultFailure(result) ? { status: 'error' } : {}),
  };
}

/**
 * Apply a derivation in place onto a message's segment list + task map, honoring
 * the stack-vs-merge rule.
 */
export function applyTaskSegment(
  derived: TaskSegmentDerivation,
  toolCallId: string,
  contentSegments: Record<string, unknown>[],
  subagentTasks: Record<string, SubagentTaskRecord>,
): void {
  const exists = contentSegments.some(
    (s) => s.type === 'subagent_task' && s.subagentId === toolCallId
  );
  if (derived.stacks || !exists) {
    contentSegments.push({ ...derived.segment });
  }
  subagentTasks[toolCallId] = derived.stacks
    ? { ...derived.record }
    : { ...(subagentTasks[toolCallId] || {}), ...derived.record };
}
