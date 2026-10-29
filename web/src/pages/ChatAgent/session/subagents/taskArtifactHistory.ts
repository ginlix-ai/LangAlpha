/**
 * A replayed task artifact (one per Task call: a spawn, a follow-up, a steer)
 * folded into its task's record for the subagent projection. Pure: replay
 * makes the writes that need the session (the inline card's status, the
 * tool-call index, the run's facts) with what this returns.
 */
import type { SSEEvent, SubagentHistoryData } from '../types';

export interface FoldedTaskArtifact {
  /** The task's id as the artifact names it. */
  taskId: string;
  /** `task:<id>`, which the projection and the history are keyed by. */
  agentId: string;
  /** The launch's action in the card's vocabulary (`init`, `update`, `resume`, ...). */
  action: string;
  /** The task's status, stamped by the server at read time. */
  status?: string;
}

function cardAction(raw: string | undefined): string {
  if (raw === 'spawned') return 'init';
  if (raw === 'steering_accepted') return 'update';
  if (raw === 'resumed') return 'resume';
  return raw || 'init';
}

/** Fold `event` into `byTaskId`; null when it names no task. */
export function foldTaskArtifact(
  byTaskId: Map<string, SubagentHistoryData>,
  event: SSEEvent,
): FoldedTaskArtifact | null {
  const payload = event.payload || {};
  const taskId = payload.task_id as string | undefined;
  const action = cardAction(payload.action as string | undefined);
  if (!taskId) return null;
  const description = payload.description as string | undefined;
  const prompt = payload.prompt as string | undefined;
  const type = payload.type as string | undefined;
  // Backend stamps the task's real status on every replayed task artifact
  // (payload.status). The top-level `status` is a hardcoded "completed"
  // and MUST be ignored.
  const status = payload.status as string | undefined;
  // Ledger failure reason, stamped on any task that settled with one.
  const error = payload.error as string | undefined;
  // Its machine spelling, which is what tells a stop the user can act
  // on (a credit stop) from one that is only worth reporting.
  const errorType = payload.error_type as string | undefined;
  const runStartedMs =
    typeof payload.projected_run_started_ms === 'number' ? payload.projected_run_started_ms : undefined;
  const activityMs = typeof payload.last_activity_ms === 'number' ? payload.last_activity_ms : undefined;

  const agentId = `task:${taskId}`;
  const existing = byTaskId.get(agentId);
  if (!existing) {
    byTaskId.set(agentId, {
      messages: [],
      events: [],
      description: description || '',
      prompt: prompt || description || '',
      type: type || 'general-purpose',
      status,
      error,
      errorType,
      projectedRunStartedMs: runStartedMs,
      lastActivityMs: activityMs,
    });
    return { taskId, agentId, action, status };
  }
  // The first artifact names the task: artifacts replay oldest first, and a
  // follow-up's description is its instruction. Folding pages together is
  // another rule (`olderPages.mergeSubagentHistory`).
  if (description && !existing.description) existing.description = description;
  if (prompt && !existing.prompt) existing.prompt = prompt;
  if (type && !existing.type) existing.type = type;
  // The failure travels with the status it arrived with, and is
  // replaced by it rather than merged under it. Every replayed
  // task artifact carries a status; a resumed task's carries no
  // failure at all, so preserving the previous run's reason is
  // exactly how a retracted credit stop comes back on the next
  // reload: the live session clears both writers when the task
  // resumes, and replay would put one of them straight back.
  if (status) {
    existing.status = status;
    existing.error = error;
    existing.errorType = errorType;
  }
  // Monotonic max: artifacts stamp claims-through-their-turn, and
  // page/artifact processing order must not regress the watermark.
  if (runStartedMs != null) {
    existing.projectedRunStartedMs =
      existing.projectedRunStartedMs != null ? Math.max(existing.projectedRunStartedMs, runStartedMs) : runStartedMs;
  }
  if (activityMs != null) {
    existing.lastActivityMs = Math.max(existing.lastActivityMs ?? activityMs, activityMs);
  }
  return { taskId, agentId, action, status };
}
