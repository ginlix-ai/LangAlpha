/**
 * Agent lane ids: `task:<taskId>` for a subagent lane, `main` for the root.
 * Parsing lives here so the prefix and its length are stated once.
 */

const TASK_AGENT_PREFIX = 'task:';

/** Whether an agent attribution names a subagent lane rather than the root. */
export function isTaskAgentId(agent: unknown): agent is string {
  return typeof agent === 'string' && agent.startsWith(TASK_AGENT_PREFIX);
}

/**
 * The task id carried by an agent lane id, or null when the id names no task.
 *
 * A prefixed-but-empty id yields `''`, not null — whether an empty task id is
 * "absent" differs by caller, so each states that itself.
 */
export function taskIdFromAgentId(agentId: string | null | undefined): string | null {
  if (!isTaskAgentId(agentId)) return null;
  return agentId.slice(TASK_AGENT_PREFIX.length);
}

const DISPLAY_ID_PREFIX = 'Task-';

/**
 * The task id a model-written reference means. Launch results and reports
 * show a task by its display id (`Task-k7Xm2p`), so the model passes that
 * back as often as the bare id, and the server takes either. A minted id is
 * six characters, too short to carry the prefix itself. Mirrors `bare_task_id`
 * in src/ptc_agent/agent/middleware/background_subagent/task.py.
 */
export function bareTaskId(taskId: string): string {
  const id = taskId.trim();
  return id.startsWith(DISPLAY_ID_PREFIX) && id.length > DISPLAY_ID_PREFIX.length + 1
    ? id.slice(DISPLAY_ID_PREFIX.length)
    : id;
}
