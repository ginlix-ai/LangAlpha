/**
 * Lazily materialize a task transcript replay never projects as a lane.
 * Workflow children have no Task-tool launch artifact in the main transcript,
 * so their durably checkpointed transcripts are only reachable on demand —
 * this fetches one and reduces it through the same projection path replayed
 * lanes take, so a hydrated child renders identically to a replayed one.
 */
import { getSubagentTaskHistory, getSubagentTaskStatus } from '../../utils/api';
import { taskIdFromAgentId } from '../../utils/agentId';
import type { SSEEvent } from '../types';
import type { SubagentRuntime } from '../runtime';
import { projectSubagentHistory } from './projectHistory';
import { isTerminalStatus } from './subagentStatus';
import { deriveChildIdentity, findWorkflowChildOwner } from './workflowRunState';

export interface TaskTranscriptMeta {
  description?: string;
  type?: string;
  status?: string;
}

/** Resolves true when an entry landed in the history ref. */
export async function hydrateTaskTranscript(
  rt: SubagentRuntime,
  threadId: string,
  subagentId: string,
  meta?: TaskTranscriptMeta,
): Promise<boolean> {
  const agentId = rt.toolCallIdToTaskIdMapRef.current.get(subagentId) || subagentId;
  if (!threadId || threadId === '__default__') return false;
  const prior = rt.subagentHistoryRef.current?.[agentId];
  if (prior?.messages?.length) return true;
  const shortId = taskIdFromAgentId(agentId) ?? agentId;

  // A state ref with content is a live stream's write surface — never stomp
  // it. An EMPTY lane is either a replay ghost lane (table-sourced
  // provenance/context events create the ref with no transcript) or a live
  // child that hasn't emitted yet; only a terminal task is safe to re-read
  // from the checkpoint, since terminal means no live writer.
  const stateRef = rt.subagentStateRefsRef.current[agentId];
  if (stateRef?.messages?.length) return false;

  let status: string | undefined = [meta?.status, prior?.status].find(isTerminalStatus);
  // Applies with or without a lane: a deep link to a still-running child has
  // no state ref yet, and hydrating it would freeze a partial transcript
  // instead of attaching to the live stream.
  if (!status) {
    try {
      const res = await getSubagentTaskStatus(threadId, shortId);
      if (isTerminalStatus(res?.status)) status = res.status as string;
    } catch { /* unreachable ledger reads as non-terminal */ }
    if (!status) return false;
  }

  // A workflow child is anonymous in its own transcript — the dispatching
  // run's reduced state is the only place its label, type and owner exist,
  // and a one-entry projection can never reach it on its own.
  const owner = findWorkflowChildOwner(rt.subagentHistoryRef.current, shortId);
  const identity = deriveChildIdentity(owner?.child, {
    description: meta?.description || prior?.description,
    type: meta?.type || prior?.type,
  });

  try {
    const res = await getSubagentTaskHistory(threadId, shortId);
    const events = (res?.items || []).map(
      (item) => ({ ...(item.data || {}), event: item.event }),
    ) as SSEEvent[];
    if (!events.length) return false;
    projectSubagentHistory(
      rt,
      new Map([
        [
          agentId,
          {
            messages: [],
            events,
            description: identity.description,
            type: identity.type,
            status,
          },
        ],
      ]),
    );
    const entry = rt.subagentHistoryRef.current?.[agentId];
    // Re-projection rebuilds the entry from transcript events alone; restore
    // identity the ghost lane learned from workflow lifecycle.
    const ownerTaskId = prior?.ownerTaskId || owner?.ownerTaskId;
    if (entry && !entry.ownerTaskId && ownerTaskId) {
      entry.ownerTaskId = ownerTaskId;
    }
    return !!entry;
  } catch {
    return false;
  }
}
