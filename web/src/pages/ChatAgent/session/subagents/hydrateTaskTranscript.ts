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
import { resolveHistoryAgentId, type SubagentHistoryView } from './historyStore';
import { isTerminalStatus } from './subagentStatus';
import { deriveChildIdentity, findWorkflowChildOwner } from './workflowRunState';

export interface TaskTranscriptMeta {
  description?: string;
  type?: string;
  status?: string;
  /** Re-asked once the transcript arrives: false when the run the caller
   * hydrates for is no longer the task's latest (a resume landed meanwhile). */
  stillCurrent?: () => boolean;
}

/** Resolves to the entry in the history, or null when none landed. */
export async function hydrateTaskTranscript(
  rt: Pick<SubagentRuntime, 't' | 'subagentHistory' | 'subagentStateRefsRef'>,
  threadId: string,
  subagentId: string,
  meta?: TaskTranscriptMeta,
): Promise<SubagentHistoryView | null> {
  const agentId = resolveHistoryAgentId(rt.subagentHistory.get().agentIdByToolCallId, subagentId);
  if (!threadId || threadId === '__default__') return null;
  const prior = rt.subagentHistory.get().entries[agentId];
  if (prior?.messages?.length) return { ...prior, agentId };
  const shortId = taskIdFromAgentId(agentId) ?? agentId;

  // A state ref with content is a live stream's write surface — never stomp
  // it. An EMPTY lane is either a replay ghost lane (table-sourced
  // provenance/context events create the ref with no transcript) or a live
  // child that hasn't emitted yet; only a terminal task is safe to re-read
  // from the checkpoint, since terminal means no live writer.
  const stateRef = rt.subagentStateRefsRef.current[agentId];
  if (stateRef?.messages?.length) return null;

  let status: string | undefined = [meta?.status, prior?.status].find(isTerminalStatus);
  // Applies with or without a lane: a deep link to a still-running child has
  // no state ref yet, and hydrating it would freeze a partial transcript
  // instead of attaching to the live stream.
  if (!status) {
    try {
      const res = await getSubagentTaskStatus(threadId, shortId);
      if (isTerminalStatus(res?.status)) status = res.status as string;
    } catch { /* unreachable ledger reads as non-terminal */ }
    if (!status) return null;
  }

  // A workflow child is anonymous in its own transcript — the dispatching
  // run's reduced state is the only place its label, type and owner exist,
  // and a one-entry projection can never reach it on its own.
  const owner = findWorkflowChildOwner(rt.subagentHistory.get().entries, shortId);
  const identity = deriveChildIdentity(owner?.child, {
    description: meta?.description || prior?.description,
    type: meta?.type || prior?.type,
  });

  try {
    const res = await getSubagentTaskHistory(threadId, shortId);
    const events = (res?.items || []).map(
      (item) => ({ ...(item.data || {}), event: item.event }),
    ) as SSEEvent[];
    if (!events.length) return null;
    // The checks above ran before the fetch: a task resumed meanwhile has a
    // successor writing its lane, possibly still empty, which this terminal
    // checkpoint must not overwrite.
    if (rt.subagentStateRefsRef.current[agentId]?.messages?.length) return null;
    if (rt.subagentHistory.get().entries[agentId]?.messages?.length) return null;
    if (meta?.stillCurrent && !meta.stillCurrent()) return null;
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
            // The transcript alone never names it; the ghost lane learned it
            // from workflow lifecycle.
            ownerTaskId: prior?.ownerTaskId || owner?.ownerTaskId,
          },
        ],
      ]),
    );
    const landed = rt.subagentHistory.get().entries[agentId];
    return landed ? { ...landed, agentId } : null;
  } catch {
    return null;
  }
}
