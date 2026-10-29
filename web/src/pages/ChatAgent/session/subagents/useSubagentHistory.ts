import { useCallback, useState } from 'react';
import type { SubagentRuntime } from '../runtime';
import {
  EMPTY_SUBAGENT_HISTORY, createSubagentHistoryStore, readSubagentHistory, resolveHistoryAgentId,
} from './historyStore';
import { hydrateTaskTranscript, type TaskTranscriptMeta } from './hydrateTaskTranscript';
import { runsOfTask } from './taskRuns';

/**
 * The subagent history bound to a view: the store the session lanes write and
 * the getters the cards read.
 *
 * The view holds each snapshot as state, so every getter takes a new identity
 * exactly when the history changes, and a card memoized on one re-renders then
 * and never on a streamed token. `beforePublish` runs ahead of every publish
 * and is read once: the chat passes its transcript's flush, so a history
 * change never renders ahead of the chunks queued before it.
 */
export function useSubagentHistory(
  beforePublish: () => void,
  { t, threadId, subagentStateRefsRef }: Pick<SubagentRuntime, 't' | 'subagentStateRefsRef'> & { threadId: string },
) {
  const [view, setView] = useState(EMPTY_SUBAGENT_HISTORY);
  const [store] = useState(() => createSubagentHistoryStore((next) => {
    beforePublish();
    setView(next);
  }));
  const { agentIdByToolCallId, runs } = view;

  // A tool-call id (an inline card's segment) to the task id card operations use.
  const resolveSubagentIdToAgentId = useCallback(
    (subagentId: string) => resolveHistoryAgentId(agentIdByToolCallId, subagentId),
    [agentIdByToolCallId],
  );
  const getSubagentHistory = useCallback(
    (subagentId: string) => readSubagentHistory(view, subagentId),
    [view],
  );
  // The runs of the task an inline card (or a task id) stands for.
  const getTaskRuns = useCallback(
    (subagentId: string) => runsOfTask(runs, resolveHistoryAgentId(agentIdByToolCallId, subagentId)),
    [runs, agentIdByToolCallId],
  );
  // Resolves to the landed entry itself: a caller continuing after the await
  // holds getters from before the fetch, which cannot see it yet.
  const hydrate = useCallback(
    (subagentId: string, meta?: TaskTranscriptMeta) =>
      hydrateTaskTranscript({ t, subagentHistory: store, subagentStateRefsRef }, threadId, subagentId, meta),
    [t, store, subagentStateRefsRef, threadId],
  );

  return {
    store,
    entries: view.entries,
    resolveSubagentIdToAgentId,
    getSubagentHistory,
    getTaskRuns,
    putTaskRun: store.runs.put,
    hydrateTaskTranscript: hydrate,
  };
}
