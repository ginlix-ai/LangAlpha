/**
 * Subagent history as one copy-on-write snapshot: the per-task projection that
 * replay and lazy transcript hydration build, plus two indexes the projection
 * never rebuilds: the tool-call id to task id index that points an inline card
 * at its task, and each run's ledger facts (`taskRuns.ts`).
 *
 * Every write swaps in a new snapshot and hands it to `onChange`, so the hook
 * can hold it as state and its getters take a new identity exactly when the
 * history changed. A getter over a mutable ref stays fresh only if it is
 * rebuilt on every render, and then every card memoized on it re-renders on
 * every streamed token. Lanes that run between renders read `get()`.
 */
import type { SubagentHistoryEntry } from '../types';
import { putTaskRun, type TaskRun, type TaskRunIndex } from './taskRuns';

export interface SubagentHistorySnapshot {
  readonly entries: Readonly<Record<string, SubagentHistoryEntry>>;
  readonly agentIdByToolCallId: ReadonlyMap<string, string>;
  /** Each run's facts by run id. */
  readonly runs: TaskRunIndex;
}

/** What `getSubagentHistory` hands a card: the entry under its resolved id. */
export type SubagentHistoryView = SubagentHistoryEntry & { agentId: string };

/** The write surface `mapToolCallIdToAgentId` needs; a plain Map fits too. */
export interface ToolCallAgentIndex {
  get(toolCallId: string): string | undefined;
  has(toolCallId: string): boolean;
  set(toolCallId: string, agentId: string): void;
}

export interface SubagentHistoryStore {
  get(): SubagentHistorySnapshot;
  readonly toolCalls: ToolCallAgentIndex;
  readonly runs: {
    /** File what a writer learned of a run (`putTaskRun`): each fact it
     *  names fills the run's, the rest stay. */
    put(runId: string, patch: TaskRun): void;
  };
  /** Replace each named entry wholesale, in one snapshot. */
  putEntries(entries: Record<string, SubagentHistoryEntry>): void;
  /** Merge fields into an existing entry; a missing entry stays missing. */
  patchEntry(agentId: string, patch: Partial<SubagentHistoryEntry>): void;
}

export const EMPTY_SUBAGENT_HISTORY: SubagentHistorySnapshot = {
  entries: {},
  agentIdByToolCallId: new Map(),
  runs: new Map(),
};

export function resolveHistoryAgentId(index: SubagentHistorySnapshot['agentIdByToolCallId'], subagentId: string): string {
  return index.get(subagentId) || subagentId;
}

export function readSubagentHistory(
  snapshot: SubagentHistorySnapshot,
  subagentId: string,
): SubagentHistoryView | null {
  const agentId = resolveHistoryAgentId(snapshot.agentIdByToolCallId, subagentId);
  const entry = snapshot.entries[agentId];
  return entry ? { ...entry, agentId } : null;
}

export function createSubagentHistoryStore(
  onChange: (snapshot: SubagentHistorySnapshot) => void = () => {},
): SubagentHistoryStore {
  let snapshot = EMPTY_SUBAGENT_HISTORY;
  const commit = (next: SubagentHistorySnapshot) => {
    snapshot = next;
    onChange(next);
  };
  return {
    get: () => snapshot,
    toolCalls: {
      get: (toolCallId) => snapshot.agentIdByToolCallId.get(toolCallId),
      has: (toolCallId) => snapshot.agentIdByToolCallId.has(toolCallId),
      set: (toolCallId, agentId) => {
        if (snapshot.agentIdByToolCallId.get(toolCallId) === agentId) return;
        const index = new Map(snapshot.agentIdByToolCallId);
        index.set(toolCallId, agentId);
        commit({ ...snapshot, agentIdByToolCallId: index });
      },
    },
    runs: {
      put: (runId, patch) => {
        const runs = putTaskRun(snapshot.runs, runId, patch);
        if (runs !== snapshot.runs) commit({ ...snapshot, runs });
      },
    },
    putEntries: (entries) => {
      if (Object.keys(entries).length === 0) return;
      commit({ ...snapshot, entries: { ...snapshot.entries, ...entries } });
    },
    patchEntry: (agentId, patch) => {
      const entry = snapshot.entries[agentId];
      if (!entry) return;
      commit({ ...snapshot, entries: { ...snapshot.entries, [agentId]: { ...entry, ...patch } } });
    },
  };
}
