import { createContext, useContext, useMemo } from 'react';
import { useStableArray } from '@/hooks/useStableArray';
import {
  resolveSubagentTelemetry,
  sameTelemetry,
  type SubagentHistoryLike,
  type SubagentTelemetry,
} from '../session/subagents/resolveSubagentTelemetry';
import type { TaskRun } from '../session/subagents/taskRuns';

export type SubagentTelemetryResolver = (subagentId: string) => SubagentTelemetry | undefined;

export interface SubagentTelemetrySource {
  resolve: SubagentTelemetryResolver;
  /** False where there are no cards to follow (the market panel), so a run
   *  launched since the page loaded shows no progress. */
  followsRuns: boolean;
}

// Consumed at the leaves (SubagentRun, SubagentStopNotices) so live token-tick
// re-renders bypass the memoized MessageBubble / MessageContentSegments
// trees instead of busting their memo on every SSE event. A provider memoizes
// its value on the resolver, so only a new resolver reaches the leaves.
export const SubagentTelemetryContext = createContext<SubagentTelemetrySource | null>(null);

/** The source for a surface that keeps no cards (the market panel): every
 *  answer comes from history and the task's runs. */
export function useHistoryTelemetry(
  getSubagentHistory: (subagentId: string) => SubagentHistoryLike | null,
  getTaskRuns: (subagentId: string) => readonly TaskRun[],
): SubagentTelemetrySource {
  return useMemo(() => ({
    // The segment's id is the Task call it stands for, which names the run
    // its row draws when the task was resumed.
    resolve: (id: string) => resolveSubagentTelemetry(undefined, getSubagentHistory(id) ?? undefined, getTaskRuns(id), id),
    followsRuns: false,
  }), [getSubagentHistory, getTaskRuns]);
}

/** Whether this surface's telemetry follows runs as they go: none at all (a
 *  shared link) does not. */
export function useTelemetryFollowsRuns(): boolean {
  return useContext(SubagentTelemetryContext)?.followsRuns ?? false;
}

const sameEach = (a: readonly (SubagentTelemetry | undefined)[], b: readonly (SubagentTelemetry | undefined)[]): boolean =>
  a.length === b.length && a.every((x, i) => sameTelemetry(x, b[i]));

/** The same read for a group of subagents, as one hook so the group's size
 *  can change between renders. The resolver is rebuilt whenever any card
 *  changes, so the array keeps its identity until one of these answers does. */
export function useSubagentTelemetries(subagentIds: readonly string[]): (SubagentTelemetry | undefined)[] {
  const resolver = useContext(SubagentTelemetryContext)?.resolve;
  return useStableArray(subagentIds.map((id) => (resolver ? resolver(id) : undefined)), sameEach);
}
