import { keepPreviousData, useQuery } from '@tanstack/react-query';
import { useStableArray } from '@/hooks/useStableArray';
import { epochMs } from '@/lib/elapsed';
import { queryKeys } from '@/lib/queryKeys';
import { useThreadRunStates } from '@/lib/threadLifecycle/store';
import {
  mapLifecycleStatus,
  mapLivenessStatus,
  type PTCDispatchStatus,
} from '../../hooks/usePTCDispatchStatus';
import { getHandoffLiveness, type DispatchLiveness } from '../../utils/api';
import { staysOnRoster, type AnalystCandidate } from './delegatedWork';

export interface RosterAnalyst {
  threadId: string;
  workspaceId: string | null;
  workspaceName: string | null;
  title: string | null;
  /** Null while nothing says how its thread stands: before the first read,
   *  or for a thread the read no longer finds. */
  status: PTCDispatchStatus | null;
  /** Start of its live run (epoch ms), which the row times it from. */
  startedAt?: number;
  /** Working, or still owing this thread its report while not waiting on the user. */
  running: boolean;
}

function sameIds(a: string[], b: string[]): boolean {
  return a.length === b.length && a.every((id, i) => id === b[i]);
}

function rosterAnalystsEqual(a: RosterAnalyst[], b: RosterAnalyst[]): boolean {
  return a.length === b.length && a.every((x, i) => {
    const y = b[i];
    return x.threadId === y.threadId && x.workspaceId === y.workspaceId && x.workspaceName === y.workspaceName
      && x.title === y.title && x.status === y.status && x.startedAt === y.startedAt && x.running === y.running;
  });
}

/**
 * The Analysts on the roster, working ones first. Status, title and when each
 * thread was last active (any turn there, whoever sent it) come from a batched
 * liveness read keyed on the threads' run states in the user's run feed, so a
 * run that starts or settles in one of them reads again. That read is made
 * after the feed's last word on them, so it is the newer one; while it is in
 * flight, the feed's status stands in. The thread's title and its workspace's
 * name come from that read too: the owed list names a thread only until it
 * reports, and the transcript holds only the question it was handed and the
 * workspace's name at the time.
 */
export function useAnalystRoster(
  candidates: AnalystCandidate[],
  turnStarts: readonly number[],
): RosterAnalyst[] {
  const ids = useStableArray(candidates.map((c) => c.threadId).sort(), sameIds);
  const { status: pushed, revision } = useThreadRunStates(ids);
  const { data, isPlaceholderData } = useQuery({
    queryKey: queryKeys.threads.dispatchActivity(ids, revision),
    // The shape the hand-off cards cache under the same key prefix, which
    // `patchThreadTitle` rewrites in place; only the rows are read here.
    queryFn: () => getHandoffLiveness({ threads: ids, runs: [], steers: [] }),
    select: (read) => read.liveness,
    enabled: ids.length > 0,
    placeholderData: keepPreviousData,
    staleTime: 30_000,
  });

  const byId = new Map<string, DispatchLiveness>((data ?? []).map((row) => [row.thread_id, row]));
  const rows: (RosterAnalyst & { lastActivityMs: number | null })[] = [];
  for (const c of candidates) {
    const live = byId.get(c.threadId);
    const read = live ? mapLivenessStatus(live.status) : null;
    const reported = (isPlaceholderData ? null : read) ?? mapLifecycleStatus(pushed.get(c.threadId) ?? 'idle') ?? read;
    // A hand-off that failed with no run on its thread yet, as its card says.
    const notStarted = c.dispatchFailed && (reported ?? 'starting') === 'starting';
    // Waiting on the user is unfinished work: it stays, though not as running,
    // even while it still owes this thread its report.
    const waiting = !notStarted && reported === 'needs_input';
    const running = !notStarted && !waiting && (c.owed || reported === 'running' || reported === 'starting');
    // It settled since the last read, whose activity time is from before: it
    // stays until the read for its new state says when it went quiet.
    const settling = !running && isPlaceholderData && (read === 'running' || read === 'needs_input');
    // Owed with nothing read: a hand-off whose run has not registered yet.
    const status = notStarted ? 'failed' : reported ?? (c.owed ? 'starting' : null);
    const lastActivityMs = Math.max(epochMs(live?.last_activity_at) ?? -Infinity, c.seenMs ?? -Infinity);
    const known = Number.isFinite(lastActivityMs) ? lastActivityMs : null;
    if (!settling && !staysOnRoster(running || waiting, known, turnStarts)) continue;
    rows.push({
      threadId: c.threadId,
      workspaceId: c.workspaceId,
      workspaceName: live?.workspace_name ?? c.workspaceName,
      title: live?.title ?? c.title,
      status,
      startedAt: read === 'running' ? epochMs(live?.run_started_at) ?? undefined : undefined,
      running,
      lastActivityMs: known,
    });
  }
  rows.sort((a, b) => Number(b.running) - Number(a.running) || (b.lastActivityMs ?? 0) - (a.lastActivityMs ?? 0));
  return useStableArray(rows.map(({ lastActivityMs: _, ...row }) => row), rosterAnalystsEqual);
}
