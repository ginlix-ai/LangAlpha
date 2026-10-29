import { useMemo } from 'react';
import { infiniteQueryOptions, useInfiniteQuery } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import type { AutomationRun } from '@/types/automation';
import { isRunLive } from '../utils/status';
import { IDLE_POLL_MS, isDeliverySettling, pollMs } from '../utils/polling';
import { listRecentRuns, type RunFeedPage } from '../utils/api';

const PAGE_SIZE = 25;

/** The run feed's pages, for a reader that only needs to start the read or
 *  know it is in: the page starts it beside the automations list rather than
 *  once the feed mounts after it. */
export const recentRunsQuery = () =>
  infiniteQueryOptions({
    queryKey: queryKeys.automations.runs(),
    queryFn: async ({ pageParam }) =>
      (await listRecentRuns({ limit: PAGE_SIZE, offset: pageParam })).data,
    initialPageParam: 0,
    getNextPageParam: (lastPage: RunFeedPage, pages: RunFeedPage[]) =>
      lastPage.has_more ? pages.reduce((n, p) => n + p.executions.length, 0) : undefined,
    staleTime: 5000,
  });

/**
 * Every automation's runs, newest first, paged by offset.
 *
 * Offsets shift when a new run lands at the top, so a refetch can hand back a
 * row the previous page already held; the flattened list keeps the first copy
 * of each run rather than rendering it twice.
 *
 * Only the first page polls. A refetch of an infinite query fetches every
 * page it holds, so once the reader has loaded more the timer stops and the
 * list waits for an invalidation (a mutation, or the lifecycle feed, which
 * refetches it on every turn) or a return to the tab. A run whose delivery
 * may still change is the exception: no event announces its later record, so
 * while one is shown every page refetches at the idle pace, until none is.
 *
 * `delivers` says whether a run's automation has delivery entries, so a run
 * that ended with no record yet is waiting for one.
 */
export function useRecentRuns(delivers: (automationId: string) => boolean) {
  const query = useInfiniteQuery({
    ...recentRunsQuery(),
    refetchInterval: (q) => {
      const pages = q.state.data?.pages ?? [];
      const settling = pages.some((p) =>
        p.executions.some((r) => isDeliverySettling(r, delivers(r.automation_id))),
      );
      if (pages.length > 1) return settling ? IDLE_POLL_MS : false;
      return pollMs(settling || !!pages[0]?.executions.some((r) => isRunLive(r.status)));
    },
    refetchIntervalInBackground: false,
  });

  const runs = useMemo(() => {
    const seen = new Set<string>();
    const out: AutomationRun[] = [];
    for (const page of query.data?.pages ?? []) {
      for (const run of page.executions) {
        if (seen.has(run.automation_execution_id)) continue;
        seen.add(run.automation_execution_id);
        out.push(run);
      }
    }
    return out;
  }, [query.data]);

  return { ...query, runs };
}
