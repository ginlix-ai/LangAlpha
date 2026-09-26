import { useMemo } from 'react';
import { useInfiniteQuery } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import type { AutomationRun } from '@/types/automation';
import { isRunLive } from '../utils/status';
import { pollMs } from '../utils/polling';
import { listRecentRuns, type RunFeedPage } from '../utils/api';

const PAGE_SIZE = 25;

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
 * refetches it on every turn) or a return to the tab.
 */
export function useRecentRuns() {
  const query = useInfiniteQuery({
    queryKey: queryKeys.automations.runs(),
    queryFn: async ({ pageParam }) =>
      (await listRecentRuns({ limit: PAGE_SIZE, offset: pageParam })).data,
    initialPageParam: 0,
    getNextPageParam: (lastPage: RunFeedPage, pages: RunFeedPage[]) =>
      lastPage.has_more ? pages.reduce((n, p) => n + p.executions.length, 0) : undefined,
    refetchInterval: (q) => {
      const pages = q.state.data?.pages ?? [];
      if (pages.length > 1) return false;
      return pollMs(!!pages[0]?.executions.some((r) => isRunLive(r.status)));
    },
    refetchIntervalInBackground: false,
    staleTime: 5000,
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
