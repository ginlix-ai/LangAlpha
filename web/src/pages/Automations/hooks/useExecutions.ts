import { useCallback } from 'react';
import { queryOptions, useQuery, useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import type { AutomationExecution } from '@/types/automation';
import { isRunLive } from '../utils/status';
import { isDeliverySettling, pollMs } from '../utils/polling';
import { listExecutions } from '../utils/api';

interface UseExecutionsResult {
  executions: AutomationExecution[];
  loading: boolean;
}

const EMPTY: AutomationExecution[] = [];

const executionsQuery = (automationId: string) =>
  queryOptions({
    queryKey: queryKeys.automations.executions(automationId),
    queryFn: async () => (await listExecutions(automationId, { limit: 20, offset: 0 })).data,
    staleTime: 5000,
  });

/** An automation's runs, polled fast while one is live or its delivery may
 *  still change. `delivers`: the automation has delivery entries, so a run
 *  that ended with no record yet is waiting for one. */
export function useExecutions(automationId: string, delivers = false): UseExecutionsResult {
  const { data, isLoading } = useQuery({
    ...executionsQuery(automationId),
    refetchInterval: (query) =>
      pollMs(!!query.state.data?.executions.some((e) => isRunLive(e.status) || isDeliverySettling(e, delivers))),
    refetchIntervalInBackground: false,
  });

  return { executions: data?.executions ?? EMPTY, loading: isLoading };
}

/** Warms an automation's runs before it is opened, so its pane renders whole
 *  instead of swapping a skeleton for the table a beat after arriving. */
export function usePrefetchExecutions() {
  const queryClient = useQueryClient();
  return useCallback(
    (automationId: string) => {
      void queryClient.prefetchQuery(executionsQuery(automationId));
    },
    [queryClient],
  );
}
