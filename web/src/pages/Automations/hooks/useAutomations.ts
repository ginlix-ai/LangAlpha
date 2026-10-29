import { useQuery } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import type { Automation } from '@/types/automation';
import { isAutomationRunning } from '../utils/status';
import { pollMs } from '../utils/polling';
import { listAutomations } from '../utils/api';

const LIST_PARAMS = { limit: 100, offset: 0 };

interface UseAutomationsResult {
  automations: Automation[];
  loading: boolean;
  /** The list arrived, as opposed to a read that failed or never ran. */
  loaded: boolean;
  error: Error | null;
}

const EMPTY: Automation[] = [];

export function useAutomations(): UseAutomationsResult {
  const { data, isLoading, error } = useQuery({
    queryKey: queryKeys.automations.list(LIST_PARAMS),
    queryFn: async () => (await listAutomations(LIST_PARAMS)).data,
    refetchInterval: (query) => pollMs(!!query.state.data?.automations.some(isAutomationRunning)),
    refetchIntervalInBackground: false,
    staleTime: 5000,
  });

  return {
    automations: data?.automations ?? EMPTY,
    loading: isLoading,
    loaded: data !== undefined,
    error,
  };
}
