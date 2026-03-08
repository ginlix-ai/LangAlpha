import { useQuery } from '@tanstack/react-query';
import { listExecutions } from '../utils/api';

const POLL_INTERVAL = 15000;

export function useExecutions(automationId) {
  const { data = { executions: [], total: 0 }, isLoading: loading } = useQuery({
    queryKey: ['executions', automationId],
    queryFn: async () => {
      const { data } = await listExecutions(automationId, { limit: 20, offset: 0 });
      return { executions: data.executions, total: data.total };
    },
    enabled: !!automationId,
    refetchInterval: POLL_INTERVAL,
    staleTime: 5000,
  });

  return { executions: data.executions, total: data.total, loading };
}
