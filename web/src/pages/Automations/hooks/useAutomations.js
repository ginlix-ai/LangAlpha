import { useQuery } from '@tanstack/react-query';
import { listAutomations } from '../utils/api';

const POLL_INTERVAL = 30000;

export function useAutomations({ status } = {}) {
  const { data = { automations: [], total: 0 }, isLoading: loading, error, refetch } = useQuery({
    queryKey: ['automations', status],
    queryFn: async () => {
      const params = { limit: 100, offset: 0 };
      if (status) params.status = status;
      const { data } = await listAutomations(params);
      return { automations: data.automations, total: data.total };
    },
    refetchInterval: POLL_INTERVAL,
    staleTime: 5000,
  });

  return { automations: data.automations, total: data.total, loading, error, refetch };
}
