import { useQuery } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import { getReportsEnabled } from '@/api/reports';
import { isPlatformMode } from '@/config/hostMode';

/** Whether this deployment takes reports, ``undefined`` while that is not
 *  known: the answer is on its way, or could not be read. A self-hosted build
 *  has nowhere to send them, so it never asks. */
export function useReportsState(): boolean | undefined {
  const { data } = useQuery({
    queryKey: queryKeys.reports.config,
    queryFn: getReportsEnabled,
    enabled: isPlatformMode,
    staleTime: Infinity,
  });
  if (!isPlatformMode) return false;
  return data;
}

/** Whether to offer a way to report; not until the deployment says it takes
 *  them, so a failed read hides the entry points. */
export function useReportsEnabled(): boolean {
  return useReportsState() ?? false;
}
