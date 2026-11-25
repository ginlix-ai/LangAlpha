import { useQuery } from '@tanstack/react-query';
import { getRunningWork, type RunningWork } from '@/api/runningWork';
import { queryKeys } from '@/lib/queryKeys';

/**
 * What a restart of the server would pause for this user, asked fresh each
 * time `enabled` turns on: it is what is running now that the person weighs.
 * Not retried, since a failed count only swaps the counts for a general
 * warning and must not hold the restart back.
 */
export function useRunningWork(enabled: boolean) {
  return useQuery<RunningWork>({
    queryKey: queryKeys.user.runningWork(),
    queryFn: getRunningWork,
    enabled,
    staleTime: 0,
    gcTime: 0,
    retry: false,
    // The server a restart stops runs on this computer, so the count is asked
    // offline too, where a paused query would hold the restart back.
    networkMode: 'always',
  });
}
