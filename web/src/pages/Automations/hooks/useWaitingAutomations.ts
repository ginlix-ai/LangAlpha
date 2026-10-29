import { useQuery } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import { isValidUuid } from '@/pages/ChatAgent/utils/uuid';
import type { AutomationRun } from '@/types/automation';
import { listRecentRuns } from '../utils/api';

// The user feed announces a run joining or leaving the line, and a feed
// reconnect refetches the lot. The poll only backs up an announcement lost
// without a reconnect, so it keeps running while the thread is open even with
// nothing waiting: stopping on an empty answer would hide a wait whose start
// was the lost announcement for as long as it lasts, up to half an hour.
const WAITING_POLL_MS = 15_000;
const NONE_WAITING_POLL_MS = 60_000;
const EMPTY: AutomationRun[] = [];

/**
 * Automation runs held until the turn running in a thread ends.
 *
 * Every open chat mounts this, so a failure reads as nothing waiting: no
 * retries, no toast, no error state. A backend without the runs endpoint
 * would otherwise put a retry burst in every chat. The poll carries on at the
 * idle pace, since the notice is a courtesy and the next answer settles it.
 * A chat kept alive behind another one does not poll: it refetches on
 * return, as a query that comes back into use does.
 */
export function useWaitingAutomations(
  threadId: string | null | undefined,
  active = true,
): AutomationRun[] {
  const enabled = isValidUuid(threadId) && active;
  const { data, isError } = useQuery({
    queryKey: queryKeys.automations.waiting(enabled ? threadId : ''),
    queryFn: async () =>
      (await listRecentRuns({ thread_id: threadId, status: 'waiting', limit: 10 })).data.executions,
    enabled,
    retry: false,
    refetchInterval: (q) =>
      q.state.status !== 'error' && q.state.data?.length ? WAITING_POLL_MS : NONE_WAITING_POLL_MS,
    staleTime: 10_000,
  });
  return enabled && !isError ? (data ?? EMPTY) : EMPTY;
}
