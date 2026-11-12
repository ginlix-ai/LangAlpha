import { useQuery } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import { isClientError } from '@/api/errors';
import { fetchThreadContext } from '../utils/api';
import { isValidUuid } from '../utils/uuid';

/** How long a count stands: reopening the panel within it shows the count it
 *  had, and after it the panel shows that count while asking again. A turn
 *  ending and a model saved end it sooner (`useThreadModel`), and an answer
 *  with nothing counted, such as a stopped computer, stands not at all. */
export const CONTEXT_STALE_MS = 60_000;

/**
 * What the thread's next call sends ahead of its messages, part by part. The
 * server counts it on request, which takes it about half a second, so only
 * the context panel calls this, and the panel exists only while it is open.
 * The panel moves the conversation row with the live window itself, so a
 * count need not follow the turn.
 */
export function useContextBreakdown(threadId: string | null | undefined) {
  const thread = isValidUuid(threadId) ? threadId : null;
  return useQuery({
    queryKey: queryKeys.threads.context(thread ?? ''),
    queryFn: () => fetchThreadContext(thread as string),
    enabled: thread !== null,
    staleTime: ({ state }) => (state.data?.parts ? CONTEXT_STALE_MS : 0),
    // One more try for a dropped request; a refusal would say the same again.
    retry: (failures, error) => failures < 1 && !isClientError(error),
    // Only an open is a reason to count.
    refetchOnWindowFocus: false,
    refetchOnReconnect: false,
  });
}
