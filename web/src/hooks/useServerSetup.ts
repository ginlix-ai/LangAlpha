/**
 * What a self-hosted server has been set up with, read by the app shell (the
 * gate, Home's warm), the setup flow and Settings. A hosted server has no
 * setup of its own, so the query is off on a hosted build, and its readers
 * leave that to the query rather than each checking the build.
 */
import { queryOptions, useQuery, type QueryClient } from '@tanstack/react-query';
import { getSetupStatus, type SetupStatus } from '@/api/setup';
import { isPlatformMode } from '@/config/hostMode';
import { queryKeys } from '@/lib/queryKeys';

export const setupStatusQuery = queryOptions({
  queryKey: queryKeys.setup.status(),
  queryFn: getSetupStatus,
  enabled: !isPlatformMode,
  // A failure opens the app rather than holding it through a retry.
  retry: false,
  staleTime: 30_000,
});

/** The status, where the build reads one and the caller wants it. */
export function useSetupStatus({ enabled = true }: { enabled?: boolean } = {}) {
  return useQuery({ ...setupStatusQuery, enabled: enabled && setupStatusQuery.enabled });
}

/** Whether this server runs inside the desktop app, where it is only up while
 *  this computer is awake and the app is open. */
export function useInDesktopApp(): { inDesktopApp: boolean; isLoading: boolean } {
  const { data, isLoading } = useSetupStatus();
  return { inDesktopApp: data?.desktop_app === true, isLoading };
}

/** Three tries in all, two seconds apart. */
const WARM_READ_RETRIES = 2;
const WARM_READ_RETRY_MS = 2000;

/**
 * The status, for what must not run on a guess: warming Home creates a computer,
 * which ends the step for good, so the failed read that opened the app is not an
 * answer. A read that works reaches the gate through the cache, which then sends
 * a server not yet set up to the step. Null on a hosted build, or once the tries
 * run out, so a server that never answers cannot hold the warm forever.
 */
export async function readSetupStatusForWarm(queryClient: QueryClient): Promise<SetupStatus | null> {
  // `fetchQuery` does not read `enabled`, so the query's own gate is read here.
  if (setupStatusQuery.enabled === false) return null;
  try {
    return await queryClient.ensureQueryData({
      ...setupStatusQuery,
      retry: WARM_READ_RETRIES,
      retryDelay: WARM_READ_RETRY_MS,
    });
  } catch {
    return null;
  }
}
