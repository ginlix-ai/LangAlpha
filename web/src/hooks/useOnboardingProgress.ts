import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import {
  getOnboardingProgress,
  patchOnboardingProgress,
  type OnboardingProgress,
  type OnboardingProgressPatch,
} from '@/api/onboarding';
import { authSessionCheck } from '@/lib/authResets';
import { queryKeys } from '@/lib/queryKeys';

/**
 * The onboarding progress record, read and written. Read again on every mount
 * and focus, since the hosted onboarding and the app on another device
 * write the same record: a setup screen that waits on the Mac passes
 * `refetchInterval` to see it move.
 *
 * A read that fails is not tried again: every reader fails open on it, and a
 * retry would only hold the app's first paint. Nor is it read again on a
 * focus, a reconnect or another reader's mount, only when a page asks
 * (`refetch`): a read with no answer goes back to loading while it runs, which
 * the app shell reads as unknown and holds every page on.
 *
 * A write stores the server's answer as it is, after dropping a read that left
 * before it; a failed one reads the record again, since its answer may be all
 * that was lost. So does one answered after a later write left, from this
 * hook or another reader's: its answer may predate that write. One answered after a sign-out or an account switch was the
 * last account's, so it rejects instead and stores nothing.
 */
/** Counts the writes sent, across every reader, so an answer knows it is the latest. */
let writesSent = 0;

const answered = (query: { state: { data: unknown } }) => query.state.data !== undefined;

export function useOnboardingProgress(options: { enabled?: boolean; refetchInterval?: number | false } = {}) {
  const queryClient = useQueryClient();
  const key = queryKeys.user.onboarding();
  const query = useQuery({
    queryKey: key,
    queryFn: getOnboardingProgress,
    enabled: options.enabled ?? true,
    refetchInterval: options.refetchInterval ?? false,
    staleTime: 0,
    retry: false,
    retryOnMount: false,
    refetchOnWindowFocus: answered,
    refetchOnReconnect: answered,
  });
  const write = useMutation({
    mutationFn: async (body: OnboardingProgressPatch) => {
      const sameAccount = authSessionCheck();
      const progress = await patchOnboardingProgress(body);
      if (!sameAccount()) throw new Error('The account changed before the record was written');
      return progress;
    },
    onMutate: () => ({ sent: ++writesSent }),
    onSuccess: async (progress: OnboardingProgress, _body, context) => {
      await queryClient.cancelQueries({ queryKey: key });
      if (context.sent === writesSent) queryClient.setQueryData(key, progress);
      else void queryClient.invalidateQueries({ queryKey: key });
    },
    onError: () => {
      void queryClient.invalidateQueries({ queryKey: key });
    },
  });
  return {
    progress: query.data ?? null,
    isLoading: query.isLoading,
    isError: query.isError,
    /** Read the record again, for a page whose read failed. */
    refetch: query.refetch,
    update: write.mutateAsync,
    isUpdating: write.isPending,
  };
}
