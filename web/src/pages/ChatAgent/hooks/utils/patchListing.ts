import type { QueryClient, QueryKey } from '@tanstack/react-query';

/**
 * Shows a mutation's answer in the cached listing it changed, or reads the
 * listing again when the answer carries nothing to show or nothing is cached.
 * A read in flight began before the mutation landed and would undo it.
 */
export async function patchListingAfterMutation<T>(
  queryClient: QueryClient,
  queryKey: QueryKey,
  patch: ((prev: T) => T) | null,
): Promise<void> {
  if (!patch) return queryClient.invalidateQueries({ queryKey });
  await queryClient.cancelQueries({ queryKey });
  const prev = queryClient.getQueryData<T>(queryKey);
  if (prev === undefined) return queryClient.invalidateQueries({ queryKey });
  queryClient.setQueryData<T>(queryKey, patch(prev));
}
