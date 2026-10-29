import { queryOptions, useQuery } from '@tanstack/react-query';
import { apiErrorStatus } from '@/api/errors';
import { queryKeys } from '@/lib/queryKeys';
import { getDeliveryOptions } from '@/pages/Automations/utils/api';

/** The chats each linked app offers an automation, asked for one workspace or
 *  for none. Automations reads the chats; Settings reads which apps are linked. */
export function deliveryOptionsQuery(workspaceId: string | null) {
  return queryOptions({
    queryKey: queryKeys.automationDelivery.options(workspaceId),
    queryFn: async () => (await getDeliveryOptions(workspaceId)).data,
    staleTime: 60_000,
    // An app is linked in the console, in another tab, so coming back asks
    // again however fresh the answer is.
    refetchOnWindowFocus: 'always',
    // A refusal or an unavailable service answers the same on a retry; only
    // a request that never got an answer is asked again.
    retry: (count, err) => count < 1 && apiErrorStatus(err) === null,
  });
}

/** The chat apps linked to the account, or null until the server has said it
 *  has a messaging service at all. */
export function useLinkedChatApps(): ReadonlySet<string> | null {
  const { data } = useQuery(deliveryOptionsQuery(null));
  return data?.enabled ? new Set(Object.keys(data.apps ?? {})) : null;
}
