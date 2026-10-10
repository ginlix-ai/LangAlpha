import { keepPreviousData, useQuery } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import { apiErrorStatus } from '@/pages/ChatAgent/utils/api/errors';
import type { Automation, DeliveryOptions } from '@/types/automation';
import { getDeliveryOptions } from '../utils/api';

interface DeliveryOptionsArgs {
  agentMode: Automation['agent_mode'];
  /** The automation's own workspace, which only a PTC automation names. */
  workspaceId: string | null | undefined;
  /** Off where nothing on screen reads the options. */
  enabled?: boolean;
}

export interface DeliveryOptionsState {
  options: DeliveryOptions | undefined;
  error: unknown;
  /** The options are still the previous workspace's, until this one's load. */
  isPlaceholderData: boolean;
  /** No answer yet, for this workspace or any before it. */
  isPending: boolean;
  /** The workspace whose default an app-only entry follows, as the server
   *  answered: null for a run in no workspace or in Home. */
  workspaceId: string | null;
}

/**
 * The chats an automation can deliver to. An automation in no workspace asks
 * with none; the workspace whose default applies is the server's answer, null
 * for a run in Home.
 */
export function useDeliveryOptions({ agentMode, workspaceId, enabled = true }: DeliveryOptionsArgs): DeliveryOptionsState {
  const asked = agentMode === 'flash' ? null : workspaceId || null;

  const query = useQuery({
    queryKey: queryKeys.automationDelivery.options(asked),
    queryFn: async () => (await getDeliveryOptions(asked)).data,
    // A PTC form with no workspace picked yet asks nothing.
    enabled: enabled && (agentMode === 'flash' || !!asked),
    staleTime: 60_000,
    // A workspace switch keeps the picker up while the new one's chats load.
    placeholderData: keepPreviousData,
    // A refusal or an unavailable service answers the same on a retry; only
    // a request that never got an answer is asked again.
    retry: (count, err) => count < 1 && apiErrorStatus(err) === null,
  });

  return {
    options: query.data,
    error: query.error,
    isPlaceholderData: query.isPlaceholderData,
    isPending: query.isPending,
    workspaceId: query.data?.workspace_id ?? null,
  };
}
