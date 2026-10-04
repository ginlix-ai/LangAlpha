import { keepPreviousData, useQuery, useQueryClient } from '@tanstack/react-query';
import { flashWorkspaceQuery } from '@/hooks/useFlashWorkspace';
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
  /** The workspace the run delivers from: a Flash run's is the Flash workspace. */
  workspaceId: string | null;
}

/**
 * The chats an automation can deliver to, for the workspace its runs use.
 * A Flash automation names none, but runs in the account's Flash workspace,
 * whose default output is the one its bare entries follow.
 */
export function useDeliveryOptions({ agentMode, workspaceId, enabled = true }: DeliveryOptionsArgs): DeliveryOptionsState {
  const queryClient = useQueryClient();
  const flash = useQuery({ ...flashWorkspaceQuery(queryClient), enabled: enabled && agentMode === 'flash' });
  const runWorkspace = agentMode === 'flash' ? flash.data?.workspace_id ?? null : workspaceId || null;

  const query = useQuery({
    queryKey: queryKeys.automationDelivery.options(runWorkspace ?? ''),
    queryFn: async () => (await getDeliveryOptions(runWorkspace as string)).data,
    enabled: enabled && !!runWorkspace,
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
    workspaceId: runWorkspace,
  };
}
