import { keepPreviousData, useQuery } from '@tanstack/react-query';
import { deliveryOptionsQuery } from '@/hooks/useLinkedChatApps';
import type { DeliveryOptions } from '@/types/automation';

interface DeliveryOptionsArgs {
  /** The workspace the automation runs in, Home's id included. */
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
 * The chats an automation can deliver to, asked with the workspace it runs
 * in. The workspace whose default applies is the server's answer, null for a
 * run in Home, which has no default chat.
 */
export function useDeliveryOptions({ workspaceId, enabled = true }: DeliveryOptionsArgs): DeliveryOptionsState {
  const asked = workspaceId || null;

  const query = useQuery({
    ...deliveryOptionsQuery(asked),
    // A form with no place picked yet asks nothing.
    enabled: enabled && !!asked,
    // A workspace switch keeps the picker up while the new one's chats load.
    placeholderData: keepPreviousData,
  });

  return {
    options: query.data,
    error: query.error,
    isPlaceholderData: query.isPlaceholderData,
    isPending: query.isPending,
    workspaceId: query.data?.workspace_id ?? null,
  };
}
