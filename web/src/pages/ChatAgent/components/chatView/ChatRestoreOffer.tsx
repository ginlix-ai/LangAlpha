import { useEffect } from 'react';
import { useQueryClient } from '@tanstack/react-query';

import { queryKeys } from '@/lib/queryKeys';

import { useComputers } from '../../hooks/useComputers';
import { RestoreOffer } from '../RestoreOffer';

/**
 * The restore offer above the chat input, for a workspace on a machine the
 * user brought. A turn re-reads the workspace row once it has the machine, so
 * an offer the turn records shows then. Opening the chat lists the folder
 * first, which can record one too, after the row was read: so once that
 * listing lands, the row is read again.
 */
export function ChatRestoreOffer({ workspaceId, computerId, listed }: {
  workspaceId: string;
  computerId: string;
  /** The chat's first file listing has come back. */
  listed: boolean;
}) {
  const queryClient = useQueryClient();
  const { data } = useComputers();
  const brought = data?.computers?.find((c) => c.computer_id === computerId)?.kind === 'ssh';

  useEffect(() => {
    if (!brought || !listed) return;
    void queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.detail(workspaceId), exact: true });
  }, [brought, listed, queryClient, workspaceId]);

  if (!brought) return null;
  return <RestoreOffer workspaceId={workspaceId} variant="banner" />;
}
