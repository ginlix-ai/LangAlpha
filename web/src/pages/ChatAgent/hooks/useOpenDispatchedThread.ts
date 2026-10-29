import { useNavigate } from 'react-router';
import { useRouteLeaveGuard } from '../contexts/RouteLeaveGuardContext';

/** The thread that hands work out, which a dispatched thread opened from it links back to. */
export interface DispatcherContext {
  threadId: string;
  workspaceId: string;
}

/**
 * Opens a dispatched thread, linked back to `from`: the hand-off card, the
 * report's caption and the composer's tray all open one this way. Asks the
 * host's leave guard first, since leaving the route drops the file panel's
 * unsaved drafts.
 */
export function useOpenDispatchedThread(from: DispatcherContext | null | undefined) {
  const navigate = useNavigate();
  const guardLeave = useRouteLeaveGuard();
  return (threadId: string, workspaceId?: string | null) => guardLeave(() => navigate(`/chat/t/${threadId}`, {
    state: {
      ...(workspaceId ? { workspaceId } : {}),
      ...(from ? { fromThreadId: from.threadId, fromWorkspaceId: from.workspaceId } : {}),
    },
  }));
}
