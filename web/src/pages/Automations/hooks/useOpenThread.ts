import { useCallback } from 'react';
import { useNavigate } from 'react-router';

/** Opens a run's thread. A thread URL carries no workspace, so the thread's
 *  own workspace, when the caller has it, rides in the navigation state for
 *  the chat to select; without one the chat looks the thread up. The
 *  automation's workspace is not a stand-in: it may have moved since. */
export function useOpenThread() {
  const navigate = useNavigate();
  return useCallback(
    (threadId: string | null, workspaceId?: string | null) => {
      if (!threadId) return;
      navigate(`/chat/t/${threadId}`, { state: workspaceId ? { workspaceId } : {} });
    },
    [navigate],
  );
}
