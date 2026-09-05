import { matchPath, useLocation } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import { getThread } from '@/pages/ChatAgent/utils/api';
import { isValidUuid } from '@/pages/ChatAgent/utils/uuid';

interface ChatRouteState {
  workspaceId?: string;
  [key: string]: unknown;
}

/**
 * Current chat workspace/thread derived from the URL, for the app-shell
 * sidebar (mounted outside ChatAgent's route element, so useParams can't see
 * the chat params). Mirrors ChatAgent's resolution order: URL workspace >
 * route state > thread-detail lookup; the lookup shares ChatAgent's query key,
 * so at most one of the two ever fetches.
 */
export function useChatRoute(): { currentWorkspaceId: string | null; currentThreadId: string | null } {
  const location = useLocation();
  const state = location.state as ChatRouteState | null;

  const threadMatch = matchPath('/chat/t/:threadId/:taskId', location.pathname)
    || matchPath('/chat/t/:threadId', location.pathname);
  const threadId = threadMatch?.params.threadId || null;

  const wsMatch = !threadMatch ? matchPath('/chat/:workspaceId', location.pathname) : null;
  const urlWorkspaceId = wsMatch && isValidUuid(wsMatch.params.workspaceId) ? wsMatch.params.workspaceId! : null;
  const stateWorkspaceId = state?.workspaceId && isValidUuid(state.workspaceId) ? state.workspaceId : null;

  const needsLookup = !!threadId && threadId !== '__default__' && !urlWorkspaceId && !stateWorkspaceId;
  const { data: threadDetail } = useQuery({
    queryKey: queryKeys.threads.detail(threadId!),
    queryFn: () => getThread(threadId!),
    enabled: needsLookup,
    retry: false,
  });
  const lookedUpWorkspaceId = (threadDetail as { workspace_id?: string } | undefined)?.workspace_id || null;

  return {
    currentThreadId: threadId,
    currentWorkspaceId: urlWorkspaceId ?? stateWorkspaceId ?? (needsLookup ? lookedUpWorkspaceId : null),
  };
}
