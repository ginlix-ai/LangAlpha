import { useEffect } from 'react';
import { useNavigate } from 'react-router';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import { threadWorkspaceFor } from '@/lib/threadWorkspace';
import { useSharedWorkspaces } from '@/hooks/useSharedWorkspaces';
import { listsSharedWorkspace, subscribeSharedThreadGone, subscribeSharedWorkspaceGone } from '@/lib/sharedWorkspaceFeed';
import { leaveGoneWorkspace, sharedGoneNav } from '@/lib/sharedGone';
import { onThreadRefused } from '@/lib/threadRefusals';
import { useLatestRef } from '@/hooks/useLatestRef';
import { useStableHandler } from '@/hooks/useStableHandler';
import { apiErrorStatus } from '@/api/errors';
import type { Thread } from '@/types/api';
import { threadDetailQuery } from '../utils/threadQueries';
import { shouldLeaveThreadRoute } from '../utils/threadRouteGuard';
import type { useChatViewCache } from './useChatViewCache';

/**
 * The workspace a chat route shows, and leaving it once the viewer no longer
 * reaches it. A thread belongs to one workspace, so the one in view before it
 * is no stand-in when the navigation names none (back/forward to an entry
 * without state, a link from outside the chat): its own comes from the
 * navigation, a cached view of it, or a lookup, and until one of them has it
 * there is none. With shared workspaces on the thread is read even when the
 * navigation names its workspace, so a name that puts it on the wrong side of
 * a shared workspace is corrected.
 *
 * A shared workspace's thread the viewer can no longer read (removed from the
 * workspace, or out of the organization, while it was open), a shared
 * workspace the feed says they lost or that was deleted, and a thread of one
 * another member deleted: every view of it goes, so nothing keeps asking for
 * it, and a page showing it leaves for the list that says why.
 */
export function useThreadRoute({ threadId, urlWorkspaceId, namedWorkspaceId, lastWorkspaceId, cache }: {
  threadId: string | undefined;
  urlWorkspaceId: string | undefined;
  /** The workspace the URL or the navigation's state names. */
  namedWorkspaceId: string | null;
  /** The workspace last in view, which a new thread (`__default__`) opens in. */
  lastWorkspaceId: string | null;
  cache: ReturnType<typeof useChatViewCache>;
}): { workspaceId: string | null; shownWorkspaceId: string | null; accessDenied: boolean; sharedAccessEnded: boolean } {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const existingThread = !!threadId && threadId !== '__default__';
  const needsThreadLookup = existingThread && !namedWorkspaceId;
  const { data: sharedList, enabled: sharedOn } = useSharedWorkspaces();
  const checksOwnWorkspace = sharedOn && existingThread;
  const { data: thread, error: threadError } = useQuery({
    ...threadDetailQuery(threadId!),
    enabled: needsThreadLookup || checksOwnWorkspace,
  });
  const accessDenied = apiErrorStatus(threadError) === 403;

  useEffect(() => {
    if (shouldLeaveThreadRoute(needsThreadLookup, threadError, accessDenied, !!thread)) {
      navigate('/chat', { replace: true });
    }
  }, [needsThreadLookup, threadError, accessDenied, thread, navigate]);

  const ownThread = checksOwnWorkspace && thread?.workspace_id
    ? { workspaceId: thread.workspace_id, shared: !!thread.shared }
    : null;
  const workspaceId = existingThread
    ? threadWorkspaceFor(
      namedWorkspaceId || cache.entries.find((e) => e.threadId === threadId)?.workspaceId || null,
      ownThread,
      (id) => listsSharedWorkspace(sharedList, id),
    ) || thread?.workspace_id || null
    : namedWorkspaceId || lastWorkspaceId;
  // The gallery of every workspace shows none, whatever was resolved last.
  const shownWorkspaceId = threadId || urlWorkspaceId ? workspaceId : null;

  const strayView = !!ownThread && workspaceId === ownThread.workspaceId
    && cache.entries.some((e) => e.threadId === threadId && e.workspaceId !== workspaceId);
  useEffect(() => {
    if (strayView && threadId && workspaceId) cache.keepOnlyIn(threadId, workspaceId);
  }, [strayView, threadId, workspaceId]); // eslint-disable-line react-hooks/exhaustive-deps

  // Found by a refused stream, or by a read of the thread refused after one
  // that showed it shared (a fresh load has no such read, and keeps the
  // access page). Any other thread is left as it was.
  const routeThreadRef = useLatestRef(threadId);
  const leaveSharedThread = useStableHandler((goneId: string) => {
    void queryClient.invalidateQueries({ queryKey: queryKeys.sharedWorkspaces.all });
    const detail = queryClient.getQueryData<Thread>(queryKeys.threads.detail(goneId));
    cache.evict(new Set([goneId]));
    void leaveGoneWorkspace(navigate, detail?.workspace_id, () => routeThreadRef.current === goneId, { threadLost: true });
  });
  useEffect(() => onThreadRefused((refusedId) => {
    if (queryClient.getQueryData<Thread>(queryKeys.threads.detail(refusedId))?.shared) leaveSharedThread(refusedId);
  }), [queryClient, leaveSharedThread]);
  // A shared workspace's thread the viewer no longer reaches answers as no
  // thread at all, so a 404 after a read that showed it shared ends it too.
  const sharedAccessEnded = !!thread?.shared && (accessDenied || apiErrorStatus(threadError) === 404);
  useEffect(() => {
    if (sharedAccessEnded && threadId) leaveSharedThread(threadId);
  }, [sharedAccessEnded, threadId, leaveSharedThread]);

  const shownWorkspaceRef = useLatestRef(shownWorkspaceId);
  useEffect(() => subscribeSharedWorkspaceGone(({ workspaceId: goneWs, reason }) => {
    const views = cache.entries.filter((e) => e.workspaceId === goneWs).map((e) => e.threadId);
    if (shownWorkspaceRef.current === goneWs) navigate('/chat', sharedGoneNav(reason));
    if (views.length) cache.evict(new Set(views));
  }), [cache, navigate, shownWorkspaceRef]);
  useEffect(() => subscribeSharedThreadGone(({ threadId: goneId, workspaceId: inWs, reason }) => {
    if (routeThreadRef.current === goneId) navigate(`/chat/${inWs}`, sharedGoneNav(reason));
    cache.evict(new Set([goneId]));
  }), [cache, navigate, routeThreadRef]);

  return { workspaceId, shownWorkspaceId, accessDenied, sharedAccessEnded };
}
