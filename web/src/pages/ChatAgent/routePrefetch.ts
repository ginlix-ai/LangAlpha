import type { QueryClient } from '@tanstack/react-query';
import { allWorkspacesQuery } from '@/hooks/useAllWorkspaces';
import { homeWorkspaceQuery } from '@/hooks/useHomeWorkspace';
import { readGallerySort } from './components/workspaceGallery/gallerySort';
import { ThreadGallery, WorkspaceGallery } from './galleries';
import { computersList } from './hooks/useComputers';
import { primeWorkflowStatus } from './utils/primedStatus';
import { isValidUuid } from './utils/uuid';

/** A thread list's query: infinite queries stay off the entry, so it comes in a
 *  chunk of its own, asked for while what it waits on is read. */
const threadLists = () => import('./utils/threadGalleryQuery');

function prefetchThreads(queryClient: QueryClient, workspaceId: string | Promise<string>): void {
  void Promise.all([workspaceId, threadLists()])
    .then(([id, { threadGalleryQuery }]) => queryClient.prefetchInfiniteQuery(threadGalleryQuery(id, false)))
    .catch(() => undefined);
}

/**
 * Starts what a chat route opens on beside the chat page's own download: its
 * gallery's code and the reads it mounts on. None of them needs the page's
 * code, so asking at boot takes them off the page's mount: otherwise each ran
 * a round trip behind the page, the gallery's reads one more behind its code,
 * and Home's threads one more behind Home. The gallery settles as loaded, so
 * the page's first render takes it without suspending. A thread's status goes
 * out beside the lookup of its workspace (primedStatus).
 */
export function preloadChatRoute(pathname: string, queryClient: QueryClient): void {
  const [, , second, thread] = pathname.split('/');
  if (!second) {
    void WorkspaceGallery.preload();
    void queryClient.prefetchQuery(allWorkspacesQuery(readGallerySort(), true));
    void queryClient.prefetchQuery(computersList());
    prefetchThreads(queryClient, queryClient.fetchQuery(homeWorkspaceQuery(queryClient)).then((home) => home.workspace_id));
  } else if (second !== 't') {
    void ThreadGallery.preload();
    if (isValidUuid(second)) prefetchThreads(queryClient, second);
  } else if (isValidUuid(thread)) {
    primeWorkflowStatus(thread);
  }
}
