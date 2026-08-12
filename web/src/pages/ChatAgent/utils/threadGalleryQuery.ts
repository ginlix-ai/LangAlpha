/**
 * ThreadGallery's paged thread list. Shared with ChatAgent's hover prefetch so
 * the warm entry lands on the SAME infinite key the gallery mounts — a finite
 * payload written to a key the gallery reads as InfiniteData would ghost the
 * whole list.
 */
import { infiniteQueryOptions } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import type { ThreadsResponse } from '@/types/api';
import { getWorkspaceThreads } from './api';

export const THREAD_GALLERY_PAGE_SIZE = 20;

export function threadGalleryQuery(workspaceId: string, archived: boolean) {
  return infiniteQueryOptions({
    queryKey: queryKeys.threads.gallery(workspaceId, archived),
    queryFn: ({ pageParam }) =>
      getWorkspaceThreads(workspaceId, THREAD_GALLERY_PAGE_SIZE, pageParam, null, archived) as Promise<ThreadsResponse>,
    initialPageParam: 0,
    // Offset paging: the next page starts where the loaded rows end, so a row
    // archived out of an earlier page can't make the tail skip an entry.
    getNextPageParam: (lastPage: ThreadsResponse, allPages: ThreadsResponse[]) => {
      const loaded = allPages.reduce((n, page) => n + (page.threads?.length ?? 0), 0);
      return loaded < (lastPage.total ?? 0) ? loaded : undefined;
    },
    staleTime: 30_000,
  });
}
