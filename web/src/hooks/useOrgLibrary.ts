import { useInfiniteQuery, useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import { getLibrary, getPublished, installFromLibrary, publishToLibrary, removeFromLibrary } from '@/api/orgLibrary';
import type { LibraryKind, LibraryPage, LibraryTarget } from '@/types/orgLibrary';
import { useFeatureEnabled } from './useFeatures';

/** Whether the library is on for this account: a member of an organization
 *  that has it. Off while the flags load, so nothing flashes in. */
export function useLibraryEnabled(): boolean {
  return useFeatureEnabled('org_library');
}

/** The organization's items a member can open now, newest first, paged on
 *  the server's cursor; a kind filter is its own list. */
export function useLibrary(kind: LibraryKind | null) {
  return useInfiniteQuery({
    queryKey: queryKeys.orgLibrary.list(kind ?? 'all'),
    queryFn: ({ pageParam }) => getLibrary(kind, pageParam),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (lastPage: LibraryPage) => lastPage.next_cursor ?? undefined,
    staleTime: 30_000,
  });
}

function targetKey(target: LibraryTarget): string {
  if (target.kind === 'thread') return `thread:${target.threadId}`;
  if (target.kind === 'file') return `file:${target.artifactId}`;
  return `skill:${target.skillName}`;
}

/** The item a thread or file of the viewer's is published as, for its share dialog. */
export function usePublished(target: LibraryTarget | null, enabled: boolean) {
  return useQuery({
    queryKey: queryKeys.orgLibrary.published(target ? targetKey(target) : ''),
    queryFn: () => getPublished(target!),
    enabled: enabled && target !== null,
    staleTime: 30_000,
  });
}

export function usePublishToLibrary() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ target, title }: { target: LibraryTarget; title?: string }) => publishToLibrary(target, title),
    onSettled: () => queryClient.invalidateQueries({ queryKey: queryKeys.orgLibrary.all }),
  });
}

export function useRemoveFromLibrary() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (itemId: string) => removeFromLibrary(itemId),
    onSettled: () => queryClient.invalidateQueries({ queryKey: queryKeys.orgLibrary.all }),
  });
}

/** Install or update a skill from the library; the viewer's skills change
 *  with it, so their lists are read again too. */
export function useInstallFromLibrary() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (itemId: string) => installFromLibrary(itemId),
    onSettled: () => {
      void queryClient.invalidateQueries({ queryKey: queryKeys.orgLibrary.all });
      void queryClient.invalidateQueries({ queryKey: queryKeys.skills.all });
    },
  });
}
