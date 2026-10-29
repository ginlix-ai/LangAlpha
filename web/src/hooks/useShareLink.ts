import { useMutation, useQuery, useQueryClient, type UseQueryResult } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import { workspaceRelativePath } from '@/pages/ChatAgent/utils/agentPaths';
import { retryUnlessClientError } from '@/api/errors';
import {
  createShareLink,
  getShareLinkFiles,
  listSharedLinks,
  patchShareLink,
  shareConflictIn,
  type ShareLinkPatch,
} from '@/pages/ChatAgent/utils/api/shareLinks';
import type { ShareLink, ShareLinkFiles, ShareLinkTarget, SharedLinksResponse } from '@/types/api';

/** Read the same link back whichever spelling of the file asked for it. */
function targetKey(target: ShareLinkTarget): string {
  return target.kind === 'file' ? workspaceRelativePath(target.path) : String(target.port);
}

/**
 * An item's stable `/a/<code>` link, minted on first read.
 *
 * The read is a get-or-create POST: the link exists from the moment anything
 * shows it, private, so a click that copies or opens it never has to wait on
 * the network and can call `window.open` inside the user's gesture.
 */
export function useShareLink(
  workspaceId: string | null | undefined,
  target: ShareLinkTarget | null,
  { enabled = true }: { enabled?: boolean } = {},
): UseQueryResult<ShareLink, Error> {
  return useQuery<ShareLink, Error>({
    queryKey: queryKeys.shareLinks.link(workspaceId ?? '', target?.kind ?? '', target ? targetKey(target) : ''),
    queryFn: () => createShareLink(workspaceId!, target!),
    enabled: enabled && !!workspaceId && !!target,
    staleTime: 5 * 60_000,
    retry: retryUnlessClientError,
  });
}

/**
 * The current file list a share would expose. Always re-read on mount: the
 * dialog that shows it is the review step, and it has to describe the files
 * as they are now, not as they were when it last opened.
 */
export function useShareLinkFiles(
  workspaceId: string | null | undefined,
  code: string | null | undefined,
  { enabled = true }: { enabled?: boolean } = {},
): UseQueryResult<ShareLinkFiles, Error> {
  return useQuery<ShareLinkFiles, Error>({
    queryKey: queryKeys.shareLinks.files(workspaceId ?? '', code ?? ''),
    queryFn: () => getShareLinkFiles(workspaceId!, code!),
    enabled: enabled && !!workspaceId && !!code,
    staleTime: 0,
    refetchOnMount: 'always',
    retry: retryUnlessClientError,
  });
}

/** The shared links in a workspace, newest first. */
export function useSharedLinks(workspaceId: string | null | undefined): UseQueryResult<ShareLink[], Error> {
  return useQuery<SharedLinksResponse, Error, ShareLink[]>({
    queryKey: queryKeys.shareLinks.shared(workspaceId ?? ''),
    queryFn: () => listSharedLinks(workspaceId!),
    select: (data) => data.links,
    enabled: !!workspaceId,
    staleTime: 30_000,
    retry: retryUnlessClientError,
  });
}

/**
 * Share, update, or stop. The answer is the link itself, so it replaces every
 * cached entry holding that code, and the workspace's shared list and the
 * link's file list (whose drift just changed) are re-read. A 409 re-reads
 * whatever it says is out of date.
 */
export function useShareLinkMutations(workspaceId: string | null | undefined) {
  const queryClient = useQueryClient();

  const refetchFiles = (ws: string, code: string) =>
    void queryClient.invalidateQueries({ queryKey: queryKeys.shareLinks.files(ws, code) });

  const refetchLink = (ws: string, code: string) => {
    void queryClient.invalidateQueries({
      queryKey: queryKeys.shareLinks.links(ws),
      predicate: (query) => (query.state.data as ShareLink | undefined)?.code === code,
    });
    void queryClient.invalidateQueries({ queryKey: queryKeys.shareLinks.shared(ws) });
    refetchFiles(ws, code);
  };

  const patch = useMutation({
    mutationFn: ({ code, patch: body }: { code: string; patch: ShareLinkPatch }) =>
      patchShareLink(workspaceId!, code, body),
    onSuccess: (link) => {
      if (!workspaceId) return;
      // A panel may have asked under another spelling of the same file, so the
      // code, not the key, says which cached entries this answer replaces.
      queryClient.setQueriesData<ShareLink>(
        { queryKey: queryKeys.shareLinks.links(workspaceId) },
        (cached) => (cached?.code === link.code ? link : cached),
      );
      void queryClient.invalidateQueries({ queryKey: queryKeys.shareLinks.shared(workspaceId) });
      refetchFiles(workspaceId, link.code);
    },
    onError: (err, { code }) => {
      if (!workspaceId) return;
      const conflict = shareConflictIn(err);
      if (conflict === 'files_changed') refetchFiles(workspaceId, code);
      else if (conflict === 'link_changed') refetchLink(workspaceId, code);
    },
  });

  return { patch };
}
