import { useMutation, useQuery, useQueryClient, type UseQueryResult } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import { workspaceRelativePath } from '@/pages/ChatAgent/utils/agentPaths';
import { retryUnlessClientError } from '@/api/errors';
import {
  createShareLink,
  listSharedLinks,
  patchShareLink,
  type ShareLinkPatch,
} from '@/pages/ChatAgent/utils/api/shareLinks';
import type { ShareLink, ShareLinkTarget, SharedLinksResponse } from '@/types/api';

/** Read the same link back whichever spelling of the file asked for it. */
function targetKey(target: ShareLinkTarget): string {
  return target.kind === 'file' ? workspaceRelativePath(target.path) : String(target.port);
}

/**
 * An app's stable `/a/<code>` link, minted on first read. A file's link is
 * its record's code, which the owner's views make (`useOwnedArtifact`).
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
 * Stop sharing a link. The answer is the link itself, so it replaces every
 * cached entry holding that code, and the workspace's shared list is re-read.
 * A file's link stops through its artifact (`useArtifactPublishing`) where
 * the record is at hand, which reads the record's views again too.
 */
export function useShareLinkMutations(workspaceId: string | null | undefined) {
  const queryClient = useQueryClient();

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
    },
  });

  return { patch };
}
