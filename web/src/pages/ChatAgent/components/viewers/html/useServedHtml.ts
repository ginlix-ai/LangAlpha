import { useShareLink } from '@/hooks/useShareLink';
import { useWorkspaceFileGrant } from '@/hooks/useWorkspaceFileGrant';
import { shareLinkHref } from '../../../utils/api/shareLinks';
import { useWorkspaceFolders } from '../../../contexts/WorkspaceContext';
import { buildServeUrl } from './wsfilesUrl';

export interface ServedHtml {
  /** Byte-faithful, what the PDF render is fetched from. Absent until the prefix is known. */
  plainUrl?: string;
  /** With `?inject=theme`, what the preview frames load. */
  themedUrl?: string;
  /** What "open in new tab" and the print fallback open. */
  openUrl?: string;
  /** The owner's grant could not be minted, so nothing will load until `retry`. */
  error: Error | null;
  retry: () => void;
}

/**
 * The URLs one HTML file is served under, for the owner's panel and a share's.
 *
 * The owner's ride the workspace's signed grant, and a new tab opens the
 * file's own `/a/` page instead, since the served URL carries the grant. A
 * share passes the prefix its page was handed, which is already scoped to
 * what it shares, so the served URL is what opens there. A null `filePath`
 * asks for nothing.
 */
export function useServedHtml(
  workspaceId: string | null | undefined,
  filePath: string | null,
  servePrefix?: string,
): ServedHtml {
  const owner = !servePrefix && !!filePath;
  const grant = useWorkspaceFileGrant(owner ? workspaceId : null);
  const { data: link } = useShareLink(
    owner ? workspaceId : null,
    owner && filePath ? { kind: 'file', path: filePath } : null,
  );

  const root = useWorkspaceFolders()?.rootDir;

  const prefix = servePrefix ?? grant.data?.prefix;
  const plainUrl = prefix && filePath ? buildServeUrl(prefix, filePath, { root }) : undefined;
  const themedUrl = prefix && filePath ? buildServeUrl(prefix, filePath, { injectTheme: true, root }) : undefined;
  const openUrl = servePrefix ? plainUrl : link ? shareLinkHref(link.code) : undefined;

  return {
    plainUrl,
    themedUrl,
    openUrl,
    // A failed renewal keeps the grant it already had, and the frame keeps
    // loading under it; only a grant that never arrived is an error to show.
    error: owner && !grant.data ? grant.error ?? null : null,
    retry: () => void grant.refetch(),
  };
}
