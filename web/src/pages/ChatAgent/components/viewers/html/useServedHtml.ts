import { useArtifactByPath, useOwnedArtifact } from '@/hooks/useArtifact';
import { useWorkspaceFileGrant } from '@/hooks/useWorkspaceFileGrant';
import { useWorkspaceFolders } from '../../../contexts/WorkspaceContext';
import type { PanelAccess } from '../../filePanel/types';
import { buildServeUrl } from './wsfilesUrl';

export interface ServedHtml {
  /** Byte-faithful, what the PDF render is fetched from. Absent until the prefix is known. */
  plainUrl?: string;
  /** What "open in new tab" and the print fallback open. */
  openUrl?: string;
  /** The owner's grant could not be minted, so nothing will load until `retry`. */
  error: Error | null;
  retry: () => void;
}

/**
 * The URLs one HTML file is served under, for the panel `access` describes.
 *
 * The owner's ride the workspace's signed grant, and a new tab opens the
 * file's own `/a/` page instead, since the served URL carries the grant; that
 * page is the file's record, made here if it has none (`useOwnedArtifact`). A
 * read-only look at a sibling's file is served the same way but makes no
 * record, so it offers the page only for a file that already has one. A
 * share passes the prefix its page was handed, which is already scoped to
 * what it shares, so the served URL is what opens there. A null `filePath`
 * asks for nothing.
 */
export function useServedHtml(
  workspaceId: string | null | undefined,
  filePath: string | null,
  access: PanelAccess,
): ServedHtml {
  const sharePrefix = access.kind === 'share' ? access.servePrefix : null;
  const granted = !sharePrefix && !!filePath;
  const grant = useWorkspaceFileGrant(granted ? workspaceId : null);
  const root = useWorkspaceFolders()?.rootDir;
  const owned = useOwnedArtifact(access.kind === 'owner' ? workspaceId : null, filePath, { root });
  const found = useArtifactByPath(access.kind === 'readOnly' ? workspaceId : null, filePath, { root });
  const record = owned.record ?? found.data ?? undefined;

  const prefix = sharePrefix ?? grant.data?.prefix;
  const plainUrl = prefix && filePath ? buildServeUrl(prefix, filePath, { root }) : undefined;
  const openUrl = sharePrefix ? plainUrl : record?.url;

  return {
    plainUrl,
    openUrl,
    // A failed renewal keeps the grant it already had, and the frame keeps
    // loading under it; only a grant that never arrived is an error to show.
    error: granted && !grant.data ? grant.error ?? null : null,
    retry: () => void grant.refetch(),
  };
}
