import { useEffect, useEffectEvent, useMemo, useState } from 'react';
import { useWorkspaceFileGrant } from '@/hooks/useWorkspaceFileGrant';
import { useWorkspaceFolders } from '../../../contexts/WorkspaceContext';
import type { FrameSource } from './BridgedFrame';
import type { PageBridge } from './usePageBridge';
import { SERVED_HTML_SANDBOX } from './sandbox';
import { buildServeUrl, type ServeInject } from './wsfilesUrl';

interface LiveFrameOptions {
  workspaceId: string;
  /** Workspace-relative, or as the panel's tab holds it. */
  filePath: string;
  /** A serve prefix the page was handed (a share's, `/a/`'s `frame_base`); omitted, the owner's grant. */
  prefix?: string | null;
  inject: ServeInject;
  /**
   * Which bytes of the file to show. A new one loads the file again, over the
   * old one with the place kept; the same one keeps the browser's copy.
   */
  revision?: string;
  /** The section a reference pointed at when the frame first loaded. */
  anchor?: string | null;
}

export interface LiveFrame {
  /** Null until the prefix is known. */
  source: FrameSource | null;
  /** The owner's grant never arrived, so nothing will load until `retry`. */
  failed: boolean;
  retry: () => void;
}

/**
 * A workspace file framed with its bridge. The fragment lands the first load
 * of a section; a later ask goes to the document (`useFragmentScroll`), which
 * keeps its place otherwise.
 */
export function useLiveFrame({ workspaceId, filePath, prefix: given = null, inject, revision, anchor = null }: LiveFrameOptions): LiveFrame {
  const root = useWorkspaceFolders()?.rootDir;
  const grant = useWorkspaceFileGrant(given ? null : workspaceId);
  const prefix = given ?? grant.data?.prefix;
  // The viewer stays mounted from tab to tab, and the panel from workspace to
  // workspace, where the same path is another file. So a file's first fragment
  // is held with its file and never lands on the next one.
  const file = `${workspaceId}:${filePath}`;
  const [first, setFirst] = useState({ file, anchor });
  if (first.file !== file) setFirst({ file, anchor });
  const firstAnchor = first.file === file ? first.anchor : anchor;
  const source = useMemo<FrameSource | null>(() => {
    if (!prefix) return null;
    const url = buildServeUrl(prefix, filePath, { inject, root, revision });
    return {
      key: `${inject}:${file}:${revision ?? ''}`,
      src: firstAnchor ? `${url}#${encodeURIComponent(firstAnchor)}` : url,
      sandbox: SERVED_HTML_SANDBOX,
    };
  }, [prefix, file, filePath, inject, root, revision, firstAnchor]);
  return {
    source,
    // A failed renewal keeps the grant it already had, and the frame keeps
    // loading under it; only a grant that never arrived is an error to show.
    failed: !prefix && !!grant.error,
    retry: () => void grant.refetch(),
  };
}

/**
 * Scrolls the document to the section a reference asked for, every time it
 * asks: a second reference to the section the URL already names moves neither
 * `src` nor the frame's location, so only the document can land it.
 * `anchorSeq` counts the asks.
 */
export function useFragmentScroll(bridge: PageBridge | null, anchor: string | null, anchorSeq: number | null): void {
  const request = anchor ? `${anchorSeq ?? ''}\u0000${anchor}` : null;
  const ask = useEffectEvent(() => {
    if (anchor && bridge?.ready) bridge.post({ type: 'page:scroll-to', fragment: anchor });
  });
  useEffect(() => { ask(); }, [request, bridge?.ready]);
}
