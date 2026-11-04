import React, { useEffect, useState } from 'react';
import { artifactPathKey } from '@/hooks/useArtifact';
import { useWorkspaceFolders } from '../../contexts/WorkspaceContext';
import PublishDialog from '../PublishDialog';
import type { HtmlArtifactView } from '../viewers/html/ArtifactFrames';
import { CommentTools } from '../viewers/html/comments/CommentTools';
import { LazyCommentLayer } from '../viewers/html/comments/lazyCommentLayer';
import type { CommentUi } from '../viewers/html/comments/useCommentUi';
import { ArtifactCrumbs } from './ArtifactCrumbs';
import { useArtifactVersionView, usePanelVersionStores, versionKey, LIVE_VIEW, type VersionStore } from './useArtifactVersionView';

/** A view with no file to keep a version for. */
const NO_STORE: VersionStore = { state: LIVE_VIEW, dispatch: () => {} };

interface HtmlArtifactTabOptions {
  workspaceId: string;
  tabId: string;
  /** The file in front, if a file tab is. */
  file: string | null;
  /** Its HTML viewer is up: not loading as another kind, not being edited. */
  html: boolean;
  /** The markup is on screen rather than the page. */
  source: boolean;
  /** The panel is the one in front. */
  active: boolean;
  /** When the panel last read the file's bytes. */
  readAt: number | null;
  /** The owner's comment controls; left out, the panel is not the owner's and the file shows alone. */
  ui: CommentUi | undefined;
}

export interface HtmlArtifactTab {
  /** The HTML file in front that takes comments and has versions, or null. */
  path: string | null;
  /**
   * The document its frame shows (workspace-relative): `path`, or another
   * file a link inside it led to. The comment box, the toolbar and the
   * header's menu all name this one.
   */
  doc: string | null;
  crumbs: React.ReactNode;
  tools: React.ReactNode;
  view: HtmlArtifactView | null;
  /** Its line count gives way to the version and comment controls, except under Source. */
  hideMeta: boolean;
  /** Opens the one Publish dialog on a file, any kind; null where the panel is not the owner's. */
  openPublish: ((path: string) => void) | null;
  dialog: React.ReactNode;
}

/**
 * The owner's file panel around the file in front: an HTML file's versions
 * and comment controls in the crumb row and the view its frames take, and the
 * Publish dialog every file's header and crumb opens. The HTML file's record
 * is made as soon as its tab is in front (`useArtifactVersionView`).
 */
export function useHtmlArtifactTab({
  workspaceId, tabId, file, html, source, active, readAt, ui,
}: HtmlArtifactTabOptions): HtmlArtifactTab {
  const root = useWorkspaceFolders()?.rootDir;
  const path = ui && html ? file : null;
  const stores = usePanelVersionStores();
  const versionView = useArtifactVersionView(
    path ? workspaceId : null,
    path,
    path ? stores(versionKey(tabId, path)) : NO_STORE,
    { root },
  );
  // Kept through its own close, so the dialog's exit plays over the file it was for.
  const [publishing, setPublishing] = useState<{ path: string; open: boolean } | null>(null);

  // The comment layer's code is fetched while the file loads, so it draws
  // with the file rather than suspending once it is up.
  useEffect(() => {
    if (path) void LazyCommentLayer.preload();
  }, [path]);

  const openPublish = ui ? (target: string) => setPublishing({ path: target, open: true }) : null;
  return {
    path,
    doc: path && ui ? ui.docOf(tabId, artifactPathKey(path, root)) : null,
    crumbs: path && openPublish && (
      <ArtifactCrumbs view={versionView} onReview={() => openPublish(path)} readAt={readAt} />
    ),
    tools: path && ui && (
      <CommentTools
        tabId={tabId}
        workspaceId={workspaceId}
        path={path}
        ui={ui}
        variant="crumb"
        // A version is read only, and the markup has no spots to pick.
        modeDisabled={versionView.version !== null || source}
      />
    ),
    view: path && ui ? { tabId, ui, active, versionView } : null,
    hideMeta: !!path && !source,
    openPublish,
    dialog: publishing && (
      <PublishDialog
        open={publishing.open}
        workspaceId={workspaceId}
        path={publishing.path}
        onClose={() => setPublishing((prev) => (prev ? { ...prev, open: false } : prev))}
      />
    ),
  };
}
