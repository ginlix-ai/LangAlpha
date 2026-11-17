import React, { useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { artifactPathKey } from '@/hooks/useArtifact';
import { useWorkspaceFolders } from '../../contexts/WorkspaceContext';
import PublishDialog from '../PublishDialog';
import type { HtmlArtifactView } from '../viewers/html/ArtifactFrames';
import { CommentTools } from '../viewers/html/comments/CommentTools';
import { LazyCommentLayer } from '../viewers/html/comments/lazyCommentLayer';
import type { CommentUi } from '../viewers/html/comments/useCommentUi';
import type { ViewerCommentsTarget } from '../viewers/comments/ViewerComments';
import { ArtifactCrumbs } from './ArtifactCrumbs';
import type { FileQuoteAnchor } from './types';
import type { FocusViewer } from './useFileFocus';
import { PinPageButton } from './PinPageButton';
import { useArtifactVersionView, usePanelVersionStores, versionKey, LIVE_VIEW, type VersionStore } from './useArtifactVersionView';

/** A view with no file to keep a version for. */
const NO_STORE: VersionStore = { state: LIVE_VIEW, dispatch: () => {} };

/** A viewer in the app that draws the comment layer, through `ViewerComments`. */
export type CommentViewer = 'markdown' | 'code' | 'pdf' | 'sheet';

const LAYERS: Partial<Record<FocusViewer, 'html' | CommentViewer>> = {
  html: 'html', markdown: 'markdown', code: 'code', pdf: 'pdf', excel: 'sheet', csv: 'sheet',
};

/**
 * The comment layer the viewer showing a file draws for its owner, or null
 * where it draws none: an image, a large tool result, an edited file. An
 * HTML file is known by its name, any other once its bytes are `read`.
 */
export function commentViewer(viewer: FocusViewer, read: boolean): 'html' | CommentViewer | null {
  const layer = LAYERS[viewer] ?? null;
  return layer === 'html' || read ? layer : null;
}

interface FileCommentsOptions {
  workspaceId: string;
  tabId: string;
  /** The file in front, if a file tab is. */
  file: string | null;
  /** The comment layer its viewer draws (`commentViewer`): a framed HTML file's, or one a viewer in the app answers. */
  commentViewer: 'html' | CommentViewer | null;
  /** The markup is on screen rather than the page. */
  source: boolean;
  /** The panel is the one in front. */
  active: boolean;
  /** When the panel last read the file's bytes. */
  readAt: number | null;
  /** The owner's comment controls; left out, the panel is not the owner's and the file shows alone. */
  ui: CommentUi | undefined;
  /** The thread beside the panel, where a dashboard set from here is edited. */
  threadId?: string | null;
  /** Lets the selection toolbar go once its Comment is taken. */
  dismissSelection: () => void;
}

export interface FileComments {
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
  /** The owner's comments on a file a viewer in the app draws, while one is up. */
  target: ViewerCommentsTarget | null;
  /** Its line count gives way to the version and comment controls, except under Source. */
  hideMeta: boolean;
  /** The selection toolbar's Comment on the words selected, where the viewer in front takes words. */
  selectionComment: (selection: FileQuoteAnchor) => (() => void) | undefined;
  /** Opens the one Publish dialog on a file, any kind; null where the panel is not the owner's. */
  openPublish: ((path: string) => void) | null;
  dialog: React.ReactNode;
}

/**
 * The owner's file panel around the file in front: its comment controls in
 * the crumb row and what its viewer's comment layer takes, an HTML file's
 * versions, and the Publish dialog every file's header and crumb opens. The
 * HTML file's record is made as soon as its tab is in front
 * (`useArtifactVersionView`).
 */
export function useFileComments({
  workspaceId, tabId, file, commentViewer, source, active, readAt, ui, threadId = null, dismissSelection,
}: FileCommentsOptions): FileComments {
  const { t } = useTranslation();
  const root = useWorkspaceFolders()?.rootDir;
  const path = ui && commentViewer === 'html' ? file : null;
  const target = useMemo(
    () => (ui && file && commentViewer && commentViewer !== 'html' ? { workspaceId, path: file, tabId, ui, active } : null),
    [ui, file, commentViewer, workspaceId, tabId, active],
  );
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
  const layered = !!(path || target);
  useEffect(() => {
    if (layered) void LazyCommentLayer.preload();
  }, [layered]);

  const openPublish = ui ? (target: string) => setPublishing({ path: target, open: true }) : null;
  return {
    path,
    doc: path && ui ? ui.docOf(tabId, artifactPathKey(path, root)) : null,
    crumbs: path && openPublish && (
      <ArtifactCrumbs view={versionView} onReview={() => openPublish(path)} readAt={readAt} />
    ),
    tools: target ? (
      <CommentTools
        tabId={tabId}
        workspaceId={workspaceId}
        path={target.path}
        ui={target.ui}
        variant="crumb"
        modeTitle={t('pageViewer.comments.modeTitleFile')}
      />
    ) : path && ui && (
      <>
        <CommentTools
          tabId={tabId}
          workspaceId={workspaceId}
          path={path}
          ui={ui}
          variant="crumb"
          // A version is read only, and the markup has no spots to pick.
          modeDisabled={versionView.version !== null || source}
        />
        <PinPageButton workspaceId={workspaceId} path={path} threadId={threadId} />
      </>
    ),
    view: path && ui ? { tabId, ui, active, versionView } : null,
    target,
    selectionComment: (selection) => (target && selection.kind === 'selection' && target.ui.takesWords(tabId, selection.range)
      ? () => {
          target.ui.commentOnWords(tabId, selection.range);
          dismissSelection();
        }
      : undefined),
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
