import { Suspense } from 'react';
import { createPortal } from 'react-dom';
import { LazyCommentLayer } from '../html/comments/lazyCommentLayer';
import type { CommentUi } from '../html/comments/useCommentUi';
import type { SpotStrategy } from './spotStrategy';
import type { ViewerPageOptions } from './viewerPage';
import { useViewerPage } from './useViewerPage';

/** The owner's comments on a file a viewer in the app draws: where the file is, and the controls its tab holds. */
export interface ViewerCommentsTarget {
  workspaceId: string;
  path: string;
  tabId: string;
  ui: CommentUi;
  /** The tab is in front of an active panel: keys and asks are its. */
  active: boolean;
}

/**
 * The comment layer over a viewer drawn in the app: the same layer a framed
 * HTML file has, answered from here (`ViewerPage`) rather than from a page's
 * bridge. Drawn beside the viewer's scroll viewport, in its parent, so it
 * covers the viewport's box without scrolling with the content, as the layer
 * over a frame does; the parent is the viewport's positioned box.
 */
export function ViewerComments({ scroller, content, strategy, target, modeHint, title }: {
  scroller: HTMLElement | null;
  content: HTMLElement | null;
  strategy: SpotStrategy | null;
  target: ViewerCommentsTarget;
  /** What a click picks in this viewer, said while the mode is on. */
  modeHint: string;
  title?: ViewerPageOptions['title'];
}) {
  const { bridge, view: pageView, pickWords, holdsWords } = useViewerPage({ scroller, content, strategy, path: target.path, title });
  const host = scroller?.parentElement;
  if (!host) return null;
  return createPortal(
    // Its own boundary: the layer's chunk loading must not hide the file.
    <Suspense fallback={null}>
      <LazyCommentLayer
        tabId={target.tabId}
        workspaceId={target.workspaceId}
        path={target.path}
        bridge={bridge}
        view={pageView}
        active={target.active}
        ui={target.ui}
        modeHint={modeHint}
        pickWords={pickWords}
        holdsWords={holdsWords}
      />
    </Suspense>,
    host,
  );
}
