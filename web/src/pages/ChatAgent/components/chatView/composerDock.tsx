import React, { useCallback, useContext, useLayoutEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { artifactPathKey } from '@/hooks/useArtifact';
import { followHeight } from '@/lib/followHeight';
import { SPRING_FOLD } from '@/lib/motion';
import { useMotionSafe } from '@/lib/reducedMotion';
import { useWorkspaceFolders } from '../../contexts/WorkspaceContext';
import { defaultsTo } from '../artifactComments/destinations';
import { useCommentDestinations } from '../artifactComments/useCommentDestinations';
import type { CommentUi } from '../viewers/html/comments/useCommentUi';
import { ComposerDockContext, dockFileKey, type ComposerDock } from './useComposerDock';
import './composerDock.css';

/**
 * The composer's place in the chat, and the composer, rendered into the
 * element that moves (useComposerDock). The place comes first in the tree,
 * so it holds that element before anything in the composer lays out.
 */
export function DockableComposer({ home, host, children }: {
  home: React.RefCallback<HTMLElement>;
  host: HTMLElement;
  children: React.ReactNode;
}): React.ReactElement {
  return (
    <>
      <div ref={home} className="flex w-full min-w-0 justify-center" />
      {createPortal(children, host)}
    </>
  );
}

const FOLD_LEG = () => SPRING_FOLD;

/**
 * The foot itself. It takes its room as the panel starts to cover the chat,
 * sized for the composer it expects, so the file above settles into its
 * height while the panel is still moving rather than once it has stopped.
 * The composer arrives when the panel lands (`taken`), rising in; if it is
 * not quite the height expected, the foot eases to the one it is.
 */
function DockFoot({ dock }: { dock: ComposerDock }): React.ReactElement {
  const boxRef = useRef<HTMLDivElement>(null);
  const placeRef = useRef<HTMLDivElement>(null);
  const motionSafe = useMotionSafe();
  // Read once, as the room opens: what the composer will bring.
  const [expected] = useState(() => dock.expectedHeight());
  const { taken, holdAt } = dock;

  const fromRef = useRef(0);
  useLayoutEffect(() => {
    const box = boxRef.current;
    const place = placeRef.current;
    if (!taken || !box || !place) return;
    const from = fromRef.current;
    const to = place.getBoundingClientRect().height;
    if (!motionSafe || !from || Math.abs(to - from) < 1) return;
    box.style.height = `${from}px`;
    box.style.overflow = 'hidden';
    const release = () => {
      box.style.height = '';
      box.style.overflow = '';
    };
    const stop = followHeight(box, place, { from, leg: FOLD_LEG, onLand: release });
    return () => {
      stop();
      release();
    };
  }, [taken, motionSafe]);
  // The height drawn before the composer came, which an arrival eases from.
  useLayoutEffect(() => {
    if (!taken) fromRef.current = placeRef.current?.getBoundingClientRect().height ?? 0;
  });
  const attachPlace = useCallback((el: HTMLDivElement | null) => {
    placeRef.current = el;
    const release = el && taken ? holdAt(el) : undefined;
    return () => {
      placeRef.current = null;
      if (typeof release === 'function') release();
    };
  }, [taken, holdAt]);

  return (
    <div ref={boxRef} className="composer-dock">
      <div ref={attachPlace} className="composer-dock-place" data-taken={taken || undefined}>
        {!taken && <div aria-hidden="true" style={{ height: expected }} />}
      </div>
    </div>
  );
}

/**
 * A file tab's foot. While the chat is out of view it holds the chat's
 * composer, if the comments on the file the frame is on come to the chat's
 * thread by default: what the composer sends goes to that thread, and a file
 * whose comments go elsewhere (bound to another thread, or sent to their
 * maker's or a new one by the owner's setting) is not its business. Below
 * the file rather than over it, so nothing in the file hides behind the
 * composer, and the comment layer, which keeps its popover and card in the
 * file's box, keeps them above it.
 */
export function ComposerDockSlot({ workspaceId, path, tabId, ui, active }: {
  workspaceId: string;
  /** The file the tab opened, as the tab holds it. */
  path: string;
  tabId: string;
  /** Which file the tab's frame is on now. */
  ui: CommentUi;
  active: boolean;
}): React.ReactElement | null {
  const dock = useContext(ComposerDockContext);
  const rootDir = useWorkspaceFolders()?.rootDir;
  const docPath = ui.docOf(tabId, artifactPathKey(path, rootDir));
  const { fallback } = useCommentDestinations(workspaceId, docPath);
  if (!dock) return null;
  // Until the server says where the file's comments go, a file the chat has
  // comments queued from is taken on trust; its word replaces that.
  const ours = fallback
    ? defaultsTo(fallback, dock.threadId)
    : dock.knownFiles.has(dockFileKey(workspaceId, docPath));
  if (!dock.open || !active || !ours) return null;
  return <DockFoot dock={dock} />;
}
