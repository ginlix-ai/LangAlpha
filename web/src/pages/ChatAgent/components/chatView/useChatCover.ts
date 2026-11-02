import { useCallback, useEffect, useLayoutEffect, useMemo, useState, type RefObject } from 'react';
import { useMotionSafe } from '@/lib/reducedMotion';
import { dockFileKey, useComposerDock } from './useComposerDock';
import { usePanelExpand } from './usePanelExpand';

/**
 * The cover as of the last commit, for what runs outside render and is wired
 * before the panel's state exists (the chat's sends): `useChatCover` fills it.
 */
export interface ChatCoverReads {
  /** The chat is out of view under the file panel. */
  covered: () => boolean;
  /** The composer sits at a file's foot. */
  docked: () => boolean;
}

interface FilledReads extends ChatCoverReads {
  fill: (covered: boolean, docked: boolean) => void;
}

export function useChatCoverReads(): FilledReads {
  const [reads] = useState<FilledReads>(() => {
    let now = { covered: false, docked: false };
    return {
      covered: () => now.covered,
      docked: () => now.docked,
      fill: (covered, docked) => { now = { covered, docked }; },
    };
  });
  return reads;
}

/**
 * Whether the chat is covered, and the composer's move that follows from it.
 * The chat is out of view under a file panel at full width, and on a phone
 * under any file panel; a file whose comments come to this thread then holds
 * the composer (useComposerDock). It moves there once the panel has finished
 * covering the chat: the edges of one expanding over it (usePanelExpand), or
 * a phone's sheet sliding in, which reports its landing (`onSheetLanded`).
 */
export function useChatCover({ isMobile, filePanel, panelExpanded, panelWidth, threadId, mainAgent, queuedComments, reads }: {
  isMobile: boolean;
  /** The right panel shows files. */
  filePanel: boolean;
  panelExpanded: boolean;
  /** The split panel's width, which an expanded one shrinks back to. */
  panelWidth: number;
  /** The chat's thread, which a file's comments must come to for it to take the composer. */
  threadId: string | undefined;
  /** The main agent's transcript is shown; a subagent's keeps its own box. */
  mainAgent: boolean;
  /** The comments queued for this thread, whose files take the composer
   *  before their records load. */
  queuedComments: readonly { workspace_id: string; path: string }[];
  reads: FilledReads;
}): {
  /** The chat is going out of view, so the file in front makes room for the composer. */
  dockOpen: boolean;
  dock: ReturnType<typeof useComposerDock>;
  /** The panel's inner box, whose edges an expand moves. */
  panelInnerRef: RefObject<HTMLDivElement | null>;
  onSheetLanded: () => void;
} {
  const { innerRef: panelInnerRef, moving: panelMoving } = usePanelExpand(panelExpanded, panelWidth);
  const motionSafe = useMotionSafe();
  const [sheetLanded, setSheetLanded] = useState(false);
  if (sheetLanded && !filePanel) setSheetLanded(false);
  const onSheetLanded = useCallback(() => setSheetLanded(true), []);

  const covered = filePanel && (isMobile || panelExpanded);
  const dockOpen = covered && mainAgent;
  const queuedKey = queuedComments.map((c) => dockFileKey(c.workspace_id, c.path)).join('\n');
  const knownFiles = useMemo(() => new Set(queuedKey ? queuedKey.split('\n') : []), [queuedKey]);
  const dock = useComposerDock({
    threadId,
    open: dockOpen,
    taken: isMobile ? sheetLanded || !motionSafe : !panelMoving,
    knownFiles,
  });
  useLayoutEffect(() => reads.fill(covered, dock.docked));

  // With comments queued, a commented file is the likely next thing opened,
  // and the HTML viewer's code is what the composer waits on to dock under it.
  const commentsQueued = queuedComments.length > 0;
  useEffect(() => {
    if (!commentsQueued) return;
    const timer = setTimeout(() => { void import('../viewers/HtmlViewer'); }, 1500);
    return () => clearTimeout(timer);
  }, [commentsQueued]);

  return { dockOpen, dock, panelInnerRef, onSheetLanded };
}
