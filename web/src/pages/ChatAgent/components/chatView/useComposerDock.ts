import { createContext, useMemo, useState, useSyncExternalStore, type RefCallback } from 'react';

/**
 * What a file tab needs to take the chat's composer while the chat is out of
 * view: the file panel lies over it at full width, or covers it on a phone.
 */
export interface ComposerDock {
  /** The chat's thread. Only a file whose comments come to it takes the composer. */
  threadId: string;
  /** Files the chat already knows send their comments here (those its queued
   *  comments sit on), by {@link dockFileKey}, so their foot can make room
   *  before the file's record has loaded. */
  knownFiles: ReadonlySet<string>;
  /** The chat is going out of view, so the file in front makes room at its
   *  foot for the composer. */
  open: boolean;
  /** The panel has finished covering the chat, so the composer moves in. Not
   *  before: carried along while the panel grows, it would reflow at every
   *  width the panel passes through. */
  taken: boolean;
  /** A ref for the element that holds the composer while it is away from the chat. */
  holdAt: RefCallback<HTMLElement>;
  /** The height, in px, the composer's stack is expected to take at the
   *  file's foot, for the room kept for it until it arrives. */
  expectedHeight: () => number;
}

export const ComposerDockContext = createContext<ComposerDock | null>(null);

/** How {@link ComposerDock.knownFiles} names a file: its workspace and workspace-relative path. */
export function dockFileKey(workspaceId: string, path: string): string {
  return `${workspaceId}\u0000${path.replace(/^\/+/, '')}`;
}

interface Places {
  /** Where the composer renders, wherever that element is at the moment. */
  host: HTMLElement;
  /** A ref for the composer's place in the chat. */
  home: RefCallback<HTMLElement>;
  holdAt: RefCallback<HTMLElement>;
  /** Whether the composer is at a file's foot, for what it draws differently there. */
  subscribe: (onChange: () => void) => () => void;
  isAway: () => boolean;
  expectedHeight: () => number;
}

function createPlaces(): Places {
  const host = document.createElement('div');
  // No box of its own: the composer lays out as a child of whichever place holds it.
  host.style.display = 'contents';
  let home: HTMLElement | null = null;
  let away: HTMLElement | null = null;
  // The height it last took away from the chat, the best guess at the next.
  let awayHeight = 0;
  const stackHeight = () => (host.firstElementChild as HTMLElement | null)?.offsetHeight ?? 0;
  const listeners = new Set<() => void>();

  const place = () => {
    const target = away ?? home;
    if (!target || host.parentElement === target) return;
    if (home && target === home) home.style.height = '';
    else if (home && host.parentElement === home && home.offsetHeight) {
      // The chat keeps the composer's height while it is away, so the
      // transcript's bottom padding and its pin to the latest stay as they
      // were and nothing moves when it comes back.
      home.style.height = `${home.offsetHeight}px`;
    }
    // Moving an element out of the document drops the focus inside it.
    const active = document.activeElement;
    const focused = active instanceof HTMLElement && host.contains(active) ? active : null;
    target.appendChild(host);
    focused?.focus({ preventScroll: true });
    for (const listener of listeners) listener();
  };

  return {
    host,
    home: (el) => {
      if (!el) return;
      home = el;
      place();
      return () => {
        if (home === el) home = null;
      };
    },
    holdAt: (el) => {
      if (!el) return;
      away = el;
      place();
      return () => {
        if (away !== el) return;
        if (host.parentElement === el) awayHeight = stackHeight() || awayHeight;
        away = null;
        place();
      };
    },
    subscribe: (onChange) => {
      listeners.add(onChange);
      return () => listeners.delete(onChange);
    },
    isAway: () => !!away && host.parentElement === away,
    // Before it has ever been away, the composer's own box in the chat: the
    // tray and the rows above it stay home, folded or empty, at the file's
    // foot.
    expectedHeight: () => awayHeight || (home?.querySelector<HTMLElement>('[data-handoff="composer"]')?.offsetHeight ?? 0),
  };
}

/**
 * One composer for the chat and the file. ChatInput holds the draft, its
 * attachments and the model picks in its own state, so a second one at the
 * file's foot would start empty, and two would have to be kept in step. The
 * composer renders once instead, through a portal into an element made here
 * (`host`), and that element moves between its place in the chat (`home`)
 * and the file's foot. React never sees the move: the portal keeps its place
 * in the tree, so nothing in it remounts, and a send from either place is
 * the same send.
 *
 * `open` is whether the chat is going out of view, and `taken` whether it has
 * gone. The file decides whether it takes the composer, since only its record
 * knows where its comments go. `docked` is whether the composer is at a
 * file's foot now.
 */
export function useComposerDock({ threadId, open, taken, knownFiles }: {
  threadId: string | undefined;
  open: boolean;
  taken: boolean;
  knownFiles: ReadonlySet<string>;
}): {
  context: ComposerDock | null;
  host: HTMLElement;
  home: RefCallback<HTMLElement>;
  docked: boolean;
} {
  const [places] = useState(createPlaces);
  const { host, home, holdAt, expectedHeight } = places;
  const docked = useSyncExternalStore(places.subscribe, places.isAway, () => false);
  const context = useMemo<ComposerDock | null>(
    () => (threadId ? { threadId, knownFiles, open, taken: open && taken, holdAt, expectedHeight } : null),
    [threadId, knownFiles, open, taken, holdAt, expectedHeight],
  );
  return { context, host, home, docked };
}
