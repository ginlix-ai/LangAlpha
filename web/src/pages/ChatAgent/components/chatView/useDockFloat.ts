import { useMemo, useState } from 'react';

/** Where the pill was dragged to, from its place at the foot's centre. */
export interface PillOffset {
  x: number;
  y: number;
}

const PILL_HOME: PillOffset = { x: 0, y: 0 };

/** What the thread is doing, for the pill to say while the composer is folded. */
export type DockStatus = 'idle' | 'running' | 'waiting';

/** The floating composer's own state, kept above the file tabs so a tab
 *  switch keeps it folded, and its pill where it was put. */
export interface DockFloatState {
  /** Folded into a pill, so what the composer covered can be read. */
  folded: boolean;
  setFolded: (folded: boolean) => void;
  pillOffset: PillOffset;
  setPillOffset: (offset: PillOffset) => void;
  status: DockStatus;
  /** Comments queued to send, for the pill to count. */
  queued: number;
}

/**
 * On a wide screen the composer floats over the file rather than taking room
 * under it, so it can be folded away. Null on a phone, where it sits below
 * the file. It comes back open each time the chat goes out of view, and
 * opens when something comes to it for the person: a comment queued from the
 * file, or the agent stopping to ask them.
 */
export function useDockFloat({ floating, open, running, waiting, queued }: {
  floating: boolean;
  /** The chat is out of view and the composer is at the file. */
  open: boolean;
  /** A turn is running. */
  running: boolean;
  /** The agent is waiting on the person's answer. */
  waiting: boolean;
  queued: number;
}): DockFloatState | null {
  const [folded, setFolded] = useState(false);
  const [pillOffset, setPillOffset] = useState<PillOffset>(PILL_HOME);
  const [seen, setSeen] = useState({ queued, waiting });
  if (seen.queued !== queued || seen.waiting !== waiting) {
    setSeen({ queued, waiting });
    if (folded && (queued > seen.queued || (waiting && !seen.waiting))) setFolded(false);
  }
  if (folded && !(floating && open)) setFolded(false);
  const status: DockStatus = waiting ? 'waiting' : running ? 'running' : 'idle';
  return useMemo(
    () => (floating ? { folded, setFolded, pillOffset, setPillOffset, status, queued } : null),
    [floating, folded, pillOffset, status, queued],
  );
}
