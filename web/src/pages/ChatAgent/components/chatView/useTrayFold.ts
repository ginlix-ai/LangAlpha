/**
 * The composer tray folds into the composer while the reader is up in the
 * history and comes back out at the end. Folding and unfolding are phases of
 * their own because both ends are drawn during them: the chips' shapes run
 * between the tray and the dock in the composer's toolbar.
 */
import { useCallback, useEffect, useEffectEvent, useRef, useState, type RefObject } from 'react';
import { useReducedMotion } from '@/lib/framer';
import { trayChipCount, type TrayContents } from './useTrayContents';

export type TrayFoldPhase = 'open' | 'folding' | 'folded' | 'unfolding';

/** Seconds each shape of a liquid run takes, and between one shape and the next. */
export const SHAPE_RUN = 0.5;
export const SHAPE_STAGGER = 0.045;

/** One liquid run of `shapes` shapes, the last one's stagger included. */
export function liquidRunMs(shapes: number): number {
  return (SHAPE_RUN + SHAPE_STAGGER * Math.max(0, shapes - 1)) * 1000;
}

/** How far the reader scrolls before a tray poured out of its dock folds again. */
const PEEK_SCROLL_PX = 48;

interface TrayFoldInputs {
  /** The reader is up in the history, away from the end. */
  scrolledUp: boolean;
  /** The jump to the end is still gliding there. */
  jumping: boolean;
  contents: TrayContents | null;
  /** The transcript's scroller, which a peek watches. */
  getScroller: () => HTMLElement | null;
}

export interface TrayFold {
  phase: TrayFoldPhase;
  /** The tray's row above the composer. */
  trayRef: RefObject<HTMLDivElement | null>;
  /** The dock in the composer's toolbar that the chips fold into. */
  dockRef: RefObject<HTMLButtonElement | null>;
  /** Pours the folded tray out where the reader is, until they scroll on. */
  peek: () => void;
}

/** What a fold takes out of the transcript's bottom padding: the tray and the gap under it. */
function foldDrop(tray: HTMLElement | null): number {
  if (!tray) return 0;
  const next = tray.nextElementSibling;
  return next instanceof HTMLElement ? next.offsetTop - tray.offsetTop : tray.offsetHeight;
}

export function useTrayFold({ scrolledUp, jumping, contents, getScroller }: TrayFoldInputs): TrayFold {
  // Framer reads the setting once, at mount: a change applies from the next
  // thread opened.
  const reducedMotion = useReducedMotion();
  const up = scrolledUp || jumping;
  const [peeking, setPeeking] = useState(false);
  if (peeking && !up) setPeeking(false);
  // A fold takes the tray out of the transcript's bottom padding, and a reader
  // nearer the end than that is clamped to it. The clamp reads as a scroll back
  // to the end, which pours the tray straight back out, so a tray in the layout
  // folds only once the reader is clear of it.
  const [clearOfTray, setClearOfTray] = useState(false);
  if (clearOfTray && !up) setClearOfTray(false);
  const [phase, setPhase] = useState<TrayFoldPhase>(up ? 'folded' : 'open');
  const inLayout = phase === 'open' || phase === 'unfolding';
  const folded = up && !peeking && (clearOfTray || !inLayout);
  // Instant under reduced motion, and while the tray is empty: a liquid run
  // started with no chips has no shapes to draw for its whole length.
  const animate = !reducedMotion && !!contents;

  // A run in flight finishes before the next one starts: its shapes are on
  // their way between two places and can only arrive at one of them.
  if (folded && phase === 'open') setPhase(animate ? 'folding' : 'folded');
  if (!folded && phase === 'folded') setPhase(animate ? 'unfolding' : 'open');
  // Timed by the chips the run starts with, which are the shapes the tray
  // measures for it: a chip that comes or goes mid-run is not in it.
  const runMs = useEffectEvent(() => liquidRunMs(trayChipCount(contents)));
  useEffect(() => {
    if (phase !== 'folding' && phase !== 'unfolding') return;
    const timer = setTimeout(() => setPhase(phase === 'folding' ? 'folded' : 'open'), runMs());
    return () => clearTimeout(timer);
  }, [phase]);

  const trayRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!up) return;
    const scroller = getScroller();
    if (!scroller) return;
    const measure = () => {
      const fromEnd = scroller.scrollHeight - scroller.clientHeight - scroller.scrollTop;
      setClearOfTray(fromEnd > foldDrop(trayRef.current));
    };
    measure();
    scroller.addEventListener('scroll', measure, { passive: true });
    return () => scroller.removeEventListener('scroll', measure);
  }, [up, getScroller]);

  // The jump's glide is not the reader scrolling on: a tray poured out stays
  // out through it and is there at the end.
  useEffect(() => {
    if (!peeking || jumping) return;
    const scroller = getScroller();
    if (!scroller) return;
    const from = scroller.scrollTop;
    const onScroll = () => {
      if (Math.abs(scroller.scrollTop - from) > PEEK_SCROLL_PX) setPeeking(false);
    };
    scroller.addEventListener('scroll', onScroll, { passive: true });
    return () => scroller.removeEventListener('scroll', onScroll);
  }, [peeking, jumping, getScroller]);

  const dockRef = useRef<HTMLButtonElement>(null);
  // The dock goes inert as the tray pours out, which drops focus on it to the
  // page; focus moves on to the first chip once they are back.
  const refocusRef = useRef(false);
  const peek = useCallback(() => {
    refocusRef.current = document.activeElement === dockRef.current;
    setPeeking(true);
  }, []);
  useEffect(() => {
    if (phase !== 'open' || !refocusRef.current) return;
    refocusRef.current = false;
    trayRef.current?.querySelector<HTMLElement>('button')?.focus();
  }, [phase]);
  return { phase, trayRef, dockRef, peek };
}
