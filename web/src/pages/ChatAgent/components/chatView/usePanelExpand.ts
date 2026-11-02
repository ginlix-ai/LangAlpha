import { useEffectEvent, useLayoutEffect, useRef, useState, type RefObject } from 'react';
import { animate } from '@/lib/framer';
import { SPRING_FOLD } from '@/lib/motion';
import { useMotionSafe } from '@/lib/reducedMotion';

type Insets = { top: number; right: number; bottom: number; left: number };
const SIDES = ['top', 'right', 'bottom', 'left'] as const;
const NONE: Insets = { top: 0, right: 0, bottom: 0, left: 0 };
const SETTLED_PX = 16;

/** Where the split panel's box sits inside the box the expanded one covers. */
function splitInsets(slot: HTMLElement, cover: HTMLElement): Insets {
  const s = slot.getBoundingClientRect();
  const c = cover.getBoundingClientRect();
  // Absolute insets count from the padding box, inside the borders.
  const left = c.left + cover.clientLeft;
  const top = c.top + cover.clientTop;
  return {
    top: s.top - top,
    left: s.left - left,
    right: left + cover.clientWidth - s.right,
    bottom: top + cover.clientHeight - s.bottom,
  };
}

/**
 * The file panel growing over the chat to full width, and shrinking back to
 * its place beside it. Expanded, the panel is an absolute box over the whole
 * view while its wrapper keeps the split width in the row (ChatView), so the
 * chat underneath never reflows; switched in one commit, the panel jumped
 * between the two boxes in a frame. Here its edges travel between them on the
 * fold spring instead: the inner box is absolute for the whole move, its
 * insets eased from the split slot to the cover or back, and handed to the
 * class the state names once they land.
 *
 * `moving` holds from the commit that flips `expanded` until the edges land,
 * read in the same render, so the composer that docks under an expanded page
 * waits for the panel to finish covering the chat. Instant under reduced
 * motion.
 */
export function usePanelExpand(expanded: boolean, splitWidth: number): {
  innerRef: RefObject<HTMLDivElement | null>;
  moving: boolean;
} {
  const innerRef = useRef<HTMLDivElement>(null);
  const motionSafe = useMotionSafe();
  // The state the panel's edges last landed at.
  const [landed, setLanded] = useState(expanded);
  const moving = motionSafe && landed !== expanded;
  if (!motionSafe && landed !== expanded) setLanded(expanded);

  const insetsRef = useRef<Insets | null>(null);
  const stopRef = useRef<(() => void) | null>(null);
  const readSplitWidth = useEffectEvent(() => splitWidth);
  const readMotionSafe = useEffectEvent(() => motionSafe);
  const shownRef = useRef(expanded);

  useLayoutEffect(() => {
    // Each flip moves the edges, from where the last move left them if it
    // was still under way; the first run is the mount, which just draws.
    if (shownRef.current === expanded) return;
    shownRef.current = expanded;
    if (!readMotionSafe()) return;
    const inner = innerRef.current;
    const slot = inner?.parentElement;
    const cover = inner?.offsetParent;
    if (!inner || !slot || !(cover instanceof HTMLElement)) {
      setLanded(expanded);
      return;
    }
    const split = splitInsets(slot, cover);
    // From wherever an interrupted move left the edges, else from the state
    // the commit just switched away from.
    const from = insetsRef.current ?? (expanded ? split : NONE);
    const to = expanded ? NONE : split;
    stopRef.current?.();

    const write = (insets: Insets) => {
      insetsRef.current = insets;
      for (const side of SIDES) inner.style[side] = `${insets[side]}px`;
    };
    // Collapsing, the class has already put the box back in the row: hold it
    // over the view, as it was, for the way down.
    inner.style.position = 'absolute';
    inner.style.zIndex = '10';
    inner.style.width = 'auto';
    inner.style.height = 'auto';
    write(from);

    const span = Math.max(1, Math.abs(to.left - from.left));
    let settling = false;
    const controls = animate(from.left, to.left, {
      ...SPRING_FOLD,
      onUpdate: (left) => {
        // The spring's last few px are a tail no one reads as motion: what
        // waits for the landing (the composer docking) starts there.
        if (!settling && Math.abs(to.left - left) < SETTLED_PX) {
          settling = true;
          setLanded(expanded);
        }
        // The left edge travels furthest and leads; the others keep pace.
        const p = Math.min(1, Math.abs(left - from.left) / span);
        const at = (side: (typeof SIDES)[number]) => from[side] + (to[side] - from[side]) * p;
        write({ top: at('top'), right: at('right'), bottom: at('bottom'), left: Math.max(0, left) });
      },
      onComplete: () => {
        stopRef.current = null;
        insetsRef.current = null;
        for (const side of SIDES) inner.style[side] = '';
        inner.style.position = '';
        inner.style.zIndex = '';
        inner.style.height = '';
        // Split again, the width is the row's: the style React set and this
        // move wrote over.
        inner.style.width = expanded ? '' : `${readSplitWidth()}px`;
        setLanded(expanded);
      },
    });
    stopRef.current = () => controls.stop();
  }, [expanded]);

  useLayoutEffect(() => () => stopRef.current?.(), []);

  return { innerRef, moving };
}
