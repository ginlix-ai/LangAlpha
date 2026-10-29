import React, { useEffect, useEffectEvent, useLayoutEffect, useRef, useState } from 'react';
import { HeldChildren } from '@/components/ui/Disclosure';
import { animate, usePresence } from '@/lib/framer';
import { followHeight } from '@/lib/followHeight';
import { useMotionSafe } from '@/lib/reducedMotion';
import { TOGETHER } from '../decisionCardMotion';

/**
 * How an answered hand-off moves what is around it when it changes more than
 * its own box: a card whose hand-off joins the stack beside it leaves its
 * place, and the stack takes it in as a row. Both are measured px tweens on
 * `TOGETHER`, started in the commit the answer lands in, so the card shutting
 * and the row opening add up to one glide of everything below. The card that
 * resolves in place is `ResolveSwap`'s.
 */

function marginTop(el: Element | null): number {
  if (!(el instanceof HTMLElement)) return 0;
  const gap = parseFloat(getComputedStyle(el).marginTop);
  return Number.isFinite(gap) ? gap : 0;
}

/**
 * A piece of a list spaced by top margins (`sibling-space-y`) that folds shut
 * when its AnimatePresence lets it go, and is removed once it has. One gap
 * goes with it: the one under it, cancelled by a negative bottom margin as it
 * shuts, or its own above it when nothing follows. Either way the last frame
 * of the fold and the first without it are the same height. Shown as it was,
 * and inert, while it goes: what is leaving has been answered.
 */
export function LeaveFold({ children }: { children: React.ReactNode }): React.ReactElement {
  const [present, safeToRemove] = usePresence();
  const motionSafe = useMotionSafe();
  const readMotionSafe = useEffectEvent(() => motionSafe);
  // A new function each time the list renders; the fold must not restart.
  const remove = useEffectEvent(() => safeToRemove?.());
  const boxRef = useRef<HTMLDivElement>(null);

  useLayoutEffect(() => {
    const box = boxRef.current;
    if (!box) return;
    if (present) {
      // Taken back before it was gone: it is a piece of the list again.
      box.style.height = box.style.overflow = box.style.opacity = '';
      box.style.marginTop = box.style.marginBottom = '';
      return;
    }
    if (!readMotionSafe()) {
      remove();
      return;
    }
    // Only reads until the tween's first frame, which every other box the
    // answer moves shares.
    const full = box.getBoundingClientRect().height;
    const below = marginTop(box.nextElementSibling);
    const above = below ? 0 : marginTop(box);
    box.style.overflow = 'hidden';
    const controls = animate(0, 1, {
      ...TOGETHER(full, 0),
      onUpdate: (shut) => {
        box.style.height = `${full * (1 - shut)}px`;
        // Dims with what is left of it, as every fold's close does.
        box.style.opacity = String(1 - shut);
        if (below) box.style.marginBottom = `${-below * shut}px`;
        else if (above) box.style.marginTop = `${above * (1 - shut)}px`;
      },
      onComplete: () => remove(),
    });
    return () => controls.stop();
  }, [present]);

  return (
    <div ref={boxRef} inert={!present || undefined}>
      <HeldChildren held={!present}>{children}</HeldChildren>
    </div>
  );
}

/**
 * A row of a list already on screen. One that `arrives` (it joined after the
 * list was drawn) unfolds from nothing to what it measures on `TOGETHER`, in
 * step with the card it takes the place of; one the list was drawn with is
 * there at once, so a reload draws the list as it stands. An arriving row
 * tells `onDrawn` once it is on screen, so a list that unmounts its rows while
 * folded draws it there when it opens again.
 */
export function ArrivingRow({ arrives, onDrawn, children }: {
  arrives: boolean;
  onDrawn?: () => void;
  children: React.ReactNode;
}): React.ReactElement {
  const [unfolds] = useState(arrives);
  const motionSafe = useMotionSafe();
  const readMotionSafe = useEffectEvent(() => motionSafe);
  const drawn = useEffectEvent(() => onDrawn?.());
  useEffect(() => {
    if (unfolds) drawn();
  }, [unfolds]);
  const boxRef = useRef<HTMLLIElement>(null);
  const innerRef = useRef<HTMLDivElement>(null);

  useLayoutEffect(() => {
    const box = boxRef.current;
    const inner = innerRef.current;
    if (!unfolds || !box || !inner || !readMotionSafe()) return;
    // Shut before the first frame paints it whole.
    box.style.height = '0px';
    box.style.overflow = 'hidden';
    const land = () => {
      box.style.height = '';
      box.style.overflow = '';
    };
    const stop = followHeight(box, inner, { from: 0, leg: TOGETHER, onLand: land });
    return () => {
      stop();
      land();
    };
  }, [unfolds]);

  return (
    <li ref={boxRef}>
      {/* flow-root: the measured box holds its children's margins. */}
      <div ref={innerRef} className="flow-root">{children}</div>
    </li>
  );
}
