import React, { useLayoutEffect, useRef, useState } from 'react';
import { ChevronRight } from 'lucide-react';
import { HeldChildren } from '@/components/ui/Disclosure';
import { motion, type ValueAnimationTransition } from '@/lib/framer';
import { followHeight } from '@/lib/followHeight';
import { DURATION, EASE_COLLAPSE, EASE_OUT, SPRING_FOLD, TWEEN } from '@/lib/motion';
import { useMotionSafe } from '@/lib/reducedMotion';

/**
 * The motion of the transcript's decision cards (approvals, questions,
 * confirmations): a card entering, a card giving way to its settled row, and
 * the chevrons of what is left, whose bodies fold in `Disclosure`. They sit
 * inside the chat scroller, so every height here is a measured px value and
 * nothing hands framer `auto` (see components/ui/Disclosure).
 */

const ENTER_FROM = { opacity: 0, y: 6 };
const SHOWN = { opacity: 1, y: 0 };
const GONE = { opacity: 0, y: 0 };
// The row comes in once the card is out, so the two never overlap on the line
// they share.
const ROW_IN = { ...TWEEN.quick, delay: DURATION.exit };

/** How a box tweens one leg of its height, from where it is to where it goes. */
export type HeightLeg = (from: number, to: number) => ValueAnimationTransition<number>;

/** A box moving on its own: shrinking, it eases shut as a tall box closing
 *  does; growing, it settles in. */
const ALONE: HeightLeg = (from, to) => ({ duration: DURATION.fold, ease: to < from ? EASE_COLLAPSE : EASE_OUT });

/**
 * The one leg for every box that one answer moves at once: a card leaving, a
 * row arriving, a swap. Boxes that each picked a curve for their own direction
 * add up to a layout that moves down and then back up; on one curve and one
 * clock, started in the same frame, every position between and below them is
 * the old layout eased into the new one, and travels once, one way.
 */
export const TOGETHER: HeightLeg = () => ({ duration: DURATION.fold, ease: EASE_COLLAPSE });

interface ResolveSwapProps {
  /** The question has its answer, and `settled` stands in for the card. */
  resolved: boolean;
  /** What is left once it is answered: a settled row, or a settled card. */
  settled: React.ReactNode;
  /** The card asking. */
  children: React.ReactNode;
  /** Whether a card first drawn unanswered enters. Off for a state that is
   *  not arriving and only ever gives way, such as a slip that may become a
   *  group. */
  enter?: boolean;
  /** How the box tweens from the card's height to the row's: on its own
   *  curve by default, `TOGETHER` when other boxes move with it. */
  leg?: HeightLeg;
}

/**
 * A decision card, and what replaces it once it is answered.
 *
 * Only a card first drawn with its question open enters, and only an answer
 * that lands while it is on screen animates: one first drawn answered is
 * history (a reload, a replay, a reconnect) and draws its row at once.
 *
 * Answered live, the box holds the card's height and tweens it to the row's
 * while the card fades out and the row fades in over it, so what is below
 * glides up instead of jumping. The card stays in flow for the whole swap and
 * the row is laid over it, so the first layout after the answer is exactly as
 * tall as the last one before it; at the end the card goes, the row takes its
 * place at the height the box already has, and the box is `auto` again.
 */
export function ResolveSwap({ resolved, settled, children, enter = true, leg = ALONE }: ResolveSwapProps): React.ReactElement {
  const motionSafe = useMotionSafe();
  const [enters] = useState(enter && !resolved);
  const [wasResolved, setWasResolved] = useState(resolved);
  const [leaving, setLeaving] = useState(false);
  if (resolved !== wasResolved) {
    setWasResolved(resolved);
    setLeaving(resolved && motionSafe);
  }
  const outerRef = useRef<HTMLDivElement>(null);
  const rowRef = useRef<HTMLDivElement>(null);

  useLayoutEffect(() => {
    const outer = outerRef.current;
    if (!outer) return;
    if (!leaving) {
      outer.style.height = '';
      return;
    }
    const row = rowRef.current;
    if (!row) return;
    // Pinned before the first write, from the layout the answer left alone.
    const height = outer.getBoundingClientRect().height;
    outer.style.height = `${height}px`;
    // A row that is still filling in (a run's status arriving) is followed
    // rather than clipped when the box lets go.
    return followHeight(outer, row, {
      from: height,
      leg,
      onLand: () => setLeaving(false),
    });
  }, [leaving, leg]);

  const showCard = !resolved || leaving;
  return (
    <div ref={outerRef} style={leaving ? { position: 'relative', overflow: 'hidden' } : undefined}>
      {showCard && (
        <motion.div
          initial={enters ? ENTER_FROM : false}
          animate={leaving ? GONE : SHOWN}
          transition={leaving ? TWEEN.exit : TWEEN.enter}
        >
          {/* A card on its way out has already been answered; a second
              click on it must not answer again. */}
          <div inert={leaving}>
            <HeldChildren held={resolved}>{children}</HeldChildren>
          </div>
        </motion.div>
      )}
      {resolved && (
        <motion.div
          ref={rowRef}
          initial={leaving ? { opacity: 0 } : false}
          animate={{ opacity: 1 }}
          transition={ROW_IN}
          style={leaving ? { position: 'absolute', top: 0, left: 0, right: 0 } : undefined}
        >
          {settled}
        </motion.div>
      )}
    </div>
  );
}

/** The one disclosure chevron: points right shut, down open, on the fold spring. */
export function FoldChevron({
  open,
  className = 'h-3.5 w-3.5',
}: {
  open: boolean;
  className?: string;
}): React.ReactElement {
  return (
    <motion.span
      aria-hidden
      className="inline-flex shrink-0"
      initial={false}
      animate={{ rotate: open ? 90 : 0 }}
      transition={SPRING_FOLD}
    >
      <ChevronRight className={className} style={{ color: 'var(--color-icon-muted)' }} />
    </motion.span>
  );
}
