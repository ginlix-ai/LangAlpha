import type { Transition } from '@/lib/framer';
import { SPRING_INDICATOR, TWEEN } from '@/lib/motion';

/** A pane's content arriving when what it shows changes: a short fade with a
 *  few pixels of rise, on the design system's entrance curve. */
export const PANE_ENTER = {
  initial: { opacity: 0, y: 4 },
  animate: { opacity: 1, y: 0 },
  transition: TWEEN.quick satisfies Transition,
};

/** One automation's details giving way to another's: both layers dissolve
 *  over the same curve, so their sum stays near full and nothing flashes
 *  blank, and neither moves. */
export const PANE_CROSSFADE = {
  initial: { opacity: 0 },
  animate: { opacity: 1 },
  exit: { opacity: 0 },
  transition: TWEEN.exit satisfies Transition,
};

/** The list's selection travelling to the next row on an arrow key.
 *  The house indicator spring, the one the tab underlines use, so it settles
 *  without a bounce. */
export const SELECTION_SPRING: Transition = SPRING_INDICATOR;

export const INSTANT: Transition = { duration: 0 };
