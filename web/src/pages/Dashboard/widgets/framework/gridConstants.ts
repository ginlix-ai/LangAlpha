import type { BreakpointKey } from '../types';

/** Breakpoints in pixels — mirrors RGL's `breakpoints` prop. */
export const BREAKPOINTS_PX = { lg: 1024, md: 0 } as const;

/** Column count per breakpoint — both the RGL prop shape and the internal use. */
export const COLS_PER_BP = { lg: 12, md: 12 } as const;

/** Flat column count for reconcile logic that doesn't need breakpoint awareness. */
export const COLS = 12;

/** Breakpoint keys as a stable, iterable list. */
export const BREAKPOINT_KEYS: readonly BreakpointKey[] = ['lg', 'md'] as const;

/** RGL base row height in pixels. */
export const ROW_HEIGHT = 8;

/** Vertical margin between cells in pixels. */
export const MARGIN_Y = 16;

/** Horizontal margin between cells in pixels. */
export const MARGIN_X = 16;

/**
 * Breathing room added to measured content height before rounding up to the
 * next row count. Set to 0 so fit-to-content cells hug their content tightly;
 * the `Math.ceil` in `pxToRows` already guarantees no clipping, and anything
 * beyond that reads as an unexplained gap under the widget.
 */
export const FIT_PADDING_PX = 0;

/** Clamp a number to [min?, max?] — undefined bounds are ignored. */
export function clamp(value: number, min: number | undefined, max: number | undefined): number {
  let out = value;
  if (typeof min === 'number') out = Math.max(out, min);
  if (typeof max === 'number') out = Math.min(out, max);
  return out;
}
