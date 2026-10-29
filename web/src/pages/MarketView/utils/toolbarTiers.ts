/**
 * Which collapse tier the chart toolbar renders at, from the width it has for
 * its own items: the container minus a host's lead and trail slots. The
 * breakpoints are the widths (px, descending) below which the toolbar sheds
 * the next set of items; the tier is how many of them the width is under.
 */
export const TOOLBAR_WIDTH_BREAKPOINTS = [1180, 880, 710, 560, 300] as const;

export type ToolbarTier = 0 | 1 | 2 | 3 | 4 | 5;

/**
 * Room a width must have past a breakpoint before the toolbar steps back up
 * to the roomier tier. A quote in the lead slot changes width by a digit on a
 * tick, so a width sitting on a breakpoint would otherwise flip tiers with
 * the price.
 */
export const TOOLBAR_TIER_HYSTERESIS_PX = 24;

export function toolbarTierFor(width: number): ToolbarTier {
  return TOOLBAR_WIDTH_BREAKPOINTS.filter((min) => width < min).length as ToolbarTier;
}

/** Steps down at a breakpoint, back up only once past it by the hysteresis. */
export function selectToolbarTier(width: number, current: ToolbarTier): ToolbarTier {
  const tighter = toolbarTierFor(width);
  if (tighter >= current) return tighter;
  return Math.min(current, toolbarTierFor(width - TOOLBAR_TIER_HYSTERESIS_PX)) as ToolbarTier;
}
