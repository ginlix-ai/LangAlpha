// @vitest-environment node
import { describe, it, expect } from 'vitest';
import {
  TOOLBAR_TIER_HYSTERESIS_PX,
  TOOLBAR_WIDTH_BREAKPOINTS,
  selectToolbarTier,
  toolbarTierFor,
} from '../toolbarTiers';

describe('toolbarTierFor', () => {
  it('counts the breakpoints the width is under', () => {
    expect(toolbarTierFor(1400)).toBe(0);
    expect(toolbarTierFor(1180)).toBe(0);
    expect(toolbarTierFor(1179)).toBe(1);
    expect(toolbarTierFor(700)).toBe(3);
    expect(toolbarTierFor(299)).toBe(5);
    expect(toolbarTierFor(-100)).toBe(TOOLBAR_WIDTH_BREAKPOINTS.length);
  });
});

describe('selectToolbarTier', () => {
  const bp = TOOLBAR_WIDTH_BREAKPOINTS[2]; // 710: tier 3 below it

  it('steps down to a tighter tier right at the breakpoint', () => {
    expect(selectToolbarTier(bp, 2)).toBe(2);
    expect(selectToolbarTier(bp - 1, 2)).toBe(3);
  });

  it('holds the tighter tier until there is the hysteresis of room past the breakpoint', () => {
    expect(selectToolbarTier(bp, 3)).toBe(3);
    expect(selectToolbarTier(bp + TOOLBAR_TIER_HYSTERESIS_PX - 1, 3)).toBe(3);
    expect(selectToolbarTier(bp + TOOLBAR_TIER_HYSTERESIS_PX, 3)).toBe(2);
  });

  // A price tick that adds a digit to the lead moves the width by ~7px; a
  // width straddling the breakpoint must settle, not alternate.
  it('does not flip on a width that oscillates across the breakpoint', () => {
    let tier = selectToolbarTier(bp + 3, 2);
    const seen = new Set<number>();
    for (let i = 0; i < 20; i++) {
      tier = selectToolbarTier(i % 2 ? bp + 3 : bp - 4, tier);
      seen.add(tier);
    }
    expect(seen).toEqual(new Set([3]));
  });

  it('steps up several tiers at once when the room is clearly there', () => {
    expect(selectToolbarTier(1400, 4)).toBe(0);
  });

  it('steps up only as far as the hysteresis allows when a wider breakpoint is close', () => {
    // 900 is over 880 but not by the hysteresis: stay under it (tier 2), not tier 1.
    expect(selectToolbarTier(900, 4)).toBe(2);
  });

  it('steps down several tiers at once', () => {
    expect(selectToolbarTier(200, 0)).toBe(5);
  });
});
