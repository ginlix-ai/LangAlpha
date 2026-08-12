import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import {
  __testing,
  shouldSkipOffscreenSpan,
  type VisibleTimeRange,
} from '../agentAnnotationsPrimitive';

const { chipWidth, withAlpha, textWidthCache, resetColorResolver, namedColorCache } = __testing;

// Minimal CanvasRenderingContext2D stub: only the members chipWidth touches.
function makeCtx(measure: (text: string) => number): {
  ctx: CanvasRenderingContext2D;
  measureText: ReturnType<typeof vi.fn>;
} {
  const measureText = vi.fn((text: string) => ({ width: measure(text) }));
  const ctx = { font: '', measureText } as unknown as CanvasRenderingContext2D;
  return { ctx, measureText };
}

describe('withAlpha', () => {
  beforeEach(() => {
    namedColorCache.clear();
    resetColorResolver();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('expands #rgb shorthand and applies alpha', () => {
    expect(withAlpha('#f00', 0.1)).toBe('rgba(255,0,0,0.1)');
  });

  it('parses #rrggbb and applies alpha', () => {
    expect(withAlpha('#22c55e', 0.7)).toBe('rgba(34,197,94,0.7)');
  });

  it('parses #rrggbbaa (drops embedded alpha, uses requested)', () => {
    expect(withAlpha('#22c55eff', 0.5)).toBe('rgba(34,197,94,0.5)');
  });

  it('rewrites rgb() with the requested alpha', () => {
    expect(withAlpha('rgb(10, 20, 30)', 0.4)).toBe('rgba(10,20,30,0.4)');
  });

  it('rewrites rgba() with the requested alpha (replacing existing)', () => {
    expect(withAlpha('rgba(10,20,30,0.9)', 0.2)).toBe('rgba(10,20,30,0.2)');
  });

  it('honors alpha for named CSS colors when a canvas resolver is available', () => {
    // jsdom has no canvas backend, so simulate a browser canvas that normalizes
    // a named color to #rrggbb the way real browsers do.
    const fakeCtx = { _fill: '' } as unknown as CanvasRenderingContext2D;
    Object.defineProperty(fakeCtx, 'fillStyle', {
      get() {
        return (this as { _fill: string })._fill;
      },
      set(v: string) {
        // Only "tomato" is recognized; anything else (the sentinel) sticks.
        if (v === 'tomato') (this as { _fill: string })._fill = '#ff6347';
        else (this as { _fill: string })._fill = v;
      },
    });
    vi.spyOn(document, 'createElement').mockReturnValue({
      getContext: () => fakeCtx,
    } as unknown as HTMLCanvasElement);

    expect(withAlpha('tomato', 0.1)).toBe('rgba(255,99,71,0.1)');
  });

  it('falls back to the original color for unknown strings with no resolution', () => {
    // Resolver present but rejects the value: fillStyle stays at the sentinel.
    const fakeCtx = { _fill: '' } as unknown as CanvasRenderingContext2D;
    Object.defineProperty(fakeCtx, 'fillStyle', {
      get() {
        return (this as { _fill: string })._fill;
      },
      set(v: string) {
        (this as { _fill: string })._fill = v; // never normalizes → sentinel sticks
      },
    });
    vi.spyOn(document, 'createElement').mockReturnValue({
      getContext: () => fakeCtx,
    } as unknown as HTMLCanvasElement);

    expect(withAlpha('definitely-not-a-color', 0.3)).toBe('definitely-not-a-color');
  });

  it('falls back unchanged for named colors when no canvas is available (jsdom)', () => {
    // Real jsdom env: getContext('2d') returns null → guarded fallback, no throw.
    expect(withAlpha('red', 0.1)).toBe('red');
  });
});

describe('chipWidth measureText cache', () => {
  beforeEach(() => {
    textWidthCache.clear();
  });

  it('measures a given text once and reuses the cached width', () => {
    const { ctx, measureText } = makeCtx((t) => t.length * 10);
    const first = chipWidth(ctx, 'Earnings');
    const second = chipWidth(ctx, 'Earnings');
    expect(first).toBe(second);
    expect(measureText).toHaveBeenCalledTimes(1);
  });

  it('measures distinct texts independently', () => {
    const { ctx, measureText } = makeCtx((t) => t.length * 10);
    chipWidth(ctx, 'A');
    chipWidth(ctx, 'BB');
    chipWidth(ctx, 'A'); // cached
    expect(measureText).toHaveBeenCalledTimes(2);
    expect(measureText).toHaveBeenNthCalledWith(1, 'A');
    expect(measureText).toHaveBeenNthCalledWith(2, 'BB');
  });

  it('includes padding/dot geometry in the returned width', () => {
    const { ctx } = makeCtx(() => 100);
    // CHIP_PAD_X*2 (14) + DOT_R*2 (6) + DOT_GAP (6) + measured (100) = 126
    expect(chipWidth(ctx, 'x')).toBe(126);
  });
});

describe('shouldSkipOffscreenSpan', () => {
  const range: VisibleTimeRange = { from: 1000, to: 2000 };

  it('does not skip when at least one anchor is on-screen', () => {
    // coordA non-null = on-screen, even if the other is off-screen.
    expect(shouldSkipOffscreenSpan(range, 50, 1500, null, 3000)).toBe(false);
    expect(shouldSkipOffscreenSpan(range, null, 500, 80, 1500)).toBe(false);
  });

  it('skips when both anchors are off-screen on the left (phantom)', () => {
    expect(shouldSkipOffscreenSpan(range, null, 200, null, 500)).toBe(true);
  });

  it('skips when both anchors are off-screen on the right (phantom)', () => {
    expect(shouldSkipOffscreenSpan(range, null, 2500, null, 3000)).toBe(true);
  });

  it('does NOT skip an opposite-side straddle (shape genuinely spans the pane)', () => {
    expect(shouldSkipOffscreenSpan(range, null, 200, null, 3000)).toBe(false);
  });

  it('does not skip when both anchors are inside the range but un-coordinated', () => {
    // Defensive: both null yet times inside the viewport → classify as inside,
    // not a same-side phantom, so we draw rather than drop a valid shape.
    expect(shouldSkipOffscreenSpan(range, null, 1200, null, 1800)).toBe(false);
  });
});
