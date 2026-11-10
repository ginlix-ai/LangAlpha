import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { __testing, measureText, withAlpha } from '../canvasKit';

const { textWidthCache, resetColorResolver, namedColorCache } = __testing;

// Minimal CanvasRenderingContext2D stub: only the members measureText touches.
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
    vi.resetAllMocks();
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

describe('measureText cache', () => {
  const FONT = '600 11px sans-serif';

  beforeEach(() => {
    textWidthCache.clear();
  });

  it('measures a given text once and reuses the cached width', () => {
    const { ctx, measureText: measure } = makeCtx((t) => t.length * 10);
    const first = measureText(ctx, FONT, 'Earnings');
    const second = measureText(ctx, FONT, 'Earnings');
    expect(first).toBe(80);
    expect(second).toBe(first);
    expect(measure).toHaveBeenCalledTimes(1);
  });

  it('measures distinct texts and fonts independently', () => {
    const { ctx, measureText: measure } = makeCtx((t) => t.length * 10);
    measureText(ctx, FONT, 'A');
    measureText(ctx, FONT, 'BB');
    measureText(ctx, FONT, 'A'); // cached
    measureText(ctx, '500 10px sans-serif', 'A');
    expect(measure).toHaveBeenCalledTimes(3);
    expect(measure).toHaveBeenNthCalledWith(1, 'A');
    expect(measure).toHaveBeenNthCalledWith(2, 'BB');
  });
});
