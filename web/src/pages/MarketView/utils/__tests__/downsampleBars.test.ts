import { describe, expect, it } from 'vitest';

import { downsampleBars } from '../downsampleBars';

/** Build `n` distinct bars (each element is its own object identity). */
function makeBars(n: number): Array<{ i: number }> {
  return Array.from({ length: n }, (_, i) => ({ i }));
}

describe('downsampleBars', () => {
  it('throws when max is not a positive integer', () => {
    expect(() => downsampleBars(makeBars(10), 0)).toThrow(RangeError);
    expect(() => downsampleBars(makeBars(10), -1)).toThrow(RangeError);
    expect(() => downsampleBars(makeBars(10), 1.5)).toThrow(RangeError);
  });

  it('passes through unchanged when n <= max (truncated false)', () => {
    const bars = makeBars(10);
    const out = downsampleBars(bars, 300);
    expect(out.truncated).toBe(false);
    expect(out.bars).toBe(bars); // same reference — no copy
    expect(out.bars).toHaveLength(10);
  });

  it('passes through at exactly n === max (truncated false)', () => {
    const bars = makeBars(300);
    const out = downsampleBars(bars, 300);
    expect(out.truncated).toBe(false);
    expect(out.bars).toHaveLength(300);
  });

  it('strides when n > max and keeps the last bar exactly once (no duplicate)', () => {
    const bars = makeBars(901); // stride = ceil(901/300) = 4
    const out = downsampleBars(bars, 300);
    expect(out.truncated).toBe(true);
    // strided indices: 0, 4, 8, ... 900 (900 % 4 === 0, so last is already included)
    const last = bars[bars.length - 1];
    expect(out.bars[out.bars.length - 1]).toBe(last);
    // last bar appears exactly once
    expect(out.bars.filter((b) => b === last)).toHaveLength(1);
    // every chosen bar is on the stride or is the forced last
    expect(out.bars.every((b) => b.i % 4 === 0)).toBe(true);
  });

  it('force-appends the last bar when the stride does not land on it', () => {
    const bars = makeBars(902); // stride = ceil(902/300) = 4; last index 901 % 4 === 1
    const out = downsampleBars(bars, 300);
    expect(out.truncated).toBe(true);
    const last = bars[bars.length - 1];
    expect(out.bars[out.bars.length - 1]).toBe(last);
    // appears exactly once (it wasn't on the stride)
    expect(out.bars.filter((b) => b === last)).toHaveLength(1);
  });

  it('does not duplicate the last bar on an exact multiple of the stride', () => {
    // n where (n-1) is divisible by the stride: stride lands on the final index.
    const bars = makeBars(601); // stride = ceil(601/300) = 3; last index 600 % 3 === 0
    const out = downsampleBars(bars, 300);
    expect(out.truncated).toBe(true);
    const last = bars[bars.length - 1];
    expect(out.bars[out.bars.length - 1]).toBe(last);
    expect(out.bars.filter((b) => b === last)).toHaveLength(1);
  });

  it('handles single-bar input (truncated false)', () => {
    const bars = makeBars(1);
    const out = downsampleBars(bars, 300);
    expect(out.truncated).toBe(false);
    expect(out.bars).toHaveLength(1);
    expect(out.bars[0]).toBe(bars[0]);
  });

  it('caps to roughly max bars (≤ max + 1 with the forced last bar)', () => {
    const out = downsampleBars(makeBars(5000), 300);
    expect(out.truncated).toBe(true);
    expect(out.bars.length).toBeLessThanOrEqual(301);
    expect(out.bars.length).toBeGreaterThan(0);
  });
});
