import { renderHook } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { useStableBars, type BarLike } from '../useStableBars';

const bars = (n: number): BarLike[] =>
  Array.from({ length: n }, (_, k) => ({ time: 1000 + k * 60, high: 10 + k, low: 5 + k }));

describe('useStableBars', () => {
  it('keeps its identity through a price tick inside the forming bar’s range, which rewrites it in a new array', () => {
    const { result, rerender } = renderHook(({ data }) => useStableBars(data), { initialProps: { data: bars(5) } });
    const first = result.current;
    rerender({ data: bars(5).map((b) => ({ ...b })) });
    expect(result.current).toBe(first);
    expect(result.current.times).toBe(first.times);
  });

  it('is new once a bar is appended or prepended, with its times and extremes', () => {
    const { result, rerender } = renderHook(({ data }) => useStableBars(data), { initialProps: { data: bars(3) } });
    const first = result.current;
    rerender({ data: bars(4) });
    expect(result.current).not.toBe(first);
    expect(result.current.times).toEqual([1000, 1060, 1120, 1180]);
    expect([...result.current.highs]).toEqual([10, 11, 12, 13]);
    const appended = result.current;
    rerender({ data: [{ time: 940, high: 9, low: 4 }, ...bars(4)] });
    expect(result.current).not.toBe(appended);
    expect(result.current.times[0]).toBe(940);
  });

  it('reads no bars as empty, and a bar without a high as NaN', () => {
    const { result, rerender } = renderHook(({ data }) => useStableBars(data), {
      initialProps: { data: null as BarLike[] | null },
    });
    expect(result.current.times).toEqual([]);
    rerender({ data: [{ time: 1000 }] });
    expect(result.current.times).toEqual([1000]);
    expect(Number.isNaN(result.current.highs[0])).toBe(true);
  });

  it('has new extremes, but the same times, once the forming bar reaches a new high or low', () => {
    const { result, rerender } = renderHook(({ data }) => useStableBars(data), { initialProps: { data: bars(5) } });
    const first = result.current;
    const lastOf = (high: number, low: number): BarLike[] => [...bars(5).slice(0, 4), { time: 1240, high, low }];
    rerender({ data: lastOf(20, 9) });
    const higher = result.current;
    expect(higher).not.toBe(first);
    expect(higher.times).toBe(first.times);
    expect(higher.highs[4]).toBe(20);
    expect(higher.highs).not.toBe(first.highs);
    rerender({ data: lastOf(20, 3) });
    expect(result.current.lows[4]).toBe(3);
    expect(result.current.times).toBe(first.times);
  });

  it('has new extremes once a reload corrects an interior bar, though the count, the ends and the forming bar are unchanged', () => {
    const { result, rerender } = renderHook(({ data }) => useStableBars(data), { initialProps: { data: bars(5) } });
    const first = result.current;
    const corrected = bars(5);
    corrected[2] = { ...corrected[2], high: 30, low: 1 };
    rerender({ data: corrected });
    expect(result.current).not.toBe(first);
    expect(result.current.times).toBe(first.times);
    expect(result.current.highs[2]).toBe(30);
    expect(result.current.lows[2]).toBe(1);
    const fixed = result.current;
    rerender({ data: corrected.map((b) => ({ ...b })) });
    expect(result.current).toBe(fixed);
  });

  it('has new times once a reload moves an interior bar, though the count and the ends are unchanged', () => {
    const { result, rerender } = renderHook(({ data }) => useStableBars(data), { initialProps: { data: bars(5) } });
    const first = result.current;
    const moved = bars(5);
    moved[2] = { ...moved[2], time: 1150 };
    rerender({ data: moved });
    expect(result.current.times).not.toBe(first.times);
    expect(result.current.times[2]).toBe(1150);
  });
});
