import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { act, renderHook } from '@testing-library/react';
import { useDebouncedValue } from '../useDebouncedValue';

/**
 * The rest a typed value takes before anything acts on it. Pinned here rather
 * than through a form, because a component test that waits out the real delay
 * pays it on every assertion, and faking the clock inside one deadlocks
 * Testing Library's async helpers.
 */
describe('useDebouncedValue', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it('holds the new value until the delay has passed', () => {
    const { result, rerender } = renderHook(({ v }) => useDebouncedValue(v, 700), {
      initialProps: { v: 'a' },
    });
    expect(result.current).toBe('a');

    rerender({ v: 'b' });
    expect(result.current).toBe('a');
    act(() => void vi.advanceTimersByTime(699));
    expect(result.current).toBe('a');
    act(() => void vi.advanceTimersByTime(1));
    expect(result.current).toBe('b');
  });

  it('starts the delay again on every change, so only the last value lands', () => {
    const { result, rerender } = renderHook(({ v }) => useDebouncedValue(v, 700), {
      initialProps: { v: 'a' },
    });
    for (const v of ['b', 'c', 'd']) {
      rerender({ v });
      act(() => void vi.advanceTimersByTime(600));
    }
    expect(result.current).toBe('a');
    act(() => void vi.advanceTimersByTime(700));
    expect(result.current).toBe('d');
  });

  it('keeps the first value when a change is taken back before it lands', () => {
    const { result, rerender } = renderHook(({ v }) => useDebouncedValue(v, 700), {
      initialProps: { v: 'a' },
    });
    rerender({ v: 'b' });
    rerender({ v: 'a' });
    act(() => void vi.advanceTimersByTime(700));
    expect(result.current).toBe('a');
  });
});
