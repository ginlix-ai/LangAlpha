import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, render, renderHook, screen } from '@testing-library/react';
import { relativeTime } from '@/lib/format';
import { useNow } from '../useNow';

const START = new Date('2026-09-28T12:00:30Z').getTime();

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(START);
});

afterEach(() => {
  vi.useRealTimers();
});

// The unit suite runs through the React Compiler, so this component caches the
// label on its inputs exactly as the app does: the label can only move if the
// time arrives as one of them.
function Ago({ at }: { at: number }) {
  const now = useNow();
  return <span>{relativeTime(at, 'en-US', now)}</span>;
}

describe('useNow', () => {
  it('moves a label rendered from the time', () => {
    render(<Ago at={START - 5.5 * 60_000} />);
    expect(screen.getByText('5m ago')).toBeTruthy();
    act(() => vi.advanceTimersByTime(90_000));
    expect(screen.getByText('7m ago')).toBeTruthy();
  });

  it('turns on the interval boundary, not an interval after mount', () => {
    const { result } = renderHook(() => useNow());
    expect(result.current).toBe(START);
    act(() => vi.advanceTimersByTime(30_000));
    expect(result.current).toBe(START + 30_000);
    act(() => vi.advanceTimersByTime(60_000));
    expect(result.current).toBe(START + 90_000);
  });

  it('shares one timer between readers of the same interval', () => {
    const a = renderHook(() => useNow());
    const b = renderHook(() => useNow());
    expect(vi.getTimerCount()).toBe(1);
    a.unmount();
    expect(vi.getTimerCount()).toBe(1);
    b.unmount();
    expect(vi.getTimerCount()).toBe(0);
  });

  it('holds still while disabled and catches up when enabled again', () => {
    const { result, rerender } = renderHook(({ on }) => useNow(1000, on), { initialProps: { on: false } });
    expect(vi.getTimerCount()).toBe(0);
    act(() => vi.advanceTimersByTime(5_000));
    expect(result.current).toBe(START);
    rerender({ on: true });
    expect(result.current).toBe(START + 5_000);
  });

  it('stops at the deadline its test of the time sets', () => {
    const { result } = renderHook(() => useNow(1000, (now) => now < START + 2_500));
    act(() => vi.advanceTimersByTime(3_000));
    expect(result.current).toBe(START + 3_000);
    expect(vi.getTimerCount()).toBe(0);
    act(() => vi.advanceTimersByTime(5_000));
    expect(result.current).toBe(START + 3_000);
  });

  it('catches up when a hidden tab comes back', () => {
    const { result } = renderHook(() => useNow());
    // Throttled timers: the clock moved, the tick has not fired.
    vi.setSystemTime(START + 20_000);
    act(() => {
      document.dispatchEvent(new Event('visibilitychange'));
    });
    expect(result.current).toBe(START + 20_000);
  });
});
