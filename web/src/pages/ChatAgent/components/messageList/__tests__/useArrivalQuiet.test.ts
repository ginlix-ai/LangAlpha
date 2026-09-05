import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { useArrivalQuiet, useLiveToolRunning } from '../useArrivalQuiet';
import type { ToolCallProcessRecord } from '../types';

describe('useArrivalQuiet', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it('starts quiet, goes busy on arrival, and returns to quiet after the window', () => {
    const { result, rerender } = renderHook(({ seq }) => useArrivalQuiet(seq, true, 700), { initialProps: { seq: 0 } });
    expect(result.current).toBe(true);
    rerender({ seq: 1 });
    expect(result.current).toBe(false);
    act(() => { vi.advanceTimersByTime(400); });
    rerender({ seq: 2 });
    act(() => { vi.advanceTimersByTime(400); });
    // Still inside the window of the latest arrival.
    expect(result.current).toBe(false);
    act(() => { vi.advanceTimersByTime(400); });
    expect(result.current).toBe(true);
  });

  it('ignores arrivals while inactive', () => {
    const { result, rerender } = renderHook(({ seq }) => useArrivalQuiet(seq, false, 700), { initialProps: { seq: 0 } });
    rerender({ seq: 1 });
    expect(result.current).toBe(true);
  });

  it('does not treat text that landed while inactive as a fresh arrival', () => {
    const { result, rerender } = renderHook(
      ({ seq, active }) => useArrivalQuiet(seq, active, 700),
      { initialProps: { seq: 0, active: false } },
    );
    rerender({ seq: 3, active: false });
    // Going active on the same sequence: nothing new arrived, so the
    // indicator shows at once instead of hiding for the quiet window.
    rerender({ seq: 3, active: true });
    expect(result.current).toBe(true);
  });

  it('goes quiet again the moment it stops being active', () => {
    const { result, rerender } = renderHook(
      ({ seq, active }) => useArrivalQuiet(seq, active, 700),
      { initialProps: { seq: 0, active: true } },
    );
    rerender({ seq: 1, active: true });
    expect(result.current).toBe(false);
    // The turn ends mid-arrival: the indicator must not be left reading busy.
    rerender({ seq: 1, active: false });
    expect(result.current).toBe(true);
  });
});

describe('useLiveToolRunning', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  const proc = (toolName: string, createdAt = Date.now()): ToolCallProcessRecord =>
    ({ toolName, isInProgress: true, _createdAt: createdAt }) as unknown as ToolCallProcessRecord;

  it('counts a running tool the live zone shows', () => {
    const { result } = renderHook(() => useLiveToolRunning({ a: proc('bash') }, true));
    expect(result.current).toBe(true);
  });

  it('ignores a running tool the transcript never draws', () => {
    // manage_threads has no card, so nothing on screen says the turn is busy:
    // the spinner must stay available rather than hide behind an invisible tool.
    const { result } = renderHook(() => useLiveToolRunning({ a: proc('manage_threads') }, true));
    expect(result.current).toBe(false);
  });

  it('stops counting a regular tool once it folds out of the live zone', () => {
    const processes = { a: proc('bash', Date.now() - 14_990) };
    const { result } = renderHook(() => useLiveToolRunning(processes, true));
    expect(result.current).toBe(true);
    act(() => {
      vi.advanceTimersByTime(50);
    });
    expect(result.current).toBe(false);
  });
});
