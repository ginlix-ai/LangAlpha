/**
 * The hint follows the server's reading: a cold summary only once its cache
 * has expired, nothing while a turn runs, and a skip that lasts one send and
 * belongs to the reading it was made on.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { act, renderHook } from '@testing-library/react';
import { useNextSummaryHint } from '../useNextSummaryHint';
import type { NextSummary } from '../../utils/api';

const NOW = 1_000_000;

describe('useNextSummaryHint', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
  });
  afterEach(() => vi.useRealTimers());

  const render = (next: NextSummary | null, running = false) =>
    renderHook(({ next, running }) => useNextSummaryHint(next, running), { initialProps: { next, running } });

  it('shows a cold summary from the time the cache expires, and nothing while a turn runs', () => {
    const cold: NextSummary = { reason: 'cold', at: NOW + 5_000, now: NOW };
    const { result, rerender } = render(cold);

    expect(result.current.reason).toBeNull();
    act(() => vi.advanceTimersByTime(5_000));
    expect(result.current.reason).toBe('cold');
    rerender({ next: cold, running: true });
    expect(result.current.reason).toBeNull();
    rerender({ next: { reason: 'threshold' }, running: false });
    expect(result.current.reason).toBe('threshold');
  });

  it("counts to the cache's expiry on the server's clock, whatever the browser's says", () => {
    for (const skew of [-600_000, 600_000]) {
      vi.setSystemTime(NOW + skew);
      const { result, unmount } = render({ reason: 'cold', at: NOW + 5_000, now: NOW });

      act(() => vi.advanceTimersByTime(4_999));
      expect(result.current.reason).toBeNull();
      act(() => vi.advanceTimersByTime(1));
      expect(result.current.reason).toBe('cold');
      unmount();
    }
  });

  it('hides a cold reading once the account turns summaries after a break off, and nothing else', () => {
    const { result, rerender } = renderHook(
      ({ next, off }) => useNextSummaryHint(next, false, off),
      { initialProps: { next: { reason: 'cold', at: NOW, now: NOW } as NextSummary, off: true } },
    );
    act(() => vi.advanceTimersByTime(0));
    expect(result.current.reason).toBeNull();
    // The setting may not be stored yet, so the send skips what it hides.
    expect(result.current.keepFullContext).toBe(true);
    expect(result.current.skipped).toBe(false);
    rerender({ next: { reason: 'rebuild', at: NOW, now: NOW }, off: true });
    act(() => vi.advanceTimersByTime(0));
    expect(result.current.reason).toBe('rebuild');
    expect(result.current.keepFullContext).toBe(false);
    rerender({ next: { reason: 'threshold' }, off: true });
    expect(result.current.reason).toBe('threshold');
    expect(result.current.keepFullContext).toBe(false);
    rerender({ next: { reason: 'cold', at: NOW, now: NOW }, off: false });
    expect(result.current.reason).toBe('cold');
    expect(result.current.keepFullContext).toBe(false);
    act(() => result.current.toggleSkip());
    expect(result.current.keepFullContext).toBe(true);
  });

  it('shows a rebuild from its time, with no skip to take', () => {
    const rebuild: NextSummary = { reason: 'rebuild', at: NOW + 5_000, now: NOW };
    const { result } = render(rebuild);

    expect(result.current.reason).toBeNull();
    act(() => vi.advanceTimersByTime(5_000));
    expect(result.current.reason).toBe('rebuild');
    act(() => result.current.toggleSkip());
    expect(result.current.skipped).toBe(false);
  });

  it('shows an urgent rebuild, which has no time, at once', () => {
    const { result } = render({ reason: 'rebuild' });

    expect(result.current.reason).toBe('rebuild');
    expect(result.current.skipped).toBe(false);
  });

  it('a skip outlasts a send that starts no run, and ends with the run one starts', () => {
    const cold: NextSummary = { reason: 'cold', at: NOW, now: NOW };
    const { result, rerender } = render(cold);
    act(() => vi.advanceTimersByTime(0));
    act(() => result.current.toggleSkip());

    // A send refused or lost before its run: the reading stands.
    rerender({ next: cold, running: true });
    rerender({ next: cold, running: false });
    expect(result.current.skipped).toBe(true);

    // A send that starts its run clears the reading, and the skip with it,
    // even when the run ends on the same reading.
    rerender({ next: null, running: true });
    rerender({ next: cold, running: false });
    expect(result.current.reason).toBe('cold');
    expect(result.current.skipped).toBe(false);
  });

  it('a new reading starts without the skip made on the last one', () => {
    const { result, rerender } = render({ reason: 'cold', at: NOW, now: NOW });
    act(() => vi.advanceTimersByTime(0));
    act(() => result.current.toggleSkip());

    rerender({ next: { reason: 'cold', at: NOW + 1, now: NOW }, running: false });
    act(() => vi.advanceTimersByTime(1));
    expect(result.current.reason).toBe('cold');
    expect(result.current.skipped).toBe(false);
  });
});
