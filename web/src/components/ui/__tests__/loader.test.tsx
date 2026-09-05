/**
 * Locks the shared-ticker regression: the subscribe callback handed to
 * useSyncExternalStore must be identity-stable across renders. An inline
 * closure resubscribed on every frame tick, so the interval was torn down and
 * recreated at frame 0 each step — the glyph froze while timers churned.
 */
import { render, screen, act } from '@testing-library/react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import Loader from '../loader';

const FRAME_COUNT = 10; // ASCII_FRAMES.length
const STEP_MS = (0.8 / FRAME_COUNT) * 1000; // default speed

describe('Loader shared ticker', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  const glyph = () =>
    screen.getByRole('status').querySelector('.font-mono')?.textContent;

  it('advances through distinct frames instead of freezing at the first tick', () => {
    render(<Loader />);
    const seen = new Set<string>();
    seen.add(glyph()!);
    for (let i = 0; i < 3; i++) {
      act(() => {
        vi.advanceTimersByTime(STEP_MS + 1);
      });
      seen.add(glyph()!);
    }
    // 4 observations across 3 ticks must yield at least 3 distinct glyphs —
    // the frozen regression yields exactly 1.
    expect(seen.size).toBeGreaterThanOrEqual(3);
  });

  it('keeps one interval alive across ticks (no per-render churn)', () => {
    const setSpy = vi.spyOn(globalThis, 'setInterval');
    const clearSpy = vi.spyOn(globalThis, 'clearInterval');
    render(<Loader />);
    act(() => {
      vi.advanceTimersByTime(STEP_MS * 5);
    });
    // One interval created on mount and never cleared while mounted; the
    // regression cleared + recreated it on every tick.
    expect(setSpy).toHaveBeenCalledTimes(1);
    expect(clearSpy).not.toHaveBeenCalled();
  });

  it('shares one interval across concurrent instances and cleans up on unmount', () => {
    const setSpy = vi.spyOn(globalThis, 'setInterval');
    const clearSpy = vi.spyOn(globalThis, 'clearInterval');
    const a = render(<Loader />);
    const b = render(<Loader />);
    expect(setSpy).toHaveBeenCalledTimes(1);
    a.unmount();
    expect(clearSpy).not.toHaveBeenCalled();
    b.unmount();
    expect(clearSpy).toHaveBeenCalledTimes(1);
  });
});
