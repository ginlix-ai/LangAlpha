// @vitest-environment node
import { afterEach, beforeEach, describe, it, expect, vi } from 'vitest';
import { createSettleWindow, isNearBottom } from '../scrollHelpers';

describe('isNearBottom', () => {
  it('is true at the very bottom', () => {
    expect(isNearBottom({ scrollTop: 900, scrollHeight: 1000, clientHeight: 100 })).toBe(true);
  });
  it('is false far from the bottom', () => {
    expect(isNearBottom({ scrollTop: 0, scrollHeight: 1000, clientHeight: 100 })).toBe(false);
  });
  it('respects the threshold boundary (default 120)', () => {
    // distance = scrollHeight - scrollTop - clientHeight
    expect(isNearBottom({ scrollTop: 781, scrollHeight: 1000, clientHeight: 100 })).toBe(true); // 119
    expect(isNearBottom({ scrollTop: 780, scrollHeight: 1000, clientHeight: 100 })).toBe(true); // 120 (inclusive)
    expect(isNearBottom({ scrollTop: 779, scrollHeight: 1000, clientHeight: 100 })).toBe(false); // 121
  });
  it('honors a custom threshold', () => {
    expect(isNearBottom({ scrollTop: 870, scrollHeight: 1000, clientHeight: 100 }, 40)).toBe(true); // 30
    expect(isNearBottom({ scrollTop: 850, scrollHeight: 1000, clientHeight: 100 }, 40)).toBe(false); // 50
  });
});

describe('createSettleWindow', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it('lapses after a quiet period, and the next landing gets a hard cap of its own', () => {
    const onExpire = vi.fn();
    const settle = createSettleWindow(onExpire);
    settle.arm();
    vi.advanceTimersByTime(1000);
    settle.arm();
    vi.advanceTimersByTime(1499);
    expect(onExpire).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(onExpire).toHaveBeenCalledTimes(1);

    // Resizes that never go quiet hold it until the cap, counted from this
    // landing rather than the last one.
    settle.arm();
    for (let i = 0; i < 7; i++) {
      vi.advanceTimersByTime(1000);
      settle.arm();
    }
    expect(onExpire).toHaveBeenCalledTimes(1);
    vi.advanceTimersByTime(1000);
    expect(onExpire).toHaveBeenCalledTimes(2);
    vi.advanceTimersByTime(10_000);
    expect(onExpire).toHaveBeenCalledTimes(2);
  });

  it('closes without expiring when cleared', () => {
    const onExpire = vi.fn();
    const settle = createSettleWindow(onExpire);
    settle.arm();
    settle.clear();
    vi.advanceTimersByTime(10_000);
    expect(onExpire).not.toHaveBeenCalled();
  });
});
