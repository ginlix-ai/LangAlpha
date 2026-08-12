import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { useResendCooldown } from '@/pages/Login/useResendCooldown';

describe('useResendCooldown', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it('defaults to a 60s cooldown that starts on mount', () => {
    const { result } = renderHook(() => useResendCooldown());
    expect(result.current.secondsLeft).toBe(60);
    expect(result.current.isCoolingDown).toBe(true);
  });

  it('honors a custom starting duration', () => {
    const { result } = renderHook(() => useResendCooldown(3));
    expect(result.current.secondsLeft).toBe(3);
  });

  // Advance one second per act() so each tick's effect re-schedules the next
  // setTimeout before we advance again (batching multiple seconds in a single
  // advanceTimersByTime drops the re-scheduled tick).
  const tick = () => act(() => { vi.advanceTimersByTime(1000); });

  it('decrements once per second and stops (isCoolingDown false) at zero', () => {
    const { result } = renderHook(() => useResendCooldown(3));

    tick();
    expect(result.current.secondsLeft).toBe(2);
    expect(result.current.isCoolingDown).toBe(true);

    tick();
    expect(result.current.secondsLeft).toBe(1);

    tick();
    expect(result.current.secondsLeft).toBe(0);
    expect(result.current.isCoolingDown).toBe(false);

    // No further ticks are scheduled once it hits zero.
    act(() => {
      vi.advanceTimersByTime(5000);
    });
    expect(result.current.secondsLeft).toBe(0);
  });

  it('start() restarts the countdown from the configured duration', () => {
    const { result } = renderHook(() => useResendCooldown(2));

    tick();
    tick();
    expect(result.current.secondsLeft).toBe(0);
    expect(result.current.isCoolingDown).toBe(false);

    act(() => {
      result.current.start();
    });
    expect(result.current.secondsLeft).toBe(2);
    expect(result.current.isCoolingDown).toBe(true);
  });
});
