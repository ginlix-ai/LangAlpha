import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { useDebouncedSave } from '../useDebouncedSave';

describe('useDebouncedSave', () => {
  beforeEach(() => { vi.useFakeTimers(); });
  afterEach(() => { vi.useRealTimers(); });

  it('starts with idle status', () => {
    const saveFn = vi.fn().mockResolvedValue(undefined);
    const { result } = renderHook(() => useDebouncedSave(saveFn, 500));
    expect(result.current.status).toBe('idle');
  });

  it('trigger() debounces then transitions idle → saving → saved → idle', async () => {
    const saveFn = vi.fn().mockResolvedValue(undefined);
    const { result } = renderHook(() => useDebouncedSave(saveFn, 500));

    act(() => { result.current.trigger(); });
    expect(saveFn).not.toHaveBeenCalled();

    await act(async () => { vi.advanceTimersByTime(500); });
    expect(saveFn).toHaveBeenCalledTimes(1);
    expect(result.current.status).toBe('saved');

    act(() => { vi.advanceTimersByTime(2000); });
    expect(result.current.status).toBe('idle');
  });

  it('rapid trigger() calls coalesce into a single save', async () => {
    const saveFn = vi.fn().mockResolvedValue(undefined);
    const { result } = renderHook(() => useDebouncedSave(saveFn, 500));

    act(() => { result.current.trigger(); });
    act(() => { vi.advanceTimersByTime(200); });
    act(() => { result.current.trigger(); });
    act(() => { vi.advanceTimersByTime(200); });
    act(() => { result.current.trigger(); });

    await act(async () => { vi.advanceTimersByTime(500); });
    expect(saveFn).toHaveBeenCalledTimes(1);
  });

  it('flush() fires immediately and cancels pending debounce', async () => {
    const saveFn = vi.fn().mockResolvedValue(undefined);
    const { result } = renderHook(() => useDebouncedSave(saveFn, 500));

    act(() => { result.current.trigger(); });
    await act(async () => { result.current.flush(); });
    expect(saveFn).toHaveBeenCalledTimes(1);

    await act(async () => { vi.advanceTimersByTime(500); });
    expect(saveFn).toHaveBeenCalledTimes(1);
  });

  it('transitions to error status when saveFn rejects', async () => {
    const saveFn = vi.fn().mockRejectedValue(new Error('network'));
    const { result } = renderHook(() => useDebouncedSave(saveFn, 500));

    await act(async () => { result.current.flush(); });
    expect(result.current.status).toBe('error');

    act(() => { vi.advanceTimersByTime(3000); });
    expect(result.current.status).toBe('idle');
  });

  it('cleans up timers on unmount', async () => {
    const saveFn = vi.fn().mockResolvedValue(undefined);
    const { result, unmount } = renderHook(() => useDebouncedSave(saveFn, 500));

    act(() => { result.current.trigger(); });
    unmount();

    await act(async () => { vi.advanceTimersByTime(1000); });
    expect(saveFn).not.toHaveBeenCalled();
  });
});
