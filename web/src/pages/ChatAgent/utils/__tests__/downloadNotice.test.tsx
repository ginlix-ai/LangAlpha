import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, renderHook } from '@testing-library/react';

vi.mock('@/components/ui/use-toast', () => ({ toast: vi.fn(() => ({ dismiss: vi.fn(), update: vi.fn() })) }));

import { trackPending, useDownloadState } from '../downloadNotice';

describe('trackPending', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it('joins a repeat save, then holds the key as started before it goes idle', async () => {
    let finish!: (handedOff: boolean) => void;
    const run = vi.fn(() => new Promise<boolean>((resolve) => { finish = resolve; }));
    const { result } = renderHook(() => useDownloadState('ws:big.bin'));
    expect(result.current).toBe('idle');

    let first!: Promise<boolean>;
    let second!: Promise<boolean>;
    act(() => {
      first = trackPending('ws:big.bin', run);
      second = trackPending('ws:big.bin', run);
    });
    expect(run).toHaveBeenCalledTimes(1);
    expect(second).toBe(first);
    expect(result.current).toBe('preparing');

    await act(async () => {
      finish(true);
      await first;
    });
    expect(result.current).toBe('started');

    // A click inside the hold is the same click again.
    await act(async () => {
      await trackPending('ws:big.bin', run);
    });
    expect(run).toHaveBeenCalledTimes(1);

    act(() => {
      vi.runAllTimers();
    });
    expect(result.current).toBe('idle');
  });

  it('goes straight back to idle when nothing was handed to the browser', async () => {
    const { result } = renderHook(() => useDownloadState('ws:gone.bin'));
    await act(async () => {
      await trackPending('ws:gone.bin', async () => false);
    });
    expect(result.current).toBe('idle');
  });
});
