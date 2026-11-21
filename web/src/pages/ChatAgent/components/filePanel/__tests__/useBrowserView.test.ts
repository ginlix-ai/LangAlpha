import { renderHook } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { BrowserViewSession } from '../browserStream';
import { useBrowserView } from '../useBrowserView';

describe('useBrowserView', () => {
  it('watches again on the computer a moved workspace now lives on', () => {
    const view = new BrowserViewSession();
    const stops: ReturnType<typeof vi.fn>[] = [];
    const watch = vi.spyOn(view, 'watch').mockImplementation(() => {
      const stop = vi.fn();
      stops.push(stop);
      return stop;
    });
    const { rerender } = renderHook(
      ({ computerId }: { computerId: string | null }) => useBrowserView(view, 'ws-1', computerId, true),
      { initialProps: { computerId: 'computer-a' } },
    );
    expect(watch).toHaveBeenCalledTimes(1);
    rerender({ computerId: 'computer-a' });
    expect(watch).toHaveBeenCalledTimes(1);
    rerender({ computerId: 'computer-b' });
    expect(stops[0]).toHaveBeenCalledTimes(1);
    expect(watch).toHaveBeenCalledTimes(2);
  });
});
