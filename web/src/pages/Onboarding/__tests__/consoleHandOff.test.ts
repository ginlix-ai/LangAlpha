import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

const h = vi.hoisted(() => ({ desktop: false, open: vi.fn() }));
vi.mock('@/lib/desktop', () => ({
  isDesktopShell: () => h.desktop,
  openConsolePage: h.open,
}));

import { openAfterStamp, STAMP_WAIT_MS } from '../consoleHandOff';

describe('openAfterStamp', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    h.open.mockClear();
  });
  afterEach(() => vi.useRealTimers());

  it('opens the page in a browser at once, inside the click', () => {
    h.desktop = false;
    openAfterStamp(new Promise(() => {}), '/account/integrations');
    expect(h.open).toHaveBeenCalledWith('/account/integrations');
  });

  it('in the desktop app leaves only once the stamp is saved', async () => {
    h.desktop = true;
    let stamped!: () => void;
    openAfterStamp(new Promise<void>((resolve) => (stamped = resolve)), '/account/integrations');
    await vi.advanceTimersByTimeAsync(STAMP_WAIT_MS - 1);
    expect(h.open).not.toHaveBeenCalled();
    stamped();
    await vi.advanceTimersByTimeAsync(0);
    expect(h.open).toHaveBeenCalledWith('/account/integrations');
  });

  it('in the desktop app leaves anyway once the wait runs out or the stamp fails', async () => {
    h.desktop = true;
    openAfterStamp(new Promise(() => {}), '/a');
    await vi.advanceTimersByTimeAsync(STAMP_WAIT_MS);
    expect(h.open).toHaveBeenCalledWith('/a');
    openAfterStamp(Promise.reject(new Error('refused')), '/b');
    await vi.advanceTimersByTimeAsync(0);
    expect(h.open).toHaveBeenCalledWith('/b');
  });
});
