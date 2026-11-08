/**
 * The fill behind first paint runs in idle time, once the reader can see the
 * transcript, and never while the tab is hidden.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { renderHook } from '@testing-library/react';
import type { HistoryWindowSnapshot, OlderHistoryStatus } from '../../../session/history/historyWindow';
import { useHistoryBackfill } from '../useHistoryBackfill';

let visibility: DocumentVisibilityState = 'visible';

function setVisibility(next: DocumentVisibilityState) {
  visibility = next;
  document.dispatchEvent(new Event('visibilitychange'));
}

describe('useHistoryBackfill', () => {
  const place = vi.fn();
  let backfill: ReturnType<typeof vi.fn<(place?: unknown) => Promise<void>>>;

  beforeEach(() => {
    vi.useFakeTimers();
    visibility = 'visible';
    vi.spyOn(document, 'visibilityState', 'get').mockImplementation(() => visibility);
    backfill = vi.fn(async () => {});
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  const snap = (status: OlderHistoryStatus, hasAhead = false): HistoryWindowSnapshot =>
    ({ status, aligned: false, hasAhead });
  const run = (snapshot: HistoryWindowSnapshot, ready = true) =>
    renderHook((props: { snapshot: HistoryWindowSnapshot; ready: boolean }) =>
      useHistoryBackfill({ ...props, backfill, place }), { initialProps: { snapshot, ready } });

  it('asks for a step once the window is idle and the reader can see the transcript', () => {
    const hook = run(snap('reloading'));
    vi.runAllTimers();
    expect(backfill).not.toHaveBeenCalled();
    hook.rerender({ snapshot: snap('idle'), ready: false });
    vi.runAllTimers();
    expect(backfill).not.toHaveBeenCalled();
    hook.rerender({ snapshot: snap('idle'), ready: true });
    vi.runAllTimers();
    expect(backfill).toHaveBeenCalledWith(place);
  });

  it('waits for a hidden tab to be shown', () => {
    visibility = 'hidden';
    run(snap('idle'));
    vi.runAllTimers();
    expect(backfill).not.toHaveBeenCalled();
    setVisibility('visible');
    vi.runAllTimers();
    expect(backfill).toHaveBeenCalledTimes(1);
  });

  it('drops a step not yet started when the status moves', () => {
    const hook = run(snap('idle'));
    hook.rerender({ snapshot: snap('reloading'), ready: true });
    vi.runAllTimers();
    expect(backfill).not.toHaveBeenCalled();
  });

  // The status stays idle across it, so only the held page leaving says so.
  it('asks again when the scroll trigger takes the page held ahead', () => {
    const hook = run(snap('idle', true));
    vi.runAllTimers();
    expect(backfill).toHaveBeenCalledTimes(1);
    hook.rerender({ snapshot: snap('idle', false), ready: true });
    vi.runAllTimers();
    expect(backfill).toHaveBeenCalledTimes(2);
  });
});
