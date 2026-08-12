import { describe, it, expect } from 'vitest';
import {
  advanceWatermark,
  centerLatestBarView,
  computeInitialLoadRange,
  dedupeMergeByTime,
  etDateStr,
  rangeBeforeOldest,
  shouldSkipPollWhileWsHealthy,
} from '../chartDataLoaders';
import { WS_RECONCILE_POLL_MS, WS_STALE_WINDOW_MS } from '../chartConstants';

describe('dedupeMergeByTime', () => {
  it('returns existing unchanged for empty incoming', () => {
    const e = [{ time: 1 }];
    expect(dedupeMergeByTime(e, [])).toEqual({ merged: e, prependedCount: 0 });
  });

  it('returns existing unchanged for null-ish incoming', () => {
    const e = [{ time: 1 }];
    // @ts-expect-error simulating a runtime null
    expect(dedupeMergeByTime(e, null)).toEqual({ merged: e, prependedCount: 0 });
  });

  it('dedupes by time, counting only net-new bars as prepended', () => {
    const r = dedupeMergeByTime([{ time: 2 }], [{ time: 1 }, { time: 2 }]);
    expect(r.prependedCount).toBe(1);
    expect(r.merged.map((b) => b.time)).toEqual([1, 2]);
  });

  it('prefers the INCOMING bar on a time collision (fresher data wins)', () => {
    // The forming head bar is re-served with the same timestamp as its OHLCV
    // updates; the merge must replace, not skip, or the last candle freezes.
    const existing = [{ time: 1, close: 10 }, { time: 2, close: 20 }];
    const incoming = [{ time: 2, close: 25 }];
    const r = dedupeMergeByTime(existing, incoming);
    expect(r.prependedCount).toBe(0);
    expect(r.merged.map((b) => b.close)).toEqual([10, 25]);
  });

  it('sorts merged output by time', () => {
    const r = dedupeMergeByTime([{ time: 3 }], [{ time: 1 }, { time: 5 }]);
    expect(r.merged.map((b) => b.time)).toEqual([1, 3, 5]);
  });

  it('reports 0 prepended when all incoming collide with existing times', () => {
    const r = dedupeMergeByTime([{ time: 1 }, { time: 2 }], [{ time: 1 }, { time: 2 }]);
    expect(r.prependedCount).toBe(0);
  });
});

describe('rangeBeforeOldest', () => {
  it('leaves a 1-day gap before the oldest bar', () => {
    const oldestSec = Date.UTC(2025, 5, 10) / 1000;
    const { toStr, fromStr } = rangeBeforeOldest(oldestSec, 5);
    expect(toStr).toBe('2025-06-09');
    expect(fromStr).toBe('2025-06-04');
  });

  it('handles a 1-day window', () => {
    const oldestSec = Date.UTC(2025, 0, 2) / 1000;
    const { fromStr, toStr } = rangeBeforeOldest(oldestSec, 1);
    expect(toStr).toBe('2025-01-01');
    expect(fromStr).toBe('2024-12-31');
  });
});

describe('etDateStr', () => {
  it('formats a noon-UTC moment as an ISO-style ET date', () => {
    // Jun 10 16:00 UTC = Jun 10 12:00 EDT — same calendar day in both zones.
    expect(etDateStr(new Date(Date.UTC(2025, 5, 10, 16, 0)))).toBe('2025-06-10');
  });

  it('rolls back to the prior ET day when UTC has already ticked over', () => {
    // Jun 10 02:00 UTC = Jun 9 22:00 EDT — UTC-based formatting would wrongly
    // return "2025-06-10". ET formatting must keep the request on Jun 9.
    expect(etDateStr(new Date(Date.UTC(2025, 5, 10, 2, 0)))).toBe('2025-06-09');
  });
});

describe('computeInitialLoadRange', () => {
  const now = new Date(Date.UTC(2025, 5, 10, 16, 0)); // Jun 10 noon ET

  it('prefers STAGE1_LOAD_DAYS over INITIAL_LOAD_DAYS', () => {
    // '1min' has STAGE1_LOAD_DAYS = 2 (vs INITIAL_LOAD_DAYS = 7)
    const r = computeInitialLoadRange('1min', { now });
    expect(r.days).toBe(2);
    expect(r.toStr).toBe('2025-06-10');
    expect(r.fromStr).toBe('2025-06-08');
  });

  it('falls back to INITIAL_LOAD_DAYS when no stage-1 entry', () => {
    // '5min' has no STAGE1_LOAD_DAYS; INITIAL_LOAD_DAYS = 30
    const r = computeInitialLoadRange('5min', { now });
    expect(r.days).toBe(30);
    expect(r.toStr).toBe('2025-06-10');
    expect(r.fromStr).toBe('2025-05-11');
  });

  it('returns undefined bounds when days is 0 (full history)', () => {
    // '1day' → INITIAL_LOAD_DAYS = 0
    const r = computeInitialLoadRange('1day', { now });
    expect(r.days).toBe(0);
    expect(r.fromStr).toBeUndefined();
    expect(r.toStr).toBeUndefined();
  });

  it('extends fromStr by MA-lookback overhead when requested', () => {
    // '5min' → 192 bars/day. MA200 → ceil(200/192 * 1.5) = 2 extra days
    const r = computeInitialLoadRange('5min', { now, maxMaPeriod: 200 });
    expect(r.days).toBe(30);
    expect(r.fromStr).toBe('2025-05-09'); // 30 + 2 days before 2025-06-10
  });
});

describe('centerLatestBarView', () => {
  it('centers the latest bar with half-width future space', () => {
    const r = centerLatestBarView({ chartWidth: 400, barSpacing: 10, dataLen: 100 });
    expect(r.from).toBe(100 - 20);
    expect(r.to).toBe(100 + 20);
  });

  it('handles a tiny chart width by floor-ing the half-bars', () => {
    const r = centerLatestBarView({ chartWidth: 25, barSpacing: 10, dataLen: 50 });
    expect(r.from).toBe(49); // floor(25/10/2) = 1
    expect(r.to).toBe(51);
  });
});

describe('advanceWatermark', () => {
  const WM = 1_750_000_000_000; // an arbitrary ms watermark
  const MIN = 60; // 1min bucket

  it('adopts the incoming watermark when there is no cursor yet', () => {
    expect(advanceWatermark(null, WM, MIN)).toBe(WM);
    expect(advanceWatermark(undefined, WM, MIN)).toBe(WM);
  });

  it('keeps the cursor on a null incoming watermark (empty/failed poll)', () => {
    expect(advanceWatermark(WM, null, MIN)).toBe(WM);
    expect(advanceWatermark(WM, undefined, MIN)).toBe(WM);
    expect(advanceWatermark(null, null, MIN)).toBeNull();
  });

  it('moves forward on a newer incoming watermark', () => {
    expect(advanceWatermark(WM, WM + 60_000, MIN)).toBe(WM + 60_000);
  });

  it('holds against small (intra-bucket) rewinds — out-of-order jitter', () => {
    expect(advanceWatermark(WM, WM - 30_000, MIN)).toBe(WM);
    expect(advanceWatermark(WM, WM - 60_000, MIN)).toBe(WM); // exactly one bucket → still jitter
  });

  it('adopts a watermark more than one bucket older — server envelope rebuilt', () => {
    const rebuilt = WM - 5 * 60_000;
    expect(advanceWatermark(WM, rebuilt, MIN)).toBe(rebuilt);
  });

  it('scales the rewind threshold with the interval', () => {
    const hourAgo = WM - 3600_000;
    // 1h bucket: one-bucket-old is jitter…
    expect(advanceWatermark(WM, hourAgo, 3600)).toBe(WM);
    // …but the same rewind on a 1min bucket is a rebuild.
    expect(advanceWatermark(WM, hourAgo, 60)).toBe(hourAgo);
  });
});

describe('shouldSkipPollWhileWsHealthy', () => {
  const NOW = 1_750_000_000_000;

  it('skips while WS is healthy and the reconcile is not due', () => {
    expect(shouldSkipPollWhileWsHealthy(NOW - 1000, NOW - 5000, NOW)).toBe(true);
  });

  it('does not skip when the WS feed has gone quiet', () => {
    expect(shouldSkipPollWhileWsHealthy(NOW - WS_STALE_WINDOW_MS - 1, NOW - 5000, NOW)).toBe(false);
    expect(shouldSkipPollWhileWsHealthy(0, 0, NOW)).toBe(false); // never ticked
  });

  it('lets the periodic reconcile through even while WS is healthy', () => {
    expect(shouldSkipPollWhileWsHealthy(NOW - 1000, NOW - WS_RECONCILE_POLL_MS, NOW)).toBe(false);
    expect(shouldSkipPollWhileWsHealthy(NOW - 1000, 0, NOW)).toBe(false); // never reconciled
  });
});
