import { describe, it, expect } from 'vitest';
import {
  centerLatestBarView,
  computeInitialLoadRange,
  consumePrefetchBuffer,
  dedupeMergeByTime,
  etDateStr,
  rangeBeforeOldest,
} from '../chartDataLoaders';

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

  it('skips duplicates and counts only new bars as prepended', () => {
    const r = dedupeMergeByTime([{ time: 2 }], [{ time: 1 }, { time: 2 }]);
    expect(r.prependedCount).toBe(1);
    expect(r.merged.map((b) => b.time)).toEqual([1, 2]);
  });

  it('sorts merged output by time', () => {
    const r = dedupeMergeByTime([{ time: 3 }], [{ time: 1 }, { time: 5 }]);
    expect(r.merged.map((b) => b.time)).toEqual([1, 3, 5]);
  });

  it('reports 0 prepended when all incoming were duplicates', () => {
    const r = dedupeMergeByTime([{ time: 1 }, { time: 2 }], [{ time: 1 }, { time: 2 }]);
    expect(r.prependedCount).toBe(0);
  });
});

describe('consumePrefetchBuffer', () => {
  it('returns null for null buffer', () => {
    expect(consumePrefetchBuffer(null, 100)).toBeNull();
  });

  it('returns null when currentOldest is null', () => {
    expect(consumePrefetchBuffer({ data: [{ time: 1 }], anchorOldest: 100 }, null)).toBeNull();
  });

  it('returns null on anchor mismatch', () => {
    expect(consumePrefetchBuffer({ data: [{ time: 1 }], anchorOldest: 100 }, 99)).toBeNull();
  });

  it('returns null for empty buffered data', () => {
    expect(consumePrefetchBuffer({ data: [], anchorOldest: 100 }, 100)).toBeNull();
  });

  it('returns the buffered data on anchor match', () => {
    const d = [{ time: 1 }];
    expect(consumePrefetchBuffer({ data: d, anchorOldest: 100 }, 100)).toBe(d);
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
