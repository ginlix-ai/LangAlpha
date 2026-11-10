// @vitest-environment node
import { describe, expect, it } from 'vitest';

import { nearestBarIndex, snapToNearestBar, toUnixSeconds } from '../barTime';

const T = (iso: string): number => Math.floor(Date.parse(iso) / 1000);

const BARS = [
  { time: T('2024-10-16T00:00:00Z') },
  { time: T('2024-11-14T00:00:00Z') },
  { time: T('2024-11-20T00:00:00Z') },
  { time: T('2024-12-20T00:00:00Z') },
];

describe('bar time helpers', () => {
  it('toUnixSeconds parses ISO and rejects garbage', () => {
    expect(toUnixSeconds('2024-10-16T00:00:00Z')).toBe(T('2024-10-16T00:00:00Z'));
    expect(toUnixSeconds('not-a-date')).toBeNull();
  });

  it('snapToNearestBar returns the closest bar time', () => {
    // 2024-11-13 is closest to the 11-14 bar
    expect(snapToNearestBar(BARS, T('2024-11-13T06:00:00Z'))).toBe(T('2024-11-14T00:00:00Z'));
    expect(snapToNearestBar([], 123)).toBeNull();
  });

  it('nearestBarIndex takes the later bar on a tie and -1 with no bars', () => {
    const mid = (BARS[1].time + BARS[2].time) / 2;
    expect(nearestBarIndex(BARS, mid)).toBe(2);
    expect(nearestBarIndex(BARS, BARS[3].time + 10_000)).toBe(3);
    expect(nearestBarIndex(null, 0)).toBe(-1);
  });
});
