// @vitest-environment node
import { describe, expect, it } from 'vitest';

import { buildChartView, logicalRangeForTimes } from '../chartView';

const DAY = 86400;
const MIN = 60;

/** 1min bars 04:00–19:59 (extended session) on each given UTC date. */
function minuteSessions(dates: string[]): number[] {
  const out: number[] = [];
  for (const d of dates) {
    const start = Date.parse(`${d}T04:00:00Z`) / 1000;
    for (let k = 0; k < 16 * 60; k++) out.push(start + k * MIN);
  }
  return out;
}

function weekdayBars(count: number, last = Date.UTC(2026, 9, 30) / 1000): number[] {
  const out: number[] = [];
  for (let t = last; out.length < count; t -= DAY) {
    const d = new Date(t * 1000).getUTCDay();
    if (d !== 0 && d !== 6) out.unshift(t);
  }
  return out;
}

const base = { symbol: 'GOOG', theme: 'dark' as const, locale: 'zh-CN' as const, width: 1180.4, height: 760 };

describe('buildChartView', () => {
  it('names the visible window in the chart time convention, sized to the canvas', () => {
    const bars = minuteSessions(['2026-10-29', '2026-10-30']);
    const view = buildChartView({ ...base, interval: '1min', barTimes: bars, range: { from: 960, to: 1919 } });
    expect(view).toEqual({
      symbol: 'GOOG', timeframe: '1min', theme: 'dark', locale: 'zh-CN',
      from: '2026-10-30T04:00', to: '2026-10-30T19:59', width: 1180, height: 760,
    });
  });

  it('extrapolates a right edge in the future room over the next session, not the night', () => {
    const bars = minuteSessions(['2026-10-29', '2026-10-30']);
    // Three slots past Friday 19:59 is Monday 04:02.
    const view = buildChartView({ ...base, interval: '1min', barTimes: bars, range: { from: 1800, to: bars.length - 1 + 3 } });
    expect(view?.to).toBe('2026-11-02T04:02');
  });

  it('round-trips through logicalRangeForTimes', () => {
    const bars = weekdayBars(300);
    const range = { from: 120, to: bars.length - 1 + 12 };
    const view = buildChartView({ ...base, interval: '1day', barTimes: bars, range })!;
    expect(view.from).toMatch(/^\d{4}-\d{2}-\d{2}$/);
    expect(logicalRangeForTimes(bars, '1day', view.from, view.to)).toEqual(range);

    const minutes = minuteSessions(['2026-10-29', '2026-10-30']);
    const intraday = { from: 37, to: minutes.length - 1 + 40 };
    const v2 = buildChartView({ ...base, interval: '1min', barTimes: minutes, range: intraday })!;
    expect(logicalRangeForTimes(minutes, '1min', v2.from, v2.to)).toEqual(intraday);
  });

  it('rounds fractional edges to the nearest bar', () => {
    const bars = weekdayBars(50);
    const view = buildChartView({ ...base, interval: '1day', barTimes: bars, range: { from: 9.6, to: 20.2 } })!;
    expect(logicalRangeForTimes(bars, '1day', view.from, view.to)).toEqual({ from: 10, to: 20 });
  });

  it('keys the timeframe as layers do', () => {
    const bars = weekdayBars(50);
    const view = buildChartView({ ...base, interval: '1day', barTimes: bars, range: { from: 0, to: 49 } })!;
    expect(view.timeframe).toBe('1day');
  });

  it('is null with nothing to describe', () => {
    const bars = weekdayBars(50);
    expect(buildChartView({ ...base, interval: '1day', barTimes: [], range: { from: 0, to: 10 } })).toBeNull();
    expect(buildChartView({ ...base, interval: '1day', barTimes: bars, range: null })).toBeNull();
    expect(buildChartView({ ...base, width: 0, interval: '1day', barTimes: bars, range: { from: 0, to: 10 } })).toBeNull();
  });
});

describe('logicalRangeForTimes', () => {
  it('is null for unreadable times or no bars', () => {
    const bars = weekdayBars(50);
    expect(logicalRangeForTimes(bars, '1day', 'nope', '2026-10-30')).toBeNull();
    expect(logicalRangeForTimes([], '1day', '2026-10-01', '2026-10-30')).toBeNull();
  });
});
