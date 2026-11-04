import { describe, it, expect, vi, beforeEach } from 'vitest';
import { renderHook } from '@testing-library/react';
import type { ISeriesApi } from 'lightweight-charts';

import type { ChartDataPoint } from '@/types/market';
import { getChartTheme } from '../../utils/chartConstants';

const setMarkers = vi.fn();
vi.mock('lightweight-charts', async (orig) => ({
  ...(await orig<object>()),
  createSeriesMarkers: () => ({ setMarkers }),
}));

import { useChartOverlays } from '../useChartOverlays';

const utc = (y: number, m: number, d: number, h = 0, min = 0): number => Date.UTC(y, m - 1, d, h, min) / 1000;
const bar = (time: number): ChartDataPoint => ({ time, open: 1, high: 1, low: 1, close: 1, volume: 1 });

function fiveMinuteSession(y: number, m: number, d: number): ChartDataPoint[] {
  const bars: ChartDataPoint[] = [];
  for (let min = 9 * 60 + 30; min < 16 * 60; min += 5) bars.push(bar(utc(y, m, d, Math.floor(min / 60), min % 60)));
  return bars;
}

const FRI_MON_5M = [...fiveMinuteSession(2026, 10, 30), ...fiveMinuteSession(2026, 11, 2)];

function earningsMarkers(chartData: ChartDataPoint[], earnings: NonNullable<Parameters<typeof useChartOverlays>[2]>) {
  setMarkers.mockClear();
  const { unmount } = renderHook(() =>
    useChartOverlays({ current: {} as ISeriesApi<'Candlestick'> }, chartData, earnings, null, { earnings: true }, 'AAPL'),
  );
  const markers = setMarkers.mock.calls[setMarkers.mock.calls.length - 1][0] as Array<{ time: number; position: string; shape: string; color: string }>;
  unmount();
  return markers;
}

describe('useChartOverlays earnings markers', () => {
  const { upColor, downColor, text: neutral } = getChartTheme('dark');

  beforeEach(() => setMarkers.mockClear());

  it('draws a beat as a green up arrow below the bar and a miss as a red down arrow above it', () => {
    const [beat] = earningsMarkers(FRI_MON_5M, [{ date: '2026-11-02', epsActual: 2.05, epsEstimate: 1.99 }]);
    const [miss] = earningsMarkers(FRI_MON_5M, [{ date: '2026-11-02', epsActual: 1.9, epsEstimate: 1.99 }]);
    expect(beat).toMatchObject({ shape: 'arrowUp', position: 'belowBar', color: upColor });
    expect(miss).toMatchObject({ shape: 'arrowDown', position: 'aboveBar', color: downColor });
  });

  it('draws a neutral circle, not a beat, when the estimate or the actual is missing', () => {
    const rows = earningsMarkers(FRI_MON_5M, [
      { date: '2026-11-02', epsActual: 2.05, epsEstimate: null },
      { date: '2026-11-02', epsActual: null, epsEstimate: 1.99 },
    ]);
    expect(rows).toHaveLength(2);
    for (const m of rows) expect(m).toMatchObject({ shape: 'circle', position: 'aboveBar', color: neutral });
  });

  it('puts a weekend date on the next session open, and a Monday date on Monday, not on Friday close', () => {
    const entry = { epsActual: 2, epsEstimate: 1 };
    const [sat] = earningsMarkers(FRI_MON_5M, [{ ...entry, date: '2026-10-31' }]);
    const [mon] = earningsMarkers(FRI_MON_5M, [{ ...entry, date: '2026-11-02' }]);
    expect(sat.time).toBe(utc(2026, 11, 2, 9, 30));
    expect(mon.time).toBe(utc(2026, 11, 2, 9, 30));
  });

  it('draws no marker for a date before the first bar or after the last bar', () => {
    const entry = { epsActual: 2, epsEstimate: 1 };
    expect(earningsMarkers(FRI_MON_5M, [{ ...entry, date: '2026-10-29' }])).toEqual([]);
    expect(earningsMarkers(FRI_MON_5M, [{ ...entry, date: '2026-11-03' }])).toEqual([]);
  });
});
