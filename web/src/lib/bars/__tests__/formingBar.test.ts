import { describe, it, expect } from 'vitest';
import { applyQuoteToDailyBar, foldMinuteBar, isSettledDailyHead } from '../formingBar';
import type { ChartBar } from '../marketProtocol';

const bar = (b: Partial<ChartBar> & { time: number }): ChartBar => ({
  open: 0,
  high: 0,
  low: 0,
  close: 0,
  volume: 0,
  ...b,
});

describe('foldMinuteBar', () => {
  // A 5-minute (300s) coarse series with one forming bucket anchored at t=1000.
  const INTERVAL = 300;
  const base = (): ChartBar[] => [
    bar({ time: 700, open: 5, high: 8, low: 4, close: 7, volume: 100 }),
    bar({ time: 1000, open: 10, high: 12, low: 9, close: 11, volume: 50 }),
  ];

  it('folds a minute bar INSIDE the forming bucket into the last bar', () => {
    const minute = bar({ time: 1120, open: 11, high: 15, low: 8, close: 14, volume: 30 });
    const out = foldMinuteBar(base(), minute, INTERVAL);
    expect(out).toHaveLength(2);
    const head = out[out.length - 1];
    expect(head.time).toBe(1000); // same bucket
    expect(head.open).toBe(10); // open preserved
    expect(head.high).toBe(15); // max(12, 15)
    expect(head.low).toBe(8); // min(9, 8)
    expect(head.close).toBe(14); // = minute.close
    expect(head.volume).toBe(80); // 50 + 30 accumulated
  });

  it('accumulates volume across multiple minute folds', () => {
    let series = base();
    series = foldMinuteBar(series, bar({ time: 1060, close: 11, high: 11, low: 11, volume: 10 }), INTERVAL);
    series = foldMinuteBar(series, bar({ time: 1120, close: 12, high: 12, low: 12, volume: 20 }), INTERVAL);
    series = foldMinuteBar(series, bar({ time: 1180, close: 13, high: 13, low: 13, volume: 5 }), INTERVAL);
    expect(series[series.length - 1].volume).toBe(50 + 10 + 20 + 5);
    expect(series[series.length - 1].close).toBe(13);
  });

  it('opens a NEW forming bar at the aligned anchor on rollover', () => {
    // Bucket end is 1300; a minute at 1305 belongs to the next bucket [1300,1600).
    const minute = bar({ time: 1305, open: 14, high: 16, low: 13, close: 15, volume: 40 });
    const out = foldMinuteBar(base(), minute, INTERVAL);
    expect(out).toHaveLength(3);
    const head = out[out.length - 1];
    expect(head.time).toBe(1300); // lastBar.time + 1*interval
    expect(head.open).toBe(14);
    expect(head.high).toBe(16);
    expect(head.low).toBe(13);
    expect(head.close).toBe(15);
    expect(head.volume).toBe(40); // seeded, not accumulated
  });

  it('anchors a multi-bucket-ahead rollover to the correct aligned bucket', () => {
    // A minute at 1650 is two buckets past t=1000: k = floor(650/300) = 2 → 1600.
    const minute = bar({ time: 1650, open: 20, high: 22, low: 19, close: 21, volume: 7 });
    const out = foldMinuteBar(base(), minute, INTERVAL);
    expect(out[out.length - 1].time).toBe(1600);
  });

  it('treats a bar exactly at the bucket end as a new bucket', () => {
    const minute = bar({ time: 1300, open: 14, high: 14, low: 14, close: 14, volume: 1 });
    const out = foldMinuteBar(base(), minute, INTERVAL);
    expect(out).toHaveLength(3);
    expect(out[out.length - 1].time).toBe(1300);
  });

  it('ignores a late bar belonging to an already-closed bucket', () => {
    const minute = bar({ time: 800, close: 99, high: 99, low: 99, volume: 5 });
    const out = foldMinuteBar(base(), minute, INTERVAL);
    expect(out).toEqual(base());
  });

  it('ignores a zero-filled minute bar entirely (no positive close)', () => {
    // Provider zero-fill rows carry no trade; folding one would wick the
    // candle to zero until the next REST reconcile.
    const zeroed = bar({ time: 1120, open: 0, high: 0, low: 0, close: 0, volume: 0 });
    expect(foldMinuteBar(base(), zeroed, INTERVAL)).toEqual(base());
  });

  it('ignores non-positive high/low when accumulating into the forming bar', () => {
    const minute = bar({ time: 1120, open: 11, high: 0, low: 0, close: 14, volume: 30 });
    const head = foldMinuteBar(base(), minute, INTERVAL)[1];
    expect(head.high).toBe(12); // prior high kept — a zero high never wins
    expect(head.low).toBe(9); // prior low kept — a zero low never wicks
    expect(head.close).toBe(14); // reconciled by the next REST poll (≤60s)
  });

  it('falls back to close for zeroed OHL on a rollover bar', () => {
    const minute = bar({ time: 1300, open: 0, high: 0, low: 0, close: 14, volume: 30 });
    const head = foldMinuteBar(base(), minute, INTERVAL)[2];
    expect(head.time).toBe(1300);
    expect(head.open).toBe(14);
    expect(head.high).toBe(14);
    expect(head.low).toBe(14);
  });

  it('does not mutate the input array or bars', () => {
    const input = base();
    const snapshot = JSON.parse(JSON.stringify(input));
    foldMinuteBar(input, bar({ time: 1120, close: 14, high: 15, low: 8, volume: 30 }), INTERVAL);
    expect(input).toEqual(snapshot);
  });

  it('is a no-op on empty bars', () => {
    const out = foldMinuteBar([], bar({ time: 1000, close: 1 }), INTERVAL);
    expect(out).toEqual([]);
  });

  it('is a no-op on non-positive interval', () => {
    const input = base();
    expect(foldMinuteBar(input, bar({ time: 1120, close: 14 }), 0)).toBe(input);
  });
});

describe('applyQuoteToDailyBar', () => {
  const daily = (): ChartBar[] => [
    bar({ time: 86400, open: 100, high: 110, low: 95, close: 105, volume: 1000 }),
    bar({ time: 172800, open: 106, high: 112, low: 104, close: 108, volume: 500 }),
  ];

  it('updates close/high/low/volume on the last daily bar', () => {
    const out = applyQuoteToDailyBar(daily(), { price: 115, high: 116, low: 103, volume: 800 });
    expect(out).toHaveLength(2);
    const head = out[out.length - 1];
    expect(head.time).toBe(172800);
    expect(head.open).toBe(106); // preserved
    expect(head.close).toBe(115); // = quote.price
    expect(head.high).toBe(116); // max(112, 116)
    expect(head.low).toBe(103); // min(104, 103)
    expect(head.volume).toBe(800);
  });

  it('keeps existing high/low when the quote does not exceed them', () => {
    const out = applyQuoteToDailyBar(daily(), { price: 109, high: 109, low: 105 });
    const head = out[out.length - 1];
    expect(head.high).toBe(112); // quote.high 109 < 112 → keep
    expect(head.low).toBe(104); // quote.low 105 > 104 → keep
    expect(head.close).toBe(109);
    expect(head.volume).toBe(500); // volume absent → preserved
  });

  it('never creates a new daily bar (update-only)', () => {
    const out = applyQuoteToDailyBar(daily(), { price: 999 });
    expect(out).toHaveLength(2);
  });

  it('is a no-op when the quote lacks a price', () => {
    const input = daily();
    expect(applyQuoteToDailyBar(input, { high: 200, low: 1 })).toBe(input);
    expect(applyQuoteToDailyBar(input, { price: null })).toBe(input);
    expect(applyQuoteToDailyBar(input, null)).toBe(input);
    expect(applyQuoteToDailyBar(input, undefined)).toBe(input);
  });

  it('is a no-op on empty bars', () => {
    expect(applyQuoteToDailyBar([], { price: 100 })).toEqual([]);
  });

  it('is a no-op on a non-positive price (quote-unavailable rows report 0)', () => {
    const input = daily();
    expect(applyQuoteToDailyBar(input, { price: 0 })).toBe(input);
    expect(applyQuoteToDailyBar(input, { price: -1 })).toBe(input);
  });

  it('ignores zeroed high/low/volume (pre-open snapshot day-aggregates)', () => {
    const out = applyQuoteToDailyBar(daily(), { price: 107, high: 0, low: 0, volume: 0 });
    const head = out[out.length - 1];
    expect(head.close).toBe(107); // price still folds
    expect(head.high).toBe(112); // 0 high ignored
    expect(head.low).toBe(104); // 0 low ignored — must NOT crater to 0
    expect(head.volume).toBe(500); // 0 volume ignored — settled volume kept
  });

  it('does not mutate the input array or bars', () => {
    const input = daily();
    const snapshot = JSON.parse(JSON.stringify(input));
    applyQuoteToDailyBar(input, { price: 115, high: 116, low: 103, volume: 800 });
    expect(input).toEqual(snapshot);
  });
});

describe('isSettledDailyHead', () => {
  const ET = 'America/New_York';
  // Daily chart times encode the venue-local date as fake UTC midnight.
  const dayBar = (isoDate: string) => ({ time: Date.parse(`${isoDate}T00:00:00Z`) / 1000 });

  it('true during pre-market: head bar is still yesterday on the venue clock', () => {
    const preMarket = new Date('2024-03-12T11:50:00Z'); // 07:50 ET Tuesday
    expect(isSettledDailyHead([dayBar('2024-03-11')], ET, preMarket)).toBe(true);
  });

  it('false during the session: head bar is the venue-today forming bar', () => {
    const midSession = new Date('2024-03-12T15:00:00Z'); // 11:00 ET Tuesday
    expect(isSettledDailyHead([dayBar('2024-03-12')], ET, midSession)).toBe(false);
  });

  it('true on weekends: head bar is Friday, now is Saturday', () => {
    const saturday = new Date('2024-03-16T16:00:00Z');
    expect(isSettledDailyHead([dayBar('2024-03-15')], ET, saturday)).toBe(true);
  });

  it('uses the venue clock, not the viewer clock', () => {
    // 21:30 ET Tuesday == 09:30 HKT Wednesday: an HK head bar dated Wednesday
    // is live on the venue clock while an ET read would still say Tuesday.
    const usEvening = new Date('2024-03-13T01:30:00Z');
    expect(isSettledDailyHead([dayBar('2024-03-13')], 'Asia/Hong_Kong', usEvening)).toBe(false);
    expect(isSettledDailyHead([dayBar('2024-03-13')], ET, usEvening)).toBe(true);
  });

  it('only the head bar matters', () => {
    const midSession = new Date('2024-03-12T15:00:00Z');
    const bars = [dayBar('2024-03-08'), dayBar('2024-03-11'), dayBar('2024-03-12')];
    expect(isSettledDailyHead(bars, ET, midSession)).toBe(false);
  });

  it('false for an empty or missing series', () => {
    expect(isSettledDailyHead([], ET)).toBe(false);
    expect(isSettledDailyHead(null, ET)).toBe(false);
    expect(isSettledDailyHead(undefined, ET)).toBe(false);
  });
});
