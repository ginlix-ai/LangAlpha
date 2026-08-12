import { describe, it, expect } from 'vitest';
import { RANGE_PRESETS, rangeStartChartSec } from '../rangePresets';
import { INTERVALS } from '../chartConstants';

const DAY = 86_400;

describe('RANGE_PRESETS', () => {
  it('every preset (and fallback) charts with a known interval key', () => {
    const known = new Set(INTERVALS.map(({ key }) => key));
    for (const p of RANGE_PRESETS) {
      expect(known.has(p.interval), `${p.key} → ${p.interval}`).toBe(true);
      if (p.fallback) expect(known.has(p.fallback), `${p.key} fallback`).toBe(true);
    }
  });

  it('covers the TradingView range set in order', () => {
    expect(RANGE_PRESETS.map((p) => p.key)).toEqual(
      ['1D', '5D', '1M', '3M', '6M', 'YTD', '1Y', '5Y', 'All'],
    );
  });
});

describe('rangeStartChartSec', () => {
  // Chart times are venue wall clock encoded as fake UTC.
  const lastBar = Date.UTC(2025, 6, 3, 14, 30) / 1000; // venue 2025-07-03 14:30

  it('1D floors to the last session\'s venue midnight', () => {
    expect(rangeStartChartSec('1D', lastBar)).toBe(Date.UTC(2025, 6, 3) / 1000);
  });

  it('YTD anchors at venue Jan 1 of the last bar\'s year', () => {
    expect(rangeStartChartSec('YTD', lastBar)).toBe(Date.UTC(2025, 0, 1) / 1000);
  });

  it('5D spans 7 calendar days so ~5 trading days survive a weekend', () => {
    expect(rangeStartChartSec('5D', lastBar)).toBe(lastBar - 7 * DAY);
  });

  it('fixed spans subtract calendar days from the last bar', () => {
    expect(rangeStartChartSec('1M', lastBar)).toBe(lastBar - 30 * DAY);
    expect(rangeStartChartSec('1Y', lastBar)).toBe(lastBar - 365 * DAY);
  });

  it('All returns null (fitContent) and unknown keys return null', () => {
    expect(rangeStartChartSec('All', lastBar)).toBeNull();
    expect(rangeStartChartSec('bogus', lastBar)).toBeNull();
  });
});
