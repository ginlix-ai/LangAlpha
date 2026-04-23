import { describe, it, expect } from 'vitest';
import { computeExtendedHoursRegions, getExtendedHoursType } from '../chartConstants';

// Timestamps use the "ET stored as UTC" convention of getExtendedHoursType:
// seconds are interpreted as if the UTC wall clock were ET wall clock.
//   mins < 570  (< 09:30)  → 'pre'
//   mins >= 960 (>= 16:00) → 'post'
const DAY1_00_00 = 1704153600; // 2024-01-02 00:00:00 UTC
const DAY2_00_00 = DAY1_00_00 + 86400;

const preAt = (dayBase: number, hours: number, mins: number) =>
  dayBase + hours * 3600 + mins * 60;

describe('computeExtendedHoursRegions', () => {
  it('returns empty for empty or null data', () => {
    expect(computeExtendedHoursRegions([])).toEqual([]);
    // @ts-expect-error — intentional null guard coverage
    expect(computeExtendedHoursRegions(null)).toEqual([]);
  });

  it('returns empty when no bars are in extended hours', () => {
    const data = [
      { time: preAt(DAY1_00_00, 10, 0) }, // 10:00 regular
      { time: preAt(DAY1_00_00, 12, 0) }, // 12:00 regular
      { time: preAt(DAY1_00_00, 15, 0) }, // 15:00 regular
    ];
    expect(computeExtendedHoursRegions(data)).toEqual([]);
  });

  it('collapses contiguous pre-market bars into one region', () => {
    const data = [
      { time: preAt(DAY1_00_00, 8, 0) },
      { time: preAt(DAY1_00_00, 8, 30) },
      { time: preAt(DAY1_00_00, 9, 0) },
      { time: preAt(DAY1_00_00, 9, 15) },
    ];
    const regions = computeExtendedHoursRegions(data);
    expect(regions).toEqual([
      { start: preAt(DAY1_00_00, 8, 0), end: preAt(DAY1_00_00, 9, 15), type: 'pre' },
    ]);
  });

  it('closes pre region when a regular-session bar appears', () => {
    const data = [
      { time: preAt(DAY1_00_00, 9, 0) },  // pre
      { time: preAt(DAY1_00_00, 9, 15) }, // pre
      { time: preAt(DAY1_00_00, 10, 0) }, // regular — closes pre
      { time: preAt(DAY1_00_00, 16, 0) }, // post
      { time: preAt(DAY1_00_00, 16, 30) }, // post
    ];
    const regions = computeExtendedHoursRegions(data);
    expect(regions).toEqual([
      { start: preAt(DAY1_00_00, 9, 0), end: preAt(DAY1_00_00, 9, 15), type: 'pre' },
      { start: preAt(DAY1_00_00, 16, 0), end: preAt(DAY1_00_00, 16, 30), type: 'post' },
    ]);
  });

  it('emits separate pre and post regions when type changes across a gap', () => {
    // Post bars on day 1 followed by pre bars on day 2 (within 2h? no — actual
    // gap is much larger). This also exercises day-boundary closing.
    const data = [
      { time: preAt(DAY1_00_00, 16, 0) }, // post
      { time: preAt(DAY1_00_00, 17, 0) }, // post
      { time: preAt(DAY2_00_00, 4, 0) },  // pre next day — gap > 2h
    ];
    const regions = computeExtendedHoursRegions(data);
    expect(regions).toEqual([
      { start: preAt(DAY1_00_00, 16, 0), end: preAt(DAY1_00_00, 17, 0), type: 'post' },
      { start: preAt(DAY2_00_00, 4, 0), end: preAt(DAY2_00_00, 4, 0), type: 'pre' },
    ]);
  });

  // --- Regression: day-boundary gap fix (EXT_REGION_MAX_GAP_SEC) ---

  it('closes pre-market region across a day-boundary gap', () => {
    // Two days of pre-market bars. Before the fix these merged into one
    // region spanning ~24h because both sides are pre and no regular-session
    // bar appeared between them.
    const data = [
      { time: preAt(DAY1_00_00, 8, 0) },
      { time: preAt(DAY1_00_00, 9, 0) },
      // ~23h gap — no bars between (weekend, closed market, or data gap)
      { time: preAt(DAY2_00_00, 8, 0) },
      { time: preAt(DAY2_00_00, 9, 0) },
    ];
    const regions = computeExtendedHoursRegions(data);
    expect(regions).toHaveLength(2);
    expect(regions[0]).toEqual({
      start: preAt(DAY1_00_00, 8, 0),
      end: preAt(DAY1_00_00, 9, 0),
      type: 'pre',
    });
    expect(regions[1]).toEqual({
      start: preAt(DAY2_00_00, 8, 0),
      end: preAt(DAY2_00_00, 9, 0),
      type: 'pre',
    });
  });

  it('preserves a single region across a small (< 2h) data gap within the pre-market window', () => {
    // 30-minute data gap inside one pre-market session should NOT split.
    const data = [
      { time: preAt(DAY1_00_00, 7, 0) },
      { time: preAt(DAY1_00_00, 7, 30) },
      // 30-min data gap
      { time: preAt(DAY1_00_00, 8, 0) },
      { time: preAt(DAY1_00_00, 8, 30) },
    ];
    const regions = computeExtendedHoursRegions(data);
    expect(regions).toEqual([
      { start: preAt(DAY1_00_00, 7, 0), end: preAt(DAY1_00_00, 8, 30), type: 'pre' },
    ]);
  });

  it('closes region when gap is 2h + 1s (boundary is exclusive)', () => {
    const GAP = 2 * 60 * 60;
    const data = [
      { time: preAt(DAY1_00_00, 5, 0) },
      { time: preAt(DAY1_00_00, 5, 0) + GAP + 1 },
    ];
    expect(computeExtendedHoursRegions(data)).toHaveLength(2);
  });

  it('preserves region when gap is exactly 2h (boundary is exclusive)', () => {
    // Pins the contract that the threshold is `>`, not `>=`. Flipping the
    // operator would split real sparse pre-market data with a single 2h gap.
    const GAP = 2 * 60 * 60;
    const data = [
      { time: preAt(DAY1_00_00, 5, 0) },
      { time: preAt(DAY1_00_00, 5, 0) + GAP },
    ];
    expect(computeExtendedHoursRegions(data)).toHaveLength(1);
  });
});

describe('getExtendedHoursType', () => {
  it('classifies pre-market (< 09:30)', () => {
    expect(getExtendedHoursType(preAt(DAY1_00_00, 9, 0))).toBe('pre');
    expect(getExtendedHoursType(preAt(DAY1_00_00, 9, 29))).toBe('pre');
  });

  it('classifies post-market (>= 16:00)', () => {
    expect(getExtendedHoursType(preAt(DAY1_00_00, 16, 0))).toBe('post');
    expect(getExtendedHoursType(preAt(DAY1_00_00, 20, 0))).toBe('post');
  });

  it('returns null during regular session', () => {
    expect(getExtendedHoursType(preAt(DAY1_00_00, 9, 30))).toBeNull();
    expect(getExtendedHoursType(preAt(DAY1_00_00, 12, 0))).toBeNull();
    expect(getExtendedHoursType(preAt(DAY1_00_00, 15, 59))).toBeNull();
  });
});
