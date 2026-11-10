// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { sparkGeometry } from '@/lib/sparkline';
import { regularHoursForSymbol, US_MARKET_HOURS } from '../exchanges';
import { latestSession, sessionLine } from '../intradaySession';

/** A chart bar at a venue wall-clock time (fake-UTC seconds, as rowsToChartBars makes them). */
const bar = (day: string, hhmm: string, close: number) => ({ time: Date.parse(`${day}T${hhmm}:00Z`) / 1000, close });

const US = US_MARKET_HOURS;
const CN = regularHoursForSymbol('600519.SH');

describe('latestSession', () => {
  it("takes today's regular-hours bars, and the close before them from the session before", () => {
    const bars = [
      bar('2026-11-05', '15:55', 100),
      bar('2026-11-05', '16:00', 101), // the closing print's bar counts
      bar('2026-11-05', '17:00', 99), // after hours does not
      bar('2026-11-06', '08:00', 98), // nor before the open
      bar('2026-11-06', '09:30', 102),
      bar('2026-11-06', '10:00', 103),
    ];
    const session = latestSession(bars, US);
    expect(session?.date).toBe('2026-11-06');
    expect(session?.bars.map((b) => b.close)).toEqual([102, 103]);
    expect(session?.previousClose).toBe(101);
  });

  it('is the last session on a day the market has not opened', () => {
    // Monday before the open: only pre-market bars today, so Friday is the session.
    const bars = [
      bar('2026-11-05', '16:00', 90),
      bar('2026-11-06', '09:30', 95),
      bar('2026-11-06', '16:00', 96),
      bar('2026-11-09', '07:00', 97),
    ];
    const session = latestSession(bars, US);
    expect(session?.date).toBe('2026-11-06');
    expect(session?.bars.map((b) => b.close)).toEqual([95, 96]);
    expect(session?.previousClose).toBe(90);
  });

  it("falls back to the last day's bars when none are in regular hours", () => {
    const bars = [bar('2026-11-06', '03:00', 20), bar('2026-11-07', '02:00', 21), bar('2026-11-07', '04:00', 22)];
    const session = latestSession(bars, US);
    expect(session?.bars.map((b) => b.close)).toEqual([21, 22]);
    expect(session?.previousClose).toBeNull();
  });

  it('is null for no bars', () => {
    expect(latestSession([], US)).toBeNull();
  });
});

describe('sessionLine', () => {
  it('places a session still trading across its hours, and only as far as it has reached', () => {
    const line = sessionLine([bar('2026-11-06', '09:30', 10), bar('2026-11-06', '12:45', 11)], US);
    expect(line?.at).toEqual([0, 0.5]);
    expect(line?.values).toEqual([10, 11]);
    expect([line?.opens, line?.closes]).toEqual([570, 960]);
  });

  it('gives a lunch break no width', () => {
    const line = sessionLine(
      [bar('2026-11-06', '09:30', 1), bar('2026-11-06', '11:30', 2), bar('2026-11-06', '13:00', 3), bar('2026-11-06', '15:00', 4)],
      CN,
    );
    expect(line?.at).toEqual([0, 0.5, 0.5, 1]);
    expect([line?.opens, line?.closes]).toEqual([570, 900]);
  });

  it('spans first bar to last on a venue with no hours listed', () => {
    const line = sessionLine([bar('2026-11-06', '08:00', 1), bar('2026-11-06', '12:00', 2), bar('2026-11-06', '16:30', 3)], null);
    expect(line?.at[0]).toBe(0);
    expect(line?.at[2]).toBe(1);
    expect([line?.opens, line?.closes]).toEqual([480, 990]);
  });

  it("keeps the session before while today's has one price", () => {
    const bars = [
      bar('2026-11-05', '16:00', 90),
      bar('2026-11-06', '09:30', 95),
      bar('2026-11-06', '16:00', 96),
      bar('2026-11-09', '09:30', 97), // the first five minutes of Monday
    ];
    const line = sessionLine(bars, US);
    expect(line?.date).toBe('2026-11-06');
    expect(line?.values).toEqual([95, 96]);
    expect(line?.previousClose).toBe(90);
  });

  it('is null with nothing to draw between', () => {
    expect(sessionLine([], US)).toBeNull();
    expect(sessionLine([bar('2026-11-06', '09:30', 10)], US)).toBeNull();
    expect(sessionLine([bar('2026-11-06', '09:30', 10), bar('2026-11-06', '09:35', 0)], US)).toBeNull();
  });
});

describe('sparkGeometry', () => {
  it('draws the values where they sit, with the baseline inside the range', () => {
    const shape = sparkGeometry([10, 12], { width: 100, height: 40, pad: 0, at: [0, 0.5], baseline: 8 });
    // Range 8–12: 10 is halfway, 12 is the top, the baseline the bottom.
    expect(shape?.line).toBe('0.0,20.0 50.0,0.0');
    expect(shape?.baselineY).toBe(40);
    expect(shape?.area).toBe('0.0,40 0.0,20.0 50.0,0.0 50.0,40');
    expect(shape?.last).toEqual({ x: 50, y: 0 });
  });

  it('spreads values evenly without positions, and draws nothing for one', () => {
    expect(sparkGeometry([1, 2, 3], { width: 10, height: 10, pad: 0 })?.line).toBe('0.0,10.0 5.0,5.0 10.0,0.0');
    expect(sparkGeometry([1], { width: 10, height: 10 })).toBeNull();
  });

  it('keeps the pad clear on every side, under the fill too', () => {
    const shape = sparkGeometry([1, 3], { width: 20, height: 10, pad: 2 });
    expect(shape?.line).toBe('2.0,8.0 18.0,2.0');
    expect(shape?.area).toBe('2.0,10 2.0,8.0 18.0,2.0 18.0,10');
    expect(shape?.last).toEqual({ x: 18, y: 2 });
  });
});
