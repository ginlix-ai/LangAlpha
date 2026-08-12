import { describe, it, expect } from 'vitest';
import { chartSecToDateStr, cn, dateStrInTz, utcMsToChartSec, utcMsToETDate, utcMsToETTime, utcOffsetLabel } from '../utils';

describe('cn', () => {
  it('merges simple class names', () => {
    expect(cn('foo', 'bar')).toBe('foo bar');
  });

  it('handles conditional classes via clsx syntax', () => {
    // eslint-disable-next-line no-constant-binary-expression
    expect(cn('base', false && 'hidden', 'visible')).toBe('base visible');
  });

  it('merges tailwind conflicting classes (last wins)', () => {
    // twMerge should resolve conflicts
    const result = cn('px-2 py-1', 'px-4');
    expect(result).toContain('px-4');
    expect(result).not.toContain('px-2');
  });

  it('returns empty string for no inputs', () => {
    expect(cn()).toBe('');
  });

  it('handles undefined and null inputs gracefully', () => {
    expect(cn('a', undefined, null, 'b')).toBe('a b');
  });
});

describe('utcMsToChartSec', () => {
  it('returns 0 for null input', () => {
    expect(utcMsToChartSec(null)).toBe(0);
  });

  it('returns 0 for NaN input', () => {
    expect(utcMsToChartSec(NaN)).toBe(0);
  });

  it('returns 0 for undefined input', () => {
    expect(utcMsToChartSec(undefined)).toBe(0);
  });

  it('returns a numeric seconds value for a valid UTC ms timestamp', () => {
    // 2024-01-15 12:00:00 UTC = 1705320000000 ms
    const utcMs = 1705320000000;
    const result = utcMsToChartSec(utcMs);
    expect(typeof result).toBe('number');
    expect(result).toBeGreaterThan(0);
    // Result should be in seconds (roughly same order of magnitude as Unix seconds)
    expect(result).toBeGreaterThan(1_700_000_000);
    expect(result).toBeLessThan(1_800_000_000);
  });

  it('defaults to ET: 14:30 UTC in January reads back as 09:30 fake-UTC', () => {
    const sec = utcMsToChartSec(Date.UTC(2024, 0, 15, 14, 30));
    expect(new Date(sec * 1000).toISOString()).toBe('2024-01-15T09:30:00.000Z');
  });

  it('encodes the requested venue wall clock (HKT has no DST)', () => {
    // 01:30 UTC = 09:30 Asia/Hong_Kong, summer and winter alike.
    for (const month of [0, 6]) {
      const sec = utcMsToChartSec(Date.UTC(2024, month, 15, 1, 30), 'Asia/Hong_Kong');
      const d = new Date(sec * 1000);
      expect([d.getUTCHours(), d.getUTCMinutes()]).toEqual([9, 30]);
    }
  });

  it('respects the venue DST rules (London summer vs winter)', () => {
    // LSE opens 08:00 local: 07:00 UTC in July (BST), 08:00 UTC in January (GMT).
    const summer = utcMsToChartSec(Date.UTC(2024, 6, 15, 7, 0), 'Europe/London');
    const winter = utcMsToChartSec(Date.UTC(2024, 0, 15, 8, 0), 'Europe/London');
    expect(new Date(summer * 1000).getUTCHours()).toBe(8);
    expect(new Date(winter * 1000).getUTCHours()).toBe(8);
  });
});

describe('chartSecToDateStr', () => {
  it('decodes a chart time back to its venue date by reading in UTC', () => {
    // A daily bar at venue midnight (fake-UTC midnight) stays on its own date
    // regardless of the machine timezone running the test.
    const chartSec = Date.UTC(2025, 6, 3, 0, 0) / 1000;
    expect(chartSecToDateStr(chartSec)).toBe('2025-07-03');
  });
});

describe('dateStrInTz', () => {
  it('formats the same instant as different venue trading dates', () => {
    // 2025-07-02 23:00 UTC = already July 3 in Hong Kong, still July 2 in New York.
    const ms = Date.UTC(2025, 6, 2, 23, 0);
    expect(dateStrInTz(ms, 'Asia/Hong_Kong')).toBe('2025-07-03');
    expect(dateStrInTz(ms, 'America/New_York')).toBe('2025-07-02');
  });
});

describe('utcOffsetLabel', () => {
  const summer = new Date(Date.UTC(2025, 6, 2, 12, 0));
  const winter = new Date(Date.UTC(2025, 0, 15, 12, 0));

  it('labels whole-hour offsets without minutes', () => {
    expect(utcOffsetLabel('Asia/Hong_Kong', summer)).toBe('UTC+8');
    expect(utcOffsetLabel('Asia/Hong_Kong', winter)).toBe('UTC+8'); // no DST
  });

  it('is DST-aware', () => {
    expect(utcOffsetLabel('America/New_York', summer)).toBe('UTC-4');
    expect(utcOffsetLabel('America/New_York', winter)).toBe('UTC-5');
    expect(utcOffsetLabel('Europe/London', summer)).toBe('UTC+1');
    expect(utcOffsetLabel('Europe/London', winter)).toBe('UTC');
  });

  it('keeps minutes for half-hour zones', () => {
    expect(utcOffsetLabel('Asia/Kolkata', summer)).toBe('UTC+5:30');
  });
});

describe('utcMsToETDate', () => {
  it('converts a UTC timestamp to ET date string in YYYY-MM-DD format', () => {
    // 2024-06-15 20:00:00 UTC is 2024-06-15 16:00:00 ET (still same day)
    const utcMs = Date.UTC(2024, 5, 15, 20, 0, 0); // June 15, 2024 20:00 UTC
    const result = utcMsToETDate(utcMs);
    expect(result).toMatch(/^\d{4}-\d{2}-\d{2}$/);
  });
});

describe('utcMsToETTime', () => {
  it('returns a time string in HH:MM 24h format', () => {
    const utcMs = Date.UTC(2024, 5, 15, 18, 30, 0); // June 15, 2024 18:30 UTC
    const result = utcMsToETTime(utcMs);
    expect(result).toMatch(/^\d{2}:\d{2}$/);
  });
});
