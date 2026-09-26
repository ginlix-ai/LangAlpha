import { afterEach, beforeEach, describe, it, expect, vi } from 'vitest';
import { formatUtcOffset, utcOffsetMinutes } from '../timezones';

const label = (tz: string, at: Date) => formatUtcOffset(utcOffsetMinutes(tz, at));

describe('formatUtcOffset', () => {
  const summer = new Date(Date.UTC(2025, 6, 2, 12, 0));
  const winter = new Date(Date.UTC(2025, 0, 15, 12, 0));

  it('labels whole-hour offsets without minutes', () => {
    expect(label('Asia/Hong_Kong', summer)).toBe('UTC+8');
    expect(label('Asia/Hong_Kong', winter)).toBe('UTC+8'); // no DST
  });

  it('is DST-aware, with a true minus sign', () => {
    expect(label('America/New_York', summer)).toBe('UTC−4');
    expect(label('America/New_York', winter)).toBe('UTC−5');
    expect(label('Europe/London', summer)).toBe('UTC+1');
  });

  it('reads a zero offset as plain UTC', () => {
    expect(label('Europe/London', winter)).toBe('UTC');
    expect(label('UTC', summer)).toBe('UTC');
  });

  it('keeps minutes for half-hour zones', () => {
    expect(label('Asia/Kolkata', summer)).toBe('UTC+5:30');
    expect(formatUtcOffset(-210)).toBe('UTC−3:30');
  });
});

describe('on an engine without the named offset styles', () => {
  const Real = Intl.DateTimeFormat;

  beforeEach(() => {
    vi.resetModules();
    // What Chrome before 95 and Safari before 15.4 do with these two styles.
    Intl.DateTimeFormat = function (locales?: string | string[], options?: Intl.DateTimeFormatOptions) {
      const style = options?.timeZoneName;
      if (style === 'longOffset' || style === 'shortOffset') {
        throw new RangeError(`Value ${style} out of range for Intl.DateTimeFormat options property timeZoneName`);
      }
      return new Real(locales, options);
    } as unknown as typeof Intl.DateTimeFormat;
  });

  afterEach(() => {
    Intl.DateTimeFormat = Real;
  });

  it('still knows zones, detects the device zone and reads offsets', async () => {
    const { detectedTimezone, isKnownTimezone } = await import('../deviceTimezone');
    const { utcOffsetMinutes } = await import('../timezones');
    expect(isKnownTimezone('Asia/Kolkata')).toBe(true);
    expect(isKnownTimezone('Mars/Olympus_Mons')).toBe(false);
    expect(detectedTimezone()).not.toBeNull();
    expect(utcOffsetMinutes('Asia/Kolkata', new Date(Date.UTC(2025, 6, 2, 12, 0)))).toBe(330);
    expect(utcOffsetMinutes('America/New_York', new Date(Date.UTC(2025, 0, 15, 12, 0)))).toBe(-300);
  });
});
