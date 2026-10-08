import { describe, it, expect, beforeEach } from 'vitest';
import { createFormatter, createDateFormatter, compactNumber, compactNumberFixed2, fixed2, formatBytes, formatList, formatTimestamp, relativeTime, signedFixed2 } from '@/lib/format';

describe('createFormatter', () => {
  it('formats numbers in the given locale', () => {
    const fmt = createFormatter({ minimumFractionDigits: 2, maximumFractionDigits: 2 });
    expect(fmt(1234.5, 'en-US')).toBe('1,234.50');
  });

  it('keeps one Intl instance per locale, so switching back and forth stays right', () => {
    const fmt = createFormatter({ style: 'currency', currency: 'USD' });
    const en = fmt(1234.5, 'en-US');
    const zh = fmt(1234.5, 'zh-CN');
    expect(en).not.toBe(zh);
    expect(zh).toContain('1,234.50');
    expect(fmt(1234.5, 'en-US')).toBe(en);
  });

  it('percent formatting respects locale', () => {
    const fmt = createFormatter({ style: 'percent', minimumFractionDigits: 2 });
    expect(fmt(0.1234, 'en-US')).toBe('12.34%');
  });
});

describe('createDateFormatter', () => {
  it('formats dates in the given locale', () => {
    const fmt = createDateFormatter({ year: 'numeric', month: 'short', day: 'numeric' });
    expect(fmt(new Date('2026-04-25T00:00:00Z'), 'en-US')).toMatch(/Apr/);
  });

  it('follows the locale it is handed', () => {
    const fmt = createDateFormatter({ year: 'numeric', month: 'long' });
    const date = new Date('2026-04-25T00:00:00Z');
    expect(fmt(date, 'en-US')).not.toBe(fmt(date, 'zh-CN'));
  });
});

describe('formatTimestamp', () => {
  it('formats a timestamp in the given locale', () => {
    const ts = '2026-04-25T14:30:00Z';
    expect(formatTimestamp(ts, 'en-US')).toMatch(/2026/);
    expect(formatTimestamp(ts, 'en-US')).not.toBe(formatTimestamp(ts, 'zh-CN'));
  });

  // Intl throws on an invalid Date, so a malformed timestamp from the server
  // would take the whole view down with it.
  it('hands back a timestamp it cannot parse as written', () => {
    expect(formatTimestamp('sometime last week', 'en-US')).toBe('sometime last week');
  });
});

describe('compactNumber', () => {
  it('renders sub-thousand values verbatim (no suffix)', () => {
    expect(compactNumber(0, 'en-US')).toBe('0');
    expect(compactNumber(7, 'en-US')).toBe('7');
    expect(compactNumber(999, 'en-US')).toBe('999');
  });

  it('compacts 4-digit-and-up values with locale suffix', () => {
    expect(compactNumber(1000, 'en-US')).toMatch(/^1K$/);
    expect(compactNumber(1234, 'en-US')).toMatch(/^1\.2K$/);
    expect(compactNumber(5142, 'en-US')).toMatch(/^5\.1K$/);
    expect(compactNumber(1_500_000, 'en-US')).toMatch(/^1\.5M$/);
    expect(compactNumber(1_500_000, 'zh-CN')).toBe('150万');
  });
});

describe('quote-strip formatters', () => {
  it('fixed2 keeps two decimals and never groups a price', () => {
    expect(fixed2(1234.5, 'en-US')).toBe('1234.50');
    expect(fixed2(0, 'en-US')).toBe('0.00');
    expect(fixed2(-4, 'en-US')).toBe('-4.00');
  });

  it('signedFixed2 signs every move and leaves a flat one unsigned', () => {
    expect(signedFixed2(1.234, 'en-US')).toBe('+1.23');
    expect(signedFixed2(-4, 'en-US')).toBe('-4.00');
    expect(signedFixed2(0, 'en-US')).toBe('0.00');
    // A sub-cent dip rounds to zero and must not print as `-0.00`.
    expect(signedFixed2(-0.001, 'en-US')).toBe('0.00');
  });

  it('compactNumberFixed2 keeps two decimals so a volume column holds its width', () => {
    expect(compactNumberFixed2(999, 'en-US')).toBe('999.00');
    expect(compactNumberFixed2(1234, 'en-US')).toBe('1.23K');
    expect(compactNumberFixed2(1_500_000, 'en-US')).toBe('1.50M');
    expect(compactNumberFixed2(2_000_000_000, 'en-US')).toBe('2.00B');
  });
});

describe('relativeTime', () => {
  const DAY = 86_400_000;
  const NOW = Date.UTC(2026, 8, 28, 12);
  const fromNow = (ms: number) => NOW + ms;

  it('keeps a count compact', () => {
    expect(relativeTime(fromNow(3 * DAY + 60_000), 'en-US', NOW)).toBe('in 3d');
    expect(relativeTime(fromNow(-5 * 60_000 - 1_000), 'en-US', NOW)).toBe('5m ago');
    expect(relativeTime(fromNow(95 * DAY), 'en-US', NOW)).toBe('in 3mo');
  });

  it('spells out a phrase instead of clipping its words', () => {
    expect(relativeTime(fromNow(35 * DAY), 'en-US', NOW)).toBe('next month');
    expect(relativeTime(fromNow(-8 * DAY), 'en-US', NOW)).toBe('last week');
    expect(relativeTime(fromNow(400 * DAY), 'en-US', NOW)).toBe('next year');
    expect(relativeTime(fromNow(DAY + 60_000), 'en-US', NOW)).toBe('tomorrow');
  });

  it('reads naturally in Chinese', () => {
    expect(relativeTime(fromNow(35 * DAY), 'zh-CN', NOW)).toBe('下个月');
    expect(relativeTime(fromNow(95 * DAY), 'zh-CN', NOW)).toBe('3个月后');
  });

  it('measures from the time it is handed, not the clock', () => {
    const at = fromNow(-10 * 60_000);
    expect(relativeTime(at, 'en-US', NOW)).toBe('10m ago');
    expect(relativeTime(at, 'en-US', NOW + 50 * 60_000)).toBe('1h ago');
  });

  it('reads a missing or unparseable timestamp as absent', () => {
    expect(relativeTime(null, 'en-US', NOW)).toBe('');
    expect(relativeTime('not a date', 'en-US', NOW)).toBe('');
  });
});

// A locale that is briefly invalid (transient changeLanguage state, a broken
// cookie) makes Intl throw. The formatter must fall back to the host default,
// or every formatted widget on the dashboard would crash at once.
describe('safe fallback for invalid locales', () => {
  const BAD = 'xx-not-a-locale-😀';

  it('createFormatter falls back to host default when locale is rejected', () => {
    const fmt = createFormatter({ minimumFractionDigits: 2 });
    expect(() => fmt(1234.5, BAD)).not.toThrow();
    expect(fmt(1234.5, BAD)).toMatch(/1.?234/);
  });

  it('createDateFormatter falls back to host default when locale is rejected', () => {
    const fmt = createDateFormatter({ year: 'numeric', month: 'short' });
    expect(() => fmt(new Date('2026-04-25T00:00:00Z'), BAD)).not.toThrow();
  });
});

describe('formatBytes', () => {
  it('keeps whole bytes and one decimal under ten', () => {
    expect(formatBytes(0, 'en-US')).toBe('0 B');
    expect(formatBytes(512, 'en-US')).toBe('512 B');
    expect(formatBytes(1536, 'en-US')).toBe('1.5 KB');
    expect(formatBytes(40960, 'en-US')).toBe('40 KB');
    expect(formatBytes(6_549_825_126, 'en-US')).toBe('6.1 GB');
    expect(formatBytes(250 * 1024 ** 3, 'en-US')).toBe('250 GB');
    expect(formatBytes(2 * 1024 ** 4, 'en-US')).toBe('2 TB');
  });

  it('carries a value that would round up to 1024 into the next unit', () => {
    expect(formatBytes(1024 * 1024 - 10, 'en-US')).toBe('1 MB');
  });

  it('reads a negative or non-finite count as zero', () => {
    expect(formatBytes(-5, 'en-US')).toBe('0 B');
    expect(formatBytes(Number.NaN, 'en-US')).toBe('0 B');
  });

  it('formats the number in the given locale', () => {
    expect(formatBytes(1536, 'de-DE')).toBe('1,5 KB');
    expect(formatBytes(2000 * 1024 ** 4, 'de-DE')).toBe('2.000 TB');
  });
});

describe('formatList', () => {
  it('joins names the way the locale says a list', () => {
    expect(formatList(['moomoo'], 'en-US')).toBe('moomoo');
    expect(formatList(['moomoo', 'Webull'], 'en-US')).toBe('moomoo and Webull');
    expect(formatList(['moomoo', 'Webull', 'IBKR'], 'en-US')).toBe('moomoo, Webull, and IBKR');
  });

  it('spaces a Chinese conjunction from the Latin names beside it, not the enumeration comma', () => {
    expect(formatList(['moomoo', 'Webull'], 'zh-CN')).toBe('moomoo 和 Webull');
    expect(formatList(['moomoo', 'Webull', 'IBKR'], 'zh-CN')).toBe('moomoo、Webull 和 IBKR');
  });
});
