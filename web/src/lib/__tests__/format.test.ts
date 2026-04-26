import { describe, it, expect, beforeEach } from 'vitest';
import i18n from '@/i18n';
import { createFormatter, createDateFormatter } from '@/lib/format';

describe('createFormatter', () => {
  beforeEach(() => {
    i18n.changeLanguage('en-US');
  });

  it('formats numbers with the active locale', () => {
    const fmt = createFormatter({ minimumFractionDigits: 2, maximumFractionDigits: 2 });
    expect(fmt(1234.5)).toBe('1,234.50');
  });

  it('reformats after locale switch (zh-CN uses same Western digits + grouping)', () => {
    const fmt = createFormatter({ style: 'currency', currency: 'USD' });
    const en = fmt(1234.5);
    i18n.changeLanguage('zh-CN');
    const zh = fmt(1234.5);
    expect(en).not.toBe(zh);
    expect(zh).toContain('1,234.50');
  });

  it('reuses Intl instance when locale unchanged', () => {
    const fmt = createFormatter({});
    expect(fmt(1)).toBe(fmt(1));
    expect(fmt(2)).toBe('2');
  });

  it('percent formatting respects locale', () => {
    const fmt = createFormatter({ style: 'percent', minimumFractionDigits: 2 });
    expect(fmt(0.1234)).toBe('12.34%');
  });
});

describe('createDateFormatter', () => {
  beforeEach(() => {
    i18n.changeLanguage('en-US');
  });

  it('formats dates with the active locale', () => {
    const fmt = createDateFormatter({ year: 'numeric', month: 'short', day: 'numeric' });
    const out = fmt(new Date('2026-04-25T00:00:00Z'));
    expect(out).toMatch(/Apr/);
  });

  it('reformats after locale switch', () => {
    const fmt = createDateFormatter({ year: 'numeric', month: 'long' });
    const en = fmt(new Date('2026-04-25T00:00:00Z'));
    i18n.changeLanguage('zh-CN');
    const zh = fmt(new Date('2026-04-25T00:00:00Z'));
    expect(en).not.toBe(zh);
  });
});

// If `i18n.language` is briefly invalid (transient changeLanguage state, broken
// localStorage), Intl throws. The closure must catch and fall back to the host
// default — otherwise every memoized formatter on the dashboard would crash on
// every subsequent call. Stage the bad locale by stubbing i18n.language directly.
describe('safe fallback for invalid locales', () => {
  it('createFormatter falls back to host default when locale is rejected', () => {
    const original = i18n.language;
    Object.defineProperty(i18n, 'language', { value: 'xx-not-a-locale-😀', configurable: true });
    const fmt = createFormatter({ minimumFractionDigits: 2 });
    expect(() => fmt(1234.5)).not.toThrow();
    expect(fmt(1234.5)).toMatch(/1.?234/);
    Object.defineProperty(i18n, 'language', { value: original, configurable: true });
  });

  it('createDateFormatter falls back to host default when locale is rejected', () => {
    const original = i18n.language;
    Object.defineProperty(i18n, 'language', { value: 'xx-not-a-locale-😀', configurable: true });
    const fmt = createDateFormatter({ year: 'numeric', month: 'short' });
    expect(() => fmt(new Date('2026-04-25T00:00:00Z'))).not.toThrow();
    Object.defineProperty(i18n, 'language', { value: original, configurable: true });
  });
});
