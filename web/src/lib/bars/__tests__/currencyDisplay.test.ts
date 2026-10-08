// @vitest-environment node
import { describe, it, expect } from 'vitest';
import type { BarPrice } from 'lightweight-charts';
import {
  chartPriceFormat,
  currencyForSymbol,
  currencySymbol,
  formatMoney,
  formatPrice,
  resolveCurrency,
  resolveDisplayCurrency,
} from '../currencyDisplay';

describe('currencySymbol', () => {
  it('maps known ISO codes to their symbols', () => {
    expect(currencySymbol('USD')).toBe('$');
    expect(currencySymbol('GBP')).toBe('£');
    expect(currencySymbol('HKD')).toBe('HK$');
    expect(currencySymbol('EUR')).toBe('€');
    expect(currencySymbol('JPY')).toBe('¥');
    expect(currencySymbol('CNY')).toBe('CN¥');
  });

  it('is case-insensitive', () => {
    expect(currencySymbol('gbp')).toBe('£');
  });

  it('falls back to "<ISO> " for unknown codes', () => {
    expect(currencySymbol('AUD')).toBe('AUD ');
    expect(currencySymbol('chf')).toBe('CHF ');
  });

  it('prints no prefix without a code, as for an index level', () => {
    expect(currencySymbol()).toBe('');
    expect(currencySymbol('')).toBe('');
    expect(currencySymbol(null)).toBe('');
  });
});

describe('currencyForSymbol', () => {
  it('maps exchange suffixes to their listing currency', () => {
    expect(currencyForSymbol('VOD.L')).toBe('GBP');
    expect(currencyForSymbol('0700.HK')).toBe('HKD');
    expect(currencyForSymbol('7203.T')).toBe('JPY');
    expect(currencyForSymbol('MC.PA')).toBe('EUR');
    expect(currencyForSymbol('SAP.DE')).toBe('EUR');
    expect(currencyForSymbol('ASML.AS')).toBe('EUR');
  });

  it('is case-insensitive on the suffix', () => {
    expect(currencyForSymbol('vod.l')).toBe('GBP');
  });

  it('defaults to USD for plain and empty symbols', () => {
    expect(currencyForSymbol('AAPL')).toBe('USD');
    expect(currencyForSymbol('')).toBe('USD');
    expect(currencyForSymbol(null)).toBe('USD');
  });
});

describe('formatPrice', () => {
  it('prefixes the currency symbol and fixes decimals', () => {
    expect(formatPrice(12.5, 'GBP', 2)).toBe('£12.50');
    expect(formatPrice(1.2345, 'USD', 2)).toBe('$1.23');
    expect(formatPrice(100, 'JPY', 0)).toBe('¥100');
    expect(formatPrice(5, 'AUD', 2)).toBe('AUD 5.00');
  });

  it('prints a bare figure at 2 decimals without a code', () => {
    expect(formatPrice(3)).toBe('3.00');
    expect(formatPrice(5000.5, null, 2)).toBe('5000.50');
  });

  it('guards non-finite values', () => {
    expect(formatPrice(NaN, 'USD', 2)).toBe('$0.00');
  });
});

describe('chartPriceFormat', () => {
  it('steps one unit in the last displayed place', () => {
    expect(chartPriceFormat({ current: { code: 'USD', decimals: 2 } }).minMove).toBe(0.01);
    expect(chartPriceFormat({ current: { code: 'USD', decimals: 4 } }).minMove).toBe(0.0001);
    expect(chartPriceFormat({ current: { code: 'JPY', decimals: 0 } }).minMove).toBe(1);
  });

  it('falls back to a cent when the decimals are unusable', () => {
    expect(chartPriceFormat({ current: { code: 'USD', decimals: NaN } }).minMove).toBe(0.01);
    expect(chartPriceFormat({ current: { code: 'USD', decimals: -1 } }).minMove).toBe(0.01);
  });

  it('labels from the ref as it is when the axis asks', () => {
    const display = { current: { code: 'GBP' as string | null, decimals: 4 } };
    const format = chartPriceFormat(display);
    expect(format.formatter(0.5432 as BarPrice)).toBe('£0.5432');
    display.current = { code: null, decimals: 2 };
    expect(format.formatter(5000.5 as BarPrice)).toBe('5000.50');
  });
});

describe('resolveDisplayCurrency', () => {
  it('prefers protocol metadata when present', () => {
    expect(resolveDisplayCurrency('AAPL', { currency: 'EUR', displayDecimals: 4 })).toEqual({
      code: 'EUR',
      decimals: 4,
    });
  });

  it('falls back to the suffix map and 2 decimals', () => {
    expect(resolveDisplayCurrency('VOD.L')).toEqual({ code: 'GBP', decimals: 2 });
    expect(resolveDisplayCurrency('AAPL', null)).toEqual({ code: 'USD', decimals: 2 });
  });

  it('gives an index no currency, even when the header names one', () => {
    expect(resolveDisplayCurrency('^GSPC')).toEqual({ code: null, decimals: 2 });
    expect(resolveDisplayCurrency('^GSPC', { currency: 'USD' }).code).toBeNull();
    expect(resolveDisplayCurrency('000300.SH', { currency: 'CNY' }).code).toBeNull();
    expect(resolveDisplayCurrency('600519.SH', { currency: 'CNY' }).code).toBe('CNY');
  });
});

describe('formatMoney', () => {
  it('puts the sign ahead of the currency symbol', () => {
    expect(formatMoney(-1.23, 'CNY', 'en-US')).toBe('-CN¥1.23');
    expect(formatMoney(-1.23, 'USD', 'en-US', { signed: true })).toBe('-$1.23');
    expect(formatMoney(1.23, 'HKD', 'en-US', { signed: true })).toBe('+HK$1.23');
    expect(formatMoney(1.23, 'USD', 'en-US')).toBe('$1.23');
  });

  it('gives a value that rounds to zero no sign', () => {
    expect(formatMoney(0, 'USD', 'en-US', { signed: true })).toBe('$0.00');
    expect(formatMoney(-0.001, 'USD', 'en-US', { signed: true })).toBe('$0.00');
    expect(formatMoney(-0.001, 'USD', 'en-US')).toBe('$0.00');
  });

  it('groups thousands and honors decimals', () => {
    expect(formatMoney(1500, 'CNY', 'en-US')).toBe('CN¥1,500.00');
    expect(formatMoney(100, 'JPY', 'en-US', { decimals: 0 })).toBe('¥100');
  });

  it('uses the locale symbol for any ISO code', () => {
    expect(formatMoney(5, 'AUD', 'en-US')).toBe('A$5.00');
    expect(formatMoney(5, 'gbp', 'en-US')).toBe('£5.00');
  });

  it('writes the symbol the way the locale does', () => {
    expect(formatMoney(1500, 'CNY', 'zh-CN')).toBe('¥1,500.00');
    expect(formatMoney(-1.23, 'USD', 'zh-CN')).toBe('-US$1.23');
    expect(formatMoney(2.5e12, 'CNY', 'zh-CN', { compact: true })).toBe('¥2.50万亿');
  });

  it('compacts large amounts with the sign still leading', () => {
    expect(formatMoney(2.5e12, 'USD', 'en-US', { compact: true })).toBe('$2.50T');
    expect(formatMoney(-3.2e9, 'CNY', 'en-US', { compact: true, signed: true })).toBe('-CN¥3.20B');
    expect(formatMoney(4500, 'GBP', 'en-US', { compact: true })).toBe('£4.50K');
  });

  it('prints an index level (no code) as the bare figure, sign still leading', () => {
    expect(formatMoney(4521.5, null, 'en-US')).toBe('4,521.50');
    expect(formatMoney(-12.25, null, 'en-US', { signed: true })).toBe('-12.25');
    expect(formatMoney(12.25, null, 'en-US', { signed: true })).toBe('+12.25');
  });

  it('names a code Intl would reject instead of throwing', () => {
    expect(formatMoney(-1.5, 'US$', 'en-US')).toBe('-US$ 1.50');
    expect(formatMoney(1.5, 'X1', 'en-US', { signed: true })).toBe('+X1 1.50');
  });

  it('reads a missing value as N/A', () => {
    expect(formatMoney(null, 'USD', 'en-US')).toBe('N/A');
    expect(formatMoney(NaN, 'USD', 'en-US')).toBe('N/A');
  });
});

describe('resolveCurrency', () => {
  it('prefers the wire code and falls back to the venue', () => {
    expect(resolveCurrency('EUR', 'AAPL')).toBe('EUR');
    expect(resolveCurrency(null, '600519.SS')).toBe('CNY');
    expect(resolveCurrency(undefined, undefined)).toBe('USD');
  });
});
