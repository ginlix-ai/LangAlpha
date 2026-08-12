import { describe, it, expect } from 'vitest';
import {
  currencyForSymbol,
  currencySymbol,
  formatPrice,
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

  it('defaults to $ when no code is given', () => {
    expect(currencySymbol()).toBe('$');
    expect(currencySymbol('')).toBe('$');
    expect(currencySymbol(null)).toBe('$');
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

  it('defaults to $ and 2 decimals', () => {
    expect(formatPrice(3)).toBe('$3.00');
  });

  it('guards non-finite values', () => {
    expect(formatPrice(NaN, 'USD', 2)).toBe('$0.00');
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
});
