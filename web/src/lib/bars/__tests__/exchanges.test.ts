import { describe, it, expect } from 'vitest';
import {
  FOREIGN_EXCHANGES,
  US_MARKET_TZ,
  currencyForSymbol,
  isUSEquity,
  timezoneForSymbol,
} from '../exchanges';

describe('exchange suffix table', () => {
  it('resolves listing currency for every mapped suffix', () => {
    expect(currencyForSymbol('VOD.L')).toBe('GBP');
    expect(currencyForSymbol('0700.HK')).toBe('HKD');
    expect(currencyForSymbol('7203.T')).toBe('JPY');
    expect(currencyForSymbol('SHOP.TO')).toBe('CAD');
    expect(currencyForSymbol('MC.PA')).toBe('EUR');
    expect(currencyForSymbol('SAP.DE')).toBe('EUR');
    expect(currencyForSymbol('ASML.AS')).toBe('EUR');
    expect(currencyForSymbol('SAN.MC')).toBe('EUR');
    expect(currencyForSymbol('600519.SS')).toBe('CNY');
    expect(currencyForSymbol('000001.SZ')).toBe('CNY');
    expect(currencyForSymbol('BHP.AX')).toBe('AUD');
  });

  it('defaults unmapped / US symbols to USD', () => {
    expect(currencyForSymbol('AAPL')).toBe('USD');
    expect(currencyForSymbol('BRK.B')).toBe('USD'); // dot, but "B" is not an exchange
    expect(currencyForSymbol('')).toBe('USD');
    expect(currencyForSymbol(null)).toBe('USD');
  });

  it('classifies foreign listings as non-US (and US/indexes correctly)', () => {
    expect(isUSEquity('AAPL')).toBe(true);
    expect(isUSEquity('BRK.B')).toBe(true); // unknown suffix → US
    expect(isUSEquity('^GSPC')).toBe(false); // index
    expect(isUSEquity('VOD.L')).toBe(false);
    expect(isUSEquity('600519.SS')).toBe(false);
    expect(isUSEquity(null)).toBe(true);
  });

  it('fixes ASML.AS both ways — EUR currency AND non-US (was USD + US)', () => {
    expect(currencyForSymbol('ASML.AS')).toBe('EUR');
    expect(isUSEquity('ASML.AS')).toBe(false);
  });

  it('FOREIGN_EXCHANGES is derived from the table (union of both old lists)', () => {
    for (const suffix of ['HK', 'SS', 'SZ', 'L', 'T', 'TO', 'AX', 'DE', 'PA', 'MC', 'AS']) {
      expect(FOREIGN_EXCHANGES.has(suffix)).toBe(true);
    }
  });

  it('resolves the venue timezone for mapped suffixes', () => {
    expect(timezoneForSymbol('VOD.L')).toBe('Europe/London');
    expect(timezoneForSymbol('0700.HK')).toBe('Asia/Hong_Kong');
    expect(timezoneForSymbol('7203.T')).toBe('Asia/Tokyo');
    expect(timezoneForSymbol('SHOP.TO')).toBe('America/Toronto');
    expect(timezoneForSymbol('600519.SS')).toBe('Asia/Shanghai');
    expect(timezoneForSymbol('BHP.AX')).toBe('Australia/Sydney');
    expect(timezoneForSymbol('005930.KS')).toBe('Asia/Seoul');
  });

  it('defaults US / index / unknown symbols to ET', () => {
    expect(timezoneForSymbol('AAPL')).toBe(US_MARKET_TZ);
    expect(timezoneForSymbol('BRK.B')).toBe(US_MARKET_TZ); // dot, but "B" is not an exchange
    expect(timezoneForSymbol('^GSPC')).toBe(US_MARKET_TZ);
    expect(timezoneForSymbol(null)).toBe(US_MARKET_TZ);
  });

  it('classifies the backend-mirrored suffixes added with tz support as foreign', () => {
    for (const sym of ['005930.KS', '035720.KQ', '2330.TW', 'D05.SI', 'RELIANCE.NS', 'ENI.MI', 'NESN.SW']) {
      expect(isUSEquity(sym)).toBe(false);
    }
    expect(currencyForSymbol('005930.KS')).toBe('KRW');
    expect(currencyForSymbol('2330.TW')).toBe('TWD');
    expect(currencyForSymbol('NESN.SW')).toBe('CHF');
  });
});
