// @vitest-environment node
import { describe, it, expect } from 'vitest';
import {
  FOREIGN_EXCHANGES,
  US_MARKET_TZ,
  currencyForSymbol,
  displaySpelling,
  isIndexFamilySpelling,
  isIndexListing,
  isUSEquity,
  quoteCurrency,
  timezoneForSymbol,
  venueLabelForSymbol,
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
    expect(currencyForSymbol('600519.SH')).toBe('CNY');
    expect(currencyForSymbol('000001.SZ')).toBe('CNY');
    expect(currencyForSymbol('430047.BJ')).toBe('CNY');
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
    expect(isUSEquity('600519.SH')).toBe(false); // .SH/.BJ are accepted inbound spellings
    expect(isUSEquity('430047.BJ')).toBe(false);
    expect(isUSEquity(null)).toBe(true);
  });

  it('fixes ASML.AS both ways — EUR currency AND non-US (was USD + US)', () => {
    expect(currencyForSymbol('ASML.AS')).toBe('EUR');
    expect(isUSEquity('ASML.AS')).toBe(false);
  });

  it('FOREIGN_EXCHANGES is derived from the table (union of both old lists)', () => {
    for (const suffix of ['HK', 'SS', 'SH', 'SZ', 'BJ', 'L', 'T', 'TO', 'AX', 'DE', 'PA', 'MC', 'AS']) {
      expect(FOREIGN_EXCHANGES.has(suffix)).toBe(true);
    }
  });

  it('resolves the venue timezone for mapped suffixes', () => {
    expect(timezoneForSymbol('VOD.L')).toBe('Europe/London');
    expect(timezoneForSymbol('0700.HK')).toBe('Asia/Hong_Kong');
    expect(timezoneForSymbol('7203.T')).toBe('Asia/Tokyo');
    expect(timezoneForSymbol('SHOP.TO')).toBe('America/Toronto');
    expect(timezoneForSymbol('600519.SS')).toBe('Asia/Shanghai');
    expect(timezoneForSymbol('600519.SH')).toBe('Asia/Shanghai');
    expect(timezoneForSymbol('430047.BJ')).toBe('Asia/Shanghai');
    expect(timezoneForSymbol('BHP.AX')).toBe('Australia/Sydney');
    expect(timezoneForSymbol('005930.KS')).toBe('Asia/Seoul');
  });

  it('defaults US / index / unknown symbols to ET', () => {
    expect(timezoneForSymbol('AAPL')).toBe(US_MARKET_TZ);
    expect(timezoneForSymbol('BRK.B')).toBe(US_MARKET_TZ); // dot, but "B" is not an exchange
    expect(timezoneForSymbol('^GSPC')).toBe(US_MARKET_TZ);
    expect(timezoneForSymbol(null)).toBe(US_MARKET_TZ);
  });

  it('gives caret-spelled foreign index families their home venue', () => {
    expect(timezoneForSymbol('^HSI')).toBe('Asia/Hong_Kong');
    expect(currencyForSymbol('^HSI')).toBe('HKD');
    expect(timezoneForSymbol('^N225')).toBe('Asia/Tokyo');
    expect(currencyForSymbol('^FTSE')).toBe('GBP');
    expect(timezoneForSymbol('^GDAXI')).toBe('Europe/Berlin');
    expect(isUSEquity('^HSI')).toBe(false);
    // Unknown families stay on the US default.
    expect(timezoneForSymbol('^XYZ')).toBe(US_MARKET_TZ);
    expect(currencyForSymbol('^XYZ')).toBe('USD');
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

describe('venueLabelForSymbol', () => {
  it('names the venue of a foreign listing and nothing else', () => {
    expect(venueLabelForSymbol('0700.HK')).toBe('HK');
    expect(venueLabelForSymbol('600519.SS')).toBe('SH');
    expect(venueLabelForSymbol('VOD.L')).toBe('LON');
    expect(venueLabelForSymbol('AMD')).toBeNull();
    expect(venueLabelForSymbol('^HSI')).toBeNull();
    expect(venueLabelForSymbol('BRK.B')).toBeNull();
  });
});

describe('displaySpelling', () => {
  it('spells Shanghai .SH and leaves every other symbol trimmed and uppercased', () => {
    expect(displaySpelling('600519.ss')).toBe('600519.SH');
    expect(displaySpelling(' 600519.SH ')).toBe('600519.SH');
    expect(displaySpelling('000001.sz')).toBe('000001.SZ');
    expect(displaySpelling('aapl')).toBe('AAPL');
    expect(displaySpelling('brk.b')).toBe('BRK.B');
    expect(displaySpelling('^gspc')).toBe('^GSPC');
  });

  it('writes an HKEX code at four digits, as the server stores it', () => {
    expect(displaySpelling('700.hk')).toBe('0700.HK');
    expect(displaySpelling('00700.HK')).toBe('0700.HK');
    expect(displaySpelling('09988.HK')).toBe('9988.HK');
    expect(displaySpelling('12345.HK')).toBe('12345.HK');
    expect(displaySpelling('0000.HK')).toBe('0000.HK');
  });

  it('folds what a Chinese IME types', () => {
    expect(displaySpelling('600519．ＳＨ')).toBe('600519.SH');
    expect(displaySpelling('600519。ss')).toBe('600519.SH');
  });
});

describe('index-ness', () => {
  it('routes on the caret spelling alone', () => {
    expect(isIndexFamilySpelling('^GSPC')).toBe(true);
    expect(isIndexFamilySpelling('000300.SH')).toBe(false);
    expect(isIndexFamilySpelling(null)).toBe(false);
  });

  it('reads a venue-listed CN index as an index and its stocks as stocks', () => {
    expect(isIndexListing('899050.BJ')).toBe(true);
    expect(isIndexListing('920000.BJ')).toBe(false);
    expect(isIndexListing('^HSI')).toBe(true);
    expect(isIndexListing('000300.SH')).toBe(true);
    expect(isIndexListing('000001.SS')).toBe(true);
    expect(isIndexListing('399001.SZ')).toBe(true);
    expect(isIndexListing('600519.SH')).toBe(false);
    expect(isIndexListing('000001.SZ')).toBe(false);
    expect(isIndexListing('AAPL')).toBe(false);
  });

  it('lets the row\'s asset class decide when it has one', () => {
    expect(isIndexListing('000001.SS', 'equity')).toBe(false);
    expect(isIndexListing('XYZ', 'index')).toBe(true);
  });

  it('gives an index level no currency and a listing its own', () => {
    expect(quoteCurrency('CNY', '000300.SH')).toBeNull();
    expect(quoteCurrency(null, '^GSPC')).toBeNull();
    expect(quoteCurrency(null, '600519.SH')).toBe('CNY');
    expect(quoteCurrency('USD', 'AAPL')).toBe('USD');
  });
});
