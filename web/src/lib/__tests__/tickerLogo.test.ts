import { describe, expect, it } from 'vitest';
import { tickerInitial, tickerLogoUrl } from '../tickerLogo';

describe('tickerLogoUrl', () => {
  it('asks for one size whatever the listing', () => {
    expect(tickerLogoUrl('AAPL')).toBe('/api/v1/market-data/logos/AAPL?size=128');
    expect(tickerLogoUrl(' 0700.HK ')).toBe('/api/v1/market-data/logos/0700.HK?size=128');
  });

  it('asks nothing for an index, an option contract or a spelling with no letter', () => {
    expect(tickerLogoUrl('^GSPC')).toBeNull();
    expect(tickerLogoUrl('000300.SH')).toBeNull();
    expect(tickerLogoUrl('O:AAPL250117C00150000')).toBeNull();
    expect(tickerLogoUrl('  ')).toBeNull();
    expect(tickerLogoUrl('..')).toBeNull();
  });

  it('keeps pairs and crypto spellings in one path segment', () => {
    expect(tickerLogoUrl('EURUSD=X')).toBe('/api/v1/market-data/logos/EURUSD%3DX?size=128');
    expect(tickerLogoUrl('X:BTCUSD')).toBe('/api/v1/market-data/logos/X%3ABTCUSD?size=128');
  });
});

describe('tickerInitial', () => {
  it('is the first letter without an index caret', () => {
    expect(tickerInitial('^gspc')).toBe('G');
    expect(tickerInitial('600519.SH')).toBe('6');
  });
});
