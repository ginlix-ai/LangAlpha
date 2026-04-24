import { describe, it, expect } from 'vitest';
import { buildSeedSymbols, DEFAULT_TICKERS } from '../TickerTapeWidget';

describe('buildSeedSymbols', () => {
  it('returns DEFAULT_TICKERS in fixed order when watchlist + portfolio are empty', () => {
    expect(buildSeedSymbols([], [])).toEqual(DEFAULT_TICKERS);
  });

  it('dedupes a bare-ticker watchlist symbol against an exchange-qualified default', () => {
    const out = buildSeedSymbols(['SPY'], []);
    // Two SPY-like symbols shouldn't appear ('AMEX:SPY' default + bare 'SPY').
    expect(out.filter((s) => s.toUpperCase().endsWith('SPY'))).toHaveLength(1);
  });

  it('appends new watchlist symbols after the defaults block', () => {
    const out = buildSeedSymbols(['NVDA'], []);
    const nvdaIdx = out.indexOf('NVDA');
    expect(nvdaIdx).toBe(DEFAULT_TICKERS.length);
  });

  it('appends portfolio symbols after watchlist', () => {
    const out = buildSeedSymbols(['NVDA'], ['MSFT']);
    expect(out.indexOf('MSFT')).toBeGreaterThan(out.indexOf('NVDA'));
  });

  it('dedupes between watchlist and portfolio (no double NVDA)', () => {
    const out = buildSeedSymbols(['NVDA'], ['NVDA', 'AAPL']);
    expect(out.filter((s) => s === 'NVDA')).toHaveLength(1);
    expect(out).toContain('AAPL');
  });

  it('skips empty/falsy entries (filtered watchlist rows)', () => {
    const out = buildSeedSymbols(['', 'NVDA'], []);
    expect(out).toContain('NVDA');
    expect(out).not.toContain('');
  });

  it('is case-insensitive when comparing tickers', () => {
    // The render path filters by .symbol but a stored mixed-case entry
    // shouldn't sneak past the dedup.
    const out = buildSeedSymbols(['spy', 'nvda'], []);
    expect(out.filter((s) => s.toUpperCase().endsWith('SPY'))).toHaveLength(1);
    expect(out.filter((s) => s.toUpperCase() === 'NVDA')).toHaveLength(1);
  });
});
