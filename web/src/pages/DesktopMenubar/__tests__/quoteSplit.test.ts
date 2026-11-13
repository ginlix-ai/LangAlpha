import { describe, expect, it } from 'vitest';
import type { MarketStatusData } from '@/lib/marketUtils';
import { splitQuote } from '../quoteSplit';

const PRE: MarketStatusData = { market: 'extended-hours', earlyHours: true };

describe('splitQuote', () => {
  it('reads a row the session has not printed at its previous close, with no move rather than 0.00%', () => {
    // No pre-market print either: nothing has moved it yet.
    const quiet = splitQuote('AAPL', { symbol: 'AAPL', price: null, previous_close: 354.88 }, PRE);
    expect(quiet).toMatchObject({ quoteAvailable: true, price: 354.88, change: null, pct: null, ext: null });
  });

  it('keeps the pre-market move beside the previous close', () => {
    const moved = splitQuote(
      'AAPL',
      {
        symbol: 'AAPL', price: null, previous_close: 354.88, last_minute_close: 356.23,
        early_trading_change: 1.35, early_trading_change_percent: 0.38,
      },
      PRE,
    );
    expect(moved).toMatchObject({ price: 354.88, change: null, pct: null, ext: { label: 'PM', price: 356.23 } });
  });

  it('leaves a printed row its own move', () => {
    const open: MarketStatusData = { market: 'open' };
    expect(splitQuote('AAPL', { symbol: 'AAPL', price: 356, change: 1.12, change_percent: 0.32, previous_close: 354.88 }, open))
      .toMatchObject({ price: 356, change: 1.12, pct: 0.32 });
  });
});
