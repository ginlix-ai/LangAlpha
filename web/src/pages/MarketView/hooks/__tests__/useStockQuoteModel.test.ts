import { describe, it, expect } from 'vitest';
import { deriveStockQuote, quoteDotState, type StockQuoteInputs } from '../useStockQuoteModel';

const base: StockQuoteInputs = {
  symbol: 'AMD',
  stockInfo: null,
  realTimePrice: null,
  quoteData: null,
  snapshot: null,
  marketStatus: { providers: [] },
  wsStatus: 'disconnected',
};

const row = { symbol: 'AMD', price: 101.23, open: 100, high: 102, low: 99, change: 1.23, changePercent: 1.23, volume: 0, previousClose: 100 };

describe('deriveStockQuote', () => {
  it('keeps a missing change pair null rather than zero', () => {
    const q = deriveStockQuote({ ...base, stockInfo: { Symbol: 'AMD', Name: 'AMD', Price: 101.23 } as never });
    expect(q.price).toBe(101.23);
    expect(q.change).toBeNull();
    expect(q.changePercent).toBeNull();
    expect(q.tone).toBe('');
    expect(q.headline).toEqual({ price: 101.23, change: null, pct: null, tone: '' });
  });

  it('headlines the row price in a regular session', () => {
    const q = deriveStockQuote({
      ...base,
      marketStatus: { market: 'open', afterHours: false, earlyHours: false, providers: [] },
      realTimePrice: row,
    });
    expect(q.ext).toBeNull();
    expect(q.headline).toEqual({ price: 101.23, change: 1.23, pct: 1.23, tone: 'positive' });
  });

  it('headlines the settled close after hours and keeps the session move in ext', () => {
    const q = deriveStockQuote({
      ...base,
      marketStatus: { market: 'closed', afterHours: false, earlyHours: false, providers: [] },
      snapshot: { symbol: 'AMD', price: 96.5, previous_close: 100, regular_trading_change: -4, late_trading_change_percent: -1, source: 'x' },
      realTimePrice: { ...row, price: 96.5, change: -3.5, changePercent: -3.5 },
    });
    expect(q.ext?.type).toBe('post');
    expect(q.headline.price).toBe(96);
    expect(q.headline.change).toBe(-4);
    expect(q.headline.pct).toBe(-4);
    expect(q.headline.tone).toBe('negative');
  });

  it('status: a live feed wins, a closed phase reads closed, anything else delayed', () => {
    expect(deriveStockQuote({ ...base, wsStatus: 'connected', wsHasData: true, marketPhase: 'closed' }).status).toBe('live');
    expect(deriveStockQuote({ ...base, marketPhase: 'closed' }).status).toBe('closed');
    expect(deriveStockQuote({ ...base, marketPhase: 'post' }).status).toBe('delayed');
    expect(deriveStockQuote({ ...base }).status).toBe('delayed');
    // Only a US equity can be live off the WS feed.
    expect(deriveStockQuote({ ...base, symbol: '0700.HK', wsStatus: 'connected', wsHasData: true }).status).toBe('delayed');
  });

  it('carries the live tick time only when the row is a tick', () => {
    expect(deriveStockQuote({ ...base, realTimePrice: row }).tickAt).toBeNull();
    expect(deriveStockQuote({ ...base, realTimePrice: { ...row, timestamp: 1700000000000 } }).tickAt).toBe(1700000000000);
  });
});

describe('quoteDotState', () => {
  const dot = (snapshot: StockQuoteInputs['snapshot']) => quoteDotState(deriveStockQuote({ ...base, snapshot }));

  it('keeps a delay neutral and warns only on stale or short-of-close data', () => {
    expect(dot({ symbol: 'AMD', tier: 'delayed_15m' })).toBe('delayed');
    expect(dot({ symbol: 'AMD', tier: 'eod' })).toBe('delayed');
    expect(dot({ symbol: 'AMD', freshness: { label: 'stale', measured: true } })).toBe('warning');
    expect(dot({ symbol: 'AMD', freshness: { label: 'incomplete', measured: true } })).toBe('warning');
  });

  it('reads a current REST print as realtime and leaves the venue states alone', () => {
    expect(dot({ symbol: 'AMD', freshness: { label: 'live', measured: true } })).toBe('realtime');
    expect(quoteDotState(deriveStockQuote({ ...base, marketPhase: 'closed', snapshot: { symbol: 'AMD', freshness: { label: 'stale', measured: true } } }))).toBe('closed');
  });
});
