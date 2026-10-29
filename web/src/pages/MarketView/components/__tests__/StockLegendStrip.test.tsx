import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import React from 'react';

import { LegendStats } from '../StockLegendStrip';
import { deriveStockQuote, type StockQuoteInputs } from '../../hooks/useStockQuoteModel';

const baseInputs: StockQuoteInputs = {
  symbol: 'AMD',
  stockInfo: null,
  realTimePrice: null,
  quoteData: null,
  snapshot: null,
  marketStatus: null,
  wsStatus: 'disconnected',
};

// The change percent tracks the last trade, so after the close it includes the
// extended session and no longer matches the regular-session headline.
describe('LegendStats change cell', () => {
  it('is labeled Chg during the regular session', () => {
    render(<LegendStats quote={deriveStockQuote({ ...baseInputs, snapshot: { symbol: 'AMD', price: 120.5 } })} />);
    expect(screen.getByText('Chg')).toBeInTheDocument();
  });

  it('is labeled Chg incl. ext when an extended-hours print is showing', () => {
    const quote = deriveStockQuote({
      ...baseInputs,
      marketStatus: { market: 'closed', afterHours: false, earlyHours: false, providers: [] } as Record<string, unknown>,
      snapshot: { symbol: 'AMD', price: 96.5, previous_close: 100, regular_trading_change: -4, late_trading_change_percent: -1.0, source: 'x' },
      realTimePrice: { symbol: 'AMD', price: 96.5, open: 0, high: 0, low: 0, change: -3.5, changePercent: -3.5, volume: 0, previousClose: 100 },
    });
    expect(quote.ext).not.toBeNull();
    render(<LegendStats quote={quote} />);
    expect(screen.getByText('Chg incl. ext')).toBeInTheDocument();
  });
});

const cellValue = (key: string): string | null | undefined => screen.getByText(key).nextElementSibling?.textContent;

describe('LegendStats close cell', () => {
  it('prints the row price in a regular session', () => {
    render(<LegendStats quote={deriveStockQuote({
      ...baseInputs,
      marketStatus: { market: 'open', afterHours: false, earlyHours: false, providers: [] } as Record<string, unknown>,
      realTimePrice: { symbol: 'AMD', price: 101.23, open: 100, high: 102, low: 99, change: 1.23, changePercent: 1.23, volume: 0, previousClose: 100 },
    })} />);
    expect(cellValue('C')).toBe('101.23');
  });

  // The candle ends at the regular close; the after-hours print (96.50) is
  // the lead's business, not the bar's.
  it('prints the settled close, not the live price, in an extended session', () => {
    render(<LegendStats quote={deriveStockQuote({
      ...baseInputs,
      marketStatus: { market: 'closed', afterHours: false, earlyHours: false, providers: [] } as Record<string, unknown>,
      snapshot: { symbol: 'AMD', price: 96.5, previous_close: 100, regular_trading_change: -4, late_trading_change_percent: -1.0, source: 'x' },
      realTimePrice: { symbol: 'AMD', price: 96.5, open: 0, high: 0, low: 0, change: -3.5, changePercent: -3.5, volume: 0, previousClose: 100 },
    })} />);
    expect(cellValue('C')).toBe('96.00');
    expect(screen.queryByText('96.50')).not.toBeInTheDocument();
  });
});

describe('LegendStats volume cell', () => {
  it('is labeled V when the row carries a session volume', () => {
    render(<LegendStats quote={deriveStockQuote({
      ...baseInputs,
      stockInfo: { Symbol: 'AMD', Name: 'AMD', Price: 120.5, Volume: 1_250_000, AverageVolume: 2_500_000 } as never,
    })} />);
    expect(cellValue('V')).toBe('1.25M');
    expect(screen.queryByText('Avg vol 3M')).not.toBeInTheDocument();
  });

  it('says it is the 3-month average when that is what it falls back to', () => {
    render(<LegendStats quote={deriveStockQuote({ ...baseInputs, quoteData: { avgVolume: 2_500_000 } })} />);
    expect(screen.queryByText('V')).not.toBeInTheDocument();
    expect(cellValue('Avg vol 3M')).toBe('2.50M');
  });
});
