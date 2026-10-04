/**
 * ``useStockData``'s quote half: which symbols keep polling while the socket is
 * up, and that a ticker switch never shows the previous symbol's row. The quote
 * layer is mocked at ``useQuote`` so each test states exactly what the cache
 * holds; the overview, analyst and market-status fetches are stubbed out.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { renderHook } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import React from 'react';

import type { SnapshotData } from '@/types/market';
import type { ConnectionStatus } from '../useMarketDataWS';

const quoteState = vi.hoisted(() => ({
  bySymbol: new Map<string, { quote: unknown; isLoading: boolean }>(),
  calls: [] as Array<{ symbol: string | null; options: Record<string, unknown> }>,
}));

vi.mock('@/lib/quotes', () => ({
  useQuote: (symbol: string | null, options: Record<string, unknown>) => {
    quoteState.calls.push({ symbol, options });
    const hit = symbol ? quoteState.bySymbol.get(symbol) : undefined;
    return { quote: hit?.quote, isLoading: hit?.isLoading ?? false, isFetching: false, refetch: () => {} };
  },
}));

vi.mock('@/lib/marketUtils', () => ({ fetchMarketStatus: () => new Promise(() => {}) }));

vi.mock('../../utils/api', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../../utils/api')>();
  return {
    ...actual,
    fetchCompanyOverview: () => new Promise(() => {}),
    fetchAnalystData: () => new Promise(() => {}),
  };
});

import { useStockData } from '../useStockData';

let qc: QueryClient;

function wrapper({ children }: { children: React.ReactNode }) {
  return React.createElement(QueryClientProvider, { client: qc }, children);
}

function renderStock(selectedStock: string, wsStatus: ConnectionStatus = 'connected') {
  return renderHook(
    (props: { selectedStock: string; wsStatus: ConnectionStatus }) => useStockData(props),
    { wrapper, initialProps: { selectedStock, wsStatus } },
  );
}

function lastRefetchInterval(): unknown {
  return quoteState.calls[quoteState.calls.length - 1]?.options.refetchInterval;
}

const AAPL_ROW: SnapshotData = { symbol: 'AAPL', price: 190, previous_close: 189, currency: 'USD', name: 'Apple Inc.' };

beforeEach(() => {
  qc = new QueryClient({ defaultOptions: { queries: { retry: false, gcTime: 0 } } });
  quoteState.bySymbol = new Map();
  quoteState.calls = [];
});

describe('useStockData quote polling', () => {
  it('leaves a US equity to the socket while it is connected', () => {
    renderStock('AAPL');
    expect(lastRefetchInterval()).toBe(false);
  });

  it.each(['600519.SH', '0700.HK', '^GSPC', '000300.SH'])(
    'keeps polling %s while the socket is connected, since the socket does not carry it',
    (symbol) => {
      renderStock(symbol);
      expect(lastRefetchInterval()).toBe(60000);
    },
  );

  it('polls a US equity once the socket is down', () => {
    renderStock('AAPL', 'reconnecting');
    expect(lastRefetchInterval()).toBe(60000);
  });
});

describe('useStockData on a ticker switch', () => {
  it('shows nothing of the previous symbol while the new quote is in flight', () => {
    quoteState.bySymbol.set('AAPL', { quote: AAPL_ROW, isLoading: false });
    const { result, rerender } = renderStock('AAPL');
    expect(result.current.stockInfo?.Name).toBe('Apple Inc.');
    expect(result.current.snapshotData?.currency).toBe('USD');

    quoteState.bySymbol.set('600519.SH', { quote: undefined, isLoading: true });
    rerender({ selectedStock: '600519.SH', wsStatus: 'connected' });

    expect(result.current.stockInfo).toBeNull();
    expect(result.current.realTimePrice).toBeNull();
    expect(result.current.snapshotData).toBeNull();
  });

  it('shows the new symbol as soon as its row lands', () => {
    quoteState.bySymbol.set('AAPL', { quote: AAPL_ROW, isLoading: false });
    const { result, rerender } = renderStock('AAPL');

    const row: SnapshotData = { symbol: '600519.SH', price: 1500, currency: 'CNY', name: '贵州茅台' };
    quoteState.bySymbol.set('600519.SH', { quote: row, isLoading: false });
    rerender({ selectedStock: '600519.SH', wsStatus: 'connected' });

    expect(result.current.stockInfo?.Symbol).toBe('600519.SH');
    expect(result.current.realTimePrice?.price).toBe(1500);
    expect(result.current.snapshotData).toBe(row);
  });

  it('answers an index family under its caret spelling, though the row is spelled bare', () => {
    const row: SnapshotData = { symbol: 'GSPC', requested: ['GSPC'], price: 5000, asset_class: 'index' };
    quoteState.bySymbol.set('^GSPC', { quote: row, isLoading: false });
    const { result } = renderStock('^GSPC');
    expect(result.current.snapshotData).toBe(row);
    expect(result.current.realTimePrice?.price).toBe(5000);
  });
});
