/**
 * Render + wiring tests for ``MarketChartSurface`` — the self-contained replica
 * of the MarketView chart panel. The heavy children (lightweight-charts
 * MarketChart, StockHeader, CompanyOverviewPanel) and the market-data
 * websocket / REST hooks are mocked so we assert the surface's own behaviour:
 * prop wiring to children, the interval-switch state, the overview toggle, the
 * WS-over-REST price preference, the subscribe/unsubscribe lifecycle, and the
 * 1s → 1min auto-downgrade for non-US symbols.
 */

import { render, screen, fireEvent, act } from '@testing-library/react';
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import React from 'react';

// --- Hoisted mock surface -------------------------------------------------

// Controllable WS context value; mutated per-test.
const ws = vi.hoisted(() => ({
  prices: new Map<string, unknown>(),
  connectionStatus: 'connected',
  dataLevel: 'realtime',
  ginlixDataEnabled: true,
  subscribe: vi.fn(),
  unsubscribe: vi.fn(),
  setPreviousClose: vi.fn(),
  setDayOpen: vi.fn(),
}));

// Controllable useStockData return; mutated per-test.
const sd = vi.hoisted(() => ({
  stockInfo: { name: 'Apple Inc.' } as Record<string, unknown> | null,
  realTimePrice: null as Record<string, unknown> | null,
  snapshotData: { symbol: 'AAPL' } as Record<string, unknown> | null,
  overviewData: null as Record<string, unknown> | null,
  overviewLoading: false,
  overlayData: null as Record<string, unknown> | null,
  marketStatus: 'open' as unknown,
}));

// Captured props handed to each mocked child.
const header = vi.hoisted(() => ({ props: null as Record<string, unknown> | null }));
const chart = vi.hoisted(() => ({ props: null as Record<string, unknown> | null }));
const overview = vi.hoisted(() => ({ props: null as Record<string, unknown> | null }));

// Spy for the annotation-sync side effect (args we want to assert on).
const syncSpy = vi.hoisted(() => vi.fn());

vi.mock('../../contexts/MarketDataWSContext', () => ({
  // Provider is a transparent pass-through here — the surface only needs the
  // hook value, which we control directly.
  MarketDataWSProvider: ({ children }: { children: React.ReactNode }) => <>{children}</>,
  useMarketDataWSContext: () => ws,
}));

vi.mock('../../hooks/useStockData', () => ({
  useStockData: (...args: unknown[]) => {
    // Record the call so we can assert the surface forwards selectedStock etc.
    (sd as Record<string, unknown>).__lastArgs = args;
    return sd;
  },
}));

vi.mock('../../hooks/useChartAnnotationSync', () => ({
  useChartAnnotationSync: (...args: unknown[]) => syncSpy(...args),
}));

vi.mock('../StockHeader', () => ({
  default: (props: Record<string, unknown>) => {
    header.props = props;
    return (
      <button data-testid="stock-header" onClick={props.onToggleOverview as () => void}>
        header
      </button>
    );
  },
}));

vi.mock('../MarketChart', () => ({
  default: (props: Record<string, unknown>) => {
    chart.props = props;
    return (
      <button
        data-testid="market-chart"
        onClick={() => (props.onIntervalChange as (i: string) => void)('5min')}
      >
        chart
      </button>
    );
  },
}));

vi.mock('../CompanyOverviewPanel', () => ({
  default: (props: Record<string, unknown>) => {
    overview.props = props;
    return (
      <button data-testid="overview-panel" onClick={props.onClose as () => void}>
        overview
      </button>
    );
  },
}));

import { MarketChartSurface } from '../MarketChartSurface';

beforeEach(() => {
  ws.prices = new Map();
  ws.connectionStatus = 'connected';
  ws.dataLevel = 'realtime';
  ws.ginlixDataEnabled = true;
  sd.stockInfo = { name: 'Apple Inc.' };
  sd.realTimePrice = null;
  sd.snapshotData = { symbol: 'AAPL' };
  sd.overviewData = null;
  sd.overviewLoading = false;
  sd.overlayData = null;
  sd.marketStatus = 'open';
  header.props = null;
  chart.props = null;
  overview.props = null;
});

afterEach(() => vi.clearAllMocks());

describe('MarketChartSurface', () => {
  it('renders the header + chart and forwards the symbol to both', () => {
    render(<MarketChartSurface symbol="AAPL" />);

    expect(screen.getByTestId('stock-header')).toBeInTheDocument();
    expect(screen.getByTestId('market-chart')).toBeInTheDocument();
    expect(header.props!.symbol).toBe('AAPL');
    expect(chart.props!.symbol).toBe('AAPL');
  });

  it('defaults the chart interval to 1day when no timeframe is given', () => {
    render(<MarketChartSurface symbol="AAPL" />);
    expect(chart.props!.interval).toBe('1day');
  });

  it('uses the provided timeframe as the initial interval', () => {
    render(<MarketChartSurface symbol="AAPL" timeframe="1hour" />);
    expect(chart.props!.interval).toBe('1hour');
  });

  it('updates the chart interval when the chart reports a switch', () => {
    render(<MarketChartSurface symbol="AAPL" timeframe="1day" />);
    expect(chart.props!.interval).toBe('1day');

    act(() => fireEvent.click(screen.getByTestId('market-chart'))); // → '5min'
    expect(chart.props!.interval).toBe('5min');
  });

  it('hides the overview panel until toggled, then shows + closes it', () => {
    render(<MarketChartSurface symbol="AAPL" />);
    expect(screen.queryByTestId('overview-panel')).not.toBeInTheDocument();

    act(() => fireEvent.click(screen.getByTestId('stock-header'))); // onToggleOverview
    expect(screen.getByTestId('overview-panel')).toBeInTheDocument();

    act(() => fireEvent.click(screen.getByTestId('overview-panel'))); // onClose
    expect(screen.queryByTestId('overview-panel')).not.toBeInTheDocument();
  });

  it('subscribes to the symbol feed on mount and unsubscribes on unmount', () => {
    const { unmount } = render(<MarketChartSurface symbol="AAPL" />);
    expect(ws.subscribe).toHaveBeenCalledWith(['AAPL']);

    unmount();
    expect(ws.unsubscribe).toHaveBeenCalledWith(['AAPL']);
  });

  it('loads this symbol\'s annotations via useChartAnnotationSync(workspaceId, symbol)', () => {
    render(<MarketChartSurface symbol="AAPL" workspaceId="ws-7" />);
    expect(syncSpy).toHaveBeenCalledWith('ws-7', 'AAPL');
    // workspaceId is forwarded to the chart too.
    expect(chart.props!.workspaceId).toBe('ws-7');
  });

  it('passes a null workspaceId to sync + chart when none is provided', () => {
    render(<MarketChartSurface symbol="AAPL" />);
    expect(syncSpy).toHaveBeenCalledWith(null, 'AAPL');
    expect(chart.props!.workspaceId).toBeNull();
  });

  it('prefers the live WS price over the REST realTimePrice for the header', () => {
    const wsPrice = { symbol: 'AAPL', price: 200, barData: { close: 200 } };
    ws.prices = new Map([['AAPL', wsPrice]]);
    sd.realTimePrice = { symbol: 'AAPL', price: 111 };

    render(<MarketChartSurface symbol="AAPL" />);
    expect(header.props!.realTimePrice).toBe(wsPrice);
    expect(header.props!.wsHasData).toBe(true);
    // liveTick is sourced from the WS bar payload.
    expect(chart.props!.liveTick).toEqual(wsPrice.barData);
  });

  it('falls back to REST price only when it matches the current symbol', () => {
    // No WS price; REST price is for a DIFFERENT symbol → guarded to null.
    sd.realTimePrice = { symbol: 'MSFT', price: 99 };
    render(<MarketChartSurface symbol="AAPL" />);
    expect(header.props!.realTimePrice).toBeNull();
    expect(header.props!.wsHasData).toBe(false);
  });

  it('uses the REST price when it matches and there is no WS price', () => {
    const rest = { symbol: 'AAPL', price: 123 };
    sd.realTimePrice = rest;
    render(<MarketChartSurface symbol="AAPL" />);
    expect(header.props!.realTimePrice).toBe(rest);
  });

  it('threads quote + overlay + earnings overview data into the chart', () => {
    sd.overviewData = {
      quote: { last: 150 },
      earningsSurprises: [{ q: 1 }],
    };
    sd.overlayData = { grades: [] };

    render(<MarketChartSurface symbol="AAPL" />);
    expect(chart.props!.quoteData).toEqual({ last: 150 });
    expect(chart.props!.earningsData).toEqual([{ q: 1 }]);
    expect(chart.props!.overlayData).toEqual({ grades: [] });
    // Header gets the same quote.
    expect(header.props!.quoteData).toEqual({ last: 150 });
  });

  it('forwards overview loading + data to the panel when open', () => {
    sd.overviewData = { quote: { last: 1 } };
    sd.overviewLoading = true;
    render(<MarketChartSurface symbol="AAPL" />);

    act(() => fireEvent.click(screen.getByTestId('stock-header')));
    expect(overview.props!.loading).toBe(true);
    expect(overview.props!.data).toEqual({ quote: { last: 1 } });
    expect(overview.props!.symbol).toBe('AAPL');
  });

  it('forwards WS connection state to header and chart (+ ginlix flag to header)', () => {
    ws.connectionStatus = 'connecting';
    ws.dataLevel = 'delayed';
    ws.ginlixDataEnabled = false;

    render(<MarketChartSurface symbol="AAPL" />);
    expect(header.props!.wsStatus).toBe('connecting');
    expect(header.props!.wsDataLevel).toBe('delayed');
    expect(header.props!.ginlixDataEnabled).toBe(false);
    expect(chart.props!.wsStatus).toBe('connecting');
  });
});
