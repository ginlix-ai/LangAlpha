/**
 * Render + wiring tests for ``MarketChartSurface`` — the self-contained replica
 * of the MarketView chart panel. The heavy child (lightweight-charts
 * MarketChart) and the market-data websocket / REST hooks are mocked so we
 * assert the surface's own behaviour: prop wiring to the chart and its legend
 * slots, the interval-switch state, the WS-over-REST price preference, and the
 * subscribe/unsubscribe lifecycle.
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

// Captured props handed to the mocked chart, and the inputs of the legend slots.
const legend = vi.hoisted(() => ({ args: null as Record<string, unknown> | null }));
const chart = vi.hoisted(() => ({ props: null as Record<string, unknown> | null }));

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

// The real slots, with their inputs recorded.
vi.mock('../../hooks/useChartLegendSlots', async (importOriginal) => {
  const orig = await importOriginal<typeof import('../../hooks/useChartLegendSlots')>();
  return {
    ...orig,
    useChartLegendSlots: (args: Parameters<typeof orig.useChartLegendSlots>[0]) => {
      legend.args = args as unknown as Record<string, unknown>;
      return orig.useChartLegendSlots(args);
    },
  };
});

vi.mock('../MarketChart', () => ({
  default: (props: Record<string, unknown>) => {
    chart.props = props;
    return (
      <>
        <button
          data-testid="market-chart"
          onClick={() => (props.onIntervalChange as (i: string) => void)('5min')}
        >
          chart
        </button>
        {props.toolbarLead as React.ReactNode}
        {props.toolbarSubrow as React.ReactNode}
      </>
    );
  },
}));

import { MarketChartSurface } from '../MarketChartSurface';

/** A pick in the legend's ticker switcher. */
const pick = (symbol: string, hit?: unknown) =>
  (legend.args!.onSwitchSymbol as (s: string, hit?: unknown) => void)(symbol, hit);

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
  legend.args = null;
  chart.props = null;
});

afterEach(() => vi.clearAllMocks());

describe('MarketChartSurface', () => {
  it('renders the chart with its legend and forwards the symbol to both', () => {
    render(<MarketChartSurface symbol="AAPL" />);

    expect(screen.getByTestId('market-chart')).toBeInTheDocument();
    expect(legend.args!.symbol).toBe('AAPL');
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

  it('relays a legend pick and a toolbar interval switch to the host unchanged', () => {
    const onSwitchSymbol = vi.fn();
    const onIntervalChange = vi.fn();
    render(<MarketChartSurface symbol="AAPL" timeframe="1day" onSwitchSymbol={onSwitchSymbol} onIntervalChange={onIntervalChange} />);

    act(() => pick('GOOGL', { symbol: 'GOOGL', name: 'Alphabet Inc.' }));
    expect(onSwitchSymbol).toHaveBeenCalledWith('GOOGL', expect.objectContaining({ name: 'Alphabet Inc.' }));

    act(() => fireEvent.click(screen.getByTestId('market-chart'))); // → '5min'
    expect(onIntervalChange).toHaveBeenCalledWith('5min');
    expect(chart.props!.interval).toBe('5min');
  });

  it('renders dashes and the bare symbol while the new symbol’s quote is loading', () => {
    // useStockData holds no row for a symbol whose quote is in flight.
    sd.stockInfo = null;
    sd.snapshotData = null;
    render(<MarketChartSurface symbol="GOOGL" />);

    const quote = legend.args!.quote as Record<string, unknown>;
    expect(quote.price).toBeNull();
    expect(quote.previousClose).toBeNull();
    // No name yet: the symbol stands in, never the previous company's name.
    expect(quote.displayName).toBe('GOOGL');
    expect(chart.props!.snapshot).toBeNull();
  });

  it('reads an index row spelled bare under its caret ticker', () => {
    // Index rows answer as `GSPC`; the page holds `^GSPC`. Rejecting the row
    // left the header Delayed with no as-of and the chart with no close line.
    sd.stockInfo = { Symbol: '^GSPC', Price: 5000 };
    sd.snapshotData = { symbol: 'GSPC', requested: ['GSPC'], asset_class: 'index', price: 5000, as_of: 1_800_000_000_000 };
    render(<MarketChartSurface symbol="^GSPC" />);

    const quote = legend.args!.quote as Record<string, unknown>;
    expect(chart.props!.snapshot).toBe(sd.snapshotData);
    expect(quote.asOf).toBe(1_800_000_000_000);
    expect(quote.currency).toBeNull();
  });

  it('names the picked company from the search hit until its own quote lands', () => {
    const onSwitchSymbol = vi.fn();
    const { rerender } = render(<MarketChartSurface symbol="AAPL" onSwitchSymbol={onSwitchSymbol} />);
    const displayName = () => (legend.args!.quote as Record<string, unknown>).displayName;
    expect(displayName()).toBe('AAPL');

    act(() => pick('GOOGL', { symbol: 'GOOGL', name: 'Alphabet Inc.' }));
    rerender(<MarketChartSurface symbol="GOOGL" onSwitchSymbol={onSwitchSymbol} />);
    expect(displayName()).toBe('Alphabet Inc.');

    // The name belongs to the symbol it was picked for, not to whatever is up next.
    rerender(<MarketChartSurface symbol="MSFT" onSwitchSymbol={onSwitchSymbol} />);
    expect(displayName()).toBe('MSFT');
  });

  it('follows a timeframe prop change after the toolbar moved the interval', () => {
    const { rerender } = render(<MarketChartSurface symbol="AAPL" timeframe="1day" />);
    act(() => fireEvent.click(screen.getByTestId('market-chart'))); // → '5min'
    expect(chart.props!.interval).toBe('5min');

    rerender(<MarketChartSurface symbol="AAPL" timeframe="1min" />);
    expect(chart.props!.interval).toBe('1min');
  });

  it('subscribes to the symbol feed on mount and unsubscribes on unmount', () => {
    const { unmount } = render(<MarketChartSurface symbol="AAPL" />);
    expect(ws.subscribe).toHaveBeenCalledWith(['AAPL']);

    unmount();
    expect(ws.unsubscribe).toHaveBeenCalledWith(['AAPL']);
  });

  it('hands the chart the workspace whose layers it reads', () => {
    render(<MarketChartSurface symbol="AAPL" workspaceId="ws-7" />);
    expect(chart.props!.workspaceId).toBe('ws-7');
  });

  it('hands the chart no workspace when none is provided', () => {
    render(<MarketChartSurface symbol="AAPL" />);
    expect(chart.props!.workspaceId).toBeNull();
  });

  it('prefers the live WS price over the REST realTimePrice for the legend', () => {
    const wsPrice = { symbol: 'AAPL', price: 200, barData: { close: 200 } };
    ws.prices = new Map([['AAPL', wsPrice]]);
    sd.realTimePrice = { symbol: 'AAPL', price: 111 };

    render(<MarketChartSurface symbol="AAPL" />);
    expect((legend.args!.quote as Record<string, unknown>).price).toBe(200);
    expect((legend.args!.source as Record<string, unknown>).wsHasData).toBe(true);
    // liveTick is sourced from the WS bar payload.
    expect(chart.props!.liveTick).toEqual(wsPrice.barData);
  });

  it('uses the REST price when it matches and there is no WS price', () => {
    const rest = { symbol: 'AAPL', price: 123 };
    sd.realTimePrice = rest;
    render(<MarketChartSurface symbol="AAPL" />);
    expect((legend.args!.quote as Record<string, unknown>).price).toBe(123);
  });

  it('threads quote + overlay + earnings overview data into the chart', () => {
    sd.overviewData = {
      quote: { last: 150, previousClose: 149 },
      earningsSurprises: [{ q: 1 }],
    };
    sd.overlayData = { grades: [] };

    render(<MarketChartSurface symbol="AAPL" />);
    expect(chart.props!.quoteData).toEqual({ last: 150, previousClose: 149 });
    expect(chart.props!.earningsData).toEqual([{ q: 1 }]);
    expect(chart.props!.overlayData).toEqual({ grades: [] });
    // The legend's model is derived from the same quote.
    expect((legend.args!.quote as Record<string, unknown>).previousClose).toBe(149);
  });

  it('forwards WS connection state to the legend and chart', () => {
    ws.connectionStatus = 'connecting';
    ws.dataLevel = 'delayed';

    render(<MarketChartSurface symbol="AAPL" />);
    expect(legend.args!.source).toMatchObject({ wsStatus: 'connecting', wsDataLevel: 'delayed' });
    expect(chart.props!.wsStatus).toBe('connecting');
  });

  it('puts the legend strips in the toolbar, reading the phase the chart reports', () => {
    render(<MarketChartSurface symbol="AAPL" />);
    // Connected without data for this symbol: not live, phase unknown.
    expect(chart.props!.toolbarLead).toBeTruthy();
    expect(screen.getByText('Delayed')).toBeInTheDocument();

    act(() => (chart.props!.onMarketPhase as (p: string | null) => void)('closed'));
    expect(screen.getByText('Closed')).toBeInTheDocument();
  });
});
