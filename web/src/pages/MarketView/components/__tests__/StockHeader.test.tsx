import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import StockHeader from '../StockHeader';
import type { SnapshotData } from '@/types/market';
import type { ConnectionStatus } from '../../hooks/useMarketDataWS';

const baseProps = {
  symbol: 'AMD',
  stockInfo: null,
  realTimePrice: null,
  chartMeta: null,
  displayOverride: null,
  onToggleOverview: () => {},
  wsStatus: 'disconnected' as ConnectionStatus,
  quoteData: null,
  marketStatus: { providers: ['ginlix-data', 'yfinance', 'fmp'] } as Record<string, unknown>,
  snapshot: null,
};

const snap = (source: string | null): SnapshotData => ({ symbol: 'AMD', price: 120.5, source });

describe('StockHeader source tooltip', () => {
  it('shows the snapshot-filling provider when not live', () => {
    render(<StockHeader {...baseProps} snapshot={snap('fmp')} />);
    expect(screen.getByText('Source: FMP')).toBeInTheDocument();
  });

  it('shows the WS feed provider when live', () => {
    render(
      <StockHeader
        {...baseProps}
        wsStatus="connected"
        wsHasData
        wsDataLevel="second"
        snapshot={snap('fmp')} // live price comes from WS, not this row
      />,
    );
    expect(screen.getByText('Source: Ginlix Data')).toBeInTheDocument();
  });

  it('falls back to the enabled-provider list when the row has no source', () => {
    render(<StockHeader {...baseProps} snapshot={snap(null)} />);
    expect(screen.getByText('Source: Ginlix Data, yfinance, FMP')).toBeInTheDocument();
  });
});

describe('StockHeader price section (market convention)', () => {
  // Quote row after the close: official close = prev 100 + regular −4 = 96;
  // the after-hours move (−1% vs that close) renders on its own line.
  const postSnap: SnapshotData = {
    symbol: 'AMD',
    price: 96.5,
    previous_close: 100,
    regular_trading_change: -4,
    late_trading_change_percent: -1.0,
    source: 'x',
  };
  const closedStatus = { market: 'closed', afterHours: false, earlyHours: false, providers: [] } as Record<string, unknown>;
  const quoteRow = { symbol: 'AMD', price: 96.5, open: 0, high: 0, low: 0, change: -3.5, changePercent: -3.5, volume: 0, previousClose: 100 };

  it('after the close, headlines the official close with a coherent change pair', () => {
    const { container } = render(
      <StockHeader {...baseProps} marketStatus={closedStatus} snapshot={postSnap} realTimePrice={quoteRow} />,
    );
    expect(screen.getByText('96.00')).toBeInTheDocument();
    expect(screen.getByText('-4.00 -4.00%')).toBeInTheDocument();
    const ext = container.querySelector('.stock-extended-hours');
    expect(ext?.textContent).toContain('95.04');
    expect(ext?.textContent).toContain('(-1.00%)');
  });

  it('the big close is refresh-stable: a quote-row price never replaces it', () => {
    // The row's own price field (96.5, a different tape moment) must not leak
    // into the big number — that mix was the refresh nondeterminism.
    render(<StockHeader {...baseProps} marketStatus={closedStatus} snapshot={postSnap} realTimePrice={quoteRow} />);
    expect(screen.queryByText('96.50')).not.toBeInTheDocument();
  });

  it('a live WS tick updates the after-hours line, not the official close', () => {
    const tick = { ...quoteRow, price: 95.5, timestamp: 1700000000000 };
    const { container } = render(
      <StockHeader {...baseProps} marketStatus={closedStatus} snapshot={postSnap} realTimePrice={tick} />,
    );
    expect(screen.getByText('96.00')).toBeInTheDocument();
    const ext = container.querySelector('.stock-extended-hours');
    expect(ext?.textContent).toContain('95.50');
    expect(ext?.textContent).toContain('(-0.52%)');
  });

  it('renders the provider-exact close and full-precision pair when the row carries them', () => {
    // The rounded change fields say close = 96.00; the exact fields say 96.06
    // with the AH print at exactly 95.00 — the exact values must win.
    const exactSnap: SnapshotData = {
      ...postSnap,
      regular_close: 96.06,
      late_trading_change: -1.06,
      late_trading_change_percent: -1.1,
    };
    const { container } = render(
      <StockHeader {...baseProps} marketStatus={closedStatus} snapshot={exactSnap} realTimePrice={quoteRow} />,
    );
    expect(container.querySelector('.stock-price')?.textContent).toBe('96.06');
    expect(screen.getByText('-3.94 -3.94%')).toBeInTheDocument();
    const ext = container.querySelector('.stock-extended-hours');
    expect(ext?.textContent).toContain('95.00');
    expect(ext?.textContent).toContain('-1.06');
    expect(ext?.textContent).toContain('(-1.10%)');
  });

  it('the after-hours line shows the aggregate close, matching the chart', () => {
    // last_minute_close (consolidated last sale) beats the provider's
    // odd-lot-tainted late change; the triple is re-derived from it.
    const aggSnap: SnapshotData = {
      ...postSnap,
      regular_close: 96.06,
      late_trading_change: -1.06,
      last_minute_close: 95.1,
    };
    const { container } = render(
      <StockHeader {...baseProps} marketStatus={closedStatus} snapshot={aggSnap} realTimePrice={quoteRow} />,
    );
    expect(container.querySelector('.stock-price')?.textContent).toBe('96.06');
    const ext = container.querySelector('.stock-extended-hours');
    expect(ext?.textContent).toContain('95.10');
    expect(ext?.textContent).toContain('-0.96');
    expect(ext?.textContent).toContain('(-1.00%)');
  });

  it('pre-market headlines the previous close with the early move on its own line', () => {
    const preStatus = { market: 'open', afterHours: false, earlyHours: true, providers: [] } as Record<string, unknown>;
    const preSnap: SnapshotData = { symbol: 'AMD', price: 102, previous_close: 100, early_trading_change_percent: 2.0, source: 'x' };
    const { container } = render(
      <StockHeader {...baseProps} marketStatus={preStatus} snapshot={preSnap} realTimePrice={{ ...quoteRow, price: 102 }} />,
    );
    expect(container.querySelector('.stock-price')?.textContent).toBe('100.00');
    expect(container.querySelector('.stock-change')).toBeNull();
    const ext = container.querySelector('.stock-extended-hours');
    expect(ext?.textContent).toContain('102.00');
    expect(ext?.textContent).toContain('(+2.00%)');
  });

  it('regular session renders the row price and change pair unchanged', () => {
    const openStatus = { market: 'open', afterHours: false, earlyHours: false, providers: [] } as Record<string, unknown>;
    render(
      <StockHeader
        {...baseProps}
        marketStatus={openStatus}
        snapshot={{ symbol: 'AMD', price: 101.23, previous_close: 100, source: 'x' }}
        realTimePrice={{ ...quoteRow, price: 101.23, change: 1.23, changePercent: 1.23 }}
      />,
    );
    expect(screen.getByText('101.23')).toBeInTheDocument();
    expect(screen.getByText('+1.23 +1.23%')).toBeInTheDocument();
  });
});

describe('StockHeader market status badge', () => {
  it('shows Closed with the venue prefix when the market phase is closed', () => {
    render(<StockHeader {...baseProps} symbol="0700.HK" marketPhase="closed" />);
    expect(screen.getByText('HK Closed')).toBeInTheDocument();
  });

  it('shows a bare Closed for US symbols', () => {
    render(<StockHeader {...baseProps} marketPhase="closed" />);
    expect(screen.getByText('Closed')).toBeInTheDocument();
  });

  it('stays on Delayed during pre/post phases and when the phase is unknown', () => {
    const { rerender } = render(<StockHeader {...baseProps} symbol="0700.HK" marketPhase="post" />);
    expect(screen.getByText('HK Delayed')).toBeInTheDocument();
    rerender(<StockHeader {...baseProps} symbol="0700.HK" marketPhase={null} />);
    expect(screen.getByText('HK Delayed')).toBeInTheDocument();
  });

  it('a live WS feed wins over a stale closed phase', () => {
    render(
      <StockHeader {...baseProps} wsStatus="connected" wsHasData wsDataLevel="second" marketPhase="closed" />,
    );
    expect(screen.getByText('Live')).toBeInTheDocument();
  });
});
