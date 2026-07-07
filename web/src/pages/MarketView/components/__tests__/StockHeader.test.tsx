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
