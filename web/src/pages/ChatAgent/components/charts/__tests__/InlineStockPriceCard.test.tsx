import { fireEvent, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { InlineStockPriceCard } from '../InlineArtifactCards';

// Neutral placeholder symbol and fabricated bars only.
const OHLCV = [
  { date: '2026-07-13', close: 100.0 },
  { date: '2026-07-14', close: 101.5 },
];

// 2026-07-14 15:45 ET, the stamp the "last bar" clause prints in venue time.
const LAST_BAR_MS = Date.UTC(2026, 6, 14, 19, 45);

const artifact = (extra: Record<string, unknown>) => ({
  type: 'stock_prices',
  symbol: 'ACME',
  ohlcv: OHLCV,
  stats: { period_change_pct: 1.5 },
  ...extra,
});

describe('InlineStockPriceCard freshness line', () => {
  it('renders nothing extra for a pre-contract artifact', () => {
    render(<InlineStockPriceCard artifact={artifact({})} />);

    expect(screen.getByText('ACME')).toBeInTheDocument();
    expect(screen.queryByText(/delayed/i)).not.toBeInTheDocument();
    expect(screen.queryByText(/last bar/i)).not.toBeInTheDocument();
  });

  it('renders nothing when the series measures live', () => {
    render(
      <InlineStockPriceCard
        artifact={artifact({
          source: 'ginlix-data',
          freshness: {
            label: 'live',
            measured: true,
            lag_s: 4,
            source: 'ginlix-data',
            interval: '5min',
            actual_latest: LAST_BAR_MS,
          },
        })}
      />,
    );

    expect(screen.queryByText(/Ginlix Data/)).not.toBeInTheDocument();
    expect(screen.queryByText(/last bar/i)).not.toBeInTheDocument();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('badges a delayed series and keeps source and last bar for the tooltip', async () => {
    // Read days after the bar, so the stamp carries its venue date.
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(Date.UTC(2026, 6, 20, 16, 0));
    render(
      <InlineStockPriceCard
        artifact={artifact({
          source: 'yfinance',
          freshness: {
            label: 'delayed',
            measured: true,
            lag_s: 900,
            source: 'yfinance',
            interval: '5min',
            actual_latest: LAST_BAR_MS,
          },
        })}
      />,
    );

    const pill = screen.getByText('Delayed 15 min');
    // The caption is gone; source and last bar ride the pill's tooltip.
    expect(screen.queryByText(/Yahoo Finance/)).not.toBeInTheDocument();
    fireEvent.focus(pill.parentElement!);
    const tooltip = await screen.findByRole('tooltip');
    expect(tooltip).toHaveTextContent('Source: Yahoo Finance');
    // 19:45 UTC is 15:45 in New York, and a bar from an earlier venue day
    // carries its date ahead of the time.
    expect(tooltip).toHaveTextContent('last bar Jul 14, 15:45');
  });

  it('names a stale series without inventing a lag', () => {
    render(
      <InlineStockPriceCard
        artifact={artifact({
          source: 'fmp',
          freshness: { label: 'stale', measured: true, lag_s: 90_000, source: 'fmp', interval: '5min', actual_latest: LAST_BAR_MS },
        })}
      />,
    );
    expect(screen.getByText('Stale')).toBeInTheDocument();
    expect(screen.queryByText(/delayed/i)).not.toBeInTheDocument();
  });

  it('dates a Shanghai or Hong Kong daily series on the venue calendar, not New York\'s', () => {
    // Daily bars stamped at venue midnight: 00:00 in Shanghai and Hong Kong is
    // 16:00 UTC the day before, still the previous afternoon in New York.
    const venueMidnight = (day: number) => Date.UTC(2026, 6, day - 1, 16, 0);
    const daily = [{ time: venueMidnight(13), close: 1500 }, { time: venueMidnight(14), close: 1510 }];
    for (const symbol of ['600519.SH', '0700.HK']) {
      const { unmount } = render(<InlineStockPriceCard artifact={{ ...artifact({}), symbol, ohlcv: daily }} />);
      expect(screen.getByText('2026-07-13, 2026-07-14')).toBeInTheDocument();
      unmount();
    }
  });

  it('prints the price in the series currency, not a hardcoded dollar', () => {
    render(<InlineStockPriceCard artifact={{ ...artifact({}), symbol: '600519.SS', price_currency: 'CNY' }} />);
    expect(screen.getByText(/¥\d/)).toBeInTheDocument();
    expect(screen.queryByText(/\$\d/)).not.toBeInTheDocument();
  });
});
