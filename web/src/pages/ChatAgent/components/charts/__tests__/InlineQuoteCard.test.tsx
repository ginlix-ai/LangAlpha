import { render, screen, fireEvent } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import { InlineQuoteCard } from '../InlineQuoteCard';
import enUS from '@/locales/en-US.json';

// Drive the extended-hours label assertion off the real i18n value, not a
// literal — the card now renders it via `toolArtifact.extendedHours.*`.
const AFTER_HOURS_LONG = enUS.toolArtifact.extendedHours.afterHours.long;

// Unified snapshot shape (snake_case) from the backend provider chain.
// Neutral placeholder symbols and fabricated numbers only.
const NVDA_REGULAR = {
  symbol: 'NVDA',
  name: 'NVIDIA Corporation',
  price: 233.45,
  change: 5.27,
  change_percent: 2.31,
  low: 227.8,
  high: 234.9,
  open: 229.1,
  previous_close: 228.18,
  volume: 187_200_000,
  market_status: 'open',
};

const TSLA_REGULAR = {
  symbol: 'TSLA',
  name: 'Tesla, Inc.',
  price: 410.0,
  change: -4.22,
  change_percent: -1.02,
  low: 405.4,
  high: 418.9,
  volume: 92_400_000,
  market_status: 'open',
};

// ginlix-data after-hours snapshot: blended fields plus the decomposition.
const NVDA_AFTER_HOURS = {
  ...NVDA_REGULAR,
  market_status: 'late_trading',
  last_trade_price: 235.3,
  regular_close: 233.45,
  regular_trading_change: 5.27,
  regular_trading_change_percent: 2.31,
  late_trading_change: 1.85,
  late_trading_change_percent: 0.79,
};

// Non-US listing: backend stamps it with the venue-local retrieval clock.
const HK_CLOSED = {
  symbol: '0700.HK',
  name: 'Tencent Holdings',
  price: 454.2,
  change: -3.4,
  change_percent: -0.74,
  low: 448.2,
  high: 457.6,
  market_status: 'closed',
  as_of_local: '2026-07-14 23:05:12 HKT',
};

// FMP fallback shape: no market_status, no last_trade / extended fields.
const FMP_ONLY = {
  symbol: 'AAPL',
  name: 'Apple Inc.',
  price: 190.99,
  change: 0.27,
  change_percent: 0.14,
  low: 189.5,
  high: 191.8,
  market_status: null,
};

const quoteArtifact = (quotes: Record<string, unknown>[]) => ({
  type: 'quote',
  quotes,
  as_of: '2026-07-14 14:32:05 ET',
  as_of_ts: 1784140325000,
});

describe('InlineQuoteCard', () => {
  it('renders a hero for a single symbol: big price, abs+pct change, stats, no list header', () => {
    render(<InlineQuoteCard artifact={quoteArtifact([NVDA_REGULAR])} />);

    expect(screen.getByText('NVDA')).toBeInTheDocument();
    expect(screen.getByText('233.45')).toBeInTheDocument();
    expect(screen.getByText('+5.27 (+2.31%)')).toBeInTheDocument();
    expect(screen.getByText(/Prev Close/)).toBeInTheDocument();
    expect(screen.getByText('187.2M')).toBeInTheDocument();
    // Labeled range bounds
    expect(screen.getByText('L 227.80')).toBeInTheDocument();
    expect(screen.getByText('H 234.90')).toBeInTheDocument();
    // Hero replaces the multi-symbol header
    expect(screen.queryByText('Live Quotes')).not.toBeInTheDocument();
  });

  it('renders compact rows for two or more symbols', () => {
    render(<InlineQuoteCard artifact={quoteArtifact([NVDA_REGULAR, TSLA_REGULAR])} />);

    expect(screen.getByText('Live Quotes')).toBeInTheDocument();
    expect(screen.getByText('2026-07-14 14:32:05 ET')).toBeInTheDocument();
    expect(screen.getByText('NVDA')).toBeInTheDocument();
    expect(screen.getByText('TSLA')).toBeInTheDocument();
    expect(screen.getByText('+2.31%')).toBeInTheDocument();
    expect(screen.getByText('-1.02%')).toBeInTheDocument();
    // Rows show pct only — the hero-style combined form must not appear
    expect(screen.queryByText('+5.27 (+2.31%)')).not.toBeInTheDocument();
  });

  it('splits the after-hours move onto its own line in the hero', () => {
    render(<InlineQuoteCard artifact={quoteArtifact([NVDA_AFTER_HOURS])} />);

    // Main price is the regular close with the session change…
    expect(screen.getByText('233.45')).toBeInTheDocument();
    expect(screen.getByText('+5.27 (+2.31%)')).toBeInTheDocument();
    // …and the extended move renders separately with the status badge.
    expect(screen.getByText(AFTER_HOURS_LONG)).toBeInTheDocument();
    expect(screen.getByText('235.30')).toBeInTheDocument();
    expect(screen.getByText('+1.85 (+0.79%)')).toBeInTheDocument();
    expect(screen.getByText('After-Hours')).toBeInTheDocument();
  });

  it('shows the after-hours chip on rows', () => {
    render(<InlineQuoteCard artifact={quoteArtifact([NVDA_AFTER_HOURS, TSLA_REGULAR])} />);

    expect(screen.getByText(/AH 235.30/)).toBeInTheDocument();
  });

  it('degrades to the blended change on the FMP fallback shape', () => {
    render(<InlineQuoteCard artifact={quoteArtifact([FMP_ONLY])} />);

    expect(screen.getByText('190.99')).toBeInTheDocument();
    expect(screen.getByText('+0.27 (+0.14%)')).toBeInTheDocument();
    expect(screen.queryByText('After-Hrs')).not.toBeInTheDocument();
    expect(screen.queryByText('Pre-Mkt')).not.toBeInTheDocument();
  });

  it('shows the venue-local clock for non-US listings', () => {
    // Hero: the market-local clock replaces the ET as-of.
    const { unmount } = render(<InlineQuoteCard artifact={quoteArtifact([HK_CLOSED])} />);
    expect(screen.getByText('2026-07-14 23:05:12 HKT')).toBeInTheDocument();
    expect(screen.queryByText('2026-07-14 14:32:05 ET')).not.toBeInTheDocument();
    unmount();

    // Rows: header keeps the ET stamp; the HK row carries its own local clock.
    render(<InlineQuoteCard artifact={quoteArtifact([HK_CLOSED, NVDA_REGULAR])} />);
    expect(screen.getByText('2026-07-14 14:32:05 ET')).toBeInTheDocument();
    expect(screen.getByText('2026-07-14 23:05:12 HKT')).toBeInTheDocument();
  });

  it('returns null for an empty quotes list', () => {
    const { container } = render(<InlineQuoteCard artifact={{ type: 'quote', quotes: [] }} />);
    expect(container.firstChild).toBeNull();
  });

  it('propagates onClick from both layouts', () => {
    const onClick = vi.fn();
    const { unmount } = render(
      <InlineQuoteCard artifact={quoteArtifact([NVDA_REGULAR])} onClick={onClick} />,
    );
    fireEvent.click(screen.getByText('233.45'));
    expect(onClick).toHaveBeenCalledTimes(1);
    unmount();

    render(<InlineQuoteCard artifact={quoteArtifact([NVDA_REGULAR, TSLA_REGULAR])} onClick={onClick} />);
    fireEvent.click(screen.getByText('Live Quotes'));
    expect(onClick).toHaveBeenCalledTimes(2);
  });
});
