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

// Freshness-bearing rows (the backend contract that gained `tier` / `freshness`).
const NVDA_REALTIME = {
  ...NVDA_REGULAR,
  tier: 'realtime',
  freshness: { label: 'live', measured: true, lag_s: 3, source: 'ginlix-data' },
};

const TSLA_DELAYED_DECLARED = {
  ...TSLA_REGULAR,
  tier: 'delayed_15m',
  freshness: { label: 'unknown', measured: false, source: 'fmp' },
};

const quoteArtifact = (quotes: Record<string, unknown>[]) => ({
  type: 'quote',
  quotes,
  as_of: '2026-07-14 14:32:05 ET',
  as_of_ts: 1784140325000,
});

describe('InlineQuoteCard', () => {
  it('prints the row currency when the row carries one, sign first', () => {
    render(<InlineQuoteCard artifact={quoteArtifact([{ ...HK_CLOSED, currency: 'HKD' }])} />);
    expect(screen.getByText('HK$454.20')).toBeInTheDocument();
    expect(screen.getByText('-HK$3.40 (-0.74%)')).toBeInTheDocument();
  });

  it('prints an index level bare though its venue stamps a currency', () => {
    // get_quote stamps a venue-listed index with its venue currency.
    const venueIndex = {
      symbol: '000300.SH', name: 'Benchmark 300', currency: 'CNY',
      price: 4439.14, change: -78.95, change_percent: -1.75, low: 4420.1, high: 4510.3, market_status: 'closed',
    };
    const { unmount } = render(<InlineQuoteCard artifact={quoteArtifact([venueIndex])} />);
    expect(screen.getByText('4,439.14')).toBeInTheDocument();
    expect(screen.getByText('-78.95 (-1.75%)')).toBeInTheDocument();
    expect(screen.queryAllByText(/¥/)).toHaveLength(0);
    unmount();

    // The wire's asset class decides where a row carries one.
    render(<InlineQuoteCard artifact={quoteArtifact([{ ...NVDA_REGULAR, symbol: 'ACMEX', asset_class: 'index', currency: 'USD', price: 1234.5 }])} />);
    expect(screen.getByText('1,234.50')).toBeInTheDocument();
    expect(screen.queryAllByText(/\$/)).toHaveLength(0);
  });

  it('prints the ticker once when the name only repeats it', () => {
    render(<InlineQuoteCard artifact={quoteArtifact([{ ...NVDA_REGULAR, name: 'NVDA' }])} />);
    expect(screen.getAllByText('NVDA')).toHaveLength(1);
  });

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

  it('leads a closed venue with the regular close, not the after-hours print', () => {
    // After 20:00 ET the row reads closed while its last trade is the
    // after-hours print; the card showed that print under a "Last close" badge.
    const closed = { label: 'live', measured: true, lag_s: 0, closed: true };
    render(
      <InlineQuoteCard
        artifact={quoteArtifact([{ ...NVDA_AFTER_HOURS, market_status: 'closed', tier: 'realtime', freshness: closed }])}
      />,
    );

    expect(screen.getByText('233.45')).toBeInTheDocument();
    expect(screen.getByText('+5.27 (+2.31%)')).toBeInTheDocument();
    expect(screen.getByText(/Last close/)).toBeInTheDocument();
    expect(screen.getByText(AFTER_HOURS_LONG)).toBeInTheDocument();
    expect(screen.getByText('235.30')).toBeInTheDocument();
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
  it('keeps "Live Quotes" and shows no freshness badge for a pre-contract artifact', () => {
    render(<InlineQuoteCard artifact={quoteArtifact([NVDA_REGULAR, TSLA_REGULAR])} />);

    expect(screen.getByText('Live Quotes')).toBeInTheDocument();
    expect(screen.queryByText('Quotes')).not.toBeInTheDocument();
    expect(screen.queryByText(/Delayed/)).not.toBeInTheDocument();
    expect(screen.queryByText('Last close')).not.toBeInTheDocument();
    expect(screen.queryByText('Stale')).not.toBeInTheDocument();
  });

  it('keeps "Live Quotes" when every row measures live', () => {
    render(
      <InlineQuoteCard
        artifact={{ ...quoteArtifact([NVDA_REALTIME, { ...TSLA_REGULAR, tier: 'realtime' }]), all_realtime: true }}
      />,
    );

    expect(screen.getByText('Live Quotes')).toBeInTheDocument();
    expect(screen.queryByText(/Delayed/)).not.toBeInTheDocument();
  });

  it('demotes the heading to "Quotes" and badges only the delayed row', () => {
    render(<InlineQuoteCard artifact={quoteArtifact([NVDA_REALTIME, TSLA_DELAYED_DECLARED])} />);

    expect(screen.getByText('Quotes')).toBeInTheDocument();
    expect(screen.queryByText('Live Quotes')).not.toBeInTheDocument();

    // Rows carry a dot only; the state rides the dot's accessible name.
    const delayed = screen.getAllByLabelText(/Delayed 15 min/);
    expect(delayed).toHaveLength(1);
    const row = delayed[0].closest('div')?.parentElement;
    expect(row?.textContent).toContain('TSLA');
    expect(row?.textContent).not.toContain('NVDA');
    expect(screen.getAllByLabelText(/Realtime/)).toHaveLength(1);
  });

  it('reads a closed venue as the last close, never live', () => {
    const closed = { label: 'live', measured: true, lag_s: 0, closed: true };
    render(
      <InlineQuoteCard
        artifact={{
          ...quoteArtifact([
            { ...NVDA_REGULAR, tier: 'realtime', freshness: closed },
            { ...TSLA_REGULAR, tier: 'realtime', freshness: closed },
          ]),
          all_realtime: false,
        }}
      />,
    );

    expect(screen.getByText('Quotes')).toBeInTheDocument();
    expect(screen.queryByText('Live Quotes')).not.toBeInTheDocument();
    expect(screen.getAllByLabelText(/Last close/)).toHaveLength(2);
    expect(screen.queryByLabelText(/Realtime/)).not.toBeInTheDocument();
  });

  it('prints each row in its listing currency with both names, and the reader clock for a mixed block', () => {
    const asOfTs = Date.UTC(2026, 8, 27, 1, 0);
    render(
      <InlineQuoteCard
        artifact={{
          type: 'quote',
          as_of: null,
          as_of_ts: asOfTs,
          quotes: [
            { ...TSLA_REGULAR, symbol: 'ACME.HK', name: 'Acme Holdings', name_local: '甲公司', name_en: 'Acme Holdings', currency: 'HKD', price: 436.6 },
            { ...NVDA_REGULAR, currency: 'USD' },
          ],
        }}
      />,
    );

    expect(screen.getByText('HK$436.60')).toBeInTheDocument();
    expect(screen.getByText('甲公司')).toBeInTheDocument();
    expect(screen.getByText('Acme Holdings')).toBeInTheDocument();
    // No shared venue clock: the retrieval instant renders in the reader's time.
    expect(screen.getByText(new Intl.DateTimeFormat('en-US', {
      year: 'numeric', month: 'numeric', day: 'numeric', hour: 'numeric', minute: 'numeric', second: 'numeric',
    }).format(asOfTs))).toBeInTheDocument();
  });

  it('shows the time once: in the badge when it carries the stamp, plain otherwise', () => {
    const HK_STAMP = '2026-07-14 23:05:12 HKT';
    const BLOCK_STAMP = '2026-07-14 14:32:05 ET';
    const noBadge = { label: 'unknown', measured: false, source: 'fmp' };

    // Freshness that resolves to no badge leaves the stamp where it was.
    const { unmount } = render(<InlineQuoteCard artifact={quoteArtifact([{ ...HK_CLOSED, freshness: noBadge }])} />);
    expect(screen.getByText(HK_STAMP)).toBeInTheDocument();
    unmount();

    // A badge with no venue stamp to carry: the block's own time stays.
    const { unmount: unmountUs } = render(<InlineQuoteCard artifact={quoteArtifact([NVDA_REALTIME])} />);
    expect(screen.getByText('Realtime')).toBeInTheDocument();
    expect(screen.getByText(BLOCK_STAMP)).toBeInTheDocument();
    unmountUs();

    // A badge that carries the stamp takes it into its tooltip.
    const { unmount: unmountHk } = render(
      <InlineQuoteCard artifact={quoteArtifact([{ ...HK_CLOSED, tier: 'delayed_15m', freshness: { label: 'delayed', measured: false } }])} />,
    );
    expect(screen.getByText('Delayed 15 min')).toBeInTheDocument();
    expect(screen.queryByText(HK_STAMP)).not.toBeInTheDocument();
    unmountHk();

    // Rows follow the same rule.
    render(<InlineQuoteCard artifact={quoteArtifact([{ ...HK_CLOSED, freshness: noBadge }, NVDA_REGULAR])} />);
    expect(screen.getByText(HK_STAMP)).toBeInTheDocument();
  });

  it('badges the hero with the measured lag', () => {
    render(
      <InlineQuoteCard
        artifact={quoteArtifact([
          { ...NVDA_REGULAR, tier: 'delayed_15m', freshness: { label: 'delayed', measured: true, lag_s: 754 } },
        ])}
      />,
    );

    expect(screen.getByText('Delayed 13 min')).toBeInTheDocument();
  });
});
