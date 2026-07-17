import { render, screen, fireEvent } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import { InlineMarketOverviewCard } from '../InlineArtifactCards';

// The composite `market_overview` artifact nests the FULL legacy
// market_indices / sector_performance artifacts verbatim under `indices` /
// `sectors`. Build minimal versions of each (neutral placeholder symbols only,
// no prod data) so the nested cards have just enough to render their headers.
const INDICES_ARTIFACT = {
  type: 'market_indices',
  indices: {
    '^GSPC': {
      name: 'S&P 500',
      ohlcv: [{ close: 5000 }, { close: 5100 }],
      stats: { period_change_pct: 1.5 },
    },
  },
};

const SECTORS_ARTIFACT = {
  type: 'sector_performance',
  sectors: [
    { sector: 'Technology', changePercentage: 2.1 },
    { sector: 'Energy', changePercentage: -0.8 },
  ],
};

const US_ARTIFACT = {
  type: 'market_overview',
  region: 'us',
  indices: INDICES_ARTIFACT,
  sectors: SECTORS_ARTIFACT,
};

const NON_US_ARTIFACT = {
  type: 'market_overview',
  region: 'jp',
  indices: {
    type: 'market_indices',
    indices: {
      '^N225': {
        name: 'Nikkei 225',
        ohlcv: [{ close: 39000 }],
        stats: { period_change_pct: 0.5 },
      },
    },
  },
};

// The unknown-region error path from the backend: no `indices`, no `sectors`.
const ERROR_ARTIFACT = { type: 'market_overview', region: 'xx' };

describe('InlineMarketOverviewCard', () => {
  it('renders both nested cards when indices and sectors are present (US)', () => {
    render(<InlineMarketOverviewCard artifact={US_ARTIFACT} />);

    // Indices card header + a rendered index row.
    expect(screen.getByText('Market Indices')).toBeInTheDocument();
    expect(screen.getByText('S&P 500')).toBeInTheDocument();
    // Sectors card header (the bar chart itself needs a measured width, which
    // jsdom's no-op ResizeObserver never supplies, but the header always shows).
    expect(screen.getByText('Sector Performance')).toBeInTheDocument();
  });

  it('renders only the indices card for a non-US artifact (no sectors key)', () => {
    render(<InlineMarketOverviewCard artifact={NON_US_ARTIFACT} />);

    expect(screen.getByText('Market Indices')).toBeInTheDocument();
    expect(screen.getByText('Nikkei 225')).toBeInTheDocument();
    expect(screen.queryByText('Sector Performance')).not.toBeInTheDocument();
  });

  it('never renders nothing — the error-path artifact shows a region fallback', () => {
    const { container } = render(<InlineMarketOverviewCard artifact={ERROR_ARTIFACT} />);

    // The tool call must stay visible: the uppercased region label is shown and
    // the component does not collapse to null.
    expect(container.firstChild).not.toBeNull();
    expect(screen.getByText('XX')).toBeInTheDocument();
    // The nested cards must NOT appear when their data is absent.
    expect(screen.queryByText('Market Indices')).not.toBeInTheDocument();
    expect(screen.queryByText('Sector Performance')).not.toBeInTheDocument();
  });

  it('falls back to the region card when nested data is degenerate (empty indices)', () => {
    const degenerate = {
      type: 'market_overview',
      region: 'us',
      indices: { type: 'market_indices', indices: {} },
    };
    const { container } = render(<InlineMarketOverviewCard artifact={degenerate} />);

    expect(container.firstChild).not.toBeNull();
    expect(screen.getByText('US')).toBeInTheDocument();
    expect(screen.queryByText('Market Indices')).not.toBeInTheDocument();
  });

  it('propagates onClick from a nested card', () => {
    const onClick = vi.fn();
    render(<InlineMarketOverviewCard artifact={US_ARTIFACT} onClick={onClick} />);

    // Clicking anywhere inside a nested card bubbles to its card-level handler.
    fireEvent.click(screen.getByText('Market Indices'));
    expect(onClick).toHaveBeenCalledTimes(1);
  });

  it('propagates onClick from the region fallback card', () => {
    const onClick = vi.fn();
    render(<InlineMarketOverviewCard artifact={ERROR_ARTIFACT} onClick={onClick} />);

    fireEvent.click(screen.getByText('XX'));
    expect(onClick).toHaveBeenCalledTimes(1);
  });
});
