import { act, render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import { afterEach, describe, expect, it } from 'vitest';

import i18n from '@/i18n';
import { CompanyOverviewCard } from '../MarketDataCharts';
import type { CompanyOverviewArtifact } from '@/lib/quotes/overview';

// Neutral placeholder names and fabricated numbers; `^GSPC` is a public
// benchmark spelling.
const INDEX_OVERVIEW: CompanyOverviewArtifact = {
  symbol: '^GSPC',
  name: 'S&P 500',
  currency: 'USD',
  assetClass: 'index',
  quote: {
    price: 5812.34, change: 12.3, changePct: 0.21,
    dayLow: 5790.1, dayHigh: 5820.5, yearLow: 4950.2, yearHigh: 5880.75,
  },
};

const EQUITY_OVERVIEW: CompanyOverviewArtifact = {
  symbol: 'ACME',
  name: 'Acme Corp',
  currency: 'USD',
  quote: {
    regularClose: 100, regularChange: 2, regularChangePct: 2.04,
    lastTradePrice: 102.5, marketStatus: 'late_trading',
    volume: 12_345_678, marketCap: 1.23e12,
  },
  float: { free_float: 9_870_000_000 },
};

const renderCard = (data: CompanyOverviewArtifact) =>
  render(
    <MemoryRouter>
      <CompanyOverviewCard data={data} />
    </MemoryRouter>,
  );

afterEach(async () => {
  await act(() => i18n.changeLanguage('en-US'));
});

describe('CompanyOverviewCard', () => {
  it('prints an index level in points, never as dollars, and the ticker once', () => {
    renderCard(INDEX_OVERVIEW);

    expect(screen.getByText('5,812.34')).toBeInTheDocument();
    expect(screen.getByText('+12.30 (+0.21%)')).toBeInTheDocument();
    expect(screen.getByText('4,950.20 - 5,880.75')).toBeInTheDocument();
    expect(screen.queryAllByText(/\$/)).toHaveLength(0);
    expect(screen.getByText('S&P 500')).toBeInTheDocument();
    expect(screen.getAllByText('^GSPC')).toHaveLength(1);
  });

  it('splits the after-hours print off the close and counts in the reader\'s locale', async () => {
    renderCard(EQUITY_OVERVIEW);

    expect(screen.getByText('$100.00')).toBeInTheDocument();
    expect(screen.getByText('+$2.00 (+2.04%)')).toBeInTheDocument();
    expect(screen.getByText('$102.50')).toBeInTheDocument();
    expect(screen.getByText('+$2.50 (+2.50%)')).toBeInTheDocument();
    expect(screen.getByText('12.35M')).toBeInTheDocument();
    expect(screen.getByText('9.87B')).toBeInTheDocument();

    await act(() => i18n.changeLanguage('zh-CN'));
    expect(screen.getByText('1234.57万')).toBeInTheDocument();
  });
});
