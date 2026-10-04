import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { InlineCompanyOverviewCard } from '../InlineArtifactCards';

// Neutral placeholder names and fabricated numbers; `^GSPC` is a public
// benchmark spelling.
const INDEX_OVERVIEW = {
  type: 'company_overview',
  symbol: '^GSPC',
  name: 'S&P 500',
  currency: 'USD',
  assetClass: 'index',
  quote: {
    price: 5812.34, change: 12.3, changePct: 0.21,
    open: 5800, previousClose: 5800.04, dayLow: 5790.1, dayHigh: 5820.5,
    yearLow: 4950.2, yearHigh: 5880.75,
  },
};

const EQUITY_OVERVIEW = {
  type: 'company_overview',
  symbol: 'ACME.HK',
  name: '甲公司',
  nameEn: 'Acme Holdings',
  currency: 'HKD',
  reportedCurrency: 'CNY',
  quote: { price: 436.6, change: -3.4, changePct: -0.77, marketCap: 4.1e12 },
};

describe('InlineCompanyOverviewCard', () => {
  it('prints an index level in points, never as dollars', () => {
    render(<InlineCompanyOverviewCard artifact={INDEX_OVERVIEW} />);

    expect(screen.getByText('5,812.34')).toBeInTheDocument();
    expect(screen.getByText('+12.30 (+0.21%)')).toBeInTheDocument();
    expect(screen.getByText('4,950.20 - 5,880.75')).toBeInTheDocument();
    expect(screen.queryAllByText(/\$/)).toHaveLength(0);
  });

  it('leads with the local name, the English one beside it, then the ticker', () => {
    render(<InlineCompanyOverviewCard artifact={EQUITY_OVERVIEW} />);

    expect(screen.getByText('甲公司')).toBeInTheDocument();
    expect(screen.getByText('Acme Holdings')).toBeInTheDocument();
    expect(screen.getByText('ACME.HK')).toBeInTheDocument();
    expect(screen.getByText('HK$436.60')).toBeInTheDocument();
    expect(screen.getByText('-HK$3.40 (-0.77%)')).toBeInTheDocument();
  });

  it('prints an unnamed listing\'s ticker once', () => {
    render(<InlineCompanyOverviewCard artifact={{ ...EQUITY_OVERVIEW, name: null, nameEn: null }} />);

    expect(screen.getAllByText('ACME.HK')).toHaveLength(1);
  });
});
