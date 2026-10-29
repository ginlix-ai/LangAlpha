import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { InlineStockScreenerCard } from '../InlineScreenerCard';

// Placeholder symbols and fabricated figures, in the shape screener.py sends.
const artifact = {
  type: 'stock_screener',
  results: [
    { symbol: 'ACME', companyName: 'Acme Corp', price: 186.26, marketCap: 4.53e12, change: 1.7 },
    { symbol: 'BETA', companyName: 'Beta Inc', price: 51.2, marketCap: 838.2e9, change: -0.42 },
    // FMP's screen sends no change at all.
    { symbol: 'GAMA', companyName: 'Gamma Ltd', price: 20, marketCap: 12e9 },
  ],
  filters: { Sector: 'Technology', 'Mkt Cap >': '$10.00B' },
  count: 3,
};

describe('InlineStockScreenerCard', () => {
  it('draws the change from the percent the producer sends as `change`', () => {
    const { container } = render(<InlineStockScreenerCard artifact={artifact} />);

    expect(screen.getByText('1.70%').closest('.term-chg')).toHaveAttribute('data-dir', 'up');
    expect(screen.getByText('0.42%').closest('.term-chg')).toHaveAttribute('data-dir', 'down');
    expect(container.querySelectorAll('.term-chg')).toHaveLength(2);
  });

  it('prints a trillion-dollar cap with a T, not as thousands of billions', () => {
    render(<InlineStockScreenerCard artifact={artifact} />);

    // As the panel prints a cap: in its currency, two decimals at any scale.
    expect(screen.getByText('$4.53T')).toBeInTheDocument();
    expect(screen.getByText('$838.20B')).toBeInTheDocument();
  });

  it('reads a filter as a match or a threshold, with no stray colon', () => {
    const { container } = render(<InlineStockScreenerCard artifact={artifact} />);

    const chips = [...container.querySelectorAll('.term-chip')].map((c) => c.textContent);
    expect(chips).toEqual(['Sector = Technology', 'Mkt Cap > $10.00B']);
  });
});
