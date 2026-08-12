import { describe, expect, it } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import IndexMovementCard from '../IndexMovementCard';
import type { IndexData } from '@/types/market';

const baseIndex = (over: Partial<IndexData> = {}): IndexData => ({
  symbol: 'GSPC',
  name: 'S&P 500',
  price: 5000.12,
  change: 12.34,
  changePercent: 0.25,
  isPositive: true,
  sparklineData: [
    { time: '09:30', val: 4990 },
    { time: '16:00', val: 5000.12 },
  ],
  ...over,
});

const renderCard = (index: IndexData) =>
  render(
    <MemoryRouter>
      <IndexMovementCard indices={[index]} />
    </MemoryRouter>,
  );

// fmt2 is locale-aware; strip grouping separators before comparing.
const noGrouping = (expected: string) => (content: string) =>
  content.replace(/[\s,]/g, '') === expected;

describe('IndexMovementCard', () => {
  it('renders the formatted price when a quote is available', () => {
    renderCard(baseIndex({ quoteAvailable: true }));
    expect(screen.getByText(noGrouping('5000.12'))).toBeInTheDocument();
    expect(screen.queryByText('N/A')).not.toBeInTheDocument();
  });

  it('renders the price when quoteAvailable is undefined (backward compat)', () => {
    // Pre-existing IndexData has no quoteAvailable field; the `!== false`
    // sentinel must treat undefined as "has quote" so old data still renders.
    renderCard(baseIndex());
    expect(screen.getByText(noGrouping('5000.12'))).toBeInTheDocument();
    expect(screen.queryByText('N/A')).not.toBeInTheDocument();
  });

  it('masks price and change as N/A when the quote is unavailable', () => {
    renderCard(
      baseIndex({ quoteAvailable: false, price: 0, change: 0, changePercent: 0 }),
    );
    // both the price and the change/percent line render N/A
    expect(screen.getAllByText('N/A')).toHaveLength(2);
    expect(screen.queryByText(noGrouping('0.00'))).not.toBeInTheDocument();
  });

  it('labels the card with the asOfDate session date (M/D), not today', () => {
    renderCard(baseIndex({ asOfDate: '2025-01-13' }));
    expect(screen.getByText('1/13')).toBeInTheDocument();
  });

  it('falls back to a valid current date label when asOfDate is missing', () => {
    renderCard(baseIndex({ asOfDate: undefined }));
    const label = screen.getByText(/^\d{1,2}\/\d{1,2}$/);
    expect(label.textContent).not.toContain('NaN');
  });

  it('falls back to a valid current date label when asOfDate is malformed', () => {
    renderCard(baseIndex({ asOfDate: 'not-a-date' }));
    const label = screen.getByText(/^\d{1,2}\/\d{1,2}$/);
    expect(label.textContent).not.toContain('NaN');
  });
});
