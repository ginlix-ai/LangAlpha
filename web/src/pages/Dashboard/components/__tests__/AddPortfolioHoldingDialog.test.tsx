import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { StockSearchHit } from '@/lib/marketUtils';
import { fireEvent, screen } from '@testing-library/react';
import { renderWithProviders } from '../../../../test/utils';
import AddPortfolioHoldingDialog from '../AddPortfolioHoldingDialog';

let hits: StockSearchHit[] = [];
vi.mock('@/hooks/useSymbolSearch', () => ({
  useSymbolSearch: () => ({ hits, loading: false }),
}));

describe('AddPortfolioHoldingDialog', () => {
  beforeEach(() => {
    hits = [{ symbol: 'VOD.L', name: 'Vodafone Group Plc', currency: 'GBp', exchangeShortName: 'LSE' }];
  });

  it('enters a London cost in pounds, the unit the app quotes it in, though search names pence', async () => {
    const onAdd = vi.fn();
    renderWithProviders(<AddPortfolioHoldingDialog open onAdd={onAdd} onClose={() => {}} />);
    fireEvent.click(await screen.findByText('VOD.L'));

    expect(screen.getByText(/Avg Cost \(GBP\)/)).toBeInTheDocument();
    const [quantity, cost] = document.querySelectorAll('input[type="number"]');
    expect(cost).toHaveAttribute('placeholder', 'e.g. 1.75');
    fireEvent.change(quantity, { target: { value: '1000' } });
    fireEvent.change(cost, { target: { value: '1.10' } });
    fireEvent.click(screen.getByRole('button', { name: 'Add to Portfolio' }));

    expect(onAdd).toHaveBeenCalledWith(expect.objectContaining({ symbol: 'VOD.L', average_cost: '1.1', currency: 'GBP' }));
  });

  it('keeps the dollars of a London line that trades in dollars', async () => {
    hits = [{ symbol: 'CSPX.L', name: 'iShares Core S&P 500 UCITS ETF', currency: 'USD', exchangeShortName: 'LSE' }];
    const onAdd = vi.fn();
    renderWithProviders(<AddPortfolioHoldingDialog open onAdd={onAdd} onClose={() => {}} />);
    fireEvent.click(await screen.findByText('CSPX.L'));

    expect(screen.getByText(/Avg Cost \(USD\)/)).toBeInTheDocument();
    const [quantity, cost] = document.querySelectorAll('input[type="number"]');
    expect(cost).toHaveAttribute('placeholder', 'e.g. 175.50');
    fireEvent.change(quantity, { target: { value: '2' } });
    fireEvent.change(cost, { target: { value: '821.98' } });
    fireEvent.click(screen.getByRole('button', { name: 'Add to Portfolio' }));

    expect(onAdd).toHaveBeenCalledWith(expect.objectContaining({ symbol: 'CSPX.L', currency: 'USD' }));
  });
});
