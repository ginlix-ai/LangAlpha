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

  it('asks for the currency of a listing on an unknown venue that search names none for', async () => {
    hits = [{ symbol: 'PETR4.SA', name: 'Petrobras' }];
    const onAdd = vi.fn();
    const alert = vi.spyOn(window, 'alert').mockImplementation(() => {});
    renderWithProviders(<AddPortfolioHoldingDialog open onAdd={onAdd} onClose={() => {}} />);
    fireEvent.click(await screen.findByText('PETR4.SA'));

    const [quantity, cost] = document.querySelectorAll('input[type="number"]');
    fireEvent.change(quantity, { target: { value: '100' } });
    fireEvent.change(cost, { target: { value: '45.78' } });
    fireEvent.click(screen.getByRole('button', { name: 'Add to Portfolio' }));
    expect(onAdd).not.toHaveBeenCalled();
    expect(alert).toHaveBeenCalled();

    fireEvent.change(screen.getByPlaceholderText('e.g. BRL'), { target: { value: 'ABC' } });
    fireEvent.click(screen.getByRole('button', { name: 'Add to Portfolio' }));
    expect(onAdd).not.toHaveBeenCalled();

    fireEvent.change(screen.getByPlaceholderText('e.g. BRL'), { target: { value: 'brl' } });
    expect(screen.getByText(/Avg Cost \(BRL\)/)).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Add to Portfolio' }));
    expect(onAdd).toHaveBeenCalledWith(expect.objectContaining({ symbol: 'PETR4.SA', currency: 'BRL' }));
    alert.mockRestore();
  });

  it('takes the currency search names for a listing on an unknown venue', async () => {
    hits = [{ symbol: 'PETR4.SA', name: 'Petrobras', currency: 'BRL' }];
    renderWithProviders(<AddPortfolioHoldingDialog open onAdd={vi.fn()} onClose={() => {}} />);
    fireEvent.click(await screen.findByText('PETR4.SA'));

    expect(screen.getByText(/Avg Cost \(BRL\)/)).toBeInTheDocument();
    expect(screen.queryByPlaceholderText('e.g. BRL')).not.toBeInTheDocument();
  });
});
