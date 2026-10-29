/**
 * The brokerage-less way in: search a ticker, then add it to holdings with a
 * quantity or to the dashboard's watchlist without one. The search hook and both
 * list APIs are stubbed; what is pinned is the payloads and the keyboard path.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, screen, fireEvent, waitFor } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import type { StockSearchHit } from '@/lib/marketUtils';

const hits: StockSearchHit[] = [
  { symbol: 'AAPL', name: 'Apple Inc.', exchangeShortName: 'NASDAQ', currency: 'USD' },
  { symbol: 'AAPD', name: 'Direxion Daily AAPL Bear', exchangeShortName: 'NASDAQ', currency: 'USD' },
];
vi.mock('@/hooks/useSymbolSearch', () => ({
  useSymbolSearch: (q: string) => ({ hits: q.trim() ? hits : [], loading: false }),
}));

// What the list endpoints hand back, which the tests change to show a row
// appearing once its add has been invalidated.
let holdingRows: unknown[] = [];
let watchRows: unknown[] = [];
const addPortfolioHolding = vi.fn(async (..._a: unknown[]) => ({}));
const deletePortfolioHolding = vi.fn(async (..._a: unknown[]) => {});
vi.mock('@/pages/Dashboard/utils/portfolio', () => ({
  addPortfolioHolding: (...a: unknown[]) => addPortfolioHolding(...a),
  deletePortfolioHolding: (...a: unknown[]) => deletePortfolioHolding(...a),
  listPortfolio: vi.fn(async () => ({ holdings: [] })),
}));
const addWatchlistItem = vi.fn(async (..._a: unknown[]) => ({}));
const deleteWatchlistItem = vi.fn(async (..._a: unknown[]) => {});
vi.mock('@/pages/Dashboard/utils/watchlistItems', () => ({
  addWatchlistItem: (...a: unknown[]) => addWatchlistItem(...a),
  deleteWatchlistItem: (...a: unknown[]) => deleteWatchlistItem(...a),
}));
const toast = vi.fn();
vi.mock('@/components/ui/use-toast', () => ({ toast: (...a: unknown[]) => toast(...a) }));
vi.mock('@/pages/Dashboard/utils/api', () => ({
  getPortfolio: vi.fn(async () => ({ holdings: holdingRows })),
  // Not the `default` alias: the list the dashboard resolves is the one
  // written to, so a row added here is one the dashboard shows.
  listWatchlists: vi.fn(async () => ({ watchlists: [{ watchlist_id: 'wl-main' }] })),
  listWatchlistItems: vi.fn(async () => ({ items: watchRows })),
  addWatchlistItem: vi.fn(),
  deleteWatchlistItem: vi.fn(),
}));

import { HoldingsEntry } from '../welcome/steps/HoldingsEntry';

function search(text: string) {
  const input = screen.getByTestId('welcome-holdings-search');
  fireEvent.focus(input);
  fireEvent.change(input, { target: { value: text } });
  return input;
}

// The two lists are read on mount; an add that lands before they settle is
// folded into that first read instead of refetching, so wait it out.
const listsLoaded = () => act(async () => new Promise((r) => setTimeout(r, 0)));

describe('HoldingsEntry', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    holdingRows = [];
    watchRows = [];
  });

  it('adds a picked symbol to holdings with its quantity', async () => {
    renderWithProviders(<HoldingsEntry />);
    await listsLoaded();
    search('aap');
    fireEvent.mouseDown(screen.getAllByRole('option')[0]);
    fireEvent.change(screen.getByTestId('welcome-holdings-quantity'), { target: { value: '25' } });
    holdingRows = [{ user_portfolio_id: 'p1', symbol: 'AAPL', quantity: 25 }];
    fireEvent.click(screen.getByTestId('welcome-holdings-to-holdings'));
    const list = await screen.findByTestId('welcome-holdings-list');
    expect(list).toHaveTextContent('AAPL');
    expect(list).toHaveTextContent('25');
    expect(addPortfolioHolding).toHaveBeenCalledWith({
      symbol: 'AAPL',
      instrument_type: 'stock',
      quantity: 25,
      exchange: 'NASDAQ',
      currency: 'USD',
    });
  });

  it("adds a picked symbol to the dashboard's watchlist without a quantity", async () => {
    renderWithProviders(<HoldingsEntry />);
    await listsLoaded();
    search('aap');
    fireEvent.mouseDown(screen.getAllByRole('option')[0]);
    watchRows = [{ watchlist_item_id: 'w1', symbol: 'AAPL' }];
    fireEvent.click(screen.getByTestId('welcome-holdings-to-watchlist'));
    expect(await screen.findByTestId('welcome-watchlist-list')).toHaveTextContent('AAPL');
    expect(addWatchlistItem).toHaveBeenCalledWith('wl-main', {
      symbol: 'AAPL',
      instrument_type: 'stock',
      exchange: 'NASDAQ',
      name: 'Apple Inc.',
    });
    expect(addPortfolioHolding).not.toHaveBeenCalled();
  });

  it('keeps the holdings button disabled until there is a quantity', () => {
    renderWithProviders(<HoldingsEntry />);
    search('aap');
    fireEvent.mouseDown(screen.getAllByRole('option')[0]);
    const button = screen.getByTestId('welcome-holdings-to-holdings');
    expect(button).toBeDisabled();
    fireEvent.change(screen.getByTestId('welcome-holdings-quantity'), { target: { value: '0' } });
    expect(button).toBeDisabled();
    fireEvent.change(screen.getByTestId('welcome-holdings-quantity'), { target: { value: '3' } });
    expect(button).toBeEnabled();
  });

  it('picks the first hit with ArrowDown then Enter', () => {
    renderWithProviders(<HoldingsEntry />);
    const input = search('aap');
    expect(input).toHaveAttribute('aria-expanded', 'true');
    fireEvent.keyDown(input, { key: 'ArrowDown' });
    // The list starts on its first row, so one step lands on the second.
    expect(input.getAttribute('aria-activedescendant')).toBe(screen.getAllByRole('option')[1].id);
    fireEvent.keyDown(input, { key: 'ArrowUp' });
    fireEvent.keyDown(input, { key: 'Enter' });
    expect(screen.queryByRole('listbox')).toBeNull();
    expect(screen.getByTestId('welcome-holdings-quantity')).toBeInTheDocument();
    expect(screen.getByText('AAPL')).toBeInTheDocument();
  });

  it('says so when a remove fails, and keeps the row', async () => {
    holdingRows = [{ user_portfolio_id: 'p1', symbol: 'AAPL', quantity: 25 }];
    watchRows = [{ watchlist_item_id: 'w1', symbol: 'MSFT' }];
    renderWithProviders(<HoldingsEntry />);
    deletePortfolioHolding.mockRejectedValueOnce(new Error('nope'));
    fireEvent.click(await screen.findByRole('button', { name: 'Remove AAPL' }));
    expect(await screen.findByRole('alert')).toHaveTextContent("Couldn't remove AAPL");
    expect(screen.getByTestId('welcome-holdings-list')).toHaveTextContent('AAPL');

    deleteWatchlistItem.mockRejectedValueOnce(new Error('nope'));
    fireEvent.click(await screen.findByRole('button', { name: 'Remove MSFT from the watchlist' }));
    await waitFor(() => expect(screen.getByRole('alert')).toHaveTextContent("Couldn't remove MSFT"));
    expect(screen.getByTestId('welcome-watchlist-list')).toHaveTextContent('MSFT');
    expect(deleteWatchlistItem).toHaveBeenCalledWith('wl-main', 'w1');
  });

  it('hands each add and remove to the flow as it starts, settled once its list is read again', async () => {
    holdingRows = [{ user_portfolio_id: 'p1', symbol: 'MSFT', quantity: 5 }];
    watchRows = [{ watchlist_item_id: 'w1', symbol: 'NVDA' }];
    const onPreparing = vi.fn((_work: Promise<void>) => {});
    renderWithProviders(<HoldingsEntry onPreparing={onPreparing} />);
    await listsLoaded();
    let land: () => void = () => {};
    addPortfolioHolding.mockImplementationOnce(() => new Promise((resolve) => (land = () => resolve({}))));
    search('aap');
    fireEvent.mouseDown(screen.getAllByRole('option')[0]);
    fireEvent.change(screen.getByTestId('welcome-holdings-quantity'), { target: { value: '25' } });
    fireEvent.click(screen.getByTestId('welcome-holdings-to-holdings'));
    expect(onPreparing).toHaveBeenCalledTimes(1);
    let settled = false;
    void onPreparing.mock.calls[0][0].then(() => (settled = true));
    await act(async () => new Promise((r) => setTimeout(r, 0)));
    expect(settled).toBe(false);

    holdingRows = [...holdingRows, { user_portfolio_id: 'p2', symbol: 'AAPL', quantity: 25 }];
    await act(async () => land());
    await act(async () => onPreparing.mock.calls[0][0]);
    expect(settled).toBe(true);
    expect(screen.getByTestId('welcome-holdings-list')).toHaveTextContent('AAPL');

    search('aap');
    fireEvent.mouseDown(screen.getAllByRole('option')[0]);
    fireEvent.click(screen.getByTestId('welcome-holdings-to-watchlist'));
    fireEvent.click(screen.getByRole('button', { name: 'Remove MSFT' }));
    fireEvent.click(screen.getByRole('button', { name: 'Remove NVDA from the watchlist' }));
    expect(onPreparing).toHaveBeenCalledTimes(4);
    await act(async () => Promise.all(onPreparing.mock.calls.map(([work]) => work)));
    expect(addWatchlistItem).toHaveBeenCalledTimes(1);
    expect(deletePortfolioHolding).toHaveBeenCalledWith('p1');
    expect(deleteWatchlistItem).toHaveBeenCalledWith('wl-main', 'w1');
  });

  it('says a failure under the search while it is shown, and in a toast once it has gone', async () => {
    const { unmount } = renderWithProviders(<HoldingsEntry />);
    await listsLoaded();
    addPortfolioHolding.mockRejectedValueOnce(new Error('nope'));
    search('aap');
    fireEvent.mouseDown(screen.getAllByRole('option')[0]);
    fireEvent.change(screen.getByTestId('welcome-holdings-quantity'), { target: { value: '25' } });
    fireEvent.click(screen.getByTestId('welcome-holdings-to-holdings'));
    expect(await screen.findByRole('alert')).toHaveTextContent("Couldn't add AAPL.");
    expect(toast).not.toHaveBeenCalled();

    // Tried again, and left while it is still being written, as Continue
    // leaves it.
    let refuse: (err: Error) => void = () => {};
    addPortfolioHolding.mockImplementationOnce(() => new Promise((_resolve, reject) => (refuse = reject)));
    fireEvent.click(screen.getByTestId('welcome-holdings-to-holdings'));
    expect(addPortfolioHolding).toHaveBeenCalledTimes(2);
    unmount();
    await act(async () => refuse(new Error('nope')));
    expect(toast).toHaveBeenCalledTimes(1);
    expect(toast.mock.calls[0][0]).toMatchObject({ variant: 'destructive', description: "Couldn't add AAPL." });
  });
});
