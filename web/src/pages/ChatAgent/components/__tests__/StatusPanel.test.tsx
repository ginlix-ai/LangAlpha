import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { screen, fireEvent, waitFor } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import type { Wakeup } from '@/types/wakeups';

let flag = true;
vi.mock('@/hooks/useFeatures', () => ({ useFeatureEnabled: () => flag }));
vi.mock('../../utils/api', () => ({ fetchWakeups: vi.fn(), cancelWakeups: vi.fn() }));

import { cancelWakeups, fetchWakeups } from '../../utils/api';
import StatusPanel from '../StatusPanel';

const mockFetch = fetchWakeups as Mock;
const mockCancel = cancelWakeups as Mock;
const THREAD = '7d1c2a4e-0b3f-4c55-9a61-2f8e5d9b0c17';

// i18n runs against the real en-US catalog (initialized in test setup), so
// assert on stable substrings: symbols, invented prices, and hint copy.
// Neutral placeholder tickers and made-up prices only.
function wakeup(over: Partial<Wakeup>): Wakeup {
  return {
    wakeup_id: 'w-1', short_id: 'w_3f2a', label: null, kind: 'price', state: 'armed',
    timezone: null, cron: null, at: null, until: null, next_fire_at: null,
    fires_done: 0, fires_total: 1, ends_at: null,
    expires_at: new Date(Date.now() + 2 * 3_600_000 + 60_000).toISOString(),
    fired_at: null,
    price: { symbol: 'NVDA', kind: 'above', value: 205, pct_from: null, armed_price: null, fired_price: null },
    order: null, cancellable: true, legacy: false, created_at: '2030-01-01T00:00:00Z',
    ...over,
  };
}

const NEXT = '2030-01-02T14:30:00Z';

const WAKEUPS: Wakeup[] = [
  wakeup({ wakeup_id: 'price' }),
  wakeup({
    wakeup_id: 'cron', short_id: 'w_91c0', label: 'Morning check', kind: 'cron', cron: '30 9 * * 1-5',
    timezone: 'America/New_York', price: null, expires_at: null, next_fire_at: NEXT, fires_done: 2, fires_total: 5,
  }),
  wakeup({
    wakeup_id: 'at', short_id: 'w_5d1e', kind: 'at', at: NEXT, timezone: 'UTC', price: null, expires_at: null,
    next_fire_at: NEXT,
  }),
  wakeup({
    wakeup_id: 'order', short_id: 'w_0be7', kind: 'order', price: null,
    order: { vendor_order_id: '7712045', summary: 'buy 100 NVDA, limit $200.00' },
  }),
  wakeup({ wakeup_id: 'held', state: 'held', cancellable: true, price: null, kind: 'at', at: NEXT, expires_at: null }),
  wakeup({ wakeup_id: 'held_usage', state: 'held_usage', cancellable: true, price: null, kind: 'at', at: NEXT, expires_at: null }),
  wakeup({ wakeup_id: 'finished', state: 'finished', cancellable: false, kind: 'cron', cron: '0 * * * *', price: null, expires_at: null }),
  wakeup({
    wakeup_id: 'fired', state: 'fired', cancellable: false, fired_at: '2030-01-01T15:05:00Z',
    price: { symbol: 'TSLA', kind: 'pct_down', value: 3, pct_from: 'previous_close', armed_price: null, fired_price: 388.4 },
  }),
  wakeup({
    wakeup_id: 'lapsed', state: 'lapsed', cancellable: false,
    price: { symbol: 'AAPL', kind: 'pct_up', value: 2, pct_from: 'now', armed_price: 230, fired_price: null },
  }),
  wakeup({ wakeup_id: 'expired', state: 'expired', cancellable: false, kind: 'order', price: null, order: { vendor_order_id: '1', summary: 'sell 5 AAPL' } }),
  wakeup({ wakeup_id: 'cancelled', state: 'cancelled', cancellable: false, price: { symbol: 'NVDA', kind: 'below', value: 180, pct_from: null, armed_price: null, fired_price: null } }),
  wakeup({
    wakeup_id: 'legacy', short_id: null, legacy: true,
    price: { symbol: 'MSFT', kind: 'pct_down', value: 4, pct_from: 'previous_close', armed_price: null, fired_price: null },
  }),
];

/** The panel on a thread whose listing answers with `items`. */
function renderWakeups(items: Wakeup[], userTimezone = 'America/New_York') {
  mockFetch.mockResolvedValue({ thread_id: THREAD, user_timezone: userTimezone, wakeups: items });
  return renderWithProviders(<StatusPanel marketWatch={null} threadId={THREAD} />);
}

const cancelledAs = (w: Wakeup): Wakeup => ({ ...w, state: 'cancelled', cancellable: false });

const CONTENT = ['As of 14:30:00 ET (Regular)', 'NVDA  178.34  +1.20%', 'TSLA  402.11  -0.85%'].join('\n');

describe('StatusPanel', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    flag = true;
  });

  it('renders the watched symbols', () => {
    renderWithProviders(<StatusPanel marketWatch={{ symbols: ['NVDA', 'TSLA'] }} />);
    expect(screen.getByText('NVDA')).toBeInTheDocument();
    expect(screen.getByText('TSLA')).toBeInTheDocument();
  });

  it('renders the quote block — header line plus quote rows — when content is present', () => {
    renderWithProviders(<StatusPanel marketWatch={{ symbols: ['NVDA', 'TSLA'], content: CONTENT }} />);
    // First line is the muted "As of …" header.
    expect(screen.getByText(/As of 14:30:00 ET/)).toBeInTheDocument();
    // Remaining lines are the quote rows (invented prices).
    expect(screen.getByText(/178\.34/)).toBeInTheDocument();
    expect(screen.getByText(/402\.11/)).toBeInTheDocument();
  });

  it('shows the waiting hint (and no updated caption) when content is absent', () => {
    renderWithProviders(<StatusPanel marketWatch={{ symbols: ['NVDA'] }} />);
    expect(screen.getByText(/Live prices appear here/)).toBeInTheDocument();
    expect(screen.queryByText(/Updated /)).not.toBeInTheDocument();
  });

  it('shows an updated-at caption when timestamp is set', () => {
    renderWithProviders(<StatusPanel marketWatch={{ symbols: ['NVDA'], timestamp: 1_700_000_000 }} />);
    expect(screen.getByText(/Updated /)).toBeInTheDocument();
  });

  it('renders a defensive empty state when there are no symbols', () => {
    renderWithProviders(<StatusPanel marketWatch={{ symbols: [] }} />);
    expect(screen.getByText(/No tickers are being watched/)).toBeInTheDocument();
  });

  it('does not crash when marketWatch is null', () => {
    renderWithProviders(<StatusPanel marketWatch={null} />);
    expect(screen.getByText(/No tickers are being watched/)).toBeInTheDocument();
  });

  it('lists every state a wakeup can be in, a legacy price target included', async () => {
    renderWakeups(WAKEUPS);
    // The last item is Cancel all, which is no wakeup.
    const rows = (await screen.findAllByRole('listitem')).filter((row) => row.hasAttribute('data-wakeup-state'));
    expect(mockFetch).toHaveBeenCalledWith(THREAD);
    expect(rows.map((row) => row.getAttribute('data-wakeup-state'))).toEqual([
      'armed', 'armed', 'armed', 'armed', 'held', 'held_usage', 'finished', 'fired', 'lapsed', 'expired', 'cancelled', 'armed',
    ]);
    expect(rows[0]).toHaveTextContent('NVDA ≥ 205');
    expect(rows[0]).toHaveTextContent('Pending · lapses in 2h');
    expect(rows[1]).toHaveTextContent('Morning check');
    expect(rows[1]).toHaveTextContent('2 of 5 · next');
    expect(rows[2]).toHaveTextContent(/^At /);
    expect(rows[3]).toHaveTextContent('Order fills or ends · buy 100 NVDA, limit $200.00');
    expect(rows[4]).toHaveTextContent('Fired · arrives when this turn ends');
    expect(rows[5]).toHaveTextContent('usage limit reached');
    expect(rows[6]).toHaveTextContent('Finished');
    expect(rows[7]).toHaveTextContent('TSLA -3% from previous close');
    expect(rows[7]).toHaveTextContent(/Fired · .* · 388\.40/);
    expect(rows[8]).toHaveTextContent('AAPL +2% from 230.00');
    expect(rows[8]).toHaveTextContent('Lapsed');
    expect(rows[9]).toHaveTextContent('Expired');
    expect(rows[10]).toHaveTextContent('NVDA ≤ 180');
    expect(rows[10]).toHaveTextContent('Cancelled');
    expect(rows[11]).toHaveTextContent('MSFT -4% from previous close');
  });

  it('names a series on another clock by its city', async () => {
    renderWakeups([WAKEUPS[1]], 'Asia/Shanghai');
    expect(await screen.findByText(/New York time/)).toBeInTheDocument();
  });

  it('offers a cancel only on a cancellable wakeup, and shows the listing its answer carries', async () => {
    mockCancel.mockResolvedValue({
      results: [{ wakeup_id: 'price', outcome: 'cancelled' }],
      wakeups: [cancelledAs(WAKEUPS[0]), WAKEUPS[10]],
    });
    renderWakeups([WAKEUPS[0], WAKEUPS[10]]);
    const cancels = await screen.findAllByRole('button', { name: /Cancel the wakeup/ });
    expect(cancels).toHaveLength(1);
    expect(cancels[0]).toHaveAccessibleName('Cancel the wakeup NVDA ≥ 205');
    fireEvent.click(cancels[0]);
    await waitFor(() => expect(mockCancel).toHaveBeenCalledWith(THREAD, ['price']));
    await waitFor(() => expect(screen.queryByRole('button', { name: /Cancel the wakeup/ })).not.toBeInTheDocument());
    expect(screen.getAllByRole('listitem').map((row) => row.getAttribute('data-wakeup-state'))).toEqual(['cancelled', 'cancelled']);
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  });

  it('says so when a cancel fails, and lets it be tried again', async () => {
    mockCancel.mockRejectedValue(new Error('Network Error'));
    renderWakeups([WAKEUPS[0]]);
    fireEvent.click(await screen.findByRole('button', { name: /Cancel the wakeup/ }));
    expect(await screen.findByRole('alert')).toHaveTextContent("Couldn't cancel this wakeup");
    expect(screen.getByRole('button', { name: /Cancel the wakeup/ })).not.toBeDisabled();
  });

  it('says the wakeup is already with the agent when the cancel is too late', async () => {
    mockCancel.mockResolvedValue({ results: [{ wakeup_id: 'held', outcome: 'too_late' }], wakeups: [WAKEUPS[4]] });
    renderWakeups([WAKEUPS[4]]);
    fireEvent.click(await screen.findByRole('button', { name: /Cancel the wakeup/ }));
    expect(await screen.findByRole('alert')).toHaveTextContent('Already with the agent. Use Stop to end its turn.');
  });

  it('cancels every pending wakeup from one button', async () => {
    mockCancel.mockResolvedValue({ results: [], wakeups: [WAKEUPS[0], WAKEUPS[1]] });
    renderWakeups([WAKEUPS[0], WAKEUPS[1]]);
    fireEvent.click(await screen.findByRole('button', { name: 'Cancel all' }));
    await waitFor(() => expect(mockCancel).toHaveBeenCalledWith(THREAD, 'all'));
  });

  it('lists no wakeups for a panel with no thread, as a share has', () => {
    renderWithProviders(<StatusPanel marketWatch={null} />);
    expect(screen.queryByText('Wakeups')).not.toBeInTheDocument();
    expect(mockFetch).not.toHaveBeenCalled();
  });

  it('says so when the thread has no wakeups', async () => {
    renderWakeups([]);
    expect(await screen.findByText('No wakeups in this thread.')).toBeInTheDocument();
  });

  it('shows nothing when wakeups are off', async () => {
    flag = false;
    renderWakeups([]);
    await waitFor(() => expect(mockFetch).not.toHaveBeenCalled());
    expect(screen.queryByText('Wakeups')).not.toBeInTheDocument();
  });

  it('shows wakeups alone without the empty watch-list state', async () => {
    renderWakeups([WAKEUPS[0]]);
    expect(await screen.findByText('Wakeups')).toBeInTheDocument();
    expect(screen.queryByText(/No tickers are being watched/)).not.toBeInTheDocument();
  });
});
