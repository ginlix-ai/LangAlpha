import { describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen, within } from '@testing-library/react';
import type { OrderAttempt, OrderSummary } from '@/pages/ChatAgent/utils/api';
import { OrdersTable } from '../components/OrdersTable';

// The row names the broker in the words the chat receipt uses, so it reads the
// shipped brokerage list. Stubbed rather than provided, to keep the table's
// tests about the table.
vi.mock('@/hooks/useMcpServers', () => ({
  useBrokerages: () => ({
    data: [
      { name: 'moomoo', label: 'moomoo' },
      { name: 'ibkr', label: 'Interactive Brokers' },
    ],
  }),
}));

/**
 * The table is the only place most attempts are ever read, and two of its
 * columns are things a screen-sharing user should be able to hide: the account
 * the order went to, and how much of it there was.
 */

const ACCOUNT = '1234567';

function attempt(overrides: Partial<OrderAttempt> = {}): OrderAttempt {
  return {
    attempt_id: '11111111-2222-4333-8444-555555555555',
    thread_id: '33333333-3333-4333-8333-333333333333',
    workspace_id: null,
    conversation_response_id: null,
    vendor: 'moomoo',
    server: 'moomoo',
    tool: 'sim_trade_input_order',
    action: 'place',
    mode: 'paper',
    account_ref: ACCOUNT,
    asset_class: 'equity',
    order: {
      asset_class: 'equity',
      instrument: { kind: 'equity', symbol: 'AAPL', venue: 'US' },
      side: 'buy',
      qty: '12',
      order_type: 'limit',
      limit_price: '180.50',
      currency: 'USD',
      time_in_force: 'day',
    },
    status: 'filled',
    approval_required: true,
    decided_at: null,
    decision_message: null,
    executed_at: null,
    completed_at: null,
    vendor_order_id: '900104',
    filled_qty: null,
    avg_fill_price: null,
    fees: null,
    action_url: null,
    route: { market: '100' },
    failure: null,
    parent_attempt_id: null,
    created_at: '2026-09-09T06:51:19+00:00',
    updated_at: '2026-09-09T06:51:20+00:00',
    ...overrides,
  };
}

/** The same sale of AAPL, priced the way one order type is. */
function priced(
  prices: Pick<OrderSummary, 'order_type' | 'limit_price' | 'stop_price'>,
): OrderAttempt {
  return attempt({
    order: {
      asset_class: 'equity',
      instrument: { kind: 'equity', symbol: 'AAPL', venue: 'US' },
      side: 'sell',
      qty: '12',
      currency: 'USD',
      ...prices,
    },
  });
}

function renderTable(
  orders: OrderAttempt[],
  valuesHidden = false,
  onOpen = vi.fn(),
) {
  render(
    <OrdersTable orders={orders} valuesHidden={valuesHidden} onOpen={onOpen} />,
  );
  return onOpen;
}

describe('OrdersTable', () => {
  it('renders one row per attempt with the order on it', () => {
    renderTable([attempt()]);
    const row = screen.getByTestId(
      'order-row-11111111-2222-4333-8444-555555555555',
    );
    const cells = within(row);
    expect(cells.getByText('moomoo')).toBeInTheDocument();
    expect(cells.getByText('AAPL · US')).toBeInTheDocument();
    expect(cells.getByText('Buy')).toBeInTheDocument();
    expect(cells.getByText('Place')).toBeInTheDocument();
    expect(cells.getByText('12')).toBeInTheDocument();
    expect(cells.getByText('Limit $180.50')).toBeInTheDocument();
    expect(cells.getByText('paper')).toBeInTheDocument();
  });

  /**
   * The plan is the whole layout: the table lays out fixed, so a column that
   * is not here has no width and a cell cannot take width from its neighbours.
   * That is what used to push the last two columns off the right of the page.
   */
  it('draws the nine planned columns, in order', () => {
    renderTable([attempt()]);
    expect(screen.getAllByRole('columnheader').map((h) => h.textContent)).toEqual([
      'Time',
      'Mode',
      'Broker',
      'Action',
      'Instrument',
      'Side',
      'Size',
      'Type and price',
      'Status',
    ]);
  });

  it('gives every column its share, and the shares add up to the table', () => {
    renderTable([attempt()]);
    const cols = [...document.querySelectorAll('.orders-table colgroup col')];
    expect(cols).toHaveLength(9);
    const total = cols.reduce(
      (sum, col) => sum + Number.parseFloat((col as HTMLElement).style.width),
      0,
    );
    expect(total).toBeCloseTo(100, 5);
  });

  // The broker's own id is an opaque token as long as a UUID and tells two
  // attempts apart for nobody scanning; the detail a row opens carries it.
  it('leaves the broker order id to the detail', () => {
    renderTable([attempt()]);
    expect(screen.queryByText('900104')).toBeNull();
  });

  // The row wears the one-word verdict; the sentence rides along as the
  // tooltip, so a reader who wants the whole story hovers or opens the drawer.
  it('names every status in a word, and keeps the sentence in the tooltip', () => {
    renderTable([
      attempt({ attempt_id: 'a', status: 'filled' }),
      attempt({ attempt_id: 'b', status: 'rejected_by_vendor' }),
      attempt({ attempt_id: 'c', status: 'pending_confirm' }),
    ]);
    expect(screen.getByText('Filled')).toBeInTheDocument();
    expect(screen.getByText('Broker rejected')).toHaveAttribute(
      'title',
      'Rejected by the brokerage',
    );
    expect(screen.getByText('Confirming')).toHaveAttribute(
      'title',
      'Waiting for confirmation at the brokerage',
    );
    expect(screen.queryByText('Rejected by the brokerage')).toBeNull();
  });

  it('never prints an account id in full', () => {
    renderTable([attempt()]);
    expect(screen.queryByText(ACCOUNT)).toBeNull();
    expect(screen.getByText('••••4567')).toBeInTheDocument();
  });

  it('hides the size and the price when values are hidden', () => {
    renderTable([attempt()], true);
    expect(screen.queryByText('12')).toBeNull();
    expect(screen.queryByText('Limit $180.50')).toBeNull();
    expect(screen.getByText('••••')).toBeInTheDocument();
    expect(screen.getByText('Limit ••••')).toBeInTheDocument();
    // What the row is about stays readable; only the amounts go.
    expect(screen.getByText('AAPL · US')).toBeInTheDocument();
    expect(screen.getByText('Filled')).toBeInTheDocument();
  });

  /**
   * A stop-limit order carries two prices that answer different questions: the
   * stop is what triggers it, the limit the worst it may then fill at. The
   * first line keeps the limit, so the trigger gets a line of its own.
   */
  it('shows the trigger of a stop-limit order under its limit', () => {
    renderTable([
      priced({ order_type: 'stop_limit', stop_price: '178.00', limit_price: '177.50' }),
    ]);
    const row = within(
      screen.getByTestId('order-row-11111111-2222-4333-8444-555555555555'),
    );
    expect(row.getByText('Stop limit $177.50')).toBeInTheDocument();
    expect(row.getByText('Stop $178.00')).toBeInTheDocument();
  });

  it('gives a limit order one line and no stop', () => {
    renderTable([priced({ order_type: 'limit', limit_price: '177.50' })]);
    expect(screen.getByText('Limit $177.50')).toBeInTheDocument();
    expect(screen.queryByText(/^Stop/)).toBeNull();
  });

  // With no limit to lead with, the stop is the first line's price, and a
  // second line would only say it again.
  it('gives a stop order one line, carrying the stop', () => {
    renderTable([priced({ order_type: 'stop', stop_price: '178.00' })]);
    expect(screen.getByText('Stop $178.00')).toBeInTheDocument();
    expect(screen.getAllByText(/\$178\.00/)).toHaveLength(1);
  });

  it('hides both prices of a stop-limit order', () => {
    renderTable(
      [priced({ order_type: 'stop_limit', stop_price: '178.00', limit_price: '177.50' })],
      true,
    );
    expect(screen.getByText('Stop limit ••••')).toBeInTheDocument();
    expect(screen.getByText('Stop ••••')).toBeInTheDocument();
    expect(screen.queryByText(/\$/)).toBeNull();
  });

  it('hides a dollar order the same way it hides a share count', () => {
    const dollarOrder = attempt({
      order: {
        asset_class: 'equity',
        instrument: { kind: 'equity', symbol: 'TSLA' },
        side: 'buy',
        qty: null,
        notional: { amount: '250', currency: 'USD' },
        order_type: 'market',
      },
    });
    renderTable([dollarOrder]);
    expect(screen.getByText('$250.00')).toBeInTheDocument();
  });

  it('opens the row it was clicked on', () => {
    const onOpen = renderTable([attempt()]);
    fireEvent.click(
      screen.getByTestId('order-row-11111111-2222-4333-8444-555555555555'),
    );
    expect(onOpen).toHaveBeenCalledWith('11111111-2222-4333-8444-555555555555');
  });

  it('opens the row from the keyboard too', () => {
    const onOpen = renderTable([attempt()]);
    fireEvent.keyDown(
      screen.getByTestId('order-row-11111111-2222-4333-8444-555555555555'),
      { key: 'Enter' },
    );
    expect(onOpen).toHaveBeenCalledTimes(1);
  });

  it('leaves an attempt with no order shape readable', () => {
    // A cancel names no instrument, side or size, and must not blank the row.
    renderTable([
      attempt({ action: 'cancel', order: null, asset_class: null, status: 'cancelled' }),
    ]);
    expect(screen.getByText('Cancel')).toBeInTheDocument();
    expect(screen.getByText('Cancelled')).toBeInTheDocument();
  });

  it('names the order a cancel acted on, since nothing else on the row does', () => {
    const target = '11111111-2222-4333-8444-555555555555';
    renderTable([
      attempt({
        action: 'cancel',
        asset_class: null,
        status: 'cancelled',
        order: { target_ref: target },
      }),
    ]);
    expect(screen.getByText('Cancel')).toBeInTheDocument();
    // Whole: the row is where two cancels are told apart.
    expect(screen.getByText(target)).toBeInTheDocument();
  });
});
