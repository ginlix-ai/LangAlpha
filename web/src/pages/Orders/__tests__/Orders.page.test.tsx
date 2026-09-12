import { describe, expect, it, vi, beforeEach } from 'vitest';
import { render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type {
  Brokerage,
  CatalogServer,
  OrderAttempt,
  OrderSummary,
} from '@/pages/ChatAgent/utils/api';
import { httpCatalogServer } from '@/test/factories';
import Orders from '../Orders';

/**
 * The page's own job, as opposed to the table's: turn the address bar into a
 * server query, and open the `?detail=order:<id>` overlay against the ledger
 * rather than against whatever row happened to be loaded.
 */

const ATTEMPT = '11111111-2222-4333-8444-555555555555';

const getOrders = vi.fn();
const getOrder = vi.fn();

const MOOMOO: Brokerage = {
  name: 'moomoo',
  label: 'moomoo',
  url: 'https://mcp.moomoo.com/mcp',
  site: 'moomoo.com',
  description: 'Balances, positions, and order placement.',
  native_callback_only: false,
  exclusive_connection: false,
  capabilities: [{ key: 'trading', tone: 'danger', rung: true }],
};

// The page reads the same catalog the Plugins page does to tell "no orders
// yet" from "no broker to place one", so both halves of that answer are
// fixtures here.
let catalogServers: CatalogServer[] = [];

// Mocked at the module, not the barrel: the broker check imports this client
// itself, when its queries run.
vi.mock('@/pages/ChatAgent/utils/api/mcp', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/pages/ChatAgent/utils/api/mcp')>()),
  getBrokerages: vi.fn(async () => [MOOMOO]),
  getMcpCatalog: vi.fn(async () => ({
    servers: catalogServers,
    workspace_servers: [],
    max_servers: 50,
  })),
}));

vi.mock('@/pages/ChatAgent/utils/api', async (importOriginal) => {
  const actual = await importOriginal<
    typeof import('@/pages/ChatAgent/utils/api')
  >();
  return {
    ...actual,
    getOrders: (...args: unknown[]) => getOrders(...args),
    getOrder: (...args: unknown[]) => getOrder(...args),
  };
});

function attempt(overrides: Partial<OrderAttempt> = {}): OrderAttempt {
  return {
    attempt_id: ATTEMPT,
    thread_id: '33333333-3333-4333-8333-333333333333',
    workspace_id: null,
    conversation_response_id: null,
    vendor: 'moomoo',
    server: 'moomoo',
    tool: 'sim_trade_input_order',
    action: 'place',
    mode: 'paper',
    account_ref: '1234567',
    asset_class: 'equity',
    order: {
      asset_class: 'equity',
      instrument: { kind: 'equity', symbol: 'AAPL', venue: 'US' },
      side: 'buy',
      qty: '12',
      order_type: 'limit',
      limit_price: '180.50',
      currency: 'USD',
    },
    status: 'failed',
    approval_required: true,
    decided_at: '2026-09-09T06:51:19+00:00',
    decision_message: 'go ahead',
    executed_at: '2026-09-09T06:51:20+00:00',
    completed_at: '2026-09-09T06:51:25+00:00',
    vendor_order_id: '900104',
    filled_qty: null,
    avg_fill_price: null,
    fees: null,
    action_url: null,
    route: { market: '100' },
    failure: { kind: 'vendor', code: '-1', message: 'insufficient funds' },
    parent_attempt_id: null,
    created_at: '2026-09-09T06:51:19+00:00',
    updated_at: '2026-09-09T06:51:25+00:00',
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

/** What a detail field reads, found by its label. */
function fieldValue(dialog: HTMLElement, label: string): Element | null {
  return within(dialog).getByText(label).nextElementSibling;
}

function renderPage(path: string) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={[path]}>
        <Orders />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

beforeEach(() => {
  catalogServers = [
    httpCatalogServer({ name: 'moomoo', url: MOOMOO.url, oauth_status: 'connected' }),
  ];
  getOrders.mockReset();
  getOrder.mockReset();
  getOrders.mockResolvedValue({ items: [attempt()], next_cursor: null });
  getOrder.mockResolvedValue(attempt());
  localStorage.clear();
});

describe('the orders page', () => {
  it('turns the address bar into the server query', async () => {
    renderPage('/orders?vendor=moomoo&mode=paper&status=failed&asset_class=equity');
    await waitFor(() => expect(getOrders).toHaveBeenCalled());
    expect(getOrders.mock.calls[0][0]).toEqual({
      vendor: 'moomoo',
      mode: 'paper',
      status: 'failed',
      asset_class: 'equity',
    });
  });

  it('asks for nothing it was not asked for', async () => {
    renderPage('/orders');
    await waitFor(() => expect(getOrders).toHaveBeenCalled());
    expect(getOrders.mock.calls[0][0]).toEqual({
      vendor: null,
      mode: null,
      status: null,
      asset_class: null,
    });
  });

  it('opens the detail overlay straight from the url', async () => {
    renderPage(`/orders?detail=order:${ATTEMPT}`);
    const dialog = await screen.findByRole('dialog');
    expect(getOrder).toHaveBeenCalledWith(ATTEMPT);
    await waitFor(() =>
      expect(dialog).toHaveTextContent('insufficient funds'),
    );
    // The timeline, the verdict and the way back to the conversation.
    expect(dialog).toHaveTextContent('go ahead');
    expect(dialog).toHaveTextContent('Open the conversation');
    expect(dialog).toHaveTextContent('••••4567');
    expect(dialog).not.toHaveTextContent('1234567');
  });

  it('opens the overlay when a row is clicked', async () => {
    renderPage('/orders');
    const row = await screen.findByTestId(`order-row-${ATTEMPT}`);
    await userEvent.click(row);
    expect(await screen.findByRole('dialog')).toBeInTheDocument();
  });

  it('says the ledger is empty rather than that the filters missed', async () => {
    getOrders.mockResolvedValue({ items: [], next_cursor: null });
    renderPage('/orders');
    expect(
      await screen.findByText(/No orders yet/),
    ).toBeInTheDocument();
  });

  it('points at the Plugins page when there is no broker to place one', async () => {
    catalogServers = [];
    getOrders.mockResolvedValue({ items: [], next_cursor: null });
    renderPage('/orders');
    expect(await screen.findByText(/No brokerage connected/)).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Connect a brokerage' })).toBeInTheDocument();
    expect(screen.queryByText(/No orders yet/)).toBeNull();
  });

  it('says the filters missed when there are filters', async () => {
    getOrders.mockResolvedValue({ items: [], next_cursor: null });
    renderPage('/orders?vendor=robinhood');
    expect(
      await screen.findByText('No orders match these filters.'),
    ).toBeInTheDocument();
  });

  it('says which order an amend acted on, and how long it was to stand', async () => {
    const target = '11111111-2222-4333-8444-555555555555';
    getOrder.mockResolvedValue(
      attempt({
        action: 'cancel',
        order: { target_ref: target, time_in_force: 'overnight_next_day' },
      }),
    );
    renderPage(`/orders?detail=order:${ATTEMPT}`);
    const dialog = await screen.findByRole('dialog');
    await waitFor(() => expect(dialog).toHaveTextContent('Target order'));
    expect(dialog).toHaveTextContent(target);
    expect(dialog).toHaveTextContent('Overnight into the next day');
  });

  it('offers the broker link for an instruction only the user can finish', async () => {
    getOrder.mockResolvedValue(
      attempt({
        action: 'stage',
        mode: 'staged',
        status: 'pending_confirm',
        action_url: 'https://example.com/staged/abc123',
      }),
    );
    renderPage(`/orders?detail=order:${ATTEMPT}`);
    await screen.findByRole('dialog');
    const link = await screen.findByTestId('order-action-link');
    expect(link).toHaveAttribute('href', 'https://example.com/staged/abc123');
    expect(link).toHaveAttribute('rel', 'noopener noreferrer');
    expect(link).toHaveTextContent('Open in moomoo');
  });

  it('offers no broker link when the vendor sent none', async () => {
    renderPage(`/orders?detail=order:${ATTEMPT}`);
    await screen.findByRole('dialog');
    expect(screen.queryByTestId('order-action-link')).toBeNull();
  });

  /**
   * What actually filled is the answer the ledger keeps and the frozen receipt
   * in the thread cannot. It belongs in the detail panel rather than as a
   * tenth column: the table is already at the width it fits in.
   */
  it('shows what filled, in the detail rather than the table', async () => {
    getOrder.mockResolvedValue(
      attempt({
        status: 'partially_filled',
        filled_qty: '4',
        avg_fill_price: '180.25',
        fees: { amount: '0.99', currency: 'USD' },
      }),
    );
    renderPage(`/orders?detail=order:${ATTEMPT}`);
    const dialog = await screen.findByRole('dialog');
    await waitFor(() => expect(dialog).toHaveTextContent('Average fill price'));
    expect(dialog).toHaveTextContent('$180.25');
    expect(dialog).toHaveTextContent('$0.99');
    expect(within(dialog).getByText('Filled')).toBeInTheDocument();
  });

  // A live order still working comes back with a filled quantity of 0 and an
  // average price of 0. That is the broker saying nothing has happened yet.
  it('draws no fill block for a vendor zero', async () => {
    getOrder.mockResolvedValue(
      attempt({ status: 'working', filled_qty: '0', avg_fill_price: '0' }),
    );
    renderPage(`/orders?detail=order:${ATTEMPT}`);
    const dialog = await screen.findByRole('dialog');
    await waitFor(() => expect(dialog).toHaveTextContent('Where it came from'));
    expect(dialog).not.toHaveTextContent('Average fill price');
  });

  it('draws no fill block for an order that never filled', async () => {
    renderPage(`/orders?detail=order:${ATTEMPT}`);
    const dialog = await screen.findByRole('dialog');
    await waitFor(() => expect(dialog).toHaveTextContent('insufficient funds'));
    expect(dialog).not.toHaveTextContent('Average fill price');
  });

  it('hides a fill with the other amounts', async () => {
    localStorage.setItem('portfolio_values_hidden', 'true');
    getOrder.mockResolvedValue(
      attempt({ status: 'filled', filled_qty: '12', avg_fill_price: '180.25' }),
    );
    renderPage(`/orders?detail=order:${ATTEMPT}`);
    const dialog = await screen.findByRole('dialog');
    await waitFor(() => expect(dialog).toHaveTextContent('Average fill price'));
    expect(dialog).not.toHaveTextContent('$180.25');
    expect(within(dialog).getAllByText('••••').length).toBeGreaterThan(0);
  });

  /**
   * The panel used to have one Price field, and it held only the limit, so the
   * trigger of a stop-limit order appeared nowhere on the page. Each price is
   * its own field now, drawn only when the order carries it, as the approval
   * card draws them.
   */
  it('gives a stop-limit order a field for each price', async () => {
    getOrder.mockResolvedValue(
      priced({ order_type: 'stop_limit', stop_price: '178.00', limit_price: '177.50' }),
    );
    renderPage(`/orders?detail=order:${ATTEMPT}`);
    const dialog = await screen.findByRole('dialog');
    await waitFor(() => expect(dialog).toHaveTextContent('Stop price'));
    expect(fieldValue(dialog, 'Limit price')).toHaveTextContent('$177.50');
    expect(fieldValue(dialog, 'Stop price')).toHaveTextContent('$178.00');
  });

  it('gives a limit order only its limit price', async () => {
    renderPage(`/orders?detail=order:${ATTEMPT}`);
    const dialog = await screen.findByRole('dialog');
    await waitFor(() => expect(dialog).toHaveTextContent('Order type'));
    expect(fieldValue(dialog, 'Limit price')).toHaveTextContent('$180.50');
    expect(dialog).not.toHaveTextContent('Stop price');
    expect(within(dialog).queryByText('Price')).toBeNull();
  });

  it('gives a stop order only its stop price', async () => {
    getOrder.mockResolvedValue(priced({ order_type: 'stop', stop_price: '178.00' }));
    renderPage(`/orders?detail=order:${ATTEMPT}`);
    const dialog = await screen.findByRole('dialog');
    await waitFor(() => expect(dialog).toHaveTextContent('Order type'));
    expect(fieldValue(dialog, 'Stop price')).toHaveTextContent('$178.00');
    expect(dialog).not.toHaveTextContent('Limit price');
  });

  // A blank price field reads as a price the ledger lost, and a market order
  // never had one.
  it('draws no price field for a market order', async () => {
    getOrder.mockResolvedValue(priced({ order_type: 'market' }));
    renderPage(`/orders?detail=order:${ATTEMPT}`);
    const dialog = await screen.findByRole('dialog');
    await waitFor(() => expect(dialog).toHaveTextContent('Order type'));
    expect(dialog).not.toHaveTextContent('Limit price');
    expect(dialog).not.toHaveTextContent('Stop price');
  });

  it('hides both prices with the other amounts', async () => {
    localStorage.setItem('portfolio_values_hidden', 'true');
    getOrder.mockResolvedValue(
      priced({ order_type: 'stop_limit', stop_price: '178.00', limit_price: '177.50' }),
    );
    renderPage(`/orders?detail=order:${ATTEMPT}`);
    const dialog = await screen.findByRole('dialog');
    await waitFor(() => expect(dialog).toHaveTextContent('Stop price'));
    expect(fieldValue(dialog, 'Limit price')).toHaveTextContent('••••');
    expect(fieldValue(dialog, 'Stop price')).toHaveTextContent('••••');
    expect(dialog).not.toHaveTextContent('$177.50');
    expect(dialog).not.toHaveTextContent('$178.00');
  });

  /**
   * A staged instruction that reached the vendor is not on a market: it waits
   * in the broker's own client for the user to confirm, and expires in seven
   * days. The row and the panel say so in the same words the thread does.
   */
  it('says a staged order is waiting to be confirmed, not that it was sent', async () => {
    const staged = attempt({ mode: 'staged', action: 'stage', status: 'submitted' });
    getOrders.mockResolvedValue({ items: [staged], next_cursor: null });
    getOrder.mockResolvedValue(staged);
    renderPage(`/orders?detail=order:${ATTEMPT}`);

    const row = await screen.findByTestId(`order-row-${ATTEMPT}`);
    const pill = within(row).getByTestId('order-status-submitted');
    expect(pill).toHaveTextContent('Staged');
    expect(pill).toHaveAttribute('title', 'Staged, confirm in moomoo');
    const dialog = await screen.findByRole('dialog');
    await waitFor(() =>
      expect(within(dialog).getByTestId('order-status-submitted')).toHaveTextContent(
        'Staged, confirm in moomoo',
      ),
    );
    expect(screen.queryByText('Sent to the brokerage')).toBeNull();
  });

  it('leaves a live order on the word it had', async () => {
    const live = attempt({ mode: 'live', status: 'submitted' });
    getOrders.mockResolvedValue({ items: [live], next_cursor: null });
    renderPage('/orders');
    const row = await screen.findByTestId(`order-row-${ATTEMPT}`);
    const pill = within(row).getByTestId('order-status-submitted');
    expect(pill).toHaveTextContent('Sent');
    expect(pill).toHaveAttribute('title', 'Sent to the brokerage');
  });

  it('hides amounts across the table when the eye is off', async () => {
    localStorage.setItem('portfolio_values_hidden', 'true');
    renderPage('/orders');
    await screen.findByTestId(`order-row-${ATTEMPT}`);
    expect(screen.queryByText('12')).toBeNull();
    expect(screen.getByText('••••')).toBeInTheDocument();
  });
});
