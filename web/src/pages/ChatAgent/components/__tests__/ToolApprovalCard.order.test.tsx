/**
 * A stopped order is asked in the shape of the receipt it becomes.
 *
 * The card the person answers and the record they read afterwards are one
 * thing seen at two moments, so the pending card carries the receipt's header,
 * its ticket and its pill, and only the footer changes. The arguments are
 * still the exact frame the vendor sees, and are still on screen, but behind a
 * disclosure: the summary above them is the question being answered, and a
 * JSON dump beside a live order buries it.
 */
import React from 'react';
import { afterEach, describe, it, expect } from 'vitest';
import { vi } from 'vitest';
import { fireEvent, screen, waitFor, within } from '@testing-library/react';
import '@testing-library/jest-dom';
import { QueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import type { OrderAttempt, OrderPage } from '@/pages/ChatAgent/utils/api';
import { renderWithProviders } from '@/test/utils';
import { HIDDEN } from '@/components/orders/format';
import enUS from '@/locales/en-US.json';
import zhCN from '@/locales/zh-CN.json';
import ToolApprovalCard from '../ToolApprovalCard';
import { instrumentLabel } from '@/components/orders/instrument';
import { ORDER_REF_KEY, ORDER_SUMMARY_KEYS, orderReadFully, orderTicket } from '../mcp/orderSummary';
import type { ToolApprovalState } from '@/types/chat';
import type { OrderProposal } from '@/types/sse';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({
    t: (key: string, opts?: Record<string, unknown>) => {
      if (opts && typeof opts === 'object') {
        let out = key;
        for (const [k, v] of Object.entries(opts)) {
          out = out.replace(new RegExp(`{{\\s*${k}\\s*}}`, 'g'), String(v));
        }
        return out;
      }
      return key;
    },
  }),
}));

vi.mock('@/hooks/useMcpServers', () => ({
  useBrokerages: () => ({ data: [{ name: 'moomoo', label: 'moomoo' }] }),
}));

const ORDER: OrderProposal = {
  action: 'place',
  mode: 'live',
  vendor: 'moomoo',
  account_ref: '12345678',
  asset_class: 'equity',
  instrument: { kind: 'equity', symbol: 'US.AAPL' },
  side: 'buy',
  qty: '1',
  order_type: 'limit',
  limit_price: '100',
  time_in_force: 'day',
};

/** Fake, and full length on purpose: an elided id cannot be cross-checked. */
const TARGET = '11111111-2222-4333-8444-555555555555';

const CANCEL: OrderProposal = {
  action: 'cancel',
  mode: 'live',
  vendor: 'moomoo',
  account_ref: '12345678',
  target_ref: TARGET,
};

const keyedPending: ToolApprovalState = {
  status: 'pending',
  toolName: 'mcp__moomoo__trading_order_place',
  server: 'moomoo',
  tool: 'trading_order_place',
  args: { acc_id: '12345678', code: 'US.AAPL', side: 'BUY', qty: '1', price: '100' },
  interruptId: 'int-1',
  actionIndex: 0,
  actionCount: 1,
  toolCallId: 'call-1',
  attemptId: 'attempt-1',
  order: ORDER,
};

const openArguments = () =>
  fireEvent.click(screen.getByText('toolArtifact.directTool.orderApproval.arguments'));

describe('the pending order card', () => {
  it('is the receipt: the action, the mode, the vendor and a verdict pill', () => {
    renderWithProviders(
      <ToolApprovalCard data={keyedPending} onApprove={() => {}} onReject={() => {}} />,
    );
    const card = screen.getByTestId('order-approval');
    expect(card).toHaveTextContent('toolArtifact.directTool.orderAction.place');
    expect(card).toHaveTextContent('plugins.detail.orderModeLive');
    expect(card).toHaveTextContent('moomoo');
    expect(screen.getByTestId('order-status-proposed')).toHaveTextContent(
      'toolArtifact.directTool.orderStatus.proposed',
    );
    expect(card).toHaveTextContent('moomoo · ••••5678');
    expect(screen.getByTestId('order-headline')).toHaveTextContent('orders.side.buy US.AAPL');
  });

  // The card answers a question about a trade. The frame that will be sent is
  // still reachable, and still masked, but it is not what is being asked.
  it('keeps the arguments off the card, one click under it', () => {
    renderWithProviders(
      <ToolApprovalCard data={keyedPending} onApprove={() => {}} onReject={() => {}} />,
    );
    expect(screen.queryByText('code')).toBeNull();
    openArguments();
    expect(screen.getByText('code')).toBeInTheDocument();
    expect(screen.queryByText('12345678')).toBeNull();
    expect(screen.getByText('••••5678')).toBeInTheDocument();
  });

  it('approves with a bare decision', () => {
    const onApprove = vi.fn();
    renderWithProviders(
      <ToolApprovalCard data={keyedPending} onApprove={onApprove} onReject={() => {}} />,
    );
    fireEvent.click(screen.getByText('toolArtifact.directTool.approve'));
    expect(onApprove).toHaveBeenCalledTimes(1);
  });

  it('rejects with the typed reason, or with none', () => {
    const onReject = vi.fn();
    const { unmount } = renderWithProviders(
      <ToolApprovalCard data={keyedPending} onApprove={() => {}} onReject={onReject} />,
    );
    fireEvent.change(
      screen.getByLabelText('toolArtifact.directTool.orderApproval.reasonPlaceholder'),
      { target: { value: 'wrong account' } },
    );
    fireEvent.click(screen.getByText('toolArtifact.directTool.reject'));
    expect(onReject).toHaveBeenCalledWith('wrong account');
    unmount();

    const bare = vi.fn();
    renderWithProviders(
      <ToolApprovalCard data={keyedPending} onApprove={() => {}} onReject={bare} />,
    );
    fireEvent.click(screen.getByText('toolArtifact.directTool.reject'));
    expect(bare).toHaveBeenCalledWith(undefined);
  });

  // Replayed history renders the card with neither handler. A stop nobody can
  // answer now must not show live buttons over an interrupt that is gone.
  it('read-only, says it was never answered and offers nothing to press', () => {
    renderWithProviders(<ToolApprovalCard data={keyedPending} />);
    expect(screen.getByTestId('order-status-unanswered')).toHaveTextContent(
      'toolArtifact.directTool.orderApproval.notAnswered',
    );
    expect(screen.queryByTestId('order-status-proposed')).toBeNull();
    expect(screen.queryByText('toolArtifact.directTool.approve')).toBeNull();
    expect(screen.queryByText('toolArtifact.directTool.reject')).toBeNull();
    expect(
      screen.queryByLabelText('toolArtifact.directTool.orderApproval.reasonPlaceholder'),
    ).toBeNull();
  });

  it('says which order an amend acts on, whole', () => {
    renderWithProviders(
      <ToolApprovalCard
        data={{
          ...keyedPending,
          tool: 'trading_order_cancel',
          toolName: 'mcp__moomoo__trading_order_cancel',
          args: { acc_id: '12345678', order_id: TARGET },
          order: CANCEL,
        }}
        onApprove={() => {}}
        onReject={() => {}}
      />,
    );
    const card = screen.getByTestId('order-approval');
    expect(card).toHaveTextContent('toolArtifact.directTool.orderAction.cancel');
    expect(screen.getByTestId('order-headline')).toHaveTextContent(
      `toolArtifact.directTool.orderTicket.order ${TARGET}`,
    );
    const target = screen.getByTestId('order-target-ref');
    expect(target.textContent).toBe(TARGET);
    expect(target.className).toContain('font-mono');
    // The fields a cancel does not carry stay off the card: on this surface a
    // blank price reads as a market order.
    expect(screen.queryByTestId('order-figures')).toBeNull();
    expect(card).toHaveTextContent('••••5678');
  });

  it('draws no target line for a place', () => {
    renderWithProviders(
      <ToolApprovalCard data={keyedPending} onApprove={() => {}} onReject={() => {}} />,
    );
    expect(screen.queryByTestId('order-target-ref')).toBeNull();
  });
});

/**
 * The window between the click and the tool's answer. Settling here would
 * state a verdict no receipt has confirmed, so the card holds its shape and
 * says where the order is instead.
 */
/**
 * A vendor that renames a field leaves the adapter a shorter order that still
 * looks whole: a buy with no size reads as a complete ticket. The card says so,
 * and opens the arguments, which are then the only full account of the trade.
 */
describe('an order the adapter could not read whole', () => {
  const pendingWith = (order: OrderProposal): ToolApprovalState => ({ ...keyedPending, order });

  it('says so, and opens the arguments, when a buy has no size', () => {
    renderWithProviders(
      <ToolApprovalCard data={pendingWith({ ...ORDER, qty: null })} onApprove={() => {}} onReject={() => {}} />,
    );
    expect(screen.getByTestId('order-incomplete')).toHaveTextContent(
      'toolArtifact.directTool.orderApproval.incomplete',
    );
    expect(screen.getByText('code')).toBeInTheDocument();
  });

  // The ticket names a spread by its strategy or its leg count; the legs
  // themselves are only in the arguments.
  it('opens the arguments of a spread, without calling it unread', () => {
    const spread: OrderProposal = {
      ...ORDER,
      side: null,
      instrument: { kind: 'combo', strategy: 'vertical', legs: [{ side: 'buy' }, { side: 'sell' }] },
    };
    renderWithProviders(
      <ToolApprovalCard data={pendingWith(spread)} onApprove={() => {}} onReject={() => {}} />,
    );
    expect(screen.queryByTestId('order-incomplete')).toBeNull();
    expect(screen.getByText('code')).toBeInTheDocument();
  });

  it('says so over an option it can name only by the vendor id', () => {
    const byId: OrderProposal = {
      ...ORDER,
      vendor: 'robinhood',
      asset_class: 'option',
      instrument: { kind: 'opaque', raw_code: '3a2b1c00-4444-4000-8000-000000000004' },
    };
    renderWithProviders(
      <ToolApprovalCard data={pendingWith(byId)} onApprove={() => {}} onReject={() => {}} />,
    );
    expect(screen.getByTestId('order-incomplete')).toBeInTheDocument();
  });

  it('says nothing over an order it read whole', () => {
    renderWithProviders(
      <ToolApprovalCard data={keyedPending} onApprove={() => {}} onReject={() => {}} />,
    );
    expect(screen.queryByTestId('order-incomplete')).toBeNull();
    expect(screen.queryByText('code')).toBeNull();
  });

  it.each<[string, boolean, OrderProposal]>([
    ['a buy with everything', true, ORDER],
    ['a buy by amount', true, { ...ORDER, qty: null, notional: { amount: '500', currency: 'USD' } }],
    ['a buy with no instrument', false, { ...ORDER, instrument: null }],
    ['a buy with no side', false, { ...ORDER, side: null }],
    [
      'a spread, whose legs carry the side',
      true,
      { ...ORDER, side: null, instrument: { kind: 'combo', legs: [{ side: 'buy' }, { side: 'sell' }] } },
    ],
    ['an order the adapter read nothing of', false, { action: 'place', mode: 'live' }],
    ['a limit order with no limit', false, { ...ORDER, limit_price: null }],
    ['a stop order with no stop', false, { ...ORDER, order_type: 'stop', limit_price: null }],
    ['a market order, which has no price to read', true, { ...ORDER, order_type: 'market', limit_price: null }],
    [
      'a share named only by its venue',
      false,
      { ...ORDER, instrument: { kind: 'equity', symbol: '', venue: 'US' } },
    ],
    [
      'an option short of its strike and expiry',
      false,
      { ...ORDER, instrument: { kind: 'option', underlying: 'AAPL', right: 'C' } },
    ],
    ['a replace naming its order and a new price', true, { ...CANCEL, action: 'replace', limit_price: '101' }],
    ['a replace that changes nothing it read', false, { ...CANCEL, action: 'replace' }],
    ['a cancel naming its order', true, CANCEL],
    ['a cancel naming nothing', false, { ...CANCEL, target_ref: null }],
    [
      'an option placed by the vendor id alone',
      false,
      { ...ORDER, asset_class: 'option', instrument: { kind: 'opaque', raw_code: 'opt-1' } },
    ],
    [
      'an exercise of an option known only by its id',
      false,
      {
        action: 'exercise',
        mode: 'live',
        asset_class: 'option',
        qty: '1',
        instrument: { kind: 'opaque', raw_code: 'opt-1' },
      },
    ],
    [
      'a staged order on a contract id that names no option',
      true,
      {
        ...ORDER,
        action: 'stage',
        mode: 'staged',
        asset_class: 'other',
        instrument: { kind: 'opaque', raw_code: '265598' },
      },
    ],
    [
      'a cancelled exercise, addressed by its contract',
      true,
      {
        action: 'cancel_exercise',
        mode: 'live',
        asset_class: 'option',
        instrument: { kind: 'opaque', raw_code: 'opt-1' },
      },
    ],
  ])('reads %s as whole: %s', (_label, whole, order) => {
    expect(orderReadFully(order)).toBe(whole);
  });
});

describe('an approved order still in flight', () => {
  const approved = { ...keyedPending, status: 'approved' as const };

  it('keeps the card, with the brokerage pill and nothing to press', () => {
    renderWithProviders(<ToolApprovalCard data={approved} resultPending />);
    expect(screen.getByTestId('order-approval-card')).toHaveAttribute(
      'data-order-state',
      'sending',
    );
    expect(screen.getByTestId('order-status-submitting')).toHaveTextContent(
      'toolArtifact.directTool.orderStatus.submitting',
    );
    expect(screen.queryByText('toolArtifact.directTool.approve')).toBeNull();
    expect(screen.queryByTestId('tool-approval-settled')).toBeNull();
  });

  it('settles once the result has landed', () => {
    renderWithProviders(<ToolApprovalCard data={approved} resultPending={false} />);
    expect(screen.getByTestId('tool-approval-settled')).toBeInTheDocument();
    expect(screen.queryByTestId('order-approval-card')).toBeNull();
  });
});

/**
 * A worker lost between the dispatch and the answer leaves an approved order
 * with no receipt, and the collapsed verdict would then be the only thing the
 * thread ever says about an order the brokerage may have filled.
 */
describe('an approved order whose call never answered', () => {
  const approved = { ...keyedPending, status: 'approved' as const };

  it('wears the ledger outcome and points at the row', () => {
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    queryClient.setQueryData<OrderAttempt>(
      queryKeys.orders.detail('attempt-1'),
      { attempt_id: 'attempt-1', status: 'filled' } as OrderAttempt,
    );
    renderWithProviders(
      <ToolApprovalCard data={approved} resultPending={false} resultLost />,
      { queryClient },
    );
    expect(screen.getByTestId('order-status-filled')).toHaveTextContent(
      'toolArtifact.directTool.orderStatusShort.filled',
    );
    expect(
      screen.getByRole('link', { name: /orderReceipt\.viewInOrders/ }),
    ).toHaveAttribute('href', '/orders?detail=order:attempt-1');
  });

  // Nothing to read and nothing to point at, so the row says only what it knows.
  it('stays the plain verdict when the card names no attempt', () => {
    renderWithProviders(
      <ToolApprovalCard data={{ ...approved, attemptId: undefined }} resultPending={false} resultLost />,
    );
    expect(screen.getByTestId('tool-approval-settled')).toBeInTheDocument();
    expect(screen.queryByTestId('order-approval-orphaned')).toBeNull();
  });
});

/**
 * Hiding values masks the receipt, not the question before it: a person
 * approving an order has to see the size and prices they are approving.
 */
describe('the order card while values are hidden', () => {
  afterEach(() => localStorage.removeItem('portfolio_values_hidden'));

  it.each([
    { state: 'pending', data: keyedPending, resultPending: false },
    { state: 'sending', data: { ...keyedPending, status: 'approved' as const }, resultPending: true },
  ])('still draws the size and the price on a $state card', ({ state, data, resultPending }) => {
    localStorage.setItem('portfolio_values_hidden', 'true');
    renderWithProviders(
      <ToolApprovalCard
        data={data}
        onApprove={() => {}}
        onReject={() => {}}
        resultPending={resultPending}
      />,
    );
    expect(screen.getByTestId('order-approval-card')).toHaveAttribute('data-order-state', state);
    const figures = screen.getByTestId('order-figures');
    expect(within(figures).getByText('1')).toBeInTheDocument();
    // The limit, and the estimate it makes at that size.
    expect(within(figures).getAllByText('100.00')).toHaveLength(2);
    expect(figures).not.toHaveTextContent(HIDDEN);
  });
});

/**
 * A settled order is one line, like every other tool call. The receipt above it
 * is the transcript's copy of the trade, so the step that asked does not state
 * the same order a second time.
 */
describe('a settled order step', () => {
  it('records the verdict, the action and the mode, over the folded arguments', () => {
    renderWithProviders(
      <ToolApprovalCard data={{ ...keyedPending, status: 'approved' }} resultPending={false} />,
    );
    const row = screen.getByTestId('tool-approval-settled');
    expect(row).toHaveTextContent('toolArtifact.directTool.orderApproval.approved');
    expect(row).toHaveTextContent('toolArtifact.directTool.orderAction.place');
    expect(row).toHaveTextContent('plugins.detail.orderModeLive');
    expect(screen.queryByTestId('order-approval')).toBeNull();
    expect(screen.queryByText('code')).toBeNull();
    fireEvent.click(screen.getByRole('button'));
    expect(screen.getByText('code')).toBeInTheDocument();
  });

  it('carries the reason a rejection was given', () => {
    renderWithProviders(
      <ToolApprovalCard data={{ ...keyedPending, status: 'rejected', reason: 'too big' }} />,
    );
    const row = screen.getByTestId('tool-approval-settled');
    expect(row).toHaveTextContent('toolArtifact.directTool.orderApproval.rejected');
    expect(row).toHaveTextContent('too big');
    expect(screen.queryByTestId('order-approval')).toBeNull();
  });

  // A rejection never reaches the brokerage, so there is nothing to wait for
  // and the step settles on the click rather than on a result.
  it('settles a rejection without waiting for a result', () => {
    renderWithProviders(
      <ToolApprovalCard data={{ ...keyedPending, status: 'rejected' }} resultPending />,
    );
    expect(screen.getByTestId('tool-approval-settled')).toBeInTheDocument();
  });
});

describe('the arguments behind a settled order', () => {
  const settled = (args: Record<string, unknown>) => ({
    ...keyedPending,
    status: 'approved' as const,
    args,
  });
  const open = () => fireEvent.click(screen.getByRole('button'));

  // The vendors do not agree on what to call this field, so the key set that
  // masks it is one expression shared with every other surface that draws one.
  it('masks the account however the vendor spelled it', () => {
    renderWithProviders(
      <ToolApprovalCard data={settled({ account_number: '123456789', symbol: 'F' })} />,
    );
    open();
    expect(screen.queryByText('123456789')).toBeNull();
    expect(screen.getByText('••••6789')).toBeInTheDocument();
  });

  it('masks an account nested inside an argument', () => {
    renderWithProviders(
      <ToolApprovalCard data={settled({ leg: { rhs_account_number: '123456789', side: 'buy' } })} />,
    );
    open();
    expect(screen.queryByText(/123456789/)).toBeNull();
    expect(screen.getByText(/••••6789/)).toBeInTheDocument();
  });

  /**
   * IBKR's order frame carries `contract_id: 0` beside the `contract_id_ex`
   * that actually addresses the instrument. A reader comparing this frame
   * against the broker's own app has to be able to see which field named the
   * contract, and a zero is never the answer.
   */
  it('drops a zero where an id belongs', () => {
    renderWithProviders(
      <ToolApprovalCard
        data={settled({ contract_id: 0, contract_id_ex: '265598', orderId: 0, conid: 0 })}
      />,
    );
    open();
    expect(screen.queryByText('contract_id')).toBeNull();
    expect(screen.queryByText('orderId')).toBeNull();
    expect(screen.getByText('contract_id_ex')).toBeInTheDocument();
    expect(screen.getByText('265598')).toBeInTheDocument();
    // Only the key decides, and this one does not end in an id.
    expect(screen.getByText('conid')).toBeInTheDocument();
  });

  // A zero quantity, a zero price and a zero id are three different facts, and
  // the first two are things the vendor was actually told.
  it('keeps a zero that is not an id', () => {
    renderWithProviders(
      <ToolApprovalCard data={settled({ quantity: 0, limit_price: 0, symbol: 'F' })} />,
    );
    open();
    expect(screen.getByText('quantity')).toBeInTheDocument();
    expect(screen.getByText('limit_price')).toBeInTheDocument();
    expect(screen.getAllByText('0').length).toBe(2);
  });

  // An order form carries every optional leg of every order type, and the rows
  // the caller left null are noise around the ones that decided the trade.
  it('drops the fields the caller sent as null', () => {
    renderWithProviders(
      <ToolApprovalCard
        data={settled({ symbol: 'F', stop_price: null, dollar_amount: '', tax_lots: null })}
      />,
    );
    open();
    expect(screen.getByText('symbol')).toBeInTheDocument();
    expect(screen.queryByText('stop_price')).toBeNull();
    expect(screen.queryByText('dollar_amount')).toBeNull();
    expect(screen.queryByText('tax_lots')).toBeNull();
  });
});

describe('what the order ticket reads off the server', () => {
  const SHOWN = { hidden: false, locale: 'en-US' };

  it('draws only the fields the adapter could fill', () => {
    const ticket = orderTicket(
      { action: 'cancel', mode: 'paper', instrument: { kind: 'equity', symbol: 'US.AAPL' } },
      SHOWN,
    );
    expect(ticket.figures).toEqual([]);
    expect(ticket.meta).toEqual([]);
    expect(ticket.side).toBeNull();
  });

  // The id is what a person matches against the broker's app: the subject of
  // the headline when nothing else names the order, the first thing under it
  // when an instrument does.
  it('leads with the target wherever the instrument does not', () => {
    expect(orderTicket(CANCEL, SHOWN)).toMatchObject({ instrument: null, targetRef: TARGET });
    const named = orderTicket(
      { ...CANCEL, instrument: { kind: 'equity', symbol: 'US.AAPL' } },
      SHOWN,
    );
    expect(named.targetRef).toBeNull();
    expect(named.meta[0]).toEqual({ field: 'target_ref', labelKey: ORDER_REF_KEY, value: TARGET });
  });

  it('writes the amount in its own currency', () => {
    const ticket = orderTicket(
      { action: 'place', mode: 'live', notional: { amount: '250.00', currency: 'USD' } },
      SHOWN,
    );
    expect(ticket.figures).toEqual([
      { field: 'notional', labelKey: 'toolArtifact.directTool.orderField.notional', value: '$250.00' },
    ]);
  });

  // moomoo sends a price on every order and ignores it on a market order.
  // Drawn without its type, that price is a limit order on the card asking
  // someone to approve a market one.
  it('names the type unless the prices already say it', () => {
    const fields = (order: Partial<OrderProposal>) =>
      orderTicket({ ...ORDER, ...order }, SHOWN).figures.map((part) => part.field);
    expect(fields({ order_type: 'market' })).toEqual(['qty', 'order_type', 'limit_price']);
    const market = orderTicket({ ...ORDER, order_type: 'market' }, SHOWN).figures;
    expect(market[1]).toMatchObject({ valueKey: 'orders.orderType.market' });
    // Not a limit, so not called one.
    expect(market[2]).toMatchObject({ labelKey: 'toolArtifact.directTool.orderField.price' });
    expect(orderTicket(ORDER, SHOWN).figures[1]).toMatchObject({
      labelKey: 'toolArtifact.directTool.orderField.limitPrice',
    });
    expect(fields({ order_type: 'stop', stop_price: '95' })).toEqual([
      'qty',
      'order_type',
      'stop_price',
      'limit_price',
    ]);
    expect(fields({ order_type: 'market_if_touched', limit_price: null, stop_price: '95' })).toEqual([
      'qty',
      'order_type',
      'stop_price',
    ]);
    expect(fields({ order_type: 'market', limit_price: null })).toEqual(['qty', 'order_type']);
    // Here the columns say it, so the type would only repeat them.
    expect(fields({})).toEqual(['qty', 'limit_price', 'total']);
    expect(fields({ order_type: 'stop_limit', stop_price: '95' })).toEqual([
      'qty',
      'stop_price',
      'limit_price',
      'total',
    ]);
    expect(fields({ order_type: 'stop', limit_price: null, stop_price: '95' })).toEqual([
      'qty',
      'stop_price',
    ]);
  });

  // The vendor's own rule decides which size it goes by, so the card draws
  // both rather than asking about an order of the other size.
  it('draws both sizes when both were sent', () => {
    const ticket = orderTicket(
      { ...ORDER, notional: { amount: '250.00', currency: 'USD' }, currency: 'USD' },
      SHOWN,
    );
    expect(ticket.figures.map((part) => part.field)).toEqual(['qty', 'notional', 'limit_price']);
  });

  // Each asset class names itself off different fields, and the server says
  // which by tagging the kind: an equity has a symbol, crypto a pair, an HK
  // warrant only its raw code, and an option nothing shorter than four parts.
  it('names the instrument whichever way the vendor addressed it', () => {
    expect(instrumentLabel({ kind: 'equity', symbol: 'US.AAPL' })).toBe('US.AAPL');
    expect(instrumentLabel({ kind: 'crypto', pair: 'BTC-USD' })).toBe('BTC-USD');
    expect(instrumentLabel({ kind: 'opaque', raw_code: 'SEHK.12345' })).toBe('SEHK.12345');
    expect(
      instrumentLabel({
        kind: 'option',
        underlying: 'AAPL',
        expiration: '2026-01-16',
        strike: '200',
        right: 'C',
      }),
    ).toBe('AAPL 2026-01-16 200 C');
    expect(instrumentLabel(null)).toBe('');
  });
});

describe('a call that places no order', () => {
  // The order card is keyed on the summary, not on the tool name, so a direct
  // call that places none keeps the generic card it always had.
  it('keeps the generic approval card, arguments and all', () => {
    renderWithProviders(
      <ToolApprovalCard
        data={{ ...keyedPending, attemptId: undefined, order: null }}
        onApprove={() => {}}
        onReject={() => {}}
      />,
    );
    expect(screen.queryByTestId('order-approval')).toBeNull();
    expect(screen.getByText('toolArtifact.directTool.approvalTitle')).toBeInTheDocument();
    expect(screen.getByText('••••5678')).toBeInTheDocument();
  });
});

/**
 * The nav hides Orders once no broker is connected and its one-row read of the
 * ledger came back empty. A proposal is already a row, so a card keyed to one
 * re-asks, or a disconnect before the verdict would hide the page it lives on.
 */
describe('the nav gate', () => {
  it('re-asks whether any order exists when the cache says there is none', async () => {
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    queryClient.setQueryData<OrderPage>(queryKeys.orders.any(), { items: [], next_cursor: null });
    renderWithProviders(
      <ToolApprovalCard data={keyedPending} onApprove={() => {}} onReject={() => {}} />,
      { queryClient },
    );
    await waitFor(() =>
      expect(queryClient.getQueryState(queryKeys.orders.any())?.isInvalidated).toBe(true),
    );
  });
});

describe('the copy the order cards ask for', () => {
  // These keys are held in a map and reach `t()` through a variable, so the
  // tree-wide locale sweep cannot see them. This is their coverage.
  it.each(ORDER_SUMMARY_KEYS)('%s resolves in both catalogs', (key) => {
    const lookup = (catalog: unknown) =>
      key.split('.').reduce<unknown>(
        (acc, part) =>
          acc && typeof acc === 'object' && part in (acc as object)
            ? (acc as Record<string, unknown>)[part]
            : undefined,
        catalog,
      );
    expect(typeof lookup(enUS)).toBe('string');
    expect(typeof lookup(zhCN)).toBe('string');
  });
});
