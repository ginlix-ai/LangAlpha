/**
 * The receipt card: a named verdict for every state, and nothing on screen that
 * a shoulder should not read.
 *
 * The status list is the point of the first test. An order's end state is the
 * one fact this card exists to carry, and a state that renders as its raw enum
 * (or as nothing) is worse than no card, so every value the ledger can hold is
 * exercised rather than a representative few.
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { fireEvent, screen, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import { renderWithProviders } from '@/test/utils';
import enUS from '@/locales/en-US.json';
import zhCN from '@/locales/zh-CN.json';
import { OrderReceiptCard } from '../mcp/OrderReceiptCard';
import {
  ORDER_RECEIPT_KEYS,
  orderOutcomeRows,
  orderReceiptOf,
  overlayLedgerRow,
} from '../mcp/useOrderReceipt';
import type { OrderAttempt } from '@/pages/ChatAgent/utils/api';
import type { OrderOutcome, OrderProposal, OrderReceipt, OrderStatus } from '@/types/sse';

/** Echoes the key, with `{{name}}` filled in, so an assertion can name both
 *  the sentence that was asked for and what went into it. */
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

/** The ledger read the card does on mount. Left unresolved by default, so a
 *  test that is not about the overlay sees only the artifact. */
const getOrder = vi.fn(
  (_attemptId: string) => new Promise<OrderAttempt>(() => {}),
);

vi.mock('@/pages/ChatAgent/utils/api', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/pages/ChatAgent/utils/api')>();
  return { ...actual, getOrder: (id: string) => getOrder(id) };
});

/** Hoisted so a test can take moomoo out of the shipped list and watch the
 *  vendor mark fall back. */
const shipped = vi.hoisted(() => ({
  brokerages: [{ name: 'moomoo', label: 'moomoo' }] as Array<{ name: string; label: string }>,
}));

vi.mock('@/hooks/useMcpServers', () => ({
  useBrokerages: () => ({ data: shipped.brokerages }),
}));

const ORDER: OrderProposal = {
  action: 'place',
  mode: 'paper',
  vendor: 'moomoo',
  account_ref: '1234567',
  instrument: { kind: 'equity', symbol: 'AAPL' },
  side: 'buy',
  qty: '1',
  order_type: 'limit',
  limit_price: '50',
};

function artifactWith(outcome: Partial<OrderOutcome> & { status: OrderStatus }) {
  return {
    type: 'order_receipt',
    direct_mcp: { server: 'moomoo', tool: 'sim_trade_input_order' },
    order_receipt: {
      attempt_id: 'attempt-1',
      vendor: 'moomoo',
      tool: 'sim_trade_input_order',
      action: 'place',
      mode: 'paper',
      account_ref: '1234567',
      order: ORDER,
      outcome,
    },
  } as Record<string, unknown>;
}

/** Fake, and deliberately full length: the point of the target line is that a
 *  person can match it character for character against the broker's own app. */
const TARGET = '11111111-2222-4333-8444-555555555555';

function cancelArtifact(outcome: Partial<OrderOutcome> & { status: OrderStatus }) {
  return {
    type: 'order_receipt',
    direct_mcp: { server: 'moomoo', tool: 'trading_order_cancel' },
    order_receipt: {
      attempt_id: 'attempt-2',
      vendor: 'moomoo',
      tool: 'trading_order_cancel',
      action: 'cancel',
      mode: 'live',
      account_ref: '1234567',
      order: {
        action: 'cancel',
        mode: 'live',
        vendor: 'moomoo',
        account_ref: '1234567',
        target_ref: TARGET,
      } satisfies OrderProposal,
      outcome,
    },
  } as Record<string, unknown>;
}

const STATUSES: OrderStatus[] = [
  'proposed',
  'approved',
  'rejected_by_user',
  'refused',
  'submitting',
  'submitted',
  'pending_confirm',
  'working',
  'partially_filled',
  'filled',
  'cancelled',
  'rejected_by_vendor',
  'failed',
  'unknown',
];

/** `t` is mocked to echo its key, so the pill's text IS the key it asked for. */
function lookup(catalog: unknown, key: string): unknown {
  return key.split('.').reduce<unknown>(
    (acc, part) =>
      acc && typeof acc === 'object' && part in (acc as object)
        ? (acc as Record<string, unknown>)[part]
        : undefined,
    catalog,
  );
}

/** A ledger row, with only the fields a receipt overlays filled in. */
function ledgerRow(overrides: Partial<OrderAttempt> = {}): OrderAttempt {
  return {
    attempt_id: 'attempt-1',
    thread_id: null,
    workspace_id: null,
    conversation_response_id: null,
    vendor: 'moomoo',
    server: 'moomoo',
    tool: 'sim_trade_input_order',
    action: 'place',
    mode: 'paper',
    account_ref: '1234567',
    asset_class: 'equity',
    order: null,
    status: 'working',
    approval_required: true,
    decided_at: null,
    decision_message: null,
    executed_at: null,
    completed_at: null,
    vendor_order_id: null,
    filled_qty: null,
    avg_fill_price: null,
    fees: null,
    action_url: null,
    route: null,
    failure: null,
    parent_attempt_id: null,
    created_at: null,
    updated_at: null,
    ...overrides,
  };
}

beforeEach(() => {
  localStorage.clear();
  shipped.brokerages = [{ name: 'moomoo', label: 'moomoo' }];
  getOrder.mockReset();
  getOrder.mockImplementation(() => new Promise<OrderAttempt>(() => {}));
});

describe('OrderReceiptCard', () => {
  it.each(STATUSES)('names a verdict for %s', (status) => {
    renderWithProviders(<OrderReceiptCard artifact={artifactWith({ status })} />);

    const pill = screen.getByTestId(`order-status-${status}`);
    const key = pill.textContent || '';
    // A sentence from the catalog, never the wire value: the card is the only
    // place a person reads what happened to their order.
    expect(key).toMatch(/^toolArtifact\.directTool\.orderStatus\./);
    expect(typeof lookup(enUS, key)).toBe('string');
  });

  it('gives each status its own verdict', () => {
    const keys = STATUSES.map((status) => {
      const { unmount } = renderWithProviders(
        <OrderReceiptCard artifact={artifactWith({ status })} />,
      );
      const key = screen.getByTestId(`order-status-${status}`).textContent || '';
      unmount();
      return key;
    });
    expect(new Set(keys).size).toBe(STATUSES.length);
  });

  it('draws the order with the account masked', () => {
    renderWithProviders(<OrderReceiptCard artifact={artifactWith({ status: 'submitted' })} />);

    const card = screen.getByTestId('order-receipt');
    expect(card).toHaveTextContent('toolArtifact.directTool.orderAction.place');
    expect(card).toHaveTextContent('plugins.detail.orderModePaper');
    expect(card).toHaveTextContent('••••4567');
    expect(card).not.toHaveTextContent('1234567');
    expect(card).toHaveTextContent('AAPL');
    expect(card).toHaveTextContent('buy');
  });

  it('shows the vendor order id, the fills and the fees when there are any', () => {
    renderWithProviders(
      <OrderReceiptCard
        artifact={artifactWith({
          status: 'partially_filled',
          vendor_order_id: '900101',
          filled_qty: '4',
          avg_fill_price: '49.5',
          fees: { amount: '0.99', currency: 'USD' },
        })}
      />,
    );

    const card = screen.getByTestId('order-receipt');
    expect(card).toHaveTextContent('900101');
    expect(card).toHaveTextContent('49.5');
    expect(card).toHaveTextContent('0.99 USD');
  });

  it('shows the failure code and message on a vendor rejection', () => {
    renderWithProviders(
      <OrderReceiptCard
        artifact={artifactWith({
          status: 'rejected_by_vendor',
          failure: { kind: 'vendor', code: '-5', message: 'backend business error' },
        })}
      />,
    );

    const card = screen.getByTestId('order-receipt');
    expect(card).toHaveTextContent('-5 backend business error');
  });

  it('hides sizes and prices while the dashboard is hiding values', () => {
    localStorage.setItem('portfolio_values_hidden', 'true');
    renderWithProviders(
      <OrderReceiptCard
        artifact={artifactWith({
          status: 'filled',
          filled_qty: '1',
          avg_fill_price: '49.5',
        })}
      />,
    );

    const card = screen.getByTestId('order-receipt');
    expect(card).not.toHaveTextContent('49.5');
    expect(card).toHaveTextContent('******');
    // What the order was is not a number, so it still reads.
    expect(card).toHaveTextContent('AAPL');
    expect(card).toHaveTextContent('••••4567');
  });

  it('renders an order the adapter could not normalize, from the attempt alone', () => {
    const artifact = artifactWith({ status: 'failed' });
    (artifact.order_receipt as Record<string, unknown>).order = null;

    renderWithProviders(<OrderReceiptCard artifact={artifact} />);

    const card = screen.getByTestId('order-receipt');
    expect(card).toHaveTextContent('••••4567');
    expect(screen.getByTestId('order-status-failed')).toBeInTheDocument();
  });

  // Whose money moved is the first thing to read on a receipt, and the vendor's
  // name in small grey text is not enough on its own.
  it('wears the vendor mark', () => {
    renderWithProviders(<OrderReceiptCard artifact={artifactWith({ status: 'submitted' })} />);

    const mark = screen.getByTestId('order-receipt').querySelector('img');
    expect(mark).not.toBeNull();
    expect(mark!.getAttribute('src')).toContain('/api/v1/mcp/brokerages/moomoo/icon');
  });

  it('falls back to the connector glyph for a vendor this build does not ship', () => {
    shipped.brokerages = [];
    renderWithProviders(<OrderReceiptCard artifact={artifactWith({ status: 'submitted' })} />);

    const card = screen.getByTestId('order-receipt');
    expect(card.querySelector('img')).toBeNull();
    expect(card.querySelector('svg.lucide-plug')).not.toBeNull();
  });

  it('offers the same attempt on the Orders page, and keeps that click to itself', () => {
    const onClick = vi.fn();
    renderWithProviders(
      <div onClick={onClick}>
        <OrderReceiptCard artifact={artifactWith({ status: 'submitted' })} />
      </div>,
    );

    const link = screen.getByRole('link', {
      name: /toolArtifact\.directTool\.orderReceipt\.viewInOrders/,
    });
    expect(link.getAttribute('href')).toBe('/orders?detail=order:attempt-1');
    fireEvent.click(link);
    expect(onClick).not.toHaveBeenCalled();
  });

  it('draws nothing for an artifact carrying no receipt', () => {
    const { container } = renderWithProviders(
      <OrderReceiptCard artifact={{ direct_mcp: { server: 'moomoo' } }} />,
    );
    expect(container.querySelector('[data-testid="order-receipt"]')).toBeNull();
  });
});

describe('the receipt for an amend', () => {
  it('names the order it acted on, whole', () => {
    renderWithProviders(<OrderReceiptCard artifact={cancelArtifact({ status: 'cancelled' })} />);

    const card = screen.getByTestId('order-receipt');
    expect(card).toHaveTextContent('toolArtifact.directTool.orderAction.cancel');
    const target = screen.getByTestId('order-target-ref');
    // Whole, not elided: this is what gets matched against the broker's app.
    expect(target.textContent).toBe(TARGET);
    expect(target.className).toContain('font-mono');
  });

  it('draws no target line for an order that acts on none', () => {
    renderWithProviders(<OrderReceiptCard artifact={artifactWith({ status: 'filled' })} />);
    expect(screen.queryByTestId('order-target-ref')).toBeNull();
  });
});

/**
 * A rejection is the one outcome whose reason came from the person reading the
 * card, and it only survives a reload because the receipt carries it.
 */
describe('the reason a person rejected with', () => {
  it('ends the field list, under the order\'s own note label', () => {
    renderWithProviders(
      <OrderReceiptCard
        artifact={artifactWith({ status: 'rejected_by_user', decision_message: 'wrong account' })}
      />,
    );

    const card = screen.getByTestId('order-receipt');
    expect(card).toHaveTextContent('toolArtifact.directTool.orderField.note');
    expect(card).toHaveTextContent('wrong account');
    expect(screen.getByTestId('order-status-rejected_by_user')).toBeInTheDocument();
  });

  // Two different sentences must never both be called Note, so the reason takes
  // a label of its own when the order already carries one.
  it('takes its own label when the order carries a note too', () => {
    const artifact = artifactWith({
      status: 'rejected_by_user',
      decision_message: 'wrong account',
    });
    const receipt = artifact.order_receipt as Record<string, unknown>;
    receipt.order = { ...ORDER, note: 'good til close' };

    renderWithProviders(<OrderReceiptCard artifact={artifact} />);

    const card = screen.getByTestId('order-receipt');
    expect(card).toHaveTextContent('toolArtifact.directTool.orderOutcome.decisionMessage');
    expect(card).toHaveTextContent('good til close');
    expect(card).toHaveTextContent('wrong account');
  });

  // The reason is free text, not a size or a price, so the eye toggle leaves it.
  it('stays readable while the dashboard is hiding values', () => {
    localStorage.setItem('portfolio_values_hidden', 'true');
    renderWithProviders(
      <OrderReceiptCard
        artifact={artifactWith({ status: 'rejected_by_user', decision_message: 'wrong account' })}
      />,
    );
    expect(screen.getByTestId('order-receipt')).toHaveTextContent('wrong account');
  });

  it('draws no row when the attempt carries none', () => {
    renderWithProviders(<OrderReceiptCard artifact={artifactWith({ status: 'filled' })} />);
    expect(screen.getByTestId('order-receipt')).not.toHaveTextContent(
      'toolArtifact.directTool.orderOutcome.decisionMessage',
    );
  });
});

describe('the way out to the broker', () => {
  it('leads the outcome row with the vendor link, and keeps the footnote', () => {
    renderWithProviders(
      <OrderReceiptCard
        artifact={artifactWith({
          status: 'submitted',
          action_url: 'https://example.com/staged/abc123',
        })}
      />,
    );

    const link = screen.getByTestId('order-action-link');
    expect(link).toHaveAttribute('href', 'https://example.com/staged/abc123');
    expect(link).toHaveAttribute('target', '_blank');
    expect(link).toHaveAttribute('rel', 'noopener noreferrer');
    expect(link).toHaveTextContent('toolArtifact.directTool.orderReceipt.openAt');
    // The Orders footnote stays where it was, as the quieter of the two.
    expect(
      screen.getByRole('link', { name: /orderReceipt\.viewInOrders/ }),
    ).toBeInTheDocument();
  });

  it('keeps its click off the surface the card sits on', () => {
    const onClick = vi.fn();
    renderWithProviders(
      <div onClick={onClick}>
        <OrderReceiptCard
          artifact={artifactWith({ status: 'submitted', action_url: 'https://example.com/a' })}
        />
      </div>,
    );
    fireEvent.click(screen.getByTestId('order-action-link'));
    expect(onClick).not.toHaveBeenCalled();
  });

  it('draws nothing when the vendor sent no link', () => {
    renderWithProviders(<OrderReceiptCard artifact={artifactWith({ status: 'submitted' })} />);
    expect(screen.queryByTestId('order-action-link')).toBeNull();
  });

  // The url is frozen when the vendor answers and nothing afterwards clears
  // it, so an instruction the broker no longer holds still carries one. Drawing
  // it beside a verdict that reads cancelled offers an action that is gone.
  it('draws nothing once the order has settled', () => {
    renderWithProviders(
      <OrderReceiptCard
        artifact={artifactWith({
          status: 'cancelled',
          action_url: 'https://example.com/staged/abc123',
        })}
      />,
    );
    expect(screen.queryByTestId('order-action-link')).toBeNull();
  });

  // The href comes off a brokerage adapter, and an anchor is where a string
  // becomes code. A dead button on an order card beats a live one here.
  it('refuses a link that is not http', () => {
    renderWithProviders(
      <OrderReceiptCard
        artifact={artifactWith({
          status: 'submitted',
          action_url: 'javascript:alert(1)',
        })}
      />,
    );
    expect(screen.queryByTestId('order-action-link')).toBeNull();
  });
});

describe('orderReceiptOf', () => {
  it('reads a receipt with no status as unknown rather than as no receipt', () => {
    const receipt = orderReceiptOf({ order_receipt: { attempt_id: 'a1' } });
    expect(receipt?.attempt_id).toBe('a1');
    expect(receipt?.outcome.status).toBe('unknown');
  });

  it('is null when the artifact carries none', () => {
    expect(orderReceiptOf({ direct_mcp: {} })).toBeNull();
    expect(orderReceiptOf(null)).toBeNull();
  });
});

describe('orderOutcomeRows', () => {
  it('draws only the fields the vendor answered', () => {
    expect(orderOutcomeRows({ status: 'submitted' })).toEqual([]);
    expect(orderOutcomeRows({ status: 'submitted', vendor_order_id: '1' })).toHaveLength(1);
  });

  it('falls back to the failure kind when there is no message', () => {
    const rows = orderOutcomeRows({ status: 'failed', failure: { kind: 'transport' } });
    expect(rows[0].value).toBe('transport');
  });
});

describe('the copy the receipt asks for', () => {
  // The verdicts and the outcome labels reach `t()` through a map, so the
  // tree-wide locale sweep cannot see them. This is their coverage.
  it.each(ORDER_RECEIPT_KEYS)('%s resolves in both catalogs', (key) => {
    expect(typeof lookup(enUS, key)).toBe('string');
    expect(typeof lookup(zhCN, key)).toBe('string');
  });
});

/**
 * A staged instruction reached the vendor and reached no market. It waits in
 * the broker's own client for the user to confirm it, and expires in seven
 * days if they never do, so "Sent to the brokerage" would tell a person their
 * order is working when nothing of the kind is true.
 */
describe('what a staged order says it is waiting on', () => {
  function stagedArtifact(status: OrderStatus = 'submitted') {
    const artifact = artifactWith({ status });
    const receipt = artifact.order_receipt as Record<string, unknown>;
    receipt.mode = 'staged';
    receipt.vendor = 'ibkr';
    receipt.order = { ...ORDER, mode: 'staged', vendor: 'ibkr' };
    return artifact;
  }

  // `t` here echoes its key, so the assertion is which sentence was asked for;
  // that the broker's name lands inside it is `orderStatusLabel`'s own test.
  it('asks for the staged sentence rather than the sent one', () => {
    shipped.brokerages = [{ name: 'ibkr', label: 'Interactive Brokers' }];
    renderWithProviders(<OrderReceiptCard artifact={stagedArtifact()} />);

    expect(screen.getByTestId('order-status-submitted')).toHaveTextContent(
      'toolArtifact.directTool.orderStatus.submittedStaged',
    );
  });

  // Every other mode kept the word it had: a paper or live order that reached
  // the broker really was sent.
  it('leaves every other mode alone', () => {
    renderWithProviders(<OrderReceiptCard artifact={artifactWith({ status: 'submitted' })} />);
    expect(screen.getByTestId('order-status-submitted')).toHaveTextContent(
      'toolArtifact.directTool.orderStatus.submitted',
    );
    expect(screen.getByTestId('order-status-submitted')).not.toHaveTextContent(
      'Staged',
    );
  });

  it('changes no other staged state', () => {
    renderWithProviders(<OrderReceiptCard artifact={stagedArtifact('cancelled')} />);
    expect(screen.getByTestId('order-status-cancelled')).toHaveTextContent(
      'toolArtifact.directTool.orderStatus.cancelled',
    );
  });
});

/**
 * The card was drawn from an artifact frozen when the tool answered, so a fill
 * or a cancel that reconciliation records minutes later would never reach the
 * thread. The card asks the ledger for its row and lays it over the artifact.
 */
describe('the receipt following the ledger', () => {
  it('renders the artifact first and takes the row when it lands', async () => {
    getOrder.mockResolvedValue(
      ledgerRow({ status: 'cancelled', vendor_order_id: '900105' }),
    );
    renderWithProviders(<OrderReceiptCard artifact={artifactWith({ status: 'submitted' })} />);

    // No flash of nothing: what the tool answered is on screen immediately.
    expect(screen.getByTestId('order-status-submitted')).toBeInTheDocument();
    await waitFor(() =>
      expect(screen.getByTestId('order-status-cancelled')).toBeInTheDocument(),
    );
    expect(screen.queryByTestId('order-status-submitted')).toBeNull();
    expect(screen.getByTestId('order-receipt')).toHaveTextContent('900105');
    expect(getOrder).toHaveBeenCalledWith('attempt-1');
  });

  it('keeps the artifact when the ledger cannot be read', async () => {
    getOrder.mockRejectedValue(new Error('nope'));
    renderWithProviders(<OrderReceiptCard artifact={artifactWith({ status: 'submitted' })} />);

    await waitFor(() => expect(getOrder).toHaveBeenCalled());
    expect(screen.getByTestId('order-status-submitted')).toBeInTheDocument();
    expect(screen.getByTestId('order-receipt')).toHaveTextContent('AAPL');
  });

  it('asks for nothing when the artifact carries no receipt', () => {
    renderWithProviders(<OrderReceiptCard artifact={{ direct_mcp: { server: 'moomoo' } }} />);
    expect(getOrder).not.toHaveBeenCalled();
  });
});

describe('overlayLedgerRow', () => {
  const receipt: OrderReceipt = {
    attempt_id: 'attempt-1',
    vendor: 'moomoo',
    action: 'place',
    mode: 'paper',
    order: ORDER,
    outcome: { status: 'submitted', vendor_order_id: '900101' },
  };

  it('is the artifact until a row arrives', () => {
    expect(overlayLedgerRow(receipt, undefined)).toBe(receipt);
    expect(overlayLedgerRow(null, ledgerRow())).toBeNull();
  });

  // Two receipts for two orders sit in one thread, and a row that answered a
  // different card must never repaint this one.
  it('refuses a row for another attempt', () => {
    expect(overlayLedgerRow(receipt, ledgerRow({ attempt_id: 'attempt-9' }))).toBe(
      receipt,
    );
  });

  it('takes the fields the ledger owns and leaves the rest', () => {
    const merged = overlayLedgerRow(
      receipt,
      ledgerRow({
        status: 'partially_filled',
        vendor_order_id: '900105',
        filled_qty: '4',
        avg_fill_price: '49.5',
        fees: { amount: '0.99', currency: 'USD' },
        action_url: 'https://example.com/staged/abc',
        completed_at: '2026-09-10T00:00:00Z',
        failure: { kind: 'vendor', code: '-5', message: 'no' },
      }),
    );
    expect(merged?.outcome).toMatchObject({
      status: 'partially_filled',
      vendor_order_id: '900105',
      filled_qty: '4',
      avg_fill_price: '49.5',
      fees: { amount: '0.99', currency: 'USD' },
      action_url: 'https://example.com/staged/abc',
      completed_at: '2026-09-10T00:00:00Z',
      failure: { kind: 'vendor', code: '-5', message: 'no' },
    });
    // The order is what was sent, and the ledger is a summary of it.
    expect(merged?.order).toBe(ORDER);
    expect(merged?.vendor).toBe('moomoo');
  });

  // A lost answer froze a transport failure into the artifact. The ledger
  // clears it once the vendor lists the order, and the card follows the ledger.
  it('drops a failure the ledger cleared after the artifact froze it', () => {
    const lost: OrderReceipt = {
      ...receipt,
      outcome: { status: 'unknown', failure: { kind: 'transport' } },
    };
    const merged = overlayLedgerRow(lost, ledgerRow({ status: 'filled' }));
    expect(merged?.outcome.status).toBe('filled');
    expect(merged?.outcome.failure).toBeNull();
  });

  // A settled receipt reads its row once and then never asks again, so it can be
  // served an older open row another surface left in the cache. Laying that over
  // a verdict the vendor already gave would retract a final answer on screen.
  it('refuses an open row against a receipt already settled', () => {
    const settled: OrderReceipt = {
      ...receipt,
      outcome: { status: 'filled', vendor_order_id: '900101', filled_qty: '10' },
    };
    expect(overlayLedgerRow(settled, ledgerRow({ status: 'working' }))).toBe(settled);
    // Nothing on the stale row is taken, not only its status.
    expect(
      overlayLedgerRow(settled, ledgerRow({ status: 'working', filled_qty: '2' }))
        ?.outcome.filled_qty,
    ).toBe('10');
    // The guard is on the direction, not the overlay: a terminal row still lands,
    // which is what carries a late enrichment onto a settled card.
    expect(
      overlayLedgerRow(settled, ledgerRow({ status: 'cancelled' }))?.outcome.status,
    ).toBe('cancelled');
  });

  it('leaves the order alone: only the outcome is the ledger\'s', () => {
    const merged = overlayLedgerRow(
      receipt,
      ledgerRow({ order: { instrument: { kind: 'equity', symbol: 'AAPL' } } }),
    );
    expect(merged?.order).toBe(ORDER);
  });
});

/**
 * Reconciliation reads the vendor's own listing after the fact, so a contract
 * id the adapter could only echo back becomes a symbol a person can read. The
 * card takes the better name; the order it was drawn from is left as sent.
 */
describe('the instrument the ledger can name', () => {
  it('draws the row from the ledger when the artifact could only echo a code', async () => {
    getOrder.mockResolvedValue(
      ledgerRow({ order: { instrument: { kind: 'equity', symbol: 'AAPL' } } }),
    );
    const artifact = artifactWith({ status: 'submitted' });
    (artifact.order_receipt as Record<string, unknown>).order = {
      ...ORDER,
      instrument: { kind: 'opaque', raw_code: '265598' },
    };
    renderWithProviders(<OrderReceiptCard artifact={artifact} />);

    await waitFor(() =>
      expect(screen.getByTestId('order-receipt')).toHaveTextContent('AAPL'),
    );
    expect(screen.getByTestId('order-receipt')).not.toHaveTextContent('265598');
  });
});

/**
 * A number under "Filled quantity" does not say whether an order is done. The
 * question a partial fill raises is how much of it is left, so where the order
 * says what was asked for, the fill is written as a sentence.
 */
describe('a fill written as progress', () => {
  it('says how much of the order filled', () => {
    const rows = orderOutcomeRows(
      { status: 'partially_filled', filled_qty: '4' },
      { action: 'place', mode: 'paper', qty: '10' } satisfies OrderProposal,
    );
    expect(rows).toEqual([
      {
        field: 'filled_qty',
        labelKey: 'toolArtifact.directTool.orderOutcome.fill',
        value: '4 / 10',
        valueKey: 'toolArtifact.directTool.orderOutcome.filledOf',
        valueParams: { filled: '4', qty: '10' },
      },
    ]);
  });

  it('says what a completed fill cost, in one row instead of two', () => {
    const rows = orderOutcomeRows(
      { status: 'filled', filled_qty: '10', avg_fill_price: '49.5' },
      { action: 'place', mode: 'paper', qty: '10' } satisfies OrderProposal,
    );
    expect(rows).toEqual([
      {
        field: 'filled_qty',
        labelKey: 'toolArtifact.directTool.orderOutcome.fill',
        value: '10 @ 49.5',
        valueKey: 'toolArtifact.directTool.orderOutcome.filledAt',
        valueParams: { filled: '10', price: '49.5' },
      },
    ]);
  });

  // The sentence needs both halves. Without them the figures go back to a row
  // each, which is what every other status has always drawn.
  it('falls back to a row each when a half is missing', () => {
    expect(
      orderOutcomeRows({ status: 'partially_filled', filled_qty: '4' }).map((r) => r.labelKey),
    ).toEqual(['toolArtifact.directTool.orderOutcome.filled']);
    expect(
      orderOutcomeRows(
        { status: 'filled', filled_qty: '10' },
        { action: 'place', mode: 'paper', qty: '10' } satisfies OrderProposal,
      ).map((r) => r.labelKey),
    ).toEqual(['toolArtifact.directTool.orderOutcome.filled']);
    expect(
      orderOutcomeRows(
        { status: 'working', filled_qty: '4', avg_fill_price: '49.5' },
        { action: 'place', mode: 'paper', qty: '10' } satisfies OrderProposal,
      ).map((r) => r.labelKey),
    ).toEqual([
      'toolArtifact.directTool.orderOutcome.filled',
      'toolArtifact.directTool.orderOutcome.avgFillPrice',
    ]);
  });

  it('draws the sentence on the card', () => {
    const artifact = artifactWith({ status: 'partially_filled', filled_qty: '4' });
    (artifact.order_receipt as Record<string, unknown>).order = { ...ORDER, qty: '10' };
    renderWithProviders(<OrderReceiptCard artifact={artifact} />);
    expect(screen.getByTestId('order-receipt')).toHaveTextContent(
      'toolArtifact.directTool.orderOutcome.filledOf',
    );
  });

  // A sentence carrying a size is the size, so the eye toggle has to take it
  // with the figures it was asked to hide.
  it('is hidden with the other amounts', () => {
    localStorage.setItem('portfolio_values_hidden', 'true');
    const artifact = artifactWith({ status: 'partially_filled', filled_qty: '4' });
    (artifact.order_receipt as Record<string, unknown>).order = { ...ORDER, qty: '10' };
    renderWithProviders(<OrderReceiptCard artifact={artifact} />);

    const card = screen.getByTestId('order-receipt');
    expect(card).not.toHaveTextContent('toolArtifact.directTool.orderOutcome.filledOf');
    expect(card).toHaveTextContent('******');
  });

  /**
   * A live order still working comes back from the broker with a filled
   * quantity of 0 and an average price of 0. That is the vendor saying nothing
   * has happened yet, not an answer, and "Average fill price 0" on a card
   * about somebody's trade is worse than no row at all.
   */
  it('reads a vendor zero as nothing filled, not as a fill', () => {
    expect(
      orderOutcomeRows(
        { status: 'working', filled_qty: '0', avg_fill_price: '0' },
        { action: 'place', mode: 'live', qty: '10' } satisfies OrderProposal,
      ),
    ).toEqual([]);
    // Still drawn once the order really does start filling.
    expect(
      orderOutcomeRows(
        { status: 'partially_filled', filled_qty: '1', avg_fill_price: '0' },
        { action: 'place', mode: 'live', qty: '10' } satisfies OrderProposal,
      ).map((r) => r.valueKey),
    ).toEqual(['toolArtifact.directTool.orderOutcome.filledOf']);
  });

  // The sentences are the only copy on this card with parameters in it, and a
  // catalog that dropped one would read as a fill of nothing.
  it('keeps both halves of each sentence in both catalogs', () => {
    for (const catalog of [enUS, zhCN]) {
      const of = lookup(catalog, 'toolArtifact.directTool.orderOutcome.filledOf') as string;
      const at = lookup(catalog, 'toolArtifact.directTool.orderOutcome.filledAt') as string;
      expect(of).toContain('{{filled}}');
      expect(of).toContain('{{qty}}');
      expect(at).toContain('{{filled}}');
      expect(at).toContain('{{price}}');
    }
  });
});
