import React from 'react';
import { useTranslation } from 'react-i18next';
import type { OrderAction, OrderMode } from '@/types/orders';
import { DirectToolRowMark } from './DirectToolMark';
import { OrderModeBadge } from '@/components/orders/OrderModeBadge';
import { OrderTicket } from './OrderTicket';
import { ORDER_ACTION_KEY, type OrderTicketView } from './orderSummary';

/**
 * The one shape an order wears in a thread, whichever moment of its life it is
 * in: a header saying what the call does and in whose account, the order
 * itself, and a footer.
 *
 * The card a person approves and the receipt they read afterwards are the same
 * record seen twice, so they are one component rather than two that resemble
 * each other. Everything that differs between the two moments is a slot: the
 * pill says where the order stands, the footer carries either the decision or
 * what the brokerage answered.
 */
export function OrderCard({
  vendor,
  action,
  mode,
  vendorLabel,
  account,
  pill,
  ticket,
  footer,
  testid,
}: {
  vendor: string;
  action: OrderAction;
  mode?: OrderMode | null;
  vendorLabel: string;
  /** Masked by the caller. It rides the vendor's name because together they
   *  answer one question, whose money this is. */
  account?: string | null;
  pill?: React.ReactNode;
  ticket?: OrderTicketView | null;
  footer?: React.ReactNode;
  testid?: string;
}): React.ReactElement {
  const { t } = useTranslation();
  const owner = [vendorLabel, account].filter(Boolean).join(' · ');
  return (
    <div
      className="rounded-lg px-4 py-3"
      style={{
        backgroundColor: 'var(--color-bg-card)',
        border: '1px solid var(--color-border-muted)',
      }}
      data-testid={testid}
    >
      <div className="flex items-center gap-2 flex-wrap pb-2">
        {/* The brokerage's own mark, at the size the timeline row would have
            given this call: on a card about an order, whose money moves is the
            first thing to read, and the vendor's name alone is quiet. */}
        <DirectToolRowMark server={vendor} />
        <span className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
          {t(ORDER_ACTION_KEY[action] || ORDER_ACTION_KEY.place)}
        </span>
        <OrderModeBadge mode={mode} />
        {owner && (
          <span className="text-xs tabular-nums" style={{ color: 'var(--color-text-tertiary)' }}>
            {owner}
          </span>
        )}
        {pill && <span className="ml-auto">{pill}</span>}
      </div>
      {ticket && <OrderTicket ticket={ticket} />}
      {footer && (
        <div className="mt-2.5 pt-2.5" style={{ borderTop: '1px solid var(--color-border-muted)' }}>
          {footer}
        </div>
      )}
    </div>
  );
}
