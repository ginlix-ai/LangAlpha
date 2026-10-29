import React from 'react';
import { useTranslation } from 'react-i18next';
import type { OrderAction, OrderMode } from '@/types/orders';
import { Disclosure } from '@/components/ui/Disclosure';
import { DirectToolRowMark } from './DirectToolMark';
import { OrderModeBadge } from '@/components/orders/OrderModeBadge';
import { OrderTicket } from './OrderTicket';
import { ORDER_ACTION_KEY, type OrderTicketView } from './orderSummary';

/**
 * An order waiting on a person: a header saying what the call does and in
 * whose account, the order itself, and a footer for the decision.
 *
 * The slots are the moments of the decision: the pill says where the order
 * stands, the notice what the card could not read, the footer the buttons.
 * Once the order is answered the thread draws its receipt instead, in the
 * transcript's terminal type with the other data cards (`OrderReceiptCard`),
 * from the same ticket parts. This card is never opened by a click: one with
 * Approve on it is not a thing to open by a stray one.
 */
export function OrderCard({
  vendor,
  action,
  mode,
  vendorLabel,
  account,
  pill,
  ticket,
  notice,
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
  /** Said under the order and before the footer, so it is read before the
   *  buttons are. */
  notice?: React.ReactNode;
  footer?: React.ReactNode;
  testid?: string;
}): React.ReactElement {
  const { t } = useTranslation();
  const owner = [vendorLabel, account].filter(Boolean).join(' · ');
  return (
    <div
      className="rounded-lg px-4 py-3 border border-(--color-border-muted) bg-(--color-bg-card)"
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
      {notice}
      {/* Folds shut rather than dropping out: an approved order keeps its card
          until the brokerage answers, and a footer gone in one frame jumped
          everything below it. Padding, not margin, so the space above the
          rule folds with it. */}
      <Disclosure open={!!footer} className="pt-2.5">
        <div className="pt-2.5" style={{ borderTop: '1px solid var(--color-border-muted)' }}>
          {footer}
        </div>
      </Disclosure>
    </div>
  );
}
