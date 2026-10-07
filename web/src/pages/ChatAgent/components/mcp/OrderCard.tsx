import React from 'react';
import { useTranslation } from 'react-i18next';
import type { OrderAction, OrderMode } from '@/types/orders';
import { cn } from '@/lib/utils';
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
 *
 * `onOpen` makes the whole card the way to the call's detail, as every other
 * card in a thread is. Only a card with nothing to answer takes it: one with
 * Approve on it is not a thing to open by a stray click.
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
  onOpen,
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
  onOpen?: () => void;
  testid?: string;
}): React.ReactElement {
  const { t } = useTranslation();
  const owner = [vendorLabel, account].filter(Boolean).join(' · ');
  const open = onOpen
    ? (e: React.MouseEvent) => {
        // A link on the card goes where it says, and a drag that selected
        // text was reading, not asking for the panel.
        if ((e.target as Element).closest('a, button, input')) return;
        if (window.getSelection()?.toString()) return;
        onOpen();
      }
    : undefined;
  // A key pressed on a link inside the card is the link's own.
  const openByKey = onOpen
    ? (e: React.KeyboardEvent) => {
        if (e.target !== e.currentTarget || (e.key !== 'Enter' && e.key !== ' ')) return;
        e.preventDefault();
        onOpen();
      }
    : undefined;
  return (
    <div
      // The fill lives in the class (not style) so the hover twin can win, as
      // on the tool-call rows; a border a shade darker did not read as hover.
      className={cn(
        'rounded-lg px-4 py-3 border border-(--color-border-muted) bg-(--color-bg-card)',
        onOpen && 'cursor-pointer transition-colors hover:bg-(--color-bg-card-hover)',
      )}
      role={onOpen ? 'button' : undefined}
      tabIndex={onOpen ? 0 : undefined}
      onClick={open}
      onKeyDown={openByKey}
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
      {footer && (
        <div className="mt-2.5 pt-2.5" style={{ borderTop: '1px solid var(--color-border-muted)' }}>
          {footer}
        </div>
      )}
    </div>
  );
}
