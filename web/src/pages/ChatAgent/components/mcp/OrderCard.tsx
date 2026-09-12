import React from 'react';
import { useTranslation } from 'react-i18next';
import type { OrderAction, OrderMode } from '@/types/orders';
import { DirectToolRowMark } from './DirectToolMark';
import { OrderModeBadge } from '@/components/orders/OrderModeBadge';
import { OrderFieldList, OrderTargetRef } from './OrderFields';
import { ORDER_ACTION_KEY, type OrderSummaryRow } from './orderSummary';

/**
 * The one shape an order wears in a thread, whichever moment of its life it is
 * in: the vendor's mark, what the call would do, the mode, the brokerage, a
 * verdict pill, the fields, and a footer.
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
  targetRef,
  mode,
  vendorLabel,
  pill,
  rows,
  footer,
  testid,
}: {
  vendor: string;
  action: OrderAction;
  targetRef?: string | null;
  mode?: OrderMode | null;
  vendorLabel: string;
  pill?: React.ReactNode;
  rows: OrderSummaryRow[];
  footer?: React.ReactNode;
  testid?: string;
}): React.ReactElement {
  const { t } = useTranslation();
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
        <OrderTargetRef targetRef={targetRef} />
        <OrderModeBadge mode={mode} />
        <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
          {vendorLabel}
        </span>
        {pill && <span className="ml-auto">{pill}</span>}
      </div>
      <OrderFieldList rows={rows} />
      {footer && (
        <div className="mt-2 pt-2" style={{ borderTop: '1px solid var(--color-border-muted)' }}>
          {footer}
        </div>
      )}
    </div>
  );
}
