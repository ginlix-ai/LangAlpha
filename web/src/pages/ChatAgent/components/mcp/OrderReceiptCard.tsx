import React from 'react';
import { OrderStatusPill } from '@/components/orders/OrderStatusPill';
import { OrderActionLink } from '@/components/orders/OrderActionLink';
import { OrdersPageLink } from '@/components/orders/OrdersPageLink';
import { OrderCard } from './OrderCard';
import { OrderFieldList } from './OrderFields';
import { useOrderReceipt } from './useOrderReceipt';

/**
 * What happened to one order, drawn from the attempt's receipt.
 *
 * The receipt rides the tool result, so this is the same card live and on a
 * reload, and it is the only place in chat where an order's end state is
 * stated in words. A rejection, a refusal and a transport failure all carry a
 * receipt too, which is why the outcome is a named verdict rather than a
 * success-or-not: "you rejected this" and "the brokerage rejected this" are
 * different facts and a person reading back a thread needs to tell them apart.
 *
 * Nothing here is a JSON tree, and the card opens nothing on click: an order is
 * not a thing to click by accident, and the two ways off it, the vendor's own
 * client and the same attempt on the Orders page, are stated as links.
 * `useOrderReceipt` decides what is on it; this draws that.
 */
export function OrderReceiptCard({
  artifact,
}: {
  artifact: Record<string, unknown>;
}): React.ReactElement | null {
  const receipt = useOrderReceipt(artifact);
  if (!receipt) return null;

  const { attemptId, outcomeRows, actionUrl, vendorLabel } = receipt;
  const footer =
    outcomeRows.length > 0 || actionUrl || attemptId ? (
      <div className="flex items-end justify-between gap-3 flex-wrap">
        <div className="min-w-0">{outcomeRows.length > 0 && <OrderFieldList rows={outcomeRows} />}</div>
        <div className="flex items-center gap-3 shrink-0">
          <OrderActionLink
            href={actionUrl}
            vendorLabel={vendorLabel}
            status={receipt.status}
          />
          {attemptId && <OrdersPageLink attemptId={attemptId} />}
        </div>
      </div>
    ) : null;

  return (
    <OrderCard
      vendor={receipt.vendor}
      action={receipt.action}
      targetRef={receipt.targetRef}
      mode={receipt.mode}
      vendorLabel={vendorLabel}
      pill={
        <OrderStatusPill
          status={receipt.status}
          mode={receipt.mode}
          vendorLabel={vendorLabel}
          surface="receipt"
        />
      }
      rows={receipt.rows}
      footer={footer}
      testid="order-receipt"
    />
  );
}
