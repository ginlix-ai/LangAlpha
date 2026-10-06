import React from 'react';
import { useTranslation } from 'react-i18next';
import { AlertCircle } from 'lucide-react';
import { OrderStatusPill } from '@/components/orders/OrderStatusPill';
import { OrderActionLink } from '@/components/orders/OrderActionLink';
import { OrdersPageLink } from '@/components/orders/OrdersPageLink';
import { OrderCard } from './OrderCard';
import { OrderPartValue } from './OrderTicket';
import { useOrderReceipt, type OrderReceiptView } from './useOrderReceipt';

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
  const { decisionMessage, failure, fill, orderId } = receipt.outcome;
  const answered =
    !!(decisionMessage || failure || fill.length || orderId || receipt.actionUrl || receipt.attemptId);
  return (
    <OrderCard
      vendor={receipt.vendor}
      action={receipt.action}
      mode={receipt.mode}
      vendorLabel={receipt.vendorLabel}
      account={receipt.account}
      pill={
        <OrderStatusPill
          status={receipt.status}
          mode={receipt.mode}
          vendorLabel={receipt.vendorLabel}
          surface="receipt"
        />
      }
      ticket={receipt.ticket}
      footer={answered ? <OrderOutcomeBlock receipt={receipt} /> : null}
      testid="order-receipt"
    />
  );
}

/**
 * The answer under the order, a line per thing there is to say: the reason
 * the person gave, why the brokerage refused, what filled, and the ids and
 * links that lead off the card.
 */
function OrderOutcomeBlock({ receipt }: { receipt: OrderReceiptView }): React.ReactElement {
  const { t } = useTranslation();
  const { decisionMessage, failure, fill, orderId } = receipt.outcome;
  const quiet = { color: 'var(--color-text-tertiary)' };
  return (
    <div className="flex flex-col gap-1.5 text-xs leading-relaxed" data-testid="order-outcome">
      {decisionMessage && (
        <p className="[overflow-wrap:anywhere]" style={{ color: 'var(--color-text-primary)' }}>
          <span style={quiet}>{t('toolArtifact.directTool.orderOutcome.decisionMessage')}</span>{' '}
          {decisionMessage}
        </p>
      )}
      {failure && (
        // The vendor's own sentence first, since it is what a person can act
        // on; the code is for matching against the broker's documentation.
        <p className="flex gap-1.5" style={{ color: 'var(--color-loss)' }} data-testid="order-failure">
          <AlertCircle className="h-3.5 w-3.5 shrink-0 mt-[3px]" aria-hidden="true" />
          <span className="min-w-0 [overflow-wrap:anywhere]">
            {failure.message}
            {failure.message && failure.code && ' '}
            {failure.code && (
              <span className="whitespace-nowrap" style={quiet}>
                {t('toolArtifact.directTool.orderOutcome.failureCode', { code: failure.code })}
              </span>
            )}
          </span>
        </p>
      )}
      {fill.length > 0 && (
        <p className="tabular-nums" style={{ color: 'var(--color-text-primary)' }} data-testid="order-fill">
          {fill.map((part, index) => (
            <React.Fragment key={part.field}>
              {index > 0 && (
                <span aria-hidden="true" className="px-1.5" style={quiet}>
                  ·
                </span>
              )}
              {part.labelKey && <span style={quiet}>{t(part.labelKey)} </span>}
              <OrderPartValue part={part} />
            </React.Fragment>
          ))}
        </p>
      )}
      <div className="flex items-center justify-between flex-wrap gap-x-3 gap-y-1.5">
        {orderId && (
          <span className="min-w-0" style={quiet}>
            {t('toolArtifact.directTool.orderOutcome.orderId')}{' '}
            <span className="font-mono break-all" style={{ color: 'var(--color-text-secondary)' }}>
              {orderId}
            </span>
          </span>
        )}
        <div className="flex items-center gap-3 ml-auto">
          <OrderActionLink
            href={receipt.actionUrl}
            vendorLabel={receipt.vendorLabel}
            status={receipt.status}
          />
          {receipt.attemptId && <OrdersPageLink attemptId={receipt.attemptId} />}
        </div>
      </div>
    </div>
  );
}
