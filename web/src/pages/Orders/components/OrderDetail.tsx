import { useId } from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import { ArrowUpRight } from 'lucide-react';
import { instrumentLabel } from '@/components/orders/instrument';
import { OrderStatusPill } from '@/components/orders/OrderStatusPill';
import {
  ListError,
  ListSkeleton,
} from '@/components/mcp/McpPrimitives';
import { OrderActionLink } from '@/components/orders/OrderActionLink';
import { OrderModeBadge } from '@/components/orders/OrderModeBadge';
import { useDirectToolVendorLabel } from '@/pages/ChatAgent/components/mcp/useDirectToolVendor';
import { maskAccountId } from '@/pages/ChatAgent/utils/directTools';
import { isDefinitiveOrderError, useOrder } from '@/hooks/useOrders';
import { fillFigure, type OrderAttempt } from '@/pages/ChatAgent/utils/api';
import {
  DetailField,
  DetailOverlay,
  DetailSection,
} from '@/pages/Plugins/components/DetailOverlay';
import {
  formatOrderTime,
  orderAmount,
  orderLimitPrice,
  orderSize,
  orderStopPrice,
  HIDDEN,
} from '../utils/format';
import {
  ORDER_ACTION_KEY,
  ORDER_ASSET_CLASS_KEY,
  ORDER_SIDE_KEY,
  ORDER_TIME_IN_FORCE_KEY,
  ORDER_TYPE_KEY,
} from '../utils/labels';

function Body({
  order,
  valuesHidden,
}: {
  order: OrderAttempt;
  valuesHidden: boolean;
}) {
  const { t, i18n } = useTranslation();
  const locale = i18n.language;
  const summary = order.order;
  const none = t('orders.detail.none');
  const assetKey = summary?.asset_class
    ? ORDER_ASSET_CLASS_KEY[summary.asset_class]
    : undefined;
  const sideKey = summary?.side ? ORDER_SIDE_KEY[summary.side] : undefined;
  const typeKey = summary?.order_type ? ORDER_TYPE_KEY[summary.order_type] : undefined;
  const tifKey = summary?.time_in_force
    ? ORDER_TIME_IN_FORCE_KEY[summary.time_in_force]
    : undefined;
  const route = Object.entries(order.route ?? {});
  const filledQty = fillFigure(order.filled_qty);
  const avgFillPrice = fillFigure(order.avg_fill_price);
  const limitPrice = orderLimitPrice(summary, { hidden: valuesHidden, locale });
  const stopPrice = orderStopPrice(summary, { hidden: valuesHidden, locale });

  return (
    <>
      {summary && (
        <DetailSection title={t('orders.detail.order')}>
          {summary.target_ref && (
            <DetailField label={t('orders.detail.target')}>
              {summary.target_ref}
            </DetailField>
          )}
          <DetailField label={t('orders.detail.assetClass')}>
            {assetKey ? t(assetKey) : summary.asset_class || none}
          </DetailField>
          <DetailField label={t('orders.detail.instrument')}>
            {instrumentLabel(summary.instrument) || none}
          </DetailField>
          <DetailField label={t('orders.detail.side')}>
            {sideKey ? t(sideKey) : summary.side || none}
          </DetailField>
          <DetailField label={t('orders.detail.size')}>
            {orderSize(summary, { hidden: valuesHidden, locale }) || none}
          </DetailField>
          <DetailField label={t('orders.detail.orderType')}>
            {typeKey ? t(typeKey) : summary.order_type || none}
          </DetailField>
          {/* Only the prices the order carries, as on the approval card: a
              market order has none, and "Not recorded" in that slot would
              read as a price the ledger lost. */}
          {limitPrice && (
            <DetailField label={t('toolArtifact.directTool.orderField.limitPrice')}>
              {limitPrice}
            </DetailField>
          )}
          {stopPrice && (
            <DetailField label={t('toolArtifact.directTool.orderField.stopPrice')}>
              {stopPrice}
            </DetailField>
          )}
          <DetailField label={t('orders.detail.timeInForce')}>
            {tifKey ? t(tifKey) : summary.time_in_force || none}
          </DetailField>
          <DetailField label={t('orders.detail.session')}>
            {summary.session || none}
          </DetailField>
          {summary.note && (
            <DetailField label={t('orders.detail.note')}>{summary.note}</DetailField>
          )}
        </DetailSection>
      )}

      {(filledQty || avgFillPrice || order.fees?.amount) && (
        <DetailSection title={t('orders.detail.fill')}>
          <DetailField label={t('orders.detail.filledQty')}>
            {filledQty ? (valuesHidden ? HIDDEN : filledQty) : none}
          </DetailField>
          <DetailField label={t('orders.detail.avgFillPrice')}>
            {orderAmount(avgFillPrice, summary?.currency, {
              hidden: valuesHidden,
              locale,
            }) || none}
          </DetailField>
          <DetailField label={t('orders.detail.fees')}>
            {orderAmount(order.fees?.amount, order.fees?.currency, {
              hidden: valuesHidden,
              locale,
            }) || none}
          </DetailField>
        </DetailSection>
      )}

      <DetailSection title={t('orders.detail.timeline')}>
        <DetailField label={t('orders.detail.created')}>
          {formatOrderTime(order.created_at) || none}
        </DetailField>
        <DetailField label={t('orders.detail.decided')}>
          {formatOrderTime(order.decided_at) || none}
        </DetailField>
        <DetailField label={t('orders.detail.executed')}>
          {formatOrderTime(order.executed_at) || none}
        </DetailField>
        <DetailField label={t('orders.detail.completed')}>
          {formatOrderTime(order.completed_at) || none}
        </DetailField>
        <DetailField label={t('orders.detail.approval')}>
          {t(
            order.approval_required
              ? 'orders.detail.approvalRequired'
              : 'orders.detail.approvalNotRequired',
          )}
        </DetailField>
        {order.decision_message && (
          <DetailField label={t('orders.detail.decisionMessage')}>
            {order.decision_message}
          </DetailField>
        )}
      </DetailSection>

      {order.failure && (
        <DetailSection title={t('orders.detail.failure')}>
          <DetailField label={t('orders.detail.failureKind')}>
            {order.failure.kind}
          </DetailField>
          {order.failure.code && (
            <DetailField label={t('orders.detail.failureCode')}>
              {order.failure.code}
            </DetailField>
          )}
          {order.failure.message && (
            <DetailField label={t('orders.detail.failureMessage')}>
              {order.failure.message}
            </DetailField>
          )}
        </DetailSection>
      )}

      {route.length > 0 && (
        <DetailSection title={t('orders.detail.route')}>
          {route.map(([key, value]) => (
            <DetailField key={key} label={key}>
              {String(value)}
            </DetailField>
          ))}
        </DetailSection>
      )}

      <DetailSection title={t('orders.detail.provenance')}>
        <DetailField label={t('orders.detail.vendor')}>{order.vendor}</DetailField>
        <DetailField label={t('orders.detail.account')}>
          {order.account_ref ? maskAccountId(order.account_ref) : none}
        </DetailField>
        <DetailField label={t('orders.detail.tool')}>
          {order.tool || none}
        </DetailField>
        <DetailField label={t('orders.detail.vendorOrderId')}>
          {order.vendor_order_id || none}
        </DetailField>
        <DetailField label={t('orders.detail.attemptId')}>
          {order.attempt_id}
        </DetailField>
        {order.parent_attempt_id && (
          <DetailField label={t('orders.detail.parent')}>
            {order.parent_attempt_id}
          </DetailField>
        )}
        <DetailField label={t('orders.detail.thread')}>
          {order.thread_id ? (
            <Link
              to={`/chat/t/${order.thread_id}`}
              className="inline-flex items-center gap-1 hover:underline"
              style={{ color: 'var(--color-accent-primary)' }}
            >
              {t('orders.detail.openThread')}
              <ArrowUpRight className="h-3 w-3" />
            </Link>
          ) : (
            none
          )}
        </DetailField>
      </DetailSection>
    </>
  );
}

/**
 * One attempt in full, opened from `?detail=order:<attempt_id>`.
 *
 * Fetched by id rather than read off the row that opened it: the link is
 * shareable with the user's own other tab, where no page of the list has
 * necessarily loaded, and the row is a summary either way.
 */
export function OrderDetail({
  attemptId,
  valuesHidden,
  onClose,
}: {
  attemptId: string;
  valuesHidden: boolean;
  onClose: () => void;
}) {
  const { t } = useTranslation();
  const labelId = useId();
  const { data: order, isLoading, error } = useOrder(attemptId, { poll: true });
  const vendorLabel = useDirectToolVendorLabel(order?.vendor || '');
  const title = order
    ? [
        order.action ? t(ORDER_ACTION_KEY[order.action]) : null,
        instrumentLabel(order.order?.instrument),
      ]
        .filter(Boolean)
        .join(' · ') || t('orders.detail.title')
    : t('orders.detail.title');

  return (
    <DetailOverlay
      labelId={labelId}
      onClose={onClose}
      header={
        <div className="flex flex-col gap-2 pr-8">
          <h2
            id={labelId}
            className="text-lg font-semibold leading-tight"
            style={{ color: 'var(--color-text-primary)' }}
          >
            {title}
          </h2>
          {order && (
            <div className="flex items-center gap-2 flex-wrap">
              <OrderStatusPill
                status={order.status}
                mode={order.mode}
                vendorLabel={vendorLabel}
                surface="detail"
              />
              <OrderModeBadge mode={order.mode} />
              <span
                className="text-[0.6875rem]"
                style={{ color: 'var(--color-text-tertiary)' }}
              >
                {order.vendor}
                {order.account_ref ? ` · ${maskAccountId(order.account_ref)}` : ''}
              </span>
            </div>
          )}
          {/* An instruction the broker is only holding is not an order until
              the user opens it there, so the way out sits with the verdict
              rather than among the fields further down. */}
          {order?.action_url && (
            <div>
              <OrderActionLink
                href={order.action_url}
                vendorLabel={vendorLabel}
                status={order.status}
              />
            </div>
          )}
        </div>
      }
    >
      {isLoading ? (
        <ListSkeleton rows={4} />
      ) : error && !isDefinitiveOrderError(error) ? (
        // A backend that faltered is not an order that does not exist, and the
        // poll is still asking, so say that rather than denying the row.
        <ListError>{t('orders.detail.loadFailed')}</ListError>
      ) : error || !order ? (
        <ListError>{t('orders.detail.notFound')}</ListError>
      ) : (
        <Body order={order} valuesHidden={valuesHidden} />
      )}
    </DetailOverlay>
  );
}
