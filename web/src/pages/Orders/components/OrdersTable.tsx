import { useTranslation } from 'react-i18next';
import { instrumentLabel } from '@/components/orders/instrument';
import { OrderStatusPill } from '@/components/orders/OrderStatusPill';
import { OrderModeBadge } from '@/components/orders/OrderModeBadge';
import { useDirectToolVendorLabel } from '@/pages/ChatAgent/components/mcp/useDirectToolVendor';
import { maskAccountId } from '@/pages/ChatAgent/utils/directTools';
import type { OrderAttempt } from '@/pages/ChatAgent/utils/api';
import {
  formatOrderTime,
  orderLimitPrice,
  orderSize,
  orderStopPrice,
} from '../utils/format';
import { ORDER_ACTION_KEY, ORDER_SIDE_KEY, ORDER_TYPE_KEY } from '../utils/labels';

/**
 * The column plan, as shares of the table rather than of the content.
 *
 * The table lays out fixed, so an option symbol or a status written out in
 * words can no longer take width from the columns after it. That is what used
 * to push "Type and price" and "Status" off the right edge of the page and
 * leave the reader nothing to scroll with. Every share here is wide enough for
 * its own vocabulary at the narrowest desktop the page has to hold, and what
 * does not fit wraps instead of being cut.
 *
 * The broker's own order id is not among them: it is an opaque token as long
 * as a UUID, it tells two attempts apart for nobody scanning a list, and the
 * detail panel a row opens has it in full.
 */
const COLUMNS: ReadonlyArray<{ key: string; width: string }> = [
  { key: 'orders.column.created', width: '13.5%' },
  { key: 'orders.column.mode', width: '9%' },
  { key: 'orders.column.vendor', width: '10.5%' },
  { key: 'orders.column.action', width: '9.5%' },
  { key: 'orders.column.instrument', width: '12.5%' },
  { key: 'orders.column.side', width: '8%' },
  { key: 'orders.column.size', width: '8%' },
  { key: 'orders.column.price', width: '12%' },
  { key: 'orders.column.status', width: '17%' },
];

/** An empty cell reads as a dash rather than as a missing column. */
const NONE = '–';

function OrderRow({
  order,
  valuesHidden,
  onOpen,
}: {
  order: OrderAttempt;
  valuesHidden: boolean;
  onOpen: (attemptId: string) => void;
}) {
  const { t, i18n } = useTranslation();
  const locale = i18n.language;
  const summary = order.order;
  const vendorLabel = useDirectToolVendorLabel(order.vendor);
  const open = () => onOpen(order.attempt_id);
  const size = orderSize(summary, { hidden: valuesHidden, locale });
  const limit = orderLimitPrice(summary, { hidden: valuesHidden, locale });
  const stop = orderStopPrice(summary, { hidden: valuesHidden, locale });
  // The first line has room for one price, so it carries the limit and falls
  // back to the stop. A stop-limit's trigger then gets a line of its own: the
  // row is the only place most attempts are ever read.
  const price = limit || stop;
  const stopLine = limit && stop ? stop : '';
  // A term the ledger sends and this build has no word for still shows: the
  // raw value beats an empty cell on a row about somebody's money.
  const actionKey = order.action ? ORDER_ACTION_KEY[order.action] : undefined;
  // A cancel or a confirm names no instrument, side or size, so the id it acts
  // on is the only thing that tells one of them from another in the list.
  const targetRef = summary?.target_ref;
  const sideKey = summary?.side ? ORDER_SIDE_KEY[summary.side] : undefined;
  const typeKey = summary?.order_type ? ORDER_TYPE_KEY[summary.order_type] : undefined;
  const orderType = typeKey ? t(typeKey) : (summary?.order_type ?? '');

  return (
    <tr
      className="orders-row"
      tabIndex={0}
      data-testid={`order-row-${order.attempt_id}`}
      onClick={open}
      onKeyDown={(e) => {
        if (e.key === 'Enter' || e.key === ' ') {
          e.preventDefault();
          open();
        }
      }}
    >
      <td className="orders-cell orders-cell-time">
        {formatOrderTime(order.created_at) || NONE}
      </td>
      <td className="orders-cell orders-cell-mode">
        <OrderModeBadge mode={order.mode} />
      </td>
      <td className="orders-cell">
        <div className="flex flex-col">
          <span>{order.vendor}</span>
          {order.account_ref && (
            <span
              className="text-[0.6875rem]"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              {maskAccountId(order.account_ref)}
            </span>
          )}
        </div>
      </td>
      <td className="orders-cell">
        <div className="flex flex-col">
          <span>{actionKey ? t(actionKey) : order.action || NONE}</span>
          {targetRef && (
            <span className="orders-mono break-all">{targetRef}</span>
          )}
        </div>
      </td>
      <td className="orders-cell orders-cell-instrument">
        {instrumentLabel(summary?.instrument) || NONE}
      </td>
      <td className="orders-cell">
        {sideKey ? t(sideKey) : summary?.side || NONE}
      </td>
      <td className="orders-cell orders-num">{size || NONE}</td>
      <td className="orders-cell orders-num">
        <div className="flex flex-col items-end">
          <span>
            {orderType || price ? [orderType, price].filter(Boolean).join(' ') : NONE}
          </span>
          {stopLine && (
            <span
              className="text-[0.6875rem]"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              {t('orders.stopLine', { price: stopLine })}
            </span>
          )}
        </div>
      </td>
      <td className="orders-cell orders-cell-status">
        <OrderStatusPill
          status={order.status}
          mode={order.mode}
          vendorLabel={vendorLabel}
        />
      </td>
    </tr>
  );
}

/**
 * The ledger as a table. Every row is one attempt, and the row is the link:
 * the detail it opens is the only place the whole record is legible, and the
 * columns here are what tells two attempts apart at a glance.
 */
export function OrdersTable({
  orders,
  valuesHidden,
  onOpen,
}: {
  orders: OrderAttempt[];
  valuesHidden: boolean;
  onOpen: (attemptId: string) => void;
}) {
  const { t } = useTranslation();
  return (
    <div className="orders-table-scroll">
      <table className="orders-table">
        <colgroup>
          {COLUMNS.map((col) => (
            <col key={col.key} style={{ width: col.width }} />
          ))}
        </colgroup>
        <thead>
          <tr>
            {COLUMNS.map((col) => (
              <th key={col.key} className="orders-head">
                {t(col.key)}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {orders.map((order) => (
            <OrderRow
              key={order.attempt_id}
              order={order}
              valuesHidden={valuesHidden}
              onOpen={onOpen}
            />
          ))}
        </tbody>
      </table>
    </div>
  );
}
