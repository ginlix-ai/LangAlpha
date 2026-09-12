import { useCallback, useMemo, useRef } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { AnimatePresence } from 'framer-motion';
import { Blocks, Eye, EyeOff } from 'lucide-react';
import {
  HeaderButton,
  ListEmpty,
  ListError,
  ListSkeleton,
} from '@/components/mcp/McpPrimitives';
import { useValuesHidden } from '@/components/orders/valuesHidden';
import { EmptyState } from '@/pages/Plugins/components/EmptyState';
import { useScrollMemory } from '@/lib/scrollMemory';
import { useHasConnectedBrokerage } from '@/hooks/useConnectedBrokerage';
import { useOrders } from '@/hooks/useOrders';
import {
  isOrderStatusGroup,
  type OrderAssetClass,
  type OrderFilters as Filters,
  type OrderStatusGroup,
} from '@/pages/ChatAgent/utils/api';
import type { OrderMode } from '@/types/orders';
import { OrderDetail } from './components/OrderDetail';
import { OrderFilters } from './components/OrderFilters';
import { OrdersTable } from './components/OrdersTable';
import { parseOrderDetail, withOrderDetail } from './utils/detailParam';
import './Orders.css';

/** An unknown status in the URL is no filter, not a request for nothing. */
function statusGroupParam(raw: string | null): OrderStatusGroup | null {
  return isOrderStatusGroup(raw) ? raw : null;
}

/**
 * /orders: every order the agent proposed at any broker, and what happened to
 * it. One row per attempt in the ledger, newest first.
 *
 * The filters live in the URL rather than in state, so a link to "moomoo, live,
 * failed" is a link somebody can send, and the `?detail=order:<id>` overlay
 * opens on top of whatever list the address bar already describes.
 *
 * Reachable by URL whether or not a broker is connected -- the sidebar drops
 * the entry, this page answers the address. Someone who has connected nothing
 * is told what to connect rather than that they have no orders, which is true
 * of them but not the thing they can act on.
 */

function Orders() {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
  const pageRef = useRef<HTMLDivElement>(null);
  useScrollMemory(pageRef, 'page:orders');

  const [valuesHidden, toggleValuesHidden] = useValuesHidden();

  // Derived from the URL rather than mirrored into state, so back and forward
  // move the filters and cannot disagree with the address bar.
  const filters: Filters = useMemo(
    () => ({
      vendor: searchParams.get('vendor'),
      mode: searchParams.get('mode') as OrderMode | null,
      status: statusGroupParam(searchParams.get('status')),
      asset_class: searchParams.get('asset_class') as OrderAssetClass | null,
    }),
    [searchParams],
  );

  const setFilters = useCallback(
    (next: Filters) => {
      const params = new URLSearchParams(searchParams);
      for (const key of ['vendor', 'mode', 'status', 'asset_class'] as const) {
        const value = next[key];
        if (value) params.set(key, value);
        else params.delete(key);
      }
      // A filter change is a different list, so the open row may not be in it.
      params.delete('detail');
      setSearchParams(params, { replace: true });
    },
    [searchParams, setSearchParams],
  );

  const detailId = parseOrderDetail(searchParams);
  const openDetail = (attemptId: string) =>
    setSearchParams(withOrderDetail(searchParams, attemptId));
  const closeDetail = () =>
    setSearchParams(withOrderDetail(searchParams, null), { replace: true });

  const {
    data,
    error,
    isLoading,
    hasNextPage,
    isFetchingNextPage,
    fetchNextPage,
  } = useOrders(filters);
  const orders = useMemo(
    () => (data?.pages ?? []).flatMap((page) => page.items),
    [data],
  );
  // `undefined` while the answer is in flight, and it is not `false`: the
  // "connect a broker" invitation is wrong for someone who has one, so an
  // unsettled read gets the ordinary empty line instead.
  const hasBrokerage = useHasConnectedBrokerage();
  const filtered = !!(
    filters.vendor ||
    filters.mode ||
    filters.status ||
    filters.asset_class
  );

  return (
    <div className="orders-page">
      {/* Doubles as the window titlebar in the desktop shell; inert elsewhere. */}
      <div className="chrome-drag-strip" aria-hidden="true" />
      <div ref={pageRef} className="orders-scroll">
        <div className="orders-container">
          <div className="flex items-start justify-between gap-3 mb-6">
            <div className="min-w-0">
              <h2
                className="text-xl font-semibold mb-1"
                style={{ color: 'var(--color-text-primary)' }}
              >
                {t('orders.title')}
              </h2>
              <p className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
                {t('orders.description')}
              </p>
            </div>
            <HeaderButton
              variant="secondary"
              icon={valuesHidden ? EyeOff : Eye}
              onClick={toggleValuesHidden}
              aria-pressed={valuesHidden}
            >
              {t(valuesHidden ? 'orders.showValues' : 'orders.hideValues')}
            </HeaderButton>
          </div>

          <div className="flex flex-col gap-4">
            <OrderFilters filters={filters} onChange={setFilters} />

            {error ? (
              <ListError>{t('orders.loadFailed')}</ListError>
            ) : isLoading ? (
              <ListSkeleton rows={5} />
            ) : orders.length === 0 ? (
              filtered ? (
                <ListEmpty>{t('orders.noMatches')}</ListEmpty>
              ) : hasBrokerage === false ? (
                <EmptyState
                  message={t('orders.empty.noBrokerage')}
                  action={
                    <HeaderButton
                      variant="primary"
                      icon={Blocks}
                      onClick={() => navigate('/plugins?tab=brokerages')}
                    >
                      {t('orders.empty.noBrokerageAction')}
                    </HeaderButton>
                  }
                />
              ) : (
                <EmptyState message={t('orders.empty.noOrders')} />
              )
            ) : (
              <>
                <OrdersTable
                  orders={orders}
                  valuesHidden={valuesHidden}
                  onOpen={openDetail}
                />
                {hasNextPage && (
                  <div className="flex justify-center">
                    <HeaderButton
                      variant="secondary"
                      onClick={() => void fetchNextPage()}
                      disabled={isFetchingNextPage}
                    >
                      {t(isFetchingNextPage ? 'orders.loading' : 'orders.loadMore')}
                    </HeaderButton>
                  </div>
                )}
              </>
            )}
          </div>
        </div>
      </div>

      <AnimatePresence>
        {detailId && (
          <OrderDetail
            key={detailId}
            attemptId={detailId}
            valuesHidden={valuesHidden}
            onClose={closeDetail}
          />
        )}
      </AnimatePresence>
    </div>
  );
}

export default Orders;
