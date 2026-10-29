import { AnimatePresence } from '@/lib/framer';
import { SwapCell } from '@/components/ui/crossfade';
import { useTranslation } from 'react-i18next';
import { Eye } from 'lucide-react';
import { useDashboardContext } from '../framework/DashboardDataContext';
import { registerWidget } from '../framework/WidgetRegistry';
import { WatchlistConfigSchema } from '../framework/configSchemas';
import { useWidgetContextExport } from '../framework/contextSnapshot';
import {
  serializeQuoteRowsToMarkdown,
  serializeQuoteRowToMarkdown,
  wrapWidgetContext,
} from '../framework/snapshotSerializers';
import { RowAttachButton } from '../../components/RowAttachButton';
import type { WidgetRenderProps } from '../types';
import { watchlistQuoteRow } from '../../hooks/useWatchlistData';
import { WidgetCardMeta } from '../framework/widgetCardSlots';
import {
  HoldingsAddButton,
  HoldingsSkeleton,
  useHoldingsSkeletonRows,
  WatchlistRowItem,
  WatchlistTable,
} from './_holdingsPrimitives';

type WatchlistConfig = Record<string, never>;

function WatchlistWidget({ instance }: WidgetRenderProps<WatchlistConfig>) {
  const { t } = useTranslation();
  const { watchlist, watchlistHandlers, dashboard } = useDashboardContext();
  const showSkeleton = watchlist.loading && watchlist.rows.length === 0;
  const skeletonRows = useHoldingsSkeletonRows('watchlist', watchlist.loaded ? watchlist.rows.length : null);

  // Register snapshot exporters: full table + per-row.
  useWidgetContextExport(instance.id, {
    full: () => {
      const rows = watchlist.rows.map(watchlistQuoteRow);
      const body = serializeQuoteRowsToMarkdown(rows);
      const text = wrapWidgetContext('watchlist.list', { count: rows.length }, body);
      return {
        widget_type: 'watchlist.list',
        widget_id: instance.id,
        label: t('dashboard.widgets.watchlist.title') + ' · ' + rows.length,
        description: rows.length ? `${rows.length} symbol${rows.length === 1 ? '' : 's'}` : 'empty',
        captured_at: new Date().toISOString(),
        text,
        data: { rows },
      };
    },
    rows: (rowId: string) => {
      const row = watchlist.rows.find((r) => (r.watchlist_item_id ?? r.symbol) === rowId);
      if (!row) return null;
      // Note: pre/post-market price is intentionally omitted. The QuoteRow
      // contract treats preMarket/postMarket as *prices*, but the row data
      // here only carries previousClose + a change-percent — passing
      // previousClose under the preMarket key would label yesterday's close
      // as the pre-market price in the agent's view. Adding correct ext-
      // hours info would need marketStatus + getExtendedHoursInfo here.
      const cleaned = watchlistQuoteRow(row);
      const body = serializeQuoteRowToMarkdown(cleaned);
      const text = wrapWidgetContext('watchlist.list/row', { symbol: row.symbol }, body);
      return {
        widget_type: 'watchlist.list/row',
        widget_id: `${instance.id}/${rowId}`,
        label:
          row.symbol +
          (row.quoteAvailable !== false && row.changePercent !== undefined
            ? ` · ${row.changePercent >= 0 ? '+' : ''}${row.changePercent.toFixed(2)}%`
            : ''),
        description: t('dashboard.widgets.watchlist.title'),
        captured_at: new Date().toISOString(),
        text,
        data: { row: cleaned },
      };
    },
  });

  return (
    <div className="widget-term dash-fill">
      {/* Held back while the rows load: a count of 0 that turns into the real
          one reads as a change, not a load. */}
      {!showSkeleton && <WidgetCardMeta>{watchlist.rows.length}</WidgetCardMeta>}
      <div className="dash-scroll">
        {/* The skeleton crossfades to the rows in place. Rows present when the
            list first renders just are; only one added later enters. */}
        <SwapCell as="div" className="items-start" swapKey={showSkeleton ? 'loading' : 'ready'}>
          <div>
            <WatchlistTable>
              {showSkeleton ? (
                <HoldingsSkeleton count={skeletonRows} />
              ) : (
                <AnimatePresence initial={false}>
                  {watchlist.rows.map((row) => (
                    <WatchlistRowItem
                      key={row.watchlist_item_id ?? row.symbol}
                      item={row}
                      marketStatus={dashboard.marketStatus}
                      onDelete={watchlistHandlers.onDelete}
                      attach={
                        <RowAttachButton
                          instanceId={instance.id}
                          rowId={String(row.watchlist_item_id ?? row.symbol)}
                        />
                      }
                    />
                  ))}
                </AnimatePresence>
              )}
            </WatchlistTable>
            <HoldingsAddButton label={t('dashboard.widgets.watchlist.addSymbol')} onClick={watchlistHandlers.onAdd} />
          </div>
        </SwapCell>
      </div>
    </div>
  );
}

registerWidget<WatchlistConfig>({
  type: 'watchlist.list',
  titleKey: 'dashboard.widgets.watchlist.title',
  cardTitleKey: 'dashboard.widgets.watchlist.header',
  descriptionKey: 'dashboard.widgets.watchlist.description',
  category: 'personal',
  icon: Eye,
  component: WatchlistWidget,
  defaultConfig: {},
  configSchema: WatchlistConfigSchema,
  defaultSize: { w: 4, h: 18 },
  minSize: { w: 3, h: 10 },
  maxSize: { w: 12, h: 30 },
  fitToContent: true,
});

export default WatchlistWidget;
