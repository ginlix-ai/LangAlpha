import { AnimatePresence, motion } from 'framer-motion';
import { useTranslation } from 'react-i18next';
import { Eye } from 'lucide-react';
import { useDashboardContext } from '../framework/DashboardDataContext';
import { registerWidget } from '../framework/WidgetRegistry';
import { WatchlistConfigSchema } from '../framework/configSchemas';
import type { WidgetRenderProps } from '../types';
import {
  HoldingsAddButton,
  HoldingsSkeleton,
  WatchlistRowItem,
} from './_holdingsPrimitives';

type WatchlistConfig = Record<string, never>;

function WatchlistWidget(_props: WidgetRenderProps<WatchlistConfig>) {
  const { t } = useTranslation();
  const { watchlist, watchlistHandlers, dashboard } = useDashboardContext();
  const showSkeleton = watchlist.loading && watchlist.rows.length === 0;

  return (
    <div className="dashboard-glass-card p-5 flex flex-col h-full">
      <div
        className="flex items-baseline justify-between mb-3 pb-3 border-b"
        style={{ borderColor: 'var(--color-border-muted)' }}
      >
        <div className="flex items-baseline gap-2.5 min-w-0">
          <Eye
            className="h-3.5 w-3.5 flex-shrink-0 self-center"
            style={{ color: 'var(--color-text-tertiary)' }}
          />
          <span
            className="text-[10px] font-semibold uppercase tracking-[0.14em]"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            {t('dashboard.widgets.watchlist.header')}
          </span>
          <span
            className="title-font text-lg leading-none dashboard-mono"
            style={{ color: 'var(--color-text-primary)' }}
          >
            {watchlist.rows.length}
          </span>
        </div>
      </div>
      <div className="flex-1 min-h-0 overflow-y-auto pr-1">
        <AnimatePresence mode="wait">
          <motion.div
            key="watchlist"
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -10 }}
            className="flex flex-col gap-1"
          >
            {showSkeleton ? (
              <HoldingsSkeleton count={5} />
            ) : (
              watchlist.rows.map((row, i) => (
                <WatchlistRowItem
                  key={row.watchlist_item_id ?? row.symbol}
                  item={row}
                  index={i}
                  marketStatus={dashboard.marketStatus}
                  onDelete={watchlistHandlers.onDelete}
                />
              ))
            )}

            <HoldingsAddButton label={t('dashboard.widgets.watchlist.addSymbol')} onClick={watchlistHandlers.onAdd} />
          </motion.div>
        </AnimatePresence>
      </div>
    </div>
  );
}

registerWidget<WatchlistConfig>({
  type: 'watchlist.list',
  titleKey: 'dashboard.widgets.watchlist.title',
  descriptionKey: 'dashboard.widgets.watchlist.description',
  category: 'personal',
  icon: Eye,
  component: WatchlistWidget,
  defaultConfig: {},
  configSchema: WatchlistConfigSchema,
  defaultSize: { w: 4, h: 26 },
  minSize: { w: 3, h: 15 },
});

export default WatchlistWidget;
