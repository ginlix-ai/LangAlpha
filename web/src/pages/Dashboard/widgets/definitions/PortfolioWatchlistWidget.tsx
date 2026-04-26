import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { AnimatePresence, motion } from 'framer-motion';
import { Wallet } from 'lucide-react';
import { useDashboardContext } from '../framework/DashboardDataContext';
import { registerWidget } from '../framework/WidgetRegistry';
import { PortfolioWatchlistConfigSchema } from '../framework/configSchemas';
import type { WidgetRenderProps } from '../types';
import {
  HoldingsAddButton,
  HoldingsSkeleton,
  PortfolioNavSummary,
  PortfolioRowItem,
  WatchlistRowItem,
} from './_holdingsPrimitives';

type PWTabKey = 'watchlist' | 'portfolio';

type PortfolioWatchlistConfig = {
  defaultTab?: PWTabKey;
  valuesHidden?: boolean;
};

function PortfolioWatchlistWidget({
  instance,
  updateConfig,
}: WidgetRenderProps<PortfolioWatchlistConfig>) {
  const { t } = useTranslation();
  const {
    portfolio,
    portfolioHandlers,
    watchlist,
    watchlistHandlers,
    dashboard,
  } = useDashboardContext();

  const [activeTab, setActiveTab] = useState<PWTabKey>(instance.config.defaultTab ?? 'watchlist');
  const [valuesHidden, setValuesHidden] = useState(!!instance.config.valuesHidden);

  const switchTab = (tab: PWTabKey) => {
    setActiveTab(tab);
    updateConfig({ defaultTab: tab });
  };

  const toggleHidden = () => {
    const next = !valuesHidden;
    setValuesHidden(next);
    updateConfig({ valuesHidden: next });
  };

  const showWatchlistSkeleton = watchlist.loading && watchlist.rows.length === 0;
  const showPortfolioSkeleton = portfolio.loading && portfolio.rows.length === 0;
  const hasRealHoldings = portfolio.hasRealHoldings;

  const activeCount =
    activeTab === 'watchlist' ? watchlist.rows.length : portfolio.rows.length;

  return (
    <div className="dashboard-glass-card p-5 flex flex-col h-full">
      <div
        className="flex items-baseline justify-between mb-3 pb-3 border-b"
        style={{ borderColor: 'var(--color-border-muted)' }}
      >
        <div className="flex items-baseline gap-2.5 min-w-0">
          <Wallet
            className="h-3.5 w-3.5 flex-shrink-0 self-center"
            style={{ color: 'var(--color-text-tertiary)' }}
          />
          <span
            className="text-[10px] font-semibold uppercase tracking-[0.14em]"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            {activeTab === 'watchlist'
              ? t('dashboard.widgets.portfolioWatchlist.headerWatchlist')
              : t('dashboard.widgets.portfolioWatchlist.headerHoldings')}
          </span>
          <span
            className="title-font text-lg leading-none dashboard-mono"
            style={{ color: 'var(--color-text-primary)' }}
          >
            {activeCount}
          </span>
        </div>
        <div
          className="flex rounded-full p-[2px]"
          style={{ backgroundColor: 'var(--color-bg-subtle)' }}
        >
          <button
            type="button"
            onClick={() => switchTab('watchlist')}
            className="px-2.5 py-[3px] text-[10.5px] uppercase tracking-wider rounded-full transition-colors"
            style={{
              backgroundColor:
                activeTab === 'watchlist' ? 'var(--color-bg-card)' : 'transparent',
              color:
                activeTab === 'watchlist'
                  ? 'var(--color-text-primary)'
                  : 'var(--color-text-tertiary)',
              boxShadow:
                activeTab === 'watchlist'
                  ? '0 1px 2px rgba(0,0,0,0.04)'
                  : 'none',
            }}
          >
            {t('dashboard.widgets.portfolioWatchlist.tabWatch')}
          </button>
          <button
            type="button"
            onClick={() => switchTab('portfolio')}
            className="px-2.5 py-[3px] text-[10.5px] uppercase tracking-wider rounded-full transition-colors"
            style={{
              backgroundColor:
                activeTab === 'portfolio' ? 'var(--color-bg-card)' : 'transparent',
              color:
                activeTab === 'portfolio'
                  ? 'var(--color-text-primary)'
                  : 'var(--color-text-tertiary)',
              boxShadow:
                activeTab === 'portfolio'
                  ? '0 1px 2px rgba(0,0,0,0.04)'
                  : 'none',
            }}
          >
            {t('dashboard.widgets.portfolioWatchlist.tabHoldings')}
          </button>
        </div>
      </div>

      <div className="flex-1 min-h-0 overflow-y-auto pr-1">
        <AnimatePresence mode="wait">
          {activeTab === 'watchlist' ? (
            <motion.div
              key="watchlist"
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -10 }}
              className="flex flex-col gap-1"
            >
              {showWatchlistSkeleton ? (
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
              <HoldingsAddButton label={t('dashboard.widgets.portfolioWatchlist.addSymbol')} onClick={watchlistHandlers.onAdd} />
            </motion.div>
          ) : (
            <motion.div
              key="portfolio"
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -10 }}
              className="flex flex-col gap-1"
            >
              {hasRealHoldings && (
                <PortfolioNavSummary
                  rows={portfolio.rows}
                  valuesHidden={valuesHidden}
                  onToggleHidden={toggleHidden}
                />
              )}

              {showPortfolioSkeleton ? (
                <HoldingsSkeleton count={3} />
              ) : (
                portfolio.rows.map((row, i) => (
                  <PortfolioRowItem
                    key={row.user_portfolio_id ?? row.symbol}
                    item={row}
                    index={i}
                    marketStatus={dashboard.marketStatus}
                    valuesHidden={valuesHidden}
                    onEdit={portfolioHandlers.onEdit}
                    onDelete={portfolioHandlers.onDelete}
                  />
                ))
              )}

              <HoldingsAddButton label={t('dashboard.widgets.portfolioWatchlist.addTransaction')} onClick={portfolioHandlers.onAdd} />
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    </div>
  );
}

registerWidget<PortfolioWatchlistConfig>({
  type: 'personal.portfolioWatchlist',
  titleKey: 'dashboard.widgets.portfolioWatchlist.title',
  descriptionKey: 'dashboard.widgets.portfolioWatchlist.description',
  category: 'personal',
  icon: Wallet,
  component: PortfolioWatchlistWidget,
  defaultConfig: { defaultTab: 'watchlist', valuesHidden: false },
  configSchema: PortfolioWatchlistConfigSchema,
  defaultSize: { w: 4, h: 30 },
  minSize: { w: 3, h: 18 },
});

export default PortfolioWatchlistWidget;
