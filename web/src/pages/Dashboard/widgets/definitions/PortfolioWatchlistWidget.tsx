import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { AnimatePresence, motion } from 'framer-motion';
import { Wallet } from 'lucide-react';
import { useDashboardContext } from '../framework/DashboardDataContext';
import { registerWidget } from '../framework/WidgetRegistry';
import { useWidgetContextExport } from '../framework/contextSnapshot';
import {
  serializeQuoteRowsToMarkdown,
  serializeQuoteRowToMarkdown,
  wrapWidgetContext,
} from '../framework/snapshotSerializers';
import { PortfolioWatchlistConfigSchema } from '../framework/configSchemas';
import { RowAttachButton } from '../../components/RowAttachButton';
import type { WidgetRenderProps } from '../types';
import type { PortfolioRow } from '../../hooks/usePortfolioData';
import type { WatchlistRow } from '../../hooks/useWatchlistData';
import {
  HoldingsAddButton,
  HoldingsSkeleton,
  PortfolioNavSummary,
  PortfolioRowItem,
  WatchlistRowItem,
} from './_holdingsPrimitives';
import { formatPortfolioNavMarkdownLine, portfolioSummary } from './_holdingsHelpers';

type PWTabKey = 'watchlist' | 'portfolio';

type PortfolioWatchlistConfig = {
  defaultTab?: PWTabKey;
  valuesHidden?: boolean;
};

function watchlistRowToQuote(r: WatchlistRow) {
  return {
    symbol: r.symbol,
    price: r.price,
    change: r.change,
    changePercent: r.changePercent,
  };
}

function portfolioRowToQuote(r: PortfolioRow) {
  return {
    symbol: r.symbol,
    price: r.price,
    shares: r.quantity ?? undefined,
    marketValue: r.marketValue,
    changePercent: r.unrealizedPlPercent ?? undefined,
  };
}

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

  // Snapshot exporter — full payload reflects whichever tab the user is viewing
  // (the agent should see what the user sees). Per-row attach delegates to the
  // active tab's row collection.
  useWidgetContextExport(instance.id, {
    full: () => {
      if (activeTab === 'watchlist') {
        const rows = watchlist.rows.map(watchlistRowToQuote);
        const body = serializeQuoteRowsToMarkdown(rows);
        const text = wrapWidgetContext(
          'personal.portfolioWatchlist',
          { tab: 'watchlist', count: rows.length },
          body,
        );
        return {
          widget_type: 'personal.portfolioWatchlist',
          widget_id: instance.id,
          label: `${t('dashboard.widgets.portfolioWatchlist.headerWatchlist')} · ${rows.length}`,
          description: rows.length ? `${rows.length} symbol${rows.length === 1 ? '' : 's'}` : 'empty',
          captured_at: new Date().toISOString(),
          text,
          data: { tab: 'watchlist', rows },
        };
      }
      const rows = portfolio.rows.map(portfolioRowToQuote);
      const summary = portfolioSummary(portfolio.rows);
      const navLine = formatPortfolioNavMarkdownLine(summary);
      const lines: string[] = [];
      if (navLine) lines.push(navLine, '');
      lines.push(serializeQuoteRowsToMarkdown(rows));
      const text = wrapWidgetContext(
        'personal.portfolioWatchlist',
        { tab: 'portfolio', count: rows.length },
        lines.join('\n'),
      );
      return {
        widget_type: 'personal.portfolioWatchlist',
        widget_id: instance.id,
        label: `${t('dashboard.widgets.portfolioWatchlist.headerHoldings')} · ${rows.length}`,
        description: rows.length ? `${rows.length} holding${rows.length === 1 ? '' : 's'}` : 'empty',
        captured_at: new Date().toISOString(),
        text,
        data: { tab: 'portfolio', rows, summary },
      };
    },
    rows: (rowId: string) => {
      if (activeTab === 'watchlist') {
        const row = watchlist.rows.find((r) => (r.watchlist_item_id ?? r.symbol) === rowId);
        if (!row) return null;
        const quote = watchlistRowToQuote(row);
        const body = serializeQuoteRowToMarkdown(quote);
        const text = wrapWidgetContext(
          'personal.portfolioWatchlist/row',
          { tab: 'watchlist', symbol: row.symbol },
          body,
        );
        return {
          widget_type: 'personal.portfolioWatchlist/row',
          widget_id: `${instance.id}/${rowId}`,
          label:
            row.symbol +
            (row.changePercent !== undefined
              ? ` · ${row.changePercent >= 0 ? '+' : ''}${row.changePercent.toFixed(2)}%`
              : ''),
          description: t('dashboard.widgets.portfolioWatchlist.headerWatchlist'),
          captured_at: new Date().toISOString(),
          text,
          data: { row: quote, tab: 'watchlist' },
        };
      }
      const row = portfolio.rows.find((r) => (r.user_portfolio_id ?? r.symbol) === rowId);
      if (!row) return null;
      const quote = portfolioRowToQuote(row);
      const body = serializeQuoteRowToMarkdown(quote);
      const text = wrapWidgetContext(
        'personal.portfolioWatchlist/row',
        { tab: 'portfolio', symbol: row.symbol },
        body,
      );
      return {
        widget_type: 'personal.portfolioWatchlist/row',
        widget_id: `${instance.id}/${rowId}`,
        label:
          row.symbol +
          (row.unrealizedPlPercent != null
            ? ` · ${row.unrealizedPlPercent >= 0 ? '+' : ''}${row.unrealizedPlPercent.toFixed(2)}%`
            : ''),
        description: t('dashboard.widgets.portfolioWatchlist.headerHoldings'),
        captured_at: new Date().toISOString(),
        text,
        data: { row: quote, tab: 'portfolio' },
      };
    },
  });

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
                  <div
                    key={row.watchlist_item_id ?? row.symbol}
                    className="row-attach-host relative"
                  >
                    <WatchlistRowItem
                      item={row}
                      index={i}
                      marketStatus={dashboard.marketStatus}
                      onDelete={watchlistHandlers.onDelete}
                    />
                    <span className="absolute right-1 top-1/2 -translate-y-1/2">
                      <RowAttachButton
                        instanceId={instance.id}
                        rowId={String(row.watchlist_item_id ?? row.symbol)}
                      />
                    </span>
                  </div>
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
                  <div
                    key={row.user_portfolio_id ?? row.symbol}
                    className="row-attach-host relative"
                  >
                    <PortfolioRowItem
                      item={row}
                      index={i}
                      marketStatus={dashboard.marketStatus}
                      valuesHidden={valuesHidden}
                      onEdit={portfolioHandlers.onEdit}
                      onDelete={portfolioHandlers.onDelete}
                    />
                    <span className="absolute right-1 top-1/2 -translate-y-1/2">
                      <RowAttachButton
                        instanceId={instance.id}
                        rowId={String(row.user_portfolio_id ?? row.symbol)}
                      />
                    </span>
                  </div>
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
