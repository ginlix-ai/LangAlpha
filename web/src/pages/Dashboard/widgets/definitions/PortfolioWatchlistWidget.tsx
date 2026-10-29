import { useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useLocale } from '@/hooks/useLocale';
import { AnimatePresence } from '@/lib/framer';
import { SwapCell } from '@/components/ui/crossfade';
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
import { watchlistQuoteRow } from '../../hooks/useWatchlistData';
import { WidgetCardMeta } from '../framework/widgetCardSlots';
import {
  HoldingsAddButton,
  HoldingsSkeleton,
  useHoldingsSkeletonRows,
  PortfolioNavSummary,
  PortfolioRowItem,
  PortfolioTable,
  WatchlistRowItem,
  WatchlistTable,
} from './_holdingsPrimitives';
import { formatPortfolioNavMarkdownLine, portfolioSummary } from './_holdingsHelpers';
import { normalizePortfolioCurrency } from '../../utils/portfolioSummary';

type PWTabKey = 'watchlist' | 'portfolio';

type PortfolioWatchlistConfig = {
  defaultTab?: PWTabKey;
  valuesHidden?: boolean;
};

function portfolioRowToQuote(r: PortfolioRow) {
  const hasQuote = r.quoteAvailable !== false;
  return {
    symbol: r.symbol,
    price: hasQuote ? r.price : undefined,
    shares: r.quantity ?? undefined,
    marketValue: hasQuote ? r.marketValue ?? undefined : undefined,
    changePercent: hasQuote ? r.unrealizedPlPercent ?? undefined : undefined,
    currency: normalizePortfolioCurrency(r.currency),
  };
}

function PortfolioWatchlistWidget({
  instance,
  updateConfig,
}: WidgetRenderProps<PortfolioWatchlistConfig>) {
  const { t } = useTranslation();
  const locale = useLocale();
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
        const rows = watchlist.rows.map(watchlistQuoteRow);
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
      const navLine = formatPortfolioNavMarkdownLine(summary, locale);
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
        const quote = watchlistQuoteRow(row);
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
            (row.quoteAvailable !== false && row.changePercent !== undefined
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
          (row.quoteAvailable !== false && row.unrealizedPlPercent != null
            ? ` · ${row.unrealizedPlPercent >= 0 ? '+' : ''}${row.unrealizedPlPercent.toFixed(2)}%`
            : ''),
        description: t('dashboard.widgets.portfolioWatchlist.headerHoldings'),
        captured_at: new Date().toISOString(),
        text,
        data: { row: quote, tab: 'portfolio' },
      };
    },
  });

  // The tabs dissolve in one cell at the same scroll offset, so a tab opens at
  // its top rather than out of view below a short list. The list scrolls in
  // its own box, or, once the widget is past its height cap, in the card's.
  const contentRef = useRef<HTMLDivElement>(null);
  const switchTab = (tab: PWTabKey) => {
    if (tab !== activeTab) {
      contentRef.current?.scrollTo({ top: 0 });
      contentRef.current?.closest('.widget-card__body')?.scrollTo({ top: 0 });
    }
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
  const showSkeleton = activeTab === 'watchlist' ? showWatchlistSkeleton : showPortfolioSkeleton;
  const watchlistSkeletonRows = useHoldingsSkeletonRows('watchlist', watchlist.loaded ? watchlist.rows.length : null);
  const portfolioSkeletonRows = useHoldingsSkeletonRows('portfolio', portfolio.loaded ? portfolio.rows.length : null);
  const hasRealHoldings = portfolio.hasRealHoldings;

  const activeCount =
    activeTab === 'watchlist' ? watchlist.rows.length : portfolio.rows.length;

  const tabs: { key: PWTabKey; label: string }[] = [
    { key: 'watchlist', label: t('dashboard.widgets.portfolioWatchlist.headerWatchlist') },
    { key: 'portfolio', label: t('dashboard.widgets.portfolioWatchlist.headerHoldings') },
  ];

  return (
    <div className="widget-term dash-fill">
      {/* Held but unseen while the rows load: a count of 0 that turns into
          the real one reads as a change, not a load. */}
      {!showSkeleton && <WidgetCardMeta>{activeCount}</WidgetCardMeta>}
      <div className="dash-chips" role="group">
        {tabs.map((tab) => (
          <button
            key={tab.key}
            type="button"
            className="term-chip widget-drag-cancel"
            // Named outright: Terminal draws the chip's brackets as generated
            // content, which would otherwise be read as part of its name.
            aria-label={tab.label}
            aria-pressed={activeTab === tab.key}
            onClick={() => switchTab(tab.key)}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {/* The tabs dissolve in one grid cell, so a switch never passes through
          an empty widget. A tab the reader picks rises; the skeleton giving way
          to its rows crossfades, and rows present at first render never enter
          one by one. */}
      <div ref={contentRef} className="dash-scroll">
        <SwapCell
          as="div"
          className="items-start"
          riseKey={activeTab}
          swapKey={`${activeTab}:${showSkeleton ? 'loading' : 'ready'}`}
        >
          {activeTab === 'watchlist' ? (
            <div>
              <WatchlistTable>
                {showWatchlistSkeleton ? (
                  <HoldingsSkeleton count={watchlistSkeletonRows} />
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
              <HoldingsAddButton label={t('dashboard.widgets.portfolioWatchlist.addSymbol')} onClick={watchlistHandlers.onAdd} />
            </div>
          ) : (
            <div>
              {hasRealHoldings && (
                <PortfolioNavSummary
                  rows={portfolio.rows}
                  valuesHidden={valuesHidden}
                  onToggleHidden={toggleHidden}
                />
              )}
              <PortfolioTable>
                {showPortfolioSkeleton ? (
                  <HoldingsSkeleton count={portfolioSkeletonRows} cols={4} />
                ) : (
                  <AnimatePresence initial={false}>
                    {portfolio.rows.map((row) => (
                      <PortfolioRowItem
                        key={row.user_portfolio_id ?? row.symbol}
                        item={row}
                        marketStatus={dashboard.marketStatus}
                        valuesHidden={valuesHidden}
                        onEdit={portfolioHandlers.onEdit}
                        onDelete={portfolioHandlers.onDelete}
                        attach={
                          <RowAttachButton
                            instanceId={instance.id}
                            rowId={String(row.user_portfolio_id ?? row.symbol)}
                          />
                        }
                      />
                    ))}
                  </AnimatePresence>
                )}
              </PortfolioTable>
              <HoldingsAddButton label={t('dashboard.widgets.portfolioWatchlist.addTransaction')} onClick={portfolioHandlers.onAdd} />
            </div>
          )}
        </SwapCell>
      </div>
    </div>
  );
}

registerWidget<PortfolioWatchlistConfig>({
  type: 'personal.portfolioWatchlist',
  titleKey: 'dashboard.widgets.portfolioWatchlist.title',
  cardTitleKey: 'dashboard.widgets.portfolioWatchlist.cardTitle',
  descriptionKey: 'dashboard.widgets.portfolioWatchlist.description',
  category: 'personal',
  icon: Wallet,
  component: PortfolioWatchlistWidget,
  defaultConfig: { defaultTab: 'watchlist', valuesHidden: false },
  configSchema: PortfolioWatchlistConfigSchema,
  defaultSize: { w: 4, h: 18 },
  minSize: { w: 3, h: 10 },
  maxSize: { w: 12, h: 30 },
  fitToContent: true,
});

export default PortfolioWatchlistWidget;
