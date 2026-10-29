import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useLocale } from '@/hooks/useLocale';
import { AnimatePresence } from '@/lib/framer';
import { SwapCell } from '@/components/ui/crossfade';
import { Briefcase } from 'lucide-react';
import { useDashboardContext } from '../framework/DashboardDataContext';
import { registerWidget } from '../framework/WidgetRegistry';
import { useWidgetContextExport } from '../framework/contextSnapshot';
import {
  serializeQuoteRowsToMarkdown,
  serializeQuoteRowToMarkdown,
  wrapWidgetContext,
} from '../framework/snapshotSerializers';
import { PortfolioConfigSchema } from '../framework/configSchemas';
import { RowAttachButton } from '../../components/RowAttachButton';
import type { WidgetRenderProps } from '../types';
import type { PortfolioRow } from '../../hooks/usePortfolioData';
import { WidgetCardMeta } from '../framework/widgetCardSlots';
import {
  HoldingsAddButton,
  HoldingsSkeleton,
  useHoldingsSkeletonRows,
  PortfolioNavSummary,
  PortfolioRowItem,
  PortfolioTable,
} from './_holdingsPrimitives';
import { formatPortfolioNavMarkdownLine, portfolioSummary } from './_holdingsHelpers';
import { normalizePortfolioCurrency } from '../../utils/portfolioSummary';

type PortfolioConfig = { valuesHidden?: boolean };

function rowToQuote(r: PortfolioRow) {
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

function PortfolioWidget({ instance, updateConfig }: WidgetRenderProps<PortfolioConfig>) {
  const { t } = useTranslation();
  const locale = useLocale();
  const { portfolio, portfolioHandlers, dashboard } = useDashboardContext();

  useWidgetContextExport(instance.id, {
    full: () => {
      const rows = portfolio.rows.map(rowToQuote);
      const summary = portfolioSummary(portfolio.rows);
      const navLine = formatPortfolioNavMarkdownLine(summary, locale);
      const lines: string[] = [];
      if (navLine) lines.push(navLine, '');
      lines.push(serializeQuoteRowsToMarkdown(rows));
      const text = wrapWidgetContext('portfolio.holdings', { count: rows.length }, lines.join('\n'));
      return {
        widget_type: 'portfolio.holdings',
        widget_id: instance.id,
        label: `${t('dashboard.widgets.portfolio.title')} · ${rows.length}`,
        description: rows.length ? `${rows.length} holding${rows.length === 1 ? '' : 's'}` : 'empty',
        captured_at: new Date().toISOString(),
        text,
        data: { rows, summary },
      };
    },
    rows: (rowId: string) => {
      const row = portfolio.rows.find((r) => (r.user_portfolio_id ?? r.symbol) === rowId);
      if (!row) return null;
      const quote = rowToQuote(row);
      const body = serializeQuoteRowToMarkdown(quote);
      const text = wrapWidgetContext('portfolio.holdings/row', { symbol: row.symbol }, body);
      return {
        widget_type: 'portfolio.holdings/row',
        widget_id: `${instance.id}/${rowId}`,
        label:
          row.symbol +
          (row.quoteAvailable !== false && row.unrealizedPlPercent != null
            ? ` · ${row.unrealizedPlPercent >= 0 ? '+' : ''}${row.unrealizedPlPercent.toFixed(2)}%`
            : ''),
        description: t('dashboard.widgets.portfolio.title'),
        captured_at: new Date().toISOString(),
        text,
        data: { row: quote },
      };
    },
  });
  const [valuesHidden, setValuesHidden] = useState(!!instance.config.valuesHidden);

  const toggleHidden = () => {
    const next = !valuesHidden;
    setValuesHidden(next);
    updateConfig({ valuesHidden: next });
  };

  const showSkeleton = portfolio.loading && portfolio.rows.length === 0;
  const skeletonRows = useHoldingsSkeletonRows('portfolio', portfolio.loaded ? portfolio.rows.length : null);
  const hasRealHoldings = portfolio.hasRealHoldings;

  return (
    <div className="widget-term dash-fill">
      {/* Held back while the rows load: a count of 0 that turns into the real
          one reads as a change, not a load. */}
      {!showSkeleton && <WidgetCardMeta>{portfolio.rows.length}</WidgetCardMeta>}
      <div className="dash-scroll">
        {/* The skeleton crossfades to the rows in place. Rows present when the
            list first renders just are; only one added later enters. */}
        <SwapCell as="div" className="items-start" swapKey={showSkeleton ? 'loading' : 'ready'}>
          <div>
            {hasRealHoldings && (
              <PortfolioNavSummary
                rows={portfolio.rows}
                valuesHidden={valuesHidden}
                onToggleHidden={toggleHidden}
              />
            )}
            <PortfolioTable>
              {showSkeleton ? (
                <HoldingsSkeleton count={skeletonRows} cols={4} />
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
            <HoldingsAddButton label={t('dashboard.widgets.portfolio.addTransaction')} onClick={portfolioHandlers.onAdd} />
          </div>
        </SwapCell>
      </div>
    </div>
  );
}

registerWidget<PortfolioConfig>({
  type: 'portfolio.holdings',
  titleKey: 'dashboard.widgets.portfolio.title',
  cardTitleKey: 'dashboard.widgets.portfolio.header',
  descriptionKey: 'dashboard.widgets.portfolio.description',
  category: 'personal',
  icon: Briefcase,
  component: PortfolioWidget,
  defaultConfig: { valuesHidden: false },
  configSchema: PortfolioConfigSchema,
  defaultSize: { w: 4, h: 18 },
  minSize: { w: 3, h: 10 },
  maxSize: { w: 12, h: 30 },
  fitToContent: true,
});

export default PortfolioWidget;
