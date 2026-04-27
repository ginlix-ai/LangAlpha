import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { AnimatePresence, motion } from 'framer-motion';
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
import {
  HoldingsAddButton,
  HoldingsSkeleton,
  PortfolioNavSummary,
  PortfolioRowItem,
} from './_holdingsPrimitives';
import { formatPortfolioNavMarkdownLine, portfolioSummary } from './_holdingsHelpers';

type PortfolioConfig = { valuesHidden?: boolean };

function rowToQuote(r: PortfolioRow) {
  return {
    symbol: r.symbol,
    price: r.price,
    shares: r.quantity ?? undefined,
    marketValue: r.marketValue,
    changePercent: r.unrealizedPlPercent ?? undefined,
  };
}

function PortfolioWidget({ instance, updateConfig }: WidgetRenderProps<PortfolioConfig>) {
  const { t } = useTranslation();
  const { portfolio, portfolioHandlers, dashboard } = useDashboardContext();

  useWidgetContextExport(instance.id, {
    full: () => {
      const rows = portfolio.rows.map(rowToQuote);
      const summary = portfolioSummary(portfolio.rows);
      const navLine = formatPortfolioNavMarkdownLine(summary);
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
          (row.unrealizedPlPercent != null
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
  const hasRealHoldings = portfolio.hasRealHoldings;

  return (
    <div className="dashboard-glass-card p-5 flex flex-col h-full">
      <div
        className="flex items-baseline justify-between mb-3 pb-3 border-b"
        style={{ borderColor: 'var(--color-border-muted)' }}
      >
        <div className="flex items-baseline gap-2.5 min-w-0">
          <Briefcase
            className="h-3.5 w-3.5 flex-shrink-0 self-center"
            style={{ color: 'var(--color-text-tertiary)' }}
          />
          <span
            className="text-[10px] font-semibold uppercase tracking-[0.14em]"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            {t('dashboard.widgets.portfolio.header')}
          </span>
          <span
            className="title-font text-lg leading-none dashboard-mono"
            style={{ color: 'var(--color-text-primary)' }}
          >
            {portfolio.rows.length}
          </span>
        </div>
      </div>
      <div className="flex-1 min-h-0 overflow-y-auto pr-1">
        <AnimatePresence mode="wait">
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

            {showSkeleton ? (
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

            <HoldingsAddButton label={t('dashboard.widgets.portfolio.addTransaction')} onClick={portfolioHandlers.onAdd} />
          </motion.div>
        </AnimatePresence>
      </div>
    </div>
  );
}

registerWidget<PortfolioConfig>({
  type: 'portfolio.holdings',
  titleKey: 'dashboard.widgets.portfolio.title',
  descriptionKey: 'dashboard.widgets.portfolio.description',
  category: 'personal',
  icon: Briefcase,
  component: PortfolioWidget,
  defaultConfig: { valuesHidden: false },
  configSchema: PortfolioConfigSchema,
  defaultSize: { w: 4, h: 26 },
  minSize: { w: 3, h: 15 },
});

export default PortfolioWidget;
