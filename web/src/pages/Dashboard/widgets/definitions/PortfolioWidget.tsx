import { useState } from 'react';
import { AnimatePresence, motion } from 'framer-motion';
import { Briefcase } from 'lucide-react';
import { useDashboardContext } from '../framework/DashboardDataContext';
import { registerWidget } from '../framework/WidgetRegistry';
import type { WidgetRenderProps } from '../types';
import {
  HoldingsAddButton,
  HoldingsSkeleton,
  PortfolioNavSummary,
  PortfolioRowItem,
} from './_holdingsPrimitives';

type PortfolioConfig = { valuesHidden?: boolean };

function PortfolioWidget({ instance, updateConfig }: WidgetRenderProps<PortfolioConfig>) {
  const { portfolio, portfolioHandlers, dashboard } = useDashboardContext();
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
            Holdings
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

            <HoldingsAddButton label="Add Transaction" onClick={portfolioHandlers.onAdd} />
          </motion.div>
        </AnimatePresence>
      </div>
    </div>
  );
}

registerWidget<PortfolioConfig>({
  type: 'portfolio.holdings',
  title: 'Portfolio',
  description: 'Your holdings with market value and unrealized P/L.',
  category: 'personal',
  icon: Briefcase,
  component: PortfolioWidget,
  defaultConfig: { valuesHidden: false },
  defaultSize: { w: 4, h: 26 },
  minSize: { w: 3, h: 15 },
});

export default PortfolioWidget;
