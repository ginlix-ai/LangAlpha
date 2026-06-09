/**
 * Pure helpers shared by the holdings/portfolio widgets and their snapshot
 * exporters. Lives in its own module (separate from `_holdingsPrimitives.tsx`)
 * so the component file stays component-only; Vite's fast-refresh requires
 * a clean components-only boundary to do precise HMR updates.
 *
 * Single source of truth for NAV math: the visual NAV card AND the snapshot
 * exporter both call `portfolioSummary()`, so the agent can never see
 * different numbers than the user.
 */

import { createFormatter } from '@/lib/format';
import type { PortfolioRow } from '../../hooks/usePortfolioData';
import {
  formatPortfolioMoneyCode,
  summarizePortfolioByCurrency,
  type CurrencyPortfolioSummary,
} from '../../utils/portfolioSummary';

export type PortfolioSummary = CurrencyPortfolioSummary;

const fmtPct = createFormatter({ minimumFractionDigits: 2, maximumFractionDigits: 2 });

export function portfolioSummary(rows: PortfolioRow[]): PortfolioSummary[] {
  return summarizePortfolioByCurrency(rows);
}

/** Render the portfolio summary as the leading markdown line for snapshot
 *  exporters. Multi-currency portfolios get one NAV line per currency. */
export function formatPortfolioNavMarkdownLine(summaries: PortfolioSummary[]): string {
  return summaries
    .filter((summary) => summary.totalValue !== 0)
    .map((summary) => {
      if (summary.totalCost <= 0) {
        return `**NAV (${summary.currency})** ${formatPortfolioMoneyCode(summary.totalValue, summary.currency)}`;
      }
      const sign = summary.totalPl >= 0 ? '+' : '-';
      return `**NAV (${summary.currency})** ${formatPortfolioMoneyCode(summary.totalValue, summary.currency)} (cost ${formatPortfolioMoneyCode(summary.totalCost, summary.currency)}, P/L ${sign}${formatPortfolioMoneyCode(Math.abs(summary.totalPl), summary.currency)} / ${sign}${fmtPct(Math.abs(summary.totalPlPct))}%)`;
    })
    .join('\n');
}
