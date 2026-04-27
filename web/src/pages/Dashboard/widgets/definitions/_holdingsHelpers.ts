/**
 * Pure helpers shared by the holdings/portfolio widgets and their snapshot
 * exporters. Lives in its own module (separate from `_holdingsPrimitives.tsx`)
 * so the component file stays component-only — Vite's fast-refresh requires
 * a clean components-only boundary to do precise HMR updates.
 *
 * Single source of truth for NAV math: the visual NAV card AND the snapshot
 * exporter both call `portfolioSummary()`, so the agent can never see
 * different numbers than the user.
 */

import type { PortfolioRow } from '../../hooks/usePortfolioData';

export interface PortfolioSummary {
  totalValue: number;
  totalCost: number;
  totalPl: number;
  totalPlPct: number;
}

export function portfolioSummary(rows: PortfolioRow[]): PortfolioSummary {
  const totalValue = rows.reduce((s, r) => s + (r.marketValue || 0), 0);
  const totalCost = rows.reduce(
    (s, r) => s + (r.average_cost != null ? r.average_cost * (r.quantity || 0) : 0),
    0,
  );
  const totalPl = totalCost > 0 ? totalValue - totalCost : 0;
  const totalPlPct = totalCost > 0 ? ((totalValue - totalCost) / totalCost) * 100 : 0;
  return { totalValue, totalCost, totalPl, totalPlPct };
}

/** Render the portfolio summary as the leading markdown line for snapshot
 *  exporters. Returns an empty string when there's no cost basis (so the
 *  caller can drop the "$0 cost" non-information). */
export function formatPortfolioNavMarkdownLine(summary: PortfolioSummary): string {
  if (summary.totalCost <= 0) return '';
  const sign = summary.totalPl >= 0 ? '+' : '';
  return `**NAV** $${summary.totalValue.toFixed(2)} (cost $${summary.totalCost.toFixed(2)}, P/L ${sign}$${summary.totalPl.toFixed(2)} / ${sign}${summary.totalPlPct.toFixed(2)}%)`;
}
