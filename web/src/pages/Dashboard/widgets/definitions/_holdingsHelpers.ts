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

import { grouped2 } from '@/lib/format';
import { fixedUpTo } from '@/lib/bars/currencyDisplay';
import type { PortfolioRow } from '../../hooks/usePortfolioData';
import {
  formatPortfolioMoneyCode,
  normalizePortfolioCurrency,
  summarizePortfolioByCurrency,
  type CurrencyPortfolioSummary,
} from '../../utils/portfolioSummary';

export type PortfolioSummary = CurrencyPortfolioSummary;

/** Places an average cost prints up to. */
const COST_PLACES = 10;
const SUB_CENT = new Intl.NumberFormat('en-US', { useGrouping: false, maximumSignificantDigits: 10 });

/** `n` up to `places` decimals, or to its significant digits when under a
 *  cent, so no nonzero cost or price reads as 0.00. */
function upToOrSignificant(n: number, places: number): string {
  return n === 0 || Math.abs(n) >= 0.01 ? fixedUpTo(n, places, 'en-US') : SUB_CENT.format(n);
}

export function portfolioSummary(rows: PortfolioRow[]): PortfolioSummary[] {
  return summarizePortfolioByCurrency(rows);
}

/** Render the portfolio summary as the leading markdown line for snapshot
 *  exporters. Multi-currency portfolios get one NAV line per currency. */
export function formatPortfolioNavMarkdownLine(summaries: PortfolioSummary[], locale: string): string {
  return summaries
    .filter((summary) => summary.totalValue !== 0)
    .map((summary) => {
      if (summary.totalCost <= 0) {
        return `**NAV (${summary.currency})** ${formatPortfolioMoneyCode(summary.totalValue, summary.currency)}`;
      }
      const sign = summary.totalPl >= 0 ? '+' : '-';
      const excluded = summary.uncosted.length
        ? `, excluding ${summary.uncosted.map((h) => `${h.symbol} (${formatPortfolioMoneyCode(h.value, summary.currency)})`).join(', ')}: no cost in ${summary.currency}`
        : '';
      return `**NAV (${summary.currency})** ${formatPortfolioMoneyCode(summary.totalValue, summary.currency)} (cost ${formatPortfolioMoneyCode(summary.totalCost, summary.currency)}, P/L ${sign}${formatPortfolioMoneyCode(Math.abs(summary.totalPl), summary.currency)} / ${sign}${grouped2(Math.abs(summary.totalPlPct), locale)}%${excluded})`;
    })
    .join('\n');
}

/** A holding as its snapshot carries it: what is held, at what cost, and its
 *  unrealized P/L since purchase. */
export function portfolioRowData(row: PortfolioRow) {
  const hasQuote = row.quoteAvailable !== false;
  return {
    symbol: row.symbol,
    currency: normalizePortfolioCurrency(row.currency),
    shares: row.quantity ?? null,
    averageCost: row.average_cost ?? null,
    costCurrency: normalizePortfolioCurrency(row.costCurrency),
    price: hasQuote ? row.price : null,
    places: row.places,
    marketValue: hasQuote ? row.marketValue ?? null : null,
    unrealizedPlPercent: hasQuote ? row.unrealizedPlPercent ?? null : null,
  };
}

export type PortfolioRowData = ReturnType<typeof portfolioRowData>;

function costCell(row: PortfolioRowData): string {
  if (row.averageCost == null) return '';
  const cost = upToOrSignificant(row.averageCost, COST_PLACES);
  return row.costCurrency === row.currency ? cost : `${cost} ${row.costCurrency}`;
}

function plCell(pct: number | null): string {
  return pct == null ? '' : `${pct >= 0 ? '+' : ''}${pct.toFixed(2)}%`;
}

/** The holdings table for the agent. Its P/L column is since purchase, never
 *  the day's move, so it is not headed as a change. */
export function serializePortfolioRowsToMarkdown(rows: PortfolioRowData[]): string {
  if (!rows.length) return '_no holdings_';
  const lines = [
    '| symbol | currency | shares | avg cost | price | value | unrealized P/L% |',
    '| --- | --- | --- | --- | --- | --- | --- |',
  ];
  rows.forEach((row) => {
    const cells = [
      row.symbol,
      row.currency,
      row.shares != null ? String(row.shares) : '',
      costCell(row),
      row.price != null ? upToOrSignificant(row.price, row.places) : '',
      row.marketValue != null ? row.marketValue.toFixed(2) : '',
      plCell(row.unrealizedPlPercent),
    ];
    lines.push(`| ${cells.join(' | ')} |`);
  });
  return lines.join('\n');
}

/** One holding for a row attach, in the table's terms. */
export function serializePortfolioRowToMarkdown(holding: PortfolioRow): string {
  const row = portfolioRowData(holding);
  const lines = [`**${row.symbol}**`];
  if (row.shares != null) lines.push(`shares ${row.shares}`);
  if (row.averageCost != null) lines.push(`avg cost ${row.costCurrency} ${upToOrSignificant(row.averageCost, COST_PLACES)}`);
  if (row.price != null) lines.push(`price ${row.currency} ${upToOrSignificant(row.price, row.places)}`);
  if (row.marketValue != null) lines.push(`value ${row.currency} ${row.marketValue.toFixed(2)}`);
  if (row.unrealizedPlPercent != null) lines.push(`unrealized P/L ${plCell(row.unrealizedPlPercent)}`);
  return lines.join('\n');
}
