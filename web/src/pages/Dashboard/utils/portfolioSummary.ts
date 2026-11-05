import { formatMoney } from '@/lib/bars/currencyDisplay';
import { currencyForSymbol, minorUnitPlacesForSymbol } from '@/lib/bars/exchanges';

const DEFAULT_PORTFOLIO_CURRENCY = 'USD';

export interface PortfolioSummaryRow {
  symbol: string;
  marketValue?: number | null;
  average_cost?: number | null;
  quantity?: number | null;
  /** The currency the holding's price and value are in. */
  currency?: string | null | undefined;
  /** The currency its average cost was entered in, when that can differ from `currency`. */
  costCurrency?: string | null;
  quoteAvailable?: boolean;
}

export interface CurrencyPortfolioSummary {
  currency: string;
  totalValue: number;
  totalCost: number;
  totalPl: number;
  totalPlPct: number;
  isPlPositive: boolean;
  /** Priced holdings left out of the cost and P/L, having no cost in this currency. */
  uncosted: { symbol: string; value: number }[];
}

function finiteNumber(value: unknown, fallback = 0): number {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

export function normalizePortfolioCurrency(currency: unknown): string {
  if (typeof currency !== 'string') return DEFAULT_PORTFOLIO_CURRENCY;
  const normalized = currency.trim().toUpperCase();
  return /^[A-Z]{3}$/.test(normalized) ? normalized : DEFAULT_PORTFOLIO_CURRENCY;
}

/**
 * What a holding cost, in the currency its value is in, or null when that is
 * unknown: no cost entered, or a cost entered in another currency than the
 * one it is quoted in, which no price can be measured against.
 */
export function costBasis(row: PortfolioSummaryRow): number | null {
  if (row.average_cost == null) return null;
  if (row.costCurrency != null && normalizePortfolioCurrency(row.costCurrency) !== normalizePortfolioCurrency(row.currency)) {
    return null;
  }
  return finiteNumber(row.average_cost) * finiteNumber(row.quantity);
}

/**
 * NAV per currency. The value counts every priced holding, while the cost
 * and P/L count only those with a cost basis, so a holding with no cost is
 * never read as all profit; those are named in `uncosted`.
 */
export function summarizePortfolioByCurrency(rows: PortfolioSummaryRow[]): CurrencyPortfolioSummary[] {
  const groups = new Map<string, { currency: string; totalValue: number; totalCost: number; costedValue: number; uncosted: CurrencyPortfolioSummary['uncosted'] }>();

  rows
    .filter((row) => row.quoteAvailable !== false && row.marketValue != null)
    .forEach((row) => {
      const currency = normalizePortfolioCurrency(row.currency);
      const current = groups.get(currency) ?? { currency, totalValue: 0, totalCost: 0, costedValue: 0, uncosted: [] };
      const value = finiteNumber(row.marketValue);
      const basis = costBasis(row);
      current.totalValue += value;
      if (basis == null) {
        current.uncosted.push({ symbol: row.symbol, value });
      } else {
        current.totalCost += basis;
        current.costedValue += value;
      }
      groups.set(currency, current);
    });

  return Array.from(groups.values())
    .map(({ costedValue, ...summary }) => {
      const totalPl = summary.totalCost > 0 ? costedValue - summary.totalCost : 0;
      const totalPlPct = summary.totalCost > 0 ? (totalPl / summary.totalCost) * 100 : 0;
      return {
        ...summary,
        totalPl,
        totalPlPct,
        isPlPositive: totalPl >= 0,
      };
    })
    .sort((a, b) => {
      if (a.currency === DEFAULT_PORTFOLIO_CURRENCY) return -1;
      if (b.currency === DEFAULT_PORTFOLIO_CURRENCY) return 1;
      return a.currency.localeCompare(b.currency);
    });
}

/** A cost entered in `currency` is in the major unit of a listing quoted in a
 *  minor one (pounds for a pence-quoted London line), so it runs to a few
 *  units where the listing's own quotes elsewhere run to hundreds. */
export function isMinorUnitListingInMajor(symbol: string | null | undefined, currency: unknown): boolean {
  return !!minorUnitPlacesForSymbol(symbol) && normalizePortfolioCurrency(currency) === currencyForSymbol(symbol);
}

export function formatPortfolioMoney(amount: number, currency: unknown, locale?: string): string {
  return formatMoney(finiteNumber(amount), normalizePortfolioCurrency(currency), locale || '');
}

export function formatPortfolioMoneyCode(amount: number, currency: unknown): string {
  return `${normalizePortfolioCurrency(currency)} ${finiteNumber(amount).toFixed(2)}`;
}
