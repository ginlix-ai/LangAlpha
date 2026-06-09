const DEFAULT_PORTFOLIO_CURRENCY = 'USD';
const portfolioMoneyFormatterCache = new Map<string, Intl.NumberFormat>();

export interface PortfolioSummaryRow {
  marketValue?: number | null;
  average_cost?: number | null;
  quantity?: number | null;
  currency?: string | null | undefined;
  quoteAvailable?: boolean;
}

export interface CurrencyPortfolioSummary {
  currency: string;
  totalValue: number;
  totalCost: number;
  totalPl: number;
  totalPlPct: number;
  isPlPositive: boolean;
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

export function summarizePortfolioByCurrency(rows: PortfolioSummaryRow[]): CurrencyPortfolioSummary[] {
  const groups = new Map<string, { currency: string; totalValue: number; totalCost: number }>();

  rows
    .filter((row) => row.quoteAvailable !== false && row.marketValue != null)
    .forEach((row) => {
      const currency = normalizePortfolioCurrency(row.currency);
      const current = groups.get(currency) ?? { currency, totalValue: 0, totalCost: 0 };
      const quantity = finiteNumber(row.quantity);
      current.totalValue += finiteNumber(row.marketValue);
      current.totalCost += row.average_cost != null ? finiteNumber(row.average_cost) * quantity : 0;
      groups.set(currency, current);
    });

  return Array.from(groups.values())
    .map((summary) => {
      const totalPl = summary.totalCost > 0 ? summary.totalValue - summary.totalCost : 0;
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

export function formatPortfolioMoney(amount: number, currency: unknown, locale?: string): string {
  const normalizedCurrency = normalizePortfolioCurrency(currency);
  const normalizedAmount = finiteNumber(amount);

  try {
    const cacheKey = `${locale || ''}::${normalizedCurrency}`;
    let formatter = portfolioMoneyFormatterCache.get(cacheKey);
    if (!formatter) {
      formatter = new Intl.NumberFormat(locale || undefined, {
        style: 'currency',
        currency: normalizedCurrency,
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      });
      portfolioMoneyFormatterCache.set(cacheKey, formatter);
    }
    return formatter.format(normalizedAmount);
  } catch {
    return `${normalizedCurrency} ${normalizedAmount.toFixed(2)}`;
  }
}

export function formatPortfolioMoneyCode(amount: number, currency: unknown): string {
  return `${normalizePortfolioCurrency(currency)} ${finiteNumber(amount).toFixed(2)}`;
}
