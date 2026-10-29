/**
 * The company-overview artifact and the one reading of its quote block.
 *
 * Three surfaces draw this block (the inline chat card, the chat detail card
 * and the MarketView panel). Reading it once here keeps them agreeing on what a
 * figure is: an index level is points on all three, never `$5,812.34` on one.
 *
 * Leaf imports only, not the `@/lib/bars` barrel: the barrel reaches the API
 * client, and this module must load in a DOM-free test.
 */
import { isIndexListing, quoteCurrency, resolveCurrency } from '@/lib/bars/exchanges';
import { resolveDualName, type DualName } from '@/lib/displayName';
import type { Freshness } from '@/types/market';

/**
 * The overview's `quote` block. Two shapes arrive under one key: a financial
 * provider's quote (`price` / `change` / `changePct`, also what an index
 * sends), and, when a snapshot was available, the snapshot's split of the
 * regular session from the extended one (`regularClose` / `regularChange` /
 * `lastTradePrice` / `marketStatus`). An alias rather than an interface, so
 * it passes where the chart takes an untyped quote record.
 */
export type OverviewQuote = {
  price?: number | null;
  change?: number | null;
  changePct?: number | null;
  regularClose?: number | null;
  regularChange?: number | null;
  regularChangePct?: number | null;
  lastTradePrice?: number | null;
  marketStatus?: string | null;
  earlyTradingChangePct?: number | null;
  lateTradingChangePct?: number | null;
  open?: number | null;
  previousClose?: number | null;
  dayHigh?: number | null;
  dayLow?: number | null;
  yearHigh?: number | null;
  yearLow?: number | null;
  volume?: number | null;
  avgVolume?: number | null;
  marketCap?: number | null;
  pe?: number | null;
  eps?: number | null;
  /** Declared tier, unnarrowed: `asQuoteTier` reads it. */
  tier?: string | null;
  freshness?: Freshness | null;
  source?: string;
  as_of_local?: string;
  printed?: boolean;
};

// Chart rows are type aliases, not interfaces: the recharts components take
// `Record<string, unknown>[]`, which only an alias is assignable to.
export type QuarterlyFundamental = {
  period: string;
  date?: string | null;
  revenue?: number | null;
  netIncome?: number | null;
  grossProfit?: number | null;
  operatingIncome?: number | null;
  ebitda?: number | null;
  epsDiluted?: number | null;
  grossMargin?: number | null;
  operatingMargin?: number | null;
  netMargin?: number | null;
};

export type EarningsSurprise = {
  period: string;
  date?: string | null;
  epsActual?: number | null;
  epsEstimate?: number | null;
  revenueActual?: number | null;
  revenueEstimate?: number | null;
};

export type CashFlowQuarter = {
  period: string;
  date?: string | null;
  operatingCashFlow?: number | null;
  capitalExpenditure?: number | null;
  freeCashFlow?: number | null;
};

export type AnalystRatings = {
  strongBuy?: number;
  buy?: number;
  hold?: number;
  sell?: number;
  strongSell?: number;
  consensus?: string;
};

export type ShareFloat = { free_float?: number | null; free_float_percent?: number | null };
export type ShortInterest = { short_interest?: number | null; settlement_date?: string | null; days_to_cover?: number | null };
export type ShortVolume = { short_volume_ratio?: number | null; date?: string | null };

/** The `company_overview` artifact, as the agent tool and the overview endpoint send it. */
export interface CompanyOverviewArtifact {
  /** `company_overview`. A plain string, since the detail view hands over a generic tool-result record. */
  type?: string;
  symbol?: string;
  name?: string | null;
  nameEn?: string | null;
  nameLocal?: string | null;
  /** ISO code the quote is priced in. */
  currency?: string | null;
  /** ISO code the statement figures are reported in (CNY for 0700.HK). */
  reportedCurrency?: string | null;
  /** `index` for an index: a level in points and no company fundamentals. */
  assetClass?: string | null;
  quote?: OverviewQuote;
  performance?: Record<string, number>;
  analystRatings?: AnalystRatings;
  quarterlyFundamentals?: QuarterlyFundamental[];
  earningsSurprises?: EarningsSurprise[];
  cashFlow?: CashFlowQuarter[];
  revenueByProduct?: Record<string, number>;
  revenueByGeo?: Record<string, number>;
  float?: ShareFloat;
  /** One record; threads recorded before that carry the history as an array. */
  shortInterest?: ShortInterest | ShortInterest[];
  shortVolume?: ShortVolume;
  error?: string;
}

export type OverviewQuoteSource = Pick<
  CompanyOverviewArtifact,
  'name' | 'nameEn' | 'nameLocal' | 'currency' | 'reportedCurrency' | 'assetClass' | 'quote'
>;

export interface DerivedOverviewQuote {
  /** The regular-session figure: the snapshot's close, else the provider's price. */
  displayPrice: number | null;
  displayChange: number | null;
  displayChangePct: number | null;
  marketStatus: string | null;
  isExtended: boolean;
  /** Last extended-hours trade, set while one prints off the close. */
  extPrice: number | null;
  /** Move of `extPrice` off the close; 0 without one. */
  extDiff: number;
  extDiffPct: number;
  hasExtPrice: boolean;
  isIndex: boolean;
  /** What the quote's figures print in, or null for an index level. */
  currency: string | null;
  /** What the statements (revenue, cash flow, segments) print in: always money. */
  statementCurrency: string;
  /** What earnings print in: consensus figures in the trading currency, so an
   *  ADR's EPS is dollars per ADS though its statements report in TWD or CNY. */
  earningsCurrency: string;
  dualName: DualName;
}

export function deriveOverviewQuote(data: OverviewQuoteSource, symbol: string): DerivedOverviewQuote {
  const q = data.quote ?? {};
  const displayPrice = q.regularClose ?? q.price ?? null;
  const marketStatus = q.marketStatus ?? null;
  const isExtended = marketStatus === 'early_trading' || marketStatus === 'late_trading';
  const extPrice = q.lastTradePrice ?? null;
  const hasExtPrice = isExtended && extPrice != null && displayPrice != null && extPrice !== displayPrice;
  const extDiff = hasExtPrice ? extPrice - displayPrice : 0;
  return {
    displayPrice,
    displayChange: q.regularChange ?? q.change ?? null,
    displayChangePct: q.regularChangePct ?? q.changePct ?? null,
    marketStatus,
    isExtended,
    extPrice,
    extDiff,
    extDiffPct: hasExtPrice && displayPrice ? (extDiff / displayPrice) * 100 : 0,
    hasExtPrice,
    isIndex: isIndexListing(symbol, data.assetClass),
    // A payload without `currency` falls back to the venue suffix, not to USD.
    currency: quoteCurrency(data.currency, symbol, data.assetClass),
    // Statement figures are money even where the quote is points, so an index
    // (which has no statements to print) still names its venue's currency.
    statementCurrency: data.reportedCurrency || resolveCurrency(data.currency, symbol),
    earningsCurrency: resolveCurrency(data.currency, symbol),
    dualName: resolveDualName({ name: data.name, nameLocal: data.nameLocal, nameEn: data.nameEn }, symbol),
  };
}
