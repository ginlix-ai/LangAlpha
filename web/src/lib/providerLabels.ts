import type { TFunction } from 'i18next';

/**
 * Display names for the data providers a quote or bar series can come from.
 * Shared so the MarketView header and the chat inline cards attribute the same
 * source with the same words. Brand names are not translated.
 */
const PROVIDER_LABELS: Record<string, string> = {
  'ginlix-data': 'Ginlix Data',
  fmp: 'FMP',
  yfinance: 'Yahoo Finance',
  tushare: 'Tushare',
};

/** Provider display name; unknown sources pass through verbatim. `daily` is a
 *  series granularity the bars API reports in the same field, so it is words
 *  rather than a brand and needs the catalog. */
export function providerLabel(source: string | null | undefined, t: TFunction): string | null {
  if (!source) return null;
  if (source === 'daily') return t('marketView.header.sourceDaily');
  return PROVIDER_LABELS[source] ?? source;
}
