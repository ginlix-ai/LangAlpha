/**
 * The quote's provenance as the lines a reader hovers for: where the quote
 * comes from and how current it is, the same for the chart's own bars, and
 * the socket. StockHeader and the legend lead both show them, so they are
 * worded once here.
 */

import { useTranslation } from 'react-i18next';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import { chartFreshnessParts, freshnessSpecText, venueTime } from '@/lib/freshness';
import type { Freshness } from '@/types/market';
import { quoteSourceText, type StockQuoteModel } from './useStockQuoteModel';
import type { ConnectionStatus, DataLevel } from './useMarketDataWS';

export interface QuoteSourceInputs {
  symbol: string;
  quote: StockQuoteModel;
  wsStatus: ConnectionStatus;
  wsHasData?: boolean;
  wsDataLevel?: DataLevel;
  /** Measured freshness of the chart's bars: a different surface, often a different provider, from the quote's. */
  chartFreshness?: Freshness | null;
}

/** Which `marketView.header.ws` phrase describes the socket. */
function wsStatusKey(status: ConnectionStatus, hasData: boolean, level: DataLevel): string {
  if (status === 'connected') {
    if (!hasData) return 'connectedNoData';
    return level === 'second' ? 'connectedSecond' : 'connectedMinute';
  }
  if (status === 'disabled') return 'unavailable';
  return status === 'reconnecting' ? 'reconnecting' : 'disconnected';
}

export function useQuoteSourceLines({
  symbol, quote: q, wsStatus, wsHasData = false, wsDataLevel = null, chartFreshness = null,
}: QuoteSourceInputs): string[] {
  const { t } = useTranslation();
  const locale = useLocale();
  // Which venue day is today decides whether a stamp carries its date.
  const now = useNow();

  // Two surfaces, measured independently: the quote and the chart routinely
  // come from different providers and disagree, which is the whole reason both
  // lines exist. With nothing to state we never fall back to "Realtime".
  const quoteLine = [
    quoteSourceText(t, q),
    freshnessSpecText(t, q.quoteBadge) ?? t('marketView.header.quoteUnknownTier'),
    venueTime(q.asOf, symbol, locale, now),
  ].filter(Boolean).join(' · ');
  const chartParts = chartFreshness ? chartFreshnessParts(t, chartFreshness, symbol, locale, now) : null;
  const chartLine = chartParts
    ? [chartParts.source, chartParts.state, chartParts.lastBar].filter(Boolean).join(' · ')
    : null;

  return [
    `${t('marketView.header.quote')}: ${quoteLine}`,
    ...(chartLine ? [`${t('marketView.header.chart')}: ${chartLine}`] : []),
    t('marketView.header.ws.label', { status: t(`marketView.header.ws.${wsStatusKey(wsStatus, wsHasData, wsDataLevel)}`) }),
  ];
}
