/**
 * The one derivation of "what the header shows" from the raw quote inputs:
 * price and change, the day's figures, the extended-hours pair, the session
 * status and the data source. Both the full StockHeader and the thin legend
 * strips read it, so a fallback rule (live tick over REST quote over
 * snapshot) and the choice of headline number are decided once.
 */

import { useMemo } from 'react';
import type { TFunction } from 'i18next';
import { fixed2 } from '@/lib/format';
import { getExtendedHoursInfo } from '@/lib/marketUtils';
import { quoteCurrency } from '@/lib/bars';
import { resolveDualName, type SymbolDisplayOverride } from '@/lib/displayName';
import { providerLabel } from '@/lib/providerLabels';
import { FRESHNESS_TONE, freshnessSpecText, isLiveRow, resolveFreshnessBadge, type FreshnessSpec } from '@/lib/freshness';
import { isUSEquity } from '../utils/chartConstants';
import type { StockInfo, RealTimePrice, SnapshotData } from '@/types/market';
import type { PriceUpdate, ConnectionStatus } from './useMarketDataWS';
import type { OverviewQuote } from '@/lib/quotes/overview';

export interface StockQuoteInputs {
  symbol: string;
  stockInfo: StockInfo | null;
  realTimePrice: PriceUpdate | RealTimePrice | null;
  quoteData: OverviewQuote | null;
  snapshot: SnapshotData | null;
  marketStatus: Record<string, unknown> | null;
  wsStatus: ConnectionStatus;
  wsHasData?: boolean;
  /** Venue market phase (`pre|open|post|closed`) from the chart's bars responses; null until known. */
  marketPhase?: string | null;
  /** A clicked search hit: a complete name source, never mixed with a stockInfo that may still hold the previous symbol. */
  displayOverride?: SymbolDisplayOverride | null;
}

/** The placeholder a quote figure prints while it has no value. */
export const DASH = '—';
/** The quote figures' formatter: `fixed2` takes a number, this decides what an absent one shows. */
export const fixed2OrDash = (n: number | null | undefined, locale: string): string => (n != null ? fixed2(n, locale) : DASH);

export type ChangeTone = 'positive' | 'negative' | '';

export type QuoteStatus = 'live' | 'closed' | 'delayed';

export interface StockQuoteModel {
  price: number | null;
  /** Null when the feed reported no change pair; a reader shows a dash, never +0.00. */
  change: number | null;
  changePercent: number | null;
  tone: ChangeTone;
  /**
   * The big number and its change pair. In an extended session this is the
   * settled close (market convention) and the session move rides in `ext`;
   * otherwise it is the row price. Decided here so the two presentations
   * cannot pick differently.
   */
  headline: { price: number | null; change: number | null; pct: number | null; tone: ChangeTone };
  /** Live feed wins over the venue phase; an unknown phase reads as delayed. */
  status: QuoteStatus;
  /** When the live tick arrived, for the time beside the Live badge. */
  tickAt: number | null;
  previousClose: number | null;
  open: number | null;
  high: number | null;
  low: number | null;
  fiftyTwoWeekHigh: number | null;
  fiftyTwoWeekLow: number | null;
  averageVolume: number | null;
  volume: number | null;
  /** The volume a row prints: the session's, or the 3-month average when the row has none. */
  shownVolume: number | null;
  /** True when `shownVolume` is the average, so the reader labels it as such. */
  volumeIsAverage: boolean;
  /** Local-script name first for a CN/HK listing; the other name rides in `displaySecondaryName`. */
  displayName: string;
  displaySecondaryName: string | null;
  /** False when no source named the listing: the fallback is the symbol, already shown beside it. */
  hasName: boolean;
  displayExchange: string;
  /** Raw provider ids behind the quote; `quoteSourceText` names them. Empty when nothing is known. */
  dataSources: string[];
  /** ISO code the figures are quoted in: the row's own, else the listing venue's. Null for an index, whose level is points. */
  currency: string | null;
  /** How current the quote row is, decided by lib/freshness; null when the row states nothing. */
  quoteBadge: FreshnessSpec | null;
  /** A REST print that is current: the session dot reads steady green rather than delayed. */
  quoteIsFresh: boolean;
  /** When the quote row's price printed, epoch ms. */
  asOf: number | null;
  /** Extended-hours session, when the venue is in one and the row carries it. */
  ext: {
    type: 'pre' | 'post';
    price: number;
    change: number | null;
    pct: number;
    /** The official close the big number shows meanwhile, with its own change pair. */
    settledClose: number;
    settledChange: number | null;
    settledChangePct: number | null;
    settledTone: ChangeTone;
  } | null;
}

function toneOf(n: number | null | undefined): ChangeTone {
  if (n == null || n === 0) return '';
  return n > 0 ? 'positive' : 'negative';
}

export function deriveStockQuote({
  symbol, stockInfo, realTimePrice, quoteData, snapshot, marketStatus, wsStatus, wsHasData = false, marketPhase = null, displayOverride = null,
}: StockQuoteInputs): StockQuoteModel {
  const price = realTimePrice?.price ?? stockInfo?.Price ?? null;
  const change = realTimePrice?.change ?? null;
  const changePercent = realTimePrice?.changePercent ?? null;
  const tone = toneOf(change);
  const previousClose = snapshot?.previous_close ?? quoteData?.previousClose ?? null;

  // Extended hours (market convention): the big number is the last official
  // close — today's regular close after-hours, the previous close pre-market —
  // with a coherent change pair against the previous close; the extended move
  // renders on its own against its declared anchor. A live tick (has a
  // timestamp; quote rows don't) overrides the derived ext price.
  const { extPct, extType, extPrice, extChange, extAnchor, regularClose } = getExtendedHoursInfo(marketStatus, snapshot);
  const tickAt = (realTimePrice as PriceUpdate | null)?.timestamp ?? null;
  const tickPrice = tickAt != null ? (realTimePrice?.price ?? null) : null;
  const extDisplayPrice = tickPrice ?? extPrice;
  const extDisplayChange = tickPrice != null && extAnchor != null ? tickPrice - extAnchor : extChange;
  const extDisplayPct = tickPrice != null && extAnchor ? ((tickPrice - extAnchor) / extAnchor) * 100 : extPct;
  const settledClose = (extType === 'post' ? regularClose : previousClose) ?? null;
  const settledChange = extType === 'post' && regularClose != null && previousClose != null ? regularClose - previousClose : null;
  const settledChangePct = settledChange != null && previousClose ? (settledChange / previousClose) * 100 : null;

  const ext = extType && settledClose != null && extDisplayPrice != null && extDisplayPct != null
    ? {
        type: extType,
        price: extDisplayPrice,
        change: extDisplayChange ?? null,
        pct: extDisplayPct,
        settledClose,
        settledChange,
        settledChangePct,
        settledTone: toneOf(settledChange),
      }
    : null;

  const headline = ext
    ? { price: ext.settledClose, change: ext.settledChange, pct: ext.settledChangePct, tone: ext.settledTone }
    : { price, change, pct: changePercent, tone };

  const isLive = wsStatus === 'connected' && isUSEquity(symbol) && wsHasData;
  const status: QuoteStatus = isLive ? 'live' : marketPhase === 'closed' ? 'closed' : 'delayed';
  const providers = (marketStatus?.providers ?? []) as string[];
  const activeSource = isLive ? 'ginlix-data' : (snapshot?.source ?? null);
  const dataSources = activeSource ? [activeSource] : providers;

  const averageVolume = quoteData?.avgVolume ?? stockInfo?.AverageVolume ?? null;
  const volume = stockInfo?.Volume ?? null;

  const nameSource = displayOverride ?? { name: stockInfo?.Name, nameLocal: stockInfo?.NameLocal, nameEn: stockInfo?.NameEn };
  const { primary: displayName, secondary: displaySecondaryName, named: hasName } = resolveDualName(nameSource, symbol);
  const quoteFreshness = snapshot?.freshness ?? null;
  const quoteFields = { tier: snapshot?.tier, freshness: quoteFreshness };

  return {
    price,
    change,
    changePercent,
    tone,
    headline,
    status,
    tickAt,
    previousClose,
    open: realTimePrice?.open ?? stockInfo?.Open ?? null,
    high: realTimePrice?.high ?? stockInfo?.High ?? null,
    low: realTimePrice?.low ?? stockInfo?.Low ?? null,
    fiftyTwoWeekHigh: quoteData?.yearHigh ?? stockInfo?.['52WeekHigh'] ?? null,
    fiftyTwoWeekLow: quoteData?.yearLow ?? stockInfo?.['52WeekLow'] ?? null,
    averageVolume,
    volume,
    shownVolume: volume ?? averageVolume,
    volumeIsAverage: volume == null && averageVolume != null,
    displayName,
    displaySecondaryName,
    hasName,
    displayExchange: displayOverride?.exchange ?? stockInfo?.Exchange ?? '',
    dataSources,
    currency: quoteCurrency(snapshot?.currency, symbol, snapshot?.asset_class),
    quoteBadge: resolveFreshnessBadge(quoteFields),
    quoteIsFresh: isLiveRow(quoteFields),
    asOf: snapshot?.as_of ?? quoteFreshness?.actual_latest ?? null,
    ext,
  };
}

/**
 * The session word both presentations print. Live and closed belong to the
 * feed and the venue; otherwise the quote row's own freshness decides, and
 * with nothing stated it reads delayed, never realtime.
 */
export function quoteSessionText(t: TFunction, q: StockQuoteModel): string {
  if (q.status === 'live') return t('marketView.quote.live');
  if (q.status === 'closed') return t('marketView.quote.closed');
  return freshnessSpecText(t, q.quoteBadge) ?? t('marketView.quote.delayed');
}

/** Where the quote comes from, in the app's language; "REST" when no provider is named. */
export function quoteSourceText(t: TFunction, q: StockQuoteModel): string {
  return q.dataSources.map((s) => providerLabel(s, t)).join(', ') || 'REST';
}

/**
 * The session dot: the pulse is the socket's, a current REST print is steady
 * green, and otherwise the quote's freshness tone decides, the same table the
 * inline badge reads, so a stale row warns in both and a delay warns in neither.
 */
export function quoteDotState(q: StockQuoteModel): QuoteStatus | 'realtime' | 'warning' {
  if (q.status !== 'delayed') return q.status;
  if (q.quoteIsFresh) return 'realtime';
  return q.quoteBadge && FRESHNESS_TONE[q.quoteBadge.state] === 'warning' ? 'warning' : 'delayed';
}

/**
 * Memoized on the input fields, not the bag, so a caller may build the bag
 * inline; `displayOverride` is the one object input and must be stable.
 */
export function useStockQuoteModel(inputs: StockQuoteInputs): StockQuoteModel {
  const { symbol, stockInfo, realTimePrice, quoteData, snapshot, marketStatus, wsStatus, wsHasData, marketPhase, displayOverride } = inputs;
  return useMemo(
    () => deriveStockQuote({ symbol, stockInfo, realTimePrice, quoteData, snapshot, marketStatus, wsStatus, wsHasData, marketPhase, displayOverride }),
    [symbol, stockInfo, realTimePrice, quoteData, snapshot, marketStatus, wsStatus, wsHasData, marketPhase, displayOverride],
  );
}
