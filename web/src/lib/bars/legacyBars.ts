/**
 * Legacy (pre-protocol) bar loader — the full-window REST fetch the CMDP
 * `fetchBarsDelta` falls back to when the progressive `/bars/` endpoint is
 * absent (404 / network). De-facto shared: MarketView, the dashboard chart
 * widgets, and ChatAgent's inline charts all read historical bars through here.
 *
 * Kept in lib (not under pages/MarketView) so nothing in lib/ imports a page.
 * Normalization goes through the same {@link rowsToChartBars}/{@link coerceWatermark}
 * helpers the protocol client uses, so both pipelines produce identical bars.
 */
import { api } from '@/api/client';

import { coerceWatermark, rowsToChartBars } from './barsClient';
import { timezoneForSymbol } from './exchanges';
import type { ChartBar, LoaderMeta } from './marketProtocol';

export interface StockDataResult {
  data: ChartBar[];
  error?: string;
  /**
   * Cache/presentation metadata from the response envelope. Legacy endpoints
   * ship `{watermark, complete, market_phase, truncated, cached}`; the protocol
   * endpoint additionally carries currency/decimals.
   */
  meta?: LoaderMeta;
}

/** Parse the (previously discarded) cache metadata off a bars response envelope. */
function parseLoaderMeta(body: Record<string, unknown> | null | undefined): LoaderMeta {
  // The wire nests cache metadata under `cache` ({symbol, data, count, cache:
  // {watermark, market_phase, ...}}); fall back to the body itself for older
  // backends that shipped the fields flat.
  const envelope = (body?.cache ?? body) as Record<string, unknown> | null | undefined;
  return {
    watermark: coerceWatermark(envelope?.watermark),
    complete: envelope?.complete !== false,
    marketPhase: (envelope?.market_phase as string) ?? null,
    nextChangeAt: typeof envelope?.next_change_at === 'number' ? envelope.next_change_at : null,
    truncated: typeof envelope?.truncated === 'boolean' ? envelope.truncated : undefined,
    cached: typeof envelope?.cached === 'boolean' ? envelope.cached : undefined,
    currency: (envelope?.price_currency as string) || undefined,
    displayDecimals: typeof envelope?.display_decimals === 'number' ? envelope.display_decimals : undefined,
  };
}

/**
 * Fetch stock/index historical bars for charting via the legacy REST endpoints
 * (`/market-data/daily/*` for 1day, `/market-data/intraday/*` otherwise).
 * Returns chart-native bars plus the response envelope metadata.
 */
export async function fetchStockData(
  symbol: string,
  interval: string = '1hour',
  fromDate: string | undefined,
  toDate: string | undefined,
  { signal }: { signal?: AbortSignal } = {},
): Promise<StockDataResult> {
  if (!symbol || !symbol.trim()) {
    return { data: [], error: 'Symbol is required' };
  }

  const symbolUpper = symbol.trim().toUpperCase();
  const isIndex = symbolUpper.startsWith('^');

  try {
    // Use daily endpoint for 1day interval, intraday endpoint for everything else
    const isDaily = interval === '1day';
    const market = isIndex ? 'indexes' : 'stocks';
    const url = isDaily
      ? `/api/v1/market-data/daily/${market}/${encodeURIComponent(symbolUpper)}`
      : `/api/v1/market-data/intraday/${market}/${encodeURIComponent(symbolUpper)}`;
    const params: Record<string, string> = isDaily ? {} : { interval };

    if (fromDate) params.from = fromDate;
    if (toDate) params.to = toDate;

    const { data } = await api.get(url, { params, signal });

    const dataPoints = data?.data || [];

    if (!Array.isArray(dataPoints) || dataPoints.length === 0) {
      return { data: [], error: 'No data available' };
    }

    // Backend returns { time: <unix_ms>, open, high, low, close, volume } →
    // chart bars in the symbol's venue wall clock.
    const chartData = rowsToChartBars(dataPoints, timezoneForSymbol(symbolUpper));

    if (chartData.length === 0) {
      return { data: [], error: 'Data conversion failed' };
    }

    return {
      data: chartData,
      meta: parseLoaderMeta(data as Record<string, unknown>),
    };
  } catch (error: unknown) {
    // Don't treat abort as an error
    if (error instanceof Error && (error.name === 'CanceledError' || error.name === 'AbortError')) {
      return { data: [], error: 'Request cancelled' };
    }
    console.error('Error fetching stock data from backend:', error);
    const axiosError = error as { response?: { data?: { detail?: string } }; message?: string };
    const errorMsg = axiosError?.response?.data?.detail || axiosError?.message || 'Failed to fetch stock data';
    return { data: [], error: errorMsg };
  }
}
