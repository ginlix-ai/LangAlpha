/**
 * The Dashboard's index cards: today's sparkline per index from its intraday
 * bars. Kept apart from ./api, which the app shell loads on first paint for
 * its user calls, so the session slicing loads with the Dashboard instead.
 */
import { api } from '@/api/client';
import { rowsToChartBars } from '@/lib/bars/barsClient';
import { US_MARKET_HOURS, US_MARKET_TZ } from '@/lib/bars/exchanges';
import { latestSession, wallClock } from '@/lib/bars/intradaySession';
import type { IndexData } from '@/types/market';
import { INDEX_NAMES, normalizeIndexSymbol } from './api';

/**
 * GET /api/v1/market-data/intraday/indexes/:symbol (Index.yml)
 * Path uses normalized symbol (e.g. GSPC). Query: interval, from, to optional.
 * Returns the most recent data point for the index.
 */
export async function getIndex(symbol: string): Promise<IndexData> {
  const norm = normalizeIndexSymbol(String(symbol).trim());
  try {
    const { data } = await api.get(`/api/v1/market-data/intraday/indexes/${encodeURIComponent(norm)}`);

    const pts = data?.data;

    if (!Array.isArray(pts) || !pts.length) {
      throw new Error(`No intraday data for ${norm}`);
    }

    const session = latestSession(rowsToChartBars(pts, US_MARKET_TZ), US_MARKET_HOURS);
    if (!session) {
      throw new Error(`No intraday data for ${norm}`);
    }
    const latestDate = session.date;
    const todayPoints = session.bars;

    const oldest = todayPoints[0];
    const mostRecent = todayPoints[todayPoints.length - 1];

    const open = oldest.open;
    const close = mostRecent.close;
    const change = close - open;
    const changePercent = open ? (change / open) * 100 : 0;

    const result: IndexData = {
      symbol: norm,
      name: INDEX_NAMES[norm] ?? norm,
      price: Math.round(close * 100) / 100,
      change: Math.round(change * 100) / 100,
      changePercent: Math.round(changePercent * 100) / 100,
      isPositive: change >= 0,
      // Direct getIndex consumers get the same zero/invalid-close masking the
      // dashboard applies via the snapshot: a non-positive close isn't a quote.
      quoteAvailable: close > 0,
      asOfDate: latestDate,
      sparklineData: todayPoints
        .filter((p) => p.close > 0)
        .map((p) => ({ time: wallClock(p.time), val: p.close })),
    };

    return result;
  } catch (e: unknown) {
    const err = e as { response?: { status?: number; data?: { detail?: unknown } }; message?: string };
    console.error(`[API] getIndex - ${norm}: Error:`, err?.message);
    const msg = err.response?.data?.detail ?? err.message;
    throw new Error(typeof msg === 'string' ? msg : String(msg));
  }
}
