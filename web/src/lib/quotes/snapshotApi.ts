/**
 * Batch snapshot API primitives for the quote layer.
 *
 * Live in lib/quotes so nothing in lib/ imports a page — `quoteBatcher` calls
 * these directly and Dashboard/utils/api re-exports them for back-compat. Both
 * hit the shared API client and return the raw batch envelope
 * (`snapshots | results | data`), swallowing network errors to `{}`.
 */
import { api } from '@/api/client';
import { INDEX_SYMBOLS } from '@/lib/indices';
import { normalizeIndexKey } from '@/lib/marketUtils';
import type { SnapshotBatchResponse, SnapshotData } from '@/types/market';

export type SnapshotEntry = SnapshotData;
export type SnapshotResponse = SnapshotBatchResponse;

/**
 * GET /api/v1/market-data/snapshots/indexes?symbols=GSPC,IXIC,...
 * Batch snapshot for index symbols (caret-stripped, uppercased).
 */
export async function getSnapshotIndexes(symbols: readonly string[] = INDEX_SYMBOLS): Promise<SnapshotResponse> {
  const list = symbols.map((s) => normalizeIndexKey(s));
  try {
    const { data } = await api.get('/api/v1/market-data/snapshots/indexes', {
      params: { symbols: list.join(',') },
    });
    return data || {};
  } catch (e: unknown) {
    const err = e as { message?: string };
    console.error('[API] getSnapshotIndexes failed:', err?.message);
    return {};
  }
}

/**
 * GET /api/v1/market-data/snapshots/stocks?symbols=AAPL,TSLA,...
 * Batch snapshot for stock symbols (trimmed, uppercased).
 */
export async function getSnapshotStocks(symbols: string[]): Promise<SnapshotResponse> {
  const list = [...(symbols || [])].map((s) => String(s).trim().toUpperCase()).filter(Boolean);
  if (!list.length) return {};
  try {
    const { data } = await api.get('/api/v1/market-data/snapshots/stocks', {
      params: { symbols: list.join(',') },
    });
    return data || {};
  } catch (e: unknown) {
    const err = e as { message?: string };
    console.error('[API] getSnapshotStocks failed:', err?.message);
    return {};
  }
}
