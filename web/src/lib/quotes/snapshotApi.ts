/**
 * Batch snapshot API primitives for the quote layer.
 *
 * Live in lib/quotes so nothing in lib/ imports a page — `quoteBatcher` calls
 * these directly and Dashboard/utils/api re-exports them for back-compat. Both
 * hit the shared API client and return the raw batch envelope
 * (`snapshots | results | data`), swallowing network errors to `{}`. The
 * batcher reads `fetchSnapshotBatch` instead, which throws: a refresh that
 * failed must not land in the cache as one that answered.
 */
import { api } from '@/api/client';
import { INDEX_SYMBOLS } from '@/lib/indices';
import { normalizeIndexKey } from '@/lib/marketUtils';
import type { SnapshotBatchResponse, SnapshotData } from '@/types/market';

export type SnapshotEntry = SnapshotData;
export type SnapshotResponse = SnapshotBatchResponse;

/** One batch read, throwing on failure; the symbols go as given. */
export async function fetchSnapshotBatch(kind: 'stocks' | 'indexes', symbols: readonly string[]): Promise<SnapshotResponse> {
  if (!symbols.length) return {};
  const { data } = await api.get(`/api/v1/market-data/snapshots/${kind}`, { params: { symbols: symbols.join(',') } });
  return data || {};
}

/**
 * GET /api/v1/market-data/snapshots/indexes?symbols=GSPC,IXIC,...
 * Batch snapshot for index symbols (caret-stripped, uppercased).
 */
export async function getSnapshotIndexes(symbols: readonly string[] = INDEX_SYMBOLS): Promise<SnapshotResponse> {
  try {
    return await fetchSnapshotBatch('indexes', symbols.map((s) => normalizeIndexKey(s)));
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
  try {
    return await fetchSnapshotBatch('stocks', [...(symbols || [])].map((s) => String(s).trim().toUpperCase()).filter(Boolean));
  } catch (e: unknown) {
    const err = e as { message?: string };
    console.error('[API] getSnapshotStocks failed:', err?.message);
    return {};
  }
}
