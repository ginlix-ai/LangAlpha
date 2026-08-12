/**
 * quoteBatcher — coalescing batcher for the unified per-symbol quote cache.
 *
 * Components request quotes per symbol (via the useQuote/useQuotes hooks, whose
 * queryFn delegates here). Requests that land within a short window (~50ms)
 * coalesce into ONE batch snapshot request — stocks and indexes batched
 * separately, since they hit different endpoints. The response fans out
 * per-symbol into the React Query cache under ['quote', SYMBOL] via
 * setQueryData, so every widget showing that symbol shares one cache entry
 * (kills Context bug #8: overlapping watchlists never sharing a quote).
 *
 * Key = uppercase legacy symbol spelling (indexes stripped of a leading '^').
 * This is interim until Phase 4 re-keys the cache on the canonical
 * instrument_key; the spelling is intentionally the same one the legacy
 * snapshot endpoints already return so batch/single/WS all collapse to one key.
 *
 * In-flight dedup: a symbol already being fetched in the current window is not
 * re-requested — the pending promise is returned instead. Unknown/unresolvable
 * symbols (dropped from the batch response) resolve to `null`, never throw, so
 * consumers surface `quote === undefined` without crashing.
 */
import type { QueryClient } from '@tanstack/react-query';
import { getSnapshotStocks, getSnapshotIndexes } from './snapshotApi';
import { normalizeIndexKey } from '@/lib/marketUtils';
import { queryKeys } from '@/lib/queryKeys';

/** The canonical quote row — the raw snapshot shape returned by the batch
 *  snapshot endpoints (and, shape-identically, the single-symbol endpoint). */
export interface QuoteRow {
  symbol: string;
  name?: string;
  price?: number | null;
  change?: number | null;
  change_percent?: number | null;
  previous_close?: number | null;
  open?: number | null;
  high?: number | null;
  low?: number | null;
  volume?: number | null;
  last_minute_close?: number | null;
  regular_close?: number | null;
  regular_trading_change?: number | null;
  early_trading_change?: number | null;
  early_trading_change_percent?: number | null;
  late_trading_change?: number | null;
  late_trading_change_percent?: number | null;
  [key: string]: unknown;
}

/** Coalescing window. Kept short so the first paint isn't visibly delayed while
 *  still wide enough for co-mounted widgets to batch into one request. */
export const BATCH_WINDOW_MS = 50;

/** Cache-key spelling for a stock: trimmed + uppercased. */
export function stockKey(symbol: string): string {
  return String(symbol ?? '').trim().toUpperCase();
}

/** Cache-key spelling for an index: trimmed, leading '^' stripped, uppercased. */
export const indexKey = normalizeIndexKey;

/** Normalize a symbol to its ['quote', KEY] cache-key spelling. */
export function quoteKey(symbol: string, isIndex = false): string {
  return isIndex ? indexKey(symbol) : stockKey(symbol);
}

interface Deferred {
  resolve: (row: QuoteRow | null) => void;
  reject: (err: unknown) => void;
}

/**
 * One batcher instance per QueryClient (see getQuoteBatcher). Bound to a client
 * so the fan-out writes into that client's cache — this also keeps tests fully
 * isolated, since each test spins up a fresh QueryClient.
 */
export class QuoteBatcher {
  private readonly queryClient: QueryClient;
  private pendingStocks = new Map<string, Deferred>();
  private pendingIndexes = new Map<string, Deferred>();
  // flight key is prefixed ('s:'/'i:') so a stock and an index sharing a spelling
  // (e.g. a ticker equal to an index alias) never collide.
  private readonly inFlight = new Map<string, Promise<QuoteRow | null>>();
  private timer: ReturnType<typeof setTimeout> | null = null;

  constructor(queryClient: QueryClient) {
    this.queryClient = queryClient;
  }

  /** Request a single symbol's quote. Coalesces into the next batch flush. */
  request(symbol: string, opts: { isIndex?: boolean } = {}): Promise<QuoteRow | null> {
    const isIndex = !!opts.isIndex;
    const key = quoteKey(symbol, isIndex);
    const flightKey = (isIndex ? 'i:' : 's:') + key;

    const existing = this.inFlight.get(flightKey);
    if (existing) return existing;

    const promise = new Promise<QuoteRow | null>((resolve, reject) => {
      const pool = isIndex ? this.pendingIndexes : this.pendingStocks;
      pool.set(key, { resolve, reject });
    });
    this.inFlight.set(flightKey, promise);
    this.schedule();
    return promise;
  }

  private schedule(): void {
    if (this.timer != null) return;
    this.timer = setTimeout(() => {
      this.timer = null;
      void this.flush();
    }, BATCH_WINDOW_MS);
  }

  private async flush(): Promise<void> {
    const stocks = this.pendingStocks;
    const indexes = this.pendingIndexes;
    this.pendingStocks = new Map();
    this.pendingIndexes = new Map();
    await Promise.all([
      this.flushGroup(stocks, false),
      this.flushGroup(indexes, true),
    ]);
  }

  private async flushGroup(pool: Map<string, Deferred>, isIndex: boolean): Promise<void> {
    const keys = [...pool.keys()];
    if (keys.length === 0) return;

    const byKey = new Map<string, QuoteRow>();
    try {
      const resp = isIndex
        ? await getSnapshotIndexes(keys)
        : await getSnapshotStocks(keys);
      const list = (resp?.snapshots || resp?.results || resp?.data || []) as unknown as QuoteRow[];
      if (Array.isArray(list)) {
        for (const row of list) {
          if (row && row.symbol != null) byKey.set(quoteKey(String(row.symbol), isIndex), row);
        }
      }
    } catch {
      // getSnapshot* already swallow network errors and return {}; this catch is
      // purely defensive so a thrown error never rejects a consumer's query.
      // Every pending symbol falls through to `null` below.
    }

    for (const [key, deferred] of pool) {
      const flightKey = (isIndex ? 'i:' : 's:') + key;
      // Missing / unknown symbol → null (not undefined): React Query rejects an
      // `undefined` queryFn result, and the hooks map null → `quote: undefined`.
      const value: QuoteRow | null = byKey.get(key) ?? null;
      this.queryClient.setQueryData(queryKeys.quote.detail(key), value);
      this.inFlight.delete(flightKey);
      deferred.resolve(value);
    }
  }
}

const batchers = new WeakMap<QueryClient, QuoteBatcher>();

/** Get (or lazily create) the batcher bound to a QueryClient. */
export function getQuoteBatcher(queryClient: QueryClient): QuoteBatcher {
  let batcher = batchers.get(queryClient);
  if (!batcher) {
    batcher = new QuoteBatcher(queryClient);
    batchers.set(queryClient, batcher);
  }
  return batcher;
}
