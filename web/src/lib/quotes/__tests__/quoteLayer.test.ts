/**
 * Quote layer core contracts: coalescing batcher (one HTTP call per window,
 * per-symbol fan-out, in-flight dedup, null-for-missing) and the WS
 * write-through guard (merge live fields, never seed unwatched symbols).
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { Mock } from 'vitest';
import { QueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';

vi.mock('@/lib/quotes/snapshotApi', () => ({
  getSnapshotStocks: vi.fn(),
  getSnapshotIndexes: vi.fn(),
}));

import { getSnapshotStocks, getSnapshotIndexes } from '@/lib/quotes/snapshotApi';
import { getQuoteBatcher, QuoteBatcher } from '../quoteBatcher';
import { snapshotToStockPrice } from '../quoteAdapters';
import { writeQuoteFromWs } from '../quoteWriteThrough';

const mockGetSnapshotStocks = getSnapshotStocks as Mock;
const mockGetSnapshotIndexes = getSnapshotIndexes as Mock;

describe('QuoteBatcher', () => {
  let client: QueryClient;
  let batcher: QuoteBatcher;

  beforeEach(() => {
    vi.clearAllMocks();
    client = new QueryClient();
    batcher = getQuoteBatcher(client);
  });

  afterEach(() => {
    client.clear();
  });

  it('coalesces same-window requests into one batched call and fans out per symbol', async () => {
    mockGetSnapshotStocks.mockResolvedValue({
      snapshots: [
        { symbol: 'AAPL', price: 100, change: 1 },
        { symbol: 'MSFT', price: 200, change: -2 },
      ],
    });

    const [a, m] = await Promise.all([batcher.request('AAPL'), batcher.request('msft ')]);

    expect(mockGetSnapshotStocks).toHaveBeenCalledTimes(1);
    expect(mockGetSnapshotStocks).toHaveBeenCalledWith(['AAPL', 'MSFT']);
    expect(a?.price).toBe(100);
    expect(m?.price).toBe(200);
    // Fan-out: both rows land in the shared per-symbol cache entries.
    expect(client.getQueryData(queryKeys.quote.detail('AAPL'))).toMatchObject({ price: 100 });
    expect(client.getQueryData(queryKeys.quote.detail('MSFT'))).toMatchObject({ price: 200 });
  });

  it('dedupes concurrent requests for the same symbol into one in-flight promise', async () => {
    mockGetSnapshotStocks.mockResolvedValue({ snapshots: [{ symbol: 'AAPL', price: 100 }] });

    const p1 = batcher.request('AAPL');
    const p2 = batcher.request('AAPL');
    expect(p2).toBe(p1);
    await p1;
    expect(mockGetSnapshotStocks).toHaveBeenCalledTimes(1);
  });

  it('resolves dropped/unknown symbols to null, never throws', async () => {
    mockGetSnapshotStocks.mockResolvedValue({ snapshots: [{ symbol: 'AAPL', price: 100 }] });

    const [known, unknown] = await Promise.all([
      batcher.request('AAPL'),
      batcher.request('ZZZFAKE'),
    ]);
    expect(known?.price).toBe(100);
    expect(unknown).toBeNull();
    expect(client.getQueryData(queryKeys.quote.detail('ZZZFAKE'))).toBeNull();
  });

  it('routes indexes through the index endpoint and strips the caret spelling', async () => {
    mockGetSnapshotIndexes.mockResolvedValue({ snapshots: [{ symbol: 'GSPC', price: 5000 }] });

    const [a, b] = await Promise.all([
      batcher.request('^GSPC', { isIndex: true }),
      batcher.request('GSPC', { isIndex: true }),
    ]);
    expect(mockGetSnapshotIndexes).toHaveBeenCalledTimes(1);
    expect(mockGetSnapshotIndexes).toHaveBeenCalledWith(['GSPC']);
    expect(mockGetSnapshotStocks).not.toHaveBeenCalled();
    expect(a).toBe(b);
  });

  it('resolves every pending symbol to null when the batch call throws', async () => {
    mockGetSnapshotStocks.mockRejectedValue(new Error('network'));
    await expect(batcher.request('AAPL')).resolves.toBeNull();
  });

  it('binds one batcher per QueryClient', () => {
    expect(getQuoteBatcher(client)).toBe(batcher);
    const other = new QueryClient();
    expect(getQuoteBatcher(other)).not.toBe(batcher);
  });
});

describe('writeQuoteFromWs', () => {
  let client: QueryClient;

  beforeEach(() => {
    client = new QueryClient();
  });

  it('merges live fields into an existing entry, preserving REST-authored fields', () => {
    const key = queryKeys.quote.detail('AAPL');
    client.setQueryData(key, {
      symbol: 'AAPL', price: 100, change: 1, change_percent: 1.0,
      previous_close: 99, name: 'Apple Inc',
    });

    writeQuoteFromWs(client, 'AAPL', { price: 101.5, change: 2.5, changePercent: 2.53 });

    expect(client.getQueryData(key)).toMatchObject({
      price: 101.5,
      change: 2.5,
      change_percent: 2.53,
      previous_close: 99,
      name: 'Apple Inc',
    });
  });

  it('never seeds an entry nobody is watching', () => {
    writeQuoteFromWs(client, 'TSLA', { price: 300, change: 1, changePercent: 0.3 });
    expect(client.getQueryData(queryKeys.quote.detail('TSLA'))).toBeUndefined();
  });
});

describe('snapshotToStockPrice', () => {
  it('rounds and maps an available quote', () => {
    expect(
      snapshotToStockPrice('AAPL', {
        symbol: 'AAPL', price: 100.456, change: -1.234, change_percent: -1.216,
        previous_close: 101.69,
      }),
    ).toMatchObject({
      symbol: 'AAPL',
      price: 100.46,
      change: -1.23,
      changePercent: -1.22,
      isPositive: false,
      quoteAvailable: true,
      previousClose: 101.69,
    });
  });

  it('marks a missing quote unavailable with zeroed display fields', () => {
    expect(snapshotToStockPrice('MSFT', null)).toEqual({
      symbol: 'MSFT', price: 0, change: 0, changePercent: 0,
      isPositive: true, quoteAvailable: false,
    });
  });
});
