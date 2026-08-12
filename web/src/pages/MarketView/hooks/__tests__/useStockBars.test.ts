/**
 * Contract tests for ``useStockBars`` — the React Query wrapper around
 * ``fetchStockData`` used by lightweight bar consumers (e.g. the chat
 * annotation preview). We mock ``fetchStockData`` so we exercise the hook's
 * own behaviour: query-key derivation (symbol upper-casing), the enabled gate,
 * the soft-error → throw translation, and the loading/error projection.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { renderHook, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import React from 'react';

import type { ChartDataPoint } from '@/types/market';
import { queryKeys } from '@/lib/queryKeys';

// Mock the data dependency. Each test sets the resolved/rejected value.
const fetchStockData = vi.fn();
vi.mock('../../utils/api', () => ({
  fetchStockData: (...args: unknown[]) => fetchStockData(...args),
}));

import { useStockBars } from '../useStockBars';

const DAY = 86_400;
const T0 = 1_700_000_000;
const BARS: ChartDataPoint[] = Array.from({ length: 3 }, (_, i) => ({
  time: T0 + i * DAY,
  open: 100 + i,
  high: 104 + i,
  low: 98 + i,
  close: 101 + i,
  volume: 1_000 + i,
}));

let qc: QueryClient;

function wrapper({ children }: { children: React.ReactNode }) {
  return React.createElement(QueryClientProvider, { client: qc }, children);
}

function renderBars(
  symbol: string | null | undefined,
  interval: string,
  opts?: { enabled?: boolean },
) {
  return renderHook(() => useStockBars(symbol, interval, opts), { wrapper });
}

describe('useStockBars', () => {
  beforeEach(() => {
    // The hook hardcodes ``retry: 1`` per-query (overriding any client default),
    // so ``retryDelay: 0`` keeps the one retry instant + deterministic instead
    // of waiting out the default exponential backoff.
    qc = new QueryClient({
      defaultOptions: { queries: { retryDelay: 0, gcTime: 0 } },
    });
    fetchStockData.mockReset();
  });
  afterEach(() => {
    qc.clear();
    vi.clearAllMocks();
  });

  it('fetches and returns the bars on success', async () => {
    fetchStockData.mockResolvedValue({ data: BARS });
    const { result } = renderBars('AAPL', '1day');

    // Starts loading, no bars yet.
    expect(result.current.isLoading).toBe(true);
    expect(result.current.bars).toEqual([]);

    await waitFor(() => expect(result.current.isLoading).toBe(false));
    expect(result.current.bars).toEqual(BARS);
    expect(result.current.isError).toBe(false);
  });

  it('upper-cases the symbol for both the fetch and the query key', async () => {
    fetchStockData.mockResolvedValue({ data: BARS });
    renderBars('aapl', '1day');

    await waitFor(() => expect(fetchStockData).toHaveBeenCalledTimes(1));
    // First positional arg to fetchStockData is the symbol.
    expect(fetchStockData.mock.calls[0][0]).toBe('AAPL');
    expect(fetchStockData.mock.calls[0][1]).toBe('1day');
    // The cache entry is keyed by the upper-cased symbol.
    const cached = qc.getQueryData(queryKeys.marketData.bars('AAPL', '1day'));
    expect(cached).toEqual(BARS);
  });

  it('passes an AbortSignal through to fetchStockData', async () => {
    fetchStockData.mockResolvedValue({ data: BARS });
    renderBars('AAPL', '1day');

    await waitFor(() => expect(fetchStockData).toHaveBeenCalled());
    const optsArg = fetchStockData.mock.calls[0][4] as { signal?: AbortSignal };
    expect(optsArg.signal).toBeInstanceOf(AbortSignal);
  });

  it('passes a from/to window for ranged intervals (e.g. 1min)', async () => {
    fetchStockData.mockResolvedValue({ data: BARS });
    renderBars('AAPL', '1min');

    await waitFor(() => expect(fetchStockData).toHaveBeenCalled());
    const [, , from, to] = fetchStockData.mock.calls[0] as [string, string, string, string];
    // INITIAL_LOAD_DAYS['1min'] = 7 (> 0) → ranged → YYYY-MM-DD strings, from < to.
    expect(from).toMatch(/^\d{4}-\d{2}-\d{2}$/);
    expect(to).toMatch(/^\d{4}-\d{2}-\d{2}$/);
    expect(from <= to).toBe(true);
  });

  it('omits the from/to window for full-history intervals (e.g. 1day → 0 days)', async () => {
    fetchStockData.mockResolvedValue({ data: BARS });
    renderBars('AAPL', '1day');

    await waitFor(() => expect(fetchStockData).toHaveBeenCalled());
    const [, , from, to] = fetchStockData.mock.calls[0] as [string, string, string?, string?];
    // INITIAL_LOAD_DAYS['1day'] = 0 → previewRange returns {} → undefined bounds.
    expect(from).toBeUndefined();
    expect(to).toBeUndefined();
  });

  it('does not fetch when disabled', () => {
    fetchStockData.mockResolvedValue({ data: BARS });
    const { result } = renderBars('AAPL', '1day', { enabled: false });

    expect(fetchStockData).not.toHaveBeenCalled();
    // Gated query is idle, not loading; consumers see empty bars.
    expect(result.current.isLoading).toBe(false);
    expect(result.current.bars).toEqual([]);
  });

  it('does not fetch when the symbol is empty/nullish', () => {
    fetchStockData.mockResolvedValue({ data: BARS });
    const { result } = renderBars(null, '1day');

    expect(fetchStockData).not.toHaveBeenCalled();
    expect(result.current.isLoading).toBe(false);
    expect(result.current.bars).toEqual([]);
  });

  it('surfaces a soft error (no data + error) as a query error', async () => {
    fetchStockData.mockResolvedValue({ data: [], error: 'No data available' });
    const { result } = renderBars('AAPL', '1day');

    await waitFor(() => expect(result.current.isError).toBe(true));
    expect(result.current.bars).toEqual([]);
    expect(result.current.isLoading).toBe(false);
  });

  it('does NOT error when data is present even if an error string is set', async () => {
    // Soft error is only promoted when there is no data; a partial result with
    // bars wins and is cached.
    fetchStockData.mockResolvedValue({ data: BARS, error: 'stale' });
    const { result } = renderBars('AAPL', '1day');

    await waitFor(() => expect(result.current.bars).toEqual(BARS));
    expect(result.current.isError).toBe(false);
  });

  it('marks isError when fetchStockData rejects', async () => {
    fetchStockData.mockRejectedValue(new Error('network down'));
    const { result } = renderBars('AAPL', '1day');

    await waitFor(() => expect(result.current.isError).toBe(true));
    expect(result.current.bars).toEqual([]);
  });

  it('dedupes repeated mounts for the same symbol/interval to one request', async () => {
    fetchStockData.mockResolvedValue({ data: BARS });
    const { result: a } = renderBars('AAPL', '1day');
    const { result: b } = renderBars('AAPL', '1day');

    await waitFor(() => expect(a.current.bars).toEqual(BARS));
    await waitFor(() => expect(b.current.bars).toEqual(BARS));
    // Shared cache key → a single network call backs both consumers.
    expect(fetchStockData).toHaveBeenCalledTimes(1);
  });

  it('keys separately by interval (different intervals → separate fetches)', async () => {
    fetchStockData.mockResolvedValue({ data: BARS });
    renderBars('AAPL', '1day');
    renderBars('AAPL', '1min');

    await waitFor(() => expect(fetchStockData).toHaveBeenCalledTimes(2));
    const intervals = fetchStockData.mock.calls.map((c) => c[1]);
    expect(intervals).toEqual(expect.arrayContaining(['1day', '1min']));
  });
});
