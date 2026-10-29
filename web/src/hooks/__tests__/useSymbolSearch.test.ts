import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, renderHook } from '@testing-library/react';

const { searchStocks } = vi.hoisted(() => ({ searchStocks: vi.fn() }));
vi.mock('@/lib/marketUtils', async (importOriginal) => {
  const orig = await importOriginal<Record<string, unknown>>();
  return { ...orig, searchStocks };
});

import { SYMBOL_SEARCH_DEBOUNCE_MS, useSymbolSearch } from '../useSymbolSearch';

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((res, rej) => { resolve = res; reject = rej; });
  return { promise, resolve, reject };
}

/** Let the rest period elapse and any resolved response land. */
const rest = () => act(async () => { await vi.advanceTimersByTimeAsync(SYMBOL_SEARCH_DEBOUNCE_MS); });

beforeEach(() => {
  vi.useFakeTimers();
  vi.clearAllMocks();
});

afterEach(() => {
  vi.useRealTimers();
});

describe('useSymbolSearch', () => {
  it('reads a body without a results array as no hits', async () => {
    searchStocks.mockResolvedValue({ detail: 'upstream changed shape' });
    const { result } = renderHook(() => useSymbolSearch('goog'));
    await rest();
    expect(result.current.hits).toEqual([]);
    expect(result.current.loading).toBe(false);
  });

  it('settles a failed request as no hits, and stops loading', async () => {
    searchStocks.mockRejectedValue(new Error('502'));
    const { result } = renderHook(() => useSymbolSearch('goog'));
    await rest();
    expect(result.current.hits).toEqual([]);
    expect(result.current.loading).toBe(false);
  });

  it('keeps the newer query\'s hits when the older response lands second', async () => {
    const first = deferred<unknown>();
    const second = deferred<unknown>();
    searchStocks.mockReturnValueOnce(first.promise).mockReturnValueOnce(second.promise);
    const { result, rerender } = renderHook(({ q }: { q: string }) => useSymbolSearch(q), { initialProps: { q: 'goo' } });
    await rest();
    rerender({ q: 'goog' });
    await rest();
    expect(searchStocks).toHaveBeenCalledTimes(2);
    expect(searchStocks).toHaveBeenLastCalledWith('goog', 8, { signal: expect.any(AbortSignal) });

    await act(async () => { second.resolve({ results: [{ symbol: 'GOOGL', name: 'Alphabet Inc.' }] }); });
    expect(result.current.hits.map((h) => h.symbol)).toEqual(['GOOGL']);
    await act(async () => { first.resolve({ results: [{ symbol: 'GOO', name: 'Stale' }] }); });
    expect(result.current.hits.map((h) => h.symbol)).toEqual(['GOOGL']);
  });

  it('is loading from the first keystroke until the matching response lands', async () => {
    const pending = deferred<unknown>();
    searchStocks.mockReturnValue(pending.promise);
    // The field mounts empty; the keystroke is the rerender.
    const { result, rerender } = renderHook(({ q }: { q: string }) => useSymbolSearch(q), { initialProps: { q: '' } });
    expect(result.current.loading).toBe(false);
    rerender({ q: 'goog' });
    expect(result.current.loading).toBe(true);
    expect(searchStocks).not.toHaveBeenCalled();

    await act(async () => { await vi.advanceTimersByTimeAsync(SYMBOL_SEARCH_DEBOUNCE_MS - 1); });
    expect(result.current.loading).toBe(true);
    expect(searchStocks).not.toHaveBeenCalled();

    await act(async () => { await vi.advanceTimersByTimeAsync(1); });
    expect(searchStocks).toHaveBeenCalledWith('goog', 8, { signal: expect.any(AbortSignal) });
    expect(result.current.loading).toBe(true);

    await act(async () => { pending.resolve({ results: [{ symbol: 'GOOGL' }] }); });
    expect(result.current.loading).toBe(false);
    expect(result.current.hits.map((h) => h.symbol)).toEqual(['GOOGL']);
  });

  it('aborts the older request when the query moves on, and on unmount', async () => {
    searchStocks.mockReturnValue(new Promise(() => {}));
    const { rerender, unmount } = renderHook(({ q }: { q: string }) => useSymbolSearch(q), { initialProps: { q: 'goo' } });
    await rest();
    const first = searchStocks.mock.calls[0][2].signal as AbortSignal;
    expect(first.aborted).toBe(false);

    rerender({ q: 'goog' });
    await rest();
    expect(first.aborted).toBe(true);
    const second = searchStocks.mock.calls[1][2].signal as AbortSignal;
    expect(second.aborted).toBe(false);

    unmount();
    expect(second.aborted).toBe(true);
  });

  it('lets an aborted request settle without touching state', async () => {
    const first = deferred<unknown>();
    const second = deferred<unknown>();
    searchStocks.mockReturnValueOnce(first.promise).mockReturnValueOnce(second.promise);
    const { result, rerender } = renderHook(({ q }: { q: string }) => useSymbolSearch(q), { initialProps: { q: 'goo' } });
    await rest();
    rerender({ q: 'goog' });
    await rest();
    // The aborted request rejects the way the API client does; the hook stays loading
    // for the live query rather than settling it as empty.
    await act(async () => { first.reject(Object.assign(new Error('canceled'), { name: 'CanceledError' })); });
    expect(result.current.loading).toBe(true);
    expect(result.current.hits).toEqual([]);
    await act(async () => { second.resolve({ results: [{ symbol: 'GOOGL' }] }); });
    expect(result.current.loading).toBe(false);
    expect(result.current.hits.map((h) => h.symbol)).toEqual(['GOOGL']);
  });
});
