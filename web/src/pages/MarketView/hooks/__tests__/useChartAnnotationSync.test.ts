import { renderHook, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { Mock } from 'vitest';

vi.mock('@/api/client', () => ({
  api: {
    get: vi.fn().mockResolvedValue({ data: { charts: [] } }),
    post: vi.fn(),
    put: vi.fn(),
    delete: vi.fn(),
    patch: vi.fn(),
  },
}));

vi.mock('../../stores/chartAnnotationStore', () => ({
  chartAnnotationStore: {
    getMutationSeq: vi.fn(() => 0),
    setChartsForSymbol: vi.fn(),
  },
}));

import { api } from '@/api/client';

import { chartAnnotationStore } from '../../stores/chartAnnotationStore';
import { useChartAnnotationSync } from '../useChartAnnotationSync';

const mockGet = api.get as Mock;
const mockGetSeq = chartAnnotationStore.getMutationSeq as Mock;
const mockSetCharts = chartAnnotationStore.setChartsForSymbol as Mock;

const WS = '11111111-1111-1111-1111-111111111111';

beforeEach(() => {
  mockGet.mockReset().mockResolvedValue({ data: { charts: [] } });
  mockGetSeq.mockReset().mockReturnValue(0);
  mockSetCharts.mockReset();
});

afterEach(() => {
  vi.clearAllMocks();
});

describe('useChartAnnotationSync', () => {
  it('does nothing when workspaceId or symbol is missing', () => {
    renderHook(() => useChartAnnotationSync(null, 'NVDA'));
    renderHook(() => useChartAnnotationSync(WS, null));
    renderHook(() => useChartAnnotationSync(undefined, undefined));

    expect(mockGet).not.toHaveBeenCalled();
    expect(mockSetCharts).not.toHaveBeenCalled();
  });

  it('fetches the chart-annotations endpoint with the symbol param on mount', async () => {
    renderHook(() => useChartAnnotationSync(WS, 'NVDA'));

    await waitFor(() => expect(mockGet).toHaveBeenCalledTimes(1));
    expect(mockGet).toHaveBeenCalledWith(
      `/api/v1/workspaces/${WS}/chart-annotations`,
      expect.objectContaining({
        params: { symbol: 'NVDA' },
        signal: expect.any(AbortSignal),
      }),
    );
  });

  it('reconciles fetched charts into the store with the captured race-guard seq', async () => {
    // Capture the seq BEFORE the fetch — bump the source value afterward to
    // prove the hook passes the pre-fetch value, not a later one.
    mockGetSeq.mockReturnValue(7);
    const charts = [
      { chart_id: 'NVDA:1day', symbol: 'NVDA', timeframe: '1day', annotations: [] },
    ];
    mockGet.mockResolvedValueOnce({ data: { charts } });

    renderHook(() => useChartAnnotationSync(WS, 'NVDA'));

    await waitFor(() => expect(mockSetCharts).toHaveBeenCalledTimes(1));
    expect(mockSetCharts).toHaveBeenCalledWith(WS, 'NVDA', charts, 7);
    // The seq is read once, before the (awaited) fetch resolves.
    expect(mockGetSeq).toHaveBeenCalledTimes(1);
  });

  it('captures the mutation seq before issuing the fetch (race-guard ordering)', () => {
    const order: string[] = [];
    mockGetSeq.mockImplementation(() => {
      order.push('seq');
      return 0;
    });
    mockGet.mockImplementation(() => {
      order.push('get');
      return Promise.resolve({ data: { charts: [] } });
    });

    renderHook(() => useChartAnnotationSync(WS, 'NVDA'));

    expect(order).toEqual(['seq', 'get']);
  });

  it('passes an empty array to the store when the response has no charts', async () => {
    mockGet.mockResolvedValueOnce({ data: {} });

    renderHook(() => useChartAnnotationSync(WS, 'NVDA'));

    await waitFor(() => expect(mockSetCharts).toHaveBeenCalledTimes(1));
    expect(mockSetCharts).toHaveBeenCalledWith(WS, 'NVDA', [], 0);
  });

  it('aborts the in-flight request on unmount', () => {
    let capturedSignal: AbortSignal | undefined;
    mockGet.mockImplementation((_url: string, config: { signal?: AbortSignal }) => {
      capturedSignal = config?.signal;
      return new Promise(() => {}); // never resolves
    });

    const { unmount } = renderHook(() => useChartAnnotationSync(WS, 'NVDA'));
    expect(capturedSignal?.aborted).toBe(false);

    unmount();
    expect(capturedSignal?.aborted).toBe(true);
  });

  it('aborts and re-fetches when a dependency changes', async () => {
    const signals: AbortSignal[] = [];
    mockGet.mockImplementation((_url: string, config: { signal?: AbortSignal }) => {
      if (config?.signal) signals.push(config.signal);
      return new Promise(() => {});
    });

    const { rerender } = renderHook(
      ({ sym }: { sym: string }) => useChartAnnotationSync(WS, sym),
      { initialProps: { sym: 'NVDA' } },
    );
    expect(mockGet).toHaveBeenCalledTimes(1);

    rerender({ sym: 'AAPL' });
    expect(mockGet).toHaveBeenCalledTimes(2);
    // The first request's signal was aborted by the cleanup; the second is live.
    expect(signals[0].aborted).toBe(true);
    expect(signals[1].aborted).toBe(false);
  });

  it('does not write to the store after the effect is cancelled mid-fetch', async () => {
    let resolveGet: (value: { data: { charts: unknown[] } }) => void = () => {};
    mockGet.mockImplementation(
      () => new Promise((res) => { resolveGet = res; }),
    );

    const { unmount } = renderHook(() => useChartAnnotationSync(WS, 'NVDA'));
    // Unmount before the request resolves — the `cancelled` guard must hold.
    unmount();
    resolveGet({ data: { charts: [] } });

    // Give the awaited continuation a tick to run.
    await Promise.resolve();
    await Promise.resolve();
    expect(mockSetCharts).not.toHaveBeenCalled();
  });

  it('does not write to the store when the request is aborted (CanceledError)', async () => {
    mockGet.mockRejectedValueOnce(
      Object.assign(new Error('canceled'), { name: 'CanceledError' }),
    );

    renderHook(() => useChartAnnotationSync(WS, 'NVDA'));

    await waitFor(() => expect(mockGet).toHaveBeenCalledTimes(1));
    // Flush the rejected-promise continuation.
    await Promise.resolve();
    await Promise.resolve();
    expect(mockSetCharts).not.toHaveBeenCalled();
  });
});
