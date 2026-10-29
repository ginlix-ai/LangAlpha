import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { useLiveBars } from '../useLiveBars';
import type { ChartBar } from '../marketProtocol';
import { fetchBarsDelta } from '../chartDataLoaders';
import type { BarsDeltaResult } from '../chartDataLoaders';
import { DELTA_POLL_CADENCE_MS } from '../chartConstants';

// Mock only the network entry point; keep advanceWatermark / dedupeMergeByTime /
// shouldSkipPollWhileWsHealthy real so the controller's decisions are exercised.
vi.mock('../chartDataLoaders', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../chartDataLoaders')>();
  return { ...actual, fetchBarsDelta: vi.fn() };
});

const mockFetch = vi.mocked(fetchBarsDelta);
const POLL_MS = DELTA_POLL_CADENCE_MS['5min']; // 30_000 — below WS_RECONCILE_POLL_MS (60_000)
const BASE = new Date('2026-01-01T15:00:00Z').getTime();

function bar(time: number, over: Partial<ChartBar> = {}): ChartBar {
  return { time, open: 1, high: 2, low: 0.5, close: 1.5, volume: 10, ...over };
}

function delta(
  bars: ChartBar[],
  extra: {
    watermark?: number | null;
    currency?: string;
    displayDecimals?: number;
    marketPhase?: string | null;
    nextChangeAt?: number | null;
    revision?: number;
    priceTreatment?: string;
  } = {},
): BarsDeltaResult {
  return {
    bars,
    meta: {
      watermark: extra.watermark ?? null,
      complete: true,
      marketPhase: extra.marketPhase ?? null,
      nextChangeAt: extra.nextChangeAt ?? null,
      currency: extra.currency,
      displayDecimals: extra.displayDecimals,
      revision: extra.revision,
      priceTreatment: extra.priceTreatment,
    },
    source: 'protocol',
  };
}

function setup(overrides: Partial<{ symbol: string; interval: string; enabled: boolean }> = {}) {
  const dataRef = { current: [] as ChartBar[] };
  const lastWsTickRef = { current: 0 };
  const onBars = vi.fn();
  const onMeta = vi.fn();
  const onPhase = vi.fn();
  const onRebuilt = vi.fn();
  const view = renderHook(
    ({ symbol, interval, enabled }) =>
      useLiveBars(symbol, interval, { enabled, dataRef, lastWsTickRef, onBars, onMeta, onPhase, onRebuilt }),
    { initialProps: { symbol: 'AAPL', interval: '5min', enabled: true, ...overrides } },
  );
  return { dataRef, lastWsTickRef, onBars, onMeta, onPhase, onRebuilt, ...view };
}

async function tick(ms = POLL_MS) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms);
  });
}

describe('useLiveBars', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(BASE);
    mockFetch.mockReset();
  });
  afterEach(() => {
    vi.useRealTimers();
    vi.resetAllMocks();
    vi.restoreAllMocks();
  });

  it('bails without fetching while the series is empty', async () => {
    const { dataRef } = setup();
    dataRef.current = [];
    await tick();
    expect(mockFetch).not.toHaveBeenCalled();
  });

  it('does not poll while disabled', async () => {
    const { dataRef } = setup({ enabled: false });
    dataRef.current = [bar(100)];
    mockFetch.mockResolvedValue(delta([bar(100, { close: 9 })]));
    await tick();
    expect(mockFetch).not.toHaveBeenCalled();
  });

  it('appends newer bars and updates dataRef in place', async () => {
    const { dataRef, onBars } = setup();
    dataRef.current = [bar(100)];
    mockFetch.mockResolvedValue(delta([bar(200)]));
    await tick();
    expect(onBars).toHaveBeenCalledTimes(1);
    const merged = onBars.mock.calls[0][0] as ChartBar[];
    expect(merged.map((b) => b.time)).toEqual([100, 200]);
    expect(dataRef.current).toBe(merged);
  });

  it('replaces the forming head bar when its OHLCV moved', async () => {
    const { dataRef, onBars } = setup();
    dataRef.current = [bar(100, { close: 1 })];
    mockFetch.mockResolvedValue(delta([bar(100, { close: 9 })]));
    await tick();
    expect(onBars).toHaveBeenCalledTimes(1);
    expect(onBars.mock.calls[0][1]).toEqual({ headChanged: true });
    expect(dataRef.current[dataRef.current.length - 1].close).toBe(9);
  });

  it('skips the redraw when the re-served head is unchanged', async () => {
    const { dataRef, onBars } = setup();
    dataRef.current = [bar(100)];
    mockFetch.mockResolvedValue(delta([bar(100)]));
    await tick();
    expect(mockFetch).toHaveBeenCalledTimes(1);
    expect(onBars).not.toHaveBeenCalled();
  });

  it('seedMeta seeds the watermark and forwards currency to onMeta', async () => {
    const { dataRef, onMeta, result } = setup();
    act(() => {
      result.current.seedMeta({ watermark: 12345, currency: 'GBP', displayDecimals: 3 });
    });
    expect(onMeta).toHaveBeenCalledWith({ currency: 'GBP', displayDecimals: 3, watermark: 12345 });
    dataRef.current = [bar(100)];
    mockFetch.mockResolvedValue(delta([bar(100)]));
    await tick();
    expect(mockFetch).toHaveBeenLastCalledWith('AAPL', '5min', 12345);
  });

  it('forwards currency metadata from a poll to onMeta', async () => {
    const { dataRef, onMeta } = setup();
    dataRef.current = [bar(100)];
    mockFetch.mockResolvedValue(delta([bar(100, { close: 9 })], { currency: 'HKD', displayDecimals: 3 }));
    await tick();
    expect(onMeta).toHaveBeenCalledWith({ currency: 'HKD', displayDecimals: 3, watermark: null });
  });

  it('seedMeta forwards the market phase to onPhase, skipping phase-less meta', () => {
    const { onPhase, result } = setup();
    act(() => {
      result.current.seedMeta({ watermark: 1, marketPhase: 'closed' });
    });
    expect(onPhase).toHaveBeenCalledWith('closed');
    act(() => {
      result.current.seedMeta({ watermark: 2 });
    });
    expect(onPhase).toHaveBeenCalledTimes(1);
  });

  it('forwards the market phase from a poll to onPhase, even without currency', async () => {
    const { dataRef, onPhase } = setup();
    dataRef.current = [bar(100)];
    mockFetch.mockResolvedValue(delta([bar(100)], { marketPhase: 'closed' }));
    await tick();
    expect(onPhase).toHaveBeenCalledWith('closed');
  });

  it('re-labels the chart from a poll that brings no new bars', async () => {
    const onFreshness = vi.fn();
    const dataRef = { current: [bar(100)] };
    renderHook(() => useLiveBars('AAPL', '5min', {
      enabled: true, dataRef, lastWsTickRef: { current: 0 }, onBars: vi.fn(), onFreshness, onRebuilt: vi.fn(),
    }));
    const freshness = { label: 'stale' as const, measured: true, lag_s: 1800, interval: '5min' };
    const empty = delta([]);
    mockFetch.mockResolvedValue({ ...empty, meta: { ...empty.meta, freshness } });
    await tick();
    expect(onFreshness).toHaveBeenCalledWith(freshness);
  });

  it('clears the label on a symbol or interval change, before the new series seeds it', () => {
    const onFreshness = vi.fn();
    const opts = {
      enabled: true, dataRef: { current: [] as ChartBar[] }, lastWsTickRef: { current: 0 },
      onBars: vi.fn(), onFreshness, onRebuilt: vi.fn(),
    };
    const { result, rerender } = renderHook(
      ({ symbol, interval }) => useLiveBars(symbol, interval, opts),
      { initialProps: { symbol: 'AAPL', interval: '5min' } },
    );
    const freshness = { label: 'delayed' as const, measured: true, lag_s: 900, interval: '5min' };
    act(() => result.current.seedMeta({ watermark: 1, freshness }));
    expect(onFreshness).toHaveBeenLastCalledWith(freshness);

    rerender({ symbol: 'AAPL', interval: '1hour' });
    expect(onFreshness).toHaveBeenLastCalledWith(null);

    act(() => result.current.seedMeta({ watermark: 1, freshness }));
    rerender({ symbol: '600519.SH', interval: '1hour' });
    expect(onFreshness).toHaveBeenLastCalledWith(null);
  });

  describe('a series rebuilt server-side', () => {
    const SEED = { watermark: 100_000, complete: true, marketPhase: 'open', revision: 3, priceTreatment: 'split_adjusted' };

    it('is reloaded, not spliced, when a poll brings a new revision', async () => {
      const { dataRef, onBars, onRebuilt, result } = setup();
      dataRef.current = [bar(100, { close: 50 })];
      act(() => result.current.seedMeta(SEED));
      mockFetch.mockResolvedValue(delta([bar(200, { close: 100 })], { revision: 4, priceTreatment: 'split_adjusted' }));
      await tick();
      expect(onRebuilt).toHaveBeenCalledTimes(1);
      expect(onBars).not.toHaveBeenCalled();
      expect(dataRef.current.map((b) => b.time)).toEqual([100]);
    });

    it('is reloaded when the adjustment basis changes, though the initial load named no revision', async () => {
      const { dataRef, onBars, onRebuilt, result } = setup();
      dataRef.current = [bar(100)];
      act(() => result.current.seedMeta({ ...SEED, revision: undefined }));
      mockFetch.mockResolvedValue(delta([bar(200)], { revision: 0, priceTreatment: 'raw' }));
      await tick();
      expect(onRebuilt).toHaveBeenCalledTimes(1);
      expect(onBars).not.toHaveBeenCalled();
    });

    it('takes its revision from the first poll when the initial load named none', async () => {
      const { dataRef, onBars, onRebuilt, result } = setup();
      dataRef.current = [bar(100)];
      act(() => result.current.seedMeta({ ...SEED, revision: undefined }));
      mockFetch.mockResolvedValue(delta([bar(200)], { revision: 3, priceTreatment: 'split_adjusted' }));
      await tick();
      expect(onRebuilt).not.toHaveBeenCalled();
      expect(onBars).toHaveBeenCalledTimes(1);

      mockFetch.mockResolvedValue(delta([bar(300)], { revision: 4, priceTreatment: 'split_adjusted' }));
      await tick();
      expect(onRebuilt).toHaveBeenCalledTimes(1);
      expect(dataRef.current.map((b) => b.time)).toEqual([100, 200]);
    });

    it('holds the polls until the reload seeds, then polls from the rebuilt series', async () => {
      const { dataRef, onBars, onRebuilt, result } = setup();
      dataRef.current = [bar(100)];
      act(() => result.current.seedMeta(SEED));
      mockFetch.mockResolvedValue(delta([bar(200)], { revision: 4, priceTreatment: 'split_adjusted' }));
      await tick();
      expect(onRebuilt).toHaveBeenCalledTimes(1);
      expect(mockFetch).toHaveBeenLastCalledWith('AAPL', '5min', 100_000);

      await tick();
      expect(mockFetch).toHaveBeenCalledTimes(1);

      // The reload lands: the rebuilt history and its own watermark.
      dataRef.current = [bar(100, { close: 2 }), bar(200, { close: 2 })];
      act(() => result.current.seedMeta({ ...SEED, watermark: 200_000, revision: 4 }));
      mockFetch.mockResolvedValue(delta([bar(300)], { revision: 4, priceTreatment: 'split_adjusted' }));
      await tick();
      expect(mockFetch).toHaveBeenLastCalledWith('AAPL', '5min', 200_000);
      expect(onRebuilt).toHaveBeenCalledTimes(1);
      expect(dataRef.current.map((b) => b.time)).toEqual([100, 200, 300]);
      expect(onBars).toHaveBeenCalledTimes(1);
    });

    it('holds rather than reloading in a loop or merging when the loader still reports the old basis', async () => {
      const { dataRef, onBars, onRebuilt, result } = setup();
      dataRef.current = [bar(100)];
      act(() => result.current.seedMeta({ ...SEED, revision: undefined }));
      mockFetch.mockResolvedValue(delta([bar(200)], { revision: 0, priceTreatment: 'raw' }));
      await tick();
      expect(onRebuilt).toHaveBeenCalledTimes(1);

      // A reload whose route still says split_adjusted: the bars on screen are
      // on that basis, so the raw tail must not be merged onto them.
      act(() => result.current.seedMeta({ ...SEED, revision: undefined }));
      await tick();
      await tick();
      expect(onRebuilt).toHaveBeenCalledTimes(1);
      expect(onBars).not.toHaveBeenCalled();
      expect(dataRef.current.map((b) => b.time)).toEqual([100]);

      // The poll moves on to yet another basis: that one is worth a reload.
      mockFetch.mockResolvedValue(delta([bar(300)], { revision: 1, priceTreatment: 'raw' }));
      await tick();
      expect(onRebuilt).toHaveBeenCalledTimes(2);
    });

    it('asks for the reload again on the cadence when it failed, rather than holding for good', async () => {
      const { dataRef, onRebuilt, result } = setup();
      dataRef.current = [bar(100)];
      act(() => result.current.seedMeta(SEED));
      mockFetch.mockResolvedValue(delta([bar(200)], { revision: 4, priceTreatment: 'split_adjusted' }));
      await tick();
      expect(onRebuilt).toHaveBeenCalledTimes(1);

      // The reload clears the chart; while it is in flight the polls hold.
      dataRef.current = [];
      await tick();
      expect(onRebuilt).toHaveBeenCalledTimes(1);

      // It comes back with an error (a 503): the next tick asks again.
      act(() => result.current.loadFailed());
      await tick();
      expect(onRebuilt).toHaveBeenCalledTimes(2);
      await tick();
      expect(onRebuilt).toHaveBeenCalledTimes(2);

      // The retry lands, and the polls resume from the rebuilt series.
      dataRef.current = [bar(100), bar(200)];
      act(() => result.current.seedMeta({ ...SEED, watermark: 200_000, revision: 4 }));
      await tick();
      expect(mockFetch).toHaveBeenCalledTimes(2);
      expect(mockFetch).toHaveBeenLastCalledWith('AAPL', '5min', 200_000);
      expect(onRebuilt).toHaveBeenCalledTimes(2);
    });

    it('starts over on a symbol change, so the next symbol\'s revision is no rebuild', async () => {
      const { dataRef, onRebuilt, result, rerender } = setup();
      dataRef.current = [bar(100)];
      act(() => result.current.seedMeta(SEED));
      rerender({ symbol: 'MSFT', interval: '5min', enabled: true });
      mockFetch.mockResolvedValue(delta([bar(200)], { revision: 0, priceTreatment: 'raw' }));
      await tick();
      expect(onRebuilt).not.toHaveBeenCalled();
    });
  });

  it('resets the watermark when the symbol changes', async () => {
    const { dataRef, result, rerender } = setup();
    act(() => {
      result.current.seedMeta({ watermark: 999 });
    });
    rerender({ symbol: 'MSFT', interval: '5min', enabled: true });
    dataRef.current = [bar(100)];
    mockFetch.mockResolvedValue(delta([bar(100)]));
    await tick();
    expect(mockFetch).toHaveBeenLastCalledWith('MSFT', '5min', null);
  });

  it('swallows aborts but logs other poll errors', async () => {
    const debugSpy = vi.spyOn(console, 'debug').mockImplementation(() => {});
    const { dataRef } = setup();
    dataRef.current = [bar(100)];
    mockFetch.mockRejectedValueOnce(Object.assign(new Error('aborted'), { name: 'AbortError' }));
    await tick();
    expect(debugSpy).not.toHaveBeenCalled();
    mockFetch.mockRejectedValueOnce(new Error('boom'));
    await tick();
    expect(debugSpy).toHaveBeenCalledTimes(1);
  });

  it('skips the poll while WS is healthy and the reconcile is not due', async () => {
    const { dataRef, lastWsTickRef } = setup();
    dataRef.current = [bar(100)];
    mockFetch.mockResolvedValue(delta([bar(100)]));
    await tick(); // first poll runs (reconcile due on mount) and stamps lastReconcile
    expect(mockFetch).toHaveBeenCalledTimes(1);
    // A very recent WS tick keeps the feed healthy; the 60s reconcile window has
    // not elapsed since the last poll (30s cadence), so the next tick skips.
    lastWsTickRef.current = Date.now() + 1_000_000;
    await tick();
    expect(mockFetch).toHaveBeenCalledTimes(1);
  });

  it('polls immediately when the tab becomes visible again', async () => {
    const { dataRef } = setup();
    dataRef.current = [bar(100)];
    mockFetch.mockResolvedValue(delta([bar(100, { close: 9 })]));
    await act(async () => {
      document.dispatchEvent(new Event('visibilitychange'));
      await Promise.resolve();
    });
    expect(mockFetch).toHaveBeenCalledTimes(1);
  });

  describe('phase-boundary poll', () => {
    it('seedMeta arms a one-shot poll just past next_change_at, ahead of the cadence', async () => {
      const { dataRef, result } = setup();
      dataRef.current = [bar(100)];
      mockFetch.mockResolvedValue(delta([bar(100)]));
      act(() => {
        result.current.seedMeta({ watermark: 1, nextChangeAt: Date.now() + 5_000 });
      });
      await tick(6_500); // boundary + 1s buffer land at 6s; cadence not due until 30s
      expect(mockFetch).toHaveBeenCalledTimes(1);
    });

    it('the boundary poll bypasses the WS-healthy skip', async () => {
      const { dataRef, lastWsTickRef, result } = setup();
      dataRef.current = [bar(100)];
      lastWsTickRef.current = Date.now(); // healthy WS would normally skip the poll
      mockFetch.mockResolvedValue(delta([bar(100)]));
      act(() => {
        result.current.seedMeta({ watermark: 1, nextChangeAt: Date.now() + 5_000 });
      });
      await tick(6_500);
      expect(mockFetch).toHaveBeenCalledTimes(1);
    });

    it('a delta poll re-arms the boundary from its own meta', async () => {
      const { dataRef } = setup();
      dataRef.current = [bar(100)];
      mockFetch.mockResolvedValue(delta([bar(100)], { nextChangeAt: BASE + POLL_MS + 5_000 }));
      await tick(); // cadence poll at 30s carries the boundary
      expect(mockFetch).toHaveBeenCalledTimes(1);
      await tick(7_000); // boundary fires at ~36s, well before the 60s cadence tick
      expect(mockFetch).toHaveBeenCalledTimes(2);
    });

    it('does not arm for a boundary already in the past', async () => {
      const { dataRef, result } = setup();
      dataRef.current = [bar(100)];
      mockFetch.mockResolvedValue(delta([bar(100)]));
      act(() => {
        result.current.seedMeta({ watermark: 1, nextChangeAt: Date.now() - 1_000 });
      });
      await tick(POLL_MS - 1_000); // nothing before the first cadence tick
      expect(mockFetch).not.toHaveBeenCalled();
    });

    it('a symbol switch clears the pending boundary', async () => {
      const { dataRef, result, rerender } = setup();
      dataRef.current = [bar(100)];
      mockFetch.mockResolvedValue(delta([bar(100)]));
      act(() => {
        result.current.seedMeta({ watermark: 1, nextChangeAt: Date.now() + 5_000 });
      });
      rerender({ symbol: 'MSFT', interval: '5min', enabled: true });
      await tick(6_500);
      expect(mockFetch).not.toHaveBeenCalled();
    });
  });
});
