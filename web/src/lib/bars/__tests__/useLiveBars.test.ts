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
  const view = renderHook(
    ({ symbol, interval, enabled }) =>
      useLiveBars(symbol, interval, { enabled, dataRef, lastWsTickRef, onBars, onMeta, onPhase }),
    { initialProps: { symbol: 'AAPL', interval: '5min', enabled: true, ...overrides } },
  );
  return { dataRef, lastWsTickRef, onBars, onMeta, onPhase, ...view };
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
