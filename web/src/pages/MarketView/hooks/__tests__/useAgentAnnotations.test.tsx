/**
 * Regression tests for the grid-signature memoization in
 * ``useAgentAnnotations``. A live price-only tick churns the ``chartData``
 * array reference but does not change bar count or first/last bar time, and the
 * annotation geometry only snaps anchors to bar TIMES — so it must NOT trigger
 * a rebuild of the canvas-primitive payload or the returned marker array.
 * Appending a bar (length + last time change) MUST recompute both.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { act, renderHook } from '@testing-library/react';
import { createRef } from 'react';
import type { IChartApi, ISeriesApi } from 'lightweight-charts';

import type { ChartDataPoint } from '@/types/market';
import {
  chartAnnotationStore,
  makeChartId,
  type StoredAnnotation,
} from '../../stores/chartAnnotationStore';

// Capture the live primitive instance + its setData calls. The primitive does
// real canvas work in production; here we only care that `setData` is (or is
// not) invoked, so a class stub with vi.fn() methods is enough.
const setDataSpy = vi.fn();
const setThemeSpy = vi.fn();
vi.mock('../../utils/agentAnnotationsPrimitive', () => {
  class AgentAnnotationsPrimitive {
    setData = setDataSpy;
    setTheme = setThemeSpy;
  }
  return { AgentAnnotationsPrimitive };
});

// Import after the mock is registered so the hook picks up the stub primitive.
import { useAgentAnnotations } from '../useAgentAnnotations';

const WORKSPACE = 'ws-1';
const SYMBOL = 'NVDA';
const TIMEFRAME = '1day';

const T = (iso: string): number => Math.floor(Date.parse(iso) / 1000);

/** A two-bar grid; later mutated to a price-only tick or a bar-append. */
function makeChart(lastClose: number, bars = 2): ChartDataPoint[] {
  const base = [
    { time: T('2024-11-12T00:00:00Z'), open: 100, high: 105, low: 99, close: 102, volume: 1 },
    { time: T('2024-11-13T00:00:00Z'), open: 102, high: 108, low: 101, close: lastClose, volume: 1 },
  ];
  if (bars > 2) {
    base.push({
      time: T('2024-11-14T00:00:00Z'),
      open: lastClose,
      high: lastClose + 5,
      low: lastClose - 2,
      close: lastClose + 3,
      volume: 1,
    });
  }
  return base;
}

/** Annotations exercising both the primitive payload and the marker array. */
function seedAnnotations(): void {
  const chartId = makeChartId(SYMBOL, TIMEFRAME);
  const rect: StoredAnnotation = {
    annotation_id: 'r1',
    symbol: SYMBOL,
    type: 'rectangle',
    point1: { time: '2024-11-12T00:00:00Z', price: 100 },
    point2: { time: '2024-11-13T00:00:00Z', price: 108 },
    label: 'Zone',
  };
  const marker: StoredAnnotation = {
    annotation_id: 'm1',
    symbol: SYMBOL,
    type: 'marker',
    time: '2024-11-13T00:00:00Z',
    shape: 'arrowUp',
    text: 'Buy',
  };
  chartAnnotationStore.add(WORKSPACE, chartId, rect);
  chartAnnotationStore.add(WORKSPACE, chartId, marker);
}

/** Stub chart/series refs the primitive + native-line effects need. */
function makeRefs() {
  const series = {
    attachPrimitive: vi.fn(),
    detachPrimitive: vi.fn(),
    createPriceLine: vi.fn(),
    removePriceLine: vi.fn(),
    setData: vi.fn(),
  } as unknown as ISeriesApi<'Candlestick'>;
  const chart = {
    addLineSeries: vi.fn(() => ({ setData: vi.fn() })),
    removeSeries: vi.fn(),
  } as unknown as IChartApi;
  const chartRef = createRef<IChartApi | null>() as { current: IChartApi | null };
  const seriesRef = createRef<ISeriesApi<'Candlestick'> | null>() as {
    current: ISeriesApi<'Candlestick'> | null;
  };
  chartRef.current = chart;
  seriesRef.current = series;
  return { chartRef, seriesRef };
}

function renderAnnotations(chartData: ChartDataPoint[]) {
  const { chartRef, seriesRef } = makeRefs();
  return renderHook(
    ({ data }: { data: ChartDataPoint[] }) =>
      useAgentAnnotations(
        chartRef,
        seriesRef,
        SYMBOL,
        'custom',
        data,
        WORKSPACE,
        TIMEFRAME,
        true,
        'dark',
      ),
    { initialProps: { data: chartData } },
  );
}

describe('useAgentAnnotations grid-signature memoization', () => {
  beforeEach(() => {
    chartAnnotationStore._resetForTesting();
    setDataSpy.mockClear();
    setThemeSpy.mockClear();
    seedAnnotations();
  });
  afterEach(() => {
    chartAnnotationStore._resetForTesting();
  });

  it('does NOT rebuild on a price-only tick (same length + first/last time)', () => {
    const { result, rerender } = renderAnnotations(makeChart(106));

    const setDataAfterMount = setDataSpy.mock.calls.length;
    const markersAfterMount = result.current;
    expect(setDataAfterMount).toBeGreaterThan(0); // built once on mount

    // Price-only tick: brand-new array reference, same 2 bars, same first/last
    // time, only the last close moved. gridSig is unchanged → no recompute.
    act(() => rerender({ data: makeChart(107) }));

    expect(setDataSpy.mock.calls.length).toBe(setDataAfterMount);
    expect(result.current).toBe(markersAfterMount); // same array identity
  });

  it('DOES recompute when a bar is appended (length + last time change)', () => {
    const { result, rerender } = renderAnnotations(makeChart(106));

    const setDataAfterMount = setDataSpy.mock.calls.length;
    const markersAfterMount = result.current;

    // Append a third bar: length 2 → 3 and last bar time advances → gridSig
    // changes → both the primitive payload and the marker array recompute.
    act(() => rerender({ data: makeChart(106, 3) }));

    expect(setDataSpy.mock.calls.length).toBeGreaterThan(setDataAfterMount);
    expect(result.current).not.toBe(markersAfterMount);
  });
});
