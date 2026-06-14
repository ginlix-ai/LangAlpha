import { render, screen, fireEvent, act } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createRef } from 'react';
import type { IChartApi, ISeriesApi } from 'lightweight-charts';

import { chartAnnotationStore } from '../../stores/chartAnnotationStore';
import type { StoredAnnotation } from '../../stores/chartAnnotationStore';
import type { ChartDataPoint } from '@/types/market';
import { AgentEventOverlay } from '../AgentEventOverlay';

const T = (iso: string): number => Math.floor(Date.parse(iso) / 1000);

/** Flush the overlay's requestAnimationFrame placement inside act(). */
const flushFrame = (): Promise<void> =>
  act(async () => {
    await new Promise<void>((resolve) => requestAnimationFrame(() => resolve()));
  });

const CHART: ChartDataPoint[] = [
  { time: T('2024-11-14T00:00:00Z'), open: 110, high: 115, low: 108, close: 112, volume: 1 },
];

const EVENT: StoredAnnotation = {
  annotation_id: 'e1',
  symbol: 'NVDA',
  type: 'event',
  time: '2024-11-14T00:00:00Z',
  price: 112,
  title: 'Q3 earnings beat',
  detail: 'Beat EPS by $0.15 and raised full-year guidance ~5%.',
};

/** A chart/series stub whose coordinate calls return fixed pixels. */
function makeRefs() {
  const timeScale = {
    timeToCoordinate: vi.fn(() => 120),
    subscribeVisibleLogicalRangeChange: vi.fn(),
    unsubscribeVisibleLogicalRangeChange: vi.fn(),
  };
  const chart = { timeScale: vi.fn(() => timeScale) } as unknown as IChartApi;
  const series = { priceToCoordinate: vi.fn(() => 90) } as unknown as ISeriesApi<'Candlestick'>;
  const chartRef = createRef<IChartApi | null>() as { current: IChartApi | null };
  const seriesRef = createRef<ISeriesApi<'Candlestick'> | null>() as {
    current: ISeriesApi<'Candlestick'> | null;
  };
  chartRef.current = chart;
  seriesRef.current = series;
  return { chartRef, seriesRef, timeScale };
}

function renderOverlay(over: Partial<Parameters<typeof AgentEventOverlay>[0]> = {}) {
  const { chartRef, seriesRef, timeScale } = makeRefs();
  const utils = render(
    <AgentEventOverlay
      chartRef={chartRef}
      seriesRef={seriesRef}
      chartData={CHART}
      theme="dark"
      visible
      workspaceId="ws"
      symbol="NVDA"
      timeframe="1day"
      {...over}
    />,
  );
  return { ...utils, timeScale };
}

describe('AgentEventOverlay', () => {
  beforeEach(() => {
    chartAnnotationStore._resetForTesting();
    chartAnnotationStore.setAll('ws', 'NVDA:1day', [EVENT]);
  });

  afterEach(() => {
    vi.clearAllMocks();
    chartAnnotationStore._resetForTesting();
  });

  it('renders a badge with the event title once positioned', async () => {
    renderOverlay();
    await flushFrame();
    expect(screen.getByText('Q3 earnings beat')).toBeInTheDocument();
    // Detail is hidden until the badge is opened.
    expect(screen.queryByText(/Beat EPS by/)).not.toBeInTheDocument();
  });

  it('reveals the detail on click (tap path)', async () => {
    renderOverlay();
    await flushFrame();
    fireEvent.click(screen.getByText('Q3 earnings beat'));
    expect(await screen.findByText(/Beat EPS by \$0\.15/)).toBeInTheDocument();
  });

  it('renders nothing when there are no event annotations', async () => {
    chartAnnotationStore._resetForTesting();
    chartAnnotationStore.setAll('ws', 'NVDA:1day', [
      { annotation_id: 'p1', symbol: 'NVDA', type: 'price_line', price: 200 },
    ]);
    renderOverlay();
    await flushFrame();
    expect(screen.queryByText('Q3 earnings beat')).not.toBeInTheDocument();
  });

  it('does not render the badge when an off-screen coordinate is returned', async () => {
    const { timeScale } = renderOverlay();
    await flushFrame();
    expect(screen.getByText('Q3 earnings beat')).toBeInTheDocument();
    timeScale.timeToCoordinate.mockReturnValue(null as unknown as number);
    // Force a reposition by invoking the visible-range subscription callback.
    const cb = timeScale.subscribeVisibleLogicalRangeChange.mock.calls[0]?.[0];
    await act(async () => {
      cb?.();
    });
    expect(screen.queryByText('Q3 earnings beat')).not.toBeInTheDocument();
  });

  it('hides everything when visible=false', async () => {
    renderOverlay({ visible: false });
    await flushFrame();
    expect(screen.queryByText('Q3 earnings beat')).not.toBeInTheDocument();
  });
});
