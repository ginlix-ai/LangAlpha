import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { MemoryRouter, Routes, Route, useLocation } from 'react-router-dom';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

// --- Mock lightweight-charts (canvas can't render in jsdom) ---------------
const fakeSeries = {
  setData: vi.fn(),
  createPriceLine: vi.fn(),
  setMarkers: vi.fn(),
  attachPrimitive: vi.fn(),
};
const fakeLineSeries = { setData: vi.fn() };
const fakeChart = {
  addCandlestickSeries: vi.fn(() => fakeSeries),
  addLineSeries: vi.fn(() => fakeLineSeries),
  timeScale: vi.fn(() => ({ fitContent: vi.fn() })),
  remove: vi.fn(),
};
vi.mock('lightweight-charts', () => ({
  createChart: vi.fn(() => fakeChart),
  ColorType: { Solid: 'solid' },
  CrosshairMode: { Normal: 1, Magnet: 0, Hidden: 2 },
  LineStyle: { Solid: 0, Dotted: 1, Dashed: 2 },
}));

// --- Mock the market-data fetch -------------------------------------------
import { fetchStockData } from '@/pages/MarketView/utils/api';
vi.mock('@/pages/MarketView/utils/api', () => ({
  fetchStockData: vi.fn(),
}));

import { WorkspaceProvider } from '../../../contexts/WorkspaceContext';
import { ChartSurfaceContext } from '../../../contexts/ChartSurfaceContext';
import { chartAnnotationStore } from '@/pages/MarketView/stores/chartAnnotationStore';
import { InlineChartAnnotationCard } from '../InlineChartAnnotationCard';

const mockedFetch = vi.mocked(fetchStockData);

const ARTIFACT = {
  type: 'chart_annotation',
  op: 'add',
  symbol: 'NVDA',
  workspace_id: 'ws-art',
  annotation_id: 'ann_1',
  annotations: [
    { annotation_id: 'ann_1', symbol: 'NVDA', type: 'price_line', price: 205, label: 'Resistance' },
    {
      annotation_id: 'ann_2',
      symbol: 'NVDA',
      type: 'rectangle',
      point1: { time: '2024-10-16T00:00:00Z', price: 150 },
      point2: { time: '2024-11-20T00:00:00Z', price: 140 },
    },
  ],
};

function LocationDisplay(): React.ReactElement {
  const loc = useLocation();
  return <div data-testid="loc">{loc.pathname + loc.search}</div>;
}

function renderCard(
  artifact: Record<string, unknown>,
  { chartPresent = false }: { chartPresent?: boolean } = {},
) {
  return render(
    <MemoryRouter initialEntries={['/chat/t/thread-123']}>
      <WorkspaceProvider workspaceId="ws-ctx" downloadFile={null}>
        <ChartSurfaceContext.Provider value={{ chartPresent }}>
          <Routes>
            <Route
              path="/chat/t/:threadId"
              element={<InlineChartAnnotationCard artifact={artifact} />}
            />
            <Route path="/market" element={<LocationDisplay />} />
          </Routes>
        </ChartSurfaceContext.Provider>
      </WorkspaceProvider>
    </MemoryRouter>,
  );
}

describe('InlineChartAnnotationCard', () => {
  beforeEach(() => {
    mockedFetch.mockResolvedValue({
      data: [
        { time: Math.floor(Date.parse('2024-10-16T00:00:00Z') / 1000), open: 100, high: 105, low: 99, close: 104, volume: 1 },
        { time: Math.floor(Date.parse('2024-11-20T00:00:00Z') / 1000), open: 140, high: 152, low: 139, close: 150, volume: 1 },
      ],
    } as never);
  });

  afterEach(() => {
    vi.clearAllMocks();
    chartAnnotationStore._resetForTesting();
  });

  it('renders a mini-chart card and applies annotations after fetch', async () => {
    renderCard(ARTIFACT);

    expect(screen.getByText('NVDA')).toBeInTheDocument();
    expect(screen.getByText('2 annotations')).toBeInTheDocument();
    expect(screen.getByText('Open in MarketView')).toBeInTheDocument();

    await waitFor(() => expect(mockedFetch).toHaveBeenCalledWith('NVDA', '1day', expect.any(String), expect.any(String), expect.any(Object)));
    // native price line + primitive applied
    await waitFor(() => expect(fakeSeries.createPriceLine).toHaveBeenCalled());
    expect(fakeSeries.attachPrimitive).toHaveBeenCalled();
  });

  it('expands into MarketView carrying symbol, ptc mode, workspace, thread, returnTo', async () => {
    renderCard(ARTIFACT);
    fireEvent.click(screen.getByText('NVDA'));

    const loc = await screen.findByTestId('loc');
    const url = loc.textContent || '';
    expect(url.startsWith('/market?')).toBe(true);
    const params = new URLSearchParams(url.slice(url.indexOf('?')));
    expect(params.get('symbol')).toBe('NVDA');
    expect(params.get('mode')).toBe('ptc');
    expect(params.get('ws')).toBe('ws-art');
    expect(params.get('thread')).toBe('thread-123');
    expect(params.get('returnTo')).toBe('/chat/t/thread-123');
  });

  it('uses the artifact timeframe for the header, fetch, and expand URL', async () => {
    const hourly = { ...ARTIFACT, timeframe: '1hour' };
    renderCard(hourly);

    // Header shows the timeframe.
    expect(screen.getByText('1hour')).toBeInTheDocument();
    // Fetch uses the timeframe, not the daily default.
    await waitFor(() =>
      expect(mockedFetch).toHaveBeenCalledWith(
        'NVDA',
        '1hour',
        expect.any(String),
        expect.any(String),
        expect.any(Object),
      ),
    );

    // Expanding carries the timeframe so MarketView lands on the right view.
    fireEvent.click(screen.getByText('NVDA'));
    const loc = await screen.findByTestId('loc');
    const url = loc.textContent || '';
    const params = new URLSearchParams(url.slice(url.indexOf('?')));
    expect(params.get('tf')).toBe('1hour');
  });

  it('collapses to a chip (no chart, no navigation) when a chart is present', async () => {
    const { createChart } = await import('lightweight-charts');
    renderCard(ARTIFACT, { chartPresent: true });

    expect(screen.getByText(/on chart/i)).toBeInTheDocument();
    expect(screen.queryByText('Open in MarketView')).not.toBeInTheDocument();
    expect(vi.mocked(createChart)).not.toHaveBeenCalled();
    expect(mockedFetch).not.toHaveBeenCalled();
  });

  it('chip restores a cleared drawing to the chart when clicked', async () => {
    // The drawing was cleared from the chart elsewhere (the Clear button).
    chartAnnotationStore.clearDisplay('ws-art', 'NVDA:1day');
    renderCard(ARTIFACT, { chartPresent: true });

    // Chip reflects the cleared state and invites re-showing.
    expect(screen.getByText(/show .* on chart/i)).toBeInTheDocument();
    expect(chartAnnotationStore.isDisplayCleared('ws-art', 'NVDA:1day')).toBe(true);

    fireEvent.click(screen.getByRole('button'));
    expect(chartAnnotationStore.isDisplayCleared('ws-art', 'NVDA:1day')).toBe(false);
  });

  it('falls back to a text summary when the data fetch fails', async () => {
    mockedFetch.mockResolvedValue({ data: [], error: 'No data available' } as never);
    renderCard(ARTIFACT);

    expect(await screen.findByText(/Chart preview unavailable/i)).toBeInTheDocument();
  });
});
