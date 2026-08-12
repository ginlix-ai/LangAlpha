import { render, screen, fireEvent } from '@testing-library/react';
import { MemoryRouter, Routes, Route, useLocation } from 'react-router-dom';
import { afterEach, describe, expect, it, vi } from 'vitest';

// Stub the heavy chart surface — it owns websockets, React Query and the real
// lightweight-charts canvas, all tested in MarketView. Here we only care that
// the card mounts it with the right props once the modal opens.
vi.mock('@/pages/MarketView/components/MarketChartSurface', () => ({
  MarketChartSurface: (props: { symbol: string; timeframe?: string; workspaceId?: string | null }) => (
    <div data-testid="surface">{`${props.symbol}:${props.timeframe}:${props.workspaceId ?? ''}`}</div>
  ),
}));

// Stub the OHLC fetch hook so the resting card's preview chart has bars without
// hitting React Query / the network. Two ascending bars → an "up" green trend.
vi.mock('@/pages/MarketView/hooks/useStockBars', () => ({
  useStockBars: () => ({
    bars: [
      { time: 1_700_000_000, open: 100, high: 102, low: 99, close: 101 },
      { time: 1_700_086_400, open: 101, high: 110, low: 100, close: 108 },
    ],
    isLoading: false,
    isError: false,
  }),
}));

import { WorkspaceProvider } from '../../../contexts/WorkspaceContext';
import { ChartSurfaceContext, type ChartSurface } from '../../../contexts/ChartSurfaceContext';
import { chartAnnotationStore } from '@/pages/MarketView/stores/chartAnnotationStore';
import { InlineChartAnnotationCard } from '../InlineChartAnnotationCard';

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
  surface: Partial<ChartSurface> = {},
) {
  const value: ChartSurface = { chartPresent: false, ...surface };
  return render(
    <MemoryRouter initialEntries={['/chat/t/thread-123']}>
      <WorkspaceProvider workspaceId="ws-ctx" downloadFile={null}>
        <ChartSurfaceContext.Provider value={value}>
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
  afterEach(() => {
    vi.clearAllMocks();
    chartAnnotationStore._resetForTesting();
  });

  it('renders the spotlight preview card, with the heavy surface deferred until opened', () => {
    renderCard(ARTIFACT);

    expect(screen.getByText('NVDA')).toBeInTheDocument();
    // The annotation count rides the card's accessible name; the floating legend
    // names the real annotations (labelled price line + the rectangle's "Zone").
    expect(screen.getByRole('button', { name: /2 annotations/ })).toBeInTheDocument();
    expect(screen.getByText('Resistance')).toBeInTheDocument();
    expect(screen.getByText('Open annotated chart')).toBeInTheDocument();
    // The resting preview is a lightweight inline SVG of the price + overlays;
    // the full lightweight-charts surface only mounts after the modal opens.
    expect(screen.queryByTestId('surface')).not.toBeInTheDocument();
  });

  it('opens the modal with the chart surface, scoped to symbol/timeframe/workspace', async () => {
    renderCard(ARTIFACT);
    fireEvent.click(screen.getByRole('button'));

    // Surface is lazy-loaded, so it resolves a tick after the modal opens.
    expect(await screen.findByTestId('surface')).toHaveTextContent('NVDA:1day:ws-art');
  });

  // The spotlight card is a role="button" — it must be keyboard-operable, not
  // just mouse-clickable. Enter and Space both open the modal.
  it.each(['Enter', ' '])('opens the modal via the %s key (keyboard a11y)', async (key) => {
    renderCard(ARTIFACT);
    const card = screen.getByRole('button');
    expect(card).toHaveAttribute('tabindex', '0');

    fireEvent.keyDown(card, { key });
    expect(await screen.findByTestId('surface')).toHaveTextContent('NVDA:1day:ws-art');
  });

  it('opens MarketView from the modal carrying symbol, ptc mode, workspace, thread, returnTo', async () => {
    renderCard(ARTIFACT);
    fireEvent.click(screen.getByRole('button')); // stage 1 -> modal
    fireEvent.click(await screen.findByText('Open in MarketView'));

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

  it('uses the artifact timeframe for the bubble, the surface, and the MarketView URL', async () => {
    const hourly = { ...ARTIFACT, timeframe: '1hour' };
    renderCard(hourly);

    // The pill shows the short label; the full timeframe rides the accessible name.
    expect(screen.getByText('1H')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /1hour/ })).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button')); // open modal
    expect(await screen.findByTestId('surface')).toHaveTextContent('NVDA:1hour:ws-art');

    // Opening MarketView carries the timeframe so it lands on the right view.
    fireEvent.click(await screen.findByText('Open in MarketView'));
    const loc = await screen.findByTestId('loc');
    const url = loc.textContent || '';
    const params = new URLSearchParams(url.slice(url.indexOf('?')));
    expect(params.get('tf')).toBe('1hour');
  });

  it('collapses to a chip (no chart, no modal) when a chart is present', () => {
    renderCard(ARTIFACT, { chartPresent: true });

    expect(screen.getByText(/on chart/i)).toBeInTheDocument();
    expect(screen.queryByText('Open in MarketView')).not.toBeInTheDocument();
    expect(screen.queryByTestId('surface')).not.toBeInTheDocument();
  });

  it('chip restores a cleared drawing to the chart when clicked', () => {
    // The drawing was cleared from the chart elsewhere (the Clear button).
    chartAnnotationStore.clearDisplay('ws-art', 'NVDA:1day');
    renderCard(ARTIFACT, { chartPresent: true });

    // Chip reflects the cleared state and invites re-showing.
    expect(screen.getByText(/show .* on chart/i)).toBeInTheDocument();
    expect(chartAnnotationStore.isDisplayCleared('ws-art', 'NVDA:1day')).toBe(true);

    fireEvent.click(screen.getByRole('button'));
    expect(chartAnnotationStore.isDisplayCleared('ws-art', 'NVDA:1day')).toBe(false);
  });

  it('chip jumps the chart to a different ticker than the one on screen', () => {
    const onJumpToChart = vi.fn();
    // Drawing is on NVDA but the live chart shows AAPL → the chip offers a jump.
    chartAnnotationStore.clearDisplay('ws-art', 'NVDA:1day');
    renderCard(ARTIFACT, {
      chartPresent: true,
      activeSymbol: 'AAPL',
      activeTimeframe: '1day',
      onJumpToChart,
    });

    expect(screen.getByText(/view .* on chart/i)).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button'));
    expect(onJumpToChart).toHaveBeenCalledWith('NVDA', '1day');
    // Jumping also un-clears the instance so it shows once the chart switches.
    expect(chartAnnotationStore.isDisplayCleared('ws-art', 'NVDA:1day')).toBe(false);
  });

  it('chip confirms (no jump) when it already describes the on-screen instance', () => {
    const onJumpToChart = vi.fn();
    renderCard(ARTIFACT, {
      chartPresent: true,
      activeSymbol: 'NVDA',
      activeTimeframe: '1day',
      onJumpToChart,
    });

    expect(screen.getByText(/on chart/i)).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button'));
    expect(onJumpToChart).not.toHaveBeenCalled();
  });
});
