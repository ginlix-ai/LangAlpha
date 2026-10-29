import { render, screen, fireEvent } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { WorkspaceProvider } from '../../../contexts/WorkspaceContext';
import { ChartSurfaceContext, type ChartSurface } from '../../../contexts/ChartSurfaceContext';
import { chartAnnotationStore, makeChartId } from '@/pages/MarketView/stores/chartAnnotationStore';
import { InlineChartAnnotationCard } from '../InlineChartAnnotationCard';
import { MessageActionsProvider } from '../../messageList/MessageActionsContext';

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

function renderCard(
  artifact: Record<string, unknown>,
  surface: Partial<ChartSurface> = {},
  onOpenChart?: (spec: { symbol: string; timeframe?: string }) => void,
  workspaceId: string | null = 'ws-ctx',
) {
  const value: ChartSurface = { chartPresent: false, ...surface };
  // No QueryClientProvider on purpose: the card fetches nothing, so a data
  // hook creeping back in would throw here.
  return render(
    <WorkspaceProvider workspaceId={workspaceId} downloadFile={null}>
      <ChartSurfaceContext.Provider value={value}>
        <MessageActionsProvider actions={onOpenChart ? { onOpenChart } : {}}>
          <InlineChartAnnotationCard artifact={artifact} />
        </MessageActionsProvider>
      </ChartSurfaceContext.Provider>
    </WorkspaceProvider>,
  );
}

describe('InlineChartAnnotationCard', () => {
  afterEach(() => {
    vi.clearAllMocks();
    chartAnnotationStore._resetForTesting();
  });

  it('renders as a turn file card', () => {
    renderCard(ARTIFACT);

    // One native button carries the card; the Open door beside it is inert.
    const buttons = screen.getAllByRole('button');
    expect(buttons).toHaveLength(1);
    expect(buttons[0].tagName).toBe('BUTTON');
    // The accessible name starts with the visible one and adds the count.
    expect(buttons[0]).toHaveAccessibleName('NVDA 1D chart, 2 annotations. Open annotated chart.');

    expect(screen.getByText('NVDA 1D chart')).toBeInTheDocument();
    expect(screen.getByText('1D')).toBeInTheDocument();
    // The meta names the real annotations: the labelled price line, and the
    // unlabelled rectangle by its kind. It truncates, so the hover title has it whole.
    const meta = screen.getByText(/^2 annotations · /);
    expect(meta).toHaveTextContent('2 annotations · Resistance, Zone');
    expect(buttons[0]).toHaveAttribute('title', '2 annotations · Resistance, Zone');
    expect(screen.getByText('Open')).toBeInTheDocument();
  });

  it('opens the chart tab in the panel, scoped to symbol and timeframe', () => {
    const onOpenChart = vi.fn();
    renderCard({ ...ARTIFACT, timeframe: '1hour' }, {}, onOpenChart);
    fireEvent.click(screen.getByRole('button'));

    // The artifact's workspace rides along: the tab draws that workspace's annotations, not the panel's.
    expect(onOpenChart).toHaveBeenCalledWith({ symbol: 'NVDA', timeframe: '1hour', workspaceId: 'ws-art' });
  });

  // Clearing the chart only hides the drawing; asking for it from the card
  // brings it back, as the MarketView chip does.
  it('re-shows a drawing the user cleared from the chart', () => {
    const chartId = makeChartId('NVDA', '1day');
    chartAnnotationStore.clearDisplay('ws-art', chartId);
    renderCard(ARTIFACT, {}, vi.fn());
    fireEvent.click(screen.getByRole('button'));
    expect(chartAnnotationStore.isDisplayCleared('ws-art', chartId)).toBe(false);
  });

  // The stripe is a native <button>, so Tab reaches it and Enter and Space
  // open the chart without a key handler of its own.
  it.each([['Enter', '{Enter}'], ['Space', ' ']])('opens the chart from the keyboard (%s)', async (_name, key) => {
    const user = userEvent.setup();
    const onOpenChart = vi.fn();
    renderCard(ARTIFACT, {}, onOpenChart);

    await user.tab();
    expect(screen.getByRole('button')).toHaveFocus();
    await user.keyboard(key);
    expect(onOpenChart).toHaveBeenCalledWith({ symbol: 'NVDA', timeframe: '1day', workspaceId: 'ws-art' });
  });

  // A share mounts the transcript with no workspace, and the chart it opens
  // carries no drawing, so the card offers the chart and promises no more.
  it('offers a plain chart, not an annotated one, on a share', () => {
    const onOpenChart = vi.fn();
    renderCard(ARTIFACT, {}, onOpenChart, null);

    const button = screen.getByRole('button');
    expect(button).toHaveAccessibleName('NVDA 1D chart, 2 annotations. Open chart.');
    expect(button).not.toHaveAccessibleName(/annotated/);

    fireEvent.click(button);
    expect(onOpenChart).toHaveBeenCalledWith({ symbol: 'NVDA', timeframe: '1day', workspaceId: 'ws-art' });
  });

  it('opens the tab on the panel workspace when the artifact names none', () => {
    const onOpenChart = vi.fn();
    const { workspace_id: _omitted, ...bare } = ARTIFACT;
    renderCard(bare, {}, onOpenChart);
    fireEvent.click(screen.getByRole('button'));

    expect(onOpenChart).toHaveBeenCalledWith({ symbol: 'NVDA', timeframe: '1day', workspaceId: 'ws-ctx' });
  });

  it('uses the artifact timeframe for the tile and the name', () => {
    renderCard({ ...ARTIFACT, timeframe: '1hour' });

    // The tile and the name both carry the interval, as a file's do its extension.
    expect(screen.getByText('1H')).toBeInTheDocument();
    expect(screen.getByText('NVDA 1H chart')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /^NVDA 1H chart,/ })).toBeInTheDocument();
  });

  it('collapses to a chip (no chart) when a chart is present', () => {
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
