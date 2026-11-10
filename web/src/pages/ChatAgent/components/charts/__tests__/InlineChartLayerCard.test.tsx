import { render, renderHook, screen, fireEvent } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { WorkspaceProvider } from '../../../contexts/WorkspaceContext';
import { ChartSurfaceContext, type ChartSurface } from '../../../contexts/ChartSurfaceContext';
import { MessageActionsProvider } from '../../messageList/MessageActionsContext';
import {
  resetLayerViewForTesting, setLayerHidden, useHiddenLayers, useShowLayersRequest,
} from '@/pages/MarketView/stores/layerViewStore';
import { chartKeyFor } from '@/pages/MarketView/utils/chartInstanceKey';
import type { PublishedLayer } from '@/pages/MarketView/types/chartLayer';
import type { ChartLayersCard } from '../../chartLayerGrouping';
import { InlineChartLayerCard } from '../InlineChartLayerCard';

const LONG = 'Price has ridden the lower rail of a rising channel since the August low. Three touches of the lower rail held, each followed by a rally; a daily close under it would void the setup and put the gap in play.';

const KEY = chartKeyFor('ws-art', 'NVDA', '1hour')!;

const layer = (layer_id: string, title: string, summary: string, as_of: string): PublishedLayer => ({
  workspace_id: 'ws-art', chart_id: 'NVDA:1hour', symbol: 'NVDA', timeframe: '1hour', layer_id, title, summary, as_of,
});

const CARD: ChartLayersCard = {
  id: KEY,
  key: KEY,
  workspaceId: 'ws-art',
  symbol: 'NVDA',
  timeframe: '1hour',
  layers: [
    layer('a', 'NVDA rising channel', LONG, '2026-10-28'),
    layer('b', 'Earnings scenarios', 'Short note.', '2026-10-28T15:30'),
  ],
};

function renderCard(
  card: ChartLayersCard = CARD,
  surface: Partial<ChartSurface> = {},
  onOpenChart?: (spec: { symbol: string; timeframe?: string }) => void,
  workspaceId: string | null = 'ws-ctx',
) {
  return render(
    <WorkspaceProvider workspaceId={workspaceId} downloadFile={null}>
      <ChartSurfaceContext.Provider value={{ chartPresent: false, ...surface }}>
        <MessageActionsProvider actions={onOpenChart ? { onOpenChart } : {}}>
          <InlineChartLayerCard card={card} />
        </MessageActionsProvider>
      </ChartSurfaceContext.Provider>
    </WorkspaceProvider>,
  );
}

// jsdom lays nothing out: a clamped summary overflows here when it is long.
beforeEach(() => {
  vi.spyOn(HTMLElement.prototype, 'clientHeight', 'get').mockReturnValue(60);
  vi.spyOn(HTMLElement.prototype, 'scrollHeight', 'get').mockImplementation(function (this: HTMLElement) {
    return (this.textContent?.length ?? 0) > 120 ? 120 : 20;
  });
});

afterEach(() => {
  vi.restoreAllMocks();
  resetLayerViewForTesting();
});

describe('InlineChartLayerCard', () => {
  it('shows the chart, a row per layer with title, as-of and summary', () => {
    renderCard();
    expect(screen.getByRole('region', { name: 'NVDA 1H layers' })).toBeInTheDocument();
    expect(screen.getByText('2 layers')).toBeInTheDocument();
    expect(screen.getByText('NVDA rising channel')).toBeInTheDocument();
    expect(screen.getByText('Earnings scenarios')).toBeInTheDocument();
    expect(screen.getByText('as of Oct 28, 2026')).toBeInTheDocument();
    expect(screen.getByText('as of Oct 28, 2026, 15:30')).toBeInTheDocument();
    expect(screen.getByText('Short note.')).toBeInTheDocument();
  });

  it('expands a summary the clamp cuts off and offers nothing to expand on one it fits', () => {
    renderCard();
    const buttons = screen.getAllByRole('button', { name: 'Show more' });
    expect(buttons).toHaveLength(1);
    expect(buttons[0]).toHaveAttribute('aria-expanded', 'false');
    expect(buttons[0]).toHaveClass('tap-target');
    fireEvent.click(buttons[0]);
    expect(screen.getByRole('button', { name: 'Show less' })).toHaveAttribute('aria-expanded', 'true');
  });

  it('opens the chart tab on this workspace, symbol and timeframe, showing its layers', () => {
    const onOpenChart = vi.fn();
    setLayerHidden(KEY, 'a', true);
    renderCard(CARD, {}, onOpenChart);
    const open = screen.getByRole('button', { name: 'NVDA 1H layers. Open chart with these layers.' });
    expect(open).toHaveClass('tap-target');
    fireEvent.click(open);
    expect(onOpenChart).toHaveBeenCalledWith({ symbol: 'NVDA', timeframe: '1hour', workspaceId: 'ws-art' });
    expect(renderHook(() => useHiddenLayers(KEY)).result.current.has('a')).toBe(false);
    expect(renderHook(() => useShowLayersRequest()).result.current).toMatchObject({ key: KEY, layerIds: ['a', 'b'] });
  });

  it('promises a plain chart on a share, which has no workspace', () => {
    const onOpenChart = vi.fn();
    renderCard(CARD, {}, onOpenChart, null);
    fireEvent.click(screen.getByRole('button', { name: 'NVDA 1H layers. Open chart.' }));
    // The owner's workspace and layers stay out of it: a share viewer cannot read them.
    expect(onOpenChart).toHaveBeenCalledWith({ symbol: 'NVDA', timeframe: '1hour' });
    expect(renderHook(() => useShowLayersRequest()).result.current).toBeNull();
  });

  it('draws nothing for a card with no layers or no symbol', () => {
    expect(renderCard({ ...CARD, layers: [] }).container).toBeEmptyDOMElement();
    expect(renderCard({ ...CARD, symbol: '' }).container).toBeEmptyDOMElement();
  });

  describe('inside MarketView, where the chart shows the layers', () => {
    it('on the instance already on screen, shows and frames its layers without switching', () => {
      const onJumpToChart = vi.fn();
      renderCard(CARD, { chartPresent: true, activeSymbol: 'NVDA', activeTimeframe: '1hour', onJumpToChart });
      const chip = screen.getByRole('button', { name: /2 layers on chart/ });
      expect(screen.queryByRole('region')).toBeNull();
      fireEvent.click(chip);
      expect(onJumpToChart).not.toHaveBeenCalled();
      expect(renderHook(() => useShowLayersRequest()).result.current).toMatchObject({ key: KEY, layerIds: ['a', 'b'] });
    });

    it('reads a class share on screen as the same chart in either spelling', () => {
      const brk = { ...CARD, symbol: 'BRK-B' };
      const onJumpToChart = vi.fn();
      renderCard(brk, { chartPresent: true, activeSymbol: 'BRK.B', activeTimeframe: '1hour', onJumpToChart });
      fireEvent.click(screen.getByRole('button', { name: /2 layers on chart/ }));
      expect(onJumpToChart).not.toHaveBeenCalled();
    });

    it('jumps the chart to another instance on click, framing its layers', () => {
      const onJumpToChart = vi.fn();
      renderCard(CARD, { chartPresent: true, activeSymbol: 'AAPL', activeTimeframe: '1day', onJumpToChart });
      const chip = screen.getByRole('button', { name: /View 2 layers on chart/ });
      // A finger-sized hit area on a phone, where the chip is drawn shorter than one.
      expect(chip).toHaveClass('tap-target');
      fireEvent.click(chip);
      expect(onJumpToChart).toHaveBeenCalledWith('NVDA', '1hour');
      expect(renderHook(() => useShowLayersRequest()).result.current).toMatchObject({ key: KEY, layerIds: ['a', 'b'] });
    });
  });
});
