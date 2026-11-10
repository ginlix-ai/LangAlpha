import { act, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { IChartApi, IPanePrimitive, ISeriesApi, MouseEventParams, SeriesType, Time } from 'lightweight-charts';

vi.mock('@/components/ui/use-toast', () => ({ useToast: () => ({ toast: vi.fn() }) }));

import { renderWithProviders } from '@/test/utils';
import { ChartLayerOverlay } from '../ChartLayerOverlay';
import type { ChartLayerPrimitive } from '../../utils/chartLayer/ChartLayerPrimitive';
import type { LayerElementInfo } from '../../utils/chartLayer/elements';

const KEY = 'l0:drawing:0';
const ELEMENTS = new Map<string, LayerElementInfo>([[KEY, {
  key: KEY,
  ref: { layerId: 'l0', layerTitle: 'Layer 0', kind: 'drawing', note: 'Support from the March low', anchors: [] },
  color: '#e9954a',
  label: 'Support',
  typeKey: 'marketView.layerExplain.types.level',
  layerSummary: null,
  symbol: 'NVDA',
  timeframe: '1day',
}]]);

/**
 * A chart as the overlay sees it. A horizontal pan moves the visible range and
 * redraws; a vertical pan of the price scale only redraws, since lightweight-charts
 * has no event for it. A redraw asks each of the pane's primitives to update its views.
 */
function fakeChart() {
  const moves = new Set<(p: MouseEventParams<Time>) => void>();
  const clicks = new Set<(p: MouseEventParams<Time>) => void>();
  const ranges = new Set<() => void>();
  const primitives = new Set<IPanePrimitive<Time>>();
  const view = { barWidth: 10, scrollX: 0, scrollY: 0 };
  const timeScale = {
    logicalToCoordinate: (l: number) => l * view.barWidth - view.scrollX,
    subscribeVisibleLogicalRangeChange: (f: () => void) => ranges.add(f),
    unsubscribeVisibleLogicalRangeChange: (f: () => void) => ranges.delete(f),
  };
  const pane = {
    attachPrimitive: (p: IPanePrimitive<Time>) => primitives.add(p),
    detachPrimitive: (p: IPanePrimitive<Time>) => primitives.delete(p),
    getHTMLElement: () => null,
  };
  const chart = {
    subscribeCrosshairMove: (f: (p: MouseEventParams<Time>) => void) => moves.add(f),
    unsubscribeCrosshairMove: (f: (p: MouseEventParams<Time>) => void) => moves.delete(f),
    subscribeClick: (f: (p: MouseEventParams<Time>) => void) => clicks.add(f),
    unsubscribeClick: (f: (p: MouseEventParams<Time>) => void) => clicks.delete(f),
    timeScale: () => timeScale,
    panes: () => [pane],
  } as unknown as IChartApi;
  const series = {
    priceToCoordinate: (price: number) => 1000 - price + view.scrollY,
    coordinateToPrice: (y: number) => 1000 - y + view.scrollY,
  } as unknown as ISeriesApi<'Candlestick'>;
  const redraw = () => primitives.forEach((p) => p.updateAllViews?.());
  return {
    chart,
    series,
    primitives,
    click: (x: number, y: number, key: string | null = KEY) => {
      const param = { point: { x, y }, logical: x / view.barWidth, paneIndex: 0, hoveredObjectId: key ?? undefined };
      clicks.forEach((f) => f(param as unknown as MouseEventParams<Time>));
    },
    panTime: (dx: number) => {
      view.scrollX += dx;
      ranges.forEach((f) => f());
      redraw();
    },
    panPrice: (dy: number) => {
      view.scrollY += dy;
      redraw();
    },
  };
}

const nextFrame = () => act(() => new Promise<void>((r) => requestAnimationFrame(() => r())));
const card = () => screen.getByRole('dialog', { name: 'Support' });

function overlay(fake: ReturnType<typeof fakeChart>, enabled = true, onCardOpenChange = (_open: boolean) => {}) {
  const primitive = { setHovered: vi.fn(), hitAt: () => null } as unknown as ChartLayerPrimitive;
  return (
    <ChartLayerOverlay
      chart={fake.chart}
      seriesRef={{ current: fake.series }}
      primitiveRef={{ current: primitive }}
      seriesKeysRef={{ current: new Map<ISeriesApi<SeriesType>, string>() }}
      elements={ELEMENTS}
      chartData={null}
      enabled={enabled}
      onCardOpenChange={onCardOpenChange}
    />
  );
}

function mount(fake: ReturnType<typeof fakeChart>) {
  return renderWithProviders(overlay(fake));
}

beforeEach(() => {
  vi.spyOn(HTMLElement.prototype, 'clientWidth', 'get').mockReturnValue(800);
  vi.spyOn(HTMLElement.prototype, 'clientHeight', 'get').mockReturnValue(500);
});
afterEach(() => vi.restoreAllMocks());

describe('ChartLayerOverlay pinned card', () => {
  // Up and to the left of the point: x - gap(14) - width(280), y - gap(14) - height(120).
  it('stays on its point through a pan of the time scale', async () => {
    const fake = fakeChart();
    mount(fake);
    act(() => fake.click(400, 300));
    expect(card()).toHaveStyle({ left: '106px', top: '166px' });
    act(() => fake.panTime(50));
    await nextFrame();
    expect(card()).toHaveStyle({ left: '56px', top: '166px' });
  });

  it('stays on its point through a pan of the price scale', async () => {
    const fake = fakeChart();
    mount(fake);
    act(() => fake.click(400, 300));
    act(() => fake.panPrice(50));
    await nextFrame();
    expect(card()).toHaveStyle({ left: '106px', top: '216px' });
  });

  it('stops listening for redraws once unpinned', () => {
    const fake = fakeChart();
    mount(fake);
    act(() => fake.click(400, 300));
    act(() => fake.click(10, 10, null));
    expect(screen.queryByRole('dialog')).toBeNull();
    expect(fake.primitives.size).toBe(0);
  });

  it('closes when the overlay is turned off, so a selection tool has the chart', () => {
    const fake = fakeChart();
    const open = vi.fn();
    const { rerender } = renderWithProviders(overlay(fake, true, open));
    act(() => fake.click(400, 300));
    expect(card()).toBeInTheDocument();
    rerender(overlay(fake, false, open));
    expect(screen.queryByRole('dialog')).toBeNull();
    expect(open).toHaveBeenLastCalledWith(false);
  });
});
