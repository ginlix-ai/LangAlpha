/**
 * Framing the chart from outside: a layer by the shared rule (`layerFrame`),
 * a time window in the chart's convention, and the window on screen as a
 * chart view (`ChartView`). Backs `MarketChart`'s handle (`frameLayer`,
 * `showTimeRange`, `getViewForContext`) and takes a request to show layers
 * on this chart (a live publish, a card's "Open chart") once they and its bars
 * are there. Everything maps through the chart's one time grid, so a time past
 * the last bar lands on the slot the layer painter draws it at.
 */

import { useEffect, useEffectEvent, useRef, useState, type RefObject } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import type { IChartApi } from 'lightweight-charts';

import { useLocale } from '@/hooks/useLocale';
import type { DefaultBarsView } from '@/lib/bars/chartDataLoaders';
import { isSupported } from '@/lib/locale';

import {
  clearShowRequest,
  showRequestExpired,
  useShowLayersRequest,
  type ShowLayersRequest,
} from '../stores/layerViewStore';
import type { StoredChartLayer } from '../types/chartLayer';
import type { ChartView } from '../types/chartView';
import type { ChartKey } from '../utils/chartInstanceKey';
import type { TimeGrid } from '../utils/chartLayer/timeGrid';
import { chartLayersKey } from '../utils/chartLayerCache';
import { buildChartView, logicalRangeForTimes } from '../utils/chartView';
import { layerFrame } from '../utils/layerFrame';

export interface UseLayerFramingArgs {
  chartRef: RefObject<IChartApi | null>;
  /** The chart's DOM box: its width frames a layer, its size goes in the chart view. */
  containerRef: RefObject<HTMLElement | null>;
  /**
   * The bar store, read only to tell whether the bars on screen are this
   * instance's: a symbol or interval switch empties it in the load effect,
   * before this hook's effect runs, while `grid` still holds the last
   * render's bars.
   */
  barsRef: RefObject<readonly unknown[]>;
  grid: TimeGrid;
  layers: readonly StoredChartLayer[];
  /** The chart's key: a show request for it is this chart's to take. */
  chartKey: ChartKey | null;
  symbol: string;
  interval: string;
  theme: 'light' | 'dark';
  defaultView: DefaultBarsView | undefined;
  /** Off on the Advanced chart, which has no view to report. */
  enabled: boolean;
  /** Bars are still loading: a frame request waits for them and the view they open on. */
  loading: boolean;
}

export interface LayerFraming {
  /** Frame layers of this chart by id. False until they and the bars are there. */
  frameLayer: (layerIds: readonly string[]) => boolean;
  /** Show a time window; false until bars are there or when a time is unreadable. */
  showTimeRange: (from: string, to: string) => boolean;
  /** The window on screen as a chart view, or null (disabled, no bars, no size). */
  getView: () => ChartView | null;
}

/** The chart's width for framing: the box it sits in, else what the chart was given. */
function widthOf(chart: IChartApi, container: HTMLElement | null): number {
  return container?.clientWidth || chart.options().width || 800;
}

/** Frame `ids` of `layers` on `chart` by the shared rule. False when none of them is there. */
function frameLayersOn(
  chart: IChartApi,
  width: number,
  layers: readonly StoredChartLayer[],
  ids: readonly string[],
  grid: TimeGrid,
  interval: string,
  defaultView: DefaultBarsView | undefined,
): boolean {
  const want = new Set(ids);
  const picked = layers.filter((l) => want.has(l.layer_id)).map((l) => l.layer);
  if (picked.length === 0) return false;
  const frame = layerFrame(picked, grid.bars, width, interval, { defaultView });
  if (!frame) return false;
  chart.timeScale().setVisibleLogicalRange(frame);
  return true;
}

export function useLayerFraming({
  chartRef,
  containerRef,
  barsRef,
  grid,
  layers,
  chartKey,
  symbol,
  interval,
  theme,
  defaultView,
  enabled,
  loading,
}: UseLayerFramingArgs): LayerFraming {
  const language = useLocale();
  const locale = isSupported(language) ? language : 'en-US';
  // The chart, once it holds this instance's bars.
  const readyChart = (): IChartApi | null => {
    const chart = chartRef.current;
    return chart && barsRef.current?.length && grid.bars.length ? chart : null;
  };

  const frameLayer = (layerIds: readonly string[]): boolean => {
    const chart = readyChart();
    if (!chart) return false;
    return frameLayersOn(chart, widthOf(chart, containerRef.current), layers, layerIds, grid, interval, defaultView);
  };

  const showTimeRange = (from: string, to: string): boolean => {
    const chart = readyChart();
    if (!chart) return false;
    const range = logicalRangeForTimes(grid.bars, interval, from, to);
    if (!range) return false;
    chart.timeScale().setVisibleLogicalRange(range);
    return true;
  };

  const getView = (): ChartView | null => {
    if (!enabled) return null;
    const chart = chartRef.current;
    const el = containerRef.current;
    if (!chart || !el) return null;
    const rect = el.getBoundingClientRect();
    return buildChartView({
      symbol,
      interval,
      theme,
      locale,
      barTimes: grid.bars,
      range: chart.timeScale().getVisibleLogicalRange(),
      width: rect.width,
      height: rect.height,
    });
  };

  // Taken once, so the user's own pans and zooms after it stand.
  // Tried again whenever what framing reads changes, until it lands.
  const request = useShowLayersRequest();
  const queryClient = useQueryClient();
  const readSeq = useRef(0);
  // The request whose read of the list has come back.
  const [readBack, setReadBack] = useState(0);
  const take = useEffectEvent((req: ShowLayersRequest, key: ChartKey) => {
    if (showRequestExpired(req)) return clearShowRequest(req.seq);
    // A card can name layers this chart has not read (a background command
    // published them with no stream), so the list is read again, once, and
    // the frame waits for it: framed on part of the group, the rest would
    // land outside the view. Once it is back, what the chart holds is framed.
    if (req.layerIds.some((id) => !layers.some((l) => l.layer_id === id))) {
      if (readSeq.current !== req.seq) {
        readSeq.current = req.seq;
        void queryClient
          .invalidateQueries({ queryKey: chartLayersKey(key) })
          .finally(() => setReadBack(req.seq));
      }
      if (readBack !== req.seq) return;
    }
    if (frameLayer(req.layerIds)) clearShowRequest(req.seq);
  });
  useEffect(() => {
    if (request && request.key === chartKey && !loading) take(request, chartKey);
  }, [request, chartKey, loading, layers, grid, interval, defaultView, readBack]);

  return { frameLayer, showTimeRange, getView };
}
