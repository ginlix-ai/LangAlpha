/**
 * Everything MarketChart does with the agent's layers, in one place: read the
 * chart's layers (`useChartLayerQuery`) less the ones the viewer hid, draw
 * them (`useChartLayers`), frame them (`useLayerFraming`), report the view to
 * the composer beside the chart, and the DOM over the chart (the explain card
 * and the legend).
 *
 * Two calls, on either side of the host's load effect. `useChartLayerDrawing`
 * comes before it, as the chart's other overlays do, so the layer objects are
 * in place when the bars land and the price scale fits them with the bars.
 * `useChartLayerIntegration` comes after it: on an instance switch that effect
 * empties the bars first, so a show request for the new instance waits for its
 * own bars, and the default view they open on, instead of framing the old ones.
 */

import React, { useEffect, useMemo, useState, type RefObject } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import type { IChartApi, ISeriesApi, SeriesMarker, Time } from 'lightweight-charts';

import { useRegisterChartView } from '@/components/chart/chartComposer';
import { useStableHandler } from '@/hooks/useStableHandler';
import type { DefaultBarsView } from '@/lib/bars/chartDataLoaders';

import { ChartLayerLegend } from '../components/ChartLayerLegend';
import { ChartLayerOverlay } from '../components/ChartLayerOverlay';
import { useHiddenLayers } from '../stores/layerViewStore';
import type { BuiltinIndicators } from '../utils/chartLayer/builtinDuplicates';
import type { LayerFrameReporter } from '../utils/chartLayer/layerReport';
import type { StoredChartLayer } from '../types/chartLayer';
import type { ChartKey } from '../utils/chartInstanceKey';
import { installChartLayersDevHelper } from '../utils/chartLayersDevHelper';
import { useChartLayerRows } from './useChartLayerQuery';
import { useChartLayers } from './useChartLayers';
import { useLayerFraming, type LayerFraming } from './useLayerFraming';

interface Bar {
  time: number;
  high: number;
  low: number;
  close: number;
}

export interface ChartLayerDrawingArgs {
  chartRef: RefObject<IChartApi | null>;
  seriesRef: RefObject<ISeriesApi<'Candlestick'> | null>;
  chartData: readonly Bar[];
  chartKey: ChartKey | null;
  interval: string;
  theme: string;
  /** Off on the Advanced chart: nothing is drawn and no view is reported. */
  enabled: boolean;
  /** What the chart plots itself; keep its identity stable. */
  builtins: BuiltinIndicators;
  onFrame?: LayerFrameReporter;
}

export interface ChartLayerDrawing {
  /** Layer markers for the host's one markers plugin to merge. */
  markers: SeriesMarker<Time>[];
  render: ReturnType<typeof useChartLayers>;
  visible: StoredChartLayer[];
}

/** Read the chart's layers less the ones the viewer hid, and draw them. */
export function useChartLayerDrawing({
  chartRef,
  seriesRef,
  chartData,
  chartKey,
  interval,
  theme,
  enabled,
  builtins,
  onFrame,
}: ChartLayerDrawingArgs): ChartLayerDrawing {
  const rows = useChartLayerRows(chartKey);
  const hidden = useHiddenLayers(chartKey);
  const visible = useMemo(() => rows.filter((row) => !hidden.has(row.layer_id)), [rows, hidden]);
  const render = useChartLayers({
    chartRef,
    seriesRef,
    chartData,
    enabled,
    interval,
    theme: theme === 'light' ? 'light' : 'dark',
    layers: visible,
    builtins,
    onFrame,
  });
  return { markers: render.markers, render, visible };
}

export interface ChartLayerIntegrationArgs {
  drawing: ChartLayerDrawing;
  /** The chart, once made; the overlay subscribes to it. */
  chart: IChartApi | null;
  chartRef: RefObject<IChartApi | null>;
  seriesRef: RefObject<ISeriesApi<'Candlestick'> | null>;
  containerRef: RefObject<HTMLElement | null>;
  /** The host's bar store; see `useLayerFraming`. */
  barsRef: RefObject<readonly unknown[]>;
  chartData: readonly Bar[];
  chartKey: ChartKey | null;
  symbol: string;
  interval: string;
  theme: string;
  enabled: boolean;
  /** Off while a selection tool is armed: its gestures own the chart. */
  explainEnabled: boolean;
  defaultView: DefaultBarsView | undefined;
  loading: boolean;
}

export interface ChartLayerIntegration {
  /** Frame and view calls, for the host's handle. */
  handle: LayerFraming;
  /** The explain card and the legend, laid over the chart. */
  ui: React.ReactNode;
  /** An explain card is open: it stands in for the host's OHLC tooltip. */
  cardOpen: boolean;
}

export function useChartLayerIntegration({
  drawing,
  chart,
  chartRef,
  seriesRef,
  containerRef,
  barsRef,
  chartData,
  chartKey,
  symbol,
  interval,
  theme,
  enabled,
  explainEnabled,
  defaultView,
  loading,
}: ChartLayerIntegrationArgs): ChartLayerIntegration {
  const layerTheme = theme === 'light' ? 'light' : 'dark';
  const { render, visible } = drawing;

  const handle = useLayerFraming({
    chartRef,
    containerRef,
    barsRef,
    grid: render.grid,
    layers: visible,
    chartKey,
    symbol,
    interval,
    theme: layerTheme,
    defaultView,
    enabled,
    loading,
  });

  // The composer beside this chart says what it shows (`chart_views`) with each send.
  const readView = useStableHandler(handle.getView);
  useRegisterChartView(readView);

  const queryClient = useQueryClient();
  useEffect(() => installChartLayersDevHelper(queryClient), [queryClient]);

  const [cardOpen, setCardOpen] = useState(false);
  const ui = enabled ? (
    <>
      <ChartLayerOverlay
        chart={chart}
        seriesRef={seriesRef}
        primitiveRef={render.primitiveRef}
        seriesKeysRef={render.seriesKeysRef}
        elements={render.elements}
        chartData={chartData}
        enabled={explainEnabled}
        onCardOpenChange={setCardOpen}
      />
      <ChartLayerLegend chartKey={chartKey} className="chart-layer-legend" />
    </>
  ) : null;

  return { handle, ui, cardOpen: enabled && cardOpen };
}
