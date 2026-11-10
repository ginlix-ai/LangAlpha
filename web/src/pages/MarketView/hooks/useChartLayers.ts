/**
 * Render the agent's chart layers on the MarketView chart.
 *
 * Owns every lightweight-charts object a layer needs: one series per layer
 * series, one pane per layer pane (created after RSI, removed when its layer
 * goes), the level and alert price lines, the fills between series, the
 * regime shading on the lower panes, a whitespace-only series that extends the
 * time scale into the future so forward drawings have somewhere to be, and the
 * `ChartLayerPrimitive` that draws the drawings and regimes. What they should
 * be is decided by `planLayerChart` without a chart; the live objects are
 * reconciled against it (`lwcObjects`). Markers come back to the caller, since
 * the one markers plugin (`useChartOverlays`) must own the whole list.
 *
 * Geometry recomputes only when the bar grid moves (a bar appended or
 * prepended, the symbol or interval changes), never on a price tick.
 *
 * What the chart already draws is not drawn twice: a layer's RSI pane with the
 * built-in RSI's period joins the built-in pane, an SMA the chart already plots
 * and a volume histogram are skipped (`planBuiltinDuplicates`), and lines at one
 * price share one axis label (`mergeAxisLines`). Each such call is a note in the
 * render report, which is built only when the host passes `onFrame`.
 *
 * Disabled (the Advanced chart), or with no layers, everything is torn down.
 */

import { useEffect, useMemo, useRef, type RefObject } from 'react';
import { useTranslation } from 'react-i18next';
import type { IChartApi, ISeriesApi, SeriesMarker, SeriesType, Time } from 'lightweight-charts';

import { useLocale } from '@/hooks/useLocale';
import { useThemeTokens } from '@/lib/themeTokens';

import type { StoredChartLayer } from '../types/chartLayer';
import { ChartLayerPrimitive } from '../utils/chartLayer/ChartLayerPrimitive';
import type { BuiltinIndicators } from '../utils/chartLayer/builtinDuplicates';
import { compileLayers, EMPTY_COMPILED } from '../utils/chartLayer/compileLayers';
import type { LayerElementInfo } from '../utils/chartLayer/elements';
import { readLayerChips } from '../utils/chartLayer/domReport';
import { readPaneTags, type LayerFrameReporter, type ReportContext } from '../utils/chartLayer/layerReport';
import { buildLayerText } from '../utils/chartLayer/layerText';
import { getLayerTheme, type LayerTheme } from '../utils/chartLayer/layerTheme';
import { maxLayerTime } from '../utils/chartLayer/layerTimes';
import {
  beginPass,
  emptyLayerObjects,
  paneTitles,
  reconcileFills,
  reconcileFuture,
  reconcilePriceLines,
  reconcileRegimes,
  reconcileSeries,
  removeLayerObjects,
} from '../utils/chartLayer/lwcObjects';
import { EMPTY_PAINT_CONTEXT, type PaintContext } from '../utils/chartLayer/painter';
import { gridFor, type TimeGrid } from '../utils/chartLayer/timeGrid';
import { gridAnchor } from '../utils/chartLayer/xMapper';
import { useStableBars, type BarLike } from './useStableBars';

const NO_BUILTINS: BuiltinIndicators = { rsiPeriod: null, maPeriods: [], volume: false };

export interface ChartLayersRender {
  /** Layer markers for `useChartOverlays` to merge. */
  markers: SeriesMarker<Time>[];
  /** Explainable elements by key, for the hover / ask overlay. */
  elements: Map<string, LayerElementInfo>;
  /** The drawing primitive, for hit tests and hover emphasis. */
  primitiveRef: RefObject<ChartLayerPrimitive | null>;
  /** Layer series → element key, to explain a hovered series. */
  seriesKeysRef: RefObject<Map<ISeriesApi<SeriesType>, string>>;
  /** The chart's time grid (its bars, plus the future room the layers reach into). */
  grid: TimeGrid;
}

export interface UseChartLayersArgs {
  chartRef: RefObject<IChartApi | null>;
  seriesRef: RefObject<ISeriesApi<'Candlestick'> | null>;
  chartData: readonly BarLike[] | null;
  /** Off on the Advanced chart: nothing is drawn and every object is removed. */
  enabled: boolean;
  interval: string;
  theme: 'light' | 'dark';
  layers: readonly StoredChartLayer[];
  /** What the chart already plots, so a layer's copy of it is not drawn twice. Keep its identity stable. */
  builtins?: BuiltinIndicators;
  /** Called with each painted frame's render report; only a snapshot page asks for one. */
  onFrame?: LayerFrameReporter;
}

export function useChartLayers({
  chartRef,
  seriesRef,
  chartData,
  enabled,
  interval,
  theme,
  layers,
  builtins = NO_BUILTINS,
  onFrame,
}: UseChartLayersArgs): ChartLayersRender {
  const applyable = enabled && layers.length > 0;
  const lt: LayerTheme = useThemeTokens(getLayerTheme, theme);
  // The renderer's own words; the painter cannot call hooks, so they ride in with the theme.
  const { t } = useTranslation();
  const locale = useLocale();
  const text = useMemo(() => buildLayerText(t, locale), [t, locale]);

  const { times, highs, lows } = useStableBars(chartData);
  const until = useMemo(() => (applyable ? maxLayerTime(layers.map((l) => l.layer)) : null), [applyable, layers]);
  const grid = useMemo(() => gridFor(times, interval, { until }), [times, interval, until]);

  const { plan, scene, markers, markerBoxes, elements } = useMemo(
    () => (applyable ? compileLayers(layers, grid, builtins, lt) : EMPTY_COMPILED),
    [applyable, layers, grid, builtins, lt],
  );

  const anchor = useMemo(() => gridAnchor(grid), [grid]);

  // ── Live LWC objects ──
  const live = useRef(emptyLayerObjects());
  const seriesKeysRef = useRef(new Map<ISeriesApi<SeriesType>, string>());
  const paintCtx = useRef<PaintContext>(EMPTY_PAINT_CONTEXT);
  const reportCtx = useRef<ReportContext | null>(null);
  // Declared before the effect that reads it; the drawing effect below fills it.
  const primitiveRef = useRef<ChartLayerPrimitive | null>(null);

  const reporting = !!onFrame;
  const rsiPeriod = builtins.rsiPeriod;
  useEffect(() => {
    const chart = chartRef.current;
    const candles = seriesRef.current;
    if (!chart || !candles) return;
    const objects = live.current;
    beginPass(objects, chart);
    seriesKeysRef.current = reconcileSeries(chart, objects, plan, { neutral: lt.neutral, muted: lt.muted });
    reconcileRegimes(chart, objects, scene.regimes, anchor);
    const axis = reconcilePriceLines(chart, objects, candles, plan.axisLines);
    reconcileFills(objects, plan.fills);
    reconcileFuture(chart, objects, enabled ? grid.future : []);

    paintCtx.current = { ...paintCtx.current, markers: markerBoxes, axisLines: axis.boxes };
    reportCtx.current =
      reporting && applyable
        ? {
            grid,
            names: elements.names,
            layers: layers.map((l) => ({ id: l.layer.id, title: l.layer.title })),
            panes: paneTitles(chart, objects, plan, rsiPeriod),
            notes: [...plan.notes, ...axis.notes],
            axisLines: plan.axisLines,
            paneTags: () => readPaneTags(candles),
            paneChips: () => readLayerChips(chart),
          }
        : null;
    primitiveRef.current?.setPaintContext(paintCtx.current);
    primitiveRef.current?.setReporter(onFrame ?? null, reportCtx.current);
  }, [chartRef, seriesRef, enabled, applyable, layers, grid, plan, scene, markerBoxes, anchor, lt, elements, reporting, onFrame, rsiPeriod]);

  // The candle extremes ride apart from the reconcile pass: a forming bar's new
  // high or low only repaints, since the plan does not read them.
  useEffect(() => {
    paintCtx.current = { ...paintCtx.current, highs, lows };
    primitiveRef.current?.setPaintContext(paintCtx.current);
  }, [highs, lows]);

  // ── The drawing primitive on the candle series ──
  const primitiveHost = useRef<ISeriesApi<'Candlestick'> | null>(null);
  useEffect(() => {
    const series = seriesRef.current;
    if (!series) return;
    if (!primitiveRef.current || primitiveHost.current !== series) {
      const prim = new ChartLayerPrimitive();
      try {
        series.attachPrimitive(prim);
      } catch (err) {
        if (import.meta.env.DEV) console.warn('[useChartLayers] attachPrimitive failed', err);
        return;
      }
      primitiveRef.current = prim;
      primitiveHost.current = series;
    }
    primitiveRef.current.setTheme(lt);
    primitiveRef.current.setText(text);
    primitiveRef.current.setPaintContext(paintCtx.current);
    primitiveRef.current.setReporter(onFrame ?? null, reportCtx.current);
    primitiveRef.current.setScene(scene, anchor);
  }, [seriesRef, scene, anchor, lt, text, onFrame]);

  // Unmount: drop everything this hook added to a chart that outlives it.
  useEffect(() => {
    const objects = live.current;
    return () => {
      const host = primitiveHost.current;
      const prim = primitiveRef.current;
      try {
        if (host && prim) host.detachPrimitive(prim);
      } catch {
        /* disposed */
      }
      removeLayerObjects(objects);
      primitiveRef.current = null;
      primitiveHost.current = null;
    };
  }, []);

  return { markers, elements: elements.explain, primitiveRef, seriesKeysRef, grid };
}
