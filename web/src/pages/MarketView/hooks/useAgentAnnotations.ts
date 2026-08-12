/**
 * Apply agent-sourced annotations (from ``chartAnnotationStore``) to the
 * lightweight-charts instance.
 *
 * Responsibilities:
 * - Create and remove ``priceLine`` primitives on the candlestick series.
 * - Create and remove two-point ``lineSeries`` primitives for trendlines
 *   via ``chart.addLineSeries()``.
 * - Drive one ``AgentAnnotationsPrimitive`` for the canvas shapes
 *   (rectangle, vertical_line, text, fib_retracement).
 * - Derive a list of ``SeriesMarker`` objects for ``marker`` annotations
 *   — the caller passes this list to ``useChartOverlays`` so they merge
 *   with earnings/grade markers (``series.setMarkers()`` replaces, so
 *   everything must be set in one call).
 *
 * The pure annotation→drawable translation lives in
 * ``utils/annotationGeometry`` so it can be shared with the inline mini
 * chart rendered in the chat transcript.
 *
 * Chart-mode guard: when ``chartMode !== 'custom'`` (Advanced/TradingView
 * iframe is active) we skip all chart mutations but leave the store
 * intact — the effect re-runs when the user toggles back to Light.
 */

import { useEffect, useMemo, useRef, type RefObject } from 'react';
import {
  LineStyle,
  type IChartApi,
  type IPriceLine,
  type ISeriesApi,
  type SeriesMarker,
  type Time,
} from 'lightweight-charts';

import type { ChartDataPoint } from '@/types/market';

import { useAnnotationsForView } from '../stores/chartAnnotationStore';
import { AgentAnnotationsPrimitive } from '../utils/agentAnnotationsPrimitive';
import {
  DEFAULT_LINE_COLOR,
  DEFAULT_TRENDLINE_COLOR,
  buildMarkers,
  buildPrimitiveData,
  isPriceLine,
  isTrendline,
  resolveTrendlineData,
  styleToLwc,
} from '../utils/annotationGeometry';

const EMPTY_PRIMITIVE_DATA = { rects: [], vlines: [], texts: [], fibs: [] };

/**
 * Apply agent annotations from the store to the chart.
 *
 * Returns derived marker definitions for the current symbol. The caller
 * should hand them to ``useChartOverlays`` so one ``setMarkers`` call
 * owns the full marker set.
 */
export function useAgentAnnotations(
  chartRef: RefObject<IChartApi | null>,
  candlestickSeriesRef: RefObject<ISeriesApi<'Candlestick'> | null>,
  symbol: string | null | undefined,
  chartMode: string,
  chartData: ChartDataPoint[] | null,
  workspaceId: string | null | undefined,
  timeframe: string | null | undefined,
  visible: boolean = true,
  theme: 'light' | 'dark' = 'dark',
): SeriesMarker<Time>[] {
  const annotations = useAnnotationsForView(workspaceId, symbol, timeframe);

  // Apply annotations only in the Light (custom) chart mode, and only when the
  // user hasn't hidden them. `visible === false` keeps the store intact but
  // removes every live primitive (same teardown path as a non-applyable mode),
  // so toggling back re-creates them.
  const applyable = chartMode === 'custom' && visible;

  // Track LWC objects by annotation_id so we can remove them cleanly.
  const priceLineRefs = useRef<Map<string, IPriceLine>>(new Map());
  const trendlineSeriesRefs = useRef<Map<string, ISeriesApi<'Line'>>>(new Map());

  // One canvas primitive owns rectangles, vertical lines, text, and fib
  // levels (shapes LWC has no native API for). We track which series it is
  // attached to so we can re-attach after a chart/series rebuild.
  const primitiveRef = useRef<AgentAnnotationsPrimitive | null>(null);
  const primitiveSeriesRef = useRef<ISeriesApi<'Candlestick'> | null>(null);

  // Cheap grid signature: bar count + first/last bar time. The geometry
  // functions only read `chartData` to snap anchor TIMES to bar times — never
  // the live price — so a price-only tick (same length + first/last time, new
  // last close) yields identical output. Memoizing on this signature instead of
  // the raw `chartData` reference skips the full rebuild on every price tick,
  // while still recomputing when a bar is appended/backfilled or the
  // symbol/timeframe changes (any of which moves length or first/last time).
  const gridSig = useMemo(() => {
    const n = chartData?.length ?? 0;
    if (!n) return '0';
    return `${n}:${chartData![0].time}:${chartData![n - 1].time}`;
  }, [chartData]);

  useEffect(() => {
    const series = candlestickSeriesRef.current;
    const chart = chartRef.current;
    if (!series || !chart) return;

    // When the chart is not in Light mode, or the symbol changed, remove
    // every live primitive. The store still holds the data so toggling
    // back re-creates them.
    const priceLines = priceLineRefs.current;
    const trendlines = trendlineSeriesRefs.current;

    const alivePriceLineIds = new Set<string>();
    const aliveTrendlineIds = new Set<string>();

    if (applyable) {
      for (const ann of annotations) {
        if (isPriceLine(ann)) {
          alivePriceLineIds.add(ann.annotation_id);
          if (priceLines.has(ann.annotation_id)) continue;
          try {
            const line = series.createPriceLine({
              price: ann.price,
              title: ann.label ?? '',
              color: ann.color ?? DEFAULT_LINE_COLOR,
              lineWidth: 1,
              lineStyle: styleToLwc(ann.style),
              axisLabelVisible: true,
              lineVisible: true,
            });
            priceLines.set(ann.annotation_id, line);
          } catch (err) {
            if (import.meta.env.DEV) {
              console.warn('[useAgentAnnotations] createPriceLine failed', err);
            }
          }
        } else if (isTrendline(ann)) {
          aliveTrendlineIds.add(ann.annotation_id);

          const lineData = resolveTrendlineData(ann, chartData);
          if (!lineData) continue; // degenerate / unparseable times — skip

          let lineSeries = trendlines.get(ann.annotation_id);
          if (!lineSeries) {
            try {
              lineSeries = chart.addLineSeries({
                color: ann.color ?? DEFAULT_TRENDLINE_COLOR,
                lineWidth: 2,
                lineStyle: LineStyle.Dashed,
                lastValueVisible: false,
                priceLineVisible: false,
                crosshairMarkerVisible: false,
                // Label is drawn as a chip at the line's end (see
                // buildPrimitiveData) — the native `title` strands it on the
                // price axis, detached from the line.
              });
              trendlines.set(ann.annotation_id, lineSeries);
            } catch (err) {
              console.warn('[useAgentAnnotations] addLineSeries failed', err);
              continue;
            }
          }
          // Always re-apply data: if chartData was empty on the first pass
          // and populated later, we re-snap to real bar times on the next
          // effect run instead of keeping stale raw timestamps.
          try {
            lineSeries.setData(lineData);
          } catch (err) {
            console.warn('[useAgentAnnotations] trendline setData failed', err);
          }
        }
      }
    }

    // Remove stale entries: anything currently in refs but no longer in
    // the store (or we went into a non-applyable mode).
    for (const [id, line] of priceLines) {
      if (!applyable || !alivePriceLineIds.has(id)) {
        try {
          series.removePriceLine(line);
        } catch {
          /* series may have been disposed */
        }
        priceLines.delete(id);
      }
    }
    for (const [id, lineSeries] of trendlines) {
      if (!applyable || !aliveTrendlineIds.has(id)) {
        try {
          chart.removeSeries(lineSeries);
        } catch {
          /* already removed */
        }
        trendlines.delete(id);
      }
    }
    // `chartData` is read inside (resolveTrendlineData) but `gridSig` is the
    // intentional dependency — see the gridSig comment above.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [annotations, applyable, symbol, chartRef, candlestickSeriesRef, gridSig]);

  // Canvas-primitive shapes: rectangle, vertical_line, text, fib_retracement.
  // Memoized on `gridSig` (not the raw `chartData` ref) so a price-only tick
  // reuses the same payload and the effect below doesn't fire `setData`.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const primitiveData = useMemo(() => buildPrimitiveData(annotations, chartData), [annotations, gridSig]);

  useEffect(() => {
    const series = candlestickSeriesRef.current;
    if (!series) return;

    // Attach a primitive to the current series. If the series was rebuilt
    // (theme/symbol change recreates the chart), the old primitive went away
    // with its disposed series — attach a fresh one to the new series.
    if (!primitiveRef.current || primitiveSeriesRef.current !== series) {
      const prim = new AgentAnnotationsPrimitive();
      try {
        series.attachPrimitive(prim);
        primitiveRef.current = prim;
        primitiveSeriesRef.current = series;
      } catch (err) {
        if (import.meta.env.DEV) {
          console.warn('[useAgentAnnotations] attachPrimitive failed', err);
        }
        return;
      }
    }

    const prim = primitiveRef.current;
    if (!prim) return;

    // Keep the chip palette in sync with the active light/dark theme.
    prim.setTheme(theme);
    // Not in Light mode → draw nothing, but keep the primitive attached so
    // toggling back re-populates from the store.
    prim.setData(applyable ? primitiveData : EMPTY_PRIMITIVE_DATA);
  }, [primitiveData, applyable, symbol, chartRef, candlestickSeriesRef, theme]);

  // Detach the primitive on unmount (chart teardown disposes it otherwise,
  // but a bare hook unmount should clean up after itself too).
  useEffect(() => {
    return () => {
      const prim = primitiveRef.current;
      const series = primitiveSeriesRef.current;
      if (prim && series) {
        try {
          series.detachPrimitive(prim);
        } catch {
          /* series already disposed */
        }
      }
      primitiveRef.current = null;
      primitiveSeriesRef.current = null;
    };
  }, []);

  // Derive marker payloads for the caller to merge with overlay markers.
  // Memoized on `gridSig` (not the raw `chartData` ref) — markers snap to bar
  // times only, so a price-only tick reuses the same array identity.
  return useMemo<SeriesMarker<Time>[]>(
    () => (applyable ? buildMarkers(annotations, chartData) : []),
    // gridSig is the intentional dependency in place of the raw chartData ref.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [annotations, applyable, gridSig],
  );
}
