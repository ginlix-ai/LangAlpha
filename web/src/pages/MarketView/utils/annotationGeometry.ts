/**
 * Pure helpers that translate stored annotations into lightweight-charts
 * drawable data (time-snapping, primitive items, markers, trendline points).
 *
 * Kept free of React so they can be shared by ``useAgentAnnotations`` (the
 * live MarketView chart) and ``InlineChartAnnotationCard`` (the one-shot
 * mini chart rendered in the chat transcript). Times are unix seconds,
 * prices are raw y-values — the primitive / series do pixel conversion.
 */

import { LineStyle, type SeriesMarker, type Time } from 'lightweight-charts';

import type { ChartDataPoint } from '@/types/market';

import type {
  FibItem,
  RectItem,
  TextItem,
  VLineItem,
  AgentAnnotationsData,
} from './agentAnnotationsPrimitive';
import type {
  FibRetracementAnnotation,
  MarkerAnnotation,
  PriceLineAnnotation,
  RectangleAnnotation,
  StoredAnnotation,
  TextAnnotation,
  TrendlineAnnotation,
  VerticalLineAnnotation,
} from '../stores/chartAnnotationStore';

// Default colors — used only when the agent omits a color. A calm, cohesive
// accent set (slate blue + muted gold for fibs) that reads cleanly on both the
// black dark-mode and cream light-mode chart backgrounds.
export const DEFAULT_LINE_COLOR = '#4F8AD6';
export const DEFAULT_TRENDLINE_COLOR = 'rgba(79,138,214,0.7)';
export const DEFAULT_MARKER_COLOR = '#4F8AD6';
export const DEFAULT_RECT_COLOR = '#4F8AD6';
export const DEFAULT_VLINE_COLOR = '#4F8AD6';
export const DEFAULT_TEXT_COLOR = '#4F8AD6';
export const DEFAULT_FIB_COLOR = '#C99A4E';

/** Standard Fibonacci retracement ratios. */
export const FIB_RATIOS = [0, 0.236, 0.382, 0.5, 0.618, 0.786, 1] as const;

export function dashForStyle(style?: 'solid' | 'dashed' | 'dotted'): number[] {
  if (style === 'dotted') return [1, 3];
  if (style === 'solid') return [];
  return [4, 4]; // dashed (default for vertical lines)
}

export function styleToLwc(style: PriceLineAnnotation['style']): LineStyle {
  if (style === 'dashed') return LineStyle.Dashed;
  if (style === 'dotted') return LineStyle.Dotted;
  return LineStyle.Solid;
}

export function toUnixSeconds(iso: string): number | null {
  const ms = Date.parse(iso);
  if (!Number.isFinite(ms)) return null;
  return Math.floor(ms / 1000);
}

export function snapToNearestBar(
  chartData: ChartDataPoint[] | null,
  target: number,
): number | null {
  if (!chartData || chartData.length === 0) return null;
  let lo = 0;
  let hi = chartData.length - 1;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (chartData[mid].time < target) lo = mid + 1;
    else hi = mid;
  }
  if (lo > 0) {
    const diffLo = Math.abs(chartData[lo].time - target);
    const diffPrev = Math.abs(chartData[lo - 1].time - target);
    if (diffPrev < diffLo) lo -= 1;
  }
  return chartData[lo].time;
}

/** ISO → unix seconds, snapped to the nearest bar when chart data exists. */
export function resolveBarTime(
  chartData: ChartDataPoint[] | null,
  iso: string,
): number | null {
  const secs = toUnixSeconds(iso);
  if (secs == null) return null;
  return snapToNearestBar(chartData, secs) ?? secs;
}

// --- Type guards ----------------------------------------------------------

export function isPriceLine(a: StoredAnnotation): a is PriceLineAnnotation {
  return a.type === 'price_line';
}
export function isTrendline(a: StoredAnnotation): a is TrendlineAnnotation {
  return a.type === 'trendline';
}
export function isMarker(a: StoredAnnotation): a is MarkerAnnotation {
  return a.type === 'marker';
}
export function isVerticalLine(a: StoredAnnotation): a is VerticalLineAnnotation {
  return a.type === 'vertical_line';
}
export function isRectangle(a: StoredAnnotation): a is RectangleAnnotation {
  return a.type === 'rectangle';
}
export function isText(a: StoredAnnotation): a is TextAnnotation {
  return a.type === 'text';
}
export function isFib(a: StoredAnnotation): a is FibRetracementAnnotation {
  return a.type === 'fib_retracement';
}

/**
 * Two-point line data for a trendline, snapped to bars when possible.
 *
 * Falls back to raw timestamps if both points snap to the same bar (LWC
 * rejects duplicate/unsorted times). Returns null for a degenerate line
 * (both anchors at the same time).
 */
export function resolveTrendlineData(
  ann: TrendlineAnnotation,
  chartData: ChartDataPoint[] | null,
): { time: Time; value: number }[] | null {
  const t1 = toUnixSeconds(ann.point1.time);
  const t2 = toUnixSeconds(ann.point2.time);
  if (t1 == null || t2 == null) return null;

  const snap1 = snapToNearestBar(chartData, t1);
  const snap2 = snapToNearestBar(chartData, t2);
  let lineT1: number;
  let lineT2: number;
  let priceA: number;
  let priceB: number;
  if (snap1 != null && snap2 != null && snap1 !== snap2) {
    [lineT1, lineT2, priceA, priceB] =
      snap1 < snap2
        ? [snap1, snap2, ann.point1.price, ann.point2.price]
        : [snap2, snap1, ann.point2.price, ann.point1.price];
  } else if (t1 !== t2) {
    [lineT1, lineT2, priceA, priceB] =
      t1 < t2
        ? [t1, t2, ann.point1.price, ann.point2.price]
        : [t2, t1, ann.point2.price, ann.point1.price];
  } else {
    return null;
  }
  return [
    { time: lineT1 as Time, value: priceA },
    { time: lineT2 as Time, value: priceB },
  ];
}

/**
 * Build the canvas-primitive data (rectangles, vertical lines, text, fib
 * levels) for a set of annotations. Items whose times can't be resolved are
 * skipped.
 */
export function buildPrimitiveData(
  annotations: StoredAnnotation[],
  chartData: ChartDataPoint[] | null,
): AgentAnnotationsData {
  const rects: RectItem[] = [];
  const vlines: VLineItem[] = [];
  const texts: TextItem[] = [];
  const fibs: FibItem[] = [];

  for (const ann of annotations) {
    if (isRectangle(ann)) {
      const t1 = resolveBarTime(chartData, ann.point1.time);
      const t2 = resolveBarTime(chartData, ann.point2.time);
      if (t1 == null || t2 == null) continue;
      rects.push({
        time1: t1,
        time2: t2,
        price1: ann.point1.price,
        price2: ann.point2.price,
        color: ann.color ?? DEFAULT_RECT_COLOR,
        label: ann.label ?? undefined,
      });
    } else if (isVerticalLine(ann)) {
      const t = resolveBarTime(chartData, ann.time);
      if (t == null) continue;
      vlines.push({
        time: t,
        color: ann.color ?? DEFAULT_VLINE_COLOR,
        dash: dashForStyle(ann.style),
        label: ann.label ?? undefined,
      });
    } else if (isText(ann)) {
      const t = resolveBarTime(chartData, ann.time);
      if (t == null) continue;
      texts.push({
        time: t,
        price: ann.price,
        text: ann.text,
        color: ann.color ?? DEFAULT_TEXT_COLOR,
      });
    } else if (isFib(ann)) {
      const t1 = resolveBarTime(chartData, ann.point1.time);
      const t2 = resolveBarTime(chartData, ann.point2.time);
      if (t1 == null || t2 == null) continue;
      const p1 = ann.point1.price;
      const p2 = ann.point2.price;
      const levels = FIB_RATIOS.map((ratio) => ({
        ratio,
        price: p2 + (p1 - p2) * ratio,
      }));
      fibs.push({
        time1: t1,
        time2: t2,
        levels,
        color: ann.color ?? DEFAULT_FIB_COLOR,
      });
    } else if (isTrendline(ann) && ann.label) {
      // The line itself is drawn natively (addLineSeries); only its label
      // becomes a chip, anchored at the chronologically-later endpoint so it
      // sits at the end of the drawn line instead of stranded on the price
      // axis (LWC's native series `title` floats it to the right gutter).
      const s1 = toUnixSeconds(ann.point1.time);
      const s2 = toUnixSeconds(ann.point2.time);
      if (s1 == null || s2 == null) continue;
      const last = s2 >= s1 ? ann.point2 : ann.point1;
      const t = resolveBarTime(chartData, last.time);
      if (t == null) continue;
      texts.push({
        time: t,
        price: last.price,
        text: ann.label,
        color: ann.color ?? DEFAULT_TRENDLINE_COLOR,
      });
    }
  }

  return { rects, vlines, texts, fibs };
}

/** Build LWC series markers for marker annotations, sorted by time. */
export function buildMarkers(
  annotations: StoredAnnotation[],
  chartData: ChartDataPoint[] | null,
): SeriesMarker<Time>[] {
  const markers: SeriesMarker<Time>[] = [];
  for (const ann of annotations) {
    if (!isMarker(ann)) continue;
    const secs = toUnixSeconds(ann.time);
    if (secs == null) continue;
    const snapped = snapToNearestBar(chartData, secs) ?? secs;
    markers.push({
      time: snapped as Time,
      position: ann.position ?? 'aboveBar',
      shape: ann.shape === 'circle' || ann.shape === 'square' ? 'circle' : ann.shape,
      color: ann.color ?? DEFAULT_MARKER_COLOR,
      text: ann.text ?? '',
    });
  }
  // LWC requires markers sorted by time.
  markers.sort((a, b) => (a.time as number) - (b.time as number));
  return markers;
}
