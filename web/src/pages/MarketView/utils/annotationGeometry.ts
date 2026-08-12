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
  EventAnnotation,
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
// dark (#232426) and light (#FFFFFF) chart backgrounds.
export const DEFAULT_LINE_COLOR = '#4F8AD6';
export const DEFAULT_TRENDLINE_COLOR = 'rgba(79,138,214,0.7)';
export const DEFAULT_MARKER_COLOR = '#4F8AD6';
export const DEFAULT_RECT_COLOR = '#4F8AD6';
export const DEFAULT_VLINE_COLOR = '#4F8AD6';
export const DEFAULT_TEXT_COLOR = '#4F8AD6';
export const DEFAULT_FIB_COLOR = '#C99A4E';
// News/event badges read as editorial callouts — the product's annotation amber
// (--color-accent-primary), which stands apart from the slate-blue technical
// accents on both chart backgrounds.
export const DEFAULT_EVENT_COLOR = '#E9954A';

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
export function isEvent(a: StoredAnnotation): a is EventAnnotation {
  return a.type === 'event';
}

// --- Compact visual summary -----------------------------------------------

export interface AnnotationVisual {
  /** Display label — the agent's label/text/title, or a per-type fallback. */
  label: string;
  /** Accent color — the agent's color, or the per-type default. */
  color: string;
  /** Human-readable kind ("Price line", "Trendline", …). */
  kind: string;
  /** Optional value detail ("$317.40"); empty when not applicable. */
  detail: string;
}

function formatPrice(n: number): string {
  if (!Number.isFinite(n)) return '';
  return `$${n.toLocaleString('en-US', { maximumFractionDigits: 2 })}`;
}

/**
 * Resolve one annotation to label / color / kind / detail for compact UIs —
 * the chat card's legend, swatches and schematic thumbnail. Pure; mirrors the
 * per-type default colors used when the agent omits one.
 */
export function describeAnnotationVisual(a: StoredAnnotation): AnnotationVisual {
  if (isPriceLine(a))
    return { label: a.label || 'Price line', color: a.color || DEFAULT_LINE_COLOR, kind: 'Price line', detail: formatPrice(a.price) };
  if (isTrendline(a))
    return { label: a.label || 'Trendline', color: a.color || DEFAULT_TRENDLINE_COLOR, kind: 'Trendline', detail: '' };
  if (isMarker(a))
    return { label: a.text || 'Marker', color: a.color || DEFAULT_MARKER_COLOR, kind: 'Marker', detail: '' };
  if (isVerticalLine(a))
    return { label: a.label || 'Time marker', color: a.color || DEFAULT_VLINE_COLOR, kind: 'Vertical line', detail: '' };
  if (isRectangle(a))
    return { label: a.label || 'Zone', color: a.color || DEFAULT_RECT_COLOR, kind: 'Zone', detail: '' };
  if (isText(a))
    return { label: a.text || 'Note', color: a.color || DEFAULT_TEXT_COLOR, kind: 'Note', detail: '' };
  if (isEvent(a))
    return { label: a.title || 'Event', color: a.color || DEFAULT_EVENT_COLOR, kind: 'Event', detail: '' };
  if (isFib(a))
    return { label: a.label || 'Fib retracement', color: a.color || DEFAULT_FIB_COLOR, kind: 'Fib retracement', detail: '' };
  return { label: 'Annotation', color: DEFAULT_LINE_COLOR, kind: 'Annotation', detail: '' };
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
  // Defensive: stored payloads come from agent-generated JSONB; a row missing
  // its anchor points would otherwise throw on the .time deref below.
  if (!ann.point1 || !ann.point2) return null;
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
 *
 * ``event`` annotations are interactive DOM badges on the live chart
 * (``AgentEventOverlay``), so they're omitted here by default. The inline chat
 * mini-chart has no DOM overlay, so it passes ``eventsAsText: true`` to render
 * the event title as a non-interactive canvas chip instead.
 */
export function buildPrimitiveData(
  annotations: StoredAnnotation[],
  chartData: ChartDataPoint[] | null,
  opts?: { eventsAsText?: boolean },
): AgentAnnotationsData {
  const rects: RectItem[] = [];
  const vlines: VLineItem[] = [];
  const texts: TextItem[] = [];
  const fibs: FibItem[] = [];

  for (const ann of annotations) {
    if (isRectangle(ann)) {
      if (!ann.point1 || !ann.point2) continue;
      const t1 = resolveBarTime(chartData, ann.point1.time);
      const t2 = resolveBarTime(chartData, ann.point2.time);
      if (t1 == null || t2 == null) continue;
      // Order corners so time1 <= time2. The box is symmetric in its two
      // corners, but the off-screen clamp in the primitive assumes time1 is the
      // earlier (left) edge — reversed anchors would otherwise paint a phantom
      // span when one corner falls outside the loaded range.
      const rectEarlierFirst = t1 <= t2;
      rects.push({
        time1: rectEarlierFirst ? t1 : t2,
        time2: rectEarlierFirst ? t2 : t1,
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
      if (!ann.point1 || !ann.point2) continue;
      const t1 = resolveBarTime(chartData, ann.point1.time);
      const t2 = resolveBarTime(chartData, ann.point2.time);
      if (t1 == null || t2 == null) continue;
      const p1 = ann.point1.price;
      const p2 = ann.point2.price;
      // Level prices are tied to point identity (point1 = 1.0, point2 = 0.0),
      // so do NOT reorder them by time. Only the rendered x-span needs time1 <=
      // time2 for the primitive's off-screen clamp to be correct.
      const levels = FIB_RATIOS.map((ratio) => ({
        ratio,
        price: p2 + (p1 - p2) * ratio,
      }));
      fibs.push({
        time1: Math.min(t1, t2),
        time2: Math.max(t1, t2),
        levels,
        color: ann.color ?? DEFAULT_FIB_COLOR,
      });
    } else if (isEvent(ann) && opts?.eventsAsText) {
      // Inline chat card only: no DOM overlay there, so surface the event
      // title as a canvas chip. The live chart renders an interactive badge.
      const t = resolveBarTime(chartData, ann.time);
      if (t == null) continue;
      texts.push({
        time: t,
        price: ann.price,
        text: ann.title,
        color: ann.color ?? DEFAULT_EVENT_COLOR,
      });
    } else if (isTrendline(ann) && ann.label) {
      if (!ann.point1 || !ann.point2) continue;
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

/** A news/event annotation resolved to a drawable bar time (unix seconds). */
export interface EventItem {
  id: string;
  time: number;
  price: number;
  title: string;
  detail: string;
  color: string;
}

/**
 * Resolve ``event`` annotations to (bar-time, price) anchors for the
 * interactive DOM overlay. Items whose time can't be resolved are skipped.
 * Sorted by time so overlapping badges stack deterministically.
 */
export function buildEvents(
  annotations: StoredAnnotation[],
  chartData: ChartDataPoint[] | null,
): EventItem[] {
  const out: EventItem[] = [];
  for (const ann of annotations) {
    if (!isEvent(ann)) continue;
    const t = resolveBarTime(chartData, ann.time);
    if (t == null) continue;
    out.push({
      id: ann.annotation_id,
      time: t,
      price: ann.price,
      title: ann.title,
      detail: ann.detail,
      color: ann.color ?? DEFAULT_EVENT_COLOR,
    });
  }
  out.sort((a, b) => a.time - b.time);
  return out;
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
      shape: ann.shape,
      color: ann.color ?? DEFAULT_MARKER_COLOR,
      text: ann.text ?? '',
    });
  }
  // LWC requires markers sorted by time.
  markers.sort((a, b) => (a.time as number) - (b.time as number));
  return markers;
}
