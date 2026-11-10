/**
 * The render report a snapshot reader (the agent) gets beside the picture:
 * the view, labels the layout hid, elements out of view, names cut short,
 * labels under the chart's own text, marker captions over candles, forecast
 * verdicts, and the notes. Built from a painted frame's own passes (geometry
 * through the painter, no layout of its own), and only when the chart was
 * given a reporter (the snapshot page): the live app never computes it.
 */

import { MismatchDirection, type ISeriesApi, type SeriesType } from 'lightweight-charts';

import type { ChartRenderReport } from '../../types/chartRenderReport';
import { cutAxisTitles, type MergedAxisLine } from './axisLines';
import type { LabelReq, Rect } from './canvasLabels';
import type { ElementName } from './elements';
import { drawingKind } from './kinds';
import { forecastVerdict, type ForecastScene } from './kinds/forecast';
import { axisTagBox, type FrameObstacles } from './obstacles';
import type { PaintContext, Painter } from './painter';
import type { ElementHandle, LayerScene } from './scene';
import { indexToSeconds, secondsToLayerTime, type TimeGrid } from './timeGrid';

/** One frame's report; the snapshot page numbers the frames. */
export type LayerFrameStats = Omit<ChartRenderReport, 'frame'>;

/** Called after each painted frame with its report, or null once no layer is on the chart. */
export type LayerFrameReporter = (stats: LayerFrameStats | null) => void;

/** Text the chart draws at the price pane's right edge, beside a price's axis label. */
export interface PaneTag {
  price: number;
  title: string;
}

/** An HTML chip over the pane (the legend's), in pane px, with the title it shows. */
export interface PaneChip extends Rect {
  /** The layer whose title it shows, or null for a chip without one (the folded "N layers"). */
  layer: string | null;
  title: string | null;
  /** What the chip shows of a title it cuts short, ellipsis included; null when it shows it whole. */
  shown: string | null;
}

/** What the report names that the painter does not need. */
export interface ReportContext {
  grid: TimeGrid;
  names: ReadonlyMap<string, ElementName>;
  layers: readonly { id: string; title: string }[];
  panes: readonly string[];
  notes: readonly string[];
  /** Every merged price line, whichever pane, for the names their axis tags cut short. */
  axisLines: readonly MergedAxisLine[];
  /** The price pane's tags besides the layer's own lines (the candles' "Close"); read each frame. */
  paneTags: () => readonly PaneTag[];
  /** The legend's chips over the pane; read each frame. */
  paneChips: () => readonly PaneChip[];
}

export function frameStats({
  report,
  painter,
  scene,
  paint,
  view,
  obstacles,
  dropped,
  lastIndex,
}: {
  report: ReportContext;
  painter: Painter;
  scene: LayerScene;
  paint: PaintContext;
  view: { from: number; to: number };
  /** The boxes the layout kept labels off, so the report names what it measured. */
  obstacles: FrameObstacles;
  dropped: readonly LabelReq[];
  lastIndex: number;
}): LayerFrameStats {
  const { grid } = report;
  const nameOf = (key: string, fallback: string): ElementName =>
    report.names.get(key) ?? { layer: fallback, element: key, kind: 'element' };
  const name = (el: ElementHandle): ElementName => nameOf(el.key, el.id.layerKey);
  const timeAt = (idx: number): string => {
    const secs = indexToSeconds(grid, idx, 'slot');
    return secs == null ? '' : secondsToLayerTime(secs, grid.step >= 86400);
  };
  // Whether a price (or a span of them) lies wholly above or below the pane.
  const priceOffscreen = (p0: number, p1 = p0): boolean => {
    const y0 = painter.Y(p0);
    const y1 = painter.Y(p1);
    if (y0 == null || y1 == null) return false;
    return Math.max(y0, y1) < 0 || Math.min(y0, y1) > painter.h;
  };

  const droppedLabels: LayerFrameStats['droppedLabels'] = uniqueBy(dropped, (r) => `${r.el.key}\u0000${r.text}`).map((r) => {
    const n = name(r.el);
    return { layer: n.layer, element: n.element, text: r.text };
  });

  const offscreen: LayerFrameStats['offscreen'] = [];
  const off = (el: ElementHandle) => {
    const n = name(el);
    offscreen.push({ layer: n.layer, element: n.element, kind: n.kind });
  };
  for (const d of scene.drawings) {
    const e = drawingKind(d.kind)?.extent(d);
    if (!e) continue;
    const out =
      (!e.openRight && e.i1 < view.from) ||
      (!e.openLeft && e.i0 > view.to) ||
      (!e.anyPrice && priceOffscreen(e.p0, e.p1));
    if (out) off(d);
  }
  for (const r of scene.regimes) if (Math.max(r.i0, r.i1) < view.from || Math.min(r.i0, r.i1) > view.to) off(r);
  for (const line of paint.axisLines) if (priceOffscreen(line.price)) for (const el of line.els) off(el);

  const chips = report.paneChips();

  const cutAxis = uniqueBy(
    report.axisLines.flatMap((line) => cutAxisTitles(line).map((m) => ({ m, line }))),
    ({ m }) => m.key,
  );
  const cutText: LayerFrameStats['cutText'] = cutAxis.map(({ m, line }) => {
    const n = name(m);
    return { layer: n.layer, element: n.element, where: 'axis', text: m.title.trim(), shown: line.title };
  });
  for (const c of chips) {
    if (c.title && c.shown != null) cutText.push({ layer: c.layer ?? '', element: 'title', where: 'title', text: c.title, shown: c.shown });
  }

  const tagBoxes: { title: string; box: Rect }[] = [];
  for (const tag of report.paneTags()) {
    const box = axisTagBox(painter, tag);
    if (box) tagBoxes.push({ title: tag.title, box });
  }
  const covers = painter.hits.flatMap((h) => {
    if (h.t !== 'rect' || !h.label) return [];
    const box = { left: Math.min(h.x0, h.x1), top: Math.min(h.y0, h.y1), width: Math.abs(h.x1 - h.x0), height: Math.abs(h.y1 - h.y0) };
    return [
      ...chips.filter((c) => intersects(box, c)).map((c) => ({ key: h.key, by: 'chip' as const, byText: c.title ?? '' })),
      ...tagBoxes.filter((t) => intersects(box, t.box)).map((t) => ({ key: h.key, by: 'tag' as const, byText: t.title })),
    ];
  });
  const coveredLabels: LayerFrameStats['coveredLabels'] = uniqueBy(covers, (c) => `${c.key}\u0000${c.by}\u0000${c.byText}`).map(
    ({ key, by, byText }) => {
      const n = nameOf(key, '');
      return { layer: n.layer, element: n.element, kind: n.kind, by, byText };
    },
  );

  const overCandles = obstacles.markers.filter((m) => m.caption && obstacles.candles.some((c) => intersects(m.caption!, c)));
  const markersOverCandles: LayerFrameStats['markersOverCandles'] = uniqueBy(overCandles, (m) => m.marker.text).map((m) => ({
    text: m.marker.text,
  }));

  const forecasts: LayerFrameStats['forecasts'] = [];
  for (const d of scene.drawings) {
    if (d.kind !== 'forecast') continue;
    const f = d as ForecastScene;
    const v = forecastVerdict(f.origin, f.target, painter.bars);
    const n = name(f);
    forecasts.push({
      layer: n.layer,
      element: n.element,
      label: f.label ?? null,
      state: v.state,
      at: timeAt(v.state === 'hit' ? v.i : f.target.i),
    });
  }

  return {
    view: {
      from: timeAt(view.from),
      to: timeAt(view.to),
      futureSlots: Math.max(0, Math.floor(view.to) - lastIndex),
      width: Math.round(painter.w),
      height: Math.round(painter.h),
      panes: [...report.panes],
    },
    layers: report.layers.map((l) => ({ id: l.id, title: l.title })),
    droppedLabels,
    offscreen,
    cutText,
    coveredLabels,
    markersOverCandles,
    forecasts,
    notes: [...report.notes],
  };
}

/** `items` without a second one under the same key, the first kept. */
function uniqueBy<T>(items: readonly T[], key: (item: T) => string): T[] {
  const seen = new Set<string>();
  return items.filter((item) => {
    const k = key(item);
    if (seen.has(k)) return false;
    seen.add(k);
    return true;
  });
}

function intersects(a: Rect, b: Rect): boolean {
  return a.left < b.left + b.width && b.left < a.left + a.width && a.top < b.top + b.height && b.top < a.top + a.height;
}

/**
 * The candle series' own tags in the pane: its title at the last price ("Close",
 * "Pre") and every price line's name. The layer's lines are among them; labels
 * already keep off those, so they show up only under a must-show label.
 */
export function readPaneTags(series: ISeriesApi<SeriesType>): PaneTag[] {
  const out: PaneTag[] = [];
  const title = series.options().title;
  if (title) {
    const last = series.dataByIndex(Number.MAX_SAFE_INTEGER, MismatchDirection.NearestLeft) as
      | { close?: number; value?: number }
      | null;
    const price = last?.close ?? last?.value;
    if (price != null && Number.isFinite(price)) out.push({ price, title });
  }
  for (const line of series.priceLines()) {
    const o = line.options();
    // LWC draws a line's name in the pane only with its axis label.
    if (o.title && o.axisLabelVisible) out.push({ price: o.price, title: o.title });
  }
  return out;
}
