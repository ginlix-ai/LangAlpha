/**
 * The visible layers compiled once, element kind by element kind, into
 * everything the chart needs: the lightweight-charts objects (`plan`), the
 * canvas scene, the markers for the one markers plugin, the marker boxes the
 * painter keeps labels off, and the explain/report registry. One walk, so the
 * rules that decide what an element is (a skipped series, an undeclared pane,
 * a marker past the last bar) cannot drift between its uses. `useChartLayers`
 * reconciles the live chart against the result without a chart in sight here.
 *
 * Elements whose anchors cannot be placed are skipped, never thrown on: a
 * newer server may send a drawing type this build does not know.
 */

import type { SeriesMarker, Time } from 'lightweight-charts';

import type { StoredChartLayer } from '../../types/chartLayer';
import { mergeAxisLines, type AxisLineSpec } from './axisLines';
import { planBuiltinDuplicates, type BuiltinIndicators } from './builtinDuplicates';
import { BUILTIN_RSI, EMPTY_PLAN, paneKeyOf, type FillPlan, type LayerChartPlan, type PanePlan, type SeriesPlan } from './chartPlan';
import { parseColor, withAlpha } from '../canvasKit';
import { fillOf } from './color';
import { describe, ElementRegistry, type ElementEntry, type LayerElements } from './elements';
import { fmtPaneValue } from './format';
import { drawingKind, type SceneDrawing } from './kinds';
import type { LabelScene } from './kinds/label';
import type { LayerTheme } from './layerTheme';
import type { MarkerBox } from './painter';
import {
  EMPTY_SCENE,
  drawingLabel,
  elementHandle,
  elementKey,
  type ElementKind,
  type LayerScene,
  type SceneBase,
  type SceneRegime,
  type SceneSeriesLabel,
} from './scene';
import { fillBetween, seriesRuns, snapSeriesData, type SnappedPoint } from './seriesData';
import { layerTimeToSeconds, makeTimeGrid, pointToIP, resolveIndex, snapToGrid, timeToIndex, type TimeGrid } from './timeGrid';

export interface CompiledLayers {
  plan: LayerChartPlan;
  scene: LayerScene;
  /** For `useChartOverlays`, which must own the whole markers list. */
  markers: SeriesMarker<Time>[];
  markerBoxes: MarkerBox[];
  elements: LayerElements;
}

export const EMPTY_COMPILED: CompiledLayers = {
  plan: EMPTY_PLAN,
  scene: EMPTY_SCENE,
  markers: [],
  markerBoxes: [],
  elements: { explain: new Map(), names: new Map() },
};

const DEFAULT_PANE_HEIGHT = 0.2;
const MARKER_SHAPES = new Set(['arrowUp', 'arrowDown', 'circle', 'square']);

export function compileLayers(
  layers: readonly StoredChartLayer[],
  grid: TimeGrid,
  builtins: BuiltinIndicators,
  th: LayerTheme,
): CompiledLayers {
  const series = new Map<string, SeriesPlan>();
  const panes = new Map<string, PanePlan>();
  const specs: AxisLineSpec[] = [];
  const fills: FillPlan[] = [];
  const notes: string[] = [];
  const drawings: SceneDrawing[] = [];
  const regimes: SceneRegime[] = [];
  const seriesLabels: SceneSeriesLabel[] = [];
  const placed: { t: number; marker: SeriesMarker<Time>; box: MarkerBox | null }[] = [];
  const futureMarkers: LabelScene[] = [];
  const registry = new ElementRegistry();

  const { bars } = grid;
  const lastBar = bars[bars.length - 1];
  // A marker sits on a bar, never in the future room past the last one.
  const barGrid = makeTimeGrid(bars, [], grid.step);

  for (const stored of layers) {
    const lk = stored.layer_id;
    const { layer } = stored;
    const builtin = planBuiltinDuplicates(layer, builtins);
    for (const note of builtin.notes) notes.push(`${layer.id}: ${note}`);
    const add = (kind: ElementKind, idx: number | string, e: ElementEntry) => registry.add(stored, kind, idx, e);

    // ── Series, the panes they open, and their end labels ──
    const declared = new Map(layer.panes.map((p) => [p.id, p]));
    const runsOf = new Map<string, SnappedPoint[][]>();
    for (const s of layer.series) {
      add('series', s.id, describe.series(s));
      if (builtin.skipSeries.has(s.id)) continue;
      const paneId = s.pane || 'price';
      if (paneId !== 'price' && !declared.has(paneId)) continue;
      const paneKey = paneKeyOf(lk, paneId, builtin);
      if (!paneKey) continue;
      const elKey = elementKey(lk, 'series', s.id);
      const runs = seriesRuns(s, snapSeriesData(grid, s.data), grid);
      if (!runs.length) continue;
      runsOf.set(s.id, runs);
      runs.forEach((data, r) => series.set(r === 0 ? elKey : `${elKey}#${r}`, { s, paneKey, elKey, data }));
      const lastRun = runs[runs.length - 1];
      const end = lastRun[lastRun.length - 1];
      if (paneKey === 'price') {
        const i = resolveIndex(grid, end.time);
        if (s.title && i != null) {
          seriesLabels.push({
            ...elementHandle(lk, 'series', s.id),
            color: s.color ?? th.neutral,
            label: s.title,
            at: { i, p: end.value },
            text: s.title,
          });
        }
      } else if (paneKey !== BUILTIN_RSI) {
        let pane = panes.get(paneKey);
        if (!pane) {
          const p = declared.get(paneId)!;
          pane = { title: p.title || s.title, height: p.height ?? DEFAULT_PANE_HEIGHT, entries: [] };
          panes.set(paneKey, pane);
        }
        // The value named is the last one, so it wears that point's colour: a
        // histogram coloured by sign reads green or red here as on its axis
        // tag, and opaque as the tag is, since a bar's tint is too faint for text.
        // A point made fully transparent is drawn as nothing, so it lends no colour.
        const hue = end.color && parseColor(end.color)?.a !== 0 ? withAlpha(end.color, 1) : s.color ?? th.neutral;
        pane.entries.push({ text: `${s.title} ${fmtPaneValue(end.value)}`, color: hue });
      }
    }

    // ── Levels and alerts: one price line per price ──
    layer.levels.forEach((lv, idx) => {
      add('level', idx, describe.level(lv, idx, layer));
      const host = paneKeyOf(lk, lv.pane || 'price', builtin);
      if (!host || !Number.isFinite(lv.price)) return;
      specs.push({
        ...elementHandle(lk, 'level', idx),
        host,
        price: lv.price,
        color: lv.color ?? th.neutral,
        style: lv.style ?? 'dashed',
        title: lv.title ?? '',
        alert: null,
      });
    });
    layer.alerts.forEach((al, idx) => {
      add('alert', idx, describe.alert(al, idx, layer));
      const c = al.condition;
      if (!Number.isFinite(c.level)) return;
      specs.push({
        ...elementHandle(lk, 'alert', idx),
        host: 'price',
        price: c.level,
        color: th.warning,
        style: 'sparse',
        title: al.title,
        alert: c.direction,
      });
    });

    // ── Markers: on a bar for the markers plugin, a label drawing past the last bar ──
    layer.markers.forEach((m, idx) => {
      add('marker', idx, describe.marker(m, idx));
      const secs = layerTimeToSeconds(m.time);
      if (!bars.length || secs == null || !MARKER_SHAPES.has(m.shape)) return;
      const el = elementHandle(lk, 'marker', idx);
      const color = m.color ?? th.neutral;
      const price = m.price != null && Number.isFinite(m.price) ? m.price : null;
      if (secs > lastBar + grid.step / 2) {
        // No candle to sit on: a priced one becomes a label at its point, an unpriced one is dropped.
        const i = resolveIndex(grid, secs);
        if (i == null || price == null) return;
        futureMarkers.push({
          ...el,
          color,
          label: m.text?.trim() || undefined,
          kind: 'label',
          at: { i, p: price },
          text: m.text?.trim() || '•',
          position: m.position === 'belowBar' ? 'below' : 'above',
        });
        return;
      }
      const t = snapToGrid(barGrid, secs);
      if (t == null) return;
      const common = { id: el.key, time: t as Time, shape: m.shape, color, text: m.text ?? '' };
      const marker: SeriesMarker<Time> =
        m.position === 'atPrice' && price != null
          ? { ...common, position: 'atPriceMiddle', price }
          : { ...common, position: m.position === 'atPrice' ? 'inBar' : m.position };
      const i = resolveIndex(grid, t);
      placed.push({
        t,
        marker,
        box: i == null ? null : { i, position: marker.position as MarkerBox['position'], price: 'price' in marker ? marker.price : undefined, text: common.text },
      });
    });

    // ── Regimes ──
    layer.regimes.forEach((r, idx) => {
      add('regime', idx, describe.regime(r, idx));
      const i0 = timeToIndex(grid, r.from);
      const i1 = timeToIndex(grid, r.to);
      if (i0 == null || i1 == null || !r.color) return;
      regimes.push({
        ...elementHandle(lk, 'regime', idx),
        color: r.color,
        label: r.label?.trim() || undefined,
        // `to` is the run's last bar, inclusive: shade to the bars' outer edges.
        i0: Math.min(i0, i1) - 0.5,
        i1: Math.max(i0, i1) + 0.5,
        fill: fillOf(r.color, 0.08, 0.1),
      });
    });

    // ── Fills between two series of one pane, on the runs the series draw ──
    for (const f of layer.fills) {
      const a = series.get(elementKey(lk, 'series', f.a));
      const b = series.get(elementKey(lk, 'series', f.b));
      const ra = runsOf.get(f.a);
      const rb = runsOf.get(f.b);
      if (!a || !b || !ra || !rb || a.paneKey !== b.paneKey || !f.color) continue;
      const points = fillBetween(ra, rb);
      if (points.length < 2) continue;
      fills.push({ a: a.elKey, b: b.elKey, color: fillOf(f.color, 0.12), points });
    }

    // ── Drawings, each through its kind's record ──
    layer.drawings.forEach((d, idx) => {
      const kind = drawingKind(d.type);
      add('drawing', idx, describe.drawing(d, idx, kind));
      if (!kind) return;
      const base: SceneBase = {
        ...elementHandle(lk, 'drawing', idx),
        color: d.color ?? th.neutral,
        label: drawingLabel(d),
      };
      const scene = kind.resolve(d, { grid, base, P: (pt) => pointToIP(grid, pt) });
      if (scene) drawings.push(scene);
    });
  }

  placed.sort((a, b) => a.t - b.t);
  return {
    plan: { series, panes, axisLines: mergeAxisLines(specs), fills, notes },
    scene: { drawings: [...drawings, ...futureMarkers], regimes, seriesLabels },
    markers: placed.map((p) => p.marker),
    markerBoxes: placed.flatMap((p) => (p.box ? [p.box] : [])),
    elements: { explain: registry.explain, names: registry.names },
  };
}
