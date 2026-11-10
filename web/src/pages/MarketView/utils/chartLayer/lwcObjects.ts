/**
 * The live lightweight-charts objects a layer plan (`planLayerChart`) asks
 * for, reconciled one kind at a time: series (diffed by key so a republish
 * does not flicker) and the panes they open, the regime shading on every pane
 * below the price pane, price lines, fills, and the whitespace series that
 * extends the time scale into the future. Each call is guarded: the chart may
 * be mid-teardown, and one bad object must not take the others with it.
 */

import {
  AreaSeries,
  HistogramSeries,
  LineSeries,
  LineStyle,
  type AreaSeriesPartialOptions,
  type CreatePriceLineOptions,
  type HistogramSeriesPartialOptions,
  type IChartApi,
  type IPaneApi,
  type IPriceLine,
  type ISeriesApi,
  type LineSeriesPartialOptions,
  type LineWidth,
  type SeriesType,
  type Time,
} from 'lightweight-charts';

import { PANE_HEIGHT, type LayerSeries, type LineStyleName } from '../../types/chartLayer';
import { PaneLabelPrimitive } from '../paneLabelPrimitive';
import type { MergedAxisLine } from './axisLines';
import { fillOf } from './color';
import { LayerFillPrimitive } from './fillPrimitive';
import { fmtPrice } from './format';
import type { AxisLineBox } from './painter';
import { LayerRegimePanePrimitive } from './paneRegimes';
import { BUILTIN_RSI, EMPTY_PLAN, type FillPlan, type LayerChartPlan, type PanePlan } from './chartPlan';
import type { SceneRegime } from './scene';
import type { SnappedPoint } from './seriesData';
import type { GridAnchor } from './xMapper';

function styleToLwc(style: LineStyleName | null | undefined): LineStyle {
  if (style === 'dashed') return LineStyle.Dashed;
  if (style === 'dotted') return LineStyle.Dotted;
  return LineStyle.Solid;
}

/** Panes the chart creates itself: price (0) and RSI (1). Layer panes follow. */
const FIXED_PANES = 2;
const RSI_PANE_INDEX = 1;
/**
 * A layer pane's scale: its caption gets a line of its own above the series,
 * and only tick labels that fit whole are drawn, so none is cut in half by the
 * pane separator.
 */
const LAYER_PANE_SCALE = { scaleMargins: { top: 0.18, bottom: 0.1 }, entireTextOnly: true };

/** A layer series' LWC series, typed by what it draws. */
export type LayerLwcSeries =
  | { type: 'line'; api: ISeriesApi<'Line'> }
  | { type: 'area'; api: ISeriesApi<'Area'> }
  | { type: 'histogram'; api: ISeriesApi<'Histogram'> };

type SeriesEntry = LayerLwcSeries & { paneKey: string };

interface PaneEntry {
  pane: IPaneApi<Time>;
  label: PaneLabelPrimitive;
  /** The height last applied, so only the layer's own change re-sizes the pane. */
  height: number;
}

/** What hosts a price line: the candles, the RSI line, or a layer series. */
interface PriceLineHost {
  createPriceLine(options: CreatePriceLineOptions): IPriceLine;
  removePriceLine(line: IPriceLine): void;
}

/** Every object the layers put on one chart. */
export interface LayerObjects {
  chart: IChartApi | null;
  series: Map<string, SeriesEntry>;
  panes: Map<string, PaneEntry>;
  regimes: Map<IPaneApi<Time>, LayerRegimePanePrimitive>;
  priceLines: { host: PriceLineHost; line: IPriceLine }[];
  fills: { host: LayerLwcSeries['api']; prim: LayerFillPrimitive }[];
  future: ISeriesApi<'Line'> | null;
}

export function emptyLayerObjects(): LayerObjects {
  return { chart: null, series: new Map(), panes: new Map(), regimes: new Map(), priceLines: [], fills: [], future: null };
}

export function safe(fn: () => void): void {
  try {
    fn();
  } catch (err) {
    if (import.meta.env.DEV) console.warn('[useChartLayers]', err);
  }
}

/**
 * Start a pass on `chart`. A rebuilt chart took every object with it: forget
 * them rather than removing from an instance that no longer has them. Price
 * lines and fills are cheap and rebuilt every pass, so they go first, before
 * the series that host them can.
 */
export function beginPass(live: LayerObjects, chart: IChartApi): void {
  if (live.chart !== chart) Object.assign(live, emptyLayerObjects(), { chart });
  for (const { host, line } of live.priceLines) safe(() => host.removePriceLine(line));
  live.priceLines = [];
  for (const { host, prim } of live.fills) safe(() => host.detachPrimitive(prim));
  live.fills = [];
}

// ─── Series and panes ────────────────────────────────────────────────

type SeriesStyle =
  | { type: 'line'; options: LineSeriesPartialOptions }
  | { type: 'area'; options: AreaSeriesPartialOptions }
  | { type: 'histogram'; options: HistogramSeriesPartialOptions };

function seriesStyle(s: LayerSeries, key: string, color: string, inPricePane: boolean): SeriesStyle {
  const common = {
    priceLineVisible: false,
    // A price-pane line is labelled at its end on the canvas; a pane of its own
    // reads its value off the axis, as RSI does, and names it in the pane's
    // caption rather than in a pill beside every value.
    lastValueVisible: !inPricePane,
    title: '',
  };
  if (s.type === 'histogram') {
    return {
      type: 'histogram',
      options: {
        ...common,
        color,
        // In the price pane a histogram rides its own scale along the bottom,
        // like volume, rather than squashing the candles to fit.
        ...(inPricePane ? { priceScaleId: `layer:${key}` } : {}),
      },
    };
  }
  const shared = {
    ...common,
    // LWC types a width as 1–4 but draws a fractional one as given.
    lineWidth: Math.max(1, Math.min(4, s.width ?? 1.5)) as LineWidth,
    lineStyle: styleToLwc(s.style),
    crosshairMarkerVisible: !inPricePane,
  };
  if (s.type === 'area') {
    return { type: 'area', options: { ...shared, lineColor: color, topColor: fillOf(color, 0.22), bottomColor: fillOf(color, 0.02) } };
  }
  return { type: 'line', options: { ...shared, color } };
}

function addSeries(chart: IChartApi, style: SeriesStyle, paneIndex: number): LayerLwcSeries {
  switch (style.type) {
    case 'area':
      return { type: 'area', api: chart.addSeries(AreaSeries, style.options, paneIndex) };
    case 'histogram':
      return { type: 'histogram', api: chart.addSeries(HistogramSeries, style.options, paneIndex) };
    case 'line':
      return { type: 'line', api: chart.addSeries(LineSeries, style.options, paneIndex) };
  }
}

function restyle(e: LayerLwcSeries, style: SeriesStyle): void {
  if (e.type === style.type) e.api.applyOptions(style.options);
}

function setSeriesData(e: LayerLwcSeries, pts: readonly SnappedPoint[]): void {
  const base = (pt: SnappedPoint) => ({ time: pt.time as Time, value: pt.value });
  if (e.type === 'area') {
    e.api.setData(pts.map((pt) => (pt.color ? { ...base(pt), lineColor: pt.color } : base(pt))));
  } else {
    e.api.setData(pts.map((pt) => (pt.color ? { ...base(pt), color: pt.color } : base(pt))));
  }
}

function removeSeries(chart: IChartApi, e: LayerLwcSeries): void {
  safe(() => chart.removeSeries(e.api));
}

/** Open a layer pane at `paneIndex`, sized to the share of the chart the layer asked for. */
/**
 * Size the layer panes together, each to its `height` as a fraction of the
 * chart, against the stretch of the panes they do not own (price, RSI). Sized
 * one at a time, each new pane would shrink the ones sized before it.
 */
function fitPanes(chart: IChartApi, live: LayerObjects): void {
  const own = [...live.panes.values()];
  if (own.length === 0) return;
  const mine = new Set(own.map((e) => e.pane.paneIndex()));
  const rest = chart.panes().reduce((sum, p, k) => (mine.has(k) ? sum : sum + p.getStretchFactor()), 0);
  const hs = own.map((e) => Math.max(PANE_HEIGHT.min, Math.min(PANE_HEIGHT.max, e.height)));
  const total = hs.reduce((a, b) => a + b, 0);
  // The schema keeps the lower panes under `lowerTotal`; this only guards the division.
  const scale = total > PANE_HEIGHT.lowerTotal ? PANE_HEIGHT.lowerTotal / total : 1;
  own.forEach((e, k) => e.pane.setStretchFactor(((hs[k] * scale) / (1 - total * scale)) * rest));
}

function openPane(chart: IChartApi, paneIndex: number, want: PanePlan, color: string, font: string): PaneEntry {
  const pane = chart.panes()[paneIndex];
  // Kept until closePane removes it: lightweight-charts drops a pane with its
  // last series, so replacing a pane's only series (a new type) would leave this
  // handle on a pane that is gone, and the replacement with nowhere to go.
  pane.setPreserveEmptyPane(true);
  const label = new PaneLabelPrimitive(want.title, color, font);
  pane.attachPrimitive(label);
  return { pane, label, height: want.height };
}

function closePane(chart: IChartApi, live: LayerObjects, paneKey: string, entry: PaneEntry): void {
  safe(() => entry.pane.detachPrimitive(entry.label));
  const regimes = live.regimes.get(entry.pane);
  if (regimes) {
    safe(() => entry.pane.detachPrimitive(regimes));
    live.regimes.delete(entry.pane);
  }
  for (const [key, s] of live.series) {
    if (s.paneKey === paneKey) {
      removeSeries(chart, s);
      live.series.delete(key);
    }
  }
  safe(() => {
    const idx = entry.pane.paneIndex();
    if (idx >= FIXED_PANES && idx < chart.panes().length) chart.removePane(idx);
  });
  live.panes.delete(paneKey);
}

function labelFontOf(chart: IChartApi): string {
  try {
    return `12px ${getComputedStyle(chart.chartElement()).fontFamily || 'sans-serif'}`;
  } catch {
    return '12px sans-serif';
  }
}

/**
 * Series and layer panes: drop what went or moved, then create or update the
 * rest. Returns each LWC series' element key, to explain a hovered series.
 */
export function reconcileSeries(
  chart: IChartApi,
  live: LayerObjects,
  plan: LayerChartPlan,
  colors: { neutral: string; muted: string },
): Map<ISeriesApi<SeriesType>, string> {
  for (const [key, entry] of live.series) {
    const want = plan.series.get(key);
    if (!want || want.s.type !== entry.type || want.paneKey !== entry.paneKey) {
      removeSeries(chart, entry);
      live.series.delete(key);
    }
  }
  // Panes opened or closed, or a height changed: the layer panes are sized again together.
  let resize = false;
  for (const [paneKey, entry] of live.panes) {
    if (!plan.panes.has(paneKey)) {
      closePane(chart, live, paneKey, entry);
      resize = true;
    }
  }

  const font = labelFontOf(chart);
  const keys = new Map<ISeriesApi<SeriesType>, string>();
  for (const [key, { s, paneKey, elKey, data }] of plan.series) {
    const inPrice = paneKey === 'price';
    const style = seriesStyle(s, key, s.color ?? colors.neutral, inPrice);
    let entry = live.series.get(key);
    if (!entry) {
      safe(() => {
        let paneIndex = 0;
        if (paneKey === BUILTIN_RSI) {
          if (chart.panes().length <= RSI_PANE_INDEX) return;
          paneIndex = RSI_PANE_INDEX;
        } else if (!inPrice) {
          const pe = live.panes.get(paneKey);
          paneIndex = pe ? pe.pane.paneIndex() : Math.max(FIXED_PANES, chart.panes().length);
        }
        const created: SeriesEntry = { ...addSeries(chart, style, paneIndex), paneKey };
        entry = created;
        live.series.set(key, created);
        const ownPane = !inPrice && paneKey !== BUILTIN_RSI;
        if (ownPane) created.api.priceScale().applyOptions(LAYER_PANE_SCALE);
        if (ownPane && !live.panes.has(paneKey)) {
          live.panes.set(paneKey, openPane(chart, paneIndex, plan.panes.get(paneKey)!, colors.muted, font));
          resize = true;
        }
        if (inPrice && s.type === 'histogram') {
          created.api.priceScale().applyOptions({ scaleMargins: { top: 0.8, bottom: 0 } });
        }
      });
    } else {
      const e = entry;
      safe(() => restyle(e, style));
    }
    if (!entry) continue;
    const e: SeriesEntry = entry;
    safe(() => setSeriesData(e, data));
    keys.set(e.api, elKey);
  }

  for (const [paneKey, pe] of live.panes) {
    const want = plan.panes.get(paneKey);
    if (want) {
      if (want.height !== pe.height) {
        pe.height = want.height;
        resize = true;
      }
      pe.label.setText(want.title);
      // A one-series pane named after its series needs no entry repeating it.
      const lone = want.entries.length === 1 && want.entries[0].text.startsWith(want.title);
      pe.label.setEntries(lone ? [] : want.entries);
    }
    pe.label.setColor(colors.muted);
  }
  // Otherwise left alone, so a pane the user dragged to another size keeps it
  // through every update that does not change the layer panes.
  if (resize) safe(() => fitPanes(chart, live));
  return keys;
}

// ─── Regimes ─────────────────────────────────────────────────────────

/** Regime shading on every pane below the price pane (whose own shading is the layer primitive's). */
export function reconcileRegimes(
  chart: IChartApi,
  live: LayerObjects,
  regimes: readonly SceneRegime[],
  anchor: GridAnchor | null,
): void {
  const panes = regimes.length ? chart.panes().slice(1) : [];
  const keep = new Set(panes);
  for (const [pane, prim] of live.regimes) {
    if (keep.has(pane)) continue;
    safe(() => pane.detachPrimitive(prim));
    live.regimes.delete(pane);
  }
  for (const pane of panes) {
    let prim = live.regimes.get(pane);
    if (!prim) {
      const created = new LayerRegimePanePrimitive();
      safe(() => {
        pane.attachPrimitive(created);
        live.regimes.set(pane, created);
      });
      prim = live.regimes.get(pane);
    }
    prim?.setRegimes(regimes, anchor);
  }
}

// ─── Price lines ─────────────────────────────────────────────────────

/**
 * One price line per merged level/alert, on the series of its pane. Returns the
 * price-pane lines (labels keep off their names) and a note per merge.
 */
export function reconcilePriceLines(
  chart: IChartApi,
  live: LayerObjects,
  candles: PriceLineHost,
  lines: readonly MergedAxisLine[],
): { boxes: AxisLineBox[]; notes: string[] } {
  const boxes: AxisLineBox[] = [];
  const notes: string[] = [];
  const hostOf = (paneKey: string): PriceLineHost | null => {
    if (paneKey === 'price') return candles;
    if (paneKey === BUILTIN_RSI) {
      try {
        return chart.panes()[RSI_PANE_INDEX]?.getSeries()[0] ?? null;
      } catch {
        return null;
      }
    }
    for (const s of live.series.values()) if (s.paneKey === paneKey) return s.api;
    return null;
  };
  for (const m of lines) {
    const host = hostOf(m.host);
    if (!host) continue;
    if (m.members.length > 1) {
      const what = m.members.map((x) => `${x.alert ? 'alert' : 'level'} ${x.title ? `'${x.title}'` : fmtPrice(x.price)}`);
      notes.push(`axis labels merged at ${fmtPrice(m.price)}: ${[...new Set(what)].join(' + ')}`);
    }
    if (m.host === 'price') boxes.push({ price: m.price, title: m.title, els: m.members });
    safe(() => {
      const line = host.createPriceLine({
        id: m.key,
        price: m.price,
        color: m.color,
        lineWidth: 1,
        lineStyle: m.style === 'sparse' ? LineStyle.SparseDotted : styleToLwc(m.style),
        axisLabelVisible: true,
        title: m.title,
      });
      live.priceLines.push({ host, line });
    });
  }
  return { boxes, notes };
}

// ─── Fills ───────────────────────────────────────────────────────────

/** The area between two series of a pane, on the series that draw them. */
export function reconcileFills(live: LayerObjects, fills: readonly FillPlan[]): void {
  for (const f of fills) {
    const a = live.series.get(f.a);
    const b = live.series.get(f.b);
    if (!a || !b) continue;
    const prim = new LayerFillPrimitive();
    safe(() => {
      a.api.attachPrimitive(prim);
      prim.setFill(f.points, f.color, b.api);
      live.fills.push({ host: a.api, prim });
    });
  }
}

// ─── Future room ─────────────────────────────────────────────────────

/**
 * A whitespace-only series past the last bar, so forward drawings have
 * somewhere to be. Its own series, never padding on the candles: the bar store
 * and every consumer of the candle data stay whitespace free.
 */
export function reconcileFuture(chart: IChartApi, live: LayerObjects, future: readonly number[]): void {
  if (future.length) {
    if (!live.future) {
      safe(() => {
        live.future = chart.addSeries(
          LineSeries,
          { color: 'rgba(0,0,0,0)', lastValueVisible: false, priceLineVisible: false, crosshairMarkerVisible: false },
          0,
        );
      });
    }
    safe(() => live.future?.setData(future.map((t) => ({ time: t as Time }))));
  } else if (live.future) {
    const fs = live.future;
    safe(() => chart.removeSeries(fs));
    live.future = null;
  }
}

// ─── Report and teardown ─────────────────────────────────────────────

/** Pane titles top to bottom, as the render report names them. */
export function paneTitles(chart: IChartApi, live: LayerObjects, plan: LayerChartPlan, rsiPeriod: number | null): string[] {
  return chart.panes().map((pane, k) => {
    if (k === 0) return 'Price';
    if (k === RSI_PANE_INDEX) return rsiPeriod != null ? `RSI (${rsiPeriod})` : 'RSI';
    for (const [key, pe] of live.panes) if (pe.pane === pane) return plan.panes.get(key)?.title ?? '';
    return '';
  });
}

/** Reconciling against nothing: only the title and colors of what no longer exists, so none are read. */
const NO_COLORS = { neutral: '', muted: '' };

/**
 * Drop everything the layers added to a chart that outlives them, by
 * reconciling it against an empty plan: the same teardown a layer going away
 * gets, so nothing this module attaches can outlast the hook. On a page
 * unmount the chart may already be gone, which each step's guard absorbs.
 */
export function removeLayerObjects(live: LayerObjects): void {
  const chart = live.chart;
  if (!chart) return;
  beginPass(live, chart);
  reconcileSeries(chart, live, EMPTY_PLAN, NO_COLORS);
  reconcileRegimes(chart, live, [], null);
  reconcileFuture(chart, live, []);
}
