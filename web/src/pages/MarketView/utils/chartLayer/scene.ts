/**
 * What a layer resolves to before it is painted: every element in grid space
 * (bar index, price) with the identity hover, ask and the render report need.
 */

import type { LayerDrawing } from '../../types/chartLayer';
import type { SceneDrawing } from './kinds';
import type { IP } from './timeGrid';

/** The kinds of element a layer holds, as their keys name them. */
export type ElementKind = 'series' | 'level' | 'alert' | 'marker' | 'regime' | 'drawing';

/** Which element of which layer: the layer's row id, the element's kind and its index or id. */
export interface ElementId {
  layerKey: string;
  kind: ElementKind;
  idx: number | string;
}

/** An element's id with its string key, the form maps, hit tests and chart objects carry. */
export interface ElementHandle {
  /** Registry key, unique across all layers on the chart. */
  key: string;
  id: ElementId;
}

export function elementKey(layerKey: string, kind: ElementKind, idx: number | string): string {
  return `${layerKey}|${kind}|${idx}`;
}

export function elementHandle(layerKey: string, kind: ElementKind, idx: number | string): ElementHandle {
  return { key: elementKey(layerKey, kind, idx), id: { layerKey, kind, idx } };
}

/** What every resolved element carries for hover / ask. */
export interface SceneBase extends ElementHandle {
  /** The agent's color, or the theme's neutral when it gave none. */
  color: string;
  label?: string;
}

export interface SceneRegime extends SceneBase {
  i0: number;
  i1: number;
  fill: string;
}

/** A price-pane series' end label ("SMA 50" at its last point). */
export interface SceneSeriesLabel extends SceneBase {
  at: IP;
  text: string;
}

/** A drawing's name on the chart: its label, or a label drawing's own text. */
export function drawingLabel(d: LayerDrawing): string | undefined {
  const l = d.label?.trim();
  if (l) return l;
  if (d.type === 'label') return d.text?.trim() || undefined;
  return undefined;
}

/**
 * The grid-space box a drawing occupies, for telling whether it is in view.
 * `openLeft` / `openRight` mark a ray that runs on to the pane's edge, and
 * `anyPrice` a drawing whose price span is unbounded (a vline, an extended ray).
 */
export interface DrawingExtent {
  i0: number;
  i1: number;
  p0: number;
  p1: number;
  openLeft: boolean;
  openRight: boolean;
  anyPrice: boolean;
}

/** The box around `pts` (and `extraPrices`), open on the sides `extend` runs to. */
export function extentOf(
  pts: readonly IP[],
  extend: 'none' | 'right' | 'left' | 'both' = 'none',
  extraPrices: readonly number[] = [],
): DrawingExtent | null {
  if (!pts.length) return null;
  let i0 = Infinity;
  let i1 = -Infinity;
  let p0 = Infinity;
  let p1 = -Infinity;
  for (const pt of pts) {
    i0 = Math.min(i0, pt.i);
    i1 = Math.max(i1, pt.i);
    p0 = Math.min(p0, pt.p);
    p1 = Math.max(p1, pt.p);
  }
  for (const p of extraPrices) {
    p0 = Math.min(p0, p);
    p1 = Math.max(p1, p);
  }
  const openLeft = extend === 'left' || extend === 'both';
  const openRight = extend === 'right' || extend === 'both';
  return { i0, i1, p0, p1, openLeft, openRight, anyPrice: false };
}

/** An extent whose open ends make its price span unbounded. */
export function rayExtent(e: DrawingExtent | null): DrawingExtent | null {
  return e && { ...e, anyPrice: e.openLeft || e.openRight };
}

/** Every visible layer in grid space: its drawings, regimes and the end labels of its price-pane series. */
export interface LayerScene {
  drawings: SceneDrawing[];
  regimes: SceneRegime[];
  seriesLabels: SceneSeriesLabel[];
}

export const EMPTY_SCENE: LayerScene = { drawings: [], regimes: [], seriesLabels: [] };
