/**
 * The chart objects the visible layers want, decided without a chart (by
 * `compileLayers`): one series per run of each layer series, the panes they
 * open, the merged price lines, the fills between series, and the notes on
 * what was folded into the chart's own indicators. `useChartLayers`
 * reconciles the live chart against it (`lwcObjects`).
 */

import type { LayerSeries } from '../../types/chartLayer';
import type { PaneLabelEntry } from '../paneLabelPrimitive';
import type { MergedAxisLine } from './axisLines';
import type { BuiltinPlan } from './builtinDuplicates';
import type { FillPoint, SnappedPoint } from './seriesData';

/** Pane key of a layer pane drawn into the built-in RSI pane. */
export const BUILTIN_RSI = 'builtin:rsi';

export interface SeriesPlan {
  s: LayerSeries;
  /** 'price', `BUILTIN_RSI`, or a layer pane's key. */
  paneKey: string;
  /** The element key a hover on any of its runs explains. */
  elKey: string;
  data: SnappedPoint[];
}

export interface PanePlan {
  title: string;
  height: number;
  entries: PaneLabelEntry[];
}

export interface FillPlan {
  /** Series keys (run 0) of the two sides; the fill rides on `a`. */
  a: string;
  b: string;
  color: string;
  points: FillPoint[];
}

export interface LayerChartPlan {
  /** By series key: the element key for run 0, `<key>#<run>` for the rest. */
  series: Map<string, SeriesPlan>;
  /** Layer panes by pane key, in the order they are first wanted. */
  panes: Map<string, PanePlan>;
  axisLines: MergedAxisLine[];
  fills: FillPlan[];
  /** What the chart's own indicators took over, per layer. */
  notes: string[];
}

export const EMPTY_PLAN: LayerChartPlan = { series: new Map(), panes: new Map(), axisLines: [], fills: [], notes: [] };

/** The pane key a layer pane's elements land in, or null when the pane is not drawn. */
export function paneKeyOf(layerKey: string, paneId: string, plan?: BuiltinPlan): string | null {
  if (paneId === 'price') return 'price';
  if (plan?.intoRsi.has(paneId)) return BUILTIN_RSI;
  if (plan?.dropPanes.has(paneId)) return null;
  return `${layerKey}|pane|${paneId}`;
}
