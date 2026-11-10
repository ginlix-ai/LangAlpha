/**
 * What the explain card and "Ask about this" need for the elements worth
 * explaining, and the name the render report gives each one. The compile walk
 * (`compileLayers`) registers every element it visits.
 */

import type {
  ChartLayer,
  LayerAlert,
  LayerDrawing,
  LayerElementRef,
  LayerLevel,
  LayerMarker,
  LayerPoint,
  LayerRegime,
  LayerSeries,
  StoredChartLayer,
} from '../../types/chartLayer';
import { UNKNOWN_KIND_NAME, type AnyDrawingKind } from './kinds';
import { drawingLabel, elementKey, type ElementKind } from './scene';

/** Everything the explain popover and "Ask about this" need for one element. */
export interface LayerElementInfo {
  key: string;
  ref: LayerElementRef;
  color: string | null;
  /** The element's own label, when it has one; the card falls back on its type's name. */
  label: string | null;
  /** i18n key of the element's type name, for the card's kicker. */
  typeKey: string;
  layerSummary: string | null;
  symbol: string;
  timeframe: string;
}

/** How an element is named in the render report: its layer's id, its own id or index, and its kind. */
export interface ElementName {
  layer: string;
  element: string;
  kind: string;
}

export interface LayerElements {
  /** Elements worth explaining (a note, a label, or a readout of their own), by key. */
  explain: Map<string, LayerElementInfo>;
  /** Every element's report name, by key. */
  names: Map<string, ElementName>;
}

const TYPE_KEYS: Record<Exclude<ElementKind, 'drawing'>, string> = {
  series: 'marketView.layerExplain.types.series',
  level: 'marketView.layerExplain.types.level',
  alert: 'marketView.layerExplain.types.alert',
  marker: 'marketView.layerExplain.types.marker',
  regime: 'marketView.layerExplain.types.regime',
};

/** What one element tells the registry: how the report names it and what its explain card shows. */
export interface ElementEntry {
  name: string;
  reportKind: string;
  type: string;
  typeKey: string;
  label: string | null | undefined;
  note: string | null | undefined;
  color: string | null | undefined;
  anchors: LayerPoint[];
  always?: boolean;
}

/** How each kind of element is named and explained. */
export const describe = {
  series(s: LayerSeries): ElementEntry {
    const last = s.data[s.data.length - 1];
    return {
      name: s.id,
      reportKind: 'series',
      type: 'series',
      typeKey: TYPE_KEYS.series,
      label: s.title,
      note: s.note,
      color: s.color,
      anchors: last ? [{ time: last.time, price: last.value }] : [],
    };
  },
  level(lv: LayerLevel, k: number, layer: ChartLayer): ElementEntry {
    return {
      name: lv.title?.trim() || `levels[${k}]`,
      reportKind: 'level',
      type: 'level',
      typeKey: TYPE_KEYS.level,
      label: lv.title,
      note: lv.note,
      color: lv.color,
      anchors: [{ time: layer.as_of ?? '', price: lv.price }],
    };
  },
  alert(al: LayerAlert, k: number, layer: ChartLayer): ElementEntry {
    return {
      name: al.id || `alerts[${k}]`,
      reportKind: 'alert',
      type: 'alert',
      typeKey: TYPE_KEYS.alert,
      label: al.title,
      note: al.note,
      color: null,
      anchors: [{ time: layer.as_of ?? '', price: al.condition.level }],
      always: true,
    };
  },
  marker(m: LayerMarker, k: number): ElementEntry {
    return {
      name: `markers[${k}]`,
      reportKind: 'marker',
      type: 'marker',
      typeKey: TYPE_KEYS.marker,
      label: m.text,
      note: m.note,
      color: m.color,
      anchors: [{ time: m.time, price: m.price ?? NaN }],
    };
  },
  regime(r: LayerRegime, k: number): ElementEntry {
    return {
      name: r.label?.trim() || `regimes[${k}]`,
      reportKind: 'regime',
      type: 'regime',
      typeKey: TYPE_KEYS.regime,
      label: r.label,
      note: r.note,
      color: r.color,
      anchors: [
        { time: r.from, price: NaN },
        { time: r.to, price: NaN },
      ],
    };
  },
  drawing(d: LayerDrawing, k: number, kind: AnyDrawingKind | undefined): ElementEntry {
    return {
      name: d.id || `drawings[${k}]`,
      reportKind: d.type,
      type: d.type,
      typeKey: kind?.nameKey ?? UNKNOWN_KIND_NAME,
      label: drawingLabel(d),
      note: d.note,
      color: d.color,
      anchors: kind?.anchors(d) ?? [],
      always: kind?.alwaysExplained,
    };
  },
};

/** Collects every element the compile walk visits: the explain card's info and the report's name. */
export class ElementRegistry {
  readonly explain = new Map<string, LayerElementInfo>();
  readonly names = new Map<string, ElementName>();

  add(stored: StoredChartLayer, kind: ElementKind, idx: number | string, e: ElementEntry): void {
    const { layer } = stored;
    const key = elementKey(stored.layer_id, kind, idx);
    this.names.set(key, { layer: layer.id, element: e.name, kind: e.reportKind });
    const label = e.label?.trim() || null;
    if (!e.always && !label && !e.note) return;
    this.explain.set(key, {
      key,
      ref: { layerId: stored.layer_id, layerTitle: layer.title, kind, type: e.type, label, note: e.note ?? null, anchors: e.anchors },
      color: e.color ?? null,
      label,
      typeKey: e.typeKey,
      layerSummary: layer.summary ?? null,
      symbol: stored.symbol || layer.symbol,
      timeframe: stored.timeframe || layer.timeframe,
    });
  }
}

/**
 * The anchors as the agent should read them. An element with no price of its
 * own (a vline, a regime, a marker on a bar) takes the bar's close at that
 * time, so every anchor the agent gets is a real (time, price) pair; one that
 * cannot be priced is dropped rather than sent as NaN.
 */
export function cleanAnchors(
  anchors: readonly LayerPoint[],
  closeAt: (time: string) => number | null,
): LayerPoint[] {
  const out: LayerPoint[] = [];
  for (const a of anchors) {
    if (Number.isFinite(a.price)) {
      out.push(a);
      continue;
    }
    const close = a.time ? closeAt(a.time) : null;
    if (close != null && Number.isFinite(close)) out.push({ time: a.time, price: close });
  }
  return out;
}
