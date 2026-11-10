/**
 * Chart layer v1: an agent-published overlay (series, panes, levels, markers, regimes,
 * drawings, alerts). Mirrors the server's pydantic model
 * (`libs/chartkit/chartkit/schema.py`), which validates every layer before it is
 * stored, so the frontend can trust the shape but must still tolerate unknown drawing types
 * from a newer server.
 *
 * Times are exchange-local wall clock with no zone, `YYYY-MM-DD` or `YYYY-MM-DDTHH:MM[:SS]`:
 * the chart's own convention, parsed as if UTC into chart seconds. Points may lie past the
 * last bar.
 */

export type LayerTime = string;

export interface LayerPoint {
  time: LayerTime;
  price: number;
}

export type LineStyleName = 'solid' | 'dashed' | 'dotted';

/** Every element may carry the agent's reason for drawing it. */
export interface Explained {
  note?: string | null;
}

/**
 * A lower pane's share of the chart's height, as the schema bounds it
 * (`PANE_MIN_HEIGHT`, `PANE_MAX_HEIGHT`, `PANES_MAX_TOTAL` in chartkit): each
 * between `min` and `max`, all of a layer's lower panes together at most
 * `lowerTotal`. The server refuses a layer outside them; the renderer clamps
 * to them anyway, since several layers' panes share one chart.
 */
export const PANE_HEIGHT = { min: 0.05, max: 0.5, lowerTotal: 0.75 } as const;

export interface LayerPane {
  id: string;
  title?: string | null;
  /** Share of the chart's height (`PANE_HEIGHT`). */
  height?: number | null;
}

export interface LayerSeriesPoint {
  time: LayerTime;
  value: number;
  color?: string | null;
}

export interface LayerSeries extends Explained {
  id: string;
  title: string;
  pane: string;
  type: 'line' | 'area' | 'histogram';
  color?: string | null;
  width?: number | null;
  style?: LineStyleName | null;
  data: LayerSeriesPoint[];
}

export interface LayerFill extends Explained {
  a: string;
  b: string;
  color: string;
}

export interface LayerLevel extends Explained {
  price: number;
  pane: string;
  title?: string | null;
  color?: string | null;
  style?: LineStyleName | null;
}

export interface LayerMarker extends Explained {
  time: LayerTime;
  position: 'aboveBar' | 'belowBar' | 'inBar' | 'atPrice';
  price?: number | null;
  shape: 'arrowUp' | 'arrowDown' | 'circle' | 'square';
  color?: string | null;
  text?: string | null;
}

export interface LayerRegime extends Explained {
  from: LayerTime;
  to: LayerTime;
  color: string;
  label?: string | null;
}

interface DrawingBase extends Explained {
  id?: string | null;
  label?: string | null;
  color?: string | null;
}

export type Extend = 'none' | 'right' | 'left' | 'both';

export interface SegmentDrawing extends DrawingBase {
  type: 'segment';
  p1: LayerPoint;
  p2: LayerPoint;
  extend?: Extend | null;
  style?: LineStyleName | null;
  width?: number | null;
}

export interface VLineDrawing extends DrawingBase {
  type: 'vline';
  time: LayerTime;
  style?: LineStyleName | null;
}

export interface ChannelDrawing extends DrawingBase {
  type: 'channel';
  p1: LayerPoint;
  p2: LayerPoint;
  p3: LayerPoint;
  extend?: Extend | null;
  fill?: string | null;
}

export interface PitchforkDrawing extends DrawingBase {
  type: 'pitchfork';
  p1: LayerPoint;
  p2: LayerPoint;
  p3: LayerPoint;
  extend?: Extend | null;
}

export interface BoxDrawing extends DrawingBase {
  type: 'box';
  p1: LayerPoint;
  p2: LayerPoint;
  fill?: string | null;
}

export interface LabelDrawing extends DrawingBase {
  type: 'label';
  at: LayerPoint;
  text: string;
  position?: 'above' | 'below' | 'left' | 'right' | null;
}

export interface ArrowDrawing extends DrawingBase {
  type: 'arrow';
  p1: LayerPoint;
  p2: LayerPoint;
  width?: number | null;
}

export interface PathDrawing extends DrawingBase {
  type: 'path';
  points: LayerPoint[];
  /** One per point (null to skip), e.g. ['LS', 'H', 'RS'] or ['1','2','3','4','5']. */
  point_labels?: (string | null)[] | null;
  style?: LineStyleName | null;
  width?: number | null;
  closed?: boolean | null;
}

export interface FibDrawing extends DrawingBase {
  type: 'fib';
  p1: LayerPoint;
  p2: LayerPoint;
  levels?: number[] | null;
  extend?: Extend | null;
}

export interface ConeBand {
  label: string;
  upper: LayerPoint[];
  lower: LayerPoint[];
  fill?: string | null;
}

export interface ConeDrawing extends DrawingBase {
  type: 'cone';
  origin: LayerPoint;
  bands: ConeBand[];
}

export interface ForecastDrawing extends DrawingBase {
  type: 'forecast';
  origin: LayerPoint;
  target: LayerPoint;
  probability?: number | null;
}

export interface AnalogBar {
  time: LayerTime;
  open: number;
  high: number;
  low: number;
  close: number;
}

export interface AnalogDrawing extends DrawingBase {
  type: 'analog';
  origin: LayerPoint;
  source_start: LayerTime;
  source_end: LayerTime;
  bars: AnalogBar[];
}

export interface PositionDrawing extends DrawingBase {
  type: 'position';
  side: 'long' | 'short';
  entry: LayerPoint;
  stop: number;
  targets: number[];
  until?: LayerTime | null;
}

export interface MeasureDrawing extends DrawingBase {
  type: 'measure';
  p1: LayerPoint;
  p2: LayerPoint;
}

export interface ProfileRow {
  low: number;
  high: number;
  volume: number;
  up_volume?: number | null;
}

export interface ProfileDrawing extends DrawingBase {
  type: 'profile';
  from: LayerTime;
  to: LayerTime;
  rows: ProfileRow[];
  poc?: number | null;
  value_area?: [number, number] | null;
}

export type LayerDrawing =
  | SegmentDrawing
  | VLineDrawing
  | ChannelDrawing
  | PitchforkDrawing
  | BoxDrawing
  | LabelDrawing
  | ArrowDrawing
  | PathDrawing
  | FibDrawing
  | ConeDrawing
  | ForecastDrawing
  | AnalogDrawing
  | PositionDrawing
  | MeasureDrawing
  | ProfileDrawing;

export type LayerDrawingType = LayerDrawing['type'];

export interface LayerAlert extends Explained {
  id: string;
  title: string;
  condition: { kind: 'price_cross'; level: number; direction: 'above' | 'below' | 'either' };
}

export interface ChartLayer {
  version: 1;
  id: string;
  title: string;
  summary?: string | null;
  symbol: string;
  timeframe: string;
  as_of?: LayerTime | null;
  source: { kind: 'python' };
  panes: LayerPane[];
  series: LayerSeries[];
  fills: LayerFill[];
  levels: LayerLevel[];
  markers: LayerMarker[];
  regimes: LayerRegime[];
  drawings: LayerDrawing[];
  alerts: LayerAlert[];
}

/** The chart instance a layer lives on: `chart_id` is `{SYMBOL}:{timeframe}`. */
export interface ChartLayerRef {
  workspace_id: string;
  chart_id: string;
  symbol: string;
  timeframe: string;
}

/**
 * A stored layer as the REST list returns it. `layer_id` is `layer.id`, the
 * row's id on its chart instance: publishing the same id replaces it.
 */
export interface StoredChartLayer extends ChartLayerRef {
  layer_id: string;
  layer: ChartLayer;
}

/** What a tool result lists (`chart_layers`) for each layer its call published. */
export interface PublishedLayer extends ChartLayerRef {
  layer_id: string;
  title: string;
  summary: string | null;
  as_of: string | null;
}

/**
 * Stream artifact `chart_layer` payload: an add is the stored row itself, or
 * its identity alone when the layer was too large for the stream, which the
 * server's list then holds.
 */
export type ChartLayerArtifactPayload =
  | ({ op: 'add' } & Omit<StoredChartLayer, 'layer'> & { layer?: StoredChartLayer['layer'] })
  | ({ op: 'remove'; ids: string[] } & ChartLayerRef)
  | ({ op: 'clear' } & ChartLayerRef);

/** What the user picked to ask the agent about (hover → "Ask about this"). */
export interface LayerElementRef {
  layerId: string;
  layerTitle: string;
  kind: 'layer' | 'series' | 'level' | 'marker' | 'regime' | 'drawing' | 'alert';
  type?: string;
  label?: string | null;
  note?: string | null;
  /** Anchor points (time, price) that identify the element for the agent. */
  anchors: LayerPoint[];
}
