/**
 * What the chart-layer renderer drew on its last frame, published by the
 * snapshot page (`chartRender/`) on `window.__LA_CHART_REPORT__` after each
 * paint of the layer primitive; the live app never computes it. It
 * tells a reader of a chart snapshot (the agent) what it could not see in the
 * picture or could miss in it: labels the collision layout hid, elements
 * outside the view, names cut short, labels under the chart's own text, marker
 * captions over candles, forecast verdicts, and where the renderer folded a
 * layer's element into something the chart already shows. Only defined while
 * at least one layer is on the chart.
 */
export interface ChartRenderReport {
  /** Increments on every paint of the layer primitive. */
  frame: number;
  view: {
    /** First and last visible bar slot, in the chart's time convention (`YYYY-MM-DD` daily, `YYYY-MM-DDTHH:MM` intraday). */
    from: string;
    to: string;
    /** Empty slots in view past the last bar. */
    futureSlots: number;
    /** The price pane's drawing area in CSS px (axis excluded). */
    width: number;
    height: number;
    /** Pane titles, top to bottom. */
    panes: string[];
  };
  layers: { id: string; title: string }[];
  /** Labels the collision layout hid; their elements still explain themselves on hover. */
  droppedLabels: { layer: string; element: string; text: string }[];
  /** Elements entirely outside the visible time or price range. */
  offscreen: { layer: string; element: string; kind: string }[];
  /** Names shown cut short: on a price-axis tag (`axis`), or a layer's title in its legend chip (`title`). */
  cutText: { layer: string; element: string; where: 'axis' | 'title'; text: string; shown: string }[];
  /**
   * Placed labels the chart draws other text over: a layer chip of the legend
   * over the pane's top-left (`chip`, `byText` its title), or a price tag at the
   * pane's right edge (`tag`, `byText` its text, e.g. "Close").
   */
  coveredLabels: { layer: string; element: string; kind: string; by: 'chip' | 'tag'; byText: string }[];
  /** Marker captions the markers plugin draws over candles. */
  markersOverCandles: { text: string }[];
  /**
   * Each forecast's verdict on the chart's bars (`forecastVerdict`): `at` is the
   * bar that hit it, or the target time it missed by or is pending until.
   * `unknown`: the bars start after its origin, so it is not judged.
   */
  forecasts: { layer: string; element: string; label: string | null; state: 'hit' | 'missed' | 'pending' | 'unknown'; at: string }[];
  /** Merges and skips, e.g. "layer pane 'RSI 14' merged into built-in RSI (14)". */
  notes: string[];
}

declare global {
  interface Window {
    __LA_CHART_REPORT__?: ChartRenderReport;
  }
}
