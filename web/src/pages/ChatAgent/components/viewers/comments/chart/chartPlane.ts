/**
 * A lightweight-charts chart as the comment marks read it (`ChartPlane`):
 * times through the chart's time grid, the same grid the agent's layers are
 * drawn on, so a mark sits where a layer drawn at that time would, past the
 * last bar too; prices through the candle series; and the shapes the layers
 * drew, by the layer renderer's own hit test.
 */
import { PriceScaleMode, type IChartApi, type IPanePrimitive, type ISeriesApi, type Logical, type MouseEventParams, type SeriesType, type Time } from 'lightweight-charts';
import { INTERVAL_SECONDS } from '@/lib/bars/chartConstants';
import type { ChartElementMark } from '@/types/artifacts';
import type { ChartLayerPrimitive } from '@/pages/MarketView/utils/chartLayer/ChartLayerPrimitive';
import type { ElementIdentity } from '@/pages/MarketView/utils/chartLayer/elements';
import { layerKeyUnder } from '@/pages/MarketView/utils/chartLayer/layerKeyUnder';
import {
  DAY, indexToSeconds, secondsToLayerTime, timeToIndex, type TimeGrid,
} from '@/pages/MarketView/utils/chartLayer/timeGrid';
import { gridAnchor, makeXMapper, type XMapper } from '@/pages/MarketView/utils/chartLayer/xMapper';
import { pricePaneHeight } from '@/pages/MarketView/utils/paneBounds';
import { findElement } from './chartElements';
import type { ChartPlane } from './chartSpots';

/** How near the last hovered shape a click must land to take it, in pixels: lightweight-charts reports a hover, not a click. */
const HOVER_REACH_PX = 3;

/** What the plane reads as it is asked, since bars load and layers change under it. */
export interface ChartPlaneSource {
  chart: IChartApi;
  series: ISeriesApi<'Candlestick'>;
  grid: () => TimeGrid;
  /** Who each shape drawn on the chart now is, by element key: the layers the viewer hid left out. */
  identities: () => ReadonlyMap<string, ElementIdentity>;
  interval: () => string;
  primitive: () => ChartLayerPrimitive | null;
  /** A layer's series on the chart, by the element key it draws. */
  seriesKeys: () => ReadonlyMap<ISeriesApi<SeriesType>, string>;
}

export interface LiveChartPlane extends ChartPlane {
  /** Stops following the pointer for shapes lightweight-charts hit tests itself. */
  dispose(): void;
}

export function chartPlane(src: ChartPlaneSource): LiveChartPlane {
  const { chart, series } = src;
  const ts = () => chart.timeScale();
  const mapper = (): { grid: TimeGrid; xm: XMapper } | null => {
    const grid = src.grid();
    const xm = makeXMapper(chart, gridAnchor(grid));
    return xm ? { grid, xm } : null;
  };
  const daily = () => (INTERVAL_SECONDS[src.interval()] ?? DAY) >= DAY;

  // Price lines and markers are hit tested by lightweight-charts, which says
  // what is under the pointer as it moves, not where a click lands: the last
  // hover near the click is what it was on.
  let hovered: { x: number; y: number; key: string } | null = null;
  const onMove = (param: MouseEventParams<Time>) => {
    const point = param.point;
    // The drawings are hit tested where the click lands, not here. Only a
    // layer's own shapes count: another object's id must not hide one beneath.
    const identities = src.identities();
    const key = layerKeyUnder(param, src.seriesKeys(), null, (k) => identities.has(k));
    hovered = point && key && (param.paneIndex ?? 0) === 0 ? { x: point.x, y: point.y, key } : null;
  };
  chart.subscribeCrosshairMove(onMove);

  return {
    area() {
      if (src.grid().bars.length === 0) return null;
      const width = ts().width();
      const height = pricePaneHeight(chart);
      return width > 0 && height > 0 ? { width, height } : null;
    },
    x(time) {
      const m = mapper();
      const i = m && timeToIndex(m.grid, time);
      return m && i != null ? m.xm.x(i) : null;
    },
    y: (price) => series.priceToCoordinate(price),
    halfBar: () => (ts().options().barSpacing ?? 0) / 2,
    timeAt(x) {
      const m = mapper();
      const logical = ts().coordinateToLogical(x);
      if (!m || logical == null) return null;
      const secs = indexToSeconds(m.grid, m.xm.index(logical), 'slot');
      return secs == null ? null : secondsToLayerTime(secs, daily());
    },
    priceAt: (y) => series.coordinateToPrice(y),
    elementAt(x, y) {
      const drawn = src.primitive()?.hitAt(x, y)?.key;
      const near = hovered && Math.hypot(hovered.x - x, hovered.y - y) <= HOVER_REACH_PX ? hovered.key : null;
      const key = drawn ?? near;
      return key ? (src.identities().get(key) ?? null) : null;
    },
    holds: (mark: ChartElementMark) => findElement(src.identities(), mark) !== null,
    show(from, to, low, high) {
      const m = mapper();
      const range = ts().getVisibleLogicalRange();
      const a = m && timeToIndex(m.grid, from);
      const b = m && timeToIndex(m.grid, to);
      if (!m || !range || a == null || b == null) return;
      // Grid index to the time scale's logical index: the mapper's inverse.
      const offset = m.xm.index(0);
      const lo = Math.min(a, b) - offset;
      const hi = Math.max(a, b) - offset;
      const width = range.to - range.from;
      // Centred at the zoom the reader has, unless the span needs more room.
      const next = hi - lo + 2 <= width
        ? { from: (lo + hi) / 2 - width / 2, to: (lo + hi) / 2 + width / 2 }
        : { from: lo - 1, to: hi + 1 };
      ts().setVisibleLogicalRange({ from: next.from as Logical, to: next.to as Logical });
      // The price scale fits the bars now in view, which can leave the mark
      // above or below them: once it has, the scale widens to take the mark
      // in too, as a drag on the price axis would. A percentage scale has no
      // prices to widen to, and keeps its fit.
      requestAnimationFrame(() => {
        const scale = series.priceScale();
        const mode = scale.options().mode;
        if (mode !== PriceScaleMode.Normal && mode !== PriceScaleMode.Logarithmic) return;
        const seen = scale.getVisibleRange();
        if (!seen || (low >= seen.from && high <= seen.to)) return;
        const lo = Math.min(low, seen.from);
        const hi = Math.max(high, seen.to);
        const pad = (hi - lo) * 0.08;
        scale.setVisibleRange({ from: lo - pad, to: hi + pad });
      });
    },
    watch(moved) {
      const pane = chart.panes()[0];
      if (!pane) return () => {};
      let raf = 0;
      // lightweight-charts has no event for a price scale's pan or zoom, a
      // pane resized, or an autoscale on a new bar, but it updates every
      // primitive's views on each repaint: one that draws nothing hears them all.
      const watcher: IPanePrimitive<Time> = {
        updateAllViews: () => {
          if (raf) return;
          raf = requestAnimationFrame(() => {
            raf = 0;
            moved();
          });
        },
      };
      pane.attachPrimitive(watcher);
      return () => {
        if (raf) cancelAnimationFrame(raf);
        try {
          pane.detachPrimitive(watcher);
        } catch {
          /* disposed */
        }
      };
    },
    dispose() {
      try {
        chart.unsubscribeCrosshairMove(onMove);
      } catch {
        /* disposed */
      }
    },
  };
}
