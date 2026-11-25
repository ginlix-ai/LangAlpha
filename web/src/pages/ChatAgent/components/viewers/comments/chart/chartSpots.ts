/**
 * Comments on a market chart. A mark is saved in the chart's own terms (bar
 * times and prices, or a shape a layer the agent drew holds), so it lands in
 * the same place at any zoom, and is a spot like a box on a picture is: an
 * element laid over the chart's price pane (`.comment-region`), placed from
 * its time and price, which the layer outlines, lists as a part and takes out
 * again as it does any other spot. A pan or a zoom moves every mark; the
 * chart says so (`ChartPlane.watch`) and each is placed again.
 */
import type { ArtifactCommentAnchor, ChartAnchor, ChartElementMark, ChartMark, ChartTime } from '@/types/artifacts';
import type { ElementIdentity } from '@/pages/MarketView/utils/chartLayer/elements';
import { MAX_CHART_MARKS } from '../../html/pageProtocol';
import { laidMarks } from '../spotKit';
import type { Edges, Found, SpotPicker, SpotStrategy } from '../spotStrategy';
import { markKey, markName, markPrice } from './chartMarks';

/**
 * The chart as the marks read it. Coordinates are the content's (the box the
 * chart is drawn in), whose top-left is the price pane's.
 */
export interface ChartPlane {
  /** The price pane's plot area, left of the price scale; null while the chart has no bars or no size. */
  area(): { width: number; height: number } | null;
  /** Where a time falls across the chart (a bar's centre, or where a bar would be); null where it cannot say. */
  x(time: ChartTime): number | null;
  y(price: number): number | null;
  /** Half the room a bar takes, so a region covers its first and last bars whole. */
  halfBar(): number;
  /** The time of the bar nearest `x`, or of the slot a bar would take past the last. */
  timeAt(x: number): ChartTime | null;
  priceAt(y: number): number | null;
  /** The shape a layer the agent drew holds under (x, y); null for an empty part of the chart. */
  elementAt(x: number, y: number): ElementIdentity | null;
  /** Whether a mark's shape is on the chart now: its layer shown, the shape still in it. */
  holds(mark: ChartElementMark): boolean;
  /** Brings the span from `from` to `to`, and the prices from `low` to `high`, into view. */
  show(from: ChartTime, to: ChartTime, low: number, high: number): void;
  /** Calls `moved` whenever the chart repaints (a pan, a zoom, new bars, a resize); returns the teardown. */
  watch(moved: () => void): () => void;
}

/** The least a region is across, in pixels, either way: less is a click. */
export const MIN_REGION_PX = 4;

/** The span a mark covers in time and price, for bringing it into view. */
function spanOf(mark: ChartMark): [ChartTime, ChartTime, number, number] {
  return mark.type === 'region' ? [mark.start, mark.end, mark.low, mark.high] : [mark.time, mark.time, mark.price, mark.price];
}

/** Where a mark sits on the content now, in its coordinates; null where the chart cannot place it. */
export function boxOn(plane: ChartPlane, mark: ChartMark): Edges | null {
  if (mark.type === 'region') {
    const x0 = plane.x(mark.start);
    const x1 = plane.x(mark.end);
    const y0 = plane.y(mark.high);
    const y1 = plane.y(mark.low);
    if (x0 == null || x1 == null || y0 == null || y1 == null) return null;
    const half = plane.halfBar();
    return { left: Math.min(x0, x1) - half, right: Math.max(x0, x1) + half, top: Math.min(y0, y1), bottom: Math.max(y0, y1) };
  }
  const x = plane.x(mark.time);
  const y = plane.y(mark.price);
  return x == null || y == null ? null : { left: x, right: x, top: y, bottom: y };
}

/** The part of `box` on the plot area, or null for none: a point on its edge is on it. */
export function onArea(box: Edges, area: { width: number; height: number }): Edges | null {
  const left = Math.max(0, box.left);
  const top = Math.max(0, box.top);
  const right = Math.min(area.width, box.right);
  const bottom = Math.min(area.height, box.bottom);
  return right >= left && bottom >= top ? { left, top, right, bottom } : null;
}

/**
 * The chart's picker and finder over `plane`, its marks laid on `content`.
 * The elements marking them are laid as anchors are found and marks drawn,
 * and the end of a pass of finds lets go of the ones it did not name.
 */
export function chartSpots(plane: ChartPlane, content: HTMLElement): SpotStrategy {
  const marks = laidMarks<ChartMark>({ field: 'mark', keyOf: markKey, isPoint: (mark) => mark.type !== 'region' });

  /** The element marking `mark` at `box`; kept through this pass and `keepFor` more. */
  const lay = (mark: ChartMark, box: Edges, keepFor = 0): HTMLElement =>
    marks.lay(content, mark, {
      left: `${box.left}px`,
      top: `${box.top}px`,
      width: `${box.right - box.left}px`,
      height: `${box.bottom - box.top}px`,
    }, keepFor);

  /** A client point in the content's coordinates. */
  const local = (x: number, y: number) => {
    const r = content.getBoundingClientRect();
    return { x: x - r.left, y: y - r.top };
  };

  /** Whether the chart itself is under a client point, not a control laid over it (the layers' legend). */
  const bare = (x: number, y: number): boolean => {
    const under = typeof document.elementFromPoint === 'function' ? document.elementFromPoint(x, y) : null;
    return !under || (content.contains(under) && under instanceof HTMLCanvasElement);
  };

  /** A new mark at (x, y) on the content: the shape there, or the point. Kept through the next pass, which may come before the layer names it. */
  const markAt = (x: number, y: number): HTMLElement | null => {
    const area = plane.area();
    if (!area || x < 0 || y < 0 || x > area.width || y > area.height) return null;
    const time = plane.timeAt(x);
    const price = plane.priceAt(y);
    if (time == null || price == null || !Number.isFinite(price)) return null;
    const shape = plane.elementAt(x, y);
    const mark: ChartMark = shape
      ? { type: 'element', ...shape, ...(shape.element_name ? { element_name: markName(shape.element_name) } : {}), time, price: markPrice(price) }
      : { type: 'point', time, price: markPrice(price) };
    const box = boxOn(plane, mark);
    const shown = box && onArea(box, area);
    return shown ? lay(mark, shown, 1) : null;
  };

  const picker: SpotPicker = {
    spotAt: (target) => {
      const spot = marks.spotAt(target);
      return spot && content.contains(spot) ? spot : null;
    },
    spotsIn: () => [],
    labelOf: () => null,
    anchorOf(spots) {
      const picked = spots.flatMap((s) => marks.valueOf(s) ?? []).slice(0, MAX_CHART_MARKS);
      return picked.length ? { kind: 'chart', marks: picked } : null;
    },
    rectsOf: marks.rectsOf,
    draw(box, from) {
      const start = local(from.x, from.y);
      const area = plane.area();
      if (!area || !bare(from.x, from.y) || !onArea({ left: start.x, right: start.x, top: start.y, bottom: start.y }, area)) return [];
      const r = content.getBoundingClientRect();
      const drawn = onArea({ left: box.left - r.left, right: box.right - r.left, top: box.top - r.top, bottom: box.bottom - r.top }, area);
      // Too small to be a region: the drag is a click where it began.
      if (!drawn || drawn.right - drawn.left < MIN_REGION_PX || drawn.bottom - drawn.top < MIN_REGION_PX) {
        const spot = markAt(start.x, start.y);
        return spot ? [spot] : [];
      }
      const a = plane.timeAt(drawn.left);
      const b = plane.timeAt(drawn.right);
      const high = plane.priceAt(drawn.top);
      const low = plane.priceAt(drawn.bottom);
      if (a == null || b == null || high == null || low == null || !Number.isFinite(high) || !Number.isFinite(low)) return [];
      const mark: ChartMark = {
        type: 'region',
        start: a <= b ? a : b,
        end: a <= b ? b : a,
        low: markPrice(Math.min(low, high)),
        high: markPrice(Math.max(low, high)),
      };
      const placed = boxOn(plane, mark);
      const shown = placed && onArea(placed, area);
      return shown ? [lay(mark, shown, 1)] : [];
    },
    pointAt(x, y) {
      if (!bare(x, y)) return null;
      const at = local(x, y);
      return markAt(at.x, at.y);
    },
    withAway(anchor, away) {
      if (anchor && anchor.kind !== 'chart') return anchor;
      const shown = anchor?.marks ?? [];
      const have = new Set(shown.map(markKey));
      const kept = (away as ChartMark[]).filter((m) => !have.has(markKey(m)));
      // The parts kept come first: past the limit, the newest marks in view
      // (laid last, so last in order) are the ones refused.
      return { kind: 'chart', marks: [...shown.slice(0, Math.max(0, MAX_CHART_MARKS - kept.length)), ...kept] };
    },
  };

  const findChart = (anchor: ChartAnchor): Found | null => {
    const area = plane.area();
    // Bars not in yet, or the chart has no size: the comments wait.
    if (!area) return { parked: content };
    const parts = anchor.marks.map((mark) => {
      if (mark.type === 'element' && !plane.holds(mark)) return null;
      const box = boxOn(plane, mark);
      const shown = box && onArea(box, area);
      return shown ? [lay(mark, shown)] : null;
    });
    if (parts.some(Boolean)) return { parts };
    // Each mark is off the part of the chart in view, or its layer is gone:
    // listed, and a press on it brings the first one into view.
    const [first] = anchor.marks;
    return {
      parked: content,
      ...(first ? { show: () => plane.show(...spanOf(first)) } : {}),
    };
  };

  return {
    fresh: marks.fresh,
    settled: marks.settled,
    dispose: marks.dispose,
    watch: (moved) => plane.watch(moved),
    spots: picker,
    find(anchor: ArtifactCommentAnchor) {
      return anchor.kind === 'chart' ? findChart(anchor) : null;
    },
  };
}
