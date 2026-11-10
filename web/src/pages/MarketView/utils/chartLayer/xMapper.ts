/**
 * Grid index → x, per frame. x maps through the time scale's logical index,
 * which extends past the data, so future anchors land where those bars will
 * be. The last bar's logical index is re-read every frame from its time, so a
 * prepend of older bars cannot leave the drawings a frame behind.
 */

import type { IChartApiBase, Logical, Time, UTCTimestamp } from 'lightweight-charts';

import type { TimeGrid } from './timeGrid';

/** Grid anchor of the series' last bar, so grid indices map to logical indices. */
export interface GridAnchor {
  lastBarTime: number;
  lastIndex: number;
}

/** The anchor of a grid's last bar; null with no bars. */
export function gridAnchor(grid: TimeGrid): GridAnchor | null {
  const n = grid.bars.length;
  return n ? { lastBarTime: grid.bars[n - 1], lastIndex: n - 1 } : null;
}

export interface XMapper {
  x(i: number): number | null;
  /** Grid index at a logical index of the time scale. */
  index(logical: number): number;
}

export function makeXMapper(chart: IChartApiBase<Time>, anchor: GridAnchor | null): XMapper | null {
  if (!anchor) return null;
  const ts = chart.timeScale();
  const lastX = ts.timeToCoordinate(anchor.lastBarTime as UTCTimestamp);
  if (lastX == null) return null;
  const rawLogical = ts.coordinateToLogical(lastX);
  if (rawLogical == null) return null;
  const lastLogical = Math.round(rawLogical);
  // logicalToCoordinate is only right on whole indices (a fractional one comes
  // back as 0), so a point between bars is interpolated from its two neighbours.
  const at = (l: number): number | null => ts.logicalToCoordinate(l as Logical);
  return {
    x(i: number) {
      const l = lastLogical + (i - anchor.lastIndex);
      const lo = Math.floor(l);
      const c0 = at(lo);
      if (c0 == null || l === lo) return c0;
      const c1 = at(lo + 1);
      return c1 == null ? c0 : c0 + (c1 - c0) * (l - lo);
    },
    index(logical: number) {
      return logical - lastLogical + anchor.lastIndex;
    },
  };
}

/** The visible span of the time scale in grid indices. */
export function visibleIndexRange(chart: IChartApiBase<Time>, xm: XMapper): { from: number; to: number } | null {
  const range = chart.timeScale().getVisibleLogicalRange();
  if (!range) return null;
  return { from: xm.index(range.from), to: xm.index(range.to) };
}
