/** Placing a time on the chart's bars: ISO to chart seconds, and the bar nearest a time. */

export function toUnixSeconds(iso: string): number | null {
  const ms = Date.parse(iso);
  if (!Number.isFinite(ms)) return null;
  return Math.floor(ms / 1000);
}

/** Index of the bar nearest `target` (the later one on a tie), or -1 with no bars. */
export function nearestBarIndex(bars: readonly { time: number }[] | null | undefined, target: number): number {
  if (!bars || bars.length === 0) return -1;
  let lo = 0;
  let hi = bars.length - 1;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (bars[mid].time < target) lo = mid + 1;
    else hi = mid;
  }
  if (lo > 0) {
    const diffLo = Math.abs(bars[lo].time - target);
    const diffPrev = Math.abs(bars[lo - 1].time - target);
    if (diffPrev < diffLo) lo -= 1;
  }
  return lo;
}

/** Time of the bar nearest `target`, or null with no bars. */
export function snapToNearestBar(
  chartData: readonly { time: number }[] | null,
  target: number,
): number | null {
  const k = nearestBarIndex(chartData, target);
  return k < 0 ? null : chartData![k].time;
}
