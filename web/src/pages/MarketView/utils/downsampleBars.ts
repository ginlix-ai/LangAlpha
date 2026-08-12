/**
 * Cap the number of bars carried with a region selection.
 *
 * When `bars` exceeds `max`, the array is strided (every `ceil(n/max)`-th bar)
 * and the final bar is force-appended so the selection's end is never dropped
 * by the stride. `truncated` reports whether any downsampling happened, so the
 * UI can flag that the agent received a reduced set.
 *
 * Generic over the bar shape — only `.length` and element identity matter here,
 * so it works for both in-memory `ChartDataBar`s and serialized `SelectionBar`s.
 */
export function downsampleBars<T>(bars: T[], max: number): { bars: T[]; truncated: boolean } {
  if (!Number.isInteger(max) || max <= 0) {
    throw new RangeError(`downsampleBars: max must be a positive integer, got ${max}`);
  }
  if (bars.length <= max) {
    return { bars, truncated: false };
  }
  const stride = Math.ceil(bars.length / max);
  const chosen = bars.filter((_, i) => i % stride === 0);
  const last = bars[bars.length - 1];
  if (chosen[chosen.length - 1] !== last) chosen.push(last);
  return { bars: chosen, truncated: true };
}
