/** Where a run of values falls in a box, as SVG point lists. */
export interface SparkGeometry {
  /** The line's points, `x,y x,y …`. */
  line: string;
  /** The line closed down to the bottom edge, for a fill under it. */
  area: string;
  /** The reference value's height, when one was given. */
  baselineY: number | null;
  /** The last point, for a dot at the latest value. */
  last: { x: number; y: number };
}

interface SparkOptions {
  width: number;
  height: number;
  /** Room kept around the line on every side, so a stroke, its round cap and an end dot are not clipped. */
  pad?: number;
  /** Where each value sits across the width, 0–1; evenly spread when omitted. */
  at?: readonly number[];
  /** A reference the vertical range always includes (a previous close). */
  baseline?: number | null;
}

/** Null with fewer than two values: one point draws no line. */
export function sparkGeometry(values: readonly number[], { width, height, pad = 2, at, baseline = null }: SparkOptions): SparkGeometry | null {
  if (values.length < 2) return null;
  const bounds = baseline == null ? values : [...values, baseline];
  const hi = Math.max(...bounds);
  const lo = Math.min(...bounds);
  const span = hi - lo || 1;
  const y = (v: number) => pad + ((hi - v) / span) * (height - pad * 2);
  const x = (i: number) => pad + (at ? at[i] : i / (values.length - 1)) * (width - pad * 2);
  const points = values.map((v, i) => [x(i), y(v)] as const);
  const line = points.map(([px, py]) => `${px.toFixed(1)},${py.toFixed(1)}`).join(' ');
  const [firstX] = points[0];
  const [lastX, lastY] = points[points.length - 1];
  return {
    line,
    area: `${firstX.toFixed(1)},${height} ${line} ${lastX.toFixed(1)},${height}`,
    baselineY: baseline == null ? null : y(baseline),
    last: { x: lastX, y: lastY },
  };
}
