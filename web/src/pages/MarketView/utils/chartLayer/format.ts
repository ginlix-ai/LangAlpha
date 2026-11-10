/** Number and style formatting for the layer renderer's canvas text and lines. */

import type { LineStyleName } from '../../types/chartLayer';

export function fmtPrice(p: number): string {
  if (!Number.isFinite(p)) return '—';
  const a = Math.abs(p);
  if (a >= 1) return p.toFixed(2);
  if (a === 0) return '0';
  return p.toPrecision(4);
}

/** A price and its move from `from`: "212.40  +13.8%". */
export function fmtMove(from: number, to: number): string {
  const pct = from > 0 ? (to / from - 1) * 100 : 0;
  return `${fmtPrice(to)}  ${pct >= 0 ? '+' : '−'}${Math.abs(pct).toFixed(1)}%`;
}

export function fmtSignedPct(frac: number): string {
  if (!Number.isFinite(frac)) return '—';
  const pct = frac * 100;
  const sign = pct > 0 ? '+' : pct < 0 ? '−' : '';
  return `${sign}${Math.abs(pct).toFixed(2)}%`;
}

export function fmtSigned(v: number): string {
  if (!Number.isFinite(v)) return '—';
  const sign = v > 0 ? '+' : v < 0 ? '−' : '';
  return `${sign}${fmtPrice(Math.abs(v))}`;
}

/** A fib ratio as written on its level: 0.618, not 0.6180000001. */
export function trimRatio(r: number): string {
  return String(Number(r.toFixed(3)));
}

/** A series' last value in its pane caption: as many decimals as its size wants. */
export function fmtPaneValue(v: number): string {
  const a = Math.abs(v);
  return v.toFixed(a >= 1000 ? 0 : a >= 10 ? 1 : 2).replace('-', '−');
}

/** The canvas dash for a line style, `fallback` when none (or an unknown one) is given; [] is solid. */
export function dashFor(style: LineStyleName | null | undefined, fallback: LineStyleName = 'solid'): number[] {
  const s = style === 'solid' || style === 'dashed' || style === 'dotted' ? style : fallback;
  if (s === 'dotted') return [1.5, 3];
  if (s === 'dashed') return [6, 4];
  return [];
}

/** A line width the agent asked for, held to what reads on a chart. */
export function clampWidth(w: number | null | undefined, fallback: number): number {
  if (w == null || !Number.isFinite(w)) return fallback;
  return Math.max(0.5, Math.min(4, w));
}
