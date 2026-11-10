/**
 * What labels keep off, as boxes in pane pixels: the visible candles, the
 * markers plugin's shapes and captions, and the names of price lines at the
 * pane's right edge. One frame computes them once; the layout keeps labels
 * clear of them and the render report reads the same boxes, so what the report
 * says a label met is what the layout saw.
 */

import { measureText } from '../canvasKit';
import { UI_FONT_FAMILY, type LabelObstacles, type Rect } from './canvasLabels';
import type { AxisLineBox, MarkerBox, PaintContext } from './painter';

/** The chart's own text (marker captions, price-line names): LWC's default 12px. */
const CHART_TEXT_FONT = `12px ${UI_FONT_FAMILY}`;

/** What the boxes are measured with: the frame's projection and its canvas, which the painter is. */
export interface BoxFrame {
  readonly ctx: CanvasRenderingContext2D;
  readonly w: number;
  readonly h: number;
  X(i: number): number | null;
  Y(p: number): number | null;
}

type View = { from: number; to: number };

/** A marker's boxes: the narrow shape, then the wide caption past it (none without text). */
export interface MarkerBoxes {
  marker: MarkerBox;
  shape: Rect;
  caption: Rect | null;
}

export interface TagBox {
  title: string;
  box: Rect;
}

export interface FrameObstacles {
  candles: Rect[];
  markers: MarkerBoxes[];
  tags: TagBox[];
}

/** Each visible candle's high-to-low box, as wide as the bar spacing leaves it. */
export function candleBoxes(f: BoxFrame, c: Pick<PaintContext, 'highs' | 'lows'>, view: View, barSpacing: number): Rect[] {
  const out: Rect[] = [];
  const n = Math.min(c.highs.length, c.lows.length);
  const half = Math.max(1.5, barSpacing * 0.4);
  const k0 = Math.max(0, Math.floor(view.from));
  const k1 = Math.min(n - 1, Math.ceil(view.to));
  for (let k = k0; k <= k1; k++) {
    const x = f.X(k);
    const yh = f.Y(c.highs[k]);
    const yl = f.Y(c.lows[k]);
    if (x == null || yh == null || yl == null) continue;
    out.push({ left: x - half, top: Math.min(yh, yl), width: half * 2, height: Math.abs(yl - yh) });
  }
  return out;
}

/**
 * The markers plugin stacks a shape and then its caption out from the bar's
 * high (or low), sized from the bar spacing as LWC sizes them.
 */
export function markerBoxes(f: BoxFrame, c: PaintContext, view: View, barSpacing: number): MarkerBoxes[] {
  const out: MarkerBoxes[] = [];
  const n = Math.min(c.highs.length, c.lows.length);
  const size = Math.min(Math.max(barSpacing, 12), 30);
  const margin = Math.max(size * 0.1, 3);
  const textH = 12 * 1.2;
  for (const m of c.markers) {
    if (m.i < view.from - 1 || m.i > view.to + 1) continue;
    const x = f.X(m.i);
    if (x == null) continue;
    const k = Math.round(m.i);
    let edge: number | null = null;
    let dir = -1;
    if (m.position === 'aboveBar' && k >= 0 && k < n) {
      edge = f.Y(c.highs[k]);
    } else if (m.position === 'belowBar' && k >= 0 && k < n) {
      edge = f.Y(c.lows[k]);
      dir = 1;
    } else if (m.price != null) {
      const y = f.Y(m.price);
      // Centred on its price, the caption above the shape.
      if (y != null) edge = y + dir * -(size / 2 + margin);
    }
    if (edge == null) continue;
    const shapeNear = edge + dir * margin;
    const shapeFar = shapeNear + dir * size;
    const shape = { left: x - size / 2, top: Math.min(shapeNear, shapeFar), width: size, height: size };
    let caption: Rect | null = null;
    if (m.text) {
      const tw = measureText(f.ctx, CHART_TEXT_FONT, m.text) + 4;
      const textFar = shapeFar + dir * textH;
      caption = { left: x - tw / 2, top: Math.min(shapeFar, textFar), width: tw, height: textH };
    }
    out.push({ marker: m, shape, caption });
  }
  return out;
}

/** A price line's name at the pane's right edge, where LWC draws it; null off the pane or with no name. */
export function axisTagBox(f: BoxFrame, line: Pick<AxisLineBox, 'price' | 'title'>): Rect | null {
  if (!line.title) return null;
  const y = f.Y(line.price);
  if (y == null || y < -10 || y > f.h + 10) return null;
  const width = measureText(f.ctx, CHART_TEXT_FONT, line.title) + 14;
  return { left: f.w - width, top: y - 10, width, height: 20 };
}

export function frameObstacles(f: BoxFrame, c: PaintContext, view: View, barSpacing: number): FrameObstacles {
  const tags: TagBox[] = [];
  for (const line of c.axisLines) {
    const box = axisTagBox(f, line);
    if (box) tags.push({ title: line.title, box });
  }
  return { candles: candleBoxes(f, c, view, barSpacing), markers: markerBoxes(f, c, view, barSpacing), tags };
}

/** What the layout keeps labels off: text the chart draws is hard, the candles soft. */
export function labelObstacles(o: FrameObstacles): LabelObstacles {
  return {
    soft: o.candles,
    hard: [...o.markers.flatMap((m) => (m.caption ? [m.shape, m.caption] : [m.shape])), ...o.tags.map((t) => t.box)],
  };
}
