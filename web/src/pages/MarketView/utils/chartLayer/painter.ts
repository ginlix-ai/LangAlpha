/**
 * One frame's drawing context for the layer renderer: projection, strokes,
 * fills, labels and hit shapes. Each drawing kind paints through it
 * (`kinds/`); the labels are laid out and drawn last, over every stroke.
 */

import type { Extend } from '../../types/chartLayer';
import { tracePath, withAlpha } from '../canvasKit';
import { arrowHead, extendSegment, type XY } from './canvasGeom';
import {
  NO_OBSTACLES,
  drawLabel,
  layoutLabels,
  type LabelAnchor,
  type LabelObstacles,
  type LabelReq,
} from './canvasLabels';
import type { HitShape } from './hitShapes';
import { DEFAULT_LAYER_TEXT, type LayerText } from './layerText';
import type { LayerTheme } from './layerTheme';
import type { ElementHandle, SceneBase } from './scene';
import type { IP } from './timeGrid';
import type { XMapper } from './xMapper';

const STROKE_ALPHA = 0.95;
/** How far a ray runs past the pane's edge, so its end is never seen. */
const EXTEND_MARGIN = 10;
/** Extra stroke width for the hovered element. */
const HOVER_WIDTH = 0.75;
/** How far a ray's fill runs past its anchors, fading out; the line itself runs on. */
const FILL_FADE_PX = 64;

export interface SeriesLike {
  priceToCoordinate(price: number): number | null;
}

/** A layer marker the markers plugin draws, so labels keep off its caption. */
export interface MarkerBox {
  i: number;
  position: 'aboveBar' | 'belowBar' | 'inBar' | 'atPriceMiddle';
  price?: number;
  text: string;
}

/** A price line on the candle series: its name sits at the pane's right edge. */
export interface AxisLineBox {
  price: number;
  title: string;
  /** The elements it stands for (a merged line stands for several). */
  els: readonly ElementHandle[];
}

/** What the painter needs besides the scene: the bars and the text the chart draws itself. */
export interface PaintContext {
  /** Candle high / low by grid bar index. */
  highs: ArrayLike<number>;
  lows: ArrayLike<number>;
  markers: readonly MarkerBox[];
  axisLines: readonly AxisLineBox[];
}

export const EMPTY_PAINT_CONTEXT: PaintContext = { highs: [], lows: [], markers: [], axisLines: [] };

type LabelInput = Omit<LabelReq, 'movable'> & { movable?: boolean };

export class Painter {
  readonly hits: HitShape[] = [];
  private readonly labels: LabelReq[] = [];

  constructor(
    readonly ctx: CanvasRenderingContext2D,
    readonly w: number,
    readonly h: number,
    private readonly xm: XMapper,
    private readonly series: SeriesLike,
    readonly th: LayerTheme,
    private readonly hovered: string | null,
    /** The candles' highs and lows by grid index: what a forecast is judged on. */
    readonly bars: Pick<PaintContext, 'highs' | 'lows'> = EMPTY_PAINT_CONTEXT,
    /** The renderer's own words, in the reader's language. */
    readonly text: LayerText = DEFAULT_LAYER_TEXT,
  ) {}

  X(i: number): number | null {
    return this.xm.x(i);
  }

  Y(p: number): number | null {
    return this.series.priceToCoordinate(p);
  }

  P(ip: IP): XY | null {
    const x = this.X(ip.i);
    const y = this.Y(ip.p);
    return x == null || y == null ? null : { x, y };
  }

  /** Whether every x is well off the pane (or none is known). */
  offscreen(xs: (number | null)[]): boolean {
    const v = xs.filter((x): x is number => x != null);
    if (!v.length) return true;
    return Math.max(...v) < -40 || Math.min(...v) > this.w + 40;
  }

  isHovered(key: string): boolean {
    return this.hovered === key;
  }

  /** A stroke width of `base`, thickened while the element is hovered: the one emphasis rule. */
  strokeWidth(d: { key: string }, base: number): number {
    return this.hovered === d.key ? base + HOVER_WIDTH : base;
  }

  /** A segment run on to the pane's edge on the sides `extend` names. */
  extended(a: XY, b: XY, extend: Extend): [XY, XY] {
    return extendSegment(a, b, extend, -EXTEND_MARGIN, this.w + EXTEND_MARGIN);
  }

  /** A horizontal span `[left, right]` run on to the pane's edge on the sides `extend` names. */
  extendedSpan(left: number, right: number, extend: Extend): [number, number] {
    return [
      extend === 'left' || extend === 'both' ? -EXTEND_MARGIN : left,
      extend === 'right' || extend === 'both' ? this.w + EXTEND_MARGIN : right,
    ];
  }

  stroke(color: string, width: number, dash: number[] = [], alpha = STROKE_ALPHA): void {
    const { ctx } = this;
    ctx.strokeStyle = withAlpha(color, alpha);
    ctx.lineWidth = width;
    ctx.setLineDash(dash);
    ctx.lineCap = 'round';
    ctx.lineJoin = 'round';
    ctx.stroke();
    ctx.setLineDash([]);
  }

  line(a: XY, b: XY, color: string, width: number, dash: number[] = [], alpha = STROKE_ALPHA): void {
    tracePath(this.ctx, [a, b]);
    this.stroke(color, width, dash, alpha);
  }

  /** Stroke a polyline through `pts`. */
  polyline(pts: readonly XY[], color: string, width: number, dash: number[] = [], alpha = STROKE_ALPHA): void {
    tracePath(this.ctx, pts);
    this.stroke(color, width, dash, alpha);
  }

  /** Fill a polygon; a null fill keeps the fill style already set (a gradient). */
  poly(pts: XY[], fill: string | null): void {
    if (pts.length < 3) return;
    const { ctx } = this;
    tracePath(ctx, pts, true);
    if (fill != null) ctx.fillStyle = fill;
    ctx.fill();
  }

  /** Fill a rectangle; a null fill keeps the fill style already set (a gradient). */
  rect(x: number, y: number, w: number, h: number, fill: string | null): void {
    if (fill != null) this.ctx.fillStyle = fill;
    this.ctx.fillRect(x, y, w, h);
  }

  /** Fill the axis-aligned box with corners `a` and `b`. */
  box(a: XY, b: XY, fill: string): void {
    this.rect(Math.min(a.x, b.x), Math.min(a.y, b.y), Math.abs(b.x - a.x), Math.abs(b.y - a.y), fill);
  }

  /** Outline a rectangle. */
  strokeRect(x: number, y: number, w: number, h: number, color: string, width: number, alpha = STROKE_ALPHA): void {
    this.ctx.strokeStyle = withAlpha(color, alpha);
    this.ctx.lineWidth = width;
    this.ctx.strokeRect(x, y, w, h);
  }

  /**
   * An arrow from `a` to `b`: a shaft that stops short of the tip so the head
   * stays sharp, and the head. False when the two coincide and nothing is drawn.
   */
  arrow(a: XY, b: XY, color: string, width: number, size: number): boolean {
    const len = Math.hypot(b.x - a.x, b.y - a.y);
    if (len < 1) return false;
    const shaft = { x: b.x - ((b.x - a.x) / len) * size * 0.7, y: b.y - ((b.y - a.y) / len) * size * 0.7 };
    this.line(a, shaft, color, width);
    this.poly(arrowHead(a, b, size), withAlpha(color, 0.95));
    return true;
  }

  /**
   * Fill a shape that may run on past its anchors (an extended channel, a fib
   * drawn to the right): full strength between `x0` and `x1`, then fading out
   * over a short run on each extended side. The fill is the zone the agent
   * measured; past it only the lines carry the projection, so an extended
   * zone cannot wash over the whole future.
   */
  fadedFill(fill: string, x0: number, x1: number, extend: Extend, paint: () => void): void {
    const { ctx } = this;
    const fadeL = extend === 'left' || extend === 'both' ? FILL_FADE_PX : 0;
    const fadeR = extend === 'right' || extend === 'both' ? FILL_FADE_PX : 0;
    const lo = Math.min(x0, x1) - fadeL;
    const hi = Math.max(x0, x1) + fadeR;
    if (hi <= lo) return;
    ctx.save();
    ctx.beginPath();
    ctx.rect(lo, -1, hi - lo, this.h + 2);
    ctx.clip();
    if (fadeL || fadeR) {
      const g = ctx.createLinearGradient(lo, 0, hi, 0);
      const span = hi - lo;
      const clear = withAlpha(fill, 0);
      g.addColorStop(0, fadeL ? clear : fill);
      if (fadeL) g.addColorStop(Math.min(0.5, fadeL / span), fill);
      if (fadeR) g.addColorStop(Math.max(0.5, 1 - fadeR / span), fill);
      g.addColorStop(1, fadeR ? clear : fill);
      ctx.fillStyle = g;
    } else {
      ctx.fillStyle = fill;
    }
    paint();
    ctx.restore();
  }

  /** A vertex dot on a halo of the chart's background. */
  dot(c: XY, r: number, color: string): void {
    const { ctx } = this;
    ctx.beginPath();
    ctx.arc(c.x, c.y, r + 1.5, 0, Math.PI * 2);
    ctx.fillStyle = this.th.halo;
    ctx.fill();
    ctx.beginPath();
    ctx.arc(c.x, c.y, r, 0, Math.PI * 2);
    ctx.fillStyle = withAlpha(color, 1);
    ctx.fill();
  }

  /** Queue a label for this frame's layout; a chip may move, anything else is tied to its anchor. */
  label(req: LabelInput): void {
    this.labels.push({ movable: req.style === 'chip', ...req });
  }

  /** An element's name as a chip at `at`: `opts.color` (the element's own by default) as the dot, where the layout may slide it clear of others. */
  nameChip(el: SceneBase, text: string, at: LabelAnchor, priority: number, opts: { alts?: LabelAnchor[]; movable?: boolean; color?: string } = {}): void {
    this.label({ el, text, style: 'chip', color: el.color, ...at, priority, ...opts });
  }

  /** A stroke's hit shape. */
  seg(key: string, a: XY, b: XY, tol = 5): void {
    this.hits.push({ key, t: 'seg', a, b, tol });
  }

  hit(shape: HitShape): void {
    this.hits.push(shape);
  }

  /** Lay out and draw this frame's labels; returns the ones the layout hid. */
  flushLabels(obstacles: LabelObstacles = NO_OBSTACLES): LabelReq[] {
    const { placed, dropped } = layoutLabels(this.ctx, this.labels, this.w, this.h, obstacles);
    for (const l of placed) {
      drawLabel(this.ctx, l, this.th, this.hovered === l.el.key);
      this.hits.push({ key: l.el.key, t: 'rect', x0: l.left, y0: l.top, x1: l.left + l.width, y1: l.top + l.height, label: true });
    }
    return dropped;
  }
}
