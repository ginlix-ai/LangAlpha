/**
 * The layer renderer's labels: four styles, a priority layout that keeps them
 * off each other and off the chart's own text, and their painting.
 */

import {
  alignedLeft,
  chipWidth,
  clampSpan,
  drawChip,
  measureText,
  roundRectPath,
  withAlpha,
  type ChipStyle,
  type LabelAlign,
} from '../canvasKit';
import type { LayerTheme } from './layerTheme';
import type { ElementHandle } from './scene';

// One family for every label, as the axis uses: a monospace tag beside a sans
// chip read as two systems on one chart.
export const UI_FONT_FAMILY = "Geist, 'Noto Sans SC', -apple-system, BlinkMacSystemFont, system-ui, sans-serif";
const TAG_FONT = `500 10.5px ${UI_FONT_FAMILY}`;
const POINT_FONT = `700 10.5px ${UI_FONT_FAMILY}`;
const PILL_FONT = `600 10.5px ${UI_FONT_FAMILY}`;

/** A named element's chip: a pill with the element's color as its dot. */
const CHIP: ChipStyle = {
  font: `600 11px ${UI_FONT_FAMILY}`,
  height: 19,
  padX: 8,
  dotR: 3,
  dotGap: 5,
  radius: 'pill',
  shadowBlur: 6,
  shadowOffsetY: 1,
};

export const CHIP_H = CHIP.height;
export const TAG_H = 13;
export const PILL_H = 17;
const PILL_PAD_X = 6;
/** Labels keep this far off the pane's edges. */
export const EDGE = 4;
/** How far past the pane a label's anchor may sit and still be drawn (clamped in). */
const OFF_PANE = 24;
const LABEL_GAP = 3;

/** chip: named element; tag: text on its line; point: a pattern vertex; pill: a solid readout (position tool). */
export type LabelStyle = 'chip' | 'tag' | 'point' | 'pill';

export interface LabelAnchor {
  x: number;
  y: number;
  align: LabelAlign;
}

export interface LabelReq extends LabelAnchor {
  /** The element the label names. */
  el: ElementHandle;
  text: string;
  style: LabelStyle;
  /** Accent dot (chip), text color (tag / point) or fill (pill). */
  color: string;
  /** A chip may slide up/down out of a collision; a tag is tied to its line and drops instead. */
  movable: boolean;
  priority: number;
  /** Other anchors worth trying, in order, before the label is dropped (a zone's far corner, a box's ends). */
  alts?: LabelAnchor[];
}

export interface PlacedLabel extends LabelReq {
  left: number;
  top: number;
  width: number;
  height: number;
}

export interface Rect {
  left: number;
  top: number;
  width: number;
  height: number;
}

function labelBox(ctx: CanvasRenderingContext2D, r: LabelReq): { width: number; height: number } {
  if (r.style === 'chip') return { width: chipWidth(ctx, CHIP, r.text), height: CHIP_H };
  if (r.style === 'pill') return { width: measureText(ctx, PILL_FONT, r.text) + PILL_PAD_X * 2, height: PILL_H };
  const font = r.style === 'tag' ? TAG_FONT : POINT_FONT;
  return { width: measureText(ctx, font, r.text) + 4, height: TAG_H };
}

function overlaps(a: Rect, left: number, top: number, w: number, h: number, gap = LABEL_GAP): boolean {
  return (
    left < a.left + a.width + gap &&
    a.left < left + w + gap &&
    top < a.top + a.height + gap &&
    a.top < top + h + gap
  );
}

/**
 * What labels keep off besides each other. `hard` is text the chart draws
 * itself (marker captions, price-line names on the axis edge): a label there
 * is dropped. `soft` is the candles: a movable label prefers a slot off them
 * and only sits over bars when no clear slot is left.
 */
export interface LabelObstacles {
  hard: readonly Rect[];
  soft: readonly Rect[];
}

export const NO_OBSTACLES: LabelObstacles = { hard: [], soft: [] };

/**
 * Place labels by priority. Each tries its anchor (and its alternates); a
 * movable one then tries slots below and above. A slot clear of labels, chart
 * text and candles wins; failing that, the first one clear of labels and text;
 * whatever still collides is dropped. Fewer labels beat labels on labels: a
 * dropped label's element still explains itself on hover. An anchor past the
 * pane's edge (`OFF_PANE` beyond it) is not tried, and a label with none left
 * is not drawn: the clamp that keeps an edge label whole would otherwise pull
 * it in beside nothing.
 */
export function layoutLabels(
  ctx: CanvasRenderingContext2D,
  reqs: LabelReq[],
  w: number,
  h: number,
  obstacles: LabelObstacles = NO_OBSTACLES,
): { placed: PlacedLabel[]; dropped: LabelReq[] } {
  const placed: PlacedLabel[] = [];
  const dropped: LabelReq[] = [];
  const order = [...reqs].sort((a, b) => b.priority - a.priority);
  for (const r of order) {
    const anchors: LabelAnchor[] = [{ x: r.x, y: r.y, align: r.align }, ...(r.alts ?? [])].filter(
      (a) => a.x >= -OFF_PANE && a.x <= w + OFF_PANE && a.y >= -OFF_PANE && a.y <= h + OFF_PANE,
    );
    if (!anchors.length) continue;
    const { width, height } = labelBox(ctx, r);
    const steps = r.movable ? [0, 1, -1, 2, -2] : [0];
    let fallback: { left: number; top: number } | null = null;
    let done = false;
    outer: for (const step of steps) {
      for (const a of anchors) {
        const left = clampSpan(alignedLeft(a.x, a.align, width), width, w, EDGE);
        const top = clampSpan(a.y - height / 2 + step * (height + LABEL_GAP), height, h, EDGE);
        if (placed.some((p) => overlaps(p, left, top, width, height))) continue;
        if (obstacles.hard.some((o) => overlaps(o, left, top, width, height, 1))) continue;
        if (r.movable && obstacles.soft.some((o) => overlaps(o, left, top, width, height, 1))) {
          fallback ??= { left, top };
          continue;
        }
        placed.push({ ...r, left, top, width, height });
        done = true;
        break outer;
      }
    }
    if (done) continue;
    if (fallback) {
      placed.push({ ...r, ...fallback, width, height });
    } else if (r.priority >= 100) {
      // Must-show labels (an explicit label drawing) sit over the clash rather than vanish.
      const a = anchors[0];
      const left = clampSpan(alignedLeft(a.x, a.align, width), width, w, EDGE);
      const top = clampSpan(a.y - height / 2, height, h, EDGE);
      placed.push({ ...r, left, top, width, height });
    } else {
      dropped.push(r);
    }
  }
  return { placed, dropped };
}

export function drawLabel(ctx: CanvasRenderingContext2D, l: PlacedLabel, th: LayerTheme, hovered: boolean): void {
  if (l.style === 'chip') {
    drawChip(ctx, CHIP, l.left, l.top, l.width, l.text, {
      bg: th.chipBg,
      border: hovered ? withAlpha(l.color, 0.9) : th.chipBorder,
      ink: th.ink,
      shadow: th.shadow,
      accent: l.color,
    });
    return;
  }
  if (l.style === 'pill') {
    roundRectPath(ctx, l.left, l.top, l.width, l.height, 3);
    ctx.fillStyle = withAlpha(l.color, hovered ? 1 : 0.92);
    ctx.fill();
    ctx.font = PILL_FONT;
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillStyle = th.onFill;
    ctx.fillText(l.text, l.left + l.width / 2, l.top + l.height / 2 + 0.5);
    return;
  }
  ctx.font = l.style === 'tag' ? TAG_FONT : POINT_FONT;
  ctx.textAlign = 'left';
  ctx.textBaseline = 'middle';
  const x = l.left + 2;
  const y = l.top + l.height / 2 + 0.5;
  ctx.lineJoin = 'round';
  ctx.lineWidth = 3;
  ctx.strokeStyle = th.halo;
  ctx.strokeText(l.text, x, y);
  ctx.fillStyle = l.color;
  ctx.fillText(l.text, x, y);
}
