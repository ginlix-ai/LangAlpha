/**
 * Lightweight-charts v4 series primitive that draws the agent annotation
 * shapes LWC has no native API for: rectangles (zones), vertical lines,
 * free-floating text, and Fibonacci retracement levels.
 *
 * Price lines, trendlines, and markers are handled elsewhere via native
 * LWC APIs (createPriceLine / addLineSeries / setMarkers); this primitive
 * only owns the canvas-drawn geometry.
 *
 * The hook (`useAgentAnnotations`) converts store annotations into the
 * coordinate-free item arrays below (times as unix seconds, prices as
 * y-values) and calls `setData`. This primitive does the per-frame
 * coordinate conversion and drawing — mirroring `ExtendedHoursBgPrimitive`.
 *
 * Labels render as theme-aware frosted chips (light/dark) with a soft shadow
 * and an accent dot keyed to the annotation color; a declutter pass keeps them
 * from overlapping each other or spilling past the pane edges.
 *
 * Usage:
 *   const prim = new AgentAnnotationsPrimitive();
 *   candlestickSeries.attachPrimitive(prim);
 *   prim.setTheme('light');                 // or 'dark' (default)
 *   prim.setData({ rects, vlines, texts, fibs });
 */

import type {
  ISeriesPrimitivePaneView,
  ISeriesPrimitivePaneRenderer,
  SeriesPrimitivePaneViewZOrder,
  Time,
  IChartApiBase,
} from 'lightweight-charts';
import type { CanvasRenderingTarget2D } from 'fancy-canvas';

export interface RectItem {
  time1: number;
  time2: number;
  price1: number;
  price2: number;
  color: string;
  label?: string;
}

export interface VLineItem {
  time: number;
  color: string;
  /** Canvas dash pattern; [] means solid. */
  dash: number[];
  label?: string;
}

export interface TextItem {
  time: number;
  price: number;
  text: string;
  color: string;
}

export interface FibLevel {
  ratio: number;
  price: number;
}

export interface FibItem {
  time1: number;
  time2: number;
  levels: FibLevel[];
  color: string;
}

export interface AgentAnnotationsData {
  rects: RectItem[];
  vlines: VLineItem[];
  texts: TextItem[];
  fibs: FibItem[];
}

interface SeriesLike {
  priceToCoordinate(price: number): number | null;
}

interface SeriesAttachedParams {
  chart: IChartApiBase<Time>;
  series: SeriesLike;
  requestUpdate: () => void;
}

const EMPTY: AgentAnnotationsData = { rects: [], vlines: [], texts: [], fibs: [] };

export type AnnotationTheme = 'light' | 'dark';

const CHIP_FONT =
  '600 11px -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif';

interface ChipPalette {
  bg: string;
  border: string;
  ink: string;
  shadow: string;
}

// Frosted chip surfaces tuned to the two chart backgrounds (#000 dark /
// #FFFCF9 light). The annotation's own color is demoted to a small accent dot
// so the label text stays high-contrast ink on whatever palette the agent picks
// — the single biggest legibility win over stamping raw color on a dark box.
const CHIP_PALETTE: Record<AnnotationTheme, ChipPalette> = {
  dark: {
    bg: 'rgba(20, 22, 27, 0.88)',
    border: 'rgba(255, 255, 255, 0.16)',
    ink: '#F4F4F5',
    shadow: 'rgba(0, 0, 0, 0.55)',
  },
  light: {
    bg: 'rgba(255, 252, 249, 0.94)',
    border: 'rgba(45, 43, 40, 0.16)',
    ink: '#2D2B28',
    shadow: 'rgba(45, 43, 40, 0.22)',
  },
};

const CHIP_H = 19; // fixed chip height → consistent vertical rhythm
const CHIP_PAD_X = 7;
const CHIP_RADIUS = 4;
const DOT_R = 3;
const DOT_GAP = 6;
const CHIP_GAP = 4; // min vertical gap between decluttered chips
const EDGE = 4; // keep chips off the pane edges

type ChipAlign = 'left' | 'center' | 'right';

interface LabelReq {
  text: string;
  accent: string;
  anchorX: number;
  anchorY: number;
  align: ChipAlign;
}

interface PlacedChip extends LabelReq {
  left: number;
  top: number;
  width: number;
}

/** Trace a rounded-rectangle path (caller fills/strokes). */
function roundRectPath(
  ctx: CanvasRenderingContext2D,
  x: number,
  y: number,
  w: number,
  h: number,
  r: number,
): void {
  const rad = Math.min(r, w / 2, h / 2);
  ctx.beginPath();
  ctx.moveTo(x + rad, y);
  ctx.arcTo(x + w, y, x + w, y + h, rad);
  ctx.arcTo(x + w, y + h, x, y + h, rad);
  ctx.arcTo(x, y + h, x, y, rad);
  ctx.arcTo(x, y, x + w, y, rad);
  ctx.closePath();
}

function chipWidth(ctx: CanvasRenderingContext2D, text: string): number {
  ctx.font = CHIP_FONT;
  return CHIP_PAD_X * 2 + DOT_R * 2 + DOT_GAP + ctx.measureText(text).width;
}

/**
 * Resolve each label anchor → clamped chip box, then nudge overlapping chips
 * downward so no two collide and none spill past the pane edges. Chips stay
 * pinned to their anchor's x (the meaningful axis); only y is decluttered.
 */
function layoutChips(
  ctx: CanvasRenderingContext2D,
  reqs: LabelReq[],
  paneW: number,
  paneH: number,
): PlacedChip[] {
  const chips: PlacedChip[] = reqs.map((r) => {
    const width = chipWidth(ctx, r.text);
    let left =
      r.align === 'center'
        ? r.anchorX - width / 2
        : r.align === 'right'
          ? r.anchorX - width
          : r.anchorX;
    left = Math.max(EDGE, Math.min(left, paneW - width - EDGE));
    let top = r.anchorY - CHIP_H / 2;
    top = Math.max(EDGE, Math.min(top, paneH - CHIP_H - EDGE));
    return { ...r, left, top, width };
  });

  // Greedy top-down declutter: each chip drops below every earlier chip it
  // would overlap horizontally. Earlier = smaller resolved top, so the chips
  // we compare against are already final.
  chips.sort((a, b) => a.top - b.top || a.left - b.left);
  for (let i = 0; i < chips.length; i++) {
    const cur = chips[i];
    let top = cur.top;
    for (let j = 0; j < i; j++) {
      const prev = chips[j];
      const xOverlap =
        cur.left < prev.left + prev.width + CHIP_GAP &&
        prev.left < cur.left + cur.width + CHIP_GAP;
      if (xOverlap) top = Math.max(top, prev.top + CHIP_H + CHIP_GAP);
    }
    if (top + CHIP_H > paneH - EDGE) top = Math.max(EDGE, paneH - EDGE - CHIP_H);
    cur.top = top;
  }
  return chips;
}

function drawChip(
  ctx: CanvasRenderingContext2D,
  chip: PlacedChip,
  pal: ChipPalette,
): void {
  const { left, top, width } = chip;
  const cy = top + CHIP_H / 2;

  // Frosted background with a soft drop shadow for separation from candles.
  ctx.save();
  ctx.shadowColor = pal.shadow;
  ctx.shadowBlur = 7;
  ctx.shadowOffsetY = 1.5;
  roundRectPath(ctx, left, top, width, CHIP_H, CHIP_RADIUS);
  ctx.fillStyle = pal.bg;
  ctx.fill();
  ctx.restore();

  // Hairline border.
  roundRectPath(ctx, left + 0.5, top + 0.5, width - 1, CHIP_H - 1, CHIP_RADIUS);
  ctx.strokeStyle = pal.border;
  ctx.lineWidth = 1;
  ctx.stroke();

  // Accent dot keyed to the annotation color (forced opaque for a crisp dot).
  const dotX = left + CHIP_PAD_X + DOT_R;
  ctx.beginPath();
  ctx.arc(dotX, cy, DOT_R, 0, Math.PI * 2);
  ctx.fillStyle = withAlpha(chip.accent, 1);
  ctx.fill();

  // Label text in high-contrast ink.
  ctx.font = CHIP_FONT;
  ctx.textAlign = 'left';
  ctx.textBaseline = 'middle';
  ctx.fillStyle = pal.ink;
  ctx.fillText(chip.text, dotX + DOT_R + DOT_GAP, cy + 0.5);
}

export class AgentAnnotationsPrimitive {
  private _data: AgentAnnotationsData = EMPTY;
  private _theme: AnnotationTheme = 'dark';
  private _chart: IChartApiBase<Time> | null = null;
  private _series: SeriesLike | null = null;
  private _requestUpdate: (() => void) | null = null;

  attached({ chart, series, requestUpdate }: SeriesAttachedParams): void {
    this._chart = chart;
    this._series = series;
    this._requestUpdate = requestUpdate;
  }

  detached(): void {
    this._chart = null;
    this._series = null;
    this._requestUpdate = null;
  }

  setData(data: AgentAnnotationsData): void {
    this._data = data;
    this._requestUpdate?.();
  }

  /** Switch the chip palette between light/dark; redraws if it changed. */
  setTheme(theme: AnnotationTheme): void {
    if (this._theme === theme) return;
    this._theme = theme;
    this._requestUpdate?.();
  }

  updateAllViews(): void {}

  paneViews(): ISeriesPrimitivePaneView[] {
    const source = this;
    // Two views: rectangle fills sit below the candles; everything else
    // (borders, lines, text, fib levels) draws on top.
    return [
      {
        zOrder(): SeriesPrimitivePaneViewZOrder {
          return 'bottom';
        },
        renderer(): ISeriesPrimitivePaneRenderer {
          return {
            draw(target: CanvasRenderingTarget2D): void {
              source._drawFills(target);
            },
          };
        },
      },
      {
        zOrder(): SeriesPrimitivePaneViewZOrder {
          return 'top';
        },
        renderer(): ISeriesPrimitivePaneRenderer {
          return {
            draw(target: CanvasRenderingTarget2D): void {
              source._drawForeground(target);
            },
          };
        },
      },
    ];
  }

  private _drawFills(target: CanvasRenderingTarget2D): void {
    const chart = this._chart;
    const series = this._series;
    if (!chart || !series) return;
    const { rects } = this._data;
    if (rects.length === 0) return;

    target.useMediaCoordinateSpace(({ context: ctx, mediaSize }) => {
      const timeScale = chart.timeScale();
      for (const r of rects) {
        const box = rectToBox(timeScale, series, r, mediaSize.width);
        if (!box) continue;
        ctx.fillStyle = withAlpha(r.color, 0.1);
        ctx.fillRect(box.left, box.top, box.width, box.height);
      }
    });
  }

  private _drawForeground(target: CanvasRenderingTarget2D): void {
    const chart = this._chart;
    const series = this._series;
    if (!chart || !series) return;
    const { rects, vlines, texts, fibs } = this._data;
    if (!rects.length && !vlines.length && !texts.length && !fibs.length) return;

    const pal = CHIP_PALETTE[this._theme];

    target.useMediaCoordinateSpace(({ context: ctx, mediaSize }) => {
      const timeScale = chart.timeScale();
      // All labels are collected, then placed + drawn last, so chips sit above
      // every stroke and a single declutter pass keeps them from colliding.
      const labels: LabelReq[] = [];

      // Rectangle borders (fills are drawn in the bottom pane view).
      for (const r of rects) {
        const box = rectToBox(timeScale, series, r, mediaSize.width);
        if (!box) continue;
        ctx.save();
        ctx.strokeStyle = withAlpha(r.color, 0.7);
        ctx.lineWidth = 1;
        ctx.strokeRect(box.left, box.top, box.width, box.height);
        ctx.restore();
        if (r.label) {
          labels.push({
            text: r.label,
            accent: r.color,
            anchorX: box.left + 2,
            anchorY: box.top + CHIP_H / 2 + 2,
            align: 'left',
          });
        }
      }

      // Vertical lines.
      for (const v of vlines) {
        const x = timeScale.timeToCoordinate(v.time as unknown as Time);
        if (x == null) continue;
        ctx.save();
        ctx.strokeStyle = withAlpha(v.color, 0.8);
        ctx.lineWidth = 1;
        ctx.setLineDash(v.dash);
        ctx.beginPath();
        ctx.moveTo(x, 0);
        ctx.lineTo(x, mediaSize.height);
        ctx.stroke();
        ctx.restore();
        if (v.label) {
          labels.push({
            text: v.label,
            accent: v.color,
            anchorX: x,
            anchorY: EDGE + CHIP_H / 2,
            align: 'center',
          });
        }
      }

      // Fibonacci retracement levels.
      for (const f of fibs) {
        const x1 = timeScale.timeToCoordinate(f.time1 as unknown as Time);
        const x2 = timeScale.timeToCoordinate(f.time2 as unknown as Time);
        const left = Math.min(x1 ?? 0, x2 ?? mediaSize.width);
        const right = Math.max(x1 ?? 0, x2 ?? mediaSize.width);
        for (const lvl of f.levels) {
          const y = series.priceToCoordinate(lvl.price);
          if (y == null) continue;
          ctx.save();
          ctx.strokeStyle = withAlpha(f.color, 0.55);
          ctx.lineWidth = 1;
          ctx.setLineDash([2, 3]);
          ctx.beginPath();
          ctx.moveTo(left, y);
          ctx.lineTo(right, y);
          ctx.stroke();
          ctx.restore();
          labels.push({
            text: `${lvl.ratio} · ${lvl.price.toFixed(2)}`,
            accent: f.color,
            anchorX: right,
            anchorY: y,
            align: 'right',
          });
        }
      }

      // Free-floating text.
      for (const t of texts) {
        if (!t.text) continue;
        const x = timeScale.timeToCoordinate(t.time as unknown as Time);
        const y = series.priceToCoordinate(t.price);
        if (x == null || y == null) continue;
        labels.push({
          text: t.text,
          accent: t.color,
          anchorX: x,
          anchorY: y,
          align: 'center',
        });
      }

      const chips = layoutChips(ctx, labels, mediaSize.width, mediaSize.height);
      for (const chip of chips) drawChip(ctx, chip, pal);
    });
  }
}

interface Box {
  left: number;
  top: number;
  width: number;
  height: number;
}

interface TimeScaleLike {
  timeToCoordinate(time: Time): number | null;
}

/** Convert a rect item to viewport-clipped pixel box, or null if undrawable. */
function rectToBox(
  timeScale: TimeScaleLike,
  series: SeriesLike,
  r: RectItem,
  width: number,
): Box | null {
  const xa = timeScale.timeToCoordinate(r.time1 as unknown as Time);
  const xb = timeScale.timeToCoordinate(r.time2 as unknown as Time);
  // Clip horizontally to the viewport when a corner is off-screen.
  const x1 = xa ?? 0;
  const x2 = xb ?? width;
  const left = Math.max(0, Math.min(x1, x2));
  const right = Math.min(width, Math.max(x1, x2));
  const ya = series.priceToCoordinate(r.price1);
  const yb = series.priceToCoordinate(r.price2);
  if (ya == null || yb == null) return null;
  const top = Math.min(ya, yb);
  const bottom = Math.max(ya, yb);
  if (right <= left || bottom <= top) return null;
  return { left, top, width: right - left, height: bottom - top };
}

/**
 * Apply an alpha to a CSS color. Handles #rgb/#rrggbb/#rrggbbaa and
 * rgb()/rgba(); for anything else returns the color unchanged (so named
 * colors still render, just without the requested transparency).
 */
function withAlpha(color: string, alpha: number): string {
  const c = color.trim();
  if (c.startsWith('#')) {
    const hex = c.slice(1);
    let r: number;
    let g: number;
    let b: number;
    if (hex.length === 3) {
      r = parseInt(hex[0] + hex[0], 16);
      g = parseInt(hex[1] + hex[1], 16);
      b = parseInt(hex[2] + hex[2], 16);
    } else if (hex.length === 6 || hex.length === 8) {
      r = parseInt(hex.slice(0, 2), 16);
      g = parseInt(hex.slice(2, 4), 16);
      b = parseInt(hex.slice(4, 6), 16);
    } else {
      return color;
    }
    if ([r, g, b].some((n) => Number.isNaN(n))) return color;
    return `rgba(${r},${g},${b},${alpha})`;
  }
  const rgbMatch = c.match(/^rgba?\(([^)]+)\)$/i);
  if (rgbMatch) {
    const parts = rgbMatch[1].split(',').map((p) => p.trim());
    if (parts.length >= 3) {
      return `rgba(${parts[0]},${parts[1]},${parts[2]},${alpha})`;
    }
  }
  return color;
}
