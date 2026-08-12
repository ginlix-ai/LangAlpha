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

// Frosted chip surfaces tuned to the two chart backgrounds (#232426 dark /
// #FFFFFF light). The annotation's own color is demoted to a small accent dot
// so the label text stays high-contrast ink on whatever palette the agent picks
// — the single biggest legibility win over stamping raw color on a dark box.
// Canvas-drawn, so these are literals; they track the bg-card / text-primary
// tokens and the hairline border weight of the card system.
const CHIP_PALETTE: Record<AnnotationTheme, ChipPalette> = {
  dark: {
    bg: 'rgba(25, 25, 25, 0.92)',
    border: 'rgba(255, 255, 255, 0.16)',
    ink: '#E6E6E4',
    shadow: 'rgba(0, 0, 0, 0.45)',
  },
  light: {
    bg: 'rgba(255, 255, 255, 0.94)',
    border: 'rgba(31, 31, 30, 0.16)',
    ink: '#1F1F1E',
    shadow: 'rgba(31, 31, 30, 0.18)',
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

// CHIP_FONT is a module constant, so a given text always measures to the same
// width. Cache measured text widths so we don't re-run measureText for every
// label on every paint (every tick/pan/zoom). Capped to bound memory across
// many distinct labels; on overflow we clear wholesale (cheaper than LRU and
// the cost is one re-measure pass, which is what we were paying every frame
// before this cache existed).
const TEXT_WIDTH_CACHE_MAX = 512;
const textWidthCache = new Map<string, number>();

function measureTextWidth(ctx: CanvasRenderingContext2D, text: string): number {
  const cached = textWidthCache.get(text);
  if (cached !== undefined) return cached;
  ctx.font = CHIP_FONT;
  const width = ctx.measureText(text).width;
  if (textWidthCache.size >= TEXT_WIDTH_CACHE_MAX) textWidthCache.clear();
  textWidthCache.set(text, width);
  return width;
}

function chipWidth(ctx: CanvasRenderingContext2D, text: string): number {
  return CHIP_PAD_X * 2 + DOT_R * 2 + DOT_GAP + measureTextWidth(ctx, text);
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
      const range = getVisibleSecondsRange(chart);
      for (const r of rects) {
        const box = rectToBox(timeScale, series, r, mediaSize.width, range);
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
      const range = getVisibleSecondsRange(chart);
      // All labels are collected, then placed + drawn last, so chips sit above
      // every stroke and a single declutter pass keeps them from colliding.
      const labels: LabelReq[] = [];

      // Rectangle borders (fills are drawn in the bottom pane view).
      for (const r of rects) {
        const box = rectToBox(timeScale, series, r, mediaSize.width, range);
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
        if (!range) continue;
        const x1 = timeScale.timeToCoordinate(f.time1 as unknown as Time);
        const x2 = timeScale.timeToCoordinate(f.time2 as unknown as Time);
        // Both anchors off-screen on the same side would clamp to a phantom
        // full-pane line; skip those (opposite-side straddles still span below).
        if (shouldSkipOffscreenSpan(range, x1, f.time1, x2, f.time2)) continue;
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

/** Visible time range as unix-second numbers (matches stored item times). */
export interface VisibleTimeRange {
  from: number;
  to: number;
}

/**
 * Read the chart's visible time range as unix-second numbers, or null if the
 * chart has no data. Times in this primitive are stored as unix seconds and the
 * chart uses UTCTimestamp (numeric) times, so `from`/`to` come back numeric —
 * mirroring how `ExtendedHoursBgPrimitive` compares them.
 */
function getVisibleSecondsRange(chart: IChartApiBase<Time>): VisibleTimeRange | null {
  const range = chart.timeScale().getVisibleRange();
  if (!range) return null;
  const from = range.from as unknown as number;
  const to = range.to as unknown as number;
  if (typeof from !== 'number' || typeof to !== 'number') return null;
  return { from, to };
}

/**
 * Decide whether a two-anchor shape (rect / fib) should be skipped because both
 * of its anchors lie off-screen on the SAME side of the viewport — in which case
 * clamping each null coordinate to 0/width would paint a phantom full-pane shape.
 *
 * Returns true (skip) only when both anchors are off-screen AND on the same side.
 * When the anchors straddle the viewport (one off-left, one off-right) the shape
 * genuinely spans the pane, so we return false and let the caller draw full-width.
 * On-screen anchors (non-null coordinate) never trigger a skip.
 */
export function shouldSkipOffscreenSpan(
  range: VisibleTimeRange,
  coordA: number | null,
  timeA: number,
  coordB: number | null,
  timeB: number,
): boolean {
  if (coordA != null || coordB != null) return false; // at least one on-screen
  const sideA = classifyOffscreenSide(range, timeA);
  const sideB = classifyOffscreenSide(range, timeB);
  // Both off-screen on the same, known side → phantom; skip.
  return sideA !== 0 && sideA === sideB;
}

/** -1 = off-left (before `from`), +1 = off-right (after `to`), 0 = inside/unknown. */
function classifyOffscreenSide(range: VisibleTimeRange, time: number): -1 | 0 | 1 {
  if (time < range.from) return -1;
  if (time > range.to) return 1;
  return 0;
}

/** Convert a rect item to viewport-clipped pixel box, or null if undrawable. */
function rectToBox(
  timeScale: TimeScaleLike,
  series: SeriesLike,
  r: RectItem,
  width: number,
  range: VisibleTimeRange | null,
): Box | null {
  if (!range) return null;
  const xa = timeScale.timeToCoordinate(r.time1 as unknown as Time);
  const xb = timeScale.timeToCoordinate(r.time2 as unknown as Time);
  // Both corners off-screen on the same side would clamp to a phantom full-pane
  // box; skip those (opposite-side straddles still draw full-width below).
  if (shouldSkipOffscreenSpan(range, xa, r.time1, xb, r.time2)) return null;
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

// Lazily-created 2d context used only to normalize named CSS colors (e.g.
// "tomato" → "#ff6347"). Resolution results are cached. `null` means we tried
// and there's no usable canvas (non-DOM / jsdom without a canvas backend), so
// we stop trying and fall back to returning named colors unchanged.
let colorResolverCtx: CanvasRenderingContext2D | null | undefined;
const NAMED_COLOR_CACHE_MAX = 512;
const namedColorCache = new Map<string, string | null>();

function getColorResolverCtx(): CanvasRenderingContext2D | null {
  if (colorResolverCtx !== undefined) return colorResolverCtx;
  try {
    if (typeof document === 'undefined') {
      colorResolverCtx = null;
      return null;
    }
    colorResolverCtx = document.createElement('canvas').getContext('2d');
  } catch {
    colorResolverCtx = null;
  }
  return colorResolverCtx;
}

/** Drop the lazily-created resolver context so it is re-acquired on next use. */
function resetColorResolver(): void {
  colorResolverCtx = undefined;
}

/**
 * Resolve a named/unknown CSS color to a normalized form the hex/rgb fast paths
 * can re-parse (browsers return `#rrggbb` or `rgb(...)`). Returns null when no
 * canvas is available or the value isn't a recognized color. Cached per input.
 */
function resolveNamedColor(color: string): string | null {
  const cached = namedColorCache.get(color);
  if (cached !== undefined) return cached;
  if (namedColorCache.size >= NAMED_COLOR_CACHE_MAX) namedColorCache.clear();
  const ctx = getColorResolverCtx();
  if (!ctx) {
    namedColorCache.set(color, null);
    return null;
  }
  // Setting fillStyle to an unparseable value is a no-op, so seed with a sentinel
  // and detect rejection by the value not changing away from it.
  const SENTINEL = '#010203';
  ctx.fillStyle = SENTINEL;
  ctx.fillStyle = color;
  const normalized = ctx.fillStyle;
  const resolved =
    typeof normalized === 'string' && normalized.toLowerCase() !== SENTINEL
      ? normalized
      : null;
  namedColorCache.set(color, resolved);
  return resolved;
}

/**
 * Apply an alpha to a CSS color. Handles #rgb/#rrggbb/#rrggbbaa, rgb()/rgba(),
 * and named CSS colors (resolved via a canvas context when one is available).
 * Falls back to returning the color unchanged only when it can't be parsed and
 * no canvas is available to normalize it.
 */
function withAlpha(color: string, alpha: number): string {
  return withAlphaInner(color, alpha, true);
}

function withAlphaInner(color: string, alpha: number, allowNamed: boolean): string {
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
  // Named/unknown color: normalize via canvas, then re-run the fast paths on the
  // normalized hex/rgb. `allowNamed` guards against infinite recursion if a
  // resolver ever returns another unparseable string.
  if (allowNamed) {
    const resolved = resolveNamedColor(c);
    if (resolved && resolved !== c) {
      return withAlphaInner(resolved, alpha, false);
    }
  }
  return color;
}

/**
 * Internal helpers exposed only for unit tests. Not part of the public API —
 * do not import from application code.
 */
export const __testing = {
  chipWidth,
  withAlpha,
  textWidthCache,
  namedColorCache,
  resetColorResolver,
};
