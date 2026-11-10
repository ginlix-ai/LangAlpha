/**
 * Canvas helpers for the chart layer renderer (`chartLayer/`): the drawing,
 * measuring and color math its painters and labels share.
 */

// ─── Color ───────────────────────────────────────────────────────────

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

/** A CSS color as channels (0-255); `a` is null when the color carries no alpha of its own. */
export interface ParsedColor {
  r: number;
  g: number;
  b: number;
  a: number | null;
}

function channel(part: string): number | null {
  const v = part.endsWith('%') ? (parseFloat(part) * 255) / 100 : parseFloat(part);
  return Number.isFinite(v) ? Math.round(v) : null;
}

function parseDirect(c: string): ParsedColor | null {
  if (c.startsWith('#')) {
    const hex = c.slice(1);
    if (!/^(?:[0-9a-f]{3,4}|[0-9a-f]{6}|[0-9a-f]{8})$/i.test(hex)) return null;
    const short = hex.length <= 4;
    const full = short ? hex.replace(/./g, (ch) => ch + ch) : hex;
    const hasA = hex.length === 4 || hex.length === 8;
    return {
      r: parseInt(full.slice(0, 2), 16),
      g: parseInt(full.slice(2, 4), 16),
      b: parseInt(full.slice(4, 6), 16),
      a: hasA ? parseInt(full.slice(6, 8), 16) / 255 : null,
    };
  }
  const m = c.match(/^rgba?\(([^)]+)\)$/i);
  if (!m) return null;
  const parts = m[1].split(/[\s,/]+/).filter(Boolean);
  if (parts.length < 3) return null;
  const [r, g, b] = parts.slice(0, 3).map(channel);
  if (r == null || g == null || b == null) return null;
  if (parts.length < 4) return { r, g, b, a: null };
  const a = parts[3].endsWith('%') ? parseFloat(parts[3]) / 100 : parseFloat(parts[3]);
  return Number.isFinite(a) ? { r, g, b, a } : null;
}

/**
 * The one reading of a CSS color: #rgb, #rgba, #rrggbb, #rrggbbaa, rgb()/rgba()
 * (comma or space separated), and named colors when a canvas can normalize
 * them. Null for anything else.
 */
export function parseColor(color: string): ParsedColor | null {
  const c = color.trim();
  const direct = parseDirect(c);
  if (direct) return direct;
  const resolved = resolveNamedColor(c);
  return resolved && resolved !== c ? parseDirect(resolved) : null;
}

/** Apply an alpha to a CSS color (see `parseColor`); one that cannot be read comes back unchanged. */
export function withAlpha(color: string, alpha: number): string {
  const c = parseColor(color);
  return c ? `rgba(${c.r},${c.g},${c.b},${alpha})` : color;
}

// ─── Text ────────────────────────────────────────────────────────────

// A font and a text always measure the same, and labels repaint on every tick,
// pan and zoom: cache the widths. Cleared wholesale past the cap, which costs
// one re-measure pass.
const TEXT_WIDTH_CACHE_MAX = 1024;
const textWidthCache = new Map<string, number>();

/** Width of `text` in `font`. Sets `ctx.font` only when it has to measure. */
export function measureText(ctx: CanvasRenderingContext2D, font: string, text: string): number {
  const key = `${font}\u0000${text}`;
  const cached = textWidthCache.get(key);
  if (cached !== undefined) return cached;
  ctx.font = font;
  const width = ctx.measureText(text).width;
  if (textWidthCache.size >= TEXT_WIDTH_CACHE_MAX) textWidthCache.clear();
  textWidthCache.set(key, width);
  return width;
}

// ─── Shapes ──────────────────────────────────────────────────────────

/** Trace a rounded-rectangle path (caller fills/strokes). */
export function roundRectPath(
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

/** Trace a polyline through `pts` as a new path, closed back to its start when asked. */
export function tracePath(
  ctx: CanvasRenderingContext2D,
  pts: readonly { x: number; y: number }[],
  close = false,
): void {
  ctx.beginPath();
  ctx.moveTo(pts[0].x, pts[0].y);
  for (let k = 1; k < pts.length; k++) ctx.lineTo(pts[k].x, pts[k].y);
  if (close) ctx.closePath();
}

// ─── Label layout ────────────────────────────────────────────────────

export type LabelAlign = 'left' | 'center' | 'right';

/** Left edge of a box `width` wide anchored at `x` by `align`. */
export function alignedLeft(x: number, align: LabelAlign, width: number): number {
  return align === 'center' ? x - width / 2 : align === 'right' ? x - width : x;
}

/** Keep a span of `size` starting at `v` inside `[edge, extent - edge]`. */
export function clampSpan(v: number, size: number, extent: number, edge: number): number {
  return Math.max(edge, Math.min(v, extent - size - edge));
}

// ─── Chips ───────────────────────────────────────────────────────────

/** A chip's geometry: a rounded box with an accent dot and its text. */
export interface ChipStyle {
  font: string;
  height: number;
  padX: number;
  dotR: number;
  dotGap: number;
  /** Corner radius; `pill` rounds the ends fully. */
  radius: number | 'pill';
  shadowBlur: number;
  shadowOffsetY: number;
}

/** A chip's colors for one paint. */
interface ChipColors {
  bg: string;
  border: string;
  ink: string;
  shadow: string;
  /** The dot, keyed to the element's own color. */
  accent: string;
}

export function chipWidth(ctx: CanvasRenderingContext2D, style: ChipStyle, text: string): number {
  return style.padX * 2 + style.dotR * 2 + style.dotGap + measureText(ctx, style.font, text);
}

/** Paint a chip whose box starts at (`left`, `top`): shadowed fill, hairline border, dot, text. */
export function drawChip(
  ctx: CanvasRenderingContext2D,
  style: ChipStyle,
  left: number,
  top: number,
  width: number,
  text: string,
  colors: ChipColors,
): void {
  const h = style.height;
  const outer = style.radius === 'pill' ? h / 2 : style.radius;
  const inner = style.radius === 'pill' ? (h - 1) / 2 : style.radius;
  ctx.save();
  ctx.shadowColor = colors.shadow;
  ctx.shadowBlur = style.shadowBlur;
  ctx.shadowOffsetY = style.shadowOffsetY;
  roundRectPath(ctx, left, top, width, h, outer);
  ctx.fillStyle = colors.bg;
  ctx.fill();
  ctx.restore();
  roundRectPath(ctx, left + 0.5, top + 0.5, width - 1, h - 1, inner);
  ctx.strokeStyle = colors.border;
  ctx.lineWidth = 1;
  ctx.stroke();
  const cy = top + h / 2;
  const dotX = left + style.padX + style.dotR;
  ctx.beginPath();
  ctx.arc(dotX, cy, style.dotR, 0, Math.PI * 2);
  ctx.fillStyle = withAlpha(colors.accent, 1);
  ctx.fill();
  ctx.font = style.font;
  ctx.textAlign = 'left';
  ctx.textBaseline = 'middle';
  ctx.fillStyle = colors.ink;
  ctx.fillText(text, dotX + style.dotR + style.dotGap, cy + 0.5);
}

/** Internals for unit tests only. */
export const __testing = { textWidthCache, namedColorCache, resetColorResolver };
