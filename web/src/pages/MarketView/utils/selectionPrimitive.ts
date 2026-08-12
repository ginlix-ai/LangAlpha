/**
 * Lightweight-charts v4 series primitive that draws the user's *in-progress*
 * chart selection — the time×price region or price level they're picking to
 * ask the agent about. Mirrors `AgentAnnotationsPrimitive` / `ExtendedHoursBgPrimitive`.
 *
 * Two render sources:
 * - A live *draft* in pixel space (`setDraft`), updated imperatively while the
 *   user drags. Pan is disabled during a drag, so raw pixels are stable and
 *   need no coordinate conversion — cheap, no per-frame time/price lookups.
 * - A list of *committed* selections in time/price (`setCommitted`), converted
 *   per frame so every box/level tracks pan/zoom until sent or removed. The one
 *   being edited is flagged `active` and drawn with a stronger border.
 *
 * The selection accent (teal) is deliberately distinct from the agent's
 * slate-blue annotations so a user draft never reads as an agent drawing.
 */

import type {
  ISeriesPrimitivePaneView,
  ISeriesPrimitivePaneRenderer,
  SeriesPrimitivePaneViewZOrder,
  Time,
  IChartApiBase,
} from 'lightweight-charts';
import type { CanvasRenderingTarget2D } from 'fancy-canvas';

export type SelectionTheme = 'light' | 'dark';

/** Committed selection in chart units (unix-second times, raw prices). */
export interface CommittedSelection {
  type: 'region' | 'price_level';
  /** unix seconds — region only. */
  time1?: number;
  /** unix seconds — region only. */
  time2?: number;
  priceLow: number;
  /** For `price_level`, equals `priceLow`. */
  priceHigh: number;
  /** Drawn with a stronger border (the selection whose editor is open). */
  active?: boolean;
}

/** In-progress draft in media (CSS) pixels relative to the chart pane. */
export interface DraftPixels {
  type: 'region' | 'price_level';
  x1: number;
  y1: number;
  x2: number;
  y2: number;
}

const FILL = 'rgba(31, 184, 201, 0.12)';
const BORDER_ACTIVE = 'rgba(31, 184, 201, 0.98)';
const BORDER_IDLE = 'rgba(31, 184, 201, 0.6)';
const LINE_DASH = [5, 4];

interface SeriesLike {
  priceToCoordinate(price: number): number | null;
}

interface SeriesAttachedParams {
  chart: IChartApiBase<Time>;
  series: SeriesLike;
  requestUpdate: () => void;
}

interface TimeScaleLike {
  timeToCoordinate(time: Time): number | null;
}

/** Draw a dashed box + translucent fill given pixel corners. */
function drawBox(
  ctx: CanvasRenderingContext2D,
  left: number,
  top: number,
  width: number,
  height: number,
  active: boolean,
): void {
  if (width <= 0 || height <= 0) return;
  ctx.save();
  ctx.fillStyle = FILL;
  ctx.fillRect(left, top, width, height);
  ctx.strokeStyle = active ? BORDER_ACTIVE : BORDER_IDLE;
  ctx.lineWidth = active ? 1.5 : 1;
  ctx.setLineDash(LINE_DASH);
  ctx.strokeRect(left, top, width, height);
  ctx.restore();
}

/** Draw a full-width dashed horizontal guide at pixel y. */
function drawHLine(ctx: CanvasRenderingContext2D, y: number, paneW: number, active: boolean): void {
  ctx.save();
  ctx.strokeStyle = active ? BORDER_ACTIVE : BORDER_IDLE;
  ctx.lineWidth = active ? 1.5 : 1;
  ctx.setLineDash(LINE_DASH);
  ctx.beginPath();
  ctx.moveTo(0, y);
  ctx.lineTo(paneW, y);
  ctx.stroke();
  ctx.restore();
}

export class SelectionPrimitive {
  private _committed: CommittedSelection[] = [];
  private _draft: DraftPixels | null = null;
  private _theme: SelectionTheme = 'dark';
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

  /** Set the live pixel draft (during drag), or null to hide it. */
  setDraft(draft: DraftPixels | null): void {
    this._draft = draft;
    this._requestUpdate?.();
  }

  /** Set the committed time/price selections (drawn every frame). */
  setCommitted(sels: CommittedSelection[]): void {
    this._committed = sels;
    this._requestUpdate?.();
  }

  setTheme(theme: SelectionTheme): void {
    if (this._theme === theme) return;
    this._theme = theme;
    this._requestUpdate?.();
  }

  updateAllViews(): void {}

  paneViews(): ISeriesPrimitivePaneView[] {
    const source = this;
    return [
      {
        zOrder(): SeriesPrimitivePaneViewZOrder {
          return 'top';
        },
        renderer(): ISeriesPrimitivePaneRenderer {
          return {
            draw(target: CanvasRenderingTarget2D): void {
              source._draw(target);
            },
          };
        },
      },
    ];
  }

  private _draw(target: CanvasRenderingTarget2D): void {
    if (!this._draft && this._committed.length === 0) return;
    target.useMediaCoordinateSpace(({ context: ctx, mediaSize }) => {
      // Committed selections (time/price) — converted per frame so they track
      // pan/zoom. Each one with an open editor is emphasized.
      const chart = this._chart;
      const series = this._series;
      if (chart && series) {
        const timeScale: TimeScaleLike = chart.timeScale();
        for (const sel of this._committed) {
          if (sel.type === 'price_level') {
            const y = series.priceToCoordinate(sel.priceLow);
            if (y == null) continue;
            drawHLine(ctx, y, mediaSize.width, !!sel.active);
            continue;
          }
          if (sel.time1 == null || sel.time2 == null) continue;
          const xa = timeScale.timeToCoordinate(sel.time1 as unknown as Time);
          const xb = timeScale.timeToCoordinate(sel.time2 as unknown as Time);
          const ya = series.priceToCoordinate(sel.priceHigh);
          const yb = series.priceToCoordinate(sel.priceLow);
          if (ya == null || yb == null) continue;
          // Clip horizontally to the pane when a corner is off-screen.
          const x1 = xa ?? 0;
          const x2 = xb ?? mediaSize.width;
          const left = Math.max(0, Math.min(x1, x2));
          const right = Math.min(mediaSize.width, Math.max(x1, x2));
          const top = Math.min(ya, yb);
          const bottom = Math.max(ya, yb);
          drawBox(ctx, left, top, right - left, bottom - top, !!sel.active);
        }
      }

      // Live pixel draft (the in-progress drag) on top.
      if (this._draft) {
        const d = this._draft;
        if (d.type === 'price_level') {
          drawHLine(ctx, d.y2, mediaSize.width, true);
        } else {
          const left = Math.min(d.x1, d.x2);
          const top = Math.min(d.y1, d.y2);
          drawBox(ctx, left, top, Math.abs(d.x2 - d.x1), Math.abs(d.y2 - d.y1), true);
        }
      }
    });
  }
}
