/** Shades the area between two series of a pane; attached to series `a`. Each
 *  boundary is placed on its own series' scale: in the price pane a histogram
 *  rides a scale of its own, so one scale for both would miss its side. */

import type {
  IChartApiBase,
  IPrimitivePaneRenderer,
  IPrimitivePaneView,
  PrimitivePaneViewZOrder,
  Time,
  UTCTimestamp,
} from 'lightweight-charts';
import type { CanvasRenderingTarget2D } from 'fancy-canvas';

import { tracePath } from '../canvasKit';
import type { XY } from './canvasGeom';
import type { SeriesLike } from './painter';
import type { FillPoint } from './seriesData';

interface AttachedParams {
  chart: IChartApiBase<Time>;
  series: SeriesLike;
  requestUpdate: () => void;
}

export class LayerFillPrimitive {
  private _points: readonly FillPoint[] = [];
  private _color = 'rgba(0,0,0,0)';
  private _chart: IChartApiBase<Time> | null = null;
  private _series: SeriesLike | null = null;
  private _other: SeriesLike | null = null;
  private _requestUpdate: (() => void) | null = null;

  attached({ chart, series, requestUpdate }: AttachedParams): void {
    this._chart = chart;
    this._series = series;
    this._requestUpdate = requestUpdate;
  }

  detached(): void {
    this._chart = null;
    this._series = null;
    this._other = null;
    this._requestUpdate = null;
  }

  setFill(points: readonly FillPoint[], color: string, other: SeriesLike | null = null): void {
    this._points = points;
    this._color = color;
    this._other = other;
    this._requestUpdate?.();
  }

  updateAllViews(): void {}

  paneViews(): IPrimitivePaneView[] {
    const source = this;
    return [
      {
        zOrder: (): PrimitivePaneViewZOrder => 'bottom',
        renderer: (): IPrimitivePaneRenderer => ({
          draw(target: CanvasRenderingTarget2D) {
            const chart = source._chart;
            const series = source._series;
            const other = source._other ?? series;
            const pts = source._points;
            if (!chart || !series || pts.length < 2) return;
            const ts = chart.timeScale();
            target.useMediaCoordinateSpace(({ context: ctx, mediaSize }) => {
              let upper: XY[] = [];
              let lower: XY[] = [];
              const flush = () => {
                if (upper.length >= 2) {
                  tracePath(ctx, [...upper, ...[...lower].reverse()], true);
                  ctx.fillStyle = source._color;
                  ctx.fill();
                }
                upper = [];
                lower = [];
              };
              const push = (x: number, p: FillPoint): boolean => {
                const ya = series.priceToCoordinate(p.a);
                const yb = other?.priceToCoordinate(p.b) ?? null;
                if (ya == null || yb == null) return false;
                upper.push({ x, y: ya });
                lower.push({ x, y: yb });
                return true;
              };
              let offLeft: { x: number; p: FillPoint } | null = null;
              for (const p of pts) {
                if (p.breakBefore) {
                  flush();
                  offLeft = null;
                }
                const x = ts.timeToCoordinate(p.time as UTCTimestamp);
                if (x == null) continue;
                if (x < 0) {
                  offLeft = { x, p };
                  continue;
                }
                // The last point before the edge, so the area reaches it.
                if (offLeft && !upper.length) push(offLeft.x, offLeft.p);
                if (x > mediaSize.width + 50) {
                  // One point past the edge keeps the area running off-screen.
                  push(x, p);
                  break;
                }
                push(x, p);
              }
              flush();
            });
          },
        }),
      },
    ];
  }
}
