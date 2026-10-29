/**
 * Lightweight-charts v4 series primitive that draws semi-transparent
 * background rectangles behind extended-hours time regions.
 *
 * Regions are tagged with type ('pre' or 'post') and each type gets
 * its own color (amber for pre-market, blue for after-hours).
 *
 * Usage:
 *   const prim = new ExtendedHoursBgPrimitive();
 *   candlestickSeries.attachPrimitive(prim);
 *   prim.setRegions(regions);         // [{start, end, type}]
 *   prim.setColors({ pre: '...', post: '...' });
 */

import type {
  IPrimitivePaneView,
  IPrimitivePaneRenderer,
  PrimitivePaneViewZOrder,
  Time,
  IChartApiBase,
} from 'lightweight-charts';
import type { CanvasRenderingTarget2D } from 'fancy-canvas';

export interface ExtendedHoursRegion {
  start: number;
  end: number;
  type: 'pre' | 'post';
}

export interface ExtendedHoursColors {
  pre: string;
  post: string;
}

interface SeriesAttachedParams {
  chart: IChartApiBase<Time>;
  requestUpdate: () => void;
}

export class ExtendedHoursBgPrimitive {
  private _regions: ExtendedHoursRegion[] = [];
  private _colors: ExtendedHoursColors = { pre: 'rgba(251,191,36,0.12)', post: 'rgba(59,130,246,0.15)' };
  private _chart: IChartApiBase<Time> | null = null;
  private _requestUpdate: (() => void) | null = null;

  attached({ chart, requestUpdate }: SeriesAttachedParams): void {
    this._chart = chart;
    this._requestUpdate = requestUpdate;
  }

  detached(): void {
    this._chart = null;
    this._requestUpdate = null;
  }

  setRegions(regions: ExtendedHoursRegion[]): void {
    this._regions = regions;
    this._requestUpdate?.();
  }

  setColors(colors: ExtendedHoursColors): void {
    this._colors = colors;
    this._requestUpdate?.();
  }

  updateAllViews(): void {}

  paneViews(): IPrimitivePaneView[] {
    const source = this;
    return [{
      zOrder(): PrimitivePaneViewZOrder { return 'bottom'; },
      renderer(): IPrimitivePaneRenderer {
        return {
          draw(target: CanvasRenderingTarget2D): void {
            const { _chart: chart, _regions: regions, _colors: colors } = source;
            if (!chart || regions.length === 0) return;

            target.useMediaCoordinateSpace(({ context: ctx, mediaSize }) => {
              const timeScale = chart.timeScale();
              const visibleRange = timeScale.getVisibleRange();
              if (!visibleRange) return;

              for (const { start, end, type } of regions) {
                // Skip regions completely outside visible range
                if (end < (visibleRange.from as unknown as number) || start > (visibleRange.to as unknown as number)) continue;

                let x1 = timeScale.timeToCoordinate(start as unknown as Time);
                let x2 = timeScale.timeToCoordinate(end as unknown as Time);

                // Clip to viewport edges when region extends beyond visible area
                const x1Val: number = x1 === null ? 0 : x1;
                const x2Val: number = x2 === null ? mediaSize.width : x2;

                // Pad by half a bar so the background covers full bar width at edges
                const halfBar = ((timeScale.options() as { barSpacing?: number }).barSpacing ?? 6) / 2;
                const left = Math.max(0, x1Val - halfBar);
                const right = Math.min(mediaSize.width, x2Val + halfBar);
                if (right > left) {
                  ctx.fillStyle = colors[type] || colors.post;
                  ctx.fillRect(left, 0, right - left, mediaSize.height);
                }
              }
            });
          },
        };
      },
    }];
  }
}
