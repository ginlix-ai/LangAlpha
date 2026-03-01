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

export class ExtendedHoursBgPrimitive {
  constructor() {
    this._regions = [];
    this._colors = { pre: 'rgba(251,191,36,0.12)', post: 'rgba(59,130,246,0.15)' };
    this._chart = null;
    this._requestUpdate = null;
  }

  attached({ chart, requestUpdate }) {
    this._chart = chart;
    this._requestUpdate = requestUpdate;
  }

  detached() {
    this._chart = null;
    this._requestUpdate = null;
  }

  setRegions(regions) {
    this._regions = regions;
    this._requestUpdate?.();
  }

  setColors(colors) {
    this._colors = colors;
    this._requestUpdate?.();
  }

  updateAllViews() {}

  paneViews() {
    const source = this;
    return [{
      zOrder() { return 'bottom'; },
      renderer() {
        return {
          draw(target) {
            const { _chart: chart, _regions: regions, _colors: colors } = source;
            if (!chart || regions.length === 0) return;

            target.useMediaCoordinateSpace(({ context: ctx, mediaSize }) => {
              const timeScale = chart.timeScale();
              const visibleRange = timeScale.getVisibleRange();
              if (!visibleRange) return;

              for (const { start, end, type } of regions) {
                // Skip regions completely outside visible range
                if (end < visibleRange.from || start > visibleRange.to) continue;

                let x1 = timeScale.timeToCoordinate(start);
                let x2 = timeScale.timeToCoordinate(end);

                // Clip to viewport edges when region extends beyond visible area
                if (x1 === null) x1 = 0;
                if (x2 === null) x2 = mediaSize.width;

                // Small padding to cover full bar widths at region edges
                const left = Math.max(0, x1 - 1);
                const right = Math.min(mediaSize.width, x2 + 1);
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
