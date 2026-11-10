/**
 * Regime shading: the price pane's (under the candles, with a firm strip along
 * the floor and a caption) and every other pane's (`LayerRegimePanePrimitive`),
 * so a shaded range reads across the whole chart.
 */

import type {
  IChartApiBase,
  IPanePrimitive,
  IPrimitivePaneRenderer,
  PaneAttachedParameter,
  PrimitivePaneViewZOrder,
  Time,
} from 'lightweight-charts';
import type { CanvasRenderingTarget2D } from 'fancy-canvas';

import { withAlpha } from '../canvasKit';
import { EDGE, TAG_H } from './canvasLabels';
import type { Painter } from './painter';
import type { SceneRegime } from './scene';
import { makeXMapper, type GridAnchor, type XMapper } from './xMapper';

const REGIME_STRIP = 3;

/** Shade each regime's run across a pane `w` × `h`; `strip` marks the run along the floor too. */
export function paintRegimes(
  ctx: CanvasRenderingContext2D,
  xm: XMapper,
  regimes: readonly SceneRegime[],
  w: number,
  h: number,
  strip: boolean,
): void {
  for (const r of regimes) {
    const x0 = xm.x(r.i0);
    const x1 = xm.x(r.i1);
    if (x0 == null || x1 == null) continue;
    const left = Math.max(-1, Math.min(x0, x1));
    const right = Math.min(w + 1, Math.max(x0, x1));
    if (right <= left) continue;
    ctx.fillStyle = r.fill;
    ctx.fillRect(left, 0, right - left, h);
    if (!strip) continue;
    // A firm edge along the floor marks the run itself; the wash alone is
    // too faint to say where it starts and stops.
    ctx.fillStyle = withAlpha(r.color, 0.55);
    ctx.fillRect(left, h - REGIME_STRIP, right - left, REGIME_STRIP);
  }
}

/**
 * A label names the state once, on its widest visible run: eleven runs of
 * "Above EMA 200" need one caption, not eleven.
 */
export function regimeLabels(p: Painter, regimes: readonly SceneRegime[]): void {
  const best = new Map<string, { r: SceneRegime; left: number; span: number }>();
  for (const r of regimes) {
    if (!r.label) continue;
    const x0 = p.X(r.i0);
    const x1 = p.X(r.i1);
    if (x0 == null || x1 == null) continue;
    const left = Math.max(0, Math.min(x0, x1));
    const span = Math.min(p.w, Math.max(x0, x1)) - left;
    if (span <= 0) continue;
    const seen = best.get(r.label);
    if (!seen || span > seen.span) best.set(r.label, { r, left, span });
  }
  for (const { r, left } of best.values()) {
    // Along the pane's floor, on the run's strip: the top-left corner belongs to the layer legend.
    p.label({ el: r, text: r.label!, style: 'tag', color: withAlpha(r.color, 1), x: Math.max(EDGE, left + 4), y: p.h - REGIME_STRIP - TAG_H / 2 - 3, align: 'left', priority: 10 });
  }
}

/** Shades regime ranges on a pane that has no candles (RSI, layer panes). */
export class LayerRegimePanePrimitive implements IPanePrimitive<Time> {
  private _regimes: readonly SceneRegime[] = [];
  private _anchor: GridAnchor | null = null;
  private _chart: IChartApiBase<Time> | null = null;
  private _requestUpdate: (() => void) | null = null;

  attached({ chart, requestUpdate }: PaneAttachedParameter<Time>): void {
    this._chart = chart;
    this._requestUpdate = requestUpdate;
  }

  detached(): void {
    this._chart = null;
    this._requestUpdate = null;
  }

  setRegimes(regimes: readonly SceneRegime[], anchor: GridAnchor | null): void {
    this._regimes = regimes;
    this._anchor = anchor;
    this._requestUpdate?.();
  }

  updateAllViews(): void {}

  paneViews() {
    const source = this;
    return [
      {
        zOrder: (): PrimitivePaneViewZOrder => 'bottom',
        renderer: (): IPrimitivePaneRenderer => ({
          draw(target: CanvasRenderingTarget2D) {
            const chart = source._chart;
            if (!chart || !source._regimes.length) return;
            const xm = makeXMapper(chart, source._anchor);
            if (!xm) return;
            target.useMediaCoordinateSpace(({ context: ctx, mediaSize }) => {
              paintRegimes(ctx, xm, source._regimes, mediaSize.width, mediaSize.height, false);
            });
          },
        }),
      },
    ];
  }
}
