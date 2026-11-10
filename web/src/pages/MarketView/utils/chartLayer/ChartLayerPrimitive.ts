/**
 * The drawing primitive on the candlestick series: every drawing, the regime
 * shading of the price pane, the end labels of price-pane series, and the
 * hover hit test. Fills (zones, channel bodies, cones, position boxes,
 * profiles, regimes) paint under the candles so bars stay crisp; strokes and
 * labels paint over them. Each drawing paints through its kind's record
 * (`kinds/`); the other panes' regimes are `LayerRegimePanePrimitive`, the
 * area between two series `LayerFillPrimitive`.
 */

import type {
  AutoscaleInfo,
  IChartApiBase,
  IPrimitivePaneRenderer,
  IPrimitivePaneView,
  Logical,
  PrimitiveHoveredItem,
  PrimitivePaneViewZOrder,
  Time,
} from 'lightweight-charts';
import type { CanvasRenderingTarget2D } from 'fancy-canvas';

import { withAlpha } from '../canvasKit';
import { NO_OBSTACLES } from './canvasLabels';
import { hitShapes, type HitShape, type LayerHit } from './hitShapes';
import { drawingKind } from './kinds';
import { frameStats, type LayerFrameReporter, type ReportContext } from './layerReport';
import { DEFAULT_LAYER_TEXT, type LayerText } from './layerText';
import { frameObstacles, labelObstacles } from './obstacles';
import { LAYER_FALLBACK, type LayerTheme } from './layerTheme';
import { EMPTY_PAINT_CONTEXT, Painter, type PaintContext, type SeriesLike } from './painter';
import { paintRegimes, regimeLabels } from './paneRegimes';
import { EMPTY_SCENE, type LayerScene } from './scene';
import { makeXMapper, visibleIndexRange, type GridAnchor } from './xMapper';

/** Pixels the price axis keeps past a forecast's target or a replay's end for its box. */
const BOX_ROOM = 34;
const BOXED_KINDS: ReadonlySet<string> = new Set(['forecast', 'analog']);

interface AttachedParams {
  chart: IChartApiBase<Time>;
  series: SeriesLike;
  requestUpdate: () => void;
}

export class ChartLayerPrimitive {
  private _scene: LayerScene = EMPTY_SCENE;
  private _paint: PaintContext = EMPTY_PAINT_CONTEXT;
  private _report: ReportContext | null = null;
  private _reporter: LayerFrameReporter | null = null;
  private _anchor: GridAnchor | null = null;
  private _theme: LayerTheme = LAYER_FALLBACK.dark;
  private _text: LayerText = DEFAULT_LAYER_TEXT;
  private _chart: IChartApiBase<Time> | null = null;
  private _series: SeriesLike | null = null;
  private _requestUpdate: (() => void) | null = null;
  private _hits: HitShape[] = [];
  private _hovered: string | null = null;

  attached({ chart, series, requestUpdate }: AttachedParams): void {
    this._chart = chart;
    this._series = series;
    this._requestUpdate = requestUpdate;
  }

  detached(): void {
    this._chart = null;
    this._series = null;
    this._requestUpdate = null;
    this._hits = [];
  }

  setScene(scene: LayerScene, anchor: GridAnchor | null): void {
    this._scene = scene;
    this._anchor = anchor;
    this._requestUpdate?.();
  }

  setPaintContext(ctx: PaintContext): void {
    this._paint = ctx;
    this._requestUpdate?.();
  }

  /**
   * Report each painted frame to `reporter`, named from `report` (null while no
   * layer is on the chart). Only a snapshot page sets one; without it no
   * report is computed.
   */
  setReporter(reporter: LayerFrameReporter | null, report: ReportContext | null): void {
    this._reporter = reporter;
    this._report = reporter ? report : null;
    this._requestUpdate?.();
  }

  setTheme(theme: LayerTheme): void {
    if (this._theme === theme) return;
    this._theme = theme;
    this._requestUpdate?.();
  }

  /** The renderer's own words in the reader's language (`buildLayerText`). */
  setText(text: LayerText): void {
    if (this._text === text) return;
    this._text = text;
    this._requestUpdate?.();
  }

  /** Emphasize one element (hovered or pinned in the explain overlay). */
  setHovered(key: string | null): void {
    if (this._hovered === key) return;
    this._hovered = key;
    this._requestUpdate?.();
  }

  /** Element under a pane-local point, from the last painted frame. */
  hitAt(x: number, y: number): LayerHit | null {
    return hitShapes(this._hits, x, y);
  }

  updateAllViews(): void {}

  /**
   * The price span of the projections in the visible time window, so the axis
   * fits a forecast's target or a replay's end above the candles instead of
   * cutting it off. Only a drawing that reaches past the last bar counts: one
   * on the history (a zone, a fib far from today's price) would squash the
   * candles to fit it, and the reader scrolls to it rather than the axis
   * stretching for it. A drawing unbounded in price (a ray, a vertical line)
   * adds nothing.
   */
  autoscaleInfo(start: Logical, end: Logical): AutoscaleInfo | null {
    const chart = this._chart;
    const anchor = this._anchor;
    if (!chart || !anchor || !this._scene.drawings.length) return null;
    const xm = makeXMapper(chart, anchor);
    if (!xm) return null;
    const from = xm.index(start);
    const to = xm.index(end);
    let lo = Infinity;
    let hi = -Infinity;
    let boxed = false;
    for (const d of this._scene.drawings) {
      const e = drawingKind(d.kind)?.extent(d);
      if (!e || e.anyPrice || e.i1 <= anchor.lastIndex) continue;
      if ((!e.openRight && e.i1 < from) || (!e.openLeft && e.i0 > to)) continue;
      lo = Math.min(lo, e.p0);
      hi = Math.max(hi, e.p1);
      boxed ||= BOXED_KINDS.has(d.kind);
    }
    if (!Number.isFinite(lo) || !Number.isFinite(hi)) return null;
    // A forecast's or a replay's box sits past its end, so leave the room for it.
    const room = boxed ? BOX_ROOM : 0;
    return { priceRange: { minValue: lo, maxValue: hi }, margins: { above: room, below: room } };
  }

  hitTest(x: number, y: number): PrimitiveHoveredItem | null {
    const hit = this.hitAt(x, y);
    if (!hit) return null;
    return {
      externalId: hit.key,
      zOrder: 'top',
      cursorStyle: 'pointer',
      distance: hit.distance,
      hitTestPriority: hit.priority,
    };
  }

  paneViews(): IPrimitivePaneView[] {
    const source = this;
    return [
      {
        zOrder: (): PrimitivePaneViewZOrder => 'bottom',
        renderer: (): IPrimitivePaneRenderer => ({
          draw: (target: CanvasRenderingTarget2D) => source._draw(target, 'under'),
        }),
      },
      {
        zOrder: (): PrimitivePaneViewZOrder => 'top',
        renderer: (): IPrimitivePaneRenderer => ({
          draw: (target: CanvasRenderingTarget2D) => source._draw(target, 'over'),
        }),
      },
    ];
  }

  private _draw(target: CanvasRenderingTarget2D, layer: 'under' | 'over'): void {
    const chart = this._chart;
    const series = this._series;
    if (!chart || !series) return;
    const scene = this._scene;
    const report = this._report;
    if (!scene.drawings.length && !scene.regimes.length && !scene.seriesLabels.length && !report?.layers.length) {
      if (layer === 'over') {
        this._hits = [];
        this._reporter?.(null);
      }
      return;
    }
    const xm = makeXMapper(chart, this._anchor);
    if (!xm) return;
    target.useMediaCoordinateSpace(({ context: ctx, mediaSize }) => {
      const painter = new Painter(ctx, mediaSize.width, mediaSize.height, xm, series, this._theme, this._hovered, this._paint, this._text);
      if (layer === 'under') {
        paintRegimes(ctx, xm, scene.regimes, painter.w, painter.h, true);
        for (const d of scene.drawings) drawingKind(d.kind)?.paintUnder?.(painter, d);
        return;
      }
      for (const d of scene.drawings) drawingKind(d.kind)?.paintOver(painter, d);
      regimeLabels(painter, scene.regimes);
      for (const s of scene.seriesLabels) {
        const pt = painter.P(s.at);
        if (!pt || pt.x < 0 || pt.x > painter.w) continue;
        painter.label({ el: s, text: s.text, style: 'tag', color: withAlpha(s.color, 1), x: pt.x + 6, y: pt.y, align: 'left', priority: 20, movable: true });
      }
      const view = visibleIndexRange(chart, xm);
      const obstacles = view ? frameObstacles(painter, this._paint, view, chart.timeScale().options().barSpacing) : null;
      const dropped = painter.flushLabels(obstacles ? labelObstacles(obstacles) : NO_OBSTACLES);
      this._hits = painter.hits;
      if (this._reporter && report?.layers.length && view && obstacles) {
        this._reporter(
          frameStats({ report, painter, scene, paint: this._paint, view, obstacles, dropped, lastIndex: this._anchor?.lastIndex ?? 0 }),
        );
      }
    });
  }
}
