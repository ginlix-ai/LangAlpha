/**
 * Pane primitive that paints a small caption in a pane's top-left corner, the
 * way TradingView names an indicator pane ("RSI (14): 62").
 *
 * It draws onto the pane's own canvas rather than floating an HTML element
 * over it, so it never needs repositioning when panes are resized or the
 * chart is laid out. It paints in the normal layer, since
 * ``chart.takeScreenshot()`` copies only that one, and the caption belongs in
 * the image handed to the agent.
 *
 * Usage:
 *   const label = new PaneLabelPrimitive('RSI (14)', color);
 *   chart.panes()[1].attachPrimitive(label);
 *   label.setText('RSI (14): 62');
 */

import type {
  IPanePrimitive,
  IPanePrimitivePaneView,
  IPrimitivePaneRenderer,
  PaneAttachedParameter,
  PrimitivePaneViewZOrder,
  Time,
} from 'lightweight-charts';
import type { CanvasRenderingTarget2D } from 'fancy-canvas';

const PADDING_X = 8;
const PADDING_Y = 6;

export class PaneLabelPrimitive implements IPanePrimitive<Time> {
  private _text: string;
  private _color: string;
  private _font: string;
  private _requestUpdate: (() => void) | null = null;

  constructor(text: string, color: string, font = '12px sans-serif') {
    this._text = text;
    this._color = color;
    this._font = font;
  }

  attached({ requestUpdate }: PaneAttachedParameter<Time>): void {
    this._requestUpdate = requestUpdate;
  }

  detached(): void {
    this._requestUpdate = null;
  }

  setText(text: string): void {
    if (text === this._text) return;
    this._text = text;
    this._requestUpdate?.();
  }

  setColor(color: string): void {
    if (color === this._color) return;
    this._color = color;
    this._requestUpdate?.();
  }

  updateAllViews(): void {}

  paneViews(): IPanePrimitivePaneView[] {
    const source = this;
    return [{
      zOrder(): PrimitivePaneViewZOrder { return 'normal'; },
      renderer(): IPrimitivePaneRenderer {
        return {
          draw(target: CanvasRenderingTarget2D): void {
            if (!source._text) return;
            target.useMediaCoordinateSpace(({ context: ctx }) => {
              ctx.save();
              ctx.font = source._font;
              ctx.fillStyle = source._color;
              ctx.textBaseline = 'top';
              ctx.textAlign = 'left';
              ctx.fillText(source._text, PADDING_X, PADDING_Y);
              ctx.restore();
            });
          },
        };
      },
    }];
  }
}
