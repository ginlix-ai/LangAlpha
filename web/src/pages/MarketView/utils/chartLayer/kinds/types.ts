/**
 * One record per drawing kind: everything the renderer, the hover card, the
 * framing rule and the render report know about it. A new kind is a module
 * here and a line in `DRAWING_KINDS` (`./index`).
 */

import type { LayerDrawing, LayerPoint, LayerTime } from '../../../types/chartLayer';
import type { Painter } from '../painter';
import type { DrawingExtent, SceneBase } from '../scene';
import type { IP, TimeGrid } from '../timeGrid';

export interface ResolveContext {
  grid: TimeGrid;
  /** The element's identity, color, label and whether it explains itself. */
  base: SceneBase;
  /** A layer point in grid space, or null when it cannot be placed. */
  P(pt: LayerPoint | null | undefined): IP | null;
}

// Method syntax throughout: a kind's record is read through the registry's
// union type (`AnyDrawingKind`), which method parameters allow.
export interface DrawingKind<D extends LayerDrawing, S extends SceneBase> {
  /** i18n key of the kind's name, for the explain card's kicker. */
  nameKey: string;
  /** Every time the drawing reaches: the grid's future room and the framing rule cover them all. */
  times(d: D): (LayerTime | null | undefined)[];
  /** The times the framing rule shows, when not all of `times` (a replay leaves out its source). */
  frameTimes?(d: D): (LayerTime | null | undefined)[];
  /** Where its end is a count of bars past one of its times rather than a time (a position with no `until`), so framing can reach it. */
  barsAfter?(d: D): { time: LayerTime; bars: number } | null;
  /** The (time, price) points that pin it for the agent; a NaN price where it has none. */
  anchors(d: D): LayerPoint[];
  /** Explainable without a note or a label: its readout is the explanation. */
  alwaysExplained?: boolean;
  /** The drawing in grid space, or null when its anchors cannot be placed. */
  resolve(d: D, ctx: ResolveContext): S | null;
  /** The grid-space box it occupies, for the render report's offscreen list. */
  extent(s: S): DrawingExtent | null;
  /** Fills, under the candles. */
  paintUnder?(p: Painter, s: S): void;
  /** Strokes, labels and hit shapes, over the candles. */
  paintOver(p: Painter, s: S): void;
}
