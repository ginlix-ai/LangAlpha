/**
 * The drawing kinds, one record each (`./types`). Everything that walks a
 * layer's drawings (scene resolution, painting, the hover card's name, the
 * agent's anchors, the grid's future room, the framing rule, the render
 * report) reads them from here, so a kind is defined in one place.
 */

import type { LayerDrawing, LayerDrawingType } from '../../../types/chartLayer';
import { analog, type AnalogScene } from './analog';
import { arrow, type ArrowScene } from './arrow';
import { box, type BoxScene } from './box';
import { channel, type ChannelScene } from './channel';
import { cone, type ConeScene } from './cone';
import { fib, type FibScene } from './fib';
import { forecast, type ForecastScene } from './forecast';
import { label, type LabelScene } from './label';
import { measure, type MeasureScene } from './measure';
import { path, type PathScene } from './path';
import { pitchfork, type PitchforkScene } from './pitchfork';
import { position, type PositionScene } from './position';
import { profile, type ProfileScene } from './profile';
import { segment, type SegmentScene } from './segment';
import type { DrawingKind } from './types';
import { vline, type VLineScene } from './vline';

interface SceneByKind {
  segment: SegmentScene;
  vline: VLineScene;
  channel: ChannelScene;
  pitchfork: PitchforkScene;
  box: BoxScene;
  label: LabelScene;
  arrow: ArrowScene;
  path: PathScene;
  fib: FibScene;
  cone: ConeScene;
  forecast: ForecastScene;
  analog: AnalogScene;
  position: PositionScene;
  measure: MeasureScene;
  profile: ProfileScene;
}

/** A resolved drawing, in grid space. */
export type SceneDrawing = SceneByKind[LayerDrawingType];

export const DRAWING_KINDS: {
  [K in LayerDrawingType]: DrawingKind<Extract<LayerDrawing, { type: K }>, SceneByKind[K]>;
} = { segment, vline, channel, pitchfork, box, label, arrow, path, fib, cone, forecast, analog, position, measure, profile };

export type AnyDrawingKind = DrawingKind<LayerDrawing, SceneDrawing>;

/**
 * The record for a drawing type, or undefined for a type this build does not
 * know: a newer server may send one, and it is skipped rather than thrown on.
 */
export function drawingKind(type: string): AnyDrawingKind | undefined {
  return Object.hasOwn(DRAWING_KINDS, type) ? DRAWING_KINDS[type as LayerDrawingType] : undefined;
}

export const UNKNOWN_KIND_NAME = 'marketView.layerExplain.types.element';
