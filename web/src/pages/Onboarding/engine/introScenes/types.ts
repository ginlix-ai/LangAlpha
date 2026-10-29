import type { ComponentType } from 'react';

/** A scene for the intro modal's visual panel, and how to stage it. */
export interface IntroScene {
  Scene: ComponentType;
  /** Script length in ms; the clock holds the last frame, then plays it again. */
  length: number;
  /**
   * The side of the visual panel the stage keeps to (`SceneStage`): `left`
   * and `right` sit it a margin in from that edge and scale it from there,
   * and `center` centers it. A panel narrower than the scaled stage crops the
   * far side, so each scene anchors the side its hot region lives on.
   */
  anchor: 'left' | 'right' | 'center';
}
