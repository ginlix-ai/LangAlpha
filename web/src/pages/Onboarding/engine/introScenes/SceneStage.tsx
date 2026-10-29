import { cn } from '@/lib/utils';
import { SceneClock } from './clock';
import type { IntroScene } from './types';

/**
 * One scene playing on its clock, at its 400px design size and scaled by
 * `.intro-scene` to sit inside the visual panel beside its anchor. The scale
 * lives on this static wrapper because the scene's entrances animate
 * `transform` on their own elements.
 */
export function SceneStage({ scene: { Scene, length, anchor } }: { scene: IntroScene }) {
  return (
    <div
      className={cn(
        'intro-scene w-[400px] shrink-0',
        anchor === 'left' && 'ml-8',
        anchor === 'right' && 'ml-auto mr-8',
        anchor === 'center' && 'mx-auto'
      )}
      style={{ transformOrigin: anchor === 'center' ? 'center center' : `${anchor} center` }}
    >
      <SceneClock length={length}>
        <Scene />
      </SceneClock>
    </div>
  );
}
