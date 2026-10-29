/**
 * The machine a form is about to make, drawn in a framed ground at its top:
 * the New computer form's pick, and the spec a Change spec moves to. What it
 * draws comes from `hostedPreview` or `ownPreview` (machine.ts), so one frame
 * serves every pick and a new pick crossfades in, a hosted one to one the
 * user brings included.
 */
import { SwapCell } from '@/components/ui/crossfade';

import { ArtFrame } from './ArtFrame';
import { MachineArt, MachineGhost } from './MachineArt';
import type { PreviewMachine } from './machine';

export function MachinePreview({ machine }: { machine: PreviewMachine }) {
  const { art, name, detail, brand, tier } = machine;
  return (
    <ArtFrame className="is-preview" tl={name} tr={detail} data-testid="new-computer-preview">
      <SwapCell
        swapKey={art ? `${art}:${tier ?? ''}:${brand}` : 'pending'}
        as="div"
        className="h-full w-full [grid-template-rows:minmax(0,1fr)]"
        layerClassName="flex h-full min-h-0 items-end justify-center"
      >
        {art ? (
          <MachineArt art={art} state="off" brand={brand} tier={tier} label={name} />
        ) : (
          // The corner says what this is; the outlines only show it.
          <MachineGhost />
        )}
      </SwapCell>
    </ArtFrame>
  );
}
