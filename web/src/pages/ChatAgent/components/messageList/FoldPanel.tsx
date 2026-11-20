import React, { useContext } from 'react';
import { Disclosure } from '@/components/ui/Disclosure';
import { CollapseClockContext, tallClose } from './transcriptMotion';

/**
 * A block of the transcript that folds shut and back open: the `Disclosure`
 * every fold is, set for the transcript.
 *
 * A long thread keeps dozens of folded turns, and the fold mounts a block's
 * children only while it is open or moving, so their process costs nothing.
 * The list spaces its blocks with a sibling margin, which the fold cancels as
 * it moves (`cancelGap`). A close takes a longer beat the taller the block,
 * and notes when it will end, since what a turn's ending brings waits for its
 * closes (see CollapseClock).
 */
export function FoldPanel({ open, children }: { open: boolean; children: React.ReactNode }): React.ReactElement {
  const clock = useContext(CollapseClockContext);
  return (
    <Disclosure
      open={open}
      cancelGap
      closeBeat={tallClose}
      onClosing={(seconds) => clock?.note(seconds)}
      data-fold-panel=""
    >
      {children}
    </Disclosure>
  );
}
