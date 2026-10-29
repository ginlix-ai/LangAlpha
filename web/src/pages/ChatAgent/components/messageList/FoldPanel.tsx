import React, { useContext } from 'react';
import { Disclosure } from '@/components/ui/Disclosure';
import { CollapseClockContext } from './transcriptMotion';

// A close is an ease, not the open's spring, and a tall process takes a longer
// beat than a short one: the exit beat front-loads its travel, and three
// screens of process crossing in its first 150ms read as a cut, with the
// answer arriving from nowhere. Bounded so nothing drags.
const tallClose = (height: number) => Math.min(0.7, 0.3 + height / 6000);

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
