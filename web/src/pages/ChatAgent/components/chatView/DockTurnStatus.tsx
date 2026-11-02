import type React from 'react';
import { useState } from 'react';
import { Disclosure } from '@/components/ui/Disclosure';
import { Loader } from '@/components/ui/loader';
import { newestTurnStart } from '../messageList/turnProjection';
import { WorkingLabel } from '../messageList/TurnFold';
import { StatusLine } from './StatusLine';
import type { MessageRecord } from './types';

/**
 * The running turn, at a file's foot. Docked, the composer is all of the chat
 * on screen, and a turn shows only as its Stop button while its replies land
 * in a transcript nobody can see. So the line the turn's row draws in the
 * transcript (Working for 12s) sits above the composer, and folds away when
 * the turn ends.
 */
export function DockTurnStatus({ running, messages }: {
  running: boolean;
  messages: readonly MessageRecord[];
}): React.ReactElement {
  // Timed as the transcript's row times it; a turn with nothing in view to
  // time it by is timed from here.
  const [seen] = useState(() => Date.now());
  const startedAt = newestTurnStart(messages) ?? seen;
  return (
    <Disclosure open={running} cancelGap>
      <StatusLine live={false} glyph={<Loader size={12} className="text-(--color-accent-primary)" />}>
        <WorkingLabel startedAt={startedAt} />
      </StatusLine>
    </Disclosure>
  );
}
