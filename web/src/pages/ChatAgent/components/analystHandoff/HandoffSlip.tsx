import { useId } from 'react';
import { cn } from '@/lib/utils';
import { RUN_FOCUS, RUN_HOVER, RUN_ROW, RunGlyph } from '../messageList/runCard';
import { AnalystMark } from './AnalystMark';
import { columnGlyph, type HandoffView } from './handoffModel';
import { HandoffAction, HandoffAnnouncement, HandoffMetaLine, HandoffStatus, HandoffTitleLine } from './HandoffParts';

/**
 * One hand-off once it is handed off, on the line a subagent's row uses: the
 * task, then how it stands and who works on it and where, and on the right
 * anything to do about it beyond opening it. The whole row opens the
 * analyst's thread; a report that has arrived is one more link, down to it.
 * `grouped`, it is a row of a group, whose header says each row is an
 * analyst's and whose tally speaks for them all. The mark on that header
 * stands for every row's avatar, so the avatar's place holds where the run
 * stands, centered under the mark, and the line under the task keeps only
 * the words.
 *
 * The row can hold focus, though Tab passes it by, for the card it replaced
 * to hand focus on to: a hand-off approved a moment ago has no thread to open
 * yet, so nothing in it is a control, and a keyboard user's place would
 * otherwise fall to the page.
 */
export function HandoffSlip({ view, grouped = false }: { view: HandoffView; grouped?: boolean }) {
  const taskId = useId();
  const metaId = useId();
  const statusId = useId();
  const { phase, onOpen } = view;

  return (
    <div
      data-handoff-phase={phase}
      data-handoff-member={view.id}
      tabIndex={-1}
      className={cn(RUN_ROW, RUN_FOCUS, '@container', onOpen && RUN_HOVER)}
    >
      {grouped ? (
        <span aria-hidden className="flex justify-center">
          <RunGlyph kind={columnGlyph(phase)} large />
        </span>
      ) : <AnalystMark />}
      <span className="min-w-0">
        <HandoffTitleLine view={view} taskId={taskId} describedBy={`${metaId} ${statusId}`} />
        <HandoffMetaLine
          id={metaId}
          status={<HandoffStatus phase={phase} startedAt={view.startedAt} finishedAt={view.finishedAt} glyph={!grouped} />}
          workspaceName={view.workspaceName}
          steered={view.steered}
          namesAnalyst={!grouped}
        />
      </span>
      <HandoffAction view={view} taskId={taskId} />
      <HandoffAnnouncement
        id={statusId}
        phase={phase}
        startedAt={view.startedAt}
        finishedAt={view.finishedAt}
        read={view.read}
        live={!grouped}
      />
    </div>
  );
}
