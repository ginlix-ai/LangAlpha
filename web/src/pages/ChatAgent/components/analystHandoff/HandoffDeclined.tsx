import { useId, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { X } from 'lucide-react';
import { cn } from '@/lib/utils';
import Markdown from '../Markdown';
import { OverflowCollapse } from '../messageList/OverflowCollapse';
import {
  RUN_FOCUS,
  RUN_HOVER,
  RUN_ROW,
  RUN_TITLE,
  RunDisclosure,
  RunFoldChevron,
  RunStatus,
} from '../messageList/runCard';
import { AnalystMark } from './AnalystMark';
import { plainBrief, type ProposalData } from './handoffModel';
import { HandoffMetaLine } from './HandoffParts';

// How much of the brief the opened row shows before Show all, in px.
const BRIEF_MAX_PX = 132;

/**
 * A hand-off the user declined: a row like the slip it would have become, in
 * quieter ink, opening in place to the brief it held. Nothing ran, so there
 * is no thread to open; the row itself is the toggle.
 */
export function HandoffDeclined({ proposal }: { proposal: ProposalData }) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  const statusId = useId();
  const taskId = useId();

  return (
    <div data-handoff-declined className="@container">
      <RunDisclosure
        open={open}
        onToggle={() => setOpen((v) => !v)}
        labelledBy={`${statusId} ${taskId}`}
        className={cn(RUN_ROW, RUN_HOVER, RUN_FOCUS)}
        head={
          <>
            <AnalystMark quiet />
            <span className="min-w-0">
              <span id={taskId} className={cn(RUN_TITLE, 'block text-(--color-text-secondary)')}>
                {plainBrief(proposal.question)}
              </span>
              <HandoffMetaLine
                status={<RunStatus id={statusId} glyph={X} words={t('chat.ptcCard.declined')} />}
                workspaceName={proposal.workspace_name?.trim()}
                className="text-(--color-text-tertiary)"
              />
            </span>
            {/* The toggle's state is aria-expanded; these words only show it.
                Narrow, the chevron alone says it. */}
            <span aria-hidden className="inline-flex shrink-0 items-center gap-1 whitespace-nowrap text-xs font-medium text-(--color-text-secondary)">
              <span className="@max-md:hidden">{open ? t('chat.ptcCard.briefLess') : t('chat.ptcCard.briefMore')}</span>
              <RunFoldChevron open={open} />
            </span>
          </>
        }
      >
        <div className="pt-1 pb-2 pl-13 text-(--color-text-secondary)">
          <OverflowCollapse enabled maxHeight={BRIEF_MAX_PX}>
            <Markdown variant="compact" content={proposal.question} className="text-[0.8125rem] leading-relaxed" />
          </OverflowCollapse>
        </div>
      </RunDisclosure>
    </div>
  );
}
