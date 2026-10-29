/**
 * A report-back, drawn in place of its hidden message as a line of its own:
 * an analyst's on the side every sender from outside the agent takes, a
 * subagent's on the agent's own, since its work is the agent's.
 */
import React, { useId } from 'react';
import { useTranslation } from 'react-i18next';
import { ArrowUpRight, CircleCheck, CircleStop, CircleX, Inbox } from 'lucide-react';
import { cn } from '@/lib/utils';
import type { MessageProvenance } from '@/types/chat';
import { mapReportOutcome } from '../../hooks/usePTCDispatchStatus';
import { useOpenDispatchedThread, type DispatcherContext } from '../../hooks/useOpenDispatchedThread';
import { OPEN_ACTION } from '../analystHandoff/HandoffParts';
import { HANDOFF_PHASES, handoffPhase } from '../analystHandoff/handoffModel';
import { commandName, commandOutcome, OUTCOME_KEYS } from '../commands/commandWords';
import { SubagentGlyph } from '../mcp/subagentGlyph';
import { useMessageActions } from './MessageActionsContext';
import { RunAvatar, RunGlyph, type RunGlyphKind } from './runCard';
import { summarize } from './subagentRun/text';

type AnalystReport = Extract<MessageProvenance, { kind: 'analyst_report' }>;
type TaskReport = Extract<MessageProvenance, { kind: 'task_report' }>;
type CommandReport = Extract<MessageProvenance, { kind: 'command_report' }>;
export type ReportProvenance = AnalystReport | TaskReport | CommandReport;

// The ring is for a keyboard reader a hand-off's report link brought here,
// drawn on the line rather than the full-width row that takes the focus.
const REPORT_LINE = cn(
  'flex h-[26px] w-fit max-w-full min-w-0 items-center gap-1.5 rounded-md text-[0.78125rem] text-(--color-text-secondary)',
  'group-focus-visible/report:ring-2 group-focus-visible/report:ring-ring',
);

/**
 * One report-back row, the element the list keys and a hand-off's report
 * link lands on. Reports with nothing between them came in together, as the
 * inputs of one turn, so `together` closes the gap above.
 */
export function ReportRow({
  id,
  provenance,
  together,
  readOnly,
  dispatcherContext,
}: {
  id: string;
  provenance: ReportProvenance;
  together?: boolean;
  readOnly?: boolean;
  dispatcherContext?: DispatcherContext | null;
}): React.ReactElement {
  const analyst = provenance.kind === 'analyst_report';
  return (
    <div
      data-message-id={id}
      data-message-role="user"
      tabIndex={-1}
      className={cn('group/report outline-hidden', analyst && 'flex justify-end')}
      // Inline, to win over the list's sibling spacing.
      style={together ? { marginTop: 0 } : undefined}
    >
      {provenance.kind === 'analyst_report'
        ? <AnalystReportRow provenance={provenance} readOnly={readOnly} dispatcherContext={dispatcherContext} />
        : provenance.kind === 'command_report'
          ? <CommandReportRow provenance={provenance} readOnly={readOnly} />
          : <TaskReportRow provenance={provenance} readOnly={readOnly} />}
    </div>
  );
}

/**
 * A report's line: what reported, its name, how its run ended, and the way in.
 * How long the run took is on the run's row, which the line most often sits
 * right under. The name truncates and the outcome never does, since the
 * outcome is the news.
 */
function ReportLineParts({
  lead,
  name,
  glyph,
  outcomeKey,
  onOpen,
}: {
  lead: React.ReactNode;
  name: string;
  glyph: RunGlyphKind | null;
  outcomeKey: string;
  onOpen: (() => void) | undefined;
}): React.ReactElement {
  const { t } = useTranslation();
  const labelId = useId();
  const outcome = t(outcomeKey);
  return (
    <div className={REPORT_LINE}>
      {lead}
      {/* The plain words as a title, for when a long name truncates. */}
      <span id={labelId} title={`${name} ${outcome}`} className="flex min-w-0 items-center gap-1.5">
        <span className="min-w-0 truncate font-medium text-(--color-text-primary)">{name}</span>
        {/* Laid out by the gap; the space is for the words read whole. */}
        {' '}
        {glyph && <RunGlyph kind={glyph} />}
        <span className="shrink-0 whitespace-nowrap">{outcome}</span>
      </span>
      {onOpen && (
        <button type="button" aria-describedby={labelId} onClick={onOpen} className={cn(OPEN_ACTION, 'ml-2')}>
          {t('chat.ptcCard.open')}
          <ArrowUpRight aria-hidden className="h-3 w-3" />
        </button>
      )}
    </div>
  );
}

/**
 * How an analyst's run ended, in the words and glyph its hand-off's phase
 * reports with. A report from before reports named their outcome reads as
 * reported back.
 */
function analystOutcome(outcome: AnalystReport['outcome']) {
  const phase = handoffPhase({ runStatus: mapReportOutcome(outcome ?? 'completed'), dispatchFailed: false });
  const { glyph, reportKey } = HANDOFF_PHASES[phase];
  // Every phase an outcome maps to is one a report can name.
  return { glyph, key: reportKey! };
}

/**
 * An analyst's report-back in the Chief of Staff's transcript: the inbox a
 * report arrives in, the analyst, how its run ended, and the way into its
 * thread.
 */
function AnalystReportRow({
  provenance,
  readOnly,
  dispatcherContext,
}: {
  provenance: AnalystReport;
  readOnly?: boolean;
  dispatcherContext?: DispatcherContext | null;
}): React.ReactElement {
  const { t } = useTranslation();
  const openDispatchedThread = useOpenDispatchedThread(dispatcherContext);
  const { threadId, workspaceId, workspaceName, outcome } = provenance;
  const { glyph, key } = analystOutcome(outcome);
  return (
    <ReportLineParts
      lead={<Inbox aria-hidden className="h-3 w-3 shrink-0" />}
      name={workspaceName ? t('chat.provenance.analyst', { name: workspaceName }) : t('chat.provenance.analystUnnamed')}
      glyph={glyph}
      outcomeKey={key}
      onOpen={threadId && !readOnly ? () => openDispatchedThread(threadId, workspaceId) : undefined}
    />
  );
}

/**
 * A background subagent's report-back in its main agent's transcript: its
 * role on a small copy of the avatar its row wears, the title the row shows,
 * how the run ended, and the way into the subagent. A report with no title
 * names the task by its id.
 */
function TaskReportRow({ provenance, readOnly }: { provenance: TaskReport; readOnly?: boolean }): React.ReactElement {
  const { onOpenSubagentTask } = useMessageActions();
  const { taskId, displayId, description, failed, subagentType } = provenance;
  return (
    <ReportLineParts
      lead={(
        <RunAvatar className="h-[18px] w-[18px]">
          <SubagentGlyph type={subagentType} className="h-[11px] w-[11px] stroke-[1.75]" />
        </RunAvatar>
      )}
      name={summarize(description ?? undefined) || displayId}
      glyph={failed ? 'alert' : null}
      outcomeKey={failed ? 'chat.provenance.taskFailed' : 'chat.provenance.taskFinished'}
      onOpen={taskId && onOpenSubagentTask && !readOnly
        ? () => onOpenSubagentTask({ subagentId: `task:${taskId}`, description: description ?? undefined })
        : undefined}
    />
  );
}

// The stop is the one the command's own row draws for a stopped command.
const COMMAND_LEADS = {
  completed: { Lead: CircleCheck, tone: 'text-(--color-success)' },
  failed: { Lead: CircleX, tone: 'text-(--color-icon-danger)' },
  stopped: { Lead: CircleStop, tone: 'text-(--color-text-tertiary)' },
} as const satisfies Record<CommandReport['status'], { Lead: React.ComponentType<{ className?: string }>; tone: string }>;

/**
 * A background command's report in its agent's transcript: how it ended, by
 * the description the agent gave it, in the words a task's report uses (one
 * stopped an hour after its turn reads as stopped early, not failed), and
 * the way back to the tool card it started from. A command that named no
 * call has no way in, as a share's row has none.
 */
function CommandReportRow({ provenance, readOnly }: { provenance: CommandReport; readOnly?: boolean }): React.ReactElement {
  const { t } = useTranslation();
  const { onToolCallDetailClick } = useMessageActions();
  const { tool, description, status, failureReason, toolCallId } = provenance;
  const outcome = commandOutcome(status, failureReason);
  const { Lead, tone } = COMMAND_LEADS[status];
  return (
    <ReportLineParts
      lead={<Lead aria-hidden className={cn('h-3.5 w-3.5 shrink-0', tone)} />}
      name={commandName(description, tool, t)}
      glyph={null}
      outcomeKey={OUTCOME_KEYS[outcome].report}
      onOpen={toolCallId && onToolCallDetailClick && !readOnly ? () => onToolCallDetailClick(toolCallId) : undefined}
    />
  );
}
