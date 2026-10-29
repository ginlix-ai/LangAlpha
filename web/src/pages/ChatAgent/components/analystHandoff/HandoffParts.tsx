import type { ReactNode } from 'react';
import { Folder } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { cn } from '@/lib/utils';
import { SteeredMark } from '../messageList/MessageProvenance';
import { ElapsedClock, RUN_DETAIL, RUN_TITLE, RunPart, RunStatus, StatusSpeech } from '../messageList/runCard';
import { HANDOFF_PHASES, phaseWords, type HandoffPhase, type HandoffView } from './handoffModel';

/**
 * The control that opens a run, wrapped around its task. Its `::after`
 * stretches over the nearest positioned box (the slip, a group's row), so the
 * whole row is the way in while a second action there can still sit above it
 * as a button of its own; a button wrapping that action could not. The ring
 * is drawn on the row's rounded box, inside, since a group's rows sit in a
 * fold that clips.
 */
const STRETCHED_BUTTON =
  'flex max-w-full min-w-0 cursor-pointer text-left outline-hidden after:absolute after:inset-0 after:rounded-lg after:content-[""] '
  + 'focus-visible:after:ring-2 focus-visible:after:ring-inset focus-visible:after:ring-(--color-focus-ring)';

/** A small action on the right of a row: words alone, in the row's quieter
 *  ink until the pointer is on them. */
const ACTION =
  'inline-flex items-center text-xs font-medium leading-[1.25] text-(--color-text-secondary) transition-colors '
  + 'hover:text-(--color-text-primary)';

/** A workspace named in words, behind the folder glyph the sidebar gives it. */
export function WorkspaceName({ name, className }: { name: string; className?: string }) {
  return (
    <span className={cn('inline-flex min-w-0 items-center gap-[5px]', className)}>
      <Folder aria-hidden className="h-3 w-3 shrink-0" />
      <span className="truncate">{name}</span>
    </span>
  );
}

/**
 * The line under a hand-off's task: where it stands, then who works on it and
 * where. Who is left out when narrow, so the workspace keeps its room, and in
 * a group, whose header says each of its rows is an analyst's.
 */
export function HandoffMetaLine({
  status,
  workspaceName,
  steered = false,
  namesAnalyst = true,
  id,
  className,
}: {
  /** Where the run stands, which leads the line. */
  status: ReactNode;
  workspaceName?: string;
  /** Sent into a run already going, which the line says last. */
  steered?: boolean;
  namesAnalyst?: boolean;
  id?: string;
  className?: string;
}) {
  const { t } = useTranslation();
  return (
    <span id={id} className={cn(RUN_DETAIL, className)}>
      {status}
      {namesAnalyst && <RunPart className="@max-md:hidden"><span className="shrink-0">{t('agents.analyst')}</span></RunPart>}
      {workspaceName && <RunPart><WorkspaceName name={workspaceName} /></RunPart>}
      {steered && <RunPart><SteeredMark className="gap-[5px]" /></RunPart>}
    </span>
  );
}

interface HandoffStatusProps {
  phase: HandoffPhase;
  startedAt?: number;
  finishedAt?: number;
}

/**
 * A hand-off's status as it is seen, leading the line under its task: glyph,
 * words, and the clock while it works, in the quiet ink a subagent row's
 * status takes. Without `glyph`, a group's column draws it. Narrow, a settled
 * run loses how long it took, so the workspace keeps its room; the plain word
 * always stays. Decorative to a screen reader, which hears
 * `HandoffAnnouncement`.
 */
export function HandoffStatus({ phase, startedAt, finishedAt, glyph = true }: HandoffStatusProps & { glyph?: boolean }) {
  const { t } = useTranslation();
  const look = HANDOFF_PHASES[phase];
  const plain = t(look.labelKey);
  const words = phaseWords(phase, startedAt, finishedAt, t);
  return (
    <RunStatus
      aria-hidden
      glyph={glyph ? look.glyph : null}
      danger={look.danger}
      words={words === plain ? plain : (
        <>
          <span className="@max-md:hidden">{words}</span>
          <span className="@md:hidden">{plain}</span>
        </>
      )}
    >
      {look.clock && <ElapsedClock startedAt={startedAt} className="ml-0.5" />}
    </RunStatus>
  );
}

/** What `HandoffStatus` shows, to a screen reader. A group's row is not a
 *  live region of its own: the group's tally says what moved. */
export function HandoffAnnouncement({
  id,
  phase,
  startedAt,
  finishedAt,
  read,
  live = true,
}: { id?: string; read: boolean; live?: boolean } & HandoffStatusProps) {
  const { t } = useTranslation();
  return (
    <StatusSpeech
      id={id}
      words={phaseWords(phase, startedAt, finishedAt, t)}
      label={t(HANDOFF_PHASES[phase].labelKey)}
      read={read}
      live={live}
    />
  );
}

/** A run's first line: its task, which is the button into the analyst's
 *  thread when there is one. */
export function HandoffTitleLine({
  view,
  taskId,
  describedBy,
}: {
  view: HandoffView;
  /** The task, which describes the report link. */
  taskId: string;
  /** What the opening button is described by, beyond its task. */
  describedBy: string;
}) {
  const task = (
    <span id={taskId} className={cn(RUN_TITLE, 'block')}>
      {view.task}
    </span>
  );
  if (!view.onOpen) return task;
  return (
    <button type="button" onClick={view.onOpen} title={view.task} aria-describedby={describedBy} className={STRETCHED_BUTTON}>
      {task}
    </button>
  );
}

/**
 * The right of a slip or a group's row: what there is to do about the run
 * beyond opening it. A report that has arrived is a link down to it; a run
 * waiting on the reader shows what pressing the row does; otherwise nothing,
 * since the row's fill already says it opens, and a chevron here would read
 * as the reply's other chevrons do, as unfolding in place.
 */
export function HandoffAction({
  view,
  taskId,
}: {
  view: HandoffView;
  /** The task, which describes the report link. */
  taskId: string;
}) {
  const { t } = useTranslation();
  const { phase, onOpen, onSeeReport } = view;
  if (onSeeReport) {
    return (
      // Above the row's stretched button, so it takes its own clicks.
      <button
        type="button"
        aria-describedby={taskId}
        onClick={onSeeReport}
        className={cn(ACTION, 'relative z-10 shrink-0 cursor-pointer whitespace-nowrap rounded-sm underline-offset-2 hover:underline')}
      >
        {t('chat.ptcCard.seeReport')}
      </button>
    );
  }
  if (!onOpen || HANDOFF_PHASES[phase].action !== 'answer') return null;
  // A hint of what pressing the row does; the row is the button.
  return (
    <span
      aria-hidden
      className="inline-flex h-6 shrink-0 items-center whitespace-nowrap rounded-full border border-(--color-border-default) bg-(--color-bg-card) px-[11px] text-xs font-medium text-(--color-text-primary)"
    >
      {t('chat.ptcCard.answer')}
    </span>
  );
}

/** The report row's way into the analyst's thread: an action, as above. */
export const OPEN_ACTION =
  'inline-flex shrink-0 cursor-pointer items-center gap-[3px] rounded-sm text-xs font-medium leading-[1.25] text-(--color-text-primary) underline-offset-2 hover:underline';
