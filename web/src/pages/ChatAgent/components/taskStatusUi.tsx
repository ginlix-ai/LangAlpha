/**
 * The one place a task's status becomes an icon and a color.
 *
 * Three vocabularies live here because the surfaces genuinely speak three.
 * `STATUS_UI` serves the task-card chip: a workflow run's card and its
 * detail header, and a task's tool-call detail. `SUBAGENT_STATUS_UI` serves
 * the nav tree and the status bar, which tell "spawned but silent" from
 * "running". `SUBAGENT_ROW_UI` serves the reply's subagent rows, which need
 * both of those plus a launch that never ran and the verbs of a follow-up.
 * Each vocabulary has exactly one table; a surface that needs its own
 * treatment declares an override rather than restating the ladder.
 *
 * Liveness is the shared ascii Loader (`live: true`), never a spinning lucide
 * glyph, the same treatment as the running-thread rows in the nav tree.
 */
import React from 'react';
import { useTranslation } from 'react-i18next';
import {
  AlertCircle, Check, CheckCircle2, Circle, RefreshCw, RotateCw, StopCircle,
  type LucideIcon,
} from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import type { SubagentDisplayStatus } from '../session/subagents/subagentStatus';
// Types only: the nav tree loads this module on first paint, and the row kit
// pulls in framer, so the row status is drawn with the row (`SubagentRow`).
import type { RunGlyphKind } from './messageList/runCard';

export type TaskCardStatusKind = 'running' | 'completed' | 'cancelled' | 'error' | 'unknown';

interface TaskCardStatusUi {
  /** i18n key for the status word; `null` renders the raw wire status instead. */
  labelKey: string | null;
  color: string;
  Icon: LucideIcon | null;
  /** Renders the ascii liveness glyph instead of a lucide icon. */
  live?: boolean;
}

/**
 * Per-status presentation for every task card, so a status can never render
 * amber on one card and red on another. Cancelled is terminal-neutral (the
 * run was stopped); Failed is the danger token, not `--color-loss`, which
 * belongs to market P&L.
 */
export const STATUS_UI: Record<TaskCardStatusKind, TaskCardStatusUi> = {
  running: {
    labelKey: 'chat.taskCard.statusRunning',
    color: 'var(--color-warning)',
    Icon: null,
    live: true,
  },
  completed: {
    labelKey: 'chat.taskCard.statusCompleted',
    color: 'var(--color-success)',
    Icon: Check,
  },
  cancelled: {
    labelKey: 'chat.taskCard.statusStopped',
    color: 'var(--color-text-tertiary)',
    Icon: StopCircle,
  },
  error: {
    labelKey: 'chat.taskCard.statusFailed',
    color: 'var(--color-icon-danger)',
    Icon: AlertCircle,
  },
  unknown: {
    labelKey: null,
    color: 'var(--color-text-tertiary)',
    Icon: null,
  },
};

/**
 * The card status a task's own wire status maps onto. Anything the vocabulary
 * doesn't name falls to `unknown`, which renders the wire value verbatim
 * rather than guessing an outcome.
 */
export function taskCardStatusKind(status: string | null | undefined): TaskCardStatusKind {
  switch (status) {
    case 'running':
    case 'completed':
    case 'cancelled':
    case 'error':
      return status;
    default:
      return 'unknown';
  }
}

/**
 * The status word every task surface shows: icon, accent and label from one
 * `STATUS_UI` row. The inline card header and the workflow-run detail header
 * render this, so the chip cannot drift between them.
 */
export function TaskStatusChip({
  kind,
  rawStatus,
  style,
}: {
  kind: TaskCardStatusKind;
  /** Wire status rendered verbatim when `kind` is `unknown`. */
  rawStatus?: string;
  style?: React.CSSProperties;
}): React.ReactElement {
  const { t } = useTranslation();
  const { labelKey, color, Icon, live } = STATUS_UI[kind];
  const label = labelKey ? t(labelKey) : rawStatus;
  return (
    <span
      style={{
        display: 'inline-flex',
        alignItems: 'center',
        gap: 5,
        color,
        fontSize: '0.6875rem',
        letterSpacing: '0.04em',
        fontWeight: 500,
        whiteSpace: 'nowrap',
        ...style,
      }}
    >
      {live ? (
        <Loader size={11} label={label || t('chat.taskCard.statusRunning')} style={{ color: 'inherit' }} />
      ) : (
        Icon && <Icon style={{ width: 11, height: 11 }} />
      )}
      {label}
    </span>
  );
}

interface SubagentStatusTreatment {
  Icon?: LucideIcon;
  color: string;
  /** Renders the ascii liveness glyph instead of a lucide icon. */
  live?: boolean;
}

/**
 * A subagent's status badge: exceptional terminal outcomes read at a glance
 * (error = red alert, stopped = stop glyph), a genuinely running task shows
 * the ascii liveness glyph (amber = live agent work, matching the nav tree's
 * running-thread rows), and a spawned-but-silent one holds an idle circle.
 * Completed renders NO glyph on the nav row: absence of the liveness glyph
 * already reads as done, and a checkmark next to the hover ✕ remove button
 * read as a second control.
 */
const SUBAGENT_STATUS_UI: Record<SubagentDisplayStatus, SubagentStatusTreatment> = {
  initializing: { Icon: Circle, color: 'var(--color-icon-muted)' },
  active: { live: true, color: 'var(--color-accent-primary)' },
  completed: { color: 'var(--color-text-tertiary)' },
  cancelled: { Icon: StopCircle, color: 'var(--color-text-tertiary)' },
  error: { Icon: AlertCircle, color: 'var(--color-icon-danger)' },
};

export type SubagentStatusSurface = 'navRow' | 'statusBar';

/** Loader glyph px per surface — the lucide icons size via `className`, but
 *  the ascii glyph sizes by font, so the surface names its density here. */
const LOADER_SIZE: Record<SubagentStatusSurface, number> = {
  navRow: 12,
  statusBar: 16,
};

/**
 * The status bar is the one surface that celebrates a finished task — it has
 * the room the nav row doesn't. Whether the two should converge is a design
 * call, not a structural one; until it's made, the difference is declared in
 * one place instead of implied by two ladders.
 */
const SUBAGENT_STATUS_OVERRIDES: Partial<
  Record<SubagentStatusSurface, Partial<Record<SubagentDisplayStatus, SubagentStatusTreatment>>>
> = {
  statusBar: {
    completed: { Icon: CheckCircle2, color: 'var(--color-accent-primary)' },
  },
};

export function SubagentStatusIcon({
  status,
  surface = 'navRow',
  className,
}: {
  status: SubagentDisplayStatus;
  surface?: SubagentStatusSurface;
  /** Sizing class for the lucide icons — the nav row is 3, the status bar 4. */
  className?: string;
}): React.ReactElement | null {
  const { t } = useTranslation();
  const { Icon, color, live } =
    SUBAGENT_STATUS_OVERRIDES[surface]?.[status] ?? SUBAGENT_STATUS_UI[status];
  if (live) {
    return (
      <Loader
        size={LOADER_SIZE[surface]}
        label={t('chat.taskCard.statusRunning')}
        style={{ color }}
      />
    );
  }
  // A status with neither liveness nor an icon is deliberately glyph-free.
  if (!Icon) return null;
  return <Icon className={className} style={{ color }} />;
}

/**
 * A subagent as a row in the reply. `starting` is a launch whose run has not
 * shown a sign of life yet; `refused` is a launch that never ran at all, so it
 * has no time and, unless it was a follow-up aimed at a task that exists,
 * nothing to open. `updated` and `resumed` are follow-ups sent to a task: the
 * verb is the news, so an update's own wire status is not read (its record
 * keeps the `running` it was born with and nothing settles it). A resume's is
 * read only for a run that failed or was stopped.
 */
export type SubagentRowKind =
  | 'starting'
  | 'working'
  | 'pausing'
  | 'done'
  | 'failed'
  | 'stopped'
  | 'refused'
  | 'updated'
  | 'resumed'
  | 'unknown';

interface SubagentRowUi {
  /** The status word, for screen readers and for a row with nothing better to
   *  say; `null` leaves the raw wire status. */
  labelKey: string | null;
  /** Absent means no glyph, which on these rows is the done state. */
  glyph?: RunGlyphKind;
  /** Still working: keeps the clock running and a group open. */
  live: boolean;
  /** What went wrong: the word takes the danger color. */
  danger: boolean;
}

export const SUBAGENT_ROW_UI: Record<SubagentRowKind, SubagentRowUi> = {
  starting: { labelKey: 'chat.subagentRun.starting', glyph: 'pending', live: true, danger: false },
  working: { labelKey: 'chat.taskCard.statusRunning', glyph: 'live', live: true, danger: false },
  // A running task under an unanswered credit pause: still live, but its own
  // gate stops it at the next model boundary, so it promises a stop rather
  // than more work.
  pausing: { labelKey: 'chat.taskCard.statusPausing', glyph: 'live', live: true, danger: false },
  // "Finished", as the group's header says it once all are.
  done: { labelKey: 'chat.subagentRun.finished', live: false, danger: false },
  failed: { labelKey: 'chat.taskCard.statusFailed', glyph: 'alert', live: false, danger: true },
  stopped: { labelKey: 'chat.taskCard.statusStopped', glyph: 'stop', live: false, danger: false },
  refused: { labelKey: 'chat.subagentRun.couldntStart', glyph: 'alert', live: false, danger: true },
  updated: { labelKey: 'chat.taskCard.statusUpdated', glyph: RefreshCw, live: false, danger: false },
  resumed: { labelKey: 'chat.taskCard.statusResumed', glyph: RotateCw, live: false, danger: false },
  unknown: { labelKey: null, live: false, danger: false },
};
