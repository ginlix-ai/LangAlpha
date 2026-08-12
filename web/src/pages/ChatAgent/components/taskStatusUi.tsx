/**
 * The one place a task's status becomes an icon and a color.
 *
 * Two vocabularies live here because the surfaces genuinely speak two: inline
 * cards read `TaskCardStatusKind` (which carries the update/resume verbs a
 * card header shows), while the nav tree and the status bar read a subagent's
 * `SubagentDisplayStatus` (which distinguishes "spawned but silent" from
 * "running"). Each vocabulary has exactly one table; a surface that needs its
 * own treatment declares an override rather than restating the ladder.
 *
 * Liveness is the shared ascii Loader (`live: true`), never a spinning lucide
 * glyph — the same treatment as the running-thread rows in the nav tree.
 */
import React from 'react';
import { useTranslation } from 'react-i18next';
import {
  AlertCircle, Check, CheckCircle2, Circle, RefreshCw, RotateCw, StopCircle,
  type LucideIcon,
} from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import type { SubagentDisplayStatus } from '../session/subagents/subagentStatus';

export type TaskCardStatusKind =
  | 'running'
  | 'completed'
  | 'cancelled'
  | 'error'
  | 'updated'
  | 'resumed'
  | 'unknown';

interface TaskCardStatusUi {
  /** i18n key for the status word; `null` renders the raw wire status instead. */
  labelKey: string | null;
  color: string;
  Icon: LucideIcon | null;
  /** Renders the ascii liveness glyph instead of a lucide icon. */
  live?: boolean;
}

/**
 * Per-status presentation for every inline task card. Subagent spawns and
 * workflow runs read this one table, so a status can never render amber on one
 * card and red on the other. Updated/Resumed share the warning amber with
 * Running because those are all "in flight or just changed"; Cancelled is
 * terminal-neutral (the run was stopped), Failed is the danger token — not
 * `--color-loss`, which belongs to market P&L.
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
  updated: {
    labelKey: 'chat.taskCard.statusUpdated',
    color: 'var(--color-warning)',
    Icon: RefreshCw,
  },
  resumed: {
    labelKey: 'chat.taskCard.statusResumed',
    color: 'var(--color-warning)',
    Icon: RotateCw,
  },
  unknown: {
    labelKey: null,
    color: 'var(--color-text-tertiary)',
    Icon: null,
  },
};

/**
 * The card status a task's own wire status maps onto, before a card's action
 * verb (`updated`/`resumed`) overrides it. Anything the vocabulary doesn't
 * name falls to `unknown`, which renders the wire value verbatim rather than
 * guessing an outcome.
 */
export function taskCardStatusKind(
  status: string | null | undefined,
): TaskCardStatusKind {
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
