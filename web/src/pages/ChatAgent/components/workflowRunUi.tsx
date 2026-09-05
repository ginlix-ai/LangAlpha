import React from 'react';
import { useTranslation } from 'react-i18next';
import {
  AlertCircle, Check, ChevronRight, StopCircle, type LucideIcon,
} from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import type {
  WorkflowChild, WorkflowChildStatus, WorkflowRunState,
} from '../session/subagents/workflowRunState';

/** The uppercase hairline label above every band in the run detail. */
export const SECTION_LABEL_STYLE: React.CSSProperties = {
  fontSize: '0.6875rem',
  color: 'var(--color-text-quaternary)',
  letterSpacing: '0.05em',
  textTransform: 'uppercase',
};

/** Elapsed seconds as `12.3s` under a minute, `2m 07s` above it. */
export function formatRunDuration(seconds: number | null | undefined): string | null {
  if (seconds == null || !Number.isFinite(seconds)) return null;
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  // Round the total before splitting: rounding the remainder on its own lets
  // it reach 60 and print an impossible `1m 60s` for anything in the top
  // half-second of a minute.
  const total = Math.round(seconds);
  const minutes = Math.floor(total / 60);
  const rest = total % 60;
  return `${minutes}m ${String(rest).padStart(2, '0')}s`;
}

/**
 * Per-child-status presentation, shared by the inline run card and the detail
 * panel. A stop is not a failure — it gets the same quiet treatment the run
 * header gives it — and an invalid result is amber, not red: the child ran,
 * its output just didn't match the schema.
 */
const WORKFLOW_CHILD_UI: Record<
  WorkflowChildStatus,
  { labelKey: string; color: string; Icon?: LucideIcon; live?: boolean }
> = {
  running: {
    labelKey: 'chat.workflowRun.childRunning',
    color: 'var(--color-warning)',
    live: true,
  },
  ok: { labelKey: 'chat.workflowRun.childDone', color: 'var(--color-success)', Icon: Check },
  invalid_schema: {
    labelKey: 'chat.workflowRun.childInvalid',
    color: 'var(--color-warning)',
    Icon: AlertCircle,
  },
  cancelled: {
    labelKey: 'chat.workflowRun.childStopped',
    color: 'var(--color-text-tertiary)',
    Icon: StopCircle,
  },
  timeout: {
    labelKey: 'chat.workflowRun.childTimedOut',
    color: 'var(--color-icon-danger)',
    Icon: AlertCircle,
  },
  error: { labelKey: 'chat.workflowRun.childFailed', color: 'var(--color-icon-danger)', Icon: AlertCircle },
};

/** i18n key for a child's status word — the row label and the icon's name. */
export function workflowChildLabelKey(status: WorkflowChildStatus): string {
  return (WORKFLOW_CHILD_UI[status] ?? WORKFLOW_CHILD_UI.error).labelKey;
}

/** The one colour a child's status is allowed to read as, row and detail alike.
 *
 * A schema miss is amber because the child ran — rendering its detail red
 * would contradict the row directly above it. */
export function workflowChildStatusColor(status: WorkflowChildStatus): string {
  return (WORKFLOW_CHILD_UI[status] ?? WORKFLOW_CHILD_UI.error).color;
}

export function WorkflowChildStatusIcon({
  status,
  size = 11,
}: {
  status: WorkflowChildStatus;
  size?: number;
}): React.ReactElement {
  const { t } = useTranslation();
  const ui = WORKFLOW_CHILD_UI[status] ?? WORKFLOW_CHILD_UI.error;
  const { Icon } = ui;
  if (ui.live || !Icon) {
    // Shared liveness glyph (nav tree, task cards): ascii spinner, amber.
    return (
      <Loader
        size={size}
        label={t(workflowChildLabelKey(status))}
        style={{ color: ui.color, flexShrink: 0 }}
      />
    );
  }
  return (
    <Icon
      aria-label={t(workflowChildLabelKey(status))}
      style={{
        width: size,
        height: size,
        flexShrink: 0,
        color: ui.color,
      }}
    />
  );
}

/** The run totals both surfaces put in their band: how many children have
 *  settled, how many were promised, and how long the run took. */
export function summarizeRun(run: WorkflowRunState | undefined): {
  children: WorkflowChild[];
  doneCount: number;
  agentCount: number;
  duration: string | null;
} {
  const children = run?.children ?? [];
  return {
    children,
    doneCount: children.filter((c) => c.status !== 'running').length,
    // `childrenTotal` is the script's promise, `children.length` what has been
    // dispatched so far — the promise wins once the run reports it.
    agentCount: run?.childrenTotal ?? children.length,
    duration: formatRunDuration(run?.durationS),
  };
}

export type WorkflowChildRowSurface = 'card' | 'detail';

interface ChildRowSurfaceUi {
  testId: string;
  row: React.CSSProperties;
  iconSize: number;
  labelColor: string;
  /** Set only where a cell's size differs from the row's own. */
  cellFontSize?: string;
  durationMinWidth: number;
  /** The inline card drops an unknown dispatch type; the detail keeps the
   *  column so every row's trailing cells stay on the same rails. */
  alwaysShowType: boolean;
  /** An unfinished child has no duration: the card leaves the cell blank, the
   *  detail names the child's state there instead. */
  namesStatusWhenUnfinished: boolean;
}

/** Two densities of one row. The inline card is a glance — tight, quiet, no
 *  affordance; the detail is a list you navigate — roomier, hoverable, and it
 *  carries per-child telemetry. Everything else about the row is shared. */
const CHILD_ROW_UI: Record<WorkflowChildRowSurface, ChildRowSurfaceUi> = {
  card: {
    testId: 'workflow-child-row',
    row: { display: 'flex', alignItems: 'center', gap: 8, fontSize: '0.6875rem', minWidth: 0 },
    iconSize: 11,
    labelColor: 'var(--color-text-secondary)',
    durationMinWidth: 42,
    alwaysShowType: false,
    namesStatusWhenUnfinished: false,
  },
  detail: {
    testId: 'workflow-detail-child-row',
    row: {
      display: 'flex',
      alignItems: 'center',
      gap: 10,
      padding: '6px 8px',
      margin: '0 -8px',
      borderRadius: 6,
      fontSize: '0.75rem',
      minWidth: 0,
      cursor: 'default',
    },
    iconSize: 12,
    labelColor: 'var(--color-text-primary)',
    cellFontSize: '0.6875rem',
    durationMinWidth: 64,
    alwaysShowType: true,
    namesStatusWhenUnfinished: true,
  },
};

/**
 * One dispatched child: status glyph, label, dispatch type and elapsed time.
 * Both the inline run card and the detail panel render this, so the label
 * fallback and the overflow rules cannot drift between them.
 */
export function WorkflowChildRow({
  child,
  surface,
  meta,
  onOpen,
}: {
  child: WorkflowChild;
  surface: WorkflowChildRowSurface;
  /** Trailing telemetry cell (tool calls · tokens); detail only. */
  meta?: string;
  /** Makes the row a control that opens the child's own task view. */
  onOpen?: () => void;
}): React.ReactElement {
  const { t } = useTranslation();
  const ui = CHILD_ROW_UI[surface];
  const cellFontSize = ui.cellFontSize;
  return (
    <div
      role={onOpen ? 'button' : undefined}
      tabIndex={onOpen ? 0 : undefined}
      data-testid={ui.testId}
      onClick={onOpen}
      onKeyDown={
        onOpen
          ? (e: React.KeyboardEvent<HTMLDivElement>) => {
              if (e.key === 'Enter' || e.key === ' ') {
                e.preventDefault();
                onOpen();
              }
            }
          : undefined
      }
      onMouseEnter={
        onOpen
          ? (e: React.MouseEvent<HTMLDivElement>) =>
              (e.currentTarget.style.background = 'var(--color-bg-elevated)')
          : undefined
      }
      onMouseLeave={
        onOpen
          ? (e: React.MouseEvent<HTMLDivElement>) =>
              (e.currentTarget.style.background = 'transparent')
          : undefined
      }
      style={onOpen ? { ...ui.row, cursor: 'pointer' } : ui.row}
    >
      <WorkflowChildStatusIcon status={child.status} size={ui.iconSize} />
      <span
        style={{
          color: ui.labelColor,
          overflow: 'hidden',
          textOverflow: 'ellipsis',
          whiteSpace: 'nowrap',
          minWidth: 0,
          flex: '1 1 auto',
        }}
      >
        {child.label || t('chat.workflowRun.childFallbackLabel', { n: child.seq + 1 })}
      </span>
      {meta && (
        <span
          style={{
            color: 'var(--color-text-quaternary)',
            whiteSpace: 'nowrap',
            flexShrink: 0,
            fontSize: cellFontSize,
          }}
        >
          {meta}
        </span>
      )}
      {(ui.alwaysShowType || !!child.subagentType) && (
        <span
          style={{
            color: 'var(--color-text-quaternary)',
            whiteSpace: 'nowrap',
            flexShrink: 0,
            fontSize: cellFontSize,
          }}
        >
          {child.subagentType}
        </span>
      )}
      <span
        style={{
          color: 'var(--color-text-tertiary)',
          whiteSpace: 'nowrap',
          flexShrink: 0,
          fontSize: cellFontSize,
          minWidth: ui.durationMinWidth,
          textAlign: 'right',
        }}
      >
        {formatRunDuration(child.durationS)
          ?? (ui.namesStatusWhenUnfinished ? t(workflowChildLabelKey(child.status)) : '')}
      </span>
      {onOpen && (
        <ChevronRight
          aria-hidden="true"
          style={{ width: 13, height: 13, flexShrink: 0, color: 'var(--color-text-quaternary)' }}
        />
      )}
    </div>
  );
}
