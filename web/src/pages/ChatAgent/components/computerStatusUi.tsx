/**
 * The computer-status vocabulary: one table, one renderer.
 *
 * A machine's lifecycle is its own vocabulary, shorter than the provider's
 * sandbox states and longer than a task's, so it gets its own table rather
 * than an override on someone else's. Every surface that shows whether a
 * machine is up reads this file, so the ladder is stated once.
 */
import { AlertCircle, CircleSlash } from 'lucide-react';
import type { LucideIcon } from 'lucide-react';
import { useTranslation } from 'react-i18next';

import { Loader } from '@/components/ui/loader';
import type { ComputerStatus } from '@/types/api';

/** How a status is drawn. `live` is the only one that animates. */
type Shape = 'live' | 'dot' | 'hollow' | 'icon';

interface ComputerStatusUi {
  labelKey: string;
  /** English default, so a missing translation still reads as words. */
  fallback: string;
  shape: Shape;
  color: string;
  icon?: LucideIcon;
}

/**
 * Status to glyph + copy. Keyed by the status union, so adding a state to it
 * forces a row here and the two locale keys the row names. Unrecognized wire
 * values fail safe to `live`: a status this build has not heard of is one the
 * machine is presumably moving through, and showing it as settled would offer
 * a Start on a running box.
 */
export const COMPUTER_STATUS_UI: Record<ComputerStatus, ComputerStatusUi> = {
  creating: {
    labelKey: 'computer.statusCreating',
    fallback: 'Creating',
    shape: 'live',
    color: 'var(--color-accent-primary)',
  },
  starting: {
    labelKey: 'computer.statusStarting',
    fallback: 'Starting',
    shape: 'live',
    color: 'var(--color-accent-primary)',
  },
  running: {
    labelKey: 'computer.statusRunning',
    fallback: 'Running',
    shape: 'dot',
    color: 'var(--color-profit)',
  },
  stopping: {
    labelKey: 'computer.statusStopping',
    fallback: 'Stopping',
    shape: 'live',
    color: 'var(--color-accent-primary)',
  },
  stopped: {
    labelKey: 'computer.statusStopped',
    fallback: 'Stopped',
    shape: 'hollow',
    color: 'var(--color-text-tertiary)',
  },
  error: {
    labelKey: 'computer.statusError',
    fallback: 'Error',
    shape: 'icon',
    // The danger token, not --color-loss: that one belongs to market P&L.
    color: 'var(--color-icon-danger)',
    icon: AlertCircle,
  },
  deleted: {
    labelKey: 'computer.statusDeleted',
    fallback: 'Deleted',
    shape: 'icon',
    color: 'var(--color-text-tertiary)',
    icon: CircleSlash,
  },
};

const UNKNOWN: ComputerStatusUi = {
  labelKey: 'computer.statusUpdating',
  fallback: 'Updating',
  shape: 'live',
  color: 'var(--color-accent-primary)',
};

function isKnownStatus(status: string): status is ComputerStatus {
  return status in COMPUTER_STATUS_UI;
}

export function computerStatusUi(status: string | undefined | null): ComputerStatusUi {
  return status && isKnownStatus(status) ? COMPUTER_STATUS_UI[status] : UNKNOWN;
}

/** Statuses the backend's status stream closes on. */
const TERMINAL_STATUSES = new Set(['running', 'error', 'deleted']);

export function isComputerStatusTerminal(status: string | undefined | null): boolean {
  return !!status && TERMINAL_STATUSES.has(status);
}

/**
 * Statuses that move on without anyone acting. These are the ones worth
 * holding a status stream open for. A stopped machine only leaves that state
 * because a user starts it, and that action opens the stream itself.
 */
const TRANSITIONAL_STATUSES = new Set(['creating', 'starting', 'stopping']);

export function isComputerStatusTransitional(status: string | undefined | null): boolean {
  return !!status && TRANSITIONAL_STATUSES.has(status);
}

interface ComputerStatusIndicatorProps {
  status: string | undefined | null;
  /** Glyph size in px; the label follows the surrounding font size. */
  glyphSize?: number;
  /** Drop the words and keep the glyph, for a surface that names the state elsewhere. */
  glyphOnly?: boolean;
  className?: string;
}

/**
 * Glyph plus text, never a bar or a wash. The dot shapes carry no text of
 * their own, so the label is what a screen reader hears; a machine in flight
 * also gets `role="status"`, which announces the transition without stealing
 * focus. Only in flight: a gallery page of settled machines would otherwise be
 * eight live regions announcing nothing.
 */
export function ComputerStatusIndicator({
  status,
  glyphSize = 11,
  glyphOnly = false,
  className,
}: ComputerStatusIndicatorProps) {
  const { t } = useTranslation();
  const ui = computerStatusUi(status);
  const label = t(ui.labelKey, ui.fallback);
  const Icon = ui.icon;
  const announces = isComputerStatusTransitional(status);

  let glyph: React.ReactNode;
  if (ui.shape === 'live') {
    glyph = <Loader size={glyphSize} style={{ color: ui.color }} />;
  } else if (ui.shape === 'icon' && Icon) {
    glyph = <Icon style={{ width: glyphSize, height: glyphSize, color: ui.color }} />;
  } else {
    glyph = (
      <span
        className="rounded-full flex-shrink-0"
        style={{
          width: glyphSize * 0.6,
          height: glyphSize * 0.6,
          backgroundColor: ui.shape === 'dot' ? ui.color : 'transparent',
          border: ui.shape === 'hollow' ? `1px solid ${ui.color}` : undefined,
        }}
      />
    );
  }

  return (
    <span
      className={className ? `inline-flex items-center gap-1.5 ${className}` : 'inline-flex items-center gap-1.5'}
      role={announces ? 'status' : undefined}
      aria-live={announces ? 'polite' : undefined}
      title={glyphOnly ? label : undefined}
    >
      <span aria-hidden="true" className="inline-flex items-center flex-shrink-0">
        {glyph}
      </span>
      {glyphOnly ? <span className="sr-only">{label}</span> : <span>{label}</span>}
    </span>
  );
}
