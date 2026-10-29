/**
 * The computer-status vocabulary: one table and the predicates over it.
 *
 * A machine's lifecycle is its own vocabulary, shorter than the provider's
 * sandbox states and longer than a task's, so it gets its own table rather
 * than an override on someone else's. Every surface that shows whether a
 * machine is up reads this file, so the ladder is stated once; the renderer
 * is `ComputerStatusIndicator`.
 */
import { AlertCircle, CircleSlash } from 'lucide-react';
import type { LucideIcon } from 'lucide-react';
import type { useTranslation } from 'react-i18next';

import type { Computer, ComputerStatus } from '@/types/api';

type Translate = ReturnType<typeof useTranslation>['t'];

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
 * 'creating' is brief, since the create request claims the row for its start
 * at once, but a new machine's preparation can be watched from it.
 */
const TRANSITIONAL_STATUSES = new Set(['creating', 'starting', 'stopping']);

export function isComputerStatusTransitional(status: string | undefined | null): boolean {
  return !!status && TRANSITIONAL_STATUSES.has(status);
}

/**
 * A machine whose first sandbox is being built: a new computer, or a primary
 * on its first start. It has never had a sandbox, which is what tells this
 * apart from an ordinary restart. No workspace can be put on it until it is
 * ready, and the server refuses one with a 409.
 */
export function isComputerPreparing(
  computer: Pick<Computer, 'status' | 'provider_ref'> | null | undefined,
): boolean {
  return !!computer
    && (computer.status === 'creating' || computer.status === 'starting')
    && !computer.provider_ref;
}

/** The user's machines, without a removed one the list may still carry for a moment. */
export function liveComputers<C extends Pick<Computer, 'status'>>(computers: readonly C[] | null | undefined): C[] {
  return (computers ?? []).filter((c) => c.status !== 'deleted');
}

/**
 * Whether a workspace can be put on this machine now, by creating one there
 * or moving one to it: still the user's, able to start, and past its first build.
 */
export function takesWorkspaces(computer: Pick<Computer, 'status' | 'provider_ref'>): boolean {
  return computer.status !== 'deleted' && computer.status !== 'error' && !isComputerPreparing(computer);
}

export const PREPARING_UI: ComputerStatusUi = {
  labelKey: 'computer.statusPreparing',
  fallback: 'Preparing',
  shape: 'live',
  color: 'var(--color-accent-primary)',
};

/** The build steps of a new computer, in the order the server runs them. */
const PREPARE_STAGES = [
  { stage: 'sandbox', labelKey: 'computer.prepare.sandbox', fallback: 'Creating the sandbox' },
  { stage: 'tools', labelKey: 'computer.prepare.tools', fallback: 'Installing tools' },
  { stage: 'finishing', labelKey: 'computer.prepare.finishing', fallback: 'Finishing setup' },
] as const;

/** A stage's place in the build, or -1 for one this build does not know. */
export function prepareStageIndex(stage: string | undefined | null): number {
  return PREPARE_STAGES.findIndex((s) => s.stage === stage);
}

/**
 * "Creating the sandbox (1 of 3)". No stage yet reads as the first step: the
 * build always opens with it, and its frame is published as the start claims
 * the row, usually before anyone is listening. A stage this build does not
 * know has no place to count from, so it gets no step and the plain label stands.
 */
export function prepareStepLabel(
  t: Translate,
  stage: string | undefined | null,
  kind?: Computer['kind'],
): string | null {
  const index = stage == null ? 0 : prepareStageIndex(stage);
  if (index < 0) return null;
  const step = PREPARE_STAGES[index];
  // A machine the user brings has no sandbox made for it: its first step
  // lays out the folder and installs the runtime there.
  const label =
    kind === 'ssh' && step.stage === 'sandbox'
      ? t('computer.prepare.runtime', 'Installing the runtime')
      : t(step.labelKey, step.fallback);
  return t('computer.prepare.step', '{{step}} ({{index}} of {{total}})', {
    step: label,
    index: index + 1,
    total: PREPARE_STAGES.length,
  });
}
