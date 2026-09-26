/**
 * The one place an automation's state and a run's status become a group, a
 * word and a glyph, so the feed, the list and the inspector cannot disagree.
 *
 * Liveness is the shared ascii Loader (`live`), a finished run carries no
 * glyph, and only a failure gets an informative one (DESIGN.md, status
 * vocabulary). A paused automation is not a failure: it keeps its group and
 * shows the pause glyph in the muted tone.
 */
import { AlertCircle, Clock, Pause, type LucideIcon } from 'lucide-react';
import type { Automation, AutomationExecution, ExecutionStatus, FailureReason, SkipReason } from '@/types/automation';
import { buildRateLimitError, type ErrorLinkSpec } from '@/utils/rateLimitError';

export function isRunLive(status: ExecutionStatus): boolean {
  return status === 'running' || status === 'pending';
}

export function isRunFailed(status: ExecutionStatus): boolean {
  return status === 'failed' || status === 'timeout';
}

/** Held until the turn running in its thread ends. A price automation reads
 *  `executing` meanwhile, so the execution row decides. */
export function isAutomationWaiting(a: Automation): boolean {
  return a.last_execution?.status === 'waiting';
}

/** Price automations flip to `executing`; cron and once runs only show on the
 *  execution row, so both have to be read. */
export function isAutomationRunning(a: Automation): boolean {
  if (isAutomationWaiting(a)) return false;
  return a.status === 'executing' || (!!a.last_execution && isRunLive(a.last_execution.status));
}

export type AutomationGroup = 'attention' | 'scheduled' | 'watching' | 'finished';

export const GROUP_ORDER: readonly AutomationGroup[] = ['attention', 'scheduled', 'watching', 'finished'];

export const GROUP_LABEL_KEY: Record<AutomationGroup, string> = {
  attention: 'automation.groupAttention',
  scheduled: 'automation.groupScheduled',
  watching: 'automation.groupWatching',
  finished: 'automation.groupFinished',
};

/**
 * Disabled means the server switched it off, after too many failures or a key
 * the provider rejected; a last run that failed, a usage limit's refusal
 * included, means the next one may fail the same way, or, once the automation
 * has ended, that its report never came. Either wants a person, unless the
 * person already paused it.
 */
export function needsAttention(a: Automation): boolean {
  if (a.status === 'disabled') return true;
  if (a.status === 'paused') return false;
  return !isAutomationRunning(a) && !!a.last_execution && isRunFailed(a.last_execution.status);
}

/**
 * Why an automation wants a person, which decides what it is called and what
 * it offers. A rejected key and a run of failures both switch it off, so
 * either is resumed. A usage limit never switches it off: a schedule carries
 * on and a one-shot price alert ends, and the reader gets the choice any
 * failed run gets, another try, or a pause while there is still one to pause.
 */
export type AttentionKind = 'key_rejected' | 'switched_off' | 'usage_limit' | 'failed';

export function attentionKind(a: Automation): AttentionKind | null {
  if (!needsAttention(a)) return null;
  if (a.status === 'disabled') return a.disable_reason === 'provider_auth' ? 'key_rejected' : 'switched_off';
  return a.last_execution?.failure_reason === 'usage_limit' ? 'usage_limit' : 'failed';
}

/** Next due by the clock: what the rail lists under "Up next" and the header
 *  counts as scheduled. A paused or price automation is not. */
export function isUpcoming(a: Automation): boolean {
  return a.status === 'active' && !!a.next_run_at && a.trigger_type !== 'price';
}

/** A price automation still listening, mid-run included. */
export function isWatching(a: Automation): boolean {
  return a.trigger_type === 'price' && (a.status === 'active' || a.status === 'executing');
}

/**
 * How many automations sit in each state the page names: every caller that
 * sums them up, the header's census and the dashboard widget, reads this one.
 * Running overlaps the rest, since a run in flight still belongs to its kind;
 * the rest count each automation once, in this order, so they add up.
 */
export interface AutomationCensus {
  running: number;
  attention: number;
  scheduled: number;
  watching: number;
  paused: number;
  finished: number;
}

export function automationCensus(automations: readonly Automation[]): AutomationCensus {
  const census: AutomationCensus = { running: 0, attention: 0, scheduled: 0, watching: 0, paused: 0, finished: 0 };
  for (const a of automations) {
    if (isAutomationRunning(a)) census.running += 1;
    if (needsAttention(a)) census.attention += 1;
    else if (isUpcoming(a)) census.scheduled += 1;
    else if (isWatching(a)) census.watching += 1;
    else if (a.status === 'paused') census.paused += 1;
    else if (a.status === 'completed') census.finished += 1;
  }
  return census;
}

/** The manage list's grouping, by kind: a paused cron stays under Scheduled
 *  there, since it is still a schedule. What runs next is isUpcoming. */
export function automationGroup(a: Automation): AutomationGroup {
  if (needsAttention(a)) return 'attention';
  if (a.status === 'completed') return 'finished';
  return a.trigger_type === 'price' ? 'watching' : 'scheduled';
}

/** Everything an automation's glyph and label can say. */
export type AutomationState = 'waiting' | 'running' | AttentionKind | 'paused' | 'finished' | 'watching' | 'scheduled';

/**
 * The ladder: which one state an automation shows. A run in flight comes
 * first, since it may be about to fix the failure below it; then why it wants
 * a person, then a pause, an end, and what kind of automation it is. The
 * groups and the header's census do not read it, since they count an
 * automation that is mid-run under its kind as well.
 */
export function automationState(a: Automation): AutomationState {
  if (isAutomationWaiting(a)) return 'waiting';
  if (isAutomationRunning(a)) return 'running';
  const attention = attentionKind(a);
  if (attention) return attention;
  if (a.status === 'paused') return 'paused';
  if (a.status === 'completed') return 'finished';
  return a.trigger_type === 'price' ? 'watching' : 'scheduled';
}

export interface StatusUi {
  labelKey: string;
  Icon: LucideIcon | null;
  /** Renders the ascii liveness glyph instead of an icon. */
  live?: boolean;
  /** The glyph's color. Amber is annotation only, so a label beside the
   *  glyph takes this color only when `danger` says it is a failure. */
  color: string;
  danger?: boolean;
}

const DANGER = 'var(--color-icon-danger)';

const STATE_UI: Record<AutomationState, StatusUi> = {
  waiting: { labelKey: 'automation.stateWaiting', Icon: Clock, color: 'var(--color-accent-primary)' },
  running: { labelKey: 'automation.stateRunning', Icon: null, live: true, color: 'var(--color-accent-primary)' },
  key_rejected: { labelKey: 'automation.stateKeyRejected', Icon: AlertCircle, color: DANGER, danger: true },
  switched_off: { labelKey: 'automation.stateDisabled', Icon: AlertCircle, color: DANGER, danger: true },
  usage_limit: { labelKey: 'automation.stateUsageLimit', Icon: AlertCircle, color: DANGER, danger: true },
  failed: { labelKey: 'automation.stateLastRunFailed', Icon: AlertCircle, color: DANGER, danger: true },
  paused: { labelKey: 'automation.statePaused', Icon: Pause, color: 'var(--color-text-tertiary)' },
  finished: { labelKey: 'automation.stateFinished', Icon: null, color: 'var(--color-text-tertiary)' },
  watching: { labelKey: 'automation.stateWatching', Icon: null, color: 'var(--color-text-tertiary)' },
  scheduled: { labelKey: 'automation.stateScheduled', Icon: null, color: 'var(--color-text-tertiary)' },
};

/** An automation's state, as the kicker and the row glyph read it. */
export function automationStatusUi(a: Automation): StatusUi {
  return STATE_UI[automationState(a)];
}

/** The actions the server takes from each state: pause from active, resume
 *  from paused or disabled, and a manual run from anything but disabled,
 *  offered only while nothing is already running or waiting. A failed run is
 *  answered by `remedy`: a resume once the server has switched the automation
 *  off, else another run, so the rail and the feed offer the same one. */
export interface AutomationActions {
  canPause: boolean;
  canResume: boolean;
  canRun: boolean;
  runBusy: boolean;
  remedy: 'resume' | 'retry' | null;
}

export function automationActions(a: Automation): AutomationActions {
  const state = automationState(a);
  const canRun = a.status !== 'disabled';
  const runBusy = state === 'waiting' || state === 'running';
  return {
    canPause: a.status === 'active',
    canResume: a.status === 'paused' || a.status === 'disabled',
    canRun,
    runBusy,
    remedy: !canRun ? 'resume' : runBusy ? null : 'retry',
  };
}

/** The same fallback every caller of the quota denial builder uses. */
const PLATFORM_URL = (import.meta.env.VITE_PLATFORM_URL as string | undefined) || '/account';

/** Where a run a usage limit stopped sends the reader: the plan and usage
 *  pages every quota denial offers, from the builder that words them, so an
 *  automation never links somewhere a chat would not. */
export function usageLimitLinks(run: AutomationExecution): ErrorLinkSpec[] {
  if (!isRunFailed(run.status) || run.failure_reason !== 'usage_limit') return [];
  return buildRateLimitError({ message: run.error_message ?? undefined }, PLATFORM_URL).links ?? [];
}

/** Where the chat sends a reader to manage their models and the keys behind
 *  them: a rejected key is fixed there before a resume. */
const MODEL_SETTINGS_LINK: ErrorLinkSpec = {
  url: '/settings?tab=model',
  label: 'Manage models',
  labelKey: 'chat.modelSelector.manageModels',
};

/** Where the reader fixes what stopped an automation's newest run: model
 *  settings for a rejected key, the plan pages for a usage limit. */
export function attentionLinks(a: Automation): ErrorLinkSpec[] {
  if (attentionKind(a) === 'key_rejected') return [MODEL_SETTINGS_LINK];
  return a.last_execution ? usageLimitLinks(a.last_execution) : [];
}

/** When an automation last ran: its newest run's end, or its start while it
 *  has not ended. */
export function lastRunAt(a: Automation): string | null {
  return a.last_execution?.completed_at ?? a.last_execution?.started_at ?? a.last_run_at;
}

/**
 * What a row says at its far end, by the same rule on every list: a state
 * the glyph alone would leave unnamed, else the next run for a schedule, the
 * distance to the trigger for a watch (`reading`, which only a caller holding
 * quotes can word; `at` is the fallback), else when it last ran.
 */
export type RowTrailing =
  | { kind: 'state'; labelKey: string }
  | { kind: 'next'; at: string }
  | { kind: 'reading'; at: string | null }
  | { kind: 'last'; at: string }
  | null;

export function rowTrailing(a: Automation, group: AutomationGroup): RowTrailing {
  const state = automationState(a);
  if (state === 'waiting' || state === 'running' || state === 'paused') {
    return { kind: 'state', labelKey: STATE_UI[state].labelKey };
  }
  if (group === 'scheduled' && a.next_run_at) return { kind: 'next', at: a.next_run_at };
  const at = lastRunAt(a);
  if (group === 'watching') return { kind: 'reading', at };
  return at ? { kind: 'last', at } : null;
}

const RUN_STATUS_UI: Record<ExecutionStatus, StatusUi> = {
  pending: { labelKey: 'automation.runQueued', Icon: null, live: true, color: 'var(--color-accent-primary)' },
  waiting: { labelKey: 'automation.runWaiting', Icon: Clock, color: 'var(--color-accent-primary)' },
  running: { labelKey: 'automation.runRunning', Icon: null, live: true, color: 'var(--color-accent-primary)' },
  completed: { labelKey: 'automation.runCompleted', Icon: null, color: 'var(--color-text-secondary)' },
  failed: { labelKey: 'automation.runFailed', Icon: AlertCircle, color: DANGER, danger: true },
  timeout: { labelKey: 'automation.runTimedOut', Icon: AlertCircle, color: DANGER, danger: true },
  skipped: { labelKey: 'automation.runSkipped', Icon: null, color: 'var(--color-text-tertiary)' },
};

/** A status this build has no word for: a newer server's, read by a tab that
 *  predates it, which is why the API keeps the column an open string. */
const UNKNOWN_RUN_UI: StatusUi = { labelKey: 'automation.runUnknown', Icon: null, color: 'var(--color-text-tertiary)' };

/** The line a skipped run carries when the reader did not skip it. Both
 *  tables name every reason the server writes, a test holds them to its
 *  Literals, and null is a reason with nothing to add. */
const SKIP_REASON_NOTE: Record<SkipReason, string | null> = {
  user: null,
  thread_busy: 'automation.skippedThreadBusy',
  interrupted: 'automation.skippedInterrupted',
};

/** The line a failed run carries when the failure was not the automation's.
 *  A usage limit and a rejected key are told by the error, the attention
 *  state and its links instead. */
const FAILURE_REASON_NOTE: Record<FailureReason, string | null> = {
  usage_limit: null,
  provider_auth: null,
  server_error: 'automation.failedServerError',
  interrupted: 'automation.failedInterrupted',
};

/** One run as the feed entry, the latest report and the history row read it.
 *  Whether it is live or failed is its glyph's to say: `ui.live`, `ui.danger`. */
export interface RunView {
  ui: StatusUi;
  waiting: boolean;
  /** Why it waits, was skipped, or failed through no fault of its own; null
   *  when there is nothing to say. */
  noteKey: string | null;
  /** Only a run that actually ran to an end has a length worth stating. */
  showDuration: boolean;
}

export function describeRun(run: AutomationExecution): RunView {
  const { status } = run;
  const waiting = status === 'waiting';
  let noteKey: string | null = null;
  if (waiting) noteKey = 'automation.waitingNote';
  else if (status === 'skipped' && run.skip_reason) noteKey = SKIP_REASON_NOTE[run.skip_reason] ?? null;
  else if (isRunFailed(status) && run.failure_reason) noteKey = FAILURE_REASON_NOTE[run.failure_reason] ?? null;
  const ended = status === 'completed' || isRunFailed(status);
  return {
    ui: RUN_STATUS_UI[status] ?? UNKNOWN_RUN_UI,
    waiting,
    noteKey,
    showDuration: ended && !!run.started_at && !!run.completed_at,
  };
}
