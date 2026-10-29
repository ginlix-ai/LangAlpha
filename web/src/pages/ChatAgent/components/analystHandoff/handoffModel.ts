/**
 * What a hand-off to a workspace analyst says about itself, read from its
 * proposal, its place in the conversation and its run: the words it leads
 * with, the phase it is in, and how each phase reads on a slip, a group's row
 * and its header. Import-free of React so the reading can be asserted on its
 * own.
 */
import { formatRunLength, runLengthMs, type TFn } from '@/lib/elapsed';
import { flattenInline, flattenMarkdown } from '@/lib/plainLine';
import type { PTCAgentProposalState } from '@/types/chat';
import type {
  DispatchPin,
  DispatchReadStatus,
  DispatchStatusView,
  DispatchWatch,
} from '../../hooks/usePTCDispatchStatus';
import type { RunGlyphKind, RunGroupPart } from '../messageList/runCard';
import type { HandoffPlace } from './reportLinks';

/** A `delegate_to_analyst` hand-off, as its proposal or its result carries it. */
export type ProposalData = PTCAgentProposalState;

/**
 * Where a handed-off run stands, as the slip and a group's row show it. Run
 * statuses map one to one, except that a dispatch that failed before any run
 * began, or a steer no run took in, is its own state.
 */
export type HandoffPhase =
  | 'starting'
  | 'working'
  | 'needs_input'
  | 'finished'
  | 'failed'
  | 'stopped'
  | 'not_started';

interface PhaseLook {
  labelKey: string;
  /** The same words with how long the run took, once the ledger knows. */
  afterKey?: string;
  glyph: RunGlyphKind | null;
  /** Said in the danger color: the run did not do what it was handed. */
  danger?: boolean;
  /** The run still moves: a group's header leads with this glyph, the first
   *  in the table's order. A phase without one is over, so a report can
   *  answer it. */
  lead?: 'live' | 'ring';
  /** A working run's clock follows its words. */
  clock?: boolean;
  /** What a row offers beyond opening the thread. */
  action?: 'answer';
  /** A group header's count; phases with the same key count as one. */
  tally: string;
  /** Kept whole on a narrow header: what went wrong. */
  keep?: boolean;
  /** For a phase a report can name: the report-back line's words, after the
   *  analyst's name. */
  reportKey?: string;
}

/**
 * Each phase, in the order a group's header counts them, which is the order
 * the reader acts on them. A finished run shows no glyph, as on every dense
 * surface: the spinner going away is the done state.
 */
export const HANDOFF_PHASES: Readonly<Record<HandoffPhase, PhaseLook>> = {
  starting: { labelKey: 'chat.ptcCard.statusStarting', glyph: 'pending', lead: 'live', tally: 'chat.ptcCard.summaryWorking' },
  working: { labelKey: 'chat.ptcCard.statusWorking', glyph: 'live', lead: 'live', clock: true, tally: 'chat.ptcCard.summaryWorking' },
  needs_input: {
    labelKey: 'chat.ptcCard.needsAnswer', glyph: 'ring', lead: 'ring', action: 'answer', tally: 'chat.ptcCard.summaryNeedsAnswer',
  },
  // The same words whether or not its report is in: the report's own line
  // says it reported back, and the row's link goes down to it.
  finished: {
    labelKey: 'chat.ptcCard.finished', afterKey: 'chat.ptcCard.finishedAfter', glyph: null, tally: 'chat.ptcCard.summaryFinished',
    reportKey: 'chat.provenance.reportedBack',
  },
  failed: {
    labelKey: 'chat.ptcCard.couldntFinish', glyph: 'alert', danger: true, tally: 'chat.ptcCard.summaryCouldntFinish', keep: true,
    reportKey: 'chat.provenance.couldntFinish',
  },
  stopped: {
    labelKey: 'chat.ptcCard.stopped', afterKey: 'chat.ptcCard.stoppedAfter', glyph: 'stop', tally: 'chat.ptcCard.summaryStopped', keep: true,
    reportKey: 'chat.provenance.stopped',
  },
  not_started: {
    labelKey: 'chat.ptcCard.couldntStart', glyph: 'alert', danger: true, tally: 'chat.ptcCard.summaryCouldntStart', keep: true,
  },
};

export function handoffPhase({
  runStatus,
  dispatchFailed,
}: {
  runStatus: DispatchReadStatus;
  dispatchFailed: boolean;
}): HandoffPhase {
  // A failed dispatch has no run to wait for, unless one turns up on the
  // thread an unknown outcome named.
  if (dispatchFailed && runStatus === 'starting') return 'not_started';
  switch (runStatus) {
    case 'starting': return 'starting';
    case 'running': return 'working';
    case 'needs_input': return 'needs_input';
    case 'completed': return 'finished';
    case 'failed': return 'failed';
    case 'stopped': return 'stopped';
    // A steer no run took in started nothing, as a failed dispatch did.
    case 'dropped': return 'not_started';
  }
}

/** Where a run stands in a group's column of glyphs, where an empty place
 *  would read as a gap: a finished run, which goes without one beside its
 *  words, takes `done`. */
export function columnGlyph(phase: HandoffPhase): RunGlyphKind {
  return HANDOFF_PHASES[phase].glyph ?? 'done';
}

/** A phase's words, with how long the run took where they say it. */
export function phaseWords(phase: HandoffPhase, startedAt: number | undefined, finishedAt: number | undefined, t: TFn): string {
  const look = HANDOFF_PHASES[phase];
  const durationMs = runLengthMs(startedAt, finishedAt);
  return look.afterKey && durationMs !== undefined
    ? t(look.afterKey, { duration: formatRunLength(durationMs, t) })
    : t(look.labelKey);
}

/** A group header's line: the glyph of the phase that still moves, if any
 *  does, and a count per tally in the table's order. */
export function handoffTally(phases: readonly HandoffPhase[], t: TFn): { lead?: RunGlyphKind; parts: RunGroupPart[] } {
  let lead: RunGlyphKind | undefined;
  const tallies = new Map<string, { count: number; danger?: boolean; keep?: boolean }>();
  for (const [phase, look] of Object.entries(HANDOFF_PHASES) as [HandoffPhase, PhaseLook][]) {
    const count = phases.filter((p) => p === phase).length;
    if (count === 0) continue;
    lead ??= look.lead;
    const tally = tallies.get(look.tally);
    tallies.set(look.tally, { count: (tally?.count ?? 0) + count, danger: look.danger, keep: look.keep });
  }
  return {
    lead,
    parts: [...tallies].map(([key, { count, danger, keep }]) => ({ text: t(key, { count }), danger, keep })),
  };
}

/**
 * The run a hand-off follows, or null when there is none: it was not handed
 * off, names no thread, or its dispatch definitely failed. One whose outcome
 * is unknown follows its thread, but only a run begun after it was made. Once
 * a report answers it, it ended on the run that report is on, however the
 * report says that ended, whatever the thread runs after. Before then, one
 * steered into a run already going follows the run that took its steer in,
 * which the poll names, since the run it was steered into can end without
 * reading it; that run stands in only for a steer the poll does not know.
 * Else the run its dispatch named, which an interrupt hands on to the
 * thread's next run.
 */
export function followedRun(proposal: ProposalData, place: HandoffPlace = {}): DispatchWatch | null {
  const threadId = proposal.thread_id;
  const failed = proposal.dispatch_failed === true;
  const unknown = failed && proposal.dispatch_unknown === true;
  if (proposal.status !== 'approved' || !threadId || (failed && !unknown)) return null;
  return { threadId, pin: pinOf(proposal, place, unknown), outcome: place.reportId ? place.outcome : undefined };
}

function pinOf(proposal: ProposalData, place: HandoffPlace, unknown: boolean): DispatchPin {
  const { run_id: runId, steer_id: steerId } = proposal;
  if (place.reportRunId) return { kind: 'run', runId: place.reportRunId };
  if (steerId && runId) return { kind: 'steer', steerId, steeredInto: runId };
  if (runId) return { kind: 'run', runId };
  return { kind: 'thread', since: unknown ? place.madeAt : undefined };
}

/** Where one handed-off run stands, as a slip or a group's row draws it. */
export interface HandoffState {
  phase: HandoffPhase;
  /** The run's title once it is named, the brief as one line until then. */
  task: string;
  /** The workspace's current name; empty when nothing has named it. */
  workspaceName: string;
  /** Sent into a run of the analyst's already going, not one of its own. */
  steered: boolean;
  startedAt?: number;
  finishedAt?: number;
  /** The report row that answers this run, once the run is over and the row
   *  is in the conversation. */
  reportId?: string;
  /** Whether a read has spoken: until then the phase is the default. */
  read: boolean;
}

/** One handed-off run, as a slip or a group's row draws it. */
export interface HandoffView extends HandoffState {
  /** The hand-off's key in its run of hand-offs. */
  id: string;
  /** Opens the analyst's thread. Absent while no thread is known. */
  onOpen?: () => void;
  /** Goes down to this run's report, or opens the thread when the report
   *  cannot be shown. Absent until a report answers this run and each run
   *  drawn with it, and while all those reports follow straight on. */
  onSeeReport?: () => void;
}

/** A hand-off as its proposal, its place and its run's read have it. */
export function describeHandoff(proposal: ProposalData, place: HandoffPlace, run: DispatchStatusView): HandoffState {
  const phase = handoffPhase({ runStatus: run.status, dispatchFailed: proposal.dispatch_failed === true });
  return {
    phase,
    task: namedTitle(run.title, proposal.question) ?? plainBrief(proposal.question),
    workspaceName: run.workspaceName || proposal.workspace_name?.trim() || '',
    // A steer no run took in was sent into nothing.
    steered: !!proposal.steer_id && phase !== 'not_started',
    startedAt: run.startedAt,
    finishedAt: run.finishedAt,
    // A report is the answer to a run that ended, however it ended.
    reportId: HANDOFF_PHASES[phase].lead ? undefined : place.reportId,
    read: run.read,
  };
}

/**
 * The thread's title, unless it is still the placeholder a new thread is given
 * until its title is generated: the question's first 255 code points, as the
 * server slices them. An exact match, so a generated title that happens to open
 * the brief still shows. Both sides trimmed, as the liveness slice trims.
 */
export function namedTitle(title: string | undefined, question: string): string | null {
  if (!title || title === Array.from(question).slice(0, 255).join('').trim()) return null;
  return title;
}

const BLOCK_LINE = /^\s*(#{1,6}|>|[-*+]|\d+\.)\s+(.*)$/;
const ENDS_CLAUSE = /[.!?:;,\u3001\u3002\uff01\uff1f\uff1a\uff1b\uff0c]$/;
const CJK_END = /[\u3400-\u9fff]$/;

/**
 * The brief as one line of plain text, for a clamped preview. A heading or a
 * list item ends its line in the source, so on one line it is closed with a
 * colon or a full stop, or `## Goal` and the sentence under it would run
 * together as "Goal Rebuild the model".
 */
export function plainBrief(question: string): string {
  return flattenMarkdown(question.split('\n').map(closeBlock).join('\n'));
}

/** Judged on the words it shows, so `- **Note:**` is already closed. */
function closeBlock(line: string): string {
  const block = BLOCK_LINE.exec(line);
  if (!block) return line;
  const text = flattenInline(block[2]).trim();
  if (!text || ENDS_CLAUSE.test(text)) return line;
  const heading = block[1].startsWith('#');
  const cjk = CJK_END.test(text);
  return line.trimEnd() + (heading ? (cjk ? '\uff1a' : ':') : cjk ? '\u3002' : '.');
}
