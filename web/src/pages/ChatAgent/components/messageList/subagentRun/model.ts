/**
 * What a run of subagents shows, derived once from the message's task records
 * and their telemetry, so the lone row, the group header and its rows cannot
 * disagree about a member's state or its clock.
 *
 * Times are shown only when they are known. A run replayed from before the
 * server kept them has no start or finish, and then shows no elapsed rather
 * than a guess.
 */
import type { SubagentTaskRecord } from '@/types/chat';
import { CREDIT_STOP_ERROR_TYPE } from '@/types/sse';
import { llmProviderArt } from '@/lib/brandArt';
import { formatClock, formatTook, type TFn } from '@/lib/elapsed';
import type { SubagentTelemetry } from '../../../session/subagents/resolveSubagentTelemetry';
import type { SubagentModel } from '../../../session/subagents/taskRuns';
import { getActiveLabel } from '../../toolDisplayConfig';
import { SUBAGENT_ROW_UI, type SubagentRowKind } from '../../taskStatusUi';
import type { RunGlyphKind, RunGroupPart } from '../runCard';
import type { SubagentInfo, ToolCallProcessRecord } from '../types';
import { clampReason, refusalReason, resultLine, summarize } from './text';

export interface SubagentRunMember {
  subagentId: string;
  task: SubagentTaskRecord | undefined;
  toolCallProcess?: ToolCallProcessRecord;
}

export interface SubagentRowView {
  id: string;
  type: string;
  kind: SubagentRowKind;
  /** The wire status, shown verbatim for a kind the vocabulary does not name. */
  rawStatus: string;
  title: string;
  /** The description uncut, for the tooltip a cut title needs. */
  fullTitle: string;
  /** What it is doing now, what it returned, or why it stopped; empty when
   *  there is nothing to add to the status beside the title. */
  detail: string;
  startMs?: number;
  /** When a settled run finished; a live one runs to now. */
  endMs?: number;
  /** Whether its run goes on: a launch's while it runs, a resume's while the
   *  run it started does. */
  live: boolean;
  /** The model it ran on, only when that differs from its launcher's and its
   *  provider has a mark we ship: shown as a badge, and one with no picture
   *  says nothing. */
  model?: SubagentModel;
  /** What opening the row opens; absent when there is nothing to open. */
  openInfo?: SubagentInfo;
}

/** `_createdAt` is stamped 1 on a reconnect, a sentinel rather than a time. */
const EPOCH_FLOOR_MS = 1_000_000_000_000;

function launchedAt(proc: ToolCallProcessRecord | undefined): number | undefined {
  const at = proc?._createdAt;
  return typeof at === 'number' && at > EPOCH_FLOOR_MS ? at : undefined;
}

/** Any sign the run itself has begun, beyond its launch being accepted. A
 *  tool call always follows a model call, so tokens cover the calls too. */
function hasBegun(telemetry: SubagentTelemetry | undefined): boolean {
  if (!telemetry) return false;
  return telemetry.startedAtMs != null
    || (telemetry.tokenUsage?.total ?? 0) > 0
    || !!telemetry.currentToolCall;
}

/**
 * `followed` says whether the telemetry on hand follows runs as they go. Where
 * it does not, a run that has not begun cannot be told from one this view
 * never hears from, so a running task reads as working, as its record says.
 */
export function subagentRowKind(
  task: SubagentTaskRecord,
  telemetry: SubagentTelemetry | undefined,
  pausing: boolean,
  followed = true,
): SubagentRowKind {
  // A refused follow-up never reached its task, so its verb would claim a
  // change that did not happen.
  if (task.status === 'error' && task.launchFailed) return 'refused';
  if (task.action === 'update') return 'updated';
  if (task.action === 'resume') {
    // A resume starts a run, and its record settles with that run: how it
    // went wrong is the news then, not the verb.
    if (task.status === 'error') return 'failed';
    if (task.status === 'cancelled') return 'stopped';
    return 'resumed';
  }
  if (task.action !== 'init') return 'unknown';
  switch (task.status) {
    case 'running':
      if (pausing) return 'pausing';
      return !followed || hasBegun(telemetry) ? 'working' : 'starting';
    case 'completed':
      return 'done';
    case 'cancelled':
      return 'stopped';
    case 'error':
      return 'failed';
    default:
      return 'unknown';
  }
}

/** The second line. The status already stands beside the title, so a state
 *  with nothing more to say leaves the line empty rather than repeat it. */
function memberDetail(
  kind: SubagentRowKind,
  task: SubagentTaskRecord,
  telemetry: SubagentTelemetry | undefined,
  root: string | null | undefined,
  t: TFn,
): string {
  switch (kind) {
    case 'working':
    case 'pausing': {
      const call = telemetry?.currentToolCall;
      return call ? getActiveLabel(call.toolName, { args: call.args }, t, root) : '';
    }
    case 'done':
      return resultLine(telemetry?.lastText);
    case 'failed':
    case 'stopped': {
      // A credit stop is told by the notice at the foot of the message;
      // saying it here too would make the notice look optional.
      const reason = telemetry?.stopReason;
      return reason && telemetry?.stopReasonType !== CREDIT_STOP_ERROR_TYPE
        ? clampReason(reason)
        : '';
    }
    case 'refused': {
      if (task.launchRefusal === 'subagents_off') return t('chat.subagentRun.subagentsOff');
      // Cut from mid-sentence, so it starts lower case; here it starts a line.
      const reason = refusalReason(task.result);
      return reason.charAt(0).toUpperCase() + reason.slice(1);
    }
    default:
      return '';
  }
}

export function describeMember(
  member: SubagentRunMember,
  telemetry: SubagentTelemetry | undefined,
  { pausing, followed = true, root, t }: {
    pausing: boolean;
    followed?: boolean;
    /** The computer's root folder, so a path under it reads as relative. */
    root?: string | null;
    t: TFn;
  },
): SubagentRowView | null {
  const { task } = member;
  if (!task) return null;
  const kind = subagentRowKind(task, telemetry, pausing, followed);
  // A resume starts the task's next run, so it goes on while that run does;
  // where runs are not followed, while its record says so, as a launch does.
  const live = SUBAGENT_ROW_UI[kind].live
    || (kind === 'resumed' && (!!telemetry?.running || (!followed && task.status === 'running')));
  // An update only speaks to a task, and a refused launch never reached one:
  // neither has a run of its own to time. Drawn from the task's newest run, an
  // update would time a run it did not start.
  const ran = isOwnRun(kind);
  // The ledger's start when the server sent one; while live, the launch call's
  // arrival stands in, which is close and never later than the truth.
  const startMs = ran
    ? telemetry?.startedAtMs ?? (live ? launchedAt(member.toolCallProcess) : undefined)
    : undefined;
  const model = ran ? telemetry?.model : undefined;
  return {
    id: member.subagentId,
    type: task.type,
    kind,
    rawStatus: task.status,
    title: summarize(task.description) || t('chat.subagentCard.titleFallback'),
    fullTitle: task.description,
    detail: memberDetail(kind, task, telemetry, root, t),
    startMs,
    endMs: ran && !live ? telemetry?.finishedAtMs : undefined,
    live,
    model: model && llmProviderArt(model.provider) ? model : undefined,
    // A launch that never ran has nothing to open: opening it would mint a tab
    // and a sidebar row for a subagent that does not exist. A refused
    // follow-up still names the task it was aimed at, which does.
    openInfo: kind === 'refused' && !task.resumeTargetId
      ? undefined
      : {
          subagentId: task.resumeTargetId || member.subagentId,
          description: task.description,
          type: task.type,
          status: task.status,
        },
  };
}

function isOwnRun(kind: SubagentRowKind): boolean {
  return kind !== 'refused' && kind !== 'updated';
}

/** Whether any member is a run of its own, so the group can be said to have
 *  finished: a group of updates only passed words on. */
export function hasOwnRun(views: readonly SubagentRowView[]): boolean {
  return views.some((v) => isOwnRun(v.kind));
}

/** How long a member has run, or ran; undefined when that is not known. */
export function memberElapsed(view: SubagentRowView, now: number): number | undefined {
  if (view.startMs == null) return undefined;
  if (view.live) return Math.max(0, now - view.startMs);
  return view.endMs != null ? Math.max(0, view.endMs - view.startMs) : undefined;
}

/** How long the run has gone on while live; once settled, how long it took,
 *  if every member that started is known to have finished. */
export function runElapsed(views: readonly SubagentRowView[], now: number): number | undefined {
  const timed = views.filter((v) => v.startMs != null);
  if (timed.length === 0) return undefined;
  const t0 = Math.min(...timed.map((v) => v.startMs!));
  if (views.some((v) => v.live)) return Math.max(0, now - t0);
  if (timed.some((v) => v.endMs == null)) return undefined;
  return Math.max(0, Math.max(...timed.map((v) => v.endMs!)) - t0);
}

/**
 * A group's tally, one word per entry, in this order. `wrong` is what went
 * wrong: a settled group keeps only those words, after how long it took, and
 * a narrow header cuts the rest of its line first.
 */
const TALLY: ReadonlyArray<{ key: string; kinds: readonly SubagentRowKind[]; wrong?: boolean }> = [
  { key: 'chat.subagentRun.tally.working', kinds: ['starting', 'working', 'pausing'] },
  { key: 'chat.subagentRun.tally.done', kinds: ['done'] },
  { key: 'chat.subagentRun.tally.failed', kinds: ['failed', 'refused'], wrong: true },
  { key: 'chat.subagentRun.tally.stopped', kinds: ['stopped'], wrong: true },
  { key: 'chat.subagentRun.tally.updated', kinds: ['updated'] },
  { key: 'chat.subagentRun.tally.resumed', kinds: ['resumed'] },
];

/** A group header's line: the spinner while any member works, the tally, and
 *  the clock while it ticks. */
export function groupSummary(
  views: readonly SubagentRowView[],
  now: number,
  t: TFn,
): { lead?: RunGlyphKind; parts: RunGroupPart[]; clock?: string } {
  const live = views.some((v) => v.live);
  const elapsed = runElapsed(views, now);
  const finished = !live && hasOwnRun(views);
  const parts: RunGroupPart[] = [];
  if (finished) {
    parts.push({
      text: elapsed != null
        ? t('chat.subagentRun.finishedIn', { duration: formatTook(elapsed, t) })
        : t('chat.subagentRun.finished'),
    });
  }
  for (const { key, kinds, wrong } of TALLY) {
    if (finished && !wrong) continue;
    const count = views.filter((v) => kinds.includes(v.kind)).length;
    if (count === 0) continue;
    parts.push({
      text: t(key, { count }),
      danger: kinds.some((k) => SUBAGENT_ROW_UI[k].danger),
      keep: wrong,
    });
  }
  return {
    lead: live ? 'live' : undefined,
    parts,
    clock: live && elapsed != null ? formatClock(elapsed) : undefined,
  };
}

