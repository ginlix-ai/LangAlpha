/**
 * The quiet checks' fold: wake turns that ended quietly and changed no
 * wakeup are drawn as one row per run of them, in place of the turns. A turn
 * still running shows as any turn does until it ends, and then joins the run
 * above it. Read from each turn's own tool calls, the same live and on replay.
 */
import type { WakeupArrival } from '@/types/chat';
import type { WakeNotice } from '@/types/wakeups';
import { quietTurnStatus } from '../../session/wakeupEvents';
import { messageTimeMs, type ProjectedMessage } from './turnProjection';
import type { MessageRecord } from './types';

/** One wake turn that ended quietly, as its fold lists it. */
export interface QuietCheck {
  turnIndex: number;
  /** Epoch ms the turn started, when known. */
  startedAt?: number;
  notices: WakeNotice[];
  /** The agent's one line on what it checked. */
  status: string;
}

/** What the list draws, in order: an entry of the visible list, by its
 *  index there, or the fold standing for a run of quiet checks. */
export type QuietPlanItem =
  | { kind: 'entry'; index: number }
  | { kind: 'quiet'; checks: QuietCheck[] };

function arrivalOf(message: MessageRecord): WakeupArrival | undefined {
  return (message.role as string) === 'notification' ? message.wakeup as WakeupArrival | undefined : undefined;
}

/**
 * The rows a wake turn spans from its opener at `start`: its replies, and
 * the rows of the notices it took in while running, through the last of
 * those after its last reply. Null when a row of any other kind sits among
 * its replies (a steer, a task's report, a divider): folding the turn would
 * hide that row, and folding around it would leave the turn half shown.
 */
function quietSpan(
  visible: readonly ProjectedMessage[],
  start: number,
  turn: number,
): { end: number; notices: WakeNotice[] } | null {
  let last = start;
  for (let k = start + 1; k < visible.length; k++) {
    const { message, turnIndex } = visible[k];
    if ((message.role as string) !== 'assistant') continue;
    if (turnIndex !== turn) break;
    last = k;
  }
  const notices: WakeNotice[] = [];
  for (let k = start + 1; k <= last; k++) {
    const { message, turnIndex } = visible[k];
    if ((message.role as string) === 'assistant' && turnIndex === turn) continue;
    const arrival = arrivalOf(message);
    if (arrival?.source !== 'delivered') return null;
    notices.push(...arrival.notices);
  }
  let end = last + 1;
  for (; end < visible.length; end++) {
    const arrival = arrivalOf(visible[end].message);
    if (arrival?.source !== 'delivered') break;
    notices.push(...arrival.notices);
  }
  return { end, notices };
}

/**
 * The list's entries with each run of back-to-back quiet wake turns drawn as
 * one fold. `turned` is the raw projection, whose bubbles a turn's calls are
 * read from, hidden ones included; `live` the turns still working.
 */
export function planQuietChecks(
  visible: readonly ProjectedMessage[],
  turned: readonly ProjectedMessage[],
  live: ReadonlySet<number>,
  isSubagentView: boolean | undefined,
): QuietPlanItem[] {
  const items: QuietPlanItem[] = [];
  if (isSubagentView) {
    for (let i = 0; i < visible.length; i++) items.push({ kind: 'entry', index: i });
    return items;
  }
  const bubbles = new Map<number, MessageRecord[]>();
  for (const { message, turnIndex } of turned) {
    if ((message.role as string) !== 'assistant' || turnIndex === null) continue;
    const list = bubbles.get(turnIndex);
    if (list) list.push(message);
    else bubbles.set(turnIndex, [message]);
  }

  let run: QuietCheck[] | null = null;
  for (let i = 0; i < visible.length; i++) {
    const { message } = visible[i];
    const arrival = arrivalOf(message);
    const turn = arrival?.source === 'wake' ? arrival.turnIndex : undefined;
    const list = turn === undefined || live.has(turn) ? undefined : bubbles.get(turn);
    const status = list ? quietTurnStatus(list) : null;
    const span = status !== null && turn !== undefined ? quietSpan(visible, i, turn) : null;
    if (!arrival || turn === undefined || status === null || !span) {
      run = null;
      items.push({ kind: 'entry', index: i });
      continue;
    }
    const check: QuietCheck = {
      turnIndex: turn,
      startedAt: messageTimeMs(message.timestamp),
      notices: [...arrival.notices, ...span.notices],
      status,
    };
    if (run) {
      run.push(check);
    } else {
      run = [check];
      items.push({ kind: 'quiet', checks: run });
    }
    i = span.end - 1;
  }
  return items;
}
