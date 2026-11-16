/**
 * The quiet checks' fold: wake turns that ended quietly and changed no
 * wakeup are drawn as one row per run of them, in place of the turns. A turn
 * still running shows as any turn does until it ends, and then joins the run
 * above it. A quiet turn that changed a wakeup stays a turn, closed by its
 * quiet line. Read from each turn's own tool calls, the same live and on
 * replay.
 */
import type { WakeupArrival } from '@/types/chat';
import type { WakeNotice } from '@/types/wakeups';
import { quietTurnEnd, type QuietEnd } from '../../session/wakeupEvents';
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
 * One turn's quiet end, kept per bubble list so it keeps one identity while
 * the list recomputes on every streamed frame. Keyed on the turn's last
 * bubble, whose record is replaced whenever it changes; the others are
 * compared too, since an earlier one can be replaced alone.
 */
const cache = new WeakMap<MessageRecord, { bubbles: MessageRecord[]; end: QuietEnd | null }>();

function cachedQuietEnd(bubbles: MessageRecord[]): QuietEnd | null {
  const tail = bubbles[bubbles.length - 1];
  const hit = cache.get(tail);
  if (hit && hit.bubbles.length === bubbles.length && hit.bubbles.every((b, k) => b === bubbles[k])) return hit.end;
  const end = quietTurnEnd(bubbles);
  cache.set(tail, { bubbles, end });
  return end;
}

/**
 * How each settled turn ended quietly, by turn. `turned` is the raw
 * projection, whose bubbles a turn's calls are read from, hidden ones
 * included; `live` the turns still working, which have not ended yet.
 */
export function quietEndsByTurn(turned: readonly ProjectedMessage[], live: ReadonlySet<number>): Map<number, QuietEnd> {
  const bubbles = new Map<number, MessageRecord[]>();
  for (const { message, turnIndex } of turned) {
    if ((message.role as string) !== 'assistant' || turnIndex === null || live.has(turnIndex)) continue;
    const list = bubbles.get(turnIndex);
    if (list) list.push(message);
    else bubbles.set(turnIndex, [message]);
  }
  const ends = new Map<number, QuietEnd>();
  for (const [turn, list] of bubbles) {
    const end = cachedQuietEnd(list);
    if (end) ends.set(turn, end);
  }
  return ends;
}

/** The list's entries with each run of back-to-back quiet wake turns that
 *  changed no wakeup drawn as one fold. */
export function planQuietChecks(
  visible: readonly ProjectedMessage[],
  quietEnds: ReadonlyMap<number, QuietEnd>,
): QuietPlanItem[] {
  const items: QuietPlanItem[] = [];
  let run: QuietCheck[] | null = null;
  for (let i = 0; i < visible.length; i++) {
    const { message } = visible[i];
    const arrival = arrivalOf(message);
    const turn = arrival?.source === 'wake' ? arrival.turnIndex : undefined;
    const end = turn === undefined ? undefined : quietEnds.get(turn);
    const span = end && !end.changed && turn !== undefined ? quietSpan(visible, i, turn) : null;
    if (!arrival || turn === undefined || !end || !span) {
      run = null;
      items.push({ kind: 'entry', index: i });
      continue;
    }
    const check: QuietCheck = {
      turnIndex: turn,
      startedAt: messageTimeMs(message.timestamp),
      notices: [...arrival.notices, ...span.notices],
      status: end.status,
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
