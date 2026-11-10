/**
 * A jump to one turn of a thread (the command palette's "In conversations"
 * hit, `LocationState.focusTurn`): what the view does next to get there, from
 * the transcript it holds and what the top of it can do.
 */
import type { OlderHistoryStatus } from '../../session/history/historyWindow';
import { opensTurn } from '../messageList/messagePredicates';

/**
 * `reveal` the bubble named; `load` the next older page toward the target (a
 * far one takes several); `wait` while a page or a reload is on its way, or a
 * fork holds the top; `failed` when the last page did not arrive, which the
 * jump asks for once more; `missing` when the thread does not have the turn
 * (a fork cut it, or the index is past the newest turn), so the reader is
 * left at the newest turn.
 */
export type FocusTurnStep =
  | { kind: 'reveal'; id: string }
  | { kind: 'load' }
  | { kind: 'wait' }
  | { kind: 'failed' }
  | { kind: 'missing' };

interface FocusMessage {
  id?: unknown;
  role?: unknown;
  turnIndex?: unknown;
  steering?: unknown;
  steeringDelivered?: unknown;
  queueError?: unknown;
}

const turnOf = (m: FocusMessage): number | null => (typeof m.turnIndex === 'number' ? m.turnIndex : null);

/**
 * The bubble turn `turn` opens with: its opening message, or, for a turn
 * that opens with nothing drawn as a bubble (a wakeup's row, a hidden
 * opener), the first bubble it drew. A notification draws no bubble a jump
 * can land on.
 */
export function turnOpenerId(messages: readonly FocusMessage[], turn: number): string | null {
  let first: string | null = null;
  for (const m of messages) {
    if (turnOf(m) !== turn || typeof m.id !== 'string' || m.role === 'notification') continue;
    if (opensTurn(m)) return m.id;
    first ??= m.id;
  }
  return first;
}

export function focusTurnStep(
  messages: readonly FocusMessage[],
  turn: number,
  status: OlderHistoryStatus,
): FocusTurnStep {
  const id = turnOpenerId(messages, turn);
  if (id) return { kind: 'reveal', id };
  // The transcript runs from its oldest loaded turn to the newest, so a turn
  // at or after the oldest one loaded and not in it is not in the thread.
  let oldest: number | null = null;
  for (const m of messages) {
    const t = turnOf(m);
    if (t !== null && (oldest === null || t < oldest)) oldest = t;
  }
  if (oldest !== null && oldest <= turn) return { kind: 'missing' };
  switch (status) {
    case 'idle':
      return { kind: 'load' };
    case 'loading':
    case 'reloading':
    case 'held':
      return { kind: 'wait' };
    case 'failed':
      return { kind: 'failed' };
    case 'end':
      // The start of the thread.
      return { kind: 'missing' };
  }
}
