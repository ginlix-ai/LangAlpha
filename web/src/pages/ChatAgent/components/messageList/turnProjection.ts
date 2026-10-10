/**
 * One RAW projection pass over the transcript, producing the turn semantics
 * every consumer needs (edit, regenerate, feedback, the regenerate tail).
 *
 * The projection MUST run over the raw array: a bubble without a stamped turn
 * takes its turn from the positional count of assistant bubbles before it, so
 * hidden bubbles still occupy their turn. Visibility filtering happens
 * afterwards, and only the tail scan (a render affordance) reads the filtered
 * list. Visibility is judged on rendered content, so it lives with that in
 * `contentProjection.ts`: the stream code reads turns from here and must not
 * load the render stack to do it.
 */
import { isSteeringContinuation, isSteeringUserMessage, type MessageLike } from './messagePredicates';
import type { MessageRecord } from './types';

/** The fields the projection reads, structural for the same reason as `MessageLike`. */
export interface TurnLike extends MessageLike {
  id?: unknown;
  turnIndex?: unknown;
}

export interface ProjectedMessage<M extends TurnLike = MessageRecord> {
  message: M;
  /** Index into the RAW messages array. */
  rawIndex: number;
  /** Backend turn this bubble belongs to. */
  turnIndex: number;
}

function stampedTurn(message: TurnLike): number | null {
  return typeof message.turnIndex === 'number' ? message.turnIndex : null;
}

/**
 * Assign every bubble its backend turn. A bubble stamped with `turnIndex` (the
 * chat view stamps every bubble it creates) keeps it; the rest count, with
 * `c` = the turn the next non-steering assistant bubble opens:
 *
 *   user message              → `c`      (the turn it initiates)
 *   non-steering assistant    → `c`, then `c++`
 *   steering continuation     → `c - 1`  (folds into the turn it continues)
 *
 * A stamp also re-seats the count, so an unstamped bubble after it continues
 * from the stamped turn rather than from zero: a paged transcript opens on
 * turn 75, and counting from its first bubble would address turn 0. Without
 * stamps (the shared view, a subagent's transcript) this is the positional
 * rule both historical derivations agree on: the edit path counted
 * non-steering assistants BEFORE a user message (exclusive) and the
 * feedback/regenerate paths counted up to AND INCLUDING an assistant bubble
 * minus one.
 */
export function projectTurns<M extends TurnLike>(messages: readonly M[]): ProjectedMessage<M>[] {
  const projected = new Array<ProjectedMessage<M>>(messages.length);
  let c = 0;
  for (let i = 0; i < messages.length; i++) {
    const message = messages[i];
    const stamp = stampedTurn(message);
    let turnIndex: number;
    if (isSteeringContinuation(message)) {
      turnIndex = stamp ?? c - 1;
    } else if (message.role === 'assistant') {
      turnIndex = stamp ?? c;
      c = turnIndex + 1;
    } else if (stamp !== null && message.role === 'user') {
      // A steering bubble sits inside the turn it steered, so it names its
      // turn without opening one.
      turnIndex = stamp;
      if (!isSteeringUserMessage(message)) c = stamp;
    } else {
      // User bubbles initiate turn `c`; notifications carry no turn of their
      // own and ride the turn they were inserted into.
      turnIndex = c;
    }
    projected[i] = { message, rawIndex: i, turnIndex };
  }
  return projected;
}

/**
 * The newest turn the transcript shows, or null when it shows none. Read from
 * the assistant bubbles alone: a notification rides the count of the turn
 * after it, and a parked message has not opened one yet.
 */
export function newestTurn(messages: readonly TurnLike[]): number | null {
  let newest: number | null = null;
  for (const { message, turnIndex } of projectTurns(messages)) {
    if (message.role === 'assistant' && (newest === null || turnIndex > newest)) newest = turnIndex;
  }
  return newest;
}

/**
 * The turn the transcript projects for the bubble with this id, or undefined
 * when there is no such bubble or it carries no turn of its own (a
 * notification). A bubble that continues another must read its turn here, not
 * off the bubble: a report-back's bubble has no stamp, only a position.
 */
export function turnOf(messages: readonly TurnLike[], id: string): number | undefined {
  const entry = projectTurns(messages).find((p) => p.message.id === id);
  return entry && entry.message.role !== 'notification' ? entry.turnIndex : undefined;
}

/**
 * Per-bubble "is this the LAST bubble of its backend turn", over the VISIBLE
 * list. A turn can render as several assistant bubbles (a steering
 * continuation, the bubble a reconnect after a dropped stream continues on)
 * but has one regenerate target, so only the turn's last bubble offers the
 * affordance. Read from the projected turn, not the steering flag: a reconnect
 * bubble is no steering continuation and still shares its turn. Computed over
 * the visible list on purpose: a continuation that settled empty is never
 * painted, so it must not steal regenerate from the bubble the user can
 * actually see.
 */
export function computeTurnTails(visible: readonly ProjectedMessage<TurnLike>[]): boolean[] {
  const tail = new Array<boolean>(visible.length).fill(false);
  const endedTurns = new Set<number>();
  for (let i = visible.length - 1; i >= 0; i--) {
    const { message, turnIndex } = visible[i];
    if (message.role === 'assistant') {
      tail[i] = !endedTurns.has(turnIndex);
      endedTurns.add(turnIndex);
    }
  }
  return tail;
}
