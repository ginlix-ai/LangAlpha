/**
 * One RAW projection pass over the transcript, producing the turn semantics
 * every consumer needs (edit, regenerate, feedback, the regenerate tail).
 *
 * The projection MUST run over the raw array: the positional count of assistant
 * bubbles is what maps UI position → backend `turn_index`, so hidden bubbles
 * still occupy their turn. Visibility filtering happens afterwards, and only
 * the tail scan (a render affordance) reads the filtered list.
 */
import { isSteeringContinuation, isOrphanAssistantMessage } from './messagePredicates';
import type { MessageRecord } from './types';

export interface ProjectedMessage {
  message: MessageRecord;
  /** Index into the RAW messages array. */
  rawIndex: number;
  /** Backend turn this bubble belongs to. */
  turnIndex: number;
}

/**
 * Assign every bubble its backend turn, with `c` = running count of
 * non-steering assistant bubbles seen so far:
 *
 *   user message              → `c`      (the turn it initiates)
 *   non-steering assistant    → `c`, then `c++`
 *   steering continuation     → `c - 1`  (folds into the turn it continues)
 *
 * This is the single rule both historical derivations agree on: the edit path
 * counted non-steering assistants BEFORE a user message (exclusive) and the
 * feedback/regenerate paths counted up to AND INCLUDING an assistant bubble
 * minus one — identical everywhere except steering continuations, where the
 * fold-back above is what the inclusive form already produced.
 */
export function projectTurns(messages: MessageRecord[]): ProjectedMessage[] {
  const projected = new Array<ProjectedMessage>(messages.length);
  let c = 0;
  for (let i = 0; i < messages.length; i++) {
    const message = messages[i];
    let turnIndex: number;
    if (isSteeringContinuation(message)) {
      turnIndex = c - 1;
    } else if (message.role === 'assistant') {
      turnIndex = c;
      c++;
    } else {
      // User bubbles initiate turn `c`; notifications carry no turn of their
      // own and ride the turn they were inserted into.
      turnIndex = c;
    }
    projected[i] = { message, rawIndex: i, turnIndex };
  }
  return projected;
}

/** Drop bubbles the list must not paint (see `isOrphanAssistantMessage`). */
export function visibleProjection(projected: ProjectedMessage[]): ProjectedMessage[] {
  return projected.filter((p) => !isOrphanAssistantMessage(p.message));
}

/**
 * Per-bubble "is this the LAST bubble of its backend turn", over the VISIBLE
 * list. A steered turn renders as several assistant bubbles but has one
 * regenerate target, so only the turn's last bubble offers the affordance.
 * Computed over the visible list on purpose: a steering continuation that
 * settled empty is never painted, so it must not steal regenerate from the
 * bubble the user can actually see.
 */
export function computeTurnTails(visible: ProjectedMessage[]): boolean[] {
  const tail = new Array<boolean>(visible.length).fill(false);
  let nextAssistantIsSteering = false;
  for (let i = visible.length - 1; i >= 0; i--) {
    const message = visible[i].message;
    if (message.role === 'assistant') {
      tail[i] = !nextAssistantIsSteering;
      nextAssistantIsSteering = isSteeringContinuation(message);
    }
  }
  return tail;
}
