/**
 * What a live resume turn recorded about the interrupts it answered.
 *
 * The forward resolvers in replayHistory settle a card from the evidence that
 * follows it. This is the same evidence kept as a claim, for the interrupt that
 * has not replayed yet.
 *
 * Checkpoint replay appends the thread's tip interrupt once, at the very end,
 * with no turn_index to place it. A turn's own ending interrupts ride that turn
 * instead — but a turn only gets them once the NEXT boundary is committed, and
 * a resume commits its boundary only when the graph consumes the Command. In
 * the window between the resume being requested and the graph taking it, the
 * interrupt therefore exists only as the tip, and replays after the resume turn
 * that answered it.
 *
 * That window is the whole scope of a claim, which is why a claim is recorded
 * only for a resume turn that is still running: once the turn is terminal, a
 * tip interrupt trailing it is a genuine re-raise, or one a failed resume never
 * consumed. Both are still owed an answer, and both keep their card's controls.
 */

import type { SSEEvent } from '../types';

/** One live resume turn's record of one interrupt it answered. */
export interface HistoryInterruptClaim {
  /** ``hitl_answers[id]``. The server records only two of the four decision
   *  shapes: an approve that carried a message (the message), and a reject that
   *  carried none (null). A bare approve and a reject that carried a message are
   *  both absent, which is why absence alone never decides an outcome. */
  answer?: string | null;
  /** Every reject message in the resume, joined. Attributable to this interrupt
   *  only when the resume answered it alone. */
  content: string;
  /** Whether the resume answered more than this one interrupt, which is what
   *  makes its shared `content` unattributable. */
  batched: boolean;
}

/**
 * Record what one replayed `user_message` answered, if it is still running.
 *
 * A terminal turn carries its run id and a live one does not, so `run_id` is
 * the gate: only a live resume can leave the interrupt it answered trailing
 * behind it.
 */
export function recordInterruptClaims(
  claims: Map<string, HistoryInterruptClaim>,
  event: SSEEvent,
): void {
  if (event.run_id) return;
  const claimedIds = event.metadata?.hitl_interrupt_ids;
  if (!Array.isArray(claimedIds)) return;
  const ids = claimedIds.filter((id): id is string => typeof id === 'string');
  const raw = event.metadata?.hitl_answers;
  const answers =
    raw && typeof raw === 'object' && !Array.isArray(raw)
      ? (raw as Record<string, unknown>)
      : undefined;
  const content = typeof event.content === 'string' ? event.content : '';
  for (const interruptId of ids) {
    const value = answers && interruptId in answers ? answers[interruptId] : undefined;
    claims.set(interruptId, {
      // Anything that is neither a string nor null is treated as unrecorded
      // rather than trusted into a card that gates spend and deletion.
      ...(value === null || typeof value === 'string' ? { answer: value } : {}),
      content,
      batched: ids.length > 1,
    });
  }
}

/** What a claim proves about its interrupt's outcome. */
type ClaimVerdict = 'approved' | 'rejected' | 'unknown';

/**
 * Read one interrupt's outcome out of its claim.
 *
 * Only two of the four decision shapes reach `hitl_answers`, so the other two
 * are told apart by the resume's content, which carries every reject message
 * and nothing else: an absent entry with no content is the bare approve. A
 * batched resume shares one content across its interrupts, so there the same
 * absence is unattributable and stays unknown rather than being guessed.
 */
function claimVerdict(claim: HistoryInterruptClaim): ClaimVerdict {
  if (claim.answer === null) return 'rejected';
  if (typeof claim.answer === 'string') return 'approved';
  if (!claim.content.trim()) return 'approved';
  return claim.batched ? 'unknown' : 'rejected';
}

/**
 * The settled card fields for an interrupt a claim already answered, or null to
 * leave it pending — which is what an unproven outcome, and a pause that was
 * declined rather than resumed, both get.
 *
 * The one thing a claim cannot recover is a proposal's result payload (a
 * dispatched ptc_agent's thread/workspace ids), which lives in the tool result;
 * the card reads approved without them until that result arrives.
 */
export function claimedCardFields(
  type: string,
  claim: HistoryInterruptClaim,
): Record<string, unknown> | null {
  const verdict = claimVerdict(claim);
  if (verdict === 'unknown') return null;
  if (type === 'credit_pause') {
    return verdict === 'approved' ? { status: 'resumed' } : null;
  }
  if (type === 'ask_user_question') {
    return verdict === 'rejected'
      ? { status: 'skipped' }
      : { status: 'answered', answer: typeof claim.answer === 'string' ? claim.answer : null };
  }
  // plan_approval and every proposal family share the one pair of names.
  return { status: verdict };
}
