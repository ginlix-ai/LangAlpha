/**
 * The answer board for one turn's HITL interrupts.
 *
 * Every decision is stored here once, and everything else is derived from it:
 * the card's status and reason, whether the batch the server demands is
 * complete, the `hitl_response` map the resume sends, and what goes back on
 * screen when that resume is refused. Splitting those across a card status, a
 * slot table and a collected-answers map is what let a card read "Approved"
 * while the board it was answered on had already been cleared.
 */

import type { MutableRefObject } from 'react';
import type { HitlDecisionBody, HitlResponseBody, HitlResumeEntry } from '@/types/api';
import type {
  AssistantMessage, CreditPauseStatus, ToolApprovalPosition, ToolApprovalState,
} from '@/types/chat';
import type { MessageRecord, SetMessages } from '../types';
import { setCardFields, setCardStatus } from './buckets';
import type { DecisionTarget } from './toolApprovalCard';

/** One answered call: which stopped call it answers, and the verdict. */
export interface ToolApprovalSlot {
  target: DecisionTarget;
  decision: HitlDecisionBody;
}

/** One click on an approval card, before the board derives anything from it. */
export interface ApprovalClick {
  approved: boolean;
  /** The reject reason the user typed, if any. */
  message?: string;
}

/**
 * The answer for one interrupt, built from its filled slots.
 *
 * Both halves always travel. `decisions` stays positional so an interrupt the
 * server reads the old way is answered the old way, and `order_decisions`
 * names every keyed attempt, because absence there is a refusal rather than an
 * approval: a keyed card whose id never reached the map would have its order
 * declined with the user's Approve still on screen.
 */
export function buildToolApprovalResumeEntry(
  slots: Array<ToolApprovalSlot | undefined>,
): HitlResumeEntry {
  const decisions = slots.map((slot) => slot?.decision ?? { type: 'reject' });
  const keyed: Record<string, HitlDecisionBody> = {};
  for (const slot of slots) {
    if (slot?.target.kind === 'attempt') keyed[slot.target.attemptId] = slot.decision;
  }
  return Object.keys(keyed).length > 0 ? { decisions, order_decisions: keyed } : { decisions };
}

/**
 * Put every approval card raised by `interruptIds` back on `pending`, wherever
 * it lives. A deduped re-raise can move the card to a bubble other than the one
 * the answer came from, so every assistant message is searched.
 */
export function restoreToolApprovalsPending(
  messages: MessageRecord[],
  interruptIds: Set<string>,
): MessageRecord[] {
  return messages.map((m) => {
    if (m.role !== 'assistant') return m;
    const msg = m as AssistantMessage;
    const cards = msg.toolApprovals;
    if (!cards) return m;
    let changed = false;
    const next: Record<string, ToolApprovalState> = {};
    for (const [id, card] of Object.entries(cards)) {
      const stranded = card.status !== 'pending' && !!card.interruptId && interruptIds.has(card.interruptId);
      if (stranded) changed = true;
      next[id] = stranded ? { ...card, status: 'pending' } : card;
    }
    return changed ? { ...msg, toolApprovals: next } : m;
  });
}

export interface AnswerBoardDeps {
  /** Interrupt ids this turn still owes an answer. Shared with the projections
   *  that arm them, which is why it stays a ref rather than board-local state. */
  pendingIds: MutableRefObject<Set<string>>;
  /** Bumped on every new run and every thread switch, so the resume settler can
   *  tell whether the board it snapshotted is still the board on screen. */
  sessionEpoch: MutableRefObject<number>;
  setMessages: SetMessages;
}

export interface AnswerBoard {
  /** Drop every answer and every armed id (thread switch, or a resume going out). */
  clear(): void;
  /** Re-arm the board from the interrupts a replay left unanswered. */
  arm(interruptIds: Array<string | undefined>): void;
  /** Record one interrupt's answer. Returns the whole `hitl_response` map once
   *  every armed interrupt has one, and null while the batch is short. */
  answer(interruptId: string, entry: HitlResumeEntry): HitlResponseBody | null;
  /** Record one stopped call's verdict and settle its card. Returns its
   *  interrupt's entry once every call the interrupt raised is answered. */
  decideToolCall(
    approvalId: string,
    interruptId: string,
    position: ToolApprovalPosition,
    click: ApprovalClick,
    attemptId?: string,
  ): HitlResumeEntry | null;
  /** Move the pause card to `resuming` and remember which pause the resume
   *  answers, so the settler can move it either way. */
  armCreditPause(pauseId: string): void;
  /** Put the pause card back on `pending`, KEEPING the pause reference.
   *
   *  The batched case, and the reason it is not a settle: another interrupt in
   *  this turn is still unanswered, so nothing was dispatched and no stream
   *  will move this card. The click still stands, so the pause this resume
   *  answers has not changed. Clearing the reference here is what strands the
   *  card: the batch dispatches later, and the real settler then finds no pause
   *  to move and leaves a Resume button on a turn that is already running. */
  restoreCreditPause(): void;
  /** Snapshot one resume attempt and return its settler.
   *
   *  The click settles cards optimistically and the dispatch clears the board,
   *  both assuming admission. A refusal opens no run, so the settler puts the
   *  snapshot and the cards back; otherwise the interrupt is unanswerable, with
   *  a card reading "Approved" and an empty board dropping every later click. */
  beginResume(interruptIds: string[]): (admitted: boolean) => void;
}

/** The card fields and the wire body one click means. */
function decisionOf(click: ApprovalClick): {
  status: 'approved' | 'rejected';
  reason: string | null;
  body: HitlDecisionBody;
} {
  if (click.approved) return { status: 'approved', reason: null, body: { type: 'approve' } };
  const reason = click.message?.trim() || null;
  return {
    status: 'rejected',
    reason,
    body: reason ? { type: 'reject', message: reason } : { type: 'reject' },
  };
}

export function createAnswerBoard(deps: AnswerBoardDeps): AnswerBoard {
  let answers: HitlResponseBody = {};
  let partials: Record<string, Array<ToolApprovalSlot | undefined>> = {};
  let creditPauseId: string | null = null;

  /** Move the in-flight pause card, once. The one unacceptable outcome is a
   *  card left on `resuming` with no way left to answer the pause, so every
   *  exit from a resume comes through here. */
  const settleCreditPause = (status: CreditPauseStatus) => {
    const pauseId = creditPauseId;
    if (!pauseId) return;
    creditPauseId = null;
    deps.setMessages((prev) => setCardStatus(prev, 'creditPauses', pauseId, status));
  };

  const clear = () => {
    deps.pendingIds.current.clear();
    answers = {};
    partials = {};
  };

  return {
    clear,

    arm(interruptIds) {
      clear();
      for (const id of interruptIds) if (id) deps.pendingIds.current.add(id);
    },

    answer(interruptId, entry) {
      answers[interruptId] = entry;
      const pending = deps.pendingIds.current;
      // A turn can hold several interrupts and the server takes one resume for
      // all of them, so one answer is often not enough.
      if (pending.size === 0 || ![...pending].every((id) => answers[id])) return null;
      return { ...answers };
    },

    decideToolCall(approvalId, interruptId, position, click, attemptId) {
      // The slot comes from the card that was clicked, not from reading it back
      // out of a setMessages updater: React is free to defer that updater, and
      // the decision would then be filed against slot 0 of a one-wide batch,
      // which either sends too few decisions or overwrites another card's answer.
      const width = Math.max(1, position.count);
      const index = Math.min(Math.max(0, position.index), width - 1);
      const { status, reason, body } = decisionOf(click);
      deps.setMessages((prev) => setCardFields(prev, 'toolApprovals', approvalId, { status, reason }));

      const slots = partials[interruptId] || new Array(width).fill(undefined);
      slots[index] = {
        target: attemptId ? { kind: 'attempt', attemptId, index } : { kind: 'position', index },
        decision: body,
      };
      partials[interruptId] = slots;
      // The resume answers every call the interrupt raised, in the order it
      // raised them, so it waits here until the last card in the batch is in.
      if (slots.length < width || slots.some((slot) => slot === undefined)) return null;
      return buildToolApprovalResumeEntry(slots);
    },

    armCreditPause(pauseId) {
      creditPauseId = pauseId;
      deps.setMessages((prev) => setCardStatus(prev, 'creditPauses', pauseId, 'resuming'));
    },

    restoreCreditPause() {
      const pauseId = creditPauseId;
      if (!pauseId) return;
      deps.setMessages((prev) => setCardStatus(prev, 'creditPauses', pauseId, 'pending'));
    },

    beginResume(interruptIds) {
      const ids = new Set(interruptIds);
      const priorPending = new Set(deps.pendingIds.current);
      const priorAnswers = { ...answers };
      // The board belongs to the thread that armed it. A thread switch clears
      // the board and bumps the epoch, but this settler runs later still, from
      // the aborted stream's `finally` -- so unfenced, a refused resume on
      // thread A restores A's pending ids on top of thread B. Nothing on B will
      // ever collect them, and B's own interrupt then fails the batch gate for
      // good. Refusal plus navigation is the ordinary path here, not an exotic
      // one: answering a credit pause means leaving to buy credits.
      const epoch = deps.sessionEpoch.current;
      return (admitted: boolean) => {
        if (deps.sessionEpoch.current !== epoch) return;
        if (!admitted) {
          deps.pendingIds.current = priorPending;
          answers = priorAnswers;
          // Dropped, not restored: the retried click has to fill a fresh set
          // rather than a half-filled one left over from the refused attempt.
          for (const id of ids) delete partials[id];
          deps.setMessages((prev) => restoreToolApprovalsPending(prev, ids));
        }
        settleCreditPause(admitted ? 'resumed' : 'pending');
      };
    },
  };
}
