/**
 * The pause a view shows: the one interrupt its composer and cards read, the
 * run that raised it, and the ids the answer board waits on. Armed and cleared
 * only here, so they never drift apart: a pause one of them still held after
 * another let it go is what left a composer disabled on the next thread, or a
 * batch gate failing on an id nothing would answer.
 */
import type { PendingInterrupt } from '../types';
import type { AnswerBoard } from './answerBoard';

export interface PauseSlot {
  /** The pause shown now, with the run that raised it; null when none. */
  current(): PendingInterrupt | null;
  /** Show `pending` and have the board wait on `interruptIds` beside any it
   *  already waits on, as parallel interrupts arrive one frame each. */
  arm(pending: PendingInterrupt, interruptIds: Array<string | undefined>): void;
  /** No pause: the slot empties and the board drops every answer and id. */
  clear(): void;
  /** The view attaches to `runId`. Another run of the thread means the pause
   *  shown was answered elsewhere (by its sender in a shared workspace, or in
   *  another tab), and that run re-raises any pause it reaches. The paused run
   *  itself coming back keeps the question: nothing would raise it again. */
  attach(runId: string): void;
}

export function createPauseSlot(show: (pending: PendingInterrupt | null) => void, board: AnswerBoard): PauseSlot {
  let shown: PendingInterrupt | null = null;
  const clear = () => {
    shown = null;
    show(null);
    board.clear();
  };
  return {
    current: () => shown,
    arm(pending, interruptIds) {
      board.track(interruptIds);
      shown = pending;
      show(pending);
    },
    clear,
    attach(runId) {
      if (runId !== shown?.runId) clear();
    },
  };
}
