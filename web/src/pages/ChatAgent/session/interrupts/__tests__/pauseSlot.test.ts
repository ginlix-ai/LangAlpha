/**
 * The pause a view shows is set and let go in one place each, so the slot
 * the composer reads, the run that raised it and the ids the answer board
 * waits on move together.
 */
import { describe, expect, it, vi } from 'vitest';
import type { ChatMessage } from '@/types/chat';
import { createAnswerBoard } from '../answerBoard';
import { createPauseSlot } from '../pauseSlot';

function build() {
  const ids = { current: new Set<string>() };
  const show = vi.fn();
  const setMessages = vi.fn() as unknown as React.Dispatch<React.SetStateAction<ChatMessage[]>>;
  const board = createAnswerBoard({ pendingIds: ids, sessionEpoch: { current: 0 }, setMessages });
  return { slot: createPauseSlot(show, board), board, ids, show };
}

describe('createPauseSlot', () => {
  it('shows a pause with its run, and waits on each id beside those already armed', () => {
    const { slot, ids, show } = build();
    slot.arm({ type: 'tool_approval', interruptId: 'i-1', runId: 'run-1' }, ['i-1']);
    slot.arm({ type: 'tool_approval', interruptId: 'i-2', runId: 'run-1' }, ['i-2', undefined]);
    expect([...ids.current]).toEqual(['i-1', 'i-2']);
    expect(show).toHaveBeenLastCalledWith({ type: 'tool_approval', interruptId: 'i-2', runId: 'run-1' });
    expect(slot.current()?.runId).toBe('run-1');
  });

  it('lets go of the slot, the run and every answer at once', () => {
    const { slot, board, ids, show } = build();
    slot.arm({ type: 'ask_user_question', interruptId: 'i-1', runId: 'run-1' }, ['i-1']);
    slot.clear();
    expect(slot.current()).toBeNull();
    expect(show).toHaveBeenLastCalledWith(null);
    expect(ids.current.size).toBe(0);
    // An answer to the old pause no longer completes a batch.
    expect(board.answer('i-1', { decisions: [] })).toBeNull();
  });

  it('keeps the pause when its own run comes back, and lets it go for another run', () => {
    const { slot } = build();
    slot.arm({ type: 'ask_user_question', interruptId: 'i-1', runId: 'run-1' }, ['i-1']);
    slot.attach('run-1');
    expect(slot.current()?.interruptId).toBe('i-1');
    slot.attach('run-2');
    expect(slot.current()).toBeNull();
  });
});
