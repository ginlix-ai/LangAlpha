/**
 * The resume settler's fence, and the one board both halves of a resume roll
 * back. Restoring a refused resume's board is correct only on the thread that
 * armed it: the settler runs from the aborted stream's `finally`, which is
 * strictly after a thread switch has already cleared the board, so an unfenced
 * restore lands thread A's ids on thread B.
 */
import { describe, it, expect, vi } from 'vitest';
import { createAnswerBoard, type AnswerBoardDeps } from '../answerBoard';
import type { MessageRecord } from '../../types';

/** One bubble carrying a pause card and two approval cards from two interrupts. */
function bubble(): MessageRecord {
  return {
    id: 'a-1',
    role: 'assistant',
    creditPauses: { 'pause-1': { status: 'pending' } },
    toolApprovals: {
      'int-a#0': { status: 'approved', interruptId: 'int-a' },
      other: { status: 'approved', interruptId: 'int-b' },
    },
  } as unknown as MessageRecord;
}

function makeDeps() {
  let messages: MessageRecord[] = [bubble()];
  const setMessages = vi.fn((updater: (prev: MessageRecord[]) => MessageRecord[]) => {
    messages = updater(messages);
  });
  const deps = {
    pendingIds: { current: new Set(['int-a']) },
    sessionEpoch: { current: 1 },
    setMessages,
  } as unknown as AnswerBoardDeps & { setMessages: typeof setMessages };
  const cards = () =>
    messages[0] as unknown as {
      creditPauses: Record<string, { status: string }>;
      toolApprovals: Record<string, { status: string; reason?: string | null }>;
    };
  return { deps, cards };
}

describe('the answer board', () => {
  it('holds a resume back until every armed interrupt is answered', () => {
    const { deps } = makeDeps();
    deps.pendingIds.current = new Set(['int-a', 'int-b']);
    const board = createAnswerBoard(deps);

    expect(board.answer('int-a', { decisions: [{ type: 'approve' }] })).toBeNull();
    expect(board.answer('int-b', { decisions: [{ type: 'reject' }] })).toEqual({
      'int-a': { decisions: [{ type: 'approve' }] },
      'int-b': { decisions: [{ type: 'reject' }] },
    });
  });

  it('answers a batched interrupt only once every stopped call is decided', () => {
    const { deps, cards } = makeDeps();
    const board = createAnswerBoard(deps);

    expect(
      board.decideToolCall('int-a#0', 'int-a', { index: 1, count: 2 }, { approved: false, message: ' no ' }),
    ).toBeNull();
    // The card takes its status and its trimmed reason from the same click.
    expect(cards().toolApprovals['int-a#0']).toMatchObject({ status: 'rejected', reason: 'no' });

    expect(
      board.decideToolCall('other', 'int-a', { index: 0, count: 2 }, { approved: true }, 'attempt-1'),
    ).toEqual({
      decisions: [{ type: 'approve' }, { type: 'reject', message: 'no' }],
      order_decisions: { 'attempt-1': { type: 'approve' } },
    });
  });

  it('puts the board and both kinds of card back when the resume is refused', () => {
    const { deps, cards } = makeDeps();
    const board = createAnswerBoard(deps);
    board.armCreditPause('pause-1');
    const settle = board.beginResume(['int-a']);
    // What resumeWithHitlResponse does next, assuming admission.
    board.clear();

    settle(false);

    expect([...deps.pendingIds.current]).toEqual(['int-a']);
    expect(cards().creditPauses['pause-1'].status).toBe('pending');
    expect(cards().toolApprovals['int-a#0'].status).toBe('pending');
    // Another interrupt's card is not this resume's to move.
    expect(cards().toolApprovals.other.status).toBe('approved');
  });

  it('leaves a departed thread’s board alone', () => {
    const { deps, cards } = makeDeps();
    const board = createAnswerBoard(deps);
    board.armCreditPause('pause-1');
    const settle = board.beginResume(['int-a']);
    board.clear();
    // The thread switch: clears the board, bumps the epoch.
    deps.sessionEpoch.current += 1;

    // ...and only then does the aborted stream's `finally` fire.
    settle(false);

    // B's board stays B's. Restoring A's unanswered id here would fail B's
    // batch gate for the rest of the session.
    expect([...deps.pendingIds.current]).toEqual([]);
    // And no card is settled on whatever thread is now on screen.
    expect(cards().toolApprovals['int-a#0'].status).toBe('approved');
  });

  it('drops the refused attempt’s half-filled slots so the retry fills a fresh set', () => {
    const { deps } = makeDeps();
    const board = createAnswerBoard(deps);
    board.decideToolCall('int-a#0', 'int-a', { index: 0, count: 2 }, { approved: true });
    board.beginResume(['int-a'])(false);

    // Slot 0 is gone, so answering slot 1 alone must not complete the batch.
    expect(
      board.decideToolCall('other', 'int-a', { index: 1, count: 2 }, { approved: true }),
    ).toBeNull();
  });

  it('moves the pause card to resumed exactly once', () => {
    const { deps, cards } = makeDeps();
    const board = createAnswerBoard(deps);
    board.armCreditPause('pause-1');
    const settle = board.beginResume(['int-a']);

    settle(true);
    expect(cards().creditPauses['pause-1'].status).toBe('resumed');
    // The `finally` runs after the run-id callback already settled it; a second
    // move would put a refused-looking card on a turn that is running.
    settle(false);
    expect(cards().creditPauses['pause-1'].status).toBe('resumed');
  });
});
