/**
 * The resume settler's fence. Restoring a refused resume's interrupt board is
 * correct only on the thread that armed it: the settler runs from the aborted
 * stream's `finally`, which is strictly after a thread switch has already
 * cleared the board, so an unfenced restore lands thread A's ids on thread B.
 */
import { describe, it, expect, vi } from 'vitest';
import { beginResume, type CreditPauseResumeRefs } from '../creditPauseResume';

function makeRefs(): CreditPauseResumeRefs {
  return {
    pauseId: { current: 'pause-1' },
    pendingInterruptIds: { current: new Set(['int-a']) },
    collectedHitlResponses: { current: {} },
    sessionEpoch: { current: 1 },
    setMessages: vi.fn(),
  } as unknown as CreditPauseResumeRefs;
}

describe('beginResume', () => {
  it('puts the board back when the resume is refused on the same thread', () => {
    const refs = makeRefs();
    const settle = beginResume(refs);
    // What resumeWithHitlResponse does next, assuming admission.
    refs.pendingInterruptIds.current = new Set();
    refs.collectedHitlResponses.current = {};

    settle(false);

    expect([...refs.pendingInterruptIds.current]).toEqual(['int-a']);
    expect(refs.pauseId.current).toBeNull();
    expect(refs.setMessages).toHaveBeenCalled();
  });

  it('leaves a departed thread’s board alone', () => {
    const refs = makeRefs();
    const settle = beginResume(refs);
    refs.pendingInterruptIds.current = new Set();
    // The thread switch: clears the board, bumps the epoch.
    refs.sessionEpoch.current += 1;

    // ...and only then does the aborted stream's `finally` fire.
    settle(false);

    // B's board stays B's. Restoring A's unanswered id here would fail B's
    // `every(id => collected[id])` batch gate for the rest of the session.
    expect([...refs.pendingInterruptIds.current]).toEqual([]);
    // And no card is settled on whatever thread is now on screen.
    expect(refs.setMessages).not.toHaveBeenCalled();
    expect(refs.pauseId.current).toBe('pause-1');
  });
});
