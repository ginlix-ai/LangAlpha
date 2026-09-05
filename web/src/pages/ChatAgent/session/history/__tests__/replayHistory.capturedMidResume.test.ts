/**
 * The real thing: a payload captured off the running server, mid-resume.
 *
 * Every other suite here builds its events by hand. This one replays what the
 * backend actually served while a resumed run was still in flight, so it pins
 * the exact shape that reached a browser: the interrupt inline on the turn it
 * paused (turn_index 0), the live resume that answered it (no run_id), and the
 * SAME interrupt again at the branch tip with no turn_index. Before the fix the
 * card settled on the resume and was then dragged back to `pending` by that
 * third event, arming a Resume button on a turn already running again.
 *
 * Captured from thread 4cd40a81 on a dev stack; regenerate with
 * scripts the PR describes if the replay contract changes.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { AssistantMessage } from '@/types/chat';

const api = vi.hoisted(() => ({ replayThreadHistory: vi.fn() }));
vi.mock('../../../utils/api', () => ({ replayThreadHistory: api.replayThreadHistory }));

import { loadConversationHistory } from '../replayHistory';
import { buildRuntime, makeDeps, replayOf } from './replayHarness';
import captured from './fixtures/replayMidResume.json';

beforeEach(() => vi.clearAllMocks());

describe('replay captured mid-resume', () => {
  it('settles the card the resume already answered', async () => {
    const { rt, read } = buildRuntime();
    api.replayThreadHistory.mockImplementation(
      replayOf(captured as unknown as Array<Record<string, unknown>>),
    );

    await loadConversationHistory(rt, makeDeps());

    const cards = read()
      .filter((m) => m.role === 'assistant')
      .flatMap((m) => Object.entries((m as unknown as AssistantMessage).creditPauses ?? {}));

    expect(cards.length).toBeGreaterThan(0);
    // The whole point: nothing still offers a live control.
    for (const [, card] of cards) {
      expect((card as { status?: string }).status).not.toBe('pending');
    }
    expect(rt.unresolvedHistoryInterruptRef.current).toEqual([]);
    expect(rt.historyHasUnresolvedInterruptRef.current).toBe(false);
  });
});
