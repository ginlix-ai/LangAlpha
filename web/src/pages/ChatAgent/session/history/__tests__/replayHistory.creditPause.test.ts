/**
 * A credit pause that was resumed must replay as resolved.
 *
 * Every other interrupt type has a resolution path in the replay; the credit
 * pause is the one gate interrupt, so it has no tool call to settle it and is
 * deliberately absent from PROPOSAL_INTERRUPT_TYPES. Its resume also carries no
 * answer (approve with no message), so it never reaches `hitl_answers` the way
 * a question does — `hitl_interrupt_ids` on the resume turn's query metadata is
 * the signal. Without it the card replays `pending` forever, survives into
 * `unresolvedHistoryInterruptRef`, and re-arms a live Resume button on a turn
 * that already resumed and completed.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { AssistantMessage } from '@/types/chat';

const api = vi.hoisted(() => ({ replayThreadHistory: vi.fn() }));

vi.mock('../../../utils/api', () => ({
  replayThreadHistory: api.replayThreadHistory,
}));

import { loadConversationHistory } from '../replayHistory';
import type { MessageRecord } from '../../types';
import { buildRuntime, makeDeps, replayOf } from './replayHarness';

/** Turn 0 asks a question and ends on a credit-pause interrupt. */
const PAUSED_TURN = [
  { event: 'user_message', data: { thread_id: 'thread-1', turn_index: 0, content: 'Analyse the filing' } },
  {
    event: 'interrupt',
    data: {
      thread_id: 'thread-1',
      turn_index: 0,
      interrupt_id: 'int-1',
      action_requests: [{ type: 'credit_pause', message: 'Out of credits.' }],
    },
  },
];

/** The resume turn: no content, and the ids it answered on its query metadata. */
const RESUME_TURN = [
  {
    event: 'user_message',
    data: {
      thread_id: 'thread-1',
      turn_index: 1,
      content: '',
      metadata: { hitl_interrupt_ids: ['int-1'] },
    },
  },
];

function pauseOn(messages: MessageRecord[]) {
  const bubble = messages.find((m) => m.id === 'history-assistant-0') as unknown as AssistantMessage;
  return bubble?.creditPauses?.['int-1'];
}

beforeEach(() => vi.clearAllMocks());

describe('history replay — credit pause resolution', () => {
  it('replays a resumed pause as resolved and leaves nothing to re-arm', async () => {
    const { rt, read } = buildRuntime();
    api.replayThreadHistory.mockImplementation(replayOf([...PAUSED_TURN, ...RESUME_TURN]));

    await loadConversationHistory(rt, makeDeps());

    expect(pauseOn(read())?.status).toBe('resumed');
    // The re-arm path in useChatMessages reads exactly these two.
    expect(rt.historyHasUnresolvedInterruptRef.current).toBe(false);
    expect(rt.unresolvedHistoryInterruptRef.current).toEqual([]);
  });

  it('carries the canonical quota-denial links rather than the card building its own', async () => {
    const { rt, read } = buildRuntime();
    api.replayThreadHistory.mockImplementation(replayOf(PAUSED_TURN));

    await loadConversationHistory(rt, makeDeps());

    expect(pauseOn(read())?.links).toEqual([
      {
        url: '/account/plans',
        label: 'Manage plan',
        labelKey: 'chat.errorLinkManagePlan',
        external: true,
      },
      {
        url: '/account/usage',
        label: 'View usage',
        labelKey: 'chat.errorLinkViewUsage',
        external: true,
      },
    ]);
  });

  it('keeps a pause the user never resumed pending, and hands it to the re-arm path', async () => {
    const { rt, read } = buildRuntime();
    api.replayThreadHistory.mockImplementation(replayOf(PAUSED_TURN));

    await loadConversationHistory(rt, makeDeps());

    expect(pauseOn(read())?.status).toBe('pending');
    expect(rt.historyHasUnresolvedInterruptRef.current).toBe(true);
    expect(rt.unresolvedHistoryInterruptRef.current).toEqual([
      expect.objectContaining({ type: 'credit_pause', interruptId: 'int-1', proposalId: 'int-1' }),
    ]);
  });

  it('resolves a pause that replays after the resume that answered it', async () => {
    // The reload-mid-resume ordering. Checkpoint replay is the source while
    // the resume run is in flight (the SSE fallback only takes over once the
    // turn is persisted), and it appends the thread's tip interrupt once at
    // the very end, carrying no turn_index to place it — the resume commits
    // the boundary that would place it only when the graph consumes the
    // Command. So the pause arrives AFTER the stamp that answered it. The
    // resume turn carrying no run_id is what says it is still running, and
    // therefore that this pause is the original, replayed late.
    const { rt, read } = buildRuntime();
    api.replayThreadHistory.mockImplementation(
      replayOf([PAUSED_TURN[0], ...RESUME_TURN, PAUSED_TURN[1]]),
    );

    await loadConversationHistory(rt, makeDeps());

    expect(pauseOn(read())?.status).toBe('resumed');
    // Nothing to re-arm: a queued entry here is a Resume button offered on a
    // turn that is already running again.
    expect(rt.historyHasUnresolvedInterruptRef.current).toBe(false);
    expect(rt.unresolvedHistoryInterruptRef.current).toEqual([]);
  });

  it('un-resolves a pause the resume turn raised again', async () => {
    // `hitl_interrupt_ids` is stamped when the resume is requested, not when
    // the graph consumes the Command, so the stamp alone can be wrong. The
    // re-raise on the resume turn is the later evidence, and without honouring
    // it the card replays `resumed` with no Resume button on the one thread
    // that still needs one.
    const { rt, read } = buildRuntime();
    api.replayThreadHistory.mockImplementation(
      replayOf([
        ...PAUSED_TURN,
        ...RESUME_TURN,
        {
          event: 'interrupt',
          data: {
            thread_id: 'thread-1',
            turn_index: 1,
            interrupt_id: 'int-1',
            action_requests: [{ type: 'credit_pause', message: 'Out of credits.' }],
          },
        },
      ]),
    );

    await loadConversationHistory(rt, makeDeps());

    expect(pauseOn(read())?.status).toBe('pending');
    // Still one card: the re-raise restores, it never renders a second.
    const bubbles = read().filter((m) => m.role === 'assistant') as unknown as AssistantMessage[];
    expect(
      bubbles.flatMap((b) => (b.contentSegments || []).filter((sg) => sg.type === 'credit_pause')),
    ).toHaveLength(1);
    // ...and the re-arm path gets it back, so the click is not dropped.
    expect(rt.historyHasUnresolvedInterruptRef.current).toBe(true);
    expect(rt.unresolvedHistoryInterruptRef.current).toEqual([
      expect.objectContaining({ type: 'credit_pause', interruptId: 'int-1', proposalId: 'int-1' }),
    ]);
  });

  it('settles a pause that a later attempt finally consumed', async () => {
    // Two refused resumes then a successful one. The re-queued entry after the
    // first re-raise names the resume turn's bubble, not the card's, so a
    // resolution that patched by bubble would flip an invisible copy and leave
    // the real card pending — a Resume button on a finished thread, with the
    // pending set never re-armed, so it would not even answer.
    const { rt, read } = buildRuntime();
    api.replayThreadHistory.mockImplementation(
      replayOf([
        ...PAUSED_TURN,
        ...RESUME_TURN,
        {
          event: 'interrupt',
          data: {
            thread_id: 'thread-1',
            turn_index: 1,
            interrupt_id: 'int-1',
            action_requests: [{ type: 'credit_pause', message: 'Out of credits.' }],
          },
        },
        {
          event: 'user_message',
          data: {
            thread_id: 'thread-1',
            turn_index: 2,
            content: '',
            metadata: { hitl_interrupt_ids: ['int-1'] },
          },
        },
      ]),
    );

    await loadConversationHistory(rt, makeDeps());

    expect(pauseOn(read())?.status).toBe('resumed');
    expect(rt.historyHasUnresolvedInterruptRef.current).toBe(false);
    expect(rt.unresolvedHistoryInterruptRef.current).toEqual([]);
  });

  it('resolves only the pause the resume names', async () => {
    const { rt, read } = buildRuntime();
    api.replayThreadHistory.mockImplementation(
      replayOf([
        ...PAUSED_TURN,
        {
          event: 'user_message',
          data: {
            thread_id: 'thread-1',
            turn_index: 1,
            content: '',
            metadata: { hitl_interrupt_ids: ['some-other-interrupt'] },
          },
        },
      ]),
    );

    await loadConversationHistory(rt, makeDeps());

    expect(pauseOn(read())?.status).toBe('pending');
    expect(rt.historyHasUnresolvedInterruptRef.current).toBe(true);
  });
});
