/**
 * An interrupt that replays AFTER the resume turn that answered it must still
 * settle, for every HITL family.
 *
 * The normal resolvers walk forward: the card is queued when the interrupt
 * replays, and the next resume turn settles it. Checkpoint replay breaks that
 * order. It appends the thread's tip interrupt once at the very end, with no
 * turn_index to place it, and a resume commits the boundary that would place it
 * only when the graph consumes the Command — so a reload taken inside that
 * window replays the interrupt last, after the evidence that answers it. Every
 * card then survives replay `pending` and re-arms live Approve/Reject/Answer
 * buttons on a turn that is already running.
 *
 * The single resume `user_message` carries what settles them: the ids it
 * answered, the answer map, and its own content. It is trusted only while the
 * turn is still running, which is what a missing `run_id` says.
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

const OPENING_TURN = {
  event: 'user_message',
  data: { thread_id: 'thread-1', turn_index: 0, content: 'Analyse the filing' },
};

const interrupt = (actionRequest: Record<string, unknown>) => ({
  event: 'interrupt',
  data: {
    thread_id: 'thread-1',
    turn_index: 0,
    interrupt_id: 'int-1',
    action_requests: [actionRequest],
  },
});

/** The resume turn, as the server persists it: ids answered, answers, content. */
const resume = (metadata: Record<string, unknown>, content = '') => ({
  event: 'user_message',
  data: { thread_id: 'thread-1', turn_index: 1, content, metadata },
});

/** Replays the interrupt LAST, which is the ordering under test. */
async function replayTailInterrupt(
  actionRequest: Record<string, unknown>,
  resumeEvent: Record<string, unknown>,
) {
  const { rt, read } = buildRuntime();
  api.replayThreadHistory.mockImplementation(
    replayOf([OPENING_TURN, resumeEvent, interrupt(actionRequest)]),
  );
  await loadConversationHistory(rt, makeDeps());
  return { rt, read };
}

function cardOn(messages: MessageRecord[], bucket: string) {
  const bubble = messages.find((m) => m.id === 'history-assistant-0') as unknown as AssistantMessage;
  return (bubble as unknown as Record<string, Record<string, Record<string, unknown>>>)?.[bucket]?.['int-1'];
}

/** Nothing queued: a queued entry here arms a button on a turn already running. */
function expectNothingToReArm(rt: ReturnType<typeof buildRuntime>['rt']) {
  expect(rt.historyHasUnresolvedInterruptRef.current).toBe(false);
  expect(rt.unresolvedHistoryInterruptRef.current).toEqual([]);
}

beforeEach(() => vi.clearAllMocks());

describe('history replay — an interrupt that replays after its answer', () => {
  it('settles an approved plan', async () => {
    // A bare approve travels with no message and no answer-map entry, so the
    // resume's empty content is what says approved — the same rule the
    // forward resolver uses.
    const { rt, read } = await replayTailInterrupt(
      { description: 'Pull the 10-K and chart revenue.' },
      resume({ hitl_interrupt_ids: ['int-1'] }),
    );

    expect(cardOn(read(), 'planApprovals')?.status).toBe('approved');
    expectNothingToReArm(rt);
  });

  it('settles a rejected plan, which rides the resume content', async () => {
    const { rt, read } = await replayTailInterrupt(
      { description: 'Pull the 10-K and chart revenue.' },
      resume({ hitl_interrupt_ids: ['int-1'] }, 'Use the 10-Q instead.'),
    );

    expect(cardOn(read(), 'planApprovals')?.status).toBe('rejected');
    expectNothingToReArm(rt);
  });

  it('settles an answered question, keeping the answer', async () => {
    const { rt, read } = await replayTailInterrupt(
      { type: 'ask_user_question', question: 'Which fiscal year?', options: ['2024', '2025'] },
      resume({ hitl_interrupt_ids: ['int-1'], hitl_answers: { 'int-1': '2025' } }),
    );

    expect(cardOn(read(), 'userQuestions')).toMatchObject({ status: 'answered', answer: '2025' });
    expectNothingToReArm(rt);
  });

  it('settles a skipped question', async () => {
    // A skip is a reject with no message, which the server records as a null
    // answer — present in the map, unlike an approve.
    const { rt, read } = await replayTailInterrupt(
      { type: 'ask_user_question', question: 'Which fiscal year?', options: ['2024', '2025'] },
      resume({ hitl_interrupt_ids: ['int-1'], hitl_answers: { 'int-1': null } }),
    );

    expect(cardOn(read(), 'userQuestions')?.status).toBe('skipped');
    expectNothingToReArm(rt);
  });

  it('settles an approved proposal', async () => {
    // Approve and reject both travel without a message, so presence in the
    // answer map is the whole discriminator.
    const { rt, read } = await replayTailInterrupt(
      { type: 'ptc_agent', question: 'Build the model', workspace_id: 'ws-1', tool_call_id: 'tc-1' },
      resume({ hitl_interrupt_ids: ['int-1'] }),
    );

    expect(cardOn(read(), 'ptcAgentProposals')?.status).toBe('approved');
    expectNothingToReArm(rt);
  });

  it('settles a rejected proposal', async () => {
    const { rt, read } = await replayTailInterrupt(
      { type: 'ptc_agent', question: 'Build the model', workspace_id: 'ws-1', tool_call_id: 'tc-1' },
      resume({ hitl_interrupt_ids: ['int-1'], hitl_answers: { 'int-1': null } }),
    );

    expect(cardOn(read(), 'ptcAgentProposals')?.status).toBe('rejected');
    expectNothingToReArm(rt);
  });

  it('settles a secretary action, which shares the proposal rule', async () => {
    const { rt, read } = await replayTailInterrupt(
      { type: 'delete_workspace', workspace_id: 'ws-1', workspace_name: 'Filings' },
      resume({ hitl_interrupt_ids: ['int-1'] }),
    );

    expect(cardOn(read(), 'secretaryActionProposals')?.status).toBe('approved');
    expectNothingToReArm(rt);
  });

  it('leaves an interrupt no resume claimed pending', async () => {
    // The guard that keeps the fix from swallowing genuinely open cards: only
    // an id the resume turn named is settled.
    const { rt, read } = await replayTailInterrupt(
      { type: 'ask_user_question', question: 'Which fiscal year?', options: ['2024', '2025'] },
      resume({ hitl_interrupt_ids: ['some-other-interrupt'] }),
    );

    expect(cardOn(read(), 'userQuestions')?.status).toBe('pending');
    expect(rt.historyHasUnresolvedInterruptRef.current).toBe(true);
  });

  it('keeps a re-raised question answered and adds no second card', async () => {
    // The chronological ordering, where the card is on screen before the resume
    // and the forward resolver settles it. The re-raise is then a second
    // occurrence, which the rendered-id branch drops rather than rendering
    // twice — behaviour the late-arrival path must leave alone.
    const { rt, read } = buildRuntime();
    const question = {
      type: 'ask_user_question',
      question: 'Which fiscal year?',
      options: ['2024', '2025'],
    };
    api.replayThreadHistory.mockImplementation(
      replayOf([
        OPENING_TURN,
        interrupt(question),
        resume({ hitl_interrupt_ids: ['int-1'], hitl_answers: { 'int-1': '2025' } }),
        { event: 'interrupt', data: { ...interrupt(question).data, turn_index: 1 } },
      ]),
    );

    await loadConversationHistory(rt, makeDeps());

    // Answered by the forward resolver, and the re-raise adds no second card.
    expect(cardOn(read(), 'userQuestions')?.status).toBe('answered');
    const bubbles = read().filter((m) => m.role === 'assistant') as unknown as AssistantMessage[];
    expect(
      bubbles.flatMap((b) => (b.contentSegments || []).filter((sg) => sg.type === 'user_question')),
    ).toHaveLength(1);
  });
  it('leaves the card pending when the resume turn already ended', async () => {
    // The window a claim describes is "requested but not yet consumed". A
    // terminal turn carries its run id, and an interrupt trailing one is either
    // a genuine re-raise or one the failed resume never took — both still owed
    // an answer. Settling here is how a dead resume would lose its controls.
    const { rt, read } = await replayTailInterrupt(
      { type: 'ptc_agent', question: 'Build the model', workspace_id: 'ws-1', tool_call_id: 'tc-1' },
      {
        event: 'user_message',
        data: {
          thread_id: 'thread-1',
          turn_index: 1,
          content: '',
          run_id: 'run-9',
          metadata: { hitl_interrupt_ids: ['int-1'] },
        },
      },
    );

    expect(cardOn(read(), 'ptcAgentProposals')?.status).toBe('pending');
    expect(rt.historyHasUnresolvedInterruptRef.current).toBe(true);
  });

  it('settles a plan rejected with no feedback', async () => {
    // A bare reject is the one shape the server DOES record, as a null answer.
    // Reading only the content here would call this plan approved.
    const { rt, read } = await replayTailInterrupt(
      { description: 'Model the filing' },
      resume({ hitl_interrupt_ids: ['int-1'], hitl_answers: { 'int-1': null } }),
    );

    expect(cardOn(read(), 'planApprovals')?.status).toBe('rejected');
    expectNothingToReArm(rt);
  });

  it('settles a proposal rejected with a message', async () => {
    // The server records neither a bare approve nor a reject that carried a
    // message, so absence alone cannot mean approved: the reject's message is
    // in the resume's content, and that is what tells them apart.
    const { rt, read } = await replayTailInterrupt(
      { type: 'delete_workspace', workspace_id: 'ws-1', workspace_name: 'Filings' },
      resume({ hitl_interrupt_ids: ['int-1'] }, 'keep it, I still need the filings'),
    );

    expect(cardOn(read(), 'secretaryActionProposals')?.status).toBe('rejected');
    expectNothingToReArm(rt);
  });

  it('leaves a batched resume pending when its content cannot be attributed', async () => {
    // One resume answers several interrupts and the server joins every reject
    // message into one content, so an absent entry beside a non-empty content
    // names no particular interrupt. Guessing here is how an approve reads
    // rejected, or a reject reads approved.
    const { rt, read } = await replayTailInterrupt(
      { type: 'ptc_agent', question: 'Build the model', workspace_id: 'ws-1', tool_call_id: 'tc-1' },
      resume({ hitl_interrupt_ids: ['int-1', 'int-2'] }, 'not this one'),
    );

    expect(cardOn(read(), 'ptcAgentProposals')?.status).toBe('pending');
    expect(rt.historyHasUnresolvedInterruptRef.current).toBe(true);
  });

  it('releases the settled id so a live re-raise still renders a card', async () => {
    // The live path suppresses an interrupt whose id is already rendered,
    // because that card is normally still answerable. A settled one is not, and
    // dropping its pending entry also takes it out of the reconnect strip that
    // would have released the id — so the settle has to release it itself.
    const { rt } = await replayTailInterrupt(
      { type: 'credit_pause' },
      resume({ hitl_interrupt_ids: ['int-1'] }),
    );

    expect(rt.renderedInterruptIdsRef.current.has('int-1')).toBe(false);
  });

  it('settles every family into a real card bucket', async () => {
    // The bucket table lives in buckets.ts while the branches that push into it
    // live in fromHistoryEvent, so a family added to one and not the other
    // would write a card under a name nothing renders.
    const families: Array<[string, Record<string, unknown>]> = [
      ['planApprovals', { description: 'Model the filing' }],
      ['userQuestions', { type: 'ask_user_question', question: 'Which year?', options: ['2024'] }],
      ['creditPauses', { type: 'credit_pause' }],
      ['workspaceProposals', { type: 'create_workspace', workspace_name: 'Filings' }],
      ['questionProposals', { type: 'start_question', workspace_id: 'ws-1', question: 'q' }],
      ['ptcAgentProposals', { type: 'ptc_agent', question: 'q', workspace_id: 'ws-1', tool_call_id: 'tc-1' }],
      ['secretaryActionProposals', { type: 'delete_workspace', workspace_id: 'ws-1' }],
    ];

    for (const [bucket, actionRequest] of families) {
      const { rt, read } = await replayTailInterrupt(
        actionRequest,
        resume({ hitl_interrupt_ids: ['int-1'] }),
      );
      const bubble = read().find((m) => m.id === 'history-assistant-0') as unknown as AssistantMessage;
      expect(Object.keys(bubble)).not.toContain('undefined');
      expect(cardOn(read(), bucket)?.status).not.toBe('pending');
      expectNothingToReArm(rt);
    }
  });
});
