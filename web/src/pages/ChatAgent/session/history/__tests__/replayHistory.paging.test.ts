/**
 * A long thread replays a page at a time: the newest page when it opens, then
 * older pages put above the transcript as the reader scrolls up.
 *
 * An older page is replayed into a transcript of its own and only joins the
 * one on screen when committed, so everything already there (a live turn
 * included) keeps its place, and nothing the newest page owns (the rendered
 * turn watermark) moves. The page boundary also splits what one replay used
 * to see whole: a resume settles the interrupt it answered, and a gated call
 * gets its result, in the turn after the one that raised it. When that turn
 * opens the newer page, the newer page carries the evidence back to the
 * older one.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { AssistantMessage, ChatMessage } from '@/types/chat';

const api = vi.hoisted(() => ({ replayThreadHistory: vi.fn() }));

vi.mock('../../../utils/api', () => ({
  replayThreadHistory: api.replayThreadHistory,
}));

import { loadConversationHistory } from '../replayHistory';
import {
  commitOlderHistoryPage,
  loadOlderHistoryPage,
  type HistoryCarry,
  type LoadedHistoryPage,
  type OlderHistoryPage,
} from '../olderPages';
import { HISTORY_PAGE_TURNS } from '../historyWindow';
import { buildRuntime, makeDeps, replayOf } from './replayHarness';
import type { HistoryRuntime } from '../../runtime';
import type { TokenUsage } from '../../types';

type Item = { event: string; data?: Record<string, unknown> };

/** A plain turn: the question, then a one-chunk answer. */
function turn(k: number): Item[] {
  return [
    { event: 'user_message', data: { thread_id: 'thread-1', turn_index: k, content: `question ${k}` } },
    {
      event: 'message_chunk',
      data: { turn_index: k, role: 'assistant', content_type: 'text', content: `answer ${k}` },
    },
  ];
}

function turns(from: number, to: number): Item[] {
  const out: Item[] = [];
  for (let k = from; k <= to; k++) out.push(...turn(k));
  return out;
}

const header = (first: number | null, hasMore: boolean): Item => ({
  event: 'history_page',
  data: { first_turn_index: first, has_more: hasMore },
});

const DONE: Item = { event: 'replay_done', data: { thread_id: 'thread-1' } };

const EMPTY_CARRY: HistoryCarry = { events: [], subagents: new Map(), steeredAgentIds: new Set() };

const ids = (messages: ChatMessage[]) => messages.map((m) => m.id);

const bubble = (messages: ChatMessage[], id: string) =>
  messages.find((m) => m.id === id) as unknown as AssistantMessage;

/** Load the newest page and return the page it reported (undefined when it reported none). */
async function loadNewest(rt: ReturnType<typeof buildRuntime>['rt'], items: Item[], limit = HISTORY_PAGE_TURNS) {
  api.replayThreadHistory.mockImplementationOnce(replayOf(items));
  const reported: { page?: LoadedHistoryPage | null } = {};
  await loadConversationHistory(rt, {
    ...makeDeps(),
    limit,
    onPage: (p) => {
      reported.page = p;
    },
  });
  return reported.page;
}

function olderRequest(beforeTurn: number, signal = new AbortController().signal) {
  return { threadId: 'thread-1', beforeTurn, limit: HISTORY_PAGE_TURNS, signal };
}

const commitDeps = () => ({
  projectSubagentHistory: vi.fn(),
  prependSubagentRuns: vi.fn(),
  isTaskLive: () => false,
  liveOutcome: () => undefined,
});

beforeEach(() => vi.clearAllMocks());

describe('history replay: the newest page', () => {
  it('asks for the page it was given a limit for', async () => {
    const { rt } = buildRuntime();
    await loadNewest(rt, [header(73, true), ...turns(73, 75), DONE], 25);
    expect(api.replayThreadHistory).toHaveBeenCalledWith('thread-1', expect.any(Function), { limit: 25 });
  });

  it('replays the whole thread when no limit is given', async () => {
    const { rt } = buildRuntime();
    api.replayThreadHistory.mockImplementationOnce(replayOf([...turns(0, 1), DONE]));
    await loadConversationHistory(rt, makeDeps());
    expect(api.replayThreadHistory.mock.calls[0][2]).toEqual({});
  });

  it('reports the page header, with an empty carry for a page that needs none', async () => {
    const { rt, read } = buildRuntime();
    const page = await loadNewest(rt, [header(73, true), ...turns(73, 75), DONE]);

    expect(page).toMatchObject({ firstTurnIndex: 73, hasMore: true });
    expect(page?.carry.events).toEqual([]);
    expect(page?.carry.subagents.size).toBe(0);
    expect(page?.carry.steeredAgentIds.size).toBe(0);
    // The page header is bookkeeping, never a bubble.
    expect(ids(read())).toEqual([
      'history-user-73', 'history-assistant-73',
      'history-user-74', 'history-assistant-74',
      'history-user-75', 'history-assistant-75',
    ]);
    expect(rt.lastRenderedTurnIndexRef.current).toBe(75);
  });

  it('reads a replay without a header as the whole thread, nothing older', async () => {
    // A server that does not page answers a paged request in full.
    const { rt } = buildRuntime();
    const page = await loadNewest(rt, [...turns(3, 5), DONE]);
    expect(page).toMatchObject({ firstTurnIndex: 3, hasMore: false });
  });

  it('reports a header-less empty thread as no page', async () => {
    const { rt } = buildRuntime();
    const page = await loadNewest(rt, [DONE]);
    expect(page).toBeNull();
  });

  it('reads a header that names no turn as no page, whatever it says is older', async () => {
    const { rt } = buildRuntime();
    const page = await loadNewest(rt, [header(null, true), DONE]);
    expect(page).toBeNull();
  });

  it('reports no page when the replay fails', async () => {
    const { rt } = buildRuntime();
    api.replayThreadHistory.mockRejectedValueOnce(new Error('HTTP 500'));
    const onPage = vi.fn();
    await loadConversationHistory(rt, { ...makeDeps(), limit: HISTORY_PAGE_TURNS, onPage });
    expect(onPage).not.toHaveBeenCalled();
  });
});

describe('history replay: an older page', () => {
  it('asks for the turns before the oldest loaded one', async () => {
    const { rt } = buildRuntime();
    const page = await loadNewest(rt, [header(73, true), ...turns(73, 75), DONE]);
    const signal = new AbortController().signal;
    api.replayThreadHistory.mockImplementationOnce(replayOf([header(53, true), ...turns(53, 72), DONE]));

    await loadOlderHistoryPage(rt, olderRequest(73, signal), page!.carry);

    expect(api.replayThreadHistory).toHaveBeenLastCalledWith(
      'thread-1',
      expect.any(Function),
      { beforeTurn: 73, limit: HISTORY_PAGE_TURNS, signal },
    );
  });

  it('goes above the transcript, leaving every bubble on screen in place', async () => {
    const { rt, read } = buildRuntime();
    const page = await loadNewest(rt, [header(73, true), ...turns(73, 75), DONE]);
    // A turn this view is streaming, after the replayed ones.
    rt.setMessages((prev) => [
      ...prev,
      { id: 'user-live', role: 'user', content: 'live question', turnIndex: 76 } as unknown as ChatMessage,
      { id: 'assistant-live', role: 'assistant', content: '', isStreaming: true, turnIndex: 76 } as unknown as ChatMessage,
    ]);
    const onScreen = ids(read());
    const startBefore = rt.newMessagesStartIndexRef.current;

    api.replayThreadHistory.mockImplementationOnce(replayOf([header(70, true), ...turns(70, 72), DONE]));
    const older = await loadOlderHistoryPage(rt, olderRequest(73), page!.carry);

    // Replayed aside: nothing on screen changes until the commit.
    expect(ids(read())).toEqual(onScreen);
    expect(older!.page).toMatchObject({ firstTurnIndex: 70, hasMore: true });

    commitOlderHistoryPage(rt, commitDeps(), older!);

    expect(ids(read())).toEqual([
      'history-user-70', 'history-assistant-70',
      'history-user-71', 'history-assistant-71',
      'history-user-72', 'history-assistant-72',
      ...onScreen,
    ]);
    // History still splices in above the live turn.
    expect(rt.newMessagesStartIndexRef.current).toBe(startBefore + older!.messages.length);
    expect(older!.messages).toHaveLength(6);
    // The watermark is the newest page's; an older page knows nothing newer.
    expect(rt.lastRenderedTurnIndexRef.current).toBe(75);
    // Each bubble names its own turn, not its position.
    expect(read()[0]).toMatchObject({ id: 'history-user-70', turnIndex: 70 });
    expect(bubble(read(), 'history-assistant-72').turnIndex).toBe(72);
    expect(bubble(read(), 'assistant-live')).toMatchObject({ isStreaming: true, turnIndex: 76 });
  });

  it('reports the start of the thread when nothing older remains', async () => {
    const { rt } = buildRuntime();
    const page = await loadNewest(rt, [header(3, true), ...turns(3, 5), DONE]);
    api.replayThreadHistory.mockImplementationOnce(replayOf([header(0, false), ...turns(0, 2), DONE]));

    const older = await loadOlderHistoryPage(rt, olderRequest(3), page!.carry);

    expect(older!.page).toMatchObject({ firstTurnIndex: 0, hasMore: false });
  });

  it('reads a page that does not start before the one it extends as the start of the thread', async () => {
    const { rt } = buildRuntime();
    const page = await loadNewest(rt, [header(73, true), ...turns(73, 75), DONE]);

    // Claims more, but starts where the newer page did.
    api.replayThreadHistory.mockImplementationOnce(replayOf([header(73, true), DONE]));
    const stuck = await loadOlderHistoryPage(rt, olderRequest(73), page!.carry);
    expect(stuck!.page).toMatchObject({ firstTurnIndex: 73, hasMore: false });

    // Holds no turn at all.
    api.replayThreadHistory.mockImplementationOnce(replayOf([header(null, true), DONE]));
    const empty = await loadOlderHistoryPage(rt, olderRequest(73), page!.carry);
    expect(empty!.messages).toEqual([]);
    expect(empty!.page).toMatchObject({ firstTurnIndex: 73, hasMore: false });
  });

  it('leaves the transcript untouched until the commit', async () => {
    const { rt, read } = buildRuntime();
    const page = await loadNewest(rt, [header(73, true), ...turns(73, 75), DONE]);
    const onScreen = read();
    const interruptsBefore = new Set(rt.renderedInterruptIdsRef.current);

    api.replayThreadHistory.mockImplementationOnce(replayOf([
      header(70, true),
      ...turns(70, 72),
      {
        event: 'interrupt',
        data: {
          turn_index: 72,
          interrupt_id: 'int-old',
          action_requests: [{ type: 'ask_user_question', question: 'Which?', options: ['A', 'B'] }],
        },
      },
      DONE,
    ]));
    const older = await loadOlderHistoryPage(rt, olderRequest(73), page!.carry);

    expect(read()).toBe(onScreen);
    expect(rt.newMessagesStartIndexRef.current).toBe(6);
    // The page's cards never reached the screen, so their ids are not claimed.
    expect(rt.renderedInterruptIdsRef.current).toEqual(interruptsBefore);
    expect(older!.renderedInterruptIds.has('int-old')).toBe(true);
  });

  it('holds an offload notice in the page itself, not in a write after it', async () => {
    vi.useFakeTimers();
    try {
      const { rt, read } = buildRuntime();
      const page = await loadNewest(rt, [header(73, true), ...turns(73, 75), DONE]);
      const onScreen = read();
      api.replayThreadHistory.mockImplementationOnce(replayOf([
        header(70, true),
        ...turns(70, 72),
        {
          event: 'context_window',
          data: { turn_index: 72, action: 'offload', signal: 'complete', kind: 'reads', offloaded_reads: 3 },
        },
        DONE,
      ]));
      const older = await loadOlderHistoryPage(rt, olderRequest(73), page!.carry);

      expect(bubble(older!.messages, 'history-assistant-72').contentSegments).toContainEqual(
        expect.objectContaining({ type: 'notification', content: 'chat.offloadedReadsNotification' }),
      );
      // Nothing is left to land on the transcript later, whether or not the page joins it.
      vi.runAllTimers();
      expect(read()).toBe(onScreen);
    } finally {
      vi.useRealTimers();
    }
  });

  it('resolves null when the fetch was aborted', async () => {
    const { rt, read } = buildRuntime();
    const page = await loadNewest(rt, [header(73, true), ...turns(73, 75), DONE]);
    const onScreen = read();

    api.replayThreadHistory.mockResolvedValueOnce({ disconnected: false, aborted: true, contentLocation: null });
    await expect(loadOlderHistoryPage(rt, olderRequest(73), page!.carry)).resolves.toBeNull();

    // A signal aborted mid-replay drops the page even when the stream ended clean.
    const controller = new AbortController();
    api.replayThreadHistory.mockImplementationOnce(async (_tid: string, onEvent: (e: Record<string, unknown>) => void) => {
      for (const item of [header(70, true), ...turns(70, 72)]) onEvent({ event: item.event, ...item.data });
      controller.abort();
      return { disconnected: false, aborted: false, contentLocation: null };
    });
    await expect(loadOlderHistoryPage(rt, olderRequest(73, controller.signal), page!.carry)).resolves.toBeNull();

    expect(read()).toBe(onScreen);
  });

  it('adds the page to the thread token totals on commit, leaving the window the newest call', async () => {
    const { rt } = buildRuntime();
    let usage: TokenUsage | null = null;
    rt.setTokenUsage = ((next) => {
      usage = typeof next === 'function' ? next(usage) : next;
    }) as HistoryRuntime['setTokenUsage'];
    const call = (k: number, input: number, output: number): Item => ({
      event: 'context_window',
      data: {
        turn_index: k, action: 'token_usage', signal: 'complete',
        input_tokens: input, output_tokens: output, total_tokens: input + output, threshold: 1000,
      },
    });
    const page = await loadNewest(rt, [header(73, true), ...turns(73, 75), call(75, 300, 20), DONE]);
    const newest = { totalInput: 300, totalOutput: 20, lastOutput: 20, total: 320, threshold: 1000 };
    expect(usage).toEqual(newest);

    api.replayThreadHistory.mockImplementationOnce(replayOf([
      header(70, true), ...turns(70, 72), call(71, 100, 10), call(72, 200, 15), DONE,
    ]));
    const older = await loadOlderHistoryPage(rt, olderRequest(73), page!.carry);
    // A page that never joins the transcript counts for nothing.
    expect(usage).toEqual(newest);

    commitOlderHistoryPage(rt, commitDeps(), older!);
    expect(usage).toEqual({ ...newest, totalInput: 600, totalOutput: 45 });
  });

  it('puts the page runs of a task a live stream wrote to ahead of what it wrote', () => {
    const { rt, read } = buildRuntime();
    rt.subagentHistory.toolCalls.set('call-done', 'task:done');
    rt.subagentHistory.toolCalls.set('call-quiet', 'task:quiet');
    const chip = (subagentId: string) => ({
      subagentId, description: '', prompt: '', type: 'general-purpose', action: 'init', status: 'running',
    });
    const task = { messages: [], events: [], status: 'running' };
    const own = { messages: [], events: [], status: 'running', description: 'the page alone' };
    const older: OlderHistoryPage = {
      messages: [{
        id: 'history-assistant-40',
        role: 'assistant',
        content: '',
        subagentTasks: { 'call-done': chip('done'), 'call-quiet': chip('quiet') },
      } as unknown as ChatMessage],
      page: { firstTurnIndex: 40, lastTurnIndex: 59, hasMore: true, carry: EMPTY_CARRY },
      renderedInterruptIds: new Set(),
      threadModels: [],
      subagents: new Map([['task:done', task], ['task:live', task], ['task:quiet', task]]),
      ownSubagents: new Map([['task:done', own], ['task:live', own], ['task:quiet', own]]),
      tokens: { input: 0, output: 0 },
    };
    const deps = {
      ...commitDeps(),
      isTaskLive: (agentId: string) => agentId === 'task:live',
      liveOutcome: (agentId: string) => (agentId === 'task:done' ? ('completed' as const) : undefined),
    };
    commitOlderHistoryPage(rt, deps, older);

    // The page's copy of a task the stream settled, or is still writing,
    // predates what the stream wrote: only the page's own runs go in, ahead of
    // it. The other task is projected whole, and the settled one's chip reads
    // its outcome.
    expect(deps.prependSubagentRuns.mock.calls).toEqual([['task:done', own], ['task:live', own]]);
    expect([...deps.projectSubagentHistory.mock.calls[0][0].keys()]).toEqual(['task:quiet']);
    const tasks = bubble(read(), 'history-assistant-40').subagentTasks!;
    expect(tasks['call-done'].status).toBe('completed');
    expect(tasks['call-quiet'].status).toBe('running');
  });

  it('fails a page whose stream dropped, rather than showing half of it', async () => {
    const { rt } = buildRuntime();
    const page = await loadNewest(rt, [header(73, true), ...turns(73, 75), DONE]);
    api.replayThreadHistory.mockResolvedValueOnce({ disconnected: true, aborted: false, contentLocation: null });

    await expect(loadOlderHistoryPage(rt, olderRequest(73), page!.carry)).rejects.toThrow('History page interrupted');
  });
});

describe('history replay: what a page carries to the one before it', () => {
  /** Turn 72 asks a question; the newer page opens on the turn that answered it. */
  const ASKED = [
    header(70, true),
    ...turns(70, 72),
    {
      event: 'interrupt',
      data: {
        turn_index: 72,
        interrupt_id: 'int-q',
        action_requests: [{ type: 'ask_user_question', question: 'Which colour?', options: ['Blue', 'Red'] }],
      },
    },
    DONE,
  ];
  const ANSWERED = [
    header(73, true),
    {
      event: 'user_message',
      data: {
        thread_id: 'thread-1',
        turn_index: 73,
        content: '',
        metadata: { hitl_answers: { 'int-q': 'Blue' }, hitl_interrupt_ids: ['int-q'] },
      },
    },
    {
      event: 'message_chunk',
      data: { turn_index: 73, role: 'assistant', content_type: 'text', content: 'Blue it is.' },
    },
    ...turns(74, 75),
    DONE,
  ];

  it('settles a question on the older page from the answer on the newer one', async () => {
    const { rt, read } = buildRuntime();
    const page = await loadNewest(rt, ANSWERED);
    expect(page!.carry.events).toEqual([
      expect.objectContaining({ event: 'user_message', turn_index: 73 }),
    ]);

    api.replayThreadHistory.mockImplementationOnce(replayOf(ASKED));
    const older = await loadOlderHistoryPage(rt, olderRequest(73), page!.carry);
    commitOlderHistoryPage(rt, commitDeps(), older!);

    expect(bubble(read(), 'history-assistant-72').userQuestions?.['int-q']).toMatchObject({
      status: 'answered',
      answer: 'Blue',
    });
    // Claimed now that its card is on screen, so a re-raise is not carded twice.
    expect(rt.renderedInterruptIdsRef.current.has('int-q')).toBe(true);
  });

  it('keeps the question pending without the newer page to answer it', async () => {
    const { rt, read } = buildRuntime();
    await loadNewest(rt, [header(73, true), ...turns(73, 75), DONE]);

    api.replayThreadHistory.mockImplementationOnce(replayOf(ASKED));
    const older = await loadOlderHistoryPage(rt, olderRequest(73), EMPTY_CARRY);
    commitOlderHistoryPage(rt, commitDeps(), older!);

    expect(bubble(read(), 'history-assistant-72').userQuestions?.['int-q']?.status).toBe('pending');
  });

  it('marks a resume card on the older page as an update when a newer page saw the task steered', async () => {
    const { rt, read } = buildRuntime();
    await loadNewest(rt, [header(73, true), ...turns(73, 75), DONE]);
    api.replayThreadHistory.mockImplementationOnce(replayOf([
      header(70, true),
      ...turns(70, 72),
      {
        event: 'tool_calls',
        data: { turn_index: 72, tool_calls: [{ id: 'call-resume', name: 'Task', args: { task_id: 'abc', action: 'resume', description: 'dig deeper' } }] },
      },
      DONE,
    ]));
    const carry: HistoryCarry = { ...EMPTY_CARRY, steeredAgentIds: new Set(['task:abc']) };
    const older = await loadOlderHistoryPage(rt, olderRequest(73), carry);
    commitOlderHistoryPage(rt, commitDeps(), older!);

    expect(bubble(read(), 'history-assistant-72').subagentTasks?.['call-resume']).toMatchObject({
      resumeTargetId: 'task:abc',
      action: 'update',
    });
  });

  /** Turn 72 makes a call a gate stopped; the newer page opens on its result. */
  const CALLED = [
    header(70, true),
    ...turns(70, 72),
    {
      event: 'tool_calls',
      data: { turn_index: 72, tool_calls: [{ id: 'call-gated', name: 'get_quote', args: { symbol: 'AAPL' } }] },
    },
    DONE,
  ];
  const RESULTED = [
    header(73, true),
    { event: 'user_message', data: { thread_id: 'thread-1', turn_index: 73, content: '' } },
    {
      event: 'tool_call_result',
      data: { turn_index: 73, tool_call_id: 'call-gated', content: 'AAPL 190.00', status: 'success' },
    },
    ...turns(74, 75),
    DONE,
  ];

  it('fills a call on the older page with its result from the newer one', async () => {
    const { rt, read } = buildRuntime();
    const page = await loadNewest(rt, RESULTED);
    expect(page!.carry.events).toEqual([
      expect.objectContaining({ event: 'tool_call_result', tool_call_id: 'call-gated' }),
    ]);

    api.replayThreadHistory.mockImplementationOnce(replayOf(CALLED));
    const older = await loadOlderHistoryPage(rt, olderRequest(73), page!.carry);
    commitOlderHistoryPage(rt, commitDeps(), older!);

    const process = bubble(read(), 'history-assistant-72').toolCallProcesses['call-gated'];
    expect(process).toMatchObject({ isComplete: true, isInProgress: false });
    expect(process.toolCallResult).toMatchObject({ content: 'AAPL 190.00', tool_call_id: 'call-gated' });
    // Applied, so not handed on to the page before this one.
    expect(older!.page.carry.events).toEqual([]);
  });

  it('hands on a result whose call is older still', async () => {
    const { rt } = buildRuntime();
    const page = await loadNewest(rt, RESULTED);

    api.replayThreadHistory.mockImplementationOnce(replayOf([header(70, true), ...turns(70, 72), DONE]));
    const older = await loadOlderHistoryPage(rt, olderRequest(73), page!.carry);

    expect(older!.page.carry.events).toEqual([
      expect.objectContaining({ event: 'tool_call_result', tool_call_id: 'call-gated' }),
    ]);
  });

  it('carries no result whose call the same page held', async () => {
    const { rt } = buildRuntime();
    const page = await loadNewest(rt, [
      header(73, true),
      ...turn(73),
      { event: 'tool_calls', data: { turn_index: 73, tool_calls: [{ id: 'call-own', name: 'get_quote', args: {} }] } },
      { event: 'tool_call_result', data: { turn_index: 73, tool_call_id: 'call-own', content: 'ok', status: 'success' } },
      DONE,
    ]);
    expect(page!.carry.events).toEqual([]);
  });
});
