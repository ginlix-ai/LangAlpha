/**
 * A stream that drops mid-answer leaves its bubble streaming, and the reconnect
 * resumes the run on a bubble of its own: the rest of the model call, and its
 * finish, land there. The bubble it continues must still settle, or it reads
 * as live after the turn has ended.
 *
 * Real hook internals; only the api module (the transport) is mocked.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';
import { settleMountEffect, threadStatus } from './chatHookHarness';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (k: string) => k }),
}));

vi.mock('@/lib/supabase', () => ({ supabase: null }));

vi.mock('../utils/threadStorage', () => ({
  getStoredThreadId: vi.fn().mockReturnValue(null),
  setStoredThreadId: vi.fn(),
  removeStoredThreadId: vi.fn(),
}));

vi.mock('../../utils/api', async () => (await import('./chatHookHarness')).apiMockModule());

import { getWorkflowStatus, reconnectToWorkflowStream, replayThreadHistory, sendChatMessageStream, sendRetryStream } from '../../utils/api';
import { useChatMessages } from '../useChatMessages';
import type { MessageRecord } from '../utils/types';
import type { StructuredError } from '@/utils/rateLimitError';

const mockStatus = getWorkflowStatus as Mock;
const mockReconnect = reconnectToWorkflowStream as Mock;
const mockReplay = replayThreadHistory as Mock;
const mockSendStream = sendChatMessageStream as Mock;
const mockRetryStream = sendRetryStream as Mock;

type Emit = (e: Record<string, unknown>) => void;

const chunk = (content: string, extra: Record<string, unknown> = {}) => ({
  event: 'message_chunk', role: 'assistant', agent: 'main', content_type: 'text', content, ...extra,
});

/** The user steered the run: the rest of it lands on a continuation bubble. */
const steered = { event: 'steering_delivered', messages: [{ content: 'also this', timestamp: 1_700_000_000 }] };

/** The run's todo list, one step done and one under way. */
const todoUpdate = {
  event: 'artifact',
  artifact_type: 'todo_update',
  artifact_id: 'todos-1',
  payload: {
    todos: [{ content: 'gather', status: 'completed' }, { content: 'write', status: 'in_progress' }],
    total: 2, completed: 1, in_progress: 1, pending: 0,
  },
};

/** The statuses of the open todo list on the bubble showing `text`. */
function todosOn(messages: MessageRecord[], text: string): string[] {
  const bubble = messages.find((m) => m.role === 'assistant' && String(m.content ?? '').includes(text));
  const lists = Object.values((bubble?.todoListProcesses ?? {}) as Record<string, { order?: number; todos: { status: string }[] }>);
  if (lists.length === 0) throw new Error(`no todo list on the bubble showing ${text}`);
  const last = lists.reduce((a, b) => ((a.order || 0) >= (b.order || 0) ? a : b));
  return last.todos.map((t) => t.status);
}

const flush = () => new Promise((r) => setTimeout(r, 0));

async function mount(status = threadStatus()) {
  mockReplay.mockImplementation(async (_tid: string, onEvent: Emit) => {
    onEvent({ event: 'user_message', turn_index: 0, content: 'question 0', run_id: 'run-0' });
    onEvent({ event: 'message_chunk', turn_index: 0, role: 'assistant', agent: 'main', content_type: 'text', content: 'answer 0' });
    onEvent({ event: 'replay_done', thread_id: 'th-1' });
  });
  mockStatus.mockResolvedValue(status);
  const hook = renderHookWithProviders(() => useChatMessages('ws', 'th-1'));
  await settleMountEffect();
  await waitFor(() => expect(hook.result.current.isLoadingHistory).toBe(false));
  return hook;
}

type Hook = Awaited<ReturnType<typeof mount>>;

/** A reconnect that plays `frames` and ends with the run. */
const endsAfter = (frames: Record<string, unknown>[]) =>
  async (_tid: string, _rid: string, _cursor: unknown, onEvent: Emit) => {
    for (const frame of frames) onEvent(frame);
    return { disconnected: false, aborted: false };
  };

/** A reconnect that plays `frames` and stays open, as for a run still going. */
const staysOpenAfter = (frames: Record<string, unknown>[]) =>
  (_tid: string, _rid: string, _cursor: unknown, onEvent: Emit, signal: AbortSignal) =>
    new Promise((resolve) => {
      for (const frame of frames) onEvent(frame);
      onEvent({ event: 'caught_up' });
      signal.addEventListener('abort', () => resolve({ disconnected: false, aborted: true }), { once: true });
    });

/** A reconnect that plays `frames`, then loses its own stream with the run still going. */
const failsAfter = (frames: Record<string, unknown>[], failure: 'throws' | 'drops') =>
  async (_tid: string, _rid: string, _cursor: unknown, onEvent: Emit) => {
    for (const frame of frames) onEvent(frame);
    if (failure === 'throws') throw new Error('network down');
    return { disconnected: true, aborted: false };
  };

type Attempt = (tid: string, rid: string, cursor: unknown, onEvent: Emit, signal: AbortSignal) => Promise<unknown>;

/** One reconnect per attempt, in order. */
const inTurn = (...list: Attempt[]): Attempt => {
  let n = 0;
  return (...args) => list[n++](...args);
};

/** Records when each attempt it wraps opened its stream. */
const clock = () => {
  const calledAt: number[] = [];
  const timed = (attempt: Attempt): Attempt => (...args) => {
    calledAt.push(Date.now());
    return attempt(...args);
  };
  return { calledAt, timed };
};

const running = () => threadStatus({ can_reconnect: true, status: 'running', run_id: 'run-1', latest_turn_index: 1 });

/** Sends a turn whose stream drops mid-answer, then reconnects with `reconnect`. */
async function dropThenReconnect(hook: Hook, reconnect: (...args: never[]) => Promise<unknown>, status = running) {
  mockStatus.mockImplementation(async () => status());
  mockReconnect.mockImplementation(reconnect);
  mockSendStream.mockImplementation(async (_msg: string, _ws: string, _tid: string | null, { onEvent }: { onEvent: Emit }) => {
    onEvent({ event: 'metadata', thread_id: 'th-1', run_id: 'run-1', _eventId: 1 });
    // Mid-sentence: the model call's finish never reaches this stream.
    onEvent({ ...chunk('the first half'), _eventId: 2 });
    return { disconnected: true };
  });
  await act(async () => {
    await hook.result.current.handleSendMessage('go on');
    await flush();
  });
  await waitFor(() => expect(mockReconnect).toHaveBeenCalled());
}

/** Opens the thread mid-turn: its load attaches to the running run with `reconnect`. */
async function openMidTurn(reconnect: Attempt) {
  mockReconnect.mockImplementation(reconnect);
  const hook = await mount(running());
  await waitFor(() => expect(mockReconnect).toHaveBeenCalled());
  return hook;
}

const emptyBubbles = (hook: Hook) =>
  (hook.result.current.liveMessages.get() as MessageRecord[]).filter(
    (m) => m.role === 'assistant' && !m.content && !(m.contentSegments as unknown[] | undefined)?.length,
  );

const ended = () => threadStatus({ can_reconnect: false, status: 'completed', latest_turn_index: 1 });

/**
 * What a give-up left, as the reload it asks for finds it. The reload checks
 * the status before its replay clears the bubbles the give-up handed over, so
 * the last look a status check took is that state.
 */
function watchReload(hook: Hook) {
  let lastSeen: MessageRecord[] = [];
  const atReload: { current: MessageRecord[] | null } = { current: null };
  mockReplay.mockImplementation(async (_tid: string, onEvent: Emit) => {
    atReload.current = lastSeen;
    onEvent({ event: 'replay_done', thread_id: 'th-1' });
  });
  const seeing = <T>(status: () => T) => () => {
    lastSeen = [...(hook.result.current.liveMessages.get() as MessageRecord[])];
    return status();
  };
  return { atReload, seeing };
}

function bubbleShowing(hook: Hook, text: string): MessageRecord {
  const bubble = (hook.result.current.liveMessages.get() as MessageRecord[]).find(
    (m) => m.role === 'assistant' && String(m.content ?? '').includes(text),
  );
  if (!bubble) throw new Error(`no assistant bubble shows ${text}`);
  return bubble;
}

/** The reload's replay: both turns whole, the steer's as a turn of its own. */
const bothTurnsWhole = async (_tid: string, onEvent: Emit) => {
  onEvent({ event: 'user_message', turn_index: 0, content: 'question 0', run_id: 'run-0' });
  onEvent({ ...chunk('answer 0'), turn_index: 0 });
  onEvent({ event: 'user_message', turn_index: 1, content: 'go on', run_id: 'run-1' });
  onEvent({ ...chunk('the first half and then the rest'), turn_index: 1 });
  onEvent({ event: 'user_message', turn_index: 2, content: 'also this', run_id: 'run-2' });
  onEvent({ ...chunk('the next answer'), turn_index: 2 });
  onEvent({ event: 'replay_done', thread_id: 'th-1' });
};

/** The turns as the screen shows them: each user message and answer once. */
function showsEachTurnOnce(hook: Hook) {
  const messages = hook.result.current.liveMessages.get() as MessageRecord[];
  for (const question of ['go on', 'also this']) {
    expect(messages.filter((m) => m.role === 'user' && m.content === question)).toHaveLength(1);
  }
  for (const answer of ['the first half and then the rest', 'the next answer']) {
    expect(messages.filter((m) => m.role === 'assistant' && String(m.content ?? '').includes(answer))).toHaveLength(1);
  }
  // Nothing of the live copy is left beside the replay's.
  expect(messages.filter((m) => /and then/.test(String(m.content ?? '')))).toHaveLength(1);
}

describe('useChatMessages: a reconnect settles the bubble it continues', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('the dropped bubble settles when the reconnected turn ends', async () => {
    const hook = await mount();
    await dropThenReconnect(hook, endsAfter([
      { ...chunk(' and the rest'), _eventId: 3 },
      { ...chunk('', { finish_reason: 'stop' }), _eventId: 4 },
    ]));
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));

    const continued = bubbleShowing(hook, 'the first half');
    expect(continued.isStreaming).toBe(false);
    expect(bubbleShowing(hook, 'and the rest').isStreaming).toBe(false);
    const streaming = (hook.result.current.liveMessages.get() as MessageRecord[]).filter((m) => m.isStreaming);
    expect(streaming).toEqual([]);
  });

  it('the dropped bubble carries the turn end when the reconnect brings no more text', async () => {
    const hook = await mount();
    // The drop came after the last word: only the call's finish is left.
    await dropThenReconnect(hook, endsAfter([{ ...chunk('', { finish_reason: 'stop' }), _eventId: 3 }]));
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));

    const continued = bubbleShowing(hook, 'the first half');
    expect(continued.isStreaming).toBe(false);
    expect(typeof continued.completionObservedAt).toBe('number');
  });

  it('the dropped bubble settles when the user stops the reconnected turn', async () => {
    const hook = await mount();
    await dropThenReconnect(hook, staysOpenAfter([{ ...chunk(' still going'), _eventId: 3 }]));
    await waitFor(() => expect(bubbleShowing(hook, 'still going')).toBeTruthy());

    await act(async () => {
      await hook.result.current.stopWorkflow();
    });

    expect(bubbleShowing(hook, 'the first half').isStreaming).toBe(false);
    // The stop belongs to the turn's tail, the bubble the run was writing to.
    expect(bubbleShowing(hook, 'the first half').stopped).toBeUndefined();
    expect(bubbleShowing(hook, 'still going').stopped).toBe(true);
  });

  it('no bubble stays live when the attach a load opened fails', async () => {
    const hook = await openMidTurn(failsAfter([{ ...chunk('and then'), _eventId: 3 }], 'throws'));
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));

    const streaming = (hook.result.current.liveMessages.get() as MessageRecord[]).filter((m) => m.isStreaming);
    expect(streaming).toEqual([]);
    // The stream failed, which says nothing of the run: the bubble claims
    // neither that the turn ended nor that it failed, and the banner says the
    // view lost the reply, in its own words.
    expect(bubbleShowing(hook, 'and then').completionObservedAt).toBeUndefined();
    expect(bubbleShowing(hook, 'and then').error).toBeUndefined();
    expect(hook.result.current.messageError).toEqual({ message: 'chat.reconnectFailed', kind: 'notice' });
  });

  it('an attach a load opened that drops is retried, and the turn ends on the retry', async () => {
    const hook = await openMidTurn(inTurn(
      failsAfter([{ ...chunk('and then'), _eventId: 3 }], 'drops'),
      endsAfter([
        { ...chunk(' the end'), _eventId: 4 },
        { ...chunk('', { finish_reason: 'stop' }), _eventId: 5 },
      ]),
    ));
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false), { timeout: 3000 });

    expect(mockReconnect).toHaveBeenCalledTimes(2);
    // The retry resumes the same run after what the attach delivered.
    expect(mockReconnect.mock.calls[1][1]).toBe('run-1');
    expect(mockReconnect.mock.calls[1][2]).toBe(3);
    expect(hook.result.current.messageError).toBeNull();
    const streaming = (hook.result.current.liveMessages.get() as MessageRecord[]).filter((m) => m.isStreaming);
    expect(streaming).toEqual([]);
    expect(typeof bubbleShowing(hook, 'the end').completionObservedAt).toBe('number');
  });

  it('a retry that cannot connect is one failed attempt, and the next one ends the turn', async () => {
    const hook = await mount();
    const { calledAt, timed } = clock();
    await dropThenReconnect(hook, inTurn(
      timed(async () => {
        throw new TypeError('Failed to fetch');
      }),
      timed(endsAfter([
        { ...chunk(' the end'), _eventId: 3 },
        { ...chunk('', { finish_reason: 'stop' }), _eventId: 4 },
      ])),
    ));
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false), { timeout: 3000 });

    expect(mockReconnect).toHaveBeenCalledTimes(2);
    // The failed attempt is charged the loop's backoff before the next one.
    expect(calledAt[1] - calledAt[0]).toBeGreaterThanOrEqual(900);
    expect(hook.result.current.messageError).toBeNull();
    // It leaves no bubble, and the retry continues the turn the drop left.
    expect(emptyBubbles(hook)).toEqual([]);
    expect(bubbleShowing(hook, 'the first half').turnIndex).toBe(1);
    expect(bubbleShowing(hook, 'the end').turnIndex).toBe(1);
    expect(typeof bubbleShowing(hook, 'the end').completionObservedAt).toBe('number');
  });

  it('a retry the tab resumes before it hears anything continues the turn the drop left', async () => {
    Object.defineProperty(document, 'visibilityState', { configurable: true, get: () => 'visible' });
    const hook = await mount();
    // Open and silent, as a reader a suspended tab left behind.
    const quiet: Attempt = (_tid, _rid, _cursor, _onEvent, signal) =>
      new Promise((resolve) => {
        signal.addEventListener('abort', () => resolve({ disconnected: false, aborted: true }), { once: true });
      });
    await dropThenReconnect(hook, inTurn(
      quiet,
      endsAfter([
        { ...chunk(' the end'), _eventId: 3 },
        { ...chunk('', { finish_reason: 'stop' }), _eventId: 4 },
      ]),
    ));
    await act(async () => {
      window.dispatchEvent(new Event('pagehide'));
      document.dispatchEvent(new Event('visibilitychange'));
      await flush();
    });
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false), { timeout: 3000 });

    expect(mockReconnect).toHaveBeenCalledTimes(2);
    expect(emptyBubbles(hook)).toEqual([]);
    expect(bubbleShowing(hook, 'the first half').turnIndex).toBe(1);
    expect(bubbleShowing(hook, 'the end').turnIndex).toBe(1);
    // The same run resumes after what the send delivered.
    expect(mockReconnect.mock.calls[1][1]).toBe('run-1');
    expect(mockReconnect.mock.calls[1][2]).toBe(2);
  });

  it.each([401, 403])('a retry the server refuses with %i ends with the banner, unretried', async (status) => {
    const hook = await mount();
    await dropThenReconnect(hook, async () => {
      throw Object.assign(new Error('Not authenticated'), { status });
    });
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));

    expect(mockReconnect).toHaveBeenCalledTimes(1);
    expect(hook.result.current.messageError).toEqual({ message: 'chat.reconnectSignedOut', kind: 'notice' });
    // Nothing reloads the transcript over the error.
    expect(mockReplay).toHaveBeenCalledTimes(1);
    const streaming = (hook.result.current.liveMessages.get() as MessageRecord[]).filter((m) => m.isStreaming);
    expect(streaming).toEqual([]);
  });

  it('a retry the server rate-limits waits as long as it asks before the next one', async () => {
    const hook = await mount();
    const { calledAt, timed } = clock();
    await dropThenReconnect(hook, inTurn(
      timed(async () => {
        throw Object.assign(new Error('Rate limit exceeded'), { status: 429, retryAfter: 2 });
      }),
      timed(endsAfter([
        { ...chunk(' the end'), _eventId: 3 },
        { ...chunk('', { finish_reason: 'stop' }), _eventId: 4 },
      ])),
    ));
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false), { timeout: 4000 });

    expect(mockReconnect).toHaveBeenCalledTimes(2);
    // Two seconds as asked, not the one the backoff would take.
    expect(calledAt[1] - calledAt[0]).toBeGreaterThanOrEqual(1900);
    expect(hook.result.current.messageError).toBeNull();
  });

  it('a retry the server has no run for gives up the way a finished run does', async () => {
    const hook = await mount();
    let expired = false;
    await dropThenReconnect(
      hook,
      async () => {
        expired = true;
        // The server's own detail text, not the transport's generic message.
        throw Object.assign(new Error('Run finished and its event stream expired.'), { status: 410 });
      },
      () => (expired ? ended() : running()),
    );
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));

    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    expect(mockReconnect).toHaveBeenCalledTimes(1);
    expect(hook.result.current.messageError).toBeNull();
  });

  it('a reconnect that drops too is retried, and the turn ends on the retry', async () => {
    const hook = await mount();
    const { calledAt, timed } = clock();
    await dropThenReconnect(hook, inTurn(
      timed(failsAfter([{ ...chunk(' and then'), _eventId: 3 }], 'drops')),
      timed(endsAfter([
        { ...chunk(' the end'), _eventId: 4 },
        { ...chunk('', { finish_reason: 'stop' }), _eventId: 5 },
      ])),
    ));
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false), { timeout: 3000 });

    expect(mockReconnect).toHaveBeenCalledTimes(2);
    // A drop that made progress keeps the full allowance but still waits
    // before reopening, so a link that drops after every event cannot spin.
    expect(calledAt[1] - calledAt[0]).toBeGreaterThanOrEqual(900);
    // The retry resumes after what the dropped attempt delivered.
    expect(mockReconnect.mock.calls[1][2]).toBe(3);
    const streaming = (hook.result.current.liveMessages.get() as MessageRecord[]).filter((m) => m.isStreaming);
    expect(streaming).toEqual([]);
    expect(typeof bubbleShowing(hook, 'the end').completionObservedAt).toBe('number');
    expect(hook.result.current.messageError).toBeNull();
  });

  it('a reconnect that drops after its run ended gives up the way a first drop does', async () => {
    const hook = await mount();
    let reconnectDropped = false;
    const dropsOnce: Attempt = failsAfter([{ ...chunk(' and then'), _eventId: 3 }], 'drops');
    await dropThenReconnect(
      hook,
      async (...args: Parameters<Attempt>) => {
        const result = await dropsOnce(...args);
        reconnectDropped = true;
        return result;
      },
      () => (reconnectDropped ? ended() : running()),
    );
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false), { timeout: 3000 });

    // The give-up reloads the transcript, rather than leaving the drop as an error.
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    expect(mockReconnect).toHaveBeenCalledTimes(1);
    expect(hook.result.current.messageError).toBeNull();
  });

  it('giving up closes the todo list a dropped reconnect was keeping', async () => {
    const hook = await mount();
    // The give-up reloads; what it left is what the reload starts from.
    const { atReload, seeing } = watchReload(hook);
    let reconnectDropped = false;
    const dropsOnce: Attempt = failsAfter([{ ...todoUpdate, _eventId: 3 }, { ...chunk(' and then'), _eventId: 4 }], 'drops');
    await dropThenReconnect(
      hook,
      async (...args: Parameters<Attempt>) => {
        const result = await dropsOnce(...args);
        reconnectDropped = true;
        return result;
      },
      seeing(() => (reconnectDropped ? ended() : running())),
    );
    await waitFor(() => expect(atReload.current).not.toBeNull(), { timeout: 3000 });

    const keeper = atReload.current!.find((m) => String(m.content ?? '').includes('and then'));
    const lists = Object.values((keeper?.todoListProcesses ?? {}) as Record<string, { todos: { status: string }[] }>);
    expect(lists).toHaveLength(1);
    expect(lists[0].todos.map((t) => t.status)).toEqual(['completed', 'stale']);
  });

  it('a turn a reconnect ends closes the todo list on every bubble it wrote to', async () => {
    const hook = await mount();
    mockStatus.mockImplementation(async () => running());
    mockReconnect.mockImplementation(endsAfter([
      { ...chunk(' then more'), _eventId: 4 },
      { ...todoUpdate, _eventId: 5 },
      { ...steered, _eventId: 6 },
      { ...chunk('after the steer'), _eventId: 7 },
      { ...todoUpdate, _eventId: 8 },
      { ...chunk('', { finish_reason: 'stop' }), _eventId: 9 },
    ]));
    mockSendStream.mockImplementation(async (_msg: string, _ws: string, _tid: string | null, { onEvent }: { onEvent: Emit }) => {
      onEvent({ event: 'metadata', thread_id: 'th-1', run_id: 'run-1', _eventId: 1 });
      onEvent({ ...chunk('the first half'), _eventId: 2 });
      onEvent({ ...todoUpdate, _eventId: 3 });
      return { disconnected: true };
    });
    await act(async () => {
      await hook.result.current.handleSendMessage('go on');
      await flush();
    });
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));

    const messages = hook.result.current.liveMessages.get() as MessageRecord[];
    // The bubble it continued, its own, and the steer's continuation.
    for (const text of ['the first half', 'then more', 'after the steer']) {
      expect(todosOn(messages, text)).toEqual(['completed', 'stale']);
    }
  });

  const secondRetry = {
    ends: endsAfter([
      { ...chunk(' the end'), _eventId: 5 },
      { ...chunk('', { finish_reason: 'stop' }), _eventId: 6 },
    ]),
    'is refused': async () => {
      throw Object.assign(new Error('Not authenticated'), { status: 401 });
    },
  };

  it.each(Object.keys(secondRetry) as (keyof typeof secondRetry)[])(
    'a turn whose second reconnect %s closes the todo list the send left',
    async (outcome) => {
      const hook = await mount();
      mockStatus.mockImplementation(async () => running());
      mockReconnect.mockImplementation(inTurn(
        failsAfter([{ ...chunk(' and then'), _eventId: 4 }], 'drops'),
        secondRetry[outcome],
      ));
      mockSendStream.mockImplementation(async (_msg: string, _ws: string, _tid: string | null, { onEvent }: { onEvent: Emit }) => {
        onEvent({ event: 'metadata', thread_id: 'th-1', run_id: 'run-1', _eventId: 1 });
        onEvent({ ...chunk('the first half'), _eventId: 2 });
        onEvent({ ...todoUpdate, _eventId: 3 });
        return { disconnected: true };
      });
      await act(async () => {
        await hook.result.current.handleSendMessage('go on');
        await flush();
      });
      await waitFor(() => expect(hook.result.current.isLoading).toBe(false), { timeout: 3000 });

      expect(mockReconnect).toHaveBeenCalledTimes(2);
      // Two hops back from the reconnect that ended the turn.
      expect(todosOn(hook.result.current.liveMessages.get() as MessageRecord[], 'the first half')).toEqual(['completed', 'stale']);
    },
  );

  it('giving up closes the todo list on every bubble a dropped reconnect wrote to', async () => {
    const hook = await mount();
    const { atReload, seeing } = watchReload(hook);
    let reconnectDropped = false;
    // The list is on the reconnect's own bubble; a steer moved it on before the drop.
    const dropsOnce: Attempt = failsAfter([
      { ...chunk(' then more'), _eventId: 3 },
      { ...todoUpdate, _eventId: 4 },
      { ...steered, _eventId: 5 },
      { ...chunk('after the steer'), _eventId: 6 },
    ], 'drops');
    await dropThenReconnect(
      hook,
      async (...args: Parameters<Attempt>) => {
        const result = await dropsOnce(...args);
        reconnectDropped = true;
        return result;
      },
      seeing(() => (reconnectDropped ? ended() : running())),
    );
    await waitFor(() => expect(atReload.current).not.toBeNull(), { timeout: 3000 });

    expect(todosOn(atReload.current!, 'then more')).toEqual(['completed', 'stale']);
  });

  it('giving up on a steered run that ended during the drop settles its continuation, though the reload fails', async () => {
    const hook = await mount();
    mockStatus.mockImplementation(async () => ended());
    mockSendStream.mockImplementationOnce(async (_msg: string, _ws: string, _tid: string | null, { onEvent }: { onEvent: Emit }) => {
      onEvent({ event: 'metadata', thread_id: 'th-1', run_id: 'run-1', turn_index: 1, _eventId: 1 });
      onEvent({ ...chunk('the answer'), _eventId: 2 });
      onEvent({ ...steered, _eventId: 3 });
      onEvent({ ...chunk('after the steer'), _eventId: 4 });
      return { disconnected: true };
    });
    // The reload the give-up asks for fails, which leaves the bubbles as the give-up did.
    mockReplay.mockImplementation(async () => {
      throw new Error('HTTP error! status: 503');
    });
    await act(async () => {
      await hook.result.current.handleSendMessage('go on');
      await flush();
    });
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(hook.result.current.isLoadingHistory).toBe(false));

    expect(hook.result.current.isLoading).toBe(false);
    expect(bubbleShowing(hook, 'after the steer').isStreaming).toBe(false);
    const streaming = (hook.result.current.liveMessages.get() as MessageRecord[]).filter((m) => m.isStreaming);
    expect(streaming).toEqual([]);
  });

  it('a stop during the retry\'s status check leaves the stopped turn alone', async () => {
    const hook = await mount();
    let answer: (status: unknown) => void = () => {};
    let checking = false;
    mockStatus.mockImplementation(() => new Promise((resolve) => {
      checking = true;
      answer = resolve;
    }));
    mockSendStream.mockImplementation(async (_msg: string, _ws: string, _tid: string | null, { onEvent }: { onEvent: Emit }) => {
      onEvent({ event: 'metadata', thread_id: 'th-1', run_id: 'run-1', _eventId: 1 });
      onEvent({ ...chunk('the first half'), _eventId: 2 });
      return { disconnected: true };
    });
    await act(async () => {
      await hook.result.current.handleSendMessage('go on');
      await flush();
    });
    await waitFor(() => expect(checking).toBe(true));

    await act(async () => {
      await hook.result.current.stopWorkflow();
    });
    // The run ended under the stop, which is what the check then reports.
    mockStatus.mockResolvedValue(ended());
    await act(async () => {
      answer(ended());
      await new Promise((r) => setTimeout(r, 50));
    });

    // The stop tore the turn down already; giving up on top would reload it.
    expect(mockReplay).toHaveBeenCalledTimes(1);
    expect(mockReconnect).not.toHaveBeenCalled();
    expect(bubbleShowing(hook, 'the first half').stopped).toBe(true);
  });

  it('a steer during the reconnect ends the turn on its continuation', async () => {
    const hook = await mount();
    await dropThenReconnect(hook, endsAfter([
      { ...chunk(' and then'), _eventId: 3 },
      { ...steered, _eventId: 4 },
      { ...chunk('after the steer'), _eventId: 5 },
      { ...chunk('', { finish_reason: 'stop' }), _eventId: 6 },
    ]));
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));

    const continuation = bubbleShowing(hook, 'after the steer');
    expect(continuation.isSteering).toBe(true);
    expect(continuation.isStreaming).toBe(false);
    // The bubble the run was writing when it ended is the turn's tail.
    expect(typeof continuation.completionObservedAt).toBe('number');
  });

  it('a steer during a load\'s attach leaves no bubble live when the attach fails', async () => {
    const hook = await openMidTurn(failsAfter([
      { ...chunk('and then'), _eventId: 3 },
      { ...steered, _eventId: 4 },
      { ...chunk('after the steer'), _eventId: 5 },
    ], 'throws'));
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));

    expect(bubbleShowing(hook, 'after the steer').isSteering).toBe(true);
    const streaming = (hook.result.current.liveMessages.get() as MessageRecord[]).filter((m) => m.isStreaming);
    expect(streaming).toEqual([]);
  });

  it('a tab resumed again and again during a quiet stretch costs no retries', async () => {
    Object.defineProperty(document, 'visibilityState', { configurable: true, get: () => 'visible' });
    const hook = await mount();
    // A long tool call writes nothing, and every resume cuts the open reader.
    const quiet: Attempt = (_tid, _rid, _cursor, _onEvent, signal) =>
      new Promise((resolve) => {
        signal.addEventListener('abort', () => resolve({ disconnected: false, aborted: true }), { once: true });
      });
    const resumes = 6;
    await dropThenReconnect(hook, inTurn(
      ...Array.from({ length: resumes }, () => quiet),
      endsAfter([
        { ...chunk(' the end'), _eventId: 3 },
        { ...chunk('', { finish_reason: 'stop' }), _eventId: 4 },
      ]),
    ));
    for (let n = 1; n <= resumes; n++) {
      // Each reader opens at once: a resume is not a failure to wait out.
      await waitFor(() => expect(mockReconnect).toHaveBeenCalledTimes(n), { timeout: 800 });
      await act(async () => {
        window.dispatchEvent(new Event('pagehide'));
        document.dispatchEvent(new Event('visibilitychange'));
        await flush();
      });
    }
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false), { timeout: 800 });

    // More resumes than the loop has attempts, and still no give-up reload.
    expect(mockReconnect).toHaveBeenCalledTimes(resumes + 1);
    expect(mockReplay).toHaveBeenCalledTimes(1);
    expect(hook.result.current.messageError).toBeNull();
    expect(typeof bubbleShowing(hook, 'the end').completionObservedAt).toBe('number');
  });

  it('a steer the ended run refused while a retry waits goes out once the retry lets the turn go, then reloads it whole', async () => {
    const hook = await mount();
    // The reconnect drops after more text; the retry waits a second to reopen.
    await dropThenReconnect(hook, failsAfter([{ ...chunk(' and then'), _eventId: 3 }], 'drops'));
    await waitFor(() => expect(bubbleShowing(hook, 'and then')).toBeTruthy());
    // The run ended in the gap, so the server refuses the steer.
    mockStatus.mockImplementation(async () => threadStatus({ can_reconnect: false, status: 'completed', latest_turn_index: 1 }));
    mockSendStream.mockImplementation(async (_msg: string, _ws: string, _tid: string | null, opts: { onEvent: Emit; steerOnly?: boolean }) => {
      if (opts.steerOnly) {
        throw Object.assign(new Error('The turn has ended.'), {
          status: 409,
          errorInfo: { code: 'not_running', message: 'The turn has ended.' },
        });
      }
      mockStatus.mockImplementation(async () => threadStatus({ can_reconnect: false, status: 'completed', latest_turn_index: 2 }));
      opts.onEvent({ event: 'metadata', thread_id: 'th-1', run_id: 'run-2', turn_index: 2, _eventId: 1 });
      opts.onEvent({ ...chunk('the next answer'), _eventId: 2 });
      opts.onEvent({ ...chunk('', { finish_reason: 'stop' }), _eventId: 3 });
      return { disconnected: false };
    });
    mockReplay.mockImplementation(async (tid: string, onEvent: Emit) => {
      if (mockSendStream.mock.calls.length >= 3) return bothTurnsWhole(tid, onEvent);
      onEvent({ event: 'user_message', turn_index: 0, content: 'question 0', run_id: 'run-0' });
      onEvent({ ...chunk('answer 0'), turn_index: 0 });
      onEvent({ event: 'user_message', turn_index: 1, content: 'go on', run_id: 'run-1' });
      onEvent({ ...chunk('the first half and then the rest'), turn_index: 1 });
      onEvent({ event: 'replay_done', thread_id: 'th-1' });
    });
    await act(async () => {
      await hook.result.current.handleSendMessage('also this');
      await flush();
    });
    // Refused, it waits on screen for the retry.
    expect(mockSendStream).toHaveBeenCalledTimes(2);
    expect(hook.result.current.isLoading).toBe(true);
    // Past the retry's wait, when it finds the run ended.
    await act(async () => {
      await new Promise((r) => setTimeout(r, 1200));
    });
    await waitFor(() => expect(mockSendStream).toHaveBeenCalledTimes(3));
    const [, steer, resend] = mockSendStream.mock.calls.map((call) => call[3] as { requestKey: string; steerOnly?: boolean });
    expect(steer.steerOnly).toBe(true);
    expect(resend.steerOnly).toBeFalsy();
    expect(resend.requestKey).toBe(steer.requestKey);
    await waitFor(() => expect(bubbleShowing(hook, 'the next answer').isStreaming).toBe(false));
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));
    await waitFor(() => expect(hook.result.current.isLoadingHistory).toBe(false));

    expect(mockReconnect).toHaveBeenCalledTimes(1);
    expect(hook.result.current.isReconnecting).toBe(false);
    showsEachTurnOnce(hook);
  });

  it('a reload the retry asks for waits on a steer the server has not answered', async () => {
    const hook = await mount();
    await dropThenReconnect(hook, failsAfter([{ ...chunk(' and then'), _eventId: 3 }], 'drops'));
    await waitFor(() => expect(bubbleShowing(hook, 'and then')).toBeTruthy());
    // The retry finds the run over and gives up; by the reload's look, the
    // steer has opened the next run, which its POST then streams itself.
    let answered = false;
    let checks = 0;
    mockStatus.mockImplementation(async () => {
      checks += 1;
      if (answered || checks === 1) return threadStatus({ can_reconnect: false, status: 'completed', latest_turn_index: answered ? 2 : 1 });
      return threadStatus({ can_reconnect: true, status: 'running', run_id: 'run-2', latest_turn_index: 2 });
    });
    const nextRun = [
      { event: 'metadata', thread_id: 'th-1', run_id: 'run-2', turn_index: 2, _eventId: 1 },
      { ...chunk('the next answer'), _eventId: 2 },
      { ...chunk('', { finish_reason: 'stop' }), _eventId: 3 },
    ];
    mockReconnect.mockImplementation(endsAfter(nextRun));
    let answer: () => void = () => {};
    mockSendStream.mockImplementation((_msg: string, _ws: string, _tid: string | null, { onEvent }: { onEvent: Emit }) =>
      new Promise((resolve) => {
        answer = () => {
          for (const frame of nextRun) onEvent(frame);
          resolve({ disconnected: false });
        };
      }));
    mockReplay.mockImplementation(bothTurnsWhole);
    await act(async () => {
      void hook.result.current.handleSendMessage('also this');
      await flush();
    });
    // Past the retry's wait and its give-up.
    await act(async () => {
      await new Promise((r) => setTimeout(r, 1200));
    });
    expect(mockReconnect.mock.calls.filter((c) => c[1] === 'run-2')).toEqual([]);

    await act(async () => {
      answered = true;
      answer();
      await flush();
    });
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(hook.result.current.isLoadingHistory).toBe(false));

    expect(mockReconnect.mock.calls.filter((c) => c[1] === 'run-2')).toEqual([]);
    showsEachTurnOnce(hook);
  });

  it('a reload that waited on a steer runs when the steer is refused, and keeps its question', async () => {
    const hook = await mount();
    await dropThenReconnect(hook, failsAfter([{ ...chunk(' and then'), _eventId: 3 }], 'drops'));
    await waitFor(() => expect(bubbleShowing(hook, 'and then')).toBeTruthy());
    mockStatus.mockImplementation(async () => ended());
    let refuse: () => void = () => {};
    mockSendStream.mockImplementation(() => new Promise((_resolve, reject) => {
      refuse = () => reject(new Error('refused'));
    }));
    mockReplay.mockImplementation(async (_tid: string, onEvent: Emit) => {
      onEvent({ event: 'user_message', turn_index: 0, content: 'question 0', run_id: 'run-0' });
      onEvent({ ...chunk('answer 0'), turn_index: 0 });
      onEvent({ event: 'user_message', turn_index: 1, content: 'go on', run_id: 'run-1' });
      onEvent({ ...chunk('the first half and then the rest'), turn_index: 1 });
      onEvent({ event: 'replay_done', thread_id: 'th-1' });
    });
    await act(async () => {
      void hook.result.current.handleSendMessage('also this');
      await flush();
    });
    await act(async () => {
      await new Promise((r) => setTimeout(r, 1200));
    });
    expect(mockReplay).toHaveBeenCalledTimes(1);

    await act(async () => {
      refuse();
      await flush();
    });
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(hook.result.current.isLoadingHistory).toBe(false));

    const messages = hook.result.current.liveMessages.get() as MessageRecord[];
    expect(messages.filter((m) => /and then/.test(String(m.content ?? '')))).toHaveLength(1);
    expect(bubbleShowing(hook, 'the first half and then the rest')).toBeTruthy();
    // The give-up marked the steer persisted while it was out; the server
    // never stored it, so it stays, with its error, below the replayed turns.
    const steer = messages.filter((m) => m.role === 'user' && m.content === 'also this');
    expect(steer).toHaveLength(1);
    expect(steer[0].queueError).toBe('Failed to send message');
    expect(messages.at(-1)).toBe(steer[0]);
    // The reload cleared the banner as it started; the refusal shows after it.
    expect(hook.result.current.messageError).toBe('Failed to send message');
  });

  /** A give-up whose reload waits on a steer, which the server then refuses. */
  async function refusedWhileReloadWaits(hook: Hook) {
    await dropThenReconnect(hook, failsAfter([{ ...chunk(' and then'), _eventId: 3 }], 'drops'));
    await waitFor(() => expect(bubbleShowing(hook, 'and then')).toBeTruthy());
    mockStatus.mockImplementation(async () => ended());
    let refuse: () => void = () => {};
    mockSendStream.mockImplementationOnce(() => new Promise((_resolve, reject) => {
      refuse = () => reject(new Error('refused'));
    }));
    await act(async () => {
      void hook.result.current.handleSendMessage('also this');
      await flush();
    });
    await act(async () => {
      await new Promise((r) => setTimeout(r, 1200));
    });
    expect(mockReplay).toHaveBeenCalledTimes(1);
    return () => act(async () => {
      refuse();
      await flush();
    });
  }

  it('a refusal the waiting reload could not show, as its replay failed, shows after the next reload', async () => {
    const hook = await mount();
    const refuse = await refusedWhileReloadWaits(hook);
    mockReplay.mockRejectedValueOnce(new Error('crafted 500'));
    await refuse();
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(hook.result.current.isLoadingHistory).toBe(false));
    expect(hook.result.current.messageError).toEqual({ message: 'chat.historyLoadFailed', kind: 'notice' });

    await act(async () => {
      window.dispatchEvent(new Event('online'));
      await flush();
    });
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(3));
    await waitFor(() => expect(hook.result.current.isLoadingHistory).toBe(false));

    expect(hook.result.current.messageError).toBe('Failed to send message');
  });

  it('a refusal whose waiting reload a new turn overtook stays down once that turn has gone through', async () => {
    const hook = await mount();
    const refuse = await refusedWhileReloadWaits(hook);
    // The waiting reload's status check is still out when the user sends.
    let release: () => void = () => {};
    mockStatus.mockImplementationOnce(() => new Promise((resolve) => {
      release = () => resolve(ended());
    }));
    await refuse();
    let finish: () => void = () => {};
    mockSendStream.mockImplementationOnce((_msg: string, _ws: string, _tid: string | null, { onEvent }: { onEvent: Emit }) =>
      new Promise((resolve) => {
        onEvent({ event: 'metadata', thread_id: 'th-1', run_id: 'run-2', turn_index: 2, _eventId: 1 });
        onEvent({ ...chunk('the next answer'), _eventId: 2 });
        finish = () => {
          onEvent({ ...chunk('', { finish_reason: 'stop' }), _eventId: 3 });
          resolve({ disconnected: false });
        };
      }));
    await act(async () => {
      void hook.result.current.handleSendMessage('next question');
      await flush();
    });
    await act(async () => {
      release();
      await flush();
    });
    await act(async () => {
      finish();
      await flush();
    });
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(hook.result.current.isLoadingHistory).toBe(false));

    expect(hook.result.current.messageError).toBeNull();
  });

  it('a steer refused inside a running turn is not marked persisted when the turn ends', async () => {
    const hook = await mount();
    let finish: () => void = () => {};
    mockSendStream.mockImplementationOnce((_msg: string, _ws: string, _tid: string | null, { onEvent }: { onEvent: Emit }) =>
      new Promise((resolve) => {
        onEvent({ event: 'metadata', thread_id: 'th-1', run_id: 'run-1', turn_index: 1, _eventId: 1 });
        onEvent({ ...chunk('the answer'), _eventId: 2 });
        finish = () => {
          onEvent({ ...chunk('', { finish_reason: 'stop' }), _eventId: 3 });
          resolve({ disconnected: false });
        };
      }));
    mockSendStream.mockImplementationOnce(async () => {
      throw Object.assign(new Error('Server error'), { status: 500 });
    });
    await act(async () => {
      void hook.result.current.handleSendMessage('go on');
      await flush();
    });
    await act(async () => {
      await hook.result.current.handleSendMessage('also this');
      await flush();
    });
    await act(async () => {
      finish();
      await flush();
    });
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));

    // A reload clears what is marked, and the server never stored this one.
    const steer = (hook.result.current.liveMessages.get() as MessageRecord[]).find(
      (m) => m.role === 'user' && m.content === 'also this',
    );
    expect(steer?.queueError).toBe('Failed to send message');
    expect(steer?.isHistory).toBeFalsy();
    expect(bubbleShowing(hook, 'the answer').isHistory).toBe(true);
  });

  /** Sends a turn, steers it, has the server refuse the steer with `refusal`, then ends the turn. */
  async function steerRefusedMidTurn(hook: Hook, refusal: Error) {
    let finish: () => void = () => {};
    mockSendStream.mockImplementationOnce((_msg: string, _ws: string, _tid: string | null, { onEvent }: { onEvent: Emit }) =>
      new Promise((resolve) => {
        onEvent({ event: 'metadata', thread_id: 'th-1', run_id: 'run-1', turn_index: 1, _eventId: 1 });
        onEvent({ ...chunk('the answer'), _eventId: 2 });
        finish = () => {
          onEvent({ ...chunk('', { finish_reason: 'stop' }), _eventId: 3 });
          resolve({ disconnected: false });
        };
      }));
    mockSendStream.mockImplementationOnce(async () => {
      throw refusal;
    });
    await act(async () => {
      void hook.result.current.handleSendMessage('go on');
      await flush();
    });
    await act(async () => {
      await hook.result.current.handleSendMessage('also this');
      await flush();
    });
    await act(async () => {
      finish();
      await flush();
    });
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));
  }

  it('a steer refused with a quota denial shows the quota service\'s message, as a send does', async () => {
    const hook = await mount();
    const message = 'Daily credits used up: 50 of 50. They renew at midnight.';
    await steerRefusedMidTurn(hook, Object.assign(new Error(message), {
      status: 429,
      rateLimitInfo: { type: 'daily_credits', message },
    }));

    const banner = hook.result.current.messageError as StructuredError;
    expect(banner.message).toBe(message);
    expect(banner.links?.map((l) => l.label)).toEqual(['Manage plan', 'View usage']);
  });

  it('a steer refused in the server\'s words shows them, as a send does', async () => {
    const hook = await mount();
    const message = 'The event-stream transport is temporarily unreachable; retry shortly.';
    await steerRefusedMidTurn(hook, Object.assign(new Error(message), {
      status: 503,
      errorInfo: { code: 'transport_unavailable', message },
    }));

    expect(hook.result.current.messageError).toBe(message);
  });

  it.each([
    ['a bare server error', Object.assign(new Error('HTTP error! status: 500'), { status: 500 })],
    ['a lost connection', new TypeError('Failed to fetch')],
  ])('a steer that failed on %s says it was not sent, never in the raw text', async (_case, failure) => {
    const hook = await mount();
    await steerRefusedMidTurn(hook, failure);

    expect(hook.result.current.messageError).toBe('Failed to send message');
  });

  it('an attach that drops after a steer closes the todo list on the bubble before the steer', async () => {
    const hook = await openMidTurn(inTurn(
      failsAfter([
        { ...chunk('and then'), _eventId: 3 },
        { ...todoUpdate, _eventId: 4 },
        { ...steered, _eventId: 5 },
        { ...chunk('after the steer'), _eventId: 6 },
      ], 'drops'),
      endsAfter([
        { ...chunk(' the end'), _eventId: 7 },
        { ...chunk('', { finish_reason: 'stop' }), _eventId: 8 },
      ]),
    ));
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false), { timeout: 3000 });

    expect(mockReconnect).toHaveBeenCalledTimes(2);
    // The retry continues the steer's bubble; the list is on the one before it.
    expect(todosOn(hook.result.current.liveMessages.get() as MessageRecord[], 'and then')).toEqual(['completed', 'stale']);
  });

  it('giving up on a run that ended during the drop shows the turn once, as the reload has it', async () => {
    const hook = await mount();
    let reconnectDropped = false;
    const dropsOnce: Attempt = failsAfter([{ ...chunk(' and then'), _eventId: 3 }], 'drops');
    await dropThenReconnect(
      hook,
      async (...args: Parameters<Attempt>) => {
        const result = await dropsOnce(...args);
        reconnectDropped = true;
        return result;
      },
      () => (reconnectDropped ? ended() : running()),
    );
    // The reload's replay has the turn the drop cut off, finished.
    mockReplay.mockImplementation(async (_tid: string, onEvent: Emit) => {
      onEvent({ event: 'user_message', turn_index: 0, content: 'question 0', run_id: 'run-0' });
      onEvent({ ...chunk('answer 0'), turn_index: 0 });
      onEvent({ event: 'user_message', turn_index: 1, content: 'go on', run_id: 'run-1' });
      onEvent({ ...chunk('the whole answer'), turn_index: 1 });
      onEvent({ event: 'replay_done', thread_id: 'th-1' });
    });
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2), { timeout: 3000 });
    await waitFor(() => expect(hook.result.current.isLoadingHistory).toBe(false));

    const messages = hook.result.current.liveMessages.get() as MessageRecord[];
    expect(messages.filter((m) => m.role === 'user' && m.content === 'go on')).toHaveLength(1);
    expect(messages.filter((m) => String(m.content ?? '').includes('the whole answer'))).toHaveLength(1);
    // Nothing of the live copy is left beside it.
    expect(messages.some((m) => /the first half|and then/.test(String(m.content ?? '')))).toBe(false);
  });

  /**
   * Sends a turn and steers it. The server takes the steer, but its POST loses
   * the answer, failing with `lostWith`, and the run delivers the steer before
   * it ends; `order` is which of the two the view sees first.
   */
  async function steerAnswerLost(hook: Hook, order: 'errorFirst' | 'deliveredFirst', lostWith = 'Failed to fetch') {
    let deliver: () => void = () => {};
    let finish: () => void = () => {};
    mockSendStream.mockImplementationOnce((_msg: string, _ws: string, _tid: string | null, { onEvent }: { onEvent: Emit }) =>
      new Promise((resolve) => {
        onEvent({ event: 'metadata', thread_id: 'th-1', run_id: 'run-1', turn_index: 1, _eventId: 1 });
        onEvent({ ...chunk('the answer'), _eventId: 2 });
        deliver = () => onEvent({ ...steered, _eventId: 3 });
        finish = () => {
          onEvent({ ...chunk('after the steer'), _eventId: 4 });
          onEvent({ ...chunk('', { finish_reason: 'stop' }), _eventId: 5 });
          resolve({ disconnected: false });
        };
      }));
    let lose: () => void = () => {};
    mockSendStream.mockImplementationOnce(() => new Promise((_resolve, reject) => {
      lose = () => reject(new TypeError(lostWith));
    }));
    await act(async () => {
      void hook.result.current.handleSendMessage('go on');
      await flush();
    });
    await act(async () => {
      void hook.result.current.handleSendMessage('also this');
      await flush();
    });
    const steps = order === 'errorFirst' ? [() => lose(), () => deliver()] : [() => deliver(), () => lose()];
    for (const step of steps) {
      await act(async () => {
        step();
        await flush();
      });
    }
    await act(async () => {
      finish();
      await flush();
    });
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));
  }

  /** The steered turn as the server stored it, and a turn another tab added after it. */
  const steeredTurnThenAnother = async (_tid: string, onEvent: Emit) => {
    onEvent({ event: 'user_message', turn_index: 0, content: 'question 0', run_id: 'run-0' });
    onEvent({ ...chunk('answer 0'), turn_index: 0 });
    onEvent({ event: 'user_message', turn_index: 1, content: 'go on', run_id: 'run-1' });
    onEvent({ ...chunk('the answer'), turn_index: 1 });
    onEvent({ ...steered, turn_index: 1 });
    onEvent({ ...chunk('after the steer'), turn_index: 1 });
    onEvent({ event: 'user_message', turn_index: 2, content: 'from another tab', run_id: 'run-2' });
    onEvent({ ...chunk('its answer'), turn_index: 2 });
    onEvent({ event: 'replay_done', thread_id: 'th-1' });
  };

  const steerCopies = (hook: Hook) => (hook.result.current.liveMessages.get() as MessageRecord[]).filter(
    (m) => m.role === 'user' && m.content === 'also this',
  );

  it.each([
    ['errorFirst', 'Failed to fetch'],
    ['deliveredFirst', 'Failed to fetch'],
    // A failure with no message of its own: the banner falls back to the send's text.
    ['errorFirst', ''],
  ] as const)(
    'a steer the run delivered though its POST lost the answer shows once, delivered (%s, "%s")',
    async (order, lostWith) => {
      const hook = await mount();
      await steerAnswerLost(hook, order, lostWith);

      const [steer, ...extra] = steerCopies(hook);
      expect(extra).toEqual([]);
      expect(steer.steeringDelivered).toBe(true);
      expect(steer.queueError).toBeUndefined();
      // The server stored it with the turn, so the reload's replay replaces it.
      expect(steer.isHistory).toBe(true);
      // Nor does the banner the lost answer raised outlive the delivery.
      expect(hook.result.current.messageError).toBeNull();

      mockStatus.mockResolvedValue(threadStatus({ can_reconnect: false, status: 'completed', latest_turn_index: 2 }));
      mockReplay.mockImplementation(steeredTurnThenAnother);
      await act(async () => {
        await hook.result.current.reconnectIfStaleRun();
      });
      await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
      await waitFor(() => expect(hook.result.current.isLoadingHistory).toBe(false));
      expect(steerCopies(hook)).toHaveLength(1);
    },
  );

  it('a reload whose replay delivers a steer left failed by a lost answer shows it once', async () => {
    const hook = await mount();
    await dropThenReconnect(hook, failsAfter([{ ...chunk(' and then'), _eventId: 3 }], 'drops'));
    await waitFor(() => expect(bubbleShowing(hook, 'and then')).toBeTruthy());
    // The run ends while the link is down; the steer reached it, and its
    // POST's answer is lost with the link.
    mockStatus.mockImplementation(async () => ended());
    let lose: () => void = () => {};
    mockSendStream.mockImplementation(() => new Promise((_resolve, reject) => {
      lose = () => reject(new TypeError('Failed to fetch'));
    }));
    mockReplay.mockImplementation(async (_tid: string, onEvent: Emit) => {
      onEvent({ event: 'user_message', turn_index: 0, content: 'question 0', run_id: 'run-0' });
      onEvent({ ...chunk('answer 0'), turn_index: 0 });
      onEvent({ event: 'user_message', turn_index: 1, content: 'go on', run_id: 'run-1' });
      onEvent({ ...chunk('the first half and then'), turn_index: 1 });
      onEvent({ ...steered, turn_index: 1 });
      onEvent({ ...chunk('the rest'), turn_index: 1 });
      onEvent({ event: 'replay_done', thread_id: 'th-1' });
    });
    await act(async () => {
      void hook.result.current.handleSendMessage('also this');
      await flush();
    });
    await act(async () => {
      await new Promise((r) => setTimeout(r, 1200));
    });
    await act(async () => {
      lose();
      await flush();
    });
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(hook.result.current.isLoadingHistory).toBe(false));

    const [steer, ...extra] = steerCopies(hook);
    expect(extra).toEqual([]);
    expect(steer.isHistory).toBe(true);
    expect(steer.steeringDelivered).toBe(true);
    expect(steer.queueError).toBeUndefined();
    // The reload carried the lost answer's banner, which the replay answered.
    expect(hook.result.current.messageError).toBeNull();
  });

  // In the server's words: a steer the server gave none for says it was not sent.
  const refusal = () => Object.assign(new Error('crafted steer refusal'), {
    status: 409,
    errorInfo: { message: 'crafted steer refusal' },
  });

  /**
   * Sends a turn and steers it twice with the same text, failing the POSTs in
   * the order `failures` gives: one loses its answer after the server took the
   * steer, the other is refused. Then the run delivers the steer it took.
   */
  async function sameSteerLostAndRefused(hook: Hook, failures: readonly ('lost' | 'refused')[]) {
    let deliver: () => void = () => {};
    let finish: () => void = () => {};
    mockSendStream.mockImplementationOnce((_msg: string, _ws: string, _tid: string | null, { onEvent }: { onEvent: Emit }) =>
      new Promise((resolve) => {
        onEvent({ event: 'metadata', thread_id: 'th-1', run_id: 'run-1', turn_index: 1, _eventId: 1 });
        onEvent({ ...chunk('the answer'), _eventId: 2 });
        deliver = () => onEvent({ ...steered, _eventId: 3 });
        finish = () => {
          onEvent({ ...chunk('after the steer'), _eventId: 4 });
          onEvent({ ...chunk('', { finish_reason: 'stop' }), _eventId: 5 });
          resolve({ disconnected: false });
        };
      }));
    for (const failure of failures) {
      mockSendStream.mockImplementationOnce(async () => {
        throw failure === 'lost' ? new TypeError('Failed to fetch') : refusal();
      });
    }
    await act(async () => {
      void hook.result.current.handleSendMessage('go on');
      await flush();
    });
    for (let i = 0; i < failures.length; i++) {
      await act(async () => {
        await hook.result.current.handleSendMessage('also this');
        await flush();
      });
    }
    for (const step of [() => deliver(), () => finish()]) {
      await act(async () => {
        step();
        await flush();
      });
    }
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));
  }

  // The banner is the last failure's, and the delivery clears it only when it
  // answers that failure.
  it.each([
    [['lost', 'refused'], 'crafted steer refusal'],
    [['refused', 'lost'], null],
  ] as const)(
    'a delivery takes the steer whose POST lost its answer, never one the server refused (%j)',
    async (failures, banner) => {
      const hook = await mount();
      await sameSteerLostAndRefused(hook, failures);

      const copies = steerCopies(hook);
      expect(copies).toHaveLength(2);
      const lost = copies[failures.indexOf('lost')];
      const refused = copies[failures.indexOf('refused')];
      expect(lost.steeringDelivered).toBe(true);
      expect(lost.queueError).toBeUndefined();
      expect(refused.steeringDelivered).toBeFalsy();
      expect(refused).toMatchObject({ queueError: 'crafted steer refusal' });
      expect(refused.isHistory).toBeFalsy();
      expect(hook.result.current.messageError).toBe(banner);

      // The reload replaces the delivered one and keeps the refusal.
      mockStatus.mockResolvedValue(threadStatus({ can_reconnect: false, status: 'completed', latest_turn_index: 2 }));
      mockReplay.mockImplementation(steeredTurnThenAnother);
      await act(async () => {
        await hook.result.current.reconnectIfStaleRun();
      });
      await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
      await waitFor(() => expect(hook.result.current.isLoadingHistory).toBe(false));
      const after = steerCopies(hook);
      expect(after.map((m) => m.queueError ?? null)).toEqual([null, 'crafted steer refusal']);
      expect(after[0].steeringDelivered).toBe(true);
    },
  );

  it('of two steers whose POSTs lost their answers a delivery takes the older, as the queue delivers in order', async () => {
    const hook = await mount();
    await sameSteerLostAndRefused(hook, ['lost', 'lost']);

    const [older, newer, ...extra] = steerCopies(hook);
    expect(extra).toEqual([]);
    expect(older.steeringDelivered).toBe(true);
    expect(older.queueError).toBeUndefined();
    expect(newer).toMatchObject({ queueError: 'Failed to send message' });
  });

  it('a steer refused after the run delivered another keeps its refusal', async () => {
    const hook = await mount();
    let deliver: () => void = () => {};
    let finish: () => void = () => {};
    mockSendStream.mockImplementationOnce((_msg: string, _ws: string, _tid: string | null, { onEvent }: { onEvent: Emit }) =>
      new Promise((resolve) => {
        onEvent({ event: 'metadata', thread_id: 'th-1', run_id: 'run-1', turn_index: 1, _eventId: 1 });
        onEvent({ ...chunk('the answer'), _eventId: 2 });
        deliver = () => onEvent({ ...steered, messages: [{ content: 'and that', timestamp: 1_700_000_000 }], _eventId: 3 });
        finish = () => {
          onEvent({ ...chunk('', { finish_reason: 'stop' }), _eventId: 4 });
          resolve({ disconnected: false });
        };
      }));
    // The first steer is taken; the second is out when the run delivers the first.
    mockSendStream.mockImplementationOnce(async (_msg: string, _ws: string, _tid: string | null, { onEvent }: { onEvent: Emit }) => {
      onEvent({ event: 'steering_accepted', thread_id: 'th-1', content: 'and that', position: 1 });
      return { disconnected: false };
    });
    let refuse: () => void = () => {};
    mockSendStream.mockImplementationOnce(() => new Promise((_resolve, reject) => {
      refuse = () => reject(refusal());
    }));
    await act(async () => {
      void hook.result.current.handleSendMessage('go on');
      await flush();
    });
    await act(async () => {
      await hook.result.current.handleSendMessage('and that');
      await flush();
    });
    await act(async () => {
      void hook.result.current.handleSendMessage('also this');
      await flush();
    });
    for (const step of [() => deliver(), () => refuse(), () => finish()]) {
      await act(async () => {
        step();
        await flush();
      });
    }
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));

    const [steer, ...extra] = steerCopies(hook);
    expect(extra).toEqual([]);
    expect(steer.steeringDelivered).toBeFalsy();
    expect(steer).toMatchObject({ queueError: 'crafted steer refusal' });
    expect(steer.isHistory).toBeFalsy();
    expect(hook.result.current.messageError).toBe('crafted steer refusal');
  });

  /** A turn that runs until `finish` is called, and is told apart by its run. */
  function runningTurn(runId: string, turnIndex: number, answer: string, extra: (onEvent: Emit) => void = () => {}) {
    const turn = { finish: () => {}, deliver: () => {} };
    mockSendStream.mockImplementationOnce((_msg: string, _ws: string, _tid: string | null, { onEvent }: { onEvent: Emit }) =>
      new Promise((resolve) => {
        onEvent({ event: 'metadata', thread_id: 'th-1', run_id: runId, turn_index: turnIndex, _eventId: 1 });
        onEvent({ ...chunk(answer), _eventId: 2 });
        extra(onEvent);
        turn.deliver = () => onEvent({ ...steered, _eventId: 3 });
        turn.finish = () => {
          onEvent({ ...chunk('', { finish_reason: 'stop' }), _eventId: 4 });
          resolve({ disconnected: false });
        };
      }));
    return turn;
  }

  async function step(fn: () => unknown) {
    await act(async () => {
      fn();
      await flush();
    });
  }

  // The server delivers a steer only to the run it was sent to, and hands back
  // at that run's end any it did not deliver, so a steer an earlier turn left
  // is never the one a later turn's delivery names.
  it.each(['failed', 'pending'] as const)(
    'a delivery takes the steered turn\'s copy of its text, never one an earlier turn left %s',
    async (left) => {
      const hook = await mount();
      const first = runningTurn('run-1', 1, 'the answer');
      mockSendStream.mockImplementationOnce(left === 'failed'
        ? async () => {
          throw Object.assign(new Error('Bad Gateway'), { status: 502 });
        }
        : async (_msg: string, _ws: string, _tid: string | null, { onEvent }: { onEvent: Emit }) => {
          onEvent({ event: 'steering_accepted', thread_id: 'th-1', content: 'also this', position: 1 });
          return { disconnected: false };
        });
      await step(() => void hook.result.current.handleSendMessage('go on'));
      await step(() => void hook.result.current.handleSendMessage('also this'));
      await step(() => first.finish());
      await waitFor(() => expect(hook.result.current.isLoading).toBe(false));

      // The next turn steers with the same text, and the POST's answer is lost.
      const second = runningTurn('run-2', 2, 'the next answer');
      mockSendStream.mockImplementationOnce(async () => {
        throw new TypeError('Failed to fetch');
      });
      await step(() => void hook.result.current.handleSendMessage('next question'));
      await step(() => void hook.result.current.handleSendMessage('also this'));
      await step(() => second.deliver());
      await step(() => second.finish());
      await waitFor(() => expect(hook.result.current.isLoading).toBe(false));

      const [older, newer, ...extra] = steerCopies(hook);
      expect(extra).toEqual([]);
      expect(newer.turnIndex).toBe(2);
      expect(newer.steeringDelivered).toBe(true);
      expect(newer.queueError).toBeUndefined();
      expect(older.turnIndex).toBe(1);
      if (left === 'failed') {
        expect(older).toMatchObject({ queueError: 'Failed to send message' });
        expect(older.steeringDelivered).toBeFalsy();
      }
    },
  );

  it('a reload whose replay delivers a steer a stale view sent before its run named the turn shows it once', async () => {
    // The view shows turn 0. Another tab added turn 1, so the server admits this send as turn 2.
    const hook = await mount();
    let drop: () => void = () => {};
    mockSendStream.mockImplementationOnce((_msg: string, _ws: string, _tid: string | null, { onRunIdResolved }: { onRunIdResolved?: (runId: string, tid: string) => void }) =>
      new Promise((resolve) => {
        onRunIdResolved?.('run-2', 'th-1');
        // The link drops before the run's metadata frame, which would have named the turn.
        drop = () => resolve({ disconnected: true });
      }));
    let lose: () => void = () => {};
    mockSendStream.mockImplementationOnce(() => new Promise((_resolve, reject) => {
      lose = () => reject(new TypeError('Failed to fetch'));
    }));
    // The run ends while the link is down, having delivered the steer.
    mockStatus.mockImplementation(async () => threadStatus({ can_reconnect: false, status: 'completed', latest_turn_index: 2 }));
    mockReplay.mockImplementation(async (_tid: string, onEvent: Emit) => {
      onEvent({ event: 'user_message', turn_index: 0, content: 'question 0', run_id: 'run-0' });
      onEvent({ ...chunk('answer 0'), turn_index: 0 });
      onEvent({ event: 'user_message', turn_index: 1, content: 'from another tab', run_id: 'run-1' });
      onEvent({ ...chunk('its answer'), turn_index: 1 });
      onEvent({ event: 'user_message', turn_index: 2, content: 'go on', run_id: 'run-2' });
      onEvent({ ...chunk('the answer'), turn_index: 2 });
      onEvent({ ...steered, turn_index: 2 });
      onEvent({ ...chunk('after the steer'), turn_index: 2 });
      onEvent({ event: 'replay_done', thread_id: 'th-1' });
    });
    await step(() => void hook.result.current.handleSendMessage('go on'));
    await step(() => void hook.result.current.handleSendMessage('also this'));
    // The view's own count, which nothing from the run has corrected.
    expect(steerCopies(hook)[0].turnIndex).toBe(1);
    await step(() => lose());
    expect(steerCopies(hook)[0]).toMatchObject({ queueError: 'Failed to send message' });
    await step(() => drop());
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(hook.result.current.isLoadingHistory).toBe(false));

    const [steer, ...extra] = steerCopies(hook);
    expect(extra).toEqual([]);
    expect(steer).toMatchObject({ turnIndex: 2, steeringDelivered: true, isHistory: true });
    expect(steer.queueError).toBeUndefined();
    expect(hook.result.current.messageError).toBeNull();
  });

  /**
   * Sends a turn and steers it, the server queueing the steer. The send's
   * stream then drops and every status check the retry makes is rate-limited,
   * so it gives up with the run still going; the reload's status check finds
   * the run as `reloadStatus` says. Returns the reload's attach, which plays
   * the turn so far and stays open until `end`.
   */
  async function queuedSteerThenGiveUp(hook: Hook, reloadStatus: () => ReturnType<typeof threadStatus>) {
    let drop: () => void = () => {};
    mockSendStream.mockImplementationOnce((_msg: string, _ws: string, _tid: string | null, { onEvent }: { onEvent: Emit }) =>
      new Promise((resolve) => {
        onEvent({ event: 'metadata', thread_id: 'th-1', run_id: 'run-1', turn_index: 1, _eventId: 1 });
        onEvent({ ...chunk('the answer'), _eventId: 2 });
        drop = () => resolve({ disconnected: true });
      }));
    mockSendStream.mockImplementationOnce(async (_msg: string, _ws: string, _tid: string | null, { onEvent }: { onEvent: Emit }) => {
      onEvent({ event: 'steering_accepted', thread_id: 'th-1', content: 'also this', position: 1 });
      return { disconnected: false };
    });
    let checks = 0;
    mockStatus.mockImplementation(async () => {
      checks += 1;
      if (checks <= 5) throw Object.assign(new Error('Rate limit exceeded'), { status: 429, retryAfter: 0.001 });
      return reloadStatus();
    });
    const attach = { send: (_e: Record<string, unknown>) => {}, end: () => {} };
    mockReconnect.mockImplementation((_tid: string, _rid: string, _cursor: unknown, onEvent: Emit, signal: AbortSignal) =>
      new Promise((resolve) => {
        onEvent({ event: 'metadata', thread_id: 'th-1', run_id: 'run-1', turn_index: 1, _eventId: 1 });
        onEvent({ ...chunk('the answer'), _eventId: 2 });
        onEvent({ event: 'caught_up' });
        attach.send = onEvent;
        attach.end = () => resolve({ disconnected: false, aborted: false });
        signal.addEventListener('abort', () => resolve({ disconnected: false, aborted: true }), { once: true });
      }));
    await step(() => void hook.result.current.handleSendMessage('go on'));
    await step(() => void hook.result.current.handleSendMessage('also this'));
    expect(steerCopies(hook)[0].steering).toBe(true);
    await step(() => drop());
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(hook.result.current.isLoadingHistory).toBe(false));
    return attach;
  }

  it('a steer still queued when the retry gives up on a live run stays through the reload until the run delivers it', async () => {
    const hook = await mount();
    // The running turn replays as its stub: the question, with no run id yet.
    mockReplay.mockImplementation(async (_tid: string, onEvent: Emit) => {
      onEvent({ event: 'user_message', turn_index: 0, content: 'question 0', run_id: 'run-0' });
      onEvent({ ...chunk('answer 0'), turn_index: 0 });
      onEvent({ event: 'user_message', turn_index: 1, content: 'go on' });
      onEvent({ event: 'replay_done', thread_id: 'th-1' });
    });
    const attach = await queuedSteerThenGiveUp(hook, running);
    await waitFor(() => expect(mockReconnect).toHaveBeenCalled());

    const [queued, ...extra] = steerCopies(hook);
    expect(extra).toEqual([]);
    expect(queued).toMatchObject({ steering: true });
    expect(queued.isHistory).toBeFalsy();
    // The attach took the replay's bubble for the turn over, which keeps
    // what the run wrote before the steer above it.
    const shown = hook.result.current.liveMessages.get() as MessageRecord[];
    expect(shown.indexOf(bubbleShowing(hook, 'the answer'))).toBe(shown.indexOf(queued) - 1);

    await step(() => attach.send({ ...steered, _eventId: 3 }));
    const [steer, ...more] = steerCopies(hook);
    expect(more).toEqual([]);
    expect(steer.steeringDelivered).toBe(true);
    await step(() => attach.end());
  });

  it('a steer still queued when the retry gives up leaves with the run the reload finds ended without it', async () => {
    const hook = await mount();
    // The run ended without delivering the steer, and no stream of this view was open for its hand-back.
    mockReplay.mockImplementation(async (_tid: string, onEvent: Emit) => {
      onEvent({ event: 'user_message', turn_index: 0, content: 'question 0', run_id: 'run-0' });
      onEvent({ ...chunk('answer 0'), turn_index: 0 });
      onEvent({ event: 'user_message', turn_index: 1, content: 'go on', run_id: 'run-1' });
      onEvent({ ...chunk('the whole answer'), turn_index: 1 });
      onEvent({ event: 'replay_done', thread_id: 'th-1' });
    });
    await queuedSteerThenGiveUp(hook, () => threadStatus({ can_reconnect: false, status: 'completed', latest_turn_index: 1 }));

    expect(steerCopies(hook)).toEqual([]);
    expect(hook.result.current.returnedSteering).toBeNull();
    expect(mockReconnect).not.toHaveBeenCalled();
  });

  it('a later turn\'s delivery leaves a steer an earlier run kept queued, until a turn delivers it by its key', async () => {
    const hook = await mount();
    // The reload cannot read the status, so it neither attaches nor learns that the run ended.
    mockReplay.mockImplementation(async (_tid: string, onEvent: Emit) => {
      onEvent({ event: 'user_message', turn_index: 0, content: 'question 0', run_id: 'run-0' });
      onEvent({ ...chunk('answer 0'), turn_index: 0 });
      onEvent({ event: 'user_message', turn_index: 1, content: 'go on' });
      onEvent({ event: 'replay_done', thread_id: 'th-1' });
    });
    await queuedSteerThenGiveUp(hook, () => {
      throw new Error('crafted 500');
    });
    expect(mockReconnect).not.toHaveBeenCalled();
    expect(steerCopies(hook)).toMatchObject([{ steering: true, turnIndex: 1 }]);

    // The run ended without taking the steer. The next turn delivers a steer of its own, then
    // the one the server still holds, by the key it went out under.
    const nextRun: { send: Emit; end: () => void } = { send: () => {}, end: () => {} };
    mockSendStream.mockImplementationOnce((_msg: string, _ws: string, _tid: string | null, { onEvent }: { onEvent: Emit }) =>
      new Promise((resolve) => {
        onEvent({ event: 'metadata', thread_id: 'th-1', run_id: 'run-2', turn_index: 2, _eventId: 1 });
        onEvent({ ...chunk('the next answer'), _eventId: 2 });
        nextRun.send = onEvent;
        nextRun.end = () => resolve({ disconnected: false });
      }));
    mockSendStream.mockImplementationOnce(async (_msg: string, _ws: string, _tid: string | null, { onEvent }: { onEvent: Emit }) => {
      onEvent({ event: 'steering_accepted', thread_id: 'th-1', content: 'and this too', position: 1 });
      return { disconnected: false };
    });
    await step(() => void hook.result.current.handleSendMessage('next question'));
    await step(() => void hook.result.current.handleSendMessage('and this too'));
    await step(() => nextRun.send({ event: 'steering_delivered', messages: [{ content: 'and this too', timestamp: 1_700_000_100 }], _eventId: 3 }));

    const messages = hook.result.current.liveMessages.get() as MessageRecord[];
    expect(messages.filter((m) => m.role === 'user' && m.content === 'and this too')).toMatchObject([{ steeringDelivered: true, turnIndex: 2 }]);
    expect(steerCopies(hook)).toMatchObject([{ steering: true, turnIndex: 1 }]);
    expect(steerCopies(hook)[0].steeringDelivered).toBeFalsy();

    const heldKey = (mockSendStream.mock.calls[1][3] as { requestKey: string }).requestKey;
    await step(() => {
      nextRun.send({
        event: 'steering_delivered',
        messages: [{ content: 'also this', timestamp: 1_700_000_200, request_key: heldKey }],
        _eventId: 4,
      });
      nextRun.send({ ...chunk('', { finish_reason: 'stop' }), _eventId: 5 });
      nextRun.end();
    });
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));
    expect(steerCopies(hook)).toMatchObject([{ steeringDelivered: true, turnIndex: 2 }]);
    expect(hook.result.current.returnedSteering).toBeNull();
  });

  it('a delivery leaves a steer the server has not yet answered for the delivery that names it', async () => {
    const hook = await mount();
    const run: { send: Emit; end: () => void } = { send: () => {}, end: () => {} };
    mockSendStream.mockImplementationOnce((_msg: string, _ws: string, _tid: string | null, { onEvent }: { onEvent: Emit }) =>
      new Promise((resolve) => {
        onEvent({ event: 'metadata', thread_id: 'th-1', run_id: 'run-1', turn_index: 1, _eventId: 1 });
        onEvent({ ...chunk('the answer'), _eventId: 2 });
        run.send = onEvent;
        run.end = () => resolve({ disconnected: false });
      }));
    mockSendStream.mockImplementationOnce(async (_msg: string, _ws: string, _tid: string | null, { onEvent }: { onEvent: Emit }) => {
      onEvent({ event: 'steering_accepted', thread_id: 'th-1', content: 'also this', position: 1 });
      return { disconnected: false };
    });
    let accept: () => void = () => {};
    mockSendStream.mockImplementationOnce((_msg: string, _ws: string, _tid: string | null, { onEvent }: { onEvent: Emit }) =>
      new Promise((resolve) => {
        accept = () => {
          onEvent({ event: 'steering_accepted', thread_id: 'th-1', content: 'and this too', position: 1 });
          resolve({ disconnected: false });
        };
      }));
    await step(() => void hook.result.current.handleSendMessage('go on'));
    await step(() => void hook.result.current.handleSendMessage('also this'));
    await step(() => void hook.result.current.handleSendMessage('and this too'));
    // The run takes the first steer while the second is still on its way to the queue.
    await step(() => run.send({ ...steered, _eventId: 3 }));
    const later = (h: Hook) => (h.result.current.liveMessages.get() as MessageRecord[]).filter(
      (m) => m.role === 'user' && m.content === 'and this too',
    );
    expect(steerCopies(hook)).toMatchObject([{ steeringDelivered: true }]);
    expect(later(hook)).toMatchObject([{ steering: true }]);

    await step(() => accept());
    await step(() => run.send({ event: 'steering_delivered', messages: [{ content: 'and this too', timestamp: 1_700_000_100 }], _eventId: 4 }));
    expect(later(hook)).toMatchObject([{ steeringDelivered: true }]);
    await step(() => {
      run.send({ ...chunk('', { finish_reason: 'stop' }), _eventId: 5 });
      run.end();
    });
  });

  it.each(['waiting', 'lost its answer'] as const)(
    'a delivery naming this view\'s steer (%s) and one sent elsewhere shows both, each once',
    async (outcome) => {
      const hook = await mount();
      const run: { send: Emit; end: () => void } = { send: () => {}, end: () => {} };
      mockSendStream.mockImplementationOnce((_msg: string, _ws: string, _tid: string | null, { onEvent }: { onEvent: Emit }) =>
        new Promise((resolve) => {
          onEvent({ event: 'metadata', thread_id: 'th-1', run_id: 'run-1', turn_index: 1, _eventId: 1 });
          onEvent({ ...chunk('the answer'), _eventId: 2 });
          run.send = onEvent;
          run.end = () => resolve({ disconnected: false });
        }));
      mockSendStream.mockImplementationOnce(async (_msg: string, _ws: string, _tid: string | null, { onEvent }: { onEvent: Emit }) => {
        if (outcome === 'lost its answer') throw new TypeError('Failed to fetch');
        onEvent({ event: 'steering_accepted', thread_id: 'th-1', content: 'also this', position: 1 });
        return { disconnected: false };
      });
      await step(() => void hook.result.current.handleSendMessage('go on'));
      await step(() => void hook.result.current.handleSendMessage('also this'));
      // One drain takes this view's steer and one another tab queued beside it.
      await step(() => run.send({
        event: 'steering_delivered',
        messages: [{ content: 'also this', timestamp: 1_700_000_100 }, { content: 'from another tab', timestamp: 1_700_000_101 }],
        _eventId: 3,
      }));
      const other = (hook.result.current.liveMessages.get() as MessageRecord[]).filter(
        (m) => m.role === 'user' && m.content === 'from another tab',
      );
      expect(steerCopies(hook)).toMatchObject([{ steeringDelivered: true, turnIndex: 1 }]);
      expect(steerCopies(hook)[0].queueError).toBeUndefined();
      expect(other).toMatchObject([{ steeringDelivered: true, turnIndex: 1 }]);
      expect(hook.result.current.messageError).toBeNull();
      await step(() => {
        run.send({ ...chunk('', { finish_reason: 'stop' }), _eventId: 4 });
        run.end();
      });
    },
  );

  it('a refusal that lands after the retry asked for its reload, before that reload runs, shows after it', async () => {
    const hook = await mount();
    await dropThenReconnect(hook, failsAfter([{ ...chunk(' and then'), _eventId: 3 }], 'drops'));
    await waitFor(() => expect(bubbleShowing(hook, 'and then')).toBeTruthy());
    let check: () => void = () => {};
    mockStatus.mockImplementationOnce(() => new Promise((resolve) => {
      check = () => resolve(ended());
    }));
    mockStatus.mockImplementation(async () => ended());
    let refuse: () => void = () => {};
    mockSendStream.mockImplementationOnce(() => new Promise((_resolve, reject) => {
      refuse = () => reject(new Error('refused'));
    }));
    await step(() => void hook.result.current.handleSendMessage('also this'));
    // Up to the retry's status check, which waits.
    await act(async () => {
      await new Promise((r) => setTimeout(r, 1200));
    });

    await act(async () => {
      // The retry finds the run over and asks for its reload, which runs once this settles.
      check();
      await flush();
      expect(mockReplay).toHaveBeenCalledTimes(1);
      refuse();
      await flush();
    });
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(hook.result.current.isLoadingHistory).toBe(false));

    expect(steerCopies(hook)).toMatchObject([{ queueError: 'Failed to send message' }]);
    expect(hook.result.current.messageError).toBe('Failed to send message');
  });

  it.each([
    ['queued', 'counts', 2],
    ['lost its answer', 'counts', 2],
    // A failed read: only the attach's first frame names the turn, after the replay has placed the steer.
    ['lost its answer', 'could not count', null],
  ] as const)(
    'a steer a stale view sent before its run named the turn, which then %s, takes that turn on a reload whose status %s it',
    async (outcome, _status, counted) => {
      // The view shows turn 0. Another tab added turn 1, so the server admits this send as turn 2.
      const hook = await mount();
      let drop: () => void = () => {};
      mockSendStream.mockImplementationOnce((_msg: string, _ws: string, _tid: string | null, { onRunIdResolved }: { onRunIdResolved?: (runId: string, tid: string) => void }) =>
        new Promise((resolve) => {
          onRunIdResolved?.('run-2', 'th-1');
          // The link drops before the run's metadata frame, which would have named the turn.
          drop = () => resolve({ disconnected: true });
        }));
      mockSendStream.mockImplementationOnce(async (_msg: string, _ws: string, _tid: string | null, { onEvent }: { onEvent: Emit }) => {
        if (outcome === 'lost its answer') throw new TypeError('Failed to fetch');
        onEvent({ event: 'steering_accepted', thread_id: 'th-1', content: 'also this', position: 1 });
        return { disconnected: false };
      });
      // Every status check the retry makes is rate-limited, so it gives up with the run going.
      let checks = 0;
      mockStatus.mockImplementation(async () => {
        checks += 1;
        if (checks <= 5) throw Object.assign(new Error('Rate limit exceeded'), { status: 429, retryAfter: 0.001 });
        return threadStatus({ can_reconnect: true, status: 'running', run_id: 'run-2', latest_turn_index: counted });
      });
      mockReplay.mockImplementation(async (_tid: string, onEvent: Emit) => {
        onEvent({ event: 'user_message', turn_index: 0, content: 'question 0', run_id: 'run-0' });
        onEvent({ ...chunk('answer 0'), turn_index: 0 });
        onEvent({ event: 'user_message', turn_index: 1, content: 'from another tab', run_id: 'run-1' });
        onEvent({ ...chunk('its answer'), turn_index: 1 });
        onEvent({ event: 'user_message', turn_index: 2, content: 'go on' });
        onEvent({ event: 'replay_done', thread_id: 'th-1' });
      });
      const attach = { send: (_e: Record<string, unknown>) => {}, end: () => {} };
      mockReconnect.mockImplementation((_tid: string, _rid: string, _cursor: unknown, onEvent: Emit, signal: AbortSignal) =>
        new Promise((resolve) => {
          onEvent({ event: 'metadata', thread_id: 'th-1', run_id: 'run-2', turn_index: 2, _eventId: 1 });
          onEvent({ ...chunk('the answer'), _eventId: 2 });
          onEvent({ event: 'caught_up' });
          attach.send = onEvent;
          attach.end = () => resolve({ disconnected: false, aborted: false });
          signal.addEventListener('abort', () => resolve({ disconnected: false, aborted: true }), { once: true });
        }));
      await step(() => void hook.result.current.handleSendMessage('go on'));
      await step(() => void hook.result.current.handleSendMessage('also this'));
      const held = outcome === 'queued' ? { steering: true } : { queueError: 'Failed to send message' };
      // The view's own count, which nothing from the run has corrected.
      expect(steerCopies(hook)).toMatchObject([{ ...held, turnIndex: 1 }]);
      await step(() => drop());
      await waitFor(() => expect(mockReconnect).toHaveBeenCalled());
      await waitFor(() => expect(hook.result.current.isLoadingHistory).toBe(false));

      expect(steerCopies(hook)).toMatchObject([{ ...held, turnIndex: 2 }]);
      if (counted !== null) {
        // Below the message that opened the turn it steered.
        const shown = hook.result.current.liveMessages.get() as MessageRecord[];
        expect(shown.findIndex((m) => m.role === 'user' && m.content === 'go on')).toBeLessThan(shown.indexOf(steerCopies(hook)[0]));
      }
      await step(() => attach.send({ ...steered, _eventId: 3 }));
      const [steer, ...extra] = steerCopies(hook);
      expect(extra).toEqual([]);
      expect(steer).toMatchObject({ steeringDelivered: true, turnIndex: 2 });
      expect(steer.queueError).toBeUndefined();
      await step(() => attach.end());
    },
  );

  /** The server's answer to a copy of a send it already took under the same key,
   *  `steered` when that copy went into a running turn as a steer. */
  const duplicateOf = (runId: string, runStatus = 'in_progress', steered = false) => {
    const message = 'This request was already accepted; reconnect to the existing run instead of resending.';
    return Object.assign(new Error(message), {
      status: 409,
      errorInfo: {
        code: 'duplicate_request', message, thread_id: 'th-1', run_id: runId, run_status: runStatus,
        ...(steered ? { steered: true } : {}),
      },
    });
  };
  const keyOf = (call: number) => (mockSendStream.mock.calls[call][3] as { requestKey: string | null }).requestKey;
  const lostAnswer = () => mockSendStream.mockImplementationOnce(async () => {
    throw new TypeError('Failed to fetch');
  });

  it('a steer sent again after its POST lost the answer shows once, waiting for the run to deliver it', async () => {
    const hook = await mount();
    const turn = runningTurn('run-1', 1, 'the answer');
    lostAnswer();
    mockSendStream.mockImplementationOnce(async () => {
      throw duplicateOf('run-1', 'in_progress', true);
    });
    await step(() => void hook.result.current.handleSendMessage('go on'));
    await step(() => void hook.result.current.handleSendMessage('also this'));
    expect(steerCopies(hook)).toMatchObject([{ queueError: 'Failed to send message' }]);
    await step(() => void hook.result.current.handleSendMessage('also this'));

    // The same send, under the key the server already took.
    expect(keyOf(2)).toBe(keyOf(1));
    const [steer, ...extra] = steerCopies(hook);
    expect(extra).toEqual([]);
    expect(steer.steering).toBe(true);
    expect(steer.queueError).toBeUndefined();
    expect(hook.result.current.messageError).toBeNull();

    await step(() => turn.deliver());
    await step(() => turn.finish());
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));
    const [delivered, ...more] = steerCopies(hook);
    expect(more).toEqual([]);
    expect(delivered.steeringDelivered).toBe(true);
    expect(delivered.queueError).toBeUndefined();
    expect(hook.result.current.messageError).toBeNull();
  });

  it('a steer sent again that the run delivers before the server answers it as a copy shows once, delivered', async () => {
    const hook = await mount();
    const turn = runningTurn('run-1', 1, 'the answer');
    lostAnswer();
    let answer: () => void = () => {};
    mockSendStream.mockImplementationOnce(() => new Promise((_resolve, reject) => {
      answer = () => reject(duplicateOf('run-1', 'in_progress', true));
    }));
    await step(() => void hook.result.current.handleSendMessage('go on'));
    await step(() => void hook.result.current.handleSendMessage('also this'));
    await step(() => void hook.result.current.handleSendMessage('also this'));
    await step(() => turn.deliver());
    await step(() => answer());

    const [steer, ...extra] = steerCopies(hook);
    expect(extra).toEqual([]);
    expect(steer.steeringDelivered).toBe(true);
    expect(steer.queueError).toBeUndefined();
    expect(hook.result.current.messageError).toBeNull();
    await step(() => turn.finish());
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));
    expect(steerCopies(hook)).toHaveLength(1);
  });

  it('a steer sent again after the run delivered an earlier send of its text keeps its own key', async () => {
    const hook = await mount();
    let emit: Emit = () => {};
    const turn = runningTurn('run-1', 1, 'the answer', (onEvent) => {
      emit = onEvent;
    });
    const yes = { event: 'steering_delivered', messages: [{ content: 'yes', timestamp: 1_700_000_000 }] };
    await step(() => void hook.result.current.handleSendMessage('go on'));
    type Accepting = { onEvent: Emit; onRunIdResolved: (id: string) => void };
    mockSendStream.mockImplementationOnce(async (_msg: string, _ws: string, _tid: string | null, { onEvent, onRunIdResolved }: Accepting) => {
      onRunIdResolved('run-1');
      onEvent({ event: 'steering_accepted', thread_id: 'th-1', content: 'yes', position: 1 });
      return { disconnected: false };
    });
    await step(() => void hook.result.current.handleSendMessage('yes'));
    let lose: () => void = () => {};
    mockSendStream.mockImplementationOnce(() => new Promise((_resolve, reject) => {
      lose = () => reject(new TypeError('Failed to fetch'));
    }));
    await step(() => void hook.result.current.handleSendMessage('yes'));
    // The run delivers the first yes while the second one's POST is out, and
    // the second one's answer is lost.
    await step(() => emit({ ...yes, _eventId: 3 }));
    await step(() => lose());
    mockSendStream.mockImplementationOnce(async () => {
      throw duplicateOf('run-1', 'in_progress', true);
    });
    await step(() => void hook.result.current.handleSendMessage('yes'));

    // A retransmit of the second yes, which the server already holds.
    expect(keyOf(3)).toBe(keyOf(2));
    expect(keyOf(2)).not.toBe(keyOf(1));
    await step(() => emit({ ...yes, _eventId: 4 }));
    const yeses = (hook.result.current.liveMessages.get() as MessageRecord[]).filter((m) => m.role === 'user' && m.content === 'yes');
    expect(yeses).toMatchObject([{ steeringDelivered: true }, { steeringDelivered: true }]);
    expect(hook.result.current.messageError).toBeNull();
    await step(() => turn.finish());
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));
  });

  type Accepting = { onEvent: Emit; onRunIdResolved: (id: string) => void };

  it.each(['answers it as a copy', 'takes it'] as const)(
    'a steer sent while another waits for its answer keeps its key when the server %s',
    async (answered) => {
      const hook = await mount();
      const turn = runningTurn('run-1', 1, 'the answer');
      if (answered === 'answers it as a copy') lostAnswer();
      let answer: () => void = () => {};
      mockSendStream.mockImplementationOnce((_msg: string, _ws: string, _tid: string | null, { onEvent, onRunIdResolved }: Accepting) =>
        new Promise((resolve, reject) => {
          answer = answered === 'answers it as a copy'
            ? () => reject(duplicateOf('run-1', 'in_progress', true))
            : () => {
              onRunIdResolved('run-1');
              onEvent({ event: 'steering_accepted', thread_id: 'th-1', content: 'also this', position: 1 });
              resolve({ disconnected: false });
            };
        }));
      await step(() => void hook.result.current.handleSendMessage('go on'));
      await step(() => void hook.result.current.handleSendMessage('also this'));
      if (answered === 'answers it as a copy') await step(() => void hook.result.current.handleSendMessage('also this'));
      // Another message while that answer is out: the server takes it, and its answer is lost.
      lostAnswer();
      await step(() => void hook.result.current.handleSendMessage('and that'));
      const other = mockSendStream.mock.calls.length - 1;
      await step(() => answer());
      mockSendStream.mockImplementationOnce(async () => {
        throw duplicateOf('run-1', 'in_progress', true);
      });
      await step(() => void hook.result.current.handleSendMessage('and that'));

      // A retransmit of the other message, under the key the server holds it by.
      expect(keyOf(other + 1)).toBe(keyOf(other));
      expect(keyOf(other)).not.toBe(keyOf(1));
      const others = (hook.result.current.liveMessages.get() as MessageRecord[]).filter((m) => m.role === 'user' && m.content === 'and that');
      expect(others).toMatchObject([{ steering: true }]);
      await step(() => turn.finish());
      await waitFor(() => expect(hook.result.current.isLoading).toBe(false));
    },
  );

  it.each(['after', 'before'] as const)(
    'a steer sent again keeps the key its lost answer went out under, though another steer went out %s that answer was lost',
    async (when) => {
      const hook = await mount();
      const turn = runningTurn('run-1', 1, 'the answer');
      let loseA: () => void = () => {};
      mockSendStream.mockImplementationOnce(() => new Promise((_resolve, reject) => {
        loseA = () => reject(new TypeError('Failed to fetch'));
      }));
      mockSendStream.mockImplementationOnce(async (_msg: string, _ws: string, _tid: string | null, { onEvent, onRunIdResolved }: Accepting) => {
        onRunIdResolved('run-1');
        onEvent({ event: 'steering_accepted', thread_id: 'th-1', content: 'and that', position: 2 });
        return { disconnected: false };
      });
      mockSendStream.mockImplementationOnce(async () => {
        throw duplicateOf('run-1', 'in_progress', true);
      });
      await step(() => void hook.result.current.handleSendMessage('go on'));
      await step(() => void hook.result.current.handleSendMessage('also this'));
      if (when === 'after') await step(() => loseA());
      await step(() => void hook.result.current.handleSendMessage('and that'));
      if (when === 'before') await step(() => loseA());
      expect(steerCopies(hook)).toMatchObject([{ queueError: 'Failed to send message' }]);
      await step(() => void hook.result.current.handleSendMessage('also this'));

      // The same send, under the key the server queued it by, which the
      // other steer did not take over.
      expect(keyOf(3)).toBe(keyOf(1));
      expect(keyOf(2)).not.toBe(keyOf(1));
      const [steer, ...extra] = steerCopies(hook);
      expect(extra).toEqual([]);
      expect(steer).toMatchObject({ steering: true });
      expect(steer.queueError).toBeUndefined();
      expect(hook.result.current.messageError).toBeNull();
      await step(() => turn.finish());
      await waitFor(() => expect(hook.result.current.isLoading).toBe(false));
    },
  );

  it('a steer sent again whose copy the run delivered before the server answered leaves a new send of its text that failed', async () => {
    const hook = await mount();
    let emit: Emit = () => {};
    const turn = runningTurn('run-1', 1, 'the answer', (onEvent) => {
      emit = onEvent;
    });
    lostAnswer();
    let answer: () => void = () => {};
    mockSendStream.mockImplementationOnce(() => new Promise((_resolve, reject) => {
      answer = () => reject(duplicateOf('run-1', 'in_progress', true));
    }));
    await step(() => void hook.result.current.handleSendMessage('go on'));
    await step(() => void hook.result.current.handleSendMessage('also this'));
    await step(() => void hook.result.current.handleSendMessage('also this'));
    await step(() => emit({ ...steered, _eventId: 3 }));
    // The same words again, a new message, whose POST never reaches the server.
    lostAnswer();
    await step(() => void hook.result.current.handleSendMessage('also this'));
    expect(keyOf(3)).not.toBe(keyOf(1));
    await step(() => answer());

    // The copies of the delivered send show it once; the new one is still failed.
    expect(steerCopies(hook)).toMatchObject([
      { steeringDelivered: true, requestKey: keyOf(1) },
      { queueError: 'Failed to send message', requestKey: keyOf(3) },
    ]);
    expect(hook.result.current.messageError).toBe('Failed to send message');
    // Sent again, it goes under its own key, which the server never took.
    mockSendStream.mockImplementationOnce(async (_msg: string, _ws: string, _tid: string | null, { onEvent, onRunIdResolved }: Accepting) => {
      onRunIdResolved('run-1');
      onEvent({ event: 'steering_accepted', thread_id: 'th-1', content: 'also this', position: 1 });
      return { disconnected: false };
    });
    await step(() => void hook.result.current.handleSendMessage('also this'));
    expect(keyOf(4)).toBe(keyOf(3));
    await step(() => turn.finish());
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));
  });

  /**
   * Sends "go on" and loses the answer of the POST that started run-1 with it,
   * then catches up with that run as a view does once it finds it running:
   * the reload draws the question and the reconnect follows the turn.
   */
  async function lostSendThenCaughtUp(hook: Hook) {
    lostAnswer();
    await step(() => void hook.result.current.handleSendMessage('go on'));
    const attach = { end: () => {} };
    mockStatus.mockResolvedValue(running());
    mockReplay.mockImplementation(async (_tid: string, onEvent: Emit) => {
      onEvent({ event: 'user_message', turn_index: 0, content: 'question 0', run_id: 'run-0' });
      onEvent({ ...chunk('answer 0'), turn_index: 0 });
      onEvent({ event: 'user_message', turn_index: 1, content: 'go on', run_id: 'run-1' });
      onEvent({ event: 'replay_done', thread_id: 'th-1' });
    });
    mockReconnect.mockImplementation((_tid: string, _rid: string, _cursor: unknown, onEvent: Emit, signal: AbortSignal) =>
      new Promise((resolve) => {
        onEvent({ event: 'metadata', thread_id: 'th-1', run_id: 'run-1', turn_index: 1, _eventId: 1 });
        onEvent({ ...chunk('the answer'), _eventId: 2 });
        onEvent({ event: 'caught_up' });
        attach.end = () => {
          onEvent({ ...chunk('', { finish_reason: 'stop' }), _eventId: 3 });
          resolve({ disconnected: false, aborted: false });
        };
        signal.addEventListener('abort', () => resolve({ disconnected: false, aborted: true }), { once: true });
      }));
    await act(async () => {
      await hook.result.current.reconnectIfStaleRun();
    });
    await waitFor(() => expect(mockReconnect).toHaveBeenCalled());
    await waitFor(() => expect(hook.result.current.isLoadingHistory).toBe(false));
    expect(hook.result.current.isLoading).toBe(true);
    return attach;
  }

  const questions = (hook: Hook) => (hook.result.current.liveMessages.get() as MessageRecord[]).filter(
    (m) => m.role === 'user' && m.content === 'go on',
  );

  it('a send again of a message whose lost copy started the turn on screen shows once, as that turn\'s question', async () => {
    const hook = await mount();
    const attach = await lostSendThenCaughtUp(hook);
    expect(questions(hook)).toHaveLength(1);
    mockSendStream.mockImplementationOnce(async () => {
      throw duplicateOf('run-1');
    });
    await step(() => void hook.result.current.handleSendMessage('go on'));

    // The same send, under the key the server started the turn with.
    expect(keyOf(1)).toBe(keyOf(0));
    const [question, ...extra] = questions(hook);
    expect(extra).toEqual([]);
    expect(question.steering).toBeFalsy();
    expect(hook.result.current.messageError).toBeNull();
    // Settled: the same text sent now is a new message.
    mockSendStream.mockImplementationOnce(async (_msg: string, _ws: string, _tid: string | null, { onEvent }: { onEvent: Emit }) => {
      onEvent({ event: 'steering_accepted', thread_id: 'th-1', content: 'go on', position: 1 });
      return { disconnected: false };
    });
    await step(() => void hook.result.current.handleSendMessage('go on'));
    expect(keyOf(2)).toBeTruthy();
    expect(keyOf(2)).not.toBe(keyOf(1));
    await step(() => attach.end());
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));
  });

  it('a send again whose lost copy started a turn this view does not show goes, and a reload shows that turn', async () => {
    const hook = await mount();
    const attach = await lostSendThenCaughtUp(hook);
    mockSendStream.mockImplementationOnce(async () => {
      throw duplicateOf('run-2');
    });
    await step(() => void hook.result.current.handleSendMessage('go on'));

    expect(keyOf(1)).toBe(keyOf(0));
    expect(questions(hook)).toMatchObject([{ isHistory: true }]);
    expect(questions(hook)[0].steering).toBeFalsy();
    expect(hook.result.current.messageError).toBeNull();
    const replays = mockReplay.mock.calls.length;
    await step(() => attach.end());
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(replays + 1));
  });

  it.each(['while the run goes on', 'after it ends'] as const)(
    'the same text sent %s, once the run delivered the copy whose answer was lost, is a new send',
    async (when) => {
      const hook = await mount();
      const turn = runningTurn('run-1', 1, 'the answer');
      lostAnswer();
      await step(() => void hook.result.current.handleSendMessage('go on'));
      await step(() => void hook.result.current.handleSendMessage('also this'));
      await step(() => turn.deliver());
      expect(steerCopies(hook)).toMatchObject([{ steeringDelivered: true }]);
      if (when === 'after it ends') {
        await step(() => turn.finish());
        await waitFor(() => expect(hook.result.current.isLoading).toBe(false));
        runningTurn('run-2', 2, 'the next answer');
      } else {
        mockSendStream.mockImplementationOnce(async (_msg: string, _ws: string, _tid: string | null, { onEvent }: { onEvent: Emit }) => {
          onEvent({ event: 'steering_accepted', thread_id: 'th-1', content: 'also this', position: 1 });
          return { disconnected: false };
        });
      }

      await step(() => void hook.result.current.handleSendMessage('also this'));
      expect(keyOf(2)).toBeTruthy();
      expect(keyOf(2)).not.toBe(keyOf(1));
    },
  );

  it('the same text sent after a reload showed the run delivered the copy whose answer was lost is a new send', async () => {
    const hook = await mount();
    const turn = runningTurn('run-1', 1, 'the answer');
    lostAnswer();
    await step(() => void hook.result.current.handleSendMessage('go on'));
    await step(() => void hook.result.current.handleSendMessage('also this'));
    // The run ends without this view seeing it deliver the steer.
    await step(() => turn.finish());
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));
    expect(steerCopies(hook)).toMatchObject([{ queueError: 'Failed to send message' }]);

    mockStatus.mockResolvedValue(threadStatus({ can_reconnect: false, status: 'completed', latest_turn_index: 2 }));
    mockReplay.mockImplementation(steeredTurnThenAnother);
    await act(async () => {
      await hook.result.current.reconnectIfStaleRun();
    });
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(hook.result.current.isLoadingHistory).toBe(false));
    expect(steerCopies(hook)).toMatchObject([{ steeringDelivered: true }]);

    runningTurn('run-3', 3, 'the next answer');
    await step(() => void hook.result.current.handleSendMessage('also this'));
    expect(keyOf(2)).toBeTruthy();
    expect(keyOf(2)).not.toBe(keyOf(1));
  });

  /** Sends "also this" on the idle view and loses the answer of a copy the
   *  server put into another turn, then sends it again the way `how` says. */
  async function sendSteeredCopyAgain(hook: Hook, how: 'sent again' | 'retried', runStatus: string) {
    lostAnswer();
    mockSendStream.mockImplementationOnce(async () => {
      throw duplicateOf('run-1', runStatus, true);
    });
    await step(() => void hook.result.current.handleSendMessage('also this'));
    expect(steerCopies(hook)).toHaveLength(1);
    if (how === 'sent again') await step(() => void hook.result.current.handleSendMessage('also this'));
    else await step(() => void hook.result.current.handleRetry());
    // The same send, under the key the server already took.
    expect(keyOf(1)).toBe(keyOf(0));
  }

  const failedReplies = (hook: Hook) => (hook.result.current.liveMessages.get() as MessageRecord[]).filter(
    (m) => m.role === 'assistant' && (m.error || /Failed to/.test(String(m.content ?? ''))),
  );

  it.each(['sent again', 'retried'] as const)(
    'a send %s after its copy went into a running turn as a steer shows once, and that turn delivers it',
    async (how) => {
      const hook = await mount();
      // The turn that took the copy, which this view did not start or follow.
      mockStatus.mockResolvedValue(threadStatus({ can_reconnect: true, status: 'running', run_id: 'run-1', latest_turn_index: 1 }));
      mockReplay.mockImplementation(async (_tid: string, onEvent: Emit) => {
        onEvent({ event: 'user_message', turn_index: 0, content: 'question 0', run_id: 'run-0' });
        onEvent({ ...chunk('answer 0'), turn_index: 0 });
        onEvent({ event: 'user_message', turn_index: 1, content: 'from another tab', run_id: 'run-1' });
        onEvent({ event: 'replay_done', thread_id: 'th-1' });
      });
      mockReconnect.mockImplementation(endsAfter([
        { event: 'metadata', thread_id: 'th-1', run_id: 'run-1', turn_index: 1, _eventId: 1 },
        { ...chunk('its answer'), _eventId: 2 },
        { ...steered, _eventId: 3 },
        { ...chunk('', { finish_reason: 'stop' }), _eventId: 4 },
      ]));

      await sendSteeredCopyAgain(hook, how, 'in_progress');

      await waitFor(() => expect(mockReconnect).toHaveBeenCalled());
      await waitFor(() => expect(hook.result.current.isLoading).toBe(false));
      const [steer, ...extra] = steerCopies(hook);
      expect(extra).toEqual([]);
      expect(steer.steeringDelivered).toBe(true);
      expect(failedReplies(hook)).toEqual([]);
      expect(hook.result.current.messageError).toBeNull();
      // The view followed no stream of that turn, so a reload showed it.
      expect(mockReplay).toHaveBeenCalledTimes(2);
    },
  );

  it.each(['sent again', 'retried'] as const)(
    'a send %s after its copy went into a turn that then failed is not that turn\'s retry, and shows once',
    async (how) => {
      const hook = await mount();
      mockStatus.mockResolvedValue(threadStatus({ can_reconnect: false, status: 'error', latest_turn_index: 1 }));
      mockReplay.mockImplementation(async (_tid: string, onEvent: Emit) => {
        onEvent({ event: 'user_message', turn_index: 0, content: 'question 0', run_id: 'run-0' });
        onEvent({ ...chunk('answer 0'), turn_index: 0 });
        onEvent({ event: 'user_message', turn_index: 1, content: 'from another tab', run_id: 'run-1' });
        onEvent({ ...chunk('its answer'), turn_index: 1 });
        onEvent({ ...steered, turn_index: 1 });
        onEvent({ event: 'error', turn_index: 1, error: 'worker lost' });
        onEvent({ event: 'replay_done', thread_id: 'th-1' });
      });

      await sendSteeredCopyAgain(hook, how, 'error');

      expect(mockRetryStream).not.toHaveBeenCalled();
      await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
      await waitFor(() => expect(hook.result.current.isLoadingHistory).toBe(false));
      expect(mockRetryStream).not.toHaveBeenCalled();
      expect(mockSendStream).toHaveBeenCalledTimes(2);
      const [steer, ...extra] = steerCopies(hook);
      expect(extra).toEqual([]);
      expect(steer).toMatchObject({ steeringDelivered: true, isHistory: true });
      expect(hook.result.current.messageError).toBeNull();
    },
  );
});
