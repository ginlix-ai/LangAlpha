/**
 * What a reload leaves of the transcript already on screen.
 *
 * A replay that fails keeps the transcript it found, under the load-failed
 * state, rather than stripping it before the request and leaving an empty
 * thread. A replay that finds a run still going gives it an empty bubble, which
 * an attach that opened while the replay was in flight already stands for.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { ChatMessage } from '@/types/chat';

const api = vi.hoisted(() => ({ replayThreadHistory: vi.fn() }));

vi.mock('../../../utils/api', () => ({
  replayThreadHistory: api.replayThreadHistory,
}));

import { loadConversationHistory } from '../replayHistory';
import { buildRuntime, makeDeps, replayOf } from './replayHarness';

const chunk = (turn: number, content: string) => ({
  event: 'message_chunk',
  data: { thread_id: 'thread-1', turn_index: turn, role: 'assistant', id: `lc_run--${turn}`, agent: 'model:abc', content_type: 'text', content },
});
const opener = (turn: number, content: string, runId?: string) => ({
  event: 'user_message',
  data: { thread_id: 'thread-1', turn_index: turn, content, ...(runId ? { run_id: runId } : {}) },
});

const ids = (messages: ChatMessage[]) => messages.map((m) => m.id);

beforeEach(() => vi.clearAllMocks());

describe('history replay: a reload that fails', () => {
  it('keeps the transcript it found, and a later reload replaces it once', async () => {
    const { rt, read } = buildRuntime();
    api.replayThreadHistory.mockImplementation(replayOf([opener(0, 'Name a river', 'run-0'), chunk(0, 'The Nile')]));
    await loadConversationHistory(rt, makeDeps());
    expect(ids(read())).toEqual(['history-user-0', 'history-assistant-0']);

    api.replayThreadHistory.mockRejectedValueOnce(new Error('crafted 500'));
    await loadConversationHistory(rt, makeDeps());

    expect(rt.setHistoryLoadFailed).toHaveBeenLastCalledWith(true);
    expect(ids(read())).toEqual(['history-user-0', 'history-assistant-0']);
    expect(read()[1].content).toBe('The Nile');

    await loadConversationHistory(rt, makeDeps());

    expect(rt.setHistoryLoadFailed).toHaveBeenLastCalledWith(false);
    expect(ids(read())).toEqual(['history-user-0', 'history-assistant-0']);
  });

  it('still empties a thread the server no longer has', async () => {
    const { rt, read } = buildRuntime();
    api.replayThreadHistory.mockImplementation(replayOf([opener(0, 'Name a river', 'run-0'), chunk(0, 'The Nile')]));
    await loadConversationHistory(rt, makeDeps());

    api.replayThreadHistory.mockRejectedValueOnce(new Error('HTTP 404'));
    await loadConversationHistory(rt, makeDeps());

    expect(read()).toEqual([]);
  });
});

describe('history replay: a running turn an attach already streams', () => {
  function reloadUnder(liveTurn: number | undefined) {
    const { rt, read } = buildRuntime();
    rt.setMessages(() => [{
      id: 'assistant-reconnect-1', role: 'assistant', content: 'Lakes form', isStreaming: true,
      ...(liveTurn !== undefined ? { turnIndex: liveTurn } : {}),
    } as unknown as ChatMessage]);
    rt.currentMessageRef.current = 'assistant-reconnect-1';
    api.replayThreadHistory.mockImplementation(replayOf([
      opener(0, 'Name a river', 'run-0'), chunk(0, 'The Nile'),
      opener(1, 'Now lakes'),
    ]));
    return loadConversationHistory(rt, makeDeps()).then(() => read());
  }

  it('drops the empty bubble the replay gave the run', async () => {
    expect(ids(await reloadUnder(1))).toEqual(['history-user-0', 'history-assistant-0', 'history-user-1', 'assistant-reconnect-1']);
  });

  it('drops it for an attach that has not been given a turn yet', async () => {
    expect(ids(await reloadUnder(undefined))).toEqual(['history-user-0', 'history-assistant-0', 'history-user-1', 'assistant-reconnect-1']);
  });

  it('keeps it when the stream counts itself another turn', async () => {
    expect(ids(await reloadUnder(2))).toContain('history-assistant-1');
  });
});

describe('history replay: the steers a dead stream left', () => {
  const steer = (turn: number, content: string) => ({
    event: 'steering_delivered',
    data: { thread_id: 'thread-1', turn_index: turn, count: 1, messages: [{ content, timestamp: 1_700_000_000 }] },
  });
  const user = (id: string, content: string, over: Record<string, unknown> = {}) =>
    ({ id, role: 'user', content, turnIndex: 1, isStreaming: false, ...over }) as unknown as ChatMessage;
  const reply = (id: string, content: string, over: Record<string, unknown> = {}) =>
    ({ id, role: 'assistant', content, turnIndex: 1, isStreaming: false, contentSegments: [], ...over }) as unknown as ChatMessage;

  // Turn 1 streamed live past a steer, then its reconnect was refused (or a
  // newer turn took the session), so nothing marked it for the replay.
  async function reloadWith(steers: ChatMessage[]) {
    const { rt, read } = buildRuntime();
    rt.setMessages(() => [
      user('user-1', 'Plan a day in Rome'),
      reply('assistant-1', 'Morning at the Forum.'),
      ...steers,
      reply('assistant-2', 'Afternoon at the Vatican.', { isSteering: true }),
    ]);
    api.replayThreadHistory.mockImplementation(replayOf([
      opener(0, 'Name a river', 'run-0'), chunk(0, 'The Nile'),
      opener(1, 'Plan a day in Rome', 'run-1'), chunk(1, 'Morning at the Forum.'),
      steer(1, 'Add a museum'), chunk(1, 'Afternoon at the Vatican.'),
    ]));
    await loadConversationHistory(rt, makeDeps());
    return read();
  }
  const steersIn = (messages: ChatMessage[], text: string) =>
    messages.filter((m) => m.role === 'user' && m.content === text).map((m) => m.id);

  it('shows a steer the run delivered once, where the replay puts it', async () => {
    const messages = await reloadWith([user('steering-user-1', 'Add a museum', { steeringDelivered: true })]);

    expect(steersIn(messages, 'Add a museum')).toEqual(['history-steering-user-1-1-0']);
    expect(ids(messages)).toEqual([
      'history-user-0', 'history-assistant-0', 'history-user-1', 'history-assistant-1',
      'history-steering-user-1-1-0', 'history-assistant-steering-1-1',
    ]);
  });

  it('keeps a refused steer with its error, since the server never stored it', async () => {
    const messages = await reloadWith([
      user('steering-user-1', 'Add a museum', { steeringDelivered: true }),
      user('user-2', 'And a cafe', { queueError: 'crafted steer refusal', queueRefused: true }),
    ]);

    expect(steersIn(messages, 'Add a museum')).toEqual(['history-steering-user-1-1-0']);
    expect(messages.find((m) => m.id === 'user-2')).toMatchObject({ queueError: 'crafted steer refusal' });
  });

  it('shows a steer still pending when the stream died once, when the replay delivers it', async () => {
    const messages = await reloadWith([user('steering-user-1', 'Add a museum', { steering: true })]);

    expect(steersIn(messages, 'Add a museum')).toEqual(['history-steering-user-1-1-0']);
  });

  it('keeps a pending steer the replay does not deliver, which the run may have handed back', async () => {
    const messages = await reloadWith([
      user('steering-user-1', 'Add a museum', { steeringDelivered: true }),
      user('user-2', 'And a cafe', { steering: true }),
    ]);

    expect(steersIn(messages, 'And a cafe')).toEqual(['user-2']);
  });

  it('delivers the pending copy of its text and leaves a refused one its error, in either order', async () => {
    const pending = user('steering-user-1', 'Add a museum', { steering: true });
    const refused = user('user-2', 'Add a museum', { queueError: 'crafted steer refusal', queueRefused: true });
    for (const steers of [[pending, refused], [refused, pending]]) {
      const messages = await reloadWith(steers);

      expect(steersIn(messages, 'Add a museum')).toEqual(['history-steering-user-1-1-0', 'user-2']);
      expect(messages.find((m) => m.id === 'user-2')).toMatchObject({ queueError: 'crafted steer refusal' });
    }
  });

  it('delivers the copy whose POST lost its answer, never one the server refused, in either order', async () => {
    const lost = user('user-2', 'Add a museum', { queueError: 'Failed to fetch' });
    const refused = user('user-3', 'Add a museum', { queueError: 'crafted steer refusal', queueRefused: true });
    for (const steers of [[lost, refused], [refused, lost]]) {
      const messages = await reloadWith(steers);

      expect(steersIn(messages, 'Add a museum')).toEqual(['history-steering-user-1-1-0', 'user-3']);
      expect(messages.find((m) => m.id === 'user-3')).toMatchObject({ queueError: 'crafted steer refusal' });
    }
  });

  it('of two pending copies delivers the older, as the queue delivers steers in the order it took them', async () => {
    const messages = await reloadWith([
      user('steering-user-1', 'Add a museum', { steering: true }),
      user('steering-user-2', 'Add a museum', { steering: true }),
    ]);

    expect(steersIn(messages, 'Add a museum')).toEqual(['history-steering-user-1-1-0', 'steering-user-2']);
  });

  it('of two copies whose POSTs lost their answers delivers the older, for the same reason', async () => {
    const messages = await reloadWith([
      user('user-2', 'Add a museum', { queueError: 'Failed to fetch' }),
      user('user-3', 'Add a museum', { queueError: 'Failed to fetch' }),
    ]);

    expect(steersIn(messages, 'Add a museum')).toEqual(['history-steering-user-1-1-0', 'user-3']);
  });
});

describe('history replay: a turn opened hidden', () => {
  // The onboarding kickoff's query is stored empty: its words were the model's.
  const hiddenOpener = (turn: number, runId?: string) => ({
    event: 'user_message',
    data: {
      thread_id: 'thread-1', turn_index: turn, content: '',
      metadata: { attachments: [{ name: 'positions.png', type: 'image/png', size: 1024 }] },
      ...(runId ? { run_id: runId } : {}),
    },
  });
  const liveReply = (over: Record<string, unknown> = {}) =>
    ({ id: 'assistant-live', role: 'assistant', content: 'Welc', turnIndex: 0, isStreaming: false, hiddenOpener: true, contentSegments: [], ...over }) as unknown as ChatMessage;

  it('draws no user row for it, attachments and all, so the reply opens the chat', async () => {
    const { rt, read } = buildRuntime();
    api.replayThreadHistory.mockImplementation(replayOf([
      hiddenOpener(0, 'run-0'), chunk(0, 'Welcome. Three places to start.'),
      opener(1, 'The first one', 'run-1'), chunk(1, 'On it.'),
    ]));
    await loadConversationHistory(rt, makeDeps());

    expect(ids(read())).toEqual(['history-assistant-0', 'history-user-1', 'history-assistant-1']);
    expect(read()[0].content).toBe('Welcome. Three places to start.');
  });

  it('gives its live copy way to the replay, as an opener with text would', async () => {
    const { rt, read } = buildRuntime();
    rt.setMessages(() => [liveReply()]);
    api.replayThreadHistory.mockImplementation(replayOf([hiddenOpener(0, 'run-0'), chunk(0, 'Welcome.')]));
    await loadConversationHistory(rt, makeDeps());

    expect(ids(read())).toEqual(['history-assistant-0']);
  });

  it('flags its reply as the live one was, which a turn of attachments alone is not', async () => {
    const { rt, read } = buildRuntime();
    const kickoff = {
      event: 'user_message',
      data: {
        thread_id: 'thread-1', turn_index: 0, content: '', run_id: 'run-0',
        // What the server keeps of the kickoff: the skill's name, never its instruction.
        metadata: { additional_context: [{ type: 'skills', name: 'onboarding' }, { type: 'directive', content: '<market-brief>' }] },
      },
    };
    const attachmentsAlone = {
      event: 'user_message',
      data: {
        thread_id: 'thread-1', turn_index: 1, content: '', run_id: 'run-1',
        metadata: { attachments: [{ name: 'positions.png', type: 'image/png', size: 1024 }] },
      },
    };
    api.replayThreadHistory.mockImplementation(replayOf([
      kickoff, chunk(0, 'Welcome.'),
      attachmentsAlone, chunk(1, 'Two positions.'),
    ]));
    await loadConversationHistory(rt, makeDeps());

    expect(ids(read())).toEqual(['history-assistant-0', 'history-assistant-1']);
    expect(read()[0]).toMatchObject({ hiddenOpener: true });
    expect((read()[1] as { hiddenOpener?: boolean }).hiddenOpener).toBeUndefined();
  });

  it('leaves unflagged a turn of attachments alone sent with Watch on, whose skill the user sent', async () => {
    const { rt, read } = buildRuntime();
    // The Watch toggle adds the `market-watch` skill to every send, so a turn
    // of files alone stores an empty query with a skill named beside them.
    const watchedAttachments = {
      event: 'user_message',
      data: {
        thread_id: 'thread-1', turn_index: 0, content: '', run_id: 'run-0',
        metadata: {
          attachments: [{ name: 'positions.png', type: 'image', size: 1024 }],
          additional_context: [{ type: 'skills', name: 'market-watch' }],
        },
      },
    };
    api.replayThreadHistory.mockImplementation(replayOf([watchedAttachments, chunk(0, 'Two positions.')]));
    await loadConversationHistory(rt, makeDeps());

    expect(ids(read())).toEqual(['history-assistant-0']);
    expect((read()[0] as { hiddenOpener?: boolean }).hiddenOpener).toBeUndefined();
  });

  it('keeps a reply the server refused, which holds no turn of the replay', async () => {
    const { rt, read } = buildRuntime();
    rt.setMessages(() => [liveReply({ unadmitted: true, error: true, content: '' })]);
    api.replayThreadHistory.mockImplementation(replayOf([hiddenOpener(0, 'run-0'), chunk(0, 'Welcome.')]));
    await loadConversationHistory(rt, makeDeps());

    expect(ids(read())).toEqual(['history-assistant-0', 'assistant-live']);
  });
});

describe('history replay: a steer refused in an earlier turn', () => {
  it('stays under the turn it was sent in, above the turns after it', async () => {
    const { rt, read } = buildRuntime();
    const row = (id: string, role: string, turnIndex: number, content: string, over: Record<string, unknown> = {}) =>
      ({ id, role, content, turnIndex, isStreaming: false, isHistory: true, ...over }) as unknown as ChatMessage;
    // Each turn's end marked its bubbles for the replay; the refusals kept theirs.
    rt.setMessages(() => [
      row('user-0', 'user', 0, 'Name a river'), row('assistant-0', 'assistant', 0, 'The Nile'),
      row('user-r0', 'user', 0, 'Shorter', { isHistory: false, queueError: 'crafted steer refusal' }),
      row('user-1', 'user', 1, 'Now lakes'), row('assistant-1', 'assistant', 1, 'Lake Baikal'),
      row('user-r1', 'user', 1, 'And seas', { isHistory: false, queueError: 'crafted steer refusal' }),
      row('user-2', 'user', 2, 'Now mountains'), row('assistant-2', 'assistant', 2, 'Everest'),
    ]);
    api.replayThreadHistory.mockImplementation(replayOf([
      opener(0, 'Name a river', 'run-0'), chunk(0, 'The Nile'),
      opener(1, 'Now lakes', 'run-1'), chunk(1, 'Lake Baikal'),
      opener(2, 'Now mountains', 'run-2'), chunk(2, 'Everest'),
    ]));
    await loadConversationHistory(rt, makeDeps());

    expect(ids(read())).toEqual([
      'history-user-0', 'history-assistant-0', 'user-r0',
      'history-user-1', 'history-assistant-1', 'user-r1',
      'history-user-2', 'history-assistant-2',
    ]);
  });
});
