// @vitest-environment node
/**
 * Price alerts in a replayed thread. A wake turn's query is written for the
 * model, so it heads its turn as an alert row rather than a user bubble; an
 * alert delivered mid-turn is a `steering_delivered` entry and splits the
 * turn where a delivered steer does. An alert row's id is its notice's, so
 * the live stream and every replay draw the same row under it.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { ChatMessage } from '@/types/chat';
import type { SSEEvent } from '../../types';
import { createStreamEventProcessor } from '../../stream/processStreamEvent';

const api = vi.hoisted(() => ({ replayThreadHistory: vi.fn() }));

vi.mock('../../../utils/api', () => ({
  replayThreadHistory: api.replayThreadHistory,
}));

import { loadConversationHistory } from '../replayHistory';
import { buildRuntime, makeDeps, replayOf } from './replayHarness';

// Neutral placeholder ticker and made-up prices only.
const NOTICE = {
  target_id: 'tg-1',
  notice: 'hit',
  symbol: 'ACME',
  kind: 'above',
  value: 205,
  pct_from: null,
  fired_price: 205.12,
  fired_at: '2030-01-01T15:05:00Z',
  expires_at: null,
};
const ALERT = { content: '', timestamp: 1_700_000_000, market_alert: NOTICE };
// The notice as a price wakeup's, which its row is drawn from.
const AS_WAKEUP = {
  wakeup_id: 'tg-1', kind: 'price', notice: 'fire', seq: 1, total: 1, fired_at: '2030-01-01T15:05:00Z',
  price: { symbol: 'ACME', kind: 'above', value: 205, pct_from: null, fired_price: 205.12 },
};

const chunk = (turn: number, content: string) => ({
  event: 'message_chunk',
  data: { thread_id: 'thread-1', turn_index: turn, role: 'assistant', agent: 'main', content_type: 'text', content },
});
const opener = (turn: number, content: string) => ({
  event: 'user_message', data: { thread_id: 'thread-1', turn_index: turn, content },
});
const delivery = (turn: number, messages: Array<Record<string, unknown>>, extra: Record<string, unknown> = {}) => ({
  event: 'steering_delivered',
  data: { thread_id: 'thread-1', turn_index: turn, count: 0, messages, ...extra },
});

type Row = Record<string, unknown>;

beforeEach(() => vi.clearAllMocks());

describe('history replay: market targets', () => {
  it('heads a wake turn with the alert row, not the query text', async () => {
    const { rt, read } = buildRuntime();
    api.replayThreadHistory.mockImplementation(replayOf([
      opener(0, 'Watch ACME above 205'),
      chunk(0, 'Watching.'),
      {
        event: 'user_message',
        data: {
          thread_id: 'thread-1',
          turn_index: 1,
          query_type: 'market_target',
          content: '<market-alert>ACME crossed above 205</market-alert>',
          metadata: { market_target: { notices: [NOTICE] } },
        },
      },
      chunk(1, 'ACME is through 205.'),
    ]));

    await loadConversationHistory(rt, makeDeps());

    const messages = read() as unknown as Row[];
    expect(messages.map((m) => m.role)).toEqual(['user', 'assistant', 'notification', 'assistant']);
    expect(JSON.stringify(messages)).not.toContain('<market-alert>');
    expect(messages[2]).toMatchObject({
      id: 'history-market-alert-wake-1',
      isHistory: true,
      wakeup: { source: 'wake', legacy: true, notices: [AS_WAKEUP] },
    });
    expect(messages[3]).toMatchObject({ turnIndex: 1, content: 'ACME is through 205.' });
  });

  it('draws the row a live attach to the wake drew, and takes the place of the live copy', async () => {
    // The row the run's first frame drew for an attach, before this reload.
    let live: ChatMessage[] = [
      { id: 'a1', role: 'assistant', content: '', isStreaming: true, contentSegments: [] } as unknown as ChatMessage,
    ];
    const setLive = (updater: (prev: ChatMessage[]) => ChatMessage[]) => {
      live = updater(live);
    };
    const process = createStreamEventProcessor(
      {
        setMessages: setLive,
        queueMessages: setLive,
        flushMessages: vi.fn(),
        currentMessageRef: { current: 'a1' },
        currentRunIdRef: { current: null },
        latestRunIdRef: { current: null },
        latestRunEpochRef: { current: 0 },
        lastEventIdRef: { current: null },
        threadIdRef: { current: 'thread-1' },
      } as unknown as Parameters<typeof createStreamEventProcessor>[0],
      { clearModelStatus: () => {}, refreshMarketWatch: vi.fn(), refreshWakeups: vi.fn(), refreshWorkspaceFolder: null } as unknown as Parameters<typeof createStreamEventProcessor>[1],
      'a1',
      {
        contentOrderCounterRef: { current: 0 },
        currentReasoningIdRef: { current: null },
        currentToolCallIdRef: { current: null },
        subagentStateRefs: {},
        isReconnect: false,
      } as unknown as Parameters<typeof createStreamEventProcessor>[3],
      (event) => (event.agent as string) ?? null,
    );
    process({ event: 'metadata', thread_id: 'thread-1', run_id: 'run-wake', turn_index: 1, market_target: NOTICE } as unknown as SSEEvent);
    const liveRow = live.find((m) => m.role === 'notification');
    expect(liveRow?.id).toBe('history-market-alert-wake-1');

    const { rt, read } = buildRuntime();
    rt.setMessages(() => [liveRow!]);
    api.replayThreadHistory.mockImplementation(replayOf([
      opener(0, 'Watch ACME above 205'),
      chunk(0, 'Watching.'),
      {
        event: 'user_message',
        data: {
          thread_id: 'thread-1',
          turn_index: 1,
          query_type: 'market_target',
          content: '<market-alert>ACME crossed above 205</market-alert>',
          metadata: { market_target: NOTICE },
        },
      },
      chunk(1, 'ACME is through 205.'),
    ]));

    await loadConversationHistory(rt, makeDeps());

    const messages = read() as unknown as Row[];
    expect(messages.map((m) => m.id)).toEqual([
      'history-user-0', 'history-assistant-0', 'history-market-alert-wake-1', 'history-assistant-1',
    ]);
    const { isHistory, timestamp: _replayedAt, ...replayed } = messages[2];
    const { timestamp: _drawnAt, ...drawn } = liveRow as unknown as Row;
    expect(isHistory).toBe(true);
    expect(replayed).toEqual(drawn);
  });

  it('keeps a wake row with nothing to name, for the generic label', async () => {
    const { rt, read } = buildRuntime();
    api.replayThreadHistory.mockImplementation(replayOf([
      {
        event: 'user_message',
        data: { thread_id: 'thread-1', turn_index: 0, query_type: 'market_target', content: '<market-alert>…</market-alert>' },
      },
      chunk(0, 'Checking.'),
    ]));

    await loadConversationHistory(rt, makeDeps());

    const [row] = read() as unknown as Row[];
    expect(row).toMatchObject({ role: 'notification', wakeup: { source: 'wake', legacy: true, notices: [] } });
  });

  it('splits a turn at a delivered alert, the rest of the turn in a continuation', async () => {
    const { rt, read } = buildRuntime();
    api.replayThreadHistory.mockImplementation(replayOf([
      opener(0, 'Watch ACME and wait'),
      chunk(0, 'Waiting.'),
      delivery(0, [ALERT]),
      chunk(0, 'It crossed.'),
    ]));

    await loadConversationHistory(rt, makeDeps());

    const messages = read() as unknown as Row[];
    expect(messages.map((m) => m.id)).toEqual([
      'history-user-0',
      'history-assistant-0',
      'market-alert-tg-1:hit',
      'history-assistant-steering-0-1',
    ]);
    expect(messages[1].content).toBe('Waiting.');
    expect(messages[2]).toMatchObject({ role: 'notification', isHistory: true, wakeup: { source: 'delivered', legacy: true, notices: [AS_WAKEUP] } });
    expect(messages[3]).toMatchObject({ content: 'It crossed.', isSteering: true, turnIndex: 0 });
  });

  it("draws the alert ahead of the user's words delivered with it", async () => {
    const { rt, read } = buildRuntime();
    api.replayThreadHistory.mockImplementation(replayOf([
      opener(0, 'Watch ACME and wait'),
      chunk(0, 'Waiting.'),
      delivery(0, [ALERT, { content: 'and check margins', timestamp: 1_700_000_001 }], { count: 1 }),
      chunk(0, 'It crossed; margins next.'),
    ]));

    await loadConversationHistory(rt, makeDeps());

    expect((read() as unknown as Row[]).map((m) => m.id)).toEqual([
      'history-user-0',
      'history-assistant-0',
      'market-alert-tg-1:hit',
      'history-steering-user-0-1-1',
      'history-assistant-steering-0-1',
    ]);
  });

  it('starts the continuation when the run took the alert in, not when replay ran', async () => {
    const { rt, read } = buildRuntime();
    api.replayThreadHistory.mockImplementation(replayOf([
      opener(0, 'Watch ACME and wait'),
      chunk(0, 'Waiting.'),
      delivery(0, [{ ...ALERT, timestamp: 1_699_999_999 }], { timestamp: 1_700_000_000 }),
      chunk(0, 'It crossed.'),
    ]));

    await loadConversationHistory(rt, makeDeps());

    const continuation = (read() as unknown as Array<{ id: string; timestamp: Date }>)
      .find((m) => m.id === 'history-assistant-steering-0-1');
    expect(continuation?.timestamp.getTime()).toBe(1_700_000_000_000);
  });

  it('draws the row the live stream drew, and takes the place of a live copy left on screen', async () => {
    // The live stream's row for the same entry.
    let live: ChatMessage[] = [
      { id: 'a1', role: 'assistant', content: '', isStreaming: true, turnIndex: 0, contentSegments: [] } as unknown as ChatMessage,
    ];
    const setLive = (updater: (prev: ChatMessage[]) => ChatMessage[]) => {
      live = updater(live);
    };
    const process = createStreamEventProcessor(
      {
        setMessages: setLive,
        queueMessages: setLive,
        flushMessages: vi.fn(),
        currentMessageRef: { current: 'a1' },
        lastEventIdRef: { current: null },
        threadIdRef: { current: 'thread-1' },
      } as unknown as Parameters<typeof createStreamEventProcessor>[0],
      { clearModelStatus: () => {}, refreshMarketWatch: vi.fn(), refreshWakeups: vi.fn() } as unknown as Parameters<typeof createStreamEventProcessor>[1],
      'a1',
      {
        contentOrderCounterRef: { current: 0 },
        currentReasoningIdRef: { current: null },
        currentToolCallIdRef: { current: null },
        subagentStateRefs: {},
        isReconnect: false,
      } as unknown as Parameters<typeof createStreamEventProcessor>[3],
      (event) => (event.agent as string) ?? null,
    );
    process({ event: 'steering_delivered', messages: [ALERT] } as unknown as SSEEvent);
    const liveRow = live.find((m) => m.role === 'notification');
    expect(liveRow?.id).toBe('market-alert-tg-1:hit');

    const { rt, read } = buildRuntime();
    rt.setMessages(() => [liveRow!]);
    api.replayThreadHistory.mockImplementation(replayOf([
      opener(0, 'Watch ACME and wait'),
      chunk(0, 'Waiting.'),
      delivery(0, [ALERT]),
      chunk(0, 'It crossed.'),
    ]));

    await loadConversationHistory(rt, makeDeps());

    const messages = read() as unknown as Row[];
    expect(messages.filter((m) => m.id === 'market-alert-tg-1:hit')).toHaveLength(1);
    expect(messages.map((m) => m.id)).toEqual([
      'history-user-0',
      'history-assistant-0',
      'market-alert-tg-1:hit',
      'history-assistant-steering-0-1',
    ]);
    const { isHistory, ...replayed } = messages[2];
    expect(isHistory).toBe(true);
    expect(replayed).toEqual(liveRow);
  });
});
